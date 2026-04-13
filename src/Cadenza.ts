import Cadenza, {
  Actor,
  ActorDefinition,
  ActorFactoryOptions,
  ActorSpec,
  AnyObject,
  CadenzaMode,
  DebounceOptions,
  DebounceTask,
  EmitOptions,
  EphemeralTask,
  EphemeralTaskOptions,
  GraphRegistry,
  GraphRoutine,
  GraphRunner,
  InquiryBroker,
  Intent,
  RuntimeValidationPolicy,
  RuntimeValidationScope,
  SignalBroker,
  Task,
  TaskFunction,
  TaskOptions,
  ThrottleTagGetter,
} from "@cadenza.io/core";
import DeputyTask from "./graph/definition/DeputyTask";
import DatabaseTask from "./graph/definition/DatabaseTask";
import ServiceRegistry from "./registry/ServiceRegistry";
import SignalTransmissionTask from "./graph/definition/SignalTransmissionTask";
import RestController from "@service-rest-controller";
import SocketController from "./network/SocketController";
import SignalController from "./signals/SignalController";
import RuntimeValidationController from "./runtime/RuntimeValidationController";
import { DbOperationPayload, DbOperationType } from "./types/queryData";
import GraphMetadataController from "./graph/controllers/GraphMetadataController";
import {
  META_ACTOR_SESSION_STATE_HYDRATE_INTENT,
  registerActorSessionPersistenceTasks,
} from "./graph/controllers/registerActorSessionPersistence";
import { DatabaseSchemaDefinition } from "./types/database";
import { camelCase, snakeCase } from "lodash-es";
import DatabaseController from "@service-database-controller";
import { v4 as uuid } from "uuid";
import GraphSyncController from "./graph/controllers/GraphSyncController";
import { isBrowser } from "./utils/environment";
import { formatTimestamp } from "./utils/tools";
import {
  BootstrapOptions,
  HydrationOptions,
  readIntegerEnv,
  readListEnv,
  readStringEnv,
  resolveBootstrapEndpoint,
} from "./utils/bootstrap";
import {
  DistributedInquiryMeta,
  DistributedInquiryOptions,
  InquiryResponderDescriptor,
  InquiryResponderStatus,
} from "./types/inquiry";
import type {
  ServiceTransportConfig,
  ServiceTransportRole,
} from "./types/transport";
import {
  compareResponderDescriptors,
  isMetaIntentName,
  mergeInquiryContexts,
  shouldExecuteInquiryResponder,
  summarizeResponderStatuses,
} from "./utils/inquiry";
import { normalizeServiceTransportConfig } from "./utils/transport";
import {
  type BrowserRuntimeActorHandle,
  type BrowserRuntimeActorOptions,
  resetBrowserRuntimeActorHandles,
  createBrowserRuntimeActor as createBrowserRuntimeActorHelper,
} from "./frontend/createBrowserRuntimeActor";
import { buildServiceManifestSnapshot } from "./registry/serviceManifest";
import { AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT } from "./registry/serviceManifestContract";
import { isAuthorityBootstrapIntent } from "./registry/authorityBootstrapControlPlane";
import type { ServiceManifestPublicationLayer } from "./types/serviceManifest";

export type SecurityProfile = "low" | "medium" | "high";
export type NetworkMode =
  | "internal"
  | "exposed"
  | "exposed-high-sec"
  | "auto"
  | "dev";

const POSTGRES_SETUP_DEBUG_ENABLED =
  process.env.CADENZA_POSTGRES_SETUP_DEBUG === "1" ||
  process.env.CADENZA_POSTGRES_SETUP_DEBUG === "true";

function resolveInquiryFailureError(
  inquiry: string,
  value: unknown,
  depth = 3,
  seen = new Set<unknown>(),
): string {
  if (depth < 0 || value === null || value === undefined) {
    return `Inquiry '${inquiry}' did not complete successfully`;
  }

  if (typeof value === "string") {
    return value;
  }

  if (value instanceof Error) {
    return value.message;
  }

  if (typeof value !== "object" || seen.has(value)) {
    return `Inquiry '${inquiry}' did not complete successfully`;
  }

  seen.add(value);

  const record = value as Record<string, unknown>;
  for (const key of ["__error", "error", "message"] as const) {
    const candidate = record[key];

    if (typeof candidate === "string" && candidate.trim().length > 0) {
      return candidate;
    }

    if (candidate && typeof candidate === "object") {
      const resolved = resolveInquiryFailureError(
        inquiry,
        candidate,
        depth - 1,
        seen,
      );
      if (resolved !== `Inquiry '${inquiry}' did not complete successfully`) {
        return resolved;
      }
    }
  }

  for (const nested of Object.values(record)) {
    if (!nested || typeof nested !== "object") {
      continue;
    }

    const resolved = resolveInquiryFailureError(
      inquiry,
      nested,
      depth - 1,
      seen,
    );
    if (resolved !== `Inquiry '${inquiry}' did not complete successfully`) {
      return resolved;
    }
  }

  return `Inquiry '${inquiry}' did not complete successfully`;
}

function normalizePositiveInteger(value: unknown, fallback: number): number {
  const normalized = Number(value);
  return Number.isInteger(normalized) && normalized > 0 ? normalized : fallback;
}

export type ServerOptions = {
  customServiceId?: string; // TODO
  loadBalance?: boolean;
  useSocket?: boolean;
  log?: boolean;
  displayName?: string;
  isMeta?: boolean;
  port?: number; // for internal network
  securityProfile?: SecurityProfile;
  networkMode?: NetworkMode;
  retryCount?: number;
  cadenzaDB?: { connect?: boolean; address?: string; port?: number };
  bootstrap?: BootstrapOptions;
  hydration?: HydrationOptions;
  transports?: ServiceTransportConfig[];
  relatedServices?: string[][];
  isDatabase?: boolean;
  isFrontend?: boolean;
};

export interface DatabaseOptions {
  databaseType?: "postgres";
  databaseName?: string;
  poolSize?: number;
  ownerServiceName?: string | null;
}

const DEFAULT_DEPUTY_TASK_CONCURRENCY = 50;
const DEFAULT_DEPUTY_TASK_TIMEOUT_MS = 120_000;
const DEFAULT_DATABASE_PROXY_TASK_CONCURRENCY = 50;
const DEFAULT_DATABASE_PROXY_TASK_TIMEOUT_MS = 120_000;
const SERVICE_MANIFEST_PUBLICATION_ORDER: ServiceManifestPublicationLayer[] = [
  "routing_capability",
  "business_structural",
  "local_meta_structural",
];

function getServiceManifestPublicationLayerRank(
  layer: ServiceManifestPublicationLayer,
): number {
  const index = SERVICE_MANIFEST_PUBLICATION_ORDER.indexOf(layer);
  return index >= 0 ? index : SERVICE_MANIFEST_PUBLICATION_ORDER.length - 1;
}

/**
 * The CadenzaService class serves as a central service layer providing various utility methods for managing tasks, signals, logging, and service interactions.
 * This class handles the initialization (`bootstrap`) and validation of services, as well as the creation of tasks associated with services and signals.
 */
export default class CadenzaService {
  public static signalBroker: SignalBroker;
  public static inquiryBroker: InquiryBroker;
  public static runner: GraphRunner;
  public static metaRunner: GraphRunner;
  public static registry: GraphRegistry;
  public static serviceRegistry: ServiceRegistry;
  protected static isBootstrapped = false;
  protected static serviceCreated = false;
  protected static bootstrapSyncCompleted = false;
  protected static bootstrapSignalRegistrationsCompleted = false;
  protected static bootstrapIntentRegistrationsCompleted = false;
  protected static defaultDatabaseServiceName: string | null = null;
  protected static warnedInvalidMetaIntentResponderKeys: Set<string> = new Set();
  protected static hydratedInquiryResults: Map<string, AnyObject> = new Map();
  protected static frontendSyncScheduled = false;
  protected static serviceManifestRevision = 0;
  protected static lastPublishedServiceManifestHashes: Partial<
    Record<ServiceManifestPublicationLayer, string>
  > = {};
  protected static serviceManifestPublicationInFlight = false;
  protected static serviceManifestPublicationPendingReason: string | null = null;
  protected static serviceManifestPublicationPendingLayer:
    | ServiceManifestPublicationLayer
    | null = null;
  private static shutdownHandlersRegistered = false;
  private static shutdownInFlight = false;
  private static shutdownHandlerCleanup: Array<() => void> = [];

  private static unregisterGracefulShutdownHandlers(): void {
    for (const cleanup of this.shutdownHandlerCleanup) {
      cleanup();
    }
    this.shutdownHandlerCleanup = [];
    this.shutdownHandlersRegistered = false;
    this.shutdownInFlight = false;
  }

  private static registerGracefulShutdownHandlers(): void {
    if (
      isBrowser ||
      this.shutdownHandlersRegistered ||
      typeof process === "undefined" ||
      typeof process.once !== "function" ||
      typeof process.removeListener !== "function"
    ) {
      return;
    }

    const gracefulShutdownTimeoutMs = Math.max(
      1_500,
      readIntegerEnv("CADENZA_GRACEFUL_SHUTDOWN_TIMEOUT_MS", 5_000),
    );
    const transportShutdownDelayMs = Math.min(
      Math.max(500, Math.floor(gracefulShutdownTimeoutMs / 3)),
      Math.max(500, gracefulShutdownTimeoutMs - 500),
    );
    const shutdownSignals = ["SIGTERM", "SIGINT"] as const;

    for (const signal of shutdownSignals) {
      const handler = () => {
        if (this.shutdownInFlight) {
          return;
        }

        this.shutdownInFlight = true;

        const exitTimer = setTimeout(() => {
          this.unregisterGracefulShutdownHandlers();
          if (typeof process.kill === "function") {
            process.kill(process.pid, signal);
            return;
          }

          process.exit(0);
        }, gracefulShutdownTimeoutMs);

        exitTimer.unref?.();

        void (async () => {
          try {
            const shutdownPersisted =
              (await this.serviceRegistry?.reportLocalShutdownToAuthority(
                signal,
                Math.max(1_500, gracefulShutdownTimeoutMs - 750),
              )) ?? false;

            if (!shutdownPersisted) {
              this.emit("meta.service_registry.instance_shutdown_reported", {
                serviceName: this.serviceRegistry?.serviceName ?? null,
                serviceInstanceId: this.serviceRegistry?.serviceInstanceId ?? null,
                reason: signal,
                graceful: true,
              });
            }
          } catch {
            try {
              this.emit("meta.service_registry.instance_shutdown_reported", {
                serviceName: this.serviceRegistry?.serviceName ?? null,
                serviceInstanceId: this.serviceRegistry?.serviceInstanceId ?? null,
                reason: signal,
                graceful: true,
              });
            } catch {
              // Best-effort shutdown reporting must not block process termination.
            }
          } finally {
            try {
              this.schedule(
                "meta.socket_server_shutdown_requested",
                { reason: signal },
                transportShutdownDelayMs,
              );
              this.schedule(
                "meta.server_shutdown_requested",
                { reason: signal },
                transportShutdownDelayMs,
              );
            } catch {
              // Best-effort shutdown reporting must not block process termination.
            }
          }
        })();
      };

      process.once(signal, handler);
      this.shutdownHandlerCleanup.push(() =>
        process.removeListener(signal, handler),
      );
    }

    this.shutdownHandlersRegistered = true;
  }

  private static replayRegisteredTaskIntentAssociations(): void {
    for (const task of this.registry.tasks.values()) {
      if (!task.register || task.isHidden || task.handlesIntents.size === 0) {
        continue;
      }

      for (const intentName of task.handlesIntents) {
        task.emitWithMetadata("meta.task.intent_associated", {
          data: {
            intentName,
            taskName: task.name,
            taskVersion: task.version,
          },
          taskInstance: task,
          __isSubMeta: task.isSubMeta,
        });
      }
    }
  }

  private static replayRegisteredTaskSignalObservations(): void {
    for (const task of this.registry.tasks.values()) {
      if (!task.register || task.isHidden || task.observedSignals.size === 0) {
        continue;
      }

      for (const signalName of task.observedSignals) {
        task.emitWithMetadata("meta.task.observed_signal", {
          data: {
            signalName,
            taskName: task.name,
            taskVersion: task.version,
          },
          taskInstance: task,
          signalName,
          __isSubMeta: task.isSubMeta,
        });
      }
    }
  }

  private static replayRegisteredTaskGraphMetadata(): void {
    this.replayRegisteredTaskSignalObservations();
    this.replayRegisteredTaskIntentAssociations();
  }

  private static normalizeServiceManifestPublicationLayer(
    value: unknown,
    fallback: ServiceManifestPublicationLayer = "business_structural",
  ): ServiceManifestPublicationLayer {
    return value === "routing_capability" ||
      value === "business_structural" ||
      value === "local_meta_structural"
      ? value
      : fallback;
  }

  private static mergeServiceManifestPublicationRequest(
    reason: string,
    targetLayer: ServiceManifestPublicationLayer,
  ): void {
    this.serviceManifestPublicationPendingReason = reason;

    const currentTargetLayer =
      this.serviceManifestPublicationPendingLayer ?? "routing_capability";
    this.serviceManifestPublicationPendingLayer =
      getServiceManifestPublicationLayerRank(targetLayer) >=
      getServiceManifestPublicationLayerRank(currentTargetLayer)
        ? targetLayer
        : currentTargetLayer;
  }

  private static requestServiceManifestPublication(
    reason: string,
    immediate = false,
    targetLayer: ServiceManifestPublicationLayer = "business_structural",
  ): void {
    if (!this.serviceRegistry.serviceName || !this.serviceRegistry.serviceInstanceId) {
      return;
    }

    const signalName = "meta.service_manifest.publish_requested";
    const payload = {
      __reason: reason,
      __serviceName: this.serviceRegistry.serviceName,
      __serviceInstanceId: this.serviceRegistry.serviceInstanceId,
      __publicationLayer: targetLayer,
    };

    if (immediate) {
      this.emit(signalName, payload);
      return;
    }

    this.debounce(signalName, payload, 100);
  }

  private static scheduleServiceManifestPublicationRetry(
    reason: string,
    targetLayer: ServiceManifestPublicationLayer = "business_structural",
  ): void {
    if (!this.serviceRegistry.serviceName || !this.serviceRegistry.serviceInstanceId) {
      return;
    }

    setTimeout(() => {
      this.requestServiceManifestPublication(reason, false, targetLayer);
    }, 1000);
  }

  private static async publishServiceManifestIfNeeded(
    reason: string,
    targetLayer: ServiceManifestPublicationLayer = "business_structural",
  ): Promise<
    | false
    | {
        serviceManifest: ReturnType<typeof buildServiceManifestSnapshot>;
        published: true;
        publicationLayer: ServiceManifestPublicationLayer;
      }
  > {
    if (
      !this.serviceRegistry.connectsToCadenzaDB ||
      !this.serviceRegistry.serviceName ||
      !this.serviceRegistry.serviceInstanceId
    ) {
      return false;
    }

    const publishReason =
      typeof reason === "string" && reason.trim().length > 0
        ? reason.trim()
        : "service_manifest_publish";
    const publishTargetLayer = this.normalizeServiceManifestPublicationLayer(
      targetLayer,
    );

    if (this.serviceManifestPublicationInFlight) {
      this.mergeServiceManifestPublicationRequest(
        publishReason,
        publishTargetLayer,
      );
      return false;
    }

    const publicationPlan = SERVICE_MANIFEST_PUBLICATION_ORDER
      .filter(
        (layer) =>
          getServiceManifestPublicationLayerRank(layer) <=
          getServiceManifestPublicationLayerRank(publishTargetLayer),
      )
      .map((layer) => {
        const snapshot = buildServiceManifestSnapshot({
          serviceName: this.serviceRegistry.serviceName!,
          serviceInstanceId: this.serviceRegistry.serviceInstanceId!,
          revision: this.serviceManifestRevision + 1,
          publishedAt: new Date().toISOString(),
          publicationLayer: layer,
        });

        return {
          layer,
          snapshot,
          changed:
            this.lastPublishedServiceManifestHashes[layer] !== snapshot.manifestHash,
        };
      });

    const nextPublication = publicationPlan.find((entry) => entry.changed);

    if (!nextPublication) {
      return false;
    }

    const { layer: publicationLayer, snapshot } = nextPublication;
    const hasPendingFollowupLayer = publicationPlan.some(
      (entry) =>
        entry.changed &&
        getServiceManifestPublicationLayerRank(entry.layer) >
          getServiceManifestPublicationLayerRank(publicationLayer),
    );

    this.serviceManifestPublicationInFlight = true;
    try {
      this.serviceRegistry.ensureBootstrapAuthorityControlPlaneForInquiry(
        AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
        snapshot,
      );
      if (!this.serviceRegistry.hasAuthorityBootstrapHandshakeEstablished()) {
        this.scheduleServiceManifestPublicationRetry(
          publishReason,
          publishTargetLayer,
        );
        return false;
      }
      await this.inquire(AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT, snapshot, {
        timeout: 15_000,
        requireComplete: true,
      });
      this.serviceManifestRevision = snapshot.revision;
      this.lastPublishedServiceManifestHashes[publicationLayer] = snapshot.manifestHash;
      if (hasPendingFollowupLayer) {
        this.mergeServiceManifestPublicationRequest(
          publishReason,
          publishTargetLayer,
        );
      }
      return {
        serviceManifest: snapshot,
        published: true,
        publicationLayer,
      };
    } catch (error) {
      this.log("Service manifest publication failed. Scheduling retry.", {
        serviceName: this.serviceRegistry.serviceName,
        serviceInstanceId: this.serviceRegistry.serviceInstanceId,
        reason: publishReason,
        publicationLayer,
        error: resolveInquiryFailureError(
          AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
          error,
        ),
        inquiryMeta:
          error && typeof error === "object" && "__inquiryMeta" in error
            ? (error as Record<string, unknown>).__inquiryMeta
            : null,
      });
      this.scheduleServiceManifestPublicationRetry(
        publishReason,
        publishTargetLayer,
      );
      return false;
    } finally {
      this.serviceManifestPublicationInFlight = false;
      if (
        this.serviceManifestPublicationPendingReason &&
        this.serviceManifestPublicationPendingLayer
      ) {
        const pendingReason = this.serviceManifestPublicationPendingReason;
        const pendingLayer = this.serviceManifestPublicationPendingLayer;
        this.serviceManifestPublicationPendingReason = null;
        this.serviceManifestPublicationPendingLayer = null;
        this.debounce(
          "meta.service_manifest.publish_requested",
          {
            __reason: pendingReason,
            __publicationLayer: pendingLayer,
          },
          100,
        );
      }
    }
  }

  private static ensureServiceManifestPublicationTasks(): void {
    if (this.get("Publish service manifest")) {
      return;
    }

    this.createMetaTask(
      "Publish service manifest",
      async (ctx) =>
        this.publishServiceManifestIfNeeded(
          typeof ctx.__reason === "string" && ctx.__reason.trim().length > 0
            ? ctx.__reason.trim()
            : "service_manifest_publish",
          this.normalizeServiceManifestPublicationLayer(
            ctx.__publicationLayer,
            "business_structural",
          ),
        ),
      "Publishes staged static manifest snapshots to authority when the manifest hash changes.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn("meta.service_manifest.publish_requested");

    this.createMetaTask(
      "Request manifest publication after structural change",
      (ctx) => {
        const reason =
          typeof ctx.signal === "string" && ctx.signal.trim().length > 0
            ? ctx.signal
            : typeof ctx.__reason === "string" && ctx.__reason.trim().length > 0
              ? ctx.__reason
              : "manifest_structural_update";
        const targetLayer =
          reason === "meta.service_registry.instance_inserted"
            ? "business_structural"
            : this.normalizeServiceManifestPublicationLayer(
                ctx.__publicationLayer,
                "business_structural",
              );
        this.requestServiceManifestPublication(
          reason,
          reason === "meta.service_registry.instance_inserted",
          targetLayer,
        );
        return true;
      },
      "Requests staged manifest publication when a static service primitive changes.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn(
      "meta.service_registry.instance_inserted",
      "meta.task.created",
      "meta.task.destroyed",
      "meta.task.relationship_added",
      "meta.task.relationship_removed",
      "meta.task.intent_associated",
      "meta.task.helper_associated",
      "meta.task.global_associated",
      "meta.task.observed_signal",
      "meta.task.attached_signal",
      "meta.task.detached_signal",
      "meta.helper.created",
      "meta.helper.updated",
      "meta.global.created",
      "meta.global.updated",
      "meta.helper.helper_associated",
      "meta.helper.global_associated",
      "meta.actor.created",
      "meta.actor.task_associated",
      "meta.fetch.handshake_complete",
      "meta.service_registry.registered_global_signals",
      "meta.service_registry.registered_global_intents",
      "meta.service_registry.initial_sync_complete",
      "global.meta.graph_metadata.routine_created",
      "global.meta.graph_metadata.routine_updated",
    );
  }

  private static buildLegacyLocalCadenzaDBTaskName(
    tableName: string,
    operation: DbOperationType,
  ): string {
    const operationPrefix =
      operation.charAt(0).toUpperCase() + operation.slice(1);
    const helperSuffix = camelCase(String(tableName ?? "").trim());

    return `db${operationPrefix}${helperSuffix.charAt(0).toUpperCase() + helperSuffix.slice(1)}`;
  }

  private static buildGeneratedLocalCadenzaDBTaskName(
    tableName: string,
    operation: DbOperationType,
  ): string {
    const operationPrefix =
      operation.charAt(0).toUpperCase() + operation.slice(1);
    return `${operationPrefix} ${String(tableName ?? "").trim()}`;
  }

  /**
   * Initializes the application by setting up necessary components and configurations.
   * This method ensures the initialization process is only executed once throughout the application lifecycle.
   *
   * @return {void} This method does not return any value.
   */
  static bootstrap(): void {
    if (this.isBootstrapped) return;
    this.isBootstrapped = true;

    Cadenza.bootstrap();
    Cadenza.setRuntimeInquiryDelegate((inquiry, context, options) =>
      this.inquire(
        inquiry,
        context,
        (options ?? {}) as DistributedInquiryOptions,
      ),
    );
    this.signalBroker = Cadenza.signalBroker;
    this.inquiryBroker = Cadenza.inquiryBroker;
    this.runner = Cadenza.runner;
    this.metaRunner = Cadenza.metaRunner;
    this.registry = Cadenza.registry;
    this.serviceRegistry = ServiceRegistry.instance;
    RestController.instance;
    SocketController.instance;
    RuntimeValidationController.instance;
  }

  private static ensureTransportControllers(isFrontend: boolean): void {
    if (!isFrontend) {
      SignalController.instance;
    }
  }

  private static setHydrationResults(
    hydration?: HydrationOptions,
  ): void {
    this.hydratedInquiryResults = new Map();

    const initialInquiryResults = hydration?.initialInquiryResults ?? {};
    for (const [key, value] of Object.entries(initialInquiryResults)) {
      this.hydratedInquiryResults.set(key, value as AnyObject);
    }
  }

  private static consumeHydratedInquiryResult(
    hydrationKey?: string,
  ): AnyObject | undefined {
    if (!hydrationKey) {
      return undefined;
    }

    const result = this.hydratedInquiryResults.get(hydrationKey);
    if (result === undefined) {
      return undefined;
    }

    this.hydratedInquiryResults.delete(hydrationKey);
    return result;
  }

  private static ensureFrontendSyncLoop(): void {
    if (this.frontendSyncScheduled) {
      return;
    }

    this.frontendSyncScheduled = true;
    Cadenza.interval("meta.sync_requested", { __syncing: false }, 180000);
    Cadenza.schedule("meta.sync_requested", { __syncing: false }, 250);
  }

  private static normalizeDeclaredTransports(
    transports: ServiceTransportConfig[] | undefined,
    serviceId: string,
    useSocket: boolean,
  ): Array<
    ServiceTransportConfig & {
      uuid: string;
    }
  > {
    return (transports ?? [])
      .map((transport) => normalizeServiceTransportConfig(transport))
      .filter(
        (
          transport,
        ): transport is ServiceTransportConfig =>
          !!transport,
      )
      .map((transport) => ({
        ...transport,
        protocols:
          transport.protocols && transport.protocols.length > 0
            ? transport.protocols
            : useSocket
              ? ["rest", "socket"]
              : ["rest"],
        uuid: uuid(),
      }));
  }

  private static createBootstrapTransport(
    serviceInstanceId: string,
    role: ServiceTransportRole,
    endpoint: ReturnType<typeof resolveBootstrapEndpoint>,
  ) {
    return {
      uuid: `${serviceInstanceId}-${role}-bootstrap`,
      service_instance_id: serviceInstanceId,
      role,
      origin: endpoint.url,
      protocols: ["rest", "socket"] as const,
      security_profile: null,
      auth_strategy: null,
    };
  }

  /**
   * Validates the provided service name based on specific rules.
   *
   * @param {string} serviceName - The service name to validate. Must be less than 100 characters,
   *                                must not contain spaces, dots, or backslashes, and must start with a capital letter.
   * @return {void} Throws an error if the service name does not meet the validation criteria.
   * @throws {Error} If the service name exceeds 100 characters.
   * @throws {Error} If the service name contains spaces.
   * @throws {Error} If the service name contains dots.
   * @throws {Error} If the service name contains backslashes.
   * @throws {Error} If the service name does not start with a capital letter.
   */
  protected static validateServiceName(serviceName: string) {
    if (serviceName.length > 100) {
      throw new Error("Service name must be less than 100 characters");
    }

    if (serviceName.includes(" ")) {
      throw new Error("Service name must not contain spaces");
    }

    if (serviceName.includes(".")) {
      throw new Error("Service name must not contain dots");
    }

    if (serviceName.includes("\\")) {
      throw new Error("Service name must not contain backslashes");
    }

    if (
      serviceName.charAt(0) !== serviceName.charAt(0).toUpperCase() &&
      serviceName.charAt(0) === serviceName.charAt(0).toLowerCase()
    ) {
      throw new Error("Service name must start with a capital letter");
    }
  }

  /**
   * Validates the provided name to ensure it meets the required criteria.
   *
   * @param {string} name - The name to be validated.
   * @return {void} Does not return any value.
   */
  protected static validateName(name: string): void {
    Cadenza.validateName(name);
  }

  /**
   * Gets the current run strategy from the Cadenza configuration.
   *
   * @return {Function} The run strategy function defined in the Cadenza configuration.
   */
  public static get runStrategy(): {
    PARALLEL: unknown;
    SEQUENTIAL: unknown;
  } {
    return Cadenza.runStrategy as {
      PARALLEL: unknown;
      SEQUENTIAL: unknown;
    };
  }

  /**
   * Sets the mode for the Cadenza application.
   *
   * @param {CadenzaMode} mode - The mode to be set for the application.
   * @return {void} This method does not return a value.
   */
  public static setMode(mode: CadenzaMode) {
    Cadenza.setMode(mode);
  }

  public static hasCompletedBootstrapSync(): boolean {
    return !this.serviceCreated || this.bootstrapSyncCompleted;
  }

  public static isServiceReady(): boolean {
    return this.serviceCreated && this.bootstrapSyncCompleted;
  }

  public static getServiceReadySignalName(serviceName?: string): string {
    const resolvedServiceName =
      serviceName ??
      this.serviceRegistry?.serviceName ??
      "";

    if (!resolvedServiceName) {
      throw new Error("Cannot resolve service-ready signal without a service name");
    }

    return `${snakeCase(resolvedServiceName)}.ready`;
  }

  private static getServiceReadyHandoffSignalName(serviceName?: string): string {
    const resolvedServiceName =
      serviceName ??
      this.serviceRegistry?.serviceName ??
      "";

    if (!resolvedServiceName) {
      throw new Error(
        "Cannot resolve service-ready handoff signal without a service name",
      );
    }

    return `meta.service_registry.emit_ready_requested:${snakeCase(resolvedServiceName)}`;
  }

  public static markBootstrapSyncCompleted(): void {
    this.bootstrapSyncCompleted = true;
  }

  /**
   * Emits a signal with the specified data using the associated broker.
   *
   * @param {string} signal - The name of the event or signal to emit.
   * @param {AnyObject} [data={}] - The data to be emitted along with the signal.
   * @param options
   * @return {void} No return value.
   *
   * @example
   * This is meant to be used as a global event emitter.
   * If you want to emit an event from within a task, you can use the `emit` method provided to the task function. See {@link TaskFunction}.
   * ```ts
   * Cadenza.emit('main.my_event', { foo: 'bar' });
   * ```
   */
  static emit(signal: string, data: AnyObject = {}, options: EmitOptions = {}) {
    Cadenza.emit(signal, data, options);
  }

  static debounce(signal: string, context: any = {}, delayMs: number = 500) {
    Cadenza.debounce(signal, context, delayMs);
  }

  public static schedule(
    signal: string,
    context: AnyObject,
    timeoutMs: number,
    exactDateTime?: Date,
  ) {
    Cadenza.schedule(signal, context, timeoutMs, exactDateTime);
  }

  public static interval(
    signal: string,
    context: AnyObject,
    intervalMs: number,
    leading = false,
    startDateTime?: Date,
  ) {
    Cadenza.interval(signal, context, intervalMs, leading, startDateTime);
  }

  public static defineIntent(intent: Intent): Intent {
    this.inquiryBroker?.addIntent(intent);
    return intent;
  }

  public static getRuntimeValidationPolicy(): RuntimeValidationPolicy {
    this.bootstrap();
    return Cadenza.getRuntimeValidationPolicy();
  }

  public static setRuntimeValidationPolicy(
    policy: RuntimeValidationPolicy = {},
  ): RuntimeValidationPolicy {
    this.bootstrap();
    return Cadenza.setRuntimeValidationPolicy(policy);
  }

  public static replaceRuntimeValidationPolicy(
    policy: RuntimeValidationPolicy = {},
  ): RuntimeValidationPolicy {
    this.bootstrap();
    return Cadenza.replaceRuntimeValidationPolicy(policy);
  }

  public static clearRuntimeValidationPolicy(): void {
    this.bootstrap();
    Cadenza.clearRuntimeValidationPolicy();
  }

  public static getRuntimeValidationScopes(): RuntimeValidationScope[] {
    this.bootstrap();
    return Cadenza.getRuntimeValidationScopes();
  }

  public static upsertRuntimeValidationScope(
    scope: RuntimeValidationScope,
  ): RuntimeValidationScope {
    this.bootstrap();
    return Cadenza.upsertRuntimeValidationScope(scope);
  }

  public static removeRuntimeValidationScope(id: string): void {
    this.bootstrap();
    Cadenza.removeRuntimeValidationScope(id);
  }

  public static clearRuntimeValidationScopes(): void {
    this.bootstrap();
    Cadenza.clearRuntimeValidationScopes();
  }

  private static getInquiryResponderDescriptor(task: Task): InquiryResponderDescriptor {
    return this.serviceRegistry.getInquiryResponderDescriptor(task);
  }

  private static compareInquiryResponders(
    left: { task: Task; descriptor: InquiryResponderDescriptor },
    right: { task: Task; descriptor: InquiryResponderDescriptor },
  ) {
    return compareResponderDescriptors(left.descriptor, right.descriptor);
  }

  private static buildInquirySummary(
    inquiry: string,
    startedAt: number,
    statuses: InquiryResponderStatus[],
    totalResponders: number,
  ): DistributedInquiryMeta {
    const counts = summarizeResponderStatuses(statuses);
    const isMetaInquiry = isMetaIntentName(inquiry);
    const eligibleResponders = statuses.length;
    return {
      inquiry,
      isMetaInquiry,
      totalResponders,
      eligibleResponders,
      filteredOutResponders: Math.max(0, totalResponders - eligibleResponders),
      responded: counts.responded,
      failed: counts.failed,
      timedOut: counts.timedOut,
      pending: counts.pending,
      durationMs: Date.now() - startedAt,
      responders: statuses,
    };
  }

  private static shouldPersistInquiry(
    inquiry: string,
    _context: AnyObject,
  ): boolean {
    return !isMetaIntentName(inquiry);
  }

  private static splitInquiryPersistenceContext(
    context: AnyObject,
  ): { context: AnyObject; metadata: AnyObject } {
    const businessContext: AnyObject = {};
    const metadata: AnyObject =
      context?.__metadata && typeof context.__metadata === "object"
        ? { ...context.__metadata }
        : {};

    for (const [key, value] of Object.entries(context ?? {})) {
      if (key === "__metadata") {
        continue;
      }

      if (key.startsWith("__")) {
        metadata[key] = value;
        continue;
      }

      businessContext[key] = value;
    }

    return {
      context: businessContext,
      metadata,
    };
  }

  private static buildInquiryPersistenceStartData(
    inquiryId: string,
    inquiry: string,
    context: AnyObject,
    startedAt: number,
  ): AnyObject {
    const normalizedTaskVersion = Number(context?.__inquirySourceTaskVersion);
    const { context: inquiryContext, metadata } =
      this.splitInquiryPersistenceContext(context);

    return {
      uuid: inquiryId,
      name: inquiry,
      taskName:
        typeof context?.__inquirySourceTaskName === "string"
          ? context.__inquirySourceTaskName
          : null,
      taskVersion:
        Number.isFinite(normalizedTaskVersion) && normalizedTaskVersion > 0
          ? normalizedTaskVersion
          : null,
      taskExecutionId:
        typeof context?.__inquirySourceTaskExecutionId === "string"
          ? context.__inquirySourceTaskExecutionId
          : null,
      serviceName: this.serviceRegistry.serviceName,
      serviceInstanceId: this.serviceRegistry.serviceInstanceId,
      executionTraceId:
        typeof (context?.__metadata?.__executionTraceId ??
          context?.__executionTraceId) === "string"
          ? context.__metadata?.__executionTraceId ?? context.__executionTraceId
          : null,
      // Persist the inquiry row first, then attach routine linkage on completion.
      routineExecutionId: null,
      context: inquiryContext,
      metadata,
      isMeta: false,
      sentAt: formatTimestamp(startedAt),
    };
  }

  public static async inquire(
    inquiry: string,
    context: AnyObject,
    options: DistributedInquiryOptions = {},
  ): Promise<AnyObject> {
    this.bootstrap();

    const hydratedResult = this.consumeHydratedInquiryResult(
      options.hydrationKey,
    );
    if (hydratedResult !== undefined) {
      return hydratedResult;
    }

    const collectAllResponders = () => {
      const observer = this.inquiryBroker?.inquiryObservers.get(inquiry);
      return observer
        ? Array.from(observer.tasks).map((task) => ({
            task,
            descriptor: this.getInquiryResponderDescriptor(task),
          }))
        : [];
    };

    let allResponders = collectAllResponders();
    if (
      allResponders.length === 0 &&
      isAuthorityBootstrapIntent(inquiry) &&
      this.serviceRegistry?.ensureBootstrapAuthorityControlPlaneForInquiry?.(
        inquiry,
        context,
      )
    ) {
      allResponders = collectAllResponders();
    }
    const isMetaInquiry = isMetaIntentName(inquiry);
    const startedAt = Date.now();
    const persistInquiry = this.shouldPersistInquiry(inquiry, context);
    const logicalInquiryId = persistInquiry ? uuid() : null;
    const inquiryStartData = logicalInquiryId
      ? this.buildInquiryPersistenceStartData(
          logicalInquiryId,
          inquiry,
          context,
          startedAt,
        )
      : null;

    if (inquiryStartData) {
      this.emit("meta.inquiry_broker.inquiry_started", {
        data: inquiryStartData,
      });
    }

    const responders = allResponders.filter(({ task, descriptor }) => {
      const shouldExecute = shouldExecuteInquiryResponder(inquiry, task.isMeta);

      if (shouldExecute) {
        return true;
      }

      const warningKey = `${inquiry}|${descriptor.serviceName}|${descriptor.taskName}|${descriptor.taskVersion}|${descriptor.localTaskName}`;
      if (!this.warnedInvalidMetaIntentResponderKeys.has(warningKey)) {
        this.warnedInvalidMetaIntentResponderKeys.add(warningKey);
        this.log(
          "Skipping non-meta task for meta intent inquiry.",
          {
            inquiry,
            responder: descriptor,
          },
          "warning",
          descriptor.serviceName,
        );
      }

      return false;
    });

    if (responders.length === 0) {
      const inquiryMeta = {
        inquiry,
        isMetaInquiry,
        totalResponders: allResponders.length,
        eligibleResponders: 0,
        filteredOutResponders: allResponders.length,
        responded: 0,
        failed: 0,
        timedOut: 0,
        pending: 0,
        durationMs: 0,
        responders: [],
      } as DistributedInquiryMeta;

      if (logicalInquiryId) {
        this.emit("meta.inquiry_broker.inquiry_completed", {
          insertData: inquiryStartData,
          data: {
            fulfilledAt: formatTimestamp(startedAt),
            duration: 0,
            metadata: {
              ...(inquiryStartData?.metadata ?? {}),
              inquiryMeta,
            },
          },
          filter: {
            uuid: logicalInquiryId,
          },
        });
      }

      if (options.requireComplete) {
        throw {
          __inquiryMeta: inquiryMeta,
          __error: `Inquiry '${inquiry}' had no eligible responders`,
          errored: true,
        };
      }

      return {
        __inquiryMeta: inquiryMeta,
      };
    }

    responders.sort(this.compareInquiryResponders.bind(this));

    const overallTimeoutMs = options.overallTimeoutMs ?? options.timeout ?? 0;
    const requireComplete = options.requireComplete ?? false;
    const perResponderTimeoutMs = options.perResponderTimeoutMs;
    const statuses: InquiryResponderStatus[] = [];
    const statusByTask = new Map<Task, InquiryResponderStatus>();
    for (const responder of responders) {
      const status: InquiryResponderStatus = {
        ...responder.descriptor,
        status: "timed_out",
        durationMs: 0,
      };
      statuses.push(status);
      statusByTask.set(responder.task, status);
    }

    const resultsByTask = new Map<Task, AnyObject>();
    const failedResultsByTask = new Map<Task, AnyObject>();
    const resolverTasks: Task[] = [];
    const pending = new Set(responders.map((r) => r.task));
    const startTimeByTask = new Map<Task, number>();

    this.emit("meta.inquiry_broker.inquire", { inquiry, context });

    return new Promise((resolve, reject) => {
      let finalized = false;
      let timeoutId: NodeJS.Timeout | undefined;

      const finalize = (timedOut: boolean) => {
        if (finalized) return;
        finalized = true;

        if (timeoutId) {
          clearTimeout(timeoutId);
          timeoutId = undefined;
        }

        for (const resolverTask of resolverTasks) {
          resolverTask.destroy();
        }

        if (timedOut && pending.size > 0) {
          for (const task of pending) {
            const status = statusByTask.get(task);
            if (!status) continue;
            status.status = "timed_out";
            status.durationMs = Date.now() - (startTimeByTask.get(task) ?? startedAt);
          }
        }

        const fulfilledContexts = responders
          .filter((responder) => resultsByTask.has(responder.task))
          .map((responder) => resultsByTask.get(responder.task)!);

        const mergedContext = mergeInquiryContexts(fulfilledContexts);
        const inquiryMeta = this.buildInquirySummary(
          inquiry,
          startedAt,
          statuses,
          allResponders.length,
        );
        const finishedAt = Date.now();
        const responseContext = {
          ...mergedContext,
          __inquiryMeta: inquiryMeta,
        };

        if (logicalInquiryId) {
          this.emit("meta.inquiry_broker.inquiry_completed", {
            insertData: inquiryStartData,
            data: {
              fulfilledAt: formatTimestamp(finishedAt),
              duration: finishedAt - startedAt,
              routineExecutionId:
                typeof inquiryStartData?.metadata?.__inquirySourceRoutineExecutionId ===
                "string"
                  ? inquiryStartData.metadata.__inquirySourceRoutineExecutionId
                  : null,
              metadata: {
                ...(inquiryStartData?.metadata ?? {}),
                inquiryMeta,
              },
            },
            filter: {
              uuid: logicalInquiryId,
            },
          });
        }

        if (
          requireComplete &&
          (timedOut ||
            inquiryMeta.failed > 0 ||
            inquiryMeta.timedOut > 0 ||
            inquiryMeta.pending > 0)
        ) {
          const failedResponderResult = responders
            .map((responder) => failedResultsByTask.get(responder.task))
            .find((result): result is AnyObject => Boolean(result));
          const failedResponderError = statuses.find(
            (status) =>
              status.status === "failed" &&
              typeof status.error === "string" &&
              status.error.trim().length > 0,
          )?.error;

          reject({
            ...responseContext,
            __error:
              (failedResponderResult
                ? resolveInquiryFailureError(inquiry, failedResponderResult)
                : undefined) ??
              failedResponderError ??
              resolveInquiryFailureError(inquiry, responseContext),
            errored: true,
          });
          return;
        }

        resolve(responseContext);
      };

      if (overallTimeoutMs > 0) {
        timeoutId = setTimeout(() => finalize(true), overallTimeoutMs);
      }

      for (const responder of responders) {
        const { task, descriptor } = responder;
        const responderInquiryId = uuid();
        startTimeByTask.set(task, Date.now());

        const resolverTask = this.createEphemeralMetaTask(
          `Resolve inquiry ${inquiry} for ${descriptor.localTaskName}`,
          (resultCtx) => {
            if (finalized) {
              return;
            }

            pending.delete(task);

            const status = statusByTask.get(task);

            if (status) {
              status.durationMs =
                Date.now() -
                (startTimeByTask.get(task) ?? startedAt);

              if (resultCtx?.errored || resultCtx?.failed) {
                status.status = "failed";
                status.error = String(
                  resultCtx?.__error ?? resultCtx?.error ?? "Inquiry responder failed",
                );
                failedResultsByTask.set(task, resultCtx);
              } else {
                status.status = "fulfilled";
                resultsByTask.set(task, resultCtx);
              }
            }

            if (pending.size === 0) {
              finalize(false);
            }
          },
          "Resolves distributed inquiry responder result",
          { register: false },
        ).doOn(`meta.node.graph_completed:${responderInquiryId}`);

        resolverTasks.push(resolverTask);

        const executionContext: AnyObject = {
          ...context,
          ...(logicalInquiryId ? { __inquiryId: logicalInquiryId } : {}),
          __routineExecId: responderInquiryId,
          __isInquiry: true,
        };

        if (perResponderTimeoutMs !== undefined) {
          executionContext.__timeout = perResponderTimeoutMs;
        }

        if (task.isMeta) {
          this.metaRunner?.run(task, executionContext);
        } else {
          this.runner?.run(task, executionContext);
        }
      }
    });
  }

  /**
   * Executes the given task or graph routine within the provided context using the configured runner.
   *
   * @param {Task | GraphRoutine} task - The task or graph routine to be executed.
   * @param {AnyObject} context - The context within which the task will be executed.
   * @return {void}
   *
   * @example
   * ```ts
   * const task = Cadenza.createTask('My task', (ctx) => {
   *   console.log('My task executed with context:', ctx);
   * });
   *
   * Cadenza.run(task, { foo: 'bar' });
   *
   * const routine = Cadenza.createRoutine('My routine', [task], 'My routine description');
   *
   * Cadenza.run(routine, { foo: 'bar' });
   * ```
   */
  static run(task: Task | GraphRoutine, context: AnyObject) {
    this.runner?.run(task, context);
  }
  /**
   * Logs a message with a specified log level and additional contextual data.
   * Records in the CadenzaDB when available.
   *
   * @param {string} message - The main message to be logged.
   * @param {any} [data={}] - Additional data or metadata to include with the log.
   * @param {"info"|"warning"|"error"|"critical"} [level="info"] - The severity level of the log message.
   * @param {string|null} [subjectServiceName=null] - The name of the subject service related to the log.
   * @param {string|null} [subjectServiceInstanceId=null] - The instance ID of the subject service related to the log.
   * @return {void} No return value.
   */
  static log(
    message: string,
    data: any = {},
    level: "info" | "warning" | "error" | "critical" = "info",
    subjectServiceName: string | null = null,
    subjectServiceInstanceId: string | null = null,
  ) {
    if (level === "critical") {
      console.error("CRITICAL:", message, data);
    } else if (level === "error") {
      console.error(message, data);
    } else if (level === "warning") {
      console.warn(message, data);
    } else {
      console.log(message, data);
    }

    this.emit("global.meta.system_log.log", {
      data: {
        data,
        level,
        message,
        serviceName: this.serviceRegistry?.serviceName,
        serviceInstanceId: this.serviceRegistry?.serviceInstanceId,
        subjectServiceName,
        subjectServiceInstanceId,
        created: formatTimestamp(Date.now()),
      },
    });
  }

  public static get(taskName: string): Task | undefined {
    return Cadenza.get(taskName);
  }

  public static getLocalCadenzaDBTask(
    tableName: string,
    operation: DbOperationType,
  ): Task | undefined {
    const generatedTaskName = this.buildGeneratedLocalCadenzaDBTaskName(
      tableName,
      operation,
    );
    const legacyTaskName = this.buildLegacyLocalCadenzaDBTaskName(
      tableName,
      operation,
    );

    return Cadenza.get(generatedTaskName) ?? Cadenza.get(legacyTaskName);
  }

  public static getLocalCadenzaDBInsertTask(
    tableName: string,
  ): Task | undefined {
    return this.getLocalCadenzaDBTask(tableName, "insert");
  }

  public static getLocalCadenzaDBQueryTask(
    tableName: string,
  ): Task | undefined {
    return this.getLocalCadenzaDBTask(tableName, "query");
  }

  public static getActor<
    D extends Record<string, any> = AnyObject,
    R = AnyObject,
  >(actorName: string): Actor<D, R> | undefined {
    const cadenzaWithActors = Cadenza as unknown as {
      getActor?: <D extends Record<string, any>, R = AnyObject>(
        actorName: string,
      ) => Actor<D, R> | undefined;
    };
    return cadenzaWithActors.getActor?.<D, R>(actorName);
  }

  public static getAllActors<
    D extends Record<string, any> = AnyObject,
    R = AnyObject,
  >(): Actor<D, R>[] {
    const cadenzaWithActors = Cadenza as unknown as {
      getAllActors?: <D extends Record<string, any>, R = AnyObject>() => Actor<
        D,
        R
      >[];
    };
    return cadenzaWithActors.getAllActors?.<D, R>() ?? [];
  }

  public static getRoutine(routineName: string): GraphRoutine | undefined {
    return Cadenza.getRoutine(routineName);
  }

  /**
   * Creates a new DeputyTask instance based on the provided routine name, service name, and options.
   * This method ensures proper task initialization, including setting a unique name,
   * validation of the routine name, and applying default option values.
   *
   * @param {string} routineName - The name of the routine the task references. This is mandatory and should be a valid string.
   * @param {string|undefined} [serviceName] - The name of the service that the routine belongs to. This is optional and defaults to undefined.
   * @param {TaskOptions} [options={}] - A configuration object for the task, allowing various properties such as concurrency, timeout, and retry settings to be customized.
   * @return {DeputyTask} - A new DeputyTask instance initialized with the specified parameters.
   *
   * @example
   * Let's say we are writing the code for a Service called "Service1".
   * We also have an additional service called "Service2" with a routine called "My Routine".
   * A flow on Service1 depends on the result of "My Routine" on Service2.
   * We can create a deputy task for the routine using the following code:
   * ```ts
   * Cadenza.createDeputyTask("My Routine", "Service2").then(
   *   Cadenza.createTask("Handle result", (ctx) => {
   *     console.log("'Handle result' executed with context:", ctx);
   *   }),
   * );
   * ```
   * Internally, this will send a request to an available "Service2" instance to execute the "My Routine" routine.
   * The deputy task will wait for the response and then execute the next task(s) in the chain.
   *
   * You can visualize the execution of the deputy task as follows:
   * ```
   * Service1 flow = [Deputy tasks for "My Routine"] -> ["Handle result"]
   *                       ||       A
   *                       V       ||
   * Service2 flow =    [[My Routine]]
   * ```
   *
   * Deputy tasks are useful for delegating flows to other services, allowing for parallel execution and load balancing.
   * But it creates tight coupling between the services, which may not be desirable in some cases.
   * In cases where an event on one service should simply trigger a flow on another service, without the need for a result,
   * it is recommended to use signals instead. Like this:
   *
   * Service1
   * ```ts
   * Cadenza.createTask("Generate event", (ctx, emit) => {
   *   // Do something
   *   emit("some.event");
   * });
   * ```
   *
   * Service2
   * ```ts
   * Cadenza.createTask("Handle event", (ctx) => {
   *   console.log("Handle event executed with context:", ctx);
   * }).doOn("Service1.some.event");
   * ```
   *
   * Every time the "Generate event" task is executed, it will emit a signal "Service1.some.event" to one Service2 instance and trigger the "Handle event" task.
   */
  static createDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    options: TaskOptions = {},
  ): DeputyTask {
    this.bootstrap();
    this.validateName(routineName);
    const name = `${routineName} (Proxy)`;

    options = {
      concurrency: DEFAULT_DEPUTY_TASK_CONCURRENCY,
      timeout: DEFAULT_DEPUTY_TASK_TIMEOUT_MS,
      register: true,
      isUnique: false,
      isMeta: false,
      isSubMeta: false,
      isHidden: false,
      getTagCallback: undefined,
      inputSchema: undefined,
      validateInputContext: false,
      outputSchema: undefined,
      validateOutputContext: false,
      retryCount: 0,
      retryDelay: 0,
      retryDelayMax: 0,
      retryDelayFactor: 1,
      ...options,
    };

    return new DeputyTask(
      name,
      routineName,
      serviceName,
      `Referencing routine in service: ${routineName} on service: ${serviceName}.`,
      options.concurrency,
      options.timeout,
      options.register,
      options.isUnique,
      options.isMeta,
      options.isSubMeta,
      options.isHidden,
      options.getTagCallback,
      options.inputSchema,
      options.validateInputContext,
      options.outputSchema,
      options.validateOutputContext,
      options.retryCount,
      options.retryDelay,
      options.retryDelayMax,
      options.retryDelayFactor,
    );
  }

  /**
   * Creates a meta deputy task by setting the `isMeta` property in the options to true,
   * and delegating task creation to the `createDeputyTask` method.
   * See {@link createDeputyTask} and {@link createMetaTask} for more information.
   *
   * @param {string} routineName - The name of the routine associated with the task.
   * @param {string | undefined} [serviceName] - The optional name of the service associated with the task.
   * @param {TaskOptions} [options={}] - Additional options for the task. Defaults to an empty object if not provided.
   * @return {DeputyTask} - The created meta deputy task.
   */
  static createMetaDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    options: TaskOptions = {},
  ): DeputyTask {
    options.isMeta = true;
    return this.createDeputyTask(routineName, serviceName, options);
  }

  /**
   * Creates a throttled deputy task with the specified parameters.
   * See {@link createThrottledTask} and {@link createDeputyTask} for more information.
   *
   * @param {string} routineName - The name of the routine to be executed.
   * @param {string | undefined} [serviceName=undefined] - The name of the service, if applicable.
   * @param {ThrottleTagGetter} [throttledIdGetter=() => "default"] - A function to get the throttled tag for the task.
   * @param {TaskOptions} [options={}] - The options for task configuration, including concurrency and callbacks.
   * @return {DeputyTask} The created throttled deputy task.
   */
  static createThrottledDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    throttledIdGetter: ThrottleTagGetter = () => "default",
    options: TaskOptions = {},
  ) {
    options.concurrency = 1;
    options.getTagCallback = throttledIdGetter;
    return this.createDeputyTask(routineName, serviceName, options);
  }

  /**
   * Creates a throttled deputy task with meta-task settings enabled.
   * See {@link createThrottledTask},{@link createDeputyTask} and {@link createMetaTask} for more information.
   *
   * @param {string} routineName - The name of the routine for which the task is being created.
   * @param {string|undefined} [serviceName=undefined] - The name of the service associated with the task, or undefined if not applicable.
   * @param {ThrottleTagGetter} [throttledIdGetter=() => "default"] - A function to compute or return the throttling identifier.
   * @param {TaskOptions} [options={}] - Additional options for the task configuration.
   * @return {any} Returns the created throttled deputy task instance.
   */
  static createMetaThrottledDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    throttledIdGetter: ThrottleTagGetter = () => "default",
    options: TaskOptions = {},
  ) {
    options.isMeta = true;
    return this.createThrottledDeputyTask(
      routineName,
      serviceName,
      throttledIdGetter,
      options,
    );
  }

  /**
   * Creates and configures a signal transmission task that handles the transmission
   * of a specified signal to a target service with a set of customizable options.
   * This is only used for internal purposes and is not exposed to the business logic layer.
   *
   * @param {string} signalName - The name of the signal to be transmitted.
   * @param {string} serviceName - The name of the target service to transmit the signal to.
   * @param {TaskOptions} [options={}] - A set of optional parameters to further configure the task.
   * @return {SignalTransmissionTask} A new instance of SignalTransmissionTask configured with the given parameters.
   */
  static createSignalTransmissionTask(
    signalName: string,
    serviceName: string,
    options: TaskOptions = {},
  ): SignalTransmissionTask | undefined {
    this.bootstrap();
    this.validateName(signalName);
    this.validateName(serviceName);

    const name = `Transmit signal: ${signalName} to ${serviceName}`;
    if (this.get(name)) {
      return;
    }

    options = {
      concurrency: DEFAULT_DATABASE_PROXY_TASK_CONCURRENCY,
      timeout: DEFAULT_DATABASE_PROXY_TASK_TIMEOUT_MS,
      register: true,
      isUnique: false,
      isMeta: true,
      isSubMeta: false,
      isHidden: false,
      getTagCallback: undefined,
      inputSchema: undefined,
      validateInputContext: false,
      outputSchema: undefined,
      validateOutputContext: false,
      retryCount: 1,
      retryDelay: 0,
      retryDelayMax: 0,
      retryDelayFactor: 1,
      ...options,
    };

    options.isMeta = true;

    return new SignalTransmissionTask(
      name,
      signalName,
      serviceName,
      `Transmits signal ${signalName} to ${serviceName} service.`,
      options.concurrency,
      options.timeout,
      options.register,
      options.isUnique,
      options.isMeta,
      options.isSubMeta,
      options.isHidden,
      options.getTagCallback,
      options.inputSchema,
      options.validateInputContext,
      options.outputSchema,
      options.validateOutputContext,
      options.retryCount,
      options.retryDelay,
      options.retryDelayMax,
      options.retryDelayFactor,
    );
  }

  /**
   * Creates and configures a database task that performs an operation on a specified table.
   *
   * @param {string} tableName - The name of the database table on which the operation will be performed.
   * @param {DbOperationType} operation - The type of database operation to execute (e.g., insert, update, delete).
   * @param {string|undefined} [databaseServiceName=undefined] - The name of the database service; defaults to "default database service" if not provided.
   * @param {DbOperationPayload} queryData - The data payload required for executing the specified database operation.
   * @param {TaskOptions} [options={}] - Optional configuration for the task, including concurrency, timeout, and retry policies.
   * @return {DatabaseTask} A configured database task instance ready for execution.
   */
  static createDatabaseTask(
    tableName: string,
    operation: DbOperationType,
    databaseServiceName: string | undefined = undefined,
    queryData: DbOperationPayload,
    options: TaskOptions = {},
  ) {
    this.bootstrap();
    this.validateName(tableName);
    this.validateName(operation);
    const resolvedDatabaseServiceName =
      databaseServiceName ?? this.defaultDatabaseServiceName ?? undefined;
    const targetDatabaseServiceName =
      resolvedDatabaseServiceName ?? "default database service";
    const name = `${operation.charAt(0).toUpperCase() + operation.slice(1)} ${tableName} in ${targetDatabaseServiceName}`;
    const description = `Executes a ${operation} on table ${tableName} in ${targetDatabaseServiceName}`;
    const taskName = `${operation.charAt(0).toUpperCase() + operation.slice(1)} ${tableName}`;

    options = {
      concurrency: DEFAULT_DATABASE_PROXY_TASK_CONCURRENCY,
      timeout: DEFAULT_DATABASE_PROXY_TASK_TIMEOUT_MS,
      register: true,
      isUnique: false,
      isMeta: false,
      isSubMeta: false,
      isHidden: false,
      getTagCallback: undefined,
      inputSchema: undefined,
      validateInputContext: false,
      outputSchema: undefined,
      validateOutputContext: false,
      retryCount: 3,
      retryDelay: 100,
      retryDelayMax: 0,
      retryDelayFactor: 1,
      ...options,
    };

    return new DatabaseTask(
      name,
      taskName,
      resolvedDatabaseServiceName,
      description,
      queryData,
      options.concurrency,
      options.timeout,
      options.register,
      options.isUnique,
      options.isMeta,
      options.isSubMeta,
      options.isHidden,
      options.getTagCallback,
      options.inputSchema,
      options.validateInputContext,
      options.outputSchema,
      options.validateOutputContext,
      options.retryCount,
      options.retryDelay,
      options.retryDelayMax,
      options.retryDelayFactor,
    );
  }

  /**
   * Creates a task for performing a database insert operation.
   *
   * @param {string} tableName - The name of the table where the insert operation will be performed.
   * @param {string | undefined} [databaseServiceName=undefined] - The name of the database service to use. Optional parameter, defaults to undefined.
   * @param {DbOperationPayload} [queryData={}] - The data payload for the insert operation. Defaults to an empty object.
   * @param {TaskOptions} [options={}] - Additional task options to configure the insert operation. Defaults to an empty object.
   * @return {object} A task configuration object for the database insert operation.
   */
  static createDatabaseInsertTask(
    tableName: string,
    databaseServiceName: string | undefined = undefined,
    queryData: DbOperationPayload = {},
    options: TaskOptions = {},
  ) {
    return this.createDatabaseTask(
      tableName,
      "insert",
      databaseServiceName,
      queryData,
      options,
    );
  }

  /**
   * Creates a database query task for the specified table and configuration.
   *
   * @param {string} tableName - The name of the database table to execute the query on.
   * @param {string | undefined} [databaseServiceName=undefined] - The name of the database service to use. If undefined, the default service will be used.
   * @param {DbOperationPayload} queryData - The payload containing the query data to be executed.
   * @param {TaskOptions} [options={}] - Optional parameters to configure the task execution.
   * @return {Task} The created database query task.
   */
  static createDatabaseQueryTask(
    tableName: string,
    databaseServiceName: string | undefined = undefined,
    queryData: DbOperationPayload,
    options: TaskOptions = {},
  ) {
    return this.createDatabaseTask(
      tableName,
      "query",
      databaseServiceName,
      queryData,
      options,
    );
  }

  /**
   * Creates a database task for the CadenzaDB with the specified parameters.
   *
   * @param {string} tableName - The name of the database table on which the operation will be performed.
   * @param {DbOperationType} operation - The type of database operation to execute (e.g., INSERT, UPDATE, DELETE).
   * @param {DbOperationPayload} queryData - The payload or data required to perform the database operation.
   * @param {TaskOptions} [options={}] - Additional options for the task, such as configuration settings.
   * @return {any} The result of creating the database task.
   */
  static createCadenzaDBTask(
    tableName: string,
    operation: DbOperationType,
    queryData: DbOperationPayload,
    options: TaskOptions = {},
  ) {
    options.isMeta = true;
    return this.createDatabaseTask(
      tableName,
      operation,
      "CadenzaDB",
      queryData,
      options,
    );
  }

  /**
   * Creates a database insert task specifically for the CadenzaDB database.
   *
   * @param {string} tableName - The name of the table into which the data will be inserted.
   * @param {DbOperationPayload} [queryData={}] - An object representing the data to be inserted.
   * @param {TaskOptions} [options={}] - Additional options to customize the task. The `isMeta` property is set to true by default.
   * @return {Task} A task object configured to perform an insert operation in the CadenzaDB database.
   */
  static createCadenzaDBInsertTask(
    tableName: string,
    queryData: DbOperationPayload = {},
    options: TaskOptions = {},
  ) {
    options.isMeta = true;
    return this.createDatabaseInsertTask(
      tableName,
      "CadenzaDB",
      queryData,
      options,
    );
  }

  /**
   * Creates a database query task specifically for the CadenzaDB.
   *
   * @param {string} tableName - The name of the database table to execute the query on.
   * @param {DbOperationPayload} queryData - The payload containing data and parameters for the database operation.
   * @param {TaskOptions} [options={}] - Additional options for the task configuration.
   * @return {any} The created task for executing a database query.
   */
  static createCadenzaDBQueryTask(
    tableName: string,
    queryData: DbOperationPayload,
    options: TaskOptions = {},
  ) {
    options.isMeta = true;
    return this.createDatabaseQueryTask(
      tableName,
      "CadenzaDB",
      queryData,
      options,
    );
  }

  /**
   * Creates a new Cadenza service with the specified configuration.
   *
   * @param {string} serviceName - The unique name of the service to create.
   * @param {string} [description] - An optional description of the service.
   * @param {ServerOptions} [options] - An optional object containing configuration options for the service.
   * @return {boolean} Returns true when the service is successfully created.
   */
  static createCadenzaService(
    serviceName: string,
    description: string = "",
    options: ServerOptions = {},
  ) {
    if (this.serviceCreated) return;
    this.bootstrap();
    this.validateName(serviceName);
    this.validateServiceName(serviceName);

    const serviceId = options.customServiceId ?? uuid();
    this.bootstrapSyncCompleted = false;
    this.bootstrapSignalRegistrationsCompleted = false;
    this.bootstrapIntentRegistrationsCompleted = false;
    this.serviceRegistry.serviceName = serviceName;
    this.serviceRegistry.serviceInstanceId = serviceId;
    this.serviceRegistry.connectsToCadenzaDB = !!options.cadenzaDB?.connect;
    this.setHydrationResults(options.hydration);
    this.registerGracefulShutdownHandlers();

    const explicitFrontendMode = options.isFrontend;

    options = {
      loadBalance: true,
      useSocket: true,
      displayName: undefined,
      isMeta: false,
      port: readIntegerEnv("HTTP_PORT", 3000),
      securityProfile:
        (readStringEnv("SECURITY_PROFILE") as SecurityProfile) ?? "medium",
      networkMode: (readStringEnv("NETWORK_MODE") as NetworkMode) ?? "dev",
      retryCount: 3,
      cadenzaDB: {
        connect: true,
      },
      relatedServices: readListEnv("RELATED_SERVICES"),
      isFrontend:
        typeof explicitFrontendMode === "boolean"
          ? explicitFrontendMode
          : isBrowser,
      ...options,
    };

    const isFrontend = !!options.isFrontend;
    const declaredTransports = this.normalizeDeclaredTransports(
      options.transports,
      serviceId,
      !!options.useSocket,
    );
    this.serviceRegistry.seedLocalInstance(
      {
        uuid: serviceId,
        service_name: serviceName,
        number_of_running_graphs: 0,
        is_primary: false,
        is_active: true,
        is_non_responsive: false,
        is_blocked: false,
        health: {},
        is_frontend: isFrontend,
        is_database: !!options.isDatabase,
        transports: declaredTransports.map((transport) => ({
          uuid: transport.uuid,
          service_instance_id: serviceId,
          role: transport.role,
          origin: transport.origin,
          protocols: transport.protocols ?? ["rest", "socket"],
          security_profile: transport.securityProfile ?? null,
          auth_strategy: transport.authStrategy ?? null,
        })),
      },
      {
        // Local startup truth should be available before the authority echo path.
        markTransportsReady: true,
      },
    );
    this.serviceRegistry.isFrontend = isFrontend;
    this.serviceRegistry.useSocket = !!options.useSocket;
    this.serviceRegistry.retryCount = options.retryCount ?? 3;
    this.ensureTransportControllers(isFrontend);
    this.ensureServiceManifestPublicationTasks();

    if (serviceName === "CadenzaDB") {
      this.serviceRegistry.ensureAuthorityFullSyncResponderTask();
      this.serviceRegistry.ensureAuthorityServiceCommunicationPersistenceTask();
    }

    const finalizeBootstrapReadinessTask = this.createMetaTask(
      "Initialize graph metadata controller after sync registrations",
      () => {
        if (
          this.bootstrapSyncCompleted ||
          !this.bootstrapSignalRegistrationsCompleted ||
          !this.bootstrapIntentRegistrationsCompleted
        ) {
          return false;
        }

        this.markBootstrapSyncCompleted();

        if (!isFrontend) {
          GraphMetadataController.instance;
          this.replayRegisteredTaskGraphMetadata();
          this.serviceRegistry.reconcileKnownGlobalSignalRegistrations();
        }

        this.emit(this.getServiceReadyHandoffSignalName(serviceName), {
          serviceName,
          serviceInstanceId: serviceId,
          ready: true,
        });

        return true;
      },
      "Marks the local service ready after bootstrap sync and emits a local ready signal for business flows.",
      {
        register: false,
        isHidden: true,
      },
    );

    this.createMetaTask(
      "Emit service ready signal",
      (ctx) => {
        // Service readiness is a service-level lifecycle event, so emit it on a
        // fresh trace instead of reusing the bootstrap handoff routine trace.
        this.emit(this.getServiceReadySignalName(serviceName), {
          serviceName:
            typeof ctx.serviceName === "string" ? ctx.serviceName : serviceName,
          serviceInstanceId:
            typeof ctx.serviceInstanceId === "string"
              ? ctx.serviceInstanceId
              : serviceId,
          ready: true,
        });

        return true;
      },
      "Emits the local business-facing ready signal after bootstrap observability is available.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn(this.getServiceReadyHandoffSignalName(serviceName));

    this.createMetaTask(
      "Mark bootstrap global signal registration complete",
      () => {
        this.bootstrapSignalRegistrationsCompleted = true;
        return true;
      },
      "Marks the bootstrap global signal registration branch as complete.",
      {
        register: false,
        isHidden: true,
      },
    )
      .doOn("meta.service_registry.registered_global_signals")
      .then(finalizeBootstrapReadinessTask);

    this.createMetaTask(
      "Mark bootstrap global intent registration complete",
      () => {
        this.bootstrapIntentRegistrationsCompleted = true;
        return true;
      },
      "Marks the bootstrap global intent registration branch as complete.",
      {
        register: false,
        isHidden: true,
      },
    )
      .doOn("meta.service_registry.registered_global_intents")
      .then(finalizeBootstrapReadinessTask);

    if (!isFrontend) {
      GraphSyncController.instance.isCadenzaDBReady =
        serviceName === "CadenzaDB";
      GraphSyncController.instance.init();
    }

    const resolvedBootstrapEndpoint = options.cadenzaDB?.connect
      ? resolveBootstrapEndpoint({
          runtime: isFrontend ? "browser" : "server",
          bootstrap: options.bootstrap,
          cadenzaDB: options.cadenzaDB,
        })
      : undefined;

    if (resolvedBootstrapEndpoint) {
      this.serviceRegistry.seedAuthorityBootstrapRoute(
        resolvedBootstrapEndpoint.url,
        isFrontend ? "public" : "internal",
      );
      options.cadenzaDB = {
        ...options.cadenzaDB,
        connect: true,
        address: resolvedBootstrapEndpoint.address,
        port: resolvedBootstrapEndpoint.port,
      };
    }

    if (options.cadenzaDB?.connect) {
      this.emit("meta.initializing_service", {
        // Seed the CadenzaDB
        serviceInstance: {
          uuid: "cadenza-db",
          serviceName: "CadenzaDB",
          numberOfRunningGraphs: 0,
          isActive: true, // Assume it is deployed
          isNonResponsive: false,
          isBlocked: false,
          health: {},
          isFrontend: false,
          isBootstrapPlaceholder: true,
          transports: resolvedBootstrapEndpoint
            ? [
                this.createBootstrapTransport(
                  "cadenza-db",
                  isFrontend ? "public" : "internal",
                  resolvedBootstrapEndpoint,
                ),
              ]
            : [],
        },
      });
    }

    options.relatedServices?.forEach((service) => {
      const relatedTransport = normalizeServiceTransportConfig({
        role: isFrontend ? "public" : "internal",
        origin: service[2].includes("://") ? service[2] : `http://${service[2]}`,
        protocols: options.useSocket ? ["rest", "socket"] : ["rest"],
      });
      this.emit("meta.initializing_service", {
        serviceInstance: {
          uuid: service[0],
          serviceName: service[1],
          numberOfRunningGraphs: 0,
          isActive: true, // Assume it is deployed
          isNonResponsive: false,
          isBlocked: false,
          health: {},
          isFrontend: false,
          isBootstrapPlaceholder: true,
          transports: relatedTransport
            ? [
                {
                  uuid: `${service[0]}-${relatedTransport.role}`,
                  service_instance_id: service[0],
                  role: relatedTransport.role,
                  origin: relatedTransport.origin,
                  protocols: relatedTransport.protocols ?? ["rest", "socket"],
                  security_profile: relatedTransport.securityProfile ?? null,
                  auth_strategy: relatedTransport.authStrategy ?? null,
                },
              ]
            : [],
        },
      });
    });

    const initContext = {
      data: {
        name: serviceName,
        description: description,
        display_name: options.displayName ?? "",
        is_meta: options.isMeta,
      },
      __registrationData: {
        name: serviceName,
        description: description,
        display_name: options.displayName ?? "",
        is_meta: options.isMeta,
      },
      __serviceName: serviceName,
      __serviceInstanceId: serviceId,
      __port: options.port,
      __loadBalance: options.loadBalance,
      __useSocket: options.useSocket,
      __securityProfile: options.securityProfile,
      __networkMode: options.networkMode,
      __retryCount: options.retryCount,
      __cadenzaDBConnect: options.cadenzaDB?.connect,
      __isDatabase: options.isDatabase,
      __isFrontend: isFrontend,
      __declaredTransports: declaredTransports,
    };

    let bootstrapServiceCreationRequested = false;

    if (options.cadenzaDB?.connect) {
      this.createMetaTask(
        "Create service",
        async (context, emit) => {
          const handshakeServiceName = String(context?.serviceName ?? "").trim();
          if (handshakeServiceName !== "CadenzaDB") {
            return false;
          }

          if (bootstrapServiceCreationRequested) {
            return false;
          }

          bootstrapServiceCreationRequested = true;
          emit("meta.create_service_requested", initContext);
          return true;
        },
        "Requests local service creation only once after the initial authority bootstrap handshake completes.",
        {
          register: false,
          isHidden: true,
        },
      ).doOn("meta.fetch.handshake_complete");
    } else {
      this.emit("meta.create_service_requested", initContext);
      this.createMetaTask("Create signal transmission for sync", (ctx, emit) => {
        emit(
          "meta.service_registry.gathered_sync_transmission_reconcile_requested",
          {
            serviceName: ctx.serviceName,
            __reason: "handshake",
          },
        );
      }).doOn("meta.rest.handshake", "meta.socket.handshake");
    }

    let serviceSetupCompletedHandled = false;

    this.createMetaTask("Handle service setup completion", (ctx, emit) => {
      if (serviceSetupCompletedHandled) {
        return false;
      }

      const insertedServiceInstanceId = String(
        ctx?.serviceInstanceId ??
          ctx?.service_instance_id ??
          ctx?.serviceInstance?.uuid ??
          ctx?.serviceInstance?.serviceInstanceId ??
          "",
      ).trim();
      const insertedServiceName = String(
        ctx?.serviceName ??
          ctx?.service_name ??
          ctx?.serviceInstance?.serviceName ??
          ctx?.serviceInstance?.service_name ??
          "",
      ).trim();

      if (!insertedServiceInstanceId && !insertedServiceName) {
        return false;
      }

      if (insertedServiceInstanceId && insertedServiceInstanceId !== serviceId) {
        return false;
      }

      if (
        !insertedServiceInstanceId &&
        insertedServiceName &&
        insertedServiceName !== serviceName
      ) {
        return false;
      }

      serviceSetupCompletedHandled = true;

      if (options.cadenzaDB?.connect) {
        this.serviceRegistry.bootstrapFullSync(emit, ctx, "service_setup_completed");
        void this.publishServiceManifestIfNeeded(
          "service_setup_completed",
          "business_structural",
        );
      }

      if (isFrontend) {
        registerActorSessionPersistenceTasks();
        this.ensureFrontendSyncLoop();
      }

      this.log("Service created.");

      return true;
    }).doOn("meta.service_registry.instance_inserted");

    if (!options.cadenzaDB?.connect) {
      Cadenza.schedule(
        "meta.service_registry.instance_registration_requested",
        {
          data: {
            uuid: serviceId,
            process_pid: isFrontend ? 1 : process.pid,
            service_name: serviceName,
            is_frontend: isFrontend,
            is_database: !!options.isDatabase,
            is_active: true,
            is_non_responsive: false,
            is_blocked: false,
            health: {},
          },
          __registrationData: {
            uuid: serviceId,
            process_pid: isFrontend ? 1 : process.pid,
            service_name: serviceName,
            is_frontend: isFrontend,
            is_database: !!options.isDatabase,
            is_active: true,
            is_non_responsive: false,
            is_blocked: false,
            health: {},
          },
          __transportData: declaredTransports.map((transport) => ({
            uuid: transport.uuid,
            service_instance_id: serviceId,
            role: transport.role,
            origin: transport.origin,
            protocols: transport.protocols ?? ["rest", "socket"],
            ...(transport.securityProfile
              ? { security_profile: transport.securityProfile }
              : {}),
            ...(transport.authStrategy
              ? { auth_strategy: transport.authStrategy }
              : {}),
          })),
          __serviceName: serviceName,
          __serviceInstanceId: serviceId,
          __useSocket: options.useSocket,
          __retryCount: options.retryCount,
          __isFrontend: true,
          __skipRemoteExecution: true,
        },
        0,
      );
    }

    this.serviceCreated = true;
    this.requestServiceManifestPublication(
      "service_created",
      true,
      "routing_capability",
    );
  }

  /**
   * Creates a Cadenza metadata service with the specified name, description, and options.
   *
   * @param {string} serviceName - The name of the metadata service to be created.
   * @param {string} description - A brief description of the metadata service.
   * @param {ServerOptions} [options={}] - Optional configuration for the metadata service. Defaults to an empty object.
   * @return {void} Does not return a value.
   */
  static createCadenzaMetaService(
    serviceName: string,
    description: string,
    options: ServerOptions = {},
  ) {
    options.isMeta = true;
    this.createCadenzaService(serviceName, description, options);
  }

  /**
   * Creates a framework-agnostic browser runtime actor on top of frontend mode.
   * The actor owns readiness and projected browser runtime state while the caller
   * remains free to adapt that state into any frontend framework.
   */
  static createBrowserRuntimeActor<
    TProjectionState extends Record<string, any>,
  >(
    options: BrowserRuntimeActorOptions<TProjectionState>,
  ): BrowserRuntimeActorHandle<TProjectionState> {
    this.bootstrap();
    return createBrowserRuntimeActorHelper(this as any, options);
  }

  /**
   * Creates and initializes a specialized PostgresActor.
   * This is actor-only and does not create or register a network service.
   *
   * @param {string} name - Logical PostgresActor name.
   * @param {DatabaseSchemaDefinition} schema - Database schema definition.
   * @param {string} [description=""] - Optional human-readable actor description.
   * @param {ServerOptions & DatabaseOptions} [options={}] - Actor/database runtime options.
   * @return {void}
   */
  static createPostgresActor(
    name: string,
    schema: DatabaseSchemaDefinition,
    description: string = "",
    options: ServerOptions & DatabaseOptions = {},
  ) {
    if (isBrowser || options.isFrontend) {
      console.warn(
        "PostgresActor creation is not supported in frontend mode.",
      );
      return;
    }

    this.bootstrap();
    this.validateName(name);
    const databaseController = DatabaseController.instance;

    const normalizedOptions = this.normalizePostgresActorOptions(name, options);

    const registration = databaseController.createPostgresActor(
      name,
      schema,
      description,
      normalizedOptions,
    );

    console.log("Creating PostgresActor", {
      name,
      actorName: registration.actorName,
      ownerServiceName: normalizedOptions.ownerServiceName ?? null,
      options: normalizedOptions,
    });

    databaseController.requestPostgresActorSetup(registration, {
      actorName: registration.actorName,
      actorToken: registration.actorToken,
      databaseName: normalizedOptions.databaseName,
      ownerServiceName: normalizedOptions.ownerServiceName ?? null,
    });
  }

  /**
   * Creates a meta PostgresActor.
   *
   * @param {string} name - Logical PostgresActor name.
   * @param {DatabaseSchemaDefinition} schema - Database schema definition.
   * @param {string} [description=""] - Optional description.
   * @param {ServerOptions & DatabaseOptions} [options={}] - Optional actor/database options.
   * @return {void}
   */
  static createMetaPostgresActor(
    name: string,
    schema: DatabaseSchemaDefinition,
    description: string = "",
    options: ServerOptions & DatabaseOptions = {},
  ) {
    this.bootstrap();
    options.isMeta = true;
    this.createPostgresActor(name, schema, description, options);
  }

  /**
   * Creates a dedicated database service by composing a PostgresActor and a Cadenza service.
   */
  static createDatabaseService(
    name: string,
    schema: DatabaseSchemaDefinition,
    description: string = "",
    options: ServerOptions & DatabaseOptions = {},
  ) {
    if (isBrowser || options.isFrontend) {
      console.warn(
        "Database service creation is not supported in frontend mode. Use the CadenzaDB service instead.",
      );
      return;
    }
    if (this.serviceCreated) return;

    this.bootstrap();
    this.validateName(name);
    this.validateServiceName(name);
    this.defaultDatabaseServiceName = name;

    const databaseController = DatabaseController.instance;
    const actorOptions = this.normalizePostgresActorOptions(name, {
      ...options,
      ownerServiceName: options.ownerServiceName ?? name,
    });
    const serviceOptions = this.normalizeDatabaseServiceOptions(name, {
      ...options,
      ownerServiceName: actorOptions.ownerServiceName,
    });

    const registration = databaseController.createPostgresActor(
      name,
      schema,
      description,
      actorOptions,
    );

    this.registerDatabaseServiceBridgeTask(
      name,
      description,
      schema,
      Boolean(serviceOptions.isMeta),
      registration.actorName,
    );

    const createServiceTaskName = `Create database service ${name} after ${registration.actorName} setup`;
    const traceSetupDoneTaskName = `Trace database service ${name} setup done`;
    if (POSTGRES_SETUP_DEBUG_ENABLED && !this.get(traceSetupDoneTaskName)) {
      this.createMetaTask(
        traceSetupDoneTaskName,
        (ctx) => {
          console.log("[CADENZA_POSTGRES_SETUP_DEBUG] setup_done_signal_observed", {
            serviceName: name,
            actorName: registration.actorName,
            payloadKeys: Object.keys(ctx ?? {}),
          });
          return true;
        },
        "Debug trace for PostgresActor setup-done signal delivery.",
        { isHidden: true, register: false },
      ).doOn(registration.setupDoneSignal);
    }
    if (!this.get(createServiceTaskName)) {
      this.createMetaTask(
        createServiceTaskName,
        () => {
          if (POSTGRES_SETUP_DEBUG_ENABLED) {
            console.log(
              "[CADENZA_POSTGRES_SETUP_DEBUG] create_database_service_task_fired",
              {
                serviceName: name,
                actorName: registration.actorName,
              },
            );
          }
          this.createCadenzaService(name, description, serviceOptions);
          return true;
        },
        "Creates the networked database service after PostgresActor setup completes.",
      ).doOn(registration.setupDoneSignal);
    }

    const setupFailureTaskName = `Handle database service ${name} bootstrap failure`;
    if (!this.get(setupFailureTaskName)) {
      this.createMetaTask(
        setupFailureTaskName,
        (ctx) => {
          this.log(
            "Database service bootstrap failed before service creation.",
            {
              serviceName: name,
              actorName: registration.actorName,
              databaseName: registration.databaseName,
              error: ctx.__error,
            },
            "error",
          );
          return true;
        },
        "Logs PostgresActor setup failures for database service bootstrap.",
      ).doOn(registration.setupFailedSignal);
    }

    console.log("Creating database service wrapper", {
      serviceName: name,
      actorName: registration.actorName,
      actorOptions,
      serviceOptions,
    });

    databaseController.requestPostgresActorSetup(registration, {
      actorName: registration.actorName,
      actorToken: registration.actorToken,
      databaseName: actorOptions.databaseName,
      ownerServiceName: actorOptions.ownerServiceName ?? name,
    });
  }

  /**
   * Creates a meta database service with the specified configuration.
   *
   * @param {string} name - The name of the database service to be created.
   * @param {DatabaseSchemaDefinition} schema - The schema definition for the database.
   * @param {string} [description=""] - An optional description of the database service.
   * @param {ServerOptions & DatabaseOptions} [options={}] - Optional server and database configuration options. The `isMeta` flag will be automatically set to true.
   * @return {void} - This method does not return a value.
   */
  static createMetaDatabaseService(
    name: string,
    schema: DatabaseSchemaDefinition,
    description: string = "",
    options: ServerOptions & DatabaseOptions = {},
  ) {
    this.createDatabaseService(name, schema, description, {
      ...options,
      isMeta: true,
    });
  }

  private static normalizePostgresActorOptions(
    name: string,
    options: ServerOptions & DatabaseOptions = {},
  ): ServerOptions & DatabaseOptions {
    return {
      isMeta: false,
      retryCount: 3,
      databaseType: "postgres",
      databaseName: snakeCase(name),
      poolSize: readIntegerEnv("DATABASE_POOL_SIZE", 10),
      ownerServiceName:
        options.ownerServiceName ?? this.serviceRegistry?.serviceName ?? null,
      ...options,
    };
  }

  private static normalizeDatabaseServiceOptions(
    name: string,
    options: ServerOptions & DatabaseOptions = {},
  ): ServerOptions & DatabaseOptions {
    return {
      loadBalance: true,
      useSocket: true,
      displayName: undefined,
      isMeta: false,
      port: readIntegerEnv("HTTP_PORT", 3000),
      securityProfile:
        (readStringEnv("SECURITY_PROFILE") as SecurityProfile) ?? "medium",
      networkMode: (readStringEnv("NETWORK_MODE") as NetworkMode) ?? "dev",
      retryCount: 3,
      cadenzaDB: {
        connect: true,
      },
      databaseType: "postgres",
      databaseName: snakeCase(name),
      poolSize: readIntegerEnv("DATABASE_POOL_SIZE", 10),
      isDatabase: true,
      ownerServiceName: options.ownerServiceName ?? name,
      ...options,
    };
  }

  private static registerDatabaseServiceBridgeTask(
    serviceName: string,
    description: string,
    schema: DatabaseSchemaDefinition,
    isMeta: boolean,
    actorName: string,
  ): void {
    const taskName = `Insert database service bridge ${serviceName}`;
    if (this.get(taskName)) {
      return;
    }

    this.createMetaTask(
      taskName,
      (ctx, emit) => {
        if (ctx.__serviceName && ctx.__serviceName !== serviceName) {
          return false;
        }

        emit("global.meta.created_database_service", {
          data: {
            service_name: serviceName,
            description,
            schema,
            is_meta: isMeta,
          },
        });
        this.log("Database service created", {
          name: serviceName,
          isMeta,
          actorName,
        });
        return true;
      },
      "Bridges database service creation into the global metadata signal.",
    ).doOn("meta.service_registry.service_inserted");
  }

  static createActor<
    D extends Record<string, any> = AnyObject,
    R = AnyObject,
  >(
    spec: ActorSpec<D, R>,
    options: ActorFactoryOptions = {},
  ): Actor<D, R> {
    this.bootstrap();
    return Cadenza.createActor(
      spec,
      this.withActorSessionHydration(
        spec,
        options as ActorFactoryOptions<D, R>,
      ),
    );
  }

  static createActorFromDefinition<
    D extends Record<string, any> = AnyObject,
    R = AnyObject,
  >(
    definition: ActorDefinition<D, R>,
    options: ActorFactoryOptions<D, R> = {},
  ): Actor<D, R> {
    this.bootstrap();
    return Cadenza.createActorFromDefinition(
      definition,
      this.withActorSessionHydration(
        {
          name: definition.name,
          description: definition.description,
          defaultKey: definition.defaultKey,
          kind: definition.kind,
          loadPolicy: definition.loadPolicy,
          writeContract: definition.writeContract,
          consistencyProfile: definition.consistencyProfile,
          retry: definition.retry,
          idempotency: definition.idempotency,
          session: definition.session,
          runtimeReadGuard: definition.runtimeReadGuard,
          key: definition.key,
          state: definition.state,
          taskBindings: definition.tasks,
          initState:
            definition.state?.durable?.initState ??
            (
              definition.state?.durable as
                | { initialState?: D | (() => D) }
                | undefined
            )?.initialState,
        },
        options,
      ),
    );
  }

  private static withActorSessionHydration<
    D extends Record<string, any>,
    R = AnyObject,
  >(
    spec: ActorSpec<D, R>,
    options: ActorFactoryOptions<D, R>,
  ): ActorFactoryOptions<D, R> {
    if (
      options.hydrateDurableState ||
      spec.session?.persistDurableState !== true
    ) {
      return options;
    }

    const actorName = String(spec.name ?? "").trim();
    const actorVersion = 1;
    const timeoutMs = normalizePositiveInteger(
      spec.session?.persistenceTimeoutMs,
      5000,
    );

    return {
      ...options,
      hydrateDurableState: async (actorKey: string) => {
        registerActorSessionPersistenceTasks();

        const response = await Cadenza.inquire(
          META_ACTOR_SESSION_STATE_HYDRATE_INTENT,
          {
            actor_name: actorName,
            actor_version: actorVersion,
            actor_key: actorKey,
          },
          {
            timeout: timeoutMs,
            requireComplete: true,
            rejectOnTimeout: true,
          },
        );

        if (!response || typeof response !== "object" || response.__success !== true) {
          throw new Error(
            resolveInquiryFailureError(
              META_ACTOR_SESSION_STATE_HYDRATE_INTENT,
              response,
            ),
          );
        }

        if (response.hydrated !== true) {
          return null;
        }

        return {
          durableState: response.durable_state as D,
          durableVersion: Number(response.durable_version),
        };
      },
    };
  }

  /**
   * Creates and registers a new task with the provided name, function, and optional details.
   *
   * @param {string} name - The name of the task to be created.
   * @param {TaskFunction} func - The function that contains the task execution logic.
   * @param {string} [description] - An optional description of what the task does.
   * @param {TaskOptions} [options={}] - An optional configuration object specifying additional task options.
   * @return {Task} - The created task instance.
   *
   * @example
   * You can use arrow functions to create tasks.
   * ```ts
   * const task = Cadenza.createTask('My task', (ctx) => {
   *   console.log('My task executed with context:', ctx);
   * }, 'My task description');
   * ```
   *
   * You can also use named functions to create tasks.
   * This is the preferred way to create tasks since it allows for code inspection in the CadenzaUI.
   * ```ts
   * function myTask(ctx) {
   *   console.log('My task executed with context:', ctx);
   * }
   *
   * const task = Cadenza.createTask('My task', myTask);
   * ```
   *
   * ** Use the TaskOptions object to configure the task. **
   *
   * With concurrency limit, timeout limit and retry settings.
   * ```ts
   * Cadenza.createTask('My task', (ctx) => {
   *   console.log('My task executed with context:', ctx);
   * }, 'My task description', {
   *   concurrency: 10,
   *   timeout: 10000,
   *   retryCount: 3,
   *   retryDelay: 1000,
   *   retryDelayFactor: 1.5,
   * });
   * ```
   *
   * You can specify the input and output context schemas for the task.
   * ```ts
   * Cadenza.createTask('My task', (ctx) => {
   *   return { bar: 'foo' + ctx.foo };
   * }, 'My task description', {
   *   inputContextSchema: {
   *     type: 'object',
   *     properties: {
   *       foo: {
   *         type: 'string',
   *       },
   *     },
   *   required: ['foo'],
   *   },
   *   validateInputContext: true, // default is false
   *   outputContextSchema: {
   *     type: 'object',
   *     properties: {
   *       bar: {
   *         type: 'string',
   *       },
   *     },
   *     required: ['bar'],
   *   },
   *   validateOutputContext: true, // default is false
   * });
   * ```
   */
  static createTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createTask(name, func, description, options);
  }

  /**
   * Creates a meta task with the specified name, functionality, description, and options.
   * This is used for creating tasks that lives on the meta layer.
   * The meta layer is a special layer that is executed separately from the business logic layer and is used for extending Cadenzas core functionality.
   * See {@link Task} or {@link createTask} for more information.
   *
   * @param {string} name - The name of the meta task.
   * @param {TaskFunction} func - The function to be executed by the meta task.
   * @param {string} [description] - An optional description of the meta task.
   * @param {TaskOptions} [options={}] - Additional optional task configuration. Automatically sets `isMeta` to true.
   * @return {Task} A task instance configured as a meta task.
   */
  static createMetaTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createMetaTask(name, func, description, options);
  }

  /**
   * Creates a unique task by wrapping the provided task function with a uniqueness constraint.
   * Unique tasks are designed to execute once per execution ID, merging parents. This is useful for
   * tasks that require fan-in/joins after parallel branches.
   * See {@link Task} for more information.
   * @param {string} name Unique identifier.
   * @param {TaskFunction} func Function receiving joinedContexts as a list (context.joinedContexts).
   * @param {string} [description] Optional description.
   * @param {TaskOptions} [options={}] Optional task options.
   * @returns {Task} The created UniqueTask.
   *
   * @example
   * ```ts
   * const splitTask = Cadenza.createTask('Split foos', function* (ctx) {
   *   for (const foo of ctx.foos) {
   *     yield { foo };
   *   }
   * }, 'Splits a list of foos into multiple sub-branches');
   *
   * const processTask = Cadenza.createTask('Process foo', (ctx) => {
   *  return { bar: 'foo' + ctx.foo };
   * }, 'Process a foo');
   *
   * const uniqueTask = Cadenza.createUniqueTask('Gather processed foos', (ctx) => {
   *   // A unique task will always be provided with a list of contexts (ctx.joinedContexts) from its predecessors.
   *   const processedFoos = ctx.joinedContexts.map((c) => c.bar);
   *   return { foos: processedFoos };
   * }, 'Gathers together the processed foos.');
   *
   * splitTask.then(
   *   processTask.then(
   *     uniqueTask,
   *   ),
   * );
   *
   * // Give the flow a name using a routine
   * Cadenza.createRoutine(
   *   'Process foos',
   *   [splitTask],
   *   'Processes a list of foos'
   * ).doOn('main.received_foos'); // Subscribe to a signal
   *
   * // Trigger the flow from anywhere
   * Cadenza.emit('main.received_foos', { foos: ['foo1', 'foo2', 'foo3'] });
   * ```
   */
  static createUniqueTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createUniqueTask(name, func, description, options);
  }

  /**
   * Creates a unique meta task with the specified name, function, description, and options.
   * See {@link createUniqueTask} and {@link createMetaTask} for more information.
   *
   * @param {string} name - The name of the task to create.
   * @param {TaskFunction} func - The function to execute when the task is run.
   * @param {string} [description] - An optional description of the task.
   * @param {TaskOptions} [options={}] - Optional settings for the task. Defaults to an empty object. Automatically sets `isMeta` and `isUnique` to true.
   * @return {Task} The created unique meta task.
   */
  static createUniqueMetaTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createUniqueMetaTask(name, func, description, options);
  }

  /**
   * Creates a throttled task with a concurrency limit of 1, ensuring that only one instance of the task can run at a time for a specific throttle tag.
   * This is useful for ensuring execution order and preventing race conditions.
   * See {@link Task} for more information.
   *
   * @param {string} name - The name of the task.
   * @param {TaskFunction} func - The function to be executed when the task runs.
   * @param {ThrottleTagGetter} [throttledIdGetter=() => "default"] - A function that generates a throttle tag identifier to group tasks for throttling.
   * @param {string} [description] - An optional description of the task.
   * @param {TaskOptions} [options={}] - Additional options to customize the task behavior.
   * @return {Task} The created throttled task.
   *
   * @example
   * ```ts
   * const task = Cadenza.createThrottledTask(
   *   'My task',
   *   async (ctx) => {
   *      await new Promise((resolve) => setTimeout(resolve, 1000));
   *      console.log('My task executed with context:', ctx);
   *   },
   *   // Will throttle by the value of ctx.foo to make sure tasks with the same value are executed sequentially
   *   (ctx) => ctx.foo,
   * );
   *
   * Cadenza.run(task, { foo: 'bar' }); // (First execution)
   * Cadenza.run(task, { foo: 'bar' }); // This will be executed after the first execution is finished
   * Cadenza.run(task, { foo: 'baz' }); // This will be executed in parallel with the first execution
   * ```
   */
  static createThrottledTask(
    name: string,
    func: TaskFunction,
    throttledIdGetter: ThrottleTagGetter = () => "default",
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createThrottledTask(
      name,
      func,
      throttledIdGetter,
      description,
      options,
    );
  }

  /**
   * Creates a throttled meta task with the specified configuration.
   * See {@link createThrottledTask} and {@link createMetaTask} for more information.
   *
   * @param {string} name - The name of the throttled meta task.
   * @param {TaskFunction} func - The task function to be executed.
   * @param {ThrottleTagGetter} throttledIdGetter - A function to retrieve the throttling identifier.
   * @param {string} [description] - An optional description of the task.
   * @param {TaskOptions} [options={}] - Additional options for configuring the task.
   * @return {Task} The created throttled meta task.
   */
  static createThrottledMetaTask(
    name: string,
    func: TaskFunction,
    throttledIdGetter: ThrottleTagGetter = () => "default",
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createThrottledMetaTask(
      name,
      func,
      throttledIdGetter,
      description,
      options,
    );
  }

  /**
   * Creates and returns a new debounced task with the specified parameters.
   * This is useful to prevent rapid execution of tasks that may be triggered by multiple events within a certain time frame.
   * See {@link DebounceTask} for more information.
   *
   * @param {string} name - The unique name of the task to be created.
   * @param {TaskFunction} func - The function to be executed by the task.
   * @param {string} [description] - An optional description of the task.
   * @param {number} [debounceTime=1000] - The debounce time in milliseconds to delay the execution of the task.
   * @param {TaskOptions & DebounceOptions} [options={}] - Additional configuration options for the task, including debounce behavior and other task properties.
   * @return {DebounceTask} A new instance of the DebounceTask with the specified configuration.
   *
   * @example
   * ```ts
   * const task = Cadenza.createDebounceTask(
   *   'My debounced task',
   *   (ctx) => {
   *      console.log('My task executed with context:', ctx);
   *   },
   *   'My debounced task description',
   *   100, // Debounce time in milliseconds. Default is 1000
   *   {
   *     leading: false, // Should the first execution of a burst be executed immediately? Default is false
   *     trailing: true, // Should the last execution of a burst be executed? Default is true
   *     maxWait: 1000, // Maximum time in milliseconds to wait for the next execution. Default is 0
   *   },
   * );
   *
   * Cadenza.run(task, { foo: 'bar' }); // This will not be executed
   * Cadenza.run(task, { foo: 'bar' }); // This will not be executed
   * Cadenza.run(task, { foo: 'baz' }); // This execution will be delayed by 100ms
   * ```
   */
  static createDebounceTask(
    name: string,
    func: TaskFunction,
    description?: string,
    debounceTime: number = 1000,
    options: TaskOptions & DebounceOptions = {},
  ): DebounceTask {
    this.bootstrap();
    return Cadenza.createDebounceTask(
      name,
      func,
      description,
      debounceTime,
      options,
    );
  }

  /**
   * Creates a debounced meta task with the specified parameters.
   * See {@link createDebounceTask} and {@link createMetaTask} for more information.
   *
   * @param {string} name - The name of the task.
   * @param {TaskFunction} func - The function to be executed by the task.
   * @param {string} [description] - Optional description of the task.
   * @param {number} [debounceTime=1000] - The debounce delay in milliseconds.
   * @param {TaskOptions & DebounceOptions} [options={}] - Additional configuration options for the task.
   * @return {DebounceTask} Returns an instance of the debounced meta task.
   */
  static createDebounceMetaTask(
    name: string,
    func: TaskFunction,
    description?: string,
    debounceTime: number = 1000,
    options: TaskOptions & DebounceOptions = {},
  ): DebounceTask {
    this.bootstrap();
    return Cadenza.createDebounceMetaTask(
      name,
      func,
      description,
      debounceTime,
      options,
    );
  }

  /**
   * Creates an ephemeral task with the specified configuration.
   * Ephemeral tasks are designed to self-destruct after execution or a certain condition is met.
   * This is useful for transient tasks such as resolving promises or performing cleanup operations.
   * They are not registered by default.
   * See {@link EphemeralTask} for more information.
   *
   * @param {string} name - The name of the task to be created.
   * @param {TaskFunction} func - The function that defines the logic of the task.
   * @param {string} [description] - An optional description of the task.
   * @param {TaskOptions & EphemeralTaskOptions} [options={}] - The configuration options for the task, including concurrency, timeouts, and retry policies.
   * @return {EphemeralTask} The created ephemeral task instance.
   *
   * @example
   * By default, ephemeral tasks are executed once and destroyed after execution.
   * ```ts
   * const task = Cadenza.createEphemeralTask('My ephemeral task', (ctx) => {
   *   console.log('My task executed with context:', ctx);
   * });
   *
   * Cadenza.run(task); // Executes the task once and destroys it after execution
   * Cadenza.run(task); // Does nothing, since the task is destroyed
   * ```
   *
   * Use destroy condition to conditionally destroy the task
   * ```ts
   * const task = Cadenza.createEphemeralTask(
   *   'My ephemeral task',
   *   (ctx) => {
   *      console.log('My task executed with context:', ctx);
   *   },
   *   'My ephemeral task description',
   *   {
   *     once: false, // Should the task be executed only once? Default is true
   *     destroyCondition: (ctx) => ctx.foo > 10, // Should the task be destroyed after execution? Default is undefined
   *   },
   * );
   *
   * Cadenza.run(task, { foo: 5 }); // The task will not be destroyed and can still be executed
   * Cadenza.run(task, { foo: 10 }); // The task will not be destroyed and can still be executed
   * Cadenza.run(task, { foo: 20 }); // The task will be destroyed after execution and cannot be executed anymore
   * Cadenza.run(task, { foo: 30 }); // This will not be executed
   * ```
   *
   * A practical use case for ephemeral tasks is to resolve a promise upon some external event.
   * ```ts
   * const task = Cadenza.createTask('Confirm something', (ctx, emit) => {
   *   return new Promise((resolve) => {
   *     ctx.foo = uuid();
   *
   *     Cadenza.createEphemeralTask(`Resolve promise of ${ctx.foo}`, (c) => {
   *       console.log('My task executed with context:', ctx);
   *       resolve(c);
   *     }).doOn(`socket.confirmation_received:${ctx.foo}`);
   *
   *     emit('this_domain.confirmation_requested', ctx);
   *   });
   * });
   * ```
   */
  static createEphemeralTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions & EphemeralTaskOptions = {},
  ): EphemeralTask {
    this.bootstrap();
    return Cadenza.createEphemeralTask(name, func, description, options);
  }

  /**
   * Creates an ephemeral meta-task with the specified name, function, description, and options.
   * See {@link createEphemeralTask} and {@link createMetaTask} for more details.
   *
   * @param {string} name - The name of the task to be created.
   * @param {TaskFunction} func - The function to be executed as part of the task.
   * @param {string} [description] - An optional description of the task.
   * @param {TaskOptions & EphemeralTaskOptions} [options={}] - Additional options for configuring the task.
   * @return {EphemeralTask} The created ephemeral meta-task.
   */
  static createEphemeralMetaTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions & EphemeralTaskOptions = {},
  ): EphemeralTask {
    this.bootstrap();
    return Cadenza.createEphemeralMetaTask(name, func, description, options);
  }

  /**
   * Creates a new routine with the specified name, tasks, and an optional description.
   * Routines are named entry points to starting tasks and are registered in the GraphRegistry.
   * They are used to group tasks together and provide a high-level structure for organizing and managing the execution of a set of tasks.
   * See {@link GraphRoutine} for more information.
   *
   * @param {string} name - The name of the routine to create.
   * @param {Task[]} tasks - A list of tasks to include in the routine.
   * @param {string} [description=""] - An optional description for the routine.
   * @return {GraphRoutine} A new instance of the GraphRoutine containing the specified tasks and description.
   *
   * @example
   * ```ts
   * const task1 = Cadenza.createTask("Task 1", () => {});
   * const task2 = Cadenza.createTask("Task 2", () => {});
   *
   * task1.then(task2);
   *
   * const routine = Cadenza.createRoutine("Some routine", [task1]);
   *
   * Cadenza.run(routine);
   *
   * // Or, routines can be triggered by signals
   * routine.doOn("some.signal");
   *
   * Cadenza.emit("some.signal", {});
   * ```
   */
  static createRoutine(
    name: string,
    tasks: Task[],
    description: string = "",
  ): GraphRoutine {
    this.bootstrap();
    return Cadenza.createRoutine(name, tasks, description);
  }

  /**
   * Creates a meta routine with a given name, tasks, and optional description.
   * Routines are named entry points to starting tasks and are registered in the GraphRegistry.
   * They are used to group tasks together and provide a high-level structure for organizing and managing the execution of a set of tasks.
   * See {@link GraphRoutine} and {@link createRoutine} for more information.
   *
   * @param {string} name - The name of the routine to be created.
   * @param {Task[]} tasks - An array of tasks that the routine will consist of.
   * @param {string} [description=""] - An optional description for the routine.
   * @return {GraphRoutine} A new instance of the `GraphRoutine` representing the created routine.
   */
  static createMetaRoutine(
    name: string,
    tasks: Task[],
    description: string = "",
  ): GraphRoutine {
    this.bootstrap();
    return Cadenza.createMetaRoutine(name, tasks, description);
  }

  static reset() {
    Cadenza.reset();
    this.serviceRegistry?.reset();
    this.unregisterGracefulShutdownHandlers();
    this.isBootstrapped = false;
    this.serviceCreated = false;
    this.bootstrapSyncCompleted = false;
    this.bootstrapSignalRegistrationsCompleted = false;
    this.bootstrapIntentRegistrationsCompleted = false;
    this.defaultDatabaseServiceName = null;
    this.warnedInvalidMetaIntentResponderKeys = new Set();
    this.hydratedInquiryResults = new Map();
    this.frontendSyncScheduled = false;
    this.serviceManifestRevision = 0;
    this.lastPublishedServiceManifestHashes = {};
    this.serviceManifestPublicationInFlight = false;
    this.serviceManifestPublicationPendingReason = null;
    this.serviceManifestPublicationPendingLayer = null;
    resetBrowserRuntimeActorHandles();
  }
}
