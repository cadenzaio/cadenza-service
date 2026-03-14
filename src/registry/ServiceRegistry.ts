import { GraphRoutine, Task } from "@cadenza.io/core";
import type { AnyObject } from "@cadenza.io/core";
import Cadenza from "../Cadenza";
import { isBrowser } from "../utils/environment";
import { InquiryResponderDescriptor } from "../types/inquiry";
import type { ServiceInstanceDescriptor } from "../types/serviceRegistry";
import type {
  ServiceTransportDescriptor,
  ServiceTransportProtocol,
  ServiceTransportRole,
} from "../types/transport";
import {
  isMetaIntentName,
  META_READINESS_INTENT,
  META_RUNTIME_STATUS_INTENT,
  META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT,
} from "../utils/inquiry";
import {
  getRouteableTransport,
  normalizeServiceInstanceDescriptor,
} from "../utils/serviceInstance";
import {
  buildTransportClientKey,
  normalizeServiceTransportDescriptor,
  transportSupportsProtocol,
} from "../utils/transport";
import {
  evaluateDependencyReadiness,
  resolveServiceReadinessState,
  summarizeDependencyReadiness,
  type DependencyReadinessState,
  type ReadinessState,
} from "../utils/readiness";
import {
  hasSignificantRuntimeStatusChange,
  resolveRuntimeStatus,
  runtimeStatusPriority,
  type RuntimeStatusSnapshot,
  type RuntimeStatusState,
} from "../utils/runtimeStatus";

const META_SERVICE_REGISTRY_FULL_SYNC_INTENT =
  "meta-service-registry-full-sync";
const META_RUNTIME_STATUS_HEARTBEAT_TICK_SIGNAL =
  "meta.service_registry.runtime_status.heartbeat_tick";
const META_RUNTIME_STATUS_MONITOR_TICK_SIGNAL =
  "meta.service_registry.runtime_status.monitor_tick";
const INTERNAL_RUNTIME_STATUS_TASK_NAMES = new Set([
  "Track local routine start",
  "Track local routine end",
  "Start runtime status sharing intervals",
  "Broadcast runtime status",
  "Monitor dependee heartbeat freshness",
  "Resolve runtime status fallback inquiry",
  "Respond runtime status inquiry",
  "Respond readiness inquiry",
  "Collect distributed readiness",
  "Get status",
]);

function readPositiveIntegerEnv(name: string, fallback: number): number {
  if (typeof process === "undefined") {
    return fallback;
  }

  const raw = process.env?.[name];
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }

  const normalized = Math.trunc(parsed);
  if (normalized <= 0) {
    return fallback;
  }

  return normalized;
}

export interface DeputyDescriptor {
  serviceName: string;
  remoteRoutineName?: string;
  signalName?: string;
  localTaskName: string;
  communicationType: string;
}

interface RemoteIntentDeputyDescriptor {
  key: string;
  intentName: string;
  serviceName: string;
  remoteTaskName: string;
  remoteTaskVersion: number;
  localTaskName: string;
  localTask: Task;
}

interface RuntimeStatusReport {
  serviceName: string;
  serviceInstanceId: string;
  transportId?: string;
  transportOrigin?: string;
  transportProtocols?: ServiceTransportProtocol[];
  isFrontend?: boolean;
  reportedAt: string;
  state: RuntimeStatusState;
  acceptingWork: boolean;
  numberOfRunningGraphs: number;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
  health?: AnyObject;
}

interface RuntimeStatusFallbackRestDiagnostic {
  attempted: boolean;
  outcome:
    | "instance_missing"
    | "fetch_unavailable"
    | "no_rest_transport"
    | "http_error"
    | "invalid_report"
    | "identity_mismatch"
    | "fetch_error"
    | "matched";
  transport?: {
    uuid: string;
    role: ServiceTransportRole;
    origin: string;
    protocols: ServiceTransportProtocol[];
    clientCreated?: boolean;
  };
  responseStatus?: number;
  responseStatusText?: string;
  payloadServiceName?: string;
  payloadServiceInstanceId?: string;
  payloadKeys?: string[];
  error?: string;
}

interface RuntimeStatusFallbackFailureDiagnostics {
  target: {
    serviceName: string;
    serviceInstanceId: string;
  };
  instance: {
    exists: boolean;
    runtimeState?: RuntimeStatusState;
    acceptingWork?: boolean;
    reportedAt?: string | null;
    isDatabase?: boolean;
    isFrontend?: boolean;
    isBootstrapPlaceholder?: boolean;
    transports: Array<{
      uuid: string;
      role: ServiceTransportRole;
      origin: string;
      protocols: ServiceTransportProtocol[];
      clientCreated?: boolean;
    }>;
  };
  directStatusCheck: RuntimeStatusFallbackRestDiagnostic;
  inquiry: {
    meta: AnyObject;
    reportTargets: Array<{
      serviceName?: string;
      serviceInstanceId?: string;
      transportId?: string;
      state?: RuntimeStatusState;
      acceptingWork?: boolean;
      reportedAt?: string;
    }>;
  };
}

interface DependencyReadinessDetail {
  serviceName: string;
  serviceInstanceId: string;
  dependencyState: DependencyReadinessState;
  runtimeState: RuntimeStatusState;
  acceptingWork: boolean;
  missedHeartbeats: number;
  stale: boolean;
  blocked: boolean;
  reason:
    | "missing"
    | "heartbeat-timeout"
    | "heartbeat-stale"
    | "runtime-unavailable"
    | "runtime-overloaded"
    | "runtime-degraded"
    | "runtime-healthy";
  lastHeartbeatAt: string | null;
  reportedAt: string | null;
}

interface ReadinessReport {
  serviceName: string;
  serviceInstanceId: string;
  reportedAt: string;
  readinessState: ReadinessState;
  runtimeState: RuntimeStatusState;
  acceptingWork: boolean;
  dependencySummary: {
    total: number;
    ready: number;
    degraded: number;
    overloaded: number;
    unavailable: number;
    stale: number;
  };
  dependencies?: DependencyReadinessDetail[];
}

/**
 * The ServiceRegistry class is a singleton that manages the registration and lifecycle of
 * service instances, deputies, and remote signals in a distributed service architecture.
 * It handles various tasks such as instance updates, remote signal registration,
 * service status synchronization, and error/event broadcasting.
 */
export default class ServiceRegistry {
  private static _instance: ServiceRegistry;
  public static get instance(): ServiceRegistry {
    if (!this._instance) this._instance = new ServiceRegistry();
    return this._instance;
  }

  private instances: Map<string, ServiceInstanceDescriptor[]> = new Map();
  private deputies: Map<string, DeputyDescriptor[]> = new Map();
  private remoteSignals: Map<string, Set<string>> = new Map();
  private remoteIntents: Map<string, Set<string>> = new Map();
  private remoteIntentDeputiesByKey: Map<string, RemoteIntentDeputyDescriptor> =
    new Map();
  private remoteIntentDeputiesByTask: Map<Task, RemoteIntentDeputyDescriptor> =
    new Map();
  private dependeesByService: Map<string, Set<string>> = new Map();
  private dependeeByInstance: Map<string, string> = new Map();
  private readinessDependeesByService: Map<string, Set<string>> = new Map();
  private readinessDependeeByInstance: Map<string, string> = new Map();
  private lastHeartbeatAtByInstance: Map<string, number> = new Map();
  private missedHeartbeatsByInstance: Map<string, number> = new Map();
  private runtimeStatusFallbackInFlightByInstance: Set<string> = new Set();
  private activeRoutineExecutionIds: Set<string> = new Set();
  private runtimeStatusHeartbeatStarted = false;
  private lastRuntimeStatusSnapshot: RuntimeStatusSnapshot | null = null;
  private readonly runtimeStatusHeartbeatIntervalMs = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_HEARTBEAT_MS",
    30_000,
  );
  private readonly runtimeStatusMissThreshold = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_MISSED_HEARTBEATS",
    3,
  );
  private readonly runtimeStatusFallbackTimeoutMs = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_FALLBACK_TIMEOUT_MS",
    1_500,
  );
  private readonly degradedGraphThreshold = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_DEGRADED_GRAPH_THRESHOLD",
    10,
  );
  private readonly overloadedGraphThreshold = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_OVERLOADED_GRAPH_THRESHOLD",
    20,
  );
  serviceName: string | null = null;
  serviceInstanceId: string | null = null;
  numberOfRunningGraphs: number = 0;
  useSocket: boolean = false;
  retryCount: number = 3;
  isFrontend: boolean = false;

  handleInstanceUpdateTask: Task;
  handleTransportUpdateTask: Task;
  handleGlobalSignalRegistrationTask: Task;
  handleGlobalIntentRegistrationTask: Task;
  handleSocketStatusUpdateTask: Task;
  fullSyncTask: GraphRoutine | Task;
  getAllInstances: Task;
  doForEachInstance: Task;
  deleteInstance: Task;
  getBalancedInstance: Task;
  getInstanceById: Task;
  getInstancesByServiceName: Task;
  handleDeputyRegistrationTask: Task;
  getStatusTask: Task;
  insertServiceTask: Task;
  insertServiceInstanceTask: Task;
  insertServiceTransportTask: Task;
  handleServiceNotRespondingTask: Task;
  handleServiceHandshakeTask: Task;
  collectTransportDiagnosticsTask: Task;
  collectReadinessTask: Task;

  private buildRemoteIntentDeputyKey(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion?: number;
  }): string {
    return `${map.intentName}|${map.serviceName}|${map.taskName}|${map.taskVersion ?? 1}`;
  }

  private normalizeIntentMaps(ctx: AnyObject): Array<{
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion: number;
    deleted?: boolean;
  }> {
    if (Array.isArray((ctx as any).intentToTaskMaps)) {
      return (ctx as any).intentToTaskMaps
        .map((m: any) => ({
          intentName: m.intentName ?? m.intent_name,
          serviceName: m.serviceName ?? m.service_name,
          taskName: m.taskName ?? m.task_name,
          taskVersion: m.taskVersion ?? m.task_version ?? 1,
          deleted: !!m.deleted,
        }))
        .filter((m: any) => m.intentName && m.serviceName && m.taskName);
    }

    const single =
      (ctx as any).intentToTaskMap ??
      (ctx as any).data ??
      ((ctx as any).intentName ? ctx : undefined);

    if (!single) return [];

    const normalized = {
      intentName: single.intentName ?? single.intent_name,
      serviceName: single.serviceName ?? single.service_name,
      taskName: single.taskName ?? single.task_name,
      taskVersion: single.taskVersion ?? single.task_version ?? 1,
      deleted: !!single.deleted,
    };

    if (!normalized.intentName || !normalized.serviceName || !normalized.taskName)
      return [];

    return [normalized];
  }

  private registerRemoteIntentDeputy(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion: number;
  }) {
    if (!this.serviceName || map.serviceName === this.serviceName) {
      return;
    }

    const key = this.buildRemoteIntentDeputyKey(map);
    if (this.remoteIntentDeputiesByKey.has(key)) {
      return;
    }

    const deputyTaskName = `Inquire ${map.intentName} via ${map.serviceName} (${map.taskName} v${map.taskVersion})`;

    const deputyTask = isMetaIntentName(map.intentName)
      ? Cadenza.createMetaDeputyTask(map.taskName, map.serviceName, {
          register: false,
          isHidden: true,
          retryCount: 1,
          retryDelay: 50,
          retryDelayFactor: 1.2,
        })
      : Cadenza.createDeputyTask(map.taskName, map.serviceName, {
          register: false,
          isHidden: true,
          retryCount: 1,
          retryDelay: 50,
          retryDelayFactor: 1.2,
        });

    deputyTask.respondsTo(map.intentName);

    if (!this.remoteIntents.has(map.serviceName)) {
      this.remoteIntents.set(map.serviceName, new Set());
    }
    this.remoteIntents.get(map.serviceName)!.add(map.intentName);

    const descriptor: RemoteIntentDeputyDescriptor = {
      key,
      intentName: map.intentName,
      serviceName: map.serviceName,
      remoteTaskName: map.taskName,
      remoteTaskVersion: map.taskVersion,
      localTaskName: deputyTask.name || deputyTaskName,
      localTask: deputyTask,
    };

    this.remoteIntentDeputiesByKey.set(key, descriptor);
    this.remoteIntentDeputiesByTask.set(deputyTask, descriptor);
  }

  private unregisterRemoteIntentDeputy(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion?: number;
  }) {
    const key = this.buildRemoteIntentDeputyKey(map);
    const descriptor = this.remoteIntentDeputiesByKey.get(key);
    if (!descriptor) {
      return;
    }

    const task = descriptor.localTask;
    if (task) {
      Cadenza.inquiryBroker.unsubscribe(descriptor.intentName, task);
      task.destroy();
    }

    this.remoteIntentDeputiesByTask.delete(descriptor.localTask);
    this.remoteIntentDeputiesByKey.delete(key);

    this.remoteIntents.get(descriptor.serviceName)?.delete(descriptor.intentName);
    if (!this.remoteIntents.get(descriptor.serviceName)?.size) {
      this.remoteIntents.delete(descriptor.serviceName);
    }

    const deputies = this.deputies.get(descriptor.serviceName);
    if (deputies) {
      this.deputies.set(
        descriptor.serviceName,
        deputies.filter((d) => d.localTaskName !== descriptor.localTaskName),
      );

      if (this.deputies.get(descriptor.serviceName)?.length === 0) {
        this.deputies.delete(descriptor.serviceName);
      }
    }
  }

  public getInquiryResponderDescriptor(task: Task): InquiryResponderDescriptor {
    const remote = this.remoteIntentDeputiesByTask.get(task);

    if (remote) {
      return {
        isRemote: true,
        serviceName: remote.serviceName,
        taskName: remote.remoteTaskName,
        taskVersion: remote.remoteTaskVersion,
        localTaskName: remote.localTaskName,
      };
    }

    return {
      isRemote: false,
      serviceName: this.serviceName ?? "UnknownService",
      taskName: task.name,
      taskVersion: task.version,
      localTaskName: task.name,
    };
  }

  private getInstance(serviceName: string, instanceId: string) {
    return this.instances
      .get(serviceName)
      ?.find((instance) => instance.uuid === instanceId);
  }

  private getLocalInstance() {
    if (!this.serviceName || !this.serviceInstanceId) {
      return undefined;
    }

    return this.getInstance(this.serviceName, this.serviceInstanceId);
  }

  private summarizeTransportForDebug(
    transport: ServiceTransportDescriptor | undefined,
  ) {
    if (!transport) {
      return undefined;
    }

    return {
      uuid: transport.uuid,
      role: transport.role,
      origin: transport.origin,
      protocols: transport.protocols,
      clientCreated: transport.clientCreated,
    };
  }

  private summarizeInstanceForRuntimeStatusFallback(
    instance: ServiceInstanceDescriptor | undefined,
  ): RuntimeStatusFallbackFailureDiagnostics["instance"] {
    return {
      exists: Boolean(instance),
      runtimeState: instance?.runtimeState,
      acceptingWork: instance?.acceptingWork,
      reportedAt: instance?.reportedAt ?? null,
      isDatabase: instance?.isDatabase,
      isFrontend: instance?.isFrontend,
      isBootstrapPlaceholder: instance?.isBootstrapPlaceholder,
      transports: (instance?.transports ?? []).map((transport) => ({
        uuid: transport.uuid,
        role: transport.role,
        origin: transport.origin,
        protocols: transport.protocols,
        clientCreated: transport.clientCreated,
      })),
    };
  }

  private summarizeRuntimeStatusInquiryReports(inquiryResult: AnyObject) {
    const reports = Array.isArray(inquiryResult.runtimeStatusReports)
      ? inquiryResult.runtimeStatusReports
      : [];

    return reports.map((candidate) => {
      const normalized = this.normalizeRuntimeStatusReport(candidate);
      if (normalized) {
        return {
          serviceName: normalized.serviceName,
          serviceInstanceId: normalized.serviceInstanceId,
          transportId: normalized.transportId,
          state: normalized.state,
          acceptingWork: normalized.acceptingWork,
          reportedAt: normalized.reportedAt,
        };
      }

      const raw =
        candidate && typeof candidate === "object"
          ? (candidate as Record<string, unknown>)
          : {};
      const rawState: RuntimeStatusState | undefined =
        raw.state === "healthy" ||
        raw.state === "degraded" ||
        raw.state === "overloaded" ||
        raw.state === "unavailable"
          ? raw.state
          : undefined;

      return {
        serviceName:
          typeof raw.serviceName === "string"
            ? raw.serviceName
            : typeof raw.__serviceName === "string"
              ? raw.__serviceName
              : undefined,
        serviceInstanceId:
          typeof raw.serviceInstanceId === "string"
            ? raw.serviceInstanceId
            : typeof raw.__serviceInstanceId === "string"
              ? raw.__serviceInstanceId
              : undefined,
        transportId:
          typeof raw.transportId === "string"
            ? raw.transportId
            : typeof raw.serviceTransportId === "string"
              ? raw.serviceTransportId
              : undefined,
        state: rawState,
        acceptingWork:
          typeof raw.acceptingWork === "boolean" ? raw.acceptingWork : undefined,
        reportedAt:
          typeof raw.reportedAt === "string" ? raw.reportedAt : undefined,
      };
    });
  }

  private createRuntimeStatusFallbackError(
    message: string,
    diagnostics: RuntimeStatusFallbackFailureDiagnostics,
  ): Error & { runtimeStatusFallback: RuntimeStatusFallbackFailureDiagnostics } {
    return Object.assign(new Error(message), {
      runtimeStatusFallback: diagnostics,
    });
  }

  public resolveLocalStatusCheck(ctx: AnyObject = {}) {
    if (!this.serviceName) {
      return {
        __status: "error",
        __error: "No service name defined",
        errored: true,
      };
    }

    if (!this.serviceInstanceId) {
      return {
        __status: "error",
        __error: "No service instance id defined",
        errored: true,
      };
    }

    const report = this.buildLocalRuntimeStatusReport("full");
    if (!report) {
      return {
        ...ctx,
        __status: "error",
        __error: "No local service instance available for status check",
        errored: true,
      };
    }

    return {
      ...ctx,
      __status: "ok",
      __serviceName: report.serviceName,
      __serviceInstanceId: report.serviceInstanceId,
      __numberOfRunningGraphs: report.numberOfRunningGraphs,
      __health: report.health ?? {},
      __active: report.isActive,
      reportedAt: report.reportedAt,
      serviceName: report.serviceName,
      serviceInstanceId: report.serviceInstanceId,
      numberOfRunningGraphs: report.numberOfRunningGraphs,
      health: report.health ?? {},
      isActive: report.isActive,
      isNonResponsive: report.isNonResponsive,
      isBlocked: report.isBlocked,
      state: report.state,
      acceptingWork: report.acceptingWork,
    };
  }

  private resolveTransportProtocolOrder(
    ctx: AnyObject,
  ): ServiceTransportProtocol[] {
    const explicit =
      ctx.__preferredTransportProtocol === "rest" ||
      ctx.__preferredTransportProtocol === "socket"
        ? ctx.__preferredTransportProtocol
        : undefined;

    const preferred = explicit ?? (this.useSocket ? "socket" : "rest");
    const fallback = preferred === "socket" ? "rest" : "socket";

    return [preferred, fallback];
  }

  private selectTransportForInstance(
    instance: ServiceInstanceDescriptor,
    ctx: AnyObject,
    role: ServiceTransportRole = this.getRoutingTransportRole(),
  ): ServiceTransportDescriptor | undefined {
    for (const protocol of this.resolveTransportProtocolOrder(ctx)) {
      const transport = this.getRouteableTransport(instance, protocol, role);
      if (transport) {
        return transport;
      }
    }

    return undefined;
  }

  private getRoutingTransportRole(): ServiceTransportRole {
    return this.isFrontend ? "public" : "internal";
  }

  private getTransportById(
    instance: ServiceInstanceDescriptor,
    transportId: string,
  ): ServiceTransportDescriptor | undefined {
    return instance.transports.find((transport) => transport.uuid === transportId);
  }

  private getRouteableTransport(
    instance: ServiceInstanceDescriptor,
    protocol?: ServiceTransportProtocol,
    role: ServiceTransportRole = this.getRoutingTransportRole(),
  ): ServiceTransportDescriptor | undefined {
    return getRouteableTransport(instance, role, protocol);
  }

  private getTransportClientKey(
    instance: ServiceInstanceDescriptor,
    protocol?: ServiceTransportProtocol,
    role: ServiceTransportRole = this.getRoutingTransportRole(),
  ): string | null {
    const transport = this.getRouteableTransport(instance, protocol, role);
    if (!transport) {
      return null;
    }

    return buildTransportClientKey(transport);
  }

  private hasTransportClientCreated(
    instance: ServiceInstanceDescriptor,
    transportId: string,
  ): boolean {
    return (instance.clientCreatedTransportIds ?? []).includes(transportId);
  }

  private markTransportClientCreated(
    instance: ServiceInstanceDescriptor,
    transportId: string,
  ): void {
    if (!instance.clientCreatedTransportIds) {
      instance.clientCreatedTransportIds = [];
    }

    if (!instance.clientCreatedTransportIds.includes(transportId)) {
      instance.clientCreatedTransportIds.push(transportId);
    }
  }

  private registerDependee(
    serviceName: string,
    serviceInstanceId: string,
    options: {
      requiredForReadiness?: boolean;
    } = {},
  ) {
    if (!serviceName || !serviceInstanceId) {
      return;
    }

    if (!this.dependeesByService.has(serviceName)) {
      this.dependeesByService.set(serviceName, new Set());
    }

    this.dependeesByService.get(serviceName)!.add(serviceInstanceId);
    this.dependeeByInstance.set(serviceInstanceId, serviceName);

    if (options.requiredForReadiness) {
      if (!this.readinessDependeesByService.has(serviceName)) {
        this.readinessDependeesByService.set(serviceName, new Set());
      }
      this.readinessDependeesByService.get(serviceName)!.add(serviceInstanceId);
      this.readinessDependeeByInstance.set(serviceInstanceId, serviceName);
    }

    this.lastHeartbeatAtByInstance.set(serviceInstanceId, Date.now());
    this.missedHeartbeatsByInstance.set(serviceInstanceId, 0);
  }

  private unregisterDependee(serviceInstanceId: string, serviceName?: string) {
    const dependeeServiceName =
      serviceName ?? this.dependeeByInstance.get(serviceInstanceId);
    if (dependeeServiceName) {
      this.dependeesByService.get(dependeeServiceName)?.delete(serviceInstanceId);
      if (!this.dependeesByService.get(dependeeServiceName)?.size) {
        this.dependeesByService.delete(dependeeServiceName);
      }
    }

    this.dependeeByInstance.delete(serviceInstanceId);
    const readinessDependeeServiceName =
      serviceName ?? this.readinessDependeeByInstance.get(serviceInstanceId);
    if (readinessDependeeServiceName) {
      this.readinessDependeesByService
        .get(readinessDependeeServiceName)
        ?.delete(serviceInstanceId);
      if (!this.readinessDependeesByService.get(readinessDependeeServiceName)?.size) {
        this.readinessDependeesByService.delete(readinessDependeeServiceName);
      }
    }

    this.readinessDependeeByInstance.delete(serviceInstanceId);
    this.lastHeartbeatAtByInstance.delete(serviceInstanceId);
    this.missedHeartbeatsByInstance.delete(serviceInstanceId);
    this.runtimeStatusFallbackInFlightByInstance.delete(serviceInstanceId);
  }

  private reconcileBootstrapPlaceholderInstance(
    serviceName: string,
    resolvedInstanceId: string,
    emit: (signalName: string, ctx: AnyObject) => void,
  ) {
    const instances = this.instances.get(serviceName);
    if (!instances?.length) {
      return;
    }

    const placeholders = instances.filter(
      (instance) =>
        instance.uuid !== resolvedInstanceId && instance.isBootstrapPlaceholder,
    );

    if (!placeholders.length) {
      return;
    }

    for (const placeholder of placeholders) {
      const wasDependee = this.dependeeByInstance.has(placeholder.uuid);
      const requiredForReadiness = this.readinessDependeeByInstance.has(
        placeholder.uuid,
      );

      for (const transport of placeholder.transports) {
        const transportKey = buildTransportClientKey(transport);
        emit(`meta.socket_shutdown_requested:${transportKey}`, {});
        emit(`meta.fetch.destroy_requested:${transportKey}`, {});
      }

      this.unregisterDependee(placeholder.uuid, serviceName);

      if (wasDependee) {
        this.registerDependee(serviceName, resolvedInstanceId, {
          requiredForReadiness,
        });
      }
    }

    this.instances.set(
      serviceName,
      instances.filter((instance) => !instance.isBootstrapPlaceholder),
    );
  }

  private getHeartbeatMisses(serviceInstanceId: string, now = Date.now()): number {
    const observedMisses = this.missedHeartbeatsByInstance.get(serviceInstanceId) ?? 0;
    const lastHeartbeatAt = this.lastHeartbeatAtByInstance.get(serviceInstanceId) ?? 0;
    if (lastHeartbeatAt <= 0) {
      return Math.max(observedMisses, this.runtimeStatusMissThreshold);
    }

    const estimatedMisses = Math.max(
      0,
      Math.floor((now - lastHeartbeatAt) / this.runtimeStatusHeartbeatIntervalMs),
    );

    return Math.max(observedMisses, estimatedMisses);
  }

  private shouldRequireReadinessFromCommunicationTypes(
    communicationTypes: unknown,
  ): boolean {
    if (!Array.isArray(communicationTypes)) {
      return false;
    }

    return communicationTypes.some((type) => {
      const normalized = String(type).toLowerCase();
      return normalized === "delegation" || normalized === "inquiry";
    });
  }

  private resolveRuntimeStatusSnapshot(
    numberOfRunningGraphs: number,
    isActive: boolean,
    isNonResponsive: boolean,
    isBlocked: boolean,
  ): RuntimeStatusSnapshot {
    return resolveRuntimeStatus({
      numberOfRunningGraphs,
      isActive,
      isNonResponsive,
      isBlocked,
      degradedGraphThreshold: this.degradedGraphThreshold,
      overloadedGraphThreshold: this.overloadedGraphThreshold,
    });
  }

  private normalizeRuntimeStatusReport(ctx: AnyObject): RuntimeStatusReport | null {
    const serviceName =
      ctx.serviceName ?? ctx.__serviceName ?? ctx.serviceInstance?.serviceName;
    const serviceInstanceId =
      ctx.serviceInstanceId ??
      ctx.__serviceInstanceId ??
      ctx.serviceInstance?.uuid;
    if (!serviceName || !serviceInstanceId) {
      return null;
    }
    const transportId =
      ctx.transportId ??
      ctx.serviceTransportId ??
      ctx.serviceTransport?.uuid ??
      undefined;
    const transportOrigin =
      ctx.transportOrigin ??
      ctx.serviceOrigin ??
      ctx.serviceTransport?.origin ??
      undefined;
    const transportProtocols = Array.isArray(
      ctx.transportProtocols ?? ctx.serviceTransport?.protocols,
    )
      ? ((ctx.transportProtocols ?? ctx.serviceTransport?.protocols) as unknown[])
          .map((entry) => String(entry))
          .filter(
            (entry): entry is ServiceTransportProtocol =>
              entry === "rest" || entry === "socket",
          )
      : undefined;

    const numberOfRunningGraphs = Math.max(
      0,
      Math.trunc(
        Number(ctx.numberOfRunningGraphs ?? ctx.__numberOfRunningGraphs ?? 0),
      ),
    );
    const isActive = Boolean(ctx.isActive ?? ctx.__active ?? true);
    const isNonResponsive = Boolean(ctx.isNonResponsive ?? false);
    const isBlocked = Boolean(ctx.isBlocked ?? false);

    const resolved = this.resolveRuntimeStatusSnapshot(
      numberOfRunningGraphs,
      isActive,
      isNonResponsive,
      isBlocked,
    );

    return {
      serviceName,
      serviceInstanceId,
      transportId:
        typeof transportId === "string" && transportId.trim().length > 0
          ? transportId
          : undefined,
      transportOrigin:
        typeof transportOrigin === "string" && transportOrigin.trim().length > 0
          ? transportOrigin
          : undefined,
      transportProtocols:
        transportProtocols && transportProtocols.length > 0
          ? Array.from(new Set(transportProtocols))
          : undefined,
      isFrontend:
        typeof ctx.isFrontend === "boolean"
          ? ctx.isFrontend
          : typeof ctx.serviceInstance?.isFrontend === "boolean"
            ? ctx.serviceInstance.isFrontend
          : undefined,
      reportedAt:
        ctx.reportedAt ??
        (typeof ctx.__reportedAt === "string" ? ctx.__reportedAt : undefined) ??
        new Date().toISOString(),
      state:
        ctx.state === "healthy" ||
        ctx.state === "degraded" ||
        ctx.state === "overloaded" ||
        ctx.state === "unavailable"
          ? ctx.state
          : resolved.state,
      acceptingWork:
        typeof ctx.acceptingWork === "boolean"
          ? ctx.acceptingWork
          : resolved.acceptingWork,
      numberOfRunningGraphs,
      isActive,
      isNonResponsive,
      isBlocked,
      health: (ctx.health ?? ctx.__health ?? {}) as AnyObject,
    };
  }

  private applyRuntimeStatusReport(report: RuntimeStatusReport): boolean {
    const instance = this.getInstance(report.serviceName, report.serviceInstanceId);
    if (!instance) {
      return false;
    }

    if (report.transportId && report.transportOrigin) {
      const protocols =
        report.transportProtocols && report.transportProtocols.length > 0
          ? report.transportProtocols
          : (["rest", "socket"] as ServiceTransportProtocol[]);
      const existingTransport = this.getTransportById(instance, report.transportId);
      if (existingTransport) {
        existingTransport.origin = report.transportOrigin;
        existingTransport.protocols = protocols;
      } else {
        instance.transports.push({
          uuid: report.transportId,
          serviceInstanceId: report.serviceInstanceId,
          role: this.getRoutingTransportRole(),
          origin: report.transportOrigin,
          protocols,
          securityProfile: null,
          authStrategy: null,
        });
      }
    }

    if (typeof report.isFrontend === "boolean") {
      instance.isFrontend = report.isFrontend;
    }

    instance.numberOfRunningGraphs = report.numberOfRunningGraphs;
    instance.isActive = report.isActive;
    instance.isNonResponsive = report.isNonResponsive;
    instance.isBlocked = report.isBlocked;
    instance.runtimeState = report.state;
    instance.acceptingWork = report.acceptingWork;
    instance.reportedAt = report.reportedAt;
    instance.health = {
      ...(instance.health ?? {}),
      ...(report.health ?? {}),
      runtimeStatus: {
        state: report.state,
        acceptingWork: report.acceptingWork,
        reportedAt: report.reportedAt,
      },
    };

    return true;
  }

  private buildLocalRuntimeStatusReport(
    detailLevel: "minimal" | "full" = "minimal",
  ): RuntimeStatusReport | null {
    if (!this.serviceName || !this.serviceInstanceId) {
      return null;
    }

    const localInstance = this.getLocalInstance();
    if (!localInstance) {
      return null;
    }

    const numberOfRunningGraphs =
      this.activeRoutineExecutionIds.size || this.numberOfRunningGraphs || 0;
    this.numberOfRunningGraphs = numberOfRunningGraphs;

    const snapshot = this.resolveRuntimeStatusSnapshot(
      numberOfRunningGraphs,
      localInstance.isActive,
      localInstance.isNonResponsive,
      localInstance.isBlocked,
    );
    const reportedAt = new Date().toISOString();

    const report: RuntimeStatusReport = {
      serviceName: this.serviceName,
      serviceInstanceId: this.serviceInstanceId,
      transportId:
        this.getRouteableTransport(
          localInstance,
          this.useSocket ? "socket" : "rest",
          "internal",
        )?.uuid ?? undefined,
      transportOrigin:
        this.getRouteableTransport(
          localInstance,
          this.useSocket ? "socket" : "rest",
          "internal",
        )?.origin ?? undefined,
      transportProtocols:
        this.getRouteableTransport(
          localInstance,
          this.useSocket ? "socket" : "rest",
          "internal",
        )?.protocols ?? undefined,
      isFrontend: localInstance.isFrontend,
      reportedAt,
      state: snapshot.state,
      acceptingWork: snapshot.acceptingWork,
      numberOfRunningGraphs: snapshot.numberOfRunningGraphs,
      isActive: snapshot.isActive,
      isNonResponsive: snapshot.isNonResponsive,
      isBlocked: snapshot.isBlocked,
      health: {
        ...(localInstance.health ?? {}),
        runtimeStatus: {
          state: snapshot.state,
          acceptingWork: snapshot.acceptingWork,
          reportedAt,
        },
      },
    };

    this.applyRuntimeStatusReport(report);
    if (detailLevel !== "full") {
      delete report.health;
    }

    return report;
  }

  private selectRuntimeStatusReportForTarget(
    inquiryResult: AnyObject,
    targetServiceName: string,
    targetServiceInstanceId: string,
  ): RuntimeStatusReport | null {
    const reports = Array.isArray(inquiryResult.runtimeStatusReports)
      ? inquiryResult.runtimeStatusReports
      : [];

    for (const candidate of reports) {
      const report = this.normalizeRuntimeStatusReport(candidate);
      if (!report) {
        continue;
      }

      if (
        report.serviceName === targetServiceName &&
        report.serviceInstanceId === targetServiceInstanceId
      ) {
        return report;
      }
    }

    return null;
  }

  private async resolveRuntimeStatusFallbackInquiry(
    serviceName: string,
    serviceInstanceId: string,
    options: {
      detailLevel?: "minimal" | "full";
      overallTimeoutMs?: number;
      perResponderTimeoutMs?: number;
      requireComplete?: boolean;
    } = {},
  ): Promise<{ report: RuntimeStatusReport; inquiryMeta: AnyObject }> {
    const instance = this.getInstance(serviceName, serviceInstanceId);
    const directStatusCheck = instance
      ? await this.requestRuntimeStatusViaRest(
          instance,
          serviceName,
          serviceInstanceId,
        )
      : {
          report: null,
          diagnostic: {
            attempted: false,
            outcome: "instance_missing",
          } satisfies RuntimeStatusFallbackRestDiagnostic,
        };

    if (directStatusCheck.report) {
      if (!this.applyRuntimeStatusReport(directStatusCheck.report)) {
        throw this.createRuntimeStatusFallbackError(
          `No tracked instance for runtime fallback ${serviceName}/${serviceInstanceId}`,
          {
            target: {
              serviceName,
              serviceInstanceId,
            },
            instance: this.summarizeInstanceForRuntimeStatusFallback(instance),
            directStatusCheck: directStatusCheck.diagnostic,
            inquiry: {
              meta: {},
              reportTargets: [],
            },
          },
        );
      }

      this.lastHeartbeatAtByInstance.set(serviceInstanceId, Date.now());
      this.missedHeartbeatsByInstance.set(serviceInstanceId, 0);

      return {
        report: directStatusCheck.report,
        inquiryMeta: {
          inquiry: META_RUNTIME_STATUS_INTENT,
          responded: 1,
          failed: 0,
          timedOut: 0,
          pending: 0,
          directStatusCheck: true,
        },
      };
    }

    const inquiryResult = await Cadenza.inquire(
      META_RUNTIME_STATUS_INTENT,
      {
        targetServiceName: serviceName,
        targetServiceInstanceId: serviceInstanceId,
        detailLevel: options.detailLevel ?? "minimal",
        __preferredTransportProtocol: "rest",
      },
      {
        overallTimeoutMs:
          options.overallTimeoutMs ?? this.runtimeStatusFallbackTimeoutMs,
        perResponderTimeoutMs:
          options.perResponderTimeoutMs ??
          Math.max(250, Math.floor(this.runtimeStatusFallbackTimeoutMs * 0.75)),
        requireComplete: options.requireComplete ?? false,
      },
    );

    const report = this.selectRuntimeStatusReportForTarget(
      inquiryResult,
      serviceName,
      serviceInstanceId,
    );

    if (!report) {
      throw this.createRuntimeStatusFallbackError(
        `No runtime status report for ${serviceName}/${serviceInstanceId}`,
        {
          target: {
            serviceName,
            serviceInstanceId,
          },
          instance: this.summarizeInstanceForRuntimeStatusFallback(instance),
          directStatusCheck: directStatusCheck.diagnostic,
          inquiry: {
            meta:
              inquiryResult.__inquiryMeta &&
              typeof inquiryResult.__inquiryMeta === "object"
                ? inquiryResult.__inquiryMeta
                : {},
            reportTargets: this.summarizeRuntimeStatusInquiryReports(inquiryResult),
          },
        },
      );
    }

    if (!this.applyRuntimeStatusReport(report)) {
      throw this.createRuntimeStatusFallbackError(
        `No tracked instance for runtime fallback ${serviceName}/${serviceInstanceId}`,
        {
          target: {
            serviceName,
            serviceInstanceId,
          },
          instance: this.summarizeInstanceForRuntimeStatusFallback(instance),
          directStatusCheck: directStatusCheck.diagnostic,
          inquiry: {
            meta:
              inquiryResult.__inquiryMeta &&
              typeof inquiryResult.__inquiryMeta === "object"
                ? inquiryResult.__inquiryMeta
                : {},
            reportTargets: this.summarizeRuntimeStatusInquiryReports(inquiryResult),
          },
        },
      );
    }

    this.lastHeartbeatAtByInstance.set(serviceInstanceId, Date.now());
    this.missedHeartbeatsByInstance.set(serviceInstanceId, 0);

    return {
      report,
      inquiryMeta: inquiryResult.__inquiryMeta ?? {},
    };
  }

  private async requestRuntimeStatusViaRest(
    instance: ServiceInstanceDescriptor,
    serviceName: string,
    serviceInstanceId: string,
  ): Promise<{
    report: RuntimeStatusReport | null;
    diagnostic: RuntimeStatusFallbackRestDiagnostic;
  }> {
    if (typeof globalThis.fetch !== "function") {
      return {
        report: null,
        diagnostic: {
          attempted: false,
          outcome: "fetch_unavailable",
        },
      };
    }

    const transport = this.getRouteableTransport(instance, "rest");
    if (!transport) {
      return {
        report: null,
        diagnostic: {
          attempted: false,
          outcome: "no_rest_transport",
        },
      };
    }

    const controller =
      typeof AbortController === "function" ? new AbortController() : null;
    const timeoutId = controller
      ? setTimeout(() => controller.abort(), this.runtimeStatusFallbackTimeoutMs)
      : null;

    try {
      const response = await globalThis.fetch(`${transport.origin}/status`, {
        method: "GET",
        signal: controller?.signal,
      });

      if ("ok" in response && response.ok === false) {
        return {
          report: null,
          diagnostic: {
            attempted: true,
            outcome: "http_error",
            transport: this.summarizeTransportForDebug(transport),
            responseStatus:
              typeof response.status === "number" ? response.status : undefined,
            responseStatusText:
              typeof response.statusText === "string"
                ? response.statusText
                : undefined,
          },
        };
      }

      const payload =
        typeof response.json === "function" ? await response.json() : response;
      const report = this.normalizeRuntimeStatusReport({
        ...payload,
        serviceTransportId: payload?.serviceTransportId ?? transport.uuid,
        serviceOrigin: payload?.serviceOrigin ?? transport.origin,
        transportProtocols: payload?.transportProtocols ?? transport.protocols,
      });

      if (!report) {
        return {
          report: null,
          diagnostic: {
            attempted: true,
            outcome: "invalid_report",
            transport: this.summarizeTransportForDebug(transport),
            payloadKeys:
              payload && typeof payload === "object"
                ? Object.keys(payload as Record<string, unknown>).sort()
                : [],
          },
        };
      }

      if (
        report.serviceName !== serviceName ||
        report.serviceInstanceId !== serviceInstanceId
      ) {
        return {
          report: null,
          diagnostic: {
            attempted: true,
            outcome: "identity_mismatch",
            transport: this.summarizeTransportForDebug(transport),
            payloadServiceName: report.serviceName,
            payloadServiceInstanceId: report.serviceInstanceId,
          },
        };
      }

      return {
        report,
        diagnostic: {
          attempted: true,
          outcome: "matched",
          transport: this.summarizeTransportForDebug(transport),
        },
      };
    } catch (error) {
      return {
        report: null,
        diagnostic: {
          attempted: true,
          outcome: "fetch_error",
          transport: this.summarizeTransportForDebug(transport),
          error: error instanceof Error ? error.message : String(error),
        },
      };
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  }

  private evaluateDependencyReadinessDetail(
    serviceName: string,
    serviceInstanceId: string,
    now = Date.now(),
  ): DependencyReadinessDetail {
    const instance = this.getInstance(serviceName, serviceInstanceId);
    const missedHeartbeats = this.getHeartbeatMisses(serviceInstanceId, now);
    const runtimeState = instance
      ? (instance.runtimeState ??
        this.resolveRuntimeStatusSnapshot(
          instance.numberOfRunningGraphs ?? 0,
          instance.isActive,
          instance.isNonResponsive,
          instance.isBlocked,
        ).state)
      : "unavailable";
    const acceptingWork = instance
      ? (typeof instance.acceptingWork === "boolean"
        ? instance.acceptingWork
        : this.resolveRuntimeStatusSnapshot(
            instance.numberOfRunningGraphs ?? 0,
            instance.isActive,
            instance.isNonResponsive,
            instance.isBlocked,
          ).acceptingWork)
      : false;

    const evaluation = evaluateDependencyReadiness({
      exists: Boolean(instance),
      runtimeState,
      acceptingWork,
      missedHeartbeats,
      missThreshold: this.runtimeStatusMissThreshold,
    });

    const lastHeartbeat = this.lastHeartbeatAtByInstance.get(serviceInstanceId);
    return {
      serviceName,
      serviceInstanceId,
      dependencyState: evaluation.state,
      runtimeState,
      acceptingWork,
      missedHeartbeats,
      stale: evaluation.stale,
      blocked: evaluation.blocked,
      reason: evaluation.reason,
      lastHeartbeatAt: lastHeartbeat
        ? new Date(lastHeartbeat).toISOString()
        : null,
      reportedAt: instance?.reportedAt ?? null,
    };
  }

  private async buildLocalReadinessReport(
    options: {
      detailLevel?: "minimal" | "full";
      includeDependencies?: boolean;
      refreshStaleDependencies?: boolean;
    } = {},
  ): Promise<ReadinessReport | null> {
    const localRuntime = this.buildLocalRuntimeStatusReport("minimal");
    if (!localRuntime) {
      return null;
    }

    const detailLevel = options.detailLevel ?? "minimal";
    const includeDependencies =
      options.includeDependencies ?? detailLevel === "full";
    const refreshStaleDependencies = options.refreshStaleDependencies ?? true;
    const dependencyPairs = Array.from(this.readinessDependeesByService.entries())
      .flatMap(([serviceName, instanceIds]) =>
        Array.from(instanceIds).map((serviceInstanceId) => ({
          serviceName,
          serviceInstanceId,
        })),
      )
      .sort((left, right) => {
        if (left.serviceName !== right.serviceName) {
          return left.serviceName.localeCompare(right.serviceName);
        }
        return left.serviceInstanceId.localeCompare(right.serviceInstanceId);
      });

    if (refreshStaleDependencies) {
      for (const dependency of dependencyPairs) {
        const misses = this.getHeartbeatMisses(dependency.serviceInstanceId);
        if (misses < this.runtimeStatusMissThreshold) {
          continue;
        }

        if (
          this.runtimeStatusFallbackInFlightByInstance.has(
            dependency.serviceInstanceId,
          )
        ) {
          continue;
        }

        this.runtimeStatusFallbackInFlightByInstance.add(
          dependency.serviceInstanceId,
        );
        try {
          await this.resolveRuntimeStatusFallbackInquiry(
            dependency.serviceName,
            dependency.serviceInstanceId,
          );
        } catch (error) {
          Cadenza.log(
            "Readiness dependency fallback failed.",
            {
              serviceName: dependency.serviceName,
              serviceInstanceId: dependency.serviceInstanceId,
              error: error instanceof Error ? error.message : String(error),
            },
            "warning",
          );
        } finally {
          this.runtimeStatusFallbackInFlightByInstance.delete(
            dependency.serviceInstanceId,
          );
        }
      }
    }

    const now = Date.now();
    const dependencyDetails = dependencyPairs.map((dependency) =>
      this.evaluateDependencyReadinessDetail(
        dependency.serviceName,
        dependency.serviceInstanceId,
        now,
      ),
    );
    const dependencySummary = summarizeDependencyReadiness(
      dependencyDetails.map((detail) => ({
        state: detail.dependencyState,
        stale: detail.stale,
        blocked: detail.blocked,
        reason: detail.reason,
      })),
    );
    const readinessState = resolveServiceReadinessState(
      localRuntime.state,
      localRuntime.acceptingWork,
      dependencySummary,
    );

    return {
      serviceName: localRuntime.serviceName,
      serviceInstanceId: localRuntime.serviceInstanceId,
      reportedAt: new Date(now).toISOString(),
      readinessState,
      runtimeState: localRuntime.state,
      acceptingWork: localRuntime.acceptingWork,
      dependencySummary,
      ...(includeDependencies ? { dependencies: dependencyDetails } : {}),
    };
  }

  /**
   * Initializes a private constructor for managing service instances, remote signals,
   * service health, and handling updates or synchronization tasks. The constructor
   * creates a variety of meta tasks that process different lifecycle events and
   * service-related updates in a distributed service registry model.
   *
   * @return {Object} An instance of the constructed class with initialized tasks
   *                  and state management necessary to process service-related events.
   */
  private constructor() {
    Cadenza.defineIntent({
      name: META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT,
      description:
        "Gather transport diagnostics across all services and communication clients.",
      input: {
        type: "object",
        properties: {
          detailLevel: {
            type: "string",
            constraints: {
              oneOf: ["summary", "full"],
            },
          },
          includeErrorHistory: {
            type: "boolean",
          },
          errorHistoryLimit: {
            type: "number",
            constraints: {
              min: 1,
              max: 200,
            },
          },
        },
      },
      output: {
        type: "object",
        properties: {
          transportDiagnostics: {
            type: "object",
          },
        },
      },
    });

    Cadenza.defineIntent({
      name: META_RUNTIME_STATUS_INTENT,
      description:
        "Gather lightweight runtime status reports from services in the distributed runtime.",
      input: {
        type: "object",
        properties: {
          detailLevel: {
            type: "string",
            constraints: {
              oneOf: ["minimal", "full"],
            },
          },
          targetServiceName: {
            type: "string",
          },
          targetServiceInstanceId: {
            type: "string",
          },
        },
      },
      output: {
        type: "object",
        properties: {
          runtimeStatusReports: {
            type: "array",
          },
        },
      },
    });

    Cadenza.createMetaTask(
      "Respond runtime status inquiry",
      (ctx) => {
        const targetServiceName = ctx.targetServiceName;
        const targetServiceInstanceId = ctx.targetServiceInstanceId;
        const detailLevel: "minimal" | "full" =
          ctx.detailLevel === "full" ? "full" : "minimal";
        const report = this.buildLocalRuntimeStatusReport(detailLevel);
        if (!report) {
          return {};
        }

        if (
          targetServiceName &&
          targetServiceName !== report.serviceName
        ) {
          return {};
        }

        if (
          targetServiceInstanceId &&
          targetServiceInstanceId !== report.serviceInstanceId
        ) {
          return {};
        }

        return {
          runtimeStatusReports: [report],
        };
      },
      "Responds to runtime-status inquiries with local service instance status.",
    ).respondsTo(META_RUNTIME_STATUS_INTENT);

    Cadenza.defineIntent({
      name: META_READINESS_INTENT,
      description:
        "Gather service readiness reports derived from local runtime status and required dependees.",
      input: {
        type: "object",
        properties: {
          detailLevel: {
            type: "string",
            constraints: {
              oneOf: ["minimal", "full"],
            },
          },
          includeDependencies: {
            type: "boolean",
          },
          refreshStaleDependencies: {
            type: "boolean",
          },
          targetServiceName: {
            type: "string",
          },
          targetServiceInstanceId: {
            type: "string",
          },
        },
      },
      output: {
        type: "object",
        properties: {
          readinessReports: {
            type: "array",
          },
        },
      },
    });

    Cadenza.createMetaTask(
      "Respond readiness inquiry",
      async (ctx) => {
        const targetServiceName = ctx.targetServiceName;
        const targetServiceInstanceId = ctx.targetServiceInstanceId;
        const report = await this.buildLocalReadinessReport({
          detailLevel: ctx.detailLevel === "full" ? "full" : "minimal",
          includeDependencies: ctx.includeDependencies,
          refreshStaleDependencies: ctx.refreshStaleDependencies,
        });
        if (!report) {
          return {};
        }

        if (targetServiceName && targetServiceName !== report.serviceName) {
          return {};
        }

        if (
          targetServiceInstanceId &&
          targetServiceInstanceId !== report.serviceInstanceId
        ) {
          return {};
        }

        return {
          readinessReports: [report],
        };
      },
      "Responds to distributed readiness inquiries using required dependee health.",
    ).respondsTo(META_READINESS_INTENT);

    this.handleInstanceUpdateTask = Cadenza.createMetaTask(
      "Handle Instance Update",
      (ctx, emit) => {
        const serviceInstance = normalizeServiceInstanceDescriptor(
          ctx.serviceInstance ??
            ctx.data ??
            ctx.queryData?.data ??
            (ctx.__serviceInstanceId || ctx.serviceInstanceId
              ? {
                  uuid: ctx.__serviceInstanceId ?? ctx.serviceInstanceId,
                  serviceName: ctx.__serviceName ?? ctx.serviceName,
                  isFrontend: !!ctx.isFrontend,
                  isActive:
                    typeof ctx.isActive === "boolean"
                      ? ctx.isActive
                      : typeof ctx.__active === "boolean"
                        ? ctx.__active
                        : true,
                  isNonResponsive: !!ctx.isNonResponsive,
                  isBlocked: !!ctx.isBlocked,
                  health: (ctx.health ?? ctx.__health ?? {}) as AnyObject,
                  numberOfRunningGraphs:
                    ctx.numberOfRunningGraphs ?? ctx.__numberOfRunningGraphs ?? 0,
                  isPrimary: false,
                  isBootstrapPlaceholder: !!ctx.isBootstrapPlaceholder,
                  transports: ctx.transports ?? [],
                }
              : undefined),
        );
        if (!serviceInstance) {
          return false;
        }
        const uuid = serviceInstance.uuid;
        const serviceName = serviceInstance.serviceName;
        const deleted = Boolean(
          ctx.deleted ?? ctx.serviceInstance?.deleted ?? ctx.data?.deleted,
        );
        if (uuid === this.serviceInstanceId) return;

        if (deleted) {
          const existingInstance = this.instances
            .get(serviceName)
            ?.find((instance) => instance.uuid === uuid);
          const indexToDelete =
            this.instances.get(serviceName)?.findIndex((i) => i.uuid === uuid) ?? -1;
          if (indexToDelete >= 0 && existingInstance) {
            this.instances.get(serviceName)?.splice(indexToDelete, 1);
            for (const transport of existingInstance.transports) {
              const transportKey = buildTransportClientKey(transport);
              emit(`meta.socket_shutdown_requested:${transportKey}`, {});
              emit(`meta.fetch.destroy_requested:${transportKey}`, {});
            }
          }

          if (this.instances.get(serviceName)?.length === 0) {
            this.instances.delete(serviceName);
          }

          this.unregisterDependee(uuid, serviceName);

          return;
        }

        if (!this.instances.has(serviceName))
          this.instances.set(serviceName, []);
        const instances = this.instances.get(serviceName)!;
        const existing = instances.find((i) => i.uuid === uuid);

        if (existing) {
          Object.assign(existing, {
            ...serviceInstance,
            transports:
              serviceInstance.transports.length > 0
                ? serviceInstance.transports
                : existing.transports,
            clientCreatedTransportIds: existing.clientCreatedTransportIds ?? [],
          });
        } else {
          instances.push(serviceInstance);
        }

        const trackedInstance =
          existing ?? instances.find((instance) => instance.uuid === uuid);
        if (trackedInstance) {
          const snapshot = this.resolveRuntimeStatusSnapshot(
            trackedInstance.numberOfRunningGraphs ?? 0,
            trackedInstance.isActive,
            trackedInstance.isNonResponsive,
            trackedInstance.isBlocked,
          );
          trackedInstance.runtimeState = snapshot.state;
          trackedInstance.acceptingWork = snapshot.acceptingWork;
          trackedInstance.reportedAt =
            trackedInstance.reportedAt ?? new Date().toISOString();
        }

        if (!serviceInstance.isBootstrapPlaceholder) {
          this.reconcileBootstrapPlaceholderInstance(serviceName, uuid, emit);
        }

        if (this.serviceName === serviceName) {
          return false;
        }

        const trackedTransport = this.getRouteableTransport(
          trackedInstance!,
          this.useSocket ? "socket" : "rest",
        );

        if (
          (!serviceInstance.isFrontend &&
            (this.deputies.has(serviceName) ||
              this.remoteIntents.has(serviceName))) ||
          this.remoteSignals.has(serviceName)
        ) {
          const communicationTypes = Array.from(
            new Set(
              this.deputies
                .get(serviceName)
                ?.map((d) => d.communicationType) ?? [],
            ),
          );

          if (
            !communicationTypes.includes("signal") &&
            this.remoteSignals.has(serviceName)
          ) {
            communicationTypes.push("signal");
          }

          if (trackedTransport) {
            const clientCreated = this.hasTransportClientCreated(
              trackedInstance!,
              trackedTransport.uuid,
            );

            if (!clientCreated) {
              emit("meta.service_registry.dependee_registered", {
                serviceName,
                serviceInstanceId: uuid,
                serviceTransportId: trackedTransport.uuid,
                serviceOrigin: trackedTransport.origin,
                transportProtocols: trackedTransport.protocols,
                communicationTypes,
              });
              this.markTransportClientCreated(
                trackedInstance!,
                trackedTransport.uuid,
              );
            }
          } else {
            emit("meta.service_registry.routeable_transport_missing", {
              serviceName,
              serviceInstanceId: uuid,
              requiredRole: this.getRoutingTransportRole(),
              isFrontend: this.isFrontend,
            });
          }
        }

        return true;
      },
      "Handles instance updates to service instances",
    )
      .emits("meta.service_registry.service_discovered")
      .doOn(
        "meta.initializing_service",
        "global.meta.service_instance.inserted",
        "global.meta.service_instance.updated",
        "meta.service_instance.inserted",
        "meta.service_instance.updated",
      )
      .attachSignal(
        "meta.service_registry.dependee_registered",
        "meta.socket_shutdown_requested",
        "meta.fetch.destroy_requested",
      );

    this.handleTransportUpdateTask = Cadenza.createMetaTask(
      "Handle Transport Update",
      (ctx, emit) => {
        const transport = normalizeServiceTransportDescriptor(
          ctx.serviceTransport ?? ctx.data ?? ctx.queryData?.data ?? ctx,
        );
        if (!transport) {
          return false;
        }

        let ownerInstance: ServiceInstanceDescriptor | undefined;
        for (const instances of this.instances.values()) {
          ownerInstance = instances.find(
            (instance) => instance.uuid === transport.serviceInstanceId,
          );
          if (ownerInstance) {
            break;
          }
        }

        if (!ownerInstance) {
          return false;
        }

        if (transport.deleted) {
          ownerInstance.transports = ownerInstance.transports.filter(
            (existingTransport) => existingTransport.uuid !== transport.uuid,
          );
          const transportKey = buildTransportClientKey(transport);
          emit(`meta.socket_shutdown_requested:${transportKey}`, {});
          emit(`meta.fetch.destroy_requested:${transportKey}`, {});
          return true;
        }

        const existingTransport = this.getTransportById(ownerInstance, transport.uuid);
        if (existingTransport) {
          Object.assign(existingTransport, transport);
        } else {
          ownerInstance.transports.push(transport);
        }

        if (ownerInstance.uuid === this.serviceInstanceId) {
          return true;
        }

        const hasRemoteInterest =
          ((!ownerInstance.isFrontend &&
            (this.deputies.has(ownerInstance.serviceName) ||
              this.remoteIntents.has(ownerInstance.serviceName))) ||
            this.remoteSignals.has(ownerInstance.serviceName)) &&
          transport.role === this.getRoutingTransportRole();

        if (!hasRemoteInterest) {
          return true;
        }

        if (!this.hasTransportClientCreated(ownerInstance, transport.uuid)) {
          const communicationTypes = Array.from(
            new Set(
              this.deputies
                .get(ownerInstance.serviceName)
                ?.map((descriptor) => descriptor.communicationType) ?? [],
            ),
          );

          if (
            !communicationTypes.includes("signal") &&
            this.remoteSignals.has(ownerInstance.serviceName)
          ) {
            communicationTypes.push("signal");
          }

          emit("meta.service_registry.dependee_registered", {
            serviceName: ownerInstance.serviceName,
            serviceInstanceId: ownerInstance.uuid,
            serviceTransportId: transport.uuid,
            serviceOrigin: transport.origin,
            transportProtocols: transport.protocols,
            communicationTypes,
          });
          this.markTransportClientCreated(ownerInstance, transport.uuid);
        }

        return true;
      },
      "Handles service transport updates independently from instance rows.",
    )
      .doOn(
        "global.meta.service_instance_transport.inserted",
        "global.meta.service_instance_transport.updated",
        "meta.service_instance_transport.inserted",
        "meta.service_instance_transport.updated",
      )
      .attachSignal(
        "meta.service_registry.dependee_registered",
        "meta.socket_shutdown_requested",
        "meta.fetch.destroy_requested",
      );

    Cadenza.createMetaTask(
      "Track dependee registration",
      (ctx) => {
        if (!ctx.serviceName || !ctx.serviceInstanceId) {
          return false;
        }

        this.registerDependee(ctx.serviceName, ctx.serviceInstanceId, {
          requiredForReadiness: this.shouldRequireReadinessFromCommunicationTypes(
            ctx.communicationTypes,
          ),
        });
        return true;
      },
      "Tracks remote dependency instances for runtime heartbeat monitoring.",
    ).doOn("meta.service_registry.dependee_registered");

    Cadenza.createMetaTask("Split service instances", function* (ctx: any) {
      if (!ctx.serviceInstances) {
        return;
      }

      for (const serviceInstance of ctx.serviceInstances) {
        yield { serviceInstance };
      }
    })
      .doOn(
        "meta.service_registry.registered_global_signals",
        "meta.service_registry.registered_global_intents",
      )
      .then(this.handleInstanceUpdateTask);

    this.handleGlobalSignalRegistrationTask = Cadenza.createMetaTask(
      "Handle global Signal Registration",
      (ctx) => {
        const { signalToTaskMaps } = ctx;
        const sortedSignalToTaskMap = signalToTaskMaps.sort(
          (a: any, b: any) => {
            if (a.deleted && !b.deleted) return -1;
            if (!a.deleted && b.deleted) return 1;
            return 0;
          },
        );

        const locallyEmittedSignals = Cadenza.signalBroker
          .listEmittedSignals()
          .filter((s: any) => s.startsWith("global."));

        for (const map of sortedSignalToTaskMap) {
          if (map.deleted) {
            this.remoteSignals.get(map.serviceName)?.delete(map.signalName);

            if (!this.remoteSignals.get(map.serviceName)?.size) {
              this.remoteSignals.delete(map.serviceName);
            }

            Cadenza.get(
              `Transmit signal: ${map.signalName} to ${map.serviceName}`,
            )?.destroy();
            continue;
          }

          if (locallyEmittedSignals.includes(map.signalName)) {
            if (!this.remoteSignals.get(map.serviceName)) {
              this.remoteSignals.set(map.serviceName, new Set());
            }

            if (!this.remoteSignals.get(map.serviceName)?.has(map.signalName)) {
              Cadenza.createSignalTransmissionTask(
                map.signalName,
                map.serviceName,
              );
            }

            this.remoteSignals.get(map.serviceName)?.add(map.signalName);
          }
        }

        return true;
      },
      "Handles registration of remote signals",
    )
      .emits("meta.service_registry.registered_global_signals")
      .doOn("global.meta.cadenza_db.gathered_sync_data");

    this.handleGlobalIntentRegistrationTask = Cadenza.createMetaTask(
      "Handle global intent registration",
      (ctx) => {
        const intentToTaskMaps = this.normalizeIntentMaps(ctx);
        const sorted = intentToTaskMaps.sort((a, b) => {
          if (a.deleted && !b.deleted) return -1;
          if (!a.deleted && b.deleted) return 1;
          return 0;
        });

        for (const map of sorted) {
          if (map.deleted) {
            this.unregisterRemoteIntentDeputy(map);
            continue;
          }

          Cadenza.inquiryBroker.addIntent({
            name: map.intentName,
          });

          this.registerRemoteIntentDeputy(map);
        }

        return true;
      },
      "Handles registration of remote inquiry intent responders",
    )
      .emits("meta.service_registry.registered_global_intents")
      .doOn(
        "global.meta.cadenza_db.gathered_sync_data",
        "global.meta.graph_metadata.task_intent_associated",
      );

    this.handleServiceNotRespondingTask = Cadenza.createMetaTask(
      "Handle service not responding",
      (ctx, emit) => {
        const { serviceName, serviceInstanceId, serviceTransportId } = ctx;
        const serviceInstances = this.instances.get(serviceName);
        const instances = serviceInstances?.filter((instance) => {
          if (serviceInstanceId && instance.uuid === serviceInstanceId) {
            return true;
          }

          if (serviceTransportId) {
            return instance.transports.some(
              (transport) => transport.uuid === serviceTransportId,
            );
          }

          return false;
        });

        Cadenza.log(
          "Service not responding.",
          {
            serviceName,
            serviceInstanceId,
            serviceTransportId,
            instances,
          },
          "warning",
          serviceName,
        );

        for (const instance of instances ?? []) {
          instance.isActive = false;
          instance.isNonResponsive = true;
          const snapshot = this.resolveRuntimeStatusSnapshot(
            instance.numberOfRunningGraphs ?? 0,
            instance.isActive,
            instance.isNonResponsive,
            instance.isBlocked,
          );
          instance.runtimeState = snapshot.state;
          instance.acceptingWork = snapshot.acceptingWork;
          instance.reportedAt = new Date().toISOString();
          emit("global.meta.service_registry.service_not_responding", {
            data: {
              isActive: false,
              isNonResponsive: true,
            },
            filter: {
              uuid: instance.uuid,
            },
          });
        }

        return true;
      },
      "Handles service not responding",
    )
      .doOn(
        "meta.fetch.handshake_failed",
        "meta.fetch.handshake_failed.*",
        "meta.socket_client.disconnected",
        "meta.socket_client.disconnected.*",
        "meta.service_registry.runtime_status_unreachable",
      )
      .attachSignal("global.meta.service_registry.service_not_responding");

    this.handleServiceHandshakeTask = Cadenza.createMetaTask(
      "Handle service handshake",
      (ctx, emit) => {
        const { serviceName, serviceInstanceId } = ctx;
        const serviceInstances = this.instances.get(serviceName);
        const instance = serviceInstances?.find(
          (i) => i.uuid === serviceInstanceId,
        );

        if (!instance) {
          return false;
        }

        instance.isActive = true;
        instance.isNonResponsive = false;
        const snapshot = this.resolveRuntimeStatusSnapshot(
          instance.numberOfRunningGraphs ?? 0,
          instance.isActive,
          instance.isNonResponsive,
          instance.isBlocked,
        );
        instance.runtimeState = snapshot.state;
        instance.acceptingWork = snapshot.acceptingWork;
        instance.reportedAt = new Date().toISOString();
        emit("global.meta.service_registry.service_handshake", {
          data: {
            isActive: instance.isActive,
            isNonResponsive: instance.isNonResponsive,
          },
          filter: {
            uuid: instance.uuid,
          },
        });

        return true;
      },
      "Handles service handshake",
    )
      .doOn("meta.fetch.handshake_complete")
      .attachSignal(
        "global.meta.service_registry.service_handshake",
        "global.meta.service_registry.deleted",
      );

    this.handleSocketStatusUpdateTask = Cadenza.createMetaTask(
      "Handle Socket Status Update",
      (ctx) => {
        const report = this.normalizeRuntimeStatusReport(ctx);
        if (!report) {
          return false;
        }

        if (
          report.serviceName === this.serviceName &&
          report.serviceInstanceId === this.serviceInstanceId
        ) {
          return false;
        }

        let applied = this.applyRuntimeStatusReport(report);
        if (
          !applied &&
          report.transportId &&
          report.transportOrigin
        ) {
          if (!this.instances.has(report.serviceName)) {
            this.instances.set(report.serviceName, []);
          }

          this.instances.get(report.serviceName)!.push({
            uuid: report.serviceInstanceId,
            serviceName: report.serviceName,
            isFrontend: !!report.isFrontend,
            isActive: report.isActive,
            isNonResponsive: report.isNonResponsive,
            isBlocked: report.isBlocked,
            numberOfRunningGraphs: report.numberOfRunningGraphs,
            runtimeState: report.state,
            acceptingWork: report.acceptingWork,
            reportedAt: report.reportedAt,
            health: report.health ?? {},
            isPrimary: false,
            transports: [
              {
                uuid: report.transportId,
                serviceInstanceId: report.serviceInstanceId,
                role: this.getRoutingTransportRole(),
                origin: report.transportOrigin,
                protocols:
                  report.transportProtocols && report.transportProtocols.length > 0
                    ? report.transportProtocols
                    : (["rest", "socket"] as ServiceTransportProtocol[]),
                securityProfile: null,
                authStrategy: null,
              },
            ],
          });
          applied = true;
        }

        if (!applied) {
          return false;
        }

        this.registerDependee(report.serviceName, report.serviceInstanceId);
        this.lastHeartbeatAtByInstance.set(report.serviceInstanceId, Date.now());
        this.missedHeartbeatsByInstance.set(report.serviceInstanceId, 0);
        this.runtimeStatusFallbackInFlightByInstance.delete(
          report.serviceInstanceId,
        );
        return true;
      },
      "Handles status update from socket broadcast",
    ).doOn("meta.socket_client.status_received");

    this.fullSyncTask = Cadenza.createMetaTask(
      "Full sync",
      async (ctx) => {
        const inquiryResult = await Cadenza.inquire(
          META_SERVICE_REGISTRY_FULL_SYNC_INTENT,
          {
            syncScope: "service-registry-full-sync",
          },
          ctx.inquiryOptions ?? ctx.__inquiryOptions ?? {},
        );

        const signalToTaskMaps = (inquiryResult.signalToTaskMaps ?? [])
          .filter((m: any) => !!m.isGlobal)
          .map((m: any) => ({
            signalName: m.signalName,
            serviceName: m.serviceName,
            deleted: !!m.deleted,
          }));

        const intentToTaskMaps = (inquiryResult.intentToTaskMaps ?? []).map(
          (m: any) => ({
            intentName: m.intentName,
            taskName: m.taskName,
            taskVersion: m.taskVersion ?? 1,
            serviceName: m.serviceName,
            deleted: !!m.deleted,
          }),
        );

        const serviceInstances = (inquiryResult.serviceInstances ?? [])
        .map((instance: AnyObject) => normalizeServiceInstanceDescriptor(instance))
        .filter(
          (instance: ServiceInstanceDescriptor | null): instance is ServiceInstanceDescriptor =>
            !!instance &&
            !!instance.isActive &&
            !instance.isNonResponsive &&
              !instance.isBlocked,
          );

        return {
          ...ctx,
          signalToTaskMaps,
          intentToTaskMaps,
          serviceInstances,
          __inquiryMeta: inquiryResult.__inquiryMeta,
        };
      },
      "Runs service registry full sync through one distributed inquiry intent.",
    )
      .doOn("meta.sync_requested")
      .emits("meta.service_registry.initial_sync_complete")
      .then(
        this.handleGlobalSignalRegistrationTask,
        this.handleGlobalIntentRegistrationTask,
      );

    this.getInstanceById = Cadenza.createMetaTask(
      "Get instance by id",
      (context) => {
        const { __id } = context;
        let instance;
        for (const instances of this.instances.values()) {
          instance = instances.find((i) => i.uuid === __id);
          if (instance) break;
        }
        return { ...context, __instance: instance };
      },
      "Gets instance by id.",
    );

    this.getInstancesByServiceName = Cadenza.createMetaTask(
      "Get instances by name",
      (context) => {
        const { __serviceName } = context;
        const instances = this.instances.get(__serviceName);
        if (!instances) {
          return false;
        }

        return { ...context, __instances: instances };
      },
      "Gets instances by name.",
    );

    this.handleDeputyRegistrationTask = Cadenza.createMetaTask(
      "Handle Deputy Registration",
      (ctx) => {
        const { serviceName } = ctx;

        if (!this.deputies.has(serviceName)) this.deputies.set(serviceName, []);

        this.deputies.get(serviceName)!.push({
          serviceName,
          remoteRoutineName: ctx.remoteRoutineName,
          signalName: ctx.signalName,
          localTaskName: ctx.localTaskName,
          communicationType: ctx.communicationType,
        });
      },
    ).doOn("meta.deputy.created");

    this.getAllInstances = Cadenza.createMetaTask(
      "Get all instances",
      (context) => ({
        ...context,
        __instances: Array.from(this.instances.values()).flat(),
      }),
      "Gets all instances.",
    );

    this.doForEachInstance = Cadenza.createMetaTask(
      "Do for each instance",
      function* (context: AnyObject) {
        // @ts-ignore
        for (const instances of this.instances.values()) {
          for (const instance of instances) {
            yield { ...context, __instance: instance };
          }
        }
      }.bind(this),
      "Yields each instance for branching.",
    );

    this.deleteInstance = Cadenza.createMetaTask(
      "Delete instance",
      (context) => {
        const { __id } = context;
        this.instances.delete(__id);
        return context;
      },
      "Deletes instance.",
    ).doOn("global.meta.service_instance.deleted");

    this.getBalancedInstance = Cadenza.createMetaTask(
      "Get balanced instance",
      (context, emit) => {
        const {
          __serviceName,
          __triedInstances,
          __retries,
          __broadcast,
          targetServiceInstanceId,
        } = context;
        let retries = __retries ?? 0;
        let triedInstances = __triedInstances ?? [];
        const preferredRole = this.getRoutingTransportRole();

        const instances = this.instances
          .get(__serviceName)
          ?.filter((instance) => {
            if (
              targetServiceInstanceId &&
              instance.uuid !== targetServiceInstanceId
            ) {
              return false;
            }

            if (
              !instance.isActive ||
              instance.isNonResponsive ||
              instance.isBlocked
            ) {
              return false;
            }

            return Boolean(
              this.selectTransportForInstance(instance, context, preferredRole),
            );
          })
          .sort((a, b) => {
            const leftStatus = this.resolveRuntimeStatusSnapshot(
              a.numberOfRunningGraphs ?? 0,
              a.isActive,
              a.isNonResponsive,
              a.isBlocked,
            );
            const rightStatus = this.resolveRuntimeStatusSnapshot(
              b.numberOfRunningGraphs ?? 0,
              b.isActive,
              b.isNonResponsive,
              b.isBlocked,
            );

            const priorityDelta =
              runtimeStatusPriority(leftStatus.state) -
              runtimeStatusPriority(rightStatus.state);
            if (priorityDelta !== 0) {
              return priorityDelta;
            }

            return (
              (a.numberOfRunningGraphs ?? 0) - (b.numberOfRunningGraphs ?? 0)
            );
          });

        if (!instances || instances.length === 0 || retries > this.retryCount) {
          context.errored = true;
          context.__error =
            this.isFrontend && preferredRole === "public"
              ? `No public transport available for ${__serviceName}.`
              : `No routeable ${preferredRole} transport available for ${__serviceName}. Retries: ${retries}.`;
          emit(
            `meta.service_registry.load_balance_failed:${context.__metadata.__deputyExecId}`,
            context,
          );
          return context;
        }

        if (__broadcast || instances[0].isFrontend) {
          for (const instance of instances) {
            const selectedTransport = this.selectTransportForInstance(
              instance,
              context,
              preferredRole,
            );
            if (!selectedTransport) {
              continue;
            }

            const transportKey = buildTransportClientKey(selectedTransport);
            emit(
              `${
                this.resolveTransportProtocolOrder(context)[0] === "socket" &&
                transportSupportsProtocol(selectedTransport, "socket")
                  ? "meta.service_registry.selected_instance_for_socket"
                  : "meta.service_registry.selected_instance_for_fetch"
              }:${transportKey}`,
              {
                ...context,
                __instance: instance.uuid,
                __transportId: selectedTransport.uuid,
                __transportOrigin: selectedTransport.origin,
                __transportProtocols: selectedTransport.protocols,
                __fetchId: transportKey,
              },
            );
          }

          return context;
        }

        let instancesToTry = instances.filter(
          (i) => !__triedInstances?.includes(i.uuid),
        );

        if (instancesToTry.length === 0) {
          if (this.useSocket) {
            emit(
              `meta.service_registry.socket_failed:${context.__fetchId}`,
              context,
            );
          }
          retries++;
          instancesToTry = instances;
          triedInstances = [];
        }

        let selected = instancesToTry[0];
        if (retries > 0) {
          selected =
            instancesToTry[Math.floor(Math.random() * instancesToTry.length)];
        }

        const selectedTransport = this.selectTransportForInstance(
          selected,
          context,
          preferredRole,
        );

        if (!selectedTransport) {
          context.errored = true;
          context.__error = `No routeable ${preferredRole} transport available for ${selected.serviceName}/${selected.uuid}.`;
          emit(
            `meta.service_registry.load_balance_failed:${context.__metadata.__deputyExecId}`,
            context,
          );
          return context;
        }

        context.__instance = selected.uuid;
        context.__transportId = selectedTransport.uuid;
        context.__transportOrigin = selectedTransport.origin;
        context.__transportProtocols = selectedTransport.protocols;
        context.__fetchId = buildTransportClientKey(selectedTransport);
        context.__triedInstances = triedInstances;
        context.__triedInstances.push(selected.uuid);
        context.__retries = retries;

        if (
          this.resolveTransportProtocolOrder(context)[0] === "socket" &&
          transportSupportsProtocol(selectedTransport, "socket")
        ) {
          emit(
            `meta.service_registry.selected_instance_for_socket:${context.__fetchId}`,
            context,
          );
        } else {
          emit(
            `meta.service_registry.selected_instance_for_fetch:${context.__fetchId}`,
            context,
          );
        }

        return context;
      },
      "Gets a balanced instance for load balancing",
    )
      .doOn(
        "meta.deputy.delegation_requested",
        "meta.signal_transmission.requested",
        "meta.socket_client.delegate_failed",
        "meta.fetch.delegate_failed",
        "meta.socket_client.signal_transmission_failed",
      )
      .attachSignal(
        "meta.service_registry.load_balance_failed",
        "meta.service_registry.selected_instance_for_socket",
        "meta.service_registry.selected_instance_for_fetch",
        "meta.service_registry.socket_failed",
      );

    this.getStatusTask = Cadenza.createMetaTask(
      "Get status",
      (ctx) => this.resolveLocalStatusCheck(ctx),
    ).doOn(
      "meta.socket.status_check_requested",
      "meta.rest.status_check_requested",
    );

    Cadenza.createMetaTask(
      "Track local routine start",
      (ctx, emit) => {
        const sourceTaskName = String(ctx.__signalEmission?.taskName ?? "");
        if (INTERNAL_RUNTIME_STATUS_TASK_NAMES.has(sourceTaskName)) {
          return false;
        }

        const routineId = String(
          ctx.filter?.uuid ?? ctx.__routineExecId ?? "",
        );
        if (!routineId) {
          return false;
        }

        this.activeRoutineExecutionIds.add(routineId);
        this.numberOfRunningGraphs = this.activeRoutineExecutionIds.size;
        const localInstance = this.getLocalInstance();
        if (!localInstance) {
          return true;
        }

        const snapshot = this.resolveRuntimeStatusSnapshot(
          this.numberOfRunningGraphs,
          localInstance.isActive,
          localInstance.isNonResponsive,
          localInstance.isBlocked,
        );
        if (
          hasSignificantRuntimeStatusChange(this.lastRuntimeStatusSnapshot, snapshot)
        ) {
          emit("meta.service_registry.runtime_status_broadcast_requested", {
            reason: "runtime-state-change",
          });
        }
        return true;
      },
      "Tracks local routine starts for runtime load status.",
    ).doOn("meta.node.started_routine_execution");

    Cadenza.createMetaTask(
      "Track local routine end",
      (ctx, emit) => {
        const sourceTaskName = String(ctx.__signalEmission?.taskName ?? "");
        if (INTERNAL_RUNTIME_STATUS_TASK_NAMES.has(sourceTaskName)) {
          return false;
        }

        const routineId = String(
          ctx.filter?.uuid ?? ctx.__routineExecId ?? "",
        );
        if (!routineId) {
          return false;
        }

        this.activeRoutineExecutionIds.delete(routineId);
        this.numberOfRunningGraphs = this.activeRoutineExecutionIds.size;
        const localInstance = this.getLocalInstance();
        if (!localInstance) {
          return true;
        }

        const snapshot = this.resolveRuntimeStatusSnapshot(
          this.numberOfRunningGraphs,
          localInstance.isActive,
          localInstance.isNonResponsive,
          localInstance.isBlocked,
        );
        if (
          hasSignificantRuntimeStatusChange(this.lastRuntimeStatusSnapshot, snapshot)
        ) {
          emit("meta.service_registry.runtime_status_broadcast_requested", {
            reason: "runtime-state-change",
          });
        }
        return true;
      },
      "Tracks local routine completion for runtime load status.",
    ).doOn("meta.node.ended_routine_execution");

    Cadenza.createMetaTask(
      "Start runtime status sharing intervals",
      () => {
        if (this.runtimeStatusHeartbeatStarted) {
          return false;
        }

        this.runtimeStatusHeartbeatStarted = true;
        Cadenza.interval(
          META_RUNTIME_STATUS_HEARTBEAT_TICK_SIGNAL,
          { reason: "heartbeat" },
          this.runtimeStatusHeartbeatIntervalMs,
          true,
        );
        Cadenza.interval(
          META_RUNTIME_STATUS_MONITOR_TICK_SIGNAL,
          {},
          this.runtimeStatusHeartbeatIntervalMs,
        );
        return true;
      },
      "Starts runtime status heartbeat and heartbeat-monitor loops once per service instance.",
    ).doOn("meta.service_registry.instance_inserted");

    Cadenza.createMetaTask(
      "Broadcast runtime status",
      (ctx, emit) => {
        const report = this.buildLocalRuntimeStatusReport(
          ctx.detailLevel === "full" ? "full" : "minimal",
        );
        if (!report) {
          return false;
        }

        const snapshot = this.resolveRuntimeStatusSnapshot(
          report.numberOfRunningGraphs,
          report.isActive,
          report.isNonResponsive,
          report.isBlocked,
        );
        const force =
          ctx.reason === "heartbeat" ||
          ctx.force === true ||
          this.lastRuntimeStatusSnapshot === null;

        if (
          !force &&
          !hasSignificantRuntimeStatusChange(this.lastRuntimeStatusSnapshot, snapshot)
        ) {
          return false;
        }

        this.lastRuntimeStatusSnapshot = snapshot;
        emit("meta.service.updated", {
          __serviceName: report.serviceName,
          __serviceInstanceId: report.serviceInstanceId,
          __reportedAt: report.reportedAt,
          __numberOfRunningGraphs: report.numberOfRunningGraphs,
          __health: report.health ?? {},
          __active: report.isActive,
          serviceName: report.serviceName,
          serviceInstanceId: report.serviceInstanceId,
          transportId: report.transportId,
          transportOrigin: report.transportOrigin,
          transportProtocols: report.transportProtocols,
          isFrontend: report.isFrontend,
          reportedAt: report.reportedAt,
          numberOfRunningGraphs: report.numberOfRunningGraphs,
          health: report.health ?? {},
          isActive: report.isActive,
          isNonResponsive: report.isNonResponsive,
          isBlocked: report.isBlocked,
          state: report.state,
          acceptingWork: report.acceptingWork,
        });
        return true;
      },
      "Broadcasts local runtime status to connected dependees.",
    ).doOn(
      META_RUNTIME_STATUS_HEARTBEAT_TICK_SIGNAL,
      "meta.service_registry.runtime_status_broadcast_requested",
    );

    Cadenza.createMetaTask(
      "Monitor dependee heartbeat freshness",
      (ctx, emit) => {
        if (!this.useSocket) {
          return false;
        }

        const now = Date.now();
        for (const [serviceName, instanceIds] of this.dependeesByService) {
          for (const serviceInstanceId of instanceIds) {
            const instance = this.getInstance(serviceName, serviceInstanceId);
            if (!instance || !instance.isActive || instance.isBlocked) {
              continue;
            }

            const lastHeartbeat =
              this.lastHeartbeatAtByInstance.get(serviceInstanceId) ?? 0;
            const misses = this.missedHeartbeatsByInstance.get(serviceInstanceId) ?? 0;
            const heartbeatBudget =
              this.runtimeStatusHeartbeatIntervalMs * (misses + 1);

            if (lastHeartbeat > 0 && now - lastHeartbeat < heartbeatBudget) {
              continue;
            }

            const nextMisses = misses + 1;
            this.missedHeartbeatsByInstance.set(serviceInstanceId, nextMisses);

            if (
              nextMisses < this.runtimeStatusMissThreshold ||
              this.runtimeStatusFallbackInFlightByInstance.has(serviceInstanceId)
            ) {
              continue;
            }

            this.runtimeStatusFallbackInFlightByInstance.add(serviceInstanceId);
            const transport = this.getRouteableTransport(
              instance,
              this.useSocket ? "socket" : "rest",
            );
            emit("meta.service_registry.runtime_status_fallback_requested", {
              ...ctx,
              serviceName,
              serviceInstanceId,
              serviceTransportId: transport?.uuid,
              serviceOrigin: transport?.origin,
              transportProtocols: transport?.protocols,
            });
          }
        }

        return true;
      },
      "Monitors dependee heartbeat freshness and requests inquiry fallback after repeated misses.",
    ).doOn(META_RUNTIME_STATUS_MONITOR_TICK_SIGNAL);

    Cadenza.createMetaTask(
      "Resolve runtime status fallback inquiry",
      async (ctx, emit) => {
        const serviceName = ctx.serviceName;
        const serviceInstanceId = ctx.serviceInstanceId;
        if (!serviceName || !serviceInstanceId) {
          return false;
        }

        try {
          const { report, inquiryMeta } =
            await this.resolveRuntimeStatusFallbackInquiry(
              serviceName,
              serviceInstanceId,
              {
                detailLevel: ctx.detailLevel === "full" ? "full" : "minimal",
                overallTimeoutMs: ctx.overallTimeoutMs,
                perResponderTimeoutMs: ctx.perResponderTimeoutMs,
                requireComplete: ctx.requireComplete,
              },
            );

          return {
            ...ctx,
            runtimeStatusReport: report,
            __inquiryMeta: inquiryMeta,
          };
        } catch (error) {
          const instance = this.getInstance(serviceName, serviceInstanceId);
          const transport = instance
            ? this.getRouteableTransport(
                instance,
                this.useSocket ? "socket" : "rest",
              )
            : undefined;
          const message =
            error instanceof Error ? error.message : String(error);
          const diagnostics =
            error &&
            typeof error === "object" &&
            "runtimeStatusFallback" in error &&
            (error as AnyObject).runtimeStatusFallback &&
            typeof (error as AnyObject).runtimeStatusFallback === "object"
              ? (error as AnyObject).runtimeStatusFallback
              : undefined;

          Cadenza.log(
            "Runtime status fallback inquiry failed.",
            {
              serviceName,
              serviceInstanceId,
              error: message,
              diagnostics,
            },
            "warning",
          );

          emit("meta.service_registry.runtime_status_unreachable", {
            ...ctx,
            serviceName,
            serviceInstanceId,
            serviceTransportId: transport?.uuid ?? ctx.serviceTransportId,
            serviceOrigin: transport?.origin ?? ctx.serviceOrigin,
            transportProtocols: transport?.protocols ?? ctx.transportProtocols,
            __error: message,
            errored: true,
          });

          return {
            ...ctx,
            __error: message,
            errored: true,
          };
        } finally {
          this.runtimeStatusFallbackInFlightByInstance.delete(serviceInstanceId);
        }
      },
      "Runs runtime-status inquiry fallback for a dependee instance after missed heartbeats.",
    )
      .doOn("meta.service_registry.runtime_status_fallback_requested")
      .emits("meta.service_registry.runtime_status_fallback_resolved")
      .emitsOnFail("meta.service_registry.runtime_status_fallback_failed");

    this.collectReadinessTask = Cadenza.createMetaTask(
      "Collect distributed readiness",
      async (ctx) => {
        const inquiryResult = await Cadenza.inquire(
          META_READINESS_INTENT,
          {
            detailLevel: ctx.detailLevel === "full" ? "full" : "minimal",
            includeDependencies: ctx.includeDependencies,
            refreshStaleDependencies: ctx.refreshStaleDependencies,
            targetServiceName: ctx.targetServiceName,
            targetServiceInstanceId: ctx.targetServiceInstanceId,
          },
          ctx.inquiryOptions ?? ctx.__inquiryOptions ?? {},
        );

        return {
          ...ctx,
          ...inquiryResult,
        };
      },
      "Collects distributed readiness reports from services.",
    )
      .doOn("meta.service_registry.readiness_requested")
      .emits("meta.service_registry.readiness_collected")
      .emitsOnFail("meta.service_registry.readiness_failed");

    this.collectTransportDiagnosticsTask = Cadenza.createMetaTask(
      "Collect transport diagnostics",
      async (ctx) => {
        const inquiryResult = await Cadenza.inquire(
          META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT,
          {
            detailLevel: ctx.detailLevel,
            includeErrorHistory: ctx.includeErrorHistory,
            errorHistoryLimit: ctx.errorHistoryLimit,
          },
          ctx.inquiryOptions ?? ctx.__inquiryOptions ?? {},
        );

        return {
          ...ctx,
          ...inquiryResult,
        };
      },
      "Collects distributed transport diagnostics using inquiry responders.",
    )
      .doOn("meta.service_registry.transport_diagnostics_requested")
      .emits("meta.service_registry.transport_diagnostics_collected")
      .emitsOnFail("meta.service_registry.transport_diagnostics_failed");

    this.insertServiceTask = Cadenza.createCadenzaDBInsertTask(
      "service",
      {
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      },
      {
        // validateInputContext: true,
        inputSchema: {
          type: "object",
          properties: {
            data: {
              type: "object",
              properties: {
                name: {
                  type: "string",
                },
                description: {
                  type: "string",
                },
                display_name: {
                  type: "string",
                },
                is_meta: {
                  type: "boolean",
                },
              },
              required: ["name"],
            },
          },
          required: ["data"],
        },
        outputSchema: {
          type: "object",
          properties: {
            __serviceName: {
              type: "string",
            },
          },
          required: ["__serviceName"],
        },
        retryCount: 100,
        retryDelay: 10000,
        retryDelayMax: 60000,
        retryDelayFactor: 1.3,
      },
    )
      .emits("meta.service_registry.service_inserted")
      .emitsOnFail("meta.service_registry.service_insertion_failed");

    this.insertServiceInstanceTask = Cadenza.createCadenzaDBInsertTask(
      "service_instance",
      {},
      {
        inputSchema: {
          type: "object",
          properties: {
            data: {
              type: "object",
              properties: {
                uuid: {
                  type: "string",
                },
                process_pid: {
                  type: "number",
                },
                is_primary: {
                  type: "boolean",
                },
                service_name: {
                  type: "string",
                },
                is_active: {
                  type: "boolean",
                },
                is_frontend: {
                  type: "boolean",
                },
                is_database: {
                  type: "boolean",
                },
                is_non_responsive: {
                  type: "boolean",
                },
                is_blocked: {
                  type: "boolean",
                },
                health: {
                  type: "object",
                },
              },
              required: ["uuid", "process_pid", "service_name"],
            },
          },
          required: ["data"],
        },
        outputSchema: {
          type: "object",
          properties: {
            uuid: {
              type: "string",
            },
          },
          required: ["uuid"],
        },
        retryCount: 5,
        retryDelay: 1000,
      },
    )
      .doOn("meta.service_registry.instance_registration_requested")
      .then(
        Cadenza.createMetaTask(
          "Setup service",
          (ctx) => {
            const {
              serviceInstance,
              data,
              queryData,
              __useSocket,
              __retryCount,
              __isFrontend,
            } = ctx;
            const normalizedLocalInstance = normalizeServiceInstanceDescriptor({
              ...(serviceInstance ?? data ?? queryData?.data ?? {}),
              transports: ctx.transportData ?? [],
            });

            if (
              !normalizedLocalInstance?.uuid ||
              !normalizedLocalInstance.serviceName
            ) {
              return false;
            }

            this.serviceInstanceId = normalizedLocalInstance.uuid;
            this.instances.set(
              normalizedLocalInstance.serviceName,
              [{ ...normalizedLocalInstance }],
            );
            this.useSocket = __useSocket;
            this.retryCount = __retryCount;
            this.isFrontend =
              typeof __isFrontend === "boolean"
                ? __isFrontend
                : !!normalizedLocalInstance.isFrontend;
            console.log("SETUP SERVICE", this.serviceInstanceId);
            return true;
          },
          "Sets service instance id after insertion",
        )
          .emits("meta.service_registry.instance_inserted")
          .then(
            Cadenza.createMetaTask(
              "Prepare service transport inserts",
              function* (ctx: AnyObject, emit) {
                const transportData = Array.isArray(ctx.transportData)
                  ? ctx.transportData
                  : [];

                for (const transport of transportData) {
                  const transportContext = {
                    ...ctx,
                    data: {
                      ...transport,
                      service_instance_id:
                        transport.service_instance_id ?? ctx.__serviceInstanceId,
                    },
                  };
                  emit(
                    "meta.service_registry.transport_registration_requested",
                    transportContext,
                  );
                  yield transportContext;
                }
              },
              "Splits declared service transports into individual insert payloads.",
            ).attachSignal("meta.service_registry.transport_registration_requested"),
          ),
      );

    this.insertServiceTransportTask = Cadenza.createCadenzaDBInsertTask(
      "service_instance_transport",
      {
        onConflict: {
          target: ["service_instance_id", "role", "origin"],
          action: {
            do: "update",
            set: {
              protocols: "excluded",
              security_profile: "excluded",
              auth_strategy: "excluded",
              deleted: "false",
            },
          },
        },
      },
      {
        inputSchema: {
          type: "object",
          properties: {
            data: {
              type: "object",
              properties: {
                uuid: {
                  type: "string",
                },
                service_instance_id: {
                  type: "string",
                },
                role: {
                  type: "string",
                },
                origin: {
                  type: "string",
                },
                protocols: {
                  type: "array",
                  items: {
                    type: "string",
                  },
                },
                security_profile: {
                  type: "string",
                },
                auth_strategy: {
                  type: "string",
                },
              },
              required: ["uuid", "service_instance_id", "role", "origin"],
            },
          },
          required: ["data"],
        },
        outputSchema: {
          type: "object",
          properties: {
            uuid: {
              type: "string",
            },
          },
          required: ["uuid"],
        },
        retryCount: 5,
        retryDelay: 1000,
      },
    )
      .doOn("meta.service_registry.transport_registration_requested")
      .emits("meta.service_registry.transport_registered")
      .emitsOnFail("meta.service_registry.transport_registration_failed");

    Cadenza.createMetaTask(
      "Handle service creation",
      (ctx) => {
        if (!ctx.__cadenzaDBConnect) {
          ctx.__skipRemoteExecution = true;
        }

        if (isBrowser || ctx.__isFrontend) {
          Cadenza.createMetaTask("Prepare for signal sync", () => {
            return {};
          })
            // .doAfter(this.fullSyncTask)
            .then(
              Cadenza.createCadenzaDBQueryTask("signal_registry", {
                fields: ["name"],
                filter: {
                  global: true,
                },
              }).then(
                Cadenza.createMetaTask(
                  // TODO this is outdated. Fix it.
                  "Create signal transmission tasks",
                  (ctx, emit) => {
                    const signalRegistry = ctx.signalRegistry;
                    for (const signal of signalRegistry) {
                      emit("meta.service_registry.foreign_signal_registered", {
                        __emitterSignalName: signal.name,
                        __listenerServiceName: signal.serviceName,
                      });
                    }

                    return true;
                  },
                ).then(
                  Cadenza.createMetaTask("Connect to services", (ctx, emit) => {
                    const services: string[] = Array.from(
                      new Set(
                        ctx.signalRegistry.map((s: any) => s.serviceName),
                      ),
                    );
                    for (const service of services) {
                      const instances = this.instances
                        .get(service)!
                        .filter((i) => i.isActive);
                      for (const instance of instances) {
                        const transport = this.getRouteableTransport(
                          instance,
                          this.useSocket ? "socket" : "rest",
                        );
                        if (!transport) {
                          continue;
                        }

                        if (
                          !this.hasTransportClientCreated(instance, transport.uuid)
                        ) {
                          emit("meta.service_registry.dependee_registered", {
                            serviceName: service,
                            serviceInstanceId: instance.uuid,
                            serviceTransportId: transport.uuid,
                            serviceOrigin: transport.origin,
                            transportProtocols: transport.protocols,
                            communicationTypes: ["signal"],
                          });
                          this.markTransportClientCreated(instance, transport.uuid);
                        }
                      }
                    }
                    return {};
                  }),
                ),
              ),
            );
        }

        return ctx;
      },
      "Handles the request to create a service instance",
    )
      .doOn("meta.create_service_requested")
      .then(this.insertServiceTask);
  }

  reset() {
    this.instances.clear();
    this.deputies.clear();
    this.remoteSignals.clear();
    this.remoteIntents.clear();
    this.remoteIntentDeputiesByKey.clear();
    this.remoteIntentDeputiesByTask.clear();
    this.dependeesByService.clear();
    this.dependeeByInstance.clear();
    this.readinessDependeesByService.clear();
    this.readinessDependeeByInstance.clear();
    this.lastHeartbeatAtByInstance.clear();
    this.missedHeartbeatsByInstance.clear();
    this.runtimeStatusFallbackInFlightByInstance.clear();
    this.activeRoutineExecutionIds.clear();
    this.numberOfRunningGraphs = 0;
    this.runtimeStatusHeartbeatStarted = false;
    this.lastRuntimeStatusSnapshot = null;
    this.isFrontend = false;
  }
}
