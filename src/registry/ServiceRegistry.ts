import { GraphRoutine, Task } from "@cadenza.io/core";
import type { AnyObject } from "@cadenza.io/core";
import { v4 as uuid, validate as isUuid } from "uuid";
import Cadenza from "../Cadenza";
import DatabaseController from "@service-database-controller";
import { isBrowser } from "../utils/environment";
import { InquiryResponderDescriptor } from "../types/inquiry";
import type { ServiceInstanceDescriptor } from "../types/serviceRegistry";
import type { ServiceManifestSnapshot } from "../types/serviceManifest";
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
  buildTransportHandleKey,
  parseTransportHandleKey,
  normalizeServiceTransportDescriptor,
  transportSupportsProtocol,
} from "../utils/transport";
import {
  attachDelegationRequestSnapshot,
  ensureDelegationContextMetadata,
  restoreDelegationRequestSnapshot,
  stripDelegationRequestSnapshot,
} from "../utils/delegation";
import {
  buildServiceCommunicationEstablishedContext,
  buildServiceCommunicationRetryContext,
  META_SERVICE_COMMUNICATION_PERSIST_RETRY_SIGNAL,
  resolveServiceCommunicationPersistenceDescriptor,
  SERVICE_COMMUNICATION_PERSIST_RETRY_DELAYS_MS,
} from "../utils/serviceCommunication";
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
import {
  RuntimeMetricsSampler,
  isNodeRuntimeMetricsSupported,
  type RuntimeMetricsSnapshot,
} from "../utils/runtimeMetrics";
import {
  AUTHORITY_RUNTIME_STATUS_REPORT_INTENT,
  RUNTIME_STATUS_AUTHORITY_SYNC_REQUESTED_SIGNAL,
  buildAuthorityRuntimeStatusSignature,
  normalizeAuthorityRuntimeStatusReport,
  type AuthorityRuntimeStatusReport,
} from "./runtimeStatusContract";
import {
  AUTHORITY_BOOTSTRAP_INTENT_SPECS,
  AUTHORITY_BOOTSTRAP_FULL_SYNC_INTENT,
  AUTHORITY_BOOTSTRAP_FULL_SYNC_RESPONDER_TASK_NAME,
  AUTHORITY_BOOTSTRAP_SIGNAL_NAMES,
  getAuthorityBootstrapInsertIntentSpecForTable,
  getAuthorityBootstrapIntentSpec,
  isAuthorityBootstrapIntent,
} from "./authorityBootstrapControlPlane";
import {
  explodeServiceManifestSnapshots,
} from "./serviceManifest";
import {
  AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
  normalizeServiceManifestSnapshot,
  selectLatestServiceManifestSnapshots,
} from "./serviceManifestContract";

const META_SERVICE_REGISTRY_FULL_SYNC_INTENT =
  AUTHORITY_BOOTSTRAP_FULL_SYNC_INTENT;
const BOOTSTRAP_FULL_SYNC_RESPONDER_TASK_NAME =
  AUTHORITY_BOOTSTRAP_FULL_SYNC_RESPONDER_TASK_NAME;
const BOOTSTRAP_FULL_SYNC_TIMEOUT_MS = 120_000;
const META_SERVICE_INSTANCE_INSERT_RESOLVED_SIGNAL =
  "meta.service_registry.insert_resolved:service_instance";
const META_GATHERED_SYNC_TRANSMISSION_RECONCILE_SIGNAL =
  "meta.service_registry.gathered_sync_transmission_reconcile_requested";
const META_RUNTIME_STATUS_HEARTBEAT_TICK_SIGNAL =
  "meta.service_registry.runtime_status.heartbeat_tick";
const META_RUNTIME_STATUS_MONITOR_TICK_SIGNAL =
  "meta.service_registry.runtime_status.monitor_tick";
const META_RUNTIME_METRICS_SAMPLE_TICK_SIGNAL =
  "meta.service_registry.runtime_metrics.sample_tick";
const META_RUNTIME_STATUS_PEER_UPDATE_REQUESTED_SIGNAL =
  "meta.service_registry.runtime_status.peer_update_requested";

interface PeerRuntimeStatusSnapshot {
  state: RuntimeStatusSnapshot["state"];
  acceptingWork: boolean;
  numberOfRunningGraphs: number;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
  cpuUsage: number | null;
  memoryUsage: number | null;
  eventLoopLag: number | null;
}
const META_SERVICE_INSTANCE_ACTIVITY_OBSERVED_SIGNAL =
  "meta.service_registry.instance_activity_observed";
const META_REMOTE_SERVICE_ACTIVITY_OBSERVED_SIGNAL =
  "meta.service_registry.remote_activity_observed";
const META_SERVICE_INSTANCE_SHUTDOWN_REPORTED_SIGNAL =
  "meta.service_registry.instance_shutdown_reported";
const META_SERVICE_INSTANCE_SHUTDOWN_TRANSPORT_DEACTIVATION_SIGNAL =
  "meta.service_registry.instance_shutdown_transport_deactivation_requested";
const META_AUTHORITY_BOOTSTRAP_HANDSHAKE_REQUESTED_SIGNAL =
  "meta.service_registry.authority_bootstrap_handshake_requested";
const META_RUNTIME_STATUS_REST_REFRESH_TICK_SIGNAL =
  "meta.service_registry.runtime_status.rest_refresh_tick";
const AUTHORITY_BOOTSTRAP_HANDSHAKE_TIMEOUT_MS = 5_000;
const EARLY_FULL_SYNC_DELAYS_MS = [
  100,
  1500,
  5000,
  12000,
  25000,
  45000,
  70000,
] as const;
const MIN_BOOTSTRAP_FULL_SYNC_ATTEMPTS = 4;
const INTERNAL_RUNTIME_STATUS_TASK_NAMES = new Set([
  "Track local routine start",
  "Track local routine end",
  "Start runtime status sharing intervals",
  "Broadcast runtime status",
  "Flush local runtime status to authority",
  "Monitor dependee heartbeat freshness",
  "Refresh REST dependee runtime status",
  "Resolve runtime status fallback inquiry",
  "Respond runtime status inquiry",
  "Respond readiness inquiry",
  "Collect distributed readiness",
  "Get status",
]);
const SERVICE_REGISTRY_TRACE_SERVICE = (
  process.env.CADENZA_SERVICE_REGISTRY_TRACE_SERVICE ?? ""
).trim();

function shouldTraceServiceRegistry(serviceName: string | null | undefined): boolean {
  return (
    SERVICE_REGISTRY_TRACE_SERVICE.length > 0 &&
    serviceName === SERVICE_REGISTRY_TRACE_SERVICE
  );
}

function buildServiceRegistryInsertQueryData(
  ctx: AnyObject,
  queryData: Record<string, unknown>,
): Record<string, unknown> {
  const joinedContexts = Array.isArray((ctx as AnyObject).joinedContexts)
    ? ((ctx as AnyObject).joinedContexts as AnyObject[])
    : [];
  const getJoinedValue = (key: "data" | "batch" | "__registrationData") => {
    for (let index = joinedContexts.length - 1; index >= 0; index -= 1) {
      const joinedContext = joinedContexts[index];
      if (
        joinedContext &&
        typeof joinedContext === "object" &&
        (Object.prototype.hasOwnProperty.call(joinedContext, key) ||
          joinedContext[key] !== undefined)
      ) {
        return joinedContext[key];
      }
    }

    return undefined;
  };
  const registrationData =
    Object.prototype.hasOwnProperty.call(ctx, "__registrationData") &&
    ctx.__registrationData !== undefined
      ? ctx.__registrationData
      : getJoinedValue("__registrationData");
  const nextQueryData: Record<string, unknown> = {
    ...queryData,
  };
  if (!Object.prototype.hasOwnProperty.call(queryData, "onConflict")) {
    delete nextQueryData.onConflict;
  }
  const resolvedData =
    Object.prototype.hasOwnProperty.call(ctx, "data") || ctx.data !== undefined
      ? ctx.data
      : getJoinedValue("data");
  const resolvedBatch =
    Object.prototype.hasOwnProperty.call(ctx, "batch") || ctx.batch !== undefined
      ? ctx.batch
      : getJoinedValue("batch");
  const nextData =
    resolvedData !== undefined
      ? resolvedData && typeof resolvedData === "object"
        ? { ...resolvedData }
        : resolvedData
      : registrationData &&
          typeof registrationData === "object" &&
          !Array.isArray(registrationData)
        ? { ...registrationData }
        : registrationData;

  if (nextData !== undefined) {
    nextQueryData.data = nextData;
  }

  if (resolvedBatch !== undefined) {
    nextQueryData.batch = Array.isArray(resolvedBatch)
      ? resolvedBatch.map((row) =>
          row && typeof row === "object" ? { ...row } : row,
        )
      : resolvedBatch;
  }

  return nextQueryData;
}

function sanitizeServiceRegistryInsertExecutionContext(ctx: AnyObject): AnyObject {
  const sanitized = stripDelegationRequestSnapshot({
    ...ctx,
  });

  delete sanitized.__status;
  delete sanitized.__success;
  delete sanitized.__nextNodes;
  delete sanitized.rowCount;
  delete sanitized.error;
  delete sanitized.errored;
  delete sanitized.failed;
  delete sanitized.returnedValue;
  delete sanitized.queryData;
  delete sanitized.onConflict;

  return sanitized;
}

function clearTransientRoutingErrorState(context: AnyObject): void {
  delete context.errored;
  delete context.failed;
  delete context.__error;
  delete context.error;
  delete context.returnedValue;
}

function isPersistedUuid(value: unknown): value is string {
  return typeof value === "string" && isUuid(value.trim());
}

function getSelfBootstrapRegistrationRetrySignal(
  tableName: string,
): string | null {
  if (tableName === "service") {
    return "meta.create_service_requested";
  }

  if (tableName === "service_instance") {
    return "meta.service_registry.instance_registration_requested";
  }

  if (tableName === "service_instance_transport") {
    return "meta.service_registry.transport_registration_requested";
  }

  return null;
}

function resolveBootstrapAuthorityInsertResult(
  tableName: string,
  ctx: AnyObject,
  queryData: Record<string, unknown>,
  rawResult: unknown,
  emit: (signal: string, ctx: AnyObject) => void,
): AnyObject | unknown {
  const normalizedResult = normalizeServiceRegistryInsertResult(
    tableName,
    ctx,
    queryData,
    rawResult,
  );

  if (!normalizedResult || typeof normalizedResult !== "object") {
    return normalizedResult;
  }

  if (
    tableName === "service_instance" &&
    (normalizedResult as AnyObject).errored !== true &&
    (normalizedResult as AnyObject).failed !== true
  ) {
    emit(META_SERVICE_INSTANCE_INSERT_RESOLVED_SIGNAL, normalizedResult as AnyObject);
  }

  const resolvedResult = {
    ...(normalizedResult as AnyObject),
  };
  delete resolvedResult.__resolverRequestId;
  delete resolvedResult.__resolverOriginalContext;
  delete resolvedResult.__resolverQueryData;
  return resolvedResult;
}

function resolveServiceNameFromContext(
  ctx: AnyObject | null | undefined,
): string {
  const candidate =
    (typeof ctx?.serviceName === "string" ? ctx.serviceName : null) ??
    (typeof ctx?.__serviceName === "string" ? ctx.__serviceName : null) ??
    (typeof ctx?.serviceInstance?.serviceName === "string"
      ? ctx.serviceInstance.serviceName
      : null) ??
    (typeof ctx?.serviceInstance?.service_name === "string"
      ? ctx.serviceInstance.service_name
      : null) ??
    (typeof ctx?.data?.service_name === "string" ? ctx.data.service_name : null) ??
    (typeof ctx?.data?.serviceName === "string" ? ctx.data.serviceName : null);

  return candidate?.trim() ?? "";
}

function normalizeServiceRegistryInsertResult(
  tableName: string,
  ctx: AnyObject,
  queryData: Record<string, unknown>,
  rawResult: unknown,
): unknown {
  if (!rawResult || typeof rawResult !== "object") {
    return rawResult;
  }

  const result = { ...(rawResult as AnyObject) };
  delete result.__resolverOriginalContext;
  delete result.__resolverQueryData;
  const normalizedQueryData =
    result.queryData && typeof result.queryData === "object"
      ? { ...(result.queryData as AnyObject) }
      : { ...queryData };
  const resolvedData =
    result.data ??
    normalizedQueryData.data ??
    queryData.data ??
    ctx.data ??
    ctx.__registrationData;

  if (resolvedData !== undefined && result.data === undefined) {
    result.data = resolvedData;
  }

  if (
    resolvedData !== undefined &&
    (normalizedQueryData.data === undefined || normalizedQueryData.data === null)
  ) {
    normalizedQueryData.data = resolvedData;
  }

  result.queryData = normalizedQueryData;

  if (tableName === "service_instance") {
    const chainedSetupKeys = [
      "__transportData",
      "transportData",
      "__useSocket",
      "__retryCount",
      "__isFrontend",
    ] as const;

    for (const key of chainedSetupKeys) {
      if (result[key] === undefined && ctx[key] !== undefined) {
        result[key] = Array.isArray(ctx[key])
          ? (ctx[key] as unknown[]).map((entry) =>
              entry && typeof entry === "object"
                ? { ...(entry as AnyObject) }
                : entry,
            )
          : ctx[key];
      }
    }
  }

  if (tableName === "service") {
    const resolvedServiceName = String(
      ctx.__serviceName ??
        result.__serviceName ??
        (resolvedData as AnyObject | undefined)?.name ??
        (resolvedData as AnyObject | undefined)?.service_name ??
        "",
    ).trim();

    if (resolvedServiceName) {
      result.__serviceName = resolvedServiceName;
    }
  }

  const resolvedLocalServiceInstanceId = String(
    ctx.__serviceInstanceId ??
      (resolvedData as AnyObject | undefined)?.uuid ??
      (resolvedData as AnyObject | undefined)?.service_instance_id ??
      "",
  ).trim();

  if (resolvedLocalServiceInstanceId) {
    result.__serviceInstanceId = resolvedLocalServiceInstanceId;
  }

  if (
    tableName === "service_instance" ||
    tableName === "service_instance_transport"
  ) {
    const resolvedUuid = String(
      result.uuid ??
        (resolvedData as AnyObject | undefined)?.uuid ??
        ctx.__serviceInstanceId ??
        "",
    ).trim();

    if (resolvedUuid) {
      result.uuid = resolvedUuid;
    }
  }

  return result;
}

function resolveServiceInstanceRegistrationPayload(
  ctx: AnyObject,
  fallbackServiceName?: string | null,
  fallbackServiceInstanceId?: string | null,
): AnyObject | null {
  const instancePayloadKeys = [
    "uuid",
    "process_pid",
    "is_primary",
    "service_name",
    "is_database",
    "is_frontend",
    "is_blocked",
    "is_non_responsive",
    "is_active",
    "last_active",
    "health",
  ] as const;
  const candidateSources: unknown[] = [
    ctx.data,
    ctx.__registrationData,
    ctx.queryData?.data,
    ctx.__resolverQueryData?.data,
    ctx.__resolverOriginalContext?.data,
    ctx.__resolverOriginalContext?.__registrationData,
    ctx.__resolverOriginalContext?.queryData?.data,
  ];

  const isInstancePayloadCandidate = (candidate: unknown): candidate is AnyObject =>
    !!candidate &&
    typeof candidate === "object" &&
    !Array.isArray(candidate) &&
    instancePayloadKeys.some(
      (key) =>
        Object.prototype.hasOwnProperty.call(candidate as AnyObject, key) &&
        (candidate as AnyObject)[key] !== undefined,
    );

  let resolvedData: AnyObject | null = null;

  for (const candidate of candidateSources) {
    if (isInstancePayloadCandidate(candidate)) {
      resolvedData = {
        ...(candidate as AnyObject),
      };
      break;
    }
  }

  if (!resolvedData) {
    for (const candidate of candidateSources) {
      if (
        candidate &&
        typeof candidate === "object" &&
        !Array.isArray(candidate)
      ) {
        resolvedData = {
          ...(candidate as AnyObject),
        };
        break;
      }
    }
  }

  if (!resolvedData) {
    return null;
  }

  const resolvedUuid = String(
    resolvedData.uuid ??
      ctx.__serviceInstanceId ??
      ctx.__resolverOriginalContext?.__serviceInstanceId ??
      fallbackServiceInstanceId ??
      "",
  ).trim();
  const resolvedServiceName = String(
    resolvedData.service_name ??
      ctx.__serviceName ??
      ctx.__resolverOriginalContext?.__serviceName ??
      fallbackServiceName ??
      "",
  ).trim();
  const resolvedProcessPid =
    typeof resolvedData.process_pid === "number"
      ? resolvedData.process_pid
      : typeof ctx.__resolverOriginalContext?.data?.process_pid === "number"
        ? ctx.__resolverOriginalContext.data.process_pid
        : typeof ctx.__resolverOriginalContext?.__registrationData?.process_pid ===
              "number"
          ? ctx.__resolverOriginalContext.__registrationData.process_pid
          : typeof ctx.queryData?.data?.process_pid === "number"
            ? ctx.queryData.data.process_pid
            : typeof ctx.__registrationData?.process_pid === "number"
              ? ctx.__registrationData.process_pid
              : null;

  if (!resolvedUuid || !resolvedServiceName || resolvedProcessPid === null) {
    return null;
  }

  const payload: AnyObject = {
    uuid: resolvedUuid,
    service_name: resolvedServiceName,
    process_pid: resolvedProcessPid,
  };

  for (const key of instancePayloadKeys) {
    if (
      key === "uuid" ||
      key === "service_name" ||
      key === "process_pid" ||
      resolvedData[key] === undefined
    ) {
      continue;
    }

    const value = resolvedData[key];
    payload[key] =
      value && typeof value === "object" && !Array.isArray(value)
        ? { ...value }
        : value;
  }

  return payload;
}

function summarizeTransportDescriptors(
  transports: Array<
    Pick<
      ServiceTransportDescriptor,
      "uuid" | "serviceInstanceId" | "role" | "origin" | "protocols"
    >
  >,
) {
  return transports.map((transport) => ({
    uuid: transport.uuid,
    serviceInstanceId: transport.serviceInstanceId,
    role: transport.role,
    origin: transport.origin,
    protocols: transport.protocols,
  }));
}

function resolveServiceRegistryInsertTask(
  tableName: string,
  queryData: Record<string, unknown> = {},
  options: Record<string, unknown> = {},
): Task {
  const remoteInsertTask = Cadenza.createCadenzaDBInsertTask(
    tableName,
    queryData,
    options,
  );

  const localExecutionRequestedSignal = `meta.service_registry.insert_execution_requested:${tableName}:local`;
  const remoteExecutionRequestedSignal = `meta.service_registry.insert_execution_requested:${tableName}:remote`;
  const executionResolvedSignal = `meta.service_registry.insert_execution_resolved:${tableName}`;
  const executionFailedSignal = `meta.service_registry.insert_execution_failed:${tableName}`;

  const createPrepareExecutionTask = (signalName: string) =>
    Cadenza.createMetaTask(
      `Prepare service registry insert execution for ${tableName} (${signalName})`,
      (ctx) => {
        const sanitizedContext = sanitizeServiceRegistryInsertExecutionContext(
          ctx,
        );
        const nextQueryData = buildServiceRegistryInsertQueryData(
          sanitizedContext,
          queryData,
        );

        const delegationContext = ensureDelegationContextMetadata({
          ...sanitizedContext,
          data:
            nextQueryData.data !== undefined
              ? nextQueryData.data
              : sanitizedContext.data,
          batch:
            nextQueryData.batch !== undefined
              ? nextQueryData.batch
              : sanitizedContext.batch,
          onConflict:
            Object.prototype.hasOwnProperty.call(nextQueryData, "onConflict")
              ? nextQueryData.onConflict
              : undefined,
          transaction:
            nextQueryData.transaction !== undefined
              ? nextQueryData.transaction
              : sanitizedContext.transaction,
          queryData: nextQueryData,
        }) as AnyObject & {
          __deputyExecId: string;
          __metadata: AnyObject;
        };
        delegationContext.__metadata.__skipRemoteExecution =
          delegationContext.__metadata.__skipRemoteExecution ??
          delegationContext.__skipRemoteExecution ??
          false;
        delegationContext.__metadata.__blockRemoteExecution =
          delegationContext.__metadata.__blockRemoteExecution ??
          delegationContext.__blockRemoteExecution ??
          false;

        const nextContext = {
          ...delegationContext,
          __resolverOriginalContext: {
            ...sanitizedContext,
          },
          __resolverQueryData: nextQueryData,
        };

        if (
          tableName === "service_instance_transport" &&
          shouldTraceServiceRegistry(
            resolveServiceNameFromContext(sanitizedContext) || Cadenza.serviceRegistry.serviceName,
          )
        ) {
          console.log("[CADENZA_SERVICE_REGISTRY_TRACE] prepare_transport_insert_execution", {
            localServiceName: Cadenza.serviceRegistry.serviceName,
            signalName,
            serviceName:
              resolveServiceNameFromContext(sanitizedContext) ||
              Cadenza.serviceRegistry.serviceName,
            serviceInstanceId:
              sanitizedContext.__serviceInstanceId ??
              sanitizedContext.data?.service_instance_id ??
              null,
            data: nextQueryData.data ?? null,
          });
        }

        if (
          (tableName === "service_instance" || tableName === "service") &&
          (process.env.CADENZA_INSTANCE_DEBUG === "1" ||
            process.env.CADENZA_INSTANCE_DEBUG === "true")
        ) {
          console.log("[CADENZA_INSTANCE_DEBUG] prepare_service_registry_insert_execution", {
            tableName,
            signalName,
            resolverRequestId: ctx.__resolverRequestId ?? null,
            hasData: nextQueryData.data !== undefined,
            onConflictTarget: Array.isArray(
              (nextQueryData as AnyObject).onConflict?.target,
            )
              ? ((nextQueryData as AnyObject).onConflict.target as unknown[])
              : null,
            hasDelegationSnapshot:
              (sanitizedContext as AnyObject).__delegationRequestContext !== undefined,
            queryDataKeys: Object.keys(nextQueryData),
            dataKeys:
              nextQueryData.data && typeof nextQueryData.data === "object"
                ? Object.keys(nextQueryData.data as AnyObject)
                : [],
          });
        }

        return nextContext;
      },
      `Prepares ${tableName} service-registry insert payloads for runner execution.`,
      {
        register: false,
        isHidden: true,
      },
    )
      .doOn(signalName)
      .emitsOnFail(executionFailedSignal);

  const prepareLocalExecutionTask = createPrepareExecutionTask(
    localExecutionRequestedSignal,
  );
  const prepareRemoteExecutionTask = createPrepareExecutionTask(
    remoteExecutionRequestedSignal,
  );

  const wiredLocalTaskNames = new Set<string>();
  const wireExecutionTarget = (targetTask: Task, prepareTask: Task) => {
    targetTask.emits(executionResolvedSignal).emitsOnFail(executionFailedSignal);
    prepareTask.then(targetTask);
  };

  wireExecutionTarget(remoteInsertTask, prepareRemoteExecutionTask);

  return Cadenza.createMetaTask(
    `Resolve service registry insert for ${tableName}`,
    (ctx, emit) =>
      new Promise((resolve) => {
        const resolverRequestId = uuid();

        Cadenza.createEphemeralMetaTask(
          `Resolve service registry insert execution for ${tableName} (${resolverRequestId})`,
          (resultCtx) => {
            if (
              tableName === "service_instance" &&
              shouldTraceServiceRegistry(
                resolveServiceNameFromContext(
                  ((resultCtx.__resolverOriginalContext as AnyObject) ??
                    resultCtx ??
                    ctx) as AnyObject,
                ) || Cadenza.serviceRegistry.serviceName,
              )
            ) {
              console.log("[CADENZA_SERVICE_REGISTRY_TRACE] resolver_service_instance_signal", {
                localServiceName: Cadenza.serviceRegistry.serviceName,
                resolverRequestId,
                incomingResolverRequestId: resultCtx.__resolverRequestId ?? null,
                keys:
                  resultCtx && typeof resultCtx === "object"
                    ? Object.keys(resultCtx)
                    : [],
              });
            }

            if (
              (tableName === "service_instance" || tableName === "service") &&
              (process.env.CADENZA_INSTANCE_DEBUG === "1" ||
                process.env.CADENZA_INSTANCE_DEBUG === "true")
            ) {
              console.log("[CADENZA_INSTANCE_DEBUG] resolve_service_registry_insert_signal", {
                tableName,
                resolverRequestId,
                incomingResolverRequestId: resultCtx.__resolverRequestId ?? null,
                errored: resultCtx.errored === true,
                error: resultCtx.__error ?? null,
                keys:
                  resultCtx && typeof resultCtx === "object"
                    ? Object.keys(resultCtx)
                    : [],
              });
            }

            if (resultCtx.__resolverRequestId !== resolverRequestId) {
              return false;
            }

            const normalizedResult = normalizeServiceRegistryInsertResult(
              tableName,
              (resultCtx.__resolverOriginalContext as AnyObject) ?? ctx,
              (resultCtx.__resolverQueryData as Record<string, unknown>) ??
                resultCtx.queryData ??
                ctx.queryData ??
                {},
              resultCtx,
            );

            if (
              (tableName === "service_instance" || tableName === "service") &&
              (process.env.CADENZA_INSTANCE_DEBUG === "1" ||
                process.env.CADENZA_INSTANCE_DEBUG === "true")
            ) {
              console.log("[CADENZA_INSTANCE_DEBUG] finalize_service_registry_insert", {
                tableName,
                hasNormalized: !!normalizedResult,
                normalizedKeys:
                  normalizedResult && typeof normalizedResult === "object"
                    ? Object.keys(normalizedResult as AnyObject)
                    : [],
                uuid:
                  normalizedResult && typeof normalizedResult === "object"
                    ? (normalizedResult as AnyObject).uuid ??
                      (normalizedResult as AnyObject).data?.uuid ??
                      (normalizedResult as AnyObject).queryData?.data?.uuid ??
                      null
                    : null,
                serviceName:
                  normalizedResult && typeof normalizedResult === "object"
                    ? (normalizedResult as AnyObject).__serviceName ??
                      (normalizedResult as AnyObject).data?.service_name ??
                      (normalizedResult as AnyObject).queryData?.data?.service_name ??
                      null
                    : null,
                errored:
                  normalizedResult && typeof normalizedResult === "object"
                    ? (normalizedResult as AnyObject).errored === true
                    : false,
                error:
                  normalizedResult && typeof normalizedResult === "object"
                    ? (normalizedResult as AnyObject).__error ?? null
                    : null,
                inquiryMeta:
                  normalizedResult && typeof normalizedResult === "object"
                    ? (normalizedResult as AnyObject).__inquiryMeta ?? null
                    : null,
              });
            }

            if (
              normalizedResult &&
              typeof normalizedResult === "object" &&
              tableName === "service_instance_transport" &&
              shouldTraceServiceRegistry(
                resolveServiceNameFromContext(
                  ((resultCtx.__resolverOriginalContext as AnyObject) ??
                    resultCtx) as AnyObject,
                ) || Cadenza.serviceRegistry.serviceName,
              )
            ) {
              console.log("[CADENZA_SERVICE_REGISTRY_TRACE] finalize_transport_insert_execution", {
                localServiceName: Cadenza.serviceRegistry.serviceName,
                serviceName:
                  resolveServiceNameFromContext(
                    ((resultCtx.__resolverOriginalContext as AnyObject) ??
                      resultCtx) as AnyObject,
                  ) || Cadenza.serviceRegistry.serviceName,
                uuid:
                  (normalizedResult as AnyObject).uuid ??
                  (normalizedResult as AnyObject).data?.uuid ??
                  null,
                errored: (normalizedResult as AnyObject).errored === true,
                error: (normalizedResult as AnyObject).__error ?? null,
                data:
                  (normalizedResult as AnyObject).data ??
                  (normalizedResult as AnyObject).queryData?.data ??
                  null,
              });
            }

            if (
              normalizedResult &&
              typeof normalizedResult === "object" &&
              tableName === "service_instance"
            ) {
              const traceServiceName =
                resolveServiceNameFromContext(
                  ((resultCtx.__resolverOriginalContext as AnyObject) ??
                    resultCtx) as AnyObject,
                ) || Cadenza.serviceRegistry.serviceName;

              if (shouldTraceServiceRegistry(traceServiceName)) {
                console.log("[CADENZA_SERVICE_REGISTRY_TRACE] service_instance_insert_resolved", {
                  localServiceName: Cadenza.serviceRegistry.serviceName,
                  serviceName: traceServiceName,
                  serviceInstanceId:
                    (normalizedResult as AnyObject).__serviceInstanceId ??
                    (normalizedResult as AnyObject).uuid ??
                    (normalizedResult as AnyObject).data?.uuid ??
                    null,
                  hasTransportData:
                    Array.isArray((normalizedResult as AnyObject).__transportData) ||
                    Array.isArray((normalizedResult as AnyObject).transportData),
                  transportCount: Array.isArray(
                    (normalizedResult as AnyObject).__transportData,
                  )
                    ? (normalizedResult as AnyObject).__transportData.length
                    : Array.isArray((normalizedResult as AnyObject).transportData)
                      ? (normalizedResult as AnyObject).transportData.length
                      : 0,
                  errored: (normalizedResult as AnyObject).errored === true,
                  error: (normalizedResult as AnyObject).__error ?? null,
                });
              }

              emit(
                META_SERVICE_INSTANCE_INSERT_RESOLVED_SIGNAL,
                normalizedResult as AnyObject,
              );
            }

            if (!normalizedResult || typeof normalizedResult !== "object") {
              resolve(normalizedResult as any);
              return normalizedResult as any;
            }

            const resolvedResult = {
              ...(normalizedResult as AnyObject),
            };
            delete resolvedResult.__resolverRequestId;
            delete resolvedResult.__resolverOriginalContext;
            delete resolvedResult.__resolverQueryData;

            resolve(resolvedResult);
            return resolvedResult;
          },
          `Resolves signal-driven ${tableName} service-registry insert execution.`,
          {
            register: false,
          },
        ).doOn(executionResolvedSignal, executionFailedSignal);

        const localInsertTask = Cadenza.getLocalCadenzaDBInsertTask(tableName);
        const bootstrapAuthorityInsertSpec =
          !localInsertTask && Cadenza.serviceRegistry.connectsToCadenzaDB
            ? getAuthorityBootstrapInsertIntentSpecForTable(tableName)
            : null;
        const resolvedTargetServiceName = resolveServiceNameFromContext(ctx);
        const selfBootstrapRetrySignal =
          Cadenza.serviceRegistry.serviceName === "CadenzaDB" &&
          resolvedTargetServiceName === "CadenzaDB" &&
          !localInsertTask
            ? getSelfBootstrapRegistrationRetrySignal(tableName)
            : null;

        if (selfBootstrapRetrySignal) {
          Cadenza.schedule(
            selfBootstrapRetrySignal,
            {
              ...ctx,
            },
            250,
          );
          resolve(false);
          return;
        }

        if (bootstrapAuthorityInsertSpec) {
          const sanitizedContext = sanitizeServiceRegistryInsertExecutionContext(ctx);
          const nextQueryData = buildServiceRegistryInsertQueryData(
            sanitizedContext,
            queryData,
          );
          const inquiryContext = ensureDelegationContextMetadata({
            ...sanitizedContext,
            data:
              nextQueryData.data !== undefined
                ? nextQueryData.data
                : sanitizedContext.data,
            batch:
              nextQueryData.batch !== undefined
                ? nextQueryData.batch
                : sanitizedContext.batch,
            onConflict:
              Object.prototype.hasOwnProperty.call(nextQueryData, "onConflict")
                ? nextQueryData.onConflict
                : undefined,
            transaction:
              nextQueryData.transaction !== undefined
                ? nextQueryData.transaction
                : sanitizedContext.transaction,
            queryData: nextQueryData,
          }) as AnyObject;

          inquiryContext.__metadata = {
            ...(inquiryContext.__metadata ?? {}),
            __skipRemoteExecution:
              inquiryContext.__metadata?.__skipRemoteExecution ??
              inquiryContext.__skipRemoteExecution ??
              false,
            __blockRemoteExecution:
              inquiryContext.__metadata?.__blockRemoteExecution ??
              inquiryContext.__blockRemoteExecution ??
              false,
          };

          Cadenza.serviceRegistry.ensureBootstrapAuthorityControlPlaneForInquiry(
            bootstrapAuthorityInsertSpec.intentName,
            inquiryContext,
          );

          void Cadenza.inquire(
            bootstrapAuthorityInsertSpec.intentName,
            inquiryContext,
            {
              requireComplete: true,
              timeout: bootstrapAuthorityInsertSpec.defaultTimeoutMs,
            },
          )
            .then((result) =>
              resolve(
                resolveBootstrapAuthorityInsertResult(
                  tableName,
                  sanitizedContext,
                  nextQueryData,
                  result,
                  emit,
                ) as any,
              ),
            )
            .catch((error) =>
              resolve(
                resolveBootstrapAuthorityInsertResult(
                  tableName,
                  sanitizedContext,
                  nextQueryData,
                  error,
                  emit,
                ) as any,
              ),
            );
          return;
        }

        const executionSignal = localInsertTask
          ? localExecutionRequestedSignal
          : remoteExecutionRequestedSignal;

        if (
          (tableName === "service_instance" || tableName === "service") &&
          (process.env.CADENZA_INSTANCE_DEBUG === "1" ||
            process.env.CADENZA_INSTANCE_DEBUG === "true")
        ) {
          console.log("[CADENZA_INSTANCE_DEBUG] resolve_service_registry_insert", {
            tableName,
            executionSignal,
            hasLocalInsertTask: !!localInsertTask,
            serviceName: ctx.__serviceName ?? ctx.data?.service_name ?? null,
            serviceInstanceId: ctx.__serviceInstanceId ?? ctx.data?.uuid ?? null,
            hasData: !!ctx.data,
            dataKeys:
              ctx.data && typeof ctx.data === "object"
                ? Object.keys(ctx.data)
                : [],
            registrationKeys:
              ctx.__registrationData &&
              typeof ctx.__registrationData === "object"
                ? Object.keys(ctx.__registrationData)
                : [],
          });
        }

        if (
          localInsertTask &&
          !wiredLocalTaskNames.has(localInsertTask.name)
        ) {
          wireExecutionTarget(localInsertTask, prepareLocalExecutionTask);
          wiredLocalTaskNames.add(localInsertTask.name);
        }

        emit(executionSignal, {
          ...ctx,
          __resolverRequestId: resolverRequestId,
        });
      }),
    `Resolves ${tableName} inserts through the local CadenzaDB task when available.`,
    options,
  );
}

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

interface RemoteRouteBalancingState {
  selectionCount: number;
  lastSelectedAt: number | null;
  lastSuccessAt: number | null;
  lastFailureAt: number | null;
  failurePenaltyUntil: number | null;
  activeDispatchTokens: Set<string>;
}

interface RemoteRouteBalancingSnapshot {
  availability: "available" | "penalized";
  runtimeState: RuntimeStatusState;
  acceptingWork: boolean;
  numberOfRunningGraphs: number;
  inFlightDelegations: number;
  selectionCount: number;
  lastSelectedAt: number | null;
  lastSuccessAt: number | null;
  lastFailureAt: number | null;
  failurePenaltyUntil: number | null;
  cpuUsage: number | null;
  memoryUsage: number | null;
  eventLoopLag: number | null;
}

interface RouteSelectionCandidate {
  instance: ServiceInstanceDescriptor;
  selectedTransport: ServiceTransportDescriptor | undefined;
  routeKey: string | null;
  route: RemoteRouteRecord | null;
  snapshot: RemoteRouteBalancingSnapshot | null;
}

interface RemoteRouteRecord {
  key: string;
  serviceName: string;
  role: ServiceTransportRole;
  origin: string;
  protocols: ServiceTransportProtocol[];
  serviceInstanceId: string;
  serviceTransportId: string;
  generation: string;
  protocolState: Partial<
    Record<
      ServiceTransportProtocol,
      {
        clientCreated: boolean;
        clientPending: boolean;
        clientReady: boolean;
      }
    >
  >;
  balancing: RemoteRouteBalancingState;
  lastUpdatedAt: number;
}

interface AuthorityBootstrapRouteState {
  origin: string | null;
  role: ServiceTransportRole;
  routeKey: string | null;
  fetchId: string | null;
  serviceInstanceId: string | null;
  serviceTransportId: string | null;
  handshakeEstablished: boolean;
}

type RuntimeStatusReport = AuthorityRuntimeStatusReport;

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

interface RoutingCooldownState {
  serviceName: string;
  role: ServiceTransportRole;
  protocol: ServiceTransportProtocol;
  failureCount: number;
  lastFailureAt: number;
  cooldownUntil: number;
  reason: string;
}

interface TransportFailureState {
  serviceName: string;
  serviceInstanceId: string;
  transportId: string;
  failureCount: number;
  lastFailureAt: number;
  lastErrorKind: string;
}

interface LocalLifecycleFlushRuntimeState {
  lastSentSignature: string | null;
  lastAckAt: string | null;
  lastError: string | null;
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
  private remoteRoutesByKey: Map<string, RemoteRouteRecord> = new Map();
  private localInstanceSeed: ServiceInstanceDescriptor | null = null;
  private deputies: Map<string, DeputyDescriptor[]> = new Map();
  private remoteSignals: Map<string, Set<string>> = new Map();
  private remoteIntents: Map<string, Set<string>> = new Map();
  private gatheredSyncTransmissionServices: Set<string> = new Set();
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
  private runtimeStatusRestRefreshInFlightByInstance: Set<string> = new Set();
  private routingCooldownsByKey: Map<string, RoutingCooldownState> = new Map();
  private transportFailuresByKey: Map<string, TransportFailureState> = new Map();
  private activeRoutineExecutionIds: Set<string> = new Set();
  private runtimeStatusHeartbeatStarted = false;
  private runtimeMetricsSamplingStarted = false;
  private lastRuntimeStatusSnapshot: RuntimeStatusSnapshot | null = null;
  private lastPeerRuntimeStatusSnapshot: PeerRuntimeStatusSnapshot | null = null;
  private latestRuntimeMetricsSnapshot: RuntimeMetricsSnapshot | null = null;
  private readonly runtimeMetricsSampler = isNodeRuntimeMetricsSupported()
    ? new RuntimeMetricsSampler()
    : null;
  private readonly runtimeStatusHeartbeatIntervalMs = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_HEARTBEAT_MS",
    30_000,
  );
  private readonly runtimeMetricsSampleIntervalMs = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_METRICS_SAMPLE_MS",
    5_000,
  );
  private readonly runtimeStatusRestRefreshIntervalMs = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_REST_REFRESH_MS",
    this.runtimeMetricsSampleIntervalMs,
  );
  private readonly runtimeStatusMissThreshold = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_MISSED_HEARTBEATS",
    3,
  );
  private readonly runtimeStatusFallbackTimeoutMs = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_FALLBACK_TIMEOUT_MS",
    1_500,
  );
  private readonly runtimeStatusAuthorityReportTimeoutMs = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_AUTHORITY_REPORT_TIMEOUT_MS",
    5_000,
  );
  private readonly runtimeStatusInactiveMissThreshold = readPositiveIntegerEnv(
    "CADENZA_RUNTIME_STATUS_INACTIVE_MISSED_HEARTBEATS",
    6,
  );
  private readonly noRouteCooldownFailureThreshold = readPositiveIntegerEnv(
    "CADENZA_NO_ROUTE_COOLDOWN_FAILURE_THRESHOLD",
    3,
  );
  private readonly noRouteCooldownWindowMs = readPositiveIntegerEnv(
    "CADENZA_NO_ROUTE_COOLDOWN_WINDOW_MS",
    5_000,
  );
  private readonly noRouteCooldownMs = readPositiveIntegerEnv(
    "CADENZA_NO_ROUTE_COOLDOWN_MS",
    20_000,
  );
  private readonly transportFailureThreshold = readPositiveIntegerEnv(
    "CADENZA_TRANSPORT_FAILURE_THRESHOLD",
    2,
  );
  private readonly transportFailureWindowMs = readPositiveIntegerEnv(
    "CADENZA_TRANSPORT_FAILURE_WINDOW_MS",
    5_000,
  );
  private readonly routeFailurePenaltyMs = readPositiveIntegerEnv(
    "CADENZA_ROUTE_FAILURE_PENALTY_MS",
    5_000,
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
  connectsToCadenzaDB: boolean = false;
  private bootstrapFullSyncRetryTimer: ReturnType<typeof setTimeout> | null =
    null;
  private bootstrapFullSyncRetryIndex = 0;
  private bootstrapFullSyncRetryGeneration = 0;
  private bootstrapFullSyncSatisfied = false;
  private bootstrapFullSyncRetryReason: string | null = null;
  private knownGlobalSignalMaps: Map<
    string,
    {
      signalName: string;
      serviceName: string;
      isGlobal: boolean;
    }
  > = new Map();
  private authorityBootstrapRoute: AuthorityBootstrapRouteState = {
    origin: null,
    role: "internal",
    routeKey: null,
    fetchId: null,
    serviceInstanceId: null,
    serviceTransportId: null,
    handshakeEstablished: false,
  };
  private authorityBootstrapHandshakeInFlight = false;

  handleInstanceUpdateTask: Task;
  handleTransportUpdateTask: Task;
  handleGlobalSignalRegistrationTask: Task;
  handleGlobalIntentRegistrationTask: Task;
  reconcileGatheredSyncTransmissionsTask: Task;
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
  private authorityFullSyncResponderTask: Task | null = null;
  private authorityServiceCommunicationPersistenceTask: Task | null = null;
  private readonly localLifecycleFlushActor = Cadenza.createActor<
    {},
    LocalLifecycleFlushRuntimeState | null
  >(
    {
      name: "ServiceLifecycleFlushActor",
      description:
        "Coalesces local runtime-status flushes so each service instance sends at most one lightweight authority lifecycle report at a time.",
      defaultKey: "service-lifecycle-flush-default",
      keyResolver: (input) =>
        String(
          input.serviceInstanceId ?? input.__serviceInstanceId ?? "",
        ).trim() || undefined,
      initState: {},
      session: {
        enabled: true,
        persistDurableState: false,
        idleTtlMs: Math.max(60_000, this.runtimeStatusHeartbeatIntervalMs * 4),
      },
    },
    { isMeta: true },
  );

  private collectBootstrapFullSyncPayload(
    ctx: AnyObject,
  ): {
    serviceInstances: Array<Record<string, unknown>>;
    serviceInstanceTransports: Array<Record<string, unknown>>;
    serviceManifests: Array<Record<string, unknown>>;
    tasks: Array<Record<string, unknown>>;
    signals: Array<Record<string, unknown>>;
    intents: Array<Record<string, unknown>>;
    actors: Array<Record<string, unknown>>;
    routines: Array<Record<string, unknown>>;
    directionalTaskMaps: Array<Record<string, unknown>>;
    actorTaskMaps: Array<Record<string, unknown>>;
    taskToRoutineMaps: Array<Record<string, unknown>>;
    signalToTaskMaps: Array<Record<string, unknown>>;
    intentToTaskMaps: Array<Record<string, unknown>>;
  } {
    const serviceInstances: Array<Record<string, unknown>> = [];
    const serviceInstanceTransports: Array<Record<string, unknown>> = [];
    const manifestSnapshots: ServiceManifestSnapshot[] = [];
    const signalToTaskMaps: Array<Record<string, unknown>> = [];
    const intentToTaskMaps: Array<Record<string, unknown>> = [];
    const tasks: Array<Record<string, unknown>> = [];
    const signals: Array<Record<string, unknown>> = [];
    const intents: Array<Record<string, unknown>> = [];
    const actors: Array<Record<string, unknown>> = [];
    const routines: Array<Record<string, unknown>> = [];
    const directionalTaskMaps: Array<Record<string, unknown>> = [];
    const actorTaskMaps: Array<Record<string, unknown>> = [];
    const taskToRoutineMaps: Array<Record<string, unknown>> = [];
    const seenServiceInstances = new Set<string>();
    const seenServiceInstanceTransports = new Set<string>();
    const seenSignalMaps = new Set<string>();
    const seenIntentMaps = new Set<string>();
    const seenTasks = new Set<string>();
    const seenSignals = new Set<string>();
    const seenIntents = new Set<string>();
    const seenActors = new Set<string>();
    const seenRoutines = new Set<string>();
    const seenDirectionalTaskMaps = new Set<string>();
    const seenActorTaskMaps = new Set<string>();
    const seenTaskToRoutineMaps = new Set<string>();
    const contexts = Array.isArray(ctx.joinedContexts)
      ? [ctx, ...ctx.joinedContexts]
      : [ctx];

    const pushUnique = (
      rows: Array<Record<string, unknown>>,
      target: Array<Record<string, unknown>>,
      seen: Set<string>,
      keyResolver: (row: Record<string, unknown>) => string,
    ) => {
      for (const row of rows) {
        const key = keyResolver(row);
        if (!key || seen.has(key)) {
          continue;
        }

        seen.add(key);
        target.push(row);
      }
    };

    const normalizeRows = (value: unknown): Array<Record<string, unknown>> =>
      Array.isArray(value)
        ? value.filter(
            (row): row is Record<string, unknown> =>
              !!row && typeof row === "object",
          )
        : [];
    const readDirectArrayPayload = (
      candidate: AnyObject,
      keys: string[],
    ): Array<Record<string, unknown>> => {
      for (const key of keys) {
        const value = candidate?.[key];
        if (Array.isArray(value)) {
          return normalizeRows(value);
        }
      }

      return [];
    };

    for (const candidate of contexts) {
      const serviceInstanceRows = readDirectArrayPayload(candidate, [
          "serviceInstances",
          "service_instances",
          "serviceInstance",
          "service_instance",
        ]);
      const serviceInstanceTransportRows = readDirectArrayPayload(candidate, [
          "serviceInstanceTransports",
          "service_instance_transports",
          "serviceInstanceTransport",
          "service_instance_transport",
        ]);
      const serviceManifestRows = readDirectArrayPayload(candidate, [
          "serviceManifests",
          "service_manifests",
          "serviceManifest",
          "service_manifest",
        ]);
      const signalMapRows = readDirectArrayPayload(candidate, [
          "signalToTaskMaps",
          "signal_to_task_maps",
          "signalToTaskMap",
          "signal_to_task_map",
        ]);
      const intentMapRows = readDirectArrayPayload(candidate, [
          "intentToTaskMaps",
          "intent_to_task_maps",
          "intentToTaskMap",
          "intent_to_task_map",
        ]);

      pushUnique(
        serviceInstanceRows,
        serviceInstances,
        seenServiceInstances,
        (row) => String(row.uuid ?? "").trim(),
      );
      pushUnique(
        serviceInstanceTransportRows,
        serviceInstanceTransports,
        seenServiceInstanceTransports,
        (row) => String(row.uuid ?? "").trim(),
      );
      for (const row of serviceManifestRows) {
        const snapshot = normalizeServiceManifestSnapshot(
          row.manifest && typeof row.manifest === "object" ? row.manifest : row,
        );
        if (snapshot) {
          manifestSnapshots.push(snapshot);
        }
      }
      pushUnique(
        signalMapRows,
        signalToTaskMaps,
        seenSignalMaps,
        (row) =>
          `${String(row.service_name ?? row.serviceName ?? "").trim()}|${String(
            row.signal_name ?? row.signalName ?? "",
          ).trim()}`,
      );
      pushUnique(
        intentMapRows,
        intentToTaskMaps,
        seenIntentMaps,
        (row) =>
          `${String(row.intent_name ?? row.intentName ?? "").trim()}|${String(
            row.service_name ?? row.serviceName ?? "",
          ).trim()}|${String(row.task_name ?? row.taskName ?? "").trim()}|${String(
            row.task_version ?? row.taskVersion ?? 1,
          ).trim()}`,
      );

      const rawRows = normalizeRows((candidate as AnyObject).rows);
      for (const row of rawRows) {
        if (row.intent_name !== undefined || row.intentName !== undefined) {
          pushUnique(
            [row],
            intentToTaskMaps,
            seenIntentMaps,
            (entry) =>
              `${String(entry.intent_name ?? entry.intentName ?? "").trim()}|${String(
                entry.service_name ?? entry.serviceName ?? "",
              ).trim()}|${String(entry.task_name ?? entry.taskName ?? "").trim()}|${String(
                entry.task_version ?? entry.taskVersion ?? 1,
              ).trim()}`,
          );
          continue;
        }

        if (row.signal_name !== undefined || row.signalName !== undefined) {
          pushUnique(
            [row],
            signalToTaskMaps,
            seenSignalMaps,
            (entry) =>
              `${String(entry.service_name ?? entry.serviceName ?? "").trim()}|${String(
                entry.signal_name ?? entry.signalName ?? "",
              ).trim()}`,
          );
          continue;
        }

        if (
          row.service_instance_id !== undefined ||
          row.serviceInstanceId !== undefined
        ) {
          pushUnique(
            [row],
            serviceInstanceTransports,
            seenServiceInstanceTransports,
            (entry) => String(entry.uuid ?? "").trim(),
          );
          continue;
        }

        if (
          row.manifest !== undefined &&
          (row.service_instance_id !== undefined ||
            row.serviceInstanceId !== undefined ||
            row.uuid !== undefined)
        ) {
          const snapshot = normalizeServiceManifestSnapshot(
            row.manifest && typeof row.manifest === "object" ? row.manifest : row,
          );
          if (snapshot) {
            manifestSnapshots.push(snapshot);
          }
          continue;
        }

        if (
          row.uuid !== undefined &&
          (row.service_name !== undefined || row.serviceName !== undefined)
        ) {
          pushUnique(
            [row],
            serviceInstances,
            seenServiceInstances,
            (entry) => String(entry.uuid ?? "").trim(),
          );
        }
      }
    }

    const hasExplicitSignalRoutingRows = signalToTaskMaps.length > 0;
    const hasExplicitIntentRoutingRows = intentToTaskMaps.length > 0;
    const latestManifestSnapshots =
      selectLatestServiceManifestSnapshots(manifestSnapshots);
    const explodedManifest = explodeServiceManifestSnapshots(
      latestManifestSnapshots,
    );
    const serviceManifests = latestManifestSnapshots.map((snapshot) => ({
      service_instance_id: snapshot.serviceInstanceId,
      service_name: snapshot.serviceName,
      revision: snapshot.revision,
      manifest_hash: snapshot.manifestHash,
      published_at: snapshot.publishedAt,
      manifest: snapshot,
    }));

    pushUnique(
      explodedManifest.tasks as Array<Record<string, unknown>>,
      tasks,
      seenTasks,
      (row) =>
        `${String(row.service_name ?? "").trim()}|${String(row.name ?? "").trim()}|${String(
          row.version ?? 1,
        ).trim()}`,
    );
    pushUnique(
      explodedManifest.signals as Array<Record<string, unknown>>,
      signals,
      seenSignals,
      (row) => String(row.name ?? "").trim(),
    );
    pushUnique(
      explodedManifest.intents as Array<Record<string, unknown>>,
      intents,
      seenIntents,
      (row) => String(row.name ?? "").trim(),
    );
    pushUnique(
      explodedManifest.actors as Array<Record<string, unknown>>,
      actors,
      seenActors,
      (row) =>
        `${String(row.service_name ?? "").trim()}|${String(row.name ?? "").trim()}|${String(
          row.version ?? 1,
        ).trim()}`,
    );
    pushUnique(
      explodedManifest.routines as Array<Record<string, unknown>>,
      routines,
      seenRoutines,
      (row) =>
        `${String(row.service_name ?? "").trim()}|${String(row.name ?? "").trim()}|${String(
          row.version ?? 1,
        ).trim()}`,
    );
    pushUnique(
      explodedManifest.directionalTaskMaps as Array<Record<string, unknown>>,
      directionalTaskMaps,
      seenDirectionalTaskMaps,
      (row) =>
        `${String(row.predecessor_service_name ?? "").trim()}|${String(
          row.predecessor_task_name ?? "",
        ).trim()}|${String(row.predecessor_task_version ?? 1).trim()}|${String(
          row.service_name ?? "",
        ).trim()}|${String(row.task_name ?? "").trim()}|${String(
          row.task_version ?? 1,
        ).trim()}`,
    );
    pushUnique(
      explodedManifest.actorTaskMaps as Array<Record<string, unknown>>,
      actorTaskMaps,
      seenActorTaskMaps,
      (row) =>
        `${String(row.actor_name ?? "").trim()}|${String(row.actor_version ?? 1).trim()}|${String(
          row.service_name ?? "",
        ).trim()}|${String(row.task_name ?? "").trim()}|${String(
          row.task_version ?? 1,
        ).trim()}`,
    );
    pushUnique(
      explodedManifest.taskToRoutineMaps as Array<Record<string, unknown>>,
      taskToRoutineMaps,
      seenTaskToRoutineMaps,
      (row) =>
        `${String(row.routine_name ?? "").trim()}|${String(row.routine_version ?? 1).trim()}|${String(
          row.service_name ?? "",
        ).trim()}|${String(row.task_name ?? "").trim()}|${String(
          row.task_version ?? 1,
        ).trim()}`,
    );
    if (!hasExplicitSignalRoutingRows) {
      pushUnique(
        explodedManifest.signalToTaskMaps as Array<Record<string, unknown>>,
        signalToTaskMaps,
        seenSignalMaps,
        (row) =>
          `${String(row.signal_name ?? "").trim()}|${String(
            row.service_name ?? "",
          ).trim()}|${String(row.task_name ?? "").trim()}|${String(
            row.task_version ?? 1,
          ).trim()}`,
      );
    }
    if (!hasExplicitIntentRoutingRows) {
      pushUnique(
        explodedManifest.intentToTaskMaps as Array<Record<string, unknown>>,
        intentToTaskMaps,
        seenIntentMaps,
        (row) =>
          `${String(row.intent_name ?? "").trim()}|${String(
            row.service_name ?? "",
          ).trim()}|${String(row.task_name ?? "").trim()}|${String(
            row.task_version ?? 1,
          ).trim()}`,
      );
    }

    return {
      serviceInstances,
      serviceInstanceTransports,
      serviceManifests,
      tasks,
      signals,
      intents,
      actors,
      routines,
      directionalTaskMaps,
      actorTaskMaps,
      taskToRoutineMaps,
      signalToTaskMaps,
      intentToTaskMaps,
    };
  }

  private buildRemoteIntentDeputyKey(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion?: number;
  }): string {
    return `${map.intentName}|${map.serviceName}|${map.taskName}|${map.taskVersion ?? 1}`;
  }

  private readArrayPayload(
    ctx: AnyObject,
    keys: string[],
  ): Array<Record<string, unknown>> {
    for (const key of keys) {
      const value = (ctx as any)?.[key];
      if (Array.isArray(value)) {
        return value as Array<Record<string, unknown>>;
      }
    }

    if (Array.isArray((ctx as any)?.rows)) {
      return (ctx as any).rows as Array<Record<string, unknown>>;
    }

    if (Array.isArray((ctx as any)?.data)) {
      return (ctx as any).data as Array<Record<string, unknown>>;
    }

    return [];
  }

  private normalizeSignalMaps(ctx: AnyObject): Array<{
    signalName: string;
    serviceName: string;
    isGlobal: boolean;
    deleted?: boolean;
  }> {
    const arrayPayload = this.readArrayPayload(ctx, [
      "signalToTaskMaps",
      "signal_to_task_maps",
      "signalToTaskMap",
      "signal_to_task_map",
    ]);

    if (arrayPayload.length > 0) {
      return arrayPayload
        .map((map) => ({
          signalName: String(
            map.signalName ?? map.signal_name ?? "",
          ).trim(),
          serviceName: String(
            map.serviceName ?? map.service_name ?? "",
          ).trim(),
          isGlobal: Boolean(map.isGlobal ?? map.is_global ?? false),
          deleted: Boolean(map.deleted),
        }))
        .filter((map) => map.signalName && map.serviceName);
    }

    const single =
      (ctx as any).signalToTaskMap ??
      (ctx as any).signal_to_task_map ??
      (ctx as any).data ??
      (((ctx as any).signalName ?? (ctx as any).signal_name)
        ? ctx
        : undefined);

    if (!single || typeof single !== "object") {
      return [];
    }

    return [
      {
        signalName: String(
          (single as any).signalName ?? (single as any).signal_name ?? "",
        ).trim(),
        serviceName: String(
          (single as any).serviceName ?? (single as any).service_name ?? "",
        ).trim(),
        isGlobal: Boolean((single as any).isGlobal ?? (single as any).is_global ?? false),
        deleted: Boolean((single as any).deleted),
      },
    ].filter((map) => map.signalName && map.serviceName);
  }

  private rememberKnownGlobalSignalMaps(
    signalMaps: Array<{
      signalName: string;
      serviceName: string;
      isGlobal: boolean;
      deleted?: boolean;
    }>,
  ): void {
    for (const map of signalMaps) {
      if (!map.isGlobal) {
        continue;
      }

      const key = `${map.serviceName}::${map.signalName}`;
      if (map.deleted) {
        this.knownGlobalSignalMaps.delete(key);
        continue;
      }

      this.knownGlobalSignalMaps.set(key, {
        signalName: map.signalName,
        serviceName: map.serviceName,
        isGlobal: true,
      });
    }
  }

  private applyGlobalSignalRegistrations(
    signalMaps: Array<{
      signalName: string;
      serviceName: string;
      isGlobal: boolean;
      deleted?: boolean;
    }>,
  ): boolean {
    let changed = false;
    const sortedSignalToTaskMap = [...signalMaps].sort((a: any, b: any) => {
      if (a.deleted && !b.deleted) return -1;
      if (!a.deleted && b.deleted) return 1;
      return 0;
    });

    const locallyEmittedSignals = Cadenza.signalBroker
      .listEmittedSignals()
      .filter((s: any) => s.startsWith("global."));

    for (const map of sortedSignalToTaskMap) {
      if (map.deleted) {
        const removed = this.remoteSignals.get(map.serviceName)?.delete(map.signalName) ?? false;

        if (!this.remoteSignals.get(map.serviceName)?.size) {
          this.remoteSignals.delete(map.serviceName);
        }

        const existingTransmissionTask = Cadenza.get(
          `Transmit signal: ${map.signalName} to ${map.serviceName}`,
        );
        if (existingTransmissionTask) {
          existingTransmissionTask.destroy();
          changed = true;
        } else if (removed) {
          changed = true;
        }

        continue;
      }

      if (map.serviceName === this.serviceName) {
        continue;
      }

      const signalObservers = (Cadenza.signalBroker as any).signalObservers;
      if (!signalObservers?.has(map.signalName)) {
        Cadenza.signalBroker.addSignal(map.signalName);
      }

      const observer = signalObservers?.get(map.signalName);
      if (observer) {
        observer.registered = true;
        observer.registrationRequested = false;
      }

      if (!locallyEmittedSignals.includes(map.signalName)) {
        continue;
      }

      if (!this.remoteSignals.get(map.serviceName)) {
        this.remoteSignals.set(map.serviceName, new Set());
      }

      if (!this.remoteSignals.get(map.serviceName)?.has(map.signalName)) {
        Cadenza.createSignalTransmissionTask(map.signalName, map.serviceName);
        changed = true;
      }

      this.remoteSignals.get(map.serviceName)?.add(map.signalName);
    }

    return changed;
  }

  public reconcileKnownGlobalSignalRegistrations(): boolean {
    if (this.knownGlobalSignalMaps.size === 0) {
      return false;
    }

    return this.applyGlobalSignalRegistrations(
      Array.from(this.knownGlobalSignalMaps.values()),
    );
  }

  private normalizeIntentMaps(ctx: AnyObject): Array<{
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion: number;
    deleted?: boolean;
  }> {
    const arrayPayload = this.readArrayPayload(ctx, [
      "intentToTaskMaps",
      "intent_to_task_maps",
      "intentToTaskMap",
      "intent_to_task_map",
    ]);

    if (arrayPayload.length > 0) {
      return arrayPayload
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
      (ctx as any).intent_to_task_map ??
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

  private normalizeServiceInstancesFromSync(
    ctx: AnyObject,
  ): ServiceInstanceDescriptor[] {
    const rawTransports = this.readArrayPayload(ctx, [
      "serviceInstanceTransports",
      "service_instance_transports",
      "serviceInstanceTransport",
      "service_instance_transport",
    ])
      .map((transport) => normalizeServiceTransportDescriptor(transport))
      .filter(
        (transport): transport is ServiceTransportDescriptor =>
          !!transport && !transport.deleted,
      );

    const transportsByInstance = new Map<string, ServiceTransportDescriptor[]>();
    for (const transport of rawTransports) {
      if (!transportsByInstance.has(transport.serviceInstanceId)) {
        transportsByInstance.set(transport.serviceInstanceId, []);
      }

      transportsByInstance.get(transport.serviceInstanceId)!.push(transport);
    }

    return this.readArrayPayload(ctx, [
      "serviceInstances",
      "service_instances",
      "serviceInstance",
      "service_instance",
    ])
      .filter(
        (instance: AnyObject) =>
          !Boolean(
            instance?.deleted ??
              instance?.is_deleted ??
              instance?.data?.deleted ??
              false,
          ),
      )
      .map((instance: AnyObject) =>
        normalizeServiceInstanceDescriptor({
          ...instance,
          transports:
            Array.isArray((instance as any)?.transports) &&
            (instance as any).transports.length > 0
              ? (instance as any).transports
              : transportsByInstance.get(
                  String(
                    (instance as any)?.uuid ??
                      (instance as any)?.serviceInstanceId ??
                      (instance as any)?.service_instance_id ??
                      "",
                  ).trim(),
                ) ?? [],
        }),
      )
      .filter(
        (instance: ServiceInstanceDescriptor | null): instance is ServiceInstanceDescriptor =>
          !!instance &&
          !!instance.isActive &&
          !instance.isNonResponsive &&
          !instance.isBlocked,
      );
  }

  private shouldReconcileGatheredSyncTransmissions(): boolean {
    return this.serviceName === "CadenzaDB";
  }

  private collectGatheredSyncTransmissionRecipients(
    ctx: AnyObject,
  ): Set<string> {
    const recipients = new Set<string>();
    const addRecipient = (serviceName: unknown) => {
      const normalizedServiceName = String(serviceName ?? "").trim();
      if (
        !normalizedServiceName ||
        normalizedServiceName === this.serviceName
      ) {
        return;
      }

      recipients.add(normalizedServiceName);
    };

    addRecipient(ctx.serviceName ?? ctx.__serviceName);

    for (const instance of this.normalizeServiceInstancesFromSync(ctx)) {
      addRecipient(instance.serviceName);
    }

    for (const [serviceName, instances] of this.instances.entries()) {
      if (!instances?.length) {
        continue;
      }

      addRecipient(serviceName);
    }

    return recipients;
  }

  private reconcileGatheredSyncTransmissions(ctx: AnyObject, emit: any): boolean {
    return false;
  }

  public seedAuthorityBootstrapRoute(
    origin: string,
    role: ServiceTransportRole = "internal",
  ): void {
    const normalizedOrigin = String(origin ?? "").trim();
    if (!normalizedOrigin) {
      return;
    }

    const routeKey = buildTransportClientKey(
      {
        uuid: `cadenza-db-${role}-bootstrap`,
        role,
        origin: normalizedOrigin,
      },
      "CadenzaDB",
    );

    this.authorityBootstrapRoute = {
      ...this.authorityBootstrapRoute,
      origin: normalizedOrigin,
      role,
      routeKey,
      fetchId: buildTransportHandleKey(routeKey, "rest"),
    };
  }

  private noteAuthorityBootstrapHandshake(ctx: AnyObject): boolean {
    if (String(ctx.serviceName ?? "").trim() !== "CadenzaDB") {
      return false;
    }

    const origin = String(
      ctx.serviceOrigin ?? ctx.__transportOrigin ?? "",
    ).trim();
    if (origin) {
      this.seedAuthorityBootstrapRoute(
        origin,
        this.isFrontend ? "public" : "internal",
      );
    }

    this.authorityBootstrapRoute = {
      ...this.authorityBootstrapRoute,
      serviceInstanceId: String(ctx.serviceInstanceId ?? "").trim() || null,
      serviceTransportId: String(ctx.serviceTransportId ?? "").trim() || null,
      handshakeEstablished: true,
    };

    return true;
  }

  private getAuthorityBootstrapRestTarget(): {
    origin: string;
    routeKey: string;
    fetchId: string;
    serviceInstanceId: string | null;
    serviceTransportId: string | null;
  } | null {
    const origin = String(this.authorityBootstrapRoute.origin ?? "").trim();
    const routeKey = String(this.authorityBootstrapRoute.routeKey ?? "").trim();
    const fetchId = String(this.authorityBootstrapRoute.fetchId ?? "").trim();

    if (!origin || !routeKey || !fetchId) {
      return null;
    }

    return {
      origin,
      routeKey,
      fetchId,
      serviceInstanceId: this.authorityBootstrapRoute.serviceInstanceId,
      serviceTransportId: this.authorityBootstrapRoute.serviceTransportId,
    };
  }

  public hasAuthorityBootstrapHandshakeEstablished(): boolean {
    return this.authorityBootstrapRoute.handshakeEstablished === true;
  }

  private invalidateAuthorityBootstrapHandshake(): void {
    this.authorityBootstrapRoute = {
      ...this.authorityBootstrapRoute,
      serviceInstanceId: null,
      serviceTransportId: null,
      handshakeEstablished: false,
    };
  }

  private requestAuthorityBootstrapHandshake(
    ctx?: AnyObject,
  ): boolean {
    if (
      !this.connectsToCadenzaDB ||
      !this.serviceName ||
      this.serviceName === "CadenzaDB"
    ) {
      return false;
    }

    const target = this.getAuthorityBootstrapRestTarget();
    if (!target) {
      return false;
    }

    const authorityInstances = this.instances.get("CadenzaDB") ?? [];
    const matchingInstance =
      authorityInstances.find((instance) =>
        instance.transports.some(
          (transport) =>
            transport.role === this.authorityBootstrapRoute.role &&
            transport.origin === target.origin &&
            !transport.deleted,
        ),
      ) ?? null;
    const matchingTransport =
      matchingInstance?.transports.find(
        (transport) =>
          transport.role === this.authorityBootstrapRoute.role &&
          transport.origin === target.origin &&
          !transport.deleted,
      ) ?? null;
    const bootstrapTransportId =
      matchingTransport?.uuid ?? target.serviceTransportId ?? null;
    const communicationTypes =
      this.resolveCommunicationTypesForService("CadenzaDB");
    const handshakeContext = {
      serviceName: "CadenzaDB",
      serviceInstanceId:
        matchingInstance?.uuid ?? target.serviceInstanceId ?? "cadenza-db",
      serviceTransportId: bootstrapTransportId,
      serviceOrigin: target.origin,
      routeKey: target.routeKey,
      __routeKey: target.routeKey,
      fetchId: target.fetchId,
      __fetchId: target.fetchId,
      socketClientId: this.buildTransportProtocolHandleKey(
        target.routeKey,
        "socket",
      ),
      routeGeneration: bootstrapTransportId ?? target.routeKey,
      transportProtocols:
        matchingTransport?.protocols ?? (["rest", "socket"] as ServiceTransportProtocol[]),
      communicationTypes,
      transportProtocol: "rest",
      __authorityBootstrapChannel: true,
      __reason:
        typeof ctx?.__reason === "string" && ctx.__reason.trim().length > 0
          ? ctx.__reason.trim()
          : "authority_bootstrap_handshake",
      handshakeData: {
        instanceId: this.serviceInstanceId,
        serviceName: this.serviceName,
      },
    };

    if (
      matchingInstance &&
      matchingTransport &&
      this.hasTransportClientCreated(matchingInstance, matchingTransport, "rest") &&
      this.authorityBootstrapRoute.handshakeEstablished
    ) {
      return false;
    }

    if (
      this.authorityBootstrapRoute.handshakeEstablished ||
      this.authorityBootstrapHandshakeInFlight
    ) {
      return false;
    }

    if (matchingInstance && matchingTransport) {
      this.markTransportClientPending(matchingInstance, matchingTransport, "rest");
    }

    this.authorityBootstrapHandshakeInFlight = true;
    void (async () => {
      const controller =
        typeof AbortController === "function" ? new AbortController() : null;
      const timeoutId = controller
        ? setTimeout(
            () => controller.abort(),
            AUTHORITY_BOOTSTRAP_HANDSHAKE_TIMEOUT_MS,
          )
        : null;

      try {
        const response = await globalThis.fetch(`${target.origin}/handshake`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(handshakeContext.handshakeData),
          signal: controller?.signal,
        });

        if ("ok" in response && response.ok === false) {
          throw new Error(
            `Bootstrap authority handshake failed with HTTP ${response.status}`,
          );
        }

        const payload =
          typeof response.json === "function" ? await response.json() : response;
        const resolvedAuthorityInstanceId = String(
          payload?.__serviceInstanceId ?? "",
        ).trim();

        if (!resolvedAuthorityInstanceId) {
          throw new Error("Bootstrap authority handshake missing service instance id");
        }

        Cadenza.emit("meta.fetch.handshake_complete", {
          ...handshakeContext,
          serviceInstanceId: resolvedAuthorityInstanceId,
          targetServiceInstanceId: resolvedAuthorityInstanceId,
          __status: "success",
        });

        if (this.serviceInstanceId) {
          for (const communicationType of communicationTypes) {
            Cadenza.emit(
              "global.meta.fetch.service_communication_established",
              buildServiceCommunicationEstablishedContext({
                serviceInstanceId: resolvedAuthorityInstanceId,
                serviceInstanceClientId: this.serviceInstanceId,
                communicationType,
              }),
            );
          }
        }
      } catch (error) {
        const failureContext = {
          ...handshakeContext,
          __error: error instanceof Error ? error.message : String(error),
          errored: true,
        };
        Cadenza.emit(`meta.fetch.handshake_failed:${target.fetchId}`, failureContext);
        Cadenza.emit("meta.fetch.handshake_failed", failureContext);
      } finally {
        this.authorityBootstrapHandshakeInFlight = false;
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
      }
    })();
    return true;
  }

  private async invokeAuthorityBootstrapRoutine(
    remoteRoutineName: string,
    context: AnyObject,
    timeoutMs: number,
  ): Promise<AnyObject> {
    if (typeof globalThis.fetch !== "function") {
      return {
        ...context,
        __error: "Bootstrap authority route requires global fetch",
        errored: true,
      };
    }

    const target = this.getAuthorityBootstrapRestTarget();
    if (!target || !this.authorityBootstrapRoute.handshakeEstablished) {
      return {
        ...context,
        __error:
          "Authority bootstrap route is not established yet. Waiting for authority handshake.",
        errored: true,
      };
    }

    const controller =
      typeof AbortController === "function" ? new AbortController() : null;
    const timeoutId = controller
      ? setTimeout(() => controller.abort(), timeoutMs)
      : null;

    try {
      const requestBody = stripDelegationRequestSnapshot(
        ensureDelegationContextMetadata(
          attachDelegationRequestSnapshot({
            ...context,
            __remoteRoutineName: remoteRoutineName,
            __serviceName: "CadenzaDB",
            __localServiceName: this.serviceName,
            __timeout: timeoutMs,
            __syncing: true,
            __transportOrigin: target.origin,
            __transportProtocol: "rest",
            __transportProtocols: ["rest"],
            __routeKey: target.routeKey,
            routeKey: target.routeKey,
            __fetchId: target.fetchId,
            fetchId: target.fetchId,
            __metadata: {
              ...(context.__metadata && typeof context.__metadata === "object"
                ? context.__metadata
                : {}),
              __timeout: timeoutMs,
              __syncing: true,
              __authorityBootstrapChannel: true,
            },
          }),
        ),
      );

      const response = await globalThis.fetch(`${target.origin}/delegation`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
        signal: controller?.signal,
      });

      if ("ok" in response && response.ok === false) {
        return {
          ...context,
          __error: `Bootstrap authority delegation failed with HTTP ${response.status}`,
          errored: true,
        };
      }

      const payload =
        typeof response.json === "function" ? await response.json() : response;
      return payload && typeof payload === "object"
        ? ({
            ...context,
            ...(payload as AnyObject),
          } as AnyObject)
        : ({
            ...context,
            returnedValue: payload,
          } as AnyObject);
    } catch (error) {
      return {
        ...context,
        __error: error instanceof Error ? error.message : String(error),
        errored: true,
      };
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  }

  private registerRemoteIntentDeputy(map: {
    intentName: string;
    serviceName: string;
    taskName: string;
    taskVersion: number;
    timeout?: number;
  }) {
    if (!this.serviceName || map.serviceName === this.serviceName) {
      return;
    }

    const key = this.buildRemoteIntentDeputyKey(map);
    if (this.remoteIntentDeputiesByKey.has(key)) {
      return;
    }

    const deputyTaskName = `Inquire ${map.intentName} via ${map.serviceName} (${map.taskName} v${map.taskVersion})`;

    const deputyTask =
      map.serviceName === "CadenzaDB" && isAuthorityBootstrapIntent(map.intentName)
        ? Cadenza.createMetaTask(
            deputyTaskName,
            async (ctx) =>
              this.invokeAuthorityBootstrapRoutine(
                map.taskName,
                ctx,
                map.timeout ?? 15_000,
              ),
            "Routes reserved authority bootstrap inquiries directly through the post-handshake authority control-plane channel.",
            {
              register: false,
              isHidden: true,
            },
          )
        : isMetaIntentName(map.intentName)
          ? Cadenza.createMetaDeputyTask(map.taskName, map.serviceName, {
              register: false,
              isHidden: true,
              timeout: map.timeout,
              retryCount: 1,
              retryDelay: 50,
              retryDelayFactor: 1.2,
            })
          : Cadenza.createDeputyTask(map.taskName, map.serviceName, {
              register: false,
              isHidden: true,
              timeout: map.timeout,
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

    if (shouldTraceServiceRegistry(this.serviceName)) {
      console.log("[CADENZA_SERVICE_REGISTRY_TRACE] register_remote_intent_deputy", {
        localServiceName: this.serviceName,
        intentName: map.intentName,
        remoteServiceName: map.serviceName,
        remoteTaskName: map.taskName,
        remoteTaskVersion: map.taskVersion,
      });
    }
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

  public ensureAuthorityFullSyncResponderTask(): Task | null {
    if (this.serviceName !== "CadenzaDB") {
      return null;
    }

    if (this.authorityFullSyncResponderTask) {
      return this.authorityFullSyncResponderTask;
    }

    const authorityFullSyncResponderTask = Cadenza.createMetaTask(
      BOOTSTRAP_FULL_SYNC_RESPONDER_TASK_NAME,
      async (ctx) => {
        const queryOptionalAuthorityRoutingRows = async (
          tableName: "signal_to_task_map" | "intent_to_task_map",
        ) => {
          try {
            return await DatabaseController.instance.queryAuthorityTableRows(
              tableName,
            );
          } catch (error) {
            const message =
              error instanceof Error ? error.message : String(error);
            if (
              message.includes(
                `Table '${tableName}' is not registered on the CadenzaDB PostgresActor`,
              )
            ) {
              return [];
            }

            throw error;
          }
        };

        const [
          serviceInstances,
          serviceInstanceTransports,
          serviceManifests,
          signalToTaskMaps,
          intentToTaskMaps,
        ] = await Promise.all([
          DatabaseController.instance.queryAuthorityTableRows("service_instance"),
          DatabaseController.instance.queryAuthorityTableRows(
            "service_instance_transport",
          ),
          DatabaseController.instance.queryAuthorityTableRows("service_manifest"),
          queryOptionalAuthorityRoutingRows("signal_to_task_map"),
          queryOptionalAuthorityRoutingRows("intent_to_task_map"),
        ]);

        return {
          ...ctx,
          __syncing: true,
          ...this.collectBootstrapFullSyncPayload({
            serviceInstances,
            serviceInstanceTransports,
            serviceManifests,
            signalToTaskMaps,
            intentToTaskMaps,
          }),
        };
      },
      "Responds to bootstrap service-registry full sync with one authority-local control-plane query that returns a complete merged payload.",
      {
        register: true,
        isHidden: false,
      },
    ).respondsTo(META_SERVICE_REGISTRY_FULL_SYNC_INTENT);

    this.authorityFullSyncResponderTask = authorityFullSyncResponderTask;
    return authorityFullSyncResponderTask;
  }

  public ensureAuthorityServiceCommunicationPersistenceTask(): Task | null {
    if (this.serviceName !== "CadenzaDB") {
      return null;
    }

    if (this.authorityServiceCommunicationPersistenceTask) {
      return this.authorityServiceCommunicationPersistenceTask;
    }

    const retrySignal = META_SERVICE_COMMUNICATION_PERSIST_RETRY_SIGNAL;
    const processTask = Cadenza.createMetaTask(
      "Process authority service communication persistence",
      async (ctx) => {
        const descriptor = resolveServiceCommunicationPersistenceDescriptor(ctx);
        if (!descriptor) {
          return false;
        }

        const scheduleRetry = (reason: string, error?: unknown) => {
          const delayMs =
            SERVICE_COMMUNICATION_PERSIST_RETRY_DELAYS_MS[descriptor.retryAttempt];
          const nextAttempt = descriptor.retryAttempt + 1;
          if (delayMs !== undefined) {
            Cadenza.schedule(
              retrySignal,
              buildServiceCommunicationRetryContext({
                ...descriptor,
                retryAttempt: nextAttempt,
              }),
              delayMs,
            );
            return false;
          }

          Cadenza.log(
            "Authority service communication persistence skipped after bounded retries",
            {
              reason,
              error:
                error instanceof Error
                  ? error.message
                  : error !== undefined
                    ? String(error)
                    : undefined,
              serviceInstanceId: descriptor.serviceInstanceId,
              serviceInstanceClientId: descriptor.serviceInstanceClientId,
              communicationType: descriptor.communicationType,
              retryAttempt: descriptor.retryAttempt,
            },
            "warning",
          );
          return false;
        };

        try {
          const [serviceRows, clientRows] = await Promise.all([
            DatabaseController.instance.queryAuthorityTableRows("service_instance", {
              fields: ["uuid"],
              filter: {
                uuid: descriptor.serviceInstanceId,
              },
            }),
            DatabaseController.instance.queryAuthorityTableRows("service_instance", {
              fields: ["uuid"],
              filter: {
                uuid: descriptor.serviceInstanceClientId,
              },
            }),
          ]);

          if (!serviceRows.length || !clientRows.length) {
            return scheduleRetry("missing_service_instance_rows");
          }

          const insertResult = await DatabaseController.instance.persistAuthorityInsert(
            "service_to_service_communication_map",
            buildServiceCommunicationEstablishedContext(descriptor),
          );

          if (insertResult?.errored) {
            return scheduleRetry(
              "authority_insert_failed",
              insertResult.__error ?? insertResult.error,
            );
          }

          return {
            ...ctx,
            ...insertResult,
          };
        } catch (error) {
          return scheduleRetry("authority_query_failed", error);
        }
      },
      "Persists service communication edges on authority only after both referenced service instances exist locally.",
      {
        register: false,
        isHidden: true,
      },
    );

    Cadenza.createMetaTask(
      "Retry authority service communication persistence",
      (ctx) => ctx,
      "Re-enters bounded local retries for authority service communication persistence.",
      {
        register: false,
        isHidden: true,
      },
    )
      .doOn(retrySignal)
      .then(processTask);

    const authorityServiceCommunicationPersistenceTask = Cadenza.createMetaTask(
      "Handle service communication established",
      (ctx) => ctx,
      "Registers persisted service-to-service communication edges only on authority after parent instance rows exist.",
      {
        register: true,
        isHidden: false,
      },
    )
      .doOn("global.meta.fetch.service_communication_established")
      .then(processTask);

    this.authorityServiceCommunicationPersistenceTask =
      authorityServiceCommunicationPersistenceTask;
    return authorityServiceCommunicationPersistenceTask;
  }

  private registerBootstrapFullSyncDeputies(
    emit: (signal: string, ctx: AnyObject) => void,
    ctx?: AnyObject,
  ): boolean {
    return this.ensureBootstrapAuthorityControlPlane(ctx, emit, [
      META_SERVICE_REGISTRY_FULL_SYNC_INTENT,
    ]);
  }

  public ensureBootstrapAuthorityControlPlaneForInquiry(
    intentName: string,
    ctx?: AnyObject,
  ): boolean {
    return this.ensureBootstrapAuthorityControlPlane(ctx, undefined, [intentName]);
  }

  private ensureBootstrapAuthorityControlPlane(
    ctx?: AnyObject,
    emit?: (signal: string, ctx: AnyObject) => void,
    intentNames?: string[],
  ): boolean {
    if (
      !this.connectsToCadenzaDB ||
      !this.serviceName ||
      this.serviceName === "CadenzaDB"
    ) {
      return false;
    }

    const normalizedIntentNames =
      Array.isArray(intentNames) && intentNames.length > 0
        ? Array.from(
            new Set(
              intentNames
                .map((intentName) => String(intentName ?? "").trim())
                .filter(Boolean),
            ),
          )
        : AUTHORITY_BOOTSTRAP_INTENT_SPECS.map((spec) => spec.intentName);

    let changed = false;
    for (const intentName of normalizedIntentNames) {
      const spec = getAuthorityBootstrapIntentSpec(intentName);
      if (!spec) {
        continue;
      }

      Cadenza.inquiryBroker.addIntent({
        name: spec.intentName,
      });

      const deputyKey = this.buildRemoteIntentDeputyKey({
        intentName: spec.intentName,
        serviceName: "CadenzaDB",
        taskName: spec.authorityTaskName,
        taskVersion: 1,
      });
      const existed = this.remoteIntentDeputiesByKey.has(deputyKey);

      this.registerRemoteIntentDeputy({
        intentName: spec.intentName,
        serviceName: "CadenzaDB",
        taskName: spec.authorityTaskName,
        taskVersion: 1,
        timeout: spec.defaultTimeoutMs,
      });

      if (!existed && this.remoteIntentDeputiesByKey.has(deputyKey)) {
        changed = true;
      }
    }

    this.requestAuthorityBootstrapHandshake(ctx);
    return changed;
  }

  private hasBootstrapFullSyncDeputies(): boolean {
    if (!this.serviceName || this.serviceName === "CadenzaDB") {
      return true;
    }

    return this.remoteIntentDeputiesByKey.has(
      this.buildRemoteIntentDeputyKey({
        intentName: META_SERVICE_REGISTRY_FULL_SYNC_INTENT,
        serviceName: "CadenzaDB",
        taskName: BOOTSTRAP_FULL_SYNC_RESPONDER_TASK_NAME,
        taskVersion: 1,
      }),
    );
  }

  private ensureAuthorityBootstrapSignalTransmissions(): boolean {
    if (this.serviceName !== "CadenzaDB") {
      return false;
    }

    let changed = false;

    for (const [serviceName, instances] of this.instances.entries()) {
      if (!serviceName || serviceName === this.serviceName || !instances?.length) {
        continue;
      }

      const hasLiveInstance = instances.some(
        (instance) =>
          (instance as AnyObject).deleted !== true && instance.isActive !== false,
      );
      if (!hasLiveInstance) {
        continue;
      }

      if (!this.remoteSignals.has(serviceName)) {
        this.remoteSignals.set(serviceName, new Set());
      }

      const registeredSignals = this.remoteSignals.get(serviceName)!;

      for (const signalName of AUTHORITY_BOOTSTRAP_SIGNAL_NAMES) {
        if (registeredSignals.has(signalName)) {
          continue;
        }

        Cadenza.createSignalTransmissionTask(signalName, serviceName, {
          register: true,
          isHidden: true,
        });
        registeredSignals.add(signalName);
        changed = true;
      }
    }

    return changed;
  }

  private clearBootstrapFullSyncRetryTimer(): void {
    if (this.bootstrapFullSyncRetryTimer) {
      clearTimeout(this.bootstrapFullSyncRetryTimer);
      this.bootstrapFullSyncRetryTimer = null;
    }
  }

  private invalidateBootstrapFullSyncRetryState(reason?: string): void {
    this.bootstrapFullSyncRetryGeneration += 1;
    this.clearBootstrapFullSyncRetryTimer();
    this.bootstrapFullSyncRetryIndex = 0;
    this.bootstrapFullSyncSatisfied = false;
    if (typeof reason === "string" && reason.trim()) {
      this.bootstrapFullSyncRetryReason = reason.trim();
    }
  }

  private markBootstrapFullSyncSatisfied(): void {
    this.bootstrapFullSyncSatisfied = true;
    this.bootstrapFullSyncRetryIndex = EARLY_FULL_SYNC_DELAYS_MS.length;
    this.clearBootstrapFullSyncRetryTimer();
  }

  private hasHealthyBootstrapFullSyncResult({
    serviceInstances,
    intentToTaskMaps,
  }: {
    serviceInstances: ServiceInstanceDescriptor[];
    intentToTaskMaps: Array<{
      intentName: string;
      serviceName: string;
      taskName: string;
      taskVersion: number;
      deleted?: boolean;
    }>;
  }): boolean {
    return (
      this.bootstrapFullSyncRetryIndex >= MIN_BOOTSTRAP_FULL_SYNC_ATTEMPTS &&
      serviceInstances.length > 0 &&
      intentToTaskMaps.length > 0
    );
  }

  private scheduleNextBootstrapFullSyncRetry(reason?: string): boolean {
    if (
      !this.connectsToCadenzaDB ||
      !this.serviceName ||
      this.serviceName === "CadenzaDB"
    ) {
      return false;
    }

    if (!this.hasBootstrapFullSyncDeputies()) {
      return false;
    }

    if (typeof reason === "string" && reason.trim()) {
      this.bootstrapFullSyncRetryReason = reason.trim();
    }

    if (this.bootstrapFullSyncSatisfied || this.bootstrapFullSyncRetryTimer) {
      return false;
    }

    if (this.bootstrapFullSyncRetryIndex >= EARLY_FULL_SYNC_DELAYS_MS.length) {
      return false;
    }

    const retryGeneration = this.bootstrapFullSyncRetryGeneration;
    const retryReason =
      this.bootstrapFullSyncRetryReason ?? "service_registry_bootstrap_retry";
    const delayMs = EARLY_FULL_SYNC_DELAYS_MS[this.bootstrapFullSyncRetryIndex];
    const attempt = this.bootstrapFullSyncRetryIndex + 1;
    this.bootstrapFullSyncRetryIndex += 1;

    this.bootstrapFullSyncRetryTimer = setTimeout(() => {
      this.bootstrapFullSyncRetryTimer = null;

      if (
        this.bootstrapFullSyncSatisfied ||
        retryGeneration !== this.bootstrapFullSyncRetryGeneration
      ) {
        return;
      }

      Cadenza.emit("meta.sync_requested", {
        __syncing: false,
        __reason: retryReason,
        __bootstrapFullSync: true,
        __bootstrapFullSyncAttempt: attempt,
      });
    }, delayMs);

    return true;
  }

  private restartBootstrapFullSyncRetryChain(reason: string): boolean {
    this.invalidateBootstrapFullSyncRetryState(reason);
    return this.scheduleNextBootstrapFullSyncRetry(reason);
  }

  public bootstrapFullSync(
    emit: (signal: string, ctx: AnyObject) => void,
    ctx?: AnyObject,
    reason = "local_instance_inserted",
  ): boolean {
    this.ensureBootstrapAuthorityControlPlane(ctx, emit);
    return this.restartBootstrapFullSyncRetryChain(reason);
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

    const existing = this.getInstance(this.serviceName, this.serviceInstanceId);
    if (existing) {
      return existing;
    }

    if (
      this.localInstanceSeed &&
      this.localInstanceSeed.serviceName === this.serviceName &&
      this.localInstanceSeed.uuid === this.serviceInstanceId
    ) {
      return this.seedLocalInstance(this.localInstanceSeed, {
        markTransportsReady: true,
      }) ?? undefined;
    }

    return undefined;
  }

  hasLocalInstanceRegistered() {
    return Boolean(this.getLocalInstance());
  }

  public seedLocalInstance(
    value: unknown,
    options: {
      markTransportsReady?: boolean;
    } = {},
  ): ServiceInstanceDescriptor | null {
    const normalized = normalizeServiceInstanceDescriptor(value);
    if (!normalized) {
      return null;
    }

    if (!this.instances.has(normalized.serviceName)) {
      this.instances.set(normalized.serviceName, []);
    }

    const instances = this.instances.get(normalized.serviceName)!;
    const existingIndex = instances.findIndex(
      (instance) => instance.uuid === normalized.uuid,
    );
    const existing = existingIndex >= 0 ? instances[existingIndex] : null;
    const seeded: ServiceInstanceDescriptor = existing
      ? {
          ...existing,
          ...normalized,
          transports:
            normalized.transports.length > 0
              ? normalized.transports
              : existing.transports,
          clientCreatedTransportIds: existing.clientCreatedTransportIds ?? [],
          clientPendingTransportIds: existing.clientPendingTransportIds ?? [],
          clientReadyTransportIds: existing.clientReadyTransportIds ?? [],
        }
      : {
          ...normalized,
          clientCreatedTransportIds: [],
          clientPendingTransportIds: [],
          clientReadyTransportIds: [],
        };

    if (options.markTransportsReady) {
      for (const transport of seeded.transports) {
        this.markTransportClientReady(seeded, transport);
      }
    }

    if (existingIndex >= 0) {
      instances[existingIndex] = seeded;
    } else {
      instances.push(seeded);
    }

    if (normalized.uuid === this.serviceInstanceId) {
      this.localInstanceSeed = {
        ...seeded,
        transports: seeded.transports.map((transport) => ({ ...transport })),
        clientCreatedTransportIds: [...(seeded.clientCreatedTransportIds ?? [])],
        clientPendingTransportIds: [...(seeded.clientPendingTransportIds ?? [])],
        clientReadyTransportIds: [...(seeded.clientReadyTransportIds ?? [])],
      };
      this.lastHeartbeatAtByInstance.set(normalized.uuid, Date.now());
      this.missedHeartbeatsByInstance.set(normalized.uuid, 0);
      this.runtimeStatusFallbackInFlightByInstance.delete(normalized.uuid);
    }

    return seeded;
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
      cpuUsage: report.cpuUsage ?? null,
      memoryUsage: report.memoryUsage ?? null,
      eventLoopLag: report.eventLoopLag ?? null,
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

  private selectReadyTransportForInstance(
    instance: ServiceInstanceDescriptor,
    ctx: AnyObject,
    role: ServiceTransportRole = this.getRoutingTransportRole(),
  ): ServiceTransportDescriptor | undefined {
    for (const protocol of this.resolveTransportProtocolOrder(ctx)) {
      const candidates = instance.transports
        .filter(
          (transport) =>
            !transport.deleted &&
            transport.role === role &&
            transportSupportsProtocol(transport, protocol),
        )
        .sort((left, right) => {
          const leftIsBootstrap = left.uuid.endsWith("-bootstrap") ? 1 : 0;
          const rightIsBootstrap = right.uuid.endsWith("-bootstrap") ? 1 : 0;
          if (leftIsBootstrap !== rightIsBootstrap) {
            return leftIsBootstrap - rightIsBootstrap;
          }

          return left.origin.localeCompare(right.origin);
        });

      for (const transport of candidates) {
        if (this.hasTransportClientReady(instance, transport, protocol)) {
          return transport;
        }
      }
    }

    return undefined;
  }

  private getRoutingTransportRole(): ServiceTransportRole {
    return this.isFrontend ? "public" : "internal";
  }

  private getPreferredRoutingProtocol(
    ctx: AnyObject,
  ): ServiceTransportProtocol {
    return this.resolveTransportProtocolOrder(ctx)[0] ?? "rest";
  }

  private buildRoutingCooldownKey(
    serviceName: string,
    role: ServiceTransportRole,
    protocol: ServiceTransportProtocol,
  ): string {
    return `${serviceName}|${role}|${protocol}`;
  }

  private getActiveRoutingCooldown(
    serviceName: string,
    role: ServiceTransportRole,
    protocol: ServiceTransportProtocol,
  ): RoutingCooldownState | null {
    const key = this.buildRoutingCooldownKey(serviceName, role, protocol);
    const state = this.routingCooldownsByKey.get(key);
    if (!state) {
      return null;
    }

    const now = Date.now();
    if (
      state.cooldownUntil <= now &&
      now - state.lastFailureAt > this.noRouteCooldownWindowMs
    ) {
      this.routingCooldownsByKey.delete(key);
      return null;
    }

    if (state.cooldownUntil <= now) {
      return null;
    }

    return state;
  }

  private recordRoutingFailure(
    serviceName: string,
    role: ServiceTransportRole,
    protocol: ServiceTransportProtocol,
    reason: string,
  ): RoutingCooldownState {
    const key = this.buildRoutingCooldownKey(serviceName, role, protocol);
    const now = Date.now();
    const previous = this.routingCooldownsByKey.get(key);
    const withinWindow =
      previous && now - previous.lastFailureAt <= this.noRouteCooldownWindowMs;
    const failureCount = withinWindow ? previous.failureCount + 1 : 1;
    const cooldownUntil =
      failureCount >= this.noRouteCooldownFailureThreshold
        ? now + this.noRouteCooldownMs
        : 0;

    const nextState: RoutingCooldownState = {
      serviceName,
      role,
      protocol,
      failureCount,
      lastFailureAt: now,
      cooldownUntil,
      reason,
    };

    if (cooldownUntil > now) {
      this.routingCooldownsByKey.set(key, nextState);
    } else {
      this.routingCooldownsByKey.set(key, nextState);
    }

    return nextState;
  }

  private clearRoutingCooldown(
    serviceName: string,
    role: ServiceTransportRole,
    protocol: ServiceTransportProtocol,
  ): void {
    this.routingCooldownsByKey.delete(
      this.buildRoutingCooldownKey(serviceName, role, protocol),
    );
  }

  private hasRouteableInstanceForRouting(
    serviceName: string,
    role: ServiceTransportRole,
    protocol: ServiceTransportProtocol,
  ): boolean {
    const routingContext: AnyObject = {
      __transportProtocol: protocol,
    };

    return (this.instances.get(serviceName) ?? []).some((instance) => {
      if (!instance.isActive || instance.isNonResponsive || instance.isBlocked) {
        return false;
      }

      if (instance.isFrontend) {
        return role === "public";
      }

      return Boolean(
        this.selectReadyTransportForInstance(instance, routingContext, role),
      );
    });
  }

  private refreshRoutingCooldownsForService(serviceName: string): void {
    if (!serviceName) {
      return;
    }

    for (const [key, state] of this.routingCooldownsByKey.entries()) {
      if (state.serviceName !== serviceName) {
        continue;
      }

      if (
        this.hasRouteableInstanceForRouting(
          serviceName,
          state.role,
          state.protocol,
        )
      ) {
        this.routingCooldownsByKey.delete(key);
      }
    }
  }

  private buildTransportFailureKey(
    serviceInstanceId: string,
    transportId: string,
  ): string {
    return `${serviceInstanceId}|${transportId}`;
  }

  private clearTransportFailureState(
    serviceInstanceId: string,
    transportId: string,
  ): void {
    if (!serviceInstanceId || !transportId) {
      return;
    }

    this.transportFailuresByKey.delete(
      this.buildTransportFailureKey(serviceInstanceId, transportId),
    );
  }

  private classifyTransportFailureKind(message: string): string | null {
    const normalized = message.toLowerCase();
    if (
      normalized.includes("enotfound") ||
      normalized.includes("econnrefused") ||
      normalized.includes("ehostunreach") ||
      normalized.includes("network error")
    ) {
      return "hard-network";
    }

    if (
      normalized.includes("aborterror") ||
      normalized.includes("timed out") ||
      normalized.includes("timeout")
    ) {
      return "timeout";
    }

    return null;
  }

  private getTransportFailureThreshold(errorKind: string): number {
    return errorKind === "hard-network" ? 1 : this.transportFailureThreshold;
  }

  private recordTransportFailure(
    serviceName: string,
    serviceInstanceId: string,
    transportId: string,
    errorKind: string,
  ): TransportFailureState {
    const key = this.buildTransportFailureKey(serviceInstanceId, transportId);
    const now = Date.now();
    const previous = this.transportFailuresByKey.get(key);
    const withinWindow =
      previous && now - previous.lastFailureAt <= this.transportFailureWindowMs;
    const failureCount = withinWindow ? previous.failureCount + 1 : 1;
    const state: TransportFailureState = {
      serviceName,
      serviceInstanceId,
      transportId,
      failureCount,
      lastFailureAt: now,
      lastErrorKind: errorKind,
    };

    this.transportFailuresByKey.set(key, state);
    return state;
  }

  private findInstanceByTransportId(
    serviceName: string,
    transportId: string,
  ): ServiceInstanceDescriptor | undefined {
    return (this.instances.get(serviceName) ?? []).find((candidate) =>
      candidate.transports.some((transport) => transport.uuid === transportId),
    );
  }

  private resolveRouteRecordFromContext(
    ctx: AnyObject,
  ): RemoteRouteRecord | undefined {
    const explicitRouteKey =
      String(ctx.routeKey ?? "").trim() ||
      parseTransportHandleKey(ctx.fetchId ?? ctx.__fetchId)?.routeKey ||
      "";
    if (explicitRouteKey) {
      return this.remoteRoutesByKey.get(explicitRouteKey);
    }

    const serviceName = String(
      ctx.serviceName ?? ctx.__serviceName ?? "",
    ).trim();
    const transportId = String(
      ctx.serviceTransportId ?? ctx.__transportId ?? "",
    ).trim();

    if (serviceName && transportId) {
      return this.findRemoteRouteRecordByTransportId(serviceName, transportId);
    }

    return undefined;
  }

  private isCurrentRouteContext(ctx: AnyObject): boolean {
    const route = this.resolveRouteRecordFromContext(ctx);
    if (!route) {
      return true;
    }

    const serviceInstanceId = String(
      ctx.serviceInstanceId ?? ctx.__instance ?? "",
    ).trim();
    const serviceTransportId = String(
      ctx.serviceTransportId ?? ctx.__transportId ?? "",
    ).trim();

    if (
      serviceInstanceId &&
      route.serviceInstanceId &&
      route.serviceInstanceId !== serviceInstanceId
    ) {
      return false;
    }

    if (
      serviceTransportId &&
      route.serviceTransportId &&
      route.serviceTransportId !== serviceTransportId
    ) {
      return false;
    }

    return true;
  }

  private maybeDemoteFailedTransport(
    context: AnyObject,
    emit: (signal: string, ctx: AnyObject) => void,
    preferredRole: ServiceTransportRole,
    preferredProtocol: ServiceTransportProtocol,
  ): void {
    const fullSignalName = String(
      context.__signalEmission?.fullSignalName ?? context.__signalName ?? "",
    ).trim();
    if (
      !fullSignalName.includes("meta.fetch.delegate_failed") &&
      !fullSignalName.includes("meta.socket_client.delegate_failed") &&
      !fullSignalName.includes("meta.fetch.signal_transmission_failed") &&
      !fullSignalName.includes("meta.socket_client.signal_transmission_failed") &&
      !fullSignalName.includes("meta.service_registry.socket_failed")
    ) {
      return;
    }

    const serviceName = String(context.__serviceName ?? "").trim();
    const transportId = String(context.__transportId ?? "").trim();
    const instanceId = String(context.__instance ?? "").trim();
    const errorMessage = String(
      context.__error ?? context.error ?? context.returnedValue?.__error ?? "",
    ).trim();

    if (!serviceName || !transportId) {
      return;
    }

    const errorKind = this.classifyTransportFailureKind(errorMessage);
    if (!errorKind) {
      return;
    }

    if (!this.isCurrentRouteContext(context)) {
      return;
    }

    const instance =
      this.getInstance(serviceName, instanceId) ??
      this.findInstanceByTransportId(serviceName, transportId);
    if (!instance) {
      return;
    }

    const failureState = this.recordTransportFailure(
      serviceName,
      instance.uuid,
      transportId,
      errorKind,
    );
    if (failureState.failureCount < this.getTransportFailureThreshold(errorKind)) {
      return;
    }

    const transport = this.getTransportById(instance, transportId);
    if (!transport) {
      return;
    }

    this.clearTransportClientState(instance, transport, preferredProtocol);

    const hasAlternativeRoute = Boolean(
      this.selectReadyTransportForInstance(
        instance,
        { __transportProtocol: preferredProtocol },
        preferredRole,
      ),
    );

    if (!hasAlternativeRoute) {
      emit("meta.service_registry.runtime_status_unreachable", {
        serviceName,
        serviceInstanceId: instance.uuid,
        serviceTransportId: transportId,
        serviceOrigin: transport?.origin,
        __error: errorMessage,
      });
      this.recordRoutingFailure(
        serviceName,
        preferredRole,
        preferredProtocol,
        "transport_failure_demoted",
      );
    }
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

    return buildTransportClientKey(transport, instance.serviceName);
  }

  private resolveCommunicationTypesForService(serviceName: string): string[] {
    const communicationTypes = Array.from(
      new Set(
        this.deputies
          .get(serviceName)
          ?.map((descriptor) => descriptor.communicationType) ?? [],
      ),
    );

    if (
      !communicationTypes.includes("signal") &&
      this.remoteSignals.has(serviceName)
    ) {
      communicationTypes.push("signal");
    }

    return communicationTypes;
  }

  private ensureDependeeClientForInstance(
    instance: ServiceInstanceDescriptor,
    emit: (signal: string, ctx: AnyObject) => void,
    ctx?: AnyObject,
  ): boolean {
    if (
      !instance ||
      instance.uuid === this.serviceInstanceId ||
      instance.serviceName === this.serviceName ||
      instance.isFrontend ||
      !instance.isActive ||
      instance.isNonResponsive ||
      instance.isBlocked
    ) {
      return false;
    }

    if (
      !this.deputies.has(instance.serviceName) &&
      !this.remoteIntents.has(instance.serviceName) &&
      !this.remoteSignals.has(instance.serviceName)
    ) {
      return false;
    }

    const transport = this.selectTransportForInstance(
      instance,
      ctx ?? {},
      this.getRoutingTransportRole(),
    );

    if (!transport) {
      return false;
    }

    const route = this.upsertRemoteRouteRecord(
      instance.serviceName,
      instance.uuid,
      transport,
    );
    const restHandleKey = this.buildTransportProtocolHandleKey(route.key, "rest");
    const socketHandleKey = this.buildTransportProtocolHandleKey(
      route.key,
      "socket",
    );

    if (this.hasTransportClientCreated(instance, transport, "rest")) {
      return false;
    }

    emit("meta.service_registry.dependee_registered", {
      serviceName: instance.serviceName,
      serviceInstanceId: instance.uuid,
      serviceTransportId: transport.uuid,
      serviceOrigin: transport.origin,
      routeKey: route.key,
      fetchId: restHandleKey,
      socketClientId: socketHandleKey,
      routeGeneration: route.generation,
      transportProtocols: transport.protocols,
      communicationTypes: this.resolveCommunicationTypesForService(
        instance.serviceName,
      ),
    });
    this.markTransportClientPending(instance, transport, "rest");
    return true;
  }

  private ensureDependeeClientsForService(
    serviceName: string,
    emit: (signal: string, ctx: AnyObject) => void,
    ctx?: AnyObject,
  ): void {
    if (!serviceName || serviceName === this.serviceName) {
      return;
    }

    for (const instance of this.instances.get(serviceName) ?? []) {
      this.ensureDependeeClientForInstance(instance, emit, ctx);
    }
  }

  private scheduleDependeeClientRecovery(
    instance: ServiceInstanceDescriptor,
    transport: ServiceTransportDescriptor,
    ctx?: AnyObject,
  ): boolean {
    if (
      !instance ||
      instance.serviceName === "CadenzaDB" ||
      instance.serviceName === this.serviceName ||
      !instance.isActive ||
      instance.isBlocked ||
      transport.deleted
    ) {
      return false;
    }

    const route = this.upsertRemoteRouteRecord(
      instance.serviceName,
      instance.uuid,
      transport,
    );
    const delayMs = Math.max(250, this.routeFailurePenaltyMs);

    Cadenza.schedule(
      "meta.service_registry.dependee_registered",
      {
        serviceName: instance.serviceName,
        serviceInstanceId: instance.uuid,
        serviceTransportId: transport.uuid,
        serviceOrigin: transport.origin,
        routeKey: route.key,
        fetchId: this.buildTransportProtocolHandleKey(route.key, "rest"),
        socketClientId: this.buildTransportProtocolHandleKey(route.key, "socket"),
        routeGeneration: route.generation,
        transportProtocols: transport.protocols,
        communicationTypes: this.resolveCommunicationTypesForService(
          instance.serviceName,
        ),
        __reason:
          typeof ctx?.__reason === "string" && ctx.__reason.trim().length > 0
            ? ctx.__reason
            : "dependee_client_recovery",
      },
      delayMs,
    );

    return true;
  }

  private shouldEagerlyEstablishRemoteClients(ctx?: AnyObject): boolean {
    if (this.serviceName !== "CadenzaDB") {
      return true;
    }

    return ctx?.__allowAuthorityEagerClientCreation === true;
  }

  private shouldDemandEstablishRemoteClients(ctx?: AnyObject): boolean {
    if (this.serviceName !== "CadenzaDB") {
      return true;
    }

    const signalName = String(
      ctx?.__signalName ?? ctx?.__signalEmission?.fullSignalName ?? "",
    )
      .split(":")[0]
      .trim();

    if (signalName.startsWith("global.meta.")) {
      return false;
    }

    return true;
  }

  private buildTransportRouteKey(
    serviceName: string,
    transport: Pick<ServiceTransportDescriptor, "uuid" | "role" | "origin">,
  ): string {
    return buildTransportClientKey(transport, serviceName);
  }

  private buildTransportProtocolHandleKey(
    routeKey: string,
    protocol: ServiceTransportProtocol,
  ): string {
    return buildTransportHandleKey(routeKey, protocol);
  }

  private resolveProtocolFromContext(
    ctx: AnyObject,
    transport?: Pick<ServiceTransportDescriptor, "protocols"> | null,
  ): ServiceTransportProtocol | null {
    const explicitProtocol = String(
      ctx.transportProtocol ?? ctx.__transportProtocol ?? "",
    ).trim();
    if (explicitProtocol === "rest" || explicitProtocol === "socket") {
      return explicitProtocol;
    }

    const parsedHandle = parseTransportHandleKey(
      ctx.fetchId ?? ctx.routeKey ?? ctx.__fetchId,
    );
    if (parsedHandle?.protocol) {
      return parsedHandle.protocol;
    }

    const protocols = transport?.protocols ?? ctx.transportProtocols;
    if (Array.isArray(protocols) && protocols.length === 1) {
      const onlyProtocol = String(protocols[0]).trim();
      if (onlyProtocol === "rest" || onlyProtocol === "socket") {
        return onlyProtocol;
      }
    }

    return null;
  }

  private ensureRouteProtocolState(
    route: RemoteRouteRecord,
    protocol: ServiceTransportProtocol,
  ): {
    clientCreated: boolean;
    clientPending: boolean;
    clientReady: boolean;
  } {
    const existing = route.protocolState[protocol];
    if (existing) {
      return existing;
    }

    const next = {
      clientCreated: false,
      clientPending: false,
      clientReady: false,
    };
    route.protocolState[protocol] = next;
    return next;
  }

  private cloneRouteBalancingState(
    existing?: RemoteRouteBalancingState,
  ): RemoteRouteBalancingState {
    return {
      selectionCount: existing?.selectionCount ?? 0,
      lastSelectedAt: existing?.lastSelectedAt ?? null,
      lastSuccessAt: existing?.lastSuccessAt ?? null,
      lastFailureAt: existing?.lastFailureAt ?? null,
      failurePenaltyUntil: existing?.failurePenaltyUntil ?? null,
      activeDispatchTokens: new Set(existing?.activeDispatchTokens ?? []),
    };
  }

  private resolveRouteDispatchToken(ctx: AnyObject): string | null {
    const deputyExecId =
      typeof ctx?.__metadata?.__deputyExecId === "string" &&
      ctx.__metadata.__deputyExecId.length > 0
        ? ctx.__metadata.__deputyExecId
        : typeof ctx?.__deputyExecId === "string" && ctx.__deputyExecId.length > 0
          ? ctx.__deputyExecId
          : null;
    if (deputyExecId) {
      return `deputy:${deputyExecId}`;
    }

    const routineExecId =
      typeof ctx?.__routineExecId === "string" && ctx.__routineExecId.length > 0
        ? ctx.__routineExecId
        : null;
    if (routineExecId) {
      return `signal:${routineExecId}`;
    }

    const signalEmissionId =
      typeof ctx?.__signalEmissionId === "string" && ctx.__signalEmissionId.length > 0
        ? ctx.__signalEmissionId
        : typeof ctx?.__signalEmission?.uuid === "string" &&
            ctx.__signalEmission.uuid.length > 0
          ? ctx.__signalEmission.uuid
          : null;
    if (signalEmissionId) {
      return `signal-emission:${signalEmissionId}`;
    }

    return null;
  }

  private getNumericHealthMetric(
    instance: ServiceInstanceDescriptor,
    ...keys: string[]
  ): number | null {
    const health =
      instance.health && typeof instance.health === "object" ? instance.health : null;
    if (!health) {
      return null;
    }

    for (const key of keys) {
      const value = (health as Record<string, unknown>)[key];
      if (typeof value === "number" && Number.isFinite(value)) {
        return value;
      }
    }

    return null;
  }

  private compareOptionalBalancingMetric(
    left: number | null,
    right: number | null,
    tolerance = 0,
  ): number {
    if (left === null || right === null) {
      return 0;
    }

    const delta = left - right;
    if (Math.abs(delta) <= tolerance) {
      return 0;
    }

    return delta;
  }

  private buildRouteBalancingSnapshot(
    route: RemoteRouteRecord,
    instance: ServiceInstanceDescriptor,
  ): RemoteRouteBalancingSnapshot {
    const runtimeStatus = this.resolveRuntimeStatusSnapshot(
      instance.numberOfRunningGraphs ?? 0,
      instance.isActive,
      instance.isNonResponsive,
      instance.isBlocked,
    );
    const now = Date.now();
    const failurePenaltyUntil = route.balancing.failurePenaltyUntil;
    const penalized =
      typeof failurePenaltyUntil === "number" && failurePenaltyUntil > now;

    return {
      availability: penalized ? "penalized" : "available",
      runtimeState: runtimeStatus.state,
      acceptingWork: instance.acceptingWork !== false,
      numberOfRunningGraphs: instance.numberOfRunningGraphs ?? 0,
      inFlightDelegations: route.balancing.activeDispatchTokens.size,
      selectionCount: route.balancing.selectionCount,
      lastSelectedAt: route.balancing.lastSelectedAt,
      lastSuccessAt: route.balancing.lastSuccessAt,
      lastFailureAt: route.balancing.lastFailureAt,
      failurePenaltyUntil,
      cpuUsage: this.getNumericHealthMetric(instance, "cpuUsage", "cpu", "cpuLoad"),
      memoryUsage: this.getNumericHealthMetric(
        instance,
        "memoryUsage",
        "memory",
        "memoryPressure",
      ),
      eventLoopLag: this.getNumericHealthMetric(
        instance,
        "eventLoopLag",
        "eventLoopLagMs",
      ),
    };
  }

  private compareRouteBalancingSnapshots(
    left: RemoteRouteBalancingSnapshot,
    right: RemoteRouteBalancingSnapshot,
  ): number {
    const availabilityDelta =
      (left.availability === "available" ? 0 : 1) -
      (right.availability === "available" ? 0 : 1);
    if (availabilityDelta !== 0) {
      return availabilityDelta;
    }

    const runtimePriorityDelta =
      runtimeStatusPriority(left.runtimeState) -
      runtimeStatusPriority(right.runtimeState);
    if (runtimePriorityDelta !== 0) {
      return runtimePriorityDelta;
    }

    const acceptingWorkDelta =
      (left.acceptingWork ? 0 : 1) - (right.acceptingWork ? 0 : 1);
    if (acceptingWorkDelta !== 0) {
      return acceptingWorkDelta;
    }

    const inFlightDelta = left.inFlightDelegations - right.inFlightDelegations;
    if (inFlightDelta !== 0) {
      return inFlightDelta;
    }

    const cpuDelta = this.compareOptionalBalancingMetric(
      left.cpuUsage,
      right.cpuUsage,
      0.05,
    );
    if (cpuDelta !== 0) {
      return cpuDelta;
    }

    const memoryDelta = this.compareOptionalBalancingMetric(
      left.memoryUsage,
      right.memoryUsage,
      0.02,
    );
    if (memoryDelta !== 0) {
      return memoryDelta;
    }

    const eventLoopDelta = this.compareOptionalBalancingMetric(
      left.eventLoopLag,
      right.eventLoopLag,
      5,
    );
    if (eventLoopDelta !== 0) {
      return eventLoopDelta;
    }

    const graphDelta = left.numberOfRunningGraphs - right.numberOfRunningGraphs;
    if (graphDelta !== 0) {
      return graphDelta;
    }

    const selectionCountDelta = left.selectionCount - right.selectionCount;
    if (selectionCountDelta !== 0) {
      return selectionCountDelta;
    }

    return (left.lastSelectedAt ?? 0) - (right.lastSelectedAt ?? 0);
  }

  private markRouteDispatchSelected(routeKey: string, ctx: AnyObject): void {
    const route = this.remoteRoutesByKey.get(routeKey);
    if (!route) {
      return;
    }

    route.balancing.selectionCount += 1;
    route.balancing.lastSelectedAt = Date.now();
    const token = this.resolveRouteDispatchToken(ctx);
    if (token) {
      route.balancing.activeDispatchTokens.add(token);
    }
  }

  private markRouteDispatchCompleted(
    routeKey: string,
    ctx: AnyObject,
    outcome: "success" | "failure" | "neutral",
  ): void {
    const route = this.remoteRoutesByKey.get(routeKey);
    if (!route) {
      return;
    }

    const token = this.resolveRouteDispatchToken(ctx);
    if (token) {
      route.balancing.activeDispatchTokens.delete(token);
    }

    const now = Date.now();
    if (outcome === "success") {
      route.balancing.lastSuccessAt = now;
      route.balancing.failurePenaltyUntil = null;
      return;
    }

    if (outcome === "neutral") {
      return;
    }

    route.balancing.lastFailureAt = now;
    route.balancing.failurePenaltyUntil = now + this.routeFailurePenaltyMs;
  }

  public recordBalancedRouteSelection(routeKey: string, ctx: AnyObject): void {
    this.markRouteDispatchSelected(routeKey, ctx);
  }

  public recordBalancedRouteOutcome(
    ctx: AnyObject,
    outcome: "success" | "failure" | "neutral",
  ): void {
    const route = this.resolveRouteRecordFromContext(ctx);
    if (!route) {
      return;
    }

    this.markRouteDispatchCompleted(route.key, ctx, outcome);
  }

  private matchesSignalBroadcastFilter(
    instance: ServiceInstanceDescriptor,
    transport: ServiceTransportDescriptor,
    filter: AnyObject | null | undefined,
  ): boolean {
    if (!filter || typeof filter !== "object") {
      return true;
    }

    const matches = (value: string, accepted: unknown): boolean => {
      if (!Array.isArray(accepted) || accepted.length === 0) {
        return true;
      }

      return accepted.some((entry) => String(entry ?? "").trim() === value);
    };

    if (!matches(instance.serviceName, filter.serviceNames)) {
      return false;
    }
    if (!matches(instance.uuid, filter.serviceInstanceIds)) {
      return false;
    }
    if (!matches(transport.origin, filter.origins)) {
      return false;
    }
    if (!matches(transport.role, filter.roles)) {
      return false;
    }

    if (Array.isArray(filter.protocols) && filter.protocols.length > 0) {
      const allowedProtocols = filter.protocols.map((protocol) =>
        String(protocol ?? "").trim(),
      );
      if (
        !(transport.protocols ?? []).some((protocol) =>
          allowedProtocols.includes(protocol),
        )
      ) {
        return false;
      }
    }

    if (Array.isArray(filter.runtimeStates) && filter.runtimeStates.length > 0) {
      const runtimeState = this.resolveRuntimeStatusSnapshot(
        instance.numberOfRunningGraphs ?? 0,
        instance.isActive,
        instance.isNonResponsive,
        instance.isBlocked,
      ).state;
      if (
        !filter.runtimeStates
          .map((state) => String(state ?? "").trim())
          .includes(runtimeState)
      ) {
        return false;
      }
    }

    return true;
  }

  private routeHasProtocolState(
    route: RemoteRouteRecord,
    predicate: (state: {
      clientCreated: boolean;
      clientPending: boolean;
      clientReady: boolean;
    }) => boolean,
  ): boolean {
    return (["rest", "socket"] as const).some((protocol) => {
      const state = route.protocolState[protocol];
      return !!state && predicate(state);
    });
  }

  private syncInstanceClientRouteState(
    instance: ServiceInstanceDescriptor,
  ): void {
    const created = new Set<string>();
    const pending = new Set<string>();
    const ready = new Set<string>();

    for (const transport of instance.transports ?? []) {
      if (transport.deleted) {
        continue;
      }

      const routeKey = this.buildTransportRouteKey(
        instance.serviceName,
        transport,
      );
      const route = this.remoteRoutesByKey.get(routeKey);
      if (!route || route.serviceInstanceId !== instance.uuid) {
        continue;
      }

      if (this.routeHasProtocolState(route, (state) => state.clientCreated)) {
        created.add(routeKey);
      }
      if (this.routeHasProtocolState(route, (state) => state.clientPending)) {
        pending.add(routeKey);
      }
      if (this.routeHasProtocolState(route, (state) => state.clientReady)) {
        ready.add(routeKey);
      }
    }

    instance.clientCreatedTransportIds = Array.from(created);
    instance.clientPendingTransportIds = Array.from(pending);
    instance.clientReadyTransportIds = Array.from(ready);
  }

  private findRemoteRouteRecordByTransportId(
    serviceName: string,
    transportId: string,
  ): RemoteRouteRecord | undefined {
    if (!serviceName || !transportId) {
      return undefined;
    }

    for (const route of this.remoteRoutesByKey.values()) {
      if (
        route.serviceName === serviceName &&
        route.serviceTransportId === transportId
      ) {
        return route;
      }
    }

    return undefined;
  }

  private upsertRemoteRouteRecord(
    serviceName: string,
    serviceInstanceId: string,
    transport: ServiceTransportDescriptor,
  ): RemoteRouteRecord {
    const key = this.buildTransportRouteKey(serviceName, transport);
    const existing = this.remoteRoutesByKey.get(key);
    const next: RemoteRouteRecord = {
      key,
      serviceName,
      role: transport.role,
      origin: transport.origin,
      protocols: [...(transport.protocols ?? [])],
      serviceInstanceId,
      serviceTransportId: transport.uuid,
      generation: `${serviceInstanceId}|${transport.uuid}`,
      protocolState: {
        rest: existing?.protocolState.rest
          ? { ...existing.protocolState.rest }
          : undefined,
        socket: existing?.protocolState.socket
          ? { ...existing.protocolState.socket }
          : undefined,
      },
      balancing: this.cloneRouteBalancingState(existing?.balancing),
      lastUpdatedAt: Date.now(),
    };

    this.remoteRoutesByKey.set(key, next);

    if (
      existing &&
      existing.serviceInstanceId &&
      existing.serviceInstanceId !== serviceInstanceId
    ) {
      const previousInstance = this.getInstance(
        existing.serviceName,
        existing.serviceInstanceId,
      );
      if (previousInstance) {
        this.syncInstanceClientRouteState(previousInstance);
      }
    }

    const currentInstance = this.getInstance(serviceName, serviceInstanceId);
    if (currentInstance) {
      this.syncInstanceClientRouteState(currentInstance);
    }

    return next;
  }

  private clearRemoteRouteRecordIfCurrent(
    serviceName: string,
    serviceInstanceId: string,
    transport: ServiceTransportDescriptor,
  ): void {
    const routeKey = this.buildTransportRouteKey(serviceName, transport);
    const existing = this.remoteRoutesByKey.get(routeKey);
    if (
      existing &&
      existing.serviceInstanceId === serviceInstanceId &&
      existing.serviceTransportId === transport.uuid
    ) {
      this.remoteRoutesByKey.delete(routeKey);
    }
  }

  private emitTransportHandleShutdowns(
    emit: (signal: string, ctx: AnyObject) => void,
    routeKey: string,
    transport: Pick<ServiceTransportDescriptor, "uuid" | "origin" | "protocols">,
  ): void {
    const protocols = transport.protocols ?? [];
    if (protocols.includes("socket")) {
      emit(
        `meta.socket_shutdown_requested:${this.buildTransportProtocolHandleKey(routeKey, "socket")}`,
        {
          routeKey,
          serviceTransportId: transport.uuid,
          serviceOrigin: transport.origin,
          transportProtocol: "socket",
        },
      );
    }

    if (protocols.includes("rest")) {
      emit(
        `meta.fetch.destroy_requested:${this.buildTransportProtocolHandleKey(routeKey, "rest")}`,
        {
          routeKey,
          serviceTransportId: transport.uuid,
          serviceOrigin: transport.origin,
          transportProtocol: "rest",
        },
      );
    }
  }

  private resolveTransportRouteKey(
    instance: ServiceInstanceDescriptor,
    transport: Pick<ServiceTransportDescriptor, "uuid" | "role" | "origin">,
  ): string {
    return this.buildTransportRouteKey(instance.serviceName, transport);
  }

  private hasTransportClientCreated(
    instance: ServiceInstanceDescriptor,
    transport: Pick<ServiceTransportDescriptor, "uuid" | "role" | "origin">,
    protocol?: ServiceTransportProtocol,
  ): boolean {
    const routeKey = this.resolveTransportRouteKey(instance, transport);
    const route = this.remoteRoutesByKey.get(routeKey);
    if (route) {
      if (protocol) {
        const state = route.protocolState[protocol];
        return !!state && (state.clientCreated || state.clientPending || state.clientReady);
      }

      return this.routeHasProtocolState(
        route,
        (state) => state.clientCreated || state.clientPending || state.clientReady,
      );
    }

    return (
      (instance.clientCreatedTransportIds ?? []).includes(routeKey) ||
      (instance.clientCreatedTransportIds ?? []).includes(transport.uuid) ||
      (instance.clientPendingTransportIds ?? []).includes(routeKey) ||
      (instance.clientPendingTransportIds ?? []).includes(transport.uuid) ||
      (instance.clientReadyTransportIds ?? []).includes(routeKey) ||
      (instance.clientReadyTransportIds ?? []).includes(transport.uuid)
    );
  }

  private markTransportClientPending(
    instance: ServiceInstanceDescriptor,
    transport: ServiceTransportDescriptor,
    protocol: ServiceTransportProtocol,
  ): void {
    const route = this.upsertRemoteRouteRecord(
      instance.serviceName,
      instance.uuid,
      transport,
    );
    const state = this.ensureRouteProtocolState(route, protocol);
    state.clientCreated = state.clientCreated || state.clientReady;
    state.clientPending = true;
    state.clientReady = false;
    this.remoteRoutesByKey.set(route.key, route);
    this.syncInstanceClientRouteState(instance);
  }

  private hasTransportClientReady(
    instance: ServiceInstanceDescriptor,
    transport: Pick<ServiceTransportDescriptor, "uuid" | "role" | "origin">,
    protocol?: ServiceTransportProtocol,
  ): boolean {
    const routeKey = this.resolveTransportRouteKey(instance, transport);
    const route = this.remoteRoutesByKey.get(routeKey);
    if (route) {
      if (protocol) {
        return route.protocolState[protocol]?.clientReady === true;
      }

      return this.routeHasProtocolState(route, (state) => state.clientReady);
    }

    return (
      (instance.clientReadyTransportIds ?? []).includes(routeKey) ||
      (instance.clientReadyTransportIds ?? []).includes(transport.uuid)
    );
  }

  private hasReadyClientTransportForProtocol(
    instance: ServiceInstanceDescriptor,
    protocol: ServiceTransportProtocol,
  ): boolean {
    return (instance.transports ?? []).some(
      (transport) =>
        !transport.deleted &&
        transport.protocols?.includes(protocol) &&
        this.hasTransportClientReady(instance, transport, protocol),
    );
  }

  private hasAnyReadyClientProtocolForTransport(
    instance: ServiceInstanceDescriptor,
    transport: Pick<ServiceTransportDescriptor, "uuid" | "role" | "origin">,
  ): boolean {
    return this.hasTransportClientReady(instance, transport);
  }

  private markTransportClientReady(
    instance: ServiceInstanceDescriptor,
    transport: ServiceTransportDescriptor,
    protocol?: ServiceTransportProtocol,
  ): void {
    const route = this.upsertRemoteRouteRecord(
      instance.serviceName,
      instance.uuid,
      transport,
    );
    const protocols = protocol ? [protocol] : transport.protocols ?? ["rest"];
    for (const currentProtocol of protocols) {
      const state = this.ensureRouteProtocolState(route, currentProtocol);
      state.clientCreated = true;
      state.clientPending = false;
      state.clientReady = true;
    }
    this.remoteRoutesByKey.set(route.key, route);
    this.syncInstanceClientRouteState(instance);
  }

  private clearTransportClientState(
    instance: ServiceInstanceDescriptor,
    transportOrRouteKey:
      | Pick<ServiceTransportDescriptor, "uuid" | "role" | "origin">
      | string,
    protocol?: ServiceTransportProtocol,
  ): void {
    const routeKey =
      typeof transportOrRouteKey === "string"
        ? transportOrRouteKey
        : this.resolveTransportRouteKey(instance, transportOrRouteKey);
    const route = this.remoteRoutesByKey.get(routeKey);
    if (route) {
      if (
        typeof transportOrRouteKey !== "string" &&
        ((route.serviceInstanceId &&
          route.serviceInstanceId !== instance.uuid) ||
          (route.serviceTransportId &&
            route.serviceTransportId !== transportOrRouteKey.uuid))
      ) {
        return;
      }

      if (protocol) {
        const state = this.ensureRouteProtocolState(route, protocol);
        state.clientCreated = false;
        state.clientPending = false;
        state.clientReady = false;
      } else {
        for (const protocolKey of ["rest", "socket"] as const) {
          const state = this.ensureRouteProtocolState(route, protocolKey);
          state.clientCreated = false;
          state.clientPending = false;
          state.clientReady = false;
        }
      }
      route.lastUpdatedAt = Date.now();
      this.remoteRoutesByKey.set(routeKey, route);
    }

    this.syncInstanceClientRouteState(instance);
  }

  public shouldProcessRemoteRouteEvent(ctx: AnyObject): boolean {
    const parsedHandle = parseTransportHandleKey(
      ctx.fetchId ?? ctx.__fetchId ?? ctx.socketClientId,
    );
    const routeKey = String(
      ctx.routeKey ?? ctx.__routeKey ?? parsedHandle?.routeKey ?? "",
    ).trim();
    if (!routeKey) {
      return true;
    }

    const route = this.remoteRoutesByKey.get(routeKey);
    if (!route) {
      return true;
    }

    const serviceTransportId = String(
      ctx.serviceTransportId ?? ctx.__transportId ?? "",
    ).trim();
    if (
      serviceTransportId &&
      route.serviceTransportId &&
      route.serviceTransportId !== serviceTransportId
    ) {
      return false;
    }

    const routeGeneration = String(ctx.routeGeneration ?? "").trim();
    if (routeGeneration && route.generation && route.generation !== routeGeneration) {
      return false;
    }

    return true;
  }

  private resolveRequiredReadinessFromCommunicationTypes(
    communicationTypes: unknown,
  ): boolean {
    return this.shouldRequireReadinessFromCommunicationTypes(
      Array.isArray(communicationTypes)
        ? (communicationTypes as unknown[])
        : undefined,
    );
  }

  private markTransportReadyFromContext(ctx: AnyObject): void {
    const serviceName = String(ctx.serviceName ?? "").trim();
    const serviceInstanceId = String(ctx.serviceInstanceId ?? "").trim();
    const serviceTransportId = String(ctx.serviceTransportId ?? "").trim();

    if (!serviceName || !serviceInstanceId || !serviceTransportId) {
      return;
    }

    let instance = this.getInstance(serviceName, serviceInstanceId);
    if (!instance) {
      instance = (this.instances.get(serviceName) ?? []).find((candidate) =>
        candidate.transports.some(
          (transport) => transport.uuid === serviceTransportId,
        ),
      );
    }

    if (!instance) {
      return;
    }

    const transport = this.getTransportById(instance, serviceTransportId);
    if (!transport) {
      return;
    }

    this.upsertRemoteRouteRecord(serviceName, instance.uuid, transport);
    this.clearTransportFailureState(instance.uuid, serviceTransportId);
    this.markTransportClientReady(
      instance,
      transport,
      this.resolveProtocolFromContext(ctx, transport) ?? undefined,
    );
    this.refreshRoutingCooldownsForService(serviceName);
  }

  private clearTransportReadyFromContext(ctx: AnyObject): void {
    const serviceName = String(ctx.serviceName ?? "").trim();
    const explicitRouteKey =
      String(ctx.routeKey ?? "").trim() ||
      parseTransportHandleKey(ctx.fetchId ?? ctx.__fetchId)?.routeKey ||
      "";
    const serviceTransportId = String(
      ctx.serviceTransportId ?? ctx.__transportId ?? "",
    ).trim();

    if (!serviceName) {
      return;
    }

    const route =
      (explicitRouteKey && this.remoteRoutesByKey.get(explicitRouteKey)) ||
      this.findRemoteRouteRecordByTransportId(serviceName, serviceTransportId);

    if (!route) {
      return;
    }

    if (
      serviceTransportId &&
      route.serviceTransportId &&
      route.serviceTransportId !== serviceTransportId
    ) {
      return;
    }

    const instance = this.getInstance(route.serviceName, route.serviceInstanceId);
    if (!instance) {
      return;
    }

    this.clearTransportClientState(
      instance,
      route.key,
      this.resolveProtocolFromContext(ctx) ?? undefined,
    );
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

  private getInstanceRouteRole(
    instance: ServiceInstanceDescriptor,
  ): ServiceTransportRole {
    return instance.isFrontend ? "public" : "internal";
  }

  private findSupersededInstancesForTransport(
    serviceName: string,
    authoritativeInstanceId: string,
    role: ServiceTransportRole,
    origin: string,
  ): ServiceInstanceDescriptor[] {
    if (!serviceName || !authoritativeInstanceId || !origin) {
      return [];
    }

    const candidates = (this.instances.get(serviceName) ?? []).filter((candidate) => {
      if (candidate.isBootstrapPlaceholder) {
        return false;
      }

      return candidate.transports.some(
        (transport) =>
          !transport.deleted &&
          transport.role === role &&
          transport.origin === origin,
      );
    });

    if (!candidates.length) {
      return [];
    }

    const preferredInstance = [...candidates].sort((left, right) =>
      this.compareInstancesForRoutePrecedence(left, right, role, origin),
    )[0];

    if (!preferredInstance || preferredInstance.uuid !== authoritativeInstanceId) {
      return [];
    }

    return candidates.filter((candidate) => candidate.uuid !== preferredInstance.uuid);
  }

  private compareInstancesForRoutePrecedence(
    left: ServiceInstanceDescriptor,
    right: ServiceInstanceDescriptor,
    role: ServiceTransportRole,
    origin: string,
  ): number {
    const leftLocal =
      left.serviceName === this.serviceName && left.uuid === this.serviceInstanceId
        ? 1
        : 0;
    const rightLocal =
      right.serviceName === this.serviceName && right.uuid === this.serviceInstanceId
        ? 1
        : 0;

    if (leftLocal !== rightLocal) {
      return rightLocal - leftLocal;
    }

    const leftReady = this.hasReadyTransportForRoute(left, role, origin) ? 1 : 0;
    const rightReady = this.hasReadyTransportForRoute(right, role, origin) ? 1 : 0;
    if (leftReady !== rightReady) {
      return rightReady - leftReady;
    }

    const leftPrimary = left.isPrimary ? 1 : 0;
    const rightPrimary = right.isPrimary ? 1 : 0;
    if (leftPrimary !== rightPrimary) {
      return rightPrimary - leftPrimary;
    }

    const leftReportedAt = Date.parse(left.reportedAt ?? "");
    const rightReportedAt = Date.parse(right.reportedAt ?? "");
    const safeLeftReportedAt = Number.isFinite(leftReportedAt) ? leftReportedAt : 0;
    const safeRightReportedAt = Number.isFinite(rightReportedAt)
      ? rightReportedAt
      : 0;

    if (safeLeftReportedAt !== safeRightReportedAt) {
      return safeRightReportedAt - safeLeftReportedAt;
    }

    return right.uuid.localeCompare(left.uuid);
  }

  private hasReadyTransportForRoute(
    instance: ServiceInstanceDescriptor,
    role: ServiceTransportRole,
    origin: string,
  ): boolean {
    return instance.transports.some(
      (transport) =>
        !transport.deleted &&
        transport.role === role &&
        transport.origin === origin &&
        this.hasTransportClientReady(instance, transport),
    );
  }

  private retireSupersededInstancesForTransport(
    authoritativeInstance: ServiceInstanceDescriptor,
    authoritativeTransport: ServiceTransportDescriptor,
    emit: (signal: string, ctx: AnyObject) => void,
  ): void {
    if (this.serviceName === "CadenzaDB") {
      return;
    }

    const persistStructuralRetirement = false;
    const supersededInstances = this.findSupersededInstancesForTransport(
      authoritativeInstance.serviceName,
      authoritativeInstance.uuid,
      authoritativeTransport.role,
      authoritativeTransport.origin,
    );

    if (!supersededInstances.length) {
      return;
    }

    const instances = this.instances.get(authoritativeInstance.serviceName);

    for (const supersededInstance of supersededInstances) {
      this.markInstanceInactive(emit, supersededInstance, {
        deactivateTransports: true,
        persist: persistStructuralRetirement,
      });

      this.unregisterDependee(
        supersededInstance.uuid,
        authoritativeInstance.serviceName,
      );

      if (instances) {
        const index = instances.findIndex(
          (candidate) => candidate.uuid === supersededInstance.uuid,
        );
        if (index >= 0) {
          instances.splice(index, 1);
        }
      }
    }

    if (instances && instances.length === 0) {
      this.instances.delete(authoritativeInstance.serviceName);
    }
  }

  private shouldRetireSupersededInstancesForTransport(
    instance: ServiceInstanceDescriptor,
    transport: ServiceTransportDescriptor,
  ): boolean {
    return (
      transport.role === this.getInstanceRouteRole(instance) &&
      !transport.deleted &&
      (instance.uuid === this.serviceInstanceId ||
        this.hasReadyTransportForRoute(
          instance,
          transport.role,
          transport.origin,
        ))
    );
  }

  private async hydrateAuthorityInstanceForTransport(
    transport: ServiceTransportDescriptor,
  ): Promise<ServiceInstanceDescriptor | undefined> {
    if (this.serviceName !== "CadenzaDB") {
      return undefined;
    }

    const rows = await DatabaseController.instance.queryAuthorityTableRows(
      "service_instance",
      {
        fields: [
          "uuid",
          "service_name",
          "is_primary",
          "is_active",
          "is_non_responsive",
          "is_blocked",
          "number_of_running_graphs",
          "reported_at",
          "health",
          "is_frontend",
          "is_database",
          "deleted",
        ],
        filter: {
          uuid: transport.serviceInstanceId,
          deleted: false,
        },
      },
    );

    const normalized = normalizeServiceInstanceDescriptor(rows[0]);
    if (!normalized) {
      return undefined;
    }

    if (!this.instances.has(normalized.serviceName)) {
      this.instances.set(normalized.serviceName, []);
    }

    const instances = this.instances.get(normalized.serviceName)!;
    const existing = instances.find(
      (candidate) => candidate.uuid === normalized.uuid,
    );

    if (existing) {
      return existing;
    }

    instances.push({
      ...normalized,
      transports: [],
    });
    return instances[instances.length - 1];
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

    const resolvedInstance = instances.find(
      (instance) => instance.uuid === resolvedInstanceId,
    );
    const frontendRoutingRole = this.isFrontend
      ? this.getRoutingTransportRole()
      : null;

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
      const preservedTransportIds = new Set<string>();

      if (resolvedInstance && frontendRoutingRole) {
        const resolvedHasFrontendRoute = resolvedInstance.transports.some(
          (transport) => transport.role === frontendRoutingRole && !transport.deleted,
        );

        if (!resolvedHasFrontendRoute) {
          const transportsToPreserve = placeholder.transports.filter(
            (transport) =>
              transport.role === frontendRoutingRole && !transport.deleted,
          );

          for (const transport of transportsToPreserve) {
            if (
              resolvedInstance.transports.some(
                (existingTransport) =>
                  existingTransport.role === transport.role &&
                  existingTransport.origin === transport.origin,
              )
            ) {
              continue;
            }

            resolvedInstance.transports.push({
              ...transport,
              serviceInstanceId: resolvedInstanceId,
            });
            preservedTransportIds.add(transport.uuid);
          }
        }
      }

      for (const transport of placeholder.transports) {
        if (preservedTransportIds.has(transport.uuid)) {
          continue;
        }
        const transportKey = this.buildTransportRouteKey(serviceName, transport);
        this.emitTransportHandleShutdowns(emit, transportKey, transport);
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

  private adoptBootstrapPlaceholderInstanceId(
    serviceName: string,
    placeholderInstanceId: string,
    resolvedInstanceId: string,
  ): ServiceInstanceDescriptor | undefined {
    if (!serviceName || !placeholderInstanceId || !resolvedInstanceId) {
      return undefined;
    }

    const instances = this.instances.get(serviceName);
    if (!instances?.length) {
      return undefined;
    }

    const resolvedInstance = instances.find(
      (instance) => instance.uuid === resolvedInstanceId,
    );
    if (resolvedInstance) {
      return resolvedInstance;
    }

    const placeholder = instances.find(
      (instance) =>
        instance.uuid === placeholderInstanceId && instance.isBootstrapPlaceholder,
    );
    if (!placeholder) {
      return undefined;
    }

    const wasDependee = this.dependeeByInstance.has(placeholderInstanceId);
    const dependeeServiceName =
      this.dependeeByInstance.get(placeholderInstanceId) ?? serviceName;
    const requiredForReadiness = this.readinessDependeeByInstance.has(
      placeholderInstanceId,
    );
    const lastHeartbeatAt =
      this.lastHeartbeatAtByInstance.get(placeholderInstanceId) ?? Date.now();
    const missedHeartbeats =
      this.missedHeartbeatsByInstance.get(placeholderInstanceId) ?? 0;
    const inFlight = this.runtimeStatusFallbackInFlightByInstance.has(
      placeholderInstanceId,
    );

    this.dependeeByInstance.delete(placeholderInstanceId);
    this.readinessDependeeByInstance.delete(placeholderInstanceId);
    this.lastHeartbeatAtByInstance.delete(placeholderInstanceId);
    this.missedHeartbeatsByInstance.delete(placeholderInstanceId);
    this.runtimeStatusFallbackInFlightByInstance.delete(placeholderInstanceId);

    placeholder.uuid = resolvedInstanceId;
    placeholder.isBootstrapPlaceholder = false;
    placeholder.transports = placeholder.transports.map((transport) => ({
      ...transport,
      serviceInstanceId: resolvedInstanceId,
    }));

    if (wasDependee) {
      this.dependeeByInstance.set(resolvedInstanceId, dependeeServiceName);
      if (this.dependeesByService.has(dependeeServiceName)) {
        this.dependeesByService.get(dependeeServiceName)!.delete(
          placeholderInstanceId,
        );
        this.dependeesByService.get(dependeeServiceName)!.add(resolvedInstanceId);
      }
      this.lastHeartbeatAtByInstance.set(resolvedInstanceId, lastHeartbeatAt);
      this.missedHeartbeatsByInstance.set(resolvedInstanceId, missedHeartbeats);
    }

    if (requiredForReadiness) {
      this.readinessDependeeByInstance.set(resolvedInstanceId, serviceName);
      if (this.readinessDependeesByService.has(serviceName)) {
        this.readinessDependeesByService.get(serviceName)!.delete(
          placeholderInstanceId,
        );
        this.readinessDependeesByService.get(serviceName)!.add(resolvedInstanceId);
      }
    }

    if (inFlight) {
      this.runtimeStatusFallbackInFlightByInstance.add(resolvedInstanceId);
    }

    return placeholder;
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

  private getRuntimeStatusInactiveThreshold(): number {
    return Math.max(
      this.runtimeStatusInactiveMissThreshold,
      this.runtimeStatusMissThreshold + 1,
    );
  }

  private buildGracefulShutdownTransportDeactivationContext(
    instance: ServiceInstanceDescriptor,
  ): AnyObject {
    return {
      serviceName: instance.serviceName,
      serviceInstanceId: instance.uuid,
      transports: instance.transports.map((transport) => ({
        uuid: transport.uuid,
        service_instance_id: transport.serviceInstanceId,
        role: transport.role,
        origin: transport.origin,
        protocols: transport.protocols,
        deleted: transport.deleted ?? false,
      })),
    };
  }

  private applyInstanceLifecycleState(
    instance: ServiceInstanceDescriptor,
    {
      isActive,
      isNonResponsive,
      reportedAt,
      health,
    }: {
      isActive: boolean;
      isNonResponsive: boolean;
      reportedAt?: string;
      health?: AnyObject;
    },
  ): void {
    instance.isActive = isActive;
    instance.isNonResponsive = isNonResponsive;
    if (health && typeof health === "object") {
      instance.health = {
        ...(instance.health ?? {}),
        ...health,
      };
    }

    const snapshot = this.resolveRuntimeStatusSnapshot(
      instance.numberOfRunningGraphs ?? 0,
      instance.isActive,
      instance.isNonResponsive,
      instance.isBlocked,
    );
    instance.runtimeState = snapshot.state;
    instance.acceptingWork = snapshot.acceptingWork;
    instance.reportedAt = reportedAt ?? new Date().toISOString();
  }

  private emitInstanceLifecyclePersistence(
    emit: (signal: string, ctx: AnyObject) => void,
    instance: ServiceInstanceDescriptor,
    data: AnyObject,
    options: {
      persist?: boolean;
    } = {},
  ): void {
    emit("global.meta.service_instance.updated", {
      data,
      filter: {
        uuid: instance.uuid,
      },
    });
    if (options.persist === false) {
      return;
    }
    emit("meta.service_registry.instance_update_requested", {
      data,
      queryData: {
        filter: {
          uuid: instance.uuid,
        },
      },
      __serviceName: instance.serviceName,
    });
  }

  private deactivateInstanceTransports(
    emit: (signal: string, ctx: AnyObject) => void,
    instance: ServiceInstanceDescriptor,
    options: {
      persist: boolean;
    } = {
      persist: true,
    },
  ): void {
    for (const transport of instance.transports) {
      const transportKey = this.buildTransportRouteKey(
        instance.serviceName,
        transport,
      );
      this.clearRemoteRouteRecordIfCurrent(
        instance.serviceName,
        instance.uuid,
        transport,
      );
      this.emitTransportHandleShutdowns(emit, transportKey, transport);
      transport.deleted = true;
      this.clearTransportClientState(instance, transport);

      if (!isPersistedUuid(transport.uuid)) {
        continue;
      }

      emit("global.meta.service_instance_transport.updated", {
        data: {
          deleted: true,
        },
        filter: {
          uuid: transport.uuid,
        },
      });
      if (!options.persist) {
        continue;
      }
      emit("meta.service_registry.transport_update_requested", {
        data: {
          deleted: true,
        },
        queryData: {
          filter: {
            uuid: transport.uuid,
          },
        },
        __serviceName: instance.serviceName,
        __serviceInstanceId: instance.uuid,
      });
    }
  }

  private emitTransportDeletionPersistence(
    emit: (signal: string, ctx: AnyObject) => void,
    instance: ServiceInstanceDescriptor,
    transportId: string,
  ): void {
    emit("global.meta.service_instance_transport.updated", {
      data: {
        deleted: true,
      },
      filter: {
        uuid: transportId,
      },
    });
    emit("meta.service_registry.transport_update_requested", {
      data: {
        deleted: true,
      },
      queryData: {
        filter: {
          uuid: transportId,
        },
      },
      __serviceName: instance.serviceName,
      __serviceInstanceId: instance.uuid,
    });
  }

  private markInstanceInactive(
    emit: (signal: string, ctx: AnyObject) => void,
    instance: ServiceInstanceDescriptor,
    options: {
      deactivateTransports?: boolean;
      persistDeleted?: boolean;
      persist?: boolean;
    } = {},
  ): void {
    this.applyInstanceLifecycleState(instance, {
      isActive: false,
      isNonResponsive: false,
    });

    if (options.deactivateTransports) {
      this.deactivateInstanceTransports(emit, instance, {
        persist: options.persist !== false,
      });
    }

    this.emitInstanceLifecyclePersistence(emit, instance, {
      is_active: false,
      is_non_responsive: false,
      deleted: options.persistDeleted === true ? true : false,
      last_active: instance.reportedAt,
      health: instance.health ?? {},
    }, {
      persist: options.persist !== false,
    });
  }

  private handleLocalInboundActivity(
    emit: (signal: string, ctx: AnyObject) => void,
    activityAt?: string,
  ): boolean {
    const localInstance = this.getLocalInstance();
    if (!localInstance) {
      return false;
    }

    const previousActive = localInstance.isActive;
    const previousNonResponsive = localInstance.isNonResponsive;
    const previousReportedAtMs = Date.parse(localInstance.reportedAt ?? "");
    const nextReportedAt =
      typeof activityAt === "string" && activityAt.trim().length > 0
        ? activityAt
        : new Date().toISOString();

    this.applyInstanceLifecycleState(localInstance, {
      isActive: true,
      isNonResponsive: false,
      reportedAt: nextReportedAt,
    });

    const previousReportedAtWasValid = Number.isFinite(previousReportedAtMs);
    const shouldPersistActivity =
      !previousActive ||
      previousNonResponsive ||
      !previousReportedAtWasValid ||
      Date.now() - previousReportedAtMs >= this.runtimeStatusHeartbeatIntervalMs;

    if (!shouldPersistActivity) {
      return true;
    }

    emit("meta.service_registry.runtime_status_broadcast_requested", {
      reason: "inbound-activity",
      force: previousNonResponsive || !previousActive,
    });

    return true;
  }

  private handleRemoteActivityObserved(ctx: AnyObject): boolean {
    const serviceName = String(ctx.serviceName ?? "").trim();
    const serviceInstanceId = String(ctx.serviceInstanceId ?? "").trim();
    if (!serviceName || !serviceInstanceId) {
      return false;
    }

    if (
      serviceName === this.serviceName &&
      serviceInstanceId === this.serviceInstanceId
    ) {
      return false;
    }

    const instance = this.getInstance(serviceName, serviceInstanceId);
    if (!instance) {
      return false;
    }

    const nextReportedAt =
      typeof ctx.activityAt === "string" && ctx.activityAt.trim().length > 0
        ? ctx.activityAt
        : new Date().toISOString();

    this.applyInstanceLifecycleState(instance, {
      isActive: true,
      isNonResponsive: false,
      reportedAt: nextReportedAt,
    });

    const transportId = String(ctx.serviceTransportId ?? "").trim();
    if (transportId) {
      this.clearTransportFailureState(instance.uuid, transportId);
      const transport = this.getTransportById(instance, transportId);
      if (transport) {
        this.markTransportClientReady(instance, transport);
      }
    }

    this.lastHeartbeatAtByInstance.set(serviceInstanceId, Date.now());
    this.missedHeartbeatsByInstance.set(serviceInstanceId, 0);
    this.runtimeStatusFallbackInFlightByInstance.delete(serviceInstanceId);
    this.refreshRoutingCooldownsForService(serviceName);

    return true;
  }

  private resolveRouteFreshnessTimestamp(instance: ServiceInstanceDescriptor): number {
    const reportedAtMs = Date.parse(instance.reportedAt ?? "");
    return Number.isFinite(reportedAtMs) ? reportedAtMs : 0;
  }

  private collapseInstancesByRouteOrigin(
    instances: ServiceInstanceDescriptor[],
    ctx: AnyObject,
    preferredRole: ServiceTransportRole,
  ): ServiceInstanceDescriptor[] {
    const bestByRouteKey = new Map<string, ServiceInstanceDescriptor>();

    for (const instance of instances) {
      if (instance.isFrontend) {
        bestByRouteKey.set(instance.uuid, instance);
        continue;
      }

      const transport = this.selectReadyTransportForInstance(
        instance,
        ctx,
        preferredRole,
      );
      if (!transport) {
        continue;
      }

      const routeKey = `${transport.role}|${transport.origin}`;
      const current = bestByRouteKey.get(routeKey);
      if (!current) {
        bestByRouteKey.set(routeKey, instance);
        continue;
      }

      const currentFreshness = this.resolveRouteFreshnessTimestamp(current);
      const candidateFreshness = this.resolveRouteFreshnessTimestamp(instance);
      if (candidateFreshness !== currentFreshness) {
        if (candidateFreshness > currentFreshness) {
          bestByRouteKey.set(routeKey, instance);
        }
        continue;
      }

      if ((instance.numberOfRunningGraphs ?? 0) < (current.numberOfRunningGraphs ?? 0)) {
        bestByRouteKey.set(routeKey, instance);
      }
    }

    return Array.from(bestByRouteKey.values());
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

  private captureRuntimeMetricsSample(): RuntimeMetricsSnapshot | null {
    if (!this.runtimeMetricsSampler) {
      return null;
    }

    const snapshot = this.runtimeMetricsSampler.sample();
    this.latestRuntimeMetricsSnapshot = snapshot;

    const localInstance = this.getLocalInstance();
    if (localInstance) {
      localInstance.health = {
        ...(localInstance.health ?? {}),
        cpuUsage: snapshot.cpuUsage,
        memoryUsage: snapshot.memoryUsage,
        eventLoopLag: snapshot.eventLoopLag,
        runtimeMetrics: {
          ...(localInstance.health?.runtimeMetrics ?? {}),
          ...snapshot,
        },
      };
    }

    return snapshot;
  }

  private getLatestRuntimeMetricsSnapshot(): RuntimeMetricsSnapshot | null {
    return (
      this.latestRuntimeMetricsSnapshot ??
      this.captureRuntimeMetricsSample()
    );
  }

  private buildRuntimeMetricsHealthPayload(
    snapshot: RuntimeMetricsSnapshot | null,
  ): AnyObject | undefined {
    if (!snapshot) {
      return undefined;
    }

    return {
      cpuUsage: snapshot.cpuUsage,
      memoryUsage: snapshot.memoryUsage,
      eventLoopLag: snapshot.eventLoopLag,
      runtimeMetrics: {
        sampledAt: snapshot.sampledAt,
        cpuUsage: snapshot.cpuUsage,
        memoryUsage: snapshot.memoryUsage,
        eventLoopLag: snapshot.eventLoopLag,
        rssBytes: snapshot.rssBytes,
        heapUsedBytes: snapshot.heapUsedBytes,
        heapTotalBytes: snapshot.heapTotalBytes,
        memoryLimitBytes: snapshot.memoryLimitBytes,
      },
    };
  }

  private stripRuntimeStatusReportForPeer(
    report: RuntimeStatusReport,
  ): RuntimeStatusReport {
    const nextReport: RuntimeStatusReport = {
      ...report,
    };

    delete nextReport.health;
    return nextReport;
  }

  private buildPeerRuntimeStatusSnapshot(
    report: RuntimeStatusReport,
  ): PeerRuntimeStatusSnapshot {
    return {
      state: report.state,
      acceptingWork: report.acceptingWork,
      numberOfRunningGraphs: report.numberOfRunningGraphs,
      isActive: report.isActive,
      isNonResponsive: report.isNonResponsive,
      isBlocked: report.isBlocked,
      cpuUsage: report.cpuUsage ?? null,
      memoryUsage: report.memoryUsage ?? null,
      eventLoopLag: report.eventLoopLag ?? null,
    };
  }

  private hasSignificantPeerRuntimeStatusChange(
    previous: PeerRuntimeStatusSnapshot | null,
    next: PeerRuntimeStatusSnapshot,
  ): boolean {
    if (!previous) {
      return true;
    }

    if (
      previous.state !== next.state ||
      previous.acceptingWork !== next.acceptingWork ||
      previous.numberOfRunningGraphs !== next.numberOfRunningGraphs ||
      previous.isActive !== next.isActive ||
      previous.isNonResponsive !== next.isNonResponsive ||
      previous.isBlocked !== next.isBlocked
    ) {
      return true;
    }

    const hasMetricChanged = (
      left: number | null,
      right: number | null,
      threshold: number,
    ): boolean => {
      if (left === null && right === null) {
        return false;
      }

      if (left === null || right === null) {
        return true;
      }

      return Math.abs(left - right) >= threshold;
    };

    return (
      hasMetricChanged(previous.cpuUsage, next.cpuUsage, 0.01) ||
      hasMetricChanged(previous.memoryUsage, next.memoryUsage, 0.01) ||
      hasMetricChanged(previous.eventLoopLag, next.eventLoopLag, 5)
    );
  }

  private readRuntimeStatusMetric(
    ctx: AnyObject,
    ...keys: string[]
  ): number | null | undefined {
    for (const key of keys) {
      const value = ctx[key] ?? ctx[`__${key}`];
      if (typeof value === "number" && Number.isFinite(value)) {
        return value;
      }
    }

    const health =
      ctx.health && typeof ctx.health === "object"
        ? (ctx.health as AnyObject)
        : ctx.__health && typeof ctx.__health === "object"
          ? (ctx.__health as AnyObject)
          : null;
    if (!health) {
      return undefined;
    }

    for (const key of keys) {
      const value = health[key];
      if (typeof value === "number" && Number.isFinite(value)) {
        return value;
      }
    }

    const runtimeMetrics =
      health.runtimeMetrics && typeof health.runtimeMetrics === "object"
        ? (health.runtimeMetrics as AnyObject)
        : null;
    if (!runtimeMetrics) {
      return undefined;
    }

    for (const key of keys) {
      const value = runtimeMetrics[key];
      if (typeof value === "number" && Number.isFinite(value)) {
        return value;
      }
    }

    return undefined;
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
    const transportRole =
      ctx.transportRole ??
      ctx.transport_role ??
      ctx.serviceTransport?.role ??
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
      transportRole:
        transportRole === "internal" || transportRole === "public"
          ? transportRole
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
      cpuUsage: this.readRuntimeStatusMetric(ctx, "cpuUsage", "cpu", "cpuLoad"),
      memoryUsage: this.readRuntimeStatusMetric(
        ctx,
        "memoryUsage",
        "memory",
        "memoryPressure",
      ),
      eventLoopLag: this.readRuntimeStatusMetric(
        ctx,
        "eventLoopLag",
        "eventLoopLagMs",
      ),
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
        if (report.transportRole) {
          existingTransport.role = report.transportRole;
        }
        existingTransport.origin = report.transportOrigin;
        existingTransport.protocols = protocols;
      } else {
        instance.transports.push({
          uuid: report.transportId,
          serviceInstanceId: report.serviceInstanceId,
          role: report.transportRole ?? this.getRoutingTransportRole(),
          origin: report.transportOrigin,
          protocols,
          securityProfile: null,
          authStrategy: null,
        });
      }

      const currentTransport = this.getTransportById(instance, report.transportId);
      if (currentTransport) {
        this.upsertRemoteRouteRecord(
          report.serviceName,
          report.serviceInstanceId,
          currentTransport,
        );
      }
    }

    if (typeof report.isFrontend === "boolean") {
      instance.isFrontend = report.isFrontend;
    }

    instance.numberOfRunningGraphs = report.numberOfRunningGraphs;
    const runtimeMetricsHealth = {
      cpuUsage: report.cpuUsage ?? null,
      memoryUsage: report.memoryUsage ?? null,
      eventLoopLag: report.eventLoopLag ?? null,
      runtimeMetrics:
        report.health?.runtimeMetrics &&
        typeof report.health.runtimeMetrics === "object"
          ? report.health.runtimeMetrics
          : undefined,
    };
    instance.isActive = report.isActive;
    instance.isNonResponsive = report.isNonResponsive;
    instance.isBlocked = report.isBlocked;
    instance.runtimeState = report.state;
    instance.acceptingWork = report.acceptingWork;
    instance.reportedAt = report.reportedAt;
    instance.health = {
      ...(instance.health ?? {}),
      ...(report.health ?? {}),
      ...runtimeMetricsHealth,
      runtimeStatus: {
        state: report.state,
        acceptingWork: report.acceptingWork,
        reportedAt: report.reportedAt,
      },
    };
    this.lastHeartbeatAtByInstance.set(report.serviceInstanceId, Date.now());
    this.missedHeartbeatsByInstance.set(report.serviceInstanceId, 0);
    this.runtimeStatusFallbackInFlightByInstance.delete(
      report.serviceInstanceId,
    );

    return true;
  }

  private retireLocalRemoteInstance(
    instance: ServiceInstanceDescriptor,
  ): void {
    for (const transport of instance.transports) {
      this.clearTransportFailureState(instance.uuid, transport.uuid);
      const transportKey = this.buildTransportRouteKey(
        instance.serviceName,
        transport,
      );
      this.clearTransportClientState(instance, transport);
      transport.deleted = true;
      const activeRoute = this.remoteRoutesByKey.get(transportKey);
      if (
        !activeRoute ||
        (activeRoute.serviceInstanceId === instance.uuid &&
          activeRoute.serviceTransportId === transport.uuid)
      ) {
        this.clearRemoteRouteRecordIfCurrent(
          instance.serviceName,
          instance.uuid,
          transport,
        );
        this.emitTransportHandleShutdowns(Cadenza.emit, transportKey, transport);
      }
    }

    this.unregisterDependee(instance.uuid, instance.serviceName);
    this.lastHeartbeatAtByInstance.delete(instance.uuid);
    this.missedHeartbeatsByInstance.delete(instance.uuid);
    this.runtimeStatusFallbackInFlightByInstance.delete(instance.uuid);

    const instances = this.instances.get(instance.serviceName);
    if (!instances) {
      return;
    }

    const index = instances.findIndex((candidate) => candidate.uuid === instance.uuid);
    if (index >= 0) {
      instances.splice(index, 1);
    }

    if (instances.length === 0) {
      this.instances.delete(instance.serviceName);
    }
  }

  private applyRuntimeStatusIdentityReplacement(
    staleInstance: ServiceInstanceDescriptor,
    report: RuntimeStatusReport,
    diagnostic: RuntimeStatusFallbackRestDiagnostic,
  ): boolean {
    if (report.serviceName !== staleInstance.serviceName) {
      return false;
    }

    const authoritativeOrigin =
      report.transportOrigin ?? diagnostic.transport?.origin ?? null;
    const authoritativeRole =
      report.transportRole ??
      diagnostic.transport?.role ??
      this.getInstanceRouteRole(staleInstance);

    if (!authoritativeOrigin) {
      return false;
    }

    const staleOriginMatch = staleInstance.transports.some(
      (transport) =>
        !transport.deleted &&
        transport.role === authoritativeRole &&
        transport.origin === authoritativeOrigin,
    );
    if (!staleOriginMatch) {
      return false;
    }

    if (!this.instances.has(report.serviceName)) {
      this.instances.set(report.serviceName, []);
    }

    const instances = this.instances.get(report.serviceName)!;
    let replacement =
      this.getInstance(report.serviceName, report.serviceInstanceId) ?? null;

    if (!replacement) {
      replacement = {
        ...staleInstance,
        uuid: report.serviceInstanceId,
        numberOfRunningGraphs: report.numberOfRunningGraphs,
        isActive: report.isActive,
        isNonResponsive: report.isNonResponsive,
        isBlocked: report.isBlocked,
        runtimeState: report.state,
        acceptingWork: report.acceptingWork,
        reportedAt: report.reportedAt,
        health: {
          ...(staleInstance.health ?? {}),
          ...(report.health ?? {}),
        },
        isFrontend:
          typeof report.isFrontend === "boolean"
            ? report.isFrontend
            : staleInstance.isFrontend,
        transports: [],
        clientCreatedTransportIds: [],
        clientPendingTransportIds: [],
        clientReadyTransportIds: [],
      };
      instances.push(replacement);
    }

    const protocols =
      report.transportProtocols && report.transportProtocols.length > 0
        ? report.transportProtocols
        : diagnostic.transport?.protocols ?? (["rest"] as ServiceTransportProtocol[]);
    const replacementTransportId =
      report.transportId ??
      replacement.transports.find(
        (transport) =>
          transport.role === authoritativeRole &&
          transport.origin === authoritativeOrigin &&
          !transport.deleted,
      )?.uuid ??
      diagnostic.transport?.uuid ??
      uuid();

    const existingTransport = this.getTransportById(
      replacement,
      replacementTransportId,
    );
    if (existingTransport) {
      existingTransport.serviceInstanceId = replacement.uuid;
      existingTransport.role = authoritativeRole;
      existingTransport.origin = authoritativeOrigin;
      existingTransport.protocols = protocols;
      existingTransport.deleted = false;
    } else {
      replacement.transports.push({
        uuid: replacementTransportId,
        serviceInstanceId: replacement.uuid,
        role: authoritativeRole,
        origin: authoritativeOrigin,
        protocols,
        securityProfile: null,
        authStrategy: null,
        deleted: false,
      });
    }

    if (!this.applyRuntimeStatusReport(report)) {
      return false;
    }

    this.clearTransportFailureState(replacement.uuid, replacementTransportId);
    const replacementTransport = this.getTransportById(
      replacement,
      replacementTransportId,
    );
    if (replacementTransport) {
      this.markTransportClientReady(replacement, replacementTransport);
    }
    this.retireLocalRemoteInstance(staleInstance);
    this.refreshRoutingCooldownsForService(report.serviceName);

    return true;
  }

  public applyAuthorityRuntimeStatusReport(
    report: AuthorityRuntimeStatusReport,
  ): boolean {
    return this.applyRuntimeStatusReport(report);
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
    const runtimeMetricsSnapshot = this.getLatestRuntimeMetricsSnapshot();

    const snapshot = this.resolveRuntimeStatusSnapshot(
      numberOfRunningGraphs,
      localInstance.isActive,
      localInstance.isNonResponsive,
      localInstance.isBlocked,
    );
    const reportedAt = new Date().toISOString();

    const transport = this.getRouteableTransport(
      localInstance,
      this.useSocket ? "socket" : "rest",
      "internal",
    );

    const report: RuntimeStatusReport = {
      serviceName: this.serviceName,
      serviceInstanceId: this.serviceInstanceId,
      transportId: transport?.uuid ?? undefined,
      transportRole: transport?.role ?? undefined,
      transportOrigin: transport?.origin ?? undefined,
      transportProtocols: transport?.protocols ?? undefined,
      isFrontend: localInstance.isFrontend,
      reportedAt,
      state: snapshot.state,
      acceptingWork: snapshot.acceptingWork,
      numberOfRunningGraphs: snapshot.numberOfRunningGraphs,
      cpuUsage: runtimeMetricsSnapshot?.cpuUsage ?? null,
      memoryUsage: runtimeMetricsSnapshot?.memoryUsage ?? null,
      eventLoopLag: runtimeMetricsSnapshot?.eventLoopLag ?? null,
      isActive: snapshot.isActive,
      isNonResponsive: snapshot.isNonResponsive,
      isBlocked: snapshot.isBlocked,
      health: {
        ...(localInstance.health ?? {}),
        ...(this.buildRuntimeMetricsHealthPayload(runtimeMetricsSnapshot) ?? {}),
        runtimeStatus: {
          state: snapshot.state,
          acceptingWork: snapshot.acceptingWork,
          reportedAt,
        },
      },
    };

    this.applyRuntimeStatusReport(report);
    return detailLevel === "full"
      ? report
      : this.stripRuntimeStatusReportForPeer(report);
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

    if (
      instance &&
      directStatusCheck.replacementReport &&
      this.applyRuntimeStatusIdentityReplacement(
        instance,
        directStatusCheck.replacementReport,
        directStatusCheck.diagnostic,
      )
    ) {
      return {
        report: directStatusCheck.replacementReport,
        inquiryMeta: {
          inquiry: META_RUNTIME_STATUS_INTENT,
          responded: 1,
          failed: 0,
          timedOut: 0,
          pending: 0,
          directStatusCheck: true,
          identityReplacement: true,
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
    replacementReport?: RuntimeStatusReport | null;
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
          replacementReport:
            report.serviceName === serviceName &&
            (report.transportOrigin ?? transport.origin) === transport.origin
              ? report
              : null,
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

  private shouldRefreshRuntimeStatusViaRest(
    instance: ServiceInstanceDescriptor,
    now = Date.now(),
  ): boolean {
    if (!instance.isActive || instance.isBlocked) {
      return false;
    }

    if (this.hasReadyClientTransportForProtocol(instance, "socket")) {
      return false;
    }

    if (!this.getRouteableTransport(instance, "rest")) {
      return false;
    }

    const lastHeartbeatAt = this.lastHeartbeatAtByInstance.get(instance.uuid) ?? 0;
    if (lastHeartbeatAt <= 0) {
      return true;
    }

    return now - lastHeartbeatAt >= this.runtimeStatusRestRefreshIntervalMs;
  }

  private getAuthorityRestTransport():
    | {
        instance: ServiceInstanceDescriptor;
        transport: ServiceTransportDescriptor;
      }
    | null {
    const authorityInstances = this.instances.get("CadenzaDB") ?? [];

    for (const instance of authorityInstances) {
      const transport = this.getRouteableTransport(instance, "rest", "internal");
      if (transport) {
        return {
          instance,
          transport,
        };
      }
    }

    return null;
  }

  private async delegateAuthorityLifecycleUpdate(
    remoteRoutineName: string,
    context: AnyObject,
    timeoutMs: number,
  ): Promise<boolean> {
    if (typeof globalThis.fetch !== "function") {
      return false;
    }

    const authorityTarget = this.getAuthorityRestTransport();
    if (!authorityTarget) {
      return false;
    }

    const controller =
      typeof AbortController === "function" ? new AbortController() : null;
    const timeoutId = controller
      ? setTimeout(() => controller.abort(), timeoutMs)
      : null;

    try {
      const requestBody = stripDelegationRequestSnapshot(
        ensureDelegationContextMetadata(
          attachDelegationRequestSnapshot({
            ...context,
            __remoteRoutineName: remoteRoutineName,
            __serviceName: "CadenzaDB",
            __localServiceName: this.serviceName,
            __timeout: timeoutMs,
            __syncing: true,
            __metadata: {
              ...(context.__metadata && typeof context.__metadata === "object"
                ? context.__metadata
                : {}),
              __timeout: timeoutMs,
              __syncing: true,
            },
          }),
        ),
      );

      const response = await globalThis.fetch(
        `${authorityTarget.transport.origin}/delegation`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(requestBody),
          signal: controller?.signal,
        },
      );

      if ("ok" in response && response.ok === false) {
        return false;
      }

      const payload =
        typeof response.json === "function" ? await response.json() : response;

      return !(
        payload?.errored === true ||
        payload?.failed === true ||
        payload?.__status === "error"
      );
    } catch {
      return false;
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  }

  async reportLocalShutdownToAuthority(
    reason: string,
    timeoutMs: number,
  ): Promise<boolean> {
    if (
      !this.connectsToCadenzaDB ||
      this.serviceName === "CadenzaDB" ||
      !this.serviceInstanceId
    ) {
      return false;
    }

    const localInstance = this.getLocalInstance();
    if (!localInstance || !isPersistedUuid(localInstance.uuid)) {
      return false;
    }

    const reportedAt = new Date().toISOString();
    this.applyInstanceLifecycleState(localInstance, {
      isActive: false,
      isNonResponsive: false,
      reportedAt,
    });

    const instancePersisted = await this.delegateAuthorityLifecycleUpdate(
      "Update service_instance",
      {
        reason,
        graceful: true,
        data: {
          is_active: false,
          is_non_responsive: false,
          deleted: false,
          last_active: localInstance.reportedAt,
          health: localInstance.health ?? {},
        },
        queryData: {
          filter: {
            uuid: localInstance.uuid,
          },
        },
        __serviceInstanceId: localInstance.uuid,
      },
      Math.max(1_000, timeoutMs),
    );

    if (!instancePersisted) {
      return false;
    }

    for (const transport of localInstance.transports) {
      if (!isPersistedUuid(transport.uuid)) {
        continue;
      }

      transport.deleted = true;
      await this.delegateAuthorityLifecycleUpdate(
        "Update service_instance_transport",
        {
          reason,
          graceful: true,
          data: {
            deleted: true,
          },
          queryData: {
            filter: {
              uuid: transport.uuid,
            },
          },
          __serviceInstanceId: localInstance.uuid,
          __serviceName: localInstance.serviceName,
        },
        Math.max(500, Math.floor(timeoutMs / 2)),
      );
    }

    return true;
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

    Cadenza.defineIntent({
      name: AUTHORITY_RUNTIME_STATUS_REPORT_INTENT,
      description:
        "Apply one lightweight runtime-status report on authority without persisting heartbeat chatter to the durable service-instance registry.",
      input: {
        type: "object",
        properties: {
          serviceName: {
            type: "string",
          },
          serviceInstanceId: {
            type: "string",
          },
          reportedAt: {
            type: "string",
          },
        },
        required: ["serviceName", "serviceInstanceId", "reportedAt"],
      },
      output: {
        type: "object",
        properties: {
          applied: {
            type: "boolean",
          },
          serviceName: {
            type: "string",
          },
          serviceInstanceId: {
            type: "string",
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
              const transportKey = this.buildTransportRouteKey(
                existingInstance.serviceName,
                transport,
              );
              const activeRoute = this.remoteRoutesByKey.get(transportKey);
              if (
                !activeRoute ||
                (activeRoute.serviceInstanceId === existingInstance.uuid &&
                  activeRoute.serviceTransportId === transport.uuid)
              ) {
                this.clearRemoteRouteRecordIfCurrent(
                  existingInstance.serviceName,
                  existingInstance.uuid,
                  transport,
                );
                this.emitTransportHandleShutdowns(emit, transportKey, transport);
              }
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
        const previousReportedAtMs = Date.parse(existing?.reportedAt ?? "");

        if (existing) {
          Object.assign(existing, {
            ...serviceInstance,
            transports:
              serviceInstance.transports.length > 0
                ? serviceInstance.transports
                : existing.transports,
            clientCreatedTransportIds: existing.clientCreatedTransportIds ?? [],
            clientPendingTransportIds: existing.clientPendingTransportIds ?? [],
            clientReadyTransportIds: existing.clientReadyTransportIds ?? [],
          });
        } else {
          instances.push(serviceInstance);
        }

        const trackedInstance =
          existing ?? instances.find((instance) => instance.uuid === uuid);
        if (trackedInstance) {
          for (const transport of trackedInstance.transports) {
            if (!transport.deleted) {
              this.upsertRemoteRouteRecord(serviceName, trackedInstance.uuid, transport);
            }
          }
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

          const incomingReportedAtMs = Date.parse(trackedInstance.reportedAt ?? "");
          const reportedAtAdvanced =
            Number.isFinite(incomingReportedAtMs) &&
            (!Number.isFinite(previousReportedAtMs) ||
              incomingReportedAtMs > previousReportedAtMs);

          if (
            reportedAtAdvanced &&
            trackedInstance.isActive &&
            !trackedInstance.isNonResponsive
          ) {
            this.lastHeartbeatAtByInstance.set(uuid, Date.now());
            this.missedHeartbeatsByInstance.set(uuid, 0);
            this.runtimeStatusFallbackInFlightByInstance.delete(uuid);
          }
        }

        this.refreshRoutingCooldownsForService(serviceName);

        if (!serviceInstance.isBootstrapPlaceholder) {
          this.reconcileBootstrapPlaceholderInstance(serviceName, uuid, emit);
        }

        const routeableTransport =
          trackedInstance &&
          this.getRouteableTransport(
            trackedInstance,
            undefined,
            this.getInstanceRouteRole(trackedInstance),
          );
        if (
          trackedInstance &&
          routeableTransport &&
          this.shouldRetireSupersededInstancesForTransport(
            trackedInstance,
            routeableTransport,
          )
        ) {
          this.retireSupersededInstancesForTransport(
            trackedInstance,
            routeableTransport,
            emit,
          );
        }

        if (this.serviceName === serviceName) {
          return false;
        }

        if (trackedInstance?.isFrontend) {
          return true;
        }

        if (
          this.deputies.has(serviceName) ||
          this.remoteIntents.has(serviceName) ||
          this.remoteSignals.has(serviceName)
        ) {
          const shouldEagerlyConnect =
            this.shouldEagerlyEstablishRemoteClients(ctx);
          const connected = shouldEagerlyConnect
            ? this.ensureDependeeClientForInstance(trackedInstance!, emit, ctx)
            : false;

          if (shouldEagerlyConnect && !connected) {
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
      async (ctx, emit) => {
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
          ownerInstance = await this.hydrateAuthorityInstanceForTransport(transport);
        }

        if (!ownerInstance) {
          return false;
        }

        if (transport.deleted) {
          this.clearTransportFailureState(ownerInstance.uuid, transport.uuid);
          const transportKey = this.buildTransportRouteKey(
            ownerInstance.serviceName,
            transport,
          );
          this.clearTransportClientState(ownerInstance, transport);
          ownerInstance.transports = ownerInstance.transports.filter(
            (existingTransport) => existingTransport.uuid !== transport.uuid,
          );
          const activeRoute = this.remoteRoutesByKey.get(transportKey);
          if (
            !activeRoute ||
            (activeRoute.serviceInstanceId === ownerInstance.uuid &&
              activeRoute.serviceTransportId === transport.uuid)
          ) {
            this.clearRemoteRouteRecordIfCurrent(
              ownerInstance.serviceName,
              ownerInstance.uuid,
              transport,
            );
            this.emitTransportHandleShutdowns(emit, transportKey, transport);
          }
          this.refreshRoutingCooldownsForService(ownerInstance.serviceName);
          return true;
        }

        const existingTransport = this.getTransportById(ownerInstance, transport.uuid);
        if (existingTransport) {
          Object.assign(existingTransport, transport);
        } else {
          ownerInstance.transports.push(transport);
        }

        this.upsertRemoteRouteRecord(
          ownerInstance.serviceName,
          ownerInstance.uuid,
          transport,
        );

        this.clearTransportFailureState(ownerInstance.uuid, transport.uuid);

        this.refreshRoutingCooldownsForService(ownerInstance.serviceName);

        const hasRemoteInterest =
          !ownerInstance.isFrontend &&
          (this.deputies.has(ownerInstance.serviceName) ||
            this.remoteIntents.has(ownerInstance.serviceName) ||
            this.remoteSignals.has(ownerInstance.serviceName)) &&
          transport.role === this.getRoutingTransportRole();

        if (this.shouldRetireSupersededInstancesForTransport(ownerInstance, transport)) {
          this.retireSupersededInstancesForTransport(ownerInstance, transport, emit);
        }

        if (ownerInstance.uuid === this.serviceInstanceId) {
          if (
            !ownerInstance.isFrontend &&
            transport.role === this.getInstanceRouteRole(ownerInstance)
          ) {
            this.markTransportClientReady(ownerInstance, transport);
          }
          return true;
        }

        if (!hasRemoteInterest) {
          return true;
        }

        if (this.shouldEagerlyEstablishRemoteClients(ctx)) {
          this.ensureDependeeClientForInstance(ownerInstance, emit, ctx);
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

        this.markTransportReadyFromContext(ctx);
        this.registerDependee(ctx.serviceName, ctx.serviceInstanceId, {
          requiredForReadiness: this.resolveRequiredReadinessFromCommunicationTypes(
            ctx.communicationTypes,
          ),
        });
        this.refreshRoutingCooldownsForService(String(ctx.serviceName ?? "").trim());
        return true;
      },
      "Tracks handshake-ready remote dependency instances for runtime heartbeat monitoring.",
    ).doOn("meta.fetch.handshake_complete", "meta.socket.handshake");

    const normalizeServiceInstancesFromSync = (ctx: AnyObject) =>
      this.normalizeServiceInstancesFromSync(ctx);
    Cadenza.createMetaTask("Split service instances", function* (ctx: any) {
      const serviceInstances = normalizeServiceInstancesFromSync(ctx);
      if (serviceInstances.length === 0) {
        return;
      }

      for (const serviceInstance of serviceInstances) {
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
        const signalToTaskMaps = this.normalizeSignalMaps(ctx);
        const authoritativeSignalMaps = this.readArrayPayload(ctx, [
          "signalToTaskMaps",
          "signal_to_task_maps",
          "signalToTaskMap",
          "signal_to_task_map",
        ]);

        if (authoritativeSignalMaps.length > 0) {
          this.rememberKnownGlobalSignalMaps(signalToTaskMaps);
        }

        return this.applyGlobalSignalRegistrations(signalToTaskMaps);
      },
      "Handles registration of remote signals",
    )
      .emits("meta.service_registry.registered_global_signals")
      .doOn("global.meta.graph_metadata.task_signal_observed");

    this.reconcileGatheredSyncTransmissionsTask = Cadenza.createMetaTask(
      "Reconcile gathered sync signal transmissions",
      () => false,
      "Legacy gathered-sync signal transmission is retired; structural updates now propagate through manifest snapshots and explicit full sync pulls.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn(META_GATHERED_SYNC_TRANSMISSION_RECONCILE_SIGNAL);

    this.handleGlobalIntentRegistrationTask = Cadenza.createMetaTask(
      "Handle global intent registration",
      (ctx, emit) => {
        const intentToTaskMaps = this.normalizeIntentMaps(ctx);
        if (shouldTraceServiceRegistry(this.serviceName)) {
          console.log("[CADENZA_SERVICE_REGISTRY_TRACE] handle_global_intents", {
            localServiceName: this.serviceName,
            intentCount: intentToTaskMaps.length,
            sample: intentToTaskMaps.slice(0, 5),
          });
        }
        const sorted = intentToTaskMaps.sort((a, b) => {
          if (a.deleted && !b.deleted) return -1;
          if (!a.deleted && b.deleted) return 1;
          return 0;
        });

        for (const map of sorted) {
          try {
            if (map.deleted) {
              this.unregisterRemoteIntentDeputy(map);
              continue;
            }

            if (map.serviceName === this.serviceName) {
              continue;
            }

            Cadenza.inquiryBroker.addIntent({
              name: map.intentName,
            });

            this.registerRemoteIntentDeputy(map);

            if (this.shouldEagerlyEstablishRemoteClients(ctx)) {
              this.ensureDependeeClientsForService(map.serviceName, emit, ctx);
            }
          } catch (error) {
            throw error;
          }
        }

        return true;
      },
      "Handles registration of remote inquiry intent responders",
    )
      .emits("meta.service_registry.registered_global_intents")
      .doOn("global.meta.graph_metadata.task_intent_associated");

    this.handleServiceNotRespondingTask = Cadenza.createMetaTask(
      "Handle service not responding",
      (ctx, emit) => {
        if (!this.isCurrentRouteContext(ctx)) {
          return false;
        }

        this.clearTransportReadyFromContext(ctx);
        const { serviceName, serviceInstanceId, serviceTransportId } = ctx;
        if (
          serviceName === "CadenzaDB" &&
          this.hasAuthorityBootstrapHandshakeEstablished()
        ) {
          const currentAuthorityInstanceId = String(
            this.authorityBootstrapRoute.serviceInstanceId ?? "",
          ).trim();
          const currentAuthorityTransportId = String(
            this.authorityBootstrapRoute.serviceTransportId ?? "",
          ).trim();
          const incomingInstanceId = String(serviceInstanceId ?? "").trim();
          const incomingTransportId = String(serviceTransportId ?? "").trim();
          const staleAuthorityInstance =
            incomingInstanceId &&
            currentAuthorityInstanceId &&
            incomingInstanceId !== currentAuthorityInstanceId;
          const staleAuthorityTransport =
            incomingTransportId &&
            currentAuthorityTransportId &&
            incomingTransportId !== currentAuthorityTransportId;

          if (staleAuthorityInstance || staleAuthorityTransport) {
            return true;
          }
        }

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
          if (
            instance.serviceName === this.serviceName &&
            instance.uuid === this.serviceInstanceId
          ) {
            continue;
          }

          const affectedTransport = serviceTransportId
            ? this.getTransportById(instance, serviceTransportId)
            : undefined;

          if (
            affectedTransport &&
            this.hasAnyReadyClientProtocolForTransport(instance, affectedTransport)
          ) {
            continue;
          }

          const signalName = String(
            ctx.__signalName ?? ctx.__signalEmission?.fullSignalName ?? "",
          ).trim();
          const isFetchHandshakeFailure =
            signalName === "meta.fetch.handshake_failed" ||
            signalName.startsWith("meta.fetch.handshake_failed:");

          if (
            isFetchHandshakeFailure &&
            affectedTransport &&
            this.isCurrentRouteContext(ctx) &&
            this.scheduleDependeeClientRecovery(instance, affectedTransport, {
              ...ctx,
              __reason: "fetch_handshake_failed_retry",
            })
          ) {
            continue;
          }

          const shouldMarkInactive =
            this.getHeartbeatMisses(instance.uuid) >=
            this.getRuntimeStatusInactiveThreshold();

          if (shouldMarkInactive) {
            this.markInstanceInactive(emit, instance, {
              persist: false,
            });
            continue;
          }

          this.applyInstanceLifecycleState(instance, {
            isActive: false,
            isNonResponsive: true,
          });
          emit("global.meta.service_registry.service_not_responding", {
            data: {
              isActive: false,
              isNonResponsive: true,
            },
            filter: {
              uuid: instance.uuid,
            },
          });
          this.emitInstanceLifecyclePersistence(
            emit,
            instance,
            {
              is_active: false,
              is_non_responsive: true,
              deleted: false,
              last_active: instance.reportedAt,
              health: instance.health ?? {},
            },
            { persist: false },
          );
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
        this.noteAuthorityBootstrapHandshake(ctx);
        const serviceInstances = this.instances.get(serviceName);
        let instance = serviceInstances?.find(
          (i) => i.uuid === serviceInstanceId,
        );

        if (!instance && serviceName && serviceInstanceId) {
          const bootstrapPlaceholder = serviceInstances?.find(
            (candidate) =>
              candidate.isBootstrapPlaceholder &&
              (!ctx.serviceTransportId ||
                candidate.transports.some(
                  (transport) => transport.uuid === ctx.serviceTransportId,
                )),
          );

          if (bootstrapPlaceholder) {
            instance = this.adoptBootstrapPlaceholderInstanceId(
              serviceName,
              bootstrapPlaceholder.uuid,
              serviceInstanceId,
            );
          }
        }

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

        const handshakeTransport = ctx.serviceTransportId
          ? this.getTransportById(instance, ctx.serviceTransportId)
          : this.selectTransportForInstance(
              instance,
              ctx,
              this.getInstanceRouteRole(instance),
            );
        if (handshakeTransport && !handshakeTransport.deleted) {
          this.markTransportClientReady(
            instance,
            handshakeTransport,
            this.resolveProtocolFromContext(ctx, handshakeTransport) ?? undefined,
          );
          this.retireSupersededInstancesForTransport(
            instance,
            handshakeTransport,
            emit,
          );
        }

        emit("global.meta.service_registry.service_handshake", {
          data: {
            isActive: instance.isActive,
            isNonResponsive: instance.isNonResponsive,
          },
          filter: {
            uuid: instance.uuid,
          },
        });
        emit("global.meta.service_instance.updated", {
          data: {
            is_active: instance.isActive,
            is_non_responsive: instance.isNonResponsive,
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
      (ctx, emit) => {
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
                role: report.transportRole ?? this.getRoutingTransportRole(),
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

        this.markTransportReadyFromContext(ctx);

        const updatedInstance = this.getInstance(
          report.serviceName,
          report.serviceInstanceId,
        );
        if (
          updatedInstance &&
          !updatedInstance.isFrontend &&
          (this.deputies.has(report.serviceName) ||
            this.remoteIntents.has(report.serviceName) ||
            this.remoteSignals.has(report.serviceName))
        ) {
          if (this.shouldEagerlyEstablishRemoteClients(ctx)) {
            this.ensureDependeeClientForInstance(updatedInstance, emit, ctx);
          }
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

    Cadenza.createMetaTask(
      "Request authority bootstrap handshake",
      (ctx) => this.requestAuthorityBootstrapHandshake(ctx),
      "Requests the first direct authority fetch handshake from the seeded bootstrap route before manifest-derived routing is available.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn("meta.initializing_service", META_AUTHORITY_BOOTSTRAP_HANDSHAKE_REQUESTED_SIGNAL);

    Cadenza.createMetaTask(
      "Invalidate bootstrap full sync after CadenzaDB connection loss",
      (ctx) => {
        if (resolveServiceNameFromContext(ctx) !== "CadenzaDB") {
          return false;
        }

        if (!this.isCurrentRouteContext(ctx)) {
          return false;
        }

        if (this.hasAuthorityBootstrapHandshakeEstablished()) {
          const currentAuthorityInstanceId = String(
            this.authorityBootstrapRoute.serviceInstanceId ?? "",
          ).trim();
          const currentAuthorityTransportId = String(
            this.authorityBootstrapRoute.serviceTransportId ?? "",
          ).trim();
          const incomingInstanceId = String(
            ctx.serviceInstanceId ?? ctx.__instance ?? "",
          ).trim();
          const incomingTransportId = String(
            ctx.serviceTransportId ?? ctx.__transportId ?? "",
          ).trim();
          const staleAuthorityInstance =
            incomingInstanceId &&
            currentAuthorityInstanceId &&
            incomingInstanceId !== currentAuthorityInstanceId;
          const staleAuthorityTransport =
            incomingTransportId &&
            currentAuthorityTransportId &&
            incomingTransportId !== currentAuthorityTransportId;

          if (staleAuthorityInstance || staleAuthorityTransport) {
            return false;
          }
        }

        this.invalidateAuthorityBootstrapHandshake();
        this.invalidateBootstrapFullSyncRetryState("cadenza_db_unreachable");
        Cadenza.emit(META_AUTHORITY_BOOTSTRAP_HANDSHAKE_REQUESTED_SIGNAL, {
          ...ctx,
          __reason: "cadenza_db_unreachable",
        });
        return true;
      },
      "Clears bootstrap full-sync retry satisfaction when the authority becomes unreachable.",
    ).doOn(
      "meta.fetch.handshake_failed",
      "meta.fetch.handshake_failed.*",
      "meta.socket_client.disconnected",
      "meta.socket_client.disconnected.*",
      "meta.service_registry.runtime_status_unreachable",
      "global.meta.service_registry.service_not_responding",
      "meta.service_registry.service_not_responding",
    );

    Cadenza.createMetaTask(
      "Request full sync after CadenzaDB fetch handshake",
      (ctx, emit) => {
        if (resolveServiceNameFromContext(ctx) !== "CadenzaDB") {
          return false;
        }

        this.ensureBootstrapAuthorityControlPlane(ctx, emit);
        return this.restartBootstrapFullSyncRetryChain(
          "cadenza_db_fetch_handshake",
        );
      },
      "Restarts the bootstrap full-sync recovery chain after the authority fetch transport comes up.",
    ).doOn("meta.fetch.handshake_complete");

    Cadenza.createMetaTask(
      "Request full sync after authority manifest update",
      (ctx, emit) => {
        if (
          !this.connectsToCadenzaDB ||
          !this.serviceName ||
          this.serviceName === "CadenzaDB"
        ) {
          return false;
        }

        this.ensureBootstrapAuthorityControlPlane(ctx, emit);
        Cadenza.debounce("meta.sync_requested", {
          __syncing: false,
          __reason: "cadenza_db_manifest_updated",
          __bootstrapFullSync: true,
          __bootstrapFullSyncAttempt: 1,
        }, 250);
        return true;
      },
      "Requests a fresh authority full sync when CadenzaDB ingests a new or updated service manifest.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn(AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL);

    Cadenza.createMetaTask(
      "Ensure authority bootstrap signal transmissions",
      () => this.ensureAuthorityBootstrapSignalTransmissions(),
      "Creates reserved authority bootstrap signal transmissions without waiting for manifest-derived signal routing maps.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn(
      "meta.service_registry.instance_inserted",
      "global.meta.service_instance.updated",
      "global.meta.sync_controller.synced",
    );

    this.fullSyncTask = Cadenza.createMetaTask(
      "Full sync",
      async (ctx) => {
        const fullSyncStartedAt = Date.now();
        if (
          this.connectsToCadenzaDB &&
          this.serviceName !== "CadenzaDB" &&
          !this.hasBootstrapFullSyncDeputies()
        ) {
          this.ensureBootstrapAuthorityControlPlane(ctx as AnyObject);
        }

        if (
          this.connectsToCadenzaDB &&
          this.serviceName !== "CadenzaDB" &&
          !this.hasAuthorityBootstrapHandshakeEstablished()
        ) {
          this.requestAuthorityBootstrapHandshake(
            {
              ...(ctx as AnyObject),
              __reason:
                typeof ctx?.__reason === "string" && ctx.__reason.trim().length > 0
                  ? ctx.__reason
                  : "bootstrap_full_sync_waiting_for_handshake",
            },
          );
          this.scheduleNextBootstrapFullSyncRetry(
            typeof ctx?.__reason === "string" ? ctx.__reason : undefined,
          );
          return false;
        }

        if (
          this.connectsToCadenzaDB &&
          this.serviceName !== "CadenzaDB" &&
          !this.hasBootstrapFullSyncDeputies()
        ) {
          if (shouldTraceServiceRegistry(this.serviceName)) {
            console.log("[CADENZA_SERVICE_REGISTRY_TRACE] full_sync_skipped_missing_bootstrap_deputies", {
              localServiceName: this.serviceName,
            });
          }
          return false;
        }

        let inquiryResult: AnyObject;
        const inquiryOptions = {
          timeout: BOOTSTRAP_FULL_SYNC_TIMEOUT_MS,
          ...(ctx.inquiryOptions ?? ctx.__inquiryOptions ?? {}),
        };

        try {
          if (shouldTraceServiceRegistry(this.serviceName)) {
            console.log("[CADENZA_SERVICE_REGISTRY_TRACE] full_sync_started", {
              localServiceName: this.serviceName,
              attempt: ctx.__bootstrapFullSyncAttempt ?? null,
              reason: typeof ctx.__reason === "string" ? ctx.__reason : null,
              timeoutMs: inquiryOptions.timeout,
              startedAt: fullSyncStartedAt,
            });
          }
          inquiryResult = await Cadenza.inquire(
            META_SERVICE_REGISTRY_FULL_SYNC_INTENT,
            {
              syncScope: "service-registry-full-sync",
              __syncing: true,
            },
            inquiryOptions,
          );
        } catch (error) {
          this.scheduleNextBootstrapFullSyncRetry(
            typeof ctx.__reason === "string" ? ctx.__reason : undefined,
          );
          throw error;
        }

        const signalToTaskMaps = this.readArrayPayload(
          inquiryResult as AnyObject,
          [
            "signalToTaskMaps",
            "signal_to_task_maps",
            "signalToTaskMap",
            "signal_to_task_map",
          ],
        );
        const globalSignalToTaskMaps = this.normalizeSignalMaps({
          signalToTaskMaps,
        }).filter((m) => !!m.isGlobal);

        const intentToTaskMaps = this.normalizeIntentMaps(
          inquiryResult as AnyObject,
        );

        const tasks = this.readArrayPayload(inquiryResult as AnyObject, [
          "tasks",
        ]);
        const signals = this.readArrayPayload(inquiryResult as AnyObject, [
          "signals",
        ]);
        const intents = this.readArrayPayload(inquiryResult as AnyObject, [
          "intents",
        ]);
        const actors = this.readArrayPayload(inquiryResult as AnyObject, [
          "actors",
        ]);
        const routines = this.readArrayPayload(inquiryResult as AnyObject, [
          "routines",
        ]);
        const directionalTaskMaps = this.readArrayPayload(
          inquiryResult as AnyObject,
          ["directionalTaskMaps", "directional_task_maps"],
        );
        const actorTaskMaps = this.readArrayPayload(inquiryResult as AnyObject, [
          "actorTaskMaps",
          "actor_task_maps",
        ]);
        const taskToRoutineMaps = this.readArrayPayload(
          inquiryResult as AnyObject,
          ["taskToRoutineMaps", "task_to_routine_maps"],
        );

        const serviceInstances = this.normalizeServiceInstancesFromSync(
          inquiryResult as AnyObject,
        );
        const serviceInstanceTransports = this.readArrayPayload(
          inquiryResult as AnyObject,
          [
            "serviceInstanceTransports",
            "service_instance_transports",
            "serviceInstanceTransport",
            "service_instance_transport",
          ],
        );
        const serviceManifests = this.readArrayPayload(
          inquiryResult as AnyObject,
          [
            "serviceManifests",
            "service_manifests",
            "serviceManifest",
            "service_manifest",
          ],
        );

        if (shouldTraceServiceRegistry(this.serviceName)) {
          console.log("[CADENZA_SERVICE_REGISTRY_TRACE] full_sync_result", {
            localServiceName: this.serviceName,
            attempt: ctx.__bootstrapFullSyncAttempt ?? null,
            durationMs: Date.now() - fullSyncStartedAt,
            inquiryMeta: inquiryResult.__inquiryMeta,
            signalToTaskMaps: globalSignalToTaskMaps.length,
            intentToTaskMaps: intentToTaskMaps.length,
            serviceInstances: serviceInstances.length,
            serviceInstanceTransports: serviceInstanceTransports.length,
            serviceManifests: serviceManifests.length,
            tasks: tasks.length,
            signals: signals.length,
            intents: intents.length,
            actors: actors.length,
            routines: routines.length,
            bootstrapSatisfied: this.bootstrapFullSyncSatisfied,
          });
        }

        if (
          this.hasHealthyBootstrapFullSyncResult({
            serviceInstances,
            intentToTaskMaps,
          })
        ) {
          this.markBootstrapFullSyncSatisfied();
        } else {
          this.scheduleNextBootstrapFullSyncRetry(
            typeof ctx.__reason === "string" ? ctx.__reason : undefined,
          );
        }

        return {
          ...ctx,
          signalToTaskMaps,
          intentToTaskMaps,
          serviceInstances,
          serviceInstanceTransports,
          serviceManifests,
          tasks,
          signals,
          intents,
          actors,
          routines,
          directionalTaskMaps,
          actorTaskMaps,
          taskToRoutineMaps,
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
      (ctx, emit) => {
        const { serviceName } = ctx;

        if (!serviceName || serviceName === this.serviceName) {
          return false;
        }

        if (!this.deputies.has(serviceName)) this.deputies.set(serviceName, []);

        this.deputies.get(serviceName)!.push({
          serviceName,
          remoteRoutineName: ctx.remoteRoutineName,
          signalName: ctx.signalName,
          localTaskName: ctx.localTaskName,
          communicationType: ctx.communicationType,
        });

        if (this.shouldEagerlyEstablishRemoteClients(ctx)) {
          this.ensureDependeeClientsForService(serviceName, emit, ctx);
        }
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
        const explicitDeputyExecId =
          typeof context.__metadata?.__deputyExecId === "string" &&
          context.__metadata.__deputyExecId.length > 0
            ? context.__metadata.__deputyExecId
            : typeof context.__deputyExecId === "string" &&
                context.__deputyExecId.length > 0
              ? context.__deputyExecId
              : undefined;
        const explicitDelegationOverrides: AnyObject = {};
        for (const key of [
          "__remoteRoutineName",
          "__serviceName",
          "__localTaskName",
          "__resolverRequestId",
          "__resolverOriginalContext",
          "__resolverQueryData",
        ] as const) {
          if (context[key] !== undefined) {
            explicitDelegationOverrides[key] = context[key];
          }
        }
        const traceServiceInstanceInsert =
          context.__remoteRoutineName === "Insert service_instance" ||
          context.__metadata?.__remoteRoutineName === "Insert service_instance" ||
          context.__localTaskName === "Insert service_instance in CadenzaDB" ||
          context.__metadata?.__deputyTaskName ===
            "Insert service_instance in CadenzaDB";
        if (context.__remoteRoutineName !== undefined) {
          context = ensureDelegationContextMetadata(
            restoreDelegationRequestSnapshot(
              attachDelegationRequestSnapshot(context),
            ),
          );

          if (explicitDeputyExecId || Object.keys(explicitDelegationOverrides).length > 0) {
            context = {
              ...context,
              ...(explicitDeputyExecId
                ? {
                    __deputyExecId: explicitDeputyExecId,
                    __metadata: {
                      ...(context.__metadata ?? {}),
                      __deputyExecId: explicitDeputyExecId,
                    },
                  }
                : {}),
              ...explicitDelegationOverrides,
            };
          }
        }

        const {
          __serviceName,
          __triedInstances,
          __retries,
          __broadcast,
          targetServiceInstanceId,
        } = context;
        const preferredRole = this.getRoutingTransportRole();
        const preferredProtocol = this.getPreferredRoutingProtocol(context);
        this.maybeDemoteFailedTransport(
          context,
          emit,
          preferredRole,
          preferredProtocol,
        );
        const activeRoutingCooldown =
          __serviceName && !targetServiceInstanceId
            ? this.getActiveRoutingCooldown(
                __serviceName,
                preferredRole,
                preferredProtocol,
              )
            : null;
        if (activeRoutingCooldown) {
          context.errored = true;
          context.__error =
            `No routeable ${preferredRole} transport available for ${__serviceName}. ` +
            "Waiting for authority route updates before retrying.";
          emit(
            `meta.service_registry.load_balance_failed:${context.__metadata.__deputyExecId}`,
            context,
          );
          return context;
        }
        if (
          (process.env.CADENZA_INSTANCE_DEBUG === "1" ||
            process.env.CADENZA_INSTANCE_DEBUG === "true") &&
          traceServiceInstanceInsert
        ) {
          console.log("[CADENZA_INSTANCE_DEBUG] load_balance_entry", {
            localServiceName: this.serviceName,
            remoteRoutineName:
              context.__remoteRoutineName ??
              context.__metadata?.__remoteRoutineName ??
              null,
            localTaskName:
              context.__localTaskName ??
              context.__metadata?.__deputyTaskName ??
              null,
            targetServiceName: __serviceName ?? null,
            deputyExecId: context.__metadata?.__deputyExecId ?? null,
            preferredRole,
            useSocket: this.useSocket,
            knownInstances: (this.instances.get(__serviceName) ?? []).map(
              (instance) => ({
                uuid: instance.uuid,
                isActive: instance.isActive,
                isNonResponsive: instance.isNonResponsive,
                isBlocked: instance.isBlocked,
                isFrontend: instance.isFrontend,
                readyTransportIds: instance.clientReadyTransportIds ?? [],
                pendingTransportIds: instance.clientPendingTransportIds ?? [],
                transports: (instance.transports ?? []).map((transport) => ({
                  uuid: transport.uuid,
                  role: transport.role,
                  origin: transport.origin,
                  protocols: transport.protocols,
                })),
              }),
            ),
            targetServiceInstanceId: targetServiceInstanceId ?? null,
            fetchId: context.__fetchId ?? null,
            triedInstances: Array.isArray(__triedInstances)
              ? __triedInstances
              : [],
          });
        }
        let retries = __retries ?? 0;
        let triedInstances = __triedInstances ?? [];
        if (this.shouldDemandEstablishRemoteClients(context)) {
          this.ensureDependeeClientsForService(__serviceName, emit, context);
        }
        const filteredInstances =
          this.instances
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

            if (instance.isFrontend) {
              return true;
            }

            return Boolean(
              this.selectReadyTransportForInstance(
                instance,
                context,
                preferredRole,
              ),
            );
          }) ?? [];
        const instances = this.collapseInstancesByRouteOrigin(
          filteredInstances,
          context,
          preferredRole,
        );
        const broadcastFilter =
          __broadcast &&
          context.__broadcastFilter &&
          typeof context.__broadcastFilter === "object"
            ? (context.__broadcastFilter as AnyObject)
            : null;
        const routeCandidates: RouteSelectionCandidate[] = instances
          .map((instance): RouteSelectionCandidate | null => {
            const selectedTransport = instance.isFrontend
              ? undefined
              : this.selectReadyTransportForInstance(
                  instance,
                  context,
                  preferredRole,
                );

            if (!instance.isFrontend && !selectedTransport) {
              return null;
            }

            if (
              __broadcast &&
              selectedTransport &&
              !this.matchesSignalBroadcastFilter(
                instance,
                selectedTransport,
                broadcastFilter,
              )
            ) {
              return null;
            }

            const routeKey = selectedTransport
              ? this.buildTransportRouteKey(instance.serviceName, selectedTransport)
              : null;
            const route = routeKey
              ? this.remoteRoutesByKey.get(routeKey) ??
                this.upsertRemoteRouteRecord(
                  instance.serviceName,
                  instance.uuid,
                  selectedTransport!,
                )
              : null;
            const snapshot = route
              ? this.buildRouteBalancingSnapshot(route, instance)
              : null;

            return {
              instance,
              selectedTransport,
              routeKey,
              route,
              snapshot,
            };
          })
          .filter(
            (candidate): candidate is RouteSelectionCandidate => candidate !== null,
          )
          .sort((left, right) => {
            if (left.snapshot && right.snapshot) {
              const snapshotDelta = this.compareRouteBalancingSnapshots(
                left.snapshot,
                right.snapshot,
              );
              if (snapshotDelta !== 0) {
                return snapshotDelta;
              }
            }

            if (left.snapshot && !right.snapshot) {
              return -1;
            }
            if (!left.snapshot && right.snapshot) {
              return 1;
            }

            return left.instance.uuid.localeCompare(right.instance.uuid);
          });

        if (
          shouldTraceServiceRegistry(this.serviceName) &&
          __serviceName === "PredictorService"
        ) {
          const tracedCandidates = routeCandidates.map((candidate) => ({
            serviceInstanceId: candidate.instance.uuid,
            transportId: candidate.selectedTransport?.uuid ?? null,
            transportOrigin: candidate.selectedTransport?.origin ?? null,
            routeKey: candidate.routeKey ?? null,
            snapshot: candidate.snapshot
              ? {
                  ...candidate.snapshot,
                }
              : null,
          }));
          console.log("[CADENZA_SERVICE_REGISTRY_TRACE] route_candidates", {
            localServiceName: this.serviceName,
            targetServiceName: __serviceName,
            preferredRole,
            preferredProtocol,
            candidates: tracedCandidates,
            candidatesJson: JSON.stringify(tracedCandidates),
          });
        }

        if (
          !routeCandidates ||
          routeCandidates.length === 0 ||
          retries > this.retryCount
        ) {
          if (__serviceName && !targetServiceInstanceId) {
            const routingFailure = this.recordRoutingFailure(
              __serviceName,
              preferredRole,
              preferredProtocol,
              "no_routeable_instance",
            );
            if (routingFailure.cooldownUntil > Date.now()) {
              context.errored = true;
              context.__error =
                `No routeable ${preferredRole} transport available for ${__serviceName}. ` +
                "Waiting for authority route updates before retrying.";
            } else {
              context.errored = true;
              context.__error =
                this.isFrontend && preferredRole === "public"
                  ? `No public transport available for ${__serviceName}.`
                  : `No routeable ${preferredRole} transport available for ${__serviceName}. Retries: ${retries}.`;
            }
          } else {
            context.errored = true;
            context.__error =
              this.isFrontend && preferredRole === "public"
                ? `No public transport available for ${__serviceName}.`
                : `No routeable ${preferredRole} transport available for ${__serviceName}. Retries: ${retries}.`;
          }
          if (
            (process.env.CADENZA_INSTANCE_DEBUG === "1" ||
              process.env.CADENZA_INSTANCE_DEBUG === "true") &&
            traceServiceInstanceInsert
          ) {
            console.log("[CADENZA_INSTANCE_DEBUG] load_balance_empty", {
              remoteRoutineName:
                context.__remoteRoutineName ??
                context.__metadata?.__remoteRoutineName ??
                null,
              localTaskName:
                context.__localTaskName ??
                context.__metadata?.__deputyTaskName ??
                null,
              targetServiceName: __serviceName ?? null,
              retries,
              retryCount: this.retryCount,
              deputyExecId: context.__metadata?.__deputyExecId ?? null,
            });
          }
          emit(
            `meta.service_registry.load_balance_failed:${context.__metadata.__deputyExecId}`,
            context,
          );
          return context;
        }

        if (__serviceName) {
          this.refreshRoutingCooldownsForService(__serviceName);
        }

        if (__broadcast || routeCandidates[0].instance.isFrontend) {
          for (const candidate of routeCandidates) {
            const { instance, selectedTransport, routeKey } = candidate;
            if (instance.isFrontend) {
              const fetchId = `browser:${instance.uuid}`;
              if (
                (process.env.CADENZA_INSTANCE_DEBUG === "1" ||
                  process.env.CADENZA_INSTANCE_DEBUG === "true") &&
                context.__remoteRoutineName === "Insert service_instance"
              ) {
                console.log("[CADENZA_INSTANCE_DEBUG] selected_instance_transport", {
                  transport: "socket",
                  serviceName: context.__serviceName ?? null,
                  targetInstanceId: instance.uuid,
                  fetchId,
                  dataKeys:
                    context.data && typeof context.data === "object"
                      ? Object.keys(context.data)
                      : [],
                  queryDataKeys:
                    context.queryData && typeof context.queryData === "object"
                      ? Object.keys(context.queryData)
                      : [],
                  queryDataDataKeys:
                    context.queryData?.data &&
                    typeof context.queryData.data === "object"
                      ? Object.keys(context.queryData.data as AnyObject)
                      : [],
                });
              }
              emit(
                `meta.service_registry.selected_instance_for_socket:${fetchId}`,
                {
                  ...context,
                  __instance: instance.uuid,
                  __transportId: undefined,
                  __transportOrigin: undefined,
                  __transportProtocols: ["socket"],
                  __fetchId: fetchId,
                },
              );
              continue;
            }
            if (!selectedTransport || !routeKey) {
              continue;
            }
            if (
              (process.env.CADENZA_INSTANCE_DEBUG === "1" ||
                process.env.CADENZA_INSTANCE_DEBUG === "true") &&
              context.__remoteRoutineName === "Insert service_instance"
            ) {
              console.log("[CADENZA_INSTANCE_DEBUG] selected_instance_transport", {
                transport:
                  this.resolveTransportProtocolOrder(context)[0] === "socket" &&
                  transportSupportsProtocol(selectedTransport, "socket")
                    ? "socket"
                    : "fetch",
                serviceName: context.__serviceName ?? null,
                targetInstanceId: instance.uuid,
                transportId: selectedTransport.uuid,
                transportOrigin: selectedTransport.origin,
                dataKeys:
                  context.data && typeof context.data === "object"
                    ? Object.keys(context.data)
                    : [],
                queryDataKeys:
                  context.queryData && typeof context.queryData === "object"
                    ? Object.keys(context.queryData)
                    : [],
                queryDataDataKeys:
                  context.queryData?.data &&
                  typeof context.queryData.data === "object"
                    ? Object.keys(context.queryData.data as AnyObject)
                    : [],
              });
            }
            const preferredProtocol = this.resolveTransportProtocolOrder(context)[0];
            const selectedProtocol =
              preferredProtocol === "socket" &&
              transportSupportsProtocol(selectedTransport, "socket")
                ? "socket"
                : "rest";
            const handleKey = this.buildTransportProtocolHandleKey(routeKey, selectedProtocol);
            this.recordBalancedRouteSelection(routeKey, {
              ...context,
              __instance: instance.uuid,
              __transportId: selectedTransport.uuid,
              __transportOrigin: selectedTransport.origin,
              __transportProtocols: selectedTransport.protocols,
              __transportProtocol: selectedProtocol,
              __routeKey: routeKey,
              routeKey,
              __fetchId: handleKey,
              fetchId: handleKey,
            });
            emit(
              `${
                selectedProtocol === "socket"
                  ? "meta.service_registry.selected_instance_for_socket"
                  : "meta.service_registry.selected_instance_for_fetch"
              }:${handleKey}`,
              {
                ...context,
                __instance: instance.uuid,
                __transportId: selectedTransport.uuid,
                __transportOrigin: selectedTransport.origin,
                __transportProtocols: selectedTransport.protocols,
                __transportProtocol: selectedProtocol,
                __routeKey: routeKey,
                routeKey,
                __fetchId: handleKey,
                fetchId: handleKey,
              },
            );
          }

          return context;
        }

        let candidatesToTry = routeCandidates.filter(
          (candidate) => !__triedInstances?.includes(candidate.instance.uuid),
        );

        if (candidatesToTry.length === 0) {
          if (this.useSocket) {
            emit(
              `meta.service_registry.socket_failed:${context.__fetchId}`,
              context,
            );
          }
          retries++;
          candidatesToTry = routeCandidates;
          triedInstances = [];
        }

        const selectedCandidate = candidatesToTry[0];
        const selected = selectedCandidate.instance;

        if (selected.isFrontend) {
          clearTransientRoutingErrorState(context);
          context.__instance = selected.uuid;
          context.__transportId = undefined;
          context.__transportOrigin = undefined;
          context.__transportProtocols = ["socket"];
          context.__fetchId = `browser:${selected.uuid}`;
          context.__triedInstances = triedInstances;
          context.__triedInstances.push(selected.uuid);
          context.__retries = retries;

          emit(
            `meta.service_registry.selected_instance_for_socket:${context.__fetchId}`,
            context,
          );
          return context;
        }

        const selectedTransport = selectedCandidate.selectedTransport;

        if (!selectedTransport) {
          if (__serviceName && !targetServiceInstanceId) {
            const routingFailure = this.recordRoutingFailure(
              __serviceName,
              preferredRole,
              preferredProtocol,
              "selected_instance_missing_transport",
            );
            if (routingFailure.cooldownUntil > Date.now()) {
              context.errored = true;
              context.__error =
                `No routeable ${preferredRole} transport available for ${selected.serviceName}. ` +
                "Waiting for authority route updates before retrying.";
            } else {
              context.errored = true;
              context.__error = `No routeable ${preferredRole} transport available for ${selected.serviceName}/${selected.uuid}.`;
            }
          } else {
            context.errored = true;
            context.__error = `No routeable ${preferredRole} transport available for ${selected.serviceName}/${selected.uuid}.`;
          }
          emit(
            `meta.service_registry.load_balance_failed:${context.__metadata.__deputyExecId}`,
            context,
          );
          return context;
        }

        if (__serviceName) {
          this.refreshRoutingCooldownsForService(__serviceName);
        }
        clearTransientRoutingErrorState(context);
        context.__instance = selected.uuid;
        context.__transportId = selectedTransport.uuid;
        context.__transportOrigin = selectedTransport.origin;
        context.__transportProtocols = selectedTransport.protocols;
        const routeKey =
          selectedCandidate.routeKey ??
          this.buildTransportRouteKey(selected.serviceName, selectedTransport);
        const selectedProtocol =
          this.resolveTransportProtocolOrder(context)[0] === "socket" &&
          transportSupportsProtocol(selectedTransport, "socket")
            ? "socket"
            : "rest";
        context.__routeKey = routeKey;
        context.routeKey = routeKey;
        context.__triedInstances = triedInstances;
        context.__triedInstances.push(selected.uuid);
        context.__retries = retries;
        context.__transportProtocol = selectedProtocol;
        context.__fetchId = this.buildTransportProtocolHandleKey(
          routeKey,
          selectedProtocol,
        );
        context.fetchId = context.__fetchId;
        this.recordBalancedRouteSelection(routeKey, context);

        if (shouldTraceServiceRegistry(this.serviceName)) {
          console.log("[CADENZA_SERVICE_REGISTRY_TRACE] selected_route", {
            localServiceName: this.serviceName,
            targetServiceName: selected.serviceName,
            targetServiceInstanceId: selected.uuid,
            targetTransportId: selectedTransport.uuid,
            targetTransportOrigin: selectedTransport.origin,
            routeKey,
            selectedProtocol,
            retries,
            triedInstances,
            deputyExecId: context.__metadata?.__deputyExecId ?? null,
            remoteRoutineName:
              context.__remoteRoutineName ??
              context.__metadata?.__remoteRoutineName ??
              null,
            localTaskName:
              context.__localTaskName ??
              context.__metadata?.__deputyTaskName ??
              null,
          });
        }

        if (selectedProtocol === "socket") {
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
      "Handle local inbound activity",
      (ctx, emit) => {
        if (
          ctx.serviceName &&
          ctx.serviceName !== this.serviceName
        ) {
          return false;
        }

        if (
          ctx.serviceInstanceId &&
          ctx.serviceInstanceId !== this.serviceInstanceId
        ) {
          return false;
        }

        return this.handleLocalInboundActivity(
          emit,
          typeof ctx.activityAt === "string" ? ctx.activityAt : undefined,
        );
      },
      "Refreshes the local instance as active and responsive after inbound activity without persisting on every call.",
    ).doOn(META_SERVICE_INSTANCE_ACTIVITY_OBSERVED_SIGNAL);

    Cadenza.createMetaTask(
      "Handle remote service activity observed",
      (ctx) => this.handleRemoteActivityObserved(ctx),
      "Refreshes tracked remote instance liveness from successful cross-service traffic without persisting authority state.",
    ).doOn(META_REMOTE_SERVICE_ACTIVITY_OBSERVED_SIGNAL);

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
        if (!this.runtimeMetricsSamplingStarted) {
          this.runtimeMetricsSamplingStarted = true;
          Cadenza.interval(
            META_RUNTIME_METRICS_SAMPLE_TICK_SIGNAL,
            {},
            this.runtimeMetricsSampleIntervalMs,
            true,
          );
        }
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
        Cadenza.interval(
          META_RUNTIME_STATUS_REST_REFRESH_TICK_SIGNAL,
          {},
          this.runtimeStatusRestRefreshIntervalMs,
          true,
        );
        return true;
      },
      "Starts runtime status heartbeat and heartbeat-monitor loops once per service instance.",
    ).doOn("meta.service_registry.instance_inserted");

    Cadenza.createMetaTask(
      "Sample runtime metrics",
      (_ctx, emit) => {
        if (!this.runtimeMetricsSampler) {
          return false;
        }

        const snapshot = this.captureRuntimeMetricsSample();
        if (!snapshot) {
          return false;
        }

        emit(META_RUNTIME_STATUS_PEER_UPDATE_REQUESTED_SIGNAL, {
          reason: "runtime_metrics_sample",
          detailLevel: "minimal",
        });
        return snapshot;
      },
      "Samples local Node runtime health metrics for later runtime-status publication and balancing.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn(META_RUNTIME_METRICS_SAMPLE_TICK_SIGNAL);

    Cadenza.createMetaTask(
      "Handle graceful service shutdown report",
      (ctx, emit) => {
        const localInstance = this.getLocalInstance();
        const reportedServiceInstanceId = String(
          ctx.serviceInstanceId ?? "",
        ).trim();
        if (
          !localInstance ||
          !reportedServiceInstanceId ||
          reportedServiceInstanceId !== localInstance.uuid
        ) {
          return false;
        }

        this.markInstanceInactive(emit, localInstance);
        Cadenza.schedule(
          META_SERVICE_INSTANCE_SHUTDOWN_TRANSPORT_DEACTIVATION_SIGNAL,
          this.buildGracefulShutdownTransportDeactivationContext(localInstance),
          750,
        );

        return {
          ...ctx,
          serviceName: localInstance.serviceName,
          serviceInstanceId: localInstance.uuid,
        };
      },
      "Marks the local instance inactive on graceful shutdown and disables routable transports immediately.",
    ).doOn(META_SERVICE_INSTANCE_SHUTDOWN_REPORTED_SIGNAL);

    Cadenza.createMetaTask(
      "Deactivate graceful shutdown transports",
      (ctx, emit) => {
        const localInstance = this.getLocalInstance();
        if (!localInstance || localInstance.uuid !== ctx.serviceInstanceId) {
          return false;
        }

        this.deactivateInstanceTransports(emit, localInstance);
        return true;
      },
      "Disables routable transports shortly after graceful shutdown state is persisted.",
    ).doOn(META_SERVICE_INSTANCE_SHUTDOWN_TRANSPORT_DEACTIVATION_SIGNAL);

    Cadenza.createMetaTask(
      "Broadcast runtime status",
      (ctx, emit) => {
        const authorityReport = this.buildLocalRuntimeStatusReport("full");
        if (!authorityReport) {
          return false;
        }
        const peerReport =
          ctx.detailLevel === "full"
            ? authorityReport
            : this.stripRuntimeStatusReportForPeer(authorityReport);

        const snapshot = this.resolveRuntimeStatusSnapshot(
          authorityReport.numberOfRunningGraphs,
          authorityReport.isActive,
          authorityReport.isNonResponsive,
          authorityReport.isBlocked,
        );
        const peerSnapshot = this.buildPeerRuntimeStatusSnapshot(peerReport);
        const force =
          ctx.reason === "heartbeat" ||
          ctx.force === true ||
          this.lastRuntimeStatusSnapshot === null;
        const peerOnly =
          ctx.peerOnly === true || ctx.reason === "runtime_metrics_sample";
        const shouldEmitPeer =
          ctx.force === true ||
          this.lastPeerRuntimeStatusSnapshot === null ||
          this.hasSignificantPeerRuntimeStatusChange(
            this.lastPeerRuntimeStatusSnapshot,
            peerSnapshot,
          );
        const shouldEmitAuthority = !peerOnly && force;

        if (!shouldEmitPeer && !shouldEmitAuthority) {
          return false;
        }

        if (shouldEmitPeer) {
          this.lastPeerRuntimeStatusSnapshot = peerSnapshot;
          emit("meta.service.updated", {
            __serviceName: peerReport.serviceName,
            __serviceInstanceId: peerReport.serviceInstanceId,
            __reportedAt: peerReport.reportedAt,
            __numberOfRunningGraphs: peerReport.numberOfRunningGraphs,
            __health: peerReport.health ?? {},
            __active: peerReport.isActive,
            serviceName: peerReport.serviceName,
            serviceInstanceId: peerReport.serviceInstanceId,
            transportId: peerReport.transportId,
            transportRole: peerReport.transportRole,
            transportOrigin: peerReport.transportOrigin,
            transportProtocols: peerReport.transportProtocols,
            isFrontend: peerReport.isFrontend,
            reportedAt: peerReport.reportedAt,
            numberOfRunningGraphs: peerReport.numberOfRunningGraphs,
            cpuUsage: peerReport.cpuUsage ?? null,
            memoryUsage: peerReport.memoryUsage ?? null,
            eventLoopLag: peerReport.eventLoopLag ?? null,
            health: peerReport.health,
            isActive: peerReport.isActive,
            isNonResponsive: peerReport.isNonResponsive,
            isBlocked: peerReport.isBlocked,
            state: peerReport.state,
            acceptingWork: peerReport.acceptingWork,
          });
        }

        if (shouldEmitAuthority) {
          this.lastRuntimeStatusSnapshot = snapshot;
          emit(RUNTIME_STATUS_AUTHORITY_SYNC_REQUESTED_SIGNAL, {
            ...authorityReport,
            force,
          });
        }
        return true;
      },
      "Broadcasts local runtime status to connected dependees.",
    ).doOn(
      META_RUNTIME_STATUS_HEARTBEAT_TICK_SIGNAL,
      META_RUNTIME_STATUS_PEER_UPDATE_REQUESTED_SIGNAL,
      "meta.service_registry.runtime_status_broadcast_requested",
    );

    Cadenza.createMetaTask(
      "Flush local runtime status to authority",
      this.localLifecycleFlushActor.task(
        async ({ input, runtimeState, setRuntimeState, inquire }) => {
          if (
            !this.connectsToCadenzaDB ||
            !this.serviceName ||
            !this.serviceInstanceId ||
            this.serviceName === "CadenzaDB"
          ) {
            return false;
          }

          const report = normalizeAuthorityRuntimeStatusReport(
            input as Record<string, any>,
          );
          if (
            !report ||
            report.serviceName !== this.serviceName ||
            report.serviceInstanceId !== this.serviceInstanceId
          ) {
            return false;
          }

          const force = input.force === true;
          const signature = buildAuthorityRuntimeStatusSignature(report);
          const currentState: LocalLifecycleFlushRuntimeState =
            runtimeState ?? {
              lastSentSignature: null,
              lastAckAt: null,
              lastError: null,
            };

          if (!force && currentState.lastSentSignature === signature) {
            return false;
          }

          try {
            this.ensureBootstrapAuthorityControlPlane(input as AnyObject);
            await inquire(AUTHORITY_RUNTIME_STATUS_REPORT_INTENT, report, {
              timeout: this.runtimeStatusAuthorityReportTimeoutMs,
              requireComplete: true,
            });
            setRuntimeState({
              lastSentSignature: signature,
              lastAckAt: new Date().toISOString(),
              lastError: null,
            });
            return {
              applied: true,
              serviceName: report.serviceName,
              serviceInstanceId: report.serviceInstanceId,
            };
          } catch (error) {
            setRuntimeState({
              ...currentState,
              lastError:
                error instanceof Error ? error.message : String(error ?? "unknown"),
            });
            throw error;
          }
        },
        { mode: "write" },
      ),
      "Coalesces local runtime-status flushes and sends one lightweight authority report per service instance at a time.",
    ).doOn(RUNTIME_STATUS_AUTHORITY_SYNC_REQUESTED_SIGNAL);

    Cadenza.createMetaTask(
      "Refresh REST dependee runtime status",
      async () => {
        const now = Date.now();

        for (const [serviceName, instanceIds] of this.dependeesByService) {
          for (const serviceInstanceId of instanceIds) {
            if (
              this.runtimeStatusFallbackInFlightByInstance.has(serviceInstanceId) ||
              this.runtimeStatusRestRefreshInFlightByInstance.has(serviceInstanceId)
            ) {
              continue;
            }

            const instance = this.getInstance(serviceName, serviceInstanceId);
            if (!instance || !this.shouldRefreshRuntimeStatusViaRest(instance, now)) {
              continue;
            }

            this.runtimeStatusRestRefreshInFlightByInstance.add(serviceInstanceId);

            try {
              const directStatusCheck = await this.requestRuntimeStatusViaRest(
                instance,
                serviceName,
                serviceInstanceId,
              );

              if (directStatusCheck.report) {
                this.applyRuntimeStatusReport(directStatusCheck.report);
                this.lastHeartbeatAtByInstance.set(serviceInstanceId, Date.now());
                this.missedHeartbeatsByInstance.set(serviceInstanceId, 0);
                continue;
              }

              if (
                directStatusCheck.replacementReport &&
                this.applyRuntimeStatusIdentityReplacement(
                  instance,
                  directStatusCheck.replacementReport,
                  directStatusCheck.diagnostic,
                )
              ) {
                continue;
              }
            } finally {
              this.runtimeStatusRestRefreshInFlightByInstance.delete(
                serviceInstanceId,
              );
            }
          }
        }

        return true;
      },
      "Refreshes stale runtime metrics for REST-only dependees so balancing can react without socket status streams.",
    ).doOn(META_RUNTIME_STATUS_REST_REFRESH_TICK_SIGNAL);

    Cadenza.createMetaTask(
      "Monitor dependee heartbeat freshness",
      (ctx, emit) => {
        const now = Date.now();
        for (const [serviceName, instanceIds] of this.dependeesByService) {
          for (const serviceInstanceId of instanceIds) {
            const instance = this.getInstance(serviceName, serviceInstanceId);
            if (!instance || !instance.isActive || instance.isBlocked) {
              continue;
            }

            if (this.hasReadyClientTransportForProtocol(instance, "socket")) {
              this.lastHeartbeatAtByInstance.set(serviceInstanceId, now);
              this.missedHeartbeatsByInstance.set(serviceInstanceId, 0);
              this.runtimeStatusFallbackInFlightByInstance.delete(serviceInstanceId);
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

    this.insertServiceTask = resolveServiceRegistryInsertTask(
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

    const insertServiceInstanceResolverTask = resolveServiceRegistryInsertTask(
      "service_instance",
      {
        onConflict: {
          target: ["uuid"],
          action: {
            do: "update",
            set: {
              process_pid: "excluded",
              is_primary: "excluded",
              service_name: "excluded",
              is_database: "excluded",
              is_frontend: "excluded",
              is_blocked: "excluded",
              is_non_responsive: "excluded",
              is_active: "excluded",
              last_active: "excluded",
              health: "excluded",
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
    ).emitsOnFail("meta.service_registry.instance_insertion_failed");

    this.insertServiceInstanceTask = insertServiceInstanceResolverTask;

    const setupServiceTask = Cadenza.createMetaTask(
      "Setup service",
      (ctx) => {
        const traceServiceName =
          resolveServiceNameFromContext(ctx) || Cadenza.serviceRegistry.serviceName;
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
          transports: ctx.__transportData ?? ctx.transportData ?? [],
        });

        if (
          !normalizedLocalInstance?.uuid ||
          !normalizedLocalInstance.serviceName
        ) {
          if (shouldTraceServiceRegistry(traceServiceName)) {
            console.log("[CADENZA_SERVICE_REGISTRY_TRACE] setup_service_rejected", {
              localServiceName: this.serviceName,
              serviceName: traceServiceName || null,
              serviceInstanceId:
                ctx.__serviceInstanceId ??
                ctx.data?.uuid ??
                ctx.queryData?.data?.uuid ??
                null,
              hasTransportData:
                Array.isArray(ctx.__transportData) || Array.isArray(ctx.transportData),
              transportCount: Array.isArray(ctx.__transportData)
                ? ctx.__transportData.length
                : Array.isArray(ctx.transportData)
                  ? ctx.transportData.length
                  : 0,
              dataKeys:
                data && typeof data === "object" ? Object.keys(data) : [],
              queryDataDataKeys:
                queryData?.data && typeof queryData.data === "object"
                  ? Object.keys(queryData.data)
                  : [],
            });
          }
          if (
            process.env.CADENZA_INSTANCE_DEBUG === "1" ||
            process.env.CADENZA_INSTANCE_DEBUG === "true"
          ) {
            console.log("[CADENZA_INSTANCE_DEBUG] setup_service_rejected_instance", {
              hasServiceInstance: !!serviceInstance,
              hasData: !!data,
              hasQueryDataData: !!queryData?.data,
              serviceInstanceKeys:
                serviceInstance && typeof serviceInstance === "object"
                  ? Object.keys(serviceInstance)
                  : [],
              dataKeys:
                data && typeof data === "object" ? Object.keys(data) : [],
              queryDataDataKeys:
                queryData?.data && typeof queryData.data === "object"
                  ? Object.keys(queryData.data)
                  : [],
              normalizedLocalInstance,
              transportCount: Array.isArray(ctx.__transportData)
                ? ctx.__transportData.length
                : Array.isArray(ctx.transportData)
                  ? ctx.transportData.length
                  : 0,
              errored: ctx.errored === true,
              error: ctx.__error ?? null,
              inquiryMeta: ctx.__inquiryMeta ?? null,
            });
          }
          return false;
        }

        if (shouldTraceServiceRegistry(normalizedLocalInstance.serviceName)) {
          console.log("[CADENZA_SERVICE_REGISTRY_TRACE] setup_service", {
            localServiceName: this.serviceName,
            serviceName: normalizedLocalInstance.serviceName,
            serviceInstanceId: normalizedLocalInstance.uuid,
            transportCount: Array.isArray(ctx.__transportData)
              ? ctx.__transportData.length
              : Array.isArray(ctx.transportData)
                ? ctx.transportData.length
                : 0,
            hasTransportData:
              Array.isArray(ctx.__transportData) || Array.isArray(ctx.transportData),
          });
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
        return {
          ...ctx,
          serviceInstance: normalizedLocalInstance,
          data: {
            ...(ctx.data ?? {}),
            uuid: normalizedLocalInstance.uuid,
            service_name: normalizedLocalInstance.serviceName,
          },
          __serviceName: normalizedLocalInstance.serviceName,
          __serviceInstanceId: normalizedLocalInstance.uuid,
        };
      },
      "Sets service instance id after insertion",
    )
      .doOn(META_SERVICE_INSTANCE_INSERT_RESOLVED_SIGNAL)
      .emits("meta.service_registry.instance_inserted");

    setupServiceTask.then(
      Cadenza.createMetaTask(
        "Prepare service transport inserts",
        function* (ctx: AnyObject, emit) {
          const traceServiceName =
            String(
              ctx.__serviceName ??
                ctx.serviceName ??
                Cadenza.serviceRegistry.serviceName ??
                "",
            ).trim();
          const transportData = Array.isArray(ctx.__transportData)
            ? ctx.__transportData
            : Array.isArray(ctx.transportData)
              ? ctx.transportData
              : [];

          if (shouldTraceServiceRegistry(traceServiceName)) {
            console.log("[CADENZA_SERVICE_REGISTRY_TRACE] prepare_transport_inserts", {
              localServiceName: Cadenza.serviceRegistry.serviceName,
              serviceName: traceServiceName || null,
              serviceInstanceId: ctx.__serviceInstanceId ?? null,
              transportCount: transportData.length,
              transports: transportData,
            });
          }

          for (const transport of transportData) {
            const transportContext = {
              ...ctx,
              data: {
                ...transport,
                service_instance_id:
                  transport.service_instance_id ?? ctx.__serviceInstanceId,
              },
              __registrationData: {
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
    );

    Cadenza.createMetaTask(
      "Ensure declared service transports are registered",
      function* (ctx: AnyObject, emit) {
        const traceServiceName =
          String(
            ctx.__serviceName ??
              ctx.serviceName ??
              Cadenza.serviceRegistry.serviceName ??
              "",
          ).trim();
        const transportData = Array.isArray(ctx.__transportData)
          ? ctx.__transportData
          : Array.isArray(ctx.transportData)
            ? ctx.transportData
            : [];

        if (transportData.length === 0) {
          return false;
        }

        if (shouldTraceServiceRegistry(traceServiceName)) {
          console.log("[CADENZA_SERVICE_REGISTRY_TRACE] ensure_transport_registration", {
            localServiceName: Cadenza.serviceRegistry.serviceName,
            serviceName: traceServiceName || null,
            serviceInstanceId: ctx.__serviceInstanceId ?? null,
            transportCount: transportData.length,
            transports: transportData,
          });
        }

        for (const transport of transportData) {
          const transportContext = {
            ...ctx,
            data: {
              ...transport,
              service_instance_id:
                transport.service_instance_id ?? ctx.__serviceInstanceId,
            },
            __registrationData: {
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
      "Idempotently replays declared startup transports so bootstrap does not depend on one instance-insert resolution event.",
    ).doOn("meta.service_registry.transport_registration_ensure_requested");

    Cadenza.createMetaTask(
      "Retry local service instance registration after failed insert",
      (ctx) => {
        const registrationPayload = resolveServiceInstanceRegistrationPayload(
          ctx,
          this.serviceName,
          this.serviceInstanceId,
        );

        if (!registrationPayload) {
          return false;
        }

        const serviceName = String(
          registrationPayload.service_name ?? this.serviceName ?? "",
        ).trim();

        if (!serviceName || serviceName !== this.serviceName) {
          return false;
        }

        Cadenza.schedule(
          "meta.service_registry.instance_registration_requested",
          {
            ...ctx,
            data: registrationPayload,
            __registrationData: registrationPayload,
            __serviceName: serviceName,
            __serviceInstanceId: registrationPayload.uuid,
          },
          5000,
        );

        return true;
      },
      "Retries local service instance registration only after the previous insert attempt has failed.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn("meta.service_registry.instance_insertion_failed");

    Cadenza.createMetaTask(
      "Prepare service instance registration",
      (ctx) => {
        const registrationPayload = resolveServiceInstanceRegistrationPayload(
          ctx,
          this.serviceName,
          this.serviceInstanceId,
        );
        const sanitizedContext = sanitizeServiceRegistryInsertExecutionContext(
          ctx,
        );

        if (
          process.env.CADENZA_INSTANCE_DEBUG === "1" ||
          process.env.CADENZA_INSTANCE_DEBUG === "true"
        ) {
          console.log("[CADENZA_INSTANCE_DEBUG] prepare_service_instance_registration", {
            serviceName:
              registrationPayload?.service_name ??
              sanitizedContext.data?.service_name ??
              sanitizedContext.__serviceName ??
              this.serviceName ??
              null,
            serviceInstanceId:
              registrationPayload?.uuid ??
              sanitizedContext.data?.uuid ??
              sanitizedContext.__serviceInstanceId ??
              this.serviceInstanceId ??
              null,
            hasData: !!registrationPayload,
            dataKeys:
              registrationPayload && typeof registrationPayload === "object"
                ? Object.keys(registrationPayload)
                : [],
            transportCount: Array.isArray(sanitizedContext.__transportData)
              ? sanitizedContext.__transportData.length
              : Array.isArray(sanitizedContext.transportData)
                ? sanitizedContext.transportData.length
                : 0,
            skipRemoteExecution: sanitizedContext.__skipRemoteExecution === true,
          });
        }

        if (!registrationPayload) {
          return false;
        }

        const serviceName = String(
          registrationPayload.service_name ??
            sanitizedContext.__serviceName ??
            this.serviceName ??
            "",
        ).trim();

        if (shouldTraceServiceRegistry(serviceName || this.serviceName)) {
          console.log("[CADENZA_SERVICE_REGISTRY_TRACE] prepare_instance_registration", {
            localServiceName: this.serviceName,
            serviceName: serviceName || null,
            serviceInstanceId: registrationPayload.uuid ?? null,
            transportCount: Array.isArray(sanitizedContext.__transportData)
              ? sanitizedContext.__transportData.length
              : Array.isArray(sanitizedContext.transportData)
                ? sanitizedContext.transportData.length
                : 0,
            hasTransportData:
              Array.isArray(sanitizedContext.__transportData) ||
              Array.isArray(sanitizedContext.transportData),
          });
        }

        if (
          serviceName === "CadenzaDB" &&
          !Cadenza.getLocalCadenzaDBInsertTask("service_instance")
        ) {
          Cadenza.schedule(
            "meta.service_registry.instance_registration_requested",
            { ...sanitizedContext },
            250,
          );
          return false;
        }

        return {
          ...sanitizedContext,
          data: registrationPayload,
          __registrationData: {
            ...(sanitizedContext.__registrationData ?? {}),
            ...registrationPayload,
          },
          __serviceName: serviceName,
          __serviceInstanceId: registrationPayload.uuid,
        };
      },
      "Waits for the exact local CadenzaDB service instance insert task during self-bootstrap.",
    )
      .doOn("meta.service_registry.instance_registration_requested")
      .then(this.insertServiceInstanceTask);

    this.insertServiceTransportTask = resolveServiceRegistryInsertTask(
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
      .emits("meta.service_registry.transport_registered")
      .emitsOnFail("meta.service_registry.transport_registration_failed");

    Cadenza.createMetaTask(
      "Prepare service transport registration",
      (ctx) => {
        const serviceName = String(
          ctx.__serviceName ?? this.serviceName ?? "",
        ).trim();

        if (shouldTraceServiceRegistry(serviceName)) {
          console.log("[CADENZA_SERVICE_REGISTRY_TRACE] prepare_transport_registration", {
            localServiceName: this.serviceName,
            serviceName,
            serviceInstanceId:
              ctx.__serviceInstanceId ?? ctx.data?.service_instance_id ?? null,
            data: ctx.data ?? null,
          });
        }

        if (
          serviceName === "CadenzaDB" &&
          !Cadenza.getLocalCadenzaDBInsertTask("service_instance_transport")
        ) {
          Cadenza.schedule(
            "meta.service_registry.transport_registration_requested",
            { ...ctx },
            250,
          );
          return false;
        }

        return ctx;
      },
      "Waits for the exact local CadenzaDB service transport insert task during self-bootstrap.",
    )
      .doOn("meta.service_registry.transport_registration_requested")
      .then(this.insertServiceTransportTask);

    Cadenza.createMetaTask(
      "Prepare service instance update persistence",
      (ctx) => {
        const instanceId = String(
          ctx.queryData?.filter?.uuid ??
            ctx.filter?.uuid ??
            ctx.__serviceInstanceId ??
            ctx.data?.uuid ??
            "",
        ).trim();
        if (!instanceId || !ctx.data || typeof ctx.data !== "object") {
          return false;
        }

        return {
          ...ctx,
          queryData: {
            ...(ctx.queryData ?? {}),
            filter: {
              ...(ctx.queryData?.filter ?? ctx.filter ?? {}),
              uuid: instanceId,
            },
          },
        };
      },
      "Persists authority-facing service instance state updates without re-running local setup.",
    )
      .doOn("meta.service_registry.instance_update_requested")
      .then(
        Cadenza.createCadenzaDBTask(
          "service_instance",
          "update",
          {},
          {
            isHidden: true,
          },
        ),
      );

    Cadenza.createMetaTask(
      "Prepare service transport update persistence",
      (ctx) => {
        const transportId = String(
          ctx.queryData?.filter?.uuid ?? ctx.filter?.uuid ?? ctx.data?.uuid ?? "",
        ).trim();
        if (
          !transportId ||
          !isPersistedUuid(transportId) ||
          !ctx.data ||
          typeof ctx.data !== "object"
        ) {
          return false;
        }

        return {
          ...ctx,
          queryData: {
            ...(ctx.queryData ?? {}),
            filter: {
              ...(ctx.queryData?.filter ?? ctx.filter ?? {}),
              uuid: transportId,
            },
          },
        };
      },
      "Persists authority-facing service transport state updates without re-running local registration.",
    )
      .doOn("meta.service_registry.transport_update_requested")
      .then(
        Cadenza.createCadenzaDBTask(
          "service_instance_transport",
          "update",
          {},
          {
            isHidden: true,
          },
        ),
      );

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
                        .filter((i) => i.isActive && !i.isFrontend);
                      for (const instance of instances) {
                        const transport = this.getRouteableTransport(
                          instance,
                          this.useSocket ? "socket" : "rest",
                        );
                        if (!transport) {
                          continue;
                        }

                        if (
                          !this.hasTransportClientCreated(
                            instance,
                            transport,
                            "rest",
                          )
                        ) {
                          const route = this.upsertRemoteRouteRecord(
                            instance.serviceName,
                            instance.uuid,
                            transport,
                          );
                          emit("meta.service_registry.dependee_registered", {
                            serviceName: service,
                            serviceInstanceId: instance.uuid,
                            serviceTransportId: transport.uuid,
                            serviceOrigin: transport.origin,
                            routeKey: route.key,
                            fetchId: this.buildTransportProtocolHandleKey(
                              route.key,
                              "rest",
                            ),
                            socketClientId: this.buildTransportProtocolHandleKey(
                              route.key,
                              "socket",
                            ),
                            routeGeneration: route.generation,
                            transportProtocols: transport.protocols,
                            communicationTypes: ["signal"],
                          });
                          this.markTransportClientPending(instance, transport, "rest");
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
    this.gatheredSyncTransmissionServices.clear();
    this.remoteIntentDeputiesByKey.clear();
    this.remoteIntentDeputiesByTask.clear();
    this.dependeesByService.clear();
    this.dependeeByInstance.clear();
    this.readinessDependeesByService.clear();
    this.readinessDependeeByInstance.clear();
    this.lastHeartbeatAtByInstance.clear();
    this.missedHeartbeatsByInstance.clear();
    this.runtimeStatusFallbackInFlightByInstance.clear();
    this.runtimeStatusRestRefreshInFlightByInstance.clear();
    this.routingCooldownsByKey.clear();
    this.transportFailuresByKey.clear();
    this.activeRoutineExecutionIds.clear();
    this.numberOfRunningGraphs = 0;
    this.runtimeStatusHeartbeatStarted = false;
    this.lastRuntimeStatusSnapshot = null;
    this.isFrontend = false;
    this.localInstanceSeed = null;
  }
}
