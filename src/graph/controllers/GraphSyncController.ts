import Cadenza from "../../Cadenza";
import {
  AnyObject,
  META_ACTOR_SESSION_STATE_PERSIST_INTENT,
  Task,
} from "@cadenza.io/core";
import ServiceRegistry from "../../registry/ServiceRegistry";
import { decomposeSignalName, formatTimestamp } from "../../utils/tools";
import { isMetaIntentName } from "../../utils/inquiry";
import { v4 as uuid } from "uuid";

type ActorTaskRuntimeMetadata = {
  actorName: string;
  actorDescription?: string;
  actorKind: "standard" | "meta";
  mode: "read" | "write" | "meta";
  forceMeta: boolean;
};

const ACTOR_TASK_METADATA = Symbol.for("@cadenza.io/core/actor-task-meta");

function getActorTaskRuntimeMetadata(
  taskFunction: unknown,
): ActorTaskRuntimeMetadata | undefined {
  if (typeof taskFunction !== "function") {
    return undefined;
  }

  return (taskFunction as { [ACTOR_TASK_METADATA]?: ActorTaskRuntimeMetadata })[
    ACTOR_TASK_METADATA
  ];
}

function sanitizeActorMetadataValue(value: unknown): unknown {
  if (value === null) {
    return null;
  }
  if (value === undefined || typeof value === "function") {
    return undefined;
  }
  if (Array.isArray(value)) {
    const items: unknown[] = [];
    for (const item of value) {
      const sanitizedItem = sanitizeActorMetadataValue(item);
      if (sanitizedItem !== undefined) {
        items.push(sanitizedItem);
      }
    }
    return items;
  }
  if (typeof value === "object") {
    const output: Record<string, unknown> = {};
    for (const [key, nestedValue] of Object.entries(value)) {
      const sanitizedNestedValue = sanitizeActorMetadataValue(nestedValue);
      if (sanitizedNestedValue !== undefined) {
        output[key] = sanitizedNestedValue;
      }
    }
    return output;
  }

  return value;
}

function buildActorRegistrationData(actor: any): Record<string, unknown> {
  const definition = sanitizeActorMetadataValue(
    typeof actor?.toDefinition === "function" ? actor.toDefinition() : {},
  ) as Record<string, unknown>;
  const stateDefinition =
    definition?.state && typeof definition.state === "object"
      ? definition.state
      : {};
  const actorKind =
    typeof definition?.kind === "string" ? definition.kind : actor?.kind;

  return {
    name: definition?.name ?? actor?.spec?.name ?? "",
    description: definition?.description ?? actor?.spec?.description ?? "",
    default_key:
      definition?.defaultKey ?? actor?.spec?.defaultKey ?? "default",
    load_policy: definition?.loadPolicy ?? actor?.spec?.loadPolicy ?? "eager",
    write_contract:
      definition?.writeContract ?? actor?.spec?.writeContract ?? "overwrite",
    runtime_read_guard:
      definition?.runtimeReadGuard ?? actor?.spec?.runtimeReadGuard ?? "none",
    consistency_profile:
      definition?.consistencyProfile ?? actor?.spec?.consistencyProfile ?? null,
    key_definition: definition?.key ?? null,
    state_definition: stateDefinition,
    retry_policy: definition?.retry ?? {},
    idempotency_policy: definition?.idempotency ?? {},
    session_policy: definition?.session ?? {},
    is_meta: actorKind === "meta",
    version: 1,
  };
}

function sanitizePersistedTaskSourceFields(
  task: Task,
  data: Record<string, unknown>,
): Record<string, unknown> {
  if (!task.isMeta && !task.isDeputy) {
    return data;
  }

  return {
    ...data,
    function_string: "",
    tag_id_getter: null,
  };
}

function resolveSyncServiceName(
  task?:
    | {
        serviceName?: string;
        ownerServiceName?: string | null;
        isDeputy?: boolean;
      }
    | null,
):
  | string
  | undefined {
  const ownerServiceName =
    typeof task?.ownerServiceName === "string"
      ? task.ownerServiceName.trim()
      : "";
  const taskServiceName =
    typeof task?.serviceName === "string" ? task.serviceName.trim() : "";
  const registryServiceName =
    typeof ServiceRegistry.instance.serviceName === "string"
      ? ServiceRegistry.instance.serviceName.trim()
      : "";

  if (task?.isDeputy) {
    return ownerServiceName || registryServiceName || taskServiceName || undefined;
  }

  return ownerServiceName || taskServiceName || registryServiceName || undefined;
}

function buildIntentRegistryData(intent: any): Record<string, unknown> | null {
  const name = String(intent?.name ?? "").trim();
  if (!name) {
    return null;
  }

  return {
    name,
    description:
      typeof intent?.description === "string" ? intent.description : "",
    input:
      intent?.input && typeof intent.input === "object"
        ? intent.input
        : { type: "object" },
    output:
      intent?.output && typeof intent.output === "object"
        ? intent.output
        : { type: "object" },
    is_meta: isMetaIntentName(name),
  };
}

function isLocalOnlySyncIntent(intentName: string): boolean {
  return intentName === META_ACTOR_SESSION_STATE_PERSIST_INTENT;
}

function getJoinedContextValue(
  ctx: Record<string, any>,
  key: "data" | "batch" | "queryData",
): unknown {
  const joinedContexts = Array.isArray(ctx.joinedContexts) ? ctx.joinedContexts : [];
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
}

function didSyncInsertSucceed(ctx: Record<string, any>): boolean {
  if (ctx.errored || ctx.__success === false) {
    return false;
  }

  const inquiryMeta =
    ctx.__inquiryMeta && typeof ctx.__inquiryMeta === "object"
      ? (ctx.__inquiryMeta as Record<string, unknown>)
      : null;

  if (!inquiryMeta) {
    return true;
  }

  const eligibleResponders = Number(inquiryMeta.eligibleResponders);
  if (Number.isFinite(eligibleResponders) && eligibleResponders === 0) {
    return false;
  }

  const responded = Number(inquiryMeta.responded);
  if (Number.isFinite(responded) && responded === 0) {
    return false;
  }

  return true;
}

function buildMinimalSyncSignalContext(
  ctx: Record<string, unknown>,
  extra: Record<string, unknown> = {},
): Record<string, unknown> {
  const nextContext: Record<string, unknown> = {
    __syncing: ctx.__syncing === true,
    ...extra,
  };

  if (typeof ctx.__reason === "string" && ctx.__reason.trim().length > 0) {
    nextContext.__reason = ctx.__reason;
  }

  return nextContext;
}

function buildSyncInsertQueryData(
  ctx: Record<string, any>,
  queryData: Record<string, unknown> = {},
): Record<string, unknown> {
  const pickQueryData = (
    source: Record<string, unknown>,
    allowedKeys: string[],
  ): Record<string, unknown> => {
    const next: Record<string, unknown> = {};

    for (const key of allowedKeys) {
      if (Object.prototype.hasOwnProperty.call(source, key)) {
        next[key] = source[key];
      }
    }

    return next;
  };
  const joinedQueryData = getJoinedContextValue(ctx, "queryData");
  const existingQueryData =
    ctx.queryData && typeof ctx.queryData === "object"
      ? ctx.queryData
      : joinedQueryData && typeof joinedQueryData === "object"
        ? joinedQueryData
        : {};
  const nextQueryData: Record<string, unknown> = {
    ...pickQueryData(existingQueryData, ["transaction"]),
    ...queryData,
  };
  const resolvedData =
    Object.prototype.hasOwnProperty.call(ctx, "data") || ctx.data !== undefined
      ? ctx.data
      : getJoinedContextValue(ctx, "data");
  const resolvedBatch =
    Object.prototype.hasOwnProperty.call(ctx, "batch") || ctx.batch !== undefined
      ? ctx.batch
      : getJoinedContextValue(ctx, "batch");

  if (resolvedData !== undefined) {
    nextQueryData.data =
      resolvedData && typeof resolvedData === "object" && !Array.isArray(resolvedData)
        ? { ...resolvedData }
        : resolvedData;
  } else {
    delete nextQueryData.data;
  }

  if (resolvedBatch !== undefined) {
    nextQueryData.batch = Array.isArray(resolvedBatch)
      ? resolvedBatch.map((row: unknown) =>
          row && typeof row === "object"
            ? { ...(row as Record<string, unknown>) }
            : row,
        )
      : resolvedBatch;
  } else {
    delete nextQueryData.batch;
  }

  return nextQueryData;
}

function buildSyncQueryQueryData(
  ctx: Record<string, any>,
  queryData: Record<string, unknown> = {},
): Record<string, unknown> {
  const joinedQueryData = getJoinedContextValue(ctx, "queryData");
  const existingQueryData =
    ctx.queryData && typeof ctx.queryData === "object"
      ? ctx.queryData
      : joinedQueryData && typeof joinedQueryData === "object"
        ? joinedQueryData
        : {};
  const nextQueryData: Record<string, unknown> = {};
  const allowedKeys = [
    "transaction",
    "filter",
    "fields",
    "joins",
    "sort",
    "limit",
    "offset",
    "queryMode",
    "aggregates",
    "groupBy",
  ];

  for (const key of allowedKeys) {
    if (Object.prototype.hasOwnProperty.call(existingQueryData, key)) {
      nextQueryData[key] = existingQueryData[key];
    }
  }

  return {
    ...nextQueryData,
    ...queryData,
  };
}

type SyncTaskGraph = {
  entryTask: Task;
  completionTask: Task;
};

type SyncCyclePhase = "idle" | "primitive" | "map";

type SyncCycleRuntimeState = {
  activeSyncCycleId: string | null;
  activeSyncCycleStartedAt: number;
  phase: SyncCyclePhase;
};

type SyncPhasePendingSummary = Record<string, number>;

const DEFAULT_SYNC_CYCLE_RUNTIME_STATE: SyncCycleRuntimeState = {
  activeSyncCycleId: null,
  activeSyncCycleStartedAt: 0,
  phase: "idle",
};

const REMOTE_AUTHORITY_SYNC_INSERT_CONCURRENCY = 15;
const REMOTE_AUTHORITY_SYNC_QUERY_CONCURRENCY = 8;

function wireSyncTaskGraph(
  predecessorTask: Task,
  graph: SyncTaskGraph | undefined,
  ...completionTasks: (Task | undefined)[]
): Task | undefined {
  if (!graph) {
    return undefined;
  }

  predecessorTask.then(graph.entryTask);
  if (completionTasks.length > 0) {
    graph.completionTask.then(...completionTasks);
  }

  return graph.completionTask;
}

function buildSyncExecutionEnvelope(
  ctx: Record<string, any>,
  queryData: Record<string, unknown>,
): Record<string, unknown> {
  const originalContext = { ...ctx };
  const syncSourceServiceName =
    typeof ctx.__syncSourceServiceName === "string" &&
    ctx.__syncSourceServiceName.trim().length > 0
      ? ctx.__syncSourceServiceName
      : typeof ctx.__serviceName === "string" && ctx.__serviceName.trim().length > 0
        ? ctx.__serviceName
        : resolveSyncServiceName();
  const rootDbOperationFields: Record<string, unknown> = {};
  for (const key of [
    "data",
    "batch",
    "transaction",
    "onConflict",
    "filter",
    "fields",
  ] as const) {
    if (Object.prototype.hasOwnProperty.call(queryData, key)) {
      rootDbOperationFields[key] = queryData[key];
    }
  }
  const nextContext: Record<string, unknown> = {
    __syncing:
      ctx.__syncing === true || ctx.__metadata?.__syncing === true || false,
    __syncSourceServiceName: syncSourceServiceName,
    __preferredTransportProtocol: "rest",
    __resolverOriginalContext: originalContext,
    __resolverQueryData: queryData,
    ...rootDbOperationFields,
    queryData,
  };

  if (typeof ctx.__reason === "string" && ctx.__reason.trim().length > 0) {
    nextContext.__reason = ctx.__reason;
  }

  return nextContext;
}

function markCompletedSyncCycle(
  completedCycles: Set<string>,
  cycleId: string,
  limit = 32,
): boolean {
  if (!cycleId) {
    return false;
  }

  if (completedCycles.has(cycleId)) {
    return false;
  }

  completedCycles.add(cycleId);
  while (completedCycles.size > limit) {
    const oldestCycleId = completedCycles.values().next().value as
      | string
      | undefined;
    if (!oldestCycleId) {
      break;
    }

    completedCycles.delete(oldestCycleId);
  }

  return true;
}

function resolveSyncInsertTask(
  isCadenzaDBReady: boolean,
  tableName: string,
  queryData: Record<string, unknown> = {},
  options: Record<string, unknown> = {},
): SyncTaskGraph | undefined {
  const localInsertTask = Cadenza.getLocalCadenzaDBInsertTask(tableName);

  if (isCadenzaDBReady && !localInsertTask) {
    return undefined;
  }
  const targetTask =
    localInsertTask ??
    Cadenza.createCadenzaDBInsertTask(tableName, queryData, {
      ...options,
      concurrency:
        Number(options.concurrency) > 0
          ? Math.min(
              Number(options.concurrency),
              REMOTE_AUTHORITY_SYNC_INSERT_CONCURRENCY,
            )
          : REMOTE_AUTHORITY_SYNC_INSERT_CONCURRENCY,
      register: false,
      isHidden: true,
    });

  const prepareExecutionTask = Cadenza.createMetaTask(
    `Prepare graph sync insert for ${tableName}`,
    (ctx) => {
      const originalQueryData = buildSyncInsertQueryData(
        ctx as Record<string, any>,
        queryData,
      );

      const hasEmptyObjectData =
        originalQueryData.data &&
        typeof originalQueryData.data === "object" &&
        !Array.isArray(originalQueryData.data) &&
        Object.keys(originalQueryData.data as Record<string, unknown>).length === 0;
      const hasMissingData = originalQueryData.data === undefined;

      if (
        (tableName === "task" ||
          tableName === "signal_registry" ||
          tableName === "directional_task_graph_map") &&
        (hasEmptyObjectData || hasMissingData)
      ) {
        console.warn("[CADENZA_SYNC_EMPTY_INSERT]", {
          tableName,
          hasMissingData,
          hasEmptyObjectData,
          taskName:
            typeof (ctx as Record<string, any>).__taskName === "string"
              ? (ctx as Record<string, any>).__taskName
              : undefined,
          reason:
            typeof (ctx as Record<string, any>).__reason === "string"
              ? (ctx as Record<string, any>).__reason
              : undefined,
          syncSourceServiceName:
            typeof (ctx as Record<string, any>).__syncSourceServiceName === "string"
              ? (ctx as Record<string, any>).__syncSourceServiceName
              : undefined,
          queryData: originalQueryData,
          ctx,
          joinedContexts: Array.isArray((ctx as Record<string, any>).joinedContexts)
            ? (ctx as Record<string, any>).joinedContexts
            : [],
        });
      }

      return buildSyncExecutionEnvelope(
        ctx as Record<string, any>,
        originalQueryData,
      );
    },
    `Prepares ${tableName} graph-sync insert payloads for runner execution.`,
    {
      register: false,
      isHidden: true,
    },
  );

  const finalizeExecutionTask = Cadenza.createMetaTask(
    `Finalize graph sync insert for ${tableName}`,
    (ctx) => {
      const originalContext =
        ctx.__resolverOriginalContext &&
        typeof ctx.__resolverOriginalContext === "object"
          ? (ctx.__resolverOriginalContext as Record<string, unknown>)
          : {};
      const originalQueryData =
        ctx.__resolverQueryData && typeof ctx.__resolverQueryData === "object"
          ? (ctx.__resolverQueryData as Record<string, unknown>)
          : undefined;
      const normalizedContext = {
        ...originalContext,
        ...ctx,
        queryData:
          ctx.queryData && typeof ctx.queryData === "object"
            ? ctx.queryData
            : originalQueryData,
      };

      if (
        originalContext.__syncing === true &&
        !didSyncInsertSucceed(normalizedContext as Record<string, any>)
      ) {
          Cadenza.debounce("meta.sync_requested", {}, 1000);
      }

      return normalizedContext;
    },
    `Finalizes ${tableName} graph-sync insert execution after the authority task finishes.`,
    {
      register: false,
      isHidden: true,
    },
  );

  prepareExecutionTask.then(targetTask);
  targetTask.then(finalizeExecutionTask);

  return {
    entryTask: prepareExecutionTask,
    completionTask: finalizeExecutionTask,
  };
}

const CADENZA_DB_REQUIRED_LOCAL_SYNC_INSERT_TABLES = [
  "intent_registry",
  "routine",
  "task_to_routine_map",
  "signal_registry",
  "task",
  "actor",
  "directional_task_graph_map",
] as const;

const BOOTSTRAP_SYNC_STALE_CYCLE_MS = 120000;
const EARLY_SYNC_TICK_DELAYS_MS = [
  400,
  16000,
  32000,
  48000,
] as const;
const SYNC_PASS_SETTLE_DELAY_MS = 3500;
const SYNC_PASS_EVALUATION_SIGNAL = "meta.sync_controller.evaluate_active_cycle";

function shouldTraceSyncPhase(serviceName: string | undefined): boolean {
  const configured = process.env.CADENZA_SYNC_PHASE_TRACE_SERVICE;
  if (!configured || !serviceName) {
    return false;
  }

  return configured === serviceName;
}

function canonicalizeSignalName(signalName: string | undefined): string {
  if (typeof signalName !== "string") {
    return "";
  }

  return signalName.split(":")[0]?.trim() ?? "";
}

function isLocalServiceReadySignal(signalName: string): boolean {
  const serviceName = resolveSyncServiceName();
  if (!serviceName) {
    return false;
  }

  return signalName === Cadenza.getServiceReadySignalName(serviceName);
}

const BOOTSTRAP_LOCAL_ONLY_ACTOR_NAMES = new Set([
  "BootstrapSyncCycleCoordinatorActor",
  "ServiceLifecycleFlushActor",
  "SocketServerActor",
  "SocketClientActor",
  "SocketClientDiagnosticsActor",
  "TrafficRuntimeActor",
]);

export function isBootstrapLocalOnlyActorName(actorName: string): boolean {
  return BOOTSTRAP_LOCAL_ONLY_ACTOR_NAMES.has(actorName);
}

function isBootstrapLocalOnlySignal(signalName: string): boolean {
  return (
    signalName.startsWith("meta.") ||
    signalName.startsWith("global.meta.") ||
    signalName === "meta.service_registry.insert_execution_requested" ||
    signalName === "meta.service_registry.routeable_transport_missing" ||
    signalName === "meta.signal_broker.added" ||
    signalName === "meta.rest.handshake" ||
    signalName === "meta.rest.delegation_target_not_found" ||
    signalName === "meta.socket.handshake" ||
    signalName === "meta.socket.delegation_target_not_found" ||
    signalName === "meta.fetch.handshake_complete" ||
    signalName === "sub_meta.signal_broker.new_trace" ||
    signalName.startsWith("meta.task.") ||
    signalName.startsWith("meta.sync_controller.") ||
    isLocalServiceReadySignal(signalName)
  );
}

function hasNonZeroPending(summary: SyncPhasePendingSummary): boolean {
  return Object.values(summary).some((value) => Number(value) > 0);
}

function isRegistrableRoutine(routine: { name?: string } | null | undefined): boolean {
  return routine?.name !== "RestServer";
}

function scheduleSyncPassEvaluation(delayMs = SYNC_PASS_SETTLE_DELAY_MS): void {
  Cadenza.debounce(SYNC_PASS_EVALUATION_SIGNAL, {}, delayMs);
}

function getRegistrableTasks(): Task[] {
  return Array.from(Cadenza.registry.tasks.values()).filter(
    (task) => task.register && !task.isHidden && !task.isDeputy,
  );
}

function getBootstrapBlockingTasks(): Task[] {
  return getRegistrableTasks().filter((task) => task.isMeta !== true);
}

function getRegistrableRoutines() {
  return Array.from(Cadenza.registry.routines.values()).filter(isRegistrableRoutine);
}

function getRegistrableSignalObservers(): Array<{
  signalName: string;
  registered?: boolean;
}> {
  const signalObservers = (Cadenza.signalBroker as any)
    .signalObservers as
    | Map<string, { registered?: boolean; tasks?: Task[] | Set<Task> }>
    | undefined;
  if (!signalObservers) {
    return [];
  }

  const canonicalObservers = new Map<
    string,
    {
      signalName: string;
      registered?: boolean;
    }
  >();

  for (const [rawSignalName, observer] of signalObservers.entries()) {
    const signalName = canonicalizeSignalName(rawSignalName);
    if (!signalName || isBootstrapLocalOnlySignal(signalName)) {
      continue;
    }

    const observerTasks = Array.isArray((observer as any)?.tasks)
      ? ((observer as any).tasks as Task[])
      : (observer as any)?.tasks instanceof Set
        ? Array.from((observer as any).tasks as Set<Task>)
        : [];

    if (
      observerTasks.length > 0 &&
      !observerTasks.some(
        (task) => task?.register && !task.isHidden && !task.isDeputy,
      )
    ) {
      continue;
    }

    const existing = canonicalObservers.get(signalName);
    canonicalObservers.set(signalName, {
      signalName,
      registered:
        existing?.registered === true || (observer as any)?.registered === true,
    });
  }

  return Array.from(canonicalObservers.values());
}

function isLocallyHandledIntentName(intentName: string): boolean {
  const observer = Cadenza.inquiryBroker.inquiryObservers.get(intentName);
  if (!observer) {
    return false;
  }

  for (const task of observer.tasks) {
    if (task.register && !task.isHidden && !task.isDeputy) {
      return true;
    }
  }

  return false;
}

function getRegistrableIntentNames(): string[] {
  return Array.from(Cadenza.inquiryBroker.intents.values())
    .map((intent) => buildIntentRegistryData(intent))
    .filter(
      (intentDefinition): intentDefinition is Record<string, unknown> =>
        intentDefinition !== null,
    )
    .filter((intentDefinition) =>
      isLocallyHandledIntentName(String(intentDefinition.name)),
    )
    .filter(
      (intentDefinition) =>
        !isLocalOnlySyncIntent(String(intentDefinition.name)),
    )
    .map((intentDefinition) => String(intentDefinition.name));
}

function isRegistrableLocalIntentDefinition(intent: any): boolean {
  const intentData = buildIntentRegistryData(intent);
  if (!intentData) {
    return false;
  }

  return isLocallyHandledIntentName(String(intentData.name));
}

function buildActorRegistrationKey(
  actor: any,
  serviceName: string,
): string | null {
  const data = buildActorRegistrationData(actor);
  const name =
    typeof data.name === "string" && data.name.trim().length > 0
      ? data.name.trim()
      : "";

  if (!name) {
    return null;
  }

  return `${name}|${data.version}|${serviceName}`;
}

function isBootstrapRegistrableActor(actor: any): boolean {
  const actorName = String(buildActorRegistrationData(actor).name ?? "").trim();
  return actorName.length > 0 && !isBootstrapLocalOnlyActorName(actorName);
}

function buildSignalTaskMapRegistrationKey(input: {
  signalName: string;
  serviceName: string;
  taskName: string;
  taskVersion?: number;
}): string {
  return `${canonicalizeSignalName(input.signalName)}|${input.serviceName}|${input.taskName}|${input.taskVersion ?? 1}`;
}

function normalizeSyncSignalMaps(
  ctx: Record<string, any>,
): Array<{
  signalName: string;
  serviceName: string;
  taskName: string;
  taskVersion: number;
  deleted: boolean;
}> {
  const rawMaps = Array.isArray(ctx.signalToTaskMaps)
    ? ctx.signalToTaskMaps
    : Array.isArray(ctx.signal_to_task_maps)
      ? ctx.signal_to_task_maps
      : Array.isArray(ctx.signalToTaskMap)
        ? ctx.signalToTaskMap
        : Array.isArray(ctx.signal_to_task_map)
          ? ctx.signal_to_task_map
          : [];

  return rawMaps
    .map((map: any) => ({
      signalName: canonicalizeSignalName(map?.signalName ?? map?.signal_name),
      serviceName: String(map?.serviceName ?? map?.service_name ?? "").trim(),
      taskName: String(map?.taskName ?? map?.task_name ?? "").trim(),
      taskVersion: Number(map?.taskVersion ?? map?.task_version ?? 1),
      deleted: map?.deleted === true,
    }))
    .filter(
      (map) =>
        map.signalName.length > 0 &&
        map.serviceName.length > 0 &&
        map.taskName.length > 0,
    );
}

function normalizeSyncIntentMaps(
  ctx: Record<string, any>,
): Array<{
  intentName: string;
  serviceName: string;
  taskName: string;
  taskVersion: number;
  deleted: boolean;
}> {
  const rawMaps = Array.isArray(ctx.intentToTaskMaps)
    ? ctx.intentToTaskMaps
    : Array.isArray(ctx.intent_to_task_maps)
      ? ctx.intent_to_task_maps
      : Array.isArray(ctx.intentToTaskMap)
        ? ctx.intentToTaskMap
        : Array.isArray(ctx.intent_to_task_map)
          ? ctx.intent_to_task_map
          : [];

  return rawMaps
    .map((map: any) => ({
      intentName: String(map?.intentName ?? map?.intent_name ?? "").trim(),
      serviceName: String(map?.serviceName ?? map?.service_name ?? "").trim(),
      taskName: String(map?.taskName ?? map?.task_name ?? "").trim(),
      taskVersion: Number(map?.taskVersion ?? map?.task_version ?? 1),
      deleted: map?.deleted === true,
    }))
    .filter(
      (map) =>
        map.intentName.length > 0 &&
        map.serviceName.length > 0 &&
        map.taskName.length > 0,
    );
}

function readManifestArray(
  ctx: Record<string, any>,
  keys: string[],
): Array<Record<string, unknown>> {
  for (const key of keys) {
    const value = ctx[key];
    if (Array.isArray(value)) {
      return value.filter(
        (entry): entry is Record<string, unknown> =>
          !!entry && typeof entry === "object" && !Array.isArray(entry),
      );
    }
  }

  return [];
}

function normalizeSyncTasks(ctx: Record<string, any>) {
  return readManifestArray(ctx, ["tasks"]).map((task) => ({
    name: String(task.name ?? "").trim(),
    serviceName: String(task.service_name ?? task.serviceName ?? "").trim(),
    version: Number(task.version ?? 1),
  }));
}

function normalizeSyncSignals(ctx: Record<string, any>) {
  return readManifestArray(ctx, ["signals"]).map((signal) => ({
    name: canonicalizeSignalName(
      typeof signal.name === "string" ? signal.name : undefined,
    ),
  }));
}

function normalizeSyncIntents(ctx: Record<string, any>) {
  return readManifestArray(ctx, ["intents"]).map((intent) => ({
    name: String(intent.name ?? "").trim(),
  }));
}

function normalizeSyncActors(ctx: Record<string, any>) {
  return readManifestArray(ctx, ["actors"]).map((actor) => ({
    name: String(actor.name ?? "").trim(),
    serviceName: String(actor.service_name ?? actor.serviceName ?? "").trim(),
    version: Number(actor.version ?? 1),
  }));
}

function normalizeSyncRoutines(ctx: Record<string, any>) {
  return readManifestArray(ctx, ["routines"]).map((routine) => ({
    name: String(routine.name ?? "").trim(),
    serviceName: String(routine.service_name ?? routine.serviceName ?? "").trim(),
    version: Number(routine.version ?? 1),
  }));
}

function normalizeSyncDirectionalTaskMaps(ctx: Record<string, any>) {
  return readManifestArray(ctx, [
    "directionalTaskMaps",
    "directional_task_maps",
  ]).map((map) => ({
    taskName: String(map.task_name ?? map.taskName ?? "").trim(),
    taskVersion: Number(map.task_version ?? map.taskVersion ?? 1),
    predecessorTaskName: String(
      map.predecessor_task_name ?? map.predecessorTaskName ?? "",
    ).trim(),
    predecessorTaskVersion: Number(
      map.predecessor_task_version ?? map.predecessorTaskVersion ?? 1,
    ),
    serviceName: String(map.service_name ?? map.serviceName ?? "").trim(),
    predecessorServiceName: String(
      map.predecessor_service_name ?? map.predecessorServiceName ?? "",
    ).trim(),
  }));
}

function normalizeSyncActorTaskMaps(ctx: Record<string, any>) {
  return readManifestArray(ctx, ["actorTaskMaps", "actor_task_maps"]).map((map) => ({
    actorName: String(map.actor_name ?? map.actorName ?? "").trim(),
    actorVersion: Number(map.actor_version ?? map.actorVersion ?? 1),
    taskName: String(map.task_name ?? map.taskName ?? "").trim(),
    taskVersion: Number(map.task_version ?? map.taskVersion ?? 1),
    serviceName: String(map.service_name ?? map.serviceName ?? "").trim(),
  }));
}

function normalizeSyncTaskToRoutineMaps(ctx: Record<string, any>) {
  return readManifestArray(ctx, [
    "taskToRoutineMaps",
    "task_to_routine_maps",
  ]).map((map) => ({
    taskName: String(map.task_name ?? map.taskName ?? "").trim(),
    taskVersion: Number(map.task_version ?? map.taskVersion ?? 1),
    routineName: String(map.routine_name ?? map.routineName ?? "").trim(),
    routineVersion: Number(map.routine_version ?? map.routineVersion ?? 1),
    serviceName: String(map.service_name ?? map.serviceName ?? "").trim(),
  }));
}

function resolveLocalTaskFromSyncContext(ctx: Record<string, any>): Task | undefined {
  const candidateTaskNames = [
    ctx.__taskName,
    ctx.data?.name,
    ctx.queryData?.data?.name,
    ctx.__resolverOriginalContext?.__taskName,
    ctx.__resolverOriginalContext?.data?.name,
    getJoinedContextValue(ctx, "data") &&
    typeof getJoinedContextValue(ctx, "data") === "object"
      ? (getJoinedContextValue(ctx, "data") as Record<string, unknown>).name
      : undefined,
    getJoinedContextValue(ctx, "queryData") &&
    typeof getJoinedContextValue(ctx, "queryData") === "object"
      ? ((getJoinedContextValue(ctx, "queryData") as Record<string, unknown>)
          .data as Record<string, unknown> | undefined)?.name
      : undefined,
  ];

  const taskName = candidateTaskNames.find(
    (candidate): candidate is string =>
      typeof candidate === "string" && candidate.trim().length > 0,
  );

  return taskName ? Cadenza.get(taskName) : undefined;
}

function resolveLocalRoutineFromSyncContext(
  ctx: Record<string, any>,
): ReturnType<typeof Cadenza.getRoutine> | undefined {
  const candidateRoutineNames = [
    ctx.__routineName,
    ctx.data?.name,
    ctx.queryData?.data?.name,
    ctx.__resolverOriginalContext?.__routineName,
    ctx.__resolverOriginalContext?.data?.name,
    getJoinedContextValue(ctx, "data") &&
    typeof getJoinedContextValue(ctx, "data") === "object"
      ? (getJoinedContextValue(ctx, "data") as Record<string, unknown>).name
      : undefined,
    getJoinedContextValue(ctx, "queryData") &&
    typeof getJoinedContextValue(ctx, "queryData") === "object"
      ? ((getJoinedContextValue(ctx, "queryData") as Record<string, unknown>)
          .data as Record<string, unknown> | undefined)?.name
      : undefined,
  ];

  const routineName = candidateRoutineNames.find(
    (candidate): candidate is string =>
      typeof candidate === "string" && candidate.trim().length > 0,
  );

  return routineName ? Cadenza.getRoutine(routineName) : undefined;
}

function shouldTrackDirectionalTaskMapForBootstrap(
  predecessorTask: Task | undefined,
  nextTask: Task | undefined,
): boolean {
  if (!predecessorTask || !nextTask) {
    return false;
  }

  return (
    predecessorTask.isMeta !== true &&
    predecessorTask.isSubMeta !== true &&
    nextTask.isMeta !== true &&
    nextTask.isSubMeta !== true
  );
}

function resolveSignalNameFromSyncContext(ctx: Record<string, any>): string | undefined {
  const candidateSignalNames = [
    ctx.signalName,
    ctx.__signal,
    ctx.data?.name,
    ctx.queryData?.data?.name,
    getJoinedContextValue(ctx, "data") &&
    typeof getJoinedContextValue(ctx, "data") === "object"
      ? (getJoinedContextValue(ctx, "data") as Record<string, unknown>).name
      : undefined,
  ];

  for (const candidate of candidateSignalNames) {
    if (typeof candidate === "string" && candidate.trim().length > 0) {
      return candidate;
    }
  }

  return undefined;
}

export default class GraphSyncController {
  private static _instance: GraphSyncController;
  public static get instance(): GraphSyncController {
    if (!this._instance) this._instance = new GraphSyncController();
    return this._instance;
  }

  splitSignalsTask: Task | undefined;
  splitTasksForRegistration: Task | undefined;
  splitIntentsTask: Task | undefined;
  registerSignalToTaskMapTask: Task | undefined;
  registerIntentToTaskMapTask: Task | undefined;
  registerTaskMapTask: Task | undefined;
  registerDeputyRelationshipTask: Task | undefined;
  splitRoutinesTask: Task | undefined;
  splitTasksInRoutines: Task | undefined;
  splitActorsForRegistration: Task | undefined;
  registerActorTaskMapTask: Task | undefined;

  registeredActors: Set<string> = new Set();
  registeredActorTaskMaps: Set<string> = new Set();
  registeredIntentDefinitions: Set<string> = new Set();
  authoritativeSignalTaskMaps: Set<string> = new Set();
  tasksSynced: boolean = false;
  actorsSynced: boolean = false;
  signalsSynced: boolean = false;
  intentsSynced: boolean = false;
  routinesSynced: boolean = false;
  directionalTaskMapsSynced: boolean = false;
  signalTaskMapsSynced: boolean = false;
  intentTaskMapsSynced: boolean = false;
  actorTaskMapsSynced: boolean = false;
  routineTaskMapsSynced: boolean = false;

  isCadenzaDBReady: boolean = false;
  initialized: boolean = false;
  initRetryScheduled: boolean = false;
  initRetryTask: Task | undefined;
  lastMissingLocalCadenzaDBInsertTablesKey: string = "";
  primitivePhaseCompletedCycles: Set<string> = new Set();
  mapPhaseCompletedCycles: Set<string> = new Set();
  syncCycleCoordinatorActor: any;
  localServiceInserted: boolean = false;
  localServiceInstanceInserted: boolean = false;

  private getMissingLocalCadenzaDBInsertTables(): string[] {
    return CADENZA_DB_REQUIRED_LOCAL_SYNC_INSERT_TABLES.filter(
      (tableName) => !Cadenza.getLocalCadenzaDBInsertTask(tableName),
    );
  }

  private ensureRetryInitTask(): Task {
    if (this.initRetryTask) {
      return this.initRetryTask;
    }

    this.initRetryTask =
      Cadenza.get("Retry graph sync init") ??
      Cadenza.createUniqueMetaTask(
        "Retry graph sync init",
        () => {
          this.initRetryScheduled = false;
          this.init();
          return true;
        },
        "Retries graph sync controller initialization once local authority tasks exist.",
      ).doOn("meta.sync_controller.init_retry");

    return this.initRetryTask;
  }

  init() {
    if (this.initialized) {
      return;
    }

    const serviceName = resolveSyncServiceName();
    if (serviceName === "CadenzaDB") {
      const missingLocalInsertTables = this.getMissingLocalCadenzaDBInsertTables();
      if (missingLocalInsertTables.length > 0) {
        this.ensureRetryInitTask();
        const missingKey = missingLocalInsertTables.join(",");
        if (missingKey !== this.lastMissingLocalCadenzaDBInsertTablesKey) {
          this.lastMissingLocalCadenzaDBInsertTablesKey = missingKey;
          Cadenza.log(
            "Waiting for local CadenzaDB sync insert tasks before initializing graph sync controller.",
            {
              missingLocalInsertTables,
            },
            "info",
          );
        }

        if (!this.initRetryScheduled) {
          this.initRetryScheduled = true;
          Cadenza.schedule(
            "meta.sync_controller.init_retry",
            {
              __missingLocalInsertTables: missingLocalInsertTables,
            },
            250,
          );
        }
        return;
      }

      this.lastMissingLocalCadenzaDBInsertTablesKey = "";
    }

    this.initialized = true;
    this.syncCycleCoordinatorActor =
      this.syncCycleCoordinatorActor ??
      Cadenza.createActor<{}, SyncCycleRuntimeState>({
        name: "BootstrapSyncCycleCoordinatorActor",
        description:
          "Coordinates the active bootstrap graph sync cycle for a service instance.",
        defaultKey: "bootstrap-graph-sync",
        initState: {},
        session: {
          persistDurableState: false,
        },
      });
    const insertIntentRegistryTask = resolveSyncInsertTask(
      this.isCadenzaDBReady,
      "intent_registry",
      {
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      },
      { concurrency: 30 },
    );
    const finalizeTaskSync = (emit: any, ctx: Record<string, unknown>) => {
      const pendingTasks = getBootstrapBlockingTasks().filter(
        (task) => !task.registered,
      );
      const serviceName =
        typeof ctx.__serviceName === "string"
          ? ctx.__serviceName
          : resolveSyncServiceName();
      if (shouldTraceSyncPhase(serviceName)) {
        console.log("[CADENZA_SYNC_PHASE_TRACE] finalize_tasks", {
          serviceName,
          pendingCount: pendingTasks.length,
          sample: pendingTasks.slice(0, 5).map((task) => task.name),
        });
      }
      if (pendingTasks.length > 0) {
        this.tasksSynced = false;
        scheduleSyncPassEvaluation();
        return false;
      }

      const shouldEmit = !this.tasksSynced;
      this.tasksSynced = true;
      scheduleSyncPassEvaluation();
      if (shouldEmit) {
        emit(
          "meta.sync_controller.synced_tasks",
          buildMinimalSyncSignalContext(ctx),
        );
      }

      return true;
    };
    const finalizeRoutineSync = (emit: any, ctx: Record<string, unknown>) => {
      const pendingRoutines = getRegistrableRoutines().filter(
        (routine) => !routine.registered,
      );
      const serviceName =
        typeof ctx.__serviceName === "string"
          ? ctx.__serviceName
          : resolveSyncServiceName();
      if (shouldTraceSyncPhase(serviceName)) {
        console.log("[CADENZA_SYNC_PHASE_TRACE] finalize_routines", {
          serviceName,
          pendingCount: pendingRoutines.length,
          sample: pendingRoutines.slice(0, 5).map((routine) => routine.name),
        });
      }
      if (pendingRoutines.length > 0) {
        this.routinesSynced = false;
        scheduleSyncPassEvaluation();
        return false;
      }

      const shouldEmit = !this.routinesSynced;
      this.routinesSynced = true;
      scheduleSyncPassEvaluation();
      if (shouldEmit) {
        emit(
          "meta.sync_controller.synced_routines",
          buildMinimalSyncSignalContext(ctx),
        );
      }

      return true;
    };
    const finalizeSignalSync = (emit: any, ctx: Record<string, unknown>) => {
      const pendingSignals = getRegistrableSignalObservers().filter(
        (observer) => observer.registered !== true,
      );
      const serviceName =
        typeof ctx.__serviceName === "string"
          ? ctx.__serviceName
          : resolveSyncServiceName();
      if (shouldTraceSyncPhase(serviceName)) {
        console.log("[CADENZA_SYNC_PHASE_TRACE] finalize_signals", {
          serviceName,
          pendingCount: pendingSignals.length,
          sample: pendingSignals.slice(0, 5).map((observer) => observer.signalName),
        });
      }
      if (pendingSignals.length > 0) {
        this.signalsSynced = false;
        scheduleSyncPassEvaluation();
        return false;
      }

      const shouldEmit = !this.signalsSynced;
      this.signalsSynced = true;
      scheduleSyncPassEvaluation();
      if (shouldEmit) {
        emit(
          "meta.sync_controller.synced_signals",
          buildMinimalSyncSignalContext(ctx),
        );
      }

      return true;
    };
    const finalizeIntentSync = (emit: any, ctx: Record<string, unknown>) => {
      const pendingIntentNames = getRegistrableIntentNames().filter(
        (intentName) => !this.registeredIntentDefinitions.has(intentName),
      );
      const serviceName =
        typeof ctx.__serviceName === "string"
          ? ctx.__serviceName
          : resolveSyncServiceName();
      if (shouldTraceSyncPhase(serviceName)) {
        console.log("[CADENZA_SYNC_PHASE_TRACE] finalize_intents", {
          serviceName,
          pendingCount: pendingIntentNames.length,
          sample: pendingIntentNames.slice(0, 5),
        });
      }
      if (pendingIntentNames.length > 0) {
        this.intentsSynced = false;
        scheduleSyncPassEvaluation();
        return false;
      }

      const shouldEmit = !this.intentsSynced;
      this.intentsSynced = true;
      scheduleSyncPassEvaluation();
      if (shouldEmit) {
        emit(
          "meta.sync_controller.synced_intents",
          buildMinimalSyncSignalContext(ctx),
        );
      }

      return true;
    };
    const finalizeActorSync = (emit: any, ctx: Record<string, unknown>) => {
      const syncServiceName = resolveSyncServiceName();
      if (!syncServiceName) {
        this.actorsSynced = false;
        return false;
      }

      const pendingActorKeys = Cadenza.getAllActors()
        .filter((actor) => isBootstrapRegistrableActor(actor))
        .map((actor) => buildActorRegistrationKey(actor, syncServiceName))
        .filter((registrationKey): registrationKey is string => Boolean(registrationKey))
        .filter((registrationKey) => !this.registeredActors.has(registrationKey));

      if (shouldTraceSyncPhase(syncServiceName)) {
        console.log("[CADENZA_SYNC_PHASE_TRACE] finalize_actors", {
          serviceName: syncServiceName,
          pendingCount: pendingActorKeys.length,
          sample: pendingActorKeys.slice(0, 5),
        });
      }

      if (pendingActorKeys.length > 0) {
        this.actorsSynced = false;
        scheduleSyncPassEvaluation();
        return false;
      }

      const shouldEmit = !this.actorsSynced;
      this.actorsSynced = true;
      scheduleSyncPassEvaluation();
      if (shouldEmit) {
        emit(
          "meta.sync_controller.synced_actors",
          buildMinimalSyncSignalContext(ctx),
        );
      }

      return true;
    };
    const buildPrimitivePendingSummary = (): SyncPhasePendingSummary => ({
      tasks: getBootstrapBlockingTasks().filter((task) => !task.registered).length,
      signals: getRegistrableSignalObservers().filter(
        (observer) => observer.registered !== true,
      ).length,
      intents: getRegistrableIntentNames().filter(
        (intentName) => !this.registeredIntentDefinitions.has(intentName),
      ).length,
      actors: (() => {
        const syncServiceName = resolveSyncServiceName();
        if (!syncServiceName) {
          return 0;
        }
        return Cadenza.getAllActors()
          .filter((actor) => isBootstrapRegistrableActor(actor))
          .map((actor) => buildActorRegistrationKey(actor, syncServiceName))
          .filter((registrationKey): registrationKey is string =>
            Boolean(registrationKey),
          )
          .filter((registrationKey) => !this.registeredActors.has(registrationKey))
          .length;
      })(),
      routines: getRegistrableRoutines().filter((routine) => !routine.registered)
        .length,
    });
    const buildMapPendingSummary = (): SyncPhasePendingSummary => ({
      directionalTaskMaps: hasPendingDirectionalTaskMaps() ? 1 : 0,
      signalTaskMaps: hasPendingSignalTaskMaps() ? 1 : 0,
      intentTaskMaps: hasPendingIntentTaskMaps() ? 1 : 0,
      actorTaskMaps: hasPendingActorTaskMaps() ? 1 : 0,
      routineTaskMaps: hasPendingRoutineTaskMaps() ? 1 : 0,
    });
    const gatherTaskRegistrationTask = Cadenza.createUniqueMetaTask(
      "Gather task registration",
      (ctx, emit) => finalizeTaskSync(emit, ctx),
      "Completes task registration when all registrable tasks are marked registered.",
      {
        register: false,
        isHidden: true,
      },
    );
    const gatherRoutineRegistrationTask = Cadenza.createUniqueMetaTask(
      "Gather routine registration",
      (ctx, emit) => finalizeRoutineSync(emit, ctx),
      "Completes routine registration when all registrable routines are marked registered.",
      {
        register: false,
        isHidden: true,
      },
    );
    const gatherSignalRegistrationTask = Cadenza.createUniqueMetaTask(
      "Gather signal registration",
      (ctx, emit) => finalizeSignalSync(emit, ctx),
      "Completes signal registration when all signal observers are marked registered.",
      {
        register: false,
        isHidden: true,
      },
    );
    const gatherIntentRegistrationTask = Cadenza.createUniqueMetaTask(
      "Gather intent registration",
      (ctx, emit) => finalizeIntentSync(emit, ctx),
      "Completes intent registration when all registrable intents are marked registered.",
      {
        register: false,
        isHidden: true,
      },
    );
    const gatherActorRegistrationTask = Cadenza.createUniqueMetaTask(
      "Gather actor registration",
      (ctx, emit) => finalizeActorSync(emit, ctx),
      "Completes actor registration when all registrable actors are marked registered.",
      {
        register: false,
        isHidden: true,
      },
    );

    this.splitRoutinesTask = Cadenza.createMetaTask(
      "Split routines for registration",
      function* (this: GraphSyncController, ctx: any) {
        const { routines } = ctx;
        if (!routines) return;
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return;
        }

        for (const routine of routines) {
          if (!isRegistrableRoutine(routine)) continue;
          if (routine.registered) continue;
          this.routinesSynced = false;
          yield {
            __syncing: ctx.__syncing,
            data: {
              name: routine.name,
              version: routine.version,
              description: routine.description,
              service_name: serviceName,
              is_meta: routine.isMeta,
            },
            __routineName: routine.name,
          };
        }
      }.bind(this),
    );

    const routineRegistrationGraph = resolveSyncInsertTask(
      this.isCadenzaDBReady,
      "routine",
      {
        onConflict: {
          target: ["name", "version", "service_name"],
          action: {
            do: "nothing",
          },
        },
      },
      { concurrency: 30 },
    );
    const registerRoutineTask = Cadenza.createMetaTask("Register routine", (ctx) => {
      if (!didSyncInsertSucceed(ctx)) {
        return;
      }

      scheduleSyncPassEvaluation();
      const routine = resolveLocalRoutineFromSyncContext(ctx);
      if (!routine) {
        return true;
      }
      routine.registered = true;

      return true;
    }).then(gatherRoutineRegistrationTask);
    wireSyncTaskGraph(this.splitRoutinesTask, routineRegistrationGraph, registerRoutineTask);

    this.splitTasksInRoutines = Cadenza.createMetaTask(
      "Split tasks in routines",
      function* (ctx: any) {
        const { routines } = ctx;
        if (!routines) return;
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return;
        }
        for (const routine of routines) {
          if (!isRegistrableRoutine(routine)) continue;
          if (!routine.registered) continue;

          for (const task of routine.tasks) {
            if (!task) {
              continue;
            }

            if (routine.registeredTasks.has(task.name)) continue;

            const tasks = task.getIterator();

            while (tasks.hasNext()) {
              const nextTask = tasks.next();
              if (!nextTask?.registered) {
                continue;
              }

              yield {
                __syncing: ctx.__syncing,
                data: {
                  task_name: nextTask.name,
                  task_version: nextTask.version,
                  routine_name: routine.name,
                  routine_version: routine.version,
                  service_name: serviceName,
                },
                __routineName: routine.name,
                __taskName: nextTask.name,
              };
            }
          }
        }
      },
    );

    const registerTaskToRoutineMapGraph = resolveSyncInsertTask(
      this.isCadenzaDBReady,
      "task_to_routine_map",
      {
        onConflict: {
          target: [
            "task_name",
            "routine_name",
            "task_version",
            "routine_version",
            "service_name",
          ],
          action: {
            do: "nothing",
          },
        },
      },
      { concurrency: 30 },
    );
    const registerTaskToRoutineMapTask = Cadenza.createMetaTask(
      "Register routine task",
      (ctx) => {
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        scheduleSyncPassEvaluation();
        const routine = resolveLocalRoutineFromSyncContext(ctx);
        if (!routine) {
          return true;
        }
        routine.registeredTasks.add(ctx.__taskName);
      },
    );
    wireSyncTaskGraph(
      this.splitTasksInRoutines,
      registerTaskToRoutineMapGraph,
      registerTaskToRoutineMapTask,
    );

    this.splitSignalsTask = Cadenza.createMetaTask(
      "Split signals for registration",
      function* (this: GraphSyncController, ctx: any) {
        const { signals } = ctx;
        if (!signals) return;

        const seenSignals = new Set<string>();
        const filteredSignals = signals
          .map((signal: { signal: string; data: any }) => ({
            signalName: canonicalizeSignalName(signal.signal),
            data: signal.data,
          }))
          .filter((signal: { signalName: string; data: any }) => {
            if (
              !signal.signalName ||
              signal.data?.registered ||
              isBootstrapLocalOnlySignal(signal.signalName)
            ) {
              return false;
            }

            if (seenSignals.has(signal.signalName)) {
              return false;
            }

            seenSignals.add(signal.signalName);
            return true;
          })
          .map((signal: { signalName: string }) => signal.signalName);

        for (const signal of filteredSignals) {
          const { isMeta, isGlobal, domain, action } =
            decomposeSignalName(signal);
          this.signalsSynced = false;

          yield {
            __syncing: ctx.__syncing,
            data: {
              name: signal,
              is_global: isGlobal,
              domain,
              action,
              is_meta: isMeta,
            },
            __signal: signal,
          };
        }
      }.bind(this),
    );

    const signalRegistrationGraph = resolveSyncInsertTask(
      this.isCadenzaDBReady,
      "signal_registry",
      {
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      },
      { concurrency: 30 },
    );
    const processSignalRegistrationTask = Cadenza.createMetaTask(
      "Process signal registration",
      (ctx, emit) => {
        const insertSucceeded = didSyncInsertSucceed(ctx);
        const signalName = resolveSignalNameFromSyncContext(ctx);

        if (!insertSucceeded) {
          return;
        }

        scheduleSyncPassEvaluation();

        if (!signalName) {
          return false;
        }

        const signalObservers = (Cadenza.signalBroker as any).signalObservers;
        if (!signalObservers?.has(signalName)) {
          Cadenza.signalBroker.addSignal(signalName);
        }

        const observer = signalObservers?.get(signalName);
        if (observer) {
          observer.registered = true;
          observer.registrationRequested = false;
        }

        emit(
          "meta.sync_controller.signal_registered",
          buildMinimalSyncSignalContext(ctx, {
            __signal: signalName,
          }),
        );

        return { signalName };
      },
    )
      .then(Cadenza.signalBroker.registerSignalTask!)
      .then(gatherSignalRegistrationTask);
    wireSyncTaskGraph(
      this.splitSignalsTask,
      signalRegistrationGraph,
      processSignalRegistrationTask,
    );

    const applyAuthoritativeLocalSignalRegistrationsTask = Cadenza.createMetaTask(
      "Apply authoritative local signal registrations",
      (ctx) => {
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return false;
        }

        let changed = false;
        for (const map of normalizeSyncSignalMaps(ctx)) {
          if (map.deleted || map.serviceName !== serviceName) {
            continue;
          }

          this.authoritativeSignalTaskMaps.add(
            buildSignalTaskMapRegistrationKey(map),
          );

          const signalObservers = (Cadenza.signalBroker as any).signalObservers;
          if (!signalObservers?.has(map.signalName)) {
            Cadenza.signalBroker.addSignal(map.signalName);
          }

          const observer = signalObservers?.get(map.signalName);
          if (observer && observer.registered !== true) {
            observer.registered = true;
            observer.registrationRequested = false;
            changed = true;
          }

          const task = Cadenza.get(map.taskName) as any;
          if (task && (!Number.isFinite(map.taskVersion) || task.version === map.taskVersion)) {
            if (task.registered !== true) {
              task.registered = true;
              task.registrationRequested = false;
              changed = true;
            }

            task.registeredSignals = task.registeredSignals ?? new Set<string>();
            if (!task.registeredSignals.has(map.signalName)) {
              task.registeredSignals.add(map.signalName);
              changed = true;
            }
          }
        }

        if (!changed) {
          return false;
        }

        scheduleSyncPassEvaluation();
        return ctx;
      },
      "Marks local signals and mapped tasks as registered when authority full sync already contains the local signal-to-task map rows.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn("meta.service_registry.registered_global_signals");
    applyAuthoritativeLocalSignalRegistrationsTask.then(
      gatherSignalRegistrationTask,
      gatherTaskRegistrationTask,
    );

    this.splitTasksForRegistration = Cadenza.createMetaTask(
      "Split tasks for registration",
      function* (this: GraphSyncController, ctx: any) {
        const tasks = ctx.tasks;
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return;
        }

        for (const task of tasks) {
          if (task.hidden || !task.register || task.isDeputy) continue;
          if (task.registered) continue;
          const { __functionString, __getTagCallback } = task.export();
          this.tasksSynced = false;

          const taskRegistrationData = sanitizePersistedTaskSourceFields(task, {
            name: task.name,
            version: task.version,
            description: task.description,
            function_string: __functionString,
            tag_id_getter: __getTagCallback,
            layer_index: task.layerIndex,
            concurrency: task.concurrency,
            timeout: task.timeout,
            is_unique: task.isUnique,
            is_signal: task.isSignal,
            is_throttled: task.isThrottled,
            is_debounce: task.isDebounce,
            is_ephemeral: task.isEphemeral,
            is_meta: task.isMeta,
            is_sub_meta: task.isSubMeta,
            is_hidden: task.isHidden,
            validate_input_context: task.validateInputContext,
            validate_output_context: task.validateOutputContext,
            retry_count: task.retryCount,
            retry_delay: task.retryDelay,
            retry_delay_max: task.retryDelayMax,
            retry_delay_factor: task.retryDelayFactor,
            service_name: serviceName,
            signals: {
              emits: Array.from(task.emitsSignals),
              signalsToEmitAfter: Array.from(task.signalsToEmitAfter),
              signalsToEmitOnFail: Array.from(task.signalsToEmitOnFail),
              observed: Array.from(task.observedSignals),
            },
            intents: Array.from(task.handlesIntents),
          });

          yield {
            __syncing: ctx.__syncing,
            data: taskRegistrationData,
            __taskName: task.name,
          };
        }
      }.bind(this),
    );

    const registerTaskGraph = resolveSyncInsertTask(
      this.isCadenzaDBReady,
      "task",
      {
        onConflict: {
          target: ["name", "service_name", "version"],
          action: {
            do: "nothing",
          },
        },
      },
      { concurrency: 30 },
    );
    const registerTaskTask = Cadenza.createMetaTask(
      "Record registration",
      (ctx, emit) => {
        const task = resolveLocalTaskFromSyncContext(ctx);
        const serviceName = resolveSyncServiceName(task);
        const insertSucceeded = didSyncInsertSucceed(ctx);

        if (!insertSucceeded) {
          return;
        }

        scheduleSyncPassEvaluation();

        if (!task) {
          return true;
        }

        task.registered = true;
        (task as any).registrationRequested = false;
        emit(
          "meta.sync_controller.task_registered",
          buildMinimalSyncSignalContext(ctx, {
            __taskName: ctx.__taskName,
          }),
        );

        return true;
      },
    ).then(gatherTaskRegistrationTask);
    wireSyncTaskGraph(this.splitTasksForRegistration, registerTaskGraph, registerTaskTask);

    this.splitActorsForRegistration = Cadenza.createMetaTask(
      "Split actors for registration",
      function* (this: GraphSyncController, ctx: any) {
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return;
        }

        const actors = ctx.actors ?? [];
        for (const actor of actors) {
          if (!isBootstrapRegistrableActor(actor)) {
            continue;
          }

          const data: Record<string, any> = {
            ...buildActorRegistrationData(actor),
            service_name: serviceName,
          };
          if (!data.name) {
            continue;
          }

          const registrationKey = `${data.name}|${data.version}|${data.service_name}`;
          if (this.registeredActors.has(registrationKey)) {
            continue;
          }
          this.actorsSynced = false;

          yield {
            data,
            __actorRegistrationKey: registrationKey,
          };
        }
      }.bind(this),
    );
    const actorRegistrationGraph = resolveSyncInsertTask(
      this.isCadenzaDBReady,
      "actor",
      {
        onConflict: {
          target: ["name", "service_name", "version"],
          action: {
            do: "nothing",
          },
        },
      },
      { concurrency: 30 },
    );
    const recordActorRegistrationTask = Cadenza.createMetaTask(
      "Record actor registration",
      (ctx) => {
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        scheduleSyncPassEvaluation();
        this.registeredActors.add(ctx.__actorRegistrationKey);
        return true;
      },
    ).then(gatherActorRegistrationTask);
    wireSyncTaskGraph(
      this.splitActorsForRegistration,
      actorRegistrationGraph,
      recordActorRegistrationTask,
    );

    this.registerActorTaskMapTask = Cadenza.createMetaTask(
      "Defer actor task maps to manifest sync",
      () => false,
      "Actor-task structural maps are derived from service manifests and full sync, not persisted incrementally on the hot registration path.",
      {
        register: false,
        isHidden: true,
      },
    );

    this.registerSignalToTaskMapTask = Cadenza.createMetaTask(
      "Defer signal task maps to manifest sync",
      () => {
        scheduleSyncPassEvaluation();
        return false;
      },
      "Signal-task structural maps are derived from service manifests and authority full sync, not persisted incrementally on the hot registration path.",
      {
        register: false,
        isHidden: true,
      },
    );

    this.splitIntentsTask = Cadenza.createMetaTask(
      "Split intents for registration",
      function* (this: GraphSyncController, ctx: any) {
        const intents = Array.isArray(ctx.intents)
          ? ctx.intents
          : Array.from(Cadenza.inquiryBroker.intents.values());

        for (const intent of intents) {
          if (!isRegistrableLocalIntentDefinition(intent)) {
            continue;
          }

          const intentData = buildIntentRegistryData(intent);
          if (!intentData) {
            continue;
          }

          if (this.registeredIntentDefinitions.has(intentData.name as string)) {
            continue;
          }

          this.intentsSynced = false;
          yield {
            __syncing: ctx.__syncing,
            data: intentData,
            __intentName: intentData.name,
          };
        }
      }.bind(this),
    );

    const recordIntentDefinitionRegistrationTask = Cadenza.createMetaTask(
      "Record intent definition registration",
      (ctx, emit) => {
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        scheduleSyncPassEvaluation();

        const intentName =
          typeof ctx.__intentName === "string" ? ctx.__intentName : "";
        this.registeredIntentDefinitions.add(intentName);

        const intentDefinition = intentName
          ? ((Cadenza.inquiryBroker.intents.get(intentName) as unknown as Record<
              string,
              unknown
            > | undefined) ?? null)
          : null;
        if (intentDefinition) {
          intentDefinition.registered = true;
          intentDefinition.registrationRequested = false;
        }

        emit(
          "meta.sync_controller.intent_registered",
          buildMinimalSyncSignalContext(ctx, {
            __intentName: intentName,
          }),
        );

        return true;
      },
    ).then(gatherIntentRegistrationTask);
    wireSyncTaskGraph(
      this.splitIntentsTask,
      insertIntentRegistryTask,
      recordIntentDefinitionRegistrationTask,
    );

    const applyAuthoritativeLocalIntentRegistrationsTask = Cadenza.createMetaTask(
      "Apply authoritative local intent registrations",
      (ctx) => {
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return false;
        }

        let changed = false;
        for (const map of normalizeSyncIntentMaps(ctx)) {
          if (map.deleted || map.serviceName !== serviceName) {
            continue;
          }

          this.registeredIntentDefinitions.add(map.intentName);

          const intentDefinition = Cadenza.inquiryBroker.intents.get(map.intentName) as
            | Record<string, any>
            | undefined;
          if (intentDefinition) {
            if (intentDefinition.registered !== true) {
              intentDefinition.registered = true;
              intentDefinition.registrationRequested = false;
              changed = true;
            }
          }

          const task = Cadenza.get(map.taskName) as any;
          if (task && (!Number.isFinite(map.taskVersion) || task.version === map.taskVersion)) {
            task.__registeredIntents = task.__registeredIntents ?? new Set<string>();
            if (!task.__registeredIntents.has(map.intentName)) {
              task.__registeredIntents.add(map.intentName);
              changed = true;
            }

            if (task.registered !== true) {
              task.registered = true;
              task.registrationRequested = false;
              changed = true;
            }
          }
        }

        if (!changed) {
          return false;
        }

        scheduleSyncPassEvaluation();
        return ctx;
      },
      "Marks local intents and mapped tasks as registered when authority full sync already contains the local intent-to-task map rows.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn("meta.service_registry.registered_global_intents");
    applyAuthoritativeLocalIntentRegistrationsTask.then(
      gatherIntentRegistrationTask,
      gatherTaskRegistrationTask,
    );

    const applyAuthoritativeLocalManifestRegistrationsTask = Cadenza.createMetaTask(
      "Apply authoritative local manifest registrations",
      (ctx) => {
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return false;
        }

        this.authoritativeSignalTaskMaps.clear();
        let changed = false;

        for (const taskDefinition of normalizeSyncTasks(ctx)) {
          if (taskDefinition.serviceName !== serviceName) {
            continue;
          }

          const task = Cadenza.get(taskDefinition.name) as any;
          if (
            task &&
            (!Number.isFinite(taskDefinition.version) ||
              task.version === taskDefinition.version) &&
            task.registered !== true
          ) {
            task.registered = true;
            task.registrationRequested = false;
            changed = true;
          }
        }

        for (const signalDefinition of normalizeSyncSignals(ctx)) {
          if (!signalDefinition.name) {
            continue;
          }

          const signalObservers = (Cadenza.signalBroker as any).signalObservers;
          if (!signalObservers?.has(signalDefinition.name)) {
            Cadenza.signalBroker.addSignal(signalDefinition.name);
          }

          const observer = signalObservers?.get(signalDefinition.name);
          if (observer && observer.registered !== true) {
            observer.registered = true;
            observer.registrationRequested = false;
            changed = true;
          }
        }

        for (const map of normalizeSyncSignalMaps(ctx)) {
          if (map.deleted || map.serviceName !== serviceName) {
            continue;
          }

          this.authoritativeSignalTaskMaps.add(
            buildSignalTaskMapRegistrationKey(map),
          );

          const signalObservers = (Cadenza.signalBroker as any).signalObservers;
          if (!signalObservers?.has(map.signalName)) {
            Cadenza.signalBroker.addSignal(map.signalName);
          }

          const observer = signalObservers?.get(map.signalName);
          if (observer && observer.registered !== true) {
            observer.registered = true;
            observer.registrationRequested = false;
            changed = true;
          }

          const task = Cadenza.get(map.taskName) as any;
          if (task && (!Number.isFinite(map.taskVersion) || task.version === map.taskVersion)) {
            if (task.registered !== true) {
              task.registered = true;
              task.registrationRequested = false;
              changed = true;
            }

            task.registeredSignals = task.registeredSignals ?? new Set<string>();
            if (!task.registeredSignals.has(map.signalName)) {
              task.registeredSignals.add(map.signalName);
              changed = true;
            }
          }
        }

        for (const intentDefinition of normalizeSyncIntents(ctx)) {
          if (!intentDefinition.name) {
            continue;
          }

          this.registeredIntentDefinitions.add(intentDefinition.name);
          const intent = Cadenza.inquiryBroker.intents.get(intentDefinition.name) as
            | Record<string, any>
            | undefined;
          if (intent && intent.registered !== true) {
            intent.registered = true;
            intent.registrationRequested = false;
            changed = true;
          }
        }

        for (const map of normalizeSyncIntentMaps(ctx)) {
          if (map.deleted || map.serviceName !== serviceName) {
            continue;
          }

          this.registeredIntentDefinitions.add(map.intentName);

          const intent = Cadenza.inquiryBroker.intents.get(map.intentName) as
            | Record<string, any>
            | undefined;
          if (intent && intent.registered !== true) {
            intent.registered = true;
            intent.registrationRequested = false;
            changed = true;
          }

          const task = Cadenza.get(map.taskName) as any;
          if (task && (!Number.isFinite(map.taskVersion) || task.version === map.taskVersion)) {
            task.__registeredIntents = task.__registeredIntents ?? new Set<string>();
            if (!task.__registeredIntents.has(map.intentName)) {
              task.__registeredIntents.add(map.intentName);
              changed = true;
            }

            if (task.registered !== true) {
              task.registered = true;
              task.registrationRequested = false;
              changed = true;
            }
          }
        }

        for (const actorDefinition of normalizeSyncActors(ctx)) {
          if (actorDefinition.serviceName !== serviceName || !actorDefinition.name) {
            continue;
          }

          this.registeredActors.add(
            `${actorDefinition.name}|${actorDefinition.version}|${actorDefinition.serviceName}`,
          );
          changed = true;
        }

        for (const routineDefinition of normalizeSyncRoutines(ctx)) {
          if (routineDefinition.serviceName !== serviceName) {
            continue;
          }

          const routine = Cadenza.getRoutine(routineDefinition.name) as any;
          if (
            routine &&
            (!Number.isFinite(routineDefinition.version) ||
              routine.version === routineDefinition.version) &&
            routine.registered !== true
          ) {
            routine.registered = true;
            changed = true;
          }
        }

        for (const map of normalizeSyncDirectionalTaskMaps(ctx)) {
          if (
            map.serviceName !== serviceName ||
            map.predecessorServiceName !== serviceName
          ) {
            continue;
          }

          const predecessorTask = Cadenza.get(map.predecessorTaskName) as any;
          const nextTask = Cadenza.get(map.taskName) as any;
          if (
            predecessorTask &&
            nextTask &&
            predecessorTask.version === map.predecessorTaskVersion &&
            nextTask.version === map.taskVersion &&
            !predecessorTask.taskMapRegistration.has(map.taskName)
          ) {
            predecessorTask.taskMapRegistration.add(map.taskName);
            changed = true;
          }
        }

        for (const map of normalizeSyncActorTaskMaps(ctx)) {
          if (map.serviceName !== serviceName) {
            continue;
          }

          this.registeredActorTaskMaps.add(
            `${map.actorName}|${map.taskName}|${map.taskVersion}|${map.serviceName}`,
          );
          changed = true;
        }

        for (const map of normalizeSyncTaskToRoutineMaps(ctx)) {
          if (map.serviceName !== serviceName) {
            continue;
          }

          const routine = Cadenza.getRoutine(map.routineName) as any;
          const task = Cadenza.get(map.taskName) as any;
          if (
            routine &&
            task &&
            routine.version === map.routineVersion &&
            task.version === map.taskVersion &&
            !routine.registeredTasks.has(map.taskName)
          ) {
            routine.registeredTasks.add(map.taskName);
            changed = true;
          }
        }

        if (!changed) {
          return false;
        }

        scheduleSyncPassEvaluation();
        return ctx;
      },
      "Marks local static primitives and structural maps as registered when authority full sync already contains the local manifest snapshot.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn(
      "meta.service_registry.initial_sync_complete",
    );
    applyAuthoritativeLocalManifestRegistrationsTask.then(
      gatherTaskRegistrationTask,
      gatherSignalRegistrationTask,
      gatherIntentRegistrationTask,
      gatherActorRegistrationTask,
      gatherRoutineRegistrationTask,
    );

    this.registerIntentToTaskMapTask = Cadenza.createMetaTask(
      "Defer intent task maps to manifest sync",
      () => {
        scheduleSyncPassEvaluation();
        return false;
      },
      "Intent-task structural maps are derived from service manifests and authority full sync, not persisted incrementally on the hot registration path.",
      {
        register: false,
        isHidden: true,
      },
    );

    this.registerTaskMapTask = Cadenza.createMetaTask(
      "Register task map to DB",
      function* (ctx) {
        const task = ctx.task;
        if (task.hidden || !task.register || task.isDeputy || !task.registered) {
          return;
        }

        const predecessorServiceName = resolveSyncServiceName(task);
        if (!predecessorServiceName) {
          return;
        }
        for (const t of task.nextTasks) {
          if (
            task.taskMapRegistration.has(t.name) ||
            t.hidden ||
            !t.register ||
            !t.registered ||
            !shouldTrackDirectionalTaskMapForBootstrap(task, t as Task)
          ) {
            continue;
          }

          const serviceName = resolveSyncServiceName(t as any);
          if (!serviceName) {
            continue;
          }

          yield {
            data: {
              task_name: t.name,
              task_version: t.version,
              predecessor_task_name: task.name,
              predecessor_task_version: task.version,
              service_name: serviceName,
              predecessor_service_name: predecessorServiceName,
            },
            __taskName: task.name,
            __nextTaskName: t.name,
          };
        }
      },
    );
    const taskMapRegistrationGraph = resolveSyncInsertTask(
      this.isCadenzaDBReady,
      "directional_task_graph_map",
      {
        onConflict: {
          target: [
            "task_name",
            "predecessor_task_name",
            "task_version",
            "predecessor_task_version",
            "service_name",
            "predecessor_service_name",
          ],
          action: {
            do: "nothing",
          },
        },
      },
      { concurrency: 30 },
    );
    const recordTaskMapRegistrationTask = Cadenza.createMetaTask(
      "Record task map registration",
      (ctx) => {
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        scheduleSyncPassEvaluation();

        Cadenza.get(ctx.__taskName)?.taskMapRegistration.add(
          ctx.__nextTaskName,
        );
      },
    );
    wireSyncTaskGraph(
      this.registerTaskMapTask,
      taskMapRegistrationGraph,
      recordTaskMapRegistrationTask,
    );

    const hasPendingDirectionalTaskMaps = () =>
      getRegistrableTasks().some((task) => {
        if (task.isHidden || !task.register || !task.registered) {
          return false;
        }

        const predecessorServiceName = resolveSyncServiceName(task);
        if (!predecessorServiceName) {
          return false;
        }

        for (const nextTask of task.nextTasks) {
          if (
            task.taskMapRegistration.has(nextTask.name) ||
            nextTask.isHidden ||
            !nextTask.register ||
            !nextTask.registered ||
            !shouldTrackDirectionalTaskMapForBootstrap(task, nextTask as Task)
          ) {
            continue;
          }

          if (resolveSyncServiceName(nextTask)) {
            return true;
          }
        }

        return false;
      });

    const getPendingSignalTaskMapEntries = () =>
      getRegistrableTasks().flatMap((task) => {
        if (task.isHidden || !task.register || !task.registered) {
          return [];
        }

        const pendingSignals: Array<{
          taskName: string;
          taskVersion: number;
          signalName: string;
          registrationKey?: string;
          authoritativeKnown?: boolean;
        }> = [];

        for (const signal of task.observedSignals) {
          const signalName = canonicalizeSignalName(signal);
          if (!signalName) {
            continue;
          }

          if (task.registeredSignals.has(signalName)) {
            continue;
          }

          const registrationKey = buildSignalTaskMapRegistrationKey({
            signalName,
            serviceName: resolveSyncServiceName(task) ?? "",
            taskName: task.name,
            taskVersion: task.version,
          });
          if (this.authoritativeSignalTaskMaps.has(registrationKey)) {
            continue;
          }

          if (!decomposeSignalName(signalName).isGlobal) {
            continue;
          }

          if (
            !(Cadenza.signalBroker as any).signalObservers?.get(signalName)
              ?.registered
          ) {
            continue;
          }

          pendingSignals.push({
            taskName: task.name,
            taskVersion: task.version,
            signalName,
            registrationKey,
            authoritativeKnown: this.authoritativeSignalTaskMaps.has(
              registrationKey,
            ),
          });
        }

        return pendingSignals;
      });

    const hasPendingSignalTaskMaps = () =>
      getPendingSignalTaskMapEntries().length > 0;

    const hasPendingIntentTaskMaps = () =>
      getRegistrableTasks().some((task) => {
        if (task.isHidden || !task.register || !task.registered) {
          return false;
        }

        const registeredIntents =
          ((task as any).__registeredIntents as Set<string> | undefined) ??
          new Set<string>();

        for (const intent of task.handlesIntents) {
          if (registeredIntents.has(intent) || isLocalOnlySyncIntent(intent)) {
            continue;
          }

          if (isMetaIntentName(intent) && !task.isMeta) {
            continue;
          }

          const intentDefinition =
            buildIntentRegistryData(Cadenza.inquiryBroker.intents.get(intent)) ??
            buildIntentRegistryData({ name: intent });

          if (!intentDefinition) {
            continue;
          }

          return true;
        }

        return false;
      });

    const hasPendingActorTaskMaps = () =>
      getRegistrableTasks().some((task) => {
        if (task.isHidden || !task.register || !task.registered) {
          return false;
        }

        const metadata = getActorTaskRuntimeMetadata(task.taskFunction);
        if (!metadata?.actorName) {
          return false;
        }

        const serviceName = resolveSyncServiceName(task);
        if (!serviceName) {
          return false;
        }

        const registrationKey = `${metadata.actorName}|${task.name}|${task.version}|${serviceName}`;
        return !this.registeredActorTaskMaps.has(registrationKey);
      });

    const hasPendingRoutineTaskMaps = () =>
      getRegistrableRoutines().some((routine: any) => {
        if (!routine.registered) {
          return false;
        }

        for (const task of routine.tasks) {
          if (!task) {
            continue;
          }

          const tasks = task.getIterator();
          while (tasks.hasNext()) {
            const nextTask = tasks.next();
            if (!nextTask?.registered) {
              continue;
            }

            if (!routine.registeredTasks.has(nextTask.name)) {
              return true;
            }
          }
        }

        return false;
      });

    const gatherDirectionalTaskMapRegistrationTask = Cadenza.createUniqueMetaTask(
      "Gather directional task map registration",
      (ctx) => {
        scheduleSyncPassEvaluation();
        if (hasPendingDirectionalTaskMaps()) {
          this.directionalTaskMapsSynced = false;
          return false;
        }

        this.directionalTaskMapsSynced = true;
        return ctx;
      },
      "Completes directional task graph registration when task edges and deputy edges are registered.",
      {
        register: false,
        isHidden: true,
      },
    );
    const gatherSignalTaskMapRegistrationTask = Cadenza.createUniqueMetaTask(
      "Gather signal task map registration",
      (ctx) => {
        scheduleSyncPassEvaluation();
        if (hasPendingSignalTaskMaps()) {
          this.signalTaskMapsSynced = false;
          return false;
        }

        this.signalTaskMapsSynced = true;
        return ctx;
      },
      "Completes signal-to-task map registration when all global observed signal edges are registered.",
      {
        register: false,
        isHidden: true,
      },
    );
    const gatherIntentTaskMapRegistrationTask = Cadenza.createUniqueMetaTask(
      "Gather intent task map registration",
      (ctx) => {
        scheduleSyncPassEvaluation();
        if (hasPendingIntentTaskMaps()) {
          this.intentTaskMapsSynced = false;
          return false;
        }

        this.intentTaskMapsSynced = true;
        return ctx;
      },
      "Completes intent-to-task map registration when all task responders are registered.",
      {
        register: false,
        isHidden: true,
      },
    );
    const gatherActorTaskMapRegistrationTask = Cadenza.createUniqueMetaTask(
      "Gather actor task map registration",
      (ctx) => {
        scheduleSyncPassEvaluation();
        if (hasPendingActorTaskMaps()) {
          this.actorTaskMapsSynced = false;
          return false;
        }

        this.actorTaskMapsSynced = true;
        return ctx;
      },
      "Completes actor-to-task map registration when all actor-backed tasks are registered.",
      {
        register: false,
        isHidden: true,
      },
    );
    const gatherRoutineTaskMapRegistrationTask = Cadenza.createUniqueMetaTask(
      "Gather routine task map registration",
      (ctx) => {
        scheduleSyncPassEvaluation();
        if (hasPendingRoutineTaskMaps()) {
          this.routineTaskMapsSynced = false;
          return false;
        }

        this.routineTaskMapsSynced = true;
        return ctx;
      },
      "Completes task-to-routine map registration when all routine task memberships are registered.",
      {
        register: false,
        isHidden: true,
      },
    );
    applyAuthoritativeLocalManifestRegistrationsTask.then(
      gatherDirectionalTaskMapRegistrationTask,
      gatherSignalTaskMapRegistrationTask,
      gatherIntentTaskMapRegistrationTask,
      gatherActorTaskMapRegistrationTask,
      gatherRoutineTaskMapRegistrationTask,
    );

    const startDirectionalTaskMapSyncTask = Cadenza.createMetaTask(
      "Start directional task map sync",
      (ctx) => ctx,
      "Starts the directional task graph map branch for the current sync cycle.",
      {
        register: false,
        isHidden: true,
      },
    );
    const startSignalTaskMapSyncTask = Cadenza.createMetaTask(
      "Start signal task map sync",
      (ctx) => ctx,
      "Starts the signal-to-task map branch for the current sync cycle.",
      {
        register: false,
        isHidden: true,
      },
    );
    const startIntentTaskMapSyncTask = Cadenza.createMetaTask(
      "Start intent task map sync",
      (ctx) => ctx,
      "Starts the intent-to-task map branch for the current sync cycle.",
      {
        register: false,
        isHidden: true,
      },
    );
    const startActorTaskMapSyncTask = Cadenza.createMetaTask(
      "Start actor task map sync",
      (ctx) => ctx,
      "Starts the actor-to-task map branch for the current sync cycle.",
      {
        register: false,
        isHidden: true,
      },
    );
    const startRoutineTaskMapSyncTask = Cadenza.createMetaTask(
      "Start routine task map sync",
      (ctx) => ctx,
      "Starts the task-to-routine map branch for the current sync cycle.",
      {
        register: false,
        isHidden: true,
      },
    );
    const completeSyncCycleTask = Cadenza.createUniqueMetaTask(
      "Complete sync cycle",
      this.syncCycleCoordinatorActor.task(
        ({
          input,
          runtimeState,
          setRuntimeState,
        }: {
          input: Record<string, any>;
          runtimeState: SyncCycleRuntimeState | null | undefined;
          setRuntimeState: (nextState: SyncCycleRuntimeState | null) => void;
        }) => {
          const state: SyncCycleRuntimeState = runtimeState ?? {
            ...DEFAULT_SYNC_CYCLE_RUNTIME_STATE,
          };
          const syncCycleId =
            typeof input?.__syncCycleId === "string" ? input.__syncCycleId.trim() : "";
          if (!syncCycleId || state.activeSyncCycleId !== syncCycleId) {
            return false;
          }

          const finishedAt = Date.now();
          const cycleDurationMs = finishedAt - state.activeSyncCycleStartedAt;
          const serviceName =
            typeof input?.__serviceName === "string"
              ? input.__serviceName
              : resolveSyncServiceName();

          setRuntimeState({
            ...state,
            activeSyncCycleId: null,
            activeSyncCycleStartedAt: 0,
            phase: "idle",
          });

          if (shouldTraceSyncPhase(serviceName)) {
            console.log("[CADENZA_SYNC_PHASE_TRACE] sync_cycle_finished", {
              serviceName,
              syncCycleId,
              cycleDurationMs,
              converged: input?.__syncConverged === true,
              shouldRerun: input?.__syncShouldRerun === true,
            });
          }

          return {
            ...input,
            __syncCycleDurationMs: cycleDurationMs,
          };
        },
        { mode: "write" },
      ),
      "Clears the active bootstrap sync cycle once a pass outcome has been decided.",
      {
        register: false,
        isHidden: true,
      },
    );

    const requestNextSyncCycleTask = Cadenza.createUniqueMetaTask(
      "Request next sync cycle",
      () => {
        Cadenza.debounce("meta.sync_controller.sync_tick", {}, 100);
        return true;
      },
      "Requests the next sequential bootstrap sync pass.",
      {
        register: false,
        isHidden: true,
      },
    );

    const emitSyncCompleteTask = Cadenza.createUniqueMetaTask(
      "Emit sync complete",
      (_ctx, emit) => {
        emit("global.meta.sync_controller.synced", {
          data: {
            is_active: true,
            is_non_responsive: false,
            is_blocked: false,
            last_active: formatTimestamp(Date.now()),
          },
          filter: {
            uuid: Cadenza.serviceRegistry.serviceInstanceId,
          },
        });

        return true;
      },
      "Emits the final synced signal after bootstrap graph sync convergence.",
      {
        register: false,
        isHidden: true,
      },
    ).attachSignal("global.meta.sync_controller.synced");

    const routeConvergedSyncOutcomeTask = Cadenza.createMetaTask(
      "Route converged sync outcome",
      (ctx) => (ctx.__syncConverged === true ? ctx : false),
      "Continues only when the bootstrap sync cycle has converged.",
      {
        register: false,
        isHidden: true,
      },
    ).then(emitSyncCompleteTask);

    const routeRerunSyncOutcomeTask = Cadenza.createMetaTask(
      "Route rerun sync outcome",
      (ctx) => (ctx.__syncShouldRerun === true ? ctx : false),
      "Continues only when another bootstrap sync pass is required.",
      {
        register: false,
        isHidden: true,
      },
    ).then(requestNextSyncCycleTask);

    const advanceToMapPhaseTask = Cadenza.createMetaTask(
      "Advance to map sync phase",
      this.syncCycleCoordinatorActor.task(
        ({
          input,
          runtimeState,
          setRuntimeState,
        }: {
          input: Record<string, any>;
          runtimeState: SyncCycleRuntimeState | null | undefined;
          setRuntimeState: (nextState: SyncCycleRuntimeState | null) => void;
        }) => {
          const state: SyncCycleRuntimeState = runtimeState ?? {
            ...DEFAULT_SYNC_CYCLE_RUNTIME_STATE,
          };
          const syncCycleId =
            typeof input?.__syncCycleId === "string" ? input.__syncCycleId.trim() : "";
          if (!syncCycleId || state.activeSyncCycleId !== syncCycleId) {
            return false;
          }

          setRuntimeState({
            ...state,
            phase: "map",
          });

          return {
            ...input,
            __syncPhase: "map",
          };
        },
        { mode: "write" },
      ),
      "Advances the active bootstrap sync cycle into the map phase.",
      {
        register: false,
        isHidden: true,
      },
    ).then(
      startDirectionalTaskMapSyncTask,
      startSignalTaskMapSyncTask,
      startIntentTaskMapSyncTask,
      startActorTaskMapSyncTask,
      startRoutineTaskMapSyncTask,
    );

    const evaluateActiveSyncCycleTask = Cadenza.createUniqueMetaTask(
      "Evaluate active sync cycle",
      this.syncCycleCoordinatorActor.task(
        ({
          input,
          runtimeState,
        }: {
          input: Record<string, any>;
          runtimeState: SyncCycleRuntimeState | null | undefined;
        }) => {
          const state: SyncCycleRuntimeState = runtimeState ?? {
            ...DEFAULT_SYNC_CYCLE_RUNTIME_STATE,
          };
          if (!state.activeSyncCycleId || state.phase === "idle") {
            return false;
          }

          const serviceName = resolveSyncServiceName();
          if (!serviceName) {
            return false;
          }

          if (state.phase === "primitive") {
            const primitivePendingSummary = buildPrimitivePendingSummary();
            if (shouldTraceSyncPhase(serviceName)) {
              console.log("[CADENZA_SYNC_PHASE_TRACE] primitive_phase_evaluated", {
                serviceName,
                syncCycleId: state.activeSyncCycleId,
                primitivePendingSummary,
              });
            }

            return {
              ...input,
              __syncing: true,
              __syncCycleId: state.activeSyncCycleId,
              __serviceName: serviceName,
              __serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
              __primitivePendingSummary: primitivePendingSummary,
              __syncAdvanceToMap: !hasNonZeroPending(primitivePendingSummary),
              __syncShouldRerun: hasNonZeroPending(primitivePendingSummary),
              __syncConverged: false,
            };
          }

          const primitivePendingSummary =
            input?.__primitivePendingSummary &&
            typeof input.__primitivePendingSummary === "object"
              ? input.__primitivePendingSummary
              : buildPrimitivePendingSummary();
          const mapPendingSummary = buildMapPendingSummary();
          const converged =
            !hasNonZeroPending(primitivePendingSummary) &&
            !hasNonZeroPending(mapPendingSummary);

          if (shouldTraceSyncPhase(serviceName)) {
            console.log("[CADENZA_SYNC_PHASE_TRACE] map_phase_evaluated", {
              serviceName,
              syncCycleId: state.activeSyncCycleId,
              primitivePendingSummary,
              mapPendingSummary,
              pendingSignalTaskMapsSample: getPendingSignalTaskMapEntries().slice(
                0,
                5,
              ),
              converged,
            });
          }

          return {
            ...input,
            __syncing: true,
            __syncCycleId: state.activeSyncCycleId,
            __serviceName: serviceName,
            __serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
            __primitivePendingSummary: primitivePendingSummary,
            __mapPendingSummary: mapPendingSummary,
            __syncAdvanceToMap: false,
            __syncShouldRerun: !converged,
            __syncConverged: converged,
          };
        },
        { mode: "write" },
      ),
      "Evaluates the current bootstrap sync pass after resource activity settles.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn(SYNC_PASS_EVALUATION_SIGNAL);

    const routePrimitiveToMapPhaseTask = Cadenza.createMetaTask(
      "Route primitive pass to map phase",
      (ctx) => (ctx.__syncAdvanceToMap === true ? ctx : false),
      "Continues only when the primitive pass has converged and the map phase should begin.",
      {
        register: false,
        isHidden: true,
      },
    ).then(advanceToMapPhaseTask);

    const routePrimitiveToCycleCompletionTask = Cadenza.createMetaTask(
      "Route primitive pass to cycle completion",
      (ctx) =>
        ctx.__syncAdvanceToMap === true ? false : ctx,
      "Continues only when the current sync pass should complete without entering the map phase.",
      {
        register: false,
        isHidden: true,
      },
    ).then(completeSyncCycleTask);

    completeSyncCycleTask.then(
      routeConvergedSyncOutcomeTask,
      routeRerunSyncOutcomeTask,
    );

    evaluateActiveSyncCycleTask.then(
      routePrimitiveToMapPhaseTask,
      routePrimitiveToCycleCompletionTask,
    );

    const markLocalServiceInsertedForBootstrapSyncTask =
      Cadenza.createUniqueMetaTask(
        "Mark local service inserted for bootstrap sync",
        (ctx) => {
          const serviceName = resolveSyncServiceName();
          const insertedServiceName =
            typeof ctx.__serviceName === "string"
              ? ctx.__serviceName
              : typeof ctx.service_name === "string"
                ? ctx.service_name
                : null;

          if (!serviceName || insertedServiceName !== serviceName) {
            return false;
          }

          this.localServiceInserted = true;
          return true;
        },
        "Marks that the local service row has been inserted in authority so bootstrap sync can start only after that prerequisite.",
        {
          register: false,
          isHidden: true,
        },
      ).doOn("meta.service_registry.service_inserted");

    const markLocalServiceInstanceInsertedForBootstrapSyncTask =
      Cadenza.createUniqueMetaTask(
        "Mark local service instance inserted for bootstrap sync",
        (ctx) => {
          const serviceName = resolveSyncServiceName();
          const insertedServiceName =
            typeof ctx.__serviceName === "string"
              ? ctx.__serviceName
              : typeof ctx.service_name === "string"
                ? ctx.service_name
                : null;

          if (!serviceName || insertedServiceName !== serviceName) {
            return false;
          }

          this.localServiceInstanceInserted = true;
          return true;
        },
        "Marks that the local service instance row has been inserted in authority so bootstrap sync can start only after that prerequisite.",
        {
          register: false,
          isHidden: true,
        },
      ).doOn("meta.service_registry.instance_inserted");

    const startBootstrapSyncTask = Cadenza.createUniqueMetaTask(
      "Start bootstrap graph sync",
      this.syncCycleCoordinatorActor.task(
        ({
          input,
          runtimeState,
          setRuntimeState,
        }: {
          input: Record<string, any>;
          runtimeState: SyncCycleRuntimeState | null | undefined;
          setRuntimeState: (nextState: SyncCycleRuntimeState | null) => void;
        }) => {
          const ctx = input as Record<string, any>;
          const now = Date.now();
          const serviceName = resolveSyncServiceName();
          const serviceInstanceId = Cadenza.serviceRegistry.serviceInstanceId;
          const state: SyncCycleRuntimeState = runtimeState ?? {
            ...DEFAULT_SYNC_CYCLE_RUNTIME_STATE,
          };
          if (!serviceName || !serviceInstanceId) {
            return false;
          }

          if (!this.localServiceInserted || !this.localServiceInstanceInserted) {
            return false;
          }

          if (!ServiceRegistry.instance.hasLocalInstanceRegistered()) {
            return false;
          }

          if (ctx.__bootstrapFullSync === true) {
            if (shouldTraceSyncPhase(serviceName)) {
              console.log(
                "[CADENZA_SYNC_PHASE_TRACE] sync_cycle_ignored_bootstrap_full_sync",
                {
                  serviceName,
                  reason:
                    typeof ctx.__reason === "string" ? ctx.__reason : undefined,
                  attempt:
                    typeof ctx.__bootstrapFullSyncAttempt === "number"
                      ? ctx.__bootstrapFullSyncAttempt
                      : undefined,
                },
              );
            }
            return false;
          }

          if (state.activeSyncCycleId) {
            const activeCycleAgeMs = now - state.activeSyncCycleStartedAt;
            if (activeCycleAgeMs < BOOTSTRAP_SYNC_STALE_CYCLE_MS) {
              if (shouldTraceSyncPhase(serviceName)) {
                console.log("[CADENZA_SYNC_PHASE_TRACE] sync_cycle_deferred", {
                  serviceName,
                  activeSyncCycleId: state.activeSyncCycleId,
                  activeCycleAgeMs,
                  staleCycleMs: BOOTSTRAP_SYNC_STALE_CYCLE_MS,
                  triggerSignal:
                    typeof ctx.__signal === "string" ? ctx.__signal : undefined,
                  reason:
                    typeof ctx.__reason === "string" ? ctx.__reason : undefined,
                });
              }
              return false;
            }

            if (shouldTraceSyncPhase(serviceName)) {
              console.log("[CADENZA_SYNC_PHASE_TRACE] sync_cycle_stale_restart", {
                serviceName,
                previousSyncCycleId: state.activeSyncCycleId,
                activeCycleAgeMs,
                staleCycleMs: BOOTSTRAP_SYNC_STALE_CYCLE_MS,
                triggerSignal:
                  typeof ctx.__signal === "string" ? ctx.__signal : undefined,
                reason:
                  typeof ctx.__reason === "string" ? ctx.__reason : undefined,
              });
            }
          }

          const syncCycleId = `${now}-${uuid()}`;
          setRuntimeState({
            activeSyncCycleId: syncCycleId,
            activeSyncCycleStartedAt: now,
            phase: "primitive",
          });

          if (shouldTraceSyncPhase(serviceName)) {
            console.log("[CADENZA_SYNC_PHASE_TRACE] sync_cycle_started", {
              serviceName,
              syncCycleId,
              startedAt: now,
              triggerSignal:
                typeof ctx.__signal === "string" ? ctx.__signal : undefined,
              reason:
                typeof ctx.__reason === "string" ? ctx.__reason : undefined,
            });
          }

          return {
            ...ctx,
            __syncing: true,
            __syncCycleId: syncCycleId,
            __serviceName: serviceName,
            __serviceInstanceId: serviceInstanceId,
          };
        },
        { mode: "write" },
      ),
      "Starts a deterministic bootstrap sync cycle once the local service instance exists.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn(
      "meta.service_registry.service_inserted",
      "meta.service_registry.instance_inserted",
      "meta.sync_controller.sync_tick",
      "meta.sync_requested",
    );

    const startTaskPrimitiveSyncTask = Cadenza.createMetaTask(
      "Start task primitive sync",
      (ctx) => ctx,
      "Starts the task registration branch for the current sync cycle.",
      {
        register: false,
        isHidden: true,
      },
    );
    const startSignalPrimitiveSyncTask = Cadenza.createMetaTask(
      "Start signal primitive sync",
      (ctx) => ctx,
      "Starts the signal registration branch for the current sync cycle.",
      {
        register: false,
        isHidden: true,
      },
    );
    const startIntentPrimitiveSyncTask = Cadenza.createMetaTask(
      "Start intent primitive sync",
      (ctx) => ctx,
      "Starts the intent registration branch for the current sync cycle.",
      {
        register: false,
        isHidden: true,
      },
    );
    const startActorPrimitiveSyncTask = Cadenza.createMetaTask(
      "Start actor primitive sync",
      (ctx) => ctx,
      "Starts the actor registration branch for the current sync cycle.",
      {
        register: false,
        isHidden: true,
      },
    );
    const startRoutinePrimitiveSyncTask = Cadenza.createMetaTask(
      "Start routine primitive sync",
      (ctx) => ctx,
      "Starts the routine registration branch for the current sync cycle.",
      {
        register: false,
        isHidden: true,
      },
    );

    startBootstrapSyncTask.then(
      startTaskPrimitiveSyncTask,
      startSignalPrimitiveSyncTask,
      startIntentPrimitiveSyncTask,
      startActorPrimitiveSyncTask,
      startRoutinePrimitiveSyncTask,
    );

    const getAllTasksForSyncTask = Cadenza.createMetaTask(
      "Get all tasks for sync",
      (ctx) => ({
        ...ctx,
        tasks: Array.from(Cadenza.registry.tasks.values()),
      }),
      "Collects local tasks for the primitive sync phase.",
      {
        register: false,
        isHidden: true,
      },
    );
    startTaskPrimitiveSyncTask.then(
      getAllTasksForSyncTask,
      gatherTaskRegistrationTask,
    );
    getAllTasksForSyncTask.then(this.splitTasksForRegistration);

    const getSignalsForSyncTask = Cadenza.createMetaTask(
      "Get signals for sync",
      (ctx) => {
        const uniqueSignals = Array.from(
          new Set([
            ...Cadenza.signalBroker.signalObservers.keys(),
            ...Cadenza.signalBroker.emittedSignalsRegistry,
          ]),
        ).filter((signal) => !signal.includes(":"));

        const processedSignals = uniqueSignals.map((signal) => ({
          signal,
          data: {
            registered:
              Cadenza.signalBroker.signalObservers.get(signal)?.registered ??
              false,
            metadata: Cadenza.signalBroker.getSignalMetadata(signal) ?? null,
          },
        }));

        return {
          ...ctx,
          signals: processedSignals,
        };
      },
      "Collects local signals for the primitive sync phase.",
      {
        register: false,
        isHidden: true,
      },
    );
    startSignalPrimitiveSyncTask.then(
      getSignalsForSyncTask,
      gatherSignalRegistrationTask,
    );
    getSignalsForSyncTask.then(this.splitSignalsTask);

    const getAllIntentsForSyncTask = Cadenza.createUniqueMetaTask(
      "Get all intents for sync",
      (ctx) => ({
        ...ctx,
        intents: Array.from(Cadenza.inquiryBroker.intents.values()).filter(
          (intent) => isRegistrableLocalIntentDefinition(intent),
        ),
      }),
      "Collects local intents for the primitive sync phase.",
      {
        register: false,
        isHidden: true,
      },
    );
    startIntentPrimitiveSyncTask.then(
      getAllIntentsForSyncTask,
      gatherIntentRegistrationTask,
    );
    getAllIntentsForSyncTask.then(this.splitIntentsTask);

    const getAllActorsForSyncTask = Cadenza.createUniqueMetaTask(
      "Get all actors for sync",
      (ctx) => ({
        ...ctx,
        actors: Cadenza.getAllActors(),
      }),
      "Collects local actors for the primitive sync phase.",
      {
        register: false,
        isHidden: true,
      },
    );
    startActorPrimitiveSyncTask.then(
      getAllActorsForSyncTask,
      gatherActorRegistrationTask,
    );
    getAllActorsForSyncTask.then(this.splitActorsForRegistration);

    const getAllRoutinesForSyncTask = Cadenza.createMetaTask(
      "Get all routines for sync",
      (ctx) => ({
        ...ctx,
        routines: Array.from(Cadenza.registry.routines.values()),
      }),
      "Collects local routines for the primitive sync phase.",
      {
        register: false,
        isHidden: true,
      },
    );
    startRoutinePrimitiveSyncTask.then(
      getAllRoutinesForSyncTask,
      gatherRoutineRegistrationTask,
    );
    getAllRoutinesForSyncTask.then(this.splitRoutinesTask);

    const iterateTasksForDirectionalTaskMapSyncTask =
      Cadenza.createMetaTask(
        "Iterate tasks for directional task map sync",
        function* (ctx: AnyObject) {
          for (const task of Cadenza.registry.tasks.values()) {
            yield { ...ctx, task };
          }
        },
        "Iterates local tasks for directional task-map sync.",
        {
          register: false,
          isHidden: true,
        },
      );
    startDirectionalTaskMapSyncTask.then(
      iterateTasksForDirectionalTaskMapSyncTask,
      gatherDirectionalTaskMapRegistrationTask,
    );
    iterateTasksForDirectionalTaskMapSyncTask.then(this.registerTaskMapTask);
    recordTaskMapRegistrationTask.then(gatherDirectionalTaskMapRegistrationTask);

    const iterateTasksForSignalTaskMapSyncTask =
      Cadenza.createMetaTask(
        "Iterate tasks for signal task map sync",
        function* (ctx: AnyObject) {
          for (const task of Cadenza.registry.tasks.values()) {
            yield { ...ctx, task };
          }
        },
        "Iterates local tasks for signal-to-task map sync.",
        {
          register: false,
          isHidden: true,
        },
      );
    startSignalTaskMapSyncTask.then(
      iterateTasksForSignalTaskMapSyncTask,
      gatherSignalTaskMapRegistrationTask,
    );
    iterateTasksForSignalTaskMapSyncTask.then(this.registerSignalToTaskMapTask);
    this.registerSignalToTaskMapTask.then(gatherSignalTaskMapRegistrationTask);

    const iterateTasksForIntentTaskMapSyncTask =
      Cadenza.createMetaTask(
        "Iterate tasks for intent task map sync",
        function* (ctx: AnyObject) {
          for (const task of Cadenza.registry.tasks.values()) {
            yield { ...ctx, task };
          }
        },
        "Iterates local tasks for intent-to-task map sync.",
        {
          register: false,
          isHidden: true,
        },
      );
    startIntentTaskMapSyncTask.then(
      iterateTasksForIntentTaskMapSyncTask,
      gatherIntentTaskMapRegistrationTask,
    );
    iterateTasksForIntentTaskMapSyncTask.then(this.registerIntentToTaskMapTask);
    this.registerIntentToTaskMapTask.then(gatherIntentTaskMapRegistrationTask);

    const iterateTasksForActorTaskMapSyncTask =
      Cadenza.createMetaTask(
        "Iterate tasks for actor task map sync",
        function* (ctx: AnyObject) {
          for (const task of Cadenza.registry.tasks.values()) {
            yield { ...ctx, task };
          }
        },
        "Iterates local tasks for actor-to-task map sync.",
        {
          register: false,
          isHidden: true,
        },
      );
    startActorTaskMapSyncTask.then(
      iterateTasksForActorTaskMapSyncTask,
      gatherActorTaskMapRegistrationTask,
    );
    iterateTasksForActorTaskMapSyncTask.then(this.registerActorTaskMapTask);

    const getAllRoutinesForTaskMapSyncTask = Cadenza.createMetaTask(
      "Get all routines for task map sync",
      (ctx) => ({
        ...ctx,
        routines: Array.from(Cadenza.registry.routines.values()),
      }),
      "Collects local routines for routine-to-task map sync.",
      {
        register: false,
        isHidden: true,
      },
    );
    startRoutineTaskMapSyncTask.then(
      getAllRoutinesForTaskMapSyncTask,
      gatherRoutineTaskMapRegistrationTask,
    );
    getAllRoutinesForTaskMapSyncTask.then(this.splitTasksInRoutines);
    registerTaskToRoutineMapTask.then(gatherRoutineTaskMapRegistrationTask);

    Cadenza.createMetaTask(
      "Request sync after local service instance registration",
      (ctx) => {
        for (const delayMs of EARLY_SYNC_TICK_DELAYS_MS) {
          Cadenza.schedule(
            "meta.sync_controller.sync_tick",
            {
              ...buildMinimalSyncSignalContext(ctx),
              __syncing: true,
            },
            delayMs,
          );
        }

        return true;
      },
      "Schedules the early bootstrap sync burst after local instance registration so startup-defined primitives can converge deterministically.",
      {
        register: false,
        isHidden: true,
      },
    ).doOn("meta.service_registry.instance_inserted");

    Cadenza.interval(
      "meta.sync_controller.sync_tick",
      { __syncing: true },
      this.isCadenzaDBReady ? 180000 : 300000,
    );
  }
}
