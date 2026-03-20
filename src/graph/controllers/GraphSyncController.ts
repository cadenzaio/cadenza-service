import Cadenza from "../../Cadenza";
import {
  META_ACTOR_SESSION_STATE_PERSIST_INTENT,
  Task,
} from "@cadenza.io/core";
import ServiceRegistry from "../../registry/ServiceRegistry";
import { decomposeSignalName, formatTimestamp } from "../../utils/tools";
import { DeputyTask } from "../../index";
import { isMetaIntentName } from "../../utils/inquiry";

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

function isLocalOnlySyncIntent(intentName: string): boolean {
  return intentName === META_ACTOR_SESSION_STATE_PERSIST_INTENT;
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
    isMeta: isMetaIntentName(name),
  };
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

const REMOTE_AUTHORITY_SYNC_INSERT_CONCURRENCY = 5;
const REMOTE_AUTHORITY_SYNC_QUERY_CONCURRENCY = 3;

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

      if (
        (tableName === "signal_registry" ||
          tableName === "directional_task_graph_map") &&
        originalQueryData.data &&
        typeof originalQueryData.data === "object" &&
        !Array.isArray(originalQueryData.data) &&
        Object.keys(originalQueryData.data as Record<string, unknown>).length === 0
      ) {
        console.warn(
          "[CADENZA_SYNC_EMPTY_INSERT]",
          {
            tableName,
            queryData: originalQueryData,
            ctx,
            joinedContexts: Array.isArray((ctx as Record<string, any>).joinedContexts)
              ? (ctx as Record<string, any>).joinedContexts
              : [],
          },
        );
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
          Cadenza.debounce("meta.sync_requested", {
            delayMs: 1000,
          });
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
  "actor_task_map",
  "signal_to_task_map",
  "intent_to_task_map",
  "directional_task_graph_map",
] as const;

const BOOTSTRAP_SYNC_STALE_CYCLE_MS = 15000;
const EARLY_SYNC_TICK_DELAYS_MS = [
  400,
  BOOTSTRAP_SYNC_STALE_CYCLE_MS + 1000,
  BOOTSTRAP_SYNC_STALE_CYCLE_MS * 2 + 2000,
  BOOTSTRAP_SYNC_STALE_CYCLE_MS * 3 + 3000,
] as const;

function getRegistrableTasks(): Task[] {
  return Array.from(Cadenza.registry.tasks.values()).filter(
    (task) => task.register && !task.isHidden && !task.isDeputy,
  );
}

function getRegistrableRoutines() {
  return Array.from(Cadenza.registry.routines.values());
}

function isAuthoritySyncSignal(signalName: string): boolean {
  return decomposeSignalName(signalName).isGlobal;
}

function getRegistrableSignalObservers(): Array<{
  signalName: string;
  registered?: boolean;
}> {
  const signalObservers = (Cadenza.signalBroker as any)
    .signalObservers as Map<string, { registered?: boolean }> | undefined;
  if (!signalObservers) {
    return [];
  }

  return Array.from(signalObservers.entries())
    .filter(([signalName]) => isAuthoritySyncSignal(signalName))
    .map(([signalName, observer]) => ({
      signalName,
      ...observer,
    }));
}

function getRegistrableIntentNames(): string[] {
  return Array.from(Cadenza.inquiryBroker.intents.values())
    .map((intent) => buildIntentRegistryData(intent))
    .filter(
      (intentDefinition): intentDefinition is Record<string, unknown> =>
        intentDefinition !== null,
    )
    .map((intentDefinition) => String(intentDefinition.name));
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

function resolveLocalTaskFromSyncContext(ctx: Record<string, any>): Task | undefined {
  const taskName =
    typeof ctx.__taskName === "string" && ctx.__taskName.trim().length > 0
      ? ctx.__taskName
      : typeof ctx.data?.name === "string" && ctx.data.name.trim().length > 0
        ? ctx.data.name
        : undefined;

  return taskName ? Cadenza.get(taskName) : undefined;
}

function resolveLocalRoutineFromSyncContext(
  ctx: Record<string, any>,
): ReturnType<typeof Cadenza.getRoutine> | undefined {
  const routineName =
    typeof ctx.__routineName === "string" && ctx.__routineName.trim().length > 0
      ? ctx.__routineName
      : typeof ctx.data?.name === "string" && ctx.data.name.trim().length > 0
        ? ctx.data.name
        : undefined;

  return routineName ? Cadenza.getRoutine(routineName) : undefined;
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
  syncCycleCounter: number = 0;
  primitivePhaseCompletedCycles: Set<string> = new Set();
  mapPhaseCompletedCycles: Set<string> = new Set();
  activeSyncCycleId: string | null = null;
  activeSyncCycleStartedAt: number = 0;
  pendingBootstrapSyncRerun: boolean = false;
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
      const pendingTasks = getRegistrableTasks().filter((task) => !task.registered);
      const serviceName =
        typeof ctx.__serviceName === "string"
          ? ctx.__serviceName
          : resolveSyncServiceName();
      if (pendingTasks.length > 0) {
        this.tasksSynced = false;
        return false;
      }

      const shouldEmit = !this.tasksSynced;
      this.tasksSynced = true;
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
      if (pendingRoutines.length > 0) {
        this.routinesSynced = false;
        return false;
      }

      const shouldEmit = !this.routinesSynced;
      this.routinesSynced = true;
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
      if (pendingSignals.length > 0) {
        this.signalsSynced = false;
        return false;
      }

      const shouldEmit = !this.signalsSynced;
      this.signalsSynced = true;
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
      if (pendingIntentNames.length > 0) {
        this.intentsSynced = false;
        return false;
      }

      const shouldEmit = !this.intentsSynced;
      this.intentsSynced = true;
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
        .map((actor) => buildActorRegistrationKey(actor, syncServiceName))
        .filter((registrationKey): registrationKey is string => Boolean(registrationKey))
        .filter((registrationKey) => !this.registeredActors.has(registrationKey));

      if (pendingActorKeys.length > 0) {
        this.actorsSynced = false;
        return false;
      }

      const shouldEmit = !this.actorsSynced;
      this.actorsSynced = true;
      if (shouldEmit) {
        emit(
          "meta.sync_controller.synced_actors",
          buildMinimalSyncSignalContext(ctx),
        );
      }

      return true;
    };
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
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 2000,
        });

        for (const routine of routines) {
          if (routine.registered) continue;
          this.routinesSynced = false;
          yield {
            __syncing: ctx.__syncing,
            data: {
              name: routine.name,
              version: routine.version,
              description: routine.description,
              serviceName,
              isMeta: routine.isMeta,
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

      Cadenza.debounce("meta.sync_controller.synced_resource", {
        delayMs: 3000,
      });
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
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });
        for (const routine of routines) {
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
                  taskName: nextTask.name,
                  taskVersion: nextTask.version,
                  routineName: routine.name,
                  routineVersion: routine.version,
                  serviceName,
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

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 2000,
        });
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
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const { signals } = ctx;
        if (!signals) return;

        const filteredSignals = signals
          .filter((signal: { signal: string; data: any }) => {
            if (signal.data.registered) {
              return false;
            }

            return isAuthoritySyncSignal(signal.signal);
          })
          .map((signal: { signal: string; data: any }) => signal.signal);

        for (const signal of filteredSignals) {
          const { isMeta, isGlobal, domain, action } =
            decomposeSignalName(signal);
          this.signalsSynced = false;

          yield {
            __syncing: ctx.__syncing,
            data: {
              name: signal,
              isGlobal,
              domain,
              action,
              isMeta,
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

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

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

    this.splitTasksForRegistration = Cadenza.createMetaTask(
      "Split tasks for registration",
      function* (this: GraphSyncController, ctx: any) {
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

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

          yield {
            __syncing: ctx.__syncing,
            data: {
              name: task.name,
              version: task.version,
              description: task.description,
              functionString: __functionString,
              tagIdGetter: __getTagCallback,
              layerIndex: task.layerIndex,
              concurrency: task.concurrency,
              timeout: task.timeout,
              isUnique: task.isUnique,
              isSignal: task.isSignal,
              isThrottled: task.isThrottled,
              isDebounce: task.isDebounce,
              isEphemeral: task.isEphemeral,
              isMeta: task.isMeta,
              isSubMeta: task.isSubMeta,
              isHidden: task.isHidden,
              validateInputContext: task.validateInputContext,
              validateOutputContext: task.validateOutputContext,
              retryCount: task.retryCount,
              retryDelay: task.retryDelay,
              retryDelayMax: task.retryDelayMax,
              retryDelayFactor: task.retryDelayFactor,
              service_name: serviceName,
              signals: {
                emits: Array.from(task.emitsSignals),
                signalsToEmitAfter: Array.from(task.signalsToEmitAfter),
                signalsToEmitOnFail: Array.from(task.signalsToEmitOnFail),
                observed: Array.from(task.observedSignals),
              },
              intents: Array.from(task.handlesIntents),
            },
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

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        if (!task) {
          return true;
        }

        task.registered = true;
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
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return;
        }

        const actors = ctx.actors ?? [];
        for (const actor of actors) {
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

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });
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
      "Split actor task maps",
      function* (this: GraphSyncController, ctx: any) {
        const task = ctx.task;
        if (!this.tasksSynced || !this.actorsSynced) {
          return;
        }

        if (task.hidden || !task.register || !task.registered) {
          return;
        }

        const metadata = getActorTaskRuntimeMetadata(task.taskFunction);
        if (!metadata?.actorName) {
          return;
        }

        const serviceName = resolveSyncServiceName(task);
        if (!serviceName) {
          return;
        }

        const registrationKey = `${metadata.actorName}|${task.name}|${task.version}|${serviceName}`;
        if (this.registeredActorTaskMaps.has(registrationKey)) {
          return;
        }

        yield {
          data: {
            actor_name: metadata.actorName,
            actor_version: 1,
            task_name: task.name,
            task_version: task.version,
            service_name: serviceName,
            mode: metadata.mode,
            description: task.description ?? metadata.actorDescription ?? "",
            is_meta: metadata.actorKind === "meta" || task.isMeta === true,
          },
          __actorTaskMapRegistrationKey: registrationKey,
        };
      }.bind(this),
    );
    const actorTaskMapRegistrationGraph = resolveSyncInsertTask(
      this.isCadenzaDBReady,
      "actor_task_map",
      {
        onConflict: {
          target: [
            "actor_name",
            "actor_version",
            "task_name",
            "task_version",
            "service_name",
          ],
          action: {
            do: "nothing",
          },
        },
      },
      { concurrency: 30 },
    );
    const recordActorTaskMapRegistrationTask = Cadenza.createMetaTask(
      "Record actor task map registration",
      (ctx) => {
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });
        this.registeredActorTaskMaps.add(ctx.__actorTaskMapRegistrationKey);
      },
    );
    wireSyncTaskGraph(
      this.registerActorTaskMapTask,
      actorTaskMapRegistrationGraph,
      recordActorTaskMapRegistrationTask,
    );

    const registerSignalTask = Cadenza.createMetaTask(
      "Record signal registration",
      (ctx) => {
        const task = resolveLocalTaskFromSyncContext(ctx);
        const serviceName = resolveSyncServiceName(task);
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const signalName = resolveSignalNameFromSyncContext(ctx);
        if (!task || !signalName) {
          return true;
        }

        task.registeredSignals.add(signalName);
      },
    );

    this.registerSignalToTaskMapTask = Cadenza.createMetaTask(
      "Split observed signals of task",
      function* (ctx) {
        const task = ctx.task;
        if (task.hidden || !task.register || task.isDeputy || !task.registered)
          return false;

        const serviceName = resolveSyncServiceName(task);
        if (!serviceName) {
          return false;
        }

        let emittedCount = 0;
        for (const signal of task.observedSignals) {
          const _signal = signal.split(":")[0];
          if (task.registeredSignals.has(signal)) continue;
          if (
            !(Cadenza.signalBroker as any).signalObservers?.get(_signal)
              ?.registered
          ) {
            continue;
          }

          const { isGlobal } = decomposeSignalName(_signal);
          if (!isGlobal) {
            continue;
          }

          yield {
            __syncing: ctx.__syncing,
            data: {
              signalName: _signal,
              isGlobal,
              taskName: task.name,
              taskVersion: task.version,
              serviceName,
            },
            __taskName: task.name,
            __signal: signal,
          };
          emittedCount += 1;
        }
        return emittedCount > 0;
      },
    );
    const signalToTaskMapGraph = resolveSyncInsertTask(
      this.isCadenzaDBReady,
      "signal_to_task_map",
      {
        onConflict: {
          target: [
            "task_name",
            "task_version",
            "service_name",
            "signal_name",
          ],
          action: {
            do: "nothing",
          },
        },
      },
      { concurrency: 30 },
    );
    wireSyncTaskGraph(
      this.registerSignalToTaskMapTask,
      signalToTaskMapGraph,
      registerSignalTask,
    );

    this.splitIntentsTask = Cadenza.createMetaTask(
      "Split intents for registration",
      function* (this: GraphSyncController, ctx: any) {
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const intents = Array.isArray(ctx.intents)
          ? ctx.intents
          : Array.from(Cadenza.inquiryBroker.intents.values());

        for (const intent of intents) {
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

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        this.registeredIntentDefinitions.add(ctx.__intentName);

        emit(
          "meta.sync_controller.intent_registered",
          buildMinimalSyncSignalContext(ctx, {
            __intentName: ctx.__intentName,
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

    const registerIntentTask = Cadenza.createMetaTask(
      "Record intent registration",
      (ctx) => {
        const task = resolveLocalTaskFromSyncContext(ctx) as any;
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        if (!task) {
          return true;
        }
        task.__registeredIntents = task.__registeredIntents ?? new Set<string>();
        task.__registeredIntents.add(ctx.__intent);
      },
    );

    this.registerIntentToTaskMapTask = Cadenza.createMetaTask(
      "Split intents of task",
      function* (this: GraphSyncController, ctx: any) {
        const task = ctx.task as any;
        if (task.hidden || !task.register || task.isDeputy || !task.registered)
          return false;

        const serviceName = resolveSyncServiceName(task);
        if (!serviceName) {
          return false;
        }

        task.__registeredIntents = task.__registeredIntents ?? new Set<string>();
        task.__invalidMetaIntentWarnings =
          task.__invalidMetaIntentWarnings ?? new Set<string>();
        let emittedCount = 0;
        for (const intent of task.handlesIntents as Set<string>) {
          if (task.__registeredIntents.has(intent)) continue;

          if (isLocalOnlySyncIntent(intent)) {
            continue;
          }

          if (isMetaIntentName(intent) && !task.isMeta) {
            if (!task.__invalidMetaIntentWarnings.has(intent)) {
              task.__invalidMetaIntentWarnings.add(intent);
              Cadenza.log(
                "Skipping intent-to-task registration: non-meta task cannot handle meta intent.",
                {
                  intent,
                  taskName: task.name,
                  taskVersion: task.version,
                },
                "warning",
              );
            }
            continue;
          }

          const intentDefinition =
            buildIntentRegistryData(Cadenza.inquiryBroker.intents.get(intent)) ??
            buildIntentRegistryData({ name: intent });
          if (!intentDefinition) {
            continue;
          }

          yield {
            __syncing: ctx.__syncing,
            data: {
              intentName: intent,
              taskName: task.name,
              taskVersion: task.version,
              serviceName,
            },
            __taskName: task.name,
            __intent: intent,
            __intentDefinition: intentDefinition,
            __intentMapData: {
              intentName: intent,
              taskName: task.name,
              taskVersion: task.version,
              serviceName,
            },
          };
          emittedCount += 1;
        }

        return emittedCount > 0;
      }.bind(this),
    );
    const intentToTaskMapGraph = resolveSyncInsertTask(
      this.isCadenzaDBReady,
      "intent_to_task_map",
      {
        onConflict: {
          target: [
            "intent_name",
            "task_name",
            "task_version",
            "service_name",
          ],
          action: {
            do: "nothing",
          },
        },
      },
      { concurrency: 30 },
    );
    wireSyncTaskGraph(
      this.registerIntentToTaskMapTask,
      intentToTaskMapGraph,
      registerIntentTask,
    );

    this.registerTaskMapTask = Cadenza.createMetaTask(
      "Register task map to DB",
      function* (ctx) {
        const task = ctx.task;
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });
        if (task.hidden || !task.register || task.isDeputy) return;

        const predecessorServiceName = resolveSyncServiceName(task);
        if (!predecessorServiceName) {
          return;
        }
        for (const t of task.nextTasks) {
          if (
            task.taskMapRegistration.has(t.name) ||
            t.hidden ||
            !t.register ||
            !t.registered
          ) {
            continue;
          }

          const serviceName = resolveSyncServiceName(t as any);
          if (!serviceName) {
            continue;
          }

          yield {
            data: {
              taskName: t.name,
              taskVersion: t.version,
              predecessorTaskName: task.name,
              predecessorTaskVersion: task.version,
              serviceName,
              predecessorServiceName,
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

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

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

    this.registerDeputyRelationshipTask = Cadenza.createMetaTask(
      "Register deputy relationship",
      (ctx) => {
        const task = ctx.task;
        if (task.hidden || !task.register) return;

        if (task.isDeputy && !task.signalName) {
          if (task.registeredDeputyMap) return;

          const serviceName = resolveSyncServiceName(task);
          const predecessorServiceName = resolveSyncServiceName();
          if (!serviceName || !predecessorServiceName) {
            return;
          }

          return {
            data: {
              task_name: task.remoteRoutineName,
              task_version: 1,
              service_name: serviceName,
              predecessor_task_name: task.name,
              predecessor_task_version: task.version,
              predecessor_service_name: predecessorServiceName,
            },
            __taskName: task.name,
          };
        }
      },
    );
    const deputyRelationshipRegistrationGraph = resolveSyncInsertTask(
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
    const recordDeputyRelationshipRegistrationTask = Cadenza.createMetaTask(
      "Record deputy relationship registration",
      (ctx) => {
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const task = resolveLocalTaskFromSyncContext(ctx) as DeputyTask | undefined;
        if (!task) {
          return true;
        }
        task.registeredDeputyMap = true;
      },
    );
    wireSyncTaskGraph(
      this.registerDeputyRelationshipTask,
      deputyRelationshipRegistrationGraph,
      recordDeputyRelationshipRegistrationTask,
    );

    const hasPendingDirectionalTaskMaps = () =>
      getRegistrableTasks().some((task) => {
        const taskWithDeputyState = task as Task & {
          signalName?: string;
          registeredDeputyMap?: boolean;
        };
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
            !nextTask.registered
          ) {
            continue;
          }

          if (resolveSyncServiceName(nextTask)) {
            return true;
          }
        }

        if (
          task.isDeputy &&
          !taskWithDeputyState.signalName &&
          !taskWithDeputyState.registeredDeputyMap
        ) {
          return Boolean(resolveSyncServiceName(task) && resolveSyncServiceName());
        }

        return false;
      });

    const hasPendingSignalTaskMaps = () =>
      getRegistrableTasks().some((task) => {
        if (task.isHidden || !task.register || !task.registered) {
          return false;
        }

        for (const signal of task.observedSignals) {
          if (task.registeredSignals.has(signal)) {
            continue;
          }

          const signalName = signal.split(":")[0];
          if (!decomposeSignalName(signalName).isGlobal) {
            continue;
          }

          if (
            !(Cadenza.signalBroker as any).signalObservers?.get(signalName)
              ?.registered
          ) {
            continue;
          }

          return true;
        }

        return false;
      });

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

    const finishSyncTask = Cadenza.createUniqueMetaTask(
      "Finish sync",
      (ctx, emit) => {
        const syncCycleId =
          typeof ctx.__syncCycleId === "string" ? ctx.__syncCycleId.trim() : "";
        if (syncCycleId && this.activeSyncCycleId === syncCycleId) {
          this.activeSyncCycleId = null;
          this.activeSyncCycleStartedAt = 0;
        }

        if (this.pendingBootstrapSyncRerun) {
          this.pendingBootstrapSyncRerun = false;
          Cadenza.debounce("meta.sync_requested", {
            delayMs: 100,
          });
        }

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
      "Marks the current bootstrap sync cycle as complete.",
      {
        register: false,
        isHidden: true,
      },
    ).attachSignal("global.meta.sync_controller.synced");

    const mapPhaseBarrierTask = Cadenza.createUniqueMetaTask(
      "Complete map sync phase",
      (ctx) => {
        const syncCycleId =
          typeof ctx.__syncCycleId === "string" ? ctx.__syncCycleId.trim() : "";
        if (!syncCycleId) {
          return false;
        }

        if (
          !this.directionalTaskMapsSynced ||
          !this.signalTaskMapsSynced ||
          !this.intentTaskMapsSynced ||
          !this.actorTaskMapsSynced ||
          !this.routineTaskMapsSynced
        ) {
          return false;
        }

        if (!markCompletedSyncCycle(this.mapPhaseCompletedCycles, syncCycleId)) {
          return false;
        }

        return ctx;
      },
      "Fans in map branch completion and ends the current sync cycle once every map branch is complete.",
      {
        register: false,
        isHidden: true,
      },
    ).then(finishSyncTask);

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

    const primitivePhaseBarrierTask = Cadenza.createUniqueMetaTask(
      "Complete primitive sync phase",
      (ctx) => {
        const syncCycleId =
          typeof ctx.__syncCycleId === "string" ? ctx.__syncCycleId.trim() : "";
        if (!syncCycleId) {
          return false;
        }

        const serviceName =
          typeof ctx.__serviceName === "string"
            ? ctx.__serviceName
            : resolveSyncServiceName();
        if (
          !this.tasksSynced ||
          !this.signalsSynced ||
          !this.intentsSynced ||
          !this.actorsSynced ||
          !this.routinesSynced
        ) {
          return false;
        }

        if (
          !markCompletedSyncCycle(this.primitivePhaseCompletedCycles, syncCycleId)
        ) {
          return false;
        }

        this.directionalTaskMapsSynced = false;
        this.signalTaskMapsSynced = false;
        this.intentTaskMapsSynced = false;
        this.actorTaskMapsSynced = false;
        this.routineTaskMapsSynced = false;

        return ctx;
      },
      "Fans in primitive registration and opens the map registration phase once every primitive branch is complete.",
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

    gatherTaskRegistrationTask.then(primitivePhaseBarrierTask);
    gatherSignalRegistrationTask.then(primitivePhaseBarrierTask);
    gatherIntentRegistrationTask.then(primitivePhaseBarrierTask);
    gatherActorRegistrationTask.then(primitivePhaseBarrierTask);
    gatherRoutineRegistrationTask.then(primitivePhaseBarrierTask);

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
      (ctx) => {
        const now = Date.now();
        const serviceName = resolveSyncServiceName();
        const serviceInstanceId = Cadenza.serviceRegistry.serviceInstanceId;
        if (!serviceName || !serviceInstanceId) {
          return false;
        }

        if (!this.localServiceInserted) {
          return false;
        }

        if (!this.localServiceInstanceInserted) {
          return false;
        }

        if (!ServiceRegistry.instance.hasLocalInstanceRegistered()) {
          return false;
        }

        if (this.activeSyncCycleId) {
          const activeCycleAgeMs = now - this.activeSyncCycleStartedAt;
          if (activeCycleAgeMs < BOOTSTRAP_SYNC_STALE_CYCLE_MS) {
            this.pendingBootstrapSyncRerun = true;
            return false;
          }

        }

        const syncCycleId = `${now}-${++this.syncCycleCounter}`;
        this.activeSyncCycleId = syncCycleId;
        this.activeSyncCycleStartedAt = now;
        this.pendingBootstrapSyncRerun = false;
        this.tasksSynced = false;
        this.signalsSynced = false;
        this.intentsSynced = false;
        this.actorsSynced = false;
        this.routinesSynced = false;
        return {
          ...ctx,
          __syncing: true,
          __syncCycleId: syncCycleId,
          __serviceName: serviceName,
          __serviceInstanceId: serviceInstanceId,
        };
      },
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

    const getAllTasksForSyncTask = Cadenza.registry.getAllTasks!.clone();
    startTaskPrimitiveSyncTask.then(
      getAllTasksForSyncTask,
      gatherTaskRegistrationTask,
    );
    getAllTasksForSyncTask.then(this.splitTasksForRegistration);

    const getSignalsForSyncTask = Cadenza.signalBroker.getSignalsTask!.clone();
    startSignalPrimitiveSyncTask.then(
      getSignalsForSyncTask,
      gatherSignalRegistrationTask,
    );
    getSignalsForSyncTask.then(this.splitSignalsTask);

    const getAllIntentsForSyncTask = Cadenza.createUniqueMetaTask(
      "Get all intents for sync",
      (ctx) => ({
        ...ctx,
        intents: Array.from(Cadenza.inquiryBroker.intents.values()),
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

    const getAllRoutinesForSyncTask = Cadenza.registry.getAllRoutines!.clone();
    startRoutinePrimitiveSyncTask.then(
      getAllRoutinesForSyncTask,
      gatherRoutineRegistrationTask,
    );
    getAllRoutinesForSyncTask.then(this.splitRoutinesTask);

    const iterateTasksForDirectionalTaskMapSyncTask =
      Cadenza.registry.doForEachTask!.clone();
    startDirectionalTaskMapSyncTask.then(
      iterateTasksForDirectionalTaskMapSyncTask,
      gatherDirectionalTaskMapRegistrationTask,
    );
    iterateTasksForDirectionalTaskMapSyncTask.then(
      this.registerTaskMapTask,
      this.registerDeputyRelationshipTask,
    );
    recordTaskMapRegistrationTask.then(gatherDirectionalTaskMapRegistrationTask);
    recordDeputyRelationshipRegistrationTask.then(
      gatherDirectionalTaskMapRegistrationTask,
    );
    gatherDirectionalTaskMapRegistrationTask.then(mapPhaseBarrierTask);

    const iterateTasksForSignalTaskMapSyncTask =
      Cadenza.registry.doForEachTask!.clone();
    startSignalTaskMapSyncTask.then(
      iterateTasksForSignalTaskMapSyncTask,
      gatherSignalTaskMapRegistrationTask,
    );
    iterateTasksForSignalTaskMapSyncTask.then(this.registerSignalToTaskMapTask);
    registerSignalTask.then(gatherSignalTaskMapRegistrationTask);
    gatherSignalTaskMapRegistrationTask.then(mapPhaseBarrierTask);

    const iterateTasksForIntentTaskMapSyncTask =
      Cadenza.registry.doForEachTask!.clone();
    startIntentTaskMapSyncTask.then(
      iterateTasksForIntentTaskMapSyncTask,
      gatherIntentTaskMapRegistrationTask,
    );
    iterateTasksForIntentTaskMapSyncTask.then(this.registerIntentToTaskMapTask);
    registerIntentTask.then(gatherIntentTaskMapRegistrationTask);
    gatherIntentTaskMapRegistrationTask.then(mapPhaseBarrierTask);

    const iterateTasksForActorTaskMapSyncTask =
      Cadenza.registry.doForEachTask!.clone();
    startActorTaskMapSyncTask.then(
      iterateTasksForActorTaskMapSyncTask,
      gatherActorTaskMapRegistrationTask,
    );
    iterateTasksForActorTaskMapSyncTask.then(this.registerActorTaskMapTask);
    recordActorTaskMapRegistrationTask.then(gatherActorTaskMapRegistrationTask);
    gatherActorTaskMapRegistrationTask.then(mapPhaseBarrierTask);

    const getAllRoutinesForTaskMapSyncTask = Cadenza.registry.getAllRoutines!.clone();
    startRoutineTaskMapSyncTask.then(
      getAllRoutinesForTaskMapSyncTask,
      gatherRoutineTaskMapRegistrationTask,
    );
    getAllRoutinesForTaskMapSyncTask.then(this.splitTasksInRoutines);
    registerTaskToRoutineMapTask.then(gatherRoutineTaskMapRegistrationTask);
    gatherRoutineTaskMapRegistrationTask.then(mapPhaseBarrierTask);

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
