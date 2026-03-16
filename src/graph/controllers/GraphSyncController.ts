import Cadenza from "../../Cadenza";
import {
  META_ACTOR_SESSION_STATE_PERSIST_INTENT,
  Task,
} from "@cadenza.io/core";
import { v4 as uuid } from "uuid";
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

function resolveSyncServiceName(task?: { serviceName?: string } | null):
  | string
  | undefined {
  const taskServiceName =
    typeof task?.serviceName === "string" ? task.serviceName.trim() : "";
  const registryServiceName =
    typeof Cadenza.serviceRegistry.serviceName === "string"
      ? Cadenza.serviceRegistry.serviceName.trim()
      : "";

  return taskServiceName || registryServiceName || undefined;
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

function buildSyncInsertQueryData(
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
  const nextQueryData: Record<string, unknown> = {
    ...existingQueryData,
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
  }

  if (resolvedBatch !== undefined) {
    nextQueryData.batch = Array.isArray(resolvedBatch)
      ? resolvedBatch.map((row: unknown) =>
          row && typeof row === "object"
            ? { ...(row as Record<string, unknown>) }
            : row,
        )
      : resolvedBatch;
  }

  return nextQueryData;
}

function resolveSyncInsertTask(
  isCadenzaDBReady: boolean,
  tableName: string,
  queryData: Record<string, unknown> = {},
  options: Record<string, unknown> = {},
): Task | undefined {
  const localInsertTask = Cadenza.getLocalCadenzaDBInsertTask(tableName);
  const remoteInsertTask = isCadenzaDBReady
    ? Cadenza.createCadenzaDBInsertTask(tableName, queryData, options)
    : undefined;
  const debugTable = shouldDebugSyncTable(tableName);

  if (!localInsertTask && !remoteInsertTask) {
    return undefined;
  }
  const targetTask = localInsertTask ?? remoteInsertTask!;

  if (debugTable) {
    logSyncDebug("insert_task_resolved", {
      tableName,
      localInsertTaskName: localInsertTask?.name ?? null,
      remoteInsertTaskName: remoteInsertTask?.name ?? null,
      targetTaskName: targetTask.name,
      queryData,
      options,
    });
  }

  const executionRequestedSignal = `meta.sync_controller.insert_execution_requested:${tableName}`;
  const executionResolvedSignal = `meta.sync_controller.insert_execution_resolved:${tableName}`;
  const executionFailedSignal = `meta.sync_controller.insert_execution_failed:${tableName}`;
  const pendingResolverContexts = new Map<
    string,
    {
      originalContext: Record<string, unknown>;
      originalQueryData?: Record<string, unknown>;
    }
  >();

  const prepareExecutionTask = Cadenza.createMetaTask(
    `Prepare graph sync insert execution for ${tableName}`,
    (ctx) => {
      const originalContext = {
        ...ctx,
      };
      const originalQueryData = buildSyncInsertQueryData(
        ctx as Record<string, any>,
        queryData,
      );

      if (typeof ctx.__resolverRequestId === "string") {
        pendingResolverContexts.set(ctx.__resolverRequestId, {
          originalContext,
          originalQueryData,
        });
      }

      return {
        ...ctx,
        __resolverOriginalContext: originalContext,
        __resolverQueryData: originalQueryData,
        queryData: originalQueryData,
      };
    },
    `Prepares ${tableName} graph-sync insert payloads for runner execution.`,
    {
      register: false,
      isHidden: true,
    },
  )
    .doOn(executionRequestedSignal)
    .emitsOnFail(executionFailedSignal);

  if (debugTable) {
    prepareExecutionTask.then(
      Cadenza.createMetaTask(
        `Log prepared graph sync insert execution for ${tableName}`,
        (ctx) => {
          logSyncDebug("insert_prepare", {
            tableName,
            targetTaskName: targetTask.name,
            ctx,
          });
          return ctx;
        },
        `Logs prepared ${tableName} sync insert payloads.`,
        {
          register: false,
          isHidden: true,
        },
      ),
    );
  }

  const finalizeExecutionTask = Cadenza.createMetaTask(
    `Finalize graph sync insert execution for ${tableName}`,
    (ctx, emit) => {
      if (!ctx.__resolverRequestId) {
        return false;
      }

      const pendingResolverContext = pendingResolverContexts.get(
        ctx.__resolverRequestId,
      );
      const originalContext =
        ctx.__resolverOriginalContext &&
        typeof ctx.__resolverOriginalContext === "object"
          ? (ctx.__resolverOriginalContext as Record<string, unknown>)
          : pendingResolverContext?.originalContext
            ? { ...pendingResolverContext.originalContext }
            : {};
      const originalQueryData =
        ctx.__resolverQueryData && typeof ctx.__resolverQueryData === "object"
          ? (ctx.__resolverQueryData as Record<string, unknown>)
          : pendingResolverContext?.originalQueryData;
      const normalizedContext = {
        ...originalContext,
        ...ctx,
        queryData:
          ctx.queryData && typeof ctx.queryData === "object"
            ? ctx.queryData
            : originalQueryData,
      };

      if (debugTable) {
        logSyncDebug("insert_finalize", {
          tableName,
          targetTaskName: targetTask.name,
          success: didSyncInsertSucceed(normalizedContext),
          ctx: normalizedContext,
        });
      }

      pendingResolverContexts.delete(ctx.__resolverRequestId);
      emit(executionResolvedSignal, normalizedContext);
      return normalizedContext;
    },
    `Resolves signal-driven ${tableName} graph-sync insert execution.`,
    {
      register: false,
      isHidden: true,
    },
  );

  targetTask.then(finalizeExecutionTask).emitsOnFail(executionFailedSignal);
  prepareExecutionTask.then(targetTask);

  if (debugTable) {
    Cadenza.createMetaTask(
      `Log failed graph sync insert execution for ${tableName}`,
      (ctx) => {
        logSyncDebug("insert_failed", {
          tableName,
          targetTaskName: targetTask.name,
          ctx,
        });
        if (typeof ctx.__resolverRequestId === "string") {
          pendingResolverContexts.delete(ctx.__resolverRequestId);
        }
        return false;
      },
      `Logs failed ${tableName} sync insert executions.`,
      {
        register: false,
        isHidden: true,
      },
    ).doOn(executionFailedSignal);
  }

  return Cadenza.createMetaTask(
    `Resolve graph sync insert for ${tableName}`,
    (ctx, emit) =>
      new Promise((resolve) => {
        const resolverRequestId = uuid();

        Cadenza.createEphemeralMetaTask(
          `Resolve graph sync insert execution for ${tableName} (${resolverRequestId})`,
          (resultCtx) => {
            if (resultCtx.__resolverRequestId !== resolverRequestId) {
              return false;
            }

            const normalizedResult = {
              ...resultCtx,
            };
            delete normalizedResult.__resolverRequestId;
            pendingResolverContexts.delete(resolverRequestId);

            resolve(normalizedResult);
            return normalizedResult;
          },
          `Waits for signal-driven ${tableName} graph-sync insert execution.`,
          {
            register: false,
          },
        ).doOn(executionResolvedSignal, executionFailedSignal);

        emit(executionRequestedSignal, {
          ...ctx,
          __resolverRequestId: resolverRequestId,
        });
      }),
    `Routes graph sync inserts for ${tableName} through the local authority task when available.`,
    {
      ...options,
      register: false,
      isHidden: true,
    },
  );
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

const AUTHORITY_QUERY_RESULT_KEYS = {
  task: "tasks",
  routine: "routines",
  signal_registry: "signalRegistrys",
  intent_registry: "intentRegistrys",
} as const;

const EARLY_SYNC_REQUEST_DELAYS_MS = [2000, 10000, 30000] as const;
const SYNC_DEBUG_PREFIX = "[CADENZA_SYNC_DEBUG]";
const SYNC_DEBUG_TABLES = new Set<string>([
  "task",
  "routine",
  "task_to_routine_map",
  "signal_registry",
  "intent_registry",
  "signal_to_task_map",
  "intent_to_task_map",
]);
const SYNC_DEBUG_TASK_NAMES = new Set<string>([
  "Query service_instance",
  "Query service_instance_transport",
  "Query intent_to_task_map",
  "Query signal_to_task_map",
  "Prepare for signal sync",
  "Compile sync data and broadcast",
  "Forward service instance sync",
  "Forward service transport sync",
  "Forward intent to task map sync",
  "Forward signal to task map sync",
]);
const SYNC_DEBUG_ROUTINE_NAMES = new Set<string>(["Sync services"]);
const SYNC_DEBUG_INTENT_NAMES = new Set<string>([
  "meta-service-registry-full-sync",
  "runner-traffic-runtime-get",
  "iot-telemetry-ingest",
  "query-pg-CadenzaDBPostgresActor-service_instance",
  "query-pg-CadenzaDBPostgresActor-service_instance_transport",
  "query-pg-CadenzaDBPostgresActor-intent_to_task_map",
  "query-pg-CadenzaDBPostgresActor-signal_to_task_map",
]);

function shouldDebugSyncTable(tableName: string): boolean {
  return SYNC_DEBUG_TABLES.has(tableName);
}

function shouldDebugSyncTaskName(taskName: unknown): boolean {
  return typeof taskName === "string" && SYNC_DEBUG_TASK_NAMES.has(taskName);
}

function shouldDebugSyncRoutineName(routineName: unknown): boolean {
  return (
    typeof routineName === "string" && SYNC_DEBUG_ROUTINE_NAMES.has(routineName)
  );
}

function shouldDebugSyncIntentName(intentName: unknown): boolean {
  return typeof intentName === "string" && SYNC_DEBUG_INTENT_NAMES.has(intentName);
}

function summarizeSyncDebugValue(value: unknown, depth: number = 0): unknown {
  if (value === null || value === undefined) {
    return value;
  }

  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }

  if (value instanceof Set) {
    return {
      __type: "Set",
      size: value.size,
      values: Array.from(value)
        .slice(0, 8)
        .map((item) => summarizeSyncDebugValue(item, depth + 1)),
    };
  }

  if (value instanceof Map) {
    return {
      __type: "Map",
      size: value.size,
    };
  }

  if (Array.isArray(value)) {
    return {
      __type: "Array",
      length: value.length,
      items: value
        .slice(0, 5)
        .map((item) => summarizeSyncDebugValue(item, depth + 1)),
    };
  }

  if (typeof value === "object") {
    if (depth >= 2) {
      return "[object]";
    }

    const output: Record<string, unknown> = {};
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([key]) =>
        ![
          "functionString",
          "tagIdGetter",
          "__functionString",
          "__getTagCallback",
          "joinedContexts",
          "task",
          "taskInstance",
          "tasks",
        ].includes(key),
      )
      .slice(0, 12);

    for (const [key, nestedValue] of entries) {
      output[key] = summarizeSyncDebugValue(nestedValue, depth + 1);
    }

    return output;
  }

  return String(value);
}

function logSyncDebug(event: string, payload: Record<string, unknown>): void {
  console.log(`${SYNC_DEBUG_PREFIX} ${event}`, summarizeSyncDebugValue(payload));
}

type AuthorityQueryTableName = keyof typeof AUTHORITY_QUERY_RESULT_KEYS;

function resolveSyncQueryRows<T extends Record<string, unknown>>(
  ctx: Record<string, any>,
  tableName: AuthorityQueryTableName,
): T[] {
  const resultKey = AUTHORITY_QUERY_RESULT_KEYS[tableName];
  const rows = ctx?.[resultKey];
  return Array.isArray(rows) ? (rows as T[]) : [];
}

function resolveSyncQueryTask(
  isCadenzaDBReady: boolean,
  tableName: AuthorityQueryTableName,
  queryData: Record<string, unknown> = {},
  options: Record<string, unknown> = {},
): Task | undefined {
  const localQueryTask = Cadenza.getLocalCadenzaDBQueryTask(tableName);
  const remoteQueryTask = isCadenzaDBReady
    ? Cadenza.createCadenzaDBQueryTask(tableName, queryData, options)
    : undefined;

  if (!localQueryTask && !remoteQueryTask) {
    return undefined;
  }

  const targetTask = localQueryTask ?? remoteQueryTask!;

  return Cadenza.createMetaTask(
    `Prepare graph sync query for ${tableName}`,
    (ctx) => ({
      ...ctx,
      queryData: {
        ...(ctx.queryData && typeof ctx.queryData === "object" ? ctx.queryData : {}),
        ...queryData,
      },
    }),
    `Prepares ${tableName} graph-sync query payloads.`,
    {
      register: false,
      isHidden: true,
    },
  ).then(targetTask);
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

  isCadenzaDBReady: boolean = false;
  initialized: boolean = false;
  initRetryScheduled: boolean = false;
  initRetryTask: Task | undefined;
  lastMissingLocalCadenzaDBInsertTablesKey: string = "";

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
    const ensureIntentRegistryBeforeIntentMapTask = resolveSyncInsertTask(
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
    const authoritativeTaskQueryTask = resolveSyncQueryTask(
      this.isCadenzaDBReady,
      "task",
      {},
      { concurrency: 10 },
    );
    const authoritativeRoutineQueryTask = resolveSyncQueryTask(
      this.isCadenzaDBReady,
      "routine",
      {},
      { concurrency: 10 },
    );
    const authoritativeSignalQueryTask = resolveSyncQueryTask(
      this.isCadenzaDBReady,
      "signal_registry",
      {},
      { concurrency: 10 },
    );
    const authoritativeIntentQueryTask = resolveSyncQueryTask(
      this.isCadenzaDBReady,
      "intent_registry",
      {},
      { concurrency: 10 },
    );

    this.splitRoutinesTask = Cadenza.createMetaTask(
      "Split routines for registration",
      (ctx, emit) => {
        const { routines } = ctx;
        if (!routines) return false;
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return false;
        }
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 2000,
        });

        let emittedCount = 0;
        for (const routine of routines) {
          if (routine.registered) continue;
          emit("meta.sync_controller.routine_registration_split", {
            __syncing: ctx.__syncing,
            data: {
              name: routine.name,
              version: routine.version,
              description: routine.description,
              serviceName,
              isMeta: routine.isMeta,
            },
            __routineName: routine.name,
          });
          emittedCount += 1;
        }
        return emittedCount > 0;
      },
    );

    Cadenza.createMetaTask("Process split routine registration", (ctx) => ctx)
      .doOn("meta.sync_controller.routine_registration_split")
      .then(
        resolveSyncInsertTask(
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
        )?.then(
          Cadenza.createMetaTask("Register routine", (ctx) => {
            if (!didSyncInsertSucceed(ctx)) {
              return;
            }

            Cadenza.debounce("meta.sync_controller.synced_resource", {
              delayMs: 3000,
            });
            Cadenza.getRoutine(ctx.__routineName)!.registered = true;
            Cadenza.debounce(
              "meta.sync_controller.routine_registration_settled",
              { __syncing: true },
              300,
            );

            return true;
          }),
        ),
      );

    Cadenza.createUniqueMetaTask(
      "Gather routine registration",
      () => {
        this.routinesSynced = true;
        return true;
      },
    )
      .doOn("meta.sync_controller.routine_registration_settled")
      .emits("meta.sync_controller.synced_routines");

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

              if (
                shouldDebugSyncRoutineName(routine.name) ||
                shouldDebugSyncTaskName(nextTask.name)
              ) {
                logSyncDebug("task_to_routine_split", {
                  routineName: routine.name,
                  routineVersion: routine.version,
                  taskName: nextTask.name,
                  taskVersion: nextTask.version,
                  serviceName,
                  registered: nextTask.registered,
                });
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

    const registerTaskToRoutineMapTask = resolveSyncInsertTask(
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
    )?.then(
      Cadenza.createMetaTask("Register routine task", (ctx) => {
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 2000,
        });
        Cadenza.getRoutine(ctx.__routineName)!.registeredTasks.add(
          ctx.__taskName,
        );
      }),
    );

    if (registerTaskToRoutineMapTask) {
      this.splitTasksInRoutines.then(registerTaskToRoutineMapTask);
    }

    this.splitSignalsTask = Cadenza.createMetaTask(
      "Split signals for registration",
      (ctx, emit) => {
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const { signals } = ctx;
        if (!signals) return false;

        const filteredSignals = signals
          .filter(
            (signal: { signal: string; data: any }) => !signal.data.registered,
          )
          .map((signal: { signal: string; data: any }) => signal.signal);

        for (const signal of filteredSignals) {
          const { isMeta, isGlobal, domain, action } =
            decomposeSignalName(signal);

          emit("meta.sync_controller.signal_registration_split", {
            __syncing: ctx.__syncing,
            data: {
              name: signal,
              isGlobal,
              domain,
              action,
              isMeta,
            },
            __signal: signal,
          });
        }
        return filteredSignals.length > 0;
      },
    );

    Cadenza.createMetaTask("Process split signal registration", (ctx) => ctx)
      .doOn("meta.sync_controller.signal_registration_split")
      .then(
        resolveSyncInsertTask(
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
        )?.then(
          Cadenza.createMetaTask("Process signal registration", (ctx) => {
            if (!didSyncInsertSucceed(ctx)) {
              return;
            }

            Cadenza.debounce("meta.sync_controller.synced_resource", {
              delayMs: 3000,
            });
            Cadenza.debounce(
              "meta.sync_controller.signal_registration_settled",
              { __syncing: true },
              300,
            );

            return { signalName: ctx.__signal };
          }).then(Cadenza.signalBroker.registerSignalTask!),
        ),
      );

    Cadenza.createUniqueMetaTask(
      "Gather signal registration",
      () => {
        this.signalsSynced = true;
        return true;
      },
    )
      .doOn("meta.sync_controller.signal_registration_settled")
      .emits("meta.sync_controller.synced_signals");

    this.splitTasksForRegistration = Cadenza.createMetaTask(
      "Split tasks for registration",
      function* (ctx: any) {
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const tasks = ctx.tasks;
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return;
        }

        for (const task of tasks) {
          if (task.registered) continue;
          const { __functionString, __getTagCallback } = task.export();

          if (shouldDebugSyncTaskName(task.name)) {
            logSyncDebug("task_registration_split", {
              taskName: task.name,
              taskVersion: task.version,
              serviceName,
              register: task.register,
              registered: task.registered,
              hidden: task.hidden,
              observedSignals: Array.from(task.observedSignals),
              handledIntents: Array.from(task.handlesIntents as Set<string>),
            });
          }

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
            },
            __taskName: task.name,
          };
        }
      },
    );

    const registerTaskTask = resolveSyncInsertTask(
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
    )?.then(
      Cadenza.createMetaTask("Record registration", (ctx, emit) => {
        if (shouldDebugSyncTaskName(ctx.__taskName)) {
          logSyncDebug("task_registration_result", {
            taskName: ctx.__taskName,
            success: didSyncInsertSucceed(ctx),
            ctx,
          });
        }

        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        Cadenza.get(ctx.__taskName)!.registered = true;
        emit("meta.sync_controller.task_registered", {
          ...ctx,
          task: Cadenza.get(ctx.__taskName),
        });
        Cadenza.debounce(
          "meta.sync_controller.task_registration_settled",
          { __syncing: true },
          300,
        );

        return true;
      }),
    );

    if (registerTaskTask) {
      this.splitTasksForRegistration.then(registerTaskTask);
    }

    Cadenza.createMetaTask(
      "Prepare created task for immediate sync",
      (ctx) => {
        const task =
          ctx.taskInstance ??
          (ctx.data?.name ? Cadenza.get(String(ctx.data.name)) : undefined);

        if (shouldDebugSyncTaskName(task?.name ?? ctx?.data?.name)) {
          logSyncDebug("task_created_for_immediate_sync", {
            incomingTaskName: ctx?.data?.name ?? null,
            resolvedTaskName: task?.name ?? null,
            exists: Boolean(task),
            hidden: task?.hidden ?? null,
            register: task?.register ?? null,
            registered: task?.registered ?? null,
          });
        }

        if (!task || task.hidden || !task.register || task.registered) {
          return false;
        }

        return {
          __syncing: true,
          tasks: [task],
        };
      },
      "Schedules newly created tasks into the graph sync registration flow without waiting for the next periodic tick.",
      {
        register: false,
        isHidden: true,
      },
    )
      .doOn("meta.task.created")
      .then(this.splitTasksForRegistration);

    Cadenza.createUniqueMetaTask(
      "Gather task registration",
      () => {
        this.tasksSynced = true;
        return true;
      },
    )
      .doOn("meta.sync_controller.task_registration_settled")
      .emits("meta.sync_controller.synced_tasks");

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

          yield {
            data,
            __actorRegistrationKey: registrationKey,
          };
        }
      }.bind(this),
    ).then(
      resolveSyncInsertTask(
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
      )?.then(
        Cadenza.createMetaTask("Record actor registration", (ctx) => {
          if (!didSyncInsertSucceed(ctx)) {
            return;
          }

          Cadenza.debounce("meta.sync_controller.synced_resource", {
            delayMs: 3000,
          });
          this.registeredActors.add(ctx.__actorRegistrationKey);
          Cadenza.debounce(
            "meta.sync_controller.actor_registration_settled",
            { __syncing: true },
            300,
          );
          return true;
        }),
      ),
    );

    Cadenza.createUniqueMetaTask(
      "Gather actor registration",
      () => {
        this.actorsSynced = true;
        return true;
      },
    )
      .doOn("meta.sync_controller.actor_registration_settled")
      .emits("meta.sync_controller.synced_actors");

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
    ).then(
      resolveSyncInsertTask(
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
      )?.then(
        Cadenza.createMetaTask("Record actor task map registration", (ctx) => {
          if (!didSyncInsertSucceed(ctx)) {
            return;
          }
          Cadenza.debounce("meta.sync_controller.synced_resource", {
            delayMs: 3000,
          });
          this.registeredActorTaskMaps.add(ctx.__actorTaskMapRegistrationKey);
        }),
      ),
    );

    const registerSignalTask = Cadenza.createMetaTask(
      "Record signal registration",
      (ctx) => {
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        Cadenza.get(ctx.__taskName)?.registeredSignals.add(ctx.__signal);
      },
    );

    this.registerSignalToTaskMapTask = Cadenza.createMetaTask(
      "Split observed signals of task",
      (ctx, emit) => {
        const task = ctx.task;
        if (task.hidden || !task.register || !task.registered) return false;

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

          if (shouldDebugSyncTaskName(task.name)) {
            logSyncDebug("signal_to_task_map_split", {
              taskName: task.name,
              signalName: _signal,
              rawSignal: signal,
              serviceName,
              observerRegistered: (Cadenza.signalBroker as any).signalObservers?.get(
                _signal,
              )?.registered,
            });
          }

          emit("meta.sync_controller.signal_task_map_split", {
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
          });
          emittedCount += 1;
        }
        return emittedCount > 0;
      },
    );

    Cadenza.createMetaTask("Process split signal-to-task map", (ctx) => ctx)
      .doOn("meta.sync_controller.signal_task_map_split")
      .then(
        resolveSyncInsertTask(
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
        )?.then(registerSignalTask),
      );

    this.splitIntentsTask = Cadenza.createMetaTask(
      "Split intents for registration",
      function (this: GraphSyncController, ctx: any, emit: any) {
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const intents = Array.isArray(ctx.intents)
          ? ctx.intents
          : Array.from(Cadenza.inquiryBroker.intents.values());

        let emittedCount = 0;
        for (const intent of intents) {
          const intentData = buildIntentRegistryData(intent);
          if (!intentData) {
            continue;
          }

          if (this.registeredIntentDefinitions.has(intentData.name as string)) {
            continue;
          }

          emit("meta.sync_controller.intent_registration_split", {
            __syncing: ctx.__syncing,
            data: intentData,
            __intentName: intentData.name,
          });
          emittedCount += 1;
        }
        return emittedCount > 0;
      }.bind(this),
    );

    Cadenza.createMetaTask("Process split intent registration", (ctx) => ctx)
      .doOn("meta.sync_controller.intent_registration_split")
      .then(
        insertIntentRegistryTask?.then(
          Cadenza.createMetaTask("Record intent definition registration", (ctx) => {
            if (!didSyncInsertSucceed(ctx)) {
              return;
            }

            Cadenza.debounce("meta.sync_controller.synced_resource", {
              delayMs: 3000,
            });

            this.registeredIntentDefinitions.add(ctx.__intentName);
            Cadenza.debounce(
              "meta.sync_controller.intent_registration_settled",
              { __syncing: true },
              300,
            );

            return true;
          }),
        ),
      );

    Cadenza.createUniqueMetaTask(
      "Gather intent registration",
      () => {
        this.intentsSynced = true;
        return true;
      },
    )
      .doOn("meta.sync_controller.intent_registration_settled")
      .emits("meta.sync_controller.synced_intents");

    const registerIntentTask = Cadenza.createMetaTask(
      "Record intent registration",
      (ctx) => {
        if (!didSyncInsertSucceed(ctx)) {
          return;
        }

        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const task = Cadenza.get(ctx.__taskName) as any;
        task.__registeredIntents = task.__registeredIntents ?? new Set<string>();
        task.__registeredIntents.add(ctx.__intent);
      },
    );

    this.registerIntentToTaskMapTask = Cadenza.createMetaTask(
      "Split intents of task",
      function (this: GraphSyncController, ctx: any, emit: any) {
        const task = ctx.task as any;
        if (task.hidden || !task.register || !task.registered) return false;

        const serviceName = resolveSyncServiceName(task);
        if (!serviceName) {
          return false;
        }

        task.__registeredIntents = task.__registeredIntents ?? new Set<string>();
        task.__invalidMetaIntentWarnings =
          task.__invalidMetaIntentWarnings ?? new Set<string>();

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

          if (
            shouldDebugSyncTaskName(task.name) ||
            shouldDebugSyncIntentName(intent)
          ) {
            logSyncDebug("intent_to_task_map_split", {
              taskName: task.name,
              taskVersion: task.version,
              intentName: intent,
              serviceName,
              intentDefinition,
            });
          }

          emit("meta.sync_controller.intent_task_map_split", {
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
          });
        }
        return true;
      }.bind(this),
    );

    Cadenza.createMetaTask("Process split intent-to-task map", (ctx) => ctx)
      .doOn("meta.sync_controller.intent_task_map_split")
      .then(
        Cadenza.createMetaTask(
          "Prepare intent definition for intent-to-task map",
          (ctx) => {
            if (!ctx.__intentDefinition || !ctx.__intentMapData) {
              return false;
            }

            if (
              shouldDebugSyncTaskName(ctx.__taskName) ||
              shouldDebugSyncIntentName(ctx.__intent)
            ) {
              logSyncDebug("intent_definition_prepare", {
                taskName: ctx.__taskName,
                intentName: ctx.__intent,
                intentDefinition: ctx.__intentDefinition,
              });
            }

            return {
              ...ctx,
              data: ctx.__intentDefinition,
            };
          },
        ).then(
          ensureIntentRegistryBeforeIntentMapTask?.then(
            Cadenza.createMetaTask(
              "Restore intent-to-task map payload",
              (ctx) => {
                if (!ctx.__intentMapData) {
                  return false;
                }

                if (
                  shouldDebugSyncTaskName(ctx.__taskName) ||
                  shouldDebugSyncIntentName(ctx.__intent)
                ) {
                  logSyncDebug("intent_map_payload_restore", {
                    taskName: ctx.__taskName,
                    intentName: ctx.__intent,
                    intentMapData: ctx.__intentMapData,
                  });
                }

                return {
                  ...ctx,
                  data: ctx.__intentMapData,
                };
              },
            ).then(
              resolveSyncInsertTask(
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
              )?.then(registerIntentTask),
            ),
          ),
        ),
      );

    this.registerTaskMapTask = Cadenza.createMetaTask(
      "Register task map to DB",
      function* (ctx) {
        const task = ctx.task;
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });
        if (task.hidden || !task.register) return;

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
    ).then(
      resolveSyncInsertTask(
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
      )?.then(
        Cadenza.createMetaTask("Record task map registration", (ctx) => {
          if (!didSyncInsertSucceed(ctx)) {
            return;
          }

          Cadenza.debounce("meta.sync_controller.synced_resource", {
            delayMs: 3000,
          });

          Cadenza.get(ctx.__taskName)?.taskMapRegistration.add(
            ctx.__nextTaskName,
          );
        }),
      ),
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
    ).then(
      resolveSyncInsertTask(
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
      )?.then(
        Cadenza.createMetaTask(
          "Record deputy relationship registration",
          (ctx) => {
            if (!didSyncInsertSucceed(ctx)) {
              return;
            }

            Cadenza.debounce("meta.sync_controller.synced_resource", {
              delayMs: 3000,
            });

            (Cadenza.get(ctx.__taskName) as DeputyTask).registeredDeputyMap =
              true;
          },
        ),
      ),
    );

    const reconcileTaskRegistrationFromAuthorityTask = Cadenza.createMetaTask(
      "Reconcile task registration from authority",
      (ctx, emit) => {
        const authoritativeTasks = resolveSyncQueryRows<{
          name?: string;
          version?: number;
          serviceName?: string;
        }>(ctx, "task");
        let changed = false;

        for (const row of authoritativeTasks) {
          const taskName = typeof row.name === "string" ? row.name : "";
          if (!taskName) {
            continue;
          }

          const task = Cadenza.get(taskName);
          if (!task || task.registered) {
            continue;
          }

          task.registered = true;
          changed = true;
          emit("meta.sync_controller.task_registered", {
            ...ctx,
            __taskName: task.name,
            task,
            __authoritativeReconciliation: true,
          });
        }

        if (authoritativeTasks.length > 0) {
          Cadenza.debounce(
            "meta.sync_controller.task_registration_settled",
            {
              __syncing: true,
              __authoritativeReconciliation: true,
            },
            300,
          );
        }

        return changed;
      },
      "Marks local tasks as registered when authority rows already exist.",
      {
        register: false,
        isHidden: true,
      },
    );

    const reconcileRoutineRegistrationFromAuthorityTask = Cadenza.createMetaTask(
      "Reconcile routine registration from authority",
      (ctx) => {
        const authoritativeRoutines = resolveSyncQueryRows<{
          name?: string;
          version?: number;
          serviceName?: string;
        }>(ctx, "routine");
        let changed = false;

        for (const row of authoritativeRoutines) {
          const routineName = typeof row.name === "string" ? row.name : "";
          if (!routineName) {
            continue;
          }

          const routine = Cadenza.getRoutine(routineName);
          if (!routine || routine.registered) {
            continue;
          }

          routine.registered = true;
          changed = true;
        }

        if (authoritativeRoutines.length > 0) {
          Cadenza.debounce(
            "meta.sync_controller.routine_registration_settled",
            {
              __syncing: true,
              __authoritativeReconciliation: true,
            },
            300,
          );
        }

        return changed;
      },
      "Marks local routines as registered when authority rows already exist.",
      {
        register: false,
        isHidden: true,
      },
    );

    const reconcileSignalRegistrationFromAuthorityTask = Cadenza.createMetaTask(
      "Reconcile signal registration from authority",
      (ctx) => {
        const authoritativeSignals = resolveSyncQueryRows<{
          name?: string;
        }>(ctx, "signal_registry");
        const signalObservers = (Cadenza.signalBroker as any).signalObservers;
        let changed = false;

        for (const row of authoritativeSignals) {
          const signalName = typeof row.name === "string" ? row.name : "";
          if (!signalName) {
            continue;
          }

          const observer = signalObservers?.get(signalName);
          if (!observer || observer.registered) {
            continue;
          }

          observer.registered = true;
          changed = true;
        }

        if (authoritativeSignals.length > 0) {
          Cadenza.debounce(
            "meta.sync_controller.signal_registration_settled",
            {
              __syncing: true,
              __authoritativeReconciliation: true,
            },
            300,
          );
        }

        return changed;
      },
      "Marks local signals as registered when authority rows already exist.",
      {
        register: false,
        isHidden: true,
      },
    );

    const reconcileIntentRegistrationFromAuthorityTask = Cadenza.createMetaTask(
      "Reconcile intent registration from authority",
      (ctx) => {
        const authoritativeIntents = resolveSyncQueryRows<{
          name?: string;
        }>(ctx, "intent_registry");
        let changed = false;

        for (const row of authoritativeIntents) {
          const intentName = typeof row.name === "string" ? row.name : "";
          if (!intentName || !Cadenza.inquiryBroker.intents.has(intentName)) {
            continue;
          }

          if (this.registeredIntentDefinitions.has(intentName)) {
            continue;
          }

          this.registeredIntentDefinitions.add(intentName);
          changed = true;
        }

        if (authoritativeIntents.length > 0) {
          Cadenza.debounce(
            "meta.sync_controller.intent_registration_settled",
            {
              __syncing: true,
              __authoritativeReconciliation: true,
            },
            300,
          );
        }

        return changed;
      },
      "Marks local intents as registered when authority rows already exist.",
      {
        register: false,
        isHidden: true,
      },
    );

    const authoritativeTaskReconciliationGraph =
      authoritativeTaskQueryTask?.then(reconcileTaskRegistrationFromAuthorityTask) ??
      Cadenza.createMetaTask(
        "Skip authoritative task reconciliation",
        () => false,
        "Skips task reconciliation when no authority query task is available.",
        {
          register: false,
          isHidden: true,
        },
      );
    const authoritativeRoutineReconciliationGraph =
      authoritativeRoutineQueryTask?.then(reconcileRoutineRegistrationFromAuthorityTask) ??
      Cadenza.createMetaTask(
        "Skip authoritative routine reconciliation",
        () => false,
        "Skips routine reconciliation when no authority query task is available.",
        {
          register: false,
          isHidden: true,
        },
      );
    const authoritativeSignalReconciliationGraph =
      authoritativeSignalQueryTask?.then(reconcileSignalRegistrationFromAuthorityTask) ??
      Cadenza.createMetaTask(
        "Skip authoritative signal reconciliation",
        () => false,
        "Skips signal reconciliation when no authority query task is available.",
        {
          register: false,
          isHidden: true,
        },
      );
    const authoritativeIntentReconciliationGraph =
      authoritativeIntentQueryTask?.then(reconcileIntentRegistrationFromAuthorityTask) ??
      Cadenza.createMetaTask(
        "Skip authoritative intent reconciliation",
        () => false,
        "Skips intent reconciliation when no authority query task is available.",
        {
          register: false,
          isHidden: true,
        },
      );
    const authoritativeRegistrationTriggers = [
      "meta.service_registry.initial_sync_complete",
      "meta.sync_requested",
      "meta.sync_controller.synced_resource",
      "meta.sync_controller.authority_registration_reconciliation_requested",
    ] as const;

    Cadenza.createMetaTask(
      "Prepare authoritative task registration query",
      (ctx) => {
        if (!this.isCadenzaDBReady) {
          return false;
        }

        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return false;
        }

        return {
          ...ctx,
          __syncServiceName: serviceName,
          queryData: {
            filter: {
              service_name: serviceName,
            },
            fields: ["name", "version", "service_name"],
          },
        };
      },
      "Builds the authority task query payload for the current service.",
      {
        register: false,
        isHidden: true,
      },
    )
      .doOn(...authoritativeRegistrationTriggers)
      .then(authoritativeTaskReconciliationGraph);

    Cadenza.createMetaTask(
      "Prepare authoritative routine registration query",
      (ctx) => {
        if (!this.isCadenzaDBReady) {
          return false;
        }

        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return false;
        }

        return {
          ...ctx,
          __syncServiceName: serviceName,
          queryData: {
            filter: {
              service_name: serviceName,
            },
            fields: ["name", "version", "service_name"],
          },
        };
      },
      "Builds the authority routine query payload for the current service.",
      {
        register: false,
        isHidden: true,
      },
    )
      .doOn(...authoritativeRegistrationTriggers)
      .then(authoritativeRoutineReconciliationGraph);

    Cadenza.createMetaTask(
      "Prepare authoritative signal registration query",
      (ctx) => {
        if (!this.isCadenzaDBReady) {
          return false;
        }

        return {
          ...ctx,
          queryData: {
            fields: ["name"],
          },
        };
      },
      "Builds the authority signal query payload for local reconciliation.",
      {
        register: false,
        isHidden: true,
      },
    )
      .doOn(...authoritativeRegistrationTriggers)
      .then(authoritativeSignalReconciliationGraph);

    Cadenza.createMetaTask(
      "Prepare authoritative intent registration query",
      (ctx) => {
        if (!this.isCadenzaDBReady) {
          return false;
        }

        return {
          ...ctx,
          queryData: {
            fields: ["name"],
          },
        };
      },
      "Builds the authority intent query payload for local reconciliation.",
      {
        register: false,
        isHidden: true,
      },
    )
      .doOn(...authoritativeRegistrationTriggers)
      .then(authoritativeIntentReconciliationGraph);

    Cadenza.signalBroker
      .getSignalsTask!.clone()
      .doOn(
        "meta.sync_controller.sync_tick",
        "meta.service_registry.initial_sync_complete",
        "meta.sync_requested",
      )
      .then(this.splitSignalsTask);

    Cadenza.registry
      .getAllTasks!.clone()
      .doOn(
        "meta.sync_controller.sync_tick",
        "meta.sync_controller.synced_signals",
        "meta.sync_requested",
      )
      .then(this.splitTasksForRegistration);

    Cadenza.createMetaTask("Get all intents", (ctx) => {
      return {
        ...ctx,
        intents: Array.from(Cadenza.inquiryBroker.intents.values()),
      };
    })
      .doOn(
        "meta.sync_controller.sync_tick",
        "meta.service_registry.initial_sync_complete",
        "meta.sync_requested",
      )
      .then(this.splitIntentsTask);

    Cadenza.registry
      .getAllRoutines!.clone()
      .doOn(
        "meta.sync_controller.sync_tick",
        "meta.service_registry.initial_sync_complete",
        "meta.sync_requested",
      )
      .then(this.splitRoutinesTask);

    Cadenza.createMetaTask("Get all actors", (ctx) => {
      return {
        ...ctx,
        actors: Cadenza.getAllActors(),
      };
    })
      .doOn(
        "meta.sync_controller.sync_tick",
        "meta.service_registry.initial_sync_complete",
        "meta.sync_requested",
      )
      .then(this.splitActorsForRegistration);

    Cadenza.createMetaTask("Get registered task for task graph sync", (ctx) => {
      const task = ctx.task ?? (ctx.__taskName ? Cadenza.get(ctx.__taskName) : undefined);
      if (!task) {
        return false;
      }

      return {
        ...ctx,
        task,
      };
    })
      .doOn("meta.sync_controller.task_registered")
      .then(
        this.registerTaskMapTask,
        this.registerDeputyRelationshipTask,
      );

    Cadenza.registry
      .doForEachTask!.clone()
      .doOn(
        "meta.sync_controller.synced_signals",
        "meta.sync_controller.synced_tasks",
        "meta.sync_requested",
      )
      .then(
        Cadenza.createMetaTask(
          "Ensure signal and task sync ready",
          (ctx) => {
            if (!this.tasksSynced || !this.signalsSynced) {
              return false;
            }

            return ctx;
          },
        ).then(this.registerSignalToTaskMapTask),
      );

    Cadenza.createMetaTask("Get registered task for signal sync", (ctx) => {
      const task = ctx.task ?? (ctx.__taskName ? Cadenza.get(ctx.__taskName) : undefined);
      if (!task) {
        return false;
      }

      return {
        ...ctx,
        task,
      };
    })
      .doOn("meta.sync_controller.task_registered")
      .then(
        Cadenza.createMetaTask(
          "Ensure signal and task sync ready from task registration",
          (ctx) => {
            if (!this.tasksSynced || !this.signalsSynced) {
              return false;
            }

            return ctx;
          },
        ).then(this.registerSignalToTaskMapTask),
      );

    Cadenza.registry
      .doForEachTask!.clone()
      .doOn(
        "meta.sync_controller.synced_intents",
        "meta.sync_controller.synced_tasks",
        "meta.sync_requested",
      )
      .then(
        Cadenza.createMetaTask(
          "Ensure intent and task sync ready",
          (ctx) => {
            if (!this.tasksSynced || !this.intentsSynced) {
              return false;
            }

            return ctx;
          },
        ).then(this.registerIntentToTaskMapTask),
      );

    Cadenza.createMetaTask("Get registered task for intent sync", (ctx) => {
      const task = ctx.task ?? (ctx.__taskName ? Cadenza.get(ctx.__taskName) : undefined);
      if (!task) {
        return false;
      }

      return {
        ...ctx,
        task,
      };
    })
      .doOn("meta.sync_controller.task_registered")
      .then(
        Cadenza.createMetaTask(
          "Ensure intent and task sync ready from task registration",
          (ctx) => {
            if (!this.tasksSynced || !this.intentsSynced) {
              return false;
            }

            return ctx;
          },
        ).then(this.registerIntentToTaskMapTask),
      );

    Cadenza.registry
      .doForEachTask!.clone()
      .doOn(
        "meta.sync_controller.synced_actors",
        "meta.sync_controller.synced_tasks",
        "meta.sync_requested",
      )
      .then(this.registerActorTaskMapTask);

    Cadenza.createMetaTask("Get registered task for actor sync", (ctx) => {
      const task = ctx.task ?? (ctx.__taskName ? Cadenza.get(ctx.__taskName) : undefined);
      if (!task) {
        return false;
      }

      return {
        ...ctx,
        task,
      };
    })
      .doOn("meta.sync_controller.task_registered")
      .then(
        Cadenza.createMetaTask(
          "Ensure actor and task sync ready from task registration",
          (ctx) => {
            if (!this.tasksSynced || !this.actorsSynced) {
              return false;
            }

            return ctx;
          },
        ).then(this.registerActorTaskMapTask),
      );

    Cadenza.registry
      .getAllRoutines!.clone()
      .doOn(
        "meta.sync_controller.synced_routines",
        "meta.sync_controller.synced_tasks",
        "meta.sync_requested",
      )
      .then(
        Cadenza.createMetaTask(
          "Ensure routine and task sync ready",
          (ctx) => {
            if (!this.tasksSynced || !this.routinesSynced) {
              return false;
            }

            return ctx;
          },
        ).then(this.splitTasksInRoutines),
      );

    Cadenza.createMetaTask("Finish sync", (ctx, emit) => {
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

      Cadenza.log("Synced resources...");
    })
      .attachSignal("global.meta.sync_controller.synced")
      .doOn("meta.sync_controller.synced_resource");

    if (!this.isCadenzaDBReady) {
      Cadenza.interval(
        "meta.sync_controller.sync_tick",
        { __syncing: true },
        300000,
        true,
      );
    } else {
      Cadenza.interval(
        "meta.sync_controller.sync_tick",
        { __syncing: true },
        180000,
      );
      Cadenza.schedule(
        "meta.sync_controller.sync_tick",
        { __syncing: true },
        250,
      );
      for (const delayMs of EARLY_SYNC_REQUEST_DELAYS_MS) {
        Cadenza.schedule("meta.sync_requested", { __syncing: true }, delayMs);
      }
    }
  }
}
