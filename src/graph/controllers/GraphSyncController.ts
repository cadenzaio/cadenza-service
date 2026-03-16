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

  if (
    !("data" in nextQueryData) &&
    resolvedData !== undefined
  ) {
    nextQueryData.data =
      resolvedData && typeof resolvedData === "object" && !Array.isArray(resolvedData)
        ? { ...resolvedData }
        : resolvedData;
  }

  if (
    !("batch" in nextQueryData) &&
    resolvedBatch !== undefined
  ) {
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

  if (!localInsertTask && !remoteInsertTask) {
    return undefined;
  }
  const targetTask = localInsertTask ?? remoteInsertTask!;

  const executionRequestedSignal = `meta.sync_controller.insert_execution_requested:${tableName}`;
  const executionResolvedSignal = `meta.sync_controller.insert_execution_resolved:${tableName}`;
  const executionFailedSignal = `meta.sync_controller.insert_execution_failed:${tableName}`;

  const prepareExecutionTask = Cadenza.createMetaTask(
    `Prepare graph sync insert execution for ${tableName}`,
    (ctx) => ({
      ...ctx,
      queryData: buildSyncInsertQueryData(
        ctx as Record<string, any>,
        queryData,
      ),
    }),
    `Prepares ${tableName} graph-sync insert payloads for runner execution.`,
    {
      register: false,
      isHidden: true,
    },
  )
    .doOn(executionRequestedSignal)
    .emitsOnFail(executionFailedSignal);

  const finalizeExecutionTask = Cadenza.createMetaTask(
    `Finalize graph sync insert execution for ${tableName}`,
    (ctx, emit) => {
      if (!ctx.__resolverRequestId) {
        return false;
      }

      emit(executionResolvedSignal, ctx);
      return ctx;
    },
    `Resolves signal-driven ${tableName} graph-sync insert execution.`,
    {
      register: false,
      isHidden: true,
    },
  );

  targetTask.then(finalizeExecutionTask).emitsOnFail(executionFailedSignal);
  prepareExecutionTask.then(targetTask);

  return Cadenza.createUniqueMetaTask(
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
            if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
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
      (ctx, emit) => {
        const { routines } = ctx;
        if (!routines) return false;
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return false;
        }
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });
        let emittedCount = 0;
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
              emit("meta.sync_controller.routine_task_map_split", {
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
              });
              emittedCount += 1;
            }
          }
        }
        return emittedCount > 0;
      },
    );

    Cadenza.createMetaTask("Process split routine task map", (ctx) => ctx)
      .doOn("meta.sync_controller.routine_task_map_split")
      .then(
        resolveSyncInsertTask(
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
            if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
              return;
            }

            Cadenza.debounce("meta.sync_controller.synced_resource", {
              delayMs: 2000,
            });
            Cadenza.getRoutine(ctx.__routineName)!.registeredTasks.add(
              ctx.__taskName,
            );
          }),
        ),
      );

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
            if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
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
      (ctx, emit) => {
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const tasks = ctx.tasks;
        const serviceName = resolveSyncServiceName();
        if (!serviceName) {
          return false;
        }
        let emittedCount = 0;
        for (const task of tasks) {
          if (task.registered) continue;
          const { __functionString, __getTagCallback } = task.export();

          emit("meta.sync_controller.task_registration_split", {
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
          });
          emittedCount += 1;
        }
        return emittedCount > 0;
      },
    );

    Cadenza.createMetaTask("Process split task registration", (ctx) => ctx)
      .doOn("meta.sync_controller.task_registration_split")
      .then(
        resolveSyncInsertTask(
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
            if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
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
        ),
      );

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
          if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
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
          if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
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
        if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
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
            if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
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
        if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
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
          if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
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
            if (!ctx.__syncing || !didSyncInsertSucceed(ctx)) {
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

    Cadenza.signalBroker
      .getSignalsTask!.clone()
      .doOn(
        "meta.sync_controller.sync_tick",
        "meta.service_registry.initial_sync_complete",
      )
      .then(this.splitSignalsTask);

    Cadenza.registry
      .getAllTasks!.clone()
      .doOn(
        "meta.sync_controller.sync_tick",
        "meta.sync_controller.synced_signals",
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
      )
      .then(this.splitIntentsTask);

    Cadenza.registry
      .getAllRoutines!.clone()
      .doOn(
        "meta.sync_controller.sync_tick",
        "meta.service_registry.initial_sync_complete",
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
        "meta.sync_controller.task_registered",
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
      Cadenza.schedule("meta.sync_requested", { __syncing: true }, 2000);
    }
  }
}
