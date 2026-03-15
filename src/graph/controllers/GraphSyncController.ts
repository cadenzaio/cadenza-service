import Cadenza from "../../Cadenza";
import { Task } from "@cadenza.io/core";
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

function resolveSyncInsertTask(
  isCadenzaDBReady: boolean,
  tableName: string,
  queryData: Record<string, unknown> = {},
  options: Record<string, unknown> = {},
): Task | undefined {
  return (
    Cadenza.getLocalCadenzaDBInsertTask(tableName) ??
    (isCadenzaDBReady
      ? Cadenza.createCadenzaDBInsertTask(tableName, queryData, options)
      : undefined)
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
  loggedAuthorityTaskIntentDiagnostics: Set<string> = new Set();

  isCadenzaDBReady: boolean = false;
  initialized: boolean = false;
  initRetryScheduled: boolean = false;
  initRetryTask: Task | undefined;
  loggedCadenzaDBIntentSweep: boolean = false;
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

    this.splitRoutinesTask = Cadenza.createMetaTask(
      "Split routines for registration",
      async function* (ctx, emit) {
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
          yield {
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
      },
    ).then(
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
          if (!ctx.__syncing) {
            return;
          }

          Cadenza.debounce("meta.sync_controller.synced_resource", {
            delayMs: 3000,
          });
          Cadenza.getRoutine(ctx.__routineName)!.registered = true;

          return true;
        }).then(
          Cadenza.createUniqueMetaTask(
            "Gather routine registration",
            () => true,
          ).emits("meta.sync_controller.synced_routines"),
        ),
      ),
    );

    this.splitTasksInRoutines = Cadenza.createMetaTask(
      "Split tasks in routines",
      function* (ctx) {
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
          for (const task of routine.tasks) {
            if (!task) {
              console.log("task is null", routine, task);
              continue;
            }

            if (routine.registeredTasks.has(task.name)) continue;

            const tasks = task.getIterator();

            while (tasks.hasNext()) {
              const nextTask = tasks.next();
              yield {
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
    ).then(
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
          if (!ctx.__syncing) {
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
      function* (ctx) {
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const { signals } = ctx;
        if (!signals) return;

        const filteredSignals = signals
          .filter(
            (signal: { signal: string; data: any }) => !signal.data.registered,
          )
          .map((signal: { signal: string; data: any }) => signal.signal);

        for (const signal of filteredSignals) {
          const { isMeta, isGlobal, domain, action } =
            decomposeSignalName(signal);

          yield {
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
      },
    ).then(
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
          if (!ctx.__syncing) {
            return;
          }

          Cadenza.debounce("meta.sync_controller.synced_resource", {
            delayMs: 3000,
          });

          return { signalName: ctx.__signal };
        }).then(
          Cadenza.signalBroker.registerSignalTask!,
          Cadenza.createUniqueMetaTask(
            "Gather signal registration",
            () => true,
          ).emits("meta.sync_controller.synced_signals"),
        ),
      ),
    );

    this.splitTasksForRegistration = Cadenza.createMetaTask(
      "Split tasks for registration",
      function* (ctx) {
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

          yield {
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
              // inputSchema: task.inputSchema,
              validateInputContext: task.validateInputContext,
              // outputSchema: task.outputSchema,
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
    ).then(
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
          if (!ctx.__syncing) {
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

          return true;
        }).then(
          Cadenza.createUniqueMetaTask(
            "Gather task registration",
            () => true,
          ).emits("meta.sync_controller.synced_tasks"),
        ),
      ),
    );

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
          if (!ctx.__syncing) {
            return;
          }

          Cadenza.debounce("meta.sync_controller.synced_resource", {
            delayMs: 3000,
          });
          this.registeredActors.add(ctx.__actorRegistrationKey);
          return true;
        }).then(
          Cadenza.createUniqueMetaTask(
            "Gather actor registration",
            () => true,
          ).emits("meta.sync_controller.synced_actors"),
        ),
      ),
    );

    this.registerActorTaskMapTask = Cadenza.createMetaTask(
      "Split actor task maps",
      function* (this: GraphSyncController, ctx: any) {
        const task = ctx.task;
        if (task.hidden || !task.register) {
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
          if (!ctx.__syncing) {
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
        if (!ctx.__syncing) {
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
      function* (ctx) {
        const task = ctx.task;
        if (task.hidden || !task.register) return;

        const serviceName = resolveSyncServiceName(task);
        if (!serviceName) {
          return;
        }

        for (const signal of task.observedSignals) {
          const _signal = signal.split(":")[0];
          if (task.registeredSignals.has(signal)) continue;

          const { isGlobal } = decomposeSignalName(_signal);

          yield {
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
        }
      },
    ).then(
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
      function* (this: GraphSyncController, ctx: any) {
        Cadenza.debounce("meta.sync_controller.synced_resource", {
          delayMs: 3000,
        });

        const intents = Array.isArray(ctx.intents)
          ? ctx.intents
          : Array.from(Cadenza.inquiryBroker.intents.values());

        if (
          resolveSyncServiceName() === "CadenzaDB" &&
          !this.loggedCadenzaDBIntentSweep
        ) {
          const intentNames = intents
            .map((intent: any) => String(intent?.name ?? "").trim())
            .filter(Boolean);
          const authorityIntentNames = intentNames.filter(
            (intentName: string) =>
              intentName === "meta-service-registry-full-sync" ||
              intentName.includes("service_instance") ||
              intentName.includes("service_instance_transport") ||
              intentName.includes("intent_to_task_map") ||
              intentName.includes("signal_to_task_map"),
          );

          Cadenza.log(
            "CadenzaDB intent sweep diagnostics.",
            {
              totalIntents: intentNames.length,
              hasMetaServiceRegistryFullSync: intentNames.includes(
                "meta-service-registry-full-sync",
              ),
              authorityIntentNames,
            },
            "info",
          );
          this.loggedCadenzaDBIntentSweep = true;
        }

        for (const intent of intents) {
          const intentData = buildIntentRegistryData(intent);
          if (!intentData) {
            continue;
          }

          if (this.registeredIntentDefinitions.has(intentData.name as string)) {
            continue;
          }

          yield {
            data: intentData,
            __intentName: intentData.name,
          };
        }
      }.bind(this),
    ).then(
      insertIntentRegistryTask?.then(
        Cadenza.createMetaTask("Record intent definition registration", (ctx) => {
          if (!ctx.__syncing) {
            return;
          }

          Cadenza.debounce("meta.sync_controller.synced_resource", {
            delayMs: 3000,
          });

          this.registeredIntentDefinitions.add(ctx.__intentName);

          return true;
        }).then(
          Cadenza.createUniqueMetaTask(
            "Gather intent registration",
            () => true,
          ).emits("meta.sync_controller.synced_intents"),
        ),
      ),
    );

    const registerIntentTask = Cadenza.createMetaTask(
      "Record intent registration",
      (ctx) => {
        if (!ctx.__syncing) {
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
      function* (this: GraphSyncController, ctx: any) {
        const task = ctx.task as any;
        if (task.hidden || !task.register) return;

        const serviceName = resolveSyncServiceName(task);
        if (!serviceName) {
          return;
        }

        task.__registeredIntents = task.__registeredIntents ?? new Set<string>();
        task.__invalidMetaIntentWarnings =
          task.__invalidMetaIntentWarnings ?? new Set<string>();

        if (
          serviceName === "CadenzaDB" &&
          [
            "Query service_instance",
            "Query service_instance_transport",
            "Query intent_to_task_map",
            "Query signal_to_task_map",
          ].includes(task.name)
        ) {
          const authorityTaskKey = `${task.name}:${task.version}`;
          if (!this.loggedAuthorityTaskIntentDiagnostics.has(authorityTaskKey)) {
            this.loggedAuthorityTaskIntentDiagnostics.add(authorityTaskKey);
            Cadenza.log(
              "CadenzaDB authority task intent diagnostics.",
              {
                taskName: task.name,
                taskVersion: task.version,
                isMeta: task.isMeta,
                handlesIntents: Array.from(task.handlesIntents ?? []),
                registeredIntents: Array.from(task.__registeredIntents ?? []),
              },
              "info",
            );
          }
        }

        for (const intent of task.handlesIntents as Set<string>) {
          if (task.__registeredIntents.has(intent)) continue;

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
        }
      }.bind(this),
    ).then(
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
        insertIntentRegistryTask?.then(
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
          if (task.taskMapRegistration.has(t.name) || t.hidden || !t.register) {
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
          if (!ctx.__syncing) {
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
            if (!ctx.__syncing) {
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
      .doOn("meta.sync_controller.synced_signals")
      .then(this.splitTasksForRegistration);

    Cadenza.createMetaTask("Get all intents", (ctx) => {
      return {
        ...ctx,
        intents: Array.from(Cadenza.inquiryBroker.intents.values()),
      };
    })
      .doOn("meta.sync_controller.synced_tasks")
      .then(this.splitIntentsTask);

    Cadenza.registry
      .getAllRoutines!.clone()
      .doOn("meta.sync_controller.synced_tasks")
      .then(this.splitRoutinesTask);

    Cadenza.createMetaTask("Get all actors", (ctx) => {
      return {
        ...ctx,
        actors: Cadenza.getAllActors(),
      };
    })
      .doOn("meta.sync_controller.synced_tasks")
      .then(this.splitActorsForRegistration);

    Cadenza.registry
      .doForEachTask!.clone()
      .doOn("meta.sync_controller.synced_tasks")
      .then(
        this.registerTaskMapTask,
        this.registerSignalToTaskMapTask,
        this.registerDeputyRelationshipTask,
      );

    Cadenza.registry
      .doForEachTask!.clone()
      .doOn("meta.sync_controller.synced_tasks", "meta.sync_controller.synced_intents")
      .then(this.registerIntentToTaskMapTask);

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
      .then(this.registerIntentToTaskMapTask);

    Cadenza.registry
      .doForEachTask!.clone()
      .doOn("meta.sync_controller.synced_tasks", "meta.sync_controller.synced_actors")
      .then(this.registerActorTaskMapTask);

    Cadenza.registry
      .getAllRoutines!.clone()
      .doOn("meta.sync_controller.synced_routines")
      .then(this.splitTasksInRoutines);

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
      Cadenza.schedule("meta.sync_requested", { __syncing: true }, 2000);
    }
  }
}
