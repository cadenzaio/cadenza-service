import Cadenza from "../../Cadenza";
import { Task } from "@cadenza.io/core";
import { decomposeSignalName, formatTimestamp } from "../../utils/tools";
import { DeputyTask } from "../../index";

export default class GraphSyncController {
  private static _instance: GraphSyncController;
  public static get instance(): GraphSyncController {
    if (!this._instance) this._instance = new GraphSyncController();
    return this._instance;
  }

  splitSignalsTask: Task | undefined;
  splitTasksForRegistration: Task | undefined;
  registerSignalToTaskMapTask: Task | undefined;
  registerTaskMapTask: Task | undefined;
  registerDeputyRelationshipTask: Task | undefined;
  splitRoutinesTask: Task | undefined;

  isCadenzaDBReady: boolean = false;

  init() {
    this.splitRoutinesTask = Cadenza.createMetaTask(
      "Split routines for registration",
      function* (ctx, emit) {
        console.log("SPLITTING ROUTINES FOR REGISTRATION");
        const { routines } = ctx;
        if (!routines) return;
        for (const routine of routines) {
          if (routine.registered) continue;
          console.log("REGISTERING ROUTINE", routine.name);
          routine.registered = true;
          emit("global.meta.sync_controller.routine_added", {
            data: {
              name: routine.name,
              version: routine.version,
              description: routine.description,
              serviceName: Cadenza.serviceRegistry.serviceName,
              isMeta: routine.isMeta,
            },
          });

          try {
            for (const task of routine.tasks) {
              if (!task) {
                console.log("task is null", routine, task);
                continue;
              }
              const tasks = task.getIterator();

              while (tasks.hasNext()) {
                const nextTask = tasks.next();
                yield {
                  data: {
                    taskName: nextTask.name,
                    taskVersion: nextTask.version,
                    routineName: routine.name,
                    routineVersion: routine.version,
                    serviceName: Cadenza.serviceRegistry.serviceName,
                  },
                };
              }
            }
          } catch (e: any) {
            return {
              errored: true,
              __error: e.message,
            };
          }
        }
      },
    )
      .attachSignal("global.meta.sync_controller.routine_added")
      .then(
        (this.isCadenzaDBReady
          ? Cadenza.createCadenzaDBInsertTask(
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
              { concurrency: 50 },
            )
          : Cadenza.get("dbInsertTaskToRoutineMap")
        )?.then(
          Cadenza.createUniqueMetaTask("Finish sync", (ctx, emit) => {
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
          }).attachSignal("global.meta.sync_controller.synced"),
        ),
      );

    this.splitSignalsTask = Cadenza.createMetaTask(
      "Split signals for registration",
      function* (ctx) {
        console.log("Splitting signals for registration...");
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

          console.log("REGISTERING SIGNAL", signal);

          yield {
            data: {
              name: signal,
              isGlobal,
              domain,
              action,
              isMeta,
            },
            signalName: signal,
          };
        }
      },
    ).then(
      (this.isCadenzaDBReady
        ? Cadenza.createCadenzaDBInsertTask(
            "signal_registry",
            {
              onConflict: {
                target: ["name"],
                action: {
                  do: "nothing",
                },
              },
            },
            { concurrency: 50 },
          )
        : Cadenza.get("dbInsertSignalRegistry")
      )?.then(
        Cadenza.createMetaTask("Process signal registration", (ctx) => {
          if (!ctx.__syncing) {
            return;
          }

          console.log("REGISTERING SIGNAL", ctx.signalName, ctx.signalRegistry);

          return { signalName: ctx.signalRegistry?.name };
        }).then(Cadenza.broker.registerSignalTask!),
      ),
    );

    this.splitTasksForRegistration = Cadenza.createMetaTask(
      "Split tasks for registration",
      function* (ctx) {
        console.log("SPLITTING TASKS FOR REGISTRATION");
        const tasks = ctx.tasks;
        for (const task of tasks) {
          if (task.registered) continue;
          const { __functionString, __getTagCallback } = task.export();
          console.log("EXPORTING TASK", task.name);

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
              service_name: Cadenza.serviceRegistry.serviceName,
              signals: {
                emits: Array.from(task.emitsSignals),
                signalsToEmitAfter: Array.from(task.signalsToEmitAfter),
                signalsToEmitOnFail: Array.from(task.signalsToEmitOnFail),
                observed: Array.from(task.observedSignals),
              },
            },
            __name: task.name,
          };
        }
      },
    ).then(
      (this.isCadenzaDBReady
        ? Cadenza.createCadenzaDBInsertTask(
            "task",
            {
              onConflict: {
                target: ["name", "service_name", "version"],
                action: {
                  do: "nothing",
                },
              },
            },
            { concurrency: 50 },
          )
        : Cadenza.get("dbInsertTask")
      )?.then(
        Cadenza.createMetaTask("Record registration", (ctx) => {
          if (!ctx.__syncing) {
            return;
          }

          console.log(
            "REGISTERING TASK",
            ctx.__name,
            !!Cadenza.get(ctx.__name),
          );

          Cadenza.get(ctx.__name)!.registered = true;
        }),
      ),
    );

    const registerSignalTask = Cadenza.createMetaTask(
      "Record signal registration",
      (ctx) => {
        if (!ctx.__syncing) {
          return;
        }

        console.log(
          "REGISTERING TASK SIGNAL",
          ctx.__name,
          ctx.signalName,
          !!Cadenza.get(ctx.__name),
        );

        Cadenza.get(ctx.__name)?.registeredSignals.add(ctx.signalName);
      },
    );

    this.registerSignalToTaskMapTask = Cadenza.createMetaTask(
      "Split observed signals of task",
      function* (ctx) {
        const task = ctx.task;
        if (task.hidden || !task.register) return;

        for (const signal of task.observedSignals) {
          const _signal = signal.split(":")[0];
          if (task.registeredSignals.has(signal)) continue;

          const { isGlobal } = decomposeSignalName(_signal);

          console.log("Registering signal map for task", task.name, signal);

          yield {
            data: {
              signalName: _signal,
              isGlobal,
              taskName: task.name,
              taskVersion: task.version,
              serviceName: Cadenza.serviceRegistry.serviceName,
            },
            __name: task.name,
            signalName: signal,
          };
        }
      },
    ).then(
      (this.isCadenzaDBReady
        ? Cadenza.createCadenzaDBInsertTask(
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
            { concurrency: 50 },
          )
        : Cadenza.get("dbInsertSignalToTaskMap")
      )?.then(registerSignalTask),
    );

    this.registerTaskMapTask = Cadenza.createMetaTask(
      "Register task map to DB",
      function* (ctx) {
        const task = ctx.task;
        if (task.hidden || !task.register) return;

        for (const t of task.nextTasks) {
          if (task.taskMapRegistration.has(t.name) || t.hidden || !t.register) {
            continue;
          }

          console.log("Registering task map for task", task.name, t.name);

          yield {
            data: {
              taskName: t.name,
              taskVersion: t.version,
              predecessorTaskName: task.name,
              predecessorTaskVersion: task.version,
              serviceName: Cadenza.serviceRegistry.serviceName,
              predecessorServiceName: Cadenza.serviceRegistry.serviceName,
            },
            __name: task.name,
            __nextTaskName: t.name,
          };
        }
      },
    ).then(
      (this.isCadenzaDBReady
        ? Cadenza.createCadenzaDBInsertTask(
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
            { concurrency: 50 },
          )
        : Cadenza.get("dbInsertDirectionalTaskGraphMap")
      )?.then(
        Cadenza.createMetaTask("Record task map registration", (ctx) => {
          if (!ctx.__syncing) {
            return;
          }

          console.log(
            "REGISTERING TASK MAP",
            ctx.__name,
            ctx.__nextTaskName,
            !!Cadenza.get(ctx.__name),
          );

          Cadenza.get(ctx.__name)?.taskMapRegistration.add(ctx.__nextTaskName);
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

          console.log(
            "Registering deputy map for task",
            task.name,
            task.remoteRoutineName,
            task.serviceName,
          );
          return {
            data: {
              task_name: task.remoteRoutineName,
              task_version: 1,
              service_name: task.serviceName,
              predecessor_task_name: task.name,
              predecessor_task_version: task.version,
              predecessor_service_name: Cadenza.serviceRegistry.serviceName,
            },
            __name: task.name,
          };
        }
      },
    ).then(
      (this.isCadenzaDBReady
        ? Cadenza.createCadenzaDBInsertTask(
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
            { concurrency: 50 },
          )
        : Cadenza.get("dbInsertDirectionalTaskGraphMap")
      )?.then(
        Cadenza.createMetaTask(
          "Record deputy relationship registration",
          (ctx) => {
            if (!ctx.__syncing) {
              return;
            }

            console.log(
              "REGISTERING DEPUTY MAP",
              ctx.__name,
              !!Cadenza.get(ctx.__name),
            );

            (Cadenza.get(ctx.__name) as DeputyTask).registeredDeputyMap = true;
          },
        ),
      ),
    );

    Cadenza.broker
      .getSignalsTask!.clone()
      .doOn("sync_controller.sync_tick", "meta.sync_requested")
      .then(
        this.splitSignalsTask,
        Cadenza.registry
          .getAllTasks!.clone()
          .then(
            this.splitTasksForRegistration,
            Cadenza.registry
              .getAllRoutines!.clone()
              .then(
                this.splitRoutinesTask,
                Cadenza.registry
                  .doForEachTask!.clone()
                  .then(
                    this.registerTaskMapTask,
                    this.registerSignalToTaskMapTask,
                    this.registerDeputyRelationshipTask,
                  ),
              ),
          ),
      );

    console.log("Sync controller init", this.isCadenzaDBReady);

    if (!this.isCadenzaDBReady) {
      Cadenza.throttle(
        "sync_controller.sync_tick",
        { __syncing: true },
        300000,
        true,
      );
    } else {
      Cadenza.throttle(
        "sync_controller.sync_tick",
        { __syncing: true },
        180000,
      );
      Cadenza.schedule("meta.sync_requested", { __syncing: true }, 2000);
    }
  }
}
