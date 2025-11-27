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

  splitSignalsTask: Task;
  splitTasksForRegistration: Task;
  registerSignalToTaskMapTask: Task;
  registerTaskToSignalMapTask: Task;
  registerTaskMapTask: Task;
  registerDeputyRelationshipTask: Task;
  splitRoutinesTask: Task;

  constructor() {
    this.splitRoutinesTask = Cadenza.createMetaTask(
      "Split routines for registration",
      (ctx, emit) => {
        const { __routines } = ctx;
        if (!__routines) return;
        for (const routine of __routines) {
          if (routine.registered) continue;
          routine.registered = true;
          emit("meta.sync_controller.routine_added", {
            data: {
              name: routine.name,
              version: routine.version,
              description: routine.description,
              serviceName: Cadenza.serviceRegistry.serviceName,
              isMeta: routine.isMeta,
            },
          });

          for (const task of routine.tasks) {
            const tasks = task.getIterator();

            while (tasks.hasNext()) {
              const nextTask = tasks.next();
              emit("meta.sync_controller.task_to_routine_map", {
                data: {
                  taskName: nextTask.name,
                  taskVersion: nextTask.version,
                  routineName: routine.name,
                  routineVersion: routine.version,
                  serviceName: Cadenza.serviceRegistry.serviceName,
                },
              });
            }
          }
        }

        emit("meta.sync_controller.synced", {
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

        Cadenza.log("Synced data...");
      },
    );

    this.splitSignalsTask = Cadenza.createMetaTask(
      "Split signals for registration",
      function* (ctx, emit) {
        const { __signals } = ctx;
        if (!__signals) return;

        const filteredSignals = __signals
          .filter(
            (signal: { signal: string; data: any }) => !signal.data.registered,
          )
          .map((signal: { signal: string; data: any }) => signal.signal);

        for (const signal of filteredSignals) {
          const { isMeta, sourceServiceName, domain, action } =
            decomposeSignalName(signal);

          yield {
            data: {
              name: signal,
              sourceServiceName,
              domain,
              action,
              isMeta,
              serviceName: Cadenza.serviceRegistry.serviceName,
            },
          };
        }
      },
    )
      .doAfter(Cadenza.broker.getSignalsTask!)
      .then(
        Cadenza.createCadenzaDBInsertTask("signal_registry", {
          onConflict: {
            target: ["name"],
            action: {
              do: "nothing",
            },
          },
        }).then(
          Cadenza.createMetaTask("Process signal registration", (ctx) => {
            return { __signalName: ctx.signalRegistry?.name };
          }).then(Cadenza.broker.registerSignalTask!),
        ),
      );

    this.splitTasksForRegistration = Cadenza.createMetaTask(
      "Split tasks for registration",
      function* (ctx) {
        const tasks = ctx.__tasks;
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
              service_name: Cadenza.serviceRegistry.serviceName,
            },
            __name: task.name,
          };
        }
      },
    ).then(
      Cadenza.createCadenzaDBInsertTask("task", {
        onConflict: {
          target: ["name", "service_name", "version"],
          action: {
            do: "nothing",
          },
        },
      }).then(
        Cadenza.createMetaTask("Record registration", (ctx) => {
          Cadenza.get(ctx.__name)!.registered = true;
        }),
      ),
    );

    const registerSignalTask = Cadenza.createMetaTask(
      "Record signal registration",
      (ctx) => {
        Cadenza.get(ctx.__name)?.registeredSignals.add(ctx.__signalName);
      },
    );

    this.registerSignalToTaskMapTask = Cadenza.createMetaTask(
      "Split observed signals of task",
      function* (ctx) {
        const task = ctx.__task;
        if (task.hidden || !task.register) return;

        for (const signal of task.observedSignals) {
          let firstChar = signal.charAt(0);
          let signalServiceName;
          let _signal = signal;
          if (
            firstChar === firstChar.toUpperCase() &&
            firstChar !== firstChar.toLowerCase()
          ) {
            signalServiceName = signal.split(".")[0];
            _signal = signal.split(".").slice(1).join(".");
          }

          if (task.registeredSignals.has(_signal)) continue;

          yield {
            data: {
              signalName: _signal,
              taskName: task.name,
              taskVersion: task.version,
              taskServiceName: Cadenza.serviceRegistry.serviceName,
              signalServiceName:
                signalServiceName ?? Cadenza.serviceRegistry.serviceName,
            },
            __name: task.name,
            __signalName: _signal,
          };
        }
      },
    ).then(
      Cadenza.createCadenzaDBInsertTask("signal_to_task_map", {
        onConflict: {
          target: [
            "task_name",
            "task_version",
            "task_service_name",
            "signal_name",
            "signal_service_name",
          ],
          action: {
            do: "nothing",
          },
        },
      }).then(registerSignalTask),
    );

    this.registerTaskToSignalMapTask = Cadenza.createMetaTask(
      "Split emitted signals of task",
      function* (ctx) {
        const task = ctx.__task;
        if (task.hidden || !task.register) return;

        for (const signal of task.signalsToEmitAfter) {
          if (task.registeredSignals.has(signal)) continue;

          yield {
            data: {
              signalName: signal,
              taskName: task.name,
              taskVersion: task.version,
              serviceName: Cadenza.serviceRegistry.serviceName,
            },
            __signalName: signal,
            __name: task.name,
          };
        }

        for (const signal of task.signalsToEmitOnFail) {
          if (task.registeredSignals.has(signal)) continue;

          yield {
            data: {
              signalName: signal,
              taskName: task.name,
              taskVersion: task.version,
              serviceName: Cadenza.serviceRegistry.serviceName,
              isOnFail: true,
            },
            __signalName: signal,
            __name: task.name,
          };
        }
      },
    ).then(
      Cadenza.createCadenzaDBInsertTask("task_to_signal_map", {
        onConflict: {
          target: ["task_name", "task_version", "service_name", "signal_name"],
          action: {
            do: "nothing",
          },
        },
      }).then(registerSignalTask),
    );

    this.registerTaskMapTask = Cadenza.createMetaTask(
      "Register task map to DB",
      function* (ctx) {
        const task = ctx.__task;
        if (task.hidden || !task.register) return;

        for (const t of task.nextTasks) {
          if (task.taskMapRegistration.has(t.name)) {
            continue;
          }
          yield {
            data: {
              taskName: t.name,
              taskVersion: t.version,
              predecessorTaskName: task.name,
              predecessorTaskVersion: task.version,
              serviceName: Cadenza.serviceRegistry.serviceName,
            },
            __name: task.name,
            __nextTaskName: t.name,
          };
        }
      },
    ).then(
      Cadenza.createCadenzaDBInsertTask("directional_task_graph_map", {
        onConflict: {
          target: [
            "task_name",
            "predecessor_task_name",
            "task_version",
            "predecessor_task_version",
            "service_name",
          ],
          action: {
            do: "nothing",
          },
        },
      }).then(
        Cadenza.createMetaTask("Record task map registration", (ctx) => {
          Cadenza.get(ctx.__name)?.taskMapRegistration.add(ctx.__nextTaskName);
        }),
      ),
    );

    this.registerDeputyRelationshipTask = Cadenza.createMetaTask(
      "Register deputy relationship",
      (ctx) => {
        const task = ctx.__task;
        if (task.hidden || !task.register) return;

        if (task.isDeputy && !task.signalName) {
          if (task.registeredDeputyMap) return;
          return {
            data: {
              triggered_task_name: task.remoteRoutineName,
              triggered_task_version: 1,
              triggered_service_name: task.serviceName,
              deputy_task_name: task.name,
              deputy_task_version: task.version,
              deputy_service_name: Cadenza.serviceRegistry.serviceName,
            },
            __name: task.name,
          };
        }
      },
    ).then(
      Cadenza.createCadenzaDBInsertTask("directional_task_graph_map", {
        onConflict: {
          target: [
            "task_name",
            "predecessor_task_name",
            "task_version",
            "predecessor_task_version",
            "service_name",
          ],
          action: {
            do: "nothing",
          },
        },
      }).then(
        Cadenza.createMetaTask(
          "Record deputy relationship registration",
          (ctx) => {
            (Cadenza.get(ctx.__name) as DeputyTask).registeredDeputyMap = true;
          },
        ),
      ),
    );

    Cadenza.createDebounceMetaTask(
      "Debounce syncing of resources",
      () => true,
      "",
      500,
    )
      .doAfter(Cadenza.serviceRegistry.handleInstanceUpdateTask)
      .then(
        Cadenza.broker
          .getSignalsTask!.clone()
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
                        this.registerTaskToSignalMapTask,
                        this.registerDeputyRelationshipTask,
                      ),
                  ),
              ),
          ),
      );
  }
}
