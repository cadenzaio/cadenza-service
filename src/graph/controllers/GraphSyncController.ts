import Cadenza from "../../Cadenza";
import { Task } from "@cadenza.io/core";
import { decomposeSignalName } from "../../utils/tools";

export default class GraphSyncController {
  private static _instance: GraphSyncController;
  public static get instance(): GraphSyncController {
    if (!this._instance) this._instance = new GraphSyncController();
    return this._instance;
  }
  constructor() {
    // Cadenza.broker.clearSignalsTask?.doOn("meta.sync_requested");

    Cadenza.createDebounceMetaTask(
      "Debounce syncing of resources",
      () => true,
      "",
      500,
    )
      .doAfter(Cadenza.serviceRegistry.handleInstanceUpdateTask)
      .then(
        Cadenza.broker.getSignalsTask!.then(
          Cadenza.registry.getAllTasks.then(Cadenza.registry.getAllRoutines),
        ),
      );

    Cadenza.createMetaTask("Split routines for registration", (ctx, emit) => {
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
    }).doAfter(Cadenza.registry.getAllRoutines);

    Cadenza.createMetaTask("Split signals for registration", (ctx, emit) => {
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

        emit("meta.sync_controller.signal_added", {
          data: {
            name: signal,
            sourceServiceName,
            domain,
            action,
            isMeta,
            serviceName: Cadenza.serviceRegistry.serviceName,
          },
        });
        emit("meta.signal.registered", {
          __signalName: signal,
        });
      }
    }).doAfter(Cadenza.broker.getSignalsTask!);

    Cadenza.createMetaTask("Split tasks for registration", (ctx, emit) => {
      const { __tasks } = ctx;
      if (!__tasks) return;

      for (const task of __tasks) {
        if (task.registered) continue;
        task.registered = true;
        console.log("REGISTERING TASK", task.name);
        const { __functionString, __getTagCallback } = task.export();
        emit("meta.sync_controller.task_added", {
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
        });

        task.mapObservedSignals((signal: string) => {
          const firstChar = signal.charAt(0);
          let _signal = signal;
          let signalServiceName;
          if (
            firstChar === firstChar.toUpperCase() &&
            firstChar !== firstChar.toLowerCase()
          ) {
            // TODO handle wildcards
            signalServiceName = signal.split(".")[0];
            _signal = signal.split(".").slice(1).join(".");
          }

          emit("meta.sync_controller.signal_to_task_map", {
            data: {
              signalName: _signal,
              taskName: task.name,
              taskVersion: task.version,
              taskServiceName: Cadenza.serviceRegistry.serviceName,
              signalServiceName:
                signalServiceName ?? Cadenza.serviceRegistry.serviceName,
            },
          });
        });

        task.mapSignals((signal: string) => {
          emit("meta.sync_controller.task_to_signal_map", {
            data: {
              signalName: signal,
              taskName: task.name,
              taskVersion: task.version,
              serviceName: Cadenza.serviceRegistry.serviceName,
            },
          });
        });

        task.mapOnFailSignals((signal: string) => {
          emit("meta.sync_controller.task_to_signal_map", {
            // TODO: handle signals that are not observed.
            data: {
              signalName: signal,
              taskName: task.name,
              taskVersion: task.version,
              serviceName: Cadenza.serviceRegistry.serviceName,
              isOnFail: true,
            },
          });
        });
      }

      for (const task of __tasks) {
        if (task.hidden || !task.register) continue;
        task.mapNext((t: Task) =>
          emit("meta.sync_controller.task_map", {
            data: {
              taskName: t.name,
              taskVersion: t.version,
              predecessorTaskName: task.name,
              predecessorTaskVersion: task.version,
              serviceName: Cadenza.serviceRegistry.serviceName,
            },
          }),
        );

        if (task.isDeputy && !task.signalName) {
          emit("meta.sync_controller.deputy_relationship_created", {
            data: {
              triggered_task_name: task.remoteRoutineName,
              triggered_task_version: 1,
              triggered_service_name: task.serviceName,
              deputy_task_name: task.name,
              deputy_task_version: task.version,
              deputy_service_name: Cadenza.serviceRegistry.serviceName,
            },
          });
        }
      }

      return true;
    }).doAfter(Cadenza.registry.getAllTasks);
  }
}
