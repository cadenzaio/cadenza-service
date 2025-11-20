import Cadenza from "../Cadenza";
import { decomposeSignalName } from "../utils/tools";

/**
 * SignalController is a singleton class that manages signal registration, transmission,
 * and metadata handling within a service instance.
 *
 * This class utilizes the Cadenza framework to handle various tasks related to signals,
 * such as registering signals, transmitting signals to remote services, adding metadata,
 * and maintaining clean-forwarding and processing of signals.
 *
 * Features:
 * - Ensures signals are properly registered and metadata is propagated.
 * - Handles foreign signal registration for cross-service communication.
 * - Forwards signal observations and manages their metadata.
 * - Adds metadata during signal emission and consumption.
 * - Implements a meta-task-based system for handling complex workflows.
 *
 * Constructor initializes the necessary meta-tasks and tasks required for signal management.
 */
export default class SignalController {
  private static _instance: SignalController;
  public static get instance(): SignalController {
    if (!this._instance) this._instance = new SignalController();
    return this._instance;
  }

  /**
   * Constructor method for initializing the signal registration and data transmission process.
   * This method defines multiple meta tasks to manage signals, forwarding, and adding metadata
   * for service instances in a distributed system.
   *
   * Some key functionalities include:
   * - Registering signals from a service instance.
   * - Handling foreign signal registration from remote services.
   * - Forwarding signal observations between services.
   * - Adding metadata to signal emissions for better traceability.
   * - Adding metadata to signal consumption events.
   *
   * It serves as an initializer for signals and tasks.
   */
  constructor() {
    Cadenza.createMetaTask(
      "Handle Signal Registration",
      (ctx, emit) => {
        const { __signalName } = ctx;
        const { isMeta, sourceServiceName, domain, action } =
          decomposeSignalName(__signalName);

        emit("meta.signal_controller.signal_added", {
          data: {
            name: __signalName,
            sourceServiceName,
            domain,
            action,
            isMeta,
            service_name: Cadenza.serviceRegistry.serviceName,
          },
        });

        return ctx;
      },
      "Handles signal registration from a service instance",
    )
      .doOn("meta.signal_broker.added")
      .then(
        Cadenza.createMetaTask(
          "Handle foreign signal registration",
          (ctx, emit) => {
            const { __signalName } = ctx;
            const firstChar = __signalName.charAt(0);

            if (
              (firstChar === firstChar.toUpperCase() &&
                firstChar !== firstChar.toLowerCase()) ||
              firstChar === "*"
            ) {
              const serviceName = __signalName.split(".")[0];

              if (Cadenza.serviceRegistry.serviceName === serviceName) {
                return false;
              }

              ctx.__listenerServiceName = Cadenza.serviceRegistry.serviceName;
              ctx.__emitterSignalName = __signalName;
              ctx.__signalName =
                "meta.signal_controller.foreign_signal_registered";
              ctx.__remoteServiceName = serviceName;

              if (serviceName === "*") {
                emit("meta.signal_controller.wildcard_signal_registered", ctx);
              } else {
                emit(
                  `meta.signal_controller.remote_signal_registered:${serviceName}`,
                  ctx,
                );
              }

              return ctx;
            }
          },
        ).then(Cadenza.serviceRegistry.handleRemoteSignalRegistrationTask),
      );

    Cadenza.createMetaTask(
      "Forward signal observations to remote service",
      (ctx, emit) => {
        const { remoteSignals, serviceName } = ctx;

        for (const remoteSignal of remoteSignals) {
          remoteSignal.__signalName =
            "meta.signal_controller.foreign_signal_registered";

          emit(
            `meta.signal_controller.remote_signal_registered:${serviceName}`,
            remoteSignal,
          );
        }

        return true;
      },
      "Forwards signal observations to remote service",
    ).doAfter(Cadenza.serviceRegistry.getRemoteSignalsTask);

    Cadenza.createMetaTask("Handle foreign signal registration", (ctx) => {
      const { __emitterSignalName, __listenerServiceName } = ctx;

      Cadenza.createSignalTransmissionTask(
        __emitterSignalName,
        __listenerServiceName,
      ).doOn(__emitterSignalName.split(".").slice(1).join("."));

      return true;
    }).doOn(
      "meta.signal_controller.foreign_signal_registered",
      "meta.service_registry.foreign_signal_registered",
    );

    // TODO: Cleanup transmission tasks?

    Cadenza.createMetaTask(
      "Add data to signal emission",
      (ctx) => {
        const signalEmission = ctx.__signalEmission;

        if (!signalEmission) {
          return false;
        }

        return {
          data: {
            uuid: signalEmission.uuid,
            signal_name: signalEmission.signalName,
            signal_tag: signalEmission.signalTag,
            task_name: signalEmission.taskName,
            task_version: signalEmission.taskVersion,
            task_execution_id: signalEmission.taskExecutionId,
            is_meta: signalEmission.isMeta,
            is_metric: signalEmission.isMetric ?? false,
            routine_execution_id: signalEmission.routineExecutionId,
            execution_trace_id: signalEmission.executionTraceId,
            data: ctx,
            service_name: Cadenza.serviceRegistry.serviceName,
            service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
          },
        };
      },
      "",
      { isSubMeta: true, concurrency: 100 },
    )
      .doOn("sub_meta.signal_broker.emitting_signal")
      .emits("sub_meta.signal_controller.signal_emitted");

    Cadenza.createMetaTask(
      "Add metadata to signal consumption",
      (ctx) => {
        return {
          data: {
            ...ctx.data,
            serviceName: Cadenza.serviceRegistry.serviceName,
            serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
          },
        };
      },
      "",
      { isSubMeta: true, concurrency: 100 },
    )
      .doOn("meta.node.consumed_signal")
      .emits("sub_meta.signal_controller.signal_consumed");
  }
}
