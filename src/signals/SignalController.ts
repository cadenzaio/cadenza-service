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
        const { signalName } = ctx;
        const { isMeta, isGlobal, domain, action } =
          decomposeSignalName(signalName);

        emit("global.meta.signal_controller.signal_added", {
          data: {
            name: signalName,
            isGlobal,
            domain,
            action,
            isMeta,
          },
        });

        return ctx;
      },
      "Handles signal registration from a service instance",
    )
      .doOn("meta.signal_broker.added")
      .attachSignal("global.meta.signal_controller.signal_added");

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
      .emits("global.sub_meta.signal_controller.signal_emitted");

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
      .emits("global.sub_meta.signal_controller.signal_consumed");
  }
}
