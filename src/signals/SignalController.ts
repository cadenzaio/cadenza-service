import Cadenza from "../Cadenza";

export default class SignalController {
  private static _instance: SignalController;
  public static get instance(): SignalController {
    if (!this._instance) this._instance = new SignalController();
    return this._instance;
  }

  constructor() {
    Cadenza.createMetaTask(
      "Handle Signal Registration",
      (ctx, emit) => {
        const { __signalName } = ctx;
        const parts = __signalName.split(".");
        const domain = parts[0] === "meta" ? parts[1] : parts[0];
        const action = parts[parts.length - 1];

        emit("meta.signal_controller.signal_added", {
          data: {
            name: __signalName,
            domain: domain,
            action: action,
            is_meta: parts[0] === "meta",
            service_name: Cadenza.serviceRegistry.serviceName,
          },
        });

        return ctx;
      },
      "Handles signal registration from a service instance",
    )
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

              ctx.__listenerServiceName = Cadenza.serviceRegistry.serviceName;
              ctx.__emitterSignalName = __signalName
                .split(".")
                .slice(1)
                .join(".");
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
      )
      .doOn("meta.signal_broker.added");

    Cadenza.createMetaTask(
      "Forward signal observations to remote service",
      (ctx, emit) => {
        const { remoteSignals, serviceName } = ctx;

        for (const remoteSignal of remoteSignals) {
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
      ).doOn(__emitterSignalName);

      return true;
    }).doOn("meta.signal_controller.foreign_signal_registered");

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
            signal_name: signalEmission.signalName,
            task_name: signalEmission.taskName,
            task_version: signalEmission.taskVersion,
            task_execution_id: signalEmission.taskExecutionId,
            is_meta: signalEmission.isMeta,
            is_metric: signalEmission.isMetric,
            data: ctx,
            service_name: Cadenza.serviceRegistry.serviceName,
            service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
          },
        };
      },
      "",
      { isSubMeta: true, concurrency: 100 },
    )
      .doOn(".*")
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
