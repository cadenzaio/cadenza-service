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
        const firstChar = __signalName.charAt(0);
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

        if (
          (firstChar === firstChar.toUpperCase() &&
            firstChar !== firstChar.toLowerCase()) ||
          firstChar === "*"
        ) {
          const serviceName = __signalName.split(".")[0];

          ctx.__listenerServiceName = Cadenza.serviceRegistry.serviceName;
          ctx.__emitterSignalName = __signalName.split(".").slice(1).join(".");
          ctx.__signalName = "meta.signal_controller.foreign_signal_registered";

          if (serviceName === "*") {
            emit("meta.signal_controller.wildcard_signal_registered", ctx);
          } else {
            emit(
              `meta.signal_controller.remote_signal_registered:${serviceName}`,
              ctx,
            );
          }
        }
        return ctx;
      },
      "Handles signal registration from a service instance",
    ).doOn("meta.signal_broker.added");

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
            ...signalEmission,
            data: ctx,
            service_name: Cadenza.serviceRegistry.serviceName,
            service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
            signal_id: {
              subOperation: "query",
              table: "signal_registry",
              filter: {
                name: ctx.__signalLog.signal_name,
                service_name: Cadenza.serviceRegistry.serviceName,
              },
              fields: ["id"],
              return: "id",
            },
          },
          transaction: true,
        };
      },
      "",
      { isSubMeta: true, concurrency: 50 },
    )
      .doOn(".*")
      .emitsAfter("sub_meta.signal_controller.signal_emitted");

    Cadenza.createMetaTask(
      "Add metadata to signal consumption",
      (ctx) => {
        return {
          data: {
            ...ctx.__data,
            serviceName: Cadenza.serviceRegistry.serviceName,
            serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
            signalId: {
              subOperation: "query",
              table: "signal_registry",
              filter: {
                name: ctx.__data.signalName,
                serviceName: Cadenza.serviceRegistry.serviceName,
              },
              fields: ["id"],
              return: "id",
            },
          },
        };
      },
      "",
      { isSubMeta: true, concurrency: 50 },
    )
      .doOn("meta.node.consumed_signal")
      .emitsAfter("sub_meta.signal_controller.signal_consumed");
  }
}
