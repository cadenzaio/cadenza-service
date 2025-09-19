import Cadenza from "../../Cadenza";

export default class GraphMetadataController {
  private static _instance: GraphMetadataController;
  public static get instance(): GraphMetadataController {
    if (!this._instance) this._instance = new GraphMetadataController();
    return this._instance;
  }

  constructor() {
    Cadenza.createMetaTask("Handle task creation", (ctx) => {
      return {
        data: {
          ...ctx.data,
          service_name: Cadenza.serviceRegistry.serviceName,
          // input_context_schema_id: ctx.data.inputContextSchema ? {  // TODO
          //
          // } : null,
          // output_context_schema_id: ctx.data.outputContextSchema ? {
          //
          // } : null,
        },
      };
    })
      .doOn("meta.task.created")
      .emits("meta.graph_metadata.task_created");

    Cadenza.createMetaTask("Handle task update", (ctx) => {
      return {
        ...ctx,
        filter: {
          ...ctx.filter,
          service_name: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.layer_index_changed", "meta.task.destroyed")
      .emits("meta.graph_metadata.task_updated");

    Cadenza.createMetaTask("Handle task relationship creation", (ctx) => {
      return {
        data: {
          ...ctx.data,
          service_name: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.relationship_added")
      .emits("meta.graph_metadata.task_relationship_created");

    Cadenza.createMetaTask("Handle task signal observation", (ctx) => {
      const firstChar = ctx.data.signalName.charAt(0);
      let signalServiceName;
      if (
        firstChar === firstChar.toUpperCase() &&
        firstChar !== firstChar.toLowerCase()
      ) {
        signalServiceName = ctx.data.signalName.split(".")[0];
      }
      return {
        data: {
          ...ctx.data,
          task_service_name: Cadenza.serviceRegistry.serviceName,
          signal_service_name:
            signalServiceName ?? Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.observed_signal")
      .emits("meta.graph_metadata.task_signal_observed");

    Cadenza.createMetaTask("Handle task signal attachment", (ctx) => {
      return {
        data: {
          ...ctx.data,
          service_name: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.attached_signal")
      .emits("meta.graph_metadata.task_attached_signal");

    Cadenza.createMetaTask("Handle task unsubscribing signal", (ctx) => {
      // const firstChar = ctx.data.signalName.charAt(0);
      // let signalServiceName;
      // if (
      //   firstChar === firstChar.toUpperCase() &&
      //   firstChar !== firstChar.toLowerCase()
      // ) {
      //   signalServiceName = ctx.data.signalName.split(".")[0];
      // }
      return {
        data: {
          deleted: true,
        },
        filter: {
          ...ctx.filter,
          task_service_name: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.unsubscribed_signal")
      .emits("meta.graph_metadata.task_unsubscribed_signal");

    Cadenza.createMetaTask("Handle task detaching signal", (ctx) => {
      return {
        data: {
          deleted: true,
        },
        filter: {
          ...ctx.filter,
          service_name: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.detached_signal")
      .emits("meta.graph_metadata.task_detached_signal");

    Cadenza.createMetaTask("Handle routine creation", (ctx) => {
      return {
        data: {
          ...ctx.data,
          service_name: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doAfter(Cadenza.registry.registerRoutine)
      .emits("meta.graph_metadata.routine_created");

    Cadenza.createMetaTask("Handle routine update", (ctx) => {
      return {
        ...ctx,
        filter: {
          ...ctx.filter,
          service_name: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.routine.destroyed")
      .emits("meta.graph_metadata.routine_updated");

    Cadenza.createMetaTask("Handle adding task to routine", (ctx) => {
      return {
        data: {
          ...ctx.data,
          service_name: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.routine.task_added")
      .emits("meta.graph_metadata.task_added_to_routine");

    Cadenza.createMetaTask("Handle new trace", (ctx) => {
      return {
        data: {
          ...ctx.data,
          service_name: Cadenza.serviceRegistry.serviceName,
          service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
          context_id: {
            subOperation: "insert",
            table: "context",
            data: {
              uuid: ctx.data.context.id,
              context: ctx.data.context.context,
              is_meta: ctx.data.isMeta,
            },
            return: "id",
          },
        },
      };
    }).doOn("meta.runner.new_trace");

    Cadenza.createMetaTask(
      "Handle routine execution creation",
      (ctx) => {
        const context = ctx.data.context;
        delete ctx.data.context;
        return {
          data: {
            ...ctx.data,
            service_name: Cadenza.serviceRegistry.serviceName,
            service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
            context_id: {
              subOperation: "insert",
              table: "context",
              data: {
                uuid: context.id,
                context: context.context,
                is_meta: ctx.data.isMeta,
              },
              return: "id",
            },
          },
        };
      },
      "Handles routine execution creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.runner.added_tasks")
      .emits("meta.graph_metadata.routine_execution_created");

    Cadenza.createMetaTask(
      "Handle task execution creation",
      (ctx) => {
        const context = ctx.data.context;
        delete ctx.data.context;
        return {
          data: {
            ...ctx.data,
            service_name: Cadenza.serviceRegistry.serviceName,
            service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
            context_id: {
              subOperation: "insert",
              table: "context",
              data: {
                uuid: context.id,
                context: context.context,
                is_meta: ctx.data.isMeta,
              },
              return: "id",
            },
          },
        };
      },
      "Handles task execution creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.scheduled")
      .emits("meta.graph_metadata.task_execution_created");

    Cadenza.createMetaTask(
      "Handle task execution relationship creation",
      (ctx) => {
        return {
          ...ctx,
          filter: {
            ...ctx.filter,
            service_name: Cadenza.serviceRegistry.serviceName,
          },
        };
      },
      "Handles task execution relationship creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.mapped")
      .emits("meta.graph_metadata.relationship_executed");
  }
}
