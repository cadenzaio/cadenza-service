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
      let _signal = ctx.data.signalName;
      let signalServiceName;
      if (
        firstChar === firstChar.toUpperCase() &&
        firstChar !== firstChar.toLowerCase()
      ) {
        // TODO handle wildcards
        signalServiceName = ctx.data.signalName.split(".")[0];
        _signal = ctx.data.signalName.split(".").slice(1).join(".");
      }
      return {
        data: {
          ...ctx.data,
          signalName: _signal,
          taskServiceName: Cadenza.serviceRegistry.serviceName,
          signalServiceName:
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
            return: "uuid",
          },
        },
      };
    })
      .doOn("meta.runner.new_trace")
      .emits("meta.graph_metadata.execution_trace_created");

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
            context_id:
              typeof context === "string"
                ? context
                : {
                    subOperation: "insert",
                    table: "context",
                    data: {
                      uuid: context.id,
                      context: context.context,
                      is_meta: ctx.data.isMeta,
                    },
                    return: "uuid",
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
      "Handle routine execution started",
      () => {
        return true;
      },
      "Handles routine execution started",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.started_routine_execution")
      .emits("meta.graph_metadata.routine_execution_started");

    Cadenza.createMetaTask(
      "Handle routine execution ended",
      (ctx) => {
        const context = ctx.data.resultContext;
        delete ctx.data.resultContext;
        return {
          data: {
            ...ctx.data,
            service_name: Cadenza.serviceRegistry.serviceName,
            service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
            result_context_id:
              typeof context === "string"
                ? context
                : {
                    subOperation: "insert",
                    table: "context",
                    data: {
                      uuid: context.id,
                      context: context.context,
                      is_meta: ctx.data.isMeta,
                    },
                    return: "uuid",
                  },
          },
          filter: {
            ...ctx.filter,
          },
        };
      },
      "Handles routine execution ended",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.ended_routine_execution")
      .emits("meta.graph_metadata.routine_execution_ended");

    Cadenza.createMetaTask(
      "Handle task execution creation",
      (ctx) => {
        const context = ctx.data.context;
        delete ctx.data.context;
        if (!ctx.data.isMeta) {
          console.log("Handle task execution creation", context);
        }
        return {
          data: {
            ...ctx.data,
            service_name: Cadenza.serviceRegistry.serviceName,
            service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
            context_id:
              typeof context === "string"
                ? context
                : {
                    subOperation: "insert",
                    table: "context",
                    data: {
                      uuid: context.id,
                      context: context.context,
                      is_meta: ctx.data.isMeta,
                    },
                    return: "uuid",
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
      "Handle task execution mapped",
      () => {
        return true;
      },
      "Handles task execution mapping",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.mapped")
      .emits("meta.graph_metadata.task_execution_mapped");

    Cadenza.createMetaTask(
      "Handle task execution started",
      () => {
        return true;
      },
      "Handles task execution started",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.started")
      .emits("meta.graph_metadata.task_execution_started");

    Cadenza.createMetaTask(
      "Handle task execution ended",
      (ctx) => {
        const context = ctx.data.resultContext;
        delete ctx.data.resultContext;
        console.log("Handle task execution ended", context);
        return {
          data: {
            ...ctx.data,
            service_name: Cadenza.serviceRegistry.serviceName,
            service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
            result_context_id:
              typeof context === "string"
                ? context
                : {
                    subOperation: "insert",
                    table: "context",
                    data: {
                      uuid: context.id,
                      context: context.context,
                      is_meta: ctx.data.isMeta ?? false,
                    },
                    return: "uuid",
                  },
          },
          filter: {
            ...ctx.filter,
          },
        };
      },
      "Handles task execution ended",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.ended")
      .emits("meta.graph_metadata.task_execution_ended");

    Cadenza.createMetaTask(
      "Handle task execution relationship creation",
      (ctx) => {
        return {
          data: {
            executionCount: "increment",
          },
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
