import Cadenza from "../../Cadenza";
import { formatTimestamp } from "../../utils/tools";

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
          serviceName: Cadenza.serviceRegistry.serviceName,
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
      .emits("global.meta.graph_metadata.task_created");

    Cadenza.createMetaTask("Handle task update", (ctx) => {
      return {
        ...ctx,
        filter: {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.layer_index_changed", "meta.task.destroyed")
      .emits("global.meta.graph_metadata.task_updated");

    Cadenza.createMetaTask("Handle task relationship creation", (ctx) => {
      return {
        data: {
          ...ctx.data,
          serviceName: Cadenza.serviceRegistry.serviceName,
          predecessorServiceName: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.relationship_added")
      .emits("global.meta.graph_metadata.task_relationship_created");

    Cadenza.createMetaTask("Handle task error", (ctx) => {
      Cadenza.log(`Error in task ${ctx.data.taskName}`, ctx.data, "error");
    }).doOn("meta.node.errored");

    Cadenza.createMetaTask("Handle task signal observation", (ctx) => {
      const isGlobal = ctx.signalName.startsWith("global.");
      return {
        data: {
          ...ctx.data,
          isGlobal,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.observed_signal")
      .emits("global.meta.graph_metadata.task_signal_observed");

    Cadenza.createMetaTask("Handle task signal attachment", (ctx) => {
      return {
        data: {
          ...ctx.data,
        },
        filter: {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.attached_signal")
      .emits("global.meta.graph_metadata.task_attached_signal");

    Cadenza.createMetaTask("Handle task unsubscribing signal", (ctx) => {
      return {
        data: {
          deleted: true,
        },
        filter: {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
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
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.task.detached_signal")
      .emits("global.meta.graph_metadata.task_detached_signal");

    Cadenza.createMetaTask("Handle routine creation", (ctx) => {
      return {
        data: {
          ...ctx.data,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doAfter(Cadenza.registry.registerRoutine)
      .emits("global.meta.graph_metadata.routine_created");

    Cadenza.createMetaTask("Handle routine update", (ctx) => {
      return {
        ...ctx,
        filter: {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.routine.destroyed")
      .emits("global.meta.graph_metadata.routine_updated");

    Cadenza.createMetaTask("Handle adding task to routine", (ctx) => {
      return {
        data: {
          ...ctx.data,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.routine.task_added")
      .emits("global.meta.graph_metadata.task_added_to_routine");

    Cadenza.createMetaTask("Handle new trace", (ctx) => {
      return {
        data: {
          ...ctx.data,
          service_name: Cadenza.serviceRegistry.serviceName,
          service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
        },
      };
    })
      .doOn("meta.runner.new_trace", "sub_meta.signal_broker.new_trace")
      .emits("global.meta.graph_metadata.execution_trace_created");

    Cadenza.createMetaTask(
      "Handle routine execution creation",
      (ctx) => {
        return {
          queryData: {
            data: {
              ...ctx.data,
              serviceName: Cadenza.serviceRegistry.serviceName,
              serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
            },
          },
        };
      },
      "Handles routine execution creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.runner.added_tasks")
      .emits("global.meta.graph_metadata.routine_execution_created");

    Cadenza.createMetaTask(
      "Handle routine execution started",
      () => {
        return true;
      },
      "Handles routine execution started",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.started_routine_execution")
      .emits("global.meta.graph_metadata.routine_execution_started");

    Cadenza.createMetaTask(
      "Handle routine execution ended",
      (ctx) => {
        return {
          data: {
            ...ctx.data,
            serviceName: Cadenza.serviceRegistry.serviceName,
            serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
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
      .emits("global.meta.graph_metadata.routine_execution_ended");

    Cadenza.createMetaTask(
      "Handle task execution creation",
      (ctx) => {
        return {
          data: {
            ...ctx.data,
            serviceName: Cadenza.serviceRegistry.serviceName,
            serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
          },
        };
      },
      "Handles task execution creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.scheduled")
      .emits("global.meta.graph_metadata.task_execution_created");

    Cadenza.createMetaTask(
      "Handle task execution mapped",
      () => {
        return true;
      },
      "Handles task execution mapping",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.mapped", "meta.node.detected_previous_task_execution")
      .emits("global.meta.graph_metadata.task_execution_mapped");

    Cadenza.createMetaTask(
      "Handle task execution started",
      () => {
        return true;
      },
      "Handles task execution started",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.started")
      .emits("global.meta.graph_metadata.task_execution_started");

    Cadenza.createMetaTask(
      "Handle task execution ended",
      (ctx) => {
        return {
          data: {
            ...ctx.data,
            serviceName: Cadenza.serviceRegistry.serviceName,
            serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
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
      .emits("global.meta.graph_metadata.task_execution_ended");

    Cadenza.createMetaTask(
      "Handle task execution relationship creation",
      (ctx) => {
        return {
          data: {
            executionCount: "increment",
            lastExecuted: formatTimestamp(Date.now()),
          },
          filter: {
            ...ctx.filter,
            serviceName: Cadenza.serviceRegistry.serviceName,
          },
        };
      },
      "Handles task execution relationship creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.mapped", "meta.node.detected_previous_task_execution")
      .emits("global.meta.graph_metadata.relationship_executed");

    Cadenza.createMetaTask("Handle Intent Creation", (ctx) => {
      return {
        data: {
          ...ctx.data,
        },
      };
    })
      .doOn("meta.inquiry_broker.added")
      .emits("global.meta.graph_metadata.intent_created");
  }
}
