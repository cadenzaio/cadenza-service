import { META_ACTOR_SESSION_STATE_PERSIST_INTENT } from "@cadenza.io/core";
import Cadenza from "../../Cadenza";
import { formatTimestamp } from "../../utils/tools";
import { isMetaIntentName } from "../../utils/inquiry";

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

    Cadenza.createMetaTask("Handle actor creation", (ctx) => {
      return {
        data: {
          ...ctx.data,
          service_name: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.actor.created")
      .emits("global.meta.graph_metadata.actor_created");

    Cadenza.createMetaTask("Handle actor task association", (ctx) => {
      return {
        data: {
          ...ctx.data,
          service_name: Cadenza.serviceRegistry.serviceName,
        },
      };
    })
      .doOn("meta.actor.task_associated")
      .emits("global.meta.graph_metadata.actor_task_associated");

    const actorSessionStateInsertTask =
      Cadenza.get("dbInsertActorSessionState") ??
      Cadenza.get("Insert actor_session_state in CadenzaDB") ??
      Cadenza.createCadenzaDBInsertTask(
        "actor_session_state",
        {},
        { concurrency: 100, isSubMeta: true },
      );

    const validateActorSessionStatePersistenceTask = Cadenza.createMetaTask(
      "Validate actor session state persistence",
      (ctx) => {
        if (ctx.errored || ctx.failed || ctx.__success !== true) {
          throw new Error(
            String(
              ctx.__error ??
                ctx.error ??
                "actor_session_state persistence query failed",
            ),
          );
        }

        const rowCount = Number(ctx.rowCount ?? 0);
        if (!Number.isFinite(rowCount) || rowCount <= 0) {
          throw new Error(
            "actor_session_state persistence did not affect any rows (possible stale durable_version)",
          );
        }

        return {
          __success: true,
          persisted: true,
          actor_name: ctx.actor_name,
          actor_version: ctx.actor_version,
          actor_key: ctx.actor_key,
          service_name: ctx.service_name,
          durable_version: ctx.durable_version,
        };
      },
      "Enforces strict actor session persistence success contract.",
      { isSubMeta: true, concurrency: 100 },
    );

    const insertAndValidateActorSessionStateTask = actorSessionStateInsertTask.then(
      validateActorSessionStatePersistenceTask,
    );

    Cadenza.createMetaTask(
      "Persist actor session state",
      (ctx) => {
        const actorName =
          typeof ctx.actor_name === "string" ? ctx.actor_name.trim() : "";
        const actorKey =
          typeof ctx.actor_key === "string" ? ctx.actor_key.trim() : "";
        const actorVersion = Number(ctx.actor_version ?? 1);
        const durableVersion = Number(ctx.durable_version);

        if (!actorName) {
          throw new Error("actor_name is required for actor session persistence");
        }

        if (!actorKey) {
          throw new Error("actor_key is required for actor session persistence");
        }

        if (!Number.isInteger(actorVersion) || actorVersion < 1) {
          throw new Error("actor_version must be a positive integer");
        }

        if (!Number.isInteger(durableVersion) || durableVersion < 0) {
          throw new Error("durable_version must be a non-negative integer");
        }

        if (
          typeof ctx.durable_state !== "object" ||
          ctx.durable_state === null ||
          Array.isArray(ctx.durable_state)
        ) {
          throw new Error("durable_state must be a non-null object");
        }

        const serviceName = Cadenza.serviceRegistry.serviceName;
        if (!serviceName) {
          throw new Error("service_name is not available for actor session persistence");
        }

        let expiresAt: string | null = null;
        if (ctx.expires_at !== undefined && ctx.expires_at !== null) {
          if (ctx.expires_at instanceof Date) {
            expiresAt = ctx.expires_at.toISOString();
          } else if (
            typeof ctx.expires_at === "string" &&
            ctx.expires_at.trim().length > 0
          ) {
            expiresAt = ctx.expires_at;
          } else {
            throw new Error("expires_at must be null, Date, or non-empty string");
          }
        }

        const updatedAt = new Date().toISOString();

        return {
          ...ctx,
          actor_name: actorName,
          actor_key: actorKey,
          actor_version: actorVersion,
          durable_version: durableVersion,
          expires_at: expiresAt,
          service_name: serviceName,
          queryData: {
            data: {
              actor_name: actorName,
              actor_version: actorVersion,
              actor_key: actorKey,
              service_name: serviceName,
              durable_state: ctx.durable_state,
              durable_version: durableVersion,
              expires_at: expiresAt,
              updated: updatedAt,
            },
            onConflict: {
              target: [
                "actor_name",
                "actor_version",
                "actor_key",
                "service_name",
              ],
              action: {
                do: "update",
                set: {
                  durable_state: "excluded",
                  durable_version: "excluded",
                  expires_at: "excluded",
                  updated: "excluded",
                },
                where:
                  "actor_session_state.durable_version <= excluded.durable_version",
              },
            },
          },
        };
      },
      "Validates and prepares actor_session_state payload for strict write-through persistence.",
      { isSubMeta: true, concurrency: 100 },
    )
      .then(insertAndValidateActorSessionStateTask)
      .respondsTo(META_ACTOR_SESSION_STATE_PERSIST_INTENT);

    Cadenza.createMetaTask("Handle Intent Creation", (ctx) => {
      const intentName = ctx.data?.name;
      return {
        data: {
          ...ctx.data,
          isMeta: intentName ? isMetaIntentName(intentName) : false,
        },
      };
    })
      .doOn("meta.inquiry_broker.added")
      .emits("global.meta.graph_metadata.intent_created");
  }
}
