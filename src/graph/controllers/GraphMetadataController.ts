import Cadenza from "../../Cadenza";
import { formatTimestamp } from "../../utils/tools";
import { isMetaIntentName } from "../../utils/inquiry";
import { registerActorSessionPersistenceTasks } from "./registerActorSessionPersistence";
import type { Task } from "@cadenza.io/core";

function buildDatabaseTriggerContext(
  data?: Record<string, unknown> | null,
  filter?: Record<string, unknown> | null,
  extra: Record<string, unknown> = {},
  queryExtra: Record<string, unknown> = {},
): Record<string, unknown> {
  const nextData =
    data && typeof data === "object" ? { ...data } : undefined;
  const nextFilter =
    filter && typeof filter === "object" ? { ...filter } : undefined;
  const queryData: Record<string, unknown> = { ...queryExtra };

  if (nextData !== undefined) {
    queryData.data = nextData;
  }

  if (nextFilter !== undefined) {
    queryData.filter = nextFilter;
  }

  return {
    ...extra,
    ...(nextData !== undefined ? { data: nextData } : {}),
    ...(nextFilter !== undefined ? { filter: nextFilter } : {}),
    ...(Object.keys(queryData).length > 0 ? { queryData } : {}),
  };
}

function resolveTaskFromMetadataContext(
  ctx: Record<string, any>,
): Task | undefined {
  const taskName = String(
    ctx?.taskName ??
      ctx?.data?.taskName ??
      ctx?.data?.task_name ??
      ctx?.filter?.taskName ??
      ctx?.filter?.task_name ??
      "",
  );

  return taskName ? Cadenza.get(taskName) : undefined;
}

function resolveTaskByName(name: unknown): Task | undefined {
  const taskName = String(name ?? "");
  return taskName ? Cadenza.get(taskName) : undefined;
}

function resolvePredecessorTaskFromMetadataContext(
  ctx: Record<string, any>,
): Task | undefined {
  return resolveTaskByName(
    ctx?.predecessorTaskName ??
      ctx?.data?.predecessorTaskName ??
      ctx?.data?.predecessor_task_name ??
      ctx?.filter?.predecessorTaskName ??
      ctx?.filter?.predecessor_task_name,
  );
}

function shouldSkipDirectTaskMetadata(task?: Task): boolean {
  return !task || !task.register || task.isHidden || task.isDeputy;
}

function shouldPersistBusinessTaskExecution(task?: Task): boolean {
  return !!task && task.register && !task.isHidden && !task.isMeta && !task.isSubMeta && !task.isDeputy;
}

function shouldEmitDirectPrimitiveMetadata(): boolean {
  return Cadenza.hasCompletedBootstrapSync();
}

function shouldPersistBusinessInquiry(ctx: Record<string, any>): boolean {
  const inquiryName = String(
    ctx?.data?.name ?? ctx?.inquiry ?? ctx?.data?.metadata?.inquiryMeta?.inquiry ?? "",
  );

  if (!inquiryName) {
    return false;
  }

  return !isMetaIntentName(inquiryName) && ctx?.data?.isMeta !== true && ctx?.data?.is_meta !== true;
}

function shouldPersistRoutineExecution(ctx: Record<string, any>): boolean {
  if (ctx?.data?.isMeta === true || ctx?.data?.is_meta === true) {
    return false;
  }

  const routineTask = resolveTaskByName(ctx?.data?.name);
  if (routineTask) {
    return shouldPersistBusinessTaskExecution(routineTask);
  }

  return true;
}

function shouldPersistTaskExecutionMetadata(
  ctx: Record<string, any>,
): boolean {
  const task = resolveTaskFromMetadataContext(ctx);
  return shouldPersistBusinessTaskExecution(task);
}

function shouldPersistTaskExecutionMap(
  ctx: Record<string, any>,
): boolean {
  return (
    shouldPersistBusinessTaskExecution(resolveTaskFromMetadataContext(ctx)) &&
    shouldPersistBusinessTaskExecution(resolvePredecessorTaskFromMetadataContext(ctx))
  );
}

function hasInquiryLink(data: Record<string, any> | undefined): boolean {
  const metaContext = data?.metaContext ?? data?.meta_context;
  const directInquiryId = metaContext?.__inquiryId ?? metaContext?.__metadata?.__inquiryId;
  return typeof directInquiryId === "string" && directInquiryId.length > 0;
}

export default class GraphMetadataController {
  private static _instance: GraphMetadataController;
  public static get instance(): GraphMetadataController {
    if (!this._instance) this._instance = new GraphMetadataController();
    return this._instance;
  }

  constructor() {
    const buildOnConflictDoNothing = (target: string[]) => ({
      target,
      action: {
        do: "nothing" as const,
      },
    });

    Cadenza.createMetaTask("Handle task creation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const taskName = String(ctx.data?.name ?? ctx.data?.taskName ?? "");
      const task = taskName ? Cadenza.get(taskName) : undefined;
      const onConflict = buildOnConflictDoNothing([
        "name",
        "service_name",
        "version",
      ]);

      if (
        shouldSkipDirectTaskMetadata(task) ||
        task?.registered ||
        (task as any)?.registrationRequested
      ) {
        return false;
      }

      if (task) {
        (task as any).registrationRequested = true;
      }

      return buildDatabaseTriggerContext(
        {
          ...ctx.data,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
        undefined,
        { onConflict },
        { onConflict },
      );
    })
      .doOn("meta.task.created")
      .emits("global.meta.graph_metadata.task_created");

    Cadenza.createMetaTask("Handle task update", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      return buildDatabaseTriggerContext(
        (ctx.data as Record<string, unknown> | undefined) ?? undefined,
        {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      );
    })
      .doOn("meta.task.layer_index_changed", "meta.task.destroyed")
      .emits("global.meta.graph_metadata.task_updated");

    Cadenza.createMetaTask("Handle task relationship creation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const taskName = ctx.data?.taskName ?? ctx.data?.task_name;
      const predecessorTaskName =
        ctx.data?.predecessorTaskName ?? ctx.data?.predecessor_task_name;
      const task = taskName ? Cadenza.get(taskName) : undefined;
      const predecessorTask = predecessorTaskName
        ? Cadenza.get(predecessorTaskName)
        : undefined;

      if (
        shouldSkipDirectTaskMetadata(task) ||
        shouldSkipDirectTaskMetadata(predecessorTask) ||
        !task?.registered ||
        !predecessorTask?.registered
      ) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        serviceName: Cadenza.serviceRegistry.serviceName,
        predecessorServiceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.task.relationship_added")
      .emits("global.meta.graph_metadata.task_relationship_created");

    Cadenza.createMetaTask("Handle task error", (ctx) => {
      Cadenza.log(`Error in task ${ctx.data.taskName}`, ctx.data, "error");
    }).doOn("meta.node.errored");

    Cadenza.createMetaTask("Handle task signal observation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const signalName = String(
        ctx.signalName ?? ctx.data?.signalName ?? "",
      ).split(":")[0];
      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      if (task?.registered && task.registeredSignals.has(signalName)) {
        return false;
      }

      const isGlobal = signalName.startsWith("global.");
      return buildDatabaseTriggerContext({
        ...ctx.data,
        signalName,
        isGlobal,
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.task.observed_signal")
      .emits("global.meta.graph_metadata.task_signal_observed");

    Cadenza.createMetaTask("Handle task signal attachment", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      return buildDatabaseTriggerContext(
        (ctx.data as Record<string, unknown> | undefined) ?? undefined,
        {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      );
    })
      .doOn("meta.task.attached_signal")
      .emits("global.meta.graph_metadata.task_attached_signal");

    Cadenza.createMetaTask("Handle task intent association", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const intentName = String(ctx.data?.intentName ?? "");
      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      if (
        task?.registered &&
        ((task as any).__registeredIntents as Set<string> | undefined)?.has(
          intentName,
        )
      ) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...(ctx.data as Record<string, unknown> | undefined),
        intentName,
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.task.intent_associated")
      .emits("global.meta.graph_metadata.task_intent_associated");

    Cadenza.createMetaTask("Handle task unsubscribing signal", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      return buildDatabaseTriggerContext(
        {
          deleted: true,
        },
        {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      );
    })
      .doOn("meta.task.unsubscribed_signal")
      .emits("meta.graph_metadata.task_unsubscribed_signal");

    Cadenza.createMetaTask("Handle task detaching signal", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const task = resolveTaskFromMetadataContext(ctx as Record<string, any>);

      if (shouldSkipDirectTaskMetadata(task)) {
        return false;
      }

      return buildDatabaseTriggerContext(
        {
          deleted: true,
        },
        {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      );
    })
      .doOn("meta.task.detached_signal")
      .emits("global.meta.graph_metadata.task_detached_signal");

    Cadenza.createMetaTask("Handle routine creation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doAfter(Cadenza.registry.registerRoutine)
      .emits("global.meta.graph_metadata.routine_created");

    Cadenza.createMetaTask("Handle routine update", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      return buildDatabaseTriggerContext(
        (ctx.data as Record<string, unknown> | undefined) ?? undefined,
        {
          ...ctx.filter,
          serviceName: Cadenza.serviceRegistry.serviceName,
        },
      );
    })
      .doOn("meta.routine.destroyed")
      .emits("global.meta.graph_metadata.routine_updated");

    Cadenza.createMetaTask("Handle adding task to routine", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        serviceName: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.routine.task_added")
      .emits("global.meta.graph_metadata.task_added_to_routine");

    Cadenza.createMetaTask("Handle new trace", (ctx) => {
      return buildDatabaseTriggerContext({
        ...ctx.data,
        service_name: Cadenza.serviceRegistry.serviceName,
        service_instance_id: Cadenza.serviceRegistry.serviceInstanceId,
      });
    })
      .doOn("meta.runner.new_trace", "sub_meta.signal_broker.new_trace")
      .emits("global.meta.graph_metadata.execution_trace_created");

    Cadenza.createMetaTask(
      "Handle routine execution creation",
      (ctx) => {
        if (!shouldPersistRoutineExecution(ctx as Record<string, any>)) {
          return false;
        }

        return buildDatabaseTriggerContext({
          ...ctx.data,
          previousRoutineExecution: hasInquiryLink(ctx.data as Record<string, any>)
            ? null
            : (ctx.data as Record<string, any>)?.previousRoutineExecution ??
              (ctx.data as Record<string, any>)?.previous_routine_execution ??
              null,
          serviceName: Cadenza.serviceRegistry.serviceName,
          serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
        });
      },
      "Handles routine execution creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.runner.added_tasks")
      .emits("global.meta.graph_metadata.routine_execution_created");

    Cadenza.createMetaTask(
      "Handle routine execution started",
      (ctx) => {
        return buildDatabaseTriggerContext(
          (ctx.data as Record<string, unknown> | undefined) ?? undefined,
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles routine execution started",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.started_routine_execution")
      .emits("global.meta.graph_metadata.routine_execution_started");

    Cadenza.createMetaTask(
      "Handle routine execution ended",
      (ctx) => {
        return buildDatabaseTriggerContext(
          {
            ...ctx.data,
            serviceName: Cadenza.serviceRegistry.serviceName,
            serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
          },
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles routine execution ended",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.ended_routine_execution")
      .emits("global.meta.graph_metadata.routine_execution_ended");

    Cadenza.createMetaTask(
      "Handle task execution creation",
      (ctx) => {
        if (!shouldPersistTaskExecutionMetadata(ctx as Record<string, any>)) {
          return false;
        }

        return buildDatabaseTriggerContext({
          ...ctx.data,
          serviceName: Cadenza.serviceRegistry.serviceName,
          serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
        });
      },
      "Handles task execution creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.scheduled")
      .emits("global.meta.graph_metadata.task_execution_created");

    Cadenza.createMetaTask(
      "Handle task execution mapped",
      (ctx) => {
        if (!shouldPersistTaskExecutionMap(ctx as Record<string, any>)) {
          return false;
        }

        return buildDatabaseTriggerContext(
          (ctx.data as Record<string, unknown> | undefined) ?? undefined,
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles task execution mapping",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.mapped", "meta.node.detected_previous_task_execution")
      .emits("global.meta.graph_metadata.task_execution_mapped");

    Cadenza.createMetaTask(
      "Handle task execution started",
      (ctx) => {
        if (!shouldPersistTaskExecutionMetadata(ctx as Record<string, any>)) {
          return false;
        }

        return buildDatabaseTriggerContext(
          (ctx.data as Record<string, unknown> | undefined) ?? undefined,
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles task execution started",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.started")
      .emits("global.meta.graph_metadata.task_execution_started");

    Cadenza.createMetaTask(
      "Handle task execution ended",
      (ctx) => {
        if (!shouldPersistTaskExecutionMetadata(ctx as Record<string, any>)) {
          return false;
        }

        return buildDatabaseTriggerContext(
          {
            ...ctx.data,
            serviceName: Cadenza.serviceRegistry.serviceName,
            serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
          },
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles task execution ended",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.ended")
      .emits("global.meta.graph_metadata.task_execution_ended");

    Cadenza.createMetaTask(
      "Handle inquiry creation",
      (ctx) => {
        if (!shouldPersistBusinessInquiry(ctx as Record<string, any>)) {
          return false;
        }

        return buildDatabaseTriggerContext({
          ...ctx.data,
          serviceName: Cadenza.serviceRegistry.serviceName,
          serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
          isMeta: false,
        });
      },
      "Handles inquiry creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.inquiry_broker.inquiry_started")
      .emits("global.meta.graph_metadata.inquiry_created");

    Cadenza.createMetaTask(
      "Handle inquiry update",
      (ctx) => {
        return buildDatabaseTriggerContext(
          (ctx.data as Record<string, unknown> | undefined) ?? undefined,
          (ctx.filter as Record<string, unknown> | undefined) ?? undefined,
        );
      },
      "Handles inquiry completion updates",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.inquiry_broker.inquiry_completed")
      .emits("global.meta.graph_metadata.inquiry_updated");

    Cadenza.createMetaTask(
      "Handle task execution relationship creation",
      (ctx) => {
        if (!shouldPersistTaskExecutionMap(ctx as Record<string, any>)) {
          return false;
        }

        return buildDatabaseTriggerContext(
          {
            executionCount: "increment",
            lastExecuted: formatTimestamp(Date.now()),
          },
          {
            ...ctx.filter,
            serviceName: Cadenza.serviceRegistry.serviceName,
          },
        );
      },
      "Handles task execution relationship creation",
      { concurrency: 100, isSubMeta: true },
    )
      .doOn("meta.node.mapped", "meta.node.detected_previous_task_execution")
      .emits("global.meta.graph_metadata.relationship_executed");

    Cadenza.createMetaTask("Handle actor creation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        service_name: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.actor.created")
      .emits("global.meta.graph_metadata.actor_created");

    Cadenza.createMetaTask("Handle actor task association", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      return buildDatabaseTriggerContext({
        ...ctx.data,
        service_name: Cadenza.serviceRegistry.serviceName,
      });
    })
      .doOn("meta.actor.task_associated")
      .emits("global.meta.graph_metadata.actor_task_associated");

    registerActorSessionPersistenceTasks();

    Cadenza.createMetaTask("Handle Intent Creation", (ctx) => {
      if (!shouldEmitDirectPrimitiveMetadata()) {
        return false;
      }

      const intentName = ctx.data?.name;
      return buildDatabaseTriggerContext({
        ...ctx.data,
        isMeta: intentName ? isMetaIntentName(intentName) : false,
      });
    })
      .doOn("meta.inquiry_broker.added")
      .emits("global.meta.graph_metadata.intent_created");
  }
}
