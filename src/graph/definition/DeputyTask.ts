import { v4 as uuid } from "uuid";
import { GraphContext, InquiryOptions, Task } from "@cadenza.io/core";
import type {
  AnyObject,
  Schema,
  TaskResult,
  ThrottleTagGetter,
} from "@cadenza.io/core";
import Cadenza from "../../Cadenza";
import {
  attachDelegationRequestSnapshot,
  hoistDelegationMetadataFields,
  restoreDelegationRequestSnapshot,
  stripDelegationRequestSnapshot,
} from "../../utils/delegation";
const SERVICE_REGISTRY_TRACE_SERVICE = (
  process.env.CADENZA_SERVICE_REGISTRY_TRACE_SERVICE ?? ""
).trim();

function deputyTaskExecutor(
  this: any,
  context: AnyObject,
  emit: (signal: string, ctx: AnyObject) => void,
  inquire: (
    inquiry: string,
    context: AnyObject,
    options: InquiryOptions,
  ) => Promise<AnyObject>,
  _tools: unknown,
  progressCallback: (progress: number) => void,
): Promise<TaskResult> {
  const task = this as DeputyTask;
  return new Promise((resolve, reject) => {
    if (context.__metadata.__blockRemoteExecution) {
      reject(new Error("Blocked remote execution"));
      return;
    }

    if (context.__metadata.__skipRemoteExecution) {
      resolve(context);
      return;
    }

    const processId = uuid();

    context.__metadata.__deputyExecId = processId;

    if (
      (process.env.CADENZA_INSTANCE_DEBUG === "1" ||
        process.env.CADENZA_INSTANCE_DEBUG === "true") &&
      context.__remoteRoutineName === "Insert service_instance"
    ) {
      console.log("[CADENZA_INSTANCE_DEBUG] deputy_delegation_requested", {
        localServiceName: Cadenza.serviceRegistry.serviceName,
        localTaskName: context.__localTaskName ?? null,
        remoteRoutineName: context.__remoteRoutineName ?? null,
        targetServiceName: context.__serviceName ?? null,
        deputyExecId: processId,
        dataKeys:
          context.data && typeof context.data === "object"
            ? Object.keys(context.data)
            : [],
        queryDataKeys:
          context.queryData && typeof context.queryData === "object"
            ? Object.keys(context.queryData)
            : [],
        queryDataDataKeys:
          context.queryData?.data && typeof context.queryData.data === "object"
            ? Object.keys(context.queryData.data as AnyObject)
            : [],
      });
    }

    emit("meta.deputy.delegation_requested", {
      ...context,
    });

    Cadenza.createEphemeralMetaTask(
      `On progress deputy ${(task as any).remoteRoutineName}`,
      (ctx) => {
        if (typeof progressCallback === "function" && ctx.progress) {
          progressCallback(ctx.progress * ctx.weight);
        }
      },
      `Ephemeral task for deputy process ${processId}`,
      {
        once: false,
        destroyCondition: (ctx: AnyObject) =>
          ctx.progress === 1 || ctx.progress === undefined,
        register: false,
      },
    ).doOn(
      `meta.socket_client.delegation_progress:${processId}`,
      `meta.socket_client.delegated:${processId}`,
      `meta.fetch.delegated:${processId}`,
      `meta.service_registry.load_balance_failed:${processId}`,
    );

    Cadenza.createEphemeralMetaTask(
      `Resolve deputy ${(task as any).remoteRoutineName}`,
      (responseCtx) => {
        const mergedResponseCtx =
          responseCtx && typeof responseCtx === "object"
            ? ({
                ...context,
                ...responseCtx,
              } as AnyObject)
            : responseCtx;
        if (responseCtx?.errored) {
          reject(new Error(responseCtx.__error));
        } else {
          if (
            SERVICE_REGISTRY_TRACE_SERVICE.length > 0 &&
            Cadenza.serviceRegistry.serviceName ===
              SERVICE_REGISTRY_TRACE_SERVICE &&
            context.__remoteRoutineName === "Insert service_instance"
          ) {
            console.log(
              "[CADENZA_SERVICE_REGISTRY_TRACE] deputy_insert_service_instance_resolved",
              {
                localServiceName: Cadenza.serviceRegistry.serviceName,
                targetServiceName: context.__serviceName ?? null,
                resolverRequestId: context.__resolverRequestId ?? null,
                serviceInstanceId:
                  mergedResponseCtx && typeof mergedResponseCtx === "object"
                    ? (mergedResponseCtx as AnyObject).__serviceInstanceId ??
                      (mergedResponseCtx as AnyObject).uuid ??
                      (mergedResponseCtx as AnyObject).data?.uuid ??
                      null
                    : null,
                hasResolverQueryData:
                  mergedResponseCtx &&
                  typeof mergedResponseCtx === "object" &&
                  (mergedResponseCtx as AnyObject).__resolverQueryData !==
                    undefined,
              },
            );
          }
          if (mergedResponseCtx && typeof mergedResponseCtx === "object") {
            delete mergedResponseCtx.__isDeputy;
          }
          resolve(mergedResponseCtx);
        }
      },
      `Ephemeral resolver for deputy process ${processId}`,
      { register: false },
    ).doOn(
      `meta.socket_client.delegated:${processId}`,
      `meta.fetch.delegated:${processId}`,
      `meta.service_registry.load_balance_failed:${processId}`,
    );
  });
}

/**
 * Represents a task that delegates execution of a routine to a remote system or service.
 * The `DeputyTask` serves as a proxy to perform and track the progress of a remote workflow.
 * It extends the `Task` class with additional delegation capabilities.
 *
 * Emits various meta-signals for monitoring delegation progress and resolution.
 */
export default class DeputyTask extends Task {
  readonly isDeputy: boolean = true;

  protected readonly remoteRoutineName: string;
  protected serviceName: string | undefined;

  registeredDeputyMap: boolean = false;

  /**
   * Constructs a new instance of the class with the specified parameters.
   *
   * @param {string} name - The name of the task.
   * @param {string} remoteRoutineName - The name of the remote routine to delegate tasks to.
   * @param {string | undefined} [serviceName=undefined] - The name of the service associated with the task.
   * @param {string} [description=""] - A brief description of the task.
   * @param {number} [concurrency=0] - The concurrency level of the task.
   * @param {number} [timeout=0] - The timeout duration for the task.
   * @param {boolean} [register=true] - Whether the task should be registered in the system.
   * @param {boolean} [isUnique=false] - Whether the task is unique.
   * @param {boolean} [isMeta=false] - Whether the task is a meta task.
   * @param {boolean} [isSubMeta=false] - Whether the task is a sub-meta task.
   * @param {boolean} [isHidden=false] - Whether the task is hidden from the system.
   * @param {ThrottleTagGetter | undefined} [getTagCallback=undefined] - A callback function to retrieve throttle tags.
   * @param {SchemaDefinition | undefined} [inputSchema=undefined] - The input schema definition for the task.
   * @param {boolean} [validateInputContext=false] - Whether to validate the input context against the input schema.
   * @param {SchemaDefinition | undefined} [outputSchema=undefined] - The output schema definition for the task.
   * @param {boolean} [validateOutputContext=false] - Whether to validate the output context against the output schema.
   * @param {number} [retryCount=0] - The number of retries allowed for task execution.
   * @param {number} [retryDelay=0] - The initial delay between retries in milliseconds.
   * @param {number} [retryDelayMax=0] - The maximum retry delay in milliseconds.
   * @param {number} [retryDelayFactor=1] - The factor by which to increase the retry delay for subsequent retries.
   * @return {void} This constructor does not return a value.
   */
  constructor(
    name: string,
    remoteRoutineName: string,
    serviceName: string | undefined = undefined,
    description: string = "",
    concurrency: number = 0,
    timeout: number = 0,
    register: boolean = true,
    isUnique: boolean = false,
    isMeta: boolean = false,
    isSubMeta: boolean = false,
    isHidden: boolean = false,
    getTagCallback: ThrottleTagGetter | undefined = undefined,
    inputSchema: Schema | undefined = undefined,
    validateInputContext: boolean = false,
    outputSchema: Schema | undefined = undefined,
    validateOutputContext: boolean = false,
    retryCount: number = 0,
    retryDelay: number = 0,
    retryDelayMax: number = 0,
    retryDelayFactor: number = 1,
  ) {
    super(
      name,
      deputyTaskExecutor as any,
      description,
      concurrency,
      timeout,
      register,
      isUnique,
      isMeta,
      isSubMeta,
      isHidden,
      getTagCallback,
      inputSchema,
      validateInputContext,
      outputSchema,
      validateOutputContext,
      retryCount,
      retryDelay,
      retryDelayMax,
      retryDelayFactor,
    );

    this.remoteRoutineName = remoteRoutineName;
    this.serviceName = serviceName;

    this.attachSignal("meta.deputy.delegation_requested");

    this.emit("meta.deputy.created", {
      localTaskName: this.name,
      localTaskVersion: this.version,
      remoteRoutineName: this.remoteRoutineName,
      serviceName: this.serviceName,
      communicationType: "delegation",
    });
  }

  clone(): never {
    throw new Error(
      `DeputyTask '${this.name}' does not support clone(). Create a new named deputy task or use a flow-specific meta task that performs an inquiry instead.`,
    );
  }

  /**
   * Executes the specified task function within the provided execution context.
   *
   * @param {GraphContext} context - The execution context containing methods and metadata for task execution.
   * @param {function(string, AnyObject): void} emit - A function for emitting signals with associated data during execution.
   * @param inquire
   * @param {function(number): void} progressCallback - A callback function to report progress updates during task processing.
   * @param {{ nodeId: string, routineExecId: string }} nodeData - Object containing identifiers for the node and routine execution.
   * @return {TaskResult} Returns the result of the task function execution.
   */
  execute(
    context: GraphContext,
    emit: (signal: string, ctx: AnyObject) => void,
    inquire: (
      inquiry: string,
      context: AnyObject,
      options: InquiryOptions,
    ) => Promise<AnyObject>,
    progressCallback: (progress: number) => void,
    nodeData: { nodeId: string; routineExecId: string },
  ): TaskResult {
    const rawContext = restoreDelegationRequestSnapshot(
      attachDelegationRequestSnapshot({
        ...context.getFullContext(),
      }),
    );
    const metadata =
      rawContext.__metadata && typeof rawContext.__metadata === "object"
        ? rawContext.__metadata
        : context.getMetadata();
    const ctx = {
      ...rawContext,
    };
    delete ctx.__metadata;

    const deputyContext = attachDelegationRequestSnapshot(
      stripDelegationRequestSnapshot(
        hoistDelegationMetadataFields({
          ...ctx,
          __timeout: this.timeout,
          __localTaskName: this.name,
          __localTaskVersion: this.version,
          __localServiceName: Cadenza.serviceRegistry.serviceName,
          __previousTaskExecutionId: nodeData.nodeId,
          __remoteRoutineName: this.remoteRoutineName,
          __serviceName: this.serviceName,
          __localRoutineExecId:
            metadata.__routineExecId ?? metadata.__metadata?.__routineExecId,
          __executionTraceId: metadata.__executionTraceId ?? null,
          __metadata: {
            ...metadata,
            __skipRemoteExecution:
              metadata.__skipRemoteExecution ?? ctx.__skipRemoteExecution ?? false,
            __blockRemoteExecution:
              metadata.__blockRemoteExecution ?? ctx.__blockRemoteExecution ?? false,
            __deputyTaskName: this.name,
          },
        }),
      ),
    );

    const resolvedTools =
      typeof (Cadenza as any).resolveToolsForOwner === "function"
        ? (Cadenza as any).resolveToolsForOwner(
            this,
            deputyContext,
            emit,
            inquire,
            progressCallback,
          )
        : {
            helpers: {},
            globals: {},
          };
    const resolvedProgressCallback =
      typeof progressCallback === "function" ? progressCallback : () => {};

    return (this.taskFunction as any)(
      deputyContext,
      emit,
      inquire,
      resolvedTools,
      resolvedProgressCallback,
    );
  }
}
