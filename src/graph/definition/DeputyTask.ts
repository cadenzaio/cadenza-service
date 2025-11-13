import { v4 as uuid } from "uuid";
import { GraphContext, Task } from "@cadenza.io/core";
import type {
  AnyObject,
  SchemaDefinition,
  TaskResult,
  ThrottleTagGetter,
} from "@cadenza.io/core";
import Cadenza from "../../Cadenza";

export default class DeputyTask extends Task {
  readonly isDeputy: boolean = true;

  protected readonly remoteRoutineName: string;
  protected serviceName: string | undefined;

  /**
   * Constructs a DeputyTask as a proxy for triggering a remote flow.
   * @param name - The local name of the DeputyTask.
   * @param remoteRoutineName - The name of the remote routine or task to trigger.
   * @param serviceName - The target service name (optional, defaults to local service if undefined).
   * @param description - A description of the task's purpose (default: '').
   * @param concurrency - The maximum number of concurrent executions (default: 0, unlimited).
   * @param timeout - Timeout in milliseconds (default: 0, handled by engine).
   * @param register - Whether to register the task in the registry (default: true).
   * @param isUnique - Whether to create a unique task (default: false). A unique task will only be executed once per execution ID, merging parents.
   * @param isMeta - Whether to create a meta task (default: false). A meta task is separate from the user logic and is only used for monitoring, optimization, and feature extensions.
   * @param isSubMeta
   * @param isHidden
   * @param getTagCallback - Callback for dynamic tagging, e.g., 'return "default"'.
   * @param inputSchema - Input schema definition.
   * @param validateInputContext - Whether to validate the input context (default: false).
   * @param outputSchema - Output schema definition.
   * @param validateOutputContext - Whether to validate the output context (default: false).
   * @param retryCount
   * @param retryDelay
   * @param retryDelayMax
   * @param retryDelayFactor
   * @emits {meta.deputy.delegation_requested} - Emitted on construction with task and service details.
   * @note Fallbacks should be handled externally via `.doOnFail`; timeouts are managed by the engine.
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
    inputSchema: SchemaDefinition | undefined = undefined,
    validateInputContext: boolean = false,
    outputSchema: SchemaDefinition | undefined = undefined,
    validateOutputContext: boolean = false,
    retryCount: number = 0,
    retryDelay: number = 0,
    retryDelayMax: number = 0,
    retryDelayFactor: number = 1,
  ) {
    const taskFunction = (
      context: AnyObject,
      emit: (signal: string, ctx: AnyObject) => void,
      progressCallback: (progress: number) => void,
    ): Promise<TaskResult> => {
      return new Promise((resolve, reject) => {
        if (context.__metadata.__blockRemoteExecution) {
          reject(new Error("Blocked remote execution"));
          return;
        }

        if (context.__metadata.__skipRemoteExecution) {
          resolve(true);
          return;
        }

        const processId = uuid();

        context.__metadata.__deputyExecId = processId;
        emit("meta.deputy.delegation_requested", {
          ...context,
        });

        // Ephemeral meta-task for progress
        Cadenza.createEphemeralMetaTask(
          `On progress deputy ${this.remoteRoutineName}`,
          (ctx) => {
            if (ctx.progress) progressCallback(ctx.progress * ctx.weight);
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

        // Ephemeral meta-task for resolution
        Cadenza.createEphemeralMetaTask(
          `Resolve deputy ${this.remoteRoutineName}`,
          (responseCtx) => {
            console.log("Resolving deputy", responseCtx);
            if (responseCtx?.errored) {
              reject(new Error(responseCtx.__error));
            } else {
              // TODO clean up metadata
              delete responseCtx.__isDeputy;
              resolve(responseCtx);
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
    };

    super(
      name,
      taskFunction,
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

    this.emit("meta.deputy.created", {
      localTaskName: this.name,
      localTaskVersion: this.version,
      remoteRoutineName: this.remoteRoutineName,
      serviceName: this.serviceName,
      communicationType: "delegation",
    });
  }

  /**
   * Triggers the delegation flow via a signal to the meta-layer.
   * @param context - The GraphContext containing execution data.
   * @param emit
   * @param progressCallback - Callback to update progress (invoked by meta-layer).
   * @param nodeData
   * @returns A Promise resolving with the task result or rejecting on error.
   * @emits {meta.deputy.executed} - Emitted with context to initiate delegation.
   * @edge Engine handles timeout and error, triggering `.doOnFail` if chained.
   * @note The resolution and progress are managed by ephemeral meta-tasks.
   */
  execute(
    context: GraphContext,
    emit: (signal: string, ctx: AnyObject) => void,
    progressCallback: (progress: number) => void,
    nodeData: { nodeId: string; routineExecId: string },
  ): TaskResult {
    const ctx = context.getContext();
    const metadata = context.getMetadata();

    const deputyContext = {
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
        __deputyTaskName: this.name,
      },
      ...ctx,
    };

    return this.taskFunction(deputyContext, emit, progressCallback);
  }
}
