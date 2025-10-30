import DeputyTask from "./DeputyTask";
import { GraphContext } from "@cadenza.io/core";
import type {
  AnyObject,
  SchemaDefinition,
  TaskResult,
  ThrottleTagGetter,
} from "@cadenza.io/core";
import { DbOperationPayload } from "../../types/queryData";
import Cadenza from "../../Cadenza";

export default class DatabaseTask extends DeputyTask {
  private readonly queryData: DbOperationPayload;

  /**
   * Constructs a DatabaseTask to execute a database operation on a remote service.
   * @param name - The local name of the DatabaseTask.
   * @param taskName - The name of the database operation task to trigger (e.g., 'dbQueryTaskExecution').
   * @param serviceName - The target database service name (optional, defaults to 'DatabaseService').
   * @param description - A description of the task's purpose (default: '').
   * @param queryData - The query data object containing operation details (e.g., { __operation: 'query', __table: 'users' }).
   * @param concurrency - The maximum number of concurrent executions (default: 0, unlimited).
   * @param timeout - Timeout in milliseconds (default: 0, handled by engine).
   * @param register - Whether to register the task in the registry (default: true).
   * @param isUnique
   * @param isMeta
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
   * @emits {meta.deputy.created} - Emitted on construction with task and service details.
   * @note Fallbacks via `.doOnFail` externally; timeouts managed by the engine.
   */
  constructor(
    name: string,
    taskName: string,
    serviceName: string | undefined = undefined,
    description: string = "",
    queryData: DbOperationPayload,
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
    super(
      name,
      taskName,
      serviceName,
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
    this.queryData = queryData;
  }

  /**
   * Triggers the database operation delegation flow via a signal to the meta-layer.
   * @param context - The GraphContext containing execution data.
   * @param emit
   * @param progressCallback - Callback to update progress (invoked by meta-layer).
   * @param nodeData
   * @returns A Promise resolving with the task result or rejecting on error.
   * @emits {meta.deputy.executed} - Emitted with context including queryData to initiate delegation.
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
    const dynamicQueryData = ctx.queryData ?? {};
    delete ctx.queryData;

    const deputyContext = {
      __localTaskName: this.name,
      __localTaskVersion: this.version,
      __localServiceName: Cadenza.serviceRegistry.serviceName,
      __previousTaskExecutionId: nodeData.nodeId,
      __remoteRoutineName: this.remoteRoutineName,
      __serviceName: this.serviceName,
      __executionTraceId: metadata.__executionTraceId ?? null,
      __localRoutineExecId:
        metadata.__routineExecId ?? metadata.__metadata?.__routineExecId,
      __metadata: {
        ...metadata,
        __deputyTaskName: this.name,
      },
      queryData: {
        ...this.queryData,
        data: {
          ...ctx.data,
        },
        ...dynamicQueryData,
      },
    };

    return this.taskFunction(deputyContext, emit, progressCallback);
  }
}
