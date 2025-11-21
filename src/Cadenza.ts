import Cadenza, {
  AnyObject,
  CadenzaMode,
  DebounceOptions,
  DebounceTask,
  EphemeralTask,
  EphemeralTaskOptions,
  GraphRegistry,
  GraphRoutine,
  GraphRunner,
  SignalBroker,
  Task,
  TaskFunction,
  TaskOptions,
  ThrottleTagGetter,
} from "@cadenza.io/core";
import DeputyTask from "./graph/definition/DeputyTask";
import DatabaseTask from "./graph/definition/DatabaseTask";
import ServiceRegistry from "./registry/ServiceRegistry";
import SignalTransmissionTask from "./graph/definition/SignalTransmissionTask";
import RestController from "./network/RestController";
import SocketController from "./network/SocketController";
import SignalController from "./signals/SignalController";
import { DbOperationPayload, DbOperationType } from "./types/queryData";
import GraphMetadataController from "./graph/controllers/GraphMetadataController";
import { SchemaDefinition } from "./types/database";
import { snakeCase } from "lodash-es";
import DatabaseController from "./database/DatabaseController";
import { v4 as uuid } from "uuid";
import GraphSyncController from "./graph/controllers/GraphSyncController";
import { isBrowser } from "./utils/environment";
import { formatTimestamp } from "./utils/tools";

export type SecurityProfile = "low" | "medium" | "high";
export type NetworkMode =
  | "internal"
  | "exposed"
  | "exposed-high-sec"
  | "auto"
  | "dev";

export type ServerOptions = {
  customServiceId?: string; // TODO
  loadBalance?: boolean;
  useSocket?: boolean;
  log?: boolean;
  displayName?: string;
  isMeta?: boolean;
  port?: number; // for internal network
  securityProfile?: SecurityProfile;
  networkMode?: NetworkMode;
  retryCount?: number;
  cadenzaDB?: { connect?: boolean; address?: string; port?: number };
  relatedServices?: string[][];
  isDatabase?: boolean;
  isFrontend?: boolean;
};

export interface DatabaseOptions {
  databaseType?: "postgres";
  databaseName?: string;
  poolSize?: number;
}

/**
 * The CadenzaService class serves as a central service layer providing various utility methods for managing tasks, signals, logging, and service interactions.
 * This class handles the initialization (`bootstrap`) and validation of services, as well as the creation of tasks associated with services and signals.
 */
export default class CadenzaService {
  public static broker: SignalBroker;
  public static runner: GraphRunner;
  public static metaRunner: GraphRunner;
  public static registry: GraphRegistry;
  public static serviceRegistry: ServiceRegistry;
  protected static isBootstrapped = false;
  protected static serviceCreated = false;

  /**
   * Initializes the application by setting up necessary components and configurations.
   * This method ensures the initialization process is only executed once throughout the application lifecycle.
   *
   * @return {void} This method does not return any value.
   */
  static bootstrap(): void {
    if (this.isBootstrapped) return;
    this.isBootstrapped = true;

    Cadenza.bootstrap();
    this.broker = Cadenza.broker;
    this.runner = Cadenza.runner;
    this.metaRunner = Cadenza.metaRunner;
    this.registry = Cadenza.registry;
    this.serviceRegistry = ServiceRegistry.instance;
    SignalController.instance;
    RestController.instance;
    SocketController.instance;
    console.log("BOOTSTRAPPED");
  }

  /**
   * Validates the provided service name based on specific rules.
   *
   * @param {string} serviceName - The service name to validate. Must be less than 100 characters,
   *                                must not contain spaces, dots, or backslashes, and must start with a capital letter.
   * @return {void} Throws an error if the service name does not meet the validation criteria.
   * @throws {Error} If the service name exceeds 100 characters.
   * @throws {Error} If the service name contains spaces.
   * @throws {Error} If the service name contains dots.
   * @throws {Error} If the service name contains backslashes.
   * @throws {Error} If the service name does not start with a capital letter.
   */
  protected static validateServiceName(serviceName: string) {
    if (serviceName.length > 100) {
      throw new Error("Service name must be less than 100 characters");
    }

    if (serviceName.includes(" ")) {
      throw new Error("Service name must not contain spaces");
    }

    if (serviceName.includes(".")) {
      throw new Error("Service name must not contain dots");
    }

    if (serviceName.includes("\\")) {
      throw new Error("Service name must not contain backslashes");
    }

    if (
      serviceName.charAt(0) !== serviceName.charAt(0).toUpperCase() &&
      serviceName.charAt(0) === serviceName.charAt(0).toLowerCase()
    ) {
      throw new Error("Service name must start with a capital letter");
    }
  }

  /**
   * Validates the provided name to ensure it meets the required criteria.
   *
   * @param {string} name - The name to be validated.
   * @return {void} Does not return any value.
   */
  protected static validateName(name: string): void {
    Cadenza.validateName(name);
  }

  /**
   * Gets the current run strategy from the Cadenza configuration.
   *
   * @return {Function} The run strategy function defined in the Cadenza configuration.
   */
  public static get runStrategy() {
    return Cadenza.runStrategy;
  }

  /**
   * Sets the mode for the Cadenza application.
   *
   * @param {CadenzaMode} mode - The mode to be set for the application.
   * @return {void} This method does not return a value.
   */
  public static setMode(mode: CadenzaMode) {
    Cadenza.setMode(mode);
  }

  /**
   * Emits a signal with the specified data using the associated broker.
   *
   * @param {string} signal - The name of the event or signal to emit.
   * @param {AnyObject} [data={}] - The data to be emitted along with the signal.
   * @return {void} No return value.
   *
   * @example
   * This is meant to be used as a global event emitter.
   * If you want to emit an event from within a task, you can use the `emit` method provided to the task function. See {@link TaskFunction}.
   * ```ts
   * Cadenza.emit('main.my_event', { foo: 'bar' });
   * ```
   */
  static emit(signal: string, data: AnyObject = {}) {
    this.broker?.emit(signal, data);
  }

  /**
   * Executes the given task or graph routine within the provided context using the configured runner.
   *
   * @param {Task | GraphRoutine} task - The task or graph routine to be executed.
   * @param {AnyObject} context - The context within which the task will be executed.
   * @return {void}
   *
   * @example
   * ```ts
   * const task = Cadenza.createTask('My task', (ctx) => {
   *   console.log('My task executed with context:', ctx);
   * });
   *
   * Cadenza.run(task, { foo: 'bar' });
   *
   * const routine = Cadenza.createRoutine('My routine', [task], 'My routine description');
   *
   * Cadenza.run(routine, { foo: 'bar' });
   * ```
   */
  static run(task: Task | GraphRoutine, context: AnyObject) {
    this.runner?.run(task, context);
  }

  /**
   * Logs a message with a specified log level and additional contextual data.
   * Records in the CadenzaDB when available.
   *
   * @param {string} message - The main message to be logged.
   * @param {any} [data={}] - Additional data or metadata to include with the log.
   * @param {"info"|"warning"|"error"|"critical"} [level="info"] - The severity level of the log message.
   * @param {string|null} [subjectServiceName=null] - The name of the subject service related to the log.
   * @param {string|null} [subjectServiceInstanceId=null] - The instance ID of the subject service related to the log.
   * @return {void} No return value.
   */
  static log(
    message: string,
    data: any = {},
    level: "info" | "warning" | "error" | "critical" = "info",
    subjectServiceName: string | null = null,
    subjectServiceInstanceId: string | null = null,
  ) {
    if (level === "critical") {
      console.error("CRITICAL:", message);
    } else if (level === "error") {
      console.error(message);
    } else if (level === "warning") {
      console.warn(message);
    } else {
      console.log(message);
    }

    this.emit("meta.system_log.log", {
      data: {
        data,
        level,
        message,
        serviceName: this.serviceRegistry?.serviceName,
        serviceInstanceId: this.serviceRegistry?.serviceInstanceId,
        subjectServiceName,
        subjectServiceInstanceId,
        created: formatTimestamp(Date.now()),
      },
    });
  }

  /**
   * Creates a new DeputyTask instance based on the provided routine name, service name, and options.
   * This method ensures proper task initialization, including setting a unique name,
   * validation of the routine name, and applying default option values.
   *
   * @param {string} routineName - The name of the routine the task references. This is mandatory and should be a valid string.
   * @param {string|undefined} [serviceName] - The name of the service that the routine belongs to. This is optional and defaults to undefined.
   * @param {TaskOptions} [options={}] - A configuration object for the task, allowing various properties such as concurrency, timeout, and retry settings to be customized.
   * @return {DeputyTask} - A new DeputyTask instance initialized with the specified parameters.
   *
   * @example
   * Let's say we are writing the code for a Service called "Service1".
   * We also have an additional service called "Service2" with a routine called "My Routine".
   * A flow on Service1 depends on the result of "My Routine" on Service2.
   * We can create a deputy task for the routine using the following code:
   * ```ts
   * Cadenza.createDeputyTask("My Routine", "Service2").then(
   *   Cadenza.createTask("Handle result", (ctx) => {
   *     console.log("'Handle result' executed with context:", ctx);
   *   }),
   * );
   * ```
   * Internally, this will send a request to an available "Service2" instance to execute the "My Routine" routine.
   * The deputy task will wait for the response and then execute the next task(s) in the chain.
   *
   * You can visualize the execution of the deputy task as follows:
   * ```
   * Service1 flow = [Deputy tasks for "My Routine"] -> ["Handle result"]
   *                       ||       A
   *                       V       ||
   * Service2 flow =    [[My Routine]]
   * ```
   *
   * Deputy tasks are useful for delegating flows to other services, allowing for parallel execution and load balancing.
   * But it creates tight coupling between the services, which may not be desirable in some cases.
   * In cases where an event on one service should simply trigger a flow on another service, without the need for a result,
   * it is recommended to use signals instead. Like this:
   *
   * Service1
   * ```ts
   * Cadenza.createTask("Generate event", (ctx, emit) => {
   *   // Do something
   *   emit("some.event");
   * });
   * ```
   *
   * Service2
   * ```ts
   * Cadenza.createTask("Handle event", (ctx) => {
   *   console.log("Handle event executed with context:", ctx);
   * }).doOn("Service1.some.event");
   * ```
   *
   * Every time the "Generate event" task is executed, it will emit a signal "Service1.some.event" to one Service2 instance and trigger the "Handle event" task.
   */
  static createDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    options: TaskOptions = {},
  ): DeputyTask {
    this.bootstrap();
    this.validateName(routineName);
    const name = `Deputy task for: ${routineName}`;

    options = {
      concurrency: 0,
      timeout: 0,
      register: true,
      isUnique: false,
      isMeta: false,
      isSubMeta: false,
      isHidden: false,
      getTagCallback: undefined,
      inputSchema: undefined,
      validateInputContext: false,
      outputSchema: undefined,
      validateOutputContext: false,
      retryCount: 0,
      retryDelay: 0,
      retryDelayMax: 0,
      retryDelayFactor: 1,
      ...options,
    };

    return new DeputyTask(
      name,
      routineName,
      serviceName,
      `Referencing routine in service: ${routineName} on service: ${serviceName}.`,
      options.concurrency,
      options.timeout,
      options.register,
      options.isUnique,
      options.isMeta,
      options.isSubMeta,
      options.isHidden,
      options.getTagCallback,
      options.inputSchema,
      options.validateInputContext,
      options.outputSchema,
      options.validateOutputContext,
      options.retryCount,
      options.retryDelay,
      options.retryDelayMax,
      options.retryDelayFactor,
    );
  }

  /**
   * Creates a meta deputy task by setting the `isMeta` property in the options to true,
   * and delegating task creation to the `createDeputyTask` method.
   * See {@link createDeputyTask} and {@link createMetaTask} for more information.
   *
   * @param {string} routineName - The name of the routine associated with the task.
   * @param {string | undefined} [serviceName] - The optional name of the service associated with the task.
   * @param {TaskOptions} [options={}] - Additional options for the task. Defaults to an empty object if not provided.
   * @return {DeputyTask} - The created meta deputy task.
   */
  static createMetaDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    options: TaskOptions = {},
  ): DeputyTask {
    options.isMeta = true;
    return this.createDeputyTask(routineName, serviceName, options);
  }

  /**
   * Creates a throttled deputy task with the specified parameters.
   * See {@link createThrottledTask} and {@link createDeputyTask} for more information.
   *
   * @param {string} routineName - The name of the routine to be executed.
   * @param {string | undefined} [serviceName=undefined] - The name of the service, if applicable.
   * @param {ThrottleTagGetter} [throttledIdGetter=() => "default"] - A function to get the throttled tag for the task.
   * @param {TaskOptions} [options={}] - The options for task configuration, including concurrency and callbacks.
   * @return {DeputyTask} The created throttled deputy task.
   */
  static createThrottledDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    throttledIdGetter: ThrottleTagGetter = () => "default",
    options: TaskOptions = {},
  ) {
    options.concurrency = 1;
    options.getTagCallback = throttledIdGetter;
    return this.createDeputyTask(routineName, serviceName, options);
  }

  /**
   * Creates a throttled deputy task with meta-task settings enabled.
   * See {@link createThrottledTask},{@link createDeputyTask} and {@link createMetaTask} for more information.
   *
   * @param {string} routineName - The name of the routine for which the task is being created.
   * @param {string|undefined} [serviceName=undefined] - The name of the service associated with the task, or undefined if not applicable.
   * @param {ThrottleTagGetter} [throttledIdGetter=() => "default"] - A function to compute or return the throttling identifier.
   * @param {TaskOptions} [options={}] - Additional options for the task configuration.
   * @return {any} Returns the created throttled deputy task instance.
   */
  static createMetaThrottledDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    throttledIdGetter: ThrottleTagGetter = () => "default",
    options: TaskOptions = {},
  ) {
    options.isMeta = true;
    return this.createThrottledDeputyTask(
      routineName,
      serviceName,
      throttledIdGetter,
      options,
    );
  }

  /**
   * Creates and configures a signal transmission task that handles the transmission
   * of a specified signal to a target service with a set of customizable options.
   * This is only used for internal purposes and is not exposed to the business logic layer.
   *
   * @param {string} signalName - The name of the signal to be transmitted.
   * @param {string} serviceName - The name of the target service to transmit the signal to.
   * @param {TaskOptions} [options={}] - A set of optional parameters to further configure the task.
   * @return {SignalTransmissionTask} A new instance of SignalTransmissionTask configured with the given parameters.
   */
  static createSignalTransmissionTask(
    signalName: string,
    serviceName: string,
    options: TaskOptions = {},
  ): SignalTransmissionTask {
    this.bootstrap();
    Cadenza.validateName(signalName);
    Cadenza.validateName(serviceName);

    options = {
      concurrency: 0,
      timeout: 0,
      register: true,
      isUnique: false,
      isMeta: true,
      isSubMeta: false,
      isHidden: false,
      getTagCallback: undefined,
      inputSchema: undefined,
      validateInputContext: false,
      outputSchema: undefined,
      validateOutputContext: false,
      retryCount: 1,
      retryDelay: 0,
      retryDelayMax: 0,
      retryDelayFactor: 1,
      ...options,
    };

    options.isMeta = true;

    const name = `Transmission of signal: ${signalName}`;
    return new SignalTransmissionTask(
      name,
      signalName,
      serviceName,
      `Transmits signal ${signalName} to ${serviceName} service.`,
      options.concurrency,
      options.timeout,
      options.register,
      options.isUnique,
      options.isMeta,
      options.isSubMeta,
      options.isHidden,
      options.getTagCallback,
      options.inputSchema,
      options.validateInputContext,
      options.outputSchema,
      options.validateOutputContext,
      options.retryCount,
      options.retryDelay,
      options.retryDelayMax,
      options.retryDelayFactor,
    );
  }

  /**
   * Creates and configures a database task that performs an operation on a specified table.
   *
   * @param {string} tableName - The name of the database table on which the operation will be performed.
   * @param {DbOperationType} operation - The type of database operation to execute (e.g., insert, update, delete).
   * @param {string|undefined} [databaseServiceName=undefined] - The name of the database service; defaults to "default database service" if not provided.
   * @param {DbOperationPayload} queryData - The data payload required for executing the specified database operation.
   * @param {TaskOptions} [options={}] - Optional configuration for the task, including concurrency, timeout, and retry policies.
   * @return {DatabaseTask} A configured database task instance ready for execution.
   */
  static createDatabaseTask(
    tableName: string,
    operation: DbOperationType,
    databaseServiceName: string | undefined = undefined,
    queryData: DbOperationPayload,
    options: TaskOptions = {},
  ) {
    this.bootstrap();
    Cadenza.validateName(tableName);
    Cadenza.validateName(operation);
    const tableNameFormatted = tableName
      .split("_")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join("");
    const name = `${operation} ${tableName} in ${databaseServiceName ?? "default database service"}`;
    const description = `Executes a ${operation} on table ${tableName} in ${databaseServiceName ?? "default database service"}`;
    const taskName = `db${operation.charAt(0).toUpperCase() + operation.slice(1)}${tableNameFormatted}`;

    options = {
      concurrency: 0,
      timeout: 0,
      register: true,
      isUnique: false,
      isMeta: false,
      isSubMeta: false,
      isHidden: false,
      getTagCallback: undefined,
      inputSchema: undefined,
      validateInputContext: false,
      outputSchema: undefined,
      validateOutputContext: false,
      retryCount: 3,
      retryDelay: 100,
      retryDelayMax: 0,
      retryDelayFactor: 1,
      ...options,
    };

    return new DatabaseTask(
      name,
      taskName,
      databaseServiceName,
      description,
      queryData,
      options.concurrency,
      options.timeout,
      options.register,
      options.isUnique,
      options.isMeta,
      options.isSubMeta,
      options.isHidden,
      options.getTagCallback,
      options.inputSchema,
      options.validateInputContext,
      options.outputSchema,
      options.validateOutputContext,
      options.retryCount,
      options.retryDelay,
      options.retryDelayMax,
      options.retryDelayFactor,
    );
  }

  /**
   * Creates a task for performing a database insert operation.
   *
   * @param {string} tableName - The name of the table where the insert operation will be performed.
   * @param {string | undefined} [databaseServiceName=undefined] - The name of the database service to use. Optional parameter, defaults to undefined.
   * @param {DbOperationPayload} [queryData={}] - The data payload for the insert operation. Defaults to an empty object.
   * @param {TaskOptions} [options={}] - Additional task options to configure the insert operation. Defaults to an empty object.
   * @return {object} A task configuration object for the database insert operation.
   */
  static createDatabaseInsertTask(
    tableName: string,
    databaseServiceName: string | undefined = undefined,
    queryData: DbOperationPayload = {},
    options: TaskOptions = {},
  ) {
    return this.createDatabaseTask(
      tableName,
      "insert",
      databaseServiceName,
      queryData,
      options,
    );
  }

  /**
   * Creates a database query task for the specified table and configuration.
   *
   * @param {string} tableName - The name of the database table to execute the query on.
   * @param {string | undefined} [databaseServiceName=undefined] - The name of the database service to use. If undefined, the default service will be used.
   * @param {DbOperationPayload} queryData - The payload containing the query data to be executed.
   * @param {TaskOptions} [options={}] - Optional parameters to configure the task execution.
   * @return {Task} The created database query task.
   */
  static createDatabaseQueryTask(
    tableName: string,
    databaseServiceName: string | undefined = undefined,
    queryData: DbOperationPayload,
    options: TaskOptions = {},
  ) {
    return this.createDatabaseTask(
      tableName,
      "query",
      databaseServiceName,
      queryData,
      options,
    );
  }

  /**
   * Creates a database task for the CadenzaDB with the specified parameters.
   *
   * @param {string} tableName - The name of the database table on which the operation will be performed.
   * @param {DbOperationType} operation - The type of database operation to execute (e.g., INSERT, UPDATE, DELETE).
   * @param {DbOperationPayload} queryData - The payload or data required to perform the database operation.
   * @param {TaskOptions} [options={}] - Additional options for the task, such as configuration settings.
   * @return {any} The result of creating the database task.
   */
  static createCadenzaDBTask(
    tableName: string,
    operation: DbOperationType,
    queryData: DbOperationPayload,
    options: TaskOptions = {},
  ) {
    options.isMeta = true;
    return this.createDatabaseTask(
      tableName,
      operation,
      "CadenzaDB",
      queryData,
      options,
    );
  }

  /**
   * Creates a database insert task specifically for the CadenzaDB database.
   *
   * @param {string} tableName - The name of the table into which the data will be inserted.
   * @param {DbOperationPayload} [queryData={}] - An object representing the data to be inserted.
   * @param {TaskOptions} [options={}] - Additional options to customize the task. The `isMeta` property is set to true by default.
   * @return {Task} A task object configured to perform an insert operation in the CadenzaDB database.
   */
  static createCadenzaDBInsertTask(
    tableName: string,
    queryData: DbOperationPayload = {},
    options: TaskOptions = {},
  ) {
    options.isMeta = true;
    return this.createDatabaseInsertTask(
      tableName,
      "CadenzaDB",
      queryData,
      options,
    );
  }

  /**
   * Creates a database query task specifically for the CadenzaDB.
   *
   * @param {string} tableName - The name of the database table to execute the query on.
   * @param {DbOperationPayload} queryData - The payload containing data and parameters for the database operation.
   * @param {TaskOptions} [options={}] - Additional options for the task configuration.
   * @return {any} The created task for executing a database query.
   */
  static createCadenzaDBQueryTask(
    tableName: string,
    queryData: DbOperationPayload,
    options: TaskOptions = {},
  ) {
    options.isMeta = true;
    return this.createDatabaseQueryTask(
      tableName,
      "CadenzaDB",
      queryData,
      options,
    );
  }

  /**
   * Creates a new Cadenza service with the specified configuration.
   *
   * @param {string} serviceName - The unique name of the service to create.
   * @param {string} [description] - An optional description of the service.
   * @param {ServerOptions} [options] - An optional object containing configuration options for the service.
   * @return {boolean} Returns true when the service is successfully created.
   */
  static createCadenzaService(
    serviceName: string,
    description: string = "",
    options: ServerOptions = {},
  ) {
    if (this.serviceCreated) return;
    this.bootstrap();
    Cadenza.validateName(serviceName);
    this.validateServiceName(serviceName);

    const serviceId = options.customServiceId ?? uuid();
    this.serviceRegistry.serviceName = serviceName;
    this.serviceRegistry.serviceInstanceId = serviceId;

    options = {
      loadBalance: true,
      useSocket: true,
      displayName: undefined,
      isMeta: false,
      port: parseInt(process.env.HTTP_PORT ?? "3000"),
      securityProfile:
        (process.env.SECURITY_PROFILE as SecurityProfile) ?? "medium",
      networkMode: (process.env.NETWORK_MODE as NetworkMode) ?? "dev",
      retryCount: 3,
      cadenzaDB: {
        connect: true,
        address: process.env.CADENZA_DB_ADDRESS ?? "localhost",
        port: parseInt(process.env.CADENZA_DB_PORT ?? "5000"),
      },
      relatedServices: process.env.RELATED_SERVICES
        ? process.env.RELATED_SERVICES.split("|").map((s) =>
            s.trim().split(","),
          )
        : [],
      isFrontend: isBrowser,
      ...options,
    };

    if (options.cadenzaDB?.connect) {
      this.emit("meta.initializing_service", {
        // Seed the CadenzaDB
        serviceInstance: {
          uuid: "cadenza-db",
          serviceName: "CadenzaDB",
          address: options.cadenzaDB?.address,
          port: options.cadenzaDB?.port,
          exposed: options.networkMode !== "dev",
          numberOfRunningGraphs: 0,
          isActive: true, // Assume it is deployed
          isNonResponsive: false,
          isBlocked: false,
          health: {},
        },
      });
    }

    options.relatedServices?.forEach((service) => {
      this.emit("meta.initializing_service", {
        serviceInstance: {
          uuid: service[0],
          serviceName: service[1],
          address: service[2].split(":")[0],
          port: service[2].split(":")[1] ?? 3000,
          exposed: options.networkMode !== "dev",
          numberOfRunningGraphs: 0,
          isActive: true, // Assume it is deployed
          isNonResponsive: false,
          isBlocked: false,
          health: {},
        },
      });
    });

    console.log("Creating service...");

    const initContext = {
      data: {
        name: serviceName,
        description: description,
        displayName: options.displayName ?? "",
        isMeta: options.isMeta,
      },
      __serviceName: serviceName,
      __serviceInstanceId: serviceId,
      __port: options.port,
      __loadBalance: options.loadBalance,
      __useSocket: options.useSocket,
      __securityProfile: options.securityProfile,
      __networkMode: options.networkMode,
      __retryCount: options.retryCount,
      __cadenzaDBConnect: options.cadenzaDB?.connect,
      __isDatabase: options.isDatabase,
    };

    if (options.cadenzaDB?.connect) {
      Cadenza.createEphemeralMetaTask("Create service", async (_, emit) => {
        emit("meta.create_service_requested", initContext);
      }).doOn("meta.fetch.handshake_complete");
    } else {
      this.emit("meta.create_service_requested", initContext);
    }

    this.createEphemeralMetaTask("Handle service setup completion", () => {
      GraphMetadataController.instance;
      GraphSyncController.instance;
      this.broker.schedule("meta.sync_requested", {}, 2000);

      if (options.cadenzaDB?.connect) {
        this.broker.throttle("meta.sync_requested", {}, 300000);
      }

      this.log("Service created.");

      return true;
    }).doOn("meta.service_registry.instance_inserted");

    this.serviceCreated = true;
  }

  /**
   * Creates a Cadenza metadata service with the specified name, description, and options.
   *
   * @param {string} serviceName - The name of the metadata service to be created.
   * @param {string} description - A brief description of the metadata service.
   * @param {ServerOptions} [options={}] - Optional configuration for the metadata service. Defaults to an empty object.
   * @return {void} Does not return a value.
   */
  static createCadenzaMetaService(
    serviceName: string,
    description: string,
    options: ServerOptions = {},
  ) {
    options.isMeta = true;
    this.createCadenzaService(serviceName, description, options);
  }

  /**
   * Creates and initializes a database service with the provided name, schema, and configuration options.
   * This method is not supported in a browser environment and will log a warning if called in such an environment.
   *
   * @param {string} name - The name of the database service to be created.
   * @param {SchemaDefinition} schema - The schema definition for the database service.
   * @param {string} [description=""] - An optional description of the database service.
   * @param {ServerOptions & DatabaseOptions} [options={}] - Optional configuration settings for the database and server.
   * @return {void} This method does not return a value.
   */
  static createDatabaseService(
    name: string,
    schema: SchemaDefinition,
    description: string = "",
    options: ServerOptions & DatabaseOptions = {},
  ) {
    if (isBrowser) {
      console.warn(
        "Database service creation is not supported in the browser. Use the CadenzaDB service instead.",
      );
      return;
    }
    if (this.serviceCreated) return;
    this.bootstrap();
    this.serviceRegistry.serviceName = name;
    DatabaseController.instance; // Ensure DB controller is created

    options = {
      loadBalance: true,
      useSocket: true,
      displayName: undefined,
      isMeta: false,
      port: parseInt(process.env.HTTP_PORT ?? "3000"),
      securityProfile:
        (process.env.SECURITY_PROFILE as SecurityProfile) ?? "medium",
      networkMode: (process.env.NETWORK_MODE as NetworkMode) ?? "dev",
      retryCount: 3,
      cadenzaDB: {
        connect: true,
        address: process.env.CADENZA_DB_ADDRESS ?? "localhost",
        port: parseInt(process.env.CADENZA_DB_PORT ?? "5000"),
      },
      databaseType: "postgres",
      databaseName: snakeCase(name),
      poolSize: parseInt(process.env.DATABASE_POOL_SIZE ?? "10"),
      isDatabase: true,
      ...options,
    };

    this.emit("meta.database_init_requested", {
      schema,
      databaseName: options.databaseName,
      options,
    });

    Cadenza.createEphemeralMetaTask("Set database connection", () => {
      if (options.cadenzaDB?.connect) {
        Cadenza.createEphemeralMetaTask(
          "Insert database service",
          (_, emit) => {
            emit("meta.created_database_service", {
              data: {
                service_name: name,
                description,
                schema,
                is_meta: options.isMeta,
              },
            });
            this.log("Database service created", {
              name,
              isMeta: options.isMeta,
            });
          },
        ).doOn("meta.service_registry.service_inserted");
      } else {
        this.emit("meta.created_database_service", {
          data: {
            service_name: name,
            description,
            schema,
            is_meta: options.isMeta,
          },
        });
        this.log("Database service created", {
          name,
          isMeta: options.isMeta,
        });
      }

      this.createCadenzaService(name, description, options);
    }).doOn("meta.database.setup_done");
  }

  /**
   * Creates a meta database service with the specified configuration.
   *
   * @param {string} name - The name of the database service to be created.
   * @param {SchemaDefinition} schema - The schema definition for the database.
   * @param {string} [description=""] - An optional description of the database service.
   * @param {ServerOptions & DatabaseOptions} [options={}] - Optional server and database configuration options. The `isMeta` flag will be automatically set to true.
   * @return {void} - This method does not return a value.
   */
  static createMetaDatabaseService(
    name: string,
    schema: SchemaDefinition,
    description: string = "",
    options: ServerOptions & DatabaseOptions = {},
  ) {
    this.bootstrap();
    options.isMeta = true;
    this.createDatabaseService(name, schema, description, options);
  }

  /**
   * Creates and registers a new task with the provided name, function, and optional details.
   *
   * @param {string} name - The name of the task to be created.
   * @param {TaskFunction} func - The function that contains the task execution logic.
   * @param {string} [description] - An optional description of what the task does.
   * @param {TaskOptions} [options={}] - An optional configuration object specifying additional task options.
   * @return {Task} - The created task instance.
   *
   * @example
   * You can use arrow functions to create tasks.
   * ```ts
   * const task = Cadenza.createTask('My task', (ctx) => {
   *   console.log('My task executed with context:', ctx);
   * }, 'My task description');
   * ```
   *
   * You can also use named functions to create tasks.
   * This is the preferred way to create tasks since it allows for code inspection in the CadenzaUI.
   * ```ts
   * function myTask(ctx) {
   *   console.log('My task executed with context:', ctx);
   * }
   *
   * const task = Cadenza.createTask('My task', myTask);
   * ```
   *
   * ** Use the TaskOptions object to configure the task. **
   *
   * With concurrency limit, timeout limit and retry settings.
   * ```ts
   * Cadenza.createTask('My task', (ctx) => {
   *   console.log('My task executed with context:', ctx);
   * }, 'My task description', {
   *   concurrency: 10,
   *   timeout: 10000,
   *   retryCount: 3,
   *   retryDelay: 1000,
   *   retryDelayFactor: 1.5,
   * });
   * ```
   *
   * You can specify the input and output context schemas for the task.
   * ```ts
   * Cadenza.createTask('My task', (ctx) => {
   *   return { bar: 'foo' + ctx.foo };
   * }, 'My task description', {
   *   inputContextSchema: {
   *     type: 'object',
   *     properties: {
   *       foo: {
   *         type: 'string',
   *       },
   *     },
   *   required: ['foo'],
   *   },
   *   validateInputContext: true, // default is false
   *   outputContextSchema: {
   *     type: 'object',
   *     properties: {
   *       bar: {
   *         type: 'string',
   *       },
   *     },
   *     required: ['bar'],
   *   },
   *   validateOutputContext: true, // default is false
   * });
   * ```
   */
  static createTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createTask(name, func, description, options);
  }

  /**
   * Creates a meta task with the specified name, functionality, description, and options.
   * This is used for creating tasks that lives on the meta layer.
   * The meta layer is a special layer that is executed separately from the business logic layer and is used for extending Cadenzas core functionality.
   * See {@link Task} or {@link createTask} for more information.
   *
   * @param {string} name - The name of the meta task.
   * @param {TaskFunction} func - The function to be executed by the meta task.
   * @param {string} [description] - An optional description of the meta task.
   * @param {TaskOptions} [options={}] - Additional optional task configuration. Automatically sets `isMeta` to true.
   * @return {Task} A task instance configured as a meta task.
   */
  static createMetaTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createMetaTask(name, func, description, options);
  }

  /**
   * Creates a unique task by wrapping the provided task function with a uniqueness constraint.
   * Unique tasks are designed to execute once per execution ID, merging parents. This is useful for
   * tasks that require fan-in/joins after parallel branches.
   * See {@link Task} for more information.
   * @param {string} name Unique identifier.
   * @param {TaskFunction} func Function receiving joinedContexts as a list (context.joinedContexts).
   * @param {string} [description] Optional description.
   * @param {TaskOptions} [options={}] Optional task options.
   * @returns {Task} The created UniqueTask.
   *
   * @example
   * ```ts
   * const splitTask = Cadenza.createTask('Split foos', function* (ctx) {
   *   for (const foo of ctx.foos) {
   *     yield { foo };
   *   }
   * }, 'Splits a list of foos into multiple sub-branches');
   *
   * const processTask = Cadenza.createTask('Process foo', (ctx) => {
   *  return { bar: 'foo' + ctx.foo };
   * }, 'Process a foo');
   *
   * const uniqueTask = Cadenza.createUniqueTask('Gather processed foos', (ctx) => {
   *   // A unique task will always be provided with a list of contexts (ctx.joinedContexts) from its predecessors.
   *   const processedFoos = ctx.joinedContexts.map((c) => c.bar);
   *   return { foos: processedFoos };
   * }, 'Gathers together the processed foos.');
   *
   * splitTask.then(
   *   processTask.then(
   *     uniqueTask,
   *   ),
   * );
   *
   * // Give the flow a name using a routine
   * Cadenza.createRoutine(
   *   'Process foos',
   *   [splitTask],
   *   'Processes a list of foos'
   * ).doOn('main.received_foos'); // Subscribe to a signal
   *
   * // Trigger the flow from anywhere
   * Cadenza.emit('main.received_foos', { foos: ['foo1', 'foo2', 'foo3'] });
   * ```
   */
  static createUniqueTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createUniqueTask(name, func, description, options);
  }

  /**
   * Creates a unique meta task with the specified name, function, description, and options.
   * See {@link createUniqueTask} and {@link createMetaTask} for more information.
   *
   * @param {string} name - The name of the task to create.
   * @param {TaskFunction} func - The function to execute when the task is run.
   * @param {string} [description] - An optional description of the task.
   * @param {TaskOptions} [options={}] - Optional settings for the task. Defaults to an empty object. Automatically sets `isMeta` and `isUnique` to true.
   * @return {Task} The created unique meta task.
   */
  static createUniqueMetaTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createUniqueMetaTask(name, func, description, options);
  }

  /**
   * Creates a throttled task with a concurrency limit of 1, ensuring that only one instance of the task can run at a time for a specific throttle tag.
   * This is useful for ensuring execution order and preventing race conditions.
   * See {@link Task} for more information.
   *
   * @param {string} name - The name of the task.
   * @param {TaskFunction} func - The function to be executed when the task runs.
   * @param {ThrottleTagGetter} [throttledIdGetter=() => "default"] - A function that generates a throttle tag identifier to group tasks for throttling.
   * @param {string} [description] - An optional description of the task.
   * @param {TaskOptions} [options={}] - Additional options to customize the task behavior.
   * @return {Task} The created throttled task.
   *
   * @example
   * ```ts
   * const task = Cadenza.createThrottledTask(
   *   'My task',
   *   async (ctx) => {
   *      await new Promise((resolve) => setTimeout(resolve, 1000));
   *      console.log('My task executed with context:', ctx);
   *   },
   *   // Will throttle by the value of ctx.foo to make sure tasks with the same value are executed sequentially
   *   (ctx) => ctx.foo,
   * );
   *
   * Cadenza.run(task, { foo: 'bar' }); // (First execution)
   * Cadenza.run(task, { foo: 'bar' }); // This will be executed after the first execution is finished
   * Cadenza.run(task, { foo: 'baz' }); // This will be executed in parallel with the first execution
   * ```
   */
  static createThrottledTask(
    name: string,
    func: TaskFunction,
    throttledIdGetter: ThrottleTagGetter = () => "default",
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createThrottledTask(
      name,
      func,
      throttledIdGetter,
      description,
      options,
    );
  }

  /**
   * Creates a throttled meta task with the specified configuration.
   * See {@link createThrottledTask} and {@link createMetaTask} for more information.
   *
   * @param {string} name - The name of the throttled meta task.
   * @param {TaskFunction} func - The task function to be executed.
   * @param {ThrottleTagGetter} throttledIdGetter - A function to retrieve the throttling identifier.
   * @param {string} [description] - An optional description of the task.
   * @param {TaskOptions} [options={}] - Additional options for configuring the task.
   * @return {Task} The created throttled meta task.
   */
  static createThrottledMetaTask(
    name: string,
    func: TaskFunction,
    throttledIdGetter: ThrottleTagGetter = () => "default",
    description?: string,
    options: TaskOptions = {},
  ): Task {
    this.bootstrap();
    return Cadenza.createThrottledMetaTask(
      name,
      func,
      throttledIdGetter,
      description,
      options,
    );
  }

  /**
   * Creates and returns a new debounced task with the specified parameters.
   * This is useful to prevent rapid execution of tasks that may be triggered by multiple events within a certain time frame.
   * See {@link DebounceTask} for more information.
   *
   * @param {string} name - The unique name of the task to be created.
   * @param {TaskFunction} func - The function to be executed by the task.
   * @param {string} [description] - An optional description of the task.
   * @param {number} [debounceTime=1000] - The debounce time in milliseconds to delay the execution of the task.
   * @param {TaskOptions & DebounceOptions} [options={}] - Additional configuration options for the task, including debounce behavior and other task properties.
   * @return {DebounceTask} A new instance of the DebounceTask with the specified configuration.
   *
   * @example
   * ```ts
   * const task = Cadenza.createDebounceTask(
   *   'My debounced task',
   *   (ctx) => {
   *      console.log('My task executed with context:', ctx);
   *   },
   *   'My debounced task description',
   *   100, // Debounce time in milliseconds. Default is 1000
   *   {
   *     leading: false, // Should the first execution of a burst be executed immediately? Default is false
   *     trailing: true, // Should the last execution of a burst be executed? Default is true
   *     maxWait: 1000, // Maximum time in milliseconds to wait for the next execution. Default is 0
   *   },
   * );
   *
   * Cadenza.run(task, { foo: 'bar' }); // This will not be executed
   * Cadenza.run(task, { foo: 'bar' }); // This will not be executed
   * Cadenza.run(task, { foo: 'baz' }); // This execution will be delayed by 100ms
   * ```
   */
  static createDebounceTask(
    name: string,
    func: TaskFunction,
    description?: string,
    debounceTime: number = 1000,
    options: TaskOptions & DebounceOptions = {},
  ): DebounceTask {
    this.bootstrap();
    return Cadenza.createDebounceTask(
      name,
      func,
      description,
      debounceTime,
      options,
    );
  }

  /**
   * Creates a debounced meta task with the specified parameters.
   * See {@link createDebounceTask} and {@link createMetaTask} for more information.
   *
   * @param {string} name - The name of the task.
   * @param {TaskFunction} func - The function to be executed by the task.
   * @param {string} [description] - Optional description of the task.
   * @param {number} [debounceTime=1000] - The debounce delay in milliseconds.
   * @param {TaskOptions & DebounceOptions} [options={}] - Additional configuration options for the task.
   * @return {DebounceTask} Returns an instance of the debounced meta task.
   */
  static createDebounceMetaTask(
    name: string,
    func: TaskFunction,
    description?: string,
    debounceTime: number = 1000,
    options: TaskOptions & DebounceOptions = {},
  ): DebounceTask {
    this.bootstrap();
    return Cadenza.createDebounceMetaTask(
      name,
      func,
      description,
      debounceTime,
      options,
    );
  }

  /**
   * Creates an ephemeral task with the specified configuration.
   * Ephemeral tasks are designed to self-destruct after execution or a certain condition is met.
   * This is useful for transient tasks such as resolving promises or performing cleanup operations.
   * They are not registered by default.
   * See {@link EphemeralTask} for more information.
   *
   * @param {string} name - The name of the task to be created.
   * @param {TaskFunction} func - The function that defines the logic of the task.
   * @param {string} [description] - An optional description of the task.
   * @param {TaskOptions & EphemeralTaskOptions} [options={}] - The configuration options for the task, including concurrency, timeouts, and retry policies.
   * @return {EphemeralTask} The created ephemeral task instance.
   *
   * @example
   * By default, ephemeral tasks are executed once and destroyed after execution.
   * ```ts
   * const task = Cadenza.createEphemeralTask('My ephemeral task', (ctx) => {
   *   console.log('My task executed with context:', ctx);
   * });
   *
   * Cadenza.run(task); // Executes the task once and destroys it after execution
   * Cadenza.run(task); // Does nothing, since the task is destroyed
   * ```
   *
   * Use destroy condition to conditionally destroy the task
   * ```ts
   * const task = Cadenza.createEphemeralTask(
   *   'My ephemeral task',
   *   (ctx) => {
   *      console.log('My task executed with context:', ctx);
   *   },
   *   'My ephemeral task description',
   *   {
   *     once: false, // Should the task be executed only once? Default is true
   *     destroyCondition: (ctx) => ctx.foo > 10, // Should the task be destroyed after execution? Default is undefined
   *   },
   * );
   *
   * Cadenza.run(task, { foo: 5 }); // The task will not be destroyed and can still be executed
   * Cadenza.run(task, { foo: 10 }); // The task will not be destroyed and can still be executed
   * Cadenza.run(task, { foo: 20 }); // The task will be destroyed after execution and cannot be executed anymore
   * Cadenza.run(task, { foo: 30 }); // This will not be executed
   * ```
   *
   * A practical use case for ephemeral tasks is to resolve a promise upon some external event.
   * ```ts
   * const task = Cadenza.createTask('Confirm something', (ctx, emit) => {
   *   return new Promise((resolve) => {
   *     ctx.foo = uuid();
   *
   *     Cadenza.createEphemeralTask(`Resolve promise of ${ctx.foo}`, (c) => {
   *       console.log('My task executed with context:', ctx);
   *       resolve(c);
   *     }).doOn(`socket.confirmation_received:${ctx.foo}`);
   *
   *     emit('this_domain.confirmation_requested', ctx);
   *   });
   * });
   * ```
   */
  static createEphemeralTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions & EphemeralTaskOptions = {},
  ): EphemeralTask {
    this.bootstrap();
    return Cadenza.createEphemeralTask(name, func, description, options);
  }

  /**
   * Creates an ephemeral meta task with the specified name, function, description, and options.
   * See {@link createEphemeralTask} and {@link createMetaTask} for more details.
   *
   * @param {string} name - The name of the task to be created.
   * @param {TaskFunction} func - The function to be executed as part of the task.
   * @param {string} [description] - An optional description of the task.
   * @param {TaskOptions & EphemeralTaskOptions} [options={}] - Additional options for configuring the task.
   * @return {EphemeralTask} The created ephemeral meta task.
   */
  static createEphemeralMetaTask(
    name: string,
    func: TaskFunction,
    description?: string,
    options: TaskOptions & EphemeralTaskOptions = {},
  ): EphemeralTask {
    this.bootstrap();
    return Cadenza.createEphemeralMetaTask(name, func, description, options);
  }

  /**
   * Creates a new routine with the specified name, tasks, and an optional description.
   * Routines are named entry points to starting tasks and are registered in the GraphRegistry.
   * They are used to group tasks together and provide a high-level structure for organizing and managing the execution of a set of tasks.
   * See {@link GraphRoutine} for more information.
   *
   * @param {string} name - The name of the routine to create.
   * @param {Task[]} tasks - A list of tasks to include in the routine.
   * @param {string} [description=""] - An optional description for the routine.
   * @return {GraphRoutine} A new instance of the GraphRoutine containing the specified tasks and description.
   *
   * @example
   * ```ts
   * const task1 = Cadenza.createTask("Task 1", () => {});
   * const task2 = Cadenza.createTask("Task 2", () => {});
   *
   * task1.then(task2);
   *
   * const routine = Cadenza.createRoutine("Some routine", [task1]);
   *
   * Cadenza.run(routine);
   *
   * // Or, routines can be triggered by signals
   * routine.doOn("some.signal");
   *
   * Cadenza.emit("some.signal", {});
   * ```
   */
  static createRoutine(
    name: string,
    tasks: Task[],
    description: string = "",
  ): GraphRoutine {
    this.bootstrap();
    return Cadenza.createRoutine(name, tasks, description);
  }

  /**
   * Creates a meta routine with a given name, tasks, and optional description.
   * Routines are named entry points to starting tasks and are registered in the GraphRegistry.
   * They are used to group tasks together and provide a high-level structure for organizing and managing the execution of a set of tasks.
   * See {@link GraphRoutine} and {@link createRoutine} for more information.
   *
   * @param {string} name - The name of the routine to be created.
   * @param {Task[]} tasks - An array of tasks that the routine will consist of.
   * @param {string} [description=""] - An optional description for the routine.
   * @return {GraphRoutine} A new instance of the `GraphRoutine` representing the created routine.
   */
  static createMetaRoutine(
    name: string,
    tasks: Task[],
    description: string = "",
  ): GraphRoutine {
    this.bootstrap();
    return Cadenza.createMetaRoutine(name, tasks, description);
  }

  static reset() {
    Cadenza.reset();
    this.serviceRegistry.reset();
  }
}
