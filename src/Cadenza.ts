import Cadenza, {
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
import TaskController from "./graph/controllers/TaskController";
import { SchemaDefinition } from "./types/database";
import { snakeCase } from "lodash-es";
import DatabaseController from "./database/DatabaseController";

export type SecurityProfile = "low" | "medium" | "high";
export type NetworkMode =
  | "internal"
  | "exposed"
  | "exposed-high-sec"
  | "auto"
  | "dev";

export type ServerOptions = {
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
};

export interface DatabaseOptions {
  type?: "postgres";
  databaseName?: string;
  poolSize?: number;
}

export default class CadenzaService {
  public static broker: SignalBroker;
  public static runner: GraphRunner;
  public static metaRunner: GraphRunner;
  public static registry: GraphRegistry;
  public static serviceRegistry: ServiceRegistry;
  protected static isBootstrapped = false;
  protected static serviceCreated = false;

  static bootstrap(): void {
    if (this.isBootstrapped) return;
    this.isBootstrapped = true;

    Cadenza.bootstrap();
    this.broker = Cadenza.broker;
    this.runner = Cadenza.runner;
    this.metaRunner = Cadenza.metaRunner;
    this.registry = Cadenza.registry;
    SignalController.instance;
    TaskController.instance;
    this.serviceRegistry = ServiceRegistry.instance;
    RestController.instance;
    SocketController.instance;
    console.log("BOOTSTRAPPED");
  }

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

  protected static validateName(name: string): void {
    Cadenza.validateName(name);
  }

  public static get runStrategy() {
    return Cadenza.runStrategy;
  }

  public static setMode(mode: CadenzaMode) {
    Cadenza.setMode(mode);
  }

  static createDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    options: TaskOptions = {},
  ): DeputyTask {
    this.bootstrap();
    this.validateName(routineName);
    const name = `Deputy task for "${routineName}"`;

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

  static createMetaDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    options: TaskOptions = {},
  ): DeputyTask {
    options.isMeta = true;
    return this.createDeputyTask(routineName, serviceName, options);
  }

  static createUniqueDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    options: TaskOptions = {},
  ) {
    options.isUnique = true;
    return this.createDeputyTask(routineName, serviceName, options);
  }

  static createUniqueMetaDeputyTask(
    routineName: string,
    serviceName: string | undefined = undefined,
    options: TaskOptions = {},
  ) {
    options.isMeta = true;
    return this.createUniqueDeputyTask(routineName, serviceName, options);
  }

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

    const name = `SignalTransmission task for "${signalName}"`;
    return new SignalTransmissionTask(
      name,
      signalName,
      serviceName,
      `Transmits signal "${signalName}" to service "${serviceName}"`,
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
    const description = `Executes a database "${operation}" on table "${tableName}" in ${databaseServiceName ?? "default database service"}`;
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

  static createDatabaseInertTask(
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

  static createCadenzaDBInsertTask(
    tableName: string,
    queryData: DbOperationPayload = {},
    options: TaskOptions = {},
  ) {
    options.isMeta = true;
    return this.createDatabaseInertTask(
      tableName,
      "CadenzaDB",
      queryData,
      options,
    );
  }

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
   * Creates a MetaTask (for meta-layer graphs) and registers it.
   * MetaTasks suppress further meta-signal emissions to prevent loops.
   * @param serviceName Unique identifier for the meta-task.
   * @param description Optional description.
   * @param options Optional service options. A service can either be connected to a database service and/or a list of related services.
   * Example RELATED_SERVICES=serviceId123,service1,http://address:port | serviceId124,service2,http://address:port
   * @returns The created MetaTask instance.
   * @throws Error if name invalid or duplicate.
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
      ...options,
    };

    if (options.cadenzaDB?.connect) {
      Cadenza.broker.emit("meta.initializing_service", {
        // Seed the CadenzaDB
        serviceInstance: {
          id: "cadenza-db",
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
      Cadenza.broker.emit("meta.initializing_service", {
        serviceInstance: {
          id: service[0],
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

    Cadenza.broker.emit("meta.create_service_requested", {
      data: {
        name: serviceName,
        description: description,
        displayName: options.displayName,
        isMeta: options.isMeta,
      },
      __serviceName: serviceName,
      __port: options.port,
      __loadBalance: options.loadBalance,
      __useSocket: options.useSocket,
      __securityProfile: options.securityProfile,
      __networkMode: options.networkMode,
      __retryCount: options.retryCount,
      __cadenzaDBConnect: options.cadenzaDB?.connect,
    });

    this.serviceCreated = true;
  }

  static createCadenzaMetaService(
    serviceName: string,
    description: string,
    options: ServerOptions = {},
  ) {
    options.isMeta = true;
    this.createCadenzaService(serviceName, description, options);
  }

  static createDatabaseService(
    name: string,
    schema: SchemaDefinition,
    description: string = "",
    options: ServerOptions & DatabaseOptions = {},
  ) {
    if (this.serviceCreated) return;
    this.bootstrap();
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
      type: "postgres",
      databaseName: snakeCase(name),
      poolSize: parseInt(process.env.DATABASE_POOL_SIZE ?? "10"),
      ...options,
    };

    Cadenza.broker.emit("meta.database_init_requested", {
      schema,
      databaseName: options.databaseName,
    });

    Cadenza.createEphemeralMetaTask("Set database connection", () => {
      this.createCadenzaService(name, description, options);
    }).doOn("meta.database.setup_done");
  }

  static createMetaDatabaseService(
    name: string,
    schema: SchemaDefinition,
    description: string = "",
    options: ServerOptions = {},
  ) {
    this.bootstrap();
    options.isMeta = true;
    this.createDatabaseService(name, schema, description, options);
  }

  /**
   * Creates a standard Task and registers it in the GraphRegistry.
   * @param name Unique identifier for the task.
   * @param func The function or async generator to execute.
   * @param description Optional human-readable description for introspection.
   * @param options Optional task options.
   * @returns The created Task instance.
   * @throws Error if name is invalid or duplicate in registry.
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
   * Creates a MetaTask (for meta-layer graphs) and registers it.
   * MetaTasks suppress further meta-signal emissions to prevent loops.
   * @param name Unique identifier for the meta-task.
   * @param func The function or async generator to execute.
   * @param description Optional description.
   * @param options Optional task options.
   * @returns The created MetaTask instance.
   * @throws Error if name invalid or duplicate.
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
   * Creates a UniqueTask (executes once per execution ID, merging parents) and registers it.
   * Use for fan-in/joins after parallel branches.
   * @param name Unique identifier.
   * @param func Function receiving joinedContexts.
   * @param description Optional description.
   * @param options Optional task options.
   * @returns The created UniqueTask.
   * @throws Error if invalid.
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
   * Creates a UniqueMetaTask for meta-layer joins.
   * @param name Unique identifier.
   * @param func Function.
   * @param description Optional.
   * @param options Optional task options.
   * @returns The created UniqueMetaTask.
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
   * Creates a ThrottledTask (rate-limited by concurrency or custom groups) and registers it.
   * @param name Unique identifier.
   * @param func Function.
   * @param throttledIdGetter Optional getter for dynamic grouping (e.g., per-user).
   * @param description Optional.
   * @param options Optional task options.
   * @returns The created ThrottledTask.
   * @edge If no getter, throttles per task ID; use for resource protection.
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
   * Creates a ThrottledMetaTask for meta-layer throttling.
   * @param name Identifier.
   * @param func Function.
   * @param throttledIdGetter Optional getter.
   * @param description Optional.
   * @param options Optional task options.
   * @returns The created ThrottledMetaTask.
   */
  static createThrottledMetaTask(
    name: string,
    func: TaskFunction,
    throttledIdGetter: ThrottleTagGetter,
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
   * Creates a DebounceTask (delays exec until quiet period) and registers it.
   * @param name Identifier.
   * @param func Function.
   * @param description Optional.
   * @param debounceTime Delay in ms (default 1000).
   * @param options Optional task options plus optional debounce config (e.g., leading/trailing).
   * @returns The created DebounceTask.
   * @edge Multiple triggers within time collapse to one exec.
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
   * Creates a DebouncedMetaTask for meta-layer debouncing.
   * @param name Identifier.
   * @param func Function.
   * @param description Optional.
   * @param debounceTime Delay in ms.
   * @param options Optional task options plus optional debounce config (e.g., leading/trailing).
   * @returns The created DebouncedMetaTask.
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
   * Creates an EphemeralTask (self-destructs after exec or condition) without default registration.
   * Useful for transients; optionally register if needed.
   * @param name Identifier (may not be unique if not registered).
   * @param func Function.
   * @param description Optional.
   * @param options Optional task options.
   * @returns The created EphemeralTask.
   * @edge Destruction triggered post-exec via Node/Builder; emits meta-signal for cleanup.
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
   * Creates an EphemeralMetaTask for meta-layer transients.
   * @param name Identifier.
   * @param func Function.
   * @param description Optional.
   * @param options Optional task options.
   * @returns The created EphemeralMetaTask.
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
   * Creates a GraphRoutine (named entry to starting tasks) and registers it.
   * @param name Unique identifier.
   * @param tasks Starting tasks (can be empty, but warns as no-op).
   * @param description Optional.
   * @returns The created GraphRoutine.
   * @edge If tasks empty, routine is valid but inert.
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
   * Creates a MetaRoutine for meta-layer entry points.
   * @param name Identifier.
   * @param tasks Starting tasks.
   * @param description Optional.
   * @returns The created MetaRoutine.
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
