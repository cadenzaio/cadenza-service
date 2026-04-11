import { META_ACTOR_SESSION_STATE_PERSIST_INTENT } from "@cadenza.io/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import Cadenza from "../src/Cadenza";
import DatabaseController from "@service-database-controller";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import GraphSyncController from "../src/graph/controllers/GraphSyncController";
import RestController from "../src/network/RestController";
import ServiceRegistry from "../src/registry/ServiceRegistry";
import SignalController from "../src/signals/SignalController";
import SocketController from "../src/network/SocketController";

async function waitForCondition(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 1_000,
  pollIntervalMs = 10,
): Promise<void> {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    if (await predicate()) {
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }

  throw new Error("Condition not met within timeout");
}

function resetRuntimeState() {
  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run resets before bootstrap.
  }

  (DatabaseController as any)._instance = undefined;
  (GraphMetadataController as any)._instance = undefined;
  (GraphSyncController as any)._instance = undefined;
  (RestController as any)._instance = undefined;
  (ServiceRegistry as any)._instance = undefined;
  (SignalController as any)._instance = undefined;
  (SocketController as any)._instance = undefined;
}

function getInsertRow(ctx: Record<string, any>): Record<string, any> {
  return (ctx.data ?? ctx.queryData?.data ?? {}) as Record<string, any>;
}

function createLocalAuthorityInsertTask(
  tableName: string,
  handler: (ctx: Record<string, any>) => Record<string, any>,
) {
  return Cadenza.createMetaTask(`Insert ${tableName}`, handler).respondsTo(
    `insert-pg-cadenza-db-postgres-actor-${tableName}`,
  );
}

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

describe("graph sync authority rows", () => {
  let consoleLogSpy: ReturnType<typeof vi.spyOn>;
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    consoleWarnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    process.env.HTTP_PORT = "0";
    resetRuntimeState();
  });

  afterEach(() => {
    resetRuntimeState();
    consoleLogSpy.mockRestore();
    consoleWarnSpy.mockRestore();
    consoleErrorSpy.mockRestore();
    vi.restoreAllMocks();
  });

  it("does not persist deputy helper relationships in directional task maps", async () => {
    const directionalRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("directional_task_graph_map", (ctx) => {
      directionalRows.push(getInsertRow(ctx));
      return ctx;
    });

    const databaseTask = Cadenza.createDatabaseInsertTask("health_metric");
    ServiceRegistry.instance.serviceName = "DiagnosticsService";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    expect(GraphSyncController.instance.registerDeputyRelationshipTask).toBeUndefined();

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(directionalRows).toHaveLength(0);
  });

  it("skips ordinary directional task-map registration for deputy tasks", async () => {
    const directionalRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("directional_task_graph_map", (ctx) => {
      directionalRows.push(getInsertRow(ctx));
      return ctx;
    });

    const databaseTask = Cadenza.createDatabaseInsertTask("health_metric");
    ServiceRegistry.instance.serviceName = "DiagnosticsService";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.registerTaskMapTask!, {
      __syncing: true,
      task: databaseTask,
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(directionalRows).toHaveLength(0);
  });

  it("skips directional task-map registration when the predecessor task is not registered", async () => {
    const directionalRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("directional_task_graph_map", (ctx) => {
      directionalRows.push(getInsertRow(ctx));
      return ctx;
    });

    const predecessorTask = Cadenza.createMetaTask(
      "Unregistered predecessor task",
      () => true,
    );
    const successorTask = Cadenza.createMetaTask(
      "Registered successor task",
      () => true,
    );
    predecessorTask.then(successorTask);
    successorTask.registered = true;

    ServiceRegistry.instance.serviceName = "DiagnosticsService";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.registerTaskMapTask!, {
      __syncing: true,
      task: predecessorTask,
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(directionalRows).toHaveLength(0);
  });

  it("ignores deferred local meta directional maps during convergence", async () => {
    const businessPredecessor = Cadenza.createTask(
      "Business predecessor task",
      () => true,
    );
    const businessSuccessor = Cadenza.createTask(
      "Business successor task",
      () => true,
    );
    const deferredMetaSuccessor = Cadenza.createMetaTask(
      "Deferred local meta successor task",
      () => true,
    );

    businessPredecessor.then(businessSuccessor);
    businessPredecessor.then(deferredMetaSuccessor);

    businessPredecessor.registered = true;
    businessSuccessor.registered = true;
    deferredMetaSuccessor.registered = true;

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.emit("meta.service_registry.initial_sync_complete", {
      directionalTaskMaps: [
        {
          predecessorTaskName: "Business predecessor task",
          predecessorTaskVersion: 1,
          predecessorServiceName: "OrdersApi",
          taskName: "Business successor task",
          taskVersion: 1,
          serviceName: "OrdersApi",
        },
      ],
    });

    await waitForCondition(() =>
      businessPredecessor.taskMapRegistration.has("Business successor task"),
    );

    GraphSyncController.instance.directionalTaskMapsSynced = false;
    Cadenza.run(Cadenza.get("Gather directional task map registration")!, {
      __syncing: true,
    });

    await waitForCondition(() => GraphSyncController.instance.directionalTaskMapsSynced);

    expect(GraphSyncController.instance.directionalTaskMapsSynced).toBe(true);
    expect(
      businessPredecessor.taskMapRegistration.has(
        "Deferred local meta successor task",
      ),
    ).toBe(false);
  });

  it("uses UUID transport ids for local routeable transports", async () => {
    const declaredTransports = (Cadenza as any).normalizeDeclaredTransports(
      [
        {
          role: "public",
          origin: "http://transport-api.localhost",
          protocols: ["rest", "socket"],
        },
      ],
      "transport-api-1",
    );

    expect(
      declaredTransports.some((transport: any) => transport.role === "public"),
    ).toBe(true);
    expect(
      declaredTransports.every((transport: any) =>
        UUID_PATTERN.test(transport.uuid),
      ),
    ).toBe(true);
  });

  it("yields compact task registration split payloads", () => {
    const splitPayloads: Array<Record<string, unknown>> = [];

    const task = Cadenza.createMetaTask("Register compact task", () => true);

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const splitTask = GraphSyncController.instance.splitTasksForRegistration as any;
    for (const payload of splitTask.taskFunction({
        __syncing: true,
        tasks: [task],
        intents: [{ name: "should-not-leak" }],
        signals: [{ signal: "should.not.leak", data: {} }],
      }) as Iterable<Record<string, unknown>>) {
      splitPayloads.push(payload);
    }

    expect(splitPayloads[0]).toMatchObject({
      __syncing: true,
      __taskName: "Register compact task",
    });
    expect(splitPayloads[0].data).toMatchObject({
      name: "Register compact task",
      service_name: "OrdersApi",
      function_string: expect.any(String),
      tag_id_getter: expect.any(String),
      is_meta: true,
      is_hidden: false,
      validate_input_context: false,
      validate_output_context: false,
    });
    expect(splitPayloads[0]).not.toHaveProperty("tasks");
    expect(splitPayloads[0]).not.toHaveProperty("intents");
    expect(splitPayloads[0]).not.toHaveProperty("signals");
  });

  it("retries CadenzaDB sync init until local authority insert tasks exist", async () => {
    const originalGetLocalInsertTask =
      Cadenza.getLocalCadenzaDBInsertTask.bind(Cadenza);
    let localTasksAvailable = false;

    vi.spyOn(Cadenza, "getLocalCadenzaDBInsertTask").mockImplementation(
      (tableName: string) =>
        localTasksAvailable ? originalGetLocalInsertTask(tableName) : undefined,
    );

    ServiceRegistry.instance.serviceName = "CadenzaDB";
    (Cadenza as any).serviceRegistry = ServiceRegistry.instance;
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    expect(GraphSyncController.instance.splitIntentsTask).toBeUndefined();

    for (const tableName of [
      "intent_registry",
      "routine",
      "task_to_routine_map",
      "signal_registry",
      "task",
      "actor",
      "directional_task_graph_map",
    ]) {
      Cadenza.createMetaTask(`Insert ${tableName}`, (ctx) => ctx);
    }

    localTasksAvailable = true;
    Cadenza.emit("meta.sync_controller.init_retry", {});

    await waitForCondition(
      () => GraphSyncController.instance.splitIntentsTask !== undefined,
      1_500,
    );

    expect(GraphSyncController.instance.splitTasksInRoutines).toBeDefined();
    expect(GraphSyncController.instance.registerSignalToTaskMapTask).toBeDefined();
    expect(GraphSyncController.instance.registerIntentToTaskMapTask).toBeDefined();
  });

  it("keeps local authority inserts in the same graph instead of routing through synthetic signals", () => {
    Cadenza.createMetaTask("Insert task", (ctx) => ctx);

    ServiceRegistry.instance.serviceName = "CadenzaDB";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const executionRequestedObserver = (Cadenza.signalBroker as any).signalObservers.get(
      "meta.sync_controller.insert_execution_requested:task",
    );

    expect(executionRequestedObserver).toBeUndefined();
  });

  it("keeps signal-to-task sync in the same graph instead of routing through split signals", () => {
    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const splitSignalObserver = (Cadenza.signalBroker as any).signalObservers.get(
      "meta.sync_controller.signal_task_map_split",
    );

    expect(splitSignalObserver).toBeUndefined();
  });

  it("keeps intent-to-task sync in the same graph instead of routing through split signals", () => {
    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const splitSignalObserver = (Cadenza.signalBroker as any).signalObservers.get(
      "meta.sync_controller.intent_task_map_split",
    );

    expect(splitSignalObserver).toBeUndefined();
  });

  it("schedules an early sync tick burst after local instance registration", () => {
    const scheduleSpy = vi.spyOn(Cadenza, "schedule");

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();

    Cadenza.run(
      Cadenza.get("Request sync after local service instance registration")!,
      {
        __syncing: true,
      },
    );

    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.sync_controller.sync_tick",
      { __syncing: true },
      400,
    );
    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.sync_controller.sync_tick",
      { __syncing: true },
      16000,
    );
    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.sync_controller.sync_tick",
      { __syncing: true },
      32000,
    );
    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.sync_controller.sync_tick",
      { __syncing: true },
      48000,
    );
  });

  it("wires meta.sync_requested to the bootstrap sync root", () => {
    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();

    const syncRequestedObserver = (Cadenza.signalBroker as any).signalObservers.get(
      "meta.sync_requested",
    );
    const taskNames = Array.from(syncRequestedObserver?.tasks ?? []).map(
      (task: any) => task.name as string,
    );

    expect(taskNames).toContain("Start bootstrap graph sync");
  });

  it("ignores bootstrap full-sync retry requests in the graph-sync root", () => {
    const debounceSpy = vi.spyOn(Cadenza, "debounce");

    ServiceRegistry.instance.serviceName = "OrdersApi";
    ServiceRegistry.instance.serviceInstanceId = "orders-api-instance";
    vi.spyOn(ServiceRegistry.instance, "hasLocalInstanceRegistered").mockReturnValue(
      true,
    );
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.localServiceInserted = true;
    GraphSyncController.instance.localServiceInstanceInserted = true;
    GraphSyncController.instance.init();

    Cadenza.run(Cadenza.get("Start bootstrap graph sync")!, {
      __bootstrapFullSync: true,
      __bootstrapFullSyncAttempt: 2,
      __reason: "service_registry_bootstrap_retry",
    });

    expect(debounceSpy).not.toHaveBeenCalledWith(
      "meta.sync_controller.synced_resource",
      expect.anything(),
    );
  });

  it("reruns pending graph sync through the dedicated sync tick signal", () => {
    const debounceSpy = vi.spyOn(Cadenza, "debounce");

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();
    Cadenza.run(Cadenza.get("Request next sync cycle")!, {});

    expect(debounceSpy).toHaveBeenCalledWith(
      "meta.sync_controller.sync_tick",
      {},
      100,
    );
  });

  it("does not mark intent definitions as registered when delegated inserts have zero responders", async () => {
    vi.spyOn(Cadenza, "getLocalCadenzaDBInsertTask").mockReturnValue(undefined);
    vi.spyOn(Cadenza, "getLocalCadenzaDBQueryTask").mockReturnValue(undefined);
    vi.spyOn(Cadenza, "createCadenzaDBQueryTask").mockImplementation((tableName) =>
      Cadenza.createMetaTask(
        `Delegated query ${tableName}`,
        (ctx) => ({
          ...ctx,
          __success: true,
        }),
        "",
        {
          register: false,
          isHidden: true,
          isMeta: true,
        },
      ),
    );
    vi.spyOn(Cadenza, "createCadenzaDBInsertTask").mockImplementation(() =>
      Cadenza.createMetaTask(
        "Delegated zero-responder intent insert",
        (ctx) => ({
          ...ctx,
          __success: true,
          __inquiryMeta: {
            inquiry: "insert-intent-registry",
            eligibleResponders: 0,
            responded: 0,
            failed: 0,
            timedOut: 0,
            pending: 0,
          },
        }),
        "Simulates a delegated intent insert with zero responders.",
        {
          register: false,
          isHidden: true,
        },
      ) as any,
    );

    Cadenza.createTask("Lookup missing responders", () => ({ ok: true })).respondsTo(
      "orders-missing-responders",
    );

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitIntentsTask!, {
      __syncing: true,
      intents: Array.from(Cadenza.inquiryBroker.intents.values()),
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(
      GraphSyncController.instance.registeredIntentDefinitions.has(
        "orders-missing-responders",
      ),
    ).toBe(false);
  });

  it("routes local sync inserts through the same graph so queryData survives", async () => {
    let capturedQueryData: Record<string, unknown> | undefined;

    createLocalAuthorityInsertTask("signal_registry", (ctx) => {
      capturedQueryData = (ctx.queryData as Record<string, unknown>) ?? undefined;
      return {
        ...ctx,
        __success: true,
        __syncing: true,
        __taskName: "Insert signal_registry",
        __signal: "orders.updated",
      };
    });

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitSignalsTask!, {
      __syncing: true,
      signals: [
        {
          signal: "global.orders.updated",
          data: { registered: false },
        },
      ],
    });

    await waitForCondition(() => capturedQueryData !== undefined, 1_500);

    expect(capturedQueryData).toMatchObject({
      onConflict: {
        target: ["name"],
        action: {
          do: "nothing",
        },
      },
    });
  });

  it("ignores the current service ready signal during bootstrap signal sync", async () => {
    const signalRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("signal_registry", (ctx) => {
      signalRows.push(getInsertRow(ctx));
      return {
        ...ctx,
        __success: true,
      };
    });

    Cadenza.createCadenzaService("OrdersApi", "Orders service", {
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "orders-api-1",
    });

    const readySignalName = Cadenza.getServiceReadySignalName("OrdersApi");
    Cadenza.createTask("Observe orders ready locally", () => true).doOn(readySignalName);

    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitSignalsTask!, {
      __syncing: true,
      signals: [
        {
          signal: readySignalName,
          data: { registered: false },
        },
      ],
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(signalRows.some((row) => row.name === readySignalName)).toBe(false);
  });

  it("does not crash task registration when the local task is not yet available", async () => {
    createLocalAuthorityInsertTask("task", (ctx) => ({
      ...ctx,
      __success: true,
    }));

    const originalGet = Cadenza.get.bind(Cadenza);
    const task = Cadenza.createMetaTask("Late sync task", () => true);

    vi.spyOn(Cadenza, "get").mockImplementation((taskName: string) =>
      taskName === "Late sync task" ? undefined : originalGet(taskName),
    );

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitTasksForRegistration!, {
      __syncing: true,
      tasks: [task],
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(task.registered).toBe(false);
  });

  it("includes task intent snapshots in task registration rows", async () => {
    let insertedTaskRow: Record<string, unknown> | undefined;

    createLocalAuthorityInsertTask("task", (ctx) => {
      insertedTaskRow = getInsertRow(ctx);
      return {
        ...ctx,
        __success: true,
      };
    });

    const task = Cadenza.createMetaTask("Sync task intent snapshot", () => true)
      .doOn("global.orders.updated")
      .respondsTo("orders-sync");

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitTasksForRegistration!, {
      __syncing: true,
      tasks: [task],
    });

    await waitForCondition(() => insertedTaskRow !== undefined, 1_500);

    expect(insertedTaskRow).toMatchObject({
      name: "Sync task intent snapshot",
      service_name: "OrdersApi",
      function_string: expect.any(String),
      tag_id_getter: expect.any(String),
      is_meta: true,
      signals: {
        observed: ["global.orders.updated"],
      },
      intents: ["orders-sync"],
    });
  });

  it("does not crash routine registration when the local routine is not yet available", async () => {
    createLocalAuthorityInsertTask("routine", (ctx) => ({
      ...ctx,
      __success: true,
    }));

    const originalGetRoutine = Cadenza.getRoutine.bind(Cadenza);
    const routine = {
      name: "Late sync routine",
      version: 1,
      description: "",
      isMeta: true,
      registered: false,
      tasks: [],
      registeredTasks: new Set<string>(),
    };

    vi.spyOn(Cadenza, "getRoutine").mockImplementation((routineName: string) =>
      routineName === "Late sync routine" ? undefined : originalGetRoutine(routineName),
    );

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitRoutinesTask!, {
      __syncing: true,
      routines: [routine],
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(routine.registered).toBe(false);
  });

  it("syncs routine rows with snake_case service metadata", async () => {
    const routineRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("routine", (ctx) => {
      routineRows.push(getInsertRow(ctx));
      return {
        ...ctx,
        __success: true,
      };
    });

    const routine = {
      name: "Sync contracts",
      version: 1,
      description: "Sync contract state",
      isMeta: true,
      registered: false,
      tasks: [],
      registeredTasks: new Set<string>(),
    };

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitRoutinesTask!, {
      __syncing: true,
      routines: [routine],
    });

    await waitForCondition(() => routineRows.length === 1, 1_500);

    expect(routineRows[0]).toMatchObject({
      name: "Sync contracts",
      version: 1,
      description: "Sync contract state",
      service_name: "OrdersApi",
      is_meta: true,
    });
  });

  it("falls back to data.name when signal registration loses __signal", async () => {
    createLocalAuthorityInsertTask("signal_registry", (ctx) => ({
      ...ctx,
      __success: true,
      __signal: undefined,
      data: {
        name: "global.orders.updated",
      },
    }));

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitSignalsTask!, {
      __syncing: true,
      signals: [
        {
          signal: "global.orders.updated",
          data: { registered: false },
        },
      ],
    });

    await waitForCondition(
      () =>
        (Cadenza.signalBroker as any).signalObservers?.get("global.orders.updated")
          ?.registered === true,
      1_500,
    );

    expect(
      (Cadenza.signalBroker as any).signalObservers?.get(
        "global.orders.updated",
      ),
    ).toMatchObject({
      registered: true,
    });
  });

  it("stops signal registration fan-out when no signal name can be resolved", async () => {
    const addSignalSpy = vi.spyOn(Cadenza.signalBroker, "addSignal");

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();
    const baselineCalls = addSignalSpy.mock.calls.length;

    const processSignalRegistrationTask = Cadenza.get(
      "Process signal registration",
    ) as any;

    Cadenza.run(processSignalRegistrationTask, {
      __success: true,
      rowCount: 1,
      queryData: {
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    const postInitCalls = addSignalSpy.mock.calls.slice(baselineCalls);
    expect(postInitCalls.some(([signalName]) => signalName === undefined)).toBe(false);
  });

  it("skips task-to-routine sync until both routines and tasks are registered", async () => {
    const taskToRoutineRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("task_to_routine_map", (ctx) => {
      taskToRoutineRows.push(getInsertRow(ctx));
      return ctx;
    });

    const routineTask = Cadenza.createMetaTask("Sync routine dependency", () => true);
    const syncRoutine = {
      name: "Sync services",
      version: 1,
      registered: false,
      tasks: [routineTask],
      registeredTasks: new Set<string>(),
    };

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitTasksInRoutines!, {
      __syncing: true,
      routines: [syncRoutine],
    });
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(taskToRoutineRows).toHaveLength(0);

    syncRoutine.registered = true;
    routineTask.registered = true;

    Cadenza.run(GraphSyncController.instance.splitTasksInRoutines!, {
      __syncing: true,
      routines: [syncRoutine],
    });

    await waitForCondition(() => taskToRoutineRows.length > 0, 1_500);

    expect(taskToRoutineRows.length).toBeGreaterThan(0);
    expect(
      taskToRoutineRows.every((row) =>
        row.task_name === "Sync routine dependency" &&
        row.routine_name === "Sync services" &&
        row.service_name === "OrdersApi",
      ),
    ).toBe(true);
  });

  it("defers signal-to-task structural maps to manifest sync", async () => {
    const observedTask = Cadenza.createMetaTask("Observe orders updated", () => true);
    observedTask.doOn("global.orders.updated");
    observedTask.registered = true;

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const result = (GraphSyncController.instance.registerSignalToTaskMapTask as any)
      .taskFunction({
        __syncing: true,
        task: observedTask,
      });

    expect(result).toBe(false);
  });

  it("syncs non-global signal names into authority but keeps signal-to-task maps global-only", async () => {
    const signalRows: Array<Record<string, unknown>> = [];
    const signalToTaskRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("signal_registry", (ctx) => {
      signalRows.push(getInsertRow(ctx));
      return ctx;
    });
    createLocalAuthorityInsertTask("signal_to_task_map", (ctx) => {
      signalToTaskRows.push(getInsertRow(ctx));
      return ctx;
    });

    const observedTask = Cadenza.createMetaTask("Observe local invoice paid", () => true);
    observedTask.doOn("billing.invoice.paid");
    observedTask.registered = true;

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(Cadenza.signalBroker.registerSignalTask!, {
      signalName: "billing.invoice.paid",
    });

    Cadenza.run(GraphSyncController.instance.splitSignalsTask!, {
      __syncing: true,
      signals: [
        {
          signal: "billing.invoice.paid",
          data: { registered: false },
        },
      ],
    });

    Cadenza.run(GraphSyncController.instance.registerSignalToTaskMapTask!, {
      __syncing: true,
      task: observedTask,
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(signalRows).toHaveLength(1);
    expect(signalRows[0]).toMatchObject({
      name: "billing.invoice.paid",
      is_global: false,
      is_meta: false,
    });
    expect(signalToTaskRows).toHaveLength(0);
  });

  it("dedupes tagged signal variants into one authority signal row", async () => {
    const signalRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("signal_registry", (ctx) => {
      signalRows.push(getInsertRow(ctx));
      return {
        ...ctx,
        __success: true,
      };
    });

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitSignalsTask!, {
      __syncing: true,
      signals: [
        { signal: "runner.tick:trace-1", data: { registered: false } },
        { signal: "runner.tick:trace-2", data: { registered: false } },
      ],
    });

    await waitForCondition(() => signalRows.length === 1, 1_500);

    expect(signalRows[0]).toMatchObject({
      name: "runner.tick",
      is_global: false,
      is_meta: false,
    });
  });

  it("skips bootstrap-local control signals during bootstrap signal sync", async () => {
    const signalRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("signal_registry", (ctx) => {
      signalRows.push(getInsertRow(ctx));
      return {
        ...ctx,
        __success: true,
      };
    });

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitSignalsTask!, {
      __syncing: true,
      signals: [
        {
          signal: "meta.service_registry.insert_execution_requested",
          data: { registered: false },
        },
        {
          signal: "meta.sync_controller.synced_tasks",
          data: { registered: false },
        },
        {
          signal: "meta.service_registry.routeable_transport_missing",
          data: { registered: false },
        },
        {
          signal: "meta.rest.handshake",
          data: { registered: false },
        },
        {
          signal: "meta.socket.handshake",
          data: { registered: false },
        },
        {
          signal: "meta.fetch.handshake_complete",
          data: { registered: false },
        },
        {
          signal: "sub_meta.signal_broker.new_trace",
          data: { registered: false },
        },
        {
          signal: "global.orders.updated",
          data: { registered: false },
        },
        {
          signal: "global.meta.service_instance.updated",
          data: { registered: false },
        },
        {
          signal: "global.meta.signal_controller.signal_added",
          data: { registered: false },
        },
      ],
    });

    await waitForCondition(() => signalRows.length === 1, 1_500);

    expect(signalRows[0]).toMatchObject({
      name: "global.orders.updated",
      is_global: true,
    });
  });

  it("does not sync signal transmission tasks into manifest-deferred signal task maps", () => {
    ServiceRegistry.instance.serviceName = "OrdersApi";

    const transmissionTask = Cadenza.createMetaTask(
      "Transmit signal: global.meta.cadenza_db.gathered_sync_data to AlertService",
      () => true,
    );
    transmissionTask.doOn("global.meta.cadenza_db.gathered_sync_data");
    Object.defineProperty(transmissionTask, "isDeputy", {
      value: true,
      configurable: true,
    });
    Object.defineProperty(transmissionTask, "serviceName", {
      value: "AlertService",
      configurable: true,
    });
    transmissionTask.registered = true;
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(Cadenza.signalBroker.registerSignalTask!, {
      signalName: "global.meta.cadenza_db.gathered_sync_data",
    });
    ((Cadenza.signalBroker as any).signalObservers.get(
      "global.meta.cadenza_db.gathered_sync_data",
    ) as any).registered = true;

    const result = (GraphSyncController.instance.registerSignalToTaskMapTask as any)
      .taskFunction({
        __syncing: true,
        task: transmissionTask,
      });

    expect(result).toBe(false);
  });

  it("does not register deputy tasks in the task bootstrap phase", () => {
    ServiceRegistry.instance.serviceName = "OrdersApi";

    const deputyTask = Cadenza.createMetaTask(
      "Transmit signal: global.meta.cadenza_db.gathered_sync_data to AlertService",
      () => true,
    );
    deputyTask.doOn("global.meta.cadenza_db.gathered_sync_data");
    Object.defineProperty(deputyTask, "isDeputy", {
      value: true,
      configurable: true,
    });

    const normalTask = Cadenza.createMetaTask("Normal registrable task", () => true);

    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const payloads = Array.from(
      (
        GraphSyncController.instance.splitTasksForRegistration as any
      ).taskFunction({
        __syncing: true,
        tasks: [deputyTask, normalTask],
      }) as Iterable<Record<string, any>>,
    );

    expect(payloads).toHaveLength(1);
    expect(payloads[0].data).toMatchObject({
      name: "Normal registrable task",
      service_name: "OrdersApi",
    });
  });

  it("fans in local instance insertion to the bootstrap sync root and early tick scheduler", () => {
    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const instanceInsertedObserver = (Cadenza.signalBroker as any).signalObservers.get(
      "meta.service_registry.instance_inserted",
    );
    const taskNames = Array.from(instanceInsertedObserver?.tasks ?? []).map(
      (task: any) => task.name as string,
    );

    expect(taskNames).toContain("Start bootstrap graph sync");
    expect(taskNames).toContain(
      "Request sync after local service instance registration",
    );
  });

  it("keeps actor session persistence local by deferring intent-to-task maps to manifests", async () => {
    const localOnlyTask = Cadenza.createMetaTask(
      "Persist actor session state",
      (ctx) => ctx,
    ).respondsTo(META_ACTOR_SESSION_STATE_PERSIST_INTENT);

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const result = (GraphSyncController.instance.registerIntentToTaskMapTask as any)
      .taskFunction({
        __syncing: true,
        task: localOnlyTask,
      });

    expect(result).toBe(false);
  });

  it("defers intent-to-task structural maps to manifest sync", async () => {
    const task = Cadenza.createMetaTask("Handle order sync", (ctx) => ctx).respondsTo(
      "orders-sync",
    );
    task.registered = true;

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const result = (GraphSyncController.instance.registerIntentToTaskMapTask as any)
      .taskFunction({
        __syncing: true,
        task,
      });

    expect(result).toBe(false);
  });

  it("starts direct graph-metadata registration only after sync registrations complete", async () => {
    const associationEvents: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture replayed task-intent association", (ctx) => {
      associationEvents.push((ctx.data as Record<string, unknown>) ?? {});
      return true;
    }).doOn("global.meta.graph_metadata.task_intent_associated");

    Cadenza.createCadenzaService("OrdersApi", "Orders service", {
      useSocket: false,
      cadenzaDB: {
        connect: true,
        address: "cadenza-db-service",
        port: 8080,
      },
      customServiceId: "orders-api-1",
    });

    associationEvents.length = 0;

    Cadenza.createMetaTask("Handle replayed order sync", (ctx) => ctx).respondsTo(
      "orders-sync-replayed",
    );

    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(associationEvents).toEqual([]);

    Cadenza.emit("meta.service_registry.registered_global_signals", {
      serviceName: "OrdersApi",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(Boolean(Cadenza.get("Handle task intent association"))).toBe(false);

    Cadenza.emit("meta.service_registry.registered_global_intents", {
      serviceName: "OrdersApi",
    });

    await waitForCondition(
      () => Boolean(Cadenza.get("Handle task intent association")),
      1_500,
    );

    Cadenza.createMetaTask("Handle live order sync", (ctx) => ctx).respondsTo(
      "orders-sync-live",
    );

    await waitForCondition(
      () =>
        associationEvents.some(
          (event) =>
            event.intentName === "orders-sync-live" &&
            event.taskName === "Handle live order sync",
        ),
      1_500,
    );
  });

  it("does not sync remote-only intent deputies into local intent_registry rows", async () => {
    const capturedIntentNames: string[] = [];

    Cadenza.createMetaTask("Handle local inventory sync", (ctx) => ctx).respondsTo(
      "inventory-sync",
    );
    Cadenza.createDeputyTask("Lookup Orders", "OrdersService", {
      register: false,
      isHidden: true,
    }).respondsTo("orders-lookup");

    ServiceRegistry.instance.serviceName = "InventoryService";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();
    GraphSyncController.instance.splitIntentsTask!.then(
      Cadenza.createMetaTask("Capture split intent registration rows", (ctx) => {
        capturedIntentNames.push(String(ctx.__intentName ?? ctx.data?.name ?? ""));
        return false;
      }),
    );

    Cadenza.run(GraphSyncController.instance.splitIntentsTask!, {
      __syncing: true,
      intents: Array.from(Cadenza.inquiryBroker.intents.values()),
    });

    await waitForCondition(() => capturedIntentNames.includes("inventory-sync"), 1_500);

    expect(capturedIntentNames).toContain("inventory-sync");
    expect(capturedIntentNames).not.toContain("orders-lookup");
  });

  it("emits one local ready signal after global signal and intent registrations complete", async () => {
    const readyEvents: Array<Record<string, any>> = [];
    const emitSpy = vi.spyOn(Cadenza, "emit");
    vi.spyOn(GraphSyncController.instance, "init").mockImplementation(() => {});

    Cadenza.createCadenzaService("OrdersApi", "Orders service", {
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "orders-api-1",
    });

    (Cadenza as any).bootstrapSyncCompleted = false;
    (Cadenza as any).bootstrapSignalRegistrationsCompleted = false;
    (Cadenza as any).bootstrapIntentRegistrationsCompleted = false;
    readyEvents.length = 0;

    Cadenza.createTask("Capture orders ready signal", (ctx) => {
      readyEvents.push(ctx as Record<string, any>);
      return true;
    }).doOn(Cadenza.getServiceReadySignalName("OrdersApi"));

    expect(Cadenza.isServiceReady()).toBe(false);

    Cadenza.emit("meta.service_registry.registered_global_signals", {
      serviceName: "OrdersApi",
    });
    Cadenza.emit("meta.service_registry.registered_global_signals", {
      serviceName: "OrdersApi",
    });

    Cadenza.emit("meta.service_registry.registered_global_intents", {
      serviceName: "OrdersApi",
    });
    Cadenza.emit("meta.service_registry.registered_global_intents", {
      serviceName: "OrdersApi",
    });

    await waitForCondition(() => readyEvents.length === 1, 1_500);

    expect(Cadenza.isServiceReady()).toBe(true);
    expect(emitSpy).toHaveBeenCalledWith(
      "meta.service_registry.emit_ready_requested:orders_api",
      expect.objectContaining({
        serviceName: "OrdersApi",
        serviceInstanceId: "orders-api-1",
        ready: true,
      }),
    );
    expect(readyEvents).toEqual([
      expect.objectContaining({
        serviceName: "OrdersApi",
        serviceInstanceId: "orders-api-1",
        ready: true,
      }),
    ]);
  });

  it("creates cached global signal transmitters after sync registrations complete", async () => {
    const createSignalTransmissionTaskSpy = vi
      .spyOn(Cadenza, "createSignalTransmissionTask")
      .mockImplementation(() => undefined as any);

    Cadenza.createCadenzaService("OrdersApi", "Orders service", {
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "orders-api-1",
    });

    const registry = ServiceRegistry.instance as any;

    Cadenza.run(registry.handleGlobalSignalRegistrationTask, {
      signalToTaskMaps: [
        {
          signalName: "global.meta.graph_metadata.execution_trace_created",
          serviceName: "CadenzaDB",
          isGlobal: true,
        },
      ],
    });

    expect(createSignalTransmissionTaskSpy).not.toHaveBeenCalled();

    Cadenza.emit("meta.service_registry.registered_global_signals", {
      serviceName: "OrdersApi",
    });
    Cadenza.emit("meta.service_registry.registered_global_intents", {
      serviceName: "OrdersApi",
    });

    await waitForCondition(
      () =>
        createSignalTransmissionTaskSpy.mock.calls.some(
          ([signalName, serviceName]) =>
            signalName === "global.meta.graph_metadata.execution_trace_created" &&
            serviceName === "CadenzaDB",
        ),
      1_500,
    );
  });

  it("marks local signal, intent, and mapped task registrations from authoritative full-sync maps", async () => {
    Cadenza.createCadenzaService("OrdersApi", "Orders service", {
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "orders-api-1",
    });

    const task = Cadenza.createTask("Process orders", () => true)
      .doOn("global.orders.created")
      .respondsTo("orders-lookup");

    const signalObservers = (Cadenza.signalBroker as any).signalObservers as Map<
      string,
      Record<string, any>
    >;

    expect(task.registered).not.toBe(true);
    expect(signalObservers.get("global.orders.created")?.registered).not.toBe(true);
    expect(
        (GraphSyncController.instance as any).registeredIntentDefinitions.has(
          "orders-lookup",
        ),
    ).toBe(false);

    Cadenza.emit("meta.service_registry.registered_global_signals", {
      signalToTaskMaps: [
        {
          signalName: "global.orders.created",
          serviceName: "OrdersApi",
          taskName: "Process orders",
          taskVersion: 1,
          isGlobal: true,
        },
      ],
    });

    Cadenza.emit("meta.service_registry.registered_global_intents", {
      intentToTaskMaps: [
        {
          intentName: "orders-lookup",
          serviceName: "OrdersApi",
          taskName: "Process orders",
          taskVersion: 1,
        },
      ],
    });

    await waitForCondition(
      () =>
        task.registered === true &&
        signalObservers.get("global.orders.created")?.registered === true &&
        (GraphSyncController.instance as any).registeredIntentDefinitions.has(
          "orders-lookup",
        ),
    );

    expect(task.registrationRequested).toBe(false);
    expect(task.registeredSignals.has("global.orders.created")).toBe(true);
    expect((task as any).__registeredIntents.has("orders-lookup")).toBe(true);
  });

  it("marks local signal and intent task maps from manifest-driven initial sync", async () => {
    Cadenza.createCadenzaService("OrdersApi", "Orders service", {
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "orders-api-1",
    });

    const task = Cadenza.createTask("Process manifest orders", () => true)
      .doOn("global.orders.created")
      .respondsTo("orders-lookup");

    const signalObservers = (Cadenza.signalBroker as any).signalObservers as Map<
      string,
      Record<string, any>
    >;

    Cadenza.emit("meta.service_registry.initial_sync_complete", {
      tasks: [
        {
          name: "Process manifest orders",
          service_name: "OrdersApi",
          version: 1,
        },
      ],
      signals: [
        {
          name: "global.orders.created",
        },
      ],
      intents: [
        {
          name: "orders-lookup",
        },
      ],
      signalToTaskMaps: [
        {
          signalName: "global.orders.created",
          serviceName: "OrdersApi",
          taskName: "Process manifest orders",
          taskVersion: 1,
          isGlobal: true,
        },
      ],
      intentToTaskMaps: [
        {
          intentName: "orders-lookup",
          serviceName: "OrdersApi",
          taskName: "Process manifest orders",
          taskVersion: 1,
        },
      ],
    });

    await waitForCondition(
      () =>
        task.registered === true &&
        signalObservers.get("global.orders.created")?.registered === true &&
        task.registeredSignals.has("global.orders.created") &&
        (((task as any).__registeredIntents as Set<string> | undefined)?.has(
          "orders-lookup",
        ) ?? false),
    );
  });

  it("treats manifest-confirmed global signal task maps as synced even if local registeredSignals drift", async () => {
    Cadenza.createCadenzaService("OrdersApi", "Orders service", {
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "orders-api-1",
    });

    const task = Cadenza.createTask("Process global manifest signal", () => true).doOn(
      "global.orders.created",
    );

    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.emit("meta.service_registry.initial_sync_complete", {
      tasks: [
        {
          name: "Process global manifest signal",
          service_name: "OrdersApi",
          version: 1,
        },
      ],
      signals: [
        {
          name: "global.orders.created",
        },
      ],
      signalToTaskMaps: [
        {
          signalName: "global.orders.created",
          serviceName: "OrdersApi",
          taskName: "Process global manifest signal",
          taskVersion: 1,
          isGlobal: true,
        },
      ],
    });

    await waitForCondition(() => task.registered === true);

    task.registeredSignals.clear();
    GraphSyncController.instance.signalTaskMapsSynced = false;

    Cadenza.run(Cadenza.get("Gather signal task map registration")!, {
      __syncing: true,
    });

    await waitForCondition(() => GraphSyncController.instance.signalTaskMapsSynced);
    expect(GraphSyncController.instance.signalTaskMapsSynced).toBe(true);
  });

  it("treats manifest-confirmed service-registry global signal maps as synced", async () => {
    Cadenza.createCadenzaService("OrdersApi", "Orders service", {
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "orders-api-1",
    });

    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const instanceUpdateTask = Cadenza.get("Handle Instance Update") as any;
    const transportUpdateTask = Cadenza.get("Handle Transport Update") as any;
    const globalSignalRegistrationTask = Cadenza.get(
      "Handle global Signal Registration",
    ) as any;

    expect(instanceUpdateTask).toBeDefined();
    expect(transportUpdateTask).toBeDefined();
    expect(globalSignalRegistrationTask).toBeDefined();

    for (const task of [
      instanceUpdateTask,
      transportUpdateTask,
      globalSignalRegistrationTask,
    ]) {
      task.registered = true;
      task.registrationRequested = false;
      task.registeredSignals.clear();
    }

    Cadenza.emit("meta.service_registry.initial_sync_complete", {
      signalToTaskMaps: [
        {
          signalName: "global.meta.service_instance.inserted",
          serviceName: "OrdersApi",
          taskName: "Handle Instance Update",
          taskVersion: 1,
          isGlobal: true,
        },
        {
          signalName: "global.meta.service_instance.updated",
          serviceName: "OrdersApi",
          taskName: "Handle Instance Update",
          taskVersion: 1,
          isGlobal: true,
        },
        {
          signalName: "global.meta.service_instance_transport.inserted",
          serviceName: "OrdersApi",
          taskName: "Handle Transport Update",
          taskVersion: 1,
          isGlobal: true,
        },
        {
          signalName: "global.meta.service_instance_transport.updated",
          serviceName: "OrdersApi",
          taskName: "Handle Transport Update",
          taskVersion: 1,
          isGlobal: true,
        },
        {
          signalName: "global.meta.graph_metadata.task_signal_observed",
          serviceName: "OrdersApi",
          taskName: "Handle global Signal Registration",
          taskVersion: 1,
          isGlobal: true,
        },
      ],
    });

    await waitForCondition(
      () =>
        (GraphSyncController.instance as any).authoritativeSignalTaskMaps.size === 5,
    );

    GraphSyncController.instance.signalTaskMapsSynced = false;
    Cadenza.run(Cadenza.get("Gather signal task map registration")!, {
      __syncing: true,
    });

    await waitForCondition(() => GraphSyncController.instance.signalTaskMapsSynced);
    expect(GraphSyncController.instance.signalTaskMapsSynced).toBe(true);
  });

  it("skips direct graph-metadata registration for task associations already persisted locally", async () => {
    const signalEvents: Array<Record<string, unknown>> = [];
    const intentEvents: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture persisted task signal observation", (ctx) => {
      const data = (ctx.data as Record<string, unknown>) ?? {};
      if (data.taskName === "Already persisted task") {
        signalEvents.push(data);
      }
      return true;
    }).doOn("global.meta.graph_metadata.task_signal_observed");

    Cadenza.createMetaTask("Capture persisted task intent association", (ctx) => {
      const data = (ctx.data as Record<string, unknown>) ?? {};
      if (data.taskName === "Already persisted task") {
        intentEvents.push(data);
      }
      return true;
    }).doOn("global.meta.graph_metadata.task_intent_associated");

    GraphMetadataController.instance;

    const task = Cadenza.createMetaTask("Already persisted task", () => true)
      .doOn("global.orders.updated")
      .respondsTo("orders-persisted");
    task.registered = true;
    task.registeredSignals.add("global.orders.updated");
    (task as any).__registeredIntents = new Set<string>(["orders-persisted"]);

    signalEvents.length = 0;
    intentEvents.length = 0;

    Cadenza.emit("meta.task.observed_signal", {
      signalName: "global.orders.updated",
      data: {
        signalName: "global.orders.updated",
        taskName: task.name,
        taskVersion: task.version,
      },
    });
    Cadenza.emit("meta.task.intent_associated", {
      data: {
        intentName: "orders-persisted",
        taskName: task.name,
        taskVersion: task.version,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(signalEvents).toEqual([]);
    expect(intentEvents).toEqual([]);
  });

  it("treats tagged observed signals as satisfied by canonical manifest signal-task maps", async () => {
    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const task = Cadenza.createMetaTask("Process tagged orders", () => true).doOn(
      "global.orders.created:tenant-a",
    );
    task.registered = true;

    Cadenza.emit("meta.service_registry.registered_global_signals", {
      signalToTaskMaps: [
        {
          signalName: "global.orders.created",
          serviceName: "OrdersApi",
          taskName: "Process tagged orders",
          taskVersion: 1,
          isGlobal: true,
        },
      ],
    });

    await waitForCondition(() =>
      task.registeredSignals.has("global.orders.created"),
    );

    expect(task.registeredSignals.has("global.orders.created")).toBe(true);
    expect(task.registeredSignals.has("global.orders.created:tenant-a")).toBe(false);
  });

  it("skips direct graph-metadata registration for internal tasks that are not registerable", async () => {
    const taskCreatedEvents: Array<Record<string, unknown>> = [];
    const signalEvents: Array<Record<string, unknown>> = [];
    const intentEvents: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Capture internal task creation", (ctx) => {
      const data = (ctx.data as Record<string, unknown>) ?? {};
      if (data.name === "Internal transport task") {
        taskCreatedEvents.push(data);
      }
      return true;
    }).doOn("global.meta.graph_metadata.task_created");

    Cadenza.createMetaTask("Capture internal task signal observation", (ctx) => {
      const data = (ctx.data as Record<string, unknown>) ?? {};
      if (data.taskName === "Internal transport task") {
        signalEvents.push(data);
      }
      return true;
    }).doOn("global.meta.graph_metadata.task_signal_observed");

    Cadenza.createMetaTask("Capture internal task intent association", (ctx) => {
      const data = (ctx.data as Record<string, unknown>) ?? {};
      if (data.taskName === "Internal transport task") {
        intentEvents.push(data);
      }
      return true;
    }).doOn("global.meta.graph_metadata.task_intent_associated");

    GraphMetadataController.instance;

    Cadenza.createMetaTask(
      "Internal transport task",
      () => true,
      "Internal task that should never participate in direct graph registration",
      {
        register: false,
        isHidden: true,
      },
    )
      .doOn("meta.internal.transport.updated")
      .respondsTo("internal-transport-updated");

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(taskCreatedEvents).toEqual([]);
    expect(signalEvents).toEqual([]);
    expect(intentEvents).toEqual([]);
  });

});
