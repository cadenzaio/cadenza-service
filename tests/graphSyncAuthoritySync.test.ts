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
    resetRuntimeState();
  });

  afterEach(() => {
    resetRuntimeState();
    consoleLogSpy.mockRestore();
    consoleWarnSpy.mockRestore();
    consoleErrorSpy.mockRestore();
    vi.restoreAllMocks();
  });

  it("falls back to the local service name for default database deputy relationships", async () => {
    const directionalRows: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Insert directional_task_graph_map", (ctx) => {
      directionalRows.push(getInsertRow(ctx));
      return ctx;
    });

    const databaseTask = Cadenza.createDatabaseInsertTask("health_metric");
    ServiceRegistry.instance.serviceName = "DiagnosticsService";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.registerDeputyRelationshipTask!, {
      __syncing: true,
      task: databaseTask,
    });

    await waitForCondition(() =>
      directionalRows.some(
        (row) =>
          row.predecessor_task_name === databaseTask.name &&
          row.task_name === "Insert health_metric",
      ),
    );

    const deputyRelationship = directionalRows.find(
      (row) =>
        row.predecessor_task_name === databaseTask.name &&
        row.task_name === "Insert health_metric",
    );

    expect(deputyRelationship).toMatchObject({
      service_name: "DiagnosticsService",
      predecessor_service_name: "DiagnosticsService",
    });
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

  it("emits compact task registration split payloads", async () => {
    const splitPayloads: Array<Record<string, unknown>> = [];

    const task = Cadenza.createMetaTask("Register compact task", () => true);

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const splitTask = GraphSyncController.instance.splitTasksForRegistration as any;
    splitTask.taskFunction(
      {
        __syncing: true,
        tasks: [task],
        intents: [{ name: "should-not-leak" }],
        signals: [{ signal: "should.not.leak", data: {} }],
      },
      (signal: string, payload: Record<string, unknown>) => {
        if (signal === "meta.sync_controller.task_registration_split") {
          splitPayloads.push(payload);
        }
      },
    );

    await waitForCondition(() => splitPayloads.length === 1, 1_500);

    expect(splitPayloads[0]).toMatchObject({
      __syncing: true,
      __taskName: "Register compact task",
    });
    expect(splitPayloads[0].data).toMatchObject({
      name: "Register compact task",
      service_name: "OrdersApi",
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
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();

    expect(GraphSyncController.instance.splitIntentsTask).toBeUndefined();

    for (const tableName of [
      "intent_registry",
      "routine",
      "task_to_routine_map",
      "signal_registry",
      "task",
      "actor",
      "actor_task_map",
      "signal_to_task_map",
      "intent_to_task_map",
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

  it("schedules an immediate sync tick for connected services", () => {
    const scheduleSpy = vi.spyOn(Cadenza, "schedule");

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();

    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.sync_controller.sync_tick",
      { __syncing: true },
      250,
    );
    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.sync_requested",
      { __syncing: true },
      2000,
    );
  });

  it("persists every split task registration signal instead of collapsing them into one resolver execution", async () => {
    const insertedTaskNames: string[] = [];

    for (const tableName of [
      "intent_registry",
      "routine",
      "task_to_routine_map",
      "signal_registry",
      "task",
      "actor",
      "actor_task_map",
      "signal_to_task_map",
      "intent_to_task_map",
      "directional_task_graph_map",
    ]) {
      Cadenza.createMetaTask(`Insert ${tableName}`, (ctx) => {
        if (tableName === "task") {
          insertedTaskNames.push(String(getInsertRow(ctx).name));
        }
        return ctx;
      });
    }

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();

    const firstTask = Cadenza.createTask(
      "Capture first order",
      () => ({ ok: true }),
      "First business task to persist through graph sync.",
    );
    const secondTask = Cadenza.createTask(
      "Capture second order",
      () => ({ ok: true }),
      "Second business task to persist through graph sync.",
    );

    Cadenza.emit("meta.sync_controller.task_registration_split", {
      __syncing: true,
      __taskName: firstTask.name,
      data: {
        name: firstTask.name,
        version: firstTask.version,
        description: firstTask.description,
        functionString: "(() => ({ ok: true }))",
        service_name: "OrdersApi",
      },
    });
    Cadenza.emit("meta.sync_controller.task_registration_split", {
      __syncing: true,
      __taskName: secondTask.name,
      data: {
        name: secondTask.name,
        version: secondTask.version,
        description: secondTask.description,
        functionString: "(() => ({ ok: true }))",
        service_name: "OrdersApi",
      },
    });

    await waitForCondition(
      () =>
        insertedTaskNames.includes("Capture first order") &&
        insertedTaskNames.includes("Capture second order"),
      1_500,
    );

    expect(
      insertedTaskNames.filter((name) => name === "Capture first order"),
    ).toHaveLength(1);
    expect(
      insertedTaskNames.filter((name) => name === "Capture second order"),
    ).toHaveLength(1);
  });

  it("does not mark intent definitions as registered when delegated inserts have zero responders", async () => {
    Cadenza.createMetaTask("Insert intent_registry", (ctx) => {
      return {
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
      };
    });

    Cadenza.createTask("Lookup missing responders", () => ({ ok: true })).respondsTo(
      "orders-missing-responders",
    );

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
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

  it("routes local sync inserts through signal-driven runner flow so queryData survives", async () => {
    let capturedQueryData: Record<string, unknown> | undefined;

    Cadenza.createMetaTask("Insert signal_registry", (ctx) => {
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
          signal: "orders.updated",
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

  it("skips task-to-routine sync until both routines and tasks are registered", async () => {
    const taskToRoutineRows: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Insert task_to_routine_map", (ctx) => {
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
        row.taskName === "Sync routine dependency" &&
        row.routineName === "Sync services" &&
        row.serviceName === "OrdersApi",
      ),
    ).toBe(true);
  });

  it("skips signal-to-task sync until the observed signal is registered", async () => {
    const signalToTaskRows: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Insert signal_to_task_map", (ctx) => {
      signalToTaskRows.push(getInsertRow(ctx));
      return ctx;
    });

    const observedTask = Cadenza.createMetaTask("Observe orders updated", () => true);
    observedTask.doOn("orders.updated");
    observedTask.registered = true;

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.registerSignalToTaskMapTask!, {
      __syncing: true,
      task: observedTask,
    });

    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(signalToTaskRows).toHaveLength(0);

    Cadenza.run(Cadenza.signalBroker.registerSignalTask!, {
      signalName: "orders.updated",
    });

    Cadenza.run(GraphSyncController.instance.registerSignalToTaskMapTask!, {
      __syncing: true,
      task: observedTask,
    });

    await waitForCondition(() => signalToTaskRows.length === 1, 1_500);

    expect(signalToTaskRows[0]).toMatchObject({
      signalName: "orders.updated",
      taskName: "Observe orders updated",
      serviceName: "OrdersApi",
    });
  });

  it("wires synced_tasks to retry relationship sync observers", () => {
    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    const syncedTasksObserver = (Cadenza.signalBroker as any).signalObservers.get(
      "meta.sync_controller.synced_tasks",
    );
    const taskNames = Array.from(syncedTasksObserver?.tasks ?? []).map(
      (task: any) => task.name as string,
    );

    expect(taskNames.filter((name) => name.startsWith("Do for each task"))).toHaveLength(3);
    expect(taskNames.some((name) => name.startsWith("Get all routines"))).toBe(
      true,
    );
  });

  it("keeps actor session persistence local by skipping intent-to-task sync", async () => {
    const intentMapRows: Array<Record<string, unknown>> = [];

    Cadenza.createMetaTask("Insert intent_to_task_map", (ctx) => {
      intentMapRows.push(getInsertRow(ctx));
      return {
        ...ctx,
        __success: true,
      };
    });

    const localOnlyTask = Cadenza.createMetaTask(
      "Persist actor session state",
      (ctx) => ctx,
    ).respondsTo(META_ACTOR_SESSION_STATE_PERSIST_INTENT);

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.registerIntentToTaskMapTask!, {
      __syncing: true,
      task: localOnlyTask,
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(intentMapRows).toHaveLength(0);
  });

  it("restores the intent-to-task map payload after ensuring the intent registry row exists", async () => {
    let insertedIntentDefinition: Record<string, unknown> | undefined;
    let insertedIntentMap: Record<string, unknown> | undefined;

    Cadenza.createMetaTask("Insert intent_registry", (ctx) => {
      insertedIntentDefinition = getInsertRow(ctx);
      return {
        ...ctx,
        __success: true,
      };
    });

    Cadenza.createMetaTask("Insert intent_to_task_map", (ctx) => {
      insertedIntentMap = getInsertRow(ctx);
      return {
        ...ctx,
        __success: true,
      };
    });

    const task = Cadenza.createMetaTask("Handle order sync", (ctx) => ctx).respondsTo(
      "orders-sync",
    );
    task.registered = true;

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.registerIntentToTaskMapTask!, {
      __syncing: true,
      task,
    });

    await waitForCondition(
      () => insertedIntentDefinition !== undefined && insertedIntentMap !== undefined,
      1_500,
    );

    expect(insertedIntentDefinition).toMatchObject({
      name: "orders-sync",
    });
    expect(insertedIntentMap).toMatchObject({
      intentName: "orders-sync",
      taskName: "Handle order sync",
      serviceName: "OrdersApi",
    });
    expect(insertedIntentMap).not.toHaveProperty("name");
    expect(insertedIntentMap).not.toHaveProperty("input");
  });

  it("preserves sync metadata when remote insert results omit the original context", async () => {
    const originalCreateCadenzaDbInsertTask =
      Cadenza.createCadenzaDBInsertTask.bind(Cadenza);

    vi.spyOn(Cadenza, "createCadenzaDBInsertTask").mockImplementation(
      (tableName: string, queryData: Record<string, unknown>, options?: Record<string, unknown>) => {
        if (tableName !== "task") {
          return originalCreateCadenzaDbInsertTask(tableName, queryData, options);
        }

        return Cadenza.createMetaTask("Insert task remotely", (ctx) => ({
          __resolverRequestId: ctx.__resolverRequestId,
          __success: true,
          queryData: ctx.queryData,
        }));
      },
    );

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();

    const task = Cadenza.createTask(
      "Register remote task metadata",
      () => ({ ok: true }),
      "Persists task metadata through the remote graph sync path.",
    );

    Cadenza.emit("meta.sync_controller.task_registration_split", {
      __syncing: true,
      __taskName: task.name,
      data: {
        name: task.name,
        version: task.version,
        description: task.description,
        functionString: "(() => ({ ok: true }))",
        service_name: "OrdersApi",
      },
    });

    await waitForCondition(() => task.registered === true, 1_500);

    expect(task.registered).toBe(true);
  });

});
