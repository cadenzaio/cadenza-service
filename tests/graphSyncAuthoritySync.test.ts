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
});
