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

    createLocalAuthorityInsertTask("directional_task_graph_map", (ctx) => {
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

  it("schedules an early sync request burst for connected services", () => {
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
    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.sync_requested",
      { __syncing: true },
      10000,
    );
    expect(scheduleSpy).toHaveBeenCalledWith(
      "meta.sync_requested",
      { __syncing: true },
      30000,
    );
  });

  it("wires meta.sync_requested to graph registration scans", () => {
    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();

    const syncRequestedObserver = (Cadenza.signalBroker as any).signalObservers.get(
      "meta.sync_requested",
    );
    const taskNames = Array.from(syncRequestedObserver?.tasks ?? []).map(
      (task: any) => task.name as string,
    );

    expect(taskNames.some((name) => name.startsWith("Get all tasks"))).toBe(true);
    expect(taskNames.some((name) => name.startsWith("Get all routines"))).toBe(true);
    expect(taskNames.some((name) => name === "Get all intents")).toBe(true);
    expect(taskNames.some((name) => name === "Get all actors")).toBe(true);
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
        row.taskName === "Sync routine dependency" &&
        row.routineName === "Sync services" &&
        row.serviceName === "OrdersApi",
      ),
    ).toBe(true);
  });

  it("skips signal-to-task sync until the observed global signal is registered", async () => {
    const signalToTaskRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("signal_to_task_map", (ctx) => {
      signalToTaskRows.push(getInsertRow(ctx));
      return ctx;
    });

    const observedTask = Cadenza.createMetaTask("Observe orders updated", () => true);
    observedTask.doOn("global.orders.updated");
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
      signalName: "global.orders.updated",
    });

    Cadenza.run(GraphSyncController.instance.registerSignalToTaskMapTask!, {
      __syncing: true,
      task: observedTask,
    });

    await waitForCondition(() => signalToTaskRows.length === 1, 1_500);

    expect(signalToTaskRows[0]).toMatchObject({
      signalName: "global.orders.updated",
      taskName: "Observe orders updated",
      serviceName: "OrdersApi",
    });
  });

  it("does not require global sync flags before registering global signal-to-task maps", async () => {
    const signalToTaskRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("signal_to_task_map", (ctx) => {
      signalToTaskRows.push(getInsertRow(ctx));
      return {
        ...ctx,
        __success: true,
      };
    });

    const observedTask = Cadenza.createMetaTask("Observe invoice paid", () => true);
    observedTask.doOn("global.billing.invoice.paid");
    observedTask.registered = true;

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();
    GraphSyncController.instance.tasksSynced = false;
    GraphSyncController.instance.signalsSynced = false;

    Cadenza.run(Cadenza.signalBroker.registerSignalTask!, {
      signalName: "global.billing.invoice.paid",
    });

    Cadenza.run(GraphSyncController.instance.registerSignalToTaskMapTask!, {
      __syncing: true,
      task: observedTask,
    });

    await waitForCondition(() => signalToTaskRows.length === 1, 1_500);

    expect(signalToTaskRows[0]).toMatchObject({
      signalName: "global.billing.invoice.paid",
      taskName: "Observe invoice paid",
      serviceName: "OrdersApi",
    });
  });

  it("ignores non-global signals for authority signal sync", async () => {
    const signalToTaskRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("signal_registry", (ctx) => ctx);
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

    expect(signalToTaskRows).toHaveLength(0);
  });

  it("keeps signal transmission task split payloads owned by the local service", () => {
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

    const payloads = Array.from(
      (
        GraphSyncController.instance.registerSignalToTaskMapTask as any
      ).taskFunction({
        __syncing: true,
        task: transmissionTask,
      }) as Iterable<Record<string, any>>,
    );

    expect(payloads).toHaveLength(1);
    expect(payloads[0].data).toMatchObject({
      signalName: "global.meta.cadenza_db.gathered_sync_data",
      taskName:
        "Transmit signal: global.meta.cadenza_db.gathered_sync_data to AlertService",
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

    createLocalAuthorityInsertTask("intent_to_task_map", (ctx) => {
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

    createLocalAuthorityInsertTask("intent_registry", (ctx) => {
      insertedIntentDefinition = getInsertRow(ctx);
      return {
        ...ctx,
        __success: true,
      };
    });

    createLocalAuthorityInsertTask("intent_to_task_map", (ctx) => {
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

  it("restores the intent-to-task map payload for remote authority inserts", async () => {
    let insertedIntentDefinition: Record<string, unknown> | undefined;
    let insertedIntentMap: Record<string, unknown> | undefined;
    let insertedIntentMapQueryData: Record<string, unknown> | undefined;

    vi.spyOn(Cadenza, "getLocalCadenzaDBInsertTask").mockReturnValue(undefined);
    vi.spyOn(Cadenza, "createCadenzaDBInsertTask").mockImplementation(
      (tableName) =>
        Cadenza.createMetaTask(
          `Remote insert ${tableName}`,
          (ctx) => {
            if (tableName === "intent_registry") {
              insertedIntentDefinition = getInsertRow(ctx);
              return {
                __success: true,
                queryData: {
                  filter: {
                    service_name: "OrdersApi",
                  },
                  fields: ["name", "version", "service_name"],
                  data: {},
                },
              };
            }

            if (tableName === "intent_to_task_map") {
              insertedIntentMap = getInsertRow(ctx);
              insertedIntentMapQueryData =
                (ctx.queryData as Record<string, unknown>) ?? undefined;
            }

            return {
              __success: true,
            };
          },
          "",
          {
            register: false,
            isHidden: true,
            isMeta: true,
          },
        ),
    );

    const task = Cadenza.createMetaTask("Handle remote order sync", (ctx) => ctx).respondsTo(
      "orders-remote-sync",
    );
    task.registered = true;

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = true;
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
      name: "orders-remote-sync",
    });
    expect(insertedIntentMap).toMatchObject({
      intentName: "orders-remote-sync",
      taskName: "Handle remote order sync",
      serviceName: "OrdersApi",
    });
    expect(insertedIntentMapQueryData).toMatchObject({
      onConflict: {
        target: ["intent_name", "task_name", "task_version", "service_name"],
        action: {
          do: "nothing",
        },
      },
      data: {
        intentName: "orders-remote-sync",
        taskName: "Handle remote order sync",
        taskVersion: 1,
        serviceName: "OrdersApi",
      },
    });
    expect(insertedIntentMapQueryData).not.toHaveProperty("fields");
    expect(insertedIntentMapQueryData).not.toHaveProperty("filter");
  });

  it("does not require global sync flags before registering intent-to-task maps", async () => {
    const intentMapRows: Array<Record<string, unknown>> = [];

    createLocalAuthorityInsertTask("intent_registry", (ctx) => ({
      ...ctx,
      __success: true,
    }));

    createLocalAuthorityInsertTask("intent_to_task_map", (ctx) => {
      intentMapRows.push(getInsertRow(ctx));
      return {
        ...ctx,
        __success: true,
      };
    });

    const task = Cadenza.createMetaTask("Handle payment reconciliation", (ctx) => ctx)
      .respondsTo("billing-payment-reconcile");
    task.registered = true;

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();
    GraphSyncController.instance.tasksSynced = false;
    GraphSyncController.instance.intentsSynced = false;

    Cadenza.run(GraphSyncController.instance.registerIntentToTaskMapTask!, {
      __syncing: true,
      task,
    });

    await waitForCondition(() => intentMapRows.length === 1, 1_500);

    expect(intentMapRows[0]).toMatchObject({
      intentName: "billing-payment-reconcile",
      taskName: "Handle payment reconciliation",
      serviceName: "OrdersApi",
    });
  });

});
