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

    Cadenza.createMetaTask("dbInsertDirectionalTaskGraphMap", (ctx) => {
      directionalRows.push(ctx.data);
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
    Cadenza.createCadenzaService("TransportApi", "Transport test service", {
      cadenzaDB: {
        connect: false,
      },
      port: 0,
      useSocket: false,
      transports: [
        {
          role: "public",
          origin: "http://transport-api.localhost",
          protocols: ["rest", "socket"],
        },
      ],
    });

    await waitForCondition(() => {
      const instance = (ServiceRegistry.instance as any).instances.get(
        "TransportApi",
      )?.[0];

      return Array.isArray(instance?.transports) && instance.transports.length >= 1;
    });

    const instance = (ServiceRegistry.instance as any).instances.get(
      "TransportApi",
    )[0];

    expect(
      instance.transports.some((transport: any) => transport.role === "public"),
    ).toBe(true);
    expect(
      instance.transports.every((transport: any) =>
        UUID_PATTERN.test(transport.uuid),
      ),
    ).toBe(true);
  });

  it("registers intent definitions before inserting intent-to-task maps", async () => {
    const insertSequence: string[] = [];

    Cadenza.createMetaTask("dbInsertSignalRegistry", (ctx) => ctx);
    Cadenza.createMetaTask("dbInsertTask", (ctx) => ctx);
    Cadenza.createMetaTask("dbInsertDirectionalTaskGraphMap", (ctx) => ctx);
    Cadenza.createMetaTask("dbInsertIntentRegistry", (ctx) => {
      if (ctx.data?.name === "orders-lookup") {
        insertSequence.push("intent_registry");
      }
      return ctx;
    });
    Cadenza.createMetaTask("dbInsertIntentToTaskMap", (ctx) => {
      if (ctx.data?.intentName === "orders-lookup") {
        insertSequence.push("intent_to_task_map");
      }
      return ctx;
    });

    const lookupTask = Cadenza.createTask("Lookup orders", () => {
      return {
        orders: [],
      };
    }).respondsTo("orders-lookup");

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitIntentsTask!, {
      __syncing: true,
      intents: Array.from(Cadenza.inquiryBroker.intents.values()),
    });
    Cadenza.run(GraphSyncController.instance.registerIntentToTaskMapTask!, {
      __syncing: true,
      task: lookupTask,
    });

    await waitForCondition(
      () =>
        insertSequence.includes("intent_registry") &&
        insertSequence.includes("intent_to_task_map"),
      1_500,
    );

    expect(insertSequence[0]).toBe("intent_registry");
    expect(insertSequence).toContain("intent_to_task_map");
  });

  it("uses exact local CadenzaDB task names when legacy dbInsert aliases are absent", async () => {
    const insertSequence: string[] = [];

    Cadenza.createMetaTask("Insert intent_registry", (ctx) => {
      if (ctx.data?.name === "orders-local-lookup") {
        insertSequence.push("intent_registry");
      }
      return ctx;
    });
    Cadenza.createMetaTask("Insert intent_to_task_map", (ctx) => {
      if (ctx.data?.intentName === "orders-local-lookup") {
        insertSequence.push("intent_to_task_map");
      }
      return ctx;
    });

    const lookupTask = Cadenza.createTask("Lookup local orders", () => {
      return {
        orders: [],
      };
    }).respondsTo("orders-local-lookup");

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitIntentsTask!, {
      __syncing: true,
      intents: Array.from(Cadenza.inquiryBroker.intents.values()),
    });
    Cadenza.run(GraphSyncController.instance.registerIntentToTaskMapTask!, {
      __syncing: true,
      task: lookupTask,
    });

    await waitForCondition(
      () =>
        insertSequence.includes("intent_registry") &&
        insertSequence.includes("intent_to_task_map"),
      1_500,
    );

    expect(insertSequence[0]).toBe("intent_registry");
    expect(insertSequence).toContain("intent_to_task_map");
  });

  it("prefers exact local CadenzaDB tasks over deputy inserts when the authority lives in-process", async () => {
    const insertSequence: string[] = [];

    Cadenza.createMetaTask("Insert intent_registry", (ctx) => {
      if (ctx.data?.name === "orders-self-sync") {
        insertSequence.push("intent_registry");
      }
      return ctx;
    });
    Cadenza.createMetaTask("Insert intent_to_task_map", (ctx) => {
      if (ctx.data?.intentName === "orders-self-sync") {
        insertSequence.push("intent_to_task_map");
      }
      return ctx;
    });
    for (const tableName of [
      "routine",
      "task_to_routine_map",
      "signal_registry",
      "task",
      "actor",
      "actor_task_map",
      "signal_to_task_map",
      "directional_task_graph_map",
    ]) {
      Cadenza.createMetaTask(`Insert ${tableName}`, (ctx) => ctx);
    }

    const lookupTask = Cadenza.createMetaTask("Lookup self-synced orders", () => {
      return {
        orders: [],
      };
    }).respondsTo("orders-self-sync");

    ServiceRegistry.instance.serviceName = "CadenzaDB";
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitIntentsTask!, {
      __syncing: true,
      intents: Array.from(Cadenza.inquiryBroker.intents.values()),
    });
    Cadenza.run(GraphSyncController.instance.registerIntentToTaskMapTask!, {
      __syncing: true,
      task: lookupTask,
    });

    await waitForCondition(
      () =>
        insertSequence.includes("intent_registry") &&
        insertSequence.includes("intent_to_task_map"),
      1_500,
    );

    expect(insertSequence[0]).toBe("intent_registry");
    expect(insertSequence).toContain("intent_to_task_map");
  });

  it("preserves sync queryData when exact local CadenzaDB insert tasks are used", async () => {
    const observedQueryData: Record<string, Array<Record<string, unknown>>> = {
      intent_registry: [],
      intent_to_task_map: [],
    };

    Cadenza.createMetaTask("Insert intent_registry", (ctx) => {
      if (ctx.data?.name === "orders-self-sync-query-data") {
        observedQueryData.intent_registry.push(ctx.queryData ?? {});
      }
      return ctx;
    });
    Cadenza.createMetaTask("Insert intent_to_task_map", (ctx) => {
      if (ctx.data?.intentName === "orders-self-sync-query-data") {
        observedQueryData.intent_to_task_map.push(ctx.queryData ?? {});
      }
      return ctx;
    });
    for (const tableName of [
      "routine",
      "task_to_routine_map",
      "signal_registry",
      "task",
      "actor",
      "actor_task_map",
      "signal_to_task_map",
      "directional_task_graph_map",
    ]) {
      Cadenza.createMetaTask(`Insert ${tableName}`, (ctx) => ctx);
    }

    const lookupTask = Cadenza.createMetaTask(
      "Lookup self-synced orders with query data",
      () => {
        return {
          orders: [],
        };
      },
    ).respondsTo("orders-self-sync-query-data");

    ServiceRegistry.instance.serviceName = "CadenzaDB";
    GraphSyncController.instance.isCadenzaDBReady = true;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitIntentsTask!, {
      __syncing: true,
      intents: Array.from(Cadenza.inquiryBroker.intents.values()),
    });
    Cadenza.run(GraphSyncController.instance.registerIntentToTaskMapTask!, {
      __syncing: true,
      task: lookupTask,
    });

    await waitForCondition(
      () =>
        observedQueryData.intent_registry.length > 0 &&
        observedQueryData.intent_to_task_map.length > 0,
      1_500,
    );

    expect(observedQueryData.intent_registry[0]?.onConflict).toEqual({
      target: ["name"],
      action: {
        do: "nothing",
      },
    });
    expect(observedQueryData.intent_registry[0]?.data).toMatchObject({
      name: "orders-self-sync-query-data",
    });
    expect(observedQueryData.intent_to_task_map[0]?.onConflict).toEqual({
      target: ["intent_name", "task_name", "task_version", "service_name"],
      action: {
        do: "nothing",
      },
    });
    expect(observedQueryData.intent_to_task_map[0]?.data).toMatchObject({
      intentName: "orders-self-sync-query-data",
      taskName: "Lookup self-synced orders with query data",
    });
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

  it("registers intent-to-task maps immediately after task rows are synced", async () => {
    const insertSequence: string[] = [];

    Cadenza.createMetaTask("Insert task", (ctx) => {
      if (ctx.data?.name === "Lookup staged orders") {
        insertSequence.push("task");
      }
      return ctx;
    });
    Cadenza.createMetaTask("Insert intent_registry", (ctx) => {
      if (ctx.data?.name === "orders-staged-lookup") {
        insertSequence.push("intent_registry");
      }
      return ctx;
    });
    Cadenza.createMetaTask("Insert intent_to_task_map", (ctx) => {
      if (ctx.data?.intentName === "orders-staged-lookup") {
        insertSequence.push("intent_to_task_map");
      }
      return ctx;
    });

    const lookupTask = Cadenza.createTask("Lookup staged orders", () => {
      return {
        orders: [],
      };
    }).respondsTo("orders-staged-lookup");

    ServiceRegistry.instance.serviceName = "OrdersApi";
    GraphSyncController.instance.isCadenzaDBReady = false;
    GraphSyncController.instance.init();

    Cadenza.run(GraphSyncController.instance.splitTasksForRegistration!, {
      __syncing: true,
      tasks: [lookupTask],
    });
    Cadenza.run(GraphSyncController.instance.splitIntentsTask!, {
      __syncing: true,
      intents: Array.from(Cadenza.inquiryBroker.intents.values()),
    });

    await waitForCondition(
      () =>
        insertSequence.includes("task") &&
        insertSequence.includes("intent_registry") &&
        insertSequence.includes("intent_to_task_map"),
      1_500,
    );

    expect(insertSequence[0]).toBe("task");
    expect(insertSequence).toContain("intent_registry");
    expect(insertSequence).toContain("intent_to_task_map");
  });
});
