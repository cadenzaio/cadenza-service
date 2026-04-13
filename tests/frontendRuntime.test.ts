import {
  META_ACTOR_SESSION_STATE_PERSIST_INTENT,
  type AnyObject,
} from "@cadenza.io/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import Cadenza from "../src/Cadenza";
import DatabaseController from "@service-database-controller";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import GraphSyncController from "../src/graph/controllers/GraphSyncController";
import RestController from "../src/network/RestController";
import ServiceRegistry from "../src/registry/ServiceRegistry";
import SignalController from "../src/signals/SignalController";
import SocketController from "../src/network/SocketController";
import { buildTransportHandleKey as buildHandleKey } from "../src/utils/transport";

function buildRouteKey(
  serviceName: string,
  role: "internal" | "public",
  origin: string,
) {
  return `${serviceName}|${role}|${origin}`;
}

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
  delete (globalThis as any).__CADENZA_RUNTIME__;
}

describe("frontend runtime mode", () => {
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

  it("creates a frontend service without process and registers a frontend instance", async () => {
    const originalProcess = (globalThis as any).process;
    (globalThis as any).process = undefined;

    try {
      Cadenza.createCadenzaService("BrowserApp", "Frontend app", {
        isFrontend: true,
        useSocket: false,
        cadenzaDB: {
          connect: false,
        },
        customServiceId: "browser-app-1",
      });
    } finally {
      (globalThis as any).process = originalProcess;
    }

    await waitForCondition(() => {
      const instances = (ServiceRegistry.instance as any).instances.get(
        "BrowserApp",
      ) as AnyObject[] | undefined;
      return Array.isArray(instances) && instances.length === 1;
    });

    const instance = (ServiceRegistry.instance as any).instances.get(
      "BrowserApp",
    )[0];

    expect(ServiceRegistry.instance.isFrontend).toBe(true);
    expect(instance.uuid).toBe("browser-app-1");
    expect(instance.is_frontend ?? instance.isFrontend).toBeTruthy();
    expect(instance.transports).toEqual([]);
    expect((SignalController as any)._instance).toBeUndefined();
  });

  it("does not publish browser-local responders or signal metadata", async () => {
    const intentEvents: AnyObject[] = [];
    const signalEvents: AnyObject[] = [];

    Cadenza.createMetaTask("Capture browser intent sync", (ctx) => {
      intentEvents.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.task_intent_associated");

    Cadenza.createMetaTask("Capture browser signal sync", (ctx) => {
      signalEvents.push(ctx);
      return true;
    }).doOn("global.meta.signal_controller.signal_added");

    Cadenza.createCadenzaService("BrowserApp", "Frontend app", {
      isFrontend: true,
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "browser-app-2",
    });

    await waitForCondition(() => ServiceRegistry.instance.isFrontend === true);

    Cadenza.createTask("Handle browser-local intent", () => {
      return {
        ok: true,
      };
    }).respondsTo("browser-local-intent");

    Cadenza.createTask("Emit browser-local signal", (_ctx, emit) => {
      emit("global.browser.local.signal", {
        source: "frontend-test",
      });
      return true;
    }).doOn("browser.local.emit");

    Cadenza.emit("browser.local.emit", {});
    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(intentEvents).toEqual([]);
    expect(signalEvents).toEqual([]);
  });

  it("does not mark a connected non-authority service as authority-ready", async () => {
    Cadenza.createCadenzaService("WorkerService", "Worker app", {
      isFrontend: false,
      useSocket: false,
      cadenzaDB: {
        connect: true,
        address: "cadenza-db-service",
        port: 8080,
      },
      customServiceId: "worker-service-1",
    });

    await waitForCondition(() => ServiceRegistry.instance.serviceName === "WorkerService");

    expect(GraphSyncController.instance.isCadenzaDBReady).toBe(false);
  });

  it("routes distributed inquiries in frontend mode through remote intent deputies", async () => {
    const selectedContexts: AnyObject[] = [];
    const fetchHandle = buildHandleKey(
      buildRouteKey("OrdersService", "public", "http://orders.example:7000"),
      "rest",
    );

    Cadenza.createMetaTask("Capture frontend distributed inquiry selection", (ctx) => {
      selectedContexts.push(ctx);
      return true;
    }).doOn(`meta.service_registry.selected_instance_for_fetch:${fetchHandle}`);

    Cadenza.createCadenzaService("BrowserApp", "Frontend app", {
      isFrontend: true,
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "browser-app-3",
    });

    await waitForCondition(
      () => ServiceRegistry.instance.serviceInstanceId === "browser-app-3",
    );

    Cadenza.emit("meta.initializing_service", {
      serviceInstance: {
        uuid: "orders-1",
        serviceName: "OrdersService",
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        isFrontend: false,
        health: {},
        transports: [
          {
            uuid: "orders-public-1",
            service_instance_id: "orders-1",
            role: "public",
            origin: "http://orders.example:7000",
            protocols: ["rest", "socket"],
          },
        ],
      },
    });
    Cadenza.emit("global.meta.graph_metadata.task_intent_associated", {
      intentName: "orders-lookup",
      serviceName: "OrdersService",
      taskName: "LookupOrders",
      taskVersion: 1,
    });

    await waitForCondition(() => {
      const instance = (ServiceRegistry.instance as any).instances
        .get("OrdersService")
        ?.find((entry: AnyObject) => entry.uuid === "orders-1");
      return Boolean(instance);
    });

    const restController = RestController.instance as any;
    vi.spyOn(restController, "fetchDataWithTimeout").mockResolvedValue({
      __status: "success",
    });

    const setupFetchClientTask = Cadenza.get("Setup fetch client") as AnyObject;
    expect(setupFetchClientTask).toBeDefined();
    setupFetchClientTask.taskFunction({
      serviceName: "OrdersService",
      serviceOrigin: "http://orders.example:7000",
      serviceTransportId: "orders-public-1",
      routeKey: buildRouteKey(
        "OrdersService",
        "public",
        "http://orders.example:7000",
      ),
      fetchId: fetchHandle,
    });

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceName: "OrdersService",
      serviceInstanceId: "orders-1",
      serviceTransportId: "orders-public-1",
      serviceOrigin: "http://orders.example:7000",
      transportProtocols: ["rest"],
      communicationTypes: ["rest"],
    });

    await waitForCondition(() => {
      const observer =
        Cadenza.inquiryBroker.inquiryObservers.get("orders-lookup");
      return Boolean(observer && observer.tasks.size === 1);
    });
    await waitForCondition(() => {
      const instance = (ServiceRegistry.instance as any).instances
        .get("OrdersService")
        ?.find((entry: AnyObject) => entry.uuid === "orders-1");
      return Boolean(instance?.clientReadyTransportIds?.length);
    });
    const observer = Cadenza.inquiryBroker.inquiryObservers.get("orders-lookup");
    expect(observer?.tasks.size).toBe(1);

    Cadenza.run((ServiceRegistry.instance as any).getBalancedInstance, {
      __remoteRoutineName: "LookupOrders",
      __serviceName: "OrdersService",
      __timeout: 500,
      __metadata: {
        __deputyExecId: "orders-lookup-deputy-1",
      },
    });

    await waitForCondition(() => selectedContexts.length === 1);

    expect(selectedContexts[0]).toEqual(
      expect.objectContaining({
        __serviceName: "OrdersService",
        __instance: "orders-1",
        __transportId: "orders-public-1",
        __transportOrigin: "http://orders.example:7000",
        __transportProtocol: "rest",
        __fetchId: fetchHandle,
      }),
    );
  });

  it("keeps public routing after internal runtime status updates", async () => {
    const selectedContexts: AnyObject[] = [];
    const fetchHandle = buildHandleKey(
      buildRouteKey("OrdersService", "public", "http://orders.example:7000"),
      "rest",
    );

    Cadenza.createMetaTask("Capture public routing after runtime status updates", (ctx) => {
      selectedContexts.push(ctx);
      return true;
    }).doOn(`meta.service_registry.selected_instance_for_fetch:${fetchHandle}`);

    Cadenza.createCadenzaService("BrowserApp", "Frontend app", {
      isFrontend: true,
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "browser-app-3c",
    });

    await waitForCondition(
      () => ServiceRegistry.instance.serviceInstanceId === "browser-app-3c",
    );

    Cadenza.emit("meta.initializing_service", {
      serviceInstance: {
        uuid: "orders-2",
        serviceName: "OrdersService",
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        isFrontend: false,
        health: {},
        transports: [
          {
            uuid: "orders-public-2",
            service_instance_id: "orders-2",
            role: "public",
            origin: "http://orders.example:7000",
            protocols: ["rest", "socket"],
          },
        ],
      },
    });
    Cadenza.emit("global.meta.graph_metadata.task_intent_associated", {
      intentName: "orders-lookup",
      serviceName: "OrdersService",
      taskName: "LookupOrders",
      taskVersion: 1,
    });

    await waitForCondition(() => {
      const instance = (ServiceRegistry.instance as any).instances
        .get("OrdersService")
        ?.find((entry: AnyObject) => entry.uuid === "orders-2");
      return Boolean(instance);
    });

    Cadenza.emit("meta.fetch.handshake_complete", {
      serviceName: "OrdersService",
      serviceInstanceId: "orders-2",
      serviceTransportId: "orders-public-2",
      serviceOrigin: "http://orders.example:7000",
      transportProtocols: ["rest"],
      communicationTypes: ["rest"],
    });

    await waitForCondition(() => {
      const ordersInstance = (ServiceRegistry.instance as any).instances
        .get("OrdersService")
        ?.find((instance: AnyObject) => instance.uuid === "orders-2");
      return Boolean(ordersInstance);
    });

    const applied = (ServiceRegistry.instance as any).applyRuntimeStatusReport({
      serviceName: "OrdersService",
      serviceInstanceId: "orders-2",
      transportId: "orders-internal-2",
      transportRole: "internal",
      transportOrigin: "http://orders-internal:3000",
      transportProtocols: ["rest"],
      isActive: true,
      isNonResponsive: false,
      isBlocked: false,
      numberOfRunningGraphs: 1,
      state: "healthy",
      acceptingWork: true,
      health: {},
      reportedAt: new Date().toISOString(),
    });
    expect(applied).toBe(true);

    await waitForCondition(() => {
      const observer =
        Cadenza.inquiryBroker.inquiryObservers.get("orders-lookup");
      return Boolean(observer && observer.tasks.size === 1);
    });
    await waitForCondition(() => {
      const instance = (ServiceRegistry.instance as any).instances
        .get("OrdersService")
        ?.find((entry: AnyObject) => entry.uuid === "orders-2");
      return Boolean(instance?.clientReadyTransportIds?.length);
    });
    Cadenza.run((ServiceRegistry.instance as any).getBalancedInstance, {
      __remoteRoutineName: "LookupOrders",
      __serviceName: "OrdersService",
      __timeout: 500,
      __metadata: {
        __deputyExecId: "orders-lookup-deputy-2",
      },
    });

    await waitForCondition(() => selectedContexts.length === 1);

    expect(selectedContexts[0]).toEqual(
      expect.objectContaining({
        __instance: "orders-2",
        __transportId: "orders-public-2",
        __transportOrigin: "http://orders.example:7000",
        __transportProtocol: "rest",
        __fetchId: fetchHandle,
      }),
    );

    const ordersInstance = (ServiceRegistry.instance as any).instances
      .get("OrdersService")
      ?.find((instance: AnyObject) => instance.uuid === "orders-2");
    expect(ordersInstance?.transports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          uuid: "orders-public-2",
          role: "public",
          origin: "http://orders.example:7000",
        }),
        expect.objectContaining({
          uuid: "orders-internal-2",
          role: "internal",
          origin: "http://orders-internal:3000",
        }),
      ]),
    );
  });

  it("rejects requireComplete inquiries when no eligible responders exist", async () => {
    await expect(
      Cadenza.inquire(
        "missing-intent",
        {
          requestId: "req-1",
        },
        {
          requireComplete: true,
        },
      ),
    ).rejects.toMatchObject({
      errored: true,
      __error: "Inquiry 'missing-intent' had no eligible responders",
      __inquiryMeta: expect.objectContaining({
        inquiry: "missing-intent",
        eligibleResponders: 0,
        responded: 0,
      }),
    });
  });

  it("preserves nested responder errors when requireComplete rejects a failed inquiry", async () => {
    const intentName = "failing-intent-preserves-error";

    Cadenza.createTask("Failing responder with nested error", () => ({
      errored: true,
      returnedValue: {
        __error:
          "No routeable internal transport available for IotDbService. Waiting for authority route updates before retrying.",
      },
    })).respondsTo(intentName);

    await expect(
      Cadenza.inquire(
        intentName,
        {},
        {
          requireComplete: true,
        },
      ),
    ).rejects.toMatchObject({
      errored: true,
      __error:
        "No routeable internal transport available for IotDbService. Waiting for authority route updates before retrying.",
    });
  });

  it("normalizes mixed full-sync payloads for remote intents and signals", async () => {
    const socketSelections: AnyObject[] = [];

    Cadenza.createCadenzaService("BrowserApp", "Frontend app", {
      isFrontend: true,
      useSocket: true,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "browser-app-3b",
    });

    await waitForCondition(
      () => ServiceRegistry.instance.serviceInstanceId === "browser-app-3b",
    );

    const originalInquire = Cadenza.inquire.bind(Cadenza);
    vi.spyOn(Cadenza, "inquire").mockImplementation(async (inquiry, ctx, options) => {
      if (inquiry === "meta-service-registry-full-sync") {
        return {
          signal_to_task_maps: [
            {
              signal_name: "global.orders.updated",
              service_name: "OrdersService",
              is_global: true,
              deleted: false,
            },
          ],
          intent_to_task_maps: [
            {
              intent_name: "orders-lookup",
              service_name: "OrdersService",
              task_name: "LookupOrders",
              task_version: 1,
              deleted: false,
            },
          ],
          service_instances: [
            {
              uuid: "orders-1b",
              service_name: "OrdersService",
              is_active: true,
              is_non_responsive: false,
              is_blocked: false,
              is_frontend: false,
              health: {},
            },
          ],
          service_instance_transports: [
            {
              uuid: "orders-public-1b",
              service_instance_id: "orders-1b",
              role: "public",
              origin: "http://orders.example:7000",
              protocols: ["rest", "socket"],
              deleted: false,
            },
          ],
        };
      }

      return originalInquire(inquiry, ctx ?? {}, options ?? {});
    });

    Cadenza.signalBroker.registerEmittedSignal("global.orders.updated");
    Cadenza.createTask("Emit browser orders signal", (_ctx, emit) => {
      emit("global.orders.updated", {
        orderId: "order-1b",
      });
      return true;
    }).doOn("browser.orders.updated");

    Cadenza.emit("meta.sync_requested", {});

    await waitForCondition(() => {
      const observer =
        Cadenza.inquiryBroker.inquiryObservers.get("orders-lookup");
      return Boolean(observer && observer.tasks.size === 1);
    });
    await waitForCondition(() =>
      Boolean(
        (ServiceRegistry.instance as any).instances.get("OrdersService")?.[0],
      ),
    );

    const ordersInstance = (ServiceRegistry.instance as any).instances.get(
      "OrdersService",
    )?.[0];

    expect(ordersInstance).toEqual(
      expect.objectContaining({
        uuid: "orders-1b",
        serviceName: "OrdersService",
      }),
    );
    expect(ordersInstance?.transports).toEqual([
      expect.objectContaining({
        uuid: "orders-public-1b",
        role: "public",
        origin: "http://orders.example:7000",
      }),
    ]);

    const normalizedTransport = ordersInstance?.transports?.[0];
    const socketHandle = buildHandleKey(
      normalizedTransport?.role && normalizedTransport?.origin
        ? buildRouteKey(
            "OrdersService",
            normalizedTransport.role,
            normalizedTransport.origin,
          )
        : String(normalizedTransport?.uuid ?? ""),
      "socket",
    );

    Cadenza.createMetaTask("Capture normalized mixed full sync socket selection", (ctx) => {
      socketSelections.push(ctx);
      return true;
    }).doOn(`meta.service_registry.selected_instance_for_socket:${socketHandle}`);

    Cadenza.emit("meta.socket.handshake", {
      serviceName: "OrdersService",
      serviceInstanceId: "orders-1b",
      serviceTransportId: "orders-public-1b",
      serviceOrigin: "http://orders.example:7000",
      transportProtocols: ["socket"],
      communicationTypes: ["socket"],
    });
    await waitForCondition(() => {
      const instance = (ServiceRegistry.instance as any).instances
        .get("OrdersService")
        ?.find((entry: AnyObject) => entry.uuid === "orders-1b");
      return Boolean(instance?.clientReadyTransportIds?.length);
    });
    expect(
      Cadenza.createSignalTransmissionTask("global.orders.updated", "OrdersService") ??
        Cadenza.get("Transmit signal: global.orders.updated to OrdersService"),
    ).toBeDefined();

    Cadenza.run((ServiceRegistry.instance as any).getBalancedInstance, {
      __serviceName: "OrdersService",
      __signalName: "global.orders.updated",
      __routineExecId: "browser-orders-signal-1",
    });

    await waitForCondition(() => socketSelections.length === 1);

    expect(socketSelections[0]).toEqual(
      expect.objectContaining({
        __signalName: "global.orders.updated",
        __serviceName: "OrdersService",
        __instance: "orders-1b",
        __transportId: "orders-public-1b",
        __transportProtocol: "socket",
        __fetchId: socketHandle,
      }),
    );
  });

  it("transmits remote signals in frontend mode through socket-selected instances", async () => {
    const socketSelections: AnyObject[] = [];
    const fetchSelections: AnyObject[] = [];
    const socketHandle = buildHandleKey(
      buildRouteKey("OrdersService", "public", "http://orders.example:7000"),
      "socket",
    );
    const fetchHandle = buildHandleKey(
      buildRouteKey("OrdersService", "public", "http://orders.example:7000"),
      "rest",
    );

    Cadenza.createMetaTask("Capture remote socket signal selection", (ctx) => {
      socketSelections.push(ctx);
      return true;
    }).doOn(`meta.service_registry.selected_instance_for_socket:${socketHandle}`);

    Cadenza.createMetaTask("Capture unexpected remote fetch signal selection", (ctx) => {
      fetchSelections.push(ctx);
      return true;
    }).doOn(`meta.service_registry.selected_instance_for_fetch:${fetchHandle}`);

    Cadenza.createCadenzaService("BrowserApp", "Frontend app", {
      isFrontend: true,
      useSocket: true,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "browser-app-4",
    });

    await waitForCondition(
      () => ServiceRegistry.instance.serviceInstanceId === "browser-app-4",
    );

    const emitSpy = vi.spyOn(Cadenza, "emit");

    Cadenza.emit("meta.initializing_service", {
      serviceInstance: {
        uuid: "orders-2",
        serviceName: "OrdersService",
        numberOfRunningGraphs: 0,
        isActive: true,
        isNonResponsive: false,
        isBlocked: false,
        health: {},
        isFrontend: false,
        transports: [
          {
            uuid: "orders-public-2",
            service_instance_id: "orders-2",
            role: "public",
            origin: "http://orders.example:7000",
            protocols: ["rest", "socket"],
          },
        ],
      },
    });

    await waitForCondition(() => {
      const instance = (ServiceRegistry.instance as any).instances
        .get("OrdersService")
        ?.find((entry: AnyObject) => entry.uuid === "orders-2");
      return Boolean(instance);
    });

    Cadenza.emit("meta.socket.handshake", {
      serviceName: "OrdersService",
      serviceInstanceId: "orders-2",
      serviceTransportId: "orders-public-2",
      serviceOrigin: "http://orders.example:7000",
      transportProtocols: ["socket"],
      communicationTypes: ["socket"],
    });
    await waitForCondition(() => {
      const instance = (ServiceRegistry.instance as any).instances
        .get("OrdersService")
        ?.find((entry: AnyObject) => entry.uuid === "orders-2");
      return Boolean(instance?.clientReadyTransportIds?.length);
    });

    expect(
      Cadenza.createSignalTransmissionTask("global.orders.updated", "OrdersService") ??
        Cadenza.get("Transmit signal: global.orders.updated to OrdersService"),
    ).toBeDefined();

    Cadenza.run((ServiceRegistry.instance as any).getBalancedInstance, {
      __serviceName: "OrdersService",
      __signalName: "global.orders.updated",
      __routineExecId: "browser-orders-signal-2",
    });

    await waitForCondition(() => socketSelections.length === 1);

    expect(fetchSelections).toEqual([]);
    expect(socketSelections[0]).toEqual(
      expect.objectContaining({
        __signalName: "global.orders.updated",
        __serviceName: "OrdersService",
        __instance: "orders-2",
        __transportId: "orders-public-2",
        __transportProtocol: "socket",
        __fetchId: socketHandle,
      }),
    );
  });

  it("keeps actor session persistence available in frontend mode", async () => {
    Cadenza.createMetaTask("dbInsertActorSessionState", (ctx) => ({
      ...ctx,
      __success: true,
      rowCount: 1,
    }));
    Cadenza.createMetaTask(
      "Insert actor_session_state in CadenzaDB",
      (ctx) => ({
        ...ctx,
        __success: true,
        rowCount: 1,
      }),
    );

    Cadenza.createCadenzaService("BrowserApp", "Frontend app", {
      isFrontend: true,
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "browser-app-5",
    });

    await waitForCondition(() => Boolean(Cadenza.get("Persist actor session state")));

    const response = await Cadenza.inquire(
      META_ACTOR_SESSION_STATE_PERSIST_INTENT,
      {
        actor_name: "SessionActor",
        actor_version: 1,
        actor_key: "browser-session",
        durable_state: {
          token: "abc123",
        },
        durable_version: 2,
      },
      {
        overallTimeoutMs: 500,
      },
    );

    expect(response).toEqual(
      expect.objectContaining({
        persisted: true,
        actor_name: "SessionActor",
        actor_key: "browser-session",
      }),
    );
  });
});
