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

  it("routes distributed inquiries in frontend mode through remote intent deputies", async () => {
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

    Cadenza.createMetaTask("Fake fetch inquiry transport", (ctx, emit) => {
      emit(`meta.fetch.delegated:${ctx.__metadata.__deputyExecId}`, {
        orders: [{ id: "order-1" }],
      });
      return true;
    }).doOn("meta.service_registry.selected_instance_for_fetch:orders-public-1");

    Cadenza.emit("global.meta.cadenza_db.gathered_sync_data", {
      signalToTaskMaps: [],
      intentToTaskMaps: [
        {
          intentName: "orders-lookup",
          serviceName: "OrdersService",
          taskName: "LookupOrders",
          taskVersion: 1,
        },
      ],
      serviceInstances: [
        {
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
      ],
    });

    await waitForCondition(() => {
      const observer =
        Cadenza.inquiryBroker.inquiryObservers.get("orders-lookup");
      return Boolean(observer && observer.tasks.size === 1);
    });

    const response = await Cadenza.inquire(
      "orders-lookup",
      {
        accountId: "acct-1",
      },
      {
        overallTimeoutMs: 500,
      },
    );

    expect(response.orders).toEqual([{ id: "order-1" }]);
    expect(response.__inquiryMeta).toEqual(
      expect.objectContaining({
        inquiry: "orders-lookup",
        responded: 1,
        failed: 0,
      }),
    );
  });

  it("normalizes mixed full-sync payloads for remote intents and signals", async () => {
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

    const transmissions: AnyObject[] = [];

    Cadenza.createMetaTask("Fake full sync fetch inquiry transport", (ctx, emit) => {
      emit(`meta.fetch.delegated:${ctx.__metadata.__deputyExecId}`, {
        orders: [{ id: "order-1b" }],
      });
      return true;
    }).doOn("meta.service_registry.selected_instance_for_fetch:orders-public-1b");

    Cadenza.createMetaTask("Track mixed-shape socket signal transmission", (ctx, emit) => {
      transmissions.push(ctx);
      emit(`meta.socket_client.transmitted:${ctx.__routineExecId}`, {
        __status: "success",
      });
      return true;
    }).doOn("meta.service_registry.selected_instance_for_socket:orders-public-1b");

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
      }),
    ]);

    Cadenza.emit("browser.orders.updated", {});

    await waitForCondition(() => transmissions.length === 1);
    expect(transmissions[0]).toEqual(
      expect.objectContaining({
        __signalName: "global.orders.updated",
      }),
    );
  });

  it("transmits remote signals in frontend mode through socket-selected instances", async () => {
    const transmissions: AnyObject[] = [];
    const fetchSelections: AnyObject[] = [];

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

    Cadenza.createMetaTask("Track socket signal transmission", (ctx, emit) => {
      transmissions.push(ctx);
      emit(`meta.socket_client.transmitted:${ctx.__routineExecId}`, {
        __status: "success",
      });
      return true;
    }).doOn("meta.service_registry.selected_instance_for_socket:orders-public-2");

    Cadenza.createMetaTask("Track fetch signal transmission", (ctx) => {
      fetchSelections.push(ctx);
      return true;
    }).doOn("meta.service_registry.selected_instance_for_fetch:orders-public-2");

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

    Cadenza.createSignalTransmissionTask(
      "global.orders.updated",
      "OrdersService",
    );

    Cadenza.emit("global.orders.updated", {
      orderId: "order-99",
    });

    await waitForCondition(
      () => transmissions.length + fetchSelections.length === 1,
    );

    expect(fetchSelections).toEqual([]);
    expect(transmissions[0]).toEqual(
      expect.objectContaining({
        __signalName: "global.orders.updated",
        __serviceName: "OrdersService",
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
