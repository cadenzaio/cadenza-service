import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { AnyObject } from "@cadenza.io/core";

import Cadenza from "../src/Cadenza";
import DatabaseController from "@service-database-controller";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import GraphSyncController from "../src/graph/controllers/GraphSyncController";
import RestController from "../src/network/RestController";
import ServiceRegistry from "../src/registry/ServiceRegistry";
import SignalController from "../src/signals/SignalController";
import SocketController from "../src/network/SocketController";
import { EXECUTION_PERSISTENCE_BUNDLE_SIGNAL } from "../src/execution/ExecutionPersistenceCoordinator";

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

function readField(data: AnyObject | undefined, snakeKey: string): unknown {
  if (!data) {
    return undefined;
  }

  const camelKey = snakeKey.replace(/_([a-z])/g, (_match, letter: string) =>
    letter.toUpperCase(),
  );
  return data[snakeKey] ?? data[camelKey];
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

describe("inquiry persistence", () => {
  beforeEach(() => {
    resetRuntimeState();
    Cadenza.createCadenzaService("InquiryService", "Inquiry test service", {
      port: 0,
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "inquiry-service-1",
    });
    GraphMetadataController.instance;
  });

  afterEach(() => {
    resetRuntimeState();
  });

  it("ensures inquiry persistence before completion updates", async () => {
    const inquiryCreatedPayloads: AnyObject[] = [];
    const inquiryCompletedPayloads: AnyObject[] = [];

    Cadenza.createMetaTask("Capture inquiry created metadata", (ctx) => {
      inquiryCreatedPayloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.inquiry_created");

    Cadenza.createMetaTask("Capture inquiry completed payload", (ctx) => {
      inquiryCompletedPayloads.push(ctx);
      return true;
    }).doOn("meta.inquiry_broker.inquiry_completed");

    Cadenza.createTask("Lookup orders", () => ({
      orders: [{ id: "order-1" }],
    })).respondsTo("orders-lookup");

    Cadenza.emit("meta.service_registry.registered_global_signals", {
      serviceName: "InquiryService",
    });
    Cadenza.emit("meta.service_registry.registered_global_intents", {
      serviceName: "InquiryService",
    });
    await waitForCondition(() => Cadenza.hasCompletedBootstrapSync());

    const result = await Cadenza.inquire(
      "orders-lookup",
      { temperature: 72, __inquirySourceRoutineExecutionId: "routine-source-1" },
      { timeout: 100 },
    );

    expect(result.orders).toEqual([{ id: "order-1" }]);
    await waitForCondition(
      () => inquiryCreatedPayloads.length > 0 && inquiryCompletedPayloads.length > 0,
    );

    const createdData = inquiryCreatedPayloads[0].data as AnyObject;
    const completedPayload = inquiryCompletedPayloads[0];
    const insertData = completedPayload.insertData as AnyObject;
    const updatedData = completedPayload.data as AnyObject;
    const updatedFilter = completedPayload.filter as AnyObject;
    const inquiryUuid = String(readField(createdData, "uuid"));

    expect(readField(createdData, "name")).toBe("orders-lookup");
    expect(readField(createdData, "service_name")).toBe("InquiryService");
    expect(readField(createdData, "service_instance_id")).toBe(
      "inquiry-service-1",
    );
    expect(readField(createdData, "is_meta")).toBe(false);
    expect(readField(createdData, "routine_execution_id")).toBeNull();
    expect((readField(createdData, "context") as AnyObject)?.temperature).toBe(72);
    expect(readField(insertData, "uuid")).toBe(inquiryUuid);
    expect(readField(updatedFilter, "uuid")).toBe(inquiryUuid);
    expect(typeof readField(updatedData, "fulfilled_at")).toBe("string");
    expect(typeof readField(updatedData, "duration")).toBe("number");
    expect(readField(updatedData, "routine_execution_id")).toBe("routine-source-1");
  });

  it("preserves inquiry ids in task execution creation metadata", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createTask("Lookup orders", () => ({
      orders: [{ id: "order-1" }],
    }));

    Cadenza.createMetaTask("Capture task execution bundle", (ctx) => {
      const ensures = Array.isArray(ctx?.ensures) ? (ctx.ensures as AnyObject[]) : [];
      const taskExecutionEvent = ensures.find(
        (event) => event?.entityType === "task_execution",
      );

      if (taskExecutionEvent) {
        payload = taskExecutionEvent;
      }
      return true;
    }).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    Cadenza.emit("meta.service_registry.registered_global_signals", {
      serviceName: "InquiryService",
    });
    Cadenza.emit("meta.service_registry.registered_global_intents", {
      serviceName: "InquiryService",
    });
    await waitForCondition(() => Cadenza.hasCompletedBootstrapSync());

    Cadenza.emit("meta.node.scheduled", {
      data: {
        uuid: "task-exec-1",
        routineExecutionId: "routine-exec-1",
        executionTraceId: "trace-1",
        inquiryId: "inquiry-1",
        context: { temperature: 72 },
        metaContext: {},
        taskName: "Lookup orders",
        taskVersion: 1,
        isMeta: false,
        isScheduled: true,
        splitGroupId: "routine-exec-1",
        signalEmissionId: null,
        previousExecutionIds: {
          ids: [],
        },
        created: "2026-03-21T00:00:00.000Z",
      },
    });

    await waitForCondition(() => Boolean(payload?.data));

    const data = payload?.data as AnyObject;
    expect(readField(data, "task_name")).toBe("Lookup orders");
    expect(readField(data, "inquiry_id")).toBe("inquiry-1");
    expect(readField(data, "service_name")).toBe("InquiryService");
    expect(readField(data, "service_instance_id")).toBe("inquiry-service-1");
  });

  it("omits local service_instance_id from inquiry execution rows before bootstrap sync completes", async () => {
    const bundles: AnyObject[] = [];

    Cadenza.createMetaTask("Capture pre-bootstrap inquiry bundle", (ctx) => {
      bundles.push(ctx);
      return true;
    }).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    Cadenza.emit("meta.inquiry_broker.inquiry_started", {
      data: {
        uuid: "inquiry-pre-bootstrap-1",
        name: "orders-lookup",
        executionTraceId: "trace-pre-bootstrap-1",
        sentAt: "2026-03-27T00:00:00.000Z",
        created: "2026-03-27T00:00:00.000Z",
        context: { orderId: "order-1" },
        isMeta: false,
      },
    });

    await waitForCondition(() => bundles.length === 1);

    const ensures = Array.isArray(bundles[0]?.ensures)
      ? (bundles[0].ensures as AnyObject[])
      : [];
    const executionTraceEvent = ensures.find(
      (event) => event?.entityType === "execution_trace",
    );

    expect(executionTraceEvent?.data?.service_instance_id).toBeUndefined();
  });

  it("preserves outer inquiry ids in delegated inquiry requests", async () => {
    let delegatedContext: AnyObject | undefined;

    Cadenza.createMetaTask("Capture delegated inquiry metadata", (ctx) => {
      if (
        ctx.__serviceName !== "RemoteOrdersService" &&
        ctx.serviceName !== "RemoteOrdersService"
      ) {
        return true;
      }

      delegatedContext = ctx;
      if (ctx.__metadata?.__deputyExecId) {
        queueMicrotask(() => {
          Cadenza.emit(
            `meta.service_registry.load_balance_failed:${ctx.__metadata.__deputyExecId}`,
            {
              ...ctx,
              errored: true,
              __error: "Synthetic load-balance failure",
            },
          );
        });
      }
      return true;
    }).doOn(
      "meta.service_registry.load_balance_requested",
      "meta.deputy.delegation_requested",
    );

    Cadenza.createDeputyTask(
      "Lookup remote orders",
      "RemoteOrdersService",
    ).respondsTo("orders-lookup-remote");

    const result = await Cadenza.inquire(
      "orders-lookup-remote",
      { orderId: "order-1" },
      { timeout: 100 },
    );

    await waitForCondition(() => Boolean(delegatedContext?.__inquiryId), 2_000);

    expect(result.__inquiryMeta?.failed).toBe(1);
    expect(delegatedContext?.__inquiryId).toEqual(expect.any(String));
    expect(delegatedContext?.__metadata?.__inquiryId).toBe(
      delegatedContext?.__inquiryId,
    );
  });

  it("does not persist deputy task executions or maps", async () => {
    const deputyTask = Cadenza.createDeputyTask(
      "Lookup orders remotely",
      "RemoteOrdersService",
    );
    Cadenza.createTask("Persist local orders", () => true);

    const executionPayloads: AnyObject[] = [];
    const mapPayloads: AnyObject[] = [];

    Cadenza.createMetaTask("Capture deputy execution metadata", (ctx) => {
      if (
        ctx.__remoteRoutineName === "Insert task_execution" &&
        ctx.data?.uuid === "task-exec-proxy"
      ) {
        executionPayloads.push(ctx);
      }
      return true;
    }).doOn("meta.deputy.delegation_requested");

    Cadenza.createMetaTask("Capture deputy execution map metadata", (ctx) => {
      mapPayloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.task_execution_mapped");

    Cadenza.emit("meta.node.scheduled", {
      data: {
        uuid: "task-exec-proxy",
        routineExecutionId: "routine-proxy",
        executionTraceId: "trace-proxy",
        inquiryId: null,
        context: {},
        metaContext: {},
        taskName: deputyTask.name,
        taskVersion: 1,
        isMeta: false,
        isScheduled: true,
        splitGroupId: "routine-proxy",
        signalEmissionId: null,
        previousExecutionIds: {
          ids: [],
        },
        created: "2026-03-21T00:00:00.000Z",
      },
    });

    Cadenza.emit("meta.node.mapped", {
      data: {
        taskExecutionId: "task-exec-local",
        previousTaskExecutionId: "task-exec-proxy",
      },
      filter: {
        taskName: "Persist local orders",
        taskVersion: 1,
        predecessorTaskName: deputyTask.name,
        predecessorTaskVersion: 1,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(executionPayloads).toEqual([]);
    expect(mapPayloads).toEqual([]);
  });

  it("does not emit legacy task execution map bundles", async () => {
    let emittedLegacyMapBundle = false;

    Cadenza.createTask("Lookup orders", () => true);
    Cadenza.createTask("Persist local orders", () => true);

    Cadenza.createMetaTask("Capture task execution map bundle", (ctx) => {
      const ensures = Array.isArray(ctx?.ensures) ? (ctx.ensures as AnyObject[]) : [];
      const taskExecutionMapEvent = ensures.find(
        (event) => event?.entityType === "task_execution_map",
      );

      if (taskExecutionMapEvent) {
        emittedLegacyMapBundle = true;
      }
      return true;
    }).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    Cadenza.emit("meta.node.mapped", {
      data: {
        taskExecutionId: "task-exec-2",
        previousTaskExecutionId: "task-exec-1",
        executionTraceId: "trace-map-1",
      },
      filter: {
        taskName: "Persist local orders",
        taskVersion: 1,
        predecessorTaskName: "Lookup orders",
        predecessorTaskVersion: 1,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(emittedLegacyMapBundle).toBe(false);
  });

  it("does not emit removed routine-parent linkage for inquiry-driven routines", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createTask("Lookup orders", () => ({
      orders: [{ id: "order-1" }],
    }));

    Cadenza.createMetaTask("Capture routine execution bundle", (ctx) => {
      const ensures = Array.isArray(ctx?.ensures) ? (ctx.ensures as AnyObject[]) : [];
      const routineEvent = ensures.find(
        (event) => event?.entityType === "routine_execution",
      );

      if (routineEvent) {
        payload = routineEvent;
      }
      return true;
    }).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    Cadenza.emit("meta.runner.added_tasks", {
      data: {
        uuid: "routine-exec-1",
        name: "Lookup orders",
        routineVersion: 1,
        isMeta: false,
        executionTraceId: "trace-1",
        context: { orderId: "order-1" },
        metaContext: {
          __inquiryId: "inquiry-1",
        },
        created: "2026-03-21T00:00:00.000Z",
      },
    });

    await waitForCondition(() => Boolean(payload?.data));

    expect(
      Object.prototype.hasOwnProperty.call(payload?.data ?? {}, "previous_routine_execution"),
    ).toBe(false);
    expect(
      Object.prototype.hasOwnProperty.call(payload?.data ?? {}, "routine_version"),
    ).toBe(false);
    expect(
      Object.prototype.hasOwnProperty.call(payload?.data ?? {}, "routineVersion"),
    ).toBe(false);
  });

  it("does not persist meta inquiries", async () => {
    const inquiryCreatedPayloads: AnyObject[] = [];
    const inquiryUpdatedPayloads: AnyObject[] = [];

    Cadenza.createMetaTask("Capture meta inquiry created metadata", (ctx) => {
      inquiryCreatedPayloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.inquiry_created");

    Cadenza.createMetaTask("Capture meta inquiry updated metadata", (ctx) => {
      inquiryUpdatedPayloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.inquiry_updated");

    Cadenza.createMetaTask("Resolve meta full sync", () => ({
      ok: true,
    })).respondsTo("meta-service-registry-full-sync");

    const result = await Cadenza.inquire(
      "meta-service-registry-full-sync",
      { syncScope: "service-registry-full-sync" },
      { timeout: 100 },
    );

    expect(result.ok).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(inquiryCreatedPayloads).toEqual([]);
    expect(inquiryUpdatedPayloads).toEqual([]);
  });

  it("does not emit execution-trace persistence for meta traces", async () => {
    const executionTracePayloads: AnyObject[] = [];

    Cadenza.createMetaTask("Capture execution trace metadata", (ctx) => {
      executionTracePayloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.execution_trace_created");

    Cadenza.emit("meta.service_registry.registered_global_signals", {
      serviceName: "InquiryService",
    });
    Cadenza.emit("meta.service_registry.registered_global_intents", {
      serviceName: "InquiryService",
    });
    await waitForCondition(() => Cadenza.hasCompletedBootstrapSync());

    const metaTask = Cadenza.createMetaTask("Run meta trace only", () => true, "", {
      register: false,
      isHidden: true,
    });

    Cadenza.run(metaTask, {});
    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(executionTracePayloads).toHaveLength(0);

    const businessTask = Cadenza.createTask("Run business trace", () => true);
    Cadenza.run(businessTask, {});

    await waitForCondition(() => executionTracePayloads.length === 1);
    expect(executionTracePayloads[0].data).toMatchObject({
      service_name: "InquiryService",
      is_meta: false,
    });
  });
});
