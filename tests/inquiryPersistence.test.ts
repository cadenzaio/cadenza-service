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
      port: 3211,
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

  it("emits business inquiry lifecycle metadata", async () => {
    const inquiryCreatedPayloads: AnyObject[] = [];
    const inquiryUpdatedPayloads: AnyObject[] = [];
    Cadenza.createMetaTask("Capture inquiry created metadata", (ctx) => {
      inquiryCreatedPayloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.inquiry_created");

    Cadenza.createMetaTask("Capture inquiry updated metadata", (ctx) => {
      inquiryUpdatedPayloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.inquiry_updated");

    Cadenza.createTask("Lookup orders", () => ({
      orders: [{ id: "order-1" }],
    })).respondsTo("orders-lookup");

    const result = await Cadenza.inquire(
      "orders-lookup",
      { temperature: 72 },
      { timeout: 100 },
    );

    expect(result.orders).toEqual([{ id: "order-1" }]);
    await waitForCondition(
      () => inquiryCreatedPayloads.length > 0 && inquiryUpdatedPayloads.length > 0,
    );

    const createdData = inquiryCreatedPayloads[0].data as AnyObject;
    const updatedData = inquiryUpdatedPayloads[0].data as AnyObject;
    const updatedFilter = inquiryUpdatedPayloads[0].filter as AnyObject;
    const inquiryUuid = String(readField(createdData, "uuid"));

    expect(readField(createdData, "name")).toBe("orders-lookup");
    expect(readField(createdData, "service_name")).toBe("InquiryService");
    expect(readField(createdData, "service_instance_id")).toBe(
      "inquiry-service-1",
    );
    expect(readField(createdData, "is_meta")).toBe(false);
    expect((readField(createdData, "context") as AnyObject)?.temperature).toBe(72);
    expect(readField(updatedFilter, "uuid")).toBe(inquiryUuid);
    expect(typeof readField(updatedData, "fulfilled_at")).toBe("string");
    expect(typeof readField(updatedData, "duration")).toBe("number");
  });

  it("preserves inquiry ids in task execution creation metadata", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createTask("Lookup orders", () => ({
      orders: [{ id: "order-1" }],
    }));

    Cadenza.createMetaTask("Capture task execution inquiry metadata", (ctx) => {
      payload = ctx;
      return true;
    }).doOn("global.meta.graph_metadata.task_execution_created");

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

  it("preserves outer inquiry ids in delegated inquiry requests", async () => {
    let delegatedContext: AnyObject | undefined;

    Cadenza.createMetaTask("Capture delegated inquiry metadata", (ctx) => {
      delegatedContext = ctx;
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
      return true;
    }).doOn("meta.deputy.delegation_requested");

    Cadenza.createDeputyTask(
      "Lookup remote orders",
      "RemoteOrdersService",
    ).respondsTo("orders-lookup-remote");

    const result = await Cadenza.inquire(
      "orders-lookup-remote",
      { orderId: "order-1" },
      { timeout: 100 },
    );

    await waitForCondition(() => Boolean(delegatedContext?.__inquiryId));

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
      executionPayloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.task_execution_created");

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

  it("clears previous routine linkage for inquiry-driven routines", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createTask("Lookup orders", () => ({
      orders: [{ id: "order-1" }],
    }));

    Cadenza.createMetaTask("Capture routine execution creation metadata", (ctx) => {
      payload = ctx;
      return true;
    }).doOn("global.meta.graph_metadata.routine_execution_created");

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
        previousRoutineExecution: "previous-routine-1",
        created: "2026-03-21T00:00:00.000Z",
      },
    });

    await waitForCondition(() => Boolean(payload?.data));

    expect(readField(payload?.data as AnyObject, "previous_routine_execution")).toBe(
      null,
    );
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
});
