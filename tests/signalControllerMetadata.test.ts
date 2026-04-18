import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AnyObject } from "@cadenza.io/core";
import Cadenza from "../src/Cadenza";
import { EXECUTION_PERSISTENCE_BUNDLE_SIGNAL } from "../src/execution/ExecutionPersistenceCoordinator";
import SignalController from "../src/signals/SignalController";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import SocketController from "../src/network/SocketController";

function resetRuntimeState() {
  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run reset errors before bootstrap.
  }

  (SignalController as any)._instance = undefined;
  (GraphMetadataController as any)._instance = undefined;
  (SocketController as any)._instance = undefined;
}

async function waitForCondition(
  predicate: () => boolean,
  timeoutMs = 2500,
  pollIntervalMs = 10,
): Promise<void> {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }

  throw new Error("Condition not met within timeout");
}

describe("Signal controller metadata contracts", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  beforeEach(() => {
    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.serviceRegistry.serviceName = "SignalMetadataService";
    SignalController.instance;
  });

  it("emits signal-added metadata with database-ready queryData", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createMetaTask("Capture signal added metadata", (ctx) => {
      payload = ctx;
      return true;
    }).doOn("global.meta.signal_controller.signal_added");

    Cadenza.emit("meta.signal_broker.added", {
      signalName: "global.iot.telemetry.ingested",
    });

    await waitForCondition(() => Boolean(payload?.queryData));

    expect((payload?.data as AnyObject)?.name).toBe(
      "global.iot.telemetry.ingested",
    );
    expect((payload?.queryData as AnyObject)?.data?.name).toBe(
      "global.iot.telemetry.ingested",
    );
    expect((payload?.queryData as AnyObject)?.data?.action).toBe("ingested");
    expect(typeof (payload?.queryData as AnyObject)?.data?.domain).toBe("string");
    expect((payload?.queryData as AnyObject)?.data?.is_global).toBe(true);
    expect((payload?.queryData as AnyObject)?.data?.is_meta).toBe(false);
    expect((payload?.queryData as AnyObject)?.onConflict).toMatchObject({
      target: ["name"],
      action: {
        do: "nothing",
      },
    });
  });

  it("emits non-global signal metadata for authority backfill with idempotent insert semantics", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createMetaTask("Capture non-global signal added metadata", (ctx) => {
      payload = ctx;
      return true;
    }).doOn("global.meta.signal_controller.signal_added");

    Cadenza.emit("meta.signal_broker.added", {
      signalName: "runner.tick",
    });

    await waitForCondition(() => Boolean(payload?.queryData));

    expect((payload?.data as AnyObject)?.name).toBe("runner.tick");
    expect((payload?.queryData as AnyObject)?.data?.is_global).toBe(false);
    expect((payload?.queryData as AnyObject)?.data?.is_meta).toBe(false);
    expect((payload?.queryData as AnyObject)?.onConflict).toMatchObject({
      target: ["name"],
      action: {
        do: "nothing",
      },
    });
  });

  it("does not re-emit signal-added metadata after the signal is already registered locally", async () => {
    const payloads: AnyObject[] = [];

    Cadenza.createMetaTask("Capture deduped signal added metadata", (ctx) => {
      payloads.push(ctx);
      return true;
    }).doOn("global.meta.signal_controller.signal_added");

    Cadenza.signalBroker.addSignal("runner.tick");
    ((Cadenza.signalBroker as any).signalObservers.get("runner.tick") as AnyObject)
      .registered = true;
    await new Promise((resolve) => setTimeout(resolve, 25));
    payloads.length = 0;

    Cadenza.emit("meta.signal_broker.added", {
      signalName: "runner.tick",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(payloads).toEqual([]);
  });

  it("suppresses managed route-recovery task errors from generic task error logging", async () => {
    GraphMetadataController.instance;
    const consoleLogSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    Cadenza.emit("meta.node.errored", {
      taskName: "Insert telemetry (Proxy)",
      taskVersion: 1,
      nodeId: "node-1",
      errorMessage:
        "Node error: Error: No routeable internal transport available for IotDbService. Waiting for authority route updates before retrying.",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(consoleLogSpy).not.toHaveBeenCalledWith(
      expect.stringContaining("Error in task Insert telemetry (Proxy)"),
      expect.anything(),
    );
  });

  it("waits until initial sync completes before emitting direct signal registration metadata", async () => {
    resetRuntimeState();
    Cadenza.createCadenzaService("SignalMetadataService", "Signal metadata", {
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "signal-metadata-service-1",
    });

    const payloads: AnyObject[] = [];

    Cadenza.createMetaTask("Capture gated signal added metadata", (ctx) => {
      payloads.push(ctx);
      return true;
    }).doOn("global.meta.signal_controller.signal_added");

    Cadenza.emit("meta.signal_broker.added", {
      signalName: "runner.tick",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(payloads).toEqual([]);

    Cadenza.emit("meta.service_registry.registered_global_signals", {
      serviceName: "SignalMetadataService",
    });

    Cadenza.emit("meta.service_registry.registered_global_intents", {
      serviceName: "SignalMetadataService",
    });

    await waitForCondition(() => Cadenza.hasCompletedBootstrapSync());

    Cadenza.emit("meta.signal_broker.added", {
      signalName: "runner.tick_scheduled",
    });

    await waitForCondition(() =>
      payloads.some(
        (payload) =>
          (payload.data as AnyObject)?.name === "runner.tick_scheduled",
      ),
    );
  });

  it("emits the local service ready signal on a fresh service-level trace after bootstrap", async () => {
    resetRuntimeState();
    Cadenza.createCadenzaService("SignalMetadataService", "Signal metadata", {
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "signal-metadata-service-1",
    });

    const payloads: AnyObject[] = [];
    const readySignalName = Cadenza.getServiceReadySignalName(
      "SignalMetadataService",
    );

    Cadenza.createMetaTask("Capture service ready signal emission metadata", (ctx) => {
      if (ctx?.__signalEmission?.signalName === readySignalName) {
        payloads.push(ctx);
      }
      return true;
    }).doOn("sub_meta.signal_broker.emitting_signal");

    Cadenza.emit("meta.service_registry.registered_global_signals", {
      serviceName: "SignalMetadataService",
    });

    Cadenza.emit("meta.service_registry.registered_global_intents", {
      serviceName: "SignalMetadataService",
    });

    await waitForCondition(() => payloads.length > 0);

    expect(payloads[0]?.ready).toBe(true);
    expect(payloads[0]?.__traceCreatedBySignalBroker).toBe(true);
    expect(payloads[0]?.__signalEmission?.signalName).toBe(readySignalName);
    expect(typeof payloads[0]?.__signalEmission?.executionTraceId).toBe("string");
  });

  it("tracks emitted-only signals so they can be registered without waiting for an observer", async () => {
    SignalController.instance;

    Cadenza.emit("sub_meta.signal_broker.emitting_signal", {
      __signalEmission: {
        signalName: "runner.tick",
        executionTraceId: "trace-1",
      },
    });

    await waitForCondition(() =>
      Boolean((Cadenza.signalBroker as any).signalObservers?.has("runner.tick")),
    );

    expect((Cadenza.signalBroker as any).signalObservers?.get("runner.tick")).toBeTruthy();
  });

  it("requests one direct registration for an existing unregistered emitted signal", async () => {
    const payloads: AnyObject[] = [];

    Cadenza.createMetaTask("Capture late emitted signal registration", (ctx) => {
      payloads.push(ctx);
      return true;
    }).doOn("global.meta.signal_controller.signal_added");

    Cadenza.signalBroker.addSignal("runner.tick");
    const runnerTickObserver = (Cadenza.signalBroker as any).signalObservers.get(
      "runner.tick",
    ) as AnyObject;

    await new Promise((resolve) => setTimeout(resolve, 10));
    payloads.length = 0;
    runnerTickObserver.registrationRequested = false;
    runnerTickObserver.registered = false;

    Cadenza.emit("sub_meta.signal_broker.emitting_signal", {
      __signalEmission: {
        signalName: "runner.tick",
        executionTraceId: "trace-2",
      },
    });

    await waitForCondition(() =>
      payloads.some((payload) => (payload.data as AnyObject)?.name === "runner.tick"),
    );

    const runnerTickPayload = payloads.find(
      (payload) => (payload.data as AnyObject)?.name === "runner.tick",
    );

    expect((runnerTickPayload?.data as AnyObject)?.name).toBe("runner.tick");
    expect(
      (Cadenza.signalBroker as any).signalObservers?.get("runner.tick")
        ?.registrationRequested,
    ).toBe(true);
  });

  it("skips execution_trace ensure when the signal emission already belongs to an existing trace", async () => {
    const bundles: AnyObject[] = [];

    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.serviceRegistry.serviceName = "CadenzaDB";
    Cadenza.serviceRegistry.serviceInstanceId =
      "signal-metadata-cadenza-db-service-1";

    Cadenza.createMetaTask("Capture existing-trace signal persistence bundle", (ctx) => {
      bundles.push(ctx);
      return true;
    }).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    SignalController.instance;

    Cadenza.emit("sub_meta.signal_broker.emitting_signal", {
      foo: "bar",
      __signalEmission: {
        uuid: "signal-emission-2",
        signalName: "execution.persistence.existing_trace_test",
        signalTag: null,
        executionTraceId: "trace-existing",
        emittedAt: "2026-03-24T00:00:00.000Z",
        isMeta: false,
      },
    });

    await waitForCondition(() => bundles.length === 1);

    const ensures = Array.isArray(bundles[0].ensures) ? bundles[0].ensures : [];
    expect(ensures.map((event: AnyObject) => event.entityType)).toEqual([
      "signal_emission",
    ]);
  });

  it("skips execution observability persistence for meta signal emissions", async () => {
    const bundles: AnyObject[] = [];

    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.serviceRegistry.serviceName = "CadenzaDB";
    Cadenza.serviceRegistry.serviceInstanceId =
      "signal-metadata-cadenza-db-service-1";

    Cadenza.createMetaTask("Capture meta signal persistence bundle", (ctx) => {
      bundles.push(ctx);
      return true;
    }).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    SignalController.instance;

    Cadenza.emit("sub_meta.signal_broker.emitting_signal", {
      __traceCreatedBySignalBroker: true,
      __signalEmission: {
        uuid: "signal-emission-meta-1",
        signalName: "meta.fetch.handshake_requested",
        signalTag: "transport-1",
        executionTraceId: "trace-meta-1",
        emittedAt: "2026-03-24T00:00:00.000Z",
        isMeta: true,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(bundles).toEqual([]);
  });

  it("does not persist signal_emission again on a receiving service instance", async () => {
    const bundles: AnyObject[] = [];

    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.serviceRegistry.serviceName = "CadenzaDB";
    Cadenza.serviceRegistry.serviceInstanceId =
      "signal-metadata-cadenza-db-service-1";

    Cadenza.createMetaTask("Capture foreign signal persistence bundle", (ctx) => {
      bundles.push(ctx);
      return true;
    }).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    SignalController.instance;

    Cadenza.emit("sub_meta.signal_broker.emitting_signal", {
      __signalEmission: {
        uuid: "signal-emission-foreign-1",
        signalName: "orders.created",
        signalTag: null,
        executionTraceId: "trace-foreign-1",
        emittedAt: "2026-03-25T00:00:00.000Z",
        serviceName: "OrdersService",
        serviceInstanceId: "orders-service-instance-1",
        taskExecutionId: "source-task-exec-1",
        isMeta: false,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(bundles).toEqual([]);
  });

  it("omits local service_instance_id from execution observability before bootstrap sync completes", async () => {
    const bundles: AnyObject[] = [];

    resetRuntimeState();
    Cadenza.createCadenzaService("SignalMetadataService", "Signal metadata", {
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "signal-metadata-service-1",
    });

    Cadenza.createMetaTask(
      "Capture pre-bootstrap signal persistence bundle",
      (ctx) => {
        bundles.push(ctx);
        return true;
      },
    ).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    SignalController.instance;

    Cadenza.emit("sub_meta.signal_broker.emitting_signal", {
      __traceCreatedBySignalBroker: true,
      __signalEmission: {
        uuid: "signal-emission-pre-bootstrap-1",
        signalName: "orders.created",
        signalTag: null,
        executionTraceId: "trace-pre-bootstrap-1",
        emittedAt: "2026-03-27T00:00:00.000Z",
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
    const signalEvent = ensures.find(
      (event) => event?.entityType === "signal_emission",
    );

    expect(executionTraceEvent?.data?.service_instance_id).toBeUndefined();
    expect(signalEvent?.data?.service_instance_id).toBeNull();
  });

  it("sanitizes signal-created execution trace contexts before emitting persistence bundles", async () => {
    const bundles: AnyObject[] = [];

    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.serviceRegistry.serviceName = "CadenzaDB";
    Cadenza.serviceRegistry.serviceInstanceId =
      "signal-metadata-cadenza-db-service-1";

    Cadenza.createMetaTask(
      "Capture sanitized signal trace persistence bundle",
      (ctx) => {
        bundles.push(ctx);
        return true;
      },
    ).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    SignalController.instance;

    Cadenza.emit("sub_meta.signal_broker.emitting_signal", {
      orderId: "order-11",
      rows: [{ id: 1 }, { id: 2 }],
      joinedContexts: [{ foo: "bar" }],
      queryData: {
        data: { orderId: "order-11", amount: 42 },
        filter: { status: "open" },
      },
      __traceCreatedBySignalBroker: true,
      __intent: "iot-anomaly-detect",
      __signalEmission: {
        uuid: "signal-emission-sanitized-1",
        signalName: "orders.created",
        signalTag: null,
        executionTraceId: "trace-sanitized-1",
        emittedAt: "2026-03-27T00:00:00.000Z",
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

    expect(executionTraceEvent?.data?.context?.context).toMatchObject({
      orderId: "order-11",
      rowsCount: 2,
      joinedContextsCount: 1,
      queryData: {
        dataKeys: ["amount", "orderId"],
        filterKeys: ["status"],
      },
    });
    expect(executionTraceEvent?.data?.context?.context?.rows).toBeUndefined();
    expect(
      executionTraceEvent?.data?.context?.context?.joinedContexts,
    ).toBeUndefined();
  });

  it("references the originating business routine from signal_emission without re-persisting a sub-meta routine", async () => {
    const bundles: AnyObject[] = [];

    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.serviceRegistry.serviceName = "CadenzaDB";
    Cadenza.serviceRegistry.serviceInstanceId =
      "signal-metadata-cadenza-db-service-1";

    Cadenza.createMetaTask("Capture routine-bound signal persistence bundle", (ctx) => {
      bundles.push(ctx);
      return true;
    }).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    SignalController.instance;

    Cadenza.emit("sub_meta.signal_broker.emitting_signal", {
      orderId: "order-7",
      __routineCreatedByRunner: true,
      __routineName: "Persist orders",
      __routineVersion: 3,
      __routineCreatedAt: "2026-03-24T00:00:00.000Z",
      __routineIsMeta: false,
      __signalEmission: {
        uuid: "signal-emission-routine-1",
        signalName: "orders.persisted",
        signalTag: null,
        taskName: "Persist order record",
        taskVersion: 1,
        taskExecutionId: "task-exec-1",
        routineExecutionId: "routine-exec-1",
        executionTraceId: "trace-existing",
        emittedAt: "2026-03-24T00:00:01.000Z",
        isMeta: false,
      },
    });

    await waitForCondition(() => bundles.length === 1);

    const ensures = Array.isArray(bundles[0].ensures) ? bundles[0].ensures : [];
    const signalEvent = ensures.find(
      (event: AnyObject) => event.entityType === "signal_emission",
    );

    expect(ensures.map((event: AnyObject) => event.entityType)).toEqual([
      "signal_emission",
    ]);
    expect(signalEvent?.data).toMatchObject({
      uuid: "signal-emission-routine-1",
      task_name: "Persist order record",
      routine_execution_id: "routine-exec-1",
      execution_trace_id: "trace-existing",
    });
    expect(signalEvent?.deps).toContain("routine_execution:routine-exec-1");
  });

  it("does not persist meta routine_execution rows while preparing business signal persistence", async () => {
    const bundles: AnyObject[] = [];

    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.serviceRegistry.serviceName = "CadenzaDB";
    Cadenza.serviceRegistry.serviceInstanceId =
      "signal-metadata-cadenza-db-service-1";

    Cadenza.createMetaTask(
      "Capture meta routine omitted from signal persistence bundle",
      (ctx) => {
        bundles.push(ctx);
        return true;
      },
    ).doOn(EXECUTION_PERSISTENCE_BUNDLE_SIGNAL);

    SignalController.instance;

    Cadenza.emit("sub_meta.signal_broker.emitting_signal", {
      __isSubMeta: true,
      orderId: "order-9",
      __routineCreatedByRunner: true,
      __routineName: "Prepare signal emission persistence",
      __routineVersion: 1,
      __routineCreatedAt: "2026-04-18T00:00:00.000Z",
      __routineIsMeta: true,
      __signalEmission: {
        uuid: "signal-emission-meta-routine-1",
        signalName: "orders.persisted",
        signalTag: null,
        taskName: "Persist order record",
        taskVersion: 1,
        taskExecutionId: "task-exec-9",
        routineExecutionId: "routine-exec-meta-1",
        executionTraceId: "trace-meta-routine-1",
        emittedAt: "2026-04-18T00:00:01.000Z",
        isMeta: false,
      },
    });

    await waitForCondition(() => bundles.length === 1);

    const ensures = Array.isArray(bundles[0]?.ensures)
      ? (bundles[0].ensures as AnyObject[])
      : [];

    expect(ensures.map((event) => event.entityType)).toEqual(["signal_emission"]);
    expect(
      ensures.some((event) => event.entityType === "routine_execution"),
    ).toBe(false);
  });
});
