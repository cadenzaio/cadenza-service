import { beforeEach, describe, expect, it } from "vitest";
import type { AnyObject } from "@cadenza.io/core";
import Cadenza from "../src/Cadenza";
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
  timeoutMs = 1000,
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

    Cadenza.emit("meta.signal_broker.added", {
      signalName: "runner.tick",
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(payloads).toEqual([]);
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

    Cadenza.emit("meta.service_registry.initial_sync_complete", {
      serviceName: "SignalMetadataService",
    });

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
});
