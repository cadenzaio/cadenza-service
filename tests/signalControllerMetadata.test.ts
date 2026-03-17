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
    expect((payload?.queryData as AnyObject)?.data?.isGlobal).toBe(true);
  });
});
