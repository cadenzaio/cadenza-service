import { afterEach, beforeEach, describe, expect, it } from "vitest";

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

describe("execution persistence boundary", () => {
  beforeEach(() => {
    resetRuntimeState();
    Cadenza.createCadenzaService("BoundaryService", "Execution boundary service", {
      port: 0,
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "boundary-service-1",
    });
    GraphMetadataController.instance;
  });

  afterEach(() => {
    resetRuntimeState();
  });

  it("does not forward meta execution traces into graph metadata persistence", async () => {
    const payloads: Array<Record<string, any>> = [];

    Cadenza.createMetaTask("Capture trace metadata boundary", (ctx) => {
      payloads.push(ctx as Record<string, any>);
      return true;
    }).doOn("global.meta.graph_metadata.execution_trace_created");

    Cadenza.run(Cadenza.createMetaTask("Boundary meta task", () => true), {});

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(payloads).toHaveLength(0);
  });

  it("still forwards business execution traces into graph metadata persistence", async () => {
    const payloads: Array<Record<string, any>> = [];

    Cadenza.createMetaTask("Capture business trace metadata boundary", (ctx) => {
      payloads.push(ctx as Record<string, any>);
      return true;
    }).doOn("global.meta.graph_metadata.execution_trace_created");

    Cadenza.run(Cadenza.createTask("Boundary business task", () => true), {});

    await waitForCondition(() => payloads.length === 1);

    expect(payloads[0]?.data?.is_meta).toBe(false);
  });
});
