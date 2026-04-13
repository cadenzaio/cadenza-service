import { afterEach, beforeEach, describe, expect, it } from "vitest";

import Cadenza from "../src/Cadenza";
import DatabaseController from "../src/database/DatabaseController";
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

describe("graph sync clone safety", () => {
  beforeEach(() => {
    resetRuntimeState();
    Cadenza.createCadenzaService(
      "GraphSyncCloneSafetyService",
      "Graph sync clone safety test",
      {
        port: 0,
        useSocket: false,
        cadenzaDB: {
          connect: false,
        },
        customServiceId: "graph-sync-clone-safety-service-1",
      },
    );
    GraphSyncController.instance;
  });

  afterEach(() => {
    resetRuntimeState();
  });

  it("does not register cloned runtime helper tasks for bootstrap sync", () => {
    const registryTaskNames = new Set(Array.from(Cadenza.registry.tasks.keys()));

    expect(registryTaskNames.has("Get all tasks (clone)")).toBe(false);
    expect(registryTaskNames.has("Get signals (clone)")).toBe(false);
    expect(registryTaskNames.has("Get all routines (clone)")).toBe(false);
    expect(
      Array.from(registryTaskNames).some((taskName) =>
        /^(Get all tasks|Get signals|Get all routines|Do for each task) \(clone /.test(
          taskName,
        ),
      ),
    ).toBe(false);

    expect(Cadenza.get("Get all tasks for sync")).toBeTruthy();
    expect(Cadenza.get("Get signals for sync")).toBeTruthy();
    expect(Cadenza.get("Get all routines for sync")).toBeTruthy();
    expect(Cadenza.get("Iterate tasks for directional task map sync")).toBeTruthy();
    expect(Cadenza.get("Iterate tasks for signal task map sync")).toBeTruthy();
    expect(Cadenza.get("Iterate tasks for intent task map sync")).toBeTruthy();
    expect(Cadenza.get("Iterate tasks for actor task map sync")).toBeTruthy();
    expect(Cadenza.get("Get all routines for task map sync")).toBeTruthy();
  });
});
