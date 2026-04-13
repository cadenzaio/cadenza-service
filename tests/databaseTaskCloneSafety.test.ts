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

describe("database/deputy task clone safety", () => {
  beforeEach(() => {
    resetRuntimeState();
    Cadenza.createCadenzaService("CloneSafetyService", "Clone safety test", {
      port: 0,
      useSocket: false,
      cadenzaDB: {
        connect: false,
      },
      customServiceId: "clone-safety-service-1",
    });
  });

  afterEach(() => {
    resetRuntimeState();
  });

  it("rejects cloning deputy tasks", () => {
    const deputyTask = Cadenza.createDeputyTask(
      "Lookup remote orders",
      "Lookup Orders",
      "OrdersService",
      {
        register: false,
      },
    );

    expect(() => deputyTask.clone()).toThrow(/does not support clone/i);
    expect(() => deputyTask.clone()).toThrow(/inquiry/i);
  });

  it("rejects cloning database tasks", () => {
    const queryTask = Cadenza.createCadenzaDBQueryTask(
      "service_instance",
      {},
      {
        register: false,
      },
    );

    expect(() => queryTask.clone()).toThrow(/does not support clone/i);
    expect(() => queryTask.clone()).toThrow(/default database inquiry/i);
  });

  it("reuses the same executor function across deputy and database task instances", () => {
    const deputyTaskA = Cadenza.createDeputyTask(
      "Lookup remote orders A",
      "Lookup Orders",
      "OrdersService",
      {
        register: false,
      },
    );
    const deputyTaskB = Cadenza.createDeputyTask(
      "Lookup remote orders B",
      "Lookup Orders",
      "OrdersService",
      {
        register: false,
      },
    );
    const queryTaskA = Cadenza.createCadenzaDBQueryTask(
      "service_instance",
      {},
      {
        register: false,
      },
    );
    const queryTaskB = Cadenza.createCadenzaDBQueryTask(
      "service_manifest",
      {},
      {
        register: false,
      },
    );

    expect(deputyTaskA.taskFunction).toBe(deputyTaskB.taskFunction);
    expect(queryTaskA.taskFunction).toBe(queryTaskB.taskFunction);
    expect(deputyTaskA.taskFunction).toBe(queryTaskA.taskFunction);
  });
});
