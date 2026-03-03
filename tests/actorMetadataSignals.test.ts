import { beforeEach, describe, expect, it } from "vitest";
import type { AnyObject } from "@cadenza.io/core";
import Cadenza from "../src/Cadenza";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import SocketController from "../src/network/SocketController";

function resetRuntimeState() {
  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run reset errors before bootstrap.
  }

  (GraphMetadataController as any)._instance = undefined;
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

describe("Actor metadata signal contracts", () => {
  beforeEach(() => {
    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.serviceRegistry.serviceName = "ActorMetadataService";
    GraphMetadataController.instance;
  });

  it("emits actor-created metadata with actor table field contract", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createMetaTask("Capture actor metadata created signal", (ctx) => {
      payload = ctx;
      return true;
    }).doOn("global.meta.graph_metadata.actor_created");

    Cadenza.emit("meta.actor.created", {
      data: {
        name: "ActorMetadataContract",
        description: "Actor metadata contract test",
        default_key: "metadata-default",
        load_policy: "eager",
        write_contract: "overwrite",
        runtime_read_guard: "none",
        consistency_profile: null,
        key_definition: null,
        state_definition: { durable: { initState: { count: 0 } } },
        retry_policy: {},
        idempotency_policy: {},
        session_policy: {},
        is_meta: false,
        version: 1,
      },
    });

    await waitForCondition(() => Boolean(payload?.data));

    const data = payload?.data as AnyObject;
    expect(readField(data, "name")).toBe("ActorMetadataContract");
    expect(readField(data, "description")).toBe("Actor metadata contract test");
    expect(readField(data, "default_key")).toBe("metadata-default");
    expect(readField(data, "load_policy")).toBe("eager");
    expect(readField(data, "write_contract")).toBe("overwrite");
    expect(readField(data, "runtime_read_guard")).toBe("none");
    expect(readField(data, "state_definition")).toBeTypeOf("object");
    expect(readField(data, "retry_policy")).toBeTypeOf("object");
    expect(readField(data, "idempotency_policy")).toBeTypeOf("object");
    expect(readField(data, "session_policy")).toBeTypeOf("object");
    expect(readField(data, "is_meta")).toBe(false);
    expect(readField(data, "version")).toBe(1);
    expect(readField(data, "service_name")).toBe("ActorMetadataService");
  });

  it("emits actor-task association metadata with actor_task_map field contract", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createMetaTask("Capture actor task association signal", (ctx) => {
      payload = ctx;
      return true;
    }).doOn("global.meta.graph_metadata.actor_task_associated");

    Cadenza.emit("meta.actor.task_associated", {
      data: {
        actor_name: "ActorTaskMapContract",
        actor_version: 1,
        task_name: "ActorTaskMapContract.Read",
        task_version: 1,
        mode: "read",
        description: "Actor task map contract test",
        is_meta: false,
      },
    });

    await waitForCondition(() => Boolean(payload?.data));

    const data = payload?.data as AnyObject;
    expect(readField(data, "actor_name")).toBe("ActorTaskMapContract");
    expect(readField(data, "actor_version")).toBe(1);
    expect(readField(data, "task_name")).toBe("ActorTaskMapContract.Read");
    expect(readField(data, "task_version")).toBe(1);
    expect(readField(data, "service_name")).toBe("ActorMetadataService");
    expect(readField(data, "mode")).toBe("read");
    expect(readField(data, "description")).toBe("Actor task map contract test");
    expect(readField(data, "is_meta")).toBe(false);
  });
});
