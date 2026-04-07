import { beforeEach, describe, expect, it } from "vitest";
import type { AnyObject } from "@cadenza.io/core";
import Cadenza from "../src/Cadenza";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import GraphSyncController from "../src/graph/controllers/GraphSyncController";
import SocketController from "../src/network/SocketController";

function resetRuntimeState() {
  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run reset errors before bootstrap.
  }

  (GraphMetadataController as any)._instance = undefined;
  (GraphSyncController as any)._instance = undefined;
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

  it("skips direct metadata for bootstrap-local-only actors", async () => {
    let actorPayload: AnyObject | undefined;
    let actorTaskPayload: AnyObject | undefined;

    Cadenza.createMetaTask("Capture skipped actor metadata signal", (ctx) => {
      actorPayload = ctx;
      return true;
    }).doOn("global.meta.graph_metadata.actor_created");

    Cadenza.createMetaTask(
      "Capture skipped actor task association signal",
      (ctx) => {
        actorTaskPayload = ctx;
        return true;
      },
    ).doOn("global.meta.graph_metadata.actor_task_associated");

    Cadenza.emit("meta.actor.created", {
      data: {
        name: "ServiceLifecycleFlushActor",
        description: "runtime-only actor",
        default_key: "service-lifecycle",
        load_policy: "eager",
        write_contract: "overwrite",
        runtime_read_guard: "none",
        consistency_profile: null,
        key_definition: null,
        state_definition: {},
        retry_policy: {},
        idempotency_policy: {},
        session_policy: {},
        is_meta: true,
        version: 1,
      },
    });

    Cadenza.emit("meta.actor.task_associated", {
      data: {
        actor_name: "ServiceLifecycleFlushActor",
        actor_version: 1,
        task_name: "Flush local runtime status to authority",
        task_version: 1,
        mode: "write",
        description: "runtime-only association",
        is_meta: true,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(actorPayload).toBeUndefined();
    expect(actorTaskPayload).toBeUndefined();
  });

  it("waits for both tasks to register before emitting task relationship metadata", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createMetaTask("Capture task relationship signal", (ctx) => {
      payload = ctx;
      return true;
    }).doOn("global.meta.graph_metadata.task_relationship_created");

    const predecessor = Cadenza.createTask("Registered predecessor", () => true);
    const successor = Cadenza.createTask("Pending successor", () => true);
    predecessor.registered = true;
    successor.registered = false;

    Cadenza.emit("meta.task.relationship_added", {
      data: {
        predecessorTaskName: "Registered predecessor",
        taskName: "Pending successor",
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(payload).toBeUndefined();

    successor.registered = true;

    Cadenza.emit("meta.task.relationship_added", {
      data: {
        predecessorTaskName: "Registered predecessor",
        taskName: "Pending successor",
      },
    });

    await waitForCondition(() => Boolean(payload?.data));

    const data = payload?.data as AnyObject;
    const queryData = payload?.queryData as AnyObject;
    expect(readField(data, "predecessor_task_name")).toBe("Registered predecessor");
    expect(readField(data, "task_name")).toBe("Pending successor");
    expect(readField(data, "service_name")).toBe("ActorMetadataService");
    expect(readField(data, "predecessor_service_name")).toBe("ActorMetadataService");
    expect(readField(queryData?.data as AnyObject, "predecessor_task_name")).toBe(
      "Registered predecessor",
    );
    expect(readField(queryData?.data as AnyObject, "task_name")).toBe(
      "Pending successor",
    );
  });

  it("emits relationship execution metadata with database-ready queryData", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createTask("Relationship predecessor", () => true);
    Cadenza.createTask("Relationship successor", () => true);

    Cadenza.createMetaTask("Capture relationship execution signal", (ctx) => {
      payload = ctx;
      return true;
    }).doOn("global.meta.graph_metadata.relationship_executed");

    Cadenza.emit("meta.node.mapped", {
      filter: {
        taskName: "Relationship successor",
        predecessorTaskName: "Relationship predecessor",
      },
    });

    await waitForCondition(() => Boolean(payload?.queryData));

    const data = payload?.data as AnyObject;
    const filter = payload?.filter as AnyObject;
    const queryData = payload?.queryData as AnyObject;
    expect(readField(data, "execution_count")).toBe("increment");
    expect(readField(filter, "task_name")).toBe("Relationship successor");
    expect(readField(filter, "predecessor_task_name")).toBe(
      "Relationship predecessor",
    );
    expect(readField(filter, "service_name")).toBe("ActorMetadataService");
    expect(readField(queryData?.data as AnyObject, "execution_count")).toBe(
      "increment",
    );
    expect(readField(queryData?.filter as AnyObject, "task_name")).toBe(
      "Relationship successor",
    );
  });

  it("emits task-created metadata with idempotent insert semantics", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createMetaTask("Capture task created signal", (ctx) => {
      payload = ctx;
      return true;
    }).doOn("global.meta.graph_metadata.task_created");

    const task = Cadenza.createTask("Dynamic metadata task", () => true);
    payload = undefined;

    Cadenza.emit("meta.task.created", {
      data: {
        name: task.name,
        version: task.version,
        description: task.description,
      },
    });

    await waitForCondition(() => Boolean(payload?.queryData));

    expect(readField(payload?.data as AnyObject, "name")).toBe(
      "Dynamic metadata task",
    );
    expect((payload?.queryData as AnyObject)?.onConflict).toMatchObject({
      target: ["name", "service_name", "version"],
      action: {
        do: "nothing",
      },
    });
    expect((task as AnyObject).registrationRequested).toBe(true);
  });

  it("does not re-emit task-created metadata after the task is already registered locally", async () => {
    const payloads: AnyObject[] = [];

    Cadenza.createMetaTask("Capture deduped task created signal", (ctx) => {
      payloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.task_created");

    const task = Cadenza.createTask("Registered metadata task", () => true);
    payloads.length = 0;
    task.registered = true;

    Cadenza.emit("meta.task.created", {
      data: {
        name: task.name,
        version: task.version,
        description: task.description,
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(
      payloads.filter(
        (payload) =>
          readField(payload.data as AnyObject, "name") === "Registered metadata task",
      ),
    ).toEqual([]);
  });

  it("emits direct intent metadata only for locally handled intents", async () => {
    const payloads: AnyObject[] = [];

    Cadenza.createMetaTask("Capture direct intent metadata", (ctx) => {
      payloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.intent_created");

    Cadenza.inquiryBroker.addIntent({
      name: "remote-report-intent",
      description: "Remote-only intent",
      input: { type: "object" },
      output: { type: "object" },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(
      payloads.filter(
        (payload) =>
          readField(payload.data as AnyObject, "name") === "remote-report-intent",
      ),
    ).toEqual([]);

    payloads.length = 0;

    Cadenza.createTask("Local orders report responder", () => true).respondsTo(
      "orders-report",
    );

    await waitForCondition(() =>
      payloads.some(
        (payload) =>
          readField(payload.data as AnyObject, "name") === "orders-report",
      ),
    );

    const localIntent = Cadenza.inquiryBroker.intents.get(
      "orders-report",
    ) as AnyObject | undefined;

    expect(localIntent?.registrationRequested).toBe(true);
    expect(
      payloads.some(
        (payload) =>
          readField(payload.data as AnyObject, "name") === "orders-report",
      ),
    ).toBe(true);
  });

  it("does not re-emit direct intent metadata after the intent is already registered locally", async () => {
    const payloads: AnyObject[] = [];

    Cadenza.createMetaTask("Capture deduped intent metadata", (ctx) => {
      payloads.push(ctx);
      return true;
    }).doOn("global.meta.graph_metadata.intent_created");

    Cadenza.createTask("Registered orders responder", () => true).respondsTo(
      "orders-registered",
    );

    await waitForCondition(() =>
      payloads.some(
        (payload) =>
          readField(payload.data as AnyObject, "name") === "orders-registered",
      ),
    );

    payloads.length = 0;

    GraphSyncController.instance.registeredIntentDefinitions.add(
      "orders-registered",
    );
    const registeredIntent = Cadenza.inquiryBroker.intents.get(
      "orders-registered",
    ) as AnyObject | undefined;
    if (registeredIntent) {
      registeredIntent.registered = true;
      registeredIntent.registrationRequested = false;
    }

    Cadenza.emit("meta.inquiry_broker.added", {
      data: {
        name: "orders-registered",
        description: "",
        input: { type: "object" },
        output: { type: "object" },
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    expect(
      payloads.filter(
        (payload) =>
          readField(payload.data as AnyObject, "name") === "orders-registered",
      ),
    ).toEqual([]);
  });

  it("emits task-intent association metadata with intent_to_task_map field contract", async () => {
    let payload: AnyObject | undefined;

    Cadenza.createMetaTask("Capture task intent association signal", (ctx) => {
      if ((ctx.data as AnyObject | undefined)?.intentName === "orders-contract-sync") {
        payload = ctx;
      }
      return true;
    }).doOn("global.meta.graph_metadata.task_intent_associated");

    Cadenza.createTask("Task intent association contract", () => true);

    Cadenza.emit("meta.task.intent_associated", {
      data: {
        intentName: "orders-contract-sync",
        taskName: "Task intent association contract",
        taskVersion: 1,
      },
    });

    await waitForCondition(() => Boolean(payload?.data));

    const data = payload?.data as AnyObject;
    const queryData = payload?.queryData as AnyObject;
    expect(readField(data, "intent_name")).toBe("orders-contract-sync");
    expect(readField(data, "task_name")).toBe("Task intent association contract");
    expect(readField(data, "task_version")).toBe(1);
    expect(readField(data, "service_name")).toBe("ActorMetadataService");
    expect(readField(queryData?.data as AnyObject, "intent_name")).toBe(
      "orders-contract-sync",
    );
    expect(readField(queryData?.data as AnyObject, "task_name")).toBe(
      "Task intent association contract",
    );
  });
});
