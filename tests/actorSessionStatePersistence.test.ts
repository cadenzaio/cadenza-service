import { META_ACTOR_SESSION_STATE_PERSIST_INTENT } from "@cadenza.io/core";
import { beforeEach, describe, expect, it } from "vitest";
import Cadenza from "../src/Cadenza";
import GraphMetadataController from "../src/graph/controllers/GraphMetadataController";
import SocketController from "../src/network/SocketController";

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

function resetRuntimeState() {
  try {
    Cadenza.reset();
  } catch {
    // Ignore first-run reset errors before bootstrap.
  }

  (GraphMetadataController as any)._instance = undefined;
  (SocketController as any)._instance = undefined;
}

describe("Actor session state persistence responder", () => {
  beforeEach(() => {
    resetRuntimeState();
    Cadenza.bootstrap();
    Cadenza.serviceRegistry.serviceName = "ActorSessionPersistenceService";
  });

  it("builds actor_session_state payload with service_name and stale-write guard", async () => {
    let capturedInsertContext: any;

    const successInsertStub = (ctx: any) => {
      capturedInsertContext = ctx;
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
      };
    };

    Cadenza.createMetaTask("dbInsertActorSessionState", successInsertStub);
    Cadenza.createMetaTask(
      "Insert actor_session_state in CadenzaDB",
      successInsertStub,
    );

    GraphMetadataController.instance;

    const response = await Cadenza.inquire(
      META_ACTOR_SESSION_STATE_PERSIST_INTENT,
      {
        actor_name: "OrderSessionActor",
        actor_version: 1,
        actor_key: "tenant:1:user:2",
        durable_state: {
          cart_total: 10,
        },
        durable_version: 4,
        expires_at: null,
      },
      { requireComplete: true },
    );

    expect(response).toMatchObject({
      __success: true,
      persisted: true,
      actor_name: "OrderSessionActor",
      actor_version: 1,
      actor_key: "tenant:1:user:2",
      service_name: "ActorSessionPersistenceService",
      durable_version: 4,
    });

    expect(capturedInsertContext?.queryData?.data).toMatchObject({
      actor_name: "OrderSessionActor",
      actor_version: 1,
      actor_key: "tenant:1:user:2",
      service_name: "ActorSessionPersistenceService",
      durable_state: {
        cart_total: 10,
      },
      durable_version: 4,
      expires_at: null,
    });

    expect(capturedInsertContext?.data).toMatchObject({
      actor_name: "OrderSessionActor",
      actor_version: 1,
      actor_key: "tenant:1:user:2",
      service_name: "ActorSessionPersistenceService",
      durable_state: {
        cart_total: 10,
      },
      durable_version: 4,
      expires_at: null,
    });

    expect(capturedInsertContext?.queryData?.onConflict).toEqual({
      target: ["actor_name", "actor_version", "actor_key", "service_name"],
      action: {
        do: "update",
        set: {
          durable_state: "excluded",
          durable_version: "excluded",
          expires_at: "excluded",
          updated: "excluded",
        },
        where:
          "actor_session_state.durable_version <= excluded.durable_version",
      },
    });
  });

  it("fails strict persistence contract when no rows are affected", async () => {
    const staleInsertStub = (ctx: any) => {
      return {
        ...ctx,
        __success: true,
        rowCount: 0,
      };
    };

    Cadenza.createMetaTask("dbInsertActorSessionState", staleInsertStub);
    Cadenza.createMetaTask(
      "Insert actor_session_state in CadenzaDB",
      staleInsertStub,
    );

    GraphMetadataController.instance;

    await expect(
      Cadenza.inquire(
        META_ACTOR_SESSION_STATE_PERSIST_INTENT,
        {
          actor_name: "OrderSessionActor",
          actor_version: 1,
          actor_key: "tenant:1:user:2",
          durable_state: { cart_total: 10 },
          durable_version: 2,
          expires_at: null,
        },
        { requireComplete: true },
      ),
    ).rejects.toMatchObject({
      errored: true,
    });
  });

  it("accepts successful remote actor_session_state inserts that omit rowCount", async () => {
    const remoteInsertStub = (ctx: any) => {
      return {
        ...ctx,
        __success: true,
        __status: "success",
        __serviceName: "CadenzaDB",
        __localTaskName: "Insert actor_session_state in CadenzaDB",
      };
    };

    Cadenza.createMetaTask("dbInsertActorSessionState", remoteInsertStub);
    Cadenza.createMetaTask(
      "Insert actor_session_state in CadenzaDB",
      remoteInsertStub,
    );

    GraphMetadataController.instance;

    const response = await Cadenza.inquire(
      META_ACTOR_SESSION_STATE_PERSIST_INTENT,
      {
        actor_name: "OrderSessionActor",
        actor_version: 1,
        actor_key: "tenant:1:user:2",
        durable_state: { cart_total: 10 },
        durable_version: 2,
        expires_at: null,
      },
      { requireComplete: true },
    );

    expect(response).toMatchObject({
      __success: true,
      persisted: true,
      actor_name: "OrderSessionActor",
      actor_key: "tenant:1:user:2",
      durable_version: 2,
      rowCount: 1,
    });
  });

  it("uses reduced concurrency for actor session persistence tasks", () => {
    GraphMetadataController.instance;

    expect(Cadenza.get("Persist actor session state")?.concurrency).toBe(20);
    expect(
      Cadenza.get("Validate actor session state persistence")?.concurrency,
    ).toBe(20);
  });

  it("persists repeated detached actor writes for the same key with increasing durable versions", async () => {
    const persistenceVersions: number[] = [];

    const successInsertStub = (ctx: any) => {
      persistenceVersions.push(Number(ctx.durable_version));
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
      };
    };

    Cadenza.createMetaTask("dbInsertActorSessionState", successInsertStub);
    Cadenza.createMetaTask(
      "Insert actor_session_state in CadenzaDB",
      successInsertStub,
    );

    GraphMetadataController.instance;

    const sessionActor = Cadenza.createActor<{
      count: number;
      lastPhase: string | null;
    }>({
      name: "DetachedPersistenceActor",
      defaultKey: "device:unknown",
      keyResolver: (input: any) =>
        typeof input?.deviceId === "string" ? input.deviceId : undefined,
      initState: {
        count: 0,
        lastPhase: null,
      },
      session: {
        persistDurableState: true,
        persistenceTimeoutMs: 30_000,
      },
    });

    Cadenza.createTask(
      "Record detached ingest session state",
      sessionActor.task(
        ({ input, state, setState }) => {
          setState({
            count: state.count + 1,
            lastPhase: String(input.phase ?? "ingest"),
          });
          return input;
        },
        { mode: "write" },
      ),
    ).doOn("test.actor.persist.ingest");

    Cadenza.createTask(
      "Record detached analysis session state",
      sessionActor.task(
        ({ input, state, setState }) => {
          setState({
            count: state.count + 1,
            lastPhase: String(input.phase ?? "analysis"),
          });
          return input;
        },
        { mode: "write" },
      ),
    ).doOn("test.actor.persist.analysis");

    Cadenza.emit("test.actor.persist.ingest", {
      deviceId: "device-1",
      phase: "ingest",
    });
    Cadenza.emit("test.actor.persist.analysis", {
      deviceId: "device-1",
      phase: "analysis",
    });

    await waitForCondition(
      () =>
        sessionActor.getDurableVersion("device-1") === 2 &&
        persistenceVersions.length === 2,
      2_000,
    );

    expect(sessionActor.getState("device-1")).toMatchObject({
      count: 2,
      lastPhase: "analysis",
    });
    expect(persistenceVersions).toEqual([1, 2]);
  });
});
