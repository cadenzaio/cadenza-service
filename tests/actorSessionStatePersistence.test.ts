import { META_ACTOR_SESSION_STATE_PERSIST_INTENT } from "@cadenza.io/core";
import { beforeEach, describe, expect, it } from "vitest";
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
});
