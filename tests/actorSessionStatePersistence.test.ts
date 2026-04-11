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

async function invokeActorTask(task: any, context: Record<string, any> = {}) {
  return task(
    context,
    () => undefined,
    (inquiry: string, inquiryContext: Record<string, any>, options = {}) =>
      Cadenza.inquire(inquiry, inquiryContext, options),
    () => undefined,
  );
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
    const queryStub = (ctx: any) => {
      return {
        ...ctx,
        __success: true,
        rowCount: 0,
        actorSessionState: null,
      };
    };

    const successInsertStub = (ctx: any) => {
      persistenceVersions.push(Number(ctx.durable_version));
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
      };
    };

    Cadenza.createMetaTask("dbQueryActorSessionState", queryStub);
    Cadenza.createMetaTask("Query actor_session_state in CadenzaDB", queryStub);
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

  it("hydrates persisted actor_session_state before the first write after restart", async () => {
    let hydrationQueryCount = 0;
    const persistenceVersions: number[] = [];

    const queryStub = (ctx: any) => {
      hydrationQueryCount += 1;
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
        actorSessionState: {
          actorName: "RestartSafeActor",
          actorVersion: 1,
          actorKey: "device-1",
          serviceName: "ActorSessionPersistenceService",
          durableState: { count: 4 },
          durableVersion: 4,
          expiresAt: null,
        },
      };
    };
    const insertStub = (ctx: any) => {
      persistenceVersions.push(Number(ctx.durable_version));
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
      };
    };

    Cadenza.createMetaTask("dbQueryActorSessionState", queryStub);
    Cadenza.createMetaTask("Query actor_session_state in CadenzaDB", queryStub);
    Cadenza.createMetaTask("dbInsertActorSessionState", insertStub);
    Cadenza.createMetaTask("Insert actor_session_state in CadenzaDB", insertStub);

    GraphMetadataController.instance;

    const sessionActor = Cadenza.createActor<{ count: number }>({
      name: "RestartSafeActor",
      defaultKey: "device:unknown",
      keyResolver: (input: any) =>
        typeof input?.deviceId === "string" ? input.deviceId : undefined,
      initState: {
        count: 0,
      },
      session: {
        persistDurableState: true,
        persistenceTimeoutMs: 30_000,
      },
    });

    const writeTask = sessionActor.task(
      ({ state, setState }) => {
        setState({
          count: state.count + 1,
        });
      },
      { mode: "write" },
    );

    await invokeActorTask(writeTask, { deviceId: "device-1" });
    await invokeActorTask(writeTask, { deviceId: "device-1" });

    expect(hydrationQueryCount).toBe(1);
    expect(sessionActor.getState("device-1")).toEqual({ count: 6 });
    expect(sessionActor.getDurableVersion("device-1")).toBe(6);
    expect(persistenceVersions).toEqual([5, 6]);
  });

  it("hydrates persisted actor_session_state rows returned inside joined contexts", async () => {
    let hydrationQueryCount = 0;
    const persistenceVersions: number[] = [];

    const queryStub = (ctx: any) => {
      hydrationQueryCount += 1;
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
        joinedContexts: [
          {
            actorSessionState: {
              actorName: "JoinedContextHydrationActor",
              actorVersion: 1,
              actorKey: "device-1",
              serviceName: "ActorSessionPersistenceService",
              durableState: { count: 7 },
              durableVersion: 7,
              expiresAt: null,
            },
          },
        ],
      };
    };
    const insertStub = (ctx: any) => {
      persistenceVersions.push(Number(ctx.durable_version));
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
      };
    };

    Cadenza.createMetaTask("dbQueryActorSessionState", queryStub);
    Cadenza.createMetaTask("Query actor_session_state in CadenzaDB", queryStub);
    Cadenza.createMetaTask("dbInsertActorSessionState", insertStub);
    Cadenza.createMetaTask("Insert actor_session_state in CadenzaDB", insertStub);

    GraphMetadataController.instance;

    const sessionActor = Cadenza.createActor<{ count: number }>({
      name: "JoinedContextHydrationActor",
      defaultKey: "device:unknown",
      keyResolver: (input: any) =>
        typeof input?.deviceId === "string" ? input.deviceId : undefined,
      initState: {
        count: 0,
      },
      session: {
        persistDurableState: true,
        persistenceTimeoutMs: 30_000,
      },
    });

    const writeTask = sessionActor.task(
      ({ state, setState }) => {
        setState({
          count: state.count + 1,
        });
      },
      { mode: "write" },
    );

    await invokeActorTask(writeTask, { deviceId: "device-1" });

    expect(hydrationQueryCount).toBe(1);
    expect(sessionActor.getState("device-1")).toEqual({ count: 8 });
    expect(sessionActor.getDurableVersion("device-1")).toBe(8);
    expect(persistenceVersions).toEqual([8]);
  });

  it("hydrates persisted actor_session_state rows returned as plural arrays", async () => {
    let hydrationQueryCount = 0;
    const persistenceVersions: number[] = [];

    const queryStub = (ctx: any) => {
      hydrationQueryCount += 1;
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
        actorSessionStates: [
          {
            actorName: "PluralHydrationActor",
            actorVersion: 1,
            actorKey: "device-1",
            serviceName: "ActorSessionPersistenceService",
            durableState: { count: 9 },
            durableVersion: 9,
            expiresAt: null,
          },
        ],
      };
    };
    const insertStub = (ctx: any) => {
      persistenceVersions.push(Number(ctx.durable_version));
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
      };
    };

    Cadenza.createMetaTask("dbQueryActorSessionState", queryStub);
    Cadenza.createMetaTask("Query actor_session_state in CadenzaDB", queryStub);
    Cadenza.createMetaTask("dbInsertActorSessionState", insertStub);
    Cadenza.createMetaTask("Insert actor_session_state in CadenzaDB", insertStub);

    GraphMetadataController.instance;

    const sessionActor = Cadenza.createActor<{ count: number }>({
      name: "PluralHydrationActor",
      defaultKey: "device:unknown",
      keyResolver: (input: any) =>
        typeof input?.deviceId === "string" ? input.deviceId : undefined,
      initState: {
        count: 0,
      },
      session: {
        persistDurableState: true,
        persistenceTimeoutMs: 30_000,
      },
    });

    const writeTask = sessionActor.task(
      ({ state, setState }) => {
        setState({
          count: state.count + 1,
        });
      },
      { mode: "write" },
    );

    await invokeActorTask(writeTask, { deviceId: "device-1" });

    expect(hydrationQueryCount).toBe(1);
    expect(sessionActor.getState("device-1")).toEqual({ count: 10 });
    expect(sessionActor.getDurableVersion("device-1")).toBe(10);
    expect(persistenceVersions).toEqual([10]);
  });

  it("hydrates persisted actor_session_state rows returned as raw rows arrays", async () => {
    let hydrationQueryCount = 0;
    const persistenceVersions: number[] = [];

    const queryStub = (ctx: any) => {
      hydrationQueryCount += 1;
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
        rows: [
          {
            actorName: "RowsHydrationActor",
            actorVersion: 1,
            actorKey: "device-1",
            serviceName: "ActorSessionPersistenceService",
            durableState: { count: 11 },
            durableVersion: 11,
            expiresAt: null,
          },
        ],
      };
    };
    const insertStub = (ctx: any) => {
      persistenceVersions.push(Number(ctx.durable_version));
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
      };
    };

    Cadenza.createMetaTask("dbQueryActorSessionState", queryStub);
    Cadenza.createMetaTask("Query actor_session_state in CadenzaDB", queryStub);
    Cadenza.createMetaTask("dbInsertActorSessionState", insertStub);
    Cadenza.createMetaTask("Insert actor_session_state in CadenzaDB", insertStub);

    GraphMetadataController.instance;

    const sessionActor = Cadenza.createActor<{ count: number }>({
      name: "RowsHydrationActor",
      defaultKey: "device:unknown",
      keyResolver: (input: any) =>
        typeof input?.deviceId === "string" ? input.deviceId : undefined,
      initState: {
        count: 0,
      },
      session: {
        persistDurableState: true,
        persistenceTimeoutMs: 30_000,
      },
    });

    const writeTask = sessionActor.task(
      ({ state, setState }) => {
        setState({
          count: state.count + 1,
        });
      },
      { mode: "write" },
    );

    await invokeActorTask(writeTask, { deviceId: "device-1" });

    expect(hydrationQueryCount).toBe(1);
    expect(sessionActor.getState("device-1")).toEqual({ count: 12 });
    expect(sessionActor.getDurableVersion("device-1")).toBe(12);
    expect(persistenceVersions).toEqual([12]);
  });

  it("ignores expired persisted actor_session_state rows during hydration", async () => {
    let hydrationQueryCount = 0;
    const persistenceVersions: number[] = [];

    const queryStub = (ctx: any) => {
      hydrationQueryCount += 1;
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
        actorSessionState: {
          actorName: "ExpiredHydrationActor",
          actorVersion: 1,
          actorKey: "device-9",
          serviceName: "ActorSessionPersistenceService",
          durableState: { count: 99 },
          durableVersion: 99,
          expiresAt: "2000-01-01T00:00:00.000Z",
        },
      };
    };
    const insertStub = (ctx: any) => {
      persistenceVersions.push(Number(ctx.durable_version));
      return {
        ...ctx,
        __success: true,
        rowCount: 1,
      };
    };

    Cadenza.createMetaTask("dbQueryActorSessionState", queryStub);
    Cadenza.createMetaTask("Query actor_session_state in CadenzaDB", queryStub);
    Cadenza.createMetaTask("dbInsertActorSessionState", insertStub);
    Cadenza.createMetaTask("Insert actor_session_state in CadenzaDB", insertStub);

    GraphMetadataController.instance;

    const sessionActor = Cadenza.createActor<{ count: number }>({
      name: "ExpiredHydrationActor",
      defaultKey: "device:unknown",
      keyResolver: (input: any) =>
        typeof input?.deviceId === "string" ? input.deviceId : undefined,
      initState: {
        count: 0,
      },
      session: {
        persistDurableState: true,
      },
    });

    const writeTask = sessionActor.task(
      ({ state, setState }) => {
        setState({
          count: state.count + 1,
        });
      },
      { mode: "write" },
    );

    await invokeActorTask(writeTask, { deviceId: "device-9" });

    expect(hydrationQueryCount).toBe(1);
    expect(sessionActor.getState("device-9")).toEqual({ count: 1 });
    expect(sessionActor.getDurableVersion("device-9")).toBe(1);
    expect(persistenceVersions).toEqual([1]);
  });
});
