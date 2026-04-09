import { META_ACTOR_SESSION_STATE_PERSIST_INTENT } from "@cadenza.io/core";
import Cadenza from "../../Cadenza";

const ACTOR_SESSION_STATE_PERSIST_CONCURRENCY = 20;
export const META_ACTOR_SESSION_STATE_HYDRATE_INTENT =
  "meta-actor-session-state-hydrate";
const ACTOR_SESSION_TRACE_ENABLED =
  process.env.CADENZA_ACTOR_SESSION_TRACE === "1" ||
  process.env.CADENZA_ACTOR_SESSION_TRACE === "true";

function shouldAssumeSuccessfulActorSessionRowCount(ctx: Record<string, any>): boolean {
  return (
    ctx.__success === true &&
    ctx.rowCount === undefined &&
    ctx.__status === "success" &&
    ctx.__serviceName === "CadenzaDB" &&
    ctx.__localTaskName === "Insert actor_session_state in CadenzaDB"
  );
}

export function registerActorSessionPersistenceTasks(): void {
  if (
    Cadenza.get("Persist actor session state") &&
    Cadenza.get("Hydrate actor session state")
  ) {
    return;
  }

  const localActorSessionTaskOptions = {
    register: false,
    isHidden: true,
    isSubMeta: true,
    concurrency: ACTOR_SESSION_STATE_PERSIST_CONCURRENCY,
  } as const;

  const actorSessionStateInsertTask =
    Cadenza.getLocalCadenzaDBInsertTask("actor_session_state") ??
    Cadenza.get("dbInsertActorSessionState") ??
    Cadenza.get("Insert actor_session_state in CadenzaDB") ??
    Cadenza.createCadenzaDBInsertTask(
      "actor_session_state",
      {},
      {
        concurrency: ACTOR_SESSION_STATE_PERSIST_CONCURRENCY,
        isSubMeta: true,
      },
    );
  const actorSessionStateQueryTask =
    Cadenza.getLocalCadenzaDBQueryTask("actor_session_state") ??
    Cadenza.get("dbQueryActorSessionState") ??
    Cadenza.get("Query actor_session_state in CadenzaDB") ??
    Cadenza.createCadenzaDBQueryTask(
      "actor_session_state",
      {},
      {
        concurrency: ACTOR_SESSION_STATE_PERSIST_CONCURRENCY,
        isSubMeta: true,
      },
    );

  const validateActorSessionStatePersistenceTask =
    Cadenza.createMetaTask(
      "Validate actor session state persistence",
      (ctx) => {
        if (ctx.errored || ctx.failed || ctx.__success !== true) {
          throw new Error(
            String(
              ctx.__error ??
                ctx.error ??
                "actor_session_state persistence query failed",
            ),
          );
        }

        const rowCount = shouldAssumeSuccessfulActorSessionRowCount(ctx)
          ? 1
          : Number(ctx.rowCount ?? 0);
        if (!Number.isFinite(rowCount) || rowCount <= 0) {
          throw new Error(
            "actor_session_state persistence did not affect any rows (possible stale durable_version)",
          );
        }

        return {
          __success: true,
          persisted: true,
          actor_name: ctx.actor_name,
          actor_version: ctx.actor_version,
          actor_key: ctx.actor_key,
          service_name: ctx.service_name,
          durable_version: ctx.durable_version,
          rowCount,
        };
      },
      "Enforces strict actor session persistence success contract.",
      localActorSessionTaskOptions,
    );

  const insertAndValidateActorSessionStateTask = actorSessionStateInsertTask.then(
    validateActorSessionStatePersistenceTask,
  );
  const validateActorSessionStateHydrationTask = Cadenza.createMetaTask(
    "Validate actor session state hydration",
    (ctx) => {
      if (ctx.errored || ctx.failed || ctx.__success !== true) {
        throw new Error(
          String(
            ctx.__error ?? ctx.error ?? "actor_session_state hydration query failed",
          ),
        );
      }

      const row =
        ctx.actorSessionState &&
        typeof ctx.actorSessionState === "object" &&
        !Array.isArray(ctx.actorSessionState)
          ? (ctx.actorSessionState as Record<string, any>)
          : null;

      if (!row) {
        return {
          __success: true,
          hydrated: false,
        };
      }

      const expiresAt =
        typeof row.expiresAt === "string"
          ? row.expiresAt
          : typeof row.expires_at === "string"
            ? row.expires_at
            : null;
      const expiresAtMs = expiresAt ? Date.parse(expiresAt) : Number.NaN;
      if (Number.isFinite(expiresAtMs) && expiresAtMs <= Date.now()) {
        return {
          __success: true,
          hydrated: false,
        };
      }

      const durableState =
        row.durableState ?? row.durable_state ?? null;
      const durableVersion = Number(
        row.durableVersion ?? row.durable_version ?? Number.NaN,
      );

      if (
        typeof durableState !== "object" ||
        durableState === null ||
        Array.isArray(durableState)
      ) {
        throw new Error("actor_session_state durable_state must be a non-null object");
      }

      if (!Number.isInteger(durableVersion) || durableVersion < 0) {
        throw new Error(
          "actor_session_state durable_version must be a non-negative integer",
        );
      }

      return {
        __success: true,
        hydrated: true,
        actor_name: row.actorName ?? row.actor_name,
        actor_version: row.actorVersion ?? row.actor_version,
        actor_key: row.actorKey ?? row.actor_key,
        service_name: row.serviceName ?? row.service_name,
        durable_state: durableState,
        durable_version: durableVersion,
      };
    },
    "Validates and normalizes hydrated actor_session_state rows.",
    localActorSessionTaskOptions,
  );
  const queryAndValidateActorSessionStateTask = actorSessionStateQueryTask.then(
    validateActorSessionStateHydrationTask,
  );

  Cadenza.createMetaTask(
    "Hydrate actor session state",
    (ctx) => {
      const actorName =
        typeof ctx.actor_name === "string" ? ctx.actor_name.trim() : "";
      const actorKey =
        typeof ctx.actor_key === "string" ? ctx.actor_key.trim() : "";
      const actorVersion = Number(ctx.actor_version ?? 1);
      const serviceName = Cadenza.serviceRegistry.serviceName;

      if (!actorName) {
        throw new Error("actor_name is required for actor session hydration");
      }

      if (!actorKey) {
        throw new Error("actor_key is required for actor session hydration");
      }

      if (!Number.isInteger(actorVersion) || actorVersion < 1) {
        throw new Error("actor_version must be a positive integer");
      }

      if (!serviceName) {
        throw new Error("service_name is not available for actor session hydration");
      }

      return {
        ...ctx,
        actor_name: actorName,
        actor_key: actorKey,
        actor_version: actorVersion,
        service_name: serviceName,
        queryData: {
          filter: {
            actor_name: actorName,
            actor_version: actorVersion,
            actor_key: actorKey,
            service_name: serviceName,
            deleted: false,
          },
          queryMode: "one" as const,
          sort: {
            updated: "desc" as const,
          },
        },
      };
    },
    "Builds a one-row actor_session_state lookup for lazy actor hydration.",
    localActorSessionTaskOptions,
  )
    .then(queryAndValidateActorSessionStateTask)
    .respondsTo(META_ACTOR_SESSION_STATE_HYDRATE_INTENT);

  Cadenza.createMetaTask(
    "Persist actor session state",
    (ctx) => {
      const actorName =
        typeof ctx.actor_name === "string" ? ctx.actor_name.trim() : "";
      const actorKey =
        typeof ctx.actor_key === "string" ? ctx.actor_key.trim() : "";
      const actorVersion = Number(ctx.actor_version ?? 1);
      const durableVersion = Number(ctx.durable_version);

      if (!actorName) {
        throw new Error("actor_name is required for actor session persistence");
      }

      if (!actorKey) {
        throw new Error("actor_key is required for actor session persistence");
      }

      if (!Number.isInteger(actorVersion) || actorVersion < 1) {
        throw new Error("actor_version must be a positive integer");
      }

      if (!Number.isInteger(durableVersion) || durableVersion < 0) {
        throw new Error("durable_version must be a non-negative integer");
      }

      if (
        typeof ctx.durable_state !== "object" ||
        ctx.durable_state === null ||
        Array.isArray(ctx.durable_state)
      ) {
        throw new Error("durable_state must be a non-null object");
      }

      const serviceName = Cadenza.serviceRegistry.serviceName;
      if (!serviceName) {
        throw new Error("service_name is not available for actor session persistence");
      }

      let expiresAt: string | null = null;
      if (ctx.expires_at !== undefined && ctx.expires_at !== null) {
        if (ctx.expires_at instanceof Date) {
          expiresAt = ctx.expires_at.toISOString();
        } else if (
          typeof ctx.expires_at === "string" &&
          ctx.expires_at.trim().length > 0
        ) {
          expiresAt = ctx.expires_at;
        } else {
          throw new Error("expires_at must be null, Date, or non-empty string");
        }
      }

      const updatedAt = new Date().toISOString();
      const persistenceRow = {
        actor_name: actorName,
        actor_version: actorVersion,
        actor_key: actorKey,
        service_name: serviceName,
        durable_state: ctx.durable_state,
        durable_version: durableVersion,
        expires_at: expiresAt,
        updated: updatedAt,
      };

      if (ACTOR_SESSION_TRACE_ENABLED) {
        console.log("[CADENZA_ACTOR_SESSION_TRACE] prepare_actor_session_persistence", {
          localServiceName: Cadenza.serviceRegistry.serviceName,
          actor_name: actorName,
          actor_key: actorKey,
          durable_version: durableVersion,
          inputKeys: Object.keys(ctx),
          dataKeys: Object.keys(persistenceRow),
        });
      }

      return {
        ...ctx,
        actor_name: actorName,
        actor_key: actorKey,
        actor_version: actorVersion,
        durable_version: durableVersion,
        expires_at: expiresAt,
        service_name: serviceName,
        data: persistenceRow,
        queryData: {
          data: persistenceRow,
          onConflict: {
            target: [
              "actor_name",
              "actor_version",
              "actor_key",
              "service_name",
            ],
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
          },
        },
      };
    },
    "Validates and prepares actor_session_state payload for strict write-through persistence.",
    localActorSessionTaskOptions,
  )
    .then(insertAndValidateActorSessionStateTask)
    .respondsTo(META_ACTOR_SESSION_STATE_PERSIST_INTENT);
}
