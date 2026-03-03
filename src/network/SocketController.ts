import Cadenza from "../Cadenza";
import { Server } from "socket.io";
import { IRateLimiterOptions, RateLimiterMemory } from "rate-limiter-flexible";
import type { AnyObject, Task } from "@cadenza.io/core";
import { io, Socket } from "socket.io-client";
import { isBrowser } from "../utils/environment";
import { waitForSocketConnection } from "./socketClientUtils";
import { META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT } from "../utils/inquiry";

type TransportDetailLevel = "summary" | "full";

interface TransportDiagnosticErrorEntry {
  at: string;
  message: string;
}

interface SocketClientDiagnosticsState {
  fetchId: string;
  serviceName: string;
  url: string;
  socketId: string | null;
  connected: boolean;
  handshake: boolean;
  reconnectAttempts: number;
  connectErrors: number;
  reconnectErrors: number;
  socketErrors: number;
  pendingDelegations: number;
  pendingTimers: number;
  destroyed: boolean;
  lastHandshakeAt: string | null;
  lastHandshakeError: string | null;
  lastDisconnectAt: string | null;
  lastError: string | null;
  lastErrorAt: number;
  errorHistory: TransportDiagnosticErrorEntry[];
  updatedAt: number;
}

type SocketClientSessionOperation =
  | "connect"
  | "handshake"
  | "delegate"
  | "transmit"
  | "shutdown";

interface SocketClientSessionState {
  fetchId: string;
  serviceInstanceId: string;
  communicationTypes: string[];
  serviceName: string;
  serviceAddress: string;
  servicePort: number;
  protocol: string;
  url: string;
  socketId: string | null;
  connected: boolean;
  handshake: boolean;
  pendingDelegations: number;
  pendingTimers: number;
  reconnectAttempts: number;
  connectErrors: number;
  reconnectErrors: number;
  socketErrors: number;
  errorCount: number;
  destroyed: boolean;
  lastHandshakeAt: string | null;
  lastHandshakeError: string | null;
  lastDisconnectAt: string | null;
  updatedAt: number;
}

type SocketEmitWhenReady = <T>(
  event: string,
  data: AnyObject,
  timeoutMs: number,
  ack?: (response: T) => void,
) => Promise<T>;

interface SocketClientRuntimeHandle {
  url: string;
  socket: Socket;
  initialized: boolean;
  handshake: boolean;
  errorCount: number;
  pendingDelegationIds: Set<string>;
  pendingTimers: Set<NodeJS.Timeout>;
  emitWhenReady: SocketEmitWhenReady | null;
  handshakeTask: Task | null;
  delegateTask: Task | null;
  transmitTask: Task | null;
}

interface SocketServerSessionState {
  serverKey: string;
  useSocket: boolean;
  status: "inactive" | "active" | "shutdown";
  securityProfile: string;
  networkType: string;
  connectionCount: number;
  lastStartedAt: string | null;
  lastConnectedAt: string | null;
  lastDisconnectedAt: string | null;
  lastShutdownAt: string | null;
  updatedAt: number;
}

interface SocketServerRuntimeHandle {
  server: Server;
  initialized: boolean;
  connectedSocketIds: Set<string>;
  broadcastStatusTask: Task | null;
  shutdownTask: Task | null;
}

/**
 * Socket transport orchestration in the Cadenza primitive ecosystem.
 *
 * - setup is signal-triggered
 * - state/runtime ownership is actor-backed
 * - dynamic runtime tasks are still allowed for advanced orchestration (ephemeral resolvers etc.)
 */
export default class SocketController {
  private static _instance: SocketController;
  public static get instance(): SocketController {
    if (!this._instance) this._instance = new SocketController();
    return this._instance;
  }

  private readonly diagnosticsErrorHistoryLimit = 100;
  private readonly diagnosticsMaxClientEntries = 500;
  private readonly destroyedDiagnosticsTtlMs = 15 * 60_000;
  private readonly socketServerDefaultKey = "socket-server-default";
  private readonly socketServerInitialSessionState: SocketServerSessionState = {
    serverKey: this.socketServerDefaultKey,
    useSocket: false,
    status: "inactive",
    securityProfile: "medium",
    networkType: "internal",
    connectionCount: 0,
    lastStartedAt: null,
    lastConnectedAt: null,
    lastDisconnectedAt: null,
    lastShutdownAt: null,
    updatedAt: 0,
  };
  private readonly socketClientInitialSessionState: SocketClientSessionState = {
    fetchId: "",
    serviceInstanceId: "",
    communicationTypes: [],
    serviceName: "",
    serviceAddress: "",
    servicePort: 0,
    protocol: "http",
    url: "",
    socketId: null,
    connected: false,
    handshake: false,
    pendingDelegations: 0,
    pendingTimers: 0,
    reconnectAttempts: 0,
    connectErrors: 0,
    reconnectErrors: 0,
    socketErrors: 0,
    errorCount: 0,
    destroyed: false,
    lastHandshakeAt: null,
    lastHandshakeError: null,
    lastDisconnectAt: null,
    updatedAt: 0,
  };

  private readonly socketServerActor = Cadenza.createActor<
    SocketServerSessionState,
    SocketServerRuntimeHandle | null
  >(
    {
      name: "SocketServerActor",
      description:
        "Holds durable socket server session state and runtime socket server handle",
      defaultKey: this.socketServerDefaultKey,
      keyResolver: (input) => this.resolveSocketServerKey(input),
      loadPolicy: "lazy",
      writeContract: "overwrite",
      initState: this.socketServerInitialSessionState,
    },
    { isMeta: true },
  );

  private readonly socketClientActor = Cadenza.createActor<
    SocketClientSessionState,
    SocketClientRuntimeHandle | null
  >(
    {
      name: "SocketClientActor",
      description:
        "Holds durable socket client session state and runtime socket connection handles",
      defaultKey: "socket-client-default",
      keyResolver: (input) => this.resolveSocketClientFetchId(input),
      loadPolicy: "lazy",
      writeContract: "overwrite",
      initState: this.socketClientInitialSessionState,
    },
    { isMeta: true },
  );

  private readonly socketClientDiagnosticsActor = Cadenza.createActor<{
    entries: Record<string, SocketClientDiagnosticsState>;
  }>(
    {
      name: "SocketClientDiagnosticsActor",
      description:
        "Tracks socket client diagnostics snapshots per fetchId for transport observability",
      defaultKey: "socket-client-diagnostics",
      loadPolicy: "eager",
      writeContract: "overwrite",
      initState: {
        entries: {},
      },
    },
    { isMeta: true },
  );

  constructor() {
    this.registerDiagnosticsTasks();
    this.registerSocketServerTasks();
    this.registerSocketClientTasks();

    Cadenza.createMetaTask(
      "Collect socket transport diagnostics",
      this.socketClientDiagnosticsActor.task(
        ({ state, input }) =>
          this.collectSocketTransportDiagnostics(input, state.entries),
        { mode: "read" },
      ),
      "Responds to distributed transport diagnostics inquiries with socket client data.",
    ).respondsTo(META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT);
  }

  private registerDiagnosticsTasks(): void {
    Cadenza.createThrottledMetaTask(
      "SocketClientDiagnosticsActor.Upsert",
      this.socketClientDiagnosticsActor.task(
        ({ state, input, setState }) => {
          const fetchId = String(input.fetchId ?? "").trim();
          if (!fetchId) {
            return;
          }

          const now = Date.now();
          const entries = { ...state.entries };
          const existing = entries[fetchId];

          const base: SocketClientDiagnosticsState = existing
            ? {
                ...existing,
                errorHistory: [...existing.errorHistory],
              }
            : {
                fetchId,
                serviceName: String(input.serviceName ?? ""),
                url: String(input.url ?? ""),
                socketId: null,
                connected: false,
                handshake: false,
                reconnectAttempts: 0,
                connectErrors: 0,
                reconnectErrors: 0,
                socketErrors: 0,
                pendingDelegations: 0,
                pendingTimers: 0,
                destroyed: false,
                lastHandshakeAt: null,
                lastHandshakeError: null,
                lastDisconnectAt: null,
                lastError: null,
                lastErrorAt: 0,
                errorHistory: [],
                updatedAt: now,
              };

          if (input.serviceName !== undefined) {
            base.serviceName = String(input.serviceName);
          }
          if (input.url !== undefined) {
            base.url = String(input.url);
          }

          const patch =
            input.patch && typeof input.patch === "object"
              ? (input.patch as Partial<SocketClientDiagnosticsState>)
              : {};

          Object.assign(base, patch);
          base.fetchId = fetchId;
          base.updatedAt = now;

          const errorMessage =
            input.error !== undefined ? this.getErrorMessage(input.error) : undefined;
          if (errorMessage) {
            base.lastError = errorMessage;
            base.lastErrorAt = now;
            base.errorHistory.push({
              at: new Date(now).toISOString(),
              message: errorMessage,
            });
            if (base.errorHistory.length > this.diagnosticsErrorHistoryLimit) {
              base.errorHistory.splice(
                0,
                base.errorHistory.length - this.diagnosticsErrorHistoryLimit,
              );
            }
          }

          entries[fetchId] = base;

          this.pruneDiagnosticsEntries(entries, now);

          setState({ entries });
        },
        { mode: "write" },
      ),
      (context) => String(context?.fetchId ?? "default"),
      "Upserts socket client diagnostics in actor state.",
    ).doOn("meta.socket_client.diagnostics_upsert_requested");
  }

  private registerSocketServerTasks(): void {
    Cadenza.createThrottledMetaTask(
      "SocketServerActor.PatchSession",
      this.socketServerActor.task(
        ({ state, input, setState }) => {
          const patch =
            input.patch && typeof input.patch === "object"
              ? (input.patch as Partial<SocketServerSessionState>)
              : {};
          setState({
            ...state,
            ...patch,
            updatedAt: Date.now(),
          });
        },
        { mode: "write" },
      ),
      (context) => String(context?.serverKey ?? this.socketServerDefaultKey),
      "Applies partial durable session updates for socket server actor.",
    ).doOn("meta.socket_server.session_patch_requested");

    Cadenza.createMetaTask(
      "SocketServerActor.ClearRuntime",
      this.socketServerActor.task(
        ({ setRuntimeState }) => {
          setRuntimeState(null);
        },
        { mode: "write" },
      ),
      "Clears socket server runtime handle after shutdown.",
    ).doOn("meta.socket_server.runtime_clear_requested");

    const setupSocketServerTask = Cadenza.createMetaTask(
      "Setup SocketServer",
      this.socketServerActor.task(
        ({ state, runtimeState, input, actor, setState, setRuntimeState, emit }) => {
          const serverKey =
            this.resolveSocketServerKey(input) ?? actor.key ?? this.socketServerDefaultKey;
          const shouldUseSocket = Boolean(input.__useSocket);

          if (!shouldUseSocket) {
            this.destroySocketServerRuntimeHandle(runtimeState);
            setRuntimeState(null);
            setState({
              ...state,
              serverKey,
              useSocket: false,
              status: "inactive",
              connectionCount: 0,
              lastShutdownAt: new Date().toISOString(),
              updatedAt: Date.now(),
            });
            return;
          }

          let runtimeHandle = runtimeState;
          if (!runtimeHandle) {
            runtimeHandle = this.createSocketServerRuntimeHandleFromContext(input);
            setRuntimeState(runtimeHandle);
          }

          const profile = String(input.__securityProfile ?? state.securityProfile ?? "medium");
          const networkType = String(input.__networkType ?? state.networkType ?? "internal");

          const schedulePatch = (patch: Partial<SocketServerSessionState>) => {
            Cadenza.emit("meta.socket_server.session_patch_requested", {
              serverKey,
              patch,
            });
          };

          if (runtimeHandle.initialized) {
            schedulePatch({
              status: "active",
              useSocket: true,
              securityProfile: profile,
              networkType,
              connectionCount: runtimeHandle.connectedSocketIds.size,
              lastStartedAt: state.lastStartedAt ?? new Date().toISOString(),
            });
            return;
          }

          const server = runtimeHandle.server;
          runtimeHandle.initialized = true;

          setState({
            ...state,
            serverKey,
            useSocket: true,
            status: "active",
            securityProfile: profile,
            networkType,
            connectionCount: runtimeHandle.connectedSocketIds.size,
            lastStartedAt: state.lastStartedAt ?? new Date().toISOString(),
            updatedAt: Date.now(),
          });

          server.use((socket, next) => {
            const origin = socket?.handshake?.headers?.origin;
            const allowedOrigins = ["*"];
            let effectiveOrigin = origin || "unknown";
            if (networkType === "internal") effectiveOrigin = "internal";

            if (
              profile !== "low" &&
              !allowedOrigins.includes(effectiveOrigin) &&
              !allowedOrigins.includes("*")
            ) {
              return next(new Error("Unauthorized origin"));
            }

            const limiterOptions: { [key: string]: IRateLimiterOptions } = {
              low: { points: Infinity, duration: 1 },
              medium: { points: 10000, duration: 10 },
              high: { points: 1000, duration: 60, blockDuration: 300 },
            };

            const limiter = new RateLimiterMemory(
              limiterOptions[profile] ?? limiterOptions.medium,
            );
            const clientKey = socket?.handshake?.address || "unknown";

            socket.use((packet, packetNext) => {
              limiter
                .consume(clientKey)
                .then(() => packetNext())
                .catch((rej) => {
                  if (rej.msBeforeNext > 0) {
                    Cadenza.log(
                      "SocketServer: Rate limit exceeded",
                      {
                        retryAfter: rej.msBeforeNext / 1000,
                        clientKey,
                        socketId: socket.id,
                      },
                      "warning",
                    );
                    socket.emit("error", {
                      message: "Rate limit exceeded",
                      retryAfter: rej.msBeforeNext / 1000,
                    });
                    packetNext(new Error("Rate limit exceeded"));
                  } else {
                    Cadenza.log(
                      "SocketServer: Rate limit exceeded, blocked",
                      {
                        clientKey,
                        socketId: socket.id,
                      },
                      "critical",
                    );
                    socket.disconnect(true);
                    packetNext(new Error("Blocked"));
                  }
                });
            });

            next();
          });

          server.on("connection", (ws: any) => {
            runtimeHandle.connectedSocketIds.add(ws.id);
            schedulePatch({
              connectionCount: runtimeHandle.connectedSocketIds.size,
              lastConnectedAt: new Date().toISOString(),
              status: "active",
            });

            try {
              ws.on("handshake", (ctx: AnyObject, callback: (result: AnyObject) => void) => {
                Cadenza.log("SocketServer: New connection", {
                  ...ctx,
                  socketId: ws.id,
                });

                callback({
                  status: "success",
                  serviceName: Cadenza.serviceRegistry.serviceName,
                });

                if (ctx.isFrontend) {
                  const fetchId = `browser:${ctx.serviceInstanceId}`;
                  Cadenza.createMetaTask(
                    `Transmit signal to ${fetchId}`,
                    (c, emitter) => {
                      if (c.__signalName === undefined) {
                        return;
                      }

                      ws.emit("signal", c);

                      if (c.__routineExecId) {
                        emitter(`meta.socket_client.transmitted:${c.__routineExecId}`, {});
                      }
                    },
                    "Transmit frontend bound signal through active websocket.",
                  )
                    .doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`)
                    .attachSignal("meta.socket_client.transmitted");
                }

                Cadenza.emit("meta.socket.handshake", ctx);
              });

              ws.on("delegation", (ctx: AnyObject, callback: (context: AnyObject) => void) => {
                const deputyExecId = ctx.__metadata.__deputyExecId;

                Cadenza.createEphemeralMetaTask(
                  "Resolve delegation",
                  (delegationCtx: AnyObject) => {
                    callback(delegationCtx);
                  },
                  "Resolves a delegation request using client callback.",
                  { register: false },
                )
                  .doOn(`meta.node.graph_completed:${deputyExecId}`)
                  .emits(`meta.socket.delegation_resolved:${deputyExecId}`);

                Cadenza.createEphemeralMetaTask(
                  "Delegation progress update",
                  (progressCtx) => {
                    if (progressCtx.__progress !== undefined) {
                      ws.emit("delegation_progress", progressCtx);
                    }
                  },
                  "Updates delegation progress to client.",
                  {
                    once: false,
                    destroyCondition: (progressCtx: AnyObject) =>
                      progressCtx.data.progress === 1.0 ||
                      progressCtx.data?.progress === undefined,
                    register: false,
                  },
                )
                  .doOn(
                    `meta.node.routine_execution_progress:${deputyExecId}`,
                    `meta.node.graph_completed:${deputyExecId}`,
                  )
                  .emitsOnFail(`meta.socket.progress_failed:${deputyExecId}`);

                Cadenza.emit("meta.socket.delegation_requested", {
                  ...ctx,
                  __name: ctx.__remoteRoutineName,
                });
              });

              ws.on("signal", (ctx: AnyObject, callback: (context: AnyObject) => void) => {
                if (Cadenza.signalBroker.listObservedSignals().includes(ctx.__signalName)) {
                  callback({
                    __status: "success",
                    __signalName: ctx.__signalName,
                  });

                  Cadenza.emit(ctx.__signalName, ctx);
                } else {
                  Cadenza.log(
                    `No such signal ${ctx.__signalName} on ${ctx.__serviceName}`,
                    "warning",
                  );
                  callback({
                    ...ctx,
                    __status: "error",
                    __error: `No such signal: ${ctx.__signalName}`,
                    errored: true,
                  });
                }
              });

              ws.on(
                "status_check",
                (ctx: AnyObject, callback: (context: AnyObject) => void) => {
                  Cadenza.createEphemeralMetaTask(
                    "Resolve status check",
                    callback,
                    "Resolves a status check request",
                    { register: false },
                  ).doAfter(Cadenza.serviceRegistry.getStatusTask);

                  Cadenza.emit("meta.socket.status_check_requested", ctx);
                },
              );

              ws.on("disconnect", () => {
                runtimeHandle.connectedSocketIds.delete(ws.id);
                schedulePatch({
                  connectionCount: runtimeHandle.connectedSocketIds.size,
                  lastDisconnectedAt: new Date().toISOString(),
                });
                Cadenza.log(
                  "Socket client disconnected",
                  { socketId: ws.id },
                  "warning",
                );
                Cadenza.emit("meta.socket.disconnected", {
                  __wsId: ws.id,
                });
              });
            } catch (error) {
              Cadenza.log(
                "SocketServer: Error in socket event",
                { error },
                "error",
              );
            }

            Cadenza.emit("meta.socket.connected", { __wsId: ws.id });
          });

          runtimeHandle.broadcastStatusTask = Cadenza.createMetaTask(
            `Broadcast status ${serverKey}`,
            (ctx) => server.emit("status_update", ctx),
            "Broadcasts the status of the server to all clients",
          ).doOn("meta.service.updated");

          runtimeHandle.shutdownTask = Cadenza.createMetaTask(
            `Shutdown SocketServer ${serverKey}`,
            async () => {
              this.destroySocketServerRuntimeHandle(runtimeHandle);

              Cadenza.emit("meta.socket_server.runtime_clear_requested", {
                serverKey,
              });
              Cadenza.emit("meta.socket_server.session_patch_requested", {
                serverKey,
                patch: {
                  useSocket: false,
                  status: "shutdown",
                  connectionCount: 0,
                  lastShutdownAt: new Date().toISOString(),
                },
              });
            },
            "Shuts down the socket server",
          )
            .doOn("meta.socket_server_shutdown_requested")
            .emits("meta.socket.shutdown");

          return true;
        },
        { mode: "write" },
      ),
      "Initializes socket server runtime through actor state.",
    );

    setupSocketServerTask.doOn("global.meta.rest.network_configured");
  }

  private registerSocketClientTasks(): void {
    Cadenza.createThrottledMetaTask(
      "SocketClientActor.ApplySessionOperation",
      this.socketClientActor.task(
        ({ state, input, setState }) => {
          const operation = String(
            input.operation ?? "transmit",
          ) as SocketClientSessionOperation;
          const patch =
            input.patch && typeof input.patch === "object"
              ? (input.patch as Partial<SocketClientSessionState>)
              : {};

          let next: SocketClientSessionState = {
            ...state,
            ...patch,
            communicationTypes:
              patch.communicationTypes !== undefined
                ? this.normalizeCommunicationTypes(patch.communicationTypes)
                : state.communicationTypes,
            updatedAt: Date.now(),
          };

          if (input.serviceName !== undefined) {
            next.serviceName = String(input.serviceName);
          }
          if (input.serviceAddress !== undefined) {
            next.serviceAddress = String(input.serviceAddress);
          }
          if (input.serviceInstanceId !== undefined) {
            next.serviceInstanceId = String(input.serviceInstanceId);
          }
          if (input.protocol !== undefined) {
            next.protocol = String(input.protocol);
          }
          if (input.url !== undefined) {
            next.url = String(input.url);
          }
          if (input.servicePort !== undefined) {
            next.servicePort = Number(input.servicePort);
          }
          if (input.fetchId !== undefined) {
            next.fetchId = String(input.fetchId);
          }

          if (operation === "connect") {
            next.destroyed = false;
          } else if (operation === "handshake") {
            next.destroyed = false;
            next.connected = patch.connected ?? true;
            next.handshake = patch.handshake ?? true;
          } else if (operation === "shutdown") {
            next.connected = false;
            next.handshake = false;
            next.destroyed = true;
            next.pendingDelegations = 0;
            next.pendingTimers = 0;
          }

          setState(next);
          return next;
        },
        { mode: "write" },
      ),
      (context) =>
        String(this.resolveSocketClientFetchId(context ?? {}) ?? "default"),
      "Applies socket client session operation patch in actor durable state.",
    ).doOn("meta.socket_client.session_operation_requested");

    Cadenza.createMetaTask(
      "SocketClientActor.ClearRuntime",
      this.socketClientActor.task(
        ({ setRuntimeState }) => {
          setRuntimeState(null);
        },
        { mode: "write" },
      ),
      "Clears socket client runtime handle.",
    ).doOn("meta.socket_client.runtime_clear_requested");

    Cadenza.createMetaTask(
      "Connect to socket server",
      this.socketClientActor.task(
        ({ state, runtimeState, input, setState, setRuntimeState, emit }) => {
          const serviceInstanceId = String(input.serviceInstanceId ?? "");
          const communicationTypes = this.normalizeCommunicationTypes(
            input.communicationTypes,
          );
          const serviceName = String(input.serviceName ?? "");
          const serviceAddress = String(input.serviceAddress ?? "");
          const protocol = String(input.protocol ?? "http");

          const normalizedPort = this.resolveServicePort(protocol, input.servicePort);
          if (!serviceAddress || !normalizedPort) {
            Cadenza.log(
              "Socket client setup skipped due to missing address/port",
              {
                serviceName,
                serviceAddress,
                servicePort: input.servicePort,
                protocol,
              },
              "warning",
            );
            return false;
          }

          const socketProtocol = protocol === "https" ? "wss" : "ws";
          const url = `${socketProtocol}://${serviceAddress}:${normalizedPort}`;
          const fetchId = `${serviceAddress}_${normalizedPort}`;

          const applySessionOperation = (
            operation: SocketClientSessionOperation,
            patch: Partial<SocketClientSessionState> = {},
          ) => {
            Cadenza.emit("meta.socket_client.session_operation_requested", {
              fetchId,
              operation,
              patch,
              serviceInstanceId,
              communicationTypes,
              serviceName,
              serviceAddress,
              servicePort: normalizedPort,
              protocol,
              url,
            });
          };

          const upsertDiagnostics = (
            patch: Partial<SocketClientDiagnosticsState>,
            error?: unknown,
          ) => {
            Cadenza.emit("meta.socket_client.diagnostics_upsert_requested", {
              fetchId,
              serviceName,
              url,
              patch,
              error,
            });
          };

          setState({
            ...state,
            fetchId,
            serviceInstanceId,
            communicationTypes,
            serviceName,
            serviceAddress,
            servicePort: normalizedPort,
            protocol,
            url,
            destroyed: false,
            updatedAt: Date.now(),
          });

          let runtimeHandle = runtimeState;
          if (!runtimeHandle || runtimeHandle.url !== url) {
            this.destroySocketClientRuntimeHandle(runtimeHandle);
            runtimeHandle = this.createSocketClientRuntimeHandle(url);
            setRuntimeState(runtimeHandle);
          }

          upsertDiagnostics({
            destroyed: false,
            connected: false,
            handshake: false,
            socketId: runtimeHandle.socket.id ?? null,
          });
          applySessionOperation("connect", {
            destroyed: false,
            connected: false,
            handshake: false,
            socketId: runtimeHandle.socket.id ?? null,
            pendingDelegations: runtimeHandle.pendingDelegationIds.size,
            pendingTimers: runtimeHandle.pendingTimers.size,
            errorCount: runtimeHandle.errorCount,
          });

          if (runtimeHandle.initialized) {
            return true;
          }

          runtimeHandle.initialized = true;
          runtimeHandle.handshake = false;
          runtimeHandle.errorCount = 0;

          const syncPendingCounts = () => {
            const pendingDelegations = runtimeHandle.pendingDelegationIds.size;
            const pendingTimers = runtimeHandle.pendingTimers.size;
            upsertDiagnostics({
              pendingDelegations,
              pendingTimers,
            });
            applySessionOperation("delegate", {
              pendingDelegations,
              pendingTimers,
            });
          };

          runtimeHandle.emitWhenReady = <T>(
            event: string,
            data: AnyObject,
            timeoutMs: number = 60_000,
            ack?: (response: T) => void,
          ): Promise<T> => {
            return new Promise((resolve) => {
              const parsedTimeout = Number(timeoutMs);
              const normalizedTimeoutMs =
                Number.isFinite(parsedTimeout) && parsedTimeout > 0
                  ? Math.trunc(parsedTimeout)
                  : 60_000;

              let timer: NodeJS.Timeout | null = null;
              let settled = false;

              const clearPendingTimer = () => {
                if (!timer) {
                  return;
                }
                clearTimeout(timer);
                runtimeHandle.pendingTimers.delete(timer);
                syncPendingCounts();
                timer = null;
              };

              const settle = (response: T) => {
                if (settled) {
                  return;
                }
                settled = true;
                clearPendingTimer();
                if (ack) ack(response);
                resolve(response);
              };

              const resolveWithError = (errorMessage: string, fallbackError?: unknown) => {
                settle({
                  ...data,
                  errored: true,
                  __error: errorMessage,
                  error:
                    fallbackError instanceof Error
                      ? fallbackError.message
                      : errorMessage,
                  socketId: runtimeHandle.socket.id,
                  serviceName,
                  url,
                } as T);
              };

              const tryEmit = async () => {
                const waitResult = await waitForSocketConnection(
                  runtimeHandle.socket,
                  normalizedTimeoutMs + 10,
                  (reason, error) => {
                    if (reason === "connect_timeout") {
                      return `Socket connect timed out before '${event}'`;
                    }
                    if (reason === "connect_error") {
                      const errMessage =
                        error instanceof Error ? error.message : String(error);
                      return `Socket connect error before '${event}': ${errMessage}`;
                    }
                    return `Socket disconnected before '${event}'`;
                  },
                );

                if (!waitResult.ok) {
                  Cadenza.log(
                    waitResult.error,
                    {
                      socketId: runtimeHandle.socket.id,
                      serviceName,
                      url,
                      event,
                    },
                    "error",
                  );
                  upsertDiagnostics({}, waitResult.error);
                  resolveWithError(waitResult.error);
                  return;
                }

                timer = setTimeout(() => {
                  if (settled) {
                    return;
                  }
                  clearPendingTimer();
                  const message = `Socket event '${event}' timed out`;
                  Cadenza.log(
                    message,
                    { socketId: runtimeHandle.socket.id, serviceName, url },
                    "error",
                  );
                  upsertDiagnostics(
                    {
                      lastHandshakeError: message,
                    },
                    message,
                  );
                  applySessionOperation("transmit", {
                    lastHandshakeError: message,
                  });
                  resolveWithError(message);
                }, normalizedTimeoutMs + 10);

                runtimeHandle.pendingTimers.add(timer);
                syncPendingCounts();

                runtimeHandle.socket
                  .timeout(normalizedTimeoutMs)
                  .emit(event, data, (err: any, response: T) => {
                    if (err) {
                      Cadenza.log(
                        "Socket timeout.",
                        {
                          event,
                          error: err.message,
                          socketId: runtimeHandle.socket.id,
                          serviceName,
                        },
                        "warning",
                      );
                      upsertDiagnostics(
                        {
                          lastHandshakeError: err.message,
                        },
                        err,
                      );
                      applySessionOperation("transmit", {
                        lastHandshakeError: err.message,
                      });
                      response = {
                        __error: `Timeout error: ${err}`,
                        errored: true,
                        ...data,
                      } as T;
                    }
                    settle(response);
                  });
              };

              void tryEmit().catch((error) => {
                Cadenza.log(
                  "Socket emit failed unexpectedly",
                  {
                    event,
                    error:
                      error instanceof Error ? error.message : String(error),
                    socketId: runtimeHandle.socket.id,
                    serviceName,
                    url,
                  },
                  "error",
                );
                const message = `Socket event '${event}' failed`;
                upsertDiagnostics(
                  {
                    lastHandshakeError:
                      error instanceof Error ? error.message : String(error),
                  },
                  error,
                );
                applySessionOperation("transmit", {
                  lastHandshakeError:
                    error instanceof Error ? error.message : String(error),
                });
                resolveWithError(message, error);
              });
            });
          };

          const socket = runtimeHandle.socket;

          socket.on("connect", () => {
            if (runtimeHandle.handshake) return;
            upsertDiagnostics({
              connected: true,
              destroyed: false,
              socketId: socket.id ?? null,
            });
            applySessionOperation("connect", {
              connected: true,
              destroyed: false,
              socketId: socket.id ?? null,
            });
            Cadenza.emit(`meta.socket_client.connected:${fetchId}`, input);
          });

          socket.on("delegation_progress", (delegationCtx) => {
            Cadenza.emit(
              `meta.socket_client.delegation_progress:${delegationCtx.__metadata.__deputyExecId}`,
              delegationCtx,
            );
          });

          socket.on("signal", (signalCtx) => {
            if (Cadenza.signalBroker.listObservedSignals().includes(signalCtx.__signalName)) {
              Cadenza.emit(signalCtx.__signalName, signalCtx);
            }
          });

          socket.on("status_update", (status) => {
            Cadenza.emit("meta.socket_client.status_received", status);
          });

          socket.on("connect_error", (err) => {
            runtimeHandle.handshake = false;
            upsertDiagnostics(
              {
                connected: false,
                handshake: false,
                connectErrors: state.connectErrors + 1,
                lastHandshakeError: err.message,
              },
              err,
            );
            applySessionOperation("connect", {
              connected: false,
              handshake: false,
              connectErrors: state.connectErrors + 1,
              lastHandshakeError: err.message,
            });
            Cadenza.log(
              "Socket connect error",
              {
                error: err.message,
                serviceName,
                socketId: socket.id,
                url,
              },
              "error",
            );
            Cadenza.emit(`meta.socket_client.connect_error:${fetchId}`, err);
          });

          socket.on("reconnect_attempt", (attempt) => {
            upsertDiagnostics({ reconnectAttempts: attempt });
            applySessionOperation("connect", {
              reconnectAttempts: attempt,
            });
            Cadenza.log(`Reconnect attempt: ${attempt}`);
          });

          socket.on("reconnect", (attempt) => {
            upsertDiagnostics({ connected: true });
            applySessionOperation("connect", {
              connected: true,
            });
            Cadenza.log(`Socket reconnected after ${attempt} tries`, {
              socketId: socket.id,
              url,
              serviceName,
            });
          });

          socket.on("reconnect_error", (err) => {
            runtimeHandle.handshake = false;
            upsertDiagnostics(
              {
                connected: false,
                handshake: false,
                reconnectErrors: state.reconnectErrors + 1,
                lastHandshakeError: err.message,
              },
              err,
            );
            applySessionOperation("connect", {
              connected: false,
              handshake: false,
              reconnectErrors: state.reconnectErrors + 1,
              lastHandshakeError: err.message,
            });
            Cadenza.log(
              "Socket reconnect failed.",
              { error: err.message, serviceName, url, socketId: socket.id },
              "warning",
            );
          });

          socket.on("error", (err) => {
            runtimeHandle.errorCount += 1;
            upsertDiagnostics(
              {
                socketErrors: state.socketErrors + 1,
                lastHandshakeError: this.getErrorMessage(err),
              },
              err,
            );
            applySessionOperation("transmit", {
              socketErrors: state.socketErrors + 1,
              errorCount: runtimeHandle.errorCount,
              lastHandshakeError: this.getErrorMessage(err),
            });
            Cadenza.log(
              "Socket error",
              { error: err, socketId: socket.id, url, serviceName },
              "error",
            );
            Cadenza.emit("meta.socket_client.error", err);
          });

          socket.on("disconnect", () => {
            const disconnectedAt = new Date().toISOString();
            upsertDiagnostics({
              connected: false,
              handshake: false,
              lastDisconnectAt: disconnectedAt,
            });
            applySessionOperation("connect", {
              connected: false,
              handshake: false,
              lastDisconnectAt: disconnectedAt,
            });
            Cadenza.log(
              "Socket disconnected.",
              { url, serviceName, socketId: socket.id },
              "warning",
            );
            Cadenza.emit(`meta.socket_client.disconnected:${fetchId}`, {
              serviceName,
              serviceAddress,
              servicePort: normalizedPort,
            });
            runtimeHandle.handshake = false;
          });

          socket.connect();

          runtimeHandle.handshakeTask = Cadenza.createMetaTask(
            `Socket handshake with ${url}`,
            async (_ctx, emitter) => {
              if (runtimeHandle.handshake) return;
              runtimeHandle.handshake = true;

              upsertDiagnostics({
                handshake: true,
              });
              applySessionOperation("handshake", {
                handshake: true,
              });

              await runtimeHandle.emitWhenReady?.(
                "handshake",
                {
                  serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
                  serviceName: Cadenza.serviceRegistry.serviceName,
                  isFrontend: isBrowser,
                  __status: "success",
                },
                10_000,
                (result: any) => {
                  if (result.status === "success") {
                    const handshakeAt = new Date().toISOString();
                    upsertDiagnostics({
                      connected: true,
                      handshake: true,
                      lastHandshakeAt: handshakeAt,
                      lastHandshakeError: null,
                      socketId: socket.id ?? null,
                    });
                    applySessionOperation("handshake", {
                      connected: true,
                      handshake: true,
                      lastHandshakeAt: handshakeAt,
                      lastHandshakeError: null,
                      socketId: socket.id ?? null,
                    });
                    Cadenza.log("Socket client connected", {
                      result,
                      serviceName,
                      socketId: socket.id,
                      url,
                    });
                  } else {
                    const errorMessage =
                      result?.__error ?? result?.error ?? "Socket handshake failed";
                    upsertDiagnostics(
                      {
                        connected: false,
                        handshake: false,
                        lastHandshakeError: errorMessage,
                      },
                      errorMessage,
                    );
                    applySessionOperation("handshake", {
                      connected: false,
                      handshake: false,
                      lastHandshakeError: errorMessage,
                    });
                    Cadenza.log(
                      "Socket handshake failed",
                      { result, serviceName, socketId: socket.id, url },
                      "warning",
                    );
                  }

                  // If needed in future:
                  // runtimeHandle.errorCount threshold can request shutdown signal.
                  void emitter;
                },
              );
            },
            "Handshakes with socket server",
          ).doOn(`meta.socket_client.connected:${fetchId}`);

          runtimeHandle.delegateTask = Cadenza.createMetaTask(
            `Delegate flow to Socket service ${url}`,
            async (delegateCtx, emitter) => {
              if (delegateCtx.__remoteRoutineName === undefined) {
                return;
              }

              delete delegateCtx.__isSubMeta;
              delete delegateCtx.__broadcast;

              const deputyExecId = delegateCtx.__metadata?.__deputyExecId;
              const requestSentAt = Date.now();
              if (deputyExecId) {
                runtimeHandle.pendingDelegationIds.add(deputyExecId);
                syncPendingCounts();
              }

              try {
                const resultContext =
                  ((await runtimeHandle.emitWhenReady?.(
                    "delegation",
                    delegateCtx,
                    delegateCtx.__timeout ?? 60_000,
                  )) as AnyObject | undefined) ??
                  ({
                    errored: true,
                    __error: "Socket delegation returned no response",
                  } as AnyObject);

                const requestDuration = Date.now() - requestSentAt;
                const metadata = resultContext.__metadata;
                delete resultContext.__metadata;

                if (deputyExecId) {
                  emitter(`meta.socket_client.delegated:${deputyExecId}`, {
                    ...resultContext,
                    ...metadata,
                    __requestDuration: requestDuration,
                  });
                }

                if (resultContext?.errored || resultContext?.failed) {
                  const errorMessage =
                    resultContext?.__error ??
                    resultContext?.error ??
                    "Socket delegation failed";
                  upsertDiagnostics(
                    {
                      lastHandshakeError: String(errorMessage),
                    },
                    errorMessage,
                  );
                  applySessionOperation("delegate", {
                    lastHandshakeError: String(errorMessage),
                  });
                }

                return resultContext;
              } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                const failedContext = {
                  errored: true,
                  __error: message,
                };

                if (deputyExecId) {
                  emitter(`meta.socket_client.delegated:${deputyExecId}`, {
                    ...failedContext,
                    __requestDuration: Date.now() - requestSentAt,
                  });
                }

                upsertDiagnostics(
                  {
                    lastHandshakeError: message,
                  },
                  error,
                );
                applySessionOperation("delegate", {
                  lastHandshakeError: message,
                });
                return failedContext;
              } finally {
                if (deputyExecId) {
                  runtimeHandle.pendingDelegationIds.delete(deputyExecId);
                  syncPendingCounts();
                }
              }
            },
            `Delegate flow to service ${serviceName} with address ${url}`,
          )
            .doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`)
            .attachSignal(
              "meta.socket_client.delegated",
              "meta.socket_shutdown_requested",
            );

          runtimeHandle.transmitTask = Cadenza.createMetaTask(
            `Transmit signal to socket server ${url}`,
            async (signalCtx, emitter) => {
              if (signalCtx.__signalName === undefined) {
                return;
              }

              delete signalCtx.__broadcast;

              const response =
                ((await runtimeHandle.emitWhenReady?.("signal", signalCtx, 5_000)) as
                  | AnyObject
                  | undefined) ??
                ({
                  errored: true,
                  __error: "Socket signal transmission returned no response",
                } as AnyObject);

              applySessionOperation("transmit", {});

              if (signalCtx.__routineExecId) {
                emitter(`meta.socket_client.transmitted:${signalCtx.__routineExecId}`, {
                  ...response,
                });
              }

              return response;
            },
            `Transmits signal to service ${serviceName} with address ${url}`,
          )
            .doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`)
            .attachSignal("meta.socket_client.transmitted");

          Cadenza.createEphemeralMetaTask(
            `Shutdown SocketClient ${url}`,
            (_ctx, emitter) => {
              runtimeHandle.handshake = false;

              upsertDiagnostics({
                connected: false,
                handshake: false,
                destroyed: true,
                pendingDelegations: 0,
                pendingTimers: 0,
              });
              applySessionOperation("shutdown", {
                connected: false,
                handshake: false,
                destroyed: true,
                pendingDelegations: 0,
                pendingTimers: 0,
              });

              Cadenza.log("Shutting down socket client", { url, serviceName });

              emitter(`meta.fetch.handshake_requested:${fetchId}`, {
                serviceInstanceId,
                serviceName,
                communicationTypes,
                serviceAddress,
                servicePort: normalizedPort,
                protocol,
                handshakeData: {
                  instanceId: Cadenza.serviceRegistry.serviceInstanceId,
                  serviceName: Cadenza.serviceRegistry.serviceName,
                },
              });

              for (const id of runtimeHandle.pendingDelegationIds) {
                emitter(`meta.socket_client.delegated:${id}`, {
                  errored: true,
                  __error: "Shutting down socket client",
                });
              }

              this.destroySocketClientRuntimeHandle(runtimeHandle);
              emitter("meta.socket_client.runtime_clear_requested", {
                fetchId,
              });
            },
            "Shuts down the socket client",
          )
            .doOn(
              `meta.socket_shutdown_requested:${fetchId}`,
              `meta.socket_client.disconnected:${fetchId}`,
              `meta.fetch.handshake_failed:${fetchId}`,
              `meta.socket_client.connect_error:${fetchId}`,
            )
            .attachSignal("meta.fetch.handshake_requested")
            .emits("meta.socket_client_shutdown_complete");

          return true;
        },
        { mode: "write" },
      ),
      "Connects to a specified socket server and wires runtime tasks.",
    )
      .doOn("meta.fetch.handshake_complete")
      .emitsOnFail("meta.socket_client.connect_failed");
  }

  private resolveSocketServerKey(input: AnyObject): string {
    return (
      String(input.serverKey ?? input.__socketServerKey ?? this.socketServerDefaultKey)
        .trim() || this.socketServerDefaultKey
    );
  }

  private resolveSocketClientFetchId(input: AnyObject): string | undefined {
    const explicitFetchId = String(input.fetchId ?? "").trim();
    if (explicitFetchId) {
      return explicitFetchId;
    }

    const serviceAddress = String(input.serviceAddress ?? "").trim();
    const protocol = String(input.protocol ?? "http").trim();
    const port = this.resolveServicePort(protocol, input.servicePort);

    if (!serviceAddress || !port) {
      return undefined;
    }

    return `${serviceAddress}_${port}`;
  }

  private resolveServicePort(
    protocol: string,
    rawPort: unknown,
  ): number | undefined {
    if (protocol === "https") {
      return 443;
    }

    const parsed = Number(rawPort);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return undefined;
    }

    return Math.trunc(parsed);
  }

  private createSocketServerRuntimeHandleFromContext(
    context: AnyObject,
  ): SocketServerRuntimeHandle {
    const baseServer = context.httpsServer ?? context.httpServer;
    if (!baseServer) {
      throw new Error(
        "Socket server runtime setup requires either httpsServer or httpServer",
      );
    }

    const server = new Server(baseServer, {
      pingInterval: 30_000,
      pingTimeout: 20_000,
      maxHttpBufferSize: 1e7,
      connectionStateRecovery: {
        maxDisconnectionDuration: 2 * 60 * 1000,
        skipMiddlewares: true,
      },
    });

    return {
      server,
      initialized: false,
      connectedSocketIds: new Set<string>(),
      broadcastStatusTask: null,
      shutdownTask: null,
    };
  }

  private destroySocketServerRuntimeHandle(
    runtimeHandle: SocketServerRuntimeHandle | null,
  ): void {
    if (!runtimeHandle) {
      return;
    }

    runtimeHandle.broadcastStatusTask?.destroy();
    runtimeHandle.shutdownTask?.destroy();
    runtimeHandle.broadcastStatusTask = null;
    runtimeHandle.shutdownTask = null;
    runtimeHandle.connectedSocketIds.clear();
    runtimeHandle.initialized = false;
    runtimeHandle.server.close();
    runtimeHandle.server.removeAllListeners();
  }

  private createSocketClientRuntimeHandle(url: string): SocketClientRuntimeHandle {
    return {
      url,
      socket: io(url, {
        reconnection: true,
        reconnectionAttempts: 5,
        reconnectionDelay: 2000,
        reconnectionDelayMax: 10000,
        randomizationFactor: 0.5,
        transports: ["websocket"],
        autoConnect: false,
      }),
      initialized: false,
      handshake: false,
      errorCount: 0,
      pendingDelegationIds: new Set<string>(),
      pendingTimers: new Set<NodeJS.Timeout>(),
      emitWhenReady: null,
      handshakeTask: null,
      delegateTask: null,
      transmitTask: null,
    };
  }

  private destroySocketClientRuntimeHandle(
    runtimeHandle: SocketClientRuntimeHandle | null,
  ): void {
    if (!runtimeHandle) {
      return;
    }

    runtimeHandle.initialized = false;
    runtimeHandle.handshake = false;
    runtimeHandle.emitWhenReady = null;

    runtimeHandle.handshakeTask?.destroy();
    runtimeHandle.delegateTask?.destroy();
    runtimeHandle.transmitTask?.destroy();

    runtimeHandle.handshakeTask = null;
    runtimeHandle.delegateTask = null;
    runtimeHandle.transmitTask = null;

    for (const timer of runtimeHandle.pendingTimers) {
      clearTimeout(timer);
    }

    runtimeHandle.pendingTimers.clear();
    runtimeHandle.pendingDelegationIds.clear();

    runtimeHandle.socket.close();
    runtimeHandle.socket.removeAllListeners();
  }

  private normalizeCommunicationTypes(value: unknown): string[] {
    if (!Array.isArray(value)) {
      return [];
    }

    return value
      .map((item) => String(item))
      .filter((item) => item.trim().length > 0);
  }

  private getErrorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }

    if (typeof error === "string") {
      return error;
    }

    try {
      return JSON.stringify(error);
    } catch {
      return String(error);
    }
  }

  private pruneDiagnosticsEntries(
    entries: Record<string, SocketClientDiagnosticsState>,
    now = Date.now(),
  ): void {
    for (const [fetchId, state] of Object.entries(entries)) {
      if (state.destroyed && now - state.updatedAt > this.destroyedDiagnosticsTtlMs) {
        delete entries[fetchId];
      }
    }

    if (Object.keys(entries).length <= this.diagnosticsMaxClientEntries) {
      return;
    }

    const entriesByEvictionPriority = Object.entries(entries).sort((left, right) => {
      if (left[1].destroyed !== right[1].destroyed) {
        return left[1].destroyed ? -1 : 1;
      }

      return left[1].updatedAt - right[1].updatedAt;
    });

    while (
      Object.keys(entries).length > this.diagnosticsMaxClientEntries &&
      entriesByEvictionPriority.length > 0
    ) {
      const [fetchId] = entriesByEvictionPriority.shift()!;
      delete entries[fetchId];
    }
  }

  public async getSocketClientDiagnosticsEntry(
    fetchId: string,
  ): Promise<SocketClientDiagnosticsState | undefined> {
    const normalized = String(fetchId ?? "").trim();
    if (!normalized) {
      return undefined;
    }

    const snapshot = this.socketClientDiagnosticsActor.getState();
    const entries = { ...snapshot.entries };
    this.pruneDiagnosticsEntries(entries);
    return entries[normalized];
  }

  private resolveTransportDiagnosticsOptions(ctx: AnyObject): {
    detailLevel: TransportDetailLevel;
    includeErrorHistory: boolean;
    errorHistoryLimit: number;
  } {
    const detailLevel: TransportDetailLevel =
      ctx.detailLevel === "full" ? "full" : "summary";
    const includeErrorHistory = Boolean(ctx.includeErrorHistory);

    const requestedLimit = Number(ctx.errorHistoryLimit);
    const errorHistoryLimit = Number.isFinite(requestedLimit)
      ? Math.max(1, Math.min(200, Math.trunc(requestedLimit)))
      : 10;

    return {
      detailLevel,
      includeErrorHistory,
      errorHistoryLimit,
    };
  }

  private collectSocketTransportDiagnostics(
    ctx: AnyObject,
    diagnosticsEntries: Record<string, SocketClientDiagnosticsState>,
  ): AnyObject {
    const { detailLevel, includeErrorHistory, errorHistoryLimit } =
      this.resolveTransportDiagnosticsOptions(ctx);
    const serviceName = Cadenza.serviceRegistry.serviceName ?? "UnknownService";

    const entries = { ...diagnosticsEntries };
    this.pruneDiagnosticsEntries(entries);

    const states = Object.values(entries).sort((a, b) =>
      a.fetchId.localeCompare(b.fetchId),
    );

    const summary = {
      detailLevel,
      totalClients: states.length,
      connectedClients: states.filter((state) => state.connected).length,
      activeHandshakes: states.filter((state) => state.handshake).length,
      pendingDelegations: states.reduce(
        (acc, state) => acc + state.pendingDelegations,
        0,
      ),
      pendingTimers: states.reduce((acc, state) => acc + state.pendingTimers, 0),
      reconnectAttempts: states.reduce(
        (acc, state) => acc + state.reconnectAttempts,
        0,
      ),
      connectErrors: states.reduce((acc, state) => acc + state.connectErrors, 0),
      reconnectErrors: states.reduce((acc, state) => acc + state.reconnectErrors, 0),
      socketErrors: states.reduce((acc, state) => acc + state.socketErrors, 0),
      latestError:
        states
          .slice()
          .sort((a, b) => b.lastErrorAt - a.lastErrorAt)
          .find((state) => state.lastError)?.lastError ?? null,
    };

    if (detailLevel === "summary") {
      return {
        transportDiagnostics: {
          [serviceName]: {
            socketClient: summary,
          },
        },
      };
    }

    const clients = states.map((state) => {
      const details: AnyObject = {
        fetchId: state.fetchId,
        serviceName: state.serviceName,
        url: state.url,
        socketId: state.socketId,
        connected: state.connected,
        handshake: state.handshake,
        reconnectAttempts: state.reconnectAttempts,
        connectErrors: state.connectErrors,
        reconnectErrors: state.reconnectErrors,
        socketErrors: state.socketErrors,
        pendingDelegations: state.pendingDelegations,
        pendingTimers: state.pendingTimers,
        destroyed: state.destroyed,
        lastHandshakeAt: state.lastHandshakeAt,
        lastHandshakeError: state.lastHandshakeError,
        lastDisconnectAt: state.lastDisconnectAt,
        latestError: state.lastError,
      };

      if (includeErrorHistory) {
        details.errorHistory = state.errorHistory.slice(-errorHistoryLimit);
      }

      return details;
    });

    return {
      transportDiagnostics: {
        [serviceName]: {
          socketClient: {
            ...summary,
            clients,
          },
        },
      },
    };
  }
}
