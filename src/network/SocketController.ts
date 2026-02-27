import Cadenza from "../Cadenza";
import { Server } from "socket.io";
import { IRateLimiterOptions, RateLimiterMemory } from "rate-limiter-flexible";
import xss from "xss";
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

/**
 * The `SocketController` class handles the setup and management of a WebSocket server,
 * ensuring secure connections, message handling, and rate-limiting.
 * This class is designed to function as a singleton, providing a unified interface
 * for WebSocket interactions in the application by standardizing the server setup
 * and integrating task-based processing through the `Cadenza` framework.
 */
export default class SocketController {
  private static _instance: SocketController;
  public static get instance(): SocketController {
    if (!this._instance) this._instance = new SocketController();
    return this._instance;
  }

  private socketClientDiagnostics: Map<string, SocketClientDiagnosticsState> =
    new Map();
  private readonly diagnosticsErrorHistoryLimit = 100;
  private readonly diagnosticsMaxClientEntries = 500;
  private readonly destroyedDiagnosticsTtlMs = 15 * 60_000;

  private pruneSocketClientDiagnostics(now = Date.now()): void {
    for (const [fetchId, state] of this.socketClientDiagnostics.entries()) {
      if (state.destroyed && now - state.updatedAt > this.destroyedDiagnosticsTtlMs) {
        this.socketClientDiagnostics.delete(fetchId);
      }
    }

    if (this.socketClientDiagnostics.size <= this.diagnosticsMaxClientEntries) {
      return;
    }

    const entriesByEvictionPriority = Array.from(
      this.socketClientDiagnostics.entries(),
    ).sort((left, right) => {
      if (left[1].destroyed !== right[1].destroyed) {
        return left[1].destroyed ? -1 : 1;
      }

      return left[1].updatedAt - right[1].updatedAt;
    });

    while (
      this.socketClientDiagnostics.size > this.diagnosticsMaxClientEntries &&
      entriesByEvictionPriority.length > 0
    ) {
      const [fetchId] = entriesByEvictionPriority.shift()!;
      this.socketClientDiagnostics.delete(fetchId);
    }
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

  private ensureSocketClientDiagnostics(
    fetchId: string,
    serviceName: string,
    url: string,
  ): SocketClientDiagnosticsState {
    const now = Date.now();
    this.pruneSocketClientDiagnostics(now);

    let state = this.socketClientDiagnostics.get(fetchId);
    if (!state) {
      state = {
        fetchId,
        serviceName,
        url,
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
      this.socketClientDiagnostics.set(fetchId, state);
    } else {
      state.serviceName = serviceName;
      state.url = url;
    }

    this.pruneSocketClientDiagnostics(now);
    return state;
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

  private recordSocketClientError(
    fetchId: string,
    serviceName: string,
    url: string,
    error: unknown,
  ): void {
    const state = this.ensureSocketClientDiagnostics(fetchId, serviceName, url);
    const message = this.getErrorMessage(error);
    const now = Date.now();

    state.lastError = message;
    state.lastErrorAt = now;
    state.updatedAt = now;
    state.errorHistory.push({
      at: new Date(now).toISOString(),
      message,
    });

    if (state.errorHistory.length > this.diagnosticsErrorHistoryLimit) {
      state.errorHistory.splice(
        0,
        state.errorHistory.length - this.diagnosticsErrorHistoryLimit,
      );
    }
  }

  private collectSocketTransportDiagnostics(ctx: AnyObject): AnyObject {
    this.pruneSocketClientDiagnostics();
    const { detailLevel, includeErrorHistory, errorHistoryLimit } =
      this.resolveTransportDiagnosticsOptions(ctx);
    const serviceName = Cadenza.serviceRegistry.serviceName ?? "UnknownService";
    const states = Array.from(this.socketClientDiagnostics.values()).sort(
      (a, b) => a.fetchId.localeCompare(b.fetchId),
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
      reconnectErrors: states.reduce(
        (acc, state) => acc + state.reconnectErrors,
        0,
      ),
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

  /**
   * Constructs the `SocketServer`, setting up a WebSocket server with specific configurations,
   * including connection state recovery, rate limiting, CORS handling, and custom event handling.
   * This class sets up the communication infrastructure for scalable, resilient, and secure WebSocket-based interactions
   * using metadata-driven task execution with `Cadenza`.
   *
   * It provides support for:
   * - Origin-based access control for connections.
   * - Optional payload sanitization.
   * - Configurable rate limiting and behavior on limit breaches (soft/hard disconnects).
   * - Event handlers for connection, handshake, delegation, signaling, status checks, and disconnection.
   *
   * The server can handle both internal and external interactions depending on the provided configurations,
   * and integrates directly with Cadenza's task workflow engine.
   *
   * Initializes the `SocketServer` to be ready for WebSocket communication.
   */
  constructor() {
    Cadenza.createMetaTask(
      "Collect socket transport diagnostics",
      (ctx) => this.collectSocketTransportDiagnostics(ctx),
      "Responds to distributed transport diagnostics inquiries with socket client data.",
    ).respondsTo(META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT);

    Cadenza.createMetaRoutine(
      "SocketServer",
      [
        Cadenza.createMetaTask("Setup SocketServer", (ctx) => {
          if (!ctx.__useSocket) {
            return;
          }

          const server = new Server(ctx.httpsServer ?? ctx.httpServer, {
            pingInterval: 30_000,
            pingTimeout: 20_000,
            maxHttpBufferSize: 1e7, // 10MB large payloads
            connectionStateRecovery: {
              maxDisconnectionDuration: 2 * 60 * 1000, // 2min
              skipMiddlewares: true, // Optional: bypass rate limiter on recover
            },
          });

          const profile = ctx.__securityProfile ?? "medium";

          server.use((socket, next) => {
            // Origin check (CORS-like)
            const origin = socket?.handshake?.headers?.origin;
            const allowedOrigins = ["*"]; // TODO From firewall_rule
            const networkType = ctx.__networkType ?? "internal"; // From meta-config
            let effectiveOrigin = origin || "unknown";
            if (networkType === "internal") effectiveOrigin = "internal"; // Assume trusted internal

            if (
              profile !== "low" &&
              !allowedOrigins.includes(effectiveOrigin) &&
              !allowedOrigins.includes("*")
            ) {
              return next(new Error("Unauthorized origin"));
            }

            // Rate limiting per socket/IP
            const limiterOptions: { [key: string]: IRateLimiterOptions } = {
              low: { points: Infinity, duration: 1 },
              medium: { points: 10000, duration: 10 },
              high: { points: 1000, duration: 60, blockDuration: 300 },
            };
            const limiter = new RateLimiterMemory(limiterOptions[profile]);
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

            // Sanitization for payloads needed?
            // socket.use((packet, next) => {
            //   if (profile !== "low") {
            //     const sanitize = (data: any) => {
            //       if (typeof data === "string") return xss(data);
            //       if (typeof data === "object") {
            //         for (const key in data) {
            //           data[key] = sanitize(data[key]);
            //         }
            //       }
            //       return data;
            //     };
            //     try {
            //       packet[1] = sanitize(packet[1]); // Sanitize event payload
            //     } catch (e) {
            //       console.error("SocketServer: Sanitization error", e);
            //     }
            //   }
            //   next();
            // });
            next();
          });

          if (!server) {
            Cadenza.log("Socket setup error: No server", {}, "error");
            return { ...ctx, __error: "No server", errored: true };
          }

          server.on("connection", (ws: any) => {
            try {
              ws.on(
                "handshake",
                (ctx: AnyObject, callback: (result: any) => void) => {
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
                      (ctx, emit) => {
                        if (ctx.__signalName === undefined) {
                          return;
                        }

                        ws.emit("signal", ctx);

                        if (ctx.__routineExecId) {
                          emit(
                            `meta.socket_client.transmitted:${ctx.__routineExecId}`,
                            {},
                          );
                        }
                      },
                    )
                      .doOn(
                        `meta.service_registry.selected_instance_for_socket:${fetchId}`,
                      )
                      .attachSignal("meta.socket_client.transmitted");
                  }

                  Cadenza.emit("meta.socket.handshake", ctx);
                },
              );

              ws.on(
                "delegation",
                (ctx: AnyObject, callback: (ctx: AnyObject) => any) => {
                  const deputyExecId = ctx.__metadata.__deputyExecId;

                  Cadenza.createEphemeralMetaTask(
                    "Resolve delegation",
                    (ctx: AnyObject) => {
                      callback(ctx);
                    },
                    "Resolves a delegation request using the provided callback from the client (.emitWithAck())",
                    { register: false },
                  )
                    .doOn(`meta.node.graph_completed:${deputyExecId}`)
                    .emits(`meta.socket.delegation_resolved:${deputyExecId}`);

                  Cadenza.createEphemeralMetaTask(
                    "Delegation progress update",
                    (ctx) => {
                      if (ctx.__progress !== undefined)
                        ws.emit("delegation_progress", ctx);
                    },
                    "Updates delegation progress",
                    {
                      once: false,
                      destroyCondition: (ctx: AnyObject) =>
                        ctx.data.progress === 1.0 ||
                        ctx.data?.progress === undefined,
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
                },
              );

              ws.on(
                "signal",
                (ctx: AnyObject, callback: (ctx: AnyObject) => any) => {
                  if (
                    Cadenza.signalBroker
                      .listObservedSignals()
                      .includes(ctx.__signalName)
                  ) {
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
                },
              );

              ws.on(
                "status_check",
                (ctx: AnyObject, callback: (ctx: AnyObject) => any) => {
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
                Cadenza.log(
                  "Socket client disconnected",
                  { socketId: ws.id },
                  "warning",
                );
                Cadenza.emit("meta.socket.disconnected", {
                  __wsId: ws.id,
                });
              });
            } catch (e) {
              Cadenza.log(
                "SocketServer: Error in socket event",
                { error: e },
                "error",
              );
            }

            Cadenza.emit("meta.socket.connected", { __wsId: ws.id });
          });

          Cadenza.createMetaTask(
            "Broadcast status",
            (ctx) => server.emit("status_update", ctx),
            "Broadcasts the status of the server to all clients",
          ).doOn("meta.service.updated");

          Cadenza.createMetaTask(
            "Shutdown SocketServer",
            () => server.close(),
            "Shuts down the socket server",
          )
            .doOn("meta.socket_server_shutdown_requested")
            .emits("meta.socket.shutdown");

          return ctx;
        }),
      ],
      "Bootstraps the socket server",
    ).doOn("global.meta.rest.network_configured");

    Cadenza.createMetaTask(
      "Connect to socket server",
      (ctx) => {
        const {
          serviceInstanceId,
          communicationTypes,
          serviceName,
          serviceAddress,
          servicePort,
          protocol,
        } = ctx;

        const socketProtocol = protocol === "https" ? "wss" : "ws";
        const port = protocol === "https" ? 443 : servicePort;
        const URL = `${socketProtocol}://${serviceAddress}:${port}`;
        const fetchId = `${serviceAddress}_${port}`;
        const socketDiagnostics = this.ensureSocketClientDiagnostics(
          fetchId,
          serviceName,
          URL,
        );
        socketDiagnostics.destroyed = false;
        socketDiagnostics.updatedAt = Date.now();
        let handshake = false;
        let errorCount = 0;
        const ERROR_LIMIT = 5;

        if (Cadenza.get(`Socket handshake with ${URL}`)) {
          console.error("Socket client already exists", URL);
          return;
        }

        const pendingDelegationIds = new Set<string>();
        const pendingTimers = new Set<NodeJS.Timeout>();
        const syncPendingCounts = () => {
          socketDiagnostics.pendingDelegations = pendingDelegationIds.size;
          socketDiagnostics.pendingTimers = pendingTimers.size;
          socketDiagnostics.updatedAt = Date.now();
        };

        let handshakeTask: Task | null = null;
        let emitWhenReady:
          | (<T>(
              event: string,
              data: any,
              timeoutMs: number,
              ack?: (response: T) => void,
            ) => Promise<T>)
          | null = null;
        let transmitTask: Task | null = null;
        let delegateTask: Task | null = null;
        let socket: Socket | null = null;

        socket = io(URL, {
          reconnection: true,
          reconnectionAttempts: 5,
          reconnectionDelay: 2000,
          reconnectionDelayMax: 10000,
          randomizationFactor: 0.5,
          transports: ["websocket"],
          autoConnect: false,
        });

        emitWhenReady = <T>(
          event: string,
          data: any,
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
              pendingTimers.delete(timer);
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
            const resolveWithError = (
              errorMessage: string,
              fallbackError?: unknown,
            ) => {
              settle({
                ...data,
                errored: true,
                __error: errorMessage,
                error:
                  fallbackError instanceof Error
                    ? fallbackError.message
                    : errorMessage,
                socketId: socket?.id,
                serviceName,
                URL,
              } as T);
            };

            const tryEmit = async () => {
              const waitTimeoutMs = normalizedTimeoutMs + 10;
              const waitResult = await waitForSocketConnection(
                socket,
                waitTimeoutMs,
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
                  { socketId: socket?.id, serviceName, URL, event },
                  "error",
                );
                this.recordSocketClientError(
                  fetchId,
                  serviceName,
                  URL,
                  waitResult.error,
                );
                resolveWithError(waitResult.error);
                return;
              }

              timer = setTimeout(() => {
                if (settled) {
                  return;
                }
                clearPendingTimer();
                Cadenza.log(
                  `Socket event '${event}' timed out`,
                  { socketId: socket?.id, serviceName, URL },
                  "error",
                );
                this.recordSocketClientError(
                  fetchId,
                  serviceName,
                  URL,
                  `Socket event '${event}' timed out`,
                );
                resolveWithError(`Socket event '${event}' timed out`);
              }, normalizedTimeoutMs + 10);
              pendingTimers.add(timer);
              syncPendingCounts();

              const connectedSocket = socket;
              if (!connectedSocket) {
                resolveWithError(
                  `Socket unavailable before emitting '${event}'`,
                );
                return;
              }

              connectedSocket
                .timeout(normalizedTimeoutMs)
                .emit(event, data, (err: any, response: T) => {
                  if (err) {
                    Cadenza.log(
                      "Socket timeout.",
                      {
                        event,
                        error: err.message,
                        socketId: socket?.id,
                        serviceName,
                      },
                      "warning",
                    );
                    this.recordSocketClientError(
                      fetchId,
                      serviceName,
                      URL,
                      err,
                    );
                    response = {
                      __error: `Timeout error: ${err}`,
                      errored: true,
                      ...ctx,
                      ...ctx.__metadata,
                    };
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
                  socketId: socket?.id,
                  serviceName,
                  URL,
                },
                "error",
              );
              this.recordSocketClientError(fetchId, serviceName, URL, error);
              resolveWithError(`Socket event '${event}' failed`, error);
            });
          });
        };

        socket.on("connect", () => {
          if (handshake) return;
          socketDiagnostics.connected = true;
          socketDiagnostics.destroyed = false;
          socketDiagnostics.socketId = socket?.id ?? null;
          socketDiagnostics.updatedAt = Date.now();
          Cadenza.emit(`meta.socket_client.connected:${fetchId}`, ctx);
        });

        socket.on("delegation_progress", (ctx) => {
          Cadenza.emit(
            `meta.socket_client.delegation_progress:${ctx.__metadata.__deputyExecId}`,
            ctx,
          );
        });

        socket.on("signal", (ctx) => {
          if (
            Cadenza.signalBroker
              .listObservedSignals()
              .includes(ctx.__signalName)
          ) {
            Cadenza.emit(ctx.__signalName, ctx);
          }
        });

        socket.on("status_update", (status) => {
          Cadenza.emit("meta.socket_client.status_received", status);
        });

        socket.on("connect_error", (err) => {
          handshake = false;
          socketDiagnostics.connected = false;
          socketDiagnostics.handshake = false;
          socketDiagnostics.connectErrors++;
          socketDiagnostics.lastHandshakeError = err.message;
          socketDiagnostics.updatedAt = Date.now();
          this.recordSocketClientError(fetchId, serviceName, URL, err);
          Cadenza.log(
            "Socket connect error",
            { error: err.message, serviceName, socketId: socket?.id, URL },
            "error",
          );
          Cadenza.emit(`meta.socket_client.connect_error:${fetchId}`, err);
        });

        socket.on("reconnect_attempt", (attempt) => {
          socketDiagnostics.reconnectAttempts = Math.max(
            socketDiagnostics.reconnectAttempts,
            attempt,
          );
          socketDiagnostics.updatedAt = Date.now();
          Cadenza.log(`Reconnect attempt: ${attempt}`);
        });

        socket.on("reconnect", (attempt) => {
          socketDiagnostics.connected = true;
          socketDiagnostics.updatedAt = Date.now();
          Cadenza.log(`Socket reconnected after ${attempt} tries`, {
            socketId: socket?.id,
            URL,
            serviceName,
          });
        });

        socket.on("reconnect_error", (err) => {
          handshake = false;
          socketDiagnostics.connected = false;
          socketDiagnostics.handshake = false;
          socketDiagnostics.reconnectErrors++;
          socketDiagnostics.lastHandshakeError = err.message;
          socketDiagnostics.updatedAt = Date.now();
          this.recordSocketClientError(fetchId, serviceName, URL, err);
          Cadenza.log(
            "Socket reconnect failed.",
            { error: err.message, serviceName, URL, socketId: socket?.id },
            "warning",
          );
        });

        socket.on("error", (err) => {
          // TODO: Retry on rate limit error
          errorCount++;
          socketDiagnostics.socketErrors++;
          socketDiagnostics.updatedAt = Date.now();
          this.recordSocketClientError(fetchId, serviceName, URL, err);

          Cadenza.log(
            "Socket error",
            { error: err, socketId: socket?.id, URL, serviceName },
            "error",
          );
          Cadenza.emit("meta.socket_client.error", err);
        });

        socket.on("disconnect", () => {
          const disconnectedAt = new Date().toISOString();
          socketDiagnostics.connected = false;
          socketDiagnostics.handshake = false;
          socketDiagnostics.lastDisconnectAt = disconnectedAt;
          socketDiagnostics.updatedAt = Date.now();
          Cadenza.log(
            "Socket disconnected.",
            { URL, serviceName, socketId: socket?.id },
            "warning",
          );
          Cadenza.emit(`meta.socket_client.disconnected:${fetchId}`, {
            serviceName,
            serviceAddress,
            servicePort,
          });
          handshake = false;
        });

        socket.connect();

        handshakeTask = Cadenza.createMetaTask(
          `Socket handshake with ${URL}`,
          async (ctx, emit) => {
            if (handshake) return;
            handshake = true;
            socketDiagnostics.handshake = true;
            socketDiagnostics.updatedAt = Date.now();

            await emitWhenReady?.(
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
                  socketDiagnostics.connected = true;
                  socketDiagnostics.handshake = true;
                  socketDiagnostics.lastHandshakeAt = new Date().toISOString();
                  socketDiagnostics.lastHandshakeError = null;
                  socketDiagnostics.updatedAt = Date.now();
                  Cadenza.log("Socket client connected", {
                    result,
                    serviceName,
                    socketId: socket?.id,
                    URL,
                  });
                } else {
                  socketDiagnostics.connected = false;
                  socketDiagnostics.handshake = false;
                  socketDiagnostics.lastHandshakeError =
                    result?.__error ?? result?.error ?? "Socket handshake failed";
                  socketDiagnostics.updatedAt = Date.now();
                  this.recordSocketClientError(
                    fetchId,
                    serviceName,
                    URL,
                    socketDiagnostics.lastHandshakeError,
                  );
                  Cadenza.log(
                    "Socket handshake failed",
                    { result, serviceName, socketId: socket?.id, URL },
                    "warning",
                  );
                }

                // if (result.errored) {
                //   errorCount++;
                //   if (errorCount > ERROR_LIMIT) {
                //     console.error("Too many errors, closing socket", URL);
                //     emit(`meta.socket_shutdown_requested:${fetchId}`, {});
                //   }
                // }
              },
            );
          },
          "Handshakes with socket server",
        ).doOn(`meta.socket_client.connected:${fetchId}`);

        delegateTask = Cadenza.createMetaTask(
          `Delegate flow to Socket service ${URL}`,
          async (ctx, emit) => {
            if (ctx.__remoteRoutineName === undefined) {
              return;
            }

            delete ctx.__isSubMeta;
            delete ctx.__broadcast;

            const deputyExecId = ctx.__metadata?.__deputyExecId;
            const requestSentAt = Date.now();
            if (deputyExecId) {
              pendingDelegationIds.add(deputyExecId);
              syncPendingCounts();
            }

            try {
              const resultContext =
                ((await emitWhenReady?.(
                  "delegation",
                  ctx,
                  ctx.__timeout ?? 60_000,
                )) as AnyObject | undefined) ??
                ({
                  errored: true,
                  __error: "Socket delegation returned no response",
                } as AnyObject);

              const requestDuration = Date.now() - requestSentAt;
              const metadata = resultContext.__metadata;
              delete resultContext.__metadata;

              if (deputyExecId) {
                emit(`meta.socket_client.delegated:${deputyExecId}`, {
                  ...resultContext,
                  ...metadata,
                  __requestDuration: requestDuration,
                });
              }

              if (resultContext?.errored || resultContext?.failed) {
                this.recordSocketClientError(
                  fetchId,
                  serviceName,
                  URL,
                  resultContext?.__error ??
                    resultContext?.error ??
                    "Socket delegation failed",
                );
              }

              return resultContext;
            } catch (error) {
              const message =
                error instanceof Error ? error.message : String(error);
              const failedContext = {
                errored: true,
                __error: message,
              };

              if (deputyExecId) {
                emit(`meta.socket_client.delegated:${deputyExecId}`, {
                  ...failedContext,
                  __requestDuration: Date.now() - requestSentAt,
                });
              }

              this.recordSocketClientError(fetchId, serviceName, URL, error);
              return failedContext;
            } finally {
              if (deputyExecId) {
                pendingDelegationIds.delete(deputyExecId);
                syncPendingCounts();
              }
            }
          },
          `Delegate flow to service ${serviceName} with address ${URL}`,
        )
          .doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`)
          .attachSignal(
            "meta.socket_client.delegated",
            "meta.socket_shutdown_requested",
          );

        transmitTask = Cadenza.createMetaTask(
          `Transmit signal to socket server ${URL}`,
          async (ctx, emit) => {
            if (ctx.__signalName === undefined) {
              return;
            }

            delete ctx.__broadcast;

            const response =
              ((await emitWhenReady?.("signal", ctx, 5_000)) as
                | AnyObject
                | undefined) ??
              ({
                errored: true,
                __error: "Socket signal transmission returned no response",
              } as AnyObject);

            if (ctx.__routineExecId) {
              emit(`meta.socket_client.transmitted:${ctx.__routineExecId}`, {
                ...response,
              });
            }

            return response;
          },
          `Transmits signal to service ${serviceName} with address ${URL}`,
        )
          .doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`)
          .attachSignal("meta.socket_client.transmitted");

        Cadenza.createEphemeralMetaTask(
          `Shutdown SocketClient ${URL}`,
          (ctx, emit) => {
            handshake = false;
            socketDiagnostics.connected = false;
            socketDiagnostics.handshake = false;
            socketDiagnostics.destroyed = true;
            socketDiagnostics.updatedAt = Date.now();
            Cadenza.log("Shutting down socket client", { URL, serviceName });
            socket?.close();
            handshakeTask?.destroy();
            delegateTask?.destroy();
            transmitTask?.destroy();
            handshakeTask = null;
            delegateTask = null;
            transmitTask = null;
            emitWhenReady = null;
            socket = null;
            emit(`meta.fetch.handshake_requested:${fetchId}`, {
              serviceInstanceId,
              serviceName,
              communicationTypes,
              serviceAddress,
              servicePort,
              protocol,
              handshakeData: {
                instanceId: Cadenza.serviceRegistry.serviceInstanceId,
                serviceName: Cadenza.serviceRegistry.serviceName,
              },
            });

            for (const id of pendingDelegationIds) {
              emit(`meta.socket_client.delegated:${id}`, {
                errored: true,
                __error: "Shutting down socket client",
              });
            }

            pendingDelegationIds.clear();
            syncPendingCounts();

            for (const timer of pendingTimers) {
              clearTimeout(timer);
            }

            pendingTimers.clear();
            syncPendingCounts();
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
      "Connects to a specified socket server",
    )
      .doOn("meta.fetch.handshake_complete")
      .emitsOnFail("meta.socket_client.connect_failed");
  }
}
