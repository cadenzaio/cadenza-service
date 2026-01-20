import Cadenza from "../Cadenza";
import { Server } from "socket.io";
import { IRateLimiterOptions, RateLimiterMemory } from "rate-limiter-flexible";
import xss from "xss";
import type { AnyObject } from "@cadenza.io/core";
import { io } from "socket.io-client";
import { isBrowser } from "../utils/environment";

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

          const handshakeMap: { [key: string]: boolean } = {};

          server.on("connection", (ws: any) => {
            try {
              ws.on(
                "handshake",
                (ctx: AnyObject, callback: (result: any) => void) => {
                  if (handshakeMap[ctx.serviceInstanceId]) {
                    callback({
                      status: "error",
                      error: "Duplicate handshake",
                    });
                    return;
                  }

                  handshakeMap[ctx.serviceInstanceId] = true;
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
        let handshake = false;

        if (Cadenza.get(`Socket handshake with ${URL}`)) {
          console.error("Socket client already exists", URL);
          return;
        }

        const socket = io(URL, {
          reconnection: true,
          reconnectionAttempts: 5,
          reconnectionDelay: 2000,
          reconnectionDelayMax: 10000,
          randomizationFactor: 0.5,
          transports: ["websocket"],
          autoConnect: false,
        });

        const emitWhenReady = <T>(
          event: string,
          data: any,
          timeoutMs: number = 20_000,
          ack?: (response: T) => void,
        ): Promise<T> => {
          return new Promise((resolve) => {
            const tryEmit = () => {
              if (!socket.connected) {
                // should never happen because we await connect below, but safety net
                socket.once("connect", tryEmit);
                return;
              }

              let timer: any;
              if (timeoutMs !== 0) {
                timer = setTimeout(() => {
                  Cadenza.log(
                    `Socket event '${event}' timed out`,
                    { ...data, socketId: socket.id, serviceName, URL },
                    "error",
                  );
                  resolve({
                    ...data,
                    errored: true,
                    __error: `Socket event '${event}' timed out`,
                    error: `Socket event '${event}' timed out`,
                    socketId: socket.id,
                    serviceName,
                    URL,
                  });
                }, timeoutMs);
              }

              socket
                .timeout(timeoutMs)
                .emit(event, data, (err: any, response: T) => {
                  if (err) {
                    Cadenza.log(
                      "Socket timeout.",
                      { error: err, socketId: socket.id, serviceName },
                      "warning",
                    );
                    response = {
                      __error: `Timeout error: ${err}`,
                      errored: true,
                      ...ctx,
                      ...ctx.__metadata,
                    };
                    resolve(response);
                    return;
                  }
                  if (timer) clearTimeout(timer);
                  if (ack) ack(response);
                  resolve(response);
                });
            };

            if (socket.connected) {
              tryEmit();
            } else {
              socket.once("connect", tryEmit);
            }
          });
        };

        socket.on("connect", () => {
          if (handshake) return;
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
          Cadenza.log(
            "Socket connect error",
            { error: err.message, serviceName, socketId: socket.id, URL },
            "error",
          );
          Cadenza.emit(`meta.socket_client.connect_error:${fetchId}`, err);
        });

        socket.on("reconnect_attempt", (attempt) => {
          Cadenza.log(`Reconnect attempt: ${attempt}`);
        });

        socket.on("reconnect", (attempt) => {
          Cadenza.log(`Socket reconnected after ${attempt} tries`, {
            socketId: socket.id,
            URL,
            serviceName,
          });
        });

        socket.on("reconnect_error", (err) => {
          Cadenza.log(
            "Socket reconnect failed.",
            { error: err.message, serviceName, URL, socketId: socket.id },
            "warning",
          );
        });

        socket.on("error", (err) => {
          // TODO: Retry on rate limit error

          Cadenza.log(
            "Socket error",
            { error: err, socketId: socket.id, URL, serviceName },
            "error",
          );
          Cadenza.emit("meta.socket_client.error", err);
        });

        socket.on("disconnect", () => {
          Cadenza.log(
            "Socket disconnected.",
            { URL, serviceName, socketId: socket.id },
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

        const handshakeTask = Cadenza.createMetaTask(
          `Socket handshake with ${URL}`,
          async () => {
            if (handshake) return;
            handshake = true;

            await emitWhenReady(
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
                  Cadenza.log("Socket connected", {
                    result,
                    serviceName,
                    socketId: socket.id,
                    URL,
                  });
                } else {
                  Cadenza.log(
                    "Socket handshake failed",
                    { result, serviceName, socketId: socket.id, URL },
                    "warning",
                  );
                }
              },
            );
          },
          "Handshakes with socket server",
        ).doOn(`meta.socket_client.connected:${fetchId}`);

        const delegateTask = Cadenza.createMetaTask(
          `Delegate flow to Socket service ${URL}`,
          async (ctx, emit) => {
            if (ctx.__remoteRoutineName === undefined) {
              return;
            }

            return new Promise((resolve) => {
              delete ctx.__isSubMeta;
              delete ctx.__broadcast;
              const requestSentAt = Date.now();
              emitWhenReady(
                "delegation",
                ctx,
                20_000,
                (resultContext: AnyObject) => {
                  const requestDuration = Date.now() - requestSentAt;
                  const metadata = resultContext.__metadata;
                  delete resultContext.__metadata;
                  emit(
                    `meta.socket_client.delegated:${ctx.__metadata.__deputyExecId}`,
                    {
                      ...resultContext,
                      ...metadata,
                      __requestDuration: requestDuration,
                    },
                  );
                  resolve(resultContext);
                },
              );
            });
          },
          `Delegate flow to service ${serviceName} with address ${URL}`,
        )
          .doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`)
          .attachSignal("meta.socket_client.delegated");

        const transmitTask = Cadenza.createMetaTask(
          `Transmit signal to socket server ${URL}`,
          async (ctx, emit) => {
            if (ctx.__signalName === undefined) {
              return;
            }

            return new Promise((resolve) => {
              delete ctx.__broadcast;

              emitWhenReady("signal", ctx, 10_000, (response: AnyObject) => {
                if (ctx.__routineExecId) {
                  emit(
                    `meta.socket_client.transmitted:${ctx.__routineExecId}`,
                    response,
                  );
                }
                resolve(response);
              });
            });
          },
          `Transmits signal to service ${serviceName} with address ${URL}`,
        )
          .doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`)
          .attachSignal("meta.socket_client.transmitted");

        Cadenza.createEphemeralMetaTask(
          `Shutdown SocketClient ${URL}`,
          (ctx, emit) => {
            Cadenza.log("Shutting down socket client", { URL, serviceName });
            socket?.close();
            handshakeTask.destroy();
            delegateTask.destroy();
            transmitTask.destroy();
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
          },
          "Shuts down the socket client",
        )
          .doOn(
            `meta.socket_shutdown_requested:${fetchId}`,
            `meta.socket_client.disconnected:${fetchId}`,
            `meta.fetch.handshake_failed:${fetchId}`,
            `meta.socket_client.connect_error:${fetchId}`,
          )
          .emits("meta.socket_client_shutdown_complete");

        return true;
      },
      "Connects to a specified socket server",
    )
      .doOn("meta.fetch.handshake_complete")
      .emitsOnFail("meta.socket_client.connect_failed");
  }
}
