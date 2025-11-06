import Cadenza from "../Cadenza";
import { Server } from "socket.io";
import { IRateLimiterOptions, RateLimiterMemory } from "rate-limiter-flexible";
import xss from "xss";
import type { AnyObject } from "@cadenza.io/core";
import { io } from "socket.io-client";
import { isBrowser } from "../utils/environment";

export default class SocketController {
  private static _instance: SocketController;
  public static get instance(): SocketController {
    if (!this._instance) this._instance = new SocketController();
    return this._instance;
  }

  constructor() {
    Cadenza.createMetaRoutine(
      "SocketServer",
      [
        Cadenza.createMetaTask("Setup SocketServer", (ctx) => {
          if (!ctx.__useSocket) {
            return;
          }

          console.log("SocketServer: Setting up");
          const server = new Server(ctx.__httpsServer ?? ctx.__httpServer, {
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
                    console.log(
                      "SocketServer: Rate limit exceeded",
                      rej.msBeforeNext / 1000,
                    );
                    socket.emit("error", {
                      message: "Rate limit exceeded",
                      retryAfter: rej.msBeforeNext / 1000,
                    });
                    packetNext(new Error("Rate limit exceeded"));
                  } else {
                    console.log("SocketServer: Rate limit exceeded, blocked");
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
          console.log("SocketServer: Setup complete");

          if (!server) {
            console.error("Socket setup error: No server");
            return { ...ctx, __error: "No server", errored: true };
          }

          const handshakeMap: { [key: string]: boolean } = {};

          server.on("connection", (ws: any) => {
            console.log("SocketServer: New connection", ws.id);

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
                  console.log("Socket HANDSHAKE", ctx);
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
                    ).doOn(
                      `meta.service_registry.selected_instance_for_socket:${fetchId}`,
                    );
                  }
                  Cadenza.broker.emit("meta.socket.handshake", ctx);
                },
              );

              ws.on(
                "delegation",
                (ctx: AnyObject, callback: (ctx: AnyObject) => any) => {
                  console.log("Received socket delegation request", ctx);
                  const deputyExecId = ctx.__metadata.__deputyExecId;

                  Cadenza.createEphemeralMetaTask(
                    "Resolve delegation",
                    callback,
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

                  Cadenza.broker.emit("meta.socket.delegation_requested", {
                    ...ctx,
                    __name: ctx.__remoteRoutineName,
                  });
                },
              );

              ws.on(
                "signal",
                (ctx: AnyObject, callback: (ctx: AnyObject) => any) => {
                  if (
                    Cadenza.broker
                      .listObservedSignals()
                      .includes(ctx.__signalName)
                  ) {
                    callback({
                      __status: "success",
                      __signalName: ctx.__signalName,
                    });
                    Cadenza.broker.emit(ctx.__signalName, ctx);
                  } else {
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

                  Cadenza.broker.emit(
                    "meta.socket.status_check_requested",
                    ctx,
                  );
                },
              );

              ws.on("disconnect", () => {
                console.log("SocketServer: Disconnected");
                Cadenza.broker.emit("meta.socket.disconnected", {
                  __wsId: ws.id,
                });
              });
            } catch (e) {
              console.error("SocketServer: Error in socket event", e);
            }

            Cadenza.broker.emit("meta.socket.connected", { __wsId: ws.id });
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

          console.log("SocketServer: Startup complete");

          return ctx;
        }),
      ],
      "Bootstraps the socket server",
    ).doOn("meta.rest.network_configured");

    Cadenza.createMetaTask(
      "Connect to socket server",
      (ctx) => {
        const { serviceName, serviceAddress, servicePort, protocol } = ctx;

        const socketProtocol = protocol === "https" ? "wss" : "ws";
        const port = protocol === "https" ? 443 : servicePort;
        const URL = `${socketProtocol}://${serviceAddress}:${port}`;
        const fetchId = `${serviceAddress}_${port}`;
        let handshake = false;

        console.log("SocketClient: Connecting to", serviceName, URL);

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
          return new Promise((resolve, reject) => {
            const tryEmit = () => {
              if (!socket.connected) {
                // should never happen because we await connect below, but safety net
                socket.once("connect", tryEmit);
                return;
              }

              let timer: any;
              if (timeoutMs !== 0) {
                timer = setTimeout(() => {
                  console.error(`${event} timed out`);
                  reject(new Error(`${event} timed out`));
                }, timeoutMs);
              }

              socket
                .timeout(timeoutMs)
                .emit(event, data, (err: any, response: T) => {
                  if (err) {
                    console.log("Timeout error:", err);
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
          console.log("SocketClient: CONNECTED", socket.id);
          if (handshake) return;

          Cadenza.broker.emit(`meta.socket_client.connected:${fetchId}`, ctx);
        });

        socket.on("connect_error", (err) => {
          console.error("Connect error:", err.message);
        });

        socket.on("delegation_progress", (ctx) => {
          Cadenza.broker.emit(
            `meta.socket_client.delegation_progress:${ctx.__metadata.__deputyExecId}`,
            ctx,
          );
        });

        socket.on("signal", (ctx) => {
          if (Cadenza.broker.listObservedSignals().includes(ctx.__signalName)) {
            Cadenza.broker.emit(ctx.__signalName, ctx);
          }
        });

        socket.on("status_update", (status) => {
          Cadenza.broker.emit("meta.socket_client.status_received", status);
        });

        socket.on("connect_error", (err) => {
          console.error("SocketClient: connect_error", err);
          Cadenza.broker.emit("meta.socket_client.connect_error", err);
        });

        socket.on("timeout", (event) => {
          console.error(event, "timed out — server didn’t respond");
        });

        socket.on("reconnect_attempt", (attempt) => {
          console.log("Reconnect attempt:", attempt);
        });

        socket.on("reconnect", (attempt) => {
          console.log("Reconnected after", attempt, "tries");
        });

        socket.on("reconnect_error", (err) => {
          console.error("Reconnect failed:", err.message);
        });

        socket.on("error", (err) => {
          // TODO: Retry on rate limit error

          console.error("SocketClient: error", err);
          Cadenza.broker.emit("meta.socket_client.error", err);
        });

        socket.on("disconnect", () => {
          console.log("SocketClient: Disconnected", URL);
          Cadenza.broker.emit(`meta.socket_client.disconnected:${fetchId}`, {
            serviceName,
            serviceAddress,
            servicePort,
          });
          handshake = false;
        });

        socket.connect();

        Cadenza.createEphemeralMetaTask(
          `Handshake with ${URL}`,
          async () => {
            console.log("SocketClient: HANDSHAKING", URL);
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
                console.log("Socket handshake result", result);
              },
            );
          },
          "Handshakes with socket server",
        ).doOn(`meta.socket_client.connected:${fetchId}`);

        const delegateTask = Cadenza.createMetaTask(
          `Delegate flow to ${URL}`,
          async (ctx, emit) => {
            if (ctx.__remoteRoutineName === undefined) {
              return;
            }

            return new Promise((resolve, reject) => {
              delete ctx.__isSubMeta;
              console.log("Socket Delegate:", socket.connected, ctx);
              emitWhenReady(
                "delegation",
                ctx,
                20_000,
                (resultContext: AnyObject) => {
                  console.log("Resolved socket delegate", resultContext);

                  const metadata = resultContext.__metadata;
                  delete resultContext.__metadata;
                  emit(
                    `meta.socket_client.delegated:${ctx.__metadata.__deputyExecId}`,
                    {
                      ...resultContext,
                      ...metadata,
                    },
                  );
                  resolve(resultContext);
                },
              );
            });
          },
          `Delegate flow to service ${serviceName} with address ${URL}`,
        ).doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`);

        const transmitTask = Cadenza.createMetaTask(
          `Transmit signal to ${URL}`,
          async (ctx, emit) => {
            if (ctx.__signalName === undefined) {
              return;
            }

            return new Promise((resolve) => {
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
        ).doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`);

        Cadenza.createEphemeralMetaTask(
          `Shutdown SocketClient ${URL}`,
          () => {
            socket?.close();
            delegateTask.destroy();
            transmitTask.destroy();
          },
          "Shuts down the socket client",
        )
          .doOn(
            `meta.socket_shutdown_requested:${fetchId}`,
            `meta.socket_client.disconnected:${fetchId}`,
            `meta.fetch.handshake_failed:${fetchId}`,
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
