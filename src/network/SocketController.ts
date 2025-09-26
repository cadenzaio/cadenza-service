import Cadenza from "../Cadenza";
import { Server } from "socket.io";
import { IRateLimiterOptions, RateLimiterMemory } from "rate-limiter-flexible";
import xss from "xss";
import { AnyObject } from "@cadenza.io/core";
import { io } from "socket.io-client";

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

          console.log("SocketServer: Setting up", ctx);
          let server;
          server = new Server(ctx.__httpsServer ?? ctx.__httpServer);

          console.log("SocketServer:", server);

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
              low: { points: Infinity, duration: 300 },
              medium: { points: 100, duration: 300 },
              high: { points: 50, duration: 60, blockDuration: 300 },
            };
            const limiter = new RateLimiterMemory(limiterOptions[profile]);
            socket.use((packet, next) => {
              limiter
                .consume(socket?.handshake?.address || "unknown")
                .then(() => next())
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
                  } else {
                    console.log("SocketServer: Rate limit exceeded, blocked");
                    socket.disconnect(true);
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

          server.on("connection", (ws: any) => {
            console.log("SocketServer: New connection");
            try {
              ws.on("handshake", (ctx: AnyObject) => {
                console.log("Socket HANDSHAKE", ctx.serviceInstanceId);
                ws.emit("handshake", {
                  serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
                  __status: "success",
                });
                Cadenza.broker.emit("meta.socket.handshake", ctx);
              });

              ws.on(
                "delegation",
                (ctx: AnyObject, callback: (ctx: AnyObject) => any) => {
                  console.log("Received socket delegation request", ctx);
                  const deputyExecId = ctx.__metadata.__deputyExecId;

                  Cadenza.createEphemeralMetaTask(
                    "Resolve delegation",
                    callback,
                    "Resolves a delegation request using the provided callback from the client (.emitWithAck())",
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

        const socket = io(URL, {
          reconnection: true,
          reconnectionAttempts: 20,
          reconnectionDelay: 1000,
          reconnectionDelayMax: 10000,
          randomizationFactor: 0.5,
          retries: 5,
        });

        console.log("SocketClient: Connecting to", URL);

        socket.on("connect", () => {
          console.log("SocketClient: CONNECTED");
          Cadenza.broker.emit("meta.socket_client.connected", ctx);
          // socket.emit("handshake", {
          //   serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
          // });
        });

        socket.on("handshake", (ctx) => {
          console.log("Socket client HANDSHAKE", ctx);
          Cadenza.broker.emit("meta.socket_client.handshake", ctx);
        });

        socket.on("delegation_progress", (ctx) => {
          Cadenza.broker.emit(
            `meta.socket_client.delegation_progress:${ctx.__metadata.__deputyExecId}`,
            ctx,
          );
        });

        socket.on("status_update", (status) => {
          Cadenza.broker.emit("meta.socket_client.status_received", status);
        });

        socket.on("connect_error", (err) => {
          console.error("SocketClient: connect_error", err);
          Cadenza.broker.emit("meta.socket_client.connect_error", err);
        });

        socket.on("error", (err) => {
          console.error("SocketClient: error", err);
          Cadenza.broker.emit("meta.socket_client.error", err);
        });

        socket.on("disconnect", () => {
          console.log("SocketClient: Disconnected", URL);
          Cadenza.broker.emit("meta.socket_client.disconnected", {
            URL,
          });
        });

        Cadenza.createMetaTask(
          `Delegate flow to ${URL}`,
          async (ctx, emit) => {
            if (ctx.__remoteRoutineName === undefined) {
              return;
            }

            console.log("Socket Delegate:", ctx);

            socket
              .timeout(10000)
              .emit("delegation", ctx, (err: any, resultContext: AnyObject) => {
                if (err) {
                  console.log("socket error:", err);
                  resultContext = {
                    __error: `Timeout error: ${err}`,
                    errored: true,
                    ...ctx,
                    ...ctx.__metadata,
                  };
                  emit(`meta.socket_client.delegate_failed`, resultContext);
                  return;
                }

                const metadata = resultContext.__metadata;
                delete resultContext.__metadata;
                console.log("SocketClient: Delegate result", resultContext);
                emit(
                  `meta.socket_client.delegated:${ctx.__metadata.__deputyExecId}`,
                  {
                    ...resultContext,
                    ...metadata,
                  },
                );
              });
          },
          `Delegate flow to service ${serviceName} with address ${URL}`,
        ).doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`);

        Cadenza.createMetaTask(
          `Transmit signal to ${URL}`,
          async (ctx, emit) => {
            if (ctx.__signalName === undefined) {
              return;
            }

            socket
              .timeout(ctx.__timeout ?? 10000)
              .emit("signal", ctx, (err: any, response: AnyObject) => {
                if (err) {
                  console.log("socket error:", err);
                  response = {
                    __error: `Timeout error: ${err}`,
                    errored: true,
                    ...ctx,
                    ...ctx.__metadata,
                  };
                  emit(
                    `meta.socket_client.signal_transmission_failed`,
                    response,
                  );
                  return;
                }

                if (ctx.__routineExecId) {
                  emit(
                    `meta.socket_client.transmitted:${ctx.__routineExecId}`,
                    response,
                  );
                }
              });
          },
          `Transmits signal to service ${serviceName} with address ${URL}`,
        ).doOn(`meta.service_registry.selected_instance_for_socket:${fetchId}`);

        Cadenza.createMetaTask(
          `Shutdown SocketClient ${URL}`,
          () => socket.close(),
          "Shuts down the socket client",
        )
          .doOn(`meta.socket_shutdown_requested:${fetchId}`) // TODO destroy tasks on close or instance removed? Also in fetch client
          .emits("meta.socket_client_shutdown_complete");

        return true;
      },
      "Connects to a specified socket server",
    )
      .doOn("meta.service_registry.dependee_registered")
      .emitsOnFail("meta.socket_client.connect_failed");
  }
}
