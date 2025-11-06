import Cadenza from "../Cadenza";
import express, { Request, Response } from "express";
import bodyParser from "body-parser";
import helmet from "helmet";
import cors from "cors";
import { RateLimiterMemory } from "rate-limiter-flexible";
import http from "node:http";
import fs from "node:fs";
import https from "node:https";
import fetch from "node-fetch";
import { isBrowser } from "../utils/environment";

export default class RestController {
  private static _instance: RestController;
  public static get instance(): RestController {
    if (!this._instance) this._instance = new RestController();
    return this._instance;
  }

  fetchDataWithTimeout = async function (
    url: string,
    requestInit: any,
    timeoutMs: number,
  ): Promise<any> {
    const signal = AbortSignal.timeout(timeoutMs); // Create a signal that aborts after timeoutMs

    try {
      const response = await fetch(url, { ...requestInit, signal }); // Send the request with the signal
      // Process the response
      return await response.json();
    } catch (error: any) {
      if (error?.name === "AbortError") {
        console.error("Fetch request timed out:", error);
        // Handle timeout specifically
      } else {
        console.error("Fetch error:", error);
        // Handle other errors
      }
      throw error; // Re-throw to propagate the error
    }
  };

  constructor() {
    Cadenza.registry.getTaskByName.doOn(
      "meta.rest.delegation_requested",
      "meta.socket.delegation_requested",
    );
    Cadenza.registry.getRoutineByName.doOn(
      "meta.rest.delegation_requested",
      "meta.socket.delegation_requested",
    );

    Cadenza.createMetaRoutine(
      "RestServer",
      [
        Cadenza.createMetaTask(
          "Setup Express app security",
          (ctx, emit) => {
            if (isBrowser) {
              emit("meta.rest.browser_detected", {
                data: {
                  uuid: ctx.__serviceInstanceId,
                  address: `browser:${ctx.__serviceInstanceId}`,
                  port: 0,
                  exposed: false,
                  process_pid: 1,
                  service_name: ctx.__serviceName,
                  is_frontend: true,
                  is_active: true,
                  is_non_responsive: false,
                  is_blocked: false,
                  health: {},
                },
                ...ctx,
              });
              return;
            }
            const app = express();
            app.use(bodyParser.json());

            switch (ctx.__securityProfile) {
              case "low":
                app.use(helmet());
                app.use(cors({ origin: "*" })); // Allow all origins (insecure for prod)
                break;

              case "medium":
                app.use(helmet());
                app.use(
                  cors({
                    origin: process.env.CORS_ORIGIN ?? "*",
                    methods: ["GET", "POST"],
                  }),
                );

                // Rate limiting (1000 req/5min per IP)
                app.use((req: any, res: any, next: any) => {
                  new RateLimiterMemory({
                    points: 10000,
                    duration: 10,
                  })
                    .consume(req.ip)
                    .then(() => next())
                    .catch(() =>
                      res.status(429).json({ error: "Too many requests" }),
                    );
                });
                break;

              case "high":
                app.use(
                  helmet({
                    contentSecurityPolicy: {
                      directives: { defaultSrc: ["'self'"] },
                    }, // Strict CSP
                    referrerPolicy: { policy: "no-referrer" },
                  }),
                );

                if (!process.env.CORS_ORIGIN) {
                  throw new Error(
                    "CORS_ORIGIN must be set for high security profile",
                  );
                }

                app.use(
                  cors({
                    origin: process.env.CORS_ORIGIN ?? "*",
                    methods: ["GET", "POST"],
                    credentials: true,
                  }),
                );

                // Rate limiting (50 req/1min per IP, block on exceed)
                app.use((req: any, res: any, next: any) => {
                  new RateLimiterMemory({
                    points: 1000,
                    duration: 60,
                    blockDuration: 300,
                  })
                    .consume(req.ip)
                    .then(() => next())
                    .catch((rej) => {
                      if (rej.msBeforeNext > 0) {
                        res.status(429).json({
                          error: "Too many requests",
                          retryAfter: rej.msBeforeNext / 1000,
                        });
                      } else {
                        res
                          .status(429)
                          .json({ error: "Rate limit exceeded, blocked" });
                      }
                    });
                });
                break;
            }

            return { ...ctx, __app: app };
          },
          "Sets up the Express server according to the security profile",
        ).then(
          Cadenza.createMetaTask(
            "Define RestServer",
            (ctx) => {
              const app = ctx.__app;

              // TODO: add body validation based on profile

              app.post("/handshake", (req: Request, res: Response) => {
                try {
                  console.log("REST HANDSHAKE", req.body);
                  Cadenza.broker.emit("meta.rest.handshake", req.body);
                  res.send({
                    __status: "success",
                    __serviceInstanceId:
                      Cadenza.serviceRegistry.serviceInstanceId,
                  });
                } catch (e) {
                  console.error("Error in handshake", e);
                  res.send({ __status: "error" });
                }
              });

              app.post("/delegation", (req: Request, res: Response) => {
                let deputyExecId;
                let ctx;
                try {
                  ctx = req.body;
                  deputyExecId = ctx.__metadata.__deputyExecId;
                  console.log("Rest delegation", deputyExecId, ctx);
                } catch (e) {
                  console.error("Error in delegation", e);
                  res.send({
                    __status: "error",
                    __error: e,
                    errored: true,
                    ...ctx,
                  });
                  return;
                }

                Cadenza.createEphemeralMetaTask(
                  "Resolve delegation",
                  (endCtx) => {
                    console.log("Resolve Rest delegation", endCtx);
                    const metadata = endCtx.__metadata;
                    delete endCtx.__metadata;
                    res.json({
                      ...endCtx,
                      ...metadata,
                      __status: "success",
                    });
                  },
                  "Resolves a delegation request",
                  { register: false },
                )
                  .doOn(`meta.node.graph_completed:${deputyExecId}`)
                  .emits(`meta.rest.delegation_resolved:${deputyExecId}`);

                // Cadenza.createEphemeralMetaTask(
                //   "Delegation progress update",
                //   (progressCtx) => {
                //     if (progressCtx.__progress !== undefined) {
                //       // TODO: Progress updates via polling or long-polling for REST, but omit broadcasting as per instruction
                //     }
                //   },
                //   "Updates delegation progress (polling-based for REST)",
                //   {
                //     once: false,
                //     destroyCondition: (progressCtx: AnyObject) =>
                //       progressCtx.__progress === 1 ||
                //       progressCtx.__graphComplete,
                //   },
                // ).doOn(
                //   `meta.node.routine_execution_progress:${routineExecId}`,
                //   `meta.node.ended_routine_execution:${routineExecId}`,
                // );

                Cadenza.broker.emit("meta.rest.delegation_requested", {
                  ...ctx,
                  __name: ctx.__remoteRoutineName,
                });
              });

              app.post("/signal", (req: Request, res: Response) => {
                let ctx;
                try {
                  ctx = req.body;
                  console.log("SIGNAL RECEIVED", ctx.__signalName);
                  if (
                    !Cadenza.broker
                      .listObservedSignals()
                      .includes(ctx.__signalName)
                  ) {
                    res.send({
                      ...ctx,
                      __status: "error",
                      __error: `No such signal: ${ctx.__signalName}`,
                      errored: true,
                    });
                    return;
                  }
                  res.send({
                    __status: "success",
                    __signalName: ctx.__signalName,
                  });
                } catch (e) {
                  console.error("Error in signal", e);
                  res.send({
                    __status: "error",
                    __error: e,
                  });
                  return;
                }

                Cadenza.broker.emit(ctx.__signalName, ctx);
              });

              app.get("/status", (req: Request, res: Response) => {
                Cadenza.createEphemeralMetaTask(
                  "Resolve status check",
                  (statusCtx) => res.json(statusCtx),
                  "Resolves a status check request",
                  { register: false },
                ).doAfter(Cadenza.serviceRegistry.getStatusTask);

                Cadenza.broker.emit(
                  "meta.rest.status_check_requested",
                  req.body.query,
                );
              });

              return true;
            },
            "Starts REST server and initiates meta-handling",
          )
            .then(
              Cadenza.createMetaTask(
                "Configure network",
                async (ctx) => {
                  let address: string = "localhost";
                  let port: number = ctx.__port;
                  let exposed: boolean = false;

                  const createHttpServer = async (ctx: any) => {
                    await new Promise((resolve, reject) => {
                      const server = http.createServer(ctx.__app);
                      ctx.__httpServer = server;
                      server.listen(ctx.__port, () => {
                        if (typeof server?.address() === "string") {
                          address = server.address() as string;
                          // @ts-ignore
                        } else if (server?.address()?.address === "::") {
                          if (process.env.NODE_ENV === "development") {
                            address = "localhost";
                          } else if (process.env.IS_DOCKER === "true") {
                            address =
                              process.env.CADENZA_SERVER_URL || "localhost";
                          }
                        } else {
                          // @ts-ignore
                          address = server?.address()?.address || "";
                        }

                        console.log(`Server is running on ${address}:${port}`);
                        resolve(address);
                      });

                      Cadenza.createMetaTask(
                        "Shutdown HTTP Server",
                        () => server.close(),
                        "Shuts down the HTTP server",
                      )
                        .doOn("meta.server_shutdown_requested")
                        .emits("meta.rest.shutdown:http");
                    });
                  };

                  const createHttpsServer = async (ctx: any) => {
                    if (
                      !process.env.SSL_KEY_PATH ||
                      !process.env.SSL_CERT_PATH
                    ) {
                      throw new Error(
                        "SSL_KEY_PATH and SSL_CERT_PATH must be set",
                      );
                    }

                    const options = {
                      key: fs.readFileSync(process.env.SSL_KEY_PATH),
                      cert: fs.readFileSync(process.env.SSL_CERT_PATH),
                    };

                    await new Promise((resolve, reject) => {
                      const httpsServer = https.createServer(
                        options,
                        ctx.__app,
                      );
                      ctx.__httpsServer = httpsServer;
                      ctx.__port = 443;
                      port = 443;
                      httpsServer.listen(443, () => {
                        if (typeof httpsServer?.address() === "string") {
                          address = httpsServer.address() as string;
                          // @ts-ignore
                        } else if (httpsServer?.address()?.address === "::") {
                          if (process.env.IS_DOCKER === "true") {
                            address =
                              process.env.CADENZA_SERVER_URL || "localhost";
                          }
                        } else {
                          // @ts-ignore
                          address = httpsServer?.address()?.address || "";
                        }

                        exposed = true;

                        console.log(
                          `HTTPS Server is running on ${address}:443`,
                        );
                        resolve(address);
                      });

                      Cadenza.createMetaTask(
                        "Shutdown HTTPS Server",
                        () => httpsServer.close(),
                        "Shuts down the HTTPS server",
                      )
                        .doOn("meta.server_shutdown_requested")
                        .emits("meta.rest.shutdown:https");
                    });
                  };

                  if (
                    ctx.__networkMode === "internal" ||
                    ctx.__networkMode === "dev"
                  ) {
                    await createHttpServer(ctx);
                  } else if (ctx.__networkMode === "exposed") {
                    await createHttpServer(ctx);
                    await createHttpsServer(ctx);
                  } else if (ctx.__networkMode === "exposed-high-sec") {
                    await createHttpsServer(ctx);
                  } else if (ctx.__networkMode === "auto") {
                    // TODO: auto-detect based on trusted network or dev mode etc.
                    await createHttpServer(ctx);
                    // createHttpsServer(ctx);
                  }

                  ctx.data = {
                    uuid: ctx.__serviceInstanceId,
                    address: address,
                    port: port,
                    exposed: exposed,
                    process_pid: process.pid,
                    service_name: ctx.__serviceName,
                    is_active: true,
                    is_database: ctx.__isDatabase,
                    is_non_responsive: false,
                    is_blocked: false,
                    health: {},
                  };

                  return ctx;
                },
                "Configures network mode",
              )
                .emits("meta.rest.network_configured")
                .emitsOnFail("meta.rest.network_configuration_failed"),
            )
            .emitsOnFail("meta.rest.failed"),
        ),
      ],
      "Bootstraps the REST server as socket fallback",
    ).doOn("meta.service_registry.service_inserted");

    Cadenza.createMetaTask(
      "Setup fetch client",
      (ctx, emit) => {
        const { serviceName, serviceAddress, servicePort, protocol } = ctx;

        const port = protocol === "https" ? 443 : servicePort;
        const URL = `${protocol}://${serviceAddress}:${port}`;
        const fetchId = `${serviceAddress}_${port}`;

        console.log("Fetch connecting to", URL);

        const handshakeTask = Cadenza.createMetaTask(
          `Send Handshake to ${URL}`,
          async (ctx, emit) => {
            console.log("Sending Rest handshake", ctx);
            try {
              const response = await this.fetchDataWithTimeout(
                `${URL}/handshake`,
                {
                  headers: {
                    "Content-Type": "application/json",
                  },
                  method: "POST",
                  body: JSON.stringify(ctx.handshakeData),
                },
                1000,
              );
              console.log("Rest Handshake result", response);
              if (response.__status !== "success") {
                const error =
                  response.__error ??
                  `Failed to connect to service ${serviceName} ${ctx.serviceInstanceId}`;
                console.error(error);
                emit(`meta.fetch.handshake_failed:${fetchId}`, response);
                return { ...ctx, __error: error, errored: true };
              }

              ctx.serviceInstanceId = response.__serviceInstanceId;

              console.log(
                `Connected to service ${serviceName} ${URL}`,
                response,
              );

              for (const communicationType of ctx.communicationTypes) {
                // TODO: Should be done in other situations as well
                emit("meta.fetch.service_communication_established", {
                  data: {
                    serviceInstanceId: ctx.serviceInstanceId,
                    serviceInstanceClientId:
                      Cadenza.serviceRegistry.serviceInstanceId,
                    communicationType,
                  },
                });
              }
            } catch (e) {
              console.error("Error in handshake", e);
              return { ...ctx, __error: e, errored: true };
            }

            return ctx;
          },
          "Sends handshake request",
          { retryCount: 5, retryDelay: 1000, retryDelayFactor: 1.5 },
        )
          .doOn(`meta.fetch.handshake_requested:${fetchId}`)
          .emits("meta.fetch.handshake_complete");

        const delegateTask = Cadenza.createMetaTask(
          `Delegate flow to REST server ${URL}`,
          async (ctx, emit) => {
            if (ctx.__remoteRoutineName === undefined) {
              return;
            }

            console.log("Delegating via REST", ctx);

            let resultContext;
            try {
              const response = await fetch(`${URL}/delegation`, {
                headers: {
                  "Content-Type": "application/json",
                },
                method: "POST",
                body: JSON.stringify(ctx),
              });
              resultContext = await response.json();
            } catch (e) {
              // TODO: Retry on too many requests
              resultContext = {
                __error: `Error: ${e}`,
                errored: true,
                ...ctx,
                ...ctx.__metadata,
              };
            } finally {
              emit(
                `meta.fetch.delegated:${ctx.__metadata.__deputyExecId}`,
                resultContext,
              );
            }

            return resultContext;
          },
          "Sends delegation request",
        )
          .doOn(
            `meta.service_registry.selected_instance_for_fetch:${fetchId}`,
            `meta.service_registry.socket_failed:${fetchId}`,
          )
          .emitsOnFail("meta.fetch.delegate_failed");

        const transmitTask = Cadenza.createMetaTask(
          `Transmit signal to server ${URL}`,
          async (ctx, emit) => {
            if (ctx.__signalName === undefined) {
              return;
            }

            let response;
            try {
              response = await this.fetchDataWithTimeout(
                `${URL}/signal`,
                {
                  headers: {
                    "Content-Type": "application/json",
                  },
                  method: "POST",
                  body: JSON.stringify(ctx),
                },
                1000,
              );

              console.log("SIGNAL TRANSMITTED", response);

              if (ctx.__routineExecId) {
                emit(`meta.fetch.transmitted:${ctx.__routineExecId}`, response);
              }
            } catch (e) {
              // TODO: Retry on too many requests

              response = {
                __error: `Error: ${e}`,
                errored: true,
                ...ctx,
              };
            }

            return response;
          },
          "Sends signal request",
        )
          .doOn(
            `meta.service_registry.selected_instance_for_fetch:${fetchId}`,
            `meta.signal_controller.remote_signal_registered:${serviceName}`,
            "meta.signal_controller.wildcard_signal_registered",
          )
          .emitsOnFail("meta.fetch.signal_transmission_failed");

        const statusTask = Cadenza.createMetaTask(
          `Request status from ${URL}`,
          async (ctx) => {
            let status;
            try {
              status = await this.fetchDataWithTimeout(
                `${URL}/status`,
                {
                  method: "GET",
                },
                1000,
              );
            } catch (e) {
              // TODO: Retry on too many requests

              status = {
                __error: `Error: ${e}`,
                errored: true,
                ...ctx,
              };
            }

            return status;
          },
          "Requests status",
        )
          .doOn("meta.fetch.status_check_requested")
          .emits("meta.fetch.status_checked")
          .emitsOnFail("meta.fetch.status_check_failed");

        Cadenza.createEphemeralMetaTask("Destroy fetch client", (ctx, emit) => {
          console.log("Destroying fetch client");
          handshakeTask.destroy();
          delegateTask.destroy();
          transmitTask.destroy();
          statusTask.destroy();
        })
          .doOn(
            "meta.fetch.destroy_requested",
            `meta.socket_client.disconnected:${fetchId}`,
            `meta.fetch.handshake_failed:${fetchId}`,
          )
          .emits("meta.fetch.destroyed");

        return true;
      },
      "Manages REST client requests as fallback",
    )
      .then(
        Cadenza.createMetaTask(
          "Prepare handshake",
          (ctx, emit) => {
            const {
              serviceName,
              serviceInstanceId,
              communicationTypes,
              serviceAddress,
              servicePort,
              protocol,
            } = ctx;

            const fetchId = `${serviceAddress}_${servicePort}`;

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
                // JWT token...
              },
            });
          },
          "Prepares handshake",
        ),
      )
      .doOn("meta.service_registry.dependee_registered")
      .emitsOnFail("meta.fetch.connect_failed");
  }
}
