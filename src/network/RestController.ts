import Cadenza from "../Cadenza";
import express, { Request, Response } from "express";
import bodyParser from "body-parser";
import helmet from "helmet";
import cors from "cors";
import { RateLimiterMemory } from "rate-limiter-flexible";
import { v4 as uuid } from "uuid";
import http from "node:http";
import fs from "node:fs";
import https from "node:https";
import fetch from "node-fetch";
import { AnyObject } from "@cadenza.io/core";

export default class RestController {
  private static _instance: RestController;
  public static get instance(): RestController {
    if (!this._instance) this._instance = new RestController();
    return this._instance;
  }

  constructor() {
    Cadenza.createMetaRoutine(
      "RestServer",
      [
        Cadenza.createMetaTask(
          "Setup Express app security",
          (ctx) => {
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

                // Rate limiting (100 req/5min per IP)
                app.use((req: any, res: any, next: any) => {
                  new RateLimiterMemory({
                    points: 100,
                    duration: 300,
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
                    points: 50,
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
                Cadenza.broker.emit("meta.rest.handshake", req.body);
                res.send({ __status: "success" });
              });

              app.post("/delegation", (req: Request, res: Response) => {
                const ctx = req.body;
                const routineExecId = ctx.__routineExecId || uuid();

                Cadenza.createEphemeralMetaTask(
                  "Resolve delegation",
                  (endCtx) =>
                    res.json({
                      __status: "success",
                      __result: endCtx.__result,
                    }),
                  "Resolves a delegation request",
                )
                  .doOn(`meta.node.ended_routine_execution:${routineExecId}`)
                  .emitsAfter(`meta.rest.delegation_resolved:${routineExecId}`);

                Cadenza.createEphemeralMetaTask(
                  "Delegation progress update",
                  (progressCtx) => {
                    if (progressCtx.__progress !== undefined) {
                      // TODO: Progress updates via polling or long-polling for REST, but omit broadcasting as per instruction
                    }
                  },
                  "Updates delegation progress (polling-based for REST)",
                  {
                    once: false,
                    destroyCondition: (progressCtx: AnyObject) =>
                      progressCtx.__progress === 1 ||
                      progressCtx.__graphComplete,
                  },
                ).doOn(
                  `meta.node.routine_execution_progress:${routineExecId}`,
                  `meta.node.ended_routine_execution:${routineExecId}`,
                );

                Cadenza.broker.emit("meta.rest.delegation_requested", ctx);
              });

              app.post("/signal", (req: Request, res: Response) => {
                const ctx = req.body;
                res.send({ __status: "success" });
                Cadenza.broker.emit(ctx.__signalName, ctx.__context);
              });

              app.get("/status", (req: Request, res: Response) => {
                Cadenza.createEphemeralMetaTask(
                  "Resolve status check",
                  (statusCtx) => res.json(statusCtx),
                  "Resolves a status check request",
                ).doAfter(Cadenza.serviceRegistry.getStatusTask);

                Cadenza.broker.emit(
                  "meta.rest.status_check_requested",
                  req.query,
                );
              });

              return true;
            },
            "Starts REST server and initiates meta-handling",
          )
            .then(
              Cadenza.createMetaTask(
                "Configure network",
                (ctx) => {
                  let address: string = "localhost";
                  let port: number = ctx.__port;
                  let exposed: boolean = false;

                  const createHttpServer = (ctx: any) => {
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
                            process.env.PG_GRAPH_SERVER_URL || "localhost";
                        }
                      } else {
                        // @ts-ignore
                        address = server?.address()?.address || "";
                      }

                      console.log(`Server is running on ${address}:${port}`);
                    });

                    Cadenza.createMetaTask(
                      "Shutdown HTTP Server",
                      () => server.close(),
                      "Shuts down the HTTP server",
                    )
                      .doOn("meta.server_shutdown_requested")
                      .emitsAfter("meta.rest.shutdown:http");
                  };

                  const createHttpsServer = (ctx: any) => {
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

                    const httpsServer = https.createServer(options, ctx.__app);
                    ctx.__httpsServer = httpsServer;
                    httpsServer.listen(443, () => {
                      if (typeof httpsServer?.address() === "string") {
                        address = httpsServer.address() as string;
                        // @ts-ignore
                      } else if (httpsServer?.address()?.address === "::") {
                        if (process.env.IS_DOCKER === "true") {
                          address =
                            process.env.PG_GRAPH_SERVER_URL || "localhost";
                        }
                      } else {
                        // @ts-ignore
                        address = httpsServer?.address()?.address || "";
                      }

                      exposed = true;

                      console.log(`HTTPS Server is running on ${address}:443`);
                    });

                    Cadenza.createMetaTask(
                      "Shutdown HTTPS Server",
                      () => httpsServer.close(),
                      "Shuts down the HTTPS server",
                    )
                      .doOn("meta.server_shutdown_requested")
                      .emitsAfter("meta.rest.shutdown:https");
                  };

                  if (
                    ctx.__networkMode === "internal" ||
                    ctx.__networkMode === "dev"
                  ) {
                    createHttpServer(ctx);
                  } else if (ctx.__networkMode === "exposed") {
                    createHttpServer(ctx);
                    createHttpsServer(ctx);
                  } else if (ctx.__networkMode === "exposed-high-sec") {
                    createHttpsServer(ctx);
                  } else if (ctx.__networkMode === "auto") {
                    // TODO: auto-detect based on trusted network or dev mode etc.
                    createHttpServer(ctx);
                    // createHttpsServer(ctx);
                  }

                  ctx.data = {
                    id: uuid(),
                    address: address,
                    port: port,
                    exposed: exposed,
                    process_pid: process.pid,
                    service_name: ctx.__serviceName,
                    is_active: true,
                    is_non_responsive: false,
                    is_blocked: false,
                    health: {},
                  };

                  return ctx;
                },
                "Configures network mode",
              )
                .emitsAfter("meta.rest.network_configured")
                .emitsOnFail("meta.rest.network_configuration_failed"),
            )
            .emitsOnFail("meta.rest.failed"),
        ),
      ],
      "Bootstraps the REST server as socket fallback",
    ).doOn("meta.service_registry.service_inserted");

    Cadenza.createMetaTask(
      "FetchClient",
      (ctx) => {
        const {
          __serviceName,
          __serviceInstanceId,
          __serviceAddress,
          __servicePort,
          __protocol,
        } = ctx;

        const port = __protocol === "https" ? 443 : __servicePort;
        const URL = `${__protocol}://${__serviceAddress}:${port}`;

        Cadenza.createMetaTask(
          "Send Handshake",
          async (ctx) => {
            const response = await fetch(`${URL}/handshake`, {
              method: "POST",
              body: JSON.stringify(ctx),
            });
            const result = (await response.json()) as AnyObject;
            result.__serviceInstanceId = __serviceInstanceId;
            return result;
          },
          "Sends handshake request",
        )
          .doOn("meta.fetch.handshake_requested")
          .emitsAfter("meta.fetch.handshake_complete");

        Cadenza.createMetaTask(
          "Delegate flow to REST server",
          async (ctx, emit) => {
            if (ctx.__remoteRoutineName === undefined) {
              return;
            }

            let resultContext;
            try {
              const response = await fetch(`${URL}/delegation`, {
                method: "POST",
                body: JSON.stringify(ctx),
              });
              resultContext = await response.json();
            } catch (e) {
              resultContext = {
                __error: `Error: ${e}`,
                errored: true,
                ...ctx,
                ...ctx.__metadata,
              };
            } finally {
              emit(`meta.fetch.delegated:${ctx.__deputyExecId}`, resultContext);
            }

            return resultContext;
          },
          "Sends delegation request",
        )
          .doOn(
            `meta.service_registry.selected_instance_for_fetch:${__serviceInstanceId}`,
            `meta.service_registry.socket_failed:${__serviceInstanceId}`,
          )
          .emitsOnFail("meta.fetch.delegate_failed");

        Cadenza.createMetaTask(
          "Transmit signal to server",
          async (ctx, emit) => {
            if (ctx.__signalName === undefined) {
              return;
            }

            let response;
            try {
              response = await fetch(`${URL}/signal`, {
                method: "POST",
                body: JSON.stringify(ctx),
              });
              response = (await response.json()) as AnyObject;

              if (ctx.__routineExecId) {
                emit(`meta.fetch.transmitted:${ctx.__routineExecId}`, response);
              }
            } catch (e) {
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
            `meta.service_registry.selected_instance_for_fetch:${__serviceInstanceId}`,
            `meta.signal_controller.remote_signal_registered:${__serviceName}`,
            "meta.signal_controller.wildcard_signal_registered",
          )
          .emitsOnFail("meta.fetch.signal_transmission_failed");

        Cadenza.createMetaTask(
          "Request status",
          async (ctx) => {
            let status;
            try {
              const response = await fetch(`${URL}/status`, { method: "GET" });
              status = await response.json();
            } catch (e) {
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
          .emitsAfter("meta.fetch.status_checked")
          .emitsOnFail("meta.fetch.status_check_failed");

        return true;
      },
      "Manages REST client requests as fallback",
    )
      .doOn("meta.service_registry.dependee_registered")
      .emitsOnFail("meta.fetch.connect_failed");
  }
}
