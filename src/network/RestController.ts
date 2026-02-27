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
import { META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT } from "../utils/inquiry";
import type { AnyObject } from "@cadenza.io/core";

type TransportDetailLevel = "summary" | "full";

interface TransportDiagnosticErrorEntry {
  at: string;
  message: string;
}

interface FetchClientDiagnosticsState {
  fetchId: string;
  serviceName: string;
  url: string;
  connected: boolean;
  destroyed: boolean;
  lastHandshakeAt: string | null;
  lastHandshakeError: string | null;
  lastError: string | null;
  lastErrorAt: number;
  errorHistory: TransportDiagnosticErrorEntry[];
  delegationRequests: number;
  delegationFailures: number;
  signalTransmissions: number;
  signalFailures: number;
  statusChecks: number;
  statusFailures: number;
  updatedAt: number;
}

/**
 * RestController class is responsible for managing RESTful interactions, including defining
 * server configurations and handling client requests. It serves as a singleton, accessible via
 * the `instance` property.
 */
export default class RestController {
  private static _instance: RestController;
  public static get instance(): RestController {
    if (!this._instance) this._instance = new RestController();
    return this._instance;
  }

  private fetchClientDiagnostics: Map<string, FetchClientDiagnosticsState> =
    new Map();
  private readonly diagnosticsErrorHistoryLimit = 100;
  private readonly diagnosticsMaxClientEntries = 500;
  private readonly destroyedDiagnosticsTtlMs = 15 * 60_000;

  private pruneFetchClientDiagnostics(now = Date.now()): void {
    for (const [fetchId, state] of this.fetchClientDiagnostics.entries()) {
      if (state.destroyed && now - state.updatedAt > this.destroyedDiagnosticsTtlMs) {
        this.fetchClientDiagnostics.delete(fetchId);
      }
    }

    if (this.fetchClientDiagnostics.size <= this.diagnosticsMaxClientEntries) {
      return;
    }

    const entriesByEvictionPriority = Array.from(
      this.fetchClientDiagnostics.entries(),
    ).sort((left, right) => {
      if (left[1].destroyed !== right[1].destroyed) {
        return left[1].destroyed ? -1 : 1;
      }

      return left[1].updatedAt - right[1].updatedAt;
    });

    while (
      this.fetchClientDiagnostics.size > this.diagnosticsMaxClientEntries &&
      entriesByEvictionPriority.length > 0
    ) {
      const [fetchId] = entriesByEvictionPriority.shift()!;
      this.fetchClientDiagnostics.delete(fetchId);
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

  private ensureFetchClientDiagnostics(
    fetchId: string,
    serviceName: string,
    url: string,
  ): FetchClientDiagnosticsState {
    const now = Date.now();
    this.pruneFetchClientDiagnostics(now);

    let state = this.fetchClientDiagnostics.get(fetchId);
    if (!state) {
      state = {
        fetchId,
        serviceName,
        url,
        connected: false,
        destroyed: false,
        lastHandshakeAt: null,
        lastHandshakeError: null,
        lastError: null,
        lastErrorAt: 0,
        errorHistory: [],
        delegationRequests: 0,
        delegationFailures: 0,
        signalTransmissions: 0,
        signalFailures: 0,
        statusChecks: 0,
        statusFailures: 0,
        updatedAt: now,
      };
      this.fetchClientDiagnostics.set(fetchId, state);
    } else {
      state.serviceName = serviceName;
      state.url = url;
    }

    this.pruneFetchClientDiagnostics(now);
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

  private recordFetchClientError(
    fetchId: string,
    serviceName: string,
    url: string,
    error: unknown,
  ): void {
    const state = this.ensureFetchClientDiagnostics(fetchId, serviceName, url);
    const message = this.getErrorMessage(error);
    const now = Date.now();

    state.lastError = message;
    state.lastErrorAt = now;
    state.updatedAt = now;
    state.errorHistory.push({ at: new Date(now).toISOString(), message });

    if (state.errorHistory.length > this.diagnosticsErrorHistoryLimit) {
      state.errorHistory.splice(
        0,
        state.errorHistory.length - this.diagnosticsErrorHistoryLimit,
      );
    }
  }

  private collectFetchTransportDiagnostics(ctx: AnyObject): AnyObject {
    this.pruneFetchClientDiagnostics();
    const { detailLevel, includeErrorHistory, errorHistoryLimit } =
      this.resolveTransportDiagnosticsOptions(ctx);
    const serviceName = Cadenza.serviceRegistry.serviceName ?? "UnknownService";
    const states = Array.from(this.fetchClientDiagnostics.values()).sort((a, b) =>
      a.fetchId.localeCompare(b.fetchId),
    );

    const summary = {
      detailLevel,
      totalClients: states.length,
      connectedClients: states.filter((state) => state.connected).length,
      destroyedClients: states.filter((state) => state.destroyed).length,
      delegationRequests: states.reduce(
        (acc, state) => acc + state.delegationRequests,
        0,
      ),
      delegationFailures: states.reduce(
        (acc, state) => acc + state.delegationFailures,
        0,
      ),
      signalTransmissions: states.reduce(
        (acc, state) => acc + state.signalTransmissions,
        0,
      ),
      signalFailures: states.reduce((acc, state) => acc + state.signalFailures, 0),
      statusChecks: states.reduce((acc, state) => acc + state.statusChecks, 0),
      statusFailures: states.reduce((acc, state) => acc + state.statusFailures, 0),
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
            fetchClient: summary,
          },
        },
      };
    }

    const clients = states.map((state) => {
      const details: AnyObject = {
        fetchId: state.fetchId,
        serviceName: state.serviceName,
        url: state.url,
        connected: state.connected,
        destroyed: state.destroyed,
        lastHandshakeAt: state.lastHandshakeAt,
        lastHandshakeError: state.lastHandshakeError,
        latestError: state.lastError,
        delegationRequests: state.delegationRequests,
        delegationFailures: state.delegationFailures,
        signalTransmissions: state.signalTransmissions,
        signalFailures: state.signalFailures,
        statusChecks: state.statusChecks,
        statusFailures: state.statusFailures,
      };

      if (includeErrorHistory) {
        details.errorHistory = state.errorHistory.slice(-errorHistoryLimit);
      }

      return details;
    });

    return {
      transportDiagnostics: {
        [serviceName]: {
          fetchClient: {
            ...summary,
            clients,
          },
        },
      },
    };
  }

  /**
   * Fetches data from the given URL with a specified timeout. This function performs
   * a fetch request with the ability to cancel the request if it exceeds the provided timeout duration.
   *
   * @param {string} url - The URL to make the request to.
   * @param {any} requestInit - The initialization object for the fetch request, which may include method, headers, and body.
   * @param {number} timeoutMs - The maximum duration in milliseconds to wait for the fetch request to complete before aborting.
   * @returns {Promise<any>} A promise that resolves to the parsed response data if the request is successful.
   * @throws {Error} Throws an error if the request fails due to issues such as timeout or other unexpected errors.
   */
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
        Cadenza.log(
          "Fetch request timed out.",
          { error, URL: url, requestInit },
          "warning",
        );
        // Handle timeout specifically
      } else {
        Cadenza.log(
          "Fetch request error.",
          { error, URL: url, requestInit },
          "error",
        );
        // Handle other errors
      }
      throw error; // Re-throw to propagate the error
    }
  };

  /**
   * Constructor for initializing the REST server and related configurations.
   *
   * This method configures and sets up the REST server tasks using Cadenza's meta-task system, defining certain endpoints
   * like `/handshake`, `/delegation`, `/signal`, and `/status`. It also integrates security settings, CORS policies,
   * and rate-limiting profiles (low, medium, high) based on the provided context. Furthermore, it starts the server and
   * establishes necessary meta-handlings to enable delegated operations and signal processing.
   *
   * It initializes and configures the REST server tasks.
   */
  constructor() {
    Cadenza.registry.getTaskByName.doOn(
      "meta.rest.delegation_requested",
      "meta.socket.delegation_requested",
    );
    Cadenza.registry.getRoutineByName.doOn(
      "meta.rest.delegation_requested",
      "meta.socket.delegation_requested",
    );

    Cadenza.createMetaTask(
      "Collect fetch transport diagnostics",
      (ctx) => this.collectFetchTransportDiagnostics(ctx),
      "Responds to distributed transport diagnostics inquiries with REST/fetch client data.",
    ).respondsTo(META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT);

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

            console.log("Service inserted...");

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
        )
          .attachSignal("meta.rest.browser_detected")
          .then(
            Cadenza.createMetaTask(
              "Define RestServer",
              (ctx) => {
                const app = ctx.__app;

                // TODO: add body validation based on profile

                app.post("/handshake", (req: Request, res: Response) => {
                  try {
                    Cadenza.log("New fetch connection.", req.body);
                    Cadenza.emit("meta.rest.handshake", req.body);
                    res.send({
                      __status: "success",
                      __serviceInstanceId:
                        Cadenza.serviceRegistry.serviceInstanceId,
                    });
                  } catch (e) {
                    Cadenza.log(
                      "Error in fetch handshake",
                      { error: e, body: req.body },
                      "error",
                    );
                    res.send({ __status: "error" });
                  }
                });

                app.post("/delegation", (req: Request, res: Response) => {
                  let deputyExecId;
                  let ctx;
                  ctx = req.body;
                  deputyExecId = ctx.__metadata.__deputyExecId;

                  Cadenza.createEphemeralMetaTask(
                    "Resolve delegation",
                    (endCtx) => {
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

                  Cadenza.emit("meta.rest.delegation_requested", {
                    ...ctx,
                    __name: ctx.__remoteRoutineName,
                  });
                });

                app.post("/signal", (req: Request, res: Response) => {
                  let ctx;
                  try {
                    ctx = req.body;
                    if (
                      !Cadenza.signalBroker
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
                    Cadenza.log(
                      "Error in REST signal consumption",
                      { error: e, ...ctx },
                      "error",
                    );
                    res.send({
                      __status: "error",
                      __error: e,
                    });
                    return;
                  }

                  Cadenza.emit(ctx.__signalName, ctx);
                });

                app.get("/status", (req: Request, res: Response) => {
                  Cadenza.createEphemeralMetaTask(
                    "Resolve status check",
                    (statusCtx) => res.json(statusCtx),
                    "Resolves a status check request",
                    { register: false },
                  ).doAfter(Cadenza.serviceRegistry.getStatusTask);

                  Cadenza.emit(
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
                    let address: string = "undefined";
                    let port: number = ctx.__port;
                    let exposed: boolean = false;

                    const createHttpServer = async (ctx: any) => {
                      await new Promise((resolve) => {
                        const server = http.createServer(ctx.__app);
                        ctx.httpServer = server;
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
                            address = server?.address()?.address || "undefined";
                          }

                          console.log(
                            `Server is running on ${address}:${port}`,
                          );
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
                        ctx.httpsServer = httpsServer;
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

                    delete ctx.__app;

                    return ctx;
                  },
                  "Configures network mode",
                )
                  .emits("global.meta.rest.network_configured")
                  .emitsOnFail("meta.rest.network_configuration_failed")
                  .then(
                    Cadenza.createMetaTask(
                      "Connect delegation to runner",
                      () => {
                        Cadenza.createMetaTask(
                          "Start run",
                          (context, emit: any) => {
                            if (context.task || context.routine) {
                              const routine = context.task ?? context.routine;
                              delete context.task;
                              delete context.routine;
                              context.__routineExecId =
                                context.__metadata?.__deputyExecId ?? null;
                              context.__isDeputy = true;
                              Cadenza.runner.run(routine, context);
                              return true;
                            } else {
                              context.errored = true;
                              context.__error = "No routine or task defined.";
                              emit("meta.runner.failed", context);
                              return false;
                            }
                          },
                          "Forward delegations to runner",
                        )
                          .attachSignal("meta.runner.failed")
                          .doAfter(
                            Cadenza.registry.getTaskByName,
                            Cadenza.registry.getRoutineByName,
                          );
                      },
                    ),
                  ),
              )
              .emitsOnFail("meta.rest.failed"),
          ),
      ],
      "Bootstraps the REST server as socket fallback",
    ).doOn("meta.service_registry.service_inserted");

    Cadenza.createMetaTask(
      "Setup fetch client",
      (ctx) => {
        const { serviceName, serviceAddress, servicePort, protocol } = ctx;

        const port = protocol === "https" ? 443 : servicePort;
        const URL = `${protocol}://${serviceAddress}:${port}`;
        const fetchId = `${serviceAddress}_${port}`;
        const fetchDiagnostics = this.ensureFetchClientDiagnostics(
          fetchId,
          serviceName,
          URL,
        );
        fetchDiagnostics.destroyed = false;
        fetchDiagnostics.updatedAt = Date.now();

        if (Cadenza.get(`Send Handshake to ${URL}`)) {
          console.error("Fetch client already exists", URL);
          return;
        }

        const handshakeTask = Cadenza.createMetaTask(
          `Send Handshake to ${URL}`,
          async (ctx, emit) => {
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
              if (response.__status !== "success") {
                const error =
                  response.__error ??
                  `Failed to connect to service ${serviceName} ${ctx.serviceInstanceId}`;
                fetchDiagnostics.connected = false;
                fetchDiagnostics.lastHandshakeError = error;
                fetchDiagnostics.updatedAt = Date.now();
                this.recordFetchClientError(fetchId, serviceName, URL, error);
                Cadenza.log(
                  "Fetch handshake failed.",
                  { error, serviceName, URL },
                  "warning",
                );
                emit(`meta.fetch.handshake_failed:${fetchId}`, response);
                return { ...ctx, __error: error, errored: true };
              }

              ctx.serviceInstanceId = response.__serviceInstanceId;
              fetchDiagnostics.connected = true;
              fetchDiagnostics.destroyed = false;
              fetchDiagnostics.lastHandshakeAt = new Date().toISOString();
              fetchDiagnostics.lastHandshakeError = null;
              fetchDiagnostics.updatedAt = Date.now();

              Cadenza.log("Fetch client connected.", {
                response,
                serviceName,
                URL,
              });

              for (const communicationType of ctx.communicationTypes) {
                // TODO: Should be done in other situations as well
                emit("global.meta.fetch.service_communication_established", {
                  data: {
                    serviceInstanceId: ctx.serviceInstanceId,
                    serviceInstanceClientId:
                      Cadenza.serviceRegistry.serviceInstanceId,
                    communicationType,
                  },
                });
              }
            } catch (e) {
              fetchDiagnostics.connected = false;
              fetchDiagnostics.lastHandshakeError = this.getErrorMessage(e);
              fetchDiagnostics.updatedAt = Date.now();
              this.recordFetchClientError(fetchId, serviceName, URL, e);
              Cadenza.log(
                "Error in fetch handshake",
                { error: e, serviceName, URL, ctx },
                "error",
              );
              return { ...ctx, __error: e, errored: true };
            }

            return ctx;
          },
          "Sends handshake request",
          { retryCount: 5, retryDelay: 1000, retryDelayFactor: 1.5 },
        )
          .doOn(`meta.fetch.handshake_requested:${fetchId}`)
          .emits("meta.fetch.handshake_complete")
          .attachSignal(
            "meta.fetch.handshake_failed",
            "global.meta.fetch.service_communication_established",
          );

        const delegateTask = Cadenza.createMetaTask(
          `Delegate flow to REST server ${URL}`,
          async (ctx, emit) => {
            if (ctx.__remoteRoutineName === undefined) {
              return;
            }

            fetchDiagnostics.delegationRequests++;
            fetchDiagnostics.updatedAt = Date.now();

            let resultContext;
            try {
              resultContext = await this.fetchDataWithTimeout(
                `${URL}/delegation`,
                {
                  headers: {
                    "Content-Type": "application/json",
                  },
                  method: "POST",
                  body: JSON.stringify(ctx),
                },
                30_000,
              );
              if (resultContext?.errored || resultContext?.failed) {
                fetchDiagnostics.delegationFailures++;
                fetchDiagnostics.updatedAt = Date.now();
                this.recordFetchClientError(
                  fetchId,
                  serviceName,
                  URL,
                  resultContext?.__error ?? resultContext?.error ?? "Delegation failed",
                );
              }
            } catch (e) {
              console.error("Error in delegation", e);
              // TODO: Retry on too many requests
              fetchDiagnostics.delegationFailures++;
              fetchDiagnostics.updatedAt = Date.now();
              this.recordFetchClientError(fetchId, serviceName, URL, e);
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
          .emitsOnFail("meta.fetch.delegate_failed")
          .attachSignal("meta.fetch.delegated");

        const transmitTask = Cadenza.createMetaTask(
          `Transmit signal to server ${URL}`,
          async (ctx, emit) => {
            if (ctx.__signalName === undefined) {
              return;
            }

            fetchDiagnostics.signalTransmissions++;
            fetchDiagnostics.updatedAt = Date.now();

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

              if (ctx.__routineExecId) {
                emit(`meta.fetch.transmitted:${ctx.__routineExecId}`, response);
              }

              if (response?.errored || response?.failed) {
                fetchDiagnostics.signalFailures++;
                fetchDiagnostics.updatedAt = Date.now();
                this.recordFetchClientError(
                  fetchId,
                  serviceName,
                  URL,
                  response?.__error ?? response?.error ?? "Signal transmission failed",
                );
              }
            } catch (e) {
              // TODO: Retry on too many requests
              console.error("Error in transmission", e);
              fetchDiagnostics.signalFailures++;
              fetchDiagnostics.updatedAt = Date.now();
              this.recordFetchClientError(fetchId, serviceName, URL, e);

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
          .emitsOnFail("meta.fetch.signal_transmission_failed")
          .attachSignal("meta.fetch.transmitted");

        const statusTask = Cadenza.createMetaTask(
          `Request status from ${URL}`,
          async (ctx) => {
            fetchDiagnostics.statusChecks++;
            fetchDiagnostics.updatedAt = Date.now();
            let status;
            try {
              status = await this.fetchDataWithTimeout(
                `${URL}/status`,
                {
                  method: "GET",
                },
                1000,
              );

              if (status?.errored || status?.failed) {
                fetchDiagnostics.statusFailures++;
                fetchDiagnostics.updatedAt = Date.now();
                this.recordFetchClientError(
                  fetchId,
                  serviceName,
                  URL,
                  status?.__error ?? status?.error ?? "Status check failed",
                );
              }
            } catch (e) {
              // TODO: Retry on too many requests
              fetchDiagnostics.statusFailures++;
              fetchDiagnostics.updatedAt = Date.now();
              this.recordFetchClientError(fetchId, serviceName, URL, e);

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

        Cadenza.createEphemeralMetaTask("Destroy fetch client", () => {
          fetchDiagnostics.connected = false;
          fetchDiagnostics.destroyed = true;
          fetchDiagnostics.updatedAt = Date.now();
          Cadenza.log("Destroying fetch client", { URL, serviceName });
          handshakeTask.destroy();
          delegateTask.destroy();
          transmitTask.destroy();
          statusTask.destroy();
        })
          .doOn(
            `meta.fetch.destroy_requested:${fetchId}`,
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
        ).attachSignal("meta.fetch.handshake_requested"),
      )
      .doOn("meta.service_registry.dependee_registered")
      .emitsOnFail("meta.fetch.connect_failed");
  }
}
