import Cadenza from "../Cadenza";
import express from "express";
import bodyParser from "body-parser";
import helmet from "helmet";
import cors from "cors";
import { RateLimiterMemory } from "rate-limiter-flexible";
import http from "node:http";
import fs from "node:fs";
import https from "node:https";
import fetch from "node-fetch";
import { v4 as uuid } from "uuid";
import { isBrowser } from "../utils/environment";
import { META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT } from "../utils/inquiry";
import {
  attachDelegationRequestSnapshot,
  buildDelegationFailureContext,
  ensureDelegationContextMetadata,
  restoreDelegationRequestSnapshot,
  stripTransportSelectionRoutingContext,
  stripDelegationRequestSnapshot,
} from "../utils/delegation";
import { buildServiceCommunicationEstablishedContext } from "../utils/serviceCommunication";
import type { AnyObject } from "@cadenza.io/core";
import { buildTransportHandleKey } from "../utils/transport";

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
  handshakeInFlight: boolean;
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

interface ParsedFetchResponse {
  ok: boolean;
  status: number;
  statusText: string;
  headers: Record<string, string>;
  data: any;
}

const FETCH_HANDSHAKE_TIMEOUT_MS = 5000;
const DEFAULT_SIGNAL_TRANSMISSION_TIMEOUT_MS = 5000;
const AUTHORITY_SIGNAL_TRANSMISSION_TIMEOUT_MS = 30000;
const AUTHORITY_SIGNAL_TRANSMISSION_CONCURRENCY = 10;
const INQUIRY_TRACE_ENABLED =
  process.env.CADENZA_INQUIRY_TRACE === "1" ||
  process.env.CADENZA_INQUIRY_TRACE === "true";
const TRACED_INQUIRY_METADATA_SIGNALS = new Set([
  "global.meta.graph_metadata.inquiry_created",
  "global.meta.graph_metadata.inquiry_updated",
]);
const ACTOR_SESSION_TRACE_ENABLED =
  process.env.CADENZA_ACTOR_SESSION_TRACE === "1" ||
  process.env.CADENZA_ACTOR_SESSION_TRACE === "true";

function summarizeRequestBodyForLogging(body: unknown): unknown {
  if (typeof body !== "string") {
    return body;
  }

  const summary: AnyObject = {
    bodyLength: body.length,
  };

  try {
    const parsed = JSON.parse(body) as AnyObject;
    const queryData =
      parsed.queryData && typeof parsed.queryData === "object"
        ? (parsed.queryData as AnyObject)
        : null;
    const queryDataData =
      queryData?.data && typeof queryData.data === "object"
        ? (queryData.data as AnyObject)
        : null;
    const data =
      parsed.data && typeof parsed.data === "object" ? (parsed.data as AnyObject) : null;

    summary.rootKeys = Object.keys(parsed).slice(0, 24);
    summary.remoteRoutineName =
      typeof parsed.__remoteRoutineName === "string" ? parsed.__remoteRoutineName : null;
    summary.serviceName =
      typeof parsed.__serviceName === "string" ? parsed.__serviceName : null;
    summary.authorityBootstrapChannel =
      parsed.__authorityBootstrapChannel === true ||
      parsed.__metadata?.__authorityBootstrapChannel === true;
    summary.hasData = data !== null;
    summary.dataKeys = data ? Object.keys(data).slice(0, 24) : [];
    summary.hasQueryData = queryData !== null;
    summary.queryDataKeys = queryData ? Object.keys(queryData).slice(0, 24) : [];
    summary.queryDataDataKeys = queryDataData
      ? Object.keys(queryDataData).slice(0, 24)
      : [];
  } catch {
    summary.preview = body.slice(0, 240);
  }

  return summary;
}

function summarizeRequestInitForLogging(requestInit: any): AnyObject {
  if (!requestInit || typeof requestInit !== "object") {
    return requestInit;
  }

  return {
    ...requestInit,
    body: summarizeRequestBodyForLogging(requestInit.body),
  };
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
        handshakeInFlight: false,
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

  private resolveJsonBodyLimit(): string {
    const configuredLimit = process.env.CADENZA_REST_BODY_LIMIT?.trim();
    return configuredLimit && configuredLimit.length > 0
      ? configuredLimit
      : "10mb";
  }

  private async parseFetchResponse(response: any): Promise<ParsedFetchResponse> {
    const contentType = response.headers.get("content-type") ?? "";
    const rawText = await response.text();
    const headers = Object.fromEntries(response.headers.entries());

    if (rawText.length === 0) {
      return {
        ok: response.ok,
        status: response.status,
        statusText: response.statusText,
        headers,
        data: {},
      };
    }

    if (!contentType.toLowerCase().includes("application/json")) {
      throw new Error(
        `Expected JSON response from ${response.url ?? "remote service"} but received ${contentType || "unknown content type"} (HTTP ${response.status}). Body preview: ${rawText.slice(0, 200)}`,
      );
    }

    try {
      return {
        ok: response.ok,
        status: response.status,
        statusText: response.statusText,
        headers,
        data: JSON.parse(rawText),
      };
    } catch (error) {
      throw new Error(
        `Failed to parse JSON response from ${response.url ?? "remote service"} (HTTP ${response.status}). Body preview: ${rawText.slice(0, 200)}. Parse error: ${this.getErrorMessage(error)}`,
      );
    }
  }

  private resolveDelegationTimeoutMs(ctx: AnyObject): number {
    const explicitTimeouts = [
      ctx?.__timeout,
      ctx?.__metadata?.__timeout,
      ...(Array.isArray(ctx?.joinedContexts)
        ? ctx.joinedContexts.flatMap((joinedCtx: AnyObject) => [
            joinedCtx?.__timeout,
            joinedCtx?.__metadata?.__timeout,
          ])
        : []),
    ];
    const explicitTimeoutMs = explicitTimeouts.find(
      (value) => typeof value === "number" && Number.isFinite(value) && value > 0,
    );

    if (typeof explicitTimeoutMs === "number") {
      return explicitTimeoutMs;
    }

    const syncing =
      ctx?.__syncing === true ||
      ctx?.__metadata?.__syncing === true ||
      (Array.isArray(ctx?.joinedContexts) &&
        ctx.joinedContexts.some(
          (joinedCtx: AnyObject) =>
            joinedCtx?.__syncing === true ||
            joinedCtx?.__metadata?.__syncing === true,
        ));

    return syncing ? 120_000 : 30_000;
  }

  private resolveSignalTransmissionTimeoutMs(
    serviceName: string,
    ctx: AnyObject,
  ): number {
    const syncing =
      ctx?.__syncing === true ||
      ctx?.__metadata?.__syncing === true ||
      (Array.isArray(ctx?.joinedContexts) &&
        ctx.joinedContexts.some(
          (joinedCtx: AnyObject) =>
            joinedCtx?.__syncing === true ||
            joinedCtx?.__metadata?.__syncing === true,
        ));

    if (syncing) {
      return 120_000;
    }

    return serviceName === "CadenzaDB"
      ? AUTHORITY_SIGNAL_TRANSMISSION_TIMEOUT_MS
      : DEFAULT_SIGNAL_TRANSMISSION_TIMEOUT_MS;
  }

  private resolveSignalTransmissionConcurrency(serviceName: string): number {
    return serviceName === "CadenzaDB"
      ? AUTHORITY_SIGNAL_TRANSMISSION_CONCURRENCY
      : 0;
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
  fetchDataWithTimeout = async (
    url: string,
    requestInit: any,
    timeoutMs: number,
  ): Promise<any> => {
    const signal = AbortSignal.timeout(timeoutMs); // Create a signal that aborts after timeoutMs

    try {
      const response = await fetch(url, { ...requestInit, signal });
      const parsedResponse = await this.parseFetchResponse(response);
      return parsedResponse.data;
    } catch (error: any) {
      const loggedRequestInit = summarizeRequestInitForLogging(requestInit);
      if (error?.name === "AbortError") {
        Cadenza.log(
          "Fetch request timed out.",
          { error, URL: url, requestInit: loggedRequestInit },
          "warning",
        );
        // Handle timeout specifically
      } else {
        Cadenza.log(
          "Fetch request error.",
          { error, URL: url, requestInit: loggedRequestInit },
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
            if (isBrowser || ctx.__isFrontend) {
              const browserTransportData = Array.isArray(ctx.__declaredTransports)
                ? ctx.__declaredTransports.map((transport: any) => ({
                    uuid: transport.uuid,
                    service_instance_id: ctx.__serviceInstanceId,
                    role: transport.role,
                    origin: transport.origin,
                    protocols: transport.protocols ?? ["rest", "socket"],
                    ...(transport.securityProfile
                      ? { security_profile: transport.securityProfile }
                      : {}),
                    ...(transport.authStrategy
                      ? { auth_strategy: transport.authStrategy }
                      : {}),
                  }))
                : [];
              emit("meta.service_registry.instance_registration_requested", {
                ...ctx,
                data: {
                  uuid: ctx.__serviceInstanceId,
                  process_pid: 1,
                  service_name: ctx.__serviceName,
                  is_frontend: true,
                  is_active: true,
                  is_non_responsive: false,
                  is_blocked: false,
                  health: {},
                },
                __registrationData: {
                  uuid: ctx.__serviceInstanceId,
                  process_pid: 1,
                  service_name: ctx.__serviceName,
                  is_frontend: true,
                  is_active: true,
                  is_non_responsive: false,
                  is_blocked: false,
                  health: {},
                },
                __transportData: browserTransportData,
              });
              if (browserTransportData.length > 0) {
                Cadenza.schedule(
                  "meta.service_registry.transport_registration_ensure_requested",
                  {
                    ...ctx,
                    __transportData: browserTransportData,
                    __serviceName: ctx.__serviceName,
                    __serviceInstanceId: ctx.__serviceInstanceId,
                  },
                  250,
                );
              }
              return;
            }

            console.log("Service inserted...");

            const app = express();
            app.use(
              bodyParser.json({
                limit: this.resolveJsonBodyLimit(),
              }),
            );

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
          .then(
            Cadenza.createMetaTask(
              "Define RestServer",
              (ctx) => {
                const app = ctx.__app;

                // TODO: add body validation based on profile

                app.post("/handshake", (req: any, res: any) => {
                  try {
                    const handshakePayload =
                      req.body && typeof req.body === "object" ? req.body : {};

                    if (
                      Object.keys(handshakePayload).length > 0 &&
                      (process.env.CADENZA_FETCH_CONNECTION_DEBUG === "1" ||
                        process.env.CADENZA_FETCH_CONNECTION_DEBUG === "true")
                    ) {
                      Cadenza.log("New fetch connection.", handshakePayload);
                    }

                    Cadenza.emit("meta.rest.handshake", handshakePayload);
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

                app.post("/delegation", (req: any, res: any) => {
                  const ctx = ensureDelegationContextMetadata(
                    attachDelegationRequestSnapshot(req.body),
                  );
                  const deputyExecId = ctx.__metadata.__deputyExecId;
                  const remoteRoutineName = ctx.__remoteRoutineName;
                  const targetNotFoundSignal = `meta.rest.delegation_target_not_found:${deputyExecId}`;
                  let resolved = false;

                  if (
                    (process.env.CADENZA_INSTANCE_DEBUG === "1" ||
                      process.env.CADENZA_INSTANCE_DEBUG === "true") &&
                    remoteRoutineName === "Insert service_instance"
                  ) {
                    console.log("[CADENZA_INSTANCE_DEBUG] rest_delegation_ingress", {
                      localServiceName: Cadenza.serviceRegistry.serviceName,
                      sourceServiceName:
                        ctx.__localServiceName ?? ctx.__metadata?.__localServiceName ?? null,
                      deputyExecId,
                      dataKeys:
                        ctx.data && typeof ctx.data === "object"
                          ? Object.keys(ctx.data)
                          : [],
                      queryDataKeys:
                        ctx.queryData && typeof ctx.queryData === "object"
                          ? Object.keys(ctx.queryData)
                          : [],
                      queryDataDataKeys:
                        ctx.queryData?.data &&
                        typeof ctx.queryData.data === "object"
                          ? Object.keys(ctx.queryData.data as AnyObject)
                          : [],
                    });
                  }

                  if (
                    ACTOR_SESSION_TRACE_ENABLED &&
                    remoteRoutineName === "Insert actor_session_state"
                  ) {
                    const queryData =
                      ctx.queryData && typeof ctx.queryData === "object"
                        ? (ctx.queryData as AnyObject)
                        : null;
                    const data =
                      ctx.data && typeof ctx.data === "object" && !Array.isArray(ctx.data)
                        ? (ctx.data as AnyObject)
                        : null;

                    console.log("[CADENZA_ACTOR_SESSION_TRACE] delegation_ingress", {
                      localServiceName: Cadenza.serviceRegistry.serviceName,
                      sourceServiceName:
                        ctx.__localServiceName ?? ctx.__metadata?.__localServiceName ?? null,
                      remoteRoutineName,
                      deputyExecId:
                        ctx.__deputyExecId ?? ctx.__metadata?.__deputyExecId ?? null,
                      inquirySourceTaskExecutionId:
                        ctx.__inquirySourceTaskExecutionId ??
                        ctx.__metadata?.__inquirySourceTaskExecutionId ??
                        null,
                      inquirySourceRoutineExecutionId:
                        ctx.__inquirySourceRoutineExecutionId ??
                        ctx.__metadata?.__inquirySourceRoutineExecutionId ??
                        null,
                      inquiryName:
                        ctx.__inquiryName ?? ctx.__metadata?.__inquiryName ?? null,
                      hasData: data !== null,
                      dataKeys: data ? Object.keys(data) : [],
                      hasQueryData: queryData !== null,
                      queryDataKeys: queryData ? Object.keys(queryData) : [],
                      hasQueryDataData:
                        !!(
                          queryData &&
                          queryData.data &&
                          typeof queryData.data === "object" &&
                          !Array.isArray(queryData.data)
                        ),
                      queryDataDataKeys:
                        queryData &&
                        queryData.data &&
                        typeof queryData.data === "object" &&
                        !Array.isArray(queryData.data)
                          ? Object.keys(queryData.data as AnyObject)
                          : [],
                      rootKeys: Object.keys(ctx ?? {}).slice(0, 24),
                    });
                  }

                  const resolveDelegation = (
                    endCtx: AnyObject,
                    status: "success" | "error",
                  ) => {
                    if (resolved || res.headersSent) {
                      return;
                    }

                    resolved = true;

                    const metadata =
                      endCtx?.__metadata && typeof endCtx.__metadata === "object"
                        ? endCtx.__metadata
                        : {};
                    if (endCtx?.__metadata) {
                      delete endCtx.__metadata;
                    }

                    res.json({
                      ...endCtx,
                      ...metadata,
                      __status: status,
                    });
                  };

                  Cadenza.createEphemeralMetaTask(
                    "Resolve delegation",
                    (endCtx) =>
                      resolveDelegation(
                        endCtx,
                        endCtx?.errored || endCtx?.failed ? "error" : "success",
                      ),
                    "Resolves a delegation request",
                    { register: false },
                  )
                    .doOn(
                      `meta.node.graph_completed:${deputyExecId}`,
                      targetNotFoundSignal,
                    )
                    .emits(`meta.rest.delegation_resolved:${deputyExecId}`);

                  if (
                    !Cadenza.get(remoteRoutineName) &&
                    !Cadenza.registry.routines.get(remoteRoutineName)
                  ) {
                    Cadenza.emit(targetNotFoundSignal, {
                      ...ctx,
                      __error: `No task or routine registered for delegation target ${remoteRoutineName}.`,
                      errored: true,
                    });
                    return;
                  }

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
                  Cadenza.emit("meta.service_registry.instance_activity_observed", {
                    serviceName: Cadenza.serviceRegistry.serviceName,
                    serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
                    activityAt: new Date().toISOString(),
                    source: "rest-delegation",
                  });
                });

                app.post("/signal", (req: any, res: any) => {
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

                    Cadenza.emit("meta.service_registry.instance_activity_observed", {
                      serviceName: Cadenza.serviceRegistry.serviceName,
                      serviceInstanceId: Cadenza.serviceRegistry.serviceInstanceId,
                      activityAt: new Date().toISOString(),
                      source: "rest-signal",
                    });
                    Cadenza.emit(ctx.__signalName, {
                      ...ctx,
                      __receivedSignalTransmission: true,
                  });
                });

                app.get("/status", (req: any, res: any) => {
                  const statusCheckQuery =
                    req?.body?.query && typeof req.body.query === "object"
                      ? req.body.query
                      : req?.query && typeof req.query === "object"
                        ? { ...req.query }
                        : {};

                  res.json(
                    Cadenza.serviceRegistry.resolveLocalStatusCheck(
                      statusCheckQuery,
                    ),
                  );
                });

                app.use(
                  (error: any, _req: any, res: any, next: any) => {
                    if (!error) {
                      next();
                      return;
                    }

                    const statusCode =
                      typeof error.statusCode === "number"
                        ? error.statusCode
                        : typeof error.status === "number"
                          ? error.status
                          : error.type === "entity.too.large"
                            ? 413
                            : 500;
                    const message =
                      error.type === "entity.too.large"
                        ? `Request payload exceeded REST body limit ${this.resolveJsonBodyLimit()}.`
                        : this.getErrorMessage(error);

                    Cadenza.log(
                      "REST request failed before route completion.",
                      {
                        error: message,
                        type: error.type,
                        statusCode,
                        path: _req?.path,
                        method: _req?.method,
                      },
                      statusCode >= 500 ? "error" : "warning",
                    );

                    res.status(statusCode).json({
                      __status: "error",
                      errored: true,
                      __error: message,
                    });
                  },
                );

                return true;
              },
              "Starts REST server and initiates meta-handling",
            )
              .then(
                Cadenza.createMetaTask(
                  "Configure network",
                  async (ctx) => {
                    let httpOrigin: string | null = null;
                    let httpsOrigin: string | null = null;

                    const resolveBoundAddress = (server: any): string => {
                      if (typeof server?.address() === "string") {
                        return server.address() as string;
                      }

                      if (server?.address()?.address === "::") {
                        if (process.env.NODE_ENV === "development") {
                          return "localhost";
                        }

                        if (process.env.IS_DOCKER === "true") {
                          return process.env.CADENZA_SERVER_URL || "localhost";
                        }
                      }

                      return server?.address()?.address || "localhost";
                    };

                    const createHttpServer = async (ctx: any) => {
                      await new Promise((resolve) => {
                        const server = http.createServer(ctx.__app);
                        ctx.httpServer = server;
                        server.listen(ctx.__port, () => {
                          const addressInfo = server.address();
                          const address = resolveBoundAddress(server);
                          const port =
                            typeof addressInfo === "object" && addressInfo
                              ? addressInfo.port || ctx.__port
                              : ctx.__port;
                          httpOrigin = `http://${address}:${port}`;

                          console.log(`Server is running on ${httpOrigin}`);
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
                        httpsServer.listen(443, () => {
                          const addressInfo = httpsServer.address();
                          const address = resolveBoundAddress(httpsServer);
                          const port =
                            typeof addressInfo === "object" && addressInfo
                              ? addressInfo.port || 443
                              : 443;
                          httpsOrigin = `https://${address}:${port}`;

                          console.log(`HTTPS Server is running on ${httpsOrigin}`);
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

                    const declaredTransports = Array.isArray(ctx.__declaredTransports)
                      ? ctx.__declaredTransports
                      : [];
                    const hasExplicitInternalTransport = declaredTransports.some(
                      (transport: any) => transport.role === "internal",
                    );
                    const transportData = declaredTransports.map((transport: any) => ({
                      uuid: transport.uuid,
                      service_instance_id: ctx.__serviceInstanceId,
                      role: transport.role,
                      origin: transport.origin,
                      protocols: transport.protocols ?? ["rest", "socket"],
                      ...(transport.securityProfile
                        ? { security_profile: transport.securityProfile }
                        : {}),
                      ...(transport.authStrategy
                        ? { auth_strategy: transport.authStrategy }
                        : {}),
                    }));

                    if (!hasExplicitInternalTransport) {
                      const internalOrigin = httpOrigin ?? httpsOrigin;
                      if (internalOrigin) {
                        transportData.unshift({
                          uuid: uuid(),
                          service_instance_id: ctx.__serviceInstanceId,
                          role: "internal",
                          origin: internalOrigin,
                          protocols: ctx.__useSocket ? ["rest", "socket"] : ["rest"],
                          ...(ctx.__securityProfile
                            ? { security_profile: ctx.__securityProfile }
                            : {}),
                        });
                      }
                    }

                    ctx.data = {
                      uuid: ctx.__serviceInstanceId,
                      process_pid: process.pid,
                      service_name: ctx.__serviceName,
                      is_active: true,
                      is_database: ctx.__isDatabase,
                      is_non_responsive: false,
                      is_blocked: false,
                      health: {},
                    };
                    ctx.__registrationData = {
                      ...ctx.data,
                    };
                    ctx.__transportData = transportData;

                    if (
                      process.env.CADENZA_INSTANCE_DEBUG === "1" ||
                      process.env.CADENZA_INSTANCE_DEBUG === "true"
                    ) {
                      console.log("[CADENZA_INSTANCE_DEBUG] configure_network_emit", {
                        serviceName: ctx.__serviceName,
                        serviceInstanceId: ctx.__serviceInstanceId,
                        isDatabase: ctx.__isDatabase === true,
                        transportCount: transportData.length,
                        transports: transportData.map((transport) => ({
                          role: transport.role,
                          origin: transport.origin,
                          protocols: transport.protocols,
                        })),
                      });
                    }

                    delete ctx.__app;

                    Cadenza.emit(
                      "meta.service_registry.instance_registration_requested",
                      ctx,
                    );
                    if (transportData.length > 0) {
                      Cadenza.schedule(
                        "meta.service_registry.transport_registration_ensure_requested",
                        {
                          ...ctx,
                          __transportData: transportData,
                          __serviceName: ctx.__serviceName,
                          __serviceInstanceId: ctx.__serviceInstanceId,
                        },
                        250,
                      );
                    }

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
                            const remoteRoutineName =
                              context.__remoteRoutineName ??
                              context.__name ??
                              "unknown";
                            const routine =
                              Cadenza.get(remoteRoutineName) ??
                              Cadenza.registry.routines.get(remoteRoutineName);

                            if (routine) {
                              context.__routineExecId =
                                context.__metadata?.__deputyExecId ?? null;
                              context.__isDeputy = true;
                              Cadenza.runner.run(routine, context);
                              return true;
                            } else {
                              const deputyExecId =
                                context.__metadata?.__deputyExecId ??
                                context.__deputyExecId;
                              context.errored = true;
                              context.__error = `No task or routine registered for delegation target ${remoteRoutineName}.`;
                              if (deputyExecId) {
                                emit(
                                  `meta.rest.delegation_target_not_found:${deputyExecId}`,
                                  context,
                                );
                              }
                              emit("meta.runner.failed", context);
                              return false;
                            }
                          },
                          "Forward delegations to runner",
                        )
                          .attachSignal("meta.runner.failed")
                          .doOn("meta.rest.delegation_requested");
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
        const serviceName = String(ctx.serviceName ?? "");
        const URL = String(ctx.serviceOrigin ?? "");
        const routeKey = String(
          ctx.routeKey ?? ctx.__routeKey ?? ctx.serviceTransportId ?? "",
        );
        const fetchId = String(
          ctx.fetchId ??
            ctx.__fetchId ??
            (routeKey ? buildTransportHandleKey(routeKey, "rest") : "") ??
            ctx.serviceTransportId ??
            "",
        );
        if (!serviceName || !URL || !fetchId) {
          return false;
        }
        const clientTaskSuffix = `${URL} (${fetchId})`;
        const fetchDiagnostics = this.ensureFetchClientDiagnostics(
          fetchId,
          serviceName,
          URL,
        );

        if (Cadenza.get(`Send Handshake to ${clientTaskSuffix}`)) {
          const shouldRetryHandshake =
            ctx.__forceHandshakeRecovery === true ||
            fetchDiagnostics.destroyed === true ||
            ((fetchDiagnostics.connected !== true ||
              typeof fetchDiagnostics.lastHandshakeError === "string") &&
              fetchDiagnostics.handshakeInFlight !== true);

          if (shouldRetryHandshake) {
            fetchDiagnostics.destroyed = false;
            fetchDiagnostics.handshakeInFlight = true;
            fetchDiagnostics.updatedAt = Date.now();
            console.error("Fetch client already exists", { URL, fetchId });
            Cadenza.debounce(
              `meta.fetch.handshake_requested:${fetchId}`,
              {
                serviceInstanceId: ctx.serviceInstanceId,
                serviceName,
                communicationTypes: ctx.communicationTypes,
                serviceTransportId: ctx.serviceTransportId,
                serviceOrigin: URL,
                fetchId,
                routeKey,
                socketClientId:
                  ctx.socketClientId ??
                  (routeKey
                    ? buildTransportHandleKey(routeKey, "socket")
                    : undefined),
                transportProtocols: ctx.transportProtocols,
                transportProtocol: "rest",
                handshakeData: ctx.handshakeData,
              },
              50,
            );
          }
          return true;
        }

        fetchDiagnostics.destroyed = false;
        fetchDiagnostics.handshakeInFlight = false;
        fetchDiagnostics.updatedAt = Date.now();

        const handshakeTask = Cadenza.createMetaTask(
          `Send Handshake to ${clientTaskSuffix}`,
          async (ctx, emit) => {
            fetchDiagnostics.handshakeInFlight = true;
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
                FETCH_HANDSHAKE_TIMEOUT_MS,
              );
              if (response.__status !== "success") {
                const error =
                  response.__error ??
                  `Failed to connect to service ${serviceName} ${ctx.serviceInstanceId}`;
                fetchDiagnostics.connected = false;
                fetchDiagnostics.handshakeInFlight = false;
                fetchDiagnostics.lastHandshakeError = error;
                fetchDiagnostics.updatedAt = Date.now();
                this.recordFetchClientError(fetchId, serviceName, URL, error);
                Cadenza.log(
                  "Fetch handshake failed.",
                  { error, serviceName, URL },
                  "warning",
                );
                emit(`meta.fetch.handshake_failed:${fetchId}`, {
                  ...ctx,
                  ...response,
                  fetchId,
                  routeKey,
                });
                return { ...ctx, __error: error, errored: true };
              }

              ctx.serviceInstanceId = response.__serviceInstanceId;
              fetchDiagnostics.connected = true;
              fetchDiagnostics.destroyed = false;
              fetchDiagnostics.handshakeInFlight = false;
              fetchDiagnostics.lastHandshakeAt = new Date().toISOString();
              fetchDiagnostics.lastHandshakeError = null;
              fetchDiagnostics.updatedAt = Date.now();

              Cadenza.log("Fetch client connected.", {
                response,
                serviceName,
                URL,
              });

              const localServiceInstanceId =
                Cadenza.serviceRegistry.serviceInstanceId;
              if (!ctx.serviceInstanceId || !localServiceInstanceId) {
                return {
                  ...ctx,
                  __error: "Fetch handshake missing service instance id",
                  errored: true,
                };
              }

              for (const communicationType of ctx.communicationTypes) {
                emit(
                  "global.meta.fetch.service_communication_established",
                  buildServiceCommunicationEstablishedContext({
                    serviceInstanceId: ctx.serviceInstanceId,
                    serviceInstanceClientId: localServiceInstanceId,
                    communicationType,
                  }),
                );
              }
            } catch (e) {
              fetchDiagnostics.connected = false;
              fetchDiagnostics.handshakeInFlight = false;
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
          {
            retryCount: 5,
            retryDelay: 1000,
            retryDelayFactor: 1.5,
            register: false,
            isHidden: true,
          },
        )
          .doOn(`meta.fetch.handshake_requested:${fetchId}`)
          .emits("meta.fetch.handshake_complete")
          .attachSignal(
            "meta.fetch.handshake_failed",
            "global.meta.fetch.service_communication_established",
          );

        const delegateTask = Cadenza.createMetaTask(
          `Delegate flow to REST server ${clientTaskSuffix}`,
          async (ctx, emit) => {
            if (ctx.__remoteRoutineName === undefined) {
              return;
            }

            const routedDelegateCtx = stripTransportSelectionRoutingContext(ctx);
            const delegateCtx = ensureDelegationContextMetadata(
              restoreDelegationRequestSnapshot(
                attachDelegationRequestSnapshot(routedDelegateCtx),
              ),
            );
            const deputyExecId = delegateCtx.__metadata.__deputyExecId;
            const requestBody = stripDelegationRequestSnapshot(delegateCtx);

            fetchDiagnostics.delegationRequests++;
            fetchDiagnostics.updatedAt = Date.now();

            let resultContext;
            let routeOutcome: "success" | "failure" | "neutral" = "neutral";
            try {
              resultContext = await this.fetchDataWithTimeout(
                `${URL}/delegation`,
                {
                  headers: {
                    "Content-Type": "application/json",
                  },
                  method: "POST",
                  body: JSON.stringify(requestBody),
                },
                this.resolveDelegationTimeoutMs(delegateCtx),
              );
              if (
                resultContext &&
                typeof resultContext === "object" &&
                resultContext.__delegationRequestContext === undefined &&
                delegateCtx.__delegationRequestContext !== undefined
              ) {
                resultContext = {
                  ...resultContext,
                  __delegationRequestContext: delegateCtx.__delegationRequestContext,
                };
              }
              if (resultContext?.errored || resultContext?.failed) {
                fetchDiagnostics.delegationFailures++;
                fetchDiagnostics.updatedAt = Date.now();
                this.recordFetchClientError(
                  fetchId,
                  serviceName,
                  URL,
                  resultContext?.__error ?? resultContext?.error ?? "Delegation failed",
                );
              } else {
                const serviceInstanceId = String(
                  delegateCtx.__instance ?? delegateCtx.targetServiceInstanceId ?? "",
                ).trim();
                if (serviceName && serviceInstanceId) {
                  emit("meta.service_registry.remote_activity_observed", {
                    serviceName,
                    serviceInstanceId,
                    serviceTransportId: String(delegateCtx.__transportId ?? "").trim(),
                    serviceOrigin: String(delegateCtx.__transportOrigin ?? "").trim(),
                    transportProtocols: Array.isArray(
                      delegateCtx.__transportProtocols,
                    )
                      ? delegateCtx.__transportProtocols
                      : [],
                    activityAt: new Date().toISOString(),
                    source: "rest-delegation-success",
                  });
                }
                routeOutcome = "success";
              }
            } catch (e) {
              console.error("Error in delegation", e);
              // TODO: Retry on too many requests
              fetchDiagnostics.delegationFailures++;
              fetchDiagnostics.updatedAt = Date.now();
              this.recordFetchClientError(fetchId, serviceName, URL, e);
              resultContext = buildDelegationFailureContext(
                "meta.fetch.delegate_failed",
                delegateCtx,
                e,
              );
              routeOutcome = "failure";
              emit("meta.fetch.delegate_failed", resultContext);
            } finally {
              Cadenza.serviceRegistry.recordBalancedRouteOutcome(
                (resultContext ?? delegateCtx) as AnyObject,
                routeOutcome,
              );
              emit(`meta.fetch.delegated:${deputyExecId}`, resultContext);
            }

            return resultContext;
          },
          "Sends delegation request",
          {
            register: false,
            isHidden: true,
          },
        )
          .doOn(
            `meta.service_registry.selected_instance_for_fetch:${fetchId}`,
            `meta.service_registry.socket_failed:${fetchId}`,
          )
          .emitsOnFail("meta.fetch.delegate_failed")
          .attachSignal("meta.fetch.delegated");

        const transmitTask = Cadenza.createMetaTask(
          `Transmit signal to server ${clientTaskSuffix}`,
          async (ctx, emit) => {
            if (ctx.__signalName === undefined) {
              return;
            }

            if (
              INQUIRY_TRACE_ENABLED &&
              serviceName === "CadenzaDB" &&
              TRACED_INQUIRY_METADATA_SIGNALS.has(String(ctx.__signalName))
            ) {
              console.log("[CADENZA_INQUIRY_TRACE] rest_transmit_start", {
                localServiceName: Cadenza.serviceRegistry.serviceName,
                targetServiceName: serviceName,
                signalName: ctx.__signalName,
                inquiryName:
                  ctx?.data?.name ??
                  ctx?.data?.metadata?.inquiryMeta?.inquiry ??
                  null,
                inquiryId: ctx?.data?.uuid ?? ctx?.filter?.uuid ?? null,
              });
            }

            fetchDiagnostics.signalTransmissions++;
            fetchDiagnostics.updatedAt = Date.now();

            let response;
            let routeOutcome: "success" | "failure" | "neutral" = "neutral";
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
                this.resolveSignalTransmissionTimeoutMs(serviceName, ctx),
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
              } else {
                routeOutcome = "success";
              }

              if (
                INQUIRY_TRACE_ENABLED &&
                serviceName === "CadenzaDB" &&
                TRACED_INQUIRY_METADATA_SIGNALS.has(String(ctx.__signalName))
              ) {
                console.log("[CADENZA_INQUIRY_TRACE] rest_transmit_result", {
                  localServiceName: Cadenza.serviceRegistry.serviceName,
                  targetServiceName: serviceName,
                  signalName: ctx.__signalName,
                  errored: response?.errored === true,
                  error: response?.__error ?? response?.error ?? null,
                });
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
              routeOutcome = "failure";

              if (
                INQUIRY_TRACE_ENABLED &&
                serviceName === "CadenzaDB" &&
                TRACED_INQUIRY_METADATA_SIGNALS.has(String(ctx.__signalName))
              ) {
                console.log("[CADENZA_INQUIRY_TRACE] rest_transmit_error", {
                  localServiceName: Cadenza.serviceRegistry.serviceName,
                  targetServiceName: serviceName,
                  signalName: ctx.__signalName,
                  error: String(e),
                });
              }
            }

            Cadenza.serviceRegistry.recordBalancedRouteOutcome(
              (response ?? ctx) as AnyObject,
              routeOutcome,
            );

            return response;
          },
          "Sends signal request",
          {
            concurrency: this.resolveSignalTransmissionConcurrency(serviceName),
            register: false,
            isHidden: true,
          },
        )
          .doOn(
            `meta.service_registry.selected_instance_for_fetch:${fetchId}`,
            `meta.signal_controller.remote_signal_registered:${serviceName}`,
            "meta.signal_controller.wildcard_signal_registered",
          )
          .emitsOnFail("meta.fetch.signal_transmission_failed")
          .attachSignal("meta.fetch.transmitted");

        const statusTask = Cadenza.createMetaTask(
          `Request status from ${clientTaskSuffix}`,
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
          {
            register: false,
            isHidden: true,
          },
        )
          .doOn("meta.fetch.status_check_requested")
          .emits("meta.fetch.status_checked")
          .emitsOnFail("meta.fetch.status_check_failed");

        Cadenza.createEphemeralMetaTask(
          `Destroy fetch client ${fetchId}`,
          (ctx) => {
            if (
              !Cadenza.serviceRegistry.shouldProcessRemoteRouteEvent({
                ...ctx,
                fetchId,
                routeKey,
              })
            ) {
              return false;
            }

            fetchDiagnostics.connected = false;
            fetchDiagnostics.handshakeInFlight = false;
            fetchDiagnostics.destroyed = true;
            fetchDiagnostics.updatedAt = Date.now();
            Cadenza.log("Destroying fetch client", { URL, serviceName });
            handshakeTask.destroy();
            delegateTask.destroy();
            transmitTask.destroy();
            statusTask.destroy();
            return true;
          },
          "",
          {
            register: false,
            isHidden: true,
          },
        )
          .doOn(
            `meta.fetch.destroy_requested:${fetchId}`,
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
          (ctx) => {
            const {
              serviceName,
              serviceInstanceId,
              communicationTypes,
              serviceTransportId,
              serviceOrigin,
              transportProtocols,
            } = ctx;

            const routeKey = String(
              ctx.routeKey ?? ctx.__routeKey ?? serviceTransportId ?? "",
            );
            const fetchId = String(
              ctx.fetchId ??
                ctx.__fetchId ??
                (routeKey
                  ? buildTransportHandleKey(routeKey, "rest")
                  : serviceTransportId ?? ""),
            );

            const fetchDiagnostics = this.ensureFetchClientDiagnostics(
              fetchId,
              String(serviceName ?? ""),
              String(serviceOrigin ?? ""),
            );
            if (fetchDiagnostics.handshakeInFlight !== true) {
              fetchDiagnostics.handshakeInFlight = true;
            }

            Cadenza.debounce(`meta.fetch.handshake_requested:${fetchId}`, {
              serviceInstanceId,
              serviceName,
              communicationTypes,
              serviceTransportId,
              serviceOrigin,
              fetchId,
              routeKey,
              socketClientId:
                ctx.socketClientId ??
                (routeKey ? buildTransportHandleKey(routeKey, "socket") : undefined),
              transportProtocols,
              transportProtocol: "rest",
              handshakeData: {
                instanceId: Cadenza.serviceRegistry.serviceInstanceId,
                serviceName: Cadenza.serviceRegistry.serviceName,
                // JWT token...
              },
            }, 50);
            return true;
          },
          "Prepares handshake",
        ).attachSignal("meta.fetch.handshake_requested"),
      )
      .doOn("meta.service_registry.dependee_registered")
      .emitsOnFail("meta.fetch.connect_failed");
  }
}
