import Cadenza from "../Cadenza";
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

    return {
      transportDiagnostics: {
        [serviceName]: {
          fetchClient: {
            ...summary,
            clients: states.map((state) => {
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
            }),
          },
        },
      },
    };
  }

  fetchDataWithTimeout = async function (
    url: string,
    requestInit: RequestInit,
    timeoutMs: number,
  ): Promise<any> {
    if (typeof globalThis.fetch !== "function") {
      throw new Error("Browser REST controller requires global fetch.");
    }

    const signal = AbortSignal.timeout(timeoutMs);
    const response = await globalThis.fetch(url, { ...requestInit, signal });
    return await response.json();
  };

  constructor() {
    Cadenza.createMetaTask(
      "Collect fetch transport diagnostics",
      (ctx) => this.collectFetchTransportDiagnostics(ctx),
      "Responds to distributed transport diagnostics inquiries with REST/fetch client data.",
    ).respondsTo(META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT);

    Cadenza.createMetaTask(
      "Declare browser network",
      (ctx, emit) => {
        emit("meta.service_registry.instance_registration_requested", {
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
          transportData: [],
          ...ctx,
        });

        return true;
      },
      "Declares frontend runtime network metadata without creating a server.",
    ).doOn("meta.service_registry.service_inserted");

    Cadenza.createMetaTask(
      "Setup fetch client",
      (ctx) => {
        const serviceName = String(ctx.serviceName ?? "");
        const URL = String(ctx.serviceOrigin ?? "");
        const fetchId = String(ctx.serviceTransportId ?? "");
        if (!serviceName || !URL || !fetchId) {
          return false;
        }
        const fetchDiagnostics = this.ensureFetchClientDiagnostics(
          fetchId,
          serviceName,
          URL,
        );
        fetchDiagnostics.destroyed = false;
        fetchDiagnostics.updatedAt = Date.now();

        if (Cadenza.get(`Send Handshake to ${URL}`)) {
          return;
        }

        const handshakeTask = Cadenza.createMetaTask(
          `Send Handshake to ${URL}`,
          async (handshakeCtx, emit) => {
            try {
              const response = await this.fetchDataWithTimeout(
                `${URL}/handshake`,
                {
                  headers: {
                    "Content-Type": "application/json",
                  },
                  method: "POST",
                  body: JSON.stringify(handshakeCtx.handshakeData),
                },
                1000,
              );
              if (response.__status !== "success") {
                const error =
                  response.__error ??
                  `Failed to connect to service ${serviceName} ${handshakeCtx.serviceInstanceId}`;
                fetchDiagnostics.connected = false;
                fetchDiagnostics.lastHandshakeError = error;
                fetchDiagnostics.updatedAt = Date.now();
                this.recordFetchClientError(fetchId, serviceName, URL, error);
                emit(`meta.fetch.handshake_failed:${fetchId}`, response);
                return { ...handshakeCtx, __error: error, errored: true };
              }

              handshakeCtx.serviceInstanceId = response.__serviceInstanceId;
              fetchDiagnostics.connected = true;
              fetchDiagnostics.destroyed = false;
              fetchDiagnostics.lastHandshakeAt = new Date().toISOString();
              fetchDiagnostics.lastHandshakeError = null;
              fetchDiagnostics.updatedAt = Date.now();

              for (const communicationType of handshakeCtx.communicationTypes) {
                emit("global.meta.fetch.service_communication_established", {
                  data: {
                    serviceInstanceId: handshakeCtx.serviceInstanceId,
                    serviceInstanceClientId:
                      Cadenza.serviceRegistry.serviceInstanceId,
                    communicationType,
                  },
                });
              }
            } catch (error) {
              fetchDiagnostics.connected = false;
              fetchDiagnostics.lastHandshakeError = this.getErrorMessage(error);
              fetchDiagnostics.updatedAt = Date.now();
              this.recordFetchClientError(fetchId, serviceName, URL, error);
              return { ...handshakeCtx, __error: error, errored: true };
            }

            return handshakeCtx;
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
          async (delegateCtx, emit) => {
            if (delegateCtx.__remoteRoutineName === undefined) {
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
                  body: JSON.stringify(delegateCtx),
                },
                30_000,
              );
            } catch (error) {
              fetchDiagnostics.delegationFailures++;
              fetchDiagnostics.updatedAt = Date.now();
              this.recordFetchClientError(fetchId, serviceName, URL, error);
              resultContext = {
                __error: `Error: ${error}`,
                errored: true,
                ...delegateCtx,
                ...delegateCtx.__metadata,
              };
            } finally {
              emit(
                `meta.fetch.delegated:${delegateCtx.__metadata.__deputyExecId}`,
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
          async (signalCtx, emit) => {
            if (signalCtx.__signalName === undefined) {
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
                  body: JSON.stringify(signalCtx),
                },
                1000,
              );

              if (signalCtx.__routineExecId) {
                emit(`meta.fetch.transmitted:${signalCtx.__routineExecId}`, response);
              }
            } catch (error) {
              fetchDiagnostics.signalFailures++;
              fetchDiagnostics.updatedAt = Date.now();
              this.recordFetchClientError(fetchId, serviceName, URL, error);
              response = {
                __error: `Error: ${error}`,
                errored: true,
                ...signalCtx,
              };
            }

            return response;
          },
          "Sends signal request",
        )
          .doOn(`meta.service_registry.selected_instance_for_fetch:${fetchId}`)
          .emitsOnFail("meta.fetch.signal_transmission_failed")
          .attachSignal("meta.fetch.transmitted");

        const statusTask = Cadenza.createMetaTask(
          `Request status from ${URL}`,
          async (statusCtx) => {
            fetchDiagnostics.statusChecks++;
            fetchDiagnostics.updatedAt = Date.now();

            try {
              return await this.fetchDataWithTimeout(
                `${URL}/status`,
                {
                  method: "GET",
                },
                1000,
              );
            } catch (error) {
              fetchDiagnostics.statusFailures++;
              fetchDiagnostics.updatedAt = Date.now();
              this.recordFetchClientError(fetchId, serviceName, URL, error);
              return {
                __error: `Error: ${error}`,
                errored: true,
                ...statusCtx,
              };
            }
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
              serviceTransportId,
              serviceOrigin,
              transportProtocols,
            } = ctx;

            const fetchId = String(serviceTransportId ?? "");

            emit(`meta.fetch.handshake_requested:${fetchId}`, {
              serviceInstanceId,
              serviceName,
              communicationTypes,
              serviceTransportId,
              serviceOrigin,
              transportProtocols,
              handshakeData: {
                instanceId: Cadenza.serviceRegistry.serviceInstanceId,
                serviceName: Cadenza.serviceRegistry.serviceName,
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
