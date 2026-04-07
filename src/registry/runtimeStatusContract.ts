import type { AnyObject } from "@cadenza.io/core";
import type { RuntimeStatusState } from "../utils/runtimeStatus";
import type {
  ServiceTransportProtocol,
  ServiceTransportRole,
} from "../types/transport";

export const AUTHORITY_RUNTIME_STATUS_REPORT_INTENT =
  "meta-service-registry-authority-runtime-status-report";
export const RUNTIME_STATUS_AUTHORITY_SYNC_REQUESTED_SIGNAL =
  "meta.service_registry.runtime_status_authority_sync_requested";

export interface RuntimeMetricsHealthDetail {
  sampledAt?: string;
  cpuUsage?: number | null;
  memoryUsage?: number | null;
  eventLoopLag?: number | null;
  rssBytes?: number | null;
  heapUsedBytes?: number | null;
  heapTotalBytes?: number | null;
  memoryLimitBytes?: number | null;
}

export interface AuthorityRuntimeStatusReport {
  serviceName: string;
  serviceInstanceId: string;
  transportId?: string;
  transportRole?: ServiceTransportRole;
  transportOrigin?: string;
  transportProtocols?: ServiceTransportProtocol[];
  isFrontend?: boolean;
  reportedAt: string;
  state: RuntimeStatusState;
  acceptingWork: boolean;
  numberOfRunningGraphs: number;
  cpuUsage?: number | null;
  memoryUsage?: number | null;
  eventLoopLag?: number | null;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
  health?: AnyObject;
}

function normalizeOptionalMetric(value: unknown): number | null | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (value === null) {
    return null;
  }
  const normalized = Number(value);
  return Number.isFinite(normalized) ? normalized : undefined;
}

function resolveHealthMetric(
  input: Record<string, any>,
  keys: string[],
): number | null | undefined {
  for (const key of keys) {
    const direct = normalizeOptionalMetric(input[key]);
    if (direct !== undefined) {
      return direct;
    }
  }

  const health =
    input.health && typeof input.health === "object"
      ? (input.health as Record<string, unknown>)
      : null;
  if (!health) {
    return undefined;
  }

  for (const key of keys) {
    const direct = normalizeOptionalMetric(health[key]);
    if (direct !== undefined) {
      return direct;
    }
  }

  const runtimeMetrics =
    health.runtimeMetrics && typeof health.runtimeMetrics === "object"
      ? (health.runtimeMetrics as Record<string, unknown>)
      : null;
  if (!runtimeMetrics) {
    return undefined;
  }

  for (const key of keys) {
    const direct = normalizeOptionalMetric(runtimeMetrics[key]);
    if (direct !== undefined) {
      return direct;
    }
  }

  return undefined;
}

export function normalizeAuthorityRuntimeStatusReport(
  input: Record<string, any> | null | undefined,
): AuthorityRuntimeStatusReport | null {
  if (!input || typeof input !== "object") {
    return null;
  }

  const serviceName = String(input.serviceName ?? input.__serviceName ?? "").trim();
  const serviceInstanceId = String(
    input.serviceInstanceId ?? input.__serviceInstanceId ?? "",
  ).trim();
  const reportedAt = String(
    input.reportedAt ?? input.__reportedAt ?? new Date().toISOString(),
  ).trim();
  const state = String(input.state ?? "").trim();

  if (
    !serviceName ||
    !serviceInstanceId ||
    !reportedAt ||
    (state !== "healthy" &&
      state !== "degraded" &&
      state !== "overloaded" &&
      state !== "unavailable")
  ) {
    return null;
  }

  const transportRole = String(
    input.transportRole ?? input.transport_role ?? "",
  ).trim();
  const transportOrigin = String(
    input.transportOrigin ?? input.transport_origin ?? "",
  ).trim();
  const transportId = String(input.transportId ?? input.transport_id ?? "").trim();
  const transportProtocolList = Array.isArray(
    input.transportProtocols ?? input.transport_protocols,
  )
    ? (input.transportProtocols ?? input.transport_protocols).filter(
        (protocol: unknown): protocol is ServiceTransportProtocol =>
          protocol === "rest" || protocol === "socket",
      )
    : [];
  const transportProtocols =
    transportProtocolList.length > 0
      ? (Array.from(new Set(transportProtocolList)) as ServiceTransportProtocol[])
      : undefined;

  return {
    serviceName,
    serviceInstanceId,
    transportId: transportId || undefined,
    transportRole:
      transportRole === "internal" || transportRole === "public"
        ? transportRole
        : undefined,
    transportOrigin: transportOrigin || undefined,
    transportProtocols:
      transportProtocols && transportProtocols.length > 0
        ? transportProtocols
        : undefined,
    isFrontend:
      typeof input.isFrontend === "boolean"
        ? input.isFrontend
        : typeof input.is_frontend === "boolean"
          ? input.is_frontend
          : undefined,
    reportedAt,
    state: state as RuntimeStatusState,
    acceptingWork: Boolean(input.acceptingWork ?? input.accepting_work),
    numberOfRunningGraphs: Math.max(
      0,
      Math.trunc(
        Number(
          input.numberOfRunningGraphs ??
            input.number_of_running_graphs ??
            input.__numberOfRunningGraphs ??
            0,
        ) || 0,
      ),
    ),
    cpuUsage: resolveHealthMetric(input, ["cpuUsage", "cpu", "cpuLoad"]),
    memoryUsage: resolveHealthMetric(input, [
      "memoryUsage",
      "memory",
      "memoryPressure",
    ]),
    eventLoopLag: resolveHealthMetric(input, [
      "eventLoopLag",
      "eventLoopLagMs",
    ]),
    isActive: Boolean(input.isActive ?? input.is_active ?? true),
    isNonResponsive: Boolean(
      input.isNonResponsive ?? input.is_non_responsive ?? false,
    ),
    isBlocked: Boolean(input.isBlocked ?? input.is_blocked ?? false),
    health:
      input.health && typeof input.health === "object"
        ? (input.health as AnyObject)
        : undefined,
  };
}

export function buildAuthorityRuntimeStatusSignature(
  report: AuthorityRuntimeStatusReport,
): string {
  return JSON.stringify({
    serviceName: report.serviceName,
    serviceInstanceId: report.serviceInstanceId,
    transportId: report.transportId ?? null,
    transportRole: report.transportRole ?? null,
    transportOrigin: report.transportOrigin ?? null,
    transportProtocols: report.transportProtocols ?? [],
    state: report.state,
    acceptingWork: report.acceptingWork,
    numberOfRunningGraphs: report.numberOfRunningGraphs,
    cpuUsage: report.cpuUsage ?? null,
    memoryUsage: report.memoryUsage ?? null,
    eventLoopLag: report.eventLoopLag ?? null,
    isActive: report.isActive,
    isNonResponsive: report.isNonResponsive,
    isBlocked: report.isBlocked,
    isFrontend: report.isFrontend ?? null,
    health: report.health ?? {},
  });
}
