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

function sanitizeRuntimeMetricsHealthDetail(
  value: unknown,
): RuntimeMetricsHealthDetail | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }

  const input = value as Record<string, unknown>;
  const sampledAt =
    typeof input.sampledAt === "string" && input.sampledAt.trim().length > 0
      ? input.sampledAt
      : undefined;
  const cpuUsage = normalizeOptionalMetric(input.cpuUsage ?? input.cpu ?? input.cpuLoad);
  const memoryUsage = normalizeOptionalMetric(
    input.memoryUsage ?? input.memory ?? input.memoryPressure,
  );
  const eventLoopLag = normalizeOptionalMetric(
    input.eventLoopLag ?? input.eventLoopLagMs,
  );
  const rssBytes = normalizeOptionalMetric(input.rssBytes);
  const heapUsedBytes = normalizeOptionalMetric(input.heapUsedBytes);
  const heapTotalBytes = normalizeOptionalMetric(input.heapTotalBytes);
  const memoryLimitBytes = normalizeOptionalMetric(input.memoryLimitBytes);

  if (
    sampledAt === undefined &&
    cpuUsage === undefined &&
    memoryUsage === undefined &&
    eventLoopLag === undefined &&
    rssBytes === undefined &&
    heapUsedBytes === undefined &&
    heapTotalBytes === undefined &&
    memoryLimitBytes === undefined
  ) {
    return undefined;
  }

  return {
    ...(sampledAt !== undefined ? { sampledAt } : {}),
    ...(cpuUsage !== undefined ? { cpuUsage } : {}),
    ...(memoryUsage !== undefined ? { memoryUsage } : {}),
    ...(eventLoopLag !== undefined ? { eventLoopLag } : {}),
    ...(rssBytes !== undefined ? { rssBytes } : {}),
    ...(heapUsedBytes !== undefined ? { heapUsedBytes } : {}),
    ...(heapTotalBytes !== undefined ? { heapTotalBytes } : {}),
    ...(memoryLimitBytes !== undefined ? { memoryLimitBytes } : {}),
  };
}

export function sanitizeAuthorityRuntimeStatusHealth(
  health: unknown,
  options?: {
    state?: RuntimeStatusState;
    acceptingWork?: boolean;
    reportedAt?: string;
    cpuUsage?: number | null | undefined;
    memoryUsage?: number | null | undefined;
    eventLoopLag?: number | null | undefined;
  },
): AnyObject | undefined {
  const input =
    health && typeof health === "object"
      ? (health as Record<string, unknown>)
      : ({} as Record<string, unknown>);

  const cpuUsage =
    options?.cpuUsage !== undefined
      ? options.cpuUsage
      : normalizeOptionalMetric(input.cpuUsage ?? input.cpu ?? input.cpuLoad);
  const memoryUsage =
    options?.memoryUsage !== undefined
      ? options.memoryUsage
      : normalizeOptionalMetric(
          input.memoryUsage ?? input.memory ?? input.memoryPressure,
        );
  const eventLoopLag =
    options?.eventLoopLag !== undefined
      ? options.eventLoopLag
      : normalizeOptionalMetric(input.eventLoopLag ?? input.eventLoopLagMs);
  const runtimeMetrics = sanitizeRuntimeMetricsHealthDetail(input.runtimeMetrics);
  const runtimeStatus =
    options?.state ||
    options?.reportedAt ||
    typeof options?.acceptingWork === "boolean"
      ? {
          ...(options?.state ? { state: options.state } : {}),
          ...(typeof options?.acceptingWork === "boolean"
            ? { acceptingWork: options.acceptingWork }
            : {}),
          ...(options?.reportedAt ? { reportedAt: options.reportedAt } : {}),
        }
      : undefined;

  if (
    cpuUsage === undefined &&
    memoryUsage === undefined &&
    eventLoopLag === undefined &&
    !runtimeMetrics &&
    !runtimeStatus
  ) {
    return undefined;
  }

  return {
    ...(cpuUsage !== undefined ? { cpuUsage } : {}),
    ...(memoryUsage !== undefined ? { memoryUsage } : {}),
    ...(eventLoopLag !== undefined ? { eventLoopLag } : {}),
    ...(runtimeMetrics ? { runtimeMetrics } : {}),
    ...(runtimeStatus ? { runtimeStatus } : {}),
  };
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
  const acceptingWork = Boolean(input.acceptingWork ?? input.accepting_work);
  const cpuUsage = resolveHealthMetric(input, ["cpuUsage", "cpu", "cpuLoad"]);
  const memoryUsage = resolveHealthMetric(input, [
    "memoryUsage",
    "memory",
    "memoryPressure",
  ]);
  const eventLoopLag = resolveHealthMetric(input, [
    "eventLoopLag",
    "eventLoopLagMs",
  ]);

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
    acceptingWork,
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
    cpuUsage,
    memoryUsage,
    eventLoopLag,
    isActive: Boolean(input.isActive ?? input.is_active ?? true),
    isNonResponsive: Boolean(
      input.isNonResponsive ?? input.is_non_responsive ?? false,
    ),
    isBlocked: Boolean(input.isBlocked ?? input.is_blocked ?? false),
    health: sanitizeAuthorityRuntimeStatusHealth(input.health, {
      state: state as RuntimeStatusState,
      acceptingWork,
      reportedAt,
      cpuUsage,
      memoryUsage,
      eventLoopLag,
    }),
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
    isActive: report.isActive,
    isNonResponsive: report.isNonResponsive,
    isBlocked: report.isBlocked,
    isFrontend: report.isFrontend ?? null,
  });
}
