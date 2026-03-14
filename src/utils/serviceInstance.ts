import type { AnyObject } from "@cadenza.io/core";
import type { ServiceInstanceDescriptor } from "../types/serviceRegistry";
import type {
  ServiceTransportDescriptor,
  ServiceTransportProtocol,
  ServiceTransportRole,
} from "../types/transport";
import {
  buildTransportClientKey,
  normalizeServiceTransportDescriptor,
  selectTransportForRole,
} from "./transport";

function normalizeString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeTransportArray(
  value: unknown,
  serviceInstanceId: string,
): ServiceTransportDescriptor[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((entry) =>
      normalizeServiceTransportDescriptor({
        ...((entry ?? {}) as Record<string, unknown>),
        service_instance_id:
          (entry as Record<string, unknown> | undefined)?.service_instance_id ??
          (entry as Record<string, unknown> | undefined)?.serviceInstanceId ??
          serviceInstanceId,
      }),
    )
    .filter((transport): transport is ServiceTransportDescriptor => !!transport)
    .sort((left, right) => left.origin.localeCompare(right.origin));
}

export function normalizeServiceInstanceDescriptor(
  value: unknown,
): ServiceInstanceDescriptor | null {
  const raw = (value ?? {}) as Record<string, unknown>;
  const uuid = normalizeString(raw.uuid);
  const serviceName = normalizeString(raw.serviceName ?? raw.service_name);

  if (!uuid || !serviceName) {
    return null;
  }

  const transports = normalizeTransportArray(raw.transports, uuid);

  return {
    uuid,
    serviceName,
    numberOfRunningGraphs: Math.max(
      0,
      Math.trunc(
        Number(raw.numberOfRunningGraphs ?? raw.number_of_running_graphs ?? 0) ||
          0,
      ),
    ),
    isPrimary: Boolean(raw.isPrimary ?? raw.is_primary ?? false),
    isActive: Boolean(raw.isActive ?? raw.is_active ?? true),
    isNonResponsive: Boolean(
      raw.isNonResponsive ?? raw.is_non_responsive ?? false,
    ),
    isBlocked: Boolean(raw.isBlocked ?? raw.is_blocked ?? false),
    runtimeState:
      raw.runtimeState === "healthy" ||
      raw.runtimeState === "degraded" ||
      raw.runtimeState === "overloaded" ||
      raw.runtimeState === "unavailable"
        ? raw.runtimeState
        : undefined,
    acceptingWork:
      typeof raw.acceptingWork === "boolean" ? raw.acceptingWork : undefined,
    reportedAt:
      typeof raw.reportedAt === "string"
        ? raw.reportedAt
        : typeof raw.reported_at === "string"
          ? raw.reported_at
          : undefined,
    health: (raw.health ?? {}) as AnyObject,
    isFrontend: Boolean(raw.isFrontend ?? raw.is_frontend ?? false),
    isDatabase: Boolean(raw.isDatabase ?? raw.is_database ?? false),
    isBootstrapPlaceholder: Boolean(
      raw.isBootstrapPlaceholder ?? raw.is_bootstrap_placeholder ?? false,
    ),
    transports,
    clientCreatedTransportIds: Array.isArray(raw.clientCreatedTransportIds)
      ? raw.clientCreatedTransportIds
          .map((entry) => normalizeString(entry))
          .filter((entry) => entry.length > 0)
      : undefined,
  };
}

export function getRouteableTransport(
  instance: ServiceInstanceDescriptor,
  role: ServiceTransportRole,
  protocol?: ServiceTransportProtocol,
): ServiceTransportDescriptor | undefined {
  return selectTransportForRole(instance.transports ?? [], role, protocol);
}

export function getTransportClientKeyForRole(
  instance: ServiceInstanceDescriptor,
  role: ServiceTransportRole,
  protocol?: ServiceTransportProtocol,
): string | null {
  const transport = getRouteableTransport(instance, role, protocol);
  if (!transport) {
    return null;
  }

  return buildTransportClientKey(transport);
}
