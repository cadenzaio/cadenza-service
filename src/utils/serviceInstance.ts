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
  const leaseStatusRaw = normalizeString(raw.leaseStatus ?? raw.lease_status);
  const leaseStatus =
    leaseStatusRaw === "active" ||
    leaseStatusRaw === "non_responsive" ||
    leaseStatusRaw === "inactive" ||
    leaseStatusRaw === "deleted"
      ? leaseStatusRaw
      : undefined;
  const isActive =
    leaseStatus === "active"
      ? true
      : leaseStatus === "non_responsive" ||
          leaseStatus === "inactive" ||
          leaseStatus === "deleted"
        ? false
        : Boolean(raw.isActive ?? raw.is_active ?? true);
  const isNonResponsive =
    leaseStatus === "non_responsive"
      ? true
      : leaseStatus === "active" ||
          leaseStatus === "inactive" ||
          leaseStatus === "deleted"
        ? false
        : Boolean(raw.isNonResponsive ?? raw.is_non_responsive ?? false);

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
    leaseStatus,
    leaseExpiresAt:
      typeof raw.leaseExpiresAt === "string"
        ? raw.leaseExpiresAt
        : typeof raw.lease_expires_at === "string"
          ? raw.lease_expires_at
          : undefined,
    lastLeaseRenewedAt:
      typeof raw.lastLeaseRenewedAt === "string"
        ? raw.lastLeaseRenewedAt
        : typeof raw.last_lease_renewed_at === "string"
          ? raw.last_lease_renewed_at
          : undefined,
    isReady:
      typeof raw.isReady === "boolean"
        ? raw.isReady
        : typeof raw.is_ready === "boolean"
          ? raw.is_ready
          : undefined,
    readinessReason:
      typeof raw.readinessReason === "string"
        ? raw.readinessReason
        : typeof raw.readiness_reason === "string"
          ? raw.readiness_reason
          : null,
    lastReadyAt:
      typeof raw.lastReadyAt === "string"
        ? raw.lastReadyAt
        : typeof raw.last_ready_at === "string"
          ? raw.last_ready_at
          : null,
    lastObservedTransportAt:
      typeof raw.lastObservedTransportAt === "string"
        ? raw.lastObservedTransportAt
        : typeof raw.last_observed_transport_at === "string"
          ? raw.last_observed_transport_at
          : null,
    shutdownRequestedAt:
      typeof raw.shutdownRequestedAt === "string"
        ? raw.shutdownRequestedAt
        : typeof raw.shutdown_requested_at === "string"
          ? raw.shutdown_requested_at
          : null,
    isActive,
    isNonResponsive,
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

  return buildTransportClientKey(transport, instance.serviceName);
}
