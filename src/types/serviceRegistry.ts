import type { AnyObject } from "@cadenza.io/core";
import type { RuntimeStatusState } from "../utils/runtimeStatus";
import type { ServiceTransportDescriptor } from "./transport";

export interface ServiceInstanceDescriptor {
  uuid: string;
  serviceName: string;
  numberOfRunningGraphs?: number;
  isPrimary: boolean;
  leaseStatus?: "active" | "non_responsive" | "inactive" | "deleted";
  leaseExpiresAt?: string;
  lastLeaseRenewedAt?: string;
  isReady?: boolean;
  readinessReason?: string | null;
  lastReadyAt?: string | null;
  lastObservedTransportAt?: string | null;
  shutdownRequestedAt?: string | null;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
  runtimeState?: RuntimeStatusState;
  acceptingWork?: boolean;
  reportedAt?: string;
  health: AnyObject;
  isFrontend: boolean;
  isDatabase?: boolean;
  isBootstrapPlaceholder?: boolean;
  transports: ServiceTransportDescriptor[];
  clientCreatedTransportIds?: string[];
  clientPendingTransportIds?: string[];
  clientReadyTransportIds?: string[];
}
