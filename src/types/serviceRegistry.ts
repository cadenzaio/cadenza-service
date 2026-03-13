import type { AnyObject } from "@cadenza.io/core";
import type { RuntimeStatusState } from "../utils/runtimeStatus";
import type { ServiceTransportDescriptor } from "./transport";

export interface ServiceInstanceDescriptor {
  uuid: string;
  serviceName: string;
  numberOfRunningGraphs?: number;
  isPrimary: boolean;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
  runtimeState?: RuntimeStatusState;
  acceptingWork?: boolean;
  reportedAt?: string;
  health: AnyObject;
  isFrontend: boolean;
  isDatabase?: boolean;
  transports: ServiceTransportDescriptor[];
  clientCreatedTransportIds?: string[];
}
