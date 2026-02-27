import type { InquiryOptions } from "@cadenza.io/core";

export interface InquiryResponderDescriptor {
  isRemote: boolean;
  serviceName: string;
  taskName: string;
  taskVersion: number;
  localTaskName: string;
}

export interface InquiryResponderStatus extends InquiryResponderDescriptor {
  status: "fulfilled" | "failed" | "timed_out";
  durationMs: number;
  error?: string;
}

export interface DistributedInquiryMeta {
  inquiry: string;
  isMetaInquiry: boolean;
  totalResponders: number;
  eligibleResponders: number;
  filteredOutResponders: number;
  responded: number;
  failed: number;
  timedOut: number;
  pending: number;
  durationMs: number;
  responders: InquiryResponderStatus[];
}

export type DistributedInquiryOptions = Partial<InquiryOptions> & {
  /**
   * Total timeout budget for the whole inquiry fan-out.
   * Falls back to InquiryOptions.timeout when not set.
   */
  overallTimeoutMs?: number;
  /**
   * If true, reject on timeout / partial completion.
   * If false (default), resolve with partial result and metadata.
   */
  requireComplete?: boolean;
  /**
   * Timeout propagated to responder execution context (__timeout).
   * Mainly used by distributed deputy responders.
   */
  perResponderTimeoutMs?: number;
};
