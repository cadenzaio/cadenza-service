import type { AnyObject } from "@cadenza.io/core";
import type {
  InquiryResponderDescriptor,
  InquiryResponderStatus,
} from "../types/inquiry";

export const META_INTENT_PREFIX = "meta-";
export const META_RUNTIME_TRANSPORT_DIAGNOSTICS_INTENT =
  "meta-runtime-transport-diagnostics";
export const META_RUNTIME_STATUS_INTENT = "meta-runtime-status";

function isPlainObject(value: unknown): value is Record<string, any> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype
  );
}

function deepMergeDeterministic(left: any, right: any): any {
  if (Array.isArray(left) && Array.isArray(right)) {
    return [...left, ...right];
  }

  if (isPlainObject(left) && isPlainObject(right)) {
    const merged: Record<string, any> = { ...left };
    const keys = Array.from(new Set([...Object.keys(left), ...Object.keys(right)])).sort();

    for (const key of keys) {
      if (!(key in left)) {
        merged[key] = right[key];
        continue;
      }

      if (!(key in right)) {
        merged[key] = left[key];
        continue;
      }

      merged[key] = deepMergeDeterministic(left[key], right[key]);
    }

    return merged;
  }

  return right;
}

export function mergeInquiryContexts(contexts: AnyObject[]): AnyObject {
  return contexts.reduce((acc, next) => deepMergeDeterministic(acc, next), {});
}

export function isMetaIntentName(intentName: string): boolean {
  return intentName.startsWith(META_INTENT_PREFIX);
}

export function shouldExecuteInquiryResponder(
  inquiry: string,
  responderIsMeta: boolean,
): boolean {
  if (!isMetaIntentName(inquiry)) {
    return true;
  }

  return responderIsMeta;
}

export function compareResponderDescriptors(
  left: InquiryResponderDescriptor,
  right: InquiryResponderDescriptor,
): number {
  if (left.serviceName !== right.serviceName) {
    return left.serviceName.localeCompare(right.serviceName);
  }

  if (left.taskName !== right.taskName) {
    return left.taskName.localeCompare(right.taskName);
  }

  if (left.taskVersion !== right.taskVersion) {
    return left.taskVersion - right.taskVersion;
  }

  return left.localTaskName.localeCompare(right.localTaskName);
}

export function summarizeResponderStatuses(
  statuses: InquiryResponderStatus[],
): {
  responded: number;
  failed: number;
  timedOut: number;
  pending: number;
} {
  let responded = 0;
  let failed = 0;
  let timedOut = 0;
  let pending = 0;

  for (const status of statuses) {
    if (status.status === "fulfilled") responded++;
    if (status.status === "failed") failed++;
    if (status.status === "timed_out") timedOut++;
  }

  pending = Math.max(0, statuses.length - responded - failed - timedOut);

  return { responded, failed, timedOut, pending };
}
