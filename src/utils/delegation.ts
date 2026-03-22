import type { AnyObject } from "@cadenza.io/core";
import { v4 as uuid } from "uuid";

const ROOT_METADATA_PASSTHROUGH_KEYS = [
  "__executionTraceId",
  "__inquiryId",
  "__inquirySourceTaskName",
  "__inquirySourceTaskVersion",
  "__inquirySourceTaskExecutionId",
  "__inquirySourceRoutineExecutionId",
] as const;

export function hoistDelegationMetadataFields<T extends AnyObject>(
  input: T | undefined,
  metadataInput?: AnyObject,
): T {
  const context =
    input && typeof input === "object" ? ({ ...input } as T) : ({} as T);
  const mutableContext = context as AnyObject;
  const metadata =
    metadataInput && typeof metadataInput === "object"
      ? metadataInput
      : context.__metadata && typeof context.__metadata === "object"
        ? context.__metadata
        : {};

  for (const key of ROOT_METADATA_PASSTHROUGH_KEYS) {
    if (
      (mutableContext[key] === undefined || mutableContext[key] === null) &&
      metadata[key] !== undefined &&
      metadata[key] !== null
    ) {
      mutableContext[key] = metadata[key];
    }
  }

  return context;
}

export function ensureDelegationContextMetadata<T extends AnyObject>(
  input: T | undefined,
): T & {
  __deputyExecId: string;
  __metadata: AnyObject;
} {
  const rawContext =
    input && typeof input === "object" ? ({ ...input } as T) : ({} as T);
  const metadata =
    rawContext.__metadata && typeof rawContext.__metadata === "object"
      ? { ...rawContext.__metadata }
      : {};
  const context = hoistDelegationMetadataFields(rawContext, metadata);
  const deputyExecId =
    typeof metadata.__deputyExecId === "string" &&
    metadata.__deputyExecId.length > 0
      ? metadata.__deputyExecId
      : typeof rawContext.__deputyExecId === "string" &&
          rawContext.__deputyExecId.length > 0
        ? context.__deputyExecId
        : uuid();

  return {
    ...context,
    __deputyExecId: deputyExecId,
    __metadata: {
      ...metadata,
      __deputyExecId: deputyExecId,
    },
  };
}
