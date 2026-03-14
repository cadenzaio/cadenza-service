import type { AnyObject } from "@cadenza.io/core";
import { v4 as uuid } from "uuid";

export function ensureDelegationContextMetadata<T extends AnyObject>(
  input: T | undefined,
): T & {
  __deputyExecId: string;
  __metadata: AnyObject;
} {
  const context =
    input && typeof input === "object" ? ({ ...input } as T) : ({} as T);
  const metadata =
    context.__metadata && typeof context.__metadata === "object"
      ? { ...context.__metadata }
      : {};
  const deputyExecId =
    typeof metadata.__deputyExecId === "string" &&
    metadata.__deputyExecId.length > 0
      ? metadata.__deputyExecId
      : typeof context.__deputyExecId === "string" &&
          context.__deputyExecId.length > 0
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
