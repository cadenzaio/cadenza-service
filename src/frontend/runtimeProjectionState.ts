import type { BrowserRuntimeActorRuntimeState } from "./createBrowserRuntimeActor";

export interface ProjectionRuntimeSnapshot<
  TProjectionState extends Record<string, any> = Record<string, any>,
> {
  ready: boolean;
  projectionState: TProjectionState;
}

export function cloneProjectionState<TProjectionState>(
  value: TProjectionState,
): TProjectionState {
  if (Array.isArray(value)) {
    return value.map((item) => cloneProjectionState(item)) as TProjectionState;
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, nested]) => [
        key,
        cloneProjectionState(nested),
      ]),
    ) as TProjectionState;
  }

  return value;
}

export function resolveInitialProjectionState<
  TProjectionState extends Record<string, any>,
>(value: TProjectionState | (() => TProjectionState)): TProjectionState {
  const resolved =
    typeof value === "function"
      ? (value as () => TProjectionState)()
      : value;
  return cloneProjectionState(resolved);
}

export function createProjectionRuntimeSnapshot<
  TProjectionState extends Record<string, any>,
>(
  runtimeState: BrowserRuntimeActorRuntimeState<TProjectionState> | undefined,
  initialProjectionState: TProjectionState | (() => TProjectionState),
): ProjectionRuntimeSnapshot<TProjectionState> {
  return {
    ready: runtimeState?.ready ?? false,
    projectionState:
      runtimeState?.projectionState ??
      resolveInitialProjectionState(initialProjectionState),
  };
}
