export type RuntimeStatusState =
  | "healthy"
  | "degraded"
  | "overloaded"
  | "unavailable";

export interface RuntimeStatusSnapshot {
  state: RuntimeStatusState;
  acceptingWork: boolean;
  numberOfRunningGraphs: number;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
}

export interface ResolveRuntimeStatusInput {
  numberOfRunningGraphs: number;
  isActive: boolean;
  isNonResponsive: boolean;
  isBlocked: boolean;
  degradedGraphThreshold: number;
  overloadedGraphThreshold: number;
}

export function resolveRuntimeStatus(
  input: ResolveRuntimeStatusInput,
): RuntimeStatusSnapshot {
  const numberOfRunningGraphs = Math.max(
    0,
    Math.trunc(Number(input.numberOfRunningGraphs) || 0),
  );

  const isActive = Boolean(input.isActive);
  const isNonResponsive = Boolean(input.isNonResponsive);
  const isBlocked = Boolean(input.isBlocked);

  if (!isActive || isNonResponsive || isBlocked) {
    return {
      state: "unavailable",
      acceptingWork: false,
      numberOfRunningGraphs,
      isActive,
      isNonResponsive,
      isBlocked,
    };
  }

  if (numberOfRunningGraphs >= input.overloadedGraphThreshold) {
    return {
      state: "overloaded",
      acceptingWork: true,
      numberOfRunningGraphs,
      isActive,
      isNonResponsive,
      isBlocked,
    };
  }

  if (numberOfRunningGraphs >= input.degradedGraphThreshold) {
    return {
      state: "degraded",
      acceptingWork: true,
      numberOfRunningGraphs,
      isActive,
      isNonResponsive,
      isBlocked,
    };
  }

  return {
    state: "healthy",
    acceptingWork: true,
    numberOfRunningGraphs,
    isActive,
    isNonResponsive,
    isBlocked,
  };
}

export function runtimeStatusPriority(state: RuntimeStatusState): number {
  switch (state) {
    case "healthy":
      return 0;
    case "degraded":
      return 1;
    case "overloaded":
      return 2;
    case "unavailable":
      return 3;
    default:
      return 4;
  }
}

export function hasSignificantRuntimeStatusChange(
  previous: RuntimeStatusSnapshot | null,
  next: RuntimeStatusSnapshot,
): boolean {
  if (!previous) {
    return true;
  }

  return (
    previous.state !== next.state ||
    previous.acceptingWork !== next.acceptingWork ||
    previous.isActive !== next.isActive ||
    previous.isNonResponsive !== next.isNonResponsive ||
    previous.isBlocked !== next.isBlocked
  );
}
