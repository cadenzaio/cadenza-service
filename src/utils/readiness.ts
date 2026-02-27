import type { RuntimeStatusState } from "./runtimeStatus";

export type ReadinessState = "ready" | "degraded" | "blocked";
export type DependencyReadinessState =
  | "ready"
  | "degraded"
  | "overloaded"
  | "unavailable";

export interface DependencyReadinessEvaluationInput {
  exists: boolean;
  runtimeState: RuntimeStatusState;
  acceptingWork: boolean;
  missedHeartbeats: number;
  missThreshold: number;
}

export interface DependencyReadinessEvaluation {
  state: DependencyReadinessState;
  stale: boolean;
  blocked: boolean;
  reason:
    | "missing"
    | "heartbeat-timeout"
    | "heartbeat-stale"
    | "runtime-unavailable"
    | "runtime-overloaded"
    | "runtime-degraded"
    | "runtime-healthy";
}

export interface DependencyReadinessSummary {
  total: number;
  ready: number;
  degraded: number;
  overloaded: number;
  unavailable: number;
  stale: number;
}

export function evaluateDependencyReadiness(
  input: DependencyReadinessEvaluationInput,
): DependencyReadinessEvaluation {
  const missedHeartbeats = Math.max(
    0,
    Math.trunc(Number(input.missedHeartbeats) || 0),
  );
  const stale = missedHeartbeats > 0;
  const timeoutReached = missedHeartbeats >= Math.max(1, input.missThreshold);

  if (!input.exists) {
    return {
      state: "unavailable",
      stale: true,
      blocked: true,
      reason: "missing",
    };
  }

  if (timeoutReached) {
    return {
      state: "unavailable",
      stale: true,
      blocked: true,
      reason: "heartbeat-timeout",
    };
  }

  if (input.runtimeState === "unavailable" || !input.acceptingWork) {
    return {
      state: "unavailable",
      stale,
      blocked: true,
      reason: "runtime-unavailable",
    };
  }

  if (stale) {
    return {
      state: "degraded",
      stale: true,
      blocked: false,
      reason: "heartbeat-stale",
    };
  }

  if (input.runtimeState === "overloaded") {
    return {
      state: "overloaded",
      stale: false,
      blocked: false,
      reason: "runtime-overloaded",
    };
  }

  if (input.runtimeState === "degraded") {
    return {
      state: "degraded",
      stale: false,
      blocked: false,
      reason: "runtime-degraded",
    };
  }

  return {
    state: "ready",
    stale: false,
    blocked: false,
    reason: "runtime-healthy",
  };
}

export function summarizeDependencyReadiness(
  evaluations: DependencyReadinessEvaluation[],
): DependencyReadinessSummary {
  const summary: DependencyReadinessSummary = {
    total: evaluations.length,
    ready: 0,
    degraded: 0,
    overloaded: 0,
    unavailable: 0,
    stale: 0,
  };

  for (const evaluation of evaluations) {
    if (evaluation.state === "ready") summary.ready++;
    if (evaluation.state === "degraded") summary.degraded++;
    if (evaluation.state === "overloaded") summary.overloaded++;
    if (evaluation.state === "unavailable") summary.unavailable++;
    if (evaluation.stale) summary.stale++;
  }

  return summary;
}

export function resolveServiceReadinessState(
  localRuntimeState: RuntimeStatusState,
  localAcceptingWork: boolean,
  dependencySummary: DependencyReadinessSummary,
): ReadinessState {
  if (localRuntimeState === "unavailable" || !localAcceptingWork) {
    return "blocked";
  }

  if (dependencySummary.unavailable > 0) {
    return "blocked";
  }

  if (
    dependencySummary.degraded > 0 ||
    dependencySummary.overloaded > 0 ||
    dependencySummary.stale > 0
  ) {
    return "degraded";
  }

  return "ready";
}
