import type {
  RuntimeValidationPolicy,
  RuntimeValidationScope,
} from "@cadenza.io/core";
import Cadenza from "../Cadenza";

export const RUNTIME_VALIDATION_INTENTS = {
  getPolicy: "meta-runtime-validation-policy-get",
  setPolicy: "meta-runtime-validation-policy-set",
  replacePolicy: "meta-runtime-validation-policy-replace",
  clearPolicy: "meta-runtime-validation-policy-clear",
  listScopes: "meta-runtime-validation-scope-list",
  upsertScope: "meta-runtime-validation-scope-upsert",
  removeScope: "meta-runtime-validation-scope-remove",
  clearScopes: "meta-runtime-validation-scope-clear",
} as const;

export const RUNTIME_VALIDATION_SIGNALS = {
  setPolicyRequested: "meta.runtime_validation.policy_set_requested",
  replacePolicyRequested: "meta.runtime_validation.policy_replace_requested",
  clearPolicyRequested: "meta.runtime_validation.policy_clear_requested",
  upsertScopeRequested: "meta.runtime_validation.scope_upsert_requested",
  removeScopeRequested: "meta.runtime_validation.scope_remove_requested",
  clearScopesRequested: "meta.runtime_validation.scope_clear_requested",
  policyUpdated: "meta.runtime_validation.policy_updated",
  policyCleared: "meta.runtime_validation.policy_cleared",
  scopeUpserted: "meta.runtime_validation.scope_upserted",
  scopeRemoved: "meta.runtime_validation.scope_removed",
  scopesCleared: "meta.runtime_validation.scopes_cleared",
} as const;

function normalizePolicyContext(ctx: Record<string, any>): RuntimeValidationPolicy {
  const source =
    ctx.policy && typeof ctx.policy === "object" && !Array.isArray(ctx.policy)
      ? ctx.policy
      : ctx;
  const policy: RuntimeValidationPolicy = {};

  for (const key of [
    "metaInput",
    "metaOutput",
    "businessInput",
    "businessOutput",
    "warnOnMissingMetaInputSchema",
    "warnOnMissingMetaOutputSchema",
    "warnOnMissingBusinessInputSchema",
    "warnOnMissingBusinessOutputSchema",
  ] as const) {
    if (source[key] !== undefined) {
      policy[key] = source[key];
    }
  }

  return policy;
}

function normalizeScopeContext(ctx: Record<string, any>): RuntimeValidationScope {
  const source =
    ctx.scope && typeof ctx.scope === "object" && !Array.isArray(ctx.scope)
      ? ctx.scope
      : ctx;

  return {
    id: source.id,
    active: source.active,
    startTaskNames: Array.isArray(source.startTaskNames)
      ? source.startTaskNames
      : undefined,
    startRoutineNames: Array.isArray(source.startRoutineNames)
      ? source.startRoutineNames
      : undefined,
    policy:
      source.policy && typeof source.policy === "object"
        ? normalizePolicyContext({ policy: source.policy })
        : undefined,
  };
}

export default class RuntimeValidationController {
  private static _instance: RuntimeValidationController;

  public static get instance(): RuntimeValidationController {
    if (!this._instance) {
      this._instance = new RuntimeValidationController();
    }

    return this._instance;
  }

  constructor() {
    Cadenza.defineIntent({
      name: RUNTIME_VALIDATION_INTENTS.getPolicy,
      description: "Get the active runtime validation policy.",
      input: { type: "object" },
      output: { type: "object" },
    });
    Cadenza.defineIntent({
      name: RUNTIME_VALIDATION_INTENTS.setPolicy,
      description: "Merge fields into the active runtime validation policy.",
      input: { type: "object" },
      output: { type: "object" },
    });
    Cadenza.defineIntent({
      name: RUNTIME_VALIDATION_INTENTS.replacePolicy,
      description: "Replace the active runtime validation policy.",
      input: { type: "object" },
      output: { type: "object" },
    });
    Cadenza.defineIntent({
      name: RUNTIME_VALIDATION_INTENTS.clearPolicy,
      description: "Clear the active runtime validation policy.",
      input: { type: "object" },
      output: { type: "object" },
    });
    Cadenza.defineIntent({
      name: RUNTIME_VALIDATION_INTENTS.listScopes,
      description: "List active runtime validation scopes.",
      input: { type: "object" },
      output: { type: "object" },
    });
    Cadenza.defineIntent({
      name: RUNTIME_VALIDATION_INTENTS.upsertScope,
      description:
        "Create or update a runtime validation scope for targeted subflow validation.",
      input: { type: "object" },
      output: { type: "object" },
    });
    Cadenza.defineIntent({
      name: RUNTIME_VALIDATION_INTENTS.removeScope,
      description: "Remove a runtime validation scope by id.",
      input: { type: "object" },
      output: { type: "object" },
    });
    Cadenza.defineIntent({
      name: RUNTIME_VALIDATION_INTENTS.clearScopes,
      description: "Clear all runtime validation scopes.",
      input: { type: "object" },
      output: { type: "object" },
    });

    Cadenza.createMetaTask("Get runtime validation policy", () => {
      return {
        policy: Cadenza.getRuntimeValidationPolicy(),
      };
    }).respondsTo(RUNTIME_VALIDATION_INTENTS.getPolicy);

    Cadenza.createMetaTask(
      "Set runtime validation policy",
      (ctx, emit) => {
        const policy = normalizePolicyContext(ctx);
        const nextPolicy = Cadenza.setRuntimeValidationPolicy(policy);
        emit(RUNTIME_VALIDATION_SIGNALS.policyUpdated, {
          policy: nextPolicy,
        });
        return {
          policy: nextPolicy,
        };
      },
      "Merges runtime validation policy fields at runtime.",
    )
      .respondsTo(RUNTIME_VALIDATION_INTENTS.setPolicy)
      .doOn(RUNTIME_VALIDATION_SIGNALS.setPolicyRequested);

    Cadenza.createMetaTask(
      "Replace runtime validation policy",
      (ctx, emit) => {
        const policy = normalizePolicyContext(ctx);
        const nextPolicy = Cadenza.replaceRuntimeValidationPolicy(policy);
        emit(RUNTIME_VALIDATION_SIGNALS.policyUpdated, {
          policy: nextPolicy,
        });
        return {
          policy: nextPolicy,
        };
      },
      "Replaces runtime validation policy fields at runtime.",
    )
      .respondsTo(RUNTIME_VALIDATION_INTENTS.replacePolicy)
      .doOn(RUNTIME_VALIDATION_SIGNALS.replacePolicyRequested);

    Cadenza.createMetaTask(
      "Clear runtime validation policy",
      (_ctx, emit) => {
        Cadenza.clearRuntimeValidationPolicy();
        emit(RUNTIME_VALIDATION_SIGNALS.policyCleared, {});
        return {
          policy: Cadenza.getRuntimeValidationPolicy(),
        };
      },
      "Clears the runtime validation policy.",
    )
      .respondsTo(RUNTIME_VALIDATION_INTENTS.clearPolicy)
      .doOn(RUNTIME_VALIDATION_SIGNALS.clearPolicyRequested);

    Cadenza.createMetaTask("List runtime validation scopes", () => {
      return {
        scopes: Cadenza.getRuntimeValidationScopes(),
      };
    }).respondsTo(RUNTIME_VALIDATION_INTENTS.listScopes);

    Cadenza.createMetaTask(
      "Upsert runtime validation scope",
      (ctx, emit) => {
        const scope = normalizeScopeContext(ctx);
        const nextScope = Cadenza.upsertRuntimeValidationScope(scope);
        emit(RUNTIME_VALIDATION_SIGNALS.scopeUpserted, {
          scope: nextScope,
        });
        return {
          scope: nextScope,
        };
      },
      "Upserts a runtime validation scope for targeted debugging.",
    )
      .respondsTo(RUNTIME_VALIDATION_INTENTS.upsertScope)
      .doOn(RUNTIME_VALIDATION_SIGNALS.upsertScopeRequested);

    Cadenza.createMetaTask(
      "Remove runtime validation scope",
      (ctx, emit) => {
        if (!ctx.id) {
          throw new Error("Runtime validation scope id is required");
        }

        Cadenza.removeRuntimeValidationScope(ctx.id);
        emit(RUNTIME_VALIDATION_SIGNALS.scopeRemoved, {
          id: ctx.id,
        });
        return {
          id: ctx.id,
          scopes: Cadenza.getRuntimeValidationScopes(),
        };
      },
      "Removes a runtime validation scope.",
    )
      .respondsTo(RUNTIME_VALIDATION_INTENTS.removeScope)
      .doOn(RUNTIME_VALIDATION_SIGNALS.removeScopeRequested);

    Cadenza.createMetaTask(
      "Clear runtime validation scopes",
      (_ctx, emit) => {
        Cadenza.clearRuntimeValidationScopes();
        emit(RUNTIME_VALIDATION_SIGNALS.scopesCleared, {});
        return {
          scopes: Cadenza.getRuntimeValidationScopes(),
        };
      },
      "Clears all runtime validation scopes.",
    )
      .respondsTo(RUNTIME_VALIDATION_INTENTS.clearScopes)
      .doOn(RUNTIME_VALIDATION_SIGNALS.clearScopesRequested);
  }
}
