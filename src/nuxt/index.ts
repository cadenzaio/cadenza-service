import type CadenzaService from "../Cadenza";
import type {
  BrowserRuntimeActorHandle,
  BrowserRuntimeActorOptions,
  BrowserRuntimeServiceOptions,
  BrowserRuntimeProjectionBinding,
} from "../frontend/createBrowserRuntimeActor";
import {
  createProjectionRuntimeSnapshot,
  type ProjectionRuntimeSnapshot,
} from "../frontend/runtimeProjectionState";
import type { HydrationOptions } from "../utils/bootstrap";

type StateLike<T> = {
  value: T;
};

type ComputedLike<T> = {
  readonly value: T;
};

type RefLike<T> = {
  readonly value: T;
  readonly __v_isRef: true;
};

type NuxtPluginSetup = () => { provide?: Record<string, unknown> } | void;

declare function useState<T>(key: string, init: () => T): StateLike<T>;
declare function useRuntimeConfig(): Record<string, any>;
declare function useNuxtApp(): Record<string, any>;

export interface CadenzaNuxtRuntimeState<
  TProjectionState extends Record<string, any> = Record<string, any>,
> extends ProjectionRuntimeSnapshot<TProjectionState> {}

export interface CadenzaNuxtRuntime<
  TProjectionState extends Record<string, any> = Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
> {
  actor: BrowserRuntimeActorHandle<TProjectionState>["actor"];
  actorHandle: BrowserRuntimeActorHandle<TProjectionState>;
  waitUntilReady: BrowserRuntimeActorHandle<TProjectionState>["waitUntilReady"];
  inquire: BrowserRuntimeActorHandle<TProjectionState>["inquire"];
  getRuntimeState: BrowserRuntimeActorHandle<TProjectionState>["getRuntimeState"];
  subscribe: BrowserRuntimeActorHandle<TProjectionState>["subscribe"];
  commands: TCommands;
}

export interface CadenzaNuxtRuntimeOptions<
  TProjectionState extends Record<string, any> = Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
> extends Omit<
    BrowserRuntimeActorOptions<TProjectionState>,
    "service" | "initialProjectionState"
  > {
  cadenza: typeof CadenzaService;
  service: Omit<BrowserRuntimeServiceOptions, "bootstrap" | "hydration">;
  bootstrapUrl: string | ((config: Record<string, any>) => string);
  hydrationStateKey?: string;
  initialProjectionState: TProjectionState | (() => TProjectionState);
  commands?: (context: {
    cadenza: typeof CadenzaService;
    runtime: BrowserRuntimeActorHandle<TProjectionState>;
  }) => TCommands;
}

type NuxtRuntimeHolder<
  TProjectionState extends Record<string, any> = Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
> = {
  runtime?: BrowserRuntimeActorHandle<TProjectionState>;
  unsubscribe?: () => void;
  providedRuntime?: CadenzaNuxtRuntime<TProjectionState, TCommands>;
};

const DEFAULT_HYDRATION_STATE_KEY = "cadenza-hydration";
const RUNTIME_PROJECTION_STATE_KEY = "__cadenza_runtime_projection_state__";
const RUNTIME_HOLDER_KEY = "__CADENZA_NUXT_RUNTIME_HOLDER__";

function getRuntimeHolder<
  TProjectionState extends Record<string, any>,
  TCommands extends Record<string, any>,
>(): NuxtRuntimeHolder<TProjectionState, TCommands> {
  const globalRecord = globalThis as Record<string, unknown>;
  if (!globalRecord[RUNTIME_HOLDER_KEY]) {
    globalRecord[RUNTIME_HOLDER_KEY] = {};
  }

  return globalRecord[RUNTIME_HOLDER_KEY] as NuxtRuntimeHolder<
    TProjectionState,
    TCommands
  >;
}

function resolveBootstrapUrl(
  bootstrapUrl: string | ((config: Record<string, any>) => string),
  runtimeConfig: Record<string, any>,
): string {
  return typeof bootstrapUrl === "function"
    ? bootstrapUrl(runtimeConfig)
    : bootstrapUrl;
}

export function defineCadenzaNuxtRuntimePlugin<
  TProjectionState extends Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
>(options: CadenzaNuxtRuntimeOptions<TProjectionState, TCommands>): NuxtPluginSetup {
  return () => {
    const {
      cadenza,
      service,
      bootstrapUrl,
      hydrationStateKey,
      initialProjectionState,
      commands,
      ...actorOptions
    } = options;
    const runtimeConfig = useRuntimeConfig();
    const hydration = useState<HydrationOptions>(
      hydrationStateKey ?? DEFAULT_HYDRATION_STATE_KEY,
      () => ({
        initialInquiryResults: {},
      }),
    );
    const browserRuntimeOptions: BrowserRuntimeActorOptions<TProjectionState> = {
      ...actorOptions,
      initialProjectionState,
      service: {
        ...service,
        bootstrap: {
          url: resolveBootstrapUrl(bootstrapUrl, runtimeConfig),
        },
        hydration: hydration.value,
      },
    };
    const projectionState = useState<CadenzaNuxtRuntimeState<TProjectionState>>(
      RUNTIME_PROJECTION_STATE_KEY,
      () => createProjectionRuntimeSnapshot(undefined, initialProjectionState),
    );
    const holder = getRuntimeHolder<TProjectionState, TCommands>();

    if (!holder.runtime) {
      holder.runtime = cadenza.createBrowserRuntimeActor(browserRuntimeOptions);
    }

    holder.unsubscribe?.();
    holder.unsubscribe = holder.runtime.subscribe((runtimeState) => {
      projectionState.value = createProjectionRuntimeSnapshot(
        runtimeState,
        initialProjectionState,
      );
    });
    projectionState.value = createProjectionRuntimeSnapshot(
      holder.runtime.getRuntimeState(),
      initialProjectionState,
    );

    if (!holder.providedRuntime) {
      const runtimeCommands = commands
        ? commands({
            cadenza,
            runtime: holder.runtime,
          })
        : ({} as TCommands);

      holder.providedRuntime = {
        actor: holder.runtime.actor,
        actorHandle: holder.runtime,
        waitUntilReady: holder.runtime.waitUntilReady,
        inquire: holder.runtime.inquire,
        getRuntimeState: holder.runtime.getRuntimeState,
        subscribe: holder.runtime.subscribe,
        commands: runtimeCommands,
      };
    }

    return {
      provide: {
        cadenzaRuntime: holder.providedRuntime,
      },
    };
  };
}

export function useCadenzaRuntime<
  TRuntime extends CadenzaNuxtRuntime<any, any> = CadenzaNuxtRuntime<any, any>,
>(): TRuntime {
  return (useNuxtApp() as { $cadenzaRuntime: TRuntime }).$cadenzaRuntime;
}

export function useCadenzaProjectionState<
  TProjectionState extends Record<string, any> = Record<string, any>,
>(): StateLike<CadenzaNuxtRuntimeState<TProjectionState>> {
  return useState<CadenzaNuxtRuntimeState<TProjectionState>>(
    RUNTIME_PROJECTION_STATE_KEY,
    () => ({
      ready: false,
      projectionState: {} as TProjectionState,
    }),
  );
}

export function useCadenzaRuntimeReady(): ComputedLike<boolean> {
  const readyState = {} as RefLike<boolean>;
  Object.defineProperty(readyState, "__v_isRef", {
    value: true,
    enumerable: false,
  });
  Object.defineProperty(readyState, "value", {
    get() {
      return useCadenzaProjectionState().value.ready;
    },
    enumerable: true,
  });
  return readyState;
}

export type { BrowserRuntimeProjectionBinding };
