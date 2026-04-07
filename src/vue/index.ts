import {
  computed,
  inject,
  markRaw,
  readonly,
  shallowRef,
  watch,
  type App,
  type ComputedRef,
  type InjectionKey,
  type ShallowRef,
} from "vue";

import type CadenzaService from "../Cadenza";
import type {
  BrowserRuntimeActorHandle,
  BrowserRuntimeActorOptions,
  BrowserRuntimeProjectionBinding,
  BrowserRuntimeServiceOptions,
} from "../frontend/createBrowserRuntimeActor";
import {
  createProjectionRuntimeSnapshot,
  type ProjectionRuntimeSnapshot,
} from "../frontend/runtimeProjectionState";
import type { HydrationOptions } from "../utils/bootstrap";

export interface CadenzaVueRuntimeState<
  TProjectionState extends Record<string, any> = Record<string, any>,
> extends ProjectionRuntimeSnapshot<TProjectionState> {}

export interface CadenzaVueRuntime<
  TProjectionState extends Record<string, any> = Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
> {
  actor: BrowserRuntimeActorHandle<TProjectionState>["actor"];
  actorHandle: BrowserRuntimeActorHandle<TProjectionState>;
  waitUntilReady: BrowserRuntimeActorHandle<TProjectionState>["waitUntilReady"];
  inquire: BrowserRuntimeActorHandle<TProjectionState>["inquire"];
  getRuntimeState: BrowserRuntimeActorHandle<TProjectionState>["getRuntimeState"];
  subscribe: BrowserRuntimeActorHandle<TProjectionState>["subscribe"];
  snapshot: Readonly<ShallowRef<CadenzaVueRuntimeState<TProjectionState>>>;
  projectionState: ComputedRef<TProjectionState>;
  ready: ComputedRef<boolean>;
  commands: TCommands;
  install: (app: App) => void;
  dispose: () => void;
}

export interface CadenzaVueRuntimeOptions<
  TProjectionState extends Record<string, any> = Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
> extends Omit<
    BrowserRuntimeActorOptions<TProjectionState>,
    "service" | "initialProjectionState"
  > {
  cadenza: typeof CadenzaService;
  service: Omit<BrowserRuntimeServiceOptions, "bootstrap" | "hydration">;
  bootstrapUrl: string | (() => string);
  hydration?: HydrationOptions | (() => HydrationOptions);
  initialProjectionState: TProjectionState | (() => TProjectionState);
  commands?: (context: {
    cadenza: typeof CadenzaService;
    runtime: BrowserRuntimeActorHandle<TProjectionState>;
  }) => TCommands;
}

type EqualityComparator<TSelected> = (
  previous: TSelected,
  next: TSelected,
) => boolean;

const DEFAULT_SELECTOR_EQUALITY = Object.is;
const CADENZA_VUE_RUNTIME_KEY: InjectionKey<CadenzaVueRuntime<any, any>> =
  Symbol("CadenzaVueRuntime");

function resolveBootstrapUrl(bootstrapUrl: string | (() => string)): string {
  return typeof bootstrapUrl === "function" ? bootstrapUrl() : bootstrapUrl;
}

function resolveHydrationOptions(
  hydration: HydrationOptions | (() => HydrationOptions) | undefined,
): HydrationOptions | undefined {
  if (!hydration) {
    return undefined;
  }

  return typeof hydration === "function"
    ? (hydration as () => HydrationOptions)()
    : hydration;
}

function getRequiredRuntime<
  TProjectionState extends Record<string, any>,
  TCommands extends Record<string, any>,
>(): CadenzaVueRuntime<TProjectionState, TCommands> {
  const runtime = inject(CADENZA_VUE_RUNTIME_KEY, null);
  if (!runtime) {
    throw new Error(
      "Cadenza Vue runtime is not installed. Call app.use(runtime) before using Cadenza Vue composables.",
    );
  }

  return runtime as CadenzaVueRuntime<TProjectionState, TCommands>;
}

export function createCadenzaVueRuntime<
  TProjectionState extends Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
>(options: CadenzaVueRuntimeOptions<TProjectionState, TCommands>) {
  const {
    cadenza,
    service,
    bootstrapUrl,
    hydration,
    initialProjectionState,
    commands,
    ...actorOptions
  } = options;
  const browserRuntimeOptions: BrowserRuntimeActorOptions<TProjectionState> = {
    ...actorOptions,
    initialProjectionState,
    service: {
      ...service,
      bootstrap: {
        url: resolveBootstrapUrl(bootstrapUrl),
      },
      hydration: resolveHydrationOptions(hydration),
    },
  };
  const runtime = cadenza.createBrowserRuntimeActor(browserRuntimeOptions);
  const snapshotRef = shallowRef<CadenzaVueRuntimeState<TProjectionState>>(
    createProjectionRuntimeSnapshot(
      runtime.getRuntimeState(),
      initialProjectionState,
    ),
  );
  const unsubscribe = runtime.subscribe((runtimeState) => {
    snapshotRef.value = createProjectionRuntimeSnapshot(
      runtimeState,
      initialProjectionState,
    );
  });
  let disposed = false;
  const runtimeCommands = commands
    ? commands({
        cadenza,
        runtime,
      })
    : ({} as TCommands);
  const runtimeObject: CadenzaVueRuntime<TProjectionState, TCommands> = {
    actor: runtime.actor,
    actorHandle: runtime,
    waitUntilReady: runtime.waitUntilReady,
    inquire: runtime.inquire,
    getRuntimeState: runtime.getRuntimeState,
    subscribe: runtime.subscribe,
    snapshot: readonly(snapshotRef) as Readonly<
      ShallowRef<CadenzaVueRuntimeState<TProjectionState>>
    >,
    projectionState: computed(
      () => snapshotRef.value.projectionState as TProjectionState,
    ),
    ready: computed(() => snapshotRef.value.ready),
    commands: runtimeCommands,
    install(app: App) {
      app.provide(CADENZA_VUE_RUNTIME_KEY, runtimeObject);
      app.config.globalProperties.$cadenzaRuntime = runtimeObject;
    },
    dispose() {
      if (disposed) {
        return;
      }

      disposed = true;
      unsubscribe();
    },
  };

  return markRaw(runtimeObject) as CadenzaVueRuntime<TProjectionState, TCommands>;
}

export function useCadenzaRuntime<
  TRuntime extends CadenzaVueRuntime<any, any> = CadenzaVueRuntime<any, any>,
>(): TRuntime {
  return getRequiredRuntime<any, any>() as TRuntime;
}

export function useCadenzaRuntimeState<
  TProjectionState extends Record<string, any> = Record<string, any>,
>(): Readonly<ShallowRef<CadenzaVueRuntimeState<TProjectionState>>> {
  return getRequiredRuntime<TProjectionState, Record<string, any>>().snapshot;
}

export function useCadenzaProjectionState<
  TProjectionState extends Record<string, any> = Record<string, any>,
>(): ComputedRef<TProjectionState> {
  return getRequiredRuntime<TProjectionState, Record<string, any>>()
    .projectionState as ComputedRef<TProjectionState>;
}

export function useCadenzaProjectionSelector<
  TProjectionState extends Record<string, any> = Record<string, any>,
  TSelected = TProjectionState,
>(
  selector: (state: TProjectionState) => TSelected,
  isEqual: EqualityComparator<TSelected> = DEFAULT_SELECTOR_EQUALITY,
): Readonly<ShallowRef<TSelected>> {
  const projectionState = useCadenzaProjectionState<TProjectionState>();
  const selectedRef = shallowRef<TSelected>(
    selector(projectionState.value as TProjectionState),
  );

  watch(
    projectionState,
    (nextProjectionState) => {
      const nextSelected = selector(nextProjectionState as TProjectionState);
      if (isEqual(selectedRef.value, nextSelected)) {
        return;
      }

      selectedRef.value = nextSelected;
    },
    {
      flush: "sync",
    },
  );

  return readonly(selectedRef) as Readonly<ShallowRef<TSelected>>;
}

export function useCadenzaRuntimeReady(): ComputedRef<boolean> {
  return getRequiredRuntime<Record<string, any>, Record<string, any>>().ready;
}

export type { BrowserRuntimeProjectionBinding };

declare module "vue" {
  interface ComponentCustomProperties {
    $cadenzaRuntime: CadenzaVueRuntime<any, any>;
  }
}
