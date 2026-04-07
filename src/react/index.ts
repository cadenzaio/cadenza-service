import {
  createContext,
  createElement,
  useContext,
  useRef,
  useSyncExternalStore,
} from "react";
import type { ReactNode } from "react";

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

export interface CadenzaReactRuntimeState<
  TProjectionState extends Record<string, any> = Record<string, any>,
> extends ProjectionRuntimeSnapshot<TProjectionState> {}

export interface CadenzaReactRuntime<
  TProjectionState extends Record<string, any> = Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
> {
  actor: BrowserRuntimeActorHandle<TProjectionState>["actor"];
  actorHandle: BrowserRuntimeActorHandle<TProjectionState>;
  waitUntilReady: BrowserRuntimeActorHandle<TProjectionState>["waitUntilReady"];
  inquire: BrowserRuntimeActorHandle<TProjectionState>["inquire"];
  getRuntimeState: BrowserRuntimeActorHandle<TProjectionState>["getRuntimeState"];
  subscribe: BrowserRuntimeActorHandle<TProjectionState>["subscribe"];
  getSnapshot: () => CadenzaReactRuntimeState<TProjectionState>;
  commands: TCommands;
}

export interface CadenzaReactRuntimeOptions<
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

export interface CadenzaRuntimeProviderProps<
  TProjectionState extends Record<string, any> = Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
> {
  runtime: CadenzaReactRuntime<TProjectionState, TCommands>;
  children?: ReactNode;
}

type EqualityComparator<TSelected> = (
  previous: TSelected,
  next: TSelected,
) => boolean;

const DEFAULT_SELECTOR_EQUALITY = Object.is;
const CadenzaRuntimeContext = createContext<CadenzaReactRuntime<any, any> | null>(
  null,
);

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
>(): CadenzaReactRuntime<TProjectionState, TCommands> {
  const runtime = useContext(CadenzaRuntimeContext);
  if (!runtime) {
    throw new Error(
      "CadenzaRuntimeProvider is required before using React Cadenza runtime hooks.",
    );
  }

  return runtime as CadenzaReactRuntime<TProjectionState, TCommands>;
}

export function createCadenzaReactRuntime<
  TProjectionState extends Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
>(options: CadenzaReactRuntimeOptions<TProjectionState, TCommands>) {
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
  let snapshot = createProjectionRuntimeSnapshot(
    runtime.getRuntimeState(),
    initialProjectionState,
  );
  const runtimeCommands = commands
    ? commands({
        cadenza,
        runtime,
      })
    : ({} as TCommands);

  return {
    actor: runtime.actor,
    actorHandle: runtime,
    waitUntilReady: runtime.waitUntilReady,
    inquire: runtime.inquire,
    getRuntimeState: runtime.getRuntimeState,
    subscribe: (listener) =>
      runtime.subscribe((runtimeState) => {
        snapshot = createProjectionRuntimeSnapshot(
          runtimeState,
          initialProjectionState,
        );
        listener(runtimeState);
      }),
    getSnapshot: () => snapshot,
    commands: runtimeCommands,
  } satisfies CadenzaReactRuntime<TProjectionState, TCommands>;
}

export function CadenzaRuntimeProvider<
  TProjectionState extends Record<string, any>,
  TCommands extends Record<string, any> = Record<string, never>,
>({ runtime, children }: CadenzaRuntimeProviderProps<TProjectionState, TCommands>) {
  return createElement(CadenzaRuntimeContext.Provider, {
    value: runtime,
    children,
  });
}

export function useCadenzaRuntime<
  TRuntime extends CadenzaReactRuntime<any, any> = CadenzaReactRuntime<any, any>,
>(): TRuntime {
  return getRequiredRuntime<any, any>() as TRuntime;
}

export function useCadenzaProjectionState<
  TProjectionState extends Record<string, any> = Record<string, any>,
>(): CadenzaReactRuntimeState<TProjectionState> {
  const runtime = getRequiredRuntime<TProjectionState, Record<string, any>>();
  return useSyncExternalStore(
    runtime.subscribe,
    runtime.getSnapshot,
    runtime.getSnapshot,
  );
}

function useCadenzaSnapshotSelector<
  TProjectionState extends Record<string, any>,
  TSelected,
>(
  selector: (snapshot: CadenzaReactRuntimeState<TProjectionState>) => TSelected,
  isEqual: EqualityComparator<TSelected> = DEFAULT_SELECTOR_EQUALITY,
): TSelected {
  const runtime = getRequiredRuntime<TProjectionState, Record<string, any>>();
  const selectionCacheRef = useRef<{
    hasValue: boolean;
    value: TSelected;
  }>({
    hasValue: false,
    value: undefined as TSelected,
  });

  const getSelectedSnapshot = () => {
    const nextValue = selector(runtime.getSnapshot());
    const cached = selectionCacheRef.current;
    if (cached.hasValue && isEqual(cached.value, nextValue)) {
      return cached.value;
    }

    selectionCacheRef.current = {
      hasValue: true,
      value: nextValue,
    };
    return nextValue;
  };

  return useSyncExternalStore(
    runtime.subscribe,
    getSelectedSnapshot,
    getSelectedSnapshot,
  );
}

export function useCadenzaProjectionSelector<
  TProjectionState extends Record<string, any> = Record<string, any>,
  TSelected = TProjectionState,
>(
  selector: (state: TProjectionState) => TSelected,
  isEqual: EqualityComparator<TSelected> = DEFAULT_SELECTOR_EQUALITY,
): TSelected {
  return useCadenzaSnapshotSelector<TProjectionState, TSelected>(
    (snapshot) => selector(snapshot.projectionState as TProjectionState),
    isEqual,
  );
}

export function useCadenzaRuntimeReady(): boolean {
  return useCadenzaSnapshotSelector((snapshot) => snapshot.ready);
}

export type { BrowserRuntimeProjectionBinding };
