import type { Actor, AnyObject } from "@cadenza.io/core";

import type { ServerOptions } from "../Cadenza";
import type { DistributedInquiryOptions } from "../types/inquiry";
import type { BootstrapOptions, HydrationOptions } from "../utils/bootstrap";
import { resolveInitialProjectionState } from "./runtimeProjectionState";

export interface BrowserRuntimeActorRuntimeState<
  TProjectionState extends Record<string, any> = Record<string, any>,
> {
  ready: boolean;
  projectionState: TProjectionState;
  lastReadyAt: string | null;
  lastSyncRequestedAt: string | null;
}

export interface BrowserRuntimeProjectionBinding<
  TProjectionState extends Record<string, any> = Record<string, any>,
> {
  signal: string;
  taskName?: string;
  description?: string;
  reduce: (
    current: TProjectionState,
    payload: Record<string, any>,
  ) => TProjectionState;
}

export interface BrowserRuntimeServiceOptions
  extends Omit<ServerOptions, "isFrontend" | "bootstrap" | "hydration"> {
  name: string;
  description?: string;
  bootstrap: BootstrapOptions;
  hydration?: HydrationOptions;
}

export interface BrowserRuntimeActorOptions<
  TProjectionState extends Record<string, any> = Record<string, any>,
> {
  actorName: string;
  actorDescription?: string;
  actorKey?: string;
  service: BrowserRuntimeServiceOptions;
  initialProjectionState: TProjectionState | (() => TProjectionState);
  signalBindings?: Array<BrowserRuntimeProjectionBinding<TProjectionState>>;
  readySignal?: string;
  syncSignals?: string[];
}

export interface BrowserRuntimeActorHandle<
  TProjectionState extends Record<string, any> = Record<string, any>,
> {
  actor: Actor<Record<string, never>, BrowserRuntimeActorRuntimeState<TProjectionState>>;
  actorKey: string;
  waitUntilReady: () => Promise<void>;
  inquire: (
    inquiry: string,
    context?: Record<string, any>,
    options?: DistributedInquiryOptions,
  ) => Promise<AnyObject>;
  getRuntimeState: () => BrowserRuntimeActorRuntimeState<TProjectionState>;
  subscribe: (
    listener: (
      state: BrowserRuntimeActorRuntimeState<TProjectionState>,
    ) => void,
  ) => () => void;
}

interface BrowserRuntimeInternalHandle<
  TProjectionState extends Record<string, any> = Record<string, any>,
> {
  listeners: Set<(state: BrowserRuntimeActorRuntimeState<TProjectionState>) => void>;
  readyPromise: Promise<void>;
  resolveReady: () => void;
  readyResolved: boolean;
}

type BrowserRuntimeActorInstance<
  TProjectionState extends Record<string, any>,
> = Actor<Record<string, never>, BrowserRuntimeActorRuntimeState<TProjectionState>>;

type BrowserRuntimeCadenzaLike = {
  createCadenzaService: (
    serviceName: string,
    description?: string,
    options?: ServerOptions,
  ) => void;
  createActor: <
    D extends Record<string, any>,
    R = AnyObject,
  >(
    spec: Record<string, any>,
    options?: Record<string, any>,
  ) => Actor<D, R>;
  getActor: <D extends Record<string, any>, R = AnyObject>(
    actorName: string,
  ) => Actor<D, R> | undefined;
  createTask: (
    name: string,
    taskFunction: (...args: any[]) => any,
    description?: string,
  ) => {
    doOn: (...signals: string[]) => unknown;
  };
  get: (taskName: string) => unknown;
  inquire: (
    inquiry: string,
    context?: Record<string, any>,
    options?: DistributedInquiryOptions,
  ) => Promise<AnyObject>;
};

const DEFAULT_READY_SIGNAL = "meta.service_registry.initial_sync_complete";
const DEFAULT_SYNC_SIGNALS = [
  "meta.fetch.handshake_complete",
  "meta.socket.handshake",
];
const DEFAULT_ACTOR_KEY = "browser-runtime";

const browserRuntimeHandles = new Map<string, BrowserRuntimeInternalHandle<any>>();

function createIdentityKey(actorName: string, actorKey: string): string {
  return `${actorName}::${actorKey}`;
}

function createDefaultRuntimeState<TProjectionState extends Record<string, any>>(
  initialProjectionState: TProjectionState | (() => TProjectionState),
): BrowserRuntimeActorRuntimeState<TProjectionState> {
  return {
    ready: false,
    projectionState: resolveInitialProjectionState(initialProjectionState),
    lastReadyAt: null,
    lastSyncRequestedAt: null,
  };
}

function ensureInternalHandle<TProjectionState extends Record<string, any>>(
  identityKey: string,
): BrowserRuntimeInternalHandle<TProjectionState> {
  const existing = browserRuntimeHandles.get(identityKey);
  if (existing) {
    return existing as BrowserRuntimeInternalHandle<TProjectionState>;
  }

  let resolveReady = () => {};
  const readyPromise = new Promise<void>((resolve) => {
    resolveReady = resolve;
  });

  const created: BrowserRuntimeInternalHandle<TProjectionState> = {
    listeners: new Set(),
    readyPromise,
    resolveReady,
    readyResolved: false,
  };
  browserRuntimeHandles.set(identityKey, created);
  return created;
}

function resolveInternalReady<TProjectionState extends Record<string, any>>(
  internalHandle: BrowserRuntimeInternalHandle<TProjectionState>,
): void {
  if (internalHandle.readyResolved) {
    return;
  }

  internalHandle.readyResolved = true;
  internalHandle.resolveReady();
}

function notifyListeners<TProjectionState extends Record<string, any>>(
  internalHandle: BrowserRuntimeInternalHandle<TProjectionState>,
  nextState: BrowserRuntimeActorRuntimeState<TProjectionState>,
): void {
  if (nextState.ready) {
    resolveInternalReady(internalHandle);
  }

  for (const listener of internalHandle.listeners) {
    listener(nextState);
  }
}

function getRuntimeStateSnapshot<TProjectionState extends Record<string, any>>(
  actor: BrowserRuntimeActorInstance<TProjectionState>,
  actorKey: string,
  initialProjectionState: TProjectionState | (() => TProjectionState),
): BrowserRuntimeActorRuntimeState<TProjectionState> {
  return (
    actor.getRuntimeState(actorKey) ??
    createDefaultRuntimeState(initialProjectionState)
  );
}

function registerReadyTask<TProjectionState extends Record<string, any>>(
  cadenza: BrowserRuntimeCadenzaLike,
  actor: BrowserRuntimeActorInstance<TProjectionState>,
  actorKey: string,
  actorName: string,
  readySignal: string,
  initialProjectionState: TProjectionState | (() => TProjectionState),
  internalHandle: BrowserRuntimeInternalHandle<TProjectionState>,
): void {
  const taskName = `${actorName}.MarkReady`;
  if (cadenza.get(taskName)) {
    return;
  }

  cadenza
    .createTask(
      taskName,
      actor.task(
        ({ runtimeState, setRuntimeState }) => {
          const current =
            runtimeState ?? createDefaultRuntimeState(initialProjectionState);
          const nextState: BrowserRuntimeActorRuntimeState<TProjectionState> = {
            ...current,
            ready: true,
            lastReadyAt: new Date().toISOString(),
          };
          setRuntimeState(nextState);
          notifyListeners(internalHandle, nextState);
          return true;
        },
        {
          mode: "write",
        },
      ),
      "Marks the browser runtime actor as ready after the initial frontend sync completes.",
    )
    .doOn(readySignal);
}

function registerSyncKickoffTask<TProjectionState extends Record<string, any>>(
  cadenza: BrowserRuntimeCadenzaLike,
  actor: BrowserRuntimeActorInstance<TProjectionState>,
  actorKey: string,
  actorName: string,
  syncSignals: string[],
  initialProjectionState: TProjectionState | (() => TProjectionState),
  internalHandle: BrowserRuntimeInternalHandle<TProjectionState>,
): void {
  const taskName = `${actorName}.RequestInitialSync`;
  if (cadenza.get(taskName)) {
    return;
  }

  cadenza
    .createTask(
      taskName,
      actor.task(
        ({ emit, input, runtimeState, setRuntimeState }) => {
          const current =
            runtimeState ?? createDefaultRuntimeState(initialProjectionState);
          const nextState: BrowserRuntimeActorRuntimeState<TProjectionState> = {
            ...current,
            lastSyncRequestedAt: new Date().toISOString(),
          };

          setRuntimeState(nextState);
          notifyListeners(internalHandle, nextState);
          emit("meta.sync_requested", {
            __syncing: false,
            __triggeredBy: "browser-runtime-actor",
            ...input,
          });
          return true;
        },
        {
          mode: "write",
        },
      ),
      "Requests the initial browser runtime sync after transport handshakes complete.",
    )
    .doOn(...syncSignals);
}

function registerSignalProjectionTasks<TProjectionState extends Record<string, any>>(
  cadenza: BrowserRuntimeCadenzaLike,
  actor: BrowserRuntimeActorInstance<TProjectionState>,
  actorKey: string,
  actorName: string,
  initialProjectionState: TProjectionState | (() => TProjectionState),
  signalBindings: Array<BrowserRuntimeProjectionBinding<TProjectionState>>,
  internalHandle: BrowserRuntimeInternalHandle<TProjectionState>,
): void {
  for (const binding of signalBindings) {
    const taskName = binding.taskName ?? `${actorName}.Project ${binding.signal}`;
    if (cadenza.get(taskName)) {
      continue;
    }

    cadenza
      .createTask(
        taskName,
        actor.task(
          ({ input, runtimeState, setRuntimeState }) => {
            const current =
              runtimeState ?? createDefaultRuntimeState(initialProjectionState);
            const nextProjectionState = binding.reduce(
              current.projectionState,
              input,
            );
            const nextState: BrowserRuntimeActorRuntimeState<TProjectionState> = {
              ...current,
              projectionState: nextProjectionState,
            };

            setRuntimeState(nextState);
            notifyListeners(internalHandle, nextState);
            return input;
          },
          {
            mode: "write",
          },
        ),
        binding.description ??
          `Projects the ${binding.signal} signal into browser runtime actor state.`,
      )
      .doOn(binding.signal);
  }
}

export function createBrowserRuntimeActor<
  TProjectionState extends Record<string, any>,
>(
  cadenza: BrowserRuntimeCadenzaLike,
  options: BrowserRuntimeActorOptions<TProjectionState>,
): BrowserRuntimeActorHandle<TProjectionState> {
  const actorKey = options.actorKey ?? DEFAULT_ACTOR_KEY;
  const identityKey = createIdentityKey(options.actorName, actorKey);
  const internalHandle = ensureInternalHandle<TProjectionState>(identityKey);
  const readySignal = options.readySignal ?? DEFAULT_READY_SIGNAL;
  const syncSignals = options.syncSignals ?? DEFAULT_SYNC_SIGNALS;

  let actor =
    cadenza.getActor<Record<string, never>, BrowserRuntimeActorRuntimeState<TProjectionState>>(
      options.actorName,
    ) as BrowserRuntimeActorInstance<TProjectionState> | undefined;

  if (!actor) {
    actor = cadenza.createActor<
      Record<string, never>,
      BrowserRuntimeActorRuntimeState<TProjectionState>
    >({
      name: options.actorName,
      description:
        options.actorDescription ??
        "Framework-agnostic browser runtime actor for frontend Cadenza environments.",
      defaultKey: actorKey,
      initState: {},
      loadPolicy: "eager",
      writeContract: "overwrite",
    });
  }

  const {
    name: serviceName,
    description: serviceDescription = "",
    bootstrap,
    hydration,
    ...serviceOptions
  } = options.service;

  cadenza.createCadenzaService(serviceName, serviceDescription, {
    ...serviceOptions,
    isFrontend: true,
    bootstrap,
    hydration,
  });

  registerReadyTask(
    cadenza,
    actor,
    actorKey,
    options.actorName,
    readySignal,
    options.initialProjectionState,
    internalHandle,
  );
  registerSyncKickoffTask(
    cadenza,
    actor,
    actorKey,
    options.actorName,
    syncSignals,
    options.initialProjectionState,
    internalHandle,
  );
  registerSignalProjectionTasks(
    cadenza,
    actor,
    actorKey,
    options.actorName,
    options.initialProjectionState,
    options.signalBindings ?? [],
    internalHandle,
  );

  const currentState = getRuntimeStateSnapshot(
    actor,
    actorKey,
    options.initialProjectionState,
  );
  if (currentState.ready) {
    resolveInternalReady(internalHandle);
  }

  return {
    actor,
    actorKey,
    waitUntilReady: async () => {
      if (
        getRuntimeStateSnapshot(actor, actorKey, options.initialProjectionState).ready
      ) {
        return;
      }

      await internalHandle.readyPromise;
    },
    inquire: async (inquiry, context = {}, inquiryOptions = {}) => {
      if (
        !getRuntimeStateSnapshot(actor, actorKey, options.initialProjectionState).ready
      ) {
        await internalHandle.readyPromise;
      }

      return cadenza.inquire(inquiry, context, inquiryOptions);
    },
    getRuntimeState: () =>
      getRuntimeStateSnapshot(actor, actorKey, options.initialProjectionState),
    subscribe: (listener) => {
      internalHandle.listeners.add(listener);
      listener(
        getRuntimeStateSnapshot(actor, actorKey, options.initialProjectionState),
      );

      return () => {
        internalHandle.listeners.delete(listener);
      };
    },
  };
}

export function resetBrowserRuntimeActorHandles(): void {
  browserRuntimeHandles.clear();
}
