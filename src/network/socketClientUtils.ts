type WaitOutcome<TError> =
  | { ok: true }
  | { ok: false; error: TError };

type SocketLike = {
  connected?: boolean;
  once: (event: string, listener: (...args: any[]) => void) => any;
  off: (event: string, listener: (...args: any[]) => void) => any;
};

/**
 * Waits for a socket to become connected with explicit timeout/error outcomes.
 * Ensures listeners are removed in all exit paths to avoid accumulation across retries.
 */
export const waitForSocketConnection = async <TError>(
  socket: SocketLike | null,
  timeoutMs: number,
  createError: (reason: "connect_timeout" | "connect_error" | "disconnected", error?: unknown) => TError,
): Promise<WaitOutcome<TError>> => {
  if (!socket) {
    return { ok: false, error: createError("disconnected") };
  }

  if (socket.connected) {
    return { ok: true };
  }

  return new Promise((resolve) => {
    let timer: NodeJS.Timeout | null = null;
    let settled = false;

    const cleanup = () => {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }

      socket.off("connect", onConnect);
      socket.off("connect_error", onConnectError);
      socket.off("disconnect", onDisconnect);
    };

    const settle = (outcome: WaitOutcome<TError>) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(outcome);
    };

    const onConnect = () => settle({ ok: true });
    const onConnectError = (error: unknown) =>
      settle({ ok: false, error: createError("connect_error", error) });
    const onDisconnect = () =>
      settle({ ok: false, error: createError("disconnected") });

    socket.once("connect", onConnect);
    socket.once("connect_error", onConnectError);
    socket.once("disconnect", onDisconnect);

    timer = setTimeout(() => {
      settle({ ok: false, error: createError("connect_timeout") });
    }, timeoutMs);
  });
};
