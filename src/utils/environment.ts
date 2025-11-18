/**
 * A boolean variable indicating whether the current runtime environment is Node.js.
 *
 * This variable checks for the presence of the `process` object, which is specific to Node.js,
 * and verifies if the `process.versions.node` property is defined to confirm the environment.
 *
 * If the runtime is Node.js, `isNode` will be `true`; otherwise, it will be `false`.
 */
export const isNode =
  typeof process !== "undefined" && process.versions?.node != null;

/**
 * A boolean variable that indicates whether the current runtime environment
 * is a browser. It checks for the presence of the `window` object and the
 * `document` property within it to determine if the code is running in a
 * browser context.
 *
 * - Returns `true` if the `window` object and its `document` property are
 *   both defined, which typically indicates a browser environment.
 * - Returns `false` if the `window` object or `document` property is
 *   undefined, which typically signifies a non-browser environment such
 *   as a Node.js environment.
 */
export const isBrowser =
  typeof window !== "undefined" && typeof window.document !== "undefined";
