/**
 * Pauses the execution of the code for a specified duration.
 *
 * @param {number} ms - The number of milliseconds to pause execution.
 * @return {Promise<void>} A promise that resolves after the specified duration.
 */
export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
