import path from "node:path";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";

describe("browser bundle regression", () => {
  it("bundles the package entry for browser consumers", async () => {
    const repoRoot = path.resolve(__dirname, "..");
    const result = await build({
      entryPoints: [path.resolve(repoRoot, "src/index.ts")],
      bundle: true,
      platform: "browser",
      format: "esm",
      write: false,
      alias: {
        "@service-rest-controller": path.resolve(
          repoRoot,
          "src/network/RestController.browser.ts",
        ),
        "@service-database-controller": path.resolve(
          repoRoot,
          "src/database/DatabaseController.browser.ts",
        ),
      },
    });

    expect(result.outputFiles.length).toBeGreaterThan(0);
  });
});
