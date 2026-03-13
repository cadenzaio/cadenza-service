import { defineConfig } from "tsup";

const sharedConfig = {
  entry: ["src/index.ts"],
  format: ["esm", "cjs"] as const,
  sourcemap: true,
  tsconfig: "./tsconfig.json",
};

export default defineConfig([
  {
    ...sharedConfig,
    dts: {
      entry: "src/index.ts",
      compilerOptions: {
        module: "NodeNext",
        moduleResolution: "NodeNext",
      },
    },
    clean: true,
    outDir: "dist",
  },
  {
    ...sharedConfig,
    dts: false,
    clean: false,
    platform: "browser",
    outDir: "dist/browser",
    esbuildOptions(options) {
      options.alias = {
        ...(options.alias ?? {}),
        "@service-rest-controller":
          "./src/network/RestController.browser.ts",
        "@service-database-controller":
          "./src/database/DatabaseController.browser.ts",
      };
    },
  },
]);
