import { defineConfig } from "tsup";

const sharedConfig = {
  entry: {
    index: "src/index.ts",
    "nuxt/index": "src/nuxt/index.ts",
    "react/index": "src/react/index.ts",
    "vue/index": "src/vue/index.ts",
  },
  format: ["esm", "cjs"] as const,
  sourcemap: true,
  tsconfig: "./tsconfig.json",
};

export default defineConfig([
  {
    ...sharedConfig,
    dts: {
      entry: {
        index: "src/index.ts",
        "nuxt/index": "src/nuxt/index.ts",
        "react/index": "src/react/index.ts",
        "vue/index": "src/vue/index.ts",
      },
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
