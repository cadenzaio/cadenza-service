import path from "node:path";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "@service-rest-controller": path.resolve(
        __dirname,
        "src/network/RestController.ts",
      ),
      "@service-database-controller": path.resolve(
        __dirname,
        "src/database/DatabaseController.ts",
      ),
    },
  },
  test: {
    globals: true,
    environment: "node",
    include: ["tests/**/*.test.ts"],
    setupFiles: [],
  },
});
