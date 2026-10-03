import path from "node:path";
import { defineWorkersConfig, readD1Migrations } from "@cloudflare/vitest-pool-workers/config";

export default defineWorkersConfig(async () => {
  const migrations = await readD1Migrations(path.join(__dirname, "migrations"));
  return {
    test: {
      setupFiles: ["./test/apply-migrations.ts"],
      poolOptions: {
        workers: {
          singleWorker: true,
          wrangler: { configPath: "./wrangler.toml" },
          miniflare: {
            bindings: {
              TEST_MIGRATIONS: migrations,
              GITHUB_CLIENT_ID: "Iv1.testclient",
              GITHUB_CLIENT_SECRET: "test-secret",
              RELAY_SHARED_SECRET: "relay-secret",
            },
          },
        },
      },
    },
  };
});
