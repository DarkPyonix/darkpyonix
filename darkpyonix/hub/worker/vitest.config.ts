import { fileURLToPath } from "node:url";
import { cloudflareTest, readD1Migrations } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

// Tests run inside workerd (the pool's own Miniflare), with the bindings of wrangler.toml:
// D1 (migrated per test file by test/apply-migrations.ts), the assets and the rate limiter.
export default defineConfig({
  plugins: [
    cloudflareTest(async () => ({
      wrangler: { configPath: "./wrangler.toml" },
      miniflare: {
        bindings: {
          TEST_MIGRATIONS: await readD1Migrations(fileURLToPath(new URL("./migrations", import.meta.url))),
          GITHUB_CLIENT_ID: "Iv1.testclient",
          GITHUB_CLIENT_SECRET: "test-secret",
          RELAY_SHARED_SECRET: "relay-secret",
        },
      },
    })),
  ],
  test: {
    setupFiles: ["./test/apply-migrations.ts"],
  },
});
