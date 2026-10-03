import type { D1Migration } from "cloudflare:test";
import type { Env as HubEnv } from "../src/env";

// `env` from cloudflare:test / cloudflare:workers is typed as Cloudflare.Env.
declare global {
  namespace Cloudflare {
    interface Env extends HubEnv {
      TEST_MIGRATIONS: D1Migration[];
    }
  }
}
