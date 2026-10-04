// Worker entry for darkpyonix.dev (SPEC §10).

import { cleanup, handle } from "./app";
import { CloudflareDns } from "./dns";
import type { Deps, Env } from "./env";

function deps(ctx: ExecutionContext): Deps {
  return {
    fetch: (input, init) => fetch(input, init),
    nowMs: () => Date.now(),
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    dns: (env) => (env.CF_API_TOKEN && env.CF_ZONE_ID ? new CloudflareDns(env.CF_API_TOKEN, env.CF_ZONE_ID, (i, n) => fetch(i, n)) : null),
    waitUntil: (promise) => ctx.waitUntil(promise),
  };
}

export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    return handle(request, env, deps(ctx));
  },
  scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): void {
    ctx.waitUntil(cleanup(env, Date.now()));
  },
} satisfies ExportedHandler<Env>;
