// NFR-H2: device tokens stay out of URLs; a read-only resolve token takes their place in
// iroh's PkarrResolver query, and the Worker never logs what it was asked.

import { afterEach, describe, expect, it, vi } from "vitest";
import wranglerToml from "../wrangler.toml?raw";
import { call, irohRecord, linkDeviceTokens, makeDeps, newDevice, signIn } from "./helpers";

const RELAY = "https://relay.darkpyonix.dev/";

async function setup(id: number) {
  const deps = makeDeps();
  const cookie = await signIn(deps, { id, login: "owner" });
  const a = await newDevice();
  const b = await newDevice();
  await linkDeviceTokens(deps, { cookie }, a);
  const tokensB = await linkDeviceTokens(deps, { cookie }, b);
  expect((await call(deps, "PUT", `/pkarr/${a.z32}`, { body: await irohRecord(a, RELAY, [], 5n) })).status).toBe(204);
  return { deps, cookie, a, b, tokensB };
}

async function code(response: Response): Promise<string | undefined> {
  return ((await response.json()) as { code?: string }).code;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("resolve tokens", () => {
  it("test_nfr_h2_resolve_token_reads_records_and_nothing_else", async () => {
    const { deps, a, tokensB } = await setup(101);
    expect(tokensB.resolve_token).toMatch(/^dpr_/);
    expect((await call(deps, "GET", `/pkarr/${a.z32}?token=${tokensB.resolve_token}`)).status).toBe(200);
    expect((await call(deps, "GET", `/pkarr/${a.z32}`, { token: tokensB.resolve_token })).status).toBe(200);
    for (const path of ["/v1/devices", "/v1/me", `/v1/devices/${a.endpointId}/addresses`]) {
      expect((await call(deps, "GET", path, { token: tokensB.resolve_token })).status, path).toBe(401);
    }
    expect((await call(deps, "POST", "/v1/me/resolve-token", { token: tokensB.resolve_token })).status).toBe(401);
  });

  it("test_nfr_h2_query_refuses_device_tokens", async () => {
    const { deps, a, tokensB } = await setup(102);
    const refused = await call(deps, "GET", `/pkarr/${a.z32}?token=${tokensB.device_token}`);
    expect(refused.status).toBe(401);
    expect(await code(refused)).toBe("invalid_credentials");
    // In the header it is still a credential.
    expect((await call(deps, "GET", `/pkarr/${a.z32}`, { token: tokensB.device_token })).status).toBe(200);
  });

  it("test_nfr_h2_resolve_token_rotates", async () => {
    const { deps, a, tokensB } = await setup(103);
    const rotated = await call(deps, "POST", "/v1/me/resolve-token", { token: tokensB.device_token });
    expect(rotated.status).toBe(201);
    const fresh = ((await rotated.json()) as { resolve_token: string }).resolve_token;
    expect(fresh).toMatch(/^dpr_/);
    expect(fresh).not.toBe(tokensB.resolve_token);
    expect((await call(deps, "GET", `/pkarr/${a.z32}?token=${tokensB.resolve_token}`)).status).toBe(401);
    expect((await call(deps, "GET", `/pkarr/${a.z32}?token=${fresh}`)).status).toBe(200);
  });

  it("test_nfr_h2_removed_device_resolve_token_is_device_removed", async () => {
    const { deps, cookie, a, b, tokensB } = await setup(104);
    expect((await call(deps, "DELETE", `/v1/devices/${b.endpointId}`, { cookie })).status).toBe(204);
    const removed = await call(deps, "GET", `/pkarr/${a.z32}?token=${tokensB.resolve_token}`);
    expect(removed.status).toBe(401);
    expect(await code(removed)).toBe("device_removed");
  });
});

describe("logging", () => {
  it("test_nfr_h2_worker_never_logs_the_query", async () => {
    const { deps, a, tokensB } = await setup(105);
    const lines: string[] = [];
    for (const level of ["log", "info", "warn", "error", "debug"] as const) {
      vi.spyOn(console, level).mockImplementation((...args: unknown[]) => {
        lines.push(args.map((x) => (x instanceof Error ? `${x.message} ${x.stack}` : String(x))).join(" "));
      });
    }
    const other = await newDevice();
    await call(deps, "GET", `/pkarr/${a.z32}?token=${tokensB.resolve_token}`); // 200
    await call(deps, "GET", `/pkarr/${a.z32}?token=${tokensB.device_token}`); // 401
    await call(deps, "GET", `/pkarr/${other.z32}?token=${tokensB.resolve_token}`); // 404
    // An unhandled failure while the query is being served.
    const broken = {
      prepare() {
        throw new Error("database unavailable");
      },
    } as unknown as D1Database;
    const failed = await call(deps, "GET", `/pkarr/${a.z32}?token=${tokensB.resolve_token}`, { env: { DB: broken } });
    expect(failed.status).toBe(500);
    expect(lines.length).toBeGreaterThan(0); // the failure itself is logged...
    for (const line of lines) {
      // ...but never the URL or the token.
      expect(line).not.toContain(tokensB.resolve_token);
      expect(line).not.toContain(tokensB.device_token);
      expect(line).not.toContain("/pkarr/");
    }
  });

  it("test_nfr_h2_invocation_logs_are_off", () => {
    const section = /\[observability\.logs\]([\s\S]*?)(\n\[|$)/.exec(wranglerToml);
    expect(section, "wrangler.toml needs [observability.logs]").not.toBeNull();
    expect(section![1]).toMatch(/^\s*invocation_logs\s*=\s*false\s*$/m);
  });
});
