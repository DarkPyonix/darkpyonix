// FR-H2: the pkarr relay protocol as iroh's PkarrPublisher / PkarrResolver use it.

import { describe, expect, it } from "vitest";
import { base64url } from "../src/util";
import { call, irohRecord, linkDevice, linkDeviceTokens, makeDeps, newDevice, signIn } from "./helpers";

const RELAY = "https://relay.darkpyonix.dev/";

describe("address directory", () => {
  it("test_fr_h2_publish_and_resolve_like_stock_iroh", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 21, login: "owner" });
    const a = await newDevice();
    const b = await newDevice();
    await linkDevice(deps, { cookie }, a);
    const { device_token: tokenB, resolve_token: resolveB } = await linkDeviceTokens(deps, { cookie }, b);
    const payload = await irohRecord(a, RELAY, ["192.0.2.7:51000"], 1_790_000_000_000_000n);
    // PkarrPublisher: PUT <relay>/<z32>, no auth header.
    expect((await call(deps, "PUT", `/pkarr/${a.z32}`, { body: payload })).status).toBe(204);
    // PkarrResolver configured with https://darkpyonix.dev/pkarr?token=<resolve token> (NFR-H2).
    const resolved = await call(deps, "GET", `/pkarr/${a.z32}?token=${encodeURIComponent(resolveB)}`);
    expect(resolved.status).toBe(200);
    expect(resolved.headers.get("content-type")).toBe("application/octet-stream");
    expect(new Uint8Array(await resolved.arrayBuffer())).toEqual(payload);

    const json = (await (await call(deps, "GET", `/v1/devices/${a.endpointId}/addresses`, { token: tokenB })).json()) as Record<string, unknown>;
    expect(json).toEqual({
      endpoint_id: a.endpointId,
      relay_urls: [RELAY],
      direct_addresses: ["192.0.2.7:51000"],
      published_at_us: 1_790_000_000_000_000,
      signed_packet: base64url(payload),
    });
  });

  it("test_fr_h2_only_newer_packets_replace_the_stored_one", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 22, login: "owner" });
    const a = await newDevice();
    await linkDevice(deps, { cookie }, a);
    expect((await call(deps, "PUT", `/pkarr/${a.z32}`, { body: await irohRecord(a, RELAY, [], 2000n) })).status).toBe(204);
    expect((await call(deps, "PUT", `/pkarr/${a.z32}`, { body: await irohRecord(a, RELAY, [], 2000n) })).status).toBe(409);
    expect((await call(deps, "PUT", `/pkarr/${a.z32}`, { body: await irohRecord(a, RELAY, [], 1000n) })).status).toBe(409);
    expect((await call(deps, "PUT", `/pkarr/${a.z32}`, { body: await irohRecord(a, RELAY, [], 3000n) })).status).toBe(204);
  });

  it("test_fr_h2_directory_rejects_unregistered_and_foreign", async () => {
    const deps = makeDeps();
    const alice = await signIn(deps, { id: 23, login: "alice" });
    const bob = await signIn(deps, { id: 24, login: "bob" });
    const a = await newDevice();
    await linkDevice(deps, { cookie: alice }, a);
    const stranger = await newDevice();
    expect((await call(deps, "PUT", `/pkarr/${stranger.z32}`, { body: await irohRecord(stranger, RELAY, [], 1n) })).status).toBe(403);
    // Signed by someone else's key for a's slot.
    expect((await call(deps, "PUT", `/pkarr/${a.z32}`, { body: await irohRecord(stranger, RELAY, [], 1n) })).status).toBe(400);
    await call(deps, "PUT", `/pkarr/${a.z32}`, { body: await irohRecord(a, RELAY, [], 1n) });
    expect((await call(deps, "GET", `/pkarr/${a.z32}`)).status).toBe(401);
    expect((await call(deps, "GET", `/pkarr/${a.z32}`, { cookie: bob })).status).toBe(404);
    expect((await call(deps, "GET", "/pkarr/not-a-key")).status).toBe(400);
  });

  it("test_fr_h2_unauthenticated_publishes_are_rate_limited", async () => {
    // wrangler.toml binds WRITE_LIMITER (30 per key per minute); the pool runs the real binding.
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 25, login: "owner" });
    const a = await newDevice();
    await linkDevice(deps, { cookie }, a);
    const statuses: number[] = [];
    for (let ts = 1n; ts <= 31n; ts++) {
      statuses.push((await call(deps, "PUT", `/pkarr/${a.z32}`, { body: await irohRecord(a, RELAY, [], ts) })).status);
    }
    expect(statuses.slice(0, 30).every((s) => s === 204)).toBe(true);
    expect(statuses[30]).toBe(429);
  });
});
