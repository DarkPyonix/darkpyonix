// FR-H2 (#132): `GET /pkarr/watch` long-polls the account's address records for changes.

import { describe, expect, it } from "vitest";
import { call, hubEnv, irohRecord, linkDevice, linkDeviceTokens, makeDeps, newDevice, signIn } from "./helpers";

const RELAY = "https://relay.example./";

async function account(id: number, overrides: Parameters<typeof makeDeps>[0] = {}) {
  const deps = makeDeps(overrides);
  const cookie = await signIn(deps, { id, login: `owner${id}` });
  const a = await newDevice();
  const b = await newDevice();
  const tokenA = await linkDevice(deps, { cookie }, a);
  await linkDevice(deps, { cookie }, b);
  return { deps, cookie, a, b, tokenA };
}

async function publish(deps: ReturnType<typeof makeDeps>, device: Awaited<ReturnType<typeof newDevice>>, ts: bigint) {
  const body = await irohRecord(device, RELAY, ["192.0.2.1:1234"], ts);
  const response = await call(deps, "PUT", `/pkarr/${device.z32}`, { body });
  expect(response.status).toBe(204);
  return body;
}

function b64(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}

type Records = { records: { key: string; timestamp: number; packet: string }[] };

describe("pkarr watch", () => {
  it("test_fr_h2_watch_returns_changed_records_at_once", async () => {
    let sleeps = 0;
    const { deps, a, b, tokenA } = await account(301, { sleep: async () => void sleeps++ });
    const packetA = await publish(deps, a, 100n);
    await publish(deps, b, 200n);
    // b is not newer than its `since`; a is.
    const response = await call(deps, "GET", `/pkarr/watch?keys=${a.z32},${b.z32}&since=0,200&wait=25`, { token: tokenA });
    expect(response.status).toBe(200);
    expect(sleeps).toBe(0);
    expect(await response.json()).toEqual({ records: [{ key: a.z32, timestamp: 100, packet: b64(packetA) }] });
  });

  it("test_fr_h2_watch_times_out_with_204", async () => {
    let sleeps = 0;
    const { deps, a, b, tokenA } = await account(302, { sleep: async () => void sleeps++ });
    await publish(deps, a, 100n);
    // b never published: allowed, just absent.
    const response = await call(deps, "GET", `/pkarr/watch?keys=${a.z32},${b.z32}&since=100,0&wait=10`, { token: tokenA });
    expect(response.status).toBe(204);
    expect(sleeps).toBe(5);
    sleeps = 0;
    expect((await call(deps, "GET", `/pkarr/watch?keys=${a.z32}&since=100`, { token: tokenA })).status).toBe(204);
    expect(sleeps).toBe(0);
  });

  it("test_fr_h2_watch_wakes_on_a_publish", async () => {
    let sleeps = 0;
    let change: () => Promise<unknown> = async () => undefined;
    const { deps, cookie, a, b } = await account(303, {
      sleep: async () => {
        sleeps++;
        if (sleeps === 3) await change();
      },
    });
    await publish(deps, a, 100n);
    let packetB: Uint8Array | null = null;
    change = async () => {
      packetB = await publish(deps, b, 7n);
    };
    const response = await call(deps, "GET", `/pkarr/watch?keys=${a.z32},${b.z32}&since=100,0&wait=25`, { cookie });
    expect(response.status).toBe(200);
    expect(sleeps).toBe(3);
    expect(await response.json()).toEqual({ records: [{ key: b.z32, timestamp: 7, packet: b64(packetB!) }] });
  });

  it("test_fr_h2_watch_refuses_foreign_or_unknown_keys_with_their_list", async () => {
    const { deps, a, tokenA } = await account(304);
    const other = await account(305);
    const unknown = await newDevice();
    const response = await call(deps, "GET", `/pkarr/watch?keys=${a.z32},${other.a.z32},${unknown.z32}&since=0,0,0`, { token: tokenA });
    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({ error: "not_found", keys: [other.a.z32, unknown.z32] });
  });

  it("test_fr_h2_watch_validates_params", async () => {
    const { deps, cookie, a, b } = await account(306);
    const keys33 = await Promise.all(Array.from({ length: 33 }, () => newDevice()));
    const bad = [
      "",
      `?keys=not-a-key&since=0`,
      `?keys=${a.z32}`,
      `?keys=${a.z32},${b.z32}&since=0`,
      `?keys=${a.z32},${a.z32}&since=0,0`,
      `?keys=${a.z32}&since=-1`,
      `?keys=${a.z32}&since=x`,
      `?keys=${a.z32}&since=0&wait=26`,
      `?keys=${a.z32}&since=0&wait=-1`,
      `?keys=${keys33.map((d) => d.z32).join(",")}&since=${keys33.map(() => "0").join(",")}`,
    ];
    for (const q of bad) {
      expect((await call(deps, "GET", `/pkarr/watch${q}`, { cookie })).status, q).toBe(400);
    }
    expect((await call(deps, "GET", `/pkarr/watch?keys=${a.z32}&since=0&wait=0`, { cookie })).status).toBe(204);
  });

  it("test_fr_h2_watch_needs_a_device_token_or_session", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 307, login: "owner307" });
    const a = await newDevice();
    const { device_token, resolve_token } = await linkDeviceTokens(deps, { cookie }, a);
    const q = `/pkarr/watch?keys=${a.z32}&since=0`;
    expect((await call(deps, "GET", q)).status).toBe(401);
    expect((await call(deps, "GET", `${q}&token=${resolve_token}`)).status).toBe(401);
    expect((await call(deps, "GET", `${q}&token=${device_token}`)).status).toBe(401);
    expect((await call(deps, "GET", q, { token: resolve_token })).status).toBe(401);
    expect((await call(deps, "GET", q, { token: device_token })).status).toBe(204);
    expect((await call(deps, "GET", q, { cookie })).status).toBe(204);
    // The iroh-compatible path is still served next to it.
    expect((await call(deps, "GET", `/pkarr/${a.z32}?token=${resolve_token}`)).status).toBe(404);
  });

  it("test_fr_h2_watch_ends_with_device_removed", async () => {
    let change: () => Promise<unknown> = async () => undefined;
    const { deps, cookie, a, b, tokenA } = await account(308, { sleep: () => change().then(() => undefined) });
    change = () => call(deps, "DELETE", `/devices/${a.endpointId}`, { cookie });
    const response = await call(deps, "GET", `/pkarr/watch?keys=${b.z32}&since=0&wait=25`, { token: tokenA });
    expect(response.status).toBe(401);
    expect(((await response.json()) as { code: string }).code).toBe("device_removed");
  });

  it("test_fr_h2_watch_is_rate_limited", async () => {
    // wrangler.toml binds WATCH_LIMITER (6 per device token per minute).
    const { deps, b, tokenA } = await account(309);
    const statuses: number[] = [];
    for (let i = 0; i < 7; i++) {
      statuses.push((await call(deps, "GET", `/pkarr/watch?keys=${b.z32}&since=0`, { token: tokenA })).status);
    }
    expect(statuses.slice(0, 6)).toEqual([204, 204, 204, 204, 204, 204]);
    expect(statuses[6]).toBe(429);
  });

  it("test_fr_h2_idle_watch_reads_only_the_version_row", async () => {
    const statements: string[] = [];
    let sleeps = 0;
    const perRound: number[] = [];
    let mark = 0;
    const { deps, a, b, tokenA } = await account(310, {
      sleep: async () => {
        if (sleeps > 0) perRound.push(statements.length - mark);
        sleeps++;
        mark = statements.length;
      },
    });
    await publish(deps, a, 100n);
    const real = hubEnv.DB;
    const counting = {
      prepare: (sql: string) => {
        statements.push(sql);
        return real.prepare(sql);
      },
      batch: (s: D1PreparedStatement[]) => real.batch(s),
    } as unknown as D1Database;
    const response = await call(deps, "GET", `/pkarr/watch?keys=${a.z32},${b.z32}&since=100,0&wait=10`, {
      token: tokenA,
      env: { DB: counting },
    });
    expect(response.status).toBe(204);
    perRound.push(statements.length - mark);
    expect(perRound).toEqual([1, 1, 1, 1, 1]);
    const waiting = statements.slice(statements.length - 5);
    expect(waiting.every((sql) => sql.includes("records_version") && !/\bFROM records\b/.test(sql))).toBe(true);
  });
});

describe("config", () => {
  it("test_fr_h8_config_advertises_pkarr_watch", async () => {
    const body = (await (await call(makeDeps(), "GET", "/config")).json()) as Record<string, unknown>;
    expect(body.pkarr_watch).toEqual({ max_keys: 32, max_wait: 25 });
  });
});
