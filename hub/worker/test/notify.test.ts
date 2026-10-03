// FR-H9: the device list carries an ETag; clients long-poll it instead of re-reading every minute.

import { describe, expect, it } from "vitest";
import { call, irohRecord, linkDevice, makeDeps, newDevice, signIn } from "./helpers";

const RELAY_SECRET = "relay-secret";

async function account(id: number, overrides: Parameters<typeof makeDeps>[0] = {}) {
  const deps = makeDeps(overrides);
  const cookie = await signIn(deps, { id, login: "owner" });
  const a = await newDevice();
  const b = await newDevice();
  const tokenA = await linkDevice(deps, { cookie }, a);
  const tokenB = await linkDevice(deps, { cookie }, b);
  return { deps, cookie, a, b, tokenA, tokenB };
}

async function etagOf(deps: ReturnType<typeof makeDeps>, token: string): Promise<string> {
  const response = await call(deps, "GET", "/v1/devices", { token });
  expect(response.status).toBe(200);
  const etag = response.headers.get("etag");
  expect(etag).toMatch(/^W\/"v\d+"$/);
  return etag!;
}

describe("device list notification", () => {
  it("test_fr_h9_device_list_has_an_etag_and_answers_304", async () => {
    const { deps, tokenA } = await account(201);
    const etag = await etagOf(deps, tokenA);
    const same = await call(deps, "GET", "/v1/devices", { token: tokenA, headers: { "if-none-match": etag } });
    expect(same.status).toBe(304);
    expect(same.headers.get("etag")).toBe(etag);
    expect(await same.text()).toBe("");
    const other = await call(deps, "GET", "/v1/devices", { token: tokenA, headers: { "if-none-match": 'W/"v0"' } });
    expect(other.status).toBe(200);
  });

  it("test_fr_h9_long_poll_wakes_on_a_change", async () => {
    let sleeps = 0;
    let change: () => Promise<unknown> = async () => undefined;
    const { deps, cookie, b, tokenA } = await account(202, {
      sleep: async () => {
        sleeps++;
        if (sleeps === 2) await change();
      },
    });
    const etag = await etagOf(deps, tokenA);
    change = () => call(deps, "PATCH", `/v1/devices/${b.endpointId}`, { cookie, json: { name: "renamed" } });
    const woke = await call(deps, "GET", "/v1/devices?wait=25", { token: tokenA, headers: { "if-none-match": etag } });
    expect(woke.status).toBe(200);
    expect(woke.headers.get("etag")).not.toBe(etag);
    const body = (await woke.json()) as { devices: { endpoint_id: string; name: string }[] };
    expect(body.devices.find((d) => d.endpoint_id === b.endpointId)?.name).toBe("renamed");
    expect(sleeps).toBe(2);

    // Removal wakes waiters too.
    sleeps = 0;
    const etag2 = woke.headers.get("etag")!;
    change = () => call(deps, "DELETE", `/v1/devices/${b.endpointId}`, { cookie });
    const removed = await call(deps, "GET", "/v1/devices?wait=25", { token: tokenA, headers: { "if-none-match": etag2 } });
    expect(removed.status).toBe(200);
    expect(((await removed.json()) as { devices: unknown[] }).devices).toHaveLength(1);
  });

  it("test_fr_h9_long_poll_times_out_with_304", async () => {
    let sleeps = 0;
    const { deps, tokenA } = await account(203, { sleep: async () => void sleeps++ });
    const etag = await etagOf(deps, tokenA);
    const response = await call(deps, "GET", "/v1/devices?wait=10", { token: tokenA, headers: { "if-none-match": etag } });
    expect(response.status).toBe(304);
    expect(sleeps).toBe(5); // every 2 seconds for 10 seconds
    // Without wait, no sleeping at all.
    sleeps = 0;
    expect((await call(deps, "GET", "/v1/devices", { token: tokenA, headers: { "if-none-match": etag } })).status).toBe(304);
    expect(sleeps).toBe(0);
  });

  it("test_fr_h9_waiting_device_that_is_removed_gets_device_removed", async () => {
    let change: () => Promise<unknown> = async () => undefined;
    const { deps, cookie, a, tokenA } = await account(204, { sleep: () => change().then(() => undefined) });
    const etag = await etagOf(deps, tokenA);
    change = () => call(deps, "DELETE", `/v1/devices/${a.endpointId}`, { cookie });
    const response = await call(deps, "GET", "/v1/devices?wait=25", { token: tokenA, headers: { "if-none-match": etag } });
    expect(response.status).toBe(401);
    expect(((await response.json()) as { code: string }).code).toBe("device_removed");
  });

  it("test_fr_h9_only_visible_changes_move_the_etag", async () => {
    const { deps, a, tokenA } = await account(205);
    const first = await etagOf(deps, tokenA);
    expect((await call(deps, "PUT", `/pkarr/${a.z32}`, { body: await irohRecord(a, "https://relay.darkpyonix.dev/", [], 9n) })).status).toBe(204);
    expect(await etagOf(deps, tokenA)).toBe(first);
    const presence = (online: boolean) =>
      call(deps, "POST", "/internal/v1/relay/presence", { token: RELAY_SECRET, json: { endpoint_id: a.endpointId, online } });
    expect((await presence(true)).status).toBe(204);
    const online = await etagOf(deps, tokenA);
    expect(online).not.toBe(first);
    expect((await presence(true)).status).toBe(204); // no change in online
    expect(await etagOf(deps, tokenA)).toBe(online);
    const app = await call(deps, "PATCH", `/v1/devices/${a.endpointId}`, { token: tokenA, json: { app: { kind: "ember", version: "1" } } });
    expect(app.status).toBe(200);
    expect(await etagOf(deps, tokenA)).not.toBe(online);
  });

  it("test_fr_h9_wait_is_validated", async () => {
    const { deps, tokenA } = await account(206);
    for (const wait of ["-1", "26", "x", "1.5"]) {
      expect((await call(deps, "GET", `/v1/devices?wait=${wait}`, { token: tokenA })).status, wait).toBe(400);
    }
  });
});
