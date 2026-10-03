// FR-H1: devices join a GitHub-backed account through device links with key possession.

import { describe, expect, it } from "vitest";
import { toHex, utf8 } from "../src/util";
import { call, irohRecord, linkDevice, makeDeps, newDevice, signIn } from "./helpers";

async function startLink(deps: ReturnType<typeof makeDeps>, endpointId: string, role = "computer") {
  const response = await call(deps, "POST", "/v1/device-links", { json: { endpoint_id: endpointId, name: "box", role } });
  expect(response.status).toBe(201);
  return (await response.json()) as {
    link_id: string;
    user_code: string;
    challenge: string;
    verification_uri_complete: string;
    interval: number;
  };
}

describe("device links", () => {
  it("test_fr_h1_register_two_iroh_endpoints", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 1, login: "owner" });
    const a = await newDevice();
    const b = await newDevice();
    await linkDevice(deps, { cookie }, a, "main_server", "mac mini");
    const tokenB = await linkDevice(deps, { cookie }, b, "computer", "laptop");
    const list = (await (await call(deps, "GET", "/v1/devices", { token: tokenB })).json()) as {
      devices: { endpoint_id: string; role: string; online: boolean }[];
    };
    expect(list.devices.map((d) => d.endpoint_id).sort()).toEqual([a.endpointId, b.endpointId].sort());
    expect(list.devices.every((d) => d.online === false)).toBe(true);
  });

  it("test_fr_h1_link_shows_code_and_polls_pending_until_approved", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 2, login: "owner" });
    const device = await newDevice();
    const link = await startLink(deps, device.endpointId);
    expect(link.user_code).toMatch(/^[BCDFGHJKLMNPQRSTVWXZ]{4}-[BCDFGHJKLMNPQRSTVWXZ]{4}$/);
    expect(link.verification_uri_complete).toBe(`https://darkpyonix.dev/link?code=${link.user_code}`);
    const signature = toHex(await device.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\n${link.challenge}`)));
    const pending = await call(deps, "POST", `/v1/device-links/${link.link_id}/token`, { json: { signature } });
    expect(pending.status).toBe(202);
    const shown = (await (await call(deps, "GET", `/v1/link-codes/${link.user_code.toLowerCase()}`, { cookie })).json()) as {
      endpoint_id: string;
    };
    expect(shown.endpoint_id).toBe(device.endpointId);
    expect((await call(deps, "POST", `/v1/link-codes/${link.user_code}`, { cookie, json: { approve: true } })).status).toBe(204);
    const claimed = await call(deps, "POST", `/v1/device-links/${link.link_id}/token`, { json: { signature } });
    expect(claimed.status).toBe(201);
    // Claimed once only.
    expect((await call(deps, "POST", `/v1/device-links/${link.link_id}/token`, { json: { signature } })).status).toBe(404);
  });

  it("test_fr_h1_registration_requires_key_possession", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 3, login: "owner" });
    const device = await newDevice();
    const thief = await newDevice();
    const link = await startLink(deps, device.endpointId);
    await call(deps, "POST", `/v1/link-codes/${link.user_code}`, { cookie, json: { approve: true } });
    const forged = toHex(await thief.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\n${link.challenge}`)));
    expect((await call(deps, "POST", `/v1/device-links/${link.link_id}/token`, { json: { signature: forged } })).status).toBe(400);
    const wrongMessage = toHex(await device.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\nother`)));
    expect((await call(deps, "POST", `/v1/device-links/${link.link_id}/token`, { json: { signature: wrongMessage } })).status).toBe(400);
    expect((await call(deps, "POST", `/v1/device-links/${link.link_id}/token`, { json: { signature: "zz" } })).status).toBe(400);
  });

  it("test_fr_h1_denied_link_is_refused", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 4, login: "owner" });
    const device = await newDevice();
    const link = await startLink(deps, device.endpointId);
    expect((await call(deps, "POST", `/v1/link-codes/${link.user_code}`, { cookie, json: { approve: false } })).status).toBe(204);
    const signature = toHex(await device.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\n${link.challenge}`)));
    expect((await call(deps, "POST", `/v1/device-links/${link.link_id}/token`, { json: { signature } })).status).toBe(403);
    // A decided code cannot be decided again.
    expect((await call(deps, "POST", `/v1/link-codes/${link.user_code}`, { cookie, json: { approve: true } })).status).toBe(404);
  });

  it("test_fr_h1_main_server_approves_computers_but_a_computer_cannot", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 5, login: "owner" });
    const main = await linkDevice(deps, { cookie }, await newDevice(), "main_server");
    const computer = await linkDevice(deps, { token: main }, await newDevice(), "computer");
    const link = await startLink(deps, (await newDevice()).endpointId);
    expect((await call(deps, "POST", `/v1/link-codes/${link.user_code}`, { token: computer, json: { approve: true } })).status).toBe(403);
  });

  it("test_fr_h1_only_a_session_approves_a_main_server_link", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 9, login: "owner" });
    const main = await linkDevice(deps, { cookie }, await newDevice(), "main_server");
    const link = await startLink(deps, (await newDevice()).endpointId, "main_server");
    const byToken = await call(deps, "POST", `/v1/link-codes/${link.user_code}`, { token: main, json: { approve: true } });
    expect(byToken.status).toBe(403);
    // Still pending: the session can approve it.
    expect((await call(deps, "POST", `/v1/link-codes/${link.user_code}`, { cookie, json: { approve: true } })).status).toBe(204);
    // A main server token may still deny a main_server link.
    const other = await startLink(deps, (await newDevice()).endpointId, "main_server");
    expect((await call(deps, "POST", `/v1/link-codes/${other.user_code}`, { token: main, json: { approve: false } })).status).toBe(204);
  });

  it("test_fr_h1_devices_are_scoped_to_their_account", async () => {
    const deps = makeDeps();
    const alice = await signIn(deps, { id: 6, login: "alice" });
    const bob = await signIn(deps, { id: 7, login: "bob" });
    const device = await newDevice();
    await linkDevice(deps, { cookie: alice }, device);
    expect((await call(deps, "GET", `/v1/devices/${device.endpointId}`, { cookie: alice })).status).toBe(200);
    expect((await call(deps, "GET", `/v1/devices/${device.endpointId}`, { cookie: bob })).status).toBe(404);
    expect((await call(deps, "DELETE", `/v1/devices/${device.endpointId}`, { cookie: bob })).status).toBe(404);
    const bobs = (await (await call(deps, "GET", "/v1/devices", { cookie: bob })).json()) as { devices: unknown[] };
    expect(bobs.devices).toHaveLength(0);
  });

  it("test_fr_h1_removed_device_is_revoked", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 8, login: "owner" });
    const device = await newDevice();
    const token = await linkDevice(deps, { cookie }, device);
    expect((await call(deps, "DELETE", `/v1/devices/${device.endpointId}`, { cookie })).status).toBe(204);
    expect((await call(deps, "GET", "/v1/devices", { token })).status).toBe(401);
    // Removed keys are not reused.
    expect(
      (await call(deps, "POST", "/v1/device-links", { json: { endpoint_id: device.endpointId, name: "again", role: "computer" } })).status,
    ).toBe(409);
    await Promise.all(deps.pending);
    expect(deps.github.relayCalls).toEqual([
      { url: "https://relay.darkpyonix.dev/admin/v1/disconnect", body: { endpoint_id: device.endpointId } },
    ]);
  });

  it("test_fr_h1_removed_device_token_is_told_apart_from_a_bad_token", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 10, login: "owner" });
    const device = await newDevice();
    const token = await linkDevice(deps, { cookie }, device);
    const bad = await call(deps, "GET", "/v1/devices", { token: "dpd_not-a-token" });
    expect(bad.status).toBe(401);
    expect(((await bad.json()) as { code: string }).code).toBe("invalid_credentials");
    const none = await call(deps, "GET", "/v1/me");
    expect(((await none.json()) as { code: string }).code).toBe("invalid_credentials");
    expect((await call(deps, "DELETE", `/v1/devices/${device.endpointId}`, { cookie })).status).toBe(204);
    for (const path of ["/v1/devices", "/v1/me", `/pkarr/${device.z32}`]) {
      const removed = await call(deps, "GET", path, { token });
      expect(removed.status, path).toBe(401);
      expect(((await removed.json()) as { code: string }).code, path).toBe("device_removed");
    }
  });

  it("test_fr_h1_a_device_removes_itself_but_not_others", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 11, login: "owner" });
    const a = await newDevice();
    const b = await newDevice();
    const tokenA = await linkDevice(deps, { cookie }, a);
    const tokenB = await linkDevice(deps, { cookie }, b);
    expect((await call(deps, "DELETE", `/v1/devices/${b.endpointId}`, { token: tokenA })).status).toBe(403);
    expect((await call(deps, "DELETE", `/v1/devices/${a.endpointId}`, { token: tokenA })).status).toBe(204);
    const left = await call(deps, "GET", "/v1/devices", { token: tokenA });
    expect(((await left.json()) as { code: string }).code).toBe("device_removed");
    const list = (await (await call(deps, "GET", "/v1/devices", { token: tokenB })).json()) as { devices: { endpoint_id: string }[] };
    expect(list.devices.map((d) => d.endpoint_id)).toEqual([b.endpointId]);
  });

  it("test_fr_h1_client_role_joins_and_connects_but_cannot_share_or_name", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 12, login: "owner" });
    const main = await linkDevice(deps, { cookie }, await newDevice(), "main_server");
    const phone = await newDevice();
    // A main server approves a client like a computer.
    const token = await linkDevice(deps, { token: main }, phone, "client", "phone");
    const list = (await (await call(deps, "GET", "/v1/devices", { token })).json()) as { devices: { endpoint_id: string; role: string }[] };
    expect(list.devices.find((d) => d.endpoint_id === phone.endpointId)?.role).toBe("client");
    expect((await call(deps, "PUT", `/pkarr/${phone.z32}`, { body: await irohRecord(phone, "https://relay.darkpyonix.dev/", [], 1n) })).status).toBe(204);
    expect((await call(deps, "POST", "/v1/shares", { token, json: { share_id: "s_00000000000000c1" } })).status).toBe(403);
    expect((await call(deps, "PUT", "/v1/names/my-phone", { token })).status).toBe(403);
    const link = await startLink(deps, (await newDevice()).endpointId);
    expect((await call(deps, "POST", `/v1/link-codes/${link.user_code}`, { token, json: { approve: true } })).status).toBe(403);
  });

  it("test_fr_h1_link_request_is_validated", async () => {
    const deps = makeDeps();
    const bad = [
      { endpoint_id: "ABC", name: "x", role: "computer" },
      { endpoint_id: "a".repeat(64), name: "", role: "computer" },
      { endpoint_id: "a".repeat(64), name: "x", role: "admin" },
    ];
    for (const json of bad) expect((await call(deps, "POST", "/v1/device-links", { json })).status).toBe(400);
  });
});
