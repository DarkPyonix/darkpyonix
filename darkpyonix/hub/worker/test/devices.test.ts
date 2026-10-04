// FR-H1: devices join a GitHub-backed account through device links with key possession.

import { describe, expect, it } from "vitest";
import { toHex, utf8 } from "../src/util";
import { call, irohRecord, linkDevice, makeDeps, newDevice, signIn } from "./helpers";

async function startLink(deps: ReturnType<typeof makeDeps>, endpointId: string, role = "computer") {
  const response = await call(deps, "POST", "/device-links", { json: { endpoint_id: endpointId, name: "box", role } });
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
    const list = (await (await call(deps, "GET", "/devices", { token: tokenB })).json()) as {
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
    const pending = await call(deps, "POST", `/device-links/${link.link_id}/token`, { json: { signature } });
    expect(pending.status).toBe(202);
    const shown = (await (await call(deps, "GET", `/link-codes/${link.user_code.toLowerCase()}`, { cookie })).json()) as {
      endpoint_id: string;
    };
    expect(shown.endpoint_id).toBe(device.endpointId);
    expect((await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: true } })).status).toBe(204);
    const claimed = await call(deps, "POST", `/device-links/${link.link_id}/token`, { json: { signature } });
    expect(claimed.status).toBe(201);
    // Claimed once only.
    expect((await call(deps, "POST", `/device-links/${link.link_id}/token`, { json: { signature } })).status).toBe(404);
  });

  it("test_fr_h1_restarted_device_reads_its_link_status", async () => {
    let now = Date.now();
    const deps = makeDeps({ nowMs: () => now });
    const cookie = await signIn(deps, { id: 17, login: "owner" });
    const device = await newDevice();
    const link = await startLink(deps, device.endpointId);
    const status = async (id = link.link_id) => {
      const r = await call(deps, "GET", `/device-links/${id}`);
      return { code: r.status, body: r.status === 200 ? ((await r.json()) as Record<string, unknown>) : null };
    };
    const pending = await status();
    expect(pending.code).toBe(200);
    expect(pending.body).toMatchObject({
      link_id: link.link_id,
      status: "pending",
      endpoint_id: device.endpointId,
      user_code: link.user_code,
      challenge: link.challenge,
      verification_uri_complete: link.verification_uri_complete,
    });
    expect(pending.body).not.toHaveProperty("device_token");
    await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: true } });
    expect((await status()).body?.status).toBe("approved");
    // With the challenge from the status, the restarted device claims.
    const signature = toHex(await device.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\n${pending.body!.challenge}`)));
    expect((await call(deps, "POST", `/device-links/${link.link_id}/token`, { json: { signature } })).status).toBe(201);
    expect((await status()).body?.status).toBe("claimed");

    const denied = await startLink(deps, (await newDevice()).endpointId);
    await call(deps, "POST", `/link-codes/${denied.user_code}`, { cookie, json: { approve: false } });
    expect((await status(denied.link_id)).body?.status).toBe("denied");

    const late = await startLink(deps, (await newDevice()).endpointId);
    now += 901_000;
    expect((await status(late.link_id)).body?.status).toBe("expired");
    expect((await status(`l_${"0".repeat(32)}`)).code).toBe(404);
    expect((await status("nope")).code).toBe(404);
  });

  it("test_fr_h1_registration_requires_key_possession", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 3, login: "owner" });
    const device = await newDevice();
    const thief = await newDevice();
    const link = await startLink(deps, device.endpointId);
    await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: true } });
    const forged = toHex(await thief.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\n${link.challenge}`)));
    expect((await call(deps, "POST", `/device-links/${link.link_id}/token`, { json: { signature: forged } })).status).toBe(400);
    const wrongMessage = toHex(await device.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\nother`)));
    expect((await call(deps, "POST", `/device-links/${link.link_id}/token`, { json: { signature: wrongMessage } })).status).toBe(400);
    expect((await call(deps, "POST", `/device-links/${link.link_id}/token`, { json: { signature: "zz" } })).status).toBe(400);
  });

  it("test_fr_h1_denied_link_is_refused", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 4, login: "owner" });
    const device = await newDevice();
    const link = await startLink(deps, device.endpointId);
    expect((await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: false } })).status).toBe(204);
    const signature = toHex(await device.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\n${link.challenge}`)));
    expect((await call(deps, "POST", `/device-links/${link.link_id}/token`, { json: { signature } })).status).toBe(403);
    // A decided code cannot be decided again.
    expect((await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: true } })).status).toBe(404);
  });

  it("test_fr_h1_main_server_approves_computers_but_a_computer_cannot", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 5, login: "owner" });
    const main = await linkDevice(deps, { cookie }, await newDevice(), "main_server");
    const computer = await linkDevice(deps, { token: main }, await newDevice(), "computer");
    const link = await startLink(deps, (await newDevice()).endpointId);
    expect((await call(deps, "POST", `/link-codes/${link.user_code}`, { token: computer, json: { approve: true } })).status).toBe(403);
  });

  it("test_fr_h1_only_a_session_approves_a_main_server_link", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 9, login: "owner" });
    const mainDevice = await newDevice();
    const main = await linkDevice(deps, { cookie }, mainDevice, "main_server");
    const link = await startLink(deps, (await newDevice()).endpointId, "main_server");
    const replace = mainDevice.endpointId;
    const byToken = await call(deps, "POST", `/link-codes/${link.user_code}`, { token: main, json: { approve: true, replace } });
    expect(byToken.status).toBe(403);
    // Still pending: the session can approve it.
    expect((await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: true, replace } })).status).toBe(204);
    // A main server token may still deny a main_server link.
    const other = await startLink(deps, (await newDevice()).endpointId, "main_server");
    expect((await call(deps, "POST", `/link-codes/${other.user_code}`, { token: main, json: { approve: false } })).status).toBe(204);
  });

  it("test_fr_h1_devices_are_scoped_to_their_account", async () => {
    const deps = makeDeps();
    const alice = await signIn(deps, { id: 6, login: "alice" });
    const bob = await signIn(deps, { id: 7, login: "bob" });
    const device = await newDevice();
    await linkDevice(deps, { cookie: alice }, device);
    expect((await call(deps, "GET", `/devices/${device.endpointId}`, { cookie: alice })).status).toBe(200);
    expect((await call(deps, "GET", `/devices/${device.endpointId}`, { cookie: bob })).status).toBe(404);
    expect((await call(deps, "DELETE", `/devices/${device.endpointId}`, { cookie: bob })).status).toBe(404);
    const bobs = (await (await call(deps, "GET", "/devices", { cookie: bob })).json()) as { devices: unknown[] };
    expect(bobs.devices).toHaveLength(0);
  });

  it("test_fr_h1_removed_device_is_revoked", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 8, login: "owner" });
    const device = await newDevice();
    const token = await linkDevice(deps, { cookie }, device);
    expect((await call(deps, "DELETE", `/devices/${device.endpointId}`, { cookie })).status).toBe(204);
    expect((await call(deps, "GET", "/devices", { token })).status).toBe(401);
    // Removed keys are not reused.
    expect(
      (await call(deps, "POST", "/device-links", { json: { endpoint_id: device.endpointId, name: "again", role: "computer" } })).status,
    ).toBe(409);
    await Promise.all(deps.pending);
    expect(deps.github.relayCalls).toEqual([
      { url: "https://relay.darkpyonix.dev/admin/disconnect", body: { endpoint_id: device.endpointId } },
    ]);
  });

  it("test_fr_h1_removed_device_token_is_told_apart_from_a_bad_token", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 10, login: "owner" });
    const device = await newDevice();
    const token = await linkDevice(deps, { cookie }, device);
    const bad = await call(deps, "GET", "/devices", { token: "dpd_not-a-token" });
    expect(bad.status).toBe(401);
    expect(((await bad.json()) as { code: string }).code).toBe("invalid_credentials");
    const none = await call(deps, "GET", "/me");
    expect(((await none.json()) as { code: string }).code).toBe("invalid_credentials");
    expect((await call(deps, "DELETE", `/devices/${device.endpointId}`, { cookie })).status).toBe(204);
    for (const path of ["/devices", "/me", `/pkarr/${device.z32}`]) {
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
    expect((await call(deps, "DELETE", `/devices/${b.endpointId}`, { token: tokenA })).status).toBe(403);
    expect((await call(deps, "DELETE", `/devices/${a.endpointId}`, { token: tokenA })).status).toBe(204);
    const left = await call(deps, "GET", "/devices", { token: tokenA });
    expect(((await left.json()) as { code: string }).code).toBe("device_removed");
    const list = (await (await call(deps, "GET", "/devices", { token: tokenB })).json()) as { devices: { endpoint_id: string }[] };
    expect(list.devices.map((d) => d.endpoint_id)).toEqual([b.endpointId]);
  });

  it("test_fr_h1_main_server_token_removes_computers_and_itself", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 25, login: "owner" });
    const a = await newDevice();
    const c = await newDevice();
    const mainA = await linkDevice(deps, { cookie }, a, "main_server");
    await linkDevice(deps, { cookie }, c, "computer");
    expect((await call(deps, "DELETE", `/devices/${c.endpointId}`, { token: mainA })).status).toBe(204);
    expect((await call(deps, "DELETE", `/devices/${a.endpointId}`, { token: mainA })).status).toBe(204);
    const list = (await (await call(deps, "GET", "/devices", { cookie })).json()) as { devices: unknown[] };
    expect(list.devices).toHaveLength(0);
  });

  it("test_fr_h1_client_role_joins_and_connects_but_cannot_share_or_name", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 12, login: "owner" });
    const main = await linkDevice(deps, { cookie }, await newDevice(), "main_server");
    const phone = await newDevice();
    // A main server approves a client like a computer.
    const token = await linkDevice(deps, { token: main }, phone, "client", "phone");
    const list = (await (await call(deps, "GET", "/devices", { token })).json()) as { devices: { endpoint_id: string; role: string }[] };
    expect(list.devices.find((d) => d.endpoint_id === phone.endpointId)?.role).toBe("client");
    expect((await call(deps, "PUT", `/pkarr/${phone.z32}`, { body: await irohRecord(phone, "https://relay.darkpyonix.dev/", [], 1n) })).status).toBe(204);
    expect((await call(deps, "POST", "/shares", { token, json: { share_id: "s_00000000000000c1" } })).status).toBe(403);
    expect((await call(deps, "PUT", "/names/my-phone", { token })).status).toBe(403);
    const link = await startLink(deps, (await newDevice()).endpointId);
    expect((await call(deps, "POST", `/link-codes/${link.user_code}`, { token, json: { approve: true } })).status).toBe(403);
  });

  it("test_fr_h1_rename_by_the_device_or_the_account", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 13, login: "owner" });
    const a = await newDevice();
    const tokenA = await linkDevice(deps, { cookie }, a, "computer", "old");
    const tokenB = await linkDevice(deps, { cookie }, await newDevice());
    const self = await call(deps, "PATCH", `/devices/${a.endpointId}`, { token: tokenA, json: { name: "studio" } });
    expect(self.status).toBe(200);
    expect(((await self.json()) as { name: string }).name).toBe("studio");
    expect((await call(deps, "PATCH", `/devices/${a.endpointId}`, { cookie, json: { name: "studio mac" } })).status).toBe(200);
    expect((await call(deps, "PATCH", `/devices/${a.endpointId}`, { token: tokenB, json: { name: "mine" } })).status).toBe(403);
    expect((await call(deps, "PATCH", `/devices/${a.endpointId}`, { cookie, json: { name: "" } })).status).toBe(400);
    expect((await call(deps, "PATCH", `/devices/${a.endpointId}`, { cookie, json: { role: "main_server" } })).status).toBe(400);
    expect((await call(deps, "PATCH", `/devices/${a.endpointId}`, { cookie, json: {} })).status).toBe(400);
    const shown = (await (await call(deps, "GET", `/devices/${a.endpointId}`, { cookie })).json()) as { name: string; role: string };
    expect(shown).toMatchObject({ name: "studio mac", role: "computer" });
  });

  it("test_fr_h1_link_request_is_validated", async () => {
    const deps = makeDeps();
    const bad = [
      { endpoint_id: "ABC", name: "x", role: "computer" },
      { endpoint_id: "a".repeat(64), name: "", role: "computer" },
      { endpoint_id: "a".repeat(64), name: "x", role: "admin" },
    ];
    for (const json of bad) expect((await call(deps, "POST", "/device-links", { json })).status).toBe(400);
  });
});

describe("one main server per account (FR-H1, Ember INTENT D3)", () => {
  const DIGEST = "b".repeat(43);

  async function approve(deps: ReturnType<typeof makeDeps>, c: { cookie?: string; token?: string }, code: string, body: Record<string, unknown>) {
    const response = await call(deps, "POST", `/link-codes/${code}`, { ...c, json: body });
    const text = await response.text();
    return { status: response.status, code: text ? ((JSON.parse(text) as { code?: string }).code ?? null) : null };
  }

  it("test_fr_h1_a_second_main_server_needs_an_explicit_replace", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 60, login: "owner" });
    const first = await newDevice();
    await linkDevice(deps, { cookie }, first, "main_server", "mac mini");
    const link = await startLink(deps, (await newDevice()).endpointId, "main_server");

    const shown = (await (await call(deps, "GET", `/link-codes/${link.user_code}`, { cookie })).json()) as Record<string, unknown>;
    expect(shown.current_main_server).toEqual({ endpoint_id: first.endpointId, name: "mac mini" });

    expect(await approve(deps, { cookie }, link.user_code, { approve: true })).toEqual({ status: 409, code: "main_server_exists" });
    const stranger = await newDevice();
    expect(await approve(deps, { cookie }, link.user_code, { approve: true, replace: stranger.endpointId })).toEqual({
      status: 409,
      code: "replace_mismatch",
    });
    expect((await approve(deps, { cookie }, link.user_code, { approve: true, replace: "nope" })).status).toBe(400);
    expect((await approve(deps, { cookie }, link.user_code, { approve: false, replace: first.endpointId })).status).toBe(400);
    // Other roles do not replace anything.
    const computer = await startLink(deps, (await newDevice()).endpointId, "computer");
    const computerShown = (await (await call(deps, "GET", `/link-codes/${computer.user_code}`, { cookie })).json()) as Record<string, unknown>;
    expect(computerShown.current_main_server).toBeNull();
    expect((await approve(deps, { cookie }, computer.user_code, { approve: true, replace: first.endpointId })).status).toBe(400);
    expect((await approve(deps, { cookie }, computer.user_code, { approve: true })).status).toBe(204);
    // Still pending after the refusals: the explicit replacement goes through.
    expect(await approve(deps, { cookie }, link.user_code, { approve: true, replace: first.endpointId })).toEqual({ status: 204, code: null });
  });

  it("test_fr_h1_replacement_removes_the_old_main_server_and_moves_its_names", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 61, login: "owner" });
    const oldDevice = await newDevice();
    const oldToken = await linkDevice(deps, { cookie }, oldDevice, "main_server", "raspberry pi");
    expect((await call(deps, "PUT", "/names/myhome", { token: oldToken })).status).toBe(201);
    await call(deps, "PUT", "/names/myhome/acme-challenge", { token: oldToken, json: { values: [DIGEST] } });
    expect((await call(deps, "POST", "/shares", { token: oldToken, json: { share_id: "s_00000000000000d3" } })).status).toBe(201);

    const newMain = await newDevice();
    const token = await linkDevice(deps, { cookie }, newMain, "main_server", "mac mini", { replace: oldDevice.endpointId });

    const old = await call(deps, "GET", "/me", { token: oldToken });
    expect(old.status).toBe(401);
    expect(((await old.json()) as { code: string }).code).toBe("device_removed");
    const list = (await (await call(deps, "GET", "/devices", { cookie })).json()) as { devices: { endpoint_id: string; role: string }[] };
    expect(list.devices.map((d) => [d.endpoint_id, d.role])).toEqual([[newMain.endpointId, "main_server"]]);
    const removedList = (await (await call(deps, "GET", "/removed-devices", { cookie })).json()) as {
      devices: { endpoint_id: string; role: string }[];
    };
    expect(removedList.devices).toEqual([expect.objectContaining({ endpoint_id: oldDevice.endpointId, role: "main_server" })]);
    // The name follows the main server; the old challenge and share are gone.
    const names = (await (await call(deps, "GET", "/names", { cookie })).json()) as { names: { name: string; endpoint_id: string }[] };
    expect(names.names).toEqual([expect.objectContaining({ name: "myhome", endpoint_id: newMain.endpointId })]);
    await Promise.all(deps.pending);
    expect(deps.memoryDns.records.size).toBe(0);
    expect((await call(deps, "GET", "/shares/s_00000000000000d3")).status).toBe(404);
    expect(deps.github.relayCalls).toContainEqual({
      url: "https://relay.darkpyonix.dev/admin/disconnect",
      body: { endpoint_id: oldDevice.endpointId },
    });
    expect((await call(deps, "PUT", "/names/myhome/acme-challenge", { token, json: { values: [DIGEST] } })).status).toBe(204);
  });

  it("test_fr_h1_only_the_first_of_two_approved_main_server_links_claims", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 62, login: "owner" });
    const a = await newDevice();
    const b = await newDevice();
    const linkA = await startLink(deps, a.endpointId, "main_server");
    const linkB = await startLink(deps, b.endpointId, "main_server");
    expect((await approve(deps, { cookie }, linkA.user_code, { approve: true })).status).toBe(204);
    expect((await approve(deps, { cookie }, linkB.user_code, { approve: true })).status).toBe(204);
    const claim = async (d: typeof a, l: typeof linkA) =>
      call(deps, "POST", `/device-links/${l.link_id}/token`, {
        json: { signature: toHex(await d.sign(utf8(`darkpyonix-hub/v2/link\n${l.link_id}\n${l.challenge}`))) },
      });
    expect((await claim(a, linkA)).status).toBe(201);
    const second = await claim(b, linkB);
    expect(second.status).toBe(409);
    expect(((await second.json()) as { code: string }).code).toBe("main_server_exists");
    const status = (await (await call(deps, "GET", `/device-links/${linkB.link_id}`)).json()) as { status: string };
    expect(status.status).toBe("denied");
    const list = (await (await call(deps, "GET", "/devices", { cookie })).json()) as { devices: { endpoint_id: string }[] };
    expect(list.devices.map((d) => d.endpoint_id)).toEqual([a.endpointId]);
  });
});

describe("device app (FR-H10)", () => {
  const ember = { kind: "ember", version: "0.4.0+mac", services: ["kernel-manager", "ash-host"] };

  it("test_fr_h10_device_reports_its_app", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 14, login: "owner" });
    const a = await newDevice();
    const token = await linkDevice(deps, { cookie }, a);
    const before = (await (await call(deps, "GET", `/devices/${a.endpointId}`, { cookie })).json()) as { app: unknown };
    expect(before.app).toBeNull();
    expect((await call(deps, "PATCH", `/devices/${a.endpointId}`, { token, json: { app: ember } })).status).toBe(200);
    const list = (await (await call(deps, "GET", "/devices", { cookie })).json()) as { devices: { app: unknown }[] };
    expect(list.devices[0].app).toEqual(ember);
    // services may be left out.
    const bare = await call(deps, "PATCH", `/devices/${a.endpointId}`, { token, json: { app: { kind: "ember", version: "1" } } });
    expect(((await bare.json()) as { app: unknown }).app).toEqual({ kind: "ember", version: "1", services: [] });
    const cleared = await call(deps, "PATCH", `/devices/${a.endpointId}`, { token, json: { app: null } });
    expect(((await cleared.json()) as { app: unknown }).app).toBeNull();
  });

  it("test_fr_h10_only_the_device_writes_its_app", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 15, login: "owner" });
    const a = await newDevice();
    await linkDevice(deps, { cookie }, a);
    const main = await linkDevice(deps, { cookie }, await newDevice(), "main_server");
    expect((await call(deps, "PATCH", `/devices/${a.endpointId}`, { cookie, json: { app: ember } })).status).toBe(403);
    expect((await call(deps, "PATCH", `/devices/${a.endpointId}`, { token: main, json: { app: ember } })).status).toBe(403);
  });

  it("test_fr_h10_app_is_validated", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 16, login: "owner" });
    const a = await newDevice();
    const token = await linkDevice(deps, { cookie }, a);
    const bad: unknown[] = [
      "ember",
      { kind: "Ember", version: "1" },
      { kind: "ember" },
      { kind: "ember", version: "1 2" },
      { kind: "ember", version: "1", services: ["a", "a"] },
      { kind: "ember", version: "1", services: Array.from({ length: 17 }, (_, i) => `s${i}`) },
      { kind: "ember", version: "1", services: "kernel" },
      { kind: "ember", version: "1", extra: true },
      { kind: "e".repeat(33), version: "1" },
    ];
    for (const app of bad) {
      expect((await call(deps, "PATCH", `/devices/${a.endpointId}`, { token, json: { app } })).status, JSON.stringify(app)).toBe(400);
    }
  });
});

describe("re-admitting removed keys (FR-H11)", () => {
  async function removed(deps: ReturnType<typeof makeDeps>, id: number) {
    const cookie = await signIn(deps, { id, login: "owner" });
    const device = await newDevice();
    const oldToken = await linkDevice(deps, { cookie }, device, "main_server", "mac mini");
    expect((await call(deps, "DELETE", `/devices/${device.endpointId}`, { cookie })).status).toBe(204);
    const mainDevice = await newDevice();
    const main = await linkDevice(deps, { cookie }, mainDevice, "main_server");
    return { cookie, main, mainDevice, device, oldToken };
  }

  it("test_fr_h11_owner_readmits_a_removed_key", async () => {
    const deps = makeDeps();
    const { cookie, main, device, oldToken } = await removed(deps, 18);
    const ask = () => call(deps, "POST", "/device-links", { json: { endpoint_id: device.endpointId, name: "mac mini", role: "computer" } });
    expect((await ask()).status).toBe(409);
    expect((await call(deps, "POST", `/devices/${device.endpointId}/readmit`, { token: main })).status).toBe(403);
    const opened = await call(deps, "POST", `/devices/${device.endpointId}/readmit`, { cookie });
    expect(opened.status).toBe(200);
    expect(await opened.json()).toMatchObject({ endpoint_id: device.endpointId, expires_at: expect.any(Number) });
    const created = await ask();
    expect(created.status).toBe(201);
    const link = (await created.json()) as { link_id: string; user_code: string; challenge: string };
    // Even for a computer link, only a session approves a re-admitted key.
    expect((await call(deps, "POST", `/link-codes/${link.user_code}`, { token: main, json: { approve: true } })).status).toBe(403);
    const stranger = await signIn(deps, { id: 19, login: "stranger" });
    expect((await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie: stranger, json: { approve: true } })).status).toBe(403);
    expect((await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: true } })).status).toBe(204);
    const signature = toHex(await device.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\n${link.challenge}`)));
    const claimed = await call(deps, "POST", `/device-links/${link.link_id}/token`, { json: { signature } });
    expect(claimed.status).toBe(201);
    const { device_token: token } = (await claimed.json()) as { device_token: string };
    const back = await call(deps, "GET", `/devices/${device.endpointId}`, { token });
    expect(back.status).toBe(200);
    expect(await back.json()).toMatchObject({ role: "computer", app: null });
    const old = await call(deps, "GET", "/devices", { token: oldToken });
    expect(old.status).toBe(401);
    expect(((await old.json()) as { code: string }).code).toBe("invalid_credentials");
  });

  it("test_fr_h11_readmission_expires", async () => {
    let now = Date.now();
    const deps = makeDeps({ nowMs: () => now });
    const { cookie, device } = await removed(deps, 20);
    expect((await call(deps, "POST", `/devices/${device.endpointId}/readmit`, { cookie })).status).toBe(200);
    now += 901_000;
    const late = await call(deps, "POST", "/device-links", { json: { endpoint_id: device.endpointId, name: "x", role: "computer" } });
    expect(late.status).toBe(409);
  });

  type Removed = {
    endpoint_id: string;
    name: string;
    role: string;
    created_at: number;
    removed_at: number;
    readmit_until: number | null;
  };
  const removedList = async (deps: ReturnType<typeof makeDeps>, cookie: string) => {
    const response = await call(deps, "GET", "/removed-devices", { cookie });
    expect(response.status).toBe(200);
    return ((await response.json()) as { devices: Removed[] }).devices;
  };

  it("test_fr_h11_owner_lists_removed_devices", async () => {
    let now = Date.now();
    const deps = makeDeps({ nowMs: () => now });
    const { cookie, device } = await removed(deps, 26);
    now += 10_000;
    const phone = await newDevice();
    await linkDevice(deps, { cookie }, phone, "client", "phone");
    expect((await call(deps, "DELETE", `/devices/${phone.endpointId}`, { cookie })).status).toBe(204);
    const stranger = await signIn(deps, { id: 27, login: "stranger" });
    await linkDevice(deps, { cookie: stranger }, await newDevice());

    const listed = await removedList(deps, cookie);
    expect(listed.map((d) => d.endpoint_id)).toEqual([phone.endpointId, device.endpointId]);
    expect(listed[1]).toMatchObject({ name: "mac mini", role: "main_server", readmit_until: null });
    expect(listed[0]!.removed_at).toBeGreaterThan(listed[1]!.removed_at);
    expect(listed[1]!.removed_at).toBeGreaterThanOrEqual(listed[1]!.created_at);
    expect(await removedList(deps, stranger)).toEqual([]);

    const opened = (await (await call(deps, "POST", `/devices/${device.endpointId}/readmit`, { cookie })).json()) as {
      expires_at: number;
    };
    expect((await removedList(deps, cookie))[1]!.readmit_until).toBe(opened.expires_at);
    // Re-admitted and claimed: back in the device list, gone from this one.
    await linkDevice(deps, { cookie }, device, "computer", "mac mini");
    expect((await removedList(deps, cookie)).map((d) => d.endpoint_id)).toEqual([phone.endpointId]);
    // An expired re-admission shows as null.
    expect((await call(deps, "POST", `/devices/${phone.endpointId}/readmit`, { cookie })).status).toBe(200);
    now += 901_000;
    expect((await removedList(deps, cookie))[0]!.readmit_until).toBeNull();
  });

  it("test_fr_h11_only_a_session_lists_removed_devices", async () => {
    const deps = makeDeps();
    const { main } = await removed(deps, 28);
    const byToken = await call(deps, "GET", "/removed-devices", { token: main });
    expect(byToken.status).toBe(403);
    const none = await call(deps, "GET", "/removed-devices");
    expect(none.status).toBe(401);
  });

  it("test_fr_h11_readmit_needs_a_removed_device_of_the_account", async () => {
    const deps = makeDeps();
    const { cookie, device } = await removed(deps, 23);
    const active = await newDevice();
    await linkDevice(deps, { cookie }, active);
    expect((await call(deps, "POST", `/devices/${active.endpointId}/readmit`, { cookie })).status).toBe(409);
    const stranger = await signIn(deps, { id: 24, login: "stranger" });
    expect((await call(deps, "POST", `/devices/${device.endpointId}/readmit`, { cookie: stranger })).status).toBe(404);
    expect((await call(deps, "POST", `/devices/${"ab".repeat(32)}/readmit`, { cookie })).status).toBe(404);
  });

  it("test_fr_h11_a_readmitted_main_server_must_replace_the_current_one", async () => {
    const deps = makeDeps();
    const { cookie, main, mainDevice, device } = await removed(deps, 66);
    expect((await call(deps, "PUT", "/names/homebase", { token: main })).status).toBe(201);
    expect((await call(deps, "POST", `/devices/${device.endpointId}/readmit`, { cookie })).status).toBe(200);
    const link = await startLink(deps, device.endpointId, "main_server");
    const plain = await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: true } });
    expect(plain.status).toBe(409);
    expect(((await plain.json()) as { code: string }).code).toBe("main_server_exists");
    const replace = mainDevice.endpointId;
    expect((await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: true, replace } })).status).toBe(204);
    const signature = toHex(await device.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\n${link.challenge}`)));
    const claimed = await call(deps, "POST", `/device-links/${link.link_id}/token`, { json: { signature } });
    expect(claimed.status).toBe(201);
    const { device_token: token } = (await claimed.json()) as { device_token: string };
    const old = await call(deps, "GET", "/devices", { token: main });
    expect(old.status).toBe(401);
    expect(((await old.json()) as { code: string }).code).toBe("device_removed");
    const list = (await (await call(deps, "GET", "/devices", { token })).json()) as { devices: { endpoint_id: string; role: string }[] };
    expect(list.devices.filter((d) => d.role === "main_server").map((d) => d.endpoint_id)).toEqual([device.endpointId]);
    const names = (await (await call(deps, "GET", "/names", { cookie })).json()) as { names: { name: string; endpoint_id: string }[] };
    expect(names.names).toEqual([expect.objectContaining({ name: "homebase", endpoint_id: device.endpointId })]);
  });
});
