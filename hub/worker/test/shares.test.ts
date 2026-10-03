// FR-H4 share links and FR-H3 relay admission (the hub side; the relay host calls these).

import { describe, expect, it } from "vitest";
import { call, irohRecord, linkDevice, makeDeps, newDevice, signIn } from "./helpers";

const RELAY_SECRET = "relay-secret";

async function admit(deps: ReturnType<typeof makeDeps>, endpointId: string, token: string | null, secret = RELAY_SECRET) {
  const response = await call(deps, "POST", "/internal/v1/relay/admit", { token: secret, json: { endpoint_id: endpointId, token } });
  return { status: response.status, body: response.status === 200 ? ((await response.json()) as { allow: boolean; kind?: string }) : null };
}

describe("shares", () => {
  it("test_fr_h4_share_resolves_to_hosting_device", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 31, login: "owner" });
    const host = await newDevice();
    const token = await linkDevice(deps, { cookie }, host, "main_server");
    const published = await call(deps, "POST", "/v1/shares", { token, json: { share_id: "s_0123456789abcdef" } });
    expect(published.status).toBe(201);
    expect(await published.json()).toEqual({ share_id: "s_0123456789abcdef", url: "https://darkpyonix.dev/s/s_0123456789abcdef" });

    // Before the host publishes addresses, guests dial our relay.
    const first = (await (await call(deps, "GET", "/v1/shares/s_0123456789abcdef")).json()) as Record<string, unknown>;
    expect(first.endpoint_id).toBe(host.endpointId);
    expect(first.relay_url).toBe("https://relay.darkpyonix.dev/");
    expect(first.relay_token).toMatch(/^dpg_/);

    // After, its published home relay.
    await call(deps, "PUT", `/pkarr/${host.z32}`, { body: await irohRecord(host, "https://eu.relay.example/", [], 10n) });
    const second = (await (await call(deps, "GET", "/v1/shares/s_0123456789abcdef")).json()) as Record<string, unknown>;
    expect(second.relay_url).toBe("https://eu.relay.example/");
  });

  it("test_fr_h4_share_ids_belong_to_one_device", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 32, login: "owner" });
    const a = await linkDevice(deps, { cookie }, await newDevice());
    const b = await linkDevice(deps, { cookie }, await newDevice());
    expect((await call(deps, "POST", "/v1/shares", { token: a, json: { share_id: "s_00000000000000aa" } })).status).toBe(201);
    expect((await call(deps, "POST", "/v1/shares", { token: a, json: { share_id: "s_00000000000000aa" } })).status).toBe(201);
    expect((await call(deps, "POST", "/v1/shares", { token: b, json: { share_id: "s_00000000000000aa" } })).status).toBe(409);
    expect((await call(deps, "POST", "/v1/shares", { token: a, json: { share_id: "nope" } })).status).toBe(400);
  });

  it("test_fr_h4_guest_pass_admits_an_unregistered_endpoint_at_the_relay", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 33, login: "owner" });
    const token = await linkDevice(deps, { cookie }, await newDevice());
    await call(deps, "POST", "/v1/shares", { token, json: { share_id: "s_00000000000000bb" } });
    const resolved = (await (await call(deps, "GET", "/v1/shares/s_00000000000000bb")).json()) as { relay_token: string };
    const guest = await newDevice();
    expect((await admit(deps, guest.endpointId, resolved.relay_token)).body).toMatchObject({ allow: true, kind: "guest" });
    expect((await admit(deps, guest.endpointId, null)).body).toMatchObject({ allow: false });
    expect((await admit(deps, guest.endpointId, "dpg_forged")).body).toMatchObject({ allow: false });
    // Unpublishing the share kills its passes.
    expect((await call(deps, "DELETE", "/v1/shares/s_00000000000000bb", { token })).status).toBe(204);
    expect((await admit(deps, guest.endpointId, resolved.relay_token)).body).toMatchObject({ allow: false });
    expect((await call(deps, "GET", "/v1/shares/s_00000000000000bb")).status).toBe(404);
  });

  it("test_fr_h4_viewer_pages_are_served", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 34, login: "owner" });
    const token = await linkDevice(deps, { cookie }, await newDevice());
    await call(deps, "POST", "/v1/shares", { token, json: { share_id: "s_00000000000000cc" } });
    const page = await call(deps, "GET", "/s/s_00000000000000cc");
    expect(page.status).toBe(200);
    expect(page.headers.get("content-type")).toContain("text/html");
    expect(await page.text()).toContain("s_00000000000000cc");
    expect((await call(deps, "GET", "/s/s_00000000000000dd")).status).toBe(404);
    const ash = await call(deps, "GET", "/ash/");
    expect(ash.status).toBe(200);
    expect(ash.headers.get("content-type")).toContain("text/html");
  });
});

describe("relay admission", () => {
  it("test_fr_h3_relay_admits_registered_and_refuses_removed_devices", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 35, login: "owner" });
    const device = await newDevice();
    await linkDevice(deps, { cookie }, device);
    expect((await admit(deps, device.endpointId, null)).body).toMatchObject({ allow: true, kind: "device" });
    await call(deps, "DELETE", `/v1/devices/${device.endpointId}`, { cookie });
    expect((await admit(deps, device.endpointId, null)).body).toMatchObject({ allow: false });
  });

  it("test_fr_h3_relay_callbacks_need_the_shared_secret", async () => {
    const deps = makeDeps();
    const device = await newDevice();
    expect((await admit(deps, device.endpointId, null, "wrong")).status).toBe(401);
    expect((await call(deps, "POST", "/internal/v1/relay/presence", { json: { endpoint_id: device.endpointId, online: true } })).status).toBe(401);
  });

  it("test_fr_h3_presence_marks_devices_online", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 36, login: "owner" });
    const device = await newDevice();
    await linkDevice(deps, { cookie }, device);
    const report = (online: boolean) =>
      call(deps, "POST", "/internal/v1/relay/presence", { token: RELAY_SECRET, json: { endpoint_id: device.endpointId, online } });
    expect((await report(true)).status).toBe(204);
    const on = (await (await call(deps, "GET", `/v1/devices/${device.endpointId}`, { cookie })).json()) as { online: boolean; last_seen: number };
    expect(on.online).toBe(true);
    expect(on.last_seen).toBeGreaterThan(0);
    await report(false);
    const off = (await (await call(deps, "GET", `/v1/devices/${device.endpointId}`, { cookie })).json()) as { online: boolean };
    expect(off.online).toBe(false);
  });
});
