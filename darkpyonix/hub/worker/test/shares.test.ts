// FR-H4 share links; FR-H3: the hub runs no relay, so a share carries no relay pass.

import { describe, expect, it } from "vitest";
import { call, linkDevice, makeDeps, newDevice, signIn } from "./helpers";

describe("shares", () => {
  it("test_fr_h4_share_resolves_to_hosting_device", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 31, login: "owner" });
    const host = await newDevice();
    const token = await linkDevice(deps, { cookie }, host, "main_server");
    const published = await call(deps, "POST", "/shares", { token, json: { share_id: "s_0123456789abcdef" } });
    expect(published.status).toBe(201);
    expect(await published.json()).toEqual({ share_id: "s_0123456789abcdef", url: "https://darkpyonix.dev/s/s_0123456789abcdef" });

    const resolved = await call(deps, "GET", "/shares/s_0123456789abcdef");
    expect(await resolved.json()).toEqual({ share_id: "s_0123456789abcdef", endpoint_id: host.endpointId });
  });

  it("test_fr_h4_share_ids_belong_to_one_device", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 32, login: "owner" });
    const a = await linkDevice(deps, { cookie }, await newDevice());
    const b = await linkDevice(deps, { cookie }, await newDevice());
    expect((await call(deps, "POST", "/shares", { token: a, json: { share_id: "s_00000000000000aa" } })).status).toBe(201);
    expect((await call(deps, "POST", "/shares", { token: a, json: { share_id: "s_00000000000000aa" } })).status).toBe(201);
    expect((await call(deps, "POST", "/shares", { token: b, json: { share_id: "s_00000000000000aa" } })).status).toBe(409);
    expect((await call(deps, "POST", "/shares", { token: a, json: { share_id: "nope" } })).status).toBe(400);
  });

  it("test_fr_h4_unpublished_share_no_longer_resolves", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 33, login: "owner" });
    const token = await linkDevice(deps, { cookie }, await newDevice());
    await call(deps, "POST", "/shares", { token, json: { share_id: "s_00000000000000bb" } });
    expect((await call(deps, "GET", "/shares/s_00000000000000bb")).status).toBe(200);
    expect((await call(deps, "DELETE", "/shares/s_00000000000000bb", { token })).status).toBe(204);
    expect((await call(deps, "GET", "/shares/s_00000000000000bb")).status).toBe(404);
  });

  it("test_fr_h4_viewer_pages_are_served", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 34, login: "owner" });
    const token = await linkDevice(deps, { cookie }, await newDevice());
    await call(deps, "POST", "/shares", { token, json: { share_id: "s_00000000000000cc" } });
    const page = await call(deps, "GET", "/s/s_00000000000000cc");
    expect(page.status).toBe(200);
    expect(page.headers.get("content-type")).toContain("text/html");
    expect(await page.text()).toContain("s_00000000000000cc");
    expect((await call(deps, "GET", "/s/s_00000000000000dd")).status).toBe(404);
    const ash = await call(deps, "GET", "/ash/");
    expect(ash.status).toBe(200);
    expect(ash.headers.get("content-type")).toContain("text/html");
    // Read through the ASSETS binding from public/ash/index.html, not the Worker's fallback text.
    expect(await ash.text()).toContain('<meta name="darkpyonix-ash" content="placeholder">');
  });
});

describe("no hub relay", () => {
  it("test_fr_h3_worker_serves_no_relay_routes", async () => {
    const deps = makeDeps();
    const device = await newDevice();
    for (const path of ["/internal/relay/admit", "/internal/relay/presence", "/admin/disconnect", "/relay"]) {
      const response = await call(deps, "POST", path, { token: "relay-secret", json: { endpoint_id: device.endpointId, online: true } });
      expect(response.status, path).toBe(404);
    }
  });

  it("test_fr_h3_device_removal_calls_no_outside_host", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 35, login: "owner" });
    const device = await newDevice();
    await linkDevice(deps, { cookie }, device);
    const before = deps.github.calls.length;
    expect((await call(deps, "DELETE", `/devices/${device.endpointId}`, { cookie })).status).toBe(204);
    await Promise.all(deps.pending);
    expect(deps.github.calls.slice(before)).toEqual([]);
  });
});
