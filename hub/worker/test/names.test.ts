// FR-H5: names and ACME DNS-01 TXT publishing through the Cloudflare DNS API.

import { describe, expect, it } from "vitest";
import { CloudflareDns, DnsError } from "../src/dns";
import { call, linkDevice, makeDeps, newDevice, signIn } from "./helpers";

const DIGEST = "a".repeat(43);

describe("names", () => {
  it("test_fr_h5_name_reservation_and_acme_txt", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 41, login: "owner" });
    const main = await linkDevice(deps, { cookie }, await newDevice(), "main_server");
    const reserved = await call(deps, "PUT", "/v1/names/studio", { token: main });
    expect(reserved.status).toBe(201);
    expect(await reserved.json()).toMatchObject({ name: "studio", fqdn: "studio.darkpyonix.dev" });
    expect((await call(deps, "PUT", "/v1/names/studio", { token: main })).status).toBe(200);

    expect((await call(deps, "PUT", "/v1/names/studio/acme-challenge", { token: main, json: { values: [DIGEST] } })).status).toBe(204);
    expect(deps.memoryDns.records.get("_acme-challenge.studio.darkpyonix.dev")).toEqual([DIGEST]);
    expect((await call(deps, "DELETE", "/v1/names/studio/acme-challenge", { token: main })).status).toBe(204);
    expect(deps.memoryDns.records.has("_acme-challenge.studio.darkpyonix.dev")).toBe(false);

    const listed = (await (await call(deps, "GET", "/v1/names", { cookie })).json()) as { names: { name: string }[] };
    expect(listed.names.map((n) => n.name)).toEqual(["studio"]);
  });

  it("test_fr_h5_only_main_servers_hold_names_and_names_are_unique", async () => {
    const deps = makeDeps();
    const alice = await signIn(deps, { id: 42, login: "alice" });
    const bob = await signIn(deps, { id: 43, login: "bob" });
    const aliceMain = await linkDevice(deps, { cookie: alice }, await newDevice(), "main_server");
    const aliceComputer = await linkDevice(deps, { cookie: alice }, await newDevice(), "computer");
    const bobMain = await linkDevice(deps, { cookie: bob }, await newDevice(), "main_server");
    expect((await call(deps, "PUT", "/v1/names/lab", { token: aliceComputer })).status).toBe(403);
    expect((await call(deps, "PUT", "/v1/names/lab", { token: aliceMain })).status).toBe(201);
    expect((await call(deps, "PUT", "/v1/names/lab", { token: bobMain })).status).toBe(409);
    expect((await call(deps, "PUT", "/v1/names/lab/acme-challenge", { token: bobMain, json: { values: [DIGEST] } })).status).toBe(404);
    expect((await call(deps, "PUT", "/v1/names/relay", { token: aliceMain })).status).toBe(400);
    expect((await call(deps, "PUT", "/v1/names/Bad_Name", { token: aliceMain })).status).toBe(400);
  });

  it("test_fr_h5_bad_values_and_provider_failures", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 44, login: "owner" });
    const main = await linkDevice(deps, { cookie }, await newDevice(), "main_server");
    await call(deps, "PUT", "/v1/names/box", { token: main });
    for (const values of [[], ["short"], [DIGEST, DIGEST, DIGEST, DIGEST, DIGEST]]) {
      expect((await call(deps, "PUT", "/v1/names/box/acme-challenge", { token: main, json: { values } })).status).toBe(400);
    }
    deps.memoryDns.failing = true;
    expect((await call(deps, "PUT", "/v1/names/box/acme-challenge", { token: main, json: { values: [DIGEST] } })).status).toBe(502);
  });

  it("test_fr_h5_release_and_device_removal_clear_records", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 45, login: "owner" });
    const device = await newDevice();
    const main = await linkDevice(deps, { cookie }, device, "main_server");
    await call(deps, "PUT", "/v1/names/one", { token: main });
    await call(deps, "PUT", "/v1/names/one/acme-challenge", { token: main, json: { values: [DIGEST] } });
    expect((await call(deps, "DELETE", "/v1/names/one", { cookie })).status).toBe(204);
    await Promise.all(deps.pending);
    expect(deps.memoryDns.records.size).toBe(0);
    await call(deps, "PUT", "/v1/names/two", { token: main });
    await call(deps, "PUT", "/v1/names/two/acme-challenge", { token: main, json: { values: [DIGEST] } });
    await call(deps, "DELETE", `/v1/devices/${device.endpointId}`, { cookie });
    await Promise.all(deps.pending);
    expect(deps.memoryDns.records.size).toBe(0);
    expect((await call(deps, "GET", "/v1/names", { cookie })).status).toBe(200);
  });
});

describe("Cloudflare DNS client", () => {
  function fakeCloudflare(existing: { id: string; content: string }[], fail = false) {
    const calls: { method: string; url: string; body: unknown; auth: string | null }[] = [];
    const fetcher = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      const body = request.method === "GET" ? null : await request.json();
      calls.push({ method: request.method, url: request.url, body, auth: request.headers.get("authorization") });
      if (fail) return Response.json({ success: false, errors: [{ code: 10000, message: "Authentication error" }], result: null }, { status: 403 });
      return Response.json({ success: true, errors: [], result: request.method === "GET" ? existing : {} });
    }) as typeof fetch;
    return { calls, fetcher };
  }

  it("test_fr_h5_cloudflare_replaces_txt_in_one_batch", async () => {
    const { calls, fetcher } = fakeCloudflare([{ id: "r1", content: '"old"' }]);
    await new CloudflareDns("tok", "zone123", fetcher).setTxt("_acme-challenge.x.darkpyonix.dev", [DIGEST]);
    expect(calls[0].method).toBe("GET");
    expect(calls[0].url).toBe(
      "https://api.cloudflare.com/client/v4/zones/zone123/dns_records?type=TXT&name=_acme-challenge.x.darkpyonix.dev&per_page=100",
    );
    expect(calls[0].auth).toBe("Bearer tok");
    expect(calls[1]).toMatchObject({
      method: "POST",
      url: "https://api.cloudflare.com/client/v4/zones/zone123/dns_records/batch",
      body: {
        deletes: [{ id: "r1" }],
        posts: [{ type: "TXT", name: "_acme-challenge.x.darkpyonix.dev", content: `"${DIGEST}"`, ttl: 60 }],
      },
    });
  });

  it("test_fr_h5_cloudflare_clear_and_errors", async () => {
    const empty = fakeCloudflare([]);
    await new CloudflareDns("tok", "z", empty.fetcher).clearTxt("_acme-challenge.y.darkpyonix.dev");
    expect(empty.calls).toHaveLength(1); // nothing to delete, no batch
    const refused = fakeCloudflare([], true);
    await expect(new CloudflareDns("tok", "z", refused.fetcher).setTxt("n", [DIGEST])).rejects.toBeInstanceOf(DnsError);
  });
});
