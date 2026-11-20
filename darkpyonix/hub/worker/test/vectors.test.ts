// FR-H1, FR-H2 (#21): bytes made by the Rust code a real device runs are accepted as they are.
// test/vectors/rust.json comes from test/vectors/gen (iroh-base key, iroh-dns signed packet).

import { describe, expect, it } from "vitest";
import { fromHex } from "../src/util";
import { call, hubEnv, makeDeps, signIn } from "./helpers";
import raw from "./vectors/rust.json?raw";

const vector = JSON.parse(raw) as {
  endpoint_id: string;
  z32: string;
  link: { link_id: string; challenge: string; signature: string };
  pkarr: { timestamp_us: number; relay: string; addrs: string[]; payload: string };
};

type Deps = ReturnType<typeof makeDeps>;

/** Opens and approves a link for the vector's key, with the vector's link id and challenge. */
async function approvedVectorLink(deps: Deps, cookie: string): Promise<void> {
  const created = await call(deps, "POST", "/device-links", {
    json: { endpoint_id: vector.endpoint_id, name: "rust device", role: "computer" },
  });
  expect(created.status).toBe(201);
  const link = (await created.json()) as { link_id: string; user_code: string };
  const approved = await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: true } });
  expect(approved.status).toBe(204);
  // The hub draws both values at random; the Rust signature covers these fixed ones.
  await hubEnv.DB.prepare("UPDATE device_links SET link_id = ?, challenge = ? WHERE link_id = ?")
    .bind(vector.link.link_id, vector.link.challenge, link.link_id)
    .run();
}

function claim(deps: Deps, signature: string): Promise<Response> {
  return call(deps, "POST", `/device-links/${vector.link.link_id}/token`, { json: { signature } });
}

describe("rust-made vectors", () => {
  let tokens: { device_token: string; resolve_token: string } | null = null;

  it("test_fr_h1_rust_device_key_signature_claims_the_token", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 901, login: "rustacean" });
    await approvedVectorLink(deps, cookie);

    // One flipped bit of the signature is refused, and the link stays claimable.
    const last = vector.link.signature.slice(-1) === "0" ? "1" : "0";
    expect((await claim(deps, vector.link.signature.slice(0, -1) + last)).status).toBe(400);

    const claimed = await claim(deps, vector.link.signature);
    expect(claimed.status).toBe(201);
    tokens = (await claimed.json()) as { device_token: string; resolve_token: string };

    const devices = await call(deps, "GET", "/devices", { token: tokens.device_token });
    const list = ((await devices.json()) as { devices: { endpoint_id: string }[] }).devices;
    expect(list.map((d) => d.endpoint_id)).toEqual([vector.endpoint_id]);
  });

  it("test_fr_h2_rust_made_packet_is_stored_and_served_byte_for_byte", async () => {
    expect(tokens, "the FR-H1 vector test registers the device").not.toBeNull();
    const deps = makeDeps();
    const payload = fromHex(vector.pkarr.payload, vector.pkarr.payload.length)!;
    // iroh-dns builds a compressed DNS packet: the second and third names are pointers.
    expect(vector.pkarr.payload).toContain("c00c");

    const put = await call(deps, "PUT", `/pkarr/${vector.z32}`, { body: payload });
    expect(put.status).toBe(204);

    const got = await call(deps, "GET", `/pkarr/${vector.z32}?token=${tokens!.resolve_token}`);
    expect(got.status).toBe(200);
    expect(new Uint8Array(await got.arrayBuffer())).toEqual(payload);

    const addresses = await call(deps, "GET", `/devices/${vector.endpoint_id}/addresses`, { token: tokens!.device_token });
    expect(addresses.status).toBe(200);
    expect(await addresses.json()).toMatchObject({
      endpoint_id: vector.endpoint_id,
      relay_urls: [vector.pkarr.relay],
      direct_addresses: vector.pkarr.addrs,
      published_at_us: vector.pkarr.timestamp_us,
    });
  });
});
