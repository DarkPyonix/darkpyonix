// FR-H17 notebook attached to a live share.

import { describe, expect, it } from "vitest";
import { call, linkDevice, makeDeps, newDevice, signIn } from "./helpers";

const newShare = () => `s_${Array.from(crypto.getRandomValues(new Uint8Array(8)), b => b.toString(16).padStart(2, "0")).join("")}`;
async function setup() {
  const deps = makeDeps(), cookie = await signIn(deps, { id: Math.floor(Math.random() * 1e9), login: `user${crypto.randomUUID().slice(0, 8)}` });
  const notebook = await (await call(deps, "POST", "/notebooks", { cookie, json: { title: "Live", visibility: "public" } })).json() as any;
  const token = await linkDevice(deps, { cookie }, await newDevice(), "main_server");
  return { deps, cookie, token, path: `/notebooks/${notebook.notebook_id}` };
}
const attach = (s: Awaited<ReturnType<typeof setup>>, share_id: string | null) => call(s.deps, "PATCH", s.path, { cookie: s.cookie, json: { share_id } });

describe("live kernel attachment", () => {
  it("test_fr_h17_attach_a_share_of_the_same_account", async () => {
    const s = await setup(), SHARE = newShare();
    expect((await (await call(s.deps, "GET", s.path)).json() as any).share_id).toBeNull();
    expect((await call(s.deps, "POST", "/shares", { token: s.token, json: { share_id: SHARE } })).status).toBe(201);
    const patched = await attach(s, SHARE);
    expect(patched.status).toBe(200);
    expect((await patched.json() as any).share_id).toBe(SHARE);
    // Visible to anyone who can read the notebook; other fields keep their values.
    const read = await (await call(s.deps, "GET", s.path)).json() as any;
    expect(read.share_id).toBe(SHARE);
    expect(read.title).toBe("Live");
    // A title change keeps the attachment; null detaches.
    expect((await (await call(s.deps, "PATCH", s.path, { cookie: s.cookie, json: { title: "Renamed" } })).json() as any).share_id).toBe(SHARE);
    expect((await (await attach(s, null)).json() as any).share_id).toBeNull();
  });

  it("test_fr_h17_other_accounts_and_unknown_shares_are_refused", async () => {
    const s = await setup();
    expect((await attach(s, "s_00000000000000ff")).status).toBe(404);
    const other = await signIn(s.deps, { id: Math.floor(Math.random() * 1e9), login: `other${crypto.randomUUID().slice(0, 8)}` });
    const foreignShare = newShare(), foreign = await linkDevice(s.deps, { cookie: other }, await newDevice(), "main_server");
    expect((await call(s.deps, "POST", "/shares", { token: foreign, json: { share_id: foreignShare } })).status).toBe(201);
    const refused = await attach(s, foreignShare);
    expect(refused.status).toBe(409);
    expect((await refused.json() as any).code).toBe("share_not_in_account");
    expect((await call(s.deps, "PATCH", s.path, { cookie: s.cookie, json: { share_id: "bad" } })).status).toBe(400);
    expect((await (await call(s.deps, "GET", s.path)).json() as any).share_id).toBeNull();
  });

  it("test_fr_h17_unpublished_share_detaches", async () => {
    const s = await setup(), SHARE = newShare();
    await call(s.deps, "POST", "/shares", { token: s.token, json: { share_id: SHARE } });
    await attach(s, SHARE);
    expect((await call(s.deps, "DELETE", `/shares/${SHARE}`, { token: s.token })).status).toBe(204);
    expect((await (await call(s.deps, "GET", s.path)).json() as any).share_id).toBeNull();
  });
});
