import { describe, expect, it } from "vitest";
import { cleanup } from "../src/app";
import { call, hubEnv, linkDevice, makeDeps, newDevice, signIn } from "./helpers";

async function setup(visibility = "private") {
  const deps = makeDeps(), cookie = await signIn(deps, { id: Math.floor(Math.random() * 1e9), login: `user${crypto.randomUUID().slice(0, 8)}` });
  const created = await call(deps, "POST", "/notebooks", { cookie, json: { title: "Notebook", visibility } });
  expect(created.status).toBe(201);
  const notebook = await created.json() as any;
  return { deps, cookie, notebook, path: `/notebooks/${notebook.notebook_id}` };
}
function form(source: Blob = new Blob(["# %%\nprint(1)\n"]), name = "test.py", run?: Blob) {
  const f = new FormData(); f.append("source", source, name); if (run) f.append("run", run, "run.ipynb"); return f;
}
async function push(s: Awaited<ReturnType<typeof setup>>, body = form()) { return call(s.deps, "POST", `${s.path}/versions`, { cookie: s.cookie, body }); }
const validRun = () => new Blob([JSON.stringify({ nbformat: 4, cells: [{ outputs: [{ output_type: "stream" }] }] })]);
describe("notebook storage", () => {
  it("test_fr_h14_create_and_push_versions", async () => { const s = await setup(); expect((await (await push(s)).json() as any).version).toBe(1); expect((await (await push(s)).json() as any).version).toBe(2); });
  it("test_fr_h14_versions_are_numbered_once_under_concurrency", async () => { const s = await setup(); const rs = await Promise.all([push(s), push(s)]); expect((await Promise.all(rs.map(r => r.json())) as any[]).map(v => v.version).sort()).toEqual([1, 2]); });
  it("test_fr_h14_content_round_trips_with_sha256", async () => { const s = await setup(); const v = await (await push(s, form(undefined, "test.py", validRun()))).json() as any; const response = await call(s.deps, "GET", `${s.path}/versions/latest/source`, { cookie: s.cookie }); const bytes = await response.arrayBuffer(); expect(new TextDecoder().decode(bytes)).toBe("# %%\nprint(1)\n"); expect(v.source.sha256).toBe(Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)), b => b.toString(16).padStart(2, "0")).join("")); expect((await call(s.deps, "GET", `${s.path}/versions/1/run`, { cookie: s.cookie })).status).toBe(200); });
  it("test_fr_h14_rejects_malformed_source_and_run", async () => { const s = await setup(); for (const f of [form(new Blob([new Uint8Array([255])])), form(new Blob(["a\0b"])), form(undefined, "bad.txt"), form(undefined, "ok.py", new Blob(['{"nbformat":3,"cells":[]}'])), form(undefined, "ok.py", new Blob(['{"nbformat":4,"cells":[{"outputs":[{"output_type":"bad"}]}]}']))]) expect((await push(s, f)).status).toBe(400); });
  it("test_fr_h14_client_devices_cannot_publish", async () => { const s = await setup(); const token = await linkDevice(s.deps, { cookie: s.cookie }, await newDevice(), "client"); expect((await call(s.deps, "POST", "/notebooks", { token, json: { title: "x" } })).status).toBe(403); expect((await call(s.deps, "POST", `${s.path}/versions`, { token, body: form() })).status).toBe(403); });
  it("test_fr_h14_content_responses_cannot_render_as_documents", async () => { const s = await setup(); await push(s, form(undefined, "test.py", validRun())); for (const part of ["source", "run"]) { const r = await call(s.deps, "GET", `${s.path}/versions/1/${part}`, { cookie: s.cookie }); expect(r.headers.get("x-content-type-options")).toBe("nosniff"); expect(r.headers.get("content-security-policy")).toBe("sandbox; default-src 'none'"); expect(r.headers.get("content-disposition")).toContain("attachment"); expect(r.headers.get("etag")).toMatch(/^"[0-9a-f]{64}"$/); } });
  it("test_fr_h15_private_notebooks_are_hidden", async () => { const s = await setup(); expect((await call(s.deps, "GET", s.path)).status).toBe(404); expect((await call(s.deps, "GET", s.path, { cookie: s.cookie })).status).toBe(200); });
  it("test_fr_h15_unlisted_notebooks_open_by_link_only", async () => { const s = await setup("unlisted"); const r = await call(s.deps, "GET", s.path); expect(r.status).toBe(200); expect(r.headers.get("x-robots-tag")).toBe("noindex"); const list = await (await call(s.deps, "GET", `/users/${s.notebook.owner.github_login}/notebooks`)).json() as any; expect(list.notebooks).toEqual([]); });
  it("test_fr_h15_public_notebooks_are_listed", async () => { const s = await setup("public"); const list = await (await call(s.deps, "GET", `/users/${s.notebook.owner.github_login}/notebooks`)).json() as any; expect(list.notebooks[0].notebook_id).toBe(s.notebook.notebook_id); });
  it("test_fr_h15_going_private_takes_effect_at_once", async () => { const s = await setup("public"); expect((await call(s.deps, "PATCH", s.path, { cookie: s.cookie, json: { visibility: "private" } })).status).toBe(200); expect((await call(s.deps, "GET", s.path)).status).toBe(404); });
  it("test_fr_h18_delete_notebook_removes_every_version", async () => { const s = await setup(); await push(s); await push(s); const before = await hubEnv.NOTEBOOKS.list(); expect(before.objects.filter(x => x.key.includes(s.notebook.notebook_id))).toHaveLength(2); expect((await call(s.deps, "DELETE", s.path, { cookie: s.cookie })).status).toBe(204); expect((await call(s.deps, "GET", s.path, { cookie: s.cookie })).status).toBe(404); expect((await hubEnv.NOTEBOOKS.list()).objects.filter(x => x.key.includes(s.notebook.notebook_id))).toHaveLength(0); });
  it("test_fr_h18_delete_version_keeps_numbers", async () => { const s = await setup(); await push(s); await push(s); expect((await call(s.deps, "DELETE", `${s.path}/versions/2`, { cookie: s.cookie })).status).toBe(204); expect((await (await call(s.deps, "GET", `${s.path}/versions/latest`, { cookie: s.cookie })).json() as any).version).toBe(1); expect((await (await push(s)).json() as any).version).toBe(3); });
  it("test_fr_h18_deleted_versions_are_gone_for_the_owner", async () => {
    const s = await setup(); await push(s, form(undefined, "test.py", validRun())); await push(s);
    expect((await call(s.deps, "DELETE", `${s.path}/versions/1`, { cookie: s.cookie })).status).toBe(204);
    for (const part of ["", "/source", "/run"]) expect((await call(s.deps, "GET", `${s.path}/versions/1${part}`, { cookie: s.cookie })).status).toBe(404);
    expect((await hubEnv.NOTEBOOKS.list({ prefix: `nb/` })).objects.filter(x => x.key.includes(`${s.notebook.notebook_id}/1/`))).toHaveLength(0);
    // The last version gone: no latest, and the notebook itself stays.
    expect((await call(s.deps, "DELETE", `${s.path}/versions/latest`, { cookie: s.cookie })).status).toBe(204);
    const left = await (await call(s.deps, "GET", s.path, { cookie: s.cookie })).json() as any;
    expect(left.latest_version).toBeNull();
    expect(left.versions).toEqual([]);
    expect((await call(s.deps, "GET", `${s.path}/versions/latest`, { cookie: s.cookie })).status).toBe(404);
    expect((await (await push(s)).json() as any).version).toBe(3);
  });
  it("test_fr_h18_other_accounts_and_clients_cannot_delete", async () => {
    const s = await setup("public"); await push(s);
    const other = await signIn(s.deps, { id: Math.floor(Math.random() * 1e9), login: `other${crypto.randomUUID().slice(0, 8)}` });
    const client = await linkDevice(s.deps, { cookie: s.cookie }, await newDevice(), "client");
    const computer = await linkDevice(s.deps, { cookie: s.cookie }, await newDevice(), "computer");
    for (const path of [s.path, `${s.path}/versions/1`]) {
      expect((await call(s.deps, "DELETE", path, { cookie: other })).status).toBe(404);
      expect((await call(s.deps, "DELETE", path, { token: client })).status).toBe(403);
      // A computer that did not create the notebook is not its admin either (FR-H14).
      expect((await call(s.deps, "DELETE", path, { token: computer })).status).toBe(403);
      expect((await call(s.deps, "DELETE", path)).status).toBe(401);
    }
    expect((await (await call(s.deps, "GET", s.path)).json() as any).latest_version).toBe(1);
  });
  it("test_fr_h18_removed_device_notebooks_stay_with_the_account", async () => {
    const s = await setup(); const device = await newDevice();
    const token = await linkDevice(s.deps, { cookie: s.cookie }, device, "computer");
    const made = await (await call(s.deps, "POST", "/notebooks", { token, json: { title: "By device" } })).json() as any;
    expect((await call(s.deps, "POST", `/notebooks/${made.notebook_id}/versions`, { token, body: form() })).status).toBe(201);
    expect((await call(s.deps, "DELETE", `/devices/${device.endpointId}`, { cookie: s.cookie })).status).toBe(204);
    // The removed device's token no longer works; the account keeps the notebook and its content.
    expect((await call(s.deps, "DELETE", `/notebooks/${made.notebook_id}`, { token })).status).toBe(401);
    const mine = await (await call(s.deps, "GET", "/notebooks", { cookie: s.cookie })).json() as any;
    expect(mine.notebooks.map((n: any) => n.notebook_id)).toContain(made.notebook_id);
    expect((await call(s.deps, "GET", `/notebooks/${made.notebook_id}/versions/1/source`, { cookie: s.cookie })).status).toBe(200);
    expect((await call(s.deps, "DELETE", `/notebooks/${made.notebook_id}`, { cookie: s.cookie })).status).toBe(204);
  });
  it("test_fr_h18_cron_purges_orphaned_blobs", async () => { const key = `nb/orphan/${crypto.randomUUID()}/1/source`; await hubEnv.NOTEBOOKS.put(key, "orphan"); await cleanup(hubEnv, Date.now() + 7200000); expect(await hubEnv.NOTEBOOKS.get(key)).toBeNull(); });
});
