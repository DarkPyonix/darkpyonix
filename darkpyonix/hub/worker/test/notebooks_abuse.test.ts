// FR-H19: size, quota and abuse limits, reports, operator takedown.
import { describe, expect, it } from "vitest";
import { LIMITS } from "../src/notebooks";
import { call, hubEnv, makeDeps, signIn } from "./helpers";

const SECRET = "operator-secret";
const operator = { OPERATOR_SECRET: SECRET };

async function setup(visibility = "private") {
  const deps = makeDeps(), cookie = await signIn(deps, { id: Math.floor(Math.random() * 1e9), login: `user${crypto.randomUUID().slice(0, 8)}` });
  const created = await call(deps, "POST", "/notebooks", { cookie, json: { title: "Notebook", visibility } });
  expect(created.status).toBe(201);
  const notebook = await created.json() as any;
  return { deps, cookie, notebook, id: notebook.notebook_id as string, path: `/notebooks/${notebook.notebook_id}` };
}
type S = Awaited<ReturnType<typeof setup>>;
const bytes = (n: number) => new Blob([new Uint8Array(n).fill(97)]);
function form(source: Blob = new Blob(["# %%\n"]), run?: Blob, pad?: Blob) {
  const f = new FormData(); f.append("source", source, "t.py"); if (run) f.append("run", run, "run.ipynb"); if (pad) f.append("pad", pad, "pad.bin"); return f;
}
const push = (s: S, body = form(), headers: Record<string, string> = {}) => call(s.deps, "POST", `${s.path}/versions`, { cookie: s.cookie, body, headers });
const validRunOf = (size: number) => { const head = '{"nbformat":4,"cells":[],"pad":"', tail = '"}'; return new Blob([head, "a".repeat(size - head.length - tail.length), tail]); };
async function accountOf(id: string) { return (await hubEnv.DB.prepare("SELECT account_id FROM notebooks WHERE notebook_id = ?").bind(id).first<{ account_id: string }>())!.account_id; }
async function insertVersion(id: string, version: number, size: number) {
  await hubEnv.DB.prepare("INSERT INTO notebook_versions (notebook_id, version, created_at, source_filename, source_sha256, source_size, source_cells) VALUES (?, ?, 0, 'x.py', 'h', ?, 0)").bind(id, version, size).run();
}

describe("size limits", () => {
  it("test_fr_h19_size_limits", async () => {
    const s = await setup();
    expect((await push(s, form(bytes(LIMITS.sourceBytes)))).status).toBe(201);
    expect((await push(s, form(bytes(LIMITS.sourceBytes + 1)))).status).toBe(413);
    expect((await push(s, form(undefined, validRunOf(LIMITS.runBytes)))).status).toBe(201);
    expect((await push(s, form(undefined, bytes(LIMITS.runBytes + 1)))).status).toBe(413);
  });
  it("test_fr_h19_request_size_limit", async () => {
    const s = await setup();
    const send = async (total: number) => {
      const measure = async (pad: number) => { const r = new Response(form(undefined, undefined, bytes(pad))); return { type: r.headers.get("content-type")!, blob: await r.blob() }; };
      const base = (await measure(0)).blob.size, m = await measure(total - base);
      expect(m.blob.size).toBe(total);
      return push(s, m.blob as unknown as FormData, { "content-type": m.type, "content-length": String(m.blob.size) });
    };
    expect((await send(LIMITS.requestBytes)).status).toBe(201);
    expect((await send(LIMITS.requestBytes + 1)).status).toBe(413);
  });
});

describe("quotas", () => {
  it("test_fr_h19_quotas", async () => {
    // notebooks per account
    const s = await setup(), account = await accountOf(s.id);
    await hubEnv.DB.batch(Array.from({ length: LIMITS.notebooksPerAccount - 2 }, (_, i) => hubEnv.DB.prepare("INSERT INTO notebooks (notebook_id, account_id, title, visibility, created_at, updated_at) VALUES (?, ?, 't', 'private', 0, 0)").bind(`n_fill${i}`, account)));
    expect((await call(s.deps, "POST", "/notebooks", { cookie: s.cookie, json: { title: "last" } })).status).toBe(201);
    const over = await call(s.deps, "POST", "/notebooks", { cookie: s.cookie, json: { title: "over" } });
    expect(over.status).toBe(409);
    expect((await over.json() as any).code).toBe("quota_exceeded");
    // versions per notebook, and nothing old is deleted
    const v = await setup();
    await hubEnv.DB.batch(Array.from({ length: LIMITS.versionsPerNotebook }, (_, i) => hubEnv.DB.prepare("INSERT INTO notebook_versions (notebook_id, version, created_at, source_filename, source_sha256, source_size, source_cells) VALUES (?, ?, 0, 'x.py', 'h', 1, 0)").bind(v.id, i + 1)));
    const full = await push(v);
    expect(full.status).toBe(409);
    expect((await full.json() as any).code).toBe("quota_exceeded");
    const rows = await hubEnv.DB.prepare("SELECT COUNT(*) AS c FROM notebook_versions WHERE notebook_id = ?").bind(v.id).first<{ c: number }>();
    expect(rows!.c).toBe(LIMITS.versionsPerNotebook);
    // stored bytes per account, across notebooks
    const b = await setup(), other = await call(b.deps, "POST", "/notebooks", { cookie: b.cookie, json: { title: "second" } });
    const otherId = (await other.json() as any).notebook_id as string;
    await insertVersion(otherId, 1, LIMITS.storedBytesPerAccount - 10);
    expect((await push(b, form(bytes(10)))).status).toBe(201);
    const bytesOver = await push(b, form(bytes(1)));
    expect(bytesOver.status).toBe(409);
    expect((await bytesOver.json() as any).code).toBe("quota_exceeded");
  });
});

describe("rate limits", () => {
  it("test_fr_h19_rate_limits", async () => {
    const s = await setup();
    const statuses: number[] = [];
    for (let i = 0; i < 11; i++) statuses.push((await push(s)).status);
    expect(statuses.slice(0, 10).every(x => x === 201)).toBe(true);
    expect(statuses[10]).toBe(429);
    const c = await setup(); // setup created 1 notebook for this fresh account
    const created: number[] = [];
    for (let i = 0; i < 10; i++) created.push((await call(c.deps, "POST", "/notebooks", { cookie: c.cookie, json: { title: "n" } })).status);
    expect(created.filter(x => x === 429).length).toBeGreaterThan(0);
    const pub = await setup("public"), headers = { "cf-connecting-ip": `reader-${crypto.randomUUID()}` };
    const reads: number[] = [];
    for (let i = 0; i < 301; i++) reads.push((await call(pub.deps, "GET", pub.path, { headers })).status);
    expect(reads.slice(0, 300).every(x => x === 200)).toBe(true);
    expect(reads[300]).toBe(429);
    // signed-in reads are not anonymous reads
    expect((await call(pub.deps, "GET", pub.path, { headers, cookie: pub.cookie })).status).toBe(200);
  });
});

describe("report and takedown", () => {
  const report = (s: S, reason: unknown, extra: { cookie?: string; headers?: Record<string, string> } = {}) => call(s.deps, "POST", `${s.path}/report`, { json: { reason }, ...extra });
  const takedown = (s: S, method: string, token?: string) => call(s.deps, method, `/admin/notebooks/${s.id}/takedown`, { token, env: operator });

  it("test_fr_h19_report_and_takedown", async () => {
    const s = await setup("public");
    expect((await report(s, "spam")).status).toBe(202);
    const row = await hubEnv.DB.prepare("SELECT reason, reporter_account FROM notebook_reports WHERE notebook_id = ?").bind(s.id).first<any>();
    expect(row).toEqual({ reason: "spam", reporter_account: null });
    expect((await takedown(s, "POST")).status).toBe(401);
    expect((await takedown(s, "POST", "wrong")).status).toBe(401);
    expect((await call(s.deps, "POST", `/admin/notebooks/${s.id}/takedown`, { token: SECRET })).status).toBe(401); // secret unset
    expect((await call(s.deps, "GET", s.path)).status).toBe(200);
    expect((await takedown(s, "POST", SECRET)).status).toBe(204);
    expect((await takedown(s, "POST", SECRET)).status).toBe(204);
    expect((await call(s.deps, "GET", s.path)).status).toBe(404);
    expect((await call(s.deps, "GET", `${s.path}/versions/latest`)).status).toBe(404);
    const list = await (await call(s.deps, "GET", `/users/${s.notebook.owner.github_login}/notebooks`)).json() as any;
    expect(list.notebooks).toEqual([]);
    expect((await report(s, "again")).status).toBe(404);
    const owner = await (await call(s.deps, "GET", s.path, { cookie: s.cookie })).json() as any;
    expect(owner.taken_down).toBe(true);
    expect((await (await call(s.deps, "GET", "/notebooks", { cookie: s.cookie })).json() as any).notebooks[0].taken_down).toBe(true);
    expect((await call(s.deps, "PATCH", s.path, { cookie: s.cookie, json: { title: "renamed" } })).status).toBe(200);
    expect((await call(s.deps, "PATCH", s.path, { cookie: s.cookie, json: { visibility: "private" } })).status).toBe(200);
    expect((await call(s.deps, "PATCH", s.path, { cookie: s.cookie, json: { visibility: "unlisted" } })).status).toBe(409);
    expect((await takedown(s, "DELETE", "wrong")).status).toBe(401);
    expect((await takedown(s, "DELETE", SECRET)).status).toBe(204);
    expect((await call(s.deps, "PATCH", s.path, { cookie: s.cookie, json: { visibility: "public" } })).status).toBe(200);
    expect((await call(s.deps, "GET", s.path)).status).toBe(200);
    expect((await call(s.deps, "POST", "/admin/notebooks/n_missing/takedown", { token: SECRET, env: operator })).status).toBe(404);
  });
  it("test_fr_h19_reports_need_a_public_or_unlisted_notebook_and_a_valid_reason", async () => {
    const priv = await setup(), unl = await setup("unlisted"), pub = await setup("public");
    expect((await report(priv, "x")).status).toBe(404);
    expect((await report(priv, "x", { cookie: priv.cookie })).status).toBe(404);
    expect((await report(unl, "x", { cookie: pub.cookie })).status).toBe(202);
    expect((await hubEnv.DB.prepare("SELECT reporter_account FROM notebook_reports WHERE notebook_id = ?").bind(unl.id).first<any>()).reporter_account).not.toBeNull();
    for (const bad of ["", "x".repeat(1001), 5, undefined]) expect((await report(pub, bad)).status).toBe(400);
    expect((await call(pub.deps, "POST", `${pub.path}/report`, { json: { reason: "x", extra: 1 } })).status).toBe(400);
    expect((await report(pub, "x".repeat(1000))).status).toBe(202);
    expect((await call(pub.deps, "POST", "/notebooks/n_missing/report", { json: { reason: "x" } })).status).toBe(404);
  });
  it("test_fr_h19_reports_are_rate_limited_per_address", async () => {
    const s = await setup("public"), headers = { "cf-connecting-ip": `reporter-${crypto.randomUUID()}` };
    for (let i = 0; i < LIMITS.reportsPerAddress; i++) expect((await report(s, "r", { headers })).status).toBe(202);
    expect((await report(s, "r", { headers })).status).toBe(429);
    expect((await report(s, "r")).status).toBe(202); // another address
    // the window moves on
    const later = { ...s.deps, nowMs: () => Date.now() + (LIMITS.reportWindowSecs + 1) * 1000 };
    expect((await call(later, "POST", `${s.path}/report`, { json: { reason: "r" }, headers })).status).toBe(202);
  });
});
