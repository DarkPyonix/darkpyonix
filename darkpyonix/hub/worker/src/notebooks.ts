import type { Deps, Env } from "./env";
import { ApiError, json, noContent, principal, readJson, type Principal } from "./http";
import { ctEq, hashToken, nowSecs, sha256, toHex } from "./util";

/**
 * FR-H19 limits. Every value is provisional ("a first value, to be changed after real use"):
 * change them here and nowhere else. Sizes are bytes; windows are seconds.
 */
export const LIMITS = {
  sourceBytes: 1 * 1024 * 1024,
  runBytes: 16 * 1024 * 1024,
  requestBytes: 18 * 1024 * 1024,
  notebooksPerAccount: 200,
  versionsPerNotebook: 100,
  storedBytesPerAccount: 512 * 1024 * 1024,
  /** Per-minute rates live in wrangler.toml (PUSH_LIMITER, CREATE_LIMITER 10; READ_LIMITER 300). */
  reportsPerAddress: 10,
  reportWindowSecs: 3600,
  reasonMaxChars: 1000,
};

type Notebook = { notebook_id: string; account_id: string; creator_device: string | null; title: string; visibility: string; created_at: number; updated_at: number; github_login: string; latest_version: number | null; taken_down: number };
type Version = { version: number; created_at: number; message: string | null; source_filename: string; source_sha256: string; source_size: number; source_cells: number; run_id: string | null; run_sha256: string | null; run_size: number | null };
const selectNotebook = `SELECT n.*, a.github_login, (SELECT MAX(version) FROM notebook_versions v WHERE v.notebook_id = n.notebook_id) AS latest_version FROM notebooks n JOIN accounts a ON a.account_id = n.account_id`;
const view = (n: Notebook) => ({ notebook_id: n.notebook_id, owner: { github_login: n.github_login }, title: n.title, visibility: n.visibility, share_id: null, latest_version: n.latest_version, created_at: n.created_at, updated_at: n.updated_at, taken_down: n.taken_down === 1 });
const versionView = (v: Version) => ({ version: v.version, created_at: v.created_at, message: v.message, source: { filename: v.source_filename, sha256: v.source_sha256, size: v.source_size, cells: v.source_cells }, run: v.run_id === null ? null : { run_id: v.run_id, sha256: v.run_sha256, size: v.run_size } });
const robot = (n: Notebook): Record<string, string> => n.visibility === "unlisted" ? { "x-robots-tag": "noindex" } : {};
const key = (n: Notebook, v: number, part: string) => `nb/${n.account_id}/${n.notebook_id}/${v}/${part}`;
async function find(env: Env, id: string): Promise<Notebook> {
  const n = await env.DB.prepare(`${selectNotebook} WHERE n.notebook_id = ?`).bind(id).first<Notebook>();
  if (!n) throw ApiError.notFound();
  return n;
}
async function caller(request: Request, env: Env, deps: Deps): Promise<Principal> { return principal(request, env, nowSecs(deps.nowMs())); }
const clientIp = (r: Request) => r.headers.get("cf-connecting-ip") ?? "unknown";
async function limit(limiter: Env["PUSH_LIMITER"], key: string) {
  if (limiter && !(await limiter.limit({ key })).success) throw ApiError.tooMany();
}
async function readable(request: Request, env: Env, deps: Deps, id: string): Promise<Notebook> {
  if (!request.headers.has("authorization") && !request.headers.has("cookie")) await limit(env.READ_LIMITER, `read:${clientIp(request)}`);
  const n = await find(env, id);
  if (n.visibility === "private" || n.taken_down) {
    let p: Principal;
    try { p = await caller(request, env, deps); } catch { throw ApiError.notFound(); }
    if (p.accountId !== n.account_id) throw ApiError.notFound();
  }
  return n;
}
async function owned(request: Request, env: Env, deps: Deps, id: string, admin: boolean): Promise<Notebook> {
  const p = await caller(request, env, deps);
  const n = await find(env, id);
  if (p.accountId !== n.account_id) throw ApiError.notFound();
  if (p.kind === "device" && (admin ? p.role !== "main_server" && p.endpointId !== n.creator_device : p.role === "client")) throw ApiError.forbidden("not permitted");
  return n;
}
function fields(body: Record<string, unknown>, partial: boolean) {
  if (Object.keys(body).some(k => !["title", "visibility"].includes(k)) || (partial && !Object.keys(body).length)) throw ApiError.badRequest("invalid fields");
  if ((!partial || body.title !== undefined) && (typeof body.title !== "string" || body.title.length < 1 || body.title.length > 120)) throw ApiError.badRequest("invalid title");
  if (body.visibility !== undefined && !["private", "unlisted", "public"].includes(body.visibility as string)) throw ApiError.badRequest("invalid visibility");
}
export async function createNotebook(r: Request, e: Env, d: Deps) {
  const p = await caller(r, e, d);
  if (p.kind === "device" && p.role === "client") throw ApiError.forbidden("not permitted");
  const body = await readJson<Record<string, unknown>>(r); fields(body, false);
  await limit(e.CREATE_LIMITER, `create:${p.accountId}`);
  const id = `n_${toHex(crypto.getRandomValues(new Uint8Array(16)))}`; const now = nowSecs(d.nowMs());
  const inserted = await e.DB.prepare("INSERT INTO notebooks (notebook_id, account_id, creator_device, title, visibility, created_at, updated_at) SELECT ?, ?, ?, ?, ?, ?, ? WHERE (SELECT COUNT(*) FROM notebooks WHERE account_id = ?) < ?").bind(id, p.accountId, p.kind === "device" ? p.endpointId : null, body.title, body.visibility ?? "private", now, now, p.accountId, LIMITS.notebooksPerAccount).run();
  if (!inserted.meta.changes) throw ApiError.conflict("notebook quota exceeded", "quota_exceeded");
  return json(201, view(await find(e, id)));
}
export async function listMine(r: Request, e: Env, d: Deps) {
  const p = await caller(r, e, d);
  const rows = await e.DB.prepare(`${selectNotebook} WHERE n.account_id = ? ORDER BY n.created_at DESC, n.rowid DESC`).bind(p.accountId).all<Notebook>();
  return json(200, { notebooks: rows.results.map(view) });
}
export async function listPublic(_r: Request, e: Env, _d: Deps, login: string) {
  const account = await e.DB.prepare("SELECT account_id FROM accounts WHERE github_login = ? COLLATE NOCASE").bind(login).first<{account_id: string}>();
  if (!account) throw ApiError.notFound();
  const rows = await e.DB.prepare(`${selectNotebook} WHERE n.account_id = ? AND n.visibility = 'public' AND n.taken_down = 0 ORDER BY n.created_at DESC, n.rowid DESC`).bind(account.account_id).all<Notebook>();
  return json(200, { notebooks: rows.results.map(view) });
}
export async function getNotebook(r: Request, e: Env, d: Deps, id: string) {
  const n = await readable(r, e, d, id);
  const rows = await e.DB.prepare("SELECT * FROM notebook_versions WHERE notebook_id = ? ORDER BY version DESC").bind(id).all<Version>();
  return json(200, { ...view(n), versions: rows.results.map(versionView) }, robot(n));
}
export async function updateNotebook(r: Request, e: Env, d: Deps, id: string) {
  const n = await owned(r, e, d, id, true); const body = await readJson<Record<string, unknown>>(r); fields(body, true);
  const rank = ["private", "unlisted", "public"];
  if (n.taken_down && body.visibility !== undefined && rank.indexOf(body.visibility as string) > rank.indexOf(n.visibility)) throw ApiError.conflict("a taken-down notebook cannot be made more visible");
  await e.DB.prepare("UPDATE notebooks SET title = ?, visibility = ?, updated_at = ? WHERE notebook_id = ?").bind(body.title ?? n.title, body.visibility ?? n.visibility, nowSecs(d.nowMs()), id).run();
  return json(200, view(await find(e, id)));
}
async function getVersion(e: Env, n: Notebook, ref: string): Promise<Version> {
  const number = ref === "latest" ? n.latest_version : /^[1-9][0-9]*$/.test(ref) ? Number(ref) : null;
  if (number === null) throw ApiError.notFound();
  const v = await e.DB.prepare("SELECT * FROM notebook_versions WHERE notebook_id = ? AND version = ?").bind(n.notebook_id, number).first<Version>();
  if (!v) throw ApiError.notFound(); return v;
}
export async function pushVersion(r: Request, e: Env, d: Deps, id: string) {
  const n = await owned(r, e, d, id, false);
  await limit(e.PUSH_LIMITER, `push:${n.account_id}`);
  if (Number(r.headers.get("content-length")) > LIMITS.requestBytes) throw ApiError.tooLarge("request over 18 MiB");
  let form: FormData; try { form = await r.formData(); } catch { throw ApiError.badRequest("invalid multipart body"); }
  const source = form.get("source"), run = form.get("run"), message = form.get("message");
  if (!(source instanceof File) || !/^[^/\\\x00]{1,128}\.(py|pynb)$/.test(source.name) || (run !== null && !(run instanceof File)) || (message !== null && (typeof message !== "string" || message.length > 500))) throw ApiError.badRequest("invalid parts");
  if (source.size > LIMITS.sourceBytes) throw ApiError.tooLarge("source over 1 MiB");
  if (run instanceof File && run.size > LIMITS.runBytes) throw ApiError.tooLarge("run over 16 MiB");
  if (source.size + (run instanceof File ? run.size : 0) > LIMITS.requestBytes) throw ApiError.tooLarge("request over 18 MiB");
  const bytes = await source.arrayBuffer(); let text: string;
  try { text = new TextDecoder("utf-8", { fatal: true }).decode(bytes); } catch { throw ApiError.badRequest("invalid UTF-8"); }
  if (text.includes("\0")) throw ApiError.badRequest("NUL in source");
  let runBytes: ArrayBuffer | null = null;
  if (run instanceof File) {
    runBytes = await run.arrayBuffer();
    try {
      const value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(runBytes));
      if (!value || typeof value !== "object" || Array.isArray(value) || value.nbformat !== 4 || !Array.isArray(value.cells) || value.cells.some((c: any) => !c || typeof c !== "object" || (c.outputs !== undefined && (!Array.isArray(c.outputs) || c.outputs.some((o: any) => !o || !["stream", "display_data", "execute_result", "error"].includes(o.output_type)))))) throw new Error();
    } catch { throw ApiError.badRequest("invalid run"); }
  }
  // The quota checks and the number allocation are one statement, so concurrent pushes cannot both pass.
  const allocated = await e.DB.prepare(`UPDATE notebooks SET next_version = next_version + 1, updated_at = ? WHERE notebook_id = ?
    AND (SELECT COUNT(*) FROM notebook_versions WHERE notebook_id = ?) < ?
    AND (SELECT COALESCE(SUM(v.source_size + COALESCE(v.run_size, 0)), 0) FROM notebook_versions v JOIN notebooks m ON m.notebook_id = v.notebook_id WHERE m.account_id = ?) + ? <= ?
    RETURNING next_version`).bind(nowSecs(d.nowMs()), id, id, LIMITS.versionsPerNotebook, n.account_id, bytes.byteLength + (runBytes?.byteLength ?? 0), LIMITS.storedBytesPerAccount).first<{next_version: number}>();
  if (!allocated) {
    if (!(await e.DB.prepare("SELECT 1 FROM notebooks WHERE notebook_id = ?").bind(id).first())) throw ApiError.notFound();
    throw ApiError.conflict("version or storage quota exceeded", "quota_exceeded");
  } const v = allocated.next_version;
  await e.NOTEBOOKS.put(key(n, v, "source"), bytes);
  if (runBytes) await e.NOTEBOOKS.put(key(n, v, "run"), runBytes);
  const sourceHash = toHex(await sha256(new Uint8Array(bytes))), runHash = runBytes ? toHex(await sha256(new Uint8Array(runBytes))) : null;
  const cells = text.split(/\r?\n/).filter(line => /^# %%(?:[ \t]+[^\[\n]*?)?(?:[ \t]*\[[A-Za-z_][A-Za-z0-9_-]*\])?[ \t]*$/.test(line)).length;
  await e.DB.prepare("INSERT INTO notebook_versions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").bind(id, v, nowSecs(d.nowMs()), message, source.name, sourceHash, bytes.byteLength, cells, run instanceof File ? run.name.replace(/\.ipynb$/, "") : null, runHash, runBytes?.byteLength ?? null).run();
  return json(201, versionView(await getVersion(e, n, String(v))));
}
export async function readVersion(r: Request, e: Env, d: Deps, id: string, ref: string) {
  const n = await readable(r, e, d, id); return json(200, versionView(await getVersion(e, n, ref)), robot(n));
}
async function content(r: Request, e: Env, d: Deps, id: string, ref: string, part: "source" | "run") {
  const n = await readable(r, e, d, id), v = await getVersion(e, n, ref);
  const hash = part === "source" ? v.source_sha256 : v.run_sha256;
  if (!hash) throw ApiError.notFound();
  const object = await e.NOTEBOOKS.get(key(n, v.version, part)); if (!object) throw ApiError.notFound();
  const filename = part === "source" ? v.source_filename : `${v.run_id}.ipynb`;
  const headers = { ...robot(n), "content-type": part === "source" ? "text/plain; charset=utf-8" : "application/json", "x-content-type-options": "nosniff", "content-security-policy": "sandbox; default-src 'none'", "content-disposition": `attachment; filename*=UTF-8''${encodeURIComponent(filename)}`, "cache-control": "private, max-age=60", etag: `"${hash}"` };
  return new Response(r.headers.get("if-none-match") === `"${hash}"` ? null : object.body, { status: r.headers.get("if-none-match") === `"${hash}"` ? 304 : 200, headers });
}
export const readSource = (r: Request, e: Env, d: Deps, id: string, ref: string) => content(r, e, d, id, ref, "source");
export const readRun = (r: Request, e: Env, d: Deps, id: string, ref: string) => content(r, e, d, id, ref, "run");
export async function deleteVersion(r: Request, e: Env, d: Deps, id: string, ref: string) {
  const n = await owned(r, e, d, id, true), v = await getVersion(e, n, ref);
  await e.DB.prepare("DELETE FROM notebook_versions WHERE notebook_id = ? AND version = ?").bind(id, v.version).run();
  try { await e.NOTEBOOKS.delete([key(n, v.version, "source"), key(n, v.version, "run")]); } catch { /* Cron retries orphan cleanup. */ }
  return noContent();
}
export async function deleteNotebook(r: Request, e: Env, d: Deps, id: string) {
  const n = await owned(r, e, d, id, true);
  const rows = await e.DB.prepare("DELETE FROM notebook_versions WHERE notebook_id = ? RETURNING version").bind(id).all<{version: number}>();
  await e.DB.prepare("DELETE FROM notebooks WHERE notebook_id = ?").bind(id).run();
  const keys = rows.results.flatMap(v => [key(n, v.version, "source"), key(n, v.version, "run")]);
  if (keys.length) try { await e.NOTEBOOKS.delete(keys); } catch { /* Cron retries orphan cleanup. */ }
  return noContent();
}
export async function purgeOrphans(e: Env, nowMs: number) {
  let cursor: string | undefined;
  do {
    const page = await e.NOTEBOOKS.list({ prefix: "nb/", cursor });
    for (const object of page.objects) {
      if (object.uploaded.getTime() > nowMs - 60 * 60 * 1000) continue;
      const match = /^nb\/([^/]+)\/([^/]+)\/(\d+)\/(source|run)$/.exec(object.key);
      if (!match) continue;
      const exists = await e.DB.prepare("SELECT 1 FROM notebook_versions v JOIN notebooks n ON n.notebook_id = v.notebook_id WHERE n.account_id = ? AND v.notebook_id = ? AND v.version = ? AND (? = 'source' OR v.run_id IS NOT NULL)").bind(match[1], match[2], Number(match[3]), match[4]).first();
      if (!exists) await e.NOTEBOOKS.delete(object.key);
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
}

export async function reportNotebook(r: Request, e: Env, d: Deps, id: string) {
  const body = await readJson<Record<string, unknown>>(r);
  if (Object.keys(body).some(k => k !== "reason") || typeof body.reason !== "string" || body.reason.length < 1 || body.reason.length > LIMITS.reasonMaxChars) throw ApiError.badRequest("invalid reason");
  const n = await readable(r, e, d, id);
  if (n.visibility === "private" || n.taken_down) throw ApiError.notFound();
  const now = nowSecs(d.nowMs()), ip = toHex(await sha256(clientIp(r)));
  const recent = await e.DB.prepare("SELECT COUNT(*) AS c FROM notebook_reports WHERE ip_hash = ? AND created_at > ?").bind(ip, now - LIMITS.reportWindowSecs).first<{c: number}>();
  if ((recent?.c ?? 0) >= LIMITS.reportsPerAddress) throw ApiError.tooMany("too many reports from this address");
  let reporter: string | null = null;
  try { reporter = (await caller(r, e, d)).accountId; } catch { /* Anonymous reports are allowed. */ }
  await e.DB.prepare("INSERT INTO notebook_reports (notebook_id, reporter_account, ip_hash, reason, created_at) VALUES (?, ?, ?, ?, ?)").bind(id, reporter, ip, body.reason, now).run();
  return new Response(null, { status: 202 });
}
async function operator(r: Request, e: Env) {
  const token = /^Bearer (.+)$/i.exec(r.headers.get("authorization") ?? "")?.[1]?.trim();
  if (!e.OPERATOR_SECRET || !token || !ctEq(await hashToken(token), await hashToken(e.OPERATOR_SECRET))) throw ApiError.unauthorized();
}
async function setTakenDown(r: Request, e: Env, d: Deps, id: string, down: boolean) {
  await operator(r, e);
  const done = await e.DB.prepare("UPDATE notebooks SET taken_down = ?, taken_down_at = ? WHERE notebook_id = ?").bind(down ? 1 : 0, down ? nowSecs(d.nowMs()) : null, id).run();
  if (!done.meta.changes) throw ApiError.notFound();
  return noContent();
}
export const takeDownNotebook = (r: Request, e: Env, d: Deps, id: string) => setTakenDown(r, e, d, id, true);
export const restoreNotebook = (r: Request, e: Env, d: Deps, id: string) => setTakenDown(r, e, d, id, false);
