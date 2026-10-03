// HTTPS names (SPEC FR-H5): `<name>.<zone>` reserved by a main server, and the ACME DNS-01
// TXT values it asks us to publish through the Cloudflare DNS API. The main server runs its
// own ACME client and keeps its private key; TLS ends on the main server.

import { DnsError } from "./dns";
import type { Deps, Env } from "./env";
import { ApiError, type Principal, json, noContent, principal, readJson, requireDevice } from "./http";
import { ACME_VALUE_RE, isValidName, nowSecs } from "./util";

interface NameRow {
  name: string;
  endpoint_id: string;
  account_id: string;
}

function nameJson(env: Env, n: NameRow) {
  return { name: n.name, fqdn: `${n.name}.${env.ZONE}`, endpoint_id: n.endpoint_id };
}

export function acmeFqdn(env: Env, name: string): string {
  return `_acme-challenge.${name}.${env.ZONE}`;
}

/** Clears a name's challenge records (best effort; used on release and device removal). */
export async function clearNameRecords(env: Env, deps: Deps, name: string): Promise<void> {
  const dns = deps.dns(env);
  if (!dns) return;
  await dns.clearTxt(acmeFqdn(env, name)).catch(() => undefined);
}

export async function listNames(request: Request, env: Env, deps: Deps): Promise<Response> {
  const p = await principal(request, env, nowSecs(deps.nowMs()));
  const { results } = await env.DB.prepare("SELECT * FROM names WHERE account_id = ? ORDER BY name")
    .bind(p.accountId)
    .all<NameRow>();
  return json(200, { names: results.map((n) => nameJson(env, n)) });
}

/** `PUT /v1/names/{name}` with the main server's device token. */
export async function reserveName(request: Request, env: Env, deps: Deps, name: string): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  const device = requireDevice(await principal(request, env, now, { deviceOnly: true }));
  if (!isValidName(name)) throw ApiError.badRequest("malformed or reserved name");
  if (device.role !== "main_server") throw ApiError.forbidden("only a main server can hold a name");
  const inserted = await env.DB.prepare(
    "INSERT INTO names (name, endpoint_id, account_id, created_at) VALUES (?, ?, ?, ?) ON CONFLICT (name) DO NOTHING",
  )
    .bind(name, device.endpointId, device.accountId, now)
    .run();
  const row = await env.DB.prepare("SELECT * FROM names WHERE name = ?").bind(name).first<NameRow>();
  if (!row || row.endpoint_id !== device.endpointId) throw ApiError.conflict("taken");
  return json(inserted.meta.changes === 1 ? 201 : 200, nameJson(env, row));
}

/** The name if the caller may manage it: its device, or (when `accountMay`) its account. */
async function ownedName(env: Env, p: Principal, name: string, accountMay: boolean): Promise<NameRow> {
  const row = await env.DB.prepare("SELECT * FROM names WHERE name = ?").bind(name).first<NameRow>();
  const allowed =
    row !== null &&
    (p.kind === "device" && p.endpointId === row.endpoint_id
      ? true
      : accountMay && p.accountId === row.account_id && (p.kind === "session" || p.role === "main_server"));
  if (!allowed || !row) throw ApiError.notFound();
  return row;
}

export async function releaseName(request: Request, env: Env, deps: Deps, name: string): Promise<Response> {
  const p = await principal(request, env, nowSecs(deps.nowMs()));
  const row = await ownedName(env, p, name, true);
  await env.DB.prepare("DELETE FROM names WHERE name = ?").bind(row.name).run();
  deps.waitUntil(clearNameRecords(env, deps, row.name));
  return noContent();
}

function provider(env: Env, deps: Deps) {
  const dns = deps.dns(env);
  if (!dns) throw new ApiError(502, "no DNS provider is configured");
  return dns;
}

function dnsFailure(err: unknown): never {
  if (err instanceof DnsError) throw new ApiError(502, err.message);
  throw err;
}

/** `PUT /v1/names/{name}/acme-challenge` `{"values": ["<43 base64url>", ...]}` */
export async function setAcmeChallenge(request: Request, env: Env, deps: Deps, name: string): Promise<Response> {
  const device = requireDevice(await principal(request, env, nowSecs(deps.nowMs()), { deviceOnly: true }));
  const row = await ownedName(env, device, name, false);
  const body = await readJson<{ values?: unknown }>(request);
  const values = body.values;
  if (
    !Array.isArray(values) ||
    values.length < 1 ||
    values.length > 4 ||
    !values.every((v) => typeof v === "string" && ACME_VALUE_RE.test(v))
  ) {
    throw ApiError.badRequest("values must be 1 to 4 base64url SHA-256 digests");
  }
  await provider(env, deps).setTxt(acmeFqdn(env, row.name), values as string[]).catch(dnsFailure);
  return noContent();
}

export async function clearAcmeChallenge(request: Request, env: Env, deps: Deps, name: string): Promise<Response> {
  const device = requireDevice(await principal(request, env, nowSecs(deps.nowMs()), { deviceOnly: true }));
  const row = await ownedName(env, device, name, false);
  await provider(env, deps).clearTxt(acmeFqdn(env, row.name)).catch(dnsFailure);
  return noContent();
}
