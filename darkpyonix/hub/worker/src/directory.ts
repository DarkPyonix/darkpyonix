// Address directory (SPEC FR-H2): iroh's pkarr relay protocol plus a JSON view.

import { accountDevice, MAX_WAIT_SECS, WAIT_POLL_MS } from "./devices";
import type { Deps, Env } from "./env";
import { ApiError, type Principal, json, noContent, principal, resolvePrincipal } from "./http";
import { endpointAddresses, parseDnsAnswers, splitRelayPayload, verifyRelayPayload, z32DecodeKey, z32Encode } from "./pkarr";
import { base64url, fromBase64url, fromHex, nowSecs, toHex } from "./util";

async function limited(env: Env, key: string): Promise<void> {
  if (!env.WRITE_LIMITER) return;
  const { success } = await env.WRITE_LIMITER.limit({ key: `pkarr:${key}` });
  if (!success) throw new ApiError(429, "publishing too often");
}

/** `PUT /pkarr/{z32}`: body is the relay payload `sig(64) || ts_us(8) || dns`. */
export async function pkarrPut(request: Request, env: Env, deps: Deps, z32: string): Promise<Response> {
  const key = z32DecodeKey(z32);
  if (!key) throw ApiError.badRequest("malformed key");
  const payload = new Uint8Array(await request.arrayBuffer());
  const verified = await verifyRelayPayload(key, payload);
  if (!verified) throw ApiError.badRequest("malformed payload or bad signature");
  const endpointId = toHex(key);
  const device = await env.DB.prepare("SELECT 1 AS x FROM devices WHERE endpoint_id = ? AND revoked_at IS NULL")
    .bind(endpointId)
    .first();
  if (!device) throw ApiError.forbidden("not a registered device");
  await limited(env, endpointId);
  if (verified.timestampUs > BigInt(Number.MAX_SAFE_INTEGER)) throw ApiError.badRequest("timestamp out of range");
  const ts = Number(verified.timestampUs);
  // Atomic "only if newer": the upsert's WHERE leaves an older or equal packet unchanged. The
  // account's records version moves in the same batch, only when the upsert changed a row.
  const [result] = await env.DB.batch([
    env.DB.prepare(
      `INSERT INTO records (endpoint_id, payload, timestamp_us) VALUES (?, ?, ?)
       ON CONFLICT (endpoint_id) DO UPDATE SET payload = excluded.payload, timestamp_us = excluded.timestamp_us
       WHERE excluded.timestamp_us > records.timestamp_us`,
    ).bind(endpointId, base64url(payload), ts),
    env.DB.prepare(
      `UPDATE accounts SET records_version = records_version + 1
       WHERE changes() = 1 AND account_id = (SELECT account_id FROM devices WHERE endpoint_id = ?)`,
    ).bind(endpointId),
  ]);
  if (result.meta.changes !== 1) throw ApiError.conflict("not newer than the stored packet");
  // A fresh record makes the device online (FR-H1); only the flip moves the list version (FR-H9).
  await env.DB.batch([
    env.DB.prepare(
      `UPDATE accounts SET devices_version = devices_version + 1 WHERE account_id =
         (SELECT account_id FROM devices WHERE endpoint_id = ? AND revoked_at IS NULL AND online = 0)`,
    ).bind(endpointId),
    env.DB.prepare("UPDATE devices SET online = 1, last_seen = ? WHERE endpoint_id = ? AND revoked_at IS NULL").bind(
      nowSecs(deps.nowMs()),
      endpointId,
    ),
  ]);
  return noContent();
}

async function storedPayload(env: Env, endpointId: string): Promise<Uint8Array> {
  const row = await env.DB.prepare("SELECT payload FROM records WHERE endpoint_id = ?")
    .bind(endpointId)
    .first<{ payload: string }>();
  const payload = row ? fromBase64url(row.payload) : null;
  if (!payload) throw ApiError.notFound("nothing published");
  return payload;
}

/** `GET /pkarr/{z32}`: same-account callers only; iroh's resolver sends a resolve token as `?token=`. */
export async function pkarrGet(request: Request, env: Env, deps: Deps, z32: string): Promise<Response> {
  const key = z32DecodeKey(z32);
  if (!key) throw ApiError.badRequest("malformed key");
  const p = await resolvePrincipal(request, env, nowSecs(deps.nowMs()));
  const endpointId = toHex(key);
  await accountDevice(env, p, endpointId);
  const payload = await storedPayload(env, endpointId);
  return new Response(payload, { status: 200, headers: { "content-type": "application/octet-stream" } });
}

/** The decoded addresses of a stored record (the payload was verified when it was stored). */
export async function storedAddresses(env: Env, endpointId: string) {
  const payload = await storedPayload(env, endpointId);
  const parts = splitRelayPayload(payload);
  const key = fromHex(endpointId, 64);
  const answers = parts ? parseDnsAnswers(parts.dns) : null;
  if (!parts || !key || !answers) throw ApiError.notFound("nothing published");
  return { payload, timestampUs: parts.timestampUs, ...endpointAddresses(z32Encode(key), answers) };
}

/** `GET /devices/{endpoint_id}/addresses` */
export async function deviceAddresses(request: Request, env: Env, deps: Deps, endpointId: string): Promise<Response> {
  const p = await principal(request, env, nowSecs(deps.nowMs()));
  await accountDevice(env, p, endpointId);
  const record = await storedAddresses(env, endpointId);
  return json(200, {
    endpoint_id: endpointId,
    relay_urls: record.relayUrls,
    direct_addresses: record.directAddresses,
    published_at_us: Number(record.timestampUs),
    signed_packet: base64url(record.payload),
  });
}

// ---------------------------------------------------------------- watch (FR-H2, #132)

/** Most keys in one `GET /pkarr/watch` (advertised in `/config` `pkarr_watch`). */
export const WATCH_MAX_KEYS = 32;
/** Longest `wait` of a watch, seconds. */
export const WATCH_MAX_WAIT_SECS = MAX_WAIT_SECS;

function standardBase64(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}

interface WatchParams {
  keys: string[];
  endpointIds: string[];
  since: number[];
  wait: number;
}

function watchParams(url: URL): WatchParams {
  const rawKeys = url.searchParams.get("keys");
  const rawSince = url.searchParams.get("since");
  const rawWait = url.searchParams.get("wait");
  if (!rawKeys || rawSince === null) throw ApiError.badRequest("keys and since are required");
  const keys = rawKeys.split(",");
  const since = rawSince.split(",");
  if (keys.length > WATCH_MAX_KEYS) throw ApiError.badRequest(`at most ${WATCH_MAX_KEYS} keys`);
  if (keys.length !== since.length) throw ApiError.badRequest("keys and since must have the same length");
  if (new Set(keys).size !== keys.length) throw ApiError.badRequest("duplicate keys");
  const endpointIds = keys.map((k) => {
    const bytes = z32DecodeKey(k);
    if (!bytes) throw ApiError.badRequest("malformed key");
    return toHex(bytes);
  });
  const sinceNums = since.map((v) => {
    const n = /^\d{1,16}$/.test(v) ? Number(v) : -1;
    if (n < 0 || n > Number.MAX_SAFE_INTEGER) throw ApiError.badRequest("since must be timestamps in microseconds");
    return n;
  });
  const wait = rawWait === null ? 0 : /^\d{1,2}$/.test(rawWait) ? Number(rawWait) : -1;
  if (wait < 0 || wait > WATCH_MAX_WAIT_SECS) throw ApiError.badRequest(`wait must be an integer from 0 to ${WATCH_MAX_WAIT_SECS}`);
  return { keys, endpointIds, since: sinceNums, wait };
}

/** One row per tick: the account's records version, and whether the calling device is still live. */
async function watchTick(env: Env, p: Principal): Promise<number> {
  if (p.kind === "device") {
    const row = await env.DB.prepare(
      `SELECT a.records_version AS v, d.revoked_at AS revoked FROM accounts a
       JOIN devices d ON d.endpoint_id = ? WHERE a.account_id = ?`,
    )
      .bind(p.endpointId, p.accountId)
      .first<{ v: number; revoked: number | null }>();
    if (!row || row.revoked !== null) throw ApiError.deviceRemoved();
    return row.v;
  }
  const row = await env.DB.prepare("SELECT records_version AS v FROM accounts WHERE account_id = ?")
    .bind(p.accountId)
    .first<{ v: number }>();
  return row?.v ?? 0;
}

type RecordRow = { endpoint_id: string; payload: string; timestamp_us: number };

function changedRecords(params: WatchParams, rows: RecordRow[]) {
  const byId = new Map(rows.map((r) => [r.endpoint_id, r]));
  const out: { key: string; timestamp: number; packet: string }[] = [];
  params.endpointIds.forEach((id, i) => {
    const row = byId.get(id);
    const payload = row ? fromBase64url(row.payload) : null;
    if (row && payload && row.timestamp_us > params.since[i]) {
      out.push({ key: params.keys[i], timestamp: row.timestamp_us, packet: standardBase64(payload) });
    }
  });
  return out;
}

/**
 * `GET /pkarr/watch?keys=&since=&wait=`: the listed records newer than their `since`, at once
 * or as soon as one changes within `wait`. While waiting, only the account's version row is read.
 */
export async function pkarrWatch(request: Request, env: Env, deps: Deps): Promise<Response> {
  const p = await principal(request, env, nowSecs(deps.nowMs()));
  // Malformed requests are refused before they count against the limit.
  const params = watchParams(new URL(request.url));
  if (env.WATCH_LIMITER) {
    const key = p.kind === "device" ? `watch:${p.endpointId}` : `watch:session:${p.accountId}`;
    if (!(await env.WATCH_LIMITER.limit({ key })).success) throw ApiError.tooMany("watching too often");
  }
  const marks = params.endpointIds.map(() => "?").join(",");
  const recordsStmt = () =>
    env.DB.prepare(`SELECT endpoint_id, payload, timestamp_us FROM records WHERE endpoint_id IN (${marks})`).bind(
      ...params.endpointIds,
    );
  // One snapshot: the version, the caller's own devices among the keys, and their records.
  const [versionRes, ownRes, recordsRes] = await env.DB.batch([
    env.DB.prepare("SELECT records_version AS v FROM accounts WHERE account_id = ?").bind(p.accountId),
    env.DB.prepare(
      `SELECT endpoint_id FROM devices WHERE account_id = ? AND revoked_at IS NULL AND endpoint_id IN (${marks})`,
    ).bind(p.accountId, ...params.endpointIds),
    recordsStmt(),
  ]);
  const own = new Set((ownRes.results as { endpoint_id: string }[]).map((r) => r.endpoint_id));
  const missing = params.keys.filter((_, i) => !own.has(params.endpointIds[i]));
  if (missing.length > 0) return json(404, { error: "not_found", keys: missing });
  let version = (versionRes.results[0] as { v: number } | undefined)?.v ?? 0;
  let records = changedRecords(params, recordsRes.results as RecordRow[]);
  for (let round = 0; round < Math.ceil((params.wait * 1000) / WAIT_POLL_MS) && records.length === 0; round++) {
    await deps.sleep(WAIT_POLL_MS);
    const now = await watchTick(env, p);
    if (now === version) continue;
    version = now;
    records = changedRecords(params, (await recordsStmt().all<RecordRow>()).results);
  }
  if (records.length === 0) return noContent();
  return json(200, { records }, { "cache-control": "private, no-cache" });
}
