// Address directory (SPEC FR-H2): iroh's pkarr relay protocol plus a JSON view.

import { accountDevice } from "./devices";
import type { Deps, Env } from "./env";
import { ApiError, json, noContent, principal } from "./http";
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
  // Atomic "only if newer": the upsert's WHERE leaves an older or equal packet unchanged.
  const result = await env.DB.prepare(
    `INSERT INTO records (endpoint_id, payload, timestamp_us) VALUES (?, ?, ?)
     ON CONFLICT (endpoint_id) DO UPDATE SET payload = excluded.payload, timestamp_us = excluded.timestamp_us
     WHERE excluded.timestamp_us > records.timestamp_us`,
  )
    .bind(endpointId, base64url(payload), ts)
    .run();
  if (result.meta.changes !== 1) throw ApiError.conflict("not newer than the stored packet");
  await env.DB.prepare("UPDATE devices SET last_seen = ? WHERE endpoint_id = ?")
    .bind(nowSecs(deps.nowMs()), endpointId)
    .run();
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

/** `GET /pkarr/{z32}`: same-account callers only; the token may be `?token=` for iroh's resolver. */
export async function pkarrGet(request: Request, env: Env, deps: Deps, z32: string): Promise<Response> {
  const key = z32DecodeKey(z32);
  if (!key) throw ApiError.badRequest("malformed key");
  const p = await principal(request, env, nowSecs(deps.nowMs()), { allowQuery: true });
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

/** `GET /v1/devices/{endpoint_id}/addresses` */
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
