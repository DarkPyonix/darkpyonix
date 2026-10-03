// What the relay host asks the hub (SPEC FR-H3). The iroh relay itself does not run on
// Workers; the relay host calls these with the shared secret during the relay handshake.

import type { Deps, Env } from "./env";
import { ApiError, json, noContent, readJson } from "./http";
import { ENDPOINT_ID_RE, ctEq, hashToken, nowSecs } from "./util";

/** How long the relay may cache an "allow" for a registered device before asking again. */
export const ADMIT_CACHE_SECS = 60;

function requireRelay(request: Request, env: Env): void {
  const header = request.headers.get("authorization") ?? "";
  const given = header.toLowerCase().startsWith("bearer ") ? header.slice(7).trim() : "";
  if (!env.RELAY_SHARED_SECRET || !given || !ctEq(given, env.RELAY_SHARED_SECRET)) {
    throw ApiError.unauthorized("relay secret missing or wrong");
  }
}

/** `POST /internal/v1/relay/admit` `{"endpoint_id": hex, "token": string|null}` */
export async function admit(request: Request, env: Env, deps: Deps): Promise<Response> {
  requireRelay(request, env);
  const body = await readJson<{ endpoint_id?: unknown; token?: unknown }>(request);
  const endpointId = body.endpoint_id;
  if (typeof endpointId !== "string" || !ENDPOINT_ID_RE.test(endpointId)) {
    throw ApiError.badRequest("endpoint_id must be 64 lowercase hex characters");
  }
  const now = nowSecs(deps.nowMs());
  const device = await env.DB.prepare("SELECT 1 AS x FROM devices WHERE endpoint_id = ? AND revoked_at IS NULL")
    .bind(endpointId)
    .first();
  if (device) {
    await env.DB.prepare("UPDATE devices SET last_seen = ? WHERE endpoint_id = ?").bind(now, endpointId).run();
    return json(200, { allow: true, kind: "device", cache_secs: ADMIT_CACHE_SECS });
  }
  if (typeof body.token === "string" && body.token) {
    const pass = await env.DB.prepare(
      `SELECT p.expires_at FROM relay_passes p JOIN shares s ON s.share_id = p.share_id
       WHERE p.token_hash = ? AND p.expires_at > ?`,
    )
      .bind(await hashToken(body.token), now)
      .first<{ expires_at: number }>();
    if (pass) return json(200, { allow: true, kind: "guest", cache_secs: 0 });
  }
  return json(200, { allow: false, reason: "not a registered device and no valid relay pass", cache_secs: 0 });
}

/** `POST /internal/v1/relay/presence` `{"endpoint_id": hex, "online": bool}` */
export async function presence(request: Request, env: Env, deps: Deps): Promise<Response> {
  requireRelay(request, env);
  const body = await readJson<{ endpoint_id?: unknown; online?: unknown }>(request);
  if (typeof body.endpoint_id !== "string" || !ENDPOINT_ID_RE.test(body.endpoint_id) || typeof body.online !== "boolean") {
    throw ApiError.badRequest("endpoint_id (hex) and online (boolean) are required");
  }
  const online = body.online ? 1 : 0;
  // One transaction: bump the account's device list version (FR-H9) only if `online` changes.
  await env.DB.batch([
    env.DB.prepare(
      `UPDATE accounts SET devices_version = devices_version + 1 WHERE account_id =
         (SELECT account_id FROM devices WHERE endpoint_id = ? AND revoked_at IS NULL AND online != ?)`,
    ).bind(body.endpoint_id, online),
    env.DB.prepare("UPDATE devices SET online = ?, last_seen = ? WHERE endpoint_id = ? AND revoked_at IS NULL").bind(
      online,
      nowSecs(deps.nowMs()),
      body.endpoint_id,
    ),
  ]);
  return noContent();
}
