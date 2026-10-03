// Accounts' devices (SPEC FR-H1): device links, the device list, removal.
//
// A device joins an account through a device link, the shape of OAuth device authorization
// (RFC 8628) with key possession added:
//   1. the device asks: POST /v1/device-links {endpoint_id, name, role} -> link_id, user_code, challenge
//   2. a person signed in with GitHub (or the account's main server) approves the user code:
//      POST /v1/link-codes/{user_code} {"approve": true}
//   3. the device claims its token, signing the challenge with its iroh secret key:
//      POST /v1/device-links/{link_id}/token {"signature": hex(sign("darkpyonix-hub/v2/link\n<link_id>\n<challenge>"))}

import { verifyEd25519 } from "./ed25519";
import type { Deps, Env } from "./env";
import {
  ApiError,
  type Principal,
  json,
  noContent,
  principal,
  RESOLVE_TOKEN_PREFIX,
  readJson,
  requireAccountAdmin,
  requireDevice,
} from "./http";
import { clearNameRecords } from "./names";
import {
  ENDPOINT_ID_RE,
  fromHex,
  hashToken,
  newToken,
  newUserCode,
  normalizeUserCode,
  nowSecs,
  randomHex,
  utf8,
} from "./util";

/** How long a person has to approve and the device to claim. */
export const LINK_TTL_SECS = 900;
/** How often a device should poll for its token. */
export const LINK_POLL_INTERVAL_SECS = 5;

export interface DeviceRow {
  endpoint_id: string;
  account_id: string;
  name: string;
  role: string;
  created_at: number;
  last_seen: number | null;
  online: number;
}

export function deviceJson(d: DeviceRow) {
  return {
    endpoint_id: d.endpoint_id,
    name: d.name,
    role: d.role,
    created_at: d.created_at,
    last_seen: d.last_seen,
    online: d.online === 1,
  };
}

export function linkMessage(linkId: string, challenge: string): Uint8Array {
  return utf8(`darkpyonix-hub/v2/link\n${linkId}\n${challenge}`);
}

function validRole(role: unknown): role is "main_server" | "computer" {
  return role === "main_server" || role === "computer";
}

function validDeviceName(name: unknown): name is string {
  return typeof name === "string" && name.length >= 1 && [...name].length <= 64;
}

async function limited(env: Env, key: string): Promise<void> {
  if (!env.WRITE_LIMITER) return;
  const { success } = await env.WRITE_LIMITER.limit({ key });
  if (!success) throw new ApiError(429, "too many requests");
}

function clientIp(request: Request): string {
  return request.headers.get("cf-connecting-ip") ?? "unknown";
}

// ---------------------------------------------------------------- /v1/me

export async function me(request: Request, env: Env, deps: Deps): Promise<Response> {
  const p = await principal(request, env, nowSecs(deps.nowMs()));
  const account = await env.DB.prepare("SELECT account_id, github_login FROM accounts WHERE account_id = ?")
    .bind(p.accountId)
    .first<{ account_id: string; github_login: string }>();
  if (!account) throw ApiError.unauthorized();
  return json(200, {
    account_id: account.account_id,
    github_login: account.github_login,
    via: p.kind,
    endpoint_id: p.kind === "device" ? p.endpointId : null,
  });
}

// ---------------------------------------------------------------- device links

export async function createLink(request: Request, env: Env, deps: Deps): Promise<Response> {
  await limited(env, `device-link:${clientIp(request)}`);
  const body = await readJson<{ endpoint_id?: unknown; name?: unknown; role?: unknown }>(request);
  const endpointId = body.endpoint_id;
  if (typeof endpointId !== "string" || !ENDPOINT_ID_RE.test(endpointId)) {
    throw ApiError.badRequest("endpoint_id must be 64 lowercase hex characters");
  }
  if (!validDeviceName(body.name)) throw ApiError.badRequest("name must be 1 to 64 characters");
  if (!validRole(body.role)) throw ApiError.badRequest("role must be main_server or computer");
  const taken = await env.DB.prepare("SELECT 1 AS x FROM devices WHERE endpoint_id = ?").bind(endpointId).first();
  if (taken) throw ApiError.conflict("endpoint id already registered (removed keys are not reused)");

  const now = nowSecs(deps.nowMs());
  const linkId = `l_${randomHex(16)}`;
  const challenge = randomHex(32);
  const expiresAt = now + LINK_TTL_SECS;
  let userCode = "";
  for (let attempt = 0; ; attempt++) {
    userCode = newUserCode();
    const result = await env.DB.prepare(
      `INSERT INTO device_links (link_id, user_code, endpoint_id, name, role, challenge, status, created_at, expires_at)
       VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?) ON CONFLICT (user_code) DO NOTHING`,
    )
      .bind(linkId, userCode, endpointId, body.name, body.role, challenge, now, expiresAt)
      .run();
    if (result.meta.changes === 1) break;
    if (attempt >= 4) throw new ApiError(503, "could not allocate a user code");
  }
  const verification = `${env.PUBLIC_URL}/link`;
  return json(201, {
    link_id: linkId,
    user_code: userCode,
    verification_uri: verification,
    verification_uri_complete: `${verification}?code=${userCode}`,
    challenge,
    interval: LINK_POLL_INTERVAL_SECS,
    expires_at: expiresAt,
  });
}

interface LinkRow {
  link_id: string;
  user_code: string;
  endpoint_id: string;
  name: string;
  role: string;
  challenge: string;
  status: string;
  account_id: string | null;
  expires_at: number;
}

async function pendingLinkByCode(env: Env, rawCode: string, now: number): Promise<LinkRow> {
  const code = normalizeUserCode(rawCode);
  if (!code) throw ApiError.notFound("unknown or expired code");
  const link = await env.DB.prepare("SELECT * FROM device_links WHERE user_code = ?").bind(code).first<LinkRow>();
  if (!link || link.status !== "pending" || link.expires_at < now) {
    throw ApiError.notFound("unknown or expired code");
  }
  return link;
}

/** `GET /v1/link-codes/{user_code}`: what the approver is about to let in. */
export async function getLinkCode(request: Request, env: Env, deps: Deps, code: string): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  const accountId = requireAccountAdmin(await principal(request, env, now));
  // User codes are short; an account may not keep guessing them.
  await limited(env, `link-code:${accountId}`);
  const link = await pendingLinkByCode(env, code, now);
  return json(200, {
    user_code: link.user_code,
    endpoint_id: link.endpoint_id,
    name: link.name,
    role: link.role,
    expires_at: link.expires_at,
  });
}

/** `POST /v1/link-codes/{user_code}` `{"approve": bool}` */
export async function decideLinkCode(request: Request, env: Env, deps: Deps, code: string): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  const p = await principal(request, env, now);
  const accountId = requireAccountAdmin(p);
  await limited(env, `link-code:${accountId}`);
  const body = await readJson<{ approve?: unknown }>(request);
  if (typeof body.approve !== "boolean") throw ApiError.badRequest("approve must be a boolean");
  const link = await pendingLinkByCode(env, code, now);
  // A leaked main server token must not mint more devices with account rights.
  if (body.approve && link.role === "main_server" && p.kind !== "session") {
    throw ApiError.forbidden("only a signed-in session may approve a main_server link");
  }
  const result = await env.DB.prepare(
    "UPDATE device_links SET status = ?, account_id = ? WHERE link_id = ? AND status = 'pending'",
  )
    .bind(body.approve ? "approved" : "denied", body.approve ? accountId : null, link.link_id)
    .run();
  if (result.meta.changes !== 1) throw ApiError.notFound("unknown or expired code");
  return noContent();
}

/** `POST /v1/device-links/{link_id}/token` `{"signature": "<128 hex>"}` */
export async function claimLink(request: Request, env: Env, deps: Deps, linkId: string): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  const body = await readJson<{ signature?: unknown }>(request);
  const link = /^l_[0-9a-f]{32}$/.test(linkId)
    ? await env.DB.prepare("SELECT * FROM device_links WHERE link_id = ?").bind(linkId).first<LinkRow>()
    : null;
  if (!link || link.expires_at < now || link.status === "claimed") {
    throw ApiError.notFound("unknown, expired or already claimed link");
  }
  const signature = typeof body.signature === "string" ? fromHex(body.signature, 128) : null;
  const key = fromHex(link.endpoint_id, 64);
  if (!signature || !key) throw ApiError.badRequest("signature must be 128 lowercase hex characters");
  if (!(await verifyEd25519(key, linkMessage(link.link_id, link.challenge), signature))) {
    throw ApiError.badRequest("signature does not verify");
  }
  if (link.status === "pending") return json(202, { status: "pending" });
  if (link.status === "denied") throw ApiError.forbidden("the link was denied");

  // approved: claim exactly once.
  const claimed = await env.DB.prepare(
    "UPDATE device_links SET status = 'claimed' WHERE link_id = ? AND status = 'approved'",
  )
    .bind(link.link_id)
    .run();
  if (claimed.meta.changes !== 1 || !link.account_id) throw ApiError.notFound("already claimed");

  const token = newToken("dpd_");
  const resolveToken = newToken(RESOLVE_TOKEN_PREFIX);
  const device: DeviceRow = {
    endpoint_id: link.endpoint_id,
    account_id: link.account_id,
    name: link.name,
    role: link.role,
    created_at: now,
    last_seen: null,
    online: 0,
  };
  try {
    await env.DB.prepare(
      `INSERT INTO devices (endpoint_id, account_id, name, role, token_hash, resolve_token_hash, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
    )
      .bind(device.endpoint_id, device.account_id, device.name, device.role, await hashToken(token), await hashToken(resolveToken), now)
      .run();
  } catch {
    throw ApiError.conflict("endpoint id already registered");
  }
  return json(201, { device: deviceJson(device), device_token: token, resolve_token: resolveToken });
}

/** `POST /v1/me/resolve-token`: a new read-only resolve token; the old one stops working (NFR-H2). */
export async function rotateResolveToken(request: Request, env: Env, deps: Deps): Promise<Response> {
  const device = requireDevice(await principal(request, env, nowSecs(deps.nowMs()), { deviceOnly: true }));
  const resolveToken = newToken(RESOLVE_TOKEN_PREFIX);
  await env.DB.prepare("UPDATE devices SET resolve_token_hash = ? WHERE endpoint_id = ? AND revoked_at IS NULL")
    .bind(await hashToken(resolveToken), device.endpointId)
    .run();
  return json(201, { resolve_token: resolveToken });
}

// ---------------------------------------------------------------- devices

export async function listDevices(request: Request, env: Env, deps: Deps): Promise<Response> {
  const p = await principal(request, env, nowSecs(deps.nowMs()));
  const { results } = await env.DB.prepare(
    "SELECT * FROM devices WHERE account_id = ? AND revoked_at IS NULL ORDER BY created_at, endpoint_id",
  )
    .bind(p.accountId)
    .all<DeviceRow>();
  return json(200, { devices: results.map(deviceJson) });
}

export async function accountDevice(env: Env, p: Principal, endpointId: string): Promise<DeviceRow> {
  const row = await env.DB.prepare(
    "SELECT * FROM devices WHERE account_id = ? AND endpoint_id = ? AND revoked_at IS NULL",
  )
    .bind(p.accountId, endpointId)
    .first<DeviceRow>();
  if (!row) throw ApiError.notFound();
  return row;
}

export async function getDevice(request: Request, env: Env, deps: Deps, endpointId: string): Promise<Response> {
  const p = await principal(request, env, nowSecs(deps.nowMs()));
  return json(200, deviceJson(await accountDevice(env, p, endpointId)));
}

/** Tells the relay host to drop a removed device's connections (best effort; SPEC FR-H3). */
async function disconnectFromRelay(env: Env, deps: Deps, endpointId: string): Promise<void> {
  if (!env.RELAY_ADMIN_URL || !env.RELAY_SHARED_SECRET) return;
  await deps
    .fetch(`${env.RELAY_ADMIN_URL}/admin/v1/disconnect`, {
      method: "POST",
      headers: { authorization: `Bearer ${env.RELAY_SHARED_SECRET}`, "content-type": "application/json" },
      body: JSON.stringify({ endpoint_id: endpointId }),
    })
    .catch(() => undefined);
}

export async function removeDevice(request: Request, env: Env, deps: Deps, endpointId: string): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  const p = await principal(request, env, now);
  // A device may always leave by itself; removing another device needs account rights.
  const accountId = p.kind === "device" && p.endpointId === endpointId ? p.accountId : requireAccountAdmin(p);
  const { results: names } = await env.DB.prepare("SELECT name FROM names WHERE endpoint_id = ?")
    .bind(endpointId)
    .all<{ name: string }>();
  const revoked = await env.DB.prepare(
    "UPDATE devices SET revoked_at = ?, online = 0 WHERE account_id = ? AND endpoint_id = ? AND revoked_at IS NULL",
  )
    .bind(now, accountId, endpointId)
    .run();
  if (revoked.meta.changes !== 1) throw ApiError.notFound();
  await env.DB.batch([
    env.DB.prepare("DELETE FROM names WHERE endpoint_id = ?").bind(endpointId),
    env.DB.prepare("DELETE FROM shares WHERE endpoint_id = ?").bind(endpointId),
    env.DB.prepare("DELETE FROM records WHERE endpoint_id = ?").bind(endpointId),
  ]);
  for (const { name } of names) deps.waitUntil(clearNameRecords(env, deps, name));
  deps.waitUntil(disconnectFromRelay(env, deps, endpointId));
  return noContent();
}
