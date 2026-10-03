// Accounts' devices (SPEC FR-H1): device links, the device list, removal.
//
// A device joins an account through a device link, the shape of OAuth device authorization
// (RFC 8628) with key possession added:
//   1. the device asks: POST /device-links {endpoint_id, name, role} -> link_id, user_code, challenge
//   2. a person signed in with GitHub (or the account's main server) approves the user code:
//      POST /link-codes/{user_code} {"approve": true}
//      (an account has one main server, Ember INTENT D3: a second main_server link is approved
//      only as an explicit replacement, {"approve": true, "replace": "<current endpoint_id>"})
//   3. the device claims its token, signing the challenge with its iroh secret key:
//      POST /device-links/{link_id}/token {"signature": hex(sign("darkpyonix-hub/v2/link\n<link_id>\n<challenge>"))}

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
  /** JSON of a validated DeviceApp, or null (FR-H10). */
  app: string | null;
}

/** What a device says it runs (FR-H10); self-reported, a hint only. */
export interface DeviceApp {
  kind: string;
  version: string;
  services: string[];
}

export function deviceJson(d: DeviceRow) {
  return {
    endpoint_id: d.endpoint_id,
    name: d.name,
    role: d.role,
    created_at: d.created_at,
    last_seen: d.last_seen,
    online: d.online === 1,
    app: d.app ? (JSON.parse(d.app) as DeviceApp) : null,
  };
}

const APP_NAME_RE = /^[a-z][a-z0-9-]{0,31}$/;
const APP_VERSION_RE = /^[0-9A-Za-z][0-9A-Za-z.+-]{0,31}$/;
const MAX_APP_SERVICES = 16;

/** A DeviceApp from untrusted JSON, or null if it is not one exactly. */
export function parseDeviceApp(value: unknown): DeviceApp | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
  const v = value as Record<string, unknown>;
  if (Object.keys(v).some((k) => k !== "kind" && k !== "version" && k !== "services")) return null;
  if (typeof v.kind !== "string" || !APP_NAME_RE.test(v.kind)) return null;
  if (typeof v.version !== "string" || !APP_VERSION_RE.test(v.version)) return null;
  const services = v.services === undefined ? [] : v.services;
  if (!Array.isArray(services) || services.length > MAX_APP_SERVICES) return null;
  if (!services.every((x) => typeof x === "string" && APP_NAME_RE.test(x))) return null;
  if (new Set(services).size !== services.length) return null;
  return { kind: v.kind, version: v.version, services: services as string[] };
}

export function linkMessage(linkId: string, challenge: string): Uint8Array {
  return utf8(`darkpyonix-hub/v2/link\n${linkId}\n${challenge}`);
}

/** Device roles (SPEC FR-H1). `client` only connects to other devices (provisional). */
export const ROLES = ["main_server", "computer", "client"] as const;
export type Role = (typeof ROLES)[number];

function validRole(role: unknown): role is Role {
  return (ROLES as readonly unknown[]).includes(role);
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

// ---------------------------------------------------------------- device list version (FR-H9)

/** Longest `wait` of a long-poll on `GET /devices`. */
export const MAX_WAIT_SECS = 25;
/** How often a long-poll re-reads the account's version. */
export const WAIT_POLL_MS = 2000;

/** Marks a change visible in the account's device list (not `last_seen` alone). */
export function bumpDevicesVersion(env: Env, accountId: string): D1PreparedStatement {
  return env.DB.prepare("UPDATE accounts SET devices_version = devices_version + 1 WHERE account_id = ?").bind(accountId);
}

async function devicesVersion(env: Env, accountId: string): Promise<number> {
  const row = await env.DB.prepare("SELECT devices_version FROM accounts WHERE account_id = ?")
    .bind(accountId)
    .first<{ devices_version: number }>();
  return row?.devices_version ?? 0;
}

function etagFor(version: number): string {
  return `W/"v${version}"`;
}

/** Weak comparison (RFC 9110 §13.1.2): `W/` is ignored; `*` matches. */
function ifNoneMatches(header: string | null, etag: string): boolean {
  if (!header) return false;
  const opaque = (tag: string) => tag.trim().replace(/^W\//, "");
  return header.split(",").some((tag) => tag.trim() === "*" || opaque(tag) === opaque(etag));
}

// ---------------------------------------------------------------- /me

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
  if (!validRole(body.role)) throw ApiError.badRequest("role must be main_server, computer or client");
  const now = nowSecs(deps.nowMs());
  const taken = await env.DB.prepare("SELECT revoked_at, readmit_until FROM devices WHERE endpoint_id = ?")
    .bind(endpointId)
    .first<{ revoked_at: number | null; readmit_until: number | null }>();
  // A removed key comes back only while its owner has re-admitted it (FR-H11).
  const readmitted = taken !== null && taken.revoked_at !== null && (taken.readmit_until ?? 0) > now;
  if (taken && !readmitted) {
    throw ApiError.conflict("endpoint id already registered, or removed and not re-admitted");
  }

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
  /** The main server this main_server link was approved to replace (FR-H1). */
  replace_endpoint_id: string | null;
}

/** The account's one active main server (FR-H1), if any. */
async function currentMainServer(env: Env, accountId: string): Promise<{ endpoint_id: string; name: string } | null> {
  return env.DB.prepare(
    "SELECT endpoint_id, name FROM devices WHERE account_id = ? AND role = 'main_server' AND revoked_at IS NULL",
  )
    .bind(accountId)
    .first<{ endpoint_id: string; name: string }>();
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

/** `GET /link-codes/{user_code}`: what the approver is about to let in. */
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
    // What approving a main_server link would replace (FR-H1).
    current_main_server: link.role === "main_server" ? await currentMainServer(env, accountId) : null,
  });
}

/** `POST /link-codes/{user_code}` `{"approve": bool, "replace"?: "<endpoint_id>"}` */
export async function decideLinkCode(request: Request, env: Env, deps: Deps, code: string): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  const p = await principal(request, env, now);
  const accountId = requireAccountAdmin(p);
  await limited(env, `link-code:${accountId}`);
  const body = await readJson<{ approve?: unknown; replace?: unknown }>(request);
  if (typeof body.approve !== "boolean") throw ApiError.badRequest("approve must be a boolean");
  const replace = body.replace;
  if (replace !== undefined && (typeof replace !== "string" || !ENDPOINT_ID_RE.test(replace))) {
    throw ApiError.badRequest("replace must be 64 lowercase hex characters");
  }
  const link = await pendingLinkByCode(env, code, now);
  if (replace !== undefined && !(body.approve && link.role === "main_server")) {
    throw ApiError.badRequest("replace only goes with approving a main_server link");
  }
  // A leaked main server token must not mint more devices with account rights.
  if (body.approve && link.role === "main_server" && p.kind !== "session") {
    throw ApiError.forbidden("only a signed-in session may approve a main_server link");
  }
  // A removed key's return is the owner's call alone (FR-H11).
  if (body.approve) {
    const known = await env.DB.prepare("SELECT account_id FROM devices WHERE endpoint_id = ?")
      .bind(link.endpoint_id)
      .first<{ account_id: string }>();
    if (known && (p.kind !== "session" || p.accountId !== known.account_id)) {
      throw ApiError.forbidden("only a signed-in session of its account may re-admit a removed key");
    }
  }
  // One main server per account (FR-H1, Ember INTENT D3): a second one only replaces the first.
  if (body.approve && link.role === "main_server") {
    const current = await currentMainServer(env, accountId);
    if (current && replace === undefined) {
      throw ApiError.conflict("the account has a main server; approve with \"replace\" to replace it", "main_server_exists");
    }
    if (replace !== undefined && current?.endpoint_id !== replace) {
      throw ApiError.conflict("replace does not name the account's current main server", "replace_mismatch");
    }
  }
  const result = await env.DB.prepare(
    `UPDATE device_links SET status = ?, account_id = ?, replace_endpoint_id = ?
     WHERE link_id = ? AND status = 'pending'`,
  )
    .bind(body.approve ? "approved" : "denied", body.approve ? accountId : null, replace ?? null, link.link_id)
    .run();
  if (result.meta.changes !== 1) throw ApiError.notFound("unknown or expired code");
  return noContent();
}

const LINK_ID_RE = /^l_[0-9a-f]{32}$/;

/** `GET /device-links/{link_id}`: the link's status, for a device that restarted while waiting. */
export async function getLink(_request: Request, env: Env, deps: Deps, linkId: string): Promise<Response> {
  const link = LINK_ID_RE.test(linkId)
    ? await env.DB.prepare("SELECT * FROM device_links WHERE link_id = ?").bind(linkId).first<LinkRow>()
    : null;
  if (!link) throw ApiError.notFound("unknown link");
  // A claimed link stays claimed; anything else past its expiry is expired.
  const expired = link.status !== "claimed" && link.expires_at < nowSecs(deps.nowMs());
  return json(200, {
    link_id: link.link_id,
    status: expired ? "expired" : link.status,
    endpoint_id: link.endpoint_id,
    name: link.name,
    role: link.role,
    user_code: link.user_code,
    verification_uri_complete: `${env.PUBLIC_URL}/link?code=${link.user_code}`,
    challenge: link.challenge,
    interval: LINK_POLL_INTERVAL_SECS,
    expires_at: link.expires_at,
  });
}

/** `POST /device-links/{link_id}/token` `{"signature": "<128 hex>"}` */
export async function claimLink(request: Request, env: Env, deps: Deps, linkId: string): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  const body = await readJson<{ signature?: unknown }>(request);
  const link = LINK_ID_RE.test(linkId)
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

  const accountId = link.account_id;
  const token = newToken("dpd_");
  const resolveToken = newToken(RESOLVE_TOKEN_PREFIX);
  const tokenHash = await hashToken(token);
  const resolveHash = await hashToken(resolveToken);
  const old = link.role === "main_server" ? link.replace_endpoint_id : null;
  const oldNames = old
    ? (await env.DB.prepare("SELECT name FROM names WHERE endpoint_id = ? AND account_id = ?").bind(old, accountId).all<{ name: string }>())
        .results
    : [];
  // One transaction: the replaced main server goes, its names move, the device joins (FR-H1).
  // The partial unique index refuses a second active main server however the links raced.
  const replacing = old
    ? [
        env.DB.prepare(
          `UPDATE devices SET revoked_at = ?, online = 0
           WHERE endpoint_id = ? AND account_id = ? AND role = 'main_server' AND revoked_at IS NULL`,
        ).bind(now, old, accountId),
      ]
    : [];
  const joining = [
    // A re-admitted removed key (FR-H11) gets its row back with new tokens; anything else is new.
    env.DB.prepare(
      `UPDATE devices SET revoked_at = NULL, readmit_until = NULL, name = ?, role = ?, token_hash = ?,
         resolve_token_hash = ?, last_seen = NULL, online = 0, app = NULL
       WHERE endpoint_id = ? AND account_id = ? AND revoked_at IS NOT NULL AND readmit_until IS NOT NULL`,
    ).bind(link.name, link.role, tokenHash, resolveHash, link.endpoint_id, accountId),
    env.DB.prepare(
      `INSERT INTO devices (endpoint_id, account_id, name, role, token_hash, resolve_token_hash, created_at)
       SELECT ?, ?, ?, ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM devices WHERE token_hash = ?)`,
    ).bind(link.endpoint_id, accountId, link.name, link.role, tokenHash, resolveHash, now, tokenHash),
  ];
  const moving = old
    ? [
        env.DB.prepare(
          `UPDATE names SET endpoint_id = ? WHERE endpoint_id = ? AND account_id = ?
             AND EXISTS (SELECT 1 FROM devices WHERE endpoint_id = ? AND revoked_at IS NOT NULL)`,
        ).bind(link.endpoint_id, old, accountId, old),
        env.DB.prepare(
          "DELETE FROM shares WHERE endpoint_id = ? AND EXISTS (SELECT 1 FROM devices WHERE endpoint_id = ? AND revoked_at IS NOT NULL)",
        ).bind(old, old),
        env.DB.prepare(
          "DELETE FROM records WHERE endpoint_id = ? AND EXISTS (SELECT 1 FROM devices WHERE endpoint_id = ? AND revoked_at IS NOT NULL)",
        ).bind(old, old),
      ]
    : [];
  let replaced = false;
  try {
    const results = await env.DB.batch([...replacing, ...joining, ...moving, bumpDevicesVersion(env, accountId)]);
    replaced = old !== null && results[0]!.meta.changes === 1;
  } catch (err) {
    if (String(err).includes("devices.account_id")) {
      // Another main server joined after this link was approved; the link is spent.
      await env.DB.prepare("UPDATE device_links SET status = 'denied' WHERE link_id = ?").bind(link.link_id).run();
      throw ApiError.conflict("the account has another main server now", "main_server_exists");
    }
    throw ApiError.conflict("endpoint id already registered");
  }
  if (replaced && old) {
    // The new main server proves the names again with its own key.
    await Promise.all(oldNames.map(({ name }) => clearNameRecords(env, deps, name)));
    deps.waitUntil(disconnectFromRelay(env, deps, old));
  }
  const device = await env.DB.prepare("SELECT * FROM devices WHERE endpoint_id = ? AND token_hash = ?")
    .bind(link.endpoint_id, tokenHash)
    .first<DeviceRow>();
  if (!device) throw ApiError.conflict("endpoint id already registered");
  return json(201, { device: deviceJson(device), device_token: token, resolve_token: resolveToken });
}

/** `POST /me/resolve-token`: a new read-only resolve token; the old one stops working (NFR-H2). */
export async function rotateResolveToken(request: Request, env: Env, deps: Deps): Promise<Response> {
  const device = requireDevice(await principal(request, env, nowSecs(deps.nowMs()), { deviceOnly: true }));
  const resolveToken = newToken(RESOLVE_TOKEN_PREFIX);
  await env.DB.prepare("UPDATE devices SET resolve_token_hash = ? WHERE endpoint_id = ? AND revoked_at IS NULL")
    .bind(await hashToken(resolveToken), device.endpointId)
    .run();
  return json(201, { resolve_token: resolveToken });
}

// ---------------------------------------------------------------- devices

/**
 * `GET /devices[?wait=<secs>]` with `If-None-Match`: 304 when unchanged; with `wait`, held
 * until the account's device list changes or `wait` passes (FR-H9).
 */
export async function listDevices(request: Request, env: Env, deps: Deps): Promise<Response> {
  const rawWait = new URL(request.url).searchParams.get("wait");
  const wait = rawWait === null ? 0 : /^\d{1,2}$/.test(rawWait) ? Number(rawWait) : -1;
  let p = await principal(request, env, nowSecs(deps.nowMs()));
  if (wait < 0 || wait > MAX_WAIT_SECS) throw ApiError.badRequest(`wait must be an integer from 0 to ${MAX_WAIT_SECS}`);
  const ifNoneMatch = request.headers.get("if-none-match");
  const version = await devicesVersion(env, p.accountId);
  if (ifNoneMatches(ifNoneMatch, etagFor(version))) {
    let changed = false;
    for (let round = 0; round < Math.ceil((wait * 1000) / WAIT_POLL_MS) && !changed; round++) {
      await deps.sleep(WAIT_POLL_MS);
      changed = (await devicesVersion(env, p.accountId)) !== version;
    }
    if (!changed) return new Response(null, { status: 304, headers: { etag: etagFor(version) } });
    // The change may be the caller's own removal.
    p = await principal(request, env, nowSecs(deps.nowMs()));
  }
  // One snapshot: the version and the list it describes.
  const [current, list] = await env.DB.batch([
    env.DB.prepare("SELECT devices_version FROM accounts WHERE account_id = ?").bind(p.accountId),
    env.DB.prepare("SELECT * FROM devices WHERE account_id = ? AND revoked_at IS NULL ORDER BY created_at, endpoint_id").bind(p.accountId),
  ]);
  const now = (current.results[0] as { devices_version: number } | undefined)?.devices_version ?? 0;
  return json(
    200,
    { devices: (list.results as DeviceRow[]).map(deviceJson) },
    { etag: etagFor(now), "cache-control": "private, no-cache" },
  );
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

/** The device itself, or account rights over it; else 403. Returns the caller's account. */
function requireSelfOrAccountAdmin(p: Principal, endpointId: string): string {
  return p.kind === "device" && p.endpointId === endpointId ? p.accountId : requireAccountAdmin(p);
}

/**
 * `PATCH /devices/{endpoint_id}` `{"name"?: string, "app"?: DeviceApp | null}`.
 * `name`: the device itself or account rights. `app`: the device itself only (FR-H10).
 */
export async function updateDevice(request: Request, env: Env, deps: Deps, endpointId: string): Promise<Response> {
  const p = await principal(request, env, nowSecs(deps.nowMs()));
  const body = await readJson<Record<string, unknown>>(request);
  const fields = Object.keys(body);
  if (fields.length === 0) throw ApiError.badRequest("nothing to change");
  const unknown = fields.filter((f) => f !== "name" && f !== "app");
  if (unknown.length > 0) throw ApiError.badRequest(`unknown or read-only field: ${unknown.join(", ")}`);
  const sets: string[] = [];
  const values: unknown[] = [];
  if ("name" in body) {
    if (!validDeviceName(body.name)) throw ApiError.badRequest("name must be 1 to 64 characters");
    sets.push("name = ?");
    values.push(body.name);
  }
  if ("app" in body) {
    const app = body.app === null ? null : parseDeviceApp(body.app);
    if (body.app !== null && !app) throw ApiError.badRequest("app must be {kind, version, services?} as documented");
    sets.push("app = ?");
    values.push(app ? JSON.stringify(app) : null);
  }
  // 404 before 403: other accounts' devices are not acknowledged.
  await accountDevice(env, p, endpointId);
  const isSelf = p.kind === "device" && p.endpointId === endpointId;
  if ("app" in body && !isSelf) throw ApiError.forbidden("only the device itself reports its app");
  const accountId = requireSelfOrAccountAdmin(p, endpointId);
  await env.DB.batch([
    env.DB.prepare(
      `UPDATE devices SET ${sets.join(", ")} WHERE account_id = ? AND endpoint_id = ? AND revoked_at IS NULL`,
    ).bind(...values, accountId, endpointId),
    bumpDevicesVersion(env, accountId),
  ]);
  return json(200, deviceJson(await accountDevice(env, p, endpointId)));
}

/** `POST /devices/{endpoint_id}/readmit`: the owner lets a removed key link again (FR-H11). */
export async function readmitDevice(request: Request, env: Env, deps: Deps, endpointId: string): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  const p = await principal(request, env, now);
  if (p.kind !== "session") throw ApiError.forbidden("only a signed-in session may re-admit a removed key");
  const row = await env.DB.prepare("SELECT revoked_at FROM devices WHERE endpoint_id = ? AND account_id = ?")
    .bind(endpointId, p.accountId)
    .first<{ revoked_at: number | null }>();
  if (!row) throw ApiError.notFound();
  if (row.revoked_at === null) throw ApiError.conflict("the device is not removed");
  const expiresAt = now + LINK_TTL_SECS;
  await env.DB.prepare("UPDATE devices SET readmit_until = ? WHERE endpoint_id = ? AND account_id = ? AND revoked_at IS NOT NULL")
    .bind(expiresAt, endpointId, p.accountId)
    .run();
  return json(200, { endpoint_id: endpointId, expires_at: expiresAt });
}

/** Most removed devices `GET /removed-devices` lists (FR-H11). */
export const MAX_REMOVED_LISTED = 100;

/** `GET /removed-devices`: the account's removed devices, newest first, for re-admission (FR-H11). */
export async function listRemovedDevices(request: Request, env: Env, deps: Deps): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  const p = await principal(request, env, now);
  if (p.kind !== "session") throw ApiError.forbidden("only a signed-in session may list removed devices");
  const { results } = await env.DB.prepare(
    `SELECT endpoint_id, name, role, created_at, revoked_at, readmit_until FROM devices
     WHERE account_id = ? AND revoked_at IS NOT NULL ORDER BY revoked_at DESC, endpoint_id LIMIT ?`,
  )
    .bind(p.accountId, MAX_REMOVED_LISTED)
    .all<{ endpoint_id: string; name: string; role: string; created_at: number; revoked_at: number; readmit_until: number | null }>();
  return json(
    200,
    {
      devices: results.map((d) => ({
        endpoint_id: d.endpoint_id,
        name: d.name,
        role: d.role,
        created_at: d.created_at,
        removed_at: d.revoked_at,
        readmit_until: d.readmit_until !== null && d.readmit_until > now ? d.readmit_until : null,
      })),
    },
    { "cache-control": "private, no-store" },
  );
}

/** Tells the relay host to drop a removed device's connections (best effort; SPEC FR-H3). */
async function disconnectFromRelay(env: Env, deps: Deps, endpointId: string): Promise<void> {
  if (!env.RELAY_ADMIN_URL || !env.RELAY_SHARED_SECRET) return;
  await deps
    .fetch(`${env.RELAY_ADMIN_URL}/admin/disconnect`, {
      method: "POST",
      headers: { authorization: `Bearer ${env.RELAY_SHARED_SECRET}`, "content-type": "application/json" },
      body: JSON.stringify({ endpoint_id: endpointId }),
    })
    .catch(() => undefined);
}

export async function removeDevice(request: Request, env: Env, deps: Deps, endpointId: string): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  // A device may always leave by itself; removing another device needs account rights. There
  // is no other main server to remove: an account has one (FR-H1), replaced only through a
  // session-approved main_server link.
  const p = await principal(request, env, now);
  const accountId = requireSelfOrAccountAdmin(p, endpointId);
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
    bumpDevicesVersion(env, accountId),
    env.DB.prepare("DELETE FROM names WHERE endpoint_id = ?").bind(endpointId),
    env.DB.prepare("DELETE FROM shares WHERE endpoint_id = ?").bind(endpointId),
    env.DB.prepare("DELETE FROM records WHERE endpoint_id = ?").bind(endpointId),
  ]);
  for (const { name } of names) deps.waitUntil(clearNameRecords(env, deps, name));
  deps.waitUntil(disconnectFromRelay(env, deps, endpointId));
  return noContent();
}
