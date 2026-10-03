// Share links and the ash viewer (SPEC FR-H4).

import { storedAddresses } from "./directory";
import type { Deps, Env } from "./env";
import { ApiError, html, json, noContent, principal, readJson, requireDevice } from "./http";
import { sharePlaceholderPage } from "./pages";
import { SHARE_ID_RE, hashToken, newToken, nowSecs } from "./util";

/** How long a guest relay pass is valid. */
export const RELAY_PASS_TTL_SECS = 600;

interface ShareHost {
  endpoint_id: string;
  account_id: string;
}

async function shareHost(env: Env, shareId: string): Promise<ShareHost | null> {
  if (!SHARE_ID_RE.test(shareId)) return null;
  return env.DB.prepare(
    `SELECT s.endpoint_id, d.account_id FROM shares s JOIN devices d ON d.endpoint_id = s.endpoint_id
     WHERE s.share_id = ? AND d.revoked_at IS NULL`,
  )
    .bind(shareId)
    .first<ShareHost>();
}

/** `POST /v1/shares` `{"share_id": "s_..."}` with the hosting device's token. */
export async function publishShare(request: Request, env: Env, deps: Deps): Promise<Response> {
  const now = nowSecs(deps.nowMs());
  const device = requireDevice(await principal(request, env, now, { deviceOnly: true }));
  const body = await readJson<{ share_id?: unknown }>(request);
  const shareId = body.share_id;
  if (typeof shareId !== "string" || !SHARE_ID_RE.test(shareId)) {
    throw ApiError.badRequest("share_id must match ^s_[0-9a-f]{16}$");
  }
  await env.DB.prepare(
    "INSERT INTO shares (share_id, endpoint_id, created_at) VALUES (?, ?, ?) ON CONFLICT (share_id) DO NOTHING",
  )
    .bind(shareId, device.endpointId, now)
    .run();
  const owner = await env.DB.prepare("SELECT endpoint_id FROM shares WHERE share_id = ?")
    .bind(shareId)
    .first<{ endpoint_id: string }>();
  if (owner?.endpoint_id !== device.endpointId) throw ApiError.conflict("published by another device");
  return json(201, { share_id: shareId, url: `${env.PUBLIC_URL}/s/${shareId}` });
}

/** `GET /v1/shares/{share_id}`: public. Returns the host and a fresh guest relay pass. */
export async function resolveShare(_request: Request, env: Env, deps: Deps, shareId: string): Promise<Response> {
  const host = await shareHost(env, shareId);
  if (!host) throw ApiError.notFound();
  let relayUrl = env.RELAY_URL;
  try {
    relayUrl = (await storedAddresses(env, host.endpoint_id)).relayUrls[0] ?? env.RELAY_URL;
  } catch {
    // Nothing published yet: dial through our relay.
  }
  const now = nowSecs(deps.nowMs());
  const token = newToken("dpg_");
  const expiresAt = now + RELAY_PASS_TTL_SECS;
  await env.DB.prepare("INSERT INTO relay_passes (token_hash, share_id, expires_at) VALUES (?, ?, ?)")
    .bind(await hashToken(token), shareId, expiresAt)
    .run();
  return json(200, {
    share_id: shareId,
    endpoint_id: host.endpoint_id,
    relay_url: relayUrl,
    relay_token: token,
    relay_token_expires_at: expiresAt,
  });
}

/** `DELETE /v1/shares/{share_id}`: the hosting device, or the account (session / main server). */
export async function unpublishShare(request: Request, env: Env, deps: Deps, shareId: string): Promise<Response> {
  const p = await principal(request, env, nowSecs(deps.nowMs()));
  const host = await shareHost(env, shareId);
  const allowed =
    host !== null &&
    (p.kind === "device" && p.role !== "main_server"
      ? p.endpointId === host.endpoint_id
      : p.accountId === host.account_id);
  if (!allowed) throw ApiError.notFound();
  await env.DB.prepare("DELETE FROM shares WHERE share_id = ?").bind(shareId).run();
  return noContent();
}

async function ashIndex(env: Env, request: Request): Promise<string | null> {
  try {
    const response = await env.ASSETS.fetch(new URL("/ash/", request.url).toString());
    return response.ok ? await response.text() : null;
  } catch {
    return null;
  }
}

/** `GET /s/{share_id}`: the ash viewer page (the token after `#` never reaches us). */
export async function sharePage(request: Request, env: Env, _deps: Deps, shareId: string): Promise<Response> {
  if (!(await shareHost(env, shareId))) return html(404, "<!doctype html><p>Unknown share.</p>\n");
  const index = await ashIndex(env, request);
  // Until darkpyonix-ash is deployed, public/ash/ holds a marked placeholder.
  const viewer = index && !index.includes('content="placeholder"') ? index : null;
  if (viewer) {
    // The viewer sets its own policy (it needs WebAssembly and the relay's WebSocket).
    return new Response(viewer, { status: 200, headers: { "content-type": "text/html; charset=utf-8" } });
  }
  return html(200, sharePlaceholderPage(shareId));
}

/** `GET /ash/` when the request reaches the Worker (assets normally answer first). */
export async function ashPage(request: Request, env: Env): Promise<Response> {
  const index = await ashIndex(env, request);
  const body = index ?? "<!doctype html><title>ash</title><p>The official ash viewer will be served here.</p>\n";
  return new Response(body, { status: 200, headers: { "content-type": "text/html; charset=utf-8" } });
}
