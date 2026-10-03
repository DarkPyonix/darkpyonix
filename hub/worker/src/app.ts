// Routing for darkpyonix.dev (docs/api/hub.openapi.yaml). Every operation of that file that is
// served by this Worker is listed here, and nothing else (CLAUDE.md: the OpenAPI is the SPEC).

import {
  claimLink,
  createLink,
  decideLinkCode,
  getDevice,
  getLink,
  getLinkCode,
  listDevices,
  listRemovedDevices,
  me,
  readmitDevice,
  removeDevice,
  rotateResolveToken,
  updateDevice,
} from "./devices";
import { deviceAddresses, pkarrGet, pkarrPut } from "./directory";
import type { Deps, Env } from "./env";
import { callback, login, logout } from "./github";
import { ApiError, SESSION_COOKIE, getCookie, html, json, redirect } from "./http";
import { clearAcmeChallenge, listNames, releaseName, reserveName, setAcmeChallenge } from "./names";
import { linkPage } from "./pages";
import { admit, presence } from "./relay";
import { ashPage, publishShare, resolveShare, sharePage, unpublishShare } from "./shares";
import { hashToken, normalizeUserCode, nowSecs } from "./util";

export const VERSION = "0.3.0";

type Handler = (request: Request, env: Env, deps: Deps, ...params: string[]) => Promise<Response> | Response;

interface Route {
  /** The OpenAPI path template, e.g. `/v1/devices/{endpoint_id}`. */
  path: string;
  pattern: RegExp;
  methods: Partial<Record<string, Handler>>;
}

const SEGMENT = "([^/]+)";

function route(path: string, methods: Partial<Record<string, Handler>>): Route {
  // Literal parts are escaped (e.g. the dots of `/.well-known/...`); `{param}` matches one segment.
  const literal = path.split(/\{[a-z_]+\}/).map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const pattern = new RegExp(`^${literal.join(SEGMENT)}$`);
  return { path, pattern, methods };
}

async function health(): Promise<Response> {
  return json(200, { status: "ok", version: VERSION });
}

/** Bumped only for a breaking change under `/v1` (FR-H8). */
export const API_VERSION = 1;

/** `GET /v1/config` (FR-H8): where the relays and the pkarr directory are. */
function config(_request: Request, env: Env): Response {
  return json(
    200,
    {
      api_version: API_VERSION,
      hub_version: VERSION,
      relay_urls: [env.RELAY_URL],
      pkarr_url: `${env.PUBLIC_URL}/pkarr`,
      link_url: `${env.PUBLIC_URL}/link`,
    },
    { "cache-control": "public, max-age=300" },
  );
}

/**
 * `GET /.well-known/org.flathub.VerifiedApps.txt` (FR-H7): Flathub's website verification file,
 * the token Flathub shows for dev.darkpyonix.Ember. A plain text file, as a static asset would be,
 * but its content is the FLATHUB_VERIFICATION_TOKEN var/secret so no token lives in the repository.
 */
function flathubVerifiedApps(_request: Request, env: Env): Response {
  const token = (env.FLATHUB_VERIFICATION_TOKEN ?? "").trim();
  if (!token) return json(404, { error: "not found" });
  return new Response(token, {
    status: 200,
    headers: { "content-type": "text/plain; charset=utf-8", "cache-control": "public, max-age=300" },
  });
}

/** `GET /link?code=...`: the approval page; signs the browser in with GitHub first. */
async function linkLanding(request: Request, env: Env, deps: Deps): Promise<Response> {
  const url = new URL(request.url);
  const code = normalizeUserCode(url.searchParams.get("code") ?? "") ?? "";
  const session = getCookie(request, SESSION_COOKIE);
  const row = session
    ? await env.DB.prepare(
        `SELECT a.github_login FROM sessions s JOIN accounts a ON a.account_id = s.account_id
         WHERE s.session_hash = ? AND s.expires_at > ?`,
      )
        .bind(await hashToken(session), nowSecs(deps.nowMs()))
        .first<{ github_login: string }>()
    : null;
  if (!row) {
    const back = code ? `/link?code=${code}` : "/link";
    return redirect(`/auth/login?return_to=${encodeURIComponent(back)}`);
  }
  return html(200, linkPage(row.github_login, code));
}

export const ROUTES: Route[] = [
  route("/health", { GET: health }),
  route("/v1/config", { GET: config }),
  route("/.well-known/org.flathub.VerifiedApps.txt", { GET: flathubVerifiedApps }),
  // FR-H6 GitHub sign-in
  route("/auth/login", { GET: login }),
  route("/auth/callback", { GET: callback }),
  route("/auth/logout", { POST: (r, e) => logout(r, e) }),
  route("/v1/me", { GET: me }),
  route("/v1/me/resolve-token", { POST: rotateResolveToken }),
  // FR-H1 devices
  route("/link", { GET: linkLanding }),
  route("/v1/device-links", { POST: createLink }),
  route("/v1/device-links/{link_id}", { GET: getLink }),
  route("/v1/device-links/{link_id}/token", { POST: claimLink }),
  route("/v1/link-codes/{user_code}", { GET: getLinkCode, POST: decideLinkCode }),
  route("/v1/devices", { GET: listDevices }),
  route("/v1/devices/{endpoint_id}", { GET: getDevice, PATCH: updateDevice, DELETE: removeDevice }),
  route("/v1/devices/{endpoint_id}/readmit", { POST: readmitDevice }),
  route("/v1/removed-devices", { GET: listRemovedDevices }),
  // FR-H2 directory
  route("/v1/devices/{endpoint_id}/addresses", { GET: deviceAddresses }),
  route("/pkarr/{key}", { PUT: pkarrPut, GET: pkarrGet }),
  // FR-H3 relay host callbacks
  route("/internal/v1/relay/admit", { POST: admit }),
  route("/internal/v1/relay/presence", { POST: presence }),
  // FR-H4 shares
  route("/v1/shares", { POST: publishShare }),
  route("/v1/shares/{share_id}", { GET: resolveShare, DELETE: unpublishShare }),
  route("/s/{share_id}", { GET: sharePage }),
  route("/ash/", { GET: (r, e) => ashPage(r, e) }),
  // FR-H5 names
  route("/v1/names", { GET: listNames }),
  route("/v1/names/{name}", { PUT: reserveName, DELETE: releaseName }),
  route("/v1/names/{name}/acme-challenge", { PUT: setAcmeChallenge, DELETE: clearAcmeChallenge }),
];

export async function handle(request: Request, env: Env, deps: Deps): Promise<Response> {
  const url = new URL(request.url);
  try {
    for (const r of ROUTES) {
      const match = r.pattern.exec(url.pathname);
      if (!match) continue;
      const handler = r.methods[request.method];
      if (!handler) {
        return json(405, { error: "method not allowed" }, { allow: Object.keys(r.methods).join(", ") });
      }
      const params = match.slice(1).map((p) => decodeURIComponent(p));
      return await handler(request, env, deps, ...params);
    }
    // Everything else: static assets (the ash viewer's files under /ash/).
    if (url.pathname.startsWith("/ash/")) return env.ASSETS.fetch(request);
    return json(404, { error: "not found" });
  } catch (err) {
    if (err instanceof ApiError) return json(err.status, err.body());
    console.error("unhandled", err);
    return json(500, { error: "internal error" });
  }
}

/** Hourly: drop expired single-use rows. */
export async function cleanup(env: Env, nowMs: number): Promise<void> {
  const now = nowSecs(nowMs);
  await env.DB.batch([
    env.DB.prepare("DELETE FROM oauth_transactions WHERE expires_at < ?").bind(now),
    env.DB.prepare("DELETE FROM device_links WHERE expires_at < ?").bind(now),
    env.DB.prepare("DELETE FROM relay_passes WHERE expires_at < ?").bind(now),
    env.DB.prepare("DELETE FROM sessions WHERE expires_at < ?").bind(now),
  ]);
}
