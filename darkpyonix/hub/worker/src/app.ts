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
import { ApiError, corsOrigin, json } from "./http";
import { clearAcmeChallenge, listNames, releaseName, reserveName, setAcmeChallenge } from "./names";
import { admit, presence } from "./relay";
import { publishShare, resolveShare, unpublishShare } from "./shares";
import { nowSecs } from "./util";

export const VERSION = "0.3.0";

type Handler = (request: Request, env: Env, deps: Deps, ...params: string[]) => Promise<Response> | Response;

interface Route {
  /** The OpenAPI path template, e.g. `/devices/{endpoint_id}`. */
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

/** `GET /config` (FR-H8): where the relays and the pkarr directory are. No API version (D16). */
function config(_request: Request, env: Env): Response {
  return json(
    200,
    {
      hub_version: VERSION,
      api_url: env.API_URL,
      front_url: env.FRONT_URL,
      relay_urls: [env.RELAY_URL],
      pkarr_url: `${env.API_URL}/pkarr`,
      link_url: `${env.FRONT_URL}/link`,
    },
    { "cache-control": "public, max-age=300" },
  );
}

export const ROUTES: Route[] = [
  route("/health", { GET: health }),
  route("/config", { GET: config }),
  // FR-H6 GitHub sign-in
  route("/auth/login", { GET: login }),
  route("/auth/callback", { GET: callback }),
  route("/auth/logout", { POST: (r, e) => logout(r, e) }),
  route("/me", { GET: me }),
  route("/me/resolve-token", { POST: rotateResolveToken }),
  // FR-H1 devices
  route("/device-links", { POST: createLink }),
  route("/device-links/{link_id}", { GET: getLink }),
  route("/device-links/{link_id}/token", { POST: claimLink }),
  route("/link-codes/{user_code}", { GET: getLinkCode, POST: decideLinkCode }),
  route("/devices", { GET: listDevices }),
  route("/devices/{endpoint_id}", { GET: getDevice, PATCH: updateDevice, DELETE: removeDevice }),
  route("/devices/{endpoint_id}/readmit", { POST: readmitDevice }),
  route("/removed-devices", { GET: listRemovedDevices }),
  // FR-H2 directory
  route("/devices/{endpoint_id}/addresses", { GET: deviceAddresses }),
  route("/pkarr/{key}", { PUT: pkarrPut, GET: pkarrGet }),
  // FR-H3 relay host callbacks
  route("/internal/relay/admit", { POST: admit }),
  route("/internal/relay/presence", { POST: presence }),
  // FR-H4 shares
  route("/shares", { POST: publishShare }),
  route("/shares/{share_id}", { GET: resolveShare, DELETE: unpublishShare }),
  // FR-H5 names
  route("/names", { GET: listNames }),
  route("/names/{name}", { PUT: reserveName, DELETE: releaseName }),
  route("/names/{name}/acme-challenge", { PUT: setAcmeChallenge, DELETE: clearAcmeChallenge }),
];

async function dispatch(request: Request, env: Env, deps: Deps): Promise<Response> {
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
    return json(404, { error: "not found" });
  } catch (err) {
    if (err instanceof ApiError) return json(err.status, err.body());
    console.error("unhandled", err);
    return json(500, { error: "internal error" });
  }
}

export async function handle(request: Request, env: Env, deps: Deps): Promise<Response> {
  const origin = request.headers.get("origin");
  const allowed = corsOrigin(origin, env);
  const response = request.method === "OPTIONS" && allowed
    ? new Response(null, { status: 204 })
    : await dispatch(request, env, deps);
  if (allowed && origin) {
    response.headers.set("Access-Control-Allow-Origin", origin);
    response.headers.set("Access-Control-Allow-Credentials", "true");
    response.headers.append("Vary", "Origin");
    response.headers.set("Access-Control-Expose-Headers", "ETag");
    if (request.method === "OPTIONS") {
      response.headers.set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE");
      response.headers.set("Access-Control-Allow-Headers", "Authorization, Content-Type, If-None-Match");
      response.headers.set("Access-Control-Max-Age", "600");
    }
  }
  return response;
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
