// Responses, errors, cookies, and who is calling.

import type { Env } from "./env";
import { hashToken } from "./util";

/** Machine-readable error codes (components/schemas/Error in the OpenAPI file). */
export type ErrorCode = "invalid_credentials" | "device_removed";

/** An error answered as `{"error": "...", "code"?: "..."}` with a status the OpenAPI file documents. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly code?: ErrorCode,
  ) {
    super(message);
  }
  body(): { error: string; code?: ErrorCode } {
    return this.code ? { error: this.message, code: this.code } : { error: this.message };
  }
  static badRequest(message: string): ApiError {
    return new ApiError(400, message);
  }
  static unauthorized(message = "missing or invalid credentials"): ApiError {
    return new ApiError(401, message, "invalid_credentials");
  }
  static deviceRemoved(): ApiError {
    return new ApiError(401, "this device was removed from its account", "device_removed");
  }
  static forbidden(message: string): ApiError {
    return new ApiError(403, message);
  }
  static notFound(message = "not found"): ApiError {
    return new ApiError(404, message);
  }
  static conflict(message: string): ApiError {
    return new ApiError(409, message);
  }
}

export function json(status: number, value: unknown, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

export function noContent(headers: Record<string, string> = {}): Response {
  return new Response(null, { status: 204, headers });
}

export function html(status: number, body: string, headers: Record<string, string> = {}): Response {
  return new Response(body, {
    status,
    headers: {
      "content-type": "text/html; charset=utf-8",
      "content-security-policy":
        "default-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
      ...headers,
    },
  });
}

export function redirect(location: string, headers: [string, string][] = []): Response {
  const h = new Headers({ location });
  for (const [k, v] of headers) h.append(k, v);
  return new Response(null, { status: 302, headers: h });
}

export async function readJson<T>(request: Request): Promise<T> {
  try {
    const value = await request.json();
    if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error();
    return value as T;
  } catch {
    throw ApiError.badRequest("body must be a JSON object");
  }
}

// ---------------------------------------------------------------- cookies

export const SESSION_COOKIE = "__Host-dp_session";
export const STATE_COOKIE = "__Host-dp_oauth";

export function getCookie(request: Request, name: string): string | null {
  const header = request.headers.get("cookie");
  if (!header) return null;
  for (const part of header.split(";")) {
    const [k, ...v] = part.trim().split("=");
    if (k === name) return v.join("=");
  }
  return null;
}

/** `__Host-` cookies: Secure, Path=/, no Domain. */
export function setCookie(name: string, value: string, maxAgeSecs: number): string {
  return `${name}=${value}; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=${maxAgeSecs}`;
}

export function clearCookie(name: string): string {
  return setCookie(name, "", 0);
}

// ---------------------------------------------------------------- callers

export type Principal =
  | { kind: "session"; accountId: string }
  | { kind: "device"; accountId: string; endpointId: string; role: string };

function bearer(request: Request): string | null {
  const value = request.headers.get("authorization");
  if (!value) return null;
  const [scheme, token] = value.split(" ", 2);
  return scheme?.toLowerCase() === "bearer" && token ? token.trim() : null;
}

/**
 * The caller, from a device token (`Authorization: Bearer`, or `?token=` where `allowQuery`)
 * or the browser session cookie. A cookie-authenticated write must come from our own origin
 * (the session cookie is SameSite=Lax; this closes the rest of CSRF).
 */
export async function principal(
  request: Request,
  env: Env,
  nowSecs: number,
  opts: { allowQuery?: boolean; deviceOnly?: boolean } = {},
): Promise<Principal> {
  let token = bearer(request);
  if (!token && opts.allowQuery) token = new URL(request.url).searchParams.get("token");
  if (token) {
    // Removed devices keep their token hash, so their token is told apart from a bad one.
    const row = await env.DB.prepare(
      "SELECT endpoint_id, account_id, role, revoked_at FROM devices WHERE token_hash = ?",
    )
      .bind(await hashToken(token))
      .first<{ endpoint_id: string; account_id: string; role: string; revoked_at: number | null }>();
    if (!row) throw ApiError.unauthorized();
    if (row.revoked_at !== null) throw ApiError.deviceRemoved();
    return { kind: "device", accountId: row.account_id, endpointId: row.endpoint_id, role: row.role };
  }
  const session = opts.deviceOnly ? null : getCookie(request, SESSION_COOKIE);
  if (session) {
    const row = await env.DB.prepare(
      "SELECT account_id FROM sessions WHERE session_hash = ? AND expires_at > ?",
    )
      .bind(await hashToken(session), nowSecs)
      .first<{ account_id: string }>();
    if (row) {
      if (request.method !== "GET" && request.method !== "HEAD") {
        const origin = request.headers.get("origin");
        if (origin !== new URL(env.PUBLIC_URL).origin) {
          throw ApiError.forbidden("cross-origin request with a session cookie");
        }
      }
      return { kind: "session", accountId: row.account_id };
    }
  }
  throw ApiError.unauthorized();
}

/**
 * Account-level rights (approve device links, remove devices, release names): a signed-in
 * browser session, or a device token of the account's main server.
 */
export function requireAccountAdmin(p: Principal): string {
  if (p.kind === "session" || p.role === "main_server") return p.accountId;
  throw ApiError.forbidden("needs a signed-in session or the account's main server");
}

export function requireDevice(p: Principal): Extract<Principal, { kind: "device" }> {
  if (p.kind === "device") return p;
  throw ApiError.unauthorized("this operation needs a device token");
}
