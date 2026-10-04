// GitHub sign-in (SPEC FR-H6): OAuth 2.0 authorization code with PKCE (S256) and state.
//
// The hub asks for no scope: an unscoped GitHub token reads only the public profile, which
// is all we need (the numeric user id). The access token is used once to read `GET /user`,
// then revoked; it is never stored.

import type { Deps, Env } from "./env";
import {
  ApiError,
  SESSION_COOKIE,
  STATE_COOKIE,
  clearCookie,
  getCookie,
  noContent,
  redirect,
  setCookie,
} from "./http";
import { base64url, hashToken, newToken, nowSecs, randomBytes, randomHex, safeReturnTo, sha256 } from "./util";

const AUTHORIZE_URL = "https://github.com/login/oauth/authorize";
const TOKEN_URL = "https://github.com/login/oauth/access_token";
const USER_URL = "https://api.github.com/user";
const USER_AGENT = "darkpyonix-hub";

const TRANSACTION_TTL_SECS = 600;
export const SESSION_TTL_SECS = 30 * 24 * 3600;

function configured(env: Env): { clientId: string; clientSecret: string } {
  if (!env.GITHUB_CLIENT_ID || !env.GITHUB_CLIENT_SECRET) {
    throw new ApiError(503, "GitHub sign-in is not configured on this hub");
  }
  return { clientId: env.GITHUB_CLIENT_ID, clientSecret: env.GITHUB_CLIENT_SECRET };
}

function callbackUrl(env: Env): string {
  return `${env.PUBLIC_URL}/auth/callback`;
}

/** `GET /auth/login?return_to=/path`: start the GitHub authorization. */
export async function login(request: Request, env: Env, deps: Deps): Promise<Response> {
  const { clientId } = configured(env);
  const returnTo = safeReturnTo(new URL(request.url).searchParams.get("return_to"));
  const state = randomHex(32);
  const verifier = base64url(randomBytes(32));
  const challenge = base64url(await sha256(verifier));
  await env.DB.prepare(
    "INSERT INTO oauth_transactions (state, code_verifier, return_to, expires_at) VALUES (?, ?, ?, ?)",
  )
    .bind(state, verifier, returnTo, nowSecs(deps.nowMs()) + TRANSACTION_TTL_SECS)
    .run();
  const url = new URL(AUTHORIZE_URL);
  url.search = new URLSearchParams({
    client_id: clientId,
    redirect_uri: callbackUrl(env),
    state,
    code_challenge: challenge,
    code_challenge_method: "S256",
    allow_signup: "true",
  }).toString();
  // The state is also bound to this browser, so a callback started elsewhere (login CSRF) fails.
  return redirect(url.toString(), [["set-cookie", setCookie(STATE_COOKIE, state, TRANSACTION_TTL_SECS)]]);
}

interface GitHubUser {
  id: number;
  login: string;
}

async function exchangeCode(env: Env, deps: Deps, code: string, verifier: string): Promise<string> {
  const { clientId, clientSecret } = configured(env);
  const response = await deps.fetch(TOKEN_URL, {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": "application/x-www-form-urlencoded",
      "user-agent": USER_AGENT,
    },
    body: new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      code,
      redirect_uri: callbackUrl(env),
      code_verifier: verifier,
    }),
  });
  // GitHub answers 200 with {"error": ...} for a bad code or verifier.
  const body = (await response.json().catch(() => ({}))) as { access_token?: string; error?: string };
  if (!response.ok || !body.access_token) {
    throw ApiError.badRequest(`GitHub refused the code: ${body.error ?? response.status}`);
  }
  return body.access_token;
}

async function fetchUser(deps: Deps, accessToken: string): Promise<GitHubUser> {
  const response = await deps.fetch(USER_URL, {
    headers: {
      accept: "application/vnd.github+json",
      authorization: `Bearer ${accessToken}`,
      "user-agent": USER_AGENT,
      "x-github-api-version": "2022-11-28",
    },
  });
  const user = (await response.json().catch(() => ({}))) as Partial<GitHubUser>;
  if (!response.ok || typeof user.id !== "number" || typeof user.login !== "string") {
    throw new ApiError(502, "could not read the GitHub user");
  }
  return { id: user.id, login: user.login };
}

/** Revokes the access token (best effort): the hub keeps no GitHub credential. */
async function revokeToken(env: Env, deps: Deps, accessToken: string): Promise<void> {
  const { clientId, clientSecret } = configured(env);
  await deps
    .fetch(`https://api.github.com/applications/${clientId}/token`, {
      method: "DELETE",
      headers: {
        accept: "application/vnd.github+json",
        authorization: `Basic ${btoa(`${clientId}:${clientSecret}`)}`,
        "user-agent": USER_AGENT,
        "content-type": "application/json",
      },
      body: JSON.stringify({ access_token: accessToken }),
    })
    .catch(() => undefined);
}

function allowed(env: Env, githubId: number): boolean {
  const list = (env.GITHUB_ALLOWED_IDS ?? "").split(",").map((s) => s.trim()).filter(Boolean);
  return list.length === 0 || list.includes(String(githubId));
}

/** Finds or creates the account of a GitHub user. */
async function accountFor(env: Env, user: GitHubUser, now: number): Promise<string> {
  const existing = await env.DB.prepare("SELECT account_id FROM accounts WHERE github_id = ?")
    .bind(user.id)
    .first<{ account_id: string }>();
  if (existing) {
    await env.DB.prepare("UPDATE accounts SET github_login = ? WHERE account_id = ?")
      .bind(user.login, existing.account_id)
      .run();
    return existing.account_id;
  }
  if (!allowed(env, user.id)) throw ApiError.forbidden("this GitHub user may not create an account here");
  const accountId = `a_${randomHex(8)}`;
  // A concurrent first login of the same user loses the UNIQUE race and reads the winner.
  await env.DB.prepare(
    "INSERT INTO accounts (account_id, github_id, github_login, created_at) VALUES (?, ?, ?, ?) ON CONFLICT (github_id) DO NOTHING",
  )
    .bind(accountId, user.id, user.login, now)
    .run();
  const row = await env.DB.prepare("SELECT account_id FROM accounts WHERE github_id = ?")
    .bind(user.id)
    .first<{ account_id: string }>();
  if (!row) throw new ApiError(500, "account creation failed");
  return row.account_id;
}

/** `GET /auth/callback?code=...&state=...`: finish sign-in and set the session cookie. */
export async function callback(request: Request, env: Env, deps: Deps): Promise<Response> {
  configured(env);
  const params = new URL(request.url).searchParams;
  const state = params.get("state");
  const code = params.get("code");
  if (params.get("error")) throw ApiError.badRequest(`GitHub: ${params.get("error")}`);
  if (!state || !code) throw ApiError.badRequest("missing code or state");
  const cookieState = getCookie(request, STATE_COOKIE);
  if (!cookieState || cookieState !== state) throw ApiError.badRequest("state does not match this browser");

  const now = nowSecs(deps.nowMs());
  const tx = await env.DB.prepare(
    "DELETE FROM oauth_transactions WHERE state = ? RETURNING code_verifier, return_to, expires_at",
  )
    .bind(state)
    .first<{ code_verifier: string; return_to: string; expires_at: number }>();
  if (!tx || tx.expires_at < now) throw ApiError.badRequest("unknown, used or expired state");

  const accessToken = await exchangeCode(env, deps, code, tx.code_verifier);
  let user: GitHubUser;
  try {
    user = await fetchUser(deps, accessToken);
  } finally {
    deps.waitUntil(revokeToken(env, deps, accessToken));
  }
  const accountId = await accountFor(env, user, now);

  const session = newToken("dps_");
  await env.DB.prepare(
    "INSERT INTO sessions (session_hash, account_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
  )
    .bind(await hashToken(session), accountId, now, now + SESSION_TTL_SECS)
    .run();
  return redirect(safeReturnTo(tx.return_to), [
    ["set-cookie", setCookie(SESSION_COOKIE, session, SESSION_TTL_SECS)],
    ["set-cookie", clearCookie(STATE_COOKIE)],
  ]);
}

/** `POST /auth/logout` */
export async function logout(request: Request, env: Env): Promise<Response> {
  const origin = request.headers.get("origin");
  if (origin !== null && origin !== new URL(env.PUBLIC_URL).origin) {
    throw ApiError.forbidden("cross-origin logout");
  }
  const session = getCookie(request, SESSION_COOKIE);
  if (session) {
    await env.DB.prepare("DELETE FROM sessions WHERE session_hash = ?").bind(await hashToken(session)).run();
  }
  return noContent({ "set-cookie": clearCookie(SESSION_COOKIE) });
}
