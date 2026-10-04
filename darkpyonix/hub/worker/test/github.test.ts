// FR-H6: GitHub sign-in (authorization code + PKCE S256 + state), against a fake GitHub.

import { describe, expect, it } from "vitest";
import { base64url, sha256 } from "../src/util";
import { call, cookieValue, hubEnv, makeDeps, signIn } from "./helpers";

describe("GitHub sign-in", () => {
  it("test_fr_h6_login_redirects_to_github_with_pkce_and_state", async () => {
    const deps = makeDeps();
    const response = await call(deps, "GET", "/auth/login?return_to=/link?code=BCDF-GHJK");
    expect(response.status).toBe(302);
    const location = new URL(response.headers.get("location")!);
    expect(location.origin + location.pathname).toBe("https://github.com/login/oauth/authorize");
    expect(location.searchParams.get("client_id")).toBe("Iv1.testclient");
    expect(location.searchParams.get("redirect_uri")).toBe("https://darkpyonix.dev/auth/callback");
    expect(location.searchParams.get("code_challenge_method")).toBe("S256");
    expect(location.searchParams.get("code_challenge")).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(location.searchParams.get("scope")).toBeNull(); // public profile only
    const state = location.searchParams.get("state")!;
    expect(state).toMatch(/^[0-9a-f]{64}$/);
    expect(cookieValue(response, "__Host-dp_oauth")).toBe(state);
    const cookie = response.headers.getSetCookie().find((c) => c.startsWith("__Host-dp_oauth="))!;
    expect(cookie).toContain("HttpOnly");
    expect(cookie).toContain("Secure");
    expect(cookie).toContain("SameSite=Lax");
  });

  it("test_fr_h6_callback_creates_one_account_per_github_user", async () => {
    const deps = makeDeps();
    const first = await signIn(deps, { id: 4242, login: "octo" });
    const second = await signIn(deps, { id: 4242, login: "octo-renamed" });
    const a = (await (await call(deps, "GET", "/me", { cookie: first })).json()) as { account_id: string };
    const b = (await (await call(deps, "GET", "/me", { cookie: second })).json()) as {
      account_id: string;
      github_login: string;
      via: string;
    };
    expect(b.account_id).toBe(a.account_id);
    expect(b.github_login).toBe("octo-renamed");
    expect(b.via).toBe("session");
    const other = await signIn(deps, { id: 7, login: "someone" });
    const c = (await (await call(deps, "GET", "/me", { cookie: other })).json()) as { account_id: string };
    expect(c.account_id).not.toBe(a.account_id);
  });

  it("test_fr_h6_github_token_is_revoked_and_not_stored", async () => {
    const deps = makeDeps();
    await signIn(deps, { id: 1, login: "one" });
    await Promise.all(deps.pending);
    expect(deps.github.revoked).toHaveLength(1);
    const columns = await hubEnv.DB.prepare("SELECT * FROM accounts").all();
    expect(JSON.stringify(columns.results)).not.toContain("gho_");
  });

  it("test_fr_h6_callback_rejects_state_from_another_browser", async () => {
    const deps = makeDeps();
    const start = await call(deps, "GET", "/auth/login");
    const location = new URL(start.headers.get("location")!);
    const state = location.searchParams.get("state")!;
    deps.github.codes.set("c1", { challenge: location.searchParams.get("code_challenge")!, user: { id: 9, login: "x" } });
    const noCookie = await call(deps, "GET", `/auth/callback?code=c1&state=${state}`);
    expect(noCookie.status).toBe(400);
    const wrong = await call(deps, "GET", `/auth/callback?code=c1&state=${state}`, { cookie: "__Host-dp_oauth=deadbeef" });
    expect(wrong.status).toBe(400);
  });

  it("test_fr_h6_state_is_single_use", async () => {
    const deps = makeDeps();
    const start = await call(deps, "GET", "/auth/login");
    const location = new URL(start.headers.get("location")!);
    const state = location.searchParams.get("state")!;
    const challenge = location.searchParams.get("code_challenge")!;
    const cookie = `__Host-dp_oauth=${state}`;
    deps.github.codes.set("c1", { challenge, user: { id: 9, login: "x" } });
    expect((await call(deps, "GET", `/auth/callback?code=c1&state=${state}`, { cookie })).status).toBe(302);
    deps.github.codes.set("c2", { challenge, user: { id: 9, login: "x" } });
    expect((await call(deps, "GET", `/auth/callback?code=c2&state=${state}`, { cookie })).status).toBe(400);
  });

  it("test_fr_h6_wrong_pkce_verifier_is_refused_by_the_provider", async () => {
    const deps = makeDeps();
    const start = await call(deps, "GET", "/auth/login");
    const state = new URL(start.headers.get("location")!).searchParams.get("state")!;
    // The fake GitHub issued this code for a different challenge.
    deps.github.codes.set("c1", { challenge: base64url(await sha256("someone else")), user: { id: 9, login: "x" } });
    const done = await call(deps, "GET", `/auth/callback?code=c1&state=${state}`, { cookie: `__Host-dp_oauth=${state}` });
    expect(done.status).toBe(400);
  });

  it("test_fr_h6_allowlist_limits_new_accounts", async () => {
    const deps = makeDeps();
    const env = { ...hubEnv, GITHUB_ALLOWED_IDS: "100, 200" };
    const { handle } = await import("../src/app");
    const start = await handle(new Request("https://darkpyonix.dev/auth/login"), env, deps);
    const location = new URL(start.headers.get("location")!);
    const state = location.searchParams.get("state")!;
    deps.github.codes.set("c1", { challenge: location.searchParams.get("code_challenge")!, user: { id: 300, login: "not-listed" } });
    const done = await handle(
      new Request(`https://darkpyonix.dev/auth/callback?code=c1&state=${state}`, { headers: { cookie: `__Host-dp_oauth=${state}` } }),
      env,
      deps,
    );
    expect(done.status).toBe(403);
  });

  it("test_fr_h6_return_to_stays_on_this_origin", async () => {
    const deps = makeDeps();
    for (const evil of ["//evil.example/", "https://evil.example/", "/\\evil.example"]) {
      const start = await call(deps, "GET", `/auth/login?return_to=${encodeURIComponent(evil)}`);
      const location = new URL(start.headers.get("location")!);
      const state = location.searchParams.get("state")!;
      deps.github.codes.set("c", { challenge: location.searchParams.get("code_challenge")!, user: { id: 5, login: "f" } });
      const done = await call(deps, "GET", `/auth/callback?code=c&state=${state}`, { cookie: `__Host-dp_oauth=${state}` });
      expect(done.headers.get("location")).toBe("/");
    }
  });

  it("test_fr_h6_logout_ends_the_session", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 11, login: "bye" });
    expect((await call(deps, "POST", "/auth/logout", { cookie })).status).toBe(204);
    expect((await call(deps, "GET", "/me", { cookie })).status).toBe(401);
  });

  it("test_fr_h6_session_writes_need_our_origin", async () => {
    const deps = makeDeps();
    const cookie = await signIn(deps, { id: 12, login: "csrf" });
    const response = await call(deps, "POST", "/link-codes/BCDF-GHJK", {
      cookie,
      origin: "https://evil.example",
      json: { approve: true },
    });
    expect(response.status).toBe(403);
  });
});
