import { describe, expect, it } from "vitest";
import { call, cookieValue, makeDeps, signIn } from "./helpers";

describe("FR-H13", () => {
  it("test_fr_h13_cors_allows_only_the_front", async () => {
    for (const origin of ["https://darkpyonix.dev", "http://localhost:5173", "https://studio.darkpyonix.dev", "null"]) {
      const r = await call(makeDeps(), "GET", "/config", { origin, env: { CORS_DEV_ORIGINS: "http://localhost:5173" } });
      const allowed = origin === "https://darkpyonix.dev" || origin === "http://localhost:5173";
      expect(r.headers.get("access-control-allow-origin")).toBe(allowed ? origin : null);
      expect(r.headers.get("access-control-allow-credentials")).toBe(allowed ? "true" : null);
      if (allowed) { expect(r.headers.get("vary")).toContain("Origin"); expect(r.headers.get("access-control-expose-headers")).toBe("ETag"); }
    }
  });
  it("test_fr_h13_preflight_lists_methods_and_headers", async () => {
    const r = await call(makeDeps(), "OPTIONS", "/me", { origin: "https://darkpyonix.dev" });
    expect(r.status).toBe(204);
    expect(r.headers.get("access-control-allow-methods")).toBe("GET, POST, PUT, PATCH, DELETE");
    expect(r.headers.get("access-control-allow-headers")).toBe("Authorization, Content-Type, If-None-Match");
    expect(r.headers.get("access-control-max-age")).toBe("600");
  });
  async function login(input: string) {
    const deps = makeDeps(); const start = await call(deps, "GET", `/auth/login?return_to=${encodeURIComponent(input)}`);
    const loc = new URL(start.headers.get("location")!); const state = loc.searchParams.get("state")!;
    deps.github.codes.set("c", { challenge: loc.searchParams.get("code_challenge")!, user: { id: 131, login: "cors" } });
    const done = await call(deps, "GET", `/auth/callback?code=c&state=${state}`, { cookie: `__Host-dp_oauth=${state}` });
    return { start, loc, done };
  }
  it("test_fr_h13_session_cookie_is_host_only_on_the_api", async () => {
    const { start, loc, done } = await login("/");
    expect(loc.searchParams.get("redirect_uri")).toBe("https://api.darkpyonix.dev/auth/callback");
    for (const cookie of [...start.headers.getSetCookie(), ...done.headers.getSetCookie()]) {
      expect(cookie).not.toMatch(/domain=/i); expect(cookie).toMatch(/Path=\//i); expect(cookie).toMatch(/Secure/i); expect(cookie).toMatch(/HttpOnly/i); expect(cookie).toMatch(/SameSite=Lax/i);
    }
    expect(cookieValue(done, "__Host-dp_session")).toBeTruthy();
  });
  it("test_fr_h13_callback_returns_to_the_front", async () => {
    for (const [input, expected] of [["/link?code=x#secret", "/link?code=x"], ["//evil", "/"], ["/\\evil", "/"], ["https://evil", "/"]]) {
      expect((await login(input)).done.headers.get("location")).toBe(`https://darkpyonix.dev${expected}`);
    }
  });
  it("test_fr_h13_cookie_writes_from_user_subdomains_are_refused", async () => {
    const deps = makeDeps(); const cookie = await signIn(deps, { id: 132, login: "cors" });
    for (const origin of ["https://studio.darkpyonix.dev", "null"]) {
      expect((await call(deps, "POST", "/auth/logout", { cookie, origin })).status).toBe(403);
      expect((await call(deps, "POST", "/link-codes/BCDF-GHJK", { cookie, origin, json: { approve: true } })).status).toBe(403);
    }
  });
});
