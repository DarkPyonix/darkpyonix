// FR-H7: Flathub verifies dev.darkpyonix.Ember through https://darkpyonix.dev.

import { describe, expect, it } from "vitest";
import { call, makeDeps } from "./helpers";

const PATH = "/.well-known/org.flathub.VerifiedApps.txt";

describe("flathub verification", () => {
  it("test_fr_h7_verified_apps_file_serves_the_configured_token", async () => {
    const deps = makeDeps();
    const token = "8d0b7c6e-3f2a-4b1c-9e5d-0a1b2c3d4e5f";
    const response = await call(deps, "GET", PATH, { env: { FLATHUB_VERIFICATION_TOKEN: ` ${token}\n` } });
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/plain; charset=utf-8");
    expect(await response.text()).toBe(token);
    expect((await call(deps, "POST", PATH, { env: { FLATHUB_VERIFICATION_TOKEN: token } })).status).toBe(405);
  });

  it("test_fr_h7_verified_apps_file_is_absent_without_a_token", async () => {
    const deps = makeDeps();
    expect((await call(deps, "GET", PATH)).status).toBe(404);
    expect((await call(deps, "GET", PATH, { env: { FLATHUB_VERIFICATION_TOKEN: "" } })).status).toBe(404);
    expect((await call(deps, "GET", PATH, { env: { FLATHUB_VERIFICATION_TOKEN: "  \n" } })).status).toBe(404);
    // The dots are literal: a look-alike path is not this file.
    expect((await call(deps, "GET", "/xwell-known/orgxflathubxVerifiedApps.txt", { env: { FLATHUB_VERIFICATION_TOKEN: "t" } })).status).toBe(404);
  });
});
