// FR-H8: clients discover the pkarr directory instead of building it in. FR-H3: no hub relay.

import { describe, expect, it } from "vitest";
import { call, makeDeps } from "./helpers";

describe("config", () => {
  it("test_fr_h8_config_names_the_pkarr_url", async () => {
    const response = await call(makeDeps(), "GET", "/config");
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("public, max-age=300");
    const body = (await response.json()) as Record<string, unknown>;
    expect(body).toEqual({
      hub_version: expect.any(String),
      api_url: "https://api.darkpyonix.dev",
      front_url: "https://darkpyonix.dev",
      relay_urls: [],
      pkarr_url: "https://api.darkpyonix.dev/pkarr",
      link_url: "https://darkpyonix.dev/link",
      pkarr_watch: { max_keys: 32, max_wait: 25 },
    });
  });

  it("test_fr_h8_config_follows_the_worker_vars", async () => {
    const response = await call(makeDeps(), "GET", "/config", {
      env: { API_URL: "https://api.example", FRONT_URL: "https://front.example" },
    });
    const body = (await response.json()) as Record<string, unknown>;
    expect(body.api_url).toBe("https://api.example");
    expect(body.front_url).toBe("https://front.example");
    expect(body.pkarr_url).toBe("https://api.example/pkarr");
    expect(body.link_url).toBe("https://front.example/link");
  });

  it("test_fr_h3_config_advertises_no_relay", async () => {
    const body = (await (await call(makeDeps(), "GET", "/config")).json()) as { relay_urls: unknown };
    expect(body.relay_urls).toEqual([]);
  });

  it("test_nfr_v1_versioned_hub_path_is_not_served", async () => {
    // INTENT D16: no version segment in REST paths.
    for (const path of ["/v1/config", "/v1/devices", "/internal/v1/relay/admit"]) {
      const response = await call(makeDeps(), "GET", path);
      expect(response.status, path).toBe(404);
    }
  });
});
