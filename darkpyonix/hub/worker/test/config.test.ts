// FR-H8: clients discover the relays and the pkarr directory instead of building them in.

import { describe, expect, it } from "vitest";
import { call, makeDeps } from "./helpers";

describe("config", () => {
  it("test_fr_h8_config_names_relays_and_pkarr_url", async () => {
    const response = await call(makeDeps(), "GET", "/config");
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("public, max-age=300");
    const body = (await response.json()) as Record<string, unknown>;
    expect(body).toEqual({
      hub_version: expect.any(String),
      relay_urls: ["https://relay.darkpyonix.dev/"],
      pkarr_url: "https://darkpyonix.dev/pkarr",
      link_url: "https://darkpyonix.dev/link",
    });
  });

  it("test_fr_h8_config_follows_the_worker_vars", async () => {
    const response = await call(makeDeps(), "GET", "/config", {
      env: { PUBLIC_URL: "https://hub.example", RELAY_URL: "https://relay.example/" },
    });
    const body = (await response.json()) as Record<string, unknown>;
    expect(body.relay_urls).toEqual(["https://relay.example/"]);
    expect(body.pkarr_url).toBe("https://hub.example/pkarr");
    expect(body.link_url).toBe("https://hub.example/link");
  });

  it("test_nfr_v1_versioned_hub_path_is_not_served", async () => {
    // INTENT D16: no version segment in REST paths.
    for (const path of ["/v1/config", "/v1/devices", "/internal/v1/relay/admit"]) {
      const response = await call(makeDeps(), "GET", path);
      expect(response.status, path).toBe(404);
    }
  });
});
