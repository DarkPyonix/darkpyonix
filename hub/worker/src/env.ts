// Bindings, configuration and injectable dependencies of the hub Worker (SPEC §10).

import type { DnsProvider } from "./dns";

export interface Env {
  /** Accounts, sessions, device links, devices, address records, shares, names (migrations/). */
  DB: D1Database;
  /** Static assets from ./public: the ash viewer under /ash/. */
  ASSETS: Fetcher;

  /** `https://darkpyonix.dev` (no trailing slash). */
  PUBLIC_URL: string;
  /** DNS zone for `<name>.<zone>`, e.g. `darkpyonix.dev`. */
  ZONE: string;
  /** The relay devices and guests dial, e.g. `https://relay.darkpyonix.dev/` (SPEC FR-H3). */
  RELAY_URL: string;
  /** Base URL of the relay host's admin API (disconnect on removal), e.g. `https://relay.darkpyonix.dev`. */
  RELAY_ADMIN_URL?: string;

  /** GitHub OAuth App client id (SPEC FR-H6). */
  GITHUB_CLIENT_ID?: string;
  /** Secret. GitHub OAuth App client secret. */
  GITHUB_CLIENT_SECRET?: string;
  /** Optional: comma-separated numeric GitHub user ids allowed to create accounts. Empty = anyone. */
  GITHUB_ALLOWED_IDS?: string;

  /** Secret. Shared by this Worker and the relay host: /internal/v1/relay/* and the relay's admin API. */
  RELAY_SHARED_SECRET?: string;

  /** Secret. Cloudflare API token limited to Zone → DNS → Edit on ZONE (SPEC FR-H5). */
  CF_API_TOKEN?: string;
  /** The Cloudflare zone id of ZONE. */
  CF_ZONE_ID?: string;

  /** Optional Workers rate limiter for unauthenticated writes. */
  WRITE_LIMITER?: { limit(options: { key: string }): Promise<{ success: boolean }> };
}

/** What tests replace: the network, the clock and the DNS provider. */
export interface Deps {
  /** Outbound HTTP (GitHub, the relay admin API). */
  fetch: typeof fetch;
  /** Unix milliseconds. */
  nowMs: () => number;
  /** The DNS provider for ACME TXT records; `null` when none is configured. */
  dns: (env: Env) => DnsProvider | null;
  /** Work that may finish after the response (ctx.waitUntil). */
  waitUntil: (promise: Promise<unknown>) => void;
}
