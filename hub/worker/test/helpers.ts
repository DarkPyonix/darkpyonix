// Test helpers: a fake GitHub, iroh-like device keys, signed pkarr packets, requests.

import { env } from "cloudflare:test";
import { handle } from "../src/app";
import { MemoryDns } from "../src/dns";
import type { Deps, Env } from "../src/env";
import { z32Encode } from "../src/pkarr";
import { base64url, concatBytes, sha256, toHex, utf8 } from "../src/util";

export const ORIGIN = "https://darkpyonix.dev";
export const hubEnv = env as unknown as Env;

// ---------------------------------------------------------------- fake GitHub

export interface FakeGitHub {
  /** code -> (PKCE challenge, user) issued by the fake authorize step. */
  codes: Map<string, { challenge: string; user: { id: number; login: string } }>;
  /** Access tokens the hub asked GitHub to revoke. */
  revoked: string[];
  /** Calls the hub made, as "METHOD url". */
  calls: string[];
  /** Calls to the relay admin API. */
  relayCalls: { url: string; body: unknown }[];
}

export function makeDeps(overrides: Partial<Deps> = {}): Deps & { github: FakeGitHub; memoryDns: MemoryDns; pending: Promise<unknown>[] } {
  const github: FakeGitHub = { codes: new Map(), revoked: [], calls: [], relayCalls: [] };
  const tokens = new Map<string, { id: number; login: string }>();
  const memoryDns = new MemoryDns();
  const pending: Promise<unknown>[] = [];
  const fakeFetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init);
    const url = request.url;
    github.calls.push(`${request.method} ${url}`);
    if (url === "https://github.com/login/oauth/access_token" && request.method === "POST") {
      const form = new URLSearchParams(new TextDecoder().decode(await request.arrayBuffer()));
      const issued = github.codes.get(form.get("code") ?? "");
      if (!issued) return Response.json({ error: "bad_verification_code" });
      const verifier = form.get("code_verifier") ?? "";
      if (base64url(await sha256(verifier)) !== issued.challenge) {
        return Response.json({ error: "invalid_grant" });
      }
      if (form.get("client_secret") !== "test-secret") return Response.json({ error: "incorrect_client_credentials" });
      github.codes.delete(form.get("code") ?? "");
      const token = `gho_${crypto.randomUUID()}`;
      tokens.set(token, issued.user);
      return Response.json({ access_token: token, token_type: "bearer", scope: "" });
    }
    if (url === "https://api.github.com/user") {
      const token = (request.headers.get("authorization") ?? "").replace(/^Bearer /, "");
      const user = tokens.get(token);
      return user ? Response.json({ id: user.id, login: user.login }) : new Response("{}", { status: 401 });
    }
    if (url.startsWith("https://api.github.com/applications/") && request.method === "DELETE") {
      const body = (await request.json()) as { access_token: string };
      github.revoked.push(body.access_token);
      return new Response(null, { status: 204 });
    }
    if (url.startsWith("https://relay.darkpyonix.dev/admin/")) {
      github.relayCalls.push({ url, body: await request.json() });
      return new Response(null, { status: 204 });
    }
    return new Response("unexpected fetch in test", { status: 599 });
  };
  return {
    fetch: fakeFetch as typeof fetch,
    nowMs: () => Date.now(),
    dns: () => memoryDns,
    waitUntil: (p) => {
      pending.push(p);
    },
    github,
    memoryDns,
    pending,
    ...overrides,
  };
}

// ---------------------------------------------------------------- requests

export interface Call {
  token?: string;
  cookie?: string;
  origin?: string | null;
  json?: unknown;
  body?: BodyInit;
  /** Bindings to override for this call (e.g. a var the test config leaves unset). */
  env?: Partial<Env>;
}

export function call(deps: Deps, method: string, path: string, c: Call = {}): Promise<Response> {
  const headers = new Headers();
  // A fresh client address per call keeps the write rate limiter out of the way of tests.
  headers.set("cf-connecting-ip", `test-${crypto.randomUUID()}`);
  if (c.token) headers.set("authorization", `Bearer ${c.token}`);
  if (c.cookie) headers.set("cookie", c.cookie);
  // Browsers send Origin on POST/PUT/DELETE; default to ours when a cookie is used.
  const origin = c.origin === undefined ? (c.cookie ? ORIGIN : null) : c.origin;
  if (origin) headers.set("origin", origin);
  let body = c.body;
  if (c.json !== undefined) {
    headers.set("content-type", "application/json");
    body = JSON.stringify(c.json);
  }
  // Define (not assign) the overrides: assigning would go through `env`'s own setter and leak.
  const env = c.env ? (Object.create(hubEnv, Object.getOwnPropertyDescriptors(c.env)) as Env) : hubEnv;
  return handle(new Request(`${ORIGIN}${path}`, { method, headers, body }), env, deps);
}

export function cookieValue(response: Response, name: string): string | null {
  for (const c of response.headers.getSetCookie()) {
    const [pair] = c.split(";");
    const [k, ...v] = pair.split("=");
    if (k === name) return v.join("=");
  }
  return null;
}

/** Signs in through the real /auth/login and /auth/callback against the fake GitHub. Returns the cookie header. */
export async function signIn(
  deps: ReturnType<typeof makeDeps>,
  user: { id: number; login: string },
): Promise<string> {
  const start = await call(deps, "GET", "/auth/login?return_to=/link");
  const location = new URL(start.headers.get("location")!);
  const state = location.searchParams.get("state")!;
  const challenge = location.searchParams.get("code_challenge")!;
  const code = `code-${crypto.randomUUID()}`;
  deps.github.codes.set(code, { challenge, user });
  const stateCookie = `__Host-dp_oauth=${cookieValue(start, "__Host-dp_oauth")}`;
  const done = await call(deps, "GET", `/auth/callback?code=${code}&state=${state}`, { cookie: stateCookie });
  if (done.status !== 302) throw new Error(`sign-in failed: ${done.status} ${await done.text()}`);
  return `__Host-dp_session=${cookieValue(done, "__Host-dp_session")}`;
}

// ---------------------------------------------------------------- iroh-like devices

export interface Device {
  key: CryptoKeyPair;
  publicKey: Uint8Array;
  endpointId: string;
  z32: string;
  sign(message: Uint8Array): Promise<Uint8Array>;
}

export async function newDevice(): Promise<Device> {
  const key = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"])) as CryptoKeyPair;
  const publicKey = new Uint8Array((await crypto.subtle.exportKey("raw", key.publicKey)) as ArrayBuffer);
  return {
    key,
    publicKey,
    endpointId: toHex(publicKey),
    z32: z32Encode(publicKey),
    sign: async (message) => new Uint8Array(await crypto.subtle.sign({ name: "Ed25519" }, key.privateKey, message)),
  };
}

/** Runs the whole device-link flow; `approver` is a session cookie or a main server's device token. */
export async function linkDevice(
  deps: Deps,
  approver: { cookie?: string; token?: string },
  device: Device,
  role: Role = "computer",
  name = "test device",
): Promise<string> {
  return (await linkDeviceTokens(deps, approver, device, role, name)).device_token;
}

export type Role = "main_server" | "computer";

/** The device-link flow, returning both tokens of the claim. */
export async function linkDeviceTokens(
  deps: Deps,
  approver: { cookie?: string; token?: string },
  device: Device,
  role: Role = "computer",
  name = "test device",
): Promise<{ device_token: string; resolve_token: string }> {
  const created = await call(deps, "POST", "/v1/device-links", {
    json: { endpoint_id: device.endpointId, name, role },
  });
  if (created.status !== 201) throw new Error(`link: ${created.status} ${await created.text()}`);
  const link = (await created.json()) as { link_id: string; user_code: string; challenge: string };
  const decided = await call(deps, "POST", `/v1/link-codes/${link.user_code}`, { ...approver, json: { approve: true } });
  if (decided.status !== 204) throw new Error(`approve: ${decided.status} ${await decided.text()}`);
  const signature = toHex(await device.sign(utf8(`darkpyonix-hub/v2/link\n${link.link_id}\n${link.challenge}`)));
  const claimed = await call(deps, "POST", `/v1/device-links/${link.link_id}/token`, { json: { signature } });
  if (claimed.status !== 201) throw new Error(`claim: ${claimed.status} ${await claimed.text()}`);
  return (await claimed.json()) as { device_token: string; resolve_token: string };
}

// ---------------------------------------------------------------- pkarr packets

function encodeName(name: string): Uint8Array {
  const parts: Uint8Array[] = [];
  for (const label of name.split(".")) {
    const bytes = utf8(label);
    parts.push(new Uint8Array([bytes.length]), bytes);
  }
  parts.push(new Uint8Array([0]));
  return concatBytes(...parts);
}

/** A DNS response with TXT answers (uncompressed names), like iroh's `_iroh` records. */
export function dnsTxtPacket(records: { name: string; txt: string }[], ttl = 30): Uint8Array {
  const header = new Uint8Array([0, 0, 0x84, 0, 0, 0, 0, records.length, 0, 0, 0, 0]);
  const answers = records.map(({ name, txt }) => {
    const value = utf8(txt);
    const rdata = concatBytes(new Uint8Array([value.length]), value);
    const fixed = new Uint8Array(10);
    const view = new DataView(fixed.buffer);
    view.setUint16(0, 16); // TXT
    view.setUint16(2, 1); // IN
    view.setUint32(4, ttl);
    view.setUint16(8, rdata.length);
    return concatBytes(encodeName(name), fixed, rdata);
  });
  return concatBytes(header, ...answers);
}

/** iroh's relay payload for `device`: sig(64) || ts_us(8) || dns, signed over the BEP 44 form. */
export async function signedPayload(device: Device, dns: Uint8Array, timestampUs: bigint): Promise<Uint8Array> {
  const signable = concatBytes(utf8(`3:seqi${timestampUs}e1:v${dns.length}:`), dns);
  const signature = await device.sign(signable);
  const ts = new Uint8Array(8);
  new DataView(ts.buffer).setBigUint64(0, timestampUs);
  return concatBytes(signature, ts, dns);
}

export async function irohRecord(device: Device, relay: string, addrs: string[], timestampUs: bigint): Promise<Uint8Array> {
  const name = `_iroh.${device.z32}`;
  const dns = dnsTxtPacket([{ name, txt: `relay=${relay}` }, ...addrs.map((a) => ({ name, txt: `addr=${a}` }))]);
  return signedPayload(device, dns, timestampUs);
}
