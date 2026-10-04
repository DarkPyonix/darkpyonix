// Encoding, hashing, tokens and input validation.

const encoder = new TextEncoder();

export function utf8(s: string): Uint8Array {
  return encoder.encode(s);
}

export function concatBytes(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

export function toHex(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += b.toString(16).padStart(2, "0");
  return s;
}

/** Lowercase hex of exactly `len` characters, or null. */
export function fromHex(s: string, len?: number): Uint8Array | null {
  if (len !== undefined && s.length !== len) return null;
  if (s.length % 2 !== 0 || !/^[0-9a-f]*$/.test(s)) return null;
  const out = new Uint8Array(s.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(s.slice(i * 2, i * 2 + 2), 16);
  return out;
}

export function base64url(bytes: Uint8Array): string {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function fromBase64url(s: string): Uint8Array | null {
  if (!/^[A-Za-z0-9_-]*$/.test(s)) return null;
  const padded = s.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((s.length + 3) % 4);
  try {
    const bin = atob(padded);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  } catch {
    return null;
  }
}

export function randomBytes(n: number): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(n));
}

export function randomHex(n: number): string {
  return toHex(randomBytes(n));
}

/** A bearer token: prefix + 32 random bytes in base64url. */
export function newToken(prefix: string): string {
  return prefix + base64url(randomBytes(32));
}

export async function sha256(data: Uint8Array | string): Promise<Uint8Array> {
  const bytes = typeof data === "string" ? utf8(data) : data;
  return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
}

/** Tokens and session ids are stored as the hex SHA-256 of the string. */
export async function hashToken(token: string): Promise<string> {
  return toHex(await sha256(token));
}

/** Constant-time string comparison (length is not hidden). */
export function ctEq(a: string, b: string): boolean {
  const x = utf8(a);
  const y = utf8(b);
  if (x.length !== y.length) return false;
  let diff = 0;
  for (let i = 0; i < x.length; i++) diff |= x[i] ^ y[i];
  return diff === 0;
}

export function nowSecs(nowMs: number): number {
  return Math.floor(nowMs / 1000);
}

// ---------------------------------------------------------------- validation

export const ENDPOINT_ID_RE = /^[0-9a-f]{64}$/;
export const SHARE_ID_RE = /^s_[0-9a-f]{16}$/;
export const ACME_VALUE_RE = /^[A-Za-z0-9_-]{43}$/;
const NAME_RE = /^[a-z0-9]([a-z0-9-]{1,30}[a-z0-9])$/;

/** Labels that stay with the hub itself (SPEC FR-H5). */
export const RESERVED_NAMES = new Set([
  "www", "api", "relay", "ash", "hub", "dns", "ns1", "ns2", "mail", "admin", "docs", "status",
  "auth", "link", "qad",
]);

export function isValidName(s: string): boolean {
  return NAME_RE.test(s) && !RESERVED_NAMES.has(s);
}

/** RFC 8628-style user code alphabet: no vowels, no look-alikes. */
const USER_CODE_ALPHABET = "BCDFGHJKLMNPQRSTVWXZ";
export const USER_CODE_RE = /^[BCDFGHJKLMNPQRSTVWXZ]{4}-[BCDFGHJKLMNPQRSTVWXZ]{4}$/;

export function newUserCode(): string {
  // 20 symbols; rejection sampling keeps the distribution uniform.
  const out: string[] = [];
  while (out.length < 8) {
    for (const b of randomBytes(16)) {
      if (b < 240 && out.length < 8) out.push(USER_CODE_ALPHABET[b % 20]);
    }
  }
  return `${out.slice(0, 4).join("")}-${out.slice(4).join("")}`;
}

/** Accepts `bcdf ghjk`, `BCDFGHJK`, `BCDF-GHJK` and returns the canonical form or null. */
export function normalizeUserCode(s: string): string | null {
  const compact = s.toUpperCase().replace(/[\s-]/g, "");
  if (compact.length !== 8) return null;
  const code = `${compact.slice(0, 4)}-${compact.slice(4)}`;
  return USER_CODE_RE.test(code) ? code : null;
}

/** A same-origin path to return to after login: `/x`, never `//host` or `/\host`. */
export function safeReturnTo(s: string | null | undefined): string {
  if (!s || !s.startsWith("/") || s.startsWith("//") || s.includes("\\")) return "/";
  return s;
}
