// iroh's pkarr relay protocol (SPEC FR-H2), compatible with iroh-dns 1.3 `SignedPacket`:
//
//   stored packet  = public key (32) || signature (64) || timestamp_us big-endian (8) || DNS packet
//   relay payload  = everything after the public key (what `PUT`/`GET /pkarr/<z32>` carry)
//   signed message = "3:seqi<timestamp_us>e1:v<len>:" || DNS packet   (BEP 44 bencoding)
//
// The DNS packet is at most 1000 bytes. iroh puts its addresses in TXT records named
// `_iroh.<z32 key>` with values `relay=<url>` and `addr=<ip:port>`.

import { verifyEd25519 } from "./ed25519";
import { concatBytes, utf8 } from "./util";

export const MAX_DNS_PACKET = 1000;
const PAYLOAD_HEADER = 64 + 8;

const Z32_ALPHABET = "ybndrfg8ejkmcpqxot1uwisza345h769";
const Z32_INDEX: Record<string, number> = Object.fromEntries(
  [...Z32_ALPHABET].map((c, i) => [c, i]),
);

/** z-base-32 encoding, most significant bit first, no padding. 32 bytes give 52 characters. */
export function z32Encode(bytes: Uint8Array): string {
  let out = "";
  let buffer = 0;
  let bits = 0;
  for (const b of bytes) {
    buffer = (buffer << 8) | b;
    bits += 8;
    while (bits >= 5) {
      out += Z32_ALPHABET[(buffer >>> (bits - 5)) & 31];
      bits -= 5;
    }
    buffer &= (1 << bits) - 1;
  }
  if (bits > 0) out += Z32_ALPHABET[(buffer << (5 - bits)) & 31];
  return out;
}

/** Decodes a 52-character z-base-32 public key to 32 bytes, or null. */
export function z32DecodeKey(s: string): Uint8Array | null {
  if (s.length !== 52) return null;
  const out = new Uint8Array(32);
  let buffer = 0;
  let bits = 0;
  let i = 0;
  for (const c of s) {
    const v = Z32_INDEX[c];
    if (v === undefined) return null;
    buffer = (buffer << 5) | v;
    bits += 5;
    if (bits >= 8) {
      out[i++] = (buffer >>> (bits - 8)) & 0xff;
      bits -= 8;
      buffer &= (1 << bits) - 1;
    }
  }
  // 52 * 5 = 260 bits: the last 4 bits are padding and must be zero (canonical form).
  if (i !== 32 || buffer !== 0) return null;
  return out;
}

export interface SignedPayload {
  signature: Uint8Array;
  timestampUs: bigint;
  dns: Uint8Array;
}

/** Splits a relay payload. Null if its size is out of range. */
export function splitRelayPayload(payload: Uint8Array): SignedPayload | null {
  if (payload.length < PAYLOAD_HEADER || payload.length > PAYLOAD_HEADER + MAX_DNS_PACKET) {
    return null;
  }
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  return {
    signature: payload.slice(0, 64),
    timestampUs: view.getBigUint64(64, false),
    dns: payload.slice(PAYLOAD_HEADER),
  };
}

export function signable(timestampUs: bigint, dns: Uint8Array): Uint8Array {
  return concatBytes(utf8(`3:seqi${timestampUs.toString()}e1:v${dns.length}:`), dns);
}

/**
 * Verifies a relay payload for `publicKey`: size, signature, and that the DNS packet parses
 * (iroh-dns `SignedPacket::from_relay_payload`). Returns the parts, or null.
 */
export async function verifyRelayPayload(
  publicKey: Uint8Array,
  payload: Uint8Array,
): Promise<(SignedPayload & { answers: DnsAnswer[] }) | null> {
  const parts = splitRelayPayload(payload);
  if (!parts) return null;
  if (!(await verifyEd25519(publicKey, signable(parts.timestampUs, parts.dns), parts.signature))) {
    return null;
  }
  const answers = parseDnsAnswers(parts.dns);
  if (!answers) return null;
  return { ...parts, answers };
}

// ---------------------------------------------------------------- DNS wire format

export interface DnsAnswer {
  /** Lowercase, dot-separated, without the trailing dot. */
  name: string;
  type: number;
  ttl: number;
  /** For TXT records: the character-strings joined. */
  txt?: string;
}

const TYPE_TXT = 16;

class Reader {
  offset = 0;
  constructor(readonly bytes: Uint8Array) {}
  need(n: number): void {
    if (this.offset + n > this.bytes.length) throw new Error("truncated");
  }
  u8(): number {
    this.need(1);
    return this.bytes[this.offset++];
  }
  u16(): number {
    this.need(2);
    const v = (this.bytes[this.offset] << 8) | this.bytes[this.offset + 1];
    this.offset += 2;
    return v;
  }
  u32(): number {
    this.need(4);
    const v =
      this.bytes[this.offset] * 0x1000000 +
      ((this.bytes[this.offset + 1] << 16) |
        (this.bytes[this.offset + 2] << 8) |
        this.bytes[this.offset + 3]);
    this.offset += 4;
    return v;
  }
  skip(n: number): void {
    this.need(n);
    this.offset += n;
  }
}

/** Reads a (possibly compressed) domain name starting at `start`; returns it and the end offset. */
function readName(bytes: Uint8Array, start: number): { name: string; end: number } {
  const labels: string[] = [];
  let offset = start;
  let end = -1;
  let jumps = 0;
  for (;;) {
    if (offset >= bytes.length) throw new Error("truncated name");
    const len = bytes[offset];
    if ((len & 0xc0) === 0xc0) {
      if (offset + 1 >= bytes.length) throw new Error("truncated pointer");
      if (++jumps > 32) throw new Error("pointer loop");
      if (end < 0) end = offset + 2;
      offset = ((len & 0x3f) << 8) | bytes[offset + 1];
      continue;
    }
    if ((len & 0xc0) !== 0) throw new Error("bad label");
    if (len === 0) {
      if (end < 0) end = offset + 1;
      break;
    }
    if (offset + 1 + len > bytes.length) throw new Error("truncated label");
    let label = "";
    for (const b of bytes.subarray(offset + 1, offset + 1 + len)) label += String.fromCharCode(b);
    labels.push(label.toLowerCase());
    offset += 1 + len;
  }
  return { name: labels.join("."), end };
}

/** Parses the answer section of a DNS message. Null if the message is malformed. */
export function parseDnsAnswers(bytes: Uint8Array): DnsAnswer[] | null {
  try {
    const r = new Reader(bytes);
    r.skip(4); // id, flags
    const qd = r.u16();
    const an = r.u16();
    r.skip(4); // ns, ar counts (not needed; trailing sections are not parsed)
    for (let i = 0; i < qd; i++) {
      r.offset = readName(bytes, r.offset).end;
      r.skip(4);
    }
    const answers: DnsAnswer[] = [];
    for (let i = 0; i < an; i++) {
      const { name, end } = readName(bytes, r.offset);
      r.offset = end;
      const type = r.u16();
      r.skip(2); // class
      const ttl = r.u32();
      const rdlen = r.u16();
      r.need(rdlen);
      const rdata = bytes.subarray(r.offset, r.offset + rdlen);
      r.offset += rdlen;
      const answer: DnsAnswer = { name, type, ttl };
      if (type === TYPE_TXT) {
        let txt = "";
        let o = 0;
        while (o < rdata.length) {
          const n = rdata[o];
          if (o + 1 + n > rdata.length) throw new Error("truncated txt");
          txt += new TextDecoder("utf-8", { fatal: true }).decode(rdata.subarray(o + 1, o + 1 + n));
          o += 1 + n;
        }
        answer.txt = txt;
      }
      answers.push(answer);
    }
    return answers;
  } catch {
    return null;
  }
}

export interface EndpointAddresses {
  relayUrls: string[];
  directAddresses: string[];
}

/**
 * iroh's `_iroh` TXT attributes (iroh-dns `EndpointInfo::from_pkarr_signed_packet`).
 * Like iroh, a value is the text between the first and the second `=`.
 */
export function endpointAddresses(z32Key: string, answers: DnsAnswer[]): EndpointAddresses {
  const name = `_iroh.${z32Key}`;
  const relayUrls: string[] = [];
  const directAddresses: string[] = [];
  for (const a of answers) {
    if (a.txt === undefined || (a.name !== name && a.name !== "_iroh")) continue;
    const [key, value] = a.txt.split("=");
    if (value === undefined) continue;
    if (key === "relay") relayUrls.push(value);
    else if (key === "addr") directAddresses.push(value);
  }
  return { relayUrls, directAddresses };
}
