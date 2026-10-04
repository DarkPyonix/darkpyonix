// FR-H2: the pkarr relay payload format of iroh-dns 1.3, reimplemented in TypeScript.

import { describe, expect, it } from "vitest";
import {
  endpointAddresses,
  parseDnsAnswers,
  splitRelayPayload,
  verifyRelayPayload,
  z32DecodeKey,
  z32Encode,
} from "../src/pkarr";
import { concatBytes, utf8 } from "../src/util";
import { dnsTxtPacket, irohRecord, newDevice, signedPayload } from "./helpers";

describe("z-base-32 keys", () => {
  it("test_fr_h2_z32_round_trips_a_published_pkarr_key", () => {
    // A key from the pkarr README.
    const key = "o4dksfbqk85ogzdb5osziw6befigbuxmuxkuxq8434q89uj56uyy";
    const bytes = z32DecodeKey(key);
    expect(bytes).not.toBeNull();
    expect(z32Encode(bytes!)).toBe(key);
  });

  it("test_fr_h2_z32_rejects_bad_length_alphabet_and_padding", () => {
    expect(z32DecodeKey("o4dksfbqk85ogzdb5osziw6befigbuxmuxkuxq8434q89uj56uy")).toBeNull();
    expect(z32DecodeKey("O4dksfbqk85ogzdb5osziw6befigbuxmuxkuxq8434q89uj56uyy")).toBeNull();
    expect(z32DecodeKey("o4dksfbqk85ogzdb5osziw6befigbuxmuxkuxq8434q89uj56uyb")).toBeNull(); // nonzero pad bits
  });

  it("test_fr_h2_z32_of_zero_and_ff_keys", () => {
    expect(z32Encode(new Uint8Array(32))).toBe("y".repeat(52));
    const ff = new Uint8Array(32).fill(0xff);
    const encoded = z32Encode(ff);
    expect(encoded).toHaveLength(52);
    expect(z32DecodeKey(encoded)).toEqual(ff);
  });
});

describe("signed packets", () => {
  it("test_fr_h2_verifies_an_iroh_style_record_and_decodes_addresses", async () => {
    const device = await newDevice();
    const payload = await irohRecord(device, "https://relay.darkpyonix.dev/", ["192.0.2.1:4433", "[2001:db8::1]:4433"], 1_790_000_000_000_000n);
    const verified = await verifyRelayPayload(device.publicKey, payload);
    expect(verified).not.toBeNull();
    expect(verified!.timestampUs).toBe(1_790_000_000_000_000n);
    expect(endpointAddresses(device.z32, verified!.answers)).toEqual({
      relayUrls: ["https://relay.darkpyonix.dev/"],
      directAddresses: ["192.0.2.1:4433", "[2001:db8::1]:4433"],
    });
  });

  it("test_fr_h2_rejects_a_tampered_or_foreign_packet", async () => {
    const device = await newDevice();
    const other = await newDevice();
    const payload = await irohRecord(device, "https://relay.darkpyonix.dev/", [], 1n);
    const tampered = payload.slice();
    tampered[tampered.length - 1] ^= 1;
    expect(await verifyRelayPayload(device.publicKey, tampered)).toBeNull();
    expect(await verifyRelayPayload(other.publicKey, payload)).toBeNull();
  });

  it("test_fr_h2_rejects_size_out_of_range_and_unparseable_dns", async () => {
    const device = await newDevice();
    expect(splitRelayPayload(new Uint8Array(71))).toBeNull();
    expect(splitRelayPayload(new Uint8Array(72 + 1001))).toBeNull();
    // Validly signed, but not a DNS message.
    const junk = await signedPayload(device, utf8("not dns"), 5n);
    expect(await verifyRelayPayload(device.publicKey, junk)).toBeNull();
  });

  it("test_fr_h2_dns_parser_follows_compression_pointers", () => {
    const first = dnsTxtPacket([{ name: "_iroh.abc", txt: "relay=https://r/" }]);
    // Second answer: name is a pointer to offset 12 (the first answer's name).
    const value = utf8("addr=192.0.2.9:1");
    const second = concatBytes(
      new Uint8Array([0xc0, 12, 0, 16, 0, 1, 0, 0, 0, 30, 0, value.length + 1, value.length]),
      value,
    );
    const packet = concatBytes(first, second);
    packet[7] = 2; // ancount
    const answers = parseDnsAnswers(packet);
    expect(answers?.map((a) => [a.name, a.txt])).toEqual([
      ["_iroh.abc", "relay=https://r/"],
      ["_iroh.abc", "addr=192.0.2.9:1"],
    ]);
  });

  it("test_fr_h2_dns_parser_rejects_pointer_loops_and_truncation", () => {
    const loop = new Uint8Array([0, 0, 0x84, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0xc0, 12]);
    expect(parseDnsAnswers(loop)).toBeNull();
    const truncated = dnsTxtPacket([{ name: "_iroh.abc", txt: "relay=x" }]).slice(0, -3);
    expect(parseDnsAnswers(truncated)).toBeNull();
  });
});
