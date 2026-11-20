// Live hub check (SPEC §10 "살아 있는 허브 점검", #21): walks the device side of the hub
// contract against a running hub, as a throwaway `client` device that removes itself.
//
// Web APIs only (fetch, WebCrypto Ed25519), so the same code runs under Node 22+ and inside
// the Worker test pool. The command line is scripts/run-live-check.mjs.

const Z32_ALPHABET = "ybndrfg8ejkmcpqxot1uwisza345h769";
const encoder = new TextEncoder();

function z32(bytes) {
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

function hex(bytes) {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

function concat(...parts) {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

function sameBytes(a, b) {
  return a.length === b.length && a.every((v, i) => v === b[i]);
}

function base64(bytes) {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}

/** A DNS response holding TXT answers under one name, uncompressed. */
function dnsTxt(name, values, ttl = 30) {
  const labels = concat(
    ...name.split(".").map((label) => concat(new Uint8Array([label.length]), encoder.encode(label))),
    new Uint8Array([0]),
  );
  const header = new Uint8Array([0, 0, 0x84, 0, 0, 0, 0, values.length, 0, 0, 0, 0]);
  const answers = values.map((value) => {
    const text = encoder.encode(value);
    const fixed = new Uint8Array(10);
    const view = new DataView(fixed.buffer);
    view.setUint16(0, 16); // TXT
    view.setUint16(2, 1); // IN
    view.setUint32(4, ttl);
    view.setUint16(8, text.length + 1);
    return concat(labels, fixed, new Uint8Array([text.length]), text);
  });
  return concat(header, ...answers);
}

/** The pkarr relay payload: sig(64) || timestamp µs big-endian (8) || dns, signed over BEP 44. */
async function signedPayload(sign, dns, timestampUs) {
  const signature = await sign(concat(encoder.encode(`3:seqi${timestampUs}e1:v${dns.length}:`), dns));
  const ts = new Uint8Array(8);
  new DataView(ts.buffer).setBigUint64(0, BigInt(timestampUs));
  return concat(signature, ts, dns);
}

class StepFailure extends Error {}

/** The steps, in the order they run. */
export const STEPS = ["config", "link", "approve", "devices", "publish", "resolve", "watch", "remove"];

/**
 * Runs the check against the hub at `baseUrl`.
 *
 * `onLink` is called once with the link (`user_code`, `verification_uri_complete`, ...): the
 * person approves it in a browser while this function polls. Returns
 * `{ ok, steps: [{ step, ok, detail }], endpointId }`; never throws for a failed check.
 */
export async function runLiveCheck(options) {
  const base = options.baseUrl.replace(/\/+$/, "");
  const fetchFn = options.fetch ?? fetch;
  const log = options.log ?? (() => {});
  const sleep = options.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  const nowMs = options.nowMs ?? (() => Date.now());
  const onLink = options.onLink ?? (() => {});

  const keys = await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"]);
  const publicKey = new Uint8Array(await crypto.subtle.exportKey("raw", keys.publicKey));
  const sign = async (message) => new Uint8Array(await crypto.subtle.sign({ name: "Ed25519" }, keys.privateKey, message));
  const endpointId = hex(publicKey);
  const key = z32(publicKey);

  const steps = [];
  let deviceToken = null;
  let removed = false;

  const request = (method, path, init = {}) => {
    const headers = { ...(init.headers ?? {}) };
    if (init.token) headers.authorization = `Bearer ${init.token}`;
    let body = init.body;
    if (init.json !== undefined) {
      headers["content-type"] = "application/json";
      body = JSON.stringify(init.json);
    }
    return fetchFn(`${base}${path}`, { method, headers, body });
  };
  const expectStatus = async (response, status, what) => {
    if (response.status === status) return;
    const text = (await response.text()).slice(0, 200);
    throw new StepFailure(`${what}: expected ${status}, got ${response.status} ${text}`);
  };
  const check = (condition, message) => {
    if (!condition) throw new StepFailure(message);
  };

  const run = {
    async config() {
      const response = await request("GET", "/config");
      await expectStatus(response, 200, "GET /config");
      const config = await response.json();
      check(Array.isArray(config.relay_urls) && config.relay_urls.length === 0, "relay_urls is not []");
      check(typeof config.pkarr_url === "string" && typeof config.link_url === "string", "pkarr_url or link_url is missing");
      run.watchAdvertised = config.pkarr_watch !== undefined && config.pkarr_watch !== null;
      return `pkarr_url ${config.pkarr_url}, watch ${run.watchAdvertised ? "advertised" : "not advertised"}`;
    },

    async link() {
      const created = await request("POST", "/device-links", {
        json: { endpoint_id: endpointId, name: "hub live check", role: "client" },
      });
      await expectStatus(created, 201, "POST /device-links");
      run.linkInfo = await created.json();
      const message = encoder.encode(`darkpyonix-hub/v2/link\n${run.linkInfo.link_id}\n${run.linkInfo.challenge}`);
      run.claim = async () =>
        request("POST", `/device-links/${run.linkInfo.link_id}/token`, { json: { signature: hex(await sign(message)) } });
      await expectStatus(await run.claim(), 202, "token poll before approval");
      return `user code ${run.linkInfo.user_code}`;
    },

    async approve() {
      const link = run.linkInfo;
      await onLink(link);
      const intervalMs = Math.max(1, Number(link.interval) || 5) * 1000;
      for (;;) {
        const response = await run.claim();
        if (response.status === 201) {
          const claimed = await response.json();
          deviceToken = claimed.device_token;
          run.resolveToken = claimed.resolve_token;
          check(typeof deviceToken === "string" && typeof run.resolveToken === "string", "claim has no tokens");
          return "approved, device token received";
        }
        if (response.status !== 202) await expectStatus(response, 201, "token claim");
        check(nowMs() / 1000 < Number(link.expires_at), "the link expired before it was approved");
        await sleep(intervalMs);
      }
    },

    async devices() {
      const response = await request("GET", "/devices", { token: deviceToken });
      await expectStatus(response, 200, "GET /devices");
      const list = (await response.json()).devices ?? [];
      const mine = list.find((d) => d.endpoint_id === endpointId);
      check(mine !== undefined, "this device is not in the account's device list");
      check(mine.role === "client", `role is ${mine.role}, expected client`);
      return `${list.length} device(s) in the account`;
    },

    async publish() {
      run.relay = "https://live-check.invalid/";
      run.addr = "192.0.2.1:9";
      run.timestampUs = Math.floor(nowMs()) * 1000;
      const dns = dnsTxt(`_iroh.${key}`, [`relay=${run.relay}`, `addr=${run.addr}`]);
      run.payload = await signedPayload(sign, dns, run.timestampUs);
      await expectStatus(await request("PUT", `/pkarr/${key}`, { body: run.payload }), 204, "PUT /pkarr");
      await expectStatus(await request("PUT", `/pkarr/${key}`, { body: run.payload }), 409, "PUT /pkarr of the same packet");
      return `${run.payload.length} bytes at timestamp ${run.timestampUs}`;
    },

    async resolve() {
      const stored = await request("GET", `/pkarr/${key}?token=${encodeURIComponent(run.resolveToken)}`);
      await expectStatus(stored, 200, "GET /pkarr with the resolve token");
      check(sameBytes(new Uint8Array(await stored.arrayBuffer()), run.payload), "the hub returned other bytes than were published");
      await expectStatus(await request("GET", `/pkarr/${key}`), 401, "GET /pkarr without a token");
      const addresses = await request("GET", `/devices/${endpointId}/addresses`, { token: deviceToken });
      await expectStatus(addresses, 200, "GET /devices/{endpoint_id}/addresses");
      const decoded = await addresses.json();
      check(JSON.stringify(decoded.relay_urls) === JSON.stringify([run.relay]), `relay_urls is ${JSON.stringify(decoded.relay_urls)}`);
      check(
        JSON.stringify(decoded.direct_addresses) === JSON.stringify([run.addr]),
        `direct_addresses is ${JSON.stringify(decoded.direct_addresses)}`,
      );
      return "byte for byte, and decoded as JSON";
    },

    async watch() {
      if (!run.watchAdvertised) return "skipped: /config advertises no pkarr_watch";
      const changed = await request("GET", `/pkarr/watch?keys=${key}&since=0&wait=0`, { token: deviceToken });
      await expectStatus(changed, 200, "GET /pkarr/watch since=0");
      const records = (await changed.json()).records ?? [];
      check(records.length === 1 && records[0].key === key, "the watch did not return this device's record");
      check(records[0].timestamp === run.timestampUs, `watch timestamp is ${records[0].timestamp}`);
      check(records[0].packet === base64(run.payload), "the watch returned another packet");
      const idle = await request("GET", `/pkarr/watch?keys=${key}&since=${run.timestampUs}&wait=1`, { token: deviceToken });
      await expectStatus(idle, 204, "GET /pkarr/watch with nothing newer");
      return "changed record at once, 204 when nothing is newer";
    },

    async remove() {
      await expectStatus(await request("DELETE", `/devices/${endpointId}`, { token: deviceToken }), 204, "DELETE own device");
      removed = true;
      const after = await request("GET", "/devices", { token: deviceToken });
      await expectStatus(after, 401, "GET /devices with the removed device's token");
      const body = await after.json().catch(() => ({}));
      check(body.code === "device_removed", `401 code is ${body.code}, expected device_removed`);
      return "device removed, its token answers 401 device_removed";
    },
  };

  let ok = true;
  for (const step of STEPS) {
    try {
      const detail = await run[step]();
      steps.push({ step, ok: true, detail });
      log(`ok    ${step}: ${detail}`);
    } catch (error) {
      ok = false;
      const detail = error instanceof Error ? error.message : String(error);
      steps.push({ step, ok: false, detail });
      log(`FAIL  ${step}: ${detail}`);
      break;
    }
  }

  // A failed run must not leave its device in the account.
  if (deviceToken !== null && !removed) {
    try {
      const response = await request("DELETE", `/devices/${endpointId}`, { token: deviceToken });
      log(response.status === 204 ? "cleanup: device removed" : `cleanup: DELETE answered ${response.status}; remove ${endpointId} by hand`);
    } catch (error) {
      log(`cleanup: could not remove ${endpointId}: ${error instanceof Error ? error.message : error}`);
    }
  }
  return { ok, steps, endpointId };
}
