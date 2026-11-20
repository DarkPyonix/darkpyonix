// SPEC §10 "살아 있는 허브 점검" (#21): the live hub check passes against this Worker and
// reports the step where a hub breaks the contract.

import { describe, expect, it } from "vitest";
import { handle } from "../src/app";
import { runLiveCheck, STEPS } from "../scripts/live-check.mjs";
import { call, hubEnv, makeDeps, signIn } from "./helpers";

const BASE = "https://api.darkpyonix.dev";

type Deps = ReturnType<typeof makeDeps>;
type Fetch = (url: string, init?: RequestInit) => Promise<Response>;

/** The script's `fetch`, answered by the Worker in this process. */
function workerFetch(deps: Deps): Fetch {
  return (url, init = {}) => {
    const headers = new Headers(init.headers);
    headers.set("cf-connecting-ip", `live-${crypto.randomUUID()}`);
    return handle(new Request(url, { ...init, headers }), hubEnv, deps);
  };
}

async function setup(githubId: number) {
  const deps = makeDeps({ sleep: async () => {} });
  const cookie = await signIn(deps, { id: githubId, login: `owner${githubId}` });
  const lines: string[] = [];
  const approve = async (link: { user_code: string }) => {
    const decided = await call(deps, "POST", `/link-codes/${link.user_code}`, { cookie, json: { approve: true } });
    expect(decided.status).toBe(204);
  };
  const deviceIds = async () => {
    const response = await call(deps, "GET", "/devices", { cookie });
    return ((await response.json()) as { devices: { endpoint_id: string }[] }).devices.map((d) => d.endpoint_id);
  };
  return { deps, lines, approve, deviceIds };
}

describe("live hub check", () => {
  it("test_hub_live_check_passes_against_the_worker", async () => {
    const { deps, lines, approve, deviceIds } = await setup(951);
    const result = await runLiveCheck({
      baseUrl: `${BASE}/`,
      fetch: workerFetch(deps),
      log: (line: string) => lines.push(line),
      sleep: async () => {},
      onLink: approve,
    });
    expect(result.steps.filter((s) => !s.ok)).toEqual([]);
    expect(result.ok).toBe(true);
    expect(result.steps.map((s) => s.step)).toEqual(STEPS);
    expect(lines.filter((l) => l.startsWith("ok "))).toHaveLength(STEPS.length);
    // The throwaway device is gone from the account.
    expect(await deviceIds()).toEqual([]);
  });

  it("test_hub_live_check_stops_at_the_failing_step_and_removes_its_device", async () => {
    const { deps, lines, approve, deviceIds } = await setup(952);
    const honest = workerFetch(deps);
    // A hub that serves other bytes than were published.
    const corrupting: Fetch = async (url, init) => {
      const response = await honest(url, init);
      if ((init?.method ?? "GET") !== "GET" || !/\/pkarr\/[a-z0-9]{52}\?token=/.test(url) || response.status !== 200) {
        return response;
      }
      const bytes = new Uint8Array(await response.arrayBuffer());
      bytes[bytes.length - 1] ^= 1;
      return new Response(bytes, { status: 200, headers: response.headers });
    };
    const result = await runLiveCheck({
      baseUrl: BASE,
      fetch: corrupting,
      log: (line: string) => lines.push(line),
      sleep: async () => {},
      onLink: approve,
    });
    expect(result.ok).toBe(false);
    const failed = result.steps[result.steps.length - 1];
    expect(failed).toMatchObject({ step: "resolve", ok: false });
    expect(failed.detail).toContain("other bytes");
    expect(result.steps.slice(0, -1).every((s) => s.ok)).toBe(true);
    expect(result.steps.map((s) => s.step)).toEqual(STEPS.slice(0, STEPS.indexOf("resolve") + 1));
    expect(lines).toContain("cleanup: device removed");
    expect(await deviceIds()).toEqual([]);
  });
});
