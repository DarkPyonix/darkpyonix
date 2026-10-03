// CLAUDE.md: the OpenAPI file is the SPEC. Every operation this Worker serves answers, with no
// credentials and an empty body, only with a status its documentation lists; and every route
// the Worker has is documented. Operations with a path-level `servers` entry live on the relay
// host (hub/server) and are checked there.

import { describe, expect, it } from "vitest";
import { parse } from "yaml";
import specText from "../../../docs/api/hub.openapi.yaml?raw";
import { ROUTES } from "../src/app";
import { z32Encode } from "../src/pkarr";
import { call, makeDeps } from "./helpers";

type Operation = { responses: Record<string, unknown> };
type PathItem = Record<string, Operation> & { servers?: unknown };

const spec = parse(specText) as { paths: Record<string, PathItem> };
const METHODS = ["get", "put", "post", "patch", "delete"] as const;

function workerPaths(): [string, PathItem][] {
  return Object.entries(spec.paths).filter(([, item]) => item.servers === undefined);
}

function fill(path: string): string {
  return path
    .replace("{endpoint_id}", "ab".repeat(32))
    .replace("{key}", z32Encode(new Uint8Array(32).fill(7)))
    .replace("{share_id}", "s_0123456789abcdef")
    .replace("{name}", "example-name")
    .replace("{link_id}", `l_${"0".repeat(32)}`)
    .replace("{user_code}", "BCDF-GHJK");
}

describe("hub.openapi.yaml", () => {
  it("test_hub_every_operation_answers_with_a_documented_status", async () => {
    const deps = makeDeps();
    let checked = 0;
    for (const [path, item] of workerPaths()) {
      for (const method of METHODS) {
        const op = item[method];
        if (!op) continue;
        const documented = Object.keys(op.responses).map(Number);
        const response = await call(deps, method.toUpperCase(), fill(path));
        expect(documented, `${method.toUpperCase()} ${path} answered ${response.status}`).toContain(response.status);
        checked++;
      }
    }
    expect(checked).toBeGreaterThanOrEqual(25);
  });

  it("test_hub_every_worker_route_is_documented_and_vice_versa", () => {
    const documented = new Set<string>();
    for (const [path, item] of workerPaths()) {
      for (const method of METHODS) if (item[method]) documented.add(`${method.toUpperCase()} ${path}`);
    }
    const served = new Set<string>();
    for (const route of ROUTES) {
      for (const method of Object.keys(route.methods)) served.add(`${method} ${route.path}`);
    }
    expect([...served].sort()).toEqual([...documented].sort());
  });
});
