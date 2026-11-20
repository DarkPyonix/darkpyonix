#!/usr/bin/env node
// Command line of the live hub check (SPEC §10, #21):
//   npm run check:live -- [--url https://api.darkpyonix.dev]
// Exit code 0 when every step passes, 1 when one fails, 2 on bad usage.

import { runLiveCheck } from "./live-check.mjs";

const args = process.argv.slice(2);
let url = "https://api.darkpyonix.dev";
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--url" && args[i + 1]) url = args[++i];
  else {
    console.error("usage: run-live-check.mjs [--url <hub API url>]");
    process.exit(2);
  }
}

console.log(`hub: ${url}`);
const result = await runLiveCheck({
  baseUrl: url,
  log: (line) => console.log(line),
  onLink: (link) => {
    console.log("");
    console.log(`  Open ${link.verification_uri_complete}`);
    console.log(`  sign in with GitHub and approve the device "hub live check" (code ${link.user_code}).`);
    // Until the main page serves /link (SPEC FR-H12), the hub's own approval page does.
    console.log(`  If that page does not exist yet: ${url.replace(/\/+$/, "")}/link?code=${link.user_code}`);
    console.log("  Waiting for the approval...");
    console.log("");
  },
});
console.log(result.ok ? "PASS: the hub answered every step as specified" : "FAIL");
process.exit(result.ok ? 0 : 1);
