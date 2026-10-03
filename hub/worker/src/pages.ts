// The few HTML pages the Worker renders itself. No user input is placed in them unescaped.

function escape(s: string): string {
  return s.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
}

const HEAD = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root { color-scheme: light dark; font-family: system-ui, sans-serif; }
body { max-width: 36rem; margin: 3rem auto; padding: 0 16px; line-height: 1.5; }
code { overflow-wrap: anywhere; }
button { font: inherit; padding: .4rem 1rem; margin-right: .5rem; }
</style>`;

/** `/s/{share_id}` before the ash viewer is deployed. */
export function sharePlaceholderPage(shareId: string): string {
  const id = escape(shareId);
  return `${HEAD}
<title>DarkPyonix share</title>
</head>
<body>
<h1>DarkPyonix shared notebook</h1>
<p>The ash viewer is not published on this hub yet. This page resolves the share
<code>${id}</code>; the access token after <code>#</code> stays in your browser.</p>
<pre id="route"></pre>
<script>
fetch("/v1/shares/${id}").then(function (r) { return r.json(); }).then(function (j) {
  document.getElementById("route").textContent = "host: " + j.endpoint_id + "\\nrelay: " + j.relay_url;
});
</script>
</body>
</html>
`;
}

/** `/link?code=XXXX-XXXX`: approve or deny a device for the signed-in account. */
export function linkPage(login: string, code: string): string {
  return `${HEAD}
<title>Link a device</title>
</head>
<body>
<h1>Link a device</h1>
<p>Signed in as <strong>${escape(login)}</strong> (GitHub).</p>
<form id="lookup">
<label>Code shown on the device <input id="code" name="code" value="${escape(code)}" autocomplete="off" required></label>
<button>Look up</button>
</form>
<section id="device" hidden>
<p>Device <strong id="name"></strong> (<span id="role"></span>) wants to join your account.</p>
<p>Endpoint id: <code id="endpoint"></code></p>
<p>Approve only if this is the code your own device shows.</p>
<button id="approve">Approve</button><button id="deny">Deny</button>
</section>
<p id="status" role="status"></p>
<script>
var current = "";
function show(text) { document.getElementById("status").textContent = text; }
function lookup(code) {
  current = code;
  fetch("/v1/link-codes/" + encodeURIComponent(code)).then(function (r) {
    if (!r.ok) { document.getElementById("device").hidden = true; show("Unknown or expired code."); return; }
    return r.json().then(function (j) {
      document.getElementById("name").textContent = j.name;
      document.getElementById("role").textContent = j.role;
      document.getElementById("endpoint").textContent = j.endpoint_id;
      document.getElementById("device").hidden = false;
      show("");
    });
  });
}
function decide(approve) {
  fetch("/v1/link-codes/" + encodeURIComponent(current), {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ approve: approve })
  }).then(function (r) {
    document.getElementById("device").hidden = true;
    show(r.ok ? (approve ? "Approved. The device finishes joining by itself." : "Denied.") : "That did not work; the code may have expired.");
  });
}
document.getElementById("lookup").addEventListener("submit", function (e) {
  e.preventDefault(); lookup(document.getElementById("code").value);
});
document.getElementById("approve").addEventListener("click", function () { decide(true); });
document.getElementById("deny").addEventListener("click", function () { decide(false); });
if (document.getElementById("code").value) lookup(document.getElementById("code").value);
</script>
</body>
</html>
`;
}
