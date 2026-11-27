"""A stand-in for the VS Code server (OSE, `code serve-web`) in the editor tests (SPEC FR-M9,
FR-M10). Standard library only.

Server mode (`--socket-path`, or `--host --port --connection-token-file` for loopback): appends its argv to `<data>/starts.jsonl`, writes its pid to
`<data>/pid`, and answers HTTP on the Unix socket. A GET answers 200 with the path and headers it
saw and logs the request to `<data>/requests.jsonl`; a request with `Upgrade` answers 101 and then
echoes every byte. On loopback, a request without the token (cookie `vscode-tkn` or query
`tkn`) answers 403 and goes no further, as the real server does. When `<data>/crash-once`
exists it is removed and the server exits 3 at once.

CLI mode: `--install-extension X` appends X to `<data>/installed.txt` (an id starting with `fail.`
exits 1 with a message). A path ending in `.vsix` is unpacked like the real server does: its
`extension/package.json` goes to `<data>/extensions/<publisher.name>-<version>/` and the id is
registered in `installed.txt`; a folder written there by anything else is never listed. `--uninstall-extension X` removes X from it (an id starting with
`stuck.` exits 1); `--list-extensions --show-versions` prints `id@version` lines from it.
"""

import json
import os
import signal
import socketserver
import sys


def arg(name):
    argv = sys.argv[1:]
    return argv[argv.index(name) + 1] if name in argv else None


data = arg("--server-data-dir") or "."
os.makedirs(data, exist_ok=True)


def append(name, line):
    with open(os.path.join(data, name), "a") as f:
        f.write(line + "\n")


if arg("--install-extension"):
    spec = arg("--install-extension")
    append("cli.jsonl", json.dumps(sys.argv[1:]))
    if spec.endswith(".vsix"):
        import zipfile
        with zipfile.ZipFile(spec) as z:
            manifest = z.read("extension/package.json")
        pkg = json.loads(manifest)
        ext_id = "%s.%s" % (pkg["publisher"], pkg["name"])
        folder = os.path.join(data, "extensions", "%s-%s" % (ext_id, pkg["version"]))
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, "package.json"), "wb") as f:
            f.write(manifest)
        path = os.path.join(data, "installed.txt")
        lines = open(path).read().split() if os.path.exists(path) else []
        lines = [l for l in lines if l.split("@")[0] != ext_id] + ["%s@%s" % (ext_id, pkg["version"])]
        with open(path, "w") as f:
            f.write("".join(l + "\n" for l in lines))
        sys.exit(0)
    if spec.startswith("fail."):
        print("Extension '%s' not found." % spec)
        sys.exit(1)
    append("installed.txt", spec)
    sys.exit(0)

if arg("--uninstall-extension"):
    spec = arg("--uninstall-extension")
    append("cli.jsonl", json.dumps(sys.argv[1:]))
    if spec.startswith("stuck."):
        print("Cannot uninstall '%s': in use." % spec)
        sys.exit(1)
    path = os.path.join(data, "installed.txt")
    lines = open(path).read().split() if os.path.exists(path) else []
    with open(path, "w") as f:
        for line in lines:
            if line.split("@")[0] != spec:
                f.write(line + "\n")
    extensions = os.path.join(data, "extensions")
    for name in os.listdir(extensions) if os.path.isdir(extensions) else []:
        if name.startswith(spec + "-"):
            import shutil
            shutil.rmtree(os.path.join(extensions, name))
    sys.exit(0)

if "--list-extensions" in sys.argv:
    path = os.path.join(data, "installed.txt")
    lines = open(path).read().split() if os.path.exists(path) else []
    for spec in lines:
        print(spec if "@" in spec else spec + "@1.0.0")
    sys.exit(0)

append("starts.jsonl", json.dumps(sys.argv[1:]))
with open(os.path.join(data, "pid"), "w") as f:
    f.write(str(os.getpid()))
crash = os.path.join(data, "crash-once")
if os.path.exists(crash):
    os.remove(crash)
    sys.exit(3)

socket_path = arg("--socket-path")
token_file = arg("--connection-token-file")
token = open(token_file).read().strip() if token_file else None


def has_token(path, headers):
    if token is None:
        return True
    cookies = dict(c.strip().split("=", 1) for c in headers.get("cookie", "").split(";") if "=" in c)
    query = path.split("?", 1)[1] if "?" in path else ""
    params = dict(p.split("=", 1) for p in query.split("&") if "=" in p)
    return cookies.get("vscode-tkn") == token or params.get("tkn") == token


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            line = self.rfile.readline()
            if not line:
                return
            method, path, _ = line.decode("latin-1").split(" ", 2)
            headers = {}
            while True:
                h = self.rfile.readline().decode("latin-1").strip()
                if not h:
                    break
                k, v = h.split(":", 1)
                headers[k.strip().lower()] = v.strip()
            length = int(headers.get("content-length", "0") or "0")
            if length:
                self.rfile.read(length)
            if not has_token(path, headers):
                self.wfile.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                self.wfile.flush()
                continue
            append("requests.jsonl", json.dumps({"method": method, "path": path, "headers": headers}))
            if "upgrade" in headers:
                self.wfile.write(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: "
                                 + headers["upgrade"].encode() + b"\r\n\r\n")
                self.wfile.flush()
                while True:
                    chunk = self.connection.recv(4096)
                    if not chunk:
                        return
                    self.connection.sendall(chunk)
            body = json.dumps({"path": path, "headers": headers}).encode()
            self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                             + str(len(body)).encode() + b"\r\n\r\n" + body)
            self.wfile.flush()


signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
if socket_path:
    class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
        daemon_threads = True
    address = socket_path
else:
    class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
        daemon_threads = True
        allow_reuse_address = True
    address = (arg("--host"), int(arg("--port")))
with Server(address, Handler) as server:
    server.serve_forever()
