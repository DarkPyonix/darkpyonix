"""A stand-in for the VS Code server (OSE, `code serve-web`) in the editor tests (SPEC FR-M9,
FR-M10). Standard library only.

Server mode (`--socket-path`): appends its argv to `<data>/starts.jsonl`, writes its pid to
`<data>/pid`, and answers HTTP on the Unix socket. A GET answers 200 with the path and headers it
saw and logs the request to `<data>/requests.jsonl`; a request with `Upgrade` answers 101 and then
echoes every byte. When `<data>/crash-once` exists it is removed and the server exits 3 at once.

CLI mode: `--install-extension X` appends X to `<data>/installed.txt` (an id starting with `fail.`
exits 1 with a message); `--list-extensions --show-versions` prints `id@version` lines from it.
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
    if spec.startswith("fail."):
        print("Extension '%s' not found." % spec)
        sys.exit(1)
    append("installed.txt", spec)
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


class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
with Server(socket_path, Handler) as server:
    server.serve_forever()
