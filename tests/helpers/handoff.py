"""What a manager does to hand a client connection to a kernel (PROTOCOL §3.7), for tests.

Standard library only. ``adopt_fd`` is the POSIX side: handshake on the control socket, the
``adopt`` frame, then one byte ``F`` carrying the descriptor in ``SCM_RIGHTS``.
"""
from __future__ import annotations

import base64
import socket
import sys
import time

from darkpyonix import _protocol as p


def http_get(path, headers=None, host="127.0.0.1"):
    lines = ["GET %s HTTP/1.1" % path, "Host: %s" % host]
    for k, v in (headers or {}).items():
        lines.append("%s: %s" % (k, v))
    return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")


def control_session(control, kernel_id):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect(control)
    hello = p.recv_frame(s)
    assert hello["op"] == "hello" and hello["kernel_id"] == kernel_id
    s.sendall(p.encode({"op": "auth", "client": {"name": "test-manager", "kind": "manager", "pid": 1}}))
    assert p.recv_frame(s)["op"] == "welcome"
    return s


def adopt_fd(control, kernel_id, conn, kind, request, label, transport="fd", send_fd=True):
    """Send ``conn`` (a connected socket) to the kernel; returns the response frame."""
    s = control_session(control, kernel_id)
    try:
        params = {"kind": kind, "transport": transport, "share": None, "label": label,
                  "request": base64.b64encode(request).decode("ascii")}
        s.sendall(p.encode({"op": "request", "id": 9, "method": "adopt", "params": params}))
        if send_fd:
            s.sendmsg([b"F"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, __import__("array").array(
                "i", [conn.fileno()]).tobytes())])
        else:
            s.sendall(b"F")
        return p.recv_frame(s)
    finally:
        s.close()


def read_head(sock, timeout=10.0):
    """Bytes up to and including the blank line, and whatever followed in the same read."""
    sock.settimeout(timeout)
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    head, _, rest = buf.partition(b"\r\n\r\n")
    return head.decode("latin-1"), rest


def read_until(sock, needle, buf=b"", timeout=10.0):
    """Read until ``needle`` is in the data. Returns all data read, or raises on EOF/timeout."""
    sock.settimeout(1.0)
    deadline = time.monotonic() + timeout
    while needle not in buf:
        if time.monotonic() > deadline:
            raise AssertionError("did not see %r; got %r" % (needle, buf[-300:]))
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            continue
        if not chunk:
            raise AssertionError("connection closed before %r; got %r" % (needle, buf[-300:]))
        buf += chunk
    return buf


def is_closed(sock, timeout=3.0):
    """True if the peer closed ``sock`` within ``timeout`` (ignoring data that still arrives)."""
    sock.settimeout(0.5)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if not sock.recv(65536):
                return True
        except socket.timeout:
            continue
        except OSError:
            return True
    return False


def tcp_pair():
    """``(client, server_side)`` of a loopback TCP connection: the second is the 'manager's'."""
    lst = socket.socket()
    lst.bind(("127.0.0.1", 0))
    lst.listen(1)
    client = socket.create_connection(lst.getsockname())
    server_side, _ = lst.accept()
    lst.close()
    return client, server_side


if __name__ == "__main__":
    # A throwaway 'manager' process: adopt the inherited descriptor, report, then wait to be
    # killed. usage: handoff.py <control> <kernel_id> <fd> <kind> <request-b64> <label-json>
    import json
    import os
    control, kid, fd, kind, req_b64, label = sys.argv[1:7]
    conn = socket.socket(fileno=int(fd))
    resp = adopt_fd(control, kid, conn, kind, base64.b64decode(req_b64), json.loads(label))
    sys.stdout.write(json.dumps(resp) + "\n")
    sys.stdout.flush()
    # Like the Rust manager, drop our copy; only SIGKILL of this process remains to test.
    conn.close()
    os.read(0, 1)
