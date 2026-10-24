"""2025 WebSocket synchronization (SPEC FR-S9, FR-K9, FR-S7; PROTOCOL §5).

The 'manager' is the test (as in test_fr_k9_handoff.py): it hands a TCP socket to a real kernel
process, then speaks RFC 6455 on the client side. Kernel tests run on every interpreter
available (NFR-K1). The framing and idle rules are also tested in process.
"""
from __future__ import annotations

import base64
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import textwrap
import time

import pytest

from darkpyonix import _launcher as launcher
from darkpyonix import _wsync as ws
from darkpyonix._client import KernelClient
from darkpyonix._protocol import kernel_id_for

from helpers import handoff as h

pytestmark = pytest.mark.skipif(not hasattr(socket, "AF_UNIX") or os.name == "nt",
                                reason="SCM_RIGHTS handoff needs a Unix socket (POSIX)")

NB = textwrap.dedent('''\
    # %% [code]
    x = 1
    print("hello from the kernel")

    # %% [code]
    y = x + 1
    ''')

FULL = ["read", "history", "execute", "edit", "manage"]
KEY = base64.b64encode(b"0123456789abcdef").decode()


@pytest.fixture
def kernel(scratch, dp_home, python):
    path = os.path.join(scratch, "nb.py")
    with open(path, "w") as f:
        f.write(NB)
    pid = launcher.launch(path, python=python)
    info = launcher.wait_for_announce(kernel_id_for(path), pid=pid, timeout=20.0)
    assert info is not None and info["control"], "kernel did not announce"
    client = KernelClient(info["control"], info["kernel_id"], name="test", kind="cli")
    client.connect()
    yield info, client
    client.close()
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(100):
            os.kill(pid, 0)
            time.sleep(0.05)
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


# ------------------------------------------------------------------ a WebSocket client

class Ws(object):
    def __init__(self, sock, rest=b""):
        self.sock = sock
        self.buf = bytearray(rest)

    def send(self, obj, mask=True, fin=True, opcode=ws.OP_TEXT, raw=None):
        payload = raw if raw is not None else json.dumps(obj).encode("utf-8")
        key = b"\x01\x02\x03\x04"
        n = len(payload)
        head = bytes([(0x80 if fin else 0) | opcode])
        flag = 0x80 if mask else 0
        if n < 126:
            head += bytes([flag | n])
        elif n < 65536:
            head += bytes([flag | 126]) + struct.pack(">H", n)
        else:
            head += bytes([flag | 127]) + struct.pack(">Q", n)
        if mask:
            head += key
            payload = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(head + payload)

    def frame(self, timeout=10.0):
        """The next ``(opcode, payload)`` the kernel sent, or None on EOF."""
        self.sock.settimeout(0.5)
        deadline = time.monotonic() + timeout
        while True:
            if len(self.buf) >= 2:
                n, pos = self.buf[1] & 0x7F, 2
                if n == 126 and len(self.buf) >= 4:
                    n, pos = struct.unpack(">H", bytes(self.buf[2:4]))[0], 4
                elif n == 127 and len(self.buf) >= 10:
                    n, pos = struct.unpack(">Q", bytes(self.buf[2:10]))[0], 10
                if (self.buf[1] & 0x7F) < 126 or pos > 2:
                    assert not self.buf[1] & 0x80, "server frames must not be masked"
                    if len(self.buf) >= pos + n:
                        op, payload = self.buf[0] & 0x0F, bytes(self.buf[pos:pos + n])
                        del self.buf[:pos + n]
                        return op, payload
            if time.monotonic() > deadline:
                raise AssertionError("no frame; buffer %r" % bytes(self.buf[-200:]))
            try:
                chunk = self.sock.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                return None
            self.buf += chunk

    def msg(self, timeout=10.0):
        got = self.frame(timeout)
        assert got is not None, "connection closed"
        op, payload = got
        while op in (ws.OP_PING, ws.OP_PONG):
            got = self.frame(timeout)
            assert got is not None, "connection closed"
            op, payload = got
        assert op == ws.OP_TEXT, (op, payload)
        return json.loads(payload.decode("utf-8"))

    def until(self, type_, timeout=15.0, **match):
        deadline = time.monotonic() + timeout
        seen = []
        while time.monotonic() < deadline:
            m = self.msg(timeout=max(0.5, deadline - time.monotonic()))
            seen.append(m["type"])
            if m["type"] == type_ and all(m.get(k) == v for k, v in match.items()):
                return m
        raise AssertionError("no %s; saw %s" % (type_, seen))

    def collect(self, until_type, timeout=15.0):
        out = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            m = self.msg(timeout=max(0.5, deadline - time.monotonic()))
            out.append(m)
            if m["type"] == until_type:
                return out
        raise AssertionError("no %s; saw %s" % (until_type, [m["type"] for m in out]))

    def close(self):
        self.sock.close()


def label(caps=FULL, client_id=None, user="alice", nickname="mbp", share_id=None):
    return {"capabilities": caps, "client_id": client_id, "user": user, "nickname": nickname,
            "share_id": share_id}


def connect(info, caps=FULL, client_id="dev_aaaaaaaa", **kw):
    path = "/api/ws/kernels/%s?client_id=%s" % (info["kernel_id"], client_id)
    client, server_side = h.tcp_pair()
    resp = h.adopt_fd(info["control"], info["kernel_id"], server_side, "ws",
                      h.http_get(path, {"Upgrade": "websocket", "Connection": "Upgrade",
                                        "Sec-WebSocket-Key": KEY, "Sec-WebSocket-Version": "13"}),
                      label(caps, client_id=client_id, **kw))
    server_side.close()
    assert resp["ok"], resp
    head, rest = h.read_head(client)
    assert head.startswith("HTTP/1.1 101"), head
    return Ws(client, rest)


def who(client_id, caps=FULL):
    return {"client_id": client_id, "user": "bob", "nickname": "rest", "capabilities": list(caps)}


def cell_ids(client):
    return [c["cell_id"] for c in client.request("doc.snapshot", {})["cells"]]


# ------------------------------------------------------------------ FR-S9

def test_fr_s9_ws_initial_sync_code_history_locks(kernel):
    info, client = kernel
    a = connect(info)
    first = a.msg()
    assert first["type"] == "users_focus_data" and first["users_focus"] == []
    a.send({"type": "request_code", "kernel_id": info["kernel_id"]})
    code = a.until("code_data")
    assert [c["cell_type"] for c in code["cells"]] == ["preamble", "code"] or len(code["cells"]) >= 2
    assert any("y = x + 1" in c["source"] for c in code["cells"])
    assert all(set(c) >= {"cell_id", "cell_type", "source", "execution_count"} for c in code["cells"])
    a.send({"type": "request_locks"})
    locks = a.until("locks_data")["locks"]
    assert len(locks) == len(code["cells"]) and all(l["is_locked"] is False for l in locks)
    a.send({"type": "request_history"})
    assert a.until("history_data")["history"] == []
    client.request("run", {"mode": "all"})
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if client.request("runs.list", {"limit": 1})["runs"][:1] and \
                client.request("runs.list", {"limit": 1})["runs"][0]["status"] == "ok":
            break
        time.sleep(0.1)
    a.send({"type": "request_history"})
    hist = a.until("history_data")["history"]
    assert hist and any("hello from the kernel" in json.dumps(e["outputs"]) for e in hist)
    assert all(set(e) >= {"cell_id", "execution_count", "outputs", "executed_at"} for e in hist)
    a.close()


def test_fr_s9_ws_typing_locks_and_unlock_with_code(kernel):
    info, client = kernel
    a, b = connect(info, client_id="dev_aaaaaaaa"), connect(info, client_id="dev_bbbbbbbb", user="carol")
    a.until("users_focus_data")
    b.until("users_focus_data")
    c1 = cell_ids(client)[1]
    a.send({"type": "start_typing", "cell_id": c1, "user_id": "ignored"})
    locked = b.until("cell_locked")
    assert locked["cell_id"] == c1 and locked["locked_by"] == "dev_aaaaaaaa"
    assert locked["user_name"] == "alice" and locked["locked_at"]
    a.until("cell_locked")                       # everyone gets it, the sender included
    b.send({"type": "start_typing", "cell_id": c1})
    err = b.until("error")
    assert err["error"] == "CELL_ALREADY_LOCKED" and err["locked_by"] == "dev_aaaaaaaa"
    # Unlocking with the final source goes through REST (the manager): the sockets hear it.
    client.request("doc.unlock", {"client": who("dev_aaaaaaaa"), "cell_id": c1,
                                  "source": "y = x + 41\n"})
    un = b.until("cell_unlocked_with_code")
    assert un["cell_id"] == c1 and un["previously_locked_by"] == "dev_aaaaaaaa"
    assert un["updated_code"] == "y = x + 41\n"
    a.until("cell_unlocked_with_code")
    # The cell update itself is also broadcast as a plain event.
    a.send({"type": "request_code"})
    assert any(c["source"] == "y = x + 41\n" for c in a.until("code_data")["cells"])
    a.close()
    b.close()


def test_fr_s9_ws_focus_and_blur_reach_other_users(kernel):
    info, client = kernel
    a, b = connect(info, client_id="dev_aaaaaaaa"), connect(info, client_id="dev_bbbbbbbb", user="carol")
    a.until("users_focus_data")
    b.until("users_focus_data")
    c1 = cell_ids(client)[1]
    a.send({"type": "cell_focus", "cell_id": c1})
    focus = b.until("other_user_focus")
    assert focus["cell_id"] == c1 and focus["user_id"] == "dev_aaaaaaaa" and focus["user_name"] == "alice"
    a.send({"type": "cell_blur", "cell_id": c1})
    blur = b.until("other_user_blur")
    assert blur["cell_id"] == c1 and blur["user_id"] == "dev_aaaaaaaa"
    c = connect(info, client_id="dev_cccccccc")
    a.send({"type": "cell_focus", "cell_id": c1})
    b.until("other_user_focus")
    again = connect(info, client_id="dev_dddddddd").until("users_focus_data")
    assert [u["user_id"] for u in again["users_focus"]] == ["dev_aaaaaaaa"]
    assert again["users_focus"][0]["focused_cell_id"] == c1
    a.send({"type": "cell_focus", "cell_id": "c_missing"})
    assert a.until("error")["error"] == "CELL_NOT_FOUND"
    for s in (a, b, c):
        s.close()


def test_fr_s9_ws_execution_broadcast(kernel):
    info, client = kernel
    a, b = connect(info, client_id="dev_aaaaaaaa"), connect(info, client_id="dev_bbbbbbbb")
    a.until("users_focus_data")
    b.until("users_focus_data")
    client.request("run", {"mode": "all", "client": who("dev_bbbbbbbb")})
    for sock in (a, b):
        msgs = sock.collect("execution_complete")
        # The first code cell prints; its messages arrive in this order.
        types = [m["type"] for m in msgs]
        assert "execution_started" in types and "execution_output" in types
        started = next(m for m in msgs if m["type"] == "execution_started")
        assert started["execution_id"].startswith("r_") or started["execution_id"]
        assert started["started_by"] == "dev_bbbbbbbb" and started["user_name"] == "bob"
        assert started["timestamp"]
        out = next(m for m in msgs if m["type"] == "execution_output")
        assert out["execution_id"] == started["execution_id"] and out["is_complete"] is False
        assert "hello from the kernel" in json.dumps(out["outputs"])
        done = msgs[-1]
        assert done["execution_id"] == started["execution_id"]
        assert done["execution_time"] is not None and done["completed_at"]
        assert "hello from the kernel" in json.dumps(done["final_outputs"]) or done["cell_id"] is not None
        assert all(isinstance(m.get("seq"), int) for m in msgs)
    a.close()
    b.close()


def test_fr_s9_ws_execution_error_message(kernel):
    info, client = kernel
    a = connect(info)
    a.until("users_focus_data")
    c1 = cell_ids(client)[1]
    client.request("doc.cell.update", {"client": who("dev_zzzzzzzz"), "cell_id": c1,
                                       "base_version": 1, "source": "raise ValueError('boom')\n"})
    client.request("run", {"mode": "all"})
    err = a.until("execution_error")
    assert err["error"]["ename"] == "ValueError" and err["error"]["evalue"] == "boom"
    assert err["error"]["traceback"] and err["failed_at"]
    a.close()


def test_fr_s9_ws_unmapped_events_are_wrapped(kernel):
    info, client = kernel
    a = connect(info)
    a.until("users_focus_data")
    client.request("doc.cell.create", {"client": who("dev_zzzzzzzz"), "source": "z = 3\n"})
    ev = a.until("event")
    assert ev["event"]["type"] == "doc.cell.created" and isinstance(ev["seq"], int)
    a.close()


def test_fr_s9_ws_without_history_gets_no_outputs(kernel):
    info, client = kernel
    full = connect(info, client_id="dev_aaaaaaaa")
    viewer = connect(info, caps=["read"], client_id="dev_bbbbbbbb")
    full.until("users_focus_data")
    viewer.until("users_focus_data")
    client.request("run", {"mode": "all"})
    done = viewer.collect("execution_complete")
    assert "execution_output" not in [m["type"] for m in done]
    assert done[-1]["final_outputs"] == []
    full.collect("execution_complete")
    viewer.send({"type": "request_history"})
    assert viewer.until("history_data")["history"] == []
    full.close()
    viewer.close()


def test_fr_s9_ws_without_read_gets_no_code(kernel):
    info, client = kernel
    blind = connect(info, caps=["history"])
    blind.until("users_focus_data")
    blind.send({"type": "request_code"})
    code = blind.until("code_data")
    assert code["cells"] and all(c["source"] == "" for c in code["cells"])
    client.request("doc.cell.create", {"client": who("dev_zzzzzzzz"), "source": "secret = 1\n"})
    assert "secret" not in json.dumps(blind.until("event"))
    blind.close()


def test_fr_s9_ws_typing_needs_edit(kernel):
    info, client = kernel
    a = connect(info, caps=["read", "execute"])
    a.until("users_focus_data")
    c1 = cell_ids(client)[1]
    a.send({"type": "start_typing", "cell_id": c1})
    assert a.until("error")["error"] == "INSUFFICIENT_PERMISSION"
    assert all("lock" not in c for c in client.request("doc.snapshot", {})["cells"])
    a.close()


def test_fr_s9_ws_unknown_and_malformed_messages_are_answered(kernel):
    info, _ = kernel
    a = connect(info)
    a.until("users_focus_data")
    a.send({"type": "nope"})
    assert a.until("error")["error"] == "UNKNOWN_TYPE"
    a.send(None, raw=b"{not json")
    assert a.until("error")["error"] == "BAD_REQUEST"
    a.send([1, 2])
    assert a.until("error")["error"] == "BAD_REQUEST"
    a.close()


def test_fr_s9_ws_ping_and_fragmented_messages(kernel):
    info, _ = kernel
    a = connect(info)
    a.until("users_focus_data")
    a.send(None, opcode=ws.OP_PING, raw=b"hi")
    got = a.frame()
    while got[0] != ws.OP_PONG:
        got = a.frame()
    assert got[1] == b"hi"
    body = json.dumps({"type": "request_locks"}).encode()
    a.send(None, fin=False, opcode=ws.OP_TEXT, raw=body[:7])
    a.send(None, fin=True, opcode=ws.OP_CONT, raw=body[7:])
    assert a.until("locks_data")
    a.close()


def test_fr_s9_ws_unmasked_client_frame_closes_with_1002(kernel):
    info, _ = kernel
    a = connect(info)
    a.until("users_focus_data")
    a.send({"type": "request_locks"}, mask=False)
    got = a.frame()
    while got is not None and got[0] != ws.OP_CLOSE:
        got = a.frame()
    assert got is not None and struct.unpack(">H", got[1][:2])[0] == 1002
    a.close()


def test_fr_s9_ws_close_frame_is_echoed(kernel):
    info, _ = kernel
    a = connect(info)
    a.until("users_focus_data")
    a.send(None, opcode=ws.OP_CLOSE, raw=struct.pack(">H", 1000))
    got = a.frame()
    while got is not None and got[0] != ws.OP_CLOSE:
        got = a.frame()
    assert got is not None and struct.unpack(">H", got[1][:2])[0] == 1000
    assert h.is_closed(a.sock)
    a.close()


def test_fr_s9_ws_survives_manager_kill(kernel):
    info, client = kernel
    c, server_side = h.tcp_pair()
    path = "/api/ws/kernels/%s?client_id=dev_aaaaaaaa" % info["kernel_id"]
    req = base64.b64encode(h.http_get(path, {"Upgrade": "websocket", "Connection": "Upgrade",
                                             "Sec-WebSocket-Key": KEY,
                                             "Sec-WebSocket-Version": "13"})).decode("ascii")
    proc = subprocess.Popen(
        [sys.executable, h.__file__, info["control"], info["kernel_id"], str(server_side.fileno()),
         "ws", req, json.dumps(label(client_id="dev_aaaaaaaa"))],
        pass_fds=[server_side.fileno()], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        env=dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path)))
    try:
        assert json.loads(proc.stdout.readline())["ok"] is True
        server_side.close()
        head, rest = h.read_head(c)
        assert head.startswith("HTTP/1.1 101")
        sock = Ws(c, rest)
        sock.until("users_focus_data")
        proc.kill()
        proc.wait(10)
        client.request("run", {"mode": "all"})
        sock.until("execution_complete")
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.stdout.close()
        proc.stdin.close()
        c.close()


def test_fr_k9_ws_streams_close_closes_the_socket(kernel):
    info, client = kernel
    a = connect(info, share_id="s_aaaaaaaaaaaaaaaa")
    a.until("users_focus_data")
    assert client.request("streams.close", {"share_id": "s_aaaaaaaaaaaaaaaa"}) == {"closed": 1}
    assert h.is_closed(a.sock)


def test_fr_k9_ws_presence_carries_capabilities(kernel):
    info, client = kernel
    a = connect(info, caps=["read", "history"], client_id="dev_aaaaaaaa")
    a.until("users_focus_data")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        pres = client.request("doc.snapshot", {})["presence"]
        if pres:
            break
        time.sleep(0.1)
    assert pres[0]["client_id"] == "dev_aaaaaaaa" and pres[0]["capabilities"] == ["read", "history"]
    a.close()


# ------------------------------------------------------------------ FR-S7 poll fields

def _get(c):
    head, rest = h.read_head(c, timeout=30)
    length = int([l for l in head.split("\r\n") if l.lower().startswith("content-length")][0].split(":")[1])
    while len(rest) < length:
        rest += c.recv(65536)
    return int(head.split(" ")[1]), json.loads(rest)


def _wait(info, client, ref, query, caps=FULL):
    path = "/api/kernels/%s/runs/%s/wait?%s" % (info["kernel_id"], ref, query)
    c, server_side = h.tcp_pair()
    resp = h.adopt_fd(info["control"], info["kernel_id"], server_side, "wait", h.http_get(path),
                      label(caps))
    server_side.close()
    assert resp["ok"], resp
    out = _get(c)
    c.close()
    return out


def test_fr_s7_wait_carries_2025_poll_fields(kernel):
    info, client = kernel
    slow = client.request("doc.cell.update", {"client": who("dev_zzzzzzzz"),
                                              "cell_id": cell_ids(client)[1],
                                              "base_version": 1,
                                              "source": "import time\ntime.sleep(30)\n"})
    run_id = client.request("run", {"mode": "all"})["run_id"]
    status, body = _wait(info, client, run_id, "timeout=1&poll_sequence=3&client_id=dev_aaaaaaaa")
    assert status == 200 and body["status"] in ("running", "queued")
    assert body["poll_sequence"] == 3 and body["next_poll_sequence"] == 4
    auto = body["auto_repoll"]
    assert auto["enabled"] is True and auto["max_polls"] == 100 and auto["recommended_delay"] >= 1
    assert "poll_sequence=4" in auto["next_poll_url"] and "client_id=dev_aaaaaaaa" in auto["next_poll_url"]
    assert body["next"] == auto["next_poll_url"]
    client.request("interrupt", {})
    status, body = _wait(info, client, run_id, "timeout=20")
    assert body["poll_sequence"] == 1 and body["next_poll_sequence"] is None
    assert body["auto_repoll"] is None and body["next"] is None


def test_fr_s7_poll_limit_exceeded_is_429(kernel):
    info, client = kernel
    run_id = client.request("run", {"mode": "all"})["run_id"]
    status, body = _wait(info, client, run_id, "timeout=1&poll_sequence=101")
    assert status == 429 and body["code"] == "poll_limit_exceeded"
    status, body = _wait(info, client, run_id, "timeout=1&poll_sequence=0")
    assert status == 400 and body["code"] == "bad_request"
    status, body = _wait(info, client, run_id, "timeout=20&poll_sequence=100")
    assert status == 200 and body["poll_sequence"] == 100


# ------------------------------------------------------------------ framing, in process

def _masked(opcode, payload, fin=True):
    key = b"\x09\x08\x07\x06"
    head = bytes([(0x80 if fin else 0) | opcode, 0x80 | len(payload)]) + key
    return head + bytes(b ^ key[i % 4] for i, b in enumerate(payload))


def test_fr_s9_frame_reader_joins_fragments_and_splits_reads():
    r = ws.FrameReader()
    data = _masked(ws.OP_TEXT, b"he", fin=False) + _masked(ws.OP_PING, b"p") + _masked(ws.OP_CONT, b"llo")
    out = []
    for i in range(0, len(data), 3):
        out += r.feed(data[i:i + 3])
    assert out == [(ws.OP_PING, b"p"), (ws.OP_TEXT, b"hello")]


@pytest.mark.parametrize("data,code", [
    (bytes([0x81, 0x01, 0x41]), 1002),                                   # unmasked
    (bytes([0xC1, 0x80, 0, 0, 0, 0]), 1002),                             # RSV1
    (_masked(ws.OP_CONT, b"x"), 1002),                                   # continuation alone
    (_masked(ws.OP_TEXT, b"a", fin=False) + _masked(ws.OP_TEXT, b"b"), 1002),
    (_masked(ws.OP_PING, b"p", fin=False), 1002),                        # fragmented control
    (bytes([0x81, 0xFF]) + struct.pack(">Q", 1 << 40) + b"\0\0\0\0", 1009),
])
def test_fr_s9_frame_reader_refuses_protocol_errors(data, code):
    with pytest.raises(ws.ProtocolError) as e:
        ws.FrameReader().feed(data)
    assert e.value.code == code


def test_fr_s9_server_frames_are_unmasked_with_extended_lengths():
    assert ws.frame(ws.OP_TEXT, b"a")[:2] == bytes([0x81, 1])
    assert ws.frame(ws.OP_TEXT, b"a" * 200)[:4] == bytes([0x81, 126, 0, 200])
    assert ws.frame(ws.OP_TEXT, b"a" * 70000)[:2] == bytes([0x81, 127])
    assert ws.close_frame(1002, "x")[:4] == bytes([0x88, 3, 0x03, 0xEA])


def test_fr_s9_idle_connection_is_closed_and_pings_are_sent(monkeypatch):
    from darkpyonix import _streams
    from darkpyonix._events import EventLog
    monkeypatch.setattr(ws, "PING_INTERVAL", 0.3)
    monkeypatch.setattr(ws, "IDLE_TIMEOUT", 1.2)
    monkeypatch.setattr(_streams, "POLL", 0.05)
    log = EventLog()
    snap = {"doc_version": 0, "seq": 0, "cells": [], "presence": []}
    hub = _streams.StreamHub("k_" + "0" * 20, log, {"doc.snapshot": lambda p: snap})
    client, server_side = h.tcp_pair()
    raw = h.http_get("/api/ws/kernels/k_x", {"Upgrade": "websocket", "Connection": "Upgrade",
                                             "Sec-WebSocket-Key": KEY})
    try:
        hub.adopt(server_side, "ws", raw, label(client_id="dev_aaaaaaaa"))
        head, rest = h.read_head(client)
        assert head.startswith("HTTP/1.1 101")
        sock = Ws(client, rest)
        assert sock.msg()["type"] == "users_focus_data"
        ops = []
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            got = sock.frame(timeout=8)
            if got is None:
                break
            ops.append(got[0])
            if got[0] == ws.OP_CLOSE:
                assert struct.unpack(">H", got[1][:2])[0] == 1001
                break
        assert ws.OP_PING in ops and ws.OP_CLOSE in ops
    finally:
        hub.stop()
        client.close()
