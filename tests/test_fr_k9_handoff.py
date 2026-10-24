"""Stream handoff (SPEC FR-K9, PR-5, PR-6; PROTOCOL §3.7).

The 'manager' here is the test: it opens a client connection, hands the descriptor to a real
kernel process over the control socket, and the kernel answers the client itself. Kernel tests
run on every interpreter available (NFR-K1).
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import time

import pytest

from darkpyonix import _launcher as launcher
from darkpyonix import _protocol as p
from darkpyonix._client import KernelClient
from darkpyonix._events import EventLog
from darkpyonix._protocol import kernel_id_for
from darkpyonix._server import ControlServer

from helpers import handoff as h

pytestmark = pytest.mark.skipif(not hasattr(socket, "AF_UNIX") or os.name == "nt",
                                reason="SCM_RIGHTS handoff needs a Unix socket (POSIX)")

NB = textwrap.dedent('''\
    # %% [code]
    print("hello from the kernel")
    ''')

FULL = ["read", "history", "execute", "edit", "manage"]
KID = "k_0123456789abcdef0123"


@pytest.fixture
def kernel(scratch, dp_home, python):
    """A kernel process on ``python``; yields ``(info, client)``. Always stopped."""
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


def handed(info, kind, path, label, headers=None, transport="fd"):
    """Open a client connection, hand its server side to the kernel; returns the client socket."""
    client, server_side = h.tcp_pair()
    resp = h.adopt_fd(info["control"], info["kernel_id"], server_side, kind,
                      h.http_get(path, headers), label, transport)
    server_side.close()  # the manager drops its copy (PROTOCOL §3.7.2 step 4)
    assert resp["ok"], resp
    assert resp["result"]["stream_id"]
    return client


def label(caps=FULL, share_id=None, client_id=None):
    return {"capabilities": caps, "client_id": client_id, "user": "alice",
            "nickname": "alice-mbp", "share_id": share_id}


def events_path(info, query=""):
    return "/api/kernels/%s/events%s" % (info["kernel_id"], query)


def run_and_finish(client):
    res = client.request("run", {"mode": "all"})
    return res["run_id"]


# ------------------------------------------------------------------ FR-K9


def test_fr_k9_adopted_sse_gets_events_on_the_handed_socket(kernel):
    info, client = kernel
    c = handed(info, "events", events_path(info), label())
    head, rest = h.read_head(c)
    assert head.startswith("HTTP/1.1 200")
    assert "content-type: text/event-stream" in head.lower()
    client.request("run", {"mode": "all"})
    data = h.read_until(c, b"event: run.finished", rest)
    assert b"event: output" in data and b"hello from the kernel" in data
    c.close()


def test_fr_k9_adopted_sse_survives_manager_kill(kernel):
    info, client = kernel
    c, server_side = h.tcp_pair()
    req = base64.b64encode(h.http_get(events_path(info))).decode("ascii")
    proc = subprocess.Popen(
        [sys.executable, h.__file__, info["control"], info["kernel_id"], str(server_side.fileno()),
         "events", req, json.dumps(label())],
        pass_fds=[server_side.fileno()], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        env=dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path)))
    try:
        line = proc.stdout.readline()
        assert json.loads(line)["ok"] is True
        server_side.close()
        head, rest = h.read_head(c)
        assert head.startswith("HTTP/1.1 200")
        proc.kill()                      # SIGKILL: the manager is gone
        proc.wait(10)
        client.request("run", {"mode": "all"})
        data = h.read_until(c, b"event: run.finished", rest)
        assert b"hello from the kernel" in data
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.stdout.close()
        proc.stdin.close()
        c.close()


def test_fr_k9_kernel_ignores_tokens_in_adopted_request(kernel):
    info, _ = kernel
    c = handed(info, "events", events_path(info, "?token=definitely-wrong"), label(),
               headers={"Authorization": "Bearer definitely-wrong"})
    head, _ = h.read_head(c)
    assert head.startswith("HTTP/1.1 200"), head
    c.close()


def test_fr_k9_label_without_history_filters_outputs(kernel):
    info, client = kernel
    viewer = handed(info, "events", events_path(info), label(["read"]))
    full = handed(info, "events", events_path(info), label())
    _, rv = h.read_head(viewer)
    _, rf = h.read_head(full)
    client.request("run", {"mode": "all"})
    seen_full = h.read_until(full, b"event: run.finished", rf)
    seen_viewer = h.read_until(viewer, b"event: run.finished", rv)
    assert b"event: output" in seen_full
    assert b"event: output" not in seen_viewer
    assert b"hello from the kernel" not in seen_viewer
    viewer.close()
    full.close()


def test_fr_k9_last_event_id_replays_from_the_kernel_ring(kernel):
    info, client = kernel
    client.request("run", {"mode": "all"})
    time.sleep(1.5)
    c = handed(info, "events", events_path(info), label(), headers={"Last-Event-ID": "0"})
    _, rest = h.read_head(c)
    data = h.read_until(c, b"event: run.finished", rest)
    assert data.count(b"id: ") >= 2
    c.close()


def test_fr_k9_streams_close_closes_only_that_share(kernel):
    info, client = kernel
    a1 = handed(info, "events", events_path(info), label(share_id="s_aaaaaaaaaaaaaaaa"))
    a2 = handed(info, "events", events_path(info), label(share_id="s_aaaaaaaaaaaaaaaa"))
    b = handed(info, "events", events_path(info), label(share_id="s_bbbbbbbbbbbbbbbb"))
    master = handed(info, "events", events_path(info), label(share_id=None))
    rests = [h.read_head(s)[1] for s in (a1, a2, b, master)]
    res = client.request("streams.close", {"share_id": "s_aaaaaaaaaaaaaaaa"})
    assert res == {"closed": 2}
    assert h.is_closed(a1) and h.is_closed(a2)
    client.request("run", {"mode": "all"})
    for s, rest in ((b, rests[2]), (master, rests[3])):
        assert b"event: run.finished" in h.read_until(s, b"event: run.finished", rest)
    for s in (b, master):
        s.close()


def test_fr_k9_wait_is_answered_by_the_kernel(kernel):
    info, client = kernel
    run_id = client.request("run", {"mode": "all"})["run_id"]
    path = "/api/kernels/%s/runs/%s/wait?timeout=20" % (info["kernel_id"], run_id)
    c = handed(info, "wait", path, label())
    head, rest = h.read_head(c, timeout=30)
    assert head.startswith("HTTP/1.1 200")
    length = int([l for l in head.split("\r\n") if l.lower().startswith("content-length")][0].split(":")[1])
    while len(rest) < length:
        rest += c.recv(65536)
    body = json.loads(rest)
    assert body["status"] == "ok" and body["run_id"] == run_id and body["next"] is None
    assert body["run"] is not None
    c.close()


def test_fr_k9_wait_without_history_omits_the_run(kernel):
    info, client = kernel
    run_id = client.request("run", {"mode": "all"})["run_id"]
    path = "/api/kernels/%s/runs/%s/wait?timeout=20" % (info["kernel_id"], run_id)
    c = handed(info, "wait", path, label(["read"]))
    head, rest = h.read_head(c, timeout=30)
    length = int([l for l in head.split("\r\n") if l.lower().startswith("content-length")][0].split(":")[1])
    while len(rest) < length:
        rest += c.recv(65536)
    assert json.loads(rest)["run"] is None
    c.close()


def test_fr_k9_ws_upgrade_is_answered_with_101(kernel):
    info, _ = kernel
    key = base64.b64encode(b"0123456789abcdef").decode()
    c = handed(info, "ws", "/api/ws/kernels/%s?client_id=dev_8f2c1a" % info["kernel_id"], label(),
               headers={"Upgrade": "websocket", "Connection": "Upgrade",
                        "Sec-WebSocket-Key": key, "Sec-WebSocket-Version": "13"})
    head, _ = h.read_head(c)
    assert head.startswith("HTTP/1.1 101")
    accept = base64.b64encode(hashlib.sha1(
        key.encode() + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest()).decode()
    assert ("sec-websocket-accept: " + accept).lower() in head.lower()
    c.close()


def test_fr_k9_adopted_socket_on_windows():
    pytest.skip("Windows socket.fromshare handoff is a follow-up of #47 (needs a Windows runner)")


# ------------------------------------------------------------------ PR-5, PR-6 (in-process server)


@pytest.fixture
def server(dp_home):
    events = EventLog()
    srv = ControlServer(KID, events, {"status": lambda params: {"status": "idle"}})
    srv.start()
    yield srv, events
    srv.stop()


def test_pr_5_adopt_fd_over_unix_socket(server):
    srv, _ = server
    client, server_side = h.tcp_pair()
    resp = h.adopt_fd(srv.address, KID, server_side, "events",
                      h.http_get("/api/kernels/%s/events" % KID), label())
    server_side.close()
    assert resp["ok"] and resp["result"]["stream_id"].startswith("st_")
    head, _ = h.read_head(client)
    assert head.startswith("HTTP/1.1 200")
    client.close()


def test_pr_5_adopt_pump_end_of_a_socketpair(server):
    srv, _ = server
    ours, theirs = socket.socketpair()
    resp = h.adopt_fd(srv.address, KID, theirs, "events",
                      h.http_get("/api/kernels/%s/events" % KID), label(), transport="pump")
    theirs.close()
    assert resp["ok"]
    head, _ = h.read_head(ours)
    assert head.startswith("HTTP/1.1 200")  # plain HTTP/1.1 on the pump end (§3.7.3)
    ours.close()


def test_pr_5_adopt_without_fd_is_bad_request(server):
    srv, _ = server
    client, server_side = h.tcp_pair()
    resp = h.adopt_fd(srv.address, KID, server_side, "events", h.http_get("/x"), label(),
                      send_fd=False)
    assert resp["ok"] is False and resp["error"]["code"] == "bad_request"
    server_side.close()
    client.close()


def test_pr_5_adopt_with_share_transport_is_bad_request_on_a_unix_socket(server):
    srv, _ = server
    client, server_side = h.tcp_pair()
    resp = h.adopt_fd(srv.address, KID, server_side, "events", h.http_get("/x"), label(),
                      transport="share")
    assert resp["ok"] is False and resp["error"]["code"] == "bad_request"
    server_side.close()
    client.close()


def test_pr_5_adopt_with_an_unparsable_request_is_bad_request(server):
    srv, _ = server
    client, server_side = h.tcp_pair()
    resp = h.adopt_fd(srv.address, KID, server_side, "events", b"garbage", label())
    assert resp["ok"] is False and resp["error"]["code"] == "bad_request"
    server_side.close()
    client.close()


def test_pr_5_adopt_over_a_windows_pipe_is_bad_request():
    pytest.skip("Windows named-pipe control channel is not served yet (#55 follow-up)")


def test_pr_6_streams_close_by_share_id(server):
    srv, _ = server
    mine, mine_srv = h.tcp_pair()
    other, other_srv = h.tcp_pair()
    for sock, sid in ((mine_srv, "s_0123456789abcdef"), (other_srv, None)):
        r = h.adopt_fd(srv.address, KID, sock, "events", h.http_get("/api/kernels/%s/events" % KID),
                       label(share_id=sid))
        assert r["ok"]
        sock.close()
    h.read_head(mine)
    h.read_head(other)
    c = KernelClient(srv.address, KID)
    c.connect()
    try:
        assert c.request("streams.close", {"share_id": "s_0123456789abcdef"}) == {"closed": 1}
        assert c.request("streams.close", {"share_id": "s_0123456789abcdef"}) == {"closed": 0}
        with pytest.raises(p.DKPError) as info:
            c.request("streams.close", {})
        assert info.value.code == "bad_request"
    finally:
        c.close()
    assert h.is_closed(mine)
    assert not h.is_closed(other, timeout=1.0)
    other.close()
