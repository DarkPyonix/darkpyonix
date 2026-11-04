"""Windows named-pipe control channel (FR-A1, PR-2, PR-5; PROTOCOL §3.1-§3.2; #80).

The first group runs everywhere: it covers names, the DACL text, the socket adapter and the bridge
with a fake pipe stream. The second group needs a real Windows pipe and is skipped elsewhere; it runs
in the Windows CI job.
"""
from __future__ import annotations

import socket
import threading
import time

import pytest

from darkpyonix import _protocol as p
from darkpyonix import _winpipe
from darkpyonix._client import KernelClient
from darkpyonix._events import EventLog
from darkpyonix._server import ControlServer

KID = "k_0123456789abcdef0123"
windows_only = pytest.mark.skipif(__import__("os").name != "nt", reason="needs a Windows named pipe")


class FakeStream:
    """The stream protocol ``_winpipe`` expects, backed by two socket pairs' worth of queues."""

    def __init__(self):
        self.to_peer = bytearray()
        self.from_peer = []          # chunks the 'client' will deliver
        self.cond = threading.Condition()
        self.closed = False
        self.aborted = False
        self.flushed = False

    def feed(self, data):
        with self.cond:
            self.from_peer.append(data)
            self.cond.notify_all()

    def read(self, n, timeout):
        with self.cond:
            end = None if timeout is None else time.monotonic() + timeout
            while not self.from_peer:
                if self.closed or self.aborted:
                    return b""
                left = None if end is None else end - time.monotonic()
                if left is not None and left <= 0:
                    raise socket.timeout("timed out")
                self.cond.wait(left)
            chunk = self.from_peer.pop(0)
            if len(chunk) > n:
                self.from_peer.insert(0, chunk[n:])
                chunk = chunk[:n]
            return chunk

    def write(self, data):
        with self.cond:
            self.to_peer += data

    def flush(self):
        self.flushed = True

    def abort(self):
        with self.cond:
            self.aborted = True
            self.cond.notify_all()

    def close(self):
        with self.cond:
            self.closed = True
            self.cond.notify_all()


def test_fr_a1_windows_pipe_name_uses_user_tag_and_kernel_id():
    name = _winpipe.pipe_name("0123456789abcdef", KID)
    assert name == "\\\\.\\pipe\\darkpyonix-0123456789abcdef-" + KID
    assert _winpipe.is_pipe_name(name)
    assert _winpipe.is_pipe_name(name.upper())
    assert not _winpipe.is_pipe_name("/home/u/.darkpyonix/sockets/k.sock")


def test_fr_a1_windows_pipe_dacl_allows_only_own_sid():
    sddl = _winpipe.dacl_sddl("S-1-5-21-1-2-3-1001")
    assert sddl == "D:P(A;;GA;;;S-1-5-21-1-2-3-1001)"
    assert sddl.count("(A;") == 1 and "WD" not in sddl and "AN" not in sddl   # one ACE, not Everyone


def test_fr_a1_pipe_socket_adapts_a_stream_to_the_socket_calls():
    stream = FakeStream()
    sock = _winpipe.PipeSocket(stream)
    stream.feed(p.encode({"op": "hello", "kernel_id": KID}))
    assert p.recv_frame(sock)["kernel_id"] == KID            # recv_frame works on the adapter
    sock.sendall(p.encode({"op": "auth"}))
    assert bytes(stream.to_peer) == p.encode({"op": "auth"})
    sock.settimeout(0.05)
    assert sock.gettimeout() == 0.05
    with pytest.raises(socket.timeout):
        sock.recv(10)
    sock.shutdown(socket.SHUT_RDWR)
    assert sock.recv(10) == b""                              # aborted: end of stream
    sock.close()
    assert stream.closed


def test_fr_a1_refused_pipe_peer_gets_no_byte():
    stream = FakeStream()
    delivered = []
    _winpipe.serve_connection(stream, delivered.append, lambda s: False)
    assert delivered == [] and stream.closed and bytes(stream.to_peer) == b""


def test_fr_a1_failing_account_check_counts_as_a_refusal():
    stream = FakeStream()

    def boom(_):
        raise OSError(5, "access denied")
    delivered = []
    _winpipe.serve_connection(stream, delivered.append, boom)
    assert delivered == [] and stream.closed


def test_fr_a1_accepted_pipe_peer_is_bridged_both_ways():
    stream = FakeStream()
    delivered = []
    _winpipe.serve_connection(stream, delivered.append, lambda s: True)
    far = delivered[0]
    far.settimeout(5)
    stream.feed(b"ping")
    assert far.recv(16) == b"ping"                           # pipe -> server side
    far.sendall(b"pong")
    deadline = time.monotonic() + 5
    while bytes(stream.to_peer) != b"pong" and time.monotonic() < deadline:
        time.sleep(0.01)
    assert bytes(stream.to_peer) == b"pong"                  # server side -> pipe
    far.close()                                              # the server closes: pipe is flushed and closed
    deadline = time.monotonic() + 5
    while not stream.closed and time.monotonic() < deadline:
        time.sleep(0.01)
    assert stream.closed and stream.flushed


def test_fr_a1_client_refuses_a_pipe_name_off_windows():
    if __import__("os").name == "nt":
        pytest.skip("this is the non-Windows refusal")
    with pytest.raises(p.DKPError) as e:
        KernelClient("\\\\.\\pipe\\darkpyonix-x-" + KID, KID).connect()
    assert e.value.code == "kernel_unreachable"


# ------------------------------------------------------------------ real pipes (Windows CI)

@pytest.fixture
def pipe_server(dp_home):
    srv = ControlServer(KID, EventLog(), {"status": lambda params: {"status": "idle"}})
    srv.start()
    yield srv
    srv.stop()


@windows_only
def test_fr_a1_windows_pipe_same_account_reaches_welcome(pipe_server):
    assert pipe_server.address.startswith("\\\\.\\pipe\\darkpyonix-")
    c = KernelClient(pipe_server.address, KID)
    c.connect()
    try:
        assert c.welcome["op"] == "welcome"
        assert c.request("status") == {"status": "idle"}
    finally:
        c.close()


@windows_only
def test_fr_a1_windows_pipe_serves_many_connections_in_turn(pipe_server):
    for _ in range(3):
        c = KernelClient(pipe_server.address, KID)
        c.connect()
        c.close()
    a, b = KernelClient(pipe_server.address, KID), KernelClient(pipe_server.address, KID)
    a.connect(), b.connect()
    assert a.request("status") and b.request("status")
    a.close(), b.close()


@windows_only
def test_fr_a1_windows_pipe_peers_report_our_own_sid(pipe_server):
    pipe = _winpipe.connect_client(pipe_server.address, 5)
    try:
        assert _winpipe.same_server_account(pipe)
        assert pipe.peer_sid(server_end=False) == _winpipe.own_sid()
    finally:
        pipe.close()


@windows_only
def test_fr_a1_windows_second_kernel_cannot_take_the_pipe_name(pipe_server):
    with pytest.raises(OSError):
        _winpipe.PipeListener(pipe_server.address, lambda s: None)   # FILE_FLAG_FIRST_PIPE_INSTANCE


@windows_only
def test_pr_2_handshake_times_out_on_a_windows_pipe(pipe_server, monkeypatch):
    monkeypatch.setattr(p, "HANDSHAKE_TIMEOUT", 0.3)
    pipe = _winpipe.connect_client(pipe_server.address, 5)
    sock = _winpipe.PipeSocket(pipe)
    sock.settimeout(5)
    assert p.recv_frame(sock)["op"] == "hello"
    t0 = time.monotonic()
    assert sock.recv(65536) == b""                           # no auth sent: the kernel closes
    assert 0.2 <= time.monotonic() - t0 < 2.0
    sock.close()


@windows_only
def test_pr_5_adopt_over_a_windows_pipe_is_bad_request(pipe_server):
    c = KernelClient(pipe_server.address, KID)
    c.connect()
    try:
        for transport in ("fd", "pump"):
            with pytest.raises(p.DKPError) as e:
                c.request("adopt", {"kind": "http", "transport": transport, "request": ""})
            assert e.value.code == "bad_request"
    finally:
        c.close()
