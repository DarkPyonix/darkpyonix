"""DKP/1 control channel (FR-A1, PR-1, PR-2, PR-3, PR-4; PROTOCOL §3)."""
from __future__ import annotations

import os
import socket
import stat
import struct
import threading
import time

import pytest

from darkpyonix import _peer
from darkpyonix import _protocol as p
from darkpyonix._client import KernelClient
from darkpyonix._events import EventLog
from darkpyonix._server import ControlServer

KID = "k_0123456789abcdef0123"


def _handlers():
    def status(params):
        return {"status": "idle", "echo": params}

    def busy(params):
        raise p.DKPError("busy", "a run is in progress", {"run_id": "r1"})

    def boom(params):
        raise ValueError("secret internals")

    def slow(params):
        time.sleep(1.0)
        return {"slept": True}

    return {"status": status, "busy": busy, "boom": boom, "slow": slow}


@pytest.fixture
def events():
    return EventLog()


@pytest.fixture
def server(events, dp_home):
    srv = ControlServer(KID, events, _handlers())
    srv.start()
    yield srv
    srv.stop()


def _client(server, **kw):
    c = KernelClient(server.address, KID, **kw)
    c.connect()
    return c


def _raw_connect(server):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect(server.address)
    hello = p.recv_frame(s)
    return s, hello


def _raw_auth(server):
    s, hello = _raw_connect(server)
    s.sendall(p.encode({"op": "auth", "client": {"name": "raw", "kind": "test", "pid": 1}}))
    welcome = p.recv_frame(s)
    assert welcome["op"] == "welcome"
    return s


def _wait_closed(s, timeout):
    s.settimeout(timeout)
    try:
        while True:
            if not s.recv(65536):
                return True
    except (ConnectionError, OSError):
        return True


def test_fr_a1_same_account_connects_without_a_key(server, dp_home):
    good = _client(server)
    assert good.welcome["op"] == "welcome"
    assert good.welcome["session"].startswith("s_")
    assert good.welcome["seq"] == 0
    good.close()
    assert not os.path.exists(os.path.join(dp_home, "user.key"))


def test_fr_a1_hello_carries_identity_and_no_secret(server):
    s, hello = _raw_connect(server)
    assert hello["op"] == "hello" and hello["dkp"] == 1
    assert hello["kernel_id"] == KID
    assert hello["kernel_version"] == p.KERNEL_VERSION
    assert "nonce" not in hello
    s.sendall(p.encode({"op": "nonsense"}))
    err = p.recv_frame(s)
    assert err["op"] == "error" and err["code"] == "bad_request"
    assert _wait_closed(s, 2)
    s.close()


def test_fr_a1_no_tcp_port_is_opened(server):
    assert server._listen.family == socket.AF_UNIX
    assert not hasattr(server, "port")
    st = os.stat(server.address)
    assert stat.S_ISSOCK(st.st_mode) and st.st_mode & 0o777 == 0o600
    assert os.path.basename(server.address) == KID + ".sock"
    # Every socket this process listens on is a Unix socket: a TCP connect to any port the
    # control channel could have used finds nothing of ours (the sockets dir holds no port file).
    assert not any(n.endswith(".port") for n in os.listdir(os.path.dirname(server.address)))


def test_fr_a1_other_account_is_refused_by_peer_identity(server, monkeypatch):
    # The OS reports a different UID for the peer: the kernel closes without sending a frame.
    monkeypatch.setattr(_peer, "peer_uid", lambda sock: os.geteuid() + 1)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect(server.address)
    try:
        assert s.recv(65536) == b""   # EOF, never a hello
    except ConnectionError:
        pass
    s.close()
    monkeypatch.undo()
    assert _client(server).welcome["op"] == "welcome"  # the server went on serving


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() != 0,
                    reason="needs root to run a process as a second OS account (CI)")
def test_fr_a1_other_account_is_refused_by_peer_identity_real_uid(server):
    os.chmod(server.address, 0o666)   # take the file mode out of the picture: only the peer check is left
    pid = os.fork()
    if pid == 0:
        code = 2
        try:
            os.setgid(65534)
            os.setuid(65534)
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect(server.address)
            code = 0 if s.recv(65536) == b"" else 1
        except ConnectionError:
            code = 0
        finally:
            os._exit(code)
    _, status = os.waitpid(pid, 0)
    assert os.WEXITSTATUS(status) == 0


def test_fr_a1_manager_refuses_a_kernel_of_another_account(server, monkeypatch):
    # The client asks the OS who serves the socket and sends nothing if it is another account.
    seen = []
    monkeypatch.setattr(_peer, "same_account", lambda sock: seen.append(sock) or False)
    c = KernelClient(server.address, KID)
    with pytest.raises(p.DKPError) as e:
        c.connect()
    assert e.value.code == "kernel_unreachable" and seen
    assert server.client_count() == 0


def test_fr_a1_client_refuses_a_socket_of_another_kernel(server):
    with pytest.raises(p.DKPError) as e:
        KernelClient(server.address, "k_ffffffffffffffffffff").connect()
    assert e.value.code == "kernel_unreachable"


def test_fr_a1_user_tag_comes_from_the_os_account(dp_home):
    import hashlib
    from darkpyonix import _home
    expect = hashlib.sha256(("uid:%d" % os.getuid()).encode()).hexdigest()[:16]
    assert _home.user_tag() == expect
    assert not os.path.exists(os.path.join(dp_home, "user.key"))


def test_fr_a1_long_home_falls_back_to_a_short_private_socket_dir(dp_home, monkeypatch):
    from darkpyonix import _home
    long_home = os.path.join(dp_home, "x" * 120)
    monkeypatch.setenv("DARKPYONIX_HOME", long_home)
    path = _home.control_path(KID)
    assert len(os.fsencode(path)) < 104 and path.endswith(KID + ".sock")
    assert os.stat(os.path.dirname(path)).st_mode & 0o077 == 0


def test_pr_2_handshake_times_out(server, monkeypatch):
    monkeypatch.setattr(p, "HANDSHAKE_TIMEOUT", 0.3)
    s, _ = _raw_connect(server)
    t0 = time.monotonic()
    assert _wait_closed(s, 5)
    elapsed = time.monotonic() - t0
    assert 0.2 <= elapsed < 2.0
    s.close()


def test_pr_1_frame_too_large_closes_connection(server):
    s = _raw_auth(server)
    s.sendall(struct.pack(">I", p.MAX_FRAME + 1))
    err = p.recv_frame(s)
    assert err["op"] == "error" and err["code"] == "frame_too_large"
    assert _wait_closed(s, 2)
    s.close()


def test_request_response_and_errors(server):
    with _client(server) as c:
        assert c.request("status", {"a": 1}) == {"status": "idle", "echo": {"a": 1}}
        with pytest.raises(p.DKPError) as e:
            c.request("busy")
        assert e.value.code == "busy" and e.value.data == {"run_id": "r1"}
        with pytest.raises(p.DKPError) as e:
            c.request("nope")
        assert e.value.code == "unknown_method"
        with pytest.raises(p.DKPError) as e:
            c.request("boom")
        assert e.value.code == "internal"
        assert "Traceback" not in e.value.message
        assert c.request("status") == {"status": "idle", "echo": {}}


def test_malformed_request_is_bad_request(server):
    s = _raw_auth(server)
    s.sendall(p.encode({"op": "request", "id": 3, "method": "status", "params": [1]}))
    r = p.recv_frame(s)
    assert r["op"] == "response" and r["id"] == 3 and r["ok"] is False
    assert r["error"]["code"] == "bad_request"
    payload = b"{not json"
    s.sendall(struct.pack(">I", len(payload)) + payload)
    r = p.recv_frame(s)
    assert r["ok"] is False and r["error"]["code"] == "bad_request"
    s.sendall(p.encode({"op": "request", "id": 4, "method": "status"}))
    r = p.recv_frame(s)
    assert r["id"] == 4 and r["ok"] is True
    s.close()


def test_pr_4_unknown_fields_are_ignored(server):
    s = _raw_auth(server)
    s.sendall(p.encode({"op": "request", "id": 1, "method": "status", "params": {},
                        "future_field": {"x": 1}}))
    r = p.recv_frame(s)
    assert r["ok"] is True and r["result"]["status"] == "idle"
    s.close()


def test_pr_3_subscribe_replays_since_and_streams_live(server, events):
    for i in range(5):
        events.append("output", {"i": i})
    with _client(server) as c:
        assert c.welcome["seq"] == 5
        res = c.subscribe(since=2)
        assert res == {"seq": 5, "replayed": 3}
        got = [c.next_event(2) for _ in range(3)]
        assert [e["seq"] for e in got] == [3, 4, 5]
        assert got[0]["type"] == "output" and got[0]["data"] == {"i": 2}
        assert got[0]["op"] == "event" and got[0]["dkp"] == 1 and got[0]["time"].endswith("Z")
        events.append("kernel.status", {"status": "busy"})
        live = c.next_event(2)
        assert live["seq"] == 6 and live["type"] == "kernel.status"
        assert c.request("unsubscribe") == {}
        events.append("kernel.status", {"status": "idle"})
        assert c.next_event(0.3) is None


def test_pr_3_subscribe_without_since_streams_only_live(server, events):
    events.append("output", {"i": 0})
    with _client(server) as c:
        assert c.subscribe() == {"seq": 1, "replayed": 0}
        events.append("output", {"i": 1})
        assert c.next_event(2)["seq"] == 2


def test_pr_3_replay_truncated_when_ring_overflows(dp_home):
    log = EventLog(max_events=10)
    for i in range(25):
        log.append("output", {"i": i})
    evs, oldest = log.since(0)
    assert oldest == 16 and [e["seq"] for e in evs] == list(range(16, 26))
    assert log.since(20) == (log.since(20)[0], None)
    assert log.since(25) == ([], None)

    srv = ControlServer(KID, log, {})
    srv.start()
    try:
        with _client(srv) as c:
            res = c.subscribe(since=3)
            assert res == {"seq": 25, "replayed": 10}
            first = c.next_event(2)
            assert first["op"] == "event" and first["type"] == "replay_truncated"
            assert first["data"] == {"oldest_seq": 16}
            assert [c.next_event(2)["seq"] for _ in range(10)] == list(range(16, 26))
    finally:
        srv.stop()


def test_pr_3_ring_is_bounded_by_bytes():
    log = EventLog(max_events=1000, max_bytes=10000)
    for i in range(100):
        log.append("output", {"blob": "x" * 1000})
    evs, oldest = log.since(0)
    assert 0 < len(evs) <= 10 and oldest == evs[0]["seq"]
    assert evs[-1]["seq"] == log.seq == 100


def test_slow_subscriber_does_not_block_append(server, events):
    s = _raw_auth(server)  # subscribes and then never reads
    s.sendall(p.encode({"op": "request", "id": 1, "method": "subscribe", "params": {}}))
    assert p.recv_frame(s)["ok"] is True
    blob = "x" * 1000
    lat = []
    t0 = time.perf_counter()
    for i in range(10000):  # ~10 MB: far beyond the socket buffers of a reader that never reads
        a = time.perf_counter()
        events.append("output", {"i": i, "blob": blob})
        lat.append(time.perf_counter() - a)
    total = time.perf_counter() - t0
    lat.sort()
    # A blocking write would hang here forever; the bounds only absorb scheduler noise.
    assert lat[int(len(lat) * 0.99)] < 0.01, lat[int(len(lat) * 0.99)]
    assert lat[-1] < 1.0, lat[-1]
    assert total < 5.0, total
    with _client(server) as c:  # the server still answers others
        assert c.request("status")["status"] == "idle"
    s.close()


def test_two_clients_receive_same_events(server, events):
    with _client(server, name="a") as a, _client(server, name="b") as b:
        a.subscribe()
        b.subscribe()
        n = 200
        for i in range(n):
            events.append("output", {"i": i})
        sa = [a.next_event(2)["seq"] for _ in range(n)]
        sb = [b.next_event(2)["seq"] for _ in range(n)]
        assert sa == sb == list(range(1, n + 1))
        assert server.client_count() == 2


def test_handler_blocking_does_not_block_other_requests(server):
    with _client(server) as slow, _client(server) as fast:
        out = {}
        t = threading.Thread(target=lambda: out.setdefault("r", slow.request("slow")))
        t.start()
        time.sleep(0.1)
        a = time.perf_counter()
        assert fast.request("status")["status"] == "idle"
        assert slow.request("status")["status"] == "idle"  # same connection too
        assert time.perf_counter() - a < 0.3
        t.join(3)
        assert out["r"] == {"slept": True}


def test_event_latency_p99(server, events, capsys):
    with _client(server) as c:
        c.subscribe()
        lat = []
        for i in range(1000):
            a = time.perf_counter()
            events.append("output", {"i": i})
            ev = c.next_event(2)
            lat.append(time.perf_counter() - a)
            assert ev["seq"] == i + 1
        lat.sort()
        p50, p99 = lat[len(lat) // 2], lat[int(len(lat) * 0.99)]
        with capsys.disabled():
            print("\nevent latency append->client: p50 %.3f ms, p99 %.3f ms"
                  % (p50 * 1e3, p99 * 1e3))
        assert p99 < 0.1


def test_client_close_ends_event_stream(server):
    c = _client(server)
    c.subscribe()
    c.close()
    assert list(c.events()) == []


def test_fr_d_announce_names_the_runtime_home(dp_home):
    """The user_tag is shared by every home of an account, so the announce says which home it is."""
    import importlib
    main = importlib.import_module("darkpyonix.__main__")
    k = main.Kernel(os.path.join(dp_home, "nb.py"))
    assert k.info()["home"] == dp_home
