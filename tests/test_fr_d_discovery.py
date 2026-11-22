"""FR-D1, FR-D2: multicast discovery only, no registry files (PROTOCOL §2, INTENT D4)."""
from __future__ import annotations

import json
import os
import signal
import socket
import threading
import time

import pytest

from darkpyonix import _home
from darkpyonix import _discovery as discovery, _launcher as launcher
from darkpyonix._protocol import QUERY_TIMEOUT, kernel_id_for

from kernel_procs import kill, notebook, reap, start

pytestmark = pytest.mark.skipif(os.name == "nt", reason="POSIX signals in these tests")


@pytest.fixture
def multicast(monkeypatch):
    monkeypatch.delenv("DARKPYONIX_DISCOVERY", raising=False)


def _start_many(python, scratch, n):
    procs, ids = [], []
    for i in range(n):
        path = notebook(scratch, "nb%d.py" % i)
        procs.append(start(python, path))
        ids.append(kernel_id_for(path))
    return procs, ids


def _wait_all(procs, ids):
    for proc, kid in zip(procs, ids):
        assert launcher.wait_for_announce(kid, pid=proc.pid, timeout=15) is not None, kid


def test_fr_d1_query_finds_all_kernels(python, dp_home, scratch, multicast):
    procs, ids = _start_many(python, scratch, 3)
    try:
        _wait_all(procs, ids)
        client = discovery.DiscoveryClient(_home.user_tag())
        try:
            t0 = time.perf_counter()
            found = client.query(timeout=QUERY_TIMEOUT, expect=3)
            elapsed = time.perf_counter() - t0
        finally:
            client.close()
        assert sorted(k["kernel_id"] for k in found) == sorted(ids)
        assert all(k["status"] == "idle" for k in found)
        assert elapsed < 0.2, "query took %.1f ms" % (elapsed * 1000)
        print("FR-D1 query latency for 3 kernels: %.2f ms" % (elapsed * 1000))

        # A query naming one kernel gets only that kernel.
        client = discovery.DiscoveryClient(_home.user_tag())
        try:
            one = client.query(kernel_id=ids[1])
        finally:
            client.close()
        assert [k["kernel_id"] for k in one] == [ids[1]]
    finally:
        for p in procs:
            reap(p)


def test_fr_d1_other_user_tag_is_ignored(python, dp_home, scratch, multicast):
    procs, ids = _start_many(python, scratch, 1)
    try:
        _wait_all(procs, ids)
        other = "0123456789abcdef"
        assert other != _home.user_tag()
        client = discovery.DiscoveryClient(other)
        try:
            assert client.query(timeout=0.3) == []
        finally:
            client.close()
        client = discovery.DiscoveryClient(_home.user_tag())
        try:
            assert [k["kernel_id"] for k in client.query(timeout=0.3)] == ids
        finally:
            client.close()
    finally:
        for p in procs:
            reap(p)


def test_fr_d1_listener_sees_announce_and_bye(python, dp_home, scratch, multicast):
    seen = []
    client = discovery.DiscoveryClient(_home.user_tag())
    client.listen(seen.append)
    procs, ids = _start_many(python, scratch, 1)
    try:
        _wait_all(procs, ids)
        reap(procs[0])  # SIGTERM: clean shutdown sends bye
        deadline = time.time() + 5
        while time.time() < deadline and not any(m["op"] == "bye" for m in seen):
            time.sleep(0.02)
    finally:
        client.close()
        for p in procs:
            reap(p)
    assert any(m["op"] == "announce" and m["kernel_id"] == ids[0] for m in seen)
    assert any(m["op"] == "bye" and m["kernel_id"] == ids[0] for m in seen)


def _json_files(home):
    found = []
    for root, _dirs, names in os.walk(home):
        found += [os.path.join(root, n) for n in names if n.endswith(".json")]
    return found


def test_fr_d2_kernel_writes_no_registry_file(python, dp_home, scratch, multicast):
    procs, ids = _start_many(python, scratch, 2)
    try:
        _wait_all(procs, ids)
        assert not [f for f in _json_files(dp_home) if os.sep + "kernels" + os.sep in f]
    finally:
        for p in procs:
            reap(p)
    assert not [f for f in _json_files(dp_home) if os.sep + "kernels" + os.sep in f]


def test_fr_d2_registry_env_is_ignored(python, dp_home, scratch, monkeypatch):
    monkeypatch.setenv("DARKPYONIX_DISCOVERY", "registry")
    procs, ids = _start_many(python, scratch, 2)
    try:
        _wait_all(procs, ids)
        found = discovery.discover()
        assert sorted(k["kernel_id"] for k in found) == sorted(ids)
        client = discovery.DiscoveryClient(_home.user_tag())
        try:
            assert sorted(k["kernel_id"] for k in client.query(timeout=0.3, expect=2)) == sorted(ids)
        finally:
            client.close()
        assert not [f for f in _json_files(dp_home) if os.sep + "kernels" + os.sep in f]
    finally:
        for p in procs:
            reap(p)


def test_fr_d2_killed_kernel_vanishes_without_pruning(python, dp_home, scratch, multicast):
    procs, ids = _start_many(python, scratch, 2)
    try:
        _wait_all(procs, ids)
        kill(procs[0].pid, signal.SIGKILL)
        procs[0].wait(5)
        found = discovery.discover()
        assert [k["kernel_id"] for k in found] == [ids[1]]
    finally:
        for p in procs:
            reap(p)


def test_fr_d1_announce_from_another_home_is_dropped(python, dp_home, scratch, multicast, monkeypatch):
    procs, ids = _start_many(python, scratch, 1)
    try:
        _wait_all(procs, ids)
        # The same home through a symlink is the same home (canonical comparison).
        alias = os.path.join(scratch, "home-link")
        os.symlink(dp_home, alias)
        client = discovery.DiscoveryClient(_home.user_tag(), home=alias)
        try:
            assert [k["kernel_id"] for k in client.query(timeout=0.3, expect=1)] == ids
        finally:
            client.close()
        other = os.path.join(scratch, "other-home")
        os.makedirs(other, mode=0o700)
        client = discovery.DiscoveryClient(_home.user_tag(), home=other)
        try:
            assert client.query(timeout=0.3, expect=1) == []
        finally:
            client.close()
        # discover() filters by this process's DARKPYONIX_HOME.
        monkeypatch.setenv("DARKPYONIX_HOME", other)
        assert discovery.discover(timeout=0.3) == []
        monkeypatch.setenv("DARKPYONIX_HOME", alias)
        assert [k["kernel_id"] for k in discovery.discover(timeout=0.3)] == ids
    finally:
        for p in procs:
            reap(p)


def test_fr_d1_announce_without_home_is_dropped_when_a_home_is_set(dp_home, multicast):
    """A hand-made announce that omits ``home`` is not counted (PROTOCOL §2.4)."""
    tag = _home.user_tag()
    sock = discovery.open_group_socket()
    sock.settimeout(0.2)
    stop = threading.Event()

    def answer():
        while not stop.is_set():
            try:
                data, addr = sock.recvfrom(discovery.MAX_DATAGRAM)
            except (socket.timeout, OSError):
                continue
            query = json.loads(data.decode("utf-8"))
            if query.get("op") != "query" or query.get("user_tag") != tag:
                continue
            reply = {"dkp": 1, "op": "announce", "user_tag": tag, "nonce": query["nonce"],
                     "kernel_id": "k_00000000000000000000", "status": "idle"}
            out = discovery.open_send_socket()
            try:
                out.sendto(json.dumps(reply).encode("utf-8"), addr)
                out.sendto(json.dumps(dict(reply, kernel_id="k_11111111111111111111",
                                           home=dp_home)).encode("utf-8"), addr)
            finally:
                out.close()

    thread = threading.Thread(target=answer, daemon=True)
    thread.start()
    try:
        found = discovery.discover(timeout=0.5)
    finally:
        stop.set()
        thread.join(2)
        sock.close()
    assert [k["kernel_id"] for k in found] == ["k_11111111111111111111"]
