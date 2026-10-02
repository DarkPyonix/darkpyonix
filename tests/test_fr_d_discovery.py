"""FR-D1, FR-D2: multicast discovery and the registry fallback (PROTOCOL §2)."""
from __future__ import annotations

import json
import os
import signal
import time

import pytest

from darkpyonix import _home
from darkpyonix.kernel import discovery, launcher, registry
from darkpyonix.kernel.protocol import QUERY_TIMEOUT, kernel_id_for

from kernel_procs import kill, notebook, reap, start

pytestmark = pytest.mark.skipif(os.name == "nt", reason="POSIX signals in these tests")


@pytest.fixture
def multicast(monkeypatch):
    monkeypatch.delenv("DARKPYONIX_DISCOVERY", raising=False)


@pytest.fixture
def registry_only(monkeypatch):
    monkeypatch.setenv("DARKPYONIX_DISCOVERY", "registry")


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


def test_fr_d2_registry_fallback_finds_kernels(python, dp_home, scratch, registry_only):
    procs, ids = _start_many(python, scratch, 2)
    try:
        _wait_all(procs, ids)
        found = discovery.discover()
        assert sorted(k["kernel_id"] for k in found) == sorted(ids)
        # The kernels really are multicast-silent in this mode.
        client = discovery.DiscoveryClient(_home.user_tag())
        try:
            assert client.query(timeout=0.3) == []
        finally:
            client.close()
        entry = json.load(open(os.path.join(_home.kernels_dir(), ids[0] + ".json")))
        assert entry["kernel_id"] == ids[0] and "nonce" not in entry
    finally:
        for p in procs:
            reap(p)


def test_fr_d2_stale_registry_is_pruned(python, dp_home, scratch, multicast):
    procs, ids = _start_many(python, scratch, 2)
    try:
        _wait_all(procs, ids)
        kill(procs[0].pid, signal.SIGKILL)
        procs[0].wait(5)
        stale = os.path.join(_home.kernels_dir(), ids[0] + ".json")
        assert os.path.exists(stale)  # kill -9 leaves the entry behind
        found = discovery.discover()
        assert [k["kernel_id"] for k in found] == [ids[1]]
        assert not os.path.exists(stale)
    finally:
        for p in procs:
            reap(p)


def test_fr_d2_registry_ignores_other_users(dp_home):
    registry.write({"kernel_id": "k_" + "0" * 20, "pid": os.getpid(), "user_tag": "ffffffffffffffff"})
    registry.write({"kernel_id": "k_" + "1" * 20, "pid": os.getpid(), "user_tag": _home.user_tag()})
    assert [k["kernel_id"] for k in registry.scan()] == ["k_" + "1" * 20]
