"""Kernel access tokens held by the kernel (SPEC FR-K10, FR-A5, FR-A6, PR-7; PROTOCOL §6).

``TokenStore`` is driven directly and through a real ``ControlServer`` + ``KernelClient`` pair.
"""
from __future__ import annotations

import json
import os

import pytest

from darkpyonix import _protocol as p
from darkpyonix._client import KernelClient
from darkpyonix._events import EventLog
from darkpyonix._server import ControlServer
from darkpyonix._tokens import CAPABILITIES, TokenStore

KID = "k_0123456789abcdef0123"


@pytest.fixture()
def nb(tmp_path):
    path = tmp_path / "nb.py"
    path.write_text("x = 1\n")
    return str(path)


def make(nb, **kw):
    return TokenStore(nb, **kw)


def test_pr_7_issue_returns_the_token_once(nb):
    ts = make(nb)
    out = ts.issue({"kind": "initial", "capabilities": list(CAPABILITIES)})
    assert isinstance(out["token"], str) and len(out["token"]) >= 32
    assert "share_id" not in out
    shared = ts.issue({"kind": "share", "capabilities": ["read"], "label": "bob"})
    assert shared["share_id"].startswith("s_") and len(shared["share_id"]) == 18
    again = ts.issue({"kind": "initial", "capabilities": list(CAPABILITIES)})
    assert again["token"] != out["token"]  # no issue-once rule (user decision 2026-10-04)
    v = ts.verify({"token": out["token"]})
    assert v["valid"] is True and v["revoked"] is False and v["kind"] == "initial"
    assert v["capabilities"] == list(CAPABILITIES)


def test_pr_7_list_never_returns_tokens(nb):
    ts = make(nb)
    a = ts.issue({"kind": "share", "capabilities": ["read", "history"], "label": "x"})
    ts.issue({"kind": "initial", "capabilities": list(CAPABILITIES)})
    listed = ts.list({})
    assert [s["share_id"] for s in listed["shares"]] == [a["share_id"]]
    blob = json.dumps(listed)
    assert a["token"] not in blob and "sha256" not in blob and "hash" not in blob
    assert set(listed["shares"][0]) == {"share_id", "capabilities", "label", "created_at",
                                        "expires_at", "revoked_at"}


def test_pr_7_unknown_capability_is_bad_request(nb):
    ts = make(nb)
    for bad in (
        {"kind": "share", "capabilities": ["read", "admin"]},
        {"kind": "share", "capabilities": []},
        {"kind": "share", "capabilities": "read"},
        {"kind": "bogus", "capabilities": ["read"]},
        {"kind": "share", "capabilities": ["read"], "expires_at": "tomorrow"},
    ):
        with pytest.raises(p.DKPError) as e:
            ts.issue(bad)
        assert e.value.code == "bad_request"
    with pytest.raises(p.DKPError) as e:
        ts.verify({})
    assert e.value.code == "bad_request"


def test_fr_a5_tokens_survive_a_new_kernel_process(nb):
    ts = make(nb)
    init = ts.issue({"kind": "initial", "capabilities": list(CAPABILITIES)})
    sh = ts.issue({"kind": "share", "capabilities": ["read"]})
    ts.revoke({"share_id": sh["share_id"]})
    ts2 = make(nb)  # a restarted kernel reads the store
    assert ts2.verify({"token": init["token"]})["valid"] is True
    v = ts2.verify({"token": sh["token"]})
    assert v["valid"] is False and v["revoked"] is True  # 2025 "blacklisted" is kept


def test_fr_k10_store_holds_hashes_only_and_is_private(nb):
    ts = make(nb)
    t = ts.issue({"kind": "initial", "capabilities": list(CAPABILITIES)})["token"]
    raw = open(ts.store_path, "rb").read().decode()
    assert t not in raw
    assert json.loads(raw)["tokens"]
    if os.name == "posix":
        assert os.stat(ts.store_path).st_mode & 0o077 == 0


def test_fr_a5_revoke_closes_streams_and_reissue_gives_a_new_token(nb):
    closed = []
    ts = make(nb, close_streams=lambda sid: closed.append(sid) or 2)
    a = ts.issue({"kind": "share", "capabilities": ["read"]})
    out = ts.revoke({"share_id": a["share_id"]})
    assert out == {"revoked": 1, "closed": 2} and closed == [a["share_id"]]
    assert ts.revoke({"share_id": a["share_id"]}) == {"revoked": 0, "closed": 0}
    b = ts.issue({"kind": "share", "capabilities": ["read"]})
    assert b["token"] != a["token"]
    assert ts.verify({"token": b["token"]})["valid"] is True
    assert ts.verify({"token": a["token"]})["valid"] is False


def test_fr_a6_expired_share_is_not_valid(nb):
    ts = make(nb, clock=lambda: 1000.0)
    t = ts.issue({"kind": "share", "capabilities": ["read"], "expires_at": "1970-01-01T00:20:00Z"})
    assert ts.verify({"token": t["token"]})["valid"] is True
    ts2 = make(nb, clock=lambda: 1300.0)
    v = ts2.verify({"token": t["token"]})
    assert v["valid"] is False and v["revoked"] is False


def test_fr_a6_unknown_token_is_simply_invalid(nb):
    v = make(nb).verify({"token": "nope"})
    assert v == {"valid": False, "revoked": False}


def test_fr_a5_file_deleted_removes_the_store(nb):
    ts = make(nb)
    t = ts.issue({"kind": "initial", "capabilities": list(CAPABILITIES)})["token"]
    assert os.path.exists(ts.store_path)
    os.unlink(nb)
    assert ts.check_file() is True
    assert not os.path.exists(ts.store_path)
    assert make(nb).verify({"token": t})["valid"] is False


def test_pr_7_methods_travel_over_dkp(nb):
    ts = make(nb)
    events = EventLog()
    srv = ControlServer(KID, events, ts.handlers())
    srv.start()
    c = KernelClient(srv.address, KID)
    try:
        c.connect()
        t = c.request("tokens.issue", {"kind": "login", "capabilities": list(CAPABILITIES)})
        v = c.request("tokens.verify", {"token": t["token"]})
        assert v["valid"] and v["kind"] == "login"
        with pytest.raises(p.DKPError) as e:
            c.request("tokens.issue", {"kind": "share", "capabilities": ["nope"]})
        assert e.value.code == "bad_request"
    finally:
        c.close()
        srv.stop()
