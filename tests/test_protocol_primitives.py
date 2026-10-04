"""PROTOCOL §2.6, §3.1, §3.2 primitives (FR-K2, FR-A1, PR-1)."""
from __future__ import annotations

import os
import socket
import struct

import pytest

from darkpyonix import _home
from darkpyonix import _protocol as p


def test_fr_k2_kernel_id_is_stable_across_path_spellings(scratch, monkeypatch):
    f = os.path.join(scratch, "train.py")
    open(f, "w").close()
    link = os.path.join(scratch, "alias.py")
    os.symlink(f, link)
    monkeypatch.chdir(scratch)
    ids = {p.kernel_id_for(f), p.kernel_id_for("train.py"), p.kernel_id_for(link),
           p.kernel_id_for(os.path.join(scratch, ".", "train.py"))}
    assert len(ids) == 1
    (kid,) = ids
    assert kid.startswith("k_") and len(kid) == 22
    other = os.path.join(scratch, "eval.py")
    open(other, "w").close()
    assert p.kernel_id_for(other) != kid


def test_frame_roundtrip_over_socketpair():
    a, b = socket.socketpair()
    try:
        a.sendall(p.encode({"op": "request", "id": 1, "method": "status", "params": {}}))
        msg = p.recv_frame(b)
        assert msg == {"op": "request", "id": 1, "method": "status", "params": {}, "dkp": 1}
    finally:
        a.close()
        b.close()


def test_pr_1_oversized_frame_is_refused():
    dec = p.FrameDecoder()
    with pytest.raises(p.FrameTooLarge):
        dec.feed(struct.pack(">I", p.MAX_FRAME + 1))


def test_frame_decoder_handles_partial_reads():
    data = p.encode({"op": "event", "seq": 3}) + p.encode({"op": "event", "seq": 4})
    dec = p.FrameDecoder()
    frames = []
    for i in range(len(data)):
        frames += dec.feed(data[i:i + 1])
    assert [f["seq"] for f in frames] == [3, 4]


def test_fr_a1_user_key_is_created_once_with_0600(dp_home):
    k1 = _home.user_key()
    k2 = _home.user_key()
    assert k1 == k2 and len(k1) == 32
    mode = os.stat(os.path.join(dp_home, "user.key")).st_mode & 0o777
    assert mode == 0o600
    assert len(_home.user_tag()) == 16


def test_fr_a1_mac_verification():
    key, nonce = os.urandom(32), p.new_nonce()
    mac = p.auth_mac(key, nonce, "k_0123456789abcdef0123")
    assert p.verify_mac(key, nonce, "k_0123456789abcdef0123", mac)
    assert not p.verify_mac(os.urandom(32), nonce, "k_0123456789abcdef0123", mac)
    assert not p.verify_mac(key, nonce, "k_ffffffffffffffffffff", mac)


def test_run_id_format():
    import re
    assert re.match(r"^\d{8}-\d{6}-[0-9a-f]{4}$", p.new_run_id())
