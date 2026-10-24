"""Kernel access tokens (SPEC FR-K10, FR-A5, FR-A6, PR-7; PROTOCOL §6).

The kernel owns its access tokens: initial, login and share tokens. It issues, verifies, revokes
and persists them; managers only ask through ``tokens.issue/verify/revoke/list``. Only SHA-256
hashes are stored (tokens are 256-bit random, so no salt or slow hash is needed). The store is
``<runs dir>/tokens.json`` (mode 0600), beside the notebook it belongs to, and it is deleted once
the notebook file is gone. Standard library only; Python 3.8+.
"""
from __future__ import annotations

import calendar
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from . import _home
from ._protocol import DKPError, now_iso
from ._runs import runs_dir_for

CAPABILITIES = ("read", "history", "execute", "edit", "manage")
KINDS = ("initial", "login", "share")
STORE_NAME = "tokens.json"


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _parse_time(value: Any) -> float:
    if isinstance(value, str):
        try:
            return float(calendar.timegm(time.strptime(value.strip().split(".")[0].rstrip("Z") + "Z",
                                                       "%Y-%m-%dT%H:%M:%SZ")))
        except ValueError:
            pass
    raise DKPError("bad_request", "expires_at must be an ISO 8601 UTC timestamp")


def _capabilities(value: Any) -> List[str]:
    if not isinstance(value, list) or not value or not all(isinstance(c, str) for c in value):
        raise DKPError("bad_request", "capabilities must be a non-empty list of names")
    unknown = [c for c in value if c not in CAPABILITIES]
    if unknown:
        raise DKPError("bad_request", "unknown capability %r" % (unknown[0],),
                       {"allowed": list(CAPABILITIES)})
    return [c for c in CAPABILITIES if c in value]  # canonical order, no duplicates


class TokenStore(object):
    """Tokens of one kernel. ``close_streams(share_id) -> int`` closes the streams handed off
    with that share (set by the handoff code, PROTOCOL §3.7.4); the default closes none."""

    def __init__(self, path: str, clock: Callable[[], float] = time.time,
                 close_streams: Optional[Callable[[str], int]] = None) -> None:
        self.path = os.path.abspath(path)
        self.store_path = os.path.join(runs_dir_for(self.path), STORE_NAME)
        self.clock = clock
        self.close_streams = close_streams or (lambda share_id: 0)
        self._lock = threading.Lock()
        self._tokens = self._load()  # type: List[Dict[str, Any]]
        self._stop = threading.Event()
        self._thread = None  # type: Optional[threading.Thread]

    # ------------------------------------------------------------ persistence

    def _load(self) -> List[Dict[str, Any]]:
        try:
            with open(self.store_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            tokens = data.get("tokens", [])
            return [t for t in tokens if isinstance(t, dict) and isinstance(t.get("sha256"), str)]
        except (OSError, ValueError, AttributeError):
            return []

    def _save(self) -> None:
        os.makedirs(os.path.dirname(self.store_path), exist_ok=True)
        blob = json.dumps({"version": 1, "tokens": self._tokens}, indent=1).encode("utf-8")
        _home.write_private(self.store_path, blob)

    # ------------------------------------------------------------ DKP methods

    def handlers(self) -> Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]:
        return {
            "tokens.issue": self.issue,
            "tokens.verify": self.verify,
            "tokens.revoke": self.revoke,
            "tokens.list": self.list,
        }

    def issue(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params = params or {}
        kind = params.get("kind")
        if kind not in KINDS:
            raise DKPError("bad_request", "kind must be one of %s" % ", ".join(KINDS))
        caps = _capabilities(params.get("capabilities"))
        label = params.get("label")
        if label is not None and not isinstance(label, str):
            raise DKPError("bad_request", "label must be a string")
        expires = params.get("expires_at")
        expires_unix = _parse_time(expires) if expires is not None else None
        token = secrets.token_urlsafe(32)
        entry = {
            "sha256": _hash(token), "kind": kind, "capabilities": caps,
            "share_id": None, "label": label, "created_at": now_iso(),
            "expires_at": expires, "expires_unix": expires_unix, "revoked_at": None,
        }  # type: Dict[str, Any]
        out = {"token": token}  # type: Dict[str, Any]
        if kind == "share":
            entry["share_id"] = "s_" + secrets.token_hex(8)
            out["share_id"] = entry["share_id"]
        with self._lock:
            self._tokens.append(entry)
            self._save()
        return out

    def verify(self, params: Dict[str, Any]) -> Dict[str, Any]:
        token = (params or {}).get("token")
        if not isinstance(token, str) or not token:
            raise DKPError("bad_request", "token is required")
        digest = _hash(token)
        with self._lock:
            found = None
            for t in self._tokens:
                if hmac.compare_digest(t["sha256"], digest):
                    found = t
        if found is None:
            return {"valid": False, "revoked": False}
        if found.get("revoked_at"):
            return {"valid": False, "revoked": True, "kind": found["kind"],
                    "share_id": found.get("share_id")}
        exp = found.get("expires_unix")
        if exp is not None and exp <= self.clock():
            return {"valid": False, "revoked": False, "kind": found["kind"],
                    "share_id": found.get("share_id")}
        out = {"valid": True, "revoked": False, "kind": found["kind"],
               "capabilities": list(found["capabilities"])}  # type: Dict[str, Any]
        if found.get("share_id"):
            out["share_id"] = found["share_id"]
            out["label"] = found.get("label")
        return out

    def revoke(self, params: Dict[str, Any]) -> Dict[str, Any]:
        share_id = (params or {}).get("share_id")
        if not isinstance(share_id, str) or not share_id:
            raise DKPError("bad_request", "share_id is required")
        n = 0
        with self._lock:
            for t in self._tokens:
                if t.get("share_id") == share_id and not t.get("revoked_at"):
                    t["revoked_at"] = now_iso()
                    n += 1
            if n:
                self._save()
        closed = self.close_streams(share_id) if n else 0
        return {"revoked": n, "closed": closed}

    def list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            shares = [{
                "share_id": t["share_id"], "capabilities": list(t["capabilities"]),
                "label": t.get("label"), "created_at": t["created_at"],
                "expires_at": t.get("expires_at"), "revoked_at": t.get("revoked_at"),
            } for t in self._tokens if t.get("share_id")]
        return {"shares": shares}

    # ------------------------------------------------------------ lifetime (FR-A5)

    def check_file(self) -> bool:
        """Delete the store when the notebook file is gone. True if it was."""
        if os.path.exists(self.path):
            return False
        with self._lock:
            self._tokens = []
            try:
                os.unlink(self.store_path)
            except OSError:
                pass
        return True

    def start(self, interval: float = 0.5) -> None:
        def loop() -> None:
            while not self._stop.wait(interval):
                self.check_file()
        self._thread = threading.Thread(target=loop, name="dpx-tokens", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
