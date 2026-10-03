"""Manager tokens and permissions (SPEC FR-A2, FR-A3).

The master token is random per manager process. Share tokens are per kernel and stored only
as SHA-256 hashes; this store is in memory until the dedicated manager's ``manager.db``
lands (issue #19, FR-M4).

SUPERSEDED PROTOTYPE: the manager is being rewritten in Rust. This Python module is kept as
a reference for the contract's behaviour; the language-neutral oracle is tests/ (fake DKP/1
kernel in tests/helpers, HTTP-level tests runnable via DARKPYONIX_MANAGER_CMD).
"""
from __future__ import annotations

import datetime
import hashlib
import hmac
import secrets
from typing import Any, Dict, List, Optional

PERMISSION_RANK = {"viewer1": 1, "viewer2": 2, "viewer3": 3, "admin": 4}


class Principal:
    """Who is calling: ``admin`` (master token) or a share bound to one kernel."""

    def __init__(self, permission: str, kernel_id: Optional[str] = None,
                 share_id: Optional[str] = None) -> None:
        self.permission = permission
        self.kernel_id = kernel_id
        self.share_id = share_id

    @property
    def is_admin(self) -> bool:
        return self.permission == "admin"

    def at_least(self, permission: str) -> bool:
        return PERMISSION_RANK[self.permission] >= PERMISSION_RANK[permission]

    def can_see(self, kernel_id: str) -> bool:
        return self.kernel_id is None or self.kernel_id == kernel_id


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _iso(t: datetime.datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


class Auth:
    def __init__(self, master_token: Optional[str] = None) -> None:
        self.master_token = master_token or secrets.token_urlsafe(32)
        self._shares = {}  # type: Dict[str, Dict[str, Any]]

    def authenticate(self, token: Optional[str]) -> Optional[Principal]:
        if not token:
            return None
        if hmac.compare_digest(token.encode("utf-8"), self.master_token.encode("utf-8")):
            return Principal("admin")
        digest = _hash(token)
        for share in self._shares.values():
            if hmac.compare_digest(digest, share["token_sha256"]):
                expires = share.get("_expires")
                if expires is not None and expires <= _now():
                    return None
                return Principal(share["permission"], share["kernel_id"], share["share_id"])
        return None

    # ------------------------------------------------------------ shares

    def list_shares(self, kernel_id: str) -> List[Dict[str, Any]]:
        return [self._public(s) for s in self._shares.values() if s["kernel_id"] == kernel_id]

    def create_share(self, kernel_id: str, permission: str, label: Optional[str] = None,
                     expires_at: Optional[datetime.datetime] = None) -> Dict[str, Any]:
        share_id = "s_" + secrets.token_hex(8)
        token = secrets.token_urlsafe(32)
        share = {
            "share_id": share_id, "kernel_id": kernel_id, "permission": permission,
            "label": label, "created_at": _iso(_now()),
            "expires_at": _iso(expires_at) if expires_at else None,
            "token_sha256": _hash(token), "_expires": expires_at,
        }
        self._shares[share_id] = share
        return dict(self._public(share), token=token)

    def revoke_share(self, kernel_id: str, share_id: str) -> bool:
        share = self._shares.get(share_id)
        if share is None or share["kernel_id"] != kernel_id:
            return False
        del self._shares[share_id]
        return True

    @staticmethod
    def _public(share: Dict[str, Any]) -> Dict[str, Any]:
        return {k: share[k] for k in ("share_id", "kernel_id", "permission", "label",
                                      "created_at", "expires_at")}
