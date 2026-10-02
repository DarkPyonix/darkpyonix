"""DKP/1 primitives shared by the kernel, its clients and the manager (docs/PROTOCOL.md).

Standard library only; Python 3.8+. Everything that both ends of the wire must agree on
lives here so that the pieces can be built in parallel against one definition.
"""
from __future__ import annotations

import datetime
import hashlib
import hmac
import json
import os
import secrets
import socket
import struct
from typing import Any, Dict, Optional

DKP_VERSION = 1
KERNEL_VERSION = "0.1.0"

# §2 discovery
DISCOVERY_GROUP = "239.255.68.80"
DISCOVERY_PORT = 46880
DISCOVERY_INTERFACE = "127.0.0.1"
ANNOUNCE_INTERVAL = 5.0        # seconds between unsolicited announces
STALE_AFTER = 15.0             # managers drop kernels silent for this long
QUERY_TIMEOUT = 0.2            # FR-D1: all kernels answer within 200 ms

# §3 control channel
MAX_FRAME = 64 * 1024 * 1024
HANDSHAKE_TIMEOUT = 5.0
NONCE_BYTES = 32

# §3.4 events
EVENT_RING_MAX_EVENTS = 10000
EVENT_RING_MAX_BYTES = 16 * 1024 * 1024
STREAM_COALESCE_SECONDS = 0.05

# FR-K3 exit code when another kernel already owns the file
EXIT_ALREADY_RUNNING = 3

ERROR_CODES = (
    "auth_failed", "bad_request", "unknown_method", "busy", "not_found",
    "frame_too_large", "shutting_down", "internal",
)

KERNEL_STATUSES = ("starting", "idle", "busy", "stopping")


class DKPError(Exception):
    """An error that travels as ``{"ok": false, "error": {...}}`` (PROTOCOL §3.3)."""

    def __init__(self, code: str, message: str = "", data: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message or code)
        self.code = code
        self.message = message or code
        self.data = data

    def to_dict(self) -> Dict[str, Any]:
        out = {"code": self.code, "message": self.message}  # type: Dict[str, Any]
        if self.data is not None:
            out["data"] = self.data
        return out


class FrameTooLarge(DKPError):
    def __init__(self, length: int) -> None:
        super().__init__("frame_too_large", "frame of %d bytes exceeds %d" % (length, MAX_FRAME))


# ---------------------------------------------------------------- identity (§2.6)

def canonical_path(path: str) -> str:
    """``realpath(abspath(path))``, plus ``normcase`` on Windows."""
    p = os.path.realpath(os.path.abspath(path))
    if os.name == "nt":
        p = os.path.normcase(p)
    return p


def kernel_id_for(path: str) -> str:
    """``"k_" + hex(SHA-256(canonical path as UTF-8))[:20]`` (FR-K2)."""
    digest = hashlib.sha256(canonical_path(path).encode("utf-8")).hexdigest()
    return "k_" + digest[:20]


# ---------------------------------------------------------------- auth (§3.2)

def new_nonce() -> bytes:
    return secrets.token_bytes(NONCE_BYTES)


def auth_mac(key: bytes, nonce: bytes, kernel_id: str) -> str:
    """``hex(HMAC-SHA256(user.key, nonce || kernel_id))``."""
    return hmac.new(key, nonce + kernel_id.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_mac(key: bytes, nonce: bytes, kernel_id: str, mac: str) -> bool:
    return hmac.compare_digest(auth_mac(key, nonce, kernel_id), str(mac))


# ---------------------------------------------------------------- frames (§3.1)

def encode(message: Dict[str, Any]) -> bytes:
    """Serialize one frame. ``dkp`` is added if missing."""
    if "dkp" not in message:
        message = dict(message, dkp=DKP_VERSION)
    payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_FRAME:
        raise FrameTooLarge(len(payload))
    return struct.pack(">I", len(payload)) + payload


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks = []
    remaining = n
    while remaining:
        chunk = sock.recv(min(remaining, 1 << 20))
        if not chunk:
            raise ConnectionError("connection closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock: socket.socket) -> Dict[str, Any]:
    """Blocking read of one frame from ``sock``."""
    (length,) = struct.unpack(">I", _recv_exact(sock, 4))
    if length > MAX_FRAME:
        raise FrameTooLarge(length)
    message = json.loads(_recv_exact(sock, length).decode("utf-8"))
    if not isinstance(message, dict):
        raise DKPError("bad_request", "frame payload is not a JSON object")
    return message


class FrameDecoder:
    """Incremental decoder for non-blocking sockets: ``feed(bytes)`` → complete frames."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes):
        self._buf.extend(data)
        frames = []
        while len(self._buf) >= 4:
            (length,) = struct.unpack(">I", bytes(self._buf[:4]))
            if length > MAX_FRAME:
                raise FrameTooLarge(length)
            if len(self._buf) < 4 + length:
                break
            payload = bytes(self._buf[4:4 + length])
            del self._buf[:4 + length]
            message = json.loads(payload.decode("utf-8"))
            if not isinstance(message, dict):
                raise DKPError("bad_request", "frame payload is not a JSON object")
            frames.append(message)
        return frames


# ---------------------------------------------------------------- time and ids

def now_iso() -> str:
    """UTC timestamp with milliseconds and a ``Z`` suffix."""
    t = datetime.datetime.now(datetime.timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (t.microsecond // 1000)


def new_run_id() -> str:
    """``YYYYMMDD-HHMMSS-<4 hex>`` in UTC (FR-R1)."""
    t = datetime.datetime.now(datetime.timezone.utc)
    return t.strftime("%Y%m%d-%H%M%S-") + secrets.token_hex(2)
