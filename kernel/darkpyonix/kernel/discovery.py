"""Kernel discovery over loopback UDP multicast, with the registry as fallback (PROTOCOL §2).

FR-D1: a query reaches every kernel of the same user; each answers with an ``announce``
that echoes the query's nonce. Answers go unicast to the querying socket; unsolicited
announces and ``bye`` go to the group. FR-D2: every announce is mirrored to the registry,
and ``DARKPYONIX_DISCOVERY=registry`` turns multicast off entirely.

Standard library only; Python 3.8+.
"""
from __future__ import annotations

import json
import os
import secrets
import socket
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from darkpyonix.kernel import registry
from darkpyonix.kernel.protocol import (
    ANNOUNCE_INTERVAL, DISCOVERY_GROUP, DISCOVERY_INTERFACE, DISCOVERY_PORT, DKP_VERSION,
    QUERY_TIMEOUT,
)

MAX_DATAGRAM = 65507
_GROUP_ADDR = (DISCOVERY_GROUP, DISCOVERY_PORT)


def multicast_enabled() -> bool:
    return os.environ.get("DARKPYONIX_DISCOVERY", "").strip().lower() != "registry"


def _log(message: str) -> None:
    try:
        sys.stderr.write("darkpyonix discovery: %s\n" % message)
        sys.stderr.flush()
    except Exception:
        pass


def _encode(message: Dict[str, Any]) -> bytes:
    return json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _decode(data: bytes) -> Optional[Dict[str, Any]]:
    try:
        message = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(message, dict) or message.get("dkp") != DKP_VERSION:
        return None
    return message


def _set_send_options(sock: socket.socket) -> None:
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(DISCOVERY_INTERFACE))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 0)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)


def open_group_socket() -> socket.socket:
    """A socket that receives datagrams sent to the discovery group on loopback (§2.1)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        # Binding to the group address (POSIX) keeps plain unicast to the port out.
        # Windows cannot bind to a multicast address, so it binds the wildcard instead.
        try:
            if os.name == "nt":
                raise OSError("bind wildcard on Windows")
            sock.bind(_GROUP_ADDR)
        except OSError:
            sock.bind(("", DISCOVERY_PORT))
        membership = socket.inet_aton(DISCOVERY_GROUP) + socket.inet_aton(DISCOVERY_INTERFACE)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
        _set_send_options(sock)
    except OSError:
        sock.close()
        raise
    return sock


def open_send_socket() -> socket.socket:
    """An ephemeral loopback socket for sending to the group and receiving unicast replies."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        _set_send_options(sock)
        sock.bind((DISCOVERY_INTERFACE, 0))
    except OSError:
        sock.close()
        raise
    return sock


class DiscoveryResponder:
    """The kernel side of PROTOCOL §2: answer queries, announce, say bye."""

    def __init__(self, info: Callable[[], Dict[str, Any]], user_tag: str) -> None:
        self._info = info
        self.user_tag = user_tag
        self._recv = None  # type: Optional[socket.socket]
        self._send = None  # type: Optional[socket.socket]
        self._send_lock = threading.Lock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = None  # type: Optional[threading.Thread]
        self.multicast = False

    # -------------------------------------------------------------- lifecycle

    def start(self) -> None:
        if multicast_enabled():
            try:
                self._send = open_send_socket()
                self._recv = open_group_socket()
                self._recv.settimeout(0.5)
                self.multicast = True
            except OSError as e:
                _log("multicast unavailable (%s); using the registry only" % e)
                self._close_sockets()
        self.announce_now()
        self._thread = threading.Thread(target=self._run, name="darkpyonix-discovery", daemon=True)
        self._thread.start()

    def stop(self, bye: bool = True) -> None:
        self._stop.set()
        self._wake.set()
        if bye:
            info = self._safe_info()
            message = {"dkp": DKP_VERSION, "op": "bye", "user_tag": self.user_tag,
                       "kernel_id": info.get("kernel_id"), "pid": info.get("pid", os.getpid())}
            self._sendto(_encode(message), _GROUP_ADDR)
        thread, self._thread = self._thread, None
        if thread is not None and thread is not threading.current_thread():
            thread.join(2.0)
        self._close_sockets()

    def announce_now(self) -> None:
        """Mirror the current body to the registry and multicast it (start, status change)."""
        body = self._body()
        try:
            registry.write(body)
        except (OSError, KeyError) as e:
            _log("registry write failed: %s" % e)
        self._sendto(_encode(body), _GROUP_ADDR)
        self._wake.set()  # restart the periodic timer

    # -------------------------------------------------------------- internals

    def _safe_info(self) -> Dict[str, Any]:
        try:
            return dict(self._info())
        except Exception as e:  # never let a status callback kill discovery
            _log("info() failed: %s" % e)
            return {}

    def _body(self, nonce: Optional[str] = None) -> Dict[str, Any]:
        body = {"dkp": DKP_VERSION, "op": "announce", "user_tag": self.user_tag}  # type: Dict[str, Any]
        if nonce is not None:
            body["nonce"] = nonce
        body.update(self._safe_info())
        body["dkp"], body["op"], body["user_tag"] = DKP_VERSION, "announce", self.user_tag
        return body

    def _sendto(self, data: bytes, addr: Tuple[str, int]) -> None:
        sock = self._send
        if sock is None:
            return
        with self._send_lock:
            try:
                sock.sendto(data, addr)
            except OSError as e:
                _log("send to %s:%d failed: %s" % (addr[0], addr[1], e))

    def _close_sockets(self) -> None:
        for name in ("_recv", "_send"):
            sock = getattr(self, name)
            setattr(self, name, None)
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

    def _run(self) -> None:
        next_announce = time.monotonic() + ANNOUNCE_INTERVAL
        while not self._stop.is_set():
            if self._wake.is_set():
                self._wake.clear()
                next_announce = time.monotonic() + ANNOUNCE_INTERVAL
            now = time.monotonic()
            if now >= next_announce:
                self.announce_now()
                self._wake.clear()
                next_announce = time.monotonic() + ANNOUNCE_INTERVAL
                continue
            sock = self._recv
            if sock is None:
                self._wake.wait(min(0.5, next_announce - now))
                continue
            try:
                data, addr = sock.recvfrom(MAX_DATAGRAM)
            except socket.timeout:
                continue
            except OSError:
                if self._stop.is_set():
                    break
                time.sleep(0.1)
                continue
            self._handle(data, addr)

    def _handle(self, data: bytes, addr: Tuple[str, int]) -> None:
        message = _decode(data)
        if message is None or message.get("op") != "query":
            return
        if message.get("user_tag") != self.user_tag:
            return
        body = self._body(nonce=message.get("nonce"))
        wanted = message.get("kernel_id")
        if wanted is not None and wanted != body.get("kernel_id"):
            return
        # Reply straight to the querying socket; the nonce marks it as the answer.
        self._sendto(_encode(body), (addr[0], addr[1]))


class DiscoveryClient:
    """The manager side of PROTOCOL §2: query the group, optionally listen for announces."""

    def __init__(self, user_tag: str) -> None:
        self.user_tag = user_tag
        self._listen_sock = None  # type: Optional[socket.socket]
        self._listen_thread = None  # type: Optional[threading.Thread]
        self._closed = threading.Event()

    def query(self, kernel_id: Optional[str] = None, timeout: float = QUERY_TIMEOUT,
              expect: Optional[int] = None) -> List[Dict[str, Any]]:
        """Send one query and collect announces until ``timeout``.

        Returns early once ``expect`` distinct kernels (or the named ``kernel_id``) answered.
        """
        if not multicast_enabled():
            return []
        if kernel_id is not None and expect is None:
            expect = 1
        nonce = secrets.token_hex(8)
        message = {"dkp": DKP_VERSION, "op": "query", "user_tag": self.user_tag, "nonce": nonce}
        if kernel_id is not None:
            message["kernel_id"] = kernel_id
        found = {}  # type: Dict[str, Dict[str, Any]]
        try:
            sock = open_send_socket()
        except OSError as e:
            _log("multicast unavailable (%s)" % e)
            return []
        try:
            deadline = time.monotonic() + timeout
            sock.sendto(_encode(message), _GROUP_ADDR)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or (expect is not None and len(found) >= expect):
                    break
                sock.settimeout(remaining)
                try:
                    data, _ = sock.recvfrom(MAX_DATAGRAM)
                except socket.timeout:
                    break
                reply = _decode(data)
                if (reply is None or reply.get("op") != "announce" or reply.get("nonce") != nonce
                        or reply.get("user_tag") != self.user_tag):
                    continue
                kid = reply.get("kernel_id")
                if not isinstance(kid, str) or (kernel_id is not None and kid != kernel_id):
                    continue
                reply.pop("nonce", None)
                found[kid] = reply  # newest wins
        except OSError as e:
            _log("query failed: %s" % e)
        finally:
            sock.close()
        return list(found.values())

    def listen(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Call ``callback(message)`` for each announce or bye of this user, in a daemon thread."""
        if self._listen_thread is not None or not multicast_enabled():
            return
        try:
            sock = open_group_socket()
        except OSError as e:
            _log("multicast unavailable (%s); not listening" % e)
            return
        sock.settimeout(0.5)
        self._listen_sock = sock

        def run() -> None:
            while not self._closed.is_set():
                try:
                    data, _ = sock.recvfrom(MAX_DATAGRAM)
                except socket.timeout:
                    continue
                except OSError:
                    break
                message = _decode(data)
                if (message is None or message.get("op") not in ("announce", "bye")
                        or message.get("user_tag") != self.user_tag):
                    continue
                try:
                    callback(message)
                except Exception as e:
                    _log("listener callback failed: %s" % e)

        self._listen_thread = threading.Thread(target=run, name="darkpyonix-discovery-listen", daemon=True)
        self._listen_thread.start()

    def close(self) -> None:
        self._closed.set()
        thread, self._listen_thread = self._listen_thread, None
        if thread is not None:
            thread.join(2.0)
        sock, self._listen_sock = self._listen_sock, None
        if sock is not None:
            sock.close()


def discover(kernel_id: Optional[str] = None, timeout: float = QUERY_TIMEOUT) -> List[Dict[str, Any]]:
    """Live kernels of this user: multicast answers merged with the registry (FR-D1, FR-D2)."""
    from darkpyonix import _home

    merged = {}  # type: Dict[str, Dict[str, Any]]
    for entry in registry.scan(prune=True):
        if kernel_id is None or entry.get("kernel_id") == kernel_id:
            merged[entry["kernel_id"]] = entry
    if multicast_enabled():
        client = DiscoveryClient(_home.user_tag())
        try:
            answers = client.query(kernel_id=kernel_id, timeout=timeout)
        finally:
            client.close()
        for entry in answers:
            if registry.pid_alive(entry.get("pid", 0)):
                merged[entry["kernel_id"]] = entry  # a live answer beats the file
    return sorted(merged.values(), key=lambda e: str(e.get("kernel_id")))
