"""The kernel's event log: a numbered ring of recent events (PROTOCOL §3.4, PR-3).

Standard library only; Python 3.8+.
"""
from __future__ import annotations

import collections
import json
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

from ._protocol import DKP_VERSION, EVENT_RING_MAX_BYTES, EVENT_RING_MAX_EVENTS, now_iso

Listener = Callable[[Dict[str, Any]], None]


class EventLog:
    """Thread-safe event ring bounded by count and approximate encoded size.

    ``append`` numbers events from 1 and calls every listener while holding the log's lock,
    so listeners see events in ``seq`` order. Listeners must therefore be quick and must not
    call back into the log.
    """

    def __init__(self, max_events: int = EVENT_RING_MAX_EVENTS,
                 max_bytes: int = EVENT_RING_MAX_BYTES) -> None:
        self._max_events = max(1, int(max_events))
        self._max_bytes = int(max_bytes)
        self._lock = threading.Lock()
        self._ring = collections.deque()  # type: collections.deque
        self._bytes = 0
        self._seq = 0
        self._listeners = []  # type: List[Listener]

    @property
    def seq(self) -> int:
        return self._seq

    def append(self, type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            self._seq += 1
            event = {"dkp": DKP_VERSION, "op": "event", "seq": self._seq, "type": type,
                     "time": now_iso(), "data": data}
            size = len(json.dumps(event, ensure_ascii=False, separators=(",", ":"))) + 4
            self._ring.append((event, size))
            self._bytes += size
            while len(self._ring) > 1 and (len(self._ring) > self._max_events
                                           or self._bytes > self._max_bytes):
                _, old = self._ring.popleft()
                self._bytes -= old
            for cb in list(self._listeners):
                try:
                    cb(event)
                except Exception:  # a broken listener must not break the kernel
                    pass
            return event

    def since(self, seq: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        """Events with ``seq`` greater than ``seq``, and ``oldest_seq`` if some were dropped."""
        seq = int(seq)
        with self._lock:
            if not self._ring or seq >= self._seq:
                return [], None
            oldest = self._ring[0][0]["seq"]
            events = [e for e, _ in self._ring if e["seq"] > seq]
            return events, (oldest if seq + 1 < oldest else None)

    def add_listener(self, cb: Listener) -> None:
        with self._lock:
            self._listeners.append(cb)

    def remove_listener(self, cb: Listener) -> None:
        with self._lock:
            try:
                self._listeners.remove(cb)
            except ValueError:
                pass
