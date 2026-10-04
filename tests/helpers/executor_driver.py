"""Drive an Executor on the main thread of this process and dump what happened as JSON.

Usage: python executor_driver.py PLAN.json OUT.json

PLAN = {"path": notebook path, "src_root": .../kernel, "steps": [...]} where each step is
one of:
  {"op": "submit", "params": {...}}            -> {"result": ...} or {"error": {...}}
  {"op": "wait_event", "type": T, "count": n}  waits until n events of type T were emitted
  {"op": "wait_idle"}                          waits until status() is idle
  {"op": "sleep", "seconds": s}
  {"op": "interrupt"} | {"op": "namespace", "limit": n} | {"op": "restart", "hard": b}
  {"op": "status"} | {"op": "cancel", "run_id": "$<step index>"}
The control steps run on a helper thread, exactly like the kernel's control channel.
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time

PLAN = json.load(open(sys.argv[1]))
sys.path.insert(0, PLAN["src_root"])

from darkpyonix._executor import Executor  # noqa: E402
from darkpyonix._protocol import DKPError  # noqa: E402

T0 = time.monotonic()
EVENTS = []
LOCK = threading.Condition()


def emit(type_, data):
    with LOCK:
        # Event data is never mutated after emit; serialise at the end, off the measured path.
        EVENTS.append({"type": type_, "data": data, "t": time.monotonic() - T0})
        LOCK.notify_all()


def run_to_dict(run):
    return {
        "run_id": run.run_id, "status": run.status, "mode": run.mode, "params": run.params,
        "file_sha256": run.file_sha256,
        "cells": [{"index": c.index, "type": c.type, "status": c.status,
                   "execution_count": c.execution_count, "outputs": c.outputs} for c in run.cells],
    }


class FakeRuns(object):
    def __repr__(self):
        return "<runs>"


class FakeStore(object):
    def __init__(self):
        self.begun, self.finished, self.updates = [], [], 0

    def begin(self, run):
        self.begun.append(run.run_id)

    def update(self, run):
        self.updates += 1

    def finish(self, run):
        self.finished.append(run_to_dict(run))

    def magic(self):
        return FakeRuns()

    def list(self, limit=20):
        return []

    def get(self, ref):
        return None


STORE = FakeStore()
EX = Executor(PLAN["path"], "k_test", emit, STORE, {"version": sys.version.split()[0]}, "testhost",
              capture_fds=PLAN.get("capture_fds", True))
RESULTS = []


def wait_for(pred, timeout):
    deadline = time.monotonic() + timeout
    with LOCK:
        while not pred():
            left = deadline - time.monotonic()
            if left <= 0:
                return False
            LOCK.wait(min(left, 0.05))
    return True


def resolve(value):
    if isinstance(value, str) and value.startswith("$"):
        return RESULTS[int(value[1:])]["result"]["run_id"]
    return value


def control():
    try:
        for step in PLAN["steps"]:
            op = step["op"]
            entry = {"op": op, "t": time.monotonic() - T0}
            try:
                if op == "submit":
                    entry["result"] = EX.submit(step["params"])
                elif op == "wait_event":
                    n = step.get("count", 1)
                    entry["result"] = wait_for(
                        lambda: sum(1 for e in EVENTS if e["type"] == step["type"]) >= n,
                        step.get("timeout", 20))
                elif op == "wait_idle":
                    entry["result"] = wait_for(lambda: EX.status()["status"] == "idle",
                                               step.get("timeout", 20))
                elif op == "sleep":
                    time.sleep(step["seconds"])
                elif op == "interrupt":
                    entry["result"] = EX.interrupt()
                elif op == "namespace":
                    entry["result"] = EX.namespace(step.get("limit", 200))
                elif op == "restart":
                    entry["result"] = EX.restart(step.get("hard", False))
                elif op == "status":
                    entry["result"] = EX.status()
                elif op == "cancel":
                    entry["result"] = EX.cancel(resolve(step["run_id"]))
                else:
                    raise ValueError(op)
            except DKPError as exc:
                entry["error"] = exc.to_dict()
            entry["t_end"] = time.monotonic() - T0
            RESULTS.append(entry)
    finally:
        EX.shutdown()


threading.Thread(target=control, daemon=True).start()
EX.run_forever()
with open(sys.argv[2], "w") as f:
    json.dump({"results": RESULTS, "events": EVENTS, "runs": STORE.finished,
               "begun": STORE.begun, "updates": STORE.updates,
               "hard_restart_requested": EX.hard_restart_requested}, f)
