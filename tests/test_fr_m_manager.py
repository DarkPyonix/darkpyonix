"""Manager HTTP API against a fake DKP/1 kernel (SPEC FR-M1, FR-M2, FR-M5, FR-A2, NFR-M3).

Language-neutral oracle: every test here talks to the manager over HTTP only, except those
marked ``prototype_only`` (they inspect the Python prototype's schema or its fake launcher).
Set ``DARKPYONIX_MANAGER_CMD`` to a command that starts an ephemeral manager (for example
the Rust build) to run the rest against that implementation; it must publish
``managers/<pid>.json`` and honour ``DARKPYONIX_HOME`` and ``DARKPYONIX_DISCOVERY=registry``.
"""
from __future__ import annotations

import os
import re
import time

import pytest
import yaml

from conftest import REPO
from darkpyonix import _home
from darkpyonix.kernel import protocol
from darkpyonix.manager.app import ManagerState, create_app
from darkpyonix.manager.auth import Auth
from darkpyonix.manager.kernels import KernelDirectory

from helpers.fake_backend import EXTERNAL_CMD, FakeBackend, make_manager, read_sse
from helpers.fake_kernel import FakeKernel

SPEC = os.path.join(REPO, "docs", "api", "manager.openapi.yaml")
METHODS = ("get", "put", "post", "delete", "patch", "head", "options")
prototype_only = pytest.mark.skipif(bool(EXTERNAL_CMD), reason="inspects the Python prototype")


@pytest.fixture
def notebook(scratch):
    path = os.path.join(scratch, "train.py")
    with open(path, "w") as f:
        f.write("print('hello')\n")
    return path


@pytest.fixture
def backend(dp_home):
    b = FakeBackend(_home.user_key())
    yield b
    b.close()


@pytest.fixture
def kernel(backend, notebook):
    return backend.add(FakeKernel(notebook, backend.key).start())


@pytest.fixture
def manager(backend):
    with make_manager(backend) as m:
        yield m


def _wait(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _error(resp, status, code):
    assert resp.status_code == status, resp.text
    body = resp.json()
    assert set(body) == {"error"} and body["error"]["code"] == code and isinstance(body["error"]["message"], str)
    return body["error"]


# ---------------------------------------------------------------- FR-M1 / NFR-M3

def _operations(schema):
    ops = {}
    for path, item in schema["paths"].items():
        for method in METHODS:
            if method in item:
                op = item[method]
                ops[(path, method)] = (op.get("operationId"), sorted(str(c) for c in op.get("responses", {})))
    return ops


@prototype_only
def test_fr_m1_prototype_operations_match_spec(dp_home):
    # The Rust manager (INTENT D10) must serve the whole spec; that equality is asserted in
    # darkpyonix/manager/crates/dpx-server/tests/api.rs. The Python prototype stopped at the
    # pre-collaboration API, so here every operation it does serve must keep the spec's
    # operationId and may only answer with status codes the spec lists.
    with open(SPEC) as f:
        spec = yaml.safe_load(f)
    app = create_app(KernelDirectory(FakeBackend(b"k" * 32)), Auth(), ManagerState())
    served = _operations(app.openapi())
    expected = _operations(spec)
    assert served and set(served) <= set(expected), sorted(set(served) - set(expected))
    for key in served:
        assert served[key][0] == expected[key][0], key
        assert set(served[key][1]) <= set(expected[key][1]), key
    # Routes outside the schema are only the API reference page.
    hidden = {r.path for r in app.routes if not getattr(r, "include_in_schema", True)}
    assert hidden == {"/docs", "/docs/", "/docs/{name}"}


def test_fr_m1_docs_page_is_served_outside_the_schema(manager):
    with manager.client(token=None) as c:
        page = c.get("/docs/")
        assert page.status_code == 200 and "swagger" in page.text.lower()
        assert c.get("/docs", follow_redirects=False).status_code in (307, 302)
        spec = c.get("/docs/manager.openapi.yaml")
        assert spec.status_code == 200
        assert spec.content == open(SPEC, "rb").read()
        assert c.get("/docs/hub.openapi.yaml").status_code == 200
        assert c.get("/docs/CLAUDE.md").status_code == 404
    with manager.client() as c:
        for path in ("/openapi.json", "/redoc"):
            _error(c.get(path), 404, "not_found")


def test_fr_m1_health_and_manager_info(manager):
    with manager.client(token=None) as c:
        health = c.get("/health").json()
        assert health["status"] == "ok" and re.match(r"^\d+\.\d+\.\d+", health["version"])
        assert set(health) == {"status", "version"}
    with manager.client() as c:
        info = c.get("/api/manager").json()
    assert info["mode"] == "ephemeral" and info["permission"] == "admin" and isinstance(info["pid"], int)
    assert {"version", "started_at", "host"} <= set(info)


# ---------------------------------------------------------------- NFR-V1 (INTENT D16)

def test_nfr_v1_no_version_segment_in_any_rest_path():
    version = re.compile(r"^v[0-9]+$")
    for name in ("manager.openapi.yaml", "hub.openapi.yaml"):
        with open(os.path.join(REPO, "docs", "api", name)) as f:
            spec = yaml.safe_load(f)
        for path in spec["paths"]:
            assert not any(version.match(seg) for seg in path.split("/")), (name, path)


def test_nfr_v1_versioned_manager_path_is_not_served(manager, kernel):
    with manager.client() as c:
        assert c.get("/api/manager").status_code == 200
        for path in ("/api/v1/manager", "/api/v1/kernels", "/api/v1/kernels/%s/events" % kernel.kernel_id):
            _error(c.get(path), 404, "not_found")


def test_fr_m1_validation_errors_are_400_bad_request(manager, kernel):
    with manager.client() as c:
        err = _error(c.post("/api/kernels", json={"python": "x"}), 400, "bad_request")
        assert err["data"]["errors"]
        _error(c.post("/api/kernels", content=b"{not json", headers={"content-type": "application/json"}),
               400, "bad_request")
        _error(c.post("/api/kernels/%s/runs" % kernel.kernel_id, json={"mode": "some"}), 400, "bad_request")
        _error(c.post("/api/kernels/%s/runs" % kernel.kernel_id, json={"mode": "cells"}), 400, "bad_request")
        _error(c.get("/api/documents"), 400, "bad_request")


# ---------------------------------------------------------------- FR-A2

def test_fr_a2_requests_without_token_are_401(manager, kernel):
    kid = kernel.kernel_id
    calls = [("GET", "/api/manager"), ("GET", "/api/kernels"), ("POST", "/api/kernels"),
             ("GET", "/api/kernels/%s" % kid), ("POST", "/api/kernels/%s/interrupt" % kid),
             ("POST", "/api/kernels/%s/runs" % kid), ("GET", "/api/kernels/%s/events" % kid),
             ("GET", "/api/documents?path=x.py"), ("GET", "/api/kernels/%s/shares" % kid)]
    for token in (None, "wrong"):
        with manager.client(token=token) as c:
            for method, path in calls:
                resp = c.request(method, path, content=b"{bad")
                _error(resp, 401, "unauthorized")
                assert resp.headers["www-authenticate"] == "Bearer"
    with manager.client(token=None) as c:
        assert c.get("/health").status_code == 200
        # ?token= is accepted on the events stream only.
        _error(c.get("/api/kernels", params={"token": manager.token}), 401, "unauthorized")
        with c.stream("GET", "/api/kernels/%s/events" % kid, params={"token": manager.token}) as r:
            assert r.status_code == 200 and r.headers["content-type"].startswith("text/event-stream")
    assert "interrupt" not in kernel.method_calls() and "run" not in kernel.method_calls()


# ---------------------------------------------------------------- kernels

def test_fr_m1_list_and_get_kernels(manager, kernel):
    with manager.client() as c:
        kernels = c.get("/api/kernels", params={"refresh": "true"}).json()["kernels"]
        assert [k["kernel_id"] for k in kernels] == [kernel.kernel_id]
        k = kernels[0]
        assert {"kernel_id", "path", "pid", "status", "python", "started_at", "runs_dir"} <= set(k)
        assert "port" not in k and k["runs_dir"].endswith(os.path.join("__runs__", "train.py"))
        info = c.get("/api/kernels/%s" % kernel.kernel_id).json()
        assert info["status"] == "idle" and info["queue"] == [] and info["kernel_version"] == protocol.KERNEL_VERSION
        _error(c.get("/api/kernels/k_00000000000000000000"), 404, "not_found")
        _error(c.get("/api/kernels/not-a-kernel"), 404, "not_found")


@prototype_only
def test_fr_m2_start_kernel_is_idempotent(manager, backend, notebook):
    with manager.client() as c:
        first = c.post("/api/kernels", json={"path": notebook})
        assert first.status_code == 201, first.text
        assert first.json()["kernel_id"] == protocol.kernel_id_for(notebook)
        second = c.post("/api/kernels", json={"path": os.path.join(os.path.dirname(notebook), ".", "train.py")})
        assert second.status_code == 200
        assert second.json()["kernel_id"] == first.json()["kernel_id"]
    assert len(backend.launches) == 1
    assert backend.launches[0]["cwd"] == os.path.dirname(protocol.canonical_path(notebook))


@prototype_only
def test_fr_m2_start_kernel_that_never_announces_is_504(manager, backend, notebook):
    backend.launchable = False
    backend.wait_timeout = 0.2
    with manager.client() as c:
        err = _error(c.post("/api/kernels", json={"path": notebook}), 504, "start_timeout")
        _error(c.post("/api/kernels", json={"path": notebook + ".missing.py"}), 400, "bad_request")
        open(notebook + ".txt", "w").close()
        _error(c.post("/api/kernels", json={"path": notebook + ".txt"}), 400, "bad_request")
    assert err["data"]["kernel_id"] == protocol.kernel_id_for(notebook)


# ---------------------------------------------------------------- runs (FR-X3, FR-X4)

def test_fr_x3_busy_run_is_409_with_busy_error(manager, kernel):
    url = "/api/kernels/%s/runs" % kernel.kernel_id
    with manager.client() as c:
        first = c.post(url, json={"mode": "all"})
        assert first.status_code == 202 and first.json()["state"] == "running"
        busy = c.post(url, json={"mode": "cells", "cells": [1]})
        err = _error(busy, 409, "busy")
        assert err["data"]["current"]["run_id"] == first.json()["run_id"]
        assert err["data"]["current"]["status"] == "running"
        queued = c.post(url, json={"mode": "all", "on_busy": "queue", "params": {"lr": 0.1}})
        assert queued.status_code == 202
        assert queued.json()["state"] == "queued" and queued.json()["position"] == 1
    method, params = kernel.calls[-1]
    assert method == "run" and params["mode"] == "all" and params["on_busy"] == "queue"
    assert params["params"] == {"lr": 0.1}


def test_fr_x4_interrupt_maps_to_kernel_interrupt(manager, kernel):
    kid = kernel.kernel_id
    with manager.client() as c:
        assert c.post("/api/kernels/%s/interrupt" % kid).json() == {"interrupted": False}
        run_id = c.post("/api/kernels/%s/runs" % kid, json={"mode": "all"}).json()["run_id"]
        assert c.post("/api/kernels/%s/interrupt" % kid).json() == {"interrupted": True, "run_id": run_id}
        summary = c.get("/api/kernels/%s/runs/%s" % (kid, run_id), params={"format": "summary"}).json()
        assert summary["status"] == "interrupted"
    assert kernel.method_calls().count("interrupt") == 2
    assert "shutdown" not in kernel.method_calls() and "restart" not in kernel.method_calls()


def test_fr_m1_runs_namespace_document_restart_shutdown(manager, kernel, notebook):
    kid = kernel.kernel_id
    base = "/api/kernels/%s" % kid
    with manager.client() as c:
        run_id = c.post(base + "/runs", json={"mode": "all"}).json()["run_id"]
        queued = c.post(base + "/runs", json={"mode": "all", "on_busy": "queue"}).json()["run_id"]
        assert c.delete(base + "/runs/%s" % queued).json() == {"cancelled": True}
        assert c.delete(base + "/runs/%s" % queued).json() == {"cancelled": False}
        assert c.delete(base + "/runs/current").json() == {"cancelled": False}
        _error(c.delete(base + "/runs/bogus"), 404, "not_found")
        kernel.finish_run()
        runs = c.get(base + "/runs", params={"limit": "1"}).json()["runs"]
        assert [r["run_id"] for r in runs] == [queued]
        assert c.get(base + "/runs", params={"limit": "abc"}).status_code == 200
        nb = c.get(base + "/runs/latest").json()
        assert nb["nbformat"] == 4 and nb["metadata"]["darkpyonix"]["run_id"] == run_id
        summary = c.get(base + "/runs/%s" % run_id, params={"format": "summary"}).json()
        assert summary["status"] == "ok" and summary["path"].endswith("%s.ipynb" % run_id)
        _error(c.get(base + "/runs/current"), 404, "not_found")
        assert c.get(base + "/namespace").json()["variables"][0]["name"] == "x"
        doc = c.get(base + "/document")
        assert doc.status_code == 200 and doc.json()["cells"]
        assert c.get("/api/documents", params={"path": notebook}).json()["cells"][0]["type"] == "preamble"
        _error(c.get("/api/documents", params={"path": notebook + ".nope.py"}), 404, "not_found")
        restarted = c.post(base + "/restart", json={"hard": False})
        assert restarted.status_code == 200 and restarted.json()["kernel_id"] == kid
        resp = c.delete(base)
        assert resp.status_code == 202 and resp.json() == {"shutting_down": True}
    assert ("restart", {"hard": False}) in kernel.calls and "shutdown" in kernel.method_calls()


# ---------------------------------------------------------------- events (FR-M1, PR-3)

def test_fr_m1_events_stream_resumes_with_last_event_id(manager, kernel):
    kid = kernel.kernel_id
    url = "/api/kernels/%s/events" % kid
    with manager.client() as c:
        with c.stream("GET", url) as stream:
            assert stream.status_code == 200
            assert c.post("/api/kernels/%s/runs" % kid, json={"mode": "all"}).status_code == 202
            live = read_sse(stream, 4)
        assert [e["event"] for e in live] == ["kernel.status", "run.started", "cell.started", "output"]
        ids = [int(e["id"]) for e in live]
        assert ids == sorted(ids) and len(set(ids)) == 4
        assert live[3]["data"]["output"] == {"output_type": "stream", "name": "stdout", "text": "hello\n"}

        # Missed while disconnected, then resumed from the second event.
        kernel.finish_run()
        with c.stream("GET", url, headers={"Last-Event-ID": str(ids[1])}, params={"since": "0"}) as stream:
            resumed = read_sse(stream, 5)
        assert [int(e["id"]) for e in resumed] == list(range(ids[1] + 1, ids[1] + 6))
        assert [e["event"] for e in resumed] == ["cell.started", "output", "cell.finished", "run.finished",
                                                 "kernel.status"]
        with c.stream("GET", url, params={"since": str(ids[3])}) as stream:
            assert [e["event"] for e in read_sse(stream, 1)] == ["cell.finished"]
    assert kernel.connections == 1  # every request and stream of one manager shares one connection


def test_pr3_resume_older_than_the_ring_reports_replay_truncated(backend, notebook):
    kernel = backend.add(FakeKernel(notebook, backend.key, ring_max=3).start())
    for i in range(6):
        kernel.emit("output", {"run_id": "x", "index": 1, "output": {"output_type": "stream", "name": "stdout",
                                                                     "text": "%d\n" % i}})
    with make_manager(backend) as m, m.client() as c:
        with c.stream("GET", "/api/kernels/%s/events" % kernel.kernel_id, params={"since": "1"}) as stream:
            got = read_sse(stream, 4)
    assert got[0] == {"event": "replay_truncated", "data": {"oldest_seq": 4}}
    assert [int(e["id"]) for e in got[1:]] == [4, 5, 6]


# ---------------------------------------------------------------- FR-M5

def test_fr_m5_two_managers_share_one_kernel(backend, kernel):
    url = "/api/kernels/%s/events" % kernel.kernel_id
    with make_manager(backend) as a, make_manager(backend) as b, a.client() as ca, b.client() as cb:
        with ca.stream("GET", url) as sa, cb.stream("GET", url) as sb:
            assert _wait(lambda: kernel.connections == 2)
            assert cb.post("/api/kernels/%s/runs" % kernel.kernel_id, json={"mode": "all"}).status_code == 202
            got_a, got_b = read_sse(sa, 4), read_sse(sb, 4)
        assert got_a == got_b
        assert [e["event"] for e in got_a] == ["kernel.status", "run.started", "cell.started", "output"]
        # The run started through b is busy for a too.
        _error(ca.post("/api/kernels/%s/runs" % kernel.kernel_id, json={"mode": "all"}), 409, "busy")


# ---------------------------------------------------------------- shares (FR-A3, minimal until #19)

def test_fr_a3_ephemeral_manager_refuses_share_creation(manager, kernel):
    kid = kernel.kernel_id
    with manager.client() as c:
        _error(c.post("/api/kernels/%s/shares" % kid, json={"permission": "viewer1"}), 403, "forbidden")
        assert c.get("/api/kernels/%s/shares" % kid).json() == {"shares": []}


def test_fr_a3_share_tokens_are_scoped_to_one_kernel_and_permission(backend, kernel):
    kid = kernel.kernel_id
    other = backend.add(FakeKernel(kernel.path + ".other.py", backend.key).start())
    with make_manager(backend, mode="dedicated") as m, m.client() as c:
        created = c.post("/api/kernels/%s/shares" % kid, json={"permission": "viewer1", "label": "demo"})
        assert created.status_code == 201
        share = created.json()
        assert share["share_id"].startswith("s_") and share["token"] and share["url"].endswith(share["token"])
        listed = c.get("/api/kernels/%s/shares" % kid).json()["shares"]
        assert [s["share_id"] for s in listed] == [share["share_id"]] and "token" not in listed[0]
        with m.client(token=share["token"]) as v:
            assert v.get("/api/manager").json()["permission"] == "viewer1"
            assert [k["kernel_id"] for k in v.get("/api/kernels").json()["kernels"]] == [kid]
            _error(v.get("/api/kernels/%s" % other.kernel_id), 404, "not_found")
            assert "outputs" not in v.get("/api/kernels/%s/document" % kid).json()["cells"][0]
            _error(v.post("/api/kernels/%s/runs" % kid, json={"mode": "all"}), 403, "forbidden")
            _error(v.post("/api/kernels/%s/interrupt" % kid), 403, "forbidden")
            _error(v.get("/api/kernels/%s/shares" % kid), 403, "forbidden")
            _error(v.post("/api/kernels", json={"path": kernel.path}), 403, "forbidden")
        assert c.delete("/api/kernels/%s/shares/%s" % (kid, share["share_id"])).status_code == 204
        _error(c.delete("/api/kernels/%s/shares/%s" % (kid, share["share_id"])), 404, "not_found")
        with m.client(token=share["token"]) as v:
            _error(v.get("/api/manager"), 401, "unauthorized")


# ---------------------------------------------------------------- NFR-M3, black-box

def _probe_url(path):
    return (path.replace("{kernel_id}", "k_00000000000000000000")
            .replace("{run_ref}", "20260101-000000-0000").replace("{share_id}", "s_0000000000000000"))


PROBE_BODIES = {"startKernel": {"path": "/nonexistent/darkpyonix/x.py"}, "startRun": {"mode": "all"},
                "restartKernel": {}, "createShare": {"permission": "viewer1"}}


def test_nfr_m3_every_operation_answers_with_a_documented_status(manager):
    """Black-box form of the schema comparison: every operation of the YAML exists and, for a
    kernel that does not exist, answers with one of the status codes the YAML lists for it."""
    with open(SPEC) as f:
        spec = yaml.safe_load(f)
    with manager.client() as admin, manager.client(token=None) as anon:
        for path, item in spec["paths"].items():
            for method in METHODS:
                if method not in item:
                    continue
                op = item[method]
                documented = {int(c) for c in op["responses"]}
                url = _probe_url(path)
                params = {"path": "/nonexistent/darkpyonix/x.py"} if op["operationId"] == "getDocument" else None
                body = PROBE_BODIES.get(op["operationId"])
                resp = admin.request(method.upper(), url, params=params, json=body)
                assert resp.status_code in documented, (op["operationId"], resp.status_code, resp.text)
                if resp.status_code >= 400:
                    assert set(resp.json()) == {"error"}
                if op.get("security") != []:
                    assert anon.request(method.upper(), url, params=params, json=body).status_code == 401
        _error(admin.get("/api/not-in-the-contract"), 404, "not_found")
