"""The manager's HTTP API: exactly the paths of docs/api/manager.openapi.yaml (SPEC FR-M1, NFR-M3).

Besides the contract's paths the app serves only the API reference page at ``/docs/`` (and the
two OpenAPI files it renders), outside the schema. FastAPI's own ``/docs``, ``/redoc`` and
``/openapi.json`` are disabled.

SUPERSEDED PROTOTYPE: the manager is being rewritten in Rust. This Python module is kept as
a reference for the contract's behaviour; the language-neutral oracle is tests/ (fake DKP/1
kernel in tests/helpers, HTTP-level tests runnable via DARKPYONIX_MANAGER_CMD).
"""
from __future__ import annotations

import asyncio
import datetime
import json
import os
import re
import signal
import socket
import time
from typing import Any, Dict, List, Literal, Optional
from urllib.parse import parse_qs

from fastapi import Depends, FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

import darkpyonix
from darkpyonix.kernel.protocol import DKPError

from .auth import Auth, Principal
from .kernels import START_TIMEOUT, NOTEBOOK_SUFFIXES, KernelDirectory, kernel_view, runs_dir_for

VERSION = darkpyonix.__version__
DOCS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "docs", "api"))
DOCS_FILES = {"manager.openapi.yaml", "hub.openapi.yaml"}
SSE_KEEPALIVE = 15.0
FORCE_KILL_GRACE = 5.0         # shutdownKernel force=true
SHARE_LINK = "https://darkpyonix.dev/s/%s#%s"

KERNEL_ID_RE = re.compile(r"^k_[0-9a-f]{20}$")
RUN_ID_RE = re.compile(r"^[0-9]{8}-[0-9]{6}-[0-9a-f]{4}$")
SHARE_ID_RE = re.compile(r"^s_[0-9a-f]{16}$")
EVENTS_PATH_RE = re.compile(r"^/api/v1/kernels/[^/]+/events$")
OUTPUT_EVENTS = ("output", "output.clear")


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class ManagerState:
    """Process facts for ``getManager`` plus the activity the idle watchdog reads (FR-M3)."""

    def __init__(self, mode: str = "ephemeral", idle_timeout: Optional[float] = 120.0) -> None:
        self.mode = mode
        self.idle_timeout = idle_timeout if mode == "ephemeral" else None
        self.pid = os.getpid()
        self.started_at = _now_iso()
        self.host = socket.gethostname()
        self.active = 0
        self.last_activity = time.monotonic()

    def touch(self) -> None:
        self.last_activity = time.monotonic()

    def idle_for(self) -> float:
        """Seconds since the last request or stream ended; 0 while any is open."""
        return 0.0 if self.active else time.monotonic() - self.last_activity


# ---------------------------------------------------------------- errors (Error schema)

class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, data: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.data = data


def error_body(code: str, message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    err = {"code": code, "message": message}  # type: Dict[str, Any]
    if data is not None:
        err["data"] = data
    return {"error": err}


def _from_dkp(exc: DKPError) -> ApiError:
    """Map a kernel or directory error onto the HTTP contract."""
    if exc.code == "busy":
        data = dict(exc.data or {})
        if "current" not in data:
            data = {"current": data}
        return ApiError(409, "busy", exc.message, data)
    if exc.code == "bad_request":
        return ApiError(400, "bad_request", exc.message, exc.data)
    if exc.code == "start_timeout":
        return ApiError(504, "start_timeout", exc.message, exc.data)
    if exc.code in ("not_found", "shutting_down"):
        return ApiError(404, "not_found", exc.message, exc.data)
    if exc.code in ("kernel_unreachable", "auth_failed"):
        # The contract has no 5xx for this; a kernel we cannot reach is, for the caller,
        # not there (yet). The code tells the two cases apart.
        return ApiError(404, "kernel_unreachable", exc.message, exc.data)
    return ApiError(500, "internal", exc.message, exc.data)


ERROR_SCHEMA = {"$ref": "#/components/schemas/Error"}


def _responses(*codes: int, **extra: str) -> Dict[int, Dict[str, Any]]:
    names = {400: "The request is malformed.", 401: "Missing or invalid token.",
             403: "The token's permission does not allow this operation.",
             404: "No such kernel, run or share.", 409: "Another run is executing.",
             504: "The kernel did not announce itself within 10 seconds."}
    out = {}
    for code in codes:
        desc = extra.get("d%d" % code) or names.get(code) or "OK"
        if code >= 400:
            out[code] = {"description": desc, "content": {"application/json": {"schema": ERROR_SCHEMA}}}
        else:
            out[code] = {"description": desc}
    return out


# ---------------------------------------------------------------- auth gate (FR-A2)

class AuthGate:
    """Pure ASGI middleware: authenticates before routing (so a bad body from an anonymous
    caller is 401, not 400) and counts open requests and streams for the idle watchdog."""

    def __init__(self, app, auth: Auth, state: ManagerState) -> None:
        self.app = app
        self.auth = auth
        self.state = state

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        self.state.active += 1
        self.state.touch()
        try:
            path = scope["path"]
            if path == "/health" or path == "/docs" or path.startswith("/docs/"):
                await self.app(scope, receive, send)
                return
            token = None
            for name, value in scope.get("headers") or ():
                if name == b"authorization":
                    scheme, _, rest = value.decode("latin-1").partition(" ")
                    if scheme.lower() == "bearer":
                        token = rest.strip()
            if token is None and EVENTS_PATH_RE.match(path):
                qs = parse_qs((scope.get("query_string") or b"").decode("latin-1"))
                token = (qs.get("token") or [None])[0]
            principal = self.auth.authenticate(token)
            if principal is None:
                body = json.dumps(error_body("unauthorized", "missing or invalid token")).encode()
                await send({"type": "http.response.start", "status": 401, "headers": [
                    (b"content-type", b"application/json"), (b"content-length", str(len(body)).encode()),
                    (b"www-authenticate", b"Bearer")]})
                await send({"type": "http.response.body", "body": body})
                return
            scope["darkpyonix.principal"] = principal
            await self.app(scope, receive, send)
        finally:
            self.state.active -= 1
            self.state.touch()


async def principal(request: Request) -> Principal:
    return request.scope["darkpyonix.principal"]


def _require(p: Principal, permission: str) -> None:
    if not p.at_least(permission):
        raise ApiError(403, "forbidden", "permission %s required" % permission)


# ---------------------------------------------------------------- lenient query parsing
# Parameters of operations whose contract lists no 400 never fail validation: bad values
# fall back to the default or are clamped into range.

def _flag(value: Optional[str]) -> bool:
    return str(value).lower() in ("1", "true", "yes", "on")


def _clamp(value: Optional[str], default: int, lo: int, hi: int) -> int:
    try:
        n = int(value) if value is not None else default
    except ValueError:
        n = default
    return max(lo, min(hi, n))


# ---------------------------------------------------------------- request bodies

class StartKernelRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    path: str
    python: Optional[str] = None
    cwd: Optional[str] = None
    env: Optional[Dict[str, str]] = None


class RunRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    mode: Literal["all", "cells"]
    cells: Optional[List[int]] = None
    source: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    on_busy: Literal["reject", "queue"] = "reject"


class CreateShareRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    permission: Literal["viewer1", "viewer2", "viewer3"]
    label: Optional[str] = Field(None, max_length=120)
    expires_at: Optional[datetime.datetime] = None


RESTART_BODY = {"requestBody": {"content": {"application/json": {"schema": {
    "type": "object", "properties": {"hard": {"type": "boolean", "default": False}}}}}}}


# ---------------------------------------------------------------- the app

def create_app(directory: KernelDirectory, auth: Auth, state: Optional[ManagerState] = None,
               docs_dir: str = DOCS_DIR) -> FastAPI:
    state = state or ManagerState()
    app = FastAPI(title="DarkPyonix Kernel Manager API", version=VERSION,
                  docs_url=None, redoc_url=None, openapi_url=None)
    app.state.directory = directory
    app.state.auth = auth
    app.state.manager = state
    app.add_middleware(AuthGate, auth=auth, state=state)
    background = set()  # type: set

    # -------------------------------------------------- error handlers

    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError):
        headers = {"WWW-Authenticate": "Bearer"} if exc.status == 401 else None
        return JSONResponse(error_body(exc.code, exc.message, exc.data), exc.status, headers=headers)

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError):
        errors = jsonable_encoder(exc.errors(), custom_encoder={Exception: str})
        return JSONResponse(error_body("bad_request", "request validation failed", {"errors": errors}), 400)

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(request: Request, exc: StarletteHTTPException):
        code = {401: "unauthorized", 403: "forbidden", 404: "not_found"}.get(exc.status_code, "bad_request")
        return JSONResponse(error_body(code, str(exc.detail)), exc.status_code)

    @app.exception_handler(Exception)
    async def _internal(request: Request, exc: Exception):
        return JSONResponse(error_body("internal", "%s: %s" % (type(exc).__name__, exc)), 500)

    # -------------------------------------------------- helpers

    async def visible(p: Principal, kernel_id: str) -> Dict[str, Any]:
        """Announce body of a running kernel the caller may see, else 404."""
        if not KERNEL_ID_RE.match(kernel_id) or not p.can_see(kernel_id):
            raise ApiError(404, "not_found", "no such kernel: %s" % kernel_id)
        announce = await directory.find(kernel_id)
        if announce is None:
            raise ApiError(404, "not_found", "no running kernel %s" % kernel_id)
        return announce

    def check_id(p: Principal, kernel_id: str) -> None:
        if not KERNEL_ID_RE.match(kernel_id) or not p.can_see(kernel_id):
            raise ApiError(404, "not_found", "no such kernel: %s" % kernel_id)

    async def call(kernel_id: str, method: str, params: Optional[Dict[str, Any]] = None) -> Any:
        try:
            conn = await directory.connection(kernel_id)
            return await conn.request(method, params or {})
        except DKPError as exc:
            if exc.code in ("kernel_unreachable", "auth_failed"):
                await directory.drop(kernel_id)
                directory.forget(kernel_id)
            raise _from_dkp(exc)

    async def kernel_info(kernel_id: str, announce: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            info = await call(kernel_id, "status")
        except ApiError:
            if announce is None:
                raise
            info = announce
        return kernel_view(info)

    def spawn(coro) -> None:
        task = asyncio.ensure_future(coro)
        background.add(task)
        task.add_done_callback(background.discard)

    # -------------------------------------------------- System

    @app.get("/health", operation_id="getHealth", tags=["System"], responses=_responses(200))
    async def get_health():
        return {"status": "ok", "version": VERSION}

    @app.get("/api/v1/manager", operation_id="getManager", tags=["System"], responses=_responses(200, 401))
    async def get_manager(p: Principal = Depends(principal)):
        return {"version": VERSION, "mode": state.mode, "pid": state.pid, "started_at": state.started_at,
                "idle_timeout": int(state.idle_timeout) if state.idle_timeout is not None else None,
                "permission": p.permission, "host": state.host}

    # -------------------------------------------------- Kernels

    @app.get("/api/v1/kernels", operation_id="listKernels", tags=["Kernels"], responses=_responses(200, 401))
    async def list_kernels(refresh: Optional[str] = Query(None), p: Principal = Depends(principal)):
        kernels = await directory.list(refresh=_flag(refresh))
        return {"kernels": [kernel_view(k) for k in kernels if p.can_see(k["kernel_id"])]}

    @app.post("/api/v1/kernels", operation_id="startKernel", tags=["Kernels"], status_code=201,
              responses=_responses(200, 201, 400, 401, 403, 504))
    async def start_kernel(body: StartKernelRequest, p: Principal = Depends(principal)):
        _require(p, "admin")
        try:
            result = await directory.ensure(body.path, body.python, body.cwd, body.env)
        except DKPError as exc:
            raise _from_dkp(exc)
        return JSONResponse(result.kernel, 201 if result.created else 200)

    @app.get("/api/v1/kernels/{kernel_id}", operation_id="getKernel", tags=["Kernels"],
             responses=_responses(200, 401, 404))
    async def get_kernel(kernel_id: str, p: Principal = Depends(principal)):
        announce = await visible(p, kernel_id)
        return await kernel_info(kernel_id, announce)

    @app.delete("/api/v1/kernels/{kernel_id}", operation_id="shutdownKernel", tags=["Kernels"],
                status_code=202, responses=_responses(202, 401, 403, 404))
    async def shutdown_kernel(kernel_id: str, force: Optional[str] = Query(None),
                              p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "admin")
        announce = await visible(p, kernel_id)
        forced = _flag(force)
        try:
            await call(kernel_id, "shutdown")
        except ApiError as exc:
            if not (forced and exc.code == "kernel_unreachable"):
                raise
        directory.forget(kernel_id)
        await directory.drop(kernel_id)
        if forced and announce.get("pid"):
            spawn(_kill_after(int(announce["pid"]), FORCE_KILL_GRACE))
        return JSONResponse({"shutting_down": True}, 202)

    @app.post("/api/v1/kernels/{kernel_id}/interrupt", operation_id="interruptKernel", tags=["Kernels"],
              responses=_responses(200, 401, 403, 404))
    async def interrupt_kernel(kernel_id: str, p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "viewer3")
        await visible(p, kernel_id)
        result = await call(kernel_id, "interrupt") or {}
        out = {"interrupted": bool(result.get("interrupted"))}
        if result.get("run_id"):
            out["run_id"] = result["run_id"]
        return out

    @app.post("/api/v1/kernels/{kernel_id}/restart", operation_id="restartKernel", tags=["Kernels"],
              responses=_responses(200, 401, 403, 404), openapi_extra=RESTART_BODY)
    async def restart_kernel(kernel_id: str, request: Request, p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "admin")
        announce = await visible(p, kernel_id)
        hard = False
        raw = await request.body()
        if raw:
            try:
                body = json.loads(raw)
                hard = isinstance(body, dict) and body.get("hard") is True
            except ValueError:
                hard = False
        await call(kernel_id, "restart", {"hard": hard})
        if hard:
            # The process re-executes itself: reconnect once it announces again (FR-K7).
            await directory.drop(kernel_id)
            directory.forget(kernel_id)
            fresh = await asyncio.to_thread(directory.backend.wait_for_announce, kernel_id, None, START_TIMEOUT)
            announce = fresh or announce
        return await kernel_info(kernel_id, announce)

    @app.get("/api/v1/kernels/{kernel_id}/namespace", operation_id="getNamespace", tags=["Kernels"],
             responses=_responses(200, 401, 403, 404))
    async def get_namespace(kernel_id: str, limit: Optional[str] = Query(None), p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "viewer2")
        await visible(p, kernel_id)
        result = await call(kernel_id, "namespace", {"limit": _clamp(limit, 200, 1, 1000)}) or {}
        return {"variables": result.get("variables", [])}

    # -------------------------------------------------- Documents

    async def document(path: str, kernel_id: Optional[str], outputs: bool) -> Dict[str, Any]:
        try:
            return await asyncio.to_thread(directory.backend.build_document, path, kernel_id, outputs)
        except FileNotFoundError:
            raise ApiError(404, "not_found", "no such file: %s" % path)

    @app.get("/api/v1/kernels/{kernel_id}/document", operation_id="getKernelDocument", tags=["Documents"],
             responses=_responses(200, 401, 404))
    async def get_kernel_document(kernel_id: str, p: Principal = Depends(principal)):
        announce = await visible(p, kernel_id)
        return await document(announce["path"], kernel_id, p.at_least("viewer2"))

    @app.get("/api/v1/documents", operation_id="getDocument", tags=["Documents"],
             responses=_responses(200, 400, 401, 403, 404))
    async def get_document(path: str = Query(...), p: Principal = Depends(principal)):
        _require(p, "admin")
        if not path.endswith(NOTEBOOK_SUFFIXES):
            raise ApiError(400, "bad_request", "not a .py or .pynb file: %s" % path)
        if not os.path.isfile(path):
            raise ApiError(404, "not_found", "no such file: %s" % path)
        return await document(os.path.realpath(os.path.abspath(path)), None, True)

    # -------------------------------------------------- Runs

    @app.get("/api/v1/kernels/{kernel_id}/runs", operation_id="listRuns", tags=["Runs"],
             responses=_responses(200, 401, 403, 404))
    async def list_runs(kernel_id: str, limit: Optional[str] = Query(None), p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "viewer2")
        await visible(p, kernel_id)
        result = await call(kernel_id, "runs.list", {"limit": _clamp(limit, 20, 1, 500)}) or {}
        return {"runs": result.get("runs", [])}

    @app.post("/api/v1/kernels/{kernel_id}/runs", operation_id="startRun", tags=["Runs"], status_code=202,
              responses=_responses(202, 400, 401, 403, 404, 409))
    async def start_run(kernel_id: str, body: RunRequest, p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "viewer3")
        await visible(p, kernel_id)
        if body.mode == "cells" and not body.cells:
            raise ApiError(400, "bad_request", "mode 'cells' needs a non-empty 'cells' list")
        if body.cells and any(i < 0 for i in body.cells):
            raise ApiError(400, "bad_request", "cell indexes start at 0")
        params = {k: v for k, v in body.model_dump().items() if v is not None}
        result = await call(kernel_id, "run", params)
        return JSONResponse(result, 202)

    def run_ref_ok(run_ref: str) -> None:
        if not (RUN_ID_RE.match(run_ref) or run_ref in ("latest", "current")):
            raise ApiError(404, "not_found", "no such run: %s" % run_ref)

    @app.get("/api/v1/kernels/{kernel_id}/runs/{run_ref}", operation_id="getRun", tags=["Runs"],
             responses=_responses(200, 401, 403, 404))
    async def get_run(kernel_id: str, run_ref: str, format: Optional[str] = Query(None),
                      p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "viewer2")
        run_ref_ok(run_ref)
        announce = await visible(p, kernel_id)
        notebook = await call(kernel_id, "runs.get", {"run_id": run_ref})
        if not isinstance(notebook, dict):
            raise ApiError(404, "not_found", "no such run: %s" % run_ref)
        if format == "summary":
            return run_summary(notebook, announce.get("path"))
        return notebook

    @app.delete("/api/v1/kernels/{kernel_id}/runs/{run_ref}", operation_id="cancelRun", tags=["Runs"],
                responses=_responses(200, 401, 403, 404))
    async def cancel_run(kernel_id: str, run_ref: str, p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "viewer3")
        run_ref_ok(run_ref)
        await visible(p, kernel_id)
        if not RUN_ID_RE.match(run_ref):
            # `latest` has finished and `current` is executing; neither is queued.
            return {"cancelled": False}
        result = await call(kernel_id, "cancel", {"run_id": run_ref}) or {}
        return {"cancelled": bool(result.get("cancelled"))}

    # -------------------------------------------------- Events (SSE)

    @app.get("/api/v1/kernels/{kernel_id}/events", operation_id="streamEvents", tags=["Events"],
             responses=_responses(200, 401, 404))
    async def stream_events(kernel_id: str, request: Request, since: Optional[str] = Query(None),
                            p: Principal = Depends(principal)):
        await visible(p, kernel_id)
        start = None  # type: Optional[int]
        for candidate in (request.headers.get("last-event-id"), since):
            if candidate is None:
                continue
            try:
                start = max(0, int(candidate))
                break
            except ValueError:
                continue
        try:
            conn = await directory.connection(kernel_id)
            backlog, truncated, listener = await conn.attach(start)
        except DKPError as exc:
            if exc.code in ("kernel_unreachable", "auth_failed"):
                await directory.drop(kernel_id)
            raise _from_dkp(exc)
        hide_outputs = not p.at_least("viewer2")

        def frame(event: Dict[str, Any]) -> Optional[str]:
            if hide_outputs and event["type"] in OUTPUT_EVENTS:
                return None
            return "id: %d\nevent: %s\ndata: %s\n\n" % (
                event["seq"], event["type"], json.dumps(event["data"], ensure_ascii=False, separators=(",", ":")))

        async def body():
            try:
                yield ": darkpyonix events %s\n\n" % kernel_id
                if truncated is not None:
                    # No id: the client's Last-Event-ID must not move past real events.
                    yield "event: replay_truncated\ndata: %s\n\n" % json.dumps({"oldest_seq": truncated})
                for event in backlog:
                    text = frame(event)
                    if text:
                        yield text
                while True:
                    try:
                        event = await asyncio.wait_for(listener.__anext__(), SSE_KEEPALIVE)
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
                        continue
                    except StopAsyncIteration:
                        return
                    text = frame(event)
                    if text:
                        yield text
            finally:
                listener.close()

        return StreamingResponse(body(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    # -------------------------------------------------- Sharing (FR-A3; persistence is #19)

    @app.get("/api/v1/kernels/{kernel_id}/shares", operation_id="listShares", tags=["Sharing"],
             responses=_responses(200, 401, 403, 404))
    async def list_shares(kernel_id: str, p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "admin")
        return {"shares": auth.list_shares(kernel_id)}

    @app.post("/api/v1/kernels/{kernel_id}/shares", operation_id="createShare", tags=["Sharing"],
              status_code=201, responses=_responses(201, 401, 403, 404))
    async def create_share(kernel_id: str, body: CreateShareRequest, p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "admin")
        if state.mode != "dedicated":
            raise ApiError(403, "forbidden", "share tokens need a dedicated manager (FR-A3)")
        await visible(p, kernel_id)
        expires = body.expires_at
        if expires is not None and expires.tzinfo is None:
            expires = expires.replace(tzinfo=datetime.timezone.utc)
        share = auth.create_share(kernel_id, body.permission, body.label, expires)
        share["url"] = SHARE_LINK % (share["share_id"], share["token"])
        return JSONResponse(share, 201)

    @app.delete("/api/v1/kernels/{kernel_id}/shares/{share_id}", operation_id="revokeShare", tags=["Sharing"],
                status_code=204, responses=_responses(204, 401, 403, 404))
    async def revoke_share(kernel_id: str, share_id: str, p: Principal = Depends(principal)):
        check_id(p, kernel_id)
        _require(p, "admin")
        if not SHARE_ID_RE.match(share_id) or not auth.revoke_share(kernel_id, share_id):
            raise ApiError(404, "not_found", "no such share: %s" % share_id)
        return Response(status_code=204)

    # -------------------------------------------------- API reference page (outside the schema)

    @app.get("/docs", include_in_schema=False)
    async def docs_redirect():
        return RedirectResponse("/docs/")

    @app.get("/docs/", include_in_schema=False)
    async def docs_index():
        return _doc_file(docs_dir, "index.html", "text/html")

    @app.get("/docs/{name}", include_in_schema=False)
    async def docs_file(name: str):
        if name not in DOCS_FILES:
            raise ApiError(404, "not_found", "no such document: %s" % name)
        return _doc_file(docs_dir, name, "application/yaml")

    app.openapi = lambda: _openapi(app)  # type: ignore[method-assign]
    return app


def _doc_file(docs_dir: str, name: str, media_type: str):
    path = os.path.join(docs_dir, name)
    if not os.path.isfile(path):
        raise ApiError(404, "not_found", "API documents are not installed with this manager")
    return FileResponse(path, media_type=media_type)


def _openapi(app: FastAPI) -> Dict[str, Any]:
    """FastAPI's schema without the ``422`` it adds to every operation with parameters:
    validation errors are answered as ``400 bad_request`` (exception handler above)."""
    if app.openapi_schema:
        return app.openapi_schema
    from fastapi.openapi.utils import get_openapi
    schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
    for item in schema.get("paths", {}).values():
        for op in item.values():
            if isinstance(op, dict):
                op.get("responses", {}).pop("422", None)
    for name in ("HTTPValidationError", "ValidationError"):
        schema.get("components", {}).get("schemas", {}).pop(name, None)
    app.openapi_schema = schema
    return schema


def run_summary(notebook: Dict[str, Any], file_path: Optional[str]) -> Dict[str, Any]:
    """``RunSummary`` from a run log's ``metadata.darkpyonix`` (FR-R1)."""
    meta = ((notebook.get("metadata") or {}).get("darkpyonix") or {})
    out = {k: meta.get(k) for k in ("run_id", "status", "mode", "started_at")}
    for k in ("cells", "params", "ended_at", "duration"):
        if k in meta:
            out[k] = meta[k]
    if out.get("duration") is None and meta.get("started_at") and meta.get("ended_at"):
        try:
            t0 = datetime.datetime.fromisoformat(meta["started_at"].replace("Z", "+00:00"))
            t1 = datetime.datetime.fromisoformat(meta["ended_at"].replace("Z", "+00:00"))
            out["duration"] = (t1 - t0).total_seconds()
        except (TypeError, ValueError):
            pass
    path = file_path or meta.get("file")
    if path and out.get("run_id"):
        out["path"] = os.path.join(runs_dir_for(path), "%s.ipynb" % out["run_id"])
    return out


async def _kill_after(pid: int, grace: float) -> None:
    """``shutdownKernel force=true``: kill the process if it has not exited after ``grace``.
    The only place a DarkPyonix component kills a kernel (INTENT D9)."""
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        if not _alive(pid):
            return
        await asyncio.sleep(0.1)
    try:
        os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
    except OSError:
        pass


def _alive(pid: int) -> bool:
    if os.name == "nt":
        return True  # signal 0 would terminate the process on Windows; wait the full grace
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True
