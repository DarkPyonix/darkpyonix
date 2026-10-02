from __future__ import annotations
import uuid
from fastapi import FastAPI, HTTPException, Path
from .kernel_registry import registry
from .direct_stream import direct_stream_broker


class KernelManager:
    def __init__(self):
        self.app = FastAPI(title="DarkPyonix Kernel Manager")
        self.cells: dict[str, dict] = {}
        self._mount_routes()

    def _mount_routes(self):
        app = self.app

        @app.post("/kernels/{kernel_id}")
        async def create_kernel(kernel_id: str = Path(...)):
            if registry.get(kernel_id):
                raise HTTPException(400, "kernel exists")
            await registry.create_kernel(kernel_id)
            return {"kernel_id": kernel_id}

        @app.get("/kernels/{kernel_id}")
        async def get_kernel(kernel_id: str):
            kp = registry.get(kernel_id)
            if not kp:
                raise HTTPException(404, "not found")
            return {"kernel_id": kernel_id, "alive": kp.proc.poll() is None}

        @app.delete("/kernels/{kernel_id}")
        async def delete_kernel(kernel_id: str):
            registry.delete(kernel_id)
            return {"deleted": True}

        @app.post("/kernels/{kernel_id}/cell-sync-init")
        async def cell_sync_init(kernel_id: str):
            if not registry.get(kernel_id):
                raise HTTPException(404, "kernel not found")
            info = await direct_stream_broker.create_token(kernel_id)
            info["purpose"] = "cell-sync"
            return {"kernel_id": kernel_id, **info}

        @app.post("/kernels/{kernel_id}/cell-alarm-init")
        async def cell_alarm_init(kernel_id: str):
            if not registry.get(kernel_id):
                raise HTTPException(404, "kernel not found")
            info = await direct_stream_broker.create_token(kernel_id)
            info["purpose"] = "cell-alarm"
            return {"kernel_id": kernel_id, **info}

        @app.post("/kernels/{kernel_id}/cells")
        async def create_cell(kernel_id: str, payload: dict):  # noqa: ARG001 demo
            cell_id = payload.get("id") or uuid.uuid4().hex
            self.cells[cell_id] = payload
            return {"id": cell_id}

        @app.get("/kernels/{kernel_id}/cells")
        async def list_cells(kernel_id: str):  # noqa: ARG001 demo
            return list(self.cells.values())

        @app.patch("/kernels/{kernel_id}/cells/{cell_id}")
        async def patch_cell(kernel_id: str, cell_id: str, payload: dict):  # noqa: ARG001 demo
            c = self.cells.get(cell_id)
            if not c:
                raise HTTPException(404, "no cell")
            c.update(payload)
            return {"id": cell_id}


manager = KernelManager()
app = manager.app


@app.on_event("startup")
async def _startup():  # pragma: no cover
    return None
