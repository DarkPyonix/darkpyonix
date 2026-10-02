from __future__ import annotations
from enum import Enum
from pydantic import BaseModel
from typing import Literal, List, Optional, Dict, Any

class ControlCommand(str, Enum):
    PING = "ping"
    START_KERNEL = "start_kernel"
    STOP_KERNEL = "stop_kernel"
    EXECUTE_CELL = "execute_cell"
    ATTACH_STREAM = "attach_stream"  # request hijack of SSE-like stream

class KernelStatus(str, Enum):
    NONE = "none"
    RUNNING = "running"
    INTERRUPTED = "interrupted"
    COMPLETED = "completed"
    FAILED = "failed"

class Cell(BaseModel):
    id: str
    index: int
    cell_type: str
    source: str
    execution_count: int = 0
    outputs: List[Dict[str, Any]] = []
    locked: bool = False
    authorized_to: Optional[str] = None
    focused_by: List[str] = []
    status: KernelStatus = KernelStatus.NONE
    auto_run: bool | None = None
    collapsed: bool | None = None
    title: str | None = None
    layout: str | None = None  # horizontal etc

class ControlMessage(BaseModel):
    cmd: ControlCommand
    kernel_id: Optional[str] = None
    payload: Dict[str, Any] | None = None

class StreamAttachRequest(BaseModel):
    kernel_id: str
    stream_id: str  # logical stream (e.g. cell output watcher)
    request_id: str  # correlate to HTTP request in manager

class StreamData(BaseModel):
    stream_id: str
    data: Any
    event: str = "message"

class ExecuteRequest(BaseModel):
    kernel_id: str
    cell_id: str
    code: str
    exec_id: str

