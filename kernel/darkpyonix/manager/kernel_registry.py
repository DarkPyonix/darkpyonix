from __future__ import annotations
import subprocess, sys, os, asyncio
from typing import Dict, Optional
try:
    from .fd_pass import fd_pass_server  # POSIX only
except Exception:  # pragma: no cover
    fd_pass_server = None  # type: ignore

class KernelProcess:
    def __init__(self, kernel_id: str, python_executable: str):
        self.kernel_id = kernel_id
        self.python_executable = python_executable
        self.proc: subprocess.Popen | None = None

    def start(self):
        if self.proc and self.proc.poll() is None:
            return
        env = os.environ.copy()
        env["DARKPYONIX_KERNEL_ID"] = self.kernel_id
        if os.name != 'nt' and fd_pass_server:
            env["FD_PASS_PATH"] = fd_pass_server.path
        cmd = [self.python_executable, "-m", "kernel.entry"]
        self.proc = subprocess.Popen(cmd, env=env)

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()

class KernelRegistry:
    def __init__(self):
        self._kernels: Dict[str, KernelProcess] = {}

    async def create_kernel(self, kernel_id: str, python_executable: Optional[str] = None):
        await self.ensure_control_server()
        if not python_executable:
            python_executable = sys.executable
        if os.name != 'nt' and fd_pass_server:
            await fd_pass_server.start()
        # control host/port no longer used; pass dummy
        kp = KernelProcess(kernel_id, python_executable, "127.0.0.1", 0)
        kp.start()
        self._kernels[kernel_id] = kp
        return kp

    def get(self, kernel_id: str) -> KernelProcess | None:
        return self._kernels.get(kernel_id)

    def delete(self, kernel_id: str):
        kp = self._kernels.pop(kernel_id, None)
        if kp:
            kp.stop()

registry = KernelRegistry()
