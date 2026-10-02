from __future__ import annotations
import asyncio, socket, os, json, logging
from typing import Dict

logger = logging.getLogger("darkpyonix.fdpass")

class FDPassServer:
    """Unix domain socket server to pass accepted client FDs (POSIX SCM_RIGHTS).
    Protocol:
      Kernel connects and sends: <kernel_id>\n
      For each FD pass: manager sends 4-byte big-endian length + JSON meta, then sendmsg with FD.
    """
    def __init__(self, path: str):
        self.path = path
        self._server: asyncio.AbstractServer | None = None
        self._kernel_conns: Dict[str, socket.socket] = {}

    async def start(self):
        if self._server:
            return
        try:
            if os.path.exists(self.path):
                os.unlink(self.path)
        except OSError:
            pass
        self._server = await asyncio.start_unix_server(self._handle_client, path=self.path)
        logger.info("FDPassServer listening at %s", self.path)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        kernel_id = None
        try:
            line = await reader.readline()
            if not line:
                return
            kernel_id = line.strip().decode()
            raw_sock: socket.socket = writer.get_extra_info("socket")
            raw_sock.setblocking(True)
            self._kernel_conns[kernel_id] = raw_sock
            logger.info("Kernel %s registered for FD passing", kernel_id)
            await reader.read()  # keep open until kernel closes
        finally:
            if kernel_id and kernel_id in self._kernel_conns:
                self._kernel_conns.pop(kernel_id, None)
            try:
                writer.close()
            except Exception:
                pass

    def send_fd(self, kernel_id: str, fd: int, meta: dict):
        sock = self._kernel_conns.get(kernel_id)
        if not sock:
            raise RuntimeError(f"kernel {kernel_id} not connected to FD pass server")
        meta_bytes = json.dumps(meta).encode()
        header = len(meta_bytes).to_bytes(4, 'big')
        sock.sendall(header + meta_bytes)
        sock.sendmsg([b'F'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fd.to_bytes(4, 'little'))])
        os.close(fd)

fd_pass_server = FDPassServer("/tmp/darkpy_fdpass.sock")
