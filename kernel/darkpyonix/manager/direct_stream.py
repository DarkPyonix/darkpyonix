from __future__ import annotations
import asyncio, socket, uuid, os, base64, logging, json
from typing import Dict
from .kernel_registry import registry
try:
    from .fd_pass import fd_pass_server  # POSIX only
except Exception:  # pragma: no cover
    fd_pass_server = None  # type: ignore

logger = logging.getLogger("darkpyonix.direct")

class PendingToken:
    __slots__ = ("kernel_id",)
    def __init__(self, kernel_id: str):
        self.kernel_id = kernel_id

class DirectStreamBroker:
    """Single listener; client sends token; manager passes client socket directly to kernel.

    Windows: uses socket.share; kernel polls parent pipe for adoption commands (simplified new path).
    POSIX:   duplicates socket fd and sends via FDPassServer (SCM_RIGHTS) with metadata.
    """
    def __init__(self):
        self.pending: Dict[str, PendingToken] = {}
        self._server: asyncio.AbstractServer | None = None
        self.port: int | None = None

    async def ensure_started(self):
        if self._server:
            return
        self._server = await asyncio.start_server(self._handle_client, host="127.0.0.1", port=0)
        sockets = self._server.sockets
        if sockets:
            self.port = sockets[0].getsockname()[1]
        asyncio.create_task(self._server.serve_forever())
        logger.info("Direct stream listener on 127.0.0.1:%s", self.port)

    async def create_token(self, kernel_id: str):
        await self.ensure_started()
        token = uuid.uuid4().hex
        self.pending[token] = PendingToken(kernel_id)
        return {"port": self.port, "token": token}

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        token = None
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            token = line.strip().decode()
            pend = self.pending.pop(token, None)
            if not pend:
                writer.write(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n")
                await writer.drain()
                writer.close()
                return
            kp = registry.get(pend.kernel_id)
            if not kp or not kp.proc or kp.proc.poll() is not None:
                writer.write(b"HTTP/1.1 410 Gone\r\nConnection: close\r\n\r\n")
                await writer.drain()
                writer.close()
                return
            raw_sock: socket.socket = writer.get_extra_info("socket")
            if os.name == 'nt':
                # Write a small side-channel file for adoption (named pipe / temp dir approach could be used; placeholder no-op)
                share_blob = raw_sock.share(kp.proc.pid)
                share_b64 = base64.b64encode(share_blob).decode()
                # Store in env-like temp mapping for kernel polling (out-of-process simple file)
                temp_path = f"_adopt_{pend.kernel_id}_{token}.sockshare"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump({"share": share_b64, "token": token}, f)
                try:
                    raw_sock.detach()
                except Exception:
                    pass
            else:
                if not fd_pass_server:
                    writer.write(b"HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\n")
                    await writer.drain(); writer.close(); return
                fd = raw_sock.fileno()
                dup_fd = os.dup(fd)
                try:
                    fd_pass_server.send_fd(pend.kernel_id, dup_fd, {"token": token})
                except Exception as e:
                    logger.error("FD pass failed: %s", e)
                    writer.write(b"HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\n")
                    await writer.drain(); writer.close(); return
                try:
                    raw_sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                raw_sock.close()
        except Exception as e:
            logger.warning("direct stream handler error: %s", e)
            try:
                writer.close()
            except Exception:
                pass
        finally:
            if token and token in self.pending:
                self.pending.pop(token, None)

direct_stream_broker = DirectStreamBroker()
