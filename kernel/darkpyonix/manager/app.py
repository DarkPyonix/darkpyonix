import asyncio
import socket
import array
import os
import platform
import json
import uuid
from typing import Dict, List, Optional, AsyncGenerator
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
#import uvloop  # Linux/macOS에서 성능 향상


class UnixFDManager:
    def __init__(self):
        self.unix_sockets: Dict[str, socket.socket] = {}

    def send_fd(self, sock: socket.socket, fd: int, msg: bytes = b"FD"):
        """Unix Domain Socket을 통해 FD 전달"""
        try:
            return sock.sendmsg(
                [msg],
                [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [fd]))]
            )
        except OSError as e:
            raise RuntimeError(f"Failed to send FD {fd}: {e}")

    def recv_fd(self, sock: socket.socket, maxfds: int = 1) -> tuple[bytes, List[int]]:
        """Unix Domain Socket에서 FD 수신"""
        fds = array.array("i")
        msg, ancdata, flags, addr = sock.recvmsg(
            1024,
            socket.CMSG_LEN(maxfds * fds.itemsize)
        )

        received_fds = []
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if (cmsg_level == socket.SOL_SOCKET and
                    cmsg_type == socket.SCM_RIGHTS):
                fds.frombytes(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
                received_fds.extend(fds.tolist())

        return msg, received_fds

# SSE 연결 관리
class SSEConnection:
    def __init__(self, client_socket: socket.socket, client_addr: tuple):
        self.client_socket = client_socket
        self.client_addr = client_addr
        self.connection_id = str(uuid.uuid4())
        self.is_active = True

    def close(self):
        """SSE 연결 종료"""
        self.is_active = False
        if self.client_socket:
            try:
                self.client_socket.close()
            except:
                pass

# 커널 매니저 클래스
class KernelManager:
    def __init__(self):
        self.kernels: Dict[str, dict] = {}  # kernel_id -> kernel_info
        self.sse_connections: Dict[str, SSEConnection] = {}
        self.kernel_sockets: Dict[str, socket.socket] = {}

        # 플랫폼별 FD 매니저 초기화
        if platform.system() == "Windows":
            self.fd_manager = WindowsFDManager()
        else:
            self.fd_manager = UnixFDManager()

    async def create_kernel(self, kernel_id: str) -> dict:
        """새 커널 프로세스 생성"""
        # Unix Domain Socket 쌍 생성 (커널 매니저 <-> 커널)
        if platform.system() != "Windows":
            sock_pair = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
            manager_sock, kernel_sock = sock_pair

            # 커널 프로세스 시작
            kernel_process = await asyncio.create_subprocess_exec(
                "python", "kernel_process.py",
                "--kernel-id", kernel_id,
                "--socket-fd", str(kernel_sock.fileno()),
                pass_fds=[kernel_sock.fileno()]  # FD를 자식 프로세스에 전달
            )

            # 소켓 저장
            self.kernel_sockets[kernel_id] = manager_sock
            kernel_sock.close()  # 부모에서는 더 이상 필요 없음

        else:
            # Windows Named Pipe 구현
            pipe_name = f"kernel_{kernel_id}"
            # Windows 구현...
            pass

        kernel_info = {
            "process": kernel_process,
            "status": "running",
            "created_at": asyncio.get_event_loop().time()
        }
        self.kernels[kernel_id] = kernel_info
        return kernel_info

    async def pass_http_socket_to_kernel(self, kernel_id: str, request: Request):
        """HTTP 연결의 소켓을 커널에 전달"""
        if platform.system() != "Windows":
            # FastAPI/Uvicorn에서 원시 소켓 추출
            transport = request.scope.get("transport")
            if not transport:
                raise ValueError("No transport found in request scope")

            # Uvicorn transport에서 소켓 추출
            client_socket = transport.get_extra_info('socket')
            if not client_socket:
                raise ValueError("Could not extract socket from transport")

            kernel_sock = self.kernel_sockets.get(kernel_id)
            if not kernel_sock:
                raise ValueError(f"Kernel {kernel_id} not found")

            # 클라이언트 소켓 FD를 커널에 전달
            client_fd = client_socket.fileno()
            self.fd_manager.send_fd(kernel_sock, client_fd)

            # 클라이언트 정보도 함께 전송
            client_info = {
                "connection_type": "sse",
                "client_addr": request.client.host if request.client else "unknown",
                "user_agent": request.headers.get("user-agent", ""),
                "connection_id": str(uuid.uuid4())
            }
            await self._send_json_to_kernel(kernel_sock, client_info)

            return client_info["connection_id"]

        else:
            # Windows: DuplicateHandle 구현
            pass

    async def _send_json_to_kernel(self, kernel_sock: socket.socket, data: dict):
        """커널에 JSON 데이터 전송"""
        message = json.dumps(data).encode() + b'\n'
        kernel_sock.send(message)

    async def execute_code(self, kernel_id: str, code: str, request: Request) -> AsyncGenerator[str, None]:
        """코드 실행 요청을 커널에 전달하고 SSE 스트림 생성"""
        # HTTP 소켓을 커널에 전달
        connection_id = await self.pass_http_socket_to_kernel(kernel_id, request)

        # 실행 요청을 커널에 전송
        execute_request = {
            "msg_type": "execute_request",
            "connection_id": connection_id,
            "content": {
                "code": code,
                "silent": False
            }
        }

        kernel_sock = self.kernel_sockets.get(kernel_id)
        if kernel_sock:
            await self._send_json_to_kernel(kernel_sock, execute_request)

        # 이제 커널이 SSE 응답을 직접 클라이언트로 전송
        # 매니저는 연결이 활성화되어 있는지만 확인
        yield f"data: {json.dumps({'status': 'executing', 'connection_id': connection_id})}\n\n"

        # 커널이 직접 응답을 보내므로 여기서는 연결 상태만 모니터링
        while True:
            await asyncio.sleep(0.1)
            # 실제로는 커널이 직접 SSE 응답을 보냄
            break

# FastAPI 애플리케이션
class JupyterKernelAPI:
    def __init__(self):
        self.app = FastAPI(title="Jupyter Kernel Manager with SSE FD Passing")
        self.kernel_manager = KernelManager()
        self.setup_routes()

    def setup_routes(self):
        @self.app.post("/kernels")
        async def create_kernel():
            """새 커널 생성"""
            kernel_id = str(uuid.uuid4())
            kernel_info = await self.kernel_manager.create_kernel(kernel_id)
            return {"kernel_id": kernel_id, "status": kernel_info["status"]}

        @self.app.get("/kernels")
        async def list_kernels():
            """커널 목록 조회"""
            return {
                "kernels": [
                    {"kernel_id": kid, "status": info["status"]}
                    for kid, info in self.kernel_manager.kernels.items()
                ]
            }

        @self.app.delete("/kernels/{kernel_id}")
        async def delete_kernel(kernel_id: str):
            """커널 삭제"""
            if kernel_id not in self.kernel_manager.kernels:
                raise HTTPException(status_code=404, detail="Kernel not found")

            kernel_info = self.kernel_manager.kernels[kernel_id]
            if kernel_info["process"]:
                kernel_info["process"].terminate()
                await kernel_info["process"].wait()

            del self.kernel_manager.kernels[kernel_id]
            return {"status": "deleted"}

        @self.app.post("/kernels/{kernel_id}/execute")
        async def execute_code_endpoint(kernel_id: str, request: Request):
            """코드 실행 (SSE 응답)"""
            if kernel_id not in self.kernel_manager.kernels:
                raise HTTPException(status_code=404, detail="Kernel not found")

            # 요청 본문에서 코드 추출
            body = await request.body()
            try:
                data = json.loads(body)
                code = data.get("code", "")
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid JSON")

            # SSE 스트림 생성 및 FD 전달
            async def sse_generator():
                async for chunk in self.kernel_manager.execute_code(kernel_id, code, request):
                    yield chunk

            return StreamingResponse(
                sse_generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Cache-Control"
                }
            )

# 커널 프로세스 측 코드 (kernel_process.py)
KERNEL_PROCESS_CODE = '''
import asyncio
import socket
import json
import sys
import array
import time
import traceback

class SSEKernelProcess:
    def __init__(self, kernel_id: str, manager_socket_fd: int):
        self.kernel_id = kernel_id
        # 매니저로부터 받은 소켓 FD 복원
        self.manager_socket = socket.fromfd(
            manager_socket_fd, 
            socket.AF_UNIX, 
            socket.SOCK_STREAM
        )
        self.client_connections = {}
        
    async def run(self):
        """커널 메인 루프"""
        print(f"Kernel {self.kernel_id} started")
        
        while True:
            try:
                # 매니저로부터 클라이언트 FD 수신 대기
                msg, fds = await self.receive_fd_from_manager()
                
                if fds:
                    # 새 클라이언트 소켓 처리
                    for fd in fds:
                        client_socket = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
                        asyncio.create_task(self.handle_client_sse(client_socket, msg))
                        
            except Exception as e:
                print(f"Kernel {self.kernel_id} error: {e}")
                traceback.print_exc()
                break
                
    async def receive_fd_from_manager(self):
        """매니저로부터 FD 수신"""
        loop = asyncio.get_event_loop()
        
        # 논블로킹으로 메시지 수신
        try:
            ready = await asyncio.wait_for(
                loop.sock_recv(self.manager_socket, 1024), 
                timeout=1.0
            )
        except asyncio.TimeoutError:
            return None, []
            
        if not ready:
            raise ConnectionError("Manager socket closed")
            
        # SCM_RIGHTS로 FD 수신
        fds = array.array("i")
        msg, ancdata, flags, addr = self.manager_socket.recvmsg(
            1024, socket.CMSG_LEN(1 * fds.itemsize)
        )
        
        received_fds = []
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if (cmsg_level == socket.SOL_SOCKET and 
                cmsg_type == socket.SCM_RIGHTS):
                fds.frombytes(cmsg_data)
                received_fds.extend(fds.tolist())
                
        # JSON 메시지 파싱
        try:
            client_info = json.loads(msg.decode().strip())
        except json.JSONDecodeError:
            client_info = {}
            
        return client_info, received_fds
        
    async def handle_client_sse(self, client_socket: socket.socket, client_info: dict):
        """SSE 클라이언트와 직접 통신"""
        connection_id = client_info.get("connection_id", "unknown")
        print(f"Kernel {self.kernel_id} handling SSE client {connection_id} directly")
        
        loop = asyncio.get_event_loop()
        
        try:
            # HTTP 요청 읽기 (이미 전달된 연결이므로 요청 데이터가 있을 수 있음)
            
            # SSE 응답 헤더 전송
            sse_headers = (
                "HTTP/1.1 200 OK\\r\\n"
                "Content-Type: text/event-stream\\r\\n"
                "Cache-Control: no-cache\\r\\n"
                "Connection: keep-alive\\r\\n"
                "Access-Control-Allow-Origin: *\\r\\n"
                "\\r\\n"
            ).encode()
            
            await loop.sock_sendall(client_socket, sse_headers)
            
            # 실행 시작 이벤트
            start_event = f"data: {json.dumps({'status': 'started', 'kernel_id': self.kernel_id})}\\n\\n"
            await loop.sock_sendall(client_socket, start_event.encode())
            
            # 실제 코드 실행 시뮬레이션
            await self.execute_python_code(client_socket, client_info)
            
        except Exception as e:
            print(f"SSE Client {connection_id} error: {e}")
            traceback.print_exc()
        finally:
            client_socket.close()
            
    async def execute_python_code(self, client_socket: socket.socket, client_info: dict):
        """Python 코드 실행 및 SSE 스트리밍"""
        loop = asyncio.get_event_loop()
        
        # 실행 중 이벤트
        executing_event = f"data: {json.dumps({'status': 'executing', 'timestamp': time.time()})}\\n\\n"
        await loop.sock_sendall(client_socket, executing_event.encode())
        
        # 코드 실행 시뮬레이션 (실제로는 IPython 커널 로직)
        await asyncio.sleep(0.1)  # 실행 지연 시뮬레이션
        
        # 결과 스트리밍
        results = [
            {"type": "stdout", "text": "Hello from kernel!"},
            {"type": "execute_result", "data": {"text/plain": "4"}},
        ]
        
        for result in results:
            result_event = f"data: {json.dumps(result)}\\n\\n"
            await loop.sock_sendall(client_socket, result_event.encode())
            await asyncio.sleep(0.05)  # 스트리밍 효과
            
        # 완료 이벤트
        complete_event = f"data: {json.dumps({'status': 'complete', 'execution_count': 1})}\\n\\n"
        await loop.sock_sendall(client_socket, complete_event.encode())

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--kernel-id", required=True)
    parser.add_argument("--socket-fd", type=int, required=True)
    args = parser.parse_args()
    
    kernel = SSEKernelProcess(args.kernel_id, args.socket_fd)
    asyncio.run(kernel.run())
'''

# 실행을 위한 설정
def setup_uvloop():
    """성능 향상을 위한 uvloop 설정 (Linux/macOS)"""
    if platform.system() != "Windows":
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            print("uvloop not available, using default event loop")

async def main():
    """메인 실행 함수"""
    setup_uvloop()

    # kernel_process.py 파일 생성
    with open("kernel_process.py", "w") as f:
        f.write(KERNEL_PROCESS_CODE)

    # FastAPI 앱 실행
    kernel_api = JupyterKernelAPI()

    import uvicorn
    config = uvicorn.Config(
        kernel_api.app,
        host="0.0.0.0",
        port=8000,
        #loop="uvloop" if platform.system() != "Windows" else "asyncio",
        access_log=False  # 성능을 위해 비활성화
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == '__main__':
    asyncio.run(main())
