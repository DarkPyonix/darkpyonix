
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
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream\r\n"
                "Cache-Control: no-cache\r\n"
                "Connection: keep-alive\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "\r\n"
            ).encode()
            
            await loop.sock_sendall(client_socket, sse_headers)
            
            # 실행 시작 이벤트
            start_event = f"data: {json.dumps({'status': 'started', 'kernel_id': self.kernel_id})}\n\n"
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
        executing_event = f"data: {json.dumps({'status': 'executing', 'timestamp': time.time()})}\n\n"
        await loop.sock_sendall(client_socket, executing_event.encode())
        
        # 코드 실행 시뮬레이션 (실제로는 IPython 커널 로직)
        await asyncio.sleep(0.1)  # 실행 지연 시뮬레이션
        
        # 결과 스트리밍
        results = [
            {"type": "stdout", "text": "Hello from kernel!"},
            {"type": "execute_result", "data": {"text/plain": "4"}},
        ]
        
        for result in results:
            result_event = f"data: {json.dumps(result)}\n\n"
            await loop.sock_sendall(client_socket, result_event.encode())
            await asyncio.sleep(0.05)  # 스트리밍 효과
            
        # 완료 이벤트
        complete_event = f"data: {json.dumps({'status': 'complete', 'execution_count': 1})}\n\n"
        await loop.sock_sendall(client_socket, complete_event.encode())

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--kernel-id", required=True)
    parser.add_argument("--socket-fd", type=int, required=True)
    args = parser.parse_args()
    
    kernel = SSEKernelProcess(args.kernel_id, args.socket_fd)
    asyncio.run(kernel.run())
