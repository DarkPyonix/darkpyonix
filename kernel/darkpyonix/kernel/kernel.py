"""
Python Kernel Core Implementation
파이썬 커널 핵심 구현 - 실시간 협업 환경용
"""

import subprocess
import sys
import json
import threading
import queue
import time
import tempfile
import uuid
import shutil
import os
import signal
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
from .__main__ import get_kernel_code


class ExecutionRequest:
    """실행 요청 정보"""
    def __init__(
        self, code: str, request_id: str, execution_count: int,
        cell_id: Optional[str] = None, user_id: Optional[str] = None
    ):
        self.code = code
        self.request_id = request_id
        self.execution_count = execution_count
        self.cell_id = cell_id
        self.user_id = user_id
        self.created_at = datetime.now()
        self.status = "pending"  # "pending", "running", "completed", "failed", "interrupted"


class PythonKernel:
    """파이썬 코드를 실행하는 커널 클래스 - 실시간 협업 지원"""

    def __init__(self, kernel_id: Optional[str] = None):
        self.kernel_id = kernel_id or str(uuid.uuid4())
        self.process = None
        self.input_queue = queue.Queue()
        self.output_queue = queue.Queue()
        self.execution_count = 0
        self.is_running = False
        self.temp_dir = tempfile.mkdtemp()
        self.stdout_thread = None
        self.stderr_thread = None

        # 실행 관리
        self.current_execution: Optional[ExecutionRequest] = None
        self.execution_history: List[ExecutionRequest] = []
        self.pending_executions: queue.Queue = queue.Queue()

        # 상태 콜백
        self.status_callbacks: List[Callable] = []
        self.output_callbacks: List[Callable] = []

        # 통계
        self.start_time = None
        self.total_execution_time = 0.0
        self.successful_executions = 0
        self.failed_executions = 0

        # 락
        self._lock = threading.RLock()

    def add_status_callback(self, callback: Callable):
        """상태 변경 콜백 등록"""
        with self._lock:
            self.status_callbacks.append(callback)

    def add_output_callback(self, callback: Callable):
        """출력 콜백 등록"""
        with self._lock:
            self.output_callbacks.append(callback)

    def _notify_status_change(self, status: str, **kwargs):
        """상태 변경 알림"""
        for callback in self.status_callbacks:
            try:
                callback(self.kernel_id, status, **kwargs)
            except Exception as e:
                print(f"Status callback error: {e}")

    def _notify_output(self, output_type: str, data: Any, **kwargs):
        """출력 알림"""
        for callback in self.output_callbacks:
            try:
                callback(self.kernel_id, output_type, data, **kwargs)
            except Exception as e:
                print(f"Output callback error: {e}")

    def start(self):
        """커널 프로세스 시작"""
        if self.is_running:
            return True

        try:
            # 파이썬 서브프로세스 시작
            self.process = subprocess.Popen(
                [sys.executable, "-u", "-c", get_kernel_code()],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=0,
                cwd=self.temp_dir,
                env=dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
            )

            self.is_running = True
            self.start_time = datetime.now()

            # 출력 모니터링 스레드 시작
            self.stdout_thread = threading.Thread(target=self._monitor_stdout, daemon=True)
            self.stderr_thread = threading.Thread(target=self._monitor_stderr, daemon=True)
            self.stdout_thread.start()
            self.stderr_thread.start()

            self._notify_status_change("started", process_id=self.process.pid)
            return True

        except Exception as e:
            self.is_running = False
            self._notify_status_change("start_failed", error=str(e))
            return False

    def stop(self):
        """커널 프로세스 종료"""
        if not self.is_running:
            return

        self.is_running = False

        # 현재 실행 중인 작업 중단
        if self.current_execution:
            self.current_execution.status = "interrupted"
            self._notify_status_change("execution_interrupted",
                                       execution=self.current_execution)

        if self.process:
            try:
                # 먼저 정상 종료 시도
                self.process.terminate()
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # 강제 종료
                self.process.kill()
                self.process.wait()
            except Exception as e:
                print(f"Error stopping kernel process: {e}")
            finally:
                self.process = None

        # 스레드 정리
        if self.stdout_thread and self.stdout_thread.is_alive():
            self.stdout_thread.join(timeout=1)
        if self.stderr_thread and self.stderr_thread.is_alive():
            self.stderr_thread.join(timeout=1)

        # 임시 디렉토리 정리
        try:
            shutil.rmtree(self.temp_dir)
        except Exception as e:
            print(f"Error cleaning temp directory: {e}")

        # 큐 정리
        self._clear_queues()

        self._notify_status_change("stopped")

    def execute(self, code: str, timeout: Optional[float] = None,
                cell_id: Optional[str] = None, user_id: Optional[str] = None) -> Dict[str, Any]:
        """코드 실행"""
        if not self.is_running:
            if not self.start():
                return {
                    "status": "error",
                    "error": "Failed to start kernel",
                    "execution_count": self.execution_count
                }

        with self._lock:
            self.execution_count += 1
            request_id = str(uuid.uuid4())

            execution_request = ExecutionRequest(
                code=code,
                request_id=request_id,
                execution_count=self.execution_count,
                cell_id=cell_id,
                user_id=user_id
            )

        # 실행 요청 전송
        try:
            request = {
                "type": "execute",
                "code": code,
                "execution_count": self.execution_count,
                "request_id": request_id
            }

            request_json = json.dumps(request) + "\n"
            self.process.stdin.write(request_json)
            self.process.stdin.flush()

            execution_request.status = "running"
            self.current_execution = execution_request
            self.execution_history.append(execution_request)

            self._notify_status_change("execution_started",
                                       execution=execution_request)

        except Exception as e:
            execution_request.status = "failed"
            error_msg = f"Failed to send code: {str(e)}"
            self._notify_status_change("execution_failed",
                                       execution=execution_request,
                                       error=error_msg)
            return {
                "status": "error",
                "execution_count": self.execution_count,
                "error": error_msg,
                "request_id": request_id
            }

        # 결과 대기
        return self._wait_for_result(execution_request, timeout)

    def _wait_for_result(self, execution_request: ExecutionRequest, timeout: Optional[float] = None) -> Dict[str, Any]:
        """실행 결과 대기"""
        start_time = time.time()
        request_id = execution_request.request_id

        while True:
            # 타임아웃 체크
            if timeout and (time.time() - start_time > timeout):
                execution_request.status = "failed"
                error_msg = "Execution timed out"
                self._notify_status_change("execution_timeout",
                                          execution=execution_request,
                                          error=error_msg)
                return {
                    "status": "error",
                    "error": error_msg,
                    "execution_count": execution_request.execution_count,
                    "request_id": request_id
                }

            # 프로세스 종료 확인
            if self.process is None or not self.is_running:
                execution_request.status = "failed"
                error_msg = "Kernel process died"
                self._notify_status_change("execution_failed",
                                          execution=execution_request,
                                          error=error_msg)
                return {
                    "status": "error",
                    "error": error_msg,
                    "execution_count": execution_request.execution_count,
                    "request_id": request_id
                }

            # 결과 큐 확인
            try:
                if not self.output_queue.empty():
                    result = self.output_queue.get(block=False)

                    # 현재 요청에 대한 결과인지 확인
                    if result.get("request_id") == request_id:
                        # 실행 완료 처리
                        execution_time = time.time() - start_time
                        self._notify_status_change("execution_completed",
                                                  execution=execution_request,
                                                  result=result,
                                                  execution_time=execution_time)

                        # 실행 상태 업데이트
                        if result.get("status") == "error":
                            execution_request.status = "failed"
                            self.failed_executions += 1
                        else:
                            execution_request.status = "completed"
                            self.successful_executions += 1

                        self.total_execution_time += execution_time
                        self.current_execution = None

                        # 결과 반환
                        result["execution_count"] = execution_request.execution_count
                        result["execution_time"] = execution_time
                        return result
                    else:
                        # 다른 요청에 대한 결과면 다시 큐에 넣음
                        self.output_queue.put(result)
            except queue.Empty:
                pass

            # 잠시 대기
            time.sleep(0.01)

    def _monitor_stdout(self):
        """표준 출력 모니터링"""
        while self.is_running and self.process:
            try:
                line = self.process.stdout.readline()
                if not line:
                    if self.process.poll() is not None:
                        break
                    continue

                try:
                    result = json.loads(line)
                    self.output_queue.put(result)

                    # result가 딕셔너리인지 먼저 확인
                    if isinstance(result, dict):
                        if "stdout" in result:
                            self._notify_output("stdout", result["stdout"],
                                                execution=self.current_execution)
                        elif "error" in result:
                            self._notify_output("error", result["error"],
                                                execution=self.current_execution)
                        elif "data" in result:
                            self._notify_output("data", result["data"],
                                                execution=self.current_execution)
                    else:
                        # result가 딕셔너리가 아닌 경우 (int, string 등)
                        self._notify_output("stdout", str(result),
                                            execution=self.current_execution)

                except json.JSONDecodeError:
                    # JSON이 아닌 일반 출력
                    self._notify_output("stdout", line.strip(),
                                        execution=self.current_execution)
            except Exception as e:
                if self.is_running:
                    print(f"Error monitoring stdout: {e}")

        self._notify_status_change("stdout_closed")

    def _monitor_stderr(self):
        """표준 에러 모니터링"""
        while self.is_running and self.process:
            try:
                line = self.process.stderr.readline()
                if not line:
                    if self.process.poll() is not None:
                        break
                    continue

                self._notify_output("stderr", line.strip(),
                                   execution=self.current_execution)
            except Exception as e:
                if self.is_running:
                    print(f"Error monitoring stderr: {e}")

        self._notify_status_change("stderr_closed")

    def _clear_queues(self):
        """큐 정리"""
        try:
            while not self.input_queue.empty():
                self.input_queue.get(block=False)
        except Exception:
            pass

        try:
            while not self.output_queue.empty():
                self.output_queue.get(block=False)
        except Exception:
            pass

        try:
            while not self.pending_executions.empty():
                self.pending_executions.get(block=False)
        except Exception:
            pass

    def interrupt(self) -> bool:
        """현재 실행 중단"""
        if not self.is_running or not self.process or self.process.poll() is not None:
            return False

        if not self.current_execution:
            return True  # 실행 중인 코드가 없음

        try:
            # 인터럽트 신호 전송
            if sys.platform == "win32":
                # Windows에서는 CTRL+C 이벤트를 보냄
                os.kill(self.process.pid, signal.CTRL_C_EVENT)
            else:
                # Unix 계열에서는 SIGINT 보냄
                os.kill(self.process.pid, signal.SIGINT)

            # 인터럽트 상태로 변경
            if self.current_execution:
                self.current_execution.status = "interrupted"
                self._notify_status_change("execution_interrupted",
                                          execution=self.current_execution)
            return True
        except Exception as e:
            print(f"Failed to interrupt execution: {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        """커널 상태 조회"""
        with self._lock:
            status = {
                "kernel_id": self.kernel_id,
                "is_running": self.is_running,
                "execution_count": self.execution_count,
                "pid": self.process.pid if self.process else None,
                "uptime": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
                "memory_usage": self.get_memory_usage(),
                "current_execution": self.current_execution.request_id if self.current_execution else None,
                "total_executions": len(self.execution_history),
                "successful_executions": self.successful_executions,
                "failed_executions": self.failed_executions,
                "total_execution_time": self.total_execution_time
            }
            return status

    def get_memory_usage(self) -> Optional[float]:
        """메모리 사용량 조회 (MB)"""
        if not self.is_running or not self.process:
            return None

        try:
            if sys.platform == "win32":
                import psutil
                process = psutil.Process(self.process.pid)
                return process.memory_info().rss / (1024 * 1024)  # MB 단위
            else:
                # Unix 계열
                with open(f"/proc/{self.process.pid}/status", "r") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            return float(line.split()[1]) / 1024  # MB 단위
                return None
        except Exception:
            return None

    def get_execution_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """실행 히스토리 조회"""
        with self._lock:
            history = []
            executions = self.execution_history

            if limit and limit > 0:
                executions = executions[-limit:]

            for execution in executions:
                history.append({
                    "request_id": execution.request_id,
                    "code": execution.code,
                    "execution_count": execution.execution_count,
                    "status": execution.status,
                    "cell_id": execution.cell_id,
                    "user_id": execution.user_id,
                    "created_at": execution.created_at.isoformat()
                })
            return history

    def is_healthy(self) -> bool:
        """커널 상태 확인"""
        if not self.is_running:
            return False

        if not self.process:
            return False

        return self.process.poll() is None


# 테스트 코드 (직접 실행될 때만 실행)
if __name__ == "__main__":
    def status_callback(kernel_id, status, **kwargs):
        print(f"[{kernel_id[:8]}] Status: {status} {kwargs}")

    def output_callback(kernel_id, output_type, data, **kwargs):
        print(f"[{kernel_id[:8]}] Output: {output_type} - {data}")

    kernel = PythonKernel()
    kernel.add_status_callback(status_callback)
    kernel.add_output_callback(output_callback)

    print("Starting kernel...")
    kernel.start()

    try:
        print("\nExecuting: print('Hello, World!')")
        result = kernel.execute("print('Hello, World!')")
        print("Result:", result)

        print("\nExecuting: x = 42; x")
        result = kernel.execute("x = 42\nx")
        print("Result:", result)

        print("\nKernel status:")
        status = kernel.get_status()
        for key, value in status.items():
            print(f"  {key}: {value}")

        print("\nExecution history:")
        history = kernel.get_execution_history()
        for i, exec_info in enumerate(history):
            print(f"  {i+1}. {exec_info['status']} - {exec_info['code'][:50]}...")

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        print("\nStopping kernel...")
        kernel.stop()
        print("Kernel stopped.")

