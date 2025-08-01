# filepath: c:\Users\com\PycharmProjects\darkpyonix\kernel\darkpyonix\manager\manager.py
"""
Real-time Collaborative Kernel Manager
실시간 협업 커널 관리자 구현
"""

import uuid
import threading
import os
import base64
import secrets
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field

from ..kernel import PythonKernel


@dataclass
class CellLock:
    """셀 락 정보"""
    cell_id: str
    user_id: str
    user_name: str
    locked_at: datetime

    def to_dict(self) -> dict:
        return {
            "cell_id": self.cell_id,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "locked_at": self.locked_at.isoformat()
        }


@dataclass
class Cell:
    """셀 정보"""
    kernel_id: str
    cell_id: str
    cell_type: str  # "code" | "markdown"
    source: str
    execution_count: Optional[int] = None
    outputs: List[dict] = field(default_factory=list)
    lock: Optional[CellLock] = None
    position: int = 0
    code_status: Optional[str] = None  # "pending" | "running" | "completed" | "failed" | "interrupted"

    def to_dict(self) -> dict:
        return {
            "kernel_id": self.kernel_id,
            "cell_id": self.cell_id,
            "cell_type": self.cell_type,
            "source": self.source,
            "execution_count": self.execution_count,
            "outputs": self.outputs,
            "lock": self.lock.to_dict() if self.lock else None,
            "position": self.position,
            "code_status": self.code_status
        }


@dataclass
class User:
    """사용자 정보"""
    kernel_id: Optional[str]
    user_id: str
    user_name: str
    permission: str  # "viewer1" | "viewer2" | "viewer3" | "admin"
    focused_cell_id: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "kernel_id": self.kernel_id,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "permission": self.permission,
            "focused_cell_id": self.focused_cell_id
        }


@dataclass
class Kernel:
    """커널 정보"""
    kernel_id: str
    tokens: List[dict] = field(default_factory=list)
    status: str = "dead"  # "dead" | "starting" | "running"
    language: str = "python"
    # 새로 추가된 필드들
    file_path: Optional[str] = None
    auto_sync_enabled: bool = True
    last_sync_time: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "kernel_id": self.kernel_id,
            "tokens": self.tokens,
            "status": self.status,
            "language": self.language,
            "file_path": self.file_path,
            "auto_sync_enabled": self.auto_sync_enabled,
            "last_sync_time": self.last_sync_time.isoformat() if self.last_sync_time else None
        }


@dataclass
class Notebook:
    """노트북 정보"""
    name: str
    kernel_id: Optional[str] = None
    cells: List[Cell] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "kernel_id": self.kernel_id,
            "cells": [cell.to_dict() for cell in self.cells]
        }


@dataclass
class Execution:
    """실행 정보"""
    cell_id: str
    kernel_id: str
    code: str
    status: str  # "pending" | "running" | "completed" | "failed" | "interrupted"
    started_at: datetime
    completed_at: Optional[datetime] = None
    execution_time: Optional[float] = None
    outputs: List[dict] = field(default_factory=list)
    error: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "cell_id": self.cell_id,
            "kernel_id": self.kernel_id,
            "code": self.code,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "execution_time": self.execution_time,
            "outputs": self.outputs,
            "error": self.error
        }


@dataclass
class History:
    """실행 히스토리"""
    kernel_id: Optional[str]
    executions: List[Execution] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "kernel_id": self.kernel_id,
            "executions": [execution.to_dict() for execution in self.executions]
        }


class KernelManager:
    """실시간 협업 커널 관리자"""

    def __init__(self):
        self.kernels: Dict[str, PythonKernel] = {}  # 실제 커널 인스턴스
        self.kernel_metadata: Dict[str, Kernel] = {}  # 커널 메타데이터
        self.notebooks: Dict[str, Notebook] = {}  # 노트북 정보 (kernel_id -> notebook)
        self.histories: Dict[str, History] = {}  # 실행 히스토리 (kernel_id -> history)
        self.users: Dict[str, User] = {}  # 연결된 사용자들 (user_id -> user)
        self.cells: Dict[str, Cell] = {}  # 모든 셀 정보 (cell_id -> cell)
        self.executions: Dict[str, Execution] = {}  # 실행 중인 작업들 (execution_id -> execution)

        # 토큰 관리
        self.tokens: Dict[str, dict] = {}  # token -> {kernel_id, permission, created_at, password}
        self.blacklisted_tokens: Set[str] = set()  # 블랙리스트된 토큰들
        self.server_password: Optional[str] = None  # 서버 마스터 패스워드

        # 동기화를 위한 락
        self._lock = threading.RLock()
        self._cell_locks: Dict[str, CellLock] = {}  # 셀별 락 (cell_id -> lock)

        # 웹소켓 연결 관리
        self.websocket_connections: Dict[str, Any] = {}  # user_id -> websocket

        # 자동 정리
        self._cleanup_timer = None
        self._auto_cleanup_enabled = False

    def _base64_file_path(self, file_path: str) -> str:
        """파일 경로를 base64로 인코딩하여 커널 ID 생성"""

        normalized_path = os.path.abspath(file_path)
        # UTF-8로 인코딩 후 base64 인코딩
        encoded_bytes = base64.b64encode(normalized_path.encode('utf-8'))
        # URL-safe base64로 변환하고 패딩 제거
        encoded_str = base64.urlsafe_b64encode(normalized_path.encode('utf-8')).decode('utf-8').rstrip('=')
        return f"file_{encoded_str}"

    def generate_server_password(self) -> str:
        """서버 시작시 마스터 패스워드 생성"""
        self.server_password = secrets.token_urlsafe(16)
        return self.server_password

    def create_token(self, kernel_id: str, permission: str = "viewer1", password: Optional[str] = None) -> str:
        """토큰 생성"""
        with self._lock:
            # 마스터 패스워드 없이 첫 토큰 생성 (서버 시작시)
            if password is None and self.server_password is None:
                self.generate_server_password()

            # 패스워드 검증 (마스터 패스워드가 설정된 후)
            if self.server_password and password != self.server_password:
                raise ValueError("Invalid password")

            token = secrets.token_urlsafe(32)
            self.tokens[token] = {
                "kernel_id": kernel_id,
                "permission": permission,
                "created_at": datetime.now(),
                "password": password or self.server_password
            }

            # 커널 메타데이터에 토큰 추가
            if kernel_id not in self.kernel_metadata:
                self.kernel_metadata[kernel_id] = Kernel(kernel_id=kernel_id)

            self.kernel_metadata[kernel_id].tokens.append({
                "token": token,
                "permission": permission,
                "created_at": datetime.now().isoformat()
            })

            return token

    def validate_token(self, token: str) -> Optional[dict]:
        """토큰 검증"""
        with self._lock:
            if token in self.blacklisted_tokens:
                return None

            return self.tokens.get(token)

    def revoke_token(self, token: str) -> bool:
        """토큰 무효화 (블랙리스트에 추가)"""
        with self._lock:
            if token in self.tokens:
                self.blacklisted_tokens.add(token)

                # 커널 메타데이터에서 토큰 제거
                token_info = self.tokens[token]
                kernel_id = token_info["kernel_id"]
                if kernel_id in self.kernel_metadata:
                    self.kernel_metadata[kernel_id].tokens = [
                        t for t in self.kernel_metadata[kernel_id].tokens
                        if t["token"] != token
                    ]

                del self.tokens[token]
                return True
            return False

    def create_kernel_from_file(self, file_path: str) -> str:
        """파일 경로로부터 커널 생성 또는 기존 커널 반환"""
        with self._lock:
            kernel_id = self._base64_file_path(file_path)

            # 기존 커널이 있으면 반환
            if kernel_id in self.kernels:
                return kernel_id

            # 새 커널 생성
            kernel = PythonKernel()
            self.kernels[kernel_id] = kernel

            # 메타데이터 초기화
            if kernel_id not in self.kernel_metadata:
                self.kernel_metadata[kernel_id] = Kernel(kernel_id=kernel_id, status="dead")

            # 노트북 초기화
            if kernel_id not in self.notebooks:
                notebook_name = os.path.basename(file_path)
                self.notebooks[kernel_id] = Notebook(name=notebook_name, kernel_id=kernel_id)

            # 히스토리 초기화
            if kernel_id not in self.histories:
                self.histories[kernel_id] = History(kernel_id=kernel_id)

            return kernel_id

    def start_kernel(self, kernel_id: str) -> bool:
        """커널 시작"""
        with self._lock:
            if kernel_id not in self.kernels:
                return False

            kernel = self.kernels[kernel_id]
            if not kernel.is_running:
                try:
                    self.kernel_metadata[kernel_id].status = "starting"
                    kernel.start()
                    self.kernel_metadata[kernel_id].status = "running"
                    return True
                except Exception as e:
                    self.kernel_metadata[kernel_id].status = "dead"
                    return False
            return True

    def connect_user(self, token: str, user_name: str, websocket=None) -> Optional[str]:
        """사용자 연결"""
        token_info = self.validate_token(token)
        if not token_info:
            return None

        with self._lock:
            user_id = str(uuid.uuid4())
            kernel_id = token_info["kernel_id"]

            user = User(
                kernel_id=kernel_id,
                user_id=user_id,
                user_name=user_name,
                permission=token_info["permission"]
            )

            self.users[user_id] = user

            if websocket:
                self.websocket_connections[user_id] = websocket

            return user_id

    def disconnect_user(self, user_id: str):
        """사용자 연결 해제"""
        with self._lock:
            if user_id in self.users:
                user = self.users[user_id]

                # 사용자가 락 걸어둔 셀들 해제
                cells_to_unlock = []
                for cell_id, lock in self._cell_locks.items():
                    if lock.user_id == user_id:
                        cells_to_unlock.append(cell_id)

                for cell_id in cells_to_unlock:
                    self.unlock_cell(cell_id, user_id)

                # 사용자 정보 삭제
                del self.users[user_id]

                if user_id in self.websocket_connections:
                    del self.websocket_connections[user_id]

    def create_cell(self, kernel_id: str, cell_type: str = "code", source: str = "", position: Optional[int] = None) -> str:
        """셀 생성"""
        with self._lock:
            cell_id = str(uuid.uuid4())

            if position is None:
                # 마지막 위치에 추가
                notebook = self.notebooks.get(kernel_id)
                position = len(notebook.cells) if notebook else 0

            cell = Cell(
                kernel_id=kernel_id,
                cell_id=cell_id,
                cell_type=cell_type,
                source=source,
                position=position
            )

            self.cells[cell_id] = cell

            # 노트북에 셀 추가
            if kernel_id in self.notebooks:
                self.notebooks[kernel_id].cells.append(cell)
                # 위치에 따라 정렬
                self.notebooks[kernel_id].cells.sort(key=lambda c: c.position)

            # 전체 노트북 파일 동기화 추가
            if self._is_auto_sync_enabled(kernel_id):
                try:
                    self._sync_notebook_to_file(kernel_id)
                except Exception as e:
                    print(f"Failed to sync notebook to file: {e}")

            return cell_id

    def update_cell(self, cell_id: str, source: str, user_id: str) -> bool:
        """셀 내용 업데이트"""
        with self._lock:
            if cell_id not in self.cells:
                return False

            cell = self.cells[cell_id]

            # 권한 체크
            user = self.users.get(user_id)
            if not user or user.permission == "viewer1":  # viewer1은 읽기 전용
                return False

            cell.source = source

            # 다른 사용자들에게 동기화 알림
            self._broadcast_cell_update(cell)

            # 파일 동기화 추가
            if self._is_auto_sync_enabled(cell.kernel_id):
                try:
                    self._sync_cell_to_file(cell_id)
                except Exception as e:
                    print(f"Failed to sync cell to file: {e}")

            return True

    def lock_cell(self, cell_id: str, user_id: str) -> bool:
        """셀에 락 걸기"""
        with self._lock:
            if cell_id not in self.cells:
                return False

            # 이미 락이 걸려있으면 실패
            if cell_id in self._cell_locks:
                return False

            user = self.users.get(user_id)
            if not user or user.permission == "viewer1":
                return False

            lock = CellLock(
                cell_id=cell_id,
                user_id=user_id,
                user_name=user.user_name,
                locked_at=datetime.now()
            )

            self._cell_locks[cell_id] = lock
            self.cells[cell_id].lock = lock

            # 다른 사용자들에게 락 정보 브로드캐스트
            self._broadcast_cell_lock(cell_id, lock)

            return True

    def unlock_cell(self, cell_id: str, user_id: str) -> bool:
        """셀 락 해제"""
        with self._lock:
            if cell_id not in self._cell_locks:
                return False

            lock = self._cell_locks[cell_id]

            # 락을 건 사용자만 해제 가능
            if lock.user_id != user_id:
                user = self.users.get(user_id)
                # admin 권한이면 강제 해제 가능
                if not user or user.permission != "admin":
                    return False

            del self._cell_locks[cell_id]
            if cell_id in self.cells:
                self.cells[cell_id].lock = None

            # 다른 사용자들에게 락 해제 알림
            self._broadcast_cell_unlock(cell_id)

            return True

    def execute_cell(self, cell_id: str, user_id: str) -> Optional[str]:
        """셀 실행"""
        with self._lock:
            if cell_id not in self.cells:
                return None

            cell = self.cells[cell_id]
            user = self.users.get(user_id)

            # 권한 체크 (viewer1은 실행 불가)
            if not user or user.permission == "viewer1":
                return None

            kernel_id = cell.kernel_id

            # 커널이 실행 중이 아니면 시작
            if not self.start_kernel(kernel_id):
                return None

            execution_id = str(uuid.uuid4())
            execution = Execution(
                cell_id=cell_id,
                kernel_id=kernel_id,
                code=cell.source,
                status="pending",
                started_at=datetime.now()
            )

            self.executions[execution_id] = execution
            cell.code_status = "pending"

            # 히스토리에 추가
            if kernel_id in self.histories:
                self.histories[kernel_id].executions.append(execution)

            # 실행 상태 브로드캐스트
            self._broadcast_execution_status(execution)

            # 백그라운드에서 실행
            threading.Thread(
                target=self._execute_cell_async,
                args=(execution_id, cell_id, kernel_id),
                daemon=True
            ).start()

            return execution_id

    def _execute_cell_async(self, execution_id: str, cell_id: str, kernel_id: str):
        """비동기 셀 실행"""
        try:
            execution = self.executions[execution_id]
            cell = self.cells[cell_id]
            kernel = self.kernels[kernel_id]

            # 실행 상태 업데이트
            execution.status = "running"
            cell.code_status = "running"
            self._broadcast_execution_status(execution)

            # 커널에서 코드 실행
            result = kernel.execute(execution.code, timeout=None)

            # 실행 완료 처리
            execution.completed_at = datetime.now()
            execution.execution_time = (execution.completed_at - execution.started_at).total_seconds()

            if result.get("status") == "ok":
                execution.status = "completed"
                cell.code_status = "completed"
                execution.outputs = [result] if result.get("data") or result.get("stdout") else []

                # 실행 카운트 증가
                if cell.execution_count is None:
                    cell.execution_count = 1
                else:
                    cell.execution_count += 1

                cell.outputs = execution.outputs

            else:
                execution.status = "failed"
                cell.code_status = "failed"
                execution.error = result.get("error")

            # 실행 결과 브로드캐스트
            self._broadcast_execution_result(execution)

        except Exception as e:
            # 실행 중 오류 발생
            execution.status = "failed"
            execution.completed_at = datetime.now()
            execution.error = {"ename": "ExecutionError", "evalue": str(e)}

            cell.code_status = "failed"

            self._broadcast_execution_result(execution)

    def interrupt_execution(self, execution_id: str, user_id: str) -> bool:
        """실행 중단"""
        with self._lock:
            if execution_id not in self.executions:
                return False

            execution = self.executions[execution_id]
            user = self.users.get(user_id)

            # 권한 체크
            if not user or user.permission == "viewer1":
                return False

            kernel = self.kernels.get(execution.kernel_id)
            if kernel:
                try:
                    kernel.interrupt()
                    execution.status = "interrupted"
                    execution.completed_at = datetime.now()

                    # 셀 상태 업데이트
                    cell = self.cells.get(execution.cell_id)
                    if cell:
                        cell.code_status = "interrupted"

                    self._broadcast_execution_result(execution)
                    return True
                except Exception:
                    return False

            return False

    def update_user_focus(self, user_id: str, cell_id: Optional[str]):
        """사용자 포커스 업데이트"""
        with self._lock:
            if user_id in self.users:
                self.users[user_id].focused_cell_id = cell_id
                # 다른 사용자들에게 포커스 변경 알림
                self._broadcast_user_focus(user_id, cell_id)

    def get_notebook_data(self, kernel_id: str) -> Optional[dict]:
        """노트북 데이터 조회"""
        with self._lock:
            if kernel_id not in self.notebooks:
                return None

            notebook = self.notebooks[kernel_id]
            return {
                "notebook": notebook.to_dict(),
                "history": self.histories[kernel_id].to_dict() if kernel_id in self.histories else None,
                "users": [user.to_dict() for user in self.users.values() if user.kernel_id == kernel_id]
            }

    def get_cell_data(self, cell_id: str) -> Optional[dict]:
        """셀 데이터 조회"""
        with self._lock:
            if cell_id not in self.cells:
                return None
            return self.cells[cell_id].to_dict()

    def get_history_data(self, kernel_id: str) -> Optional[dict]:
        """히스토리 데이터 조회"""
        with self._lock:
            if kernel_id not in self.histories:
                return None
            return self.histories[kernel_id].to_dict()

    def _broadcast_cell_update(self, cell: Cell):
        """셀 업데이트 브로드캐스트"""
        message = {
            "type": "cell_update",
            "data": cell.to_dict()
        }
        self._broadcast_to_kernel_users(cell.kernel_id, message)

    def _broadcast_cell_lock(self, cell_id: str, lock: CellLock):
        """셀 락 브로드캐스트"""
        cell = self.cells.get(cell_id)
        if cell:
            message = {
                "type": "cell_lock",
                "data": {
                    "cell_id": cell_id,
                    "lock": lock.to_dict()
                }
            }
            self._broadcast_to_kernel_users(cell.kernel_id, message)

    def _broadcast_cell_unlock(self, cell_id: str):
        """셀 락 해제 브로드캐스트"""
        cell = self.cells.get(cell_id)
        if cell:
            message = {
                "type": "cell_unlock",
                "data": {"cell_id": cell_id}
            }
            self._broadcast_to_kernel_users(cell.kernel_id, message)

    def _broadcast_execution_status(self, execution: Execution):
        """실행 상태 브로드캐스트"""
        message = {
            "type": "execution_status",
            "data": execution.to_dict()
        }
        self._broadcast_to_kernel_users(execution.kernel_id, message)

    def _broadcast_execution_result(self, execution: Execution):
        """실행 결과 브로드캐스트"""
        message = {
            "type": "execution_result",
            "data": execution.to_dict()
        }
        self._broadcast_to_kernel_users(execution.kernel_id, message)

    def _broadcast_user_focus(self, user_id: str, cell_id: Optional[str]):
        """사용자 포커스 브로드캐스트"""
        user = self.users.get(user_id)
        if user:
            message = {
                "type": "user_focus",
                "data": {
                    "user_id": user_id,
                    "user_name": user.user_name,
                    "cell_id": cell_id
                }
            }
            self._broadcast_to_kernel_users(user.kernel_id, message, exclude_user=user_id)

    def _broadcast_to_kernel_users(self, kernel_id: str, message: dict, exclude_user: Optional[str] = None):
        """특정 커널의 모든 사용자에게 메시지 브로드캐스트"""
        for user_id, user in self.users.items():
            if user.kernel_id == kernel_id and user_id != exclude_user:
                websocket = self.websocket_connections.get(user_id)
                if websocket:
                    try:
                        # 실제 웹소켓 구현에서는 여기서 메시지 전송
                        # websocket.send(json.dumps(message))
                        pass
                    except Exception as e:
                        print(f"Failed to send message to user {user_id}: {e}")

    def load_notebook_from_file(self, file_path: str) -> str:
        """파일에서 노트북 로드 (.py 또는 .ipynb)"""
        kernel_id = self.create_kernel_from_file(file_path)

        if file_path.endswith('.py'):
            self._load_python_file(file_path, kernel_id)
        elif file_path.endswith('.ipynb'):
            self._load_jupyter_notebook(file_path, kernel_id)

        return kernel_id

    def _load_python_file(self, file_path: str, kernel_id: str):
        """Python 파일 로드 (#%% 기준으로 셀 분할)"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 파일 경로 저장 추가
            if kernel_id in self.kernel_metadata:
                self.kernel_metadata[kernel_id].file_path = os.path.abspath(file_path)

            # #%% 기준으로 셀 분할
            cells_content = content.split('#%%')

            # 첫 번째 셀이 비어있으면 제거
            if cells_content and not cells_content[0].strip():
                cells_content.pop(0)

            for i, cell_content in enumerate(cells_content):
                cell_content = cell_content.strip()
                if cell_content:
                    self.create_cell(kernel_id, "code", cell_content, i)

        except Exception as e:
            print(f"Failed to load Python file {file_path}: {e}")


    def _load_jupyter_notebook(self, file_path: str, kernel_id: str):
        """Jupyter 노트북 파일 로드"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                notebook_data = json.load(f)

            # 파일 경로 저장 추가
            if kernel_id in self.kernel_metadata:
                self.kernel_metadata[kernel_id].file_path = os.path.abspath(file_path)

            for i, cell_data in enumerate(notebook_data.get('cells', [])):
                cell_type = cell_data.get('cell_type', 'code')
                source = ''.join(cell_data.get('source', []))

                self.create_cell(kernel_id, cell_type, source, i)

        except Exception as e:
            print(f"Failed to load Jupyter notebook {file_path}: {e}")
    def save_notebook_to_file(self, kernel_id: str, file_path: str) -> bool:
        """노트북을 파일로 저장"""
        try:
            if file_path.endswith('.py'):
                return self._save_as_python_file(kernel_id, file_path)
            elif file_path.endswith('.ipynb'):
                return self._save_as_jupyter_notebook(kernel_id, file_path)
            return False
        except Exception as e:
            print(f"Failed to save notebook to {file_path}: {e}")
            return False

    def _save_as_python_file(self, kernel_id: str, file_path: str) -> bool:
        """Python 파일로 저장"""
        notebook = self.notebooks.get(kernel_id)
        if not notebook:
            return False

        content_parts = []
        for cell in sorted(notebook.cells, key=lambda c: c.position):
            if cell.cell_type == "code" and cell.source.strip():
                content_parts.append(f"#%%\n{cell.source}")

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('\n\n'.join(content_parts))

        return True

    def _save_as_jupyter_notebook(self, kernel_id: str, file_path: str) -> bool:
        """Jupyter 노트북으로 저장"""
        notebook = self.notebooks.get(kernel_id)
        if not notebook:
            return False

        jupyter_data = {
            "cells": [],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }

        for cell in sorted(notebook.cells, key=lambda c: c.position):
            cell_data = {
                "cell_type": cell.cell_type,
                "source": cell.source.splitlines(True),
                "metadata": {}
            }

            if cell.cell_type == "code":
                cell_data["execution_count"] = cell.execution_count
                cell_data["outputs"] = cell.outputs or []

            jupyter_data["cells"].append(cell_data)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(jupyter_data, f, indent=2, ensure_ascii=False)

        return True

    def shutdown_kernel(self, kernel_id: str) -> bool:
        """커널 종료"""
        with self._lock:
            if kernel_id not in self.kernels:
                return False

            kernel = self.kernels[kernel_id]
            try:
                kernel.stop()
                self.kernel_metadata[kernel_id].status = "dead"

                # 해당 커널의 모든 사용자 연결 해제
                users_to_disconnect = [
                    user_id for user_id, user in self.users.items()
                    if user.kernel_id == kernel_id
                ]

                for user_id in users_to_disconnect:
                    self.disconnect_user(user_id)

                return True
            except Exception as e:
                print(f"Failed to shutdown kernel {kernel_id}: {e}")
                return False

    def restart_kernel(self, kernel_id: str) -> bool:
        """커널 재시작"""
        with self._lock:
            if kernel_id not in self.kernels:
                return False

            # 기존 커널 종료
            kernel = self.kernels[kernel_id]
            try:
                kernel.stop()
            except Exception:
                pass

            # 새 커널 인스턴스 생성
            new_kernel = PythonKernel()
            self.kernels[kernel_id] = new_kernel

            # 실행 히스토리 초기화
            if kernel_id in self.histories:
                self.histories[kernel_id].executions = []

            # 모든 셀의 실행 카운트와 출력 초기화
            notebook = self.notebooks.get(kernel_id)
            if notebook:
                for cell in notebook.cells:
                    cell.execution_count = None
                    cell.outputs = []
                    cell.code_status = None

            # 커널 시작
            return self.start_kernel(kernel_id)

    def get_kernel_status(self, kernel_id: str) -> Optional[str]:
        """커널 상태 조회"""
        with self._lock:
            if kernel_id not in self.kernel_metadata:
                return None
            return self.kernel_metadata[kernel_id].status

    def get_connected_users(self, kernel_id: str) -> List[dict]:
        """연결된 사용자 목록 조회"""
        with self._lock:
            return [
                user.to_dict() for user in self.users.values()
                if user.kernel_id == kernel_id
            ]

    def get_server_info(self) -> dict:
        """서버 정보 조회"""
        with self._lock:
            total_kernels = len(self.kernels)
            running_kernels = sum(
                1 for kernel in self.kernels.values()
                if kernel.is_running
            )
            total_users = len(self.users)
            total_cells = len(self.cells)

            return {
                "server_password": self.server_password,
                "total_kernels": total_kernels,
                "running_kernels": running_kernels,
                "total_users": total_users,
                "total_cells": total_cells,
                "total_tokens": len(self.tokens),
                "blacklisted_tokens": len(self.blacklisted_tokens),
                "kernels": [
                    {
                        "kernel_id": kernel_id,
                        "status": meta.status,
                        "user_count": len([u for u in self.users.values() if u.kernel_id == kernel_id]),
                        "cell_count": len([c for c in self.cells.values() if c.kernel_id == kernel_id])
                    }
                    for kernel_id, meta in self.kernel_metadata.items()
                ]
            }

    def cleanup_dead_kernels(self) -> int:
        """죽은 커널들 정리"""
        cleaned_count = 0
        with self._lock:
            dead_kernels = []

            for kernel_id, kernel in self.kernels.items():
                try:
                    if kernel.process and kernel.process.poll() is not None:
                        dead_kernels.append(kernel_id)
                except Exception:
                    dead_kernels.append(kernel_id)

            for kernel_id in dead_kernels:
                try:
                    self.shutdown_kernel(kernel_id)
                    cleaned_count += 1
                except Exception as e:
                    print(f"Failed to cleanup kernel {kernel_id}: {e}")

        return cleaned_count

    def enable_auto_cleanup(self, check_interval: float = 300):
        """자동 정리 활성화 (5분마다 체크)"""
        self._auto_cleanup_enabled = True
        self._schedule_cleanup(check_interval)

    def disable_auto_cleanup(self):
        """자동 정리 비활성화"""
        self._auto_cleanup_enabled = False
        if self._cleanup_timer:
            self._cleanup_timer.cancel()
            self._cleanup_timer = None

    def _schedule_cleanup(self, check_interval: float):
        """정리 스케줄링"""
        if not self._auto_cleanup_enabled:
            return

        def cleanup_task():
            try:
                cleaned = self.cleanup_dead_kernels()
                if cleaned > 0:
                    print(f"자동 정리: {cleaned}개 죽은 커널 제거됨")
            except Exception as e:
                print(f"자동 정리 중 오류: {e}")
            finally:
                if self._auto_cleanup_enabled:
                    self._schedule_cleanup(check_interval)

        self._cleanup_timer = threading.Timer(check_interval, cleanup_task)
        self._cleanup_timer.daemon = True
        self._cleanup_timer.start()

    def export_notebook_data(self, kernel_id: str) -> Optional[dict]:
        """노트북 데이터 내보내기"""
        with self._lock:
            if kernel_id not in self.notebooks:
                return None

            return {
                "timestamp": datetime.now().isoformat(),
                "kernel_id": kernel_id,
                "notebook": self.notebooks[kernel_id].to_dict(),
                "history": self.histories[kernel_id].to_dict() if kernel_id in self.histories else None,
                "metadata": self.kernel_metadata[kernel_id].to_dict() if kernel_id in self.kernel_metadata else None
            }
    def _is_auto_sync_enabled(self, kernel_id: str) -> bool:
        """자동 동기화가 활성화되어 있는지 확인"""
        metadata = self.kernel_metadata.get(kernel_id)
        return metadata and metadata.auto_sync_enabled and metadata.file_path is not None

    def _get_kernel_file_path(self, kernel_id: str) -> Optional[str]:
        """커널의 원본 파일 경로 조회"""
        metadata = self.kernel_metadata.get(kernel_id)
        return metadata.file_path if metadata else None

    def _sync_cell_to_file(self, cell_id: str) -> bool:
        """특정 셀의 내용을 원본 파일에 반영"""
        try:
            cell = self.cells.get(cell_id)
            if not cell:
                return False

            return self._sync_notebook_to_file(cell.kernel_id)
        except Exception as e:
            print(f"Error syncing cell to file: {e}")
            return False

    def _sync_notebook_to_file(self, kernel_id: str) -> bool:
        """전체 노트북을 원본 파일에 동기화"""
        try:
            file_path = self._get_kernel_file_path(kernel_id)
            if not file_path:
                return False

            notebook = self.notebooks.get(kernel_id)
            if not notebook:
                return False

            # 파일 확장자에 따라 다른 형식으로 저장
            if file_path.endswith('.py'):
                success = self._sync_to_python_file(kernel_id, file_path)
            elif file_path.endswith('.ipynb'):
                success = self._sync_to_jupyter_file(kernel_id, file_path)
            else:
                return False

            if success:
                # 동기화 시간 업데이트
                self.kernel_metadata[kernel_id].last_sync_time = datetime.now()

            return success

        except Exception as e:
            print(f"Error syncing notebook to file: {e}")
            return False

    def _sync_to_python_file(self, kernel_id: str, file_path: str) -> bool:
        """Python 파일로 동기화"""
        try:
            notebook = self.notebooks.get(kernel_id)
            if not notebook:
                return False

            content_parts = []
            for cell in sorted(notebook.cells, key=lambda c: c.position):
                if cell.cell_type == "code" and cell.source.strip():
                    content_parts.append(f"#%%\n{cell.source}")

            # 백업 파일 생성
            backup_path = f"{file_path}.backup"
            if os.path.exists(file_path):
                import shutil
                shutil.copy2(file_path, backup_path)

            # 파일 쓰기
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n\n'.join(content_parts))

            return True

        except Exception as e:
            print(f"Error syncing to Python file: {e}")
            # 백업에서 복원 시도
            backup_path = f"{file_path}.backup"
            if os.path.exists(backup_path):
                try:
                    import shutil
                    shutil.copy2(backup_path, file_path)
                except Exception:
                    pass
            return False

    def _sync_to_jupyter_file(self, kernel_id: str, file_path: str) -> bool:
        """Jupyter 노트북 파일로 동기화"""
        try:
            notebook = self.notebooks.get(kernel_id)
            if not notebook:
                return False

            jupyter_data = {
                "cells": [],
                "metadata": {
                    "kernelspec": {
                        "display_name": "Python 3",
                        "language": "python",
                        "name": "python3"
                    }
                },
                "nbformat": 4,
                "nbformat_minor": 4
            }

            for cell in sorted(notebook.cells, key=lambda c: c.position):
                cell_data = {
                    "cell_type": cell.cell_type,
                    "source": cell.source.splitlines(True),
                    "metadata": {}
                }

                if cell.cell_type == "code":
                    cell_data["execution_count"] = cell.execution_count
                    cell_data["outputs"] = cell.outputs or []

                jupyter_data["cells"].append(cell_data)

            # 백업 파일 생성
            backup_path = f"{file_path}.backup"
            if os.path.exists(file_path):
                import shutil
                shutil.copy2(file_path, backup_path)

            # 파일 쓰기
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(jupyter_data, f, indent=2, ensure_ascii=False)

            return True

        except Exception as e:
            print(f"Error syncing to Jupyter file: {e}")
            # 백업에서 복원 시도
            backup_path = f"{file_path}.backup"
            if os.path.exists(backup_path):
                try:
                    import shutil
                    shutil.copy2(backup_path, file_path)
                except Exception:
                    pass
            return False

    def enable_auto_file_sync(self, kernel_id: str, enabled: bool = True) -> bool:
        """특정 커널의 자동 파일 동기화 활성화/비활성화"""
        with self._lock:
            if kernel_id not in self.kernel_metadata:
                return False

            self.kernel_metadata[kernel_id].auto_sync_enabled = enabled
            return True

    def get_sync_status(self, kernel_id: str) -> Optional[Dict[str, Any]]:
        """동기화 상태 조회"""
        with self._lock:
            metadata = self.kernel_metadata.get(kernel_id)
            if not metadata:
                return None

            return {
                "kernel_id": kernel_id,
                "file_path": metadata.file_path,
                "auto_sync_enabled": metadata.auto_sync_enabled,
                "last_sync_time": metadata.last_sync_time.isoformat() if metadata.last_sync_time else None,
                "file_exists": os.path.exists(metadata.file_path) if metadata.file_path else False,
                "is_sync_available": metadata.file_path is not None
            }

    def manual_sync_to_file(self, kernel_id: str) -> Dict[str, Any]:
        """수동으로 파일 동기화 실행"""
        try:
            if kernel_id not in self.kernel_metadata:
                return {"success": False, "error": "Kernel not found"}

            metadata = self.kernel_metadata[kernel_id]
            if not metadata.file_path:
                return {"success": False, "error": "No file path associated with kernel"}

            success = self._sync_notebook_to_file(kernel_id)

            return {
                "success": success,
                "kernel_id": kernel_id,
                "file_path": metadata.file_path,
                "sync_time": datetime.now().isoformat(),
                "error": None if success else "Failed to sync to file"
            }

        except Exception as e:
            return {
                "success": False,
                "kernel_id": kernel_id,
                "error": str(e),
                "sync_time": datetime.now().isoformat()
            }
    def import_notebook_data(self, data: dict, file_path: str) -> Optional[str]:
        """노트북 데이터 가져오기"""
        try:
            kernel_id = self.create_kernel_from_file(file_path)

            # 노트북 데이터 복원
            notebook_data = data.get("notebook", {})
            if notebook_data:
                notebook = Notebook(
                    name=notebook_data.get("name", ""),
                    kernel_id=kernel_id
                )

                # 셀들 복원
                for cell_data in notebook_data.get("cells", []):
                    cell = Cell(
                        kernel_id=kernel_id,
                        cell_id=str(uuid.uuid4()),  # 새 ID 생성
                        cell_type=cell_data.get("cell_type", "code"),
                        source=cell_data.get("source", ""),
                        execution_count=cell_data.get("execution_count"),
                        outputs=cell_data.get("outputs", []),
                        position=cell_data.get("position", 0),
                        code_status=cell_data.get("code_status")
                    )

                    self.cells[cell.cell_id] = cell
                    notebook.cells.append(cell)

                self.notebooks[kernel_id] = notebook

            # 히스토리 데이터 복원
            history_data = data.get("history", {})
            if history_data:
                history = History(kernel_id=kernel_id)

                for exec_data in history_data.get("executions", []):
                    execution = Execution(
                        cell_id=exec_data.get("cell_id", ""),
                        kernel_id=kernel_id,
                        code=exec_data.get("code", ""),
                        status=exec_data.get("status", "completed"),
                        started_at=datetime.fromisoformat(exec_data.get("started_at", datetime.now().isoformat())),
                        completed_at=datetime.fromisoformat(exec_data["completed_at"]) if exec_data.get("completed_at") else None,
                        execution_time=exec_data.get("execution_time"),
                        outputs=exec_data.get("outputs", []),
                        error=exec_data.get("error")
                    )
                    history.executions.append(execution)

                self.histories[kernel_id] = history

            return kernel_id

        except Exception as e:
            print(f"Failed to import notebook data: {e}")
            return None

    def create_share_link(self, kernel_id: str, permission: str = "viewer1") -> Optional[str]:
        """공유 링크 생성"""
        if kernel_id not in self.kernel_metadata:
            return None

        try:
            token = self.create_token(kernel_id, permission, self.server_password)
            return f"/notebook?token={token}"
        except Exception as e:
            print(f"Failed to create share link: {e}")
            return None
    def list_shared_kernels(self) -> List[str]:
        """공유 커널 목록 반환"""
        with self._lock:
            return [kernel_id for kernel_id, metadata in self.kernel_metadata.items()
                    if metadata.tokens]  # 토큰이 있는 커널들

    def get_kernel_info(self, kernel_id: str) -> Optional[dict]:
        """커널 상세 정보 조회"""
        with self._lock:
            if kernel_id not in self.kernels:
                return None

            kernel = self.kernels[kernel_id]
            metadata = self.kernel_metadata.get(kernel_id)

            # 기본 커널 정보
            kernel_info = {
                "kernel_id": kernel_id,
                "is_running": kernel.is_running if kernel else False,
                "execution_count": kernel.execution_count if kernel else 0,
                "process_id": kernel.process.pid if kernel and kernel.process else None,
            }

            # 메타데이터 정보 추가
            if metadata:
                kernel_info.update({
                    "status": metadata.status,
                    "language": metadata.language,
                    "tokens": metadata.tokens.copy(),
                    "token_count": len(metadata.tokens)
                })

            # 노트북 정보
            notebook = self.notebooks.get(kernel_id)
            if notebook:
                kernel_info.update({
                    "notebook_name": notebook.name,
                    "cell_count": len(notebook.cells),
                    "cells": [
                        {
                            "cell_id": cell.cell_id,
                            "cell_type": cell.cell_type,
                            "position": cell.position,
                            "execution_count": cell.execution_count,
                            "code_status": cell.code_status,
                            "has_lock": cell.lock is not None,
                            "locked_by": cell.lock.user_name if cell.lock else None
                        }
                        for cell in sorted(notebook.cells, key=lambda c: c.position)
                    ]
                })

            # 연결된 사용자 정보
            connected_users = [
                {
                    "user_id": user.user_id,
                    "user_name": user.user_name,
                    "permission": user.permission,
                    "focused_cell_id": user.focused_cell_id
                }
                for user in self.users.values()
                if user.kernel_id == kernel_id
            ]
            kernel_info["connected_users"] = connected_users
            kernel_info["user_count"] = len(connected_users)

            # 실행 히스토리 요약
            history = self.histories.get(kernel_id)
            if history:
                executions = history.executions
                completed_executions = [e for e in executions if e.status == "completed"]
                failed_executions = [e for e in executions if e.status == "failed"]

                kernel_info.update({
                    "total_executions": len(executions),
                    "completed_executions": len(completed_executions),
                    "failed_executions": len(failed_executions),
                    "success_rate": len(completed_executions) / len(executions) if executions else 0,
                    "last_execution": executions[-1].to_dict() if executions else None
                })

            # 커널 상태 세부 정보 (실제 커널이 존재하는 경우)
            if kernel:
                try:
                    if hasattr(kernel, 'get_status'):
                        detailed_status = kernel.get_status()
                        kernel_info.update({
                            "queue_size": detailed_status.get("queue_size", 0),
                            "uptime": detailed_status.get("uptime"),
                            "successful_executions": detailed_status.get("successful_executions", 0),
                            "failed_executions_kernel": detailed_status.get("failed_executions", 0),
                            "temp_dir": detailed_status.get("temp_dir")
                        })

                    # 건강 상태 체크
                    if hasattr(kernel, 'is_healthy'):
                        kernel_info["is_healthy"] = kernel.is_healthy()

                    # 메모리 사용량
                    if hasattr(kernel, 'get_memory_usage'):
                        memory_usage = kernel.get_memory_usage()
                        if memory_usage:
                            kernel_info["memory_usage"] = memory_usage

                except Exception as e:
                    kernel_info["status_error"] = str(e)

            return kernel_info

    def create_kernel(self, kernel_id: Optional[str] = None) -> str:
        """일반 커널 생성 (파일 경로 없이)"""
        with self._lock:
            if kernel_id is None:
                kernel_id = f"kernel_{str(uuid.uuid4())[:8]}"

            if kernel_id in self.kernels:
                raise ValueError(f"커널 ID '{kernel_id}'가 이미 존재합니다.")

            # 새 커널 인스턴스 생성
            kernel = PythonKernel(kernel_id=kernel_id)
            self.kernels[kernel_id] = kernel

            # 메타데이터 초기화
            self.kernel_metadata[kernel_id] = Kernel(
                kernel_id=kernel_id,
                status="dead"
            )

            # 노트북 초기화 (일반 커널용)
            self.notebooks[kernel_id] = Notebook(
                name=f"Notebook_{kernel_id[:8]}",
                kernel_id=kernel_id
            )

            # 히스토리 초기화
            self.histories[kernel_id] = History(kernel_id=kernel_id)

            return kernel_id

    def execute_on_kernel(self, kernel_id: str, code: str, timeout: Optional[float] = None,
                          cell_id: Optional[str] = None, user_id: Optional[str] = None) -> Dict[str, Any]:
        """커널에서 직접 코드 실행"""
        with self._lock:
            if kernel_id not in self.kernels:
                return {
                    "status": "error",
                    "error": f"커널을 찾을 수 없음: {kernel_id}",
                    "kernel_id": kernel_id
                }

            kernel = self.kernels[kernel_id]

            # 커널이 실행 중이 아니면 시작
            if not kernel.is_running:
                try:
                    self.kernel_metadata[kernel_id].status = "starting"
                    kernel.start()
                    self.kernel_metadata[kernel_id].status = "running"
                except Exception as e:
                    self.kernel_metadata[kernel_id].status = "dead"
                    return {
                        "status": "error",
                        "error": f"커널 시작 실패: {str(e)}",
                        "kernel_id": kernel_id
                    }

        # 락 해제 후 실행 (실행 중에는 다른 작업 가능)
        try:
            # 커널에서 직접 코드 실행
            result = kernel.execute(code, timeout, cell_id, user_id)

            # 실행 정보를 히스토리에 추가
            if kernel_id in self.histories:
                execution = Execution(
                    cell_id=cell_id or "direct_execution",
                    kernel_id=kernel_id,
                    code=code,
                    status="completed" if result.get("status") == "ok" else "failed",
                    started_at=datetime.now(),
                    completed_at=datetime.now(),
                    execution_time=0.0,  # 실제로는 커널에서 측정
                    outputs=[result] if result.get("data") or result.get("stdout") else [],
                    error=result.get("error") if result.get("status") == "error" else None
                )
                self.histories[kernel_id].executions.append(execution)

            # 결과에 커널 정보 추가
            result["kernel_id"] = kernel_id
            result["executed_at"] = datetime.now().isoformat()

            return result

        except Exception as e:
            error_result = {
                "status": "error",
                "error": f"실행 중 오류: {str(e)}",
                "kernel_id": kernel_id,
                "executed_at": datetime.now().isoformat()
            }

            # 에러도 히스토리에 기록
            if kernel_id in self.histories:
                execution = Execution(
                    cell_id=cell_id or "direct_execution",
                    kernel_id=kernel_id,
                    code=code,
                    status="failed",
                    started_at=datetime.now(),
                    completed_at=datetime.now(),
                    execution_time=0.0,
                    outputs=[],
                    error={"ename": "ExecutionError", "evalue": str(e)}
                )
                self.histories[kernel_id].executions.append(execution)

            return error_result

    def get_kernel_by_id(self, kernel_id: str) -> Optional[PythonKernel]:
        """커널 ID로 커널 인스턴스 직접 조회"""
        with self._lock:
            return self.kernels.get(kernel_id)

    def list_all_kernels(self) -> List[str]:
        """모든 커널 목록 반환 (공유/비공유 구분 없이)"""
        with self._lock:
            return list(self.kernels.keys())

    def get_kernel_statistics(self, kernel_id: str) -> Optional[Dict[str, Any]]:
        """특정 커널의 통계 정보"""
        with self._lock:
            if kernel_id not in self.kernels:
                return None

            kernel = self.kernels[kernel_id]
            history = self.histories.get(kernel_id)
            notebook = self.notebooks.get(kernel_id)

            stats = {
                "kernel_id": kernel_id,
                "is_running": kernel.is_running if kernel else False,
                "execution_count": kernel.execution_count if kernel else 0,
            }

            if history:
                executions = history.executions
                stats.update({
                    "total_executions": len(executions),
                    "successful_executions": len([e for e in executions if e.status == "completed"]),
                    "failed_executions": len([e for e in executions if e.status == "failed"]),
                    "interrupted_executions": len([e for e in executions if e.status == "interrupted"]),
                    "average_execution_time": sum(e.execution_time or 0 for e in executions) / len(executions) if executions else 0
                })

            if notebook:
                stats.update({
                    "total_cells": len(notebook.cells),
                    "code_cells": len([c for c in notebook.cells if c.cell_type == "code"]),
                    "markdown_cells": len([c for c in notebook.cells if c.cell_type == "markdown"]),
                    "locked_cells": len([c for c in notebook.cells if c.lock is not None])
                })

            # 연결된 사용자 통계
            connected_users = [u for u in self.users.values() if u.kernel_id == kernel_id]
            stats.update({
                "connected_users": len(connected_users),
                "admin_users": len([u for u in connected_users if u.permission == "admin"]),
                "viewer_users": len([u for u in connected_users if u.permission.startswith("viewer")])
            })

            return stats

    def is_kernel_shared(self, kernel_id: str) -> bool:
        """커널이 공유 커널인지 확인"""
        with self._lock:
            metadata = self.kernel_metadata.get(kernel_id)
            return bool(metadata and metadata.tokens) if metadata else False

    def get_kernel_tokens(self, kernel_id: str) -> List[dict]:
        """커널의 모든 토큰 정보 조회"""
        with self._lock:
            metadata = self.kernel_metadata.get(kernel_id)
            if not metadata:
                return []

            # 토큰 세부 정보와 함께 반환
            detailed_tokens = []
            for token_info in metadata.tokens:
                token = token_info["token"]
                if token in self.tokens:
                    full_token_info = self.tokens[token].copy()
                    full_token_info.update(token_info)
                    full_token_info["is_blacklisted"] = token in self.blacklisted_tokens
                    detailed_tokens.append(full_token_info)

            return detailed_tokens

    def get_active_executions(self, kernel_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """활성 실행 목록 조회"""
        with self._lock:
            active_executions = []

            for execution_id, execution in self.executions.items():
                if kernel_id is None or execution.kernel_id == kernel_id:
                    if execution.status in ["pending", "running"]:
                        active_executions.append({
                            "execution_id": execution_id,
                            "kernel_id": execution.kernel_id,
                            "cell_id": execution.cell_id,
                            "status": execution.status,
                            "started_at": execution.started_at.isoformat(),
                            "code_preview": execution.code[:100] + "..." if len(execution.code) > 100 else execution.code
                        })

            return active_executions

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료시 모든 커널 정리"""
        with self._lock:
            # 자동 정리 비활성화
            self.disable_auto_cleanup()

            # 모든 커널 종료
            for kernel_id in list(self.kernels.keys()):
                try:
                    self.shutdown_kernel(kernel_id)
                except Exception as e:
                    print(f"Failed to shutdown kernel {kernel_id}: {e}")

            # 모든 데이터 정리
            self.kernels.clear()
            self.kernel_metadata.clear()
            self.notebooks.clear()
            self.histories.clear()
            self.users.clear()
            self.cells.clear()
            self.executions.clear()
            self.tokens.clear()
            self.blacklisted_tokens.clear()
            self.websocket_connections.clear()
            self._cell_locks.clear()

    def __len__(self):
        return len(self.kernels)

    def __contains__(self, kernel_id: str):
        return kernel_id in self.kernels

    def __iter__(self):
        return iter(self.kernels.keys())

    def __repr__(self):
        return f"KernelManager({len(self.kernels)} kernels, {len(self.users)} users)"

    def __str__(self):
        running = sum(1 for k in self.kernels.values() if k.is_running)
        shared = len([k for k in self.kernel_metadata.values() if k.tokens])
        return f"CollaborativeKernelManager: {len(self.kernels)} kernels ({shared} shared), {running} running, {len(self.users)} users"

