"""
Kernel Manager Package
커널 관리자 패키지
"""

from .manager import KernelManager

__version__ = "1.0.0"
__all__ = ["KernelManager"]

# 편의 함수
def create_manager():
    """새 커널 매니저 인스턴스 생성"""
    return KernelManager()

def quick_manager_execute(code: str, kernel_id: str = None):
    """빠른 매니저 실행 (일회성 매니저 사용)"""
    with KernelManager() as manager:
        if kernel_id is None:
            kernel_id = manager.create_kernel()

        kernel = manager.get_kernel(kernel_id)
        if kernel:
            kernel.start()
            return kernel.execute(code)
        else:
            return {"status": "error", "error": "커널을 찾을 수 없습니다."}