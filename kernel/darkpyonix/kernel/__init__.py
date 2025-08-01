"""
Python Kernel Package
주피터 커널과 유사한 기능을 제공하는 파이썬 커널 패키지
"""

from .kernel import PythonKernel

__version__ = "1.0.0"
__all__ = ["PythonKernel"]

# 편의 함수
def create_kernel():
    """새 커널 인스턴스 생성"""
    return PythonKernel()

def quick_execute(code: str, timeout=None):
    """빠른 코드 실행 (일회성 커널 사용)"""
    with PythonKernel() as kernel:
        return kernel.execute(code, timeout)