"""
DarkPyonix - Python Kernel System
주피터 커널과 유사한 기능을 제공하는 파이썬 커널 시스템
"""

# 주요 클래스들을 최상위에서 바로 사용할 수 있도록 import
from .kernel import PythonKernel
from .manager import KernelManager

__version__ = "1.0.0"
__author__ = "DarkPyonix"
__description__ = "Python Kernel System similar to Jupyter"

__all__ = [
    "PythonKernel",
    "KernelManager"
]

# 편의 함수들
def create_kernel():
    """빠른 커널 생성"""
    return PythonKernel()

def create_manager():
    """빠른 매니저 생성"""
    return KernelManager()

def quick_execute(code: str):
    """빠른 코드 실행 (일회성)"""
    with PythonKernel() as kernel:
        return kernel.execute(code)

# 사용 예제 출력 함수
def print_examples():
    """사용 예제 출력"""
    examples = """
DarkPyonix 사용 예제:

1. 단일 커널 사용:
   import darkpyonix
   kernel = darkpyonix.create_kernel()
   kernel.start()
   result = kernel.execute("2 + 3")
   kernel.stop()

2. 컨텍스트 매니저 사용:
   with darkpyonix.PythonKernel() as kernel:
       result = kernel.execute("print('Hello')")

3. 매니저를 통한 다중 커널:
   manager = darkpyonix.create_manager()
   kernel_id = manager.create_kernel()
   result = manager.execute_on_kernel(kernel_id, "x = 10")

4. 빠른 실행:
   result = darkpyonix.quick_execute("2 ** 10")

5. CLI 사용:
   python -m manager -i  # 대화형 모드
   python -m kernel.test  # 테스트 실행
"""
    print(examples)

