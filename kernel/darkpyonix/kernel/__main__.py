"""
Kernel Process Main Module
커널 프로세스에서 실행되는 메인 모듈
서브프로세스로 실행될 때 사용됩니다.
"""

import sys
import json
import traceback
import io
from contextlib import redirect_stdout, redirect_stderr


# 전역 네임스페이스
global_namespace = {}


def execute_code(code, execution_count, request_id):
    """코드 실행 함수"""
    result = {
        "request_id": request_id,
        "type": "execute_result"
    }

    # 출력 캡처를 위한 StringIO 객체
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    try:
        # 출력 리다이렉션과 함께 코드 실행
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            # compile과 exec를 사용하여 코드 실행
            compiled_code = compile(code, f"<execution_{execution_count}>", "exec")
            exec(compiled_code, global_namespace)

        # 결과 수집
        stdout_text = stdout_capture.getvalue()
        stderr_text = stderr_capture.getvalue()

        result["status"] = "ok"
        if stdout_text:
            result["stdout"] = stdout_text
        if stderr_text:
            result["stderr"] = stderr_text

        # 마지막 표현식의 결과 확인
        if code.strip():
            try:
                # 마지막 줄이 표현식인지 확인
                lines = code.strip().split('\n')
                last_line = lines[-1].strip()
                if last_line and not last_line.startswith(('import ', 'from ', 'def ', 'class ', 'if ', 'for ', 'while ', 'try:', 'with ', '@')):
                    # 표현식 평가 시도
                    expr_result = eval(last_line, global_namespace)
                    if expr_result is not None:
                        result["data"] = {"text/plain": repr(expr_result)}
            except:
                pass  # 표현식이 아니거나 평가할 수 없음

    except Exception as e:
        result["status"] = "error"
        result["error"] = {
            "ename": type(e).__name__,
            "evalue": str(e),
            "traceback": traceback.format_exc().split('\n')
        }

        stderr_text = stderr_capture.getvalue()
        if stderr_text:
            result["stderr"] = stderr_text

    return result


def get_kernel_code():
    """커널 프로세스에서 실행될 코드 반환 (문자열 형태)"""
    return '''
import sys
import json
import traceback
import io
from contextlib import redirect_stdout, redirect_stderr

# 전역 네임스페이스
global_namespace = {}

def execute_code(code, execution_count, request_id):
    """코드 실행 함수"""
    result = {
        "request_id": request_id,
        "type": "execute_result"
    }
    
    # 출력 캡처를 위한 StringIO 객체
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    
    try:
        # 출력 리다이렉션과 함께 코드 실행
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            # compile과 exec를 사용하여 코드 실행
            compiled_code = compile(code, f"<execution_{execution_count}>", "exec")
            exec(compiled_code, global_namespace)
        
        # 결과 수집
        stdout_text = stdout_capture.getvalue()
        stderr_text = stderr_capture.getvalue()
        
        result["status"] = "ok"
        if stdout_text:
            result["stdout"] = stdout_text
        if stderr_text:
            result["stderr"] = stderr_text
            
        # 마지막 표현식의 결과 확인
        if code.strip():
            try:
                # 마지막 줄이 표현식인지 확인
                lines = code.strip().split('\\n')
                last_line = lines[-1].strip()
                if last_line and not last_line.startswith(('import ', 'from ', 'def ', 'class ', 'if ', 'for ', 'while ', 'try:', 'with ', '@')):
                    # 표현식 평가 시도
                    expr_result = eval(last_line, global_namespace)
                    if expr_result is not None:
                        result["data"] = {"text/plain": repr(expr_result)}
            except:
                pass  # 표현식이 아니거나 평가할 수 없음
                
    except Exception as e:
        result["status"] = "error"
        result["error"] = {
            "ename": type(e).__name__,
            "evalue": str(e),
            "traceback": traceback.format_exc().split('\\n')
        }
        
        stderr_text = stderr_capture.getvalue()
        if stderr_text:
            result["stderr"] = stderr_text
    
    return result

# 메인 루프
while True:
    try:
        line = input()
        if not line:
            continue
            
        request = json.loads(line)
        
        if request["type"] == "execute":
            result = execute_code(
                request["code"], 
                request["execution_count"],
                request["request_id"]
            )
            print(json.dumps(result))
            sys.stdout.flush()
            
    except EOFError:
        break
    except KeyboardInterrupt:
        break
    except Exception as e:
        error_result = {
            "status": "error",
            "error": f"Kernel error: {str(e)}"
        }
        print(json.dumps(error_result))
        sys.stdout.flush()
'''


def main():
    """커널 프로세스 메인 함수"""
    while True:
        try:
            line = input()
            if not line:
                continue

            request = json.loads(line)

            if request["type"] == "execute":
                result = execute_code(
                    request["code"],
                    request["execution_count"],
                    request["request_id"]
                )
                print(json.dumps(result))
                sys.stdout.flush()

        except EOFError:
            break
        except KeyboardInterrupt:
            break
        except Exception as e:
            error_result = {
                "status": "error",
                "error": f"Kernel error: {str(e)}"
            }
            print(json.dumps(error_result))
            sys.stdout.flush()


if __name__ == "__main__":
    main()