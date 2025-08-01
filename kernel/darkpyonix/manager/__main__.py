"""
Manager CLI Interface
커널 매니저 명령줄 인터페이스
"""

import sys
import json
import argparse
import os

# 상위 디렉토리를 파이썬 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from .manager import KernelManager


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="Python Kernel Manager")
    parser.add_argument("--interactive", "-i", action="store_true",
                        help="대화형 모드로 실행")
    parser.add_argument("--list", "-l", action="store_true",
                        help="활성 커널 목록 표시")
    parser.add_argument("--create", "-c", metavar="KERNEL_ID",
                        help="새 커널 생성")
    parser.add_argument("--delete", "-d", metavar="KERNEL_ID",
                        help="커널 삭제")
    parser.add_argument("--execute", "-e", nargs=2, metavar=("KERNEL_ID", "CODE"),
                        help="지정된 커널에서 코드 실행")
    parser.add_argument("--status", "-s", metavar="KERNEL_ID",
                        help="커널 상태 확인")
    parser.add_argument("--restart", "-r", metavar="KERNEL_ID",
                        help="커널 재시작")
    parser.add_argument("--stats", action="store_true",
                        help="매니저 통계 정보 표시")

    args = parser.parse_args()

    manager = KernelManager()

    try:
        if args.interactive:
            run_interactive_mode(manager)
        elif args.list:
            kernels = manager.list_kernels()
            if kernels:
                print(f"활성 커널: {kernels}")
                for kernel_id in kernels:
                    info = manager.get_kernel_info(kernel_id)
                    print(f"  {kernel_id}: {info}")
            else:
                print("활성 커널이 없습니다.")
        elif args.create:
            kernel_id = manager.create_kernel(args.create)
            print(f"커널 생성됨: {kernel_id}")
        elif args.delete:
            success = manager.delete_kernel(args.delete)
            if success:
                print(f"커널 삭제됨: {args.delete}")
            else:
                print(f"커널을 찾을 수 없음: {args.delete}")
        elif args.execute:
            kernel_id, code = args.execute
            result = manager.execute_on_kernel(kernel_id, code)
            print_execution_result(result)
        elif args.status:
            info = manager.get_kernel_info(args.status)
            if info:
                print(f"커널 {args.status} 상태:")
                for key, value in info.items():
                    print(f"  {key}: {value}")
            else:
                print(f"커널을 찾을 수 없음: {args.status}")
        elif args.restart:
            success = manager.restart_kernel(args.restart)
            if success:
                print(f"커널 재시작됨: {args.restart}")
            else:
                print(f"커널을 찾을 수 없음: {args.restart}")
        elif args.stats:
            stats = manager.get_statistics()
            print("매니저 통계:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
        else:
            parser.print_help()

    except KeyboardInterrupt:
        print("\n프로그램이 중단되었습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        manager.shutdown_all()


def run_interactive_mode(manager: KernelManager):
    """대화형 모드 실행"""
    print("Python Kernel Manager - 대화형 모드")
    print("명령어: create, list, delete, execute, help, quit")
    print("도움말: help 명령으로 전체 명령어 확인")
    print("-" * 50)

    current_kernel_id = None

    while True:
        try:
            # 프롬프트 표시
            prompt = f"[{current_kernel_id}]>>> " if current_kernel_id else ">>> "
            command = input(prompt).strip()

            if not command:
                continue

            parts = command.split(None, 2)
            cmd = parts[0].lower()

            if cmd in ["quit", "exit", "q"]:
                break
            elif cmd == "help" or cmd == "h":
                print_help()
            elif cmd == "create":
                kernel_id = parts[1] if len(parts) > 1 else None
                new_id = manager.create_kernel(kernel_id)
                print(f"커널 생성됨: {new_id}")
                if current_kernel_id is None:
                    current_kernel_id = new_id
                    print(f"현재 커널 설정: {current_kernel_id}")
            elif cmd == "list" or cmd == "ls":
                kernels = manager.list_kernels()
                if kernels:
                    print(f"활성 커널: {kernels}")
                    if current_kernel_id:
                        print(f"현재 커널: {current_kernel_id}")
                    # 각 커널의 간단한 정보 표시
                    for kernel_id in kernels:
                        info = manager.get_kernel_info(kernel_id)
                        status = "실행중" if info.get("is_running") else "정지"
                        exec_count = info.get("execution_count", 0)
                        print(f"  {kernel_id}: {status}, 실행횟수: {exec_count}")
                else:
                    print("활성 커널이 없습니다.")
            elif cmd == "delete" or cmd == "del":
                if len(parts) < 2:
                    print("사용법: delete <kernel_id>")
                    continue
                success = manager.delete_kernel(parts[1])
                if success:
                    print(f"커널 삭제됨: {parts[1]}")
                    if current_kernel_id == parts[1]:
                        current_kernel_id = None
                        print("현재 커널이 삭제되었습니다.")
                else:
                    print(f"커널을 찾을 수 없음: {parts[1]}")
            elif cmd == "use":
                if len(parts) < 2:
                    print("사용법: use <kernel_id>")
                    continue
                if manager.get_kernel(parts[1]):
                    current_kernel_id = parts[1]
                    print(f"현재 커널 변경: {current_kernel_id}")
                else:
                    print(f"커널을 찾을 수 없음: {parts[1]}")
            elif cmd in ["execute", "exec", "run"]:
                if len(parts) < 2:
                    print("사용법: execute <code> 또는 exec <kernel_id> <code>")
                    continue

                # 커널 ID가 지정된 경우와 현재 커널 사용하는 경우 구분
                if len(parts) == 3:
                    # execute <kernel_id> <code>
                    kernel_id = parts[1]
                    code = parts[2]
                else:
                    # execute <code> (현재 커널 사용)
                    if not current_kernel_id:
                        print("현재 커널이 설정되지 않았습니다. create 명령으로 커널을 생성하거나 use 명령으로 커널을 선택하세요.")
                        continue
                    kernel_id = current_kernel_id
                    code = parts[1]

                result = manager.execute_on_kernel(kernel_id, code)
                print_execution_result(result)
            elif cmd == "status":
                if len(parts) > 1:
                    kernel_id = parts[1]
                else:
                    kernel_id = current_kernel_id

                if not kernel_id:
                    print("커널 ID를 지정하거나 현재 커널을 설정하세요.")
                    continue

                info = manager.get_kernel_info(kernel_id)
                if info:
                    print(f"커널 {kernel_id} 상태:")
                    for key, value in info.items():
                        print(f"  {key}: {value}")
                else:
                    print(f"커널을 찾을 수 없음: {kernel_id}")
            elif cmd == "restart":
                if len(parts) > 1:
                    kernel_id = parts[1]
                else:
                    kernel_id = current_kernel_id

                if not kernel_id:
                    print("커널 ID를 지정하거나 현재 커널을 설정하세요.")
                    continue

                success = manager.restart_kernel(kernel_id)
                if success:
                    print(f"커널 {kernel_id} 재시작 완료")
                else:
                    print(f"커널을 찾을 수 없음: {kernel_id}")
            elif cmd == "interrupt":
                if len(parts) > 1:
                    kernel_id = parts[1]
                else:
                    kernel_id = current_kernel_id

                if not kernel_id:
                    print("커널 ID를 지정하거나 현재 커널을 설정하세요.")
                    continue

                success = manager.interrupt_kernel(kernel_id)
                if success:
                    print(f"커널 {kernel_id} 실행 중단 신호 전송")
                else:
                    print(f"커널을 찾을 수 없음: {kernel_id}")
            elif cmd == "stats":
                stats = manager.get_statistics()
                print("매니저 통계:")
                for key, value in stats.items():
                    if value is not None:
                        if "time" in key and isinstance(value, (int, float)):
                            import datetime
                            time_str = datetime.datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")
                            print(f"  {key}: {time_str}")
                        else:
                            print(f"  {key}: {value}")
            elif cmd == "cleanup":
                timeout = float(parts[1]) if len(parts) > 1 else 3600
                cleaned = manager.cleanup_idle_kernels(timeout)
                print(f"{cleaned}개의 유휴 커널이 정리되었습니다.")
            elif cmd == "pool":
                if len(parts) < 2:
                    print("사용법: pool <size> [name]")
                    continue
                size = int(parts[1])
                name = parts[2] if len(parts) > 2 else "default"
                kernel_ids = manager.create_kernel_pool(size, name)
                print(f"커널 풀 '{name}' 생성됨: {kernel_ids}")
            elif cmd == "clear":
                # 화면 정리
                os.system('cls' if os.name == 'nt' else 'clear')
            else:
                print(f"알 수 없는 명령어: {cmd}")
                print("'help'를 입력하여 사용 가능한 명령어를 확인하세요.")

        except KeyboardInterrupt:
            print("\n프로그램을 종료합니다.")
            break
        except EOFError:
            break
        except Exception as e:
            print(f"오류: {e}")


def print_help():
    """도움말 출력"""
    help_text = """
사용 가능한 명령어:

기본 커널 관리:
  create [kernel_id]      - 새 커널 생성 (ID 미지정시 자동 생성)
  list, ls                - 활성 커널 목록 표시
  use <kernel_id>         - 현재 사용할 커널 선택
  delete <kernel_id>      - 지정된 커널 삭제

코드 실행:
  execute <code>          - 현재 커널에서 코드 실행
  exec <kernel_id> <code> - 지정된 커널에서 코드 실행
  run <code>              - execute와 동일

커널 제어:
  status [kernel_id]      - 커널 상태 확인 (ID 미지정시 현재 커널)
  restart [kernel_id]     - 커널 재시작 (ID 미지정시 현재 커널)
  interrupt [kernel_id]   - 실행 중인 코드 중단 (ID 미지정시 현재 커널)

고급 기능:
  stats                   - 매니저 통계 정보 표시
  cleanup [timeout]       - 유휴 커널 정리 (기본 3600초)
  pool <size> [name]      - 커널 풀 생성

기타:
  clear                   - 화면 정리
  help, h                 - 이 도움말 표시
  quit, exit, q           - 프로그램 종료

예제:
  create my_kernel        # 'my_kernel' ID로 커널 생성
  use my_kernel           # 현재 커널을 'my_kernel'로 설정
  execute print("hi")     # 현재 커널에서 코드 실행
  exec kernel2 2+3        # 'kernel2'에서 코드 실행
  pool 3 test_pool        # 3개 커널로 구성된 'test_pool' 생성
"""
    print(help_text)


def print_execution_result(result):
    """실행 결과를 보기 좋게 출력"""
    if result.get("status") == "ok":
        # 표준 출력
        if "stdout" in result:
            print(result["stdout"], end="")

        # 표현식 결과
        if "data" in result and "text/plain" in result["data"]:
            exec_count = result.get('execution_count', '?')
            print(f"Out[{exec_count}]: {result['data']['text/plain']}")

        # 표준 에러 (경고 등)
        if "stderr" in result:
            print(f"stderr: {result['stderr']}", end="")

    elif result.get("status") == "error":
        error_info = result.get("error", {})
        if isinstance(error_info, dict):
            print(f"Error: {error_info.get('ename', 'Unknown')}: {error_info.get('evalue', '')}")
            if "traceback" in error_info and error_info["traceback"]:
                for line in error_info["traceback"]:
                    if line.strip():
                        print(line)
        else:
            print(f"Error: {result.get('error', 'Unknown error')}")
    else:
        # 알 수 없는 응답
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()