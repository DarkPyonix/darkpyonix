# darkpyonix
DarkPyonix Kernel for AI/ML Development

> Persistent reconnectable notebook kernel architecture.

## 핵심 아이디어
- 단일 Kernel Manager 프로세스가 외부 FastAPI 서버로 동작
- 각 커널은 subprocess (Python) 로 실행되고, 매니저와 UNIX Domain Socket(Windows 에서는 Named Pipe / TCP loopback) 으로 제어 채널 유지
- SSE 엔드포인트 최초 진입 시 FastAPI 가 HTTP 소켓 FD 를 dup 하여 커널 프로세스에 전달 -> 이후 해당 연결은 커널이 직접 write
- 매니저는 이벤트 라우팅/셀 메타 관리만 수행, 실행/스트림 I/O 는 커널이 직접 처리

## 디렉토리
- manager: FastAPI app, kernel process lifecycle
- kernel: 개별 커널 프로세스 엔트리
- common: 프로토콜, 메시지 스키마

## 개발 메모
Windows 에서는 표준 FD 전달 (SCM_RIGHTS) 가 불가하므로 다음 전략 중 하나 사용:
1) 127.0.0.1 loopback 전용 upgrade: 매니저가 커널에게 포트/토큰 전달, 커널이 해당 소켓에 attach (SO_REUSEPORT 불가 시 별도 핸드오프 라우트) 
2) pywin32 로 DuplicateHandle 사용 (추후 구현) 

현재 PoC 는 플랫폼 공통성을 위해 커널이 manager 로부터 control channel 통해 'hijack request id' 를 받고, 내부 connection map 에서 raw socket 객체를 커널로 프록시하는 thread 를 붙여 zero-copy 에 가깝게 전달.
