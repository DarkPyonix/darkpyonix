# PROTOCOL — DKP/1

커널과 그 클라이언트(매니저, 테스트, 향후 CLI 직결) 사이의 와이어 형식입니다. 이 문서는 SPEC의 일부이며 `PR-*` 요구사항이 여기를 가리킵니다. 결정의 근거는 [INTENT.md](INTENT.md) D4–D6에 있습니다.

## 1. 공통

- **인코딩:** 모든 메시지는 UTF-8 JSON 객체입니다. 바이너리(이미지 등)는 nbformat처럼 base64 문자열로 MIME 번들 안에 넣습니다.
- **버전:** 모든 메시지에 `"dkp": 1`이 있습니다. 모르는 버전은 무시(데이터그램)하거나 `error`로 닫습니다(TCP).
- **모르는 필드:** 받는 쪽은 모르는 필드를 무시합니다. 필드 추가는 버전을 올리지 않습니다.
- **런타임 홈:** `DARKPYONIX_HOME`(기본 `~/.darkpyonix`). 아래 경로는 모두 이 아래입니다.

| 경로 | 권한 | 내용 |
|---|---|---|
| `kernels/<kernel_id>.log` | 0600 | 커널 자신의 진단 로그(사용자 출력이 아님) |
| `locks/<kernel_id>.lock` | 0600 | 파일당 커널 하나를 보장하는 OS 잠금 |
| `tokens/<kernel_id>.json` | 0600 (폴더 0700) | 커널 접근 토큰 저장소(§6, SPEC FR-A5·FR-A6). 같은 계정의 모든 매니저가 읽고 씁니다 |
| `tokens/<kernel_id>.lock` | 0600 | 토큰 저장소를 고치는 쪽이 거는 OS 잠금(§6) |
| `sockets/<kernel_id>.sock` | 0600 (폴더 0700) | POSIX 전용. 커널의 제어 채널(§3)인 유닉스 도메인 소켓. 스트림 넘김(§3.7)도 여기서 받습니다. 경로가 OS 한도(macOS 104바이트, Linux 108바이트)를 넘으면 `<tempfile.gettempdir()>/darkpyonix-<uid>/<kernel_id>.sock`(폴더 0700)을 씁니다. 실제 경로는 announce의 `control`로 알립니다 |

Windows 커널의 제어 채널은 이름 있는 파이프 `\\.\pipe\darkpyonix-<user_tag>-<kernel_id>`입니다. 파일이 아니라서 런타임 홈 아래에 없습니다. 이름은 announce의 `control`로 알립니다.

## 2. 발견 (UDP 멀티캐스트)

### 2.1 주소

- 그룹 `239.255.68.80`, 포트 `46880`, 인터페이스 `127.0.0.1`, TTL 0(호스트 밖으로 나가지 않음), `IP_MULTICAST_LOOP` 켬.
- 커널과 매니저 모두 `SO_REUSEADDR`(가능하면 `SO_REUSEPORT`)로 같은 포트에 바인드하고 그룹에 가입합니다.
- 발견은 이 멀티캐스트 하나입니다. 등록 파일(`kernels/<kernel_id>.json`, `managers/<pid>.json`)과 `DARKPYONIX_DISCOVERY=registry`는 사용자 지시로 지웁니다(INTENT D4, 구현 대기 #50). 루프백 멀티캐스트가 막힌 환경과 WSL↔Windows 사이 발견은 지원 범위 밖입니다(SPEC FR-D3).
- 그룹 주소와 포트는 리더 결정, 사용자 확인 대기입니다(INTENT D4, PROJECT Q14).

### 2.2 사용자 태그

`user_tag = hex(SHA-256(account))[:16]`. `account`는 POSIX에서 `"uid:" + str(os.getuid())`, Windows에서 `"sid:" + 문자열 SID`(`S-1-5-21-…`)의 UTF-8입니다. 모든 데이터그램에 들어가고, 자기 태그와 다른 데이터그램은 무시합니다. 같은 기계의 다른 OS 사용자 커널과 섞이지 않게 하는 필터일 뿐 인증이 아닙니다(계정 확인은 §3.2). 2026-10-04까지는 `user.key`에서 만들었습니다(INTENT D5).

### 2.3 query (매니저 → 그룹)

```json
{"dkp": 1, "op": "query", "user_tag": "…", "nonce": "8f2c…", "kernel_id": "k_…"}
```

`kernel_id`가 없으면 모든 커널이 응답합니다. 있으면 그 커널만 응답합니다.

### 2.4 announce (커널 → 그룹)

```json
{
  "dkp": 1, "op": "announce", "user_tag": "…", "nonce": "8f2c…",
  "kernel_id": "k_3f9a0c1b2d4e5f607182", "path": "/home/u/exp/train.py",
  "pid": 41234, "control": "/home/u/.darkpyonix/sockets/k_3f9a0c1b2d4e5f607182.sock", "status": "busy",
  "run_id": "20261003-142233-a1f0",
  "python": {"version": "3.11.9", "implementation": "CPython", "executable": "/usr/bin/python3.11"},
  "kernel_version": "0.1.0", "runs_dir": "/home/u/exp/__runs__/train.py", "started_at": "2026-10-03T05:22:33Z", "host": "macmini"
}
```

`control`은 제어 채널의 주소입니다(§3). POSIX는 유닉스 도메인 소켓 경로, Windows는 파이프 이름입니다. 2026-10-04까지 있던 TCP `port`와 넘김용 `handoff`는 이 하나로 합쳤습니다(INTENT D5). 구현 대기(#55).

커널은 다음 때 announce를 보냅니다.
- query에 응답할 때(`nonce`를 그대로 돌려줌)
- 시작을 마쳤을 때, 상태(`status`, `run_id`)가 바뀔 때
- 5초마다(생존 신호)

`status`는 `starting | idle | busy | stopping`입니다. 커널은 announce를 파일로 남기지 않습니다(INTENT D4).

### 2.5 bye (커널 → 그룹)

```json
{"dkp": 1, "op": "bye", "user_tag": "…", "kernel_id": "k_…", "pid": 41234}
```

정상 종료 직전에 보냅니다. 비정상 종료 때는 보내지 못하므로 매니저는 15초 동안 announce가 없거나 `pid`가 살아 있지 않은 커널을 목록에서 뺍니다.

### 2.6 커널 ID

```
canonical = realpath(abspath(path))        # Windows는 normcase까지
kernel_id = "k_" + hex(SHA-256(canonical as UTF-8))[:20]
```

같은 파일을 가리키는 심볼릭 링크와 상대 경로는 같은 ID가 됩니다.

## 3. 제어 채널 (POSIX 유닉스 도메인 소켓, Windows 이름 있는 파이프)

### 3.1 프레임

```
+----------------+---------------------------+
| length: u32 BE | payload: UTF-8 JSON object |
+----------------+---------------------------+
```

- `length`는 payload 바이트 수이고 최대 64 MiB입니다. 넘으면 받는 쪽이 `error {code: "frame_too_large"}`를 보내고 연결을 닫습니다.
- 커널은 TCP 포트를 열지 않습니다. POSIX는 `sockets/<kernel_id>.sock`(0600, §1), Windows는 파이프 `\\.\pipe\darkpyonix-<user_tag>-<kernel_id>`에서만 받습니다. 주소는 announce의 `control`입니다. 같은 채널로 요청, 이벤트, 스트림 넘김(§3.7)을 모두 나릅니다.
- Windows 파이프는 `selectors`에 넣을 수 없어서, Windows 커널은 파이프 연결마다 스레드 하나를 둡니다.

### 3.2 핸드셰이크

INTENT D5. 사용자 결정(2026-10-04): "같은 컴퓨터의 같은 계정만 통과시켜야 해." 확인은 OS가 알려 주는 상대 계정으로 합니다. 키 파일과 비밀값은 없습니다. 구현 대기(#55).

**먼저 계정을 확인합니다.** 연결이 맺어지면 프레임을 주고받기 전에 양쪽이 OS에 상대 프로세스의 계정을 묻습니다. 커널은 상대가 자기 계정이 아니면 아무것도 보내지 않고 닫습니다. 매니저도 커널 쪽이 자기 계정이 아니면 닫습니다. 다른 계정이 같은 경로나 파이프 이름을 먼저 차지해 커널인 척하는 것을 막으려는 것입니다.

| OS | 커널(받는 쪽, 파이썬 3.8+ 표준 라이브러리) | 매니저(거는 쪽) |
|---|---|---|
| Linux | `conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))` → `struct ucred (pid, uid, gid)`. `uid == os.getuid()` | 같은 `SO_PEERCRED` |
| macOS | `conn.getsockopt(0, socket.LOCAL_PEERCRED, 76)` → `struct xucred`. 앞 8바이트 `struct.unpack_from("II", raw)`가 `(cr_version, cr_uid)`. 3.8에는 `socket.SOL_LOCAL`이 없어서 값 0을 씁니다 | `getpeereid` |
| 그 밖의 BSD | `ctypes.CDLL(None).getpeereid(fd, byref(uid), byref(gid))` | `getpeereid` |
| Windows | `ctypes`로 kernel32 `CreateNamedPipeW`를 부릅니다. 보안 설명자는 `D:P(A;;GA;;;<자기 SID>)`(`ConvertStringSecurityDescriptorToSecurityDescriptorW`), 모드에 `PIPE_REJECT_REMOTE_CLIENTS`, 첫 인스턴스에 `FILE_FLAG_FIRST_PIPE_INSTANCE`. 연결마다 advapi32 `ImpersonateNamedPipeClient` → `OpenThreadToken` → `GetTokenInformation(TokenUser)` → `EqualSid(자기 SID)` → `RevertToSelf` | `GetNamedPipeServerProcessId` → `OpenProcess` → `OpenProcessToken` → `GetTokenInformation(TokenUser)`를 자기 SID와 비교 |

- `_winapi`에는 상대 계정을 묻는 함수가 없어서 Windows 커널은 `ctypes`를 씁니다. `ctypes`는 표준 라이브러리이고 3.8에도 있습니다.
- 측정 기록(2026-10-04, 맥미니 macOS, CPython 3.8.20): `socketpair(AF_UNIX)`에서 `getsockopt(0, socket.LOCAL_PEERCRED, 76)`이 76바이트를 돌려주고, `cr_version` 0, `cr_uid`가 `os.getuid()`(501)와 같았습니다. `socket.LOCAL_PEERCRED`는 3.8.20부터 3.15.0rc1까지 1로 있었습니다.

**그다음 핸드셰이크입니다.**

```
커널  → {"dkp":1,"op":"hello","kernel_id":"k_…","kernel_version":"0.1.0"}
클라 → {"dkp":1,"op":"auth","client":{"name":"manager","kind":"manager","pid":123}}
커널  → {"dkp":1,"op":"welcome","session":"s_…","seq":1042}
```

`auth`는 이름만 남았고 비밀값이 없습니다. 클라이언트가 누구인지 알리는 정보뿐입니다. `seq`는 커널이 마지막으로 낸 이벤트 번호입니다. 핸드셰이크가 5초 안에 끝나지 않으면 커널이 연결을 닫습니다. 2026-10-04까지 있던 `hello.nonce`, `auth.mac`(`user.key` HMAC)과 `auth_failed` 오류는 지웁니다(INTENT D5).

### 3.3 요청과 응답

```json
{"dkp":1,"op":"request","id":7,"method":"run","params":{…}}
{"dkp":1,"op":"response","id":7,"ok":true,"result":{…}}
{"dkp":1,"op":"response","id":7,"ok":false,"error":{"code":"busy","message":"…","data":{…}}}
```

| method | params | result | 비고 |
|---|---|---|---|
| `status` | `{}` | §3.5 KernelInfo | |
| `run` | `mode: "all"\|"cells"`, `cells: [int]`, `source: str?`, `params: {str: any}?`, `on_busy: "reject"\|"queue"` (기본 reject) | `{run_id, state: "running"\|"queued", position?}` | 바쁘면 `busy` 오류, `data`에 현재 실행 |
| `cancel` | `run_id` | `{cancelled: bool}` | 대기 중인 실행만 취소 |
| `interrupt` | `{}` | `{interrupted: bool, run_id?}` | 실행 중인 셀에 `KeyboardInterrupt` |
| `restart` | `hard: bool` (기본 false) | `{restarted: true}` | soft는 네임스페이스만 새로, hard는 같은 인자로 프로세스 재실행 |
| `shutdown` | `{}` | `{shutting_down: true}` | 실행 중이면 먼저 인터럽트 후 종료 |
| `namespace` | `limit: int` (기본 200) | `{variables: [Variable]}` | 바쁠 때는 `repr`를 생략(§3.6) |
| `runs.list` | `limit: int` | `{runs: [RunSummary]}` | 최신순 |
| `runs.get` | `run_id: str \| "latest" \| "current"` | nbformat 4 노트북 | |
| `subscribe` | `since: int?` | `{seq: int, replayed: int}` | `since` 이후의 이벤트를 다시 보내고 이어서 실시간 전송 |
| `unsubscribe` | `{}` | `{}` | |
| `adopt` | §3.7.1 | `{stream_id}` | 매니저가 인증한 스트림 연결을 커널에 넘김. 구현 대기(#47) |
| `streams.close` | `share_id` | `{closed: int}` | 그 공유로 넘겨받은 스트림을 모두 닫음. 구현 대기(#47) |

오류 코드: `bad_request`, `unknown_method`, `busy`, `not_found`, `frame_too_large`, `shutting_down`, `internal`.

### 3.4 이벤트

```json
{"dkp":1,"op":"event","seq":1043,"type":"output","time":"2026-10-03T05:22:34.120Z","data":{…}}
```

`seq`는 커널 수명 동안 1씩 늘어납니다. 커널은 최근 이벤트를 링 버퍼(기본 10,000개 또는 16 MiB)에 둡니다. `subscribe since`가 버퍼보다 오래되면 `replay_truncated` 이벤트를 먼저 보냅니다. 이 이벤트는 로그에 속하지 않으므로 `seq`가 `null`입니다.

| type | data |
|---|---|
| `kernel.status` | `{status, run_id?}` |
| `run.queued` | `{run_id, position}` |
| `run.started` | `{run_id, mode, cells: [int], cell_ids: [str\|null], params}` |
| `run.finished` | `{run_id, status: "ok"\|"error"\|"interrupted"\|"cancelled", duration}` |
| `cell.started` | `{run_id, index, cell_id, execution_count}` |
| `cell.finished` | `{run_id, index, cell_id, status: "ok"\|"error"\|"interrupted", duration}` |
| `output` | `{run_id, index, cell_id, output: <nbformat 4 output>}` |
| `output.clear` | `{run_id, index, cell_id, wait: bool}` |
| `replay_truncated` | `{oldest_seq}` |

`index`는 실행이 읽은 파일에서의 셀 위치이고, `cell_id`는 그 셀의 공유 문서 ID(§4, `doc.snapshot`의 `cell_id`)입니다. 실행 중에 셀이 옮겨지거나 지워져도 `cell_id`로 셀을 찾아야 합니다. 실행이 시작될 때 공유 문서와 파일이 맞지 않으면(커널이 문서를 들고 있지 않을 때 등) `cell_id`는 `null`입니다. `run.started`의 `cell_ids`는 `cells`와 같은 순서·길이의 배열이고 같은 규칙을 따릅니다.

`output`의 `output` 필드는 nbformat 4의 출력 객체(`stream`, `display_data`, `execute_result`, `error`)를 그대로 씁니다. 같은 셀의 연속된 `stream` 출력은 커널이 최대 50 ms 동안 모아서 하나로 보냅니다.

### 3.5 KernelInfo

```json
{
  "kernel_id": "k_…", "path": "/home/u/exp/train.py", "pid": 41234, "control": "/home/u/.darkpyonix/sockets/k_….sock",
  "status": "busy", "run_id": "20261003-142233-a1f0", "queue": ["20261003-142301-0b1c"],
  "execution_count": 12, "python": {"version": "3.11.9", "implementation": "CPython", "executable": "…"},
  "started_at": "…", "host": "macmini", "kernel_version": "0.1.0", "runs_dir": "/home/u/exp/__runs__/train.py"
}
```

### 3.6 Variable

```json
{"name": "model", "type": "torch.nn.Module", "repr": "Classifier(…)", "shape": null, "dtype": null, "len": null}
```

커널은 `repr`를 `reprlib`로 200자까지만 만듭니다. `shape`·`dtype`·`len`은 읽을 수 있을 때만 채웁니다. 이 값들을 만들면 사용자 코드(`__repr__`, 프로퍼티)가 실행되므로, 셀이 실행 중일 때는 다른 스레드에서 부르지 않도록 `name`과 `type`만 채우고 나머지는 `null`로 둡니다. `_`로 시작하는 이름, 모듈, 커널이 넣은 이름(`__runs__`, `darkpyonix`)은 뺍니다.

### 3.7 스트림 넘김 (`adopt`)

INTENT D6. 매니저는 HTTP 요청을 인증하고 권한을 검사한 뒤, 오래 열린 스트림이면 그 연결을 커널에 넘깁니다. 넘기는 연결은 셋입니다.

| `kind` | HTTP 경로 | 커널이 쓰는 응답 |
|---|---|---|
| `events` | `GET /api/kernels/{kernel_id}/events` | `200 text/event-stream`, §3.4·§4의 이벤트 |
| `ws` | `GET /api/ws/kernels/{kernel_id}` (Upgrade) | `101 Switching Protocols`, §5의 2025 WebSocket 동기화 |
| `wait` | `GET /api/kernels/{kernel_id}/runs/{run_ref}/wait` | 실행이 끝나거나 `timeout`이 지나면 JSON 한 번(SPEC FR-S7) |

짧은 요청(§3.3의 나머지 메서드)은 넘기지 않습니다. 구현 대기(#47).

#### 3.7.1 `adopt` 요청

```json
{"dkp":1,"op":"request","id":9,"method":"adopt","params":{
  "kind": "events",
  "transport": "fd",
  "request": "<base64: 매니저가 이미 읽은 요청 바이트 전체(요청 줄, 헤더, 읽은 본문)>",
  "label": {"capabilities": ["read", "history"], "client_id": "dev_8f2c1a", "user": "alice",
            "nickname": "alice-mbp", "share_id": "s_0123456789abcdef"},
  "share": null
}}
```

- `transport`: `fd`(POSIX, 날 소켓), `share`(Windows, 날 소켓), `pump`(소켓 쌍의 한쪽, §3.7.3).
- `request`: 커널은 이 바이트를 소켓에서 읽은 것처럼 다룹니다. 매니저는 요청을 다시 쓰지 않습니다. 요청에 든 토큰(`Authorization`, `?token=`)은 커널이 보지 않습니다.
- `label`: 매니저가 인증 결과로 채웁니다. `capabilities`는 그 토큰의 능력 집합(`read`, `history`, `execute`, `edit`, `manage`, SPEC FR-A3)입니다. 매니저는 FD나 `share` 바이트와 함께 이 집합을 넘깁니다. `share_id`는 공유 토큰으로 들어온 연결에만 있고, 마스터 토큰이나 커널 로그인·초기 토큰이면 `null`입니다. 커널은 `capabilities`를 무엇을 보낼지 거르는 데만 씁니다(SPEC FR-K9). 인증이나 토큰 검사는 하지 않습니다.
- `share`: Windows에서만 씁니다. 매니저가 `WSADuplicateSocketW(socket, kernel_pid, &info)`로 만든 `WSAPROTOCOL_INFOW`의 base64입니다. 커널은 `socket.fromshare(base64 디코드 값)`으로 엽니다. 파이썬 `socket.share`와 같은 형식입니다. `kernel_pid`는 매니저가 제어 파이프에서 `GetNamedPipeServerProcessId`로 얻고, announce의 `pid`와 같아야 합니다.

#### 3.7.2 POSIX: `SCM_RIGHTS`

`SCM_RIGHTS`는 유닉스 도메인 소켓에서만 됩니다. POSIX 제어 채널이 바로 유닉스 도메인 소켓이므로(§3.1) 넘김용 소켓을 따로 두지 않습니다.

1. 매니저가 announce의 `control` 경로에 연결하고, 상대 계정 확인과 핸드셰이크(§3.2)를 마칩니다.
2. `adopt` 요청 프레임(`transport: "fd"`)을 보낸 직후, 1바이트 `b"F"`를 `sendmsg`의 보조 데이터 `(SOL_SOCKET, SCM_RIGHTS, fd)`와 함께 보냅니다.
3. 커널은 프레임을 읽은 뒤 `socket.recvmsg(1, socket.CMSG_LEN(4))`로 FD를 받고 `socket.socket(fileno=fd)`로 엽니다. 3.8에는 `socket.recv_fds`가 없으므로 `recvmsg`를 직접 씁니다.
4. 커널이 `response {stream_id}`를 보내면 매니저는 자기 쪽 FD를 닫습니다. 그 뒤 매니저는 그 연결과 무관합니다.

Windows 파이프로 `transport: "fd"`가 오면 `bad_request`입니다. FD를 받지 못하면(보조 데이터 없음) `bad_request`이고, 매니저는 그 연결에 `502`를 쓰고 닫습니다.

#### 3.7.3 소켓 쌍과 퍼 나르기 (`pump`)

TLS, HTTP/2, P2P 터널 위의 연결에는 넘길 날 소켓이 없습니다. 매니저는 소켓 쌍(POSIX `socketpair(AF_UNIX, SOCK_STREAM)`, Windows는 루프백 TCP 쌍)을 만들어 한쪽을 §3.7.1·§3.7.2와 같이 넘기고(`transport: "pump"`, POSIX는 `fd`로, Windows는 `share`로), 다른 쪽과 클라이언트 연결 사이에서 바이트를 퍼 나릅니다.

- 커널은 소켓 쌍 위에서도 HTTP/1.1로 응답합니다(상태 줄, 헤더, 본문. `ws`는 `101` 뒤 WebSocket 프레임).
- 매니저는 본문 바이트를 바꾸지 않습니다. 매니저가 맡는 것은 바깥 운반뿐입니다. TLS 레코드의 암복호화, HTTP/2에서는 커널 응답 머리(상태 줄과 헤더)를 HEADERS 프레임으로 옮기고 본문을 DATA 프레임에 그대로 싣는 일, 터널에서는 터널 스트림에 싣는 일입니다.
- 이런 스트림은 매니저와 함께 끝납니다. 매니저가 끝나면 소켓 쌍이 닫히고, 커널은 그 스트림을 닫힌 연결로 처리합니다.

#### 3.7.4 공유 철회 (`streams.close`)

```json
{"dkp":1,"op":"request","id":12,"method":"streams.close","params":{"share_id":"s_0123456789abcdef"}}
```

매니저는 공유를 지우거나 그 타입의 공유 토큰을 다시 만들 때(SPEC FR-A4) 이 요청을 보냅니다. 커널은 `label.share_id`가 같은 넘겨받은 스트림을 모두 닫고 닫은 개수를 돌려줍니다. 커널은 넘겨받은 스트림의 라벨만 기억하고, 공유 목록이나 토큰은 모릅니다.

#### 3.7.5 넘겨받은 스트림의 수명

- 커널은 넘겨받은 스트림마다 `stream_id`와 라벨을 기억합니다. 클라이언트가 끊거나 쓰기가 실패하면 그 스트림을 버립니다. 클라이언트별 송신 버퍼가 64 MiB를 넘으면 그 클라이언트를 끊습니다(PR-3과 같은 규칙).
- 날 소켓(`fd`, `share`)으로 넘긴 스트림은 매니저가 죽어도 이어집니다(INTENT D2).
- `events`와 `ws` 스트림에 `client_id`가 있으면, 그 스트림이 열려 있는 동안 그 클라이언트는 접속자입니다. 스트림이 닫히고 30초 뒤 `presence.leave`가 됩니다(SPEC FR-S4).

## 4. 협업 문서 (SPEC §10a)

커널은 파일의 공유 문서 상태(셀, 셀별 버전, 잠금, 접속자)를 들고 있습니다. 아래 메서드와 이벤트는 §3과 같은 채널을 씁니다. 모든 편집 메서드는 `client`를 받습니다. `client`는 `{client_id, nickname, user, avatar?, capabilities}`이고, 매니저가 토큰에서 채워 넘깁니다. 커널은 `capabilities`를 검사하지 않습니다. 권한은 매니저가 이미 검사했습니다(INTENT D5, D18). 커널은 `capabilities`를 접속자 표시에 싣기만 합니다.

| method | params | result |
|---|---|---|
| `doc.snapshot` | `{}` | `{doc_version, seq, cells: [DocumentCell], presence: [Presence]}` (아래 스냅숏 규칙) |
| `doc.cell.create` | `client, request_id?, after?: cell_id, before?: cell_id, type?, title?, source?, metadata?` | `{cell}`. `after`·`before`가 둘 다 없으면 **문서 끝에 붙입니다**. 둘 다 주면 `bad_request`. `type` 기본값 `code`, `source` 기본값 `""` |
| `doc.cell.update` | `client, request_id?, cell_id, base_version, source?, type?, title?, metadata?` | `{cell}`. 버전이 다르면 `conflict`, 다른 클라이언트가 잠갔으면 `locked`. `title: null`이나 `""`는 제목을 지웁니다. 프리앰블은 `source`만 바꿀 수 있습니다 |
| `doc.cell.delete` | `client, request_id?, cell_id, base_version` | `{deleted: true}` |
| `doc.cell.move` | `client, request_id?, cell_id, to_index` | `{cell}` |
| `doc.lock` | `client, request_id?, cell_id` | `{lock}`. 이미 잠겨 있으면 `locked`, `data.locked_by` |
| `doc.unlock` | `client, request_id?, cell_id, source?, base_version?` | `{cell}`. 최종 소스를 함께 보내면 저장 후 해제 |
| `presence.update` | `client, request_id?, focused_cell_id?, cursor?: {cell_id, line, column, selection?: [[l,c],[l,c]]}` | `{}` |
| `presence.leave` | `client, request_id?` | `{}` |
| `runs.wait` | `run_id \| "latest" \| "current", timeout` | 끝나면 실행 요약, 아니면 `{status: "running", progress?}` |

오류 코드 추가: `conflict`, `locked`. 2026-10-03까지 있던 `forbidden`(커널 쪽 권한 검사)은 지웁니다(구현 대기 #49).

| event type | data |
|---|---|
| `doc.cell.created` / `doc.cell.updated` / `doc.cell.deleted` / `doc.cell.moved` | `{doc_version, cell, by, request_id?}` (`deleted`는 `cell` 대신 `cell_id`) |
| `doc.lock` / `doc.unlock` | `{doc_version, cell_id, lock?, by, reason?: "released"\|"idle"\|"disconnected", request_id?}` |
| `doc.reloaded` | `{doc_version, cells, cause: "external"}`. 바깥 편집으로 다시 파싱한 뒤 보내는 전체 셀 목록 |
| `doc.conflict` | `{doc_version, cell_id, local: {source, version, by}, disk: {source}}` |
| `presence.update` / `presence.leave` | `{doc_version, client_id, nickname, user, avatar?, capabilities, focused_cell_id?, focused_at?, cursor?, last_seen, request_id?}` |

`Presence = {client_id, nickname, user, avatar?, capabilities, focused_cell_id?, focused_at?, cursor?, last_seen}`.

`DocumentCell = {cell_id, index, type, raw_type, title, metadata, source, source_sha256, version, lock?, conflict?}`.

- `source`는 파서(FORMAT §2.4)가 낸 본문 그대로입니다. 표식과 메타데이터 줄은 빠지고, 다음 표식 앞의 빈 줄(앞 셀 본문에 속함)과 줄 끝(`\r\n` 포함)은 남습니다. 클라이언트가 이 값을 바꾸지 않고 돌려보내면 파일 바이트도 바뀌지 않습니다.
- `type`은 정규 타입(소문자, 별칭 해석, 타입 없는 표식은 `code`, 첫 셀은 `preamble`)이고, `raw_type`은 표식의 `[ ]` 안에 쓰인 그대로(없으면 `null`)입니다. 표시는 `type`으로, 원문 보존이 필요하면 `raw_type`을 씁니다.
- `title`은 표식의 제목이고 없으면 `null`입니다.

**스냅숏의 `seq`.** `seq`는 **스냅숏에 이미 반영된 마지막 이벤트의 번호**입니다. 클라이언트는 `subscribe since=seq`(HTTP는 `?since=seq`)로 구독하고, `seq`가 그보다 큰 이벤트만 스냅숏 위에 적용합니다. 커널은 문서 상태를 바꾸는 일과 그 이벤트를 내는 일을 한 잠금 안에서 하고, 스냅숏도 같은 잠금 안에서 만듭니다. 따라서 스냅숏과 경쟁한 편집은 스냅숏에 들어 있거나(`seq` 이하) 구독으로 오거나(`seq` 초과) 둘 중 정확히 하나입니다. 클라이언트가 `seq`가 자기 것 이하인 이벤트를 받으면 무시합니다(재연결로 겹칠 때).

**`doc_version`.** 문서 내용이나 구조가 바뀔 때만 1 늘어납니다: 셀 생성·수정·삭제·이동(`doc.cell.*`)과 다시 읽기(`doc.reloaded`). 잠금 해제로 충돌이 풀려 셀이 바뀌면 그것도 `doc.cell.updated`이므로 늘어납니다. 잠금·해제(`doc.lock`/`doc.unlock`), 충돌 표시(`doc.conflict`), 접속자(`presence.*`)는 늘리지 않지만, 이 이벤트들도 내는 순간의 현재 `doc_version`을 담습니다. 바뀐 것이 없는 수정·이동(같은 값, 같은 위치)은 이벤트를 내지 않고 `doc_version`도 그대로입니다.

**`request_id`.** `doc.*`(snapshot 제외)와 `presence.*` 요청은 클라이언트가 고른 `request_id`(문자열, 1–64자)를 받을 수 있습니다. 그 요청으로 생긴 이벤트는 모두 같은 `request_id`를 그대로 담습니다(예: `doc.unlock`에 최종 소스를 보내면 `doc.cell.updated`와 `doc.unlock` 둘 다, `presence.leave`가 잠금을 풀면 `doc.unlock`과 `presence.leave` 둘 다). 합쳐서 나중에 보내는 커서 `presence.update`는 마지막으로 합쳐진 요청의 `request_id`를 담습니다. 유휴 해제처럼 요청 없이 생긴 이벤트에는 없습니다. 같은 `client_id`를 쓰는 두 창이 자기 편집의 메아리를 구별하는 용도이며, 커널은 값을 해석하지 않습니다. 형식이 틀리면 `bad_request`입니다.

`run.queued`, `run.started`, `run.finished`, `cell.started`, `cell.finished` 이벤트의 `data`에는 `started_by`(client_id, user, nickname)가 들어갑니다. 인터럽트로 끝난 실행에는 `interrupted_by`가 더해집니다(SPEC FR-S6). `run` 메서드는 `cells` 대신 `cell_ids`를 받을 수 있습니다.

## 5. 2025 WebSocket 동기화

SPEC FR-S9, INTENT D19. 2025 설계의 `/ws/kernels/{kernel_id}?token={token}`을 `/api/ws/kernels/{kernel_id}`로 되살립니다. 매니저가 토큰과 권한을 확인하고 연결을 넘기면(§3.7, `kind: "ws"`), 커널이 업그레이드에 `101 Switching Protocols`로 답하고 이 절의 메시지를 주고받습니다. 구현 대기(#49).

### 5.1 프레임

- RFC 6455입니다. 커널은 `Sec-WebSocket-Accept = base64(SHA-1(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))`를 표준 라이브러리로 만듭니다.
- 메시지는 텍스트 프레임 하나에 JSON 객체 하나이고, `type` 필드로 구별합니다. 커널이 보내는 프레임은 마스크하지 않고, 클라이언트 프레임은 마스크되어 있어야 합니다(RFC 6455).
- 커널은 30초마다 ping을 보내고, 60초 동안 아무 프레임도 받지 못하면 닫습니다. 두 값은 리더 제안입니다(2025 명세에 없음).
- 이벤트에서 나온 메시지에는 커널 이벤트 번호 `seq`가 더해집니다(2025에 없는 필드, 더하기만 함).

### 5.2 클라이언트 → 커널

| `type` | 2025 필드 | 커널 동작 | 필요한 능력 |
|---|---|---|---|
| `request_code` | `kernel_id` | `code_data`로 답함 | `read`. 없으면 `source`가 빈 `code_data` |
| `request_history` | `kernel_id` | `history_data`로 답함(최신 실행 기록의 셀별 출력) | `history`. 없으면 빈 `history` |
| `request_locks` | `kernel_id` | `locks_data`로 답함 | `read` |
| `start_typing` | `cell_id`, `user_id` | `doc.lock`과 같음. 성공하면 모두에게 `cell_locked`. 이미 잠겼으면 `{"type":"error","error":"CELL_ALREADY_LOCKED","cell_id","locked_by"}` | `edit`. 없으면 `{"type":"error","error":"INSUFFICIENT_PERMISSION"}` |
| `cell_focus` | `cell_id`, `user_id` | `presence.update`와 같음. 모두에게 `other_user_focus` | `read` |
| `cell_blur` | `cell_id` | 포커스를 지움. 모두에게 `other_user_blur`. 2025에 송신 메시지가 없어 더함 | `read` |

2025 메시지의 `user_id`는 쓰지 않습니다. 보낸 사람은 넘김 라벨(`client_id`, `user`, `nickname`)입니다. 모르는 `type`은 `{"type":"error","error":"UNKNOWN_TYPE"}`입니다.

### 5.3 커널 → 클라이언트

| `type` | 2025 필드 | 커널 이벤트 |
|---|---|---|
| `code_data` | `cells: [{cell_id, cell_type, source, execution_count}]` | `doc.snapshot` |
| `history_data` | `history: [{cell_id, execution_count, outputs, executed_at}]` | 최신 실행 기록 |
| `locks_data` | `locks: [{cell_id, is_locked, locked_by?, user_name?, locked_at?}]` | 문서의 잠금 |
| `users_focus_data` | `users_focus: [{user_id, user_name, user_avatar, focused_cell_id, focused_at}]` | 접속자. 연결 직후 한 번 |
| `other_user_focus` | `cell_id, user_id, user_name, user_avatar, timestamp` | `presence.update`(포커스 있음) |
| `other_user_blur` | `cell_id, user_id, user_name, timestamp` | `presence.update`(포커스 없음) |
| `cell_locked` | `cell_id, locked_by, user_name, locked_at` | `doc.lock` |
| `cell_unlocked_with_code` | `cell_id, previously_locked_by, user_name, updated_code` | `doc.unlock`(최종 소스 포함) |
| `execution_started` | `cell_id, execution_id, started_by, user_name, code, timestamp` | `cell.started` |
| `execution_output` | `cell_id, execution_id, outputs, is_complete, timestamp` | `output` |
| `execution_complete` | `cell_id, execution_id, final_outputs, execution_time, completed_at` | `cell.finished`(`ok`) |
| `execution_error` | `cell_id, execution_id, error {ename, evalue, traceback}, failed_at` | `cell.finished`(`error`) |
| `execution_interrupted` | `cell_id, execution_id, interrupted_by, user_name, interrupted_at, partial_outputs` | `cell.finished`(`interrupted`) |
| `event` | `event: <§3.4·§4 이벤트>` | 2025 이름이 없는 이벤트(`doc.cell.created` 등) |

- `execution_id`는 `run_id`입니다. `user_id`는 `client_id`, `user_name`은 `user`(없으면 `nickname`)입니다.
- `history`가 없는 라벨에는 `execution_output`을 보내지 않고, `final_outputs`, `partial_outputs`, `error.traceback`, `outputs`를 빈 값으로 보냅니다(2025: "viewer1: 코드만 적혀있고 History 없음"). `read`가 없는 라벨에는 `code`, `updated_code`, 셀 `source`를 빈 값으로 보냅니다.
- 편집, 잠금 해제, 실행, 인터럽트는 REST(매니저)로 보내고, 그 결과가 위 메시지로 퍼집니다. 2025 명세도 같았습니다.

## 6. 커널 접근 토큰 저장소

SPEC FR-A5, FR-A6, INTENT D17. 커널 접근 토큰(초기, 로그인, 공유)은 커널에 묶이고, 어느 매니저로 들어와도 통해야 합니다. 사용자(2026-10-04): "아니, 그게 아니고 해당 커널에 접근 가능한 토큰을 말하는거야. 매니저가 여러개잖아." 그래서 토큰은 한 매니저의 `manager.db`가 아니라 런타임 홈의 파일에 둡니다. 위치는 리더 결정, 사용자 확인 대기입니다(PROJECT Q16). 구현 대기(#48).

```json
{
  "kernel_id": "k_3f9a0c1b2d4e5f607182",
  "path": "/home/u/exp/train.py",
  "tokens": [
    {"token_sha256": "<hex>", "kind": "initial", "capabilities": ["read", "history", "execute", "edit", "manage"],
     "share_id": null, "label": null, "created_at": "2026-10-04T01:02:03Z",
     "expires_at": null, "revoked_at": null},
    {"token_sha256": "<hex>", "kind": "share", "capabilities": ["read", "history"],
     "share_id": "s_0123456789abcdef", "label": "lab", "created_at": "…",
     "expires_at": null, "revoked_at": "2026-10-05T09:00:00Z"}
  ]
}
```

- **토큰 값은 두지 않습니다.** 토큰은 32바이트 무작위 값이라 소금 없는 SHA-256으로 충분합니다. 매니저는 받은 토큰의 SHA-256을 `hmac.compare_digest`로 비교합니다.
- **`kind`:** `initial`(SPEC FR-A6 초기 토큰), `login`(커널 로그인), `share`(공유 토큰).
- **거둔 토큰은 남깁니다.** `revoked_at`이 있는 토큰은 `401 token_revoked`(2025 `TOKEN_BLACKLISTED`)이고, 확인 응답의 `blacklisted`가 `true`입니다. 파일이 지워질 때 함께 지워집니다.
- **고치는 방법.** 고치는 쪽(매니저, 또는 파일 삭제를 본 커널)은 `tokens/<kernel_id>.lock`에 OS 잠금(POSIX `fcntl.flock`, Windows `msvcrt.locking`)을 걸고, 읽고, 바꾼 내용을 같은 폴더의 임시 파일에 쓴 뒤 `os.replace`로 바꿉니다. 읽는 쪽은 잠그지 않습니다. `os.replace`가 원자적이라 반쯤 쓴 파일을 읽지 않습니다.
- **파일이 지워지면.** `path`에 파일이 없으면 저장소를 지웁니다. 매니저는 시작할 때 `tokens/` 전체를, 토큰을 검사할 때마다 그 저장소를 확인합니다. 커널은 자기 파일이 지워진 것을 알아채면(SPEC FR-S5) 지웁니다.
- **누가 읽나.** 같은 OS 계정의 프로세스만 읽습니다(0600, 폴더 0700). 매니저와 커널 사이도 같은 계정만 통과하므로(§3.2) 이 범위가 같습니다.
- **커널은 검사하지 않습니다.** 커널은 이 파일로 토큰을 검사하지 않고, 넘겨받은 요청의 토큰도 보지 않습니다(INTENT D5, SPEC FR-K9).
