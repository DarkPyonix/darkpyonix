# PROTOCOL — DKP/1

커널과 그 클라이언트(매니저, 테스트, 향후 CLI 직결) 사이의 와이어 형식입니다. 이 문서는 SPEC의 일부이며 `PR-*` 요구사항이 여기를 가리킵니다. 결정의 근거는 [INTENT.md](INTENT.md) D4–D6에 있습니다.

## 1. 공통

- **인코딩:** 모든 메시지는 UTF-8 JSON 객체입니다. 바이너리(이미지 등)는 nbformat처럼 base64 문자열로 MIME 번들 안에 넣습니다.
- **버전:** 모든 메시지에 `"dkp": 1`이 있습니다. 모르는 버전은 무시(데이터그램)하거나 `error`로 닫습니다(TCP).
- **모르는 필드:** 받는 쪽은 모르는 필드를 무시합니다. 필드 추가는 버전을 올리지 않습니다.
- **런타임 홈:** `DARKPYONIX_HOME`(기본 `~/.darkpyonix`). 아래 경로는 모두 이 아래입니다.

| 경로 | 권한 | 내용 |
|---|---|---|
| `user.key` | 0600 | 32바이트 무작위 키. 없으면 처음 쓰는 쪽이 원자적으로 만듭니다 |
| `kernels/<kernel_id>.json` | 0644 | 발견 보조 등록. §2.4의 announce 본문과 같고 비밀값이 없습니다 |
| `kernels/<kernel_id>.log` | 0600 | 커널 자신의 진단 로그(사용자 출력이 아님) |
| `locks/<kernel_id>.lock` | 0600 | 파일당 커널 하나를 보장하는 OS 잠금 |
| `sockets/<kernel_id>.sock` | 0600 (폴더 0700) | POSIX 전용. 스트림 넘김(§3.7)을 받는 유닉스 도메인 소켓. 발견에는 쓰지 않습니다. 경로는 announce의 `handoff`로 알립니다 |
| `managers/<pid>.json` | 0600 | 매니저의 `url`, `token`, `mode`, `pid`, `started_at` |

## 2. 발견 (UDP 멀티캐스트)

### 2.1 주소

- 그룹 `239.255.68.80`, 포트 `46880`, 인터페이스 `127.0.0.1`, TTL 0(호스트 밖으로 나가지 않음), `IP_MULTICAST_LOOP` 켬.
- 커널과 매니저 모두 `SO_REUSEADDR`(가능하면 `SO_REUSEPORT`)로 같은 포트에 바인드하고 그룹에 가입합니다.
- `DARKPYONIX_DISCOVERY=registry`이면 멀티캐스트를 쓰지 않고 등록 파일만 씁니다.

### 2.2 사용자 태그

`user_tag = hex(SHA-256(user.key))[:16]`. 모든 데이터그램에 들어가고, 자기 태그와 다른 데이터그램은 무시합니다. 같은 기계의 다른 OS 사용자 커널과 섞이지 않게 하는 필터일 뿐 인증이 아닙니다(인증은 §3).

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
  "pid": 41234, "port": 53122, "status": "busy",
  "run_id": "20261003-142233-a1f0",
  "python": {"version": "3.11.9", "implementation": "CPython", "executable": "/usr/bin/python3.11"},
  "kernel_version": "0.1.0", "runs_dir": "/home/u/exp/__runs__/train.py", "started_at": "2026-10-03T05:22:33Z", "host": "macmini",
  "handoff": "/home/u/.darkpyonix/sockets/k_3f9a0c1b2d4e5f607182.sock"
}
```

`handoff`는 스트림 넘김용 유닉스 도메인 소켓 경로입니다(§3.7). Windows에서는 `null`입니다. 경로가 OS 한도(macOS 104바이트, Linux 108바이트)를 넘으면 커널은 `null`을 알리고, 그 커널로 가는 스트림은 매니저가 소켓 쌍으로 퍼 나릅니다(§3.7.3). 구현 대기(#47).

커널은 다음 때 announce를 보냅니다.
- query에 응답할 때(`nonce`를 그대로 돌려줌)
- 시작을 마쳤을 때, 상태(`status`, `run_id`)가 바뀔 때
- 5초마다(생존 신호)

`status`는 `starting | idle | busy | stopping`입니다. 커널은 announce와 같은 본문을 `kernels/<kernel_id>.json`에도 원자적으로 씁니다.

### 2.5 bye (커널 → 그룹)

```json
{"dkp": 1, "op": "bye", "user_tag": "…", "kernel_id": "k_…", "pid": 41234}
```

정상 종료 직전에 보내고 등록 파일을 지웁니다. 비정상 종료 때는 보내지 못하므로 매니저는 15초 동안 announce가 없거나 `pid`가 살아 있지 않은 커널을 목록에서 뺍니다.

### 2.6 커널 ID

```
canonical = realpath(abspath(path))        # Windows는 normcase까지
kernel_id = "k_" + hex(SHA-256(canonical as UTF-8))[:20]
```

같은 파일을 가리키는 심볼릭 링크와 상대 경로는 같은 ID가 됩니다.

## 3. 제어 채널 (루프백 TCP, POSIX는 유닉스 도메인 소켓도)

### 3.1 프레임

```
+----------------+---------------------------+
| length: u32 BE | payload: UTF-8 JSON object |
+----------------+---------------------------+
```

- `length`는 payload 바이트 수이고 최대 64 MiB입니다. 넘으면 받는 쪽이 `error {code: "frame_too_large"}`를 보내고 연결을 닫습니다.
- 커널은 `127.0.0.1`에만 리슨합니다. POSIX 커널은 스트림 넘김용으로 `sockets/<kernel_id>.sock`(0600)에서도 같은 프레임을 받습니다(§3.7.2).

### 3.2 핸드셰이크

```
커널  → {"dkp":1,"op":"hello","kernel_id":"k_…","nonce":"<base64 32B>","kernel_version":"0.1.0"}
클라 → {"dkp":1,"op":"auth","client":{"name":"manager","kind":"manager","pid":123},
        "mac":"<hex HMAC-SHA256(user.key, nonce_bytes + kernel_id.encode())>"}
커널  → {"dkp":1,"op":"welcome","session":"s_…","seq":1042}
   또는 {"dkp":1,"op":"error","code":"auth_failed","message":"…"} 후 연결 종료
```

`seq`는 커널이 마지막으로 낸 이벤트 번호입니다. 핸드셰이크가 5초 안에 끝나지 않으면 커널이 연결을 닫습니다. 비교는 상수 시간(`hmac.compare_digest`)으로 합니다.

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

오류 코드: `auth_failed`, `bad_request`, `unknown_method`, `busy`, `not_found`, `frame_too_large`, `shutting_down`, `internal`.

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
  "kernel_id": "k_…", "path": "/home/u/exp/train.py", "pid": 41234, "port": 53122,
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
  "label": {"permission": "viewer2", "client_id": "dev_8f2c1a", "user": "alice",
            "nickname": "alice-mbp", "share_id": "s_0123456789abcdef"},
  "share": null
}}
```

- `transport`: `fd`(POSIX, 날 소켓), `share`(Windows, 날 소켓), `pump`(소켓 쌍의 한쪽, §3.7.3).
- `request`: 커널은 이 바이트를 소켓에서 읽은 것처럼 다룹니다. 매니저는 요청을 다시 쓰지 않습니다. 요청에 든 토큰(`Authorization`, `?token=`)은 커널이 보지 않습니다.
- `label`: 매니저가 인증 결과로 채웁니다. `share_id`는 공유 토큰으로 들어온 연결에만 있고, 마스터·관리자 토큰이면 `null`입니다. 커널은 `permission`을 무엇을 보낼지 거르는 데만 씁니다(SPEC FR-K9). 인증이나 토큰 검사는 하지 않습니다.
- `share`: Windows에서만 씁니다. 매니저가 `WSADuplicateSocketW(socket, kernel_pid, &info)`로 만든 `WSAPROTOCOL_INFOW`의 base64입니다. 커널은 `socket.fromshare(base64 디코드 값)`으로 엽니다. 파이썬 `socket.share`와 같은 형식입니다. 커널 `pid`는 announce에 있습니다.

#### 3.7.2 POSIX: `SCM_RIGHTS`

`SCM_RIGHTS`는 유닉스 도메인 소켓에서만 됩니다. 그래서 POSIX 커널은 루프백 TCP 제어 채널과 함께 `sockets/<kernel_id>.sock`(announce의 `handoff`)에서도 DKP/1 연결을 받습니다. 같은 핸드셰이크(§3.2)를 거칩니다.

1. 매니저가 `handoff` 경로에 연결하고 핸드셰이크를 마칩니다.
2. `adopt` 요청 프레임(`transport: "fd"`)을 보낸 직후, 1바이트 `b"F"`를 `sendmsg`의 보조 데이터 `(SOL_SOCKET, SCM_RIGHTS, fd)`와 함께 보냅니다.
3. 커널은 프레임을 읽은 뒤 `socket.recvmsg(1, socket.CMSG_LEN(4))`로 FD를 받고 `socket.socket(fileno=fd)`로 엽니다. 3.8에는 `socket.recv_fds`가 없으므로 `recvmsg`를 직접 씁니다.
4. 커널이 `response {stream_id}`를 보내면 매니저는 자기 쪽 FD를 닫습니다. 그 뒤 매니저는 그 연결과 무관합니다.

`adopt`가 TCP 제어 채널로 `transport: "fd"`를 받으면 `bad_request`입니다. FD를 받지 못하면(보조 데이터 없음) `bad_request`이고, 매니저는 그 연결에 `502`를 쓰고 닫습니다.

#### 3.7.3 소켓 쌍과 퍼 나르기 (`pump`)

TLS, HTTP/2, P2P 터널 위의 연결, 그리고 `handoff`가 `null`인 POSIX 커널로 가는 연결에는 넘길 날 소켓이 없습니다. 매니저는 소켓 쌍(POSIX `socketpair(AF_UNIX, SOCK_STREAM)`, Windows는 루프백 TCP 쌍)을 만들어 한쪽을 §3.7.1·§3.7.2와 같이 넘기고(`transport: "pump"`, POSIX는 `fd`로, Windows는 `share`로), 다른 쪽과 클라이언트 연결 사이에서 바이트를 퍼 나릅니다.

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

커널은 파일의 공유 문서 상태(셀, 셀별 버전, 잠금, 접속자)를 들고 있습니다. 아래 메서드와 이벤트는 §3과 같은 채널을 씁니다. 모든 편집 메서드는 `client`를 받습니다. `client`는 `{client_id, nickname, user, avatar?, permission}`이고, 매니저가 토큰에서 채워 넘깁니다.

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

오류 코드 추가: `conflict`, `locked`, `forbidden`.

| event type | data |
|---|---|
| `doc.cell.created` / `doc.cell.updated` / `doc.cell.deleted` / `doc.cell.moved` | `{doc_version, cell, by, request_id?}` (`deleted`는 `cell` 대신 `cell_id`) |
| `doc.lock` / `doc.unlock` | `{doc_version, cell_id, lock?, by, reason?: "released"\|"idle"\|"disconnected", request_id?}` |
| `doc.reloaded` | `{doc_version, cells, cause: "external"}`. 바깥 편집으로 다시 파싱한 뒤 보내는 전체 셀 목록 |
| `doc.conflict` | `{doc_version, cell_id, local: {source, version, by}, disk: {source}}` |
| `presence.update` / `presence.leave` | `{doc_version, client_id, nickname, user, avatar?, permission, focused_cell_id?, focused_at?, cursor?, last_seen, request_id?}` |

`Presence = {client_id, nickname, user, avatar?, permission, focused_cell_id?, focused_at?, cursor?, last_seen}`.

`DocumentCell = {cell_id, index, type, raw_type, title, metadata, source, source_sha256, version, lock?, conflict?}`.

- `source`는 파서(FORMAT §2.4)가 낸 본문 그대로입니다. 표식과 메타데이터 줄은 빠지고, 다음 표식 앞의 빈 줄(앞 셀 본문에 속함)과 줄 끝(`\r\n` 포함)은 남습니다. 클라이언트가 이 값을 바꾸지 않고 돌려보내면 파일 바이트도 바뀌지 않습니다.
- `type`은 정규 타입(소문자, 별칭 해석, 타입 없는 표식은 `code`, 첫 셀은 `preamble`)이고, `raw_type`은 표식의 `[ ]` 안에 쓰인 그대로(없으면 `null`)입니다. 표시는 `type`으로, 원문 보존이 필요하면 `raw_type`을 씁니다.
- `title`은 표식의 제목이고 없으면 `null`입니다.

**스냅숏의 `seq`.** `seq`는 **스냅숏에 이미 반영된 마지막 이벤트의 번호**입니다. 클라이언트는 `subscribe since=seq`(HTTP는 `?since=seq`)로 구독하고, `seq`가 그보다 큰 이벤트만 스냅숏 위에 적용합니다. 커널은 문서 상태를 바꾸는 일과 그 이벤트를 내는 일을 한 잠금 안에서 하고, 스냅숏도 같은 잠금 안에서 만듭니다. 따라서 스냅숏과 경쟁한 편집은 스냅숏에 들어 있거나(`seq` 이하) 구독으로 오거나(`seq` 초과) 둘 중 정확히 하나입니다. 클라이언트가 `seq`가 자기 것 이하인 이벤트를 받으면 무시합니다(재연결로 겹칠 때).

**`doc_version`.** 문서 내용이나 구조가 바뀔 때만 1 늘어납니다: 셀 생성·수정·삭제·이동(`doc.cell.*`)과 다시 읽기(`doc.reloaded`). 잠금 해제로 충돌이 풀려 셀이 바뀌면 그것도 `doc.cell.updated`이므로 늘어납니다. 잠금·해제(`doc.lock`/`doc.unlock`), 충돌 표시(`doc.conflict`), 접속자(`presence.*`)는 늘리지 않지만, 이 이벤트들도 내는 순간의 현재 `doc_version`을 담습니다. 바뀐 것이 없는 수정·이동(같은 값, 같은 위치)은 이벤트를 내지 않고 `doc_version`도 그대로입니다.

**`request_id`.** `doc.*`(snapshot 제외)와 `presence.*` 요청은 클라이언트가 고른 `request_id`(문자열, 1–64자)를 받을 수 있습니다. 그 요청으로 생긴 이벤트는 모두 같은 `request_id`를 그대로 담습니다(예: `doc.unlock`에 최종 소스를 보내면 `doc.cell.updated`와 `doc.unlock` 둘 다, `presence.leave`가 잠금을 풀면 `doc.unlock`과 `presence.leave` 둘 다). 합쳐서 나중에 보내는 커서 `presence.update`는 마지막으로 합쳐진 요청의 `request_id`를 담습니다. 유휴 해제처럼 요청 없이 생긴 이벤트에는 없습니다. 같은 `client_id`를 쓰는 두 창이 자기 편집의 메아리를 구별하는 용도이며, 커널은 값을 해석하지 않습니다. 형식이 틀리면 `bad_request`입니다.

`run.queued`, `run.started`, `run.finished`, `cell.started`, `cell.finished` 이벤트의 `data`에는 `started_by`(client_id, user, nickname)가 들어갑니다. 인터럽트로 끝난 실행에는 `interrupted_by`가 더해집니다(SPEC FR-S6). `run` 메서드는 `cells` 대신 `cell_ids`를 받을 수 있습니다.
