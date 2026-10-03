# ARCHITECTURE

DarkPyonix 전체의 프로세스 배치와 데이터 흐름입니다. 이 저장소가 맡는 부분(커널, 매니저, 허브)은 자세히, 다른 저장소가 맡는 부분은 경계만 그립니다. 요구사항 ID는 [SPEC.md](SPEC.md), 와이어 형식은 [PROTOCOL.md](PROTOCOL.md), HTTP API는 [api/](api/)에 있습니다.

## 1. 전체 배치

사용자의 비유를 그대로 씁니다. **프로젝트는 회사, 컴퓨터는 지부**입니다. 메인 서버 한 대가 본사이고, 모든 대화와 계정이 거기 있습니다. 에이전트는 지부를 옮겨 다니며 일합니다.

```mermaid
flowchart LR
  subgraph Clients["클라이언트"]
    EMBER["darkpyonix-ember<br/>런처·대화 화면 (dioxus-compose)"]
    IDE["IDE 창<br/>ember / VS Code / Gateway"]
    ASH["darkpyonix-ash<br/>공유 노트북 (WASM)"]
  end

  subgraph Main["메인 서버 (맥미니·라즈베리파이)"]
    ES["ember server<br/>세션 저장소·계정·실행 라우터·A2A·브라우저 프로필"]
    CLIS["Claude Code / Codex /<br/>Antigravity / OMP (래핑된 셸)"]
    DM["darkpyonix manager<br/>(dedicated)"]
  end

  subgraph Branch["지부 컴퓨터 (여러 대)"]
    NODE["ember node (실행 데몬)<br/>파일·명령·PTY·브라우저 출구"]
    VSC["VS Code 서버<br/>(proxy/ 래핑)"]
    EM["darkpyonix manager<br/>(ephemeral)"]
    K1["kernel: train.py"]
    K2["kernel: eval.py"]
  end

  FRONT["darkpyonix.dev/ (메인 페이지, GitHub Pages)<br/>랜딩 + ash 웹 앱"]
  HUB["hub: api.darkpyonix.dev (Cloudflare Worker)<br/>계정·기기·주소·공유·이름·노트북 보관<br/>+ relay.darkpyonix.dev (iroh 릴레이·QAD)"]

  EMBER -- "HTTPS (P2P 터널 위)" --> ES
  IDE -- "워크벤치" --> VSC
  IDE -- "노트북 확장: HTTP/SSE" --> EM
  ASH -. "darkpyonix.dev에서 받음" .- FRONT
  ASH -- "API (CORS)" --> HUB
  ASH -- "공유 토큰 (릴레이·P2P)" --> DM
  ES --- CLIS
  CLIS -- 래핑된 셸 명령 --> NODE
  ES <-. P2P 터널 .-> NODE
  ES -. 시그널링 .-> HUB
  NODE -. 시그널링 .-> HUB
  NODE --> EM
  EM -- DKP/1 --> K1
  EM -- DKP/1 --> K2
  DM -- 터널 경유 --> EM
```

| 구성 요소 | 저장소 | 역할 |
|---|---|---|
| **kernel** | 이 저장소 `darkpyonix/kernel/darkpyonix/kernel` | 파일 하나에 묶인 실행 프로세스. 표준 라이브러리만 씁니다 |
| **manager** | 이 저장소 `darkpyonix/manager`(Rust) | 커널을 찾고 띄우는 HTTP 앞단. 임시/전용 두 모드 |
| **runtime API** | 이 저장소 `darkpyonix/kernel/darkpyonix` | 노트북 파일이 `import darkpyonix`로 쓰는 API |
| **hub** | 이 저장소 `hub/worker/`, `hub/server/` | api.darkpyonix.dev(Cloudflare Worker): GitHub 계정, 기기 등록, iroh 주소 디렉터리, 공유 해석, HTTPS 이름, 노트북 보관(D1 + R2). relay.darkpyonix.dev(Rust): iroh 릴레이와 QUIC 주소 발견(INTENT D15와 그 개정, §7) |
| **메인 페이지** | `darkpyonix-ash` | darkpyonix.dev 루트: 랜딩과 ash 웹 앱. ash CI가 빌드해 조직 GitHub Pages 사이트(`DarkPyonix.github.io`)에 올립니다. 정적 파일만 내고 위 API를 부릅니다(SPEC FR-H12) |
| ember server / ember node / 클라이언트 | `darkpyonix-ember` | 대화 우선 워크벤치, 셸 래핑, 컴퓨터 전환, A2A, 원격 브라우저 |
| ash | `darkpyonix-ash` | Starboard 포크. darkpyonix.dev 메인 페이지로 배포됩니다. 호스팅한 노트북을 커널 없이 보여 주고, 공유 토큰으로 공유된 커널에 접근합니다 |
| 노트북 렌더러 | `vscode-darkpyonix`, `intellij-darkpyonix` | `.py`/`.pynb` 셀 표시와 실행 기록 맵핑. 그 컴퓨터의 매니저에 붙습니다 |
| IDE 창 워크벤치 | `darkpyonix-ember` (`proxy/`) | 각 컴퓨터의 VS Code 서버를 감싸서 냅니다 |
| 테마 | `vscode-darkpyonix-theme` | 불사조 디자인 테마 |

클라이언트와 ember server 사이, ember server와 각 컴퓨터 사이는 모두 P2P 터널(darkpyonix.dev가 홀펀칭을 돕고 실패하면 중계) 위의 HTTPS입니다. 에이전트 셸 래핑과 머신 이동의 세부(트랜스크립트, 무효화, A2A)는 ember 문서가 정의합니다. 이 문서는 그 경계에서 커널 스택이 무엇을 제공하는지만 다룹니다(§6).

## 2. 한 컴퓨터 안: 커널과 매니저

```mermaid
flowchart TB
  subgraph Home["~/.darkpyonix  (DARKPYONIX_HOME)"]
    KEY["user.key (0600)"]
    REG["kernels/&lt;kernel_id&gt;.json<br/>발견 보조 등록 (비밀값 없음)"]
    LOCK["locks/&lt;kernel_id&gt;.lock<br/>파일당 커널 하나 (OS 잠금)"]
    MAN["managers/&lt;pid&gt;.json (0600)<br/>매니저 주소·토큰"]
  end

  CLI["darkpyonix CLI<br/>(에이전트·사람)"] -->|managers/*.json 탐색, 없으면 띄움| M1
  IDE2["IDE 확장"] --> M1
  M1["manager (ephemeral)<br/>127.0.0.1:임의 포트"]
  M2["manager (dedicated)<br/>외부 접속용"]
  M1 -->|DKP/1 + HMAC| K["kernel: train.py<br/>127.0.0.1:임의 포트"]
  M2 -->|DKP/1 + HMAC| K
  M1 <-->|멀티캐스트 질의/공지| MC(("239.255.68.80:46880<br/>루프백"))
  K <--> MC
  K --- LOCK
  K --- REG
  K -->|기록| RUNS["train.py 옆<br/>__runs__/train.py/&lt;run_id&gt;.ipynb"]
```

- **커널은 누구의 자식도 아닙니다.** 매니저가 띄우더라도 새 세션으로 분리되어 시작합니다(INTENT D2). 매니저가 끝나도 커널은 남습니다.
- **매니저는 여러 개가 동시에 떠 있어도 됩니다.** 모두 같은 커널을 발견하고, 같은 사용자 키로 인증합니다(INTENT D5). 에이전트가 기존 매니저의 토큰을 모르거나 떠 있는 매니저가 없으면 CLI가 새 임시 매니저를 띄웁니다.
- **파일당 커널 하나**는 커널이 `locks/<kernel_id>.lock`에 거는 OS 잠금이 보장합니다. 프로세스가 죽으면 OS가 잠금을 풀므로 남은 잠금 파일 때문에 막히는 일이 없습니다.

### 2.1 커널 내부 스레드

```mermaid
flowchart LR
  subgraph KP["kernel 프로세스"]
    MT["메인 스레드<br/>Executor: 셀 실행"]
    CT["control 스레드<br/>DKP 서버 (selectors)"]
    DT["discovery 스레드<br/>멀티캐스트 응답·공지"]
    OT["output 스레드<br/>캡처 큐 → 브로드캐스트·기록"]
    FT["fd 리더 스레드<br/>fd 1/2 파이프 (C 확장 출력)"]
  end
  CT -->|실행 요청 큐| MT
  CT -->|interrupt_main| MT
  MT -->|sys.stdout/stderr, display| Q[(출력 큐)]
  FT --> Q
  Q --> OT
  OT -->|event 프레임| CT
  OT -->|원자적 저장, 1초 간격| LOG["__runs__/…/run.ipynb"]
```

사용자 코드는 메인 스레드에서만 돕니다(INTENT D12). 출력은 큐에 넣고 바로 돌아오므로 느린 클라이언트나 디스크가 학습 루프를 붙잡지 않습니다. 인터럽트는 control 스레드가 `_thread.interrupt_main()`으로 메인 스레드에 `KeyboardInterrupt`를 일으킵니다. POSIX와 Windows가 같은 경로를 씁니다.

## 3. 커널 수명

```mermaid
sequenceDiagram
  participant A as 에이전트 (CLI)
  participant M as manager
  participant K as kernel (train.py)
  participant FS as ~/.darkpyonix

  A->>M: POST /api/v1/kernels {path: train.py}
  M->>M: kernel_id = H(정규화 경로)
  M->>K: 멀티캐스트 query {kernel_id}
  alt 이미 살아 있음
    K-->>M: announce {port, status}
  else 없음
    M->>K: 부트스트랩으로 실행 (분리 세션)
    K->>FS: locks/<id>.lock 잠금, kernels/<id>.json 기록
    K-->>M: announce {port, status: idle}
  end
  M->>K: TCP 연결, hello/auth(HMAC)/welcome
  M-->>A: 200 Kernel
  A->>M: POST /kernels/{id}/runs {mode: all}
  M->>K: request run
  K-->>M: event run.started, cell.*, output…
  M-->>A: SSE 스트림
  Note over M: 클라이언트가 모두 떠나고 유휴 시간이 지나면<br/>임시 매니저는 끝남. 커널은 계속 실행
  A->>M: (나중에, 새 매니저) GET /kernels/{id}/runs/latest
```

같은 파일에 대해 두 번째 `run`이 들어오면 기본 정책은 거절(`409 busy`, 현재 실행 정보 포함)입니다. 요청에 `on_busy: queue`를 주면 대기열에 넣습니다(SPEC FR-X3). 에이전트가 같은 파일을 두 번 돌리는 문제(INTENT 1.2 C)를 여기서 막습니다.

## 4. 실행 기록

```
experiments/
├─ train.py
└─ __runs__/
   └─ train.py/
      ├─ 20261003-142233-a1f0.ipynb   ← 실행 하나 = 노트북 하나 (nbformat 4)
      ├─ 20261003-150102-77c2.ipynb
      └─ index.json                    ← 실행 목록 요약 (최신순)
```

- 실행 하나(`run`)는 요청 한 번입니다. 파일 전체 실행이든 셀 몇 개든 같습니다.
- 커널은 실행 중에도 최대 1초 간격으로 기록을 원자적으로 다시 씁니다(임시 파일 후 이름 변경). 커널이 죽어도 직전까지의 출력이 남습니다.
- 셀마다 `metadata.darkpyonix`에 소스 해시와 셀 위치를 남깁니다. 클라이언트는 파일을 열 때 최신 기록을 이 해시로 셀에 맵핑합니다. 소스가 바뀐 셀은 "이전 소스의 결과"로 표시할 수 있습니다(SPEC FR-R4).
- 커널 안에서는 `__runs__.current`, `__runs__.latest`, `__runs__[run_id]`로 같은 내용을 dict로 읽고, `run.cells[1].text`처럼 속성으로도 읽습니다(SPEC FR-R3).

## 5. 원격 접근과 공유

```mermaid
flowchart LR
  ASH2["ash (브라우저)<br/>darkpyonix.dev/s/&lt;share&gt;#&lt;token&gt;"] -->|GET /v1/shares/&lt;share&gt;| HUB2["api.darkpyonix.dev"]
  ASH2 -->|릴레이(손님 통행권) 또는 홀펀칭| DM2["dedicated manager<br/>(메인 서버 또는 지부)"]
  DM2 -->|권한: viewer1·2·3| K3["kernel"]
```

- 외부 접근은 항상 **전용 매니저**를 거칩니다. 커널은 루프백에만 열립니다.
- 공유 토큰은 커널(=파일)마다 발급하며 권한은 2025년 설계를 따릅니다. `viewer1` 코드만, `viewer2` 코드와 실행 기록, `viewer3` 코드·실행 기록·실행, `admin` 전부입니다(SPEC FR-A3).
- 허브는 연결을 이어 줄 뿐이고, 내용은 끝단 사이에서 암호화합니다(SPEC NFR-H1, Draft).

## 6. ember와의 경계

Ember의 에이전트 CLI는 메인 서버에서 돌고, 실행 라우터가 도구 호출을 세션의 현재 컴퓨터에 있는 ember node(실행 데몬)로 보냅니다(Ember ARCHITECTURE §1.2). 그 셸 안에서 에이전트가 쓰는 것은 평범한 CLI입니다.

| 에이전트가 하던 일 | DarkPyonix로 바뀐 형태 |
|---|---|
| `python train.py > log.txt &` | `darkpyonix run train.py --detach` (기록은 자동) |
| `pkill -f train.py` | `darkpyonix stop train.py` (인터럽트, 상태 유지) |
| `tail -f log.txt` | `darkpyonix logs train.py --follow` |
| 같은 파일을 실수로 다시 실행 | `409 busy`와 현재 실행 정보가 돌아옴 |
| 무엇을 계산했는지 잊음 | `darkpyonix vars train.py`, 커널 안의 `__runs__` |

ember server는 지부의 매니저 HTTP API를 터널 너머에서 그대로 부릅니다. 대화 화면이 실행 상태와 최신 출력을 보여 줄 때 쓰는 것도 이 API입니다. 커널 스택은 대화, 계정, 머신 전환을 알지 못합니다.

## 7. darkpyonix.dev 호스트 배치

INTENT D15 개정(2026-10-03, 제안): 동적 기능은 서브도메인, 루트는 그것을 띄우는 정적 메인 페이지입니다. 프로젝트 가이드는 지금처럼 `darkpyonix.dev/<저장소>/`에 있습니다.

| 호스트 | 내는 것 | 구현 위치 | 배포 |
|---|---|---|---|
| `darkpyonix.dev/` | 메인 페이지: 랜딩, ash 웹 앱(`/n/<id>`, `/s/<id>#<token>`, `/link`, `/new`), 렌더러 `/sandbox/`, Flathub 검증 파일 | darkpyonix-ash 웹 빌드 | 조직 GitHub Pages `DarkPyonix/DarkPyonix.github.io`(`main` 루트). ash CI가 배포 키로 빌드 결과를 커밋. 동적 경로는 `404.html` 앱, CSP는 meta |
| `darkpyonix.dev/<저장소>/` | 프로젝트 가이드(예: `/dioxus-compose/`) | 각 저장소 | 각 저장소의 GitHub Pages 프로젝트 사이트 |
| `api.darkpyonix.dev` | 허브 API: 로그인, 기기, 주소 디렉터리(pkarr), 공유 해석, 이름·ACME TXT, 설정, 노트북 보관, 릴레이 입장 판정 | `hub/worker/`(TypeScript) | Cloudflare Worker custom domain, D1(메타데이터), R2(노트북 본문, 비공개), Cron |
| `relay.darkpyonix.dev` | iroh 릴레이, QAD(UDP 7842) | `hub/server/`(Rust) | VPS, DNS only(SPEC FR-H3) |
| `<name>.darkpyonix.dev` | 사용자 메인 서버 | 사용자의 ember server | 사용자 기기, 인증서는 ACME DNS-01(SPEC FR-H5) |

```mermaid
flowchart LR
  B["브라우저"] -->|정적 파일| F["darkpyonix.dev/<br/>메인 페이지 (ash, GitHub Pages)"]
  B -->|정적 파일| GP["darkpyonix.dev/&lt;저장소&gt;/<br/>가이드 (프로젝트 Pages)"]
  CI["darkpyonix-ash CI"] -->|빌드 결과 커밋| F
  F -. "sandbox iframe (opaque)" .- SB["/sandbox/ 렌더러"]
  B -->|"CORS + __Host-dp_session"| A["api.darkpyonix.dev<br/>Worker"]
  A --- D1[("D1<br/>계정·기기·공유·노트북 메타데이터")]
  A --- R2[("R2 (비공개)<br/>노트북 본문")]
  B -->|"손님 통행권"| R["relay.darkpyonix.dev"]
  R -->|입장 판정| A
  R <-->|암호문| M["사용자 기기의<br/>전용 매니저"]
  G["GitHub OAuth"] -->|/auth/callback| A
```

- **쿠키는 API 호스트에만 있습니다.** `__Host-` 쿠키는 호스트 전용이라 `api.darkpyonix.dev`에만 붙고, 메인 페이지는 `credentials: "include"`로 부릅니다. 두 호스트는 같은 사이트라 `SameSite=Lax` 쿠키가 실립니다. CORS는 메인 페이지 출처 하나에만 엽니다(SPEC FR-H13). 로그인은 API 호스트로 최상위 이동했다가 메인 페이지 경로로 돌아오고, URL 조각(공유 토큰)은 `sessionStorage`에 두었다 되살려 API로 보내지 않습니다.
- **GitHub Pages라서.** 응답 헤더를 못 정하므로 CSP는 meta로 두고, 클릭재킹은 `SameSite=Lax`(끼워진 페이지의 API 요청에 쿠키가 없음)와 앱의 프레임 검사로 막습니다. 교차 출처 격리가 필요한 ash 화면만 ash의 서비스 워커로 COOP/COEP를 덧붙입니다. 노트북 보기와 라이브 열기는 격리가 필요 없습니다(SPEC FR-H12).
- **남의 내용은 샌드박스에서만.** 노트북 출력의 HTML·스크립트와 ash가 브라우저에서 돌리는 코드는 렌더러 페이지 `/sandbox/`를 `allow-same-origin` 없이 읽은 iframe(opaque 출처)에서 돌고, 내용은 `postMessage`로 받습니다(SPEC NFR-H3). 렌더러를 별도 등록 도메인으로 옮길지는 열린 질문입니다(PROJECT Q10).

### 7.1 노트북 올리기와 읽기 전용 보기

```mermaid
sequenceDiagram
  participant C as 기기 (ember·CLI) 또는 브라우저
  participant A as api.darkpyonix.dev
  participant R2 as R2
  participant V as 보는 사람 (darkpyonix.dev/n/<id>)

  C->>A: POST /v1/notebooks {title, visibility}
  A-->>C: 201 {notebook_id: n_…}
  C->>A: POST /v1/notebooks/n_…/versions (source=train.py, run=<run_id>.ipynb)
  A->>R2: 본문 쓰기
  A-->>C: 201 {version: 1}
  V->>A: GET /v1/notebooks/n_… , …/versions/latest/source, …/run
  A-->>V: 메타데이터, 본문 (nosniff, CSP sandbox)
  Note over V: FORMAT으로 셀 분할, FR-R4 규칙으로 출력 맵핑<br/>HTML 출력은 샌드박스 iframe. 커널·릴레이 없음
```

### 7.2 라이브 커널로 열기

```mermaid
sequenceDiagram
  participant V as 브라우저 (darkpyonix.dev/n/<id>#<token>)
  participant A as api.darkpyonix.dev
  participant R as relay.darkpyonix.dev
  participant M as 주인 기기의 전용 매니저

  V->>A: GET /v1/notebooks/n_… (share_id 확인)
  V->>A: GET /v1/shares/s_…
  A-->>V: endpoint_id, relay_url, 손님 통행권
  V->>R: 통행권으로 입장 (R → A 입장 판정)
  V->>M: iroh 연결 + 공유 토큰 (허브는 토큰을 보지 않음)
  M-->>V: 권한(viewer1~admin)에 따라 문서·실행 (SPEC FR-A3)
```
