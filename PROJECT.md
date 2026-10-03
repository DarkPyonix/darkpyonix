# PROJECT

## 한 줄 정의

파일 하나에 묶이고 매니저 없이도 사는 표준 라이브러리 전용 파이썬 커널과, 그것을 사람·IDE·에이전트가 함께 쓰게 하는 매니저와 허브입니다.

## 배경

[docs/INTENT.md](docs/INTENT.md) §1에 있습니다. 요약하면 2025년의 "세션이 끊기지 않는 노트북"이 2026년에는 에이전트에게 필요한 네 가지로 바뀌었습니다. 강제 종료 대신 인터럽트, 저절로 남는 실행 기록, 파일당 커널 하나, 그리고 기억이 되는 커널입니다.

## 범위

**포함**
- `darkpyonix/kernel/darkpyonix/kernel`: 파일에 묶인 커널(표준 라이브러리 전용, 3.8+)
- `darkpyonix/manager`: 임시/전용 매니저, HTTP API, `darkpyonix` CLI(Rust 워크스페이스)
- `darkpyonix/kernel/darkpyonix`: 노트북 파일이 쓰는 런타임 API와 셀 파서
- `hub/`: darkpyonix.dev(시그널링, 중계, HTTPS, ash 호스팅)
- 문서: INTENT, SPEC, ARCHITECTURE, PROTOCOL, FORMAT, OpenAPI, 클래스 다이어그램(`darkpyonix.mermaid`)

**제외 (다른 저장소)**
- 대화 우선 워크벤치, 셸 래핑, 컴퓨터 전환, A2A, 원격 브라우저: `darkpyonix-ember`
- 공유 노트북 UI: `darkpyonix-ash`
- 편집기 확장: `vscode-darkpyonix`, `intellij-darkpyonix`, `vscode-darkpyonix-theme`
- 에이전트 하네스(INTENT D13)
- Android·iOS의 노트북 실행 경로(앱 안의 미리 정한 프로세스, INTENT D20): Ember 모바일 앱

## 개발 방식

dioxus-compose와 같은 SDD + TDD입니다. 규칙은 [AGENTS.md](AGENTS.md)에 있습니다. SPEC과 OpenAPI가 먼저이고, 실패하는 테스트, 그다음이 코드입니다. 새 기능은 이슈 → 기능 브랜치 → `develop`으로의 PR로 들어갑니다.

## 마일스톤

사용자 지시(2026-10-02): "10월 셋째쭈 구현까지 기간 당겨야 해. 기간을 당기고 서브 에이전트를 충분히 활용하는걸로 하자." 그래서 쓸 수 있는 수준의 구현은 모두 10월 셋째 주, 2026-10-18까지 끝냅니다. 이 문서에 먼저 기록돼 있던 "11월 안으로" 지시(thisisthepy 리더 경유)를 대체하는 일정이고, 서브 에이전트를 병렬로 써서 기간을 줄입니다. 그 안에 못 들어가는 것은 이유와 함께 범위에서 뺍니다. 품질 기준은 낮추지 않습니다. 아래 목표일은 GitHub 마일스톤의 기한과 같습니다. 이 저장소의 실측 구현 속도가 쌓이면 다시 맞춥니다.

| # | 이름 | 목표일 | 완료 조건 (SPEC) | 의존 |
|---|---|---|---|---|
| M0 | 문서 확정 | 2026-10-03 | INTENT, SPEC, ARCHITECTURE, PROTOCOL, FORMAT, OpenAPI, 클래스 다이어그램이 `develop`에 있음 | — |
| M1 | 커널 코어 | 2026-10-06 | FR-K1~K8, FR-X1~X5, FR-R1~R4, FR-D1(FR-D2는 2026-10-03 Withdrawn), FR-F1, FR-A1, PR-1~4, NFR-K1·K2 | M0 |
| M2 | 매니저·CLI·런타임 API | 2026-10-10 | FR-M1~M3·M5, FR-C1~C2, FR-F2~F6, FR-A2, FR-R5, FR-X6, NFR-K3·K4, NFR-M1~M3 | M1 |
| M3 | 전용 매니저와 공유 | 2026-10-13 | FR-M4, FR-A3, ash가 공유 토큰으로 커널에 붙는 시연 | M2 |
| M3b | 노트북 렌더러 확장 | 2026-10-16 | `vscode-darkpyonix`(Ember 기본 설치)와 `intellij-darkpyonix`가 `.py`/`.pynb` 셀을 그리고, 매니저 API로 `__runs__`의 최근 실행 기록을 셀에 맞춰 보여 줌(FR-R4). 실행·중지와 SSE 실시간 출력. VS Code 먼저, IntelliJ 다음. 코드는 확장 저장소에 있고 여기서는 추적만 함 | M2 |
| M4 | 허브 | 2026-10-18 | FR-H1~H7, NFR-H1. 허브 API는 Cloudflare Worker(`hub/worker/`), 계정은 GitHub 로그인(FR-H6), 릴레이는 `relay.darkpyonix.dev`(`hub/server/`, FR-H3). 릴레이를 VPS에 둘지 Container로 옮길지는 Ember NFR-N1의 QAD 켬/끔 측정으로 정함(Q9). 사용자 준비물: Cloudflare 존·API 토큰, GitHub OAuth App, 릴레이 VPS | Q1(조건부 결정됨, iroh로 시작), Ember M5와 함께 |

**10월 셋째 주 범위에서 뺀 것과 이유**
- (변수 체크포인트·복원은 범위가 아닙니다. 변수는 실행 기록에 남은 코드로 재현합니다. INTENT 1.2 D.)

`vscode-darkpyonix`와 `intellij-darkpyonix`가 매니저 API에 붙는 작업은 M2 이후에 시작할 수 있습니다. ember는 M1~M4가 커널 API에 의존하지 않습니다(Ember 구현 담당 확인).

## 열린 질문

| ID | 질문 | 상태 |
|---|---|---|
| Q1 | 컴퓨터 사이 P2P 전송 | 재검토(2026-10-03). 사용자 지적: tailcat은 모바일에서 앱(런타임)이 둘이 되고, "직접 구현이 몇 달"이라는 리더 판단은 틀렸다. 비교: **tailcat**(Go, 사이드카 또는 gomobile → Rust 앱 안에 런타임 둘, iOS는 별도 프로세스 불가) / **[iroh](https://github.com/n0-computer/iroh) 1.0**(Rust, 같은 프로세스, QUIC 홀펀칭 직결 약 90%, 릴레이 자체 운영, MIT·Apache-2.0, 모바일 바인딩) / **[rustunnel](https://github.com/joaoh82/rustunnel)**(Rust, TLS WebSocket 릴레이형 터널·서브도메인·HTTPS, 홀펀칭 없음, **AGPL-3.0**) / **직접 구현**(UDP 반사·홀펀칭·Noise 암호화·릴레이 폴백. 릴레이가 안전망이 되므로 몇 주 규모, 대칭 NAT·포트 매핑·로밍 품질은 점진적). 리더 권장: 전송은 iroh로 시작하고 darkpyonix.dev가 iroh-relay와 주소 디렉터리를 운영, 전송 계층은 인터페이스 뒤에 둬서 직접 구현으로 바꿀 수 있게 함. rustunnel은 허브의 공개 HTTPS 엣지(FR-H5) 참고 구현으로만 검토(AGPL이라 클라이언트에 링크하지 않음). **결정(2026-10-03, 조건부)**: 사용자 "P2P를 iroh로 가는건 일단 허용하는데 그게 품질이 별로면 아예 직접 구현하는거도 고민해봐". 품질 기준(직결 성공률, 지연, 수립·전환 시간, 모바일 배터리)은 Ember SPEC에 숫자로 두고, 미달이면 직접 구현으로 바꿉니다. 그래서 전송 계층은 인터페이스 뒤에 둡니다 |
| Q2 | OpenAI 계정 로그인과 ChatGPT 플랜 사용량 | 결정(2026-10-03, 사용자: "OpenAI 로그인은 엠버 서버에서 사용자가 자체적으로 하는걸로 하고 허브는 깃허브 로그인으로 하자."). "Sign in with ChatGPT"(OAuth 2.0 + OIDC, PKCE, 루프백 리디렉트, 동적 클라이언트 등록)로 오픈소스·로컬 호스팅 앱은 사용자의 ChatGPT 플랜으로 Responses API를 쓸 수 있습니다(`store:false`, `stream:true` 필수, 앱별 주간 상한). 그래서 OpenAI 로그인과 플랜 사용은 사용자가 직접 띄운 ember server에서만 합니다. darkpyonix.dev 허브는 OpenAI를 쓰지 않고 GitHub 로그인으로 계정을 만듭니다(SPEC FR-H6, INTENT D15). 허브가 ember의 OpenAI 토큰을 받지 않는 이유(audience)는 INTENT D15에 있습니다 |
| Q3 | 멀티캐스트가 막힌 환경의 보조 발견 | 결정(2026-10-03, 사용자: "멀티캐스트가 막힌 특수한 이상한 상황은 가정하지 말고, wsl 안에서 돌고 있는거에 윈도우에서 연결해야 할 이유도 없어. 안드로이드랑 iOS의 경우 PyREPL 구현처럼 별도 미리 정의된 프로세스 내에서만 노트북이 실행 가능하도록 하면 되는거야."). 보조 발견은 두지 않습니다. 막힌 환경과 WSL↔Windows는 지원 범위 밖, 모바일은 앱 안의 미리 정한 프로세스(INTENT D4, D20, SPEC FR-D3) |
| Q4 | 기존 Jupyter 도구와의 호환(Jupyter Server REST 흉내)이 필요한지 | 보류 |
| Q5 | `__runs__/`의 보관 정책 | Git 추적은 결정됨(기본 포함, 원하면 폴더째 제외, INTENT D8). 용량 상한과 오래된 실행 정리는 FR-R5의 사이드카 외에 두지 않음 |
| Q6 | `restart hard`에서 OS 잠금이 잠깐 풀리는 틈을 어떻게 막을지(재실행 전 잠금 파일 핸들 상속 등) | M1에서 결정 |
| Q7 | grid 레이아웃을 여닫는 태그가 필요한지, horizontal/vertical 전환으로 충분한지 | 추천안: 태그 없이 전환만(SPEC FR-F7 `Draft`, [제안](docs/proposals/cells-parallel-interop.md) §2). 이슈 #7에서 인용 답 대기, 답이 다르면 답을 따름 |
| Q8 | `parallel`/`concurrent`와 언어 interop 셀의 실행 의미 | 추천안을 SPEC FR-X7~X12, FR-F8~F13에 `Draft`로 반영(asyncio+스레드 기본, 프로세스는 옵션이며 10-18 범위 밖). 이슈 #5, #7 답 대기 |
| Q9 | iroh 릴레이를 어디서 돌릴지: 작은 VPS(릴레이 + QAD) 또는 Cloudflare Container(WebSocket만, QAD 없음) | 권장(2026-10-03): VPS로 시작. Ember NFR-N1 측정을 VPS 위에서 QAD 켬/끔 두 번 하고, QAD를 끈 직접 경로 성공률도 85% 이상이면 Container로 옮김(SPEC FR-H3). 사용자 확인 대기 |
| Q10 | 2025 인증 계열의 범위와 경로. 2025 문서의 요약표(`/auth`…, 파일 구분 없음)와 상세 페이지(`/kernels/{kernel_id}/…`)가 다릅니다 | 결정(2026-10-04). 비밀번호와 마스터 토큰은 사용자: "매니저 단위 맞아." 요약표 넷은 `/api/auth…`이고 파일마다 비밀번호는 지웁니다(SPEC FR-A4). 상세의 `/kernels/{kernel_id}/tokens/…`는 사용자: "아니, 그게 아니고 해당 커널에 접근 가능한 토큰을 말하는거야. 매니저가 여러개잖아." 그래서 커널에 묶이고 어느 매니저로든 통합니다(SPEC FR-A5, FR-A6) |
| Q11 | 초기 토큰(2025, 인증 없음)을 바깥에 열린 전용 매니저에서 누가 먼저 받을 수 있는지 | 결정(2026-10-04, 사용자: "초기 토큰은 애초에 열 때 토큰을 발급했을건데 뭐가 문제야? 토큰이 없으면 연결이 안되잖아. 초기 토큰 발급은 건드리지 마."). 2025 그대로 둡니다. "한 번만 발급"과 `409 already_initialized`는 지웠습니다(SPEC FR-A4) |
| Q12 | 매니저와 커널 사이 인증 | 결정(2026-10-04, 사용자: "같은 컴퓨터의 같은 계정만 통과시켜야 해.", 방법은 "OS가 알려주는 상대 계정 확인"). POSIX 유닉스 도메인 소켓의 상대 UID, Windows 이름 있는 파이프의 상대 SID를 봅니다. `user.key`와 HMAC은 지웁니다. 스트림 넘김도 같은 소켓·파이프로 합니다(INTENT D5, SPEC FR-A1, PROTOCOL §3.2, 구현 대기 #55) |
| Q13 | 2025 `user_permission: "write"`를 `viewer3`와 `admin`으로 읽은 것(셀 편집과 잠금도 `viewer3`부터) | 결정(2026-10-04, 사용자: "권한 이름 저따위 아니거든? 시멘틱하게 다시 추론해 … 실행 권한이랑 코드 수정 권한은 다른거야. 권한 등급 개념 아니니까 이상한 방향으로 가지 마."). 사용자가 승인한 능력 다섯(`read` 코드 보기, `history` 실행 기록과 출력, `execute` 실행과 인터럽트, `edit` 코드 수정과 셀 잠금, `manage` 공유 설정)의 집합으로 바꿨습니다. 등급과 순서는 없습니다. 2025 `"write"`는 실행이면 `execute`, 셀 편집·잠금이면 `edit`, 공유 설정의 `"admin"`은 `manage`입니다(INTENT D18, SPEC FR-A3) |
| Q14 | 발견 멀티캐스트 그룹 주소와 포트. `239.255.68.80:46880`은 리더가 처음 정한 값입니다 | 결정(2026-10-04, 사용자: "그대로 사용"). 이 값을 씁니다(INTENT D4) |
| Q15 | 매니저 인증의 세부. (1) 2025 `GET /auth`의 "비밀번호를 세션에 넣어서"를 `Authorization: Basic`(사용자 이름 비움)으로 읽은 것. (2) `{token_type}`을 능력 조합(`read+history`)으로 적은 것. (3) 초기 토큰은 매니저에 비밀번호가 없을 때만 비밀번호를 정할 수 있게 한 것(SPEC FR-A4) | 결정(2026-10-04, 사용자: "이대로"). 셋 모두 이대로 둡니다 |
| Q16 | 커널 접근 토큰 저장 위치. 초안은 런타임 홈의 `tokens/<kernel_id>.json`에 두고 같은 계정의 모든 매니저가 함께 읽고 썼습니다 | 결정(2026-10-04, 사용자: "커널이 들고 있게"). 커널이 토큰의 주인이고, 매니저는 제어 채널(`tokens.*`)로 확인을 요청합니다. 커널이 떠 있지 않으면 매니저가 먼저 띄웁니다. 커널이 스스로 영속화해 2025 수명 규칙을 지킵니다. 저장소의 정확한 위치는 [provisional]이고 #48에서 정합니다(INTENT D17, SPEC FR-A5, FR-K10, PROTOCOL §6) |
| Q17 | 능력 다섯에 들지 않은 작업의 대응. 커널 시작·재시작·종료·강제 종료는 `execute`, 네임스페이스 조회와 실행 대기 결과는 `history`, 커널 목록·상태·접속자 표시와 포커스는 `read`로 두었습니다(INTENT D18, SPEC FR-A3) | 임시 결정(2026-10-04, 사용자: "일단 그렇게 해둬 나중에 내가 다시 볼게"). 사용자 재검토 예정 |
