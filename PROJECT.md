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

## 개발 방식

dioxus-compose와 같은 SDD + TDD입니다. 규칙은 [AGENTS.md](AGENTS.md)에 있습니다. SPEC과 OpenAPI가 먼저이고, 실패하는 테스트, 그다음이 코드입니다. 새 기능은 이슈 → 기능 브랜치 → `develop`으로의 PR로 들어갑니다.

## 마일스톤

사용자 지시(2026-10-03, thisisthepy 리더 경유): "실제 사용할 수 있는 정도의 수준으로 개발 완료는 전부 2026년 11월 안으로 당겨야 한다." 그래서 쓸 수 있는 수준의 완료는 모두 2026-11-30 이전이고, 그 안에 못 들어가는 것은 이유와 함께 범위에서 뺍니다. 품질 기준은 낮추지 않습니다. 날짜는 2026-10-03에 리더가 정한 목표입니다. 근거는 남은 작업량과 의존 관계이고, 이 저장소에 아직 구현 이력이 없어서 실측 속도는 반영되지 않았습니다. M1을 마친 뒤 실제 속도로 다시 맞춥니다.

| # | 이름 | 목표일 | 완료 조건 (SPEC) | 의존 |
|---|---|---|---|---|
| M0 | 문서 확정 | 2026-10-03 | INTENT, SPEC, ARCHITECTURE, PROTOCOL, FORMAT, OpenAPI, 클래스 다이어그램이 `develop`에 있음 | — |
| M1 | 커널 코어 | 2026-10-10 | FR-K1~K8, FR-X1~X5, FR-R1~R4, FR-D1~D2, FR-F1, FR-A1, PR-1~4, NFR-K1·K2 | M0 |
| M2 | 매니저·CLI·런타임 API | 2026-10-17 | FR-M1~M3·M5, FR-C1~C2, FR-F2~F6, FR-A2, FR-R5, FR-X6, NFR-K3·K4, NFR-M1~M3 | M1 |
| M3 | 전용 매니저와 공유 | 2026-10-24 | FR-M4, FR-A3, ash가 공유 토큰으로 커널에 붙는 시연 | M2 |
| M4 | 허브 | 2026-11-20 | FR-H1~H7, NFR-H1. 허브 API는 Cloudflare Worker(`hub/worker/`), 계정은 GitHub 로그인(FR-H6), 릴레이는 `relay.darkpyonix.dev`(`hub/server/`, FR-H3). 릴레이를 VPS에 둘지 Container로 옮길지는 Ember NFR-N1의 QAD 켬/끔 측정으로 정함(Q9). 사용자 준비물: Cloudflare 존·API 토큰, GitHub OAuth App, 릴레이 VPS | Q1을 2026-10-24까지 결정, Ember M5와 함께 |

**11월 범위에서 뺀 것과 이유**
- (변수 체크포인트·복원은 범위가 아닙니다. 변수는 실행 기록에 남은 코드로 재현합니다. INTENT 1.2 D.)
- `parallel`/`concurrent`와 interop 셀의 실행 의미(Q7, Q8): 문법은 받아들이고 보존하지만, 실행 의미는 이슈 #5와 #7의 결정이 먼저입니다.

`vscode-darkpyonix`와 `intellij-darkpyonix`가 매니저 API에 붙는 작업은 M2 이후에 시작할 수 있습니다. ember는 M1~M4가 커널 API에 의존하지 않습니다(Ember 구현 담당 확인).

## 열린 질문

| ID | 질문 | 상태 |
|---|---|---|
| Q1 | 컴퓨터 사이 P2P 전송 | 재검토(2026-10-03). 사용자 지적: tailcat은 모바일에서 앱(런타임)이 둘이 되고, "직접 구현이 몇 달"이라는 리더 판단은 틀렸다. 비교: **tailcat**(Go, 사이드카 또는 gomobile → Rust 앱 안에 런타임 둘, iOS는 별도 프로세스 불가) / **[iroh](https://github.com/n0-computer/iroh) 1.0**(Rust, 같은 프로세스, QUIC 홀펀칭 직결 약 90%, 릴레이 자체 운영, MIT·Apache-2.0, 모바일 바인딩) / **[rustunnel](https://github.com/joaoh82/rustunnel)**(Rust, TLS WebSocket 릴레이형 터널·서브도메인·HTTPS, 홀펀칭 없음, **AGPL-3.0**) / **직접 구현**(UDP 반사·홀펀칭·Noise 암호화·릴레이 폴백. 릴레이가 안전망이 되므로 몇 주 규모, 대칭 NAT·포트 매핑·로밍 품질은 점진적). 리더 권장: 전송은 iroh로 시작하고 darkpyonix.dev가 iroh-relay와 주소 디렉터리를 운영, 전송 계층은 인터페이스 뒤에 둬서 직접 구현으로 바꿀 수 있게 함. rustunnel은 허브의 공개 HTTPS 엣지(FR-H5) 참고 구현으로만 검토(AGPL이라 클라이언트에 링크하지 않음). **결정(2026-10-03, 조건부)**: 사용자 "P2P를 iroh로 가는건 일단 허용하는데 그게 품질이 별로면 아예 직접 구현하는거도 고민해봐". 품질 기준(직결 성공률, 지연, 수립·전환 시간, 모바일 배터리)은 Ember SPEC에 숫자로 두고, 미달이면 직접 구현으로 바꿉니다. 그래서 전송 계층은 인터페이스 뒤에 둡니다 |
| Q2 | OpenAI 계정 로그인과 ChatGPT 플랜 사용량 | 결정(2026-10-03, 사용자: "OpenAI 로그인은 엠버 서버에서 사용자가 자체적으로 하는걸로 하고 허브는 깃허브 로그인으로 하자."). "Sign in with ChatGPT"(OAuth 2.0 + OIDC, PKCE, 루프백 리디렉트, 동적 클라이언트 등록)로 오픈소스·로컬 호스팅 앱은 사용자의 ChatGPT 플랜으로 Responses API를 쓸 수 있습니다(`store:false`, `stream:true` 필수, 앱별 주간 상한). 그래서 OpenAI 로그인과 플랜 사용은 사용자가 직접 띄운 ember server에서만 합니다. darkpyonix.dev 허브는 OpenAI를 쓰지 않고 GitHub 로그인으로 계정을 만듭니다(SPEC FR-H6, INTENT D15). 허브가 ember의 OpenAI 토큰을 받지 않는 이유(audience)는 INTENT D15에 있습니다 |
| Q3 | Windows 루프백 멀티캐스트의 신뢰성. 안 되면 Windows 기본값을 등록 파일 발견으로 둘지 | M1에서 측정 |
| Q4 | 기존 Jupyter 도구와의 호환(Jupyter Server REST 흉내)이 필요한지 | 보류 |
| Q5 | `__runs__/`의 보관 정책 | Git 추적은 결정됨(기본 포함, 원하면 폴더째 제외, INTENT D8). 용량 상한과 오래된 실행 정리는 FR-R5의 사이드카 외에 두지 않음 |
| Q6 | `restart hard`에서 OS 잠금이 잠깐 풀리는 틈을 어떻게 막을지(재실행 전 잠금 파일 핸들 상속 등) | M1에서 결정 |
| Q7 | grid 레이아웃을 여닫는 태그가 필요한지, horizontal/vertical 전환으로 충분한지 | 이슈 #7 |
| Q8 | `parallel`/`concurrent`와 언어 interop 셀의 실행 의미 | 이슈 #5, #7 |
| Q9 | iroh 릴레이를 어디서 돌릴지: 작은 VPS(릴레이 + QAD) 또는 Cloudflare Container(WebSocket만, QAD 없음) | 권장(2026-10-03): VPS로 시작. Ember NFR-N1 측정을 VPS 위에서 QAD 켬/끔 두 번 하고, QAD를 끈 직접 경로 성공률도 85% 이상이면 Container로 옮김(SPEC FR-H3). 사용자 확인 대기 |
| Q10 | darkpyonix.dev 역할 분담과 ash 노트북 호스팅(INTENT D15 개정, SPEC FR-H12~H19, NFR-H3) | 제안(2026-10-03, 사용자 결정: "ash 노트북 호스팅까지", "동적 기능은 서브 도메인으로 ... 루트 darkpyonix.dev는 가이드가 아니야"). 루트는 정적 프런트(Workers 정적 자산), API는 `api.darkpyonix.dev`, 가이드는 `docs.darkpyonix.dev`. 사용자 확인 대기: 프런트를 빌드·배포할 저장소, 가이드 이동, 노트북 한도 값 |
