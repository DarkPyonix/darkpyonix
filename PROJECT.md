# PROJECT

## 한 줄 정의

파일 하나에 묶이고 매니저 없이도 사는 표준 라이브러리 전용 파이썬 커널과, 그것을 사람·IDE·에이전트가 함께 쓰게 하는 매니저와 허브입니다.

## 배경

[docs/INTENT.md](docs/INTENT.md) §1에 있습니다. 요약하면 2025년의 "세션이 끊기지 않는 노트북"이 2026년에는 에이전트에게 필요한 네 가지로 바뀌었습니다. 강제 종료 대신 인터럽트, 저절로 남는 실행 기록, 파일당 커널 하나, 그리고 기억이 되는 커널입니다.

## 범위

**포함**
- `kernel/darkpyonix/kernel`: 파일에 묶인 커널(표준 라이브러리 전용, 3.8+)
- `kernel/darkpyonix/manager`: 임시/전용 매니저, HTTP API, `darkpyonix` CLI
- `kernel/darkpyonix`: 노트북 파일이 쓰는 런타임 API와 셀 파서
- `hub/`: darkpyonix.dev(시그널링, 중계, HTTPS, ash 호스팅)
- 문서: INTENT, SPEC, ARCHITECTURE, PROTOCOL, FORMAT, OpenAPI, 클래스 다이어그램(`darkpyonix.mermaid`)

**제외 (다른 저장소)**
- 대화 우선 워크벤치, 셸 래핑, 컴퓨터 전환, A2A, 원격 브라우저: `darkpyonix-ember`
- 공유 노트북 UI: `darkpyonix-ash`
- 편집기 확장: `vscode-darkpyonix`, `intellij-darkpyonix`, `vscode-darkpyonix-theme`
- 에이전트 하네스(INTENT D13)

## 개발 방식

dioxus-compose와 같은 SDD + TDD입니다. 규칙은 [CLAUDE.md](CLAUDE.md)에 있습니다. SPEC과 OpenAPI가 먼저이고, 실패하는 테스트, 그다음이 코드입니다. 새 기능은 이슈 → 기능 브랜치 → `develop`으로의 PR로 들어갑니다.

## 마일스톤

사용자 지시(2026-10-03, thisisthepy 리더 경유): "실제 사용할 수 있는 정도의 수준으로 개발 완료는 전부 2026년 11월 안으로 당겨야 한다." 그래서 쓸 수 있는 수준의 완료는 모두 2026-11-30 이전이고, 그 안에 못 들어가는 것은 이유와 함께 범위에서 뺍니다. 품질 기준은 낮추지 않습니다. 날짜는 2026-10-03에 리더가 정한 목표입니다. 근거는 남은 작업량과 의존 관계이고, 이 저장소에 아직 구현 이력이 없어서 실측 속도는 반영되지 않았습니다. M1을 마친 뒤 실제 속도로 다시 맞춥니다.

| # | 이름 | 목표일 | 완료 조건 (SPEC) | 의존 |
|---|---|---|---|---|
| M0 | 문서 확정 | 2026-10-03 | INTENT, SPEC, ARCHITECTURE, PROTOCOL, FORMAT, OpenAPI, 클래스 다이어그램이 `develop`에 있음 | — |
| M1 | 커널 코어 | 2026-10-10 | FR-K1~K8, FR-X1~X5, FR-R1~R4, FR-D1~D2, FR-F1, FR-A1, PR-1~4, NFR-K1·K2 | M0 |
| M2 | 매니저·CLI·런타임 API | 2026-10-17 | FR-M1~M3·M5, FR-C1~C2, FR-F2~F6, FR-A2, FR-R5, FR-X6, NFR-K3·K4, NFR-M1~M3 | M1 |
| M3 | 전용 매니저와 공유 | 2026-10-24 | FR-M4, FR-A3, ash가 공유 토큰으로 커널에 붙는 시연 | M2 |
| M4 | 허브 | 2026-11-20 | FR-H1~H5, NFR-H1. OpenAI 로그인(FR-H6)은 허브가 아니라 ember server에서 함(Q2) | Q1을 2026-10-24까지 결정, Ember M5와 함께 |
| M5 | 기억으로서의 커널 | 11월 범위 밖 | 네임스페이스 체크포인트·복원 설계(INTENT 1.2 D) | M2 |

**11월 범위에서 뺀 것과 이유**
- M5 체크포인트·복원: 살아 있는 네임스페이스와 실행 기록(`__runs__`)만으로도 에이전트가 상태를 조회할 수 있습니다(FR-K6, FR-R3). 복원은 pickle 없이 설계해야 해서(INTENT §4) 연구가 먼저 필요합니다.
- `parallel`/`concurrent`와 interop 셀의 실행 의미(Q7, Q8): 문법은 받아들이고 보존하지만, 실행 의미는 이슈 #5와 #7의 결정이 먼저입니다.
- FR-H6 허브의 OpenAI 로그인: 원격 호스팅 서비스는 OpenAI 관심 신청서와 승인이 필요하므로 11월에 넣지 않습니다. 사용자 플랜 사용은 ember server(로컬 호스팅)에서 합니다(Q2).

`vscode-darkpyonix`와 `intellij-darkpyonix`가 매니저 API에 붙는 작업은 M2 이후에 시작할 수 있습니다. ember는 M1~M4가 커널 API에 의존하지 않습니다(Ember 구현 담당 확인).

## 열린 질문

| ID | 질문 | 상태 |
|---|---|---|
| Q1 | 컴퓨터 사이 P2P 전송 | 재검토(2026-10-03). 사용자 지적: tailcat은 모바일에서 앱(런타임)이 둘이 되고, "직접 구현이 몇 달"이라는 리더 판단은 틀렸다. 비교: **tailcat**(Go, 사이드카 또는 gomobile → Rust 앱 안에 런타임 둘, iOS는 별도 프로세스 불가) / **[iroh](https://github.com/n0-computer/iroh) 1.0**(Rust, 같은 프로세스, QUIC 홀펀칭 직결 약 90%, 릴레이 자체 운영, MIT·Apache-2.0, 모바일 바인딩) / **[rustunnel](https://github.com/joaoh82/rustunnel)**(Rust, TLS WebSocket 릴레이형 터널·서브도메인·HTTPS, 홀펀칭 없음, **AGPL-3.0**) / **직접 구현**(UDP 반사·홀펀칭·Noise 암호화·릴레이 폴백. 릴레이가 안전망이 되므로 몇 주 규모, 대칭 NAT·포트 매핑·로밍 품질은 점진적). 리더 권장: 전송은 iroh로 시작하고 darkpyonix.dev가 iroh-relay와 주소 디렉터리를 운영, 전송 계층은 인터페이스 뒤에 둬서 직접 구현으로 바꿀 수 있게 함. rustunnel은 허브의 공개 HTTPS 엣지(FR-H5) 참고 구현으로만 검토(AGPL이라 클라이언트에 링크하지 않음). 사용자 확인 대기 |
| Q2 | OpenAI 계정 로그인과 ChatGPT 플랜 사용량 | 조사 완료(2026-10-03). "Sign in with ChatGPT"(OAuth 2.0 + OIDC, PKCE, 루프백 리디렉트)로 오픈소스·로컬 호스팅 앱은 사용자의 ChatGPT 플랜으로 Responses API를 쓸 수 있습니다(`store:false`, `stream:true` 필수, 앱별 주간 상한). 유료·원격 호스팅 앱은 관심 신청서가 필요합니다. 그래서 플랜 사용은 사용자가 직접 띄운 ember server에서 하고, darkpyonix.dev 허브는 이 경로를 쓰지 않습니다 |
| Q3 | Windows 루프백 멀티캐스트의 신뢰성. 안 되면 Windows 기본값을 등록 파일 발견으로 둘지 | M1에서 측정 |
| Q4 | 기존 Jupyter 도구와의 호환(Jupyter Server REST 흉내)이 필요한지 | 보류 |
| Q5 | `__runs__/`의 보관 정책 | Git 추적은 결정됨(기본 포함, 원하면 폴더째 제외, INTENT D8). 용량 상한과 오래된 실행 정리는 FR-R5의 사이드카 외에 두지 않음 |
| Q6 | `restart hard`에서 OS 잠금이 잠깐 풀리는 틈을 어떻게 막을지(재실행 전 잠금 파일 핸들 상속 등) | M1에서 결정 |
| Q7 | grid 레이아웃을 여닫는 태그가 필요한지, horizontal/vertical 전환으로 충분한지 | 이슈 #7 |
| Q8 | `parallel`/`concurrent`와 언어 interop 셀의 실행 의미 | 이슈 #5, #7 |
