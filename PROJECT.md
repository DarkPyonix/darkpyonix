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

날짜는 2026-10-03에 리더가 정한 목표입니다. 근거는 남은 작업량과 의존 관계이고, 이 저장소에 아직 구현 이력이 없어서 실측 속도는 반영되지 않았습니다. M1을 마친 뒤 실제 속도로 다시 맞춥니다.

| # | 이름 | 목표일 | 완료 조건 (SPEC) | 의존 |
|---|---|---|---|---|
| M0 | 문서 확정 | 2026-10-03 | INTENT, SPEC, ARCHITECTURE, PROTOCOL, FORMAT, OpenAPI, 클래스 다이어그램이 `develop`에 있음 | — |
| M1 | 커널 코어 | 2026-10-10 | FR-K1~K8, FR-X1~X5, FR-R1~R4, FR-D1~D2, FR-F1, FR-A1, PR-1~4, NFR-K1·K2 | M0 |
| M2 | 매니저·CLI·런타임 API | 2026-10-17 | FR-M1~M3·M5, FR-C1~C2, FR-F2~F6, FR-A2, FR-R5, FR-X6, NFR-K3·K4, NFR-M1~M3 | M1 |
| M3 | 전용 매니저와 공유 | 2026-10-24 | FR-M4, FR-A3, ash가 공유 토큰으로 커널에 붙는 시연 | M2 |
| M4 | 허브 | 미정 | FR-H1~H6, NFR-H1 | Q1, Q2 결정 |
| M5 | 기억으로서의 커널 | 미정 | 네임스페이스 체크포인트·복원 설계(INTENT 1.2 D) | M2 |

`vscode-darkpyonix`와 `intellij-darkpyonix`가 매니저 API에 붙는 작업은 M2 이후에 시작할 수 있습니다. ember는 M1~M4가 커널 API에 의존하지 않습니다(Ember 구현 담당 확인).

## 열린 질문

| ID | 질문 | 상태 |
|---|---|---|
| Q1 | 컴퓨터 사이 P2P를 직접 만든 Rust 터널로 할지 "tailcat"으로 할지. 사용자가 쓴 "tailcat"이 Tailscale을 뜻하는지도 확인해야 합니다 | 사용자 확인 필요 |
| Q2 | OpenAI 계정 로그인을 제3자 서비스가 쓸 수 있는지, Codex 외 Chat 사용량을 쓰는 페이지가 약관상 가능한지 | 조사 필요 |
| Q3 | Windows 루프백 멀티캐스트의 신뢰성. 안 되면 Windows 기본값을 등록 파일 발견으로 둘지 | M1에서 측정 |
| Q4 | 기존 Jupyter 도구와의 호환(Jupyter Server REST 흉내)이 필요한지 | 보류 |
| Q5 | `__runs__/`의 보관 정책(용량 상한, 오래된 실행 정리)과 Git 추적 여부 | 사용자 확인 필요 |
| Q6 | `restart hard`에서 OS 잠금이 잠깐 풀리는 틈을 어떻게 막을지(재실행 전 잠금 파일 핸들 상속 등) | M1에서 결정 |
| Q7 | grid 레이아웃을 여닫는 태그가 필요한지, horizontal/vertical 전환으로 충분한지 | 이슈 #7 |
| Q8 | `parallel`/`concurrent`와 언어 interop 셀의 실행 의미 | 이슈 #5, #7 |
