# docs/guide

DarkPyonix 사용자 가이드입니다. 사이트 생성기 없이 손으로 쓴 정적 HTML/CSS이고 빌드 단계가 없습니다.
`.github/workflows/pages.yml`이 `main`에 들어온 이 디렉터리를 그대로 GitHub Pages에 올립니다.
주소는 `https://darkpyonix.dev/darkpyonix/`입니다.

프로젝트 문서(README, PROJECT, INTENT, SPEC, ARCHITECTURE, PROTOCOL, FORMAT)는 한국어로만 쓰지만,
이 가이드는 사용자용이라 **영어와 한국어를 같은 내용으로** 둡니다. 영어가 기본 진입 언어입니다.
형식과 스타일은 `compose-rust`, `dioxus-compose`의 `docs/guide/`와 같습니다.

## 구조

```
docs/guide/
├─ index.html            # 진입점. 기본은 en/, 이전에 한국어를 본 독자는 ko/로 보냅니다
├─ robots.txt, sitemap.xml
├─ assets/
│  ├─ style.css          # 유일한 스타일시트 (compose-rust 가이드와 같음)
│  └─ guide.js           # 유일한 자바스크립트 (테마, 사이드바, 코드 복사)
├─ en/
│  ├─ index.html             개요 (랜딩)
│  ├─ getting-started.html   설치와 첫 실행
│  ├─ notebook-format.html   노트북 파일 (docs/FORMAT.md)
│  ├─ cli.html               darkpyonix CLI (SPEC FR-C2)
│  ├─ run-logs.html          실행 기록 (__runs__)
│  ├─ manager-api.html       매니저 HTTP API (docs/api/manager.openapi.yaml)
│  ├─ clients.html           편집기, ash, 에이전트
│  └─ hub.html               darkpyonix.dev 허브 (계획)
└─ ko/                   # 파일 이름은 en/과 1:1로 같습니다
```

**파일 이름은 두 언어에서 같아야 합니다.** 언어 전환 링크가 같은 이름의 파일을 가리키기 때문입니다.

## 규칙

- 사실은 현재형으로 씁니다. 아직 없는 것은 상태 배지와 이슈 번호를 붙입니다.
  - `<span class="pill works">implemented</span>` / `구현`
  - `<span class="pill planned">planned #N</span>` / `계획 #N`
  - `<span class="pill partial">changing #N</span>` / `변경 중 #N`: 지금 동작하지만 재설계로 바뀌는 부분
- 재설계 중인 부분(#47 스트림 넘김, #48 비밀번호 인증, #49 권한·WebSocket 동기화, #50 등록 파일 제거)이
  `develop`에 들어오면 해당 페이지의 "변경 중" 표시와 설명을 고칩니다.
- em-dash(U+2014)를 쓰지 않습니다. 파이썬 설치·실행 예시는 uv, ppp, tcl만 씁니다.
- 코드 예시는 SPEC, FORMAT, OpenAPI, CLI 인자 정의(`darkpyonix/manager/crates/darkpyonix/src/args.rs`)와
  대조한 것만 씁니다.
- 한국어는 합니다체로, 영어를 직역하지 않고 같은 내용을 씁니다.

## 페이지 추가하기

1. 비슷한 `en/` 페이지를 복사합니다. 머리말, 사이드바, 푸터 구조를 그대로 둡니다.
2. `<title>`, `<meta name="description">`, `canonical`, `hreflang` 세 줄, 상단 언어 전환 링크를 새 이름으로 맞춥니다.
3. 같은 이름으로 `ko/` 페이지를 만들고 `<html lang="ko">`로 바꿉니다.
4. 모든 페이지의 사이드바에 새 항목을 넣고, 앞뒤 페이지의 `.pagenav`를 고칩니다.
5. `sitemap.xml`에 en, ko 두 URL을 더합니다.

## 로컬에서 확인하기

```bash
uv run --no-project python -m http.server -d docs/guide 8000
# http://localhost:8000/ → en/으로 이동합니다
```
