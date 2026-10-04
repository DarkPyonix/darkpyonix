# FORMAT — DarkPyonix 노트북 파일

`.py`와 `.pynb` 파일 형식입니다. 원본 예시는 인용(dlsdyd)이 이슈 #6에 첨부한 [`examples/darkpyonix_format.py`](examples/darkpyonix_format.py)입니다. 관련 논의는 이슈 #3(마크업 조사), #5(언어·데이터 형식), #6(binding 셀 분리), #7(레이아웃과 병렬 실행 분리)에 있습니다.

## 1. 원칙

1. **파일은 유효한 파이썬입니다.** 셀 경계와 메타데이터는 주석이고, 마크다운과 파라미터는 `darkpyonix` 런타임 API 호출입니다. `python file.py`로 실행하면 커널에서 "전체 실행"한 것과 같은 결과를 냅니다(SPEC FR-F5).
2. **출력은 파일에 넣지 않습니다.** 출력은 `__runs__/`의 실행 기록에 남고(INTENT D8), 클라이언트가 파일을 열 때 맵핑합니다.
3. **`.pynb`는 `.py`와 문법이 같습니다.** 확장자는 편집기가 노트북 화면으로 열지, 일반 코드 편집기로 열지를 정하는 신호일 뿐입니다. 커널은 둘을 똑같이 다룹니다.

## 2. 구조

```python
"""Module docstring (선택)"""
import darkpyonix                          # 프리앰블: 첫 셀 표식 전의 모든 줄


# %% [code]                               ← 셀 표식
# @width: 1fr                              ← 셀 메타데이터 (표식 바로 아래 연속된 줄)
import torch
```

### 2.1 프리앰블

첫 셀 표식 앞의 줄은 프리앰블입니다. 커널은 프리앰블을 "0번 셀"(`type: preamble`)로 보고 어떤 실행에서든 가장 먼저, 한 번만 실행합니다. 프리앰블에는 import와 docstring만 두기를 권합니다.

### 2.2 셀 표식

```
^# %%(?:[ \t]+(?P<title>[^\[\n]*?))?(?:[ \t]*\[(?P<type>[A-Za-z_][A-Za-z0-9_-]*)\])?[ \t]*$
```

- `# %%`로 시작하는 줄 하나가 새 셀을 엽니다. 다음 표식이나 파일 끝까지가 그 셀입니다.
- `[type]`이 없으면 `code`입니다.
- `# %% 데이터 로드 [code]`처럼 제목을 줄 수 있습니다.
- 줄 끝 공백은 무시합니다. 예시 파일의 `# %% [code]  `도 표식입니다.
- Jupytext 퍼센트 형식(`# %% [markdown]`)과 경계 표기가 같습니다. 다만 마크다운 본문을 주석이 아니라 `darkpyonix.markdown()` 호출로 쓴다는 점이 다릅니다(§3.2).
- 파서는 Jupytext처럼 줄 단위로 동작하므로, 삼중 따옴표 문자열 안에 있는 `# %%` 줄도 셀 표식으로 봅니다.

### 2.3 셀 메타데이터

표식 바로 다음에 이어지는 `# @key: value` 줄이 그 셀의 메타데이터입니다. 처음으로 이 형식이 아닌 줄이 나오면 메타데이터가 끝납니다. 값은 문자열이며, JSON으로 해석되면 그 값을 씁니다.

| 키 | 의미 | 예 |
|---|---|---|
| `width` | 가로 배치에서 셀 너비(CSS grid 트랙) | `1fr`, `320px` |
| `layout` | 이 셀부터 시작하는 배치 | `horizontal`, `vertical`, `grid` |
| `collapsed` | 처음에 접어서 표시 | `true` |
| `auto_run` | 커널을 연결하면 자동 실행 | `true` |
| `id` | 클라이언트가 붙이는 안정 ID(선택) | `"c-3f2a"` |

모르는 키는 보존하고 무시합니다.

### 2.4 셀 식별

파일에는 출력이 없으므로 실행 기록과 셀을 잇는 고리가 필요합니다. 셀은 세 가지로 식별합니다.

- `index`: 프리앰블을 0으로 하는 순서
- `source_sha256`: 표식과 메타데이터 줄을 뺀 본문의 SHA-256(줄 끝 `\r\n`은 `\n`으로 정규화)
  - 셀 사이의 빈 줄은 앞 셀의 본문에 속하지만, 해시는 본문 끝의 빈 줄(공백만 있는 줄 포함)과 마지막 줄바꿈을 뺀 뒤 계산합니다. 다음 표식 앞에 빈 줄을 넣거나 빼도 셀의 실행 기록이 낡은 것으로 바뀌지 않게 하기 위해서입니다.
- `id`: 메타데이터에 있으면 그것

클라이언트는 `id` → `source_sha256` → `index` 순서로 기록을 맵핑합니다(SPEC FR-R4).

## 3. 셀 타입

| 타입 | 상태 | 실행 의미 | 표시 |
|---|---|---|---|
| `code` | 정의됨 | 본문을 그대로 실행. 마지막 식의 값은 `execute_result`로 내고 `_`에 저장 | 코드 셀 |
| `markdown` | 정의됨 | `darkpyonix.markdown("""…""", silent=…)` 호출을 실행 | 렌더된 마크다운 |
| `argparse` | 정의됨 | `darkpyonix.params.get(...)` 호출 실행. 값은 실행 요청의 `params` 또는 명령줄 인자에서 옴 | 파라미터 폼 |
| `binding` | 정의됨 | `@darkpyonix.binding`이 붙은 정의를 격리 네임스페이스에서 다시 평가(이슈 #6) | 코드 셀 |
| `shell` | 정의됨 | `darkpyonix.run_command("""…""")` 실행. 출력은 스트림으로 냄 | 터미널 출력 |
| `parallel` | 예약 | 다음 셀들을 병렬로 실행하는 묶음의 시작(이슈 #7) | 가로 배치 |
| `concurrent` | 예약 | 병렬 묶음을 실행하는 셀. 예시 파일의 `concorrunt`는 이 타입의 오타로 보고 별칭으로 받음 | |
| `cinterop` / `cppinterop` / `rustinterop` | 예약 | C(cython) / C++(cppyy) / Rust(maturin·PyO3) 코드를 즉시 컴파일해 파이썬 네임스페이스에 등록(이슈 #5) | 코드 셀 |
| `sql`, `toml`, `yaml`, `json` | 예약 | 평가 결과를 파이썬 네임스페이스에 바로 노출(이슈 #5) | |
| (모르는 타입) | — | `code`처럼 실행하고 원래 타입을 보존 | 코드 셀 |

"예약"은 문법상 받아들이고 보존하지만 v1 커널이 특별한 의미를 주지 않는다는 뜻입니다. 예약 타입의 본문도 유효한 파이썬이므로 `code`처럼 실행됩니다. `darkpyonix.run_cinterop()` 같은 API는 v1에서 `NotImplementedError`를 냅니다.

예약 타입(`parallel`, `concurrent`, interop, 데이터 형식)과 `layout`의 실행 의미·계산 규칙은 SPEC FR-X7~X12, FR-F7~F13(`Draft`)과 [proposals/cells-parallel-interop.md](proposals/cells-parallel-interop.md)에 있습니다. 합의(`Agreed`)되면 이 표와 §2.3을 그 내용으로 바꿉니다.

### 3.1 레이아웃과 병렬은 다릅니다 (이슈 #7)

- `layout: horizontal`은 **화면 배치**입니다. 셀은 여전히 차례대로 실행되고 실행 버튼도 셀마다 따로 있습니다.
- `parallel`은 **실행 의미**입니다. 묶인 셀이 동시에 실행되고, 화면에서는 보통 가로로 놓입니다.
- 가로로 놓인 셀은 병렬일 수도, 단순 가로 배치일 수도 있습니다. 그래서 둘을 다른 키로 둡니다. grid를 여닫는 태그를 둘지는 이슈 #7에서 결정합니다(PROJECT Q7).

### 3.2 마크다운

```python
# %% [markdown]
darkpyonix.markdown("""
# 제목
본문
""", silent=True)
```

- `silent=True`이면 실행 기록에 출력을 남기지 않고 화면에만 렌더합니다. 기본값은 `False`입니다.
- `python file.py`로 실행하면 `darkpyonix.markdown`은 아무것도 출력하지 않습니다. 원래 동작을 바꾸지 않기 위해서입니다.
- 예시 파일의 `slient=`는 `silent=`의 오타입니다. 런타임은 모르는 키워드 인자를 오류 없이 무시하고 경고만 남깁니다.

### 3.3 파라미터 (`argparse` 셀)

```python
MODEL_ID = darkpyonix.params.get("model_id", default=0, choices=["default_model", "swin_t"])
MODEL_HEIGHT = darkpyonix.params.get("model_height", default=50, range=(30, 100, 1))
```

| 인자 | 의미 |
|---|---|
| `name` | 파라미터 이름. 명령줄에서는 `--name` |
| `default` | `choices`가 있으면 **인덱스**, 없으면 값 |
| `choices` | 허용 값 목록. 클라이언트는 선택 상자로 표시 |
| `range` | `(min, max, step)`. 클라이언트는 슬라이더로 표시 |
| `type` | 생략하면 `default`(또는 `choices`) 값의 타입으로 변환 |
| `help` | 설명 |

값은 다음 순서로 정해집니다. 실행 요청의 `params` → 명령줄 `--name value` → `default`. 범위를 벗어나거나 `choices`에 없는 값은 `ValueError`로 셀을 실패시킵니다.

### 3.4 binding (이슈 #6)

`[binding]` 셀의 정의는 `[code]` 셀의 변수를 볼 수 없어야 합니다. `@darkpyonix.binding`은 정의 시점의 전역 스냅숏(import와 앞선 binding만 있음)에서 그 정의를 다시 평가합니다. 그래서 `[code]` 변수를 참조하면 `NameError`가 납니다. 다른 데코레이터는 보존하고 `binding`만 벗겨 냅니다. 바인딩끼리 서로 앞뒤로 참조하는 문제와 순서 의존성은 이슈 #6의 미결 항목을 그대로 따릅니다.

## 4. 런타임 API 요약

| API | 커널 안 | `python file.py` |
|---|---|---|
| `darkpyonix.markdown(text, silent=False)` | `text/markdown` 출력 | 아무것도 안 함 |
| `darkpyonix.params.get(...)` | 실행 요청의 `params` 우선 | 명령줄 인자 |
| `@darkpyonix.binding` | 격리 재평가 | 격리 재평가(같음) |
| `darkpyonix.run_command(cmd, check=False)` | 출력을 스트림 출력으로 | 표준 출력으로 |
| `darkpyonix.display(obj)` | `display_data` 출력 | `print(repr(obj))` |
| `darkpyonix.uv.add/remove`, `darkpyonix.pip.install` | 커널 인터프리터 환경에 설치 | 같음 |
| `darkpyonix.run_parallel(*aws)` | 코루틴들을 모아 실행 | 같음 |
| `darkpyonix.run_cinterop/run_cppinterop/run_rustinterop` | `NotImplementedError` (예약) | 같음 |

런타임 API는 표준 라이브러리만 씁니다. 커널 밖에서 쓰려면 `pip install darkpyonix`로 설치합니다. 커널 안에서는 설치하지 않아도 커널이 경로를 넣어 줍니다(INTENT D3).
