# 제안 — 레이아웃, 병렬 셀, interop 셀, 데이터 형식 셀

- 상태: **제안(Draft)**. 합의되면 FORMAT·SPEC·INTENT에 옮기고 이 문서는 근거로 남깁니다.
- 대상: PROJECT Q7(grid 여닫는 태그), Q8(`parallel`/`concurrent`와 interop 셀의 실행 의미)
- 목표일: 2026-10-18 구현
- 관련: 이슈 [#5](https://github.com/DarkPyonix/darkpyonix/issues/5), [#6](https://github.com/DarkPyonix/darkpyonix/issues/6), [#7](https://github.com/DarkPyonix/darkpyonix/issues/7), [#8](https://github.com/DarkPyonix/darkpyonix/issues/8), [#11](https://github.com/DarkPyonix/darkpyonix/issues/11), [#17](https://github.com/DarkPyonix/darkpyonix/issues/17), FORMAT §2.3·§3·§3.1, SPEC FR-X1~X6·FR-F1~F6·FR-K5·NFR-K2

## 0. 요약

| 주제 | 결정 상태 | 이 문서의 안 |
|---|---|---|
| 레이아웃과 병렬의 분리 | **결정됨**(이슈 #7 본문) | 레이아웃은 화면 배치이고 병렬은 실행 의미입니다. 둘은 다른 키로 둡니다 |
| grid 여닫는 태그 | **기록 없음**(이슈 #7에 질문으로만 있음) | 태그를 두지 않습니다. 셀마다 `# @layout: horizontal \| vertical` 전환만 쓰고, 가로 줄을 쌓으면 grid가 됩니다(§2) |
| 병렬 묶음의 경계 | 참조 파일(이슈 #6 첨부)에만 있음 | `[parallel]`이 열고 `[concurrent]`가 닫습니다(§3) |
| 병렬 실행 방식 | 참조 파일의 `__co_routines__`와 `darkpyonix.run_parallel`, 주석 "automatically await" | 메인 스레드의 asyncio로 awaitable을 모아 실행하고, 일반 callable은 스레드에서 돌립니다. 프로세스는 옵션으로 두고 나중에 합니다(§3) |
| C / C++ / Rust interop | 이슈 #5: "셀 블록에서 바로 평가해 파이썬 전역 네임스페이스에 등록" | `darkpyonix.run_cinterop`(Cython), `run_cppinterop`(cppyy), `run_rustinterop`(maturin·PyO3). 결과는 해시 키로 캐시합니다. 툴체인은 사용자 인터프리터에 깔려 있을 때만 씁니다(§4) |
| toml/yaml/json/sql | 이슈 #5: "평가하면 바로 네임스페이스에서 접근", "`_` 대신 저장할 변수를 바꿀 수 있게", "DarkPyonix 밖에서는 `_`를 직접 대입하는 코드가 필요할 수 있음" | `darkpyonix.toml/yaml/json/sql("""…""", target="_")`. 반환하면서 호출한 쪽 전역에도 묶습니다. 그래서 `python file.py`에서도 똑같습니다(§5) |

grid 태그와 병렬 실행 방식은 인용(dlsdyd)에게 이슈 #7에서 확인을 요청해 두었습니다([#7 comment](https://github.com/DarkPyonix/darkpyonix/issues/7#issuecomment-5962995715)). 답이 이 문서와 다르면 답을 따르고 이 문서를 고칩니다.

## 1. 찾은 근거

### 1.1 이슈 #7 "grid, horizontal, vertical은 parallel이랑 분리" (dlsdyd, 2026-06-02 20:27 UTC, 댓글 없음)

> - 병렬 실행과 단순 ui를 horizontal로 지정하는 것은 다른 동작
> - 병렬 실행은 horizontal일 뿐 아니라 동시에 실행되는 것이고 horizontal은 그냥 차례대로 실행되며 실행버튼이 분리됨
> - grid 구성에서도 가로 구성은 parallel일 수도 horizontal일 수도
> - grid 태그 열고 닫음이 필요할 지 아니면 단순히 horizontal / vertical 전환 만으로 충분할 지 생각이 필요

- 앞의 세 줄은 결정입니다. 마지막 줄은 열린 질문이고, 이슈에도 커밋에도 답이 없습니다.
- 타임라인에는 2026-10-02 b-re-w의 M0 커밋(`1e40d15`)이 참조로 걸리고, #17과 교차 참조된 것만 있습니다.
- 사용자는 "grid 여닫는 건 안 하기로 했을 것"이라고 기억합니다. 이를 뒷받침하거나 뒤집는 기록은 찾지 못했습니다.

### 1.2 이슈 #6 첨부 `darkpyonix_format.py` (dlsdyd, 2026-06-02 18:55 UTC)

첨부 파일은 [`docs/examples/darkpyonix_format.py`](../examples/darkpyonix_format.py)와 바이트 단위로 같습니다(다시 내려받아 `diff`로 확인). 이 파일이 **#7보다 1시간 30분 먼저** 올라왔습니다. 그래서 #7은 이 파일의 병렬 표기를 보고 나서 "레이아웃과 병렬을 분리하자"고 한 것으로 읽힙니다. 병렬 부분은 이렇습니다.

```python
# %% [parallel]
__co_routines__ = []

# %% [code]
# @width: 1fr
__co_routines__.append(data.load("imagenet", split="train"))

# %% [code]
# @width: 1fr
__co_routines__.append(data.load("imagenet", split="val"))

# %% [concorrunt]
if __name__  == '__main__':
    darkpyonix.run_parallel(*__co_routines__)  # automatically await corrutines
```

여기서 읽히는 것은 다음과 같습니다.
- 묶음은 `[parallel]` 셀이 열고 `[concurrent]` 셀이 닫습니다. **병렬 묶음에는 사실상 여닫는 표식이 이미 있습니다.**
- 묶인 셀은 일을 직접 하지 않고 awaitable을 모으기만 합니다. 실제 동시 실행은 `run_parallel`에서 일어납니다. 그래서 `python file.py`로 돌려도 의미가 같습니다(FR-F5).
- 가로 배치는 `# @width: 1fr`로만 표시합니다. 레이아웃 키는 셀 메타데이터입니다.
- `if __name__ == '__main__':` 가드는 multiprocessing(spawn)을 쓸 때 필요한 관용구입니다. 프로세스 실행도 염두에 두었을 가능성이 있습니다.
- interop 셀은 `darkpyonix.run_cinterop("""…""")`처럼 문자열 안에 다른 언어를 넣습니다. 마크다운 셀과 같은 방식입니다.

### 1.3 이슈 #5 "Define language and data format support" (dlsdyd, 2026-06-02, 댓글 없음)

> **C, C++, Rust** — directly evaluated from cell block and registered to python global namespace, so that python process is able to access to just-in-time compiled c functions/cpp classes.
>
> **SQL output storage** — Currently, the evaluation result of every cell is stored in the underscore variable (`_`). Make this behavior configurable so the user can change where SQL output is stored. ⚠️ Caveat: For environments where DarkPyonix is *not* running, we may need code that assigns `_` directly to a user-specified variable as a fallback.
>
> **TOML and YAML**: once evaluated, the parsed content should be immediately accessible within the Python namespace. Add the same support for **JSON**.

언어 목록은 C(cython), C++(cppyy), Rust(maturin/PyO3), SQL입니다. JS·Java·Kotlin·Swift·ObjC는 "Python Multi-platform"으로 import해서 쓰는 것이라 이 문서의 범위가 아닙니다(thisisthepy/PythonMultiplatform 담당).

### 1.4 2025년 설계(노션 내보내기, `legacy/poc-2025`)

- 노션 "API 명세서 / 셀 생성"의 셀 필드는 `auto_run`, `collapsed`, `title`, `layout - horizontal`입니다([설계초안 HTML](../설계초안/DarkPyonix/API%20명세서/셀%20생성%202f80fa5b84fe81d0b68ce64d7556d7ef.html)).
- `legacy/poc-2025/common/protocol.py`의 `Cell.layout: str | None = None  # horizontal etc`.
- 2025년에도 레이아웃은 **셀 하나의 속성**이었고, 여닫는 구조는 없었습니다.

### 1.5 찾았지만 관련 없던 곳

- GitHub DarkPyonix 조직의 모든 저장소에서 `parallel`, `concurrent`, `grid`, `horizontal`, `layout`, `interop`, `cppyy`, `cython`, `maturin`, `run_parallel`, `__co_routines__`로 커밋과 코드를 검색했습니다. 걸린 것은 letify와 dioxus-compose의 무관한 커밋뿐입니다.
- darkpyonix 저장소에는 Discussions가 꺼져 있습니다. 관련 PR은 없습니다.
- darkpyonix-ash(Starboard 포크), vscode-darkpyonix, intellij-darkpyonix의 브랜치는 모두 업스트림 커밋뿐이고, 팀 커밋은 없습니다.
- dlsdyd 개인 저장소(darkpyonix-ember 포크 등)와 gist에는 관련 내용이 없습니다.
- 로컬 저장소(`/Volumes/macMini/darkpyonix/*`, `/Volumes/macMini/thisisthepy/*`)의 커밋 메시지에도 없습니다. darkpyonix-core의 `-S` 이력은 M0 이후 문서 커밋(`1e40d15`, `dd08a7d` 등)뿐입니다. thisisthepy/PythonMultiplatform의 `ada2fe84`(Cython으로 `@typedpython.compiled` 컴파일)는 Cython 사용 사례로 참고할 만합니다.

### 1.6 결론

- **결정된 것**: 레이아웃과 병렬은 다른 개념입니다(#7). 병렬 묶음은 `[parallel]` … `[concurrent]`와 `run_parallel`로 씁니다(참조 파일). 레이아웃은 셀 메타데이터입니다(참조 파일 `@width`, 2025 `layout`).
- **기록이 없는 것**: grid 여닫는 태그, 병렬 실행 방식(스레드/프로세스/asyncio), 실패와 인터럽트 의미, interop과 데이터 셀의 세부.
- 아래 §2~§5는 결정된 것을 그대로 두고, 빈 곳을 채운 **추천안**입니다.

## 2. 레이아웃 (Q7)

### 2.1 추천: grid 여닫는 태그를 두지 않습니다

근거
1. 2025년 설계와 참조 파일 모두 셀 하나의 속성(`layout`, `@width`)만 썼습니다.
2. 여닫는 태그가 있으면 협업 편집(SPEC §10a)에서 셀을 옮기거나 지울 때 짝이 깨집니다. 짝이 깨지면 파일 전체 배치가 무너집니다. 셀 속성은 그 셀에만 영향을 줍니다.
3. 2차원 grid는 "가로 줄을 세로로 쌓은 것"으로 충분히 나타낼 수 있습니다. 셀이 두 줄에 걸치는(row span) 배치는 노트북에서 쓸 일이 거의 없습니다.
4. 병렬 묶음에는 이미 `[parallel]`/`[concurrent]` 경계가 있습니다. 레이아웃용 경계를 또 두면 두 경계가 엇갈리는 경우(병렬 묶음이 grid 중간에서 끝나는 등)를 정의해야 합니다.

### 2.2 문법

FORMAT §2.3의 `layout`과 `width`를 다음처럼 확정합니다.

| 키 | 값 | 의미 |
|---|---|---|
| `layout` | `horizontal` | 이 셀에서 새 가로 줄이 시작됩니다. 이미 가로 줄 안이면 그 줄을 닫고 새 줄을 엽니다 |
| `layout` | `vertical` | 가로 줄을 닫습니다. 이 셀부터 다시 세로로 쌓입니다 |
| `width` | CSS grid 트랙(`1fr`, `2fr`, `320px`, `minmax(200px, 1fr)`) | 가로 줄 안에서 이 셀의 너비. 기본 `1fr`. 세로 배치에서는 무시합니다 |

- `@layout`이 없는 셀은 앞 셀의 배치를 이어받습니다. 가로 줄 안이면 그 줄의 다음 칸이 됩니다.
- 파일 맨 앞의 배치는 `vertical`입니다.
- `layout: grid`(현재 FORMAT 표에 있음)는 값에서 뺍니다. 파서는 이 값을 보존하고, 클라이언트는 `horizontal`로 다룹니다.
- 모르는 값은 보존하고 `vertical`로 다룹니다.
- 좁은 화면에서 줄을 접을지는 클라이언트가 정합니다. 파일 형식에는 넣지 않습니다.

예: 2×2 grid 뒤에 일반 셀

```python
# %% 학습 손실 [code]
# @layout: horizontal
plot(loss)

# %% 검증 손실 [code]
plot(val_loss)

# %% 학습률 [code]
# @layout: horizontal
plot(lr)

# %% GPU 메모리 [code]
# @width: 2fr
plot(mem)

# %% [code]
# @layout: vertical
summary()
```

### 2.3 실행과 무관함

레이아웃은 실행 순서와 실행 단위를 바꾸지 않습니다. 가로 줄의 셀은 파일 순서대로 하나씩 실행되고, 실행 버튼도 셀마다 따로 있습니다(#7). 커널은 `layout`과 `width`를 읽지 않습니다.

### 2.4 병렬 묶음의 기본 배치

병렬 묶음([§3](#3-병렬-묶음-parallel--concurrent-q8))은 화면에서 다음처럼 놓입니다.
- `[parallel]` 셀은 한 줄을 다 씁니다.
- 묶인 셀들은 기본으로 한 가로 줄이 됩니다. 이때 첫 묶인 셀에 `@layout: horizontal`을 쓰지 않아도 됩니다.
- `[concurrent]` 셀은 그 줄을 닫고 한 줄을 다 씁니다.
- 묶인 셀에 `@layout`을 명시하면 그 값을 따릅니다(예: 세로로 쌓은 병렬 묶음). 이 경우에도 화면 배치만 바뀌고 실행 의미는 그대로입니다.

## 3. 병렬 묶음: `[parallel]` … `[concurrent]` (Q8)

### 3.1 경계

- 묶음은 `[parallel]` 셀에서 시작해, 그 뒤 처음 나오는 `[concurrent]` 셀(별칭 `concorrunt`)에서 끝납니다. 두 셀 사이의 셀이 **묶인 셀**입니다.
- 묶음은 겹치지 않습니다. `[concurrent]`보다 `[parallel]`이 먼저 다시 나오거나 파일이 끝나면, 그 묶음은 **닫히지 않은 묶음**입니다. 파서는 오류 없이 보존합니다. 커널은 그 셀들을 일반 셀처럼 차례로 실행하고, 표준 오류에 경고를 한 줄 씁니다.
- 앞에 `[parallel]`이 없는 `[concurrent]`는 일반 셀처럼 실행합니다.
- 파서는 셀마다 `group` 정보(`{head: int, end: int}`)를 계산해 돌려줍니다. 이 값은 파일에 쓰지 않습니다.

### 3.2 실행 단위

- 묶음은 한 단위로 실행됩니다. `mode: cells`에 묶음 안 셀(머리, 묶인 셀, 끝 셀 중 무엇이든)이 하나라도 있으면, 커널은 그것을 묶음 전체 `head..end`로 넓힙니다. `run.started.cells`에는 넓힌 목록이 들어갑니다.
- 화면에서 병렬 묶음의 실행 버튼은 묶음에 하나입니다. 가로 배치의 셀마다 실행 버튼이 있는 것과 다릅니다(#7).

### 3.3 실행 순서 (커널과 `python file.py`가 같음)

1. `[parallel]` 셀을 실행합니다. 관례상 `__co_routines__ = []`입니다.
2. 묶인 셀을 파일 순서대로 **하나씩** 실행합니다. 각 셀은 일을 직접 하지 않고 awaitable이나 callable을 `__co_routines__`에 append합니다.
3. `[concurrent]` 셀을 실행합니다. 관례상 `darkpyonix.run_parallel(*__co_routines__)`입니다. **실제 동시 실행은 여기서만 일어납니다.**

1~3은 평범한 파이썬 실행입니다. 그래서 `python file.py`에서도 같은 순서와 같은 의미가 됩니다(FR-F5). 커널이 더 하는 일은 출력을 셀별로 나누는 것(§3.5)과 셀 상태를 표시하는 것뿐입니다.

### 3.4 `darkpyonix.run_parallel(*items, fail_fast=False, max_threads=None)`

표준 라이브러리만 씁니다(`asyncio`, `concurrent.futures`, `contextvars`).

| 항목의 종류 | 실행 방식 |
|---|---|
| awaitable(코루틴, `Future`, `Task`, `__await__`가 있는 객체) | 새 이벤트 루프 하나에서 `asyncio.gather`로 함께 기다립니다. 루프는 **메인 스레드**에서 돕니다(FR-K5, INTENT D12) |
| 인자 없는 callable(`functools.partial` 등) | 같은 루프의 `run_in_executor`로 `ThreadPoolExecutor`(최대 `max_threads`, 기본은 항목 수)에서 돌립니다 |
| 그 밖의 값 | 아무것도 실행하기 전에 `TypeError`를 냅니다 |

- 반환값은 항목 순서대로의 결과 목록입니다(`gather`와 같음).
- 이미 이벤트 루프가 도는 스레드에서 부르면 `RuntimeError`를 냅니다. 커널은 셀 사이에 루프를 돌리지 않으므로 노트북에서는 이 경우가 생기지 않습니다.
- CPU를 쓰는 순수 파이썬 함수는 GIL 때문에 스레드로 빨라지지 않습니다. 이는 문서에 적고, 프로세스 실행(§3.8)으로 해결합니다.
- 루프는 매번 새로 만들고 끝나면 닫습니다(`asyncio.new_event_loop()`. `asyncio.run`은 3.8에도 있지만, 아래 인터럽트 처리 때문에 루프를 직접 다룹니다).

### 3.5 출력 라우팅

- 커널은 묶인 셀 하나가 끝날 때마다, 그 셀이 실행되는 동안 `__co_routines__`에 새로 들어간 항목(객체 `id`로 비교)을 그 셀에 연결해 둡니다.
- `run_parallel`은 커널 안에서 항목마다 `contextvars` 값 `current_cell`을 그 셀 인덱스로 둔 컨텍스트에서 실행합니다. asyncio 태스크는 만들어질 때 컨텍스트를 복사하고, 스레드 항목은 `copy_context().run`으로 실행합니다.
- 출력 라우터(FR-X5)는 `sys.stdout`/`sys.stderr` 쓰기와 `display()`를 `current_cell` 기준으로 그 셀의 기록과 `output` 이벤트에 넣습니다. 값이 없으면 지금 실행 중인 셀, 즉 `[concurrent]` 셀로 보냅니다.
- 어느 셀에도 연결되지 않은 항목(`__co_routines__`를 쓰지 않고 `run_parallel(a(), b())`처럼 부른 경우)의 출력은 `[concurrent]` 셀로 갑니다.
- 파일 디스크립터 1·2에 직접 쓰는 출력(C 확장, 하위 프로세스)은 어느 셀 것인지 알 수 없습니다. 이런 출력은 `[concurrent]` 셀로 보냅니다. 한계로 문서에 적습니다.
- `python file.py`에서는 모두 표준 출력으로 나가고, 섞이는 순서는 정해지지 않습니다.

### 3.6 셀 상태와 이벤트

- 묶인 셀의 `cell.started`는 그 셀의 2단계 실행이 시작될 때 나갑니다. `cell.finished`는 그 셀에 연결된 항목이 모두 끝날 때 나갑니다. 연결된 항목이 없으면 2단계가 끝날 때 나갑니다.
- 그래서 묶음이 실행되는 동안에는 `running` 셀이 여러 개일 수 있습니다. 클라이언트는 `output` 이벤트의 `index`로 셀을 구분합니다(PROTOCOL §3.4는 이미 `index`를 담습니다).
- `cell.started`에 선택 필드 `group: {head, end}`를 더합니다(PR-4 호환 규칙상 필드 추가는 호환됩니다).
- 실행 기록(FR-R1)에서 묶인 셀의 `started_at`/`ended_at`은 2단계 시작부터 항목이 끝날 때까지입니다.

### 3.7 실패와 인터럽트

**실패** (기본 `fail_fast=False`)
- 한 항목이 예외를 내도 나머지는 끝까지 돕니다. 오래 도는 학습이 옆 셀의 실패 때문에 죽지 않게 하려는 것입니다.
- 실패한 항목의 traceback은 그 항목이 연결된 셀의 `error` 출력이 되고, 그 셀의 상태는 `error`입니다.
- 모든 항목이 끝나면 `run_parallel`은 `darkpyonix.ParallelError`를 냅니다. 이 예외는 `.errors`에 `[(항목 위치, 예외)]` 목록을 담습니다. 3.11의 `ExceptionGroup`은 3.8에서 쓸 수 없으므로 쓰지 않습니다. 그래서 `[concurrent]` 셀도 `error`로 끝나고, FR-X2대로 그 실행의 남은 셀은 건너뜁니다.
- `fail_fast=True`이면 처음 실패할 때 남은 awaitable을 취소하고(`CancelledError`), 스레드 항목에는 아래 인터럽트와 같은 방법으로 중단을 요청합니다.

**인터럽트** (FR-X4)
- SIGINT는 메인 스레드에 오므로, `run_until_complete` 안에서 `KeyboardInterrupt`가 납니다.
- `run_parallel`은 이를 잡아 다음을 합니다.
  1. 남은 태스크를 모두 취소하고, 취소가 끝날 때까지 루프를 돌립니다.
  2. 아직 도는 스레드 항목에는 `ctypes.pythonapi.PyThreadState_SetAsyncExc`로 `KeyboardInterrupt`를 보냅니다. 이 방법은 최선의 시도일 뿐이어서, C 코드 안에서 막혀 있는 스레드는 그 호출이 돌아올 때까지 멈추지 않습니다.
  3. 스레드를 최대 1초 기다린 뒤 `KeyboardInterrupt`를 다시 일으킵니다.
- 실행은 `interrupted`로 끝나고, 아직 끝나지 않은 묶인 셀도 `interrupted`가 됩니다. 1초 안에 멈추지 않은 스레드는 데몬처럼 남고, 그 뒤의 출력은 `[concurrent]` 셀이 아니라 커널 로그로 갑니다.
- 네임스페이스는 남습니다(FR-X4).

### 3.8 프로세스 실행 (옵션, 10-18 범위 밖)

`run_parallel(*callables, executor="process")`는 `ProcessPoolExecutor`로 돌립니다. 항목은 pickle할 수 있는 callable이어야 합니다.
- 커널은 `sys.modules["__main__"]`을 노트북 네임스페이스로 묶고 `__file__`을 둡니다. 그래서 spawn 자식은 `python file.py`와 같은 방식으로 파일을 다시 import하고, `if __name__ == '__main__'` 가드가 재실행을 막습니다. 참조 파일에 가드가 있는 것도 이 때문으로 보입니다.
- 인터럽트가 오면 자식에게 SIGINT를 전달합니다. 죽이지 않습니다(INTENT 조건 5).
- 상태는 `Draft`로 둡니다.

## 4. interop 셀: `cinterop`, `cppinterop`, `rustinterop`

### 4.1 공통 원칙

1. **셀 본문은 파이썬입니다.** 다른 언어 소스는 런타임 API 호출의 문자열 인자로 넣습니다. 참조 파일과 같고, `[markdown]`과도 같은 방식입니다. 셀 타입은 편집기가 그 문자열을 C/C++/Rust로 하이라이트하는 데만 씁니다. 커널은 셀 타입을 보고 따로 하는 일이 없습니다. 그래서 `python file.py`에서도 같습니다(FR-F5).
2. **툴체인은 선택 사항입니다.** 커널과 런타임 API는 표준 라이브러리만 씁니다. Cython, cppyy, maturin, C/C++ 컴파일러, cargo는 **사용자가 고른 인터프리터에 깔려 있을 때만** 씁니다. 없으면 `darkpyonix.InteropUnavailable`(`ImportError`의 하위 클래스)을 냅니다. 메시지에는 무엇이 없는지와 설치 방법(`darkpyonix.uv.add("cython")` 등)을 적습니다.
3. **컴파일은 하위 프로세스에서 합니다.** C와 Rust는 `sys.executable -m cython`, `sys.executable -m maturin` 등을 하위 프로세스로 실행합니다. 그래서 커널 프로세스는 Cython이나 maturin을 import하지 않습니다. 만들어진 확장 모듈은 `importlib.util.spec_from_file_location`으로 읽습니다(표준 라이브러리). C++(cppyy)만 JIT이라 프로세스 안에서 import해야 합니다(§4.4).
4. **캐시**
   - 키: `sha256(언어, 소스, 옵션, 툴체인 버전, sysconfig EXT_SUFFIX, 플랫폼)`
   - 위치: `$DARKPYONIX_HOME/interop/<lang>/<key>/`. 노트북 옆이나 `__runs__/`에는 두지 않습니다. 저장소를 더럽히지 않고, 같은 코드를 여러 노트북이 공유하게 하려는 것입니다.
   - 같은 키가 있으면 컴파일하지 않고 바로 읽습니다.
   - 빌드는 `<key>.tmp-<pid>/`에서 하고, 끝나면 `os.replace`로 옮깁니다.
   - 같은 키를 동시에 빌드하지 않도록 `<key>.lock`에 OS 잠금(FR-K3와 같은 방식)을 겁니다.
   - 정리 명령은 `darkpyonix interop clean`(CLI)입니다. 자동 정리는 하지 않습니다.
5. **네임스페이스 등록**
   - 내보낸 이름을 **호출한 쪽의 전역**(`sys._getframe(1).f_globals`)에 묶고, 모듈 객체를 반환합니다.
   - `name="mylib"`을 주면 그 이름으로도 묶고, `sys.modules["darkpyonix.interop.mylib"]`에도 등록합니다.
   - `exports=[...]`를 주면 그 이름만 내보냅니다.
6. **출력과 인터럽트**
   - 컴파일러 출력은 실패할 때만 표준 오류로 냅니다. 성공하면 한 줄 요약(`compiled <lang> <key[:8]> in 3.2s` 또는 `cached`)을 표준 오류에 씁니다. FR-F5는 표준 출력만 비교하므로 영향이 없습니다.
   - 컴파일 중의 인터럽트는 FR-F6처럼 하위 프로세스 그룹에 SIGINT를 전달합니다. 임시 폴더는 지웁니다.
7. **이미 읽은 확장 모듈은 내릴 수 없습니다.** 소스를 고치면 키가 바뀌므로 새 모듈 이름(`_dp_<key[:16]>`)으로 새로 읽고, 전역의 이름을 새 객체로 바꿔 묶습니다. 이전 모듈은 프로세스가 끝날 때까지 메모리에 남습니다.

### 4.2 `darkpyonix.run_cinterop(src, *, name=None, exports=None, cflags=(), libraries=())` — C via Cython

- `src`는 **Cython 소스**(`.pyx`)입니다. C 코드를 그대로 넣으려면 Cython의 verbatim C 블록을 씁니다.
  ```python
  # %% [cinterop]
  darkpyonix.run_cinterop("""
  cdef extern from *:
      '''
      static int add(int a, int b) { return a + b; }
      '''
      int add(int a, int b)

  cpdef int c_add(int a, int b):
      return add(a, b)
  """)
  c_add(1, 2)  # 3
  ```
- 내보내는 이름의 기본값은 모듈의 공개 이름(`_`로 시작하지 않는 `def`/`cpdef`/`cdef class`)입니다.
- 빌드: `<key>/mod.pyx`를 쓰고, `sys.executable -c "from Cython.Build import cythonize; from setuptools import setup, Extension; ..." build_ext --inplace`로 빌드합니다. 필요한 것은 Cython, setuptools, C 컴파일러입니다.
- 감지: `importlib.util.find_spec("Cython")`과 `find_spec("setuptools")`로 확인합니다(import하지 않음). 컴파일러가 없는 것은 빌드 실패 메시지로 알립니다.
- 날 C를 Cython 없이 쓰는 안(cffi, ctypes + 시스템 `cc`)은 이슈 #5가 "cython"을 지정했으므로 택하지 않습니다.

### 4.3 `darkpyonix.run_rustinterop(src, *, name=None, exports=None, dependencies=None, release=True)` — Rust via maturin·PyO3

- `src`는 PyO3 속성을 붙인 Rust 항목입니다(`#[pyfunction]`, `#[pyclass]`, `#[pymethods]`).
  ```python
  # %% [rustinterop]
  darkpyonix.run_rustinterop("""
  #[pyfunction]
  fn fib(n: u64) -> u64 { if n < 2 { n } else { fib(n - 1) + fib(n - 2) } }
  """)
  fib(30)
  ```
- 커널이 크레이트를 만듭니다.
  - `Cargo.toml`: `crate-type = ["cdylib"]`, `pyo3 = { version = "<고정 버전>", features = ["extension-module", "abi3-py38"] }`, 그리고 `dependencies`
  - `src/lib.rs`: `use pyo3::prelude::*;`, 사용자 소스, 그리고 `#[pyfunction]`·`#[pyclass]`가 붙은 이름을 정규식으로 찾아 등록하는 `#[pymodule] fn _dp_<key>`
  - `#[pymodule]`이 소스에 이미 있으면 생성하지 않고 그것을 씁니다.
- 빌드: `sys.executable -m maturin build --release --interpreter <sys.executable> --out <tmp>/wheels`로 휠을 만들고, `zipfile`로 풀어 확장 모듈을 꺼냅니다.
- cargo 대상 폴더는 `$DARKPYONIX_HOME/interop/rust/target-<abi>`를 함께 씁니다. pyo3 의존성을 매번 다시 빌드하지 않기 위해서이고, 동시 빌드는 잠금으로 막습니다. 첫 빌드는 수십 초 걸릴 수 있습니다.
- 감지: `find_spec("maturin")`과 `shutil.which("cargo")`로 확인합니다.

### 4.4 `darkpyonix.run_cppinterop(src, *, name=None, exports=None)` — C++ via cppyy

- cppyy는 Cling JIT이라 컴파일 산출물이 없고, 프로세스 안에서 `cppyy.cppdef(src)`를 불러야 합니다.
- Cling은 같은 이름을 다시 정의할 수 없습니다. 그래서 셀 소스를 `namespace __dp_<key[:16]> { … }`로 감싸 정의하고, 그 네임스페이스에서 이름을 꺼내 전역에 묶습니다. 셀을 고쳐 다시 실행하면 새 네임스페이스가 생기므로 재정의 오류가 나지 않습니다. 같은 소스를 다시 실행하면 이미 정의된 키라서 아무것도 하지 않습니다.
- 내보내는 이름의 기본값은 중괄호 깊이 0에서 선언된 `class`/`struct`/`enum`/`namespace`/함수 이름입니다. 간단한 스캐너로 찾고, 찾지 못하면 `exports=`를 요구하는 오류를 냅니다. 전체 네임스페이스는 `darkpyonix.cpp`(= `cppyy.gbl`)로 접근합니다.
- **표준 라이브러리 원칙의 예외가 필요합니다.** cppyy는 프로세스 안에서 import해야 합니다. 그래서 다음을 제안합니다.
  - `darkpyonix/interop.py`의 함수 안에서만, 사용자가 그 API를 불렀을 때만 `importlib.import_module("cppyy")`(또는 YAML용 `yaml`, §5)을 허용합니다.
  - 모듈 수준 import와 커널 코드 경로에서는 금지를 유지합니다.
  - NFR-K2 테스트에는 이 허용 목록을 명시합니다. 정적 `import` 문은 여전히 금지이고, 테스트는 `importlib.import_module` 문자열 인자를 허용 목록과 비교합니다.
  - 이 예외는 INTENT §2 조건 1의 해석을 바꾸는 것이므로 **사용자 확인이 필요합니다.**

### 4.5 `python file.py`에서의 동작 (FR-F5)

같은 함수가 같은 캐시와 같은 툴체인을 씁니다. 툴체인이 없으면 커널에서와 똑같이 `InteropUnavailable`이 납니다. 두 경우가 다르게 동작하는 일은 없습니다. 차이는 컴파일 요약 줄이 커널에서는 셀의 `stderr` 스트림이 되고, 일반 실행에서는 터미널 표준 오류로 나간다는 것뿐입니다.

## 5. 데이터 형식 셀: `toml`, `yaml`, `json`, `sql`

### 5.1 문법

```python
# %% [toml]
config = darkpyonix.toml("""
[model]
name = "swin_t"
lr = 1e-3
""")

# %% [json]
darkpyonix.json("""{"classes": ["cat", "dog"]}""", target="labels")

# %% [sql]
top = darkpyonix.sql("""
SELECT name, score FROM results WHERE score > :min ORDER BY score DESC
""", params={"min": 0.9})
```

### 5.2 `_`와 대상 변수 (이슈 #5)

- 모든 데이터 함수는 파싱한 값을 **반환**하고, 호출한 쪽 전역의 `target` 이름(기본 `"_"`)에도 **묶습니다**. `target=None`이면 묶지 않습니다.
- 이유: 이슈 #5의 주의점("DarkPyonix 밖에서는 `_`를 직접 대입하는 코드가 필요")을 런타임 API 안에서 해결하기 위해서입니다. `python file.py`는 스크립트에서 `_`를 설정하지 않습니다. 그래서 함수가 직접 대입해야 커널과 일반 실행이 같아집니다.
- 대입문(`config = darkpyonix.toml(...)`)으로 써도 됩니다. 편집기의 "저장할 변수" 칸은 `target=` 인자나 대입문의 왼쪽 이름에 대응하고, 칸을 바꾸면 그 코드를 고칩니다. 대상 변수를 주석 메타데이터(`# @target:`)에 두는 안은 택하지 않습니다. 주석은 `python file.py`가 읽지 않기 때문입니다.
- 마지막 문장이 호출 식이면 FR-X2대로 `execute_result`도 나옵니다.

### 5.3 형식별 처리

| 함수 | 파서 | 반환 |
|---|---|---|
| `darkpyonix.json(text, target="_")` | 표준 `json` | 파이썬 값 |
| `darkpyonix.toml(text, target="_")` | 3.11 이상이면 `tomllib`, 아니면 내장한 순수 파이썬 TOML 1.0 파서 `darkpyonix/_vendor/tomli`(MIT, 3.8 호환 버전 고정). 내장 코드는 우리 소스 트리에 있으므로 표준 라이브러리 원칙을 어기지 않습니다 | `dict` |
| `darkpyonix.yaml(text, target="_")` | PyYAML(`yaml.safe_load`)이 설치되어 있으면 씁니다. 없으면 `InteropUnavailable`. YAML 1.2 전체를 직접 구현하지 않습니다. 이 import는 §4.4의 허용 목록에 넣습니다 | 파이썬 값 |
| `darkpyonix.sql(query, target="_", params=None, con=None)` | DB-API 2.0 연결 `con`. 없으면 `darkpyonix.sql.connection`, 그것도 없으면 프로세스마다 하나인 `sqlite3` 메모리 DB | `darkpyonix.SQLResult` |

`SQLResult`는 다음과 같습니다.
- 튜플의 리스트이고 `.columns`를 가집니다.
- `_repr_html_`로 표를 그립니다(FR-X5).
- `.to_pandas()`는 pandas가 있을 때만 씁니다. pandas는 사용자 코드가 이미 import한 경우에만 `sys.modules`에서 꺼냅니다.
- 행을 내지 않는 문장이면 `rowcount`만 담습니다.

SQL 규칙
- 값은 `params`로만 넘깁니다. 파이썬 변수를 문자열에 끼워 넣는 기능(f-string 치환)은 SQL 주입 위험 때문에 두지 않습니다.
- DuckDB 같은 연결은 `con=`이나 `darkpyonix.sql.connection = duckdb.connect()`로 씁니다.

## 6. SPEC 반영안 (FR 번호)

기존 FR-X1~X6, FR-F1~F6 다음 번호를 씁니다. 다시 쓰는 번호는 없습니다. 모두 `Draft`로 시작하고, 이슈 #7 답과 사용자 확인 뒤 `Agreed`로 올립니다.

### FR-F7 레이아웃 메타데이터 — `Draft`
§2.2. `layout`은 `horizontal`/`vertical` 전환이고, 여닫는 태그는 두지 않습니다. 파서는 셀마다 `row`(가로 줄 번호, 세로면 `null`)와 `width`를 계산해 내보냅니다. 커널은 이 값을 읽지 않습니다.
- 수용 기준: §2.2의 예시를 파싱하면 행이 `[0,0,1,1,null]`이고 트랙이 `["1fr","1fr"]`, `["1fr","2fr"]`입니다. `layout: grid`는 보존되면서 `horizontal`로 계산됩니다. 가로 줄 셀 4개가 파일 순서대로 하나씩 실행됩니다.
- 테스트: `test_fr_f7_layout_rows_from_switches`, `test_fr_f7_layout_does_not_change_execution`

### FR-X7 병렬 묶음 경계와 실행 단위 — `Draft`
§3.1~§3.2.
- 수용 기준:
  - 참조 파일의 `[parallel]`..`[concurrent]`가 묶음 하나(묶인 셀 2개)입니다.
  - `mode: cells, cells: [묶인 셀 하나]`가 묶음 전체로 넓혀지고, `run.started.cells`에 넓힌 목록이 들어갑니다.
  - 닫히지 않은 묶음은 일반 셀로 실행되고 경고가 한 줄 나옵니다.
- 테스트: `test_fr_x7_parallel_group_bounds`, `test_fr_x7_running_one_member_runs_the_group`, `test_fr_x7_unterminated_group_runs_sequentially`

### FR-X8 `darkpyonix.run_parallel` — `Draft`
§3.4. awaitable은 메인 스레드의 asyncio에서 모아 실행하고, callable은 스레드에서 돌립니다. 표준 라이브러리만 씁니다.
- 수용 기준:
  - `asyncio.sleep(1)`을 하는 코루틴 3개가 1.5초 안에 끝나고, 결과가 항목 순서대로 나옵니다.
  - 코루틴 안에서 `threading.current_thread() is threading.main_thread()`가 `True`입니다.
  - `time.sleep(1)` callable 3개도 1.5초 안에 끝납니다.
  - 정수를 넘기면 아무것도 실행되기 전에 `TypeError`가 납니다.
  - 같은 파일을 `python file.py`로 돌려도 같은 결과를 냅니다.
- 테스트: `test_fr_x8_run_parallel_awaits_concurrently_on_main_thread`, `test_fr_x8_callables_run_in_threads`, `test_fr_x8_same_under_plain_python`

### FR-X9 병렬 출력 라우팅과 셀 상태 — `Draft`
§3.5~§3.6.
- 수용 기준:
  - 묶인 셀 A와 B의 코루틴이 각각 `print`한 줄은 A와 B의 `stream` 출력에만 들어갑니다.
  - A와 B가 동시에 `running`입니다.
  - A의 `cell.finished`는 A의 코루틴이 끝날 때 나가고, `cell.started`에 `group`이 들어갑니다.
  - 연결되지 않은 항목의 출력은 `[concurrent]` 셀에 들어갑니다.
- 테스트: `test_fr_x9_member_output_goes_to_its_cell`, `test_fr_x9_members_finish_independently`

### FR-X10 병렬 실패 — `Draft`
§3.7 실패.
- 수용 기준:
  - 기본값에서 A가 예외를 내도 B는 끝까지 돕니다.
  - A에는 `error` 출력이, `[concurrent]` 셀에는 `.errors`에 A의 예외를 담은 `ParallelError`가 나옵니다.
  - 그 실행의 다음 셀은 건너뜁니다.
  - `fail_fast=True`면 B가 취소됩니다.
- 테스트: `test_fr_x10_failure_waits_for_siblings_then_raises`, `test_fr_x10_fail_fast_cancels_siblings`

### FR-X11 병렬 인터럽트 — `Draft`
§3.7 인터럽트. FR-X4를 병렬 묶음으로 넓힙니다.
- 수용 기준:
  - `while True: await asyncio.sleep(0.01)` 코루틴 2개와 `while True: n += 1` 스레드 callable 1개로 된 묶음을 인터럽트하면, 1초 안에 실행이 `interrupted`로 끝납니다.
  - 세 항목이 모두 멈추고, 네임스페이스는 남습니다.
- 테스트: `test_fr_x11_interrupt_cancels_tasks_and_threads`

### FR-X12 프로세스 실행 — `Draft` (10-18 범위 밖)
§3.8. `executor="process"`.
- 테스트: `test_fr_x12_process_executor_runs_picklable_callables`

### FR-F8 interop 공통: 툴체인 감지, 캐시, 네임스페이스 — `Draft`
§4.1, §4.5.
- 수용 기준:
  - 툴체인이 없는 인터프리터에서 `run_cinterop`이 `InteropUnavailable`(설치 안내 포함)을 냅니다. 커널과 `python file.py`에서 같습니다.
  - 같은 소스를 두 번 실행하면 두 번째는 컴파일하지 않습니다.
  - 두 커널이 같은 소스를 동시에 실행해도 빌드는 한 번입니다.
  - 내보낸 이름이 호출한 쪽 전역에 생깁니다.
  - 커널 모듈은 여전히 표준 라이브러리만 import합니다(NFR-K2, 허용 목록 예외 포함).
- 테스트: `test_fr_f8_missing_toolchain_raises_interop_unavailable`, `test_fr_f8_build_is_cached_by_source_hash`, `test_fr_f8_concurrent_builds_share_one_artifact`

### FR-F9 `run_cinterop` (Cython) — `Draft`
§4.2.
- 수용 기준: §4.2 예시 뒤 `c_add(1, 2) == 3`입니다. 소스를 고쳐 다시 실행하면 새 정의가 쓰입니다.
- 테스트: `test_fr_f9_cinterop_exports_cpdef_functions` (Cython과 컴파일러가 없으면 skip)

### FR-F10 `run_cppinterop` (cppyy) — `Draft`
§4.4.
- 수용 기준: `struct P { int x; }; int twice(int a) { return 2*a; }` 뒤 `twice(2) == 4`이고 `P().x`가 접근됩니다. 같은 이름을 고쳐 다시 실행해도 재정의 오류가 나지 않습니다.
- 테스트: `test_fr_f10_cppinterop_exports_and_redefines` (cppyy가 없으면 skip)

### FR-F11 `run_rustinterop` (maturin·PyO3) — `Draft`
§4.3.
- 수용 기준: §4.3 예시 뒤 `fib(10) == 55`입니다. 두 번째 실행은 캐시를 씁니다. 인터럽트하면 cargo가 멈추고 임시 폴더가 남지 않습니다.
- 테스트: `test_fr_f11_rustinterop_builds_pyo3_module` (maturin과 cargo가 없으면 skip. CI 전용으로 둘 수 있습니다. 하위 에이전트는 이 테스트를 돌리지 않습니다)

### FR-F12 데이터 형식 함수 `json`/`toml`/`yaml` — `Draft`
§5.1~§5.3.
- 수용 기준:
  - 3.8과 3.12 모두에서 `darkpyonix.toml(...)`이 같은 `dict`를 냅니다.
  - `target="cfg"`이면 `cfg`가 생기고, 기본값이면 `_`가 생깁니다. `python file.py`에서도 같습니다.
  - PyYAML이 없으면 `yaml()`이 `InteropUnavailable`을 냅니다.
- 테스트: `test_fr_f12_toml_on_all_interpreters`, `test_fr_f12_target_binds_in_plain_python`, `test_fr_f12_yaml_requires_pyyaml`

### FR-F13 `darkpyonix.sql` — `Draft`
§5.3.
- 수용 기준:
  - 기본 연결은 sqlite 메모리 DB이고, 셀 사이에 유지됩니다.
  - `params`로 값을 바인딩합니다.
  - 결과의 `columns`와 행이 맞고, 마지막 식이면 `text/html` `execute_result`가 나옵니다.
  - `con=`으로 다른 DB-API 연결을 씁니다.
- 테스트: `test_fr_f13_sql_default_sqlite_and_params`, `test_fr_f13_sql_result_renders_html`

### NFR-K2 개정안
`importlib.import_module` 호출은 문자열 상수 인자만 쓰고, 그 값이 허용 목록(`cppyy`, `yaml`)에 있을 때만 허용합니다. 위치는 `kernel/darkpyonix/interop.py`, `kernel/darkpyonix/data.py`의 함수 본문으로 한정합니다. 정적 `import` 문의 규칙은 그대로입니다.

## 7. 10-18까지의 순서

| 순서 | 내용 | 비고 |
|---|---|---|
| 1 | FORMAT·SPEC 반영(FR-F7, FR-X7~X11, FR-F8~F13), INTENT 조건 1의 허용 목록 | 이슈 #7 답 확인 후. 답이 없으면 이 문서대로 진행하고 답이 오면 고칩니다 |
| 2 | 파서의 `group`·`row` 계산(FR-F7, FR-X7) | #8 파서 위에 더합니다 |
| 3 | `run_parallel`, 출력 라우터의 `contextvars` 라우팅, 묶음 단위 실행(FR-X8~X11) | #11 실행기 위에 더합니다 |
| 4 | `json`/`toml`(tomli 내장)/`yaml`/`sql`(FR-F12~F13) | 표준 라이브러리만으로 거의 다 됩니다 |
| 5 | interop 공통과 `run_cinterop`, `run_cppinterop`(FR-F8~F10) | 툴체인 테스트는 있는 환경에서만 |
| 6 | `run_rustinterop`(FR-F11) | 빌드가 무거우므로 마지막에 하고 CI에서 확인합니다. 늦어지면 이것만 11월로 미룹니다 |
| — | 프로세스 실행(FR-X12) | 10-18 범위 밖 |

## 8. 인용에게 확인할 것 (이슈 #7 댓글로 보냄)

1. grid 여닫는 태그: (A) 없음, `@layout: horizontal|vertical` 전환만 / (B) `[grid]`…`[/grid]` 여닫는 표식 / (C) 그 밖
2. 병렬 묶음: `[parallel]`이 열고 `[concurrent]`의 `run_parallel`이 닫으면서 실행하는 것이 최종인지. 실행 방식은 (a) asyncio와 스레드 / (b) 프로세스 / (c) (a)가 기본, (b)는 옵션 중 무엇인지. 묶음 단위로 실행하는지. 실패해도 나머지를 계속 돌리는지
3. `concorrunt`를 `concurrent`로 확정해도 되는지
