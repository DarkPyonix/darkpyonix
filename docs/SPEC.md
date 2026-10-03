# SPEC

DarkPyonix 커널 스택의 요구사항과 수용 기준입니다. 근거는 [INTENT.md](INTENT.md)에 있고, 와이어 형식은 [PROTOCOL.md](PROTOCOL.md), 파일 형식은 [FORMAT.md](FORMAT.md), HTTP API는 [api/manager.openapi.yaml](api/manager.openapi.yaml)과 [api/hub.openapi.yaml](api/hub.openapi.yaml)입니다. OpenAPI 파일은 이 SPEC의 일부입니다.

- 상태: `Draft`(합의 전), `Agreed`(구현 가능), `Done`(수용 기준 통과)
- ID는 다시 쓰지 않습니다. 폐기하면 `Withdrawn`으로 남깁니다.
- 테스트 이름은 ID를 따릅니다. 예: `test_fr_k3_second_kernel_for_same_file_is_refused`
- 2025년 명세는 [설계초안/SPEC-2025.md](설계초안/SPEC-2025.md)에 보존합니다. 이 문서와 다르면 이 문서가 기준입니다.

## 1. 용어

| 용어 | 정의 |
|---|---|
| 노트북 파일 | `# %%` 셀 표식을 쓰는 `.py` 또는 `.pynb` 파일(FORMAT) |
| 커널 | 노트북 파일 하나에 묶인 실행 프로세스 |
| 커널 ID | 정규화 경로에서 만든 식별자 `k_<20 hex>`(PROTOCOL §2.6) |
| 실행(run) | 실행 요청 하나. 파일 전체 또는 셀 몇 개 |
| 실행 기록 | 실행 하나를 담은 nbformat 4 노트북 파일(`__runs__/…/<run_id>.ipynb`) |
| 매니저 | 커널을 발견·실행하고 HTTP API를 내는 프로세스. 임시/전용 모드 |
| 런타임 홈 | `DARKPYONIX_HOME`, 기본 `~/.darkpyonix` |
| 호스팅 노트북 | 허브(`api.darkpyonix.dev`)에 보관한 노트북. 판(version)의 묶음이고, 판은 노트북 파일 하나와 선택적인 실행 기록 하나입니다(FR-H14) |
| 프런트(메인 페이지) | 루트 `darkpyonix.dev/`의 정적 웹 앱(랜딩 + ash). darkpyonix-ash가 빌드하고 조직 GitHub Pages가 냅니다. API를 부를 뿐 서버 코드가 없습니다(FR-H12) |

## 2. 커널 (K)

### FR-K1 설치 없이 어떤 인터프리터로도 실행 — `Done`
매니저는 사용자가 고른 인터프리터에, 커널 소스 루트를 `sys.path` 앞에 넣는 부트스트랩(`-c`)으로 커널을 띄웁니다. 그 인터프리터에 DarkPyonix가 설치되어 있지 않아도 됩니다.
- 수용 기준: DarkPyonix가 설치되지 않은 가상환경의 인터프리터로 커널을 띄우고 셀을 실행할 수 있습니다. 사용자 코드의 `import darkpyonix`가 성공합니다.
- 테스트: `test_fr_k1_kernel_runs_from_uninstalled_interpreter`, `test_fr_k1_kernel_from_uninstalled_venv_runs_a_cell`(인터프리터마다 `.scratch/` 아래에 `--without-pip` 가상환경을 만들고, 그 인터프리터로 띄운 커널의 셀에서 `import darkpyonix`가 커널 소스 루트에서 불러와지고 `sys.prefix`가 그 가상환경임을 확인)

### FR-K2 파일에 묶인 커널 ID — `Done`
커널 ID는 PROTOCOL §2.6의 규칙으로 만듭니다.
- 수용 기준: 같은 파일의 절대 경로, 상대 경로, 심볼릭 링크가 같은 ID를 냅니다. 다른 파일은 다른 ID를 냅니다.
- 테스트: `test_fr_k2_kernel_id_is_stable_across_path_spellings`, Rust `fr_m2_kernel_id_matches_python`(`darkpyonix/manager/crates/dpx-kernel/tests/discovery_launch.rs`, 매니저의 ID가 파이썬과 같음)

### FR-K3 파일당 커널 하나 — `Done`
커널은 시작할 때 `locks/<kernel_id>.lock`에 OS 배타 잠금(POSIX `fcntl.flock`, Windows `msvcrt.locking`)을 겁니다. 잠그지 못하면 종료 코드 3으로 끝나고, 표준 오류에 이미 떠 있는 커널의 announce 본문을 씁니다.
- 수용 기준: 같은 파일로 커널을 두 번 띄우면 두 번째가 코드 3으로 끝나고 첫 번째는 영향받지 않습니다. 첫 번째를 `kill -9`로 죽인 뒤에는 새 커널이 바로 뜹니다.
- 테스트: `test_fr_k3_second_kernel_for_same_file_is_refused`, `test_fr_k3_lock_is_released_when_kernel_dies`

### FR-K4 매니저 독립 수명 — `Done`
커널은 분리된 세션(POSIX `start_new_session`, Windows `DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP`)으로 시작하고, 표준 입력은 닫고, 진단 출력은 `kernels/<kernel_id>.log`로 보냅니다.
- 수용 기준: 실행 중인 셀이 있는 상태에서 커널을 띄운 매니저를 `SIGKILL`로 죽여도 셀은 끝까지 실행되고, 새 매니저가 그 실행의 결과를 읽을 수 있습니다.
- 테스트: `test_fr_k4_kernel_survives_manager_kill`, `test_fr_k4_kernel_survives_launcher_exit`

### FR-K5 사용자 코드는 메인 스레드 — `Done`
셀 코드는 커널 프로세스의 메인 스레드에서 실행합니다(INTENT D12).
- 수용 기준: 셀 안에서 `threading.current_thread() is threading.main_thread()`가 `True`이고, `signal.signal`을 호출할 수 있습니다.
- 테스트: `test_fr_k5_cells_run_on_main_thread`

### FR-K6 네임스페이스 조회 — `Done`
`namespace` 요청은 사용자 변수의 이름, 타입, 요약을 PROTOCOL §3.6 형식으로 돌려줍니다.
- 수용 기준: 대기 중에는 `repr`가 채워지고, 실행 중에는 `name`과 `type`만 채워집니다. 실행 중에 조회해도 사용자 객체의 `__repr__`가 호출되지 않습니다.
- 테스트: `test_fr_k6_namespace_lists_user_variables`, `test_fr_k6_namespace_does_not_call_repr_while_busy`

### FR-K7 재시작 — `Done`
`restart`(soft)는 네임스페이스를 비우고 실행 횟수를 0으로 돌립니다. `restart hard`는 같은 인터프리터와 인자로 프로세스를 다시 실행합니다(커널 ID 유지).
- 수용 기준: soft 재시작 뒤 이전 변수가 없습니다. hard 재시작 뒤 커널 ID가 같고 `pid`나 시작 시각이 바뀝니다.
- 테스트: `test_fr_k7_soft_restart_clears_namespace`, `test_fr_k7_hard_restart_stops_loop_and_sets_flag`(실행기 쪽), `test_fr_k7_hard_restart_keeps_kernel_id`(실제 커널 프로세스: 같은 커널 ID, 같은 인터프리터와 경로로 다시 announce하고 `started_at`이 늦어지며 이전 변수가 없음. POSIX에서는 `os.execv`라 `pid`는 그대로입니다)

### FR-K8 종료 — `Done`
`shutdown`은 실행 중인 셀을 인터럽트하고, 실행 기록을 마저 쓰고, `bye`를 보내고, 등록 파일을 지우고, 코드 0으로 끝납니다.
- 수용 기준: 종료 뒤 등록 파일과 잠금이 남지 않고 실행 기록의 상태는 `interrupted`입니다.
- 테스트: `test_fr_k8_shutdown_interrupts_running_cell_and_finishes_run`(실행기 쪽), `test_fr_k8_shutdown_is_graceful`(실제 커널 프로세스: 셀이 도는 중에 `shutdown`을 보내면 `bye` 데이터그램, 종료 코드 0, 등록 파일 없음, 잠금을 곧바로 다시 잡을 수 있음, 실행 기록 `interrupted`). "잠금이 남지 않음"은 OS 잠금이 풀린다는 뜻이고, 잠금 파일 자체는 지우지 않습니다(지우면 `flock`과 경합이 생깁니다).

## 3. 실행 (X)

### FR-X1 전체 실행과 셀 실행 — `Done`
`run`은 `mode: all`(프리앰블과 모든 셀을 순서대로)과 `mode: cells`(프리앰블이 아직 실행되지 않았으면 먼저 실행, 그다음 지정한 셀)를 받습니다. `source`가 오면 디스크 파일 대신 그 텍스트를 파싱합니다(저장하지 않은 편집기 버퍼). 실행 네임스페이스의 `__name__`은 `"__main__"`, `__file__`은 파일 경로입니다.
- 수용 기준: 전체 실행의 출력이 `python file.py`의 출력과 같습니다(FR-F5). 셀 실행은 지정한 셀만 실행합니다. 앞 셀에서 만든 변수는 다음 실행에도 남습니다.
- 테스트: `test_fr_x1_run_all_matches_plain_python`, `test_fr_x1_run_cells_keeps_namespace`, `test_fr_x1_source_overrides_file_and_file_is_main`, `test_e2e_run_all_logs_and_outputs`(실제 커널 프로세스)

### FR-X2 셀 실행 의미 — `Done`
셀 본문은 `exec`로 실행하되, 마지막 문장이 식이면 그 값을 `execute_result`로 내고 `_`에 저장합니다(값이 `None`이면 내지 않음). 실패하면 `error` 출력(ename, evalue, traceback)을 내고 그 실행의 남은 셀을 건너뜁니다.
- 수용 기준: `x = [i**2 for i in range(5)]; x` 셀이 `execute_result`로 `[0, 1, 4, 9, 16]`을 냅니다. traceback에 커널 내부 프레임이 들어가지 않습니다.
- 테스트: `test_fr_x2_last_expression_is_execute_result`, `test_fr_x2_error_stops_run_and_hides_kernel_frames`, `test_fr_x2_syntax_error_is_an_error_output`

### FR-X3 바쁠 때의 정책 — `Done`
실행 중에 `run`이 오면 기본(`on_busy: reject`)은 `busy` 오류이고, `data`에 현재 실행 요약이 들어갑니다. `on_busy: queue`이면 대기열에 넣고 `position`을 돌려줍니다. 대기열은 들어온 순서로 실행합니다.
- 수용 기준: 같은 파일에 `run`을 연달아 두 번 보내면 두 번째가 `busy`를 받습니다. `queue`로 보내면 첫 실행이 끝난 뒤 실행됩니다.
- 테스트: `test_fr_x3_second_run_is_rejected_when_busy`, `test_fr_x3_queued_run_waits_its_turn`, `test_e2e_interrupt_keeps_state_and_busy_is_rejected`(실제 커널 프로세스)

### FR-X4 인터럽트 — `Done`
`interrupt`는 실행 중인 셀에 `KeyboardInterrupt`를 일으킵니다. 네임스페이스와 커널 프로세스는 그대로 남고, 실행은 `interrupted`로 끝나며, 대기열은 유지합니다.
- 수용 기준: `while True: step += 1` 셀을 인터럽트하면 1초 안에 실행이 `interrupted`로 끝나고, 이어지는 실행에서 `step`을 읽을 수 있습니다.
- 테스트: `test_fr_x4_interrupt_stops_cell_and_keeps_state`, `test_e2e_interrupt_keeps_state_and_busy_is_rejected`(실제 커널 프로세스)

### FR-X5 출력 캡처 — `Done`
`sys.stdout`/`sys.stderr` 쓰기는 `stream` 출력이 됩니다. 파일 디스크립터 1·2에 직접 쓰는 출력(C 확장, 하위 프로세스)도 파이프로 받아 같은 스트림에 넣습니다. `display()`와 IPython식 `_repr_*_` 프로토콜(`_repr_mimebundle_`, `_repr_html_`, `_repr_png_`, `_repr_markdown_`, `_repr_json_`)은 `display_data`/`execute_result` MIME 번들이 됩니다.
- 수용 기준: `os.write(1, b"x\n")`와 `subprocess.run(["echo","y"])`의 출력이 해당 셀의 `stream` 출력에 나타납니다. `_repr_html_`이 있는 객체를 마지막 식으로 두면 `text/html`이 있는 `execute_result`가 나옵니다.
- 테스트: `test_fr_x5_fd_level_output_is_captured`, `test_fr_x5_repr_protocol_becomes_mime_bundle`

### FR-X6 matplotlib — `Done`
matplotlib이 설치된 인터프리터에서는 커널이 `plt.show()`와 셀 끝에 남은 그림을 `image/png` `display_data`로 냅니다(`text/plain`은 그림의 `repr`). 낸 그림은 닫으므로 다음 셀이 같은 그림을 다시 내지 않습니다. 셀이 오류로 끝나도 그때까지 그린 그림은 냅니다.

커널은 matplotlib을 import하지 않습니다. 사용자 코드가 `matplotlib.pyplot`을 처음 import할 때, 그 시점에 백엔드가 아직 정해지지 않았으면(`MPLBACKEND`, matplotlibrc, `matplotlib.use()` 어디에서도 정하지 않음) 커널의 백엔드 `module://darkpyonix.kernel.mplbackend`를 고릅니다. 사용자가 백엔드를 정했으면 그대로 둡니다. 커널은 환경 변수를 바꾸지 않으므로 하위 프로세스의 matplotlib에는 영향이 없습니다. `darkpyonix.kernel.mplbackend`는 matplotlib이 불러오는 모듈이라 `matplotlib`을 import할 수 있는 유일한 커널 모듈입니다(NFR-K2의 예외).
- 수용 기준:
  - `plt.plot([1,2]); plt.show()` 셀이 PNG `display_data` 하나를 내고, 실행 기록에도 남습니다.
  - `show()` 없이 `plt.plot([1,2])`로 끝나는 셀도 셀 끝에 PNG 하나를 내고, 다음 셀은 그 그림을 다시 내지 않습니다.
  - matplotlib을 쓰지 않는 실행 뒤 커널 프로세스의 `sys.modules`에 `matplotlib`이 없습니다.
  - pyplot import 전에 `matplotlib.use("agg")`로 백엔드를 정한 셀은 PNG를 내지 않습니다.
- 테스트: `test_fr_x6_matplotlib_show_emits_png`, `test_fr_x6_figure_left_at_cell_end_is_shown_once`, `test_fr_x6_kernel_does_not_import_matplotlib`, `test_fr_x6_user_chosen_backend_is_kept` (matplotlib이 없는 인터프리터에서는 skip, 셋째 테스트는 모든 인터프리터에서 실행)

## 4. 실행 기록 (R)

### FR-R1 자동 기록 — `Done`
모든 실행은 `<파일 폴더>/__runs__/<파일 이름>/<run_id>.ipynb`에 nbformat 4.5 노트북으로 기록됩니다. `run_id`는 `YYYYMMDD-HHMMSS-<4 hex>`(UTC)입니다. 노트북 `metadata.darkpyonix`에는 `run_id`, `kernel_id`, `file`, `file_sha256`, `mode`, `params`, `status`, `started_at`, `ended_at`, `python`, `host`가 들어갑니다. 셀마다 `metadata.darkpyonix`에 `index`, `type`, `title`, `source_sha256`, `status`, `started_at`, `ended_at`이 들어갑니다.
- 수용 기준: 실행 기록이 `nbformat.validate`를 통과합니다(테스트 환경에 nbformat이 있을 때). 표준 라이브러리 `json`으로 읽은 구조가 위 필드를 모두 가집니다.
- 테스트: `test_fr_r1_run_log_is_valid_nbformat`, `test_fr_r1_index_lists_runs_newest_first_and_is_rebuilt_when_corrupt`

### FR-R2 실행 중 저장 — `Done`
커널은 실행 중에도 기록을 최대 1초 간격으로 원자적으로(임시 파일 → `os.replace`) 다시 씁니다. `index.json`에는 최신순 실행 요약(`run_id`, `status`, `started_at`, `ended_at`, `mode`)을 둡니다.
- 수용 기준: 실행 중에 커널을 `SIGKILL`로 죽여도 기록 파일은 유효한 JSON이고, 죽기 1초 전까지의 출력이 들어 있습니다. 상태는 `running`으로 남고, 다음 커널이 그 파일을 열면 `crashed`로 바꿉니다.
- 테스트: `test_fr_r2_log_survives_kernel_kill`, `test_fr_r2_update_is_throttled`, `test_fr_r2_recover_leaves_this_processes_current_run_alone`, `test_fr_r2_real_kernel_killed_mid_run_is_recovered_as_crashed`(실제 커널을 다음 다시 쓰기 직전, 즉 가장 불리한 순간에 `SIGKILL`하고, 셀이 찍은 모든 출력 중 죽기 1초 전보다 오래된 것이 기록에 다 있는지 확인한 뒤, 같은 파일로 새 커널을 띄워 기록이 `crashed`가 되는지 봅니다)
- 구현 메모: 다시 쓰기 주기는 스냅숏 시작부터 다음 스냅숏 시작까지 0.8초(`runs.WRITE_INTERVAL`)입니다. 1초 주기로는 캡처 라우터의 폴링 지연(최대 20 ms)과 쓰기 시간이 더해져 가장 불리한 순간에 1.01–1.04초 전 출력이 빠졌습니다(2026-10-03 측정).
- 측정(2026-10-03, Mac mini 8코어, 가장 불리한 순간에 죽임): 기록에 없는 가장 오래된 출력이 죽기 0.79–0.86초 전. CPU를 16개 바쁜 루프로 2배 초과 점유한 상태에서도 0.77–0.86초.

### FR-R3 매직 변수 `__runs__` — `Done`
커널 네임스페이스에는 `__runs__` 객체가 있습니다.

| 표현 | 값 |
|---|---|
| `__runs__.current` | 진행 중인 실행의 노트북 dict, 없으면 `None` |
| `__runs__.latest` | 가장 최근에 끝난 실행의 노트북 dict, 없으면 `None` |
| `__runs__[run_id]`, `__runs__[-1]` | 해당 실행의 노트북 dict |
| `__runs__.list(limit=20)` | 최신순 요약 목록 |
| `__runs__.dir` | 기록 폴더 경로(str) |

목적은 실행 기록 `.ipynb`를 `json` import 없이 편하게 다루는 것입니다. 그래서 반환값은 dict이면서 속성 접근도 됩니다. 실행 하나(`RunLog`)와 셀 하나(`CellLog`)는 `dict`의 하위 클래스이고, 그 안의 dict와 list도 같은 방식으로 속성 접근이 됩니다.
- 속성 이름은 먼저 dict의 키에서, 없으면 `metadata.darkpyonix`의 필드에서 찾습니다. 둘 다 없으면 `AttributeError`입니다. 그래서 `run.run_id`, `run.status`, `run.params`, `run.mode`, `run.started_at`, `run.cells`, `cell.index`, `cell.title`, `cell.status`, `cell.source`, `cell.outputs`가 됩니다.
- `run.cells`: 기록에 남은 셀의 목록입니다(실행된 셀만, 실행 순서). `run.cells[i]`는 이 목록의 위치이고, 파일 안의 셀 번호는 `cell.index`입니다.
- `cell.text`: 그 셀의 `stdout` 스트림 출력을 순서대로 이어 붙인 문자열(없으면 `""`). `cell.stderr`: 같은 방식의 `stderr`.
- `cell.result`: 그 셀의 마지막 `execute_result`의 `text/plain` 문자열, 없으면 `None`.
- `run.cell(key)`: 문자열이면 셀 ID(`metadata.darkpyonix.id`), 그다음 제목(`title`)이 같은 첫 셀, 정수면 파일 셀 번호(`index`)가 같은 첫 셀. 없으면 `KeyError`.
- `run.path`: 그 실행 기록 `.ipynb`의 절대 경로(str). 진행 중인 실행도 기록이 쓰이는 경로입니다.
- `run.notebook`: 원본 nbformat dict(속성 접근이 없는 순수 `dict`/`list`). `json.load`로 그 파일을 읽은 것과 같습니다.
- `__runs__.list()`의 요약도 속성 접근이 됩니다(`s.run_id`, `s.path`).
- `text`, `stderr`, `result`, `path`, `notebook`은 dict의 키가 아니므로 `json.dumps(run)`의 결과는 노트북 그대로입니다. 반환값을 바꿔도 기록은 바뀌지 않습니다.
- 수용 기준(실제 커널 셀 안에서, 사용 가능한 모든 인터프리터에서, NFR-K1):
  - 두 번째 실행의 셀에서 `__runs__.latest.run_id`가 첫 번째 실행의 ID이고, `__runs__.latest.status`가 `"ok"`입니다.
  - `__runs__.latest.cells[1].text`가 첫 실행에서 기록된 둘째 셀의 표준 출력이고, `.stderr`는 표준 오류입니다. 결과 값을 남긴 셀의 `.result`는 그 `repr`이고, 결과가 없는 셀은 `None`입니다.
  - `run.cell("제목")`과 `run.cell(파일 셀 번호)`가 그 셀을 돌려주고, 없는 키는 `KeyError`입니다.
  - `run.path`의 파일을 `json.load`한 값이 `run.notebook`과 같고, `type(run.notebook) is dict`입니다.
  - `__runs__.current.run_id`는 지금 실행의 ID이고, `__runs__.current.path`는 그 실행의 기록 경로입니다.
  - `json.dumps(__runs__.latest)`가 성공하고, `json.loads`한 값이 `run.notebook`과 같습니다.
- 테스트: `test_fr_r3_runs_magic_exposes_logs_as_json`, `test_fr_r3_runs_magic_attribute_access_in_kernel_cell`
- 상태 메모 (2026-10-03): 속성 접근(`RunLog`, `CellLog`)을 구현했고, 위 수용 기준을 실제 커널 셀 안에서 이 기기의 모든 인터프리터(3.9, 3.11, 3.13, 3.14, 3.15)로 검증했습니다. 이 기기에는 3.8 인터프리터가 없어 3.8에서는 돌리지 않았고, 코드는 `ast.parse(feature_version=(3, 8))`를 통과합니다.

### FR-R4 기록과 셀 맵핑 — `Done`
매니저의 `GET /kernels/{id}/document`는 파일의 셀 목록에 최신 실행의 출력을 붙여서 돌려줍니다. 맵핑 순서는 `id` → `source_sha256` → `index`이고, 소스 해시가 다르면 `stale: true`로 표시합니다.
- 수용 기준: 실행 뒤 셀 하나를 고치면 그 셀만 `stale: true`이고 출력은 남아 있습니다.
- 테스트: `test_fr_r4_document_maps_latest_outputs_and_marks_stale`, `test_fr_r4_document_without_runs`, Rust `fr_r4_document_is_built_by_the_embedded_python`(`darkpyonix/manager/crates/dpx-kernel/tests/discovery_launch.rs`)

### FR-R5 큰 출력 — `Done`
셀 하나의 스트림 출력이 기록 안에서 `DARKPYONIX_RUN_OUTPUT_LIMIT`(기본 16 MiB)를 넘으면, 넘는 부분은 `<run_id>.cell<index>.log`에 이어 쓰고 노트북에는 그 사실을 알리는 스트림 한 줄을 남깁니다. 실시간 이벤트에는 제한이 없습니다.
- 수용 기준: 20 MiB를 출력하는 셀의 기록 노트북이 17 MiB를 넘지 않고, 사이드카 로그와 합치면 전체 출력이 됩니다.
- 테스트: `test_fr_r5_oversized_stream_spills_to_sidecar`

## 5. 발견 (D)

### FR-D1 멀티캐스트 발견 — `Done`
PROTOCOL §2를 구현합니다. 매니저의 query에 같은 사용자의 모든 커널이 200 ms 안에 응답합니다.
- 수용 기준: 커널 세 개를 띄우고 query 하나를 보내면 세 개의 announce를 받습니다. 다른 `user_tag`의 query에는 응답하지 않습니다.
- 테스트: `test_fr_d1_query_finds_all_kernels`, `test_fr_d1_other_user_tag_is_ignored`, `test_fr_d1_listener_sees_announce_and_bye`, Rust `fr_d1_query_finds_all_kernels`(`darkpyonix/manager/crates/dpx-kernel/tests/discovery_launch.rs`)

### FR-D2 등록 파일 보조 — `Done`
커널은 announce 본문을 `kernels/<kernel_id>.json`에 둡니다. 매니저는 멀티캐스트 결과와 등록 파일을 합치되, `pid`가 살아 있지 않은 등록은 지웁니다. `DARKPYONIX_DISCOVERY=registry`이면 등록 파일만 씁니다.
- 수용 기준: 멀티캐스트를 끈 상태에서도 매니저가 커널을 찾습니다. `kill -9`로 죽은 커널의 등록은 다음 발견에서 사라집니다.
- 테스트: `test_fr_d2_registry_fallback_finds_kernels`, `test_fr_d2_stale_registry_is_pruned`, `test_fr_d2_registry_ignores_other_users`, Rust `fr_d2_registry_fallback_finds_kernels`, `fr_d2_stale_registry_is_pruned`(`darkpyonix/manager/crates/dpx-kernel/tests/discovery_launch.rs`)

## 6. 런타임 API와 파일 형식 (F)

### FR-F1 셀 파서 — `Done`
FORMAT §2의 문법(프리앰블, 셀 표식, 제목, 타입, 메타데이터, 셀 식별)을 파싱합니다. 파서는 커널, 매니저, 런타임 API가 공유하며 표준 라이브러리만 씁니다.
- 수용 기준: `docs/examples/darkpyonix_format.py`를 파싱하면 프리앰블 1개와 셀 22개(code 11, markdown 2, binding 2, argparse·shell·parallel·concurrent·cinterop·cppinterop·rustinterop 각 1)가 나오고, 타입, 제목, `@width` 메타데이터, `concorrunt`→`concurrent` 별칭이 FORMAT대로 나옵니다. 파싱 후 다시 직렬화하면 원문과 바이트 단위로 같습니다.
- 테스트: `test_fr_f1_reference_file_parses`, `test_fr_f1_parse_serialize_roundtrip`

### FR-F2 `darkpyonix.markdown` — `Done`
FORMAT §3.2. 커널 안에서는 `text/markdown` `display_data`를 내고(`silent=True`이면 기록에 남기지 않음), 커널 밖에서는 아무것도 하지 않습니다. 모르는 키워드 인자는 경고만 남깁니다.
- 테스트: `test_fr_f2_markdown_in_kernel_and_plain_python`(커널 밖 동작, 경고, `hostctx` 계약), `test_fr_f2_markdown_in_real_kernel_is_display_data_and_silent_is_not_logged`(실제 커널: 두 호출 모두 실시간 `output` 이벤트로 `text/markdown` `display_data`가 나가고, 실행 기록에는 `silent=True`가 아닌 것만 남음)

### FR-F3 `darkpyonix.params` — `Done`
FORMAT §3.3. 값의 우선순위는 실행 요청 `params` → 명령줄 `--name` → `default`입니다.
- 수용 기준: `choices`와 정수 `default`면 인덱스로 고르고, `range`를 벗어난 값은 `ValueError`입니다. `python file.py --model_id swin_t`가 `"swin_t"`를 냅니다.
- 테스트: `test_fr_f3_params_precedence_and_validation`

### FR-F4 `darkpyonix.binding` — `Done`
FORMAT §3.4. 이슈 #6의 참조 구현을 따르되, `binding` 데코레이터만 벗기고 다른 데코레이터는 보존합니다.
- 수용 기준: `[code]` 셀 변수를 참조하는 binding 클래스 본문은 `NameError`를 냅니다. import한 이름과 앞선 binding은 보입니다.
- 테스트: `test_fr_f4_binding_cannot_see_code_cell_variables`

### FR-F5 일반 파이썬과 같은 동작 — `Done`
노트북 파일을 `python file.py`로 실행한 결과(표준 출력, 종료 코드)가 커널 전체 실행의 스트림 출력과 같습니다. 마크다운 출력과 `display`의 MIME 번들은 이 비교에서 뺍니다. 종료 코드 0은 실행 상태 `ok`, 0이 아닌 종료 코드는 `error`에 대응합니다.
- 테스트: `test_fr_x1_run_all_matches_plain_python` (FR-X1과 공유), `test_fr_f5_reduced_reference_runs_under_plain_python`, `test_fr_f5_exit_code_matches_run_status`(실제 커널: 정상 종료 0 ↔ `ok`, 예외 1 ↔ `error`, `sys.exit(3)` ↔ `error`, 중간의 `sys.exit(0)` ↔ `ok`이고 남은 셀을 실행하지 않음. 각 경우 표준 출력도 같음)

### FR-F6 `darkpyonix.run_command` — `Done`
셸 명령을 하위 프로세스로 실행하고 출력을 줄 단위로 스트림 출력으로 보냅니다. `check=True`이면 실패 시 `CalledProcessError`입니다. 인터럽트가 오면 하위 프로세스 그룹에 SIGINT를 전달합니다.
- 테스트: `test_fr_f6_run_command_streams_and_forwards_interrupt`

## 7. 매니저 (M)

### FR-M1 HTTP API — `Done`
매니저는 [api/manager.openapi.yaml](api/manager.openapi.yaml)의 경로를 모두, 그리고 그 경로만 냅니다. 이벤트 스트림은 SSE(`text/event-stream`)이고 SSE `id`는 커널의 `seq`입니다. `Last-Event-ID` 헤더나 `since` 쿼리로 이어 받습니다.
- 테스트: Rust `test_nfr_m3_every_operation_answers_with_a_documented_status`, `test_nfr_m3_undocumented_methods_are_not_served`(`darkpyonix/manager/crates/dpx-server/tests/openapi.rs`), `test_fr_m1_events_stream_resumes_with_last_event_id`, `test_fr_m1_events_errors_and_keepalive`(`darkpyonix/manager/crates/dpx-server/tests/sse.rs`), 각 경로의 동작 테스트(`darkpyonix/manager/crates/dpx-server/tests/api.rs`). 파이썬 시제품 기준 `test_fr_m1_*`(`tests/test_fr_m_manager.py`)

### FR-M2 커널 시작은 멱등 — `Done`
`POST /kernels {path}`는 그 파일의 커널이 살아 있으면 그 커널을 `200`으로, 없으면 새로 띄워서 `201`로 돌려줍니다. 커널이 announce를 낼 때까지 최대 10초를 기다립니다.
- 테스트: `test_fr_m2_start_kernel_is_idempotent`(Rust `darkpyonix/manager/crates/dpx-server/tests/api.rs`, 파이썬 시제품), Rust `fr_m2_start_kernel_is_idempotent_on_every_interpreter`, `fr_m2_start_timeout_when_no_announce`, `fr_m2_ensure_starts_the_interpreter_without_a_discovery_wait`, `fr_m2_ensure_attaches_to_a_live_kernel_missing_from_the_registry`(`darkpyonix/manager/crates/dpx-kernel/tests/discovery_launch.rs`)

### FR-M3 임시 모드 수명 — `Done`
임시 매니저는 `127.0.0.1`의 임의 포트에 리슨합니다. `managers/<pid>.json`(0600)에 주소와 토큰을 쓰고, 열린 SSE 스트림이 없고 HTTP 요청도 없는 상태가 `idle_timeout`(기본 120초) 동안 이어지면 스스로 끝납니다. 끝날 때 등록을 지우고 커널은 건드리지 않습니다.
- 테스트: `test_fr_m3_ephemeral_manager_exits_when_idle_and_kernels_remain`(Rust `darkpyonix/manager/crates/dpx-server/tests/lifecycle.rs`, 파이썬 시제품), Rust `test_fr_m3_registry_file_is_private_and_complete`, `test_fr_m3_shutdown_removes_registry_file`

### FR-M4 전용 모드 — `Done`
`darkpyonix manager --dedicated`는 유휴 종료 없이 돌고, 설정한 호스트·포트에 리슨하고, 마스터 토큰과 공유 토큰으로 인증합니다. 토큰은 해시로만 `manager.db`(SQLite)에 저장합니다.
- 테스트: Rust `test_fr_m4_dedicated_manager_never_idles_and_requires_token`(`darkpyonix/manager/crates/dpx-server/tests/lifecycle.rs`), `test_fr_m4_shares_are_stored_hashed_and_survive_restart`(`darkpyonix/manager/crates/dpx-server/tests/api.rs`)

### FR-M5 매니저 여러 개 공존 — `Done`
같은 사용자의 매니저 여러 개가 같은 커널에 동시에 붙을 수 있고, 각자 같은 이벤트를 받습니다.
- 테스트: `test_fr_m5_two_managers_share_one_kernel`(파이썬 시제품), Rust `fr_m5_two_managers_share_one_kernel`, `fr_m5_reconnects_on_demand_after_kernel_restart`(`darkpyonix/manager/crates/dpx-kernel/tests/dkp_fake_kernel.rs`)

## 8. CLI (C)

### FR-C1 매니저 찾기 — `Done`
`darkpyonix` CLI는 `managers/*.json`에서 살아 있는 매니저를 고르고, 없으면 임시 매니저를 띄웁니다. 에이전트가 기존 매니저의 토큰을 몰라도 같은 OS 사용자라면 그대로 동작합니다.
- 테스트: Rust `test_fr_c1_cli_spawns_manager_when_none_is_running`, `test_fr_c1_skips_dead_and_unhealthy_registrations`, `test_fr_c1_spawn_failure_is_reported`(`darkpyonix/manager/crates/darkpyonix/tests/cli.rs`)

### FR-C2 명령 — `Done`

| 명령 | 동작 |
|---|---|
| `darkpyonix run FILE [--cells 1,3] [--param k=v]… [--python PATH] [--queue] [--detach]` | 실행. 기본은 출력을 따라가며 보여 주고, 종료 코드는 실행 상태(ok 0, error 1, interrupted 130)를 따릅니다. 따라가는 중의 Ctrl-C는 **실행 인터럽트**입니다 |
| `darkpyonix stop FILE` | 인터럽트 |
| `darkpyonix status [FILE]`, `darkpyonix ps` | 커널 상태 / 목록 |
| `darkpyonix logs FILE [--run latest\|ID] [--follow]` | 실행 기록 출력 |
| `darkpyonix attach FILE` | 이벤트 따라가기 |
| `darkpyonix vars FILE` | 네임스페이스 |
| `darkpyonix restart FILE [--hard]`, `darkpyonix shutdown FILE [--force]` | 재시작 / 종료. `--force`만 프로세스를 죽입니다 |
| `darkpyonix kernel FILE [--python PATH]` | 실행 없이 커널만 띄움 |
| `darkpyonix share FILE --permission viewer1\|viewer2\|viewer3` | 공유 토큰 발급(전용 매니저) |
| `darkpyonix manager [--ephemeral\|--dedicated] [--host H] [--port P] [--idle-timeout S]` | 매니저 실행. 기본은 `--ephemeral`(FR-M3: 루프백, 유휴 시 종료, `managers/<pid>.json`에 토큰), `--dedicated`는 FR-M4. 둘을 함께 주면 오류입니다 |

- 수용 기준: `darkpyonix run a.py`를 두 번째로 실행하면 종료 코드 75와 함께 현재 실행 정보와 `--queue`/`stop` 안내를 출력합니다.
- 테스트: Rust `test_fr_c2_cli_commands`, `test_fr_c2_second_run_exits_75_with_hint`, `test_fr_c2_run_follows_outputs_and_exits_0`, `test_fr_c2_run_error_exits_1_with_traceback`, `test_fr_c2_ctrl_c_interrupts_the_run_and_exits_130`, `test_fr_c2_second_ctrl_c_detaches_and_the_run_continues`, `test_fr_c2_detach_prints_the_run_id_and_does_not_follow`, `test_fr_c2_run_options_reach_the_api`, `test_fr_c2_logs_follow_replays_the_executing_run`(`darkpyonix/manager/crates/darkpyonix/tests/cli.rs`), `test_fr_c2_every_command_parses`, `test_fr_c2_invalid_usage_is_rejected`(`darkpyonix/manager/crates/darkpyonix/src/args.rs`)
- 상태 메모 (2026-10-03 감사): `attach`, 전용 매니저에서의 `share`, `manager` 하위 명령은 인자 파싱 시험만 있고 동작 시험은 없습니다.

## 9. 인증과 공유 (A)

### FR-A1 커널 인증 — `Done`
PROTOCOL §3.2의 HMAC 도전-응답입니다. 사용자 키가 없으면 처음 쓰는 쪽이 0600으로 원자적으로 만듭니다.
- 테스트: `test_fr_a1_wrong_key_is_rejected`, `test_fr_a1_hello_carries_identity_and_nonce`, `test_fr_a1_user_key_is_created_once_with_0600`

### FR-A2 매니저 토큰 — `Done`
모든 HTTP 요청은 `Authorization: Bearer <token>`이 필요합니다. 헤더를 붙일 수 없는 SSE(`EventSource`)와 공유 링크만 `?token=`을 받습니다. `/health`만 인증 없이 열립니다.
- 테스트: `test_fr_a2_requests_without_token_are_401`(Rust `darkpyonix/manager/crates/dpx-server/tests/api.rs`, 파이썬 시제품), Rust `test_fr_a2_registry_token_is_used`(`darkpyonix/manager/crates/darkpyonix/tests/cli.rs`)

### FR-A3 공유 권한 — `Done`
공유 토큰은 커널(파일)마다 발급하고 권한은 아래와 같습니다(2025 설계 유지).

| 권한 | 셀 코드 | 실행 기록·출력 | 실행·인터럽트 | 종료·공유 관리 |
|---|---|---|---|---|
| `viewer1` | ✓ | | | |
| `viewer2` | ✓ | ✓ | | |
| `viewer3` | ✓ | ✓ | ✓ | |
| `editor` | ✓ (편집·잠금 포함, FR-S8) | ✓ | ✓ | |
| `admin`(마스터) | ✓ | ✓ | ✓ | ✓ |

작업별 최소 권한: 커널·실행 기록 조회 `viewer1`(실행 기록과 출력은 `viewer2`부터), 네임스페이스 조회 `viewer2`, 실행·인터럽트·대기 실행 취소 `viewer3`, 재시작·종료·공유 관리·새 커널 시작 `admin`. 공유 토큰은 한 커널에만 묶이며, 다른 커널을 가리키면 `403`이 아니라 `404`입니다.
- 테스트: Rust `test_fr_a3_permission_matrix`, `test_fr_a3_share_tokens_are_scoped_to_one_kernel_and_permission`, `test_fr_a3_ephemeral_manager_refuses_share_creation`(`darkpyonix/manager/crates/dpx-server/tests/api.rs`), `test_fr_a3_viewer1_events_omit_outputs`(`darkpyonix/manager/crates/dpx-server/tests/sse.rs`)

## 10a. 협업 문서 (S)

한 커널(=파일)에 여러 클라이언트가 동시에 붙습니다. VS Code 확장, IntelliJ, ash, Ember 대화 화면, 에이전트가 함께 붙을 수 있습니다. 2025 설계의 셀 동기화, 셀 잠금, 포커스, 실행 알림, 알람을 이어받습니다(`설계초안/`의 WS 명세). 커널이 이 상태를 들고 있습니다. 매니저는 언제든 사라질 수 있고, 같은 파일에 서로 다른 매니저(로컬 임시 매니저와 전용 매니저)로 붙은 클라이언트도 같은 상태를 봐야 하기 때문입니다.

### FR-S1 공유 문서 상태 — `Done`
커널은 파일을 파싱한 문서(셀 목록)를 메모리에 두고 문서 버전 `doc_version`을 관리합니다. `doc_version`은 셀 생성·수정·삭제·이동과 바깥 편집으로 다시 읽기(`doc.reloaded`)에서만 1 늘어나고, 잠금·해제·충돌 표시·접속자 이벤트에서는 늘지 않습니다(그 이벤트들도 현재 `doc_version`을 담습니다, PROTOCOL §4). 셀마다 커널 수명 동안 바뀌지 않는 `cell_id`를 둡니다. 파일에 `# @id`가 있으면 그 값을 쓰고, 없으면 `c_<hex>`를 만들되 파일에는 쓰지 않습니다. 첫 동기화 스냅숏(`GET /kernels/{id}/document`)에는 셀(`cell_id`, 셀별 `version`, 소스, 최신 출력), 잠금, 접속자, `doc_version`, 그리고 `seq`가 함께 들어 있습니다. `seq`는 **스냅숏에 이미 반영된 마지막 이벤트의 번호**이고, 클라이언트는 `since=seq`로 구독해 `seq`보다 큰 이벤트만 적용합니다. 커널은 상태 변경·이벤트 발행·스냅숏을 한 잠금 안에서 하므로, 스냅숏과 경쟁한 편집은 빠지지도 두 번 적용되지도 않습니다. 셀의 `source`는 파서가 낸 본문 그대로(다음 표식 앞의 빈 줄 포함)이고, `type`은 정규 타입, `raw_type`은 표식에 쓰인 타입 원문(없으면 `null`)입니다.
- 수용 기준: 두 클라이언트가 같은 스냅숏을 받은 뒤 한쪽이 편집하면, 다른 쪽은 이벤트만으로 같은 문서 상태에 도달합니다(셀 순서, 소스, 버전이 같음).
- 테스트: `test_fr_s1_snapshot_plus_events_converge`, `test_fr_s1_snapshot_racing_edits_is_exact`, `test_fr_s1_doc_version_bumps_only_on_content`, `test_fr_s1_s2_s3_s5_s8_two_clients_edit_converge_save_and_run` (실제 커널 프로세스)
- 상태 메모: 커널의 `doc.snapshot`과 `doc.*` 이벤트를 검증했습니다. 스냅숏에 셀별 최신 출력을 합치는 일(`cell.*` 이벤트의 `cell_id`와 실행 기록으로)은 매니저가 맡고, Rust `test_fr_s1_document_combines_snapshot_and_outputs`, `test_fr_s1_snapshot_cells_find_their_outputs_after_a_move`(`darkpyonix/manager/crates/dpx-server/tests/collab.rs`)가 검증합니다.

### FR-S2 셀 편집 — `Done`
셀 생성(위치는 `after`/`before` `cell_id`, 둘 다 없으면 문서 끝에 붙임; 타입, 제목, 소스, 메타데이터), 소스·타입·제목·메타데이터 수정, 삭제, 이동을 지원합니다. 수정은 `base_version`(그 셀의 버전)을 받고, 다르면 `409 conflict`와 현재 셀을 돌려줍니다. 다른 클라이언트가 잠근 셀의 수정·삭제는 `409 locked`입니다. 편집마다 `doc.cell.*` 이벤트를 모든 구독자에게 보내고, 이벤트에는 누가 했는지(`by`)가 들어갑니다. 편집·잠금·접속자 요청은 클라이언트가 고른 `request_id`(1–64자)를 받을 수 있고, 그 요청으로 생긴 이벤트에 그대로 돌아옵니다. 같은 `client_id`를 쓰는 두 창이 자기 편집을 구별하는 데 씁니다(HTTP 헤더 `X-DarkPyonix-Request`).
- 수용 기준: 버전이 맞지 않는 수정은 거절되고 문서는 바뀌지 않습니다. 생성·수정·삭제·이동이 이벤트로 퍼집니다.
- 테스트: `test_fr_s2_edit_ops_and_version_conflict`, `test_fr_s2_cells_source_type_title_and_append`, `test_fr_s2_request_id_is_echoed`, `test_fr_s1_s2_s3_s5_s8_two_clients_edit_converge_save_and_run`

### FR-S3 셀 잠금 — `Done`
편집을 시작하는 클라이언트는 그 셀을 잠급니다(2025 설계의 `start_typing`). 잠금은 셀마다 하나이고 `locked_by`(클라이언트), 사용자 이름, `locked_at`, `last_activity`를 가집니다. 잠근 클라이언트가 수정할 때마다 `last_activity`가 갱신됩니다. 잠금은 세 경우에 풀립니다. 잠근 클라이언트가 해제할 때(최종 소스를 함께 보낼 수 있음, 2025 `cell_unlocked_with_code`), 3분 동안 활동이 없을 때, 그 클라이언트가 접속을 끊을 때입니다. 잠금과 해제는 `doc.lock`/`doc.unlock` 이벤트로 퍼집니다.
- 수용 기준: 잠긴 셀을 다른 클라이언트가 잠그면 `409 locked`와 `locked_by`를 받습니다. 3분 무활동 뒤에는 자동으로 풀립니다(테스트에서는 시간을 줄임). 접속이 끊긴 클라이언트의 잠금도 풀립니다.
- 테스트: `test_fr_s3_lock_exclusive_idle_release_and_disconnect`, `test_fr_s1_s2_s3_s5_s8_two_clients_edit_converge_save_and_run`

### FR-S4 접속자, 포커스, 커서 — `Done`
클라이언트는 접속할 때 `client_id`(기기마다 고유)와 `nickname`(기기 이름, 2025 `?nickname=`)을 알립니다. 사용자 이름과 아바타는 토큰에서 정해지고, 없으면 클라이언트가 준 값을 씁니다. 접속자 목록은 다음을 담습니다: 사용자, 기기, 권한, 포커스한 셀(`focused_cell_id`, `focused_at`), 커서(`cell_id`, `line`, `column`, 선택 범위). 포커스, 블러, 커서 변경은 `presence.update` 이벤트로 퍼집니다(커서는 클라이언트마다 초당 최대 20회로 합칩니다). 이벤트 스트림이 끊기고 30초가 지나면 그 클라이언트는 `presence.leave`가 됩니다.
- 테스트: `test_fr_s4_presence_focus_cursor_and_leave`, `test_fr_s4_presence_and_leave_release_locks_through_kernel`
- 상태 메모: 커널 쪽(`presence.update`/`presence.leave`, 30초 유예)을 검증했습니다. 이벤트 스트림이 열려 있는 동안 약 10초마다 `presence.update` 하트비트를 보내는 일은 매니저가 맡고, Rust `test_fr_s4_event_stream_with_client_id_heartbeats_presence`(`darkpyonix/manager/crates/dpx-server/tests/collab.rs`)가 검증합니다.

### FR-S5 디스크 파일과의 동기화 — `Done`
- 클라이언트 편집은 300ms 디바운스 뒤 파일에 원자적으로 저장합니다(FORMAT 직렬화, 손대지 않은 셀은 바이트 그대로).
- 바깥에서 파일이 바뀌면(에이전트의 Edit 도구, `git checkout`, 다른 편집기) 커널이 1초 안에 알아채고 다시 파싱합니다. 셀을 `cell_id` → `source_sha256` → 순서로 맞추고 `doc.reloaded` 이벤트를 보냅니다.
- 그때 잠긴 셀이 바깥에서도 바뀌었으면, 잠근 쪽의 내용을 지우지 않습니다. 그 셀을 `conflict`로 표시하고 두 버전을 모두 이벤트에 담습니다. 잠근 클라이언트가 해제하거나 다시 수정하면 충돌이 풀립니다.
- 수용 기준: 커널이 붙어 있는 동안 파일을 밖에서 고치면 모든 클라이언트에 반영됩니다. 클라이언트 편집이 파일에 저장되고, 고치지 않은 셀의 바이트는 그대로입니다.
- 테스트: `test_fr_s5_external_edit_reloads_and_locked_cell_conflicts`, `test_fr_s5_client_edit_is_saved_byte_exact`, `test_fr_s1_s2_s3_s5_s8_two_clients_edit_converge_save_and_run`

### FR-S6 실행한 사람 표시 — `Done`
`run.queued`, `run.started`, `run.finished`, `cell.*` 이벤트와 실행 기록 메타데이터에 실행을 요청한 클라이언트(`started_by`: client_id, 사용자, 기기)를 넣습니다. 인터럽트하면 `interrupted_by`도 넣습니다(2025 `execution_started.started_by`, `execution_interrupted.interrupted_by`). 셀을 실행하는 요청에는 `cell_ids`를 쓸 수 있습니다(인덱스 `cells`와 둘 중 하나). `run.started`는 `cells`와 나란한 `cell_ids`를, `cell.*`·`output`·`output.clear`는 공유 문서의 `cell_id`를 담아, 실행 중에 셀이 옮겨져도 출력이 맞는 셀로 갑니다.
- 테스트: `test_fr_s6_runs_are_attributed`, `test_fr_s6_run_and_output_events_carry_cell_ids`, `test_fr_s6_output_clear_carries_cell_id`

### FR-S7 알람 — `Done`
SSE를 계속 붙잡을 수 없는 클라이언트(모바일 백그라운드, 웹훅 대체)를 위해 롱폴링을 둡니다. `GET /kernels/{id}/runs/{run_ref}/wait?timeout=`는 그 실행이 끝나면 바로, 아니면 `timeout`(기본 60초, 최대 300초) 뒤에 돌려줍니다. 응답은 끝났을 때 실행 요약, 아직이면 `status: running`과 진행 정보이고, 다시 부를 때 쓸 `next` 정보가 들어 있습니다(2025 `executions/{cell_id}/wait`의 timeout·재폴링 모델). 실행이 끝나면 `run.finished` 이벤트가 모든 구독자에게 가므로, Ember는 이 이벤트로 휴대폰 푸시를 보냅니다.
- 테스트: `test_fr_s7_wait_returns_on_finish_or_timeout`
- 상태 메모: 커널의 `runs.wait`(끝나면 바로, 아니면 `timeout` 뒤)를 검증했습니다. HTTP 응답의 `next` 정보는 매니저가 붙이고, Rust `test_fr_s7_wait_returns_on_finish_or_timeout`(`darkpyonix/manager/crates/dpx-server/tests/collab.rs`)가 검증합니다.

### FR-S8 권한 — `Done`
셀 편집(FR-S2)과 잠금(FR-S3)은 `editor` 이상만 할 수 있습니다. `editor`는 `viewer3`(실행 가능)에 셀 편집을 더한 공유 권한이고, 2025 설계의 `user_permission: "write"`에 해당합니다. 접속자 표시와 포커스(FR-S4)는 `viewer1`부터 할 수 있습니다. FR-A3 표에 `editor`를 더합니다.
- 테스트: `test_fr_a3_permission_matrix` (FR-A3과 공유), `test_fr_s8_edit_and_lock_need_editor`, `test_fr_s1_s2_s3_s5_s8_two_clients_edit_converge_save_and_run`
- 상태 메모: 커널의 권한 검사(`client.permission`)를 검증했습니다. 매니저 쪽은 Rust `test_fr_a3_permission_matrix`(`darkpyonix/manager/crates/dpx-server/tests/api.rs`)와 `test_fr_s8_permission_matrix_for_collaboration`(`darkpyonix/manager/crates/dpx-server/tests/collab.rs`)가 검증합니다.

## 10. 허브 (H)

전송은 iroh 1.x로 정했습니다(PROJECT Q1, 2026-10-03, 조건부: ember SPEC NFR-N1을 못 맞추면 직접 구현을 검토). 그래서 허브는 직접 만든 랑데부·중계 대신 iroh가 이미 쓰는 프로토콜을 그대로 받습니다. 기기는 iroh 엔드포인트이고, 기기 ID는 그 엔드포인트 ID(ed25519 공개 키, 소문자 hex 64자)입니다.

**호스트 역할 분담(INTENT D15와 그 개정, 2026-10-03, 개정은 제안).** darkpyonix.dev의 DNS는 Cloudflare이고, 허브는 가벼운 부분을 Cloudflare Workers에서 돌립니다(사용자 결정). Workers와 Containers는 들어오는 UDP를 받지 못하므로 iroh 릴레이만 따로 둡니다. 동적 기능은 서브도메인에 두고, 루트는 그것을 띄우는 정적 메인 페이지입니다. 프로젝트 가이드는 지금처럼 `darkpyonix.dev/<저장소>/`에 있습니다(FR-H12).

| 호스트 | 구현 | 맡는 일 |
|---|---|---|
| `https://darkpyonix.dev/` | 정적 메인 페이지: darkpyonix-ash 웹 빌드, 조직 GitHub Pages 사이트 `DarkPyonix/DarkPyonix.github.io` | 랜딩, ash 웹 앱, 노트북 페이지(FR-H16, FR-H17), 공유 링크 페이지(FR-H4), 기기 승인 페이지(FR-H1), Flathub 검증 파일(FR-H7). 서버 코드와 쿠키가 없습니다 |
| `https://darkpyonix.dev/<저장소>/` | 각 저장소의 GitHub Pages 프로젝트 사이트 | 프로젝트 가이드(예: `/dioxus-compose/`). 허브의 일이 아니고, 메인 페이지와 경로가 겹치지 않게만 합니다(FR-H12) |
| `https://api.darkpyonix.dev` | Cloudflare Worker `hub/worker/` (TypeScript, D1, R2, Cron) | GitHub 로그인(FR-H6, FR-H13), 기기 등록(FR-H1), 주소 디렉터리(FR-H2), 릴레이 입장 판정 API(FR-H3), 공유 해석(FR-H4), 이름과 ACME TXT(FR-H5), 설정 발견(FR-H8), 노트북 보관(FR-H14~H19) |
| `https://relay.darkpyonix.dev` | 릴레이 호스트 `hub/server/` (Rust, `iroh-relay` 서버 크레이트) | iroh 릴레이 `/relay`, `/ping`, `/generate_204`, UDP 7842의 QUIC 주소 발견(QAD). 누구를 들일지는 Worker에 묻습니다 |

- API Worker는 `api.darkpyonix.dev`에만 붙습니다(custom domain). apex(`darkpyonix.dev`)는 지금처럼 GitHub Pages를 가리킵니다(DNS only). `relay.darkpyonix.dev`는 Cloudflare 프록시를 끈(DNS only) A/AAAA 레코드로 릴레이 호스트를 가리킵니다. 프록시는 UDP 7842를 넘기지 않고, QAD는 릴레이 호스트 자신의 TLS 인증서를 쓰기 때문입니다.
- Worker의 부하: iroh `PkarrPublisher`는 5분마다(그리고 주소가 바뀔 때) 다시 올립니다. 기기 10대면 하루 약 3,000번의 `PUT /pkarr`와 그만큼의 D1 쓰기 두 번이고, 요청마다 ed25519 검증 한 번과 D1 질의 몇 개입니다. Workers 무료 한도(하루 10만 요청, D1 쓰기 10만)의 몇 % 수준이라 "가볍다"는 조건을 만족합니다. 운영은 CPU 한도 여유를 위해 Workers Paid를 권합니다.
- 언어는 TypeScript입니다. 근거는 INTENT D15에 있습니다.

**이행 메모(D15 개정, 제안).** 저장소 기준으로 Worker는 아직 배포되지 않았습니다(`wrangler.toml`의 `database_id`가 자리표시값). 그래서 옛 apex API와의 호환 기간은 두지 않고, 개정이 승인되면 한 번에 바꿉니다.

| 이미 만든 것 | 바뀌는 것 |
|---|---|
| Worker 라우트 `darkpyonix.dev`(custom domain), 변수 `PUBLIC_URL` | 라우트 `api.darkpyonix.dev`, 변수 `API_URL`·`FRONT_URL`(FR-H8). R2 바인딩 `NOTEBOOKS`와 마이그레이션 `0007_notebooks.sql` 추가 |
| FR-H4 자리표시 뷰어(`hub/worker/public/ash/`, `GET /ash/`, `GET /s/{share_id}`), `/link` HTML 페이지 | Worker의 정적 자산과 HTML 페이지를 걷어 내고 메인 페이지(darkpyonix-ash)로 옮김. 계약에서 `/ash/`, `/s/{share_id}`, `/link`, `/.well-known/org.flathub.VerifiedApps.txt` 연산을 뺌 |
| 쿠키 `__Host-dp_session`, `__Host-dp_oauth`(apex) | 같은 이름으로 `api.darkpyonix.dev`에. 출처 검사는 `FRONT_URL` 기준(FR-H13). 배포 전이라 옮길 세션이 없음 |
| GitHub OAuth App 콜백 `https://darkpyonix.dev/auth/callback` | `https://api.darkpyonix.dev/auth/callback`, `return_to`는 프런트 경로(FR-H6 개정) |
| 릴레이 호스트의 입장·presence 호출 대상(apex) | `https://api.darkpyonix.dev/internal/v1/relay/*` |
| Ember의 기본 허브 URL `https://darkpyonix.dev`(darkpyonix-ember `hub/src/config.rs` `DEFAULT_HUB_URL`과 그 시험들) | `https://api.darkpyonix.dev`. 그 뒤의 주소는 모두 `GET /v1/config`(FR-H8)에서 읽음 |
| 조직 Pages `DarkPyonix.github.io`(CNAME `darkpyonix.dev`, 지금 `README.md`·`CNAME`뿐), apex의 GitHub Pages A/AAAA 레코드 | 저장소와 DNS는 그대로. darkpyonix-ash CI가 그 저장소 `main`에 빌드 결과를 커밋함(배포 키 하나). 프로젝트 가이드 주소는 바뀌지 않음(FR-H12) |

계약은 [api/hub.openapi.yaml](api/hub.openapi.yaml) 하나이고, 릴레이 호스트가 답하는 연산은 경로 단위 `servers: relay.darkpyonix.dev`로 표시합니다. Worker 테스트가 Worker의 모든 연산이 문서의 상태 코드로만 답하고 Worker의 라우트와 문서의 연산이 정확히 같음을 확인합니다(`test_hub_every_operation_answers_with_a_documented_status`, `test_hub_every_worker_route_is_documented_and_vice_versa`). 릴레이 호스트의 같은 이름 테스트(`hub/server/tests/hub/openapi.rs`)는 `servers`가 붙은 연산만 확인합니다.

전송 계층 교체 가능성: 허브가 iroh에 묶이는 곳은 릴레이 호스트와 주소 레코드 형식(pkarr 서명 패킷)뿐입니다. 계정, 기기 등록, 공유, 이름은 "ed25519 공개 키 하나 = 기기"라는 가정만 씁니다. 직접 구현으로 바꾸면 그 두 곳만 바꿉니다.

**인증 모델.** 계정은 GitHub 사용자입니다(FR-H6). 사람은 브라우저에서 GitHub로 로그인해 `api.darkpyonix.dev`의 호스트 전용 세션 쿠키(`__Host-dp_session`)를 받고(프런트는 자격 증명을 실은 CORS 요청으로 씁니다, FR-H13), 기기는 기기 링크(FR-H1)로 계정에 들어와 기기 토큰을 받습니다. "계정 권한"은 로그인한 세션 또는 그 계정의 `main_server` 기기 토큰입니다. 단, `main_server` 역할을 들이는 승인(메인 서버 바꾸기 포함, FR-H1)은 세션만 할 수 있습니다. 토큰과 세션 ID는 SHA-256 해시로만 저장하고, GitHub 액세스 토큰은 사용자 정보를 한 번 읽은 뒤 바로 폐기(revoke)하며 저장하지 않습니다. 쿠키로 인증한 쓰기 요청은 `Origin`이 프런트(`https://darkpyonix.dev`)가 아니면 403입니다(FR-H13). 사용자 이름 서브도메인(`<name>.darkpyonix.dev`, FR-H5)은 같은 사이트이지만 남의 서버이므로 신뢰하지 않습니다. 자격 증명 오류(401)의 본문은 `{"error": <설명>, "code": <코드>}`이고, `code`는 `device_removed`(지운 기기의 토큰. 다시 시도해도 소용없으니 기기는 토큰을 버리고 사용자에게 알립니다) 또는 `invalid_credentials`(없거나 모르는 토큰·세션)입니다. 상태 코드는 둘 다 401입니다. 410은 자원이 사라졌다는 뜻이지 자격 증명이 틀렸다는 뜻이 아니고, 401을 유지하면 "401이면 다시 인증"하는 기존 클라이언트가 그대로 동작합니다(FR-H1). OpenAI 로그인("Sign in with ChatGPT")과 ChatGPT 플랜 사용은 허브 기능이 아니고, 사용자가 직접 띄운 ember server가 합니다(PROJECT Q2).

### FR-H1 기기 등록 — `Agreed`
기기는 iroh 엔드포인트 ID로 계정에 들어옵니다. 흐름은 OAuth 기기 인증(RFC 8628) 모양에 키 소유 증명을 더한 **기기 링크**입니다.
1. 기기가 `POST /v1/device-links {endpoint_id, name, role}`로 요청하고 `link_id`, 사용자 코드(`BCDF-GHJK` 형식, 모음 없는 20글자), 챌린지, 만료(15분)를 받습니다.
2. 사람이 `https://darkpyonix.dev/link?code=<사용자 코드>`(프런트 페이지, FR-H12)를 열어 GitHub로 로그인한 상태에서 기기 이름·역할·엔드포인트 ID를 확인하고 승인하거나 거절합니다(`POST /v1/link-codes/{user_code} {"approve": bool}`). 같은 계정의 메인 서버도 기기 토큰으로 승인할 수 있어서, 새 컴퓨터는 브라우저 없이 메인 서버(ember server)를 거쳐 들어올 수 있습니다. `computer` 기기 토큰으로는 승인할 수 없습니다(403). **`main_server` 역할을 요청한 링크는 로그인한 브라우저 세션만 승인할 수 있습니다.** 메인 서버의 기기 토큰으로 그런 링크를 승인하면 403이고(거절은 됩니다), 그래서 새어 나간 메인 서버 토큰 하나로 계정 권한을 가진 기기를 더 만들 수 없습니다. 사용자 코드는 짧으므로 코드 조회와 결정은 계정당 분당 30번으로 제한합니다(429). 링크 요청도 주소당 분당 30번입니다.
3. 기기는 `interval`마다 `POST /v1/device-links/{link_id}/token`을 부르며, 매번 `darkpyonix-hub/v2/link\n<link_id>\n<challenge>`에 대한 자기 키 서명을 냅니다. 결정 전에는 202, 승인되면 한 번만 201과 기기 토큰, 거절되면 403입니다.
4. 다시 시작한 기기는 `GET /v1/device-links/{link_id}`로 링크 상태(`pending`, `approved`, `denied`, `claimed`, `expired`)와 사용자 코드, 챌린지, 만료를 다시 읽습니다. 자격 증명은 필요 없습니다. `link_id`는 128비트 난수라 기기만 알고, 챌린지는 비밀이 아니며(서명에는 기기 키가 필요), 토큰은 이 응답에 없습니다. 만료된 링크는 시간마다 지워지므로 그 뒤에는 404입니다. `claimed`인데 기기가 토큰을 잃었다면 그 키는 지우고 다시 들입니다(FR-H11).

**역할 [provisional].** 기기의 역할은 셋입니다. 권한이 가장 좁은 쪽을 기본으로 둡니다.

| 역할 | 무엇 | 계정 권한 | 기기 지우기 | 이름(FR-H5) | 공유 게시(FR-H4) | 주소 게시·조회, 릴레이 |
|---|---|---|---|---|---|---|
| `main_server` | 사용자의 메인 서버(ember server). 계정에 하나 | 있음 | 자기 자신, `computer`, `client` | 가짐 | 됨 | 됨 |
| `computer` | 커널을 돌리는 다른 컴퓨터 | 없음 | 자기 자신만 | 못 가짐(403) | 됨 | 됨 |
| `client` | 남의 기기에 붙기만 하는 기기(휴대폰, 노트북의 ember 앱) | 없음 | 자기 자신만 | 못 가짐(403) | 안 됨(403) | 됨 |

로그인한 브라우저 세션은 계정의 어느 기기든 지울 수 있습니다. 메인 서버는 계정에 하나뿐이므로(아래) 메인 서버 토큰이 지울 다른 `main_server`는 없고, 메인 서버가 다른 기기로 넘어가는 것은 **바꾸기 승인**으로만 일어납니다. 그 승인은 `main_server` 링크 승인이라 세션만 할 수 있으므로, 새어 나간 메인 서버 토큰 하나로 진짜 메인 서버를 밀어내고 이름을 가져갈 수 없습니다. 메인 서버가 `computer`·`client`를 지우는 것은 그대로 둡니다(브라우저 없이 기기를 정리하는 것이 메인 서버의 역할이고, 그 기기들은 계정 권한이 없어 잃는 것이 적습니다).

`client` 링크는 `computer` 링크처럼 세션이나 메인 서버 토큰이 승인합니다. `client`가 공유를 게시하지 못하는 이유: 공유는 그것을 연 기기로 손님을 들이는 것(FR-H4)인데, 아무것도 서비스하지 않는 기기가 손님 통행권을 만들 이유가 없고, 잃어버리기 쉬운 휴대폰 토큰으로 할 수 있는 일을 줄입니다. 주소 게시는 허용합니다(상대가 휴대폰으로 되걸 수 있게). 역할은 링크로 정하고 바꾸지 않습니다. 바꾸려면 지우고 다시 들입니다(FR-H11). Ember의 실제 사용으로 확정할 때까지 `[provisional]`입니다.

**메인 서버는 계정에 하나 [Ember INTENT D3].** 메인 서버 한 대가 모든 대화·셸·계정을 가진다는 사용자 결정(darkpyonix-ember `docs/INTENT.md` D3, `[user]`)에 따라, 계정에는 지우지 않은 `main_server` 기기가 많아야 하나입니다.
- 메인 서버가 없는 계정에서는 `main_server` 링크를 보통처럼 `{"approve": true}`로 승인합니다.
- 메인 서버가 있는 계정에서 `main_server` 링크를 `{"approve": true}`로만 승인하면 409이고 `code`는 `main_server_exists`입니다. 승인하려면 **바꾸기**를 분명히 적습니다: `{"approve": true, "replace": "<지금 메인 서버의 endpoint_id>"}`. `replace`가 지금 메인 서버가 아니면(다른 기기이거나 메인 서버가 없으면) 409 `replace_mismatch`입니다. `replace`는 `main_server` 링크를 승인할 때만 쓰고, 거절이나 다른 역할의 링크에 붙이면 400입니다. 승인 화면이 무엇을 바꾸는지 보여 줄 수 있도록 `GET /v1/link-codes/{user_code}`는 `main_server` 링크에 대해 계정의 지금 메인 서버(`current_main_server: {endpoint_id, name}`, 없거나 다른 역할의 링크면 `null`)를 함께 줍니다.
- 바꾸기는 새 기기가 토큰을 받을 때(3단계) 한 트랜잭션으로 일어납니다: 옛 메인 서버를 지우고(그 토큰은 401 `device_removed`, 지운 기기 목록에 나옴, 공유·주소 레코드는 지우고 릴레이 연결을 끊음), **옛 메인 서버의 이름(FR-H5)을 새 메인 서버로 옮기고**, 새 기기를 등록합니다. 이름을 옮기는 이유: 사용자의 주소(`https://<name>.darkpyonix.dev`)가 기계를 바꿔도 그대로 통해야 하기 때문입니다. 옛 메인 서버가 게시한 ACME TXT는 지우고, 새 메인 서버가 자기 키로 인증서를 다시 받습니다.
- 승인 뒤 받기 전에 계정의 메인 서버가 바뀌었으면(그사이 다른 `main_server` 링크가 먼저 받음) 받기는 409 `main_server_exists`이고 그 링크는 거절 상태가 됩니다. 계정당 하나는 D1의 부분 유일 인덱스(`role = 'main_server' AND revoked_at IS NULL`)가 보장합니다.
- 메인 서버를 지우면 계정에 메인 서버가 없어지고, 그 뒤의 `main_server` 링크는 `replace` 없이 승인합니다. 지운 메인 서버를 다시 들일 때도(FR-H11) 같은 규칙입니다.

기기는 계정 하나에만 속하고, 기기 목록과 조회는 같은 계정 안에서만 보입니다. 기기 이름은 `PATCH /v1/devices/{endpoint_id} {"name": …}`로 바꿉니다(1~64자). 계정 권한이나 그 기기 자신만 바꿀 수 있고, 다른 `computer`/`client` 토큰은 403입니다. 기기를 지우면(위 표의 권한: 세션, `computer`·`client`를 지우는 메인 서버 토큰, 또는 그 기기 자신의 기기 토큰: 앱을 지우거나 계정에서 나갈 때 기기가 스스로 나갑니다) 그 키는 폐기되어 계정 주인이 다시 들이기 전에는(FR-H11) 다시 등록할 수 없고, 그 기기의 이름·공유·주소 레코드가 지워지며, Worker가 릴레이 호스트에 연결을 끊으라고 알립니다(`POST /admin/v1/disconnect`, FR-H3).
- 수용 기준: 두 엔드포인트가 기기 링크로 등록되면 계정의 기기 목록에 두 엔드포인트 ID가 나옵니다. 다른 키의 서명이나 다른 메시지의 서명은 400입니다. 승인 전 폴링은 202, 거절된 링크는 403, 한 번 받은 링크를 다시 받으면 404입니다. 링크 상태 조회는 대기·승인·받음·거절·만료를 그대로 보여 주고, 모르는 링크는 404입니다. 메인 서버 토큰은 `computer` 링크를 승인하지만 `main_server` 링크의 승인은 403이고, 같은 링크를 세션은 승인합니다. 이미 등록되었거나 지운 키의 링크 요청은 409입니다. 기기 이름은 세션과 그 기기 자신이 바꾸고 다른 `computer` 토큰은 403, 빈 이름은 400입니다. `client`로 들어온 기기는 목록·주소 게시와 조회가 되고, 공유 게시·이름 예약·승인은 403입니다. 다른 계정에서는 그 기기가 보이지 않습니다(404). `computer` 기기 토큰으로 다른 기기를 지우면 403이고 자기 자신은 지울 수 있습니다(204). 메인 서버 토큰으로 `computer`를 지우고 자기 자신도 지울 수 있습니다(204). 메인 서버가 있는 계정에서 `replace` 없는 `main_server` 승인은 409 `main_server_exists`, 지금 메인 서버가 아닌 `replace`는 409 `replace_mismatch`, 거절이나 `computer` 링크에 붙인 `replace`는 400이고, 링크 코드 조회는 지금 메인 서버를 `current_main_server`로 보여 줍니다. `replace`로 승인하고 받으면 기기 목록의 `main_server`는 새 기기 하나이고, 옛 메인 서버의 토큰은 401 `device_removed`이며 지운 기기 목록에 `main_server`로 나오고, 그 이름은 새 메인 서버의 것이 되며 옛 ACME TXT와 공유는 지워지고 릴레이에 끊기를 알립니다. 메인 서버가 없을 때 승인된 `main_server` 링크 둘 중 먼저 받은 것은 201, 나중 것은 409 `main_server_exists`이고 그 링크는 `denied`입니다. 지운 기기의 토큰은 401이고 `code`가 `device_removed`이며, 모르는 토큰은 401에 `invalid_credentials`입니다. 실제 iroh 엔드포인트(Rust `SecretKey::sign`)의 서명이 받아들여지는 것은 ember 전송 크레이트 연동 시험에서 확인합니다.
- 테스트(`hub/worker/test/devices.test.ts`): `test_fr_h1_register_two_iroh_endpoints`, `test_fr_h1_link_shows_code_and_polls_pending_until_approved`, `test_fr_h1_registration_requires_key_possession`, `test_fr_h1_denied_link_is_refused`, `test_fr_h1_restarted_device_reads_its_link_status`, `test_fr_h1_main_server_approves_computers_but_a_computer_cannot`, `test_fr_h1_only_a_session_approves_a_main_server_link`, `test_fr_h1_devices_are_scoped_to_their_account`, `test_fr_h1_removed_device_is_revoked`, `test_fr_h1_removed_device_token_is_told_apart_from_a_bad_token`, `test_fr_h1_a_device_removes_itself_but_not_others`, `test_fr_h1_only_a_session_removes_another_main_server`, `test_fr_h1_client_role_joins_and_connects_but_cannot_share_or_name`, `test_fr_h1_rename_by_the_device_or_the_account`, `test_fr_h1_link_request_is_validated`

### FR-H2 주소 디렉터리와 발견 — `Agreed`
기기는 현재 iroh 주소(릴레이 URL과 직접 주소)를 자기 키로 서명한 pkarr 패킷으로 허브에 올리고, 같은 계정의 기기는 엔드포인트 ID만으로 서로의 주소를 찾습니다.
- 프로토콜: iroh의 pkarr 릴레이 HTTP 프로토콜을 그대로 씁니다. `PUT /pkarr/<z32 키>`로 올리고 `GET /pkarr/<z32 키>`로 받습니다. 본문은 `서명(64) || 타임스탬프 µs 빅엔디언(8) || DNS 패킷(최대 1000바이트)`이고, 서명 대상은 BEP 44 형식 `3:seqi<ts>e1:v<len>:<dns>`입니다(iroh-dns 1.3 `SignedPacket`). Worker는 이 형식과 DNS 응답 파싱(이름 압축 포함), `_iroh` TXT의 `relay=`/`addr=` 속성 해석을 TypeScript로 다시 구현합니다. 그래서 iroh의 기본 `PkarrPublisher`·`PkarrResolver`를 `https://api.darkpyonix.dev/pkarr?token=<조회 토큰>`(D15 개정 전에는 apex)에 그대로 붙일 수 있습니다. 쿼리 문자열에는 기기 토큰이 아니라 조회 전용 토큰만 넣습니다(NFR-H2). 같은 내용을 JSON으로 보는 `GET /v1/devices/{endpoint_id}/addresses`도 둡니다.
- 받는 쪽 검사: 서명이 맞고, DNS 패킷이 파싱되고, 키가 폐기되지 않은 등록 기기이고, 타임스탬프가 저장된 것보다 큰 것만 받습니다(아니면 400/403/409). "더 새 것만"은 D1의 조건부 upsert 한 문장이라 동시 요청에도 원자적입니다. 너무 잦은 게시는 429입니다(Workers Rate Limiting, 키당 분당 30번).
- 조회 범위: `GET`은 같은 계정의 기기 토큰이나 세션이 있어야 합니다. 조회가 공개되지 않으므로 기기는 직접 주소까지 올려도(`AddrFilter::unfiltered`) 공인 IP가 계정 밖으로 새지 않습니다.
- DNS 발견(iroh-dns-server, `_iroh.<z32>.<도메인>` TXT)은 쓰지 않습니다. DNS 질의에는 계정 범위를 걸 수 없고, 우리 기기는 모두 허브와 HTTPS로 말하므로 얻는 것이 없습니다.
- 수용 기준: 기기가 올린 패킷을 같은 계정의 기기가 `?token=<조회 토큰>`으로 바이트 그대로 받고, JSON 조회가 릴레이 URL·직접 주소·타임스탬프를 풀어 냅니다. 같거나 오래된 타임스탬프는 409, 등록되지 않은 키의 `PUT`은 403, 남의 키로 서명한 패킷은 400, 토큰 없는 `GET`은 401, 다른 계정의 `GET`은 404입니다. 두 실제 iroh 엔드포인트가 기본 `PkarrPublisher`/`PkarrResolver`로 이 Worker를 거쳐 연결하는 것은 배포 후 연동 시험으로 확인합니다(Rust 쪽이 만든 패킷 바이트를 시험 벡터로 Worker 테스트에 넣는 것도 그때 함).
- 테스트(`hub/worker/test/pkarr.test.ts`, `directory.test.ts`): `test_fr_h2_z32_round_trips_a_published_pkarr_key`, `test_fr_h2_verifies_an_iroh_style_record_and_decodes_addresses`, `test_fr_h2_rejects_a_tampered_or_foreign_packet`, `test_fr_h2_dns_parser_follows_compression_pointers`, `test_fr_h2_publish_and_resolve_like_stock_iroh`, `test_fr_h2_only_newer_packets_replace_the_stored_one`, `test_fr_h2_directory_rejects_unregistered_and_foreign`

### FR-H3 중계 — `Draft`
iroh-relay는 Workers에서 온전히 돌 수 없습니다. 릴레이 자체는 HTTPS 위 WebSocket이라 TCP로 되지만, iroh 1.x가 공인 주소를 알아내는 QUIC 주소 발견(QAD)은 UDP 7842가 필요하고, Workers와 Containers는 들어오는 UDP를 받지 않습니다(들어오는 TCP는 2026년 8월부터 Spectrum으로 가능). iroh는 STUN을 쓰지 않으므로 STUN 서버는 두지 않습니다.
- 비교:
  - (A) **작은 VPS 한 대에 `hub/server`(iroh-relay + QAD)**: 릴레이와 QAD가 모두 됩니다. 한 달 수 달러 수준의 VPS 한 대를 따로 운영해야 하고(OS 갱신, 인증서, 감시), 그 한 대가 단일 장애점입니다.
  - (B) **Cloudflare Container에 iroh-relay(HTTPS/WebSocket만, QAD 없음)**: 운영할 서버가 없고 Worker와 같은 계정·배포로 묶입니다. 대신 QAD가 없어 기기가 자기 공인 주소를 모르므로 직접 연결 비율이 떨어지고 릴레이를 거치는 연결이 늘어납니다. 요청은 Worker → Durable Object → 컨테이너로 한 번 더 거치고, 모든 기기가 같은 릴레이 인스턴스를 만나야 하므로 인스턴스 하나에 몰립니다. 상시 켜진 인스턴스의 실행 시간과 전송량이 과금됩니다.
  - (C) **둘 다(Container 릴레이 + QAD 전용 VPS)**: iroh에서 QAD는 릴레이 목록(`RelayMap`)의 항목마다 붙고(`RelayConfig::quic`), 그 호스트는 릴레이 URL의 호스트입니다. 그래서 "QAD만 하는 VPS"도 릴레이 항목으로 올라가야 하고, 기기가 그것을 홈 릴레이로 고를 수 있으니 결국 릴레이도 돌려야 합니다. VPS를 없애지 못하면서 구성만 둘이 되므로 이득이 없습니다.
- **권장: (A)로 시작하고, ember NFR-N1 측정으로 (B)로 옮길지 정합니다.** ember NFR-N1의 기준은 대칭 NAT를 뺀 조합에서 직접 경로 성공률 85% 이상입니다. iroh가 말하는 약 90% 직접 연결은 QAD를 전제로 한 수치라, QAD 없이 이 기준을 맞춘다는 근거가 아직 없습니다. 측정은 (A) 위에서 두 번 합니다. 클라이언트 `RelayMap`에 QAD를 켠 경우(`quic: Some(7842)`)와 끈 경우(`quic: None`, (B)와 같은 조건)입니다. QAD를 끈 경우도 85%를 넘으면 릴레이를 Container로 옮기고 VPS를 없앱니다(`hub/worker/wrangler.toml`에 주석으로 둔 컨테이너 바인딩). 못 넘으면 (A)를 유지합니다. 사용자 확인 전이라 `Draft`입니다.
- 입장 정책: 릴레이 핸드셰이크가 증명한 엔드포인트 ID와 클라이언트가 낸 인증 토큰(있으면)을 릴레이 호스트가 `POST https://api.darkpyonix.dev/internal/v1/relay/admit`(D15 개정 전에는 apex)로 묻습니다(공유 비밀 `RELAY_SHARED_SECRET`). 폐기되지 않은 등록 기기면 허용(`cache_secs` 60초 동안 새 연결에 재사용 가능), 유효한 손님 통행권(FR-H4가 발급, 그 공유가 아직 게시 중)이 있으면 허용(캐시 안 함), 그 밖에는 거절입니다. 릴레이 호스트는 엔드포인트의 첫 연결이 열리고 마지막 연결이 닫힐 때 `POST /internal/v1/relay/presence`로 알려 기기 목록의 `online`을 갱신합니다. 기기를 지우면 Worker가 `POST https://relay.darkpyonix.dev/admin/v1/disconnect`로 끊습니다.
- 릴레이 호스트 상태: `hub/server`는 지금 D15 이전 구현(API·SQLite 포함)이고, 릴레이 전용으로 줄이는 작업(위 입장 API 사용, `/admin/v1/disconnect` 추가, API·DB 제거)은 빌드가 필요한 별도 변경입니다(`hub/server/src/lib.rs` 머리 주석).
- 수용 기준: 두 등록 기기가 IP 전송을 끈 릴레이 전용 모드로 우리 릴레이를 거쳐 연결하고 데이터를 주고받습니다(선택된 경로가 릴레이). 같은 두 기기가 루프백에서 직접 경로로도 연결합니다. 등록되지 않은 엔드포인트는 릴레이가 거절해 연결하지 못하고, 지운 기기의 연결은 끊깁니다. Worker 쪽: 등록 기기는 허용, 지운 기기와 통행권 없는 엔드포인트는 거절, 비밀이 틀리면 401, presence가 `online`을 바꿉니다. 루프백 처리량과 왕복 지연, NFR-N1의 QAD 켬/끔 직접 연결 비율을 측정해 여기에 적습니다.
- 테스트: Worker `test_fr_h3_relay_admits_registered_and_refuses_removed_devices`, `test_fr_h3_relay_callbacks_need_the_shared_secret`, `test_fr_h3_presence_marks_devices_online`(`hub/worker/test/shares.test.ts`). 릴레이 호스트 `test_fr_h3_relay_only_connection_through_hub`, `test_fr_h3_direct_connection_on_loopback`, `test_fr_h3_relay_rejects_unregistered_endpoint`, `test_fr_h3_relay_throughput_and_latency`(지금은 D15 이전 구현 기준, 릴레이 축소 때 스텁 입장 API로 바꿈)
- 측정 기록: (구현 후 기입)

### FR-H4 ash 호스팅과 공유 링크 — `Agreed`
**개정(제안, 2026-10-03, D15 개정).** ash는 더는 이 Worker의 정적 자산(`/ash/`)이 아니라 루트 메인 페이지(FR-H12, darkpyonix-ash가 빌드해 조직 GitHub Pages로 배포)입니다. 공유 링크의 모양 `https://darkpyonix.dev/s/<share_id>#<token>`은 그대로이고, 그 페이지는 메인 페이지의 `404.html` 앱이 냅니다(HTTP 상태 404, 토큰은 그대로 남음, FR-H12). API(`POST /v1/shares`, `GET`/`DELETE /v1/shares/{share_id}`)는 `api.darkpyonix.dev`로 옮기고, Worker의 HTML 연산 `GET /s/{share_id}`와 `GET /ash/`는 계약에서 뺍니다. 프런트는 공유 페이지에서 `GET https://api.darkpyonix.dev/v1/shares/{share_id}`(인증 없음, CORS)를 부르고, 없는 공유는 화면에서 "없는 공유"로 보여 줍니다. 아래 본문의 `/ash/`·`/s/` HTML 부분과 `test_fr_h4_viewer_pages_are_served`는 이 개정이 승인되면 darkpyonix-ash의 시험(FR-H12)으로 옮깁니다.

(개정 전 본문) `https://darkpyonix.dev/ash/`에서 공식 ash 뷰어를 Workers 정적 자산으로 호스팅하고(`hub/worker/public/ash/`에 darkpyonix-ash 빌드 결과를 넣어 배포), 공유 링크 `https://darkpyonix.dev/s/<share_id>#<token>`을 그 공유를 연 기기로 이어 줍니다. 공유 토큰은 URL 조각(`#` 뒤)에 있어서 허브로 가지 않습니다. 권한 검사는 끝단의 전용 매니저가 합니다(FR-A3).
- 기기는 `POST /v1/shares`로 자기 공유를 게시하고, 누구나 `GET /v1/shares/{share_id}`로 그 공유를 연 기기의 엔드포인트 ID와 릴레이 URL(기기가 올린 홈 릴레이, 없으면 `https://relay.darkpyonix.dev/`), 10분짜리 손님 릴레이 통행권을 받습니다. ash(브라우저 iroh, 릴레이 전용)는 그 통행권으로 릴레이에 붙어 기기에 연결합니다. `GET /s/{share_id}`는 ash 뷰어 페이지를 냅니다(뷰어가 배포되기 전까지는 자리표시 페이지). 공유를 내리면 그 공유의 통행권도 더는 통하지 않습니다.
- 수용 기준: 게시한 공유가 기기 ID와 통행권으로 풀리고, 기기가 주소를 올린 뒤에는 그 홈 릴레이 URL로 풀립니다. 다른 기기가 같은 공유 ID를 게시하면 409입니다. 그 통행권으로 미등록 엔드포인트의 릴레이 입장이 허용되고, 통행권이 없거나 위조이거나 공유를 내린 뒤면 거절됩니다. 게시를 지우면 404입니다. `/s/{share_id}`와 `/ash/`가 HTML을 냅니다. 브라우저 ash가 실제로 릴레이를 거쳐 기기에 붙는 것은 릴레이 호스트 연동 시험으로 확인합니다.
- 테스트(`hub/worker/test/shares.test.ts`): `test_fr_h4_share_resolves_to_hosting_device`, `test_fr_h4_share_ids_belong_to_one_device`, `test_fr_h4_guest_pass_admits_an_unregistered_endpoint_at_the_relay`, `test_fr_h4_viewer_pages_are_served`

### FR-H5 HTTPS 이름 — `Agreed`
메인 서버가 `https://<name>.darkpyonix.dev` 주소와 공인 인증서를 얻게 합니다(모바일 웹뷰의 보안 컨텍스트 요건, ember FR-N4).
- 방식 비교:
  - (A) **ACME DNS-01을 허브가 대신 게시.** 메인 서버가 자기 개인 키로 인증서를 받고, 허브는 `_acme-challenge.<name>.darkpyonix.dev` TXT만 게시합니다. TLS가 메인 서버에서 끝나므로 허브는 평문을 보지 않습니다(NFR-H1 유지). 대신 공인 IP가 없는 기기에 브라우저가 직접 닿지 못하므로, ember 앱이 루프백 포워더(127.0.0.1 → iroh)로 그 이름을 열어야 합니다.
  - (B) **허브가 TLS를 끝내는 HTTPS 엣지.** 아무 브라우저나 닿지만 허브가 평문을 봅니다. NFR-H1을 깨므로 쓰지 않습니다.
  - (C) **SNI 패스스루 엣지.** 허브가 ClientHello의 SNI만 읽고 TLS 바이트를 그대로 iroh로 기기에 넘깁니다. 앱 없는 브라우저에서도 닿지만 공개 트래픽 대역폭이 허브에 걸리고, Workers로는 할 수 없어(TCP 패스스루) 릴레이 호스트나 Spectrum이 필요합니다.
- 결정: (A)를 씁니다. (C)는 앱 없는 브라우저 접근이 필요해지면 따로 다룹니다. (B)는 쓰지 않습니다. 허브는 이름을 메인 서버 기기에 예약하고(`PUT /v1/names/{name}`), 그 기기가 요청한 TXT 값을 **Cloudflare DNS API**로 게시합니다(`PUT /v1/names/{name}/acme-challenge`). 기존 값 삭제와 새 값 생성은 `POST /zones/{zone_id}/dns_records/batch` 한 번이라 원자적이고, TTL은 60초입니다. API 토큰은 darkpyonix.dev 존 하나의 `Zone → DNS → Edit`만 가집니다. DNS 공급자는 `DnsProvider` 인터페이스 뒤에 있습니다. 이름을 놓거나 기기를 지우면 그 TXT도 지웁니다. 메인 서버를 바꾸면(FR-H1) 옛 메인 서버의 이름은 지우지 않고 새 메인 서버로 옮겨 가며, TXT만 지웁니다(사용자의 주소가 기계를 바꿔도 그대로 통하도록). 예약어(`www`, `api`, `relay`, `ash`, `hub`, `dns`, `ns1`, `ns2`, `mail`, `admin`, `docs`, `status`, `auth`, `link`, `qad`, 그리고 D15 개정으로 더하는 `nb`, `app`, `static`, `cdn`, `sandbox`, `usercontent`, `guide`, `blog`)는 받지 않습니다.
- 수용 기준: 메인 서버가 이름을 예약하면 201, 같은 기기가 다시 하면 200, 다른 기기는 409, `computer`는 403, 형식이 틀리거나 예약어면 400입니다. TXT 값 1~4개(각 43자 base64url)를 게시하고 지울 수 있고, 다른 값은 400, 공급자가 거절하면 502입니다. 메인 서버를 바꾸면 이름 목록의 그 이름이 새 메인 서버를 가리키고, 새 메인 서버는 그 이름의 TXT를 게시하며 옛 메인 서버는 401입니다. Cloudflare 클라이언트는 기존 레코드를 조회한 뒤 삭제와 생성을 한 batch로 보냅니다. 실제 존에서 Let's Encrypt 스테이징 인증서를 받는 것은 배포 후 확인합니다.
- 테스트(`hub/worker/test/names.test.ts`): `test_fr_h5_name_reservation_and_acme_txt`, `test_fr_h5_only_main_servers_hold_names_and_names_are_unique`, `test_fr_h5_bad_values_and_provider_failures`, `test_fr_h5_release_and_device_removal_clear_records`, `test_fr_h5_cloudflare_replaces_txt_in_one_batch`, `test_fr_h5_cloudflare_clear_and_errors`

### FR-H6 GitHub 로그인 — `Agreed`
허브 계정은 GitHub 로그인으로 만듭니다(사용자 결정, 2026-10-03: "OpenAI 로그인은 엠버 서버에서 사용자가 자체적으로 하는걸로 하고 허브는 깃허브 로그인으로 하자."). 계정의 정체는 GitHub 사용자의 숫자 ID(바뀌지 않고 재사용되지 않음)이고, 로그인 이름은 표시용으로만 저장합니다.
- 흐름: GitHub OAuth App, 인가 코드 + PKCE(S256) + state. `GET /auth/login`이 무작위 `state`와 PKCE 검증자를 D1에 10분짜리 일회용 거래로 남기고, `state`를 `__Host-dp_oauth` 쿠키에도 묶은 뒤 `https://github.com/login/oauth/authorize`로 보냅니다. 범위(scope)는 요청하지 않습니다(공개 프로필만 읽음). `GET /auth/callback`은 쿠키의 `state`와 같고 아직 쓰지 않은 거래인지 확인하고, 코드를 검증자와 함께 `https://github.com/login/oauth/access_token`에서 바꾸고, `GET https://api.github.com/user`로 `id`와 `login`을 읽은 뒤 그 GitHub 토큰을 폐기합니다. 그 GitHub ID의 계정을 찾거나 만들고, 30일짜리 세션 쿠키(`__Host-dp_session`, HttpOnly, Secure, SameSite=Lax)를 줍니다. `return_to`는 같은 출처의 경로만 받습니다.
- **개정(제안, 2026-10-03, D15 개정·FR-H13).** 로그인과 콜백은 `https://api.darkpyonix.dev/auth/login`, `/auth/callback`이고, GitHub OAuth App의 콜백 URL도 그 주소로 바꿉니다. 두 쿠키(`__Host-dp_oauth`, `__Host-dp_session`)는 API 호스트 전용입니다. `return_to`는 **프런트(`FRONT_URL`)의 경로**만 받고, 콜백은 `https://darkpyonix.dev<return_to>`로 보냅니다(그 밖은 `https://darkpyonix.dev/`). 로그아웃(`POST /auth/logout`)은 프런트가 CORS로 부릅니다.
- 운영자 선택 사항: `GITHUB_ALLOWED_IDS`(쉼표로 구분한 GitHub 사용자 ID)를 두면 그 사람들만 새 계정을 만들 수 있습니다(비우면 누구나).
- OpenAI / Sign in with ChatGPT는 허브에 넣지 않습니다. 사용자의 ChatGPT 플랜 사용은 사용자가 직접 띄운 ember server가 맡습니다(PROJECT Q2).
- 수용 기준: 로그인 시작이 `client_id`, 콜백 URL, `state`, S256 `code_challenge`를 담아 GitHub로 보내고 같은 `state`를 쿠키로 둡니다. 같은 GitHub ID로 두 번 로그인하면 같은 계정이고 로그인 이름만 갱신되며, 다른 ID는 다른 계정입니다. GitHub 토큰은 폐기되고 저장되지 않습니다. 다른 브라우저의 `state`, 다시 쓴 `state`, 틀린 PKCE 검증자는 400입니다. 허용 목록 밖의 새 사용자는 403입니다. 밖으로 나가는 `return_to`는 `/`가 됩니다. 로그아웃 뒤 세션은 401입니다. 다른 출처의 쿠키 쓰기는 403입니다. 실제 GitHub OAuth App으로 로그인되는 것은 배포 후 확인합니다.
- 테스트(`hub/worker/test/github.test.ts`, 가짜 GitHub): `test_fr_h6_login_redirects_to_github_with_pkce_and_state`, `test_fr_h6_callback_creates_one_account_per_github_user`, `test_fr_h6_github_token_is_revoked_and_not_stored`, `test_fr_h6_callback_rejects_state_from_another_browser`, `test_fr_h6_state_is_single_use`, `test_fr_h6_wrong_pkce_verifier_is_refused_by_the_provider`, `test_fr_h6_allowlist_limits_new_accounts`, `test_fr_h6_return_to_stays_on_this_origin`, `test_fr_h6_logout_ends_the_session`, `test_fr_h6_session_writes_need_our_origin`

### FR-H7 Flathub 앱 검증 — `Agreed`
Flathub의 앱 ID `dev.darkpyonix.Ember`는 도메인 darkpyonix.dev로 검증합니다. Flathub가 주는 토큰을 `https://darkpyonix.dev/.well-known/org.flathub.VerifiedApps.txt`에 평문으로 둡니다. 내용은 Worker 변수 또는 비밀값 `FLATHUB_VERIFICATION_TOKEN`에서 오므로 저장소에 토큰을 넣지 않습니다. 비어 있거나 없으면 404입니다.
- **개정(제안, 2026-10-03, D15 개정).** Flathub는 도메인 `darkpyonix.dev` 자체를 보므로 이 파일은 apex에 있어야 하고, apex는 조직 GitHub Pages 사이트(메인 페이지)입니다. 그래서 이 파일은 darkpyonix-ash의 빌드 결과에 들어가 Pages가 정적 파일로 냅니다(FR-H12). 점으로 시작하는 경로이므로 빌드 결과에 `.nojekyll`이 있어야 합니다. 내용은 ash CI가 저장소 변수 `FLATHUB_VERIFICATION_TOKEN`에서 넣습니다. 이 값은 공개되는 파일의 내용이라 비밀은 아니지만, 소스에 박지 않고 변수로 둬서 다시 발급할 때 빌드만 다시 돌립니다. 변수가 비면 파일을 만들지 않습니다(404). API Worker의 같은 연산과 `flathub.test.ts`는 계약과 Worker에서 빼고, 수용 기준은 FR-H12의 배포 점검으로 옮깁니다.
- 수용 기준: 토큰이 있으면 200 `text/plain`이고 본문은 앞뒤 공백을 뺀 토큰입니다. 없거나 공백뿐이면 404입니다. 실제 Flathub 검증은 배포 후 확인합니다.
- 테스트(`hub/worker/test/flathub.test.ts`): `test_fr_h7_verified_apps_file_serves_the_configured_token`, `test_fr_h7_verified_apps_file_is_absent_without_a_token`

### FR-H8 허브 설정 발견 — `Agreed`
클라이언트(ember, ash)가 릴레이 주소나 pkarr URL을 코드에 박아 두지 않도록, 허브가 자기 설정을 공개합니다(Ember FR-N2 연동 중 보고, 2026-10-03).
- `GET /v1/config`(인증 없음, `Cache-Control: public, max-age=300`)는 `{api_version, hub_version, relay_urls, pkarr_url, link_url}`를 냅니다. `api_version`은 정수이고 `/v1` 아래 계약을 깨는 변경이 있을 때만 올립니다(지금 1). `relay_urls`는 기기가 iroh `RelayMap`에 넣을 릴레이 목록(지금은 Worker 변수 `RELAY_URL` 하나), `pkarr_url`은 iroh `PkarrPublisher`/`PkarrResolver`에 줄 기준 URL(`<PUBLIC_URL>/pkarr`, 조회할 때는 `?token=<조회 토큰>`을 붙임, NFR-H2), `link_url`은 기기 링크 승인 페이지입니다.
- **개정(제안, 2026-10-03, D15 개정).** `GET https://api.darkpyonix.dev/v1/config`에 `api_url`(`https://api.darkpyonix.dev`)과 `front_url`(`https://darkpyonix.dev`)을 더합니다. `pkarr_url`은 `<API_URL>/pkarr`, `link_url`은 `<FRONT_URL>/link`입니다. Worker 변수 `PUBLIC_URL`은 `API_URL`과 `FRONT_URL` 둘로 나눕니다. 필드 추가라 `api_version`은 1 그대로입니다. 메인 페이지 빌드에는 API 주소(`https://api.darkpyonix.dev`) 하나만 넣고, 릴레이·pkarr 주소는 실행 중에 이 응답에서 읽습니다. 그래서 릴레이를 옮겨도 메인 페이지를 다시 배포하지 않습니다.
- 수용 기준: 인증 없이 200이고, 값이 Worker 변수(`PUBLIC_URL`, `RELAY_URL`, 개정 뒤에는 `API_URL`, `FRONT_URL`, `RELAY_URL`)를 따릅니다.
- 테스트(`hub/worker/test/config.test.ts`): `test_fr_h8_config_names_relays_and_pkarr_url`, `test_fr_h8_config_follows_the_worker_vars`

### FR-H9 기기 목록 변경 알림 — `Agreed` [provisional]
Ember는 기기 목록을 60초마다 다시 읽었고, 그래서 "지운 기기는 하트비트 한 번 안에 끊긴다"(ember FR-N3)를 맞출 수 없었습니다(Ember FR-N2 연동 중 보고, 2026-10-03). 허브가 목록이 바뀐 것을 알려 줍니다.
- **판(version)과 ETag.** 계정마다 기기 목록의 판 번호를 두고, 목록에 보이는 것이 바뀔 때마다 하나 올립니다: 기기 추가·되살림(FR-H11), 삭제, 이름·앱 정보 변경, `online` 변화. `last_seen`만 바뀌는 것(주소 게시, 릴레이 입장)은 판을 올리지 않습니다(5분마다 오는 게시가 모든 대기자를 깨우지 않도록). `GET /v1/devices`는 `ETag: W/"v<판>"`을 붙입니다(약한 ETag: `last_seen`은 판에 들지 않음).
- **조건부 요청.** `If-None-Match`가 지금 ETag와 같으면 304(본문 없음)입니다.
- **롱 폴링.** `?wait=<초>`(0~25)를 함께 주면, ETag가 같을 때 바로 304를 내지 않고 판이 바뀌거나 `wait`초가 지날 때까지 기다립니다. 바뀌면 200과 새 목록·새 ETag, 시간이 다 되면 304입니다. Worker는 기다리는 동안 2초마다 그 계정의 판 한 행만 읽습니다. 그래서 변경은 최대 약 2초 뒤에 전해지고, 대기 중인 클라이언트 하나는 25초에 D1 읽기 14번 정도(하루 약 4만8천 번)를 씁니다. 판이 바뀐 뒤에는 자격 증명을 다시 확인하므로, 기다리던 기기 자신이 지워졌으면 401 `device_removed`가 옵니다. 25초 상한은 프록시·모바일 망이 유휴 연결을 끊는 시간보다 짧게 둔 값입니다.
- **SSE가 아니라 롱 폴링인 이유.** Durable Object 없이 Worker는 다른 요청이 한 쓰기를 밀어 받을 수 없으므로, SSE로 해도 연결 안에서 똑같이 D1을 주기적으로 읽어야 합니다. 그러면 SSE는 연결을 더 오래 잡고(Worker 동시 연결, 모바일 배터리), 중간 프록시의 버퍼링 문제가 생기며, 다시 붙을 때의 상태 맞추기를 따로 정해야 합니다. 롱 폴링+ETag는 보통 HTTP 클라이언트로 되고, 끊겨도 마지막 ETag로 이어서 묻기만 하면 되며, `wait` 없이 쓰면 값싼 조건부 폴링이 됩니다. 계약은 그대로 두고 나중에 Durable Object로 대기자를 즉시 깨우게 바꿀 수 있습니다. 실제 부하와 Ember 사용으로 확정할 때까지 `[provisional]`입니다.
- 수용 기준: 응답에 ETag가 있고, 같은 ETag의 `If-None-Match`는 304, 다른 ETag는 바로 200입니다. 기다리는 중에 기기를 지우거나 이름을 바꾸면 200과 새 ETag가 오고, 아무 일 없으면 `wait` 뒤 304입니다. 기다리던 기기가 지워지면 401 `device_removed`입니다. 주소 게시는 ETag를 바꾸지 않고, `online` 변화는 바꿉니다. `wait`가 범위 밖이면 400입니다.
- 테스트(`hub/worker/test/notify.test.ts`): `test_fr_h9_device_list_has_an_etag_and_answers_304`, `test_fr_h9_long_poll_wakes_on_a_change`, `test_fr_h9_long_poll_times_out_with_304`, `test_fr_h9_waiting_device_that_is_removed_gets_device_removed`, `test_fr_h9_only_visible_changes_move_the_etag`, `test_fr_h9_wait_is_validated`

### FR-H10 기기 앱 정보 — `Agreed` [provisional]
기기 목록만으로 "어느 기기가 ember 노드이고 무슨 버전이며 무엇을 제공하는지" 알 수 있게, 기기가 자기 앱 정보를 허브에 적습니다(Ember FR-N2 연동 중 보고, 2026-10-03).
- 기기는 `PATCH /v1/devices/{자기 endpoint_id} {"app": {...}}`로 적고 `{"app": null}`로 지웁니다. **그 기기 자신만** 적을 수 있습니다(세션이나 메인 서버가 남의 `app`을 적으면 403). 기기 목록과 조회의 `Device.app`에 그대로 나옵니다(없으면 `null`).
- 형식(엄격, 알 수 없는 필드는 400): `kind`(필수, `^[a-z][a-z0-9-]{0,31}$`, 예: `ember`), `version`(필수, `^[0-9A-Za-z][0-9A-Za-z.+-]{0,31}$`), `services`(선택, 기본 `[]`, 같은 형식의 이름 최대 16개, 중복 없음, 예: `["kernel-manager", "ash-host"]`).
- 허브는 이 값을 표시와 힌트로만 씁니다. 기기가 스스로 말한 것이라 권한 판단에 쓰지 않고, 클라이언트도 연결 상대를 고르는 힌트로만 씁니다(상대 인증은 iroh 키가 함). 형식을 좁게 둔 이유: 계정의 모든 기기에 그대로 보이는 값이므로 크기와 문자 집합을 묶어 두고, 자유 형식 필드가 필요해지면 그때 넓힙니다. Ember의 실제 사용으로 확정할 때까지 `[provisional]`입니다.
- 수용 기준: 기기가 적은 앱 정보가 같은 계정의 목록과 조회에 나오고, `null`로 지워집니다. 남이 적으면 403, 형식이 틀리거나 알 수 없는 필드면 400입니다.
- 테스트(`hub/worker/test/devices.test.ts`): `test_fr_h10_device_reports_its_app`, `test_fr_h10_only_the_device_writes_its_app`, `test_fr_h10_app_is_validated`

### FR-H11 지운 키 다시 들이기 — `Agreed` [provisional]
지운 키는 다시 등록할 수 없으므로(FR-H1), 실수로 지운 메인 서버는 새 키(새 엔드포인트 ID)를 만들어야 하고 그 키를 아는 모든 곳을 고쳐야 합니다. 그래서 **계정 주인만** 지운 키를 다시 들일 수 있게 합니다(Ember FR-N2 연동 중 보고, 2026-10-03).
- 흐름: (1) 로그인한 브라우저 세션이 `POST /v1/devices/{endpoint_id}/readmit`로 자기 계정에서 지운 키를 15분 동안 다시 받아들이겠다고 표시합니다. (2) 그 기기가 보통의 기기 링크(FR-H1)를 시작합니다. 표시가 살아 있는 동안만 그 키의 링크 요청이 409가 아닙니다. (3) 그 링크의 승인도 **같은 계정의 세션만** 할 수 있습니다(메인 서버 토큰, 다른 계정은 403). (4) 기기가 받으면 같은 기기 행이 되살아나고 기기 토큰과 조회 토큰은 새로 발급됩니다. 역할과 이름은 새 링크의 것이고 앱 정보는 비웁니다. `main_server`로 돌아오는 링크에는 메인 서버 하나 규칙(FR-H1)이 그대로 걸립니다. 계정에 다른 메인 서버가 있으면 `replace`로만 승인되고, 받으면 그 메인 서버가 지워지며 이름이 되살아난 기기로 옮겨 갑니다.
- 지운 기기 목록: 다시 들일 키를 고르려면 그 엔드포인트 ID를 알아야 하므로, 로그인한 세션은 `GET /v1/removed-devices`로 자기 계정에서 지운 기기를 봅니다. 항목마다 `endpoint_id`, 지울 때의 `name`과 `role`, `created_at`, `removed_at`, 그리고 다시 들이기 표시가 살아 있으면 그 만료(`readmit_until`, 아니면 `null`)가 나옵니다. 최근에 지운 것부터 100개까지입니다. 다시 들여 받은 기기는 이 목록에서 빠지고 기기 목록(`GET /v1/devices`)에 나옵니다. 기기 토큰(메인 서버 것도)은 403입니다. 다시 들이기 자체가 세션만의 일이므로 목록도 세션에만 보입니다. 지운 기기의 이름과 역할은 계정 바깥에 드러나지 않습니다.
- 지울 때 이미 없어진 것(이름, 공유, 주소 레코드, 옛 토큰)은 돌아오지 않습니다. 옛 기기 토큰은 그 뒤 `invalid_credentials`입니다.
- 설계 이유: 키가 새서 지운 경우를 생각하면 키 소유 증명만으로 돌아오게 할 수 없습니다. 그래서 두 번의 사람 확인(표시와 승인)을 세션에만 맡기고, 표시는 짧게(링크 수명과 같은 15분) 둡니다. 메인 서버 토큰을 빼는 이유는 FR-H1의 `main_server` 승인 제한과 같습니다(새어 나간 메인 서버 토큰으로 지운 기기를 되살리지 못하게). Ember의 실제 사용으로 확정할 때까지 `[provisional]`입니다.
- 수용 기준: 지운 키의 링크 요청은 409, 세션이 다시 들이기를 표시한 뒤에는 201입니다. 메인 서버 토큰의 표시와 승인은 403입니다. 받은 뒤 기기가 목록에 다시 나오고 새 토큰이 통하며 옛 토큰은 401 `invalid_credentials`입니다. 15분이 지나면 다시 409입니다. 지우지 않은 기기의 표시는 409, 다른 계정의 기기는 404입니다. 다른 메인 서버가 있을 때 `main_server`로 돌아오는 링크의 승인은 `replace` 없이 409 `main_server_exists`이고, `replace`로 승인해 받으면 되살아난 기기가 유일한 메인 서버가 되고 바뀐 메인 서버의 토큰은 401 `device_removed`이며 이름이 옮겨 갑니다. 지운 기기 목록은 세션에 지운 기기를 최근 것부터 지울 때의 이름·역할·`removed_at`과 함께 보여 주고, 다시 들이기를 표시하면 `readmit_until`이 나오며, 되살아난 기기와 다른 계정의 기기는 나오지 않습니다. 기기 토큰은 403입니다.
- 테스트(`hub/worker/test/devices.test.ts`): `test_fr_h11_owner_readmits_a_removed_key`, `test_fr_h11_readmission_expires`, `test_fr_h11_readmit_needs_a_removed_device_of_the_account`, `test_fr_h11_owner_lists_removed_devices`, `test_fr_h11_only_a_session_lists_removed_devices`

### FR-H12 호스트 역할 분담과 루트 메인 페이지 — `Draft`
INTENT D15 개정의 사용자 결정(2026-10-03): "동적 기능은 서브 도메인으로 해놓고, 루트 darkpyonix.dev가 그걸 띄우도록", "darkpyonix.dev/는 메인 페이지고 darkpyonix.dev/dioxus-compose는 가이드인거잖아", 프런트 저장소는 "darkpyonix-ash". 호스트는 §10 머리의 표대로 나눕니다.
- **루트(`darkpyonix.dev/`)는 정적 메인 페이지입니다.** darkpyonix-ash 웹 빌드(랜딩 + ash 웹 앱)를 조직 GitHub Pages 사이트 `DarkPyonix/DarkPyonix.github.io`(`main`의 루트, custom domain `darkpyonix.dev`, HTTPS 강제)로 냅니다. 서버 코드, 쿠키, 비밀값이 없고, 동적인 일은 모두 `api.darkpyonix.dev`와 `relay.darkpyonix.dev`를 부릅니다. apex의 DNS(GitHub Pages A/AAAA, DNS only)와 `www`는 지금 그대로입니다.
- **빌드와 배포는 darkpyonix-ash가 합니다.** ash CI가 웹 빌드를 만들고, `DarkPyonix.github.io`에만 쓰기 권한이 있는 배포 키(ash 저장소 비밀값 `PAGES_DEPLOY_KEY`)로 빌드 결과 전체를 그 저장소 `main`에 새 커밋으로 올립니다(강제 푸시 없음, 이력이 곧 배포 기록). 조직 사이트 저장소에는 워크플로가 없습니다. 이 저장소(darkpyonix)는 메인 페이지가 지켜야 할 경로·파일 계약과 API만 가집니다. 빌드 결과의 최상위는 다음과 같습니다.
  - `index.html`(앱 HTML), `404.html`(`index.html`과 같은 내용), `link.html`·`new.html`(같은 내용. Pages는 `/link`를 `link.html`로 리디렉트 없이 내므로 `?code=`가 그대로 남음), `assets/`(해시 붙은 JS·CSS, 모든 참조는 `/assets/…` 절대 경로), `CNAME`(`darkpyonix.dev`), `.nojekyll`(Jekyll 처리를 끄고 점으로 시작하는 경로를 내게 함), `sandbox/index.html`(NFR-H3의 렌더러 페이지, 자기 meta CSP를 가짐), `.well-known/org.flathub.VerifiedApps.txt`(FR-H7), `robots.txt`, 그리고 교차 출처 격리를 쓸 때 서비스 워커 파일(아래).
  - Pyodide와 그 패키지는 빌드에 넣지 않고 CDN(jsDelivr)에서 받습니다. GitHub Pages의 사이트 크기(1 GB)와 대역폭(월 100 GB, 권고) 한도 안에 머물기 위해서입니다.
- **경로와 상태 코드.** `/`(랜딩, 로그인했으면 내 노트북), `/link`(기기 승인, FR-H1), `/new`(노트북 올리기)는 실제 파일이라 200입니다. `/n/<notebook_id>`, `/n/<notebook_id>/v/<version>`(FR-H16, FR-H17), `/s/<share_id>`(FR-H4, 토큰은 `#` 뒤)는 파일이 없으므로 Pages가 `404.html`을 냅니다. 내용은 같은 앱이라 그대로 뜨고, 앱은 `location.pathname`으로 경로를 고릅니다(다른 주소로 리디렉트하지 않으므로 `#` 뒤 토큰이 그대로 남음). HTTP 상태는 404이므로 검색 엔진과 링크 미리보기는 이 페이지를 쓰지 못합니다(FR-H15). 없는 노트북이나 공유는 앱이 API 응답을 보고 화면에 표시합니다.
- **가이드와 경로 나눠 쓰기.** 프로젝트 가이드는 각 저장소의 GitHub Pages 프로젝트 사이트로 `darkpyonix.dev/<저장소>/`에 그대로 있습니다(예: `/dioxus-compose/`). Pages는 프로젝트 사이트가 있는 저장소 이름의 경로를 그 사이트로 먼저 보내므로 두 규칙을 지킵니다. (1) 메인 페이지는 Pages가 켜진 저장소의 이름을 최상위 경로로 쓰지 않습니다. (2) 조직은 메인 페이지의 최상위 이름(`n`, `s`, `link`, `new`, `assets`, `sandbox`, `.well-known`)과 같은 이름의 저장소에 Pages를 켜지 않습니다. ash CI는 배포 전에 조직의 Pages 사이트 목록과 빌드 결과의 최상위 이름이 겹치지 않는지 확인합니다.
- **헤더를 정할 수 없는 것의 처리.** GitHub Pages는 응답 헤더를 바꿀 수 없습니다.
  - CSP는 앱 HTML의 `<meta http-equiv="Content-Security-Policy">`로 둡니다: `default-src 'self'`, `script-src 'self'`와 ash 런타임이 받는 CDN, `connect-src 'self' https://api.darkpyonix.dev https://relay.darkpyonix.dev wss://relay.darkpyonix.dev`와 그 CDN, `frame-src 'self'`(NFR-H3의 렌더러 페이지. 사용자 콘텐츠 도메인을 쓰기로 하면 그 도메인), `base-uri 'none'`, `form-action 'none'`. `frame-ancestors`는 meta로 쓸 수 없어서 빠집니다. `Referrer-Policy`는 `<meta name="referrer" content="no-referrer">`입니다.
  - **클릭재킹**(`/link` 승인을 남의 사이트에 끼워 누르게 하기)은 두 겹으로 막습니다. API 세션 쿠키가 `SameSite=Lax`라(FR-H13) 남의 사이트 안에 끼워진 메인 페이지가 보내는 API 요청에는 쿠키가 실리지 않고, 그래서 승인은 401입니다. 그리고 앱은 `window.top !== window.self`이면 화면을 그리지 않습니다. 앱 전체가 스크립트로 그려지므로 스크립트를 끈 iframe에서는 아무것도 보이지 않습니다.
  - **교차 출처 격리(COOP/COEP, `SharedArrayBuffer`).** 호스팅한 노트북 보기(FR-H16)와 라이브 커널로 열기(FR-H17)는 브라우저에서 Python을 돌리지 않으므로 필요 없습니다. ash의 브라우저 내 Python(Pyodide)은 격리가 있으면 Web Worker에서(화면이 멈추지 않고 인터럽트 가능), 없으면 메인 스레드에서 돕니다. 격리가 필요한 화면은 ash가 이미 가진 서비스 워커(응답에 COOP/COEP를 덧붙임, `starboard-sw.js`)로 켭니다 [provisional]. 이 방식은 첫 방문 때 한 번 다시 읽어야 하고, 그 화면이 받는 교차 출처 자원은 CORS나 `Cross-Origin-Resource-Policy`가 있어야 합니다(API는 CORS, jsDelivr는 CORS `*`). 격리는 최상위 문서의 성질이라 iframe만 다른 호스트에서 띄워서는 얻을 수 없습니다.
- **API 호스트는 HTML을 내지 않습니다.** `api.darkpyonix.dev`가 내는 HTML은 없고, `/auth/*`는 리디렉트만 합니다. 반대로 `https://darkpyonix.dev/v1/...`는 API가 아니라 메인 페이지의 404 응답입니다.
- 수용 기준(배포 후 점검 스크립트, ash CI의 배포 뒤 단계): `https://darkpyonix.dev/`, `/link?code=BCDF-GHJK`, `/new`가 200 `text/html`, `/n/n_<32 hex>`와 `/s/s_<16 hex>`가 404 `text/html`이고 본문이 `index.html`과 같으며, 모든 앱 HTML에 위 meta CSP와 referrer가 있습니다. `/dioxus-compose/`가 그 저장소의 가이드를 냅니다(메인 페이지가 가로채지 않음). `/.well-known/org.flathub.VerifiedApps.txt`는 200 `text/plain`입니다(FR-H7). 응답에 `Set-Cookie`가 없습니다. 메인 페이지를 다른 출처의 iframe에 넣으면 아무것도 그려지지 않고, 그 안에서 `/link` 승인을 보내도 401입니다. `https://api.darkpyonix.dev/v1/config`는 200 JSON입니다.
- 테스트(darkpyonix-ash): `test_fr_h12_front_routes_serve_the_app`, `test_fr_h12_guides_stay_on_project_sites`, `test_fr_h12_build_names_do_not_collide_with_pages_repos`, `test_fr_h12_front_sets_no_cookies`, `test_fr_h12_framed_front_renders_nothing_and_cannot_approve`

### FR-H13 브라우저에서 API 부르기(CORS, 쿠키, OAuth) — `Draft`
프런트와 API가 다른 호스트이므로 브라우저 경로를 정합니다.
- **CORS.** 허용 출처는 정확히 `FRONT_URL`(`https://darkpyonix.dev`) 하나이고, 운영자가 `CORS_DEV_ORIGINS`(쉼표 구분, 예: `http://localhost:5173`)를 두면 그것도 더합니다 [provisional]. 허용 출처의 요청에는 `Access-Control-Allow-Origin: <그 출처>`, `Access-Control-Allow-Credentials: true`, `Vary: Origin`, `Access-Control-Expose-Headers: ETag`를 붙입니다. 사전 요청(`OPTIONS`)은 204와 `Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE`, `Access-Control-Allow-Headers: Authorization, Content-Type, If-None-Match`, `Access-Control-Max-Age: 600`입니다. 그 밖의 출처(사용자 이름 서브도메인 `<name>.darkpyonix.dev`, 출처 `null` 포함)에는 CORS 헤더를 붙이지 않습니다. 기기 토큰을 쓰는 기기(ember, CLI)는 브라우저가 아니라 CORS와 무관합니다.
- **쿠키.** `__Host-dp_session`과 `__Host-dp_oauth`는 `api.darkpyonix.dev`가 `Domain` 없이(`__Host-` 접두어 규칙) `Path=/; Secure; HttpOnly; SameSite=Lax`로 둡니다. 프런트는 쿠키를 읽지 못하고, 로그인 여부는 `GET /v1/me`(401이면 로그아웃 상태)로 압니다. 프런트와 API는 같은 사이트(`darkpyonix.dev`)이므로 `SameSite=Lax` 쿠키가 `credentials: "include"` 요청에 실리고, 서드파티 쿠키 차단과 무관합니다.
- **쿠키 쓰기의 출처 검사.** 쿠키로 인증한 쓰기 요청은 `Origin`이 `FRONT_URL`(또는 `CORS_DEV_ORIGINS`)이 아니면 403입니다(§10 인증 모델). 같은 사이트의 사용자 이름 서브도메인은 남의 서버이므로 여기서 막힙니다.
- **OAuth.** FR-H6 개정대로 로그인은 메인 페이지가 `https://api.darkpyonix.dev/auth/login?return_to=<경로>`로 최상위 이동하는 것이고(iframe·팝업 아님), 콜백은 `https://api.darkpyonix.dev/auth/callback`이며, 끝나면 `https://darkpyonix.dev<return_to>`로 302합니다. `return_to`는 `/`로 시작하고 `//`나 `\`로 시작하지 않는 경로와 쿼리만 받습니다. **URL 조각(`#` 뒤)은 `return_to`에 넣지 않습니다.** 공유 토큰(FR-H4, FR-H17)이 API로 가면 안 되므로, 메인 페이지는 로그인으로 떠나기 전에 조각을 `sessionStorage`에 두고 돌아와서 되살립니다. 콜백 302는 GitHub OAuth와 API 쪽 리디렉트만 거치므로 상태가 메인 페이지 호스팅(Pages)과 무관합니다.
- 수용 기준: `Origin: https://darkpyonix.dev`의 사전 요청은 204와 위 헤더, 실제 요청은 그 출처를 그대로 돌려줍니다. `Origin: https://studio.darkpyonix.dev`나 `null`에는 `Access-Control-Allow-Origin`이 없고, 그 출처의 쿠키 쓰기는 403입니다. 로그인 콜백의 `Set-Cookie`에 `Domain`이 없고 이름이 `__Host-`로 시작합니다. 콜백은 `https://darkpyonix.dev/<return_to>`로 302하고, 바깥 `return_to`(`https://…`, `//evil`, `/\evil`)는 `https://darkpyonix.dev/`가 됩니다.
- 테스트(`hub/worker/test/cors.test.ts`): `test_fr_h13_cors_allows_only_the_front`, `test_fr_h13_preflight_lists_methods_and_headers`, `test_fr_h13_session_cookie_is_host_only_on_the_api`, `test_fr_h13_callback_returns_to_the_front`, `test_fr_h13_cookie_writes_from_user_subdomains_are_refused`

### FR-H14 노트북 보관과 판 — `Draft`
사용자 결정(2026-10-03): 허브는 "ash 노트북 호스팅까지" 합니다. 허브는 노트북을 보관하고 보여 줄 뿐 실행하지 않습니다(INTENT D15 개정).
- **노트북.** `notebook_id`는 `n_<32 hex>`(128비트 난수)입니다. `unlisted` 링크(FR-H15)는 ID를 아는 것이 곧 권한이므로 추측할 수 없어야 합니다. 노트북은 계정 하나의 것이고 `title`(1~120자), `visibility`(FR-H15), `share_id`(FR-H17, 없으면 `null`), `latest_version`, `created_at`, `updated_at`, 만든 기기(`created_by`: 세션이면 `null`)를 가집니다.
- **판(version).** 판은 바꿀 수 없는 스냅숏이고 노트북마다 1부터 하나씩 올라갑니다(지운 번호는 다시 쓰지 않음). 판 하나는
  - `source`(필수): 노트북 파일 하나(FORMAT의 `.py` 또는 `.pynb`) 그대로. 파일 이름은 `^[^/\\\x00]{1,128}\.(py|pynb)$`이고, 본문은 UTF-8이며 NUL이 없어야 합니다. 허브는 FORMAT §2.2의 셀 표식으로 셀 수만 세어 둡니다(검사용, 실행하지 않음).
  - `run`(선택): 그 파일의 실행 기록 하나(`__runs__/<파일 이름>/<run_id>.ipynb`). JSON 객체이고 `nbformat`이 4, `cells`가 배열, 출력의 `output_type`이 `stream`·`display_data`·`execute_result`·`error` 중 하나여야 합니다. `run_id`(파일 이름에서)를 함께 저장합니다. 실행 기록과 소스가 맞는지는 검사하지 않습니다. 보여 줄 때 FR-R4 맵핑 규칙이 소스가 다른 셀을 `stale`로 표시합니다. FR-R5의 사이드카 로그(`.cell<n>.log`)는 올리지 않습니다.
  - `message`(선택, 0~500자)와 각 부분의 `sha256`, 크기, `created_at`, 올린 주체.
- **올리기.** `POST /v1/notebooks`로 노트북을 만들고, `POST /v1/notebooks/{notebook_id}/versions`(`multipart/form-data`, 부분 `source`·`run`·`message`)로 판을 더합니다. 새 판 번호는 D1 한 문장(`UPDATE notebooks SET latest_version = latest_version + 1 WHERE … RETURNING`)으로 정하므로 동시에 올려도 번호가 겹치지 않습니다. 본문을 R2에 먼저 쓰고 D1에 행을 넣습니다. D1이 실패해 남은 R2 객체는 Cron이 지웁니다.
- **누가.** 만들기와 판 올리기: 로그인 세션, 또는 그 계정의 `main_server`·`computer` 기기 토큰(공유 게시와 같은 범위, `client`는 403). 제목·공개 범위·`share_id` 바꾸기와 지우기: 계정 권한(세션 또는 `main_server`) 또는 그 노트북을 만든 기기 [provisional]. 다른 계정에서는 비공개 노트북이 없는 것과 같습니다(404).
- **저장소.** 목록·권한·판 메타데이터는 D1(`notebooks`, `notebook_versions`), 본문은 비공개 R2 버킷(`darkpyonix-notebooks`, 키 `nb/<account_id>/<notebook_id>/<version>/source|run`)입니다. D1의 행·값 상한(약 2 MB)과 DB 크기 상한 때문에 본문을 D1에 두지 않습니다. 본문은 Worker를 거쳐서만 나갑니다(R2 공개 버킷 없음).
- **읽기.** `GET /v1/notebooks/{notebook_id}`(메타데이터와 판 목록), `GET /v1/notebooks/{notebook_id}/versions/{version}`(판 메타데이터), `…/source`, `…/run`(본문). `{version}`에는 `latest`를 쓸 수 있습니다. 본문 응답은 `source`가 `text/plain; charset=utf-8`, `run`이 `application/json`이고, `X-Content-Type-Options: nosniff`, `Content-Security-Policy: sandbox; default-src 'none'`, `Content-Disposition: attachment; filename=…`, `ETag: "<sha256>"`를 붙입니다. 그래서 주소창으로 열어도 문서로 실행되지 않습니다(NFR-H3). 캐시는 `private, max-age=60` [provisional]: 판은 바뀌지 않지만 공개 범위는 바뀔 수 있어서 공유 캐시에 오래 두지 않습니다.
- 수용 기준: 노트북을 만들면 201과 `n_<32 hex>`, 판을 둘 올리면 1과 2, 동시에 둘 올려도 서로 다른 연속 번호입니다. 받은 `source`·`run`이 올린 바이트와 같고(`sha256`), `latest`가 가장 높은 판입니다. UTF-8이 아니거나 NUL이 있는 소스, 확장자가 틀린 이름, `nbformat`이 4가 아니거나 출력 형식이 틀린 실행 기록은 400입니다. `client` 기기 토큰은 403, 다른 계정은 404입니다. 본문 응답에 위 헤더가 있습니다.
- 테스트(`hub/worker/test/notebooks.test.ts`): `test_fr_h14_create_and_push_versions`, `test_fr_h14_versions_are_numbered_once_under_concurrency`, `test_fr_h14_content_round_trips_with_sha256`, `test_fr_h14_rejects_malformed_source_and_run`, `test_fr_h14_client_devices_cannot_publish`, `test_fr_h14_content_responses_cannot_render_as_documents`

### FR-H15 공개 범위 — `Draft`
노트북의 `visibility`는 셋입니다. 기본은 가장 좁은 `private`입니다.

| 값 | 누가 읽나 | 목록에 나오나 | 검색 엔진 |
|---|---|---|---|
| `private` | 그 계정(세션, 그 계정의 기기 토큰) | 자기 목록(`GET /v1/notebooks`)에만 | 아님 |
| `unlisted` | 링크(ID)를 아는 누구나, 인증 없이 | 자기 목록에만 | `X-Robots-Tag: noindex` |
| `public` | 누구나, 인증 없이 | 자기 목록과 `GET /v1/users/{github_login}/notebooks` | 막지 않음. 단 메인 페이지의 `/n/<id>`는 HTTP 404 상태라(FR-H12) 실제로 색인되지 않음 [provisional] |

- 읽기 권한이 없으면 403이 아니라 404입니다(있는지도 드러내지 않음). `private`으로 바꾸면 그 순간부터 남의 API 읽기는 404입니다. 이미 받아 간 사본과 60초 캐시(FR-H14)는 회수할 수 없습니다.
- 공개 범위는 노트북 단위이고 모든 판에 같이 걸립니다. 판마다 다르게 두지 않습니다(판을 숨기려면 그 판을 지움, FR-H18).
- 수용 기준: `private` 노트북은 인증 없이 404, 같은 계정의 세션·기기 토큰으로 200입니다. `unlisted`는 인증 없이 200이고 `X-Robots-Tag: noindex`가 붙으며 사용자 공개 목록에 나오지 않습니다. `public`은 인증 없이 200이고 사용자 공개 목록에 나옵니다. `public`을 `private`으로 바꾸면 인증 없는 읽기가 바로 404입니다.
- 테스트(`hub/worker/test/notebooks.test.ts`): `test_fr_h15_private_notebooks_are_hidden`, `test_fr_h15_unlisted_notebooks_open_by_link_only`, `test_fr_h15_public_notebooks_are_listed`, `test_fr_h15_going_private_takes_effect_at_once`

### FR-H16 커널 없이 읽기 전용으로 보기 — `Draft`
프런트의 `/n/<notebook_id>`(최신 판)와 `/n/<notebook_id>/v/<version>`은 저장된 판을 **커널, 릴레이, 기기 없이** 보여 줍니다.
- 프런트는 API에서 판 메타데이터와 `source`·`run`을 받아, FORMAT의 규칙으로 셀을 나누고(FR-F1과 같은 결과), FR-R4의 맵핑 순서(`id` → `source_sha256` → `index`)로 실행 기록의 출력을 셀에 붙이며, 소스가 다른 셀은 `stale`로 표시합니다. 마크다운 셀(`darkpyonix.markdown`)은 마크다운으로, 출력은 nbformat MIME 우선순위대로 그립니다.
- `text/html`, `application/javascript`, `image/svg+xml`, 위젯 MIME처럼 스크립트가 돌 수 있는 출력과 마크다운 안의 HTML은 NFR-H3의 샌드박스에서만 그립니다. `text/plain`, `image/png`, `image/jpeg`, `error`, `stream`은 프런트가 텍스트·이미지로 그립니다.
- 판 고르기, 소스와 실행 기록 내려받기, "라이브로 열기"(FR-H17, 공유가 걸려 있을 때)를 둡니다.
- 수용 기준: 주인의 기기가 꺼져 있고 릴레이에 닿지 못해도 `public` 노트북의 모든 셀과 출력이 그려집니다. 실행 기록 뒤에 고친 셀은 `stale`로 표시되고 출력은 남습니다. 실행 기록이 없는 판은 코드와 마크다운만 그려집니다. `text/html` 출력 안의 스크립트는 프런트의 DOM, 저장소, API 쿠키에 닿지 못합니다(NFR-H3).
- 테스트(darkpyonix-ash): `test_fr_h16_renders_stored_outputs_without_a_kernel`, `test_fr_h16_marks_stale_cells`, `test_fr_h16_cell_split_matches_the_kernel_parser`(FORMAT 예시 파일로 FR-F1 파서와 같은 셀 목록인지 확인)

### FR-H17 라이브 커널로 열기 — `Draft`
호스팅한 노트북을 주인의 살아 있는 커널에 붙여 엽니다. 실행은 언제나 주인의 기기에서 일어나고, 허브는 실행하지 않습니다.
- 주인은 `PATCH /v1/notebooks/{notebook_id} {"share_id": "s_…"}`로 공유(FR-H4)를 겁니다. 그 공유는 같은 계정의 기기가 게시한 것이어야 합니다(아니면 409 `share_not_in_account`, 게시되지 않았으면 404). `{"share_id": null}`로 뗍니다. 노트북을 읽을 수 있는 사람에게 `share_id`가 보입니다. `share_id`만으로는 커널에 접근할 수 없습니다(토큰은 매니저가 검사, FR-A3).
- 링크는 `https://darkpyonix.dev/n/<notebook_id>#<공유 토큰>`입니다. 토큰은 URL 조각이라 허브로 가지 않습니다. 프런트는 조각에 토큰이 있고 노트북에 `share_id`가 있으면 `GET /v1/shares/{share_id}`(FR-H4)로 기기와 손님 릴레이 통행권을 받고, 브라우저 iroh로 그 기기의 전용 매니저에 붙습니다. 무엇을 할 수 있는지는 매니저가 토큰의 권한(`viewer1`~`admin`, FR-A3)으로 정합니다. 그냥 공유 링크 `https://darkpyonix.dev/s/<share_id>#<토큰>`도 그대로 씁니다.
- 라이브 화면은 호스팅한 판이 아니라 주인 기기의 파일(매니저의 `GET /kernels/{id}/document`)을 보여 줍니다. 호스팅한 판의 소스 해시와 다르면 프런트가 그 사실을 표시합니다.
- 토큰이 없거나, 공유가 내려갔거나, 기기에 닿지 못하면 프런트는 읽기 전용 보기(FR-H16)로 남고 이유를 보여 줍니다.
- 수용 기준: 같은 계정의 공유는 걸리고, 다른 계정의 공유는 409, 없는 공유는 404입니다. 노트북 메타데이터에 `share_id`가 나옵니다. 공유가 내려가면(FR-H4) 노트북의 `share_id`는 `null`이 됩니다. 브라우저에서 토큰이 있는 노트북 링크가 기기의 매니저에 붙어 실행하는 것(`viewer3`)과 토큰 없이 읽기 전용으로 남는 것은 릴레이 호스트 연동 시험으로 확인합니다.
- 테스트(`hub/worker/test/notebooks.test.ts`): `test_fr_h17_attach_a_share_of_the_same_account`, `test_fr_h17_unpublished_share_detaches`. 연동: `test_fr_h17_notebook_link_with_token_reaches_the_live_kernel`

### FR-H18 지우기 — `Draft`
- `DELETE /v1/notebooks/{notebook_id}`는 노트북과 모든 판을 지웁니다(204). 그 뒤 모든 읽기는 404이고, R2 본문은 같은 요청에서 지우되 실패한 것은 Cron이 24시간 안에 지웁니다.
- `DELETE /v1/notebooks/{notebook_id}/versions/{version}`은 판 하나를 지웁니다(204). 번호는 다시 쓰지 않고, `latest`는 남은 판 중 가장 높은 것을 가리킵니다. 남은 판이 없으면 `latest_version`은 `null`이고 `latest` 읽기는 404입니다.
- 권한은 FR-H14의 "지우기"와 같습니다. 기기를 지워도(FR-H1) 그 기기가 만든 노트북은 계정에 남고, 그 기기의 공유가 지워지므로 그 공유를 건 노트북의 `share_id`는 `null`이 됩니다.
- 계정 삭제는 지금 허브 연산이 없습니다. 생기면 그 계정의 노트북과 R2 본문을 모두 지우는 것을 그 요구사항의 수용 기준에 넣습니다.
- 수용 기준: 지운 노트북과 판은 주인에게도 404이고 R2에 본문이 남지 않습니다(Cron 뒤). 판을 지우면 `latest`가 내려가고, 새로 올린 판은 지운 번호 다음 번호입니다. 다른 계정이나 `client`의 삭제는 404/403입니다. 기기를 지우면 그 기기의 공유를 건 노트북의 `share_id`가 `null`이 됩니다.
- 테스트(`hub/worker/test/notebooks.test.ts`): `test_fr_h18_delete_notebook_removes_every_version`, `test_fr_h18_delete_version_keeps_numbers`, `test_fr_h18_device_removal_detaches_shares`, `test_fr_h18_cron_purges_orphaned_blobs`

### FR-H19 크기·할당량·남용 제한 — `Draft` [provisional]
값은 모두 실제 사용을 보고 바꿀 수 있는 처음 값입니다.
- **크기.** `source` 1 MiB, `run` 16 MiB(Worker 메모리 128 MB 안에서 JSON을 검사할 수 있는 크기이고, FR-R5가 셀 하나의 스트림을 16 MiB로 자르는 것과 맞춤), 요청 전체 18 MiB. 넘으면 413입니다.
- **할당량(계정당).** 노트북 200개, 노트북당 판 100개, 저장 합계 512 MiB. 넘으면 409 `quota_exceeded`이고, 오래된 판을 저절로 지우지 않습니다.
- **빈도.** 판 올리기는 계정당 분당 10번(Workers Rate Limiting), 노트북 만들기는 계정당 분당 10번입니다. 인증 없는 노트북 읽기는 IP당 분당 300번입니다. 넘으면 429입니다.
- **신고와 내리기.** 누구나 `POST /v1/notebooks/{notebook_id}/report {"reason": …}`로 `public`·`unlisted` 노트북을 신고할 수 있습니다(IP당 시간당 10번). 운영자는 `POST /admin/v1/notebooks/{notebook_id}/takedown`(`OPERATOR_SECRET`)으로 노트북을 내립니다. 내린 노트북은 주인 아닌 사람에게 404이고, 주인에게는 `taken_down: true`로 보이며 공개 범위를 넓힐 수 없습니다(409). 가입 허용 목록(`GITHUB_ALLOWED_IDS`, FR-H6)은 그대로 첫 방어선입니다.
- 수용 기준: 각 크기 한계를 1바이트 넘기면 413, 할당량을 넘기면 409 `quota_exceeded`, 빈도를 넘기면 429입니다. 내린 노트북은 인증 없이 404, 주인에게 `taken_down: true`이고, 운영자 비밀이 틀리면 401입니다.
- 테스트(`hub/worker/test/notebooks.test.ts`): `test_fr_h19_size_limits`, `test_fr_h19_quotas`, `test_fr_h19_rate_limits`, `test_fr_h19_report_and_takedown`

## 11. 비기능 요구사항

### NFR-K1 인터프리터 범위 — `Done`
커널과 런타임 API는 CPython 3.8 이상 모든 마이너 버전에서 동작합니다. 테스트는 그 기계에 있는 모든 인터프리터(`DARKPYONIX_TEST_PYTHONS`, 기본은 PATH에서 찾은 `python3.*`)로 커널 테스트를 돌립니다.
- 테스트: `python` 픽스처로 매개변수화된 커널·런타임 테스트 39개(`tests/conftest.py`)
- 측정 기록 (2026-10-03, macOS arm64): `DARKPYONIX_TEST_PYTHONS`에 CPython 3.8.20, 3.9.6, 3.10.20, 3.11.10, 3.12.13, 3.13.0(intel64), 3.14.7, 3.15.0rc1을 넣어 39개 × 8개 인터프리터 = 312개 중 304개 통과, 8개는 `DARKPYONIX_SKIP_PERF=1`로 뺀 NFR-K3 측정입니다(NFR-K3는 따로 돌려 통과). 3.8·3.10·3.12는 `uv python install`로 `.scratch/` 아래에 받은 인터프리터입니다.

### NFR-K2 표준 라이브러리 전용 — `Done`
`darkpyonix/kernel/darkpyonix/kernel/`, `darkpyonix/kernel/darkpyonix/*.py`, `darkpyonix/kernel/darkpyonix/format/`의 모든 import가 표준 라이브러리임을 테스트가 AST로 확인합니다(`sys.stdlib_module_names`, 3.8용 고정 목록 병행). 예외는 `darkpyonix/kernel/darkpyonix/kernel/mplbackend.py` 하나입니다. 사용자 코드가 pyplot을 import할 때 matplotlib이 직접 불러오는 백엔드 모듈이라 `matplotlib`을 import할 수 있고(FR-X6), 다른 커널 코드는 이 모듈을 import하지 않습니다.
- 테스트: `test_nfr_k2_kernel_imports_stdlib_only`, `test_nfr_k2_no_kernel_code_imports_the_matplotlib_backend`

### NFR-K3 출력 오버헤드 — `Done`
`print`를 100,000번 하는 셀의 실행 시간이 같은 인터프리터의 일반 실행 대비 1.5배를 넘지 않습니다. 구독자가 느려도 메인 스레드가 막히지 않습니다(출력 큐 상한을 넘으면 기록은 계속하되 실시간 이벤트를 합칩니다).
- 측정 기록 (2026-10-03, macOS arm64, `test_nfr_k3_print_overhead`): 셀 안 100,000번 `print`를 파이프로 출력하는 일반 실행과 비교, 5회 중 최솟값의 프로세스 CPU 시간 비율은 3.9 1.08, 3.11 1.17, 3.13 1.24, 3.14 1.42, 3.15 1.21입니다. 측정 당시 머신의 부하 평균이 100을 넘어 벽시계 시간은 같은 측정 안에서도 0.7~8배로 흔들렸으므로 판정에 쓰지 않았습니다. 한가한 머신에서 벽시계 시간을 다시 재야 `Done`이 됩니다.
- 측정 기록 (2026-10-03 14:2x, 맥미니 M 시리즈, 부하 평균 약 4.5, `test_nfr_k3_print_overhead` 3회): 벽시계 시간 비율(5회 중 최솟값끼리)은 3.8 1.17~1.23, 3.9 1.07~1.23, 3.10 1.11~1.13, 3.11 1.12~1.24, 3.12 1.14~1.16, 3.13 1.06~1.15, 3.14 1.14~1.19, 3.15 1.08~1.14로 모두 1.5배 이하입니다. 100,000줄 일반 실행은 16~25 ms였습니다.

### NFR-K4 시작 시간 — `Done`
커널 시작(프로세스 실행부터 announce까지)은 기준 기계(맥미니 M 시리즈)에서 300 ms 이하입니다.
- 측정 기록 (2026-10-03, 맥미니 M 시리즈, 부하 평균 약 4): `bootstrap_command`로 프로세스를 실행한 순간부터 등록 파일이 생길 때까지(등록 파일은 첫 멀티캐스트 announce 직전에 씁니다, `discovery.announce_now`) 7회 중앙값이 3.8 38 ms, 3.9 63 ms, 3.11 54 ms, 3.12 39 ms, 3.13 56 ms, 3.14 46 ms입니다. 같은 인터프리터의 `python -c pass`는 13~22 ms였습니다. 측정 스크립트는 `.scratch/k4/measure.py`(커밋하지 않음)입니다.
- 참고: Rust 매니저의 `ensure()`는 처음에 300~344 ms였습니다. 커널을 띄우기 전에 보내는 표적 멀티캐스트 질의가 없는 커널을 기다리느라 늘 200 ms(`QUERY_TIMEOUT`)를 썼기 때문입니다. 질의를 없앤 뒤(FR-M2, `fr_m2_ensure_starts_the_interpreter_without_a_discovery_wait`) 호출부터 인터프리터 실행까지 5~8 ms, 전체 92~120 ms입니다(2026-10-03, 부하 평균 약 1.5, `/usr/bin/python3` 셈은 Xcode 셈 때문에 170~600 ms).

### NFR-M1 발견 지연 — `Done`
매니저의 커널 목록 조회는 커널 20개에서 300 ms 이하입니다.
- 테스트: Rust `test_nfr_m1_list_kernels_with_20_real_kernels_within_300_ms`(`darkpyonix/manager/crates/darkpyonix/tests/manager_latency.rs`, 실제 커널 20개, 캐시·`refresh=true`·새 매니저의 첫 조회)
- 측정 기록 (2026-10-03, 맥미니 M 시리즈, 부하 평균 약 1.3, 디버그 빌드): 커널 20개에서 캐시 조회 p50 2.2 ms·p99 4.9 ms, `refresh=true` p50 3.9 ms·p99 5.1 ms, 새 매니저의 첫 조회 p50 4.6 ms·최대 4.7 ms입니다.

### NFR-M2 이벤트 지연 — `Done`
커널의 출력이 매니저 SSE 구독자에게 도달하기까지 p99 100 ms 이하입니다(스트림 병합 50 ms 포함).
- 테스트: Rust `test_nfr_m2_output_reaches_an_sse_subscriber_within_100_ms_p99`(`darkpyonix/manager/crates/darkpyonix/tests/manager_latency.rs`, 실제 커널의 `print` 시각부터 SSE 수신까지)
- 측정 기록 (2026-10-03, 맥미니 M 시리즈, 부하 평균 약 1.3, 디버그 빌드): 10 ms 간격 200줄에서 p50 31 ms, p99 61 ms, 최대 63 ms입니다(50 ms 병합 창 포함).

### NFR-M3 문서와 코드의 일치 — `Done`
매니저가 실제로 답하는 경로·메서드·응답 코드가 `docs/api/manager.openapi.yaml`과 같습니다. 구현 언어와 무관하게, 테스트는 모든 연산을 HTTP로 불러 문서에 있는 상태 코드로만 답하는지 확인합니다(`test_nfr_m3_every_operation_answers_with_a_documented_status`). 예외: API 문서 페이지(`/docs/`, `/docs/manager.openapi.yaml`, `/docs/hub.openapi.yaml`)는 계약 밖의 정적 파일입니다.
- 테스트: Rust `test_nfr_m3_every_operation_answers_with_a_documented_status`, `test_nfr_m3_documented_statuses_with_a_live_kernel_and_dedicated_mode`, `test_nfr_m3_undocumented_methods_are_not_served`(`darkpyonix/manager/crates/dpx-server/tests/openapi.rs`), 파이썬 시제품 기준 `test_nfr_m3_every_operation_answers_with_a_documented_status`

### NFR-H1 종단 간 암호화 — `Agreed`
허브는 중계하는 내용을 볼 수 없습니다. 기기 사이 연결은 iroh의 QUIC TLS 1.3이고, 상대 인증은 양쪽의 ed25519 엔드포인트 키로 끝단끼리 합니다. 세션 키는 허브를 거치지 않고 합의하며, 릴레이는 암호문 데이터그램만 전달합니다. 허브가 TLS를 끝내는 구성(FR-H5 방식 B)은 두지 않습니다. Cloudflare Worker(darkpyonix.dev)는 기기 사이 트래픽의 경로에 있지 않고(서명된 주소 레코드와 메타데이터만 다룸), 암호문이 지나가는 곳은 릴레이 호스트뿐입니다(INTENT D15). 릴레이를 Cloudflare Container로 옮기더라도 Cloudflare 프록시가 보는 것은 릴레이 WebSocket 안의 암호문입니다.
- 수용 기준: 릴레이 전용 연결로 알려진 평문 표식을 보낼 때, 클라이언트와 허브 사이의 바이트(허브까지 TLS 없이 평문 HTTP 릴레이로 둔 경우에도)에 그 표식이 나타나지 않고, 상대 끝단에서는 그대로 받습니다.
- 테스트: `test_nfr_h1_relay_sees_only_ciphertext`

### NFR-H2 토큰 노출 최소화 — `Agreed`
iroh의 기본 `PkarrResolver`는 헤더를 붙일 수 없어서 `GET /pkarr/{key}`의 자격 증명이 URL 쿼리(`?token=`)로 갑니다. URL은 로그·프록시·크래시 보고에 남기 쉬우므로 그 자리에 계정 권한이 있는 토큰을 두지 않습니다(Ember FR-N2 연동 중 보고, 2026-10-03).
- **조회 토큰(`dpr_…`).** 기기 링크를 받을 때(`POST /v1/device-links/{link_id}/token`의 201) 기기 토큰과 함께 `resolve_token`을 하나 받습니다. 조회 토큰은 같은 계정 기기의 `GET /pkarr/{key}`에만 쓰이고(쿼리 또는 `Authorization: Bearer`), 다른 연산에서는 자격 증명이 아닙니다(401). 해시로만 저장합니다. 기기는 `POST /v1/me/resolve-token`(자기 기기 토큰, 헤더)으로 새 조회 토큰을 받을 수 있고, 그러면 이전 조회 토큰은 즉시 무효입니다(교체). 지운 기기의 조회 토큰은 401 `device_removed`입니다.
- **쿼리의 기기 토큰은 받지 않습니다.** `?token=`에 기기 토큰(`dpd_…`)을 넣으면 401 `invalid_credentials`입니다. 기기 토큰과 세션은 헤더·쿠키로만 받습니다.
- **로그.** Worker 코드는 요청 URL, 쿼리, `Authorization` 헤더를 로그로 남기지 않습니다(예외 처리기는 예외만 남김). `wrangler.toml`은 Workers Logs의 호출 로그(요청 URL을 기록함)를 끄고(`[observability.logs] invocation_logs = false`), `console` 로그만 남깁니다. 운영자는 이 Worker에 요청 URL 필드를 담는 Logpush를 켜지 않습니다.
- 수용 기준: 링크로 받은 조회 토큰으로 `?token=` 조회가 200이고, 같은 조회 토큰으로 `GET /v1/devices`·`GET /v1/me`는 401입니다. 쿼리의 기기 토큰은 401 `invalid_credentials`입니다. 조회 토큰을 교체하면 이전 것은 401이고 새 것은 200입니다. 지운 기기의 조회 토큰은 401 `device_removed`입니다. 조회 요청(성공, 401, 404, 처리되지 않은 예외)을 처리하는 동안 Worker가 쓰는 `console` 출력에 토큰이 나타나지 않습니다. `wrangler.toml`의 호출 로그 설정이 꺼져 있습니다.
- 테스트(`hub/worker/test/tokens.test.ts`): `test_nfr_h2_resolve_token_reads_records_and_nothing_else`, `test_nfr_h2_query_refuses_device_tokens`, `test_nfr_h2_resolve_token_rotates`, `test_nfr_h2_removed_device_resolve_token_is_device_removed`, `test_nfr_h2_worker_never_logs_the_query`, `test_nfr_h2_invocation_logs_are_off`

### NFR-H3 남의 노트북 내용 격리 — `Draft`
호스팅한 노트북의 출력, 라이브 커널의 출력, 마크다운 안의 HTML, ash가 브라우저에서 돌리는 코드(Pyodide 등)는 모두 남이 쓴 것입니다. 메인 페이지 출처(`https://darkpyonix.dev`)는 API에 자격 증명을 실을 수 있으므로(FR-H13), 이것들은 메인 페이지 출처에서 돌지 않습니다(INTENT D15 개정).
- 스크립트가 돌 수 있는 내용은 `sandbox="allow-scripts"`(그 밖의 허용은 필요한 것만, **`allow-same-origin`은 절대 없음**) iframe 안에서만 돌립니다. 그 iframe은 `srcdoc`이나 `blob:`이 아니라 메인 페이지가 내는 렌더러 페이지 `https://darkpyonix.dev/sandbox/`를 읽고, 그릴 내용은 `postMessage`로 받습니다. `srcdoc`·`blob:` 문서는 메인 페이지의 CSP를 물려받아 출력 안의 인라인 스크립트(Plotly, Bokeh 등)가 막히지만, 따로 읽은 렌더러 페이지는 자기 CSP를 가집니다. 렌더러를 별도 등록 도메인으로 옮기는 것은 그 URL 하나만 바꾸면 되도록 둡니다. 그 iframe의 출처는 opaque(`null`)이므로 프런트의 DOM·저장소·쿠키에 닿지 못하고, 그 iframe의 요청에는 교차 사이트 규칙이 걸려 `SameSite=Lax` 세션 쿠키가 실리지 않으며, API는 `null` 출처에 CORS를 열지 않습니다(FR-H13).
- API의 노트북 본문 응답은 문서로 실행되지 않는 헤더를 붙입니다(FR-H14). 별도 콘텐츠 호스트나 R2 공개 버킷으로 본문을 내지 않습니다.
- **열린 질문(PROJECT Q10): 사용자 콘텐츠 도메인.** 렌더러 iframe을 `darkpyonix.dev`와 다른 등록 도메인(예: `darkpyonix-usercontent.dev`, 이름 미정)에서 띄울지 정하지 않았습니다. opaque 샌드박스는 메인 페이지의 DOM·저장소·API 쿠키를 지키지만, 같은 사이트 안에 남는 것(Safe Browsing 판정이 `darkpyonix.dev` 전체에 걸릴 위험, 브라우저에 따라 같은 프로세스, 그 프레임 안의 피싱 화면)과 opaque 출처의 제약(저장소·서비스 워커 없음)은 풀지 못합니다. 정해지면 이 항목과 FR-H12의 `frame-src`를 고칩니다.
- 사용자 이름 서브도메인(`<name>.darkpyonix.dev`, FR-H5)은 같은 사이트의 남의 서버입니다. 세션 쿠키는 `__Host-`라 그 서브도메인이 덮어쓸 수 없고, 출처 검사가 그 서브도메인의 쿠키 쓰기를 막습니다(FR-H13).
- 수용 기준: `text/html` 출력으로 `<script>`가 `document.cookie`, `parent.document`, `localStorage`를 읽고 `fetch("https://api.darkpyonix.dev/v1/me", {credentials: "include"})`를 부르는 노트북을 열었을 때, 앞의 셋은 예외나 빈 값이고, API 요청은 쿠키 없이 `Origin: null`로 도착해 401이며 응답을 읽지 못합니다. 같은 내용을 API 본문 주소로 직접 열면 내려받기가 되고 실행되지 않습니다.
- 테스트: darkpyonix-ash 브라우저 시험 `test_nfr_h3_untrusted_output_cannot_reach_the_front_or_the_api`, Worker `test_nfr_h3_null_origin_gets_no_cors`

## 12. 프로토콜 요구사항

### PR-1 DKP/1 프레임 — `Done`
PROTOCOL §3.1. 64 MiB를 넘는 프레임은 거절합니다.
- 테스트: `test_pr_1_oversized_frame_is_refused`, `test_pr_1_frame_too_large_closes_connection`

### PR-2 핸드셰이크 — `Done`
PROTOCOL §3.2. 5초 제한, 상수 시간 비교.
- 테스트: `test_pr_2_handshake_times_out`

### PR-3 이벤트 재전송 — `Done`
PROTOCOL §3.4. 링 버퍼와 `replay_truncated`. 읽지 않는 구독자가 있어도 이벤트를 내는 쪽은 막히지 않습니다(클라이언트별 송신 버퍼, 64 MiB를 넘으면 그 클라이언트를 끊음).
- 테스트: `test_pr_3_subscribe_replays_since_and_streams_live`, `test_pr_3_replay_truncated_when_ring_overflows`, `test_pr_3_ring_is_bounded_by_bytes`, `test_slow_subscriber_does_not_block_append`
- 측정 기록: 이벤트 append → 루프백 클라이언트 수신 지연, 1,000개 기준 p50 약 0.04–0.15 ms, p99 0.1–3.4 ms (맥미니, 다른 작업으로 load average 약 31인 상태, Python 3.13; 3.9·3.11도 p99 0.4–4.2 ms). `test_event_latency_p99`

### PR-4 호환성 — `Done`
모르는 필드는 무시하고, 필드 추가는 버전을 올리지 않습니다. 의미를 바꾸는 변경은 `dkp` 버전을 올립니다.
- 테스트: `test_pr_4_unknown_fields_are_ignored`
