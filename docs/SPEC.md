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
`shutdown`은 실행 중인 셀을 인터럽트하고, 실행 기록을 마저 쓰고, `bye`를 보내고, 코드 0으로 끝납니다.
- 수용 기준: 종료 뒤 잠금이 남지 않고 실행 기록의 상태는 `interrupted`입니다.
- 상태 메모 (2026-10-03): 등록 파일을 지웠으므로(FR-D2) "종료 뒤 등록 파일 없음" 검사는 의미가 없어집니다. 테스트에서 그 검사를 빼는 일은 #50에서 합니다.
- 테스트: `test_fr_k8_shutdown_interrupts_running_cell_and_finishes_run`(실행기 쪽), `test_fr_k8_shutdown_is_graceful`(실제 커널 프로세스: 셀이 도는 중에 `shutdown`을 보내면 `bye` 데이터그램, 종료 코드 0, 등록 파일 없음, 잠금을 곧바로 다시 잡을 수 있음, 실행 기록 `interrupted`). "잠금이 남지 않음"은 OS 잠금이 풀린다는 뜻이고, 잠금 파일 자체는 지우지 않습니다(지우면 `flock`과 경합이 생깁니다).

### FR-K9 넘겨받은 스트림 — `Agreed` (사용자 결정 2026-10-03, 구현 대기 #47)
커널은 매니저가 `adopt`로 넘긴 연결(PROTOCOL §3.7)에 직접 응답합니다. `events`는 SSE, `ws`는 2025 WebSocket 동기화(PROTOCOL §5), `wait`는 실행 대기 응답입니다. 커널은 넘겨받은 요청 바이트를 소켓에서 읽은 것처럼 다루고, 인증하거나 토큰을 검사하지 않습니다. 권한 라벨(`label.permission`)은 보낼 내용을 거르는 데만 씁니다. `viewer1`에게는 출력과 실행 기록을 보내지 않고, `viewer1`·`viewer2`의 WebSocket 편집 메시지(`start_typing`)는 받지 않습니다(FR-A3). `streams.close {share_id}`가 오면 그 공유로 넘겨받은 스트림을 모두 닫습니다.
- 수용 기준(사용 가능한 모든 인터프리터, NFR-K1):
  - POSIX에서 `SCM_RIGHTS`로 넘긴 SSE 연결에 커널이 `200 text/event-stream`과 이벤트를 쓰고, 넘긴 매니저를 `SIGKILL`로 죽인 뒤에도 같은 연결로 이벤트가 계속 옵니다.
  - Windows에서 `socket.share` 형식의 바이트로 넘긴 연결이 같은 결과를 냅니다.
  - 넘겨받은 요청에 틀린 토큰이 들어 있어도 커널은 라벨대로 응답합니다(토큰을 보지 않음).
  - `viewer1` 라벨의 SSE에는 `output` 이벤트가 없습니다.
  - `streams.close`가 그 `share_id`의 스트림만 닫고, 다른 스트림은 그대로입니다.
- 테스트(계획): `test_fr_k9_adopted_sse_survives_manager_kill`, `test_fr_k9_adopted_socket_on_windows`, `test_fr_k9_kernel_ignores_tokens_in_adopted_request`, `test_fr_k9_viewer1_label_filters_outputs`, `test_fr_k9_streams_close_closes_only_that_share`

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
PROTOCOL §2를 구현합니다. 매니저의 query에 같은 사용자의 모든 커널이 200 ms 안에 응답합니다. 발견은 이 루프백 멀티캐스트 하나입니다(INTENT D4). 그룹 주소와 포트(`239.255.68.80:46880`)는 리더 결정, 사용자 확인 대기입니다(PROJECT Q14).
- 수용 기준: 커널 세 개를 띄우고 query 하나를 보내면 세 개의 announce를 받습니다. 다른 `user_tag`의 query에는 응답하지 않습니다.
- 테스트: `test_fr_d1_query_finds_all_kernels`, `test_fr_d1_other_user_tag_is_ignored`, `test_fr_d1_listener_sees_announce_and_bye`, Rust `fr_d1_query_finds_all_kernels`(`darkpyonix/manager/crates/dpx-kernel/tests/discovery_launch.rs`)

### FR-D2 등록 파일 보조 — `Withdrawn` (사용자 지시 2026-10-03, 코드 제거 대기 #50)
커널 등록 파일(`kernels/<kernel_id>.json`)과 `DARKPYONIX_DISCOVERY=registry`를 지웁니다. 리더가 사용자 승인 없이 더한 보조 발견입니다. 사용자(2026-10-03): "커널 등록 파일 써도 된다고 내가 말한 적이 절대 없고요"(INTENT D4, §5).
- 코드: 커널은 등록 파일을 쓰지 않고, 매니저는 등록 파일을 읽지 않습니다. `test_fr_d2_*`와 Rust `fr_d2_*` 테스트는 코드와 함께 지웁니다(#50).
- 수용 기준(#50): 커널을 띄우고 끈 뒤에도 `DARKPYONIX_HOME/kernels/` 아래에 `.json`이 생기지 않습니다(진단 로그 `.log`만 있음). `DARKPYONIX_DISCOVERY=registry`를 줘도 동작이 같습니다.
- 테스트(계획): `test_fr_d2_kernel_writes_no_registry_file`

### FR-D3 지원 범위 — `Agreed` (사용자 결정 2026-10-03)
사용자 결정: "멀티캐스트가 막힌 특수한 이상한 상황은 가정하지 말고, wsl 안에서 돌고 있는거에 윈도우에서 연결해야 할 이유도 없어. 안드로이드랑 iOS의 경우 PyREPL 구현처럼 별도 미리 정의된 프로세스 내에서만 노트북이 실행 가능하도록 하면 되는거야."
- 루프백 멀티캐스트가 막힌 환경(멀티캐스트 루프백이 없는 컨테이너 네트워크, 그것을 막는 방화벽·VPN 소프트웨어 등)은 지원 범위 밖입니다. 보조 발견(등록 파일, 커널 ID에서 정하는 결정적 포트)은 두지 않습니다.
- WSL 안에서 도는 커널에 Windows 쪽 매니저나 클라이언트가 붙는 경우는 지원 범위 밖입니다. WSL 안에서는 WSL 안의 매니저를 씁니다.
- Android와 iOS는 데스크톱 발견을 쓰지 않습니다. 앱 안에 미리 정한 프로세스(PyREPL 방식)에서만 노트북을 실행합니다(INTENT D20). 이 경로는 Ember 모바일 앱이 다루고, 이 저장소의 SPEC 범위 밖입니다. 상태: 계획.
- 수용 기준: 이 요구사항은 범위를 정하는 것이라 테스트가 없습니다. FR-D1의 테스트가 지원 범위 안의 동작을 검증합니다.

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

### FR-M1 HTTP API — `Agreed` (스트림 넘김으로 바뀜, 구현 대기 #47)
매니저는 [api/manager.openapi.yaml](api/manager.openapi.yaml)의 경로를 모두, 그리고 그 경로만 냅니다. 이벤트 스트림은 SSE(`text/event-stream`)이고 SSE `id`는 커널의 `seq`입니다. `Last-Event-ID` 헤더나 `since` 쿼리로 이어 받습니다. OpenAPI에 `x-darkpyonix-handoff: kernel`로 표시한 연산(이벤트 스트림, WebSocket 동기화, 실행 대기)은 매니저가 인증·권한 검사만 하고 연결을 커널에 넘깁니다(FR-M6). 응답은 커널이 씁니다.
- 상태 메모 (2026-10-03): 아래 테스트는 매니저가 이벤트를 중계하던 구현을 검증합니다. 사용자 결정 "스트림만 넘김"(INTENT D6)으로 이벤트 스트림의 응답 주체가 커널로 바뀌므로, 넘김 구현(#47)과 함께 다시 통과해야 `Done`입니다.
- 테스트: Rust `test_nfr_m3_every_operation_answers_with_a_documented_status`, `test_nfr_m3_undocumented_methods_are_not_served`(`darkpyonix/manager/crates/dpx-server/tests/openapi.rs`), `test_fr_m1_events_stream_resumes_with_last_event_id`, `test_fr_m1_events_errors_and_keepalive`(`darkpyonix/manager/crates/dpx-server/tests/sse.rs`), 각 경로의 동작 테스트(`darkpyonix/manager/crates/dpx-server/tests/api.rs`). 파이썬 시제품 기준 `test_fr_m1_*`(`tests/test_fr_m_manager.py`)

### FR-M2 커널 시작은 멱등 — `Done`
`POST /kernels {path}`는 그 파일의 커널이 살아 있으면 그 커널을 `200`으로, 없으면 새로 띄워서 `201`로 돌려줍니다. 커널이 announce를 낼 때까지 최대 10초를 기다립니다.
- 테스트: `test_fr_m2_start_kernel_is_idempotent`(Rust `darkpyonix/manager/crates/dpx-server/tests/api.rs`, 파이썬 시제품), Rust `fr_m2_start_kernel_is_idempotent_on_every_interpreter`, `fr_m2_start_timeout_when_no_announce`, `fr_m2_ensure_starts_the_interpreter_without_a_discovery_wait`, `fr_m2_ensure_attaches_to_a_live_kernel_missing_from_the_registry`(`darkpyonix/manager/crates/dpx-kernel/tests/discovery_launch.rs`)

### FR-M3 임시 모드 수명 — `Agreed` (유휴 판정이 바뀜, 구현 대기 #47)
임시 매니저는 `127.0.0.1`의 임의 포트에 리슨합니다. 주소와 토큰은 자기를 띄운 프로세스에게 표준 출력 한 줄(JSON `{url, token, pid}`)로만 알리고 파일에 쓰지 않습니다(INTENT D4, 구현 대기 #50). HTTP 요청이 없는 상태가 `idle_timeout`(기본 120초) 동안 이어지면 스스로 끝납니다. 커널에 넘긴 스트림은 세지 않습니다. 매니저가 그 연결을 들고 있지 않고, 매니저가 끝나도 이어지기 때문입니다(INTENT D6, 구현 대기 #47). 끝날 때 커널은 건드리지 않습니다.
- 테스트: `test_fr_m3_ephemeral_manager_exits_when_idle_and_kernels_remain`(Rust `darkpyonix/manager/crates/dpx-server/tests/lifecycle.rs`, 파이썬 시제품), Rust `test_fr_m3_registry_file_is_private_and_complete`, `test_fr_m3_shutdown_removes_registry_file`(등록 파일을 지우므로 #50에서 `test_fr_m3_url_and_token_are_printed_on_stdout`, `test_fr_m3_writes_no_registry_file`로 바꿉니다)

### FR-M4 전용 모드 — `Done`
`darkpyonix manager --dedicated`는 유휴 종료 없이 돌고, 설정한 호스트·포트에 리슨하고, 마스터 토큰과 공유 토큰으로 인증합니다. 토큰은 해시로만 `manager.db`(SQLite)에 저장합니다. 2025 비밀번호 인증과 파일 단위 토큰(FR-A4), 토큰 수명 규칙(FR-A5)도 전용 매니저가 맡습니다(구현 대기 #48).
- 테스트: Rust `test_fr_m4_dedicated_manager_never_idles_and_requires_token`(`darkpyonix/manager/crates/dpx-server/tests/lifecycle.rs`), `test_fr_m4_shares_are_stored_hashed_and_survive_restart`(`darkpyonix/manager/crates/dpx-server/tests/api.rs`)

### FR-M5 매니저 여러 개 공존 — `Done`
같은 사용자의 매니저 여러 개가 같은 커널에 동시에 붙을 수 있고, 각자 같은 이벤트를 받습니다.
- 테스트: `test_fr_m5_two_managers_share_one_kernel`(파이썬 시제품), Rust `fr_m5_two_managers_share_one_kernel`, `fr_m5_reconnects_on_demand_after_kernel_restart`(`darkpyonix/manager/crates/dpx-kernel/tests/dkp_fake_kernel.rs`)

### FR-M6 오래 열린 스트림은 커널에 넘김 — `Agreed` (사용자 결정 2026-10-03, 구현 대기 #47)
INTENT D6, PROTOCOL §3.7. 사용자 결정: "스트림만 넘김". 매니저는 이벤트 스트림(`GET /api/kernels/{kernel_id}/events`), WebSocket 동기화(`GET /api/ws/kernels/{kernel_id}`), 실행 대기(`GET /api/kernels/{kernel_id}/runs/{run_ref}/wait`)를 인증하고 권한을 검사한 뒤 연결을 커널에 넘깁니다. 이미 읽은 요청 바이트와 `{permission, client_id, user, nickname, share_id}`를 함께 보냅니다. 나머지 REST 호출은 매니저가 DKP/1 요청으로 처리합니다.
- POSIX 날 소켓: 커널의 제어 채널인 유닉스 도메인 소켓(announce의 `control`, FR-A1)으로 `SCM_RIGHTS`. 넘긴 뒤 매니저는 자기 FD를 닫습니다.
- Windows 날 소켓: `WSADuplicateSocketW`로 만든 `WSAPROTOCOL_INFOW`를 제어 파이프 위 `adopt`의 `share`로 보냅니다.
- TLS, HTTP/2, P2P 터널: 소켓 쌍의 한쪽을 넘기고 바이트를 퍼 나릅니다. 본문은 바꾸지 않습니다. 이런 스트림은 매니저와 함께 끝납니다.
- 공유를 지우거나 그 타입의 토큰을 다시 만들면 `streams.close {share_id}`를 보냅니다.
- 인증이나 권한에서 실패한 요청은 넘기지 않고 매니저가 `401`/`403`/`404`로 답합니다.
- 수용 기준:
  - 평문 HTTP로 연 이벤트 스트림이 넘겨진 뒤 매니저를 `SIGKILL`로 죽여도, 다음 출력 이벤트가 같은 연결로 옵니다.
  - TLS(전용 모드)로 연 이벤트 스트림은 넘겨진 뒤에도 이벤트가 오고, 매니저가 끝나면 함께 끝납니다.
  - 틀린 토큰의 스트림 요청은 커널에 닿지 않고 `401`입니다.
  - 공유를 지우면 그 공유 토큰으로 연 스트림이 1초 안에 닫히고, 마스터 토큰으로 연 스트림은 남습니다.
- 테스트(계획): `test_fr_m6_plain_stream_survives_manager_kill`, `test_fr_m6_tls_stream_is_pumped_and_ends_with_manager`, `test_fr_m6_unauthenticated_stream_never_reaches_kernel`, `test_fr_m6_revoking_a_share_closes_its_streams`

## 8. CLI (C)

### FR-C1 매니저 찾기 — `Agreed` (사용자 지시, 구현 대기 #50)
매니저의 주소와 토큰을 파일로 나누지 않습니다(INTENT D4). `darkpyonix` CLI는 `DARKPYONIX_MANAGER_URL`과 `DARKPYONIX_TOKEN`이 있으면 그 매니저를 씁니다. 에이전트가 이미 아는 토큰입니다. 없으면 자기 임시 매니저를 자식 프로세스로 띄우고, 그 매니저가 표준 출력 첫 줄에 쓴 `{url, token, pid}`를 파이프로 읽어 씁니다. 토큰을 모르는 에이전트는 이렇게 자기 매니저를 씁니다. 커널은 모든 매니저가 멀티캐스트로 찾으므로 어느 매니저로 붙어도 같은 커널입니다(FR-M5).
- `darkpyonix manager`를 직접 실행해도 같은 한 줄을 표준 출력에 씁니다. 에이전트는 그 값을 환경 변수로 두고 다음 호출에 쓸 수 있습니다. 두지 않으면 호출마다 새 임시 매니저가 뜨고, 각자 `idle_timeout` 뒤에 끝납니다.
- 수용 기준: 환경 변수 없이 `darkpyonix run a.py`가 매니저를 띄워 동작하고, `DARKPYONIX_HOME/managers/`가 생기지 않습니다. 환경 변수를 주면 새 매니저를 띄우지 않습니다.
- 테스트(계획): `test_fr_c1_cli_spawns_its_own_manager_and_reads_stdout`, `test_fr_c1_cli_uses_the_given_manager`, `test_fr_c1_spawn_failure_is_reported`. 지금의 `test_fr_c1_skips_dead_and_unhealthy_registrations`는 지웁니다.

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
| `darkpyonix manager [--ephemeral\|--dedicated] [--host H] [--port P] [--idle-timeout S]` | 매니저 실행. 기본은 `--ephemeral`(FR-M3: 루프백, 유휴 시 종료, 주소와 토큰은 표준 출력으로), `--dedicated`는 FR-M4. 둘을 함께 주면 오류입니다 |

- 수용 기준: `darkpyonix run a.py`를 두 번째로 실행하면 종료 코드 75와 함께 현재 실행 정보와 `--queue`/`stop` 안내를 출력합니다.
- 테스트: Rust `test_fr_c2_cli_commands`, `test_fr_c2_second_run_exits_75_with_hint`, `test_fr_c2_run_follows_outputs_and_exits_0`, `test_fr_c2_run_error_exits_1_with_traceback`, `test_fr_c2_ctrl_c_interrupts_the_run_and_exits_130`, `test_fr_c2_second_ctrl_c_detaches_and_the_run_continues`, `test_fr_c2_detach_prints_the_run_id_and_does_not_follow`, `test_fr_c2_run_options_reach_the_api`, `test_fr_c2_logs_follow_replays_the_executing_run`(`darkpyonix/manager/crates/darkpyonix/tests/cli.rs`), `test_fr_c2_every_command_parses`, `test_fr_c2_invalid_usage_is_rejected`(`darkpyonix/manager/crates/darkpyonix/src/args.rs`)
- 상태 메모 (2026-10-03 감사): `attach`, 전용 매니저에서의 `share`, `manager` 하위 명령은 인자 파싱 시험만 있고 동작 시험은 없습니다.

## 9. 인증과 공유 (A)

### FR-A1 커널 인증 — `Agreed` (사용자 결정 2026-10-04, 구현 대기 #55)
INTENT D5, PROTOCOL §3.2. 사용자 결정(2026-10-04): "같은 컴퓨터의 같은 계정만 통과시켜야 해." 방법은 사용자가 고른 "OS가 알려 주는 상대 계정 확인"입니다. 커널의 제어 채널은 POSIX 유닉스 도메인 소켓과 Windows 이름 있는 파이프이고, 커널은 연결마다 상대 UID(POSIX)나 SID(Windows)가 자기 계정과 같은지 OS에 묻습니다. 매니저도 커널 쪽 계정을 같은 방법으로 확인합니다. 키 파일과 HMAC은 없습니다. 클라이언트의 인증과 권한 검사는 매니저가 하고, 커널은 매니저를 믿습니다.
- 수용 기준(사용 가능한 모든 인터프리터, POSIX와 Windows):
  - 다른 OS 계정의 프로세스가 커널의 소켓이나 파이프에 붙으면 커널은 프레임을 하나도 보내지 않고 닫습니다.
  - 같은 계정의 프로세스는 키 없이 `welcome`까지 갑니다.
  - 커널은 TCP 포트를 열지 않습니다.
  - 매니저는 소켓이나 파이프 건너편이 다른 계정이면 요청을 보내지 않고 그 커널을 `kernel_unreachable`로 둡니다.
  - 런타임 홈에 `user.key`가 생기지 않습니다.
  - 다른 계정 시험은 두 번째 OS 계정을 만들 수 있는 CI에서 돕니다.
- 테스트(계획): `test_fr_a1_other_account_is_refused_by_peer_identity`, `test_fr_a1_same_account_connects_without_a_key`, `test_fr_a1_no_tcp_port_is_opened`, `test_fr_a1_manager_refuses_a_kernel_of_another_account`. 지금의 `test_fr_a1_wrong_key_is_rejected`, `test_fr_a1_hello_carries_identity_and_nonce`, `test_fr_a1_user_key_is_created_once_with_0600`는 지웁니다(#55).

### FR-A2 매니저 토큰 — `Agreed` (인증 예외와 WebSocket 토큰이 더해짐, 구현 대기 #48, #49)
모든 HTTP 요청은 `Authorization: Bearer <token>`이 필요합니다. 헤더를 붙일 수 없는 SSE(`EventSource`), 2025 WebSocket 동기화(`/api/ws/kernels/{kernel_id}?token=`, FR-S9)와 공유 링크만 `?token=`을 받습니다. `/health`와 2025 인증 계열의 세 연산(초기 토큰 발급, 비밀번호 로그인, 마스터 토큰 재설정, FR-A4)만 `Authorization` 없이 열립니다. 뒤의 둘은 본문의 비밀번호로 인증합니다.
- 테스트: `test_fr_a2_requests_without_token_are_401`(Rust `darkpyonix/manager/crates/dpx-server/tests/api.rs`, 파이썬 시제품), Rust `test_fr_a2_registry_token_is_used`(`darkpyonix/manager/crates/darkpyonix/tests/cli.rs`, 등록 파일 제거와 함께 #50에서 `test_fr_a2_cli_uses_the_token_from_stdout`으로 바꿈)

### FR-A3 공유 권한 — `Agreed` (사용자 결정 2026-10-03, 구현 대기 #49)
INTENT D18. 사용자 결정: "원 설계대로 복구". 공유 토큰은 커널(파일)마다 발급하고 권한은 2025 설계의 넷입니다. 2025 요청 인자 `user_permission: "write"`가 쓰기를 뜻하고, 쓰기는 `viewer3`와 `admin`입니다.

| 권한 | 셀 코드 | 실행 기록·출력 | 쓰기(`user_permission: "write"`): 실행·인터럽트·셀 편집·잠금 | 종료·공유 관리 |
|---|---|---|---|---|
| `viewer1` | ✓ | | | |
| `viewer2` | ✓ | ✓ | | |
| `viewer3` | ✓ | ✓ | ✓ | |
| `admin`(마스터, 파일 관리자 토큰) | ✓ | ✓ | ✓ | ✓ |

작업별 최소 권한: 커널·문서 조회 `viewer1`(실행 기록과 출력은 `viewer2`부터), 네임스페이스 조회 `viewer2`, 실행·인터럽트·대기 실행 취소·셀 생성·수정·삭제·이동·잠금·해제 `viewer3`, 재시작·종료·공유 관리·새 커널 시작 `admin`. 접속자 표시와 포커스는 `viewer1`부터입니다. 쓰기 요청은 `user_permission: "write"`를 실을 수 있지만, 허락은 토큰이 정합니다. 쓰기가 없는 토큰은 `403 forbidden`(2025 `INSUFFICIENT_PERMISSION`, "Read-only access, cannot edit cells")입니다. 공유 토큰은 한 커널에만 묶이며, 다른 커널을 가리키면 `403`이 아니라 `404`입니다. 권한 검사는 매니저만 합니다(INTENT D5).
- `editor` 등급은 지웠습니다. 리더가 사용자 승인 없이 더한 등급입니다(INTENT §5). 셀 편집을 `viewer3`에 여는 대응은 2025 문서에서 끌어낸 것이라 사용자 확인 대기입니다(PROJECT Q13).
- 수용 기준: 위 표대로 각 권한이 허락되거나 `403`을 받습니다. `viewer2` 토큰으로 `user_permission: "write"`를 실은 잠금 요청은 `403`입니다. `editor`로 공유를 만들면 `400`입니다.
- 테스트: Rust `test_fr_a3_permission_matrix`, `test_fr_a3_share_tokens_are_scoped_to_one_kernel_and_permission`, `test_fr_a3_ephemeral_manager_refuses_share_creation`(`darkpyonix/manager/crates/dpx-server/tests/api.rs`), `test_fr_a3_viewer1_events_omit_outputs`(`darkpyonix/manager/crates/dpx-server/tests/sse.rs`). 계획: `test_fr_a3_write_needs_viewer3`, `test_fr_a3_user_permission_write_does_not_raise_a_viewer_token`, `test_fr_a3_editor_is_not_a_permission`
- 상태 메모: 위 Rust 테스트는 `editor`가 있던 표를 검증합니다. 새 표로 고친 뒤 통과해야 `Done`입니다.

### FR-A4 비밀번호 인증 (2025 복구) — `Agreed` (사용자 결정 2026-10-03, 구현 대기 #48)
INTENT D17. 사용자 결정: "원 설계대로 복구". 전용 매니저는 파일(커널 ID)마다 비밀번호 하나, 관리자(마스터) 토큰, 공유 토큰을 둡니다. 2025 설계는 토큰을 `File`에 두었습니다(`class File: kernel_id, tokens, cells`). 임시 매니저는 이 연산에 `403 forbidden`으로 답합니다.

| 2025 기능 | 2025 경로(요약표 / 상세) | 이 API | 인증 | 결과 |
|---|---|---|---|---|
| 초기 토큰 | 상세 `POST /kernels/{kernel_id}/tokens/initial` | `POST /api/kernels/{kernel_id}/tokens/initial` | 없음 | `{token, permission: "admin", password_required: true}` |
| 비밀번호 재설정 | `PUT /auth/password` / 상세 `PUT /kernels/{kernel_id}/password` | `PUT /api/kernels/{kernel_id}/password` | 초기 토큰(첫 설정) 또는 그 파일의 관리자 토큰 | `{message, kernel_id}` |
| 로그인 | `GET /auth` / 상세 `POST /kernels/{kernel_id}/tokens/auth` | `POST /api/kernels/{kernel_id}/tokens/auth` | 본문의 비밀번호 | `{token, kernel_id, permission}` |
| 마스터 토큰 재설정 | `PUT /auth/tokens/master` | `PUT /api/kernels/{kernel_id}/tokens/master` | 본문의 비밀번호 | 새 관리자 토큰. 이전 관리자 토큰은 `401` |
| 공유 토큰 재설정 | `PUT /auth/tokens/shared/{token_type}` | `PUT /api/kernels/{kernel_id}/tokens/shared/{token_type}` | 그 파일의 관리자 토큰 | 그 타입의 공유를 모두 거두고 새 공유 토큰 하나 |
| 공유 토큰 발급 | 상세 `POST /kernels/{kernel_id}/tokens/share` | `POST /api/kernels/{kernel_id}/shares`(이미 있음) | 관리자 토큰 | `{share_id, token, url, permission}` |
| 토큰 확인 | 상세 `GET /kernels/{kernel_id}/tokens/{token}/verify` | `GET /api/kernels/{kernel_id}/tokens/{token}/verify` | 같은 토큰 | `{valid, kernel_id, permission, blacklisted, password_set}` |

- 2025 문서는 같은 기능을 요약표와 상세 페이지 두 표기로 적었습니다. 상세가 요청·응답 본문을 가지고 있고, 비밀번호가 파일마다라는 것(`PASSWORD_NOT_SET: Password has not been set for this kernel`)과 맞습니다. 그래서 경로는 상세 모양을 `/api` 아래(INTENT D16)에 두고, 상세가 없는 두 재설정은 요약표의 끝 조각을 같은 파일 단위 경로에 붙였습니다. 이 대응은 리더 결정, 사용자 확인 대기입니다(PROJECT Q10).
- 비밀번호는 솔트를 넣은 느린 해시로, 토큰은 해시로만 `manager.db`에 둡니다. 커널은 비밀번호와 토큰을 모릅니다(INTENT D5).
- 파일 단위 관리자 토큰은 `admin` 권한이고 그 커널에만 묶입니다. 다른 커널을 가리키면 `404`입니다(공유 토큰과 같은 규칙, FR-A3). 전용 매니저 전체의 마스터 토큰(FR-M4)은 그대로 있습니다.
- 초기 토큰은 2025 명세 그대로입니다. 커널을 열 때 자격 증명 없이 받고(본문은 2025처럼 `{kernel_id}`를 실을 수 있음), 응답은 `{token, permission: "admin", password_required: true}`입니다. 커널 ID 형식이 틀리면 `400`(2025 `INVALID_KERNEL_ID`)입니다. 발급 횟수, 출발지(루프백), 쓰임새에 제한을 두지 않습니다. 사용자 결정(2026-10-04): "초기 토큰은 애초에 열 때 토큰을 발급했을건데 뭐가 문제야? 토큰이 없으면 연결이 안되잖아. 초기 토큰 발급은 건드리지 마." 리더가 더했던 "한 번만 발급"(`409 already_initialized`)과 "비밀번호 설정에만 씀"은 지웠습니다(PROJECT Q11).
- 오류 코드: 2025 `PASSWORD_NOT_SET` → `400 password_not_set`, `INVALID_TOKEN` → `401 unauthorized`, `TOKEN_BLACKLISTED` → `401 token_revoked`, `INSUFFICIENT_PERMISSION` → `403 forbidden`, `KERNEL_NOT_FOUND` → `404 not_found`.
- 수용 기준: 초기 토큰 → 비밀번호 설정 → 로그인 → 마스터 토큰 재설정 순서가 되고, 재설정 뒤 이전 관리자 토큰은 `401`입니다. 비밀번호 전 로그인은 `400 password_not_set`입니다. 공유 토큰 재설정 뒤 그 타입의 이전 공유 토큰은 `401 token_revoked`이고, 그 토큰으로 연 스트림은 닫힙니다(FR-M6). 임시 매니저는 모두 `403`입니다.
- 테스트(계획): `test_fr_a4_initial_password_login_master_reset`, `test_fr_a4_login_before_password_is_400`, `test_fr_a4_initial_token_is_issued_without_credentials_every_time`, `test_fr_a4_shared_token_reset_revokes_and_closes_streams`, `test_fr_a4_verify_reports_permission_and_revocation`, `test_fr_a4_ephemeral_manager_refuses_auth_family`

### FR-A5 토큰 수명 (2025 복구) — `Agreed` (사용자 결정 2026-10-03, 구현 대기 #48)
2025 설계: "커널이 파일과 논리 커널로 분리되어 커널이 지워지면 토큰은 보관되나, 파일이 지워지면 토큰도 지워져야 함", "공유 버튼을 눌렀다가 다시 해제하고 다시 누르는 경우 토큰 초기화 필요".
- 커널을 종료하거나 재시작해도, 매니저를 다시 시작해도 그 파일의 비밀번호, 관리자 토큰, 공유 토큰은 남습니다.
- 파일이 지워지면 매니저는 그 파일의 비밀번호와 토큰을 모두 지웁니다. 매니저는 `manager.db`에 커널 ID와 함께 경로를 두고, 시작할 때와 그 파일의 토큰을 쓸 때마다 경로가 있는지 확인합니다. 파일을 옮기거나 이름을 바꾸면 커널 ID가 바뀌므로(INTENT D1) 옛 경로의 토큰도 같은 규칙으로 지워집니다.
- 공유를 거둔 뒤 다시 공유하면 새 토큰이 나옵니다. 거둔 토큰은 되살리지 않습니다.
- 수용 기준: 커널 종료와 매니저 재시작 뒤에도 같은 공유 토큰이 통합니다. 파일을 지우면 그 토큰이 `401`이 되고 `manager.db`에 행이 남지 않습니다. 공유를 거두고 다시 만들면 토큰이 다릅니다.
- 테스트(계획): `test_fr_a5_tokens_survive_kernel_shutdown_and_manager_restart`, `test_fr_a5_tokens_are_deleted_with_the_file`, `test_fr_a5_reshare_issues_a_new_token`

## 10a. 협업 문서 (S)

한 커널(=파일)에 여러 클라이언트가 동시에 붙습니다. VS Code 확장, IntelliJ, ash, Ember 대화 화면, 에이전트가 함께 붙을 수 있습니다. 2025 설계의 셀 동기화, 셀 잠금, 포커스, 실행 알림, 알람을 이어받습니다(`설계초안/`의 WS 명세). 2025 WebSocket 동기화 자체도 되살립니다(FR-S9, 사용자 결정 2026-10-03). 커널이 이 상태를 들고 있습니다. 매니저는 언제든 사라질 수 있고, 같은 파일에 서로 다른 매니저(로컬 임시 매니저와 전용 매니저)로 붙은 클라이언트도 같은 상태를 봐야 하기 때문입니다.

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
- 상태 메모: 커널 쪽(`presence.update`/`presence.leave`, 30초 유예)을 검증했습니다. 지금은 이벤트 스트림이 열려 있는 동안 매니저가 약 10초마다 `presence.update` 하트비트를 보냅니다(Rust `test_fr_s4_event_stream_with_client_id_heartbeats_presence`). 스트림 넘김(FR-M6) 뒤에는 커널이 넘겨받은 스트림으로 접속 여부를 직접 알므로 하트비트가 필요 없습니다. 스트림이 닫히고 30초 뒤 `presence.leave`입니다(PROTOCOL §3.7.5, 구현 대기 #47).

### FR-S5 디스크 파일과의 동기화 — `Done`
- 클라이언트 편집은 300ms 디바운스 뒤 파일에 원자적으로 저장합니다(FORMAT 직렬화, 손대지 않은 셀은 바이트 그대로).
- 바깥에서 파일이 바뀌면(에이전트의 Edit 도구, `git checkout`, 다른 편집기) 커널이 1초 안에 알아채고 다시 파싱합니다. 셀을 `cell_id` → `source_sha256` → 순서로 맞추고 `doc.reloaded` 이벤트를 보냅니다.
- 그때 잠긴 셀이 바깥에서도 바뀌었으면, 잠근 쪽의 내용을 지우지 않습니다. 그 셀을 `conflict`로 표시하고 두 버전을 모두 이벤트에 담습니다. 잠근 클라이언트가 해제하거나 다시 수정하면 충돌이 풀립니다.
- 수용 기준: 커널이 붙어 있는 동안 파일을 밖에서 고치면 모든 클라이언트에 반영됩니다. 클라이언트 편집이 파일에 저장되고, 고치지 않은 셀의 바이트는 그대로입니다.
- 테스트: `test_fr_s5_external_edit_reloads_and_locked_cell_conflicts`, `test_fr_s5_client_edit_is_saved_byte_exact`, `test_fr_s1_s2_s3_s5_s8_two_clients_edit_converge_save_and_run`

### FR-S6 실행한 사람 표시 — `Done`
`run.queued`, `run.started`, `run.finished`, `cell.*` 이벤트와 실행 기록 메타데이터에 실행을 요청한 클라이언트(`started_by`: client_id, 사용자, 기기)를 넣습니다. 인터럽트하면 `interrupted_by`도 넣습니다(2025 `execution_started.started_by`, `execution_interrupted.interrupted_by`). 셀을 실행하는 요청에는 `cell_ids`를 쓸 수 있습니다(인덱스 `cells`와 둘 중 하나). `run.started`는 `cells`와 나란한 `cell_ids`를, `cell.*`·`output`·`output.clear`는 공유 문서의 `cell_id`를 담아, 실행 중에 셀이 옮겨져도 출력이 맞는 셀로 갑니다.
- 테스트: `test_fr_s6_runs_are_attributed`, `test_fr_s6_run_and_output_events_carry_cell_ids`, `test_fr_s6_output_clear_carries_cell_id`

### FR-S7 알람 — `Agreed` (2025 알람 필드 복구, 구현 대기 #47, #49)
SSE를 계속 붙잡을 수 없는 클라이언트(모바일 백그라운드, 웹훅 대체)를 위해 롱폴링을 둡니다. `GET /kernels/{id}/runs/{run_ref}/wait?timeout=`는 그 실행이 끝나면 바로, 아니면 `timeout`(기본 60초, 최대 300초) 뒤에 돌려줍니다. 응답은 끝났을 때 실행 요약, 아직이면 `status: running`과 진행 정보이고, 다시 부를 때 쓸 `next` 정보가 들어 있습니다(2025 `executions/{cell_id}/wait`의 timeout·재폴링 모델). 실행이 끝나면 `run.finished` 이벤트가 모든 구독자에게 가므로, Ember는 이 이벤트로 휴대폰 푸시를 보냅니다.
- 2025 알람 필드(복구, 구현 대기 #49): 요청은 `client_id`와 `poll_sequence`(기본 1)를 받고, 응답은 `poll_sequence`, `next_poll_sequence`, `auto_repoll {enabled, next_poll_url, recommended_delay, max_polls}`를 담습니다. `poll_sequence`가 `max_polls`(2025 값 100)를 넘으면 `429 poll_limit_exceeded`(2025 `POLL_LIMIT_EXCEEDED`)입니다.
- 테스트: `test_fr_s7_wait_returns_on_finish_or_timeout`, 계획 `test_fr_s7_wait_carries_2025_poll_fields`
- 넘김: 이 롱폴링은 매니저가 인증한 뒤 커널에 넘깁니다(FR-M6, `kind: "wait"`). 커널이 응답과 `next` 정보를 직접 씁니다. 구현 대기(#47).
- 상태 메모: 커널의 `runs.wait`(끝나면 바로, 아니면 `timeout` 뒤)를 검증했습니다. 지금 구현에서는 HTTP 응답의 `next` 정보를 매니저가 붙이고, Rust `test_fr_s7_wait_returns_on_finish_or_timeout`(`darkpyonix/manager/crates/dpx-server/tests/collab.rs`)가 검증합니다.

### FR-S8 권한 — `Agreed` (사용자 결정 2026-10-03, 구현 대기 #49)
셀 편집(FR-S2)과 잠금(FR-S3)은 쓰기 권한(2025 `user_permission: "write"`, `viewer3`와 `admin`)이 있어야 합니다. 접속자 표시와 포커스(FR-S4)는 `viewer1`부터입니다(FR-A3). 이 검사는 매니저만 합니다. 커널은 `client.permission`을 검사하지 않고 `forbidden`을 내지 않습니다(INTENT D5, D18). 사용자(2026-10-03): "커널 매니저가 이미 권한 검사를 하고 넘겨줬는데 커널이 왜 검증해야 할게 많나".
- 수용 기준: `viewer2` 토큰의 셀 수정·잠금은 매니저가 `403`으로 거절하고 커널에 닿지 않습니다. 커널에 `permission: "viewer1"` 라벨로 `doc.cell.update`를 직접 보내도 커널은 권한 오류를 내지 않습니다.
- 테스트(계획): `test_fr_s8_write_is_checked_by_the_manager`, `test_fr_s8_kernel_does_not_check_permission`. 지금의 `test_fr_s8_edit_and_lock_need_editor`(커널 쪽 검사)는 지웁니다.

### FR-S9 2025 WebSocket 동기화 — `Agreed` (사용자 결정 2026-10-03, 구현 대기 #49)
INTENT D19, PROTOCOL §5. 사용자 결정: "원 설계대로 복구". `GET /api/ws/kernels/{kernel_id}?token=&nickname=&client_id=`(2025 `/ws/kernels/{kernel_id}?token={token}`)는 매니저가 토큰과 권한을 확인한 뒤 커널에 넘깁니다(FR-M6). 커널은 `101`로 업그레이드하고 2025 메시지를 주고받습니다.
- 받는 메시지: `request_code` → `code_data`, `request_history` → `history_data`, `request_locks` → `locks_data`, `start_typing` → 잠금과 `cell_locked`, `cell_focus` → `other_user_focus`, `cell_blur` → `other_user_blur`(2025에 송신 메시지가 없어 더함).
- 보내는 메시지: 위 응답과 `users_focus_data`(접속 직후), `cell_unlocked_with_code`, `execution_started`, `execution_output`, `execution_complete`, `execution_error`, `execution_interrupted`. 2025 이름이 없는 이벤트는 `{"type": "event", "event": …}`로 그대로 싣습니다.
- 2025 메시지의 `user_id`는 커널이 쓰지 않습니다. 누구인지는 매니저가 붙인 라벨(`client_id`, `user`, `nickname`)로 정합니다.
- 권한 라벨로 거릅니다(FR-K9). `viewer1`은 `history_data`가 빈 목록이고 `execution_output`과 출력이 담긴 필드를 받지 않습니다. `viewer1`·`viewer2`의 `start_typing`은 잠그지 않고 `{"type": "error", "error": "INSUFFICIENT_PERMISSION"}`를 돌려받습니다.
- 편집, 잠금 해제(최종 소스), 실행, 인터럽트는 2025처럼 REST(매니저)이고, 결과는 WebSocket으로 퍼집니다.
- 수용 기준: 두 클라이언트가 WebSocket으로 붙은 뒤 한쪽이 `start_typing`하면 다른 쪽이 `cell_locked`를 받고, REST로 잠금을 풀며 소스를 보내면 `cell_unlocked_with_code`를 받습니다. REST로 실행하면 두 쪽 모두 `execution_started`, `execution_output`, `execution_complete`를 받습니다. 매니저를 `SIGKILL`로 죽여도(평문 연결) 다음 실행의 메시지가 옵니다. `viewer1` 클라이언트는 출력을 받지 않습니다.
- 테스트(계획): `test_fr_s9_ws_initial_sync_code_history_locks`, `test_fr_s9_ws_typing_locks_and_unlock_with_code`, `test_fr_s9_ws_execution_broadcast`, `test_fr_s9_ws_survives_manager_kill`, `test_fr_s9_ws_viewer1_gets_no_outputs`

## 10. 허브 (H)

전송은 iroh 1.x로 정했습니다(PROJECT Q1, 2026-10-03, 조건부: ember SPEC NFR-N1을 못 맞추면 직접 구현을 검토). 그래서 허브는 직접 만든 랑데부·중계 대신 iroh가 이미 쓰는 프로토콜을 그대로 받습니다. 기기는 iroh 엔드포인트이고, 기기 ID는 그 엔드포인트 ID(ed25519 공개 키, 소문자 hex 64자)입니다.

**두 호스트(INTENT D15, 2026-10-03).** darkpyonix.dev의 DNS는 Cloudflare이고, 허브는 가벼운 부분을 Cloudflare Workers에서 돌립니다(사용자 결정). Workers와 Containers는 들어오는 UDP를 받지 못하므로 iroh 릴레이만 따로 둡니다.

| 호스트 | 구현 | 맡는 일 |
|---|---|---|
| `https://darkpyonix.dev` | Cloudflare Worker `hub/worker/` (TypeScript, D1, 정적 자산, Cron) | GitHub 로그인(FR-H6), 기기 등록(FR-H1), 주소 디렉터리(FR-H2), 릴레이 입장 판정 API(FR-H3), 공유와 ash 호스팅(FR-H4), 이름과 ACME TXT(FR-H5), Flathub 검증 파일(FR-H7), 설정 발견(FR-H8) |
| `https://relay.darkpyonix.dev` | 릴레이 호스트 `hub/server/` (Rust, `iroh-relay` 서버 크레이트) | iroh 릴레이 `/relay`, `/ping`, `/generate_204`, UDP 7842의 QUIC 주소 발견(QAD). 누구를 들일지는 Worker에 묻습니다 |

- Worker는 apex(`darkpyonix.dev`)에만 붙습니다(custom domain). `relay.darkpyonix.dev`는 Cloudflare 프록시를 끈(DNS only) A/AAAA 레코드로 릴레이 호스트를 가리킵니다. 프록시는 UDP 7842를 넘기지 않고, QAD는 릴레이 호스트 자신의 TLS 인증서를 쓰기 때문입니다.
- Worker의 부하: iroh `PkarrPublisher`는 5분마다(그리고 주소가 바뀔 때) 다시 올립니다. 기기 10대면 하루 약 3,000번의 `PUT /pkarr`와 그만큼의 D1 쓰기 두 번이고, 요청마다 ed25519 검증 한 번과 D1 질의 몇 개입니다. Workers 무료 한도(하루 10만 요청, D1 쓰기 10만)의 몇 % 수준이라 "가볍다"는 조건을 만족합니다. 운영은 CPU 한도 여유를 위해 Workers Paid를 권합니다.
- 언어는 TypeScript입니다. 근거는 INTENT D15에 있습니다.

계약은 [api/hub.openapi.yaml](api/hub.openapi.yaml) 하나이고, 릴레이 호스트가 답하는 연산은 경로 단위 `servers: relay.darkpyonix.dev`로 표시합니다. Worker 테스트가 Worker의 모든 연산이 문서의 상태 코드로만 답하고 Worker의 라우트와 문서의 연산이 정확히 같음을 확인합니다(`test_hub_every_operation_answers_with_a_documented_status`, `test_hub_every_worker_route_is_documented_and_vice_versa`). 릴레이 호스트의 같은 이름 테스트(`hub/server/tests/hub/openapi.rs`)는 `servers`가 붙은 연산만 확인합니다.

전송 계층 교체 가능성: 허브가 iroh에 묶이는 곳은 릴레이 호스트와 주소 레코드 형식(pkarr 서명 패킷)뿐입니다. 계정, 기기 등록, 공유, 이름은 "ed25519 공개 키 하나 = 기기"라는 가정만 씁니다. 직접 구현으로 바꾸면 그 두 곳만 바꿉니다.

**인증 모델.** 계정은 GitHub 사용자입니다(FR-H6). 사람은 브라우저에서 GitHub로 로그인해 세션 쿠키(`__Host-dp_session`)를 받고, 기기는 기기 링크(FR-H1)로 계정에 들어와 기기 토큰을 받습니다. "계정 권한"은 로그인한 세션 또는 그 계정의 `main_server` 기기 토큰입니다. 단, `main_server` 역할을 들이는 승인(메인 서버 바꾸기 포함, FR-H1)은 세션만 할 수 있습니다. 토큰과 세션 ID는 SHA-256 해시로만 저장하고, GitHub 액세스 토큰은 사용자 정보를 한 번 읽은 뒤 바로 폐기(revoke)하며 저장하지 않습니다. 쿠키로 인증한 쓰기 요청은 `Origin`이 `https://darkpyonix.dev`가 아니면 403입니다. 자격 증명 오류(401)의 본문은 `{"error": <설명>, "code": <코드>}`이고, `code`는 `device_removed`(지운 기기의 토큰. 다시 시도해도 소용없으니 기기는 토큰을 버리고 사용자에게 알립니다) 또는 `invalid_credentials`(없거나 모르는 토큰·세션)입니다. 상태 코드는 둘 다 401입니다. 410은 자원이 사라졌다는 뜻이지 자격 증명이 틀렸다는 뜻이 아니고, 401을 유지하면 "401이면 다시 인증"하는 기존 클라이언트가 그대로 동작합니다(FR-H1). OpenAI 로그인("Sign in with ChatGPT")과 ChatGPT 플랜 사용은 허브 기능이 아니고, 사용자가 직접 띄운 ember server가 합니다(PROJECT Q2).

### FR-H1 기기 등록 — `Agreed`
기기는 iroh 엔드포인트 ID로 계정에 들어옵니다. 흐름은 OAuth 기기 인증(RFC 8628) 모양에 키 소유 증명을 더한 **기기 링크**입니다.
1. 기기가 `POST /device-links {endpoint_id, name, role}`로 요청하고 `link_id`, 사용자 코드(`BCDF-GHJK` 형식, 모음 없는 20글자), 챌린지, 만료(15분)를 받습니다.
2. 사람이 `https://darkpyonix.dev/link?code=<사용자 코드>`를 열어 GitHub로 로그인한 상태에서 기기 이름·역할·엔드포인트 ID를 확인하고 승인하거나 거절합니다(`POST /link-codes/{user_code} {"approve": bool}`). 같은 계정의 메인 서버도 기기 토큰으로 승인할 수 있어서, 새 컴퓨터는 브라우저 없이 메인 서버(ember server)를 거쳐 들어올 수 있습니다. `computer` 기기 토큰으로는 승인할 수 없습니다(403). **`main_server` 역할을 요청한 링크는 로그인한 브라우저 세션만 승인할 수 있습니다.** 메인 서버의 기기 토큰으로 그런 링크를 승인하면 403이고(거절은 됩니다), 그래서 새어 나간 메인 서버 토큰 하나로 계정 권한을 가진 기기를 더 만들 수 없습니다. 사용자 코드는 짧으므로 코드 조회와 결정은 계정당 분당 30번으로 제한합니다(429). 링크 요청도 주소당 분당 30번입니다.
3. 기기는 `interval`마다 `POST /device-links/{link_id}/token`을 부르며, 매번 `darkpyonix-hub/v2/link\n<link_id>\n<challenge>`에 대한 자기 키 서명을 냅니다. 결정 전에는 202, 승인되면 한 번만 201과 기기 토큰, 거절되면 403입니다.
4. 다시 시작한 기기는 `GET /device-links/{link_id}`로 링크 상태(`pending`, `approved`, `denied`, `claimed`, `expired`)와 사용자 코드, 챌린지, 만료를 다시 읽습니다. 자격 증명은 필요 없습니다. `link_id`는 128비트 난수라 기기만 알고, 챌린지는 비밀이 아니며(서명에는 기기 키가 필요), 토큰은 이 응답에 없습니다. 만료된 링크는 시간마다 지워지므로 그 뒤에는 404입니다. `claimed`인데 기기가 토큰을 잃었다면 그 키는 지우고 다시 들입니다(FR-H11).

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
- 메인 서버가 있는 계정에서 `main_server` 링크를 `{"approve": true}`로만 승인하면 409이고 `code`는 `main_server_exists`입니다. 승인하려면 **바꾸기**를 분명히 적습니다: `{"approve": true, "replace": "<지금 메인 서버의 endpoint_id>"}`. `replace`가 지금 메인 서버가 아니면(다른 기기이거나 메인 서버가 없으면) 409 `replace_mismatch`입니다. `replace`는 `main_server` 링크를 승인할 때만 쓰고, 거절이나 다른 역할의 링크에 붙이면 400입니다. 승인 화면이 무엇을 바꾸는지 보여 줄 수 있도록 `GET /link-codes/{user_code}`는 `main_server` 링크에 대해 계정의 지금 메인 서버(`current_main_server: {endpoint_id, name}`, 없거나 다른 역할의 링크면 `null`)를 함께 줍니다.
- 바꾸기는 새 기기가 토큰을 받을 때(3단계) 한 트랜잭션으로 일어납니다: 옛 메인 서버를 지우고(그 토큰은 401 `device_removed`, 지운 기기 목록에 나옴, 공유·주소 레코드는 지우고 릴레이 연결을 끊음), **옛 메인 서버의 이름(FR-H5)을 새 메인 서버로 옮기고**, 새 기기를 등록합니다. 이름을 옮기는 이유: 사용자의 주소(`https://<name>.darkpyonix.dev`)가 기계를 바꿔도 그대로 통해야 하기 때문입니다. 옛 메인 서버가 게시한 ACME TXT는 지우고, 새 메인 서버가 자기 키로 인증서를 다시 받습니다.
- 승인 뒤 받기 전에 계정의 메인 서버가 바뀌었으면(그사이 다른 `main_server` 링크가 먼저 받음) 받기는 409 `main_server_exists`이고 그 링크는 거절 상태가 됩니다. 계정당 하나는 D1의 부분 유일 인덱스(`role = 'main_server' AND revoked_at IS NULL`)가 보장합니다.
- 메인 서버를 지우면 계정에 메인 서버가 없어지고, 그 뒤의 `main_server` 링크는 `replace` 없이 승인합니다. 지운 메인 서버를 다시 들일 때도(FR-H11) 같은 규칙입니다.

기기는 계정 하나에만 속하고, 기기 목록과 조회는 같은 계정 안에서만 보입니다. 기기 이름은 `PATCH /devices/{endpoint_id} {"name": …}`로 바꿉니다(1~64자). 계정 권한이나 그 기기 자신만 바꿀 수 있고, 다른 `computer`/`client` 토큰은 403입니다. 기기를 지우면(위 표의 권한: 세션, `computer`·`client`를 지우는 메인 서버 토큰, 또는 그 기기 자신의 기기 토큰: 앱을 지우거나 계정에서 나갈 때 기기가 스스로 나갑니다) 그 키는 폐기되어 계정 주인이 다시 들이기 전에는(FR-H11) 다시 등록할 수 없고, 그 기기의 이름·공유·주소 레코드가 지워지며, Worker가 릴레이 호스트에 연결을 끊으라고 알립니다(`POST /admin/disconnect`, FR-H3).
- 수용 기준: 두 엔드포인트가 기기 링크로 등록되면 계정의 기기 목록에 두 엔드포인트 ID가 나옵니다. 다른 키의 서명이나 다른 메시지의 서명은 400입니다. 승인 전 폴링은 202, 거절된 링크는 403, 한 번 받은 링크를 다시 받으면 404입니다. 링크 상태 조회는 대기·승인·받음·거절·만료를 그대로 보여 주고, 모르는 링크는 404입니다. 메인 서버 토큰은 `computer` 링크를 승인하지만 `main_server` 링크의 승인은 403이고, 같은 링크를 세션은 승인합니다. 이미 등록되었거나 지운 키의 링크 요청은 409입니다. 기기 이름은 세션과 그 기기 자신이 바꾸고 다른 `computer` 토큰은 403, 빈 이름은 400입니다. `client`로 들어온 기기는 목록·주소 게시와 조회가 되고, 공유 게시·이름 예약·승인은 403입니다. 다른 계정에서는 그 기기가 보이지 않습니다(404). `computer` 기기 토큰으로 다른 기기를 지우면 403이고 자기 자신은 지울 수 있습니다(204). 메인 서버 토큰으로 `computer`를 지우고 자기 자신도 지울 수 있습니다(204). 메인 서버가 있는 계정에서 `replace` 없는 `main_server` 승인은 409 `main_server_exists`, 지금 메인 서버가 아닌 `replace`는 409 `replace_mismatch`, 거절이나 `computer` 링크에 붙인 `replace`는 400이고, 링크 코드 조회는 지금 메인 서버를 `current_main_server`로 보여 줍니다. `replace`로 승인하고 받으면 기기 목록의 `main_server`는 새 기기 하나이고, 옛 메인 서버의 토큰은 401 `device_removed`이며 지운 기기 목록에 `main_server`로 나오고, 그 이름은 새 메인 서버의 것이 되며 옛 ACME TXT와 공유는 지워지고 릴레이에 끊기를 알립니다. 메인 서버가 없을 때 승인된 `main_server` 링크 둘 중 먼저 받은 것은 201, 나중 것은 409 `main_server_exists`이고 그 링크는 `denied`입니다. 지운 기기의 토큰은 401이고 `code`가 `device_removed`이며, 모르는 토큰은 401에 `invalid_credentials`입니다. 실제 iroh 엔드포인트(Rust `SecretKey::sign`)의 서명이 받아들여지는 것은 ember 전송 크레이트 연동 시험에서 확인합니다.
- 테스트(`hub/worker/test/devices.test.ts`): `test_fr_h1_register_two_iroh_endpoints`, `test_fr_h1_link_shows_code_and_polls_pending_until_approved`, `test_fr_h1_registration_requires_key_possession`, `test_fr_h1_denied_link_is_refused`, `test_fr_h1_restarted_device_reads_its_link_status`, `test_fr_h1_main_server_approves_computers_but_a_computer_cannot`, `test_fr_h1_only_a_session_approves_a_main_server_link`, `test_fr_h1_devices_are_scoped_to_their_account`, `test_fr_h1_removed_device_is_revoked`, `test_fr_h1_removed_device_token_is_told_apart_from_a_bad_token`, `test_fr_h1_a_device_removes_itself_but_not_others`, `test_fr_h1_only_a_session_removes_another_main_server`, `test_fr_h1_client_role_joins_and_connects_but_cannot_share_or_name`, `test_fr_h1_rename_by_the_device_or_the_account`, `test_fr_h1_link_request_is_validated`

### FR-H2 주소 디렉터리와 발견 — `Agreed`
기기는 현재 iroh 주소(릴레이 URL과 직접 주소)를 자기 키로 서명한 pkarr 패킷으로 허브에 올리고, 같은 계정의 기기는 엔드포인트 ID만으로 서로의 주소를 찾습니다.
- 프로토콜: iroh의 pkarr 릴레이 HTTP 프로토콜을 그대로 씁니다. `PUT /pkarr/<z32 키>`로 올리고 `GET /pkarr/<z32 키>`로 받습니다. 본문은 `서명(64) || 타임스탬프 µs 빅엔디언(8) || DNS 패킷(최대 1000바이트)`이고, 서명 대상은 BEP 44 형식 `3:seqi<ts>e1:v<len>:<dns>`입니다(iroh-dns 1.3 `SignedPacket`). Worker는 이 형식과 DNS 응답 파싱(이름 압축 포함), `_iroh` TXT의 `relay=`/`addr=` 속성 해석을 TypeScript로 다시 구현합니다. 그래서 iroh의 기본 `PkarrPublisher`·`PkarrResolver`를 `https://darkpyonix.dev/pkarr?token=<조회 토큰>`에 그대로 붙일 수 있습니다. 쿼리 문자열에는 기기 토큰이 아니라 조회 전용 토큰만 넣습니다(NFR-H2). 같은 내용을 JSON으로 보는 `GET /devices/{endpoint_id}/addresses`도 둡니다.
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
- 입장 정책: 릴레이 핸드셰이크가 증명한 엔드포인트 ID와 클라이언트가 낸 인증 토큰(있으면)을 릴레이 호스트가 `POST https://darkpyonix.dev/internal/relay/admit`로 묻습니다(공유 비밀 `RELAY_SHARED_SECRET`). 폐기되지 않은 등록 기기면 허용(`cache_secs` 60초 동안 새 연결에 재사용 가능), 유효한 손님 통행권(FR-H4가 발급, 그 공유가 아직 게시 중)이 있으면 허용(캐시 안 함), 그 밖에는 거절입니다. 릴레이 호스트는 엔드포인트의 첫 연결이 열리고 마지막 연결이 닫힐 때 `POST /internal/relay/presence`로 알려 기기 목록의 `online`을 갱신합니다. 기기를 지우면 Worker가 `POST https://relay.darkpyonix.dev/admin/disconnect`로 끊습니다.
- 릴레이 호스트 상태: `hub/server`는 지금 D15 이전 구현(API·SQLite 포함)이고, 릴레이 전용으로 줄이는 작업(위 입장 API 사용, `/admin/disconnect` 추가, API·DB 제거)은 빌드가 필요한 별도 변경입니다(`hub/server/src/lib.rs` 머리 주석).
- 수용 기준: 두 등록 기기가 IP 전송을 끈 릴레이 전용 모드로 우리 릴레이를 거쳐 연결하고 데이터를 주고받습니다(선택된 경로가 릴레이). 같은 두 기기가 루프백에서 직접 경로로도 연결합니다. 등록되지 않은 엔드포인트는 릴레이가 거절해 연결하지 못하고, 지운 기기의 연결은 끊깁니다. Worker 쪽: 등록 기기는 허용, 지운 기기와 통행권 없는 엔드포인트는 거절, 비밀이 틀리면 401, presence가 `online`을 바꿉니다. 루프백 처리량과 왕복 지연, NFR-N1의 QAD 켬/끔 직접 연결 비율을 측정해 여기에 적습니다.
- 테스트: Worker `test_fr_h3_relay_admits_registered_and_refuses_removed_devices`, `test_fr_h3_relay_callbacks_need_the_shared_secret`, `test_fr_h3_presence_marks_devices_online`(`hub/worker/test/shares.test.ts`). 릴레이 호스트 `test_fr_h3_relay_only_connection_through_hub`, `test_fr_h3_direct_connection_on_loopback`, `test_fr_h3_relay_rejects_unregistered_endpoint`, `test_fr_h3_relay_throughput_and_latency`(지금은 D15 이전 구현 기준, 릴레이 축소 때 스텁 입장 API로 바꿈)
- 측정 기록: (구현 후 기입)

### FR-H4 ash 호스팅과 공유 링크 — `Agreed`
`https://darkpyonix.dev/ash/`에서 공식 ash 뷰어를 Workers 정적 자산으로 호스팅하고(`hub/worker/public/ash/`에 darkpyonix-ash 빌드 결과를 넣어 배포), 공유 링크 `https://darkpyonix.dev/s/<share_id>#<token>`을 그 공유를 연 기기로 이어 줍니다. 공유 토큰은 URL 조각(`#` 뒤)에 있어서 허브로 가지 않습니다. 권한 검사는 끝단의 전용 매니저가 합니다(FR-A3).
- 기기는 `POST /shares`로 자기 공유를 게시하고, 누구나 `GET /shares/{share_id}`로 그 공유를 연 기기의 엔드포인트 ID와 릴레이 URL(기기가 올린 홈 릴레이, 없으면 `https://relay.darkpyonix.dev/`), 10분짜리 손님 릴레이 통행권을 받습니다. ash(브라우저 iroh, 릴레이 전용)는 그 통행권으로 릴레이에 붙어 기기에 연결합니다. `GET /s/{share_id}`는 ash 뷰어 페이지를 냅니다(뷰어가 배포되기 전까지는 자리표시 페이지). 공유를 내리면 그 공유의 통행권도 더는 통하지 않습니다.
- 수용 기준: 게시한 공유가 기기 ID와 통행권으로 풀리고, 기기가 주소를 올린 뒤에는 그 홈 릴레이 URL로 풀립니다. 다른 기기가 같은 공유 ID를 게시하면 409입니다. 그 통행권으로 미등록 엔드포인트의 릴레이 입장이 허용되고, 통행권이 없거나 위조이거나 공유를 내린 뒤면 거절됩니다. 게시를 지우면 404입니다. `/s/{share_id}`와 `/ash/`가 HTML을 냅니다. 브라우저 ash가 실제로 릴레이를 거쳐 기기에 붙는 것은 릴레이 호스트 연동 시험으로 확인합니다.
- 테스트(`hub/worker/test/shares.test.ts`): `test_fr_h4_share_resolves_to_hosting_device`, `test_fr_h4_share_ids_belong_to_one_device`, `test_fr_h4_guest_pass_admits_an_unregistered_endpoint_at_the_relay`, `test_fr_h4_viewer_pages_are_served`

### FR-H5 HTTPS 이름 — `Agreed`
메인 서버가 `https://<name>.darkpyonix.dev` 주소와 공인 인증서를 얻게 합니다(모바일 웹뷰의 보안 컨텍스트 요건, ember FR-N4).
- 방식 비교:
  - (A) **ACME DNS-01을 허브가 대신 게시.** 메인 서버가 자기 개인 키로 인증서를 받고, 허브는 `_acme-challenge.<name>.darkpyonix.dev` TXT만 게시합니다. TLS가 메인 서버에서 끝나므로 허브는 평문을 보지 않습니다(NFR-H1 유지). 대신 공인 IP가 없는 기기에 브라우저가 직접 닿지 못하므로, ember 앱이 루프백 포워더(127.0.0.1 → iroh)로 그 이름을 열어야 합니다.
  - (B) **허브가 TLS를 끝내는 HTTPS 엣지.** 아무 브라우저나 닿지만 허브가 평문을 봅니다. NFR-H1을 깨므로 쓰지 않습니다.
  - (C) **SNI 패스스루 엣지.** 허브가 ClientHello의 SNI만 읽고 TLS 바이트를 그대로 iroh로 기기에 넘깁니다. 앱 없는 브라우저에서도 닿지만 공개 트래픽 대역폭이 허브에 걸리고, Workers로는 할 수 없어(TCP 패스스루) 릴레이 호스트나 Spectrum이 필요합니다.
- 결정: (A)를 씁니다. (C)는 앱 없는 브라우저 접근이 필요해지면 따로 다룹니다. (B)는 쓰지 않습니다. 허브는 이름을 메인 서버 기기에 예약하고(`PUT /names/{name}`), 그 기기가 요청한 TXT 값을 **Cloudflare DNS API**로 게시합니다(`PUT /names/{name}/acme-challenge`). 기존 값 삭제와 새 값 생성은 `POST /zones/{zone_id}/dns_records/batch` 한 번이라 원자적이고, TTL은 60초입니다. API 토큰은 darkpyonix.dev 존 하나의 `Zone → DNS → Edit`만 가집니다. DNS 공급자는 `DnsProvider` 인터페이스 뒤에 있습니다. 이름을 놓거나 기기를 지우면 그 TXT도 지웁니다. 메인 서버를 바꾸면(FR-H1) 옛 메인 서버의 이름은 지우지 않고 새 메인 서버로 옮겨 가며, TXT만 지웁니다(사용자의 주소가 기계를 바꿔도 그대로 통하도록). 예약어(`www`, `api`, `relay`, `ash`, `hub`, `dns`, `ns1`, `ns2`, `mail`, `admin`, `docs`, `status`, `auth`, `link`, `qad`)는 받지 않습니다.
- 수용 기준: 메인 서버가 이름을 예약하면 201, 같은 기기가 다시 하면 200, 다른 기기는 409, `computer`는 403, 형식이 틀리거나 예약어면 400입니다. TXT 값 1~4개(각 43자 base64url)를 게시하고 지울 수 있고, 다른 값은 400, 공급자가 거절하면 502입니다. 메인 서버를 바꾸면 이름 목록의 그 이름이 새 메인 서버를 가리키고, 새 메인 서버는 그 이름의 TXT를 게시하며 옛 메인 서버는 401입니다. Cloudflare 클라이언트는 기존 레코드를 조회한 뒤 삭제와 생성을 한 batch로 보냅니다. 실제 존에서 Let's Encrypt 스테이징 인증서를 받는 것은 배포 후 확인합니다.
- 테스트(`hub/worker/test/names.test.ts`): `test_fr_h5_name_reservation_and_acme_txt`, `test_fr_h5_only_main_servers_hold_names_and_names_are_unique`, `test_fr_h5_bad_values_and_provider_failures`, `test_fr_h5_release_and_device_removal_clear_records`, `test_fr_h5_cloudflare_replaces_txt_in_one_batch`, `test_fr_h5_cloudflare_clear_and_errors`

### FR-H6 GitHub 로그인 — `Agreed`
허브 계정은 GitHub 로그인으로 만듭니다(사용자 결정, 2026-10-03: "OpenAI 로그인은 엠버 서버에서 사용자가 자체적으로 하는걸로 하고 허브는 깃허브 로그인으로 하자."). 계정의 정체는 GitHub 사용자의 숫자 ID(바뀌지 않고 재사용되지 않음)이고, 로그인 이름은 표시용으로만 저장합니다.
- 흐름: GitHub OAuth App, 인가 코드 + PKCE(S256) + state. `GET /auth/login`이 무작위 `state`와 PKCE 검증자를 D1에 10분짜리 일회용 거래로 남기고, `state`를 `__Host-dp_oauth` 쿠키에도 묶은 뒤 `https://github.com/login/oauth/authorize`로 보냅니다. 범위(scope)는 요청하지 않습니다(공개 프로필만 읽음). `GET /auth/callback`은 쿠키의 `state`와 같고 아직 쓰지 않은 거래인지 확인하고, 코드를 검증자와 함께 `https://github.com/login/oauth/access_token`에서 바꾸고, `GET https://api.github.com/user`로 `id`와 `login`을 읽은 뒤 그 GitHub 토큰을 폐기합니다. 그 GitHub ID의 계정을 찾거나 만들고, 30일짜리 세션 쿠키(`__Host-dp_session`, HttpOnly, Secure, SameSite=Lax)를 줍니다. `return_to`는 같은 출처의 경로만 받습니다.
- 운영자 선택 사항: `GITHUB_ALLOWED_IDS`(쉼표로 구분한 GitHub 사용자 ID)를 두면 그 사람들만 새 계정을 만들 수 있습니다(비우면 누구나).
- OpenAI / Sign in with ChatGPT는 허브에 넣지 않습니다. 사용자의 ChatGPT 플랜 사용은 사용자가 직접 띄운 ember server가 맡습니다(PROJECT Q2).
- 수용 기준: 로그인 시작이 `client_id`, 콜백 URL, `state`, S256 `code_challenge`를 담아 GitHub로 보내고 같은 `state`를 쿠키로 둡니다. 같은 GitHub ID로 두 번 로그인하면 같은 계정이고 로그인 이름만 갱신되며, 다른 ID는 다른 계정입니다. GitHub 토큰은 폐기되고 저장되지 않습니다. 다른 브라우저의 `state`, 다시 쓴 `state`, 틀린 PKCE 검증자는 400입니다. 허용 목록 밖의 새 사용자는 403입니다. 밖으로 나가는 `return_to`는 `/`가 됩니다. 로그아웃 뒤 세션은 401입니다. 다른 출처의 쿠키 쓰기는 403입니다. 실제 GitHub OAuth App으로 로그인되는 것은 배포 후 확인합니다.
- 테스트(`hub/worker/test/github.test.ts`, 가짜 GitHub): `test_fr_h6_login_redirects_to_github_with_pkce_and_state`, `test_fr_h6_callback_creates_one_account_per_github_user`, `test_fr_h6_github_token_is_revoked_and_not_stored`, `test_fr_h6_callback_rejects_state_from_another_browser`, `test_fr_h6_state_is_single_use`, `test_fr_h6_wrong_pkce_verifier_is_refused_by_the_provider`, `test_fr_h6_allowlist_limits_new_accounts`, `test_fr_h6_return_to_stays_on_this_origin`, `test_fr_h6_logout_ends_the_session`, `test_fr_h6_session_writes_need_our_origin`

### FR-H7 Flathub 앱 검증 — `Agreed`
Flathub의 앱 ID `dev.darkpyonix.Ember`는 도메인 darkpyonix.dev로 검증합니다. Flathub가 주는 토큰을 `https://darkpyonix.dev/.well-known/org.flathub.VerifiedApps.txt`에 평문으로 둡니다. 내용은 Worker 변수 또는 비밀값 `FLATHUB_VERIFICATION_TOKEN`에서 오므로 저장소에 토큰을 넣지 않습니다. 비어 있거나 없으면 404입니다.
- 수용 기준: 토큰이 있으면 200 `text/plain`이고 본문은 앞뒤 공백을 뺀 토큰입니다. 없거나 공백뿐이면 404입니다. 실제 Flathub 검증은 배포 후 확인합니다.
- 테스트(`hub/worker/test/flathub.test.ts`): `test_fr_h7_verified_apps_file_serves_the_configured_token`, `test_fr_h7_verified_apps_file_is_absent_without_a_token`

### FR-H8 허브 설정 발견 — `Agreed`
클라이언트(ember, ash)가 릴레이 주소나 pkarr URL을 코드에 박아 두지 않도록, 허브가 자기 설정을 공개합니다(Ember FR-N2 연동 중 보고, 2026-10-03).
- `GET /config`(인증 없음, `Cache-Control: public, max-age=300`)는 `{hub_version, relay_urls, pkarr_url, link_url}`를 냅니다. 판 번호(`api_version`)는 두지 않습니다(INTENT D16). `hub_version`은 배포된 빌드를 알리는 정보용 문자열이고 클라이언트는 이 값으로 분기하지 않습니다. `relay_urls`는 기기가 iroh `RelayMap`에 넣을 릴레이 목록(지금은 Worker 변수 `RELAY_URL` 하나), `pkarr_url`은 iroh `PkarrPublisher`/`PkarrResolver`에 줄 기준 URL(`<PUBLIC_URL>/pkarr`, 조회할 때는 `?token=<조회 토큰>`을 붙임, NFR-H2), `link_url`은 기기 링크 승인 페이지입니다.
- 수용 기준: 인증 없이 200이고, 값이 Worker 변수(`PUBLIC_URL`, `RELAY_URL`)를 따릅니다. 응답에 `api_version`이 없습니다.
- 테스트(`hub/worker/test/config.test.ts`): `test_fr_h8_config_names_relays_and_pkarr_url`, `test_fr_h8_config_follows_the_worker_vars`

### FR-H9 기기 목록 변경 알림 — `Agreed` [provisional]
Ember는 기기 목록을 60초마다 다시 읽었고, 그래서 "지운 기기는 하트비트 한 번 안에 끊긴다"(ember FR-N3)를 맞출 수 없었습니다(Ember FR-N2 연동 중 보고, 2026-10-03). 허브가 목록이 바뀐 것을 알려 줍니다.
- **판(version)과 ETag.** 계정마다 기기 목록의 판 번호를 두고, 목록에 보이는 것이 바뀔 때마다 하나 올립니다: 기기 추가·되살림(FR-H11), 삭제, 이름·앱 정보 변경, `online` 변화. `last_seen`만 바뀌는 것(주소 게시, 릴레이 입장)은 판을 올리지 않습니다(5분마다 오는 게시가 모든 대기자를 깨우지 않도록). `GET /devices`는 `ETag: W/"v<판>"`을 붙입니다(약한 ETag: `last_seen`은 판에 들지 않음).
- **조건부 요청.** `If-None-Match`가 지금 ETag와 같으면 304(본문 없음)입니다.
- **롱 폴링.** `?wait=<초>`(0~25)를 함께 주면, ETag가 같을 때 바로 304를 내지 않고 판이 바뀌거나 `wait`초가 지날 때까지 기다립니다. 바뀌면 200과 새 목록·새 ETag, 시간이 다 되면 304입니다. Worker는 기다리는 동안 2초마다 그 계정의 판 한 행만 읽습니다. 그래서 변경은 최대 약 2초 뒤에 전해지고, 대기 중인 클라이언트 하나는 25초에 D1 읽기 14번 정도(하루 약 4만8천 번)를 씁니다. 판이 바뀐 뒤에는 자격 증명을 다시 확인하므로, 기다리던 기기 자신이 지워졌으면 401 `device_removed`가 옵니다. 25초 상한은 프록시·모바일 망이 유휴 연결을 끊는 시간보다 짧게 둔 값입니다.
- **SSE가 아니라 롱 폴링인 이유.** Durable Object 없이 Worker는 다른 요청이 한 쓰기를 밀어 받을 수 없으므로, SSE로 해도 연결 안에서 똑같이 D1을 주기적으로 읽어야 합니다. 그러면 SSE는 연결을 더 오래 잡고(Worker 동시 연결, 모바일 배터리), 중간 프록시의 버퍼링 문제가 생기며, 다시 붙을 때의 상태 맞추기를 따로 정해야 합니다. 롱 폴링+ETag는 보통 HTTP 클라이언트로 되고, 끊겨도 마지막 ETag로 이어서 묻기만 하면 되며, `wait` 없이 쓰면 값싼 조건부 폴링이 됩니다. 계약은 그대로 두고 나중에 Durable Object로 대기자를 즉시 깨우게 바꿀 수 있습니다. 실제 부하와 Ember 사용으로 확정할 때까지 `[provisional]`입니다.
- 수용 기준: 응답에 ETag가 있고, 같은 ETag의 `If-None-Match`는 304, 다른 ETag는 바로 200입니다. 기다리는 중에 기기를 지우거나 이름을 바꾸면 200과 새 ETag가 오고, 아무 일 없으면 `wait` 뒤 304입니다. 기다리던 기기가 지워지면 401 `device_removed`입니다. 주소 게시는 ETag를 바꾸지 않고, `online` 변화는 바꿉니다. `wait`가 범위 밖이면 400입니다.
- 테스트(`hub/worker/test/notify.test.ts`): `test_fr_h9_device_list_has_an_etag_and_answers_304`, `test_fr_h9_long_poll_wakes_on_a_change`, `test_fr_h9_long_poll_times_out_with_304`, `test_fr_h9_waiting_device_that_is_removed_gets_device_removed`, `test_fr_h9_only_visible_changes_move_the_etag`, `test_fr_h9_wait_is_validated`

### FR-H10 기기 앱 정보 — `Agreed` [provisional]
기기 목록만으로 "어느 기기가 ember 노드이고 무슨 버전이며 무엇을 제공하는지" 알 수 있게, 기기가 자기 앱 정보를 허브에 적습니다(Ember FR-N2 연동 중 보고, 2026-10-03).
- 기기는 `PATCH /devices/{자기 endpoint_id} {"app": {...}}`로 적고 `{"app": null}`로 지웁니다. **그 기기 자신만** 적을 수 있습니다(세션이나 메인 서버가 남의 `app`을 적으면 403). 기기 목록과 조회의 `Device.app`에 그대로 나옵니다(없으면 `null`).
- 형식(엄격, 알 수 없는 필드는 400): `kind`(필수, `^[a-z][a-z0-9-]{0,31}$`, 예: `ember`), `version`(필수, `^[0-9A-Za-z][0-9A-Za-z.+-]{0,31}$`), `services`(선택, 기본 `[]`, 같은 형식의 이름 최대 16개, 중복 없음, 예: `["kernel-manager", "ash-host"]`).
- 허브는 이 값을 표시와 힌트로만 씁니다. 기기가 스스로 말한 것이라 권한 판단에 쓰지 않고, 클라이언트도 연결 상대를 고르는 힌트로만 씁니다(상대 인증은 iroh 키가 함). 형식을 좁게 둔 이유: 계정의 모든 기기에 그대로 보이는 값이므로 크기와 문자 집합을 묶어 두고, 자유 형식 필드가 필요해지면 그때 넓힙니다. Ember의 실제 사용으로 확정할 때까지 `[provisional]`입니다.
- 수용 기준: 기기가 적은 앱 정보가 같은 계정의 목록과 조회에 나오고, `null`로 지워집니다. 남이 적으면 403, 형식이 틀리거나 알 수 없는 필드면 400입니다.
- 테스트(`hub/worker/test/devices.test.ts`): `test_fr_h10_device_reports_its_app`, `test_fr_h10_only_the_device_writes_its_app`, `test_fr_h10_app_is_validated`

### FR-H11 지운 키 다시 들이기 — `Agreed` [provisional]
지운 키는 다시 등록할 수 없으므로(FR-H1), 실수로 지운 메인 서버는 새 키(새 엔드포인트 ID)를 만들어야 하고 그 키를 아는 모든 곳을 고쳐야 합니다. 그래서 **계정 주인만** 지운 키를 다시 들일 수 있게 합니다(Ember FR-N2 연동 중 보고, 2026-10-03).
- 흐름: (1) 로그인한 브라우저 세션이 `POST /devices/{endpoint_id}/readmit`로 자기 계정에서 지운 키를 15분 동안 다시 받아들이겠다고 표시합니다. (2) 그 기기가 보통의 기기 링크(FR-H1)를 시작합니다. 표시가 살아 있는 동안만 그 키의 링크 요청이 409가 아닙니다. (3) 그 링크의 승인도 **같은 계정의 세션만** 할 수 있습니다(메인 서버 토큰, 다른 계정은 403). (4) 기기가 받으면 같은 기기 행이 되살아나고 기기 토큰과 조회 토큰은 새로 발급됩니다. 역할과 이름은 새 링크의 것이고 앱 정보는 비웁니다. `main_server`로 돌아오는 링크에는 메인 서버 하나 규칙(FR-H1)이 그대로 걸립니다. 계정에 다른 메인 서버가 있으면 `replace`로만 승인되고, 받으면 그 메인 서버가 지워지며 이름이 되살아난 기기로 옮겨 갑니다.
- 지운 기기 목록: 다시 들일 키를 고르려면 그 엔드포인트 ID를 알아야 하므로, 로그인한 세션은 `GET /removed-devices`로 자기 계정에서 지운 기기를 봅니다. 항목마다 `endpoint_id`, 지울 때의 `name`과 `role`, `created_at`, `removed_at`, 그리고 다시 들이기 표시가 살아 있으면 그 만료(`readmit_until`, 아니면 `null`)가 나옵니다. 최근에 지운 것부터 100개까지입니다. 다시 들여 받은 기기는 이 목록에서 빠지고 기기 목록(`GET /devices`)에 나옵니다. 기기 토큰(메인 서버 것도)은 403입니다. 다시 들이기 자체가 세션만의 일이므로 목록도 세션에만 보입니다. 지운 기기의 이름과 역할은 계정 바깥에 드러나지 않습니다.
- 지울 때 이미 없어진 것(이름, 공유, 주소 레코드, 옛 토큰)은 돌아오지 않습니다. 옛 기기 토큰은 그 뒤 `invalid_credentials`입니다.
- 설계 이유: 키가 새서 지운 경우를 생각하면 키 소유 증명만으로 돌아오게 할 수 없습니다. 그래서 두 번의 사람 확인(표시와 승인)을 세션에만 맡기고, 표시는 짧게(링크 수명과 같은 15분) 둡니다. 메인 서버 토큰을 빼는 이유는 FR-H1의 `main_server` 승인 제한과 같습니다(새어 나간 메인 서버 토큰으로 지운 기기를 되살리지 못하게). Ember의 실제 사용으로 확정할 때까지 `[provisional]`입니다.
- 수용 기준: 지운 키의 링크 요청은 409, 세션이 다시 들이기를 표시한 뒤에는 201입니다. 메인 서버 토큰의 표시와 승인은 403입니다. 받은 뒤 기기가 목록에 다시 나오고 새 토큰이 통하며 옛 토큰은 401 `invalid_credentials`입니다. 15분이 지나면 다시 409입니다. 지우지 않은 기기의 표시는 409, 다른 계정의 기기는 404입니다. 다른 메인 서버가 있을 때 `main_server`로 돌아오는 링크의 승인은 `replace` 없이 409 `main_server_exists`이고, `replace`로 승인해 받으면 되살아난 기기가 유일한 메인 서버가 되고 바뀐 메인 서버의 토큰은 401 `device_removed`이며 이름이 옮겨 갑니다. 지운 기기 목록은 세션에 지운 기기를 최근 것부터 지울 때의 이름·역할·`removed_at`과 함께 보여 주고, 다시 들이기를 표시하면 `readmit_until`이 나오며, 되살아난 기기와 다른 계정의 기기는 나오지 않습니다. 기기 토큰은 403입니다.
- 테스트(`hub/worker/test/devices.test.ts`): `test_fr_h11_owner_readmits_a_removed_key`, `test_fr_h11_readmission_expires`, `test_fr_h11_readmit_needs_a_removed_device_of_the_account`, `test_fr_h11_owner_lists_removed_devices`, `test_fr_h11_only_a_session_lists_removed_devices`

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

### NFR-K4 시작 시간 — `Agreed` (재측정 대기 #50)
커널 시작(프로세스 실행부터 announce까지)은 기준 기계(맥미니 M 시리즈)에서 300 ms 이하입니다. 측정점은 프로세스를 실행한 순간부터 멀티캐스트 그룹의 리스너가 그 커널의 첫 announce를 받은 순간까지입니다.
- 상태 메모 (2026-10-03): 아래 기록은 등록 파일이 생긴 순간을 측정점으로 썼습니다. 등록 파일을 지우므로(FR-D2) 첫 announce 수신 시각으로 다시 재야 `Done`입니다.
- 측정 기록 (2026-10-03, 맥미니 M 시리즈, 부하 평균 약 4): `bootstrap_command`로 프로세스를 실행한 순간부터 등록 파일이 생길 때까지(등록 파일은 첫 멀티캐스트 announce 직전에 씁니다, `discovery.announce_now`) 7회 중앙값이 3.8 38 ms, 3.9 63 ms, 3.11 54 ms, 3.12 39 ms, 3.13 56 ms, 3.14 46 ms입니다. 같은 인터프리터의 `python -c pass`는 13~22 ms였습니다. 측정 스크립트는 `.scratch/k4/measure.py`(커밋하지 않음)입니다.
- 참고: Rust 매니저의 `ensure()`는 처음에 300~344 ms였습니다. 커널을 띄우기 전에 보내는 표적 멀티캐스트 질의가 없는 커널을 기다리느라 늘 200 ms(`QUERY_TIMEOUT`)를 썼기 때문입니다. 질의를 없앤 뒤(FR-M2, `fr_m2_ensure_starts_the_interpreter_without_a_discovery_wait`) 호출부터 인터프리터 실행까지 5~8 ms, 전체 92~120 ms입니다(2026-10-03, 부하 평균 약 1.5, `/usr/bin/python3` 셈은 Xcode 셈 때문에 170~600 ms).

### NFR-M1 발견 지연 — `Done`
매니저의 커널 목록 조회는 커널 20개에서 300 ms 이하입니다.
- 테스트: Rust `test_nfr_m1_list_kernels_with_20_real_kernels_within_300_ms`(`darkpyonix/manager/crates/darkpyonix/tests/manager_latency.rs`, 실제 커널 20개, 캐시·`refresh=true`·새 매니저의 첫 조회)
- 측정 기록 (2026-10-03, 맥미니 M 시리즈, 부하 평균 약 1.3, 디버그 빌드): 커널 20개에서 캐시 조회 p50 2.2 ms·p99 4.9 ms, `refresh=true` p50 3.9 ms·p99 5.1 ms, 새 매니저의 첫 조회 p50 4.6 ms·최대 4.7 ms입니다.

### NFR-M2 이벤트 지연 — `Agreed` (재측정 대기 #47)
커널의 출력이 SSE 구독자에게 도달하기까지 p99 100 ms 이하입니다(스트림 병합 50 ms 포함). 스트림 넘김(FR-M6) 뒤에는 커널이 구독자 소켓에 직접 쓰므로 그 경로로 다시 잽니다. 아래 기록은 매니저가 중계하던 구현의 값입니다.
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
- **조회 토큰(`dpr_…`).** 기기 링크를 받을 때(`POST /device-links/{link_id}/token`의 201) 기기 토큰과 함께 `resolve_token`을 하나 받습니다. 조회 토큰은 같은 계정 기기의 `GET /pkarr/{key}`에만 쓰이고(쿼리 또는 `Authorization: Bearer`), 다른 연산에서는 자격 증명이 아닙니다(401). 해시로만 저장합니다. 기기는 `POST /me/resolve-token`(자기 기기 토큰, 헤더)으로 새 조회 토큰을 받을 수 있고, 그러면 이전 조회 토큰은 즉시 무효입니다(교체). 지운 기기의 조회 토큰은 401 `device_removed`입니다.
- **쿼리의 기기 토큰은 받지 않습니다.** `?token=`에 기기 토큰(`dpd_…`)을 넣으면 401 `invalid_credentials`입니다. 기기 토큰과 세션은 헤더·쿠키로만 받습니다.
- **로그.** Worker 코드는 요청 URL, 쿼리, `Authorization` 헤더를 로그로 남기지 않습니다(예외 처리기는 예외만 남김). `wrangler.toml`은 Workers Logs의 호출 로그(요청 URL을 기록함)를 끄고(`[observability.logs] invocation_logs = false`), `console` 로그만 남깁니다. 운영자는 이 Worker에 요청 URL 필드를 담는 Logpush를 켜지 않습니다.
- 수용 기준: 링크로 받은 조회 토큰으로 `?token=` 조회가 200이고, 같은 조회 토큰으로 `GET /devices`·`GET /me`는 401입니다. 쿼리의 기기 토큰은 401 `invalid_credentials`입니다. 조회 토큰을 교체하면 이전 것은 401이고 새 것은 200입니다. 지운 기기의 조회 토큰은 401 `device_removed`입니다. 조회 요청(성공, 401, 404, 처리되지 않은 예외)을 처리하는 동안 Worker가 쓰는 `console` 출력에 토큰이 나타나지 않습니다. `wrangler.toml`의 호출 로그 설정이 꺼져 있습니다.
- 테스트(`hub/worker/test/tokens.test.ts`): `test_nfr_h2_resolve_token_reads_records_and_nothing_else`, `test_nfr_h2_query_refuses_device_tokens`, `test_nfr_h2_resolve_token_rotates`, `test_nfr_h2_removed_device_resolve_token_is_device_removed`, `test_nfr_h2_worker_never_logs_the_query`, `test_nfr_h2_invocation_logs_are_off`

### NFR-V1 버전 없는 REST API — `Done`
INTENT D16. 매니저와 허브의 REST 경로에는 버전 조각(`v1`, `v2`, …)이 없습니다. 매니저는 `/api/...`, 허브는 접두 없이 `/devices`, `/config`처럼 씁니다. 계약은 더하기만 합니다: 필드·선택 요청 필드·경로·오류 `code`를 더할 수 있고, 있는 것의 이름·타입·뜻을 바꾸거나 지우지 않습니다. 클라이언트는 모르는 필드를 무시합니다. 바꿔야 하면 새 필드나 경로를 더하고 옛것은 OpenAPI에서 `deprecated: true`로 남깁니다.
- 수용 기준: `docs/api/manager.openapi.yaml`과 `docs/api/hub.openapi.yaml`의 어떤 경로에도 `v<숫자>` 조각이 없습니다. 예전 버전 경로(`/api/v1/manager`, `/v1/config`)는 404입니다.
- 테스트: `test_nfr_v1_no_version_segment_in_any_rest_path`, `test_nfr_v1_versioned_manager_path_is_not_served`(`tests/test_fr_m_manager.py`), `test_nfr_v1_versioned_hub_path_is_not_served`(`hub/worker/test/config.test.ts`)

## 12. 프로토콜 요구사항

### PR-1 DKP/1 프레임 — `Done`
PROTOCOL §3.1. 64 MiB를 넘는 프레임은 거절합니다.
- 테스트: `test_pr_1_oversized_frame_is_refused`, `test_pr_1_frame_too_large_closes_connection`

### PR-2 핸드셰이크 — `Agreed` (새 채널에서 다시 통과 대기 #55)
PROTOCOL §3.2. 상대 계정 확인(FR-A1)이 먼저이고, 그다음 `hello`/`auth`/`welcome`입니다. 5초 제한. 비밀값이 없으므로 상수 시간 비교도 없습니다.
- 테스트: `test_pr_2_handshake_times_out`(지금은 TCP 채널에서 통과. 소켓·파이프 채널로 옮긴 뒤 다시 통과해야 `Done`)

### PR-3 이벤트 재전송 — `Done`
PROTOCOL §3.4. 링 버퍼와 `replay_truncated`. 읽지 않는 구독자가 있어도 이벤트를 내는 쪽은 막히지 않습니다(클라이언트별 송신 버퍼, 64 MiB를 넘으면 그 클라이언트를 끊음).
- 테스트: `test_pr_3_subscribe_replays_since_and_streams_live`, `test_pr_3_replay_truncated_when_ring_overflows`, `test_pr_3_ring_is_bounded_by_bytes`, `test_slow_subscriber_does_not_block_append`
- 측정 기록: 이벤트 append → 루프백 클라이언트 수신 지연, 1,000개 기준 p50 약 0.04–0.15 ms, p99 0.1–3.4 ms (맥미니, 다른 작업으로 load average 약 31인 상태, Python 3.13; 3.9·3.11도 p99 0.4–4.2 ms). `test_event_latency_p99`

### PR-4 호환성 — `Done`
모르는 필드는 무시하고, 필드 추가는 버전을 올리지 않습니다. 의미를 바꾸는 변경은 `dkp` 버전을 올립니다.
- 테스트: `test_pr_4_unknown_fields_are_ignored`

### PR-5 스트림 넘김 `adopt` — `Agreed` (사용자 결정 2026-10-03, 구현 대기 #47)
PROTOCOL §3.7.1–§3.7.3. POSIX는 제어 채널인 유닉스 도메인 소켓 위에서 `adopt` 프레임 직후 `SCM_RIGHTS`로 FD 하나, Windows는 제어 파이프 위 `share`에 `WSAPROTOCOL_INFOW`의 base64, 퍼 나르기는 소켓 쌍의 한쪽입니다. 커널은 파이썬 3.8에서도 되도록 `socket.recvmsg`로 받습니다(`recv_fds` 없음).
- 수용 기준: Windows 파이프로 온 `transport: "fd"`는 `bad_request`입니다. FD 없이 온 `adopt`는 `bad_request`입니다. `adopt`가 성공하면 `stream_id`를 돌려줍니다.
- 테스트(계획): `test_pr_5_adopt_fd_over_unix_socket`, `test_pr_5_adopt_fd_over_a_windows_pipe_is_bad_request`, `test_pr_5_adopt_without_fd_is_bad_request`

### PR-6 공유 철회 `streams.close` — `Agreed` (사용자 결정 2026-10-03, 구현 대기 #47)
PROTOCOL §3.7.4. `streams.close {share_id}`는 그 `share_id` 라벨의 넘겨받은 스트림을 모두 닫고 `{closed}`를 돌려줍니다.
- 테스트(계획): `test_pr_6_streams_close_by_share_id`
