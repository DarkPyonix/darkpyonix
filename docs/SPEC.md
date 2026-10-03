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
- 테스트: `test_fr_k2_kernel_id_is_stable_across_path_spellings`, Rust `fr_m2_kernel_id_matches_python`(`manager/crates/dpx-kernel/tests/discovery_launch.rs`, 매니저의 ID가 파이썬과 같음)

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

### FR-K7 재시작 — `Agreed`
`restart`(soft)는 네임스페이스를 비우고 실행 횟수를 0으로 돌립니다. `restart hard`는 같은 인터프리터와 인자로 프로세스를 다시 실행합니다(커널 ID 유지).
- 수용 기준: soft 재시작 뒤 이전 변수가 없습니다. hard 재시작 뒤 커널 ID가 같고 `pid`나 시작 시각이 바뀝니다.
- 테스트: `test_fr_k7_soft_restart_clears_namespace`, `test_fr_k7_hard_restart_stops_loop_and_sets_flag`(실행기 쪽), `test_fr_k7_hard_restart_keeps_kernel_id`(아직 없음)
- 상태 메모 (2026-10-03 감사): soft 재시작은 검증했습니다. hard 재시작은 실행기가 루프를 멈추고 플래그를 세우는 데까지만 검증했습니다. 실제 커널 프로세스가 다시 실행되어 커널 ID가 같고 `pid`나 시작 시각이 바뀌는 시험은 아직 없습니다.

### FR-K8 종료 — `Agreed`
`shutdown`은 실행 중인 셀을 인터럽트하고, 실행 기록을 마저 쓰고, `bye`를 보내고, 등록 파일을 지우고, 코드 0으로 끝납니다.
- 수용 기준: 종료 뒤 등록 파일과 잠금이 남지 않고 실행 기록의 상태는 `interrupted`입니다.
- 테스트: `test_fr_k8_shutdown_interrupts_running_cell_and_finishes_run`(실행기 쪽), `test_fr_k8_shutdown_is_graceful`(아직 없음)
- 상태 메모 (2026-10-03 감사): 실행 중인 셀을 인터럽트하고 실행 기록을 `interrupted`로 마무리하는 실행기 쪽과, SIGTERM으로 끝날 때 등록 파일이 지워지는 것(`test_fr_k4_kernel_survives_launcher_exit`)은 검증했습니다. 실제 커널에 `shutdown` 요청을 보내 `bye`, 종료 코드 0, 등록 파일과 잠금이 남지 않음을 확인하는 시험은 아직 없습니다.

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

### FR-R2 실행 중 저장 — `Agreed`
커널은 실행 중에도 기록을 최대 1초 간격으로 원자적으로(임시 파일 → `os.replace`) 다시 씁니다. `index.json`에는 최신순 실행 요약(`run_id`, `status`, `started_at`, `ended_at`, `mode`)을 둡니다.
- 수용 기준: 실행 중에 커널을 `SIGKILL`로 죽여도 기록 파일은 유효한 JSON이고, 죽기 1초 전까지의 출력이 들어 있습니다. 상태는 `running`으로 남고, 다음 커널이 그 파일을 열면 `crashed`로 바꿉니다.
- 테스트: `test_fr_r2_log_survives_kernel_kill`, `test_fr_r2_update_is_throttled`, `test_fr_r2_recover_leaves_this_processes_current_run_alone`
- 상태 메모 (2026-10-03 감사): 기록이 최대 1초 간격으로 원자적으로 다시 쓰이는 것, `RunStore`로 기록하던 프로세스를 `SIGKILL`로 죽여도 유효한 JSON과 `running` 상태가 남는 것, `recover_crashed()`가 `crashed`로 바꾸는 것은 검증했습니다. 시험은 실제 커널 대신 `RunStore`만 쓰는 대역 프로세스를 죽이고 복구 함수를 직접 부르며, 마지막 출력이 죽기 1.3초 전 이내인지 봅니다(기준은 1초). 실제 커널을 실행 중에 죽이고, 다음 커널이 시작하면서 그 기록을 `crashed`로 바꾸는 시험은 아직 없습니다.

### FR-R3 매직 변수 `__runs__` — `Agreed`
커널 네임스페이스에는 `__runs__` 객체가 있습니다.

| 표현 | 값 |
|---|---|
| `__runs__.current` | 진행 중인 실행의 노트북 dict, 없으면 `None` |
| `__runs__.latest` | 가장 최근에 끝난 실행의 노트북 dict, 없으면 `None` |
| `__runs__[run_id]`, `__runs__[-1]` | 해당 실행의 노트북 dict |
| `__runs__.list(limit=20)` | 최신순 요약 목록 |
| `__runs__.dir` | 기록 폴더 경로(str) |

목적은 실행 기록 `.ipynb`를 `json` import 없이 편하게 다루는 것입니다. 그래서 반환값은 dict이면서 속성 접근도 됩니다.
- `run.run_id`, `run.status`, `run.params`, `run.cells`
- `run.cells[i].outputs`, `run.cells[i].text`: 스트림 출력 문자열을 이어 붙인 것
- `run.cells[i].result`: `execute_result`의 `text/plain`
- `run.cell("cell_id 또는 제목")`
- `run.path`
- `run.notebook`: 원본 nbformat dict
- 수용 기준:
  - 두 번째 실행의 셀에서 `__runs__.latest.run_id`가 첫 번째 실행의 ID입니다.
  - `__runs__.latest.cells[1].text`가 그 셀의 표준 출력입니다.
  - 반환값은 `json.dumps`로 직렬화됩니다.
- 테스트: `test_fr_r3_runs_magic_exposes_logs_as_json`
- 상태 메모 (2026-10-03 감사): dict로서의 `__runs__`(`current`, `latest`, `[run_id]`, `[-1]`, `list()`, `dir`, `json.dumps`)는 검증했습니다. 속성 접근(`run.run_id`, `run.cells[i].text`, `.result`, `run.cell(...)`, `run.path`, `run.notebook`)은 아직 구현되지 않았고, 커널 셀 안에서 `__runs__.latest.run_id`와 `__runs__.latest.cells[1].text`를 읽는 시험도 없습니다.

### FR-R4 기록과 셀 맵핑 — `Done`
매니저의 `GET /kernels/{id}/document`는 파일의 셀 목록에 최신 실행의 출력을 붙여서 돌려줍니다. 맵핑 순서는 `id` → `source_sha256` → `index`이고, 소스 해시가 다르면 `stale: true`로 표시합니다.
- 수용 기준: 실행 뒤 셀 하나를 고치면 그 셀만 `stale: true`이고 출력은 남아 있습니다.
- 테스트: `test_fr_r4_document_maps_latest_outputs_and_marks_stale`, `test_fr_r4_document_without_runs`, Rust `fr_r4_document_is_built_by_the_embedded_python`(`manager/crates/dpx-kernel/tests/discovery_launch.rs`)

### FR-R5 큰 출력 — `Done`
셀 하나의 스트림 출력이 기록 안에서 `DARKPYONIX_RUN_OUTPUT_LIMIT`(기본 16 MiB)를 넘으면, 넘는 부분은 `<run_id>.cell<index>.log`에 이어 쓰고 노트북에는 그 사실을 알리는 스트림 한 줄을 남깁니다. 실시간 이벤트에는 제한이 없습니다.
- 수용 기준: 20 MiB를 출력하는 셀의 기록 노트북이 17 MiB를 넘지 않고, 사이드카 로그와 합치면 전체 출력이 됩니다.
- 테스트: `test_fr_r5_oversized_stream_spills_to_sidecar`

## 5. 발견 (D)

### FR-D1 멀티캐스트 발견 — `Done`
PROTOCOL §2를 구현합니다. 매니저의 query에 같은 사용자의 모든 커널이 200 ms 안에 응답합니다.
- 수용 기준: 커널 세 개를 띄우고 query 하나를 보내면 세 개의 announce를 받습니다. 다른 `user_tag`의 query에는 응답하지 않습니다.
- 테스트: `test_fr_d1_query_finds_all_kernels`, `test_fr_d1_other_user_tag_is_ignored`, `test_fr_d1_listener_sees_announce_and_bye`, Rust `fr_d1_query_finds_all_kernels`(`manager/crates/dpx-kernel/tests/discovery_launch.rs`)

### FR-D2 등록 파일 보조 — `Done`
커널은 announce 본문을 `kernels/<kernel_id>.json`에 둡니다. 매니저는 멀티캐스트 결과와 등록 파일을 합치되, `pid`가 살아 있지 않은 등록은 지웁니다. `DARKPYONIX_DISCOVERY=registry`이면 등록 파일만 씁니다.
- 수용 기준: 멀티캐스트를 끈 상태에서도 매니저가 커널을 찾습니다. `kill -9`로 죽은 커널의 등록은 다음 발견에서 사라집니다.
- 테스트: `test_fr_d2_registry_fallback_finds_kernels`, `test_fr_d2_stale_registry_is_pruned`, `test_fr_d2_registry_ignores_other_users`, Rust `fr_d2_registry_fallback_finds_kernels`, `fr_d2_stale_registry_is_pruned`(`manager/crates/dpx-kernel/tests/discovery_launch.rs`)

## 6. 런타임 API와 파일 형식 (F)

### FR-F1 셀 파서 — `Done`
FORMAT §2의 문법(프리앰블, 셀 표식, 제목, 타입, 메타데이터, 셀 식별)을 파싱합니다. 파서는 커널, 매니저, 런타임 API가 공유하며 표준 라이브러리만 씁니다.
- 수용 기준: `docs/examples/darkpyonix_format.py`를 파싱하면 프리앰블 1개와 셀 22개(code 11, markdown 2, binding 2, argparse·shell·parallel·concurrent·cinterop·cppinterop·rustinterop 각 1)가 나오고, 타입, 제목, `@width` 메타데이터, `concorrunt`→`concurrent` 별칭이 FORMAT대로 나옵니다. 파싱 후 다시 직렬화하면 원문과 바이트 단위로 같습니다.
- 테스트: `test_fr_f1_reference_file_parses`, `test_fr_f1_parse_serialize_roundtrip`

### FR-F2 `darkpyonix.markdown` — `Agreed`
FORMAT §3.2. 커널 안에서는 `text/markdown` `display_data`를 내고(`silent=True`이면 기록에 남기지 않음), 커널 밖에서는 아무것도 하지 않습니다. 모르는 키워드 인자는 경고만 남깁니다.
- 테스트: `test_fr_f2_markdown_in_kernel_and_plain_python`
- 상태 메모: 커널 밖 동작과 `darkpyonix.kernel.hostctx` 계약까지는 검증했습니다. 실제 커널이 `display_data`로 내보내는 경로는 실행기(executor)와 합친 뒤 검증하고 `Done`으로 바꿉니다. 2026-10-03 감사: 실행기는 합쳐졌고 `hostctx.emit_display`의 커널 경로는 `test_fr_x5_hostctx_exposes_params_and_display`가 검증합니다. 셀 안에서 `darkpyonix.markdown`을 불러 `text/markdown` `display_data`가 나오고 `silent=True`면 기록에 남지 않는지 보는 시험은 아직 없습니다.

### FR-F3 `darkpyonix.params` — `Done`
FORMAT §3.3. 값의 우선순위는 실행 요청 `params` → 명령줄 `--name` → `default`입니다.
- 수용 기준: `choices`와 정수 `default`면 인덱스로 고르고, `range`를 벗어난 값은 `ValueError`입니다. `python file.py --model_id swin_t`가 `"swin_t"`를 냅니다.
- 테스트: `test_fr_f3_params_precedence_and_validation`

### FR-F4 `darkpyonix.binding` — `Done`
FORMAT §3.4. 이슈 #6의 참조 구현을 따르되, `binding` 데코레이터만 벗기고 다른 데코레이터는 보존합니다.
- 수용 기준: `[code]` 셀 변수를 참조하는 binding 클래스 본문은 `NameError`를 냅니다. import한 이름과 앞선 binding은 보입니다.
- 테스트: `test_fr_f4_binding_cannot_see_code_cell_variables`

### FR-F5 일반 파이썬과 같은 동작 — `Agreed`
노트북 파일을 `python file.py`로 실행한 결과(표준 출력, 종료 코드)가 커널 전체 실행의 스트림 출력과 같습니다. 마크다운 출력과 `display`의 MIME 번들은 이 비교에서 뺍니다.
- 테스트: `test_fr_x1_run_all_matches_plain_python` (FR-X1과 공유), `test_fr_f5_reduced_reference_runs_under_plain_python`
- 상태 메모 (2026-10-03 감사): 표준 출력과 표준 오류가 같음은 검증했습니다. 종료 코드(오류로 끝나는 파일에서 `python file.py`의 0이 아닌 종료 코드와 커널 실행 상태 `error`)를 맞춰 보는 시험은 아직 없습니다.

### FR-F6 `darkpyonix.run_command` — `Done`
셸 명령을 하위 프로세스로 실행하고 출력을 줄 단위로 스트림 출력으로 보냅니다. `check=True`이면 실패 시 `CalledProcessError`입니다. 인터럽트가 오면 하위 프로세스 그룹에 SIGINT를 전달합니다.
- 테스트: `test_fr_f6_run_command_streams_and_forwards_interrupt`

## 7. 매니저 (M)

### FR-M1 HTTP API — `Done`
매니저는 [api/manager.openapi.yaml](api/manager.openapi.yaml)의 경로를 모두, 그리고 그 경로만 냅니다. 이벤트 스트림은 SSE(`text/event-stream`)이고 SSE `id`는 커널의 `seq`입니다. `Last-Event-ID` 헤더나 `since` 쿼리로 이어 받습니다.
- 테스트: Rust `test_nfr_m3_every_operation_answers_with_a_documented_status`, `test_nfr_m3_undocumented_methods_are_not_served`(`manager/crates/dpx-server/tests/openapi.rs`), `test_fr_m1_events_stream_resumes_with_last_event_id`, `test_fr_m1_events_errors_and_keepalive`(`manager/crates/dpx-server/tests/sse.rs`), 각 경로의 동작 테스트(`manager/crates/dpx-server/tests/api.rs`). 파이썬 시제품 기준 `test_fr_m1_*`(`tests/test_fr_m_manager.py`)

### FR-M2 커널 시작은 멱등 — `Done`
`POST /kernels {path}`는 그 파일의 커널이 살아 있으면 그 커널을 `200`으로, 없으면 새로 띄워서 `201`로 돌려줍니다. 커널이 announce를 낼 때까지 최대 10초를 기다립니다.
- 테스트: `test_fr_m2_start_kernel_is_idempotent`(Rust `manager/crates/dpx-server/tests/api.rs`, 파이썬 시제품), Rust `fr_m2_start_kernel_is_idempotent_on_every_interpreter`, `fr_m2_start_timeout_when_no_announce`(`manager/crates/dpx-kernel/tests/discovery_launch.rs`)

### FR-M3 임시 모드 수명 — `Done`
임시 매니저는 `127.0.0.1`의 임의 포트에 리슨합니다. `managers/<pid>.json`(0600)에 주소와 토큰을 쓰고, 열린 SSE 스트림이 없고 HTTP 요청도 없는 상태가 `idle_timeout`(기본 120초) 동안 이어지면 스스로 끝납니다. 끝날 때 등록을 지우고 커널은 건드리지 않습니다.
- 테스트: `test_fr_m3_ephemeral_manager_exits_when_idle_and_kernels_remain`(Rust `manager/crates/dpx-server/tests/lifecycle.rs`, 파이썬 시제품), Rust `test_fr_m3_registry_file_is_private_and_complete`, `test_fr_m3_shutdown_removes_registry_file`

### FR-M4 전용 모드 — `Done`
`darkpyonix manager --dedicated`는 유휴 종료 없이 돌고, 설정한 호스트·포트에 리슨하고, 마스터 토큰과 공유 토큰으로 인증합니다. 토큰은 해시로만 `manager.db`(SQLite)에 저장합니다.
- 테스트: Rust `test_fr_m4_dedicated_manager_never_idles_and_requires_token`(`manager/crates/dpx-server/tests/lifecycle.rs`), `test_fr_m4_shares_are_stored_hashed_and_survive_restart`(`manager/crates/dpx-server/tests/api.rs`)

### FR-M5 매니저 여러 개 공존 — `Done`
같은 사용자의 매니저 여러 개가 같은 커널에 동시에 붙을 수 있고, 각자 같은 이벤트를 받습니다.
- 테스트: `test_fr_m5_two_managers_share_one_kernel`(파이썬 시제품), Rust `fr_m5_two_managers_share_one_kernel`, `fr_m5_reconnects_on_demand_after_kernel_restart`(`manager/crates/dpx-kernel/tests/dkp_fake_kernel.rs`)

## 8. CLI (C)

### FR-C1 매니저 찾기 — `Done`
`darkpyonix` CLI는 `managers/*.json`에서 살아 있는 매니저를 고르고, 없으면 임시 매니저를 띄웁니다. 에이전트가 기존 매니저의 토큰을 몰라도 같은 OS 사용자라면 그대로 동작합니다.
- 테스트: Rust `test_fr_c1_cli_spawns_manager_when_none_is_running`, `test_fr_c1_skips_dead_and_unhealthy_registrations`, `test_fr_c1_spawn_failure_is_reported`(`manager/crates/darkpyonix/tests/cli.rs`)

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
- 테스트: Rust `test_fr_c2_cli_commands`, `test_fr_c2_second_run_exits_75_with_hint`, `test_fr_c2_run_follows_outputs_and_exits_0`, `test_fr_c2_run_error_exits_1_with_traceback`, `test_fr_c2_ctrl_c_interrupts_the_run_and_exits_130`, `test_fr_c2_second_ctrl_c_detaches_and_the_run_continues`, `test_fr_c2_detach_prints_the_run_id_and_does_not_follow`, `test_fr_c2_run_options_reach_the_api`, `test_fr_c2_logs_follow_replays_the_executing_run`(`manager/crates/darkpyonix/tests/cli.rs`), `test_fr_c2_every_command_parses`, `test_fr_c2_invalid_usage_is_rejected`(`manager/crates/darkpyonix/src/args.rs`)
- 상태 메모 (2026-10-03 감사): `attach`, 전용 매니저에서의 `share`, `manager` 하위 명령은 인자 파싱 시험만 있고 동작 시험은 없습니다.

## 9. 인증과 공유 (A)

### FR-A1 커널 인증 — `Done`
PROTOCOL §3.2의 HMAC 도전-응답입니다. 사용자 키가 없으면 처음 쓰는 쪽이 0600으로 원자적으로 만듭니다.
- 테스트: `test_fr_a1_wrong_key_is_rejected`, `test_fr_a1_hello_carries_identity_and_nonce`, `test_fr_a1_user_key_is_created_once_with_0600`

### FR-A2 매니저 토큰 — `Done`
모든 HTTP 요청은 `Authorization: Bearer <token>`이 필요합니다. 헤더를 붙일 수 없는 SSE(`EventSource`)와 공유 링크만 `?token=`을 받습니다. `/health`만 인증 없이 열립니다.
- 테스트: `test_fr_a2_requests_without_token_are_401`(Rust `manager/crates/dpx-server/tests/api.rs`, 파이썬 시제품), Rust `test_fr_a2_registry_token_is_used`(`manager/crates/darkpyonix/tests/cli.rs`)

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
- 테스트: Rust `test_fr_a3_permission_matrix`, `test_fr_a3_share_tokens_are_scoped_to_one_kernel_and_permission`, `test_fr_a3_ephemeral_manager_refuses_share_creation`(`manager/crates/dpx-server/tests/api.rs`), `test_fr_a3_viewer1_events_omit_outputs`(`manager/crates/dpx-server/tests/sse.rs`)

## 10a. 협업 문서 (S)

한 커널(=파일)에 여러 클라이언트가 동시에 붙습니다. VS Code 확장, IntelliJ, ash, Ember 대화 화면, 에이전트가 함께 붙을 수 있습니다. 2025 설계의 셀 동기화, 셀 잠금, 포커스, 실행 알림, 알람을 이어받습니다(`설계초안/`의 WS 명세). 커널이 이 상태를 들고 있습니다. 매니저는 언제든 사라질 수 있고, 같은 파일에 서로 다른 매니저(로컬 임시 매니저와 전용 매니저)로 붙은 클라이언트도 같은 상태를 봐야 하기 때문입니다.

### FR-S1 공유 문서 상태 — `Done`
커널은 파일을 파싱한 문서(셀 목록)를 메모리에 두고 문서 버전 `doc_version`을 관리합니다. `doc_version`은 셀 생성·수정·삭제·이동과 바깥 편집으로 다시 읽기(`doc.reloaded`)에서만 1 늘어나고, 잠금·해제·충돌 표시·접속자 이벤트에서는 늘지 않습니다(그 이벤트들도 현재 `doc_version`을 담습니다, PROTOCOL §4). 셀마다 커널 수명 동안 바뀌지 않는 `cell_id`를 둡니다. 파일에 `# @id`가 있으면 그 값을 쓰고, 없으면 `c_<hex>`를 만들되 파일에는 쓰지 않습니다. 첫 동기화 스냅숏(`GET /kernels/{id}/document`)에는 셀(`cell_id`, 셀별 `version`, 소스, 최신 출력), 잠금, 접속자, `doc_version`, 그리고 `seq`가 함께 들어 있습니다. `seq`는 **스냅숏에 이미 반영된 마지막 이벤트의 번호**이고, 클라이언트는 `since=seq`로 구독해 `seq`보다 큰 이벤트만 적용합니다. 커널은 상태 변경·이벤트 발행·스냅숏을 한 잠금 안에서 하므로, 스냅숏과 경쟁한 편집은 빠지지도 두 번 적용되지도 않습니다. 셀의 `source`는 파서가 낸 본문 그대로(다음 표식 앞의 빈 줄 포함)이고, `type`은 정규 타입, `raw_type`은 표식에 쓰인 타입 원문(없으면 `null`)입니다.
- 수용 기준: 두 클라이언트가 같은 스냅숏을 받은 뒤 한쪽이 편집하면, 다른 쪽은 이벤트만으로 같은 문서 상태에 도달합니다(셀 순서, 소스, 버전이 같음).
- 테스트: `test_fr_s1_snapshot_plus_events_converge`, `test_fr_s1_snapshot_racing_edits_is_exact`, `test_fr_s1_doc_version_bumps_only_on_content`, `test_fr_s1_s2_s3_s5_s8_two_clients_edit_converge_save_and_run` (실제 커널 프로세스)
- 상태 메모: 커널의 `doc.snapshot`과 `doc.*` 이벤트를 검증했습니다. 스냅숏에 셀별 최신 출력을 합치는 일(`cell.*` 이벤트의 `cell_id`와 실행 기록으로)은 매니저가 맡고, Rust `test_fr_s1_document_combines_snapshot_and_outputs`, `test_fr_s1_snapshot_cells_find_their_outputs_after_a_move`(`manager/crates/dpx-server/tests/collab.rs`)가 검증합니다.

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
- 상태 메모: 커널 쪽(`presence.update`/`presence.leave`, 30초 유예)을 검증했습니다. 이벤트 스트림이 열려 있는 동안 약 10초마다 `presence.update` 하트비트를 보내는 일은 매니저가 맡고, Rust `test_fr_s4_event_stream_with_client_id_heartbeats_presence`(`manager/crates/dpx-server/tests/collab.rs`)가 검증합니다.

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
- 상태 메모: 커널의 `runs.wait`(끝나면 바로, 아니면 `timeout` 뒤)를 검증했습니다. HTTP 응답의 `next` 정보는 매니저가 붙이고, Rust `test_fr_s7_wait_returns_on_finish_or_timeout`(`manager/crates/dpx-server/tests/collab.rs`)가 검증합니다.

### FR-S8 권한 — `Done`
셀 편집(FR-S2)과 잠금(FR-S3)은 `editor` 이상만 할 수 있습니다. `editor`는 `viewer3`(실행 가능)에 셀 편집을 더한 공유 권한이고, 2025 설계의 `user_permission: "write"`에 해당합니다. 접속자 표시와 포커스(FR-S4)는 `viewer1`부터 할 수 있습니다. FR-A3 표에 `editor`를 더합니다.
- 테스트: `test_fr_a3_permission_matrix` (FR-A3과 공유), `test_fr_s8_edit_and_lock_need_editor`, `test_fr_s1_s2_s3_s5_s8_two_clients_edit_converge_save_and_run`
- 상태 메모: 커널의 권한 검사(`client.permission`)를 검증했습니다. 매니저 쪽은 Rust `test_fr_a3_permission_matrix`(`manager/crates/dpx-server/tests/api.rs`)와 `test_fr_s8_permission_matrix_for_collaboration`(`manager/crates/dpx-server/tests/collab.rs`)가 검증합니다.

## 10. 허브 (H)

허브는 터널 방식(PROJECT Q1)과 로그인 방식(Q2)이 정해진 뒤에 `Agreed`로 올립니다.

### FR-H1 기기 등록 — `Draft`
기기(메인 서버·지부)는 허브에 공개 키로 등록하고, 허브는 계정별 기기 목록을 냅니다.

### FR-H2 랑데부와 홀펀칭 — `Draft`
허브는 기기 사이의 연결 후보를 교환하는 시그널링과 공인 주소 반사를 제공해, 사용자가 연결을 신경 쓰지 않아도 P2P가 맺어지게 합니다.

### FR-H3 중계 — `Draft`
홀펀칭이 실패하면 허브가 암호화된 바이트를 중계합니다.

### FR-H4 ash 호스팅과 공유 링크 — `Draft`
`https://darkpyonix.dev/ash/`에서 공식 ash 뷰어를 호스팅하고, `https://darkpyonix.dev/s/<share_id>` 링크로 공유 커널에 연결합니다.

### FR-H5 HTTPS — `Draft`
메인 서버가 `https://<name>.darkpyonix.dev` 형식의 주소와 공인 인증서를 얻게 합니다(모바일 웹뷰의 보안 컨텍스트 요건).

### FR-H6 로그인 — `Draft`
OpenAI 계정 로그인을 지원하고, Codex 토큰 사용량 외에 Chat 사용량도 쓸 수 있는 페이지를 둡니다. 2026-10-03 조사 결과 ChatGPT 플랜 사용("Sign in with ChatGPT")은 오픈소스·로컬 호스팅 앱에 열려 있고 원격 호스팅은 별도 승인이 필요합니다. 그래서 플랜 사용은 ember server가 맡고(ember SPEC), 허브의 로그인은 승인을 받은 뒤에 다룹니다(PROJECT Q2).

## 11. 비기능 요구사항

### NFR-K1 인터프리터 범위 — `Done`
커널과 런타임 API는 CPython 3.8 이상 모든 마이너 버전에서 동작합니다. 테스트는 그 기계에 있는 모든 인터프리터(`DARKPYONIX_TEST_PYTHONS`, 기본은 PATH에서 찾은 `python3.*`)로 커널 테스트를 돌립니다.
- 테스트: `python` 픽스처로 매개변수화된 커널·런타임 테스트 39개(`tests/conftest.py`)
- 측정 기록 (2026-10-03, macOS arm64): `DARKPYONIX_TEST_PYTHONS`에 CPython 3.8.20, 3.9.6, 3.10.20, 3.11.10, 3.12.13, 3.13.0(intel64), 3.14.7, 3.15.0rc1을 넣어 39개 × 8개 인터프리터 = 312개 중 304개 통과, 8개는 `DARKPYONIX_SKIP_PERF=1`로 뺀 NFR-K3 측정입니다(NFR-K3는 따로 돌려 통과). 3.8·3.10·3.12는 `uv python install`로 `.scratch/` 아래에 받은 인터프리터입니다.

### NFR-K2 표준 라이브러리 전용 — `Done`
`kernel/darkpyonix/kernel/`, `kernel/darkpyonix/*.py`, `kernel/darkpyonix/format/`의 모든 import가 표준 라이브러리임을 테스트가 AST로 확인합니다(`sys.stdlib_module_names`, 3.8용 고정 목록 병행). 예외는 `kernel/darkpyonix/kernel/mplbackend.py` 하나입니다. 사용자 코드가 pyplot을 import할 때 matplotlib이 직접 불러오는 백엔드 모듈이라 `matplotlib`을 import할 수 있고(FR-X6), 다른 커널 코드는 이 모듈을 import하지 않습니다.
- 테스트: `test_nfr_k2_kernel_imports_stdlib_only`, `test_nfr_k2_no_kernel_code_imports_the_matplotlib_backend`

### NFR-K3 출력 오버헤드 — `Agreed`
`print`를 100,000번 하는 셀의 실행 시간이 같은 인터프리터의 일반 실행 대비 1.5배를 넘지 않습니다. 구독자가 느려도 메인 스레드가 막히지 않습니다(출력 큐 상한을 넘으면 기록은 계속하되 실시간 이벤트를 합칩니다).
- 측정 기록 (2026-10-03, macOS arm64, `test_nfr_k3_print_overhead`): 셀 안 100,000번 `print`를 파이프로 출력하는 일반 실행과 비교, 5회 중 최솟값의 프로세스 CPU 시간 비율은 3.9 1.08, 3.11 1.17, 3.13 1.24, 3.14 1.42, 3.15 1.21입니다. 측정 당시 머신의 부하 평균이 100을 넘어 벽시계 시간은 같은 측정 안에서도 0.7~8배로 흔들렸으므로 판정에 쓰지 않았습니다. 한가한 머신에서 벽시계 시간을 다시 재야 `Done`이 됩니다.

### NFR-K4 시작 시간 — `Agreed`
커널 시작(프로세스 실행부터 announce까지)은 기준 기계(맥미니 M 시리즈)에서 300 ms 이하입니다.
- 측정 기록: (구현 후 기입)

### NFR-M1 발견 지연 — `Agreed`
매니저의 커널 목록 조회는 커널 20개에서 300 ms 이하입니다.

### NFR-M2 이벤트 지연 — `Agreed`
커널의 출력이 매니저 SSE 구독자에게 도달하기까지 p99 100 ms 이하입니다(스트림 병합 50 ms 포함).

### NFR-M3 문서와 코드의 일치 — `Done`
매니저가 실제로 답하는 경로·메서드·응답 코드가 `docs/api/manager.openapi.yaml`과 같습니다. 구현 언어와 무관하게, 테스트는 모든 연산을 HTTP로 불러 문서에 있는 상태 코드로만 답하는지 확인합니다(`test_nfr_m3_every_operation_answers_with_a_documented_status`). 예외: API 문서 페이지(`/docs/`, `/docs/manager.openapi.yaml`, `/docs/hub.openapi.yaml`)는 계약 밖의 정적 파일입니다.
- 테스트: Rust `test_nfr_m3_every_operation_answers_with_a_documented_status`, `test_nfr_m3_documented_statuses_with_a_live_kernel_and_dedicated_mode`, `test_nfr_m3_undocumented_methods_are_not_served`(`manager/crates/dpx-server/tests/openapi.rs`), 파이썬 시제품 기준 `test_nfr_m3_every_operation_answers_with_a_documented_status`

### NFR-H1 종단 간 암호화 — `Draft`
허브는 중계하는 내용을 볼 수 없습니다. 기기 사이의 세션 키는 허브를 거치지 않고 합의합니다.

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
