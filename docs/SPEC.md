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

### FR-K1 설치 없이 어떤 인터프리터로도 실행 — `Agreed`
매니저는 사용자가 고른 인터프리터에, 커널 소스 루트를 `sys.path` 앞에 넣는 부트스트랩(`-c`)으로 커널을 띄웁니다. 그 인터프리터에 DarkPyonix가 설치되어 있지 않아도 됩니다.
- 수용 기준: DarkPyonix가 설치되지 않은 가상환경의 인터프리터로 커널을 띄우고 셀을 실행할 수 있습니다. 사용자 코드의 `import darkpyonix`가 성공합니다.
- 테스트: `test_fr_k1_kernel_runs_from_uninstalled_interpreter`

### FR-K2 파일에 묶인 커널 ID — `Agreed`
커널 ID는 PROTOCOL §2.6의 규칙으로 만듭니다.
- 수용 기준: 같은 파일의 절대 경로, 상대 경로, 심볼릭 링크가 같은 ID를 냅니다. 다른 파일은 다른 ID를 냅니다.
- 테스트: `test_fr_k2_kernel_id_is_stable_across_path_spellings`

### FR-K3 파일당 커널 하나 — `Agreed`
커널은 시작할 때 `locks/<kernel_id>.lock`에 OS 배타 잠금(POSIX `fcntl.flock`, Windows `msvcrt.locking`)을 겁니다. 잠그지 못하면 종료 코드 3으로 끝나고, 표준 오류에 이미 떠 있는 커널의 announce 본문을 씁니다.
- 수용 기준: 같은 파일로 커널을 두 번 띄우면 두 번째가 코드 3으로 끝나고 첫 번째는 영향받지 않습니다. 첫 번째를 `kill -9`로 죽인 뒤에는 새 커널이 바로 뜹니다.
- 테스트: `test_fr_k3_second_kernel_for_same_file_is_refused`, `test_fr_k3_lock_is_released_when_kernel_dies`

### FR-K4 매니저 독립 수명 — `Agreed`
커널은 분리된 세션(POSIX `start_new_session`, Windows `DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP`)으로 시작하고, 표준 입력은 닫고, 진단 출력은 `kernels/<kernel_id>.log`로 보냅니다.
- 수용 기준: 실행 중인 셀이 있는 상태에서 커널을 띄운 매니저를 `SIGKILL`로 죽여도 셀은 끝까지 실행되고, 새 매니저가 그 실행의 결과를 읽을 수 있습니다.
- 테스트: `test_fr_k4_kernel_survives_manager_kill`

### FR-K5 사용자 코드는 메인 스레드 — `Agreed`
셀 코드는 커널 프로세스의 메인 스레드에서 실행합니다(INTENT D12).
- 수용 기준: 셀 안에서 `threading.current_thread() is threading.main_thread()`가 `True`이고, `signal.signal`을 호출할 수 있습니다.
- 테스트: `test_fr_k5_cells_run_on_main_thread`

### FR-K6 네임스페이스 조회 — `Agreed`
`namespace` 요청은 사용자 변수의 이름, 타입, 요약을 PROTOCOL §3.6 형식으로 돌려줍니다.
- 수용 기준: 대기 중에는 `repr`가 채워지고, 실행 중에는 `name`과 `type`만 채워집니다. 실행 중에 조회해도 사용자 객체의 `__repr__`가 호출되지 않습니다.
- 테스트: `test_fr_k6_namespace_lists_user_variables`, `test_fr_k6_namespace_does_not_call_repr_while_busy`

### FR-K7 재시작 — `Agreed`
`restart`(soft)는 네임스페이스를 비우고 실행 횟수를 0으로 돌립니다. `restart hard`는 같은 인터프리터와 인자로 프로세스를 다시 실행합니다(커널 ID 유지).
- 수용 기준: soft 재시작 뒤 이전 변수가 없습니다. hard 재시작 뒤 커널 ID가 같고 `pid`나 시작 시각이 바뀝니다.
- 테스트: `test_fr_k7_soft_restart_clears_namespace`, `test_fr_k7_hard_restart_keeps_kernel_id`

### FR-K8 종료 — `Agreed`
`shutdown`은 실행 중인 셀을 인터럽트하고, 실행 기록을 마저 쓰고, `bye`를 보내고, 등록 파일을 지우고, 코드 0으로 끝납니다.
- 수용 기준: 종료 뒤 등록 파일과 잠금이 남지 않고 실행 기록의 상태는 `interrupted`입니다.
- 테스트: `test_fr_k8_shutdown_is_graceful`

## 3. 실행 (X)

### FR-X1 전체 실행과 셀 실행 — `Agreed`
`run`은 `mode: all`(프리앰블과 모든 셀을 순서대로)과 `mode: cells`(프리앰블이 아직 실행되지 않았으면 먼저 실행, 그다음 지정한 셀)를 받습니다. `source`가 오면 디스크 파일 대신 그 텍스트를 파싱합니다(저장하지 않은 편집기 버퍼). 실행 네임스페이스의 `__name__`은 `"__main__"`, `__file__`은 파일 경로입니다.
- 수용 기준: 전체 실행의 출력이 `python file.py`의 출력과 같습니다(FR-F5). 셀 실행은 지정한 셀만 실행합니다. 앞 셀에서 만든 변수는 다음 실행에도 남습니다.
- 테스트: `test_fr_x1_run_all_matches_plain_python`, `test_fr_x1_run_cells_keeps_namespace`

### FR-X2 셀 실행 의미 — `Agreed`
셀 본문은 `exec`로 실행하되, 마지막 문장이 식이면 그 값을 `execute_result`로 내고 `_`에 저장합니다(값이 `None`이면 내지 않음). 실패하면 `error` 출력(ename, evalue, traceback)을 내고 그 실행의 남은 셀을 건너뜁니다.
- 수용 기준: `x = [i**2 for i in range(5)]; x` 셀이 `execute_result`로 `[0, 1, 4, 9, 16]`을 냅니다. traceback에 커널 내부 프레임이 들어가지 않습니다.
- 테스트: `test_fr_x2_last_expression_is_execute_result`, `test_fr_x2_error_stops_run_and_hides_kernel_frames`

### FR-X3 바쁠 때의 정책 — `Agreed`
실행 중에 `run`이 오면 기본(`on_busy: reject`)은 `busy` 오류이고, `data`에 현재 실행 요약이 들어갑니다. `on_busy: queue`이면 대기열에 넣고 `position`을 돌려줍니다. 대기열은 들어온 순서로 실행합니다.
- 수용 기준: 같은 파일에 `run`을 연달아 두 번 보내면 두 번째가 `busy`를 받습니다. `queue`로 보내면 첫 실행이 끝난 뒤 실행됩니다.
- 테스트: `test_fr_x3_second_run_is_rejected_when_busy`, `test_fr_x3_queued_run_waits_its_turn`

### FR-X4 인터럽트 — `Agreed`
`interrupt`는 실행 중인 셀에 `KeyboardInterrupt`를 일으킵니다. 네임스페이스와 커널 프로세스는 그대로 남고, 실행은 `interrupted`로 끝나며, 대기열은 유지합니다.
- 수용 기준: `while True: step += 1` 셀을 인터럽트하면 1초 안에 실행이 `interrupted`로 끝나고, 이어지는 실행에서 `step`을 읽을 수 있습니다.
- 테스트: `test_fr_x4_interrupt_stops_cell_and_keeps_state`

### FR-X5 출력 캡처 — `Agreed`
`sys.stdout`/`sys.stderr` 쓰기는 `stream` 출력이 됩니다. 파일 디스크립터 1·2에 직접 쓰는 출력(C 확장, 하위 프로세스)도 파이프로 받아 같은 스트림에 넣습니다. `display()`와 IPython식 `_repr_*_` 프로토콜(`_repr_mimebundle_`, `_repr_html_`, `_repr_png_`, `_repr_markdown_`, `_repr_json_`)은 `display_data`/`execute_result` MIME 번들이 됩니다.
- 수용 기준: `os.write(1, b"x\n")`와 `subprocess.run(["echo","y"])`의 출력이 해당 셀의 `stream` 출력에 나타납니다. `_repr_html_`이 있는 객체를 마지막 식으로 두면 `text/html`이 있는 `execute_result`가 나옵니다.
- 테스트: `test_fr_x5_fd_level_output_is_captured`, `test_fr_x5_repr_protocol_becomes_mime_bundle`

### FR-X6 matplotlib — `Agreed`
matplotlib이 설치된 인터프리터에서는 커널이 `plt.show()`와 셀 끝에 남은 그림을 `image/png` `display_data`로 냅니다. matplotlib이 없으면 아무것도 하지 않습니다(커널은 matplotlib을 import하지 않고, 사용자 코드가 import했을 때만 훅을 겁니다).
- 수용 기준: `plt.plot([1,2]); plt.show()` 셀이 PNG 출력을 냅니다.
- 테스트: `test_fr_x6_matplotlib_show_emits_png` (matplotlib이 없으면 skip)

## 4. 실행 기록 (R)

### FR-R1 자동 기록 — `Agreed`
모든 실행은 `<파일 폴더>/__runs__/<파일 이름>/<run_id>.ipynb`에 nbformat 4.5 노트북으로 기록됩니다. `run_id`는 `YYYYMMDD-HHMMSS-<4 hex>`(UTC)입니다. 노트북 `metadata.darkpyonix`에는 `run_id`, `kernel_id`, `file`, `file_sha256`, `mode`, `params`, `status`, `started_at`, `ended_at`, `python`, `host`가 들어갑니다. 셀마다 `metadata.darkpyonix`에 `index`, `type`, `title`, `source_sha256`, `status`, `started_at`, `ended_at`이 들어갑니다.
- 수용 기준: 실행 기록이 `nbformat.validate`를 통과합니다(테스트 환경에 nbformat이 있을 때). 표준 라이브러리 `json`으로 읽은 구조가 위 필드를 모두 가집니다.
- 테스트: `test_fr_r1_run_log_is_valid_nbformat`

### FR-R2 실행 중 저장 — `Agreed`
커널은 실행 중에도 기록을 최대 1초 간격으로 원자적으로(임시 파일 → `os.replace`) 다시 씁니다. `index.json`에는 최신순 실행 요약(`run_id`, `status`, `started_at`, `ended_at`, `mode`)을 둡니다.
- 수용 기준: 실행 중에 커널을 `SIGKILL`로 죽여도 기록 파일은 유효한 JSON이고, 죽기 1초 전까지의 출력이 들어 있습니다. 상태는 `running`으로 남고, 다음 커널이 그 파일을 열면 `crashed`로 바꿉니다.
- 테스트: `test_fr_r2_log_survives_kernel_kill`

### FR-R3 매직 변수 `__runs__` — `Agreed`
커널 네임스페이스에는 `__runs__` 객체가 있습니다.

| 표현 | 값 |
|---|---|
| `__runs__.current` | 진행 중인 실행의 노트북 dict, 없으면 `None` |
| `__runs__.latest` | 가장 최근에 끝난 실행의 노트북 dict, 없으면 `None` |
| `__runs__[run_id]`, `__runs__[-1]` | 해당 실행의 노트북 dict |
| `__runs__.list(limit=20)` | 최신순 요약 목록 |
| `__runs__.dir` | 기록 폴더 경로(str) |

- 수용 기준: 두 번째 실행의 셀에서 `__runs__.latest["metadata"]["darkpyonix"]["run_id"]`가 첫 번째 실행의 ID입니다. 반환값은 `json.dumps`로 직렬화됩니다.
- 테스트: `test_fr_r3_runs_magic_exposes_logs_as_json`

### FR-R4 기록과 셀 맵핑 — `Agreed`
매니저의 `GET /kernels/{id}/document`는 파일의 셀 목록에 최신 실행의 출력을 붙여서 돌려줍니다. 맵핑 순서는 `id` → `source_sha256` → `index`이고, 소스 해시가 다르면 `stale: true`로 표시합니다.
- 수용 기준: 실행 뒤 셀 하나를 고치면 그 셀만 `stale: true`이고 출력은 남아 있습니다.
- 테스트: `test_fr_r4_document_maps_latest_outputs_and_marks_stale`

### FR-R5 큰 출력 — `Agreed`
셀 하나의 스트림 출력이 기록 안에서 `DARKPYONIX_RUN_OUTPUT_LIMIT`(기본 16 MiB)를 넘으면, 넘는 부분은 `<run_id>.cell<index>.log`에 이어 쓰고 노트북에는 그 사실을 알리는 스트림 한 줄을 남깁니다. 실시간 이벤트에는 제한이 없습니다.
- 수용 기준: 20 MiB를 출력하는 셀의 기록 노트북이 17 MiB를 넘지 않고, 사이드카 로그와 합치면 전체 출력이 됩니다.
- 테스트: `test_fr_r5_oversized_stream_spills_to_sidecar`

## 5. 발견 (D)

### FR-D1 멀티캐스트 발견 — `Agreed`
PROTOCOL §2를 구현합니다. 매니저의 query에 같은 사용자의 모든 커널이 200 ms 안에 응답합니다.
- 수용 기준: 커널 세 개를 띄우고 query 하나를 보내면 세 개의 announce를 받습니다. 다른 `user_tag`의 query에는 응답하지 않습니다.
- 테스트: `test_fr_d1_query_finds_all_kernels`, `test_fr_d1_other_user_tag_is_ignored`

### FR-D2 등록 파일 보조 — `Agreed`
커널은 announce 본문을 `kernels/<kernel_id>.json`에 둡니다. 매니저는 멀티캐스트 결과와 등록 파일을 합치되, `pid`가 살아 있지 않은 등록은 지웁니다. `DARKPYONIX_DISCOVERY=registry`이면 등록 파일만 씁니다.
- 수용 기준: 멀티캐스트를 끈 상태에서도 매니저가 커널을 찾습니다. `kill -9`로 죽은 커널의 등록은 다음 발견에서 사라집니다.
- 테스트: `test_fr_d2_registry_fallback_finds_kernels`, `test_fr_d2_stale_registry_is_pruned`

## 6. 런타임 API와 파일 형식 (F)

### FR-F1 셀 파서 — `Agreed`
FORMAT §2의 문법(프리앰블, 셀 표식, 제목, 타입, 메타데이터, 셀 식별)을 파싱합니다. 파서는 커널, 매니저, 런타임 API가 공유하며 표준 라이브러리만 씁니다.
- 수용 기준: `docs/examples/darkpyonix_format.py`를 파싱하면 프리앰블 1개와 셀 22개(code 11, markdown 2, binding 2, argparse·shell·parallel·concurrent·cinterop·cppinterop·rustinterop 각 1)가 나오고, 타입, 제목, `@width` 메타데이터, `concorrunt`→`concurrent` 별칭이 FORMAT대로 나옵니다. 파싱 후 다시 직렬화하면 원문과 바이트 단위로 같습니다.
- 테스트: `test_fr_f1_reference_file_parses`, `test_fr_f1_parse_serialize_roundtrip`

### FR-F2 `darkpyonix.markdown` — `Agreed`
FORMAT §3.2. 커널 안에서는 `text/markdown` `display_data`를 내고(`silent=True`이면 기록에 남기지 않음), 커널 밖에서는 아무것도 하지 않습니다. 모르는 키워드 인자는 경고만 남깁니다.
- 테스트: `test_fr_f2_markdown_in_kernel_and_plain_python`

### FR-F3 `darkpyonix.params` — `Agreed`
FORMAT §3.3. 값의 우선순위는 실행 요청 `params` → 명령줄 `--name` → `default`입니다.
- 수용 기준: `choices`와 정수 `default`면 인덱스로 고르고, `range`를 벗어난 값은 `ValueError`입니다. `python file.py --model_id swin_t`가 `"swin_t"`를 냅니다.
- 테스트: `test_fr_f3_params_precedence_and_validation`

### FR-F4 `darkpyonix.binding` — `Agreed`
FORMAT §3.4. 이슈 #6의 참조 구현을 따르되, `binding` 데코레이터만 벗기고 다른 데코레이터는 보존합니다.
- 수용 기준: `[code]` 셀 변수를 참조하는 binding 클래스 본문은 `NameError`를 냅니다. import한 이름과 앞선 binding은 보입니다.
- 테스트: `test_fr_f4_binding_cannot_see_code_cell_variables`

### FR-F5 일반 파이썬과 같은 동작 — `Agreed`
노트북 파일을 `python file.py`로 실행한 결과(표준 출력, 종료 코드)가 커널 전체 실행의 스트림 출력과 같습니다. 마크다운 출력과 `display`의 MIME 번들은 이 비교에서 뺍니다.
- 테스트: `test_fr_x1_run_all_matches_plain_python` (FR-X1과 공유)

### FR-F6 `darkpyonix.run_command` — `Agreed`
셸 명령을 하위 프로세스로 실행하고 출력을 줄 단위로 스트림 출력으로 보냅니다. `check=True`이면 실패 시 `CalledProcessError`입니다. 인터럽트가 오면 하위 프로세스 그룹에 SIGINT를 전달합니다.
- 테스트: `test_fr_f6_run_command_streams_and_forwards_interrupt`

## 7. 매니저 (M)

### FR-M1 HTTP API — `Agreed`
매니저는 [api/manager.openapi.yaml](api/manager.openapi.yaml)의 경로를 모두, 그리고 그 경로만 냅니다. 이벤트 스트림은 SSE(`text/event-stream`)이고 SSE `id`는 커널의 `seq`입니다. `Last-Event-ID` 헤더나 `since` 쿼리로 이어 받습니다.
- 테스트: `test_fr_m1_served_schema_matches_spec` (NFR-M3), 각 경로의 동작 테스트

### FR-M2 커널 시작은 멱등 — `Agreed`
`POST /kernels {path}`는 그 파일의 커널이 살아 있으면 그 커널을 `200`으로, 없으면 새로 띄워서 `201`로 돌려줍니다. 커널이 announce를 낼 때까지 최대 10초를 기다립니다.
- 테스트: `test_fr_m2_start_kernel_is_idempotent`

### FR-M3 임시 모드 수명 — `Agreed`
임시 매니저는 `127.0.0.1`의 임의 포트에 리슨합니다. `managers/<pid>.json`(0600)에 주소와 토큰을 쓰고, 열린 SSE 스트림이 없고 HTTP 요청도 없는 상태가 `idle_timeout`(기본 120초) 동안 이어지면 스스로 끝납니다. 끝날 때 등록을 지우고 커널은 건드리지 않습니다.
- 테스트: `test_fr_m3_ephemeral_manager_exits_when_idle_and_kernels_remain`

### FR-M4 전용 모드 — `Agreed`
`darkpyonix manager --dedicated`는 유휴 종료 없이 돌고, 설정한 호스트·포트에 리슨하고, 마스터 토큰과 공유 토큰으로 인증합니다. 토큰은 해시로만 `manager.db`(SQLite)에 저장합니다.
- 테스트: `test_fr_m4_dedicated_manager_requires_token`

### FR-M5 매니저 여러 개 공존 — `Agreed`
같은 사용자의 매니저 여러 개가 같은 커널에 동시에 붙을 수 있고, 각자 같은 이벤트를 받습니다.
- 테스트: `test_fr_m5_two_managers_share_one_kernel`

## 8. CLI (C)

### FR-C1 매니저 찾기 — `Agreed`
`darkpyonix` CLI는 `managers/*.json`에서 살아 있는 매니저를 고르고, 없으면 임시 매니저를 띄웁니다. 에이전트가 기존 매니저의 토큰을 몰라도 같은 OS 사용자라면 그대로 동작합니다.
- 테스트: `test_fr_c1_cli_spawns_manager_when_none_is_running`

### FR-C2 명령 — `Agreed`

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
| `darkpyonix manager [--dedicated] [--host H] [--port P] [--idle-timeout S]` | 매니저 실행 |

- 수용 기준: `darkpyonix run a.py`를 두 번째로 실행하면 종료 코드 75와 함께 현재 실행 정보와 `--queue`/`stop` 안내를 출력합니다.
- 테스트: `test_fr_c2_cli_commands`, `test_fr_c2_second_run_exits_75_with_hint`

## 9. 인증과 공유 (A)

### FR-A1 커널 인증 — `Agreed`
PROTOCOL §3.2의 HMAC 도전-응답입니다. 사용자 키가 없으면 처음 쓰는 쪽이 0600으로 원자적으로 만듭니다.
- 테스트: `test_fr_a1_wrong_key_is_rejected`

### FR-A2 매니저 토큰 — `Agreed`
모든 HTTP 요청은 `Authorization: Bearer <token>`이 필요합니다. 헤더를 붙일 수 없는 SSE(`EventSource`)와 공유 링크만 `?token=`을 받습니다. `/health`만 인증 없이 열립니다.
- 테스트: `test_fr_a2_requests_without_token_are_401`

### FR-A3 공유 권한 — `Agreed`
공유 토큰은 커널(파일)마다 발급하고 권한은 아래와 같습니다(2025 설계 유지).

| 권한 | 셀 코드 | 실행 기록·출력 | 실행·인터럽트 | 종료·공유 관리 |
|---|---|---|---|---|
| `viewer1` | ✓ | | | |
| `viewer2` | ✓ | ✓ | | |
| `viewer3` | ✓ | ✓ | ✓ | |
| `admin`(마스터) | ✓ | ✓ | ✓ | ✓ |

- 테스트: `test_fr_a3_permission_matrix`

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
OpenAI 계정 로그인을 지원하고, Codex 토큰 사용량 외에 Chat 사용량도 쓸 수 있는 페이지를 둡니다. 외부 서비스에 이 방식의 로그인이 열려 있는지부터 확인해야 합니다(PROJECT Q2).

## 11. 비기능 요구사항

### NFR-K1 인터프리터 범위 — `Agreed`
커널과 런타임 API는 CPython 3.8 이상 모든 마이너 버전에서 동작합니다. 테스트는 그 기계에 있는 모든 인터프리터(`DARKPYONIX_TEST_PYTHONS`, 기본은 PATH에서 찾은 `python3.*`)로 커널 테스트를 돌립니다.
- 측정 기록: (구현 후 기입)

### NFR-K2 표준 라이브러리 전용 — `Agreed`
`kernel/darkpyonix/kernel/`, `kernel/darkpyonix/*.py`, `kernel/darkpyonix/format/`의 모든 import가 표준 라이브러리임을 테스트가 AST로 확인합니다(`sys.stdlib_module_names`, 3.8용 고정 목록 병행).
- 테스트: `test_nfr_k2_kernel_imports_stdlib_only`

### NFR-K3 출력 오버헤드 — `Agreed`
`print`를 100,000번 하는 셀의 실행 시간이 같은 인터프리터의 일반 실행 대비 1.5배를 넘지 않습니다. 구독자가 느려도 메인 스레드가 막히지 않습니다(출력 큐 상한을 넘으면 기록은 계속하되 실시간 이벤트를 합칩니다).
- 측정 기록: (구현 후 기입)

### NFR-K4 시작 시간 — `Agreed`
커널 시작(프로세스 실행부터 announce까지)은 기준 기계(맥미니 M 시리즈)에서 300 ms 이하입니다.
- 측정 기록: (구현 후 기입)

### NFR-M1 발견 지연 — `Agreed`
매니저의 커널 목록 조회는 커널 20개에서 300 ms 이하입니다.

### NFR-M2 이벤트 지연 — `Agreed`
커널의 출력이 매니저 SSE 구독자에게 도달하기까지 p99 100 ms 이하입니다(스트림 병합 50 ms 포함).

### NFR-M3 문서와 코드의 일치 — `Agreed`
매니저가 내는 OpenAPI 스키마의 경로·메서드·응답 코드가 `docs/api/manager.openapi.yaml`과 같습니다. 테스트가 비교합니다.

### NFR-H1 종단 간 암호화 — `Draft`
허브는 중계하는 내용을 볼 수 없습니다. 기기 사이의 세션 키는 허브를 거치지 않고 합의합니다.

## 12. 프로토콜 요구사항

### PR-1 DKP/1 프레임 — `Agreed`
PROTOCOL §3.1. 64 MiB를 넘는 프레임은 거절합니다.

### PR-2 핸드셰이크 — `Agreed`
PROTOCOL §3.2. 5초 제한, 상수 시간 비교.

### PR-3 이벤트 재전송 — `Agreed`
PROTOCOL §3.4. 링 버퍼와 `replay_truncated`.

### PR-4 호환성 — `Agreed`
모르는 필드는 무시하고, 필드 추가는 버전을 올리지 않습니다. 의미를 바꾸는 변경은 `dkp` 버전을 올립니다.
