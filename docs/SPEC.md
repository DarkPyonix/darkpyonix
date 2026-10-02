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

### FR-F1 셀 파서 — `Done`
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

### FR-A1 커널 인증 — `Done`
PROTOCOL §3.2의 HMAC 도전-응답입니다. 사용자 키가 없으면 처음 쓰는 쪽이 0600으로 원자적으로 만듭니다.
- 테스트: `test_fr_a1_wrong_key_is_rejected`, `test_fr_a1_hello_carries_identity_and_nonce`, `test_fr_a1_user_key_is_created_once_with_0600`

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

작업별 최소 권한: 커널·실행 기록 조회 `viewer1`(실행 기록과 출력은 `viewer2`부터), 네임스페이스 조회 `viewer2`, 실행·인터럽트·대기 실행 취소 `viewer3`, 재시작·종료·공유 관리·새 커널 시작 `admin`. 공유 토큰은 한 커널에만 묶이며, 다른 커널을 가리키면 `403`이 아니라 `404`입니다.
- 테스트: `test_fr_a3_permission_matrix`

## 10. 허브 (H)

전송은 iroh 1.x로 정했습니다(PROJECT Q1, 2026-10-03, 조건부: ember SPEC NFR-N1을 못 맞추면 직접 구현을 검토). 그래서 허브는 직접 만든 랑데부·중계 대신 iroh가 이미 쓰는 프로토콜을 그대로 받는 서버입니다. 기기는 iroh 엔드포인트이고, 기기 ID는 그 엔드포인트 ID(ed25519 공개 키, 소문자 hex 64자)입니다. ChatGPT 플랜 로그인은 로컬 호스팅 앱에만 열려 있어서 허브에 넣지 않습니다(Q2, FR-H6).

허브는 Rust 단일 바이너리 `darkpyonix-hub`(`hub/server/`)이고, nginx 없이 TLS·HTTP API·iroh 릴레이·정적 파일을 한 프로세스가 맡습니다(INTENT D10과 같은 원칙). 포트는 TCP 443(API, `/relay`, `/pkarr`, ash), TCP 80(`/generate_204`와 HTTPS 리디렉트), UDP 7842(QUIC 주소 발견, QAD)입니다. 계약은 [api/hub.openapi.yaml](api/hub.openapi.yaml)이고, 허브가 답하는 경로·메서드·상태 코드가 그 문서와 같음을 테스트가 확인합니다(`test_hub_every_operation_answers_with_a_documented_status`, NFR-M3와 같은 방식).

전송 계층 교체 가능성: 허브가 iroh에 묶이는 곳은 릴레이(`/relay`, QAD)와 주소 레코드 형식(pkarr 서명 패킷)뿐입니다. 기기 등록, 계정, 공유, 이름은 "ed25519 공개 키 하나 = 기기"라는 가정만 씁니다. 직접 구현으로 바꾸면 그 두 곳만 바꿉니다.

**인증 모델(11월 범위).** 로그인(FR-H6)이 없으므로 계정은 허브가 발급하는 계정 토큰으로 식별합니다. `POST /v1/accounts`가 계정과 계정 토큰을 만들고(운영자가 가입 비밀값을 설정하면 그 값이 있어야 함), 계정 토큰으로 기기를 등록하면 기기마다 기기 토큰이 나옵니다. 토큰은 SHA-256 해시로만 저장합니다. 계정 토큰은 메인 서버(ember server)가 보관하고, 새 컴퓨터는 메인 서버를 거쳐 등록합니다.

### FR-H1 기기 등록 — `Agreed`
기기는 iroh 엔드포인트 ID로 등록하고, 등록할 때 허브가 낸 일회용 챌린지(5분 유효)에 서명해 개인 키를 가졌음을 증명합니다. 서명하는 메시지는 `darkpyonix-hub/v1/register\n<account_id>\n<challenge>`입니다. 기기는 계정 하나에만 속하고, 기기 목록과 조회는 같은 계정 안에서만 보입니다. 기기를 지우면 그 키는 폐기되어 다시 등록할 수 없고, 릴레이에 붙어 있던 연결은 바로 끊깁니다.
- 수용 기준: 실제 iroh 엔드포인트 둘을 등록하면 계정의 기기 목록에 두 엔드포인트 ID가 나옵니다. 서명이 틀리거나, 챌린지를 다시 쓰거나, 다른 계정 소속 챌린지를 쓰면 400입니다. 이미 등록된 키는 409입니다. 다른 계정의 토큰으로는 그 기기가 보이지 않습니다(404). 지운 기기의 토큰은 401입니다.
- 테스트: `test_fr_h1_register_two_iroh_endpoints`, `test_fr_h1_registration_requires_key_possession`, `test_fr_h1_devices_are_scoped_to_their_account`, `test_fr_h1_removed_device_is_revoked`

### FR-H2 주소 디렉터리와 발견 — `Agreed`
기기는 현재 iroh 주소(릴레이 URL과 직접 주소)를 자기 키로 서명한 pkarr 패킷으로 허브에 올리고, 같은 계정의 기기는 엔드포인트 ID만으로 서로의 주소를 찾습니다.
- 프로토콜: iroh의 pkarr 릴레이 HTTP 프로토콜을 그대로 씁니다. `PUT /pkarr/<z32 키>`로 올리고 `GET /pkarr/<z32 키>`로 받습니다. 그래서 iroh의 기본 `PkarrPublisher`·`PkarrResolver`를 `https://darkpyonix.dev/pkarr?token=<기기 토큰>`에 그대로 붙일 수 있습니다(ember 전송 크레이트가 따로 구현할 것이 없음). 같은 내용을 JSON으로 보는 `GET /v1/devices/{endpoint_id}/addresses`도 둡니다.
- 받는 쪽 검사: 서명이 맞고, 키가 폐기되지 않은 등록 기기이고, 타임스탬프가 저장된 것보다 새 것만 받습니다(아니면 400/403/409).
- 조회 범위: `GET`은 같은 계정의 기기 토큰이나 계정 토큰이 있어야 합니다. 조회가 공개되지 않으므로 기기는 직접 주소까지 올려도(`AddrFilter::unfiltered`) 공인 IP가 계정 밖으로 새지 않습니다.
- DNS 발견(iroh-dns-server, `_iroh.<z32>.<도메인>` TXT)은 쓰지 않습니다. DNS 질의에는 계정 범위를 걸 수 없고, 권한 DNS 서버와 NS 위임을 따로 운영해야 하며, 우리 기기는 모두 허브와 HTTPS로 말하므로 얻는 것이 없습니다. 저장하는 레코드가 같은 서명 패킷이라 나중에 필요하면 DNS 앞단만 더할 수 있습니다.
- 수용 기준: 두 엔드포인트가 기본 `PkarrPublisher`로 주소를 올리고, 한쪽이 기본 `PkarrResolver`로 상대의 엔드포인트 ID만 가지고 연결합니다. 등록되지 않은 키의 `PUT`은 403, 토큰 없는 `GET`은 401, 다른 계정의 `GET`은 404입니다.
- 테스트: `test_fr_h2_publish_and_resolve_with_stock_iroh_lookup`, `test_fr_h2_directory_rejects_unregistered_and_foreign`

### FR-H3 중계 — `Agreed`
허브는 iroh-relay 서버 크레이트의 릴레이 서비스를 같은 프로세스에서 돌리고(`GET /relay` WebSocket), iroh가 쓰는 보조 서비스도 함께 냅니다. HTTPS 지연 프로브 `GET /ping`, 캡티브 포털 검사 `GET /generate_204`, UDP 7842의 QUIC 주소 발견(QAD)입니다. iroh 1.x는 STUN을 쓰지 않고 QAD로 공인 주소를 알아내므로 STUN 서버는 두지 않습니다.
- 접근 정책: 릴레이 핸드셰이크가 증명한 엔드포인트 ID가 폐기되지 않은 등록 기기이면 받습니다. 등록 기기가 아니면 유효한 손님 통행권(FR-H4가 발급, `Authorization: Bearer` 또는 `?token=`)이 있을 때만 받습니다. 그 밖에는 거절합니다.
- 수용 기준: 두 등록 기기가 IP 전송을 끈 릴레이 전용 모드로 우리 릴레이를 거쳐 연결하고 데이터를 주고받습니다(선택된 경로가 릴레이). 같은 두 기기가 루프백에서 직접 경로로도 연결합니다. 등록되지 않은 엔드포인트는 릴레이가 거절해 연결하지 못합니다. 루프백 처리량과 왕복 지연을 측정해 여기에 적습니다.
- 테스트: `test_fr_h3_relay_only_connection_through_hub`, `test_fr_h3_direct_connection_on_loopback`, `test_fr_h3_relay_rejects_unregistered_endpoint`, `test_fr_h3_relay_throughput_and_latency`
- 측정 기록: (구현 후 기입)

### FR-H4 ash 호스팅과 공유 링크 — `Agreed`
`https://darkpyonix.dev/ash/`에서 공식 ash 뷰어를 호스팅하고, 공유 링크 `https://darkpyonix.dev/s/<share_id>#<token>`을 그 공유를 연 기기로 이어 줍니다. 공유 토큰은 URL 조각(`#` 뒤)에 있어서 허브로 가지 않습니다. 권한 검사는 끝단의 전용 매니저가 합니다(FR-A3).
- 기기는 `POST /v1/shares`로 자기 공유를 게시하고, 누구나 `GET /v1/shares/{share_id}`로 그 공유를 연 기기의 엔드포인트 ID와 릴레이 URL, 10분짜리 손님 릴레이 통행권을 받습니다. ash(브라우저 iroh, 릴레이 전용)는 그 통행권으로 릴레이에 붙어 기기에 연결합니다. `GET /s/{share_id}`는 ash 뷰어 페이지를 냅니다(뷰어가 나오기 전까지는 자리표시 페이지).
- 수용 기준: 게시한 공유가 기기 ID와 통행권으로 풀립니다. 그 통행권을 든 미등록 엔드포인트는 릴레이를 거쳐 공유한 기기에 연결하고, 통행권이 없으면 거절됩니다. 게시를 지우면 404입니다. `/s/{share_id}`와 `/ash/`가 HTML을 냅니다.
- 테스트: `test_fr_h4_share_resolves_to_hosting_device`, `test_fr_h4_guest_reaches_share_host_through_relay_with_pass`, `test_fr_h4_viewer_pages_are_served`

### FR-H5 HTTPS 이름 — `Draft`
메인 서버가 `https://<name>.darkpyonix.dev` 주소와 공인 인증서를 얻게 합니다(모바일 웹뷰의 보안 컨텍스트 요건, ember FR-N4).
- 방식 비교:
  - (A) **ACME DNS-01을 허브가 대신 게시.** 메인 서버가 자기 개인 키로 인증서를 받고, 허브는 `_acme-challenge.<name>.darkpyonix.dev` TXT만 게시합니다. TLS가 메인 서버에서 끝나므로 허브는 평문을 보지 않습니다(NFR-H1 유지). 대신 공인 IP가 없는 기기에 브라우저가 직접 닿지 못하므로, ember 앱이 루프백 포워더(127.0.0.1 → iroh)로 그 이름을 열어야 합니다(이름의 A 레코드를 127.0.0.1로 둘 수도 있음).
  - (B) **허브가 TLS를 끝내는 HTTPS 엣지**(rustunnel 방식). 아무 브라우저나 닿지만 허브가 평문을 봅니다. NFR-H1을 깨므로 쓰지 않습니다.
  - (C) **SNI 패스스루 엣지.** 허브가 ClientHello의 SNI만 읽고 TLS 바이트를 그대로 iroh로 기기에 넘깁니다. 인증서는 (A)로 받은 기기의 것이라 평문은 여전히 기기에서만 보입니다. 앱 없는 브라우저에서도 닿지만 공개 트래픽 대역폭이 허브에 걸립니다.
- 결정: (A)를 기본으로 하고, 앱 없는 브라우저 접근이 필요해지면 (C)를 더합니다. (B)는 쓰지 않습니다. 허브는 이름을 메인 서버 기기에 예약하고(`PUT /v1/names/{name}`), 그 기기가 요청한 TXT 값을 DNS 공급자로 게시합니다(`PUT /v1/names/{name}/acme-challenge`). DNS 공급자는 교체 가능한 인터페이스 뒤에 둡니다.
- `Draft`인 이유: darkpyonix.dev의 DNS 공급자(그 API)와 (C)의 필요 여부가 정해지지 않았습니다. 이름 예약과 TXT 게시 API는 메모리 공급자로 구현·시험합니다.
- 테스트(부분): `test_fr_h5_name_reservation_and_acme_txt`

### FR-H6 로그인 — `Draft` (11월 범위 밖)
OpenAI 계정 로그인과 Chat 사용량 페이지입니다. ChatGPT 플랜 사용("Sign in with ChatGPT")은 오픈소스·로컬 호스팅 앱에만 열려 있고 원격 호스팅은 별도 신청과 승인이 필요합니다. 그래서 플랜 사용은 ember server가 맡고(ember SPEC), 허브의 로그인은 승인을 받은 뒤에 다룹니다(PROJECT Q2, M4 비고). 11월에는 위의 계정 토큰 모델을 씁니다.

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
매니저가 실제로 답하는 경로·메서드·응답 코드가 `docs/api/manager.openapi.yaml`과 같습니다. 구현 언어와 무관하게, 테스트는 모든 연산을 HTTP로 불러 문서에 있는 상태 코드로만 답하는지 확인합니다(`test_nfr_m3_every_operation_answers_with_a_documented_status`). 예외: API 문서 페이지(`/docs/`, `/docs/manager.openapi.yaml`, `/docs/hub.openapi.yaml`)는 계약 밖의 정적 파일입니다.

### NFR-H1 종단 간 암호화 — `Agreed`
허브는 중계하는 내용을 볼 수 없습니다. 기기 사이 연결은 iroh의 QUIC TLS 1.3이고, 상대 인증은 양쪽의 ed25519 엔드포인트 키로 끝단끼리 합니다. 세션 키는 허브를 거치지 않고 합의하며, 릴레이는 암호문 데이터그램만 전달합니다. 허브가 TLS를 끝내는 구성(FR-H5 방식 B)은 두지 않습니다.
- 수용 기준: 릴레이 전용 연결로 알려진 평문 표식을 보낼 때, 클라이언트와 허브 사이의 바이트(허브까지 TLS 없이 평문 HTTP 릴레이로 둔 경우에도)에 그 표식이 나타나지 않고, 상대 끝단에서는 그대로 받습니다.
- 테스트: `test_nfr_h1_relay_sees_only_ciphertext`

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
