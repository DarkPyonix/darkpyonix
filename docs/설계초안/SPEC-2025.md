### DarkPyonix 서버 구성
#### Kernel Manager
- 커널 매니저는 실제 클라이언트의 요청을 받는 서버
- 커널 매니저는 클라이언트의 요청에 따라 커널을 켜거나 끄거나 중지시킴
- 커널은 커널 매니저에 의해 subprocess 형태로 런치됨
- 커널과 커널 매니저는 자식-부모 관계이되, 커널이 죽었다고 해서 커널 매니저가 죽지는 않음
- 커널 매니저가 죽으면 모든 커널은 죽음

#### Kernel
- 실제 파이썬 코드가 돌아갈 커널
- 커널 매니저와 별도의 프로세스
- 커널 매니저로부터 클라이언트 소켓 파일 디스크립터를 받아 클라이언트와 직접 소통

### DarkPyonix 커널 매니저 API 명세서
# API 엔드포인트 문서

## 인증 및 기본 기능

| 기능 | HTTP 메서드 | URI | Request | Response |
|------|-------------|-----|---------|----------|
| 로그인 | GET | /auth | 비밀번호를 세션에 넣어서 진행 | |
| 비밀번호 재설정 | PUT | /auth/password | 토큰을 인증하고, 새 비밀번호 등록 | |
| 마스터 토큰 재설정 | PUT | /auth/tokens/master | 비밀번호를 인증하고, 새 토큰 발행 | |
| 공유 토큰 재설정 | PUT | /auth/tokens/shared/{token_type} | 마스터 토큰을 세션에 넣어 인증하고, 각 타입별 공유 토큰 생성 | |
| vscode 포워딩 | GET/WS | /code/~ | | |

## 커널 관리

| 기능 | HTTP 메서드 | URI | Request | Response |
|------|-------------|-----|---------|----------|
| 커널 생성 | POST | /kernels/{kernel_id} | kernel_id: None→id | 커널 파이썬 위치 지정 필요 |
| 커널 조회 | GET | /kernels/{kernel_id} | | |
| 커널 정지 | PATCH | /kernels/{kernel_id} | status: running → interrupted | |
| 커널 삭제 | DELETE | /kernels/{kernel_id} | kernel_id : id → None | |

## 셀 관리

| 기능 | HTTP 메서드 | URI | Request | Response |
|------|-------------|-----|---------|----------|
| 셀 생성 | POST | /kernels/{kernel_id}/cells | id: str<br>(index: int 클라이언트에서 전송)<br>cell_type: str<br>source: str<br>execution_count: int<br>outputs: List[dict]<br>locked: bool = False<br>authorized_to: Optional[str] = None<br>focused_by: List[str] = []<br>status: str = "none" \| "running" \| "completed" \| "failed"<br>auto_run<br>collapsed<br>title<br>layout - horizontal |  |
| 셀 동기화 | GET | /kernels/{kernel_id}/cells?nickname={device_name} | 같은 토큰이더라도 어느 클라이언트인지 닉네임으로 구분 | |
| 셀 알람 | GET | /kernels/{kernel_id}/cells/{cell_id}?watch=status&client_id={uuid} | | |
| 셀 실행 | PATCH | /kernels/{kernel_id}/cells/{cell_id} | status: none → pending |  |
| 셀 실행 취소 | PATCH | /kernels/{kernel_id}/cells/{cell_id} | status: pending → none | |
| 셀 클릭 | PATCH | /kernels/{kernel_id}/cells/{cell_id} | focus_by.append(user_id) |  |
| 셀 락 | PATCH | /kernels/{kernel_id}/cells/{cell_id} | locked: False → True<br>authorized_to: None → 유저명 |  |
| 셀 언락 | PATCH | /kernels/{kernel_id}/cells/{cell_id} | locked: True → False<br>authorized_to: 유저명 → None | |
| 셀 삭제 | DELETE | /kernels/{kernel_id}/cells/{cell_id} | 셀 리스트에서 해당 cell_id를 갖고 있는 셀 삭제 |  |
