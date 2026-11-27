# darkpyonix.dev 허브 Worker

`api.darkpyonix.dev`의 Cloudflare Worker입니다. 요구사항은 [docs/SPEC.md](../../../docs/SPEC.md) §10, 계약은 [docs/api/hub.openapi.yaml](../../../docs/api/hub.openapi.yaml)입니다. 허브는 릴레이를 운영하지 않습니다(INTENT D23).

## 개발

```sh
npm ci
npm run typecheck
npm test            # workerd 안에서 도는 vitest, D1은 시험 파일마다 새로 만듭니다
```

## 처음 배포

준비물은 SPEC §10 "사용자 준비물"에 있습니다(Cloudflare 무료 플랜, GitHub OAuth App). 아래 명령은 이 폴더에서 실행합니다.

```sh
npx wrangler login
npx wrangler d1 create darkpyonix-hub             # 나온 database_id를 wrangler.toml에 넣습니다
npx wrangler r2 bucket create darkpyonix-notebooks
npm run migrate:remote
npx wrangler secret put GITHUB_CLIENT_ID
npx wrangler secret put GITHUB_CLIENT_SECRET
npx wrangler secret put OPERATOR_SECRET           # 직접 만든 무작위 값
npx wrangler secret put CF_API_TOKEN              # darkpyonix.dev 존의 Zone → DNS → Edit만
npx wrangler secret put CF_ZONE_ID
npm run deploy
```

## 배포 뒤 점검

```sh
npm run check:live                                # https://api.darkpyonix.dev
npm run check:live -- --url http://127.0.0.1:8787 # 로컬 `npm run dev`
```

화면에 나온 링크를 열어 GitHub로 로그인하고 코드를 승인하면, 스크립트가 기기 등록, 주소 게시·조회·감시, 기기 삭제까지 확인하고 `PASS`나 실패한 단계를 알립니다. 한 번 쓰고 버리는 `client` 기기로 들어가서 끝나면 스스로 지웁니다. 단계와 수용 기준은 SPEC §10 "살아 있는 허브 점검"에 있습니다. 로컬 Worker에서 승인하려면 로컬 D1에 계정이 있어야 하므로(GitHub 로그인), 로컬에서는 보통 `npm test`의 `live_check.test.ts`로 충분합니다.

## 시험 벡터

`test/vectors/rust.json`은 실제 기기가 쓰는 Rust 코드(iroh-base 키, iroh-dns 서명 패킷)가 만든 바이트입니다(FR-H1, FR-H2). 만드는 프로그램은 `test/vectors/gen/`이고 빌드에 들어가지 않습니다. 다시 만들 때만 그 폴더의 `Cargo.toml` 머리말대로 실행합니다.
