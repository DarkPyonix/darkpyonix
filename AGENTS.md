# AGENTS.md

Guidance for working in this repository.

## Project

`darkpyonix` (repo `DarkPyonix/darkpyonix`, checked out as `darkpyonix-core`) is the
DarkPyonix kernel stack:

- `darkpyonix/kernel/darkpyonix/kernel/`: the **file-bound kernel**. One kernel per source
  file, independent of any manager, standard library only, runnable by any Python 3.8+
  interpreter without installation.
- `darkpyonix/manager/`: the **kernel manager** and the `darkpyonix` CLI, a Rust workspace
  (INTENT D10). A disposable HTTP front for kernels. It discovers running kernels, launches
  new ones, and serves IDEs, agents and the shared notebook (ash). It embeds the Python kernel
  sources from `darkpyonix/kernel/`. `darkpyonix/kernel/darkpyonix/manager/` is the superseded
  Python prototype.
- `darkpyonix/kernel/darkpyonix/` (top level): the **runtime API** that notebook files import
  (`darkpyonix.markdown`, `darkpyonix.params`, `darkpyonix.binding`, …). Standard library only.
- `hub/`: `darkpyonix.dev`. Rendezvous and relay for machine-to-machine connections, HTTPS,
  and hosting of the official darkpyonix-ash viewer.

The rest of the product lives in sibling repositories: `darkpyonix-ember` (IDE and agent
workbench), `darkpyonix-ash` (shared WASM notebook), `vscode-darkpyonix`,
`vscode-darkpyonix-theme`, `intellij-darkpyonix`. `docs/ARCHITECTURE.md` shows how they fit.

Documents:

- `PROJECT.md`: scope, milestones, open questions.
- `docs/INTENT.md`: why, decisions (D1…), rejected alternatives.
- `docs/SPEC.md`: requirements (`FR-*`, `NFR-*`, `PR-*`) with acceptance criteria.
- `docs/ARCHITECTURE.md`: processes, discovery, data flow across all DarkPyonix components.
- `docs/PROTOCOL.md`: the kernel wire protocol (discovery datagrams and control frames).
- `docs/FORMAT.md`: the `.py` / `.pynb` notebook file format.
- `docs/api/*.openapi.yaml`: the HTTP APIs. `docs/api/index.html` renders them.
- `darkpyonix.mermaid`: the class diagram of the object model.

## Repository root

The root holds exactly these entries:

- `.gitignore`: ignored paths.
- `.github/`: CI workflows (when present).
- `AGENTS.md`: these working agreements.
- `CLAUDE.md`: a pointer to `AGENTS.md`.
- `LICENSE`: MIT.
- `PROJECT.md`: scope, milestones, open questions.
- `README.md`: what the project is and how it is laid out.
- `darkpyonix.mermaid`: the class diagram of the object model.
- `darkpyonix/`: the product code: `kernel/` (Python kernel and runtime API) and `manager/`
  (Rust manager and CLI).
- `docs/`: INTENT, SPEC, ARCHITECTURE, PROTOCOL, FORMAT and the OpenAPI files.
- `hub/`: `darkpyonix.dev` (Cloudflare Worker and relay host).
- `pyproject.toml`: the Python package and pytest configuration.
- `tests/`: the Python test suite.

**Do not add a top-level folder or file without the user's approval.** Propose what you want
to add and why, explain why no existing directory fits, then wait for the answer. Ignored
local work areas (`.scratch/`, `.claude/worktrees/`) are not part of the tree.

## Spec Driven Development

1. **SPEC is the source of truth.** Find the SPEC ID before implementing a behavior. If none
   exists, add or amend the SPEC first, in its own commit.
2. If code and SPEC disagree, fix the code. If the SPEC is wrong, fix the SPEC first and say why.
3. Decision changes go to `docs/INTENT.md` first, then SPEC, then code.
4. When a requirement's acceptance criteria are verified, update its status
   (`Draft` → `Agreed` → `Done`) in the same change that proves it.
5. **The OpenAPI files are part of the SPEC.** An endpoint that is not in
   `docs/api/*.openapi.yaml` does not exist; a test compares the served schema with the file.
6. `darkpyonix.mermaid` is updated in the same change that adds or renames a class it shows.
7. Project docs (README, PROJECT, INTENT, SPEC, ARCHITECTURE, PROTOCOL, FORMAT) are written
   in Korean. Code, code comments, OpenAPI descriptions and this file are in English.

## Test Driven Development

1. **Red, green, refactor.** Failing test first, then the simplest change, then clean up.
2. **Name tests after the requirement**: `test_fr_k3_second_kernel_for_same_file_is_refused`.
3. **No production change without a test that would have caught its absence.**
4. The test and the code that makes it pass go in the same commit; every commit is green.
5. Test through the public surface: the CLI, the HTTP API, the kernel wire protocol, and the
   runtime API a notebook imports. Do not assert on internals the SPEC does not describe.
6. Kernel tests run against every interpreter available on the machine (`NFR-K1`), not only
   the one running the test suite.

## Hard constraints (INTENT)

Never introduce anything that violates these. If a task seems to require it, stop and ask.

- **The kernel and the runtime API import the standard library only.** No third-party import
  at module level or inside functions on the kernel's own code paths. User code may import
  anything; the kernel may not.
- **The kernel must run on Python 3.8 through the newest release** from source, without being
  installed in that interpreter. No syntax or stdlib API newer than 3.8 in kernel code.
- **A kernel never depends on a manager being alive.** Managers come and go; kernels stay.
- **One live kernel per file per machine.** Enforced by the kernel itself, not by a manager.
- **Stopping is interrupting.** DarkPyonix tools never kill a kernel to stop a run. Killing is
  an explicit, separately named operation.
- **A notebook file stays valid Python** and behaves the same under `python file.py`.
- **No pickle, no eval of anything received over the wire.** Frames are JSON.

## Where files go

1. **Everything this project makes stays inside this repository**: worktrees, experiments,
   probes, scratch files, test kernels, build outputs, downloads.
   Not `/tmp`, not a directory beside this checkout, not the home directory.
2. Where inside:
   - worktrees: `.claude/worktrees/<name>/` (ignored by git);
   - throwaway work, probes, downloads, test run directories: `.scratch/<name>/` (ignored);
   - Rust build output: `darkpyonix/manager/target/` and `hub/server/target/` (ignored);
   - experiments worth keeping: ask first (see "Repository root"); there is no top-level
     `experiments/` folder.
3. The one exception is what the product itself writes at run time on a user's machine
   (`~/.darkpyonix/`, `__runs__/` beside a notebook). Tests point those at `.scratch/`
   through `DARKPYONIX_HOME` instead of writing to the real home directory.
4. If a task seems to need a path outside the repository, ask first.

## Sub-agents and builds

- **Coding, research and documentation sub-agents never build** (`cargo build/test/clippy/run`,
  or anything that compiles Rust such as `maturin`).
- **Builds and tests go to one temporary builder sub-agent** that does only that job. Run one
  builder at a time, with `CARGO_BUILD_JOBS=2`. The session (the leader) never builds itself, so
  it stays free for other work. (User rule, 2026-10-03: "빌드 작업 니가 직접 하지 말고 서브
  에이전트 하나 임시로 만들어서 개한테 시켜야지", "니가 작업 붙잡고 있으면 다른 일들도 진행이
  안되잖아".)
- When no builder is used, push the branch and let CI run it.
- Never share one `CARGO_TARGET_DIR` between worktrees: path crates from different worktrees
  overwrite each other's artifacts.
- Python test runs are allowed in sub-agents; keep them inside `.scratch/` and kill every process
  they start.

## Git

- Branches: `develop` (integration, where work lands) and `main` (protected, default). The
  remote keeps only `main`, `develop` and `release` as long-lived branches.
- Work branches are named `feat/<topic>`, made off `develop` in a worktree under
  `.claude/worktrees/<name>/`.
- Merge with `gh pr merge --delete-branch`, then remove the local branch and its worktree.
- Merged branches are deleted periodically. A branch whose history is worth keeping gets an
  `archive/<name>` tag first; verify the tag equals the branch head
  (`git rev-parse archive/<name>` = `git rev-parse origin/<branch>`) before deleting it.
- **Push right after every commit.** Never push to `main` directly. Force-push only with the
  user's confirmation.
- **New features:** search issues first
  (`gh issue list -R DarkPyonix/darkpyonix --state all --search "<keyword>"`). If none fits,
  write one with completion criteria. Commit on a feature branch off `develop`, push, and open
  a PR into `develop` with `Closes #<N>` in the body. Merge only through that PR.
  `develop` is not the default branch, so close the issue by hand after merging:
  `gh issue close <N> --comment "Landed via #<PR>"`.
- Doc-only changes may be committed on `develop` and pushed directly.
- Subject format `<Type>: <imperative summary>` with `Feat`, `Fix`, `Refactor`, `Docs`,
  `Test`, `Chore`. Reference SPEC IDs when relevant.
- **No `Co-Authored-By` trailer and no "Generated with" line** in commits or PR bodies.
- One logical change per commit. Do not commit `.DS_Store`, `__pycache__`, `.scratch/`,
  `__runs__/` produced by tests, or local databases.

## Verification

- A milestone is complete only when its SPEC acceptance criteria pass. Report failures with the
  actual output; never mark an item `Done` on assumption.
- Performance claims (startup time, per-output overhead, discovery latency) need measured
  numbers recorded next to the SPEC item.
