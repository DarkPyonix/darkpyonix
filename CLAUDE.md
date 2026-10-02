# CLAUDE.md

Guidance for working in this repository.

## Project

`darkpyonix` (repo `DarkPyonix/darkpyonix`, checked out as `darkpyonix-core`) is the
DarkPyonix kernel stack:

- `kernel/darkpyonix/kernel/`: the **file-bound kernel**. One kernel per source file,
  independent of any manager, standard library only, runnable by any Python 3.8+ interpreter
  without installation.
- `kernel/darkpyonix/manager/`: the **kernel manager**. A disposable HTTP front for kernels.
  It discovers running kernels, launches new ones, and serves IDEs, agents and the shared
  notebook (ash).
- `kernel/darkpyonix/` (top level): the **runtime API** that notebook files import
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
   - experiments worth keeping: `experiments/<name>/`, committed.
3. The one exception is what the product itself writes at run time on a user's machine
   (`~/.darkpyonix/`, `__runs__/` beside a notebook). Tests point those at `.scratch/`
   through `DARKPYONIX_HOME` instead of writing to the real home directory.
4. If a task seems to need a path outside the repository, ask first.

## Git

- Branches: `develop` (integration, where work lands) and `main` (protected, default).
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
