# darkpyonix

DarkPyonix binds one Python kernel to one notebook file. Close the editor, lose the network or kill
the manager: the run keeps going, and the next tool that opens the file finds the same kernel. It is
built for long AI/ML runs shared by people and agents.

```
darkpyonix run train.py                  # run the whole file, follow the output
darkpyonix run train.py --detach         # start it and return the run id
darkpyonix logs train.py --follow        # follow the latest run
darkpyonix stop train.py                 # interrupt the running cell; the kernel stays
darkpyonix vars train.py                 # what the kernel holds now
```

- **One kernel per file.** The kernel id comes from the file's path. Every IDE, agent and shared
  viewer reaches the same kernel, and a second run of a busy file is refused, not duplicated.
- **Kernels outlive managers.** A kernel never depends on a manager being alive, so any manager
  can come and go.
- **Stop means interrupt.** Stopping raises `KeyboardInterrupt` in the running cell. The
  variables stay, so the next run continues from them.
- **Runs are recorded.** Each run is saved beside the file in `__runs__/` as an nbformat 4
  `.ipynb`.
- **Standard library only, Python 3.8+.** The kernel starts from any interpreter without being
  installed there.
- **Notebooks are plain `.py` / `.pynb` files** with `# %%` cells. They stay valid Python under
  `python file.py`.

## What this package is

This package is the runtime API that a notebook imports as `darkpyonix` (`markdown`, `params`,
`binding`, `display` and the rest). It has no dependencies. Inside a kernel, `import darkpyonix`
works without it, because the kernel puts its own sources on `sys.path`. Install it so a notebook
also runs outside a kernel, for example with `uv run python train.py` in CI.

```
uv add darkpyonix                         # into a uv project
ppp core add "darkpyonix==0.2.0"          # with pypackpack
tcl install darkpyonix                    # with toolchain-lite
```

The `darkpyonix` CLI and the manager are a separate Rust binary. It is not in this package yet,
because prebuilt binaries are not published (planned). The
[install guide](https://darkpyonix.dev/darkpyonix/en/getting-started.html) shows how to build it
from a checkout.

## Status

Version 0.2.0 is alpha. What works today and what is planned:

| Part | Status |
|---|---|
| Kernel: one per file, outlives managers, interrupt, run logs | implemented |
| Runtime API and notebook parser (this package) | implemented |
| Rust manager and `darkpyonix` CLI | implemented, built from source |
| Prebuilt CLI binary in a wheel | planned |
| VS Code and IntelliJ notebook renderers | planned ([#22](https://github.com/DarkPyonix/darkpyonix/issues/22)) |
| ash opening a shared kernel | planned ([#20](https://github.com/DarkPyonix/darkpyonix/issues/20)) |
| darkpyonix.dev hub | planned ([#21](https://github.com/DarkPyonix/darkpyonix/issues/21)) |

Parts of the manager are being redesigned. The
[guide's overview](https://darkpyonix.dev/darkpyonix/en/) lists each change with its issue.

## Documentation

- [User guide](https://darkpyonix.dev/darkpyonix/) (English and Korean): install, notebook files,
  the CLI, run logs, the manager API, editors and agents.
- [Notebook file format](https://github.com/DarkPyonix/darkpyonix/blob/develop/docs/FORMAT.md)
- [Kernel wire protocol](https://github.com/DarkPyonix/darkpyonix/blob/develop/docs/PROTOCOL.md)
- [Manager HTTP API (OpenAPI)](https://github.com/DarkPyonix/darkpyonix/blob/develop/docs/api/manager.openapi.yaml)
- [Architecture](https://github.com/DarkPyonix/darkpyonix/blob/develop/docs/ARCHITECTURE.md)

## Repository layout

```
darkpyonix/kernel/darkpyonix/            runtime API + notebook parser (stdlib only)
darkpyonix/kernel/darkpyonix/kernel/     the kernel process (stdlib only)
darkpyonix/kernel/darkpyonix/manager/    the superseded Python manager prototype
darkpyonix/manager/                      the manager and the darkpyonix CLI (Rust workspace)
darkpyonix/hub/worker/                              darkpyonix.dev hub API (Cloudflare Worker, TypeScript)
darkpyonix/hub/server/                              relay.darkpyonix.dev iroh relay host (Rust)
docs/                                    design documents and the user guide
tests/                                   Python test suite
```

## License

Apache License 2.0 (SPDX `Apache-2.0`). See
[LICENSE](https://github.com/DarkPyonix/darkpyonix/blob/develop/LICENSE).
