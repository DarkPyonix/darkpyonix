# darkpyonix

DarkPyonix Kernel — a file-bound, manager-independent Python kernel for AI/ML work done by people and agents.

```
darkpyonix run train.py          # runs in the file's kernel; logs land in __runs__/train.py/
darkpyonix stop train.py         # interrupts the running cell — never kills
darkpyonix logs train.py -f      # follow the latest run
darkpyonix vars train.py         # what the kernel currently holds
```

- **One kernel per file.** The kernel id comes from the file's path, so every IDE, agent and shared viewer finds the same kernel. Running the same file twice is refused, not duplicated.
- **Kernels outlive managers.** Close the IDE, lose the network, kill the manager: the training keeps going, and the next manager finds it again.
- **Stop means interrupt.** `KeyboardInterrupt` in the running cell; the namespace stays.
- **Runs are recorded automatically** beside the file as `.ipynb` (nbformat 4), and inside the kernel as `__runs__`.
- **Standard library only, Python 3.8+, nothing to install** in the interpreter that runs the kernel.
- **Notebooks are plain `.py` / `.pynb`** with `# %% [type]` cells, valid Python under `python file.py`.

## Status

Design is fixed (milestone M0); implementation starts with M1. See [PROJECT.md](PROJECT.md).

## Documents

| Document | Content |
|---|---|
| [PROJECT.md](PROJECT.md) | Scope, milestones with dates, open questions |
| [docs/INTENT.md](docs/INTENT.md) | Why, decisions (D1–D15), rejected alternatives |
| [docs/SPEC.md](docs/SPEC.md) | Requirements and acceptance criteria |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How kernel, manager, hub, Ember and ash fit together |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | DKP/1: discovery datagrams and the kernel control channel |
| [docs/FORMAT.md](docs/FORMAT.md) | The notebook file format |
| [docs/api/](docs/api/) | OpenAPI for the manager and the hub, rendered by `docs/api/index.html` |
| [darkpyonix.mermaid](darkpyonix.mermaid) | Class diagram of the object model |
| [docs/설계초안/](docs/설계초안/) | The 2025 design materials, kept for reference |

To browse the API locally: `python3 -m http.server -d docs/api 8000` and open <http://127.0.0.1:8000/>.

## Repository layout

```
darkpyonix/kernel/darkpyonix/            runtime API + notebook parser (stdlib only)
darkpyonix/kernel/darkpyonix/kernel/     the kernel process (stdlib only)
darkpyonix/kernel/darkpyonix/manager/    the superseded Python manager prototype
darkpyonix/manager/                      the manager and the darkpyonix CLI (Rust workspace)
hub/worker/                              darkpyonix.dev hub API (Cloudflare Worker, TypeScript)
hub/server/                              relay.darkpyonix.dev iroh relay host (Rust)
docs/                                    design documents
tests/                                   Python test suite
```

## License

MIT. See [LICENSE](LICENSE).
