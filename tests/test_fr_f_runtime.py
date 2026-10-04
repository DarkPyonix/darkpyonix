"""FR-F2..F6: the runtime API notebook files import (docs/FORMAT.md §3–§4).

Plain-python behaviour is checked by running scripts under every interpreter on the machine
(NFR-K1); kernel behaviour is checked through ``darkpyonix.kernel.hostctx``.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import time
import warnings

import pytest

import darkpyonix
from conftest import KERNEL_ROOT
from darkpyonix.kernel import hostctx


def _env(**extra):
    env = dict(os.environ)
    env["PYTHONPATH"] = KERNEL_ROOT
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.update(extra)
    return env


def _write(scratch, name, text):
    path = os.path.join(scratch, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(text).lstrip("\n"))
    return path


def _run(python, path, *args, **kw):
    return subprocess.run([python, path] + list(args), capture_output=True, text=True,
                          env=_env(), cwd=os.path.dirname(path), timeout=60, **kw)


@pytest.fixture
def kernel():
    """Simulate a kernel hosting a run; yields (set_params, emitted bundles)."""
    emitted = []

    def activate(params=None):
        hostctx.set_active(params or {}, lambda bundle, silent: emitted.append((bundle, silent)))

    activate()
    yield activate, emitted
    hostctx.clear()


# --------------------------------------------------------------------------- import


def test_runtime_import_is_cheap_and_never_loads_the_manager(python, scratch):
    out = subprocess.run(
        [python, "-c", "import sys, darkpyonix; darkpyonix.params; darkpyonix.markdown('x');"
                       "print(sorted(m for m in sys.modules if m.startswith('darkpyonix.')))"],
        capture_output=True, text=True, env=_env(), cwd=scratch, timeout=30)
    assert out.returncode == 0, out.stderr
    loaded = eval(out.stdout)
    assert not [m for m in loaded if m.startswith("darkpyonix.manager")]
    assert not [m for m in loaded if m.startswith("darkpyonix.kernel.") and m != "darkpyonix.kernel.hostctx"]


# --------------------------------------------------------------------------- FR-F2


def test_fr_f2_markdown_in_kernel_and_plain_python(python, scratch, kernel):
    activate, emitted = kernel
    darkpyonix.markdown("""
        # Title
        body
    """, silent=True)
    darkpyonix.markdown("plain")
    assert emitted == [({"text/markdown": "# Title\nbody"}, True),
                       ({"text/markdown": "plain"}, False)]

    with warnings.catch_warnings():  # the warning itself is checked in the subprocess below
        warnings.simplefilter("ignore")
        darkpyonix.markdown("typo", slient=False)
    assert emitted[-1] == ({"text/markdown": "typo"}, False)

    hostctx.clear()
    del emitted[:]
    darkpyonix.markdown("outside")
    assert emitted == []

    path = _write(scratch, "md.py", '''
        import darkpyonix
        print("before")
        darkpyonix.markdown("""
        # hidden
        """, silent=True)
        darkpyonix.markdown("typo", slient=False)
        darkpyonix.markdown("typo again", slient=False)
        print("after")
    ''')
    out = _run(python, path)
    assert out.returncode == 0, out.stderr
    assert out.stdout == "before\nafter\n"
    assert out.stderr.count("slient") >= 1 and out.stderr.count("UserWarning") == 1


# --------------------------------------------------------------------------- FR-F3


def test_fr_f3_params_precedence_and_validation(python, scratch, kernel, monkeypatch):
    activate, _ = kernel
    p = darkpyonix.params
    choices = ["default_model", "swin_t", "resnet", "vi-t"]

    monkeypatch.setattr(sys, "argv", ["nb.py"])
    assert p.get("model_id", default=0, choices=choices) == "default_model"  # index into choices
    assert p.get("model_height", default=50, range=(30, 100, 1)) == 50
    assert p.get("lr", default=0, choices=[1e-5, 1e-3]) == 1e-5

    # command line beats default; both spellings; unknown args are left alone
    monkeypatch.setattr(sys, "argv", ["nb.py", "--other", "1", "--model_id", "swin_t",
                                      "--model_height=70", "--lr", "0.001", "--flag"])
    assert p.get("model_id", default=0, choices=choices) == "swin_t"
    assert p.get("model_height", default=50, range=(30, 100, 1)) == 70
    assert p.get("lr", default=0, choices=[1e-5, 1e-3]) == 1e-3
    assert p.get("flag", default=False) is True
    assert sys.argv[1:3] == ["--other", "1"]

    # run request params beat the command line
    activate({"model_id": "resnet", "model_height": 40, "flag": "false"})
    assert p.get("model_id", default=0, choices=choices) == "resnet"
    assert p.get("model_height", default=50, range=(30, 100, 1)) == 40
    assert p.get("flag", default=True) is False
    assert p.all()["model_id"] == "resnet"

    spec = {s["name"]: s for s in p.spec()}
    assert spec["model_id"]["choices"] == choices and spec["model_id"]["default"] == "default_model"
    assert spec["model_id"]["default_index"] == 0 and spec["model_id"]["source"] == "run"
    assert spec["model_height"]["range"] == [30, 100, 1] and spec["model_height"]["type"] == "int"

    for bad, kwargs in [
        ({"model_height": 101}, dict(default=50, range=(30, 100, 1))),
        ({"model_height": "abc"}, dict(default=50, range=(30, 100, 1))),
        ({"step": 31}, dict(default=30, range=(30, 100, 2))),
        ({"model_id": "vgg"}, dict(default=0, choices=choices)),
        ({"flag": "maybe"}, dict(default=True)),
    ]:
        activate(bad)
        name = next(iter(bad))
        with pytest.raises(ValueError, match=repr(name)):
            p.get(name, **kwargs)
    activate({})
    with pytest.raises(ValueError, match="'idx'"):
        p.get("idx", default=9, choices=["a"])

    path = _write(scratch, "params.py", '''
        import darkpyonix
        MODEL_ID = darkpyonix.params.get("model_id", default=0, choices=["default_model", "swin_t", "resnet", "vi-t"])
        MODEL_HEIGHT = darkpyonix.params.get("model_height", default=50, range=(30, 100, 1))
        print(MODEL_ID, MODEL_HEIGHT)
    ''')
    out = _run(python, path, "--model_id", "swin_t")
    assert (out.returncode, out.stdout) == (0, "swin_t 50\n"), out.stderr
    out = _run(python, path, "--model_height=101")
    assert out.returncode == 1 and "ValueError" in out.stderr and "'model_height'" in out.stderr


# --------------------------------------------------------------------------- FR-F4


BINDING_NOTEBOOK = '''
    import functools
    import json
    import darkpyonix
    import darkpyonix as dp
    from darkpyonix import binding


    # %% [binding]
    @darkpyonix.binding
    class Encoder:
        kind = json.__name__          # imports are visible

        def encode(self, value):
            return json.dumps(value)

        def greet(self):
            return message            # a [code] variable assigned later


    # %% [binding]
    def tag(fn):
        fn.tagged = True
        return fn


    @tag
    @dp.binding
    @functools.lru_cache(maxsize=None)
    def square(x):
        return x * x


    # %% [binding]
    @binding
    def uses_encoder(v):
        return Encoder().encode(v)    # earlier binding is visible


    # %% [binding]
    @darkpyonix.binding
    def leaks():
        return message


    # %% [code]
    message = "Hello!"
    print(Encoder.kind, uses_encoder([1]), square(3), square.cache_info().hits,
          getattr(square, "tagged", False))
    try:
        leaks()
    except NameError as e:
        print("NameError", e)

    try:
        Encoder().greet()
    except NameError as e:
        print("class NameError", e)
    print(message)                    # the [code] cell itself still sees it


    @darkpyonix.binding
    def boom():
        raise RuntimeError("line check")


    try:
        boom()
    except RuntimeError:
        import traceback
        print("line", traceback.extract_tb(sys.exc_info()[2])[-1].lineno)
'''


def test_fr_f4_binding_cannot_see_code_cell_variables(python, scratch):
    text = "import sys\n" + textwrap.dedent(BINDING_NOTEBOOK).lstrip("\n")
    path = _write(scratch, "binding_nb.py", text)
    boom_line = text.splitlines().index('    raise RuntimeError("line check")') + 1
    out = _run(python, path)
    assert out.returncode == 0, out.stderr
    lines = out.stdout.splitlines()
    assert lines[0] == 'json [1] 9 0 True'          # other decorators kept, applied once
    assert lines[1] == "NameError name 'message' is not defined"
    assert lines[2] == "class NameError name 'message' is not defined"
    assert lines[3] == "Hello!"
    assert lines[4] == "line %d" % boom_line          # tracebacks point at the file
    assert out.stderr == ""

    # The kernel compiles cells against the file's name, so the same holds there.
    env = _env()
    runner = "import runpy, sys; sys.argv=[%r]; runpy.run_path(%r, run_name='__main__')" % (path, path)
    out2 = subprocess.run([python, "-c", runner], capture_output=True, text=True, env=env, timeout=60)
    assert out2.stdout == out.stdout, out2.stderr


def test_fr_f4_binding_without_source_returns_object_with_warning():
    ns = {"darkpyonix": darkpyonix}
    with pytest.warns(UserWarning, match="unchanged"):
        exec(compile("@darkpyonix.binding\ndef f():\n    return 1\n", "<no-source>", "exec"), ns)
    assert ns["f"]() == 1


# --------------------------------------------------------------------------- FR-F6


def test_fr_f6_run_command_streams_and_forwards_interrupt(python, scratch, capsys):
    code = darkpyonix.run_command("""
        echo one
        echo err >&2
        echo two
        exit 3
    """)
    assert code == 3
    captured = capsys.readouterr()
    assert captured.out == "one\ntwo\n" and captured.err == "err\n"

    assert darkpyonix.run_command([sys.executable, "-c", "print('argv')"], shell=False) == 0
    assert darkpyonix.run_command("echo $DP_X", env={"DP_X": "envok"}, cwd=scratch) == 0
    assert capsys.readouterr().out == "argv\nenvok\n"
    with pytest.raises(subprocess.CalledProcessError):
        darkpyonix.run_command("exit 2", check=True)

    # Lines arrive while the command is still running (streaming, not buffered to the end).
    path = _write(scratch, "stream.py", '''
        import sys, time, darkpyonix
        darkpyonix.run_command("echo first; sleep 3; echo second")
    ''')
    proc = subprocess.Popen([python, path], stdout=subprocess.PIPE, text=True, env=_env(), cwd=scratch)
    assert proc.stdout.readline() == "first\n"
    t_first = time.monotonic()
    assert proc.stdout.read() == "second\n" and proc.wait(10) == 0
    assert time.monotonic() - t_first > 1.0  # "first" came out before the sleep ended

    # SIGINT to the notebook process reaches the child's process group; the run re-raises.
    path = _write(scratch, "interrupt.py", '''
        import darkpyonix
        try:
            darkpyonix.run_command("trap 'echo child-got-sigint; exit 7' INT; echo ready; "
                                   "while true; do sleep 0.1; done")
        except KeyboardInterrupt:
            print("interrupted")
    ''')
    proc = subprocess.Popen([python, path], stdout=subprocess.PIPE, text=True, env=_env(), cwd=scratch)
    try:
        assert proc.stdout.readline() == "ready\n"
        os.kill(proc.pid, signal.SIGINT)
        rest = proc.stdout.read()
        assert proc.wait(15) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
    assert rest == "child-got-sigint\ninterrupted\n"


def test_package_helpers_install_into_current_interpreter_and_raise_on_failure(scratch, capsys):
    missing = os.path.join(scratch, "no-such-package")
    with pytest.raises(subprocess.CalledProcessError) as info:
        darkpyonix.pip.install(missing)
    assert info.value.cmd[:3] == [sys.executable, "-m", "pip"]
    with pytest.raises(subprocess.CalledProcessError) as info:
        darkpyonix.uv.add(missing)
    assert sys.executable in info.value.cmd


# --------------------------------------------------------------------------- display, parallel, interop


def test_display_parallel_and_reserved_interop(kernel, capsys):
    activate, emitted = kernel
    darkpyonix.display({"a": 1})
    if not os.path.exists(os.path.join(KERNEL_ROOT, "darkpyonix", "kernel", "display.py")):
        assert emitted[-1] == ({"text/plain": "{'a': 1}"}, False)
    hostctx.clear()
    darkpyonix.display([1, 2], "s")
    assert capsys.readouterr().out == "[1, 2]\n's'\n"

    import asyncio

    async def slow(v, d):
        await asyncio.sleep(d)
        return v

    t0 = time.monotonic()
    assert darkpyonix.run_parallel(slow(1, 0.3), slow(2, 0.1), 3) == [1, 2, 3]
    assert time.monotonic() - t0 < 0.55
    assert darkpyonix.run_parallel() == []

    async def nested():
        return darkpyonix.run_parallel(slow(1, 0))

    with pytest.raises(RuntimeError, match="running event loop"):
        asyncio.run(nested())

    for api in ("run_cinterop", "run_cppinterop", "run_rustinterop"):
        with pytest.raises(NotImplementedError, match="reserved: issue #5"):
            getattr(darkpyonix, api)("blabla")


# --------------------------------------------------------------------------- reference file


REDUCED_REFERENCE = '''
    """Starboard Notebook: Python support (reduced: no torch/pandas)"""
    import darkpyonix


    # %% [code]
    import json
    import asyncio


    # %% [argparse]
    MODEL_ID = darkpyonix.params.get("model_id", default=0, choices=["default_model", "swin_t", "resnet", "vi-t"])
    MODEL_HEIGHT = darkpyonix.params.get("model_height", default=50, range=(30, 100, 1))
    BASE_LEARNING_RATE = darkpyonix.params.get("base_learning_rate", default=0, choices=[1e-5, 1e-3])


    # %% [code]
    print("Running preprocessor for Python support...")


    # %% [markdown]
    darkpyonix.markdown("""
    # Python support in Starboard Notebook
    """, silent=True)


    # %% [code]
    message = "Hello Python!"
    print(message)
    x = [i**2 for i in range(5)]
    x


    # %% [binding]
    @darkpyonix.binding
    class Classifier(object):
        def __init__(self, backbone: str):
            self.net = json.dumps({"backbone": backbone})

        def forward(self, x):
            return self.net


    # %% [binding]
    @darkpyonix.binding
    def train_step(model, batch):
        return model.forward(batch), df


    # %% [shell]
    darkpyonix.run_command("""echo name,memory.total""")


    # %% [markdown]
    darkpyonix.markdown("""
    ## Visualizing car data
    """, slient=False)


    # %% [code]
    df = "data"
    print(MODEL_ID, MODEL_HEIGHT, BASE_LEARNING_RATE, Classifier("swin_t").forward(None))
    try:
        train_step(Classifier("x"), None)
    except NameError as e:
        print("binding isolated:", e)


    # %% [cinterop]
    try:
        darkpyonix.run_cinterop("""
            blabla
        """)
    except NotImplementedError as e:
        print(e)


    # %% [parallel]
    __co_routines__ = []


    # %% [code]
    # @width: 1fr
    async def load(split):
        await asyncio.sleep(0.01)
        return split

    __co_routines__.append(load("train"))


    # %% [code]
    # @width: 1fr
    __co_routines__.append(load("val"))


    # %% [concorrunt]
    if __name__  == '__main__':
        print(darkpyonix.run_parallel(*__co_routines__))
'''

REDUCED_STDOUT = (
    "Running preprocessor for Python support...\n"
    "Hello Python!\n"
    "name,memory.total\n"
    'swin_t 50 1e-05 {"backbone": "swin_t"}\n'
    "binding isolated: name 'df' is not defined\n"
    "darkpyonix.run_cinterop is reserved: issue #5\n"
    "['train', 'val']\n"
)


def test_fr_f5_reduced_reference_runs_under_plain_python(python, scratch):
    path = _write(scratch, "reference.py", REDUCED_REFERENCE)
    out = _run(python, path, "--model_id", "swin_t")
    assert out.returncode == 0, out.stderr
    assert out.stdout == REDUCED_STDOUT
    assert "slient" in out.stderr


def test_fr_f5_reduced_reference_in_kernel_emits_markdown(scratch, kernel, capsys):
    activate, emitted = kernel
    activate({"model_id": "swin_t"})
    path = _write(scratch, "reference.py", REDUCED_REFERENCE)
    with open(path, encoding="utf-8") as f:
        code = compile(f.read(), path, "exec")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        exec(code, {"__name__": "__main__", "__file__": path})
    assert capsys.readouterr().out == REDUCED_STDOUT
    assert emitted == [({"text/markdown": "# Python support in Starboard Notebook"}, True),
                       ({"text/markdown": "## Visualizing car data"}, False)]


# --------------------------------------------------------------------------- real kernel


@pytest.fixture
def real_kernel(dp_home):
    """Start a real kernel for a file; yields ``open(path, python) -> client``."""
    from darkpyonix.kernel import launcher
    from darkpyonix.kernel.protocol import kernel_id_for
    from kernel_procs import connect, kill, wait_pid_gone
    started = []

    def open_kernel(path, python):
        pid = launcher.launch(path, python=python)
        started.append(pid)
        info = launcher.wait_for_announce(kernel_id_for(path), pid=pid, timeout=15)
        assert info is not None, "kernel did not announce"
        c = connect(info)
        started.append(c)
        return c

    yield open_kernel
    for item in reversed(started):
        if isinstance(item, int):
            kill(item, signal.SIGTERM)
            if not wait_pid_gone(item):
                kill(item, signal.SIGKILL)
                wait_pid_gone(item)
        else:
            item.close()


def test_fr_f2_markdown_in_real_kernel_is_display_data_and_silent_is_not_logged(
        python, scratch, real_kernel):
    from kernel_procs import run_and_wait
    path = _write(scratch, "md_kernel.py", '''
        import darkpyonix

        # %% [code]
        darkpyonix.markdown("""
            # Shown
            kept in the log
        """)
        darkpyonix.markdown("# Hidden", silent=True)
        print("done")
    ''')
    c = real_kernel(path, python)
    status, events = run_and_wait(c)
    assert status == "ok"
    live = [e["data"]["output"] for e in events if e["type"] == "output"
            and e["data"]["output"]["output_type"] == "display_data"]
    assert [o["data"] for o in live] == [{"text/markdown": "# Shown\nkept in the log"},
                                         {"text/markdown": "# Hidden"}]
    nb = c.request("runs.get", {"run_id": "latest"})
    logged = [o for cell in nb["cells"] for o in cell["outputs"]
              if o["output_type"] == "display_data"]
    assert [o["data"] for o in logged] == [{"text/markdown": "# Shown\nkept in the log"}]
    outputs = [o for cell in nb["cells"] for o in cell["outputs"]]
    assert not any("# Hidden" in str(o) for o in outputs)
    assert [o.get("text") for o in outputs if o["output_type"] == "stream"] == ["done\n"]


F5_CASES = {
    "ok": ('''
        print("preamble")
        # %% [code]
        print("one")
        # %% [code]
        print("two")
    ''', 0, "ok"),
    "raises": ('''
        print("preamble")
        # %% [code]
        print("one")
        raise ValueError("boom")
        # %% [code]
        print("never")
    ''', 1, "error"),
    "exit_3": ('''
        import sys
        # %% [code]
        print("one")
        sys.exit(3)
        # %% [code]
        print("never")
    ''', 3, "error"),
    "exit_0": ('''
        import sys
        # %% [code]
        print("one")
        sys.exit(0)
        # %% [code]
        print("never")
    ''', 0, "ok"),
}


@pytest.mark.parametrize("case", sorted(F5_CASES))
def test_fr_f5_exit_code_matches_run_status(python, scratch, real_kernel, case):
    """Exit code 0 under ``python file.py`` ↔ run status ``ok``; non-zero ↔ ``error``.
    Standard output matches too."""
    from kernel_procs import run_and_wait, stream_text
    text, code, status = F5_CASES[case]
    path = _write(scratch, "f5_%s.py" % case, text)
    plain = _run(python, path)
    assert plain.returncode == code, plain.stderr
    c = real_kernel(path, python)
    got, _ = run_and_wait(c)
    assert got == status
    assert (plain.returncode == 0) == (got == "ok")
    nb = c.request("runs.get", {"run_id": "latest"})
    assert nb["metadata"]["darkpyonix"]["status"] == status
    assert stream_text(nb) == plain.stdout
