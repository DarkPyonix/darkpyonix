"""Smoke test of an installed darkpyonix wheel (SPEC FR-C3 acceptance criteria 1 to 5).

Run it with any Python 3 interpreter, from a directory that is not the checkout::

    python -I packaging/smoke.py --venv DIR --version 0.3.0            # platform wheel
    python -I packaging/smoke.py --venv DIR --version 0.3.0 --fallback # py3-none-any wheel

``--venv`` is a fresh virtual environment with only the wheel installed. The script talks to
the installed ``darkpyonix`` command, an ephemeral manager it starts itself, and a kernel it
starts for a tiny file. State goes to a temporary DARKPYONIX_HOME. Standard library only.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

TIMEOUT = 90


def say(msg):
    print("smoke: " + msg, flush=True)


STATE = {"home": None, "mgr": None}


def dump_diagnostics():
    """On failure: the manager's own output and every file under the smoke home (logs)."""
    mgr = STATE["mgr"]
    if mgr is not None:
        mgr.terminate()
        try:
            out, err = mgr.communicate(timeout=15)
        except Exception:
            mgr.kill()
            out, err = "", ""
        print("smoke: ---- manager stdout (rest) ----\n%s" % out, flush=True)
        print("smoke: ---- manager stderr ----\n%s" % err, flush=True)
    home = STATE["home"]
    if not home or not os.path.isdir(home):
        print("smoke: no home directory at %r" % home, flush=True)
        return
    for root, _dirs, files in os.walk(home):
        print("smoke: ---- listing %s ----" % root, flush=True)
        for name in sorted(files):
            path = os.path.join(root, name)
            try:
                size = os.path.getsize(path)
            except OSError:
                size = -1
            print("smoke:   %s (%s bytes)" % (name, size), flush=True)
    for root, _dirs, files in os.walk(home):
        for name in sorted(files):
            if name.endswith((".log", ".txt", ".json", ".err", ".out")):
                path = os.path.join(root, name)
                try:
                    with open(path, "r", errors="replace") as f:
                        text = f.read()[-20000:]
                except OSError as e:
                    text = "<unreadable: %s>" % e
                print("smoke: ---- %s ----\n%s" % (path, text), flush=True)


def fail(msg):
    print("smoke: FAIL: " + msg, file=sys.stderr, flush=True)
    try:
        dump_diagnostics()
    except Exception as e:  # diagnostics must never hide the failure
        print("smoke: diagnostics failed: %r" % (e,), flush=True)
    sys.exit(1)


def run(cmd, env, cwd, expect=0):
    proc = subprocess.run(
        cmd, env=env, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, timeout=TIMEOUT,
    )
    say("$ %s -> %s" % (" ".join(cmd), proc.returncode))
    if proc.stdout.strip():
        print(proc.stdout.rstrip(), flush=True)
    if proc.stderr.strip():
        print(proc.stderr.rstrip(), file=sys.stderr, flush=True)
    if expect is not None and proc.returncode != expect:
        fail("expected exit code %s, got %s: %s\n--- stdout ---\n%s\n--- stderr ---\n%s"
             % (expect, proc.returncode, " ".join(cmd), proc.stdout, proc.stderr))
    return proc


def api_get(url, token):
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status, resp.read().decode("utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--venv", required=True)
    ap.add_argument("--version", required=True, help="the package version the wheel must carry")
    ap.add_argument("--fallback", action="store_true", help="the py3-none-any wheel: no manager")
    args = ap.parse_args()

    bindir = os.path.join(os.path.abspath(args.venv), "Scripts" if os.name == "nt" else "bin")
    py = os.path.join(bindir, "python.exe" if os.name == "nt" else "python")
    exe = os.path.join(bindir, "darkpyonix.exe" if os.name == "nt" else "darkpyonix")
    work = tempfile.mkdtemp(prefix="dpx-smoke-")
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("DARKPYONIX_") and k not in ("PYTHONPATH", "PYTHONHOME")}
    env["DARKPYONIX_HOME"] = os.path.join(work, "home")
    STATE["home"] = env["DARKPYONIX_HOME"]
    cwd = os.path.join(work, "cwd")
    os.makedirs(cwd)
    if not os.path.isfile(exe):
        fail("the wheel installed no %s" % exe)

    # 4. the runtime API and the kernel module resolve to the installed package.
    proc = run([py, "-I", "-c",
                "import darkpyonix, darkpyonix._protocol as p; "
                "assert p.KERNEL_VERSION == darkpyonix.__version__; "
                "print(darkpyonix.__version__, darkpyonix.__file__)"], env, cwd)
    if proc.stdout.split()[0] != args.version:
        fail("import darkpyonix gives %r, expected %s" % (proc.stdout, args.version))
    if os.path.abspath(args.venv) not in os.path.abspath(proc.stdout.split(None, 1)[1]):
        fail("darkpyonix was imported from outside the venv: %s" % proc.stdout)
    run([py, "-I", "-m", "darkpyonix", "--help"], env, cwd)

    if args.fallback:
        # 5. no manager here: the command says so and exits 1.
        proc = run([exe, "--version"], env, cwd, expect=1)
        if "not available on this platform" not in proc.stderr.replace("\n", " "):
            fail("the fallback command did not say the platform is unsupported")
        say("OK (fallback wheel)")
        shutil.rmtree(work, ignore_errors=True)
        return

    # 1. the binary is the packaged version.
    proc = run([exe, "--version"], env, cwd)
    if proc.stdout.split()[-1] != args.version:
        fail("`darkpyonix --version` printed %r, the package is %s (Cargo.toml workspace "
             "version out of step with darkpyonix/kernel/__init__.py?)"
             % (proc.stdout.strip(), args.version))

    # 2. an ephemeral manager starts, prints {url, token, pid} and answers its API.
    mgr = subprocess.Popen([exe, "manager", "--ephemeral", "--idle-timeout", "120"],
                           env=env, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           universal_newlines=True)
    STATE["mgr"] = mgr
    try:
        line = mgr.stdout.readline()
        if not line:
            fail("the manager printed nothing; stderr: %s" % mgr.stderr.read())
        info = json.loads(line)
        url, token = info["url"].rstrip("/"), info["token"]
        say("manager at %s pid %s" % (url, info["pid"]))
        status, _ = api_get(url + "/health", None)
        if status != 200:
            fail("/health answered %s" % status)
        status, body = api_get(url + "/api/kernels", token)
        if status != 200:
            fail("/api/kernels answered %s" % status)
        json.loads(body)
        try:
            api_get(url + "/api/kernels", None)
            fail("/api/kernels answered without a token")
        except urllib.error.HTTPError as e:
            if e.code != 401:
                fail("/api/kernels without a token answered %s, expected 401" % e.code)

        # 3. a kernel starts for a tiny file, runs it, and shuts down. The kernel runs from
        #    the sources embedded in the binary, with the venv interpreter.
        nb = os.path.join(cwd, "hello.py")
        with open(nb, "w") as f:
            f.write('# %%\nimport darkpyonix\nprint("smoke-ok", 6 * 7)\n')
        menv = dict(env, DARKPYONIX_MANAGER_URL=url, DARKPYONIX_TOKEN=token)
        try:
            proc = run([exe, "run", nb, "--python", py], menv, cwd)
            if "smoke-ok 42" not in proc.stdout:
                fail("the run did not print the expected output")
            proc = run([exe, "ps"], menv, cwd)
            if "hello.py" not in proc.stdout:
                fail("`darkpyonix ps` does not list the kernel of hello.py")
        finally:
            run([exe, "shutdown", nb], menv, cwd, expect=None)
    finally:
        mgr.terminate()
        try:
            mgr.wait(timeout=15)
        except subprocess.TimeoutExpired:
            mgr.kill()
    say("OK")
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
