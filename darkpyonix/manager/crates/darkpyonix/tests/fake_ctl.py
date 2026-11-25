#!/usr/bin/env python3
"""A stand-in for `launchctl` and `systemctl --user` in the service tests (SPEC FR-M7).
Standard library only. Called as `fake_ctl.py <tool> <args...>` through DARKPYONIX_SERVICE_CTL.

It logs every call to `$FAKE_CTL_STATE/calls.jsonl`. Starting a unit runs the command the unit
file names (ProgramArguments and EnvironmentVariables of the plist, ExecStart and Environment
of the systemd unit) as a detached process and writes its pid to `$FAKE_CTL_STATE/pid`;
stopping it sends SIGTERM to that pid and waits for it to end.
"""

import json
import os
import plistlib
import shlex
import signal
import subprocess
import sys
import time

state = os.environ["FAKE_CTL_STATE"]
os.makedirs(state, exist_ok=True)
pid_file = os.path.join(state, "pid")
argv = sys.argv[1:]
with open(os.path.join(state, "calls.jsonl"), "a") as f:
    f.write(json.dumps(argv) + "\n")


def start(program_args, env):
    full = dict(os.environ)
    full.update(env)
    log = open(os.path.join(state, "manager.log"), "ab")
    p = subprocess.Popen(program_args, env=full, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                         start_new_session=True)
    with open(pid_file, "w") as f:
        f.write(str(p.pid))


def stop():
    if not os.path.exists(pid_file):
        return
    pid = int(open(pid_file).read())
    os.remove(pid_file)
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    os.kill(pid, signal.SIGKILL)


tool = argv[0]
if tool == "launchctl":
    if argv[1] == "bootstrap":
        with open(argv[3], "rb") as f:
            plist = plistlib.load(f)
        start(plist["ProgramArguments"], plist.get("EnvironmentVariables", {}))
    elif argv[1] == "bootout":
        if not os.path.exists(pid_file):
            sys.exit(3)  # launchctl: no such service
        stop()
elif tool == "systemctl":
    unit_dir = os.path.join(os.environ["DARKPYONIX_SERVICE_ROOT"], ".config/systemd/user")
    if "enable" in argv:
        exec_args, env = None, {}
        for line in open(os.path.join(unit_dir, argv[-1])):
            key, _, value = line.strip().partition("=")
            if key == "ExecStart":
                exec_args = [a.replace("%%", "%") for a in shlex.split(value)]
            elif key == "Environment":
                k, _, v = shlex.split(value)[0].partition("=")
                env[k] = v
        start(exec_args, env)
    elif "disable" in argv:
        stop()
sys.exit(0)
