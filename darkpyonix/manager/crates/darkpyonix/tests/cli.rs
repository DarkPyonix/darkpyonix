//! End-to-end CLI tests against a fake manager (SPEC FR-C1, FR-C2).

mod common;

use std::io::{BufRead, BufReader};
use std::process::Stdio;
use std::time::{Duration, Instant};

use common::*;
use serde_json::{json, Value};

fn sigint(pid: u32) {
    // SAFETY: plain kill(2) on a child we spawned.
    assert_eq!(unsafe { libc::kill(pid as libc::pid_t, libc::SIGINT) }, 0);
}

#[test]
fn test_fr_c2_run_follows_outputs_and_exits_0() {
    let fake = Fake::start("run_ok", Scenario::Ok);
    let f = fake.file();
    let o = run(&fake.home, &["run", "train.py"]);
    assert_eq!(o.status.code(), Some(0), "stderr: {}", text(&o.stderr));
    assert_eq!(text(&o.stdout), "hello\n42\n");
    assert!(text(&o.stderr).contains("warn\n"));
    assert!(!text(&o.stdout).contains("NOT MINE"));

    let canonical = std::fs::canonicalize(&f).unwrap();
    assert_eq!(fake.state.body_of("POST /kernels").unwrap(), json!({"path": canonical.to_string_lossy()}));
    let kid = kernel_id(&canonical.to_string_lossy());
    let body = fake.state.body_of(&format!("POST /kernels/{kid}/runs")).unwrap();
    assert_eq!(body, json!({"mode": "all", "on_busy": "reject"}));
    // The stream was opened before the run was started.
    let calls: Vec<String> = fake.state.calls().into_iter().map(|c| c.0).collect();
    let ev = calls.iter().position(|c| c.ends_with("/events")).unwrap();
    let rn = calls.iter().position(|c| c.ends_with("/runs")).unwrap();
    assert!(ev < rn, "{calls:?}");
}

#[test]
fn test_fr_c2_run_error_exits_1_with_traceback() {
    let fake = Fake::start("run_error", Scenario::Error);
    fake.file();
    let o = run(&fake.home, &["run", "train.py"]);
    assert_eq!(o.status.code(), Some(1));
    assert_eq!(text(&o.stdout), "before\n");
    let err = text(&o.stderr);
    // Not a terminal: ANSI colours are stripped from the traceback.
    assert!(
        err.contains("Traceback (most recent call last):\n  File \"train.py\", line 2, in <module>\nValueError: bad\n"),
        "{err}"
    );
}

#[test]
fn test_fr_c2_ctrl_c_interrupts_the_run_and_exits_130() {
    let fake = Fake::start("ctrl_c", Scenario::WaitInterrupt { finish: true });
    fake.file();
    let mut child = cli(&fake.home).args(["run", "train.py"]).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    assert_eq!(lines.next().unwrap().unwrap(), "started");
    sigint(child.id());
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(130));
    assert_eq!(fake.state.count("POST /kernels/k_"), 2, "run + one interrupt: {:?}", fake.state.calls());
    assert_eq!(fake.state.calls().iter().filter(|c| c.0.ends_with("/interrupt")).count(), 1);
    assert_eq!(fake.state.count("DELETE"), 0, "stopping must never kill");
    let mut err = String::new();
    std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut err).unwrap();
    assert!(err.contains("KeyboardInterrupt"), "{err}");
}

#[test]
fn test_fr_c2_second_ctrl_c_detaches_and_the_run_continues() {
    let fake = Fake::start("ctrl_c_twice", Scenario::WaitInterrupt { finish: false });
    fake.file();
    let mut child = cli(&fake.home).args(["run", "train.py"]).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    assert_eq!(lines.next().unwrap().unwrap(), "started");
    sigint(child.id());
    wait_until("the interrupt request", Duration::from_secs(5), || {
        fake.state.calls().iter().any(|c| c.0.ends_with("/interrupt"))
    });
    std::thread::sleep(Duration::from_millis(50));
    assert!(child.try_wait().unwrap().is_none(), "first Ctrl-C must keep following");
    sigint(child.id());
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(130));
    assert_eq!(fake.state.calls().iter().filter(|c| c.0.ends_with("/interrupt")).count(), 1);
    assert_eq!(fake.state.count("DELETE"), 0);
    let mut err = String::new();
    std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut err).unwrap();
    assert!(err.contains("detached; run 20261003-142233-a1f0 continues"), "{err}");
}

#[test]
fn test_fr_c2_second_run_exits_75_with_hint() {
    let fake = Fake::start("busy", Scenario::Busy);
    fake.file();
    let o = run(&fake.home, &["run", "train.py"]);
    assert_eq!(o.status.code(), Some(75));
    let err = text(&o.stderr);
    assert!(err.contains("train.py is busy: run 20261003-142233-a1f0 (running)"), "{err}");
    assert!(err.contains("use --queue or `darkpyonix stop train.py`"), "{err}");

    let o = run(&fake.home, &["run", "train.py", "--json"]);
    assert_eq!(o.status.code(), Some(75));
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["error"]["code"], "busy");
    assert_eq!(v["error"]["data"]["current"]["run_id"], RUN);
    assert!(v["error"]["hint"].as_str().unwrap().contains("--queue"));
}

#[test]
fn test_fr_c2_detach_prints_the_run_id_and_does_not_follow() {
    let fake = Fake::start("detach", Scenario::Ok);
    fake.file();
    let o = run(&fake.home, &["run", "train.py", "--detach"]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    assert_eq!(text(&o.stdout), format!("{RUN}\n"));
    assert_eq!(fake.state.calls().iter().filter(|c| c.0.ends_with("/events")).count(), 0);

    let o = run(&fake.home, &["--json", "run", "train.py", "--detach"]);
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!((v["run_id"].as_str(), v["state"].as_str()), (Some(RUN), Some("running")));
    assert!(v["kernel_id"].as_str().unwrap().starts_with("k_"));
}

#[test]
fn test_fr_c2_run_options_reach_the_api() {
    let fake = Fake::start("options", Scenario::Ok);
    fake.file();
    let o = run(
        &fake.home,
        &["run", "train.py", "--queue", "--cells", "1,3", "--param", "lr=0.1", "--param", "model=swin_t",
          "--python", "/usr/bin/python3", "--detach"],
    );
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    assert_eq!(fake.state.body_of("POST /kernels").unwrap()["python"], "/usr/bin/python3");
    let body = fake.state.calls().into_iter().find(|c| c.0.ends_with("/runs")).unwrap().1;
    assert_eq!(
        body,
        json!({"mode": "cells", "cells": [1, 3], "on_busy": "queue", "params": {"lr": 0.1, "model": "swin_t"}})
    );
}

#[test]
fn test_fr_c2_json_output_for_agents() {
    let fake = Fake::start("json", Scenario::Ok);
    fake.file();
    let o = run(&fake.home, &["run", "train.py", "--json"]);
    assert_eq!(o.status.code(), Some(0));
    let events: Vec<Value> = text(&o.stdout).lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert!(events.iter().all(|e| e["data"]["run_id"] == RUN));
    assert_eq!(events.last().unwrap()["type"], "run.finished");
    assert_eq!(events.last().unwrap()["data"]["status"], "ok");
    assert!(events.iter().any(|e| e["type"] == "output" && e["data"]["output"]["text"] == "hello\n"));

    let o = run(&fake.home, &["ps", "--json"]);
    assert_eq!(o.status.code(), Some(0));
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["kernels"][0]["status"], "idle");

    let o = run(&fake.home, &["status", "missing.py", "--json"]);
    assert_eq!(o.status.code(), Some(1));
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["error"]["code"], "bad_request");
}

#[test]
fn test_fr_c2_cli_commands() {
    let fake = Fake::start("commands", Scenario::Ok);
    fake.file();
    let h = &fake.home;

    // No kernel yet: stop is a no-op, status/vars say so.
    let o = run(h, &["stop", "train.py"]);
    assert_eq!(o.status.code(), Some(0));
    assert!(text(&o.stdout).contains("no kernel is running"));
    let o = run(h, &["status", "train.py"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(text(&o.stderr).contains("no kernel is running for train.py"), "{}", text(&o.stderr));
    // logs without a kernel read the latest log through /documents.
    let o = run(h, &["logs", "train.py"]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    assert_eq!(text(&o.stdout), "logged line\n");
    assert!(text(&o.stderr).contains("ZeroDivisionError: division by zero"));

    let o = run(h, &["kernel", "train.py"]);
    assert_eq!(o.status.code(), Some(0));
    assert!(text(&o.stdout).starts_with("started k_"), "{}", text(&o.stdout));
    let o = run(h, &["kernel", "train.py", "--json"]);
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["launched"], false);

    let o = run(h, &["status", "train.py"]);
    assert_eq!(o.status.code(), Some(0));
    assert!(text(&o.stdout).contains("status   idle"), "{}", text(&o.stdout));

    let o = run(h, &["ps"]);
    assert!(text(&o.stdout).starts_with("KERNEL"));
    assert!(text(&o.stdout).contains("train.py"));

    let o = run(h, &["status"]);
    assert_eq!(o.status.code(), Some(0));
    assert!(text(&o.stdout).starts_with("manager  ephemeral http://127.0.0.1:"), "{}", text(&o.stdout));

    let o = run(h, &["vars", "train.py"]);
    assert!(text(&o.stdout).contains("shape=(2, 3) dtype=float32 len=2"), "{}", text(&o.stdout));

    let o = run(h, &["logs", "train.py", "--run", "latest"]);
    assert_eq!(text(&o.stdout), "logged line\n");
    // Not following when nothing is executing.
    let o = run(h, &["logs", "train.py", "--follow"]);
    assert_eq!(o.status.code(), Some(0));
    assert_eq!(text(&o.stdout), "logged line\n");

    let o = run(h, &["stop", "train.py"]);
    assert_eq!(o.status.code(), Some(0));
    assert!(text(&o.stdout).contains("interrupted run"));

    let o = run(h, &["restart", "train.py", "--hard"]);
    assert_eq!(o.status.code(), Some(0));
    assert_eq!(fake.state.calls().iter().find(|c| c.0.ends_with("/restart")).unwrap().1, json!({"hard": true}));

    let o = run(h, &["shutdown", "train.py"]);
    assert_eq!(o.status.code(), Some(0));
    let o = run(h, &["shutdown", "train.py", "--force"]);
    assert_eq!(o.status.code(), Some(0));
    let deletes: Vec<Value> = fake.state.calls().into_iter().filter(|c| c.0.starts_with("DELETE")).map(|c| c.1).collect();
    assert_eq!(deletes, vec![json!({}), json!({"force": "true"})]);

    // The fake is ephemeral: share needs a dedicated manager.
    let o = run(h, &["share", "train.py", "--permission", "viewer1"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(text(&o.stderr).contains("dedicated manager"), "{}", text(&o.stderr));
    let shares = fake.state.calls().iter().filter(|c| c.0.ends_with("/shares")).count();
    assert_eq!(shares, 0, "share must not reach an ephemeral manager");
}

#[test]
fn test_fr_c2_logs_follow_replays_the_executing_run() {
    // Busy: the kernel reports RUN as executing; its early output is only in the replay buffer.
    let fake = Fake::start("logs_follow", Scenario::Busy);
    fake.file();
    assert_eq!(run(&fake.home, &["kernel", "train.py"]).status.code(), Some(0));
    fake.state.emit("run.started", json!({"run_id": RUN, "mode": "all", "cells": [], "params": {}}));
    fake.state.out(RUN, json!({"output_type": "stream", "name": "stdout", "text": "replayed\n"}));
    fake.state.out(OTHER_RUN, json!({"output_type": "stream", "name": "stdout", "text": "NOT MINE\n"}));
    let mut child =
        cli(&fake.home).args(["logs", "train.py", "--follow"]).stdout(Stdio::piped()).spawn().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    assert_eq!(lines.next().unwrap().unwrap(), "replayed");
    fake.state.out(RUN, json!({"output_type": "stream", "name": "stdout", "text": "live\n"}));
    fake.state.emit("run.finished", json!({"run_id": RUN, "status": "error", "duration": 1.0}));
    assert_eq!(lines.next().unwrap().unwrap(), "live");
    assert_eq!(child.wait().unwrap().code(), Some(1), "exit code follows the run status");
    let ev = fake.state.calls().into_iter().find(|c| c.0.ends_with("/events")).unwrap().1;
    assert_eq!(ev["query"]["since"], "0");
    assert_eq!(fake.state.count("POST /kernels/k_"), 0, "logs never interrupts");
}

#[test]
fn test_fr_c1_skips_dead_and_unhealthy_registrations() {
    let fake = Fake::start("stale", Scenario::Ok);
    let mut c = std::process::Command::new("true").spawn().unwrap();
    let dead = c.id();
    c.wait().unwrap();
    // A dead dedicated manager (would be preferred) and a live pid that does not answer.
    let dead_rec = json!({"url": "http://127.0.0.1:9", "token": "x", "mode": "dedicated", "pid": dead,
                          "started_at": "2026-10-04T00:00:00Z"});
    std::fs::write(fake.home.join(format!("managers/{dead}.json")), dead_rec.to_string()).unwrap();
    let parent = std::os::unix::process::parent_id();
    let mute = json!({"url": "http://127.0.0.1:9", "token": "x", "mode": "ephemeral", "pid": parent,
                      "started_at": "2026-10-05T00:00:00Z"});
    std::fs::write(fake.home.join(format!("managers/{parent}.json")), mute.to_string()).unwrap();
    let o = run(&fake.home, &["ps", "--json"]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    assert_eq!(fake.state.count("GET /kernels"), 1);
}

fn write_script(path: &std::path::Path, body: &str) {
    std::fs::write(path, format!("#!/bin/sh\n{body}")).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn test_fr_c1_cli_spawns_manager_when_none_is_running() {
    let home = scratch_home("spawn");
    let fake = Fake::start_unregistered(home.clone(), Scenario::Ok);
    let exe = home.join("fake-manager.sh");
    // Stands in for `darkpyonix manager --ephemeral`: registers the fake under its own pid.
    write_script(
        &exe,
        r#"echo "$$ $*" > "$DARKPYONIX_HOME/spawned-args"
printf '{"url":"%s","token":"%s","mode":"ephemeral","pid":%s,"started_at":"2026-10-03T00:00:00Z"}' \
  "$FAKE_URL" "$FAKE_TOKEN" "$$" > "$DARKPYONIX_HOME/managers/$$.json.tmp"
mv "$DARKPYONIX_HOME/managers/$$.json.tmp" "$DARKPYONIX_HOME/managers/$$.json"
exec sleep 30
"#,
    );
    let started = Instant::now();
    let o = cli(&home)
        .args(["ps", "--json"])
        .env("DARKPYONIX_SELF_EXE", &exe)
        .env("FAKE_URL", fake.url())
        .env("FAKE_TOKEN", TOKEN)
        .output()
        .unwrap();
    let spawned = std::fs::read_to_string(home.join("spawned-args")).unwrap();
    let (pid, args) = spawned.trim().split_once(' ').unwrap();
    let pid: i32 = pid.parse().unwrap();
    // SAFETY: kill the stand-in manager we caused to be spawned.
    unsafe { libc::kill(pid, libc::SIGTERM) };
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    assert_eq!(args, "manager --ephemeral");
    assert!(started.elapsed() < Duration::from_secs(5));
    let v: Value = serde_json::from_slice(&o.stdout).unwrap();
    assert!(v["kernels"].is_array());
}

#[test]
fn test_fr_c1_spawn_failure_is_reported() {
    let home = scratch_home("spawn_fail");
    let exe = home.join("broken-manager.sh");
    write_script(&exe, "echo 'boom: cannot bind' >&2\nexit 3\n");
    let o = cli(&home).args(["ps"]).env("DARKPYONIX_SELF_EXE", &exe).output().unwrap();
    assert_eq!(o.status.code(), Some(1));
    let err = text(&o.stderr);
    assert!(err.contains("could not start an ephemeral manager"), "{err}");
    assert!(err.contains("boom: cannot bind"), "{err}");
}

#[test]
fn test_fr_a2_registry_token_is_used() {
    let fake = Fake::start("token", Scenario::Ok);
    let pid = std::process::id();
    let bad = json!({"url": fake.url(), "token": "wrong", "mode": "ephemeral", "pid": pid});
    std::fs::write(fake.home.join(format!("managers/{pid}.json")), bad.to_string()).unwrap();
    let o = run(&fake.home, &["ps"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(text(&o.stderr).contains("missing or invalid token"), "{}", text(&o.stderr));
}
