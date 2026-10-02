//! Discovery (FR-D1, FR-D2) and launching the EMBEDDED kernel (FR-M2, INTENT D3/D10) with real
//! Python processes: the kernel entry point (currently the placeholder that only announces)
//! is started from the sources extracted to `<home>/runtime/`.

mod common;

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};

use common::*;
use dpx_core::{KernelBackend, StartKernel};
use dpx_kernel::discovery::Discovery;
use dpx_kernel::{canonical_path, kernel_id_for, process, Config, RealBackend};

fn start(path: &Path, python: Option<&Path>) -> StartKernel {
    StartKernel {
        path: path.to_string_lossy().into_owned(),
        python: python.map(|p| p.to_string_lossy().into_owned()),
        ..Default::default()
    }
}

async fn wait_dead(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while process::pid_alive(pid) {
        assert!(Instant::now() < deadline, "pid {pid} still alive");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn launch_three(be: &RealBackend, dir: &Path, reaper: &Reaper) -> Vec<String> {
    let mut ids = Vec::new();
    for i in 0..3 {
        let nb = write_notebook(dir, &format!("nb{i}.py"));
        let e = be.ensure(start(&nb, None)).await.unwrap();
        assert!(e.launched);
        reaper.add(e.kernel.pid);
        ids.push(e.kernel.kernel_id);
    }
    ids.sort();
    ids
}

#[test]
fn fr_m2_kernel_id_matches_python() {
    let dir = scratch("kid");
    std::fs::create_dir_all(dir.join("real/sub")).unwrap();
    std::fs::write(dir.join("real/a.py"), "").unwrap();
    std::fs::write(dir.join("real/노트북.py"), "").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(dir.join("real"), dir.join("link")).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("../real/a.py", dir.join("real/sub/rel_link.py")).unwrap();
    let cwd = std::env::current_dir().unwrap();
    let rel = format!(
        "../../../.scratch/rust-kernel/{}/real/a.py",
        dir.file_name().unwrap().to_string_lossy()
    );
    assert_eq!(cwd, Path::new(env!("CARGO_MANIFEST_DIR")));
    let d = dir.to_string_lossy().into_owned();
    let paths = vec![
        format!("{d}/real/a.py"),
        format!("{d}/link/a.py"),
        format!("{d}/link/sub/../a.py"),
        format!("{d}/real/sub/rel_link.py"),
        format!("{d}/real/./sub//../노트북.py"),
        format!("{d}/real/A.PY"),
        format!("{d}/real/missing/new.py"),
        format!("{d}/link/missing.py"),
        rel,
        "relative-missing.py".to_string(),
    ];
    let script = format!(
        "import sys, json; sys.path.insert(0, {:?}); from darkpyonix.kernel.protocol import canonical_path, kernel_id_for; \
         print(json.dumps([[canonical_path(p), kernel_id_for(p)] for p in sys.argv[1:]]))",
        repo_root().join("kernel").to_string_lossy()
    );
    let out = std::process::Command::new(venv_python())
        .arg("-c")
        .arg(script)
        .args(&paths)
        .current_dir(&cwd)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let py: Vec<(String, String)> = serde_json::from_slice(&out.stdout).unwrap();
    for (p, (canon, kid)) in paths.iter().zip(py) {
        assert_eq!(canonical_path(Path::new(p)), canon, "canonical of {p}");
        assert_eq!(kernel_id_for(Path::new(p)), kid, "kernel id of {p}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fr_m2_start_kernel_is_idempotent_on_every_interpreter() {
    for python in interpreters() {
        let dir = scratch("m2");
        let home = dir.join("home");
        let be = backend(&home, true);
        let reaper = Reaper::default();
        let nb = write_notebook(&dir, "train.py");

        let t = Instant::now();
        let first = be.ensure(start(&nb, Some(&python))).await.unwrap();
        let took = t.elapsed();
        reaper.add(first.kernel.pid);
        eprintln!(
            "MEASURE ensure() launch to announce with {}: {:?}",
            python.display(),
            took
        );
        assert!(first.launched, "{}", python.display());
        assert_eq!(first.kernel.kernel_id, kernel_id_for(&nb));
        assert_eq!(first.kernel.path, canonical_path(&nb));
        assert_eq!(first.kernel.status, "idle");
        let exe = std::process::Command::new(&python)
            .args(["-c", "import sys; print(sys.executable)"])
            .output()
            .unwrap();
        assert_eq!(
            first.kernel.python["executable"],
            String::from_utf8_lossy(&exe.stdout).trim(),
            "{}",
            python.display()
        );

        // Idempotent: the second call attaches to the same process.
        let second = be.ensure(start(&nb, Some(&python))).await.unwrap();
        assert!(!second.launched);
        assert_eq!(second.kernel.pid, first.kernel.pid);
        // A second manager (fresh discovery state) sees the same kernel.
        let other = backend(&home, true);
        let third = other.ensure(start(&nb, None)).await.unwrap();
        assert!(!third.launched);
        assert_eq!(third.kernel.pid, first.kernel.pid);

        // Launched from the embedded sources extracted to <home>/runtime/<version>-<hash>/.
        let root = be.runtime_root().unwrap();
        assert!(root.starts_with(home.join("runtime")));
        assert!(root.join("darkpyonix/kernel/__main__.py").is_file());
        assert!(!root.join("darkpyonix/manager").exists());
        let cmd = std::process::Command::new("ps")
            .args(["-o", "command=", "-p", &first.kernel.pid.to_string()])
            .output()
            .unwrap();
        let cmd = String::from_utf8_lossy(&cmd.stdout);
        assert!(cmd.contains(&*root.to_string_lossy()), "{cmd}");
        assert!(
            cmd.contains(&format!("--file {}", canonical_path(&nb))),
            "{cmd}"
        );
        // Detached (INTENT D2): the kernel leads its own session.
        #[cfg(unix)]
        // SAFETY: getsid only reads process state.
        unsafe {
            assert_eq!(
                libc::getsid(first.kernel.pid as libc::pid_t),
                first.kernel.pid as libc::pid_t
            );
        }
        assert!(home
            .join("kernels")
            .join(format!("{}.log", first.kernel.kernel_id))
            .is_file());

        be.kill(&first.kernel.kernel_id).await.unwrap();
        wait_dead(first.kernel.pid).await; // reaped: no zombie left behind
        assert!(be.list(true).await.unwrap().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fr_m2_two_managers_starting_one_file_get_one_kernel() {
    let dir = scratch("m2-race");
    let home = dir.join("home");
    let a = backend(&home, true);
    let b = backend(&home, true);
    let reaper = Reaper::default();
    let nb = write_notebook(&dir, "nb.py");
    let (ra, rb) = tokio::join!(a.ensure(start(&nb, None)), b.ensure(start(&nb, None)));
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    reaper.add(ra.kernel.pid);
    reaper.add(rb.kernel.pid);
    assert_eq!(ra.kernel.pid, rb.kernel.pid);
    assert!(!(ra.launched && rb.launched));
    assert_eq!(a.list(true).await.unwrap().len(), 1);
    // The loser's process never lingers: it exits (lock held) or is stopped by its manager,
    // so it cannot take the file over later when the winner exits.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let pids = kernel_pids_for(&nb);
        for p in &pids {
            reaper.add(*p);
        }
        if pids == vec![ra.kernel.pid] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "kernel processes for one file: {pids:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Pids of kernel processes started for `file` (by command line).
fn kernel_pids_for(file: &Path) -> Vec<u32> {
    let out = std::process::Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .unwrap();
    let needle = format!("--file {}", canonical_path(file));
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.contains("darkpyonix.kernel.__main__") && l.trim_end().ends_with(&needle))
        .filter_map(|l| l.split_whitespace().next()?.parse().ok())
        .filter(|p| process::pid_alive(*p))
        .collect()
}

#[tokio::test]
async fn fr_m2_start_timeout_when_no_announce() {
    let dir = scratch("m2-timeout");
    let home = dir.join("home");
    let reaper = Reaper::default();
    let nb = write_notebook(&dir, "nb.py");
    let hang = dir.join("hang.sh");
    std::fs::write(&hang, "#!/bin/sh\nexec sleep 30\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&hang, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let mut cfg = Config::with_home(&home);
    cfg.start_timeout = Duration::from_secs(1);
    let be = RealBackend::new(cfg).unwrap();

    let t = Instant::now();
    let err = be.ensure(start(&nb, Some(&hang))).await.unwrap_err();
    assert_eq!(err.code, "start_timeout");
    let pid = err.data.as_ref().unwrap()["pid"].as_u64().unwrap() as u32;
    reaper.add(pid);
    assert!(t.elapsed() >= Duration::from_secs(1) && t.elapsed() < Duration::from_secs(3));

    // An interpreter that exits at once fails fast, with the log tail attached.
    let t = Instant::now();
    let err = be
        .ensure(start(&nb, Some(Path::new("/usr/bin/false"))))
        .await
        .unwrap_err();
    assert_eq!(err.code, "start_timeout");
    assert!(err.data.unwrap()["log"].as_str().unwrap().ends_with(".log"));
    assert!(t.elapsed() < Duration::from_secs(1));

    let err = be
        .ensure(start(&dir.join("missing.py"), None))
        .await
        .unwrap_err();
    assert_eq!(err.code, "bad_request");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fr_d1_query_finds_all_kernels() {
    let dir = scratch("d1");
    let home = dir.join("home");
    let be = backend(&home, true);
    let reaper = Reaper::default();
    let ids = launch_three(&be, &dir, &reaper).await;

    // Multicast only: a Discovery over an empty registry dir counts only datagram answers.
    let empty = dir.join("empty-registry");
    std::fs::create_dir_all(&empty).unwrap();
    let mcast = Discovery::new(be.user_tag().to_string(), empty.clone(), true);
    let n = mcast
        .query(None, Duration::from_millis(200), &HashSet::new())
        .await;
    assert_eq!(n, 3);
    let mut found: Vec<String> = mcast
        .snapshot(None)
        .iter()
        .map(|e| e["kernel_id"].as_str().unwrap().to_string())
        .collect();
    found.sort();
    assert_eq!(found, ids);

    let expect: HashSet<String> = ids.iter().cloned().collect();
    let mut lat = Vec::new();
    for _ in 0..20 {
        let d = Discovery::new(be.user_tag().to_string(), empty.clone(), true);
        let t = Instant::now();
        assert_eq!(d.query(None, Duration::from_millis(200), &expect).await, 3);
        lat.push(t.elapsed());
    }
    lat.sort();
    eprintln!(
        "MEASURE multicast query -> 3 announces: p50 {:?} max {:?}",
        percentile(&lat, 0.5),
        lat[lat.len() - 1]
    );
    assert!(
        lat[lat.len() / 2] < Duration::from_millis(200),
        "FR-D1: answers within 200 ms"
    );

    // A fresh manager's list(refresh) (registry + multicast, early exit when all known answered).
    let mut lat = Vec::new();
    for _ in 0..10 {
        let fresh = backend(&home, true);
        let t = Instant::now();
        assert_eq!(fresh.list(true).await.unwrap().len(), 3);
        lat.push(t.elapsed());
    }
    lat.sort();
    eprintln!(
        "MEASURE fresh manager list(refresh=true), 3 kernels: p50 {:?} max {:?}",
        percentile(&lat, 0.5),
        lat[lat.len() - 1]
    );

    // A query with another user's tag gets no answer.
    let other = Discovery::new("ffffffffffffffff".into(), empty, true);
    assert_eq!(
        other
            .query(None, Duration::from_millis(200), &HashSet::new())
            .await,
        0
    );

    // The listener drops a kernel on its `bye` (SIGTERM = clean exit).
    let victim = be.get(&ids[0]).await.unwrap();
    // SAFETY: plain kill(2) of a kernel this test launched.
    #[cfg(unix)]
    unsafe {
        libc::kill(victim.pid as libc::pid_t, libc::SIGTERM);
    }
    wait_dead(victim.pid).await;
    let left: Vec<_> = be
        .list(false)
        .await
        .unwrap()
        .into_iter()
        .map(|k| k.kernel_id)
        .collect();
    assert_eq!(left, ids[1..].to_vec());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fr_d2_registry_fallback_finds_kernels() {
    let dir = scratch("d2");
    let home = dir.join("home");
    let launcher = backend(&home, true);
    let reaper = Reaper::default();
    let ids = launch_three(&launcher, &dir, &reaper).await;
    let reg_only = backend(&home, false); // DARKPYONIX_DISCOVERY=registry
    let mut found: Vec<_> = reg_only
        .list(true)
        .await
        .unwrap()
        .into_iter()
        .map(|k| k.kernel_id)
        .collect();
    found.sort();
    assert_eq!(found, ids);
    // Registry-only is also how other users' kernels are kept out.
    let stranger = backend(&dir.join("other-home"), false);
    assert!(stranger.list(true).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fr_d2_stale_registry_is_pruned() {
    let dir = scratch("d2-prune");
    let home = dir.join("home");
    let be = backend(&home, true);
    let reaper = Reaper::default();
    let ids = launch_three(&be, &dir, &reaper).await;
    let victim = be.get(&ids[1]).await.unwrap();
    process::kill_pid(victim.pid).unwrap(); // kill -9: no bye, registry entry left behind
    wait_dead(victim.pid).await;
    let entry = home.join("kernels").join(format!("{}.json", ids[1]));
    assert!(entry.exists());
    let left: Vec<_> = be
        .list(true)
        .await
        .unwrap()
        .into_iter()
        .map(|k| k.kernel_id)
        .collect();
    assert_eq!(left, vec![ids[0].clone(), ids[2].clone()]);
    assert!(!entry.exists(), "dead kernel's registry entry is deleted");
}

#[tokio::test]
async fn fr_r4_document_is_built_by_the_embedded_python() {
    let dir = scratch("r4");
    let home = dir.join("home");
    let be = backend(&home, false);
    let nb = dir.join("doc.py");
    std::fs::write(&nb, "# %% [markdown]\n# Title\n\n# %%\nx = 1\n").unwrap();
    let doc = be.document(&nb.to_string_lossy(), true).await.unwrap();
    assert_eq!(doc["path"], canonical_path(&nb));
    assert_eq!(doc["kernel_id"], kernel_id_for(&nb));
    let cells = doc["cells"].as_array().unwrap();
    assert!(!cells.is_empty());
    assert!(cells.iter().all(|c| c.get("outputs").is_some()));
    let viewer = be.document(&nb.to_string_lossy(), false).await.unwrap();
    assert!(viewer["cells"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c.get("outputs").is_none()));
    let err = be
        .document(&dir.join("nope.py").to_string_lossy(), true)
        .await
        .unwrap_err();
    assert_eq!(err.code, "not_found");
}
