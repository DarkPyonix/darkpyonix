//! SPEC FR-M8: `darkpyonix editor install` for the three runtimes. OSE comes from a release
//! served over file:// (`DARKPYONIX_EDITOR_RELEASES`), `vsc` and `script` from fake commands.
#![cfg(unix)]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use common::*;
use sha2::{Digest, Sha256};

fn target() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => "darwin-arm64",
        ("macos", "x86_64") => "darwin-x64",
        ("linux", "x86_64") => "linux-x64",
        ("linux", "aarch64") => "linux-arm64",
        other => panic!("no OSE target for {other:?}"),
    }
}

fn executable(path: &Path, text: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// `<home>/../rel/<tag>/` with the archive and `SHA256SUMS` (a wrong sum when asked).
fn release(home: &Path, tag: &str, good_sum: bool) -> PathBuf {
    let rel = home
        .parent()
        .unwrap()
        .join(format!("rel-{}", dpx_server::auth::random_hex(4)));
    let name = format!("dpx-ose-{}-{}", tag.trim_start_matches("ose-v"), target());
    let stage = rel.join("stage").join(&name);
    executable(
        &stage.join("bin").join("dpx-ose-server"),
        "#!/bin/sh\nexit 0\n",
    );
    std::fs::write(
        stage.join("product.json"),
        r#"{"commit": "c0ffee", "version": "9.9.9"}"#,
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let gz = flate2::write::GzEncoder::new(&mut buf, flate2::Compression::fast());
        let mut tar = tar::Builder::new(gz);
        tar.append_dir_all(&name, &stage).unwrap();
        tar.into_inner().unwrap().finish().unwrap();
    }
    let dir = rel.join(tag);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{name}.tar.gz")), &buf).unwrap();
    let sum: String = if good_sum {
        Sha256::digest(&buf)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    } else {
        "00".repeat(32)
    };
    std::fs::write(dir.join("SHA256SUMS"), format!("{sum}  {name}.tar.gz\n")).unwrap();
    rel
}

fn install(home: &Path, rel: &Path, args: &[&str]) -> std::process::Output {
    cli(home)
        .env(
            "DARKPYONIX_EDITOR_RELEASES",
            format!("file://{}", rel.display()),
        )
        .args(["editor", "install"])
        .args(args)
        .output()
        .unwrap()
}

fn toml(home: &Path) -> String {
    std::fs::read_to_string(home.join("editor.toml")).unwrap_or_default()
}

#[test]
fn test_fr_m8_ose_install_checks_the_sum_and_records_the_commit() {
    let home = scratch_home("ed-ose");
    let rel = release(&home, "ose-v9.9.9", true);
    let out = install(
        &home,
        &rel,
        &["--runtime", "ose", "--release", "ose-v9.9.9"],
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    let t = toml(&home);
    assert!(
        t.contains("c0ffee") && t.contains("9.9.9") && t.contains("ose-v9.9.9"),
        "{t}"
    );
    let launcher = home
        .join("editor/ose/ose-v9.9.9")
        .join(format!("dpx-ose-9.9.9-{}", target()))
        .join("bin/dpx-ose-server");
    assert!(launcher.is_file());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["commit"], "c0ffee");
    assert_eq!(v["path"], "/ide/ose/");
}

#[test]
fn test_fr_m8_a_bad_sum_leaves_nothing_behind() {
    let home = scratch_home("ed-badsum");
    let rel = release(&home, "ose-v9.9.9", false);
    let out = install(
        &home,
        &rel,
        &["--runtime", "ose", "--release", "ose-v9.9.9"],
    );
    assert_eq!(out.status.code(), Some(1), "{}", text(&out.stderr));
    assert!(
        text(&out.stderr).contains("sha256"),
        "{}",
        text(&out.stderr)
    );
    assert!(!home.join("editor.toml").exists());
    let left: Vec<_> = std::fs::read_dir(home.join("editor/ose"))
        .map(|d| d.flatten().map(|e| e.file_name()).collect())
        .unwrap_or_default();
    assert!(left.is_empty(), "{left:?}");
}

#[test]
fn test_fr_m8_the_same_tag_is_not_downloaded_twice() {
    let home = scratch_home("ed-twice");
    let rel = release(&home, "ose-v9.9.9", true);
    assert!(install(
        &home,
        &rel,
        &["--runtime", "ose", "--release", "ose-v9.9.9"]
    )
    .status
    .success());
    std::fs::remove_dir_all(rel.join("ose-v9.9.9")).unwrap();
    let out = install(
        &home,
        &rel,
        &["--runtime", "ose", "--release", "ose-v9.9.9"],
    );
    assert!(
        out.status.success(),
        "a second install must not fetch: {}",
        text(&out.stderr)
    );
}

#[test]
fn test_fr_m8_vsc_records_the_users_code_and_downloads_nothing() {
    let home = scratch_home("ed-vsc");
    let code = home.join("fakebin").join("code");
    executable(
        &code,
        "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 1.106.0; echo abc123; echo arm64; exit 0; fi\n\
         if [ \"$1\" = serve-web ]; then echo usage; exit 0; fi\nexit 1\n",
    );
    let nowhere = home.join("no-releases");
    let out = install(
        &home,
        &nowhere,
        &[
            "--runtime",
            "vsc",
            "--accept-server-license-terms",
            "--code",
            code.to_str().unwrap(),
        ],
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    let t = toml(&home);
    assert!(
        t.contains("1.106.0") && t.contains("abc123") && t.contains("license_accepted = true"),
        "{t}"
    );
    assert!(!home.join("editor/ose").exists(), "nothing downloaded");
}

#[test]
fn test_fr_m8_vsc_without_code_is_an_error() {
    let home = scratch_home("ed-nocode");
    let out = install(
        &home,
        &home,
        &[
            "--runtime",
            "vsc",
            "--accept-server-license-terms",
            "--code",
            "/nonexistent/code",
        ],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(!home.join("editor.toml").exists());
}

#[test]
fn test_fr_m8_vsc_needs_the_license_flag() {
    let home = scratch_home("ed-license");
    let out = install(
        &home,
        &home,
        &["--runtime", "vsc", "--code", "/nonexistent/code"],
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(
        text(&out.stderr).contains("https://code.visualstudio.com/license/server"),
        "{}",
        text(&out.stderr)
    );
    assert!(!home.join("editor.toml").exists());
}

#[test]
fn test_fr_m8_script_records_the_server_it_prints() {
    let home = scratch_home("ed-script");
    let script = home.join("install.sh");
    std::fs::write(
        &script,
        "set -e\nmkdir -p \"$DARKPYONIX_EDITOR_DIR/bin\"\nprintf '#!/bin/sh\\nexit 0\\n' > \"$DARKPYONIX_EDITOR_DIR/bin/srv\"\n\
         chmod +x \"$DARKPYONIX_EDITOR_DIR/bin/srv\"\necho installing\n\
         echo \"{\\\"program\\\": \\\"$DARKPYONIX_EDITOR_DIR/bin/srv\\\", \\\"args\\\": [\\\"serve\\\"], \\\"version\\\": \\\"2.0\\\"}\"\n",
    )
    .unwrap();
    let out = install(
        &home,
        &home,
        &["--runtime", "script", "--script", script.to_str().unwrap()],
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    let t = toml(&home);
    assert!(
        t.contains("[script]") && t.contains("bin/srv") && t.contains("\"serve\""),
        "{t}"
    );
}

#[test]
fn test_fr_m8_a_failing_script_changes_nothing() {
    let home = scratch_home("ed-script-fail");
    for (name, body) in [
        ("fails.sh", "echo '{\"program\": \"/bin/sh\"}'\nexit 3\n"),
        ("noline.sh", "echo done\n"),
    ] {
        let script = home.join(name);
        std::fs::write(&script, body).unwrap();
        let out = install(
            &home,
            &home,
            &["--runtime", "script", "--script", script.to_str().unwrap()],
        );
        assert_eq!(out.status.code(), Some(1), "{name}: {}", text(&out.stderr));
        assert!(!home.join("editor.toml").exists(), "{name}");
    }
}
