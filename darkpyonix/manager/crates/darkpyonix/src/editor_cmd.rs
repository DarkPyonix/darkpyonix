//! `darkpyonix editor …`: install and configure the editor server a dedicated manager runs
//! (SPEC FR-M8, INTENT D27). Local only: it writes `<home>/editor.toml` and the installed files,
//! and talks to no manager. A running manager picks a new install up at the next start of that
//! runtime (`POST /api/editor/{runtime}/stop`, then `launch`).

use std::io::Read;
use std::path::{Path, PathBuf};

use dpx_server::editor::{self, EditorConfig, OseInstall, ScriptInstall, VscInstall};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::args::{EditorArgs, EditorCommand, EditorInstallArgs};

pub const DEFAULT_RELEASES: &str =
    "https://github.com/DarkPyonix/darkpyonix-ember/releases/download";
pub const RELEASES_ENV: &str = "DARKPYONIX_EDITOR_RELEASES";
const EXIT_USAGE: i32 = 2;

/// A failure with its exit code.
pub struct Fail(pub i32, pub String);

fn fail(msg: impl Into<String>) -> Fail {
    Fail(crate::render::EXIT_ERROR, msg.into())
}

pub fn run(a: EditorArgs) -> i32 {
    let home = dpx_server::default_home();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    match rt.block_on(command(&home, a.command)) {
        Ok(v) => {
            println!("{v}");
            crate::render::EXIT_OK
        }
        Err(Fail(code, msg)) => {
            eprintln!("darkpyonix editor: {msg}");
            code
        }
    }
}

async fn command(home: &Path, c: EditorCommand) -> Result<Value, Fail> {
    let load = || EditorConfig::load(home).map_err(|e| fail(e.to_string()));
    let save = |cfg: &EditorConfig| {
        cfg.save(home)
            .map_err(|e| fail(format!("cannot write editor.toml: {e}")))
    };
    match c {
        EditorCommand::Install(a) => install(home, a).await,
        EditorCommand::Use { runtime } => {
            let mut cfg = load()?;
            cfg.default_runtime = runtime;
            save(&cfg)?;
            Ok(status(&cfg))
        }
        EditorCommand::Autostart { value } => {
            let mut cfg = load()?;
            cfg.autostart = value == "on";
            save(&cfg)?;
            Ok(status(&cfg))
        }
        EditorCommand::Status => Ok(status(&load()?)),
    }
}

/// What is installed, without the run state (that is the manager's `GET /api/editor`).
pub fn status(cfg: &EditorConfig) -> Value {
    let runtimes: Vec<Value> = editor::RUNTIMES
        .iter()
        .filter_map(|rt| {
            let i = cfg.installed(rt)?;
            Some(json!({"runtime": rt, "version": i.version, "commit": i.commit, "program": i.program, "path": editor::base_path(rt)}))
        })
        .collect();
    json!({"runtimes": runtimes, "default_runtime": cfg.default_runtime, "autostart": cfg.autostart})
}

async fn install(home: &Path, a: EditorInstallArgs) -> Result<Value, Fail> {
    let mut cfg = EditorConfig::load(home).map_err(|e| fail(e.to_string()))?;
    match a.runtime.as_str() {
        "ose" => {
            let tag = a.release.ok_or_else(|| {
                Fail(
                    EXIT_USAGE,
                    "--runtime ose needs --release ose-v<tag>".into(),
                )
            })?;
            cfg.ose = Some(install_ose(home, &tag, cfg.ose.as_ref()).await?);
        }
        "vsc" => {
            if !a.accept_server_license_terms {
                return Err(Fail(
                    EXIT_USAGE,
                    format!(
                        "Microsoft's VS Code Server is used under its own licence ({}). Read it, then pass \
                         --accept-server-license-terms to accept it.",
                        editor::VSC_LICENSE_URL
                    ),
                ));
            }
            cfg.vsc = Some(find_vsc(a.code)?);
        }
        "script" => {
            let script = a
                .script
                .ok_or_else(|| Fail(EXIT_USAGE, "--runtime script needs --script <file>".into()))?;
            cfg.script = Some(run_script(home, &script)?);
        }
        other => return Err(Fail(EXIT_USAGE, format!("unknown runtime {other}"))),
    }
    cfg.save(home)
        .map_err(|e| fail(format!("cannot write editor.toml: {e}")))?;
    let i = cfg.installed(&a.runtime).expect("just installed");
    Ok(
        json!({"runtime": a.runtime, "version": i.version, "commit": i.commit, "program": i.program, "path": editor::base_path(&a.runtime)}),
    )
}

// ------------------------------------------------------------------ ose

/// The OSE target name of this machine (`ember/ose/README.md`).
pub fn ose_target() -> Result<&'static str, Fail> {
    Ok(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => "darwin-arm64",
        ("macos", "x86_64") => "darwin-x64",
        ("linux", "x86_64") => "linux-x64",
        ("linux", "aarch64") => "linux-arm64",
        ("windows", "x86_64") => "windows-x64",
        (os, arch) => return Err(fail(format!("no OSE build for {os} {arch}"))),
    })
}

/// `ose-v1.139.1` and `ose-v1.139.1-dpx.2` both name `dpx-ose-1.139.1-<target>.tar.gz`.
pub fn ose_archive_name(tag: &str, target: &str) -> Result<String, Fail> {
    let rest = tag
        .strip_prefix("ose-v")
        .filter(|r| r.starts_with(|c: char| c.is_ascii_digit()));
    let Some(rest) = rest else {
        return Err(Fail(
            EXIT_USAGE,
            format!("{tag} is not an OSE release tag (ose-v<code-oss tag>)"),
        ));
    };
    if !tag
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
    {
        return Err(Fail(EXIT_USAGE, format!("{tag} is not an OSE release tag")));
    }
    let code_oss = rest.split('-').next().unwrap_or(rest);
    Ok(format!("dpx-ose-{code_oss}-{target}"))
}

async fn fetch(url: &str) -> Result<Vec<u8>, Fail> {
    if let Some(path) = url.strip_prefix("file://") {
        return std::fs::read(path).map_err(|e| fail(format!("cannot read {url}: {e}")));
    }
    let r = reqwest::get(url)
        .await
        .map_err(|e| fail(format!("cannot download {url}: {e}")))?;
    if !r.status().is_success() {
        return Err(fail(format!("cannot download {url}: {}", r.status())));
    }
    Ok(r.bytes()
        .await
        .map_err(|e| fail(format!("cannot download {url}: {e}")))?
        .to_vec())
}

/// The sum `SHA256SUMS` lists for `file`.
pub fn listed_sum(sums: &str, file: &str) -> Option<String> {
    sums.lines().find_map(|l| {
        let mut parts = l.split_whitespace();
        let sum = parts.next()?;
        let name = parts.next()?.trim_start_matches('*');
        (name == file).then(|| sum.to_ascii_lowercase())
    })
}

async fn install_ose(
    home: &Path,
    tag: &str,
    current: Option<&OseInstall>,
) -> Result<OseInstall, Fail> {
    let name = ose_archive_name(tag, ose_target()?)?;
    let root = editor::editor_dir(home).join("ose");
    let dest = root.join(tag);
    if let Some(c) = current.filter(|c| c.release == tag && editor::ose_launcher(&c.dir).is_file())
    {
        return Ok(c.clone());
    }
    let base = std::env::var(RELEASES_ENV)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_RELEASES.into());
    let base = base.trim_end_matches('/');
    let archive_file = format!("{name}.tar.gz");
    let sums = fetch(&format!("{base}/{tag}/SHA256SUMS")).await?;
    let sums = String::from_utf8_lossy(&sums);
    let want = listed_sum(&sums, &archive_file)
        .ok_or_else(|| fail(format!("SHA256SUMS of {tag} does not list {archive_file}")))?;
    let archive = fetch(&format!("{base}/{tag}/{archive_file}")).await?;
    let got = hex(&Sha256::digest(&archive));
    if got != want {
        return Err(fail(format!(
            "{archive_file}: sha256 {got}, SHA256SUMS says {want}"
        )));
    }
    dpx_server::util::ensure_private_dir(&root).map_err(|e| fail(e.to_string()))?;
    let tmp = root.join(format!(".tmp-{}", dpx_server::auth::random_hex(6)));
    let result = (|| {
        std::fs::create_dir_all(&tmp)?;
        tar::Archive::new(flate2::read::GzDecoder::new(&archive[..])).unpack(&tmp)?;
        Ok::<_, std::io::Error>(())
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(fail(format!("cannot unpack {archive_file}: {e}")));
    }
    let unpacked = tmp.join(&name);
    let product = std::fs::read(unpacked.join("product.json"))
        .ok()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok());
    if !editor::ose_launcher(&unpacked).is_file() || product.is_none() {
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(fail(format!(
            "{archive_file} has no {name}/bin/dpx-ose-server or product.json"
        )));
    }
    let product = product.unwrap_or_default();
    let _ = std::fs::remove_dir_all(&dest);
    if let Err(e) = std::fs::rename(&tmp, &dest) {
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(fail(format!(
            "cannot move the unpacked server into place: {e}"
        )));
    }
    // Keep the release a running server may still use; older ones go.
    let keep: Vec<&str> = [Some(tag), current.map(|c| c.release.as_str())]
        .into_iter()
        .flatten()
        .collect();
    if let Ok(entries) = std::fs::read_dir(&root) {
        for e in entries.flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if n.starts_with("ose-v") && !keep.contains(&n.as_str()) {
                let _ = std::fs::remove_dir_all(e.path());
            }
        }
    }
    Ok(OseInstall {
        release: tag.to_string(),
        dir: dest.join(&name),
        commit: product["commit"].as_str().unwrap_or_default().to_string(),
        version: product["version"].as_str().unwrap_or_default().to_string(),
    })
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// ------------------------------------------------------------------ vsc

fn on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    let names: Vec<String> = if cfg!(windows) {
        vec![format!("{name}.cmd"), format!("{name}.exe")]
    } else {
        vec![name.into()]
    };
    std::env::split_paths(&path)
        .flat_map(|d| names.iter().map(move |n| d.join(n)))
        .find(|p| p.is_file())
}

fn find_vsc(given: Option<PathBuf>) -> Result<VscInstall, Fail> {
    let code = match given {
        Some(p) => p,
        None => on_path("code")
            .or_else(|| {
                let mac = PathBuf::from(
                    "/Applications/Visual Studio Code.app/Contents/Resources/app/bin/code",
                );
                (cfg!(target_os = "macos") && mac.is_file()).then_some(mac)
            })
            .ok_or_else(|| {
                fail("no `code` on PATH; install Visual Studio Code or pass --code <path>")
            })?,
    };
    let out = std::process::Command::new(&code)
        .arg("--version")
        .output()
        .map_err(|e| fail(format!("cannot run {}: {e}", code.display())))?;
    if !out.status.success() {
        return Err(fail(format!("{} --version failed", code.display())));
    }
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let mut lines = text.lines();
    let version = lines.next().unwrap_or_default().trim().to_string();
    let commit = lines.next().unwrap_or_default().trim().to_string();
    let help = std::process::Command::new(&code)
        .args(["serve-web", "--help"])
        .output()
        .map_err(|e| fail(format!("cannot run {}: {e}", code.display())))?;
    if !help.status.success() {
        return Err(fail(format!(
            "{} has no `serve-web` (VS Code 1.86 or later is needed)",
            code.display()
        )));
    }
    let code = code.canonicalize().unwrap_or(code);
    Ok(VscInstall {
        code,
        version,
        commit,
        license_accepted: true,
    })
}

// ------------------------------------------------------------------ script

/// What the last stdout line of a Manual Script names.
#[derive(serde::Deserialize)]
struct ScriptResult {
    program: PathBuf,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    version: String,
    #[serde(default)]
    commit: String,
}

fn run_script(home: &Path, script: &Path) -> Result<ScriptInstall, Fail> {
    let dir = editor::editor_dir(home).join("script");
    dpx_server::util::ensure_private_dir(&dir).map_err(|e| fail(e.to_string()))?;
    let mut cmd = if cfg!(windows) {
        let mut c = std::process::Command::new("powershell");
        c.args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
            .arg(script);
        c
    } else {
        let mut c = std::process::Command::new("sh");
        c.arg(script);
        c
    };
    let mut child = cmd
        .env("DARKPYONIX_EDITOR_DIR", &dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .map_err(|e| fail(format!("cannot run {}: {e}", script.display())))?;
    let mut out = String::new();
    if let Some(mut s) = child.stdout.take() {
        let _ = s.read_to_string(&mut out);
    }
    let status = child.wait().map_err(|e| fail(e.to_string()))?;
    if !status.success() {
        return Err(fail(format!("{} ended with {status}", script.display())));
    }
    let last = out
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_default();
    let r: ScriptResult = serde_json::from_str(last.trim()).map_err(|e| {
        fail(format!(
            "the last line of {} must be {{\"program\": ..., \"args\": [...]}}: {e}",
            script.display()
        ))
    })?;
    if !r.program.is_absolute() || !is_executable(&r.program) {
        return Err(fail(format!(
            "{} is not an executable file with an absolute path",
            r.program.display()
        )));
    }
    Ok(ScriptInstall {
        program: r.program,
        args: r.args,
        version: r.version,
        commit: r.commit,
    })
}

#[cfg(unix)]
fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(p: &Path) -> bool {
    p.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fr_m8_archive_names_follow_the_release_tag() {
        assert_eq!(
            ose_archive_name("ose-v1.139.1", "darwin-arm64")
                .ok()
                .unwrap(),
            "dpx-ose-1.139.1-darwin-arm64"
        );
        assert_eq!(
            ose_archive_name("ose-v1.139.1-dpx.2", "linux-x64")
                .ok()
                .unwrap(),
            "dpx-ose-1.139.1-linux-x64"
        );
        assert!(ose_archive_name("v1.139.1", "linux-x64").is_err());
        assert!(ose_archive_name("ose-v1/../x", "linux-x64").is_err());
    }

    #[test]
    fn test_fr_m8_sums_are_read_per_file() {
        let s = "aa  dpx-ose-1-linux-x64.tar.gz\nBB *dpx-ose-1-darwin-arm64.tar.gz\n";
        assert_eq!(
            listed_sum(s, "dpx-ose-1-darwin-arm64.tar.gz").as_deref(),
            Some("bb")
        );
        assert_eq!(listed_sum(s, "other.tar.gz"), None);
    }
}
