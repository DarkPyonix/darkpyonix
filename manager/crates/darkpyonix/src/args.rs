//! Command line surface of SPEC FR-C2. Parsing only: nothing here touches the network.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use serde_json::Value;

#[derive(Debug, Parser)]
#[command(
    name = "darkpyonix",
    version,
    about = "Run notebook files on file-bound DarkPyonix kernels",
    after_help = "Exit codes: 0 ok, 1 error, 2 usage, 75 file busy (another run is executing), \
                  130 run interrupted."
)]
pub struct Cli {
    /// Machine-readable output: JSON objects (NDJSON events while following a run).
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the file (or selected cells) and follow its output. Ctrl-C interrupts the run;
    /// a second Ctrl-C detaches and leaves the run going.
    Run(RunArgs),
    /// Interrupt the running cell (KeyboardInterrupt). The kernel and its namespace stay.
    Stop(FileArg),
    /// Kernel status of a file, or the manager and its kernels without FILE.
    Status(OptFileArg),
    /// List running kernels.
    Ps,
    /// Print a run log's outputs.
    Logs(LogsArgs),
    /// Follow the kernel's live events until Ctrl-C.
    Attach(FileArg),
    /// Show the kernel's user variables.
    Vars(FileArg),
    /// Restart the kernel (clears the namespace).
    Restart(RestartArgs),
    /// Shut the kernel down. Only --force kills the process.
    Shutdown(ShutdownArgs),
    /// Start the kernel for a file without running anything.
    Kernel(KernelArgs),
    /// Create a share token for a file's kernel (dedicated manager only).
    Share(ShareArgs),
    /// Run a kernel manager in the foreground.
    Manager(ManagerArgs),
}

#[derive(Debug, Args)]
pub struct FileArg {
    pub file: PathBuf,
}

#[derive(Debug, Args)]
pub struct OptFileArg {
    pub file: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    pub file: PathBuf,
    /// Cell indexes to run (preamble is 0), e.g. `1,3` or `2-4`.
    #[arg(long, value_parser = parse_cells)]
    pub cells: Option<Cells>,
    /// A value for darkpyonix.params: `name=value`, value parsed as JSON, else a string.
    #[arg(long = "param", value_name = "K=V", value_parser = parse_param)]
    pub params: Vec<(String, Value)>,
    /// Interpreter to start the kernel with (when it is not running yet).
    #[arg(long)]
    pub python: Option<String>,
    /// Wait for the current run instead of failing with exit code 75.
    #[arg(long)]
    pub queue: bool,
    /// Return once the run is accepted and print its run id.
    #[arg(long)]
    pub detach: bool,
}

#[derive(Debug, Args)]
pub struct LogsArgs {
    pub file: PathBuf,
    /// `latest`, `current` or a run id. Default: the executing run, else the latest.
    #[arg(long = "run", value_name = "latest|ID")]
    pub run: Option<String>,
    /// Keep printing the executing run's outputs until it finishes.
    #[arg(long, short = 'f')]
    pub follow: bool,
}

#[derive(Debug, Args)]
pub struct RestartArgs {
    pub file: PathBuf,
    /// Re-execute the kernel process instead of clearing the namespace in place.
    #[arg(long)]
    pub hard: bool,
}

#[derive(Debug, Args)]
pub struct ShutdownArgs {
    pub file: PathBuf,
    /// Kill the process if it has not exited within 5 seconds.
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct KernelArgs {
    pub file: PathBuf,
    #[arg(long)]
    pub python: Option<String>,
}

#[derive(Debug, Args)]
pub struct ShareArgs {
    pub file: PathBuf,
    #[arg(long, value_parser = ["viewer1", "viewer2", "viewer3", "editor"])]
    pub permission: String,
    #[arg(long)]
    pub label: Option<String>,
}

#[derive(Debug, Args, Clone, PartialEq)]
pub struct ManagerArgs {
    /// Long-lived manager for remote access and sharing (no idle exit).
    #[arg(long, conflicts_with = "ephemeral")]
    pub dedicated: bool,
    /// Loopback manager that exits when idle (what the CLI spawns).
    #[arg(long)]
    pub ephemeral: bool,
    #[arg(long)]
    pub host: Option<String>,
    #[arg(long)]
    pub port: Option<u16>,
    /// Seconds without requests or streams before an ephemeral manager exits.
    #[arg(long = "idle-timeout", value_name = "S")]
    pub idle_timeout: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cells(pub Vec<u64>);

/// `1,3` / `2-4` / `0,2-3` → sorted, de-duplicated cell indexes.
pub fn parse_cells(s: &str) -> Result<Cells, String> {
    let mut out = Vec::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let num = |t: &str| t.trim().parse::<u64>().map_err(|_| format!("not a cell index: {t:?}"));
        match part.split_once('-') {
            Some((a, b)) => {
                let (a, b) = (num(a)?, num(b)?);
                if a > b {
                    return Err(format!("empty cell range: {part:?}"));
                }
                out.extend(a..=b);
            }
            None => out.push(num(part)?),
        }
    }
    if out.is_empty() {
        return Err("no cells given".into());
    }
    out.sort_unstable();
    out.dedup();
    Ok(Cells(out))
}

/// `k=v`: `v` is JSON when it parses as JSON (`3`, `true`, `[1,2]`, `"x"`), else the raw string.
pub fn parse_param(s: &str) -> Result<(String, Value), String> {
    let (k, v) = s.split_once('=').ok_or_else(|| format!("expected NAME=VALUE, got {s:?}"))?;
    let k = k.trim();
    if k.is_empty() {
        return Err(format!("empty parameter name in {s:?}"));
    }
    let v = serde_json::from_str(v).unwrap_or_else(|_| Value::String(v.to_string()));
    Ok((k.to_string(), v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("darkpyonix").chain(args.iter().copied()))
    }

    #[test]
    fn test_fr_c2_run_flags_parse() {
        let cli = parse(&[
            "run", "a.py", "--cells", "1,3", "--param", "lr=0.1", "--param", "name=swin_t",
            "--python", "/usr/bin/python3", "--queue", "--detach", "--json",
        ])
        .unwrap();
        assert!(cli.json);
        let Command::Run(r) = cli.command else { panic!("not run") };
        assert_eq!(r.file, PathBuf::from("a.py"));
        assert_eq!(r.cells, Some(Cells(vec![1, 3])));
        assert_eq!(r.params, vec![("lr".into(), json!(0.1)), ("name".into(), json!("swin_t"))]);
        assert_eq!(r.python.as_deref(), Some("/usr/bin/python3"));
        assert!(r.queue && r.detach);
    }

    #[test]
    fn test_fr_c2_cells_accept_lists_and_ranges() {
        assert_eq!(parse_cells("1,3").unwrap(), Cells(vec![1, 3]));
        assert_eq!(parse_cells("3, 0,2-4,3").unwrap(), Cells(vec![0, 2, 3, 4]));
        assert!(parse_cells("a").is_err());
        assert!(parse_cells("4-2").is_err());
        assert!(parse_cells("").is_err());
        assert!(parse_cells("-1").is_err());
    }

    #[test]
    fn test_fr_c2_param_value_is_json_else_string() {
        assert_eq!(parse_param("n=3").unwrap(), ("n".into(), json!(3)));
        assert_eq!(parse_param("f=true").unwrap(), ("f".into(), json!(true)));
        assert_eq!(parse_param("l=[1,2]").unwrap(), ("l".into(), json!([1, 2])));
        assert_eq!(parse_param("s=\"3\"").unwrap(), ("s".into(), json!("3")));
        assert_eq!(parse_param("m=swin_t").unwrap(), ("m".into(), json!("swin_t")));
        assert_eq!(parse_param("e=a=b").unwrap(), ("e".into(), json!("a=b")));
        assert_eq!(parse_param("z=").unwrap(), ("z".into(), json!("")));
        assert!(parse_param("noequals").is_err());
        assert!(parse_param("=1").is_err());
    }

    #[test]
    fn test_fr_c2_every_command_parses() {
        for args in [
            &["stop", "a.py"][..],
            &["status"],
            &["status", "a.py"],
            &["ps", "--json"],
            &["logs", "a.py", "--run", "latest", "--follow"],
            &["attach", "a.py"],
            &["vars", "a.py"],
            &["restart", "a.py", "--hard"],
            &["shutdown", "a.py", "--force"],
            &["kernel", "a.py", "--python", "python3.8"],
            &["share", "a.py", "--permission", "viewer2"],
            &["manager", "--dedicated", "--host", "0.0.0.0", "--port", "8443"],
            &["manager", "--ephemeral", "--idle-timeout", "30"],
        ] {
            parse(args).unwrap_or_else(|e| panic!("{args:?}: {e}"));
        }
    }

    #[test]
    fn test_fr_c2_invalid_usage_is_rejected() {
        assert!(parse(&["share", "a.py", "--permission", "admin"]).is_err());
        assert!(parse(&["manager", "--dedicated", "--ephemeral"]).is_err());
        assert!(parse(&["run"]).is_err());
        assert!(parse(&["run", "a.py", "--param", "x"]).is_err());
        assert_eq!(parse(&["bogus"]).unwrap_err().exit_code(), 2);
    }
}
