//! `darkpyonix manager`: the manager half of the single binary (INTENT D10, SPEC FR-M3/M4).

use crate::args::ManagerArgs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Loopback, random port, exits after `idle_timeout` without requests or streams (FR-M3).
    Ephemeral,
    /// Configured host/port, no idle exit, master and share tokens (FR-M4).
    Dedicated,
}

/// What `darkpyonix manager ...` asked for, with the defaults of SPEC FR-M3/FR-M4 applied.
#[derive(Debug, Clone, PartialEq)]
pub struct ManagerOpts {
    pub mode: Mode,
    pub host: String,
    /// 0 = any free port.
    pub port: u16,
    /// Seconds; `None` for a dedicated manager.
    pub idle_timeout: Option<u64>,
}

pub const DEFAULT_IDLE_TIMEOUT: u64 = 120;

impl From<&ManagerArgs> for ManagerOpts {
    fn from(a: &ManagerArgs) -> Self {
        let mode = if a.dedicated { Mode::Dedicated } else { Mode::Ephemeral };
        ManagerOpts {
            mode,
            host: a.host.clone().unwrap_or_else(|| "127.0.0.1".into()),
            port: a.port.unwrap_or(0),
            idle_timeout: match mode {
                Mode::Ephemeral => Some(a.idle_timeout.unwrap_or(DEFAULT_IDLE_TIMEOUT)),
                Mode::Dedicated => a.idle_timeout,
            },
        }
    }
}

/// INTEGRATION POINT (leader): serve the manager API until shutdown.
///
/// Wire `dpx_kernel::RealBackend` into `dpx_server::serve` here. Contract the CLI relies on
/// (SPEC FR-C1, FR-M3, PROTOCOL §1):
/// * listen on `opts.host:opts.port` (port 0 = any free port);
/// * once `/health` answers, write `<DARKPYONIX_HOME>/managers/<own pid>.json`, mode 0600,
///   atomically (write a temp file, then rename) with
///   `{"url": "http://127.0.0.1:<port>", "token": "<master token>", "mode": "ephemeral"|"dedicated",
///     "pid": <own pid>, "started_at": "<RFC 3339>"}` — see `discovery::ManagerRecord`;
///   the CLI that spawned it polls for exactly that file name for 5 s;
/// * ephemeral: exit after `idle_timeout` seconds with no request and no open SSE stream,
///   removing the registry file and leaving every kernel running;
/// * never read stdin or write to stdout (the CLI spawns it with both closed; stderr goes
///   to `<DARKPYONIX_HOME>/managers/spawn.log`).
///
/// It runs on its own Tokio runtime (build a multi-thread one here); `main` calls it before
/// any client-side runtime exists. Return `Err(message)` to exit 1 with that message.
pub fn run_manager(opts: ManagerOpts) -> Result<(), String> {
    let _ = opts;
    Err("manager not wired yet: `darkpyonix manager` awaits dpx-kernel::RealBackend + dpx-server::serve".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(dedicated: bool, port: Option<u16>, idle: Option<u64>) -> ManagerArgs {
        ManagerArgs { dedicated, ephemeral: !dedicated, host: None, port, idle_timeout: idle }
    }

    #[test]
    fn test_fr_m3_ephemeral_defaults() {
        let o = ManagerOpts::from(&args(false, None, None));
        assert_eq!(o, ManagerOpts { mode: Mode::Ephemeral, host: "127.0.0.1".into(), port: 0, idle_timeout: Some(120) });
        assert_eq!(ManagerOpts::from(&args(false, None, Some(5))).idle_timeout, Some(5));
    }

    #[test]
    fn test_fr_m4_dedicated_has_no_idle_timeout_by_default() {
        let o = ManagerOpts::from(&args(true, Some(8443), None));
        assert_eq!((o.mode, o.port, o.idle_timeout), (Mode::Dedicated, 8443, None));
    }
}
