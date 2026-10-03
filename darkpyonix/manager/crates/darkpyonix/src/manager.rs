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

/// Serve the manager API until shutdown (INTENT D10, SPEC FR-C1, FR-M3, FR-M4).
///
/// `dpx_kernel::RealBackend` (discovery, DKP/1, the embedded kernel) behind
/// `dpx_server::serve` (the OpenAPI over HTTP/SSE, TLS, proxy). Ephemeral managers bind
/// loopback and write `<DARKPYONIX_HOME>/managers/<pid>.json` (0600) once listening; they
/// exit after `idle_timeout` and never touch kernels. Never reads stdin or writes stdout.
pub fn run_manager(opts: ManagerOpts) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("cannot start runtime: {e}"))?;
    rt.block_on(async move {
        let backend = dpx_kernel::RealBackend::from_env()
            .map_err(|e| format!("cannot start kernel backend: {e}"))?;
        let home = dpx_server::default_home();
        let mut config = match opts.mode {
            Mode::Ephemeral => dpx_server::ServerConfig::ephemeral(home),
            Mode::Dedicated => dpx_server::ServerConfig::dedicated(home, opts.host.clone(), opts.port),
        };
        if opts.mode == Mode::Ephemeral {
            config.port = opts.port;
        }
        config.idle_timeout = opts.idle_timeout.map(std::time::Duration::from_secs);
        if let Ok(token) = std::env::var("DARKPYONIX_MANAGER_TOKEN") {
            if !token.is_empty() {
                config.master_token = Some(token);
            }
        }
        let dedicated_random_token = opts.mode == Mode::Dedicated && config.master_token.is_none();
        let handle = dpx_server::serve(config, std::sync::Arc::new(backend))
            .await
            .map_err(|e| format!("cannot serve: {e}"))?;
        eprintln!("darkpyonix manager listening on {}", handle.url);
        if dedicated_random_token {
            eprintln!("master token (shown once; set DARKPYONIX_MANAGER_TOKEN to fix it): {}", handle.token);
        }
        dpx_server::run_until_signal(handle).await;
        Ok(())
    })
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
