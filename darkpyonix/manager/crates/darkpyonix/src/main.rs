//! `darkpyonix`: the CLI agents and people call, and `darkpyonix manager` (INTENT D10).
//!
//! Client commands only speak the manager HTTP API (`docs/api/manager.openapi.yaml`); see
//! SPEC FR-C1 (finding or spawning a manager) and FR-C2 (commands and exit codes).
//! Nothing heavy happens before argument parsing: agents call this constantly.

mod args;
mod client;
mod commands;
mod discovery;
mod manager;
mod render;
mod sse;

use clap::Parser;

fn main() {
    let cli = args::Cli::parse();
    if let args::Command::Manager(m) = &cli.command {
        let code = match manager::run_manager(manager::ManagerOpts::from(m)) {
            Ok(()) => 0,
            Err(msg) => {
                eprintln!("darkpyonix manager: {msg}");
                render::EXIT_ERROR
            }
        };
        std::process::exit(code);
    }
    let out = commands::Out::new(cli.json);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio runtime");
    let code = rt.block_on(commands::dispatch(cli.command, &out));
    std::process::exit(code);
}
