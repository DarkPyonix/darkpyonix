//! The client commands of SPEC FR-C2, each a few HTTP calls to a manager.

use std::io::{IsTerminal, Write};
use std::path::Path;
use std::time::Duration;

use reqwest::Method;
use serde_json::{json, Map, Value};

use crate::args::{Command, LogsArgs, RunArgs};
use crate::client::{http_client, Api, CliError};
use crate::discovery::{connect, resolve_file};
use crate::render::{self, Stream, EXIT_BUSY, EXIT_ERROR, EXIT_INTERRUPTED, EXIT_OK};
use crate::sse::{SseEvent, SseParser};

/// Where text goes, and in which form.
pub struct Out {
    pub json: bool,
    strip_tracebacks: bool,
}

impl Out {
    pub fn new(json: bool) -> Self {
        Self { json, strip_tracebacks: !std::io::stderr().is_terminal() }
    }

    pub fn write(&self, stream: Stream, text: &str) {
        // A closed pipe (`| head`) is not an error worth reporting.
        let _ = match stream {
            Stream::Stdout => {
                let mut o = std::io::stdout().lock();
                o.write_all(text.as_bytes()).and_then(|_| o.flush())
            }
            Stream::Stderr => {
                let mut e = std::io::stderr().lock();
                e.write_all(text.as_bytes()).and_then(|_| e.flush())
            }
        };
    }

    pub fn json_line(&self, v: &Value) {
        self.write(Stream::Stdout, &format!("{v}\n"));
    }

    /// Human text on stdout, or the JSON value with `--json`.
    pub fn result(&self, human: &str, v: &Value) {
        if self.json {
            self.json_line(v);
        } else {
            self.write(Stream::Stdout, human);
        }
    }

    pub fn note(&self, msg: &str) {
        self.write(Stream::Stderr, &format!("darkpyonix: {msg}\n"));
    }

    pub fn error(&self, e: &CliError) {
        if self.json {
            self.json_line(&e.to_json());
        } else {
            self.note(&e.message);
        }
    }

    pub fn output(&self, output: &Value) {
        if let Some((stream, text)) = render::render_output(output) {
            let is_error = output.get("output_type").and_then(Value::as_str) == Some("error");
            if is_error && self.strip_tracebacks {
                self.write(stream, &render::strip_ansi(&text));
            } else {
                self.write(stream, &text);
            }
        }
    }
}

/// Run one client command; returns the process exit code.
pub async fn dispatch(cmd: Command, out: &Out) -> i32 {
    match dispatch_inner(cmd, out).await {
        Ok(code) => code,
        Err(e) => {
            out.error(&e);
            EXIT_ERROR
        }
    }
}

async fn dispatch_inner(cmd: Command, out: &Out) -> Result<i32, CliError> {
    // Resolve the file before contacting (or spawning) a manager: a typo fails fast.
    let file = match &cmd {
        Command::Run(a) => Some(a.file.clone()),
        Command::Stop(a) | Command::Attach(a) | Command::Vars(a) => Some(a.file.clone()),
        Command::Status(a) => a.file.clone(),
        Command::Logs(a) => Some(a.file.clone()),
        Command::Restart(a) => Some(a.file.clone()),
        Command::Shutdown(a) => Some(a.file.clone()),
        Command::Kernel(a) => Some(a.file.clone()),
        Command::Share(a) => Some(a.file.clone()),
        Command::Ps | Command::Manager(_) => None,
    };
    let target = file.as_deref().map(resolve_file).transpose()?;
    let http = http_client();
    let api = connect(&http, matches!(cmd, Command::Share(_))).await?;
    let label = file.as_deref().map(|f| f.display().to_string()).unwrap_or_default();
    let kpath = |kid: &str, rest: &str| format!("/api/v1/kernels/{kid}{rest}");
    let (path, kid) = target.unwrap_or_default();
    let not_running = |e: CliError| {
        if e.is("not_found") {
            CliError::new("not_found", format!("no kernel is running for {label}"))
        } else {
            e
        }
    };

    match cmd {
        Command::Run(a) => run(&api, out, &a, &path, &label).await,
        Command::Stop(_) => match api.post(&kpath(&kid, "/interrupt"), &json!({})).await {
            Ok((_, v)) => {
                let human = match v.get("run_id").and_then(Value::as_str) {
                    Some(r) if v["interrupted"] == true => format!("interrupted run {r} of {label}\n"),
                    _ => format!("nothing was running in {label}\n"),
                };
                out.result(&human, &v);
                Ok(EXIT_OK)
            }
            Err(e) if e.is("not_found") => {
                out.result(&format!("nothing to stop: no kernel is running for {label}\n"), &json!({"interrupted": false}));
                Ok(EXIT_OK)
            }
            Err(e) => Err(e),
        },
        Command::Status(a) if a.file.is_some() => {
            let k = api.get(&kpath(&kid, "")).await.map_err(not_running)?;
            out.result(&render::kernel_details(&k), &k);
            Ok(EXIT_OK)
        }
        Command::Status(_) => {
            let m = api.get("/api/v1/manager").await?;
            let ks = api.get("/api/v1/kernels").await?;
            let list = ks["kernels"].as_array().cloned().unwrap_or_default();
            let human = format!(
                "manager  {} {} (pid {}, version {})\n\n{}",
                render::str_field(&m, "mode"),
                api.base,
                m.get("pid").map_or(String::new(), Value::to_string),
                render::str_field(&m, "version"),
                render::kernels_table(&list),
            );
            let mut mj = m.clone();
            mj["url"] = json!(api.base);
            out.result(&human, &json!({"manager": mj, "kernels": list}));
            Ok(EXIT_OK)
        }
        Command::Ps => {
            let ks = api.get("/api/v1/kernels").await?;
            let list = ks["kernels"].as_array().cloned().unwrap_or_default();
            out.result(&render::kernels_table(&list), &ks);
            Ok(EXIT_OK)
        }
        Command::Logs(a) => logs(&api, out, &a, &path, &kid, &label).await,
        Command::Attach(_) => {
            let k = api.get(&kpath(&kid, "")).await.map_err(not_running)?;
            if !out.json {
                out.note(&format!(
                    "attached to {} ({}{}); Ctrl-C detaches",
                    label,
                    render::str_field(&k, "status"),
                    k.get("run_id").and_then(Value::as_str).map(|r| format!(", run {r}")).unwrap_or_default()
                ));
            }
            let resp = api.events(&kid, None).await.map_err(not_running)?;
            let ctrl = CtrlC::install()?;
            follow(&api, out, &kid, resp, Target::All, ctrl, false, &label).await
        }
        Command::Vars(_) => {
            let v = api.get(&kpath(&kid, "/namespace")).await.map_err(not_running)?;
            let list = v["variables"].as_array().cloned().unwrap_or_default();
            out.result(&render::variables_table(&list), &v);
            Ok(EXIT_OK)
        }
        Command::Restart(a) => {
            let (_, k) = api.post(&kpath(&kid, "/restart"), &json!({"hard": a.hard})).await.map_err(not_running)?;
            let how = if a.hard { "hard" } else { "soft" };
            out.result(&format!("restarted {label} ({how}; namespace cleared)\n"), &k);
            Ok(EXIT_OK)
        }
        Command::Shutdown(a) => {
            let q = if a.force { "?force=true" } else { "" };
            let (_, v) = api.call(Method::DELETE, &kpath(&kid, q), None).await.map_err(not_running)?;
            let how = if a.force { " (forced)" } else { "" };
            out.result(&format!("shutting down the kernel of {label}{how}\n"), &v);
            Ok(EXIT_OK)
        }
        Command::Kernel(a) => {
            let k = ensure_kernel(&api, &path, a.python.as_deref()).await?;
            let launched = k.1;
            let mut v = k.0;
            let verb = if launched { "started" } else { "already running:" };
            let human = format!(
                "{verb} {} for {} (pid {}, Python {})\n",
                render::str_field(&v, "kernel_id"),
                label,
                v.get("pid").map_or(String::new(), Value::to_string),
                v.pointer("/python/version").and_then(Value::as_str).unwrap_or("?"),
            );
            v["launched"] = json!(launched);
            out.result(&human, &v);
            Ok(EXIT_OK)
        }
        Command::Share(a) => {
            let mut body = json!({"permission": a.permission});
            if let Some(l) = a.label {
                body["label"] = json!(l);
            }
            let (_, v) = api.post(&kpath(&kid, "/shares"), &body).await.map_err(|e| {
                if e.is("forbidden") {
                    CliError::new(
                        "forbidden",
                        format!("the manager at {} refused to share: {} (sharing needs a dedicated manager)", api.base, e.message),
                    )
                } else {
                    not_running(e)
                }
            })?;
            let human = format!(
                "{} share for {label}\nurl    {}\ntoken  {}\n(the token is shown only once)\n",
                render::str_field(&v, "permission"),
                render::str_field(&v, "url"),
                render::str_field(&v, "token"),
            );
            out.result(&human, &v);
            Ok(EXIT_OK)
        }
        Command::Manager(_) => unreachable!("handled in main"),
    }
}

/// `POST /api/v1/kernels`: the kernel for `path`, and whether it was launched now (201).
async fn ensure_kernel(api: &Api, path: &Path, python: Option<&str>) -> Result<(Value, bool), CliError> {
    let mut body = json!({"path": path.to_string_lossy()});
    if let Some(p) = python {
        body["python"] = json!(p);
    }
    let (status, k) = api.post("/api/v1/kernels", &body).await?;
    Ok((k, status == 201))
}

async fn run(api: &Api, out: &Out, a: &RunArgs, path: &Path, label: &str) -> Result<i32, CliError> {
    let (kernel, _) = ensure_kernel(api, path, a.python.as_deref()).await?;
    let kid = render::str_field(&kernel, "kernel_id").to_string();
    let mut req = json!({"on_busy": if a.queue { "queue" } else { "reject" }});
    match &a.cells {
        Some(c) => {
            req["mode"] = json!("cells");
            req["cells"] = json!(c.0);
        }
        None => req["mode"] = json!("all"),
    }
    if !a.params.is_empty() {
        req["params"] = Value::Object(a.params.iter().cloned().collect::<Map<_, _>>());
    }

    // Subscribe before starting the run so none of its events can be missed.
    let resp = if a.detach { None } else { Some(api.events(&kid, None).await?) };
    // Ctrl-C from here on interrupts the run instead of killing this process.
    let ctrl = if a.detach { None } else { Some(CtrlC::install()?) };
    let accepted = match api.post(&format!("/api/v1/kernels/{kid}/runs"), &req).await {
        Ok((_, v)) => v,
        Err(e) if e.is("busy") => return Ok(busy(out, &e, label)),
        Err(e) => return Err(e),
    };
    let run_id = render::str_field(&accepted, "run_id").to_string();
    let queued = render::str_field(&accepted, "state") == "queued";

    if a.detach {
        let mut v = accepted.clone();
        v["kernel_id"] = json!(kid);
        let mut human = format!("{run_id}\n");
        if queued {
            human = format!("{run_id} (queued at position {})\n", accepted.get("position").unwrap_or(&json!("?")));
        }
        out.result(&human, &v);
        return Ok(EXIT_OK);
    }
    if queued && !out.json {
        out.note(&format!(
            "run {run_id} is queued at position {}; waiting for its turn",
            accepted.get("position").unwrap_or(&json!("?"))
        ));
    }
    let (Some(resp), Some(ctrl)) = (resp, ctrl) else { unreachable!() };
    follow(api, out, &kid, resp, Target::Run(run_id), ctrl, true, label).await
}

/// SPEC FR-C2: a second run of a busy file prints the current run and exits 75.
fn busy(out: &Out, e: &CliError, label: &str) -> i32 {
    let hint = format!("use --queue or `darkpyonix stop {label}`");
    if out.json {
        let mut v = e.to_json();
        v["error"]["hint"] = json!(hint);
        out.json_line(&v);
        return EXIT_BUSY;
    }
    let data = e.data.clone().unwrap_or(Value::Null);
    let mut msg = format!("{label} is busy");
    if let Some(cur) = data.get("current") {
        msg.push_str(&format!(": {}", render::run_summary_line(cur)));
    }
    if let Some(n) = data.get("queue_length").and_then(Value::as_u64).filter(|n| *n > 0) {
        msg.push_str(&format!(", {n} queued"));
    }
    out.note(&msg);
    out.note(&format!("hint: {hint}"));
    EXIT_BUSY
}

async fn logs(api: &Api, out: &Out, a: &LogsArgs, path: &Path, kid: &str, label: &str) -> Result<i32, CliError> {
    if a.follow {
        match api.get(&format!("/api/v1/kernels/{kid}")).await {
            Ok(k) => {
                let current = k.get("run_id").and_then(Value::as_str).map(str::to_string);
                let wants_current = match a.run.as_deref() {
                    None | Some("current") => true,
                    Some(r) => current.as_deref() == Some(r),
                };
                if let Some(run_id) = current.filter(|_| wants_current) {
                    // Replay what the kernel still buffers for this run, then stay live.
                    let resp = api.events(kid, Some(0)).await?;
                    let ctrl = CtrlC::install()?;
                    return follow(api, out, kid, resp, Target::Run(run_id), ctrl, false, label).await;
                }
            }
            Err(e) if e.is("not_found") => {}
            Err(e) => return Err(e),
        }
        // Nothing executing: print the log as without --follow.
    }

    let refs: Vec<String> = match &a.run {
        Some(r) => vec![r.clone()],
        None => vec!["current".into(), "latest".into()],
    };
    for r in &refs {
        match api.get(&format!("/api/v1/kernels/{kid}/runs/{r}")).await {
            Ok(nb) => {
                print_cells(out, &nb);
                return Ok(EXIT_OK);
            }
            Err(e) if e.is("not_found") => continue,
            Err(e) => return Err(e),
        }
    }
    if a.run.as_deref().is_none_or(|r| r == "latest") {
        // No kernel (or no run in it): the manager reads `__runs__/` directly.
        let q = format!("/api/v1/documents?path={}", urlencode(&path.to_string_lossy()));
        match api.get(&q).await {
            Ok(doc) if !doc.get("latest_run").is_none_or(Value::is_null) => {
                print_cells(out, &doc);
                return Ok(EXIT_OK);
            }
            Ok(_) => {}
            Err(e) if e.is("not_found") => {}
            Err(e) => return Err(e),
        }
    }
    let which = a.run.as_deref().unwrap_or("latest");
    Err(CliError::new("not_found", format!("no run log `{which}` for {label}")))
}

/// Outputs of a run notebook or a Document, in cell order.
fn print_cells(out: &Out, doc: &Value) {
    if out.json {
        out.json_line(doc);
        return;
    }
    for cell in doc.get("cells").and_then(Value::as_array).into_iter().flatten() {
        for o in cell.get("outputs").and_then(Value::as_array).into_iter().flatten() {
            out.output(o);
        }
    }
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// SIGINT as a stream of presses. Installing it stops Ctrl-C from killing the CLI.
pub struct CtrlC {
    #[cfg(unix)]
    sig: tokio::signal::unix::Signal,
}

impl CtrlC {
    pub fn install() -> Result<Self, CliError> {
        #[cfg(unix)]
        {
            let sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .map_err(|e| CliError::new("internal", format!("cannot handle Ctrl-C: {e}")))?;
            Ok(Self { sig })
        }
        #[cfg(not(unix))]
        Ok(Self {})
    }

    async fn pressed(&mut self) {
        #[cfg(unix)]
        {
            self.sig.recv().await;
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

pub enum Target {
    /// One run: print its outputs, finish with its `run.finished`.
    Run(String),
    /// Every event of the kernel until Ctrl-C.
    All,
}

const RECONNECTS: u32 = 5;

/// Follow the events stream. With `interrupt_on_ctrl_c` the first Ctrl-C sends
/// `POST /interrupt` (never a kill) and keeps following; the next one detaches.
#[allow(clippy::too_many_arguments)]
async fn follow(
    api: &Api,
    out: &Out,
    kid: &str,
    mut resp: reqwest::Response,
    target: Target,
    mut ctrl: CtrlC,
    interrupt_on_ctrl_c: bool,
    label: &str,
) -> Result<i32, CliError> {
    let mut parser = SseParser::default();
    let mut last_seq: Option<u64> = None;
    let mut presses = 0u32;
    let mut failures = 0u32;
    let run_id = match &target {
        Target::Run(r) => Some(r.as_str()),
        Target::All => None,
    };
    loop {
        tokio::select! {
            _ = ctrl.pressed() => {
                presses += 1;
                if interrupt_on_ctrl_c && presses == 1 {
                    match api.post(&format!("/api/v1/kernels/{kid}/interrupt"), &json!({})).await {
                        Ok(_) => out.note(&format!(
                            "interrupting run {} of {label}; Ctrl-C again detaches (the run keeps going)",
                            run_id.unwrap_or("?"))),
                        Err(e) => out.note(&format!("interrupt failed: {}", e.message)),
                    }
                    continue;
                }
                return Ok(match run_id {
                    Some(r) => {
                        out.note(&format!(
                            "detached; run {r} continues. `darkpyonix logs {label} --follow` resumes, \
                             `darkpyonix stop {label}` interrupts"));
                        EXIT_INTERRUPTED
                    }
                    None => EXIT_OK,
                });
            }
            chunk = resp.chunk() => {
                match chunk {
                    Ok(Some(bytes)) => {
                        failures = 0;
                        for ev in parser.push(&bytes) {
                            if let Some(seq) = ev.id {
                                last_seq = Some(seq);
                            }
                            if let Some(code) = handle_event(out, &ev, run_id) {
                                return Ok(code);
                            }
                        }
                    }
                    Ok(None) | Err(_) => {
                        failures += 1;
                        if failures > RECONNECTS {
                            return Err(CliError::new("stream_lost", format!(
                                "lost the event stream of {label}; the run (if any) continues — \
                                 see `darkpyonix status {label}`")));
                        }
                        tokio::time::sleep(Duration::from_millis(100 * u64::from(failures))).await;
                        let since = last_seq.or(run_id.map(|_| 0));
                        parser = SseParser::default();
                        match api.events(kid, since).await {
                            Ok(r) => resp = r,
                            Err(e) if e.is("not_found") => {
                                return Err(CliError::new("not_found", format!("the kernel of {label} is gone")));
                            }
                            Err(_) => continue,
                        }
                    }
                }
            }
        }
    }
}

/// Print one event; `Some(exit code)` when the followed run finished.
fn handle_event(out: &Out, ev: &SseEvent, run_id: Option<&str>) -> Option<i32> {
    let ev_run = ev.data.get("run_id").and_then(Value::as_str);
    if let Some(r) = run_id {
        // Only this run's events (others may share the stream); replay_truncated passes.
        if ev_run != Some(r) && ev.kind != "replay_truncated" {
            return None;
        }
    }
    if out.json {
        out.json_line(&json!({"seq": ev.id, "type": ev.kind, "data": ev.data}));
    } else {
        match ev.kind.as_str() {
            "output" => out.output(ev.data.get("output").unwrap_or(&Value::Null)),
            "replay_truncated" => out.note("earlier output is no longer in the kernel's replay buffer"),
            "run.started" if run_id.is_none() => out.note(&format!("run {} started", ev_run.unwrap_or("?"))),
            "run.finished" if run_id.is_none() => out.note(&format!(
                "run {} finished: {}",
                ev_run.unwrap_or("?"),
                render::str_field(&ev.data, "status")
            )),
            _ => {}
        }
    }
    if ev.kind == "run.finished" && run_id.is_some() {
        let status = render::str_field(&ev.data, "status");
        if !out.json && status != "ok" {
            out.note(&format!("run {} {status}", ev_run.unwrap_or("?")));
        }
        return Some(render::exit_code_for_run_status(status));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_urlencode_path_query() {
        assert_eq!(urlencode("/a b/ü&x.py"), "/a%20b/%C3%BC%26x.py");
    }

    #[test]
    fn test_fr_c2_followed_run_ends_with_its_status() {
        let out = Out { json: true, strip_tracebacks: false };
        let fin = |r: &str, s: &str| SseEvent {
            id: Some(1),
            kind: "run.finished".into(),
            data: json!({"run_id": r, "status": s}),
        };
        assert_eq!(handle_event(&out, &fin("other", "ok"), Some("mine")), None);
        assert_eq!(handle_event(&out, &fin("mine", "ok"), Some("mine")), Some(0));
        assert_eq!(handle_event(&out, &fin("mine", "error"), Some("mine")), Some(1));
        assert_eq!(handle_event(&out, &fin("mine", "interrupted"), Some("mine")), Some(130));
        assert_eq!(handle_event(&out, &fin("mine", "ok"), None), None);
    }
}
