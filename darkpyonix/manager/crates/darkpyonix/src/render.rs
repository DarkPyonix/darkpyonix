//! Turning API objects into terminal text: nbformat outputs, tables, exit codes.

use serde_json::Value;

pub const EXIT_OK: i32 = 0;
pub const EXIT_ERROR: i32 = 1;
/// EX_TEMPFAIL: the file's kernel is busy with another run (SPEC FR-C2).
pub const EXIT_BUSY: i32 = 75;
/// 128 + SIGINT: the run was interrupted, or the user detached with Ctrl-C.
pub const EXIT_INTERRUPTED: i32 = 130;

/// Exit code for a finished run's status (PROTOCOL §3.4 `run.finished`).
pub fn exit_code_for_run_status(status: &str) -> i32 {
    match status {
        "ok" => EXIT_OK,
        "interrupted" => EXIT_INTERRUPTED,
        _ => EXIT_ERROR, // error, crashed, cancelled, anything unknown
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

/// nbformat multiline strings are a string or a list of strings.
pub fn multiline(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Array(parts) => parts.iter().filter_map(Value::as_str).collect(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn with_newline(mut s: String) -> String {
    if !s.ends_with('\n') {
        s.push('\n');
    }
    s
}

/// How one nbformat 4 output appears in a terminal:
/// * `stream` → its text on stdout or stderr as it came;
/// * `display_data` / `execute_result` → `text/plain` (else `text/markdown`, else a
///   `[mime, ...]` placeholder) on stdout;
/// * `error` → the traceback on stderr, like Python prints it.
pub fn render_output(output: &Value) -> Option<(Stream, String)> {
    match output.get("output_type")?.as_str()? {
        "stream" => {
            let stream = if output.get("name").and_then(Value::as_str) == Some("stderr") {
                Stream::Stderr
            } else {
                Stream::Stdout
            };
            let text = multiline(output.get("text").unwrap_or(&Value::Null));
            (!text.is_empty()).then_some((stream, text))
        }
        "display_data" | "execute_result" => {
            let data = output.get("data")?.as_object()?;
            let text = ["text/plain", "text/markdown"]
                .iter()
                .find_map(|m| data.get(*m).map(multiline))
                .unwrap_or_else(|| {
                    let mimes: Vec<&str> = data.keys().map(String::as_str).collect();
                    format!("[{}]", mimes.join(", "))
                });
            Some((Stream::Stdout, with_newline(text)))
        }
        "error" => {
            let tb: Vec<String> = output
                .get("traceback")
                .and_then(Value::as_array)
                .map(|a| a.iter().map(multiline).collect())
                .unwrap_or_default();
            let text = if tb.is_empty() {
                let field = |k: &str| output.get(k).and_then(Value::as_str).unwrap_or("").to_string();
                format!("{}: {}", field("ename"), field("evalue"))
            } else {
                // Each traceback entry may itself end with a newline (Python's format_exception).
                tb.iter().map(|l| l.trim_end_matches('\n')).collect::<Vec<_>>().join("\n")
            };
            Some((Stream::Stderr, with_newline(text)))
        }
        _ => None,
    }
}

/// Remove ANSI escape sequences (CSI `ESC [ ... final`, and lone `ESC x`).
pub fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\x1b' {
            out.push(c);
            continue;
        }
        if chars.next() == Some('[') {
            for c in chars.by_ref() {
                if ('@'..='~').contains(&c) {
                    break;
                }
            }
        }
    }
    out
}

/// A left-aligned text table; the last column is not padded.
pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
    }
    let line = |cells: Vec<&str>| {
        let mut s = String::new();
        for (i, cell) in cells.iter().enumerate() {
            if i + 1 == cells.len() {
                s.push_str(cell);
            } else {
                s.push_str(cell);
                s.push_str(&" ".repeat(widths[i] - cell.chars().count() + 2));
            }
        }
        s.trim_end().to_string() + "\n"
    };
    let mut out = line(headers.to_vec());
    for row in rows {
        out.push_str(&line(row.iter().map(String::as_str).collect()));
    }
    out
}

pub fn str_field<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

/// `ps` rows from OpenAPI `Kernel` objects.
pub fn kernels_table(kernels: &[Value]) -> String {
    let rows: Vec<Vec<String>> = kernels
        .iter()
        .map(|k| {
            let queue = k.get("queue").and_then(Value::as_array).map_or(0, Vec::len);
            vec![
                str_field(k, "kernel_id").to_string(),
                str_field(k, "status").to_string(),
                k.get("run_id").and_then(Value::as_str).unwrap_or("-").to_string(),
                queue.to_string(),
                k.get("pid").map_or(String::new(), Value::to_string),
                k.pointer("/python/version").and_then(Value::as_str).unwrap_or("").to_string(),
                str_field(k, "path").to_string(),
            ]
        })
        .collect();
    table(&["KERNEL", "STATUS", "RUN", "QUEUE", "PID", "PYTHON", "PATH"], &rows)
}

/// `vars` rows from OpenAPI `Variable` objects.
pub fn variables_table(vars: &[Value]) -> String {
    let rows: Vec<Vec<String>> = vars
        .iter()
        .map(|v| {
            let mut info = Vec::new();
            if let Some(shape) = v.get("shape").and_then(Value::as_array) {
                let dims: Vec<String> = shape.iter().map(Value::to_string).collect();
                info.push(format!("shape=({})", dims.join(", ")));
            }
            if let Some(d) = v.get("dtype").and_then(Value::as_str) {
                info.push(format!("dtype={d}"));
            }
            if let Some(n) = v.get("len").and_then(Value::as_u64) {
                info.push(format!("len={n}"));
            }
            let repr = v.get("repr").and_then(Value::as_str).unwrap_or("").replace('\n', " ");
            vec![str_field(v, "name").into(), str_field(v, "type").into(), info.join(" "), repr]
        })
        .collect();
    table(&["NAME", "TYPE", "INFO", "VALUE"], &rows)
}

/// `status FILE` lines from an OpenAPI `Kernel` object.
pub fn kernel_details(k: &Value) -> String {
    let queue: Vec<&str> = k
        .get("queue")
        .and_then(Value::as_array)
        .map(|q| q.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let python = format!(
        "{} {} ({})",
        k.pointer("/python/implementation").and_then(Value::as_str).unwrap_or(""),
        k.pointer("/python/version").and_then(Value::as_str).unwrap_or(""),
        k.pointer("/python/executable").and_then(Value::as_str).unwrap_or(""),
    );
    let rows = [
        ("path", str_field(k, "path").to_string()),
        ("kernel", str_field(k, "kernel_id").to_string()),
        ("status", str_field(k, "status").to_string()),
        ("run", k.get("run_id").and_then(Value::as_str).unwrap_or("-").to_string()),
        ("queue", if queue.is_empty() { "-".into() } else { queue.join(", ") }),
        ("pid", k.get("pid").map_or(String::new(), Value::to_string)),
        ("python", python),
        ("started", str_field(k, "started_at").to_string()),
        ("runs", str_field(k, "runs_dir").to_string()),
    ];
    rows.iter().map(|(k, v)| format!("{k:<8} {v}\n")).collect()
}

/// One line describing a `RunSummary` (used for the busy hint).
pub fn run_summary_line(run: &Value) -> String {
    let mut s = format!("run {} ({})", str_field(run, "run_id"), str_field(run, "status"));
    let started = str_field(run, "started_at");
    if !started.is_empty() {
        s.push_str(&format!(", started {started}"));
    }
    if str_field(run, "mode") == "cells" {
        if let Some(cells) = run.get("cells") {
            s.push_str(&format!(", cells {cells}"));
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_fr_c2_exit_code_follows_run_status() {
        assert_eq!(exit_code_for_run_status("ok"), 0);
        assert_eq!(exit_code_for_run_status("error"), 1);
        assert_eq!(exit_code_for_run_status("interrupted"), 130);
        assert_eq!(exit_code_for_run_status("crashed"), 1);
        assert_eq!(exit_code_for_run_status("cancelled"), 1);
        assert_eq!(EXIT_BUSY, 75);
    }

    #[test]
    fn test_render_stream_outputs_go_to_their_stream() {
        let out = json!({"output_type": "stream", "name": "stdout", "text": "a\n"});
        assert_eq!(render_output(&out), Some((Stream::Stdout, "a\n".into())));
        let err = json!({"output_type": "stream", "name": "stderr", "text": ["x", "y\n"]});
        assert_eq!(render_output(&err), Some((Stream::Stderr, "xy\n".into())));
        // Partial lines (progress bars) are not given a newline.
        let p = json!({"output_type": "stream", "name": "stdout", "text": "50%\r"});
        assert_eq!(render_output(&p), Some((Stream::Stdout, "50%\r".into())));
    }

    #[test]
    fn test_render_rich_outputs_use_text_plain() {
        let r = json!({"output_type": "execute_result", "execution_count": 3,
                       "data": {"text/plain": "42", "text/html": "<b>42</b>"}, "metadata": {}});
        assert_eq!(render_output(&r), Some((Stream::Stdout, "42\n".into())));
        let md = json!({"output_type": "display_data", "data": {"text/markdown": ["# T", "\n"]}});
        assert_eq!(render_output(&md), Some((Stream::Stdout, "# T\n".into())));
        let img = json!({"output_type": "display_data", "data": {"image/png": "AAA"}});
        assert_eq!(render_output(&img), Some((Stream::Stdout, "[image/png]\n".into())));
    }

    #[test]
    fn test_render_error_prints_traceback_to_stderr() {
        let e = json!({"output_type": "error", "ename": "ZeroDivisionError", "evalue": "division by zero",
                       "traceback": ["Traceback (most recent call last):\n",
                                     "  File \"a.py\", line 3, in <module>\n",
                                     "ZeroDivisionError: division by zero\n"]});
        assert_eq!(
            render_output(&e),
            Some((
                Stream::Stderr,
                "Traceback (most recent call last):\n  File \"a.py\", line 3, in <module>\nZeroDivisionError: division by zero\n"
                    .into()
            ))
        );
        let bare = json!({"output_type": "error", "ename": "KeyboardInterrupt", "evalue": "", "traceback": []});
        assert_eq!(render_output(&bare), Some((Stream::Stderr, "KeyboardInterrupt: \n".into())));
    }

    #[test]
    fn test_render_unknown_output_is_skipped() {
        assert_eq!(render_output(&json!({"output_type": "weird"})), None);
        assert_eq!(render_output(&json!({})), None);
    }

    #[test]
    fn test_strip_ansi() {
        assert_eq!(strip_ansi("\x1b[0;31mValueError\x1b[0m: x"), "ValueError: x");
        assert_eq!(strip_ansi("plain"), "plain");
    }

    #[test]
    fn test_tables_align_columns() {
        let t = table(&["A", "BB"], &[vec!["xxx".into(), "y".into()]]);
        assert_eq!(t, "A    BB\nxxx  y\n");
        let k = json!({"kernel_id": "k_00000000000000000001", "path": "/w/a.py", "pid": 7, "status": "busy",
                       "run_id": "20261003-142233-a1f0", "queue": ["x"], "python": {"version": "3.11.9"},
                       "started_at": "t"});
        let ps = kernels_table(&[k]);
        assert!(ps.starts_with("KERNEL"));
        assert!(ps.contains("k_00000000000000000001  busy    20261003-142233-a1f0  1      7    3.11.9  /w/a.py"));
        let v = variables_table(&[json!({"name": "x", "type": "ndarray", "shape": [2, 3], "dtype": "float32",
                                         "repr": "array(...)"})]);
        assert!(v.contains("x     ndarray  shape=(2, 3) dtype=float32  array(...)"), "{v}");
    }
}
