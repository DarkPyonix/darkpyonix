//! The discovery registry `<home>/kernels/<kernel_id>.json` (FR-D2, PROTOCOL §1, §2.4).

use std::path::Path;

use serde_json::{Map, Value};

use crate::process::pid_alive;

pub type Entry = Map<String, Value>;

pub fn entry_pid(entry: &Entry) -> u32 {
    entry
        .get("pid")
        .and_then(Value::as_u64)
        .map(|p| p.min(u32::MAX as u64) as u32)
        .unwrap_or(0)
}

/// Live entries of this user's kernels. Dead, foreign and unreadable entries are deleted
/// when `prune` is set (a kernel killed with `kill -9` disappears at the next scan).
pub fn scan(kernels_dir: &Path, user_tag: &str, prune: bool) -> Vec<Entry> {
    let mut names: Vec<String> = match std::fs::read_dir(kernels_dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect(),
        Err(_) => return Vec::new(),
    };
    names.sort();
    let mut out = Vec::new();
    for name in names {
        let Some(stem) = name.strip_suffix(".json") else {
            continue;
        };
        let path = kernels_dir.join(&name);
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => Vec::new(),
        };
        let entry = serde_json::from_slice::<Value>(&data)
            .ok()
            .and_then(|v| match v {
                Value::Object(m) => Some(m),
                _ => None,
            });
        let ok = entry.as_ref().is_some_and(|e| {
            e.get("user_tag").and_then(Value::as_str) == Some(user_tag)
                && e.get("kernel_id").and_then(Value::as_str) == Some(stem)
                && pid_alive(entry_pid(e))
        });
        if ok {
            out.push(entry.unwrap());
        } else if prune {
            let _ = std::fs::remove_file(&path);
        }
    }
    out
}

/// One live entry of this user, if any (does not prune others).
pub fn read(kernels_dir: &Path, user_tag: &str, kernel_id: &str) -> Option<Entry> {
    let data = std::fs::read(kernels_dir.join(format!("{kernel_id}.json"))).ok()?;
    let Value::Object(e) = serde_json::from_slice::<Value>(&data).ok()? else {
        return None;
    };
    let ok = e.get("user_tag").and_then(Value::as_str) == Some(user_tag)
        && e.get("kernel_id").and_then(Value::as_str) == Some(kernel_id)
        && pid_alive(entry_pid(&e));
    ok.then_some(e)
}
