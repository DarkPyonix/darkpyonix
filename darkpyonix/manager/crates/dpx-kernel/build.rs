//! Embeds the stdlib-Python kernel sources (the `darkpyonix/kernel` folder, which is the
//! `darkpyonix` package) into the binary (INTENT D3, D10). Generates `$OUT_DIR/embedded.rs` with the file
//! table and a content hash that names the extraction directory `<home>/runtime/<version>-<hash>/`.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

fn collect(dir: &Path, rel: &str, out: &mut Vec<(String, PathBuf)>, dirs: &mut Vec<PathBuf>) {
    dirs.push(dir.to_path_buf());
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .map(|e| e.expect("dir entry"))
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        let file_type = entry.file_type().expect("file type");
        let child = format!("{rel}/{name}");
        if file_type.is_dir() {
            if name == "__pycache__" || name.starts_with('.') {
                continue;
            }
            collect(&path, &child, out, dirs);
        } else if file_type.is_file() {
            if name.starts_with('.') || name.ends_with(".pyc") || name.ends_with(".pyo") {
                continue;
            }
            out.push((child, path));
        }
    }
}

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    // crates/dpx-kernel -> darkpyonix/manager/crates -> darkpyonix/ -> darkpyonix/kernel (the `darkpyonix` package)
    let root = manifest.join("../../../kernel");
    let root = root
        .canonicalize()
        .unwrap_or_else(|e| panic!("kernel sources {}: {e}", root.display()));

    let mut files = Vec::new();
    let mut dirs = Vec::new();
    collect(&root, "darkpyonix", &mut files, &mut dirs);
    assert!(
        files.iter().any(|(rel, _)| rel == "darkpyonix/__main__.py"),
        "darkpyonix/kernel/__main__.py missing from the embedded sources"
    );

    let mut hasher = Sha256::new();
    let mut table = String::from("pub const KERNEL_FILES: &[(&str, &[u8])] = &[\n");
    for (rel, path) in &files {
        let data = std::fs::read(path).expect("read kernel source");
        hasher.update(rel.as_bytes());
        hasher.update([0u8]);
        hasher.update((data.len() as u64).to_be_bytes());
        hasher.update(&data);
        let abs = path.to_str().expect("utf-8 path");
        writeln!(table, "    ({rel:?}, include_bytes!({abs:?})),").unwrap();
        println!("cargo:rerun-if-changed={abs}");
    }
    table.push_str("];\n");
    for dir in &dirs {
        println!("cargo:rerun-if-changed={}", dir.display());
    }
    let hash = hex::encode(hasher.finalize());
    writeln!(table, "pub const KERNEL_HASH: &str = {:?};", &hash[..16]).unwrap();

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("embedded.rs");
    std::fs::write(out, table).expect("write embedded.rs");
}
