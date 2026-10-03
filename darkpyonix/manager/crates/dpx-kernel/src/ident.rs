//! Kernel identity (PROTOCOL §2.6, FR-K2): `kernel_id = "k_" + hex(SHA-256(canonical))[:20]`
//! with `canonical = realpath(abspath(path))` (plus `normcase` on Windows).
//!
//! The functions port Python's `posixpath.abspath`/`realpath` so that the Rust manager and the
//! Python kernel derive the same id for every spelling of a path, including paths that do not
//! exist yet and paths whose case differs from the on-disk name.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// `"k_" + hex(SHA-256(canonical path as UTF-8))[:20]`.
pub fn kernel_id_for(path: &Path) -> String {
    kernel_id_for_canonical(&canonical_path(path))
}

/// The id of an already canonical path.
pub fn kernel_id_for_canonical(canonical: &str) -> String {
    let digest = hex::encode(Sha256::digest(canonical.as_bytes()));
    format!("k_{}", &digest[..20])
}

/// `realpath(abspath(path))`, plus `normcase` on Windows. Lossy for non-UTF-8 paths.
pub fn canonical_path(path: &Path) -> String {
    imp::canonical_path(path)
}

/// `os.path.abspath`: absolute and lexically normalised (no symlink resolution).
pub fn abspath(path: &Path) -> PathBuf {
    imp::abspath(path)
}

#[cfg(not(windows))]
mod imp {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    pub fn canonical_path(path: &Path) -> String {
        let abs = abspath_str(&path.to_string_lossy());
        realpath(&abs)
    }

    pub fn abspath(path: &Path) -> PathBuf {
        PathBuf::from(abspath_str(&path.to_string_lossy()))
    }

    fn cwd() -> String {
        std::env::current_dir()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "/".into())
    }

    /// `posixpath.abspath`.
    pub fn abspath_str(path: &str) -> String {
        if path.starts_with('/') {
            normpath(path)
        } else {
            normpath(&join(&cwd(), path))
        }
    }

    /// `posixpath.join(a, b)`.
    fn join(a: &str, b: &str) -> String {
        if b.starts_with('/') {
            b.to_string()
        } else if a.is_empty() || a.ends_with('/') {
            format!("{a}{b}")
        } else {
            format!("{a}/{b}")
        }
    }

    /// `posixpath.split`.
    fn split(path: &str) -> (String, String) {
        let i = path.rfind('/').map(|i| i + 1).unwrap_or(0);
        let (mut head, tail) = (path[..i].to_string(), path[i..].to_string());
        if !head.is_empty() && !head.chars().all(|c| c == '/') {
            head = head.trim_end_matches('/').to_string();
        }
        (head, tail)
    }

    /// `posixpath.normpath`.
    pub fn normpath(path: &str) -> String {
        if path.is_empty() {
            return ".".into();
        }
        let initial = if path.starts_with('/') {
            if path.starts_with("//") && !path.starts_with("///") {
                2
            } else {
                1
            }
        } else {
            0
        };
        let mut comps: Vec<&str> = Vec::new();
        for comp in path.split('/') {
            if comp.is_empty() || comp == "." {
                continue;
            }
            if comp != ".." || (initial == 0 && comps.is_empty()) || comps.last() == Some(&"..") {
                comps.push(comp);
            } else if !comps.is_empty() {
                comps.pop();
            }
        }
        let joined = comps.join("/");
        let out = format!("{}{}", "/".repeat(initial), joined);
        if out.is_empty() {
            ".".into()
        } else {
            out
        }
    }

    /// `posixpath.realpath` (non-strict).
    pub fn realpath(filename: &str) -> String {
        let mut seen = HashMap::new();
        let (path, _) = join_real(String::new(), filename, &mut seen, 0);
        abspath_str(&path)
    }

    fn join_real(
        mut path: String,
        rest: &str,
        seen: &mut HashMap<String, Option<String>>,
        depth: u32,
    ) -> (String, bool) {
        let mut rest = rest;
        if rest.starts_with('/') {
            rest = &rest[1..];
            path = "/".into();
        }
        if depth > 64 {
            return (join(&path, rest), false);
        }
        while !rest.is_empty() {
            let (name, tail) = match rest.find('/') {
                Some(i) => (&rest[..i], &rest[i + 1..]),
                None => (rest, ""),
            };
            rest = tail;
            if name.is_empty() || name == "." {
                continue;
            }
            if name == ".." {
                if !path.is_empty() {
                    let (head, last) = split(&path);
                    path = head;
                    if last == ".." {
                        path = join(&join(&path, ".."), "..");
                    }
                } else {
                    path = "..".into();
                }
                continue;
            }
            let newpath = join(&path, name);
            let is_link = std::fs::symlink_metadata(&newpath)
                .map(|m| m.file_type().is_symlink())
                .unwrap_or(false);
            if !is_link {
                path = newpath;
                continue;
            }
            if let Some(known) = seen.get(&newpath) {
                match known {
                    Some(resolved) => {
                        path = resolved.clone();
                        continue;
                    }
                    None => return (join(&newpath, rest), false), // symlink loop
                }
            }
            seen.insert(newpath.clone(), None);
            let target = match std::fs::read_link(&newpath) {
                Ok(t) => t.to_string_lossy().into_owned(),
                Err(_) => {
                    path = newpath;
                    continue;
                }
            };
            let (resolved, ok) = join_real(path, &target, seen, depth + 1);
            path = resolved;
            if !ok {
                return (join(&path, rest), false);
            }
            seen.insert(newpath, Some(path.clone()));
        }
        (path, true)
    }
}

#[cfg(windows)]
mod imp {
    use std::path::{Path, PathBuf};

    pub fn abspath(path: &Path) -> PathBuf {
        std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
    }

    fn strip_verbatim(s: &str) -> String {
        if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
            format!(r"\\{rest}")
        } else if let Some(rest) = s.strip_prefix(r"\\?\") {
            rest.to_string()
        } else {
            s.to_string()
        }
    }

    pub fn canonical_path(path: &Path) -> String {
        let abs = abspath(path);
        let real = match std::fs::canonicalize(&abs) {
            Ok(p) => strip_verbatim(&p.to_string_lossy()),
            Err(_) => abs.to_string_lossy().into_owned(),
        };
        real.replace('/', "\\").to_lowercase()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(windows))]
    #[test]
    fn normpath_matches_python() {
        use super::imp::normpath;
        assert_eq!(normpath("/a/./b/../c//d/"), "/a/c/d");
        assert_eq!(normpath("//a"), "//a");
        assert_eq!(normpath("///a/.."), "/");
        assert_eq!(normpath("/.."), "/");
        assert_eq!(normpath("../x"), "../x");
    }
}
