//! Small helpers: tokens, hashing, clock, input validation.

use std::time::{SystemTime, UNIX_EPOCH};

use data_encoding::BASE64URL_NOPAD;
use sha2::{Digest, Sha256};

/// Unix seconds.
pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A new bearer token: `<prefix>` followed by 32 random bytes in base64url.
pub(crate) fn new_token(prefix: &str) -> String {
    let bytes: [u8; 32] = rand::random();
    format!("{prefix}{}", BASE64URL_NOPAD.encode(&bytes))
}

/// Random lowercase hex of `n` bytes.
pub(crate) fn random_hex(n: usize) -> String {
    let bytes: Vec<u8> = (0..n).map(|_| rand::random::<u8>()).collect();
    hex::encode(bytes)
}

/// Tokens are stored as the hex SHA-256 of the token string.
pub(crate) fn hash_token(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// Constant-time string equality.
pub(crate) fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub(crate) fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// `^s_[0-9a-f]{16}$`
pub(crate) fn is_share_id(s: &str) -> bool {
    s.strip_prefix("s_")
        .is_some_and(|rest| is_lower_hex(rest, 16))
}

/// Labels that stay with the hub itself.
const RESERVED_NAMES: &[&str] = &[
    "www", "api", "relay", "ash", "hub", "dns", "ns1", "ns2", "mail", "admin", "docs", "status",
];

/// `^[a-z0-9]([a-z0-9-]{1,30}[a-z0-9])$` and not reserved.
pub(crate) fn is_valid_name(s: &str) -> bool {
    let b = s.as_bytes();
    let alnum = |c: &u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    (3..=32).contains(&b.len())
        && alnum(&b[0])
        && alnum(&b[b.len() - 1])
        && b.iter().all(|c| alnum(c) || *c == b'-')
        && !RESERVED_NAMES.contains(&s)
}

/// An ACME DNS-01 TXT value: base64url (no padding) of a SHA-256 digest, 43 characters.
pub(crate) fn is_acme_txt_value(s: &str) -> bool {
    s.len() == 43
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

/// The message a device signs to register (SPEC FR-H1).
pub(crate) fn registration_message(account_id: &str, challenge: &str) -> String {
    format!("darkpyonix-hub/v1/register\n{account_id}\n{challenge}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_documented_pattern() {
        assert!(is_valid_name("studio"));
        assert!(is_valid_name("my-mac-mini"));
        assert!(!is_valid_name("ab"));
        assert!(!is_valid_name("-abc"));
        assert!(!is_valid_name("abc-"));
        assert!(!is_valid_name("ABC"));
        assert!(!is_valid_name("relay"));
        assert!(!is_valid_name(&"a".repeat(33)));
    }

    #[test]
    fn share_ids_follow_the_documented_pattern() {
        assert!(is_share_id("s_0f1e2d3c4b5a6978"));
        assert!(!is_share_id("s_0F1E2D3C4B5A6978"));
        assert!(!is_share_id("s_0f1e"));
        assert!(!is_share_id("x_0f1e2d3c4b5a6978"));
    }

    #[test]
    fn ct_eq_compares_whole_strings() {
        assert!(ct_eq("abc", "abc"));
        assert!(!ct_eq("abc", "abd"));
        assert!(!ct_eq("abc", "abcd"));
    }
}
