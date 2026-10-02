//! Where the hub publishes ACME DNS-01 TXT records (SPEC FR-H5).
//!
//! The DNS provider for darkpyonix.dev is not chosen yet, so the provider sits behind
//! [`DnsProvider`]. [`MemoryDns`] keeps records in memory; it is what tests and a
//! development hub use.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

/// A boxed, sendable future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A DNS provider refused or failed a change.
#[derive(Debug, thiserror::Error)]
#[error("dns provider: {0}")]
pub struct DnsError(pub String);

/// Publishes and removes TXT records under the hub's zone.
pub trait DnsProvider: Send + Sync + 'static {
    /// Replaces the TXT values at `fqdn`.
    fn set_txt<'a>(
        &'a self,
        fqdn: &'a str,
        values: &'a [String],
    ) -> BoxFuture<'a, Result<(), DnsError>>;
    /// Removes every TXT value at `fqdn`.
    fn clear_txt<'a>(&'a self, fqdn: &'a str) -> BoxFuture<'a, Result<(), DnsError>>;
}

/// TXT records kept in memory.
#[derive(Debug, Clone, Default)]
pub struct MemoryDns {
    records: Arc<Mutex<BTreeMap<String, Vec<String>>>>,
}

impl MemoryDns {
    /// The TXT values currently published at `fqdn`.
    pub fn txt(&self, fqdn: &str) -> Option<Vec<String>> {
        self.records
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(fqdn)
            .cloned()
    }
}

impl DnsProvider for MemoryDns {
    fn set_txt<'a>(
        &'a self,
        fqdn: &'a str,
        values: &'a [String],
    ) -> BoxFuture<'a, Result<(), DnsError>> {
        Box::pin(async move {
            self.records
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(fqdn.to_string(), values.to_vec());
            Ok(())
        })
    }

    fn clear_txt<'a>(&'a self, fqdn: &'a str) -> BoxFuture<'a, Result<(), DnsError>> {
        Box::pin(async move {
            self.records
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(fqdn);
            Ok(())
        })
    }
}
