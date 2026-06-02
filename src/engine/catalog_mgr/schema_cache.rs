//! Schema cache for the connector metadata layer.
//!
//! Read-mostly cache of `TableMetadata` keyed by `TableIdentity`. Crucially it
//! NEVER drops entries as part of a refresh — staleness is handled by rebuild
//! (on `schema_id` mismatch) or explicit `invalidate`. This eliminates the
//! "table temporarily absent" window that the old per-query drop+reload had.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::engine::catalog_mgr::metadata::{TableIdentity, TableMetadata};

struct CachedEntry {
    /// schema id the cached metadata was built for. `None` means "not tracked"
    /// (the builder didn't probe a schema id); such entries hit on any `None`
    /// lookup and are refreshed only via `invalidate`.
    schema_id: Option<i32>,
    metadata: TableMetadata,
}

#[derive(Default)]
pub(crate) struct SchemaCache {
    entries: RwLock<HashMap<TableIdentity, CachedEntry>>,
}

impl SchemaCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Return cached metadata if present and its `schema_id` matches
    /// `current_schema_id`; otherwise build via `builder`, cache, and return.
    ///
    /// The builder runs WITHOUT holding the cache lock, so a slow remote build
    /// never blocks other tables. Two threads racing the same key may both
    /// build (builds are idempotent); the last write wins. There is never a
    /// window where the key is absent for an unrelated reader.
    pub(crate) fn get_or_build_validated<F>(
        &self,
        id: &TableIdentity,
        current_schema_id: Option<i32>,
        builder: F,
    ) -> Result<TableMetadata, String>
    where
        F: FnOnce() -> Result<TableMetadata, String>,
    {
        // Fast path: read lock, check hit + schema_id match.
        {
            let guard = self.entries.read().expect("schema cache read lock");
            if let Some(entry) = guard.get(id)
                && entry.schema_id == current_schema_id
            {
                return Ok(entry.metadata.clone());
            }
        }
        // Miss or stale: build without holding the lock.
        let metadata = builder()?;
        let mut guard = self.entries.write().expect("schema cache write lock");
        guard.insert(
            id.clone(),
            CachedEntry {
                schema_id: current_schema_id,
                metadata: metadata.clone(),
            },
        );
        Ok(metadata)
    }

    /// Drop the cached entry for one table (e.g. after a local write/DDL).
    pub(crate) fn invalidate(&self, id: &TableIdentity) {
        self.entries
            .write()
            .expect("schema cache write lock")
            .remove(id);
    }

    /// Drop all cached entries.
    pub(crate) fn invalidate_all(&self) {
        self.entries
            .write()
            .expect("schema cache write lock")
            .clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog_mgr::metadata::{TableBinding, TableIdentity, TableMetadata};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn meta(id: &TableIdentity, ncols: usize) -> TableMetadata {
        TableMetadata {
            identity: id.clone(),
            columns: Vec::with_capacity(ncols),
            iceberg_row_lineage_columns: vec![],
            binding: TableBinding::Internal {
                db_id: 1,
                table_id: 1,
            },
        }
    }

    #[test]
    fn builds_on_miss_and_caches_on_hit() {
        let cache = SchemaCache::new();
        let id = TableIdentity::new("c", "ns", "t");
        let calls = AtomicUsize::new(0);

        let build = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(meta(&id, 0))
        };

        let _ = cache
            .get_or_build_validated(&id, Some(1), build)
            .expect("build");
        let _ = cache
            .get_or_build_validated(&id, Some(1), build)
            .expect("hit");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "second call must hit cache"
        );
    }

    #[test]
    fn rebuilds_when_schema_id_changes() {
        let cache = SchemaCache::new();
        let id = TableIdentity::new("c", "ns", "t");
        let calls = AtomicUsize::new(0);
        let build = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(meta(&id, 0))
        };

        let _ = cache
            .get_or_build_validated(&id, Some(1), build)
            .expect("build v1");
        let _ = cache
            .get_or_build_validated(&id, Some(2), build)
            .expect("rebuild v2");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "schema_id change must rebuild"
        );
    }

    #[test]
    fn invalidate_forces_rebuild() {
        let cache = SchemaCache::new();
        let id = TableIdentity::new("c", "ns", "t");
        let calls = AtomicUsize::new(0);
        let build = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(meta(&id, 0))
        };

        let _ = cache
            .get_or_build_validated(&id, Some(1), build)
            .expect("build");
        cache.invalidate(&id);
        let _ = cache
            .get_or_build_validated(&id, Some(1), build)
            .expect("rebuild");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn none_schema_id_always_hits_after_build() {
        // P1 IcebergCatalog passes None (no schema_id probe yet): once built,
        // subsequent None lookups must hit the cache.
        let cache = SchemaCache::new();
        let id = TableIdentity::new("c", "ns", "t");
        let calls = AtomicUsize::new(0);
        let build = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(meta(&id, 0))
        };
        let _ = cache
            .get_or_build_validated(&id, None, build)
            .expect("build");
        let _ = cache.get_or_build_validated(&id, None, build).expect("hit");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_get_or_build_is_consistent() {
        let cache = Arc::new(SchemaCache::new());
        let id = TableIdentity::new("c", "ns", "t");
        let mut handles = vec![];
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let id = id.clone();
            handles.push(std::thread::spawn(move || {
                cache
                    .get_or_build_validated(&id, Some(1), || Ok(meta(&id, 0)))
                    .expect("build")
                    .identity
                    .table
                    .clone()
            }));
        }
        for h in handles {
            assert_eq!(h.join().expect("join"), "t");
        }
    }
}
