// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::identifier::TableIdentity;

struct CachedEntry<M> {
    schema_id: Option<i32>,
    metadata: M,
}

pub struct SchemaCache<M> {
    entries: RwLock<HashMap<TableIdentity, CachedEntry<M>>>,
}

impl<M> Default for SchemaCache<M> {
    fn default() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }
}

impl<M: Clone> SchemaCache<M> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_build_validated<F>(
        &self,
        id: &TableIdentity,
        current_schema_id: Option<i32>,
        builder: F,
    ) -> Result<M, String>
    where
        F: FnOnce() -> Result<M, String>,
    {
        {
            let entries = self.entries.read().expect("schema cache read lock");
            if let Some(entry) = entries.get(id)
                && entry.schema_id == current_schema_id
            {
                return Ok(entry.metadata.clone());
            }
        }

        let metadata = builder()?;
        self.entries
            .write()
            .expect("schema cache write lock")
            .insert(
                id.clone(),
                CachedEntry {
                    schema_id: current_schema_id,
                    metadata: metadata.clone(),
                },
            );
        Ok(metadata)
    }

    pub fn invalidate(&self, id: &TableIdentity) {
        self.entries
            .write()
            .expect("schema cache write lock")
            .remove(id);
    }

    pub fn invalidate_all(&self) {
        self.entries
            .write()
            .expect("schema cache write lock")
            .clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, mpsc};
    use std::time::Duration;

    use super::SchemaCache;
    use crate::identifier::TableIdentity;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestMetadata {
        revision: usize,
    }

    #[test]
    fn builds_on_miss_and_returns_cached_metadata_on_matching_schema_id() {
        let cache = SchemaCache::new();
        let id = TableIdentity::new("c", "ns", "t");
        let calls = AtomicUsize::new(0);
        let builder = || {
            let revision = calls.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(TestMetadata { revision })
        };

        assert_eq!(
            cache
                .get_or_build_validated(&id, Some(7), builder)
                .expect("build"),
            TestMetadata { revision: 1 }
        );
        assert_eq!(
            cache
                .get_or_build_validated(&id, Some(7), builder)
                .expect("cache hit"),
            TestMetadata { revision: 1 }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn schema_id_change_rebuilds_and_none_hits_until_invalidation() {
        let cache = SchemaCache::new();
        let versioned = TableIdentity::new("c", "ns", "versioned");
        let unversioned = TableIdentity::new("c", "ns", "unversioned");
        let calls = AtomicUsize::new(0);
        let builder = || {
            let revision = calls.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(TestMetadata { revision })
        };

        cache
            .get_or_build_validated(&versioned, Some(1), builder)
            .expect("build schema 1");
        assert_eq!(
            cache
                .get_or_build_validated(&versioned, Some(2), builder)
                .expect("rebuild schema 2"),
            TestMetadata { revision: 2 }
        );
        cache
            .get_or_build_validated(&unversioned, None, builder)
            .expect("build unversioned");
        assert_eq!(
            cache
                .get_or_build_validated(&unversioned, None, builder)
                .expect("unversioned hit"),
            TestMetadata { revision: 3 }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn explicit_table_and_all_invalidation_force_rebuilds() {
        let cache = SchemaCache::new();
        let first = TableIdentity::new("c", "ns", "first");
        let second = TableIdentity::new("c", "ns", "second");
        let calls = AtomicUsize::new(0);
        let builder = || {
            let revision = calls.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(TestMetadata { revision })
        };

        cache
            .get_or_build_validated(&first, Some(1), builder)
            .expect("build first");
        cache
            .get_or_build_validated(&second, Some(1), builder)
            .expect("build second");
        cache.invalidate(&first);
        assert_eq!(
            cache
                .get_or_build_validated(&first, Some(1), builder)
                .expect("rebuild invalidated table"),
            TestMetadata { revision: 3 }
        );
        assert_eq!(
            cache
                .get_or_build_validated(&second, Some(1), builder)
                .expect("other table remains cached"),
            TestMetadata { revision: 2 }
        );

        cache.invalidate_all();
        assert_eq!(
            cache
                .get_or_build_validated(&second, Some(1), builder)
                .expect("rebuild after invalidate all"),
            TestMetadata { revision: 4 }
        );
    }

    #[test]
    fn builder_runs_without_holding_the_cache_lock() {
        let cache = Arc::new(SchemaCache::new());
        let slow = TableIdentity::new("c", "ns", "slow");
        let ready = TableIdentity::new("c", "ns", "ready");
        cache
            .get_or_build_validated(&ready, Some(1), || Ok(TestMetadata { revision: 9 }))
            .expect("preload ready entry");

        let builder_started = Arc::new(Barrier::new(2));
        let release_builder = Arc::new(Barrier::new(2));
        let slow_handle = {
            let cache = Arc::clone(&cache);
            let started = Arc::clone(&builder_started);
            let release = Arc::clone(&release_builder);
            std::thread::spawn(move || {
                cache.get_or_build_validated(&slow, Some(1), || {
                    started.wait();
                    release.wait();
                    Ok(TestMetadata { revision: 1 })
                })
            })
        };

        builder_started.wait();
        let (sender, receiver) = mpsc::channel();
        let ready_handle = {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || {
                let result = cache.get_or_build_validated(&ready, Some(1), || {
                    panic!("ready entry should hit cache")
                });
                sender.send(result).expect("send ready result");
            })
        };
        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("ready lookup must not wait for slow builder")
                .expect("ready lookup"),
            TestMetadata { revision: 9 }
        );

        release_builder.wait();
        slow_handle
            .join()
            .expect("join slow builder")
            .expect("slow build");
        ready_handle.join().expect("join ready lookup");
    }

    #[test]
    fn concurrent_builds_remain_consistent() {
        let cache = Arc::new(SchemaCache::new());
        let id = TableIdentity::new("c", "ns", "t");
        let mut handles = Vec::new();

        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let id = id.clone();
            handles.push(std::thread::spawn(move || {
                cache
                    .get_or_build_validated(&id, Some(1), || Ok(TestMetadata { revision: 1 }))
                    .expect("concurrent lookup")
            }));
        }

        for handle in handles {
            assert_eq!(
                handle.join().expect("join lookup"),
                TestMetadata { revision: 1 }
            );
        }
        assert_eq!(
            cache
                .get_or_build_validated(&id, Some(1), || {
                    panic!("settled entry should hit cache")
                })
                .expect("settled lookup"),
            TestMetadata { revision: 1 }
        );
    }
}
