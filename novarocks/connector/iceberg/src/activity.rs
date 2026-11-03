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

//! Activity follows actual resource/work ownership. It is never publication state.

use novarocks_spi::connector::{
    CatalogActivityEvents, CatalogActivityPhase as Phase, CatalogActivityReader,
    CatalogActivitySnapshot, CatalogGenerationActivity, CatalogPhaseActivity, CatalogScopeIdentity,
    ProviderBindingEpoch,
};
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

fn add(value: &AtomicU64, delta: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        Some(n.saturating_add(delta))
    });
}
#[derive(Default)]
struct PhaseCounter {
    inflight: AtomicU64,
    started: AtomicU64,
    exited: AtomicU64,
    elapsed_count: AtomicU64,
    elapsed_sum_ns: AtomicU64,
    elapsed_max_ns: AtomicU64,
}
impl PhaseCounter {
    fn enter(&self) {
        add(&self.started, 1);
        add(&self.inflight, 1);
    }
    fn exit(&self, elapsed: u64) {
        add(&self.exited, 1);
        add(&self.elapsed_count, 1);
        add(&self.elapsed_sum_ns, elapsed);
        self.elapsed_max_ns.fetch_max(elapsed, Ordering::Relaxed);
        let _ = self
            .inflight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
    }
    fn snapshot(&self, phase: Phase) -> CatalogPhaseActivity {
        CatalogPhaseActivity {
            phase,
            inflight: self.inflight.load(Ordering::Relaxed),
            started: self.started.load(Ordering::Relaxed),
            exited: self.exited.load(Ordering::Relaxed),
            elapsed_count: self.elapsed_count.load(Ordering::Relaxed),
            elapsed_sum_ns: self.elapsed_sum_ns.load(Ordering::Relaxed),
            elapsed_max_ns: self.elapsed_max_ns.load(Ordering::Relaxed),
        }
    }
}
#[derive(Default)]
struct Counters {
    phases: [PhaseCounter; Phase::ALL.len()],
    cancelled: AtomicU64,
    timed_out: AtomicU64,
    expired: AtomicU64,
    spawn_failed: AtomicU64,
}
impl Counters {
    fn phases(&self) -> Vec<CatalogPhaseActivity> {
        Phase::ALL
            .into_iter()
            .map(|p| self.phases[p as usize].snapshot(p))
            .collect()
    }
    fn events(&self) -> CatalogActivityEvents {
        CatalogActivityEvents {
            pre_dispatch_cancelled: self.cancelled.load(Ordering::Relaxed),
            pre_dispatch_timed_out: self.timed_out.load(Ordering::Relaxed),
            evidence_expired: self.expired.load(Ordering::Relaxed),
            bridge_spawn_failed: self.spawn_failed.load(Ordering::Relaxed),
        }
    }
}
pub(crate) struct CatalogActivity {
    identity: CatalogScopeIdentity,
    counters: Counters,
    generations: Mutex<Vec<Weak<GenerationActivity>>>,
}
impl CatalogActivity {
    pub(crate) fn new(identity: CatalogScopeIdentity) -> Arc<Self> {
        Arc::new(Self {
            identity,
            counters: Counters::default(),
            generations: Mutex::new(Vec::new()),
        })
    }
    pub(crate) fn generation(self: &Arc<Self>) -> Arc<GenerationActivity> {
        let child = Arc::new(GenerationActivity {
            root: self.clone(),
            epoch: OnceLock::new(),
            counters: Counters::default(),
        });
        let mut rows = self
            .generations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rows.retain(|row| row.strong_count() > 0);
        rows.push(Arc::downgrade(&child));
        child
    }
}
impl CatalogActivityReader for CatalogActivity {
    fn snapshot(&self) -> CatalogActivitySnapshot {
        let children = {
            let mut rows = self
                .generations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            rows.retain(|row| row.strong_count() > 0);
            rows.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
        };
        CatalogActivitySnapshot {
            provider_id: self.identity.provider_id().as_str().to_owned(),
            catalog: self.identity.instance_id().as_str().to_owned(),
            source_entry_id: self.identity.source_entry_id(),
            phases: self.counters.phases(),
            events: self.counters.events(),
            generations: children
                .iter()
                .map(|child| CatalogGenerationActivity {
                    generation: child.epoch.get().copied().map(uuid::Uuid::from_bytes),
                    phases: child.counters.phases(),
                    events: child.counters.events(),
                })
                .collect(),
        }
    }
}
pub(crate) struct GenerationActivity {
    root: Arc<CatalogActivity>,
    epoch: OnceLock<[u8; 16]>,
    counters: Counters,
}
impl GenerationActivity {
    pub(crate) fn bind_epoch(&self, epoch: ProviderBindingEpoch) {
        // The catalog owner already enforces the exact epoch. Observation may
        // neither replace it nor make business work fail.
        let _ = self.epoch.set(epoch.to_bytes());
    }
    pub(crate) fn enter(self: &Arc<Self>, phase: Phase) -> ActivityGuard {
        self.counters.phases[phase as usize].enter();
        self.root.counters.phases[phase as usize].enter();
        ActivityGuard {
            owner: self.clone(),
            phase,
            started: Instant::now(),
        }
    }
    pub(crate) fn preparation_failed(
        &self,
        failure: &novarocks_spi::connector::ConnectorMutationFailure,
    ) {
        use novarocks_spi::connector::ConnectorMutationFailureKind as Kind;
        match failure.kind() {
            Kind::Cancelled => {
                add(&self.counters.cancelled, 1);
                add(&self.root.counters.cancelled, 1);
            }
            Kind::DeadlineExceeded => {
                add(&self.counters.timed_out, 1);
                add(&self.root.counters.timed_out, 1);
            }
            _ => {}
        }
    }
    pub(crate) fn evidence_expired(&self) {
        add(&self.counters.expired, 1);
        add(&self.root.counters.expired, 1);
    }
    pub(crate) fn bridge_spawn_failed(&self) {
        add(&self.counters.spawn_failed, 1);
        add(&self.root.counters.spawn_failed, 1);
    }
}
/// Non-cloneable: one actual phase lifetime releases exactly one charge.
pub(crate) struct ActivityGuard {
    owner: Arc<GenerationActivity>,
    phase: Phase,
    started: Instant,
}
impl Drop for ActivityGuard {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.owner.counters.phases[self.phase as usize].exit(elapsed);
        self.owner.root.counters.phases[self.phase as usize].exit(elapsed);
    }
}

impl crate::resources::IcebergCatalogRuntime {
    /// Observe one explicit mutation preparation segment. The caller retains
    /// its original error and remains the sole publication/cleanup authority.
    /// `classify` must be a pure projection of the existing typed error.
    pub(crate) fn observe_preparation<T, E>(
        &self,
        prepare: impl FnOnce() -> Result<T, E>,
        classify: impl FnOnce(&E) -> novarocks_spi::connector::ConnectorMutationFailure,
    ) -> Result<T, E> {
        let _preparing = self
            .activity()
            .map(|owner| owner.enter(novarocks_spi::connector::CatalogActivityPhase::PreDispatch));
        let result = prepare();
        if let (Some(owner), Err(error)) = (self.activity(), &result) {
            owner.preparation_failed(&classify(error));
        }
        result
    }

    /// The guard lives until the supplied future returns or actually drops.
    /// This does not make non-abortable work cancellation-safe: the existing
    /// I/O owner must still retain and join that work to its actual exit.
    /// Do not wrap dispatch/finalization/cleanup or a runtime's ordinary reads.
    pub(crate) async fn observe_preparation_async<T, E>(
        &self,
        preparation: impl std::future::Future<Output = Result<T, E>>,
        classify: impl FnOnce(&E) -> novarocks_spi::connector::ConnectorMutationFailure,
    ) -> Result<T, E> {
        let _preparing = self
            .activity()
            .map(|owner| owner.enter(novarocks_spi::connector::CatalogActivityPhase::PreDispatch));
        let result = preparation.await;
        if let (Some(owner), Err(error)) = (self.activity(), &result) {
            owner.preparation_failed(&classify(error));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId};
    pub(crate) fn scope(name: &str) -> Arc<CatalogActivity> {
        CatalogActivity::new(CatalogScopeIdentity::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            ConnectorInstanceId::parse(name).unwrap(),
            uuid::Uuid::now_v7(),
        ))
    }
    pub(crate) fn inflight(root: &CatalogActivity, phase: Phase) -> u64 {
        root.snapshot().phases[phase as usize].inflight
    }
    #[test]
    fn detail_retains_only_live_generations_and_counts_guard_ownership_once() {
        let root = scope("catalog.one");
        for _ in 0..1000 {
            drop(root.generation());
        }
        assert!(root.snapshot().generations.is_empty());
        let child = root.generation();
        let epoch = ProviderBindingEpoch::new();
        child.bind_epoch(epoch);
        child.bind_epoch(epoch);
        let guard = child.enter(Phase::Evidence);
        drop(child);
        let snapshot = root.snapshot();
        assert_eq!(snapshot.generations.len(), 1);
        assert_eq!(
            snapshot.generations[0].generation.unwrap().as_bytes(),
            &epoch.to_bytes()
        );
        assert_eq!(inflight(&root, Phase::Evidence), 1);
        drop(guard);
        let snapshot = root.snapshot();
        assert!(snapshot.generations.is_empty());
        let phase = &snapshot.phases[Phase::Evidence as usize];
        assert_eq!(
            (
                phase.inflight,
                phase.started,
                phase.exited,
                phase.elapsed_count
            ),
            (0, 1, 1, 1)
        );
    }
    #[test]
    fn explicit_runtime_bridge_stays_visible_until_join_and_isolates_catalogs() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let root = scope("catalog.blocked");
        let other = scope("catalog.normal");
        let child = root.generation();
        let resources = crate::resources::IcebergMetadataResources::new(
            crate::access_binding::IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                    runtime.handle().clone(),
                )),
                Arc::new(novarocks_fs::TokioFileIoSpawner::new(
                    runtime.handle().clone(),
                )),
            ),
            runtime.handle().clone(),
        )
        .with_activity(child);
        let bridge = resources.catalog_runtime().clone();
        let entered = Arc::new(tokio::sync::Notify::new());
        let exit = Arc::new(tokio::sync::Notify::new());
        let work_entered = entered.clone();
        let work_exit = exit.clone();
        let worker = std::thread::spawn(move || {
            bridge.block_on(async move {
                work_entered.notify_one();
                work_exit.notified().await;
                7_u8
            })
        });
        runtime.block_on(entered.notified());
        assert_eq!(inflight(&root, Phase::Bridge), 1);
        assert_eq!(inflight(&other, Phase::Bridge), 0);
        let other_resources = resources.clone().with_activity(other.generation());
        assert_eq!(
            other_resources.catalog_runtime().block_on(async { 9_u8 }),
            Ok(9)
        );
        assert_eq!(other.snapshot().phases[Phase::Bridge as usize].exited, 1);
        assert_eq!(inflight(&root, Phase::Bridge), 1);
        assert!(!worker.is_finished());
        exit.notify_one();
        assert_eq!(worker.join().unwrap(), Ok(7));
        let phase = &root.snapshot().phases[Phase::Bridge as usize];
        assert_eq!(
            (phase.inflight, phase.exited, phase.elapsed_count),
            (0, 1, 1)
        );
        assert_eq!(other.snapshot().phases[Phase::Bridge as usize].started, 1);
    }
}
