// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.

//! FE-owned periodic delivery of complete catalog reachability snapshots.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_proto_codec::catalog::PruneCatalogsRequest;
use tokio::sync::Notify;
use tokio::task::JoinSet;

use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::transport::{CatalogPruneDispatchOutcome, prune_catalogs};
use novarocks_catalog_application::CatalogApplicationService;
use novarocks_query_application::api::BackendTopologyService;

#[derive(Clone, Debug)]
pub struct CatalogPruneConfig {
    pub interval: Duration,
    pub rpc_timeout: Duration,
    pub max_inflight: usize,
}

impl CatalogPruneConfig {
    pub fn try_new(
        interval: Duration,
        rpc_timeout: Duration,
        max_inflight: usize,
    ) -> Result<Self, String> {
        if interval.is_zero() || rpc_timeout.is_zero() || max_inflight == 0 {
            return Err("catalog prune configuration contains a zero bound".to_string());
        }
        Ok(Self {
            interval,
            rpc_timeout,
            max_inflight,
        })
    }
}

/// A best-effort worker: no failed or late prune changes query correctness.
pub(crate) struct FrontendCatalogPruneService {
    catalogs: Arc<CatalogApplicationService>,
    topology: BackendTopologyService,
    data_runtime: FrontendDataRuntime,
    config: CatalogPruneConfig,
    stopping: AtomicBool,
    wake: Notify,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl FrontendCatalogPruneService {
    pub(crate) fn new(
        catalogs: Arc<CatalogApplicationService>,
        topology: BackendTopologyService,
        data_runtime: FrontendDataRuntime,
        config: CatalogPruneConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            catalogs,
            topology,
            data_runtime,
            config,
            stopping: AtomicBool::new(false),
            wake: Notify::new(),
            worker: Mutex::new(None),
        })
    }

    pub(crate) fn start(self: &Arc<Self>) -> Result<(), String> {
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| "catalog prune worker lock is poisoned")?;
        if worker.is_some() {
            return Err("catalog prune worker is already running".to_string());
        }
        self.stopping.store(false, Ordering::Release);
        let service = Arc::clone(self);
        *worker = Some(self.data_runtime.spawn(async move {
            service.run().await;
        }));
        Ok(())
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) {
        self.request_stop_for_process_exit();
        let handle = self.worker.lock().ok().and_then(|mut worker| worker.take());
        if let Some(mut handle) = handle {
            if tokio::time::timeout(timeout, &mut handle).await.is_err() {
                handle.abort();
                let _ = handle.await;
            }
        }
    }

    /// Wakes a sleeping best-effort worker when the enclosing role has
    /// committed to process exit. Joining remains owned by bounded shutdown.
    pub(crate) fn request_stop_for_process_exit(&self) {
        self.stopping.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    async fn run(&self) {
        while !self.stopping.load(Ordering::Acquire) {
            self.dispatch_once().await;
            tokio::select! {
                () = tokio::time::sleep(self.config.interval) => {}
                () = self.wake.notified() => {}
            }
        }
    }

    async fn dispatch_once(&self) {
        let Some(reachable) = self.catalogs.reachable_catalog_handles() else {
            tracing::debug!(
                "catalog prune skipped because no complete desired-state snapshot is available"
            );
            return;
        };
        let request = match PruneCatalogsRequest::new(reachable) {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!(%error, "catalog prune snapshot is invalid; skipping round");
                return;
            }
        };
        let targets = match self.topology.snapshot() {
            Ok(snapshot) => snapshot.targets().to_vec(),
            Err(error) => {
                tracing::debug!(%error, "catalog prune skipped because backend topology is unavailable");
                return;
            }
        };
        let mut pending = targets.into_iter();
        let mut workers = JoinSet::new();
        loop {
            while workers.len() < self.config.max_inflight {
                let Some(target) = pending.next() else {
                    break;
                };
                let endpoint = match target.endpoint() {
                    Ok(endpoint) => endpoint,
                    Err(error) => {
                        tracing::warn!(%error, backend = target.backend_idx(), "catalog prune skipped invalid backend endpoint");
                        continue;
                    }
                };
                let request = request.clone();
                let data_runtime = self.data_runtime.clone();
                let timeout = self.config.rpc_timeout;
                workers.spawn_blocking(move || {
                    (
                        target.backend_idx(),
                        prune_catalogs(&data_runtime, endpoint, &request, timeout),
                    )
                });
            }
            let Some(result) = workers.join_next().await else {
                break;
            };
            match result {
                Ok((backend, Ok(CatalogPruneDispatchOutcome::Accepted))) => {
                    tracing::debug!(backend, "catalog prune accepted")
                }
                Ok((backend, Ok(CatalogPruneDispatchOutcome::Rejected { safe_detail }))) => {
                    tracing::warn!(backend, %safe_detail, "catalog prune rejected")
                }
                Ok((backend, Err(error))) => {
                    tracing::debug!(backend, %error, "catalog prune delivery failed")
                }
                Err(error) => tracing::warn!(%error, "catalog prune worker failed"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::CatalogPruneConfig;

    #[test]
    fn configuration_rejects_zero_safety_bounds() {
        assert!(CatalogPruneConfig::try_new(Duration::ZERO, Duration::from_secs(1), 1).is_err());
        assert!(CatalogPruneConfig::try_new(Duration::from_secs(1), Duration::ZERO, 1).is_err());
        assert!(
            CatalogPruneConfig::try_new(Duration::from_secs(1), Duration::from_secs(1), 0).is_err()
        );
    }
}
