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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use novarocks_spi::state_store::{
    MAX_KEY_BYTES, StateStore, StateStoreError, StateStoreErrorKind, StateStoreOpenRequest,
    StateStoreProviderDescriptor, StateStoreProviderFactory, StateStoreProviderInstance,
    StateStoreProviderLifecycle,
};

use super::{SQLITE_STATE_STORE_PROVIDER_ID, SqliteHistoryRetentionConfig, SqliteStateStore};

pub struct SqliteStateStoreProviderFactory {
    descriptor: StateStoreProviderDescriptor,
    path: PathBuf,
    history_retention: SqliteHistoryRetentionConfig,
}

impl SqliteStateStoreProviderFactory {
    pub fn new(path: PathBuf, history_retention: SqliteHistoryRetentionConfig) -> Self {
        Self {
            descriptor: StateStoreProviderDescriptor::new(
                SQLITE_STATE_STORE_PROVIDER_ID,
                MAX_KEY_BYTES,
            ),
            path,
            history_retention,
        }
    }
}

#[async_trait]
impl StateStoreProviderFactory for SqliteStateStoreProviderFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        if Instant::now() >= request.deadline {
            return Err(deadline_error());
        }
        let store =
            SqliteStateStore::open(self.path, self.history_retention, request.clone()).await?;
        if Instant::now() >= request.deadline {
            drop(store);
            return Err(deadline_error());
        }
        let state_store: Arc<dyn StateStore> = Arc::new(store);
        Ok(Box::new(SqliteStateStoreProviderInstance {
            descriptor: self.descriptor,
            lifecycle: StateStoreProviderLifecycle::Ready,
            state_store: Some(state_store),
        }))
    }
}

struct SqliteStateStoreProviderInstance {
    descriptor: StateStoreProviderDescriptor,
    lifecycle: StateStoreProviderLifecycle,
    state_store: Option<Arc<dyn StateStore>>,
}

#[async_trait]
impl StateStoreProviderInstance for SqliteStateStoreProviderInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        self.lifecycle
    }

    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        if self.lifecycle == StateStoreProviderLifecycle::Ready {
            self.state_store.clone()
        } else {
            None
        }
    }

    async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        if self.lifecycle == StateStoreProviderLifecycle::Stopped {
            return Ok(());
        }
        self.lifecycle = StateStoreProviderLifecycle::Draining;
        if self.state_store.is_none() {
            self.lifecycle = StateStoreProviderLifecycle::Stopped;
            return Ok(());
        }
        loop {
            if Arc::strong_count(
                self.state_store
                    .as_ref()
                    .expect("draining SQLite provider owns its store"),
            ) == 1
            {
                self.state_store.take();
                self.lifecycle = StateStoreProviderLifecycle::Stopped;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(deadline_error());
            }
            tokio::task::yield_now().await;
        }
    }
}

fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "SQLite state store provider deadline exceeded",
    )
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::task::Poll;
    use std::time::Duration;

    use tempfile::TempDir;

    use super::*;

    fn request(deadline: Instant) -> StateStoreOpenRequest {
        StateStoreOpenRequest {
            cluster_id: "cluster-a".to_owned(),
            limits: novarocks_spi::state_store::StateStoreLimits::default(),
            deadline,
        }
    }

    async fn open_instance(
        temp: &TempDir,
        deadline: Instant,
    ) -> Box<dyn StateStoreProviderInstance> {
        Box::new(SqliteStateStoreProviderFactory::new(
            temp.path().join("state-store.sqlite"),
            SqliteHistoryRetentionConfig::default(),
        ))
        .open(request(deadline))
        .await
        .expect("open SQLite provider instance")
    }

    #[tokio::test]
    async fn draining_instance_does_not_expose_store_after_deadline() {
        let temp = TempDir::new().unwrap();
        let mut instance = open_instance(&temp, Instant::now() + Duration::from_secs(5)).await;
        let held_store = instance.state_store().unwrap();

        let error = instance.shutdown(Instant::now()).await.unwrap_err();

        assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded);
        assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Draining);
        assert!(instance.state_store().is_none());
        drop(held_store);
        instance
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_shutdown_retains_owner_for_retry_and_reopen() {
        let temp = TempDir::new().unwrap();
        let mut instance = open_instance(&temp, Instant::now() + Duration::from_secs(5)).await;
        let held_store = instance.state_store().unwrap();
        let mut shutdown = Box::pin(instance.shutdown(Instant::now() + Duration::from_secs(5)));
        poll_fn(|context| match shutdown.as_mut().poll(context) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(result) => {
                panic!("shutdown must wait for the external store handle: {result:?}")
            }
        })
        .await;
        drop(shutdown);

        assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Draining);
        assert!(instance.state_store().is_none());
        let error = instance.shutdown(Instant::now()).await.unwrap_err();
        assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded);

        drop(held_store);
        instance
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Stopped);

        let mut reopened = open_instance(&temp, Instant::now() + Duration::from_secs(5)).await;
        reopened
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
