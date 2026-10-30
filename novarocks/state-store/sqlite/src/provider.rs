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
use novarocks_state_store_api::{
    MAX_KEY_BYTES, StateStore, StateStoreBlockingOriginal, StateStoreError, StateStoreErrorKind,
    StateStoreOpenRequest, StateStoreProviderDescriptor, StateStoreProviderFactory,
    StateStoreProviderInstance, StateStoreProviderLifecycle,
};

use super::{SQLITE_STATE_STORE_PROVIDER_ID, SqliteStateStore};

pub struct SqliteStateStoreProviderFactory {
    descriptor: StateStoreProviderDescriptor,
    path: PathBuf,
}

impl SqliteStateStoreProviderFactory {
    pub fn new(path: PathBuf) -> Self {
        Self {
            descriptor: StateStoreProviderDescriptor::new(
                SQLITE_STATE_STORE_PROVIDER_ID,
                MAX_KEY_BYTES,
            ),
            path,
        }
    }
}

#[async_trait]
impl StateStoreProviderFactory for SqliteStateStoreProviderFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    fn blocking_dependency_facts(
        &self,
    ) -> Option<Vec<novarocks_state_store_api::SharedDependency>> {
        Some(vec![novarocks_state_store_api::SharedDependency::LocalHost])
    }
    fn blocking_source_vocabulary(
        &self,
    ) -> Option<novarocks_state_store_api::StateStoreBlockingSourceVocabulary> {
        Some(super::original::vocabulary())
    }
    fn prepare_open_in(
        self: Box<Self>,
        request: StateStoreOpenRequest,
        execution: &novarocks_state_store_api::StateStoreExecutionContext,
        target: &mut Option<Box<dyn novarocks_state_store_api::StateStoreOriginalOpen>>,
    ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
        super::original::prepare_open_in(self.path, self.descriptor, request, execution, target)
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        if Instant::now() >= request.deadline {
            return Err(deadline_error());
        }
        let store = SqliteStateStore::open(self.path, request.clone()).await?;
        if Instant::now() >= request.deadline {
            drop(store);
            return Err(deadline_error());
        }
        Ok(instance_from_store(self.descriptor, store))
    }
}

struct SqliteStateStoreProviderInstance {
    descriptor: StateStoreProviderDescriptor,
    lifecycle: StateStoreProviderLifecycle,
    state_store: Option<Arc<SqliteStateStore>>,
    original_close: Option<Arc<super::original::OpenOriginal>>,
    lifetime: Option<Arc<super::SqliteInstanceLifetime>>,
    scope: novarocks_state_store_api::InstanceScope,
}

pub(super) fn instance_from_store(
    descriptor: StateStoreProviderDescriptor,
    store: SqliteStateStore,
) -> Box<dyn StateStoreProviderInstance> {
    let lifetime = store.lifetime.clone();
    let scope = store.attempts.scope();
    Box::new(SqliteStateStoreProviderInstance {
        descriptor,
        lifecycle: StateStoreProviderLifecycle::Ready,
        state_store: Some(Arc::new(store)),
        original_close: None,
        lifetime: Some(lifetime),
        scope,
    })
}

#[async_trait]
impl StateStoreProviderInstance for SqliteStateStoreProviderInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    fn blocking_dependency_facts(
        &self,
    ) -> Option<Vec<novarocks_state_store_api::SharedDependency>> {
        Some(vec![novarocks_state_store_api::SharedDependency::LocalHost])
    }
    fn blocking_source_vocabulary(
        &self,
    ) -> Option<novarocks_state_store_api::StateStoreBlockingSourceVocabulary> {
        Some(super::original::vocabulary())
    }
    fn blocking_instance_scope(&self) -> Option<novarocks_state_store_api::InstanceScope> {
        Some(self.scope)
    }
    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        if self
            .original_close
            .as_ref()
            .is_some_and(|source| source.is_complete() && source.local_failure().is_none())
        {
            StateStoreProviderLifecycle::Stopped
        } else {
            self.lifecycle
        }
    }

    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        if self.lifecycle == StateStoreProviderLifecycle::Ready {
            self.state_store
                .as_ref()
                .map(|store| Arc::clone(store) as Arc<dyn StateStore>)
        } else {
            None
        }
    }

    fn prepare_close_in(
        &mut self,
        execution: &novarocks_state_store_api::StateStoreExecutionContext,
        target: &mut Option<Box<dyn novarocks_state_store_api::StateStoreOriginalClose>>,
    ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
        if let Some(source) = &self.original_close {
            return super::original::borrow_instance_close_in(source, execution, target);
        }
        if self.lifecycle != StateStoreProviderLifecycle::Ready {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "SQLite instance has already entered legacy draining",
            )
            .into_original_failure());
        }
        let source = super::original::prepare_instance_close_in(
            self.descriptor,
            self.scope,
            self.state_store.as_ref().unwrap().clone(),
            self.lifetime.as_ref().unwrap().clone(),
            execution,
            target,
        )?;
        // All fallible receiving work precedes this same-owner transfer. These
        // provider aliases cannot be the last: the installed Source owns both.
        self.lifecycle = StateStoreProviderLifecycle::Draining;
        self.original_close = Some(source.clone());
        let store = self.state_store.take();
        let lifetime = self.lifetime.take();
        drop(store);
        drop(lifetime);
        source.request_close();
        Ok(())
    }

    async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        if let Some(source) = &self.original_close {
            if source.is_complete() && source.local_failure().is_none() {
                self.lifecycle = StateStoreProviderLifecycle::Stopped;
                return Ok(());
            }
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "SQLite original instance close has not actually completed",
            ));
        }
        if self.lifecycle == StateStoreProviderLifecycle::Stopped {
            return Ok(());
        }
        self.lifecycle = StateStoreProviderLifecycle::Draining;
        loop {
            if self
                .state_store
                .as_ref()
                .is_some_and(|store| Arc::strong_count(store) == 1)
            {
                self.state_store.take();
            }
            if self.state_store.is_none()
                && self
                    .lifetime
                    .as_ref()
                    .is_none_or(|lease| Arc::strong_count(lease) == 1)
            {
                self.lifetime.take();
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
            limits: novarocks_state_store_api::StateStoreLimits::default(),
            deadline,
        }
    }

    async fn open_instance(
        temp: &TempDir,
        deadline: Instant,
    ) -> Box<dyn StateStoreProviderInstance> {
        Box::new(SqliteStateStoreProviderFactory::new(
            temp.path().join("state-store.sqlite"),
        ))
        .open(request(deadline))
        .await
        .expect("open SQLite provider instance")
    }

    #[tokio::test]
    async fn original_sqlite_dependencies_come_from_factory_and_actual_instance() {
        let temp = TempDir::new().unwrap();
        let factory = SqliteStateStoreProviderFactory::new(temp.path().join("selected.sqlite"));
        let expected = factory
            .blocking_dependency_facts()
            .expect("actual local SQLite author");
        assert_eq!(
            expected,
            vec![novarocks_state_store_api::SharedDependency::LocalHost]
        );
        let mut instance = Box::new(factory)
            .open(request(Instant::now() + Duration::from_secs(5)))
            .await
            .unwrap();
        assert_eq!(
            instance.blocking_dependency_facts().as_ref(),
            Some(&expected)
        );
        instance
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
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
