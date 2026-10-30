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

//! Explicit pure transaction adapter for Frontend fixtures only.
//! The shared testkit's InMemory default keeps its Unsupported contract.
use super::*;
use novarocks_state_store_api::{
    AttemptId, AttemptSupervisor, CommitOutcome, Key, OriginalReadTransaction,
    OriginalWriteTransaction, Precondition, RangePage, RangeRequest, ReadTransaction, StateRecord,
    StateStoreErrorKind, StateStoreExecutionContext, StateStoreExecutionFailure, StoreIdentity,
    Value, WriteAttempt, WriteTransaction,
};

// Only this module's FA factories construct this concrete inner. They never
// expose the testkit's commit-hold injection hook; all actual operations below
// are the existing immediate in-memory map/CAS operations, with no I/O worker.
pub(super) struct PureOriginalMemoryStore {
    inner: Arc<InMemoryStateStore>,
}
impl PureOriginalMemoryStore {
    pub(super) fn new(inner: Arc<InMemoryStateStore>) -> Self {
        Self { inner }
    }
}
fn active(execution: &StateStoreExecutionContext) -> Result<(), StateStoreExecutionFailure> {
    if Instant::now() >= execution.deadline() {
        Err(StateStoreError::new(
            StateStoreErrorKind::DeadlineExceeded,
            "frontend pure memory operation deadline exceeded",
        )
        .into_original_failure())
    } else {
        Ok(())
    }
}
fn consumed() -> StateStoreExecutionFailure {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "frontend pure memory transaction was already consumed",
    )
    .into_original_failure()
}
#[async_trait::async_trait]
impl StateStore for PureOriginalMemoryStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }
    fn attempts(&self) -> &AttemptSupervisor {
        self.inner.attempts()
    }
    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }
    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        self.inner.begin_read().await
    }
    async fn begin_write(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        self.inner.begin_write(attempt, purpose).await
    }
    async fn begin_original_read_in(
        &self,
        execution: &StateStoreExecutionContext,
    ) -> Result<Box<dyn OriginalReadTransaction>, StateStoreExecutionFailure> {
        active(execution)?;
        Ok(Box::new(PureRead {
            transaction: Some(
                self.inner
                    .begin_read()
                    .await
                    .map_err(StateStoreError::into_original_failure)?,
            ),
            execution: execution.clone(),
        }))
    }
    async fn begin_original_write_in(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
        execution: &StateStoreExecutionContext,
    ) -> Result<Box<dyn OriginalWriteTransaction>, StateStoreExecutionFailure> {
        active(execution)?;
        let id = attempt.id();
        Ok(Box::new(PureWrite {
            attempt: id,
            transaction: Some(
                self.inner
                    .begin_write(attempt, purpose)
                    .await
                    .map_err(StateStoreError::into_original_failure)?,
            ),
            execution: execution.clone(),
        }))
    }
}
struct PureRead {
    transaction: Option<Box<dyn ReadTransaction>>,
    execution: StateStoreExecutionContext,
}
#[async_trait::async_trait]
impl OriginalReadTransaction for PureRead {
    async fn get_in(
        &mut self,
        key: &Key,
    ) -> Result<Option<StateRecord>, StateStoreExecutionFailure> {
        active(&self.execution)?;
        self.transaction
            .as_mut()
            .ok_or_else(consumed)?
            .get(key)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn range_in(
        &mut self,
        request: &RangeRequest,
    ) -> Result<RangePage, StateStoreExecutionFailure> {
        active(&self.execution)?;
        self.transaction
            .as_mut()
            .ok_or_else(consumed)?
            .range(request)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn close_in(&mut self) -> Result<(), StateStoreExecutionFailure> {
        if let Some(transaction) = self.transaction.take() {
            transaction
                .abort()
                .await
                .map_err(StateStoreError::into_original_failure)?;
        }
        Ok(())
    }
}
struct PureWrite {
    attempt: AttemptId,
    transaction: Option<Box<dyn WriteTransaction>>,
    execution: StateStoreExecutionContext,
}
#[async_trait::async_trait]
impl OriginalReadTransaction for PureWrite {
    async fn get_in(
        &mut self,
        key: &Key,
    ) -> Result<Option<StateRecord>, StateStoreExecutionFailure> {
        active(&self.execution)?;
        self.transaction
            .as_mut()
            .ok_or_else(consumed)?
            .get(key)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn range_in(
        &mut self,
        request: &RangeRequest,
    ) -> Result<RangePage, StateStoreExecutionFailure> {
        active(&self.execution)?;
        self.transaction
            .as_mut()
            .ok_or_else(consumed)?
            .range(request)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn close_in(&mut self) -> Result<(), StateStoreExecutionFailure> {
        if let Some(transaction) = self.transaction.take() {
            transaction
                .abort()
                .await
                .map_err(StateStoreError::into_original_failure)?;
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl OriginalWriteTransaction for PureWrite {
    fn attempt(&self) -> AttemptId {
        self.attempt
    }
    fn original_failure(&self) -> Option<StateStoreExecutionFailure> {
        None
    } // No physical worker exists.
    async fn put_in(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreExecutionFailure> {
        active(&self.execution)?;
        self.transaction
            .as_mut()
            .ok_or_else(consumed)?
            .put(key, value, precondition)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn delete_in(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreExecutionFailure> {
        active(&self.execution)?;
        self.transaction
            .as_mut()
            .ok_or_else(consumed)?
            .delete(key, precondition)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn commit_in(&mut self) -> Result<CommitOutcome, StateStoreExecutionFailure> {
        active(&self.execution)?;
        Ok(self.transaction.take().ok_or_else(consumed)?.commit().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_state_store_api::{
        StateStoreBlockingOriginal, StateStoreBlockingSlot, StateStoreExecutionPort,
    };
    struct PureContext;
    impl StateStoreExecutionPort for PureContext {
        fn bind_original_in(
            &self,
            _: &Arc<dyn StateStoreBlockingOriginal>,
            _: Instant,
            _: &mut Option<Box<dyn StateStoreBlockingSlot>>,
        ) -> Result<(), StateStoreExecutionFailure> {
            panic!("this explicit pure fixture must not request a physical worker");
        }
        fn request_progress(&self) {
            panic!("pure map operations do not create a worker inventory");
        }
    }
    fn execution() -> (Arc<dyn StateStoreExecutionPort>, StateStoreExecutionContext) {
        let port: Arc<dyn StateStoreExecutionPort> = Arc::new(PureContext);
        let context = StateStoreExecutionContext::new(
            &port,
            Instant::now() + std::time::Duration::from_secs(5),
        );
        (port, context)
    }
    #[tokio::test]
    async fn shared_default_remains_unsupported_but_fa_factory_explicitly_supports_pure_transactions()
     {
        let (_port, context) = execution();
        let shared = InMemoryStateStore::new("unchanged-default");
        let Err(refused) = shared.begin_original_read_in(&context).await else {
            panic!("shared default must remain unsupported");
        };
        assert!(
            refused
                .downcast_ref::<novarocks_state_store_api::StateStoreOriginalUnsupported>()
                .is_some()
        );
        let (attempt, observation) = shared.attempts().reserve().unwrap();
        let Err(refused) = shared
            .begin_original_write_in(attempt, "unchanged-default", &context)
            .await
        else {
            panic!("shared write default must remain unsupported");
        };
        assert!(
            refused
                .downcast_ref::<novarocks_state_store_api::StateStoreOriginalUnsupported>()
                .is_some()
        );
        drop(observation);
        assert_eq!(shared.attempts().outstanding(), 0);

        // Exercise the actual FA registry instance, not a standalone wrapper.
        let registry = super::super::registry();
        let mut host = FrontendStateStoreHost::open(
            &registry,
            super::super::input("explicit-fa-pure"),
            context.deadline(),
        )
        .await
        .unwrap();
        let store = host.state_store().unwrap();
        assert_eq!(
            host.original_blocking_sources().unwrap().1,
            store.attempts().scope()
        );
        let key = Key::try_from(bytes::Bytes::from_static(b"pure-key")).unwrap();
        let value = Value::try_from(bytes::Bytes::from_static(b"pure-value")).unwrap();
        let (attempt, observation) = store.attempts().reserve().unwrap();
        let id = attempt.id();
        let mut write = store
            .begin_original_write_in(attempt, "pure-cas", &context)
            .await
            .unwrap();
        assert_eq!(write.attempt(), id);
        write
            .put_in(key.clone(), value.clone(), Precondition::Absent)
            .await
            .unwrap();
        let CommitOutcome::Committed(receipt) = write.commit_in().await.unwrap() else {
            panic!("pure original commit");
        };
        assert_eq!(write.attempt(), id); // Identity survives consumption of the transaction.
        assert_eq!(
            observation.peek().unwrap(),
            Some(novarocks_state_store_api::AttemptOutcome::Committed(
                receipt
            ))
        );
        write.close_in().await.unwrap();
        drop(write);
        let mut read = store.begin_original_read_in(&context).await.unwrap();
        assert_eq!(read.get_in(&key).await.unwrap().unwrap().value, value);
        read.close_in().await.unwrap();
        drop(read);
        drop(observation);
        assert_eq!(store.attempts().outstanding(), 0);
        drop(store);
        host.shutdown(context.deadline()).await.unwrap();
    }
    #[tokio::test]
    async fn pure_adaptation_keeps_foreign_attempt_refusal_and_close_after_deadline() {
        let (_port, context) = execution();
        let inner = Arc::new(InMemoryStateStore::new("explicit-pure"));
        let store = PureOriginalMemoryStore::new(inner.clone());
        let foreign = InMemoryStateStore::new("foreign-pure");
        let (attempt, observation) = foreign.attempts().reserve().unwrap();
        let Err(cause) = store
            .begin_original_write_in(attempt, "foreign", &context)
            .await
        else {
            panic!("foreign scope must be refused");
        };
        assert_eq!(
            cause.downcast_ref::<StateStoreError>().unwrap().kind(),
            StateStoreErrorKind::InvalidRequest
        );
        drop(observation);
        assert_eq!(foreign.attempts().outstanding(), 0);
        let expired = context.with_deadline(Instant::now() - std::time::Duration::from_secs(1));
        let Err(cause) = store.begin_original_read_in(&expired).await else {
            panic!("expired read must be refused");
        };
        assert_eq!(
            cause.downcast_ref::<StateStoreError>().unwrap().kind(),
            StateStoreErrorKind::DeadlineExceeded
        );
        // This is the same actual pure transaction; only its business deadline
        // has elapsed. Close remains possible and does not create a worker.
        let mut read = PureRead {
            transaction: Some(inner.begin_read().await.unwrap()),
            execution: expired,
        };
        let key = Key::try_from(bytes::Bytes::from_static(b"pure-key")).unwrap();
        assert!(read.get_in(&key).await.is_err());
        read.close_in().await.unwrap();
        assert!(read.transaction.is_none());
    }
}
