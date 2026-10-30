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

use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn completed() -> ConnectorMetadataMaintenanceOutcome {
    ExternalMutationOutcome::KnownUncommitted {
        failure: ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Conflict,
            "original rejection",
        ),
        cleanup: ExternalMutationFinalization::Complete,
    }
}
fn row() -> (Arc<Active>, Arc<Terminal>, Arc<MaintenanceExecution>) {
    let active = Arc::new(Mutex::new(HashMap::new()));
    let terminal = Arc::new(Mutex::new(HashMap::new()));
    let row = Arc::new(MaintenanceExecution::new(
        ConnectorMutationOperationId::new(),
        [7; 32],
        Arc::downgrade(&active),
        Arc::clone(&terminal),
    ));
    active
        .lock()
        .unwrap()
        .insert(row.operation, Arc::clone(&row));
    (active, terminal, row)
}

struct PendingPhysical {
    row: Weak<MaintenanceExecution>,
    ready: Arc<AtomicBool>,
    drops: Arc<AtomicUsize>,
}
impl Future for PendingPhysical {
    type Output = Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        // Real original-row poll must not hold its mutex across foreign code.
        assert!(self.row.upgrade().unwrap().state.try_lock().is_ok());
        if self.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(completed()))
        } else {
            Poll::Pending
        }
    }
}
impl Drop for PendingPhysical {
    fn drop(&mut self) {
        assert!(self.row.upgrade().unwrap().state.try_lock().is_ok());
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelled_borrow_keeps_original_active_future_until_actual_exit() {
    let (active, terminal, row) = row();
    let ready = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    row.install(Box::pin(PendingPhysical {
        row: Arc::downgrade(&row),
        ready: ready.clone(),
        drops: drops.clone(),
    }));
    let mut execution = ConnectorMetadataMaintenanceExecution::from_source(row.clone());
    assert_eq!(
        execution.cleanup_responsibility(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained
    );
    let mut wait = Box::pin(execution.wait());
    assert!(futures::poll!(&mut wait).is_pending());
    drop(wait);
    assert!(Arc::ptr_eq(
        active.lock().unwrap().get(&row.operation).unwrap(),
        &row
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(terminal.lock().unwrap().is_empty());
    ready.store(true, Ordering::SeqCst);
    assert!(matches!(
        execution.wait().await.unwrap(),
        ExternalMutationOutcome::KnownUncommitted { .. }
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(active.lock().unwrap().is_empty());
    assert_eq!(row.state.lock().unwrap().phase, Phase::Closed);
    assert_eq!(
        execution.cleanup_responsibility(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Disposed
    );
    assert_eq!(
        terminal
            .lock()
            .unwrap()
            .get(&row.operation)
            .unwrap()
            .plan_digest,
        [7; 32]
    );
}

struct PanicPayload(Arc<AtomicUsize>);
impl Drop for PanicPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct ReadyDropPanic {
    row: Weak<MaintenanceExecution>,
    terminal: Arc<Terminal>,
    operation: ConnectorMutationOperationId,
    payload_drops: Arc<AtomicUsize>,
}
impl Future for ReadyDropPanic {
    type Output = Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(completed()))
    }
}
impl Drop for ReadyDropPanic {
    fn drop(&mut self) {
        let row = self.row.upgrade().unwrap();
        assert!(row.state.try_lock().is_ok());
        assert!(row.retained_result().is_some());
        assert!(self.terminal.lock().unwrap().contains_key(&self.operation));
        std::panic::panic_any(PanicPayload(self.payload_drops.clone()));
    }
}

#[tokio::test]
async fn terminal_precedes_future_drop_panic_and_original_cause_stays_in_active_row() {
    let (active, terminal, row) = row();
    let payload_drops = Arc::new(AtomicUsize::new(0));
    row.install(Box::pin(ReadyDropPanic {
        row: Arc::downgrade(&row),
        terminal: terminal.clone(),
        operation: row.operation,
        payload_drops: payload_drops.clone(),
    }));
    let mut execution = ConnectorMetadataMaintenanceExecution::from_source(row.clone());
    let error = execution.wait().await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    assert!(matches!(
        execution.retained_result().unwrap().unwrap(),
        ExternalMutationOutcome::KnownUncommitted { .. }
    ));
    assert!(active.lock().unwrap().contains_key(&row.operation));
    assert_eq!(row.state.lock().unwrap().phase, Phase::Poisoned);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    drop(execution);
    drop(error);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    let disposition = active.lock().unwrap().remove(&row.operation);
    drop(disposition);
    drop(row);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
}
