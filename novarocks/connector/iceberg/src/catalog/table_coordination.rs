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

//! One logical catalog's table mutation coordination, shared across generations.
//!
//! The registry lock never crosses an await. Both holders and queued waiters
//! retain an entry lease; a temporarily unlocked mutex cannot split a table
//! into two coordination domains while a waiter is still queued.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use uuid::Uuid;

#[derive(Default)]
pub(crate) struct IcebergTableCoordination {
    entries: Mutex<BTreeMap<Uuid, Entry>>,
}

struct Entry {
    mutex: Arc<AsyncMutex<()>>,
    pins: usize,
}

impl IcebergTableCoordination {
    pub(crate) fn acquire_entry(
        self: &Arc<Self>,
        table_uuid: Uuid,
    ) -> Result<IcebergTableEntryPin, ConnectorError> {
        let mut entries = self.entries.lock().map_err(|_| unavailable())?;
        let entry = entries.entry(table_uuid).or_insert_with(|| Entry {
            mutex: Arc::new(AsyncMutex::new(())),
            pins: 0,
        });
        entry.pins = entry.pins.checked_add(1).ok_or_else(unavailable)?;
        Ok(IcebergTableEntryPin {
            owner: Arc::clone(self),
            table_uuid,
            mutex: Arc::clone(&entry.mutex),
        })
    }

    pub(crate) async fn acquire(
        self: &Arc<Self>,
        table_uuid: Uuid,
        request: &ConnectorRequestContext,
    ) -> Result<IcebergTableCoordinationGuard, ConnectorError> {
        request.check_active()?;
        self.acquire_entry(table_uuid)?.lock(request).await
    }

    #[cfg(test)]
    fn retained_entry_count(&self) -> usize {
        self.entries.lock().expect("coordination registry").len()
    }
}

pub(crate) struct IcebergTableEntryPin {
    owner: Arc<IcebergTableCoordination>,
    table_uuid: Uuid,
    mutex: Arc<AsyncMutex<()>>,
}

impl IcebergTableEntryPin {
    pub(crate) async fn lock(
        self,
        request: &ConnectorRequestContext,
    ) -> Result<IcebergTableCoordinationGuard, ConnectorError> {
        request.check_active()?;
        let stopped = request.stop().stopped();
        let deadline = tokio::time::sleep_until(tokio::time::Instant::from_std(request.deadline()));
        let guard = tokio::select! {
            biased;
            _ = stopped => return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled, "Iceberg table coordination wait was cancelled")),
            _ = deadline => return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded, "Iceberg table coordination deadline elapsed")),
            guard = Arc::clone(&self.mutex).lock_owned() => guard,
        };
        // Cancellation racing a grant still stops before fresh observation or
        // mutation dispatch. The caller retains the original request context.
        request.check_active()?;
        Ok(IcebergTableCoordinationGuard {
            _guard: guard,
            _entry: self,
        })
    }
}

impl Drop for IcebergTableEntryPin {
    fn drop(&mut self) {
        let Ok(mut entries) = self.owner.entries.lock() else {
            return;
        };
        let Some(entry) = entries.get_mut(&self.table_uuid) else {
            return;
        };
        // Check identity as well as the count: an old lease must never reclaim
        // a replacement entry for the same UUID.
        if !Arc::ptr_eq(&entry.mutex, &self.mutex) {
            return;
        }
        debug_assert!(entry.pins > 0, "coordination entry lease underflow");
        entry.pins -= 1;
        if entry.pins == 0 {
            entries.remove(&self.table_uuid);
        }
    }
}

/// Drop the owned mutex guard before unregistering its holder lease.
pub(crate) struct IcebergTableCoordinationGuard {
    _guard: OwnedMutexGuard<()>,
    _entry: IcebergTableEntryPin,
}

fn unavailable() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "Iceberg table coordination registry is unavailable",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorStopOwner;
    use std::time::{Duration, Instant};

    fn request(stop: &ConnectorStopOwner, timeout: Duration) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(Instant::now() + timeout, stop.view(), 1024, 1024)
            .expect("request")
    }

    #[tokio::test]
    async fn holders_and_waiters_keep_one_entry_until_the_last_lease_exits() {
        let coordination = Arc::new(IcebergTableCoordination::default());
        let stop = ConnectorStopOwner::new();
        let request = request(&stop, Duration::from_secs(5));
        let table = Uuid::now_v7();
        let holder = coordination.acquire(table, &request).await.expect("holder");
        let waiting = coordination.acquire_entry(table).expect("waiter entry");
        let same_mutex = Arc::clone(&waiting.mutex);
        let waiter = waiting.lock(&request);
        tokio::pin!(waiter);
        assert!(futures::poll!(&mut waiter).is_pending());
        drop(holder);
        assert_eq!(coordination.retained_entry_count(), 1);
        let next = coordination.acquire_entry(table).expect("new arrival");
        assert!(Arc::ptr_eq(&same_mutex, &next.mutex));
        let granted = waiter.await.expect("waiter grant");
        drop(granted);
        assert_eq!(coordination.retained_entry_count(), 1);
        drop(next);
        assert_eq!(coordination.retained_entry_count(), 0);
    }

    #[tokio::test]
    async fn another_table_progresses_and_cancelled_waiter_never_enters() {
        let coordination = Arc::new(IcebergTableCoordination::default());
        let stop = ConnectorStopOwner::new();
        let first_request = request(&stop, Duration::from_secs(5));
        let table = Uuid::now_v7();
        let holder = coordination
            .acquire(table, &first_request)
            .await
            .expect("holder");
        let other = coordination
            .acquire(Uuid::now_v7(), &first_request)
            .await
            .expect("other table");
        drop(other);
        let waiting_stop = ConnectorStopOwner::new();
        let waiting_request = request(&waiting_stop, Duration::from_secs(5));
        let waiting = coordination.acquire(table, &waiting_request);
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        waiting_stop.request_stop();
        assert!(
            matches!(waiting.await, Err(error) if error.kind() == ConnectorErrorKind::Cancelled)
        );
        assert_eq!(coordination.retained_entry_count(), 1);
        drop(holder);
        assert_eq!(coordination.retained_entry_count(), 0);
    }

    #[tokio::test]
    async fn deadline_wait_and_dropped_waiter_reclaim_without_a_grant() {
        let coordination = Arc::new(IcebergTableCoordination::default());
        let stop = ConnectorStopOwner::new();
        let request = request(&stop, Duration::from_secs(5));
        let table = Uuid::now_v7();
        let holder = coordination.acquire(table, &request).await.expect("holder");
        let short = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_millis(5),
            stop.view(),
            1024,
            1024,
        )
        .unwrap();
        assert!(matches!(coordination.acquire(table, &short).await,
            Err(error) if error.kind() == ConnectorErrorKind::DeadlineExceeded));
        {
            let waiting = coordination.acquire(table, &request);
            tokio::pin!(waiting);
            assert!(futures::poll!(&mut waiting).is_pending());
        }
        drop(holder);
        assert_eq!(coordination.retained_entry_count(), 0);
    }
}
