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

//! Original source Box custody, distinct from its local close callback.

use crate::task_execution::blocking_io::{
    ConnectorBlockingIoJoinPin, ConnectorBlockingIoResponsibility, ConnectorSourceCloseResult,
};
use novarocks_spi::connector::read_stack::{
    ConnectorReadDynamicFilterSnapshot, ConnectorReadSplit, ConnectorReadSplitSource,
    ConnectorSplitBatch, SplitSourceProfile,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

pub(crate) struct SupervisedSplitSource {
    cell: Option<Arc<SourceCell>>,
}
struct SourceCell {
    source: Mutex<Option<Box<dyn ConnectorReadSplitSource>>>,
    closed: AtomicBool,
    requested: AtomicBool,
    poisoned: AtomicBool,
    retired: AtomicBool,
    _plan_node_id: i32,
    owner: SourceOwner,
}
enum SourceOwner {
    Production {
        responsibility: ConnectorBlockingIoResponsibility,
    },
    #[cfg(test)]
    Fixture,
}
struct SourceLease<'a> {
    cell: &'a SourceCell,
    source: Option<Box<dyn ConnectorReadSplitSource>>,
}
impl SourceCell {
    fn borrow(&self) -> Result<SourceLease<'_>, ConnectorError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(invalid("original split source callback unwound"));
        }
        self.borrow_original()
    }
    // Retirement is a distinct once-only operation, not a retry of an unwound
    // enumeration/profile callback. It must still reach the same original Box.
    fn borrow_original(&self) -> Result<SourceLease<'_>, ConnectorError> {
        let source = self
            .source
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| invalid("original split source is busy or already retired"))?;
        Ok(SourceLease {
            cell: self,
            source: Some(source),
        })
    }
}
impl Drop for SourceLease<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.cell.poisoned.store(true, Ordering::Release);
        }
        let source = self.source.take();
        if source.is_none() {
            return;
        }
        let mut original = self.cell.source.lock().unwrap_or_else(|e| e.into_inner());
        if original.is_some() {
            drop(original);
            std::mem::forget(source);
            panic!("original split source lease returned into occupied cell");
        }
        *original = source;
    }
}
fn invalid(detail: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, detail)
}
impl SupervisedSplitSource {
    pub(crate) fn new(
        source: Box<dyn ConnectorReadSplitSource>,
        plan_node_id: i32,
        responsibility: ConnectorBlockingIoResponsibility,
    ) -> Self {
        Self {
            cell: Some(Arc::new(SourceCell {
                source: Mutex::new(Some(source)),
                closed: AtomicBool::new(false),
                requested: AtomicBool::new(false),
                poisoned: AtomicBool::new(false),
                retired: AtomicBool::new(false),
                _plan_node_id: plan_node_id,
                owner: SourceOwner::Production { responsibility },
            })),
        }
    }
    #[cfg(test)]
    pub(crate) fn fixture(source: Box<dyn ConnectorReadSplitSource>) -> Self {
        Self {
            cell: Some(Arc::new(SourceCell {
                source: Mutex::new(Some(source)),
                closed: AtomicBool::new(false),
                requested: AtomicBool::new(false),
                poisoned: AtomicBool::new(false),
                retired: AtomicBool::new(false),
                _plan_node_id: 0,
                owner: SourceOwner::Fixture,
            })),
        }
    }
    fn cell(&self) -> &Arc<SourceCell> {
        self.cell.as_ref().expect("original source cell retained")
    }
    pub(crate) fn join_pin(&self) -> Option<ConnectorBlockingIoJoinPin> {
        match &self.cell().owner {
            SourceOwner::Production { responsibility, .. } => Some(responsibility.join_pin()),
            #[cfg(test)]
            SourceOwner::Fixture => None,
        }
    }
    /// Call close once, install its typed Err in the SAME stable header, then
    /// retire the actual original Box and Adapter/provider backing lock-free.
    /// The caller is the source's already retained CPU/retirement author, not
    /// a new task started by Drop. A callback unwind poisons the same cell.
    pub(crate) fn retire_in(
        &mut self,
        result: &ConnectorSourceCloseResult<'_>,
    ) -> Result<(), ConnectorError> {
        if self.cell().retired.load(Ordering::Acquire) {
            return Ok(());
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.close())) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => result.record_error(error),
            Err(original) => result.record_panic(original),
        }
        let cell = self.cell();
        let mut source = cell.borrow_original()?;
        let raw = source
            .source
            .take()
            .expect("retiring the original source Box");
        let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(raw)));
        // This observes the SAME Box's destructor returning normally or by
        // unwind. It proves no successful Provider close; the original raw
        // cause remains in the header and still prevents healthy convergence.
        if let Err(cause) = dropped {
            result.record_source_drop_panic(cause);
        }
        cell.retired.store(true, Ordering::Release);
        Ok(())
    }
}
impl ConnectorReadSplitSource for SupervisedSplitSource {
    fn profile_snapshot(&self) -> SplitSourceProfile {
        self.cell()
            .borrow()
            .expect("original serial source profile lease")
            .source
            .as_ref()
            .unwrap()
            .profile_snapshot()
    }
    fn initial_dynamic_filter_wait_request(&self) -> std::time::Duration {
        self.cell()
            .borrow()
            .expect("original serial source wait lease")
            .source
            .as_ref()
            .unwrap()
            .initial_dynamic_filter_wait_request()
    }
    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &ConnectorReadDynamicFilterSnapshot,
    ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
        if self.cell().closed.load(Ordering::Acquire)
            || self.cell().requested.load(Ordering::Acquire)
        {
            return Ok(ConnectorSplitBatch::finished());
        }
        self.cell()
            .borrow()?
            .source
            .as_mut()
            .unwrap()
            .next_batch(max_size, dynamic_filter)
    }
    fn is_finished(&self) -> bool {
        if self.cell().closed.load(Ordering::Acquire)
            || self.cell().requested.load(Ordering::Acquire)
        {
            return true;
        }
        self.cell()
            .borrow()
            .expect("original serial source finished lease")
            .source
            .as_ref()
            .unwrap()
            .is_finished()
    }
    fn close(&mut self) -> Result<(), ConnectorError> {
        if self.cell().closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.cell()
            .borrow_original()?
            .source
            .as_mut()
            .unwrap()
            .close()
    }
}
impl Drop for SupervisedSplitSource {
    fn drop(&mut self) {
        let Some(cell) = self.cell.take() else {
            return;
        };
        if !cell.retired.load(Ordering::Acquire) {
            // No detached cleanup. An invalid whole-source owner Drop cannot
            // claim that the still-owned Provider/credential backing exited.
            // Original finite Work custody remains charged and stops drain.
            cell.requested.store(true, Ordering::Release);
            std::mem::forget(cell);
        }
    }
}
