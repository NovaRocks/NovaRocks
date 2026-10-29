// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual publication cells. Generation validation and value replacement share
//! the scope-failure gate; host I/O happens only after commit returns.

use crate::{
    sync::{Arc, Mutex, MutexGuard},
    transaction::ScopeHandle,
};

pub struct PublishedSnapshot<T> {
    pub(crate) owner: ScopeHandle,
    pub(crate) value: Arc<Mutex<Arc<T>>>,
}
/// A short read of the actual cell without cloning its possibly rich owner.
/// Copy only known neutral values while borrowed; never invoke host callbacks,
/// retain this guard across a wait, or destroy an opaque source under it.
pub struct SnapshotBorrow<'a, T> {
    value: MutexGuard<'a, Arc<T>>,
}
impl<T> std::ops::Deref for SnapshotBorrow<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T> Clone for PublishedSnapshot<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            value: self.value.clone(),
        }
    }
}
impl<T> PublishedSnapshot<T> {
    /// Qualify only the original owner scope. Matching grants no publication
    /// action, resource credit, current-generation start or physical exit.
    pub fn owned_by(&self, owner: &ScopeHandle) -> bool {
        self.owner.same_scope(owner)
    }
    /// `initial` is the already published bootstrap projection. Every later
    /// business update must use ActorRuntime::commit_snapshot.
    pub fn new(owner: &ScopeHandle, initial: Arc<T>) -> Self {
        Self {
            owner: owner.clone(),
            value: Arc::new(Mutex::new(initial)),
        }
    }
    /// Readers only clone the actual immutable projection; no ledger lock or
    /// domain callback participates in the read.
    pub fn read(&self) -> Arc<T> {
        self.value.lock().expect("published snapshot lock").clone()
    }
    /// A nonblocking read of the same cell without cloning its rich projection.
    /// Busy or poisoned cells are unavailable; the ordinary read paths are unchanged.
    pub fn try_borrow(&self) -> Option<SnapshotBorrow<'_, T>> {
        self.value
            .try_lock()
            .ok()
            .map(|value| SnapshotBorrow { value })
    }
    pub fn borrow(&self) -> SnapshotBorrow<'_, T> {
        SnapshotBorrow {
            value: self.value.lock().expect("published snapshot lock"),
        }
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum ReplyPhase {
    Empty,
    Committed,
    Taken,
}
pub(crate) struct ReplyCell<T> {
    pub(crate) phase: ReplyPhase,
    pub(crate) value: Option<Arc<T>>,
}
pub struct PublishedReply<T> {
    pub(crate) owner: ScopeHandle,
    pub(crate) cell: Arc<Mutex<ReplyCell<T>>>,
}
impl<T> Clone for PublishedReply<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            cell: self.cell.clone(),
        }
    }
}
impl<T> PublishedReply<T> {
    pub fn new(owner: &ScopeHandle) -> Self {
        Self {
            owner: owner.clone(),
            cell: Arc::new(Mutex::new(ReplyCell {
                phase: ReplyPhase::Empty,
                value: None,
            })),
        }
    }
    /// The sending owner consumes the committed outbox value exactly once.
    /// Taking/sending cannot reopen the cell for a second reply.
    pub fn take_once(&self) -> Option<Arc<T>> {
        let mut cell = self.cell.lock().expect("published reply lock");
        if cell.phase != ReplyPhase::Committed {
            return None;
        }
        cell.phase = ReplyPhase::Taken;
        cell.value.take()
    }
    pub fn is_committed(&self) -> bool {
        self.cell.lock().expect("published reply lock").phase != ReplyPhase::Empty
    }
}

/// Failure-owner publication uses the original running cleanup work authority.
pub use crate::driver::FailurePublicationSession;
