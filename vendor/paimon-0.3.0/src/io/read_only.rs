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

//! Host-injected, read-only I/O and cooperative resource control.

use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;

use bytes::Bytes;
use futures::Stream;

use super::FileStatus;

pub trait ReadReservation: Any + Debug + Send {
    fn bytes(&self) -> u64;

    /// Convert an opaque reservation into `Any` without losing ownership.
    ///
    /// An embedding host can use this only after its own [`ReadExecutionResources`]
    /// accepted an output handoff. SDK code otherwise keeps reservations
    /// opaque and tied to the value that owns them.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
}

/// Request-local cancellation and deadline boundary.
pub trait ReadControl: Debug + Send + Sync {
    fn check_active(&self) -> crate::Result<()>;
    fn checkpoint(&self) -> crate::Result<()>;
}

/// Mandatory execution-side retained-memory authority.
///
/// Metadata planning has no instance of this interface. An execution reader
/// receives it separately from operation control and authorized file access.
pub trait ReadExecutionResources: ReadControl {
    fn try_reserve(&self, bytes: u64) -> crate::Result<Box<dyn ReadReservation>>;

    /// Reserve a batch that is about to cross the SDK output boundary.
    ///
    /// The default uses the normal retained-state budget. Hosts that classify
    /// output separately may override this while preserving the same owner.
    fn try_reserve_output(&self, bytes: u64) -> crate::Result<Box<dyn ReadReservation>> {
        self.try_reserve(bytes)
    }

    /// Offer ownership of an output reservation to the embedding host.
    ///
    /// Returning `Some` declines the handoff, so the SDK retains the returned
    /// reservation across `yield` and drops it only after the stream resumes.
    /// Returning `None` accepts ownership; the host must keep the charge live
    /// until it attaches the same reservation to the delivered output or drops
    /// that output.
    fn handoff_output(
        &self,
        reservation: Box<dyn ReadReservation>,
    ) -> crate::Result<Option<Box<dyn ReadReservation>>> {
        Ok(Some(reservation))
    }

    /// A cooperative yield point, awaited at each unit of CPU work that may
    /// produce no output: a source batch a merge takes or skips, or a data
    /// file a reader moves to.
    ///
    /// A host resolves it at once while its scheduling turn has budget and
    /// returns `Pending` once when the turn is spent, so a long run of such
    /// work gives the host its turn back without the SDK yielding a batch.
    /// The default never yields.
    fn cooperate(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(std::future::ready(()))
    }
}

pub type FileStatusStream = Pin<Box<dyn Stream<Item = crate::Result<FileStatus>> + Send + 'static>>;

/// Authorized read-only filesystem supplied by the embedding host.
///
/// Paths remain fully qualified so the host can enforce scheme, authority,
/// warehouse and credential scope for every operation. Listing is a stream so
/// the SDK can checkpoint and charge each entry before collecting the next.
#[async_trait::async_trait]
pub trait ReadOnlyFileIO: Debug + Send + Sync {
    async fn stat(&self, path: &str) -> crate::Result<FileStatus>;
    async fn exists(&self, path: &str) -> crate::Result<bool>;
    /// Reads exactly `range` of the object at `path`.
    ///
    /// `known_size` is the object's size when the SDK already knows it, from
    /// frozen file metadata or from its own stat of the same object. A host
    /// then reads without probing the size again, and fails rather than
    /// chooses when its own knowledge of the object disagrees.
    async fn read(
        &self,
        path: &str,
        range: Range<u64>,
        known_size: Option<u64>,
    ) -> crate::Result<Bytes>;
    async fn list(&self, path: &str, recursive: bool) -> crate::Result<FileStatusStream>;
}
