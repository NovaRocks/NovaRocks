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
use std::ops::Range;
use std::pin::Pin;

use bytes::Bytes;
use futures::Stream;

use super::FileStatus;

pub trait ReadReservation: Any + Debug + Send {
    fn bytes(&self) -> u64;

    /// Convert an opaque reservation into `Any` without losing ownership.
    ///
    /// An embedding host can use this only after its own [`ReadControl`]
    /// accepted an output handoff. SDK code otherwise keeps reservations
    /// opaque and tied to the value that owns them.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
}

/// Reservations that travel with an SDK-owned retained value.
///
/// This is intentionally crate-private: it is the ownership link between
/// controlled decoders and later SDK stages, not another host-facing resource
/// authority. Dropping the retained value's final owner drops these tokens.
#[derive(Debug, Default)]
pub(crate) struct ReadRetention {
    reservations: Vec<Box<dyn ReadReservation>>,
}

/// A value whose host reservation remains live until the value is reduced,
/// replaced, or dropped.
///
/// The reservation itself stays opaque: callers may transform the value while
/// preserving its ownership, but cannot detach or duplicate the token.
#[derive(Debug)]
pub struct RetainedRead<T> {
    value: T,
    retention: ReadRetention,
}

impl<T> RetainedRead<T> {
    pub(crate) fn new(value: T, retention: ReadRetention) -> Self {
        Self { value, retention }
    }

    pub(crate) fn unretained(value: T) -> Self {
        Self::new(value, ReadRetention::default())
    }

    pub(crate) fn value(&self) -> &T {
        &self.value
    }

    pub(crate) fn into_value(self) -> T {
        self.value
    }

    /// Transform a retained value while keeping the source reservation alive
    /// until the replacement value has been constructed.
    pub(crate) fn map<U>(self, transform: impl FnOnce(T) -> U) -> U {
        let Self { value, retention } = self;
        let output = transform(value);
        drop(retention);
        output
    }

    /// Fallible form of [`Self::map`]. Source reservations are also released
    /// after an error has dropped any partially constructed replacement.
    pub fn try_map<U, E>(self, transform: impl FnOnce(T) -> Result<U, E>) -> Result<U, E> {
        let Self { value, retention } = self;
        let output = transform(value);
        drop(retention);
        output
    }

    /// Replace the carried value without releasing the existing reservation.
    pub(crate) fn replace<U>(self, value: U) -> RetainedRead<U> {
        let Self { retention, .. } = self;
        RetainedRead { value, retention }
    }
}

impl ReadRetention {
    pub(crate) fn push(&mut self, reservation: Box<dyn ReadReservation>) {
        self.reservations.push(reservation);
    }

    pub(crate) fn append(&mut self, mut other: Self) {
        self.reservations.append(&mut other.reservations);
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> u64 {
        self.reservations.iter().fold(0_u64, |sum, reservation| {
            sum.saturating_add(reservation.bytes())
        })
    }
}

/// Request-local cancellation, deadline and retained-memory boundary.
pub trait ReadControl: Debug + Send + Sync {
    fn check_active(&self) -> crate::Result<()>;
    fn checkpoint(&self) -> crate::Result<()>;
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
    async fn read(&self, path: &str, range: Range<u64>) -> crate::Result<Bytes>;
    async fn list(&self, path: &str, recursive: bool) -> crate::Result<FileStatusStream>;
}
