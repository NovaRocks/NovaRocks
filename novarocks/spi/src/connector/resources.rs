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

//! Request-owned connector resource accounting.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use super::{ConnectorError, ConnectorErrorKind};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorResourceClass {
    Metadata,
    SplitPlanning,
    ReaderState,
    ReaderOutput,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorResourceCheckpoint(u64);

impl ConnectorResourceCheckpoint {
    pub const fn new(sequence: u64) -> Self {
        Self(sequence)
    }

    pub const fn sequence(self) -> u64 {
        self.0
    }
}

pub trait ConnectorResourceLease: Send {
    fn bytes(&self) -> u64;
    fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError>;
    fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError>;
}

pub trait ConnectorResourceLedger: Send + Sync {
    fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError>;

    fn try_reserve(
        &self,
        class: ConnectorResourceClass,
        bytes: u64,
    ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorRequestResources {
    ledger: Arc<dyn ConnectorResourceLedger>,
}

impl ConnectorRequestResources {
    pub fn new(ledger: Arc<dyn ConnectorResourceLedger>) -> Self {
        Self { ledger }
    }

    pub fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
        self.ledger.checkpoint()
    }

    pub fn try_reserve(
        &self,
        class: ConnectorResourceClass,
        bytes: u64,
    ) -> Result<ConnectorResourceReservation, ConnectorError> {
        if bytes == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector reservation must be non-zero",
            ));
        }
        Ok(ConnectorResourceReservation {
            class,
            lease: self.ledger.try_reserve(class, bytes)?,
        })
    }
}

#[must_use = "dropping a connector reservation releases its host charge"]
pub struct ConnectorResourceReservation {
    class: ConnectorResourceClass,
    lease: Box<dyn ConnectorResourceLease>,
}

impl ConnectorResourceReservation {
    pub const fn class(&self) -> ConnectorResourceClass {
        self.class
    }

    pub fn bytes(&self) -> u64 {
        self.lease.bytes()
    }

    pub fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
        if additional == 0 {
            return Ok(());
        }
        self.lease.try_grow(additional)
    }

    pub fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
        if bytes > self.bytes() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector reservation cannot grow through shrink_to",
            ));
        }
        self.lease.shrink_to(bytes)
    }

    pub fn release(self) {}

    pub fn into_output(
        mut self,
        exact_bytes: u64,
    ) -> Result<ConnectorOutputMemoryToken, ConnectorError> {
        if self.class != ConnectorResourceClass::ReaderOutput {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "only a reader-output reservation may become an output token",
            ));
        }
        self.shrink_to(exact_bytes)?;
        Ok(ConnectorOutputMemoryToken { reservation: self })
    }
}

impl Debug for ConnectorResourceReservation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorResourceReservation")
            .field("class", &self.class)
            .field("bytes", &self.bytes())
            .finish_non_exhaustive()
    }
}

#[must_use = "the output token must move with the Arrow buffers it accounts"]
pub struct ConnectorOutputMemoryToken {
    reservation: ConnectorResourceReservation,
}

impl ConnectorOutputMemoryToken {
    pub fn bytes(&self) -> u64 {
        self.reservation.bytes()
    }

    /// Release bytes that no longer back Arrow buffers while retaining the
    /// same output owner for the remaining buffers.
    pub fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
        self.reservation.shrink_to(bytes)
    }

    pub fn into_reservation(self) -> ConnectorResourceReservation {
        self.reservation
    }
}

impl Debug for ConnectorOutputMemoryToken {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorOutputMemoryToken")
            .field("bytes", &self.bytes())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    struct Ledger {
        retained: Arc<AtomicU64>,
    }

    impl ConnectorResourceLedger for Ledger {
        fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
            Ok(ConnectorResourceCheckpoint::new(9))
        }

        fn try_reserve(
            &self,
            _class: ConnectorResourceClass,
            bytes: u64,
        ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
            self.retained.fetch_add(bytes, Ordering::AcqRel);
            Ok(Box::new(Lease {
                bytes,
                retained: Arc::clone(&self.retained),
            }))
        }
    }

    struct Lease {
        bytes: u64,
        retained: Arc<AtomicU64>,
    }

    impl ConnectorResourceLease for Lease {
        fn bytes(&self) -> u64 {
            self.bytes
        }

        fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
            self.retained.fetch_add(additional, Ordering::AcqRel);
            self.bytes += additional;
            Ok(())
        }

        fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
            let released = self.bytes - bytes;
            self.retained.fetch_sub(released, Ordering::AcqRel);
            self.bytes = bytes;
            Ok(())
        }
    }

    impl Drop for Lease {
        fn drop(&mut self) {
            self.retained.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }

    #[test]
    fn output_token_keeps_the_exact_reservation_until_the_last_owner_drops() {
        let retained = Arc::new(AtomicU64::new(0));
        let resources = ConnectorRequestResources::new(Arc::new(Ledger {
            retained: Arc::clone(&retained),
        }));
        assert_eq!(resources.checkpoint().unwrap().sequence(), 9);
        let mut reservation = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, 32)
            .unwrap();
        reservation.try_grow(8).unwrap();
        assert_eq!(retained.load(Ordering::Acquire), 40);
        let token = reservation.into_output(17).unwrap();
        assert_eq!(token.bytes(), 17);
        assert_eq!(retained.load(Ordering::Acquire), 17);
        let moved = token;
        drop(moved);
        assert_eq!(retained.load(Ordering::Acquire), 0);
    }

    #[test]
    fn only_output_reservations_can_cross_the_page_boundary() {
        let resources = ConnectorRequestResources::new(Arc::new(Ledger {
            retained: Arc::new(AtomicU64::new(0)),
        }));
        let reservation = resources
            .try_reserve(ConnectorResourceClass::ReaderState, 1)
            .unwrap();
        assert_eq!(
            reservation.into_output(1).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}
