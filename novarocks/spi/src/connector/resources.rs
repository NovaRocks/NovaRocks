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

//! Connector execution resource accounting.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use super::{ConnectorError, ConnectorErrorKind};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorResourceClass {
    Metadata,
    SplitPlanning,
    ReaderState,
    ReaderOutput,
    WriterState,
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

pub trait ConnectorReservationBackend: Send {
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
    ) -> Result<Box<dyn ConnectorReservationBackend>, ConnectorError>;
}

/// Mandatory resource capability for a backend reader or page source.
///
/// A role host obtains this only after the exact task's tracker is installed.
/// The type makes the execution dependency explicit; the host's admission
/// check and real tracker tests establish the provenance of its ledger.
#[derive(Clone)]
pub struct ConnectorExecutionResources {
    ledger: Arc<dyn ConnectorResourceLedger>,
}

impl ConnectorExecutionResources {
    pub fn from_admitted_ledger(ledger: Arc<dyn ConnectorResourceLedger>) -> Self {
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
        reserve(&self.ledger, class, bytes)
    }
}

fn reserve(
    ledger: &Arc<dyn ConnectorResourceLedger>,
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
        backend: ledger.try_reserve(class, bytes)?,
    })
}

#[must_use = "dropping a connector reservation releases its host charge"]
pub struct ConnectorResourceReservation {
    class: ConnectorResourceClass,
    backend: Box<dyn ConnectorReservationBackend>,
}

impl ConnectorResourceReservation {
    pub const fn class(&self) -> ConnectorResourceClass {
        self.class
    }

    pub fn bytes(&self) -> u64 {
        self.backend.bytes()
    }

    pub fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
        if additional == 0 {
            return Ok(());
        }
        self.backend.try_grow(additional)
    }

    pub fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
        if bytes > self.bytes() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector reservation cannot grow through shrink_to",
            ));
        }
        self.backend.shrink_to(bytes)
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
mod tests;
