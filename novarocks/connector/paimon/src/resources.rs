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

use std::sync::Arc;
use std::time::Instant;

use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorError, ConnectorErrorKind, ConnectorOutputMemoryToken,
    ConnectorRequestContext, ConnectorRequestResources, ConnectorResourceClass,
    ConnectorResourceReservation,
};

#[derive(Clone)]
pub struct PaimonRequestResources {
    resources: ConnectorRequestResources,
    cancellation: Arc<dyn ConnectorCancellation>,
    deadline: Instant,
}

impl PaimonRequestResources {
    pub fn new(
        resources: ConnectorRequestResources,
        cancellation: Arc<dyn ConnectorCancellation>,
        deadline: Instant,
    ) -> Self {
        Self {
            resources,
            cancellation,
            deadline,
        }
    }

    /// Capture the exact attempt liveness alongside its admitted ledger.
    pub fn from_request(request: &ConnectorRequestContext) -> Result<Self, ConnectorError> {
        Ok(Self::new(
            request.resources()?.clone(),
            Arc::clone(request.cancellation()),
            request.deadline(),
        ))
    }

    pub fn checkpoint(&self) -> Result<(), ConnectorError> {
        if self.cancellation.is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "Paimon request was cancelled",
            ));
        }
        if Instant::now() >= self.deadline {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "Paimon request deadline elapsed",
            ));
        }
        self.resources.checkpoint().map(|_| ())
    }

    pub fn reserve_metadata(
        &self,
        bytes: u64,
    ) -> Result<ConnectorResourceReservation, ConnectorError> {
        self.resources
            .try_reserve(ConnectorResourceClass::Metadata, bytes)
    }

    pub fn reserve_reader_state(
        &self,
        bytes: u64,
    ) -> Result<ConnectorResourceReservation, ConnectorError> {
        self.resources
            .try_reserve(ConnectorResourceClass::ReaderState, bytes)
    }

    pub fn reserve_split_planning(
        &self,
        bytes: u64,
    ) -> Result<ConnectorResourceReservation, ConnectorError> {
        self.resources
            .try_reserve(ConnectorResourceClass::SplitPlanning, bytes)
    }

    pub fn reserve_output(
        &self,
        bytes: u64,
    ) -> Result<ConnectorResourceReservation, ConnectorError> {
        self.resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, bytes)
    }

    pub fn transfer_output(
        &self,
        reservation: ConnectorResourceReservation,
        exact_bytes: u64,
    ) -> Result<ConnectorOutputMemoryToken, ConnectorError> {
        reservation.into_output(exact_bytes)
    }
}

impl std::fmt::Debug for PaimonRequestResources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PaimonRequestResources(<request ledger>)")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use novarocks_spi::connector::{
        ConnectorResourceCheckpoint, ConnectorResourceLease, ConnectorResourceLedger,
    };

    use super::*;

    struct TestCancellation(AtomicBool);

    impl ConnectorCancellation for TestCancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::Acquire)
        }
    }

    struct CheckpointLedger(AtomicUsize);

    impl ConnectorResourceLedger for CheckpointLedger {
        fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
            self.0.fetch_add(1, Ordering::AcqRel);
            Ok(ConnectorResourceCheckpoint::new(1))
        }

        fn try_reserve(
            &self,
            _class: ConnectorResourceClass,
            _bytes: u64,
        ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
            unreachable!("checkpoint tests never reserve")
        }
    }

    #[test]
    fn cpu_only_checkpoint_observes_attempt_cancellation_before_ledger() {
        let cancellation = Arc::new(TestCancellation(AtomicBool::new(true)));
        let ledger = Arc::new(CheckpointLedger(AtomicUsize::new(0)));
        let resources = PaimonRequestResources::new(
            ConnectorRequestResources::new(ledger.clone()),
            cancellation,
            Instant::now() + Duration::from_secs(60),
        );

        let error = resources.checkpoint().unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
        assert_eq!(ledger.0.load(Ordering::Acquire), 0);
    }

    #[test]
    fn cpu_only_checkpoint_observes_attempt_deadline_before_ledger() {
        let cancellation = Arc::new(TestCancellation(AtomicBool::new(false)));
        let ledger = Arc::new(CheckpointLedger(AtomicUsize::new(0)));
        let resources = PaimonRequestResources::new(
            ConnectorRequestResources::new(ledger.clone()),
            cancellation,
            Instant::now() - Duration::from_millis(1),
        );

        let error = resources.checkpoint().unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
        assert_eq!(ledger.0.load(Ordering::Acquire), 0);
    }
}
