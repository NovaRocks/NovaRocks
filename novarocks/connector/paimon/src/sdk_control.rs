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

use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorResourceReservation};
use paimon::io::{ReadControl, ReadReservation};

use crate::resources::PaimonRequestResources;

#[derive(Default)]
struct OutputHandoff {
    pending: Mutex<Option<ConnectorResourceReservation>>,
}

#[derive(Clone)]
pub struct PaimonSdkReadControl {
    resources: PaimonRequestResources,
    output_handoff: Arc<OutputHandoff>,
}

impl PaimonSdkReadControl {
    pub fn new(resources: PaimonRequestResources) -> Self {
        Self {
            resources,
            output_handoff: Arc::new(OutputHandoff::default()),
        }
    }

    pub(crate) fn take_output_reservation(
        &self,
    ) -> Result<Option<ConnectorResourceReservation>, ConnectorError> {
        self.output_handoff
            .pending
            .lock()
            .map_err(|_| internal("Paimon output handoff lock was poisoned"))
            .map(|mut pending| pending.take())
    }
}

impl std::fmt::Debug for PaimonSdkReadControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PaimonSdkReadControl(<request resources>)")
    }
}

impl ReadControl for PaimonSdkReadControl {
    fn check_active(&self) -> paimon::Result<()> {
        self.resources.checkpoint().map_err(map_resource_error)
    }
    fn checkpoint(&self) -> paimon::Result<()> {
        self.resources.checkpoint().map_err(map_resource_error)
    }
    fn try_reserve(&self, bytes: u64) -> paimon::Result<Box<dyn ReadReservation>> {
        self.resources
            .reserve_reader_state(bytes.max(1))
            .map(|reservation| Box::new(SdkReservation(reservation)) as Box<dyn ReadReservation>)
            .map_err(map_resource_error)
    }

    fn try_reserve_output(&self, bytes: u64) -> paimon::Result<Box<dyn ReadReservation>> {
        self.resources
            .reserve_output(bytes.max(1))
            .map(|reservation| Box::new(SdkReservation(reservation)) as Box<dyn ReadReservation>)
            .map_err(map_resource_error)
    }

    fn handoff_output(
        &self,
        reservation: Box<dyn ReadReservation>,
    ) -> paimon::Result<Option<Box<dyn ReadReservation>>> {
        let reservation = reservation
            .into_any()
            .downcast::<SdkReservation>()
            .map_err(|_| paimon::Error::UnexpectedError {
                message: "Paimon output reservation has another host type".to_string(),
                source: None,
            })?
            .0;
        if reservation.class() != novarocks_spi::connector::ConnectorResourceClass::ReaderOutput {
            return Err(paimon::Error::UnexpectedError {
                message: "Paimon output handoff received a non-output reservation".to_string(),
                source: None,
            });
        }
        let mut pending =
            self.output_handoff
                .pending
                .lock()
                .map_err(|_| paimon::Error::UnexpectedError {
                    message: "Paimon output handoff lock was poisoned".to_string(),
                    source: None,
                })?;
        if pending.is_some() {
            return Err(paimon::Error::UnexpectedError {
                message: "Paimon output handoff already owns an undelivered batch".to_string(),
                source: None,
            });
        }
        *pending = Some(reservation);
        Ok(None)
    }
}

struct SdkReservation(ConnectorResourceReservation);

impl std::fmt::Debug for SdkReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("SdkReservation")
            .field(&self.0.bytes())
            .finish()
    }
}

impl ReadReservation for SdkReservation {
    fn bytes(&self) -> u64 {
        self.0.bytes()
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

fn internal(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

fn map_resource_error(error: ConnectorError) -> paimon::Error {
    paimon::Error::UnexpectedError {
        message: "host rejected Paimon read resource request".to_string(),
        source: Some(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorResourceCheckpoint, ConnectorResourceLease,
        ConnectorResourceLedger,
    };

    use super::*;

    #[derive(Default)]
    struct Ledger {
        retained: Arc<AtomicU64>,
        reservations: AtomicUsize,
    }

    impl ConnectorResourceLedger for Ledger {
        fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
            Ok(ConnectorResourceCheckpoint::new(1))
        }

        fn try_reserve(
            &self,
            class: novarocks_spi::connector::ConnectorResourceClass,
            bytes: u64,
        ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
            assert_eq!(
                class,
                novarocks_spi::connector::ConnectorResourceClass::ReaderOutput
            );
            self.reservations.fetch_add(1, Ordering::AcqRel);
            self.retained.fetch_add(bytes, Ordering::AcqRel);
            Ok(Box::new(Lease {
                retained: Arc::clone(&self.retained),
                bytes,
            }))
        }
    }

    impl ConnectorCancellation for Ledger {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct Lease {
        retained: Arc<AtomicU64>,
        bytes: u64,
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
            self.retained
                .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
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
    fn output_handoff_preserves_the_same_host_reservation() {
        let ledger = Arc::new(Ledger::default());
        let resources = PaimonRequestResources::new(
            novarocks_spi::connector::ConnectorRequestResources::new(ledger.clone()),
            ledger.clone(),
            Instant::now() + Duration::from_secs(60),
        );
        let control = PaimonSdkReadControl::new(resources);

        let sdk_reservation = control.try_reserve_output(64).unwrap();
        assert_eq!(ledger.retained.load(Ordering::Acquire), 64);
        assert_eq!(ledger.reservations.load(Ordering::Acquire), 1);
        assert!(control.handoff_output(sdk_reservation).unwrap().is_none());
        assert_eq!(ledger.retained.load(Ordering::Acquire), 64);

        let reservation = control
            .take_output_reservation()
            .unwrap()
            .expect("handed-off output reservation");
        assert_eq!(reservation.bytes(), 64);
        let output = reservation.into_output(48).unwrap();
        assert_eq!(output.bytes(), 48);
        assert_eq!(ledger.retained.load(Ordering::Acquire), 48);
        drop(output);
        assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
        assert_eq!(ledger.reservations.load(Ordering::Acquire), 1);
    }
}
