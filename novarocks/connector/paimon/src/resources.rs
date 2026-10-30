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

use std::time::Instant;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionResources, ConnectorOutputMemoryToken,
    ConnectorRequestContext, ConnectorResourceClass, ConnectorResourceReservation,
    ConnectorStopView,
};

/// Request liveness shared by FE planning and BE execution. It owns no ledger.
#[derive(Clone)]
pub struct PaimonRequestControl {
    stop: ConnectorStopView,
    deadline: Instant,
}

impl PaimonRequestControl {
    pub fn new(stop: ConnectorStopView, deadline: Instant) -> Self {
        Self { stop, deadline }
    }

    pub fn from_request(request: &ConnectorRequestContext) -> Self {
        Self {
            stop: request.stop().clone(),
            deadline: request.deadline(),
        }
    }

    pub fn checkpoint(&self) -> Result<(), ConnectorError> {
        if self.stop.is_stopped() {
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
        Ok(())
    }
}

impl std::fmt::Debug for PaimonRequestControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PaimonRequestControl(<request liveness>)")
    }
}

/// BE-only capability. The caller must obtain `resources` from the admitted
/// task; a request context cannot manufacture or substitute it.
#[derive(Clone)]
pub struct PaimonExecutionResources {
    control: PaimonRequestControl,
    resources: ConnectorExecutionResources,
}

impl PaimonExecutionResources {
    pub fn new(control: PaimonRequestControl, resources: ConnectorExecutionResources) -> Self {
        Self { control, resources }
    }

    pub fn checkpoint(&self) -> Result<(), ConnectorError> {
        self.control.checkpoint()?;
        self.resources.checkpoint().map(|_| ())
    }

    pub fn control(&self) -> &PaimonRequestControl {
        &self.control
    }

    pub fn reserve_reader_state(
        &self,
        bytes: u64,
    ) -> Result<ConnectorResourceReservation, ConnectorError> {
        self.resources
            .try_reserve(ConnectorResourceClass::ReaderState, bytes)
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

impl std::fmt::Debug for PaimonExecutionResources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PaimonExecutionResources(<admitted ledger>)")
    }
}
