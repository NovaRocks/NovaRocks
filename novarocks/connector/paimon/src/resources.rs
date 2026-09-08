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

use novarocks_spi::connector::{
    ConnectorError, ConnectorOutputMemoryToken, ConnectorRequestResources, ConnectorResourceClass,
    ConnectorResourceReservation,
};

#[derive(Clone)]
pub struct PaimonRequestResources {
    resources: ConnectorRequestResources,
}

impl PaimonRequestResources {
    pub fn new(resources: ConnectorRequestResources) -> Self {
        Self { resources }
    }

    pub fn checkpoint(&self) -> Result<(), ConnectorError> {
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
