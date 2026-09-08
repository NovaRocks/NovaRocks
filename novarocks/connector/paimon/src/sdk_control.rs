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

use novarocks_spi::connector::{ConnectorError, ConnectorResourceReservation};
use paimon::io::{ReadControl, ReadReservation};

use crate::resources::PaimonRequestResources;

#[derive(Clone, Debug)]
pub struct PaimonSdkReadControl {
    resources: PaimonRequestResources,
}

impl PaimonSdkReadControl {
    pub fn new(resources: PaimonRequestResources) -> Self {
        Self { resources }
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
}

fn map_resource_error(error: ConnectorError) -> paimon::Error {
    paimon::Error::UnexpectedError {
        message: "host rejected Paimon read resource request".to_string(),
        source: Some(Box::new(error)),
    }
}
