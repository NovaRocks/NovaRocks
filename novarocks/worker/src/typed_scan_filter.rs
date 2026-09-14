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

//! Worker port for a typed scan's Native-carrier dynamic-filter projection.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_execution::runtime_filter::{
    RuntimeFilterConsumerContract, RuntimeFilterContractViolation, RuntimeFilterSessionRef,
};
use novarocks_spi::connector::read_stack::ConnectorReadDynamicFilter;

/// Builds one live provider dynamic filter from an already-decoded scan carrier.
///
/// The Native adapter owns the carrier interpretation. Worker only requests a
/// filter after its attempt lifecycle makes the session available.
pub trait TypedScanLiveDynamicFilterFactory: Send + Sync {
    fn build(
        &self,
        session: Option<&RuntimeFilterSessionRef>,
        contracts: &BTreeMap<u32, RuntimeFilterConsumerContract>,
    ) -> Result<Arc<ConnectorReadDynamicFilter>, RuntimeFilterContractViolation>;
}
