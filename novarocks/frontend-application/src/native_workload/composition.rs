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

//! Composition of actual Frontend runtime configuration, without a solver profile.
use super::*;
use novarocks_native_adapter::FrontendTaskTransportBudget;

/// The original Server/MySQL limits, still enforced by that ingress owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontendStatementIngress {
    pub ordinary_connections: NonZeroUsize,
    pub control_connections: NonZeroUsize,
    pub ordinary_command_bytes: NonZeroUsize,
    pub control_command_bytes: NonZeroUsize,
}
impl FrontendWorkloadCapacityProfile {
    pub fn try_for_native(
        inputs: FrontendNativeCapacityInputs,
        workload: &WorkloadConfig,
        result: FrontendResultCapacityProfile,
        transport: FrontendTaskTransportBudget,
        ingress: FrontendStatementIngress,
    ) -> Result<Self, WorkError> {
        Self::from_configuration(
            inputs,
            workload,
            Some(result.declared()),
            Some(transport),
            Some(ingress),
        )
    }
    pub fn statement_ingress(&self) -> Option<FrontendStatementIngress> {
        self.statement_ingress
    }
}
