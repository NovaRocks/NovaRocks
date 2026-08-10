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

//! Side-effect-free SQL handoff for a distributed connector writer.

use crate::protocol::native::encode::NativeFragmentBundle;
use crate::query_execution::contract::{
    ConnectorWriteExecutionRegistration, ConnectorWriteOperationRegistration,
    DistributedQueryError, DistributedQueryIntent, DistributedQueryRequest,
    build_distributed_query_request_with_execution, with_connector_write_operation,
};
use crate::query_execution::preparation::PreparedFragmentSet;
use crate::query_execution::request_context::QueryExecutionContext;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_spi::connector::{
    ConnectorWriteCohortId, ConnectorWriteLease, ConnectorWriteOperationId,
};

/// SQL-owned prepared fragments and native bundle for one connector write.
/// It deliberately contains no backend topology, writer handle, or execution
/// attempt. It does retain the exact write lease that admitted the sealed
/// preparation; bind must never reacquire a later current generation.
pub struct PreparedDistributedWriteRequest {
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    query_options: Option<QueryOptions>,
    registration: ConnectorWriteOperationRegistration,
    cohort_id: ConnectorWriteCohortId,
    lease: ConnectorWriteLease,
}

impl PreparedDistributedWriteRequest {
    pub(crate) fn new(
        prepared: PreparedFragmentSet,
        native_bundle: NativeFragmentBundle,
        query_options: Option<QueryOptions>,
        registration: ConnectorWriteOperationRegistration,
        cohort_id: ConnectorWriteCohortId,
        lease: ConnectorWriteLease,
    ) -> Result<Self, DistributedQueryError> {
        if registration
            .clone()
            .into_cohorts()
            .iter()
            .all(|template| template.cohort_id() != cohort_id)
        {
            return Err(DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                "prepared connector write request references a cohort outside its sealed registration",
            ));
        }
        if registration.owner() != lease.binding_key() {
            return Err(DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                "prepared connector write request registration does not match its retained lease",
            ));
        }
        if registration
            .clone()
            .into_cohorts()
            .iter()
            .any(|template| !template.retains_lease_generation(&lease))
        {
            return Err(DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                "prepared connector write request does not retain the planning template lease generation",
            ));
        }
        Ok(Self {
            prepared,
            native_bundle,
            query_options,
            registration,
            cohort_id,
            lease,
        })
    }

    pub fn write_operation_id(&self) -> ConnectorWriteOperationId {
        self.registration.operation_id()
    }

    pub const fn write_cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    /// Clone the complete sealed operation registration for the application
    /// owner to begin through its retained exact-generation write lease.
    pub fn registration(&self) -> ConnectorWriteOperationRegistration {
        self.registration.clone()
    }

    pub fn lease(&self) -> ConnectorWriteLease {
        self.lease.clone()
    }

    /// Bind the precomputed artifact to one admitted query execution and one
    /// already-sealed operation session. No current generation is acquired and
    /// no writer is started by preparation.
    pub fn into_request(
        self,
        execution: &QueryExecutionContext,
        registration: ConnectorWriteExecutionRegistration,
    ) -> Result<DistributedQueryRequest, DistributedQueryError> {
        if registration.session().operation_id() != self.registration.operation_id()
            || registration.cohort_id() != self.cohort_id
        {
            return Err(DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                "prepared connector write request does not match the sealed operation session",
            ));
        }
        let request = build_distributed_query_request_with_execution(
            self.prepared,
            self.native_bundle,
            self.query_options,
            DistributedQueryIntent::Write,
            execution,
        )?;
        with_connector_write_operation(request, registration)
    }
}
