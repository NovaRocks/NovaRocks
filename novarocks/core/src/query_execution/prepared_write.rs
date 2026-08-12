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

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::native::encode::NativeFragmentBundle;
use crate::query_execution::contract::{
    ConnectorWriteExecutionRegistration, ConnectorWriteOperationRegistration,
    DistributedQueryError, DistributedQueryIntent, DistributedQueryRequest,
    build_distributed_query_request_with_execution, with_connector_write_operation,
};
use crate::query_execution::preparation::PreparedFragmentSet;
use crate::query_execution::request_context::QueryExecutionContext;
use crate::sql::planner::distributed::FragmentId;
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
    terminal_writer_fragment_ids: BTreeSet<FragmentId>,
    writer_fragment_cohorts: BTreeMap<FragmentId, ConnectorWriteCohortId>,
    single_cohort_id: Option<ConnectorWriteCohortId>,
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
        let terminal_writer_fragment_ids = terminal_writer_fragment_ids(&prepared);
        let writer_fragment_cohorts = terminal_writer_fragment_ids
            .iter()
            .copied()
            .map(|fragment_id| (fragment_id, cohort_id));
        Self::new_with_writer_fragment_cohorts(
            prepared,
            native_bundle,
            query_options,
            registration,
            writer_fragment_cohorts,
            lease,
        )
    }

    pub(crate) fn new_with_writer_fragment_cohorts<I>(
        prepared: PreparedFragmentSet,
        native_bundle: NativeFragmentBundle,
        query_options: Option<QueryOptions>,
        registration: ConnectorWriteOperationRegistration,
        writer_fragment_cohorts: I,
        lease: ConnectorWriteLease,
    ) -> Result<Self, DistributedQueryError>
    where
        I: IntoIterator<Item = (FragmentId, ConnectorWriteCohortId)>,
    {
        let terminal_writer_fragment_ids = terminal_writer_fragment_ids(&prepared);
        if terminal_writer_fragment_ids.is_empty() {
            return Err(DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                "prepared connector write request has no terminal writer fragments",
            ));
        }
        let mut canonical_writer_fragment_cohorts = BTreeMap::new();
        for (fragment_id, cohort_id) in writer_fragment_cohorts {
            if canonical_writer_fragment_cohorts
                .insert(fragment_id, cohort_id)
                .is_some()
            {
                return Err(DistributedQueryError::new(
                    crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                    "prepared connector write request contains a duplicate writer fragment mapping",
                ));
            }
        }
        if canonical_writer_fragment_cohorts
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
            != terminal_writer_fragment_ids
        {
            return Err(DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                "prepared connector write request writer mapping does not exactly match its terminal fragments",
            ));
        }
        let templates = registration.clone().into_cohorts();
        let registered_cohort_ids = templates
            .iter()
            .map(|template| template.cohort_id())
            .collect::<BTreeSet<_>>();
        let mapped_cohort_ids = canonical_writer_fragment_cohorts
            .values()
            .copied()
            .collect::<BTreeSet<_>>();
        if mapped_cohort_ids != registered_cohort_ids {
            return Err(DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                "prepared connector write request writer mapping does not exactly cover its sealed cohorts",
            ));
        }
        if registration.owner() != lease.binding_key() {
            return Err(DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                "prepared connector write request registration does not match its retained lease",
            ));
        }
        if templates
            .iter()
            .any(|template| !template.retains_lease_generation(&lease))
        {
            return Err(DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                "prepared connector write request does not retain the planning template lease generation",
            ));
        }
        let single_cohort_id = (mapped_cohort_ids.len() == 1)
            .then(|| mapped_cohort_ids.into_iter().next().expect("one mapped cohort"));
        Ok(Self {
            prepared,
            native_bundle,
            query_options,
            registration,
            terminal_writer_fragment_ids,
            writer_fragment_cohorts: canonical_writer_fragment_cohorts,
            single_cohort_id,
            lease,
        })
    }

    pub fn write_operation_id(&self) -> ConnectorWriteOperationId {
        self.registration.operation_id()
    }

    /// Compatibility accessor for callers that are structurally
    /// single-cohort. Multi-cohort callers must use the exact writer mapping.
    pub fn write_cohort_id(&self) -> ConnectorWriteCohortId {
        self.single_cohort_id
            .expect("write_cohort_id requires an exact single-cohort registration")
    }

    pub fn terminal_writer_fragment_ids(&self) -> &BTreeSet<FragmentId> {
        &self.terminal_writer_fragment_ids
    }

    pub fn writer_fragment_cohorts(
        &self,
    ) -> &BTreeMap<FragmentId, ConnectorWriteCohortId> {
        &self.writer_fragment_cohorts
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
        let session_templates_match = self.registration.clone().into_cohorts().iter().all(
            |template| {
                registration
                    .session()
                    .preparation(template.cohort_id())
                    .is_ok_and(|preparation| preparation.digest() == template.preparation().digest())
            },
        );
        if registration.session().operation_id() != self.registration.operation_id()
            || registration.session().owner() != self.registration.owner()
            || registration.writer_fragment_cohorts() != &self.writer_fragment_cohorts
            || !session_templates_match
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

fn terminal_writer_fragment_ids(prepared: &PreparedFragmentSet) -> BTreeSet<FragmentId> {
    prepared
        .scheduling_view()
        .fragments()
        .filter(|fragment| fragment.execution_role().is_terminal_write())
        .map(|fragment| fragment.fragment_id())
        .collect()
}
