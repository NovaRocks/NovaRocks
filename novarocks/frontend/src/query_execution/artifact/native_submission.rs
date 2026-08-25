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

//! Sealed schedule-time native submission handoff.
//!
//! The view exposes only a stable identity/key projection.  The attachment is
//! consuming and verifies that a mapper returns exactly one native submission
//! for every sealed placement before Core creates `StageBatch` values.

use std::collections::BTreeSet;

use super::{
    ExpectedOutputSchema, FragmentId, RootFetchMetadata, ValidatedNativeSubmission,
    WriterRegistrationSet,
};
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::preparation::PreparedFragmentSet;
use crate::query_execution::schedule::SchedulingPlan;
use crate::query_execution::write_plan::ConnectorWritePlanAttachment;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_proto::lifecycle::QueryExecutionId;
use novarocks_spi::connector::ConnectorWriteCohortId;
use novarocks_sql::plan_read::{ColumnId, CteId, FragmentEdge, FragmentId as PlannerFragmentId};
use novarocks_types::UniqueId;
use std::collections::BTreeMap;

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// One frozen native-submission identity.  It intentionally contains no
/// mutable schedule, connector lease, or payload-construction capability.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NativeSubmissionKey {
    backend_idx: usize,
    fragment_id: FragmentId,
    fragment_instance_id: UniqueId,
}

impl NativeSubmissionKey {
    pub(crate) const fn new(
        backend_idx: usize,
        fragment_id: FragmentId,
        fragment_instance_id: UniqueId,
    ) -> Self {
        Self {
            backend_idx,
            fragment_id,
            fragment_instance_id,
        }
    }

    pub const fn backend_idx(self) -> usize {
        self.backend_idx
    }

    pub const fn fragment_id(self) -> FragmentId {
        self.fragment_id
    }

    pub const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }
}

/// Borrow-only facts for encoding one fully prepared, control-ready native
/// submission.  It has no public constructor.
#[derive(Clone)]
pub struct NativeSubmissionEncodingView<'a> {
    handoff_id: u64,
    execution_id: QueryExecutionId,
    keys: Vec<NativeSubmissionKey>,
    root: NativeSubmissionKey,
    prepared: &'a PreparedFragmentSet,
    native_fragments: &'a NativeFragmentAttachment,
    schedule: &'a SchedulingPlan,
    options: &'a QueryOptions,
    connector_write_plans: &'a BTreeMap<ConnectorWriteCohortId, ConnectorWritePlanAttachment>,
    root_fetch: RootFetchMetadata,
    expected_output: ExpectedOutputSchema,
}

impl<'a> NativeSubmissionEncodingView<'a> {
    #[expect(
        clippy::too_many_arguments,
        reason = "Native submission must retain independently frozen fragment, schedule, connector, and output facts."
    )]
    pub(crate) fn new(
        handoff_id: u64,
        execution_id: QueryExecutionId,
        keys: Vec<NativeSubmissionKey>,
        root: NativeSubmissionKey,
        prepared: &'a PreparedFragmentSet,
        native_fragments: &'a NativeFragmentAttachment,
        schedule: &'a SchedulingPlan,
        options: &'a QueryOptions,
        connector_write_plans: &'a BTreeMap<ConnectorWriteCohortId, ConnectorWritePlanAttachment>,
        root_fetch: RootFetchMetadata,
        expected_output: ExpectedOutputSchema,
    ) -> Result<Self, DistributedQueryError> {
        validate_keys(&keys, root)?;
        Ok(Self {
            handoff_id,
            execution_id,
            keys,
            root,
            prepared,
            native_fragments,
            schedule,
            options,
            connector_write_plans,
            root_fetch,
            expected_output,
        })
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub fn placement_keys(&self) -> impl ExactSizeIterator<Item = NativeSubmissionKey> + '_ {
        self.keys.iter().copied()
    }

    pub fn root_key(&self) -> NativeSubmissionKey {
        self.root
    }

    /// Exact static native templates projected from the sealed attachment.
    /// The attachment itself remains Core-owned and consuming; this narrow
    /// borrow gives the Frontend no replacement, reuse, or fragment-set
    /// construction capability.
    pub fn native_fragments_in_id_order(
        &self,
    ) -> impl ExactSizeIterator<Item = (FragmentId, &novarocks_proto::plan::PlanFragment)> + '_
    {
        self.native_fragments.fragments_in_id_order()
    }

    /// Frozen schedule placements, including assigned scan splits and stream
    /// destinations.  The encoder receives no mutable scheduling capability.
    pub fn schedule(&self) -> &'a SchedulingPlan {
        self.schedule
    }

    pub fn query_options(&self) -> &'a QueryOptions {
        self.options
    }

    pub fn connector_write_plans(
        &self,
    ) -> &'a BTreeMap<ConnectorWriteCohortId, ConnectorWritePlanAttachment> {
        self.connector_write_plans
    }

    pub fn query_id(&self) -> UniqueId {
        let query_id = self.execution_id.query_id();
        UniqueId::new(query_id.high(), query_id.low())
    }

    pub fn topological_fragment_order(&self) -> &'a [PlannerFragmentId] {
        self.prepared.scheduling_view().topological_order()
    }

    pub fn edges(&self) -> &'a [FragmentEdge] {
        self.prepared.scheduling_view().edges()
    }

    pub fn fragments(
        &self,
    ) -> impl ExactSizeIterator<Item = NativeSubmissionFragmentFacts<'a>> + '_ {
        self.prepared
            .scheduling_view()
            .fragments()
            .map(NativeSubmissionFragmentFacts::new)
    }

    pub fn fragment(&self, fragment_id: FragmentId) -> Option<NativeSubmissionFragmentFacts<'a>> {
        self.prepared
            .scheduling_view()
            .fragment(fragment_id)
            .map(NativeSubmissionFragmentFacts::new)
    }

    pub fn seal(
        &self,
        submissions: Vec<ValidatedNativeSubmission>,
        writer_registrations: WriterRegistrationSet,
    ) -> Result<NativeSubmissionAttachment, DistributedQueryError> {
        let expected = self.keys.iter().copied().collect::<BTreeSet<_>>();
        let mut actual = BTreeSet::new();
        for submission in &submissions {
            if submission.execution_id() != self.execution_id {
                return Err(contract_error(
                    "native submission attachment execution id differs from sealed view",
                ));
            }
            let key = NativeSubmissionKey::new(
                submission.backend_idx(),
                submission.fragment_id(),
                submission.fragment_instance_id(),
            );
            if !actual.insert(key) {
                return Err(contract_error(format!(
                    "native submission attachment repeats placement key {key:?}"
                )));
            }
        }
        if actual != expected {
            let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
            let unknown = actual.difference(&expected).copied().collect::<Vec<_>>();
            return Err(contract_error(format!(
                "native submission attachment placement set mismatch: missing={missing:?} unknown={unknown:?}"
            )));
        }
        let root = NativeSubmissionKey::new(
            self.root_fetch.backend_idx(),
            self.root_fetch.fragment_id(),
            self.root_fetch.fragment_instance_id(),
        );
        if root != self.root {
            return Err(contract_error(
                "native submission attachment root metadata differs from sealed view",
            ));
        }
        Ok(NativeSubmissionAttachment {
            handoff_id: self.handoff_id,
            execution_id: self.execution_id,
            submissions,
            root_fetch: self.root_fetch.clone(),
            writer_registrations,
            expected_output: self.expected_output.clone(),
        })
    }
}

fn validate_keys(
    keys: &[NativeSubmissionKey],
    root: NativeSubmissionKey,
) -> Result<(), DistributedQueryError> {
    let actual = keys.iter().copied().collect::<BTreeSet<_>>();
    if actual.len() != keys.len() {
        return Err(contract_error(
            "native submission encoding view repeats a sealed placement key",
        ));
    }
    if !actual.contains(&root) {
        return Err(contract_error(
            "native submission encoding view root is absent from sealed placement keys",
        ));
    }
    Ok(())
}

/// The subset of prepared-fragment facts needed by placement-local native
/// submission mapping.  It is deliberately a read-only projection, not a
/// way to reconstruct planning or scheduling state.
#[derive(Clone, Copy)]
pub struct NativeSubmissionFragmentFacts<'a> {
    fragment: &'a crate::query_execution::preparation::PreparedFragment,
}

impl<'a> NativeSubmissionFragmentFacts<'a> {
    fn new(fragment: &'a crate::query_execution::preparation::PreparedFragment) -> Self {
        Self { fragment }
    }

    pub fn fragment_id(self) -> FragmentId {
        self.fragment.fragment_id()
    }

    pub fn role(self) -> NativeSubmissionFragmentRole {
        match self.fragment.execution_role() {
            crate::query_execution::preparation::PreparedFragmentRole::Result => {
                NativeSubmissionFragmentRole::Result
            }
            crate::query_execution::preparation::PreparedFragmentRole::Statistics => {
                NativeSubmissionFragmentRole::Statistics
            }
            crate::query_execution::preparation::PreparedFragmentRole::TerminalWrite => {
                NativeSubmissionFragmentRole::TerminalWrite
            }
            crate::query_execution::preparation::PreparedFragmentRole::NonTerminal => {
                NativeSubmissionFragmentRole::NonTerminal
            }
        }
    }

    pub fn cte_id(self) -> Option<CteId> {
        self.fragment.boundary_projection().cte_id()
    }

    pub fn cte_exchange_nodes(self) -> &'a [(CteId, i32, Vec<ColumnId>)] {
        self.fragment.boundary_projection().cte_exchange_nodes()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeSubmissionFragmentRole {
    Result,
    Statistics,
    TerminalWrite,
    NonTerminal,
}

impl NativeSubmissionFragmentRole {
    pub const fn uses_result_buffer(self) -> bool {
        matches!(self, Self::Result)
    }

    pub const fn is_terminal_write(self) -> bool {
        matches!(self, Self::TerminalWrite)
    }
}

/// Consuming, artifact-bound native submission payload.  Core validates this
/// attachment before it constructs lifecycle `StageBatch` values.
pub struct NativeSubmissionAttachment {
    handoff_id: u64,
    execution_id: QueryExecutionId,
    submissions: Vec<ValidatedNativeSubmission>,
    root_fetch: RootFetchMetadata,
    writer_registrations: WriterRegistrationSet,
    expected_output: ExpectedOutputSchema,
}

impl NativeSubmissionAttachment {
    pub(crate) fn matches(&self, handoff_id: u64, execution_id: QueryExecutionId) -> bool {
        self.handoff_id == handoff_id && self.execution_id == execution_id
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<ValidatedNativeSubmission>,
        RootFetchMetadata,
        WriterRegistrationSet,
        ExpectedOutputSchema,
    ) {
        (
            self.submissions,
            self.root_fetch,
            self.writer_registrations,
            self.expected_output,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::contract::QueryId;
    use novarocks_proto::lifecycle::AttemptId;

    #[allow(
        dead_code,
        reason = "Retained for staged query-execution contract and lifecycle integration."
    )]
    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(7, 9),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("valid execution id")
    }

    #[test]
    fn view_rejects_duplicate_placement_key() {
        let key = NativeSubmissionKey::new(2, 3, UniqueId::new(5, 7));
        let error = validate_keys(&[key, key], key).expect_err("duplicate key must be rejected");
        assert!(error.message().contains("repeats a sealed placement key"));
    }
}
