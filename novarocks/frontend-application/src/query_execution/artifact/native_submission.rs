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
//! for every sealed placement before any task descriptor is built from it.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::{FragmentId, RootFetchMetadata, ValidatedNativeSubmission};
use crate::native::fragment_encoder::frozen::FragmentArtifact;
use crate::query_execution::attempt_plan_facts::PlanOutputColumn;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::schedule::SchedulingPlan;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_types::UniqueId;

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// One frozen native-submission identity.  It intentionally contains no
/// mutable schedule, connector lease, or payload-construction capability.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NativeSubmissionKey {
    backend_idx: usize,
    fragment_id: FragmentId,
    task_key: UniqueId,
}

impl NativeSubmissionKey {
    pub(crate) const fn new(
        backend_idx: usize,
        fragment_id: FragmentId,
        task_key: UniqueId,
    ) -> Self {
        Self {
            backend_idx,
            fragment_id,
            task_key,
        }
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
    plan: SubmissionPlanFacts,
    native_fragments: &'a NativeFragmentAttachment,
    schedule: &'a SchedulingPlan,
    options: &'a QueryOptions,
    root_fetch: RootFetchMetadata,
}

impl<'a> NativeSubmissionEncodingView<'a> {
    pub(crate) fn new(
        handoff_id: u64,
        execution_id: QueryExecutionId,
        keys: Vec<NativeSubmissionKey>,
        root: NativeSubmissionKey,
        plan: SubmissionPlanFacts,
        native_fragments: &'a NativeFragmentAttachment,
        schedule: &'a SchedulingPlan,
        options: &'a QueryOptions,
        root_fetch: RootFetchMetadata,
    ) -> Result<Self, DistributedQueryError> {
        validate_keys(&keys, root)?;
        Ok(Self {
            handoff_id,
            execution_id,
            keys,
            root,
            plan,
            native_fragments,
            schedule,
            options,
            root_fetch,
        })
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    /// One fragment's static plan, as the completed plan froze it.
    ///
    /// The attachment itself remains Core-owned; this narrow borrow gives the
    /// Frontend no replacement, re-encoding, or fragment-set construction
    /// capability -- only a share of bytes that already exist.
    pub(crate) fn native_fragment(
        &self,
        fragment_id: FragmentId,
    ) -> Option<&'a Arc<FragmentArtifact>> {
        self.native_fragments.get(fragment_id)
    }

    /// The fragment the plan was frozen with as its root.
    pub(crate) fn native_root(&self) -> FragmentId {
        self.native_fragments.root()
    }

    /// Frozen schedule placements, including assigned scan splits and stream
    /// destinations.  The encoder receives no mutable scheduling capability.
    pub fn schedule(&self) -> &'a SchedulingPlan {
        self.schedule
    }

    pub fn query_options(&self) -> &'a QueryOptions {
        self.options
    }

    pub fn topological_fragment_order(&self) -> &[FragmentId] {
        &self.plan.order
    }

    pub fn seal(
        &self,
        submissions: Vec<ValidatedNativeSubmission>,
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
                submission.task_key(),
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
            self.root_fetch.task_key(),
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

/// One fragment, as submission encoding reads it.
#[derive(Clone)]
pub(crate) struct SubmissionFragmentFacts {
    fragment_id: FragmentId,
    role: NativeSubmissionFragmentRole,
    /// What this fragment delivers. Only the root's is read -- to tell the
    /// fetch path what shape to expect -- but which fragment is the root is
    /// the schedule's answer, not the plan's, so every fragment carries its
    /// own rather than the plan guessing which one will be asked.
    output_columns: Vec<PlanOutputColumn>,
}

impl SubmissionFragmentFacts {
    /// One fragment of a completed plan.
    pub(crate) const fn for_completed_plan(
        fragment_id: FragmentId,
        role: NativeSubmissionFragmentRole,
        output_columns: Vec<PlanOutputColumn>,
    ) -> Self {
        Self {
            fragment_id,
            role,
            output_columns,
        }
    }

    pub(crate) const fn role(&self) -> NativeSubmissionFragmentRole {
        self.role
    }

    pub(crate) fn output_columns(&self) -> &[PlanOutputColumn] {
        &self.output_columns
    }
}

/// What submission encoding reads about a plan, as values.
///
/// A stream edge is read only for whether it exists, so submission carries
/// the set of fragments that have one beside their result role and columns.
#[derive(Clone)]
pub(crate) struct SubmissionPlanFacts {
    order: Vec<FragmentId>,
    fragments: Vec<SubmissionFragmentFacts>,
    stream_edge_sources: std::collections::BTreeSet<FragmentId>,
}

impl SubmissionPlanFacts {
    /// The completed plan supplies exact fragment roles and stream ownership.
    pub(crate) fn for_completed_plan(
        order: Vec<FragmentId>,
        fragments: Vec<SubmissionFragmentFacts>,
        stream_edge_sources: std::collections::BTreeSet<FragmentId>,
    ) -> Self {
        Self {
            order,
            fragments,
            stream_edge_sources,
        }
    }

    /// The fragments this plan has, as the set every other artifact is
    /// checked against.
    pub(crate) fn fragment_ids(&self) -> std::collections::BTreeSet<FragmentId> {
        self.fragments
            .iter()
            .map(|fragment| fragment.fragment_id)
            .collect()
    }

    pub(crate) fn fragment(&self, fragment_id: FragmentId) -> Option<&SubmissionFragmentFacts> {
        self.fragments
            .iter()
            .find(|fragment| fragment.fragment_id == fragment_id)
    }

    pub(crate) fn has_stream_edge_from(&self, fragment_id: FragmentId) -> bool {
        self.stream_edge_sources.contains(&fragment_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeSubmissionFragmentRole {
    Result,
    NonTerminal,
}

impl NativeSubmissionFragmentRole {
    pub const fn uses_result_buffer(self) -> bool {
        matches!(self, Self::Result)
    }
}

/// Consuming, artifact-bound native submission payload.  Core validates this
/// attachment before it builds any task fragment plan from it.
pub struct NativeSubmissionAttachment {
    handoff_id: u64,
    execution_id: QueryExecutionId,
    submissions: Vec<ValidatedNativeSubmission>,
    root_fetch: RootFetchMetadata,
}

impl NativeSubmissionAttachment {
    pub(crate) fn matches(&self, handoff_id: u64, execution_id: QueryExecutionId) -> bool {
        self.handoff_id == handoff_id && self.execution_id == execution_id
    }

    pub(crate) fn into_parts(self) -> (Vec<ValidatedNativeSubmission>, RootFetchMetadata) {
        (self.submissions, self.root_fetch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_proto_codec::lifecycle::AttemptId;
    use novarocks_types::QueryId;

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
