// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The plan facts one attempt reads while it runs.

use super::artifact::FragmentId;
use novarocks_spi::connector::read_stack::{
    ConnectorReadColumnHandle, ConnectorReadConstraint, runtime::ConnectorReadAssignment,
};
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;

use super::attempt_runtime_filter_facts::AttemptRuntimeFilterFacts;

/// A client-visible name paired with the completed producer's full binding.
/// This owner remains independent of the completed plan and keeps its creation
/// lifetime when the plan representation has been released.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlanOutputColumn {
    pub(crate) name: String,
    pub(crate) ty: novarocks_type_contract::FunctionValueType,
}

#[cfg(test)]
impl PlanOutputColumn {
    pub(crate) fn data_type(&self) -> &arrow::datatypes::DataType {
        self.ty.data_type()
    }

    pub(crate) fn nullable(&self) -> bool {
        self.ty.nullable()
    }

    pub(crate) fn logical_type(&self) -> novarocks_type_contract::ValueLogicalType {
        self.ty.logical_type()
    }
}

/// Test authors declare their intended root domain explicitly. This constructor
/// never reads a Field annotation and refuses impossible carrier declarations.
#[cfg(test)]
pub(crate) fn declared_test_value(
    data_type: arrow::datatypes::DataType,
    nullable: bool,
    domain: novarocks_physical_plan::ResultValueDomain,
) -> Result<novarocks_type_contract::FunctionValueType, novarocks_type_contract::ValueTypeError> {
    use novarocks_physical_plan::ResultValueDomain as D;
    use novarocks_type_contract::ValueLogicalType as L;
    novarocks_type_contract::FunctionValueType::try_with_logical_type(
        data_type,
        nullable,
        match domain {
            D::Plain => L::Physical,
            D::Json => L::Json,
            D::Variant => L::Variant,
            D::Hll => L::Hll,
            D::Bitmap => L::Bitmap,
            D::Object => L::Object,
            D::Percentile => L::Percentile,
        },
    )
}

/// What an attempt reads about its plan after preparation has finished with
/// it.
///
/// These are values, not a borrow of the representation that produced them.
/// An attempt asks how rows move between fragments, whether any runtime
/// filter has a channel to deploy, and which write targets its root owns --
/// none of which requires knowing whether a sealed plan or a completed plan
/// built it. Projecting once, here, is what lets both answer.
/// One provider read of this plan, as the attempt that opens it reads it.
///
/// Everything here is a plan fact. The capability that performs the read is
/// not: it belongs to the attempt and is joined to these facts when the split
/// source is opened.
pub(crate) struct AttemptScanFacts {
    pub(crate) fragment_id: FragmentId,
    pub(crate) plan_node_id: i32,
    /// The provider columns this scan reads, in the order it produces them.
    pub(crate) assignments: Vec<ConnectorReadAssignment>,
    /// Which runtime filter constrains which provider column, already
    /// resolved against the assignments.
    pub(crate) dynamic_filters: Vec<(u32, ConnectorReadColumnHandle)>,
    pub(crate) constraint: ConnectorReadConstraint,
}

/// One exchange edge, as the pieces that place and connect tasks read it.
///
/// The placement and delivery fields are what the task graph and the runtime-filter compiler
/// actually read. The sealed plan's own edge additionally carries the
/// analyzer expressions a hash partition was derived from and the payloads
/// of the CTE and change-stream edge kinds; nothing on this path reads
/// those, and a completed plan names its partition keys by value rather than
/// by expression, so carrying them would put something in the contract that
/// only one of the two representations can fill.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AttemptEdgeFacts {
    pub(crate) source_fragment_id: FragmentId,
    pub(crate) target_fragment_id: FragmentId,
    pub(crate) target_exchange_node_id: i32,
    pub(crate) partition_kind: AttemptPartitionKind,
    pub(crate) delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
}

/// The transport topology choice needed by Task placement for one edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AttemptPartitionKind {
    Unpartitioned,
    Random,
    Hash,
}

pub(crate) struct WriteRootFacts {
    targets: Box<[WriteTargetOrdinal]>,
    summary_kinds: novarocks_result_contract::PerKind<()>,
}

impl WriteRootFacts {
    /// Both facts come from the same completed publication-owning TableFinish.
    pub(crate) fn new(
        targets: Box<[WriteTargetOrdinal]>,
        summary_kinds: novarocks_result_contract::PerKind<()>,
    ) -> Self {
        Self {
            targets,
            summary_kinds,
        }
    }
    pub(crate) fn targets(&self) -> &[WriteTargetOrdinal] {
        &self.targets
    }
    pub(crate) const fn summary_kinds(&self) -> &novarocks_result_contract::PerKind<()> {
        &self.summary_kinds
    }
}

pub(crate) struct AttemptPlanFacts {
    scheduling: super::fragment_scheduling::FragmentSchedulingFacts,
    edges: Box<[AttemptEdgeFacts]>,
    scans: Box<[AttemptScanFacts]>,
    runtime_filters: AttemptRuntimeFilterFacts,
    submission: super::artifact::native_submission::SubmissionPlanFacts,
    write_root: Option<WriteRootFacts>,
}

impl AttemptPlanFacts {
    /// The same facts, for a plan that was completed rather than sealed.
    pub(crate) fn from_completed(
        scheduling: super::fragment_scheduling::FragmentSchedulingFacts,
        edges: Vec<AttemptEdgeFacts>,
        scans: Vec<AttemptScanFacts>,
        submission: super::artifact::native_submission::SubmissionPlanFacts,
        runtime_filters: AttemptRuntimeFilterFacts,
        write_root: Option<WriteRootFacts>,
    ) -> Self {
        Self {
            scheduling,
            edges: edges.into_boxed_slice(),
            scans: scans.into_boxed_slice(),
            runtime_filters,
            submission,
            write_root,
        }
    }

    /// The facts scheduling reads, projected once from whichever
    /// representation built this execution.
    pub(crate) const fn scheduling(&self) -> &super::fragment_scheduling::FragmentSchedulingFacts {
        &self.scheduling
    }

    /// Every provider read this plan performs, in plan order.
    pub(crate) const fn scans(&self) -> &[AttemptScanFacts] {
        &self.scans
    }

    pub(crate) fn scan(
        &self,
        fragment_id: FragmentId,
        plan_node_id: i32,
    ) -> Option<&AttemptScanFacts> {
        self.scans
            .iter()
            .find(|scan| scan.fragment_id == fragment_id && scan.plan_node_id == plan_node_id)
    }

    /// The exchange edges of this plan, in the planner's own order.
    pub(crate) const fn edges(&self) -> &[AttemptEdgeFacts] {
        &self.edges
    }

    pub(crate) const fn runtime_filters(&self) -> &AttemptRuntimeFilterFacts {
        &self.runtime_filters
    }

    /// What putting this plan's fragments on the wire reads about it.
    pub(crate) const fn submission(
        &self,
    ) -> &super::artifact::native_submission::SubmissionPlanFacts {
        &self.submission
    }

    pub(crate) fn write_root_targets(&self) -> Option<&[WriteTargetOrdinal]> {
        self.write_root.as_ref().map(WriteRootFacts::targets)
    }

    /// Declared kinds from the same original TableFinish as its target set.
    /// No observed record, task status or receive order can mint this fact.
    pub(crate) fn write_root_summary_kinds(
        &self,
    ) -> Option<&novarocks_result_contract::PerKind<()>> {
        self.write_root.as_ref().map(WriteRootFacts::summary_kinds)
    }
}
