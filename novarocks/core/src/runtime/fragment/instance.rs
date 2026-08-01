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

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use crate::common::types::UniqueId;
use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use crate::exec::fragment::program::{FragmentContractVersion, FragmentNodeId};
use crate::exec::node::scan::BoundScanRanges;
use crate::runtime::endpoint::FragmentDestination;
use crate::runtime::query_context::QueryId;
use crate::runtime::query_options::QueryOptions;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FragmentInstanceId(UniqueId);

impl FragmentInstanceId {
    pub const fn new(value: UniqueId) -> Self {
        Self(value)
    }

    pub const fn get(self) -> UniqueId {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackendNum(i32);

impl BackendNum {
    pub fn try_new(value: i32) -> Result<Self, FragmentBindingError> {
        if value < 0 {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::Instance,
                FragmentBindingErrorKind::InvalidAssignment,
                format!("backend number must be non-negative, got {value}"),
            ));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

/// A single scan node's instance-local enriched connector ranges.
///
/// There is no `kind` here on purpose: the connector type is already carried by
/// the `BoundScanRanges` variant, and variant-vs-source correctness is enforced
/// at materialize time by `ScanSource::bind`. (The decoders' pre-enrichment
/// kind guards read the transient `(ScanAssignmentKind, Vec<ScanRangeParams>)`
/// carrier instead.) jdbc/mysql scans legitimately have no `ScanAssignmentKind`.
#[derive(Clone, Debug)]
pub struct ScanAssignment {
    ranges: BoundScanRanges,
}

impl ScanAssignment {
    fn new(ranges: BoundScanRanges) -> Self {
        Self { ranges }
    }

    /// The instance's enriched connector ranges. `materialize_scan_bindings`
    /// clones these into `ScanSource::bind` to produce this instance's op.
    pub fn ranges(&self) -> &BoundScanRanges {
        &self.ranges
    }
}

#[derive(Clone, Debug, Default)]
pub struct ScanAssignments(BTreeMap<FragmentNodeId, ScanAssignment>);

impl ScanAssignments {
    /// Carry the already-enriched `BoundScanRanges` per plan scan node. No
    /// validation happens here (kept `Result` for call-site symmetry): the
    /// old per-range kind assertion is gone, presence is cross-checked in
    /// `FragmentSubmission::try_new`, and variant-vs-source correctness is
    /// enforced at materialize time by `ScanSource::bind`.
    #[allow(clippy::unnecessary_wraps)]
    pub fn try_new(
        assignments: BTreeMap<FragmentNodeId, BoundScanRanges>,
    ) -> Result<Self, FragmentBindingError> {
        let bound_assignments = assignments
            .into_iter()
            .map(|(node_id, ranges)| (node_id, ScanAssignment::new(ranges)))
            .collect();
        Ok(Self(bound_assignments))
    }

    pub fn get(&self, node_id: &FragmentNodeId) -> Option<&ScanAssignment> {
        self.0.get(node_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&FragmentNodeId, &ScanAssignment)> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeInputAssignment {
    sender_count: NonZeroUsize,
}

impl ExchangeInputAssignment {
    pub const fn new(sender_count: NonZeroUsize) -> Self {
        Self { sender_count }
    }

    pub const fn sender_count(&self) -> NonZeroUsize {
        self.sender_count
    }
}

#[derive(Clone, Debug, Default)]
pub struct ExchangeInputAssignments(BTreeMap<FragmentNodeId, ExchangeInputAssignment>);

impl ExchangeInputAssignments {
    pub fn new(assignments: BTreeMap<FragmentNodeId, ExchangeInputAssignment>) -> Self {
        Self(assignments)
    }

    pub fn get(&self, node_id: &FragmentNodeId) -> Option<&ExchangeInputAssignment> {
        self.0.get(node_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&FragmentNodeId, &ExchangeInputAssignment)> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FragmentSinkAssignment {
    None,
    StreamDestinations {
        destinations: Vec<FragmentDestination>,
        sender_id: Option<i32>,
    },
    DestinationGroups {
        groups: Vec<Vec<FragmentDestination>>,
        sender_id: Option<i32>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct FragmentRuntimeOptions {
    query_options: QueryOptions,
    typed_result_sink: bool,
}

impl FragmentRuntimeOptions {
    pub fn new(query_options: QueryOptions, typed_result_sink: bool) -> Self {
        Self {
            query_options,
            typed_result_sink,
        }
    }

    pub fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }

    pub const fn typed_result_sink(&self) -> bool {
        self.typed_result_sink
    }
}

#[derive(Debug)]
pub struct FragmentInstanceSpec {
    contract_version: FragmentContractVersion,
    query_id: QueryId,
    fragment_instance_id: FragmentInstanceId,
    scan_assignments: ScanAssignments,
    exchange_inputs: ExchangeInputAssignments,
    sink_assignment: FragmentSinkAssignment,
    runtime_options: FragmentRuntimeOptions,
    pipeline_dop: NonZeroUsize,
    backend_num: BackendNum,
}

impl FragmentInstanceSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn new_native(
        contract_version: FragmentContractVersion,
        query_id: QueryId,
        fragment_instance_id: FragmentInstanceId,
        scan_assignments: ScanAssignments,
        exchange_inputs: ExchangeInputAssignments,
        sink_assignment: FragmentSinkAssignment,
        runtime_options: FragmentRuntimeOptions,
        pipeline_dop: NonZeroUsize,
        backend_num: BackendNum,
    ) -> Self {
        Self {
            contract_version,
            query_id,
            fragment_instance_id,
            scan_assignments,
            exchange_inputs,
            sink_assignment,
            runtime_options,
            pipeline_dop,
            backend_num,
        }
    }

    pub const fn contract_version(&self) -> FragmentContractVersion {
        self.contract_version
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub const fn fragment_instance_id(&self) -> FragmentInstanceId {
        self.fragment_instance_id
    }

    pub const fn scan_assignments(&self) -> &ScanAssignments {
        &self.scan_assignments
    }

    pub const fn exchange_inputs(&self) -> &ExchangeInputAssignments {
        &self.exchange_inputs
    }

    pub const fn sink_assignment(&self) -> &FragmentSinkAssignment {
        &self.sink_assignment
    }

    pub const fn runtime_options(&self) -> &FragmentRuntimeOptions {
        &self.runtime_options
    }

    pub const fn pipeline_dop(&self) -> NonZeroUsize {
        self.pipeline_dop
    }

    pub const fn backend_num(&self) -> BackendNum {
        self.backend_num
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;

    use crate::common::types::UniqueId;
    use crate::exec::fragment::error::{FragmentBindingErrorKind, FragmentBindingTarget};
    use crate::exec::fragment::program::{FragmentContractVersion, FragmentNodeId};
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::scan_range::{FileFormat, ScanRange, ScanRangeParams};

    use super::*;

    /// A simple connector-neutral assignment carrier for exercising instance
    /// binding without exposing provider file range values.
    fn bound_ranges() -> BoundScanRanges {
        BoundScanRanges::None
    }

    #[test]
    fn backend_num_rejects_negative_values() {
        let error = BackendNum::try_new(-1).expect_err("negative backend number must fail");
        assert_eq!(error.target(), FragmentBindingTarget::Instance);
        assert_eq!(error.kind(), FragmentBindingErrorKind::InvalidAssignment);
        assert!(error.detail().contains("-1"), "{}", error.detail());

        assert_eq!(BackendNum::try_new(0).expect("zero is valid").get(), 0);
    }

    #[test]
    fn fragment_instance_id_round_trips_unique_id() {
        let raw = UniqueId::new(7, 11);
        assert_eq!(FragmentInstanceId::new(raw).get(), raw);
    }

    #[test]
    fn scan_assignments_carry_bound_ranges_per_node() {
        // The instance carrier now holds the enriched `BoundScanRanges` per
        // node with no kind and no per-range validation (that moved to
        // `ScanSource::bind` at materialize time). Empty and non-empty carriers
        // both round-trip through the map key.
        let node_id = FragmentNodeId::new(17);
        let assignments =
            ScanAssignments::try_new(BTreeMap::from([(node_id, BoundScanRanges::None)]))
                .expect("carrier");
        let assignment = assignments.get(&node_id).expect("assignment at map key");
        assert!(matches!(assignment.ranges(), BoundScanRanges::None));

        let second_node = FragmentNodeId::new(23);
        let assignments = ScanAssignments::try_new(BTreeMap::from([(second_node, bound_ranges())]))
            .expect("carrier");
        assert!(matches!(
            assignments.get(&second_node).expect("assignment").ranges(),
            BoundScanRanges::None
        ));
    }

    #[test]
    fn assignment_collections_use_typed_ordered_node_ids_and_nonzero_senders() {
        let scan_node = FragmentNodeId::new(3);
        let scans = ScanAssignments::try_new(BTreeMap::from([(scan_node, bound_ranges())]))
            .expect("file assignment");
        assert_eq!(scans.len(), 1);
        assert!(!scans.is_empty());
        assert!(scans.get(&scan_node).is_some());
        assert_eq!(
            scans
                .iter()
                .map(|(node_id, _)| node_id.get())
                .collect::<Vec<_>>(),
            vec![3]
        );

        assert!(NonZeroUsize::new(0).is_none());
        let exchange_node = FragmentNodeId::new(5);
        let exchange_assignment =
            ExchangeInputAssignment::new(NonZeroUsize::new(2).expect("non-zero sender count"));
        let exchanges =
            ExchangeInputAssignments::new(BTreeMap::from([(exchange_node, exchange_assignment)]));
        assert_eq!(exchanges.len(), 1);
        assert!(!exchanges.is_empty());
        assert_eq!(
            exchanges
                .get(&exchange_node)
                .expect("exchange assignment")
                .sender_count()
                .get(),
            2
        );
        assert_eq!(
            exchanges
                .iter()
                .map(|(node_id, _)| node_id.get())
                .collect::<Vec<_>>(),
            vec![5]
        );
    }

    #[test]
    fn sink_assignment_variants_preserve_explicit_placement() {
        assert!(matches!(
            FragmentSinkAssignment::None,
            FragmentSinkAssignment::None
        ));
        assert!(matches!(
            FragmentSinkAssignment::StreamDestinations {
                destinations: Vec::new(),
                sender_id: Some(9),
            },
            FragmentSinkAssignment::StreamDestinations {
                destinations,
                sender_id: Some(9),
            } if destinations.is_empty()
        ));
        assert!(matches!(
            FragmentSinkAssignment::DestinationGroups {
                groups: vec![Vec::new()],
                sender_id: None,
            },
            FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            } if groups.len() == 1
        ));
        assert!(matches!(
            FragmentSinkAssignment::DestinationGroups {
                groups: vec![Vec::new()],
                sender_id: Some(11),
            },
            FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: Some(11),
            } if groups.len() == 1
        ));
    }

    #[test]
    fn instance_spec_exposes_immutable_domain_parts() {
        let scan_node = FragmentNodeId::new(3);
        let scans = ScanAssignments::try_new(BTreeMap::from([(scan_node, bound_ranges())]))
            .expect("empty file assignment");
        let exchange_node = FragmentNodeId::new(5);
        let exchanges = ExchangeInputAssignments::new(BTreeMap::from([(
            exchange_node,
            ExchangeInputAssignment::new(NonZeroUsize::new(2).expect("sender count")),
        )]));
        let runtime_options = FragmentRuntimeOptions::new(QueryOptions::default(), true);
        let spec = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId::new(13, 17),
            FragmentInstanceId::new(UniqueId::new(19, 23)),
            scans,
            exchanges,
            FragmentSinkAssignment::StreamDestinations {
                destinations: Vec::new(),
                sender_id: None,
            },
            runtime_options,
            NonZeroUsize::new(4).expect("pipeline DOP"),
            BackendNum::try_new(0).expect("backend number"),
        );

        assert_eq!(spec.contract_version(), FragmentContractVersion::CURRENT);
        assert_eq!(spec.query_id(), QueryId::new(13, 17));
        assert_eq!(spec.fragment_instance_id().get(), UniqueId::new(19, 23));
        assert!(spec.scan_assignments().get(&scan_node).is_some());
        assert_eq!(
            spec.exchange_inputs()
                .get(&exchange_node)
                .expect("exchange assignment")
                .sender_count()
                .get(),
            2
        );
        assert!(matches!(
            spec.sink_assignment(),
            FragmentSinkAssignment::StreamDestinations {
                destinations,
                sender_id: None,
            } if destinations.is_empty()
        ));
        assert!(
            spec.runtime_options()
                .query_options()
                .eq(&QueryOptions::default())
        );
        assert!(spec.runtime_options().typed_result_sink());
        assert_eq!(spec.pipeline_dop().get(), 4);
        assert_eq!(spec.backend_num().get(), 0);
    }
}
