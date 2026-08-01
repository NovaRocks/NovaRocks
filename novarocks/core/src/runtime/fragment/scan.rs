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

//! Instance-local scan materialization.
//!
//! The plan's `ScanNode` is static: it holds only a `ScanSource`. Each
//! instance carries its own enriched `BoundScanRanges` in the
//! `FragmentInstanceSpec`. At launch time this module replays those ranges
//! through `ScanSource::bind` to produce the per-instance `ScanOp`, keyed by
//! plan node id, which the pipeline builder consumes. This mirrors the
//! exchange-binding materialization (`runtime::fragment::exchange`): one
//! shared `Arc<FragmentProgram>` can back many instances, each with its own
//! bound ops, without cloning or mutating the program.

use crate::exec::fragment::program::{FragmentNodeId, FragmentProgram};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::exec::pipeline::binding::ScanBindings;
use crate::runtime::fragment::error::{
    FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::instance::FragmentInstanceSpec;

/// Materialize per-node scan bindings for `instance` from the static program.
///
/// Walks the plan the same way `submission::ProgramInventory` enumerates scan
/// nodes; for each `ExecNodeKind::Scan` with a `node_id`, looks up this
/// instance's `ScanAssignment` and binds the node's `ScanSource` with the
/// enriched `BoundScanRanges`. `ScanSource::bind` is where variant-vs-source
/// correctness is enforced (a wrong `BoundScanRanges` variant fails here,
/// before any pipeline runs).
///
pub(crate) fn materialize_scan_bindings(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
) -> Result<ScanBindings, FragmentLaunchError> {
    let mut bindings = ScanBindings::default();
    visit(&program.plan().root, instance, &mut bindings)?;
    Ok(bindings)
}

fn visit(
    node: &ExecNode,
    instance: &FragmentInstanceSpec,
    bindings: &mut ScanBindings,
) -> Result<(), FragmentLaunchError> {
    match &node.kind {
        ExecNodeKind::Scan(scan) => {
            if let Some(node_id) = scan.node_id() {
                bind_scan(scan, node_id, instance, bindings)?;
            }
            Ok(())
        }
        ExecNodeKind::Values(_) | ExecNodeKind::ExchangeSource(_) | ExecNodeKind::LookUp(_) => {
            Ok(())
        }
        ExecNodeKind::AssertNumRows(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::Project(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::Filter(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::Repeat(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::ChangeEventExpand(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::UnionAll(node) => visit_inputs(&node.inputs, instance, bindings),
        ExecNodeKind::Limit(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::Fetch(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::Aggregate(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::Join(node) => {
            visit(&node.left, instance, bindings)?;
            visit(&node.right, instance, bindings)
        }
        ExecNodeKind::NestedLoopJoin(node) => {
            visit(&node.left, instance, bindings)?;
            visit(&node.right, instance, bindings)
        }
        ExecNodeKind::Sort(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::TableFunction(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::Analytic(node) => visit(&node.input, instance, bindings),
        ExecNodeKind::SetOp(node) => visit_inputs(&node.inputs, instance, bindings),
        ExecNodeKind::RuntimeFilterConsumer(node) => visit(&node.input, instance, bindings),
    }
}

fn visit_inputs(
    inputs: &[ExecNode],
    instance: &FragmentInstanceSpec,
    bindings: &mut ScanBindings,
) -> Result<(), FragmentLaunchError> {
    for input in inputs {
        visit(input, instance, bindings)?;
    }
    Ok(())
}

fn bind_scan(
    scan: &crate::exec::node::scan::ScanNode,
    node_id: i32,
    instance: &FragmentInstanceSpec,
    bindings: &mut ScanBindings,
) -> Result<(), FragmentLaunchError> {
    let assignment = instance
        .scan_assignments()
        .get(&FragmentNodeId::new(node_id))
        .ok_or_else(|| {
            FragmentLaunchError::new(
                FragmentLaunchStage::Materialize,
                FragmentLaunchErrorKind::Materialization,
                format!("missing scan assignment for node {node_id}"),
            )
        })?;
    let op = scan
        .source()
        .bind(assignment.ranges().clone())
        .map_err(|error| {
            FragmentLaunchError::new(
                FragmentLaunchStage::Materialize,
                FragmentLaunchErrorKind::Materialization,
                format!("scan node {node_id} bind failed: {error}"),
            )
        })?;
    bindings.insert(node_id, op);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::common::types::UniqueId;
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::{
        FragmentContractVersion, FragmentNodeId, FragmentProgram, FragmentProgramOptions,
        FragmentSinkSpec, RuntimeFilterContract, ScanAssignmentKind, ScanSourceContract,
    };
    use crate::exec::fragment::sink::FragmentSinkProgram;
    use crate::exec::node::scan::{
        BoundScanRanges, RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanNode, ScanOp,
        ScanSource,
    };
    use crate::exec::node::{BoxedExecIter, ExecNode, ExecNodeKind, ExecPlan};
    use crate::runtime::fragment::error::{FragmentLaunchErrorKind, FragmentLaunchStage};
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
        FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::profile::RuntimeProfile;
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;

    use super::materialize_scan_bindings;

    /// Static source used to ensure materialization reads instance assignments.
    struct CountingFileSource;

    impl ScanSource for CountingFileSource {
        fn bind(&self, ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String> {
            match ranges {
                BoundScanRanges::None => Ok(Arc::new(CountingFileOp { morsels: 1 })),
                other => Err(format!(
                    "CountingFileSource expects no ranges, got {other:?}"
                )),
            }
        }
    }

    struct CountingFileOp {
        morsels: usize,
    }

    impl ScanOp for CountingFileOp {
        fn execute_iter(
            &self,
            _morsel: ScanMorsel,
            _profile: Option<RuntimeProfile>,
            _runtime_filters: Option<&RuntimeFilterContext>,
        ) -> Result<BoxedExecIter, String> {
            Ok(Box::new(std::iter::empty()))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            let morsels = (0..self.morsels).map(test_file_morsel).collect();
            Ok(ScanMorsels::new(morsels, false))
        }
    }

    fn test_file_morsel(index: usize) -> ScanMorsel {
        ScanMorsel::FileRange {
            path: format!("s3://bucket/file-{index}.parquet"),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: index as i32,
            external_datacache: None,
        }
    }

    fn file_ranges(_count: usize) -> BoundScanRanges {
        BoundScanRanges::None
    }

    const SCAN_NODE_ID: i32 = 7;

    /// A one-node program: a scan node holding only a static `CountingFileSource`.
    fn scan_program() -> FragmentProgram {
        let root = ExecNode {
            kind: ExecNodeKind::Scan(
                ScanNode::new(Arc::new(CountingFileSource)).with_node_id(SCAN_NODE_ID),
            ),
        };
        FragmentProgram::new(
            ExecPlan {
                arena: ExprArena::default(),
                root,
            },
            FragmentSinkSpec::try_new(FragmentSinkProgram::Noop).expect("noop sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::from([(
                FragmentNodeId::new(SCAN_NODE_ID),
                ScanSourceContract::new(ScanAssignmentKind::File),
            )]),
            BTreeMap::new(),
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        )
    }

    fn instance_with_scan(assignments: ScanAssignments, finst: UniqueId) -> FragmentInstanceSpec {
        FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId::new(1, 2),
            FragmentInstanceId::new(finst),
            assignments,
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::new(1).expect("non-zero DOP"),
            BackendNum::try_new(1).expect("backend number"),
        )
    }

    fn scan_assignments(ranges: BoundScanRanges) -> ScanAssignments {
        ScanAssignments::try_new(BTreeMap::from([(
            FragmentNodeId::new(SCAN_NODE_ID),
            ranges,
        )]))
        .expect("scan assignments")
    }

    #[test]
    fn materializes_op_from_instance_ranges() {
        let program = scan_program();
        let instance = instance_with_scan(scan_assignments(file_ranges(3)), UniqueId::new(10, 11));

        let bindings = materialize_scan_bindings(&program, &instance).expect("materialize");
        let op = bindings.get(SCAN_NODE_ID).expect("bound op for scan node");
        assert_eq!(op.build_morsels().expect("morsels").morsels.len(), 3);
    }

    #[test]
    fn multi_instance_sharing_yields_independent_ops_and_leaves_program_untouched() {
        // One shared program, two instances with different File range counts.
        let program = Arc::new(scan_program());

        let instance_a = instance_with_scan(scan_assignments(file_ranges(2)), UniqueId::new(20, 1));
        let instance_b = instance_with_scan(scan_assignments(file_ranges(5)), UniqueId::new(20, 2));

        let bindings_a = materialize_scan_bindings(&program, &instance_a).expect("materialize a");
        let bindings_b = materialize_scan_bindings(&program, &instance_b).expect("materialize b");

        let op_a = bindings_a.get(SCAN_NODE_ID).expect("op a");
        let op_b = bindings_b.get(SCAN_NODE_ID).expect("op b");

        // Independent op sets: each reflects its own instance's ranges.
        assert_eq!(op_a.build_morsels().expect("a morsels").morsels.len(), 2);
        assert_eq!(op_b.build_morsels().expect("b morsels").morsels.len(), 5);

        // The shared program is untouched: re-binding against a third instance
        // still works and reads only the static source, and the two Arcs above
        // point at distinct ops.
        assert!(!Arc::ptr_eq(&op_a, &op_b));
        let instance_c = instance_with_scan(scan_assignments(file_ranges(1)), UniqueId::new(20, 3));
        let bindings_c = materialize_scan_bindings(&program, &instance_c).expect("materialize c");
        assert_eq!(
            bindings_c
                .get(SCAN_NODE_ID)
                .expect("op c")
                .build_morsels()
                .expect("c morsels")
                .morsels
                .len(),
            1
        );
    }

    #[test]
    fn missing_assignment_is_a_materialize_error() {
        let program = scan_program();
        // Instance with no scan assignment at all.
        let instance = instance_with_scan(ScanAssignments::default(), UniqueId::new(30, 1));

        let error = materialize_scan_bindings(&program, &instance)
            .expect_err("missing assignment must fail materialize");
        assert_eq!(error.stage(), FragmentLaunchStage::Materialize);
        assert_eq!(error.kind(), FragmentLaunchErrorKind::Materialization);
        assert!(
            error.detail().contains("missing scan assignment"),
            "{}",
            error.detail()
        );
    }

    #[test]
    fn wrong_range_variant_fails_at_bind() {
        let program = scan_program();
        // The source expects File ranges; hand it a None assignment instead.
        let instance = instance_with_scan(
            scan_assignments(BoundScanRanges::None),
            UniqueId::new(40, 1),
        );

        let error = materialize_scan_bindings(&program, &instance)
            .expect_err("wrong range variant must fail at bind");
        assert_eq!(error.stage(), FragmentLaunchStage::Materialize);
        assert_eq!(error.kind(), FragmentLaunchErrorKind::Materialization);
        assert!(error.detail().contains("bind failed"), "{}", error.detail());
    }
}
