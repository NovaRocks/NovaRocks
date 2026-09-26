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
//! The pure LocalProgram names scan requirements. A FragmentSubmission owns
//! the exact Task-local ScanSource capabilities and enriched scan ranges. At
//! launch this module binds the latter to per-instance ScanOps keyed by native
//! node ID. A shared FragmentProgram retains no provider runtime capability.

use crate::exec::fragment::program::{FragmentNodeId, FragmentProgram};
use crate::exec::node::LocalRuntimeBindings;
use crate::exec::pipeline::binding::ScanBindings;
use crate::runtime::fragment::error::{
    FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::instance::FragmentInstanceSpec;
use novarocks_local_program::ProgramNodeKind;

/// Bind the exact Task-owned scan sources validated by FragmentSubmission.
/// ScanSource::bind checks the assignment variant before any pipeline runs.
pub(crate) fn materialize_scan_bindings(
    program: &FragmentProgram,
    runtime_bindings: &LocalRuntimeBindings,
    instance: &FragmentInstanceSpec,
) -> Result<ScanBindings, FragmentLaunchError> {
    let mut bindings = ScanBindings::default();
    for (node_id, source) in &runtime_bindings.scans {
        let node = program
            .local_program()
            .nodes()
            .get(node_id.index())
            .ok_or_else(|| {
                FragmentLaunchError::new(
                    FragmentLaunchStage::Materialize,
                    FragmentLaunchErrorKind::Materialization,
                    format!(
                        "runtime scan binding targets unknown local node {}",
                        node_id.index()
                    ),
                )
            })?;
        if !matches!(node.kind(), ProgramNodeKind::Scan { .. }) {
            return Err(FragmentLaunchError::new(
                FragmentLaunchStage::Materialize,
                FragmentLaunchErrorKind::Materialization,
                format!(
                    "runtime scan binding targets non-scan node {}",
                    node.native_node_id()
                ),
            ));
        }
        bind_scan(source, node.native_node_id(), instance, &mut bindings)?;
    }
    Ok(bindings)
}

fn bind_scan(
    source: &std::sync::Arc<dyn crate::exec::node::scan::ScanSource>,
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
    let op = source.bind(assignment.ranges().clone()).map_err(|error| {
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
    use crate::runtime::query_options::QueryOptions;
    use novarocks_types::QueryId;
    use novarocks_types::UniqueId;

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

    fn static_source_ranges() -> BoundScanRanges {
        BoundScanRanges::None
    }

    const SCAN_NODE_ID: i32 = 7;

    /// A one-node program: a scan node holding only a static `CountingFileSource`.
    fn scan_program() -> FragmentProgram {
        let root = ExecNode {
            kind: ExecNodeKind::Scan(
                ScanNode::new(Arc::new(CountingFileSource))
                    .with_node_id(SCAN_NODE_ID)
                    .with_output_chunk_schema(Arc::new(crate::exec::chunk::ChunkSchema::empty())),
            ),
        };
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root,
        };
        let profile = plan
            .local_compile_profile(NonZeroUsize::new(1).unwrap(), None)
            .unwrap();
        let (local, runtime) = plan
            .into_local_program_and_bindings(
                profile,
                BTreeMap::from([(
                    SCAN_NODE_ID,
                    crate::runtime::fragment::submission::tests::static_scan_for_test(),
                )]),
                Vec::new(),
                novarocks_local_program::StaticSinkProgram::Noop,
            )
            .unwrap();
        assert_eq!(runtime.scan_count(), 1);
        FragmentProgram::try_new(
            Arc::new(local),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::from([(
                FragmentNodeId::new(SCAN_NODE_ID),
                ScanSourceContract::new(ScanAssignmentKind::File),
            )]),
            BTreeMap::new(),
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        )
        .unwrap()
    }

    fn runtime_for_scan() -> crate::exec::node::LocalRuntimeBindings {
        crate::exec::node::LocalRuntimeBindings {
            scans: BTreeMap::from([(
                novarocks_local_program::ProgramNodeId::new(0),
                Arc::new(CountingFileSource) as Arc<dyn ScanSource>,
            )]),
            writers: BTreeMap::new(),
            finishers: BTreeMap::new(),
        }
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
    fn materializes_op_from_static_source_assignment() {
        let program = scan_program();
        let instance = instance_with_scan(
            scan_assignments(static_source_ranges()),
            UniqueId::new(10, 11),
        );

        let bindings = materialize_scan_bindings(&program, &runtime_for_scan(), &instance)
            .expect("materialize");
        let op = bindings.get(SCAN_NODE_ID).expect("bound op for scan node");
        assert_eq!(op.build_morsels().expect("morsels").morsels.len(), 1);
    }

    #[test]
    fn multi_instance_sharing_yields_independent_ops_and_leaves_program_untouched() {
        // One shared program, two instances with independent static assignments.
        let program = Arc::new(scan_program());

        let instance_a = instance_with_scan(
            scan_assignments(static_source_ranges()),
            UniqueId::new(20, 1),
        );
        let instance_b = instance_with_scan(
            scan_assignments(static_source_ranges()),
            UniqueId::new(20, 2),
        );

        let bindings_a = materialize_scan_bindings(&program, &runtime_for_scan(), &instance_a)
            .expect("materialize a");
        let bindings_b = materialize_scan_bindings(&program, &runtime_for_scan(), &instance_b)
            .expect("materialize b");

        let op_a = bindings_a.get(SCAN_NODE_ID).expect("op a");
        let op_b = bindings_b.get(SCAN_NODE_ID).expect("op b");

        // Independent op sets remain distinct even when their assignments have
        // identical provider-neutral shapes.
        assert_eq!(op_a.build_morsels().expect("a morsels").morsels.len(), 1);
        assert_eq!(op_b.build_morsels().expect("b morsels").morsels.len(), 1);

        // The shared program is untouched: re-binding against a third instance
        // still works and reads only the static source, and the two Arcs above
        // point at distinct ops.
        assert!(!Arc::ptr_eq(&op_a, &op_b));
        let instance_c = instance_with_scan(
            scan_assignments(static_source_ranges()),
            UniqueId::new(20, 3),
        );
        let bindings_c = materialize_scan_bindings(&program, &runtime_for_scan(), &instance_c)
            .expect("materialize c");
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

        let error = materialize_scan_bindings(&program, &runtime_for_scan(), &instance)
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
    fn incompatible_range_variant_fails_at_bind() {
        let program = scan_program();
        // The source expects its provider-neutral static assignment; hand it a
        // schema-selection assignment instead.
        let instance = instance_with_scan(
            scan_assignments(BoundScanRanges::SchemaSelection { should_scan: true }),
            UniqueId::new(40, 1),
        );

        let error = materialize_scan_bindings(&program, &runtime_for_scan(), &instance)
            .expect_err("wrong range variant must fail at bind");
        assert_eq!(error.stage(), FragmentLaunchStage::Materialize);
        assert_eq!(error.kind(), FragmentLaunchErrorKind::Materialization);
        assert!(error.detail().contains("bind failed"), "{}", error.detail());
    }
}
