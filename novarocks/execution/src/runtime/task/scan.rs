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
//! A compiled program addresses each Scan by its physical scan node instead:
//! the same node keys the Task's split queue, its range assignment and the
//! bound operation the compiled pipeline polls.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::node::scan::ScanSource;
use crate::exec::pipeline::binding::ScanBindings;
use crate::runtime::task::error::{
    LocalTaskLaunchError, LocalTaskLaunchErrorKind, LocalTaskLaunchStage,
};
use crate::runtime::task::instance::TaskInstanceSpec;
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};

/// The Task-owned scan sources of one compiled program: exactly one per
/// compiled Scan node. Each is bound to its instance assignment at launch.
pub type CompiledScanSources = BTreeMap<ProgramNodeId, Arc<dyn ScanSource>>;

/// Every compiled Scan's physical scan node, keyed by its local node. The
/// program's scan addresses must name exactly its Scan nodes, once each.
pub fn compiled_scan_nodes(
    program: &LocalProgram,
) -> Result<BTreeMap<ProgramNodeId, FragmentNodeId>, FragmentBindingError> {
    let scans = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .filter(|(_, node)| matches!(node.kind(), ProgramNodeKind::Scan { .. }))
        .map(|(index, _)| ProgramNodeId::new(index))
        .collect::<BTreeSet<_>>();
    if scans != program.scan_inputs().keys().copied().collect() {
        return Err(FragmentBindingError::new(
            FragmentBindingTarget::Program,
            FragmentBindingErrorKind::InvalidAssignment,
            "compiled scan addresses do not match the program's scans",
        ));
    }
    let mut nodes = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for (id, input) in program.scan_inputs() {
        let scan_node = i32::try_from(input.scan_node).map_err(|_| {
            FragmentBindingError::new(
                FragmentBindingTarget::Program,
                FragmentBindingErrorKind::InvalidAssignment,
                format!("compiled scan node {} exceeds i32", input.scan_node),
            )
        })?;
        if !seen.insert(scan_node) {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::ScanNode(scan_node),
                FragmentBindingErrorKind::InvalidAssignment,
                "compiled scan node is addressed by more than one scan",
            ));
        }
        nodes.insert(*id, FragmentNodeId::new(scan_node));
    }
    Ok(nodes)
}

/// A compiled submission's scan facts agree exactly: one Task source per
/// compiled Scan, and one instance assignment per physical scan node, with
/// nothing assigned to a node the program does not scan.
pub(crate) fn validate_compiled_scan_sources(
    program: &LocalProgram,
    sources: &CompiledScanSources,
    instance: &TaskInstanceSpec,
) -> Result<(), FragmentBindingError> {
    let nodes = compiled_scan_nodes(program)?;
    if let Some(id) = nodes.keys().find(|id| !sources.contains_key(id)) {
        return Err(FragmentBindingError::new(
            FragmentBindingTarget::ScanNode(nodes[id].get()),
            FragmentBindingErrorKind::MissingAssignment,
            "compiled scan has no Task scan source",
        ));
    }
    if let Some(id) = sources.keys().find(|id| !nodes.contains_key(id)) {
        return Err(FragmentBindingError::new(
            FragmentBindingTarget::Program,
            FragmentBindingErrorKind::ExtraAssignment,
            format!(
                "Task scan source targets local node {}, which is not a compiled scan",
                id.index()
            ),
        ));
    }
    let scan_nodes = nodes.values().copied().collect::<BTreeSet<_>>();
    if let Some(node) = scan_nodes
        .iter()
        .find(|node| instance.scan_assignments().get(node).is_none())
    {
        return Err(FragmentBindingError::new(
            FragmentBindingTarget::ScanNode(node.get()),
            FragmentBindingErrorKind::MissingAssignment,
            "compiled scan has no instance scan assignment",
        ));
    }
    if let Some((node, _)) = instance
        .scan_assignments()
        .iter()
        .find(|(node, _)| !scan_nodes.contains(node))
    {
        return Err(FragmentBindingError::new(
            FragmentBindingTarget::ScanNode(node.get()),
            FragmentBindingErrorKind::ExtraAssignment,
            "scan assignment names no compiled scan",
        ));
    }
    Ok(())
}

/// Bind every compiled Scan's Task source to its instance assignment, keyed
/// by physical scan node. ScanSource::bind checks the assignment variant.
pub(crate) fn materialize_compiled_scan_bindings(
    program: &LocalProgram,
    sources: &CompiledScanSources,
    instance: &TaskInstanceSpec,
) -> Result<ScanBindings, LocalTaskLaunchError> {
    validate_compiled_scan_sources(program, sources, instance).map_err(|error| {
        LocalTaskLaunchError::from_failure(
            LocalTaskLaunchStage::Materialize,
            LocalTaskLaunchErrorKind::Materialization,
            error.into(),
        )
    })?;
    let nodes = compiled_scan_nodes(program).map_err(|error| {
        LocalTaskLaunchError::from_failure(
            LocalTaskLaunchStage::Materialize,
            LocalTaskLaunchErrorKind::Materialization,
            error.into(),
        )
    })?;
    let mut bindings = ScanBindings::default();
    for (id, source) in sources {
        bind_scan(source, nodes[id].get(), instance, &mut bindings)?;
    }
    Ok(bindings)
}

fn bind_scan(
    source: &std::sync::Arc<dyn crate::exec::node::scan::ScanSource>,
    node_id: i32,
    instance: &TaskInstanceSpec,
    bindings: &mut ScanBindings,
) -> Result<(), LocalTaskLaunchError> {
    let assignment = instance
        .scan_assignments()
        .get(&FragmentNodeId::new(node_id))
        .ok_or_else(|| {
            LocalTaskLaunchError::from_failure(
                LocalTaskLaunchStage::Materialize,
                LocalTaskLaunchErrorKind::Materialization,
                FragmentBindingError::new(
                    FragmentBindingTarget::ScanNode(node_id),
                    FragmentBindingErrorKind::MissingAssignment,
                    format!("missing scan assignment for node {node_id}"),
                )
                .into(),
            )
        })?;
    let op = source.bind(assignment.ranges().clone()).map_err(|error| {
        LocalTaskLaunchError::from_failure(
            LocalTaskLaunchStage::Materialize,
            LocalTaskLaunchErrorKind::Materialization,
            error,
        )
    })?;
    bindings.insert(node_id, op);
    Ok(())
}

#[cfg(test)]
#[path = "compiled_scan_fixture.rs"]
pub(crate) mod compiled_fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::node::scan::{BoundScanRanges, ScanOp, ScanStreamSource, UnusedScanStream};
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::task::instance::{
        ExchangeInputAssignments, InstanceOrdinal, ScanAssignments, TaskKey, TaskRuntimeOptions,
        TaskSinkAssignment,
    };
    use novarocks_execution_contract::FragmentContractVersion;
    use novarocks_types::{QueryId, UniqueId};
    use std::num::NonZeroUsize;

    // Each bind creates an independent operation, while the pure compiled
    // program and the task-owned source can safely be shared by instances.
    struct CountingSource;
    struct CountingOp;
    impl ScanSource for CountingSource {
        fn bind(
            &self,
            ranges: BoundScanRanges,
        ) -> crate::runtime::task::ExecutionResult<Arc<dyn ScanOp>> {
            match ranges {
                BoundScanRanges::None => Ok(Arc::new(CountingOp)),
                other => Err(novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new(&format!(
                        "counting source expects no ranges, got {other:?}"
                    )),
                )
                .into()),
            }
        }
    }
    impl ScanOp for CountingOp {
        fn stream_source(&self) -> Arc<dyn ScanStreamSource> {
            Arc::new(UnusedScanStream)
        }
        fn profile_name(&self) -> Option<String> {
            Some("CountingCompiledScan".into())
        }
    }

    #[test]
    fn shared_compiled_program_materializes_independent_ops_for_each_instance() {
        let program = compiled_fixture::scan_program(1, false);
        let scan = *program.scan_inputs().keys().next().unwrap();
        assert_eq!(program.scan_inputs().len(), 1);
        let nodes_before = program.graph().nodes().len();
        let sources = BTreeMap::from([(scan, Arc::new(CountingSource) as Arc<dyn ScanSource>)]);
        let bind = |ordinal| {
            let instance = TaskInstanceSpec::new_native(
                FragmentContractVersion::CURRENT,
                QueryId::new(20, 30),
                TaskKey::new(UniqueId::new(20, ordinal)),
                ScanAssignments::try_new(BTreeMap::from([(
                    FragmentNodeId::new(compiled_fixture::SCAN_NODE),
                    BoundScanRanges::None,
                )]))
                .unwrap(),
                ExchangeInputAssignments::default(),
                TaskSinkAssignment::None,
                TaskRuntimeOptions::new(QueryOptions::default(), false),
                NonZeroUsize::MIN,
                InstanceOrdinal::try_new(1).unwrap(),
            );
            materialize_compiled_scan_bindings(&program, &sources, &instance)
                .unwrap()
                .get(compiled_fixture::SCAN_NODE)
                .unwrap()
        };
        let a = bind(1);
        let b = bind(2);
        let c = bind(3);
        for op in [&a, &b, &c] {
            assert_eq!(op.profile_name().as_deref(), Some("CountingCompiledScan"));
        }
        assert!(!Arc::ptr_eq(&a, &b));
        assert!(!Arc::ptr_eq(&a, &c));
        assert!(!Arc::ptr_eq(&b, &c));
        assert_eq!(program.graph().nodes().len(), nodes_before);
        assert_eq!(program.scan_inputs().len(), 1);
        assert_eq!(
            compiled_scan_nodes(&program).unwrap()[&scan].get(),
            compiled_fixture::SCAN_NODE
        );
    }
}
