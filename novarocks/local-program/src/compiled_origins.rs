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

//! Correspondence between an actual local graph and diagnostic provenance.
//! This is a source-reference/identity check, not proof of lowering semantics.
//! Synthetic operators with no lowered nodes retain the existing author's
//! permission; the compiler still proves that such an actual entity exists.

use crate::{
    DiagnosticSourceNodeId, LocalOperatorProvenance, LocalProgramGraph, MAX_PROFILE_REFERENCES,
    ProgramNodeId, ProgramProvenance, ProvenanceError,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{collections::BTreeSet, fmt};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompiledOriginsError {
    Control(CompileControlError),
    Provenance(ProvenanceError),
    InvalidLocalIdentity,
    SourceMismatch,
}
impl From<CompileControlError> for CompiledOriginsError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ProvenanceError> for CompiledOriginsError {
    fn from(error: ProvenanceError) -> Self {
        match error {
            ProvenanceError::Control(cause) => Self::Control(cause),
            other => Self::Provenance(other),
        }
    }
}
impl fmt::Display for CompiledOriginsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Provenance(error) => error.fmt(f),
            Self::InvalidLocalIdentity => {
                f.write_str("compiled graph has an invalid local node identity")
            }
            Self::SourceMismatch => {
                f.write_str("operator sources differ from the actual lowered nodes")
            }
        }
    }
}
impl std::error::Error for CompiledOriginsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Provenance(error) => Some(error),
            Self::InvalidLocalIdentity | Self::SourceMismatch => None,
        }
    }
}

/// Borrow the actual graph: a count-equivalent receipt is not correspondence.
/// Graph source references and the original operator-reference table keep
/// their existing independent bounds. BTree key operations and destruction
/// are not allocation authorization or an internal library cost proof.
pub(crate) fn compile_origins(
    graph: &LocalProgramGraph,
    operators: Vec<LocalOperatorProvenance>,
    allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
    control: &dyn PureCompileControl,
) -> Result<ProgramProvenance, CompiledOriginsError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = compile_origins_core(graph, operators, allowed_sources, &mut work);
    if matches!(&result, Err(CompiledOriginsError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn compile_origins_core(
    graph: &LocalProgramGraph,
    operators: Vec<LocalOperatorProvenance>,
    allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProgramProvenance, CompiledOriginsError> {
    let mut count = 0usize;
    // Preflight all source lengths before allocating per-node sets.
    for (index, node) in graph.nodes().iter().enumerate() {
        let same = node.local_id() == Some(ProgramNodeId::new(index));
        work.step()?;
        if !same {
            return Err(CompiledOriginsError::InvalidLocalIdentity);
        }
        let next = count.checked_add(node.physical_sources().len());
        work.step()?;
        count = next
            .filter(|n| *n <= MAX_PROFILE_REFERENCES)
            .ok_or(CompileControlError::ResourceExhausted)?;
    }
    for node in graph.nodes() {
        let mut sources = BTreeSet::new();
        for source in node.physical_sources() {
            let allowed = allowed_sources.contains(source);
            work.step()?;
            if !allowed {
                return Err(ProvenanceError::InvalidSource.into());
            }
            let unique = sources.insert(*source);
            work.step()?;
            if !unique {
                return Err(ProvenanceError::DuplicateSource.into());
            }
        }
    }
    // Use the sole original normalization/coverage/cost-owner author with the
    // actual graph length. Its checked output supplies valid canonical node
    // references; no independently supplied lowered-node count is accepted.
    work.flush()?;
    let provenance = ProgramProvenance::try_new(
        operators,
        graph.nodes().len(),
        allowed_sources,
        work.control(),
    )
    .map_err(CompiledOriginsError::from)?;
    for operator in provenance.operators().values() {
        let empty = operator.lowered_nodes.is_empty();
        work.step()?;
        if empty {
            continue;
        }
        let mut expected = BTreeSet::new();
        for node in &operator.lowered_nodes {
            let actual = &graph.nodes()[node.index()];
            work.step()?;
            for source in actual.physical_sources() {
                expected.insert(*source);
                work.step()?;
            }
        }
        let same_length = expected.len() == operator.sources.len();
        work.step()?;
        if !same_length {
            return Err(CompiledOriginsError::SourceMismatch);
        }
        for (actual, claimed) in expected.iter().zip(operator.sources.iter()) {
            let same = actual == claimed;
            work.step()?;
            if !same {
                return Err(CompiledOriginsError::SourceMismatch);
            }
        }
    }
    Ok(provenance)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BindingRequirements, CompileProfile, KernelAbiVersion, LocalOperatorId,
        LocalOperatorOrigin, MetricAggregation, OperatorMetricAggregation, ProgramNode,
        ProgramNodeKind, StaticLayout, StaticValues, SyntheticOperatorReason,
    };
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use novarocks_types::SlotId;
    use std::{
        num::NonZeroUsize,
        sync::{Arc, Mutex},
    };

    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<u32>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::LowerProgram);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            let index = trace.len();
            trace.push(units);
            if let Some((at, cause)) = self.stop.clone()
                && index == at
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn graph(sources: Vec<Vec<DiagnosticSourceNodeId>>, legacy: bool) -> LocalProgramGraph {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let layout = StaticLayout::try_new(schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
        let values = StaticValues::try_new(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7]))]).unwrap(),
            layout.clone(),
        )
        .unwrap();
        let mut nodes = Vec::new();
        for (index, sources) in sources.into_iter().enumerate() {
            let kind = if index == 0 {
                ProgramNodeKind::Values {
                    values: values.clone(),
                }
            } else {
                ProgramNodeKind::Limit {
                    input: ProgramNodeId::new(index - 1),
                    limit: Some(7),
                    offset: 0,
                }
            };
            nodes.push(if legacy {
                ProgramNode::new(index as i32, kind, layout.clone())
            } else {
                ProgramNode::new_local(ProgramNodeId::new(index), sources, kind, layout.clone())
            });
        }
        let root = ProgramNodeId::new(nodes.len() - 1);
        LocalProgramGraph::try_new(
            nodes,
            root,
            &[],
            CompileProfile::new(
                NonZeroUsize::new(1).unwrap(),
                None,
                layout.identity().unwrap(),
                KernelAbiVersion::CURRENT,
            ),
            BindingRequirements::try_new(vec![]).unwrap(),
        )
        .unwrap()
    }
    fn operator(nodes: &[usize], sources: &[u32]) -> LocalOperatorProvenance {
        LocalOperatorProvenance {
            id: LocalOperatorId::new(0),
            lowered_nodes: nodes.iter().copied().map(ProgramNodeId::new).collect(),
            sources: sources
                .iter()
                .copied()
                .map(DiagnosticSourceNodeId::new)
                .collect(),
            origin: if nodes.len() == 1 && sources.len() == 1 {
                LocalOperatorOrigin::Direct
            } else {
                LocalOperatorOrigin::Fused
            },
            cost_owner: LocalOperatorId::new(0),
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        }
    }
    fn allowed(sources: &[u32]) -> BTreeSet<DiagnosticSourceNodeId> {
        sources
            .iter()
            .copied()
            .map(DiagnosticSourceNodeId::new)
            .collect()
    }

    #[test]
    fn exact_actual_node_union_preserves_sparse_sources_and_original_normalization() {
        let allowed = allowed(&[0, u32::MAX]);
        let source = graph(
            vec![
                vec![DiagnosticSourceNodeId::new(u32::MAX)],
                vec![DiagnosticSourceNodeId::new(0)],
            ],
            false,
        );
        let receipt = compile_origins(
            &source,
            vec![operator(&[1, 0], &[u32::MAX, 0])],
            &allowed,
            &Control::default(),
        )
        .unwrap();
        let actual = receipt.get(LocalOperatorId::new(0)).unwrap();
        assert_eq!(
            actual.lowered_nodes.as_ref(),
            &[ProgramNodeId::new(0), ProgramNodeId::new(1)]
        );
        assert_eq!(
            actual.sources.as_ref(),
            &[
                DiagnosticSourceNodeId::new(0),
                DiagnosticSourceNodeId::new(u32::MAX)
            ]
        );
        assert_eq!(receipt.cost_owners().count(), 1);
        // A count-identical graph with different sources cannot reuse this claim.
        let foreign = graph(vec![vec![DiagnosticSourceNodeId::new(0)]], false);
        assert_eq!(
            compile_origins(
                &foreign,
                vec![operator(&[0], &[u32::MAX])],
                &allowed,
                &Control::default()
            )
            .unwrap_err(),
            CompiledOriginsError::SourceMismatch
        );
    }

    #[test]
    fn legacy_identity_disallowed_and_duplicate_actual_sources_fail_closed() {
        assert_eq!(
            compile_origins(
                &graph(vec![vec![]], true),
                vec![operator(&[0], &[0])],
                &allowed(&[0]),
                &Control::default()
            )
            .unwrap_err(),
            CompiledOriginsError::InvalidLocalIdentity
        );
        for (sources, expected) in [
            (
                vec![DiagnosticSourceNodeId::new(9)],
                ProvenanceError::InvalidSource,
            ),
            (
                vec![DiagnosticSourceNodeId::new(0); 2],
                ProvenanceError::DuplicateSource,
            ),
        ] {
            assert_eq!(
                compile_origins(
                    &graph(vec![sources], false),
                    vec![operator(&[0], &[0])],
                    &allowed(&[0]),
                    &Control::default()
                )
                .unwrap_err(),
                CompiledOriginsError::Provenance(expected)
            );
        }
    }

    #[test]
    fn sole_provenance_author_keeps_coverage_duplicate_and_cost_owner_rules() {
        let source = graph(vec![vec![DiagnosticSourceNodeId::new(0)]], false);
        let base = operator(&[0], &[0]);
        assert_eq!(
            compile_origins(
                &source,
                vec![base.clone(), base.clone()],
                &allowed(&[0]),
                &Control::default()
            )
            .unwrap_err(),
            CompiledOriginsError::Provenance(ProvenanceError::DuplicateOperator)
        );
        let mut missing = base;
        missing.cost_owner = LocalOperatorId::new(u32::MAX);
        assert_eq!(
            compile_origins(&source, vec![missing], &allowed(&[0]), &Control::default())
                .unwrap_err(),
            CompiledOriginsError::Provenance(ProvenanceError::MissingCostOwner)
        );
        let wider = graph(
            vec![
                vec![DiagnosticSourceNodeId::new(0)],
                vec![DiagnosticSourceNodeId::new(0)],
            ],
            false,
        );
        assert_eq!(
            compile_origins(
                &wider,
                vec![operator(&[0], &[0])],
                &allowed(&[0]),
                &Control::default()
            )
            .unwrap_err(),
            CompiledOriginsError::Provenance(ProvenanceError::UncoveredLoweredNode)
        );
    }

    #[test]
    fn synthetic_empty_lowered_nodes_keep_original_permission_without_new_authority() {
        let source = graph(vec![vec![DiagnosticSourceNodeId::new(0)]], false);
        let mut synthetic = operator(&[], &[]);
        synthetic.id = LocalOperatorId::new(u32::MAX);
        synthetic.cost_owner = synthetic.id;
        synthetic.origin = LocalOperatorOrigin::Synthetic {
            reason: SyntheticOperatorReason::ResultSink,
        };
        let receipt = compile_origins(
            &source,
            vec![operator(&[0], &[0]), synthetic],
            &allowed(&[0]),
            &Control::default(),
        )
        .unwrap();
        assert!(
            receipt
                .get(LocalOperatorId::new(u32::MAX))
                .unwrap()
                .lowered_nodes
                .is_empty()
        );
    }

    #[test]
    fn actual_source_walk_and_delegate_keep_entry_quantum_tail_three_causes() {
        let ids = (0..320u32).collect::<Vec<_>>();
        let source = graph(
            vec![
                ids.iter()
                    .copied()
                    .map(DiagnosticSourceNodeId::new)
                    .collect(),
            ],
            false,
        );
        let claim = operator(&[0], &ids);
        let allowed = allowed(&ids);
        let baseline = Control::default();
        compile_origins(&source, vec![claim.clone()], &allowed, &baseline).unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Mutex::default(),
                    stop: Some((at, cause.clone())),
                };
                assert_eq!(
                    compile_origins(&source, vec![claim.clone()], &allowed, &control).unwrap_err(),
                    CompiledOriginsError::Control(cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
        let ordinary = Control::default();
        assert_eq!(
            compile_origins(&source, vec![operator(&[0], &[0])], &allowed, &ordinary).unwrap_err(),
            CompiledOriginsError::SourceMismatch
        );
        let trace = ordinary.trace.lock().unwrap().clone();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::default(),
                stop: Some((trace.len() - 1, cause.clone())),
            };
            assert_eq!(
                compile_origins(&source, vec![operator(&[0], &[0])], &allowed, &control)
                    .unwrap_err(),
                CompiledOriginsError::Control(cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace);
        }
    }

    #[test]
    fn actual_graph_reference_budget_is_checked_before_source_indexing() {
        let source = graph(
            vec![vec![
                DiagnosticSourceNodeId::new(0);
                MAX_PROFILE_REFERENCES + 1
            ]],
            false,
        );
        assert_eq!(
            compile_origins(
                &source,
                vec![operator(&[0], &[0])],
                &allowed(&[0]),
                &Control::default()
            )
            .unwrap_err(),
            CompiledOriginsError::Control(CompileControlError::ResourceExhausted)
        );
    }
}
