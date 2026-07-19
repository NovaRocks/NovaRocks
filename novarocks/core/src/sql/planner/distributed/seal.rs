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

use std::fmt;

use crate::runtime_filter::model::graph::RuntimeFilterGraph;
use crate::runtime_filter::model::validation::GraphValidationError;

use super::boundary::{
    BoundaryCatalog, BoundaryError, ExecutionColumnIdAllocator, build_boundary_catalog,
};
use super::fragment::{DistributedPlanDraft, FragmentEdge, FragmentId, PlanFragment};
use super::output::{
    FragmentEdgeOutputCatalog, NodeOutputCatalog, NodeOutputError, WriteContractCatalog,
    WriteContractError, build_fragment_edge_output_catalog, build_node_output_catalog,
    build_write_contract_catalog,
};
use super::topology::{TopologyContract, TopologyError, build_topology_contract};
use super::validation::{self, DistributedPlanValidationError, RuntimeFilterPlanValidationError};

#[derive(Clone, Debug)]
struct DistributedPlanData {
    fragments: Vec<PlanFragment>,
    root_fragment_id: FragmentId,
    edges: Vec<FragmentEdge>,
    runtime_filter_graph: RuntimeFilterGraph,
    // Authoritative boundary membership catalog derived at seal time.
    boundaries: BoundaryCatalog,
    // Final state of the single query-scoped occurrence allocator. Preserved so
    // later stages can resume allocating internal occurrences without rebuilding it.
    execution_column_id_allocator: ExecutionColumnIdAllocator,
    // Authoritative fragment-graph execution shape derived at seal time.
    topology: TopologyContract,
    // Authoritative covered node execution outputs derived at seal time.
    node_outputs: NodeOutputCatalog,
    // Authoritative fragment output columns and stream-edge projections derived
    // at seal time. The native encoder maps these 1:1.
    fragment_edge_outputs: FragmentEdgeOutputCatalog,
    // Authoritative Iceberg write output/target-schema and change-stream router
    // branch partitions derived at seal time. The native encoder maps these 1:1
    // instead of synthesizing the write output or reconstructing a router
    // partition from ordinals.
    write_contracts: WriteContractCatalog,
}

#[derive(Clone, Debug)]
pub(crate) struct DistributedPlan {
    data: DistributedPlanData,
}

impl DistributedPlan {
    pub(crate) fn fragments(&self) -> &[PlanFragment] {
        &self.data.fragments
    }

    pub(crate) fn root_fragment_id(&self) -> FragmentId {
        self.data.root_fragment_id
    }

    pub(crate) fn edges(&self) -> &[FragmentEdge] {
        &self.data.edges
    }

    pub(crate) fn runtime_filter_graph(&self) -> &RuntimeFilterGraph {
        &self.data.runtime_filter_graph
    }

    // Consumed by coordinator preparation for production boundary validation.
    // The remaining boundary projection is test-only diagnostics.
    pub(crate) fn boundaries(&self) -> &BoundaryCatalog {
        &self.data.boundaries
    }

    // CGO-9C resumes occurrence allocation from this preserved state.
    #[allow(dead_code)]
    pub(crate) fn execution_column_id_allocator(&self) -> &ExecutionColumnIdAllocator {
        &self.data.execution_column_id_allocator
    }

    // Consumed on the production path by codegen `build()` (CGO-9B/Task 4),
    // which projects the sealed order/anchor into the scheduler's topology.
    pub(crate) fn topology(&self) -> &TopologyContract {
        &self.data.topology
    }

    // Consumed by the native encoder (CGO-9C Task 1), which reads each covered
    // node's execution output from this catalog instead of re-deriving it, and
    // by later CGO-9C tasks that thread the occurrence mapping.
    pub(crate) fn node_outputs(&self) -> &NodeOutputCatalog {
        &self.data.node_outputs
    }

    // Consumed by the native encoder (CGO-9C Task 2), which maps each fragment's
    // finalized output columns and each stream edge's finalized projection 1:1
    // instead of re-deriving a stream schema or patching the exchange receiver.
    pub(crate) fn fragment_edge_outputs(&self) -> &FragmentEdgeOutputCatalog {
        &self.data.fragment_edge_outputs
    }

    // Consumed by the native encoder (CGO-9C Task 3), which maps each Iceberg
    // write fragment's finalized output expressions/target schema and each
    // change-stream router branch's finalized partition 1:1 instead of
    // synthesizing the write output or reconstructing a partition from ordinals.
    pub(crate) fn write_contracts(&self) -> &WriteContractCatalog {
        &self.data.write_contracts
    }

    #[cfg(test)]
    pub(in crate::sql::planner::distributed) fn remove_fragment_output_for_test(
        &mut self,
        fragment_id: FragmentId,
    ) {
        self.data
            .fragment_edge_outputs
            .remove_fragment_output_for_test(fragment_id);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::sql::planner::distributed) enum DistributedPlanSealError {
    EmptyFragments,
    MissingRootFragmentId,
    RootFragmentNotFound { root_fragment_id: FragmentId },
    Structural(DistributedPlanValidationError),
    RuntimeFilterGraph(GraphValidationError),
    RuntimeFilterPlan(RuntimeFilterPlanValidationError),
    Boundary(BoundaryError),
    Topology(TopologyError),
    NodeOutput(NodeOutputError),
    WriteContract(WriteContractError),
}

impl fmt::Display for DistributedPlanSealError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFragments => formatter.write_str("distributed plan has no fragments"),
            Self::MissingRootFragmentId => {
                formatter.write_str("distributed plan is missing root fragment id")
            }
            Self::RootFragmentNotFound { root_fragment_id } => write!(
                formatter,
                "distributed plan root fragment id={root_fragment_id} was not found"
            ),
            Self::Structural(error) => error.fmt(formatter),
            Self::RuntimeFilterGraph(error) => error.fmt(formatter),
            Self::RuntimeFilterPlan(error) => error.fmt(formatter),
            Self::Boundary(error) => error.fmt(formatter),
            Self::Topology(error) => error.fmt(formatter),
            Self::NodeOutput(error) => error.fmt(formatter),
            Self::WriteContract(error) => error.fmt(formatter),
        }
    }
}

pub(in crate::sql::planner::distributed) fn seal_draft(
    draft: DistributedPlanDraft,
) -> Result<DistributedPlan, DistributedPlanSealError> {
    let DistributedPlanDraft {
        fragments,
        root_fragment_id,
        edges,
        runtime_filter_graph,
    } = draft;
    if fragments.is_empty() {
        return Err(DistributedPlanSealError::EmptyFragments);
    }
    let root_fragment_id =
        root_fragment_id.ok_or(DistributedPlanSealError::MissingRootFragmentId)?;
    if !fragments
        .iter()
        .any(|fragment| fragment.fragment_id == root_fragment_id)
    {
        return Err(DistributedPlanSealError::RootFragmentNotFound { root_fragment_id });
    }
    validation::validate_distributed_structure(&fragments, root_fragment_id, &edges)
        .map_err(DistributedPlanSealError::Structural)?;
    // Finalize the fragment-graph execution shape as an extension of structural
    // validation: this derives the topological order, execution anchor, and the
    // producer/terminal-write/result sets, and fails fast on the graph-level
    // invariants (acyclicity, order consistency, anchor determinacy) that
    // structural validation does not cover. Runs before construction and before
    // the runtime-filter graph check.
    let topology = build_topology_contract(&fragments, root_fragment_id, &edges)
        .map_err(DistributedPlanSealError::Topology)?;
    // Validate the query-global graph first, then its bidirectional ownership
    // relation with the distributed nodes. No node carries a semantic RF DTO.
    runtime_filter_graph
        .validate()
        .map_err(DistributedPlanSealError::RuntimeFilterGraph)?;
    validation::validate_runtime_filter_graph_against_plan(&runtime_filter_graph, &fragments)
        .map_err(DistributedPlanSealError::RuntimeFilterPlan)?;
    // Finalize logical boundary membership and occurrence identity. This only
    // derives from the now known-valid fragments/edges/sinks; it fails fast on
    // any unresolved column reference and never repairs or guesses. The final
    // allocator state is preserved in the sealed plan for CGO-9C.
    let mut execution_column_id_allocator = ExecutionColumnIdAllocator::new();
    let boundaries = build_boundary_catalog(
        &fragments,
        root_fragment_id,
        &edges,
        &mut execution_column_id_allocator,
    )
    .map_err(DistributedPlanSealError::Boundary)?;
    // Finalize the covered node execution outputs. This reads each covered
    // node's planner-computed output columns (for a hash-aggregate, with per-mode
    // intermediate aggregate-state types applied), fails fast on any missing or
    // inconsistent output, and numbers each occurrence from the SAME allocator
    // (reusing boundary occurrences for boundary-participating root outputs).
    // Runs after boundary derivation so the allocator continues from there.
    let node_outputs =
        build_node_output_catalog(&fragments, &boundaries, &mut execution_column_id_allocator)
            .map_err(DistributedPlanSealError::NodeOutput)?;
    // Finalize each fragment's output columns and each stream edge's projection
    // from the sealed fragments, edges, and the node-output catalog. This is the
    // planner-side successor of the native encoder's fragment-output derivation,
    // stream-schema reselection, and exchange-receiver patch: it fails fast on an
    // inconsistency rather than falling back, and the encoder maps it 1:1.
    let fragment_edge_outputs =
        build_fragment_edge_output_catalog(&fragments, &edges, &node_outputs)
            .map_err(DistributedPlanSealError::NodeOutput)?;
    // Finalize the write-path semantics the native encoder used to synthesize or
    // reconstruct at encode time: each Iceberg write fragment's output
    // expressions and target output schema, and each change-stream router
    // branch's typed partition. Derives purely from the sealed fragments' sinks
    // and output columns/exprs and fails fast rather than falling back; the
    // encoder maps the result 1:1.
    let write_contracts = build_write_contract_catalog(&fragments)
        .map_err(DistributedPlanSealError::WriteContract)?;
    Ok(DistributedPlan {
        data: DistributedPlanData {
            fragments,
            root_fragment_id,
            edges,
            runtime_filter_graph,
            boundaries,
            execution_column_id_allocator,
            topology,
            node_outputs,
            fragment_edge_outputs,
            write_contracts,
        },
    })
}

#[cfg(test)]
pub(super) mod test_support {
    use crate::runtime_filter::model::graph::RuntimeFilterGraph;
    use crate::sql::planner::distributed::fragment::{
        DataPartition, DataSink, DistributedPlanDraft, PlanFragment,
    };
    use crate::sql::planner::distributed::node::{DistributedNode, DistributedNodeKind};
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    pub(super) fn single_fragment_draft(root_fragment_id: Option<u32>) -> DistributedPlanDraft {
        DistributedPlanDraft {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 1,
                    fragment_id: 0,
                    tuple_ids: Vec::new(),
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
                    runtime_filter_binding_ids: Vec::new(),
                    children: Vec::new(),
                    stats: PhysicalPlanStats {
                        output_row_count: 0.0,
                        row_count_confidence: PlannerConfidence::Fallback,
                        column_statistics: Default::default(),
                        cost_estimate: None,
                        broadcast_decision: None,
                    },
                    payload: DistributedNodeKind::Values(
                        crate::sql::planner::payload::PlanValuesNode {
                            rows: Vec::new(),
                            columns: Vec::new(),
                        },
                    ),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: Vec::new(),
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id,
            edges: Vec::new(),
            runtime_filter_graph: RuntimeFilterGraph::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, NullSemantics, PlanFragmentId, PlanNodeId,
        ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
        RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::model::graph::{
        ApplyPoint, ConsumerRequirement, PlanLocation, ProducerRequirement,
        RuntimeFilterBindingRole, RuntimeFilterBindingSpec, RuntimeFilterChannelSpec,
        RuntimeFilterGraph,
    };
    use crate::runtime_filter::model::validation::GraphValidationErrorKind;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::planner::distributed::fragment::DistributedPlanDraft;
    use crate::sql::planner::distributed::validation::RuntimeFilterPlanValidationError;

    use super::{DistributedPlanSealError, seal_draft};

    /// Minimal `TypedExpr` used to populate binding expressions. The seal only
    /// inspects the graph's structural contract, never the bound expression.
    fn expression() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    /// Value-domain (Join) membership channel mirroring the RFD-1 `join_channel`
    /// fixture so a sealed graph exercises the same contract the model validates.
    fn join_channel_spec(channel_id: ChannelId) -> RuntimeFilterChannelSpec {
        RuntimeFilterChannelSpec {
            channel_id,
            logical_domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            reduction_requirement: ReductionRequirement::SetUnion,
            allowed_contribution_kinds: BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            required_consumer_capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 4096,
                deadline_ms: 30_000,
                max_retries: 3,
            },
        }
    }

    fn join_producer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
    ) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id,
            channel_id,
            coverage_witness_id: Some(CoverageWitnessId::new(1)),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(0),
                node_id: PlanNodeId::new(1),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                contribution_kinds: BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ]),
                completion_requirement: CompletionRequirement::ProducerClosed,
                join_key_ordinal: 0,
            }),
        }
    }

    fn join_consumer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
    ) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id,
            channel_id,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(0),
                node_id: PlanNodeId::new(1),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                activation: ConsumerActivation::BlockingSnapshot,
                target: crate::runtime_filter::model::graph::ConsumerBindingTarget::SourceBoundary,
            }),
        }
    }

    /// A structurally valid, non-empty graph: one channel with a matched
    /// producer/consumer pair. `RuntimeFilterGraph::validate` accepts it.
    fn valid_non_empty_graph() -> RuntimeFilterGraph {
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_channel(join_channel_spec(ChannelId::new(1)))
            .unwrap();
        graph
            .insert_binding(join_producer_binding(BindingId::new(1), ChannelId::new(1)))
            .unwrap();
        graph
            .insert_binding(join_consumer_binding(BindingId::new(2), ChannelId::new(1)))
            .unwrap();
        graph
    }

    /// A non-empty but structurally invalid graph: a producer binding points at a
    /// channel that was never inserted. `validate` rejects it with `UnknownChannel`.
    fn graph_with_binding_to_unknown_channel() -> RuntimeFilterGraph {
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_binding(join_producer_binding(BindingId::new(1), ChannelId::new(99)))
            .unwrap();
        graph
    }

    #[test]
    fn minimal_seal_rejects_empty_fragments_before_root_state() {
        let draft = DistributedPlanDraft {
            fragments: Vec::new(),
            root_fragment_id: None,
            edges: Vec::new(),
            runtime_filter_graph: RuntimeFilterGraph::default(),
        };

        let error = seal_draft(draft).expect_err("empty draft must not seal");

        assert!(matches!(error, DistributedPlanSealError::EmptyFragments));
        assert_eq!(error.to_string(), "distributed plan has no fragments");
    }

    #[test]
    fn minimal_seal_rejects_missing_root_id() {
        let draft = super::test_support::single_fragment_draft(None);

        let error = seal_draft(draft).expect_err("missing root id must not seal");

        assert!(matches!(
            error,
            DistributedPlanSealError::MissingRootFragmentId
        ));
        assert_eq!(
            error.to_string(),
            "distributed plan is missing root fragment id"
        );
    }

    #[test]
    fn minimal_seal_rejects_root_id_not_present_in_fragments() {
        let draft = super::test_support::single_fragment_draft(Some(7));

        let error = seal_draft(draft).expect_err("unknown root id must not seal");

        assert!(matches!(
            error,
            DistributedPlanSealError::RootFragmentNotFound {
                root_fragment_id: 7
            }
        ));
        assert_eq!(
            error.to_string(),
            "distributed plan root fragment id=7 was not found"
        );
    }

    #[test]
    fn minimal_seal_constructs_an_immutable_plan_with_read_only_accessors() {
        let plan = seal_draft(super::test_support::single_fragment_draft(Some(0)))
            .expect("valid draft seals");

        assert_eq!(plan.fragments().len(), 1);
        assert_eq!(plan.root_fragment_id(), 0);
        assert!(plan.edges().is_empty());
        assert!(plan.runtime_filter_graph().is_empty());
    }

    #[test]
    fn seal_validates_and_accepts_an_empty_runtime_filter_graph() {
        let draft = super::test_support::single_fragment_draft(Some(0));
        assert!(draft.runtime_filter_graph.is_empty());

        let plan = seal_draft(draft).expect("an empty runtime filter graph is legal and validates");

        assert!(plan.runtime_filter_graph().is_empty());
    }

    #[test]
    fn seal_validates_and_accepts_a_valid_non_empty_runtime_filter_graph() {
        let mut draft = super::test_support::single_fragment_draft(Some(0));
        draft.runtime_filter_graph = valid_non_empty_graph();
        draft.fragments[0].root.runtime_filter_binding_ids =
            vec![BindingId::new(1), BindingId::new(2)];

        let plan =
            seal_draft(draft).expect("a structurally valid non-empty graph must seal successfully");

        assert_eq!(plan.runtime_filter_graph().channel_count(), 1);
        assert_eq!(plan.runtime_filter_graph().binding_count(), 2);
    }

    #[test]
    fn seal_rejects_a_structurally_invalid_runtime_filter_graph() {
        let mut draft = super::test_support::single_fragment_draft(Some(0));
        draft.runtime_filter_graph = graph_with_binding_to_unknown_channel();

        let error = seal_draft(draft).expect_err("a graph that fails validation must not seal");

        let DistributedPlanSealError::RuntimeFilterGraph(graph_error) = error else {
            panic!("expected a runtime filter graph seal error, got {error:?}");
        };
        // The typed `GraphValidationError` is preserved verbatim through the seal.
        assert_eq!(graph_error.kind, GraphValidationErrorKind::UnknownChannel);
    }

    #[test]
    fn seal_reports_the_structural_error_before_the_runtime_filter_graph_error() {
        // A draft that is BOTH structurally invalid (root fragment carries a
        // non-result, non-write sink) AND carries an invalid runtime filter graph
        // must surface the structural error first: RF-graph validation runs only
        // after `validate_distributed_structure` succeeds.
        let mut draft = super::test_support::single_fragment_draft(Some(0));
        draft.fragments[0].sink = crate::sql::planner::distributed::fragment::DataSink::Noop;
        draft.runtime_filter_graph = graph_with_binding_to_unknown_channel();

        let error = seal_draft(draft).expect_err("a structurally invalid draft must not seal");

        assert!(
            matches!(error, DistributedPlanSealError::Structural(_)),
            "structural validation must precede runtime filter graph validation, got {error:?}"
        );
    }

    #[test]
    fn seal_rejects_a_graph_binding_that_is_not_attached_to_its_node() {
        let mut draft = super::test_support::single_fragment_draft(Some(0));
        draft.runtime_filter_graph = valid_non_empty_graph();

        let error = seal_draft(draft).expect_err("unattached graph bindings must fail sealing");

        assert!(matches!(
            error,
            DistributedPlanSealError::RuntimeFilterPlan(
                RuntimeFilterPlanValidationError::BindingNotAttached(binding_id)
            ) if binding_id == BindingId::new(1)
        ));
    }

    #[test]
    fn seal_rejects_an_attached_binding_with_a_mismatched_location() {
        let mut draft = super::test_support::single_fragment_draft(Some(0));
        draft.runtime_filter_graph = valid_non_empty_graph();
        draft.fragments[0].root.runtime_filter_binding_ids =
            vec![BindingId::new(1), BindingId::new(2)];
        draft
            .runtime_filter_graph
            .binding_mut_for_test(BindingId::new(2))
            .expect("consumer binding")
            .location
            .node_id = PlanNodeId::new(99);

        let error = seal_draft(draft).expect_err("location mismatch must fail sealing");

        assert!(matches!(
            error,
            DistributedPlanSealError::RuntimeFilterPlan(
                RuntimeFilterPlanValidationError::BindingLocationMismatch(binding_id)
            ) if binding_id == BindingId::new(2)
        ));
    }

    #[test]
    fn seal_rejects_a_binding_expression_type_that_disagrees_with_its_channel() {
        let mut draft = super::test_support::single_fragment_draft(Some(0));
        draft.runtime_filter_graph = valid_non_empty_graph();
        draft.fragments[0].root.runtime_filter_binding_ids =
            vec![BindingId::new(1), BindingId::new(2)];
        draft
            .runtime_filter_graph
            .binding_mut_for_test(BindingId::new(2))
            .expect("consumer binding")
            .expression
            .data_type = DataType::Utf8;

        let error = seal_draft(draft).expect_err("type mismatch must fail sealing");

        assert!(matches!(
            error,
            DistributedPlanSealError::RuntimeFilterPlan(
                RuntimeFilterPlanValidationError::ExpressionTypeMismatch(binding_id)
            ) if binding_id == BindingId::new(2)
        ));
    }
}
