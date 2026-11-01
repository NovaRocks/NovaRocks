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

//! One fragment's static plan, frozen exactly once.
//!
//! Every task of a fragment is created from the same static plan, and every
//! resend of a create repeats it. So the frontend encodes it once, here, into
//! immutable bytes that every task and every resend share, and the generated
//! message is dropped as soon as those bytes exist.
//!
//! The few typed facts the frontend needs afterwards -- which edges the sink
//! branches bind to, what the sink does, the root node an operator profile is
//! keyed by, whether the fragment writes -- are read from the very plan this
//! call encodes, in the same call. That is what makes it impossible to pair
//! these bytes with facts describing some other plan, and it is why nothing
//! later ever parses the bytes or reaches back into a generated message: the
//! plan leaves this module as bytes and facts only.
//!
//! A compiled-package carrier is frozen the same way: the checked package is
//! encoded here, and its facts are read from the package's own physical
//! fragment and the plan edges its sink names. No plan tree exists for it.
// Design: ADR-0158 (docs/adr/ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md)

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::query_execution::package_freeze::CompiledPackageCarrier;
use novarocks_execution::task_execution::FrozenBytes;
use novarocks_physical_plan::{
    Edge, EdgeId, Fragment, FragmentPackage, FragmentSink, NodeId, NodeKind, PipelineDopDomain,
    WriteTargetOrdinal,
};
use novarocks_proto_models::novarocks as wire;
use novarocks_type_contract::PureCompileControl;
use prost::Message;

use crate::metrics::task_creation::{RetainedPayload, static_fragment_frozen};
use crate::query_execution::artifact::FragmentId;
use crate::query_execution::package_freeze::{PackageFreezeError, encode_checked_package};

/// One static sink branch, in the order the static plan declares it.
///
/// The execution kernel binds its n-th sink branch to the n-th edge a task's
/// assignment names, so this order is part of the contract and is read, not
/// reconstructed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StaticSinkTarget {
    edge: EdgeId,
    role: novarocks_physical_plan::OutputRole,
    target_fragment_id: FragmentId,
    target_exchange_node_id: i32,
    delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
}

impl StaticSinkTarget {
    #[cfg(test)]
    pub(crate) const fn for_test(
        target_fragment_id: FragmentId,
        target_exchange_node_id: i32,
        delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
    ) -> Self {
        Self {
            edge: EdgeId::new(0),
            role: novarocks_physical_plan::OutputRole::Relational,
            target_fragment_id,
            target_exchange_node_id,
            delivery,
        }
    }

    pub(crate) const fn edge(self) -> EdgeId {
        self.edge
    }

    pub(crate) const fn role(self) -> novarocks_physical_plan::OutputRole {
        self.role
    }

    pub(crate) const fn delivery(
        self,
    ) -> novarocks_execution_contract::ExchangeDeliveryRequirement {
        self.delivery
    }

    pub(crate) const fn target_fragment_id(self) -> FragmentId {
        self.target_fragment_id
    }

    pub(crate) const fn target_exchange_node_id(self) -> i32 {
        self.target_exchange_node_id
    }
}

/// A writer occurrence in the same checked package as its target recipe.
/// This declares a Task writer cohort, not its BE-local drivers or objects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StaticTableWriter {
    node: NodeId,
    target: WriteTargetOrdinal,
}
impl StaticTableWriter {
    pub(crate) const fn node(self) -> NodeId {
        self.node
    }
    pub(crate) const fn target(self) -> WriteTargetOrdinal {
        self.target
    }
}

/// The typed facts of one frozen fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FragmentFacts {
    fragment_id: FragmentId,
    dop_domain: PipelineDopDomain,
    sink_targets: Box<[StaticSinkTarget]>,
    logical_summary_output: Option<StaticSinkTarget>,
    #[cfg(test)]
    root_plan_node_id: i32,
    table_writers: Box<[StaticTableWriter]>,
    carries_runtime_filter_bindings: bool,
}

impl FragmentFacts {
    pub(crate) const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub(crate) const fn dop_domain(&self) -> PipelineDopDomain {
        self.dop_domain
    }

    /// The static sink branches, in declared order.
    pub(crate) fn sink_targets(&self) -> &[StaticSinkTarget] {
        &self.sink_targets
    }

    /// The independent summary output declared by this exact checked package.
    /// This never grants provider writer membership.
    pub(crate) const fn logical_summary_output(&self) -> Option<StaticSinkTarget> {
        self.logical_summary_output
    }

    /// The plan-node id of this fragment's root (output) node.
    ///
    /// It is the identity EXPLAIN ANALYZE keys a fragment by: the renderer
    /// looks each `PLAN FRAGMENT` up by its sealed root node id, and the
    /// encoder copied that id onto the frozen plan unchanged.
    #[cfg(test)]
    pub(crate) const fn root_plan_node_id(&self) -> i32 {
        self.root_plan_node_id
    }

    /// Whether this fragment's plan contains a connector table writer.
    ///
    /// Read off the frozen plan rather than inferred from the intent: a
    /// distributed write's writer set is what decides whether the write
    /// completed, and an exchange or scan fragment of a write plan is not a
    /// writer.
    #[cfg(test)]
    pub(crate) fn declares_table_writer(&self) -> bool {
        !self.table_writers.is_empty()
    }

    pub(crate) fn table_writers(&self) -> &[StaticTableWriter] {
        &self.table_writers
    }

    /// Whether this fragment was encoded with its runtime-filter binding
    /// table. The backend requires the table on every fragment, empty or not,
    /// so this is not the same question as whether the plan declares a filter.
    pub(crate) const fn carries_runtime_filter_bindings(&self) -> bool {
        self.carries_runtime_filter_bindings
    }
}

/// One fragment's static plan as immutable bytes, with its typed facts.
///
/// Its constructor [`FragmentArtifact::freeze_package`] freezes a checked package, so the bytes
/// and the facts always come from one plan. Tasks and resends hold it by
/// `Arc` and clone the bytes' shared backing; nothing re-encodes it.
#[derive(Debug)]
pub(crate) struct FragmentArtifact {
    content: FrozenBytes,
    facts: FragmentFacts,
    /// This plan's share of the retained static-plan gauges, returned when
    /// the last task, resend or template holding the plan drops it.
    _retained: RetainedPayload,
}

impl FragmentArtifact {
    /// Encodes one fragment's checked v2 package, once, as a compiled
    /// carrier.
    ///
    /// The bytes are a `FrozenFragment` whose only field is the package: the
    /// package owns its plan version, contract revision and DOP domain, so
    /// nothing beside it restates them. The facts are read from the same
    /// package's physical fragment, and its sink branches from the plan
    /// edges that sink names, in declared order.
    pub(crate) fn freeze_package(
        package: &FragmentPackage,
        edges: &BTreeMap<EdgeId, Edge>,
        carrier: &CompiledPackageCarrier,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self>, PackageFreezeError> {
        let fragment = package.fragment();
        let fragment_id = fragment.id().get();
        // Validate the signed native representation in every build. Only tests
        // retain an additional copy of this already encoded package fact.
        let _validated_root_plan_node_id = i32::try_from(fragment.root().get()).map_err(|_| {
            PackageFreezeError::Facts(format!(
                "fragment {fragment_id} root node {} exceeds the wire node identity",
                fragment.root().get()
            ))
        })?;
        let sink_targets = package_sink_targets(fragment, edges)?;
        let logical_summary_output = match fragment.sink() {
            FragmentSink::Router { summary, .. } => Some(
                sink_targets
                    .iter()
                    .copied()
                    .find(|target| {
                        target.edge() == summary.edge
                            && target.role() == novarocks_physical_plan::OutputRole::LogicalSummary
                    })
                    .ok_or_else(|| {
                        PackageFreezeError::Facts(format!(
                            "fragment {fragment_id} has no exact frozen logical-summary output"
                        ))
                    })?,
            ),
            _ => None,
        };
        let facts = FragmentFacts {
            fragment_id,
            dop_domain: fragment.dop_domain(),
            sink_targets,
            logical_summary_output,
            #[cfg(test)]
            root_plan_node_id: _validated_root_plan_node_id,
            table_writers: fragment
                .nodes()
                .values()
                .filter_map(|node| match &node.kind {
                    NodeKind::TableWriter { target } => Some(StaticTableWriter {
                        node: node.id,
                        target: target.write_target_ordinal,
                    }),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            // The package states the fragment's complete runtime-filter
            // participation itself; no binding table is attached to it later.
            carries_runtime_filter_bindings: true,
        };
        let frozen = wire::FrozenFragment {
            package: encode_checked_package(package, carrier, control)?.into(),
        };
        let content = FrozenBytes::freeze_encoded(frozen.encode_to_vec());
        #[cfg(test)]
        tests::record_freeze();
        let retained = static_fragment_frozen(content.len());
        Ok(Arc::new(Self {
            content,
            facts,
            _retained: retained,
        }))
    }

    /// The frozen bytes every task of this fragment is created from.
    pub(crate) const fn content(&self) -> &FrozenBytes {
        &self.content
    }

    pub(crate) const fn facts(&self) -> &FragmentFacts {
        &self.facts
    }
}

/// A physical fragment's static sink branches, in the order its sink declares
/// them, each addressed by the destination of the plan edge it feeds.
fn package_sink_targets(
    fragment: &Fragment,
    edges: &BTreeMap<EdgeId, Edge>,
) -> Result<Box<[StaticSinkTarget]>, PackageFreezeError> {
    let fragment_id = fragment.id();
    let branches = match fragment.sink() {
        FragmentSink::Result | FragmentSink::RootResult(_) | FragmentSink::Noop => Vec::new(),
        FragmentSink::Stream { edge } => vec![(*edge, None)],
        FragmentSink::Multicast { edges } => edges.iter().map(|edge| (*edge, None)).collect(),
        FragmentSink::Router {
            routes, summary, ..
        } => routes
            .iter()
            .map(|route| {
                (
                    route.edge,
                    Some(novarocks_physical_plan::OutputRole::ProviderRoute {
                        target: route.write_target_ordinal,
                    }),
                )
            })
            .chain(std::iter::once((
                summary.edge,
                Some(novarocks_physical_plan::OutputRole::LogicalSummary),
            )))
            .collect(),
    };
    branches
        .into_iter()
        .map(|(edge_id, expected_role)| {
            let edge = edges.get(&edge_id).ok_or_else(|| {
                PackageFreezeError::Facts(format!(
                    "fragment {} sinks into absent edge {}",
                    fragment_id.get(),
                    edge_id.get()
                ))
            })?;
            if edge.id != edge_id || edge.source.fragment != fragment_id {
                return Err(PackageFreezeError::Facts(format!(
                    "fragment {} sinks into edge {} produced by fragment {}",
                    fragment_id.get(),
                    edge_id.get(),
                    edge.source.fragment.get()
                )));
            }
            if let Some(expected_role) = expected_role {
                let expected_port =
                    if expected_role == novarocks_physical_plan::OutputRole::LogicalSummary {
                        novarocks_physical_plan::FragmentOutputPort::LogicalSummary
                    } else {
                        novarocks_physical_plan::FragmentOutputPort::Root
                    };
                let exact_summary_projection = match fragment.sink() {
                    FragmentSink::Router { summary, .. } if edge_id == summary.edge => {
                        edge.source.projection.as_ref() == summary.columns.values().as_slice()
                    }
                    _ => true,
                };
                if edge.role != expected_role
                    || edge.source.port != expected_port
                    || edge.delivery != novarocks_physical_plan::DeliveryRequirement::MustDrain
                    || !exact_summary_projection
                {
                    return Err(PackageFreezeError::Facts(format!(
                        "Router edge {} does not match its exact output role, port, or delivery",
                        edge_id.get()
                    )));
                }
            }
            let target_exchange_node_id =
                i32::try_from(edge.destination.node.get()).map_err(|_| {
                    PackageFreezeError::Facts(format!(
                        "edge {} destination node {} exceeds the wire node identity",
                        edge_id.get(),
                        edge.destination.node.get()
                    ))
                })?;
            Ok(StaticSinkTarget {
                edge: edge_id,
                role: edge.role,
                target_fragment_id: edge.destination.fragment.get(),
                target_exchange_node_id,
                delivery: match edge.delivery {
                    novarocks_physical_plan::DeliveryRequirement::DemandDriven => {
                        novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven
                    }
                    novarocks_physical_plan::DeliveryRequirement::MustDrain => {
                        novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
                    }
                },
            })
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use std::cell::Cell;

    use super::*;

    thread_local! {
        static FREEZES: Cell<usize> = const { Cell::new(0) };
    }

    pub(super) fn record_freeze() {
        FREEZES.with(|count| count.set(count.get() + 1));
    }

    /// Inspect the production sink-fact author using a completed physical plan.
    /// This exercises no package bytes or Native execution capability.
    pub(crate) fn sink_targets_for_completed_fragment(
        fragment: &Fragment,
        edges: &BTreeMap<EdgeId, Edge>,
    ) -> Result<Box<[StaticSinkTarget]>, PackageFreezeError> {
        package_sink_targets(fragment, edges)
    }

    /// How many static plans this thread has frozen. Tests read the delta
    /// around the call they measure, so parallel tests cannot interfere.
    pub(crate) fn freezes_on_this_thread() -> usize {
        FREEZES.with(Cell::get)
    }

    /// Freeze a structurally checked package for control-plane tests. Padding
    /// belongs to real uncompressed Binary constants, so descriptor capacity
    /// tests measure the actual carrier bytes. This fixture claims no runtime
    /// support beyond Result and a single Stream sink.
    pub(crate) fn frozen_package(
        fragment_id: u32,
        root_node_id: i32,
        version: [u8; 16],
        dop_domain: PipelineDopDomain,
        target: Option<(
            u32,
            i32,
            novarocks_execution_contract::ExchangeDeliveryRequirement,
        )>,
        padding: usize,
    ) -> Arc<FragmentArtifact> {
        use novarocks_physical_plan as p;
        use novarocks_type_contract::{
            CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
            ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation,
            ExpressionUseId, SemanticParameters,
        };
        let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
        let id = p::FragmentId::new(fragment_id);
        let node = p::NodeId::new(u32::try_from(root_node_id).expect("nonnegative root"));
        let ty = p::ValueType::new(arrow::datatypes::DataType::Binary, false);
        let mut builder = p::FragmentBuilder::new(id);
        let mut constants = p::ConstantPools::empty();
        let mut values = Vec::new();
        let mut expressions = Vec::new();
        // Each pool stays below the flat-batch per-pool ceiling while the
        // complete package can exceed the task descriptor ceiling.
        let mut remaining = padding;
        loop {
            let width = remaining.min(4 << 20);
            let bytes = vec![1; width];
            let literal = p::ConstantValue::from_binary(
                Arc::new(arrow::datatypes::Field::new(
                    "payload",
                    arrow::datatypes::DataType::Binary,
                    false,
                )),
                ty.clone(),
                &bytes,
                novarocks_plan_codec::physical_package_v2::FROZEN_CONSTANT_POLICY,
                CompilePhase::Validate,
                &control,
            )
            .expect("an admitted real Binary constant");
            let pool = p::ConstantPoolId::new(values.len() as u32);
            constants
                .insert(pool, literal.pool().clone())
                .expect("unique constant pool");
            let value = builder
                .add_value(
                    ty.clone(),
                    p::ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: values.len() as u32,
                    },
                )
                .expect("output value");
            let expr = builder
                .add_expression(
                    node,
                    ty.clone(),
                    p::ExprKind::Constant(p::ConstantReference {
                        pool,
                        ordinal: literal.ordinal(),
                    }),
                )
                .expect("constant expression");
            values.push(value);
            expressions.push(expr);
            remaining -= width;
            if remaining == 0 {
                break;
            }
        }
        builder
            .add_values(
                node,
                vec![expressions.into_boxed_slice()].into_boxed_slice(),
                values.clone().into_boxed_slice(),
            )
            .expect("Values node");
        let edge_id = p::EdgeId::new(0);
        let sink = if target.is_some() {
            p::FragmentSink::Stream { edge: edge_id }
        } else {
            p::FragmentSink::Result
        };
        let fragment = builder
            .finish_definition(
                node,
                sink,
                dop_domain,
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .expect("checked fragment definition");
        let roots = p::PhysicalExpressionRoots::try_new(&fragment, &control).expect("roots");
        let bindings = roots
            .sites()
            .iter()
            .enumerate()
            .map(|(i, (site, _))| (*site, ExpressionUseId::new(i as u32)))
            .collect();
        let uses = roots
            .sites()
            .iter()
            .enumerate()
            .map(|(i, (_, root))| ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(i as u32),
                    domain: EvaluationDomainId::new(0),
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            })
            .collect();
        let flow = ExpressionControlFlow::try_new(
            vec![ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(0),
                parent: None,
                guard: None,
            }],
            uses,
            fragment.expressions(),
            CompilePhase::Validate,
            &control,
        )
        .expect("complete eager constant flow");
        let expression_uses = p::PhysicalRootUses::try_new(&fragment, flow, bindings, &control)
            .expect("exact root uses");
        let calls = p::FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &control)
            .expect("complete empty calls");
        let pruning =
            p::FrozenFragmentPruning::try_new(id, vec![], &control).expect("empty pruning");
        let mut edges = BTreeMap::new();
        let mut cuts = p::FragmentCuts::default();
        let result = match target {
            Some((target_fragment, target_node, delivery)) => {
                let delivery = match delivery {
                    novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven => {
                        p::DeliveryRequirement::DemandDriven
                    }
                    novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain => {
                        p::DeliveryRequirement::MustDrain
                    }
                };
                let target_fragment = p::FragmentId::new(target_fragment);
                let target_node =
                    p::NodeId::new(u32::try_from(target_node).expect("nonnegative target"));
                let partitioning = p::EdgePartitioning {
                    source: p::Distribution::Singleton,
                    source_multiplicity: p::RowMultiplicity::SingleCopy,
                    destination: p::Distribution::Singleton,
                    destination_multiplicity: p::RowMultiplicity::SingleCopy,
                };
                edges.insert(
                    edge_id,
                    p::Edge {
                        role: novarocks_physical_plan::OutputRole::Relational,
                        delivery,
                        id: edge_id,
                        kind: p::EdgeKind::Stream,
                        source: p::EdgeSource {
                            port: novarocks_physical_plan::FragmentOutputPort::Root,
                            fragment: id,
                            projection: values.clone().into_boxed_slice(),
                        },
                        destination: p::EdgeDestination {
                            fragment: target_fragment,
                            node: target_node,
                            receive_mapping: values.iter().map(|v| (*v, *v)).collect(),
                        },
                        partitioning: partitioning.clone(),
                    },
                );
                cuts.outbound = vec![p::OutboundFragmentCut {
                    edge: edge_id,
                    kind: p::EdgeKind::Stream,
                    destination_fragment: target_fragment,
                    destination_node: target_node,
                    projection: values
                        .iter()
                        .map(|v| p::CutValue {
                            value: *v,
                            ty: ty.clone(),
                        })
                        .collect(),
                    destination_imports: values
                        .iter()
                        .map(|v| p::CutImport {
                            source: p::CutValue {
                                value: *v,
                                ty: ty.clone(),
                            },
                            destination: *v,
                        })
                        .collect(),
                    partitioning,
                    source_port: novarocks_physical_plan::FragmentOutputPort::Root,
                    delivery,
                    payload: novarocks_physical_plan::ExchangePayloadContract::Relational,
                }]
                .into_boxed_slice();
                None
            }
            None => Some(p::ResultPort {
                scalar_schema: None,
                fragment: id,
                output: fragment.nodes()[&node].output.clone(),
                fields: values
                    .iter()
                    .enumerate()
                    .map(|(i, v)| p::ResultField {
                        domain: p::ResultValueDomain::Plain,
                        name: format!("payload{i}").into(),
                        alias: None,
                        value: *v,
                        ty: ty.clone(),
                    })
                    .collect(),
            }),
        };
        let package = p::FragmentPackage::try_new(
            p::FragmentPackageInput {
                constants,
                version: p::PlanVersionId::try_new(version).expect("nonzero version"),
                required: p::RequiredContracts::default(),
                fragment,
                expression_uses,
                calls,
                pruning,
                cuts,
                result,
                parameters: SemanticParameters::default(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
            &control,
        )
        .expect("checked package");
        FragmentArtifact::freeze_package(
            &package,
            &edges,
            &CompiledPackageCarrier::for_test(
                novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
                novarocks_plan_codec::physical_package_v2::PackageEncodeLimits::FROZEN,
            ),
            &control,
        )
        .expect("a genuine package freezes")
    }

    #[test]
    fn freezing_encodes_the_package_once_and_reads_its_facts_from_the_same_package() {
        let before = freezes_on_this_thread();
        let artifact = frozen_package(
            3,
            11,
            [7; 16],
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: false,
            },
            Some((
                5,
                20,
                novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            )),
            0,
        );
        assert_eq!(freezes_on_this_thread() - before, 1);
        let facts = artifact.facts();
        assert_eq!(facts.fragment_id(), 3);
        assert_eq!(facts.root_plan_node_id(), 11);
        assert_eq!(
            facts.sink_targets(),
            &[StaticSinkTarget::for_test(
                5,
                20,
                novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            )]
        );
        assert!(!facts.declares_table_writer());
        assert_eq!(facts.logical_summary_output(), None);
        assert!(facts.carries_runtime_filter_bindings());
        let frozen = wire::FrozenFragment::decode(artifact.content().bytes().clone())
            .expect("FrozenFragment");
        assert!(!frozen.package.is_empty());
        let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
        let model =
            novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel::try_new(
                &control,
            )
            .expect("decode model");
        let decoded = novarocks_plan_codec::physical_package_v2::decode_fragment_package(
            &frozen.package,
            &model,
            &novarocks_plan_codec::physical_package_v2::test_support::decode_limits(),
            &control,
        )
        .expect("checked package decodes");
        assert_eq!(decoded.version().as_bytes(), &[7; 16]);
        assert_eq!(decoded.fragment().id().get(), 3);
        assert_eq!(decoded.fragment().root().get(), 11);
    }
}
