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

//! Pure final-physical-plan to native-wire-v1 translation.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};

use novarocks_functions::{
    AggregateBindingSelection, EngineFunctionCatalog, FunctionArgument, FunctionBindingRequest,
    FunctionBindingSelection, FunctionLiteral, FunctionResultType, FunctionSemantics,
    ResolvedFunctionBinding,
};
use novarocks_physical_plan::{
    AggregateBinding, AggregatePhase, Distribution, Edge, EdgeId, EdgeKind, ExprId, ExprKind,
    Fragment, FragmentId, FragmentSink, FunctionArgumentType, JoinDistribution, JoinKind, JoinSide,
    LiteralValue, NodeId, NodeKind, PhysicalNode, PhysicalPlan, ProviderColumnReference,
    ProviderReadReference, ResultPort, RowCountAssertion, RowCountAssertionSpec, SetOperationKind,
    SortMode, TopNPhase, UnpivotConstant, ValueId, ValueOrigin, ValueType,
};
use novarocks_proto_codec::{FieldPath, arrow_physical};
use novarocks_proto_models::{common, expr, plan};
use novarocks_spi::connector::read_stack::ConnectorReadWorkSource;
use novarocks_spi::connector::read_stack::ConnectorValueType;
use prost::Message;
use sha2::{Digest, Sha256};

use crate::physical_expr::{
    MAX_WIRE_LAMBDA_PARAMETERS, NATIVE_V1_MAX_WIRE_NESTING, ValueResolution,
    WireExpressionPreflight, encode_exprs, encode_physical_expr, encode_sort_items,
    encode_window_frame, wire_function_name,
};
use crate::physical_type::{
    arrow_authoritative_wire_depths, encode_arrow_authoritative_compatibility_type,
    encode_physical_type, validate_arrow_authoritative_compatibility_type, validate_physical_type,
};
use crate::physical_v1::native_v1_node_wire_depths;
use crate::{WireLayout, WireSlotId, preflight_physical_plan_v1};

/// Exact, already-frozen v1 facts for one provider column.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalV1ScanColumn {
    pub column: ProviderColumnReference,
    pub name: Box<str>,
    pub ty: ValueType,
    pub connector_type: ConnectorValueType,
    pub internal: bool,
}

/// Wire-ready provider facts for one final scan.
///
/// `table` includes the provider-owned typed scan source. The surrounding
/// physical contract remains authoritative for output order, residuals,
/// relation identity, resource budget, and fragment topology.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalV1ScanFact {
    pub occurrence: novarocks_physical_plan::ProviderReadOccurrenceId,
    pub read: ProviderReadReference,
    pub selection_digest: [u8; 32],
    pub source_seal_digest: [u8; 32],
    pub database: Box<str>,
    pub alias: Option<Box<str>>,
    pub table: plan::TableDef,
    pub columns: Box<[PhysicalV1ScanColumn]>,
}

/// Seal one wire-ready scan source to its exact public final-plan identity.
pub fn physical_v1_scan_source_seal_digest(
    occurrence: novarocks_physical_plan::ProviderReadOccurrenceId,
    read: &ProviderReadReference,
    selection_digest: [u8; 32],
    source: &novarocks_proto_models::connector_read::ConnectorTableScanSource,
) -> Result<[u8; 32], String> {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.physical-v1.scan-source-seal");
    digest.update(1_u16.to_be_bytes());
    digest.update(occurrence.get().to_be_bytes());
    digest_part(
        &mut digest,
        read.binding.descriptor().provider_id.as_str().as_bytes(),
    )?;
    digest_part(
        &mut digest,
        read.binding.descriptor().instance_id.as_str().as_bytes(),
    )?;
    digest_part(
        &mut digest,
        read.binding
            .catalog_handle()
            .catalog_name()
            .as_str()
            .as_bytes(),
    )?;
    digest.update(read.binding.catalog_handle().version().as_bytes());
    digest_part(&mut digest, read.input_version.as_bytes())?;
    digest.update([encode_read_relation_kind(read.relation.kind())]);
    digest_part(
        &mut digest,
        &novarocks_proto_codec::connector_common::encode_connector_payload(read.relation.table()),
    )?;
    digest_part(
        &mut digest,
        &novarocks_proto_codec::connector_common::encode_connector_payload(read.relation.view()),
    )?;
    digest.update(selection_digest);
    digest_part(&mut digest, &source.encode_to_vec())?;
    Ok(digest.finalize().into())
}

fn digest_part(digest: &mut Sha256, value: &[u8]) -> Result<(), String> {
    let length = u64::try_from(value.len())
        .map_err(|_| "native wire v1 scan source seal component exceeds u64".to_string())?;
    digest.update(length.to_be_bytes());
    digest.update(value);
    Ok(())
}

const fn encode_read_relation_kind(
    kind: novarocks_spi::connector::read_stack::ConnectorReadRelationKind,
) -> u8 {
    use novarocks_spi::connector::read_stack::ConnectorReadRelationKind;
    match kind {
        ConnectorReadRelationKind::Table => 1,
        ConnectorReadRelationKind::TableFunction => 2,
        ConnectorReadRelationKind::ChangeWindow => 3,
        ConnectorReadRelationKind::SystemTable => 4,
        ConnectorReadRelationKind::TableExecute => 5,
        ConnectorReadRelationKind::MergeTable => 6,
    }
}

/// Exact public v1 envelope for one provider-owned final write target.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalV1WriteFact {
    pub handle: novarocks_proto_models::connector_write::ConnectorWriterHandle,
    /// What the provider calls each field this target accepts.
    ///
    /// The plan names them by the token the provider issued, because a name
    /// is the provider's and a plan restating it is a second place for it to
    /// be wrong. The provider matches its own frozen schema by name, so the
    /// name is put back here, from the very binding the token came from.
    pub field_names: BTreeMap<[u8; 32], Box<str>>,
}

pub trait PhysicalV1PrivateFacts {
    fn scan_fact(&self, fragment: FragmentId, node: NodeId) -> Option<&PhysicalV1ScanFact>;

    fn write_fact(
        &self,
        _target: novarocks_physical_plan::WriteTargetOrdinal,
    ) -> Option<&PhysicalV1WriteFact> {
        None
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoPhysicalV1PrivateFacts;

impl PhysicalV1PrivateFacts for NoPhysicalV1PrivateFacts {
    fn scan_fact(&self, _fragment: FragmentId, _node: NodeId) -> Option<&PhysicalV1ScanFact> {
        None
    }
}

/// Encode one complete immutable final plan without I/O or semantic repair.
pub fn encode_physical_plan_v1(
    physical: &PhysicalPlan,
    function_catalog: &EngineFunctionCatalog,
    private_facts: &impl PhysicalV1PrivateFacts,
) -> Result<plan::DistributedPlan, String> {
    // Recursive node, expression, and type trees are constructed only after
    // their complete nesting and expansion shape has passed preflight.
    preflight_physical_plan_v1(physical).map_err(|error| error.to_string())?;
    preflight_native_v1_wire_shape(physical)?;
    preflight_encoder(physical, function_catalog, private_facts)?;

    let layouts = physical
        .fragments()
        .iter()
        .map(|(id, fragment)| {
            WireLayout::try_new(fragment)
                .map(|layout| (*id, layout))
                .map_err(|error| error.to_string())
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    let result = physical
        .result_port()
        .ok_or_else(|| "native wire v1 requires one result port".to_string())?;
    let runtime_filters = encode_runtime_filters(physical, &layouts)?;
    // Derived once: every fragment's columns read the same names.
    let names = result_value_names(physical);
    let fragments = physical
        .fragments()
        .values()
        .map(|fragment| {
            encode_fragment(
                physical,
                fragment,
                &layouts[&fragment.id()],
                private_facts,
                &runtime_filters,
                &names,
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    let edges = physical
        .edges()
        .values()
        .map(|edge| encode_edge(physical, edge, &layouts))
        .collect::<Result<Vec<_>, String>>()?;
    Ok(plan::DistributedPlan {
        fragments,
        root_fragment_id: result.fragment.get(),
        edges,
    })
}

struct EncodedRuntimeFilters {
    tables: BTreeMap<FragmentId, plan::RuntimeFilterBindingTable>,
    node_bindings: BTreeMap<(FragmentId, NodeId), Vec<u32>>,
}

/// Which runtime filters each scan consumes, and under which wire binding id.
pub type ScanRuntimeFilterBindings = BTreeMap<(FragmentId, NodeId), Vec<(u32, ValueId)>>;

/// The runtime filters a scan must name in its provider-owned scan source.
///
/// The encoder derives these itself and checks what it is handed against them.
/// Exposing the same derivation is what keeps a producer of scan sources from
/// having to guess the binding identities: two derivations of one numbering
/// disagree the moment either changes, and the disagreement would surface as a
/// plan that cannot be encoded rather than as the numbering bug it is.
pub fn physical_v1_scan_runtime_filters(
    physical: &PhysicalPlan,
) -> Result<ScanRuntimeFilterBindings, String> {
    preflight_runtime_filters(physical)
}

/// One consumer of one completed plan's CTE, as submitting it reads it.
///
/// Which instances receive is placement's answer and is not here. Everything
/// else about a consumer is a property of the plan, and naming a column takes
/// the wire layout, which only this crate has -- so it is derived here rather
/// than guessed by the submitter.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalV1CteConsumer {
    pub cte_id: u32,
    pub target_fragment_id: u32,
    pub target_exchange_node_id: i32,
    pub output_partition: plan::DataPartition,
    pub output_slot_ids: Vec<i32>,
    pub receive_producer_column_ids: Vec<u32>,
}

/// Every CTE consumer of one completed plan, in edge order.
pub fn physical_v1_cte_consumers(
    physical: &PhysicalPlan,
) -> Result<Vec<PhysicalV1CteConsumer>, String> {
    let mut consumers = Vec::new();
    for edge in physical.edges().values() {
        if edge.kind != EdgeKind::CteMulticast {
            continue;
        }
        let fragment = physical
            .fragments()
            .get(&edge.source.fragment)
            .ok_or_else(|| {
                format!(
                    "cte multicast edge {} names absent source fragment {}",
                    edge.id.get(),
                    edge.source.fragment.get()
                )
            })?;
        let layout = WireLayout::try_new(fragment).map_err(|error| error.to_string())?;
        let output_slot_ids = layout
            .project_output(fragment, fragment.root(), &edge.source.projection)
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(WireSlotId::get)
            .collect::<Vec<_>>();
        consumers.push(PhysicalV1CteConsumer {
            cte_id: edge.source.fragment.get(),
            target_fragment_id: edge.destination.fragment.get(),
            target_exchange_node_id: i32::try_from(edge.destination.node.get())
                .map_err(|_| "cte multicast destination node exceeds i32".to_string())?,
            output_partition: encode_data_partition(
                fragment,
                &layout,
                &fragment.nodes()[&fragment.root()],
                &edge.partitioning.source,
                true,
            )?,
            receive_producer_column_ids: output_slot_ids_u32(&output_slot_ids)?,
            output_slot_ids,
        });
    }
    Ok(consumers)
}

/// Which runtime-filter role one wire binding identity names.
///
/// The index is into that filter's own `producers` or `consumers`, so a
/// binding identity resolves back to the exact endpoint it was minted for
/// without a second lookup key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalV1RuntimeFilterBindingRole {
    Producer(usize),
    Consumer(usize),
}

/// One wire runtime-filter binding identity, and what it names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalV1RuntimeFilterBinding {
    pub binding_id: u32,
    pub filter: novarocks_physical_plan::RuntimeFilterId,
    pub fragment: FragmentId,
    pub node: NodeId,
    pub role: PhysicalV1RuntimeFilterBindingRole,
}

/// Number every runtime-filter binding of one plan, once.
///
/// The numbering is a property of the plan: fragments in id order, each
/// fragment's attached filters in its own order, producers before consumers.
/// Everything that needs a binding identity -- the encoder, the scan sources,
/// and the facts an attempt deploys from -- reads this one derivation, because
/// two derivations of one numbering disagree the moment either changes, and
/// the disagreement would surface as a plan that cannot be encoded rather than
/// as the numbering bug it is.
pub fn physical_v1_runtime_filter_bindings(
    physical: &PhysicalPlan,
) -> Result<Vec<PhysicalV1RuntimeFilterBinding>, String> {
    let mut bindings = Vec::new();
    let mut next_binding = 1_u32;
    let mut mint = |filter: novarocks_physical_plan::RuntimeFilterId,
                    fragment: FragmentId,
                    node: NodeId,
                    role: PhysicalV1RuntimeFilterBindingRole|
     -> Result<(), String> {
        let binding_id = next_binding;
        next_binding = next_binding.checked_add(1).ok_or_else(|| {
            "native wire v1 runtime-filter binding identity space exhausted".to_string()
        })?;
        bindings.push(PhysicalV1RuntimeFilterBinding {
            binding_id,
            filter,
            fragment,
            node,
            role,
        });
        Ok(())
    };
    for fragment in physical.fragments().values() {
        for filter_id in fragment.runtime_filters() {
            let filter = physical.runtime_filters().get(filter_id).ok_or_else(|| {
                format!(
                    "fragment references absent runtime filter {}",
                    filter_id.get()
                )
            })?;
            for (index, producer) in filter.producers.iter().enumerate() {
                if producer.endpoint.fragment == fragment.id() {
                    mint(
                        filter.id,
                        fragment.id(),
                        producer.endpoint.node,
                        PhysicalV1RuntimeFilterBindingRole::Producer(index),
                    )?;
                }
            }
            for (index, consumer) in filter.consumers.iter().enumerate() {
                if consumer.endpoint.fragment == fragment.id() {
                    mint(
                        filter.id,
                        fragment.id(),
                        consumer.endpoint.node,
                        PhysicalV1RuntimeFilterBindingRole::Consumer(index),
                    )?;
                }
            }
        }
    }
    Ok(bindings)
}

fn encode_runtime_filters(
    physical: &PhysicalPlan,
    layouts: &BTreeMap<FragmentId, WireLayout>,
) -> Result<EncodedRuntimeFilters, String> {
    let mut tables = physical
        .fragments()
        .keys()
        .map(|fragment_id| {
            (
                *fragment_id,
                plan::RuntimeFilterBindingTable {
                    fragment_id: fragment_id.get(),
                    bindings: Vec::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut node_bindings = BTreeMap::<(FragmentId, NodeId), Vec<u32>>::new();
    for binding in physical_v1_runtime_filter_bindings(physical)? {
        let fragment = &physical.fragments()[&binding.fragment];
        let filter = &physical.runtime_filters()[&binding.filter];
        let encoded = match binding.role {
            PhysicalV1RuntimeFilterBindingRole::Producer(index) => encode_runtime_filter_producer(
                fragment,
                &layouts[&binding.fragment],
                filter,
                &filter.producers[index],
                binding.binding_id,
            )?,
            PhysicalV1RuntimeFilterBindingRole::Consumer(index) => encode_runtime_filter_consumer(
                fragment,
                &layouts[&binding.fragment],
                filter,
                &filter.consumers[index],
                binding.binding_id,
            )?,
        };
        tables
            .get_mut(&binding.fragment)
            .expect("every plan fragment has a binding table")
            .bindings
            .push(encoded);
        node_bindings
            .entry((binding.fragment, binding.node))
            .or_default()
            .push(binding.binding_id);
    }
    Ok(EncodedRuntimeFilters {
        tables,
        node_bindings,
    })
}

fn encode_runtime_filter_producer(
    fragment: &Fragment,
    layout: &WireLayout,
    filter: &novarocks_physical_plan::RuntimeFilter,
    producer: &novarocks_physical_plan::RuntimeFilterProducer,
    binding_id: u32,
) -> Result<plan::RuntimeFilterBinding, String> {
    use novarocks_physical_plan::{
        RuntimeFilterCompletion, RuntimeFilterContributionKind, RuntimeFilterProducerTarget,
    };
    let producer_scope = match producer.apply_point {
        novarocks_physical_plan::RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
            novarocks_physical_plan::RuntimeFilterApplyPoint::NodeInput { input_ordinal }
        }
        novarocks_physical_plan::RuntimeFilterApplyPoint::NodeOutput
        | novarocks_physical_plan::RuntimeFilterApplyPoint::ScanSource => {
            return Err("runtime filter producer has no final node-input build scope".into());
        }
    };
    let expression =
        encode_runtime_filter_endpoint(fragment, layout, &producer.endpoint, producer_scope)?;
    let target = match producer.target {
        RuntimeFilterProducerTarget::JoinBuildKey { equality } => {
            let witness = filter
                .equality_witnesses
                .iter()
                .find(|witness| witness.id == equality)
                .ok_or_else(|| "runtime filter equality witness disappeared".to_string())?;
            plan::runtime_filter_producer_role::Target::JoinBuildKey(
                plan::RuntimeFilterJoinBuildKey {
                    ordinal: witness.key_ordinal,
                },
            )
        }
        RuntimeFilterProducerTarget::AggregateTopNKey {
            group_key_ordinal,
            limit,
            ..
        } => plan::runtime_filter_producer_role::Target::AggregateTopnKey(
            plan::RuntimeFilterAggregateTopNKey {
                group_key_ordinal,
                limit: u32::try_from(limit).map_err(|_| {
                    "native wire v1 Aggregate TopN runtime-filter limit exceeds u32".to_string()
                })?,
            },
        ),
    };
    Ok(plan::RuntimeFilterBinding {
        binding_id,
        channel_id: filter.id.get(),
        node_id: i32::try_from(producer.endpoint.node.get())
            .map_err(|_| "runtime filter producer node exceeds i32".to_string())?,
        // The expression reads the final build child, while the producer runs
        // after this node has consumed that input and produced its contribution.
        apply_point: plan::RuntimeFilterApplyPoint::NodeOutput as i32,
        expression: Some(expression),
        contract: Some(encode_runtime_filter_contract(filter)?),
        reduction: Some(encode_runtime_filter_reduction(filter)?),
        role: Some(plan::runtime_filter_binding::Role::Producer(
            plan::RuntimeFilterProducerRole {
                contribution_kinds: producer
                    .contribution_kinds
                    .iter()
                    .map(|kind| match kind {
                        RuntimeFilterContributionKind::ValueDomainDelta => {
                            plan::RuntimeFilterContributionKind::ValueDomainDelta as i32
                        }
                        RuntimeFilterContributionKind::FinalDomainShard => {
                            plan::RuntimeFilterContributionKind::FinalDomainShard as i32
                        }
                        RuntimeFilterContributionKind::OrderedBoundUpdate => {
                            plan::RuntimeFilterContributionKind::OrderedBoundUpdate as i32
                        }
                        RuntimeFilterContributionKind::FinalOrderedHullShard => {
                            plan::RuntimeFilterContributionKind::TopkSummary as i32
                        }
                        RuntimeFilterContributionKind::ProducerClosed => {
                            plan::RuntimeFilterContributionKind::ProducerClosed as i32
                        }
                    })
                    .collect(),
                completion_requirement: match producer.completion {
                    RuntimeFilterCompletion::ProducerClosed => {
                        plan::RuntimeFilterCompletionRequirement::ProducerClosed as i32
                    }
                    RuntimeFilterCompletion::FencedCommittedDomain => {
                        plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen as i32
                    }
                },
                target: Some(target),
            },
        )),
    })
}

fn encode_runtime_filter_consumer(
    fragment: &Fragment,
    layout: &WireLayout,
    filter: &novarocks_physical_plan::RuntimeFilter,
    consumer: &novarocks_physical_plan::RuntimeFilterConsumer,
    binding_id: u32,
) -> Result<plan::RuntimeFilterBinding, String> {
    use novarocks_physical_plan::{
        RuntimeFilterArtifactCapability, RuntimeFilterConsumerActivation,
        RuntimeFilterConsumerTarget,
    };
    let expression =
        encode_runtime_filter_endpoint(fragment, layout, &consumer.endpoint, consumer.apply_point)?;
    let target = match &consumer.target {
        RuntimeFilterConsumerTarget::JoinProbeKey { .. } => {
            let novarocks_physical_plan::RuntimeFilterApplyPoint::NodeInput { input_ordinal } =
                consumer.apply_point
            else {
                return Err("join-probe runtime filter consumer is not a node input".into());
            };
            plan::runtime_filter_consumer_role::Target::DirectInputOrdinal(input_ordinal)
        }
        RuntimeFilterConsumerTarget::ScanField { .. }
        | RuntimeFilterConsumerTarget::AggregateTopNScanField { .. } => {
            plan::runtime_filter_consumer_role::Target::SourceBoundaryTarget(
                plan::RuntimeFilterSourceBoundaryTarget {},
            )
        }
    };
    Ok(plan::RuntimeFilterBinding {
        binding_id,
        channel_id: filter.id.get(),
        node_id: i32::try_from(consumer.endpoint.node.get())
            .map_err(|_| "runtime filter consumer node exceeds i32".to_string())?,
        // Both direct-child and scan-source consumers are installed at an
        // input boundary by the v1 decoder.
        apply_point: plan::RuntimeFilterApplyPoint::NodeInput as i32,
        expression: Some(expression),
        contract: Some(encode_runtime_filter_contract(filter)?),
        reduction: Some(encode_runtime_filter_reduction(filter)?),
        role: Some(plan::runtime_filter_binding::Role::Consumer(
            plan::RuntimeFilterConsumerRole {
                capabilities: consumer
                    .capabilities
                    .iter()
                    .map(|capability| match capability {
                        RuntimeFilterArtifactCapability::Membership => {
                            plan::RuntimeFilterArtifactCapability::Membership as i32
                        }
                        RuntimeFilterArtifactCapability::OrderedRange => {
                            plan::RuntimeFilterArtifactCapability::OrderedRange as i32
                        }
                        RuntimeFilterArtifactCapability::EmptyDomain => {
                            plan::RuntimeFilterArtifactCapability::EmptyDomain as i32
                        }
                    })
                    .collect(),
                activation: Some(plan::RuntimeFilterConsumerActivation {
                    kind: Some(match consumer.activation {
                        RuntimeFilterConsumerActivation::BlockingSnapshot => {
                            plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true)
                        }
                        RuntimeFilterConsumerActivation::NonBlockingLive { late_apply } => {
                            plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(
                                encode_late_apply(late_apply),
                            )
                        }
                        // A filter that is published once has exactly one
                        // update, and it is the complete one. So installing
                        // "every update at the boundary" and installing "the
                        // complete snapshot at the boundary" are the same
                        // instruction, and the wire's own kind says it. A
                        // filter that publishes monotonic updates is not the
                        // same, and preflight refuses it.
                        RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
                            late_apply,
                        } => plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(
                            encode_late_apply(late_apply),
                        ),
                    }),
                }),
                target: Some(target),
            },
        )),
    })
}

const fn encode_late_apply(granularity: novarocks_physical_plan::LateApplyGranularity) -> i32 {
    use novarocks_physical_plan::LateApplyGranularity;
    match granularity {
        LateApplyGranularity::Row => plan::RuntimeFilterLateApplyGranularity::Row as i32,
        LateApplyGranularity::Batch => plan::RuntimeFilterLateApplyGranularity::Batch as i32,
        LateApplyGranularity::RowGroup => plan::RuntimeFilterLateApplyGranularity::RowGroup as i32,
        LateApplyGranularity::Split => plan::RuntimeFilterLateApplyGranularity::Split as i32,
        LateApplyGranularity::File => plan::RuntimeFilterLateApplyGranularity::File as i32,
    }
}

fn encode_runtime_filter_endpoint(
    fragment: &Fragment,
    layout: &WireLayout,
    endpoint: &novarocks_physical_plan::RuntimeFilterEndpoint,
    apply_point: novarocks_physical_plan::RuntimeFilterApplyPoint,
) -> Result<expr::Expr, String> {
    let value = only_endpoint_value(endpoint)?;
    let slot = match apply_point {
        novarocks_physical_plan::RuntimeFilterApplyPoint::NodeInput { input_ordinal } => layout
            .input_value_slot_at(endpoint.node, input_ordinal, value)
            .map_err(|error| error.to_string())?,
        novarocks_physical_plan::RuntimeFilterApplyPoint::NodeOutput
        | novarocks_physical_plan::RuntimeFilterApplyPoint::ScanSource => {
            output_slot_for_value(layout, &fragment.nodes()[&endpoint.node], value)?
        }
    };
    value_expr(fragment, value, slot)
}

fn only_endpoint_value(
    endpoint: &novarocks_physical_plan::RuntimeFilterEndpoint,
) -> Result<ValueId, String> {
    let [value] = endpoint.values.as_ref() else {
        return Err(format!(
            "native wire v1 runtime filter endpoint at fragment {} node {} requires exactly one value",
            endpoint.fragment.get(),
            endpoint.node.get()
        ));
    };
    Ok(*value)
}

fn encode_runtime_filter_contract(
    filter: &novarocks_physical_plan::RuntimeFilter,
) -> Result<plan::RuntimeFilterContract, String> {
    use novarocks_physical_plan::{RuntimeFilterDomain, RuntimeFilterNullSemantics};
    use plan::runtime_filter_contract::Kind;
    let kind = match &filter.domain {
        RuntimeFilterDomain::Membership { null_semantics, .. } => {
            Kind::Membership(plan::RuntimeFilterMembershipContract {
                null_semantics: match null_semantics {
                    RuntimeFilterNullSemantics::NeverMatches => {
                        plan::RuntimeFilterMembershipNullSemantics::NeverMatches as i32
                    }
                    RuntimeFilterNullSemantics::NullSafeEqual => {
                        plan::RuntimeFilterMembershipNullSemantics::NullSafeEqual as i32
                    }
                },
            })
        }
        RuntimeFilterDomain::Ordered {
            key,
            inclusive,
            comparator,
        } => {
            if !inclusive {
                return Err("native wire v1 cannot encode exclusive runtime-filter bounds".into());
            }
            let encoded_type = encode_physical_type(&key.ty.data_type)?;
            if comparator.stable_name() != "novarocks.native-scalar-order.v1" {
                return Err("native wire v1 runtime-filter comparator is unsupported".into());
            }
            let (comparator_digest, order_contract_digest) =
                runtime_filter_order_digests(&key.ty.data_type, key.direction, key.null_ordering)?;
            Kind::Ordered(plan::RuntimeFilterOrderedContract {
                keys: vec![plan::RuntimeFilterOrderKey {
                    r#type: Some(encoded_type),
                    direction: match key.direction {
                        novarocks_physical_plan::SortDirection::Ascending => {
                            plan::RuntimeFilterSortDirection::Ascending as i32
                        }
                        novarocks_physical_plan::SortDirection::Descending => {
                            plan::RuntimeFilterSortDirection::Descending as i32
                        }
                    },
                    null_order: match key.null_ordering {
                        novarocks_physical_plan::NullOrdering::First => {
                            plan::RuntimeFilterNullOrder::First as i32
                        }
                        novarocks_physical_plan::NullOrdering::Last => {
                            plan::RuntimeFilterNullOrder::Last as i32
                        }
                    },
                }],
                comparator_digest: comparator_digest.to_vec(),
                order_contract_digest: order_contract_digest.to_vec(),
            })
        }
    };
    Ok(plan::RuntimeFilterContract { kind: Some(kind) })
}

fn encode_runtime_filter_reduction(
    filter: &novarocks_physical_plan::RuntimeFilter,
) -> Result<plan::RuntimeFilterReductionContract, String> {
    use novarocks_physical_plan::{RuntimeFilterProducerTarget, RuntimeFilterReduction};
    use plan::runtime_filter_reduction_contract::Kind;
    let kind = match filter.reduction {
        RuntimeFilterReduction::SetUnion => Kind::SetUnion(true),
        RuntimeFilterReduction::TightenOrderedBound => Kind::TightenOrderedBound(true),
        RuntimeFilterReduction::UnionOrderedHull => {
            let limit = filter
                .producers
                .iter()
                .find_map(|producer| match producer.target {
                    RuntimeFilterProducerTarget::AggregateTopNKey { limit, .. } => Some(limit),
                    RuntimeFilterProducerTarget::JoinBuildKey { .. } => None,
                })
                .ok_or_else(|| {
                    "ordered-hull reduction has no Aggregate TopN producer".to_string()
                })?;
            let limit = u32::try_from(limit)
                .map_err(|_| "native wire v1 TopK runtime-filter limit exceeds u32".to_string())?;
            let contract = encode_runtime_filter_contract(filter)?;
            let Some(plan::runtime_filter_contract::Kind::Ordered(ordered)) = contract.kind else {
                return Err("TopK reduction requires an ordered runtime-filter contract".into());
            };
            let digest = digest_parts(&[
                b"novarocks.runtime-filter.top-k-summary-contract",
                &1_u16.to_be_bytes(),
                &ordered.order_contract_digest,
                &limit.to_be_bytes(),
            ]);
            Kind::MergeTopkSummary(plan::RuntimeFilterTopKReduction {
                k: limit,
                contract_digest: digest.to_vec(),
            })
        }
    };
    Ok(plan::RuntimeFilterReductionContract { kind: Some(kind) })
}

fn digest_parts(parts: &[&[u8]]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update(part);
    }
    digest.finalize().into()
}

/// The comparator this plan's ordered runtime filter is compared under.
///
/// Both the wire contract and the facts an attempt deploys from name the
/// comparator by digest, and they must name the same one. Deriving it once
/// here is what makes that true by construction rather than by review.
pub fn physical_v1_runtime_filter_comparator_digest(
    filter: &novarocks_physical_plan::RuntimeFilter,
) -> Result<Option<[u8; 32]>, String> {
    let novarocks_physical_plan::RuntimeFilterDomain::Ordered { key, .. } = &filter.domain else {
        return Ok(None);
    };
    let (comparator_digest, _) =
        runtime_filter_order_digests(&key.ty.data_type, key.direction, key.null_ordering)?;
    Ok(Some(comparator_digest))
}

fn runtime_filter_order_digests(
    data_type: &arrow::datatypes::DataType,
    direction: novarocks_physical_plan::SortDirection,
    null_ordering: novarocks_physical_plan::NullOrdering,
) -> Result<([u8; 32], [u8; 32]), String> {
    let mut canonical_keys = Vec::with_capacity(64);
    canonical_keys.extend_from_slice(&1_u32.to_be_bytes());
    encode_runtime_filter_key_type(data_type, &mut canonical_keys)?;
    canonical_keys.push(match direction {
        novarocks_physical_plan::SortDirection::Ascending => 1,
        novarocks_physical_plan::SortDirection::Descending => 2,
    });
    canonical_keys.push(match null_ordering {
        novarocks_physical_plan::NullOrdering::First => 1,
        novarocks_physical_plan::NullOrdering::Last => 2,
    });
    let comparator_digest = digest_parts(&[
        b"novarocks.runtime-filter.comparator",
        &1_u16.to_be_bytes(),
        &canonical_keys,
    ]);
    let order_contract_digest = digest_parts(&[
        b"novarocks.runtime-filter.order-contract",
        &1_u16.to_be_bytes(),
        &canonical_keys,
        &[1],
        &comparator_digest,
        &1_u16.to_be_bytes(),
    ]);
    Ok((comparator_digest, order_contract_digest))
}

fn encode_runtime_filter_key_type(
    data_type: &arrow::datatypes::DataType,
    output: &mut Vec<u8>,
) -> Result<(), String> {
    use arrow::datatypes::{DataType, TimeUnit};
    match data_type {
        DataType::Boolean => output.push(1),
        DataType::Int8 => output.push(2),
        DataType::Int16 => output.push(3),
        DataType::Int32 => output.push(4),
        DataType::Int64 => output.push(5),
        DataType::FixedSizeBinary(16) => output.push(6),
        DataType::Utf8 => output.push(9),
        DataType::Date32 => output.push(10),
        DataType::Timestamp(unit, timezone) => {
            output.extend_from_slice(&[
                11,
                match unit {
                    TimeUnit::Second => 1,
                    TimeUnit::Millisecond => 2,
                    TimeUnit::Microsecond => 3,
                    TimeUnit::Nanosecond => 4,
                },
            ]);
            if let Some(timezone) = timezone {
                output.push(1);
                let length = u32::try_from(timezone.len())
                    .map_err(|_| "runtime-filter timezone length exceeds u32".to_string())?;
                output.extend_from_slice(&length.to_be_bytes());
                output.extend_from_slice(timezone.as_bytes());
            } else {
                output.push(0);
            }
        }
        DataType::Decimal128(precision, scale)
            if *precision != 0
                && *precision <= 38
                && *scale <= 38
                && (*scale <= 0 || (*scale as u8) <= *precision) =>
        {
            output.extend_from_slice(&[12, *precision, *scale as u8]);
        }
        _ => return Err("runtime-filter ordered key type is unsupported".into()),
    }
    Ok(())
}

const NATIVE_V1_PLAN_FRAGMENT_PREFIX_DEPTH: usize = 2;
// Covers operator/item carriers and the fixed WindowFrame/WindowBound branch.
const NATIVE_V1_EXPRESSION_WRAPPER_DEPTH: usize = 6;
const NATIVE_V1_WRITER_ARROW_WRAPPER_DEPTH: usize = 6;
const NATIVE_V1_WRITER_SQL_WRAPPER_DEPTH: usize = 4;

/// Validate the complete recursive wire path before any protobuf tree exists.
///
/// Node depth alone is insufficient: expressions and writer schemas add their
/// own recursive messages below a node. These conservative fixed prefixes cover
/// `DistributedPlan`, `PlanFragment`, the node payload, and the carrier-specific
/// wrappers above the recursive expression or type.
fn preflight_native_v1_wire_shape(physical: &PhysicalPlan) -> Result<(), String> {
    for fragment in physical.fragments().values() {
        let node_depths =
            native_v1_node_wire_depths(fragment).map_err(|error| error.to_string())?;
        let mut expressions = WireExpressionPreflight::try_new(fragment)?;
        for node in fragment.nodes().values() {
            let node_depth = node_depths.get(&node.id).copied().ok_or_else(|| {
                format!(
                    "fragment {} node {} is unreachable from the wire root",
                    fragment.id().get(),
                    node.id.get()
                )
            })?;
            let expression_prefix = NATIVE_V1_PLAN_FRAGMENT_PREFIX_DEPTH
                .saturating_add(node_depth)
                .saturating_add(NATIVE_V1_EXPRESSION_WRAPPER_DEPTH);
            charge_node_expressions(fragment, node, expression_prefix, &mut expressions)?;

            match &node.kind {
                NodeKind::TableWriter { target } => {
                    validate_writer_wire_depth(fragment, node, node_depth, &target.output_schema)?
                }
                NodeKind::TableFinish(spec) => {
                    validate_writer_wire_depth(fragment, node, node_depth, &spec.input_schema)?;
                    validate_writer_wire_depth(fragment, node, node_depth, &spec.output_schema)?;
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn charge_node_expressions(
    fragment: &Fragment,
    node: &PhysicalNode,
    enclosing_depth: usize,
    preflight: &mut WireExpressionPreflight<'_>,
) -> Result<(), String> {
    let mut charge = |expression| preflight.charge(expression, enclosing_depth);
    match &node.kind {
        NodeKind::Scan { residuals, .. } => {
            for expression in residuals {
                charge(*expression)?;
            }
        }
        NodeKind::Filter { predicates } => {
            for predicate in predicates {
                charge(*predicate)?;
            }
        }
        NodeKind::Project { expressions } => {
            for (expression, _) in expressions {
                charge(*expression)?;
            }
        }
        NodeKind::Aggregate {
            group_by, calls, ..
        } => {
            for (expression, _) in group_by {
                charge(*expression)?;
            }
            for call in calls {
                for expression in &call.arguments {
                    charge(*expression)?;
                }
                for item in &call.order_by {
                    charge(item.expr)?;
                }
            }
        }
        NodeKind::HashJoin { keys, residual, .. } => {
            for key in keys {
                charge(key.left)?;
                charge(key.right)?;
            }
            if let Some(residual) = residual {
                charge(*residual)?;
            }
        }
        NodeKind::NestLoopJoin { predicate, .. } => {
            if let Some(predicate) = predicate {
                charge(*predicate)?;
            }
        }
        NodeKind::Sort { order_by, mode } => {
            for item in order_by {
                charge(item.expr)?;
            }
            match mode {
                SortMode::Global => {}
                SortMode::Analytic { partition_by }
                | SortMode::PartitionTopN { partition_by, .. } => {
                    for item in partition_by {
                        charge(item.expr)?;
                    }
                }
            }
        }
        NodeKind::TopN { order_by, .. } => {
            for item in order_by {
                charge(item.expr)?;
            }
        }
        NodeKind::Window(spec) => {
            for window in &spec.expressions {
                let expression =
                    fragment
                        .expressions()
                        .get(window.expression)
                        .ok_or_else(|| {
                            format!("window expression {} is absent", window.expression.get())
                        })?;
                let ExprKind::WindowCall {
                    args,
                    function_order_by,
                    ..
                } = &expression.kind
                else {
                    return Err(format!(
                        "Window node {} expression {} is not a WindowCall",
                        node.id.get(),
                        window.expression.get()
                    ));
                };
                for expression in args {
                    charge(*expression)?;
                }
                for item in function_order_by {
                    charge(item.expr)?;
                }
                for item in &spec.partition_by {
                    charge(item.expr)?;
                }
                for item in &spec.order_by {
                    charge(item.expr)?;
                }
            }
        }
        NodeKind::Values { rows } => {
            for row in rows {
                for expression in row {
                    charge(*expression)?;
                }
            }
        }
        NodeKind::Unpivot { spec } => {
            for mapping in &spec.mappings {
                for constant in &mapping.constants {
                    if let UnpivotConstant::Scalar(expression) = constant {
                        charge(*expression)?;
                    }
                }
            }
        }
        NodeKind::TableFunction { arguments, .. } => {
            for expression in arguments {
                charge(*expression)?;
            }
        }
        NodeKind::ChangeEventExpand { events, .. } => {
            for event in events {
                if let Some(predicate) = event.predicate {
                    charge(predicate)?;
                }
                for (_, expression) in &event.assignments {
                    if let Some(expression) = expression {
                        charge(*expression)?;
                    }
                }
            }
        }
        NodeKind::TableFinish(spec) => {
            if let Some(unpivot) = &spec.grouped_unpivot {
                for mapping in &unpivot.mappings {
                    for constant in &mapping.constants {
                        if let UnpivotConstant::Scalar(expression) = constant {
                            charge(*expression)?;
                        }
                    }
                }
            }
        }
        NodeKind::Limit { .. }
        | NodeKind::SetOp { .. }
        | NodeKind::Repeat { .. }
        | NodeKind::GenerateSeries { .. }
        | NodeKind::AssertOneRow(_)
        | NodeKind::ExchangeSource { .. }
        | NodeKind::TableWriter { .. } => {}
    }
    Ok(())
}

fn validate_writer_wire_depth(
    fragment: &Fragment,
    node: &PhysicalNode,
    node_depth: usize,
    schema: &novarocks_physical_plan::WriterRelationSchema,
) -> Result<(), String> {
    for field in &schema.fields {
        let (sql_depth, arrow_depth) = arrow_authoritative_wire_depths(&field.ty.data_type)?;
        let sql_wire_depth = NATIVE_V1_PLAN_FRAGMENT_PREFIX_DEPTH
            .saturating_add(node_depth)
            .saturating_add(NATIVE_V1_WRITER_SQL_WRAPPER_DEPTH)
            .saturating_add(sql_depth);
        let arrow_wire_depth = NATIVE_V1_PLAN_FRAGMENT_PREFIX_DEPTH
            .saturating_add(node_depth)
            .saturating_add(NATIVE_V1_WRITER_ARROW_WRAPPER_DEPTH)
            .saturating_add(arrow_depth);
        let wire_depth = sql_wire_depth.max(arrow_wire_depth);
        if wire_depth > NATIVE_V1_MAX_WIRE_NESTING {
            return Err(format!(
                "native wire v1 writer field `{}` at fragment {} node {} reaches message depth {wire_depth}, exceeding decoder-safe maximum {}",
                field.name,
                fragment.id().get(),
                node.id.get(),
                NATIVE_V1_MAX_WIRE_NESTING
            ));
        }
    }
    Ok(())
}

fn preflight_encoder(
    physical: &PhysicalPlan,
    function_catalog: &EngineFunctionCatalog,
    private_facts: &impl PhysicalV1PrivateFacts,
) -> Result<(), String> {
    let Some(result) = physical.result_port() else {
        return Err("native wire v1 requires one result port".into());
    };
    let result_fragment = &physical.fragments()[&result.fragment];
    for field in &result.fields {
        let writer_derived = result_fragment
            .values()
            .get(&field.value)
            .is_some_and(|value| matches!(value.origin, ValueOrigin::WriterDerived { .. }));
        if !writer_derived {
            validate_physical_type(&field.ty.data_type)
                .map_err(|reason| format!("result field `{}`: {reason}", field.name))?;
        }
    }
    let scan_dynamic_filters = preflight_runtime_filters(physical)?;
    for fragment in physical.fragments().values() {
        let writer_count = fragment
            .nodes()
            .values()
            .filter(|node| matches!(node.kind, NodeKind::TableWriter { .. }))
            .count();
        if writer_count > 1 {
            return Err(format!(
                "native wire v1 fragment {} cannot assign exact writer ordinals to {writer_count} writers",
                fragment.id().get()
            ));
        }
        for value in fragment.values().values() {
            if !matches!(value.origin, ValueOrigin::WriterDerived { .. }) {
                validate_physical_type(&value.ty.data_type).map_err(|reason| {
                    format!(
                        "fragment {} value {}: {reason}",
                        fragment.id().get(),
                        value.id.get()
                    )
                })?;
            }
        }
        for (id, expression) in fragment.expressions().iter() {
            let subject = |reason: String| {
                format!(
                    "fragment {} expression {}: {reason}",
                    fragment.id().get(),
                    id.get()
                )
            };
            validate_physical_type(&expression.ty.data_type).map_err(subject)?;
            match &expression.kind {
                ExprKind::Cast { target, .. } => {
                    validate_physical_type(target).map_err(|reason| {
                        format!(
                            "fragment {} expression {} cast target: {reason}",
                            fragment.id().get(),
                            id.get()
                        )
                    })?
                }
                ExprKind::FunctionCall { function, args } => {
                    validate_scalar_binding(function_catalog, fragment, function, args)?
                }
                ExprKind::WindowCall {
                    function,
                    args,
                    function_order_by,
                    aggregate_binding,
                    ..
                } => {
                    if let Some(binding) = aggregate_binding {
                        let arguments = args
                            .iter()
                            .copied()
                            .chain(function_order_by.iter().map(|item| item.expr))
                            .collect::<Vec<_>>();
                        validate_aggregate_binding(
                            function_catalog,
                            fragment,
                            binding,
                            &arguments,
                        )?;
                    } else {
                        validate_scalar_binding(function_catalog, fragment, function, args)?;
                    }
                }
                ExprKind::Lambda {
                    parameter_types, ..
                } => {
                    // A lambda's parameters are addressed in the reserved
                    // slot range, which is wide but not unbounded.
                    if parameter_types.len() > MAX_WIRE_LAMBDA_PARAMETERS {
                        return Err(format!(
                            "fragment {} node {} lambda declares {} parameters; native wire v1 addresses at most {MAX_WIRE_LAMBDA_PARAMETERS}",
                            fragment.id().get(),
                            expression.owner.get(),
                            parameter_types.len()
                        ));
                    }
                }
                _ => {}
            }
        }
        for node in fragment.nodes().values() {
            match &node.kind {
                NodeKind::Scan {
                    occurrence,
                    relation,
                    read_budget,
                    provider_outputs,
                    derived_values,
                    ..
                } => {
                    let fact =
                        private_facts
                            .scan_fact(fragment.id(), node.id)
                            .ok_or_else(|| {
                                format!(
                                    "native wire v1 scan fact missing for fragment {} node {}",
                                    fragment.id().get(),
                                    node.id.get()
                                )
                            })?;
                    if fact.occurrence != *occurrence {
                        return Err(format!(
                            "native wire v1 scan occurrence mismatch at fragment {} node {}",
                            fragment.id().get(),
                            node.id.get()
                        ));
                    }
                    if &fact.read != relation.read() {
                        return Err(format!(
                            "native wire v1 scan fact relation mismatch at fragment {} node {}",
                            fragment.id().get(),
                            node.id.get()
                        ));
                    }
                    let typed = typed_scan_source(&fact.table).ok_or_else(|| {
                        format!(
                            "native wire v1 scan fact has no typed source at fragment {} node {}",
                            fragment.id().get(),
                            node.id.get()
                        )
                    })?;
                    if typed.max_batch_rows != read_budget.max_batch_rows
                        || typed.max_batch_bytes != read_budget.max_batch_bytes
                    {
                        return Err(format!(
                            "native wire v1 scan fact read budget mismatch at fragment {} node {}",
                            fragment.id().get(),
                            node.id.get()
                        ));
                    }
                    let expected_work_source = match relation.work_source() {
                        ConnectorReadWorkSource::RuntimeSplits => {
                            novarocks_proto_models::connector_read::ScanWorkSource::RuntimeSplits
                                as i32
                        }
                        ConnectorReadWorkSource::WholeRelation => {
                            novarocks_proto_models::connector_read::ScanWorkSource::WholeRelation
                                as i32
                        }
                    };
                    if typed.work_source != expected_work_source {
                        return Err(format!(
                            "native wire v1 scan fact work source mismatch at fragment {} node {}",
                            fragment.id().get(),
                            node.id.get()
                        ));
                    }
                    preflight_scan_source_identity(relation, fact, typed)?;
                    let scan_columns = preflight_scan_columns(
                        fragment,
                        node,
                        relation.schema(),
                        provider_outputs,
                        fact,
                        typed,
                    )?;
                    preflight_scan_dynamic_filters(
                        fragment,
                        node,
                        &scan_columns,
                        typed,
                        scan_dynamic_filters
                            .get(&(fragment.id(), node.id))
                            .map(Vec::as_slice)
                            .unwrap_or_default(),
                    )?;
                }
                NodeKind::Repeat {
                    grouping_values, ..
                } if !v1_repeat_grouping_values_are_lossless(fragment, node, grouping_values) => {
                    return unsupported(
                        fragment,
                        node,
                        "Repeat that moves a null-extended grouping column",
                    );
                }
                NodeKind::TableWriter { target } => {
                    let fact = private_facts
                        .write_fact(target.write_target_ordinal)
                        .ok_or_else(|| {
                            format!(
                                "native wire v1 write fact missing for target {}",
                                target.write_target_ordinal.get()
                            )
                        })?;
                    let expected =
                        novarocks_proto_codec::connector_common::encode_connector_payload_message(
                            &target.handle,
                        );
                    let expected_handle =
                        novarocks_proto_models::connector_write::ConnectorWriterHandle {
                            provider_payload: Some(expected),
                        };
                    if fact.handle != expected_handle {
                        return Err(format!(
                            "native wire v1 writer handle payload or public header mismatch for target {}",
                            target.write_target_ordinal.get()
                        ));
                    }
                    for field in &target.target_fields {
                        validate_physical_type(&field.ty.data_type)?;
                    }
                    validate_writer_schema(&target.output_schema)?;
                    for call in &target.partial_aggregates {
                        if !matches!(call.binding.phase, AggregatePhase::Partial { .. }) {
                            return unsupported(
                                fragment,
                                node,
                                "writer aggregate whose phase is not Partial",
                            );
                        }
                        validate_aggregate_binding_from_types(function_catalog, &call.binding)?;
                    }
                }
                NodeKind::TableFinish(spec) => {
                    validate_writer_schema(&spec.input_schema)?;
                    validate_writer_schema(&spec.output_schema)?;
                    for call in &spec.final_aggregates {
                        if !matches!(call.binding.phase, AggregatePhase::Final { .. }) {
                            return unsupported(
                                fragment,
                                node,
                                "table-finish aggregate whose phase is not Final",
                            );
                        }
                        validate_aggregate_binding_from_types(function_catalog, &call.binding)?;
                    }
                }
                NodeKind::Aggregate { calls, .. } => {
                    // Each call says for itself whether it reads values or a
                    // state, and the wire carries that per call, so calls of
                    // different phases in one node travel intact -- as long
                    // as they agree on finalizing, which the wire states once
                    // for the node.
                    if !v1_aggregate_phases_are_lossless(calls) {
                        return unsupported(
                            fragment,
                            node,
                            "Aggregate whose calls disagree about finalizing",
                        );
                    }
                    for call in calls {
                        let arguments = call
                            .arguments
                            .iter()
                            .copied()
                            .chain(call.order_by.iter().map(|item| item.expr))
                            .collect::<Vec<_>>();
                        validate_aggregate_binding(
                            function_catalog,
                            fragment,
                            &call.binding,
                            &arguments,
                        )?;
                    }
                }
                NodeKind::TableFunction {
                    function,
                    arguments,
                    outputs,
                    ..
                } => {
                    validate_table_binding(function_catalog, fragment, function, arguments)?;
                    preflight_table_function_v1(fragment, node, function, outputs)?;
                }
                NodeKind::Sort {
                    mode: SortMode::PartitionTopN { limit, .. },
                    ..
                } => {
                    if !v1_partition_topn_limit_is_addressable(*limit) {
                        return unsupported(
                            fragment,
                            node,
                            "partition TopN with a zero or unaddressable limit",
                        );
                    }
                }
                NodeKind::TopN {
                    limit,
                    offset,
                    phase,
                    ..
                } => {
                    if !v1_topn_phase_is_lossless(*phase) {
                        return unsupported(
                            fragment,
                            node,
                            "split TopN sequence requiring ExchangeReceiver TopNSplit",
                        );
                    }
                    preflight_i64(*limit, fragment, node, "TopN limit")?;
                    preflight_i64(*offset, fragment, node, "TopN offset")?;
                }
                NodeKind::Limit { limit, offset } => {
                    if let Some(limit) = limit {
                        preflight_i64(*limit, fragment, node, "Limit limit")?;
                    }
                    preflight_i64(*offset, fragment, node, "Limit offset")?;
                }
                NodeKind::AssertOneRow(RowCountAssertionSpec::Global { desired_rows, .. }) => {
                    preflight_i64(*desired_rows, fragment, node, "assert desired row count")?;
                }
                NodeKind::HashJoin {
                    kind, build_side, ..
                } => {
                    if !v1_hash_join_build_is_lossless(*kind, *build_side) {
                        return unsupported(
                            fragment,
                            node,
                            &format!("{build_side:?}-build {kind:?} HashJoin"),
                        );
                    }
                    if !v1_join_output_is_lossless(fragment, node, *kind) {
                        return unsupported(fragment, node, "unrepresentable HashJoin output port");
                    }
                }
                NodeKind::NestLoopJoin { kind, .. }
                    if !v1_join_output_is_lossless(fragment, node, *kind) =>
                {
                    return unsupported(fragment, node, "unrepresentable NestLoopJoin output port");
                }
                _ => {}
            }
        }
        match fragment.sink() {
            FragmentSink::Router { routes, .. } => {
                wire_group_id(fragment.id()).map_err(|error| {
                    format!(
                        "fragment {} router group identity is not representable: {error}",
                        fragment.id().get()
                    )
                })?;
                let root = &fragment.nodes()[&fragment.root()];
                for route in routes {
                    let edge = physical
                        .edges()
                        .get(&route.edge)
                        .ok_or_else(|| format!("router names absent edge {}", route.edge.get()))?;
                    if edge.kind != EdgeKind::ChangeStreamRouter {
                        return Err(format!(
                            "router route edge {} is not a change-stream edge",
                            edge.id.get()
                        ));
                    }
                    router_route_ordinals(root, route).map_err(|error| {
                        format!(
                            "fragment {} router route {:?} occurrences are not representable: {error}",
                            fragment.id().get(),
                            route.route_id
                        )
                    })?;
                }
            }
            FragmentSink::Result | FragmentSink::Stream { .. } | FragmentSink::Multicast { .. } => {
            }
            FragmentSink::SealedArtifact(_) | FragmentSink::Noop => {
                unreachable!("shared preflight rejects these sinks")
            }
        }
    }
    Ok(())
}

fn preflight_runtime_filters(physical: &PhysicalPlan) -> Result<ScanRuntimeFilterBindings, String> {
    use novarocks_physical_plan::{
        RuntimeFilterConsumerActivation, RuntimeFilterDomain, RuntimeFilterProducerTarget,
    };

    for filter in physical.runtime_filters().values() {
        match &filter.domain {
            RuntimeFilterDomain::Membership { ty, .. } => {
                validate_physical_type(&ty.data_type)?;
            }
            RuntimeFilterDomain::Ordered {
                key,
                inclusive,
                comparator,
            } => {
                validate_physical_type(&key.ty.data_type)?;
                if !inclusive {
                    return Err(format!(
                        "native wire v1 cannot encode exclusive runtime-filter {} bounds",
                        filter.id.get()
                    ));
                }
                if comparator.stable_name() != "novarocks.native-scalar-order.v1" {
                    return Err(format!(
                        "native wire v1 runtime-filter {} comparator is unsupported",
                        filter.id.get()
                    ));
                }
                let mut canonical_type = Vec::new();
                encode_runtime_filter_key_type(&key.ty.data_type, &mut canonical_type)?;
            }
        }
        for producer in &filter.producers {
            only_endpoint_value(&producer.endpoint)?;
            i32::try_from(producer.endpoint.node.get()).map_err(|_| {
                format!(
                    "runtime filter {} producer node exceeds i32",
                    filter.id.get()
                )
            })?;
            match producer.target {
                RuntimeFilterProducerTarget::JoinBuildKey { .. }
                    if !v1_join_build_runtime_filter_domain_is_lossless(&filter.domain) =>
                {
                    return Err(format!(
                        "native wire v1 cannot attach ordered runtime-filter {} to a HashJoin build key",
                        filter.id.get()
                    ));
                }
                RuntimeFilterProducerTarget::AggregateTopNKey { limit, .. } => {
                    u32::try_from(limit).map_err(|_| {
                        format!(
                            "native wire v1 Aggregate TopN runtime-filter {} limit exceeds u32",
                            filter.id.get()
                        )
                    })?;
                }
                RuntimeFilterProducerTarget::JoinBuildKey { .. } => {}
            }
        }
        for consumer in &filter.consumers {
            only_endpoint_value(&consumer.endpoint)?;
            i32::try_from(consumer.endpoint.node.get()).map_err(|_| {
                format!(
                    "runtime filter {} consumer node exceeds i32",
                    filter.id.get()
                )
            })?;
            // The wire names two activations, and a consumer that installs
            // only the complete snapshot is the second of them exactly when
            // the filter publishes once: then its only update is the complete
            // one. A filter that publishes monotonic updates would have its
            // partial ones installed too, and a partial membership set prunes
            // rows the complete one keeps.
            if matches!(
                consumer.activation,
                RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete { .. }
            ) && filter.lifecycle
                != novarocks_physical_plan::RuntimeFilterLifecycle::CompleteOnce
            {
                return Err(format!(
                    "native wire v1 cannot install only the complete snapshot of runtime filter {}, which publishes monotonic updates",
                    filter.id.get()
                ));
            }
        }
    }
    let mut scan_bindings = BTreeMap::<(FragmentId, NodeId), Vec<(u32, ValueId)>>::new();
    for binding in physical_v1_runtime_filter_bindings(physical)? {
        let PhysicalV1RuntimeFilterBindingRole::Consumer(index) = binding.role else {
            continue;
        };
        let consumer = &physical.runtime_filters()[&binding.filter].consumers[index];
        if consumer.apply_point == novarocks_physical_plan::RuntimeFilterApplyPoint::ScanSource {
            scan_bindings
                .entry((binding.fragment, binding.node))
                .or_default()
                .push((binding.binding_id, only_endpoint_value(&consumer.endpoint)?));
        }
    }
    Ok(scan_bindings)
}

fn validate_scalar_binding(
    catalog: &EngineFunctionCatalog,
    fragment: &Fragment,
    function: &novarocks_physical_plan::BoundFunction,
    arguments: &[ExprId],
) -> Result<(), String> {
    let request_arguments = arguments
        .iter()
        .map(|argument| physical_function_argument(fragment, *argument))
        .collect::<Result<Vec<_>, _>>()?;
    validate_bound_function(
        catalog,
        function,
        &request_arguments,
        request_arguments.len(),
        FunctionResultType::Scalar(function.result_type.clone()),
        None,
    )
}

fn validate_table_binding(
    catalog: &EngineFunctionCatalog,
    fragment: &Fragment,
    function: &novarocks_physical_plan::BoundTableFunction,
    arguments: &[ExprId],
) -> Result<(), String> {
    for argument in &function.argument_types {
        validate_function_argument_type(argument)?;
    }
    for result in &function.result_types {
        validate_physical_type(&result.data_type)
            .map_err(|reason| format!("table function result: {reason}"))?;
    }
    let request_arguments = arguments
        .iter()
        .map(|argument| physical_function_argument(fragment, *argument))
        .collect::<Result<Vec<_>, _>>()?;
    let bound = ResolvedFunctionBinding {
        function_id: function.function_id.clone(),
        kind: novarocks_functions::FunctionKind::Table,
        semantics: FunctionSemantics {
            volatility: function.volatility,
            argument_evaluation: function.argument_evaluation,
            failure_behavior: function.failure_behavior,
        },
        logical_argument_count: request_arguments.len(),
        selected: FunctionBindingSelection {
            overload: function.overload.clone(),
            argument_types: function.argument_types.clone(),
            result_type: FunctionResultType::Relation(function.result_types.clone()),
            aggregate: None,
        },
    };
    catalog
        .validate_bound(
            &bound,
            FunctionBindingRequest {
                arguments: &request_arguments,
                logical_argument_count: request_arguments.len(),
            },
        )
        .map_err(|error| {
            format!(
                "native wire v1 table function binding `{}` is not in the exact catalog: {error}",
                function.function_id.as_str()
            )
        })
}

fn validate_aggregate_binding(
    catalog: &EngineFunctionCatalog,
    fragment: &Fragment,
    binding: &AggregateBinding,
    arguments: &[ExprId],
) -> Result<(), String> {
    let request_arguments = if binding.phase.consumes_logical_arguments() {
        arguments
            .iter()
            .map(|argument| physical_function_argument(fragment, *argument))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        binding
            .function
            .argument_types
            .iter()
            .map(function_argument_from_type)
            .collect()
    };
    validate_bound_function(
        catalog,
        &binding.function,
        &request_arguments,
        usize::try_from(binding.logical_argument_count)
            .map_err(|_| "aggregate logical argument count exceeds usize".to_string())?,
        FunctionResultType::Scalar(binding.function.result_type.clone()),
        Some(AggregateBindingSelection {
            intermediate_type: binding.intermediate_type.clone(),
            state_format: binding.state_format.clone(),
        }),
    )
}

fn validate_aggregate_binding_from_types(
    catalog: &EngineFunctionCatalog,
    binding: &AggregateBinding,
) -> Result<(), String> {
    let arguments = binding
        .function
        .argument_types
        .iter()
        .map(function_argument_from_type)
        .collect::<Vec<_>>();
    validate_bound_function(
        catalog,
        &binding.function,
        &arguments,
        usize::try_from(binding.logical_argument_count)
            .map_err(|_| "aggregate logical argument count exceeds usize".to_string())?,
        FunctionResultType::Scalar(binding.function.result_type.clone()),
        Some(AggregateBindingSelection {
            intermediate_type: binding.intermediate_type.clone(),
            state_format: binding.state_format.clone(),
        }),
    )
}

fn validate_bound_function(
    catalog: &EngineFunctionCatalog,
    function: &novarocks_physical_plan::BoundFunction,
    arguments: &[FunctionArgument],
    logical_argument_count: usize,
    result_type: FunctionResultType,
    aggregate: Option<AggregateBindingSelection>,
) -> Result<(), String> {
    for argument in &function.argument_types {
        validate_function_argument_type(argument)?;
    }
    validate_physical_type(&function.result_type.data_type)
        .map_err(|reason| format!("bound function result: {reason}"))?;
    if let Some(aggregate) = &aggregate {
        validate_physical_type(&aggregate.intermediate_type.data_type)
            .map_err(|reason| format!("aggregate intermediate: {reason}"))?;
    }
    let bound = ResolvedFunctionBinding {
        function_id: function.function_id.clone(),
        kind: function.kind,
        semantics: FunctionSemantics {
            volatility: function.volatility,
            argument_evaluation: function.argument_evaluation,
            failure_behavior: function.failure_behavior,
        },
        logical_argument_count,
        selected: FunctionBindingSelection {
            overload: function.overload.clone(),
            argument_types: function.argument_types.clone(),
            result_type,
            aggregate,
        },
    };
    catalog
        .validate_bound(
            &bound,
            FunctionBindingRequest {
                arguments,
                logical_argument_count,
            },
        )
        .map_err(|error| {
            format!(
                "native wire v1 function binding `{}` is not in the exact catalog: {error}",
                function.function_id.as_str()
            )
        })
}

fn validate_function_argument_type(argument: &FunctionArgumentType) -> Result<(), String> {
    match argument {
        FunctionArgumentType::Value(value_type) => validate_physical_type(&value_type.data_type),
        FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } => {
            for parameter in parameter_types {
                validate_physical_type(&parameter.data_type)?;
            }
            validate_physical_type(&result_type.data_type)
        }
    }
}

fn physical_function_argument(
    fragment: &Fragment,
    expression: ExprId,
) -> Result<FunctionArgument, String> {
    let expression = fragment.expressions().get(expression).ok_or_else(|| {
        format!(
            "function argument expression {} disappeared after physical validation",
            expression.get()
        )
    })?;
    Ok(match &expression.kind {
        ExprKind::Lambda {
            parameter_types, ..
        } => FunctionArgument::Lambda {
            parameter_types: parameter_types.clone(),
            result_type: expression.ty.clone(),
        },
        ExprKind::Literal(literal) => FunctionArgument::Value {
            value_type: expression.ty.clone(),
            constant: physical_function_literal(literal),
        },
        _ => FunctionArgument::Value {
            value_type: expression.ty.clone(),
            constant: None,
        },
    })
}

fn function_argument_from_type(argument: &FunctionArgumentType) -> FunctionArgument {
    match argument {
        FunctionArgumentType::Value(value_type) => FunctionArgument::Value {
            value_type: value_type.clone(),
            constant: None,
        },
        FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } => FunctionArgument::Lambda {
            parameter_types: parameter_types.clone(),
            result_type: result_type.clone(),
        },
    }
}

fn physical_function_literal(literal: &LiteralValue) -> Option<FunctionLiteral> {
    match literal {
        LiteralValue::Null => Some(FunctionLiteral::Null),
        LiteralValue::Boolean(value) => Some(FunctionLiteral::Boolean(*value)),
        LiteralValue::Int64(value) => Some(FunctionLiteral::Int64(*value)),
        LiteralValue::UInt64(value) => Some(FunctionLiteral::UInt64(*value)),
        LiteralValue::Float64Bits(value) => Some(FunctionLiteral::Float64Bits(*value)),
        LiteralValue::LargeInt(value) => Some(FunctionLiteral::LargeInt(*value)),
        LiteralValue::Decimal128(value) => Some(FunctionLiteral::Decimal128(*value)),
        LiteralValue::Utf8(value) => Some(FunctionLiteral::Utf8(value.clone())),
        LiteralValue::Binary(value) => Some(FunctionLiteral::Binary(value.clone())),
        LiteralValue::Date32(_)
        | LiteralValue::Time64(_)
        | LiteralValue::Timestamp(_)
        | LiteralValue::IntervalMonthDayNano(_)
        // A constant-folding fact is carried in the vocabulary the function
        // registry speaks, which has no 256-bit decimal in it.
        | LiteralValue::Decimal256(_) => None,
    }
}

fn preflight_scan_source_identity(
    relation: &novarocks_physical_plan::Relation,
    fact: &PhysicalV1ScanFact,
    typed: &novarocks_proto_models::connector_read::ConnectorTableScanSource,
) -> Result<(), String> {
    use novarocks_proto_models::connector_read::catalog_table_handle::Relation as WireRelation;
    use novarocks_spi::connector::read_stack::ConnectorReadRelationKind;

    let selection_digest = match relation {
        novarocks_physical_plan::Relation::Data(relation) => relation.selection_digest,
        novarocks_physical_plan::Relation::Metadata(relation) => relation.selection_digest,
    };
    if fact.selection_digest != selection_digest {
        return Err("native wire v1 scan fact selection identity mismatch".into());
    }
    let expected_seal = physical_v1_scan_source_seal_digest(
        fact.occurrence,
        relation.read(),
        selection_digest,
        typed,
    )?;
    if fact.source_seal_digest != expected_seal {
        return Err("native wire v1 scan source seal mismatch".into());
    }

    let table = typed
        .table
        .as_ref()
        .ok_or_else(|| "native wire v1 scan source has no catalog table handle".to_string())?;
    let expected_catalog = novarocks_proto_codec::catalog::encode_catalog_handle(
        relation.read().binding.catalog_handle(),
    );
    if table.catalog_handle.as_ref() != Some(&expected_catalog) {
        return Err("native wire v1 scan source catalog identity mismatch".into());
    }
    let expected_view = novarocks_proto_codec::connector_common::encode_connector_payload_message(
        relation.read().relation.view(),
    );
    let actual_view = table
        .transaction
        .as_ref()
        .and_then(|transaction| transaction.provider_payload.as_ref());
    if actual_view != Some(&expected_view) {
        return Err("native wire v1 scan source read-view identity mismatch".into());
    }
    let expected_table = novarocks_proto_codec::connector_common::encode_connector_payload_message(
        relation.read().relation.table(),
    );
    let actual_table = match (&table.relation, relation.read().relation.kind()) {
        (Some(WireRelation::Table(handle)), ConnectorReadRelationKind::Table) => {
            handle.provider_payload.as_ref()
        }
        (Some(WireRelation::TableFunction(handle)), ConnectorReadRelationKind::TableFunction) => {
            handle.provider_payload.as_ref()
        }
        (Some(WireRelation::ChangeWindow(handle)), ConnectorReadRelationKind::ChangeWindow) => {
            handle.provider_payload.as_ref()
        }
        (Some(WireRelation::SystemTable(handle)), ConnectorReadRelationKind::SystemTable) => {
            handle.provider_payload.as_ref()
        }
        (Some(WireRelation::TableExecute(handle)), ConnectorReadRelationKind::TableExecute) => {
            handle.provider_payload.as_ref()
        }
        (Some(WireRelation::MergeTable(handle)), ConnectorReadRelationKind::MergeTable) => {
            handle.provider_payload.as_ref()
        }
        _ => {
            return Err("native wire v1 scan source relation kind mismatch".into());
        }
    };
    if actual_table != Some(&expected_table) {
        return Err("native wire v1 scan source table identity mismatch".into());
    }
    Ok(())
}

fn preflight_scan_columns<'a>(
    fragment: &Fragment,
    node: &PhysicalNode,
    relation_fields: &[novarocks_physical_plan::RelationField],
    provider_outputs: &'a [(ProviderColumnReference, ValueId)],
    fact: &'a PhysicalV1ScanFact,
    typed: &novarocks_proto_models::connector_read::ConnectorTableScanSource,
) -> Result<ScanColumnIndex<'a>, String> {
    let index = ScanColumnIndex::try_new(fragment, node, provider_outputs, fact)?;
    if fact.columns.len() != relation_fields.len() {
        return Err(format!(
            "native wire v1 scan fact column set differs from the final relation at fragment {} node {}",
            fragment.id().get(),
            node.id.get()
        ));
    }
    if typed.assignments.len() != fact.columns.len() {
        return Err(format!(
            "native wire v1 scan assignment count mismatch at fragment {} node {}",
            fragment.id().get(),
            node.id.get()
        ));
    }
    let mut variables = BTreeSet::new();
    for (assignment, column) in typed.assignments.iter().zip(&fact.columns) {
        if assignment.variable.is_empty() || !variables.insert(assignment.variable.as_str()) {
            return Err(format!(
                "native wire v1 scan assignments are not uniquely identified at fragment {} node {}",
                fragment.id().get(),
                node.id.get()
            ));
        }
        let actual = assignment
            .column
            .as_ref()
            .and_then(|column| column.provider_payload.as_ref());
        let expected = novarocks_proto_codec::connector_common::encode_connector_payload_message(
            &column.column.column_payload,
        );
        if actual != Some(&expected) {
            return Err(format!(
                "native wire v1 scan assignment column identity mismatch at fragment {} node {}",
                fragment.id().get(),
                node.id.get()
            ));
        }
        let expected_connector_type =
            novarocks_proto_codec::connector_read::encode_value_type(column.connector_type);
        if assignment.value_type.as_ref() != Some(&expected_connector_type)
            || !physical_type_accepts_connector_type(&column.ty.data_type, column.connector_type)
        {
            return Err(format!(
                "native wire v1 scan assignment type mismatch at fragment {} node {}",
                fragment.id().get(),
                node.id.get()
            ));
        }
        let expected_type = encode_physical_type(&column.ty.data_type)?;
        let table_column = index.table_column(column.internal, &column.name);
        if !table_column.is_some_and(|table_column| {
            table_column.nullable == column.ty.nullable
                && table_column.data_type.as_ref() == Some(&expected_type)
        }) {
            return Err(format!(
                "native wire v1 scan table schema does not exactly contain fact column `{}` at fragment {} node {}",
                column.name,
                fragment.id().get(),
                node.id.get()
            ));
        }
    }
    for field in relation_fields {
        validate_physical_type(&field.ty.data_type)?;
        let Some((_, column)) = index.fact_column(&field.column) else {
            return Err(format!(
                "native wire v1 scan fact omits provider column at fragment {} node {}",
                fragment.id().get(),
                node.id.get()
            ));
        };
        if column.ty != field.ty {
            return Err(format!(
                "native wire v1 scan fact type mismatch at fragment {} node {}",
                fragment.id().get(),
                node.id.get()
            ));
        }
    }
    for (column, value) in provider_outputs {
        let Some((_, fact_column)) = index.fact_column(column) else {
            return Err(format!(
                "native wire v1 scan output value {} has no exact provider column fact",
                value.get()
            ));
        };
        let value_type = &fragment.values()[value].ty;
        if &fact_column.ty != value_type {
            return Err(format!(
                "native wire v1 scan output value {} disagrees with provider type",
                value.get()
            ));
        }
    }
    Ok(index)
}

struct ScanColumnIndex<'a> {
    facts_by_payload: HashMap<ProviderPayloadKey<'a>, (usize, &'a PhysicalV1ScanColumn)>,
    providers_by_value: HashMap<ValueId, &'a ProviderColumnReference>,
    table_columns: HashMap<(bool, &'a str), &'a plan::ColumnDef>,
}

#[derive(Clone, Copy)]
struct ProviderPayloadKey<'a>(&'a novarocks_connector_contract::ConnectorEncodedPayload);

impl PartialEq for ProviderPayloadKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for ProviderPayloadKey<'_> {}

impl Hash for ProviderPayloadKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let header = self.0.header();
        header.provider_id().hash(state);
        header.catalog().hash(state);
        header.category().hash(state);
        header.codec_revision().get().hash(state);
        self.0.payload().hash(state);
    }
}

impl<'a> ScanColumnIndex<'a> {
    fn try_new(
        fragment: &Fragment,
        node: &PhysicalNode,
        provider_outputs: &'a [(ProviderColumnReference, ValueId)],
        fact: &'a PhysicalV1ScanFact,
    ) -> Result<Self, String> {
        let mut facts_by_payload = HashMap::with_capacity(fact.columns.len());
        for (ordinal, column) in fact.columns.iter().enumerate() {
            if facts_by_payload
                .insert(
                    ProviderPayloadKey(&column.column.column_payload),
                    (ordinal, column),
                )
                .is_some()
            {
                return Err(format!(
                    "native wire v1 scan fact repeats a provider column at fragment {} node {}",
                    fragment.id().get(),
                    node.id.get()
                ));
            }
        }

        let mut providers_by_value = HashMap::with_capacity(provider_outputs.len());
        let mut provider_payloads = HashSet::with_capacity(provider_outputs.len());
        for (provider, value) in provider_outputs {
            if providers_by_value.insert(*value, provider).is_some() {
                return Err(format!(
                    "native wire v1 scan has duplicate provider value {} at fragment {} node {}",
                    value.get(),
                    fragment.id().get(),
                    node.id.get()
                ));
            }
            if !provider_payloads.insert(ProviderPayloadKey(&provider.column_payload)) {
                return Err(format!(
                    "native wire v1 scan maps one provider column more than once at fragment {} node {}",
                    fragment.id().get(),
                    node.id.get()
                ));
            }
        }

        let mut table_columns = HashMap::with_capacity(
            fact.table.columns.len() + fact.table.iceberg_row_lineage_metadata_columns.len(),
        );
        for (internal, columns) in [
            (false, fact.table.columns.as_slice()),
            (
                true,
                fact.table.iceberg_row_lineage_metadata_columns.as_slice(),
            ),
        ] {
            for column in columns {
                if table_columns
                    .insert((internal, column.name.as_str()), column)
                    .is_some()
                {
                    return Err(format!(
                        "native wire v1 scan table schema repeats column `{}` at fragment {} node {}",
                        column.name,
                        fragment.id().get(),
                        node.id.get()
                    ));
                }
            }
        }

        Ok(Self {
            facts_by_payload,
            providers_by_value,
            table_columns,
        })
    }

    fn fact_column(
        &self,
        provider: &ProviderColumnReference,
    ) -> Option<(usize, &'a PhysicalV1ScanColumn)> {
        self.facts_by_payload
            .get(&ProviderPayloadKey(&provider.column_payload))
            .copied()
    }

    fn fact_column_for_value(&self, value: ValueId) -> Option<(usize, &'a PhysicalV1ScanColumn)> {
        self.providers_by_value
            .get(&value)
            .and_then(|provider| self.fact_column(provider))
    }

    fn table_column(&self, internal: bool, name: &str) -> Option<&'a plan::ColumnDef> {
        self.table_columns.get(&(internal, name)).copied()
    }
}

fn physical_type_accepts_connector_type(
    data_type: &arrow::datatypes::DataType,
    connector_type: ConnectorValueType,
) -> bool {
    use arrow::datatypes::{DataType, TimeUnit};

    matches!(
        (data_type, connector_type),
        (DataType::Boolean, ConnectorValueType::Boolean)
            | (DataType::Int8, ConnectorValueType::TinyInt)
            | (DataType::Int16, ConnectorValueType::SmallInt)
            | (DataType::Int32, ConnectorValueType::Integer)
            | (DataType::Int64, ConnectorValueType::BigInt)
            | (DataType::Float32, ConnectorValueType::Real)
            | (DataType::Float64, ConnectorValueType::Double)
            | (DataType::Date32, ConnectorValueType::Date)
            | (
                DataType::Time64(TimeUnit::Microsecond),
                ConnectorValueType::TimeMicros
            )
            | (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                ConnectorValueType::TimestampMillis
            )
            | (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                ConnectorValueType::TimestampMicros
            )
            | (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                ConnectorValueType::TimestampNanos
            )
            | (
                DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
                ConnectorValueType::TimestampTzMicros
            )
            | (
                DataType::Timestamp(TimeUnit::Nanosecond, Some(_)),
                ConnectorValueType::TimestampTzNanos
            )
            | (DataType::Utf8, ConnectorValueType::Varchar)
            | (DataType::Binary, ConnectorValueType::Varbinary)
            | (DataType::FixedSizeBinary(16), ConnectorValueType::Uuid)
            | (
                DataType::FixedSizeBinary(16),
                ConnectorValueType::Fixed { length: 16 }
            )
            // `NonComparable` is the connector's own name for a column whose
            // engine type has no comparable counterpart -- ROW, ARRAY, MAP,
            // and the variant a large binary carries -- so those engine types
            // are exactly what it types.
            | (
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::Map(_, _)
                    | DataType::Struct(_)
                    | DataType::LargeBinary,
                ConnectorValueType::NonComparable
            )
    ) || matches!(
        (data_type, connector_type),
        (
            DataType::Decimal128(precision, scale),
            ConnectorValueType::Decimal {
                precision: connector_precision,
                scale: connector_scale,
            }
        ) if *precision == connector_precision && *scale == connector_scale
    )
}

fn preflight_scan_dynamic_filters(
    fragment: &Fragment,
    node: &PhysicalNode,
    columns: &ScanColumnIndex<'_>,
    typed: &novarocks_proto_models::connector_read::ConnectorTableScanSource,
    expected: &[(u32, ValueId)],
) -> Result<(), String> {
    if typed.dynamic_filters.len() != expected.len() {
        return Err(format!(
            "native wire v1 scan dynamic-filter count mismatch at fragment {} node {}",
            fragment.id().get(),
            node.id.get()
        ));
    }
    for (actual, (binding_id, value)) in typed.dynamic_filters.iter().zip(expected) {
        let (assignment_ordinal, _) = columns.fact_column_for_value(*value).ok_or_else(|| {
            format!(
                "scan-source runtime filter binding {} value {} has no exact provider fact",
                binding_id,
                value.get()
            )
        })?;
        let variable = &typed.assignments[assignment_ordinal].variable;
        if actual.filter_id != *binding_id || &actual.variable != variable {
            return Err(format!(
                "native wire v1 scan dynamic-filter binding {} mismatch at fragment {} node {}",
                binding_id,
                fragment.id().get(),
                node.id.get()
            ));
        }
    }
    Ok(())
}

fn typed_scan_source(
    table: &plan::TableDef,
) -> Option<&novarocks_proto_models::connector_read::ConnectorTableScanSource> {
    match table.source.as_ref()?.kind.as_ref()? {
        plan::scan_source::Kind::TypedConnectorRead(source) => Some(source),
    }
}

fn unsupported<T>(fragment: &Fragment, node: &PhysicalNode, kind: &str) -> Result<T, String> {
    Err(format!(
        "native wire v1 cannot encode {kind} losslessly at fragment {} node {}",
        fragment.id().get(),
        node.id.get()
    ))
}

fn validate_writer_schema(
    schema: &novarocks_physical_plan::WriterRelationSchema,
) -> Result<(), String> {
    for field in &schema.fields {
        validate_arrow_authoritative_compatibility_type(&field.ty.data_type)?;
    }
    Ok(())
}

fn preflight_table_function_v1(
    fragment: &Fragment,
    node: &PhysicalNode,
    function: &novarocks_physical_plan::BoundTableFunction,
    outputs: &[novarocks_physical_plan::TableFunctionOutput],
) -> Result<(), String> {
    use novarocks_physical_plan::TableFunctionOutput;

    let [input_id] = node.inputs.as_ref() else {
        return unsupported(
            fragment,
            node,
            "TableFunction without exactly one v1 outer input",
        );
    };
    let child = &fragment.nodes()[input_id];
    if outputs.len() != child.output.columns.len() + function.result_types.len() {
        return unsupported(
            fragment,
            node,
            "TableFunction output is not child columns followed by every function result",
        );
    }
    for (actual, expected) in outputs
        .iter()
        .take(child.output.columns.len())
        .zip(&child.output.columns)
    {
        if *actual != TableFunctionOutput::PassThrough(*expected) {
            return unsupported(
                fragment,
                node,
                "TableFunction pass-through order differs from the complete child layout",
            );
        }
    }
    for (ordinal, output) in outputs.iter().skip(child.output.columns.len()).enumerate() {
        let TableFunctionOutput::FunctionResult { result_ordinal, .. } = output else {
            return unsupported(
                fragment,
                node,
                "TableFunction interleaves pass-through and function-result columns",
            );
        };
        if usize::try_from(*result_ordinal).ok() != Some(ordinal) {
            return unsupported(
                fragment,
                node,
                "TableFunction result columns differ from result_ordinal order",
            );
        }
    }
    Ok(())
}

fn encode_fragment(
    physical: &PhysicalPlan,
    fragment: &Fragment,
    layout: &WireLayout,
    scan_facts: &impl PhysicalV1PrivateFacts,
    runtime_filters: &EncodedRuntimeFilters,
    names: &OutputValueNames,
) -> Result<plan::PlanFragment, String> {
    let root = encode_tree(
        physical,
        fragment,
        layout,
        fragment.root(),
        scan_facts,
        names,
        runtime_filters,
    )?;
    let root_node = &fragment.nodes()[&fragment.root()];
    Ok(plan::PlanFragment {
        fragment_id: fragment.id().get(),
        root: Some(root),
        // These fragment-wide v1 fields cannot represent independent input
        // edges or multicast/router branches. Execution reads the exact
        // per-edge and per-sink contracts encoded below, so keep this legacy
        // summary deterministic and semantically neutral.
        data_partition: Some(compatibility_fragment_partition()),
        output_partition: Some(compatibility_fragment_partition()),
        sink: Some(encode_sink(physical, fragment, layout)?),
        output_exprs: Vec::new(),
        output_columns: output_columns(names, fragment, layout, root_node)?,
        cte_id: matches!(fragment.sink(), FragmentSink::Multicast { .. })
            .then(|| fragment.id().get()),
        cte_exchange_nodes: physical
            .edges()
            .values()
            .filter(|edge| {
                edge.kind == EdgeKind::CteMulticast && edge.destination.fragment == fragment.id()
            })
            .map(|edge| {
                let source = &physical.fragments()[&edge.source.fragment];
                let slots = WireLayout::try_new(source)
                    .map_err(|error| error.to_string())?
                    .project_output(source, source.root(), &edge.source.projection)
                    .map_err(|error| error.to_string())?;
                Ok(plan::CteExchangeBinding {
                    cte_id: edge.source.fragment.get(),
                    node_id: i32::try_from(edge.destination.node.get())
                        .map_err(|_| "CTE destination node exceeds i32".to_string())?,
                    column_ids: slots.into_iter().map(WireSlotId::get_u32).collect(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        runtime_filter_bindings: runtime_filters.tables.get(&fragment.id()).cloned(),
    })
}

fn compatibility_fragment_partition() -> plan::DataPartition {
    plan::DataPartition {
        kind: plan::PartitionKind::Unpartitioned as i32,
        exprs: Vec::new(),
    }
}

fn encode_tree(
    physical: &PhysicalPlan,
    fragment: &Fragment,
    layout: &WireLayout,
    node_id: NodeId,
    scan_facts: &impl PhysicalV1PrivateFacts,
    names: &OutputValueNames,
    runtime_filters: &EncodedRuntimeFilters,
) -> Result<plan::DistributedNode, String> {
    let node = fragment.nodes().get(&node_id).ok_or_else(|| {
        format!(
            "fragment {} is missing node {}",
            fragment.id().get(),
            node_id.get()
        )
    })?;
    let children = node
        .inputs
        .iter()
        .map(|child| {
            encode_tree(
                physical,
                fragment,
                layout,
                *child,
                scan_facts,
                names,
                runtime_filters,
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    let payload = encode_node_payload(physical, fragment, layout, node, scan_facts, names)?;
    Ok(plan::DistributedNode {
        node_id: i32::try_from(node.id.get())
            .map_err(|_| "native wire v1 node identity exceeds i32".to_string())?,
        fragment_id: fragment.id().get(),
        tuple_ids: Vec::new(),
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        children,
        runtime_filter_binding_ids: runtime_filters
            .node_bindings
            .get(&(fragment.id(), node.id))
            .cloned()
            .unwrap_or_default(),
        payload: Some(payload),
    })
}

fn encode_node_payload(
    physical: &PhysicalPlan,
    fragment: &Fragment,
    layout: &WireLayout,
    node: &PhysicalNode,
    scan_facts: &impl PhysicalV1PrivateFacts,
    names: &OutputValueNames,
) -> Result<plan::distributed_node::Payload, String> {
    use plan::distributed_node::Payload;
    use plan::plan_node::Kind;

    if let NodeKind::ExchangeSource { edge, .. } = &node.kind {
        return Ok(Payload::Exchange(encode_exchange_source(
            physical, fragment, layout, node, *edge, names,
        )?));
    }
    if let NodeKind::TableWriter { target } = &node.kind {
        return Ok(Payload::TableWriter(encode_table_writer(
            fragment, layout, node, target, scan_facts,
        )?));
    }
    if let NodeKind::TableFinish(spec) = &node.kind {
        return Ok(Payload::TableFinish(encode_table_finish(
            fragment, layout, node, spec,
        )?));
    }
    let outputs = output_columns(names, fragment, layout, node)?;
    let kind = match &node.kind {
        NodeKind::Scan {
            relation,
            provider_outputs,
            residuals,
            derived_values,
            ..
        } => Kind::Scan(encode_scan(
            fragment,
            layout,
            node,
            provider_outputs,
            residuals,
            derived_values,
            relation.schema(),
            scan_facts,
        )?),
        NodeKind::Filter { predicates } => Kind::Filter(plan::FilterNode {
            predicate: Some(crate::physical_expr::encode_predicate_conjunction(
                fragment,
                layout,
                node.id,
                predicates,
                ValueResolution::NodeInput,
            )?),
        }),
        NodeKind::Project { expressions } => Kind::Project(plan::ProjectNode {
            items: expressions
                .iter()
                .enumerate()
                .map(|(ordinal, (expression, _))| {
                    // One value may be published twice -- `SELECT x AS a, x AS
                    // b` is two columns of one value -- and each occurrence
                    // carries its own name. The node's own output columns
                    // already say which name stands at which ordinal; reading
                    // the name off the value would give both occurrences the
                    // first one.
                    let output = outputs.get(ordinal).ok_or_else(|| {
                        format!(
                            "fragment {} node {} project item {ordinal} has no output column",
                            fragment.id().get(),
                            node.id.get()
                        )
                    })?;
                    Ok(plan::ProjectItem {
                        expr: Some(encode_physical_expr(
                            fragment,
                            layout,
                            node.id,
                            *expression,
                            ValueResolution::NodeInput,
                        )?),
                        output_name: output.name.clone(),
                        output_column_id: output.column_id,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            output_qualifier: None,
        }),
        NodeKind::Values { rows } => Kind::Values(plan::ValuesNode {
            rows: rows
                .iter()
                .map(|row| {
                    Ok(plan::ExprList {
                        values: encode_exprs(
                            fragment,
                            layout,
                            node.id,
                            row,
                            &ValueResolution::NodeInput,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            columns: outputs.clone(),
        }),
        NodeKind::Limit { limit, offset } => Kind::Limit(plan::LimitNode {
            limit: limit.map(i64_from_u64).transpose()?,
            offset: Some(i64_from_u64(*offset)?),
        }),
        NodeKind::Sort { order_by, mode } => Kind::Sort(plan::SortNode {
            // The wire's items are the keys this sort actually sorts by. An
            // analytic sort groups its partitions before it orders within
            // them, so its partition keys lead; a partition TopN's own
            // operator groups by the partition keys itself and ranks by the
            // order keys, so they stay apart there.
            items: match mode {
                SortMode::Analytic { partition_by } => encode_sort_items(
                    fragment,
                    layout,
                    node.id,
                    &partition_by
                        .iter()
                        .chain(order_by.iter())
                        .cloned()
                        .collect::<Vec<_>>(),
                )?,
                SortMode::Global | SortMode::PartitionTopN { .. } => {
                    encode_sort_items(fragment, layout, node.id, order_by)?
                }
            },
            analytic_partition_by: match mode {
                SortMode::Global => Vec::new(),
                SortMode::Analytic { partition_by }
                | SortMode::PartitionTopN { partition_by, .. } => partition_by
                    .iter()
                    .map(|item| {
                        encode_physical_expr(
                            fragment,
                            layout,
                            node.id,
                            item.expr,
                            ValueResolution::NodeInput,
                        )
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            },
            output_columns: outputs.clone(),
            offset: None,
            partition_limit: match mode {
                SortMode::PartitionTopN { limit, .. } => Some(*limit),
                SortMode::Global | SortMode::Analytic { .. } => None,
            },
            topn_type: match mode {
                SortMode::PartitionTopN { kind, .. } => Some(encode_partition_topn_type(*kind)),
                SortMode::Global | SortMode::Analytic { .. } => None,
            },
        }),
        NodeKind::TopN {
            order_by,
            limit,
            offset,
            phase,
        } => Kind::Topn(plan::TopNNode {
            items: encode_sort_items(fragment, layout, node.id, order_by)?,
            limit: Some(i64_from_u64(*limit)?),
            offset: Some(i64_from_u64(*offset)?),
            phase: match phase {
                TopNPhase::Partial { .. } => plan::TopNPhase::TopnPhasePartial as i32,
                TopNPhase::Final { .. } | TopNPhase::Single => {
                    plan::TopNPhase::TopnPhaseFinal as i32
                }
            },
            // `is_split` says the final half of a split is collapsed into a
            // merging exchange, which the receiver then owns. A completed plan
            // states both halves as nodes of its own with a plain gather
            // between them, so nothing here is collapsed.
            is_split: false,
        }),
        NodeKind::HashJoin {
            kind,
            keys,
            build_side: _,
            distribution,
            residual,
            ..
        } => Kind::HashJoin(plan::HashJoinNode {
            join_type: encode_join_kind(*kind),
            eq_conditions: keys
                .iter()
                .map(|key| {
                    Ok(plan::HashJoinEqCondition {
                        left: Some(encode_physical_expr(
                            fragment,
                            layout,
                            node.id,
                            key.left,
                            ValueResolution::NodeInput,
                        )?),
                        right: Some(encode_physical_expr(
                            fragment,
                            layout,
                            node.id,
                            key.right,
                            ValueResolution::NodeInput,
                        )?),
                        null_safe: key.null_safe,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            other_condition: residual
                .map(|expression| {
                    encode_physical_expr(
                        fragment,
                        layout,
                        node.id,
                        expression,
                        ValueResolution::NodeInput,
                    )
                })
                .transpose()?,
            distribution: encode_join_distribution(*distribution),
            execution_mode: Some(encode_join_execution(*distribution)),
        }),
        NodeKind::NestLoopJoin {
            kind, predicate, ..
        } => Kind::NestLoopJoin(plan::NestLoopJoinNode {
            join_type: encode_join_kind(*kind),
            condition: predicate
                .map(|expression| {
                    encode_physical_expr(
                        fragment,
                        layout,
                        node.id,
                        expression,
                        ValueResolution::NodeInput,
                    )
                })
                .transpose()?,
        }),
        NodeKind::SetOp {
            kind,
            input_mappings,
        } => Kind::SetOp(plan::SetOpNode {
            kind: match kind {
                SetOperationKind::UnionAll => plan::PlanSetOpKind::UnionAll as i32,
                SetOperationKind::Intersect => plan::PlanSetOpKind::Intersect as i32,
                SetOperationKind::Except => plan::PlanSetOpKind::Except as i32,
            },
            output_columns: outputs.clone(),
            child_output_columns: node
                .inputs
                .iter()
                .zip(input_mappings)
                .map(|(input, mapping)| {
                    let child = &fragment.nodes()[input];
                    projected_columns(fragment, layout, child, mapping)
                        .map(|columns| plan::OutputColumnList { columns })
                })
                .collect::<Result<Vec<_>, String>>()?,
        }),
        NodeKind::Aggregate {
            group_by,
            calls,
            grouping,
        } => {
            if !v1_aggregate_phases_are_lossless(calls) {
                return unsupported(
                    fragment,
                    node,
                    "Aggregate whose calls disagree about finalizing",
                );
            }
            // The one thing the wire's reader takes from the mode is whether
            // this node finalizes.  The five names it may carry are the
            // sealed planner's, and among the names that carry the bit this
            // node needs, the mode is the one whose meaning also matches what
            // the node says about its groups.  Whether a call reads values or
            // a state is said per call, so a node that merges some calls
            // while computing others -- a DISTINCT beside a plain aggregate
            // -- needs no single phase across them.
            let finalizes = calls
                .iter()
                .any(|call| call.binding.phase.produces_final_result());
            let merges = calls
                .iter()
                .any(|call| call.binding.phase.sequence().is_some());
            let mode = match (grouping, finalizes) {
                (_, true) if merges => plan::AggMode::Global,
                (_, true) => plan::AggMode::Single,
                (novarocks_physical_plan::AggregateGrouping::Partial, false) => {
                    plan::AggMode::Local
                }
                // Groups finished, values not: the dedup phase a rollup reads.
                (novarocks_physical_plan::AggregateGrouping::Complete, false) => {
                    plan::AggMode::DistinctGlobal
                }
            };
            // A group key and a call stand where this node's port writes
            // them -- the keys first, in their own order, then the calls --
            // and that position is what names their slot. Two keys may be the
            // one value: a statement that marks a column and groups by both
            // writes it twice, and the port gives each occurrence its own
            // column.
            let group_key_columns = group_by
                .iter()
                .enumerate()
                .map(|(ordinal, (_, value))| {
                    output_column_at(fragment, layout, node, ordinal, *value, names)
                })
                .collect::<Result<Vec<_>, String>>()?;
            let aggregate_columns = calls
                .iter()
                .enumerate()
                .map(|(ordinal, call)| {
                    output_column_at(
                        fragment,
                        layout,
                        node,
                        group_by.len() + ordinal,
                        call.output,
                        names,
                    )
                })
                .collect::<Result<Vec<_>, String>>()?;
            Kind::HashAggregate(plan::HashAggregateNode {
                mode: mode as i32,
                group_by: group_by
                    .iter()
                    .map(|(expression, _)| {
                        encode_physical_expr(
                            fragment,
                            layout,
                            node.id,
                            *expression,
                            ValueResolution::NodeInput,
                        )
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                aggregates: calls
                    .iter()
                    .enumerate()
                    .map(|(call_ordinal, call)| {
                        Ok(plan::PlanAggregateCall {
                            name: wire_function_name(&call.binding.function.function_id)?.into(),
                            args: encode_exprs(
                                fragment,
                                layout,
                                node.id,
                                &call.arguments,
                                &ValueResolution::NodeInput,
                            )?,
                            distinct: call.distinct,
                            // The wire field is the aggregate's SQL result
                            // type, which every phase of it shares. What this
                            // phase's own column carries is sealed separately
                            // in the output layout.
                            result_type: Some(encode_physical_type(
                                &call.binding.function.result_type.data_type,
                            )?),
                            order_by: encode_sort_items(fragment, layout, node.id, &call.order_by)?,
                            output_column_id: output_slot_at(
                                layout,
                                node,
                                group_by.len() + call_ordinal,
                                call.output,
                            )?
                            .get_u32(),
                            resolved_signature: Some(encode_aggregate_signature(&call.binding)?),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                is_merge: calls
                    .iter()
                    .map(|call| !call.binding.phase.consumes_logical_arguments())
                    .collect(),
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns,
                    aggregate_columns,
                }),
                output_columns: outputs.clone(),
            })
        }
        NodeKind::Window(spec) => Kind::Window(plan::WindowNode {
            window_exprs: spec
                .expressions
                .iter()
                .map(|window| {
                    let expression =
                        fragment
                            .expressions()
                            .get(window.expression)
                            .ok_or_else(|| {
                                format!("window expression {} is absent", window.expression.get())
                            })?;
                    let ExprKind::WindowCall {
                        function,
                        distinct,
                        args,
                        function_order_by,
                        frame,
                        ignore_nulls,
                        aggregate_binding,
                    } = &expression.kind
                    else {
                        return Err(format!(
                            "Window node {} expression {} is not a WindowCall",
                            node.id.get(),
                            window.expression.get()
                        ));
                    };
                    Ok(plan::WindowExpr {
                        name: wire_function_name(&function.function_id)?.into(),
                        args: encode_exprs(
                            fragment,
                            layout,
                            node.id,
                            args,
                            &ValueResolution::NodeInput,
                        )?,
                        distinct: *distinct,
                        partition_by: spec
                            .partition_by
                            .iter()
                            .map(|item| {
                                encode_physical_expr(
                                    fragment,
                                    layout,
                                    node.id,
                                    item.expr,
                                    ValueResolution::NodeInput,
                                )
                            })
                            .collect::<Result<Vec<_>, String>>()?,
                        order_by: encode_sort_items(fragment, layout, node.id, &spec.order_by)?,
                        window_frame: frame
                            .as_ref()
                            .map(|frame| encode_window_frame(fragment, frame))
                            .transpose()?,
                        result_type: Some(encode_physical_type(&expression.ty.data_type)?),
                        output_name: value_name(window.output),
                        output_column_id: output_slot_for_value(layout, node, window.output)?
                            .get_u32(),
                        ignore_nulls: *ignore_nulls,
                        function_order_by: encode_sort_items(
                            fragment,
                            layout,
                            node.id,
                            function_order_by,
                        )?,
                        aggregate_binding: aggregate_binding
                            .as_ref()
                            .map(encode_aggregate_signature)
                            .transpose()?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            output_columns: outputs.clone(),
        }),
        NodeKind::Unpivot { spec } => Kind::Unpivot(plan::UnpivotNode {
            passthrough_columns: spec
                .passthrough
                .iter()
                .map(|(input, output)| {
                    Ok(plan::UnpivotPassthroughColumn {
                        input_column_id: layout
                            .input_value_slot(node.id, *input)
                            .map_err(|error| error.to_string())?
                            .get_u32(),
                        output_column_id: output_slot_for_value(layout, node, *output)?.get_u32(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            value_output_column_id: output_slot_for_value(layout, node, spec.value_output)?
                .get_u32(),
            literal_output_column_ids: spec
                .literal_outputs
                .iter()
                .map(|value| output_slot_for_value(layout, node, *value).map(WireSlotId::get_u32))
                .collect::<Result<Vec<_>, String>>()?,
            value_mappings: spec
                .mappings
                .iter()
                .map(|mapping| {
                    Ok(plan::UnpivotValueMapping {
                        input_value_column_id: layout
                            .input_value_slot(node.id, mapping.input)
                            .map_err(|error| error.to_string())?
                            .get_u32(),
                        constants: mapping
                            .constants
                            .iter()
                            .map(|constant| {
                                encode_unpivot_constant(fragment, layout, node.id, constant)
                            })
                            .collect::<Result<Vec<_>, String>>()?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            max_output_rows: spec.max_output_rows,
            max_output_bytes: spec.max_output_bytes,
            output_schema: Some(encode_unpivot_schema(fragment, layout, node)?),
        }),
        NodeKind::GenerateSeries { start, stop, step } => {
            Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: int64_literal(fragment, *start)?,
                end: int64_literal(fragment, *stop)?,
                step: step
                    .map(|value| int64_literal(fragment, value))
                    .transpose()?
                    .unwrap_or(1),
                column_name: outputs
                    .first()
                    .map_or_else(|| "value".into(), |column| column.name.clone()),
                alias: None,
                output_column_id: outputs.first().map_or(0, |column| column.column_id),
            })
        }
        NodeKind::TableFunction {
            function,
            arguments,
            outputs: table_outputs,
            left_outer,
        } => {
            let child_width = fragment.nodes()[&node.inputs[0]].output.columns.len();
            debug_assert!(table_outputs[..child_width].iter().all(|output| matches!(
                output,
                novarocks_physical_plan::TableFunctionOutput::PassThrough(_)
            )));
            Kind::TableFunction(plan::TableFunctionNode {
                function_name: wire_function_name(&function.function_id)?.into(),
                args: encode_exprs(
                    fragment,
                    layout,
                    node.id,
                    arguments,
                    &ValueResolution::NodeInput,
                )?,
                // The v1 decoder always prepends the complete child layout.
                // This carrier therefore contains relation-result columns only.
                output_columns: outputs[child_width..].to_vec(),
                alias: None,
                is_left_join: *left_outer,
            })
        }
        NodeKind::AssertOneRow(assertion) => {
            Kind::AssertOneRow(encode_assertion(layout, node, assertion)?)
        }
        NodeKind::ChangeEventExpand {
            events,
            effect_output,
        } => Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
            events: events
                .iter()
                .map(|event| {
                    Ok(plan::DistributedChangeEventSpec {
                        predicate: event
                            .predicate
                            .map(|expression| {
                                encode_physical_expr(
                                    fragment,
                                    layout,
                                    node.id,
                                    expression,
                                    ValueResolution::NodeInput,
                                )
                            })
                            .transpose()?,
                        assignments: event
                            .assignments
                            .iter()
                            .map(|(value, expression)| {
                                Ok(plan::DistributedChangeEventOutputExpr {
                                    output_column_id: output_slot_for_value(layout, node, *value)?
                                        .get_u32(),
                                    expr: expression
                                        .map(|expression| {
                                            encode_physical_expr(
                                                fragment,
                                                layout,
                                                node.id,
                                                expression,
                                                ValueResolution::NodeInput,
                                            )
                                        })
                                        .transpose()?,
                                })
                            })
                            .collect::<Result<Vec<_>, String>>()?,
                        effect: encode_effect(event.effect),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            output_columns: outputs.clone(),
            effect_column_id: output_slot_for_value(layout, node, *effect_output)?.get_u32(),
        }),
        NodeKind::Repeat {
            rollup_keys,
            grouping_sets,
            grouping_values,
            grouping_outputs,
        } => Kind::Repeat(encode_repeat(
            layout,
            node,
            rollup_keys,
            grouping_sets,
            grouping_values,
            grouping_outputs,
        )?),
        NodeKind::TableWriter { .. }
        | NodeKind::TableFinish(_)
        | NodeKind::ExchangeSource { .. } => {
            return unsupported(fragment, node, node_kind_name(&node.kind));
        }
    };
    let physical_output_columns = if matches!(&node.kind, NodeKind::Unpivot { .. }) {
        // The v1 decoder treats UnpivotNode.output_schema as the sole physical
        // schema authority and rejects a parallel PlanNode output schema.
        Vec::new()
    } else {
        outputs
    };
    Ok(Payload::Physical(plan::PlanNode {
        output_columns: physical_output_columns,
        kind: Some(kind),
    }))
}

#[allow(clippy::too_many_arguments)]
fn encode_scan(
    fragment: &Fragment,
    layout: &WireLayout,
    node: &PhysicalNode,
    provider_outputs: &[(ProviderColumnReference, ValueId)],
    residuals: &[ExprId],
    derived_values: &[ValueId],
    relation_fields: &[novarocks_physical_plan::RelationField],
    scan_facts: &impl PhysicalV1PrivateFacts,
) -> Result<plan::ScanNode, String> {
    let fact = scan_facts
        .scan_fact(fragment.id(), node.id)
        .ok_or_else(|| "scan fact disappeared after preflight".to_string())?;
    let index = ScanColumnIndex::try_new(fragment, node, provider_outputs, fact)?;
    let mut exact_scope = BTreeMap::new();
    // A scan publishes the provider's columns and the ones it derives from
    // them. A derived column is named by the value it publishes, since the
    // provider has no name for something it did not produce.
    let columns =
        node.output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| {
                let slot = layout
                    .output_slot(node.id, ordinal_u32(ordinal)?)
                    .map_err(|error| error.to_string())?;
                exact_scope.insert(*value, slot);
                match index.fact_column_for_value(*value) {
                    Some((_, fact_column)) => output_column(
                        slot,
                        &fact_column.name,
                        &fact_column.ty,
                        fact_column.internal,
                    ),
                    None if derived_values.contains(value) => {
                        let definition = fragment.values().get(value).ok_or_else(|| {
                            format!("scan derived value {} is absent", value.get())
                        })?;
                        output_column(slot, &value_name(*value), &definition.ty, false)
                    }
                    None => Err(format!(
                        "scan output value {} is neither provider-owned nor derived",
                        value.get()
                    )),
                }
            })
            .collect::<Result<Vec<_>, String>>()?;
    let variant_columns = derived_values
        .iter()
        .map(|value| encode_scan_variant_column(fragment, &index, &exact_scope, *value))
        .collect::<Result<Vec<_>, String>>()?;
    Ok(plan::ScanNode {
        database: fact.database.to_string(),
        table: Some(fact.table.clone()),
        alias: fact.alias.as_deref().map(str::to_owned),
        columns,
        predicates: residuals
            .iter()
            .map(|expression| {
                encode_physical_expr(
                    fragment,
                    layout,
                    node.id,
                    *expression,
                    ValueResolution::Exact(&exact_scope),
                )
            })
            .collect::<Result<Vec<_>, String>>()?,
        // What the reader must produce: the provider columns this scan reads,
        // and the columns it derives from them, which are produced while it
        // reads and are not the provider's to name.
        required_columns: relation_fields
            .iter()
            .map(|field| {
                index
                    .fact_column(&field.column)
                    .map(|(_, column)| column.name.to_string())
                    .ok_or_else(|| {
                        "scan relation field fact disappeared after preflight".to_string()
                    })
            })
            .chain(
                derived_values
                    .iter()
                    .copied()
                    .map(|value| Ok(value_name(value))),
            )
            .collect::<Result<Vec<_>, String>>()?,
        dict_columns: Vec::new(),
        variant_columns,
        mv_rewritten_from: None,
    })
}

/// One variant path a scan reads out of a column it is already reading.
///
/// The plan states it as the call it is -- `variant_get(column, path, type)`
/// over one of the scan's own provider columns -- and the wire states the same
/// call as a descriptor the reader applies while it reads. Whether a missing
/// path is an error or a null is the difference between the two functions the
/// statement could have written, so the binding is where that is read from.
fn encode_scan_variant_column(
    fragment: &Fragment,
    index: &ScanColumnIndex<'_>,
    slots: &BTreeMap<ValueId, WireSlotId>,
    value: ValueId,
) -> Result<plan::ScanVariantColumn, String> {
    let absent = || format!("scan derived value {} is not one variant path", value.get());
    let definition = fragment.values().get(&value).ok_or_else(absent)?;
    let novarocks_physical_plan::ValueOrigin::Expr { expr, .. } = definition.origin else {
        return Err(absent());
    };
    let ExprKind::FunctionCall { function, args } =
        &fragment.expressions().get(expr).ok_or_else(absent)?.kind
    else {
        return Err(absent());
    };
    let [source, path, requested] = args.as_ref() else {
        return Err(absent());
    };
    let strict = match wire_function_name(&function.function_id)? {
        "variant_get" => true,
        "try_variant_get" => false,
        other => {
            return Err(format!(
                "scan derived value {} is built by `{other}`, which is not a variant path",
                value.get()
            ));
        }
    };
    let source_value = match &fragment.expressions().get(*source).ok_or_else(absent)?.kind {
        ExprKind::Value(value) => *value,
        _ => return Err(absent()),
    };
    let (_, source_column) = index.fact_column_for_value(source_value).ok_or_else(|| {
        format!(
            "scan derived value {} reads value {}, which the provider does not produce",
            value.get(),
            source_value.get()
        )
    })?;
    let canonical_path = utf8_literal(fragment, *path).ok_or_else(absent)?;
    // The type literal the statement wrote is what the analyzer resolved this
    // column's type from; the wire carries the resolved type, which the reader
    // compares against the column it fills.
    utf8_literal(fragment, *requested).ok_or_else(absent)?;
    Ok(plan::ScanVariantColumn {
        source_column_id: slots
            .get(&source_value)
            .copied()
            .ok_or_else(absent)?
            .get_u32(),
        source_column: source_column.name.to_string(),
        synthetic_column_id: slots.get(&value).copied().ok_or_else(absent)?.get_u32(),
        synthetic_column: value_name(value),
        canonical_path: canonical_path.to_string(),
        requested_type: Some(encode_physical_type(&definition.ty.data_type)?),
        strict,
    })
}

/// The text one literal expression carries, when it is one.
fn utf8_literal(fragment: &Fragment, expression: ExprId) -> Option<&str> {
    match &fragment.expressions().get(expression)?.kind {
        ExprKind::Literal(LiteralValue::Utf8(value)) => Some(value),
        _ => None,
    }
}

fn encode_table_writer(
    fragment: &Fragment,
    layout: &WireLayout,
    node: &PhysicalNode,
    target: &novarocks_physical_plan::WriterTarget,
    facts: &impl PhysicalV1PrivateFacts,
) -> Result<plan::TableWriterNode, String> {
    let fact = facts
        .write_fact(target.write_target_ordinal)
        .ok_or_else(|| "write fact disappeared after preflight".to_string())?;
    let child = fragment
        .nodes()
        .get(&node.inputs[0])
        .ok_or_else(|| "table writer input disappeared after validation".to_string())?;
    let input_ordinals = project_output_ordinals(child, &target.input)?;
    let output_exprs = target
        .input
        .iter()
        .map(|value| encode_physical_value(fragment, layout, node.id, *value))
        .collect::<Result<Vec<_>, String>>()?;
    let target_schema = target
        .target_fields
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            Ok(common::OutputColumn {
                column_id: ordinal_u32(ordinal)?
                    .checked_add(1)
                    .ok_or_else(|| "writer target schema slot overflowed".to_string())?,
                name: field_token_name(fact, field.token)?,
                r#type: Some(encode_physical_type(&field.ty.data_type)?),
                nullable: field.ty.nullable,
                is_internal: field.hidden,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let catalog = target.handle.header().catalog();
    Ok(plan::TableWriterNode {
        catalog_handle: Some(novarocks_proto_models::catalog::CatalogHandle {
            catalog_name: catalog.catalog_name().as_str().into(),
            version: catalog.version().as_bytes().to_vec(),
        }),
        write_target_ordinal: target.write_target_ordinal.get(),
        handle: Some(fact.handle.clone()),
        input: Some(plan::ConnectorWriteInputBinding {
            kind: Some(plan::connector_write_input_binding::Kind::OutputOrdinals(
                plan::UInt64List {
                    values: input_ordinals,
                },
            )),
        }),
        writer_ordinal: 0,
        output_exprs,
        target_schema,
        writer_multiplex_schema: Some(encode_writer_relation_schema(
            layout,
            node,
            &target.output_schema,
        )?),
        partial_aggregate_plan: Some(plan::WriterPartialAggregatePlan {
            calls: target
                .partial_aggregates
                .iter()
                .map(|call| {
                    let input_ordinal = target
                        .input
                        .iter()
                        .position(|value| *value == call.input)
                        .map(ordinal_u32)
                        .transpose()?
                        .ok_or_else(|| {
                            format!(
                                "writer aggregate input {} is not projected",
                                call.input.get()
                            )
                        })?
                        .checked_add(1)
                        .ok_or_else(|| "writer aggregate input slot overflowed".to_string())?;
                    Ok(plan::WriterPartialAggregateCall {
                        input_slot_id: input_ordinal,
                        function_name: wire_function_name(&call.binding.function.function_id)?
                            .into(),
                        resolved_signature: Some(encode_aggregate_signature(&call.binding)?),
                        intermediate_slot_id: output_slot_for_value(layout, node, call.output)?
                            .get_u32(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
        }),
    })
}

fn project_output_ordinals(node: &PhysicalNode, values: &[ValueId]) -> Result<Vec<u64>, String> {
    let mut available = BTreeMap::<ValueId, VecDeque<u64>>::new();
    for (ordinal, value) in node.output.columns.iter().copied().enumerate() {
        available
            .entry(value)
            .or_default()
            .push_back(u64::from(ordinal_u32(ordinal)?));
    }
    values
        .iter()
        .map(|value| {
            available
                .get_mut(value)
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| {
                    format!(
                        "node {} cannot project another occurrence of value {}",
                        node.id.get(),
                        value.get()
                    )
                })
        })
        .collect()
}

fn router_route_ordinals(
    root: &PhysicalNode,
    route: &novarocks_physical_plan::ChangeStreamRoute,
) -> Result<(Vec<u64>, Vec<u64>), String> {
    let input_values = route
        .input_mapping
        .iter()
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    Ok((
        project_output_ordinals(root, &input_values)?,
        project_output_ordinals(root, &route.partition_by)?,
    ))
}

fn encode_unpivot_constant(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    constant: &UnpivotConstant,
) -> Result<plan::UnpivotConstant, String> {
    use plan::unpivot_constant::Value;
    let value = match constant {
        UnpivotConstant::Scalar(expression) => {
            if !matches!(
                fragment
                    .expressions()
                    .get(*expression)
                    .map(|node| &node.kind),
                Some(ExprKind::Literal(_))
            ) {
                return Err("native wire v1 Unpivot scalar constant is not a literal".into());
            }
            Value::ScalarLiteral(encode_physical_expr(
                fragment,
                layout,
                owner,
                *expression,
                ValueResolution::NodeInput,
            )?)
        }
        UnpivotConstant::Int32List(values) => Value::Int32List(plan::Int32List {
            values: values.to_vec(),
        }),
        UnpivotConstant::Utf8Map(entries) => Value::Utf8Map(plan::Utf8Map {
            entries: entries
                .iter()
                .map(|(key, value)| plan::Utf8MapEntry {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .collect(),
        }),
    };
    Ok(plan::UnpivotConstant { value: Some(value) })
}

fn encode_unpivot_schema(
    fragment: &Fragment,
    layout: &WireLayout,
    node: &PhysicalNode,
) -> Result<plan::ArrowPhysicalSchema, String> {
    let schema = arrow::datatypes::Schema::new(
        node.output
            .columns
            .iter()
            .map(|value| {
                let ty = &fragment.values()[value].ty;
                arrow::datatypes::Field::new(value_name(*value), ty.data_type.clone(), ty.nullable)
            })
            .collect::<Vec<_>>(),
    );
    let slots = node
        .output
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, _)| {
            layout
                .output_slot(node.id, ordinal_u32(ordinal)?)
                .map(WireSlotId::get_u32)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, String>>()?;
    let (columns, schema_metadata) = arrow_physical::encode_schema(
        &schema,
        &slots,
        false,
        FieldPath::root("unpivot.output_schema"),
    )
    .map_err(|error| error.to_string())?;
    Ok(plan::ArrowPhysicalSchema {
        columns,
        schema_metadata,
    })
}

fn encode_table_finish(
    fragment: &Fragment,
    layout: &WireLayout,
    node: &PhysicalNode,
    spec: &novarocks_physical_plan::WriterFinishSpec,
) -> Result<plan::TableFinishNode, String> {
    let input_node = fragment
        .nodes()
        .get(&node.inputs[0])
        .ok_or_else(|| "table finish input disappeared after validation".to_string())?;
    Ok(plan::TableFinishNode {
        expected_target_ordinals: spec
            .expected_target_ordinals
            .iter()
            .map(|target| target.get())
            .collect(),
        writer_multiplex_schema: Some(
            encode_writer_relation_schema(layout, input_node, &spec.input_schema)
                .map_err(|error| format!("finish input schema: {error}"))?,
        ),
        root_result_schema: Some(
            encode_root_writer_relation_schema(layout, node, &spec.output_schema)
                .map_err(|error| format!("finish output schema: {error}"))?,
        ),
        final_aggregate_plan: Some(plan::WriterFinalAggregatePlan {
            calls: spec
                .final_aggregates
                .iter()
                .map(|call| {
                    Ok(plan::WriterFinalAggregateCall {
                        function_name: wire_function_name(&call.binding.function.function_id)?
                            .into(),
                        resolved_signature: Some(encode_aggregate_signature(&call.binding)?),
                        intermediate_input_slot_id: output_slot_for_value(
                            layout, input_node, call.input,
                        )
                        .map_err(|error| format!("final aggregate input: {error}"))?
                        .get_u32(),
                        final_output_slot_id: output_slot_for_value(layout, node, call.output)?
                            .get_u32(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            unpivot: spec
                .grouped_unpivot
                .as_ref()
                .map(|unpivot| {
                    encode_writer_grouped_unpivot(fragment, layout, node, input_node, unpivot)
                })
                .transpose()?,
        }),
    })
}

fn encode_writer_grouped_unpivot(
    fragment: &Fragment,
    layout: &WireLayout,
    node: &PhysicalNode,
    input_node: &PhysicalNode,
    unpivot: &novarocks_physical_plan::WriterGroupedUnpivotSpec,
) -> Result<plan::WriterGroupedUnpivotPlan, String> {
    Ok(plan::WriterGroupedUnpivotPlan {
        grouping_input_slot_id: output_slot_for_value(layout, input_node, unpivot.grouping_input)?
            .get_u32(),
        grouping_output_slot_id: output_slot_for_value(layout, node, unpivot.grouping_output)?
            .get_u32(),
        passthrough_output_slot_id: output_slot_for_value(
            layout,
            node,
            unpivot.passthrough_output,
        )?
        .get_u32(),
        value_output_slot_id: output_slot_for_value(layout, node, unpivot.value_output)?.get_u32(),
        literal_output_slot_ids: unpivot
            .literal_outputs
            .iter()
            .map(|value| output_slot_for_value(layout, node, *value).map(WireSlotId::get_u32))
            .collect::<Result<Vec<_>, String>>()?,
        mappings: unpivot
            .mappings
            .iter()
            .map(|mapping| {
                Ok(plan::WriterGroupedUnpivotMapping {
                    grouping_key: mapping.write_target_ordinal.get(),
                    // A mapping stacks what this node's own merge produced,
                    // not a column that arrived in it, so its value is
                    // addressed on this node.
                    input_value_slot_id: output_slot_for_value(layout, node, mapping.input)?
                        .get_u32(),
                    constants: mapping
                        .constants
                        .iter()
                        .map(|constant| {
                            encode_unpivot_constant(fragment, layout, node.id, constant)
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        max_output_rows: unpivot.max_output_rows,
        max_output_bytes: unpivot.max_output_bytes,
    })
}

fn encode_writer_relation_schema(
    layout: &WireLayout,
    node: &PhysicalNode,
    schema: &novarocks_physical_plan::WriterRelationSchema,
) -> Result<plan::WriterMultiplexSchema, String> {
    let (columns, schema_metadata) = encode_exact_relation_schema(layout, node, schema)?;
    Ok(plan::WriterMultiplexSchema {
        contract_version: schema.revision,
        columns,
        schema_metadata,
    })
}

fn encode_root_writer_relation_schema(
    layout: &WireLayout,
    node: &PhysicalNode,
    schema: &novarocks_physical_plan::WriterRelationSchema,
) -> Result<plan::RootWriteResultSchema, String> {
    let (columns, schema_metadata) = encode_exact_relation_schema(layout, node, schema)?;
    Ok(plan::RootWriteResultSchema {
        contract_version: schema.revision,
        columns,
        schema_metadata,
    })
}

/// Write one relation schema down by the slots its fragment addresses it by.
///
/// A write relation's fixed columns are addressed by the ids its contract
/// reserves -- the reader knows the contract, not this plan's layout -- and
/// the layout is what puts them there, so this reads the same slots as every
/// other column.
fn encode_exact_relation_schema(
    layout: &WireLayout,
    node: &PhysicalNode,
    schema: &novarocks_physical_plan::WriterRelationSchema,
) -> Result<
    (
        Vec<plan::ArrowPhysicalColumn>,
        Vec<plan::ArrowFieldMetadataEntry>,
    ),
    String,
> {
    let arrow_schema = arrow::datatypes::Schema::new(
        schema
            .fields
            .iter()
            .map(|field| {
                arrow::datatypes::Field::new(
                    field.name.as_ref(),
                    field.ty.data_type.clone(),
                    field.ty.nullable,
                )
            })
            .collect::<Vec<_>>(),
    );
    let slots = schema
        .fields
        .iter()
        .map(|field| output_slot_for_value(layout, node, field.value).map(WireSlotId::get_u32))
        .collect::<Result<Vec<_>, String>>()?;
    arrow_physical::encode_schema(
        &arrow_schema,
        &slots,
        true,
        FieldPath::root("physical_plan.writer_schema"),
    )
    .map_err(|error| error.to_string())
}

/// What the provider calls the field one token names.
fn field_token_name(
    fact: &PhysicalV1WriteFact,
    token: novarocks_spi::connector::ConnectorWriteFieldToken,
) -> Result<String, String> {
    fact.field_names
        .get(&token.to_bytes())
        .map(|name| name.as_ref().to_string())
        .ok_or_else(|| {
            format!(
                "writer target schema names field {} which the write target does not accept; it accepts {:?}",
                token.to_bytes().iter().map(|b| format!("{b:02x}")).collect::<String>(),
                fact.field_names
                    .iter()
                    .map(|(token, name)| format!("{}={name}", token.iter().map(|b| format!("{b:02x}")).collect::<String>()))
                    .collect::<Vec<_>>()
            )
        })
}

fn encode_exchange_source(
    physical: &PhysicalPlan,
    fragment: &Fragment,
    layout: &WireLayout,
    node: &PhysicalNode,
    edge_id: EdgeId,
    names: &OutputValueNames,
) -> Result<plan::ExchangeReceiver, String> {
    let edge = &physical.edges()[&edge_id];
    let exact_scope = node
        .output
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, value)| {
            layout
                .output_slot(node.id, ordinal_u32(ordinal)?)
                .map(|slot| (*value, slot))
                .map_err(|error| error.to_string())
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    Ok(plan::ExchangeReceiver {
        partition_type: encode_partition_type(&edge.partitioning.destination),
        partition_exprs: distribution_keys(&edge.partitioning.destination)
            .iter()
            .map(|value| value_expr(fragment, *value, exact_scope[value]))
            .collect::<Result<Vec<_>, String>>()?,
        source_fragment_id: edge.source.fragment.get(),
        output_columns: output_columns(names, fragment, layout, node)?,
        output_qualifier: None,
        flavor: Some(plan::ExchangeFlavor {
            kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
        }),
    })
}

fn encode_sink(
    physical: &PhysicalPlan,
    fragment: &Fragment,
    layout: &WireLayout,
) -> Result<plan::DataSink, String> {
    use plan::data_sink::Kind;

    let kind = match fragment.sink() {
        FragmentSink::Result => Kind::Result(true),
        FragmentSink::Stream { edge } => {
            Kind::DataStream(encode_stream_sink(physical, fragment, layout, *edge)?)
        }
        FragmentSink::Multicast { edges } => {
            Kind::MultiCastDataStream(plan::MultiCastDataStreamSink {
                sinks: edges
                    .iter()
                    .map(|edge| encode_stream_sink(physical, fragment, layout, *edge))
                    .collect::<Result<Vec<_>, String>>()?,
            })
        }
        FragmentSink::Router { effect, routes } => {
            let root = &fragment.nodes()[&fragment.root()];
            let effect_ordinal = output_ordinal(root, *effect)?;
            Kind::ChangeStreamRouter(plan::ChangeStreamRouterSink {
                group_id: wire_group_id(fragment.id())?,
                effect_output_ordinal: u64::from(effect_ordinal),
                routes: routes
                    .iter()
                    .map(|route| {
                        let edge = &physical.edges()[&route.edge];
                        let (input_ordinals, partition_ordinals) =
                            router_route_ordinals(root, route)?;
                        Ok(plan::ChangeStreamBranchRoute {
                            target_fragment_id: edge.destination.fragment.get(),
                            target_exchange_node_id: i32::try_from(edge.destination.node.get())
                                .map_err(|_| "router destination node exceeds i32".to_string())?,
                            output_partition_ordinals: partition_ordinals,
                            output_partition: Some(encode_data_partition(
                                fragment,
                                layout,
                                root,
                                &edge.partitioning.source,
                                true,
                            )?),
                            route_id: route.route_id.to_bytes().to_vec(),
                            accepted_effects: route
                                .accepted_effects
                                .iter()
                                .copied()
                                .map(encode_effect)
                                .collect(),
                            input_ordinals,
                            write_target_ordinal: route.write_target_ordinal.get(),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            })
        }
        FragmentSink::SealedArtifact(_) | FragmentSink::Noop => {
            unreachable!("shared preflight rejects these sinks")
        }
    };
    Ok(plan::DataSink { kind: Some(kind) })
}

fn encode_stream_sink(
    physical: &PhysicalPlan,
    fragment: &Fragment,
    layout: &WireLayout,
    edge_id: EdgeId,
) -> Result<plan::DataStreamSink, String> {
    let edge = &physical.edges()[&edge_id];
    let slots = layout
        .project_output(fragment, fragment.root(), &edge.source.projection)
        .map_err(|error| error.to_string())?;
    Ok(plan::DataStreamSink {
        dest_node_id: i32::try_from(edge.destination.node.get())
            .map_err(|_| "stream destination node exceeds i32".to_string())?,
        target_fragment_id: edge.destination.fragment.get(),
        output_partition: Some(encode_data_partition(
            fragment,
            layout,
            &fragment.nodes()[&fragment.root()],
            &edge.partitioning.source,
            true,
        )?),
        output_columns: slots.into_iter().map(WireSlotId::get).collect(),
        limit: None,
    })
}

fn encode_edge(
    physical: &PhysicalPlan,
    edge: &Edge,
    layouts: &BTreeMap<FragmentId, WireLayout>,
) -> Result<plan::FragmentEdge, String> {
    let source = &physical.fragments()[&edge.source.fragment];
    let output_slot_ids = layouts[&edge.source.fragment]
        .project_output(source, source.root(), &edge.source.projection)
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(WireSlotId::get)
        .collect::<Vec<_>>();
    let kind = match edge.kind {
        EdgeKind::Stream => plan::fragment_edge_kind::Kind::Stream(true),
        EdgeKind::CteMulticast => {
            plan::fragment_edge_kind::Kind::CteMulticast(plan::CteMulticastEdge {
                cte_id: edge.source.fragment.get(),
                receive_producer_column_ids: output_slot_ids_u32(&output_slot_ids)?,
            })
        }
        EdgeKind::ChangeStreamRouter => {
            plan::fragment_edge_kind::Kind::ChangeStreamRouter(plan::ChangeStreamRouterEdge {
                router_group_id: wire_group_id(edge.source.fragment)?,
                route_id: route_id_for_edge(physical, edge)?.to_bytes().to_vec(),
            })
        }
    };
    Ok(plan::FragmentEdge {
        source_fragment_id: edge.source.fragment.get(),
        target_fragment_id: edge.destination.fragment.get(),
        target_exchange_node_id: i32::try_from(edge.destination.node.get())
            .map_err(|_| "edge destination node exceeds i32".to_string())?,
        output_partition: encode_partition_type(&edge.partitioning.source),
        stream_kind: encode_stream_kind(&edge.partitioning.destination),
        edge_kind: Some(plan::FragmentEdgeKind { kind: Some(kind) }),
        output_slot_ids,
    })
}

fn route_id_for_edge<'a>(
    physical: &'a PhysicalPlan,
    edge: &Edge,
) -> Result<&'a novarocks_physical_plan::ConnectorWriteRouteId, String> {
    let source = &physical.fragments()[&edge.source.fragment];
    let FragmentSink::Router { routes, .. } = source.sink() else {
        return Err(format!(
            "change-stream edge {} has no router source sink",
            edge.id.get()
        ));
    };
    routes
        .iter()
        .find(|route| route.edge == edge.id)
        .map(|route| &route.route_id)
        .ok_or_else(|| format!("change-stream edge {} has no exact route", edge.id.get()))
}

fn encode_data_partition(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: &PhysicalNode,
    distribution: &Distribution,
    output_scope: bool,
) -> Result<plan::DataPartition, String> {
    let kind = encode_data_partition_kind(distribution);
    let exact = output_scope
        .then(|| output_scope_map(layout, owner))
        .transpose()?;
    let exprs = distribution_keys(distribution)
        .iter()
        .map(|value| match &exact {
            Some(values) => value_expr(fragment, *value, values[value]),
            None => encode_physical_value(fragment, layout, owner.id, *value),
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(plan::DataPartition {
        kind: kind as i32,
        exprs,
    })
}

fn encode_data_partition_kind(distribution: &Distribution) -> plan::PartitionKind {
    match distribution {
        Distribution::Singleton | Distribution::Broadcast => plan::PartitionKind::Unpartitioned,
        Distribution::RoundRobin | Distribution::Unconstrained => plan::PartitionKind::Random,
        Distribution::Hash { .. } | Distribution::BucketShuffle { .. } => plan::PartitionKind::Hash,
    }
}

fn encode_physical_value(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    value: ValueId,
) -> Result<expr::Expr, String> {
    let slot = layout
        .input_value_slot(owner, value)
        .map_err(|error| error.to_string())?;
    value_expr(fragment, value, slot)
}

fn value_expr(fragment: &Fragment, value: ValueId, slot: WireSlotId) -> Result<expr::Expr, String> {
    let ty = &fragment.values()[&value].ty;
    Ok(expr::Expr {
        r#type: Some(encode_physical_type(&ty.data_type)?),
        nullable: ty.nullable,
        kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
            column_id: slot.get_u32(),
            qualifier: None,
            column: None,
        })),
    })
}

fn output_scope_map(
    layout: &WireLayout,
    node: &PhysicalNode,
) -> Result<BTreeMap<ValueId, WireSlotId>, String> {
    node.output
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, value)| {
            layout
                .output_slot(node.id, ordinal_u32(ordinal)?)
                .map(|slot| (*value, slot))
                .map_err(|error| error.to_string())
        })
        .collect()
}

/// The result port, and the user-facing name each fragment's output value
/// carries.
struct OutputValueNames<'a> {
    result: Option<&'a ResultPort>,
    by_value: BTreeMap<(FragmentId, ValueId), Box<str>>,
    /// The name each output occurrence of a node carries.
    ///
    /// A value is not enough to name a column: `SELECT x AS a, x AS b` is one
    /// value published twice, and the two columns have different names. So a
    /// name that travels between fragments travels by position, which is what
    /// an edge pairs.
    by_output: BTreeMap<(FragmentId, NodeId, u32), Box<str>>,
}

impl OutputValueNames<'_> {
    /// What a produced value is called where a name reaches the client.
    ///
    /// A value the result port never names is called after the value itself:
    /// nothing downstream reads that name, and inventing a semantic one would
    /// put a name in the plan that the statement never gave.
    fn output_name(&self, fragment: FragmentId, value: ValueId) -> String {
        self.by_value
            .get(&(fragment, value))
            .map_or_else(|| value_name(value), |name| name.as_ref().to_string())
    }
}

/// The user-facing name each fragment's output value carries.
///
/// Only the result port names anything. Every other fragment's output is
/// some projection of what eventually reaches it, and the client sees the
/// rows a producer sent, so a name travels backwards along the edges that
/// carry its value. A value no name reaches stays unnamed, which is what
/// `value_name_ref` says about it.
fn result_value_names(physical: &PhysicalPlan) -> OutputValueNames<'_> {
    let mut names = BTreeMap::new();
    let mut occurrences: BTreeMap<(FragmentId, NodeId, u32), Box<str>> = BTreeMap::new();
    // A write-relation column is called what its contract calls it, wherever
    // it appears: the reader that takes those rows knows the contract and not
    // this plan, and the two sides of the exchange between a writer and the
    // node that finishes it must agree without either having named the other.
    for (id, fragment) in physical.fragments() {
        for node in fragment.nodes().values() {
            let schemas: [&novarocks_physical_plan::WriterRelationSchema; 2] = match &node.kind {
                NodeKind::TableWriter { target } => [&target.output_schema, &target.output_schema],
                NodeKind::TableFinish(spec) => [&spec.input_schema, &spec.output_schema],
                _ => continue,
            };
            for schema in schemas {
                for field in &schema.fields {
                    names
                        .entry((*id, field.value))
                        .or_insert_with(|| field.name.clone());
                }
            }
        }
    }
    let Some(result) = physical.result_port() else {
        return OutputValueNames {
            result: None,
            by_value: names,
            by_output: occurrences,
        };
    };
    for (ordinal, field) in result.fields.iter().enumerate() {
        let name: Box<str> = Box::from(field.alias.as_deref().unwrap_or(&field.name));
        names
            .entry((result.fragment, field.value))
            .or_insert_with(|| name.clone());
        if let Ok(ordinal) = u32::try_from(ordinal) {
            occurrences
                .entry((result.fragment, result.output.node, ordinal))
                .or_insert(name);
        }
    }
    // A sink sends its fragment's root output in order and the receiver
    // publishes it in the same order, so a name reaches the fragment that
    // produced the column by the ordinal it stands at.
    loop {
        let mut carried = false;
        for edge in physical.edges().values() {
            let Some(source) = physical.fragments().get(&edge.source.fragment) else {
                continue;
            };
            let root = source.root();
            let width = physical
                .fragments()
                .get(&edge.destination.fragment)
                .and_then(|fragment| fragment.nodes().get(&edge.destination.node))
                .map_or(0, |node| node.output.columns.len());
            for ordinal in 0..width {
                let Ok(ordinal) = u32::try_from(ordinal) else {
                    continue;
                };
                let Some(name) = occurrences
                    .get(&(edge.destination.fragment, edge.destination.node, ordinal))
                    .cloned()
                else {
                    continue;
                };
                if let std::collections::btree_map::Entry::Vacant(slot) =
                    occurrences.entry((edge.source.fragment, root, ordinal))
                {
                    slot.insert(name);
                    carried = true;
                }
            }
        }
        if !carried {
            break;
        }
    }
    // Carry each name one hop at a time until nothing changes. Edges form a
    // DAG and a pass can only add, so this needs no order and terminates.
    loop {
        let mut carried = false;
        for edge in physical.edges().values() {
            for (source_value, destination_value) in &edge.destination.receive_mapping {
                let Some(name) = names
                    .get(&(edge.destination.fragment, *destination_value))
                    .cloned()
                else {
                    continue;
                };
                if let std::collections::btree_map::Entry::Vacant(slot) =
                    names.entry((edge.source.fragment, *source_value))
                {
                    slot.insert(name);
                    carried = true;
                }
            }
        }
        if !carried {
            break;
        }
    }
    OutputValueNames {
        result: Some(result),
        by_value: names,
        by_output: occurrences,
    }
}

fn output_columns(
    names: &OutputValueNames<'_>,
    fragment: &Fragment,
    layout: &WireLayout,
    node: &PhysicalNode,
) -> Result<Vec<common::OutputColumn>, String> {
    let result = names.result;
    node.output
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, value)| {
            let slot = layout
                .output_slot(node.id, ordinal_u32(ordinal)?)
                .map_err(|error| error.to_string())?;
            let result_field = result
                .filter(|result| result.fragment == fragment.id() && result.output.node == node.id)
                .and_then(|result| result.fields.get(ordinal))
                .filter(|field| field.value == *value);
            let name = result_field
                .map(|field| field.alias.as_deref().unwrap_or(&field.name))
                .or_else(|| {
                    names
                        .by_output
                        .get(&(fragment.id(), node.id, ordinal_u32(ordinal).ok()?))
                        .map(std::convert::AsRef::as_ref)
                })
                .or_else(|| {
                    names
                        .by_value
                        .get(&(fragment.id(), *value))
                        .map(std::convert::AsRef::as_ref)
                })
                .unwrap_or_else(|| value_name_ref(*value));
            let ty = &fragment.values()[value].ty;
            let internal = matches!(
                fragment.values()[value].origin,
                ValueOrigin::WriterDerived { .. }
            );
            output_column(slot, name, ty, internal)
        })
        .collect()
}

fn projected_columns(
    fragment: &Fragment,
    layout: &WireLayout,
    node: &PhysicalNode,
    values: &[ValueId],
) -> Result<Vec<common::OutputColumn>, String> {
    let slots = layout
        .project_output(fragment, node.id, values)
        .map_err(|error| error.to_string())?;
    values
        .iter()
        .zip(slots)
        .map(|(value, slot)| {
            output_column(
                slot,
                value_name_ref(*value),
                &fragment.values()[value].ty,
                false,
            )
        })
        .collect()
}

fn output_column(
    slot: WireSlotId,
    name: &str,
    ty: &ValueType,
    internal: bool,
) -> Result<common::OutputColumn, String> {
    let wire_type = if internal {
        // Writer relation values are paired with the mandatory exact
        // ArrowPhysicalSchema on TableWriter/TableFinish. That schema owns
        // execution type identity; this legacy descriptor is only its SQL
        // compatibility projection.
        encode_arrow_authoritative_compatibility_type(&ty.data_type)?
    } else {
        encode_physical_type(&ty.data_type)?
    };
    Ok(common::OutputColumn {
        column_id: slot.get_u32(),
        name: name.into(),
        r#type: Some(wire_type),
        nullable: ty.nullable,
        is_internal: internal,
    })
}

fn output_slot_for_value(
    layout: &WireLayout,
    node: &PhysicalNode,
    value: ValueId,
) -> Result<WireSlotId, String> {
    // A value a node computes without publishing has no position in its port,
    // so it is addressed by the slot the layout gave it instead.
    if let Some(slot) = layout.internal_slot(node.id, value) {
        return Ok(slot);
    }
    let ordinal = output_ordinal(node, value)?;
    layout
        .output_slot(node.id, ordinal)
        .map_err(|error| error.to_string())
}

fn encode_aggregate_signature(
    binding: &AggregateBinding,
) -> Result<plan::ResolvedAggregateSignature, String> {
    let argument_types = binding
        .function
        .argument_types
        .iter()
        .map(|argument| match argument {
            FunctionArgumentType::Value(value) => encode_physical_type(&value.data_type),
            FunctionArgumentType::Lambda { .. } => {
                Err("native wire v1 aggregate signature cannot carry lambda channels".into())
            }
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(plan::ResolvedAggregateSignature {
        overload_identity: binding.function.overload.as_str().into(),
        argument_types,
        intermediate_type: Some(encode_physical_type(&binding.intermediate_type.data_type)?),
        output_type: Some(encode_physical_type(
            &binding.function.result_type.data_type,
        )?),
        state_format_identity: binding.state_format.as_str().into(),
    })
}

/// The one position a node's port publishes a value at.
///
/// A port is positional: the layout hands every occurrence its own slot, so a
/// value a node publishes twice has no single slot and nothing may resolve it
/// by name. The plan states where each of those occurrences belongs -- an
/// aggregate's group keys stand at its leading ordinals -- and the encoder
/// reads them there through [`output_slot_at`] rather than searching.
fn output_ordinal(node: &PhysicalNode, value: ValueId) -> Result<u32, String> {
    let mut occurrences = node
        .output
        .columns
        .iter()
        .enumerate()
        .filter(|(_, candidate)| **candidate == value)
        .map(|(ordinal, _)| ordinal);
    let ordinal = occurrences.next().ok_or_else(|| {
        format!(
            "node {} ({}) output omits value {}; it publishes {:?}",
            node.id.get(),
            node_kind_name(&node.kind),
            value.get(),
            node.output
                .columns
                .iter()
                .map(|value| value.get())
                .collect::<Vec<_>>()
        )
    })?;
    if occurrences.next().is_some() {
        return Err(format!(
            "node {} output publishes value {} more than once, so it has no one slot",
            node.id.get(),
            value.get()
        ));
    }
    ordinal_u32(ordinal)
}

/// The slot a node's port publishes one stated ordinal at.
fn output_slot_at(
    layout: &WireLayout,
    node: &PhysicalNode,
    ordinal: usize,
    expected: ValueId,
) -> Result<WireSlotId, String> {
    match node.output.columns.get(ordinal) {
        Some(value) if *value == expected => layout
            .output_slot(node.id, ordinal_u32(ordinal)?)
            .map_err(|error| error.to_string()),
        _ => Err(format!(
            "node {} output ordinal {} is not value {}",
            node.id.get(),
            ordinal,
            expected.get()
        )),
    }
}

/// The published column a node's port carries at one stated ordinal.
///
/// An aggregate can be the last thing a statement does, in which case its own
/// layout is where the client's column names come from.
fn output_column_at(
    fragment: &Fragment,
    layout: &WireLayout,
    node: &PhysicalNode,
    ordinal: usize,
    expected: ValueId,
    names: &OutputValueNames<'_>,
) -> Result<common::OutputColumn, String> {
    output_column(
        output_slot_at(layout, node, ordinal, expected)?,
        &names.output_name(fragment.id(), expected),
        &fragment.values()[&expected].ty,
        false,
    )
}

fn ordinal_u32(value: usize) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| "native wire v1 output ordinal exceeds u32".into())
}

fn value_name(value: ValueId) -> String {
    format!("value_{}", value.get())
}

fn value_name_ref(_value: ValueId) -> &'static str {
    // Only diagnostic when the final ResultPort or provider fact does not own a
    // user-facing name. Keeping one constant avoids inventing semantic names.
    "value"
}

fn distribution_keys(distribution: &Distribution) -> &[ValueId] {
    match distribution {
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. } => keys,
        _ => &[],
    }
}

fn encode_partition_type(distribution: &Distribution) -> i32 {
    match distribution {
        Distribution::Singleton => plan::PartitionType::Unpartitioned as i32,
        Distribution::RoundRobin | Distribution::Unconstrained => {
            plan::PartitionType::Random as i32
        }
        Distribution::Broadcast => plan::PartitionType::Unpartitioned as i32,
        Distribution::Hash { .. } => plan::PartitionType::Hash as i32,
        Distribution::BucketShuffle { .. } => plan::PartitionType::BucketShuffleHash as i32,
    }
}

fn encode_stream_kind(distribution: &Distribution) -> i32 {
    match distribution {
        Distribution::Singleton => plan::FragmentStreamKind::Gather as i32,
        Distribution::Broadcast => plan::FragmentStreamKind::Broadcast as i32,
        Distribution::Hash { .. } | Distribution::BucketShuffle { .. } => {
            plan::FragmentStreamKind::Partitioned as i32
        }
        Distribution::RoundRobin | Distribution::Unconstrained => {
            plan::FragmentStreamKind::Other as i32
        }
    }
}

fn encode_join_kind(kind: JoinKind) -> i32 {
    match kind {
        JoinKind::Cross => plan::JoinKind::Cross as i32,
        JoinKind::Inner => plan::JoinKind::Inner as i32,
        JoinKind::LeftOuter => plan::JoinKind::LeftOuter as i32,
        JoinKind::RightOuter => plan::JoinKind::RightOuter as i32,
        JoinKind::FullOuter => plan::JoinKind::FullOuter as i32,
        JoinKind::LeftSemi => plan::JoinKind::LeftSemi as i32,
        JoinKind::RightSemi => plan::JoinKind::RightSemi as i32,
        JoinKind::LeftAnti => plan::JoinKind::LeftAnti as i32,
        JoinKind::RightAnti => plan::JoinKind::RightAnti as i32,
        JoinKind::NullAwareLeftAnti => plan::JoinKind::NullAwareLeftAnti as i32,
    }
}

fn v1_hash_join_build_is_lossless(kind: JoinKind, build_side: JoinSide) -> bool {
    match kind {
        JoinKind::RightSemi | JoinKind::RightAnti => build_side == JoinSide::Left,
        _ => build_side == JoinSide::Right,
    }
}

fn v1_join_output_is_lossless(fragment: &Fragment, node: &PhysicalNode, kind: JoinKind) -> bool {
    let Some(left) = node.inputs.first().and_then(|id| fragment.nodes().get(id)) else {
        return false;
    };
    let Some(right) = node.inputs.get(1).and_then(|id| fragment.nodes().get(id)) else {
        return false;
    };
    let sources = left
        .output
        .columns
        .iter()
        .copied()
        .map(|value| (value, JoinSide::Left))
        .chain(
            right
                .output
                .columns
                .iter()
                .copied()
                .map(|value| (value, JoinSide::Right)),
        )
        .collect::<Vec<_>>();
    let preserved = match kind {
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
            Some(left.output.columns.as_ref())
        }
        JoinKind::RightSemi | JoinKind::RightAnti => Some(right.output.columns.as_ref()),
        _ => None,
    };
    if let Some(preserved) = preserved {
        return !preserved.is_empty()
            && node.output.columns.as_ref() == preserved
            && node.output.columns.iter().all(|value| {
                fragment.values().get(value).is_some_and(|definition| {
                    sources
                        .iter()
                        .find(|(source, _)| source == value)
                        .and_then(|(source, _)| fragment.values().get(source))
                        .is_some_and(|source| definition.ty == source.ty)
                })
            });
    }
    if node.output.columns.is_empty() {
        return sources.is_empty();
    }

    let mut available = BTreeMap::<ValueId, VecDeque<JoinSide>>::new();
    for (source, side) in sources {
        available.entry(source).or_default().push_back(side);
    }
    node.output.columns.iter().copied().all(|actual| {
        let (source, is_null_extended) = match fragment.values().get(&actual) {
            Some(definition) => match definition.origin {
                ValueOrigin::NullExtended { node: owner, of } if owner == node.id => (of, true),
                _ => (actual, false),
            },
            None => return false,
        };
        let Some(side) = available.get_mut(&source).and_then(VecDeque::pop_front) else {
            return false;
        };
        let must_be_nullable = join_side_nullable(kind, side);
        if must_be_nullable != is_null_extended {
            return false;
        }
        let Some(actual_definition) = fragment.values().get(&actual) else {
            return false;
        };
        let Some(source_definition) = fragment.values().get(&source) else {
            return false;
        };
        actual_definition.ty
            == ValueType::new(
                source_definition.ty.data_type.clone(),
                source_definition.ty.nullable || must_be_nullable,
            )
    })
}

fn join_side_nullable(kind: JoinKind, side: JoinSide) -> bool {
    matches!(
        (kind, side),
        (JoinKind::LeftOuter, JoinSide::Right)
            | (JoinKind::RightOuter, JoinSide::Left)
            | (JoinKind::FullOuter, _)
    )
}

fn encode_partition_topn_type(kind: novarocks_physical_plan::PartitionTopNType) -> i32 {
    match kind {
        novarocks_physical_plan::PartitionTopNType::RowNumber => {
            plan::SortTopNType::SortTopnTypeRowNumber as i32
        }
        novarocks_physical_plan::PartitionTopNType::Rank => {
            plan::SortTopNType::SortTopnTypeRank as i32
        }
        novarocks_physical_plan::PartitionTopNType::DenseRank => {
            plan::SortTopNType::SortTopnTypeDenseRank as i32
        }
    }
}

fn v1_partition_topn_limit_is_addressable(limit: u64) -> bool {
    limit != 0 && usize::try_from(limit).is_ok()
}

/// Whether native wire v1 gives this TopN phase back unchanged.
///
/// The wire carries a TopN's phase, its limit and its offset, and a completed
/// plan states each half of a split as its own node, so every phase travels.
/// What the wire cannot carry is a final half collapsed into the merging
/// exchange that feeds it, and no completed plan writes one.
const fn v1_topn_phase_is_lossless(phase: TopNPhase) -> bool {
    let _ = phase;
    true
}

/// Whether native wire v1 gives this Aggregate's phases back unchanged.
///
/// The wire says per call whether it reads values or a state, so the phases
/// themselves travel -- including an intermediate one, which reads a state
/// and writes a state.  What the wire says once for the whole node is whether
/// its calls finalize, so calls that disagree about that cannot travel
/// together.
fn v1_aggregate_phases_are_lossless(calls: &[novarocks_physical_plan::AggregateCall]) -> bool {
    let mut phases = calls
        .iter()
        .map(|call| call.binding.phase.produces_final_result());
    let Some(first) = phases.next() else {
        return true;
    };
    phases.all(|finalizes| finalizes == first)
}

/// Whether native wire v1 gives this Repeat's grouping values back unchanged.
///
/// The wire nulls a grouping column in place: for a set that drops it, the
/// same slot arrives empty. So a null-extended value is the same wire column
/// as the input it replaces, and the plan's separate identity for it survives
/// exactly as long as the node publishes it where its input arrived.
fn v1_repeat_grouping_values_are_lossless(
    fragment: &Fragment,
    node: &PhysicalNode,
    grouping_values: &[(ValueId, ValueId)],
) -> bool {
    if grouping_values.is_empty() {
        return true;
    }
    let Some(input) = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
    else {
        return false;
    };
    let replacements = grouping_values
        .iter()
        .copied()
        .collect::<std::collections::BTreeMap<_, _>>();
    input
        .output
        .columns
        .iter()
        .enumerate()
        .all(|(ordinal, value)| {
            let expected = replacements.get(value).copied().unwrap_or(*value);
            node.output.columns.get(ordinal) == Some(&expected)
        })
}

fn v1_join_build_runtime_filter_domain_is_lossless(
    domain: &novarocks_physical_plan::RuntimeFilterDomain,
) -> bool {
    matches!(
        domain,
        novarocks_physical_plan::RuntimeFilterDomain::Membership { .. }
    )
}

fn encode_join_distribution(distribution: JoinDistribution) -> i32 {
    match distribution {
        JoinDistribution::Colocated => plan::JoinDistribution::Colocate as i32,
        JoinDistribution::Partitioned => plan::JoinDistribution::Shuffle as i32,
        JoinDistribution::BroadcastBuild => plan::JoinDistribution::Broadcast as i32,
        JoinDistribution::Singleton => plan::JoinDistribution::Colocate as i32,
    }
}

fn encode_join_execution(distribution: JoinDistribution) -> i32 {
    match distribution {
        JoinDistribution::Colocated => plan::JoinExecutionMode::Colocate as i32,
        JoinDistribution::Partitioned => plan::JoinExecutionMode::Partitioned as i32,
        JoinDistribution::BroadcastBuild => plan::JoinExecutionMode::Broadcast as i32,
        JoinDistribution::Singleton => plan::JoinExecutionMode::Colocate as i32,
    }
}

fn encode_effect(effect: novarocks_spi::connector::ConnectorRowMutationEffect) -> i32 {
    use novarocks_spi::connector::ConnectorRowMutationEffect;
    match effect {
        ConnectorRowMutationEffect::Delete => plan::RowMutationEffect::Delete as i32,
        ConnectorRowMutationEffect::Replace => plan::RowMutationEffect::Replace as i32,
        ConnectorRowMutationEffect::Insert => plan::RowMutationEffect::Insert as i32,
    }
}

fn encode_assertion(
    layout: &WireLayout,
    node: &PhysicalNode,
    assertion: &RowCountAssertionSpec,
) -> Result<plan::AssertOneRowNode, String> {
    Ok(match assertion {
        RowCountAssertionSpec::Global {
            subject,
            desired_rows,
            comparison,
        } => plan::AssertOneRowNode {
            subquery_text: subject.to_string(),
            desired_num_rows: Some(i64_from_u64(*desired_rows)?),
            assertion: match comparison {
                RowCountAssertion::Eq => plan::RowCountAssertion::Eq as i32,
                RowCountAssertion::Ne => plan::RowCountAssertion::Ne as i32,
                RowCountAssertion::Lt => plan::RowCountAssertion::Lt as i32,
                RowCountAssertion::Le => plan::RowCountAssertion::Le as i32,
                RowCountAssertion::Gt => plan::RowCountAssertion::Gt as i32,
                RowCountAssertion::Ge => plan::RowCountAssertion::Ge as i32,
            },
            group_key_column_ids: Vec::new(),
            group_key_labels: Vec::new(),
            keyed_message_prefix: None,
        },
        RowCountAssertionSpec::PerKeyAtMostOne {
            keys,
            labels,
            message,
        } => plan::AssertOneRowNode {
            subquery_text: String::new(),
            desired_num_rows: Some(1),
            assertion: plan::RowCountAssertion::Le as i32,
            group_key_column_ids: keys
                .iter()
                .map(|value| {
                    layout
                        .input_value_slot(node.id, *value)
                        .map(WireSlotId::get_u32)
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, String>>()?,
            group_key_labels: labels.iter().map(ToString::to_string).collect(),
            keyed_message_prefix: Some(message.to_string()),
        },
    })
}

fn encode_repeat(
    layout: &WireLayout,
    node: &PhysicalNode,
    rollup_keys: &[ValueId],
    grouping_sets: &[Box<[ValueId]>],
    grouping_values: &[(ValueId, ValueId)],
    grouping_outputs: &[novarocks_physical_plan::GroupingOutput],
) -> Result<plan::RepeatNode, String> {
    // The keys, not the ones that go null: a set that keeps every key nulls
    // nothing, and the backend still reads each set against the whole domain.
    let all_inputs = rollup_keys;
    let grouping_ids = grouping_sets
        .iter()
        .map(|set| {
            all_inputs
                .iter()
                .enumerate()
                .fold(0_u64, |bits, (ordinal, value)| {
                    if set.contains(value) {
                        bits
                    } else {
                        bits | (1_u64 << (all_inputs.len() - ordinal - 1))
                    }
                })
        })
        .collect();
    Ok(plan::RepeatNode {
        repeat_column_ref_list: grouping_sets
            .iter()
            .map(|set| plan::StringList {
                values: vec!["value".into(); set.len()],
            })
            .collect(),
        repeat_column_ref_ids: grouping_sets
            .iter()
            .map(|set| {
                Ok(plan::UInt32List {
                    values: set
                        .iter()
                        .map(|value| {
                            layout
                                .input_value_slot(node.id, *value)
                                .map(WireSlotId::get_u32)
                                .map_err(|error| error.to_string())
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        grouping_ids,
        all_rollup_columns: vec!["value".into(); all_inputs.len()],
        all_rollup_column_ids: all_inputs
            .iter()
            .map(|value| {
                layout
                    .input_value_slot(node.id, *value)
                    .map(WireSlotId::get_u32)
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, String>>()?,
        grouping_key_aliases: grouping_values
            .iter()
            .map(|_| plan::StringPair {
                first: "value".into(),
                second: "value".into(),
            })
            .collect(),
        grouping_fn_args: grouping_outputs
            .iter()
            .map(|output| plan::NamedStringList {
                name: "value".into(),
                values: vec!["value".into(); output.arguments.len()],
            })
            .collect(),
        grouping_fn_arg_ids: grouping_outputs
            .iter()
            .map(|output| {
                Ok(plan::UInt32List {
                    values: output
                        .arguments
                        .iter()
                        .map(|value| {
                            layout
                                .input_value_slot(node.id, *value)
                                .map(WireSlotId::get_u32)
                                .map_err(|error| error.to_string())
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        grouping_fn_ids: grouping_outputs
            .iter()
            .map(|output| {
                Ok(plan::NamedUInt32 {
                    name: "value".into(),
                    value: output_slot_for_value(layout, node, output.output)?.get_u32(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        virtual_tuple_id: None,
    })
}

fn int64_literal(fragment: &Fragment, expression: ExprId) -> Result<i64, String> {
    match fragment
        .expressions()
        .get(expression)
        .map(|node| &node.kind)
    {
        Some(ExprKind::Literal(LiteralValue::Int64(value))) => Ok(*value),
        _ => Err("native wire v1 GenerateSeries requires Int64 literal bounds".into()),
    }
}

fn i64_from_u64(value: u64) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| "native wire v1 integer exceeds i64".into())
}

fn preflight_i64(
    value: u64,
    fragment: &Fragment,
    node: &PhysicalNode,
    field: &str,
) -> Result<(), String> {
    i64::try_from(value).map(|_| ()).map_err(|_| {
        format!(
            "native wire v1 fragment {} node {} {field} exceeds i64",
            fragment.id().get(),
            node.id.get()
        )
    })
}

fn wire_group_id(fragment: FragmentId) -> Result<i32, String> {
    i32::try_from(fragment.get())
        .map_err(|_| "native wire v1 router group identity exceeds i32".into())
}

fn output_slot_ids_u32(values: &[i32]) -> Result<Vec<u32>, String> {
    values
        .iter()
        .map(|value| u32::try_from(*value).map_err(|_| "wire slot is not positive".into()))
        .collect()
}

fn node_kind_name(kind: &NodeKind) -> &'static str {
    match kind {
        NodeKind::Aggregate { .. } => "Aggregate",
        NodeKind::Window(_) => "Window",
        NodeKind::Unpivot { .. } => "Unpivot",
        NodeKind::Repeat { .. } => "Repeat",
        NodeKind::TableWriter { .. } => "TableWriter",
        NodeKind::TableFinish(_) => "TableFinish",
        NodeKind::ExchangeSource { .. } => "ExchangeSource",
        _ => "physical node",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use novarocks_functions::{
        EngineFunctionCatalogBuilder, FunctionBindingDeclaration, FunctionBindingError,
        FunctionBindingResolver, FunctionDefinition, FunctionKind, FunctionOverloadDeclaration,
        FunctionOverloadId, FunctionValueType, FunctionVisibility, FunctionVolatility,
    };
    use novarocks_physical_plan::{
        FragmentBuilder, FunctionArgumentEvaluation, FunctionFailureBehavior, FunctionId, JoinKey,
        OutputPort, PhysicalProperties, PipelineDopDomain, PlanBuilder, PlanVersionId, ResultField,
        RowMultiplicity, ValueOrigin,
    };

    use super::*;

    #[derive(Clone)]
    struct ExactResolver {
        selected: FunctionBindingSelection,
    }

    impl FunctionBindingResolver for ExactResolver {
        fn resolve(
            &self,
            _request: FunctionBindingRequest<'_>,
        ) -> Result<FunctionBindingSelection, FunctionBindingError> {
            Ok(self.selected.clone())
        }

        fn validate_selected(
            &self,
            selected: &FunctionBindingSelection,
            _request: FunctionBindingRequest<'_>,
        ) -> Result<(), FunctionBindingError> {
            if selected == &self.selected {
                Ok(())
            } else {
                Err(FunctionBindingError::InvalidBinding(
                    "selected binding differs from test declaration".into(),
                ))
            }
        }
    }

    fn exact_scalar_catalog() -> (
        EngineFunctionCatalog,
        novarocks_physical_plan::BoundFunction,
    ) {
        let value_type = FunctionValueType::new(DataType::Int64, false);
        let function_id = FunctionId::try_new("builtin.scalar/test_identity/v1").unwrap();
        let overload =
            FunctionOverloadId::try_new("builtin.scalar/test_identity/int64/v1").unwrap();
        let semantics = FunctionSemantics {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
        };
        let selected = FunctionBindingSelection {
            overload: overload.clone(),
            argument_types: Box::from([FunctionArgumentType::Value(value_type.clone())]),
            result_type: FunctionResultType::Scalar(value_type.clone()),
            aggregate: None,
        };
        let declaration = FunctionBindingDeclaration::try_new(
            function_id.clone(),
            FunctionKind::Scalar,
            semantics,
            [FunctionOverloadDeclaration {
                identity: overload.clone(),
                argument_pattern: "(Int64)".into(),
                result_pattern: "Int64".into(),
                aggregate: None,
            }],
        )
        .unwrap();
        let definition = FunctionDefinition::try_new_bound(
            "test_identity",
            FunctionVisibility::Public,
            declaration,
            Arc::new(ExactResolver {
                selected: selected.clone(),
            }),
        )
        .unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder.register(definition).unwrap();
        let catalog = builder.seal_bound().unwrap();
        let function = novarocks_physical_plan::BoundFunction {
            function_id,
            overload,
            kind: FunctionKind::Scalar,
            argument_types: selected.argument_types,
            result_type: value_type,
            volatility: semantics.volatility,
            argument_evaluation: semantics.argument_evaluation,
            failure_behavior: semantics.failure_behavior,
        };
        (catalog, function)
    }

    fn validate_test_scalar(
        catalog: &EngineFunctionCatalog,
        function: &novarocks_physical_plan::BoundFunction,
    ) -> Result<(), String> {
        let arguments = [FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int64, false),
            constant: Some(FunctionLiteral::Int64(7)),
        }];
        validate_bound_function(
            catalog,
            function,
            &arguments,
            1,
            FunctionResultType::Scalar(function.result_type.clone()),
            None,
        )
    }

    #[test]
    fn function_preflight_rejects_forged_selected_binding_and_semantics() {
        let (catalog, function) = exact_scalar_catalog();
        validate_test_scalar(&catalog, &function).unwrap();

        let mut forged_overload = function.clone();
        forged_overload.overload =
            FunctionOverloadId::try_new("builtin.scalar/test_identity/forged/v1").unwrap();
        assert!(validate_test_scalar(&catalog, &forged_overload).is_err());

        let mut forged_semantics = function.clone();
        forged_semantics.volatility = FunctionVolatility::Stable;
        assert!(validate_test_scalar(&catalog, &forged_semantics).is_err());

        let mut forged_argument = function.clone();
        forged_argument.argument_types = Box::from([FunctionArgumentType::Value(
            FunctionValueType::new(DataType::Utf8, false),
        )]);
        assert!(validate_test_scalar(&catalog, &forged_argument).is_err());

        let mut forged_result = function;
        forged_result.result_type = FunctionValueType::new(DataType::Utf8, false);
        assert!(validate_test_scalar(&catalog, &forged_result).is_err());
    }

    #[test]
    fn ordered_runtime_filter_digest_matches_canonical_int64_golden() {
        let (comparator, order) = runtime_filter_order_digests(
            &DataType::Int64,
            novarocks_physical_plan::SortDirection::Ascending,
            novarocks_physical_plan::NullOrdering::Last,
        )
        .unwrap();
        assert_eq!(
            hex::encode(comparator),
            "ef75ae29c8dd2bce7ddb857cfff1d89b8afa8c30950bd3afb669ffb52d8755d8"
        );
        assert_eq!(
            hex::encode(order),
            "ee277431b71d9a36652809581e72c910c527d050916ec4c96ac2f4b4636549bb"
        );
        assert!(
            runtime_filter_order_digests(
                &DataType::Float64,
                novarocks_physical_plan::SortDirection::Ascending,
                novarocks_physical_plan::NullOrdering::Last,
            )
            .is_err()
        );
    }

    #[test]
    fn scan_source_seal_rejects_typed_table_and_selection_tampering() {
        use novarocks_physical_plan::{
            DataRelation, ExactInputVersion, ProviderReadOccurrenceId, Relation,
        };
        use novarocks_proto_models::connector_read as wire;
        use novarocks_spi::connector::read_stack::{
            ConnectorReadBinding, ConnectorReadRelationKind,
        };
        use novarocks_spi::connector::{
            CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
            ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
            ConnectorInstanceId, ConnectorProviderId, ConnectorReadRelationPayload,
        };

        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([3; 32]));
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        );
        let revision = ConnectorCodecRevision::try_new(1).unwrap();
        let table_payload = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                provider.clone(),
                catalog.clone(),
                ConnectorCodecCategory::ReadTable,
                revision,
            ),
            vec![1, 2, 3].into(),
        );
        let view_payload = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                provider,
                catalog.clone(),
                ConnectorCodecCategory::ReadView,
                revision,
            ),
            vec![4, 5, 6].into(),
        );
        let read = ProviderReadReference {
            binding,
            input_version: ExactInputVersion::try_new(vec![9]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                table_payload.clone(),
                view_payload.clone(),
            ),
        };
        let relation = Relation::Data(DataRelation {
            read: read.clone(),
            work_source: ConnectorReadWorkSource::RuntimeSplits,
            selection_digest: [7; 32],
            schema: Box::default(),
            predicate_guarantees: Box::default(),
            provided_properties: properties(),
            artifact_inputs: Box::default(),
        });
        let mut source = wire::ConnectorTableScanSource {
            table: Some(wire::CatalogTableHandle {
                catalog_handle: Some(novarocks_proto_codec::catalog::encode_catalog_handle(
                    &catalog,
                )),
                transaction: Some(wire::ConnectorTransactionHandle {
                    provider_payload: Some(
                        novarocks_proto_codec::connector_common::encode_connector_payload_message(
                            &view_payload,
                        ),
                    ),
                }),
                relation: Some(wire::catalog_table_handle::Relation::Table(
                    wire::ConnectorTableHandle {
                        provider_payload: Some(
                            novarocks_proto_codec::connector_common::encode_connector_payload_message(
                                &table_payload,
                            ),
                        ),
                    },
                )),
            }),
            ..Default::default()
        };
        let occurrence = ProviderReadOccurrenceId::new(11);
        let mut fact = PhysicalV1ScanFact {
            occurrence,
            read,
            selection_digest: [7; 32],
            source_seal_digest: physical_v1_scan_source_seal_digest(
                occurrence,
                relation.read(),
                [7; 32],
                &source,
            )
            .unwrap(),
            database: "db".into(),
            alias: None,
            table: plan::TableDef::default(),
            columns: Box::default(),
        };
        preflight_scan_source_identity(&relation, &fact, &source).unwrap();

        source.max_batch_rows = 1;
        assert!(preflight_scan_source_identity(&relation, &fact, &source).is_err());
        fact.source_seal_digest =
            physical_v1_scan_source_seal_digest(occurrence, relation.read(), [7; 32], &source)
                .unwrap();
        let Some(wire::catalog_table_handle::Relation::Table(table)) = source
            .table
            .as_mut()
            .and_then(|table| table.relation.as_mut())
        else {
            panic!("table relation")
        };
        table.provider_payload.as_mut().unwrap().payload.push(99);
        assert!(preflight_scan_source_identity(&relation, &fact, &source).is_err());

        fact.selection_digest = [8; 32];
        assert!(preflight_scan_source_identity(&relation, &fact, &source).is_err());
    }

    #[test]
    fn maximum_width_scan_index_resolves_exact_identities() {
        use novarocks_physical_plan::{ExactInputVersion, ProviderReadOccurrenceId};
        use novarocks_spi::connector::read_stack::{
            ConnectorReadBinding, ConnectorReadRelationKind,
        };
        use novarocks_spi::connector::{
            CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
            ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
            ConnectorInstanceId, ConnectorProviderId, ConnectorReadRelationPayload,
        };

        const WIDTH: usize = 4_096;
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let instance = ConnectorInstanceId::try_from_canonical("wide").unwrap();
        let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([4; 32]));
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        );
        let revision = ConnectorCodecRevision::try_new(1).unwrap();
        let payload = |category, bytes: Vec<u8>| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(provider.clone(), catalog.clone(), category, revision),
                bytes.into(),
            )
        };
        let read = ProviderReadReference {
            binding,
            input_version: ExactInputVersion::try_new(vec![1]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                payload(ConnectorCodecCategory::ReadTable, vec![2]),
                payload(ConnectorCodecCategory::ReadView, vec![3]),
            ),
        };
        let ty = ValueType::new(DataType::Int64, false);
        let mut provider_outputs = Vec::with_capacity(WIDTH);
        let mut columns = Vec::with_capacity(WIDTH);
        let mut table_columns = Vec::with_capacity(WIDTH);
        for ordinal in 0..WIDTH {
            let value = ValueId::new(u32::try_from(ordinal + 1).unwrap());
            let column = ProviderColumnReference {
                column_payload: payload(
                    ConnectorCodecCategory::ReadColumn,
                    u32::try_from(ordinal).unwrap().to_be_bytes().to_vec(),
                ),
            };
            let name = format!("c{ordinal}");
            provider_outputs.push((column.clone(), value));
            columns.push(PhysicalV1ScanColumn {
                column,
                name: name.clone().into(),
                ty: ty.clone(),
                connector_type: ConnectorValueType::BigInt,
                internal: false,
            });
            table_columns.push(plan::ColumnDef {
                name,
                data_type: Some(encode_physical_type(&DataType::Int64).unwrap()),
                nullable: false,
                write_default_json: None,
                logical_type: None,
            });
        }
        let fact = PhysicalV1ScanFact {
            occurrence: ProviderReadOccurrenceId::new(1),
            read,
            selection_digest: [0; 32],
            source_seal_digest: [0; 32],
            database: "db".into(),
            alias: None,
            table: plan::TableDef {
                name: "wide".into(),
                columns: table_columns,
                ..Default::default()
            },
            columns: columns.into_boxed_slice(),
        };
        let physical = finish_limit_chain_plan(1);
        let fragment = physical.fragments().values().next().unwrap();
        let node = &fragment.nodes()[&fragment.root()];
        let index = ScanColumnIndex::try_new(fragment, node, &provider_outputs, &fact).unwrap();
        for (ordinal, (_, value)) in provider_outputs.iter().enumerate() {
            let (actual_ordinal, column) = index.fact_column_for_value(*value).unwrap();
            assert_eq!(actual_ordinal, ordinal);
            assert_eq!(column.name.as_ref(), format!("c{ordinal}"));
            assert!(index.table_column(false, &column.name).is_some());
        }
    }

    #[test]
    fn partition_topn_preserves_all_v1_ranking_kinds_and_checked_limits() {
        use novarocks_physical_plan::PartitionTopNType;

        assert_eq!(
            encode_partition_topn_type(PartitionTopNType::RowNumber),
            plan::SortTopNType::SortTopnTypeRowNumber as i32
        );
        assert_eq!(
            encode_partition_topn_type(PartitionTopNType::Rank),
            plan::SortTopNType::SortTopnTypeRank as i32
        );
        assert_eq!(
            encode_partition_topn_type(PartitionTopNType::DenseRank),
            plan::SortTopNType::SortTopnTypeDenseRank as i32
        );
        assert!(!v1_partition_topn_limit_is_addressable(0));
        assert!(v1_partition_topn_limit_is_addressable(1));
        #[cfg(target_pointer_width = "32")]
        assert!(!v1_partition_topn_limit_is_addressable(u64::MAX));
    }

    #[test]
    fn a_split_topn_travels_as_two_nodes_neither_of_them_collapsed() {
        use novarocks_physical_plan::TopNSequenceId;

        let sequence = TopNSequenceId::new(1);
        assert!(v1_topn_phase_is_lossless(TopNPhase::Single));
        assert!(v1_topn_phase_is_lossless(TopNPhase::Partial { sequence }));
        assert!(v1_topn_phase_is_lossless(TopNPhase::Final { sequence }));

        let physical = finish_split_topn_plan();
        let (catalog, _) = exact_scalar_catalog();
        let encoded = encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect("a split TopN states both halves as nodes of its own");
        let mut phases = Vec::new();
        for fragment in &encoded.fragments {
            let mut pending = fragment.root.iter().collect::<Vec<_>>();
            while let Some(node) = pending.pop() {
                pending.extend(node.children.iter());
                if let Some(plan::distributed_node::Payload::Physical(physical)) =
                    node.payload.as_ref()
                    && let Some(plan::plan_node::Kind::Topn(topn)) = physical.kind.as_ref()
                {
                    assert!(!topn.is_split, "no half of this split is collapsed");
                    phases.push(topn.phase);
                }
            }
        }
        phases.sort_unstable();
        assert_eq!(
            phases,
            vec![
                plan::TopNPhase::TopnPhasePartial as i32,
                plan::TopNPhase::TopnPhaseFinal as i32,
            ]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_v1_aggregate_agrees_with_itself_about_finalizing() {
        use novarocks_physical_plan::AggregateSequenceId;

        let sequence = AggregateSequenceId::new(1);
        assert!(!AggregatePhase::Partial { sequence }.produces_final_result());
        assert!(!AggregatePhase::Intermediate { sequence }.produces_final_result());
        assert!(AggregatePhase::Single.produces_final_result());
        assert!(AggregatePhase::Final { sequence }.produces_final_result());
    }

    #[test]
    fn ordered_join_build_runtime_filter_is_not_a_v1_hash_join_contract() {
        use novarocks_physical_plan::OrderedComparisonAlgorithm;
        use novarocks_physical_plan::{
            RuntimeFilterDomain, RuntimeFilterNullSemantics, RuntimeFilterOrderKey,
        };

        assert!(v1_join_build_runtime_filter_domain_is_lossless(
            &RuntimeFilterDomain::Membership {
                ty: ValueType::new(DataType::Int64, false),
                null_semantics: RuntimeFilterNullSemantics::NeverMatches,
            }
        ));
        assert!(!v1_join_build_runtime_filter_domain_is_lossless(
            &RuntimeFilterDomain::Ordered {
                key: RuntimeFilterOrderKey {
                    ty: ValueType::new(DataType::Int64, false),
                    direction: novarocks_physical_plan::SortDirection::Ascending,
                    null_ordering: novarocks_physical_plan::NullOrdering::Last,
                },
                inclusive: true,
                comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
            }
        ));

        let physical = finish_ordered_join_build_filter_plan();
        let (catalog, _) = exact_scalar_catalog();
        let error = encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect_err("ordered JoinBuildKey must fail in encoder preflight");
        assert!(
            error.contains("cannot attach ordered runtime-filter 40 to a HashJoin build key"),
            "{error}"
        );
    }

    #[test]
    fn broadcast_uses_the_v1_broadcast_partition_carrier() {
        assert_eq!(
            encode_data_partition_kind(&Distribution::Broadcast),
            plan::PartitionKind::Unpartitioned
        );
        assert_eq!(
            encode_partition_type(&Distribution::Broadcast),
            plan::PartitionType::Unpartitioned as i32
        );
        assert_eq!(
            encode_stream_kind(&Distribution::Broadcast),
            plan::FragmentStreamKind::Broadcast as i32
        );
    }

    #[test]
    fn hash_join_build_preflight_matches_v1_decoder_semantics() {
        for kind in [
            JoinKind::Inner,
            JoinKind::LeftOuter,
            JoinKind::RightOuter,
            JoinKind::FullOuter,
            JoinKind::LeftSemi,
            JoinKind::LeftAnti,
            JoinKind::NullAwareLeftAnti,
        ] {
            assert!(v1_hash_join_build_is_lossless(kind, JoinSide::Right));
            assert!(!v1_hash_join_build_is_lossless(kind, JoinSide::Left));
        }
        for kind in [JoinKind::RightSemi, JoinKind::RightAnti] {
            assert!(v1_hash_join_build_is_lossless(kind, JoinSide::Left));
            assert!(!v1_hash_join_build_is_lossless(kind, JoinSide::Right));
        }
    }

    #[test]
    fn right_anti_final_plan_encodes_the_preserved_side() {
        let (catalog, _) = exact_scalar_catalog();
        let mut builder = FragmentBuilder::new(FragmentId::new(20));
        let ty = ValueType::new(DataType::Int64, false);

        let left = builder.reserve_node_id().unwrap();
        let left_literal = builder
            .add_expression(left, ty.clone(), ExprKind::Literal(LiteralValue::Int64(1)))
            .unwrap();
        let left_value = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: left,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: left,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: left,
                    columns: Box::from([left_value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([left_literal])]),
                },
            })
            .unwrap();

        let right = builder.reserve_node_id().unwrap();
        let right_literal = builder
            .add_expression(right, ty.clone(), ExprKind::Literal(LiteralValue::Int64(2)))
            .unwrap();
        let right_value = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: right,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: right,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: right,
                    columns: Box::from([right_value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([right_literal])]),
                },
            })
            .unwrap();

        let join = builder.reserve_node_id().unwrap();
        let left_key = builder
            .add_expression(join, ty.clone(), ExprKind::Value(left_value))
            .unwrap();
        let right_key = builder
            .add_expression(join, ty.clone(), ExprKind::Value(right_value))
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: join,
                inputs: Box::from([left, right]),
                required_inputs: Box::from([properties(), properties()]),
                output_properties: properties(),
                output: OutputPort {
                    node: join,
                    columns: Box::from([right_value]),
                },
                kind: NodeKind::HashJoin {
                    kind: JoinKind::RightAnti,
                    keys: Box::from([JoinKey {
                        left: left_key,
                        right: right_key,
                        null_safe: false,
                    }]),
                    build_side: JoinSide::Left,
                    distribution: JoinDistribution::Singleton,
                    residual: None,
                    null_extended: Box::default(),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                join,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([20; 16]).unwrap());
        plan_builder.add_fragment(fragment).unwrap();
        plan_builder
            .set_result_port(ResultPort {
                fragment: FragmentId::new(20),
                output: OutputPort {
                    node: join,
                    columns: Box::from([right_value]),
                },
                fields: Box::from([ResultField {
                    name: "right".into(),
                    alias: None,
                    value: right_value,
                    ty,
                }]),
            })
            .unwrap();
        let physical = plan_builder.finish().unwrap();

        let encoded = encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect("HashJoin RightAnti preserved-side output is representable");
        assert_eq!(encoded.fragments.len(), 1);
    }

    #[test]
    fn cropped_inner_join_output_is_encoded_as_a_wire_projection() {
        let (catalog, _) = exact_scalar_catalog();
        let mut builder = FragmentBuilder::new(FragmentId::new(22));
        let (left, left_value) = append_test_i64_values(&mut builder, 1);
        let (right, right_value) = append_test_i64_values(&mut builder, 2);
        let ty = ValueType::new(DataType::Int64, false);
        let join = builder.reserve_node_id().unwrap();
        let left_key = builder
            .add_expression(join, ty.clone(), ExprKind::Value(left_value))
            .unwrap();
        let right_key = builder
            .add_expression(join, ty.clone(), ExprKind::Value(right_value))
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: join,
                inputs: Box::from([left, right]),
                required_inputs: Box::from([properties(), properties()]),
                output_properties: properties(),
                output: OutputPort {
                    node: join,
                    columns: Box::from([left_value]),
                },
                kind: NodeKind::HashJoin {
                    kind: JoinKind::Inner,
                    keys: Box::from([JoinKey {
                        left: left_key,
                        right: right_key,
                        null_safe: false,
                    }]),
                    build_side: JoinSide::Right,
                    distribution: JoinDistribution::Singleton,
                    residual: None,
                    null_extended: Box::default(),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                join,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([22; 16]).unwrap());
        plan_builder.add_fragment(fragment).unwrap();
        plan_builder
            .set_result_port(ResultPort {
                fragment: FragmentId::new(22),
                output: OutputPort {
                    node: join,
                    columns: Box::from([left_value]),
                },
                fields: Box::from([ResultField {
                    name: "left".into(),
                    alias: None,
                    value: left_value,
                    ty,
                }]),
            })
            .unwrap();
        let physical = plan_builder.finish().unwrap();
        let encoded = encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect("the v1 backend projects cropped HashJoin output columns");
        assert_eq!(encoded.fragments.len(), 1);
    }

    fn properties() -> PhysicalProperties {
        PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        }
    }

    fn finish_limit_chain_plan(depth: usize) -> PhysicalPlan {
        assert!(depth > 0);
        let fragment_id = FragmentId::new(23);
        let mut builder = FragmentBuilder::new(fragment_id);
        let ty = ValueType::new(DataType::Int64, false);
        let values = builder.reserve_node_id().unwrap();
        let literal = builder
            .add_expression(
                values,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(1)),
            )
            .unwrap();
        let value = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: values,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: values,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: values,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([literal])]),
                },
            })
            .unwrap();

        let mut root = values;
        for _ in 1..depth {
            let limit = builder.reserve_node_id().unwrap();
            builder
                .insert_node_unchecked(PhysicalNode {
                    id: limit,
                    inputs: Box::from([root]),
                    required_inputs: Box::from([properties()]),
                    output_properties: properties(),
                    output: OutputPort {
                        node: limit,
                        columns: Box::from([value]),
                    },
                    kind: NodeKind::Limit {
                        limit: Some(1),
                        offset: 0,
                    },
                })
                .unwrap();
            root = limit;
        }
        let fragment = builder
            .finish_definition(
                root,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([23; 16]).unwrap());
        plan_builder.add_fragment(fragment).unwrap();
        plan_builder
            .set_result_port(ResultPort {
                fragment: fragment_id,
                output: OutputPort {
                    node: root,
                    columns: Box::from([value]),
                },
                fields: Box::from([ResultField {
                    name: "value".into(),
                    alias: None,
                    value,
                    ty,
                }]),
            })
            .unwrap();
        plan_builder.finish().unwrap()
    }

    fn finish_project_expression_plan(cast_depth: usize, diamond_depth: usize) -> PhysicalPlan {
        let fragment_id = FragmentId::new(24);
        let mut builder = FragmentBuilder::new(fragment_id);
        let ty = ValueType::new(DataType::Int64, false);
        let values = builder.reserve_node_id().unwrap();
        let literal = builder
            .add_expression(
                values,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(1)),
            )
            .unwrap();
        let input = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: values,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: values,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: values,
                    columns: Box::from([input]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([literal])]),
                },
            })
            .unwrap();

        let project = builder.reserve_node_id().unwrap();
        let mut expression = builder
            .add_expression(project, ty.clone(), ExprKind::Value(input))
            .unwrap();
        for _ in 0..cast_depth {
            expression = builder
                .add_expression(
                    project,
                    ty.clone(),
                    ExprKind::Cast {
                        expr: expression,
                        target: DataType::Int64,
                    },
                )
                .unwrap();
        }
        for _ in 0..diamond_depth {
            expression = builder
                .add_expression(
                    project,
                    ty.clone(),
                    ExprKind::Binary {
                        op: novarocks_physical_plan::BinaryOperator::Add,
                        left: expression,
                        right: expression,
                    },
                )
                .unwrap();
        }
        let output = builder
            .add_value(
                ty.clone(),
                ValueOrigin::Expr {
                    node: project,
                    expr: expression,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: project,
                inputs: Box::from([values]),
                required_inputs: Box::from([properties()]),
                output_properties: properties(),
                output: OutputPort {
                    node: project,
                    columns: Box::from([output]),
                },
                kind: NodeKind::Project {
                    expressions: Box::from([(expression, output)]),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                project,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([24; 16]).unwrap());
        plan_builder.add_fragment(fragment).unwrap();
        plan_builder
            .set_result_port(ResultPort {
                fragment: fragment_id,
                output: OutputPort {
                    node: project,
                    columns: Box::from([output]),
                },
                fields: Box::from([ResultField {
                    name: "value".into(),
                    alias: None,
                    value: output,
                    ty,
                }]),
            })
            .unwrap();
        plan_builder.finish().unwrap()
    }

    fn writer_schema_carrier(
        list_depth: usize,
        node_depth: usize,
    ) -> (
        novarocks_physical_plan::Fragment,
        NodeId,
        novarocks_physical_plan::WriterRelationSchema,
    ) {
        let mut data_type = DataType::Int64;
        for _ in 0..list_depth {
            data_type = DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item", data_type, true,
            )));
        }
        let fragment_id = FragmentId::new(25);
        let mut builder = FragmentBuilder::new(fragment_id);
        let node = builder.reserve_node_id().unwrap();
        let ty = ValueType::new(data_type, true);
        let value = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Values {
                    rows: Box::default(),
                },
            })
            .unwrap();
        let mut root = node;
        for _ in 1..node_depth {
            let limit = builder.reserve_node_id().unwrap();
            builder
                .insert_node_unchecked(PhysicalNode {
                    id: limit,
                    inputs: Box::from([root]),
                    required_inputs: Box::from([properties()]),
                    output_properties: properties(),
                    output: OutputPort {
                        node: limit,
                        columns: Box::from([value]),
                    },
                    kind: NodeKind::Limit {
                        limit: Some(1),
                        offset: 0,
                    },
                })
                .unwrap();
            root = limit;
        }
        let fragment = builder
            .finish_definition(
                root,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let schema = novarocks_physical_plan::WriterRelationSchema {
            revision: novarocks_physical_plan::WRITER_MULTIPLEX_SCHEMA_REVISION,
            fields: Box::from([novarocks_physical_plan::WriterRelationField {
                value,
                name: "nested".into(),
                ty,
                role: novarocks_physical_plan::WriterRelationFieldRole::Auxiliary,
            }]),
        };
        (fragment, node, schema)
    }

    #[test]
    fn decoder_safe_tree_depth_encodes_and_prost_decodes() {
        let physical = finish_limit_chain_plan(crate::NATIVE_V1_MAX_TREE_DEPTH);
        let (catalog, _) = exact_scalar_catalog();
        let encoded = encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect("the decoder-safe boundary is encodable");
        let bytes = encoded.encode_to_vec();
        let decoded = plan::DistributedPlan::decode(bytes.as_slice())
            .expect("the production prost decoder accepts the boundary");
        assert_eq!(decoded, encoded);
    }

    #[test]
    fn tree_depth_over_decoder_safe_boundary_fails_in_preflight() {
        let physical = finish_limit_chain_plan(crate::NATIVE_V1_MAX_TREE_DEPTH + 1);
        let fragment = physical.fragments().values().next().unwrap();
        let preflight = preflight_physical_plan_v1(&physical).unwrap_err();
        assert!(matches!(
            preflight,
            crate::PhysicalV1PreflightError::TreeDepth { .. }
        ));
        assert!(matches!(
            WireLayout::try_new(fragment),
            Err(crate::WireLayoutError::TreeDepthExceeded { .. })
        ));
        let (catalog, _) = exact_scalar_catalog();
        let error = encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect_err("preflight must reject before layout or protobuf tree construction");
        assert!(error.contains("decoder-safe maximum"));
    }

    #[test]
    fn combined_expression_depth_boundary_encodes_and_prost_decodes() {
        let physical = finish_project_expression_plan(41, 0);
        let (catalog, _) = exact_scalar_catalog();
        let encoded = encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect("the combined decoder-safe expression boundary is encodable");
        let bytes = encoded.encode_to_vec();
        let decoded = plan::DistributedPlan::decode(bytes.as_slice())
            .expect("the production prost decoder accepts the expression boundary");
        assert_eq!(decoded, encoded);
    }

    #[test]
    fn combined_expression_depth_over_boundary_fails_before_encoding() {
        let physical = finish_project_expression_plan(42, 0);
        let (catalog, _) = exact_scalar_catalog();
        let error = encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect_err("combined node and expression nesting must fail closed");
        assert!(error.contains("message depth"), "{error}");
    }

    #[test]
    fn diamond_expression_expansion_fails_before_tree_materialization() {
        let physical = finish_project_expression_plan(0, 15);
        let (catalog, _) = exact_scalar_catalog();
        let error = encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect_err("an exponentially expanded v1 expression must fail closed");
        assert!(error.contains("expanded expressions"), "{error}");
    }

    #[test]
    fn large_wide_expression_preflight_visits_each_reference_once() {
        const WIDTH: usize = 16_384;
        let fragment_id = FragmentId::new(26);
        let mut builder = FragmentBuilder::new(fragment_id);
        let ty = ValueType::new(DataType::Int64, false);
        let values = builder.reserve_node_id().unwrap();
        let literal = builder
            .add_expression(
                values,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(1)),
            )
            .unwrap();
        let input = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: values,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: values,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: values,
                    columns: Box::from([input]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([literal])]),
                },
            })
            .unwrap();
        let project = builder.reserve_node_id().unwrap();
        let mut expressions = Vec::with_capacity(WIDTH);
        let mut outputs = Vec::with_capacity(WIDTH);
        for _ in 0..WIDTH {
            let expression = builder
                .add_expression(project, ty.clone(), ExprKind::Value(input))
                .unwrap();
            let output = builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::Expr {
                        node: project,
                        expr: expression,
                    },
                )
                .unwrap();
            expressions.push((expression, output));
            outputs.push(output);
        }
        builder
            .insert_node_unchecked(PhysicalNode {
                id: project,
                inputs: Box::from([values]),
                required_inputs: Box::from([properties()]),
                output_properties: properties(),
                output: OutputPort {
                    node: project,
                    columns: outputs.into_boxed_slice(),
                },
                kind: NodeKind::Project {
                    expressions: expressions.clone().into_boxed_slice(),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                project,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let mut preflight = WireExpressionPreflight::try_new(&fragment).unwrap();
        for (expression, _) in expressions {
            preflight.charge(expression, 9).unwrap();
        }
    }

    #[test]
    fn combined_writer_type_boundary_prost_decodes() {
        let (fragment, writer_node, schema) = writer_schema_carrier(31, 24);
        let node = &fragment.nodes()[&writer_node];
        validate_writer_wire_depth(&fragment, node, 24, &schema)
            .expect("the combined writer type boundary is decoder-safe");
        let layout = WireLayout::try_new(&fragment).unwrap();
        let writer_schema = encode_writer_relation_schema(&layout, node, &schema).unwrap();
        let mut root = plan::DistributedNode {
            node_id: i32::try_from(node.id.get()).unwrap(),
            fragment_id: fragment.id().get(),
            limit: -1,
            payload: Some(plan::distributed_node::Payload::TableWriter(
                plan::TableWriterNode {
                    writer_multiplex_schema: Some(writer_schema),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        for depth in 1_i32..24 {
            root = plan::DistributedNode {
                node_id: depth,
                fragment_id: fragment.id().get(),
                limit: -1,
                children: vec![root],
                payload: Some(plan::distributed_node::Payload::Physical(
                    plan::PlanNode::default(),
                )),
                ..Default::default()
            };
        }
        let wire = plan::DistributedPlan {
            fragments: vec![plan::PlanFragment {
                fragment_id: fragment.id().get(),
                root: Some(root),
                ..Default::default()
            }],
            root_fragment_id: fragment.id().get(),
            edges: Vec::new(),
        };
        let bytes = wire.encode_to_vec();
        let decoded = plan::DistributedPlan::decode(bytes.as_slice())
            .expect("the production prost decoder accepts the writer type boundary");
        assert_eq!(decoded, wire);
    }

    #[test]
    fn combined_writer_type_over_boundary_fails_before_encoding() {
        let (fragment, writer_node, schema) = writer_schema_carrier(31, 25);
        let node = &fragment.nodes()[&writer_node];
        let error = validate_writer_wire_depth(&fragment, node, 25, &schema)
            .expect_err("combined node and writer type nesting must fail closed");
        assert!(error.contains("message depth"), "{error}");
    }

    fn append_test_i64_values(
        builder: &mut FragmentBuilder,
        literal_value: i64,
    ) -> (NodeId, ValueId) {
        let node = builder.reserve_node_id().unwrap();
        let ty = ValueType::new(DataType::Int64, false);
        let literal = builder
            .add_expression(
                node,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(literal_value)),
            )
            .unwrap();
        let value = builder
            .add_value(
                ty,
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([literal])]),
                },
            })
            .unwrap();
        (node, value)
    }

    fn test_hash_scheme(seed: u8) -> novarocks_physical_plan::HashPartitionScheme {
        novarocks_physical_plan::HashPartitionScheme {
            space: novarocks_physical_plan::PartitionSpaceId::try_new([seed; 32]).unwrap(),
            count: novarocks_physical_plan::PartitionCountParameter {
                id: novarocks_physical_plan::PartitionCountParameterId::try_new(
                    [seed.wrapping_add(1); 32],
                )
                .unwrap(),
                admissible: novarocks_physical_plan::PartitionCountDomain {
                    min: 1,
                    max: 64,
                    requires_power_of_two: true,
                },
            },
            definition: novarocks_physical_plan::HashDefinition::native_exchange(),
        }
    }

    fn finish_split_topn_plan() -> PhysicalPlan {
        use novarocks_physical_plan::{
            EdgeDestination, EdgePartitioning, EdgeSource, NullOrdering, OrderingKey,
            SortDirection, SortExpr, TopNSequenceId,
        };

        let source_fragment = FragmentId::new(30);
        let partial_fragment = FragmentId::new(31);
        let final_fragment = FragmentId::new(32);
        let partial_edge = EdgeId::new(30);
        let final_edge = EdgeId::new(31);
        let ty = ValueType::new(DataType::Int64, false);

        let mut source_builder = FragmentBuilder::new(source_fragment);
        let (source_node, source_value) = append_test_i64_values(&mut source_builder, 7);
        let source = source_builder
            .finish_definition(
                source_node,
                FragmentSink::Stream { edge: partial_edge },
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();

        let mut partial_builder = FragmentBuilder::new(partial_fragment);
        let partial_source = partial_builder.reserve_node_id().unwrap();
        let partial_value = partial_builder
            .add_value(
                ty.clone(),
                ValueOrigin::ExchangeImport {
                    edge: partial_edge,
                    source_value,
                },
            )
            .unwrap();
        let partial_distribution = Distribution::Hash {
            keys: Box::from([partial_value]),
            scheme: test_hash_scheme(30),
        };
        let partial_input_properties = PhysicalProperties {
            distribution: partial_distribution.clone(),
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        partial_builder
            .insert_node_unchecked(PhysicalNode {
                id: partial_source,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: partial_input_properties.clone(),
                output: OutputPort {
                    node: partial_source,
                    columns: Box::from([partial_value]),
                },
                kind: NodeKind::ExchangeSource {
                    edge: partial_edge,
                    imports: Box::from([(source_value, partial_value)]),
                },
            })
            .unwrap();
        let partial_topn = partial_builder.reserve_node_id().unwrap();
        let partial_order = partial_builder
            .add_expression(partial_topn, ty.clone(), ExprKind::Value(partial_value))
            .unwrap();
        let sequence = TopNSequenceId::new(1);
        partial_builder
            .insert_node_unchecked(PhysicalNode {
                id: partial_topn,
                inputs: Box::from([partial_source]),
                required_inputs: Box::from([partial_input_properties]),
                output_properties: PhysicalProperties {
                    distribution: partial_distribution.clone(),
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::from([OrderingKey {
                        value: partial_value,
                        direction: SortDirection::Ascending,
                        null_ordering: NullOrdering::Last,
                    }]),
                },
                output: OutputPort {
                    node: partial_topn,
                    columns: Box::from([partial_value]),
                },
                kind: NodeKind::TopN {
                    order_by: Box::from([SortExpr {
                        expr: partial_order,
                        direction: SortDirection::Ascending,
                        null_ordering: NullOrdering::Last,
                    }]),
                    limit: 10,
                    offset: 0,
                    phase: TopNPhase::Partial { sequence },
                },
            })
            .unwrap();
        let partial = partial_builder
            .finish_definition(
                partial_topn,
                FragmentSink::Stream { edge: final_edge },
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();

        let mut final_builder = FragmentBuilder::new(final_fragment);
        let final_source = final_builder.reserve_node_id().unwrap();
        let final_value = final_builder
            .add_value(
                ty.clone(),
                ValueOrigin::ExchangeImport {
                    edge: final_edge,
                    source_value: partial_value,
                },
            )
            .unwrap();
        final_builder
            .insert_node_unchecked(PhysicalNode {
                id: final_source,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: final_source,
                    columns: Box::from([final_value]),
                },
                kind: NodeKind::ExchangeSource {
                    edge: final_edge,
                    imports: Box::from([(partial_value, final_value)]),
                },
            })
            .unwrap();
        let final_topn = final_builder.reserve_node_id().unwrap();
        let final_order = final_builder
            .add_expression(final_topn, ty.clone(), ExprKind::Value(final_value))
            .unwrap();
        final_builder
            .insert_node_unchecked(PhysicalNode {
                id: final_topn,
                inputs: Box::from([final_source]),
                required_inputs: Box::from([properties()]),
                output_properties: PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::from([OrderingKey {
                        value: final_value,
                        direction: SortDirection::Ascending,
                        null_ordering: NullOrdering::Last,
                    }]),
                },
                output: OutputPort {
                    node: final_topn,
                    columns: Box::from([final_value]),
                },
                kind: NodeKind::TopN {
                    order_by: Box::from([SortExpr {
                        expr: final_order,
                        direction: SortDirection::Ascending,
                        null_ordering: NullOrdering::Last,
                    }]),
                    limit: 10,
                    offset: 0,
                    phase: TopNPhase::Final { sequence },
                },
            })
            .unwrap();
        let final_stage = final_builder
            .finish_definition(
                final_topn,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();

        let mut plan = PlanBuilder::new(PlanVersionId::try_new([30; 16]).unwrap());
        plan.add_fragment(source).unwrap();
        plan.add_fragment(partial).unwrap();
        plan.add_fragment(final_stage).unwrap();
        plan.add_edge(Edge {
            id: partial_edge,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                fragment: source_fragment,
                projection: Box::from([source_value]),
            },
            destination: EdgeDestination {
                fragment: partial_fragment,
                node: partial_source,
                receive_mapping: Box::from([(source_value, partial_value)]),
            },
            partitioning: EdgePartitioning {
                source: Distribution::Hash {
                    keys: Box::from([source_value]),
                    scheme: test_hash_scheme(30),
                },
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: partial_distribution,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
        plan.add_edge(Edge {
            id: final_edge,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                fragment: partial_fragment,
                projection: Box::from([partial_value]),
            },
            destination: EdgeDestination {
                fragment: final_fragment,
                node: final_source,
                receive_mapping: Box::from([(partial_value, final_value)]),
            },
            partitioning: EdgePartitioning {
                source: Distribution::Singleton,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Singleton,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
        plan.set_result_port(ResultPort {
            fragment: final_fragment,
            output: OutputPort {
                node: final_topn,
                columns: Box::from([final_value]),
            },
            fields: Box::from([ResultField {
                name: "value".into(),
                alias: None,
                value: final_value,
                ty,
            }]),
        })
        .unwrap();
        plan.finish().unwrap()
    }

    fn test_runtime_filter_coverage(
        witness: novarocks_physical_plan::RuntimeFilterWitnessId,
    ) -> novarocks_physical_plan::RuntimeFilterCoverage {
        use novarocks_physical_plan::RuntimeFilterCoverageNode;

        novarocks_physical_plan::RuntimeFilterCoverage {
            nodes: Box::from([
                RuntimeFilterCoverageNode::Witness(witness),
                RuntimeFilterCoverageNode::AllOf {
                    children: Box::from([0]),
                },
            ]),
            root: 1,
        }
    }

    fn finish_ordered_join_build_filter_plan() -> PhysicalPlan {
        use novarocks_physical_plan::{
            EdgeDestination, EdgePartitioning, EdgeSource, NullOrdering,
            OrderedComparisonAlgorithm, RuntimeFilter, RuntimeFilterApplyPoint,
            RuntimeFilterArtifactCapability, RuntimeFilterCompletion, RuntimeFilterConsumer,
            RuntimeFilterConsumerActivation, RuntimeFilterConsumerTarget,
            RuntimeFilterContributionKind, RuntimeFilterDomain, RuntimeFilterEqualityWitness,
            RuntimeFilterEqualityWitnessId, RuntimeFilterId, RuntimeFilterKind,
            RuntimeFilterLifecycle, RuntimeFilterOrderKey, RuntimeFilterPolicy,
            RuntimeFilterProducer, RuntimeFilterProducerProgress, RuntimeFilterProducerTarget,
            RuntimeFilterReduction, RuntimeFilterWitnessId, SortDirection,
        };

        let left_fragment = FragmentId::new(40);
        let right_fragment = FragmentId::new(41);
        let join_fragment = FragmentId::new(42);
        let left_edge = EdgeId::new(40);
        let right_edge = EdgeId::new(41);
        let scheme = test_hash_scheme(40);
        let ty = ValueType::new(DataType::Int64, false);

        let mut left_builder = FragmentBuilder::new(left_fragment);
        let (left_source_node, left_source_value) = append_test_i64_values(&mut left_builder, 1);
        let left_source = left_builder
            .finish_definition(
                left_source_node,
                FragmentSink::Stream { edge: left_edge },
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let mut right_builder = FragmentBuilder::new(right_fragment);
        let (right_source_node, right_source_value) = append_test_i64_values(&mut right_builder, 2);
        let right_source = right_builder
            .finish_definition(
                right_source_node,
                FragmentSink::Stream { edge: right_edge },
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();

        let mut join_builder = FragmentBuilder::new(join_fragment);
        let left = join_builder.reserve_node_id().unwrap();
        let left_value = join_builder
            .add_value(
                ty.clone(),
                ValueOrigin::ExchangeImport {
                    edge: left_edge,
                    source_value: left_source_value,
                },
            )
            .unwrap();
        let left_properties = PhysicalProperties {
            distribution: Distribution::Hash {
                keys: Box::from([left_value]),
                scheme: scheme.clone(),
            },
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        join_builder
            .insert_node_unchecked(PhysicalNode {
                id: left,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: left_properties.clone(),
                output: OutputPort {
                    node: left,
                    columns: Box::from([left_value]),
                },
                kind: NodeKind::ExchangeSource {
                    edge: left_edge,
                    imports: Box::from([(left_source_value, left_value)]),
                },
            })
            .unwrap();

        let right = join_builder.reserve_node_id().unwrap();
        let right_value = join_builder
            .add_value(
                ty.clone(),
                ValueOrigin::ExchangeImport {
                    edge: right_edge,
                    source_value: right_source_value,
                },
            )
            .unwrap();
        let right_properties = PhysicalProperties {
            distribution: Distribution::Hash {
                keys: Box::from([right_value]),
                scheme: scheme.clone(),
            },
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        join_builder
            .insert_node_unchecked(PhysicalNode {
                id: right,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: right_properties.clone(),
                output: OutputPort {
                    node: right,
                    columns: Box::from([right_value]),
                },
                kind: NodeKind::ExchangeSource {
                    edge: right_edge,
                    imports: Box::from([(right_source_value, right_value)]),
                },
            })
            .unwrap();
        let aggregate = join_builder.reserve_node_id().unwrap();
        let group_key = join_builder
            .add_expression(aggregate, ty.clone(), ExprKind::Value(right_value))
            .unwrap();
        join_builder
            .insert_node_unchecked(PhysicalNode {
                id: aggregate,
                inputs: Box::from([right]),
                required_inputs: Box::from([right_properties.clone()]),
                output_properties: right_properties.clone(),
                output: OutputPort {
                    node: aggregate,
                    columns: Box::from([right_value]),
                },
                kind: NodeKind::Aggregate {
                    group_by: Box::from([(group_key, right_value)]),
                    calls: Box::default(),
                    grouping: novarocks_physical_plan::AggregateGrouping::Complete,
                },
            })
            .unwrap();
        let join = join_builder.reserve_node_id().unwrap();
        let left_key = join_builder
            .add_expression(join, ty.clone(), ExprKind::Value(left_value))
            .unwrap();
        let right_key = join_builder
            .add_expression(join, ty.clone(), ExprKind::Value(right_value))
            .unwrap();
        join_builder
            .insert_node_unchecked(PhysicalNode {
                id: join,
                inputs: Box::from([left, aggregate]),
                required_inputs: Box::from([left_properties.clone(), right_properties]),
                output_properties: left_properties,
                output: OutputPort {
                    node: join,
                    columns: Box::from([left_value, right_value]),
                },
                kind: NodeKind::HashJoin {
                    kind: JoinKind::Inner,
                    keys: Box::from([JoinKey {
                        left: left_key,
                        right: right_key,
                        null_safe: false,
                    }]),
                    build_side: JoinSide::Right,
                    distribution: JoinDistribution::Partitioned,
                    residual: None,
                    null_extended: Box::default(),
                },
            })
            .unwrap();
        let filter_id = RuntimeFilterId::new(40);
        join_builder.attach_runtime_filter(filter_id).unwrap();
        let join_stage = join_builder
            .finish_definition(
                join,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();

        let witness = RuntimeFilterWitnessId::new(40);
        let equality = RuntimeFilterEqualityWitnessId::new(40);
        let filter = RuntimeFilter {
            id: filter_id,
            kind: RuntimeFilterKind::MinMax,
            domain: RuntimeFilterDomain::Ordered {
                key: RuntimeFilterOrderKey {
                    ty: ty.clone(),
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::Last,
                },
                inclusive: true,
                comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            reduction: RuntimeFilterReduction::UnionOrderedHull,
            availability_coverage: test_runtime_filter_coverage(witness),
            terminal_coverage: test_runtime_filter_coverage(witness),
            equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
                id: equality,
                fragment: join_fragment,
                join,
                key_ordinal: 0,
                domain_side: JoinSide::Right,
            }]),
            producers: Box::from([RuntimeFilterProducer {
                witness,
                endpoint: novarocks_physical_plan::RuntimeFilterEndpoint {
                    fragment: join_fragment,
                    node: join,
                    values: Box::from([right_value]),
                },
                apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
                contribution_kinds: Box::from([
                    RuntimeFilterContributionKind::FinalOrderedHullShard,
                    RuntimeFilterContributionKind::ProducerClosed,
                ]),
                completion: RuntimeFilterCompletion::FencedCommittedDomain,
                progress: RuntimeFilterProducerProgress {
                    build_edges: Box::from([right_edge]),
                    non_build_edges: Box::from([left_edge]),
                },
                target: RuntimeFilterProducerTarget::JoinBuildKey { equality },
            }]),
            consumers: Box::from([RuntimeFilterConsumer {
                endpoint: novarocks_physical_plan::RuntimeFilterEndpoint {
                    fragment: join_fragment,
                    node: join,
                    values: Box::from([left_value]),
                },
                apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
                capabilities: Box::from([
                    RuntimeFilterArtifactCapability::OrderedRange,
                    RuntimeFilterArtifactCapability::EmptyDomain,
                ]),
                activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
                target: RuntimeFilterConsumerTarget::JoinProbeKey { equality },
            }]),
            policy: RuntimeFilterPolicy {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 1,
            },
        };

        let mut plan = PlanBuilder::new(PlanVersionId::try_new([40; 16]).unwrap());
        plan.add_fragment(left_source).unwrap();
        plan.add_fragment(right_source).unwrap();
        plan.add_fragment(join_stage).unwrap();
        plan.add_edge(Edge {
            id: left_edge,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                fragment: left_fragment,
                projection: Box::from([left_source_value]),
            },
            destination: EdgeDestination {
                fragment: join_fragment,
                node: left,
                receive_mapping: Box::from([(left_source_value, left_value)]),
            },
            partitioning: EdgePartitioning {
                source: Distribution::Hash {
                    keys: Box::from([left_source_value]),
                    scheme: scheme.clone(),
                },
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Hash {
                    keys: Box::from([left_value]),
                    scheme: scheme.clone(),
                },
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
        plan.add_edge(Edge {
            id: right_edge,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                fragment: right_fragment,
                projection: Box::from([right_source_value]),
            },
            destination: EdgeDestination {
                fragment: join_fragment,
                node: right,
                receive_mapping: Box::from([(right_source_value, right_value)]),
            },
            partitioning: EdgePartitioning {
                source: Distribution::Hash {
                    keys: Box::from([right_source_value]),
                    scheme: scheme.clone(),
                },
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Hash {
                    keys: Box::from([right_value]),
                    scheme,
                },
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
        plan.add_runtime_filter(filter).unwrap();
        plan.set_result_port(ResultPort {
            fragment: join_fragment,
            output: OutputPort {
                node: join,
                columns: Box::from([left_value, right_value]),
            },
            fields: Box::from([
                ResultField {
                    name: "left".into(),
                    alias: None,
                    value: left_value,
                    ty: ty.clone(),
                },
                ResultField {
                    name: "right".into(),
                    alias: None,
                    value: right_value,
                    ty,
                },
            ]),
        })
        .unwrap();
        plan.finish().unwrap()
    }

    #[test]
    fn writer_projection_consumes_repeated_value_occurrences_left_to_right() {
        let first = ValueId::new(41);
        let second = ValueId::new(42);
        let node = PhysicalNode {
            id: NodeId::new(7),
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties(),
            output: OutputPort {
                node: NodeId::new(7),
                columns: Box::from([first, second, first]),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        };
        assert_eq!(
            project_output_ordinals(&node, &[first, first, second]).unwrap(),
            vec![0, 2, 1]
        );
        assert!(project_output_ordinals(&node, &[first, first, first]).is_err());
    }

    #[test]
    fn router_projection_consumes_occurrences_and_rejects_exhaustion() {
        let value = ValueId::new(51);
        let node = PhysicalNode {
            id: NodeId::new(8),
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties(),
            output: OutputPort {
                node: NodeId::new(8),
                columns: Box::from([value, value]),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        };
        assert_eq!(
            project_output_ordinals(&node, &[value, value]).unwrap(),
            vec![0, 1]
        );
        assert!(project_output_ordinals(&node, &[value, value, value]).is_err());

        let route = novarocks_physical_plan::ChangeStreamRoute {
            route_id: novarocks_physical_plan::ConnectorWriteRouteId::from_bytes([9; 32]),
            write_target_ordinal: novarocks_physical_plan::WriteTargetOrdinal::try_new(0).unwrap(),
            accepted_effects: Box::from([
                novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
            ]),
            input_mapping: Box::from([
                (
                    novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes([1; 32]),
                    value,
                ),
                (
                    novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes([2; 32]),
                    value,
                ),
            ]),
            partition_by: Box::from([value]),
            edge: EdgeId::new(9),
        };
        assert_eq!(
            router_route_ordinals(&node, &route).unwrap(),
            (vec![0, 1], vec![0])
        );
    }

    #[test]
    fn direct_encoder_preserves_repeated_result_occurrences() {
        let (catalog, _) = exact_scalar_catalog();
        let mut fragment_builder = FragmentBuilder::new(FragmentId::new(19));
        let values = fragment_builder.reserve_node_id().unwrap();
        let ty = ValueType::new(DataType::Int64, false);
        let literal = fragment_builder
            .add_expression(
                values,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(9)),
            )
            .unwrap();
        let value = fragment_builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: values,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        fragment_builder
            .insert_node_unchecked(PhysicalNode {
                id: values,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: values,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([literal])]),
                },
            })
            .unwrap();
        let project = fragment_builder.reserve_node_id().unwrap();
        let reference = fragment_builder
            .add_expression(project, ty.clone(), ExprKind::Value(value))
            .unwrap();
        fragment_builder
            .insert_node_unchecked(PhysicalNode {
                id: project,
                inputs: Box::from([values]),
                required_inputs: Box::from([properties()]),
                output_properties: properties(),
                output: OutputPort {
                    node: project,
                    columns: Box::from([value, value]),
                },
                kind: NodeKind::Project {
                    expressions: Box::from([(reference, value), (reference, value)]),
                },
            })
            .unwrap();
        let fragment = fragment_builder
            .finish_definition(
                project,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([19; 16]).unwrap());
        plan_builder.add_fragment(fragment).unwrap();
        plan_builder
            .set_result_port(ResultPort {
                fragment: FragmentId::new(19),
                output: OutputPort {
                    node: project,
                    columns: Box::from([value, value]),
                },
                fields: Box::from([
                    ResultField {
                        name: "first".into(),
                        alias: None,
                        value,
                        ty: ty.clone(),
                    },
                    ResultField {
                        name: "second".into(),
                        alias: None,
                        value,
                        ty,
                    },
                ]),
            })
            .unwrap();
        let physical = plan_builder.finish().unwrap();
        let encoded =
            encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts).unwrap();
        let fragment = &encoded.fragments[0];
        assert_eq!(fragment.fragment_id, 19);
        assert_eq!(fragment.output_columns.len(), 2);
        assert_ne!(
            fragment.output_columns[0].column_id,
            fragment.output_columns[1].column_id
        );
        assert_eq!(fragment.output_columns[0].name, "first");
        assert_eq!(fragment.output_columns[1].name, "second");
    }

    #[test]
    fn limit_i64_overflow_is_rejected_by_encoder_preflight() {
        let (catalog, _) = exact_scalar_catalog();
        let mut builder = FragmentBuilder::new(FragmentId::new(21));
        let (values, value) = append_test_i64_values(&mut builder, 1);
        let limit = builder.reserve_node_id().unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: limit,
                inputs: Box::from([values]),
                required_inputs: Box::from([properties()]),
                output_properties: properties(),
                output: OutputPort {
                    node: limit,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Limit {
                    limit: Some(u64::MAX),
                    offset: 0,
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                limit,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let ty = ValueType::new(DataType::Int64, false);
        let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([21; 16]).unwrap());
        plan_builder.add_fragment(fragment).unwrap();
        plan_builder
            .set_result_port(ResultPort {
                fragment: FragmentId::new(21),
                output: OutputPort {
                    node: limit,
                    columns: Box::from([value]),
                },
                fields: Box::from([ResultField {
                    name: "value".into(),
                    alias: None,
                    value,
                    ty,
                }]),
            })
            .unwrap();
        let physical = plan_builder.finish().unwrap();

        let error = encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect_err("Limit overflow must fail in encoder preflight");
        assert!(error.contains("Limit limit exceeds i64"), "{error}");
    }

    #[test]
    fn repeat_grouping_sets_preserve_wire_slots_and_sql_bit_order() {
        let mut builder = FragmentBuilder::new(FragmentId::new(18));
        let source = builder.reserve_node_id().unwrap();
        let ty = ValueType::new(DataType::Int64, false);
        let left_literal = builder
            .add_expression(
                source,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(1)),
            )
            .unwrap();
        let right_literal = builder
            .add_expression(
                source,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(2)),
            )
            .unwrap();
        let left = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: source,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let right = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: source,
                    output_ordinal: 1,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: source,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: source,
                    columns: Box::from([left, right]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([left_literal, right_literal])]),
                },
            })
            .unwrap();

        let repeat = builder.reserve_node_id().unwrap();
        let nullable_ty = ValueType::new(DataType::Int64, true);
        let nullable_left = builder
            .add_value(
                nullable_ty.clone(),
                ValueOrigin::NullExtended {
                    node: repeat,
                    of: left,
                },
            )
            .unwrap();
        let nullable_right = builder
            .add_value(
                nullable_ty,
                ValueOrigin::NullExtended {
                    node: repeat,
                    of: right,
                },
            )
            .unwrap();
        let grouping = builder
            .add_value(
                ty,
                ValueOrigin::NodeOutput {
                    node: repeat,
                    output_ordinal: 2,
                },
            )
            .unwrap();
        let rollup_keys: Box<[ValueId]> = Box::from([left, right]);
        let grouping_sets: Box<[Box<[ValueId]>]> = Box::from([
            Box::from([left, right]),
            Box::from([left]),
            Box::from([right]),
            Box::default(),
        ]);
        let grouping_values: Box<[(ValueId, ValueId)]> =
            Box::from([(left, nullable_left), (right, nullable_right)]);
        let grouping_outputs: Box<[novarocks_physical_plan::GroupingOutput]> =
            Box::from([novarocks_physical_plan::GroupingOutput {
                output: grouping,
                arguments: Box::from([left, right]),
            }]);
        builder
            .insert_node_unchecked(PhysicalNode {
                id: repeat,
                inputs: Box::from([source]),
                required_inputs: Box::from([properties()]),
                output_properties: properties(),
                output: OutputPort {
                    node: repeat,
                    columns: Box::from([nullable_left, nullable_right, grouping]),
                },
                kind: NodeKind::Repeat {
                    rollup_keys: rollup_keys.clone(),
                    grouping_sets: grouping_sets.clone(),
                    grouping_values: grouping_values.clone(),
                    grouping_outputs: grouping_outputs.clone(),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                repeat,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let physical_fragment = fragment.clone();
        let layout = WireLayout::try_new(&fragment).unwrap();
        let left_slot = layout.input_value_slot(repeat, left).unwrap().get_u32();
        let right_slot = layout.input_value_slot(repeat, right).unwrap().get_u32();
        let encoded = encode_repeat(
            &layout,
            &fragment.nodes()[&repeat],
            &rollup_keys,
            &grouping_sets,
            &grouping_values,
            &grouping_outputs,
        )
        .unwrap();
        assert_eq!(encoded.grouping_ids, vec![0, 1, 2, 3]);
        assert_eq!(encoded.all_rollup_column_ids, vec![left_slot, right_slot]);
        assert_eq!(
            encoded
                .repeat_column_ref_ids
                .iter()
                .map(|ids| ids.values.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![left_slot, right_slot],
                vec![left_slot],
                vec![right_slot],
                vec![]
            ]
        );
        assert_eq!(
            encoded.grouping_fn_ids[0].value,
            layout.output_slot(repeat, 2).unwrap().get_u32()
        );

        let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([18; 16]).unwrap());
        plan_builder.add_fragment(fragment).unwrap();
        plan_builder
            .set_result_port(ResultPort {
                fragment: FragmentId::new(18),
                output: OutputPort {
                    node: repeat,
                    columns: Box::from([nullable_left, nullable_right, grouping]),
                },
                fields: Box::from([
                    ResultField {
                        name: "left".into(),
                        alias: None,
                        value: nullable_left,
                        ty: ValueType::new(DataType::Int64, true),
                    },
                    ResultField {
                        name: "right".into(),
                        alias: None,
                        value: nullable_right,
                        ty: ValueType::new(DataType::Int64, true),
                    },
                    ResultField {
                        name: "grouping".into(),
                        alias: None,
                        value: grouping,
                        ty: ValueType::new(DataType::Int64, false),
                    },
                ]),
            })
            .unwrap();
        // The null-extended columns stand where their inputs arrived, which is
        // where the wire nulls them, so this plan encodes.
        assert!(v1_repeat_grouping_values_are_lossless(
            &physical_fragment,
            &physical_fragment.nodes()[&repeat],
            &grouping_values
        ));
        let physical = plan_builder.finish().unwrap();
        let (catalog, _) = exact_scalar_catalog();
        encode_physical_plan_v1(&physical, &catalog, &NoPhysicalV1PrivateFacts)
            .expect("a Repeat that nulls its grouping columns in place encodes");
    }

    #[test]
    fn keyed_assertion_uses_input_wire_slots_instead_of_value_ids() {
        let mut builder = FragmentBuilder::new(FragmentId::new(17));
        let source = builder.reserve_node_id().unwrap();
        let ty = ValueType::new(DataType::Int64, false);
        let first_literal = builder
            .add_expression(
                source,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(1)),
            )
            .unwrap();
        let second_literal = builder
            .add_expression(
                source,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(2)),
            )
            .unwrap();
        let key = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: source,
                    output_ordinal: 1,
                },
            )
            .unwrap();
        let first = builder
            .add_value(
                ty,
                ValueOrigin::NodeOutput {
                    node: source,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: source,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: source,
                    columns: Box::from([first, key]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([first_literal, second_literal])]),
                },
            })
            .unwrap();
        let assertion = builder.reserve_node_id().unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: assertion,
                inputs: Box::from([source]),
                required_inputs: Box::from([properties()]),
                output_properties: properties(),
                output: OutputPort {
                    node: assertion,
                    columns: Box::from([first, key]),
                },
                kind: NodeKind::AssertOneRow(RowCountAssertionSpec::PerKeyAtMostOne {
                    keys: Box::from([key]),
                    labels: Box::from([Box::<str>::from("k")]),
                    message: "duplicate".into(),
                }),
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                assertion,
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let layout = WireLayout::try_new(&fragment).unwrap();
        let wire_slot = layout.input_value_slot(assertion, key).unwrap().get_u32();
        assert_ne!(wire_slot, key.get());
        let encoded = encode_assertion(
            &layout,
            &fragment.nodes()[&assertion],
            match &fragment.nodes()[&assertion].kind {
                NodeKind::AssertOneRow(assertion) => assertion,
                _ => unreachable!(),
            },
        )
        .unwrap();
        assert_eq!(encoded.group_key_column_ids, vec![wire_slot]);
    }
}
