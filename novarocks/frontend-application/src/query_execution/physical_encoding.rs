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

//! What a completed plan's scans need in order to go on the wire.
//!
//! The plan says which provider relation each scan reads and in what order its
//! columns come back. It deliberately does not say what those columns are
//! called, what the provider agreed to enforce, or how to address the relation
//! in a provider's own encoding - those are private to the freeze that produced
//! the scan, and the freeze kept them.
//!
//! This joins the two. Every scan in the plan must find its frozen read, and
//! every frozen read must belong to a scan; that is already guaranteed by the
//! pairing the plan was published with, so a miss here is a defect rather than
//! a case to handle.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use novarocks_functions::EngineFunctionCatalog;
use novarocks_physical_plan::PlanVersionId;
use novarocks_physical_plan::{
    Distribution, EdgeKind, FragmentId, FragmentSink, NodeId, NodeKind, PhysicalPlan,
    ProviderColumnReference, ProviderReadOccurrenceId, Relation, WriteTargetOrdinal,
};
use novarocks_plan_codec::{
    PhysicalV1PrivateFacts, PhysicalV1ScanColumn, PhysicalV1ScanFact, PhysicalV1WriteFact,
    SealedWriteTargets, encode_physical_plan_v1, physical_v1_scan_runtime_filters,
    physical_v1_scan_source_seal_digest,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::connector_read::{
    ConnectorReadEncoder, ConnectorTableScanSource, encode_connector_expression,
};
use novarocks_proto_models::{connector_read as dto, plan};
use novarocks_query_application::preparation::CompletedPlanWithAccess;
use novarocks_spi::connector::read_stack::ConnectorReadWorkSource;

use crate::native::fragment_encoder::submission::freeze_completed_fragments;
use crate::query_execution::artifact::native_submission::{
    NativeSubmissionFragmentRole, SubmissionFragmentFacts, SubmissionPlanFacts,
};
use crate::query_execution::assembly::CteMulticastConsumer;
use crate::query_execution::attempt_plan_facts::{
    AttemptEdgeFacts, AttemptPartitionKind, AttemptPlanFacts, AttemptScanFacts, PlanOutputColumn,
};
use crate::query_execution::attempt_runtime_filter_facts::AttemptRuntimeFilterFacts;
use crate::query_execution::fragment_scheduling::{
    FragmentSchedulingFacts, SchedulingEdgeFacts, SchedulingFragmentFacts, SchedulingScanFacts,
    SchedulingStreamKind,
};
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::post_compile::mint_native_encoding_provenance;
use crate::query_execution::preparation::attempt_access::{
    ConnectorAttemptAccessPlan, attempt_access_for_completed_plan,
};
use crate::query_execution::provider_read_facts::{FrozenProviderRead, FrozenReadEncoding};

/// A completed plan on the wire, the capabilities its reads will be performed
/// with, and what opening each of those reads takes.
/// One plan's version.
///
/// A version distinguishes one plan from every other, including two
/// compilations of the same text and two collections of the same table, so it
/// is minted per plan from a time-ordered unique identity rather than derived
/// from whatever the plan was built from.
pub(crate) fn mint_plan_version() -> PlanVersionId {
    PlanVersionId::try_new(*uuid::Uuid::now_v7().as_bytes())
        .expect("a v7 identity is never the reserved zero version")
}

pub(crate) struct EncodedCompletedPlan {
    /// The validated semantic candidate shared with the logical execution
    /// owner; Native projection consumes only a second reference to its plan.
    pub(crate) semantic_candidate:
        novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
    /// Every fragment's static plan, frozen once for every attempt of this
    /// plan, keyed for submission and stamped so it cannot be paired with
    /// another encoding's artifacts. The generated messages it was frozen
    /// from are gone.
    pub(crate) native: NativeFragmentAttachment,
    pub(crate) topology: CompletedPlanTopology,
    /// Everything the attempt that runs this plan reads about it.
    pub(crate) plan_facts: AttemptPlanFacts,
    pub(crate) access: ConnectorAttemptAccessPlan,
}

impl EncodedCompletedPlan {
    pub(crate) fn into_attempt_template_with_candidate(
        self,
        version: PlanVersionId,
    ) -> (
        crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
        novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
    ) {
        let candidate = self.semantic_candidate.clone();
        (self.into_attempt_template(version), candidate)
    }

    /// Hand this encoding to the owner that runs attempts of it.
    ///
    /// Everything an attempt reads is already here and already keyed to this
    /// encoding; the template is where the plan facts, the encoded fragments
    /// and the read capabilities stop being separate values. Opening a read
    /// takes both, and the round that opens it asks the template for the
    /// pair, exactly as it does for a sealed plan.
    pub(crate) fn into_attempt_template(
        self,
        plan: PlanVersionId,
    ) -> crate::query_execution::artifact::PreparedDistributedAttemptTemplate {
        crate::query_execution::artifact::PreparedDistributedAttemptTemplate::for_completed_plan(
            novarocks_query_application::api::PlanSeal::Version(plan),
            self.plan_facts,
            self.native,
            self.access,
        )
    }
}

/// Put one completed plan on the wire, and place everything its scans were
/// frozen with where the attempt that runs them will look.
///
/// A freeze leaves three things and each has its own consumer: facts that put
/// the plan on the wire, a capability that performs the read, and what opening
/// that read's splits takes. They separate exactly here, after having been
/// accounted for together, and the capability moves rather than copies because
/// a capability cannot be copied.
/// What one plan's write targets were sealed and admitted with.
///
/// The handle is what the wire carries; the field names are what the provider
/// matches its own schema by. Both belong to the same admission, so they are
/// taken together rather than as two independent caller choices.
pub(crate) struct WriteTargetFacts<'a> {
    pub(crate) sealed: &'a SealedWriteTargets,
    pub(crate) field_names: BTreeMap<WriteTargetOrdinal, BTreeMap<[u8; 32], Box<str>>>,
}

// Design: ADR-0153 (docs/adr/ADR-0153-completed-physical-plan-is-the-static-execution-authority.md)
pub(crate) fn encode_completed_plan(
    paired: CompletedPlanWithAccess<FrozenProviderRead>,
    functions: &EngineFunctionCatalog,
    write_targets: Option<&WriteTargetFacts<'_>>,
) -> Result<EncodedCompletedPlan, String> {
    let semantic_candidate = paired.candidate().clone();
    let (candidate, reads) = paired.into_parts();
    let plan = candidate.plan();
    let mut encodings = BTreeMap::new();
    let mut capabilities = BTreeMap::new();
    for (occurrence, read) in reads.into_occurrences() {
        let FrozenProviderRead {
            access,
            generation,
            catalog,
            encoding,
        } = read.access;
        encodings.insert(occurrence, encoding);
        capabilities.insert(occurrence, (read.binding, access, generation, catalog));
    }
    let facts = physical_v1_private_facts(plan, &encodings, write_targets)?;
    let encoded = encode_physical_plan_v1(plan, functions, &facts)?;
    let access = attempt_access_for_completed_plan(plan, capabilities)?;
    let scans = completed_plan_scan_facts(plan, &encodings)?;
    let provenance = mint_native_encoding_provenance();
    let topology = completed_plan_topology(plan)?;
    let submission = completed_plan_submission_facts(plan, &encoded, &topology)?;
    // A static plan is a property of the plan, not of an attempt: every
    // fragment is frozen here, once, and every attempt -- a recovery
    // included -- creates its tasks from these bytes.
    let native = NativeFragmentAttachment::for_completed_plan(
        freeze_completed_fragments(encoded.fragments, &submission, topology.anchor)?,
        topology.anchor,
        provenance,
    )?;
    let scheduling = completed_plan_scheduling_facts(plan, &encodings, &topology, provenance)?;
    let plan_facts = AttemptPlanFacts::from_completed(
        scheduling,
        completed_plan_edge_facts(plan)?,
        scans,
        submission,
        AttemptRuntimeFilterFacts::from_completed(plan)?,
        // A plan that writes states which targets its root delivers, because
        // that is what the commit is taken over. A read plan writes none, and
        // says none rather than an empty list.
        Some(completed_plan_write_targets(plan)).filter(|targets| !targets.is_empty()),
    );
    Ok(EncodedCompletedPlan {
        semantic_candidate,
        native,
        topology,
        plan_facts,
        access,
    })
}

/// How one completed plan's fragments relate to each other.
///
/// Every field is derived from the fragments and edges alone, so two
/// structurally identical plans produce identical topology - including the
/// order, which decides the order fragments are established in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletedPlanTopology {
    /// Fragments with producers before consumers. A plan whose fragments
    /// cannot be ordered this way has a cycle, and no order would let it run.
    pub(crate) order: Vec<u32>,
    /// Fragments that feed at least one other fragment.
    pub(crate) producers: Vec<u32>,
    /// Where the query's rows are delivered, absent for a plan that only
    /// writes.
    pub(crate) result: Option<u32>,
    /// The one fragment whose completion is the execution's completion.
    pub(crate) anchor: u32,
}

/// Derive what submission encoding reads from one completed plan and its one
/// native encoding. Router identities come from those encoded edges; their
/// partition and slot contracts remain in the native fragment template.
pub(crate) fn completed_plan_submission_facts(
    plan: &PhysicalPlan,
    encoded: &plan::DistributedPlan,
    topology: &CompletedPlanTopology,
) -> Result<SubmissionPlanFacts, String> {
    let mut stream_edge_sources = BTreeSet::new();
    for edge in plan.edges().values() {
        match edge.kind {
            EdgeKind::Stream => {
                stream_edge_sources.insert(u32::from(edge.source.fragment.get()));
            }
            EdgeKind::CteMulticast => {}
            EdgeKind::ChangeStreamRouter => {}
        }
    }
    let mut fragments = Vec::with_capacity(plan.fragments().len());
    for fragment in plan.fragments().values() {
        let role = match fragment.sink() {
            FragmentSink::Result => NativeSubmissionFragmentRole::Result,
            FragmentSink::Stream { .. }
            | FragmentSink::Multicast { .. }
            | FragmentSink::Router { .. } => NativeSubmissionFragmentRole::NonTerminal,
            other => {
                return Err(format!(
                    "completed plan fragment {} has sink {other:?}, which this path does not submit",
                    fragment.id().get()
                ));
            }
        };
        fragments.push(SubmissionFragmentFacts::for_completed_plan(
            u32::from(fragment.id().get()),
            role,
            completed_fragment_output_columns(plan, fragment.id()),
            // A multicast sink is a CTE producer, and the plan names that CTE
            // by the fragment that produces it -- the same name its edges
            // carry, so a consumer and its producer agree without a second
            // identity.
            matches!(fragment.sink(), FragmentSink::Multicast { .. })
                .then(|| u32::from(fragment.id().get())),
            fragment.dop_domain(),
        ));
    }
    let mut cte_consumers = BTreeMap::<u32, Vec<CteMulticastConsumer>>::new();
    for consumer in novarocks_plan_codec::physical_v1_cte_consumers(plan)? {
        cte_consumers.entry(consumer.cte_id).or_default().push((
            consumer.target_fragment_id,
            consumer.target_exchange_node_id,
            consumer.output_partition,
            consumer.output_slot_ids,
            consumer.receive_producer_column_ids,
        ));
    }
    let router_edges =
        encoded
            .edges
            .iter()
            .filter_map(|edge| {
                let Some(plan::fragment_edge_kind::Kind::ChangeStreamRouter(router)) =
                    edge.edge_kind.as_ref().and_then(|kind| kind.kind.as_ref())
                else {
                    return None;
                };
                Some((edge, router))
            })
            .map(|(edge, router)| {
                Ok(crate::query_execution::assembly::RouterSubmissionEdge {
                    source_fragment_id: edge.source_fragment_id,
                    target_fragment_id: edge.target_fragment_id,
                    target_exchange_node_id: edge.target_exchange_node_id,
                    router_group_id: router.router_group_id,
                    route_id: router.route_id.as_slice().try_into().map_err(|_| {
                        "completed router edge has a non-32-byte route ID".to_string()
                    })?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
    Ok(SubmissionPlanFacts::for_completed_plan(
        plan.version(),
        plan.required().plan_contract_revision,
        topology.order.clone(),
        fragments,
        stream_edge_sources,
        cte_consumers,
        router_edges,
    ))
}

/// What one fragment of a completed plan delivers.
///
/// Only the result fragment delivers anything a consumer names: every other
/// fragment hands its rows to an exchange, which addresses them by position
/// and never by name. A fragment that is not the result port's therefore has
/// no output columns rather than an unnamed list of them.
///
/// The name is the alias where the statement gave one, which is the name the
/// client asked for and the same rule the wire encoder applies.
fn completed_fragment_output_columns(
    plan: &PhysicalPlan,
    fragment_id: novarocks_physical_plan::FragmentId,
) -> Vec<PlanOutputColumn> {
    plan.result_port()
        .filter(|result| result.fragment == fragment_id)
        .map(|result| {
            result
                .fields
                .iter()
                .map(|field| PlanOutputColumn {
                    name: field.alias.as_deref().unwrap_or(&field.name).to_string(),
                    data_type: field.ty.data_type.clone(),
                    nullable: field.ty.nullable,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Derive the topology of one completed plan.
pub(crate) fn completed_plan_topology(
    plan: &PhysicalPlan,
) -> Result<CompletedPlanTopology, String> {
    let mut in_degree = plan
        .fragments()
        .keys()
        .map(|id| (u32::from(id.get()), 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut consumers_of = BTreeMap::<u32, Vec<u32>>::new();
    let mut producers = BTreeSet::new();
    for edge in plan.edges().values() {
        let source = u32::from(edge.source.fragment.get());
        let destination = u32::from(edge.destination.fragment.get());
        *in_degree.entry(destination).or_insert(0) += 1;
        consumers_of.entry(source).or_default().push(destination);
        producers.insert(source);
    }

    // Producers first, in ascending id order at every step, so the order is a
    // property of the plan rather than of how it was walked.
    let mut ready = in_degree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(*id))
        .collect::<VecDeque<_>>();
    let mut order = Vec::with_capacity(in_degree.len());
    while let Some(fragment) = ready.pop_front() {
        order.push(fragment);
        for consumer in consumers_of.get(&fragment).map_or(&[][..], Vec::as_slice) {
            let degree = in_degree
                .get_mut(consumer)
                .ok_or_else(|| format!("plan edge names absent fragment {consumer}"))?;
            *degree -= 1;
            if *degree == 0 {
                ready.push_back(*consumer);
            }
        }
    }
    if order.len() != in_degree.len() {
        return Err("completed plan fragments cannot be ordered: a cycle feeds itself".to_string());
    }

    // The anchor is the one fragment nothing consumes. Two of those would mean
    // two independent completions with no statement to bind them.
    let mut terminals = in_degree
        .keys()
        .copied()
        .filter(|id| !producers.contains(id))
        .collect::<Vec<_>>();
    let anchor = match terminals.len() {
        1 => terminals.remove(0),
        0 => return Err("completed plan has no fragment that ends it".to_string()),
        _ => {
            return Err(format!(
                "completed plan ends in more than one fragment: {terminals:?}"
            ));
        }
    };
    Ok(CompletedPlanTopology {
        order,
        producers: producers.into_iter().collect(),
        result: plan
            .result_port()
            .map(|result| u32::from(result.fragment.get())),
        anchor,
    })
}

/// What scheduling reads about one completed plan.
///
/// A provider read's work reaches a backend one of two ways, and only the
/// provider knows which, so that answer comes from the freeze. Nothing has
/// enumerated any splits yet - that belongs to the attempt - so every scan
/// starts with no ranges to spread.
fn completed_plan_scheduling_facts(
    plan: &PhysicalPlan,
    encodings: &BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
    topology: &CompletedPlanTopology,
    handoff_id: u64,
) -> Result<FragmentSchedulingFacts, String> {
    let mut fragments = BTreeMap::new();
    for fragment in plan.fragments().values() {
        let mut scans = Vec::new();
        for node in fragment.nodes().values() {
            let NodeKind::Scan { occurrence, .. } = &node.kind else {
                continue;
            };
            let encoding = encodings.get(occurrence).ok_or_else(|| {
                format!(
                    "completed plan scans provider read occurrence {} with no frozen read",
                    occurrence.get()
                )
            })?;
            scans.push(SchedulingScanFacts {
                node_id: wire_node_id(node.id)?,
                ranges: Vec::new(),
                work_source: Some(encoding.work_source),
            });
        }
        fragments.insert(
            u32::from(fragment.id().get()),
            SchedulingFragmentFacts { scans },
        );
    }
    let edges = plan
        .edges()
        .values()
        .map(|edge| {
            Ok(SchedulingEdgeFacts {
                source: u32::from(edge.source.fragment.get()),
                target: u32::from(edge.destination.fragment.get()),
                target_exchange_node_id: wire_node_id(edge.destination.node)?,
                native_hash_partitioned: matches!(
                    edge.partitioning.destination,
                    Distribution::Hash { .. }
                ),
                stream_kind: match edge.partitioning.destination {
                    Distribution::Singleton => SchedulingStreamKind::Gather,
                    Distribution::Broadcast => SchedulingStreamKind::Broadcast,
                    Distribution::Hash { .. } | Distribution::BucketShuffle { .. } => {
                        SchedulingStreamKind::Partitioned
                    }
                    Distribution::Unconstrained | Distribution::RoundRobin => {
                        SchedulingStreamKind::Other
                    }
                },
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(FragmentSchedulingFacts {
        handoff_id,
        order: topology.order.clone(),
        anchor: topology.anchor,
        fragments,
        edges,
    })
}

/// Every provider read of one completed plan, as the attempt that runs them
/// reads them.
///
/// The freeze left each read's provider columns and the constraint the
/// provider was offered; the plan says which runtime filter constrains which
/// produced value. Resolving the two here, once, is what lets the attempt
/// carry already-matched pairs instead of names to be matched again later.
fn completed_plan_scan_facts(
    plan: &PhysicalPlan,
    encodings: &BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
) -> Result<Vec<AttemptScanFacts>, String> {
    let runtime_filters = physical_v1_scan_runtime_filters(plan)?;
    let mut scans = Vec::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Scan {
                occurrence,
                provider_outputs,
                ..
            } = &node.kind
            else {
                continue;
            };
            let encoding = encodings.get(occurrence).ok_or_else(|| {
                format!(
                    "completed plan scans provider read occurrence {} with no frozen read",
                    occurrence.get()
                )
            })?;
            let node_id = wire_node_id(node.id)?;
            let fragment_id = u32::from(fragment.id().get());
            let dynamic_filters = runtime_filters
                .get(&(fragment.id(), node.id))
                .map_or(&[][..], Vec::as_slice)
                .iter()
                .map(|(filter_id, value)| {
                    let ordinal = provider_outputs
                        .iter()
                        .position(|(_, output)| output == value)
                        .ok_or_else(|| {
                            format!(
                                "runtime filter {filter_id} constrains a value scan node {node_id} does not produce"
                            )
                        })?;
                    Ok((*filter_id, encoding.assignments[ordinal].column().clone()))
                })
                .collect::<Result<Vec<_>, String>>()?;
            scans.push(AttemptScanFacts {
                fragment_id,
                plan_node_id: node_id,
                assignments: encoding.assignments.clone(),
                dynamic_filters,
                constraint: encoding.offered_constraint.clone(),
            });
        }
    }
    Ok(scans)
}

/// The exchange edges of one completed plan, as placing and connecting tasks
/// reads them.
fn completed_plan_edge_facts(plan: &PhysicalPlan) -> Result<Vec<AttemptEdgeFacts>, String> {
    plan.edges()
        .values()
        .map(|edge| {
            Ok(AttemptEdgeFacts {
                source_fragment_id: u32::from(edge.source.fragment.get()),
                target_fragment_id: u32::from(edge.destination.fragment.get()),
                target_exchange_node_id: wire_node_id(edge.destination.node)?,
                partition_kind: completed_edge_partition_kind(&edge.partitioning.destination),
            })
        })
        .collect()
}

/// How the destination of one completed-plan edge is partitioned, in the
/// vocabulary the wire stream type is named by.
///
/// A broadcast edge is unpartitioned: every destination receives every row,
/// which is a property of the stream rather than of the partitioning, and the
/// sealed plan says the same thing about its own broadcast edges.
const fn completed_edge_partition_kind(destination: &Distribution) -> AttemptPartitionKind {
    match destination {
        Distribution::Singleton | Distribution::Broadcast => AttemptPartitionKind::Unpartitioned,
        Distribution::Hash { .. } | Distribution::BucketShuffle { .. } => {
            AttemptPartitionKind::Hash
        }
        Distribution::Unconstrained | Distribution::RoundRobin => AttemptPartitionKind::Random,
    }
}

fn wire_node_id(node: NodeId) -> Result<i32, String> {
    i32::try_from(node.get())
        .map_err(|_| format!("scan node {} exceeds the wire node identity", node.get()))
}

/// One plan's wire-private facts, addressed the way the encoder asks for
/// them.
///
/// A scan's facts are addressed by the node that performs it; a writer's by
/// the target it writes, because one target may be written from more than one
/// node and every one of them writes the same handle.
pub(crate) struct FrontendPhysicalV1Facts {
    by_node: BTreeMap<(FragmentId, NodeId), PhysicalV1ScanFact>,
    by_target: BTreeMap<WriteTargetOrdinal, PhysicalV1WriteFact>,
}

impl PhysicalV1PrivateFacts for FrontendPhysicalV1Facts {
    fn scan_fact(&self, fragment: FragmentId, node: NodeId) -> Option<&PhysicalV1ScanFact> {
        self.by_node.get(&(fragment, node))
    }

    fn write_fact(&self, target: WriteTargetOrdinal) -> Option<&PhysicalV1WriteFact> {
        self.by_target.get(&target)
    }
}

/// The write targets one completed plan writes, in ordinal order.
fn completed_plan_write_targets(plan: &PhysicalPlan) -> Vec<WriteTargetOrdinal> {
    let mut targets = plan
        .fragments()
        .values()
        .flat_map(|fragment| fragment.nodes().values())
        .filter_map(|node| match &node.kind {
            NodeKind::TableWriter { target } => Some(target.write_target_ordinal),
            _ => None,
        })
        .collect::<Vec<_>>();
    targets.sort_unstable_by_key(|ordinal| ordinal.get());
    targets.dedup();
    targets
}

/// Build the private facts for every scan of one completed plan.
fn physical_v1_private_facts(
    plan: &PhysicalPlan,
    encodings: &BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
    write_targets: Option<&WriteTargetFacts<'_>>,
) -> Result<FrontendPhysicalV1Facts, String> {
    // The encoder derives the runtime-filter binding identities itself and
    // checks what it is handed against them. Asking it rather than repeating
    // the numbering is what keeps the two from drifting apart.
    let runtime_filters = physical_v1_scan_runtime_filters(plan)?;
    let mut by_node = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Scan {
                occurrence,
                relation,
                read_budget,
                provider_outputs,
                ..
            } = &node.kind
            else {
                continue;
            };
            let encoding = encodings.get(occurrence).ok_or_else(|| {
                format!(
                    "completed plan scans provider read occurrence {} with no frozen read",
                    occurrence.get()
                )
            })?;
            let dynamic_filters = runtime_filters
                .get(&(fragment.id(), node.id))
                .map_or(&[][..], Vec::as_slice);
            let source = scan_source(
                encoding,
                read_budget.max_batch_rows,
                read_budget.max_batch_bytes,
                dynamic_filters,
                provider_outputs,
            )?;
            let seal = physical_v1_scan_source_seal_digest(
                *occurrence,
                relation.read(),
                selection_digest(relation),
                source.as_proto(),
            )?;
            by_node.insert(
                (fragment.id(), node.id),
                PhysicalV1ScanFact {
                    occurrence: *occurrence,
                    read: relation.read().clone(),
                    selection_digest: selection_digest(relation),
                    source_seal_digest: seal,
                    database: encoding.identity.namespace.as_str().into(),
                    // A scan addresses its relation by frozen reference, so it
                    // has no alias to carry: an alias is a name a statement
                    // used, and no reader resolves anything by it.
                    alias: None,
                    table: table_def(encoding, source)?,
                    columns: scan_columns(encoding, provider_outputs)?,
                },
            );
        }
    }
    // Every target the plan writes must be one this session sealed: a target
    // the session did not seal is a writer handle nobody accounted for.
    //
    // The converse does not hold. One session seals every target its statement
    // writes and then drives a query per target -- a copy-on-write UPDATE
    // seals the delete and the insert together -- so a sealed target this
    // plan does not write belongs to one of that statement's other queries.
    let written = completed_plan_write_targets(plan);
    let mut by_target = BTreeMap::new();
    for target in &written {
        let facts = write_targets.ok_or_else(|| {
            format!(
                "completed plan writes target {} with no sealed writer handle",
                target.get()
            )
        })?;
        let handle = facts.sealed.handle_for_target(*target).ok_or_else(|| {
            format!(
                "completed plan writes target {} with no sealed writer handle",
                target.get()
            )
        })?;
        let field_names = facts.field_names.get(target).cloned().ok_or_else(|| {
            format!(
                "completed plan writes target {} with no accepted field bindings",
                target.get()
            )
        })?;
        by_target.insert(
            *target,
            PhysicalV1WriteFact {
                handle,
                field_names,
            },
        );
    }
    Ok(FrontendPhysicalV1Facts { by_node, by_target })
}

const fn selection_digest(relation: &Relation) -> [u8; 32] {
    match relation {
        Relation::Data(relation) => relation.selection_digest,
        Relation::Metadata(relation) => relation.selection_digest,
    }
}

/// The provider-owned half of one scan, as the wire carries it.
fn scan_source(
    encoding: &FrozenReadEncoding,
    max_batch_rows: u64,
    max_batch_bytes: u64,
    dynamic_filters: &[(u32, novarocks_physical_plan::ValueId)],
    provider_outputs: &[(ProviderColumnReference, novarocks_physical_plan::ValueId)],
) -> Result<ConnectorTableScanSource, String> {
    let encoder = encoding.encoder.as_ref();
    let assignments = encoding
        .assignments
        .iter()
        .enumerate()
        .map(|(index, assignment)| {
            encoder.encode_assignment(
                assignment,
                FieldPath::root("connector_table_scan_source")
                    .field("assignments")
                    .index(index),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let raw = dto::ConnectorTableScanSource {
        table: Some(
            encoder
                .encode_relation(&encoding.relation)
                .map_err(|error| error.to_string())?,
        ),
        assignments,
        enforced_predicate: Some(
            encoder
                .encode_tuple_domain(
                    &encoding.enforced_predicate,
                    FieldPath::root("connector_table_scan_source").field("enforced_predicate"),
                )
                .map_err(|error| error.to_string())?,
        ),
        unenforced_predicate: Some(
            encoder
                .encode_tuple_domain(
                    &encoding.unenforced_predicate,
                    FieldPath::root("connector_table_scan_source").field("unenforced_predicate"),
                )
                .map_err(|error| error.to_string())?,
        ),
        remaining_expression: encoding
            .remaining_expression
            .as_ref()
            .map(encode_connector_expression),
        dynamic_filters: dynamic_filters
            .iter()
            .map(|(filter_id, value)| {
                let ordinal = provider_outputs
                    .iter()
                    .position(|(_, output)| output == value)
                    .ok_or_else(|| {
                        format!(
                            "runtime filter {filter_id} constrains a value this scan does not produce"
                        )
                    })?;
                Ok(dto::DynamicFilterBinding {
                    filter_id: *filter_id,
                    variable: encoding.assignments[ordinal].variable().to_owned(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        max_batch_rows,
        max_batch_bytes,
        work_source: match encoding.work_source {
            ConnectorReadWorkSource::RuntimeSplits => dto::ScanWorkSource::RuntimeSplits as i32,
            ConnectorReadWorkSource::WholeRelation => dto::ScanWorkSource::WholeRelation as i32,
        },
    };
    ConnectorTableScanSource::parse(raw, FieldPath::root("connector_table_scan_source"))
        .map_err(|error| error.to_string())
}

fn table_def(
    encoding: &FrozenReadEncoding,
    source: ConnectorTableScanSource,
) -> Result<plan::TableDef, String> {
    Ok(plan::TableDef {
        name: encoding.identity.table.clone(),
        columns: encoding
            .columns
            .iter()
            .map(|(_, column)| column_def(column))
            .collect::<Result<Vec<_>, String>>()?,
        // Row-lineage metadata columns are an Iceberg read shape the completion
        // contract states as ordinary projected columns, so there is no second
        // list to fill here.
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: Some(plan::ScanSource {
            kind: Some(plan::scan_source::Kind::TypedConnectorRead(
                source.as_proto().clone(),
            )),
        }),
    })
}

fn column_def(
    column: &novarocks_sql::compiler::ProviderReadColumnNeed,
) -> Result<plan::ColumnDef, String> {
    Ok(plan::ColumnDef {
        name: column.name().to_string(),
        data_type: Some(
            novarocks_plan_codec::encode_native_type(&novarocks_types::undecorated_nested_type(
                &column.engine_type().data_type,
            ))
            .map_err(|error| error.to_string())?,
        ),
        nullable: column.engine_type().nullable,
        // No decoder consumes this deprecated field, and a write default
        // reaches execution through the provider schema instead.
        write_default_json: None,
        // The completion contract states a column's engine type outright, so
        // there is no Arrow representation left to disambiguate.
        logical_type: None,
    })
}

/// Each column the scan produces, in the order the plan produces them.
fn scan_columns(
    encoding: &FrozenReadEncoding,
    provider_outputs: &[(ProviderColumnReference, novarocks_physical_plan::ValueId)],
) -> Result<Box<[PhysicalV1ScanColumn]>, String> {
    provider_outputs
        .iter()
        .map(|(reference, _)| {
            let (_, column) = encoding
                .columns
                .iter()
                .find(|(frozen, _)| frozen == reference)
                .ok_or_else(|| {
                    "completed plan scan produces a provider column its freeze did not project"
                        .to_string()
                })?;
            Ok(PhysicalV1ScanColumn {
                column: reference.clone(),
                name: column.name().into(),
                // The plan states a column's type without the provider's own
                // decoration on its nested fields, and this fact stands beside
                // the plan's values.
                ty: novarocks_physical_plan::ValueType::new(
                    novarocks_types::undecorated_nested_type(&column.engine_type().data_type),
                    column.engine_type().nullable,
                ),
                connector_type: column.connector_type(),
                // A scan produces relation columns; `internal` marks a writer
                // relation value, which a read never carries.
                internal: false,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_physical_plan::{
        MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId, ScanReadBudget,
    };
    use novarocks_query_application::preparation::{
        FinalPlanCompletionDriver, ReadAccessSink, SqlCompletionFactSource,
    };
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
        SqlFactBatch, SqlFinalPlanCompileRequest, SqlNeedBatch, SqlPlanningEnvironment,
        SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog,
        noop_constant_evaluator,
    };
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;

    struct NoFacts;

    #[async_trait::async_trait]
    impl SqlCompletionFactSource for NoFacts {
        type Access = FrozenProviderRead;

        async fn resolve(
            &self,
            _: &SqlNeedBatch,
            _: &ReadAccessSink<FrozenProviderRead>,
        ) -> Result<SqlFactBatch, String> {
            panic!("a VALUES statement asks for nothing")
        }
    }

    /// A statement over literal rows completes, encodes, and asks for no
    /// capability - which is the whole chain from SQL text to wire form with
    /// nothing in it that a provider had to answer.
    #[test]
    fn a_statement_over_literal_rows_reaches_the_wire_with_no_capability() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let (_root, scope) = query_scope();
        let completed = runtime
            .block_on(FinalPlanCompletionDriver::new(Arc::new(NoFacts)).complete(request(), &scope))
            .unwrap_or_else(|error| panic!("VALUES completes without facts: {error}"));
        let plan_fragments = completed.candidate().plan().fragments().len();

        let freezes = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
        let encoded = encode_completed_plan(
            completed,
            &novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin engine function catalog"),
            None,
        )
        .expect("a completed plan encodes");
        // The shape is the distributed one - rows are produced somewhere and
        // gathered at the result - and nothing in it had to be frozen.
        assert!(plan_fragments >= 2);
        assert_eq!(
            crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread() - freezes,
            plan_fragments,
            "encoding freezes each fragment's static plan exactly once"
        );
        assert_eq!(encoded.access.iter().count(), 0);
        assert!(encoded.plan_facts.scans().is_empty());
        // Scheduling sees the same fragments, in the same order, and reads no
        // scan because there is none.
        assert_eq!(
            encoded.plan_facts.scheduling().fragments.len(),
            plan_fragments
        );
        assert_eq!(
            encoded.plan_facts.scheduling().order,
            encoded.topology.order
        );
        assert!(
            encoded
                .plan_facts
                .scheduling()
                .fragments
                .values()
                .all(|fragment| !fragment.has_scans())
        );
        // The submission bundle is the same fragment set, keyed.
        assert_eq!(encoded.native.fragment_ids().count(), plan_fragments);
    }

    /// A static plan is frozen once per plan, not once per attempt: every
    /// attempt of one template - a recovery included - is handed the very
    /// bytes the encoding froze, and making an attempt freezes nothing.
    #[test]
    fn every_attempt_of_a_template_shares_the_fragments_frozen_at_encoding() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let (_root, scope) = query_scope();
        let completed = runtime
            .block_on(FinalPlanCompletionDriver::new(Arc::new(NoFacts)).complete(request(), &scope))
            .unwrap_or_else(|error| panic!("VALUES completes without facts: {error}"));
        let version = completed.candidate().plan().version();
        let encoded = encode_completed_plan(
            completed,
            &novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin engine function catalog"),
            None,
        )
        .expect("a completed plan encodes");
        let template = encoded.into_attempt_template(version);

        let freezes = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
        let first = template.instantiate();
        let recovery = template.instantiate();
        assert_eq!(
            crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread(),
            freezes,
            "making an attempt freezes no static plan"
        );
        assert!(
            first.shares_native_fragments_with(&recovery),
            "a recovery attempt creates its tasks from the first attempt's frozen bytes"
        );
    }

    /// Rows are produced somewhere and gathered where the query reads them, so
    /// the producer comes first, one fragment ends the execution, and that
    /// fragment is where the result is.
    #[test]
    fn a_distributed_statement_orders_its_producers_before_its_result() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let (_root, scope) = query_scope();
        let completed = runtime
            .block_on(FinalPlanCompletionDriver::new(Arc::new(NoFacts)).complete(request(), &scope))
            .unwrap_or_else(|error| panic!("VALUES completes without facts: {error}"));
        let topology = completed_plan_topology(completed.candidate().plan())
            .expect("a completed plan has a topology");

        assert_eq!(
            topology.order.len(),
            completed.candidate().plan().fragments().len()
        );
        assert_eq!(topology.result, Some(topology.anchor));
        assert!(!topology.producers.contains(&topology.anchor));
        assert_eq!(
            topology.order.last().copied(),
            Some(topology.anchor),
            "the fragment that ends the execution is ordered last"
        );
    }

    fn query_scope() -> (
        novarocks_workload_control::RootWork,
        novarocks_workload_control::WorkScope,
    ) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        control.mark_ready().expect("workload control ready");
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("query root");
        let scope = root.owner.scope();
        (root, scope)
    }

    fn request() -> SqlFinalPlanCompileRequest {
        SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([9; 16]).expect("plan version"),
            SqlStatementInput::sql("SELECT 1"),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            noop_constant_evaluator(),
            SqlCompileControl::unbounded(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: MAX_SCAN_BATCH_ROWS,
                max_batch_bytes: MAX_SCAN_BATCH_BYTES,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
    }
}
