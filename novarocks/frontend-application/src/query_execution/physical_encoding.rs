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

// Design: ADR-0185 (docs/adr/ADR-0185-compiled-packages-are-the-sole-static-plan-path.md)

#[cfg(test)]
use crate::query_execution::completion_facts::test_support::FinalPlanCompletionDriverTestExt;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use novarocks_physical_plan::PlanVersionId;
use novarocks_physical_plan::{
    Distribution, EdgeKind, FragmentId, FragmentSink, NodeId, NodeKind, PhysicalPlan,
    ProviderReadOccurrenceId, WriteTargetOrdinal,
};
use novarocks_query_application::preparation::CompletedPlanWithAccess;

use crate::native::fragment_encoder::submission::freeze_completed_packages;
use crate::query_execution::artifact::native_submission::{
    NativeSubmissionFragmentRole, SubmissionFragmentFacts, SubmissionPlanFacts,
};
use crate::query_execution::attempt_plan_facts::{
    AttemptEdgeFacts, AttemptPartitionKind, AttemptPlanFacts, AttemptScanFacts, PlanOutputColumn,
};
use crate::query_execution::attempt_runtime_filter_facts::AttemptRuntimeFilterFacts;
use crate::query_execution::fragment_scheduling::{
    FragmentSchedulingFacts, SchedulingEdgeFacts, SchedulingFragmentFacts, SchedulingScanFacts,
    SchedulingStreamKind,
};
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::package_freeze::{CompiledPackageCarrier, extract_checked_packages};
use crate::query_execution::package_writes::{WriteRecipeSession, author_frozen_writes};
use crate::query_execution::post_compile::mint_native_encoding_provenance;
use crate::query_execution::preparation::attempt_access::{
    ConnectorAttemptAccessPlan, FrozenReadCapability, attempt_access_for_completed_plan,
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
    #[cfg(test)]
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

/// The exact session that authors the package's frozen writer recipes.
pub(crate) struct WriteTargetFacts<'a> {
    pub(crate) session: &'a dyn WriteRecipeSession,
}

/// Freeze one completed plan into compiled packages under the host's explicit
/// admission and encode limits and the statement's own constant policy.
// Design: ADR-0153 (docs/adr/ADR-0153-completed-physical-plan-is-the-static-execution-authority.md)
pub(crate) fn encode_completed_plan(
    paired: CompletedPlanWithAccess<FrozenProviderRead>,
    carrier: &CompiledPackageCarrier,
    statement_constant_policy: novarocks_physical_plan::ConstantPolicy,
    write_targets: Option<&WriteTargetFacts<'_>>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<EncodedCompletedPlan, novarocks_plan_codec::PhysicalEncodeError> {
    control.checkpoint(novarocks_type_contract::CompilePhase::Encode, 0)?;
    let result = encode_completed_packages(
        paired,
        carrier,
        statement_constant_policy,
        write_targets,
        control,
    );
    if matches!(
        &result,
        Err(novarocks_plan_codec::PhysicalEncodeError::Control(_))
    ) || result
        .as_ref()
        .err()
        .is_some_and(crate::query_execution::package_freeze::is_receiver_refusal)
    {
        return result;
    }
    control.checkpoint(novarocks_type_contract::CompilePhase::Encode, 0)?;
    result
}

/// What each frozen read leaves the encoding and what it leaves the attempt,
/// separated by occurrence.
///
/// The capability moves rather than copies, because a capability cannot be
/// copied; the encoding stays behind for whichever carrier reads it.
pub(crate) fn split_frozen_reads(
    reads: novarocks_query_application::preparation::FinalPlanRuntimeAccess<FrozenProviderRead>,
) -> (
    BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
    BTreeMap<ProviderReadOccurrenceId, FrozenReadCapability>,
) {
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
    (encodings, capabilities)
}

/// Freeze one completed plan as compiled packages, one per fragment.
///
/// Every fact here comes from the physical plan, the reads its scans froze,
/// the write session that sealed its targets and the packages authored from
/// them; no plan tree is built or consulted. What the package carrier cannot
/// express yet is refused before any package is authored.
fn encode_completed_packages(
    paired: CompletedPlanWithAccess<FrozenProviderRead>,
    carrier: &CompiledPackageCarrier,
    statement_constant_policy: novarocks_physical_plan::ConstantPolicy,
    write_targets: Option<&WriteTargetFacts<'_>>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<EncodedCompletedPlan, novarocks_plan_codec::PhysicalEncodeError> {
    let (candidate, reads) = paired.into_parts();
    let (encodings, capabilities) = split_frozen_reads(reads);
    let parts = encode_static_packages(
        candidate,
        &encodings,
        carrier,
        statement_constant_policy,
        write_targets,
        control,
    )?;
    parts.bind_capabilities(capabilities)
}

/// Static output only. The original linear Provider access stays in its
/// caller's stable Source throughout the closed worker call and its unwind.
pub(crate) struct EncodedStaticPlan {
    candidate: novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
    native: NativeFragmentAttachment,
    #[cfg(test)]
    topology: CompletedPlanTopology,
    plan_facts: AttemptPlanFacts,
}
/// A neutral static plan with its exact linear-read mapping checked while the
/// original access still resides in the caller's stable source.
pub(crate) struct PreparedStaticPlan {
    encoded: EncodedStaticPlan,
    access: crate::query_execution::preparation::attempt_access::PreparedAttemptAccess,
}
impl PreparedStaticPlan {
    pub(crate) fn candidate(
        &self,
    ) -> &novarocks_query_application::preparation::CompletedPhysicalPlanCandidate {
        &self.encoded.candidate
    }
    pub(crate) fn scheduling_facts(
        &self,
        version: PlanVersionId,
    ) -> Result<novarocks_query_application::api::ExecutionSchedulingFacts, String> {
        self.encoded
            .plan_facts
            .scheduling()
            .attempt_scheduling_facts(novarocks_query_application::api::PlanSeal::Version(version))
    }
    pub(crate) fn bind_access(
        self,
        access: novarocks_query_application::preparation::FinalPlanRuntimeAccess<
            FrozenProviderRead,
        >,
    ) -> EncodedCompletedPlan {
        let (_, capabilities) = split_frozen_reads(access);
        self.bind_capabilities(capabilities)
    }
    /// The original Source retired the encodings on its CPU turn and retains
    /// these same capabilities until that turn's actual exit. This checked
    /// receiving step performs no opaque encoding destructor.
    pub(crate) fn bind_capabilities(
        self,
        capabilities: BTreeMap<ProviderReadOccurrenceId, FrozenReadCapability>,
    ) -> EncodedCompletedPlan {
        let access = self
            .access
            .bind(self.encoded.candidate.plan(), capabilities);
        EncodedCompletedPlan {
            semantic_candidate: self.encoded.candidate,
            native: self.encoded.native,
            #[cfg(test)]
            topology: self.encoded.topology,
            plan_facts: self.encoded.plan_facts,
            access,
        }
    }
}
impl EncodedStaticPlan {
    pub(crate) fn prepare_access(
        self,
        access: &novarocks_query_application::preparation::FinalPlanRuntimeAccess<
            FrozenProviderRead,
        >,
    ) -> Result<PreparedStaticPlan, novarocks_plan_codec::PhysicalEncodeError> {
        let mapping =
            crate::query_execution::preparation::attempt_access::PreparedAttemptAccess::prepare(
                self.candidate.plan(),
                access,
            )?;
        Ok(PreparedStaticPlan {
            encoded: self,
            access: mapping,
        })
    }
    fn bind_capabilities(
        self,
        capabilities: BTreeMap<ProviderReadOccurrenceId, FrozenReadCapability>,
    ) -> Result<EncodedCompletedPlan, novarocks_plan_codec::PhysicalEncodeError> {
        let access = attempt_access_for_completed_plan(self.candidate.plan(), capabilities)?;
        Ok(EncodedCompletedPlan {
            semantic_candidate: self.candidate,
            native: self.native,
            #[cfg(test)]
            topology: self.topology,
            plan_facts: self.plan_facts,
            access,
        })
    }
}

/// The same encoder with borrowed original access. Only neutral encoding facts
/// are copied; no Provider access/generation owner moves onto the CPU stack.
pub(crate) fn encode_completed_plan_borrowed(
    candidate: novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
    access: &novarocks_query_application::preparation::FinalPlanRuntimeAccess<FrozenProviderRead>,
    carrier: &CompiledPackageCarrier,
    policy: novarocks_physical_plan::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<EncodedStaticPlan, novarocks_plan_codec::PhysicalEncodeError> {
    encode_borrowed_original(candidate, access, carrier, policy, None, control)
}

pub(crate) fn encode_completed_write_plan_borrowed(
    candidate: novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
    access: &novarocks_query_application::preparation::FinalPlanRuntimeAccess<FrozenProviderRead>,
    carrier: &CompiledPackageCarrier,
    policy: novarocks_physical_plan::ConstantPolicy,
    write_targets: &WriteTargetFacts<'_>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<EncodedStaticPlan, novarocks_plan_codec::PhysicalEncodeError> {
    encode_borrowed_original(
        candidate,
        access,
        carrier,
        policy,
        Some(write_targets),
        control,
    )
}

fn encode_borrowed_original(
    candidate: novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
    access: &novarocks_query_application::preparation::FinalPlanRuntimeAccess<FrozenProviderRead>,
    carrier: &CompiledPackageCarrier,
    policy: novarocks_physical_plan::ConstantPolicy,
    write_targets: Option<&WriteTargetFacts<'_>>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<EncodedStaticPlan, novarocks_plan_codec::PhysicalEncodeError> {
    control.checkpoint(novarocks_type_contract::CompilePhase::Encode, 0)?;
    let mut encodings = BTreeMap::new();
    for fragment in candidate.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Scan { occurrence, .. } = &node.kind {
                let read = access.get(*occurrence).ok_or_else(|| {
                    novarocks_plan_codec::PhysicalEncodeError::from(
                        "completed plan has no original read access".to_owned(),
                    )
                })?;
                encodings
                    .entry(*occurrence)
                    .or_insert_with(|| read.access.encoding.clone());
            }
        }
    }
    if encodings.len() != access.len() {
        return Err("completed plan has unused original read access"
            .to_owned()
            .into());
    }
    let result = encode_static_packages(
        candidate,
        &encodings,
        carrier,
        policy,
        write_targets,
        control,
    );
    if matches!(
        &result,
        Err(novarocks_plan_codec::PhysicalEncodeError::Control(_))
    ) || result
        .as_ref()
        .err()
        .is_some_and(crate::query_execution::package_freeze::is_receiver_refusal)
    {
        return result;
    }
    control.checkpoint(novarocks_type_contract::CompilePhase::Encode, 0)?;
    result
}

// Sole package/Native fact author for both consuming and borrowing callers.
fn encode_static_packages(
    candidate: novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
    encodings: &BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
    carrier: &CompiledPackageCarrier,
    statement_constant_policy: novarocks_physical_plan::ConstantPolicy,
    write_targets: Option<&WriteTargetFacts<'_>>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<EncodedStaticPlan, novarocks_plan_codec::PhysicalEncodeError> {
    let plan = candidate.plan();
    refuse_uncompiled_plan_shapes(plan)?;
    let topology = completed_plan_topology(plan)?;
    // Each written target's frozen recipe comes from the session that sealed
    // it; a plan that writes with no sealing session has no recipe author.
    let written = completed_plan_write_targets(plan);
    let writes = match (written.first(), write_targets) {
        (None, _) => BTreeMap::new(),
        (Some(target), None) => {
            return Err(format!(
                "completed plan writes target {} with no sealed write session",
                target.get()
            )
            .into());
        }
        (Some(_), Some(facts)) => author_frozen_writes(plan, facts.session, control)?,
    };
    let packages = extract_checked_packages(
        &candidate,
        encodings,
        &writes,
        statement_constant_policy,
        carrier.admission(),
        control,
    )?;
    let submission = compiled_plan_submission_facts(&candidate, &topology)?;
    let provenance = mint_native_encoding_provenance();
    let native = NativeFragmentAttachment::for_completed_plan(
        freeze_completed_packages(
            packages,
            plan,
            &submission,
            topology.anchor,
            carrier,
            control,
        )?,
        topology.anchor,
    )?;
    // Scan scheduling and the attempt's scan facts are read from the plan and
    // the freezes; no wire encoding is consulted.
    let scheduling = completed_plan_scheduling_facts(plan, encodings, &topology, provenance)?;
    let plan_facts = AttemptPlanFacts::from_completed(
        scheduling,
        completed_plan_edge_facts(plan)?,
        completed_plan_scan_facts(plan, encodings)?,
        submission,
        // The attempt deploys the plan's filters by the same plan-global
        // binding numbering each package's cuts carry.
        AttemptRuntimeFilterFacts::from_completed(plan)?,
        // A plan that writes states which targets its root delivers, because
        // that is what the commit is taken over; a read plan says none.
        Some(written).filter(|targets| !targets.is_empty()),
    );
    Ok(EncodedStaticPlan {
        candidate,
        native,
        #[cfg(test)]
        topology,
        plan_facts,
    })
}

/// Refuse what the compiled package carrier cannot express yet.
///
/// Each refusal names the missing owner. None of them is a reason to encode
/// the plan another way: the backend interprets only the carrier its own
/// composition chose.
fn refuse_uncompiled_plan_shapes(
    plan: &PhysicalPlan,
) -> Result<(), novarocks_plan_codec::PhysicalEncodeError> {
    // Every node family has a package author: what the local compiler cannot
    // lower yet is refused by the backend's compiler, by name.
    for fragment in plan.fragments().values() {
        if let Some(reason) = compiled_sink_refusal(fragment.sink()) {
            return Err(
                novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(reason.into()),
            );
        }
    }
    for edge in plan.edges().values() {
        if let Some(reason) = compiled_edge_refusal(edge.kind) {
            return Err(
                novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(reason.into()),
            );
        }
    }
    Ok(())
}

const fn compiled_sink_refusal(sink: &FragmentSink) -> Option<&'static str> {
    match sink {
        FragmentSink::Multicast { .. } => {
            Some("the compiled package carrier does not express CTE multicast sinks yet")
        }
        FragmentSink::Result
        | FragmentSink::RootResult(_)
        | FragmentSink::Stream { .. }
        | FragmentSink::Router { .. }
        | FragmentSink::Noop => None,
    }
}

const fn compiled_edge_refusal(kind: EdgeKind) -> Option<&'static str> {
    match kind {
        EdgeKind::Stream | EdgeKind::ChangeStreamRouter => None,
        EdgeKind::CteMulticast => {
            Some("the compiled package carrier does not express CTE multicast edges yet")
        }
    }
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

/// The per-fragment submission facts of one completed plan and the fragments
/// that feed a stream edge, both read from the plan alone.
fn completed_plan_submission_fragments(
    plan: &PhysicalPlan,
    public_result: Option<&novarocks_physical_plan::ResultPort>,
) -> Result<(Vec<SubmissionFragmentFacts>, BTreeSet<u32>), novarocks_plan_codec::PhysicalEncodeError>
{
    let mut stream_edge_sources = BTreeSet::new();
    for edge in plan.edges().values() {
        match edge.kind {
            EdgeKind::Stream | EdgeKind::ChangeStreamRouter => {
                stream_edge_sources.insert(edge.source.fragment.get());
            }
            EdgeKind::CteMulticast => {}
        }
    }
    let mut fragments = Vec::with_capacity(plan.fragments().len());
    for fragment in plan.fragments().values() {
        let role = match fragment.sink() {
            FragmentSink::Result | FragmentSink::RootResult(_) => {
                NativeSubmissionFragmentRole::Result
            }
            FragmentSink::Stream { .. }
            | FragmentSink::Multicast { .. }
            | FragmentSink::Router { .. } => NativeSubmissionFragmentRole::NonTerminal,
            other => {
                return Err(format!(
                    "completed plan fragment {} has sink {other:?}, which this path does not submit",
                    fragment.id().get()
                ).into());
            }
        };
        fragments.push(SubmissionFragmentFacts::for_completed_plan(
            fragment.id().get(),
            role,
            completed_fragment_output_columns(public_result, fragment.id()),
        ));
    }
    Ok((fragments, stream_edge_sources))
}

/// What submission encoding reads about a plan frozen into compiled packages.
///
/// It is derived from the physical plan alone. Every declared routed output
/// keeps its original producer; no placement or receiver is patched into the
/// shared package. Unsupported sinks retain their separate admission refusal.
fn compiled_plan_submission_facts(
    candidate: &novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
    topology: &CompletedPlanTopology,
) -> Result<SubmissionPlanFacts, novarocks_plan_codec::PhysicalEncodeError> {
    let plan = candidate.plan();
    let (fragments, stream_edge_sources) =
        completed_plan_submission_fragments(plan, candidate.original_public_result_port())?;
    Ok(SubmissionPlanFacts::for_completed_plan(
        topology.order.clone(),
        fragments,
        stream_edge_sources,
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
    public_result: Option<&novarocks_physical_plan::ResultPort>,
    fragment_id: novarocks_physical_plan::FragmentId,
) -> Vec<PlanOutputColumn> {
    public_result
        .filter(|result| result.fragment == fragment_id)
        .map(|result| {
            result
                .fields
                .iter()
                .map(|field| PlanOutputColumn {
                    name: field.alias.as_deref().unwrap_or(&field.name).to_string(),
                    ty: field.ty.clone(),
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
        .map(|id| (id.get(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut consumers_of = BTreeMap::<u32, Vec<u32>>::new();
    let mut producers = BTreeSet::new();
    for edge in plan.edges().values() {
        let source = edge.source.fragment.get();
        let destination = edge.destination.fragment.get();
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
        result: plan.result_port().map(|result| result.fragment.get()),
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
                runtime_splits: Some(encoding.placement),
            });
        }
        fragments.insert(
            fragment.id().get(),
            SchedulingFragmentFacts {
                placement: fragment.stage_placement().clone(),
                scans,
            },
        );
    }
    let edges = plan
        .edges()
        .values()
        .map(|edge| {
            Ok(SchedulingEdgeFacts {
                source: edge.source.fragment.get(),
                target: edge.destination.fragment.get(),
                target_exchange_node_id: wire_node_id(edge.destination.node)?,
                native_hash_partitioned: matches!(
                    edge.partitioning.destination,
                    Distribution::Hash { .. } | Distribution::ConditionalKey { .. }
                ),
                stream_kind: match edge.partitioning.destination {
                    Distribution::Singleton => SchedulingStreamKind::Gather,
                    Distribution::Broadcast => SchedulingStreamKind::Broadcast,
                    Distribution::Hash { .. }
                    | Distribution::ConditionalKey { .. }
                    | Distribution::BucketShuffle { .. } => SchedulingStreamKind::Partitioned,
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
    // The physical plan owns the one global binding numbering. This FE
    // projection reads only scan consumers and imposes no carrier restrictions
    // on other endpoints (including ordered JoinBuildKey producers).
    let mut runtime_filters =
        BTreeMap::<(FragmentId, NodeId), Vec<(u32, novarocks_physical_plan::ValueId)>>::new();
    for binding in
        novarocks_physical_plan::runtime_filter_bindings(plan).map_err(|error| error.to_string())?
    {
        let novarocks_physical_plan::RuntimeFilterBindingRole::Consumer(index) = binding.role
        else {
            continue;
        };
        let consumer = &plan.runtime_filters()[&binding.filter].consumers[index];
        if consumer.apply_point != novarocks_physical_plan::RuntimeFilterApplyPoint::ScanSource {
            continue;
        }
        let [value] = consumer.endpoint.values.as_ref() else {
            return Err(format!(
                "scan runtime filter endpoint at fragment {} node {} requires exactly one value",
                binding.fragment.get(),
                binding.node.get()
            ));
        };
        runtime_filters
            .entry((binding.fragment, binding.node))
            .or_default()
            .push((binding.binding_id, *value));
    }
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
            let fragment_id = fragment.id().get();
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
                source_fragment_id: edge.source.fragment.get(),
                target_fragment_id: edge.destination.fragment.get(),
                target_exchange_node_id: wire_node_id(edge.destination.node)?,
                partition_kind: completed_edge_partition_kind(&edge.partitioning.destination),
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

/// How the destination of one completed-plan edge is partitioned, in the
/// vocabulary the wire stream type is named by.
///
/// A broadcast edge is unpartitioned: every destination receives every row,
/// which is a property of the stream rather than of the partitioning, and the
/// sealed plan says the same thing about its own broadcast edges.
const fn completed_edge_partition_kind(destination: &Distribution) -> AttemptPartitionKind {
    match destination {
        Distribution::Singleton | Distribution::Broadcast => AttemptPartitionKind::Unpartitioned,
        Distribution::Hash { .. }
        | Distribution::ConditionalKey { .. }
        | Distribution::BucketShuffle { .. } => AttemptPartitionKind::Hash,
        Distribution::Unconstrained | Distribution::RoundRobin => AttemptPartitionKind::Random,
    }
}

fn wire_node_id(node: NodeId) -> Result<i32, String> {
    i32::try_from(node.get())
        .map_err(|_| format!("scan node {} exceeds the wire node identity", node.get()))
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
            .block_on(
                FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                    .complete_for_test(request(), &scope),
            )
            .unwrap_or_else(|error| panic!("VALUES completes without facts: {error}"));
        let plan_fragments = completed.candidate().plan().fragments().len();

        let freezes = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
        let encoded = encode_completed_plan(
            completed,
            &compiled_carrier(),
            crate::application::test_constant_policy(),
            None,
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
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

    /// The same completed statement freezes into one checked v2 package per
    /// fragment; the receiver reads each back through the original
    /// constructors and the sender reproduces the exact bytes.
    /// Complete a table-free statement and freeze its v2 package bytes,
    /// returning the plan's fragment ids beside them.
    fn frozen_packages(
        sql: &str,
    ) -> (
        Vec<novarocks_physical_plan::FragmentId>,
        std::collections::BTreeMap<novarocks_physical_plan::FragmentId, Vec<u8>>,
    ) {
        use novarocks_plan_codec::physical_package_v2::test_support::encode_limits;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let (_root, scope) = query_scope();
        let completed = runtime
            .block_on(
                FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                    .complete_for_test(request_for(sql), &scope),
            )
            .unwrap_or_else(|error| panic!("{sql}: completes without facts: {error}"));
        let control = SqlCompileControl::unbounded();
        let packages = crate::query_execution::package_freeze::freeze_fragment_packages(
            completed.candidate(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            crate::application::test_constant_policy(),
            &test_package_admission(),
            &encode_limits(),
            &control,
        )
        .unwrap_or_else(|error| panic!("{sql}: checked v2 packages: {error}"));
        let fragments = completed
            .candidate()
            .plan()
            .fragments()
            .keys()
            .copied()
            .collect();
        (fragments, packages)
    }

    #[test]
    fn a_completed_statement_freezes_v2_packages_that_roundtrip_through_the_receiver() {
        use novarocks_plan_codec::physical_package_v2::test_support::{
            decode_limits, encode_limits,
        };
        use novarocks_plan_codec::physical_package_v2::{
            decode_fragment_package, encode_fragment_package,
        };
        use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
        use prost::Message;

        let (fragments, packages) = frozen_packages("SELECT 1");
        let control = SqlCompileControl::unbounded();
        assert_eq!(packages.keys().copied().collect::<Vec<_>>(), fragments);
        let model = FragmentDecodeResourceModel::try_new(&control).expect("decode model");
        for (id, bytes) in &packages {
            let decoded = decode_fragment_package(bytes, &model, &decode_limits(), &control)
                .unwrap_or_else(|error| panic!("fragment {id:?} receives: {error}"));
            assert_eq!(decoded.fragment().id(), *id);
            let again = encode_fragment_package(&decoded, &encode_limits(), &control)
                .expect("re-encode")
                .encode_to_vec();
            assert_eq!(&again, bytes, "fragment {id:?} roundtrips byte-identically");
        }
    }

    /// A real sealed RAND-only subset; a table-free statement binds no
    /// function, but the compiler refuses an empty pure catalog. This is not
    /// the Server catalogue.
    fn sealed_rand_subset() -> novarocks_functions::PureEngineFunctionCatalog {
        use novarocks_functions::{
            EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
            InstalledPureKernel, PureImplementationDeclaration, PureImplementationId,
            PureKernelAbi,
        };
        let actual =
            novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
                .expect("builtin catalogue");
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                actual
                    .definition("rand", FunctionKind::Scalar)
                    .expect("rand definition")
                    .clone(),
            )
            .expect("register rand");
        builder
            .seal_pure(
                [
                    "builtin.scalar/rand/()->f64;strict;legacy",
                    "builtin.scalar/rand/(i64)->f64;strict;legacy",
                ]
                .into_iter()
                .map(|overload| InstalledPureKernel {
                    function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                    kind: FunctionKind::Scalar,
                    implementation: PureImplementationDeclaration {
                        overload: FunctionOverloadId::try_new(overload).unwrap(),
                        implementation: PureImplementationId::try_new(
                            "builtin.scalar/rand/selected-v1",
                        )
                        .unwrap(),
                        abi: PureKernelAbi::ScalarV1,
                    },
                    aggregate_state_format: None,
                }),
            )
            .expect("sealed rand subset")
    }

    /// Package admission within the test encode limits' source/coexistence
    /// envelope: the package's retained source floor travels in the sender's
    /// cumulative source invoice. This is not a production sizing.
    fn test_package_admission() -> novarocks_physical_plan::FragmentPackageAdmission {
        novarocks_physical_plan::FragmentPackageAdmission {
            plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
            source_retained_bytes: 64 << 20,
            property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
                max_request_bytes: 16 << 20,
                max_coexisting_bytes: 256 << 20,
                max_projection_work: 16 << 20,
            },
        }
    }

    /// The compiled package carrier under test sizing.
    fn compiled_carrier() -> CompiledPackageCarrier {
        CompiledPackageCarrier::for_test(
            test_package_admission(),
            novarocks_plan_codec::physical_package_v2::test_support::encode_limits(),
        )
    }

    /// Complete one table-free statement with the production Frontend
    /// constant evaluator.
    fn completed_for(sql: &str) -> CompletedPlanWithAccess<FrozenProviderRead> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let (_root, scope) = query_scope();
        runtime
            .block_on(
                FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                    .complete_for_test(request_for(sql), &scope),
            )
            .unwrap_or_else(|error| panic!("{sql}: completes without facts: {error}"))
    }

    fn encode_with(
        sql: &str,
        carrier: &CompiledPackageCarrier,
    ) -> (
        Arc<PhysicalPlan>,
        Result<EncodedCompletedPlan, novarocks_plan_codec::PhysicalEncodeError>,
    ) {
        let completed = completed_for(sql);
        let plan = Arc::clone(completed.candidate().plan());
        let encoded = encode_completed_plan(
            completed,
            carrier,
            crate::application::test_constant_policy(),
            None,
            &SqlCompileControl::unbounded(),
        );
        (plan, encoded)
    }

    /// The compiled carrier freezes every fragment of a real FE plan once, as
    /// a `FrozenFragment` that holds only the package. The task codec reads
    /// it as a compiled carrier, the package receiver reads the package back,
    /// and every fact the Frontend keeps equals the physical plan's.
    #[test]
    fn the_compiled_carrier_freezes_each_fragment_as_a_package_with_the_plans_facts() {
        use novarocks_plan_codec::physical_package_v2::decode_fragment_package;
        use novarocks_plan_codec::physical_package_v2::test_support::decode_limits;
        use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;

        let control = SqlCompileControl::unbounded();
        let model = FragmentDecodeResourceModel::try_new(&control).expect("decode model");
        for sql in [
            "SELECT 1",
            "SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2) t ORDER BY a",
        ] {
            let freezes = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
            let (plan, encoded) = encode_with(sql, &compiled_carrier());
            let encoded = encoded
                .unwrap_or_else(|error| panic!("{sql}: the compiled carrier encodes: {error}"));
            assert_eq!(
                crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread() - freezes,
                plan.fragments().len(),
                "{sql}: each fragment is frozen exactly once"
            );
            assert_eq!(
                encoded.native.fragment_ids().collect::<Vec<_>>(),
                plan.fragments()
                    .keys()
                    .map(|id| id.get())
                    .collect::<Vec<_>>(),
                "{sql}: one artifact per fragment"
            );
            assert_eq!(
                encoded.topology,
                completed_plan_topology(&plan).expect("topology")
            );
            assert!(encoded.native.carries_runtime_filter_bindings());
            assert!(encoded.plan_facts.scans().is_empty());
            assert_eq!(encoded.access.iter().count(), 0);
            assert_eq!(
                encoded.plan_facts.scheduling().order,
                encoded.topology.order
            );
            let (_, packages) = frozen_packages(sql);
            for fragment in plan.fragments().values() {
                let id = fragment.id().get();
                let artifact = encoded.native.get(id).expect("artifact");
                let facts = artifact.facts();
                assert_eq!(facts.fragment_id(), id);
                assert_eq!(facts.dop_domain(), fragment.dop_domain());
                assert_eq!(
                    facts.root_plan_node_id(),
                    i32::try_from(fragment.root().get()).unwrap()
                );
                assert!(!facts.declares_table_writer());
                assert!(facts.carries_runtime_filter_bindings());
                let expected_targets = match fragment.sink() {
                    FragmentSink::Result | FragmentSink::RootResult(_) => Vec::new(),
                    FragmentSink::Stream { edge } => {
                        let edge = &plan.edges()[edge];
                        vec![(
                            edge.destination.fragment.get(),
                            i32::try_from(edge.destination.node.get()).unwrap(),
                        )]
                    }
                    other => panic!("{sql}: unexpected sink {other:?}"),
                };
                assert_eq!(
                    facts
                        .sink_targets()
                        .iter()
                        .map(|target| (
                            target.target_fragment_id(),
                            target.target_exchange_node_id()
                        ))
                        .collect::<Vec<_>>(),
                    expected_targets,
                    "{sql}: fragment {id} sink branches are the plan edges' destinations"
                );

                let carried = novarocks_task_codec::creation::decode_static_package(
                    artifact.content(),
                    novarocks_proto_codec::FieldPath::root("frozen_fragment"),
                )
                .unwrap_or_else(|error| {
                    panic!("{sql}: fragment {id} is a compiled carrier: {error}")
                });
                assert_eq!(
                    carried.package().as_ref(),
                    packages[&fragment.id()].as_slice(),
                    "{sql}: fragment {id} carries exactly its frozen package"
                );
                let decoded =
                    decode_fragment_package(carried.package(), &model, &decode_limits(), &control)
                        .unwrap_or_else(|error| {
                            panic!("{sql}: fragment {id} package receives: {error}")
                        });
                assert_eq!(decoded.fragment().id(), fragment.id());
                assert_eq!(decoded.fragment().root(), fragment.root());
                assert_eq!(decoded.fragment().sink(), fragment.sink());
                assert_eq!(decoded.fragment().dop_domain(), fragment.dop_domain());
            }
        }
    }

    /// What the compiled carrier cannot express yet is refused by name before
    /// any package is authored, and is never encoded as a plan tree instead.
    /// A scan is expressed, from the read its freeze kept, and a writer from
    /// the session that sealed its target.
    #[test]
    fn compiled_router_uses_package_validation_and_multicast_remains_refused() {
        use novarocks_physical_plan::ValueId;

        let (plan, _) = crate::query_execution::package_writes::tests::insert_values_plan();
        refuse_uncompiled_plan_shapes(&plan)
            .expect("a writer and its finish have a compiled shape");
        assert!(
            compiled_sink_refusal(&FragmentSink::Router {
                summary: novarocks_physical_plan::LogicalSummaryOutput {
                    edge: novarocks_physical_plan::EdgeId::new(1),
                    columns: novarocks_physical_plan::LogicalSummaryColumns {
                        inserted: ValueId::new(1),
                        replaced: ValueId::new(2),
                        deleted: ValueId::new(3),
                    },
                },
                effect: ValueId::new(0),
                routes: Box::default(),
            })
            .is_none()
        );
        assert!(
            compiled_sink_refusal(&FragmentSink::Multicast {
                edges: Box::default(),
            })
            .is_some_and(|reason| reason.contains("multicast"))
        );
        assert_eq!(compiled_edge_refusal(EdgeKind::ChangeStreamRouter), None);
        assert!(
            compiled_edge_refusal(EdgeKind::CteMulticast)
                .is_some_and(|reason| reason.contains("multicast"))
        );
        assert_eq!(compiled_edge_refusal(EdgeKind::Stream), None);
        assert_eq!(compiled_sink_refusal(&FragmentSink::Result), None);

        // A validated provider-read program has a compiled shape; its frozen
        // read is authored from the freeze, and a scan with none is refused
        // there rather than given guessed facts.
        let plan = scan_plan();
        refuse_uncompiled_plan_shapes(&plan).expect("a scan has a compiled shape");
        assert!(matches!(
            crate::query_execution::package_reads::author_frozen_reads(
                &plan,
                &BTreeMap::new(),
                &SqlCompileControl::unbounded(),
            ),
            Err(crate::query_execution::package_freeze::PackageFreezeError::Facts(reason))
                if reason.contains("no frozen read")
        ));
        // A real FE plan has none of these shapes.
        let (plan, _) = encode_with("SELECT 1", &compiled_carrier());
        refuse_uncompiled_plan_shapes(&plan).expect("SELECT 1 has a compiled shape");
    }

    /// A real FE plan whose CTE feeds two consumers multicasts it. The
    /// compiled carrier refuses that plan by name and freezes nothing.
    #[test]
    fn a_multicast_plan_is_refused_by_the_compiled_carrier_and_freezes_nothing() {
        let sql = "WITH t AS (SELECT 1 AS a) SELECT a FROM t UNION ALL SELECT a FROM t";
        let freezes = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
        let (plan, refused) = encode_with(sql, &compiled_carrier());
        assert!(
            plan.edges()
                .values()
                .any(|edge| edge.kind == EdgeKind::CteMulticast),
            "{sql}: the CTE is multicast"
        );
        assert!(matches!(
            refused,
            Err(novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(reason))
                if reason.contains("multicast")
        ));
        assert_eq!(
            crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread(),
            freezes,
            "a refused plan freezes nothing"
        );
    }

    /// Decode the FE's own v2 bytes and compile every fragment with the
    /// independent local compiler, as a BE host would.
    fn compiled_programs(
        sql: &str,
    ) -> std::collections::BTreeMap<
        novarocks_physical_plan::FragmentId,
        novarocks_local_program::LocalProgram,
    > {
        use novarocks_connector_contract::PureProviderProgramCatalog;
        use novarocks_local_compiler::{
            LocalCompileOptions, compile_fragment, validate_fragment_providers,
        };
        use novarocks_plan_codec::physical_package_v2::decode_fragment_package;
        use novarocks_plan_codec::physical_package_v2::test_support::decode_limits;
        use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
        use std::num::NonZeroUsize;

        let (_, packages) = frozen_packages(sql);
        let control = SqlCompileControl::unbounded();
        let model = FragmentDecodeResourceModel::try_new(&control).expect("decode model");
        let functions = sealed_rand_subset();
        let providers =
            PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &control)
                .expect("empty provider catalog");
        packages
            .iter()
            .map(|(id, bytes)| {
                let decoded = decode_fragment_package(bytes, &model, &decode_limits(), &control)
                    .unwrap_or_else(|error| panic!("{sql}: fragment {id:?} receives: {error}"));
                let result_sink = decoded.result().is_some();
                let validated =
                    validate_fragment_providers(Arc::new(decoded), &providers, &control)
                        .unwrap_or_else(|error| {
                            panic!("{sql}: fragment {id:?} validates providers: {error}")
                        });
                let options = LocalCompileOptions {
                    pipeline_dop: NonZeroUsize::new(1).unwrap(),
                    // The frozen profile places a result sink on one driver.
                    root_sink_dop: result_sink.then(|| NonZeroUsize::new(1).unwrap()),
                    kernel_abi: novarocks_local_program::KernelAbiVersion::CURRENT,
                    constants: crate::application::test_constant_policy(),
                    exchange_wait: std::time::Duration::from_secs(120),
                };
                let program = compile_fragment(validated, &functions, options, &control)
                    .unwrap_or_else(|error| panic!("{sql}: fragment {id:?} compiles: {error}"));
                (*id, program)
            })
            .collect()
    }

    // The packages a real FE plan freezes compile into local programs whose
    // stream routing key is exactly the consumer's compiled receiver address.
    #[test]
    fn a_completed_statement_compiles_through_the_independent_local_compiler() {
        use novarocks_local_program::StaticSinkProgram;

        let programs = compiled_programs("SELECT 1");
        assert_eq!(programs.len(), 2, "SELECT 1 plans a producer and a gather");
        let mut routes = Vec::new();
        let mut receivers = Vec::new();
        for program in programs.values() {
            match program.graph().sink() {
                Some(StaticSinkProgram::DataStream { branch, .. }) => {
                    routes.push(branch.dest_node_id())
                }
                Some(StaticSinkProgram::Result) => {
                    receivers.extend(
                        program
                            .exchange_inputs()
                            .values()
                            .map(|input| i32::try_from(input.receiver_node).unwrap()),
                    );
                }
                other => panic!("unexpected compiled sink {other:?}"),
            }
        }
        assert_eq!(routes.len(), 1);
        assert_eq!(
            routes, receivers,
            "producer routes to the compiled receiver"
        );
    }

    // Real FE plans over table-free relations compile end to end.
    #[test]
    fn table_free_statements_compile_from_their_real_fe_packages() {
        for sql in [
            "SELECT 1 + 1",
            "SELECT 'x'",
            "SELECT a FROM (SELECT 1 AS a) t WHERE a > 0",
            "SELECT a FROM (SELECT 1 AS a) t WHERE a > 0 AND a < 5 OR a IS NULL",
            "SELECT CASE WHEN a > 1 THEN a ELSE 0 END FROM (SELECT 2 AS a) t",
            "SELECT 1 AS a UNION ALL SELECT 2",
            "SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2) t ORDER BY a",
            "SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2) t LIMIT 1",
            "SELECT a + 1 FROM (SELECT 1 AS a UNION ALL SELECT 2) t",
            "SELECT 'x' AS b UNION ALL SELECT 'yy'",
            "SELECT b FROM (VALUES ('x'), ('yy')) AS t(b)",
        ] {
            assert_eq!(compiled_programs(sql).len(), 2, "{sql}");
        }
    }

    // A multi-row VALUES keeps each literal at its analyzed type and assigns
    // the common type through an explicit cast cell; the compiler lowers each
    // such cell as a runtime cell root rather than refusing it.
    #[test]
    fn values_with_common_type_cast_cells_compile_as_runtime_cells() {
        let sql = "SELECT a FROM (VALUES (1), (2), (3)) AS t(a)";
        assert_eq!(compiled_programs(sql).len(), 2, "{sql}");
    }

    // Every literal cell of a multi-row VALUES adds type roots to the sender's
    // cumulative source invoice, which starts at the declared source floor,
    // and each constant record's writer charges whole-source metadata scans
    // against that invoice. Seven three-column rows, the shape the candidate
    // island first refused, take that charge past a fixed 1 GiB per-record
    // cap; record work is sized like its namespace's, so the statement
    // freezes under the compiled carrier and compiles on the receiver.
    #[test]
    fn multi_row_values_freeze_past_a_fixed_record_work_cap() {
        for sql in [
            "SELECT a, b, c FROM (VALUES (1, 10, 5), (2, 10, 7), (3, 20, 7), (4, 20, 8), \
             (5, 20, 1), (6, 3, 2), (7, 30, 9)) AS t(a, b, c)",
            "SELECT a, b FROM (VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), \
             (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12)) AS t(a, b)",
        ] {
            assert_eq!(compiled_programs(sql).len(), 2, "{sql}");
        }
    }

    // The native statistics seed projects these exact 1024 additions before
    // its writer. Exercise the common SQL-to-package path with the production
    // policy and ceilings; small test-carrier limits are not its admission.
    // This source-only test does not claim to execute the INSERT or its writer.
    #[test]
    fn wide_statistics_source_freezes_with_production_limits() {
        use novarocks_plan_codec::physical_package_v2::{
            FROZEN_CONSTANT_POLICY, FROZEN_PACKAGE_ADMISSION, PackageDecodeLimits,
            PackageEncodeLimits, decode_fragment_package,
        };
        use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;

        let projection = (0..1024)
            .map(|index| format!("generate_series + {}", index * 1_000_000))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {projection} FROM TABLE(generate_series(1, 4))");
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([19; 16]).expect("plan version"),
            SqlStatementInput::sql(&sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("native_statistics".into()),
                current_database: "native_statistics_db".into(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            crate::query_execution::constant_eval::constant_evaluator(),
            FROZEN_CONSTANT_POLICY,
            control.clone(),
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
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let (_root, scope) = query_scope();
        let completed = runtime
            .block_on(
                FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                    .complete_for_test(request, &scope),
            )
            .unwrap_or_else(|error| panic!("wide statistics source completion: {error:#?}"));
        let plan = completed.candidate().plan();
        assert_eq!(
            plan.result_port().expect("source result").fields.len(),
            1024
        );
        let fragment_ids = plan.fragments().keys().copied().collect::<Vec<_>>();
        let packages = crate::query_execution::package_freeze::freeze_fragment_packages(
            completed.candidate(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            FROZEN_CONSTANT_POLICY,
            &FROZEN_PACKAGE_ADMISSION,
            &PackageEncodeLimits::FROZEN,
            &control,
        )
        .unwrap_or_else(|error| panic!("wide statistics source package freeze: {error:#?}"));
        assert_eq!(packages.keys().copied().collect::<Vec<_>>(), fragment_ids);
        let model = FragmentDecodeResourceModel::try_new(&control).expect("decode model");
        for (id, bytes) in &packages {
            let decoded =
                decode_fragment_package(bytes, &model, &PackageDecodeLimits::FROZEN, &control)
                    .unwrap_or_else(|error| {
                        panic!("wide statistics source fragment {id:?} decode: {error:#?}")
                    });
            assert_eq!(decoded.fragment().id(), *id);
        }
    }

    #[test]
    fn completed_values_receiver_refusal_freezes_no_native_artifact_and_keeps_its_cause() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use novarocks_plan_codec::physical_package_v2::{
            FROZEN_PACKAGE_ADMISSION, PackageDecodeLimits, PackageEncodeLimits,
            encode_fragment_package,
        };
        use novarocks_plan_codec::resource_preflight_v2::{
            FragmentDecodeResourceModel, ResourceModelError,
        };
        use prost::Message;
        let control = SqlCompileControl::unbounded();
        let completed = completed_for("SELECT a FROM (VALUES (1),(2),(3)) t(a) ORDER BY a");
        let packages = crate::query_execution::package_freeze::extract_checked_packages(
            completed.candidate(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            crate::application::test_constant_policy(),
            &FROZEN_PACKAGE_ADMISSION,
            &control,
        )
        .unwrap();
        let model = Arc::new(FragmentDecodeResourceModel::try_new(&control).unwrap());
        let first = packages.first_key_value().unwrap().1;
        let bytes = encode_fragment_package(first, &PackageEncodeLimits::FROZEN, &control)
            .unwrap()
            .encode_to_vec();
        let usage = model
            .preflight(&bytes, PackageDecodeLimits::FROZEN.wire, &control)
            .unwrap()
            .usage;
        // A complete genuine producer DTO is over this receiver's exact tag
        // budget. Bytes are within production limits; this is not a byte-only gate.
        let mut receiver = PackageDecodeLimits::FROZEN.wire;
        receiver.max_field_occurrences = usage.field_occurrences.checked_sub(1).unwrap();
        let expected = model.preflight(&bytes, receiver, &control).unwrap_err();
        assert!(matches!(expected, ResourceModelError::Limit(_)));
        let carrier = CompiledPackageCarrier::new(
            FROZEN_PACKAGE_ADMISSION,
            PackageEncodeLimits::FROZEN,
            model,
            receiver,
        );
        struct TailRefusal {
            decoded: std::sync::atomic::AtomicBool,
        }
        impl novarocks_type_contract::PureCompileControl for TailRefusal {
            fn checkpoint(
                &self,
                phase: novarocks_type_contract::CompilePhase,
                _: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                use std::sync::atomic::Ordering;
                if phase == novarocks_type_contract::CompilePhase::Decode {
                    self.decoded.store(true, Ordering::SeqCst);
                }
                if phase == novarocks_type_contract::CompilePhase::Encode
                    && self.decoded.load(Ordering::SeqCst)
                {
                    return Err(novarocks_type_contract::CompileControlError::Cancelled);
                }
                Ok(())
            }
        }
        let tail = TailRefusal {
            decoded: std::sync::atomic::AtomicBool::new(false),
        };
        let before = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
        let error = match encode_completed_plan(
            completed,
            &carrier,
            crate::application::test_constant_policy(),
            None,
            &tail,
        ) {
            Err(error) => error,
            Ok(_) => panic!("receiver refusal must not produce a Native encoding"),
        };
        assert_eq!(error.failure_fact(), expected.failure_fact());
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        let novarocks_plan_codec::PhysicalEncodeError::Source(source) = error else {
            panic!("original resource author");
        };
        let freeze = source
            .downcast_ref::<crate::query_execution::package_freeze::PackageFreezeError>()
            .unwrap();
        assert!(
            matches!(freeze, crate::query_execution::package_freeze::PackageFreezeError::Receiver(actual) if actual == &expected)
        );
        // Without a frozen artifact or EncodedCompletedPlan the original
        // attempt/transport owner has no submission capability to dispatch.
        assert_eq!(
            crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread(),
            before
        );
    }

    #[test]
    fn final_package_preflight_keeps_every_original_fe_control_cause() {
        use novarocks_failure_contract::SharedFailureCause;
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct Reject {
            cause: CompileControlError,
        }
        impl PureCompileControl for Reject {
            fn checkpoint(&self, phase: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                if phase == CompilePhase::Decode {
                    Err(self.cause.clone())
                } else {
                    Ok(())
                }
            }
        }
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::Source(SharedFailureCause::new(
                novarocks_functions::ComparisonPrepareError::Unsupported,
            )),
        ] {
            let completed = completed_for("SELECT a FROM (VALUES (1),(2)) t(a)");
            let before = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
            let error = match encode_completed_plan(
                completed,
                &compiled_carrier(),
                crate::application::test_constant_policy(),
                None,
                &Reject {
                    cause: cause.clone(),
                },
            ) {
                Err(error) => error,
                Ok(_) => panic!("original control must refuse Native encoding"),
            };
            assert_eq!(
                error,
                novarocks_plan_codec::PhysicalEncodeError::Control(cause)
            );
            assert_eq!(
                crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread(),
                before
            );
        }
    }

    #[test]
    fn original_information_schema_scale_values_freeze_under_the_production_receiver() {
        use novarocks_plan_codec::physical_package_v2::{
            FROZEN_CONSTANT_POLICY, FROZEN_PACKAGE_ADMISSION, PackageDecodeLimits,
            PackageEncodeLimits, decode_fragment_package,
        };
        use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
        let rows = (0..32)
            .flat_map(|namespace| {
                (0..512).map(move |table| {
                    format!(
                        "('cl_normal','cl_ns_{namespace:04}','cl_table_{table:06}','BASE TABLE')"
                    )
                })
            })
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT COUNT(*) FROM (VALUES {rows}) t(table_catalog,table_schema,table_name,table_type) WHERE table_catalog='cl_normal' AND table_schema LIKE 'cl_ns_%'"
        );
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([23; 16]).unwrap(),
            SqlStatementInput::sql(&sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".into()),
                current_database: "db".into(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            crate::query_execution::constant_eval::constant_evaluator(),
            FROZEN_CONSTANT_POLICY,
            control.clone(),
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
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (_root, scope) = query_scope();
        let completed = runtime
            .block_on(
                FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                    .complete_for_test(request, &scope),
            )
            .unwrap();
        let packages = crate::query_execution::package_freeze::extract_checked_packages(
            completed.candidate(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            FROZEN_CONSTANT_POLICY,
            &FROZEN_PACKAGE_ADMISSION,
            &control,
        )
        .unwrap();
        let model = Arc::new(FragmentDecodeResourceModel::try_new(&control).unwrap());
        let carrier = CompiledPackageCarrier::new(
            FROZEN_PACKAGE_ADMISSION,
            PackageEncodeLimits::FROZEN,
            Arc::clone(&model),
            PackageDecodeLimits::FROZEN.wire,
        );
        assert!(packages.values().any(|package| {
            package
                .fragment()
                .nodes()
                .values()
                .any(|node| matches!(&node.kind, NodeKind::Values { rows } if rows.len() == 16_384))
        }));
        for (id, package) in packages {
            let bytes = crate::query_execution::package_freeze::encode_checked_package(
                &package, &carrier, &control,
            )
            .unwrap_or_else(|error| panic!("original-scale fragment {id:?}: {error}"));
            let projection = model
                .preflight(&bytes, PackageDecodeLimits::FROZEN.wire, &control)
                .unwrap();
            assert_eq!(projection.usage.input_bytes, bytes.len());
            let decoded =
                decode_fragment_package(&bytes, &model, &PackageDecodeLimits::FROZEN, &control)
                    .unwrap();
            assert_eq!(decoded.fragment().id(), id);
        }
    }

    #[test]
    fn four_column_values_at_original_scale_preserve_frozen_field_resource_boundary() {
        use crate::native::fragment_encoder::frozen::FragmentArtifact;
        use crate::query_execution::package_freeze::PackageFreezeError;
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use novarocks_plan_codec::physical_package_v2::{
            FROZEN_CONSTANT_POLICY, FROZEN_PACKAGE_ADMISSION, PackageDecodeLimits,
            PackageEncodeLimits, decode_fragment_package, encode_fragment_package,
        };
        use novarocks_plan_codec::resource_preflight_v2::{
            FragmentDecodeResourceModel, ResourceModelError,
        };
        use prost::Message;
        use std::error::Error;
        // The separate COUNT/LIKE regression above keeps the original query
        // shape. Here every output and every column varies, so ordinary column
        // pruning or constant lifting cannot turn the scale check into a
        // narrower VALUES package. Production accepts this legal package;
        // an explicit caller field budget must still preserve the original
        // resource failure through artifact freezing without creating one.
        let rows = (0..16_384)
            .map(|row| {
                format!("('catalog_{row:05}','schema_{row:05}','table_{row:05}','type_{row:05}')")
            })
            .collect::<Vec<_>>()
            .join(",");
        // VALUES exposes all four columns without adding projection roots to
        // the existing 65,536 expression-use boundary.
        let sql = format!("VALUES {rows}");
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([24; 16]).unwrap(),
            SqlStatementInput::sql(&sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".into()),
                current_database: "db".into(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            crate::query_execution::constant_eval::constant_evaluator(),
            FROZEN_CONSTANT_POLICY,
            control.clone(),
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
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (_root, scope) = query_scope();
        let completed = runtime
            .block_on(
                FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                    .complete_for_test(request, &scope),
            )
            .unwrap();
        assert_eq!(
            completed
                .candidate()
                .plan()
                .result_port()
                .unwrap()
                .fields
                .len(),
            4
        );
        let packages = crate::query_execution::package_freeze::extract_checked_packages(
            completed.candidate(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            FROZEN_CONSTANT_POLICY,
            &FROZEN_PACKAGE_ADMISSION,
            &control,
        )
        .unwrap();
        let model = Arc::new(FragmentDecodeResourceModel::try_new(&control).unwrap());
        let carrier = CompiledPackageCarrier::new(
            FROZEN_PACKAGE_ADMISSION,
            PackageEncodeLimits::FROZEN,
            Arc::clone(&model),
            PackageDecodeLimits::FROZEN.wire,
        );
        let mut values_nodes = 0;
        let mut expression_positions = 0;
        for package in packages.values() {
            for node in package.fragment().nodes().values() {
                let NodeKind::Values { rows } = &node.kind else {
                    continue;
                };
                values_nodes += 1;
                assert_eq!(rows.len(), 16_384);
                for row in rows {
                    assert_eq!(row.len(), 4);
                    expression_positions += row.len();
                }
            }
        }
        assert_eq!(values_nodes, 1);
        assert_eq!(expression_positions, 65_536);
        let mut limited_values_packages = 0;
        for (id, package) in packages {
            let contains_values = package
                .fragment()
                .nodes()
                .values()
                .any(|node| matches!(&node.kind, NodeKind::Values { .. }));
            if contains_values {
                limited_values_packages += 1;
                // Measure the complete legal producer bytes, then exercise an
                // explicit caller budget one field below their actual usage.
                let bytes =
                    encode_fragment_package(&package, &PackageEncodeLimits::FROZEN, &control)
                        .unwrap()
                        .encode_to_vec();
                let production = model
                    .preflight(&bytes, PackageDecodeLimits::FROZEN.wire, &control)
                    .expect("production accepts the complete legal four-column package");
                assert_eq!(production.usage.input_bytes, bytes.len());
                let mut receiver = PackageDecodeLimits::FROZEN.wire;
                receiver.max_field_occurrences =
                    production.usage.field_occurrences.checked_sub(1).unwrap();
                let limited_carrier = CompiledPackageCarrier::new(
                    FROZEN_PACKAGE_ADMISSION,
                    PackageEncodeLimits::FROZEN,
                    Arc::clone(&model),
                    receiver,
                );
                let original = model
                    .preflight(&bytes, receiver, &control)
                    .expect_err("the explicit caller field limit must reject the full package");
                assert_eq!(
                    original,
                    ResourceModelError::Limit("protobuf field occurrences exceed caller limit")
                );
                assert_eq!(original.failure_fact().class(), FailureClass::Resource);
                let before =
                    crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
                let error = match FragmentArtifact::freeze_package(
                    &package,
                    completed.candidate().plan().edges(),
                    &limited_carrier,
                    &control,
                ) {
                    Err(error) => error,
                    Ok(_) => panic!("an over-limit package must not produce its Native artifact"),
                };
                assert_eq!(error.failure_fact(), original.failure_fact());
                let PackageFreezeError::Receiver(actual) = &error else {
                    panic!("the original receiver resource author must survive artifact freezing");
                };
                assert_eq!(actual, &original);
                let source = error
                    .source()
                    .unwrap()
                    .downcast_ref::<ResourceModelError>()
                    .unwrap();
                assert!(std::ptr::eq(actual, source));
                assert_eq!(
                    crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread(),
                    before,
                    "the refused Values fragment creates no artifact"
                );
                // The caller-limited failure is separate from the normal
                // production round trip, which must also accept this package.
            }
            let bytes = crate::query_execution::package_freeze::encode_checked_package(
                &package, &carrier, &control,
            )
            .unwrap_or_else(|error| panic!("four-column original-scale fragment {id:?}: {error}"));
            let projection = model
                .preflight(&bytes, PackageDecodeLimits::FROZEN.wire, &control)
                .unwrap();
            assert_eq!(projection.usage.input_bytes, bytes.len());
            let decoded =
                decode_fragment_package(&bytes, &model, &PackageDecodeLimits::FROZEN, &control)
                    .unwrap();
            assert_eq!(decoded.fragment().id(), id);
        }
        assert_eq!(
            limited_values_packages, 1,
            "the caller-limited check uses the one actual full Values package"
        );
    }

    // Compile the original 1024-column statistics seed through the public
    // DML writer planner and production package ceilings. The marker handle
    // is a test-owned write declaration, not an Iceberg execution capability.
    #[test]
    fn wide_statistics_insert_plan_freezes_with_production_limits() {
        use arrow::datatypes::{DataType, Field};
        use novarocks_connector_contract::{ConnectorWriteBinding, ConnectorWriteRecipeDraft};
        use novarocks_plan_codec::physical_package_v2::{
            FROZEN_CONSTANT_POLICY, FROZEN_PACKAGE_ADMISSION, PackageDecodeLimits,
            PackageEncodeLimits, decode_fragment_package,
        };
        use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
        use novarocks_query_application::preparation::CompletedPhysicalPlanCandidate;
        use novarocks_spi::connector::{
            CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
            ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
            ConnectorInstanceId, ConnectorProviderId, ConnectorWriteFieldBinding,
            ConnectorWriteFieldToken, ConnectorWriteInputShape,
        };
        use novarocks_sql::binding::SqlTableBindingAllocator;
        use novarocks_sql::compiler::{
            RootDistributionRequirement, SqlAnalyzeRequest, SqlCompiler, SqlOptimizeRequest,
            SqlPlannerTableSnapshot,
        };
        use novarocks_sql::planning::catalog::PlannerMemoryCatalog;
        use novarocks_sql::planning::dml::{
            ConnectorWriteInputBinding, DmlFinalPlanContext, DmlFinalWritePlanContext,
            DmlFinalizedProviderReadSet, DmlFinalizedWriteTarget, DmlFinalizedWriteTargetSet,
            DmlStatisticsSnapshot, DmlWritePlanInput, DmlWriteSinkMode, DmlWriteTarget,
            DmlWriteTargetField, compile_final_connector_write_plan,
        };
        use novarocks_type_contract::FunctionValueType;
        use novarocks_types::schema::ColumnDef;

        let projection = (0..1024)
            .map(|index| format!("generate_series + {}", index * 1_000_000))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {projection} FROM TABLE(generate_series(1, 4))");
        let control = SqlCompileControl::unbounded();
        let settings = SessionOptimizerSettings::default();
        let catalog = PlannerMemoryCatalog::default();
        let snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let functions = novarocks_sql::compiler::build_builtin_engine_function_catalog()
            .expect("real builtin SQL catalogue");
        let semantics = novarocks_sql::sql_mode::SqlSemanticSettings::default();
        let decimal = semantics.sql_mode().decimal_overflow_policy();
        let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
            SqlStatementInput::sql(&sql),
            SqlCompileIntent::IcebergWrite {
                root_distribution: RootDistributionRequirement::Any,
            },
            SqlSessionContext {
                sql_semantics: semantics,
                current_catalog: Some("native_statistics".into()),
                current_database: "native_statistics_db".into(),
                optimizer_settings: settings.clone(),
            },
            SqlPlanningEnvironment::Distributed,
            &snapshot,
            &functions,
            crate::query_execution::constant_eval::constant_evaluator(),
            None,
            FROZEN_CONSTANT_POLICY,
            control.clone(),
        ))
        .unwrap_or_else(|error| panic!("wide INSERT source analysis: {error:#?}"))
        .into_pending()
        .expect("pending DML source");
        let columns = (0..1024)
            .map(|index| ColumnDef {
                name: format!("v{index}"),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            })
            .collect::<Vec<_>>();
        let token = |index: usize| {
            let mut bytes = [0u8; 32];
            bytes[..4].copy_from_slice(&u32::try_from(index + 1).unwrap().to_be_bytes());
            ConnectorWriteFieldToken::from_bytes(bytes)
        };
        let mut allocator = SqlTableBindingAllocator::new_unique().expect("binding allocator");
        let sink = DmlWritePlanInput::try_new(
            DmlWriteSinkMode::Data,
            DmlWriteTarget {
                binding: allocator.allocate().expect("admitted test target"),
                catalog: "native_statistics".into(),
                namespace: "native_statistics_db".into(),
                table: "native_statistics_wide_source".into(),
                fields: columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| DmlWriteTargetField {
                        token: token(index),
                        column: column.clone(),
                        is_hidden: false,
                    })
                    .collect(),
            },
            columns.clone(),
            ConnectorWriteInputBinding::RootOutputByOrdinal,
        )
        .expect("exact 1024-field write contract");
        let provider = ConnectorProviderId::parse("statistics_test").unwrap();
        let instance = ConnectorInstanceId::parse("native_statistics").unwrap();
        let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([21; 32]));
        let binding = ConnectorWriteBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        );
        let handle = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                provider,
                catalog,
                ConnectorCodecCategory::WriteHandle,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            bytes::Bytes::from_static(b"wide-statistics-owned-test-handle"),
        );
        let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
        let statistics = DmlStatisticsSnapshot::empty();
        // The original failing seed disables collect-on-write, so there are
        // no auxiliary statistics requirements, exactly as in this fixture.
        let owner = compile_final_connector_write_plan(
            SqlOptimizeRequest::new(analyzed, &statistics, control.clone()),
            sink,
            ordinal,
            &[],
            &settings,
            DmlFinalWritePlanContext::new(
                DmlFinalPlanContext::new(
                    PlanVersionId::try_new([21; 16]).unwrap(),
                    PipelineDopDomain {
                        min: 1,
                        max: 8,
                        requires_power_of_two: true,
                    },
                    DmlFinalizedProviderReadSet::empty(),
                ),
                DmlFinalizedWriteTargetSet::try_new([DmlFinalizedWriteTarget {
                    ordinal,
                    handle: handle.clone(),
                }])
                .unwrap(),
            ),
            decimal,
        )
        .unwrap_or_else(|error| panic!("wide INSERT writer planning: {error:#?}"));
        let widths = owner
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| fragment.nodes().values())
            .filter_map(|node| match &node.kind {
                NodeKind::TableWriter { target } => Some(target.target_fields.len()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            widths,
            vec![1024],
            "the real planner retains one wide writer"
        );
        let recipe = ConnectorWriteRecipeDraft::try_new(
            binding,
            handle,
            ConnectorWriteInputShape::Data {
                fields: columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        ConnectorWriteFieldBinding::try_new_typed(
                            token(index),
                            Field::new(&column.name, DataType::Int64, true),
                            FunctionValueType::new(DataType::Int64, true),
                        )
                        .expect("owned provider field declaration")
                    })
                    .collect(),
            },
        )
        .unwrap_or_else(|error| panic!("wide INSERT test writer recipe: {error:#?}"));
        let candidate = CompletedPhysicalPlanCandidate::for_sql_program(owner, &control)
            .and_then(|candidate| {
                candidate.freeze_root_output(
                    novarocks_result_contract::FrozenRootOutput::InternalFacts(
                        novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
                    ),
                )
            })
            .unwrap_or_else(|error| panic!("wide INSERT completed candidate: {error:#?}"));
        let packages = crate::query_execution::package_freeze::extract_checked_packages(
            &candidate,
            &BTreeMap::new(),
            &BTreeMap::from([(ordinal, recipe)]),
            FROZEN_CONSTANT_POLICY,
            &FROZEN_PACKAGE_ADMISSION,
            &control,
        )
        .unwrap_or_else(|error| panic!("wide INSERT checked package extraction: {error:#?}"));
        assert_eq!(packages.len(), candidate.plan().fragments().len());
        let model = FragmentDecodeResourceModel::try_new(&control).expect("decode model");
        for (id, package) in &packages {
            let bytes = crate::query_execution::package_freeze::encode_checked_package(
                package,
                &CompiledPackageCarrier::for_test(
                    FROZEN_PACKAGE_ADMISSION,
                    PackageEncodeLimits::FROZEN,
                ),
                &control,
            )
            .unwrap_or_else(|error| panic!("wide INSERT fragment {id:?} encode: {error:#?}"));
            let decoded =
                decode_fragment_package(&bytes, &model, &PackageDecodeLimits::FROZEN, &control)
                    .unwrap_or_else(|error| {
                        panic!("wide INSERT fragment {id:?} decode: {error:#?}")
                    });
            assert_eq!(decoded.fragment().id(), *id);
        }
    }

    // A VALUES cell keeps its analyzed type until final lowering assigns its
    // column's, so a NULL cell is a NULL of its column's type. Lowered in its
    // own type first and then replaced, it left a definition no operator
    // read, which final validation refused before package encoding.
    #[test]
    fn values_with_null_cells_complete_and_encode_as_compiled_packages() {
        for sql in [
            "SELECT a, b FROM (VALUES (1, NULL), (2, 3)) t(a, b) ORDER BY a",
            "SELECT a, b, c FROM (VALUES (1, 10, 'x'), (2, NULL, 'y'), (3, 20, NULL), \
             (NULL, 20, 'z'), (5, 20, 'w'), (6, NULL, NULL), (7, 30, 'v')) AS t(a, b, c)",
            "SELECT a, b, c, d FROM (VALUES (1, NULL, 'x', 1.5), (2, 3, NULL, NULL), \
             (NULL, 4, 'y', 2.5)) AS t(a, b, c, d)",
        ] {
            let (plan, encoded) = encode_with(sql, &compiled_carrier());
            encoded.unwrap_or_else(|error| panic!("{sql}: the compiled package encodes: {error}"));
            assert_eq!(
                compiled_programs(sql).len(),
                plan.fragments().len(),
                "{sql}"
            );
        }
    }

    #[test]
    fn completed_encoding_observes_tail_after_freezing_and_preserves_first_control_failure() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CompletedTailControl {
            encode_checkpoints: AtomicUsize,
            checkpoints: Mutex<Vec<(CompilePhase, u32)>>,
            /// The Encode checkpoint that refuses; `None` only observes.
            tail_at: Option<usize>,
            tail_error: CompileControlError,
            earlier_error: Option<CompileControlError>,
        }
        impl PureCompileControl for CompletedTailControl {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                self.checkpoints.lock().unwrap().push((phase, units));
                // The last Encode checkpoint is the FE exit, after the actual
                // native fragment freezes and attempt-fact projection.
                if phase == CompilePhase::Encode
                    && Some(self.encode_checkpoints.fetch_add(1, Ordering::SeqCst)) == self.tail_at
                {
                    return Err(self.tail_error.clone());
                }
                if phase == CompilePhase::Validate
                    && let Some(error) = self.earlier_error.clone()
                {
                    return Err(error);
                }
                Ok(())
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let (_root, scope) = query_scope();
            let completed = runtime
                .block_on(
                    FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                        .complete_for_test(request(), &scope),
                )
                .expect("the actual admitted VALUES plan completes");
            let fragments = completed.candidate().plan().fragments().len();
            // Observe the actual Encode checkpoint sequence once, so the
            // refusal lands on its true tail rather than a counted guess.
            let observed = CompletedTailControl {
                encode_checkpoints: AtomicUsize::new(0),
                checkpoints: Mutex::new(Vec::new()),
                tail_at: None,
                tail_error: error.clone(),
                earlier_error: None,
            };
            encode_completed_plan(
                completed,
                &compiled_carrier(),
                crate::application::test_constant_policy(),
                None,
                &observed,
            )
            .expect("the observed encoding succeeds");
            let encode_checkpoints = observed.encode_checkpoints.load(Ordering::SeqCst);
            assert!(encode_checkpoints >= 4, "FE and codec entry/exit at least");
            let observed_checkpoints = observed.checkpoints.into_inner().unwrap();
            assert_eq!(
                observed_checkpoints.first(),
                Some(&(CompilePhase::Encode, 0))
            );
            let first_validate = observed_checkpoints
                .iter()
                .position(|(phase, _)| *phase == CompilePhase::Validate)
                .expect("the actual package author validates its semantic facts");
            // Package read authoring has its own completed Encode scope before
            // semantic validation. Keep that legitimate prefix, rather than
            // assuming validation immediately follows the outer FE entry.
            let before_validate = &observed_checkpoints[..first_validate];
            assert!(
                before_validate
                    .iter()
                    .all(|(phase, _)| *phase == CompilePhase::Encode)
            );
            let completed = runtime
                .block_on(
                    FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                        .complete_for_test(request(), &scope),
                )
                .expect("the actual admitted VALUES plan completes");
            let freezes = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
            let control = CompletedTailControl {
                encode_checkpoints: AtomicUsize::new(0),
                checkpoints: Mutex::new(Vec::new()),
                tail_at: Some(encode_checkpoints - 1),
                tail_error: error.clone(),
                earlier_error: None,
            };
            assert!(matches!(
                encode_completed_plan(
                    completed,
                    &compiled_carrier(),
                    crate::application::test_constant_policy(),
                    None,
                    &control,
                ),
                Err(novarocks_plan_codec::PhysicalEncodeError::Control(actual)) if actual == error,
            ));
            assert_eq!(
                control.encode_checkpoints.load(Ordering::SeqCst),
                encode_checkpoints
            );
            assert_eq!(
                crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread() - freezes,
                fragments,
                "tail refusal must occur after actual native freezing, rather than at entry",
            );

            let completed = runtime
                .block_on(
                    FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                        .complete_for_test(request(), &scope),
                )
                .unwrap();
            let freezes_before_refusal =
                crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
            let first_failure = CompletedTailControl {
                encode_checkpoints: AtomicUsize::new(0),
                checkpoints: Mutex::new(Vec::new()),
                tail_at: Some(encode_checkpoints - 1),
                tail_error: if error == CompileControlError::ResourceExhausted {
                    CompileControlError::Cancelled
                } else {
                    CompileControlError::ResourceExhausted
                },
                earlier_error: Some(error.clone()),
            };
            assert!(matches!(
                encode_completed_plan(
                    completed,
                    &compiled_carrier(),
                    crate::application::test_constant_policy(),
                    None,
                    &first_failure,
                ),
                Err(novarocks_plan_codec::PhysicalEncodeError::Control(actual)) if actual == error,
            ));
            assert_eq!(
                first_failure.encode_checkpoints.load(Ordering::SeqCst),
                before_validate.len(),
                "the first Validate refusal preserves completed author scopes without an outer Encode footer",
            );
            assert_eq!(
                first_failure.checkpoints.lock().unwrap().as_slice(),
                &observed_checkpoints[..=first_validate],
                "the first Validate refusal stops at its exact checkpoint with no subsequent checkpoint",
            );
            assert_eq!(
                crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread(),
                freezes_before_refusal,
                "the first Validate refusal precedes any native fragment freezing",
            );
        }
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
            .block_on(
                FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                    .complete_for_test(request(), &scope),
            )
            .unwrap_or_else(|error| panic!("VALUES completes without facts: {error}"));
        let version = completed.candidate().plan().version();
        let encoded = encode_completed_plan(
            completed,
            &compiled_carrier(),
            crate::application::test_constant_policy(),
            None,
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .expect("a completed plan encodes");
        let template = encoded.into_attempt_template(version);

        let freezes = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
        let first = template.instantiate_static_fragment_fixture();
        let recovery = template.instantiate_static_fragment_fixture();
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
            .block_on(
                FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                    .complete_for_test(request(), &scope),
            )
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

    fn fixture_payload(
        category: novarocks_connector_contract::ConnectorCodecCategory,
        byte: u8,
    ) -> novarocks_connector_contract::ConnectorEncodedPayload {
        use novarocks_connector_contract::{
            CatalogHandle, CatalogVersion, ConnectorCodecRevision, ConnectorEnvelopeHeader,
            ConnectorInstanceId, ConnectorProviderId,
        };
        novarocks_connector_contract::ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse("iceberg").expect("provider id"),
                CatalogHandle::new(
                    ConnectorInstanceId::parse("fixture").expect("instance id"),
                    CatalogVersion::from_bytes([3; 32]),
                ),
                category,
                ConnectorCodecRevision::try_new(1).expect("codec revision"),
            ),
            vec![byte].into(),
        )
    }

    /// One validated single-scan program with no freeze behind it.
    fn scan_plan() -> PhysicalPlan {
        use novarocks_connector_contract::{
            CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorInstanceDescriptor,
            ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding,
            ConnectorReadRelationKind, ConnectorReadRelationPayload,
        };
        use novarocks_physical_plan::{
            ExactInputVersion, FragmentBuilder, MetadataRelation, MetadataRelationKind,
            PhysicalProperties, PlanBuilder, ProviderColumnReference, Relation, RelationField,
            RowMultiplicity, ValueOrigin, ValueType,
        };

        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
                instance_id: ConnectorInstanceId::parse("fixture").expect("instance id"),
            },
            CatalogHandle::new(
                ConnectorInstanceId::parse("fixture").expect("instance id"),
                CatalogVersion::from_bytes([3; 32]),
            ),
        );
        let field = ProviderColumnReference {
            column_payload: fixture_payload(ConnectorCodecCategory::ReadColumn, 33),
        };
        let relation = Relation::Metadata(MetadataRelation {
            kind: MetadataRelationKind::try_new("iceberg.manifest.entries").expect("kind"),
            read: novarocks_physical_plan::ProviderReadReference {
                binding,
                input_version: ExactInputVersion::try_new(vec![9]).expect("input version"),
                relation: ConnectorReadRelationPayload::new(
                    ConnectorReadRelationKind::SystemTable,
                    fixture_payload(ConnectorCodecCategory::ReadTable, 1),
                    fixture_payload(ConnectorCodecCategory::ReadView, 2),
                ),
            },
            selection_digest: [8; 32],
            schema: Box::from([RelationField {
                column: field.clone(),
                ty: ValueType::new(arrow::datatypes::DataType::Int64, false),
            }]),
            predicate_guarantees: Box::default(),
            provided_properties: PhysicalProperties {
                distribution: Distribution::Unconstrained,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            coverage_evidence: Box::from([4]),
        });
        let mut fragment = FragmentBuilder::new(FragmentId::new(1));
        let root = fragment.reserve_node_id().expect("scan node id");
        let value = fragment
            .add_value(
                ValueType::new(arrow::datatypes::DataType::Int64, false),
                ValueOrigin::ProviderField {
                    scan_node: root,
                    field: field.clone(),
                },
            )
            .expect("scan output");
        fragment
            .add_scan(
                root,
                NodeKind::Scan {
                    occurrence: ProviderReadOccurrenceId::new(0),
                    relation: Box::new(relation),
                    read_budget: ScanReadBudget {
                        max_batch_rows: 4096,
                        max_batch_bytes: 8 * 1024 * 1024,
                    },
                    provider_outputs: Box::from([(field, value)]),
                    residuals: Box::default(),
                    derived_values: Box::default(),
                },
                Box::from([value]),
            )
            .expect("scan node");
        let fragment = fragment
            .finish_definition(
                root,
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .expect("scan fragment");
        let mut builder = PlanBuilder::new(PlanVersionId::try_new([11; 16]).expect("version"));
        builder.add_fragment(fragment).expect("one fragment");
        builder.finish().expect("validated scan plan")
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

    #[test]
    fn m07_completed_result_occurrences_supply_exact_attempt_domains() {
        // Producer-domain propagation is independent of the unavailable to_bitmap
        // scalar. A source-free bitmap_agg supplies the same opaque Bitmap domain;
        // bitmap_to_string supplies a Physical domain without assuming its carrier.
        use novarocks_type_contract::ValueLogicalType as D;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (_root, scope) = query_scope();
        let completed = runtime.block_on(FinalPlanCompletionDriver::new(Arc::new(NoFacts)).complete_for_test(
            request_for("SELECT hll_hash('x') AS h,bitmap_agg(1) AS b,percentile_hash(1.0) AS p,parse_json('{}') AS j,bitmap_to_string(bitmap_agg(1)) AS raw"), &scope,
        )).unwrap();
        let plan = completed.candidate().plan();
        let result = plan.result_port().unwrap();
        let outputs = completed_fragment_output_columns(
            completed.candidate().original_public_result_port(),
            result.fragment,
        );
        assert_eq!(
            outputs
                .iter()
                .map(|column| column.logical_type())
                .collect::<Vec<_>>(),
            vec![D::Hll, D::Bitmap, D::Percentile, D::Json, D::Physical]
        );
        assert_eq!(
            outputs
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["h", "b", "p", "j", "raw"]
        );
        for (output, original) in outputs.iter().zip(result.fields.iter()) {
            assert_eq!(output.ty, original.ty);
        }
        assert_eq!(outputs[4].logical_type(), D::Physical);
    }

    fn request() -> SqlFinalPlanCompileRequest {
        request_with("SELECT 1", noop_constant_evaluator())
    }

    /// A request folding constants with the production Frontend evaluator,
    /// so the frozen packages are the ones production would freeze.
    fn request_for(sql: &str) -> SqlFinalPlanCompileRequest {
        request_with(
            sql,
            crate::query_execution::constant_eval::constant_evaluator(),
        )
    }

    fn request_with(
        sql: &str,
        constant_evaluator: &'static dyn novarocks_sql::compiler::SqlConstantEvaluator,
    ) -> SqlFinalPlanCompileRequest {
        SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([9; 16]).expect("plan version"),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            constant_evaluator,
            crate::application::test_constant_policy(),
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
