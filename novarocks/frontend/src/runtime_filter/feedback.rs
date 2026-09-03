//! FE attempt-local admission state for terminal runtime-filter feedback.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Condvar, Mutex};

use novarocks_execution::runtime_filter::contribution::MembershipValues;
use novarocks_execution::runtime_filter::feedback_domain::RuntimeFilterFeedbackDomain;
use novarocks_proto_codec::lifecycle::{
    ParticipantAttemptRef, QueryControlEvent, QueryExecutionId,
};
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorReadColumnHandle, ConnectorReadDynamicFilterSnapshot, ConnectorValue, Domain,
    Range, TupleDomain, ValueSet,
};
use novarocks_types::{BackendProcessId, QueryId};

use super::install_encoder::{
    FrontendRuntimeFilterFeedbackDeclaration, FrontendRuntimeFilterFeedbackPublisherSlot,
    FrontendRuntimeFilterFeedbackWaitEligibility,
};

#[derive(Default)]
struct FeedbackState {
    closed: bool,
    generation: u64,
    channels: BTreeMap<u32, ChannelState>,
}

struct ChannelState {
    contract_digest: [u8; 32],
    max_encoded_domain_bytes: usize,
    data_type: arrow::datatypes::DataType,
    publishers: BTreeMap<u32, FrontendRuntimeFilterFeedbackPublisherSlot>,
    scan_bindings: BTreeSet<(i32, u32)>,
    wait_eligible: bool,
    /// Which authorized publishers reported that they will never produce a
    /// usable domain for this channel.
    ///
    /// The set, not the reasons. Only its size is ever read -- a channel is
    /// terminal once every any-of publisher has closed -- and the two carriers
    /// cannot say the same thing about *why*: the task carrier's envelope has
    /// one unavailable kind where the control stream had four reasons. Keeping
    /// a reason here would mean one carrier storing a value it invented, so
    /// the reason stays where it is produced, in the backend's own log.
    unavailable: BTreeSet<u32>,
    winner: Option<Vec<u8>>,
}

/// A query-owned feedback admission gate. It intentionally retains no
/// StateStore reference or process-global registration.
pub(crate) struct RuntimeFilterFeedbackState {
    execution_id: QueryExecutionId,
    state: (Mutex<FeedbackState>, Condvar),
}

/// Admission dispositions that preserve the distinction between an accepted
/// feedback update and an untrusted nonterminal carrier that was fenced before
/// it could alter any query-local state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterFeedbackAdmission {
    Applied,
    IgnoredRetiredAttempt,
    RejectedForeignParticipant,
}

impl RuntimeFilterFeedbackState {
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        declaration: FrontendRuntimeFilterFeedbackDeclaration,
    ) -> Result<Self, String> {
        Ok(Self {
            execution_id,
            state: (
                Mutex::new(FeedbackState {
                    closed: false,
                    generation: 0,
                    channels: channel_states(declaration)?,
                }),
                Condvar::new(),
            ),
        })
    }

    /// The declaration is installed before the attempt's readers and split
    /// pump start.  Keeping the state object stable lets both owners share
    /// one attempt-local admission domain without any process-global lookup.
    pub(crate) fn configure(
        &self,
        declaration: FrontendRuntimeFilterFeedbackDeclaration,
    ) -> Result<(), String> {
        let mut state = self.state.0.lock().expect("runtime filter feedback state");
        if state.closed {
            return Err("cannot configure closed runtime filter feedback state".into());
        }
        state.channels = channel_states(declaration)?;
        state.generation = state.generation.saturating_add(1);
        self.state.1.notify_all();
        Ok(())
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.0.lock().expect("runtime filter feedback state");
        state.closed = true;
        state.generation = state.generation.saturating_add(1);
        self.state.1.notify_all();
    }

    /// Block the split-assignment worker until feedback changes or the small
    /// scheduler-owned budget elapses. The caller always rechecks its stop
    /// handle and its source deadline, so feedback is never a cancellation
    /// authority and this method never sleeps a connector implementation.
    pub(crate) fn wait_for_change(&self, observed_generation: u64, budget: std::time::Duration) {
        let state = self.state.0.lock().expect("runtime filter feedback state");
        if state.closed || state.generation != observed_generation || budget.is_zero() {
            return;
        }
        let _ = self
            .state
            .1
            .wait_timeout(state, budget)
            .expect("runtime filter feedback condvar");
    }

    pub(crate) fn generation(&self) -> u64 {
        self.state
            .0
            .lock()
            .expect("runtime filter feedback state")
            .generation
    }

    /// Snapshot the currently admitted domains for one connector scan.  A
    /// channel that is still pending (or that reported unavailable) widens to
    /// `all`; therefore this method is an optimization boundary and can never
    /// make a source omit a file merely because feedback was delayed.
    pub(crate) fn snapshot_for_scan(
        &self,
        plan_node_id: i32,
        bindings: &[(u32, ConnectorReadColumnHandle)],
    ) -> ConnectorReadDynamicFilterSnapshot {
        let state = self.state.0.lock().expect("runtime filter feedback state");
        if state.closed {
            return ConnectorReadDynamicFilterSnapshot::all_complete();
        }

        let mut domains: BTreeMap<ConnectorReadColumnHandle, Domain> = BTreeMap::new();
        let mut complete = true;
        for (binding_id, column) in bindings {
            let Some(channel) = state
                .channels
                .values()
                .find(|channel| channel.scan_bindings.contains(&(plan_node_id, *binding_id)))
            else {
                continue;
            };
            complete &= channel.is_terminal();
            let Some(encoded) = channel.winner.as_deref() else {
                continue;
            };
            let Ok(domain) = RuntimeFilterFeedbackDomain::decode(
                encoded,
                &channel.data_type,
                channel.max_encoded_domain_bytes,
            ) else {
                // Admission already rejects malformed feedback.  Retaining a
                // fail-open guard here keeps a future codec extension from
                // turning a split-planning optimization into a correctness
                // risk.
                continue;
            };
            let Some(domain) = connector_domain(&domain) else {
                continue;
            };
            match domains.remove(column) {
                Some(existing) => match existing.intersect(&domain) {
                    Ok(intersection) => {
                        domains.insert(column.clone(), intersection);
                    }
                    Err(_) => {
                        // A type mismatch cannot be a pruning authority.
                    }
                },
                None => {
                    domains.insert(column.clone(), domain);
                }
            }
        }
        let predicate = TupleDomain::with_column_domains(domains)
            .expect("feedback declarations bind at most the connector tuple-domain limit");
        ConnectorReadDynamicFilterSnapshot::new(predicate, complete)
    }

    /// Whether an initial source wait can still improve pruning. One usable
    /// exact/range domain is enough to start enumeration immediately, even if
    /// another channel remains pending; a terminal unavailable/`All` channel
    /// never becomes a pruning authority.
    pub(crate) fn is_initial_wait_blocked(
        &self,
        plan_node_id: i32,
        bindings: impl IntoIterator<Item = u32>,
    ) -> bool {
        let state = self.state.0.lock().expect("runtime filter feedback state");
        if state.closed {
            return false;
        }
        let bindings = bindings.into_iter().collect::<BTreeSet<_>>();
        let relevant = state.channels.values().filter(|channel| {
            channel.wait_eligible
                && bindings
                    .iter()
                    .any(|binding_id| channel.scan_bindings.contains(&(plan_node_id, *binding_id)))
        });
        let mut pending = false;
        for channel in relevant {
            pending |= !channel.is_terminal();
            let Some(encoded) = channel.winner.as_deref() else {
                continue;
            };
            if matches!(
                RuntimeFilterFeedbackDomain::decode(
                    encoded,
                    &channel.data_type,
                    channel.max_encoded_domain_bytes,
                ),
                Ok(RuntimeFilterFeedbackDomain::Exact(_)
                    | RuntimeFilterFeedbackDomain::EnclosingRange { .. })
            ) {
                return false;
            }
        }
        pending
    }

    /// Validates and admits one active-stream event. A retired attempt is
    /// ignored and a foreign participant is rejected before it can mutate
    /// query-local state. The control-stream owner decides how to observe
    /// that nonterminal rejection without treating it as a terminal outcome.
    pub(crate) fn admit(
        &self,
        event: &QueryControlEvent,
        participant: &ParticipantAttemptRef,
    ) -> Result<RuntimeFilterFeedbackAdmission, String> {
        use novarocks_proto_models::novarocks::query_control_response::Event;
        use novarocks_proto_models::novarocks::runtime_filter_feedback_event::TerminalOutcome;

        let Some(Event::RuntimeFilterFeedback(feedback)) = event.as_proto().event.as_ref() else {
            return Err("runtime filter feedback admission received a non-feedback event".into());
        };
        let feedback =
            novarocks_proto_codec::lifecycle::RuntimeFilterFeedbackEvent::parse(feedback.clone())
                .map_err(|error| error.to_string())?;
        let feedback_participant = feedback.participant().map_err(|error| error.to_string())?;
        if feedback_participant
            .execution_id()
            .map_err(|error| error.to_string())?
            != self.execution_id
        {
            return Ok(RuntimeFilterFeedbackAdmission::IgnoredRetiredAttempt);
        }
        if feedback_participant != *participant {
            return Ok(RuntimeFilterFeedbackAdmission::RejectedForeignParticipant);
        }
        let participant_process = participant
            .backend_process_id()
            .map_err(|error| error.to_string())?;
        let outcome = match feedback.as_proto().terminal_outcome.as_ref() {
            Some(TerminalOutcome::CanonicalDomain(encoded)) => {
                TerminalFeedback::CanonicalDomain(encoded.as_slice())
            }
            // The reason is dropped here rather than stored: nothing reads it,
            // and the task carrier cannot produce one. It is logged so the fact
            // is not lost from an operator's view.
            Some(TerminalOutcome::UnavailableReason(reason)) => {
                tracing::debug!(
                    channel_id = feedback.as_proto().channel_id,
                    participant_id = feedback.as_proto().participant_id,
                    reason,
                    "runtime filter feedback reports an unavailable channel"
                );
                TerminalFeedback::Unavailable
            }
            None => return Err("runtime filter feedback terminal outcome is absent".into()),
        };
        self.admit_terminal(
            &TerminalFeedbackFacts {
                channel_id: feedback.as_proto().channel_id,
                deployment_epoch: feedback.as_proto().deployment_epoch,
                contract_digest: feedback.as_proto().contract_digest.as_slice(),
                publisher: PublisherIdentity::Declared {
                    participant_id: feedback.as_proto().participant_id,
                },
                publisher_process: participant_process,
            },
            outcome,
        )
    }

    /// Admits one publisher's terminal feedback as the task carrier delivers it.
    ///
    /// It presents the same authorization facts as the control-stream carrier,
    /// with one difference that is a property of the carrier rather than a
    /// relaxation: the task path is keyed by the backend process that actually
    /// ran the producing task. A `TaskIdentity` names that process, so the
    /// declared publisher slot is found *by* it instead of being named on the
    /// wire and then checked against it. There is no way to claim another
    /// process's slot, because the claim never travels.
    pub(crate) fn admit_task_feedback(
        &self,
        feedback: &TaskRuntimeFilterFeedback,
        publisher_process: BackendProcessId,
    ) -> Result<RuntimeFilterFeedbackAdmission, String> {
        if feedback.query_id != self.execution_id.query_id() {
            return Ok(RuntimeFilterFeedbackAdmission::IgnoredRetiredAttempt);
        }
        self.admit_terminal(
            &TerminalFeedbackFacts {
                channel_id: feedback.channel_id,
                deployment_epoch: feedback.deployment_epoch,
                contract_digest: &feedback.contract_digest,
                publisher: PublisherIdentity::ByProcess,
                publisher_process,
            },
            match &feedback.outcome {
                TaskFeedbackOutcome::CanonicalDomain(encoded) => {
                    TerminalFeedback::CanonicalDomain(encoded.as_slice())
                }
                TaskFeedbackOutcome::Unavailable => TerminalFeedback::Unavailable,
            },
        )
    }

    /// The one admission core both carriers reach.
    fn admit_terminal(
        &self,
        facts: &TerminalFeedbackFacts<'_>,
        outcome: TerminalFeedback<'_>,
    ) -> Result<RuntimeFilterFeedbackAdmission, String> {
        if facts.deployment_epoch != self.execution_id.attempt_id().get() {
            return Err(
                "runtime filter feedback deployment epoch differs from active attempt".into(),
            );
        }
        let mut state = self.state.0.lock().expect("runtime filter feedback state");
        if state.closed {
            return Ok(RuntimeFilterFeedbackAdmission::Applied);
        }
        let channel = state
            .channels
            .get_mut(&facts.channel_id)
            .ok_or("runtime filter feedback channel is not declared for this attempt")?;
        if facts.contract_digest != channel.contract_digest {
            return Err("runtime filter feedback contract digest differs from declaration".into());
        }
        let participant_id = match facts.publisher {
            PublisherIdentity::Declared { participant_id } => {
                let slot = channel
                    .publishers
                    .get(&participant_id)
                    .ok_or("runtime filter feedback publisher is not authorized")?;
                if slot.backend_process_id != facts.publisher_process {
                    return Err(
                        "runtime filter feedback publisher process differs from declaration".into(),
                    );
                }
                participant_id
            }
            PublisherIdentity::ByProcess => {
                channel
                    .publishers
                    .values()
                    .find(|slot| slot.backend_process_id == facts.publisher_process)
                    .ok_or("runtime filter feedback publisher is not authorized")?
                    .participant_id
            }
        };
        match outcome {
            TerminalFeedback::CanonicalDomain(encoded) => {
                RuntimeFilterFeedbackDomain::decode(
                    encoded,
                    &channel.data_type,
                    channel.max_encoded_domain_bytes,
                )
                .map_err(|error| error.to_string())?;
                match &channel.winner {
                    Some(existing) if existing.as_slice() == encoded => {
                        Ok(RuntimeFilterFeedbackAdmission::Applied)
                    }
                    Some(_) => Err(
                        "runtime filter feedback terminal domain conflicts with first winner"
                            .into(),
                    ),
                    None => {
                        channel.winner = Some(encoded.to_vec());
                        state.generation = state.generation.saturating_add(1);
                        self.state.1.notify_all();
                        Ok(RuntimeFilterFeedbackAdmission::Applied)
                    }
                }
            }
            TerminalFeedback::Unavailable => {
                if channel.winner.is_none() {
                    channel.unavailable.insert(participant_id);
                    state.generation = state.generation.saturating_add(1);
                    self.state.1.notify_all();
                }
                Ok(RuntimeFilterFeedbackAdmission::Applied)
            }
        }
    }
}

/// How one carrier names the publisher of a terminal feedback.
#[derive(Copy, Clone, Debug)]
enum PublisherIdentity {
    /// The carrier names a participant slot, which is then checked against the
    /// process the carrier authenticated.
    Declared { participant_id: u32 },
    /// The carrier names no slot; the authenticated process selects its own.
    ByProcess,
}

/// The fences one terminal feedback must clear, independent of its carrier.
struct TerminalFeedbackFacts<'a> {
    channel_id: u32,
    deployment_epoch: u64,
    contract_digest: &'a [u8],
    publisher: PublisherIdentity,
    publisher_process: BackendProcessId,
}

/// What one publisher says is final for one channel.
enum TerminalFeedback<'a> {
    CanonicalDomain(&'a [u8]),
    Unavailable,
}

/// What one publisher reported for one channel, as the task carrier delivers it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TaskFeedbackOutcome {
    CanonicalDomain(Vec<u8>),
    /// This publisher will never produce a usable domain for this channel.
    Unavailable,
}

/// One channel's terminal feedback, recovered from a task's dynamic filter
/// domain.
///
/// This is the read side of what a backend advertises through its task status
/// and retains for a `FetchTaskDynamicFilters`. It is parsed here, beside the
/// admission that consumes it, so the wire shape has exactly one reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TaskRuntimeFilterFeedback {
    query_id: QueryId,
    channel_id: u32,
    deployment_epoch: u64,
    contract_digest: [u8; 32],
    outcome: TaskFeedbackOutcome,
}

impl TaskRuntimeFilterFeedback {
    /// Recovers one channel's feedback from the envelope a task advertised.
    ///
    /// Every field is required. A kind this carrier does not use, a digest of
    /// the wrong width, or a canonical domain with no payload is refused rather
    /// than read as an empty or default statement: an unusable envelope is a
    /// disagreement about the carrier, not a channel that reported nothing.
    pub(crate) fn parse(
        envelope: &novarocks_proto_models::filter::RuntimeFilterEnvelope,
    ) -> Result<Self, String> {
        use novarocks_proto_models::filter::RuntimeFilterEnvelopeKind;

        let kind = RuntimeFilterEnvelopeKind::try_from(envelope.kind).map_err(|_| {
            format!(
                "task runtime filter feedback kind {} is unknown",
                envelope.kind
            )
        })?;
        let outcome = match kind {
            RuntimeFilterEnvelopeKind::DegradedLogical => {
                if envelope.payload.is_empty() {
                    return Err(
                        "task runtime filter feedback claims a canonical domain but carries no \
                         payload"
                            .into(),
                    );
                }
                TaskFeedbackOutcome::CanonicalDomain(envelope.payload.clone())
            }
            RuntimeFilterEnvelopeKind::Unavailable => TaskFeedbackOutcome::Unavailable,
            other => {
                return Err(format!(
                    "task runtime filter feedback carries envelope kind {other:?}, which is not a \
                     terminal logical outcome"
                ));
            }
        };
        let query_id = envelope
            .query_id
            .as_ref()
            .map(|id| QueryId::new(id.hi, id.lo))
            .ok_or("task runtime filter feedback carries no query id")?;
        let contract_digest: [u8; 32] = envelope
            .schema_digest
            .as_slice()
            .try_into()
            .map_err(|_| "task runtime filter feedback contract digest is not 32 bytes")?;
        Ok(Self {
            query_id,
            channel_id: envelope.channel_id,
            deployment_epoch: envelope.deployment_epoch,
            contract_digest,
            outcome,
        })
    }

    pub(crate) const fn channel_id(&self) -> u32 {
        self.channel_id
    }
}

fn channel_states(
    declaration: FrontendRuntimeFilterFeedbackDeclaration,
) -> Result<BTreeMap<u32, ChannelState>, String> {
    let mut channels = BTreeMap::new();
    for channel in declaration.channels() {
        let Some(binding) = channel.scan_bindings().first() else {
            return Err("runtime filter feedback declaration has no scan binding".into());
        };
        if channel
            .scan_bindings()
            .iter()
            .any(|candidate| candidate.data_type != binding.data_type)
        {
            return Err(
                "runtime filter feedback channel binds incompatible scan value types".into(),
            );
        }
        channels.insert(
            channel.channel_id(),
            ChannelState {
                contract_digest: channel.contract_digest(),
                max_encoded_domain_bytes: usize::try_from(channel.max_encoded_domain_bytes())
                    .map_err(|_| "runtime filter feedback domain budget exceeds usize")?,
                data_type: binding.data_type.clone(),
                publishers: channel
                    .publishers()
                    .iter()
                    .copied()
                    .map(|slot| (slot.participant_id, slot))
                    .collect(),
                scan_bindings: channel
                    .scan_bindings()
                    .iter()
                    .map(|binding| (binding.plan_node_id, binding.binding_id))
                    .collect(),
                wait_eligible: matches!(
                    channel.wait_eligibility(),
                    FrontendRuntimeFilterFeedbackWaitEligibility::Eligible
                ),
                unavailable: BTreeSet::new(),
                winner: None,
            },
        );
    }
    Ok(channels)
}

impl ChannelState {
    fn is_terminal(&self) -> bool {
        self.winner.is_some() || self.unavailable.len() == self.publishers.len()
    }
}

/// Convert only exact, lossless representations into the Trino-style SPI
/// domain.  Any unsupported value, range, or resource shape becomes `None`,
/// which the caller treats as unconstrained.
fn connector_domain(feedback: &RuntimeFilterFeedbackDomain) -> Option<Domain> {
    match feedback {
        RuntimeFilterFeedbackDomain::All => None,
        RuntimeFilterFeedbackDomain::Exact(values) => {
            let values = connector_values(values)?;
            let value_type = values.first()?.value_type();
            let values = ValueSet::of_values(value_type, values).ok()?;
            Some(Domain::new(values, values_contains_null(feedback)))
        }
        RuntimeFilterFeedbackDomain::EnclosingRange {
            lower,
            upper,
            contains_null,
        } => {
            let mut lower = connector_values(lower)?;
            let mut upper = connector_values(upper)?;
            let [lower] = lower.as_mut_slice() else {
                return None;
            };
            let [upper] = upper.as_mut_slice() else {
                return None;
            };
            if lower.value_type() != upper.value_type() {
                return None;
            }
            let value_type = lower.value_type();
            let range = Range::try_new(
                value_type,
                Bound::Inclusive(lower.clone()),
                Bound::Inclusive(upper.clone()),
            )
            .ok()?;
            let values = ValueSet::of_ranges(value_type, vec![range]).ok()?;
            Some(Domain::new(values, *contains_null))
        }
    }
}

fn values_contains_null(feedback: &RuntimeFilterFeedbackDomain) -> bool {
    match feedback {
        RuntimeFilterFeedbackDomain::Exact(values) => values.contains_null(),
        RuntimeFilterFeedbackDomain::EnclosingRange { contains_null, .. } => *contains_null,
        RuntimeFilterFeedbackDomain::All => true,
    }
}

fn connector_values(
    values: &novarocks_execution::runtime_filter::contribution::ValueDomainDelta,
) -> Option<Vec<ConnectorValue>> {
    let values = match values.values() {
        MembershipValues::Boolean(values) => values
            .iter()
            .copied()
            .map(ConnectorValue::Boolean)
            .collect(),
        MembershipValues::Int8(values) => values
            .iter()
            .copied()
            .map(ConnectorValue::TinyInt)
            .collect(),
        MembershipValues::Int32(values) => values
            .iter()
            .copied()
            .map(ConnectorValue::Integer)
            .collect(),
        MembershipValues::Int64(values) => {
            values.iter().copied().map(ConnectorValue::BigInt).collect()
        }
        MembershipValues::Float32(values) => values
            .iter()
            .map(|value| f32::from_bits(value.bits()))
            .map(ConnectorValue::Real)
            .collect(),
        MembershipValues::Float64(values) => values
            .iter()
            .map(|value| f64::from_bits(value.bits()))
            .map(ConnectorValue::Double)
            .collect(),
        MembershipValues::Utf8(values) => values
            .iter()
            .cloned()
            .map(Into::into)
            .map(ConnectorValue::Varchar)
            .collect(),
        MembershipValues::Date32(values) => {
            values.iter().copied().map(ConnectorValue::Date).collect()
        }
        MembershipValues::Timestamp {
            unit,
            timezone,
            values,
        } => match (unit, timezone.as_deref()) {
            (arrow::datatypes::TimeUnit::Microsecond, None) => values
                .iter()
                .copied()
                .map(ConnectorValue::TimestampMicros)
                .collect(),
            (arrow::datatypes::TimeUnit::Nanosecond, None) => values
                .iter()
                .copied()
                .map(ConnectorValue::TimestampNanos)
                .collect(),
            (arrow::datatypes::TimeUnit::Microsecond, Some(zone))
                if zone.eq_ignore_ascii_case("UTC") =>
            {
                values
                    .iter()
                    .copied()
                    .map(ConnectorValue::TimestampTzMicros)
                    .collect()
            }
            (arrow::datatypes::TimeUnit::Nanosecond, Some(zone))
                if zone.eq_ignore_ascii_case("UTC") =>
            {
                values
                    .iter()
                    .copied()
                    .map(ConnectorValue::TimestampTzNanos)
                    .collect()
            }
            _ => return None,
        },
        MembershipValues::Decimal128 {
            precision,
            scale,
            values,
        } => values
            .iter()
            .map(|value| ConnectorValue::try_decimal(*value, *precision, *scale).ok())
            .collect::<Option<Vec<_>>>()?,
        MembershipValues::LargeInt(values) => values
            .iter()
            .map(|value| ConnectorValue::Fixed(value.to_be_bytes().into()))
            .collect(),
        // The declaration may support these FE values, but the connector SPI
        // has no equal-width predicate representation for them.
        MembershipValues::Int16(_) => return None,
    };
    if values.iter().any(|value| {
        matches!(value, ConnectorValue::Real(value) if value.is_nan())
            || matches!(value, ConnectorValue::Double(value) if value.is_nan())
    }) {
        return None;
    }
    Some(values)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::contribution::ValueDomainDelta;
    use novarocks_proto_codec::lifecycle::AttemptId;
    use novarocks_proto_models::novarocks;
    use novarocks_types::{BackendProcessId, QueryId};

    use super::super::install_encoder::{
        FrontendRuntimeFilterFeedbackChannel, FrontendRuntimeFilterFeedbackPublisherOwner,
        FrontendRuntimeFilterFeedbackScanBinding,
    };
    use super::*;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(11, 12),
            AttemptId::new(3).expect("valid attempt"),
        )
        .expect("valid execution id")
    }

    fn declaration(process: BackendProcessId) -> FrontendRuntimeFilterFeedbackDeclaration {
        FrontendRuntimeFilterFeedbackDeclaration::new([FrontendRuntimeFilterFeedbackChannel {
            channel_id: 7,
            contract_digest: [9; 32],
            max_encoded_domain_bytes: 64 * 1024,
            publishers: vec![FrontendRuntimeFilterFeedbackPublisherSlot {
                participant_id: 5,
                backend_process_id: process,
                owner: FrontendRuntimeFilterFeedbackPublisherOwner::Aggregator,
            }],
            scan_bindings: vec![FrontendRuntimeFilterFeedbackScanBinding {
                fragment_id: 2,
                plan_node_id: 17,
                binding_id: 7,
                data_type: DataType::Int64,
                nullable: false,
            }],
            wait_eligibility: FrontendRuntimeFilterFeedbackWaitEligibility::Eligible,
        }])
        .expect("valid declaration")
    }

    fn participant(
        execution_id: QueryExecutionId,
        process: BackendProcessId,
    ) -> ParticipantAttemptRef {
        ParticipantAttemptRef::new(execution_id, process).expect("valid participant attempt")
    }

    fn event(
        execution_id: QueryExecutionId,
        process: BackendProcessId,
        encoded: Vec<u8>,
    ) -> QueryControlEvent {
        QueryControlEvent::parse(novarocks::QueryControlResponse {
            event: Some(
                novarocks::query_control_response::Event::RuntimeFilterFeedback(
                    novarocks::RuntimeFilterFeedbackEvent {
                        participant_attempt: Some(participant(execution_id, process).as_proto().clone()),
                        participant_id: 5,
                        deployment_epoch: execution_id.attempt_id().get(),
                        channel_id: 7,
                        contract_digest: vec![9; 32],
                        terminal_outcome: Some(
                            novarocks::runtime_filter_feedback_event::TerminalOutcome::CanonicalDomain(
                                encoded,
                            ),
                        ),
                    },
                ),
            ),
        })
        .expect("valid event")
    }

    fn unavailable_event(
        execution_id: QueryExecutionId,
        process: BackendProcessId,
        participant_id: u32,
    ) -> QueryControlEvent {
        QueryControlEvent::parse(novarocks::QueryControlResponse {
            event: Some(
                novarocks::query_control_response::Event::RuntimeFilterFeedback(
                    novarocks::RuntimeFilterFeedbackEvent {
                        participant_attempt: Some(participant(execution_id, process).as_proto().clone()),
                        participant_id,
                        deployment_epoch: execution_id.attempt_id().get(),
                        channel_id: 7,
                        contract_digest: vec![9; 32],
                        terminal_outcome: Some(
                            novarocks::runtime_filter_feedback_event::TerminalOutcome::UnavailableReason(
                                novarocks::RuntimeFilterFeedbackUnavailableReason::DomainBudget
                                    as i32,
                            ),
                        ),
                    },
                ),
            ),
        })
        .expect("valid unavailable event")
    }

    fn exact(value: i64) -> Vec<u8> {
        RuntimeFilterFeedbackDomain::Exact(ValueDomainDelta::new(
            MembershipValues::int64([value]),
            false,
        ))
        .encode(64 * 1024)
        .expect("canonical domain")
    }

    #[test]
    fn admits_only_the_first_authorized_terminal_domain_for_the_active_attempt() {
        let execution_id = execution_id();
        let process = BackendProcessId::new_v7();
        let state = RuntimeFilterFeedbackState::new(execution_id, declaration(process))
            .expect("feedback state");
        let first = exact(41);

        state
            .admit(
                &event(execution_id, process, first.clone()),
                &participant(execution_id, process),
            )
            .expect("first terminal domain is admitted");
        state
            .admit(
                &event(execution_id, process, first.clone()),
                &participant(execution_id, process),
            )
            .expect("identical duplicate is idempotent");
        let conflict = state
            .admit(
                &event(execution_id, process, exact(42)),
                &participant(execution_id, process),
            )
            .expect_err("a distinct terminal domain cannot replace the winner");
        assert!(conflict.contains("conflicts with first winner"));

        let state = state.state.0.lock().expect("feedback state");
        assert_eq!(state.channels[&7].winner.as_deref(), Some(first.as_slice()));
        assert!(state.channels[&7].is_terminal());
    }

    #[test]
    fn ignores_a_retired_attempt_before_authorizing_any_slot() {
        let execution_id = execution_id();
        let process = BackendProcessId::new_v7();
        let state = RuntimeFilterFeedbackState::new(execution_id, declaration(process))
            .expect("feedback state");
        let retired = QueryExecutionId::new(
            QueryId::new(11, 12),
            AttemptId::new(2).expect("valid attempt"),
        )
        .expect("valid execution id");

        state
            .admit(
                &event(retired, process, exact(41)),
                &participant(execution_id, process),
            )
            .expect("retired event is ignored");
        let state = state.state.0.lock().expect("feedback state");
        assert!(state.channels[&7].winner.is_none());
    }

    #[test]
    fn rejects_a_foreign_participant_for_the_active_attempt() {
        let execution_id = execution_id();
        let process = BackendProcessId::new_v7();
        let state = RuntimeFilterFeedbackState::new(execution_id, declaration(process))
            .expect("feedback state");

        let outcome = state
            .admit(
                &event(execution_id, BackendProcessId::new_v7(), exact(41)),
                &participant(execution_id, process),
            )
            .expect("active-attempt feedback is fenced without mutating query state");
        assert_eq!(
            outcome,
            RuntimeFilterFeedbackAdmission::RejectedForeignParticipant
        );
        assert!(
            state.state.0.lock().expect("feedback state").channels[&7]
                .winner
                .is_none()
        );
    }

    #[test]
    fn unavailable_is_fail_open_only_after_all_anyof_publishers_close() {
        let execution_id = execution_id();
        let first = BackendProcessId::new_v7();
        let second = BackendProcessId::new_v7();
        let declaration =
            FrontendRuntimeFilterFeedbackDeclaration::new([FrontendRuntimeFilterFeedbackChannel {
                channel_id: 7,
                contract_digest: [9; 32],
                max_encoded_domain_bytes: 64 * 1024,
                publishers: vec![
                    FrontendRuntimeFilterFeedbackPublisherSlot {
                        participant_id: 5,
                        backend_process_id: first,
                        owner: FrontendRuntimeFilterFeedbackPublisherOwner::DirectSource,
                    },
                    FrontendRuntimeFilterFeedbackPublisherSlot {
                        participant_id: 6,
                        backend_process_id: second,
                        owner: FrontendRuntimeFilterFeedbackPublisherOwner::DirectSource,
                    },
                ],
                scan_bindings: vec![FrontendRuntimeFilterFeedbackScanBinding {
                    fragment_id: 2,
                    plan_node_id: 17,
                    binding_id: 7,
                    data_type: DataType::Int64,
                    nullable: false,
                }],
                wait_eligibility: FrontendRuntimeFilterFeedbackWaitEligibility::Eligible,
            }])
            .expect("valid declaration");
        let state = RuntimeFilterFeedbackState::new(execution_id, declaration).expect("state");

        state
            .admit(
                &unavailable_event(execution_id, first, 5),
                &participant(execution_id, first),
            )
            .expect("first unavailable is admitted");
        assert!(!state.state.0.lock().expect("state").channels[&7].is_terminal());
        state
            .admit(
                &unavailable_event(execution_id, second, 6),
                &participant(execution_id, second),
            )
            .expect("second unavailable is admitted");
        let state = state.state.0.lock().expect("state");
        assert!(state.channels[&7].winner.is_none());
        assert!(state.channels[&7].is_terminal());
    }

    #[test]
    fn close_suppresses_late_active_feedback_without_reopening_the_channel() {
        let execution_id = execution_id();
        let process = BackendProcessId::new_v7();
        let state = RuntimeFilterFeedbackState::new(execution_id, declaration(process))
            .expect("feedback state");
        state.close();
        state
            .admit(
                &event(execution_id, process, exact(41)),
                &participant(execution_id, process),
            )
            .expect("closed state drops late feedback");
        let state = state.state.0.lock().expect("feedback state");
        assert!(state.closed);
        assert!(state.channels[&7].winner.is_none());
    }

    fn task_envelope(
        execution_id: QueryExecutionId,
        channel_id: u32,
        digest: [u8; 32],
        payload: Vec<u8>,
    ) -> novarocks_proto_models::filter::RuntimeFilterEnvelope {
        use novarocks_proto_models::filter;

        filter::RuntimeFilterEnvelope {
            kind: filter::RuntimeFilterEnvelopeKind::DegradedLogical as i32,
            query_id: Some(novarocks_proto_models::common::UniqueId {
                hi: execution_id.query_id().high(),
                lo: execution_id.query_id().low(),
            }),
            channel_id,
            deployment_epoch: execution_id.attempt_id().get(),
            route_identity: None,
            schema_digest: digest.to_vec(),
            payload,
            producer_open: None,
        }
    }

    #[test]
    fn the_task_carrier_is_authorized_by_the_process_that_ran_the_producing_task() {
        // The task carrier names no participant slot on the wire, so the slot
        // is found *by* the backend process the task identity already fences.
        // That is the same authorization fact the control stream presented; it
        // just cannot be claimed, because the claim never travels. A process
        // that declares no publisher for the channel is refused.
        let execution_id = execution_id();
        let process = BackendProcessId::new_v7();
        let state = RuntimeFilterFeedbackState::new(execution_id, declaration(process))
            .expect("feedback state");
        let domain = exact(41);
        let feedback = TaskRuntimeFilterFeedback::parse(&task_envelope(
            execution_id,
            7,
            [9; 32],
            domain.clone(),
        ))
        .expect("a legal envelope");

        let stranger = state
            .admit_task_feedback(&feedback, BackendProcessId::new_v7())
            .expect_err("an undeclared process is not a publisher of this channel");
        assert!(
            stranger.contains("publisher is not authorized"),
            "{stranger}"
        );
        assert!(
            state.state.0.lock().expect("state").channels[&7]
                .winner
                .is_none()
        );

        assert_eq!(
            state
                .admit_task_feedback(&feedback, process)
                .expect("the declared publisher is admitted"),
            RuntimeFilterFeedbackAdmission::Applied
        );
        assert_eq!(
            state.state.0.lock().expect("state").channels[&7]
                .winner
                .as_deref(),
            Some(domain.as_slice())
        );

        // The same fences the control-stream carrier applies still apply: a
        // digest that is not the declared contract's, and an attempt that is
        // not the active one.
        let wrong_digest =
            TaskRuntimeFilterFeedback::parse(&task_envelope(execution_id, 7, [1; 32], exact(41)))
                .expect("a legal envelope");
        let refused = state
            .admit_task_feedback(&wrong_digest, process)
            .expect_err("a foreign contract digest is refused");
        assert!(refused.contains("contract digest differs"), "{refused}");

        let retired = QueryExecutionId::new(
            QueryId::new(99, 99),
            AttemptId::new(3).expect("valid attempt"),
        )
        .expect("valid execution id");
        let foreign_query =
            TaskRuntimeFilterFeedback::parse(&task_envelope(retired, 7, [9; 32], exact(41)))
                .expect("a legal envelope");
        assert_eq!(
            state
                .admit_task_feedback(&foreign_query, process)
                .expect("a retired attempt is ignored, not an error"),
            RuntimeFilterFeedbackAdmission::IgnoredRetiredAttempt
        );
    }

    #[test]
    fn an_unavailable_task_envelope_closes_the_channel_without_a_domain() {
        use novarocks_proto_models::filter;

        // The carrier has one unavailable kind where the control stream had
        // four reasons. Nothing reads the reason, so the fact that survives is
        // the one that matters: this publisher will never produce a usable
        // domain, which makes an any-of channel terminal.
        let execution_id = execution_id();
        let process = BackendProcessId::new_v7();
        let state = RuntimeFilterFeedbackState::new(execution_id, declaration(process))
            .expect("feedback state");
        let mut envelope = task_envelope(execution_id, 7, [9; 32], Vec::new());
        envelope.kind = filter::RuntimeFilterEnvelopeKind::Unavailable as i32;
        let feedback = TaskRuntimeFilterFeedback::parse(&envelope).expect("a legal envelope");

        assert!(!state.state.0.lock().expect("state").channels[&7].is_terminal());
        state
            .admit_task_feedback(&feedback, process)
            .expect("an unavailable channel is admitted");
        let locked = state.state.0.lock().expect("state");
        assert!(locked.channels[&7].winner.is_none());
        assert!(locked.channels[&7].is_terminal());
    }

    #[test]
    fn a_task_envelope_this_carrier_cannot_mean_is_refused_rather_than_read_as_empty() {
        use novarocks_proto_models::filter;

        // Every field is required. An artifact envelope, a canonical domain
        // with no payload, or a digest of the wrong width is a disagreement
        // about the carrier, not a channel that reported nothing.
        let execution_id = execution_id();
        let mut artifact = task_envelope(execution_id, 7, [9; 32], vec![1]);
        artifact.kind = filter::RuntimeFilterEnvelopeKind::Artifact as i32;
        assert!(TaskRuntimeFilterFeedback::parse(&artifact).is_err());

        let empty_domain = task_envelope(execution_id, 7, [9; 32], Vec::new());
        let error = TaskRuntimeFilterFeedback::parse(&empty_domain)
            .expect_err("a canonical domain with no payload says nothing");
        assert!(error.contains("carries no payload"), "{error}");

        let mut short_digest = task_envelope(execution_id, 7, [9; 32], vec![1]);
        short_digest.schema_digest = vec![9; 16];
        assert!(TaskRuntimeFilterFeedback::parse(&short_digest).is_err());

        let mut no_query = task_envelope(execution_id, 7, [9; 32], vec![1]);
        no_query.query_id = None;
        assert!(TaskRuntimeFilterFeedback::parse(&no_query).is_err());
    }

    #[test]
    fn initial_wait_stops_at_the_first_usable_domain() {
        let execution_id = execution_id();
        let first = BackendProcessId::new_v7();
        let second = BackendProcessId::new_v7();
        let declaration = FrontendRuntimeFilterFeedbackDeclaration::new([
            FrontendRuntimeFilterFeedbackChannel {
                channel_id: 7,
                contract_digest: [9; 32],
                max_encoded_domain_bytes: 64 * 1024,
                publishers: vec![FrontendRuntimeFilterFeedbackPublisherSlot {
                    participant_id: 5,
                    backend_process_id: first,
                    owner: FrontendRuntimeFilterFeedbackPublisherOwner::Aggregator,
                }],
                scan_bindings: vec![FrontendRuntimeFilterFeedbackScanBinding {
                    fragment_id: 2,
                    plan_node_id: 17,
                    binding_id: 7,
                    data_type: DataType::Int64,
                    nullable: false,
                }],
                wait_eligibility: FrontendRuntimeFilterFeedbackWaitEligibility::Eligible,
            },
            FrontendRuntimeFilterFeedbackChannel {
                channel_id: 8,
                contract_digest: [8; 32],
                max_encoded_domain_bytes: 64 * 1024,
                publishers: vec![FrontendRuntimeFilterFeedbackPublisherSlot {
                    participant_id: 6,
                    backend_process_id: second,
                    owner: FrontendRuntimeFilterFeedbackPublisherOwner::Aggregator,
                }],
                scan_bindings: vec![FrontendRuntimeFilterFeedbackScanBinding {
                    fragment_id: 2,
                    plan_node_id: 17,
                    binding_id: 8,
                    data_type: DataType::Int64,
                    nullable: false,
                }],
                wait_eligibility: FrontendRuntimeFilterFeedbackWaitEligibility::Eligible,
            },
        ])
        .expect("valid declaration");
        let state = RuntimeFilterFeedbackState::new(execution_id, declaration).expect("state");
        assert!(state.is_initial_wait_blocked(17, [7, 8]));
        state
            .admit(
                &event(execution_id, first, exact(41)),
                &participant(execution_id, first),
            )
            .expect("usable domain");
        assert!(
            !state.is_initial_wait_blocked(17, [7, 8]),
            "a later pending channel must not delay the first usable domain"
        );
    }
}
