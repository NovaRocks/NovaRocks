//! Backend-local reduction state over strict Execution contributions.
//!
//! The state deliberately accepts no Arrow data, scan facts, reader handles,
//! or evaluator results.  It owns only participant stream/replay state and
//! produces immutable Backend publication candidates.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use novarocks_execution::runtime_filter::{
    LogicalVersion, ProducerSequence, RuntimeFilterContribution as EncodedContribution,
    RuntimeFilterExecutionContract, RuntimeFilterProducerKind,
    contribution::{
        OrderedTuple, RuntimeFilterContribution as TypedContribution, TopKSummary, ValueDomainDelta,
    },
};

use super::{
    BackendInstallPolicy, BackendInstallPolicyError, BackendProducerStreamIdentity,
    BackendReducedLogicalSnapshot, MembershipReducer, ReducerError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendReductionApply {
    Applied { version: LogicalVersion },
    Duplicate,
    Stale,
    SequenceAdvancedEqual,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendReductionStateError {
    Install(BackendInstallPolicyError),
    ReplayConflict,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    SequenceAfterClose,
    Reducer(ReducerError),
    OrderedContract,
    VersionOverflow,
}

#[derive(Clone, Debug, Default)]
struct StreamReplay {
    highest: Option<ProducerSequence>,
    digest: Option<[u8; 32]>,
}

impl StreamReplay {
    fn admit(
        &mut self,
        sequence: ProducerSequence,
        digest: [u8; 32],
    ) -> Result<Option<BackendReductionApply>, BackendReductionStateError> {
        let Some(highest) = self.highest else {
            self.highest = Some(sequence);
            self.digest = Some(digest);
            return Ok(None);
        };
        if sequence < highest {
            return Ok(Some(BackendReductionApply::Stale));
        }
        if sequence == highest {
            return if self.digest == Some(digest) {
                Ok(Some(BackendReductionApply::Duplicate))
            } else {
                Err(BackendReductionStateError::ReplayConflict)
            };
        }
        self.highest = Some(sequence);
        self.digest = Some(digest);
        Ok(None)
    }
}

/// One Backend channel's mutable participant reduction.  Contribution bodies
/// are copied only from Execution typed values; canonical frame validation
/// happened in [`BackendInstallPolicy`].
pub struct BackendReductionState {
    policy: BackendInstallPolicy,
    streams: BTreeMap<BackendProducerStreamIdentity, StreamReplay>,
    membership: Option<MembershipReducer>,
    ordered: Option<OrderedTuple>,
    topk: BTreeMap<BackendProducerStreamIdentity, TopKSummary>,
    version: Option<LogicalVersion>,
}

impl BackendReductionState {
    pub fn new(policy: BackendInstallPolicy) -> Result<Self, BackendReductionStateError> {
        let membership = match policy.producer().contract() {
            RuntimeFilterExecutionContract::Membership(schema) => Some(
                MembershipReducer::try_new(schema.data_type().clone(), schema.null_semantics())
                    .map_err(BackendReductionStateError::Reducer)?,
            ),
            RuntimeFilterExecutionContract::Ordered(_) => None,
        };
        Ok(Self {
            policy,
            streams: BTreeMap::new(),
            membership,
            ordered: None,
            topk: BTreeMap::new(),
            version: None,
        })
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn policy(&self) -> &BackendInstallPolicy {
        &self.policy
    }

    pub fn latest_snapshot(&self) -> Option<BackendReducedLogicalSnapshot> {
        let version = self.version?;
        match self.policy.producer().contract() {
            RuntimeFilterExecutionContract::Membership(_) => {
                Some(BackendReducedLogicalSnapshot::membership(
                    self.policy.channel().channel_id(),
                    version,
                    self.membership
                        .as_ref()
                        .expect("membership contract owns reducer")
                        .domain()
                        .clone(),
                ))
            }
            RuntimeFilterExecutionContract::Ordered(_) => {
                Some(BackendReducedLogicalSnapshot::ordered_bound(
                    self.policy.channel().channel_id(),
                    version,
                    self.ordered.clone()?,
                ))
            }
        }
    }

    /// The initial reducer domain is known only after the authorized demand's
    /// entire producer coverage closes successfully.
    pub(super) fn completed_membership_snapshot(
        &self,
        empty_content_demand: bool,
    ) -> Option<BackendReducedLogicalSnapshot> {
        self.latest_snapshot().or_else(|| {
            if !empty_content_demand {
                return None;
            }
            Some(BackendReducedLogicalSnapshot::membership(
                self.policy.channel().channel_id(),
                LogicalVersion::FIRST,
                self.membership.as_ref()?.domain().clone(),
            ))
        })
    }

    pub fn submit(
        &mut self,
        stream: BackendProducerStreamIdentity,
        sequence: ProducerSequence,
        encoded: EncodedContribution,
    ) -> Result<
        (BackendReductionApply, Option<BackendReducedLogicalSnapshot>),
        BackendReductionStateError,
    > {
        let decoded = self
            .policy
            .decode_contribution(&encoded)
            .map_err(BackendReductionStateError::Install)?;
        let digest = contribution_digest(&decoded);
        let mut next_replay = self.streams.get(&stream).cloned().unwrap_or_default();
        if let Some(outcome) = next_replay.admit(sequence, digest)? {
            return Ok((outcome, None));
        }
        let changed = match (&decoded, self.policy.producer().kind()) {
            (TypedContribution::Membership(_), RuntimeFilterProducerKind::Membership)
            | (TypedContribution::FinalDomain(_), RuntimeFilterProducerKind::FinalDomain) => {
                let delta: &ValueDomainDelta = match &decoded {
                    TypedContribution::Membership(delta) => delta,
                    TypedContribution::FinalDomain(shard) => shard.domain(),
                    _ => unreachable!(),
                };
                let reducer = self
                    .membership
                    .as_mut()
                    .expect("membership contract owns reducer");
                let projection = reducer
                    .preflight(delta)
                    .map_err(BackendReductionStateError::Reducer)?;
                if projection.retained_growth() == 0 {
                    false
                } else {
                    reducer
                        .commit_preflighted(delta)
                        .map_err(BackendReductionStateError::Reducer)?;
                    true
                }
            }
            (TypedContribution::OrderedBound(update), RuntimeFilterProducerKind::OrderedBound) => {
                self.tighten(update.bound())?
            }
            (TypedContribution::TopKSummary(summary), RuntimeFilterProducerKind::TopKSummary) => {
                self.replace_topk(stream, summary)?
            }
            _ => return Err(BackendReductionStateError::OrderedContract),
        };
        if !changed {
            self.streams.insert(stream, next_replay);
            return Ok((BackendReductionApply::SequenceAdvancedEqual, None));
        }
        let version = self.version.map_or(Ok(LogicalVersion::FIRST), |v| {
            v.checked_next()
                .ok_or(BackendReductionStateError::VersionOverflow)
        })?;
        self.version = Some(version);
        self.streams.insert(stream, next_replay);
        let snapshot = self
            .latest_snapshot()
            .expect("changed reduction owns a current logical snapshot");
        Ok((BackendReductionApply::Applied { version }, Some(snapshot)))
    }

    fn tighten(&mut self, incoming: &OrderedTuple) -> Result<bool, BackendReductionStateError> {
        let RuntimeFilterExecutionContract::Ordered(contract) = self.policy.producer().contract()
        else {
            return Err(BackendReductionStateError::OrderedContract);
        };
        contract
            .validate_tuple(incoming)
            .map_err(|_| BackendReductionStateError::OrderedContract)?;
        let changed = self.ordered.as_ref().is_none_or(|current| {
            contract
                .compare(incoming, current)
                .is_ok_and(|order| order == Ordering::Less)
        });
        if changed {
            self.ordered = Some(incoming.clone());
        }
        Ok(changed)
    }

    fn replace_topk(
        &mut self,
        stream: BackendProducerStreamIdentity,
        incoming: &TopKSummary,
    ) -> Result<bool, BackendReductionStateError> {
        let RuntimeFilterExecutionContract::Ordered(order) = self.policy.producer().contract()
        else {
            return Err(BackendReductionStateError::OrderedContract);
        };
        let k = match self.policy.producer().reduction() {
            novarocks_execution::runtime_filter::RuntimeFilterReduction::MergeTopKSummary {
                k,
                ..
            } => k,
            _ => return Err(BackendReductionStateError::OrderedContract),
        };
        if incoming.contract_digest() != order.digest() || incoming.candidates().len() > k as usize
        {
            return Err(BackendReductionStateError::OrderedContract);
        }
        if let Some(previous) = self.topk.get(&stream) {
            validate_topk_transition(order, k, previous.candidates(), incoming.candidates())?;
        }
        // Validate and derive the aggregate from a prospective map first. A
        // rejected contribution must not consume either its stream sequence or
        // the stream's last accepted summary.
        let mut next_topk = self.topk.clone();
        next_topk.insert(stream, incoming.clone());
        let mut candidates = next_topk
            .values()
            .flat_map(|summary| summary.candidates().iter().cloned())
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| order.compare(left, right).unwrap_or(Ordering::Equal));
        let rank = usize::try_from(k)
            .map_err(|_| BackendReductionStateError::OrderedContract)?
            .saturating_sub(1);
        let changed = match candidates.get(rank) {
            Some(bound) => self.tighten(bound)?,
            None => false,
        };
        self.topk = next_topk;
        Ok(changed)
    }
}

fn validate_topk_transition(
    order: &novarocks_execution::runtime_filter::contribution::RuntimeOrderContract,
    k: u32,
    previous: &[OrderedTuple],
    next: &[OrderedTuple],
) -> Result<(), BackendReductionStateError> {
    if next.len() < previous.len() {
        return Err(BackendReductionStateError::OrderedContract);
    }
    if next.len() < k as usize {
        let mut next_index = 0;
        for previous_candidate in previous {
            loop {
                let Some(next_candidate) = next.get(next_index) else {
                    return Err(BackendReductionStateError::OrderedContract);
                };
                match order
                    .compare(next_candidate, previous_candidate)
                    .map_err(|_| BackendReductionStateError::OrderedContract)?
                {
                    Ordering::Less => next_index += 1,
                    Ordering::Equal => {
                        next_index += 1;
                        break;
                    }
                    Ordering::Greater => return Err(BackendReductionStateError::OrderedContract),
                }
            }
        }
    } else if next.iter().zip(previous).any(|(next, previous)| {
        matches!(
            order.compare(next, previous),
            Ok(Ordering::Greater) | Err(_)
        )
    }) {
        return Err(BackendReductionStateError::OrderedContract);
    }
    Ok(())
}

fn contribution_digest(contribution: &TypedContribution) -> [u8; 32] {
    match contribution {
        TypedContribution::Membership(delta) => delta.fingerprint(),
        TypedContribution::OrderedBound(update) => update.replay_digest(),
        TypedContribution::TopKSummary(summary) => summary.replay_digest(),
        TypedContribution::FinalDomain(shard) => shard.replay_digest(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::{
        LogicalVersion, PartitionId, ProducerSequence, RuntimeFilterBindingId,
        RuntimeFilterChannelId, RuntimeFilterContribution, RuntimeFilterContributionKind,
        RuntimeFilterExecutionContract, RuntimeFilterProducerContract,
        contribution::{
            ContributionCodecExpectation, OrderedScalar, OrderedTuple,
            RuntimeFilterContribution as TypedContribution, RuntimeOrderContract, RuntimeOrderKey,
            RuntimeTopKSummaryContract, TopKSummary, encode_contribution,
        },
    };
    use novarocks_types::UniqueId;

    use super::*;
    use crate::runtime_filter::domain::{
        BackendCoverage, BackendCoverageWitnessId, BackendParticipantIdentity,
        BackendReducedLogicalDomain,
    };

    const CONTRIBUTION_BUDGET: usize = 4096;

    fn topk_policy(k: u32) -> (BackendInstallPolicy, Arc<RuntimeOrderContract>) {
        let order = Arc::new(RuntimeOrderContract::new(
            [RuntimeOrderKey::new(DataType::Int64)],
            [0xA1; 32],
        ));
        let producer = RuntimeFilterProducerContract::top_k_summary(
            RuntimeFilterBindingId::new(7),
            RuntimeFilterChannelId::new(11),
            k,
            RuntimeFilterExecutionContract::Ordered(Arc::clone(&order).into()),
        )
        .expect("TopK producer contract is valid");
        let policy = BackendInstallPolicy::new(
            BackendParticipantIdentity::new(UniqueId::new(17, 19), 23),
            producer,
            BackendCoverage::witness(BackendCoverageWitnessId::new(29)),
            CONTRIBUTION_BUDGET,
        )
        .expect("Backend install policy is valid");
        (policy, order)
    }

    fn stream(policy: &BackendInstallPolicy, fragment: i64) -> BackendProducerStreamIdentity {
        BackendProducerStreamIdentity::new(
            policy.channel(),
            UniqueId::new(fragment, fragment + 1),
            PartitionId::new(0),
        )
    }

    fn tuple(value: i64) -> OrderedTuple {
        OrderedTuple::new([Some(OrderedScalar::Int64(value))])
    }

    fn contribution(
        order: &RuntimeOrderContract,
        contract_k: u32,
        values: impl IntoIterator<Item = i64>,
    ) -> RuntimeFilterContribution {
        let contract = RuntimeTopKSummaryContract::new(order.clone(), contract_k, order.digest());
        let summary = TopKSummary::try_new(&contract, values.into_iter().map(tuple))
            .expect("test contribution is canonical");
        let encoded = encode_contribution(
            &TypedContribution::top_k_summary(summary),
            ContributionCodecExpectation::TopKSummary(&contract),
            CONTRIBUTION_BUDGET,
        )
        .expect("test contribution fits its budget");
        RuntimeFilterContribution::new(
            RuntimeFilterContributionKind::TopKSummary,
            *encoded.schema_digest(),
            encoded.into_parts().1,
        )
    }

    fn ordered_bound(snapshot: BackendReducedLogicalSnapshot) -> i64 {
        match snapshot.domain() {
            BackendReducedLogicalDomain::OrderedBound(bound) => match bound.values() {
                [Some(OrderedScalar::Int64(value))] => *value,
                actual => panic!("expected one Int64 ordered bound, got {actual:?}"),
            },
            actual => panic!("expected ordered bound snapshot, got {actual:?}"),
        }
    }

    #[test]
    fn topk_reduction_waits_for_k_then_tightens_the_global_bound() {
        let (policy, order) = topk_policy(2);
        let first = stream(&policy, 31);
        let second = stream(&policy, 41);
        let mut state = BackendReductionState::new(policy).expect("valid TopK state");

        let (outcome, snapshot) = state
            .submit(
                first,
                ProducerSequence::new(0),
                contribution(&order, 2, [3]),
            )
            .expect("first shard is accepted");
        assert_eq!(outcome, BackendReductionApply::SequenceAdvancedEqual);
        assert!(
            snapshot.is_none(),
            "fewer than K candidates must not publish"
        );

        let (outcome, snapshot) = state
            .submit(
                second,
                ProducerSequence::new(0),
                contribution(&order, 2, [7]),
            )
            .expect("second shard reaches K");
        assert_eq!(
            outcome,
            BackendReductionApply::Applied {
                version: LogicalVersion::FIRST
            }
        );
        assert_eq!(ordered_bound(snapshot.expect("K candidates publish")), 7);

        let (outcome, snapshot) = state
            .submit(
                first,
                ProducerSequence::new(1),
                contribution(&order, 2, [2, 3]),
            )
            .expect("monotonic shard replacement is accepted");
        assert_eq!(
            outcome,
            BackendReductionApply::Applied {
                version: LogicalVersion::new(2)
            }
        );
        assert_eq!(
            ordered_bound(snapshot.expect("a tighter global bound publishes")),
            3
        );
    }

    #[test]
    fn topk_reduction_tracks_duplicate_stale_and_equal_replay_without_publication() {
        let (policy, order) = topk_policy(2);
        let stream = stream(&policy, 31);
        let initial = contribution(&order, 2, [3, 7]);
        let mut state = BackendReductionState::new(policy).expect("valid TopK state");

        let (outcome, snapshot) = state
            .submit(stream, ProducerSequence::new(0), initial.clone())
            .expect("initial summary is accepted");
        assert_eq!(
            outcome,
            BackendReductionApply::Applied {
                version: LogicalVersion::FIRST
            }
        );
        assert_eq!(ordered_bound(snapshot.expect("initial bound publishes")), 7);

        assert_eq!(
            state
                .submit(stream, ProducerSequence::new(0), initial.clone())
                .expect("identical replay is accepted"),
            (BackendReductionApply::Duplicate, None)
        );
        assert_eq!(
            state
                .submit(stream, ProducerSequence::new(1), initial)
                .expect("new sequence with equal bound is accepted"),
            (BackendReductionApply::SequenceAdvancedEqual, None)
        );
        assert_eq!(
            state
                .submit(
                    stream,
                    ProducerSequence::new(0),
                    contribution(&order, 2, [2, 3])
                )
                .expect("older sequence is ignored"),
            (BackendReductionApply::Stale, None)
        );
        assert_eq!(
            ordered_bound(
                state
                    .latest_snapshot()
                    .expect("published snapshot is retained")
            ),
            7
        );
    }

    #[test]
    fn topk_reduction_rejects_invalid_contributions_without_consuming_stream_state() {
        let (policy, order) = topk_policy(2);
        let stream = stream(&policy, 31);
        let mut state = BackendReductionState::new(policy.clone()).expect("valid TopK state");

        let overfull = contribution(&order, 3, [1, 2, 3]);
        assert!(matches!(
            state.submit(stream, ProducerSequence::new(0), overfull),
            Err(BackendReductionStateError::Install(
                BackendInstallPolicyError::ContributionDecodeFailed(_)
            ))
        ));

        let first = contribution(&order, 2, [2]);
        assert_eq!(
            state
                .submit(stream, ProducerSequence::new(0), first.clone())
                .expect("rejected contribution did not consume sequence"),
            (BackendReductionApply::SequenceAdvancedEqual, None)
        );

        let digest_mismatch = RuntimeFilterContribution::from_shared(
            RuntimeFilterContributionKind::TopKSummary,
            [0xF1; 32],
            first.canonical_bytes().clone(),
        );
        assert!(matches!(
            state.submit(stream, ProducerSequence::new(1), digest_mismatch),
            Err(BackendReductionStateError::Install(
                BackendInstallPolicyError::ContributionDigestMismatch
            ))
        ));

        let replay_conflict = contribution(&order, 2, [1]);
        assert_eq!(
            state.submit(stream, ProducerSequence::new(0), replay_conflict),
            Err(BackendReductionStateError::ReplayConflict)
        );
        let (outcome, snapshot) = state
            .submit(
                stream,
                ProducerSequence::new(1),
                contribution(&order, 2, [1, 2]),
            )
            .expect("valid next sequence remains admissible");
        assert_eq!(
            outcome,
            BackendReductionApply::Applied {
                version: LogicalVersion::FIRST
            }
        );
        assert_eq!(ordered_bound(snapshot.expect("valid summary publishes")), 2);

        assert_eq!(
            state.submit(
                stream,
                ProducerSequence::new(2),
                contribution(&order, 2, [2, 3])
            ),
            Err(BackendReductionStateError::OrderedContract)
        );
        assert_eq!(
            state
                .submit(
                    stream,
                    ProducerSequence::new(2),
                    contribution(&order, 2, [1, 2])
                )
                .expect("rejected non-monotonic replacement did not consume sequence"),
            (BackendReductionApply::SequenceAdvancedEqual, None)
        );
    }
}
