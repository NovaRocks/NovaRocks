//! Backend-local runtime-filter fixtures.
//!
//! These fixtures intentionally build only Execution contracts and canonical
//! contributions. They neither call a Frontend compiler nor borrow Core's
//! runtime-filter test support.

use arrow::datatypes::DataType;
use novarocks_execution::runtime_filter::{
    RuntimeFilterBindingId, RuntimeFilterChannelId, RuntimeFilterContribution,
    RuntimeFilterContributionKind, RuntimeFilterExecutionContract, RuntimeFilterMembershipSchema,
    RuntimeFilterNullSemantics, RuntimeFilterProducerContract, contribution,
};
use novarocks_types::UniqueId;

use super::domain::{BackendCoverage, BackendCoverageWitnessId, BackendParticipantIdentity};

pub(crate) struct BackendRuntimeFilterFixture {
    identity: BackendParticipantIdentity,
    producer_contract: RuntimeFilterProducerContract,
    coverage: BackendCoverage,
    membership_contribution: RuntimeFilterContribution,
}

impl BackendRuntimeFilterFixture {
    pub(crate) fn membership() -> Self {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .expect("Int64 membership schema is supported");
        let contract = RuntimeFilterExecutionContract::Membership(schema.clone());
        let producer_contract = RuntimeFilterProducerContract::membership(
            RuntimeFilterBindingId::new(7),
            RuntimeFilterChannelId::new(11),
            contract,
        )
        .expect("membership producer contract is valid");
        let domain = contribution::ValueDomainDelta::new(
            contribution::MembershipValues::int64([3, 9]),
            false,
        );
        let encoded = contribution::encode_contribution(
            &contribution::RuntimeFilterContribution::membership(domain),
            contribution::ContributionCodecExpectation::membership(
                schema.data_type(),
                schema.digest(),
            ),
            1024,
        )
        .expect("fixture contribution fits its budget");
        Self {
            identity: BackendParticipantIdentity::new(UniqueId::new(17, 19), 23),
            producer_contract,
            coverage: BackendCoverage::witness(BackendCoverageWitnessId::new(29)),
            membership_contribution: RuntimeFilterContribution::new(
                RuntimeFilterContributionKind::Membership,
                *encoded.schema_digest(),
                encoded.into_parts().1,
            ),
        }
    }

    pub(crate) const fn identity(&self) -> BackendParticipantIdentity {
        self.identity
    }

    pub(crate) fn producer_contract(&self) -> RuntimeFilterProducerContract {
        self.producer_contract.clone()
    }

    pub(crate) fn coverage(&self) -> BackendCoverage {
        self.coverage.clone()
    }

    pub(crate) fn membership_contribution(&self) -> RuntimeFilterContribution {
        self.membership_contribution.clone()
    }

    pub(crate) fn membership_contribution_with_values(
        &self,
        values: impl IntoIterator<Item = i64>,
    ) -> RuntimeFilterContribution {
        let RuntimeFilterExecutionContract::Membership(schema) = self.producer_contract.contract()
        else {
            panic!("membership fixture must use a membership contract")
        };
        let encoded = contribution::encode_contribution(
            &contribution::RuntimeFilterContribution::membership(
                contribution::ValueDomainDelta::new(
                    contribution::MembershipValues::int64(values),
                    false,
                ),
            ),
            contribution::ContributionCodecExpectation::membership(
                schema.data_type(),
                schema.digest(),
            ),
            1024,
        )
        .expect("fixture contribution fits its budget");
        RuntimeFilterContribution::new(
            RuntimeFilterContributionKind::Membership,
            *encoded.schema_digest(),
            encoded.into_parts().1,
        )
    }

    pub(crate) fn contribution_with_digest(&self, digest: [u8; 32]) -> RuntimeFilterContribution {
        RuntimeFilterContribution::new(
            RuntimeFilterContributionKind::Membership,
            digest,
            self.membership_contribution.canonical_bytes().clone(),
        )
    }
}

/// One installed, channel-less participant on a well-known attempt.
///
/// Channel-less is deliberate: it is the cheapest install that is still a real
/// participant, so a test about *reaching* a participant does not have to
/// build a route graph first. Any envelope it receives is refused by its own
/// route authority, which is exactly what distinguishes "the participant
/// decided" from "nobody was asked".
pub(crate) fn participant_for_test() -> std::sync::Arc<super::participant::RuntimeFilterParticipant>
{
    use super::participant::{
        BackendRuntimeFilterParticipantFactory, RuntimeFilterParticipantFactory,
    };

    let contribution = novarocks_proto_codec::lifecycle::RuntimeFilterContribution::parse(
        novarocks_proto_models::novarocks::RuntimeFilterContribution {
            participant_id: 1,
            lifecycle: Some(
                novarocks_proto_models::filter::RuntimeFilterQueryLifecycleOptions {
                    delivery_expire_ms: 1,
                    query_expire_ms: 1,
                    transport_retry_interval_ms: 1,
                    transport_max_attempts: 1,
                    transport_deadline_ms: 1,
                    transport_max_pending_entries: 1,
                    transport_max_pending_bytes: 1,
                },
            ),
            install: Some(
                novarocks_proto_models::filter::RuntimeFilterParticipantInstall::default(),
            ),
        },
    )
    .expect("a channel-less contribution is legal");
    let decoded = super::install_decode::decode_runtime_filter_contribution(
        participant_execution_id(),
        &contribution,
    )
    .expect("a channel-less contribution decodes");
    BackendRuntimeFilterParticipantFactory::new(crate::rpc::runtime::test_backend_data_runtime())
        .install(participant_execution_id(), decoded)
        .expect("a channel-less participant installs")
}

/// The attempt `participant_for_test` installs on.
pub(crate) fn participant_execution_id() -> novarocks_proto_codec::lifecycle::QueryExecutionId {
    use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId};

    QueryExecutionId::new(
        QueryId::new(0x51, 0x53),
        AttemptId::new(59).expect("nonzero attempt"),
    )
    .expect("valid execution id")
}

/// One delivery envelope addressed to `participant_for_test`'s attempt.
pub(crate) fn delivery_envelope_for_test(
    kind: super::domain::BackendEnvelopeKind,
) -> super::rpc::BackendNativeRuntimeFilterEnvelope {
    use super::domain::{BackendRouteEdgeId, BackendTransportSequence};
    use super::rpc::{BackendNativeDeliveryRouteIdentity, BackendNativeRouteIdentity};
    use novarocks_execution::runtime_filter::RuntimeFilterChannelId;

    let execution_id = participant_execution_id();
    super::rpc::BackendNativeRuntimeFilterEnvelope::new(
        kind,
        BackendParticipantIdentity::new(
            UniqueId::new(
                execution_id.query_id().high(),
                execution_id.query_id().low(),
            ),
            execution_id.attempt_id().get(),
        ),
        RuntimeFilterChannelId::new(3),
        BackendNativeRouteIdentity::delivery(BackendNativeDeliveryRouteIdentity::new(
            BackendRouteEdgeId::new(5),
            BackendTransportSequence::new(1),
        )),
        None,
        None,
        [0; 32],
        std::sync::Arc::<[u8]>::from([]),
    )
    .expect("a delivery envelope with no payload is legal for this kind")
}
