//! Backend-native runtime-filter test helpers.
//!
//! These helpers construct adapter-owned participants and envelopes. Pure
//! contract fixtures are Worker-owned in `runtime_filter::fixture`.

use novarocks_types::UniqueId;
use novarocks_worker::runtime_filter::domain::BackendParticipantIdentity;

/// One installed, channel-less participant on a well-known attempt.
///
/// Channel-less is deliberate: it is the cheapest install that is still a real
/// participant, so a test about *reaching* a participant does not have to
/// build a route graph first. Any envelope it receives is refused by its own
/// route authority, which is exactly what distinguishes "the participant
/// decided" from "nobody was asked".
pub(crate) fn participant_for_test()
-> std::sync::Arc<novarocks_native_adapter::runtime_filter_participant::RuntimeFilterParticipant> {
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
    let decoded =
        novarocks_native_adapter::runtime_filter_install::decode_runtime_filter_contribution(
            participant_execution_id(),
            &contribution,
        )
        .expect("a channel-less contribution decodes");
    BackendRuntimeFilterParticipantFactory::new(
        novarocks_native_adapter::backend_test_support::test_backend_data_runtime(),
    )
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
    kind: novarocks_worker::runtime_filter::domain::BackendEnvelopeKind,
) -> novarocks_native_adapter::runtime_filter_rpc::BackendNativeRuntimeFilterEnvelope {
    use novarocks_execution::runtime_filter::RuntimeFilterChannelId;
    use novarocks_native_adapter::runtime_filter_rpc::{
        BackendNativeDeliveryRouteIdentity, BackendNativeRouteIdentity,
    };
    use novarocks_worker::runtime_filter::domain::{BackendRouteEdgeId, BackendTransportSequence};

    let execution_id = participant_execution_id();
    novarocks_native_adapter::runtime_filter_rpc::BackendNativeRuntimeFilterEnvelope::new(
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
