use std::fmt;

use novarocks_execution_contract::{
    ExchangeCloseDisposition, ExchangeDeliveryRequirement, ExchangeStreamIdentity, TaskIdentity,
};
use novarocks_types::UniqueId;

use crate::runtime::endpoint::RuntimeEndpoint;
use crate::task_execution::status::CancelReason;

use super::FragmentIoError;

/// Encoded exchange payload prepared by the execution kernel for a host-owned
/// transport adapter.
#[derive(Clone, Debug)]
pub struct ExchangeFrame {
    /// Local operator fixtures have no Task. Native transport requires the
    /// complete stream and checks every duplicated envelope field against it.
    pub stream: Option<ExchangeStreamIdentity>,
    pub delivery: ExchangeDeliveryRequirement,
    pub destination: RuntimeEndpoint,
    pub destination_fragment_instance_id: UniqueId,
    pub destination_task_identity: Option<TaskIdentity>,
    pub sender_fragment_instance_id: UniqueId,
    pub sender_ordinal: u32,
    pub sender_count: u32,
    pub destination_node_id: i32,
    pub sender_id: i32,
    pub backend_number: i32,
    pub sequence: i64,
    pub eos: bool,
    pub payload: Vec<u8>,
}

/// Why a destination refused one exchange frame.
///
/// A destination's normal departure and a real failure reach the producer
/// through the same call, and only one of them is this producer's error. The
/// enum is closed and keeps [`FragmentIoError`] for everything else, so a
/// failure can never be reported as a success-compatible cancellation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExchangeTransmitRejection {
    /// A receiver-authored close for this exact stream. This is separate from
    /// an ordinary ACK, which proves admission rather than consumption.
    NormallyClosed(ExchangeCloseDisposition),
    /// The destination withdrew its ingress capability because it no longer
    /// needs this producer's output. The producer closes that one edge,
    /// discards that edge's frames, and waits to be cancelled by its
    /// coordinator; it never concludes on its own that its stage succeeded.
    DestinationCanceled(CancelReason),
    /// Every other outcome: a failed, aborted, finished, or unknown
    /// destination, an identity, process, source, or edge mismatch, a
    /// malformed request, and every transport error.
    Failed(FragmentIoError),
}

impl ExchangeTransmitRejection {
    /// The normal cancellation reason, if this is a destination's normal
    /// departure rather than a failure.
    pub const fn cancel_reason(&self) -> Option<CancelReason> {
        match self {
            Self::NormallyClosed(_) => None,
            Self::DestinationCanceled(reason) => Some(*reason),
            Self::Failed(_) => None,
        }
    }

    /// The producer's own failure cause, if this rejection is one.
    pub const fn failure(&self) -> Option<&FragmentIoError> {
        match self {
            Self::NormallyClosed(_) => None,
            Self::DestinationCanceled(_) => None,
            Self::Failed(error) => Some(error),
        }
    }
}

impl fmt::Display for ExchangeTransmitRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NormallyClosed(disposition) => {
                write!(formatter, "exchange input closed: {disposition:?}")
            }
            Self::DestinationCanceled(reason) => {
                write!(formatter, "exchange destination cancelled: {reason}")
            }
            Self::Failed(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ExchangeTransmitRejection {}

impl From<FragmentIoError> for ExchangeTransmitRejection {
    fn from(error: FragmentIoError) -> Self {
        Self::Failed(error)
    }
}

/// Host-owned transport boundary for exchange frames.
pub trait ExchangeFrameTransmitter: Send + Sync + 'static {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), ExchangeTransmitRejection>;

    /// A task-scoped stop signal. Transports with cancellable I/O should
    /// override this so cancellation interrupts a blocked send as well as
    /// preventing queued successors from starting.
    fn transmit_cancellable(
        &self,
        frame: ExchangeFrame,
        _stop: tokio_util::sync::CancellationToken,
    ) -> Result<(), ExchangeTransmitRejection> {
        self.transmit(frame)
    }
}

#[cfg(test)]
pub(crate) fn discard_exchange_transmitter() -> std::sync::Arc<dyn ExchangeFrameTransmitter> {
    std::sync::Arc::new(DiscardExchangeFrameTransmitter)
}

#[cfg(test)]
struct DiscardExchangeFrameTransmitter;

#[cfg(test)]
impl ExchangeFrameTransmitter for DiscardExchangeFrameTransmitter {
    fn transmit(&self, _frame: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn in_process_test_exchange_receiver_port()
-> std::sync::Arc<dyn super::ExchangeReceiverPort> {
    std::sync::Arc::new(TestExchangeReceiverPort::default())
}

#[cfg(test)]
#[derive(Default)]
struct TestExchangeReceiverPort {
    registry: std::sync::Arc<crate::runtime::exchange::ExecutionExchangeRegistry>,
}

#[cfg(test)]
impl TestExchangeReceiverPort {
    fn key(key: super::ExchangeReceiverKey) -> crate::runtime::exchange::ExchangeKey {
        crate::runtime::exchange::ExchangeKey {
            finst_id_hi: key.fragment_instance_id.high(),
            finst_id_lo: key.fragment_instance_id.low(),
            node_id: key.node_id,
        }
    }
}

#[cfg(test)]
impl super::ExchangeReceiverPort for TestExchangeReceiverPort {
    fn register(
        &self,
        registration: super::ExchangeReceiverRegistration,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        self.registry
            .try_register_expected_chunk_schema_with_binding(
                Self::key(registration.key),
                registration.expected_senders,
                registration.expected_chunk_schema,
                registration.column_binding,
                registration.creation_factory,
            )
    }

    fn push(
        &self,
        key: super::ExchangeReceiverKey,
        frame: super::ExchangeReceiverFrame,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        self.registry.admit_native_frame(Self::key(key), frame)
    }

    fn cancel(&self, key: super::ExchangeReceiverKey) {
        self.registry.cancel_exchange_key(Self::key(key));
    }
    fn remove(&self, key: super::ExchangeReceiverKey) {
        self.registry.remove_exchange_key(Self::key(key));
    }
    fn cancel_fragment(&self, fragment_instance_id: UniqueId) {
        self.registry
            .cancel_fragment(fragment_instance_id.high(), fragment_instance_id.low());
    }
    fn receiver_handle(
        &self,
        key: super::ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<
        crate::runtime::exchange::ExchangeReceiverHandle,
        crate::runtime::exchange::ExchangeFailure,
    > {
        self.registry
            .get_receiver_handle(Self::key(key), expected_senders)
    }
    fn stream_close_facts(
        &self,
        key: super::ExchangeReceiverKey,
        expected: &[novarocks_execution_contract::ExchangeStreamIdentity],
    ) -> Result<
        Vec<novarocks_execution_contract::ExchangeStreamClose>,
        crate::runtime::exchange::ExchangeFailure,
    > {
        self.registry.stream_close_facts(Self::key(key), expected)
    }
    fn ensure_mem_tracker(
        &self,
        key: super::ExchangeReceiverKey,
        root: &std::sync::Arc<crate::runtime::mem_tracker::MemTracker>,
    ) -> Result<
        std::sync::Arc<crate::runtime::mem_tracker::MemTracker>,
        crate::runtime::exchange::ExchangeFailure,
    > {
        self.registry
            .ensure_receiver_mem_tracker(Self::key(key), root)
    }
    fn push_local(
        &self,
        key: super::ExchangeReceiverKey,
        sender_id: i32,
        backend_number: i32,
        chunks: Vec<crate::exec::chunk::Chunk>,
        eos: bool,
    ) {
        self.registry.push_chunks(
            Self::key(key),
            crate::runtime::exchange::ExchangeSenderIdentity::local(sender_id, backend_number),
            chunks,
            eos,
        );
    }
    fn snapshot(
        &self,
        key: super::ExchangeReceiverKey,
    ) -> Option<crate::runtime::exchange::ExchangeReceiverSnapshot> {
        self.registry.snapshot_receiver_state(Self::key(key))
    }
}

#[cfg(test)]
pub(crate) fn fixture_stream(
    source_kernel: UniqueId,
    destination_kernel: UniqueId,
    node: i32,
    ordinal: u32,
) -> ExchangeStreamIdentity {
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    static PROCESS: std::sync::OnceLock<BackendProcessId> = std::sync::OnceLock::new();
    let process = *PROCESS.get_or_init(BackendProcessId::new_v7);
    let query = QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
    let source = TaskIdentity::new(
        query,
        StageId::new(1).unwrap(),
        TaskId::new(ordinal + 1).unwrap(),
        process,
    );
    let target = TaskIdentity::new(
        query,
        StageId::new(2).unwrap(),
        TaskId::new(1).unwrap(),
        process,
    );
    ExchangeStreamIdentity::try_new(
        novarocks_execution_contract::ExchangeSource::new(source, source_kernel, ordinal),
        target,
        destination_kernel,
        novarocks_execution_contract::FragmentNodeId::new(node),
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::in_process_test_exchange_receiver_port;
    use crate::runtime::fragment::io::{ExchangeReceiverFrame, ExchangeReceiverKey};
    use novarocks_types::UniqueId;

    #[test]
    fn native_eos_uses_exact_source_identity_when_legacy_sender_fields_collide() {
        let port = in_process_test_exchange_receiver_port();
        let key = ExchangeReceiverKey {
            fragment_instance_id: UniqueId::new(50, 60),
            node_id: 70,
        };
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("exact native EOS fixture");
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(), std::sync::Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        let layout = crate::exec::chunk::typed_fixture::layout(
            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            &[],
            &[],
        );
        let expected = factory.create_compiled_chunk_schema(&layout).unwrap();
        port.register(crate::runtime::fragment::io::ExchangeReceiverRegistration {
            key,
            expected_senders: 2,
            expected_chunk_schema: expected,
            column_binding: crate::runtime::exchange::ExchangeColumnBinding::Positional,
            creation_factory: factory.clone(),
        })
        .expect("actual native receiver installation");
        let frame = |source_fragment_instance_id, sender_ordinal| ExchangeReceiverFrame {
            stream: super::fixture_stream(
                source_fragment_instance_id,
                key.fragment_instance_id,
                key.node_id,
                sender_ordinal,
            ),
            source_fragment_instance_id,
            sender_ordinal,
            sender_count: 2,
            // These legacy fields intentionally collide. Native completion
            // identity must not use either of them.
            sender_id: 7,
            backend_number: 11,
            sequence: 0,
            eos: true,
            payload: Vec::new(),
        };

        port.push(key, frame(UniqueId::new(1, 1), 0))
            .expect("first native eos");
        port.push(key, frame(UniqueId::new(2, 2), 1))
            .expect("second native eos");

        let snapshot = port.snapshot(key).expect("receiver snapshot");
        assert_eq!(snapshot.finished_senders, 2);
        port.remove(key);
        assert!(port.snapshot(key).is_none());
        drop(factory);
        assert_eq!(
            tracker.current(),
            0,
            "removed receiver releases its finite creation"
        );
    }
}
