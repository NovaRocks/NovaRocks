//! Host capability ports used by a prepared fragment.

pub mod commit;
pub mod error;
pub mod events;
pub mod exchange;
pub mod exchange_edge;
pub mod exchange_metrics;
pub mod exchange_receiver;
pub(crate) mod exchange_retained_payload;
pub(crate) mod exchange_sender_group;
pub mod exchange_stream_sender;
pub mod exchange_unit;
pub mod result;

pub use commit::{
    TabletCommitInfo, TabletFailInfo, TaskCommitPort, TaskCommitRegistration, TaskCommitReport,
    TaskSinkLoadStats, UnavailableTaskCommitPort,
};
pub use error::{TaskIoError, TaskIoErrorKind, TaskIoOperation};
pub use events::{NoopTaskEventSink, TaskEvent, TaskEventSink, TaskProfileSnapshot, TaskProgress};
pub use exchange::ExchangeFrameTransmitter;
pub use exchange_edge::{
    EdgeOpenAccepted, EdgeSendGate, EdgeSendState, ExchangeDestinationKey, ExchangeEdgeGateError,
    ExchangeEdgeGates,
};
pub use exchange_receiver::{
    ExchangeReceiverKey, ExchangeReceiverPort, ExchangeReceiverRegistration,
    ExecutionRuntimeExchangeReceiverPort, UnavailableExchangeReceiverPort,
};
pub use result::{
    ResultAbort, ResultInputMetadata, ResultPresentation, ResultProjection, ResultWriteAdmission,
    ResultWriteCredit, ResultWriteFailure, ResultWriteSpec, RootFailureObservation,
    RootInputAdmission, RootInputAuthority, RootInputPermit, RootInputReadinessNotifier,
    RootInputReservation, RootInputReservationAdmission, RootProducerState, RootResultInput,
    RootResultSession, RootResultWriteSpec, RootResultWriter, RootWorkExitProgress,
    TaskResultSession, TaskResultWriter,
};
