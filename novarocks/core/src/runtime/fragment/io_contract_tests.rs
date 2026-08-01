use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int32Array};

use super::io::error::{FragmentIoError, FragmentIoErrorKind, FragmentIoOperation};
use super::io::events::{
    FragmentEvent, FragmentEventSink, FragmentProgress, NoopFragmentEventSink,
};
use super::io::exchange::{ExchangeFrame, ExchangeFrameTransmitter};
use super::io::lookup::{LookupColumn, LookupKind, LookupRequest, LookupTarget};
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::query_context::QueryId;

#[test]
fn fragment_io_contract_error_preserves_operation_and_kind() {
    let error = FragmentIoError::new(
        FragmentIoOperation::ExchangeTransmit,
        FragmentIoErrorKind::Unavailable,
        "destination BE-2 is unavailable",
    );

    assert_eq!(error.operation(), FragmentIoOperation::ExchangeTransmit);
    assert_eq!(error.kind(), FragmentIoErrorKind::Unavailable);
    assert_eq!(
        error.to_string(),
        "exchange transmit unavailable: destination BE-2 is unavailable"
    );
}

#[derive(Default)]
struct RecordingTransmitter {
    frames: Mutex<Vec<ExchangeFrame>>,
}

impl ExchangeFrameTransmitter for RecordingTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), FragmentIoError> {
        self.frames
            .lock()
            .expect("record exchange frame")
            .push(frame);
        Ok(())
    }
}

#[test]
fn exchange_frame_transmitter_preserves_sequence_eos_and_payload() {
    let transmitter = RecordingTransmitter::default();
    let frame = ExchangeFrame {
        destination: RuntimeEndpoint::new("be-2", 9060).expect("valid destination"),
        destination_fragment_instance_id: UniqueId::new(1, 2),
        sender_fragment_instance_id: UniqueId::new(3, 4),
        destination_node_id: 17,
        sender_id: 9,
        backend_number: 2,
        sequence: 2,
        eos: true,
        payload: vec![7, 8, 9],
    };

    transmitter.transmit(frame).expect("transmit frame");

    let frames = transmitter.frames.lock().expect("read exchange frames");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].destination.host(), "be-2");
    assert_eq!(frames[0].destination.port(), 9060);
    assert_eq!(frames[0].sequence, 2);
    assert!(frames[0].eos);
    assert_eq!(frames[0].payload, vec![7, 8, 9]);
}

#[test]
fn lookup_request_keeps_target_identity_and_arrow_columns() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![101, 202]));
    let request = LookupRequest::new(
        QueryId::new(10, 11),
        22,
        33,
        LookupKind::PrimaryKey,
        LookupTarget::new(
            44,
            Some(RuntimeEndpoint::new("be-3", 9060).expect("valid target")),
        ),
        vec![LookupColumn::new(SlotId::new(55), values)],
    );

    assert_eq!(request.query_id(), QueryId::new(10, 11));
    assert_eq!(request.lookup_node_id(), 22);
    assert_eq!(request.tuple_id(), 33);
    assert_eq!(request.kind(), LookupKind::PrimaryKey);
    assert_eq!(request.target().backend_id(), 44);
    assert_eq!(
        request.target().endpoint().expect("endpoint").host(),
        "be-3"
    );
    assert_eq!(request.columns().len(), 1);
    assert_eq!(request.columns()[0].slot_id(), SlotId::new(55));
    assert_eq!(request.columns()[0].values().len(), 2);
}

#[test]
fn noop_fragment_event_sink_never_changes_execution_control_flow() {
    let sink = NoopFragmentEventSink;

    sink.record(FragmentEvent::Progress(FragmentProgress::new(
        UniqueId::new(7, 8),
        620,
        580,
        1000,
    )));
}
