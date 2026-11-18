// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use novarocks_types::{AttemptId, QueryId};
use std::{future::Future, pin::Pin, task::Poll};

fn activation(n: u64) -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        17,
        QueryExecutionId::new(QueryId::new(19, 23), AttemptId::new(n).unwrap()).unwrap(),
        n + 1,
    )
}
fn id(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}
fn pending() -> (State, oneshot::Receiver<RelayResponse>) {
    let (reply, receiver) = oneshot::channel();
    let mut state = State {
        next: 2,
        attempts: BTreeMap::new(),
        pending: BTreeMap::new(),
        stopped: BTreeSet::new(),
        supervision: None,
    };
    state.pending.insert(
        LogicalRequestToken::new(id(1)),
        Pending {
            source: LogicalSourceId::new(id(7)),
            activation: activation(1),
            reply,
        },
    );
    (state, receiver)
}
async fn poll_once<F: Future + ?Sized>(mut f: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(f.as_mut().poll(cx))).await
}
#[tokio::test]
async fn reply_checks_source_token_and_activation_before_consuming_original_position() {
    let (mut state, mut receiver) = pending();
    for (source, token, activation) in [
        (
            LogicalSourceId::new(id(8)),
            LogicalRequestToken::new(id(1)),
            activation(1),
        ),
        (
            LogicalSourceId::new(id(7)),
            LogicalRequestToken::new(id(2)),
            activation(1),
        ),
        (
            LogicalSourceId::new(id(7)),
            LogicalRequestToken::new(id(1)),
            activation(2),
        ),
    ] {
        assert!(
            consume_reply(&mut state, source, token, activation, RelayResponse::Opened).is_err()
        );
        assert!(poll_once(Pin::new(&mut receiver)).await.is_pending());
        assert_eq!(state.pending.len(), 1);
    }
    consume_reply(
        &mut state,
        LogicalSourceId::new(id(7)),
        LogicalRequestToken::new(id(1)),
        activation(1),
        RelayResponse::Opened,
    )
    .unwrap();
    assert!(matches!(receiver.await.unwrap(), RelayResponse::Opened));
    assert!(state.pending.is_empty());
}
#[test]
fn late_reply_after_stopped_wait_still_consumes_exact_original_position() {
    let (mut state, receiver) = pending();
    drop(receiver);
    consume_reply(
        &mut state,
        LogicalSourceId::new(id(7)),
        LogicalRequestToken::new(id(1)),
        activation(1),
        RelayResponse::Applied,
    )
    .unwrap();
    assert!(state.pending.is_empty());
    assert!(
        consume_reply(
            &mut state,
            LogicalSourceId::new(id(7)),
            LogicalRequestToken::new(id(1)),
            activation(1),
            RelayResponse::Applied
        )
        .is_err()
    );
}
#[tokio::test]
async fn stop_latch_is_level_triggered_before_and_after_subscription() {
    let (sender, mut receiver) = watch::channel(false);
    let mut waiting = Box::pin(stopped(&mut receiver));
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    sender.send_replace(true);
    waiting.await;
    let mut late = sender.subscribe();
    stopped(&mut late).await;
}
#[test]
fn typed_fetch_failure_preserves_original_attempt_failure() {
    let original = protocol("original root transport failure");
    let input = original.execution_failure().unwrap().clone();
    let projected = fetch_report(RootResultFetchFailure::from_input(input.clone()));
    assert_eq!(projected.error, original);
    assert_eq!(projected.original_attempt_failure.as_deref(), Some(&input));
}

use novarocks_execution_contract::{
    TaskIdentity,
    root_result::{RootReadOutcome, RootReadResponse, RootResultData, RootResultReply},
};
use novarocks_result_contract::{ClientRowProfile, RootOutputKind, RootProfileId, RootProfileV1};
use novarocks_types::{BackendProcessId, StageId, TaskId};
use novarocks_workload_control::{
    ResourceConfig, ResultCapacityConfig, ResultWindowGrant, RootWork, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use std::{collections::VecDeque, time::Duration};

struct OriginalWindow {
    _control: WorkloadControl,
    _work: RootWork,
    window: ResultWindowGrant,
}
fn original_window(kind: RootOutputKind) -> OriginalWindow {
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 12,
            per_scope_bytes: 1 << 18,
        },
    )
    .unwrap();
    let capacity = control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let class = if kind == RootOutputKind::ClientRows {
        ResultWindowClass::Client
    } else {
        ResultWindowClass::Internal
    };
    let window = capacity.try_acquire(&work.owner.scope(), class).unwrap();
    OriginalWindow {
        _control: control,
        _work: work,
        window,
    }
}
fn original_root() -> TaskIdentity {
    TaskIdentity::new(
        activation(1).execution(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}
fn profile() -> ClientRowProfile {
    ClientRowProfile::try_new(RootProfileV1::SEGMENT_BYTES, 1 << 20).unwrap()
}
fn carrier(kind: RootOutputKind) -> novarocks_logical_execution_protocol::start::RootCarrier {
    novarocks_logical_execution_protocol::start::RootCarrier {
        kind,
        client_rows: (kind == RootOutputKind::ClientRows).then(profile),
    }
}
async fn original_consumer(kind: RootOutputKind) -> (ConsumerExecutor, QueryResultStream) {
    let plan =
        PlanSeal::Version(novarocks_physical_plan::PlanVersionId::try_new([37; 16]).unwrap());
    let owner = ConsumerExecutor::new_client(
        QueryId::new(19, 23),
        ResultSchema::new(Vec::<ResultField>::new()),
        carrier(kind),
        plan,
        std::num::NonZeroUsize::MIN,
        None,
    )
    .unwrap();
    owner
        .execute(
            novarocks_logical_execution_protocol::effect::ConsumerEffect::ExposeOutput {
                activation: activation(1),
            },
        )
        .await;
    let mut stream = owner.take_exposed_output().unwrap();
    stream.begin_schema().unwrap().complete();
    (owner, stream)
}
struct Port {
    requests: Mutex<Vec<(Option<u64>, u64)>>,
    outcomes: Mutex<VecDeque<RootReadOutcome>>,
}
impl Port {
    fn new(outcomes: Vec<RootReadOutcome>) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            outcomes: Mutex::new(outcomes.into()),
        })
    }
    fn requests(&self) -> Vec<(Option<u64>, u64)> {
        self.requests.lock().unwrap().clone()
    }
}
impl BoundedRootReadPort for Port {
    fn read(
        &self,
        request: RootResultRead,
        physical_guard: novarocks_workload_control::ResultWindowAlias,
    ) -> LogicalFuture<'static, Result<RetainedRootReply, RootResultFetchFailure>> {
        self.requests
            .lock()
            .unwrap()
            .push((request.wanted().map(|n| n.get()), request.consumed()));
        let outcome = self
            .outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(RootReadOutcome::NotReady);
        let response = RootReadResponse::Reply(RootResultReply {
            root_task: request.root_task(),
            profile: request.profile(),
            kind: request.kind(),
            accepted_consumed: request.consumed(),
            outcome,
        });
        Box::pin(async move {
            RetainedRootReply::try_new(response, physical_guard, 64 * 1024).map_err(|_| {
                RootResultFetchFailure::from_input(
                    protocol("test reply retention failed")
                        .execution_failure()
                        .unwrap()
                        .clone(),
                )
            })
        })
    }
    fn seal(
        &self,
        _: novarocks_execution_contract::root_lifetime::RootReadSealed,
    ) -> Result<(), QueryExecutionError> {
        Ok(())
    }
}
struct Source {
    state: Arc<Mutex<State>>,
    requests: Arc<Mutex<Vec<RelayRequest>>>,
}
impl LogicalFactSource for Source {
    fn id(&self) -> LogicalSourceId {
        LogicalSourceId::new(id(7))
    }
    fn send(
        &mut self,
        fact: LogicalHostFact,
    ) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
        Box::pin(async move {
            let LogicalHostFact::Relay {
                token,
                activation,
                request,
            } = fact
            else {
                panic!("relay fact expected");
            };
            let reply = matches!(request, RelayRequest::RequestVisibility)
                .then_some(RelayResponse::VisibilityGranted);
            self.requests.lock().unwrap().push(request);
            if let Some(response) = reply {
                consume_reply(
                    &mut self.state.lock().unwrap(),
                    self.id(),
                    token,
                    activation,
                    response,
                )
                .unwrap();
            }
            Ok(())
        })
    }
}
fn state_and_source() -> (Arc<Mutex<State>>, Source) {
    let state = Arc::new(Mutex::new(State {
        next: 1,
        attempts: BTreeMap::new(),
        pending: BTreeMap::new(),
        stopped: BTreeSet::new(),
        supervision: None,
    }));
    let source = Source {
        state: Arc::clone(&state),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    (state, source)
}
fn binding(window: &OriginalWindow, port: Arc<Port>, kind: RootOutputKind) -> RootRelayBinding {
    RootRelayBinding {
        port,
        frontier: crate::coordination::RootRelayFrontier::new(
            original_root(),
            RootProfileId::V1,
            kind,
            carrier(kind).client_rows,
        )
        .unwrap(),
        window: window.window.retain_alias(),
        max_wait: Duration::from_millis(1),
    }
}
fn rows(sequence: u64, body: &'static [u8], end: Option<(u64, u64)>) -> RootReadOutcome {
    RootReadOutcome::Data(
        RootResultData::try_new(
            RootOutputKind::ClientRows,
            id(sequence),
            bytes::Bytes::from_static(body),
            end.map(|(sequence, output_rows)| RootResultEnd {
                sequence: id(sequence),
                output_rows,
            }),
        )
        .unwrap(),
    )
}
#[tokio::test]
async fn relay_prefetches_one_segment_and_waits_for_original_receipts_before_end() {
    let kind = RootOutputKind::ClientRows;
    let window = original_window(kind);
    let (consumer, mut stream) = original_consumer(kind).await;
    let port = Port::new(vec![
        rows(1, &[5, 0, 0, 0, b'a', b'b'], None),
        rows(2, &[b'c', b'd', b'e', 1, 0, 0, 0, b'f'], Some((3, 2))),
    ]);
    let (state, mut source) = state_and_source();
    let (_stop, mut stopped) = watch::channel(false);
    let mut pump = Box::pin(RelayExecutor::pump_data(
        &state,
        &consumer,
        activation(1),
        &mut source,
        &mut stopped,
        binding(&window, port.clone(), kind),
    ));
    assert!(poll_once(pump.as_mut()).await.is_pending());
    let ResultDelivery::Segment(first) = stream.next().await.unwrap().unwrap() else {
        panic!("first original segment expected");
    };
    assert_eq!(first.rows(), 0);
    assert_eq!(port.requests(), vec![(Some(1), 0), (Some(2), 0)]);
    assert!(poll_once(pump.as_mut()).await.is_pending());
    assert_eq!(port.requests().len(), 2);
    first.complete();
    assert!(poll_once(pump.as_mut()).await.is_pending());
    let ResultDelivery::Segment(second) = stream.next().await.unwrap().unwrap() else {
        panic!("second original segment expected");
    };
    assert_eq!(second.rows(), 2);
    assert!(poll_once(pump.as_mut()).await.is_pending());
    second.complete();
    let (end, ack, summary) = pump.await.unwrap().unwrap();
    assert_eq!(end.output_rows, 2);
    assert_eq!(summary.accepted_packets, 2);
    assert_eq!(summary.delivered_through, Some(1));
    assert!(ack.is_some());
    assert_eq!(
        port.requests(),
        vec![(Some(1), 0), (Some(2), 0)],
        "optional ACK factory waits for core success"
    );
}
#[tokio::test]
async fn root_withdrawal_requests_exact_control_and_stops_all_read_and_ack() {
    let kind = RootOutputKind::CountOnly;
    let window = original_window(kind);
    let (consumer, _stream) = original_consumer(kind).await;
    let port = Port::new(vec![RootReadOutcome::AwaitTerminalControl]);
    let (state, mut source) = state_and_source();
    let observed = source.requests.clone();
    let (stop, mut stopped) = watch::channel(false);
    let mut pump = Box::pin(RelayExecutor::pump_data(
        &state,
        &consumer,
        activation(1),
        &mut source,
        &mut stopped,
        binding(&window, port.clone(), kind),
    ));
    assert!(poll_once(pump.as_mut()).await.is_pending());
    assert!(
        matches!(observed.lock().unwrap().as_slice(), [RelayRequest::RootWithdrawn { root }] if root.query_execution_id() == activation(1).execution())
    );
    assert!(poll_once(pump.as_mut()).await.is_pending());
    assert_eq!(port.requests(), vec![(Some(1), 0)]);
    stop.send_replace(true);
    assert!(pump.await.unwrap().is_none());
    assert_eq!(port.requests(), vec![(Some(1), 0)]);
}
#[tokio::test]
async fn stop_retains_actual_delivery_and_validated_prefetch_without_ack() {
    let kind = RootOutputKind::ClientRows;
    let window = original_window(kind);
    let (consumer, mut stream) = original_consumer(kind).await;
    let port = Port::new(vec![
        rows(1, &[1, 0, 0, 0, b'a'], None),
        rows(2, &[1, 0, 0, 0, b'b'], Some((3, 2))),
    ]);
    let (state, mut source) = state_and_source();
    let (stop, mut stopped) = watch::channel(false);
    let mut pump = Box::pin(RelayExecutor::pump_data(
        &state,
        &consumer,
        activation(1),
        &mut source,
        &mut stopped,
        binding(&window, port.clone(), kind),
    ));
    assert!(poll_once(pump.as_mut()).await.is_pending());
    let ResultDelivery::Segment(held) = stream.next().await.unwrap().unwrap() else {
        panic!("held original segment expected");
    };
    state.lock().unwrap().stopped.insert(key(activation(1)));
    stop.send_replace(true);
    assert!(
        poll_once(pump.as_mut()).await.is_pending(),
        "Stop must retain the original delivered receipt"
    );
    assert_eq!(port.requests(), vec![(Some(1), 0), (Some(2), 0)]);
    held.complete();
    assert!(pump.await.unwrap().is_none());
    assert_eq!(port.requests(), vec![(Some(1), 0), (Some(2), 0)]);
}
#[tokio::test]
async fn malformed_rows_and_end_mismatch_are_refused_before_delivery_or_ack() {
    for outcome in [
        rows(1, &[1, 0, 0, 0], None),
        rows(1, &[1, 0, 0, 0, b'a', 1, 0, 0, 0], None),
        rows(1, &[2, 0, 0, 0, b'a'], Some((2, 1))),
        rows(1, &[1, 0, 0, 0, b'a'], Some((2, 2))),
    ] {
        let kind = RootOutputKind::ClientRows;
        let window = original_window(kind);
        let (consumer, mut stream) = original_consumer(kind).await;
        let port = Port::new(vec![outcome]);
        let (state, mut source) = state_and_source();
        let (_stop, mut stopped) = watch::channel(false);
        let failure = RelayExecutor::pump_data(
            &state,
            &consumer,
            activation(1),
            &mut source,
            &mut stopped,
            binding(&window, port.clone(), kind),
        )
        .await
        .err()
        .unwrap();
        assert!(failure.error.execution_failure().is_some());
        assert_eq!(port.requests(), vec![(Some(1), 0)]);
        let mut next = Box::pin(stream.next());
        assert!(poll_once(next.as_mut()).await.is_pending());
    }
}
#[tokio::test]
async fn original_delivery_failure_never_advances_ack_frontier() {
    let kind = RootOutputKind::ClientRows;
    let window = original_window(kind);
    let (consumer, mut stream) = original_consumer(kind).await;
    let port = Port::new(vec![rows(1, &[1, 0, 0, 0, b'a'], Some((2, 1)))]);
    let (state, mut source) = state_and_source();
    let (_stop, mut stopped) = watch::channel(false);
    let mut pump = Box::pin(RelayExecutor::pump_data(
        &state,
        &consumer,
        activation(1),
        &mut source,
        &mut stopped,
        binding(&window, port.clone(), kind),
    ));
    assert!(poll_once(pump.as_mut()).await.is_pending());
    let ResultDelivery::Segment(original) = stream.next().await.unwrap().unwrap() else {
        panic!("original segment expected");
    };
    let error = protocol("original writer failure");
    original.fail(error.clone());
    assert_eq!(pump.await.err().unwrap().error, error);
    assert_eq!(port.requests(), vec![(Some(1), 0)]);
}
#[tokio::test]
async fn count_only_preserves_original_end_rows_without_data_or_optional_ack() {
    let kind = RootOutputKind::CountOnly;
    let window = original_window(kind);
    let (consumer, _stream) = original_consumer(kind).await;
    let original = RootResultEnd {
        sequence: id(1),
        output_rows: 42,
    };
    let port = Port::new(vec![RootReadOutcome::End(original)]);
    let (state, mut source) = state_and_source();
    let (_stop, mut stopped) = watch::channel(false);
    let (end, ack, summary) = RelayExecutor::pump_data(
        &state,
        &consumer,
        activation(1),
        &mut source,
        &mut stopped,
        binding(&window, port.clone(), kind),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(end, original);
    assert_eq!(summary.accepted_packets, 0);
    assert_eq!(summary.delivered_through, None);
    assert!(ack.is_some());
    assert_eq!(port.requests(), vec![(Some(1), 0)]);
}
#[tokio::test]
async fn forged_end_sequence_cannot_fill_its_own_delivery_summary() {
    let kind = RootOutputKind::ClientRows;
    let window = original_window(kind);
    let (consumer, mut stream) = original_consumer(kind).await;
    let port = Port::new(vec![
        rows(1, &[1, 0, 0, 0, b'a'], None),
        RootReadOutcome::End(RootResultEnd {
            sequence: id(3),
            output_rows: 1,
        }),
    ]);
    let (state, mut source) = state_and_source();
    let (_stop, mut stopped) = watch::channel(false);
    let mut pump = Box::pin(RelayExecutor::pump_data(
        &state,
        &consumer,
        activation(1),
        &mut source,
        &mut stopped,
        binding(&window, port.clone(), kind),
    ));
    // The independent frontier has accepted one packet; an End claiming two
    // packets is refused before any End summary can be emitted to the core.
    assert!(matches!(
        poll_once(pump.as_mut()).await,
        Poll::Ready(Err(_))
    ));
    let mut queued = Box::pin(stream.next());
    if let Poll::Ready(Ok(Some(ResultDelivery::Segment(original)))) =
        poll_once(queued.as_mut()).await
    {
        original.complete();
    }
    assert!(port.requests().iter().all(|(_, consumed)| *consumed == 0));
}
#[tokio::test]
async fn foreign_scope_and_wrong_window_class_are_refused_before_first_read() {
    let kind = RootOutputKind::ClientRows;
    let first = original_window(kind);
    let foreign = original_window(kind);
    let (consumer, _stream) = original_consumer(kind).await;
    let port = Port::new(Vec::new());
    let scope = first._work.owner.scope();
    let mut original = binding(&foreign, port.clone(), kind);
    assert!(validate_binding(&scope, &consumer, activation(1), &original).is_err());
    let wrong = scope
        .result_capacity()
        .unwrap()
        .try_acquire(&scope, ResultWindowClass::Local)
        .unwrap();
    original.window = wrong.retain_alias();
    assert!(validate_binding(&scope, &consumer, activation(1), &original).is_err());
    assert!(port.requests().is_empty());
}
#[tokio::test]
async fn scalar_value_and_no_rows_preserve_actual_end_count_after_receipt() {
    use novarocks_result_contract::{
        BorrowedScalarLeaf, InternalResultDomain, ScalarField, ScalarLeafCursor, ScalarSchema,
        ScalarValueType,
    };
    let schema = ScalarSchema::try_new(ScalarField {
        nullable: true,
        value_type: ScalarValueType::SignedInteger(64),
    })
    .unwrap();
    let kind = RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1);
    for value in [
        BorrowedScalarLeaf::NoRows,
        BorrowedScalarLeaf::Null,
        BorrowedScalarLeaf::SignedInteger { bits: 64, value: 7 },
    ] {
        let cursor = ScalarLeafCursor::try_new(&schema, value).unwrap();
        let row_count = cursor.rows();
        let mut body = vec![0; cursor.encoded_len()];
        cursor.copy_range(0, &mut body).unwrap();
        let outcome = RootReadOutcome::Data(
            RootResultData::try_new(
                kind,
                id(1),
                bytes::Bytes::from(body),
                Some(RootResultEnd {
                    sequence: id(2),
                    output_rows: row_count,
                }),
            )
            .unwrap(),
        );
        let window = original_window(kind);
        let (consumer, mut stream) = original_consumer(kind).await;
        let port = Port::new(vec![outcome]);
        let (state, mut source) = state_and_source();
        let (_stop, mut stopped) = watch::channel(false);
        let mut pump = Box::pin(RelayExecutor::pump_data(
            &state,
            &consumer,
            activation(1),
            &mut source,
            &mut stopped,
            binding(&window, port.clone(), kind),
        ));
        assert!(poll_once(pump.as_mut()).await.is_pending());
        let ResultDelivery::Segment(original) = stream.next().await.unwrap().unwrap() else {
            panic!("original scalar segment expected");
        };
        assert_eq!(original.rows(), row_count);
        original.complete();
        let (end, _, summary) = pump.await.unwrap().unwrap();
        assert_eq!(end.output_rows, row_count);
        assert_eq!(summary.accepted_packets, 1);
        assert_eq!(summary.delivered_through, Some(0));
        assert_eq!(port.requests(), vec![(Some(1), 0)]);
    }
}
#[tokio::test]
async fn a_gapped_packet_is_refused_without_delivery_or_ack() {
    let kind = RootOutputKind::ClientRows;
    let window = original_window(kind);
    let (consumer, _stream) = original_consumer(kind).await;
    let port = Port::new(vec![rows(2, &[1, 0, 0, 0, b'a'], None)]);
    let (state, mut source) = state_and_source();
    let (_stop, mut stopped) = watch::channel(false);
    assert!(
        RelayExecutor::pump_data(
            &state,
            &consumer,
            activation(1),
            &mut source,
            &mut stopped,
            binding(&window, port.clone(), kind)
        )
        .await
        .is_err()
    );
    assert_eq!(port.requests(), vec![(Some(1), 0)]);
}

struct BlockingRead {
    original: Option<novarocks_workload_control::ResultWindowAlias>,
    entered: Option<std::sync::mpsc::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}
impl Future for BlockingRead {
    type Output = Result<RetainedRootReply, RootResultFetchFailure>;
    fn poll(self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for BlockingRead {
    fn drop(&mut self) {
        let _ = self.entered.take().unwrap().send(());
        self.release.recv_timeout(Duration::from_secs(5)).unwrap();
        self.original.take();
    }
}
#[tokio::test]
async fn cancelled_ordinary_read_keeps_original_window_until_future_drop_exits() {
    let OriginalWindow {
        _control,
        _work,
        window,
    } = original_window(RootOutputKind::ClientRows);
    let capacity = _work.owner.scope().result_capacity().unwrap();
    let (entered, observed) = std::sync::mpsc::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let original = BlockingRead {
        original: Some(window.retain_alias()),
        entered: Some(entered),
        release: gate,
    };
    drop(window);
    #[expect(
        clippy::disallowed_methods,
        reason = "This test retains and joins the original blocking destructor so its response-window alias is checked through actual Drop exit."
    )]
    let drop_job = tokio::task::spawn_blocking(move || drop(original));
    observed.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(capacity.snapshot().held_positions.iter().sum::<usize>(), 1);
    release.send(()).unwrap();
    drop_job.await.unwrap();
    assert_eq!(capacity.snapshot().held_positions.iter().sum::<usize>(), 0);
    drop((_control, _work));
}
async fn original_segment(
    window: &OriginalWindow,
    sequence: u64,
) -> (RootSegmentDelivery, ResultDeliveryReceipt) {
    let kind = RootOutputKind::ClientRows;
    let port = Port::new(vec![rows(sequence, &[1, 0, 0, 0, b'a'], None)]);
    let request = RootResultRead::try_new(
        original_root(),
        RootProfileId::V1,
        kind,
        Some(id(sequence)),
        sequence - 1,
        Duration::from_millis(1),
    )
    .unwrap();
    let reply = port
        .read(request, window.window.retain_alias())
        .await
        .unwrap();
    RootSegmentDelivery::try_new(
        activation(1).execution(),
        ResultPacketSequence::new(sequence - 1),
        reply,
        Some((
            profile(),
            novarocks_result_contract::ClientRowStreamCursor::new(),
        )),
        1,
    )
    .unwrap()
}
#[tokio::test]
async fn cancelled_delivery_waiter_recovers_same_receipt_and_durable_watermark() {
    let kind = RootOutputKind::ClientRows;
    let window = original_window(kind);
    let (consumer, mut stream) = original_consumer(kind).await;
    let (delivery, receipt) = original_segment(&window, 1).await;
    let slot = consumer.reserve_segment(activation(1)).await.unwrap();
    let mut waiter = consumer.deliver_segment(activation(1), slot, delivery, receipt);
    assert!(poll_once(waiter.as_mut()).await.is_pending());
    drop(waiter);
    assert_eq!(consumer.delivered_through(activation(1)), None);
    let ResultDelivery::Segment(original) = stream.next().await.unwrap().unwrap() else {
        panic!("same original segment expected");
    };
    original.complete();
    consumer.settle_segment(activation(1)).await.unwrap();
    assert_eq!(consumer.delivered_through(activation(1)), Some(0));
    consumer.settle_segment(activation(1)).await.unwrap();
    assert_eq!(consumer.delivered_through(activation(1)), Some(0));
}
#[tokio::test]
async fn accepted_failure_beats_queued_segment_and_window_exits_with_actual_payload() {
    let kind = RootOutputKind::ClientRows;
    let OriginalWindow {
        _control,
        _work,
        window,
    } = original_window(kind);
    let capacity = _work.owner.scope().result_capacity().unwrap();
    let fixture = OriginalWindow {
        _control,
        _work,
        window,
    };
    let (consumer, mut stream) = original_consumer(kind).await;
    let port = Port::new(vec![rows(1, &[1, 0, 0, 0, b'a'], None)]);
    let request = RootResultRead::try_new(
        original_root(),
        RootProfileId::V1,
        kind,
        Some(id(1)),
        0,
        Duration::from_millis(1),
    )
    .unwrap();
    // Retain an actual alias of the same original payload, not another grant.
    // QueryResultStream::next rejects and destroys queued deliveries on failure;
    // that destruction cannot release a still-live alias of these same bytes.
    let physical_reply = Arc::new(
        port.read(request, fixture.window.retain_alias())
            .await
            .unwrap(),
    );
    let (delivery, receipt) = RootSegmentDelivery::try_new(
        activation(1).execution(),
        ResultPacketSequence::new(0),
        Arc::clone(&physical_reply),
        Some((
            profile(),
            novarocks_result_contract::ClientRowStreamCursor::new(),
        )),
        1,
    )
    .unwrap();
    let slot = consumer.reserve_segment(activation(1)).await.unwrap();
    let waiter = consumer.deliver_segment(activation(1), slot, delivery, receipt);
    drop(waiter);
    drop(fixture.window);
    let original = protocol("original accepted governed cancellation");
    consumer
        .execute(
            novarocks_logical_execution_protocol::effect::ConsumerEffect::PublishFailure(
                original.clone(),
            ),
        )
        .await;
    assert_eq!(capacity.snapshot().held_positions.iter().sum::<usize>(), 1);
    assert_eq!(stream.next().await.err(), Some(original));
    assert_eq!(
        capacity.snapshot().held_positions.iter().sum::<usize>(),
        1,
        "the rejected queue cannot release a live alias of the original payload"
    );
    drop(stream);
    assert_eq!(
        consumer.settle_segment(activation(1)).await.unwrap_err(),
        novarocks_logical_execution_protocol::failure::result_delivery_failed(),
    );
    assert_eq!(capacity.snapshot().held_positions.iter().sum::<usize>(), 1);
    assert_eq!(port.requests(), vec![(Some(1), 0)]);
    drop(physical_reply);
    assert_eq!(capacity.snapshot().held_positions.iter().sum::<usize>(), 0);
}
#[test]
fn failure_wrapper_preserves_unsupported_task_site_and_stop_observation() {
    use novarocks_coordination_contract::{ExecutionFailureInput, ExecutionFailureSite};
    use novarocks_execution_contract::TaskFailurePhase;
    use novarocks_execution_contract::failure::{
        FailureClass, FailureEvidence, FailureFact, FailureReason, domain,
    };
    use std::num::NonZeroU16;
    let reason = FailureReason::new(domain::LOCAL_COMPILER, NonZeroU16::MIN);
    let original = ExecutionFailureInput::new(
        FailureFact::new(FailureClass::Unsupported, reason),
        novarocks_execution_contract::SafeDetail::truncating("original unsupported compiled shape"),
        ExecutionFailureSite::Task {
            identity: original_root(),
            phase: TaskFailurePhase::Preparation,
        },
    );
    let projected = fetch_report(RootResultFetchFailure::from_input(original.clone()));
    assert_eq!(
        projected.original_attempt_failure.as_deref(),
        Some(&original)
    );
    assert_eq!(projected.error.execution_failure(), Some(&original));
    let observed = ExecutionFailureInput::new(
        FailureFact::stop_observation(reason),
        novarocks_execution_contract::SafeDetail::truncating("original authority stopped"),
        ExecutionFailureSite::Coordinator,
    );
    let projected = fetch_report(RootResultFetchFailure::from_input(observed.clone()));
    assert_eq!(projected.error.execution_failure(), Some(&observed));
    assert_eq!(
        projected
            .error
            .execution_failure()
            .unwrap()
            .fact()
            .evidence(),
        FailureEvidence::StopObservation
    );
}
#[tokio::test]
async fn failed_prefetch_keeps_held_delivery_unsettled_until_independent_stop_cleanup() {
    let kind = RootOutputKind::ClientRows;
    let window = original_window(kind);
    let (consumer, mut stream) = original_consumer(kind).await;
    let port = Port::new(vec![
        rows(1, &[1, 0, 0, 0, b'a'], None),
        RootReadOutcome::End(RootResultEnd {
            sequence: id(3),
            output_rows: 1,
        }),
    ]);
    let (state, mut source) = state_and_source();
    let (_stop, mut stopped) = watch::channel(false);
    let failed = RelayExecutor::pump_data(
        &state,
        &consumer,
        activation(1),
        &mut source,
        &mut stopped,
        binding(&window, port.clone(), kind),
    )
    .await;
    assert!(failed.is_err());
    assert!(
        !consumer.deliveries_settled(activation(1)),
        "a failed Start cannot certify the held original delivery"
    );
    let ResultDelivery::Segment(original) = stream.next().await.unwrap().unwrap() else {
        panic!("held original segment expected");
    };
    let mut cleanup = Box::pin(consumer.settle_deliveries(activation(1)));
    assert!(
        poll_once(cleanup.as_mut()).await.is_pending(),
        "cleanup must independently await the exact stable receipt"
    );
    assert_eq!(port.requests(), vec![(Some(1), 0), (Some(2), 0)]);
    original.complete();
    cleanup.await.unwrap();
    assert!(consumer.deliveries_settled(activation(1)));
    assert_eq!(consumer.delivered_through(activation(1)), Some(0));
    assert_eq!(
        port.requests(),
        vec![(Some(1), 0), (Some(2), 0)],
        "cleanup cannot issue a fresh data read or ACK"
    );
}

#[tokio::test]
async fn supervised_failure_drains_original_relay_reply_once_and_rejects_late_request() {
    let (state, mut source) = state_and_source();
    let held = Arc::new(Mutex::new(Vec::new()));
    struct HoldingSource(Arc<Mutex<Vec<LogicalHostFact>>>);
    impl LogicalFactSource for HoldingSource {
        fn id(&self) -> LogicalSourceId {
            LogicalSourceId::new(id(7))
        }
        fn send(
            &mut self,
            fact: LogicalHostFact,
        ) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
            Box::pin(async move {
                self.0.lock().unwrap().push(fact);
                Ok(())
            })
        }
    }
    let mut original = HoldingSource(held.clone());
    let mut request = Box::pin(relay_request(
        &state,
        &mut original,
        activation(1),
        RelayRequest::RequestVisibility,
    ));
    assert!(poll_once(request.as_mut()).await.is_pending());
    assert_eq!(held.lock().unwrap().len(), 1);
    let error = protocol("original relay owner failure");
    begin_supervised_failure(&state, error.clone());
    begin_supervised_failure(&state, protocol("secondary relay failure"));
    assert!(fail_one_supervised_request(&state));
    assert!(!fail_one_supervised_request(&state));
    assert!(matches!(request.await, Err(actual) if actual == error));
    assert!(
        matches!(relay_request(&state, &mut source, activation(1), RelayRequest::RequestVisibility).await,
        Err(actual) if actual == error)
    );
    assert!(state.lock().unwrap().pending.is_empty());
    assert!(
        source.requests.lock().unwrap().is_empty(),
        "no fresh source send after failure latch"
    );
}

// Data-plane component tier: Core's replacement policy/qualification is covered
// by its public tests. This uses one original client transport and two passive
// activation identities; it never claims an actual Native/AP Scope exited.
#[tokio::test]
async fn successor_segment_uses_same_original_global_client_stream_while_predecessor_cleanup_is_held()
 {
    use std::sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct OriginalCleanupFrame {
        _original_window: novarocks_workload_control::ResultWindowAlias,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for OriginalCleanupFrame {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    type PumpResult = Result<
        Option<(RootResultEnd, Option<FinalAck>, DeliverySummary)>,
        novarocks_logical_execution_protocol::input::FailureReport,
    >;
    struct Originals {
        control: Option<WorkloadControl>,
        work: Option<RootWork>,
        window: Option<ResultWindowGrant>,
        consumer: Option<Arc<ConsumerExecutor>>,
        stream: Option<QueryResultStream>,
        cleanup: Option<LogicalFuture<'static, ()>>,
        release: Option<oneshot::Sender<()>>,
        pump: Option<LogicalFuture<'static, PumpResult>>,
        stop: Option<watch::Sender<bool>>,
        state: Arc<Mutex<State>>,
        held: Option<RootSegmentDelivery>,
    }
    struct KeepWhole(Option<Box<Originals>>);
    impl Drop for KeepWhole {
        fn drop(&mut self) {
            if std::thread::panicking() {
                static ORIGINALS: OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                    OnceLock::new();
                if let Some(original) = self.0.take() {
                    ORIGINALS
                        .get_or_init(Default::default)
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(original);
                }
            }
        }
    }
    let OriginalWindow {
        _control: control,
        _work: work,
        window,
    } = original_window(RootOutputKind::ClientRows);
    let scope = work.owner.scope();
    let capacity = scope.result_capacity().unwrap();
    let (consumer, stream) = original_consumer(RootOutputKind::ClientRows).await;
    let consumer = Arc::new(consumer);
    let (state, source) = state_and_source();
    let (release, gate) = oneshot::channel();
    let entered = Arc::new(AtomicBool::new(false));
    let cleanup_drops = Arc::new(AtomicUsize::new(0));
    let original_frame = OriginalCleanupFrame {
        _original_window: window.retain_alias(),
        drops: cleanup_drops.clone(),
    };
    let seen = entered.clone();
    let cleanup = Box::pin(async move {
        let _same_original_cleanup_frame = original_frame;
        seen.store(true, Ordering::SeqCst);
        gate.await.unwrap();
    });
    let mut keep = KeepWhole(Some(Box::new(Originals {
        control: Some(control),
        work: Some(work),
        window: Some(window),
        consumer: Some(consumer.clone()),
        stream: Some(stream),
        cleanup: Some(cleanup),
        release: Some(release),
        pump: None,
        stop: None,
        state: state.clone(),
        held: None,
    })));
    let actual = keep.0.as_deref_mut().unwrap();
    assert!(
        poll_once(actual.cleanup.as_mut().unwrap().as_mut())
            .await
            .is_pending()
    );
    assert!(entered.load(Ordering::SeqCst));
    assert_eq!(cleanup_drops.load(Ordering::SeqCst), 0);
    assert_eq!(capacity.snapshot().held_positions[0], 1);
    assert!(
        actual.stream.as_mut().unwrap().begin_schema().is_none(),
        "the original schema is already written once"
    );
    let successor = activation(2);
    let original_root = original_root();
    let successor_root = TaskIdentity::new(
        successor.execution(),
        original_root.stage_id(),
        original_root.task_id(),
        original_root.backend_process_id(),
    );
    let port = Port::new(vec![
        rows(1, &[1, 0, 0, 0, b'b'], None),
        rows(2, &[1, 0, 0, 0, b'c'], Some((3, 2))),
    ]);
    let binding = RootRelayBinding {
        port: port.clone(),
        frontier: crate::coordination::RootRelayFrontier::new(
            successor_root,
            RootProfileId::V1,
            RootOutputKind::ClientRows,
            Some(profile()),
        )
        .unwrap(),
        window: actual.window.as_ref().unwrap().retain_alias(),
        max_wait: Duration::from_millis(1),
    };
    let (stop, mut stopped) = watch::channel(false);
    actual.stop = Some(stop);
    actual.pump = Some(Box::pin(async move {
        let mut source = source;
        RelayExecutor::pump_data(
            &state,
            &consumer,
            successor,
            &mut source,
            &mut stopped,
            binding,
        )
        .await
    }));
    assert!(
        poll_once(actual.pump.as_mut().unwrap().as_mut())
            .await
            .is_pending()
    );
    let Some(ResultDelivery::Segment(segment)) =
        actual.stream.as_mut().unwrap().next().await.unwrap()
    else {
        panic!("same original global client stream must receive the successor segment");
    };
    actual.held = Some(segment);
    let segment = actual.held.as_ref().unwrap();
    assert_eq!(segment.execution_id(), successor.execution());
    assert_eq!(segment.sequence(), ResultPacketSequence::new(0));
    assert_eq!(segment.rows(), 1);
    assert_eq!(segment.body(), &[1, 0, 0, 0, b'b']);
    assert_eq!(port.requests(), vec![(Some(1), 0), (Some(2), 0)]);
    assert!(
        actual
            .consumer
            .as_ref()
            .unwrap()
            .take_exposed_output()
            .is_none(),
        "no second client stream was constructed or handed off"
    );
    assert!(
        poll_once(actual.cleanup.as_mut().unwrap().as_mut())
            .await
            .is_pending()
    );
    assert_eq!(
        cleanup_drops.load(Ordering::SeqCst),
        0,
        "B delivery cannot release A's actual cleanup frame"
    );
    actual.state.lock().unwrap().stopped.insert(key(successor));
    actual.stop.as_ref().unwrap().send_replace(true);
    assert!(
        poll_once(actual.pump.as_mut().unwrap().as_mut())
            .await
            .is_pending(),
        "the actual B receipt is still held"
    );
    actual.held.take().unwrap().complete();
    assert!(actual.pump.take().unwrap().await.unwrap().is_none());
    assert_eq!(
        port.requests(),
        vec![(Some(1), 0), (Some(2), 0)],
        "cancelled prefetch does not send a new read or ACK"
    );
    drop(actual.consumer.take());
    drop(actual.stream.take());
    drop(actual.window.take());
    assert_eq!(
        capacity.snapshot().held_positions[0],
        1,
        "only A's actual cleanup alias remains"
    );
    actual.release.take().unwrap().send(()).unwrap();
    actual.cleanup.take().unwrap().await;
    assert_eq!(cleanup_drops.load(Ordering::SeqCst), 1);
    assert_eq!(capacity.snapshot().held_positions[0], 0);
    let work = actual.work.take().unwrap();
    work.business.release();
    work.owner.complete();
    scope.wait_released().await;
    actual.control.as_ref().unwrap().close_admission();
    if let Err(failure) = actual.control.take().unwrap().shutdown() {
        let (error, control) = failure.into_parts();
        actual.control = Some(control);
        panic!("same original client-window component refused actual shutdown: {error}");
    }
}

#[path = "tests/carrier_kind.rs"]
mod carrier_kind;
