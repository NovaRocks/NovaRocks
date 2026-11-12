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
use novarocks_attempt_protocol::{
    AttemptCore, AttemptStep, Core, InputSelection,
    capacity::{QueueStage, StepContext},
    config::DispatchLane,
    effect::{AttemptEffect, EffectBody, Publication},
    input::{
        AttemptInputClaim, AttemptInputId, AttemptSourceCut, AttemptSourceId, PublicationApplied,
    },
    start::AttemptSources,
};
use novarocks_control_runtime::history::HistoryCapture;
use novarocks_control_runtime_core::history::HistoryFact;
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion};

pub(super) type Capture = Arc<Mutex<Option<(InputEnvelope, AttemptInput)>>>;

pub(super) struct Trace {
    pub(super) key: StreamKey,
    pub(super) history: HistoryCapture,
    pub(super) capture: Capture,
}

fn at(ms: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_millis(ms))
}

fn positive(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}

struct Turn(usize);

impl StepContext for Turn {
    fn charge(&mut self, units: usize) -> bool {
        let Some(remaining) = self.0.checked_sub(units) else {
            return false;
        };
        self.0 = remaining;
        true
    }

    fn reserve_queue_stage(
        &mut self,
        _: DispatchLane,
        _: bool,
        _: BackendProcessId,
        _: usize,
        _: usize,
    ) -> QueueStage {
        panic!("the original observation Source cannot admit Native sends")
    }
}

struct Driver {
    core: Core,
    sources: AttemptSources,
    continuation: u64,
}

impl Driver {
    fn bootstrap() -> (Self, StreamKey, TaskIdentity) {
        let mut start = crate::task_execution::host::adapter::tests::start();
        let root = start.plan.graph.root;
        let participation = start.plan.graph.tasks[&root.task_id()].participation;
        let source = |n| AttemptSourceId(positive(n));
        start.sources = AttemptSources {
            actor: source(1),
            observation: source(2),
            subscription: source(3),
            publication: source(6),
            qa: source(7),
            process: source(8),
            native: source(9),
            transport: source(10),
            capacity: source(11),
            membership: source(12),
            ..start.sources
        };
        let sources = start.sources;
        let mut driver = Self {
            core: Core::new(start).unwrap(),
            sources,
            continuation: 100_000,
        };
        let bootstrap = driver.synthetic(at(0), sources.actor, AttemptInput::Bootstrap);
        assert_eq!(bootstrap.selection, InputSelection::Prefer(sources.native));
        let generation = driver
            .core
            .open_subscription_for_test(
                participation,
                AttemptActionId::new(positive(1_000_000)),
                token(1_000_000),
                &mut Turn(64),
            )
            .unwrap();
        (
            driver,
            StreamKey {
                participation,
                generation,
            },
            root,
        )
    }

    fn synthetic(
        &mut self,
        now: MonotonicInstant,
        source: AttemptSourceId,
        input: AttemptInput,
    ) -> AttemptStep {
        self.continuation += 1;
        self.handle(
            now,
            source,
            AttemptInputId(positive(self.continuation)),
            input,
        )
    }

    fn handle(
        &mut self,
        now: MonotonicInstant,
        source: AttemptSourceId,
        id: AttemptInputId,
        input: AttemptInput,
    ) -> AttemptStep {
        self.core
            .handle(
                now,
                AttemptInputClaim {
                    source,
                    input: id,
                    native_cut: Some(AttemptSourceCut {
                        source: self.sources.native,
                        tail: None,
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: self.sources.observation,
                        tail: (source == self.sources.observation).then_some(id),
                    }),
                },
                input,
                &mut Turn(64),
            )
            .unwrap()
    }

    fn finish_round(&mut self, now: MonotonicInstant, mut step: AttemptStep) -> Vec<EffectBody> {
        let mut effects = Vec::new();
        for _ in 0..32 {
            assert!(!step.stop_producing, "{step:?}");
            if let Some(result) = &step.round_result {
                assert!(result.is_ok(), "{result:?}");
            }
            let applied = step
                .effect
                .take()
                .and_then(|AttemptEffect { id, body, .. }| {
                    let applied = match body {
                        EffectBody::Publish(Publication::ProtocolBatch(_)) => {
                            PublicationApplied::ProtocolBatch { published: true }
                        }
                        EffectBody::Publish(Publication::RootStatus(_)) => {
                            PublicationApplied::RootStatus
                        }
                        EffectBody::AdjustReadWindow(_) => return None,
                        effect => {
                            effects.push(effect);
                            return None;
                        }
                    };
                    Some(ExecutorFact::PublicationApplied {
                        effect: id,
                        result: Ok(applied),
                    })
                });
            match (step.selection, applied) {
                (InputSelection::Only(source), Some(fact))
                    if source == self.sources.publication =>
                {
                    step = self.synthetic(now, source, AttemptInput::Executor(fact));
                }
                (InputSelection::Only(source), None)
                    if source == self.sources.actor || source == self.sources.observation =>
                {
                    step = self.synthetic(now, self.sources.actor, AttemptInput::Timer);
                }
                (InputSelection::Prefer(source), None) if source == self.sources.native => {
                    return effects;
                }
                other => panic!("unexpected original observation continuation: {other:?}"),
            }
        }
        panic!("the original observation round did not finish");
    }

    fn input(
        &mut self,
        now: MonotonicInstant,
        envelope: InputEnvelope,
        input: AttemptInput,
    ) -> Vec<EffectBody> {
        let step = self.handle(
            now,
            AttemptSourceId(positive(envelope.source.get())),
            AttemptInputId(positive(envelope.input.get())),
            input,
        );
        self.finish_round(now, step)
    }

    fn timer(&mut self, now: MonotonicInstant) -> Vec<EffectBody> {
        let step = self.synthetic(now, self.sources.actor, AttemptInput::Timer);
        self.finish_round(now, step)
    }
}

struct Run {
    fixture: Fixture,
    driver: Driver,
    root: TaskIdentity,
    history: HistoryCapture,
    capture: Capture,
    consumed: Vec<InputId>,
}

impl Run {
    fn new(byte_frames: usize) -> Self {
        let (driver, key, root) = Driver::bootstrap();
        let history = HistoryCapture::new(4096);
        let capture = Capture::default();
        let fixture = Fixture::make_with_trace(
            4,
            byte_frames,
            true,
            Some(Trace {
                key,
                history: history.clone(),
                capture: capture.clone(),
            }),
        );
        Self {
            fixture,
            driver,
            root,
            history,
            capture,
            consumed: Vec::new(),
        }
    }

    fn accept(&self, ms: u64, fact: ObservationFact) {
        let f = &self.fixture;
        f.clock.at(ms);
        let credit = f
            .bank
            .credit(false, Some(f.key))
            .expect("original ordered input credit");
        f.input
            .accept(
                f.key,
                ObservationEvent::Fact {
                    source_revision: None,
                    fact,
                },
                0,
                credit,
            )
            .unwrap();
    }

    fn ordinary(&mut self, ms: u64, retire: bool) -> InputId {
        let status = TaskStatus::try_new(
            self.root,
            TaskStatusVersion::new(ms).unwrap(),
            TaskState::Planned,
            None,
            TaskOutputFacts::default(),
        )
        .unwrap();
        self.accept(ms, ObservationFact::Status(status));
        self.ordered(retire)
    }

    fn ordered(&mut self, retire: bool) -> InputId {
        let f = &self.fixture;
        let id = f
            .input
            .0
            .state
            .lock()
            .unwrap()
            .in_flight
            .as_ref()
            .unwrap()
            .input;
        assert!(f.runtime.poll_one(Tick(1)).unwrap());
        let (envelope, input) = self.capture.lock().unwrap().take().unwrap();
        assert_eq!(
            (envelope.input, envelope.class),
            (id, InputClass::Lifecycle)
        );
        assert!(matches!(
            &input,
            AttemptInput::Executor(ExecutorFact::Observations(_))
        ));
        assert!(self.driver.input(f.clock.now(), envelope, input).is_empty());
        self.consumed.push(id);
        if retire {
            self.retire_ordered(id);
        }
        id
    }

    fn retire_ordered(&self, id: InputId) {
        let f = &self.fixture;
        f.runtime.retire_input(actor(), id).unwrap();
        f.input.input_retired(id).unwrap();
    }

    fn timer_confirmation(&mut self, ms: u64) -> AttemptRequestToken {
        assert_eq!(self.driver.core.next_deadline(), Some(at(ms)));
        let effects = self.driver.timer(at(ms));
        self.confirmation_request(&effects, ms)
    }

    fn confirmation_request(&self, effects: &[EffectBody], ms: u64) -> AttemptRequestToken {
        let [
            EffectBody::ConfirmReadState {
                participation,
                generation,
                token,
                requested_at,
            },
        ] = effects
        else {
            panic!("expected only the Core's own read confirmation: {effects:?}");
        };
        assert_eq!(
            (*participation, *generation, *requested_at),
            (
                self.fixture.key.participation,
                self.fixture.key.generation,
                at(ms)
            )
        );
        *token
    }

    fn snapshot(
        &mut self,
        token: AttemptRequestToken,
        requested: u64,
        sampled: u64,
    ) -> (InputEnvelope, ReadStateSnapshot) {
        let f = &self.fixture;
        f.clock.at(sampled);
        let mut result = f.confirmation();
        let mut confirming = Box::pin(f.input.confirm(f.key, token, at(requested), &mut result));
        let Poll::Ready(Ok(id)) = confirming
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("complete Source cut must use its independent paid result position");
        };
        drop(confirming);
        assert!(f.runtime.poll_one(Tick(1)).unwrap());
        let (envelope, input) = self.capture.lock().unwrap().take().unwrap();
        assert_eq!(
            (envelope.input, envelope.class),
            (id, InputClass::Lifecycle)
        );
        let AttemptInput::Executor(ExecutorFact::ReadState(snapshot)) = input else {
            panic!("expected the original confirmation receipt");
        };
        assert_eq!(
            (snapshot.request, snapshot.generation, snapshot.snapshot_at),
            (token, f.key.generation, at(sampled))
        );
        assert_eq!(
            snapshot.received_through_snapshot,
            ReceptionCoverage::Complete
        );
        f.runtime.retire_input(actor(), id).unwrap();
        result.close_after_exit().unwrap();
        self.consumed.push(id);
        (envelope, snapshot)
    }

    fn apply_snapshot(
        &mut self,
        delivered: u64,
        (envelope, snapshot): (InputEnvelope, ReadStateSnapshot),
    ) -> Vec<EffectBody> {
        self.driver.input(
            at(delivered),
            envelope,
            AttemptInput::Executor(ExecutorFact::ReadState(snapshot)),
        )
    }

    fn finish(self) {
        self.fixture.finish();
        assert!(self.capture.lock().unwrap().is_none());
        let history = self.history.drain(4096);
        assert!(
            history.complete && history.remaining == 0,
            "{:?}",
            history.error
        );
        let actual = history
            .entries
            .iter()
            .filter_map(|entry| match entry.fact {
                HistoryFact::InputConsumed { input, .. } => Some(input),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, self.consumed);
    }
}

#[test]
fn full_ordered_input_confirms_the_same_cut_and_excludes_actual_local_pause() {
    let mut run = Run::new(1);
    run.ordinary(1_000, true);
    let held = run.ordinary(4_000, false);
    run.fixture.clock.at(4_900);
    let ingress = run.fixture.input.clone();
    let mut read = Box::pin(ingress.reserve_read(run.fixture.key));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(read.as_mut().poll(&mut cx).is_pending());

    let request = run.timer_confirmation(5_000);
    let sampled = run.snapshot(request, 5_000, 6_000);
    assert_eq!(
        sampled.1.pauses,
        ReadPauseCoverage {
            from: at(0),
            through: at(6_000),
            paused: Duration::from_millis(1_100),
        }
    );
    assert!(sampled.1.reading_paused);
    assert_eq!(sampled.1.resume_revision, 0);
    assert!(!sampled.1.had_liveness);
    assert!(
        run.fixture
            .bank
            .credit(false, Some(run.fixture.key))
            .is_none()
    );

    run.fixture.clock.at(7_000);
    run.retire_ordered(held);
    let Poll::Ready(Ok(credit)) = read.as_mut().poll(&mut cx) else {
        panic!("the same original reader must resume when its ordered position retires");
    };
    drop(read);
    drop(ingress);
    drop(credit);
    // The physical resume happened after the snapshot. Its real Ordinary
    // notification has not been delivered to the Core at the snapshot cut.
    assert!(run.apply_snapshot(7_000, sampled).is_empty());
    assert_eq!(run.driver.core.next_deadline(), Some(at(11_000)));
    run.fixture.drain_progress();
    let (envelope, progress) = run.capture.lock().unwrap().take().unwrap();
    assert_eq!(envelope.class, InputClass::Ordinary);
    let AttemptInput::Executor(ExecutorFact::ReadProgress(progress)) = progress else {
        panic!("expected the real read-resume fact");
    };
    assert_eq!(
        (
            progress.generation,
            progress.resumed_at,
            progress.resume_revision
        ),
        (run.fixture.key.generation, at(7_000), positive(1))
    );
    run.consumed.push(envelope.input);
    let effects = run.driver.input(
        at(7_000),
        envelope,
        AttemptInput::Executor(ExecutorFact::ReadProgress(progress)),
    );
    let request = run.confirmation_request(&effects, 7_000);
    let resumed = run.snapshot(request, 7_000, 7_000);
    assert!(!resumed.1.reading_paused);
    assert_eq!(resumed.1.pauses.through, at(7_000));
    assert_eq!(resumed.1.pauses.paused, Duration::from_millis(2_100));
    assert!(run.apply_snapshot(7_000, resumed).is_empty());
    assert_eq!(run.driver.core.next_deadline(), Some(at(7_100)));
    run.finish();
}

#[test]
fn continuous_ordinary_ordered_frames_without_pause_do_not_renew_liveness() {
    let mut run = Run::new(1);
    for ms in [1_000, 2_500, 4_000, 4_999] {
        run.ordinary(ms, true);
    }
    let request = run.timer_confirmation(5_000);
    let sampled = run.snapshot(request, 5_000, 5_000);
    assert!(!sampled.1.had_liveness);
    assert!(!sampled.1.reading_paused);
    assert_eq!(sampled.1.last_liveness_at, at(0));
    assert_eq!(sampled.1.pauses.paused, Duration::ZERO);
    assert!(matches!(
        run.apply_snapshot(5_000, sampled).as_slice(),
        [EffectBody::CloseSubscription { participation, generation }]
            if (*participation, *generation) == (run.fixture.key.participation, run.fixture.key.generation)
    ));
    run.finish();
}

#[test]
fn paid_confirmation_observes_a_bookmark_still_behind_the_ordered_input() {
    let mut run = Run::new(2);
    run.ordinary(1_000, true);
    let held = run.ordinary(4_999, false);
    let request = run.timer_confirmation(5_000);
    run.accept(
        5_200,
        ObservationFact::Bookmark {
            generation: run.fixture.key.generation.get(),
            sequence: 1,
            covered_prefix: 0,
            source_cut: 0,
        },
    );
    let sampled = run.snapshot(request, 5_000, 5_500);
    assert!(sampled.1.had_liveness);
    assert_eq!(sampled.1.last_liveness_at, at(5_200));
    assert_eq!(sampled.1.pauses.from, at(5_200));
    assert_eq!(sampled.1.pauses.through, at(5_500));
    assert_eq!(sampled.1.pauses.paused, Duration::ZERO);
    assert!(run.apply_snapshot(5_500, sampled).is_empty());
    assert_eq!(run.driver.core.next_deadline(), Some(at(10_200)));

    run.retire_ordered(held);
    run.ordered(true);
    assert_eq!(run.driver.core.next_deadline(), Some(at(10_200)));
    run.finish();
}
