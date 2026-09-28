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

//! Control for one typed scan's speculative successor window. The flow lock
//! never encloses a network wait or a stream poll; timeout reclaim issues
//! nonblocking control requests under it to preserve generation ordering.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::time::Instant;

use novarocks_spi::connector::read_stack::ConnectorPreparationControl;

use crate::ScanPreparationConfig;

pub(super) struct StreamPreparationFlow {
    config: ScanPreparationConfig,
    timer: Arc<ScanPreparationTimer>,
    state: Mutex<FlowState>,
}

/// One BE-local timer worker schedules all scan pause deadlines. Short pauses
/// create a queued deadline, never an OS thread per buffered chunk.
pub struct ScanPreparationTimer {
    sender: mpsc::Sender<TimerEntry>,
    next_sequence: AtomicU64,
}

struct TimerEntry {
    deadline: Instant,
    sequence: u64,
    pause_epoch: u64,
    flow: Weak<StreamPreparationFlow>,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.sequence == other.sequence
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.deadline, self.sequence).cmp(&(other.deadline, other.sequence))
    }
}

impl ScanPreparationTimer {
    pub fn new() -> Arc<Self> {
        let (sender, receiver) = mpsc::channel();
        std::thread::Builder::new()
            .name("nova-scan-pause-timer".to_string())
            .spawn(move || run_timer(receiver))
            .expect("start backend scan preparation timer");
        Arc::new(Self {
            sender,
            next_sequence: AtomicU64::new(0),
        })
    }

    fn schedule(&self, flow: &Arc<StreamPreparationFlow>, pause_epoch: u64, deadline: Instant) {
        self.sender
            .send(TimerEntry {
                deadline,
                sequence: self.next_sequence.fetch_add(1, AtomicOrdering::Relaxed),
                pause_epoch,
                flow: Arc::downgrade(flow),
            })
            .expect("backend scan preparation timer is running");
    }
}

fn run_timer(receiver: mpsc::Receiver<TimerEntry>) {
    let mut deadlines = BinaryHeap::<Reverse<TimerEntry>>::new();
    loop {
        let next_wait = deadlines
            .peek()
            .map(|entry| entry.0.deadline.saturating_duration_since(Instant::now()));
        let received = match next_wait {
            Some(wait) => receiver.recv_timeout(wait),
            None => match receiver.recv() {
                Ok(entry) => {
                    deadlines.push(Reverse(entry));
                    continue;
                }
                Err(_) => return,
            },
        };
        match received {
            Ok(entry) => deadlines.push(Reverse(entry)),
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        let now = Instant::now();
        while deadlines
            .peek()
            .is_some_and(|entry| entry.0.deadline <= now)
        {
            let entry = deadlines.pop().expect("due deadline").0;
            if let Some(flow) = entry.flow.upgrade() {
                flow.expire_pause(entry.pause_epoch, now);
            }
        }
    }
}

struct FlowState {
    closed: bool,
    paused_since: Option<Instant>,
    pause_epoch: u64,
    control_epoch: u64,
    reclaimed_pause_epoch: Option<u64>,
    rearm_required: bool,
    rearm_eligible: bool,
    recovery_started: Option<Instant>,
    last_progress_bucket: Option<u128>,
    consecutive_progress_buckets: u128,
    next_control_id: u64,
    controls: BTreeMap<u64, Arc<dyn ConnectorPreparationControl>>,
    retired: BTreeSet<u64>,
}

impl StreamPreparationFlow {
    pub(super) fn new(
        config: ScanPreparationConfig,
        timer: Arc<ScanPreparationTimer>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            timer,
            state: Mutex::new(FlowState {
                closed: false,
                paused_since: None,
                pause_epoch: 0,
                control_epoch: 0,
                reclaimed_pause_epoch: None,
                rearm_required: false,
                rearm_eligible: false,
                recovery_started: None,
                last_progress_bucket: None,
                consecutive_progress_buckets: 0,
                next_control_id: 0,
                controls: BTreeMap::new(),
                retired: BTreeSet::new(),
            }),
        })
    }

    /// Registers a candidate's control. A flow that already stopped stops
    /// the candidate at once; its drain is observed by [`Self::drained`].
    pub(super) fn register(&self, control: Arc<dyn ConnectorPreparationControl>) -> u64 {
        let id = {
            let mut state = self.state.lock().expect("typed preparation flow lock");
            let id = state.next_control_id;
            state.next_control_id = state.next_control_id.wrapping_add(1);
            state.controls.insert(id, Arc::clone(&control));
            state.control_epoch = state.control_epoch.wrapping_add(1);
            id
        };
        self.sync_control(&control);
        id
    }

    pub(super) fn unregister(&self, id: u64) {
        let mut state = self.state.lock().expect("typed preparation flow lock");
        state.controls.remove(&id);
        state.retired.remove(&id);
        state.control_epoch = state.control_epoch.wrapping_add(1);
    }

    /// A promoted candidate no longer owns its ready backing, but its old
    /// speculative operation keeps its B/N occupancy until actual exit.
    pub(super) fn retire(&self, id: u64) {
        self.state
            .lock()
            .expect("typed preparation flow lock")
            .retired
            .insert(id);
        self.reap_retired();
    }

    pub(super) fn retired_count(&self) -> usize {
        self.reap_retired();
        self.state
            .lock()
            .expect("typed preparation flow lock")
            .retired
            .len()
    }

    fn reap_retired(&self) {
        let retired = {
            let state = self.state.lock().expect("typed preparation flow lock");
            state
                .retired
                .iter()
                .filter_map(|id| {
                    state
                        .controls
                        .get(id)
                        .map(|control| (*id, Arc::clone(control)))
                })
                .collect::<Vec<_>>()
        };
        let drained = retired
            .into_iter()
            .filter_map(|(id, control)| control.is_drained().then_some(id))
            .collect::<Vec<_>>();
        let mut state = self.state.lock().expect("typed preparation flow lock");
        for id in drained {
            state.retired.remove(&id);
            state.controls.remove(&id);
            state.control_epoch = state.control_epoch.wrapping_add(1);
        }
    }

    pub(super) fn retained_input_bytes(&self) -> u64 {
        self.reap_retired();
        let controls = self.controls();
        controls.iter().fold(0_u64, |sum, control| {
            sum.saturating_add(control.retained_input_bytes())
        })
    }

    pub(super) fn may_prepare(&self) -> bool {
        self.reap_retired();
        let (epoch, controls) = {
            let state = self.state.lock().expect("typed preparation flow lock");
            if state.closed || state.paused_since.is_some() {
                return false;
            }
            if !state.rearm_required {
                return true;
            }
            if !state.rearm_eligible {
                return false;
            }
            (
                state.control_epoch,
                state.controls.values().cloned().collect::<Vec<_>>(),
            )
        };
        if controls.iter().any(|control| !control.is_drained()) {
            return false;
        }
        let mut state = self.state.lock().expect("typed preparation flow lock");
        if state.closed || state.paused_since.is_some() || state.control_epoch != epoch {
            return false;
        }
        state.rearm_required = false;
        state.rearm_eligible = false;
        state.recovery_started = None;
        state.last_progress_bucket = None;
        state.consecutive_progress_buckets = 0;
        state.control_epoch = state.control_epoch.wrapping_add(1);
        drop(state);
        for control in &controls {
            self.sync_control(control);
        }
        true
    }

    pub(super) fn on_backpressure(self: &Arc<Self>, paused: bool) {
        let now = Instant::now();
        if !paused {
            // A delayed timer must not turn an already-long pause into a
            // short one merely because the consumer resumed first.
            let expired_epoch = {
                let state = self.state.lock().expect("typed preparation flow lock");
                state.paused_since.and_then(|since| {
                    (now.duration_since(since) >= self.config.pause_release)
                        .then_some(state.pause_epoch)
                })
            };
            if let Some(epoch) = expired_epoch {
                self.expire_pause(epoch, now);
            }
        }
        let (epoch, controls) = {
            let mut state = self.state.lock().expect("typed preparation flow lock");
            if state.closed {
                return;
            }
            if paused {
                if state.paused_since.is_some() {
                    return;
                }
                state.paused_since = Some(now);
            } else {
                if state.paused_since.is_none() {
                    return;
                }
                state.paused_since = None;
                if state.rearm_required && state.recovery_started.is_none() {
                    state.recovery_started = Some(now);
                }
            }
            state.pause_epoch = state.pause_epoch.wrapping_add(1);
            state.control_epoch = state.control_epoch.wrapping_add(1);
            (
                state.pause_epoch,
                state.controls.values().cloned().collect::<Vec<_>>(),
            )
        };
        for control in controls {
            self.sync_control(&control);
        }
        if paused {
            self.timer
                .schedule(self, epoch, now + self.config.pause_release);
        }
    }

    fn expire_pause(&self, epoch: u64, now: Instant) {
        let mut state = self.state.lock().expect("typed preparation flow lock");
        if state.closed
            || state.pause_epoch != epoch
            || state.reclaimed_pause_epoch == Some(epoch)
            || !state
                .paused_since
                .is_some_and(|since| now.duration_since(since) >= self.config.pause_release)
        {
            return;
        }
        state.rearm_required = true;
        state.reclaimed_pause_epoch = Some(epoch);
        state.control_epoch = state.control_epoch.wrapping_add(1);
        state.rearm_eligible = false;
        state.recovery_started = None;
        state.last_progress_bucket = None;
        state.consecutive_progress_buckets = 0;
        // The timeout owns this generation's reclaim verdict. These control
        // requests are nonblocking; keeping the small flow lock prevents a
        // concurrent resume or rearm from skipping or overtaking reclaim.
        for control in state.controls.values() {
            control.request_reclaim();
        }
    }

    pub(super) fn on_nonempty_chunk_consumed(&self) {
        self.record_consumption(Instant::now());
    }

    fn record_consumption(&self, now: Instant) {
        let mut state = self.state.lock().expect("typed preparation flow lock");
        // A one-slot scan consumes the chunk while its output buffer still
        // reports backpressure. That real consumption is recovery progress;
        // only the long-pause expiry resets the recovery observation.
        if state.closed || !state.rearm_required {
            return;
        }
        let Some(started) = state.recovery_started else {
            return;
        };
        let bucket =
            now.duration_since(started).as_nanos() / self.config.progress_bucket.as_nanos();
        match state.last_progress_bucket {
            Some(last) if last == bucket => return,
            Some(last) if last.checked_add(1) == Some(bucket) => {
                state.consecutive_progress_buckets += 1;
            }
            _ => state.consecutive_progress_buckets = 1,
        }
        state.last_progress_bucket = Some(bucket);
        let required = self.config.rearm.as_nanos() / self.config.progress_bucket.as_nanos();
        if state.consecutive_progress_buckets >= required
            && now.duration_since(started) >= self.config.rearm
        {
            state.rearm_eligible = true;
        }
    }

    /// Stops every candidate, now and whatever registers later, without
    /// waiting: [`Self::drained`] observes their exit.
    pub(super) fn stop(&self) {
        let controls = {
            let mut state = self.state.lock().expect("typed preparation flow lock");
            state.closed = true;
            state.pause_epoch = state.pause_epoch.wrapping_add(1);
            state.control_epoch = state.control_epoch.wrapping_add(1);
            state.controls.values().cloned().collect::<Vec<_>>()
        };
        for control in &controls {
            self.sync_control(control);
        }
    }

    /// Resolves once every candidate registered now has drained.
    pub(super) fn drained(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let controls = self.controls();
        async move {
            for control in controls {
                control.wait_drained().await;
            }
        }
    }

    fn controls(&self) -> Vec<Arc<dyn ConnectorPreparationControl>> {
        self.state
            .lock()
            .expect("typed preparation flow lock")
            .controls
            .values()
            .cloned()
            .collect()
    }

    fn sync_control(&self, control: &Arc<dyn ConnectorPreparationControl>) {
        loop {
            let (epoch, closed, paused, reclaimed) = {
                let state = self.state.lock().expect("typed preparation flow lock");
                (
                    state.control_epoch,
                    state.closed,
                    state.paused_since.is_some(),
                    state.rearm_required,
                )
            };
            if closed {
                control.request_stop();
            } else if paused && reclaimed {
                control.request_reclaim();
            } else if paused || reclaimed {
                control.request_pause();
            } else {
                control.request_resume();
            }
            if self
                .state
                .lock()
                .expect("typed preparation flow lock")
                .control_epoch
                == epoch
            {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, ready};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[derive(Default)]
    struct ProbeControl {
        pauses: AtomicUsize,
        resumes: AtomicUsize,
        reclaims: AtomicUsize,
        stops: AtomicUsize,
        drained: AtomicBool,
    }

    impl ConnectorPreparationControl for ProbeControl {
        fn request_pause(&self) {
            self.pauses.fetch_add(1, Ordering::AcqRel);
        }
        fn request_resume(&self) {
            self.resumes.fetch_add(1, Ordering::AcqRel);
        }
        fn request_reclaim(&self) {
            self.reclaims.fetch_add(1, Ordering::AcqRel);
        }
        fn request_stop(&self) {
            self.stops.fetch_add(1, Ordering::AcqRel);
            self.drained.store(true, Ordering::Release);
        }
        fn retained_input_bytes(&self) -> u64 {
            8
        }
        fn is_drained(&self) -> bool {
            self.drained.load(Ordering::Acquire)
        }
        fn wait_drained(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(ready(()))
        }
    }

    fn config() -> ScanPreparationConfig {
        ScanPreparationConfig::try_new(
            64,
            2,
            Duration::from_secs(60),
            Duration::from_millis(300),
            Duration::from_millis(100),
        )
        .expect("valid test preparation configuration")
    }

    #[test]
    fn short_pause_retains_input_and_resume_keeps_generation() {
        let flow = StreamPreparationFlow::new(config(), ScanPreparationTimer::new());
        let control = Arc::new(ProbeControl::default());
        flow.register(control.clone());
        flow.on_backpressure(true);
        let epoch = flow.state.lock().expect("flow").pause_epoch;
        flow.on_backpressure(true);
        assert_eq!(flow.state.lock().expect("flow").pause_epoch, epoch);
        assert!(!flow.may_prepare());
        flow.on_backpressure(false);
        flow.expire_pause(epoch, Instant::now() + Duration::from_secs(61));
        assert!(flow.may_prepare());
        assert_eq!(flow.retained_input_bytes(), 8);
        assert_eq!(control.pauses.load(Ordering::Acquire), 1);
        assert_eq!(control.resumes.load(Ordering::Acquire), 2);
        assert_eq!(control.reclaims.load(Ordering::Acquire), 0);
    }

    #[test]
    fn long_pause_reclaims_once_and_requires_bucketed_consumption_and_drain() {
        let flow = StreamPreparationFlow::new(config(), ScanPreparationTimer::new());
        let control = Arc::new(ProbeControl::default());
        flow.register(control.clone());
        flow.on_backpressure(true);
        let (epoch, since) = {
            let state = flow.state.lock().expect("flow");
            (state.pause_epoch, state.paused_since.expect("paused"))
        };
        flow.expire_pause(epoch, since + Duration::from_secs(60));
        flow.expire_pause(epoch, since + Duration::from_secs(61));
        assert_eq!(control.reclaims.load(Ordering::Acquire), 1);
        flow.on_backpressure(false);
        assert_eq!(control.resumes.load(Ordering::Acquire), 1);
        let started = flow
            .state
            .lock()
            .expect("flow")
            .recovery_started
            .expect("recovery");
        for ms in [10, 110, 210, 310] {
            flow.record_consumption(started + Duration::from_millis(ms));
        }
        assert!(!flow.may_prepare(), "retiring input has not drained");
        control.drained.store(true, Ordering::Release);
        assert!(flow.may_prepare());
        assert_eq!(control.resumes.load(Ordering::Acquire), 2);
    }

    #[test]
    fn missing_progress_bucket_resets_rearm_observation() {
        let flow = StreamPreparationFlow::new(config(), ScanPreparationTimer::new());
        flow.on_backpressure(true);
        let (epoch, since) = {
            let state = flow.state.lock().expect("flow");
            (state.pause_epoch, state.paused_since.expect("paused"))
        };
        flow.expire_pause(epoch, since + Duration::from_secs(60));
        flow.on_backpressure(false);
        let started = flow
            .state
            .lock()
            .expect("flow")
            .recovery_started
            .expect("recovery");
        for ms in [10, 210, 310] {
            flow.record_consumption(started + Duration::from_millis(ms));
        }
        assert!(!flow.may_prepare());
        flow.record_consumption(started + Duration::from_millis(410));
        assert!(flow.may_prepare());
    }

    #[test]
    fn repeated_one_slot_pause_consumption_rearms_after_long_reclaim() {
        let flow = StreamPreparationFlow::new(config(), ScanPreparationTimer::new());
        flow.on_backpressure(true);
        let (epoch, since) = {
            let state = flow.state.lock().expect("flow");
            (state.pause_epoch, state.paused_since.expect("paused"))
        };
        flow.expire_pause(epoch, since + Duration::from_secs(60));
        flow.on_backpressure(false);
        let started = flow
            .state
            .lock()
            .expect("flow")
            .recovery_started
            .expect("recovery");
        for ms in [10, 110, 210, 310] {
            flow.on_backpressure(true);
            flow.record_consumption(started + Duration::from_millis(ms));
            flow.on_backpressure(false);
        }
        assert!(flow.may_prepare());
    }

    #[test]
    fn resume_after_timer_delay_still_reclaims_long_pause() {
        let flow = StreamPreparationFlow::new(config(), ScanPreparationTimer::new());
        let control = Arc::new(ProbeControl::default());
        flow.register(control.clone());
        flow.on_backpressure(true);
        flow.state.lock().expect("flow").paused_since =
            Some(Instant::now() - Duration::from_secs(61));
        flow.on_backpressure(false);
        assert_eq!(control.reclaims.load(Ordering::Acquire), 1);
        assert!(!flow.may_prepare());
    }

    #[test]
    fn promoted_operation_keeps_b_and_n_until_actual_drain() {
        let flow = StreamPreparationFlow::new(config(), ScanPreparationTimer::new());
        let control = Arc::new(ProbeControl::default());
        let id = flow.register(control.clone());
        flow.retire(id);
        assert_eq!(flow.retired_count(), 1);
        assert_eq!(flow.retained_input_bytes(), 8);
        control.drained.store(true, Ordering::Release);
        assert_eq!(flow.retired_count(), 0);
        assert_eq!(flow.retained_input_bytes(), 0);
    }
}
