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
//! Consumes ordered chunks and Unit/End boundaries from the registered receiver.
//! The receiver owns decoding. This source reports actual handoff and waits for
//! receiver progress or Task control without an input idle deadline.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::task::ExecutionResult;

use std::sync::{Arc, Mutex};

use crate::exec::pipeline::binding::ExchangeBinding;
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::pipeline::shape::{Source, SourceKernel};
use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};
use crate::runtime::exchange;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::io::ExchangeReceiverKey;
use novarocks_types::UniqueId;
use tracing::debug;

struct ExchangeReceiverProgressState {
    receiver_observable: Option<std::sync::Weak<Observable>>,
    listeners: Vec<std::sync::Weak<Observable>>,
}

/// Forward the shared receiver's progress to every driver's construction signal.
struct ExchangeReceiverProgress {
    state: Mutex<ExchangeReceiverProgressState>,
}

impl ExchangeReceiverProgress {
    fn new() -> Self {
        Self {
            state: Mutex::new(ExchangeReceiverProgressState {
                receiver_observable: None,
                listeners: Vec::new(),
            }),
        }
    }

    fn register(&self, observable: &Arc<Observable>) {
        self.state
            .lock()
            .expect("exchange receiver progress lock")
            .listeners
            .push(Arc::downgrade(observable));
    }

    fn attach_receiver(self: &Arc<Self>, receiver_observable: &Arc<Observable>) {
        let should_attach = {
            let mut state = self.state.lock().expect("exchange receiver progress lock");
            match state.receiver_observable.as_ref() {
                Some(current) if current.ptr_eq(&Arc::downgrade(receiver_observable)) => false,
                Some(_) => {
                    debug_assert!(false, "exchange source factory changed receiver observable");
                    false
                }
                None => {
                    state.receiver_observable = Some(Arc::downgrade(receiver_observable));
                    true
                }
            }
        };
        if !should_attach {
            return;
        }
        let progress = Arc::downgrade(self);
        receiver_observable.add_observer(Arc::new(move || {
            if let Some(progress) = progress.upgrade() {
                progress.publish();
            }
        }));
    }

    fn publish(&self) {
        let listeners = {
            let mut state = self.state.lock().expect("exchange receiver progress lock");
            let mut listeners = Vec::with_capacity(state.listeners.len());
            state.listeners.retain(|listener| {
                let Some(observable) = listener.upgrade() else {
                    return false;
                };
                listeners.push(observable);
                true
            });
            listeners
        };
        // A packet belongs to the shared receiver. Wake every driver even if
        // one sibling consumes it before the others get scheduled. Invoke
        // callbacks outside the lock because they can reenter scheduling.
        for observable in listeners {
            observable.notify_observers();
        }
    }
}

/// Factory for exchange source operators that fetch and decode remote stream pages.
pub struct ExchangeSourceFactory {
    name: String,
    node: ExchangeSourceIdentity,
    binding: ExchangeBinding,
    receiver_progress: Arc<ExchangeReceiverProgress>,
}

/// The receiver registry owns the exact schema and decoded chunks. A source
/// driver needs only routing facts, never another schema owner.
#[derive(Clone, Copy)]
struct ExchangeSourceIdentity {
    node_id: i32,
}

impl ExchangeSourceFactory {
    /// Build the source for a compiled ExchangeSource (local-compiler
    /// output). Its builder validates the frozen local node's supported shape.
    /// The receiver registry supplies exact typed chunks; no expression arena
    /// or duplicate expected schema is constructed for this driver.
    pub(crate) fn new_compiled(node_id: i32, binding: ExchangeBinding) -> Self {
        Self {
            name: format!("EXCHANGE_SOURCE (id={node_id})"),
            node: ExchangeSourceIdentity { node_id },
            binding,
            receiver_progress: Arc::new(ExchangeReceiverProgress::new()),
        }
    }
}

impl ExchangeSourceFactory {
    // The signal is construction metadata, not a readiness predicate. Tests
    // observe this same signal without polling work.
    pub(super) fn create_source(&self, driver_id: i32) -> (Box<dyn Operator>, Arc<Observable>) {
        let source_observable = Arc::new(Observable::new());
        self.receiver_progress.register(&source_observable);
        let operator: Box<dyn Operator> = Box::new(Source::new(ExchangeSourceOperator {
            name: self.name.clone(),
            node: self.node,
            binding: self.binding.clone(),
            driver_id,
            receiver: None,
            receiver_progress: Arc::clone(&self.receiver_progress),
            source_observable: Arc::clone(&source_observable),
            logged_first_pull: false,
            logged_first_none: false,
            #[cfg(novarocks_dev_controls)]
            last_partial_eos_marker_count: 0,
            receiver_mem_tracker_ready: false,
            receiver_mem_tracker_root: None,
            #[cfg(novarocks_dev_controls)]
            test_observer: None,
        }));
        (operator, source_observable)
    }
}

impl OperatorFactory for ExchangeSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        self.create_source(driver_id).0
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct ExchangeSourceOperator {
    name: String,
    node: ExchangeSourceIdentity,
    binding: ExchangeBinding,
    driver_id: i32,
    receiver: Option<exchange::ExchangeReceiverPin>,
    source_observable: Arc<Observable>,
    receiver_progress: Arc<ExchangeReceiverProgress>,
    logged_first_pull: bool,
    logged_first_none: bool,
    #[cfg(novarocks_dev_controls)]
    last_partial_eos_marker_count: usize,
    receiver_mem_tracker_ready: bool,
    receiver_mem_tracker_root: Option<Arc<crate::runtime::mem_tracker::MemTracker>>,
    #[cfg(novarocks_dev_controls)]
    test_observer: Option<Arc<dyn exchange::TestEventObserver>>,
}

impl ExchangeSourceOperator {
    fn waiting(&self, observed_generation: u64) -> StepResult {
        StepResult::new(
            None,
            NextStep::Wait(WaitFact {
                reason: WaitReason::Input,
                observable: Arc::clone(&self.source_observable),
                observed_generation,
                deadline: None,
            }),
            0,
        )
    }
}

#[derive(Debug)]
enum ExchangeSourceFailure {
    NotPrepared,
}
impl std::fmt::Display for ExchangeSourceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPrepared => f.write_str("exchange source operator not prepared"),
        }
    }
}
impl std::error::Error for ExchangeSourceFailure {}
impl novarocks_failure_contract::FailureProjection for ExchangeSourceFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, reason) = match self {
            Self::NotPrepared => (FailureClass::Invariant, 2),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::EXCHANGE, std::num::NonZeroU16::new(reason).unwrap()),
        )
    }
}

impl OperatorLifecycle for ExchangeSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.receiver.is_some() {
            return Ok(());
        }
        let receiver = self.binding.receiver_port.receiver_pin(
            receiver_key(self.binding.key),
            self.binding.expected_senders,
        )?;
        self.receiver_progress
            .attach_receiver(&receiver.observable());
        self.receiver = Some(receiver);
        debug!(
            "ExchangeSource prepared: task_key={} node_id={} expected_senders={}",
            self.binding.key.task_key_uuid(),
            self.node.node_id,
            self.binding.expected_senders
        );
        Ok(())
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.receiver_mem_tracker_root = state.mem_tracker();
        #[cfg(novarocks_dev_controls)]
        {
            self.test_observer = state.exchange_test_observer();
        }
        Ok(())
    }
}

impl SourceKernel for ExchangeSourceOperator {
    fn poll_output(
        &mut self,
        ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<StepResult> {
        if self.receiver.is_none() {
            return Err(crate::runtime::task::ExecutionFailure::from_domain(
                ExchangeSourceFailure::NotPrepared,
            ));
        }
        if !self.receiver_mem_tracker_ready {
            self.receiver_mem_tracker_ready = true;
            if let Some(root) = self.receiver_mem_tracker_root.as_ref() {
                self.binding
                    .receiver_port
                    .ensure_mem_tracker(receiver_key(self.binding.key), root)?;
            }
        }
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let observed_generation = self.source_observable.generation();
        if !self.logged_first_pull {
            self.logged_first_pull = true;
            debug!(
                "ExchangeSource first pull: node_id={} driver_id={}",
                self.node.node_id, self.driver_id
            );
        }

        let receiver = self.receiver.as_ref().expect("receiver");
        let out = receiver.try_pop_next_with_stats(self.binding.expected_senders)?;

        match out {
            Some(exchange::ExchangePopResult::Chunk(chunk)) => {
                let input_rows = chunk.len();
                if chunk.is_empty() {
                    debug!(
                        "ExchangeSource filtered empty chunk: node_id={} driver_id={} input_rows={}",
                        self.node.node_id, self.driver_id, input_rows
                    );
                    return Ok(StepResult::new(None, NextStep::Runnable, input_rows as u64));
                }
                debug!(
                    "ExchangeSource output chunk: node_id={} driver_id={} input_rows={} output_rows={}",
                    self.node.node_id,
                    self.driver_id,
                    input_rows,
                    chunk.len()
                );
                Ok(StepResult::new(
                    Some(chunk),
                    NextStep::Runnable,
                    input_rows as u64,
                ))
            }
            Some(exchange::ExchangePopResult::Finished(stats)) => {
                receiver.complete_input_consumption(self.binding.expected_senders)?;
                #[cfg(novarocks_dev_controls)]
                if let Some(observer) = self
                    .test_observer
                    .as_ref()
                    .filter(|observer| observer.is_active())
                {
                    let snapshot = self
                        .binding
                        .receiver_port
                        .snapshot(receiver_key(self.binding.key));
                    exchange::emit_exchange_snapshot_marker(Some(observer.as_ref()), || {
                        format!(
                            "event=source_finished task_key={} node_id={} driver_id={} expected_senders={} snapshot={:?} source_generation={}",
                            self.binding.key.task_key_uuid(),
                            self.node.node_id,
                            self.driver_id,
                            self.binding.expected_senders,
                            snapshot,
                            self.source_observable.generation(),
                        )
                    });
                }
                debug!(
                    "ExchangeSource finished: task_key={} node_id={} driver_id={} request_received={} bytes_received={} deserialize_ns={} chunks_received={} rows_received={}",
                    self.binding.key.task_key_uuid(),
                    self.node.node_id,
                    self.driver_id,
                    stats.request_received,
                    stats.bytes_received,
                    stats.deserialize_ns,
                    stats.chunks_received,
                    stats.rows_received
                );
                Ok(StepResult::new(None, NextStep::Finished, 0))
            }
            None => {
                let first_none = !self.logged_first_none;
                if first_none {
                    self.logged_first_none = true;
                }
                #[cfg(novarocks_dev_controls)]
                if let Some(observer) = self
                    .test_observer
                    .as_ref()
                    .filter(|observer| observer.is_active())
                {
                    let snapshot = self
                        .binding
                        .receiver_port
                        .snapshot(receiver_key(self.binding.key));
                    if first_none {
                        let first_snapshot = snapshot.clone();
                        exchange::emit_exchange_snapshot_marker(Some(observer.as_ref()), || {
                            format!(
                                "event=source_first_block task_key={} node_id={} driver_id={} expected_senders={} snapshot={:?} source_generation={}",
                                self.binding.key.task_key_uuid(),
                                self.node.node_id,
                                self.driver_id,
                                self.binding.expected_senders,
                                first_snapshot,
                                self.source_observable.generation(),
                            )
                        });
                    }
                    if let Some(snapshot) = snapshot
                        && snapshot.finished_senders > self.last_partial_eos_marker_count
                        && snapshot.finished_senders < self.binding.expected_senders
                    {
                        self.last_partial_eos_marker_count = snapshot.finished_senders;
                        exchange::emit_exchange_snapshot_marker(Some(observer.as_ref()), || {
                            format!(
                                "event=source_partial_eos_block task_key={} node_id={} driver_id={} expected_senders={} snapshot={:?} source_generation={}",
                                self.binding.key.task_key_uuid(),
                                self.node.node_id,
                                self.driver_id,
                                self.binding.expected_senders,
                                snapshot,
                                self.source_observable.generation(),
                            )
                        });
                    }
                }
                if first_none {
                    debug!(
                        "ExchangeSource no output yet: node_id={} driver_id={}",
                        self.node.node_id, self.driver_id
                    );
                }
                Ok(self.waiting(observed_generation))
            }
        }
    }
}

fn receiver_key(key: exchange::ExchangeKey) -> ExchangeReceiverKey {
    ExchangeReceiverKey {
        task_key: UniqueId::new(key.task_key_hi, key.task_key_lo),
        node_id: key.node_id,
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_support;
