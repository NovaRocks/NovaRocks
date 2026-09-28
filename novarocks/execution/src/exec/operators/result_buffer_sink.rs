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

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::io::{
    FragmentResultSession, ResultWriteAdmission, ResultWriteCredit,
};
use crate::runtime::observable::Observable;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::SlotId;

#[derive(Debug)]
struct ResultBufferSinkShared {
    remaining_drivers: AtomicI32,
}

impl ResultBufferSinkShared {
    fn new() -> Self {
        Self {
            remaining_drivers: AtomicI32::new(-1),
        }
    }

    fn init_driver_count(&self, dop: i32) {
        let dop = dop.max(1);
        let _ =
            self.remaining_drivers
                .compare_exchange(-1, dop, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// Factory for result sinks that stream encoded batches directly into the fetch result buffer.
pub struct ResultBufferSinkFactory {
    name: String,
    session: Arc<dyn FragmentResultSession>,
    shared: Arc<ResultBufferSinkShared>,
}

impl ResultBufferSinkFactory {
    pub fn new(session: Arc<dyn FragmentResultSession>, plan_node_id: Option<i32>) -> Self {
        let plan_node_id = match plan_node_id {
            Some(id) if id >= 0 => id,
            _ => -1,
        };
        Self {
            name: format!("RESULT_BUFFER_SINK (plan_node_id={plan_node_id})"),
            session,
            shared: Arc::new(ResultBufferSinkShared::new()),
        }
    }
}

impl OperatorFactory for ResultBufferSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        self.shared.init_driver_count(dop);
        Box::new(ResultBufferSinkOperator {
            name: self.name.clone(),
            session: Arc::clone(&self.session),
            shared: Arc::clone(&self.shared),
            credit: Mutex::new(ResultSinkCreditState::Ready),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct ResultBufferSinkOperator {
    name: String,
    session: Arc<dyn FragmentResultSession>,
    shared: Arc<ResultBufferSinkShared>,
    credit: Mutex<ResultSinkCreditState>,
    finished: bool,
}

enum ResultSinkCreditState {
    Ready,
    Blocked {
        bytes: usize,
        observable: Arc<Observable>,
        generation: u64,
    },
    Reserved(ResultWriteCredit),
}

impl Operator for ResultBufferSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn cancel(&mut self) {
        *self.credit.lock().expect("result sink credit lock") = ResultSinkCreditState::Ready;
    }
}

impl ProcessorOperator for ResultBufferSinkOperator {
    fn need_input(&self) -> bool {
        if self.finished {
            return false;
        }
        match &*self.credit.lock().expect("result sink credit lock") {
            ResultSinkCreditState::Blocked {
                observable,
                generation,
                ..
            } => self.session.writable_observable().is_none_or(|current| {
                !Arc::ptr_eq(observable, &current) || current.generation() != *generation
            }),
            _ => true,
        }
    }

    fn can_accept_input(&self, chunk: &Chunk) -> Result<bool, String> {
        if self.finished || chunk.is_empty() {
            return Ok(!self.finished);
        }
        let bytes = self
            .session
            .reservation_bytes(chunk)
            .map_err(|error| error.to_string())?;
        let mut state = self.credit.lock().expect("result sink credit lock");
        if let ResultSinkCreditState::Reserved(credit) = &*state {
            if credit.bytes() != bytes {
                return Err(format!(
                    "RESULT_SINK retained credit for {} bytes but next chunk requires {bytes}",
                    credit.bytes()
                ));
            }
            return Ok(true);
        }
        // Freeze the event before the budget check. A release can race with a
        // rejected acquisition and must invalidate that cached rejection even
        // if the scheduler has not frozen its own wait generation yet.
        let observable = self.session.writable_observable();
        if let ResultSinkCreditState::Blocked {
            bytes: blocked_bytes,
            observable: blocked,
            generation,
        } = &*state
        {
            let current = observable.as_ref().ok_or_else(|| {
                "RESULT_SINK blocked credit lost its readiness observable".to_string()
            })?;
            if !Arc::ptr_eq(blocked, current) {
                return Err(
                    "RESULT_SINK blocked credit changed its readiness observable".to_string(),
                );
            }
            if bytes == *blocked_bytes && current.generation() == *generation {
                return Ok(false);
            }
        }
        let generation = observable.as_ref().map(|event| event.generation());
        match self
            .session
            .try_acquire(bytes)
            .map_err(|error| error.to_string())?
        {
            ResultWriteAdmission::Granted(credit) => {
                if credit.bytes() != bytes {
                    return Err(format!(
                        "RESULT_SINK received credit for {} bytes but chunk requires {bytes}",
                        credit.bytes()
                    ));
                }
                *state = ResultSinkCreditState::Reserved(credit);
                Ok(true)
            }
            ResultWriteAdmission::Blocked => {
                let observable = observable.ok_or_else(|| {
                    "RESULT_SINK blocked credit has no readiness observable".to_string()
                })?;
                let current = self.session.writable_observable().ok_or_else(|| {
                    "RESULT_SINK blocked credit lost its readiness observable".to_string()
                })?;
                if !Arc::ptr_eq(&observable, &current) {
                    return Err(
                        "RESULT_SINK blocked credit changed its readiness observable".to_string(),
                    );
                }
                *state = ResultSinkCreditState::Blocked {
                    bytes,
                    observable,
                    generation: generation.expect("blocked credit observable generation"),
                };
                Ok(false)
            }
        }
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished || chunk.is_empty() {
            return Ok(());
        }

        let bytes = self
            .session
            .reservation_bytes(&chunk)
            .map_err(|error| error.to_string())?;
        let credit = {
            let mut state = self.credit.lock().expect("result sink credit lock");
            match std::mem::replace(&mut *state, ResultSinkCreditState::Ready) {
                ResultSinkCreditState::Reserved(credit) if credit.bytes() == bytes => credit,
                ResultSinkCreditState::Reserved(credit) => {
                    return Err(format!(
                        "RESULT_SINK retained credit for {} bytes but pushed chunk requires {bytes}",
                        credit.bytes()
                    ));
                }
                ResultSinkCreditState::Ready | ResultSinkCreditState::Blocked { .. } => {
                    drop(state);
                    match self
                        .session
                        .try_acquire(bytes)
                        .map_err(|error| error.to_string())?
                    {
                        ResultWriteAdmission::Granted(credit) if credit.bytes() == bytes => credit,
                        ResultWriteAdmission::Granted(credit) => {
                            return Err(format!(
                                "RESULT_SINK received credit for {} bytes but chunk requires {bytes}",
                                credit.bytes()
                            ));
                        }
                        ResultWriteAdmission::Blocked => {
                            return Err(
                                "RESULT_SINK push reached a result session without reserved byte credit"
                                    .to_string(),
                            );
                        }
                    }
                }
            }
        };
        self.session
            .write_with_credit(chunk, credit)
            .map_err(|error| error.to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        *self.credit.lock().expect("result sink credit lock") = ResultSinkCreditState::Ready;
        self.finished = true;

        let prev = self.shared.remaining_drivers.fetch_sub(1, Ordering::AcqRel);
        if prev <= 0 {
            return Err("RESULT_SINK driver count underflow".to_string());
        }
        if prev == 1 {
            self.session.finish().map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        self.session.writable_observable()
    }

    fn accepts_encoded_column(&self, _slot_id: SlotId, data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32
                    && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Mutex, Weak};
    use std::time::Duration;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::operator::BlockedReason;
    use crate::runtime::fragment::io::{
        FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, ResultAbort,
    };
    use crate::runtime::query_options::QueryOptions;
    use novarocks_types::SlotId;

    struct CreditSession {
        inner: Arc<CreditSessionInner>,
    }

    struct CreditSessionInner {
        capacity: usize,
        state: Mutex<CreditSessionState>,
        writable: Arc<Observable>,
    }

    struct CreditSessionState {
        retained: usize,
        high_water: usize,
        queue: VecDeque<(Chunk, ResultWriteCredit)>,
    }

    impl CreditSession {
        fn new(capacity: usize) -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(CreditSessionInner {
                    capacity,
                    state: Mutex::new(CreditSessionState {
                        retained: 0,
                        high_water: 0,
                        queue: VecDeque::new(),
                    }),
                    writable: Arc::new(Observable::new()),
                }),
            })
        }

        fn release_credit(owner: Weak<CreditSessionInner>, bytes: usize) {
            let Some(owner) = owner.upgrade() else {
                return;
            };
            {
                let mut state = owner.state.lock().expect("credit session state lock");
                state.retained = state
                    .retained
                    .checked_sub(bytes)
                    .expect("released result bytes were retained");
            }
            owner.writable.notify_observers();
        }

        fn consume_one(&self) {
            let retained = self
                .inner
                .state
                .lock()
                .expect("credit session state lock")
                .queue
                .pop_front();
            drop(retained);
        }

        fn retained(&self) -> usize {
            self.inner
                .state
                .lock()
                .expect("credit session state lock")
                .retained
        }

        fn high_water(&self) -> usize {
            self.inner
                .state
                .lock()
                .expect("credit session state lock")
                .high_water
        }
    }

    impl FragmentResultSession for CreditSession {
        fn reservation_bytes(&self, chunk: &Chunk) -> Result<usize, FragmentIoError> {
            Ok(chunk.logical_bytes())
        }

        fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, FragmentIoError> {
            if bytes > self.inner.capacity {
                return Err(FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::InvalidResponse,
                    format!(
                        "result chunk of {bytes} bytes exceeds capacity {}",
                        self.inner.capacity
                    ),
                ));
            }
            {
                let mut state = self.inner.state.lock().expect("credit session state lock");
                let Some(next) = state.retained.checked_add(bytes) else {
                    return Ok(ResultWriteAdmission::Blocked);
                };
                if next > self.inner.capacity {
                    return Ok(ResultWriteAdmission::Blocked);
                }
                state.retained = next;
                state.high_water = state.high_water.max(next);
            }
            let owner = Arc::downgrade(&self.inner);
            Ok(ResultWriteAdmission::Granted(ResultWriteCredit::new(
                bytes,
                move |released| Self::release_credit(owner.clone(), released),
            )))
        }

        fn writable_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.inner.writable))
        }

        fn write_with_credit(
            &self,
            chunk: Chunk,
            credit: ResultWriteCredit,
        ) -> Result<(), FragmentIoError> {
            assert_eq!(chunk.logical_bytes(), credit.bytes());
            self.inner
                .state
                .lock()
                .expect("credit session state lock")
                .queue
                .push_back((chunk, credit));
            Ok(())
        }

        fn finish(&self) -> Result<(), FragmentIoError> {
            Ok(())
        }

        fn abort(&self, _reason: ResultAbort) {
            let queued = {
                let mut state = self.inner.state.lock().expect("credit session state lock");
                std::mem::take(&mut state.queue)
            };
            drop(queued);
        }
    }

    fn one_row_chunk() -> Chunk {
        values_chunk(vec![7])
    }

    fn values_chunk(values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values))],
        )
        .expect("one row batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("one row chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn runtime_state() -> RuntimeState {
        RuntimeState::new(
            Some(QueryOptions::default()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[test]
    fn byte_credit_blocks_and_consumer_release_wakes_without_overshoot() {
        let first = one_row_chunk();
        let second = one_row_chunk();
        let bytes = first.logical_bytes();
        assert!(bytes > 0);
        let session = CreditSession::new(bytes);
        let factory = ResultBufferSinkFactory::new(session.clone(), None);
        let mut operator = factory.create(1, 0);
        let processor = operator.as_processor_mut().expect("result processor");
        let runtime = runtime_state();

        assert!(processor.can_accept_input(&first).expect("first admission"));
        processor
            .push_chunk(&runtime, first)
            .expect("first result write");
        assert_eq!(session.retained(), bytes);
        assert_eq!(session.high_water(), bytes);

        assert!(
            !processor
                .can_accept_input(&second)
                .expect("blocked admission")
        );
        assert!(!processor.need_input());
        let writable = processor
            .sink_observable()
            .expect("blocked credit has a stable observable");
        let generation = writable.generation();

        session.consume_one();
        assert_eq!(session.retained(), 0);
        assert!(writable.generation() > generation);
        assert!(
            processor
                .can_accept_input(&second)
                .expect("retry admission")
        );
        processor
            .push_chunk(&runtime, second)
            .expect("second result write");
        processor
            .set_finishing(&runtime)
            .expect("result stream finishes");

        assert_eq!(
            session.retained(),
            bytes,
            "stream finish must not return bytes still retained for the consumer"
        );
        assert_eq!(session.high_water(), bytes);
        session.consume_one();
        assert_eq!(session.retained(), 0);
    }

    #[test]
    fn credit_rejection_does_not_block_a_smaller_reservation_without_a_release() {
        let first = one_row_chunk();
        let small = one_row_chunk();
        let large = values_chunk(vec![7, 8]);
        let bytes = first.logical_bytes();
        assert_eq!(large.logical_bytes(), 2 * bytes);
        let session = CreditSession::new(2 * bytes);
        let factory = ResultBufferSinkFactory::new(session.clone(), None);
        let mut operator = factory.create(1, 0);
        let processor = operator.as_processor_mut().expect("result processor");
        assert!(processor.can_accept_input(&first).expect("first admission"));
        processor
            .push_chunk(&runtime_state(), first)
            .expect("write");
        let generation = session.inner.writable.generation();
        assert!(!processor.can_accept_input(&large).expect("large blocked"));
        assert!(processor.can_accept_input(&small).expect("smaller fits"));
        assert_eq!(session.inner.writable.generation(), generation);
        processor
            .push_chunk(&runtime_state(), small)
            .expect("small write");
        assert_eq!(session.high_water(), 2 * bytes);
        session.consume_one();
        session.consume_one();
        assert_eq!(session.retained(), 0);
    }

    struct OneChunkSource(Option<Chunk>);

    impl Operator for OneChunkSource {
        fn name(&self) -> &str {
            "OneChunkSource"
        }
        fn is_finished(&self) -> bool {
            self.0.is_none()
        }
        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for OneChunkSource {
        fn need_input(&self) -> bool {
            false
        }
        fn has_output(&self) -> bool {
            self.0.is_some()
        }
        fn push_chunk(&mut self, _: &RuntimeState, _: Chunk) -> Result<(), String> {
            Err("source cannot accept input".to_string())
        }
        fn pull_chunk(&mut self, _: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(self.0.take())
        }
        fn set_finishing(&mut self, _: &RuntimeState) -> Result<(), String> {
            Ok(())
        }
    }

    struct RacingCreditSession {
        inner: Arc<CreditSession>,
        notify_before_freeze: bool,
        rejected: AtomicBool,
        notified: AtomicBool,
        attempts: AtomicUsize,
        finished: AtomicBool,
    }

    impl RacingCreditSession {
        fn new(bytes: usize, notify_before_freeze: bool) -> Arc<Self> {
            Arc::new(Self {
                inner: CreditSession::new(bytes),
                notify_before_freeze,
                rejected: AtomicBool::new(false),
                notified: AtomicBool::new(false),
                attempts: AtomicUsize::new(0),
                finished: AtomicBool::new(false),
            })
        }
    }

    impl FragmentResultSession for RacingCreditSession {
        fn reservation_bytes(&self, chunk: &Chunk) -> Result<usize, FragmentIoError> {
            self.inner.reservation_bytes(chunk)
        }
        fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, FragmentIoError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if !self.notified.load(Ordering::SeqCst) {
                self.rejected.store(true, Ordering::SeqCst);
                return Ok(ResultWriteAdmission::Blocked);
            }
            self.inner.try_acquire(bytes)
        }
        fn writable_observable(&self) -> Option<Arc<Observable>> {
            let observable = self.inner.writable_observable().expect("credit observable");
            // The sole release occurs after a rejected acquisition but before
            // the driver's terminal-sink wait captures its generation. This
            // also exercises production code that cached an unversioned reject.
            if self.notify_before_freeze
                && self.rejected.load(Ordering::SeqCst)
                && !self.notified.swap(true, Ordering::SeqCst)
            {
                observable.notify_observers();
            }
            Some(observable)
        }
        fn write_with_credit(
            &self,
            chunk: Chunk,
            credit: ResultWriteCredit,
        ) -> Result<(), FragmentIoError> {
            self.inner.write_with_credit(chunk, credit)
        }
        fn finish(&self) -> Result<(), FragmentIoError> {
            self.finished.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn abort(&self, reason: ResultAbort) {
            self.inner.abort(reason);
        }
    }

    fn credit_driver(session: Arc<RacingCreditSession>, chunk: Chunk) -> PipelineDriver {
        let factory = ResultBufferSinkFactory::new(session, None);
        PipelineDriver::new(
            0,
            vec![Box::new(OneChunkSource(Some(chunk))), factory.create(1, 0)],
            None,
            Vec::new(),
            Arc::new(runtime_state()),
            None,
        )
    }

    #[test]
    fn driver_retries_credit_released_before_wait_generation_freeze() {
        let chunk = one_row_chunk();
        let bytes = chunk.logical_bytes();
        let session = RacingCreditSession::new(bytes, true);
        let mut driver = credit_driver(Arc::clone(&session), chunk);
        let state = driver.process(Duration::from_secs(1));
        assert!(matches!(state, DriverState::Finished), "actual: {state:?}");
        assert!(session.finished.load(Ordering::SeqCst));
        assert_eq!(session.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            session.inner.inner.writable.generation(),
            1,
            "only one writable event"
        );
        assert_eq!(session.inner.retained(), bytes);
        assert_eq!(session.inner.high_water(), bytes);
        session.inner.consume_one();
        assert_eq!(session.inner.retained(), 0);
    }

    #[test]
    fn driver_parks_unchanged_credit_rejection_without_retry_spin() {
        let chunk = one_row_chunk();
        let session = RacingCreditSession::new(chunk.logical_bytes(), false);
        let mut driver = credit_driver(Arc::clone(&session), chunk);
        let state = driver.process(Duration::from_secs(1));
        assert!(
            matches!(state, DriverState::Blocked(BlockedReason::OutputFull)),
            "actual: {state:?}"
        );
        assert_eq!(session.attempts.load(Ordering::SeqCst), 1);
        let (observable, generation, _) =
            driver.blocked_observable_snapshot().expect("frozen wait");
        assert_eq!(observable.generation(), generation);
        assert_eq!(session.inner.retained(), 0);
        assert!(!session.finished.load(Ordering::SeqCst));
    }
}
