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

#[derive(Debug)]
enum ResultSinkCreditState {
    Ready,
    Blocked,
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
        !self.finished
            && !matches!(
                *self.credit.lock().expect("result sink credit lock"),
                ResultSinkCreditState::Blocked
            )
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
                *state = ResultSinkCreditState::Blocked;
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
                ResultSinkCreditState::Ready | ResultSinkCreditState::Blocked => {
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
    use std::sync::{Arc, Mutex, Weak};

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::exec::chunk::ChunkSchema;
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![7]))],
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
}
