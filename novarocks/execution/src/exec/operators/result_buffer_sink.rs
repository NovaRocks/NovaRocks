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

#[cfg(test)]
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::OperatorLifecycle;
#[cfg(test)]
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::{AcceptedInput, InputPreparationFailure, RawInput},
    operator::{ConstructionFailure, InputConstructionFactory, InputEncodingPolicy, Operator},
    operator_factory::OperatorFactory,
    shape::{
        HydrateInput, InputPreparation, PreparationStep, ReceiverPreparation, ReceiverSlot,
        StepOperator,
    },
    step::{AdmissionNext, InputResult, NextStep, StepResult, WaitFact, WaitReason},
};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use crate::runtime::{
    mem_tracker::MemTracker,
    task::io::{
        ResultInputMetadata, ResultWriteAdmission, ResultWriteCredit, ResultWriteFailure,
        TaskResultSession,
    },
};
use arrow::datatypes::DataType;
use novarocks_types::SlotId;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

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
    fn init_driver_count(&self, dop: i32) -> bool {
        if dop <= 0 {
            return false;
        }
        match self
            .remaining_drivers
            .compare_exchange(-1, dop, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => true,
            Err(actual) => actual == dop,
        }
    }
}

pub struct ResultBufferSinkFactory {
    name: String,
    session: Arc<dyn TaskResultSession>,
    shared: Arc<ResultBufferSinkShared>,
}
impl ResultBufferSinkFactory {
    pub fn new(session: Arc<dyn TaskResultSession>, plan_node_id: Option<i32>) -> Self {
        let plan_node_id = plan_node_id.filter(|id| *id >= 0).unwrap_or(-1);
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
    fn create(&self, dop: i32, _: i32) -> Box<dyn Operator> {
        Box::new(ResultBufferSinkOperator {
            name: self.name.clone(),
            session: self.session.clone(),
            shared: self.shared.clone(),
            receiver: ReceiverSlot::new(ResultPreparation::default()),
            next: NextStep::NeedInput,
            finishing: false,
            contributed: false,
            construction_factory: None,
            prepared: false,
            valid_dop: self.shared.init_driver_count(dop),
            failure: None,
        })
    }
    fn is_sink(&self) -> bool {
        true
    }
}

struct ResultEncoding;
impl InputEncodingPolicy for ResultEncoding {
    fn retains_encoded_column(&self, _: SlotId, data_type: &DataType) -> bool {
        matches!(data_type, DataType::Dictionary(key, value)
            if key.as_ref() == &DataType::Int32 && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8))
    }
}
#[derive(Default)]
struct ResultPreparation {
    reserved: Option<ResultWriteCredit>,
    blocked: Option<(usize, WaitFact)>,
    hydrate: HydrateInput,
}
impl InputPreparation<ResultEncoding> for ResultPreparation {
    fn admission(
        &mut self,
        _: &mut DriverStep<'_>,
        input: &RawInput,
        _: &ResultEncoding,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        HydrateInput::validate_uncovered(input)?;
        Ok(None)
    }
    fn accept(&mut self, input: RawInput) -> AcceptedInput {
        self.hydrate.accept_raw(input)
    }
    fn prepare(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: AcceptedInput,
        encoding: &ResultEncoding,
    ) -> Result<PreparationStep, InputPreparationFailure> {
        self.hydrate.prepare_accepted(ctx, input, encoding)
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.hydrate.install_tracker(tracker);
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.hydrate.install_factory(factory);
    }
}
impl ResultPreparation {
    fn reserve(
        &mut self,
        input: ResultInputMetadata,
        session: &dyn TaskResultSession,
    ) -> ExecutionResult<Option<WaitFact>> {
        if input.rows() == 0 {
            return Ok(None);
        }
        let bytes = session.reservation_bytes(input)?;
        if let Some((previous, wait)) = &self.blocked {
            let current = session
                .writable_observable()
                .ok_or_else(|| fault(ResultReceiverFailure::NoReadiness))?;
            if !Arc::ptr_eq(&current, &wait.observable) {
                return Err(fault(ResultReceiverFailure::ChangedReadiness));
            }
            if *previous == bytes && !wait.is_ready() {
                return Ok(Some(wait.clone()));
            }
        }
        let observable = session.writable_observable();
        let generation = observable.as_ref().map(|event| event.generation());
        match session.try_acquire(bytes)? {
            ResultWriteAdmission::Granted(credit) => {
                if credit.bytes() != bytes {
                    return Err(fault(ResultReceiverFailure::WrongCredit {
                        expected: bytes,
                        actual: credit.bytes(),
                    }));
                }
                self.blocked = None;
                self.reserved = Some(credit);
                Ok(None)
            }
            ResultWriteAdmission::Blocked => {
                let observable =
                    observable.ok_or_else(|| fault(ResultReceiverFailure::NoReadiness))?;
                let current = session
                    .writable_observable()
                    .ok_or_else(|| fault(ResultReceiverFailure::NoReadiness))?;
                if !Arc::ptr_eq(&current, &observable) {
                    return Err(fault(ResultReceiverFailure::ChangedReadiness));
                }
                let wait = WaitFact {
                    reason: WaitReason::Output,
                    observable,
                    observed_generation: generation.expect("observed before acquisition"),
                    deadline: None,
                };
                self.blocked = Some((bytes, wait.clone()));
                Ok(Some(wait))
            }
        }
    }
}
struct ResultBufferSinkOperator {
    // Accepted storage exits before its reserved credit and private bindings.
    receiver: ReceiverSlot<ResultPreparation>,
    name: String,
    session: Arc<dyn TaskResultSession>,
    shared: Arc<ResultBufferSinkShared>,
    construction_factory: Option<Arc<dyn InputConstructionFactory>>,
    next: NextStep,
    failure: Option<ExecutionFailure>,
    prepared: bool,
    valid_dop: bool,
    finishing: bool,
    contributed: bool,
}
#[derive(Debug)]
enum ResultReceiverFailure {
    Closed,
    MissingCredit,
    WrongCredit { expected: usize, actual: usize },
    NoReadiness,
    ChangedReadiness,
    DriverUnderflow,
    InvalidDop,
}
impl std::fmt::Display for ResultReceiverFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("result input reached a closed receiver"),
            Self::MissingCredit => f.write_str("result input has no reserved byte credit"),
            Self::WrongCredit { expected, actual } => write!(
                f,
                "result input requires {expected} bytes but owns {actual} bytes of credit"
            ),
            Self::NoReadiness => f.write_str("blocked result credit has no readiness observable"),
            Self::ChangedReadiness => {
                f.write_str("result credit changed its installed readiness observable")
            }
            Self::DriverUnderflow => f.write_str("result driver finish count underflow"),
            Self::InvalidDop => f.write_str("result factory and prepared driver DOP disagree"),
        }
    }
}
impl std::error::Error for ResultReceiverFailure {}
impl novarocks_failure_contract::FailureProjection for ResultReceiverFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let code = match self {
            Self::Closed => 80,
            Self::MissingCredit => 81,
            Self::WrongCredit { .. } => 82,
            Self::NoReadiness => 83,
            Self::ChangedReadiness => 84,
            Self::DriverUnderflow => 85,
            Self::InvalidDop => 86,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::RESULT, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
fn fault(reason: ResultReceiverFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(reason)
}
impl ResultBufferSinkOperator {
    fn prepared_factory(&self) -> ExecutionResult<&dyn InputConstructionFactory> {
        if !self.prepared {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        self.construction_factory
            .as_deref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }
    fn fail(&mut self, ctx: &DriverStep<'_>, error: ExecutionFailure) -> ExecutionFailure {
        ctx.report_failure(error.clone());
        self.failure.get_or_insert(error).clone()
    }
    fn discard(&mut self) {
        self.receiver.discard();
        let preparation = self.receiver.preparation_mut();
        drop(preparation.reserved.take());
        preparation.blocked = None;
    }
    fn advance(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        match self.receiver.prepare(ctx, &ResultEncoding)? {
            ReceiverPreparation::Pending(next) => return Ok(StepResult::new(None, next, 0)),
            ReceiverPreparation::Ready(prepared) => {
                let rows = prepared.chunk().len() as u64;
                if rows > 0 {
                    let bytes = self
                        .session
                        .reservation_bytes(prepared.chunk().into())
                        .map_err(|error| {
                            let failure: ExecutionFailure = error.into();
                            ctx.report_failure(failure.clone());
                            failure
                        })?;
                    let credit =
                        self.receiver
                            .preparation_mut()
                            .reserved
                            .take()
                            .ok_or_else(|| {
                                let failure = fault(ResultReceiverFailure::MissingCredit);
                                ctx.report_failure(failure.clone());
                                failure
                            })?;
                    if credit.bytes() != bytes {
                        let error = fault(ResultReceiverFailure::WrongCredit {
                            expected: bytes,
                            actual: credit.bytes(),
                        });
                        ctx.report_failure(error.clone());
                        drop(prepared);
                        drop(credit);
                        return Err(error);
                    }
                    let control = ctx.kernel_control();
                    self.session
                        .write_with_credit(
                            prepared.into_uncovered_chunk()?,
                            credit,
                            self.prepared_factory()?,
                            &control,
                        )
                        .map_err(ResultWriteFailure::into_execution)?;
                }
                let next = if self.finishing {
                    NextStep::Runnable
                } else {
                    NextStep::NeedInput
                };
                return Ok(StepResult::new(None, next, rows));
            }
            ReceiverPreparation::Empty => {}
        }
        if self.finishing {
            if !self.contributed {
                let previous = self
                    .shared
                    .remaining_drivers
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |previous| {
                        previous.checked_sub(1).filter(|next| *next >= 0)
                    })
                    .map_err(|_| fault(ResultReceiverFailure::DriverUnderflow))?;
                self.contributed = true;
                if previous == 1 {
                    self.session.finish()?;
                }
            }
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        Ok(StepResult::new(None, NextStep::NeedInput, 0))
    }
}
impl OperatorLifecycle for ResultBufferSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.receiver
            .preparation_mut()
            .hydrate
            .install_tracker(tracker);
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.receiver
            .preparation_mut()
            .hydrate
            .install_factory(factory.clone());
        self.construction_factory = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        let result = if !self.valid_dop {
            Err(fault(ResultReceiverFailure::InvalidDop))
        } else {
            self.construction_factory
                .as_ref()
                .map(|_| ())
                .ok_or_else(|| ConstructionFailure::MissingOwner.into())
        };
        if let Err(failure) = &result {
            self.failure = Some(failure.clone());
        } else {
            self.prepared = true;
        }
        result
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.discard();
        self.next = NextStep::Finished;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.discard();
        self.next = NextStep::Finished;
    }
}
impl StepOperator for ResultBufferSinkOperator {
    fn initial_step(&self) -> NextStep {
        self.next.clone()
    }
    fn try_push(&mut self, ctx: &mut DriverStep<'_>, input: RawInput) -> InputResult {
        if let Some(failure) = self.failure.clone().or_else(|| ctx.checkpoint().err()) {
            return InputResult::Rejected { input, failure };
        }
        if let Err(error) = self.prepared_factory() {
            let failure = self.fail(ctx, error);
            return InputResult::Rejected { input, failure };
        }
        if self.finishing || matches!(self.next, NextStep::Finished) {
            let failure = self.fail(ctx, fault(ResultReceiverFailure::Closed));
            return InputResult::Rejected { input, failure };
        }
        if self.receiver.is_occupied() {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            };
        }
        if let Err(error) = HydrateInput::validate_uncovered(&input) {
            let failure = self.fail(ctx, error);
            return InputResult::Rejected { input, failure };
        }
        match self
            .receiver
            .preparation_mut()
            .reserve(input.result_metadata(), self.session.as_ref())
        {
            Err(error) => {
                let failure = self.fail(ctx, error);
                InputResult::Rejected { input, failure }
            }
            Ok(Some(wait)) => InputResult::Deferred {
                input,
                next: AdmissionNext::Wait(wait),
            },
            Ok(None) => {
                let result = self.receiver.try_accept(ctx, input, &ResultEncoding);
                if matches!(&result, InputResult::Accepted(_)) {
                    self.next = NextStep::Runnable;
                }
                result
            }
        }
    }
    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        ctx.checkpoint()?;
        if let Err(error) = self.prepared_factory() {
            return Err(self.fail(ctx, error));
        }
        if matches!(self.next, NextStep::Finished) {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        let result = self.advance(ctx);
        match result {
            Ok(step) => {
                self.next = step.next.clone();
                Ok(step)
            }
            Err(error) => Err(self.fail(ctx, error)),
        }
    }
    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<()> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        ctx.checkpoint()?;
        if let Err(error) = self.prepared_factory() {
            return Err(self.fail(ctx, error));
        }
        self.finishing = true;
        if !matches!(self.next, NextStep::Finished) {
            self.next = NextStep::Runnable;
        }
        Ok(())
    }
}
impl Drop for ResultBufferSinkOperator {
    fn drop(&mut self) {
        self.discard();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Mutex, Weak};
    use std::time::Duration;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};

    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::operator::BlockedReason;

    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_state::RuntimeState;
    use crate::runtime::task::io::{ResultAbort, TaskIoError, TaskIoErrorKind, TaskIoOperation};
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
        release_backing: Option<Weak<dyn arrow::array::Array>>,
        released_live_backing: bool,
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
                        release_backing: None,
                        released_live_backing: false,
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
                if state
                    .release_backing
                    .as_ref()
                    .is_some_and(|backing| backing.upgrade().is_some())
                {
                    state.released_live_backing = true;
                }
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

    impl TaskResultSession for CreditSession {
        fn reservation_bytes(
            &self,
            input: crate::runtime::task::io::ResultInputMetadata,
        ) -> Result<usize, TaskIoError> {
            Ok(input.retained_bytes())
        }

        fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, TaskIoError> {
            if bytes > self.inner.capacity {
                return Err(TaskIoError::new(
                    TaskIoOperation::ResultWrite,
                    TaskIoErrorKind::InvalidResponse,
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
            _factory: &dyn InputConstructionFactory,
            _control: &dyn novarocks_functions::KernelEvaluationControl,
        ) -> Result<(), ResultWriteFailure> {
            assert_eq!(chunk.logical_bytes(), credit.bytes());
            self.inner
                .state
                .lock()
                .expect("credit session state lock")
                .queue
                .push_back((chunk, credit));
            Ok(())
        }

        fn finish(&self) -> Result<(), TaskIoError> {
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
        use crate::exec::chunk::ChunkSlotSchema;
        use novarocks_type_contract::{
            TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
        };
        let rows = values.len();
        let pure = ValueTypeStorageLifetime::pure();
        let binding = TypedValueBinding::try_new(
            ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, pure.clone()).unwrap(),
            ValueLayout::try_new(&DataType::Int32, pure.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("value", DataType::Int32, false)),
            binding,
            None,
            pure.clone(),
        )
        .unwrap();
        let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
        Chunk::try_new_typed_with_columns_observed::<crate::runtime::task::ExecutionFailure, _>(
            schema,
            vec![Arc::new(Int32Array::from(values))],
            rows,
            |_| Ok(()),
        )
        .unwrap()
    }

    fn runtime_state() -> RuntimeState {
        RuntimeState::for_local_execution(
            Some(QueryOptions::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn prepare_test_sink(operator: &mut dyn Operator) {
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("result sink fixture"),
            runtime_state().error_state(),
        ).unwrap();
        operator.set_construction_factory(factory);
        operator.prepare().unwrap();
    }

    fn with_context<T>(action: impl FnOnce(&mut DriverStep<'_>) -> T) -> T {
        let control = crate::runtime::runtime_state::RuntimeErrorState::default();
        let mut turn =
            crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        action(&mut DriverStep::new(
            &control,
            &mut turn,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
    }
    fn push(operator: &mut dyn Operator, input: RawInput) -> InputResult {
        with_context(|ctx| operator.try_push(ctx, input))
    }
    fn drive(operator: &mut dyn Operator) -> ExecutionResult<StepResult> {
        with_context(|ctx| operator.drive(ctx))
    }
    fn accepted(result: InputResult) {
        match result {
            InputResult::Accepted(result) => {
                result.unwrap();
            }
            InputResult::Rejected { failure, .. } => panic!("input rejected: {failure}"),
            InputResult::Deferred { .. } => panic!("input was not accepted"),
        }
    }
    #[test]
    fn byte_credit_blocks_and_consumer_release_wakes_without_overshoot() {
        let first = one_row_chunk();
        let second = one_row_chunk();
        let original = second.columns()[0].clone();
        let bytes = first.logical_bytes();
        assert!(bytes > 0);
        let edge = MemTracker::new_root("result edge");
        let receiver = MemTracker::new_root("result receiver");
        let mut second = second;
        second.transfer_to(&edge);
        let edge_charge = edge.current();
        let session = CreditSession::new(bytes);
        let factory = ResultBufferSinkFactory::new(session.clone(), None);
        let mut operator = factory.create(1, 0);
        prepare_test_sink(operator.as_mut());
        operator.set_mem_tracker(receiver.clone());
        accepted(push(operator.as_mut(), RawInput::for_test(first)));
        drive(operator.as_mut()).unwrap();
        assert_eq!(session.retained(), bytes);
        assert_eq!(session.high_water(), bytes);
        let raw = RawInput::for_test(second);
        let (raw, wait) = match push(operator.as_mut(), raw) {
            InputResult::Deferred {
                input,
                next: AdmissionNext::Wait(wait),
            } => (input, wait),
            _ => panic!("full receiver must return the original input with its capacity wait"),
        };
        assert!(Arc::ptr_eq(
            &original,
            &raw.original_for_test().columns()[0]
        ));
        assert_eq!(edge.current(), edge_charge);
        let raw = match push(operator.as_mut(), raw) {
            InputResult::Deferred {
                input,
                next: AdmissionNext::Wait(same),
            } => {
                assert_eq!(same.observed_generation, wait.observed_generation);
                assert!(Arc::ptr_eq(&same.observable, &wait.observable));
                input
            }
            _ => panic!("unchanged capacity still refuses the same input"),
        };
        session.consume_one();
        assert_eq!(session.retained(), 0);
        assert!(wait.is_ready());
        accepted(push(operator.as_mut(), raw));
        assert_eq!(edge.current(), 0);
        drive(operator.as_mut()).unwrap();
        with_context(|ctx| operator.as_mut().finish(ctx)).unwrap();
        assert!(matches!(
            drive(operator.as_mut()).unwrap().next,
            NextStep::Finished
        ));
        assert_eq!(
            session.retained(),
            bytes,
            "finish cannot return consumer-owned bytes"
        );
        assert_eq!(session.high_water(), bytes);
        session.consume_one();
        assert_eq!(session.retained(), 0);
        assert_eq!(receiver.current(), 0);
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
        prepare_test_sink(operator.as_mut());
        accepted(push(operator.as_mut(), RawInput::for_test(first)));
        drive(operator.as_mut()).unwrap();
        let generation = session.inner.writable.generation();
        let large = match push(operator.as_mut(), RawInput::for_test(large)) {
            InputResult::Deferred { input, .. } => input,
            _ => panic!("large reservation should not fit"),
        };
        accepted(push(operator.as_mut(), RawInput::for_test(small)));
        assert_eq!(session.inner.writable.generation(), generation);
        drive(operator.as_mut()).unwrap();
        assert_eq!(session.high_water(), 2 * bytes);
        session.consume_one();
        session.consume_one();
        assert_eq!(session.retained(), 0);
        assert_eq!(large.rows(), 2);
    }

    #[test]
    fn accepted_result_input_and_credit_survive_yield_and_eos_until_ordered_cancel() {
        let chunk = one_row_chunk();
        let backing = Arc::downgrade(&chunk.columns()[0]);
        let bytes = chunk.logical_bytes();
        let session = CreditSession::new(bytes);
        let factory = ResultBufferSinkFactory::new(session.clone(), None);
        let mut operator = factory.create(1, 0);
        prepare_test_sink(operator.as_mut());
        let tracker = MemTracker::new_root("result pending input");
        operator.set_mem_tracker(tracker.clone());
        accepted(push(operator.as_mut(), RawInput::for_test(chunk)));
        assert_eq!(session.retained(), bytes);
        assert!(tracker.current() > 0);
        let allocated = tracker.allocated();
        let control = crate::runtime::runtime_state::RuntimeErrorState::default();
        let mut turn =
            crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 0);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut turn, &mut value_conversions);
        let step = operator.as_mut().drive(&mut ctx).unwrap();
        assert!(matches!(step.next, NextStep::Yield));
        operator.as_mut().finish(&mut ctx).unwrap();
        assert!(matches!(
            operator.as_mut().drive(&mut ctx).unwrap().next,
            NextStep::Yield
        ));
        assert_eq!(tracker.allocated(), allocated);
        assert_eq!(session.retained(), bytes);
        assert!(session.inner.state.lock().unwrap().queue.is_empty());
        session.inner.state.lock().unwrap().release_backing = Some(backing.clone());
        operator.cancel();
        assert!(backing.upgrade().is_none());
        assert_eq!(tracker.current(), 0);
        assert_eq!(session.retained(), 0);
        assert!(!session.inner.state.lock().unwrap().released_live_backing);
    }

    struct OneChunkSource(Option<Chunk>);

    impl OperatorLifecycle for OneChunkSource {
        fn name(&self) -> &str {
            "OneChunkSource"
        }
    }
    impl crate::exec::pipeline::shape::SourceKernel for OneChunkSource {
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            Ok(crate::exec::pipeline::step::StepResult::new(
                self.0.take(),
                crate::exec::pipeline::step::NextStep::Finished,
                0,
            ))
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

    impl TaskResultSession for RacingCreditSession {
        fn reservation_bytes(
            &self,
            input: crate::runtime::task::io::ResultInputMetadata,
        ) -> Result<usize, TaskIoError> {
            self.inner.reservation_bytes(input)
        }
        fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, TaskIoError> {
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
            factory: &dyn InputConstructionFactory,
            control: &dyn novarocks_functions::KernelEvaluationControl,
        ) -> Result<(), ResultWriteFailure> {
            self.inner
                .write_with_credit(chunk, credit, factory, control)
        }
        fn finish(&self) -> Result<(), TaskIoError> {
            self.finished.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn abort(&self, reason: ResultAbort) {
            self.inner.abort(reason);
        }
    }

    fn credit_driver(session: Arc<RacingCreditSession>, chunk: Chunk) -> PipelineDriver {
        let factory = ResultBufferSinkFactory::new(session, None);
        let mut sink = factory.create(1, 0);
        prepare_test_sink(sink.as_mut());
        PipelineDriver::new(
            0,
            vec![
                Box::new(crate::exec::pipeline::shape::Source::new(OneChunkSource(
                    Some(chunk),
                ))),
                sink,
            ],
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
    #[test]
    fn result_sink_missing_creator_refuses_before_credit_or_input_consumption() {
        let input = one_row_chunk();
        let session = CreditSession::new(input.logical_bytes());
        let factory = ResultBufferSinkFactory::new(session.clone(), None);
        let mut operator = factory.create(1, 0);
        assert!(operator.creates_typed_storage());
        let error = operator.prepare().unwrap_err();
        assert!(matches!(
            error.cause(),
            crate::runtime::task::ExecutionFailureCause::Construction(
                ConstructionFailure::MissingOwner
            )
        ));
        let original = input.columns()[0].clone();
        let raw = match push(operator.as_mut(), RawInput::for_test(input.clone())) {
            InputResult::Rejected { input, failure } => {
                assert!(failure.same_occurrence(&error));
                input
            }
            _ => panic!("unprepared receiver cannot accept input"),
        };
        assert!(Arc::ptr_eq(
            &original,
            &raw.original_for_test().columns()[0]
        ));
        let repeated = drive(operator.as_mut()).unwrap_err();
        assert!(repeated.same_occurrence(&error));
        assert_eq!(session.retained(), 0);
        assert_eq!(session.high_water(), 0);
        assert!(session.inner.state.lock().unwrap().queue.is_empty());
        assert_eq!(input.len(), 1);
        assert_eq!(
            input.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            7
        );
    }
}
