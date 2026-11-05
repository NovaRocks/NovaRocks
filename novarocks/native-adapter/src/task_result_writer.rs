use std::num::NonZeroUsize;
use std::sync::Arc;

use novarocks_execution::runtime::exchange;
use novarocks_execution::runtime::observable::Observable;
#[cfg(test)]
use novarocks_execution::runtime::task::io::ResultPresentation;
use novarocks_execution::runtime::task::io::{
    ResultAbort, ResultWriteAdmission, ResultWriteCredit, ResultWriteFailure, ResultWriteSpec,
    TaskIoError, TaskIoErrorKind, TaskIoOperation, TaskResultSession, TaskResultWriter,
};
use novarocks_worker::result_buffer::{
    ResultBufferKey, ResultBufferWriteObligation, ResultPublication, ResultRetainedBudget,
};

pub fn native_result_writer(
    retained_budget: Arc<ResultRetainedBudget>,
    root_retained_byte_cap: NonZeroUsize,
) -> Arc<dyn TaskResultWriter> {
    Arc::new(NativeTaskResultWriter {
        retained_budget,
        root_retained_byte_cap,
    })
}

#[cfg(any(test, novarocks_dev_controls))]
pub fn test_native_result_writer() -> Arc<dyn TaskResultWriter> {
    native_result_writer(
        ResultRetainedBudget::new(
            NonZeroUsize::new(32 * 1024 * 1024).expect("test process result cap is nonzero"),
        ),
        NonZeroUsize::new(16 * 1024 * 1024).expect("test root result cap is nonzero"),
    )
}

struct NativeTaskResultWriter {
    retained_budget: Arc<ResultRetainedBudget>,
    root_retained_byte_cap: NonZeroUsize,
}

impl TaskResultWriter for NativeTaskResultWriter {
    fn open(&self, spec: ResultWriteSpec) -> Result<Arc<dyn TaskResultSession>, TaskIoError> {
        if !spec.is_typed() {
            return Err(TaskIoError::new(
                TaskIoOperation::ResultOpen,
                TaskIoErrorKind::RemoteRejected,
                "native task result stream requires typed Arrow IPC",
            ));
        }
        let identity = spec.task_identity().ok_or_else(|| {
            TaskIoError::new(
                TaskIoOperation::ResultOpen,
                TaskIoErrorKind::InvalidResponse,
                "native result stream requires a complete task identity",
            )
        })?;
        let handle = ResultBufferWriteObligation::open(
            ResultBufferKey::Task(identity),
            spec.is_typed(),
            self.root_retained_byte_cap,
            Arc::clone(&self.retained_budget),
            None,
        )
        .map_err(|error| {
            TaskIoError::new(
                TaskIoOperation::ResultOpen,
                TaskIoErrorKind::Internal,
                error,
            )
        })?;
        Ok(Arc::new(NativeTaskResultSession {
            handle,
            root_retained_byte_cap: self.root_retained_byte_cap,
        }))
    }
}

struct NativeTaskResultSession {
    handle: ResultBufferWriteObligation,
    root_retained_byte_cap: NonZeroUsize,
}

impl TaskResultSession for NativeTaskResultSession {
    fn reservation_bytes(
        &self,
        input: novarocks_execution::runtime::task::io::ResultInputMetadata,
    ) -> Result<usize, TaskIoError> {
        if input.rows() == 0 {
            return Ok(0);
        }
        if input.retained_bytes() >= self.root_retained_byte_cap.get() {
            return Err(TaskIoError::new(
                TaskIoOperation::ResultWrite,
                TaskIoErrorKind::Unavailable,
                format!(
                    "result chunk retains {} bytes and leaves no encoded capacity within joint cap {}",
                    input.retained_bytes(),
                    self.root_retained_byte_cap
                ),
            ));
        }
        Ok(self.root_retained_byte_cap.get())
    }

    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, TaskIoError> {
        self.handle.try_acquire(bytes).map_err(|error| {
            TaskIoError::from_domain(
                TaskIoOperation::ResultWrite,
                TaskIoErrorKind::Unavailable,
                error,
            )
        })
    }

    fn writable_observable(&self) -> Option<Arc<Observable>> {
        Some(self.handle.writable_observable())
    }

    fn write_with_credit(
        &self,
        chunk: novarocks_execution::exec::chunk::Chunk,
        credit: ResultWriteCredit,
        factory: &dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<(), ResultWriteFailure> {
        if chunk.is_empty() {
            return Ok(());
        }
        let arrow_retained_bytes = chunk.logical_bytes();
        let encoded_byte_cap = credit
            .bytes()
            .checked_sub(arrow_retained_bytes)
            .ok_or_else(|| {
                TaskIoError::new(
                    TaskIoOperation::ResultWrite,
                    TaskIoErrorKind::InvalidResponse,
                    format!(
                        "result write owns {} joint bytes but its Arrow input retains {arrow_retained_bytes} bytes",
                        credit.bytes()
                    ),
                )
            })?;
        let encoded = exchange::encode_chunks_bounded_with_creation(
            &[chunk],
            true,
            encoded_byte_cap,
            factory,
            control,
        )
        .map_err(|cause| ResultWriteFailure::Execution(cause.into_execution()))?;
        self.handle
            .write_typed_with_credit(encoded, credit)
            .map(|_| ())
            .map_err(|error| {
                TaskIoError::new(
                    TaskIoOperation::ResultWrite,
                    TaskIoErrorKind::Internal,
                    error,
                )
                .into()
            })
    }

    fn finish(&self) -> Result<(), TaskIoError> {
        self.handle
            .finish()
            .map(|publication| {
                if publishes_terminal(publication) {
                    crate::backend_metrics::record_task_result_terminal("finished");
                }
            })
            .map_err(|error| {
                TaskIoError::new(
                    TaskIoOperation::ResultFinish,
                    TaskIoErrorKind::Internal,
                    error,
                )
            })
    }

    fn abort(&self, reason: ResultAbort) {
        if publishes_terminal(self.handle.abort(reason)) {
            crate::backend_metrics::record_task_result_terminal("aborted");
        }
    }
}

fn publishes_terminal(publication: ResultPublication) -> bool {
    publication == ResultPublication::TerminalReady
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field};
    use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_types::{SlotId, UniqueId};

    fn task_identity(seed: u32) -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(
                QueryId::new(i64::from(seed), i64::from(seed) + 1),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query identity"),
            StageId::new(seed).expect("nonzero stage"),
            TaskId::new(seed).expect("nonzero task"),
            BackendProcessId::new_v7(),
        )
    }

    fn typed_chunk() -> Chunk {
        use novarocks_type_contract::{
            TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
        };
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
        Chunk::try_new_typed_with_columns_observed::<
            novarocks_execution::runtime::task::ExecutionFailure,
            _,
        >(
            schema,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            1,
            |_| Ok(()),
        )
        .unwrap()
    }

    struct ResultTestControl;
    impl novarocks_functions::KernelEvaluationControl for ResultTestControl {
        fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
            panic!("IPC construction does not wait");
        }
    }
    fn actual_result_factory()
    -> Arc<dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory> {
        novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root("native result fixture"),
            Arc::new(novarocks_execution::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap()
    }

    #[test]
    fn native_result_writer_rejects_a_stream_without_task_identity() {
        let result = test_native_result_writer().open(ResultWriteSpec::new(
            novarocks_types::UniqueId::new(1, 2),
            ResultPresentation::ArrowIpc,
            None,
            true,
        ));
        let Err(error) = result else {
            panic!("native result ownership requires the complete task identity");
        };
        assert_eq!(error.kind(), TaskIoErrorKind::InvalidResponse);
    }

    #[test]
    fn native_result_writer_rejects_untyped_task_compatibility_path() {
        let identity = task_identity(811);
        let result = test_native_result_writer().open(
            ResultWriteSpec::new(
                UniqueId::new(811, 1),
                ResultPresentation::ArrowIpc,
                None,
                false,
            )
            .with_task_identity(identity),
        );
        let Err(error) = result else {
            panic!("native task results must use the bounded typed path");
        };
        assert_eq!(error.kind(), TaskIoErrorKind::RemoteRejected);
    }

    #[test]
    fn typed_result_joint_cap_counts_arrow_and_encoded_backings() {
        let arrow_retained = typed_chunk().logical_bytes();
        let required_encoded_bytes = exchange::encode_chunks_bounded_with_creation(
            &[typed_chunk()],
            true,
            1024 * 1024,
            actual_result_factory().as_ref(),
            &ResultTestControl,
        )
        .expect("measure bounded result payload")
        .len();
        let joint_cap = arrow_retained
            .checked_add(required_encoded_bytes)
            .expect("joint cap fits usize");
        let too_small = NonZeroUsize::new(joint_cap - 1).expect("nonzero joint cap");
        let identity = task_identity(812);
        let session = native_result_writer(ResultRetainedBudget::new(too_small), too_small)
            .open(
                ResultWriteSpec::new(
                    UniqueId::new(812, 1),
                    ResultPresentation::ArrowIpc,
                    None,
                    true,
                )
                .with_task_identity(identity),
            )
            .expect("open bounded typed task result");
        let reservation = session
            .reservation_bytes((&typed_chunk()).into())
            .expect("Arrow input fits the joint cap");
        let ResultWriteAdmission::Granted(credit) = session
            .try_acquire(reservation)
            .expect("joint reservation is valid")
        else {
            panic!("empty stream owns the joint cap");
        };

        let error = session
            .write_with_credit(
                typed_chunk(),
                credit,
                actual_result_factory().as_ref(),
                &ResultTestControl,
            )
            .expect_err("Arrow plus encoded backing must not exceed the joint cap");
        let ResultWriteFailure::Execution(error) = error else {
            panic!("joint cap refusal retains the actual bounded encoder cause");
        };
        use novarocks_execution_contract::failure::{
            FailureClass, FailureProjection, FailureReason, RepresentationBoundary, domain,
        };
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(
            fact.reason(),
            FailureReason::new(domain::EXCHANGE, std::num::NonZeroU16::new(112).unwrap())
        );
        let representation = fact.representation().expect("actual encoded packet extent");
        assert_eq!(
            representation.boundary,
            RepresentationBoundary::SingleOutput
        );
        let encoded_cap = reservation.checked_sub(arrow_retained).unwrap() as u64;
        assert_eq!(representation.extent_limit, Some(encoded_cap));
        assert!(representation.required.unwrap() > encoded_cap);
        assert_eq!(representation.representable_prefix, None);
        assert!(
            std::error::Error::source(&error).is_some(),
            "the original encoder source remains available locally"
        );
        session.abort(ResultAbort::NeverStarted);
    }

    #[test]
    fn only_terminal_ready_is_a_published_result_terminal() {
        assert!(publishes_terminal(ResultPublication::TerminalReady));
        assert!(!publishes_terminal(ResultPublication::DataReady));
        assert!(!publishes_terminal(ResultPublication::Removed));
        assert!(!publishes_terminal(ResultPublication::NoChange));
    }
    mod bounded_root_creator_tests {
        use super::*;
        use novarocks_execution::exec::chunk::{
            ArrayNormalizationStep, ArrayPathStep, ChunkSchema,
        };
        use novarocks_execution::exec::pipeline::operator::{
            ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
        };
        use novarocks_execution::runtime::mem_tracker::MemTracker;
        use novarocks_execution::runtime::task::ExecutionFailureCause;
        use novarocks_type_contract::ValueTypeStorageLifetime;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        struct FirstHeaderRefusalFactory {
            inner: Arc<dyn InputConstructionFactory>,
            tracker: Arc<MemTracker>,
            requests: Arc<AtomicUsize>,
            refused: Arc<AtomicBool>,
            tail: Arc<AtomicUsize>,
        }
        struct FirstHeaderRefusalOwner {
            inner: Box<dyn InputConstructionOwner>,
            tracker: Arc<MemTracker>,
            requests: Arc<AtomicUsize>,
            refused: Arc<AtomicBool>,
            tail: Arc<AtomicUsize>,
        }
        impl FirstHeaderRefusalOwner {
            fn before(&self) {
                if self.refused.load(Ordering::SeqCst) {
                    self.tail.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        impl InputConstructionOwner for FirstHeaderRefusalOwner {
            fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
                self.inner.storage_lifetime()
            }
            fn request(&self, layout: std::alloc::Layout) -> Result<(), ConstructionFailure> {
                self.before();
                if self.requests.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.tracker
                        .install_limit_once(self.tracker.current() + 1)
                        .unwrap();
                }
                let result = self.inner.request(layout);
                if result.is_err() {
                    self.refused.store(true, Ordering::SeqCst);
                }
                result
            }
            fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
                self.before();
                self.inner.observe_opaque_upper_bound(bytes)
            }
            fn observe(
                &self,
                occurrence: usize,
                path: &[ArrayPathStep],
                step: ArrayNormalizationStep<'_>,
            ) -> Result<(), ConstructionFailure> {
                self.before();
                self.inner.observe(occurrence, path, step)
            }
        }
        impl InputConstructionFactory for FirstHeaderRefusalFactory {
            fn observe_input_work(&self, units: usize) -> Result<(), ConstructionFailure> {
                self.inner.observe_input_work(units)
            }
            fn begin_typed_construction(
                &self,
                schema: &ChunkSchema,
            ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
                Ok(Box::new(FirstHeaderRefusalOwner {
                    inner: self.inner.begin_typed_construction(schema)?,
                    tracker: self.tracker.clone(),
                    requests: self.requests.clone(),
                    refused: self.refused.clone(),
                    tail: self.tail.clone(),
                }))
            }
            fn begin_compiled_layout_construction(
                &self,
                layout: &novarocks_local_program::StaticLayout,
            ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
                self.inner.begin_compiled_layout_construction(layout)
            }
        }
        fn open(
            seed: u32,
            budget: Arc<ResultRetainedBudget>,
            cap: NonZeroUsize,
        ) -> (TaskIdentity, Arc<dyn TaskResultSession>) {
            let identity = task_identity(seed);
            let session = native_result_writer(budget, cap)
                .open(
                    ResultWriteSpec::new(
                        UniqueId::new(i64::from(seed), 1),
                        ResultPresentation::ArrowIpc,
                        None,
                        true,
                    )
                    .with_task_identity(identity),
                )
                .unwrap();
            (identity, session)
        }
        async fn assert_no_publication(identity: TaskIdentity) {
            assert!(matches!(
                novarocks_worker::result_buffer::wait_fetch_task_typed(
                    identity,
                    None,
                    std::time::Duration::ZERO,
                    novarocks_execution_contract::ResultByteLimit::new(1024 * 1024).unwrap(),
                )
                .await,
                novarocks_worker::result_buffer::TryFetchTypedResult::NotReady
            ));
        }
        #[tokio::test]
        async fn native_bounded_result_first_real_header_refusal_keeps_cause_credit_and_no_tail() {
            let cap = NonZeroUsize::new(1024 * 1024).unwrap();
            let budget = ResultRetainedBudget::new(cap);
            let (identity, session) = open(1801, budget.clone(), cap);
            let tracker = MemTracker::new_root("native bounded header refusal");
            let inner = novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                tracker.clone(), Arc::new(novarocks_execution::runtime::runtime_state::RuntimeErrorState::default()),
            ).unwrap();
            let requests = Arc::new(AtomicUsize::new(0));
            let tail = Arc::new(AtomicUsize::new(0));
            let factory = FirstHeaderRefusalFactory {
                inner,
                tracker: tracker.clone(),
                requests: requests.clone(),
                refused: Arc::new(AtomicBool::new(false)),
                tail: tail.clone(),
            };
            let baseline = tracker.current();
            let input = typed_chunk();
            let ResultWriteAdmission::Granted(credit) = session
                .try_acquire(session.reservation_bytes((&input).into()).unwrap())
                .unwrap()
            else {
                panic!("fresh stream has its original credit");
            };
            let error = session
                .write_with_credit(input.clone(), credit, &factory, &ResultTestControl)
                .unwrap_err();
            let ResultWriteFailure::Execution(error) = error else {
                panic!("actual creator refusal must remain nominal");
            };
            assert!(matches!(
                error.cause(),
                ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
            ));
            assert_eq!(requests.load(Ordering::SeqCst), 1);
            assert_eq!(tail.load(Ordering::SeqCst), 0);
            assert_eq!(
                tracker.current(),
                baseline,
                "failed temporary writer scope is rolled back"
            );
            assert_no_publication(identity).await;
            let ResultWriteAdmission::Granted(returned) =
                budget.try_reserve_process(cap.get()).unwrap()
            else {
                panic!("failed write must return its entire original result credit");
            };
            drop(returned);
            assert_eq!(
                input.columns()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                1
            );
            session.abort(ResultAbort::NeverStarted);
            drop(factory);
            assert_eq!(tracker.current(), 0);
        }
        struct CancelControl(AtomicUsize);
        impl novarocks_functions::KernelEvaluationControl for CancelControl {
            fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(novarocks_functions::KernelFailure::Cancelled)
            }
            fn wait(
                &self,
                _: std::time::Duration,
            ) -> Result<(), novarocks_functions::KernelFailure> {
                panic!("writer does not wait");
            }
        }
        #[tokio::test]
        async fn native_bounded_result_original_control_refusal_is_not_io_or_publication() {
            let cap = NonZeroUsize::new(1024 * 1024).unwrap();
            let budget = ResultRetainedBudget::new(cap);
            let (identity, session) = open(1802, budget.clone(), cap);
            let factory = actual_result_factory();
            let control = CancelControl(AtomicUsize::new(0));
            let input = typed_chunk();
            let ResultWriteAdmission::Granted(credit) = session
                .try_acquire(session.reservation_bytes((&input).into()).unwrap())
                .unwrap()
            else {
                panic!("fresh stream credit");
            };
            let error = session
                .write_with_credit(input, credit, factory.as_ref(), &control)
                .unwrap_err();
            let ResultWriteFailure::Execution(error) = error else {
                panic!("control cause must remain nominal");
            };
            assert!(matches!(
                error.cause(),
                ExecutionFailureCause::Kernel(novarocks_functions::KernelFailure::Cancelled)
            ));
            assert_eq!(control.0.load(Ordering::SeqCst), 1);
            assert_no_publication(identity).await;
            let ResultWriteAdmission::Granted(returned) =
                budget.try_reserve_process(cap.get()).unwrap()
            else {
                panic!("cancelled write returns credit");
            };
            drop(returned);
            session.abort(ResultAbort::NeverStarted);
        }
        struct OriginalCallerFactory {
            inner: Arc<dyn InputConstructionFactory>,
            original: novarocks_execution::runtime::task::ExecutionFailure,
            requests: Arc<AtomicUsize>,
        }
        struct OriginalCallerOwner {
            inner: Box<dyn InputConstructionOwner>,
            original: novarocks_execution::runtime::task::ExecutionFailure,
            requests: Arc<AtomicUsize>,
        }
        impl InputConstructionOwner for OriginalCallerOwner {
            fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
                self.inner.storage_lifetime()
            }
            fn request(&self, _layout: std::alloc::Layout) -> Result<(), ConstructionFailure> {
                self.requests.fetch_add(1, Ordering::SeqCst);
                Err(ConstructionFailure::Caller(Box::new(self.original.clone())))
            }
            fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
                self.inner.observe_opaque_upper_bound(bytes)
            }
            fn observe(
                &self,
                occurrence: usize,
                path: &[ArrayPathStep],
                step: ArrayNormalizationStep<'_>,
            ) -> Result<(), ConstructionFailure> {
                self.inner.observe(occurrence, path, step)
            }
        }
        impl InputConstructionFactory for OriginalCallerFactory {
            fn observe_input_work(&self, units: usize) -> Result<(), ConstructionFailure> {
                self.inner.observe_input_work(units)
            }
            fn begin_typed_construction(
                &self,
                schema: &ChunkSchema,
            ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
                Ok(Box::new(OriginalCallerOwner {
                    inner: self.inner.begin_typed_construction(schema)?,
                    original: self.original.clone(),
                    requests: self.requests.clone(),
                }))
            }
            fn begin_compiled_layout_construction(
                &self,
                layout: &novarocks_local_program::StaticLayout,
            ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
                self.inner.begin_compiled_layout_construction(layout)
            }
        }
        #[tokio::test]
        async fn native_bounded_result_original_pipeline_caller_is_not_wire_io() {
            let cap = NonZeroUsize::new(1024 * 1024).unwrap();
            let budget = ResultRetainedBudget::new(cap);
            let (identity, session) = open(1803, budget.clone(), cap);
            let original: novarocks_execution::runtime::task::ExecutionFailure =
                novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new(
                        "actual creator original pipeline refusal",
                    ),
                )
                .into();
            let requests = Arc::new(AtomicUsize::new(0));
            let factory = OriginalCallerFactory {
                inner: actual_result_factory(),
                original: original.clone(),
                requests: requests.clone(),
            };
            let input = typed_chunk();
            let ResultWriteAdmission::Granted(credit) = session
                .try_acquire(session.reservation_bytes((&input).into()).unwrap())
                .unwrap()
            else {
                panic!("fresh result credit");
            };
            let error = session
                .write_with_credit(input, credit, &factory, &ResultTestControl)
                .unwrap_err();
            let ResultWriteFailure::Execution(error) = error else {
                panic!("creator Caller(Pipeline) must not be classified as Wire");
            };
            assert_eq!(error, original);
            assert_eq!(requests.load(Ordering::SeqCst), 1);
            assert_no_publication(identity).await;
            let ResultWriteAdmission::Granted(returned) =
                budget.try_reserve_process(cap.get()).unwrap()
            else {
                panic!("original Caller refusal returns credit");
            };
            drop(returned);
            session.abort(ResultAbort::NeverStarted);
        }
    }
}
