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
//! Core operator traits and blocking semantics.
//!
//! Responsibilities:
//! - Defines source/processor/sink execution contracts and blocked-reason signaling.
//! - Used by drivers to orchestrate cooperative operator execution steps.
//!
//! Key exported interfaces:
//! - Types: `BlockedReason`, `FinishWatch`, `OperatorLifecycle`, `Operator`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::runtime::task::{ExecutionFailure, ExecutionResult};

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::io::TaskEventSink;
use arrow::datatypes::DataType;
use novarocks_types::SlotId;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[path = "input_construction.rs"]
pub mod input_construction;
pub use input_construction::{
    ConstructionFailure, EncodedValueOccurrence, InputConstructionFactory, InputConstructionOwner,
};

#[derive(Clone, Debug, PartialEq, Eq)]
/// The execution engine uses cooperative scheduling.
///
/// Operators are driven by a [`PipelineDriver`](crate::exec::pipeline::driver::PipelineDriver)
/// which repeatedly tries to move data from upstream to downstream.
/// When a driver cannot make progress without blocking, it records a [`BlockedReason`]
/// and yields.
pub enum BlockedReason {
    /// Upstream currently has no data available.
    InputEmpty,
    /// Downstream cannot accept more output at the moment.
    OutputFull,
    /// Blocked on a dependency object (e.g. build-side ready).
    Dependency(DependencyHandle),
}

/// Worker-frozen deadline that may invalidate one event-blocked decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverBlockDeadline {
    at: Instant,
    token: u64,
}

impl DriverBlockDeadline {
    pub(crate) fn new(at: Instant, token: u64) -> Self {
        Self { at, token }
    }

    pub(crate) fn at(self) -> Instant {
        self.at
    }

    pub(crate) fn token(self) -> u64 {
        self.token
    }
}

/// How a driver learns that an operator's pending finish work may have ended.
///
/// An operator can only report pending finish work together with a watch, so
/// a driver never has to guess when to look again.
#[derive(Clone)]
pub enum FinishWatch {
    /// Notified whenever the pending work may have ended. The observable keeps
    /// its identity for the operator's lifetime.
    Notify(Arc<Observable>),
    /// Nothing announces the end of the pending work; the driver checks again
    /// after this interval. Only for owners without a completion event: every
    /// recheck costs a scheduling turn.
    RecheckAfter(Duration),
}

impl std::fmt::Debug for FinishWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Notify(observable) => f
                .debug_tuple("Notify")
                .field(&observable.generation())
                .finish(),
            Self::RecheckAfter(interval) => f.debug_tuple("RecheckAfter").field(interval).finish(),
        }
    }
}

/// Physical exit is independent of data EOS and the accepted Task conclusion.
/// Neither an error nor a stop request certifies that an owner has exited.
#[derive(Debug)]
pub enum ExitProgress {
    Pending(FinishWatch),
    Exited,
    Unresolved(FinishWatch),
}

/// The author identifies which obligation failed. A caller never infers this
/// role from the error class, component, message, or accepted Task conclusion.
#[derive(Debug)]
pub enum ExitFailure {
    Required(ExecutionFailure),
    Cleanup(ExecutionFailure),
}

/// The worker consumes each failure once and may read physical progress again.
/// Authors that already reported an event through the bound runtime port leave
/// `failure` empty; repeated observations cannot submit the same event again.
#[derive(Debug)]
pub struct ExitObservation {
    pub progress: ExitProgress,
    pub failure: Option<ExitFailure>,
    /// A newly observed physical-exit fault, consumed once by the same worker.
    /// This is health evidence and grants no Task conclusion authority.
    pub unresolved_exit: Option<ExecutionFailure>,
}
impl ExitObservation {
    pub fn pending(watch: FinishWatch) -> Self {
        Self {
            progress: ExitProgress::Pending(watch),
            failure: None,
            unresolved_exit: None,
        }
    }
    pub fn exited() -> Self {
        Self {
            progress: ExitProgress::Exited,
            failure: None,
            unresolved_exit: None,
        }
    }
    pub fn unresolved(watch: FinishWatch) -> Self {
        Self {
            progress: ExitProgress::Unresolved(watch),
            failure: None,
            unresolved_exit: None,
        }
    }
    pub fn with_failure(mut self, failure: ExitFailure) -> Self {
        self.failure = Some(failure);
        self
    }
    pub fn with_unresolved_exit(mut self, failure: ExecutionFailure) -> Self {
        self.unresolved_exit = Some(failure);
        self
    }
    pub(crate) fn report_to(
        mut self,
        control: &crate::runtime::runtime_state::RuntimeErrorState,
    ) -> ExitProgress {
        if let Some(failure) = self.unresolved_exit.take() {
            control.report_unresolved_exit(&failure);
        }
        match self.failure.take() {
            Some(ExitFailure::Required(failure)) => control.set_error(failure),
            Some(ExitFailure::Cleanup(failure)) => control.report_cleanup(&failure),
            None => {}
        }
        self.progress
    }
}

/// Base operator contract implemented by source/processor/sink operator implementations.
pub trait OperatorLifecycle: Send {
    fn name(&self) -> &str;

    /// Install the owned provider handle before activation. Only the driver
    /// replenishes the shared allowance; synchronous work borrows DriverStep.
    fn bind_poll_budget(
        &mut self,
        _budget: novarocks_spi::connector::read_stack::ConnectorPollBudget,
    ) {
    }

    fn on_downstream_backpressure(&mut self, _paused: bool) {}

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let _ = tracker;
    }

    /// The preparation owner installs construction observations before any
    /// source or processor can create independently retained typed storage.
    fn set_construction_factory(&mut self, _factory: Arc<dyn InputConstructionFactory>) {}

    fn creates_typed_storage(&self) -> bool {
        false
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        let _ = profiles;
    }

    /// An exact frozen schema construction site, independent of funding.
    /// The preparation caller may qualify its real body with original scope.
    fn compiled_schema_preparation_site(
        &self,
    ) -> Option<(
        &Arc<novarocks_local_program::LocalProgram>,
        crate::runtime::preparation_metadata::CompiledSchemaSite,
    )> {
        None
    }

    #[cfg(any(test, novarocks_dev_controls))]
    fn prepared_schema_for_test(&self) -> Option<crate::exec::chunk::ChunkSchemaRef> {
        None
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        Ok(())
    }

    fn bind_runtime_state(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        Ok(())
    }

    /// Start task-owned activity after the driver has been admitted for execution.
    /// Preparation and binding must leave providers and subscriptions dormant.
    fn activate(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        Ok(())
    }

    fn set_task_event_sink(&mut self, _sink: Arc<dyn TaskEventSink>) {}

    /// End production and start outstanding owner exits exactly once. Errors
    /// from this cleanup request are diagnostics, never required-output events.
    /// It may initiate asynchronous closure but cannot certify actual exit.
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        Ok(())
    }

    fn observe_exit(&mut self) -> ExitObservation {
        match self.pending_finish() {
            Some(watch) => ExitObservation::pending(watch),
            None => ExitObservation::exited(),
        }
    }

    /// Composite owners expose each real exit separately. An error from one
    /// child must not hide a sibling which still owns asynchronous work.
    fn visit_exits(&mut self, visit: &mut dyn FnMut(ExitObservation)) {
        visit(self.observe_exit());
    }

    /// Release resources after production has ended. Required output and
    /// external commit belong to the normal step/finish continuation. An error
    /// here is a cleanup diagnostic; physical exit is observed independently.
    fn close(&mut self) -> ExecutionResult<()> {
        Ok(())
    }

    fn cancel(&mut self) {
        // Default: nothing to cancel.
    }

    /// Handles a driver execution failure before the operator is closed.
    ///
    /// The default preserves the existing cleanup behavior for operators that do not distinguish
    /// execution failure from cancellation.
    fn on_driver_failure(&mut self) {
        self.cancel();
    }

    /// Asynchronous work this operator still owns after its driver reached a
    /// terminal state, as the way to learn that the work may have ended.
    ///
    /// While this is `Some`, the driver cannot complete: it parks on the watch
    /// and asks again when the watch fires.
    fn pending_finish(&self) -> Option<FinishWatch> {
        None
    }
}

/// A factory returns one object with complete lifecycle and step contracts.
/// Kernels become operators through a production shape; stateful credit and
/// asynchronous owners implement their own step contract directly.
pub trait Operator: OperatorLifecycle + super::shape::StepOperator {}
impl<T: OperatorLifecycle + super::shape::StepOperator + ?Sized> Operator for T {}

/// Forwards readiness transitions without retaining the target observable.
///
/// Composite operators use this once during construction to keep their public
/// observable identity stable while covering every child readiness source.
pub(crate) fn forward_observable(source: &Arc<Observable>, target: &Arc<Observable>) {
    if Arc::ptr_eq(source, target) {
        return;
    }
    let target = Arc::downgrade(target);
    source.add_observer(Arc::new(move || {
        if let Some(target) = target.upgrade() {
            target.notify_observers();
        }
    }));
}

/// The installed consumer's encoding contract. Preparation borrows this
/// declaration from the same receiver that will execute the prepared input.
pub trait InputEncodingPolicy: Send {
    fn retains_encoded_column(&self, _slot_id: SlotId, _data_type: &DataType) -> bool {
        false
    }
    fn retains_encoded_path(&self, value: EncodedValueOccurrence<'_>) -> bool {
        value.path.is_empty() && self.retains_encoded_column(value.slot, value.carrier)
    }
}

/// Prepare an input using the receiving owner's encoding declaration and
/// private construction capability.
pub(crate) fn hydrate_for_downstream_with_factory(
    chunk: &Chunk,
    downstream: &(impl InputEncodingPolicy + ?Sized),
    factory: Option<&dyn InputConstructionFactory>,
    conversions: &mut novarocks_value_arrays::decode::ValueConversionContext,
) -> ExecutionResult<Chunk> {
    if chunk.chunk_schema().is_typed() {
        let mut accepts =
            |slot, occurrence, path: &[crate::exec::chunk::ArrayPathStep], carrier: &DataType| {
                downstream.retains_encoded_path(EncodedValueOccurrence {
                    slot,
                    occurrence,
                    path,
                    carrier,
                })
            };
        if !crate::exec::chunk::dictionary_hydration_required_observed::<ConstructionFailure, _, _>(
            chunk,
            &mut accepts,
            |_, path, _| match factory {
                Some(factory) => factory.observe_input_work(1 + path.len()),
                None => Ok(()),
            },
        )? {
            return Ok(chunk.clone());
        }
        let factory = factory.ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_input_construction(chunk)?;
        return crate::exec::chunk::hydrate_dictionary_columns_owned_observed(
            chunk,
            owner.storage_lifetime(),
            conversions,
            accepts,
            |occurrence, path, step| owner.observe(occurrence, path, step),
            |layout| owner.request(layout),
            |units| factory.observe_input_work(units),
        )
        .map_err(Into::into);
    }
    if chunk
        .columns()
        .iter()
        .any(|array| crate::exec::chunk::contains_dictionary(array.data_type()))
    {
        return Err(ConstructionFailure::from(
            crate::exec::chunk::ChunkConstructionFailure::missing_hydration_binding(),
        )
        .into());
    }
    Ok(chunk.clone())
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct DictionaryCarrierStats {
    pub input_rows: i64,
    pub input_columns: i64,
    pub kept_rows: i64,
    pub kept_columns: i64,
    pub hydrated_rows: i64,
    pub hydrated_columns: i64,
    pub unsupported_columns: i64,
}

pub(crate) fn dictionary_carrier_stats(
    chunk: &Chunk,
    downstream: &(impl InputEncodingPolicy + ?Sized),
) -> DictionaryCarrierStats {
    let rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
    let mut stats = DictionaryCarrierStats::default();

    for (occurrence, slot) in chunk.chunk_schema().slots().iter().enumerate() {
        let data_type = slot.data_type();
        if !matches!(data_type, arrow::datatypes::DataType::Dictionary(_, _)) {
            continue;
        }

        stats.input_columns = stats.input_columns.saturating_add(1);
        stats.input_rows = stats.input_rows.saturating_add(rows);

        if downstream.retains_encoded_path(EncodedValueOccurrence {
            slot: slot.slot_id(),
            occurrence,
            path: &[],
            carrier: data_type,
        }) {
            stats.kept_columns = stats.kept_columns.saturating_add(1);
            stats.kept_rows = stats.kept_rows.saturating_add(rows);
        } else {
            stats.hydrated_columns = stats.hydrated_columns.saturating_add(1);
            stats.hydrated_rows = stats.hydrated_rows.saturating_add(rows);
            stats.unsupported_columns = stats.unsupported_columns.saturating_add(1);
        }
    }

    stats
}

#[cfg(test)]
mod typed_input_construction_tests {
    use super::*;
    use crate::exec::chunk::{ArrayNormalizationStep, ArrayPathStep, ChunkSchema, ChunkSlotSchema};
    use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{
        TypedValueBinding, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Owner {
        storage: ValueTypeStorageLifetime,
        requests: Arc<AtomicUsize>,
        inspected: Arc<AtomicUsize>,
        reject_requests: bool,
    }
    impl InputConstructionOwner for Owner {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            self.storage.clone()
        }
        fn request(&self, _: std::alloc::Layout) -> Result<(), ConstructionFailure> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            if self.reject_requests {
                Err(ConstructionFailure::Caller(Box::new(
                    novarocks_functions::KernelFailure::ResourceExhausted.into(),
                )))
            } else {
                Ok(())
            }
        }
        fn observe_opaque_upper_bound(&self, _: usize) -> Result<(), ConstructionFailure> {
            self.inspected.fetch_add(1, Ordering::SeqCst);
            if self.reject_requests {
                Err(ConstructionFailure::Caller(Box::new(
                    novarocks_functions::KernelFailure::ResourceExhausted.into(),
                )))
            } else {
                Ok(())
            }
        }
        fn observe(
            &self,
            _: usize,
            _: &[ArrayPathStep],
            _: ArrayNormalizationStep<'_>,
        ) -> Result<(), ConstructionFailure> {
            self.inspected.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    struct Factory {
        drops: Arc<AtomicUsize>,
        begins: AtomicUsize,
        factory_requests: AtomicUsize,
        requests: Arc<AtomicUsize>,
        inspected: Arc<AtomicUsize>,
        reject_requests: bool,
        control_error: Option<novarocks_type_contract::CompileControlError>,
    }
    impl InputConstructionFactory for Factory {
        fn observe_input_work(&self, _: usize) -> Result<(), ConstructionFailure> {
            match self.control_error.clone() {
                Some(error) => Err(error.into()),
                None => Ok(()),
            }
        }
        fn begin_compiled_layout_construction(
            &self,
            layout: &novarocks_local_program::StaticLayout,
        ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
            if !layout.is_typed() {
                return Err(ConstructionFailure::InvalidSource(
                    "fixture requires a typed layout".into(),
                ));
            }
            self.begin_fixture_scope()
        }
        fn begin_typed_construction(
            &self,
            _: &ChunkSchema,
        ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
            self.begin_fixture_scope()
        }
    }
    impl Factory {
        fn begin_fixture_scope(
            &self,
        ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
            self.begins.fetch_add(1, Ordering::SeqCst);
            let _hold_request = novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<Hold>(),
            )
            .map_err(|error| {
                ConstructionFailure::InvalidSource(format!("test factory hold request: {error:?}"))
            })?;
            self.factory_requests.fetch_add(1, Ordering::SeqCst);
            let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(self.drops.clone())));
            let _scope_request = std::alloc::Layout::new::<Owner>();
            self.factory_requests.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Owner {
                storage,
                requests: self.requests.clone(),
                inspected: self.inspected.clone(),
                reject_requests: self.reject_requests,
            }))
        }
    }
    struct Consumer {
        factory: Option<Factory>,
        nested: bool,
    }

    impl InputEncodingPolicy for Consumer {
        fn retains_encoded_column(&self, slot: SlotId, _: &DataType) -> bool {
            slot == SlotId::new(1)
        }
        fn retains_encoded_path(&self, value: EncodedValueOccurrence<'_>) -> bool {
            if value.path.is_empty() {
                self.retains_encoded_column(value.slot, value.carrier)
            } else {
                self.nested
                    && value.occurrence == 0
                    && value.path == [ArrayPathStep::StructField(0)]
            }
        }
    }
    fn source() -> Chunk {
        let pure = ValueTypeStorageLifetime::pure();
        let dictionary: ArrayRef = Arc::new(
            vec![Some("{}"), None]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let sibling: ArrayRef = Arc::new(Int32Array::from(vec![7, 8]));
        let fields = vec![
            Arc::new(Field::new("payload", dictionary.data_type().clone(), true)),
            Arc::new(Field::new("row", DataType::Int32, false)),
        ];
        let array: ArrayRef =
            Arc::new(StructArray::try_new(fields.into(), vec![dictionary, sibling], None).unwrap());
        let json = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            true,
            pure.clone(),
        )
        .unwrap();
        let row =
            ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, pure.clone()).unwrap();
        let value = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![
                    ValueField::new("payload", json, pure.clone()),
                    ValueField::new("row", row, pure.clone()),
                ]
                .into_boxed_slice(),
            ),
            false,
            pure.clone(),
        )
        .unwrap();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(array.data_type(), pure.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("output", array.data_type().clone(), false)),
            binding,
            None,
            pure.clone(),
        )
        .unwrap();
        Chunk::try_new_typed_with_columns_owned_observed::<
            crate::runtime::task::ExecutionFailure,
            _,
        >(
            Arc::new(ChunkSchema::try_new_typed(vec![slot], pure.clone()).unwrap()),
            vec![array],
            2,
            pure,
            |_| Ok(()),
        )
        .unwrap()
    }
    fn consumer(drops: &Arc<AtomicUsize>, nested: bool, reject_requests: bool) -> Consumer {
        Consumer {
            factory: Some(Factory {
                drops: drops.clone(),
                begins: AtomicUsize::new(0),
                factory_requests: AtomicUsize::new(0),
                requests: Arc::new(AtomicUsize::new(0)),
                inspected: Arc::new(AtomicUsize::new(0)),
                reject_requests,
                control_error: None,
            }),
            nested,
        }
    }
    #[test]
    fn typed_consumer_requires_an_installed_construction_owner() {
        let source = source();
        let op = Consumer {
            factory: None,
            nested: false,
        };
        assert!(
            matches!(hydrate_for_downstream_with_factory(&source, &op, op.factory.as_ref().map(|factory| factory as &dyn InputConstructionFactory), &mut novarocks_value_arrays::decode::ValueConversionContext::default()), Err(error) if matches!(error.cause(), crate::runtime::task::ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)))
        );
    }
    #[test]
    fn typed_delivery_uses_nested_capability_and_keeps_actual_owner_until_drop() {
        let source = source();
        let drops = Arc::new(AtomicUsize::new(0));
        let op = consumer(&drops, false, false);
        let result = hydrate_for_downstream_with_factory(
            &source,
            &op,
            op.factory
                .as_ref()
                .map(|factory| factory as &dyn InputConstructionFactory),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        )
        .unwrap();
        let array = result.columns()[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(array.column(0).data_type(), &DataType::Utf8);
        assert_eq!(
            array
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "{}"
        );
        let original = source.columns()[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(Arc::ptr_eq(array.column(1), original.column(1)));
        assert!(
            result.chunk_schema().slots()[0]
                .typed_binding()
                .same_semantics(source.chunk_schema().slots()[0].typed_binding())
        );
        assert!(op.factory.as_ref().unwrap().requests.load(Ordering::SeqCst) > 0);
        assert!(
            op.factory
                .as_ref()
                .unwrap()
                .inspected
                .load(Ordering::SeqCst)
                > 0
        );
        drop(op);
        drop(source);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(result);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn typed_delivery_preserves_only_the_exact_nested_capability() {
        let source = source();
        let drops = Arc::new(AtomicUsize::new(0));
        let op = consumer(&drops, true, false);
        let result = hydrate_for_downstream_with_factory(
            &source,
            &op,
            op.factory
                .as_ref()
                .map(|factory| factory as &dyn InputConstructionFactory),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&source.columns()[0], &result.columns()[0]));
        assert_eq!(
            op.factory.as_ref().unwrap().begins.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            op.factory
                .as_ref()
                .unwrap()
                .factory_requests
                .load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            op.factory.as_ref().unwrap().requests.load(Ordering::SeqCst),
            0
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn typed_delivery_refuses_the_first_scaffold_request_without_replacing_source() {
        let source = source();
        let original = source.columns()[0].clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let op = consumer(&drops, false, true);
        assert!(
            matches!(hydrate_for_downstream_with_factory(&source, &op, op.factory.as_ref().map(|factory| factory as &dyn InputConstructionFactory), &mut novarocks_value_arrays::decode::ValueConversionContext::default()), Err(error) if matches!(error.cause(), crate::runtime::task::ExecutionFailureCause::Kernel(novarocks_functions::KernelFailure::ResourceExhausted)))
        );
        assert_eq!(
            op.factory.as_ref().unwrap().requests.load(Ordering::SeqCst),
            1
        );
        assert!(Arc::ptr_eq(&original, &source.columns()[0]));
    }
    #[test]
    fn each_actual_input_construction_has_an_independent_retained_scope() {
        let source = source();
        let drops = Arc::new(AtomicUsize::new(0));
        let op = consumer(&drops, false, false);
        let first = hydrate_for_downstream_with_factory(
            &source,
            &op,
            op.factory
                .as_ref()
                .map(|factory| factory as &dyn InputConstructionFactory),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        )
        .unwrap();
        let second = hydrate_for_downstream_with_factory(
            &source,
            &op,
            op.factory
                .as_ref()
                .map(|factory| factory as &dyn InputConstructionFactory),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        )
        .unwrap();
        assert_eq!(
            op.factory.as_ref().unwrap().begins.load(Ordering::SeqCst),
            2
        );
        assert_eq!(
            op.factory
                .as_ref()
                .unwrap()
                .factory_requests
                .load(Ordering::SeqCst),
            4
        );
        drop(op);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(first);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(second.len(), 2);
        drop(second);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn demand_probe_preserves_control_refusal_before_creating_a_scope() {
        use novarocks_type_contract::CompileControlError;
        let source = source();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let drops = Arc::new(AtomicUsize::new(0));
            let mut op = consumer(&drops, false, false);
            op.factory.as_mut().unwrap().control_error = Some(cause.clone());
            assert!(
                matches!(hydrate_for_downstream_with_factory(&source, &op, op.factory.as_ref().map(|factory| factory as &dyn InputConstructionFactory), &mut novarocks_value_arrays::decode::ValueConversionContext::default()), Err(error) if matches!(error.cause(), crate::runtime::task::ExecutionFailureCause::Construction(ConstructionFailure::Control(actual)) if *actual == cause))
            );
            assert_eq!(
                op.factory.as_ref().unwrap().begins.load(Ordering::SeqCst),
                0
            );
            assert_eq!(drops.load(Ordering::SeqCst), 0);
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::{
        Chunk, InputEncodingPolicy, dictionary_carrier_stats, forward_observable,
        hydrate_for_downstream_with_factory,
    };
    use crate::exec::pipeline::schedule::observer::Observable;
    use arrow::array::{Array, ArrayRef, DictionaryArray, StringArray};
    use arrow::datatypes::{DataType, Int32Type};
    use novarocks_types::SlotId;

    struct StubOp;

    impl InputEncodingPolicy for StubOp {}

    struct KeepSlot1Op;

    impl InputEncodingPolicy for KeepSlot1Op {
        fn retains_encoded_column(&self, slot_id: SlotId, data_type: &DataType) -> bool {
            slot_id == SlotId::new(1)
                && matches!(
                    data_type,
                    DataType::Dictionary(key_type, value_type)
                        if key_type.as_ref() == &DataType::Int32
                            && value_type.as_ref() == &DataType::Utf8
                )
        }
    }

    fn dict_utf8_with_nulls_and_empty() -> ArrayRef {
        Arc::new(
            vec![Some("PAID"), None, Some(""), Some("NEW")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }

    fn two_dictionary_column_chunk() -> Chunk {
        use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        let columns = vec![
            dict_utf8_with_nulls_and_empty(),
            dict_utf8_with_nulls_and_empty(),
        ];
        let storage = ValueTypeStorageLifetime::pure();
        // Semantics and frozen layouts are authored independently. Fields
        // describe the actual encoded batch, never a compiled static layout.
        let declaration = FunctionValueType::new(DataType::Utf8, true);
        let slots = columns
            .iter()
            .zip(["status_1", "status_2"])
            .enumerate()
            .map(|(i, (array, name))| {
                ChunkSlotSchema::try_new_typed(
                    SlotId::new(i as u32 + 1),
                    Arc::new(arrow::datatypes::Field::new(
                        name,
                        array.data_type().clone(),
                        true,
                    )),
                    declaration.typed_binding().clone(),
                    None,
                    storage.clone(),
                )
                .unwrap()
            })
            .collect();
        let schema = Arc::new(ChunkSchema::try_new_typed(slots, storage).unwrap());
        let factory = fixture_factory();
        let owner = factory.begin_typed_construction(&schema).unwrap();
        Chunk::try_new_typed_with_columns_owned_observed::<super::ConstructionFailure, _>(
            schema,
            columns,
            4,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap()
    }
    fn fixture_factory() -> Arc<dyn super::InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("typed receiver fixture"),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        )
        .unwrap()
    }

    fn assert_utf8_values(column: &ArrayRef) {
        let values = column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(values.value(0), "PAID");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "");
        assert_eq!(values.value(3), "NEW");
    }

    #[test]
    fn processor_operator_rejects_physical_encodings_by_default() {
        let op = StubOp;
        let slot_id = SlotId::new(1);
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        assert!(!op.retains_encoded_column(slot_id, &dictionary_type));
        assert!(!op.retains_encoded_column(slot_id, &DataType::Utf8));
    }

    #[test]
    fn hydrate_for_downstream_hydrates_all_dictionary_columns_by_default() {
        let chunk = two_dictionary_column_chunk();
        let op = StubOp;

        let hydrated = hydrate_for_downstream_with_factory(
            &chunk,
            &op,
            Some(fixture_factory().as_ref()),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        )
        .expect("hydrate for downstream");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::Utf8);
        assert_eq!(hydrated.columns()[1].data_type(), &DataType::Utf8);
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(1))
                .expect("slot 1")
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(2))
                .expect("slot 2")
                .data_type(),
            &DataType::Utf8
        );
        assert_utf8_values(&hydrated.columns()[0]);
        assert_utf8_values(&hydrated.columns()[1]);
    }

    #[test]
    fn hydrate_for_downstream_keeps_only_declared_encoded_slots() {
        let chunk = two_dictionary_column_chunk();
        let op = KeepSlot1Op;

        let hydrated = hydrate_for_downstream_with_factory(
            &chunk,
            &op,
            Some(fixture_factory().as_ref()),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        )
        .expect("hydrate for downstream");

        assert!(matches!(
            hydrated.columns()[0].data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));
        assert!(matches!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(1))
                .expect("slot 1")
                .data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));
        assert_eq!(hydrated.columns()[1].data_type(), &DataType::Utf8);
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(2))
                .expect("slot 2")
                .data_type(),
            &DataType::Utf8
        );
        assert_utf8_values(&hydrated.columns()[1]);
    }

    #[test]
    fn root_encoded_capability_never_authorizes_a_nested_occurrence() {
        use super::EncodedValueOccurrence;
        use crate::exec::chunk::ArrayPathStep;
        let operator = KeepSlot1Op;
        let carrier = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert!(operator.retains_encoded_path(EncodedValueOccurrence {
            slot: SlotId::new(1),
            occurrence: 0,
            path: &[],
            carrier: &carrier,
        }));
        assert!(!operator.retains_encoded_path(EncodedValueOccurrence {
            slot: SlotId::new(1),
            occurrence: 0,
            path: &[ArrayPathStep::StructField(0)],
            carrier: &carrier,
        }));
    }

    #[test]
    fn dictionary_carrier_stats_hydrates_all_dictionary_columns_by_default() {
        let chunk = two_dictionary_column_chunk();
        let op = StubOp;

        let stats = dictionary_carrier_stats(&chunk, &op);

        assert_eq!(stats.input_rows, 8);
        assert_eq!(stats.input_columns, 2);
        assert_eq!(stats.kept_rows, 0);
        assert_eq!(stats.kept_columns, 0);
        assert_eq!(stats.hydrated_rows, 8);
        assert_eq!(stats.hydrated_columns, 2);
        assert_eq!(stats.unsupported_columns, 2);
    }

    #[test]
    fn dictionary_carrier_stats_keeps_declared_encoded_slot() {
        let chunk = two_dictionary_column_chunk();
        let op = KeepSlot1Op;

        let stats = dictionary_carrier_stats(&chunk, &op);

        assert_eq!(stats.input_rows, 8);
        assert_eq!(stats.input_columns, 2);
        assert_eq!(stats.kept_rows, 4);
        assert_eq!(stats.kept_columns, 1);
        assert_eq!(stats.hydrated_rows, 4);
        assert_eq!(stats.hydrated_columns, 1);
        assert_eq!(stats.unsupported_columns, 1);
    }

    #[test]
    fn observable_forwarding_does_not_retain_the_target() {
        let source = Arc::new(Observable::new());
        let target = Arc::new(Observable::new());
        let target_weak = Arc::downgrade(&target);
        forward_observable(&source, &target);

        drop(target);
        source.notify_observers();

        assert!(target_weak.upgrade().is_none());
    }
}
