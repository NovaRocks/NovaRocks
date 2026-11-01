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

//! Shared state foundation for DISTINCT set-operation execution.
//!
//! Responsibilities:
//! - Maintains hash-based row membership and stage coordination for multi-input set operations.
//! - Applies operation-specific probe/update/output semantics through `DistinctSetSemantics`.
//!
//! Key exported interfaces:
//! - Types: `DistinctSetSharedState`, `DistinctSetSemantics`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::hash_table::{
    allocation::{AggregateAllocator, AggregateVec},
    key_index::{
        ExportedKeys, IndexPurpose, KeyId, KeyIndex, KeyInput, KeyMatch, KeySpec, NullPolicy,
        PreparedKeyBatch, hash_batch,
    },
    partition_keys::source_key_owner,
};
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use crate::runtime::fragment::ExecutionFailure;
use crate::runtime::mem_tracker::MemTracker;
use novarocks_functions::KernelEvaluationControl;
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::{
    ValueError, ValueViolation, batch::hash::HashScratch, decode::ValueConversionContext,
    reachability::ReachableRows,
};

use super::failure::{SetStateFailure, fail};
use super::set_op_stage::SetOpStageController;
use crate::runtime::fragment::ExecutionResult;

pub trait DistinctSetSemantics: Clone + Send + Sync + 'static {
    type Marker: Clone + Send + Sync + 'static;

    const NODE_NAME: &'static str;
    const SINK_OPERATOR_NAME: &'static str;
    const SOURCE_OPERATOR_NAME: &'static str;
    const SOURCE_REJECT_INPUT_ERROR: &'static str;
    const SOURCE_SLOT_MISMATCH_PREFIX: &'static str;

    fn new_marker() -> Self::Marker;
    fn apply_probe(marker: &mut Self::Marker, stage: usize);
    fn marker_selected(marker: &Self::Marker, stage_total: usize) -> bool;
}

struct Keys<M> {
    index: KeyIndex,
    markers: AggregateVec<M>,
    allocator: AggregateAllocator,
}
struct DistinctSetInner<M> {
    keys: Option<Keys<M>>,
    failed: Option<ExecutionFailure>,
}
impl<M> DistinctSetInner<M> {
    fn check(&self) -> ExecutionResult<()> {
        match &self.failed {
            Some(cause) => Err(cause.clone()),
            None => Ok(()),
        }
    }
    fn latch<T>(&mut self, result: ExecutionResult<T>) -> ExecutionResult<T> {
        if let Err(cause) = &result {
            self.failed.get_or_insert_with(|| cause.clone());
        }
        result
    }
}

pub(crate) struct SetSelectionCursor {
    next_group: usize,
    indices: AggregateVec<u64>,
    allocator: AggregateAllocator,
    failed: Option<ExecutionFailure>,
}
impl SetSelectionCursor {
    pub(crate) fn new(allocator: AggregateAllocator) -> Self {
        Self {
            next_group: 0,
            indices: AggregateVec::new_in(allocator.clone()),
            allocator,
            failed: None,
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
    pub(crate) fn take_indices(&mut self) -> ExecutionResult<AggregateVec<u64>> {
        self.check()?;
        Ok(std::mem::replace(
            &mut self.indices,
            AggregateVec::new_in(self.allocator.clone()),
        ))
    }
    fn check(&self) -> ExecutionResult<()> {
        match &self.failed {
            Some(cause) => Err(cause.clone()),
            None => Ok(()),
        }
    }
}
pub(crate) enum SetSelectionProgress {
    Ready { exhausted: bool },
    Yield,
}

#[derive(Clone)]
pub(crate) struct DistinctSetSharedState<S: DistinctSetSemantics> {
    controller: SetOpStageController,
    output_chunk_schema: ChunkSchemaRef,
    inner: Arc<Mutex<DistinctSetInner<S::Marker>>>,
    _semantics: PhantomData<S>,
}

impl<S: DistinctSetSemantics> DistinctSetSharedState<S> {
    #[cfg(test)]
    pub(crate) fn new(
        controller: SetOpStageController,
        output_chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self {
            controller,
            output_chunk_schema,
            inner: Arc::new(Mutex::new(DistinctSetInner {
                keys: None,
                failed: None,
            })),
            _semantics: PhantomData,
        }
    }

    pub(crate) fn controller(&self) -> &SetOpStageController {
        &self.controller
    }

    pub(crate) fn output_chunk_schema(&self) -> ChunkSchemaRef {
        Arc::clone(&self.output_chunk_schema)
    }

    /// Input construction and cancellation are the caller's original ports.
    /// This state owns equality classes and markers, never a runtime factory.
    pub(crate) fn ingest_build(
        &self,
        chunk: &Chunk,
        tracker: Arc<MemTracker>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
        conversions: &mut ValueConversionContext,
    ) -> ExecutionResult<()> {
        let mut guard = self.inner.lock().expect("distinct set state lock");
        guard.check()?;
        let result = (|| {
            control.checkpoint(0)?;
            if chunk.is_empty() {
                return Ok(());
            }
            if chunk.columns().is_empty() {
                return Err(fail(SetStateFailure::ZeroColumns { node: S::NODE_NAME }));
            }
            if guard.keys.is_none() {
                let types = schema_types(&self.output_chunk_schema, owner, control)?;
                let allocator =
                    AggregateAllocator::new(MemTracker::new_child("DistinctSetKeyIndex", &tracker));
                let mut nulls = AggregateVec::new_in(allocator.clone());
                nulls.try_reserve_exact(types.len()).map_err(|cause| {
                    allocator.allocation_error("reserve set NULL policy", cause)
                })?;
                for _ in &types {
                    nulls.push(NullPolicy::Equal);
                }
                let spec = KeySpec::new(&types, &nulls, owner, &|| {
                    control.checkpoint(0).map_err(ExecutionFailure::from)
                })?;
                let index = KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner)?;
                guard.keys = Some(Keys {
                    index,
                    markers: AggregateVec::new_in(allocator.clone()),
                    allocator,
                });
            }
            let keys = guard.keys.as_mut().expect("original set keys installed");
            let source_owner = source_key_owner(chunk, owner)?;
            let input = prepare_input(
                chunk,
                keys.index.spec(),
                &tracker,
                source_owner.as_ref(),
                control,
                conversions,
            )?;
            let prepared = PreparedKeyBatch::from_captured(
                keys.index.spec(),
                &input,
                AggregateAllocator::new(tracker.clone()),
                &|| control.checkpoint(0).map_err(ExecutionFailure::from),
            )?;
            let hashes = hash_batch(
                &prepared,
                keys.index.policy(),
                &mut HashScratch::new(),
                owner,
                &|| control.checkpoint(0).map_err(ExecutionFailure::from),
            )?;
            let added = keys
                .index
                .intern_batch(&prepared, &hashes, owner, control)?;
            keys.markers
                .try_reserve(added.new_keys().len())
                .map_err(|cause| {
                    keys.allocator
                        .allocation_error("reserve distinct set markers", cause)
                })?;
            for new_key in added.new_keys() {
                control.checkpoint(0)?;
                if new_key.id.index() != keys.markers.len() {
                    return Err(fail(SetStateFailure::GroupSequence {
                        node: S::NODE_NAME,
                        expected: keys.markers.len(),
                        actual: new_key.id.index(),
                    }));
                }
                keys.markers.push(S::new_marker());
            }
            Ok(())
        })();
        guard.latch(result)
    }

    pub(crate) fn ingest_probe(
        &self,
        stage: usize,
        chunk: &Chunk,
        tracker: Arc<MemTracker>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
        conversions: &mut ValueConversionContext,
    ) -> ExecutionResult<()> {
        let mut guard = self.inner.lock().expect("distinct set state lock");
        guard.check()?;
        let result = (|| {
            control.checkpoint(0)?;
            if chunk.is_empty() {
                return Ok(());
            }
            if chunk.columns().is_empty() {
                return Err(fail(SetStateFailure::ZeroColumns { node: S::NODE_NAME }));
            }
            let Some(keys) = guard.keys.as_mut() else {
                return Ok(());
            };
            let source_owner = source_key_owner(chunk, owner)?;
            let input = prepare_input(
                chunk,
                keys.index.spec(),
                &tracker,
                source_owner.as_ref(),
                control,
                conversions,
            )?;
            let prepared = PreparedKeyBatch::from_captured(
                keys.index.spec(),
                &input,
                AggregateAllocator::new(tracker),
                &|| control.checkpoint(0).map_err(ExecutionFailure::from),
            )?;
            let hashes = hash_batch(
                &prepared,
                keys.index.policy(),
                &mut HashScratch::new(),
                owner,
                &|| control.checkpoint(0).map_err(ExecutionFailure::from),
            )?;
            let found = keys.index.probe_batch(&prepared, &hashes, owner, control)?;
            for found in found.matches() {
                control.checkpoint(0)?;
                let KeyMatch::Found(id) = found else { continue };
                let markers = keys.markers.len();
                let marker = keys.markers.get_mut(id.index()).ok_or_else(|| {
                    fail(SetStateFailure::GroupBounds {
                        node: S::NODE_NAME,
                        group: id.index(),
                        markers,
                    })
                })?;
                S::apply_probe(marker, stage);
            }
            Ok(())
        })();
        guard.latch(result)
    }

    pub(crate) fn finish_stage(
        &self,
        stage: usize,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        self.inner
            .lock()
            .expect("distinct set state lock")
            .check()?;
        self.controller
            .producer_finished(stage)
            .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)
    }

    /// Export only the source cursor's requested dense positions. K preserves
    /// signed target layouts and owns all value copying; no whole-index
    /// snapshot or second Arrow-take implementation remains in this state.
    pub(crate) fn export_selected_positions(
        &self,
        positions: &[u64],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<ExportedKeys> {
        let guard = self.inner.lock().expect("distinct set state lock");
        guard.check()?;
        control.checkpoint(0)?;
        let keys = guard.keys.as_ref().ok_or_else(|| {
            fail(SetStateFailure::GroupBounds {
                node: S::NODE_NAME,
                group: 0,
                markers: 0,
            })
        })?;
        let mut ids = observed_vec::<KeyId>(positions.len(), owner)?;
        for &position in positions {
            control.checkpoint(0)?;
            let group = usize::try_from(position).map_err(|_| {
                ExecutionFailure::from_domain(ValueError::Invariant(ValueViolation::Bounds))
            })?;
            if group >= keys.markers.len() {
                return Err(fail(SetStateFailure::GroupBounds {
                    node: S::NODE_NAME,
                    group,
                    markers: keys.markers.len(),
                }));
            }
            ids.push(keys.index.key_id_at(group)?);
        }
        let targets = schema_types(&self.output_chunk_schema, owner, control)?;
        keys.index.export_keys(&ids, &targets, owner, control)
    }

    /// Retain at most one configured output batch of indices. A CPU yield
    /// preserves both the group cursor and its partial batch on the source.
    pub(crate) fn advance_selection(
        &self,
        cursor: &mut SetSelectionCursor,
        rows: std::num::NonZeroUsize,
        mut interval: impl FnMut() -> ExecutionResult<bool>,
    ) -> ExecutionResult<SetSelectionProgress> {
        cursor.check()?;
        let result: ExecutionResult<SetSelectionProgress> = (|| {
            let stage_total = self.controller.stage_total();
            let guard = self.inner.lock().expect("distinct set state lock");
            guard.check()?;
            let Some(keys) = guard.keys.as_ref() else {
                return Ok(SetSelectionProgress::Ready { exhausted: true });
            };
            while cursor.next_group < keys.markers.len() && cursor.indices.len() < rows.get() {
                if !interval()? {
                    return Ok(SetSelectionProgress::Yield);
                }
                let count = (keys.markers.len() - cursor.next_group)
                    .min(novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize);
                let end = cursor.next_group + count;
                while cursor.next_group < end && cursor.indices.len() < rows.get() {
                    let group_id = cursor.next_group;
                    if S::marker_selected(&keys.markers[group_id], stage_total) {
                        cursor.indices.try_reserve(1).map_err(|cause| {
                            cursor
                                .allocator
                                .allocation_error("reserve distinct set selected positions", cause)
                        })?;
                        cursor.indices.push(group_id as u64);
                    }
                    cursor.next_group += 1;
                }
            }
            Ok(SetSelectionProgress::Ready {
                exhausted: cursor.next_group == keys.markers.len(),
            })
        })();
        if let Err(cause) = &result {
            cursor.failed.get_or_insert_with(|| cause.clone());
        }
        result
    }
}

fn observed_vec<T>(count: usize, owner: &dyn InputConstructionOwner) -> ExecutionResult<Vec<T>> {
    let layout = std::alloc::Layout::array::<T>(count).map_err(|_| {
        ConstructionFailure::from(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?;
    if layout.size() != 0 {
        owner.request(layout)?;
    }
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|source| {
        ExecutionFailure::from_domain(ValueError::Allocation {
            requested: layout.size(),
            source,
        })
    })?;
    Ok(values)
}
fn schema_types(
    schema: &crate::exec::chunk::ChunkSchema,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<Vec<FunctionValueType>> {
    let mut types = observed_vec(schema.slots().len(), owner)?;
    for slot in schema.slots() {
        control.checkpoint(0)?;
        types.push(
            FunctionValueType::try_from_typed_binding(slot.typed_binding().clone())
                .map_err(novarocks_functions::KernelFailure::from)?,
        );
    }
    Ok(types)
}
fn prepare_input(
    chunk: &Chunk,
    spec: &KeySpec,
    tracker: &Arc<MemTracker>,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
    conversions: &mut ValueConversionContext,
) -> ExecutionResult<KeyInput> {
    let bindings = schema_types(chunk.chunk_schema(), owner, control)?;
    let allocator = AggregateAllocator::new(tracker.clone());
    let mut rows = AggregateVec::new_in(allocator.clone());
    rows.try_reserve_exact(chunk.len())
        .map_err(|cause| allocator.allocation_error("reserve set input rows", cause))?;
    for row in 0..chunk.len() {
        if row.is_multiple_of(256) {
            control.checkpoint(0)?;
        }
        rows.push(row);
    }
    let selected = ReachableRows::all(chunk.len())
        .materialize_observed(
            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| control.checkpoint(0).map_err(ExecutionFailure::from),
        )
        .map_err(|cause| match cause {
            novarocks_value_arrays::ValueFailure::Value(cause) => {
                ExecutionFailure::from_domain(cause)
            }
            novarocks_value_arrays::ValueFailure::Caller(cause) => cause,
        })?;
    KeyInput::new_for_spec(
        spec,
        chunk.columns(),
        &bindings,
        &selected,
        &rows,
        conversions,
        owner,
        &|| control.checkpoint(0).map_err(ExecutionFailure::from),
    )
}

#[cfg(test)]
mod key_index_tests {
    //! These validate the existing set-state helpers only. They neither admit
    //! unsupported SQL set shapes nor replace native subquery/set-op coverage.
    use super::super::except_shared::ExceptSemantics;
    use super::*;
    use arrow::{
        array::{ArrayRef, DictionaryArray, Int32Array, StringArray, StringViewArray},
        datatypes::{DataType, Int32Type, Schema},
    };
    use novarocks_functions::KernelFailure;
    use novarocks_types::SlotId;
    struct Control;
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("set key operations never wait")
        }
    }
    fn fixture() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
        let tracker = MemTracker::new_root("set K original fixture");
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default())).unwrap();
        let layout = crate::exec::chunk::typed_fixture::layout(Arc::new(Schema::empty()), &[], &[]);
        let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
        (factory.begin_typed_construction(&schema).unwrap(), tracker)
    }
    fn strings(array: ArrayRef, ty: &FunctionValueType) -> Chunk {
        let rows = array.len();
        crate::exec::hash_table::partition_keys::typed_encoded_tuple_fixture(
            &["k"],
            &[SlotId::new(1)],
            std::slice::from_ref(ty),
            vec![array],
            rows,
        )
    }
    fn shared(chunk: &Chunk) -> DistinctSetSharedState<ExceptSemantics> {
        DistinctSetSharedState::new(
            SetOpStageController::new("existing set helper", vec![1, 1]).unwrap(),
            chunk.chunk_schema_ref(),
        )
    }
    #[test]
    fn changing_dictionary_codes_and_bottom_nulls_keep_dense_markers_and_exact_view_output() {
        let (owner, tracker) = fixture();
        let ty = FunctionValueType::new(DataType::Utf8View, true);
        let first = strings(
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(1), Some(0), Some(1), None, Some(2)]),
                    Arc::new(StringArray::from(vec![Some("beta"), Some("alpha"), None])),
                )
                .unwrap(),
            ),
            &ty,
        );
        let state = shared(&first);
        let mut conversions = ValueConversionContext::default();
        state
            .ingest_build(
                &first,
                tracker.clone(),
                owner.as_ref(),
                &Control,
                &mut conversions,
            )
            .unwrap();
        let next = strings(
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![1, 0]),
                    Arc::new(StringArray::from(vec!["alpha", "gamma"])),
                )
                .unwrap(),
            ),
            &ty,
        );
        state
            .ingest_build(
                &next,
                tracker.clone(),
                owner.as_ref(),
                &Control,
                &mut conversions,
            )
            .unwrap();
        assert_eq!(
            state
                .inner
                .lock()
                .unwrap()
                .keys
                .as_ref()
                .unwrap()
                .markers
                .len(),
            4
        );
        let probe = strings(
            Arc::new(StringViewArray::from(vec![
                Some("beta"),
                None,
                Some("absent"),
            ])),
            &ty,
        );
        state
            .ingest_probe(
                1,
                &probe,
                tracker.clone(),
                owner.as_ref(),
                &Control,
                &mut conversions,
            )
            .unwrap();
        let mut selected = SetSelectionCursor::new(AggregateAllocator::new(tracker.clone()));
        assert!(matches!(
            state
                .advance_selection(
                    &mut selected,
                    std::num::NonZeroUsize::new(8).unwrap(),
                    || Ok(true)
                )
                .unwrap(),
            SetSelectionProgress::Ready { exhausted: true }
        ));
        assert_eq!(selected.indices.as_slice(), &[0, 3]);
        let output = state
            .export_selected_positions(&[3, 0, 3], owner.as_ref(), &Control)
            .unwrap();
        assert_eq!(output.bindings(), std::slice::from_ref(&ty));
        assert_eq!(
            output.arrays()[0]
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("gamma"), Some("alpha"), Some("gamma")]
        );
        drop(output);
        drop(selected);
        drop(state);
        drop(owner);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn empty_build_and_bounded_selection_preserve_original_stage_controller() {
        let (owner, tracker) = fixture();
        let ty = FunctionValueType::new(DataType::Utf8, true);
        let empty = strings(Arc::new(StringArray::from(Vec::<&str>::new())), &ty);
        let state = shared(&empty);
        state
            .ingest_build(
                &empty,
                tracker.clone(),
                owner.as_ref(),
                &Control,
                &mut ValueConversionContext::default(),
            )
            .unwrap();
        assert!(state.inner.lock().unwrap().keys.is_none());
        let mut selection = SetSelectionCursor::new(AggregateAllocator::new(tracker.clone()));
        assert!(matches!(
            state
                .advance_selection(
                    &mut selection,
                    std::num::NonZeroUsize::new(2).unwrap(),
                    || Ok(true)
                )
                .unwrap(),
            SetSelectionProgress::Ready { exhausted: true }
        ));
        assert!(selection.is_empty());
        state.finish_stage(0).unwrap();
        assert!(state.controller().is_stage_ready(1));
        state.finish_stage(1).unwrap();
        assert!(state.controller().is_stage_ready(2));
    }
    struct Stop;
    impl KernelEvaluationControl for Stop {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Err(KernelFailure::DeadlineExceeded)
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("set key operations never wait")
        }
    }
    #[test]
    fn failed_mutation_latches_original_cause_and_never_exposes_marker_prefix_as_output() {
        let (owner, tracker) = fixture();
        let ty = FunctionValueType::new(DataType::Utf8, true);
        let chunk = strings(Arc::new(StringArray::from(vec!["a", "b", "a"])), &ty);
        let state = shared(&chunk);
        let cause = state
            .ingest_build(
                &chunk,
                tracker.clone(),
                owner.as_ref(),
                &Stop,
                &mut ValueConversionContext::default(),
            )
            .unwrap_err();
        assert_eq!(
            state
                .ingest_build(
                    &chunk,
                    tracker.clone(),
                    owner.as_ref(),
                    &Control,
                    &mut ValueConversionContext::default()
                )
                .unwrap_err(),
            cause
        );
        let mut selected = SetSelectionCursor::new(AggregateAllocator::new(tracker.clone()));
        let again = state
            .advance_selection(
                &mut selected,
                std::num::NonZeroUsize::new(2).unwrap(),
                || Ok(true),
            )
            .err()
            .unwrap();
        assert_eq!(again, cause);
    }
    #[test]
    fn signed_type_drift_is_refused_before_marker_change_and_readonly_bad_position_does_not_poison()
    {
        let (owner, tracker) = fixture();
        let ty = FunctionValueType::new(DataType::Utf8, true);
        let first = strings(Arc::new(StringArray::from(vec!["a", "a", "b"])), &ty);
        let state = shared(&first);
        state
            .ingest_build(
                &first,
                tracker.clone(),
                owner.as_ref(),
                &Control,
                &mut ValueConversionContext::default(),
            )
            .unwrap();
        assert!(
            state
                .export_selected_positions(&[2], owner.as_ref(), &Control)
                .is_err()
        );
        let good = state
            .export_selected_positions(&[1, 0], owner.as_ref(), &Control)
            .unwrap();
        assert_eq!(
            good.arrays()[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("b"), Some("a")]
        );
        let other = crate::exec::chunk::typed_fixture::columns(
            &["k"],
            &[SlotId::new(1)],
            &[FunctionValueType::new(DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1]))],
            1,
        );
        assert!(
            state
                .ingest_probe(
                    1,
                    &other,
                    tracker.clone(),
                    owner.as_ref(),
                    &Control,
                    &mut ValueConversionContext::default()
                )
                .is_err()
        );
        assert_eq!(
            state
                .inner
                .lock()
                .unwrap()
                .keys
                .as_ref()
                .unwrap()
                .markers
                .as_slice(),
            &[false, false]
        );
    }
    #[test]
    fn selection_backing_retains_actual_allocator_and_refusal_cannot_publish_a_private_prefix() {
        let (owner, tracker) = fixture();
        let ty = FunctionValueType::new(DataType::Utf8, false);
        let interval = novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize;
        let count = interval * 2 + 7;
        let values = (0..count).map(|i| format!("key-{i}")).collect::<Vec<_>>();
        let chunk = strings(Arc::new(StringArray::from(values)), &ty);
        let state = shared(&chunk);
        state
            .ingest_build(
                &chunk,
                tracker,
                owner.as_ref(),
                &Control,
                &mut ValueConversionContext::default(),
            )
            .unwrap();
        let output_tracker = MemTracker::new_root("actual set selection backing");
        let mut cursor = SetSelectionCursor::new(AggregateAllocator::new(output_tracker.clone()));
        let rows = std::num::NonZeroUsize::new(interval * 2 + 4).unwrap();
        let mut observations = 0;
        assert!(matches!(
            state
                .advance_selection(&mut cursor, rows, || {
                    observations += 1;
                    Ok(observations == 1)
                })
                .unwrap(),
            SetSelectionProgress::Yield
        ));
        assert!(!cursor.is_empty());
        assert!(output_tracker.current() > 0);
        output_tracker
            .install_limit_once(output_tracker.current())
            .unwrap();
        let cause = state
            .advance_selection(&mut cursor, rows, || Ok(true))
            .err()
            .expect("growth must retain old and replacement backing");
        assert_eq!(
            cursor.take_indices().err().unwrap(),
            cause,
            "no private partial batch can escape as success"
        );
        assert!(
            state.inner.lock().unwrap().failed.is_none(),
            "readonly cursor failure never poisons index or markers"
        );
        drop(cursor);
        assert_eq!(output_tracker.current(), 0);
        let new_tracker = MemTracker::new_root("new actual set selection");
        let mut cursor = SetSelectionCursor::new(AggregateAllocator::new(new_tracker.clone()));
        assert!(matches!(
            state
                .advance_selection(
                    &mut cursor,
                    std::num::NonZeroUsize::new(count).unwrap(),
                    || Ok(true)
                )
                .unwrap(),
            SetSelectionProgress::Ready { exhausted: true }
        ));
        let selected = cursor.take_indices().unwrap();
        assert_eq!(selected.len(), count);
        drop(cursor);
        assert!(
            new_tracker.current() > 0,
            "moved positions keep their own original allocator"
        );
        drop(selected);
        assert_eq!(new_tracker.current(), 0);
    }
}
