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

//! Grouped physical positions on the file's original driver, sealed only by
//! local exchange EOS. Each emitted row is one complete Roaring container.

use std::alloc::Layout;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use allocator_api2::alloc::{AllocError, Allocator};
use allocator_api2::boxed::Box as StateBox;
use allocator_api2::vec::Vec as StateVec;
use arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, StringArray, StructArray, UInt64Array,
};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::DataType;
use hashbrown::HashMap;
use hashbrown::hash_map::{DefaultHashBuilder, IntoIter};
use novarocks_connector_contract::PositionShardWindow;
use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
use novarocks_functions::selected_copy::{self, CopyConstructionError, CopyIndexView};
use novarocks_functions::{AggregateStateAllocator, KernelDiagnostic, KernelFailure};
use novarocks_local_program::StaticLayout;
use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
use roaring::RoaringBitmap;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::pipeline::driver_step::DriverStep;
use crate::exec::pipeline::input::PreparedInput;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner, InputEncodingPolicy,
    Operator, OperatorLifecycle,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{Transform, TransformKernel};
use crate::exec::pipeline::step::{NextStep, StepResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

use super::compiled_expression::RuntimeKernelControl;

#[cfg(test)]
mod tests;

const WINDOW_BYTES: usize = (1 << 16) / 8;

/// Bridge collection allocation to the real operator state allocator. Keep the
/// original capacity cause when a collection API reports only AllocError.
#[derive(Clone)]
struct StateAllocator {
    host: Arc<dyn AggregateStateAllocator>,
    failure: Arc<Mutex<Option<KernelFailure>>>,
}

impl StateAllocator {
    fn new(host: Arc<dyn AggregateStateAllocator>) -> Self {
        Self {
            host,
            failure: Arc::new(Mutex::new(None)),
        }
    }

    fn allocation_failure(&self) -> ExecutionFailure {
        self.failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .unwrap_or(KernelFailure::ResourceExhausted)
            .into()
    }
}

// SAFETY: nonempty blocks and their exact layouts stay with the original host.
// Empty allocations use only an aligned, non-dereferenceable pointer.
unsafe impl Allocator for StateAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() == 0 {
            return Ok(NonNull::slice_from_raw_parts(
                NonNull::without_provenance(std::num::NonZeroUsize::new(layout.align()).unwrap()),
                0,
            ));
        }
        match self.host.allocate(layout) {
            Ok(block) => Ok(NonNull::slice_from_raw_parts(block, layout.size())),
            Err(error) => {
                self.failure
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .get_or_insert(error);
                Err(AllocError)
            }
        }
    }

    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        if layout.size() != 0 {
            // SAFETY: this is the exact nonempty block returned by the host.
            unsafe { self.host.release(pointer, layout) };
        }
    }
}

type StateMap<K, V> = HashMap<K, V, DefaultHashBuilder, StateAllocator>;
type Bitmap = StateBox<[u8; WINDOW_BYTES], StateAllocator>;

struct FilePath(StateVec<u8, StateAllocator>);
impl FilePath {
    fn new(path: &str, allocator: &StateAllocator) -> ExecutionResult<Self> {
        let mut bytes = StateVec::new_in(allocator.clone());
        bytes
            .try_reserve_exact(path.len())
            .map_err(|_| allocator.allocation_failure())?;
        bytes.extend_from_slice(path.as_bytes());
        Ok(Self(bytes))
    }
}
impl Borrow<str> for FilePath {
    fn borrow(&self) -> &str {
        // SAFETY: the only constructor copies the exact bytes of a &str.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}
impl Hash for FilePath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Borrow::<str>::borrow(self).hash(state);
    }
}
impl PartialEq for FilePath {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_slice() == other.0.as_slice()
    }
}
impl Eq for FilePath {}

struct PartitionRow {
    array: ArrayRef,
    storage: ValueTypeStorageLifetime,
}

struct FilePositions {
    spec_id: i32,
    partition: PartitionRow,
    windows: StateMap<PositionShardWindow, Bitmap>,
}

struct FileDrain {
    path: FilePath,
    spec_id: i32,
    partition: PartitionRow,
    windows: allocator_api2::vec::IntoIter<(PositionShardWindow, Bitmap), StateAllocator>,
    count: i64,
    index: i64,
}

struct FileAccumulator {
    allocator: StateAllocator,
    files: StateMap<FilePath, FilePositions>,
}
impl FileAccumulator {
    fn new(allocator: StateAllocator) -> Self {
        Self {
            files: StateMap::new_in(allocator.clone()),
            allocator,
        }
    }

    fn insert(
        &mut self,
        path: &str,
        spec_id: i32,
        position: i64,
        partition_matches: impl FnOnce(&PartitionRow) -> ExecutionResult<bool>,
        copy_partition: impl FnOnce() -> ExecutionResult<PartitionRow>,
    ) -> ExecutionResult<()> {
        let (window, low32) = PositionShardWindow::from_position(position)
            .ok_or_else(|| invalid("Position contribution has a negative row position"))?;
        if let Some(file) = self.files.get_mut(path) {
            if file.spec_id != spec_id || !partition_matches(&file.partition)? {
                return Err(invalid(
                    "Conflicting position metadata for the same data file",
                ));
            }
            insert_position(file, window, low32, &self.allocator)?;
        } else {
            self.files
                .try_reserve(1)
                .map_err(|_| self.allocator.allocation_failure())?;
            let mut file = FilePositions {
                spec_id,
                partition: copy_partition()?,
                windows: StateMap::new_in(self.allocator.clone()),
            };
            insert_position(&mut file, window, low32, &self.allocator)?;
            self.files
                .insert(FilePath::new(path, &self.allocator)?, file);
        }
        Ok(())
    }
}

fn insert_position(
    file: &mut FilePositions,
    window: PositionShardWindow,
    low32: u32,
    allocator: &StateAllocator,
) -> ExecutionResult<()> {
    if !file.windows.contains_key(&window) {
        file.windows
            .try_reserve(1)
            .map_err(|_| allocator.allocation_failure())?;
        let bitmap = StateBox::try_new_in([0; WINDOW_BYTES], allocator.clone())
            .map_err(|_| allocator.allocation_failure())?;
        file.windows.insert(window, bitmap);
    }
    let bitmap = file
        .windows
        .get_mut(&window)
        .expect("window has been installed");
    let position = low32 as u16;
    bitmap[usize::from(position / 8)] |= 1 << (position % 8);
    Ok(())
}

pub(crate) struct PositionShardFactory {
    output: StaticLayout,
    error: Arc<RuntimeErrorState>,
    dop: i32,
}
impl PositionShardFactory {
    pub(crate) fn new(output: StaticLayout, error: Arc<RuntimeErrorState>, dop: i32) -> Self {
        Self { output, error, dop }
    }
}
impl OperatorFactory for PositionShardFactory {
    fn name(&self) -> &str {
        "POSITION_SHARD"
    }

    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        if dop != self.dop || dop <= 0 {
            return Err(invalid(
                "Position shard pipeline differs from its frozen author width",
            ));
        }
        Ok(())
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(PositionShard {
            layout: self.output.clone(),
            driver_ordinal: i64::from(driver_id),
            #[cfg(novarocks_dev_controls)]
            observation: None,
            #[cfg(novarocks_dev_controls)]
            emitted_rows: 0,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            factory: None,
            output: None,
            input: None,
            collecting: None,
            draining: None,
            current: None,
        }))
    }
}

struct PositionShard {
    layout: StaticLayout,
    driver_ordinal: i64,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::runtime::runtime_state::PositionShardTestObservation>,
    #[cfg(novarocks_dev_controls)]
    emitted_rows: u64,
    control: RuntimeKernelControl,
    factory: Option<Arc<dyn InputConstructionFactory>>,
    output: Option<ChunkSchemaRef>,
    input: Option<(Chunk, usize)>,
    collecting: Option<FileAccumulator>,
    draining: Option<IntoIter<FilePath, FilePositions, StateAllocator>>,
    current: Option<FileDrain>,
}

impl PositionShard {
    fn factory(&self) -> ExecutionResult<&dyn InputConstructionFactory> {
        self.factory
            .as_deref()
            .ok_or(ConstructionFailure::MissingOwner.into())
    }

    fn clear(&mut self) {
        self.input = None;
        self.collecting = None;
        self.current = None;
        self.draining = None;
    }

    fn collect(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        let (chunk, mut row) = self
            .input
            .take()
            .ok_or_else(|| invalid("Missing position input cursor"))?;
        let [files, positions, specs, partitions] = chunk.columns() else {
            return Err(invalid("Position contribution width differs"));
        };
        let files = files
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("Position contribution file is not Utf8"))?;
        let positions = positions
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("Position contribution position is not Int64"))?;
        let specs = specs
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| invalid("Position contribution spec ID is not Int32"))?;
        let partitions = partitions
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| invalid("Position contribution partition is not a struct"))?;
        let start = row;
        let factory = self
            .factory
            .as_deref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let accumulator = self
            .collecting
            .as_mut()
            .ok_or_else(|| invalid("Position contribution arrived after EOS"))?;
        while row < chunk.len() && ctx.work().try_consume(1) {
            ctx.checkpoint()?;
            if files.is_null(row)
                || positions.is_null(row)
                || specs.is_null(row)
                || partitions.is_null(row)
                || files.value(row).is_empty()
                || specs.value(row) < 0
            {
                return Err(invalid(
                    "Selected position contribution is missing exact file metadata",
                ));
            }
            accumulator.insert(
                files.value(row),
                specs.value(row),
                positions.value(row),
                |stored| partition_matches(factory, &chunk, stored, partitions, row),
                || copy_partition(factory, &chunk, partitions, row, ctx),
            )?;
            row += 1;
        }
        let committed = (row - start) as u64;
        if row < chunk.len() {
            self.input = Some((chunk, row));
            Ok(StepResult::new(None, NextStep::Yield, committed))
        } else {
            Ok(StepResult::new(None, NextStep::NeedInput, committed))
        }
    }

    #[cfg(novarocks_dev_controls)]
    fn observe(&self, marker: &str, finished: bool) {
        if let Some(facts) = &self.observation
            && facts.observer.is_active()
        {
            let execution = facts.execution;
            let task_key = facts.task_key;
            let mut event = format!(
                "{marker} execution_id={}:{}:{} task_key={:x}:{:x} driver_id={}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                task_key.high(),
                task_key.low(),
                self.driver_ordinal,
            );
            if finished {
                use std::fmt::Write;
                let _ = write!(event, " emitted_rows={}", self.emitted_rows);
            }
            facts.observer.observe(&event);
        }
    }

    fn emit(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        ctx.checkpoint()?;
        if self.current.is_none() {
            let next = self.draining.as_mut().and_then(Iterator::next);
            let Some((path, file)) = next else {
                self.draining = None;
                #[cfg(novarocks_dev_controls)]
                self.observe(
                    novarocks_failpoint::observation_marker::POSITION_SHARD_DRIVER_FINISHED,
                    true,
                );
                return Ok(StepResult::new(None, NextStep::Finished, 0));
            };
            let allocator = file.windows.allocator().clone();
            let mut windows = StateVec::new_in(allocator.clone());
            windows
                .try_reserve_exact(file.windows.len())
                .map_err(|_| allocator.allocation_failure())?;
            for (ordinal, entry) in file.windows.into_iter().enumerate() {
                if ordinal.is_multiple_of(256) {
                    ctx.checkpoint()?;
                }
                windows.push(entry);
            }
            windows.sort_unstable_by_key(|(window, _)| *window);
            ctx.checkpoint()?;
            self.current = Some(FileDrain {
                path,
                spec_id: file.spec_id,
                partition: file.partition,
                count: i64::try_from(windows.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?,
                windows: windows.into_iter(),
                index: 0,
            });
        }

        let output = self
            .output
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let host = self
            .control
            .allocator()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let mut file = self.current.take().expect("current file is installed");
        let (window, bitmap) = file
            .windows
            .next()
            .ok_or_else(|| invalid("Position shard window is absent"))?;
        let row = build_shard(
            self.factory()?,
            output,
            host,
            &file,
            self.driver_ordinal,
            window,
            &bitmap,
            ctx,
        )?;
        drop(bitmap);
        #[cfg(novarocks_dev_controls)]
        {
            self.emitted_rows = self
                .emitted_rows
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
        file.index += 1;
        if file.index < file.count {
            self.current = Some(file);
        }
        let next = if self.current.is_none()
            && self.draining.as_ref().is_none_or(|files| files.len() == 0)
        {
            self.draining = None;
            #[cfg(novarocks_dev_controls)]
            self.observe(
                novarocks_failpoint::observation_marker::POSITION_SHARD_DRIVER_FINISHED,
                true,
            );
            NextStep::Finished
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(row), next, 0))
    }
}

impl OperatorLifecycle for PositionShard {
    fn name(&self) -> &str {
        "POSITION_SHARD"
    }

    fn creates_typed_storage(&self) -> bool {
        true
    }

    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.factory = Some(factory);
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(tracker);
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.output.is_none() {
            let output = self.factory()?.create_compiled_chunk_schema(&self.layout)?;
            let host = self
                .control
                .allocator()
                .ok_or(ConstructionFailure::MissingOwner)?;
            self.collecting = Some(FileAccumulator::new(StateAllocator::new(host)));
            self.output = Some(output);
        }
        Ok(())
    }

    #[cfg(novarocks_dev_controls)]
    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.observation = state.position_shard_observation.clone();
        self.observe(
            novarocks_failpoint::observation_marker::POSITION_SHARD_DRIVER_OPENED,
            false,
        );
        Ok(())
    }

    fn cancel(&mut self) {
        self.clear();
    }
    fn on_driver_failure(&mut self) {
        self.clear();
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.clear();
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
}

impl InputEncodingPolicy for PositionShard {}
impl TransformKernel for PositionShard {
    fn apply(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        if self.input.is_some() {
            return Err(invalid("Position shard still owns a previous input"));
        }
        self.input = Some((input.into_uncovered_chunk()?, 0));
        self.collect(ctx)
    }

    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if self.input.is_some() {
            self.collect(ctx)
        } else {
            self.emit(ctx)
        }
    }

    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        let collecting = self
            .collecting
            .take()
            .ok_or_else(|| invalid("Position shard received duplicate EOS"))?;
        self.draining = Some(collecting.files.into_iter());
        self.emit(ctx)
    }
}

fn copy_failure(error: CopyConstructionError<ConstructionFailure>) -> ExecutionFailure {
    match error {
        CopyConstructionError::Creator(error) => error.into(),
        CopyConstructionError::Resource(error) => ConstructionFailure::Resource(error).into(),
        CopyConstructionError::Copy(error) => error.into_kernel().into(),
    }
}

fn creation_scope<'a>(
    owner: &'a dyn InputConstructionOwner,
    request: &'a mut impl FnMut(Layout) -> Result<(), ConstructionFailure>,
    opaque: &'a mut impl FnMut(usize) -> Result<(), ConstructionFailure>,
) -> ValueSchemaCreation<'a, ConstructionFailure> {
    ValueSchemaCreation::new(owner.storage_lifetime(), request, opaque)
}

fn partition_matches(
    factory: &dyn InputConstructionFactory,
    source: &Chunk,
    stored: &PartitionRow,
    incoming: &StructArray,
    row: usize,
) -> ExecutionResult<bool> {
    let scratch = factory.begin_input_construction(source)?;
    let mut observe = |boundary: bool| {
        factory
            .observe_input_work(usize::from(!boundary))
            .map_err(ExecutionFailure::from)
    };
    let incoming_metadata =
        novarocks_functions::source_metadata_bytes_observed(incoming, &mut observe)?;
    let stored_metadata =
        novarocks_functions::source_metadata_bytes_observed(stored.array.as_ref(), &mut observe)?;
    scratch.observe_opaque_upper_bound(
        incoming_metadata
            .checked_add(stored_metadata)
            .ok_or(KernelFailure::ResourceExhausted)?,
    )?;
    Ok(stored.array.to_data() == incoming.slice(row, 1).to_data())
}

/// Arrow take copies Iceberg's scalar carriers and recursively copies struct
/// children. Keep the original buffer owner for any other carrier, including
/// view and dictionary leaves that Arrow may share with the input.
fn take_owns_partition_buffers(ty: &DataType) -> bool {
    match ty {
        DataType::Struct(fields) => fields
            .iter()
            .all(|field| take_owns_partition_buffers(field.data_type())),
        DataType::Boolean | DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_) => {
            true
        }
        _ => ty.is_primitive(),
    }
}

fn copy_partition(
    factory: &dyn InputConstructionFactory,
    source: &Chunk,
    array: &StructArray,
    row: usize,
    ctx: &DriverStep<'_>,
) -> ExecutionResult<PartitionRow> {
    let temporary = factory.begin_input_construction(source)?;
    let retained = factory.begin_input_construction(source)?;
    let mut temp_request = |layout| temporary.request(layout);
    let mut temp_opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
    let mut result_request = |layout| retained.request(layout);
    let mut result_opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
    let mut scratch = creation_scope(temporary.as_ref(), &mut temp_request, &mut temp_opaque);
    let mut result = creation_scope(retained.as_ref(), &mut result_request, &mut result_opaque);
    temporary.request(Layout::new::<u64>())?;
    selected_copy::preflight_uint64_indices_construction(&mut scratch, &ctx.kernel_control())
        .map_err(copy_failure)?;
    let indices = UInt64Array::from(vec![row as u64]);
    selected_copy::preflight_take_construction_observed(
        array,
        CopyIndexView::UInt64(&indices),
        &mut scratch,
        &mut result,
        &mut |boundary| factory.observe_input_work(usize::from(!boundary)),
    )
    .map_err(copy_failure)?;
    let selected = arrow::compute::take(array, &indices, None)
        .map_err(|error| invalid(format!("Cannot retain exact file partition: {error}")))?;
    ctx.checkpoint()?;
    let storage = retained.storage_lifetime();
    let storage = if take_owns_partition_buffers(array.data_type()) {
        storage
            .retaining_observed::<ConstructionFailure>(
                &source.array_type_storage_lifetime(),
                |layout| retained.request(layout),
            )?
            .retaining_observed::<ConstructionFailure>(
                &source.chunk_schema().storage_lifetime(),
                |layout| retained.request(layout),
            )?
    } else {
        source.source_storage_lifetime_observed::<ConstructionFailure>(storage, |layout| {
            retained.request(layout)
        })?
    };
    Ok(PartitionRow {
        array: selected,
        storage,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_shard(
    factory: &dyn InputConstructionFactory,
    schema: &ChunkSchemaRef,
    host: Arc<dyn AggregateStateAllocator>,
    file: &FileDrain,
    driver: i64,
    window: PositionShardWindow,
    bitmap: &[u8; WINDOW_BYTES],
    ctx: &DriverStep<'_>,
) -> ExecutionResult<Chunk> {
    let output = factory.begin_typed_construction(schema)?;
    output.request(
        Layout::array::<ArrayRef>(9)
            .map_err(|_| LayoutResourceError::Arithmetic)
            .map_err(ConstructionFailure::from)?,
    )?;
    let mut request = |layout| output.request(layout);
    let mut opaque = |bytes| output.observe_opaque_upper_bound(bytes);
    let mut creation = creation_scope(output.as_ref(), &mut request, &mut opaque);
    for ordinal in [0, 1, 3, 4, 5, 6, 7, 8] {
        selected_copy::preflight_flat_array_structure_construction(
            schema.slots()[ordinal].typed_binding().layout(),
            false,
            &mut creation,
            &mut |boundary| factory.observe_input_work(usize::from(!boundary)),
        )
        .map_err(copy_failure)?;
    }
    output.request(Layout::new::<[i32; 7]>())?;
    output.request(Layout::new::<[i64; 3]>())?;
    let path: &str = file.path.borrow();
    let path_len = i32::try_from(path.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
    output.request(
        Layout::array::<u8>(path.len())
            .map_err(|_| LayoutResourceError::Arithmetic)
            .map_err(ConstructionFailure::from)?,
    )?;
    let mut path_bytes = Vec::new();
    path_bytes
        .try_reserve_exact(path.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    path_bytes.extend_from_slice(path.as_bytes());
    let file_array = StringArray::new(
        OffsetBuffer::new(vec![0, path_len].into()),
        Buffer::from_vec(path_bytes),
        None,
    );

    // The aligned, full single-container API allocates one container descriptor
    // and either a dense 8 KiB box or a smaller sparse u16 array in roaring
    // 0.10.12. No incremental sparse-to-dense conversion is needed here.
    let opaque = OpaqueRetainedCharge::try_new(host)?;
    let _roaring = opaque.reserve_operation(WINDOW_BYTES + size_of::<[usize; 8]>())?;
    let roaring = RoaringBitmap::from_lsb0_bytes(u32::from(window.container_key()) << 16, bitmap);
    if roaring.is_empty() {
        return Err(invalid("Position shard encoded an empty container"));
    }
    let size = roaring.serialized_size();
    output.request(
        Layout::array::<u8>(size)
            .map_err(|_| LayoutResourceError::Arithmetic)
            .map_err(ConstructionFailure::from)?,
    )?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    roaring
        .serialize_into(&mut bytes)
        .map_err(|error| invalid(format!("Cannot serialize position shard: {error}")))?;
    drop(roaring);
    drop(_roaring);
    let binary_len = i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
    let binary = BinaryArray::new(
        OffsetBuffer::new(vec![0, binary_len].into()),
        Buffer::from_vec(bytes),
        None,
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(file_array),
        Arc::new(Int32Array::from(vec![file.spec_id])),
        Arc::clone(&file.partition.array),
        Arc::new(Int64Array::from(vec![driver])),
        Arc::new(Int32Array::from(vec![window.high32() as i32])),
        Arc::new(Int32Array::from(vec![i32::from(window.container_key())])),
        Arc::new(Int64Array::from(vec![file.index])),
        Arc::new(Int64Array::from(vec![file.count])),
        Arc::new(binary),
    ];
    ctx.checkpoint()?;
    let storage = output
        .storage_lifetime()
        .retaining_observed::<ConstructionFailure>(&file.partition.storage, |layout| {
            output.request(layout)
        })?;
    Ok(Chunk::try_new_typed_with_columns_owned_observed::<
        ConstructionFailure,
        _,
    >(Arc::clone(schema), columns, 1, storage, |step| {
        output.observe(0, &[], step)
    })?)
}

fn invalid(message: impl Into<String>) -> ExecutionFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(&message.into())).into()
}
