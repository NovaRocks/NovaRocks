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

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

use novarocks_expression::{
    EvaluationInput, InputColumnProjection, InputSourceStorage, InputStorageFailure,
};

/// The evaluator's borrowed input plus its private Slot-read observation.
#[derive(Clone, Copy)]
pub(super) struct FrameInput<'a> {
    input: EvaluationInput<'a>,
    #[cfg(test)]
    observer: Option<&'a SlotSourceObserver>,
}

impl<'a> FrameInput<'a> {
    pub(super) fn new(input: EvaluationInput<'a>) -> Self {
        Self {
            input,
            #[cfg(test)]
            observer: None,
        }
    }

    #[cfg(test)]
    pub(super) fn observed<'b>(self, observer: &'b SlotSourceObserver) -> FrameInput<'b>
    where
        'a: 'b,
    {
        FrameInput {
            input: self.input,
            observer: Some(observer),
        }
    }

    /// Called after checked_column has reported the original Slot source.
    /// Capture all finite creators, not just native/provider authority.
    pub(super) fn capture_source(
        self,
        allocator: Option<&std::sync::Arc<dyn novarocks_functions::AggregateStateAllocator>>,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<Option<CapturedInputSource>, novarocks_functions::KernelFailure> {
        use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
        use novarocks_functions::{EvaluationCheckpoints, SourceBackingOwner};
        use novarocks_type_contract::ValueTypeStorageLifetime;
        let Some(source) = self.input.source_storage() else {
            return Ok(None);
        };
        let mut work = EvaluationCheckpoints::new(control);
        let mut stock: Option<OpaqueRetainedCharge> = None;
        let mut retained = 0usize;
        let storage = source
            .storage_lifetime_observed(ValueTypeStorageLifetime::pure(), &mut |layout| {
                work.flush()?;
                let host = allocator.ok_or_else(|| {
                    super::invalid(
                        "compiled input finite custody requires its actual allocation host",
                    )
                })?;
                if stock.is_none() {
                    stock = Some(OpaqueRetainedCharge::try_new(host.clone())?);
                }
                let charge = stock
                    .as_mut()
                    .expect("actual host installed for finite custody");
                let mut reservation = charge.reserve_operation(layout.size())?;
                retained = retained
                    .checked_add(layout.size())
                    .ok_or(novarocks_functions::KernelFailure::ResourceExhausted)?;
                charge.reconcile_under_reservation(retained, &mut reservation)?;
                Ok(())
            })
            .map_err(|cause| match cause {
                InputStorageFailure::Kernel(cause) => cause,
                InputStorageFailure::Layout(_) => {
                    novarocks_functions::KernelFailure::ResourceExhausted
                }
            })?;
        // No request callback does not imply pure: a single existing creator
        // hold can be cloned with no new holder allocation.
        if storage.is_pure() {
            work.finish()?;
            return Ok(None);
        }
        let host = allocator.ok_or_else(|| {
            super::invalid("compiled input finite custody requires its actual allocation host")
        })?;
        let backing = SourceBackingOwner::try_from_owned_direct(
            FiniteSourceStorage {
                _storage: storage,
                _stock: stock,
            },
            host.clone(),
            &mut work,
        )?;
        work.finish()?;
        Ok(Some(CapturedInputSource {
            backing,
            host: host.clone(),
        }))
    }

    pub(super) fn batch(self) -> &'a RecordBatch {
        self.input.batch()
    }

    pub(super) fn num_rows(self) -> usize {
        self.batch().num_rows()
    }

    pub(super) fn schema(self) -> arrow::datatypes::SchemaRef {
        self.input.declared_schema()
    }

    fn projection(self) -> Option<&'a std::sync::Arc<InputColumnProjection>> {
        self.input.projection()
    }

    fn projected_ordinals(self) -> Option<&'a [usize]> {
        self.projection().map(|projection| projection.ordinals())
    }
    pub(crate) fn is_projected(self) -> bool {
        self.projection().is_some()
    }
    /// Inspect only the explicit coordinates, without reporting a Slot read.
    pub(crate) fn has_materialized_column(self, ordinal: usize) -> bool {
        match self.projected_ordinals() {
            Some(map) => map.binary_search(&ordinal).is_ok(),
            None => ordinal < self.batch().num_columns(),
        }
    }
    pub(crate) fn columns_with_ordinals(self) -> impl Iterator<Item = (usize, &'a ArrayRef)> {
        let ordinals = self.projected_ordinals();
        self.batch()
            .columns()
            .iter()
            .enumerate()
            .map(move |(physical, array)| (ordinals.map_or(physical, |map| map[physical]), array))
    }

    /// Called only after the original Frame has checked the lexical input
    /// node/role. An absent ordinal remains the original Slot error.
    pub(crate) fn checked_column(self, ordinal: usize) -> Option<BorrowedInputColumn<'a>> {
        let physical = match self.projected_ordinals() {
            Some(map) => map.binary_search(&ordinal).ok()?,
            None => ordinal,
        };
        let array = self.batch().columns().get(physical)?;
        let source = match self.input.source_storage() {
            Some(storage) => BorrowedInputSource::Storage(storage),
            None => BorrowedInputSource::NoStorage,
        };
        let column = BorrowedInputColumn { array, source };
        #[cfg(test)]
        if let Some(observer) = self.observer {
            observer(column);
        }
        Some(column)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum BorrowedInputSource<'a> {
    NoStorage,
    #[allow(
        dead_code,
        reason = "Prepared Slot source view; leaf source consumption is a later slice"
    )]
    Storage(&'a dyn InputSourceStorage),
}

/// A view of the original checked column and its factual source. It never
/// becomes an owned Frame result or changes the current leaf argument ABI.
#[derive(Clone, Copy)]
pub(crate) struct BorrowedInputColumn<'a> {
    array: &'a ArrayRef,
    source: BorrowedInputSource<'a>,
}

impl<'a> BorrowedInputColumn<'a> {
    pub(crate) fn into_parts(self) -> (&'a ArrayRef, BorrowedInputSource<'a>) {
        (self.array, self.source)
    }
}

#[cfg(test)]
pub(crate) type SlotSourceObserver = dyn for<'a> Fn(BorrowedInputColumn<'a>) + Send + Sync;

/// One lazy shared finite source keeper per real Frame evaluation. Every
/// produced Array that can escape a child/Frame retains this same continuation.
pub(super) struct CapturedInputSource {
    backing: novarocks_functions::SourceBackingOwner<FiniteSourceStorage>,
    host: std::sync::Arc<dyn novarocks_functions::AggregateStateAllocator>,
}
impl CapturedInputSource {
    pub(super) fn continuation(
        &self,
        _array: &ArrayRef,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<ArrayContinuation, novarocks_functions::KernelFailure> {
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        work.flush()?;
        // This is the original finite source captured from the actual Chunk,
        // not a replacement inferred from the output Array's equal datatype.
        let result = ArrayContinuation {
            backing: ContinuationBacking::InputStorage(self.backing.clone()),
            host: Some(self.host.clone()),
        };
        work.finish()?;
        Ok(result)
    }

    pub(super) fn retain(
        &self,
        array: ArrayRef,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<ArrayRef, novarocks_functions::KernelFailure> {
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        let prepared = novarocks_functions::PreparedSourceCustody::prepare(&array, &mut work)?;
        let authority =
            novarocks_functions::opaque_memory::OpaqueRetainedCharge::try_new(self.host.clone())?;
        let mut reservation =
            authority.reserve_operation(prepared.request_facts().global_peak_upper())?;
        let array = prepared.retain(&self.backing, &mut reservation, &mut work)?;
        work.finish()?;
        Ok(array)
    }
}

/// A finite continuation of an actual source or creator backing. Copies inherit
/// this owner; an equal current type cannot replace the original creator.
/// Input loans may retain their real Array; fresh result creators retain their
/// actual type backing independently of the newly produced payload buffers.
#[derive(Clone)]
pub(super) struct ArrayContinuation {
    backing: ContinuationBacking,
    host: Option<std::sync::Arc<dyn novarocks_functions::AggregateStateAllocator>>,
}
#[derive(Clone)]
enum ContinuationBacking {
    Array(novarocks_functions::SourceBackingOwner<ArrayRef>),
    InputStorage(novarocks_functions::SourceBackingOwner<FiniteSourceStorage>),
    ResultCreator(novarocks_functions::SourceBackingOwner<novarocks_functions::FunctionValueType>),
    Pair(novarocks_functions::SourceBackingOwner<[ArrayContinuation; 2]>),
    Constant(novarocks_functions::ConstantValue),
    Sources(novarocks_functions::SourceBackingOwner<Vec<ArrayContinuation>>),
}
impl ArrayContinuation {
    /// Borrowing a checked Constant needs no new host request. Only an actual
    /// copy or successful result materialization retains its factual backing.
    pub(super) fn from_constant(
        value: &novarocks_functions::ConstantValue,
        allocator: Option<&std::sync::Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    ) -> Option<Self> {
        let layout = value.value_type().typed_binding().layout();
        let projection = value.pool().array_projection();
        if layout.storage_lifetime().is_pure()
            && layout.arrow_backing_storage_lifetime().is_pure()
            && projection.storage_lifetime().is_pure()
            && value.pool().source_owner().is_none()
        {
            return None;
        }
        Some(Self {
            backing: ContinuationBacking::Constant(value.clone()),
            host: allocator.cloned(),
        })
    }
    pub(super) fn capture(
        source: &ArrayRef,
        host: &std::sync::Arc<dyn novarocks_functions::AggregateStateAllocator>,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<Self, novarocks_functions::KernelFailure> {
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        let backing = novarocks_functions::SourceBackingOwner::try_from_owned_direct(
            source.clone(),
            host.clone(),
            &mut work,
        )?;
        work.finish()?;
        Ok(Self {
            backing: ContinuationBacking::Array(backing),
            host: Some(host.clone()),
        })
    }
    pub(super) fn combine(
        mut sources: Vec<Self>,
        allocator: Option<&std::sync::Arc<dyn novarocks_functions::AggregateStateAllocator>>,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<Option<Self>, novarocks_functions::KernelFailure> {
        if sources.is_empty() {
            return Ok(None);
        }
        if sources.len() == 1 {
            return Ok(sources.pop());
        }
        let host = allocator.ok_or_else(|| {
            super::invalid("compiled finite result sources require their actual allocation host")
        })?;
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        let backing = novarocks_functions::SourceBackingOwner::try_from_owned_direct(
            sources,
            host.clone(),
            &mut work,
        )?;
        work.finish()?;
        Ok(Some(Self {
            backing: ContinuationBacking::Sources(backing),
            host: Some(host.clone()),
        }))
    }
    /// Keep the previous actual source independently of a new result creator.
    /// The fixed pair is paid as one real typed host block; no unadmitted Vec
    /// or whole output Array is introduced to carry these finite owner pins.
    pub(super) fn retaining_previous(
        self,
        previous: Option<Self>,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<Self, novarocks_functions::KernelFailure> {
        let Some(previous) = previous else {
            return Ok(self);
        };
        let host = self.host.clone().ok_or_else(|| {
            super::invalid("compiled creator continuation requires its actual allocation host")
        })?;
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        let backing = novarocks_functions::SourceBackingOwner::try_from_owned_direct(
            [previous, self],
            host.clone(),
            &mut work,
        )?;
        work.finish()?;
        Ok(Self {
            backing: ContinuationBacking::Pair(backing),
            host: Some(host),
        })
    }

    pub(super) fn retain(
        &self,
        output: ArrayRef,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<ArrayRef, novarocks_functions::KernelFailure> {
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        let host = self.host.as_ref().ok_or_else(|| {
            super::invalid("compiled finite Array source requires its actual allocation host")
        })?;
        fn retain<C: Send + Sync + 'static>(
            output: ArrayRef,
            backing: &novarocks_functions::SourceBackingOwner<C>,
            host: &std::sync::Arc<dyn novarocks_functions::AggregateStateAllocator>,
            work: &mut novarocks_functions::EvaluationCheckpoints<'_>,
        ) -> Result<ArrayRef, novarocks_functions::KernelFailure> {
            let prepared = novarocks_functions::PreparedSourceCustody::<C>::prepare(&output, work)?;
            let authority =
                novarocks_functions::opaque_memory::OpaqueRetainedCharge::try_new(host.clone())?;
            let mut reservation =
                authority.reserve_operation(prepared.request_facts().global_peak_upper())?;
            prepared.retain(backing, &mut reservation, work)
        }
        let output = match &self.backing {
            ContinuationBacking::Constant(value) => {
                let backing = novarocks_functions::SourceBackingOwner::try_from_owned_direct(
                    value.clone(),
                    host.clone(),
                    &mut work,
                )?;
                retain(output, &backing, host, &mut work)?
            }
            ContinuationBacking::Array(backing) => retain(output, backing, host, &mut work)?,
            ContinuationBacking::InputStorage(backing) => retain(output, backing, host, &mut work)?,
            ContinuationBacking::ResultCreator(backing) => {
                retain(output, backing, host, &mut work)?
            }
            ContinuationBacking::Pair(backing) => retain(output, backing, host, &mut work)?,
            ContinuationBacking::Sources(backing) => retain(output, backing, host, &mut work)?,
        };
        work.finish()?;
        Ok(output)
    }
}

// Drop the actual finite responsibility before its new metadata stock.
struct FiniteSourceStorage {
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
    _stock: Option<novarocks_functions::opaque_memory::OpaqueRetainedCharge>,
}

/// Authored by the real root/control, never reconstructed by a Functions leaf.
/// The batch is an actual input/filter result; the map gives its original rows.
pub(super) struct ActualInvocationDomain {
    batch: RecordBatch,
    batch_rows: usize,
    rows: Vec<usize>,
    projection: Option<std::sync::Arc<InputColumnProjection>>,
}
impl ActualInvocationDomain {
    pub(super) fn root(
        input: FrameInput<'_>,
        actual: novarocks_functions::Selection<'_>,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<Self, novarocks_functions::KernelFailure> {
        if actual.batch_rows() != input.num_rows() {
            return Err(super::invalid(
                "actual root domain differs from its input extent",
            ));
        }
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        let mut rows = Vec::with_capacity(actual.len());
        for row in actual.iter() {
            rows.push(row);
            work.step()?;
        }
        let batch = if actual.is_all() {
            input.batch().clone()
        } else {
            // This is the selected-root author's actual filter, including
            // contiguous masks. It is never inferred to be an Arrow slice.
            let mask = Self::mask(input.num_rows(), 0..input.num_rows(), &rows, &mut work)?;
            work.flush()?;
            let batch = arrow::compute::filter_record_batch(input.batch(), &mask)
                .map_err(|error| super::invalid(&format!("actual root filter failed: {error}")))?;
            work.flush()?;
            batch
        };
        work.finish()?;
        Ok(Self {
            batch,
            batch_rows: actual.batch_rows(),
            rows,
            projection: input.projection().cloned(),
        })
    }
    fn mask(
        len: usize,
        parent: impl Iterator<Item = usize>,
        rows: &[usize],
        work: &mut novarocks_functions::EvaluationCheckpoints<'_>,
    ) -> Result<arrow::array::BooleanArray, novarocks_functions::KernelFailure> {
        let mut next = rows.iter().copied().peekable();
        let mut values = Vec::with_capacity(len);
        for row in parent {
            let selected = next.peek().is_some_and(|selected| *selected == row);
            if selected {
                next.next();
            }
            values.push(selected);
            work.step()?;
        }
        if next.next().is_some() {
            return Err(super::invalid(
                "authored filter row is outside its actual parent domain",
            ));
        }
        work.flush()?;
        let mask = arrow::array::BooleanArray::from(values);
        work.flush()?;
        Ok(mask)
    }
    /// The caller supplies the actual selected rows of this control phase.
    /// All-true is a real no-filter fast path; mixed always calls Arrow filter.
    pub(super) fn filtered(
        self: &std::sync::Arc<Self>,
        rows: &[usize],
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<std::sync::Arc<Self>, novarocks_functions::KernelFailure> {
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        let mut same = rows.len() == self.rows.len();
        for (&left, &right) in rows.iter().zip(&self.rows) {
            same &= left == right;
            work.step()?;
        }
        if same {
            work.finish()?;
            return Ok(self.clone());
        }
        let mask = Self::mask(self.rows.len(), self.rows.iter().copied(), rows, &mut work)?;
        work.flush()?;
        let batch = arrow::compute::filter_record_batch(&self.batch, &mask)
            .map_err(|error| super::invalid(&format!("actual control filter failed: {error}")))?;
        work.flush()?;
        let mut mapped = Vec::with_capacity(rows.len());
        for &row in rows {
            mapped.push(row);
            work.step()?;
        }
        work.finish()?;
        Ok(std::sync::Arc::new(Self {
            batch,
            batch_rows: self.batch_rows,
            rows: mapped,
            projection: self.projection.clone(),
        }))
    }
    pub(super) fn rows(&self) -> &[usize] {
        &self.rows
    }
    pub(super) fn column(&self, ordinal: usize) -> Option<&ArrayRef> {
        let physical = match &self.projection {
            Some(projection) => projection.ordinals().binary_search(&ordinal).ok()?,
            None => ordinal,
        };
        self.batch.columns().get(physical)
    }
    pub(super) fn selection(
        &self,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<novarocks_functions::Selection<'_>, novarocks_functions::KernelFailure> {
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        let selection = novarocks_functions::Selection::try_sparse_observed(
            self.batch_rows,
            &self.rows,
            || work.step(),
        )?;
        work.finish()?;
        Ok(selection)
    }
    pub(super) fn physical_ordinal(&self, row: usize) -> Option<usize> {
        self.rows.binary_search(&row).ok()
    }
}

/// The canonical result creator is independent of factual input custody.
/// Equal Arrow types do not transfer one creator's lifetime to another.
pub(super) fn retain_result_type(
    array: ArrayRef,
    result_type: &novarocks_functions::FunctionValueType,
    allocator: Option<&std::sync::Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> Result<ArrayRef, novarocks_functions::KernelFailure> {
    retain_result_type_with_creator(array, result_type, allocator, control).map(|(array, _)| array)
}

/// Author the canonical result exactly once and return that SAME created
/// backing as a finite continuation. This pin contains the real result type
/// owner, not its output payload buffers or an equal source's creator.
pub(super) fn retain_result_type_with_creator(
    array: ArrayRef,
    result_type: &novarocks_functions::FunctionValueType,
    allocator: Option<&std::sync::Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> Result<(ArrayRef, Option<ArrayContinuation>), novarocks_functions::KernelFailure> {
    let layout = result_type.typed_binding().layout();
    if layout.storage_lifetime().is_pure() && layout.arrow_backing_storage_lifetime().is_pure() {
        return Ok((array, None));
    }
    let host = allocator.ok_or_else(|| {
        super::invalid("compiled result Arrow creator requires its actual allocation host")
    })?;
    let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
    let backing = novarocks_functions::SourceBackingOwner::try_from_owned_direct(
        result_type.clone(),
        host.clone(),
        &mut work,
    )?;
    let prepared = novarocks_functions::PreparedSourceCustody::prepare(&array, &mut work)?;
    let authority =
        novarocks_functions::opaque_memory::OpaqueRetainedCharge::try_new(host.clone())?;
    let mut reservation =
        authority.reserve_operation(prepared.request_facts().global_peak_upper())?;
    let array = prepared.retain(&backing, &mut reservation, &mut work)?;
    let creator = ArrayContinuation {
        backing: ContinuationBacking::ResultCreator(backing),
        host: Some(host.clone()),
    };
    work.finish()?;
    Ok((array, Some(creator)))
}
