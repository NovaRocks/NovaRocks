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

//! Lifetime transfer for one already-authorized Arrow result backing group.
//! This author changes buffer custody only, never bytes, schema or row meaning.

use crate::ScalarResourceError;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostShared;
use crate::kernel_control::invalid;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::{KernelEvaluationControl, KernelFailure};
use arrow_array::{Array, ArrayRef, make_array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use novarocks_type_contract::owned_resources::layout::arc_layout;
use std::{alloc::Layout, marker::PhantomData};
use std::{any::Any, panic::RefUnwindSafe, sync::Arc};

use crate::source_custody_resources::{
    SourceCustodyRequestFacts, SourceRequestPricing, model_failure,
};

/// Declaration order is significant: all original buffers die before their
/// host charge. Keeping the whole original group alive also prevents releasing
/// one group's admission while a different derived buffer still aliases it.
struct OriginalBacking<C> {
    original: ArrayRef,
    data: ArrayData,
    custody: C,
}
struct BufferCustodian<C>(HostShared<OriginalBacking<C>>);
// The shared block is immutable. Neither observation nor unwinding changes its
// custody; its last-reference Drop destroys backing before releasing admission.
impl<C> RefUnwindSafe for BufferCustodian<C> {}

fn checked_add(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}
fn checked_mul(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_mul(b).ok_or(KernelFailure::ResourceExhausted)
}
fn extent_add(a: usize, b: usize) -> Result<usize, ScalarResourceError> {
    a.checked_add(b).ok_or(ScalarResourceError::Arithmetic)
}
fn extent_mul(a: usize, b: usize) -> Result<usize, ScalarResourceError> {
    a.checked_mul(b).ok_or(ScalarResourceError::Arithmetic)
}

/// Conservative metadata peak of the pinned Arrow 58.4.0 custom-buffer route.
/// Its private Bytes contains pointer, length and the three-word Deallocation
/// (Arrow's own alloc::tests::test_size_of_deallocation proves that bound).
/// Arc headers are two atomic usize counters; padding is covered by one extra
/// machine word per object. No amount here authorizes an allocation itself.
fn custom_buffer_metadata() -> Result<usize, KernelFailure> {
    custom_buffer_metadata_facts().map_err(model_failure)
}
fn custom_buffer_metadata_facts() -> Result<usize, ScalarResourceError> {
    let word = size_of::<usize>();
    // Arc<BufferCustodian> plus Arc<Bytes>, including both reference headers.
    extent_add(
        size_of::<BufferCustodian<OpaqueRetainedCharge>>(),
        extent_mul(word, 11)?,
    )
}

/// A type-structure envelope for the custody route before a result exists.
/// Original scalar output has at most three data buffers and one validity
/// buffer per node; its child tables contain at most one entry per child node.
pub(crate) fn custody_type_metadata_upper_bound(nodes: usize) -> Result<usize, KernelFailure> {
    let concrete_body = [
        size_of::<arrow_array::StructArray>(),
        size_of::<arrow_array::MapArray>(),
        size_of::<arrow_array::LargeListArray>(),
        size_of::<arrow_array::StringArray>(),
        size_of::<arrow_array::Decimal256Array>(),
        size_of::<BufferlessResult<OpaqueRetainedCharge>>(),
    ]
    .into_iter()
    .max()
    .expect("known scalar Arrow bodies");
    let one = checked_add(
        checked_mul(size_of::<ArrayData>(), 4)?,
        checked_add(
            checked_mul(size_of::<Buffer>(), 4)?,
            checked_add(
                checked_mul(custom_buffer_metadata()?, 4)?,
                checked_add(
                    concrete_body,
                    checked_add(size_of::<ArrayRef>(), checked_mul(size_of::<usize>(), 3)?)?,
                )?,
            )?,
        )?,
    )?;
    checked_mul(nodes, one)
}

/// Count only actual Arrow structure. This is not a decoder and does not inspect
/// a logical value, invent a UTF8 row bound, or change selected row addresses.
pub(crate) fn custody_metadata_upper_bound(
    data: &ArrayData,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    custody_metadata_with_backing(data, work, None)
}

/// Optional actual backing observation extends the SAME table visitor. It is
/// used only after an earlier opaque operation grant, never to authorize work.
trait CustodyBackingObservation {
    fn buffer(
        &mut self,
        buffer: &Buffer,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure>;
}
fn custody_metadata_with_backing(
    data: &ArrayData,
    work: &mut EvaluationCheckpoints<'_>,
    mut backing: Option<&mut dyn CustodyBackingObservation>,
) -> Result<usize, KernelFailure> {
    work.step()?;
    let mut bytes = custody_typed_node_metadata_upper_bound(
        data.data_type(),
        data.buffers().len(),
        data.nulls().is_some(),
        data.child_data().len(),
    )?;
    if let Some(observer) = backing.as_deref_mut() {
        for buffer in data.buffers() {
            observer.buffer(buffer, work)?;
        }
        if let Some(nulls) = data.nulls() {
            observer.buffer(nulls.buffer(), work)?;
        }
    }
    for child in data.child_data() {
        let child_bytes = match backing.as_deref_mut() {
            None => custody_metadata_with_backing(child, work, None)?,
            Some(observer) => custody_metadata_with_backing(child, work, Some(observer))?,
        };
        bytes = checked_add(bytes, child_bytes)?;
    }
    Ok(bytes)
}

/// Include the actual indexed Union table allocated by Arrow 58.2.0's
/// UnionArray::from(ArrayData), rather than counting only populated children.
/// This is metadata construction/retention, not payload or a host grant.
pub(crate) fn custody_typed_node_metadata_upper_bound(
    data_type: &DataType,
    data_buffers: usize,
    has_nulls: bool,
    children: usize,
) -> Result<usize, KernelFailure> {
    let base = custody_node_metadata_upper_bound(data_buffers, has_nulls, children)?;
    let DataType::Union(fields, _) = data_type else {
        return Ok(base);
    };
    // Arrow's constructor authors an indexed Vec even when no fields exist.
    // Valid built-in Union carriers use non-negative i8 field ids.
    let mut maximum = 0usize;
    for (id, _) in fields.iter() {
        let id = usize::try_from(id)
            .map_err(|_| invalid("source Union has a negative Arrow field id"))?;
        maximum = maximum.max(id);
    }
    let slots = checked_add(maximum, 1)?;
    // to_data clones the original indexed table; make_array builds the result
    // table. Cover both in the peak envelope and retain that conservative bound.
    let tables = checked_mul(checked_mul(slots, size_of::<Option<ArrayRef>>())?, 2)?;
    let body_growth = size_of::<arrow_array::UnionArray>().saturating_sub(
        [
            size_of::<arrow_array::StructArray>(),
            size_of::<arrow_array::MapArray>(),
            size_of::<arrow_array::LargeListArray>(),
            size_of::<arrow_array::StringArray>(),
            size_of::<arrow_array::Decimal256Array>(),
        ]
        .into_iter()
        .max()
        .expect("same known supported scalar Arrow bodies"),
    );
    checked_add(base, checked_add(tables, body_growth)?)
}

/// ONE table-metadata arithmetic for actual ArrayData and borrowed source facts.
/// It is a conservative pinned-Arrow envelope, never a capacity grant.
pub(crate) fn custody_node_metadata_upper_bound(
    data_buffers: usize,
    has_nulls: bool,
    children: usize,
) -> Result<usize, KernelFailure> {
    custody_node_metadata_facts(data_buffers, has_nulls, children).map_err(model_failure)
}
fn custody_node_metadata_facts(
    data_buffers: usize,
    has_nulls: bool,
    children: usize,
) -> Result<usize, ScalarResourceError> {
    let buffers = extent_add(data_buffers, usize::from(has_nulls))?;
    let vector_bytes = extent_add(
        extent_mul(data_buffers, size_of::<Buffer>())?,
        extent_mul(children, size_of::<ArrayData>())?,
    )?;
    // ArrayDataBuilder, the resulting ArrayData, and the original borrowed data
    // can coexist. make_array may own child ArrayRefs and one concrete body.
    let concrete_body = [
        size_of::<arrow_array::StructArray>(),
        size_of::<arrow_array::MapArray>(),
        size_of::<arrow_array::LargeListArray>(),
        size_of::<arrow_array::StringArray>(),
        size_of::<arrow_array::Decimal256Array>(),
    ]
    .into_iter()
    .max()
    .expect("known supported scalar Arrow bodies");
    let mut bytes = extent_add(extent_mul(size_of::<ArrayData>(), 3)?, vector_bytes)?;
    bytes = extent_add(
        bytes,
        extent_add(concrete_body, extent_mul(size_of::<usize>(), 3)?)?,
    )?;
    bytes = extent_add(bytes, extent_mul(buffers, custom_buffer_metadata_facts()?)?)?;
    bytes = extent_add(bytes, extent_mul(children, size_of::<ArrayRef>())?)?;
    Ok(bytes)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FlatByteKind {
    Binary,
    Utf8,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequestUpper {
    pub(crate) bytes: usize,
    pub(crate) requests: usize,
}
/// Original default byte-builder finish and direct opaque custody only.
/// Payload capacities and allocator metadata have their own source authors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FlatDirectByteFacts {
    pub(crate) finish_global: RequestUpper,
    pub(crate) custody_global: RequestUpper,
    pub(crate) custody_typed: RequestUpper,
    pub(crate) finish_global_peak_extra: usize,
    pub(crate) custody_global_peak_extra: usize,
    pub(crate) custody_typed_peak_extra: usize,
    pub(crate) global_retained_metadata_upper: usize,
    pub(crate) typed_retained_metadata_upper: usize,
    pub(crate) original_metadata_stock_upper: usize,
    pub(crate) settlement_metadata_upper: usize,
}

/// Facts from the actual private direct-opaque types, without constructing an
/// array, charge, holder or fabricated validity bitmap. Validity is an upper
/// shape, not a claim that the original builder will actually create it.
pub(crate) fn direct_flat_byte_facts(
    kind: FlatByteKind,
    validity_upper: bool,
) -> Result<FlatDirectByteFacts, ScalarResourceError> {
    crate::scalar_resources::require_arrow_model()?;
    let shared = |payload: Layout| {
        arc_layout(payload)
            .map(|layout| layout.size())
            .map_err(|_| ScalarResourceError::Arithmetic)
    };
    // Pinned Bytes is bounded by 48/8; this is not its private representation.
    let bytes_arc =
        shared(Layout::from_size_align(48, 8).map_err(|_| ScalarResourceError::Arithmetic)?)?;
    let array_arc = shared(match kind {
        FlatByteKind::Binary => Layout::new::<arrow_array::BinaryArray>(),
        FlatByteKind::Utf8 => Layout::new::<arrow_array::StringArray>(),
    })?;
    let holder = HostShared::<OriginalBacking<OpaqueRetainedCharge>>::allocation_layout().size();
    let custodian = shared(Layout::new::<BufferCustodian<OpaqueRetainedCharge>>())?;
    let buffers = extent_add(2, usize::from(validity_upper))?;
    let b = size_of::<Buffer>();
    let original = extent_add(extent_mul(buffers, bytes_arc)?, array_arc)?;
    let finish = extent_add(
        extent_add(original, extent_mul(4, b)?)?,
        extent_mul(4, size_of::<i32>())?,
    )?;
    let custody = extent_add(
        extent_add(
            extent_mul(6, b)?,
            extent_mul(buffers, extent_add(custodian, bytes_arc)?)?,
        )?,
        array_arc,
    )?;
    // A dependent Arrow build may enable validation. Cover the three original
    // sites conservatively; their scratch lives sequentially, not all at once.
    let validation = Layout::array::<arrow_data::BufferSpec>(2)
        .map_err(|_| ScalarResourceError::Arithmetic)?
        .size();
    Ok(FlatDirectByteFacts {
        finish_global: RequestUpper {
            bytes: extent_add(finish, validation)?,
            requests: extent_add(buffers, 4)?,
        },
        custody_global: RequestUpper {
            bytes: extent_add(custody, extent_mul(2, validation)?)?,
            requests: extent_add(6, extent_mul(2, buffers)?)?,
        },
        custody_typed: RequestUpper {
            bytes: holder,
            requests: 1,
        },
        finish_global_peak_extra: extent_add(finish, validation)?,
        custody_global_peak_extra: extent_add(original, extent_add(custody, validation)?)?,
        custody_typed_peak_extra: holder,
        global_retained_metadata_upper: extent_add(
            extent_add(original, extent_add(array_arc, extent_mul(2, b)?)?)?,
            extent_mul(buffers, extent_add(custodian, bytes_arc)?)?,
        )?,
        typed_retained_metadata_upper: holder,
        original_metadata_stock_upper: original,
        settlement_metadata_upper: custody_node_metadata_facts(2, validity_upper, 0)?,
    })
}

#[cfg(test)]
mod flat_byte_pricing_tests {
    use super::*;
    use arrow_array::builder::{BinaryBuilder, StringBuilder};

    #[test]
    fn direct_flat_byte_zero_rows_keep_original_two_buffers_and_actual_private_holder() {
        for kind in [FlatByteKind::Binary, FlatByteKind::Utf8] {
            let original: ArrayRef = match kind {
                FlatByteKind::Binary => Arc::new(BinaryBuilder::new().finish()),
                FlatByteKind::Utf8 => Arc::new(StringBuilder::new().finish()),
            };
            let data = original.to_data();
            assert_eq!(original.len(), 0);
            assert_eq!(data.buffers().len(), 2);
            assert_eq!(data.buffers()[0].capacity(), 4100);
            assert_eq!(data.buffers()[1].capacity(), 1024);
            assert!(data.nulls().is_none());
            let facts = direct_flat_byte_facts(kind, false).unwrap();
            // Three original validation sites are conservative extra requests.
            assert_eq!(facts.finish_global.requests, 6);
            assert_eq!(facts.custody_global.requests, 10);
            assert_eq!(facts.custody_typed.requests, 1);
            assert_eq!(
                facts.custody_typed.bytes,
                HostShared::<OriginalBacking<OpaqueRetainedCharge>>::allocation_layout().size()
            );
            assert_eq!(
                facts.typed_retained_metadata_upper,
                facts.custody_typed.bytes
            );
            assert_eq!(
                facts.settlement_metadata_upper,
                custody_metadata_upper_bound(&data, &mut EvaluationCheckpoints::new(&Control))
                    .unwrap()
            );
            assert!(facts.finish_global_peak_extra < facts.custody_global_peak_extra);
        }
    }
    struct Control;
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            unreachable!()
        }
    }
    #[test]
    fn direct_flat_byte_nullable_shape_covers_real_original_validity_and_settlement() {
        for kind in [FlatByteKind::Binary, FlatByteKind::Utf8] {
            let original: ArrayRef = match kind {
                FlatByteKind::Binary => {
                    let mut builder = BinaryBuilder::new();
                    builder.append_value(b"body");
                    builder.append_null();
                    Arc::new(builder.finish())
                }
                FlatByteKind::Utf8 => {
                    let mut builder = StringBuilder::new();
                    builder.append_value("body");
                    builder.append_null();
                    Arc::new(builder.finish())
                }
            };
            let data = original.to_data();
            assert!(data.nulls().is_some());
            let base = direct_flat_byte_facts(kind, false).unwrap();
            let nullable = direct_flat_byte_facts(kind, true).unwrap();
            assert_eq!(
                nullable.finish_global.requests,
                base.finish_global.requests + 1
            );
            assert_eq!(
                nullable.custody_global.requests,
                base.custody_global.requests + 2
            );
            assert_eq!(nullable.custody_typed, base.custody_typed);
            assert!(nullable.global_retained_metadata_upper > base.global_retained_metadata_upper);
            assert_eq!(
                nullable.settlement_metadata_upper,
                custody_metadata_upper_bound(&data, &mut EvaluationCheckpoints::new(&Control))
                    .unwrap()
            );
        }
    }
    #[test]
    fn direct_flat_byte_checked_metadata_keeps_original_arithmetic_failure_projection() {
        for (buffers, children) in [(usize::MAX, 0), (0, usize::MAX)] {
            assert_eq!(
                custody_node_metadata_facts(buffers, false, children),
                Err(ScalarResourceError::Arithmetic)
            );
            assert_eq!(
                custody_node_metadata_upper_bound(buffers, false, children),
                Err(KernelFailure::ResourceExhausted)
            );
        }
    }
}

fn wrap_buffer<C: Send + Sync + 'static>(
    source: &Buffer,
    owner: &HostShared<OriginalBacking<C>>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Buffer, KernelFailure> {
    work.flush()?;
    let offset = source.ptr_offset();
    let length = checked_add(offset, source.len())?;
    let allocation: Arc<dyn arrow_buffer::alloc::Allocation> =
        Arc::new(BufferCustodian(owner.clone()));
    // SAFETY: OriginalBacking pins the original immutable allocation. Its
    // initialized prefix includes offset + len (the exact original Buffer
    // view). We never expose spare, potentially uninitialized capacity.
    let wrapped = unsafe { Buffer::from_custom_allocation(source.data_ptr(), length, allocation) }
        .slice_with_length(offset, source.len());
    work.flush()?;
    Ok(wrapped)
}
fn wrap_data<C: Send + Sync + 'static>(
    original: &ArrayData,
    owner: &HostShared<OriginalBacking<C>>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayData, KernelFailure> {
    work.flush()?;
    let mut buffers = Vec::new();
    buffers
        .try_reserve_exact(original.buffers().len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    for buffer in original.buffers() {
        buffers.push(wrap_buffer(buffer, owner, work)?);
        work.step()?;
    }
    let mut children = Vec::new();
    children
        .try_reserve_exact(original.child_data().len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    for child in original.child_data() {
        children.push(wrap_data(child, owner, work)?);
        work.step()?;
    }
    let nulls = match original.nulls() {
        None => None,
        Some(nulls) => {
            let buffer = wrap_buffer(nulls.buffer(), owner, work)?;
            let bits = BooleanBuffer::new(buffer, nulls.offset(), nulls.len());
            // SAFETY: the original validated bitmap's bytes, offset, length and
            // cached null count are unchanged. Custody is the only difference.
            Some(unsafe { NullBuffer::new_unchecked(bits, nulls.null_count()) })
        }
    };
    work.flush()?;
    // SAFETY: every structural field and every visible byte comes from the
    // same original ArrayData. No field, logical offset, validity bit, child
    // order, nested metadata or primitive alignment changes in this operation.
    let data = unsafe {
        original
            .clone()
            .into_builder()
            .buffers(buffers)
            .child_data(children)
            .nulls(nulls)
            .build_unchecked()
    };
    work.flush()?;
    Ok(data)
}

pub(crate) struct RetainedArrowResult {
    pub(crate) values: ArrayRef,
    pub(crate) original_carrier_stock: usize,
    pub(crate) retained_envelope: usize,
}

/// Transfers the caller's actual admitted output charge to Arrow backing.
/// The operation reservation was granted before scalar construction. Its
/// remaining bytes must cover this metadata transition before it is entered.
pub(crate) fn retain_result_backing(
    original: ArrayRef,
    charge: OpaqueRetainedCharge,
    reservation: &mut OpaqueReservation,
    allocator: HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<RetainedArrowResult, KernelFailure> {
    retain_result_backing_with(
        (original, charge),
        reservation,
        allocator,
        work,
        HostShared::try_new,
    )
}

/// Same custody author, with a single typed host block and no failure journal.
/// The supplied allocator is dedicated to this direct custody path.
pub(crate) fn retain_result_backing_direct(
    original: ArrayRef,
    charge: OpaqueRetainedCharge,
    reservation: &mut OpaqueReservation,
    allocator: HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<RetainedArrowResult, KernelFailure> {
    retain_result_backing_with(
        (original, charge),
        reservation,
        allocator,
        work,
        HostShared::try_new_direct,
    )
}

fn retain_result_backing_with(
    input: (ArrayRef, OpaqueRetainedCharge),
    reservation: &mut OpaqueReservation,
    allocator: HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
    make_owner: impl FnOnce(
        OriginalBacking<OpaqueRetainedCharge>,
        HostAggregateAllocator,
        &mut EvaluationCheckpoints<'_>,
    )
        -> Result<HostShared<OriginalBacking<OpaqueRetainedCharge>>, KernelFailure>,
) -> Result<RetainedArrowResult, KernelFailure> {
    let (original, mut charge) = input;
    work.flush()?;
    let data = original.to_data();
    work.flush()?;
    let metadata = custody_metadata_upper_bound(&data, work)?;
    if metadata > reservation.remaining_bytes() {
        return Err(invalid(
            "result custody exceeds its actual host operation reservation",
        ));
    }
    // Charge the whole original group's real stock, including capacities which
    // are not visible through sliced custom Buffer views, plus the conservative
    // metadata envelope. A stock observation never substitutes for the earlier
    // host grant; reconcile verifies that grant has sufficient unused backing.
    work.flush()?;
    let original_carrier_stock = original.get_array_memory_size();
    let retained = checked_add(original_carrier_stock, metadata)?;
    work.flush()?;
    charge.reconcile_under_reservation(retained, reservation)?;
    let owner = make_owner(
        OriginalBacking {
            original,
            data,
            custody: charge,
        },
        allocator,
        work,
    )?;
    let retained_envelope = checked_add(retained, owner.block_bytes())?;
    let values = wrap_original_group(owner, work)?;
    Ok(RetainedArrowResult {
        values,
        original_carrier_stock,
        retained_envelope,
    })
}

/// Allocate the shared owner through the same real host, without selecting
/// value math or inferring any admission from stock. The caller has already
/// granted the opaque metadata/payload envelope before constructing `data`.
fn granted_original_group<C: Send + Sync + 'static>(
    original: ArrayRef,
    data: ArrayData,
    custody: C,
    allocator: HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<HostShared<OriginalBacking<C>>, KernelFailure> {
    HostShared::try_new(
        OriginalBacking {
            original,
            data,
            custody,
        },
        allocator,
        work,
    )
}

/// New copy backing and already-owned source have distinct owners. Retaining
/// the source preserves its original lease; it never authorizes fresh payload.
struct CopiedBacking<C: Send + Sync + 'static> {
    source: C,
    charge: OpaqueRetainedCharge,
}

pub(crate) struct RetainedCopiedArrowResult {
    pub(crate) values: ArrayRef,
    pub(crate) new_backing_envelope: usize,
    pub(crate) new_buffer_stock: usize,
}

/// One real buffer-identity table for post-copy settlement under an ALREADY
/// admitted peak. Source identities are not bytes, grants, or value decoders.
/// The actual HostVec owns each Layout; no observation replaces admission.
pub(crate) trait CopyInputBacking {
    fn source_count(&self) -> usize;
    fn source_array_at(&self, ordinal: usize) -> Option<&dyn Array>;
    fn index_array(&self) -> Option<&dyn Array>;
}
struct CopyInputPacket<C> {
    original: ArrayRef,
    source: C,
}

struct CopyBackingSettlement {
    identities: allocator_api2::vec::Vec<usize, HostAggregateAllocator>,
    source_phase: bool,
    new_bytes: usize,
}
impl CopyBackingSettlement {
    fn new(allocator: HostAggregateAllocator) -> Self {
        Self {
            identities: allocator_api2::vec::Vec::new_in(allocator),
            source_phase: true,
            new_bytes: 0,
        }
    }
}
impl CustodyBackingObservation for CopyBackingSettlement {
    fn buffer(
        &mut self,
        buffer: &Buffer,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        work.step()?;
        let address = buffer.data_ptr().as_ptr().addr();
        for known in &self.identities {
            work.step()?;
            if *known == address {
                return Ok(());
            }
        }
        // A zero-capacity buffer owns no newly allocated payload. Source custom
        // Buffers remain pinned by their original input owner independently.
        if self.identities.try_reserve(1).is_err() {
            return Err(self
                .identities
                .allocator()
                .recorded_failure()
                .unwrap_or_else(|| {
                    invalid("copy backing identity table exceeds its representable Layout")
                }));
        }
        self.identities.push(address);
        if !self.source_phase {
            self.new_bytes = checked_add(self.new_bytes, buffer.capacity())?;
        }
        Ok(())
    }
}

/// The resource author must have granted the whole copy/transient/metadata
/// envelope BEFORE the original Arrow copy. Its immutable retained bound
/// excludes already-owned source payload, including Dictionary/View buffers.
/// Facts are sealed by the SAME selected-copy traversal and bound to the actual
/// original take result once. No caller-supplied byte estimate can enter here.
pub(crate) fn retain_copied_result_backing<C: CopyInputBacking + Send + Sync + 'static>(
    original: ArrayRef,
    source: C,
    mut charge: OpaqueRetainedCharge,
    reservation: &mut OpaqueReservation,
    allocator: HostAggregateAllocator,
    facts: &crate::selected_copy::CopyOperationFacts,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<RetainedCopiedArrowResult, KernelFailure> {
    // Failed exits destroy output aliases before their source lease. No
    // temporary caller Arc loan can outlive and invalidate this custody order.
    let packet = CopyInputPacket { original, source };
    if !facts.matches_original_copy(&packet.original) {
        return Err(invalid(
            "copy custody received facts for another actual Arrow result",
        ));
    }
    if !reservation.belongs_to_allocator(&allocator) {
        return Err(invalid(
            "copy custody uses a different actual reservation host",
        ));
    }
    // Reservation remains live across original to_data construction. This is
    // not post-copy grant: its amount came from the pre-copy resource author.
    work.flush()?;
    let data = packet.original.to_data();
    work.flush()?;
    // Source/index loans are created inside the SAME earlier metadata/peak
    // reservation. Their identities classify aliases, never infer a grant.
    let mut source_data = allocator_api2::vec::Vec::<ArrayData, _>::new_in(allocator.clone());
    if source_data
        .try_reserve_exact(packet.source.source_count())
        .is_err()
    {
        return Err(allocator.recorded_failure().unwrap_or_else(|| {
            invalid("copy source metadata table exceeds its representable Layout")
        }));
    }
    for ordinal in 0..packet.source.source_count() {
        work.step()?;
        let source = packet
            .source
            .source_array_at(ordinal)
            .ok_or_else(|| invalid("copied backing owner has no exact source ordinal"))?;
        source_data.push(source.to_data());
        work.flush()?;
    }
    let index_data = match packet.source.index_array() {
        Some(indices) => {
            let data = indices.to_data();
            work.flush()?;
            Some(data)
        }
        None => None,
    };
    let mut settlement = CopyBackingSettlement::new(allocator.clone());
    for source in source_data.iter() {
        custody_metadata_with_backing(source, work, Some(&mut settlement))?;
    }
    if let Some(index_data) = &index_data {
        custody_metadata_with_backing(index_data, work, Some(&mut settlement))?;
    }
    settlement.source_phase = false;
    let metadata = custody_metadata_with_backing(&data, work, Some(&mut settlement))?;
    let new_buffer_stock = settlement.new_bytes;
    if new_buffer_stock > facts.retained_new_backing_upper() {
        return Err(invalid(
            "copy output backing exceeds its earlier selected-copy envelope",
        ));
    }
    drop(settlement);
    drop(index_data);
    drop(source_data);
    let retained = checked_add(new_buffer_stock, metadata)?;
    if retained > reservation.remaining_bytes() {
        return Err(invalid(
            "copy custody exceeds its actual operation reservation",
        ));
    }
    charge.reconcile_under_reservation(retained, reservation)?;
    let owner = granted_original_group(
        packet.original,
        data,
        CopiedBacking {
            source: packet.source,
            charge,
        },
        allocator,
        work,
    )?;
    let envelope = checked_add(retained, owner.block_bytes())?;
    let values = wrap_original_group(owner, work)?;
    Ok(RetainedCopiedArrowResult {
        values,
        new_backing_envelope: envelope,
        new_buffer_stock,
    })
}

/// ONE original immutable Buffer/ArrayData custody author for output charges
/// and already-owned source leases. Generic custody never selects value math.
fn wrap_original_group<C: Send + Sync + 'static>(
    owner: HostShared<OriginalBacking<C>>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let data = wrap_data(&owner.data, &owner, work)?;
    work.flush()?;
    let result =
        if data.buffers().is_empty() && data.child_data().is_empty() && data.nulls().is_none() {
            // A bufferless root has no payload alias; the root object retains
            // the actual backing owner without inventing a validity buffer.
            Arc::new(BufferlessResult {
                values: make_array(data),
                owner,
            }) as ArrayRef
        } else {
            make_array(data)
        };
    work.flush()?;
    Ok(result)
}

struct BufferlessResult<C> {
    values: ArrayRef,
    owner: HostShared<OriginalBacking<C>>,
}
impl<C> std::fmt::Debug for BufferlessResult<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.values.fmt(f)
    }
}
// SAFETY: this wrapper is created only from make_array(original ArrayData)
// when that data has no buffers, child data, or validity buffer. Every Array
// fact and as_any downcast is delegated to that SAME immutable built-in array;
// no length, type, offset, bitmap, alignment, or pointer is authored here.
// slice delegates bounds/type construction to the original array and retains
// the same custody owner. The extra owner does not affect Arrow data facts.
unsafe impl<C: Send + Sync + 'static> Array for BufferlessResult<C> {
    fn as_any(&self) -> &dyn Any {
        self.values.as_any()
    }
    fn to_data(&self) -> ArrayData {
        self.values.to_data()
    }
    fn into_data(self) -> ArrayData {
        self.values.to_data()
    }
    fn data_type(&self) -> &DataType {
        self.values.data_type()
    }
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self {
            values: self.values.slice(offset, length),
            owner: self.owner.clone(),
        })
    }
    fn len(&self) -> usize {
        self.values.len()
    }
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    fn offset(&self) -> usize {
        self.values.offset()
    }
    fn nulls(&self) -> Option<&NullBuffer> {
        self.values.nulls()
    }
    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.values.logical_nulls()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.values.get_buffer_memory_size()
    }
    fn get_array_memory_size(&self) -> usize {
        self.values.get_array_memory_size()
    }
}

/// Actual caller-owned input backing. Moving the owner here preserves its
/// original authority and teardown; this type creates no payload grant. Only
/// its immutable shared block is allocated through the actual supplied host.
pub struct SourceBackingOwner<C: Send + Sync + 'static> {
    backing: HostShared<C>,
    allocator: HostAggregateAllocator,
    host: Arc<dyn crate::AggregateStateAllocator>,
}
impl<C: Send + Sync + 'static> Clone for SourceBackingOwner<C> {
    fn clone(&self) -> Self {
        Self {
            backing: self.backing.clone(),
            allocator: self.allocator.clone(),
            host: Arc::clone(&self.host),
        }
    }
}
impl<C: Send + Sync + 'static> SourceBackingOwner<C> {
    /// Fresh typed holder requests, without allocating or granting capacity.
    pub fn owned_request_facts() -> Result<SourceCustodyRequestFacts, crate::ScalarResourceError> {
        crate::scalar_resources::require_arrow_model()?;
        SourceCustodyRequestFacts::owned(
            HostShared::<C>::allocation_layout(),
            HostAggregateAllocator::metadata_allocation_bytes(),
        )
    }

    /// Move an already-paid source into a dedicated cold direct allocator.
    /// The caller funds the prefix before entry and qualifies its callbacks.
    /// Failure drops C: surviving raw aliases still need their original owner.
    pub fn try_from_owned_direct(
        backing: C,
        host: Arc<dyn crate::AggregateStateAllocator>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        work.flush()?;
        crate::scalar_resources::require_arrow_model().map_err(model_failure)?;
        let capability = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        drop(capability);
        work.flush()?;
        let allocator = HostAggregateAllocator::try_new(Arc::clone(&host))?;
        work.flush()?;
        let backing = HostShared::try_new_direct(backing, allocator.clone(), work)?;
        Ok(Self {
            backing,
            allocator,
            host,
        })
    }

    pub fn try_from_owned(
        backing: C,
        host: Arc<dyn crate::AggregateStateAllocator>,
        control: &dyn crate::KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        let observation = crate::kernel_control::KernelControlObservation::new(control);
        observation.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(&observation);
        let result: Result<Self, KernelFailure> = (|| {
            // Refuse absent capability before moving any source to a published
            // owner. Its original Drop still runs on every failed path.
            let capability = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
            drop(capability);
            work.flush()?;
            let allocator = HostAggregateAllocator::try_new(Arc::clone(&host))?;
            work.flush()?;
            let backing = HostShared::try_new(backing, allocator.clone(), &mut work)?;
            Ok(Self {
                backing,
                allocator,
                host,
            })
        })();
        match result {
            Ok(value) => {
                work.finish()?;
                observation.finish(Ok(value))
            }
            Err(cause) => observation.finish(Err(cause)),
        }
    }
    pub fn borrowed(&self) -> &C {
        &self.backing
    }
}

/// Source lease and newly admitted metadata have different responsibilities.
/// The original shared source owner is destroyed before its wrapping metadata
/// charge, while OriginalBacking destroys its data/payload aliases first.
struct SourceCustody<C: Send + Sync + 'static> {
    source: SourceBackingOwner<C>,
    metadata: OpaqueRetainedCharge,
}

fn source_request_pricing<C: Send + Sync + 'static>() -> SourceRequestPricing {
    SourceRequestPricing {
        group: HostShared::<OriginalBacking<SourceCustody<C>>>::allocation_layout(),
        custodian: Layout::new::<BufferCustodian<SourceCustody<C>>>(),
        bufferless: Layout::new::<BufferlessResult<SourceCustody<C>>>(),
    }
}

/// A sealed request packet borrowing the exact audited source continuation.
/// The caller supplies the original source premise and its actual paid owner;
/// neither schema nor as_any alone certifies an arbitrary Array implementation.
pub struct PreparedSourceCustody<'source, C: Send + Sync + 'static> {
    original: &'source ArrayRef,
    facts: SourceCustodyRequestFacts,
    identity: PhantomData<C>,
}
impl<'source, C: Send + Sync + 'static> PreparedSourceCustody<'source, C> {
    /// No ArrayData, ArrayRef clone, Field clone, or grant is created here.
    /// The caller has already funded the preparation prefix and callbacks.
    pub fn prepare(
        original: &'source ArrayRef,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        work.flush()?;
        crate::scalar_resources::require_arrow_model().map_err(model_failure)?;
        let pricing = source_request_pricing::<C>();
        let facts =
            crate::array_backing_geometry::source_request_facts(original.as_ref(), &pricing, work)?;
        Ok(Self {
            original,
            facts,
            identity: PhantomData,
        })
    }
    pub const fn request_facts(&self) -> SourceCustodyRequestFacts {
        self.facts
    }

    /// Consume the same source loan under an already-paid complete operation.
    /// Only GLOBAL metadata is transferred from this same-host reservation;
    /// typed blocks have separate host grants. The original source is not paid
    /// again. This path borrows work and owns neither completion nor a journal.
    ///
    /// The caller's qualified GLOBAL physical owner must survive real free,
    /// including outer Arc/Weak tails after the opaque charge has exited.
    pub fn retain(
        self,
        source: &SourceBackingOwner<C>,
        reservation: &mut OpaqueReservation,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<ArrayRef, KernelFailure> {
        work.flush()?;
        if !reservation.belongs_to_allocator(&source.allocator) {
            return Err(invalid(
                "source custody uses a different actual reservation host",
            ));
        }
        if self.facts.global_peak_upper() > reservation.remaining_bytes() {
            return Err(invalid(
                "source custody exceeds its complete operation reservation",
            ));
        }
        let mut metadata = OpaqueRetainedCharge::try_new(Arc::clone(&source.host))?;
        metadata.reconcile_under_reservation(self.facts.global_retained_upper(), reservation)?;
        work.flush()?;
        let data = self.original.to_data();
        work.flush()?;
        let owner = HostShared::try_new_direct(
            OriginalBacking {
                original: Arc::clone(self.original),
                data,
                custody: SourceCustody {
                    source: source.clone(),
                    metadata,
                },
            },
            source.allocator.clone(),
            work,
        )?;
        // Same original buffer transform, rebox, refcounts, and destruction.
        wrap_original_group(owner, work)
    }
}

/// Alias one original source array while transferring its ACTUAL input owner
/// to every returned Buffer. The original owner must contain the real source
/// backing/lease, not a numerical observation token. No input stock is charged
/// a second time here, and no allocating to_data occurs before host admission.
pub fn retain_source_backing<C: Send + Sync + 'static>(
    original: ArrayRef,
    source: &SourceBackingOwner<C>,
    control: &dyn crate::KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    let observation = crate::kernel_control::KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result: Result<ArrayRef, KernelFailure> = (|| {
        let envelope =
            crate::array_backing_geometry::source_metadata_bytes(original.as_ref(), &mut work)?;
        let mut metadata = OpaqueRetainedCharge::try_new(Arc::clone(&source.host))?;
        work.flush()?;
        let mut reservation = metadata.reserve_operation(envelope)?;
        work.flush()?;
        // This is the original Arrow author. The preceding reservation covers
        // its actual variable buffer/child tables without any value decoding.
        let data = original.to_data();
        work.flush()?;
        let actual_metadata = custody_metadata_upper_bound(&data, &mut work)?;
        if actual_metadata > reservation.remaining_bytes() {
            return Err(invalid(
                "source custody exceeds its admitted structural envelope",
            ));
        }
        metadata.reconcile_under_reservation(actual_metadata, &mut reservation)?;
        let owner = HostShared::try_new(
            OriginalBacking {
                original,
                data,
                custody: SourceCustody {
                    source: source.clone(),
                    metadata,
                },
            },
            source.allocator.clone(),
            &mut work,
        )?;
        // The same ONE buffer transform is used by original scalar output
        // and existing source aliases; no new serializer or value operation.
        let result = wrap_original_group(owner, &mut work)?;
        drop(reservation);
        Ok(result)
    })();
    match result {
        Ok(value) => {
            work.finish()?;
            observation.finish(Ok(value))
        }
        Err(cause) => observation.finish(Err(cause)),
    }
}
