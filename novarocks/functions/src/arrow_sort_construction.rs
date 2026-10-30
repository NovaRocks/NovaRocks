// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//! Arrow 60.1.0 lexsort constructor structures over actual admitted keys.
//! Row indices/valid tuples/rank payloads keep the caller's original policy.
use crate::selected_copy::{
    CopyConstructionError, CopyError, preflight_flat_arrow_structure_construction,
    preflight_primitive_vec_adoption_construction_observed,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, MapArray, StringArray, StructArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_ord::{ord::DynComparator, sort::SortColumn};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ControlResourceError, ValueSchemaCreation,
    owned_resources::{layout::LayoutResourceError, vec::original_fresh_push_requests},
};
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LexsortStructureFacts {
    /// Comparator Box/vector upper only; temporary bitmap headers use the
    /// separate sole Buffer-adoption author and are not this subtotal.
    pub comparator_upper_bytes: usize,
    pub comparator_nodes: usize,
    pub comparator_vectors: usize,
}
fn arithmetic<H>() -> CopyConstructionError<H> {
    LayoutResourceError::Arithmetic.into()
}
fn add<H>(a: usize, b: usize) -> Result<usize, CopyConstructionError<H>> {
    a.checked_add(b).ok_or_else(arithmetic)
}

/// Private closure field order is not an allocator Layout. Its actual public
/// captures are bounded by each field's own maximum alignment padding, plus
/// final aggregate padding. No private closure representation is recreated.
fn capture_upper<H>(
    fields: impl IntoIterator<Item = Layout>,
) -> Result<usize, CopyConstructionError<H>> {
    let mut bytes = 0;
    let mut maximum_alignment = 1;
    for field in fields {
        bytes = add(bytes, add(field.size(), field.align() - 1)?)?;
        maximum_alignment = maximum_alignment.max(field.align());
    }
    add(bytes, maximum_alignment - 1)
}
enum VectorFailure<H> {
    Creator(H),
    Resource(ControlResourceError),
}
impl<H> From<ControlResourceError> for VectorFailure<H> {
    fn from(e: ControlResourceError) -> Self {
        Self::Resource(e)
    }
}
struct Walk<'a, 'port, H, W> {
    temporary: &'a mut ValueSchemaCreation<'port, H>,
    work: &'a mut W,
    facts: LexsortStructureFacts,
    // Only the original Run take entry bypasses the lexsort ingress profile.
    raw_take: bool,
}
impl<H, W: FnMut(bool) -> Result<(), H>> Walk<'_, '_, H, W> {
    fn visit(&mut self) -> Result<(), CopyConstructionError<H>> {
        (self.work)(false).map_err(CopyConstructionError::Creator)
    }
    fn opaque(&mut self, bytes: usize) -> Result<(), CopyConstructionError<H>> {
        if bytes == 0 {
            return Ok(());
        }
        self.temporary
            .observe_opaque_upper_bound(bytes)
            .map_err(CopyConstructionError::Creator)?;
        self.facts.comparator_upper_bytes = add(self.facts.comparator_upper_bytes, bytes)?;
        Ok(())
    }
    fn vector(&mut self, count: usize) -> Result<(), CopyConstructionError<H>> {
        self.fresh_vector::<DynComparator>(count)?;
        self.facts.comparator_vectors = add(self.facts.comparator_vectors, 1)?;
        Ok(())
    }
    fn fresh_vector<T>(&mut self, count: usize) -> Result<(), CopyConstructionError<H>> {
        // ord.rs344/1042/1083 Result.collect<Vec<DynComparator>> creates a
        // new unreserved Vec through GenericShunt's zero lower size hint.
        // Reuse the sole locked RawVec amortized growth operation author.
        let bytes = original_fresh_push_requests::<T, VectorFailure<H>>(count, &mut || {
            (self.work)(false).map_err(VectorFailure::Creator)
        })
        .map_err(|e| match e {
            VectorFailure::Creator(e) => CopyConstructionError::Creator(e),
            VectorFailure::Resource(source) => {
                CopyError::Control(crate::KernelFailure::from_domain(source)).into()
            }
        })?;
        self.opaque(bytes)?;
        Ok(())
    }
    fn bitmap_header(&mut self) -> Result<(), CopyConstructionError<H>> {
        // BooleanBufferBuilder::finish and !BooleanBuffer adopt one private
        // Buffer owner. Reuse that existing header-only author, not payload.
        // No additional Array Arc is requested by this caller.
        preflight_primitive_vec_adoption_construction_observed(self.temporary, self.work)?;
        Ok(())
    }
    fn map<K, V>(&mut self, count: usize) -> Result<(), CopyConstructionError<H>> {
        let facts =
            novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<K, V>(count)
                .map_err(|cause| {
                    CopyError::Control(crate::KernelFailure::from_domain(ComparatorMapFailure(
                        cause,
                    )))
                })?;
        if let Some(layout) = facts.layout {
            self.opaque(layout.size())?;
        }
        Ok(())
    }
    fn logical_bitmap(&mut self, rows: usize) -> Result<(), CopyConstructionError<H>> {
        let bytes = add(rows / 8, usize::from(!rows.is_multiple_of(8)))?;
        let rounded = add(bytes, 63)? / 64 * 64;
        // collect_bool/BooleanBufferBuilder request round64; sparse mask's
        // remainder push can grow to twice it while the old block coexists.
        self.opaque(rounded.checked_mul(3).ok_or_else(arithmetic)?)?;
        self.bitmap_header()
    }
    fn logical_nulls(&mut self, array: &dyn Array) -> Result<(), CopyConstructionError<H>> {
        self.visit()?;
        match array.data_type() {
            DataType::Dictionary(_, _) => {
                let values = array
                    .as_any_dictionary_opt()
                    .ok_or_else(arithmetic)?
                    .values();
                self.logical_nulls(values.as_ref())?;
                self.logical_bitmap(array.len())?;
            }
            DataType::RunEndEncoded(ends, _) => {
                let values = match ends.data_type() {
                    DataType::Int16 => array.as_run::<Int16Type>().values(),
                    DataType::Int32 => array.as_run::<Int32Type>().values(),
                    DataType::Int64 => array.as_run::<Int64Type>().values(),
                    _ => return Ok(()),
                };
                self.logical_nulls(values.as_ref())?;
                self.logical_bitmap(array.len())?;
            }
            DataType::Union(fields, _) if !fields.is_empty() => {
                let union = array
                    .as_any()
                    .downcast_ref::<arrow_array::UnionArray>()
                    .ok_or_else(arithmetic)?;
                for (id, _) in fields.iter() {
                    self.logical_nulls(union.child(id).as_ref())?;
                }
                // fields_logical_nulls can filter every field; collect starts
                // unreserved. Mask variants additionally own public bit-chunk
                // tables, two HashSets, and the remaining type-id vector.
                // Sum branch-specific requests to cover all target strategies;
                // this is an opaque source upper, never a private layout claim.
                self.fresh_vector::<(i8, NullBuffer)>(fields.len())?;
                self.fresh_vector::<(i8, arrow_buffer::bit_chunk_iterator::BitChunks<'_>)>(
                    fields.len(),
                )?;
                self.fresh_vector::<(i8, arrow_buffer::bit_chunk_iterator::BitChunkIterator<'_>)>(
                    fields.len(),
                )?;
                self.map::<i8, ()>(fields.len())?;
                self.map::<i8, ()>(fields.len())?;
                self.fresh_vector::<i8>(fields.len())?;
                self.logical_bitmap(1)?; // gather's one-null buffer
                self.logical_bitmap(1)?; // gather's one-valid buffer
                self.logical_bitmap(array.len())?;
            }
            _ => {} // ordinary Array::logical_nulls clones its borrowed validity
        }
        Ok(())
    }
    fn comparator(&mut self, array: &dyn Array) -> Result<(), CopyConstructionError<H>> {
        self.visit()?;
        self.facts.comparator_nodes = add(self.facts.comparator_nodes, 1)?;
        if self.raw_take && *array.data_type() != DataType::Null {
            // compare() independently materializes logical validity for each
            // borrowed operand. The operands alias here; the two operations do
            // not share newly constructed validity buffers.
            self.logical_nulls(array)?;
            self.logical_nulls(array)?;
        }
        let nulls = [
            Layout::new::<Option<NullBuffer>>(),
            Layout::new::<Option<NullBuffer>>(),
            Layout::new::<std::cmp::Ordering>(),
            Layout::new::<std::cmp::Ordering>(),
        ];
        let mut payload = [Layout::new::<()>(); 8];
        let payload_len = if array.data_type().is_primitive() {
            // ord.rs119: two cloned ScalarBuffers. Native T does not add
            // storage to ScalarBuffer; PhantomData is zero sized.
            payload[..2].copy_from_slice(&[
                Layout::new::<ScalarBuffer<u8>>(),
                Layout::new::<ScalarBuffer<u8>>(),
            ]);
            2
        } else {
            match array.data_type() {
                DataType::Null => return Ok(()), // ord.rs565 Box<ZST> has no heap request
                DataType::Boolean => {
                    let fields = [
                        Layout::new::<BooleanBuffer>(),
                        Layout::new::<BooleanBuffer>(),
                    ];
                    payload[..fields.len()].copy_from_slice(&fields);
                    fields.len()
                }
                DataType::Utf8 => {
                    let fields = [Layout::new::<StringArray>(), Layout::new::<StringArray>()];
                    payload[..fields.len()].copy_from_slice(&fields);
                    fields.len()
                }
                DataType::LargeUtf8 => {
                    let fields = [
                        Layout::new::<LargeStringArray>(),
                        Layout::new::<LargeStringArray>(),
                    ];
                    payload[..fields.len()].copy_from_slice(&fields);
                    fields.len()
                }
                DataType::Binary => {
                    let fields = [Layout::new::<BinaryArray>(), Layout::new::<BinaryArray>()];
                    payload[..fields.len()].copy_from_slice(&fields);
                    fields.len()
                }
                DataType::LargeBinary => {
                    let fields = [
                        Layout::new::<LargeBinaryArray>(),
                        Layout::new::<LargeBinaryArray>(),
                    ];
                    payload[..fields.len()].copy_from_slice(&fields);
                    fields.len()
                }
                DataType::FixedSizeBinary(_) => {
                    let fields = [
                        Layout::new::<FixedSizeBinaryArray>(),
                        Layout::new::<FixedSizeBinaryArray>(),
                    ];
                    payload[..fields.len()].copy_from_slice(&fields);
                    fields.len()
                }
                DataType::List(_) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(arithmetic)?;
                    self.comparator(a.values().as_ref())?;
                    {
                        let fields = [
                            Layout::new::<DynComparator>(),
                            Layout::new::<OffsetBuffer<i32>>(),
                            Layout::new::<OffsetBuffer<i32>>(),
                        ];
                        payload[..fields.len()].copy_from_slice(&fields);
                        fields.len()
                    }
                }
                DataType::LargeList(_) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .ok_or_else(arithmetic)?;
                    self.comparator(a.values().as_ref())?;
                    {
                        let fields = [
                            Layout::new::<DynComparator>(),
                            Layout::new::<OffsetBuffer<i64>>(),
                            Layout::new::<OffsetBuffer<i64>>(),
                        ];
                        payload[..fields.len()].copy_from_slice(&fields);
                        fields.len()
                    }
                }
                DataType::FixedSizeList(_, _) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(arithmetic)?;
                    self.comparator(a.values().as_ref())?;
                    {
                        let fields = [
                            Layout::new::<DynComparator>(),
                            Layout::new::<usize>(),
                            Layout::new::<usize>(),
                            Layout::new::<std::cmp::Ordering>(),
                        ];
                        payload[..fields.len()].copy_from_slice(&fields);
                        fields.len()
                    }
                }
                DataType::Struct(_) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(arithmetic)?;
                    self.vector(a.columns().len())?;
                    for child in a.columns() {
                        self.comparator(child.as_ref())?;
                    }
                    {
                        let fields = [Layout::new::<Vec<DynComparator>>()];
                        payload[..fields.len()].copy_from_slice(&fields);
                        fields.len()
                    }
                }
                DataType::Map(_, _) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<MapArray>()
                        .ok_or_else(arithmetic)?;
                    self.comparator(a.entries())?;
                    {
                        let fields = [
                            Layout::new::<DynComparator>(),
                            Layout::new::<OffsetBuffer<i32>>(),
                            Layout::new::<OffsetBuffer<i32>>(),
                        ];
                        payload[..fields.len()].copy_from_slice(&fields);
                        fields.len()
                    }
                }
                DataType::Dictionary(key, value)
                    if !self.raw_take
                        && **key == DataType::Int32
                        && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
                {
                    let a = array
                        .as_any()
                        .downcast_ref::<arrow_array::DictionaryArray<Int32Type>>()
                        .ok_or_else(arithmetic)?;
                    self.comparator(a.values().as_ref())?;
                    if a.values().is_nullable() {
                        self.bitmap_header()?;
                        self.bitmap_header()?;
                    }
                    {
                        let fields = [
                            Layout::new::<DynComparator>(),
                            Layout::new::<ScalarBuffer<i32>>(),
                            Layout::new::<ScalarBuffer<i32>>(),
                        ];
                        payload[..fields.len()].copy_from_slice(&fields);
                        fields.len()
                    }
                }
                DataType::Utf8View | DataType::BinaryView if self.raw_take => {
                    // GenericByteViewArray60.1 clones its Arc region table.
                    // Both concrete headers have the same public carriers.
                    let fields = if *array.data_type() == DataType::Utf8View {
                        [Layout::new::<arrow_array::StringViewArray>(); 2]
                    } else {
                        [Layout::new::<arrow_array::BinaryViewArray>(); 2]
                    };
                    payload[..2].copy_from_slice(&fields);
                    2
                }
                DataType::ListView(_) | DataType::LargeListView(_) if self.raw_take => {
                    let child = if matches!(array.data_type(), DataType::ListView(_)) {
                        array
                            .as_any()
                            .downcast_ref::<arrow_array::ListViewArray>()
                            .ok_or_else(arithmetic)?
                            .values()
                    } else {
                        array
                            .as_any()
                            .downcast_ref::<arrow_array::LargeListViewArray>()
                            .ok_or_else(arithmetic)?
                            .values()
                    };
                    self.comparator(child.as_ref())?;
                    let fields = [
                        Layout::new::<DynComparator>(),
                        Layout::new::<ScalarBuffer<i64>>(),
                        Layout::new::<ScalarBuffer<i64>>(),
                        Layout::new::<ScalarBuffer<i64>>(),
                        Layout::new::<ScalarBuffer<i64>>(),
                    ];
                    payload[..fields.len()].copy_from_slice(&fields);
                    fields.len()
                }
                DataType::RunEndEncoded(ends, _) if self.raw_take => {
                    let values = match ends.data_type() {
                        DataType::Int16 => array.as_run::<Int16Type>().values(),
                        DataType::Int32 => array.as_run::<Int32Type>().values(),
                        DataType::Int64 => array.as_run::<Int64Type>().values(),
                        _ => return Ok(()), // original SDK owns invalid-carrier failure
                    };
                    self.comparator(values.as_ref())?;
                    let fields = [
                        Layout::new::<DynComparator>(),
                        Layout::new::<arrow_buffer::RunEndBuffer<i64>>(),
                        Layout::new::<arrow_buffer::RunEndBuffer<i64>>(),
                    ];
                    payload[..fields.len()].copy_from_slice(&fields);
                    fields.len()
                }
                DataType::Union(fields, _) if self.raw_take => {
                    let union = array
                        .as_any()
                        .downcast_ref::<arrow_array::UnionArray>()
                        .ok_or_else(arithmetic)?;
                    self.map::<i8, DynComparator>(fields.len())?;
                    for (id, _) in fields.iter() {
                        self.comparator(union.child(id).as_ref())?;
                    }
                    let fields = [
                        Layout::new::<std::collections::HashMap<i8, DynComparator>>(),
                        Layout::new::<ScalarBuffer<i8>>(),
                        Layout::new::<ScalarBuffer<i8>>(),
                        Layout::new::<Option<ScalarBuffer<i32>>>(),
                        Layout::new::<Option<ScalarBuffer<i32>>>(),
                    ];
                    payload[..fields.len()].copy_from_slice(&fields);
                    fields.len()
                }
                DataType::Dictionary(_, _) if self.raw_take => {
                    let dictionary = array.as_any_dictionary_opt().ok_or_else(arithmetic)?;
                    let values = dictionary.values();
                    if matches!(
                        values.data_type(),
                        DataType::Utf8
                            | DataType::LargeUtf8
                            | DataType::Binary
                            | DataType::LargeBinary
                            | DataType::FixedSizeBinary(_)
                    ) {
                        //60.1's byte specialization captures both value-array
                        // headers and key ScalarBuffers in ONE root closure.
                        let value_layout = Layout::for_value(values.as_ref());
                        let fields = [
                            value_layout,
                            value_layout,
                            Layout::new::<ScalarBuffer<u64>>(),
                            Layout::new::<ScalarBuffer<u64>>(),
                        ];
                        payload[..fields.len()].copy_from_slice(&fields);
                        fields.len()
                    } else {
                        self.comparator(values.as_ref())?;
                        let fields = [
                            Layout::new::<DynComparator>(),
                            Layout::new::<ScalarBuffer<u64>>(),
                            Layout::new::<ScalarBuffer<u64>>(),
                        ];
                        payload[..fields.len()].copy_from_slice(&fields);
                        fields.len()
                    }
                }
                // These Arrow grammars are supported upstream but excluded by
                // the current execution key ingress. Coverage is explicit,
                // never a false assertion that Arrow has no ordering support.
                DataType::Utf8View
                | DataType::BinaryView
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::RunEndEncoded(_, _)
                | DataType::Union(_, _)
                | DataType::Dictionary(_, _) => return Err(LayoutResourceError::SourceModel.into()),
                _ => return Err(CopyError::unsupported(array.data_type()).into()),
            }
        };
        self.opaque(capture_upper(
            payload[..payload_len].iter().copied().chain(nulls),
        )?)
    }
}
#[derive(Debug)]
struct ComparatorMapFailure(
    novarocks_type_contract::owned_resources::hashmap::HashMapResourceError,
);
impl std::fmt::Display for ComparatorMapFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for ComparatorMapFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for ComparatorMapFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.0)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ComparatorMapFailure {}

/// Numeric contribution of the actual make_comparator(values,values) used by
/// take_run. Existing source/values stay borrowed; the returned number owns no
/// capacity. All requests belong to the caller's original temporary scope.
/// Lexsort still passes its separate unchanged source_coverage admission.
pub(crate) fn original_take_comparator_upper_observed<E: From<CopyError>>(
    array: &dyn Array,
    work: &mut impl FnMut(bool) -> Result<(), E>,
) -> Result<usize, E> {
    let (mut exact_bytes, mut opaque_bytes) = (0usize, 0usize);
    let mut exact = |layout: Layout| {
        exact_bytes = exact_bytes
            .checked_add(layout.size())
            .ok_or_else(|| E::from(CopyError::arithmetic(None)))?;
        Ok(())
    };
    let mut opaque = |bytes| {
        opaque_bytes = opaque_bytes
            .checked_add(bytes)
            .ok_or_else(|| E::from(CopyError::arithmetic(None)))?;
        Ok(())
    };
    let mut temporary = ValueSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut exact,
        &mut opaque,
    );
    let result = Walk {
        temporary: &mut temporary,
        work,
        facts: LexsortStructureFacts::default(),
        raw_take: true,
    }
    .comparator(array);
    match result {
        Ok(()) => exact_bytes
            .checked_add(opaque_bytes)
            .ok_or_else(|| E::from(CopyError::arithmetic(None))),
        Err(CopyConstructionError::Creator(cause)) => Err(cause),
        Err(CopyConstructionError::Copy(cause)) => Err(cause.into()),
        Err(CopyConstructionError::Resource(cause)) => {
            Err(CopyError::Control(crate::KernelFailure::from_domain(cause)).into())
        }
    }
}

fn can_rank(ty: &DataType) -> bool {
    ty.is_primitive()
        || matches!(
            ty,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
        )
}
fn nonlexical(ty: &DataType) -> bool {
    ty.is_primitive()
        || matches!(
            ty,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::FixedSizeBinary(_)
        )
        || match ty {
            DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
                can_rank(f.data_type())
            }
            DataType::Dictionary(_, value) => can_rank(value),
            _ => false,
        }
}

fn source_coverage<H>(
    array: &dyn Array,
    work: &mut impl FnMut(bool) -> Result<(), H>,
) -> Result<(), CopyConstructionError<H>> {
    work(false).map_err(CopyConstructionError::Creator)?;
    match array.data_type() {
        ty if ty.is_primitive() => Ok(()),
        DataType::Null
        | DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => Ok(()),
        DataType::List(_) => source_coverage(
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(arithmetic)?
                .values()
                .as_ref(),
            work,
        ),
        DataType::LargeList(_) => source_coverage(
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(arithmetic)?
                .values()
                .as_ref(),
            work,
        ),
        DataType::FixedSizeList(_, _) => source_coverage(
            array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(arithmetic)?
                .values()
                .as_ref(),
            work,
        ),
        DataType::Struct(_) => {
            for child in array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(arithmetic)?
                .columns()
            {
                source_coverage(child.as_ref(), work)?;
            }
            Ok(())
        }
        DataType::Map(_, _) => source_coverage(
            array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(arithmetic)?
                .entries(),
            work,
        ),
        DataType::Dictionary(key, value)
            if **key == DataType::Int32
                && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            Ok(())
        }
        DataType::Utf8View
        | DataType::BinaryView
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::RunEndEncoded(_, _)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _) => Err(LayoutResourceError::SourceModel.into()),
        _ => Err(CopyError::unsupported(array.data_type()).into()),
    }
}
/// Preflight structures of ONE original Arrow lexsort_to_indices call. Caller
/// must retain temporary through the real kernel, and retained through the
/// independent UInt32 indices' last use. It checkpoints before/after the opaque
/// original operation; this model claims no cooperation inside that operation.
/// Rows/valid tuples/ranks/input payload remain with the original sort owner.
pub fn preflight_lexsort_structure_construction<H>(
    columns: &[SortColumn],
    _limit: Option<usize>,
    temporary: &mut ValueSchemaCreation<'_, H>,
    retained: &mut ValueSchemaCreation<'_, H>,
    work: &mut impl FnMut(bool) -> Result<(), H>,
) -> Result<LexsortStructureFacts, CopyConstructionError<H>> {
    work(true).map_err(CopyConstructionError::Creator)?;
    if columns.is_empty() {
        return Err(CopyError::Invalid("Sort requires at least one column").into());
    }
    for column in columns {
        source_coverage(column.values.as_ref(), work)?;
    }
    let mut walk = Walk {
        temporary,
        work,
        facts: LexsortStructureFacts::default(),
        raw_take: false,
    };
    if columns.len() == 1 && nonlexical(columns[0].values.data_type()) {
        // sort.rs271 partition_validity and scalar/list/dictionary sorting.
        // child_rank computes only original Vec row stock, no ArrayData or
        // comparator Box. Its payload does not enter this structure model.
        walk.visit()?;
        if columns[0].values.null_count() > 0 {
            walk.bitmap_header()?;
        }
    } else {
        let rows = columns[0].values.len();
        for c in columns {
            walk.visit()?;
            if c.values.len() != rows {
                return Err(
                    CopyError::Invalid("lexical sort columns have different row counts").into(),
                );
            }
        }
        walk.vector(columns.len())?;
        for c in columns {
            walk.comparator(c.values.as_ref())?;
        }
    }
    (walk.work)(true).map_err(CopyConstructionError::Creator)?;
    // The sole result-custody header bound includes the caller's ONE
    // Arc<UInt32Array> publication. Do not request that Arc a second time.
    preflight_flat_arrow_structure_construction(&DataType::UInt32, false, retained, walk.work)?;
    Ok(walk.facts)
}

#[cfg(test)]
#[path = "arrow_sort_construction_tests.rs"]
mod tests;
