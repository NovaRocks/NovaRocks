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

//! Source-bound request facts for the original Arrow custody continuation.
//! Facts describe new requested Layouts; they neither grant capacity nor
//! certify an arbitrary Array, allocator, control callback, or source owner.
use crate::kernel_control::invalid;
use crate::{KernelFailure, ScalarResourceError};
use arrow_array::ArrayRef;
use arrow_buffer::{Buffer, NullBuffer};
use arrow_data::{ArrayData, BufferSpec};
use arrow_schema::{DataType, UnionMode};
use novarocks_type_contract::owned_resources::layout::arc_layout;
use std::alloc::Layout;

/// A conservative cumulative sum of new nonzero allocation requests.
/// Replacement allocations count again; preexisting input stock does not.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceCustodyAllocationFacts {
    bytes: usize,
    requests: usize,
}
impl SourceCustodyAllocationFacts {
    pub const fn bytes(self) -> usize {
        self.bytes
    }
    pub const fn requests(self) -> usize {
        self.requests
    }
    pub(crate) const fn empty() -> Self {
        Self {
            bytes: 0,
            requests: 0,
        }
    }
    pub(crate) const fn layout(layout: Layout) -> Self {
        Self {
            bytes: layout.size(),
            requests: if layout.size() == 0 { 0 } else { 1 },
        }
    }
    fn plus(self, other: Self) -> Result<Self, KernelFailure> {
        Ok(Self {
            bytes: add(self.bytes, other.bytes)?,
            requests: add(self.requests, other.requests)?,
        })
    }
    fn times(self, count: usize) -> Result<Self, KernelFailure> {
        Ok(Self {
            bytes: multiply(self.bytes, count)?,
            requests: multiply(self.requests, count)?,
        })
    }
}

/// Pure preparation prefix. The caller funds this BEFORE the first flush.
/// Actual callback and destructor requests remain the caller's own premise.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceCustodyPrefixFacts {
    requests: SourceCustodyAllocationFacts,
    failure_retained_upper: usize,
}
impl SourceCustodyPrefixFacts {
    pub const fn requests(self) -> SourceCustodyAllocationFacts {
        self.requests
    }
    pub const fn failure_retained_upper(self) -> usize {
        self.failure_retained_upper
    }
}
pub const fn source_custody_prefix_facts() -> SourceCustodyPrefixFacts {
    SourceCustodyPrefixFacts {
        requests: SourceCustodyAllocationFacts {
            bytes: crate::MAX_ROW_ERROR_MESSAGE_BYTES,
            requests: 1,
        },
        failure_retained_upper: crate::MAX_ROW_ERROR_MESSAGE_BYTES,
    }
}

/// Typed host requests and GLOBAL requests are separately paid responsibilities.
/// Peak and retained fields are source-derived upper bounds, not actual stock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceCustodyRequestFacts {
    typed: SourceCustodyAllocationFacts,
    global: SourceCustodyAllocationFacts,
    typed_peak_upper: usize,
    typed_retained_upper: usize,
    global_peak_upper: usize,
    global_retained_upper: usize,
}
impl SourceCustodyRequestFacts {
    pub const fn typed(self) -> SourceCustodyAllocationFacts {
        self.typed
    }
    pub const fn global(self) -> SourceCustodyAllocationFacts {
        self.global
    }
    pub const fn typed_peak_upper(self) -> usize {
        self.typed_peak_upper
    }
    pub const fn typed_retained_upper(self) -> usize {
        self.typed_retained_upper
    }
    pub const fn global_peak_upper(self) -> usize {
        self.global_peak_upper
    }
    pub const fn global_retained_upper(self) -> usize {
        self.global_retained_upper
    }

    pub(crate) fn from_parts(
        typed: SourceCustodyAllocationFacts,
        global: SourceCustodyAllocationFacts,
    ) -> Result<Self, KernelFailure> {
        // Check the complete operation even though each payer remains separate.
        let _ = typed.plus(global)?;
        Ok(Self {
            typed,
            global,
            typed_peak_upper: typed.bytes,
            typed_retained_upper: typed.bytes,
            global_peak_upper: global.bytes,
            global_retained_upper: global.bytes,
        })
    }
    pub(crate) fn owned(
        layout: Layout,
        allocator_bytes: usize,
    ) -> Result<Self, ScalarResourceError> {
        let bytes = layout
            .size()
            .checked_add(allocator_bytes)
            .ok_or(ScalarResourceError::Arithmetic)?;
        Ok(Self {
            typed: SourceCustodyAllocationFacts { bytes, requests: 2 },
            global: SourceCustodyAllocationFacts::empty(),
            typed_peak_upper: bytes,
            typed_retained_upper: bytes,
            global_peak_upper: 0,
            global_retained_upper: 0,
        })
    }
}

pub(crate) fn model_failure(cause: ScalarResourceError) -> KernelFailure {
    match cause {
        ScalarResourceError::Arithmetic => KernelFailure::ResourceExhausted,
        ScalarResourceError::SourceModel(detail) | ScalarResourceError::Invariant(detail) => {
            invalid(detail)
        }
    }
}
fn add(left: usize, right: usize) -> Result<usize, KernelFailure> {
    left.checked_add(right)
        .ok_or(KernelFailure::ResourceExhausted)
}
fn multiply(left: usize, right: usize) -> Result<usize, KernelFailure> {
    left.checked_mul(right)
        .ok_or(KernelFailure::ResourceExhausted)
}
fn table<T>(count: usize) -> Result<SourceCustodyAllocationFacts, KernelFailure> {
    Layout::array::<T>(count)
        .map(SourceCustodyAllocationFacts::layout)
        .map_err(|_| KernelFailure::ResourceExhausted)
}
fn shared(payload: Layout) -> Result<SourceCustodyAllocationFacts, KernelFailure> {
    arc_layout(payload)
        .map(SourceCustodyAllocationFacts::layout)
        .map_err(|_| KernelFailure::ResourceExhausted)
}

/// Source construction decides which nodes are inline; the model does not
/// invent a make_array call for Map entries or physical run ends.
#[derive(Clone, Copy)]
pub(crate) enum SourceNodeRole {
    Rebox,
    MapEntries,
    RunEnds,
}
pub(crate) struct RequestSourceNode<'a> {
    pub(crate) data_type: &'a DataType,
    pub(crate) data_buffers: usize,
    pub(crate) child_count: usize,
    pub(crate) nulls: Option<&'a NullBuffer>,
    pub(crate) header_fields: usize,
    pub(crate) body: Option<Layout>,
    pub(crate) role: SourceNodeRole,
}
/// Layouts originate from the custody author's actual private types.
pub(crate) struct SourceRequestPricing {
    pub(crate) group: Layout,
    pub(crate) custodian: Layout,
    pub(crate) bufferless: Layout,
}

/// Child summaries live on the same source visitor's call stack. No heap
/// stack, address deduplication, ArrayData, or value interpretation is needed.
#[derive(Clone, Copy)]
pub(crate) struct RequestSummary {
    clone_data: SourceCustodyAllocationFacts,
    cheap_validation: SourceCustodyAllocationFacts,
    to_data: SourceCustodyAllocationFacts,
    wrap: SourceCustodyAllocationFacts,
    make: SourceCustodyAllocationFacts,
}
impl RequestSummary {
    pub(crate) const fn empty() -> Self {
        let empty = SourceCustodyAllocationFacts::empty();
        Self {
            clone_data: empty,
            cheap_validation: empty,
            to_data: empty,
            wrap: empty,
            make: empty,
        }
    }
    pub(crate) fn plus(self, child: Self) -> Result<Self, KernelFailure> {
        Ok(Self {
            clone_data: self.clone_data.plus(child.clone_data)?,
            cheap_validation: self.cheap_validation.plus(child.cheap_validation)?,
            to_data: self.to_data.plus(child.to_data)?,
            wrap: self.wrap.plus(child.wrap)?,
            make: self.make.plus(child.make)?,
        })
    }
    pub(crate) fn finish(
        self,
        pricing: &SourceRequestPricing,
        bufferless_root: bool,
    ) -> Result<SourceCustodyRequestFacts, KernelFailure> {
        let mut global = self.to_data.plus(self.wrap)?.plus(self.make)?;
        if bufferless_root {
            global = global.plus(shared(pricing.bufferless)?)?;
        }
        SourceCustodyRequestFacts::from_parts(
            SourceCustodyAllocationFacts::layout(pricing.group),
            global,
        )
    }
}

fn datatype_clone(
    data_type: &DataType,
    work: &mut crate::EvaluationCheckpoints<'_>,
) -> Result<SourceCustodyAllocationFacts, KernelFailure> {
    work.step()?;
    match data_type {
        DataType::Dictionary(key, value) => {
            SourceCustodyAllocationFacts::layout(Layout::new::<DataType>())
                .times(2)?
                .plus(datatype_clone(key, work)?)?
                .plus(datatype_clone(value, work)?)
        }
        _ => Ok(SourceCustodyAllocationFacts::empty()),
    }
}
fn union_slots(
    data_type: &DataType,
    work: &mut crate::EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    let DataType::Union(fields, _) = data_type else {
        return Err(invalid(
            "source custody indexed table requires an Arrow Union",
        ));
    };
    // This is Arrow's empty indexed-table construction, not a budget default.
    let mut maximum = 0;
    for (id, _) in fields.iter() {
        work.step()?;
        let id = usize::try_from(id)
            .map_err(|_| invalid("source Union has a negative Arrow field id"))?;
        maximum = maximum.max(id);
    }
    add(maximum, 1)
}
fn validation_table(data_type: &DataType) -> Result<SourceCustodyAllocationFacts, KernelFailure> {
    let count = match data_type {
        DataType::Null
        | DataType::Struct(_)
        | DataType::FixedSizeList(..)
        | DataType::RunEndEncoded(..) => 0,
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Union(_, UnionMode::Dense) => 2,
        DataType::Dictionary(key, _) => return validation_table(key),
        _ => 1,
    };
    table::<BufferSpec>(count)
}

pub(crate) fn summarize_source_node(
    node: RequestSourceNode<'_>,
    children: RequestSummary,
    pricing: &SourceRequestPricing,
    work: &mut crate::EvaluationCheckpoints<'_>,
) -> Result<RequestSummary, KernelFailure> {
    let buffers = table::<Buffer>(node.data_buffers)?;
    let child_table = table::<ArrayData>(node.child_count)?;
    let kind_clone = datatype_clone(node.data_type, work)?;
    let clone_data = kind_clone
        .plus(buffers)?
        .plus(child_table)?
        .plus(children.clone_data)?;
    let cheap_validation = validation_table(node.data_type)?.plus(children.cheap_validation)?;
    // Include force_validate's lawful superset. Cheap child validation recurses;
    // only this build root runs its full null/value checks.
    let mut validation = cheap_validation;
    if let DataType::FixedSizeList(field, width) = node.data_type {
        if !field.is_nullable() {
            if let Some(nulls) = node.nulls {
                let width = usize::try_from(*width)
                    .map_err(|_| invalid("source FixedSizeList has a negative Arrow width"))?;
                let bits = multiply(nulls.len(), width)?;
                let bytes = bits / 8 + usize::from(bits % 8 != 0);
                let payload = Layout::from_size_align(bytes, arrow_buffer::alloc::ALIGNMENT)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                validation = validation
                    .plus(SourceCustodyAllocationFacts::layout(payload))?
                    .plus(shared(
                        Layout::from_size_align(48, 8)
                            .map_err(|_| KernelFailure::ResourceExhausted)?,
                    )?)?;
            }
        }
    }
    if matches!(node.data_type, DataType::RunEndEncoded(..)) {
        validation = validation.plus(table::<Buffer>(1)?)?;
    }

    let mut header = kind_clone;
    match node.data_type {
        DataType::Struct(_) => header = header.plus(table::<ArrayRef>(node.child_count)?)?,
        DataType::Map(..) => header = header.plus(table::<ArrayRef>(node.header_fields)?)?,
        DataType::Union(..) => {
            header = header.plus(table::<Option<ArrayRef>>(union_slots(
                node.data_type,
                work,
            )?)?)?
        }
        _ => {}
    }
    let mut into_data = buffers.plus(child_table)?;
    match node.data_type {
        DataType::Utf8View | DataType::BinaryView => {
            let count = node
                .data_buffers
                .checked_sub(1)
                .ok_or(KernelFailure::ResourceExhausted)?;
            let element = size_of::<Buffer>();
            let minimum = if element == 1 {
                8
            } else if element <= 1024 {
                4
            } else {
                1
            };
            let grown = multiply(count, 2)?.max(add(count, 1)?).max(minimum);
            into_data = table::<Buffer>(count)?.plus(table::<Buffer>(grown)?)?;
        }
        DataType::Union(..) => {
            let element = size_of::<ArrayData>();
            let minimum = if element == 1 {
                8
            } else if element <= 1024 {
                4
            } else {
                1
            };
            let capacity = if node.child_count == 0 {
                0
            } else {
                node.child_count.max(minimum)
            };
            into_data = buffers.plus(table::<ArrayData>(capacity)?)?;
        }
        _ => {}
    }
    let mut to_data = header
        .plus(into_data)?
        .plus(children.to_data)?
        .plus(validation)?;
    if let DataType::Dictionary(key, _) = node.data_type {
        to_data = to_data.plus(validation_table(key)?)?;
    }

    // Existing guard + pinned enum/aggregate source prove Bytes <=48/align8.
    // This is an upper layout, never a private mirror or exact-size claim.
    let bytes =
        shared(Layout::from_size_align(48, 8).map_err(|_| KernelFailure::ResourceExhausted)?)?;
    let customs = shared(pricing.custodian)?
        .plus(bytes)?
        .times(add(node.data_buffers, usize::from(node.nulls.is_some()))?)?;
    let wrap = buffers
        .plus(child_table)?
        .plus(customs)?
        .plus(clone_data)?
        .plus(children.wrap)?
        .plus(validation)?;

    let make = if matches!(node.role, SourceNodeRole::RunEnds) {
        SourceCustodyAllocationFacts::empty()
    } else {
        let mut make = children.make;
        if matches!(node.role, SourceNodeRole::Rebox) {
            let body = node
                .body
                .ok_or_else(|| invalid("source custody lacks its actual Arrow rebox layout"))?;
            make = make.plus(shared(body)?)?;
        }
        match node.data_type {
            DataType::Struct(_) => {
                // Incoming child Vec is W reserve_exact(c), or S's len clone.
                let data = Layout::array::<ArrayData>(node.child_count)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                let refs = Layout::array::<ArrayRef>(node.child_count)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                let size = data.size().max(refs.size());
                let collected = Layout::from_size_align(size, data.align().max(refs.align()))
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                make = make
                    .plus(SourceCustodyAllocationFacts::layout(collected))?
                    .plus(children.clone_data)?;
            }
            DataType::FixedSizeList(..) => make = make.plus(children.clone_data)?,
            DataType::Utf8View | DataType::BinaryView => {
                let count = node
                    .data_buffers
                    .checked_sub(1)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                let table =
                    Layout::array::<Buffer>(count).map_err(|_| KernelFailure::ResourceExhausted)?;
                make = make.plus(shared(table)?)?;
            }
            DataType::Union(..) => {
                make = make.plus(table::<Option<ArrayRef>>(union_slots(
                    node.data_type,
                    work,
                )?)?)?
            }
            _ => {}
        }
        make
    };
    Ok(RequestSummary {
        clone_data,
        cheap_validation,
        to_data,
        wrap,
        make,
    })
}
