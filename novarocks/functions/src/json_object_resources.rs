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

//! Borrowed checked geometry for the shared JSONObject computation.
//! These facts allocate no payload and grant no host authority. They describe
//! physical occurrences, including prefixes of unsupported scalar reads.

use crate::aggregate_scalar::AggScalarValue;
use crate::{KernelEvaluationControl, KernelFailure};
use arrow_array::{
    Array, ArrayRef, BinaryArray, LargeBinaryArray, ListArray, MapArray, StringArray, StructArray,
};
use arrow_schema::{DataType, Field};
use std::alloc::Layout;

fn add(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_mul(b).ok_or(KernelFailure::ResourceExhausted)
}
fn quoted_debug(bytes: usize) -> Result<usize, KernelFailure> {
    add(mul(bytes, 10)?, 2)
}
fn sequence_syntax(count: usize) -> Result<usize, KernelFailure> {
    add(2, mul(count.saturating_sub(1), 2)?)
}

/// Cumulative requests and peak stock for exact-copy scalar backing. Each
/// occurrence is counted even when several addresses borrow the same source.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CopyGeometry {
    pub(crate) bytes: usize,
    pub(crate) requests: usize,
}
impl CopyGeometry {
    fn exact(count: usize, element: Layout) -> Result<Self, KernelFailure> {
        let bytes = mul(count, element.size())?;
        Layout::from_size_align(bytes, element.align())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        Ok(Self {
            bytes,
            requests: usize::from(bytes != 0),
        })
    }
    fn plus(self, other: Self) -> Result<Self, KernelFailure> {
        Ok(Self {
            bytes: add(self.bytes, other.bytes)?,
            requests: add(self.requests, other.requests)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FormatGeometry {
    Null,
    Bool,
    Integer,
    Float,
    Date,
    Timestamp,
    Decimal128,
    Decimal256,
    Text {
        bytes: usize,
    },
    Binary {
        bytes: usize,
    },
    Complex {
        inner_debug: usize,
        scalar_debug: usize,
    },
}
impl FormatGeometry {
    fn option_debug(self) -> Result<usize, KernelFailure> {
        Ok(match self {
            Self::Null => 4,
            _ => add(6, self.scalar_debug()?)?,
        })
    }
    fn scalar_debug(self) -> Result<usize, KernelFailure> {
        let (label, payload) = match self {
            Self::Null => return Ok(4),
            Self::Bool => (4, 5),
            Self::Integer => (5, 20),
            Self::Float => (7, 24),
            Self::Date => (6, 11),
            Self::Timestamp => (9, 20),
            Self::Decimal128 => (10, 40),
            Self::Decimal256 => (10, 78),
            Self::Text { bytes } => (4, quoted_debug(bytes)?),
            Self::Binary { bytes } => (6, add(mul(bytes, 5)?, 2)?),
            Self::Complex { scalar_debug, .. } => return Ok(scalar_debug),
        };
        add(add(label, 2)?, payload)
    }
}
/// FailureOnly does not represent a successful NULL, value or empty fragment.
/// The real reader decides if this occurrence is reached at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScalarGeometry {
    Known {
        copy: CopyGeometry,
        format: FormatGeometry,
    },
    FailureOnly {
        copy: CopyGeometry,
        diagnostic_bytes: usize,
    },
}
impl ScalarGeometry {
    pub(crate) fn copy(self) -> CopyGeometry {
        match self {
            Self::Known { copy, .. } | Self::FailureOnly { copy, .. } => copy,
        }
    }
    pub(crate) fn diagnostic_bytes(self) -> usize {
        match self {
            Self::FailureOnly {
                diagnostic_bytes, ..
            } => diagnostic_bytes,
            _ => 0,
        }
    }
}

fn field_debug(
    field: &Field,
    control: &dyn KernelEvaluationControl,
) -> Result<usize, KernelFailure> {
    control.checkpoint(1)?;
    let mut metadata = add(2, mul(field.metadata().len(), 8)?)?;
    for (key, value) in field.metadata() {
        control.checkpoint(1)?;
        metadata = add(metadata, mul(add(key.len(), value.len())?, 10)?)?;
    }
    // Cover private dict_id/dict_is_ordered even when the public getters return
    // None on a non-Dictionary Field. Do not normalize or clone metadata.
    add(
        add(
            add(111, quoted_debug(field.name().len())?)?,
            type_debug(field.data_type(), control)?,
        )?,
        metadata,
    )
}

/// Exhaustive default Debug geometry of pinned Arrow DataType/Field authors.
/// This is a borrowed extent walk, never a formatter or an unsupported verdict.
fn type_debug(
    ty: &DataType,
    control: &dyn KernelEvaluationControl,
) -> Result<usize, KernelFailure> {
    control.checkpoint(1)?;
    use DataType::*;
    Ok(match ty {
        Null => 4,
        Boolean => 7,
        Int8 => 4,
        Int16 | Int32 | Int64 => 5,
        UInt8 => 5,
        UInt16 | UInt32 | UInt64 => 6,
        Float16 | Float32 | Float64 => 7,
        Date32 | Date64 => 6,
        Binary => 6,
        LargeBinary => 11,
        BinaryView => 10,
        Utf8 => 4,
        LargeUtf8 => 9,
        Utf8View => 8,
        Timestamp(_, zone) => add(
            24,
            match zone {
                None => 4,
                Some(zone) => add(6, quoted_debug(zone.len())?)?,
            },
        )?,
        Time32(_) | Time64(_) => 19,
        Duration(_) => 21,
        Interval(_) => 22,
        FixedSizeBinary(_) => 28,
        List(f) => add(6, field_debug(f, control)?)?,
        ListView(f) => add(10, field_debug(f, control)?)?,
        LargeList(f) => add(11, field_debug(f, control)?)?,
        LargeListView(f) => add(15, field_debug(f, control)?)?,
        FixedSizeList(f, _) => add(28, field_debug(f, control)?)?,
        Struct(fields) => {
            let mut total = add(8, sequence_syntax(fields.len())?)?;
            for field in fields {
                total = add(total, field_debug(field, control)?)?;
            }
            total
        }
        Union(fields, _) => {
            let mut total = add(15, sequence_syntax(fields.len())?)?;
            for (_, field) in fields.iter() {
                total = add(total, add(8, field_debug(field, control)?)?)?;
            }
            total
        }
        Dictionary(key, value) => add(
            add(14, type_debug(key, control)?)?,
            type_debug(value, control)?,
        )?,
        Decimal32(_, _) | Decimal64(_, _) => 20,
        Decimal128(_, _) | Decimal256(_, _) => 21,
        Map(f, _) => add(12, field_debug(f, control)?)?,
        RunEndEncoded(ends, values) => add(
            add(17, field_debug(ends, control)?)?,
            field_debug(values, control)?,
        )?,
    })
}

fn carrier<A: Array + 'static>(array: &ArrayRef) -> Result<&A, KernelFailure> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        crate::kernel_control::internal(
            "json_object resource geometry requires its exact Arrow carrier",
        )
    })
}

/// Only null bits, offsets, child structure and resident byte extents are read.
/// No numeric value, AggScalarValue, JSON parser or rendered text is produced.
pub(crate) fn scalar_geometry(
    array: &ArrayRef,
    row: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<ScalarGeometry, KernelFailure> {
    control.checkpoint(1)?;
    if row >= array.len() {
        return Err(crate::kernel_control::internal(
            "json_object resource address exceeds carrier",
        ));
    }
    use DataType::*;
    let supported = matches!(
        array.data_type(),
        Null | Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | Float32
            | Float64
            | Utf8
            | Binary
            | LargeBinary
            | Date32
            | Timestamp(_, _)
            | Decimal128(_, _)
            | Decimal256(_, _)
            | List(_)
            | Struct(_)
            | Map(_, _)
    ) || matches!(array.data_type(), FixedSizeBinary(16));
    if !supported {
        return Ok(ScalarGeometry::FailureOnly {
            copy: CopyGeometry::default(),
            diagnostic_bytes: add(25, type_debug(array.data_type(), control)?)?,
        });
    }
    if array.is_null(row) || matches!(array.data_type(), Null) {
        return Ok(ScalarGeometry::Known {
            copy: CopyGeometry::default(),
            format: FormatGeometry::Null,
        });
    }
    let mut copy = CopyGeometry::default();
    let format = match array.data_type() {
        Boolean => FormatGeometry::Bool,
        Int8 | Int16 | Int32 | Int64 => FormatGeometry::Integer,
        Float32 | Float64 => FormatGeometry::Float,
        Date32 => FormatGeometry::Date,
        Timestamp(_, _) => FormatGeometry::Timestamp,
        Decimal128(_, _) | FixedSizeBinary(16) => FormatGeometry::Decimal128,
        Decimal256(_, _) => FormatGeometry::Decimal256,
        Utf8 => {
            let bytes = carrier::<StringArray>(array)?.value(row).len();
            copy = CopyGeometry::exact(bytes, Layout::new::<u8>())?;
            FormatGeometry::Text { bytes }
        }
        Binary | LargeBinary => {
            let bytes = if matches!(array.data_type(), Binary) {
                carrier::<BinaryArray>(array)?.value(row).len()
            } else {
                carrier::<LargeBinaryArray>(array)?.value(row).len()
            };
            copy = CopyGeometry::exact(bytes, Layout::new::<u8>())?;
            FormatGeometry::Binary { bytes }
        }
        List(_) | Struct(_) | Map(_, _) => return container_geometry(array, row, control),
        _ => unreachable!("supported non-null scalar geometry"),
    };
    Ok(ScalarGeometry::Known { copy, format })
}

struct ChildrenGeometry {
    copy: CopyGeometry,
    inner_debug: usize,
    diagnostic: usize,
    failed: bool,
}
impl ChildrenGeometry {
    fn child(&mut self, child: ScalarGeometry) -> Result<(), KernelFailure> {
        self.copy = self.copy.plus(child.copy())?;
        match child {
            ScalarGeometry::Known { format, .. } => {
                self.inner_debug = add(self.inner_debug, format.option_debug()?)?
            }
            ScalarGeometry::FailureOnly {
                diagnostic_bytes, ..
            } => {
                self.failed = true;
                self.diagnostic = self.diagnostic.max(diagnostic_bytes);
            }
        }
        Ok(())
    }
    fn finish(self, label: usize) -> Result<ScalarGeometry, KernelFailure> {
        if self.failed {
            Ok(ScalarGeometry::FailureOnly {
                copy: self.copy,
                diagnostic_bytes: self.diagnostic,
            })
        } else {
            Ok(ScalarGeometry::Known {
                copy: self.copy,
                format: FormatGeometry::Complex {
                    inner_debug: self.inner_debug,
                    scalar_debug: add(add(label, 2)?, self.inner_debug)?,
                },
            })
        }
    }
}
fn container_geometry(
    array: &ArrayRef,
    row: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<ScalarGeometry, KernelFailure> {
    match array.data_type() {
        DataType::List(_) => {
            let array = carrier::<ListArray>(array)?;
            let offsets = array.value_offsets();
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let count = end
                .checked_sub(start)
                .ok_or(KernelFailure::ResourceExhausted)?;
            let mut children = ChildrenGeometry {
                copy: CopyGeometry::exact(count, Layout::new::<Option<AggScalarValue>>())?,
                inner_debug: sequence_syntax(count)?,
                diagnostic: 0,
                failed: false,
            };
            for child in start..end {
                children.child(scalar_geometry(array.values(), child, control)?)?;
            }
            children.finish(4)
        }
        DataType::Struct(_) => {
            let array = carrier::<StructArray>(array)?;
            let mut children = ChildrenGeometry {
                copy: CopyGeometry::exact(
                    array.num_columns(),
                    Layout::new::<Option<AggScalarValue>>(),
                )?,
                inner_debug: sequence_syntax(array.num_columns())?,
                diagnostic: 0,
                failed: false,
            };
            for column in array.columns() {
                children.child(scalar_geometry(column, row, control)?)?;
            }
            children.finish(6)
        }
        DataType::Map(_, _) => {
            let array = carrier::<MapArray>(array)?;
            let offsets = array.value_offsets();
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let count = end
                .checked_sub(start)
                .ok_or(KernelFailure::ResourceExhausted)?;
            let mut children = ChildrenGeometry {
                copy: CopyGeometry::exact(
                    count,
                    Layout::new::<(Option<AggScalarValue>, Option<AggScalarValue>)>(),
                )?,
                inner_debug: sequence_syntax(count)?,
                diagnostic: 0,
                failed: false,
            };
            for child in start..end {
                // Pair punctuation; there is no enclosing Option around a pair.
                children.inner_debug = add(children.inner_debug, 4)?;
                children.child(scalar_geometry(array.keys(), child, control)?)?;
                children.child(scalar_geometry(array.values(), child, control)?)?;
            }
            children.finish(3)
        }
        _ => unreachable!("supported container geometry"),
    }
}

const GROWTH_REQUESTS: usize = usize::BITS as usize + 2;
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AllocationGeometry {
    pub(crate) requests: usize,
    pub(crate) requested_bytes: usize,
    pub(crate) peak_bytes: usize,
}
impl AllocationGeometry {
    pub(crate) fn exact(bytes: usize) -> Self {
        Self {
            requests: usize::from(bytes != 0),
            requested_bytes: bytes,
            peak_bytes: bytes,
        }
    }
    pub(crate) fn plus(self, other: Self) -> Result<Self, KernelFailure> {
        Ok(Self {
            requests: add(self.requests, other.requests)?,
            requested_bytes: add(self.requested_bytes, other.requested_bytes)?,
            peak_bytes: add(self.peak_bytes, other.peak_bytes)?,
        })
    }
    fn growing(demand: usize, initial: usize, minimum: usize) -> Result<Self, KernelFailure> {
        let capacity = add(add(initial, mul(demand, 2)?)?, minimum)?;
        Ok(Self {
            requests: GROWTH_REQUESTS,
            requested_bytes: mul(GROWTH_REQUESTS, capacity)?,
            peak_bytes: mul(capacity, 2)?,
        })
    }
}
fn quote_bytes(bytes: usize) -> Result<usize, KernelFailure> {
    add(mul(bytes, 6)?, 2)
}
fn quote_allocation(bytes: usize) -> Result<AllocationGeometry, KernelFailure> {
    AllocationGeometry::growing(quote_bytes(bytes)?, 128, 8)
}
fn field_envelope(fields: &[Layout], alignment: usize) -> Result<Layout, KernelFailure> {
    let mut bytes = 0;
    let mut alignment = alignment;
    for field in fields {
        bytes = add(bytes, add(field.size(), field.align() - 1)?)?;
        alignment = alignment.max(field.align());
    }
    bytes = add(bytes, alignment - 1)?;
    Layout::from_size_align(bytes, alignment)
        .map(|layout| layout.pad_to_align())
        .map_err(|_| KernelFailure::ResourceExhausted)
}
fn btree_node_bytes() -> Result<usize, KernelFailure> {
    let leaf = field_envelope(
        &[
            Layout::new::<Option<std::ptr::NonNull<()>>>(),
            Layout::new::<u16>(),
            Layout::new::<u16>(),
            Layout::array::<String>(11).map_err(|_| KernelFailure::ResourceExhausted)?,
            Layout::array::<serde_json::Value>(11).map_err(|_| KernelFailure::ResourceExhausted)?,
        ],
        align_of::<(usize, u16, u16, [String; 11], [serde_json::Value; 11])>(),
    )?;
    Ok(field_envelope(
        &[
            leaf,
            Layout::array::<std::ptr::NonNull<()>>(12)
                .map_err(|_| KernelFailure::ResourceExhausted)?,
        ],
        leaf.align(),
    )?
    .size())
}
fn serde_error_bytes() -> Result<usize, KernelFailure> {
    let alignment = align_of::<(Box<str>, std::io::Error)>();
    let tag = Layout::from_size_align(alignment, alignment)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    let code = field_envelope(
        &[
            tag,
            Layout::new::<Box<str>>(),
            Layout::new::<std::io::Error>(),
        ],
        alignment,
    )?;
    Ok(field_envelope(
        &[code, Layout::new::<usize>(), Layout::new::<usize>()],
        align_of::<(Box<str>, std::io::Error, usize)>(),
    )?
    .size())
}

/// JSONObject's direct parser/error-drop route, not PARSE_JSON's wrapper
/// invoice. All numbers derive from the pinned original request families.
fn parser_geometry(bytes: usize) -> Result<(usize, AllocationGeometry), KernelFailure> {
    let nodes = add(bytes, 1)?;
    let parses = add(bytes / "$serde_json::private::RawValue".len(), 1)?;
    let parsed_nodes = mul(nodes, parses)?;
    let copied = mul(bytes, parses)?;
    let mut result = AllocationGeometry {
        requests: parsed_nodes,
        requested_bytes: copied,
        peak_bytes: copied,
    };
    let tree_nodes = add(parsed_nodes, 2)?;
    let trees = mul(tree_nodes, btree_node_bytes()?)?;
    result = result.plus(AllocationGeometry {
        requests: tree_nodes,
        requested_bytes: trees,
        peak_bytes: trees,
    })?;
    let arrays = mul(mul(parsed_nodes, 6)?, size_of::<serde_json::Value>())?;
    result = result.plus(AllocationGeometry {
        requests: mul(GROWTH_REQUESTS, parsed_nodes)?,
        requested_bytes: mul(GROWTH_REQUESTS, arrays)?,
        peak_bytes: mul(arrays, 2)?,
    })?;
    let scratch = add(mul(copied, 2)?, mul(parses, 16)?)?;
    result = result.plus(AllocationGeometry {
        requests: mul(GROWTH_REQUESTS, parses)?,
        requested_bytes: mul(GROWTH_REQUESTS, scratch)?,
        peak_bytes: mul(scratch, 2)?,
    })?;
    let canonical = add(mul(bytes, 6)?, mul(nodes, 32)?)?;
    result = result.plus(AllocationGeometry {
        requests: nodes,
        requested_bytes: mul(nodes, 24)?,
        peak_bytes: 24,
    })?;
    result = result.plus(AllocationGeometry::growing(canonical, 0, 8)?)?;
    let keys = mul(mul(nodes, 6)?, size_of::<&String>())?;
    result = result.plus(AllocationGeometry {
        requests: mul(GROWTH_REQUESTS, nodes)?,
        requested_bytes: mul(GROWTH_REQUESTS, keys)?,
        peak_bytes: mul(keys, 2)?,
    })?;
    let escaped = add(mul(nodes, 288)?, mul(bytes, 12)?)?;
    let ancestors = add(nodes, 1)?;
    let escaped_peak = mul(
        add(
            mul(ancestors, 136)?,
            mul(add(mul(bytes, 6)?, mul(ancestors, 2)?)?, 2)?,
        )?,
        2,
    )?;
    result = result.plus(AllocationGeometry {
        requests: mul(mul(GROWTH_REQUESTS, nodes)?, 2)?,
        requested_bytes: mul(GROWTH_REQUESTS, escaped)?,
        peak_bytes: escaped_peak,
    })?;
    let error = serde_error_bytes()?;
    result = result.plus(AllocationGeometry {
        requests: add(parsed_nodes, mul(parses, 12)?)?,
        requested_bytes: add(mul(add(parsed_nodes, parses)?, error)?, mul(parses, 720)?)?,
        peak_bytes: add(mul(error, 2)?, 720)?,
    })?;
    // No outer invalid-input String: parse_value Err is dropped before fallback.
    Ok((canonical, result))
}
#[derive(Clone, Copy, Debug)]
struct FragmentGeometry {
    bytes: Option<usize>,
    allocation: AllocationGeometry,
}
fn fragment_geometry(format: FormatGeometry, key: bool) -> Result<FragmentGeometry, KernelFailure> {
    use FormatGeometry::*;
    let (bytes, allocation) = match format {
        Null => {
            return Ok(FragmentGeometry {
                bytes: if key { None } else { Some(4) },
                allocation: AllocationGeometry::default(),
            });
        }
        Bool => (5, AllocationGeometry::exact(5)),
        Integer | Timestamp if key => (20, AllocationGeometry::exact(20)),
        Integer => (20, AllocationGeometry::exact(20)),
        Timestamp => (
            22,
            AllocationGeometry::exact(20).plus(quote_allocation(20)?)?,
        ),
        Float => (
            327,
            AllocationGeometry {
                requests: 7,
                requested_bytes: 1307,
                peak_bytes: 981,
            },
        ),
        Date if key => (
            13,
            AllocationGeometry {
                requests: 3,
                requested_bytes: 37,
                peak_bytes: 29,
            },
        ),
        Date => (
            quote_bytes(13)?,
            AllocationGeometry {
                requests: 3,
                requested_bytes: 37,
                peak_bytes: 29,
            }
            .plus(quote_allocation(13)?)?,
        ),
        Decimal128 => (40, AllocationGeometry::exact(40)),
        Decimal256 => (
            78,
            AllocationGeometry {
                requests: 2,
                requested_bytes: 86,
                peak_bytes: 86,
            },
        ),
        Text { bytes } if key => (bytes, AllocationGeometry::exact(bytes)),
        Text { bytes } => {
            let (canonical, parse) = parser_geometry(bytes)?;
            (canonical, parse.plus(quote_allocation(bytes)?)?)
        }
        Binary { bytes } => {
            let hex_bytes = mul(bytes, 2)?;
            let copy = AllocationGeometry::exact(if bytes == 0 { 0 } else { hex_bytes.max(8) });
            if key {
                (hex_bytes, copy)
            } else {
                (add(hex_bytes, 2)?, copy.plus(quote_allocation(hex_bytes)?)?)
            }
        }
        Complex { .. } if key => {
            return Ok(FragmentGeometry {
                bytes: None,
                allocation: AllocationGeometry::default(),
            });
        }
        Complex { inner_debug, .. } => (
            quote_bytes(inner_debug)?,
            AllocationGeometry::growing(inner_debug, 0, 8)?.plus(quote_allocation(inner_debug)?)?,
        ),
    };
    Ok(if key {
        FragmentGeometry {
            bytes: Some(quote_bytes(bytes)?),
            allocation: allocation.plus(quote_allocation(bytes)?)?,
        }
    } else {
        FragmentGeometry {
            bytes: Some(bytes),
            allocation,
        }
    })
}

#[derive(Default, Debug)]
pub(crate) struct JsonObjectBatchResources {
    text_bytes: usize,
    row_peak: usize,
    row_requests: usize,
    row_requested_bytes: usize,
    diagnostic_bytes: usize,
}
impl JsonObjectBatchResources {
    /// Fold one borrowed row. Unknown fragments remain absent; their copy
    /// prefixes and complete diagnostics still contribute to admission.
    pub(crate) fn add_row<A: crate::json_object_core::JsonObjectArguments + ?Sized>(
        &mut self,
        arguments: &A,
        ordinal: usize,
        batch_row: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let mut prefix = 2;
        let mut allocations = AllocationGeometry::default();
        for index in 0..arguments.len() {
            let scalar = scalar_geometry(
                arguments.array(index),
                arguments.value_row(index, ordinal, batch_row),
                control,
            )?;
            let copy = scalar.copy();
            allocations = allocations.plus(AllocationGeometry {
                requests: copy.requests,
                requested_bytes: copy.bytes,
                peak_bytes: copy.bytes,
            })?;
            self.diagnostic_bytes = self.diagnostic_bytes.max(scalar.diagnostic_bytes());
            if index % 2 == 0 {
                prefix = add(prefix, 3)?;
                if index + 1 == arguments.len() {
                    prefix = add(prefix, 4)?;
                }
            }
            if let ScalarGeometry::Known { format, .. } = scalar {
                let fragment = fragment_geometry(format, index % 2 == 0)?;
                allocations = allocations.plus(fragment.allocation)?;
                if let Some(bytes) = fragment.bytes {
                    prefix = add(prefix, bytes)?;
                }
            }
        }
        allocations = allocations.plus(AllocationGeometry::growing(prefix, 1, 8)?)?;
        self.text_bytes = add(self.text_bytes, prefix)?;
        self.row_peak = self.row_peak.max(allocations.peak_bytes);
        self.row_requests = add(self.row_requests, allocations.requests)?;
        self.row_requested_bytes = add(self.row_requested_bytes, allocations.requested_bytes)?;
        Ok(())
    }
    pub(crate) fn diagnostic_grant(&self) -> Result<usize, KernelFailure> {
        let bytes = self.diagnostic_bytes.max(178);
        mul(add(add(76, mul(bytes, 2)?)?, 8)?, 2)
    }
    /// The original computation returns at most one complete Data String.
    /// Its cumulative growth is separate from the opaque simultaneous stock.
    pub(crate) fn diagnostic_geometry(&self) -> Result<AllocationGeometry, KernelFailure> {
        AllocationGeometry::growing(self.diagnostic_bytes.max(178), 76, 8)
    }
    /// Body and original Data destination only. Entry validation, the direct
    /// slot and actual host callbacks must be composed by their own authors.
    pub(crate) fn body_and_data_geometry(
        &self,
        selected_rows: usize,
    ) -> Result<AllocationGeometry, KernelFailure> {
        let validation = Layout::array::<arrow_data::BufferSpec>(2)
            .map_err(|_| KernelFailure::ResourceExhausted)?
            .size();
        self.body_geometry(selected_rows)?
            .plus(self.diagnostic_geometry()?)?
            // force_validate may be enabled by a dependent crate. The three
            // original Utf8 builds each allocate this public layout once.
            .plus(AllocationGeometry {
                requests: 3,
                requested_bytes: mul(validation, 3)?,
                peak_bytes: validation,
            })
    }
    pub(crate) fn body_grant(&self, selected_rows: usize) -> Result<usize, KernelFailure> {
        Ok(self.body_geometry(selected_rows)?.peak_bytes)
    }
    /// Cumulative original requests are distinct from the simultaneous stock
    /// admitted by the opaque host. Typed headers keep their own exact author.
    fn body_geometry(&self, selected_rows: usize) -> Result<AllocationGeometry, KernelFailure> {
        let offsets = mul(
            add(add(1025, mul(add(selected_rows, 1)?, 2)?)?, 4)?,
            size_of::<i32>(),
        )?;
        let values = add(add(1024, mul(self.text_bytes, 2)?)?, 8)?;
        let validity = add(128, mul(add(add(selected_rows, 7)? / 8, 63)?, 2)?)?;
        let payload = add(add(offsets, values)?, validity)?;
        let word = size_of::<usize>();
        let buffer = size_of::<arrow_buffer::Buffer>();
        let array_arc = field_envelope(
            &[
                Layout::new::<usize>(),
                Layout::new::<usize>(),
                Layout::new::<StringArray>(),
            ],
            align_of::<StringArray>(),
        )?
        .size();
        let bytes_arc = mul(word, 8)?;
        let custodian_arc = mul(word, 3)?;
        let group = add(size_of::<arrow_data::ArrayData>(), mul(word, 7)?)?;
        let finish = add(
            add(
                add(add(mul(payload, 2)?, self.row_peak)?, mul(bytes_arc, 3)?)?,
                16,
            )?,
            add(mul(buffer, 4)?, array_arc)?,
        )?;
        let mut custody = payload;
        for part in [
            mul(bytes_arc, 3)?,
            array_arc,
            group,
            mul(add(custodian_arc, bytes_arc)?, 3)?,
            mul(buffer, 6)?,
            array_arc,
        ] {
            custody = add(custody, part)?;
        }
        let retained = add(
            add(payload, size_of::<StringArray>())?,
            crate::arrow_result_custody::custody_typed_node_metadata_upper_bound(
                &DataType::Utf8,
                2,
                true,
                0,
            )?,
        )?;
        let cause_bytes = mul(crate::MAX_ROW_ERROR_MESSAGE_BYTES, 2)?;
        let finish_bytes = add(
            add(mul(bytes_arc, 3)?, mul(buffer, 4)?)?,
            add(16, array_arc)?,
        )?;
        let custody_bytes = add(
            add(mul(buffer, 6)?, group)?,
            add(mul(add(custodian_arc, bytes_arc)?, 3)?, array_arc)?,
        )?;
        Ok(AllocationGeometry {
            requests: add(add(self.row_requests, mul(GROWTH_REQUESTS, 3)?)?, 19)?,
            requested_bytes: add(
                add(self.row_requested_bytes, mul(GROWTH_REQUESTS, payload)?)?,
                add(add(finish_bytes, custody_bytes)?, cause_bytes)?,
            )?,
            peak_bytes: add(finish.max(custody).max(retained), cause_bytes)?,
        })
    }
}

#[cfg(test)]
#[path = "json_object_resources_tests.rs"]
mod tests;
