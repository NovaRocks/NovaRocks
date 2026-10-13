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

//! Borrowed scalar copy and complete default type diagnostic geometry.
//! One traversal serves summaries without evaluating or formatting values.
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
    pub(crate) fn exact(count: usize, element: Layout) -> Result<Self, KernelFailure> {
        let bytes = mul(count, element.size())?;
        Layout::from_size_align(bytes, element.align())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        Ok(Self {
            bytes,
            requests: usize::from(bytes != 0),
        })
    }
    pub(crate) fn plus(self, other: Self) -> Result<Self, KernelFailure> {
        Ok(Self {
            bytes: add(self.bytes, other.bytes)?,
            requests: add(self.requests, other.requests)?,
        })
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
            arrow_default_debug_bytes_upper(field.data_type(), control)?,
        )?,
        metadata,
    )
}

/// Exhaustive default Debug geometry of pinned Arrow DataType/Field authors.
/// This is a borrowed extent walk, never a formatter or an unsupported verdict.
pub(crate) fn arrow_default_debug_bytes_upper(
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
            add(14, arrow_default_debug_bytes_upper(key, control)?)?,
            arrow_default_debug_bytes_upper(value, control)?,
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

#[derive(Clone, Copy, Debug)]
pub(crate) enum LeafExtent {
    Null,
    Bool,
    Integer,
    Float,
    Date,
    Timestamp,
    Decimal128,
    Decimal256,
    Text { bytes: usize },
    Binary { bytes: usize },
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum ContainerExtent {
    List,
    Struct,
    Map,
}
/// Two summaries share ONE carrier/NULL/child traversal and copy author.
pub(crate) trait GeometryFold: Sized {
    type Children;
    fn leaf(copy: CopyGeometry, extent: LeafExtent) -> Result<Self, KernelFailure>;
    fn failure(copy: CopyGeometry, diagnostic_bytes: usize) -> Self;
    fn begin(
        kind: ContainerExtent,
        count: usize,
        copy: CopyGeometry,
    ) -> Result<Self::Children, KernelFailure>;
    fn pair(children: &mut Self::Children) -> Result<(), KernelFailure>;
    fn child(children: &mut Self::Children, child: Self) -> Result<(), KernelFailure>;
    fn end(kind: ContainerExtent, children: Self::Children) -> Result<Self, KernelFailure>;
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ScalarCopyFacts {
    pub(crate) requested_bytes_upper: usize,
    pub(crate) requests_upper: usize,
    pub(crate) peak_bytes_upper: usize,
}
impl From<CopyGeometry> for ScalarCopyFacts {
    fn from(copy: CopyGeometry) -> Self {
        Self {
            requested_bytes_upper: copy.bytes,
            requests_upper: copy.requests,
            peak_bytes_upper: copy.bytes,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScalarCopyDiagnosticGeometry {
    Known {
        copy: ScalarCopyFacts,
    },
    FailureOnly {
        copy: ScalarCopyFacts,
        diagnostic_bytes_upper: usize,
    },
}
struct CopyDiagnosticFold {
    copy: CopyGeometry,
    diagnostic: usize,
    failed: bool,
}
impl GeometryFold for CopyDiagnosticFold {
    type Children = Self;
    fn leaf(copy: CopyGeometry, _: LeafExtent) -> Result<Self, KernelFailure> {
        Ok(Self {
            copy,
            diagnostic: 0,
            failed: false,
        })
    }
    fn failure(copy: CopyGeometry, diagnostic: usize) -> Self {
        Self {
            copy,
            diagnostic,
            failed: true,
        }
    }
    fn begin(_: ContainerExtent, _: usize, copy: CopyGeometry) -> Result<Self, KernelFailure> {
        Self::leaf(copy, LeafExtent::Null)
    }
    fn pair(_: &mut Self) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn child(parent: &mut Self, child: Self) -> Result<(), KernelFailure> {
        parent.copy = parent.copy.plus(child.copy)?;
        parent.diagnostic = parent.diagnostic.max(child.diagnostic);
        parent.failed |= child.failed;
        Ok(())
    }
    fn end(_: ContainerExtent, children: Self) -> Result<Self, KernelFailure> {
        Ok(children)
    }
}
pub(crate) fn scalar_copy_diagnostic_geometry(
    array: &ArrayRef,
    row: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<ScalarCopyDiagnosticGeometry, KernelFailure> {
    let folded = observe_scalar_geometry::<CopyDiagnosticFold>(array, row, control)?;
    let copy = folded.copy.into();
    Ok(if folded.failed {
        ScalarCopyDiagnosticGeometry::FailureOnly {
            copy,
            diagnostic_bytes_upper: folded.diagnostic,
        }
    } else {
        ScalarCopyDiagnosticGeometry::Known { copy }
    })
}
/// Only null bits, offsets, child structure and resident byte extents are read.
/// No numeric value, AggScalarValue, JSON parser or rendered text is produced.
pub(crate) fn observe_scalar_geometry<F: GeometryFold>(
    array: &ArrayRef,
    row: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<F, KernelFailure> {
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
        return Ok(F::failure(
            CopyGeometry::default(),
            add(
                25,
                arrow_default_debug_bytes_upper(array.data_type(), control)?,
            )?,
        ));
    }
    if array.is_null(row) || matches!(array.data_type(), Null) {
        return F::leaf(CopyGeometry::default(), LeafExtent::Null);
    }
    let mut copy = CopyGeometry::default();
    let format = match array.data_type() {
        Boolean => LeafExtent::Bool,
        Int8 | Int16 | Int32 | Int64 => LeafExtent::Integer,
        Float32 | Float64 => LeafExtent::Float,
        Date32 => LeafExtent::Date,
        Timestamp(_, _) => LeafExtent::Timestamp,
        Decimal128(_, _) | FixedSizeBinary(16) => LeafExtent::Decimal128,
        Decimal256(_, _) => LeafExtent::Decimal256,
        Utf8 => {
            let bytes = carrier::<StringArray>(array)?.value(row).len();
            copy = CopyGeometry::exact(bytes, Layout::new::<u8>())?;
            LeafExtent::Text { bytes }
        }
        Binary | LargeBinary => {
            let bytes = if matches!(array.data_type(), Binary) {
                carrier::<BinaryArray>(array)?.value(row).len()
            } else {
                carrier::<LargeBinaryArray>(array)?.value(row).len()
            };
            copy = CopyGeometry::exact(bytes, Layout::new::<u8>())?;
            LeafExtent::Binary { bytes }
        }
        List(_) | Struct(_) | Map(_, _) => return container_geometry::<F>(array, row, control),
        _ => unreachable!("supported non-null scalar geometry"),
    };
    F::leaf(copy, format)
}

fn container_geometry<F: GeometryFold>(
    array: &ArrayRef,
    row: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<F, KernelFailure> {
    match array.data_type() {
        DataType::List(_) => {
            let array = carrier::<ListArray>(array)?;
            let offsets = array.value_offsets();
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let count = end
                .checked_sub(start)
                .ok_or(KernelFailure::ResourceExhausted)?;
            let mut children = F::begin(
                ContainerExtent::List,
                count,
                CopyGeometry::exact(count, Layout::new::<Option<AggScalarValue>>())?,
            )?;
            for child in start..end {
                F::child(
                    &mut children,
                    observe_scalar_geometry::<F>(array.values(), child, control)?,
                )?;
            }
            F::end(ContainerExtent::List, children)
        }
        DataType::Struct(_) => {
            let array = carrier::<StructArray>(array)?;
            let mut children = F::begin(
                ContainerExtent::Struct,
                array.num_columns(),
                CopyGeometry::exact(array.num_columns(), Layout::new::<Option<AggScalarValue>>())?,
            )?;
            for column in array.columns() {
                F::child(
                    &mut children,
                    observe_scalar_geometry::<F>(column, row, control)?,
                )?;
            }
            F::end(ContainerExtent::Struct, children)
        }
        DataType::Map(_, _) => {
            let array = carrier::<MapArray>(array)?;
            let offsets = array.value_offsets();
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let count = end
                .checked_sub(start)
                .ok_or(KernelFailure::ResourceExhausted)?;
            let mut children = F::begin(
                ContainerExtent::Map,
                count,
                CopyGeometry::exact(
                    count,
                    Layout::new::<(Option<AggScalarValue>, Option<AggScalarValue>)>(),
                )?,
            )?;
            for child in start..end {
                // Pair punctuation; there is no enclosing Option around a pair.
                F::pair(&mut children)?;
                F::child(
                    &mut children,
                    observe_scalar_geometry::<F>(array.keys(), child, control)?,
                )?;
                F::child(
                    &mut children,
                    observe_scalar_geometry::<F>(array.values(), child, control)?,
                )?;
            }
            F::end(ContainerExtent::Map, children)
        }
        _ => unreachable!("supported container geometry"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KernelDiagnostic, json_object_resources::scalar_geometry};
    use arrow_array::{
        UInt32Array,
        builder::{ListBuilder, MapBuilder, StringBuilder, UInt32Builder},
    };
    use arrow_buffer::NullBuffer;
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[derive(Default)]
    struct Control {
        seen: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert_eq!(units, 1);
            let mut seen = self.seen.lock().unwrap();
            let at = seen.len();
            if let Some((stop, _)) = &self.refusal {
                assert!(at <= *stop);
            }
            seen.push(units);
            match &self.refusal {
                Some((stop, cause)) if at == *stop => Err(cause.clone()),
                _ => Ok(()),
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            unreachable!()
        }
    }
    fn nested() -> ArrayRef {
        let fields = vec![
            Arc::new(Field::new("text", DataType::Utf8, false)),
            Arc::new(Field::new("unsupported", DataType::UInt32, true)),
            Arc::new(Field::new("binary", DataType::Binary, false)),
        ]
        .into();
        Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(StringArray::from(vec!["abc"])),
                Arc::new(UInt32Array::from(vec![None])),
                Arc::new(BinaryArray::from_vec(vec![b"12345"])),
            ],
            None,
        ))
    }
    #[test]
    fn scalar_copy_geometry_both_summaries_share_copy_failure_and_observation_order() {
        let mut list = ListBuilder::new(StringBuilder::new());
        list.values().append_value("\0µ");
        list.values().append_null();
        list.append(true);
        let mut map = MapBuilder::new(None, StringBuilder::new(), UInt32Builder::new());
        map.keys().append_value("key");
        map.values().append_null();
        map.append(true).unwrap();
        let arrays: Vec<ArrayRef> = vec![
            nested(),
            Arc::new(list.finish()),
            Arc::new(map.finish()),
            Arc::new(UInt32Array::from(vec![None])),
            Arc::new(StringArray::from(vec!["bytes"])),
        ];
        for array in arrays {
            let json_control = Control::default();
            let json = scalar_geometry(&array, 0, &json_control).unwrap();
            let copy_control = Control::default();
            let geometry = scalar_copy_diagnostic_geometry(&array, 0, &copy_control).unwrap();
            let (copy, diagnostic) = match geometry {
                ScalarCopyDiagnosticGeometry::Known { copy } => (copy, 0),
                ScalarCopyDiagnosticGeometry::FailureOnly {
                    copy,
                    diagnostic_bytes_upper,
                } => (copy, diagnostic_bytes_upper),
            };
            assert_eq!(copy.requested_bytes_upper, json.copy().bytes);
            assert_eq!(copy.peak_bytes_upper, json.copy().bytes);
            assert_eq!(copy.requests_upper, json.copy().requests);
            assert_eq!(diagnostic, json.diagnostic_bytes());
            assert_eq!(
                *json_control.seen.lock().unwrap(),
                *copy_control.seen.lock().unwrap()
            );
        }
    }
    #[test]
    fn scalar_copy_geometry_null_parent_skips_unsupported_children() {
        let child: ArrayRef = Arc::new(UInt32Array::from(vec![None]));
        let parent: ArrayRef = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("child", DataType::UInt32, true))].into(),
            vec![child],
            Some(NullBuffer::from(vec![false])),
        ));
        let control = Control::default();
        assert_eq!(
            scalar_copy_diagnostic_geometry(&parent, 0, &control).unwrap(),
            ScalarCopyDiagnosticGeometry::Known {
                copy: ScalarCopyFacts::default()
            }
        );
        assert_eq!(*control.seen.lock().unwrap(), [1]);
    }
    #[test]
    fn scalar_copy_geometry_fold_does_not_evaluate_unused_formatter_arithmetic() {
        // This is a fold-only extent probe, not a fabricated Array carrier.
        assert_eq!(
            sequence_syntax(usize::MAX),
            Err(KernelFailure::ResourceExhausted)
        );
        let folded =
            CopyDiagnosticFold::begin(ContainerExtent::List, usize::MAX, CopyGeometry::default())
                .unwrap();
        assert_eq!(folded.copy, CopyGeometry::default());
        assert!(!folded.failed);
        let leaf = CopyDiagnosticFold::leaf(
            CopyGeometry::default(),
            LeafExtent::Text { bytes: usize::MAX },
        )
        .unwrap();
        assert_eq!(leaf.copy, CopyGeometry::default());
    }
    #[test]
    fn scalar_copy_geometry_every_reached_callback_keeps_all_seven_causes() {
        let array = nested();
        let reference = Control::default();
        scalar_copy_diagnostic_geometry(&array, 0, &reference).unwrap();
        let count = reference.seen.lock().unwrap().len();
        let diagnostic = KernelDiagnostic::new("original geometry refusal");
        let causes = [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(diagnostic.clone()),
            KernelFailure::Internal(diagnostic.clone()),
            KernelFailure::Operational(diagnostic),
            KernelFailure::InstanceFailed,
        ];
        for at in 0..count {
            for cause in &causes {
                let control = Control {
                    refusal: Some((at, cause.clone())),
                    ..Default::default()
                };
                assert_eq!(
                    scalar_copy_diagnostic_geometry(&array, 0, &control),
                    Err(cause.clone())
                );
                assert_eq!(control.seen.lock().unwrap().len(), at + 1);
            }
        }
    }
}
