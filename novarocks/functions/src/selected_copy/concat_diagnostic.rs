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

//! Pre-format geometry for the ONE original Arrow concat diagnostic. Arrow's
//! DataType::Display allocates nested Strings; it
//! must not be called before admission, even with a counting fmt::Write sink.
//! This module measures their borrowed structural inputs, never renders an
//! alternative message or selects concat value semantics.
use super::copy_diagnostic::{OriginalCopyData, OriginalTakeDiagnosticFacts};
use super::{CopyError, add, buffer_extent, mul};
use crate::KernelFailure;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use arrow_array::Array;
use arrow_schema::{ArrowError, DataType, Field};
use std::fmt::{self, Write};

trait TypeFormatWork {
    type Error: From<CopyError>;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
}
impl TypeFormatWork for EvaluationCheckpoints<'_> {
    type Error = CopyError;
    fn step(&mut self) -> Result<(), CopyError> {
        EvaluationCheckpoints::step(self).map_err(CopyError::Control)
    }
    fn flush(&mut self) -> Result<(), CopyError> {
        EvaluationCheckpoints::flush(self).map_err(CopyError::Control)
    }
}
struct ObservedFormatWork<'a, W>(&'a mut W);
impl<E: From<CopyError>, W: FnMut(bool) -> Result<(), E>> TypeFormatWork
    for ObservedFormatWork<'_, W>
{
    type Error = E;
    fn step(&mut self) -> Result<(), E> {
        self.0(false)
    }
    fn flush(&mut self) -> Result<(), E> {
        self.0(true)
    }
}

fn debug_bytes<W: TypeFormatWork>(
    value: &impl fmt::Debug,
    work: &mut W,
) -> Result<usize, W::Error> {
    // The borrowed string/primitive/map Debug authors used below allocate no
    // backing. Their original opaque formatting work remains bracketed.
    struct Count(usize);
    impl Write for Count {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
            Ok(())
        }
    }
    work.flush()?;
    let mut count = Count(0);
    fmt::write(&mut count, format_args!("{value:?}")).map_err(|_| CopyError::arithmetic(None))?;
    work.flush()?;
    Ok(count.0)
}
fn string_peak(bytes: usize) -> Result<usize, CopyError> {
    // The original format! hint is <=2*rendered bytes. RawVec reserve grows
    // to max(required,2*previous,8); old and replacement can coexist. This
    // covers one original String construction, not an arbitrary message cap.
    mul(3, bytes.max(8))
}
#[derive(Default)]
struct TypeFormatGeometry {
    bytes: usize,
    temporary: usize,
}
impl TypeFormatGeometry {
    fn metadata<W: TypeFormatWork>(field: &Field, work: &mut W) -> Result<Self, W::Error> {
        if field.metadata().is_empty() {
            return Ok(Self::default());
        }
        let bytes = add(", metadata: ".len(), debug_bytes(field.metadata(), work)?)?;
        // Arrow60.1 FormatMetadata iterates the ordered Metadata directly.
        // Its actual format! String remains; no sorting table is constructed.
        Ok(Self {
            bytes,
            temporary: string_peak(bytes)?,
        })
    }
    fn field<W: TypeFormatWork>(field: &Field, work: &mut W) -> Result<Self, W::Error> {
        work.step()?;
        let child = Self::data_type(field.data_type(), work)?;
        let metadata = Self::metadata(field, work)?;
        let name = debug_bytes(field.name(), work)?;
        let nullable = if field.is_nullable() {
            0
        } else {
            "non-null ".len()
        };
        let bytes = add(
            add(add(name, ": ".len())?, nullable)?,
            add(child.bytes, metadata.bytes)?,
        )?;
        Ok(Self {
            bytes,
            temporary: add(
                add(child.temporary, metadata.temporary)?,
                string_peak(bytes)?,
            )?,
        })
    }
    fn list<W: TypeFormatWork>(
        field: &Field,
        prefix: usize,
        work: &mut W,
    ) -> Result<Self, W::Error> {
        work.step()?;
        let child = Self::data_type(field.data_type(), work)?;
        let metadata = Self::metadata(field, work)?;
        let name = if field.name() == "item" {
            0
        } else {
            add(", field: ''".len(), field.name().len())?
        };
        let nullable = if field.is_nullable() {
            0
        } else {
            "non-null ".len()
        };
        let bytes = add(
            add(prefix, nullable)?,
            add(child.bytes, add(name, metadata.bytes)?)?,
        )?;
        let name_peak = if name == 0 { 0 } else { string_peak(name)? };
        Ok(Self {
            bytes,
            temporary: add(add(child.temporary, metadata.temporary)?, name_peak)?,
        })
    }
    fn data_type<W: TypeFormatWork>(ty: &DataType, work: &mut W) -> Result<Self, W::Error> {
        work.step()?;
        match ty {
            DataType::List(field) => Self::list(field, "List()".len(), work),
            DataType::LargeList(field) => Self::list(field, "LargeList()".len(), work),
            DataType::ListView(field) => Self::list(field, "ListView()".len(), work),
            DataType::LargeListView(field) => Self::list(field, "LargeListView()".len(), work),
            DataType::FixedSizeList(field, width) => Self::list(
                field,
                add("FixedSizeList( x )".len(), debug_bytes(width, work)?)?,
                work,
            ),
            DataType::Struct(fields) => {
                let mut bytes = "Struct()".len();
                let mut temporary = mul(fields.len(), size_of::<String>())?;
                let mut joined = 0;
                for (ordinal, field) in fields.iter().enumerate() {
                    let field = Self::field(field, work)?;
                    if ordinal != 0 {
                        joined = add(joined, ", ".len())?;
                    }
                    joined = add(joined, field.bytes)?;
                    temporary = add(temporary, field.temporary)?;
                }
                bytes = add(bytes, joined)?;
                if !fields.is_empty() {
                    temporary = add(temporary, string_peak(joined)?)?;
                }
                Ok(Self { bytes, temporary })
            }
            DataType::Union(fields, mode) => {
                let mut bytes = add("Union()".len(), debug_bytes(mode, work)?)?;
                let mut temporary = mul(fields.len(), size_of::<String>())?;
                let mut joined = 0;
                for (ordinal, (id, field)) in fields.iter().enumerate() {
                    let field = Self::field(field, work)?;
                    let wrapped = add(add(debug_bytes(&id, work)?, ": ()".len())?, field.bytes)?;
                    if ordinal != 0 {
                        joined = add(joined, ", ".len())?;
                    }
                    joined = add(joined, wrapped)?;
                    temporary = add(temporary, add(field.temporary, string_peak(wrapped)?)?)?;
                }
                if !fields.is_empty() {
                    bytes = add(bytes, add(", ".len(), joined)?)?;
                    temporary = add(temporary, string_peak(joined)?)?;
                }
                Ok(Self { bytes, temporary })
            }
            DataType::Map(field, sorted) => {
                let field = Self::field(field, work)?;
                let marker = if *sorted { "sorted" } else { "unsorted" };
                Ok(Self {
                    bytes: add("Map(, )".len(), add(field.bytes, marker.len())?)?,
                    temporary: field.temporary,
                })
            }
            DataType::RunEndEncoded(ends, values) => {
                let ends = Self::field(ends, work)?;
                let values = Self::field(values, work)?;
                Ok(Self {
                    bytes: add("RunEndEncoded(, )".len(), add(ends.bytes, values.bytes)?)?,
                    temporary: add(ends.temporary, values.temporary)?,
                })
            }
            DataType::Dictionary(key, value) => {
                let key = Self::data_type(key, work)?;
                let value = Self::data_type(value, work)?;
                Ok(Self {
                    bytes: add("Dictionary(, )".len(), add(key.bytes, value.bytes)?)?,
                    temporary: add(key.temporary, value.temporary)?,
                })
            }
            // Every remaining Arrow60.1.0 variant is a direct primitive/numeric/
            // timezone write with no nested String or metadata constructor.
            // Primitive tokens match; Debug's long TimeUnit names conservatively
            // bound Display's shorter s/ms/µs/ns spelling without rendering it.
            atom => Ok(Self {
                bytes: debug_bytes(atom, work)?,
                temporary: 0,
            }),
        }
    }
}

/// Sparse Union's empty-unselected-child route now validates newly created
/// ArrayData. Its original error can render nested types; count the SAME
/// borrowed source formatting structure before the SDK owns the formatter.
/// No alternative error is rendered and no message cap is introduced.
pub(super) fn original_take_validation_diagnostic_observed<E: From<CopyError>>(
    ty: &DataType,
    observe: &mut impl FnMut(bool) -> Result<(), E>,
) -> Result<Option<(usize, usize)>, E> {
    fn has_sparse<W: TypeFormatWork>(ty: &DataType, work: &mut W) -> Result<bool, W::Error> {
        work.step()?;
        match ty {
            DataType::Union(_, arrow_schema::UnionMode::Sparse) => Ok(true),
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    if has_sparse(field.data_type(), work)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => has_sparse(f.data_type(), work),
            DataType::RunEndEncoded(_, values) => has_sparse(values.data_type(), work),
            DataType::Dictionary(_, values) => has_sparse(values, work),
            DataType::Struct(fields) => {
                for field in fields {
                    if has_sparse(field.data_type(), work)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            _ => Ok(false),
        }
    }
    let mut work = ObservedFormatWork(observe);
    if !has_sparse(ty, &mut work)? {
        return Ok(None);
    }
    let geometry = TypeFormatGeometry::data_type(ty, &mut work)?;
    // The new root build's validation errors contain at most a child type,
    // a parent type, and numeric lengths. Both rendered trees are subtrees of
    // this original root. The longest relevant validation literal is explicit.
    let mut n = usize::MAX;
    let mut digits = 1;
    while n >= 10 {
        n /= 10;
        digits += 1;
    }
    let description = add(
        "Sparse union child array # has length smaller than expected for union array ( < )"
            .len()
            .max("non-nullable child of type  contains nulls not present in parent ".len()),
        add(mul(2, geometry.bytes)?, mul(4, add(digits, 1)?)?)?,
    )?;
    let display = add(description, "Invalid argument error: ".len())?;
    let peak = add(
        mul(2, geometry.temporary)?,
        add(string_peak(description)?, string_peak(display)?)?,
    )?;
    Ok(Some((display, peak)))
}

pub(super) struct OriginalConcatDiagnosticFacts {
    full: OriginalTakeDiagnosticFacts,
}
impl OriginalConcatDiagnosticFacts {
    pub(super) fn try_new(
        sources: &[&dyn Array],
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, CopyError> {
        let mut type_bytes = 0;
        let mut type_temporary = 0;
        let mut maximum_type = 0;
        let mut maximum_name = 0;
        for source in sources {
            let geometry = TypeFormatGeometry::data_type(source.data_type(), work)?;
            type_bytes = add(type_bytes, geometry.bytes)?;
            maximum_type = maximum_type.max(geometry.bytes);
            type_temporary = add(type_temporary, geometry.temporary)?;
            // A borrowed whole-type Debug includes every original nested field
            // name and metadata. It allocates no String/join/sorting vector.
            maximum_name = maximum_name.max(debug_bytes(source.data_type(), work)?);
        }
        let mut n = usize::MAX;
        let mut digits = 1;
        while n >= 10 {
            n /= 10;
            digits += 1;
        }
        // Original mismatch writes the first type, at most nine further types,
        // optional ellipsis, and separators. Sum actual source format extents
        // conservatively instead of choosing another uniqueness/hash author.
        let mismatch = add(
            "It is not possible to concatenate arrays of different data types ()., ...".len(),
            add(type_bytes, mul(sources.len(), ", ".len())?)?,
        )?;
        // Longest pinned concat constructor/ArrayData validation literal;
        // their dynamic fields are at most three actual source type renders,
        // one borrowed field name, and four signed/unsigned machine numbers.
        let validation = add(
            "The offset + length of array should be less or equal to last value in the run_ends array. The last value of run_ends array is  and offset + length of array is ".len(),
            add(add(mul(3, maximum_type)?, maximum_name)?, mul(4, add(digits, 1)?)?)?,
        )?;
        let description = mismatch
            .max(validation)
            .max("concat requires input of at least one array".len());
        let display = add(description, "Invalid argument error: ".len())?;
        // std HashSet::with_capacity(11) has sixteen buckets at its 7/8 load
        // factor. Each original key is &DataType. The pinned std hashbrown
        // group width is at most 16 on x86_64/aarch64; include exact bucket
        // control bytes plus the largest group/padding, not another set.
        let mismatch_table = add(mul(16, size_of::<&DataType>())?, 2 * 16)?;
        let peak = add(
            add(mul(3, type_temporary)?, mismatch_table)?,
            add(
                add(string_peak(description)?, string_peak(display)?)?,
                string_peak(maximum_type)?,
            )?,
        )?;
        buffer_extent(peak, 1)?;
        Ok(Self {
            full: OriginalTakeDiagnosticFacts::from_original_geometry(display, peak),
        })
    }
    pub(super) fn operation_peak_bytes(&self) -> usize {
        self.full.operation_peak_bytes()
    }
    pub(super) fn retain(
        self,
        error: ArrowError,
        charge: OpaqueRetainedCharge,
        reservation: &mut OpaqueReservation,
    ) -> Result<OriginalCopyData, KernelFailure> {
        self.full.retain(error, charge, reservation)
    }
}

#[cfg(test)]
#[path = "concat_diagnostic_tests.rs"]
mod tests;
