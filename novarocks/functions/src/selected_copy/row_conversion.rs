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

//! Arrow-row 60.0 RowConverter::new STRUCTURES, not encoded row payload or
//! lexsort's different comparator. No converter, null array or row is made here.
use super::{CopyConstructionError, CopyError, add, mul};
use arrow_buffer::ScalarBuffer;
use arrow_row::{OwnedRow, Row, RowConverter, Rows, SortField};
use arrow_schema::DataType;
use novarocks_type_contract::{ValueSchemaCreation, owned_resources::layout::arc_layout};
use std::{
    alloc::Layout,
    mem::{align_of, size_of},
};

#[derive(Debug)]
pub enum RowConversionStructureFailure<H> {
    Construction(CopyConstructionError<H>),
    Unsupported(&'static str),
    ValueType(novarocks_type_contract::ValueTypeError),
}
impl<H> From<novarocks_type_contract::ValueTypeError> for RowConversionStructureFailure<H> {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}
impl<H> From<CopyConstructionError<H>> for RowConversionStructureFailure<H> {
    fn from(error: CopyConstructionError<H>) -> Self {
        Self::Construction(error)
    }
}
impl<H> From<CopyError> for RowConversionStructureFailure<H> {
    fn from(error: CopyError) -> Self {
        Self::Construction(error.into())
    }
}

struct Model<'a, 'temporary, 'retained, 'work, H> {
    temporary: &'a mut ValueSchemaCreation<'temporary, H>,
    retained: &'a mut ValueSchemaCreation<'retained, H>,
    observe: &'work mut dyn FnMut(bool) -> Result<(), H>,
}
impl<H> Model<'_, '_, '_, '_, H> {
    fn step(&mut self) -> Result<(), RowConversionStructureFailure<H>> {
        (self.observe)(false).map_err(|h| CopyConstructionError::Creator(h).into())
    }
    fn exact(
        &mut self,
        layout: Layout,
        retained: bool,
    ) -> Result<(), RowConversionStructureFailure<H>> {
        (self.observe)(true).map_err(CopyConstructionError::Creator)?;
        let result = if retained {
            self.retained.request_layout(layout)
        } else {
            self.temporary.request_layout(layout)
        };
        result.map_err(|h| CopyConstructionError::Creator(h).into())
    }
    fn opaque(
        &mut self,
        bytes: usize,
        retained: bool,
    ) -> Result<(), RowConversionStructureFailure<H>> {
        if bytes == 0 {
            return Ok(());
        }
        (self.observe)(true).map_err(CopyConstructionError::Creator)?;
        let result = if retained {
            self.retained.observe_opaque_upper_bound(bytes)
        } else {
            self.temporary.observe_opaque_upper_bound(bytes)
        };
        result.map_err(|h| CopyConstructionError::Creator(h).into())
    }
    fn table<T>(
        &mut self,
        len: usize,
        retained: bool,
    ) -> Result<(), RowConversionStructureFailure<H>> {
        if len == 0 {
            return Ok(());
        }
        self.exact(
            Layout::array::<T>(len).map_err(|source| CopyError::layout::<T>(len, source))?,
            retained,
        )
    }
    fn clone_type(&mut self, ty: &DataType) -> Result<(), RowConversionStructureFailure<H>> {
        self.step()?;
        if let DataType::Dictionary(key, value) = ty {
            self.exact(Layout::new::<DataType>(), true)?;
            self.clone_type(key)?;
            self.exact(Layout::new::<DataType>(), true)?;
            self.clone_type(value)?;
        }
        // Other variants copy scalar fields or share existing Field/Fields/
        // UnionFields/timezone Arcs. Caller loans only their finite type hold.
        Ok(())
    }
    fn checked_types<'s, I>(&mut self, types: I) -> Result<(), RowConversionStructureFailure<H>>
    where
        I: Iterator<Item = &'s DataType>,
    {
        for ty in types {
            self.supported(ty)?;
        }
        Ok(())
    }
    fn is_nested_observed(
        &mut self,
        ty: &DataType,
    ) -> Result<bool, RowConversionStructureFailure<H>> {
        self.step()?;
        Ok(match ty {
            DataType::Dictionary(_, value) => self.is_nested_observed(value)?,
            DataType::RunEndEncoded(_, value) => self.is_nested_observed(value.data_type())?,
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(..)
            | DataType::Struct(_)
            | DataType::Map(..)
            | DataType::Union(..) => true,
            _ => false,
        })
    }
    fn supported(&mut self, ty: &DataType) -> Result<(), RowConversionStructureFailure<H>> {
        self.step()?;
        // Preserve the existing admitted first-party resource profile. Arrow
        // 60 itself supports Map, but this upgrade does not widen the product's
        // row-conversion capability through allocation-model admission.
        if !self.is_nested_observed(ty)? {
            return Ok(());
        }
        match ty {
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _) => self.supported(f.data_type()),
            DataType::Struct(fields) => self.checked_types(fields.iter().map(|f| f.data_type())),
            DataType::RunEndEncoded(_, values) => self.supported(values.data_type()),
            DataType::Union(fields, _) => {
                self.checked_types(fields.iter().map(|(_, f)| f.data_type()))
            }
            _ => Err(RowConversionStructureFailure::Unsupported(
                "current row conversion resource profile does not admit this nested carrier",
            )),
        }
    }
    fn private_vector(
        &mut self,
        len: usize,
        width: usize,
        retained: bool,
    ) -> Result<(), RowConversionStructureFailure<H>> {
        if len == 0 {
            return Ok(());
        }
        // The pinned Result collection uses GenericShunt's zero lower hint.
        // Codec/Encoder are >1 and <1024 bytes from their public payload upper;
        // RawVec's minimum4 and doubling are the actual constructor growth law.
        let capacity = len
            .max(4)
            .checked_next_power_of_two()
            .ok_or_else(|| CopyError::arithmetic(None))?;
        let final_bytes = mul(capacity, width)?;
        // Older prospective buffers are temporary. The final conservative
        // private block upper belongs to its actual owning converter/encoder.
        let mut previous = 4;
        while previous < capacity {
            self.opaque(mul(previous, width)?, false)?;
            previous = previous
                .checked_mul(2)
                .ok_or_else(|| CopyError::arithmetic(None))?;
        }
        self.opaque(final_bytes, retained)
    }
    fn converter<'s, I>(&mut self, types: I) -> Result<(), RowConversionStructureFailure<H>>
    where
        I: Iterator<Item = &'s DataType> + ExactSizeIterator + Clone,
    {
        self.checked_types(types.clone())?;
        let count = types.len();
        // Actual caller/child Codec creates Vec<SortField>; RowConverter::new
        // later moves each type to its fresh Arc slice without another clone.
        self.table::<SortField>(count, false)?;
        for ty in types.clone() {
            self.clone_type(ty)?;
        }
        self.private_vector(count, codec_upper()?, true)?;
        for ty in types {
            self.codec(ty)?;
        }
        self.exact(
            arc_layout(
                Layout::array::<SortField>(count)
                    .map_err(|source| CopyError::layout::<SortField>(count, source))?,
            )
            .map_err(CopyConstructionError::from)?,
            true,
        )
    }
    fn codec(&mut self, ty: &DataType) -> Result<(), RowConversionStructureFailure<H>> {
        self.step()?;
        match ty {
            DataType::Dictionary(_, value) => {
                self.converter(std::iter::once(value.as_ref()))?;
                self.null_array(value)?;
                self.null_conversion(std::iter::once(value.as_ref()), 1)?;
                self.null_owned_row()
            }
            DataType::RunEndEncoded(_, value) => self.converter(std::iter::once(value.data_type())),
            _ if !self.is_nested_observed(ty)? => Ok(()),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _) => {
                self.converter(std::iter::once(field.data_type()))
            }
            DataType::Struct(fields) => {
                self.converter(fields.iter().map(|f| f.data_type()))?;
                self.table::<arrow_array::ArrayRef>(fields.len(), false)?;
                for field in fields.iter() {
                    self.null_array(field.data_type())?;
                }
                self.null_conversion(fields.iter().map(|f| f.data_type()), 1)?;
                self.null_owned_row()
            }
            DataType::Union(fields, _) => {
                self.table::<RowConverter>(fields.len(), true)?;
                self.table::<i8>(fields.len(), true)?;
                self.table::<OwnedRow>(fields.len(), true)?;
                for (_, field) in fields.iter() {
                    self.converter(std::iter::once(field.data_type()))?;
                    self.null_array(field.data_type())?;
                    self.null_conversion(std::iter::once(field.data_type()), 1)?;
                    self.null_owned_row()?;
                }
                Ok(())
            }
            _ => Err(RowConversionStructureFailure::Unsupported(
                "row conversion resource profile has no admitted constructor for this carrier",
            )),
        }
    }
    fn null_array(&mut self, ty: &DataType) -> Result<(), RowConversionStructureFailure<H>> {
        // SAME Arrow new_null_array structure author; no null array fabricated
        // for inspecting an invoice. All these arrays die inside Codec::new.
        super::construction::preflight_null_arrow_structure_observed(
            ty,
            1,
            self.temporary,
            &mut |boundary| (self.observe)(boundary),
        )?;
        Ok(())
    }
    fn null_owned_row(&mut self) -> Result<(), RowConversionStructureFailure<H>> {
        // OwnedRow.data is Box<[u8]>, NOT Arc. The existing row Vec->Box
        // conversion is payload allocation/shrink only. Its RowConfig just
        // shares the already observed fields Arc; there is no phantom header.
        self.step()
    }
    fn temporary_slice_headers(
        &mut self,
        ty: &DataType,
    ) -> Result<(), RowConversionStructureFailure<H>> {
        // These are Codec::encoder's actual List/Run null-sentinel child
        // slices. Reuse the sole borrowed layout walker and result-custody TYPE
        // envelope; no dummy array/to_data packet/second decoder is constructed.
        // This is conservative temporary wrapper/table geometry, not payload.
        let mut nodes = 0usize;
        let mut scratch = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
        // Stack initialization is real caller work, not an allocation invoice.
        for _ in 0..scratch.len() {
            self.step()?;
        }
        novarocks_type_contract::validate_arrow_layout_structure_with_scratch_observed(
            ty,
            &mut scratch,
            |event| {
                if matches!(event, novarocks_type_contract::ValueTypeVisit::TypeNode(_)) {
                    nodes = nodes
                        .checked_add(1)
                        .ok_or_else(|| CopyError::arithmetic(None))?;
                }
                self.step()
            },
        )?;
        let bytes = crate::arrow_result_custody::custody_type_metadata_upper_bound(nodes)
            .map_err(CopyError::Control)?;
        self.opaque(bytes, false)
    }
    fn null_conversion<'s, I>(
        &mut self,
        types: I,
        rows_bound: usize,
    ) -> Result<(), RowConversionStructureFailure<H>>
    where
        I: Iterator<Item = &'s DataType> + ExactSizeIterator + Clone,
    {
        let count = types.len();
        self.null_conversion_borrowed(&mut types.into_iter(), count, rows_bound)
    }
    fn null_conversion_borrowed(
        &mut self,
        types: &mut dyn Iterator<Item = &DataType>,
        count: usize,
        rows_bound: usize,
    ) -> Result<(), RowConversionStructureFailure<H>> {
        // Internal null sentinel conversion only. Offset/LengthTracker tables
        // are conservative bounded original structure, not encoded byte stock.
        self.opaque(mul(add(rows_bound, 1)?, size_of::<usize>())?, false)?;
        self.opaque(mul(rows_bound, size_of::<usize>())?, false)?;
        self.private_vector(count, encoder_upper()?, false)?;
        for ty in types {
            self.step()?;
            match ty {
                DataType::Dictionary(_, value) => {
                    self.null_conversion_borrowed(&mut std::iter::once(value.as_ref()), 1, 0)?
                }
                DataType::Struct(fields) => self.null_conversion_borrowed(
                    &mut fields.iter().map(|f| f.data_type()),
                    fields.len(),
                    rows_bound,
                )?,
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::ListView(field)
                | DataType::LargeListView(field) => {
                    self.temporary_slice_headers(field.data_type())?;
                    self.null_conversion_borrowed(&mut std::iter::once(field.data_type()), 1, 0)?
                }
                DataType::FixedSizeList(field, width) => self.null_conversion_borrowed(
                    &mut std::iter::once(field.data_type()),
                    1,
                    mul(
                        rows_bound,
                        usize::try_from(*width).map_err(|_| {
                            CopyError::Invalid("copy frozen carrier has a negative width")
                        })?,
                    )?,
                )?,
                DataType::RunEndEncoded(_, value) => {
                    self.temporary_slice_headers(value.data_type())?;
                    self.null_conversion_borrowed(
                        &mut std::iter::once(value.data_type()),
                        1,
                        rows_bound,
                    )?
                }
                DataType::Union(fields, _) => {
                    self.table::<Rows>(fields.len(), false)?;
                    self.table::<i8>(fields.len(), false)?;
                    for (_, field) in fields.iter() {
                        self.null_conversion_borrowed(
                            &mut std::iter::once(field.data_type()),
                            1,
                            rows_bound,
                        )?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}
fn codec_upper() -> Result<usize, CopyError> {
    // Arrow 60 Codec adds Map(RowConverter), dominated by the existing
    // Struct/Union payloads below. No mirrored enum or fabricated Layout:
    // largest public payload plus tag/alignment padding remains an upper.
    let payload = size_of::<(RowConverter, OwnedRow)>().max(size_of::<(
        Vec<RowConverter>,
        Vec<i8>,
        Vec<OwnedRow>,
    )>());
    add(payload, mul(2, align_of::<usize>())?)
}
fn encoder_upper() -> Result<usize, CopyError> {
    // Arrow 60 Encoder adds Map(Rows), dominated by the existing Rows/Row
    // and four public Union payloads. This is temporary opaque geometry only.
    let payload = size_of::<(Rows, Row<'static>)>().max(size_of::<(
        Vec<Rows>,
        Vec<i8>,
        ScalarBuffer<i8>,
        Option<ScalarBuffer<i32>>,
    )>());
    add(payload, mul(2, align_of::<usize>())?)
}

/// Inputs are previously validated immutable carrier declarations, borrowed
/// through a cloneable exact-size iterator. No Arrow metadata supplies semantics.
/// Observe the actual SortField Vec/type clones, RowConverter fields Arc,
/// private codec vectors and internal null-sentinel constructor structures.
/// The caller invokes ONE original RowConverter::new only after success, keeps
/// temporary through it, and retains the finite marker plus original TYPE hold
/// in its converter wrapper. Never retain key arrays to represent this hold.
/// This does not quote arbitrary convert_columns payload/OwnedRow buffers or
/// lexsort comparators; those have distinct original operation owners.
pub fn preflight_row_converter_structure<'s, H, I>(
    types: I,
    temporary: &mut ValueSchemaCreation<'_, H>,
    retained: &mut ValueSchemaCreation<'_, H>,
    observe: &mut impl FnMut(bool) -> Result<(), H>,
) -> Result<(), RowConversionStructureFailure<H>>
where
    I: Iterator<Item = &'s DataType> + ExactSizeIterator + Clone,
{
    let mut model = Model {
        temporary,
        retained,
        observe,
    };
    model.converter(types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, ArrayRef, DictionaryArray, Int32Array, ListArray, StringArray, StructArray,
        types::Int32Type,
    };
    use arrow_schema::Field;
    use novarocks_type_contract::ValueTypeStorageLifetime;
    use std::{cell::Cell, sync::Arc};

    #[test]
    fn row_converter_constructor_structure_preserves_original_nested_rows() {
        let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(3), None]),
            None,
            Some(vec![Some(1)]),
        ]));
        let structure: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("n", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(3), None, Some(1)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("s", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("c"), None, Some("a")])) as ArrayRef,
            ),
        ]));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), None, Some(1)]),
                Arc::new(StringArray::from(vec!["c", "a"])),
            )
            .unwrap(),
        );
        let arrays = [list, structure, dictionary];
        let temporary_requests = Cell::new(0usize);
        let retained_requests = Cell::new(0usize);
        let mut temporary_exact = |_: Layout| {
            temporary_requests.set(temporary_requests.get() + 1);
            Ok::<_, &'static str>(())
        };
        let mut temporary_opaque = |_: usize| {
            temporary_requests.set(temporary_requests.get() + 1);
            Ok::<_, &'static str>(())
        };
        let mut retained_exact = |_: Layout| {
            retained_requests.set(retained_requests.get() + 1);
            Ok::<_, &'static str>(())
        };
        let mut retained_opaque = |_: usize| {
            retained_requests.set(retained_requests.get() + 1);
            Ok::<_, &'static str>(())
        };
        let mut temporary = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut temporary_exact,
            &mut temporary_opaque,
        );
        let mut retained = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut retained_exact,
            &mut retained_opaque,
        );
        preflight_row_converter_structure(
            arrays.iter().map(|a| a.data_type()),
            &mut temporary,
            &mut retained,
            &mut |_| Ok(()),
        )
        .unwrap();
        assert!(temporary_requests.get() > 0 && retained_requests.get() > 0);
        // Run the actual pinned constructor and codec, not a mirrored encoder.
        let converter = RowConverter::new(
            arrays
                .iter()
                .map(|a| SortField::new(a.data_type().clone()))
                .collect(),
        )
        .unwrap();
        let rows = converter.convert_columns(&arrays).unwrap();
        assert_eq!(rows.num_rows(), 3);
        assert_ne!(rows.row(0), rows.row(1));
        assert_ne!(rows.row(1), rows.row(2));
        let restored = converter.convert_rows(&rows).unwrap();
        // Arrow decodes dictionary values into their value carrier. Re-encode
        // those actual returned declarations instead of reusing encoded fields.
        assert_eq!(restored[2].data_type(), &DataType::Utf8);
        let restored_converter = RowConverter::new(
            restored
                .iter()
                .map(|array| SortField::new(array.data_type().clone()))
                .collect(),
        )
        .unwrap();
        let restored_rows = restored_converter.convert_columns(&restored).unwrap();
        assert!(rows.iter().zip(restored_rows.iter()).all(|(a, b)| a == b));
    }

    #[test]
    fn row_converter_structure_first_host_refusal_has_no_tail_or_retained_request() {
        let events = Cell::new(0usize);
        let at_refusal = Cell::new(0usize);
        let retained_requests = Cell::new(0usize);
        let mut first = |_: Layout| {
            at_refusal.set(events.get());
            Err("first original host")
        };
        let mut unexpected = |_: usize| panic!("opaque after first refusal");
        let mut retained_exact = |_: Layout| {
            retained_requests.set(retained_requests.get() + 1);
            Ok(())
        };
        let mut retained_opaque = |_: usize| {
            retained_requests.set(retained_requests.get() + 1);
            Ok(())
        };
        let mut temporary = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut first,
            &mut unexpected,
        );
        let mut retained = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut retained_exact,
            &mut retained_opaque,
        );
        let failure = preflight_row_converter_structure(
            std::iter::once(&DataType::Int32),
            &mut temporary,
            &mut retained,
            &mut |_| {
                events.set(events.get() + 1);
                Ok(())
            },
        );
        assert!(matches!(
            failure,
            Err(RowConversionStructureFailure::Construction(
                CopyConstructionError::Creator("first original host")
            ))
        ));
        assert_eq!(events.get(), at_refusal.get());
        assert_eq!(retained_requests.get(), 0);
        // This original unsupported carrier is refused before any constructor
        // quote; no default codec, metadata inference or capability is added.
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, false)),
                        Arc::new(Field::new("value", DataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let unsupported = preflight_row_converter_structure(
            std::iter::once(&map),
            &mut temporary,
            &mut retained,
            &mut |_| Ok(()),
        );
        assert!(matches!(
            unsupported,
            Err(RowConversionStructureFailure::Unsupported(_))
        ));
        assert_eq!(retained_requests.get(), 0);
    }
}
