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

//! Borrowed selected collection reads from the sole checked constant owner.
//! These carrier reads do not authorize a logical domain or collection policy.

use super::{
    ConstantError, ConstantValue, Row, list_range, primitive_bytes, resolve_row,
    selected_scalar::selected_row, variable_bytes,
};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

/// One selected List<Int32>, retaining the original constant and child backing.
/// NULL elements remain distinct from a NULL root and from an invalid ordinal.
pub struct SelectedInt32List<'a> {
    source: &'a ConstantValue,
    child: &'a ArrayData,
    start: usize,
    len: usize,
}
/// One selected List<Utf8>, retaining the original constant and child backing.
/// This carrier view does not authorize a physical String or nominal domain.
pub struct SelectedUtf8List<'a> {
    source: &'a ConstantValue,
    child: &'a ArrayData,
    start: usize,
    len: usize,
}
/// One selected Map<Utf8, Utf8>, in its original entry order. Empty, duplicate
/// and NULL keys/values are preserved; no Unpivot-specific policy is imposed.
pub struct SelectedUtf8Map<'a> {
    source: &'a ConstantValue,
    keys: &'a ArrayData,
    values: &'a ArrayData,
    start: usize,
    len: usize,
}

fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, ConstantError>,
) -> Result<T, ConstantError> {
    if matches!(
        &result,
        Err(ConstantError::Control(_) | ConstantError::Limit(_))
    ) {
        return result;
    }
    work.finish()?;
    result
}

impl ConstantValue {
    pub fn int32_list_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<SelectedInt32List<'_>>, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = self.int32_list_in(&mut work);
        finish(work, result)
    }

    /// Borrow the selected collection using the caller's original meter.
    /// No entry or tail is published; items retain this exact source owner.
    pub fn int32_list_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<SelectedInt32List<'_>>, ConstantError> {
        let Some(row) = selected_row(self, work)? else {
            return Ok(None);
        };
        let valid = matches!(row.data.data_type(), DataType::List(field) if field.data_type() == &DataType::Int32);
        work.step()?;
        if !valid {
            return Err(ConstantError::Invalid(
                "selected constant is not a List<Int32>",
            ));
        }
        let range = list_range(row);
        work.step()?;
        let (start, end) = range?;
        Ok(Some(SelectedInt32List {
            source: self,
            child: &row.data.child_data()[0],
            start,
            len: end - start,
        }))
    }

    pub fn utf8_list_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<SelectedUtf8List<'_>>, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = self.utf8_list_in(&mut work);
        finish(work, result)
    }

    /// Borrow only List<Utf8> after resolving the selected root encoding.
    /// The caller separately validates the complete semantic FVT. No source,
    /// entry or tail is recreated, and a NULL root remains None.
    pub fn utf8_list_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<SelectedUtf8List<'_>>, ConstantError> {
        let Some(row) = selected_row(self, work)? else {
            return Ok(None);
        };
        let valid = matches!(row.data.data_type(), DataType::List(field)
            if field.data_type() == &DataType::Utf8);
        work.step()?;
        if !valid {
            return Err(ConstantError::Invalid(
                "selected constant is not a List<Utf8>",
            ));
        }
        let range = list_range(row);
        work.step()?;
        let (start, end) = range?;
        Ok(Some(SelectedUtf8List {
            source: self,
            child: &row.data.child_data()[0],
            start,
            len: end - start,
        }))
    }

    pub fn utf8_map_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<SelectedUtf8Map<'_>>, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = self.utf8_map_in(&mut work);
        finish(work, result)
    }

    /// Borrow VARCHAR keys and UNKNOWN values. An UNKNOWN value has no
    /// payload, so each selected entry is NULL.
    pub fn utf8_map_with_unknown_values_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<SelectedUtf8Map<'_>>, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = self.utf8_map_with_value_type_in(&mut work, &DataType::Null);
        finish(work, result)
    }

    /// Inspect the actual entry range of a selected non-resident map key.
    /// The caller validates the complete declared key and value types.
    pub fn unknown_map_is_empty_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<bool>, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = (|| {
            let Some(row) = selected_row(self, &mut work)? else {
                return Ok(None);
            };
            let valid = matches!(row.data.data_type(), DataType::Map(entries, _)
                if matches!(entries.data_type(), DataType::Struct(fields)
                    if fields.len() == 2
                        && fields[0].data_type() == &DataType::Null));
            work.step()?;
            if !valid {
                return Err(ConstantError::Invalid(
                    "selected constant is not a map with UNKNOWN keys",
                ));
            }
            let (start, end) = list_range(row)?;
            work.step()?;
            Ok(Some(start == end))
        })();
        finish(work, result)
    }

    /// Borrow the selected collection using the caller's original meter.
    /// No entry or tail is published; items retain this exact source owner.
    pub fn utf8_map_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<SelectedUtf8Map<'_>>, ConstantError> {
        self.utf8_map_with_value_type_in(work, &DataType::Utf8)
    }

    fn utf8_map_with_value_type_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
        value_type: &DataType,
    ) -> Result<Option<SelectedUtf8Map<'_>>, ConstantError> {
        let Some(row) = selected_row(self, work)? else {
            return Ok(None);
        };
        let valid = matches!(row.data.data_type(), DataType::Map(entries, _)
            if matches!(entries.data_type(), DataType::Struct(fields)
                if fields.len() == 2 && fields[0].data_type() == &DataType::Utf8
                    && fields[1].data_type() == value_type));
        work.step()?;
        if !valid {
            return Err(ConstantError::Invalid(
                "selected constant is not a Map<Utf8, Utf8>",
            ));
        }
        let range = list_range(row);
        work.step()?;
        let (start, end) = range?;
        // ConstantPool canonicalization has already applied Struct parent
        // offsets to its children. Reuse the original row addressing.
        let children = row.data.child_data()[0].child_data();
        Ok(Some(SelectedUtf8Map {
            source: self,
            keys: &children[0],
            values: &children[1],
            start,
            len: end - start,
        }))
    }
}

fn ordinal(
    start: usize,
    len: usize,
    index: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, ConstantError> {
    let valid = index < len;
    work.step()?;
    if !valid {
        return Err(ConstantError::Invalid(
            "selected collection item is outside its range",
        ));
    }
    let ordinal = start
        .checked_add(index)
        .ok_or(ConstantError::Limit("selected collection ordinal overflow"))?;
    work.step()?;
    Ok(ordinal)
}

impl<'a> SelectedInt32List<'a> {
    pub fn source(&self) -> &'a ConstantValue {
        self.source
    }
    pub const fn len(&self) -> usize {
        self.len
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn item(
        &self,
        index: usize,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<i32>, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = self.item_observed(index, &mut work);
        finish(work, result)
    }
    /// The caller owns entry/finish. Reuse its original meter across items;
    /// this method neither creates another meter nor publishes a tail.
    pub fn item_observed(
        &self,
        index: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<i32>, ConstantError> {
        let index = ordinal(self.start, self.len, index, work)?;
        let Some(row) = resolve_row(
            Row {
                data: self.child,
                index,
            },
            work,
        )?
        else {
            return Ok(None);
        };
        let value = primitive_bytes(row, 4).and_then(|bytes| {
            bytes
                .try_into()
                .map(i32::from_ne_bytes)
                .map_err(|_| ConstantError::Invalid("selected Int32 item has wrong width"))
        });
        work.step()?;
        value.map(Some)
    }
}

fn text<'a>(
    data: &'a ArrayData,
    index: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<&'a str>, ConstantError> {
    let Some(row) = resolve_row(Row { data, index }, work)? else {
        return Ok(None);
    };
    let bytes = variable_bytes(row);
    work.step()?;
    let bytes = bytes?;
    // The exact selected bytes are borrowed, never copied or rescanned by a
    // fictitious checkpoint loop. Standard UTF8 validation performs actual
    // length-dependent work inside these opaque boundaries; its internal
    // cooperation/temporary-cost admission is not claimed by this accessor.
    work.flush()?;
    let value = std::str::from_utf8(bytes)
        .map_err(|_| ConstantError::Invalid("selected collection item text is not UTF8"));
    work.flush()?;
    work.step()?;
    value.map(Some)
}

impl<'a> SelectedUtf8List<'a> {
    pub fn source(&self) -> &'a ConstantValue {
        self.source
    }
    pub const fn len(&self) -> usize {
        self.len
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn item(
        &self,
        index: usize,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<&'a str>, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = self.item_observed(index, &mut work);
        finish(work, result)
    }
    /// NULL payloads are never read. The caller owns entry/finish and may
    /// reuse one original meter across the selected range.
    pub fn item_observed(
        &self,
        index: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'a str>, ConstantError> {
        let index = ordinal(self.start, self.len, index, work)?;
        text(self.child, index, work)
    }
}

impl<'a> SelectedUtf8Map<'a> {
    pub fn source(&self) -> &'a ConstantValue {
        self.source
    }
    pub const fn len(&self) -> usize {
        self.len
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn item(
        &self,
        index: usize,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<(Option<&'a str>, Option<&'a str>), ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = self.item_observed(index, &mut work);
        finish(work, result)
    }
    /// NULL key/value payloads are never read. The caller owns entry/finish.
    pub fn item_observed(
        &self,
        index: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(Option<&'a str>, Option<&'a str>), ConstantError> {
        let index = ordinal(self.start, self.len, index, work)?;
        let key = text(self.keys, index, work)?;
        let value = if self.values.data_type() == &DataType::Null {
            work.step()?;
            None
        } else {
            text(self.values, index, work)?
        };
        Ok((key, value))
    }
}

#[cfg(test)]
#[path = "selected_collections_tests.rs"]
mod tests;
