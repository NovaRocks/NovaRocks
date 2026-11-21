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

use std::fmt;

use arrow_schema::{DataType, Field};

use crate::{
    CompileCheckpoints, CompileControlError, ExecutionValueTypeError, FunctionValueType,
    ValueLayout, ValueTypeError, ValueTypeStorageLifetime,
};

#[derive(Debug)]
pub enum AssignmentCommonTypeError {
    Control(CompileControlError),
    ValueType(ValueTypeError),
    Construction(ExecutionValueTypeError),
    IncompatibleShape(&'static str),
}

impl From<CompileControlError> for AssignmentCommonTypeError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

impl fmt::Display for AssignmentCommonTypeError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(output),
            Self::ValueType(error) => error.fmt(output),
            Self::Construction(error) => error.fmt(output),
            Self::IncompatibleShape(message) => output.write_str(message),
        }
    }
}

impl std::error::Error for AssignmentCommonTypeError {}

/// Select an assignment domain for CASE, VALUES and set operands. The
/// existing layout coercion supplies the physical carrier; nested semantics
/// are derived from the input value types, independently of Arrow metadata.
pub fn assignment_common_value_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, AssignmentCommonTypeError> {
    work.step()?;
    let nullable = left.nullable() || right.nullable();
    if left.data_type() == &DataType::Null {
        return right
            .with_nullable(nullable)
            .map_err(AssignmentCommonTypeError::ValueType);
    }
    if right.data_type() == &DataType::Null {
        return left
            .with_nullable(nullable)
            .map_err(AssignmentCommonTypeError::ValueType);
    }
    work.flush()?;
    let carrier = if let Some(common) = temporal_common(left.value_type(), right.value_type())? {
        common.carrier()
    } else {
        crate::wider_type(left.data_type(), right.data_type())
    };
    work.flush()?;
    let layout = ValueLayout::try_new(&carrier, ValueTypeStorageLifetime::pure())
        .map_err(AssignmentCommonTypeError::Construction)?;
    let value = common_semantics(
        layout.data_type(),
        left.value_type(),
        right.value_type(),
        nullable,
        work,
    )?;
    FunctionValueType::try_from_value(value, layout).map_err(AssignmentCommonTypeError::ValueType)
}

fn temporal_common(
    left: &crate::ValueTypeRef,
    right: &crate::ValueTypeRef,
) -> Result<Option<crate::TemporalType>, AssignmentCommonTypeError> {
    use crate::{TemporalType, ValueKind};
    let common = match (left.temporal(), right.temporal()) {
        (None, None) => return Ok(None),
        (Some(a), Some(b)) => a.common_type(b),
        (
            Some(ty @ (TemporalType::Timestamp(_) | TemporalType::TimestampWithTimeZone(_))),
            None,
        ) if matches!(right.kind(), ValueKind::Scalar(DataType::Date32)) => Some(ty),
        (
            None,
            Some(ty @ (TemporalType::Timestamp(_) | TemporalType::TimestampWithTimeZone(_))),
        ) if matches!(left.kind(), ValueKind::Scalar(DataType::Date32)) => Some(ty),
        _ => None,
    };
    common
        .map(Some)
        .ok_or(AssignmentCommonTypeError::IncompatibleShape(
            "assignment lacks a declared temporal common type",
        ))
}

fn common_domain(
    left: &crate::ValueTypeRef,
    right: &crate::ValueTypeRef,
    target: &DataType,
) -> crate::ValueLogicalType {
    use crate::{ValueKind, ValueLogicalType as V};
    if matches!(left.kind(), ValueKind::Scalar(DataType::Null)) {
        return right.domain().unwrap_or(V::Physical);
    }
    if matches!(right.kind(), ValueKind::Scalar(DataType::Null)) {
        return left.domain().unwrap_or(V::Physical);
    }
    if let Some(domain) = left.domain()
        && Some(domain) == right.domain()
        && domain.validate_carrier(target).is_ok()
    {
        return domain;
    }
    let integer = |value: &crate::ValueTypeRef| {
        matches!(
            value.kind(),
            ValueKind::Scalar(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64)
        )
    };
    if matches!(target, DataType::FixedSizeBinary(16))
        && ((left.domain() == Some(V::LargeInt) && integer(right))
            || (right.domain() == Some(V::LargeInt) && integer(left)))
    {
        return V::LargeInt;
    }
    V::Physical
}

fn compare_names(
    left: &str,
    right: &str,
    work: &mut crate::CompileCheckpoints<'_>,
) -> Result<std::cmp::Ordering, AssignmentCommonTypeError> {
    for (a, b) in left.bytes().zip(right.bytes()) {
        work.step()?;
        let ordering = a.cmp(&b);
        if ordering != std::cmp::Ordering::Equal {
            return Ok(ordering);
        }
    }
    work.step()?;
    Ok(left.len().cmp(&right.len()))
}
fn named_index(
    fields: &[crate::ValueField],
    work: &mut crate::CompileCheckpoints<'_>,
) -> Result<Vec<usize>, AssignmentCommonTypeError> {
    fn sift(
        indices: &mut [usize],
        fields: &[crate::ValueField],
        mut root: usize,
        end: usize,
        work: &mut crate::CompileCheckpoints<'_>,
    ) -> Result<(), AssignmentCommonTypeError> {
        let compare = |a: usize, b: usize, work: &mut crate::CompileCheckpoints<'_>| {
            Ok::<_, AssignmentCommonTypeError>(
                compare_names(fields[a].name(), fields[b].name(), work)?.then(a.cmp(&b)),
            )
        };
        while root < end / 2 {
            let mut child = root * 2 + 1;
            if child + 1 < end && compare(indices[child], indices[child + 1], work)?.is_lt() {
                child += 1;
            }
            if !compare(indices[root], indices[child], work)?.is_lt() {
                break;
            }
            indices.swap(root, child);
            work.step()?;
            root = child;
        }
        Ok(())
    }
    let mut indices = Vec::with_capacity(fields.len());
    for index in 0..fields.len() {
        work.step()?;
        indices.push(index);
    }
    let len = indices.len();
    for root in (0..len / 2).rev() {
        sift(&mut indices, fields, root, len, work)?;
    }
    for end in (1..len).rev() {
        indices.swap(0, end);
        work.step()?;
        sift(&mut indices, fields, 0, end, work)?;
    }
    Ok(indices)
}
fn named_child<'a>(
    fields: &'a [crate::ValueField],
    indices: &[usize],
    name: &str,
    fallback: usize,
    work: &mut crate::CompileCheckpoints<'_>,
) -> Result<&'a crate::ValueTypeRef, AssignmentCommonTypeError> {
    let (mut low, mut high) = (0, indices.len());
    while low < high {
        let mid = low + (high - low) / 2;
        if compare_names(fields[indices[mid]].name(), name, work)?.is_lt() {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    let index =
        if low < indices.len() && compare_names(fields[indices[low]].name(), name, work)?.is_eq() {
            indices[low]
        } else {
            fallback
        };
    Ok(fields[index].value_type())
}

fn common_semantics(
    target: &DataType,
    left: &crate::ValueTypeRef,
    right: &crate::ValueTypeRef,
    nullable: bool,
    work: &mut crate::CompileCheckpoints<'_>,
) -> Result<crate::ValueTypeRef, AssignmentCommonTypeError> {
    use crate::{ValueField, ValueKind, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime};
    work.step()?;
    let storage = ValueTypeStorageLifetime::pure();
    if matches!(left.kind(), ValueKind::Scalar(DataType::Null)) {
        return right
            .with_nullable(nullable, storage)
            .map_err(AssignmentCommonTypeError::Construction);
    }
    if matches!(right.kind(), ValueKind::Scalar(DataType::Null)) {
        return left
            .with_nullable(nullable, storage)
            .map_err(AssignmentCommonTypeError::Construction);
    }
    let child = |index: usize, field: &Field, work: &mut crate::CompileCheckpoints<'_>| {
        let left = left.child_type_at(index).ok_or_else(|| {
            AssignmentCommonTypeError::IncompatibleShape("common type left child is absent")
        })?;
        let right = right.child_type_at(index).ok_or_else(|| {
            AssignmentCommonTypeError::IncompatibleShape("common type right child is absent")
        })?;
        common_semantics(field.data_type(), left, right, field.is_nullable(), work)
    };
    let domain = common_domain(left, right, target);
    let kind = if let Some(temporal) = temporal_common(left, right)? {
        if temporal.carrier() != *target {
            return Err(AssignmentCommonTypeError::IncompatibleShape(
                "assignment lacks a compatible temporal carrier",
            ));
        }
        ValueKind::Temporal(temporal)
    } else if domain != ValueLogicalType::Physical {
        ValueKind::Nominal(domain)
    } else {
        match target {
            DataType::List(item) | DataType::LargeList(item) => {
                ValueKind::List(child(0, item, work)?)
            }
            DataType::FixedSizeList(item, length) => ValueKind::FixedSizeList {
                item: child(0, item, work)?,
                length: *length,
            },
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(AssignmentCommonTypeError::IncompatibleShape(
                        "common Map entries are not Struct",
                    ));
                };
                ValueKind::Map {
                    key: child(0, &fields[0], work)?,
                    value: child(1, &fields[1], work)?,
                }
            }
            DataType::Struct(target_fields) => {
                let (ValueKind::Struct(left_fields), ValueKind::Struct(right_fields)) =
                    (left.kind(), right.kind())
                else {
                    return Err(AssignmentCommonTypeError::IncompatibleShape(
                        "common Struct semantics do not match layout",
                    ));
                };
                if left_fields.len() != target_fields.len()
                    || right_fields.len() != target_fields.len()
                {
                    return Err(AssignmentCommonTypeError::IncompatibleShape(
                        "common Struct arity mismatch",
                    ));
                }
                let left_index = named_index(left_fields, work)?;
                let right_index = named_index(right_fields, work)?;
                let mut fields = Vec::with_capacity(target_fields.len());
                for (ordinal, field) in target_fields.iter().enumerate() {
                    let l = named_child(left_fields, &left_index, field.name(), ordinal, work)?;
                    let r = named_child(right_fields, &right_index, field.name(), ordinal, work)?;
                    let value =
                        common_semantics(field.data_type(), l, r, field.is_nullable(), work)?;
                    for _ in field.name().bytes() {
                        work.step()?;
                    }
                    fields.push(ValueField::new(
                        field.name().as_str(),
                        value,
                        storage.clone(),
                    ));
                }
                ValueKind::Struct(fields.into())
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ValueKind::String,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => ValueKind::Binary,
            scalar => ValueKind::Scalar(scalar.clone()),
        }
    };
    ValueTypeRef::try_new(kind, nullable, storage).map_err(AssignmentCommonTypeError::Construction)
}
