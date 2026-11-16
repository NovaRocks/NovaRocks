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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};
use novarocks_parser::{ast, printer};

use crate::analyze_error::AnalyzeError;

/// Preserve the authored domain while changing a scope's null padding.
pub(super) fn with_nullability(
    ty: novarocks_type_contract::FunctionValueType,
    nullable: bool,
) -> novarocks_type_contract::FunctionValueType {
    ty.with_nullable(nullable)
        .expect("validated value root nullability transform")
}

/// A read-only SQL spelling projection of the sole value-domain identity.
pub(crate) fn sql_logical_projection(
    logical: novarocks_type_contract::ValueLogicalType,
) -> Option<novarocks_types::schema::SqlType> {
    use novarocks_type_contract::ValueLogicalType as V;
    use novarocks_types::schema::SqlType as S;
    match logical {
        V::Json => Some(S::Json),
        V::Hll => Some(S::Hll),
        V::Bitmap => Some(S::Bitmap),
        V::Variant => Some(S::Variant),
        V::LargeInt => Some(S::LargeInt),
        _ => None,
    }
}

enum ColumnProjectionError {
    Source(novarocks_types::ColumnValueTypeError),
    Control(novarocks_type_contract::CompileControlError),
}
impl From<novarocks_types::ColumnValueTypeError> for ColumnProjectionError {
    fn from(error: novarocks_types::ColumnValueTypeError) -> Self {
        Self::Source(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for ColumnProjectionError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Source(error.into())
    }
}

/// Catalog declaration validation borrows the original request's control.
pub(super) fn column_value_type(
    column: &novarocks_types::schema::ColumnDef,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_type_contract::FunctionValueType, AnalyzeError> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::Validate,
    )
    .map_err(AnalyzeError::control)?;
    let result = column
        .declared_value_type_observed(|| work.step().map_err(ColumnProjectionError::Control))
        .map_err(|error| match error {
            ColumnProjectionError::Control(error) => AnalyzeError::control(error),
            ColumnProjectionError::Source(error) => {
                AnalyzeError::internal(format!("invalid catalog column `{}`: {error}", column.name))
            }
        });
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.control_error().is_some())
    {
        return result;
    }
    work.finish().map_err(AnalyzeError::control)?;
    result
}

pub(super) fn validate_value_type(
    ty: &novarocks_type_contract::FunctionValueType,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), AnalyzeError> {
    use novarocks_functions::KernelFailure;
    use novarocks_type_contract::{CompileCheckpoints, CompileControlError, CompilePhase};
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(AnalyzeError::control)?;
    let result = novarocks_functions::validate_function_value_type_observed(ty, &mut work).map_err(
        |error| match error {
            KernelFailure::Cancelled => AnalyzeError::control(CompileControlError::Cancelled),
            KernelFailure::DeadlineExceeded => {
                AnalyzeError::control(CompileControlError::DeadlineExceeded)
            }
            KernelFailure::ResourceExhausted => {
                AnalyzeError::control(CompileControlError::ResourceExhausted)
            }
            error => AnalyzeError::internal(error.to_string()),
        },
    );
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.control_error().is_some())
    {
        return result;
    }
    work.finish().map_err(AnalyzeError::control)?;
    result
}

/// The SQL assignment-common owner preserves established domains and NULL
/// neutrality. Mixed JSON/string deliberately selects the existing STRING
/// domain; lowering must represent that semantic conversion by its exact owner,
/// rather than admitting an identity-erasing primitive carrier cast.
pub(super) fn assignment_common_value_type(
    left: &novarocks_type_contract::FunctionValueType,
    right: &novarocks_type_contract::FunctionValueType,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_type_contract::FunctionValueType, AnalyzeError> {
    use novarocks_type_contract::{
        CompileCheckpoints, CompilePhase, ValueLayout, ValueTypeStorageLifetime,
    };
    validate_value_type(left, control)?;
    validate_value_type(right, control)?;
    let nullable = left.nullable() || right.nullable();
    if left.data_type() == &DataType::Null {
        return Ok(with_nullability(right.clone(), nullable));
    }
    if right.data_type() == &DataType::Null {
        return Ok(with_nullability(left.clone(), nullable));
    }
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(AnalyzeError::control)?;
    // The established layout coercion remains the sole physical common-type
    // policy. The recursive semantic author below neither reads nor emits tags.
    work.flush().map_err(AnalyzeError::control)?;
    let carrier = novarocks_types::wider_type(left.data_type(), right.data_type());
    work.flush().map_err(AnalyzeError::control)?;
    let layout = ValueLayout::try_new(&carrier, ValueTypeStorageLifetime::pure())
        .map_err(|e| AnalyzeError::internal(e.to_string()))?;
    let value = common_semantics(
        layout.data_type(),
        left.value_type(),
        right.value_type(),
        nullable,
        &mut work,
    )?;
    work.finish().map_err(AnalyzeError::control)?;
    novarocks_type_contract::FunctionValueType::try_from_value(value, layout)
        .map_err(|e| AnalyzeError::internal(e.to_string()))
}

fn common_domain(
    left: &novarocks_type_contract::ValueTypeRef,
    right: &novarocks_type_contract::ValueTypeRef,
    target: &DataType,
) -> novarocks_type_contract::ValueLogicalType {
    use novarocks_type_contract::{ValueKind, ValueLogicalType as V};
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
    let integer = |value: &novarocks_type_contract::ValueTypeRef| {
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
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<std::cmp::Ordering, AnalyzeError> {
    for (a, b) in left.bytes().zip(right.bytes()) {
        work.step().map_err(AnalyzeError::control)?;
        let ordering = a.cmp(&b);
        if ordering != std::cmp::Ordering::Equal {
            return Ok(ordering);
        }
    }
    work.step().map_err(AnalyzeError::control)?;
    Ok(left.len().cmp(&right.len()))
}
fn named_index(
    fields: &[novarocks_type_contract::ValueField],
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<Vec<usize>, AnalyzeError> {
    fn sift(
        indices: &mut [usize],
        fields: &[novarocks_type_contract::ValueField],
        mut root: usize,
        end: usize,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<(), AnalyzeError> {
        let compare =
            |a: usize, b: usize, work: &mut novarocks_type_contract::CompileCheckpoints<'_>| {
                Ok::<_, AnalyzeError>(
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
            work.step().map_err(AnalyzeError::control)?;
            root = child;
        }
        Ok(())
    }
    let mut indices = Vec::with_capacity(fields.len());
    for index in 0..fields.len() {
        work.step().map_err(AnalyzeError::control)?;
        indices.push(index);
    }
    let len = indices.len();
    for root in (0..len / 2).rev() {
        sift(&mut indices, fields, root, len, work)?;
    }
    for end in (1..len).rev() {
        indices.swap(0, end);
        work.step().map_err(AnalyzeError::control)?;
        sift(&mut indices, fields, 0, end, work)?;
    }
    Ok(indices)
}
fn named_child<'a>(
    fields: &'a [novarocks_type_contract::ValueField],
    indices: &[usize],
    name: &str,
    fallback: usize,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<&'a novarocks_type_contract::ValueTypeRef, AnalyzeError> {
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
    left: &novarocks_type_contract::ValueTypeRef,
    right: &novarocks_type_contract::ValueTypeRef,
    nullable: bool,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<novarocks_type_contract::ValueTypeRef, AnalyzeError> {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
    };
    work.step().map_err(AnalyzeError::control)?;
    let storage = ValueTypeStorageLifetime::pure();
    if matches!(left.kind(), ValueKind::Scalar(DataType::Null)) {
        return right
            .with_nullable(nullable, storage)
            .map_err(|e| AnalyzeError::internal(e.to_string()));
    }
    if matches!(right.kind(), ValueKind::Scalar(DataType::Null)) {
        return left
            .with_nullable(nullable, storage)
            .map_err(|e| AnalyzeError::internal(e.to_string()));
    }
    let child = |index: usize,
                 field: &Field,
                 work: &mut novarocks_type_contract::CompileCheckpoints<'_>| {
        let left = left
            .child_type_at(index)
            .ok_or_else(|| AnalyzeError::internal("common type left child is absent"))?;
        let right = right
            .child_type_at(index)
            .ok_or_else(|| AnalyzeError::internal("common type right child is absent"))?;
        common_semantics(field.data_type(), left, right, field.is_nullable(), work)
    };
    let domain = common_domain(left, right, target);
    let kind = if domain != ValueLogicalType::Physical {
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
                    return Err(AnalyzeError::internal("common Map entries are not Struct"));
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
                    return Err(AnalyzeError::internal(
                        "common Struct semantics do not match layout",
                    ));
                };
                if left_fields.len() != target_fields.len()
                    || right_fields.len() != target_fields.len()
                {
                    return Err(AnalyzeError::internal("common Struct arity mismatch"));
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
                        work.step().map_err(AnalyzeError::control)?;
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
    ValueTypeRef::try_new(kind, nullable, storage)
        .map_err(|e| AnalyzeError::internal(e.to_string()))
}

/// A declared JSON cast does not validate ordinary non-NULL text. Transform
/// explicit semantics recursively while retaining the declared exact layout.
pub(super) fn declared_cast_value_type(
    source: &crate::analysis::TypedExpr,
    declared: novarocks_type_contract::FunctionValueType,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_type_contract::FunctionValueType, AnalyzeError> {
    if matches!(
        source.kind,
        crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Null)
    ) {
        return Ok(declared);
    }
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::Validate,
    )
    .map_err(AnalyzeError::control)?;
    let semantics = cast_semantics(
        declared.value_type(),
        source.value_type.value_type(),
        &mut work,
    )?;
    work.finish().map_err(AnalyzeError::control)?;
    novarocks_type_contract::FunctionValueType::try_from_value(
        semantics,
        declared.typed_binding().layout().clone(),
    )
    .map_err(|e| AnalyzeError::internal(e.to_string()))
}
fn cast_semantics(
    target: &novarocks_type_contract::ValueTypeRef,
    source: &novarocks_type_contract::ValueTypeRef,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<novarocks_type_contract::ValueTypeRef, AnalyzeError> {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLogicalType, ValueTypeStorageLifetime,
    };
    work.step().map_err(AnalyzeError::control)?;
    if matches!(source.kind(), ValueKind::Scalar(DataType::Null)) {
        return Ok(target.clone());
    }
    if target.domain() == Some(ValueLogicalType::Json)
        && source.domain() != Some(ValueLogicalType::Json)
    {
        return novarocks_type_contract::ValueTypeRef::try_new(
            ValueKind::String,
            target.nullable(),
            ValueTypeStorageLifetime::pure(),
        )
        .map_err(|e| AnalyzeError::internal(e.to_string()));
    }
    let child = |index: usize,
                 value: &novarocks_type_contract::ValueTypeRef,
                 work: &mut novarocks_type_contract::CompileCheckpoints<'_>| {
        match source.child_type_at(index) {
            Some(source) => cast_semantics(value, source, work),
            None => Ok(value.clone()),
        }
    };
    let kind = match target.kind() {
        ValueKind::List(item) => ValueKind::List(child(0, item, work)?),
        ValueKind::FixedSizeList { item, length } => ValueKind::FixedSizeList {
            item: child(0, item, work)?,
            length: *length,
        },
        ValueKind::Map { key, value } => ValueKind::Map {
            key: child(0, key, work)?,
            value: child(1, value, work)?,
        },
        ValueKind::Struct(fields) => {
            let mut output = Vec::with_capacity(fields.len());
            for (index, field) in fields.iter().enumerate() {
                let value = child(index, field.value_type(), work)?;
                for _ in field.name().bytes() {
                    work.step().map_err(AnalyzeError::control)?;
                }
                output.push(ValueField::new(
                    field.name(),
                    value,
                    ValueTypeStorageLifetime::pure(),
                ));
            }
            ValueKind::Struct(output.into())
        }
        _ => return Ok(target.clone()),
    };
    novarocks_type_contract::ValueTypeRef::try_new(
        kind,
        target.nullable(),
        ValueTypeStorageLifetime::pure(),
    )
    .map_err(|e| AnalyzeError::internal(e.to_string()))
}

fn declared_root(sql_type: &ast::TypeName) -> novarocks_type_contract::ValueLogicalType {
    use novarocks_type_contract::ValueLogicalType as V;
    let ast::TypeKind::Generic(generic) = &sql_type.kind else {
        return V::Physical;
    };
    match generic.name.value.to_ascii_lowercase().as_str() {
        "json" | "jsonb" => V::Json,
        "variant" => V::Variant,
        "largeint" => V::LargeInt,
        "hll" => V::Hll,
        "bitmap" => V::Bitmap,
        _ => V::Physical,
    }
}

fn generic_type_arguments(
    sql_type: &ast::TypeName,
) -> Result<&[ast::DataTypeParameter], AnalyzeError> {
    match &sql_type.kind {
        ast::TypeKind::Generic(generic) => Ok(&generic.arguments),
        _ => Err(AnalyzeError::internal(
            "container layout lost its generic SQL type author",
        )),
    }
}

/// The AST declaration establishes identity; its carrier alone never does.
pub(super) fn sql_type_to_value_type(
    sql_type: &ast::TypeName,
    nullable: bool,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_type_contract::FunctionValueType, AnalyzeError> {
    use novarocks_type_contract::{
        CompileCheckpoints, CompilePhase, ValueLayout, ValueTypeStorageLifetime,
    };
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(AnalyzeError::control)?;
    validate_ast_type(sql_type, 1, &mut 0, &mut work)?;
    work.flush().map_err(AnalyzeError::control)?;
    let carrier = sql_type_to_arrow(sql_type)?;
    work.flush().map_err(AnalyzeError::control)?;
    let layout = ValueLayout::try_from_normalized_arrow(carrier, ValueTypeStorageLifetime::pure())
        .map_err(|e| AnalyzeError::invalid_argument(e.to_string(), sql_type.span))?;
    let value = author_ast_type(sql_type, layout.data_type(), nullable, &mut work)?;
    work.finish().map_err(AnalyzeError::control)?;
    novarocks_type_contract::FunctionValueType::try_from_value(value, layout)
        .map_err(|e| AnalyzeError::invalid_argument(e.to_string(), sql_type.span))
}
fn validate_ast_type(
    sql_type: &ast::TypeName,
    depth: usize,
    nodes: &mut usize,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), AnalyzeError> {
    work.step().map_err(AnalyzeError::control)?;
    *nodes += 1;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH
        || *nodes > novarocks_type_contract::MAX_VALUE_TYPE_NODES
    {
        return Err(AnalyzeError::invalid_argument(
            "SQL type exceeds the recursive type profile",
            sql_type.span,
        ));
    }
    match &sql_type.kind {
        ast::TypeKind::Generic(generic) => {
            for parameter in &generic.arguments {
                match parameter {
                    ast::DataTypeParameter::Type(child) => {
                        validate_ast_type(&child.data_type, depth + 1, nodes, work)?
                    }
                    ast::DataTypeParameter::Numeric(_) => {
                        work.step().map_err(AnalyzeError::control)?
                    }
                }
            }
        }
        ast::TypeKind::Row(row) => {
            for field in &row.fields {
                validate_ast_type(&field.data_type, depth + 1, nodes, work)?;
            }
        }
        ast::TypeKind::DateTime(_) => {}
        ast::TypeKind::Interval(_) => {
            return Err(AnalyzeError::unsupported_expression(
                "INTERVAL data types are not supported",
                sql_type.span,
            ));
        }
        ast::TypeKind::ExplicitArray(_) => {
            return Err(AnalyzeError::unsupported_expression(
                "explicit ARRAY lengths are not supported",
                sql_type.span,
            ));
        }
    }
    Ok(())
}
fn author_ast_type(
    sql_type: &ast::TypeName,
    layout: &DataType,
    nullable: bool,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<novarocks_type_contract::ValueTypeRef, AnalyzeError> {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
    };
    work.step().map_err(AnalyzeError::control)?;
    let storage = ValueTypeStorageLifetime::pure();
    let domain = declared_root(sql_type);
    let kind = if domain != ValueLogicalType::Physical {
        ValueKind::Nominal(domain)
    } else {
        match layout {
            DataType::List(item) => ValueKind::List(author_ast_type(
                type_type_argument(generic_type_arguments(sql_type)?, 0, "ARRAY", sql_type.span)?,
                item.data_type(),
                item.is_nullable(),
                work,
            )?),
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(AnalyzeError::internal(
                        "authored Map entries are not Struct",
                    ));
                };
                ValueKind::Map {
                    key: author_ast_type(
                        type_type_argument(
                            generic_type_arguments(sql_type)?,
                            0,
                            "MAP",
                            sql_type.span,
                        )?,
                        fields[0].data_type(),
                        false,
                        work,
                    )?,
                    value: author_ast_type(
                        type_type_argument(
                            generic_type_arguments(sql_type)?,
                            1,
                            "MAP",
                            sql_type.span,
                        )?,
                        fields[1].data_type(),
                        fields[1].is_nullable(),
                        work,
                    )?,
                }
            }
            DataType::Struct(fields) => {
                let mut values = Vec::with_capacity(fields.len());
                let ast::TypeKind::Row(row) = &sql_type.kind else {
                    return Err(AnalyzeError::internal(
                        "Struct layout lost its ROW syntax author",
                    ));
                };
                for (field, argument) in fields.iter().zip(&row.fields) {
                    let child = &argument.data_type;
                    let value =
                        author_ast_type(child, field.data_type(), field.is_nullable(), work)?;
                    for _ in field.name().bytes() {
                        work.step().map_err(AnalyzeError::control)?;
                    }
                    values.push(ValueField::new(
                        field.name().as_str(),
                        value,
                        storage.clone(),
                    ));
                }
                ValueKind::Struct(values.into())
            }
            DataType::Utf8 | DataType::LargeUtf8 => ValueKind::String,
            DataType::Binary | DataType::LargeBinary => ValueKind::Binary,
            scalar => ValueKind::Scalar(scalar.clone()),
        }
    };
    ValueTypeRef::try_new(kind, nullable, storage)
        .map_err(|e| AnalyzeError::invalid_argument(e.to_string(), sql_type.span))
}

// SQL layout projection contains no semantic annotation. Its caller retains
// the separately authored complete type for each node.
fn nested_field_with_logical_type(
    name: &str,
    sql_type: &ast::TypeName,
    nullable: bool,
) -> Result<Field, AnalyzeError> {
    Ok(Field::new(name, sql_type_to_arrow(sql_type)?, nullable))
}

pub(super) fn sql_type_to_arrow(
    sql_type: &novarocks_parser::ast::TypeName,
) -> Result<DataType, AnalyzeError> {
    let generic = match &sql_type.kind {
        ast::TypeKind::Generic(generic) => generic,
        ast::TypeKind::Row(row) => {
            let fields = row
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let name = field
                        .name
                        .as_ref()
                        .map(|name| name.value.clone())
                        .unwrap_or_else(|| format!("f{}", index + 1));
                    nested_field_with_logical_type(&name, &field.data_type, true).map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(DataType::Struct(Fields::from(fields)));
        }
        ast::TypeKind::DateTime(datetime) => {
            if datetime.with_time_zone || datetime.precision.as_deref().is_some_and(|parameter| !matches!(parameter, ast::DataTypeParameter::Numeric(value) if value.parsed_value == 6)) {
                return Err(AnalyzeError::unsupported_expression("TIME/TIMESTAMP only support microsecond precision without time zone", sql_type.span));
            }
            return Ok(match datetime.kind {
                ast::DateTimeType::Time => {
                    DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
                }
                ast::DateTimeType::Timestamp => {
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                }
            });
        }
        ast::TypeKind::Interval(_) => {
            return Err(AnalyzeError::unsupported_expression(
                "INTERVAL data types are not supported",
                sql_type.span,
            ));
        }
        ast::TypeKind::ExplicitArray(_) => {
            return Err(AnalyzeError::unsupported_expression(
                "explicit ARRAY lengths are not supported",
                sql_type.span,
            ));
        }
    };
    let type_name = generic.name.value.to_ascii_lowercase();
    let type_args = &generic.arguments;
    match type_name.as_str() {
        "tinyint" => Ok(DataType::Int8),
        "smallint" => Ok(DataType::Int16),
        "int" | "integer" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "float" | "real" => Ok(DataType::Float32),
        "double" | "double precision" => Ok(DataType::Float64),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "varchar" | "char" | "character" | "string" | "text" => Ok(DataType::Utf8),
        "json" | "jsonb" => Ok(DataType::Utf8),
        "varbinary" | "binary" | "bitmap" | "hll" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "datetime" | "timestamp" | "timestamptz" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        "time" => Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)),
        "largeint" => Ok(DataType::FixedSizeBinary(
            novarocks_types::largeint::LARGEINT_BYTE_WIDTH,
        )),
        "variant" => Ok(DataType::LargeBinary),
        "datetime_ns" | "timestamp_ns" | "timestamptz_ns" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            None,
        )),
        "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
            if type_args.len() > 2 {
                return Err(AnalyzeError::invalid_argument(
                    "DECIMAL type accepts at most precision and scale",
                    sql_type.span,
                ));
            }
            // Generic SQL DECIMAL chooses its carrier by precision. Explicit
            // width aliases keep their declared domain; small decimals retain
            // the engine's existing Decimal128 representation.
            let (max_precision, default_precision) = match type_name.as_str() {
                "decimal32" => (9_u64, 9_u64),
                "decimal64" => (18, 18),
                "decimal128" => (38, 38),
                _ => (76, 38),
            };
            let precision = type_numeric_argument(type_args, 0)?.unwrap_or(default_precision);
            let scale = type_numeric_argument(type_args, 1)?.unwrap_or(0);
            if precision == 0 || precision > max_precision {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "{type_name} precision {precision} must be between 1 and {max_precision}"
                    ),
                    sql_type.span,
                ));
            }
            if scale > precision {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "{type_name} scale {scale} must be between 0 and precision {precision}"
                    ),
                    sql_type.span,
                ));
            }
            let precision = u8::try_from(precision).map_err(|_| {
                AnalyzeError::invalid_argument(
                    "DECIMAL precision exceeds its metadata domain",
                    sql_type.span,
                )
            })?;
            let scale = i8::try_from(scale).map_err(|_| {
                AnalyzeError::invalid_argument(
                    "DECIMAL scale exceeds its metadata domain",
                    sql_type.span,
                )
            })?;
            if precision > 38 {
                Ok(DataType::Decimal256(precision, scale))
            } else {
                Ok(DataType::Decimal128(precision, scale))
            }
        }
        "array" => {
            let element = type_type_argument(type_args, 0, "ARRAY", sql_type.span)?;
            Ok(DataType::List(Arc::new(nested_field_with_logical_type(
                "item", element, true,
            )?)))
        }
        "map" => {
            let key = type_type_argument(type_args, 0, "MAP", sql_type.span)?;
            let value = type_type_argument(type_args, 1, "MAP", sql_type.span)?;
            let key = nested_field_with_logical_type("key", key, false)?;
            let value = nested_field_with_logical_type("value", value, true)?;
            Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![Arc::new(key), Arc::new(value)])),
                    false,
                )),
                false,
            ))
        }
        _ => Err(AnalyzeError::unsupported_query_shape(
            format!("unsupported SQL type: {type_name}"),
            sql_type.span,
        )),
    }
}

fn type_numeric_argument(
    arguments: &[ast::DataTypeParameter],
    index: usize,
) -> Result<Option<u64>, AnalyzeError> {
    let Some(argument) = arguments.get(index) else {
        return Ok(None);
    };
    let ast::DataTypeParameter::Numeric(value) = argument else {
        return Err(AnalyzeError::invalid_literal(
            "numeric type parameter must be an integer",
            type_name_argument_span(argument),
        ));
    };
    u64::try_from(value.parsed_value).map(Some).map_err(|_| {
        AnalyzeError::invalid_literal(
            format!(
                "numeric type parameter must be nonnegative: {}",
                value.spelling
            ),
            value.span,
        )
    })
}

fn type_type_argument<'a>(
    arguments: &'a [ast::DataTypeParameter],
    index: usize,
    kind: &str,
    type_span: novarocks_parser::Span,
) -> Result<&'a ast::TypeName, AnalyzeError> {
    let Some(ast::DataTypeParameter::Type(value)) = arguments.get(index) else {
        return Err(AnalyzeError::invalid_argument(
            format!("{kind} type requires a type parameter"),
            arguments
                .get(index)
                .map(type_name_argument_span)
                .unwrap_or(type_span),
        ));
    };
    Ok(&value.data_type)
}

fn type_name_argument_span(argument: &ast::DataTypeParameter) -> novarocks_parser::Span {
    match argument {
        ast::DataTypeParameter::Type(value) => value.data_type.span,
        ast::DataTypeParameter::Numeric(value) => value.span,
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Expression display name
// ---------------------------------------------------------------------------

pub(super) fn expr_display_name(expr: &ast::Expr) -> String {
    match expr {
        ast::Expr::Nested(nested) => expr_display_name(&nested.expression),
        ast::Expr::Literal(literal) => format_literal_display_name(literal),
        ast::Expr::Identifier(ident) => ident.value.clone(),
        ast::Expr::CompoundIdentifier(identifier) if !identifier.parts.is_empty() => identifier
            .parts
            .last()
            .expect("non-empty compound identifier")
            .value
            .clone(),
        ast::Expr::Access(access) => format_access_display_name(access),
        ast::Expr::Array(array) => format!(
            "[{}]",
            array
                .elements
                .iter()
                .map(expr_display_name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ast::Expr::Map(map) => format!(
            "map{{{}}}",
            map.entries
                .iter()
                .map(|entry| format!(
                    "{}:{}",
                    expr_display_name(&entry.key),
                    expr_display_name(&entry.value)
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
        ast::Expr::Lambda(lambda) => {
            let parameters = lambda
                .arguments
                .iter()
                .map(|argument| argument.name.value.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let parameters = if lambda.arguments.len() == 1 {
                parameters
            } else {
                format!("({parameters})")
            };
            format!(
                "{parameters} -> {}",
                expr_display_name_preserve_path(&lambda.body)
            )
        }
        ast::Expr::FunctionCall(function) => format_function_display_name(function),
        ast::Expr::Predicated(node) => format_predicated_display_name(expr, node),
        ast::Expr::Cast(cast) => {
            if type_name_is(&cast.data_type, "array")
                && matches!(&cast.expression, ast::Expr::Array(_))
            {
                return expr_display_name(&cast.expression);
            }
            format!(
                "CAST({} AS {})",
                expr_display_name_with_parens(&cast.expression),
                format_cast_type_for_expression(&cast.data_type, &cast.expression)
            )
        }
        ast::Expr::Binary(binary) => format!(
            "{} {} {}",
            expr_display_name_with_parens(&binary.left),
            binary_operator_display(binary.operator),
            expr_display_name_with_parens(&binary.right)
        ),
        ast::Expr::Logical(logical) => {
            let separator = match logical.operator {
                ast::LogicalOperator::And => " AND ",
                ast::LogicalOperator::Or => " OR ",
            };
            // Output names retain the existing adjacent-pair grouping. Only
            // rendering strings are grouped; syntax and source terms stay flat.
            let mut level = logical
                .terms
                .iter()
                .map(expr_display_name_with_parens)
                .collect::<Vec<_>>();
            while level.len() > 2 {
                let mut next = Vec::with_capacity(level.len().div_ceil(2));
                let mut terms = level.into_iter();
                while let Some(left) = terms.next() {
                    next.push(match terms.next() {
                        Some(right) => format!("({left}{separator}{right})"),
                        None => left,
                    });
                }
                level = next;
            }
            level.join(separator)
        }
        ast::Expr::InList(list) => format!(
            "({})",
            list.values
                .iter()
                .map(expr_display_name_with_parens)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other => lowercase_leading_keyword(&printer::print_expr(other)),
    }
}

fn format_subquery_display_name(query: &ast::Query) -> String {
    let mut query = query.clone();
    mark_query_aliases_explicit(&mut query);
    printer::print_query(&query)
}

fn mark_query_aliases_explicit(query: &mut ast::Query) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.ctes {
            mark_query_aliases_explicit(&mut cte.query);
        }
    }
    mark_set_expr_aliases_explicit(&mut query.body);
}

fn mark_set_expr_aliases_explicit(set_expr: &mut ast::SetExpr) {
    match set_expr {
        ast::SetExpr::Select(select) => {
            for table_with_joins in &mut select.from {
                mark_table_with_joins_aliases_explicit(table_with_joins);
            }
        }
        ast::SetExpr::Query(query) => mark_query_aliases_explicit(query),
        ast::SetExpr::SetOperation(operation) => {
            for input in &mut operation.inputs {
                mark_set_expr_aliases_explicit(input);
            }
        }
        ast::SetExpr::Values(_) => {}
    }
}

fn mark_table_with_joins_aliases_explicit(table_with_joins: &mut ast::TableWithJoins) {
    mark_table_factor_aliases_explicit(&mut table_with_joins.relation);
    for join in &mut table_with_joins.joins {
        mark_table_factor_aliases_explicit(&mut join.relation);
    }
}

fn mark_table_factor_aliases_explicit(factor: &mut ast::TableFactor) {
    match factor {
        ast::TableFactor::JsonTable { alias, .. }
        | ast::TableFactor::Table { alias, .. }
        | ast::TableFactor::TableFunction { alias, .. }
        | ast::TableFactor::Unnest { alias, .. } => {
            if let Some(alias) = alias {
                alias.explicit_as = true;
            }
        }
        ast::TableFactor::Derived {
            subquery, alias, ..
        } => {
            if let Some(alias) = alias {
                alias.explicit_as = true;
            }
            mark_query_aliases_explicit(subquery);
        }
        ast::TableFactor::NestedJoin {
            table_with_joins,
            alias,
            ..
        } => {
            if let Some(alias) = alias {
                alias.explicit_as = true;
            }
            mark_table_with_joins_aliases_explicit(table_with_joins);
        }
    }
}

fn format_access_display_name(access: &ast::AccessExpr) -> String {
    if let ast::AccessKind::Field(field) = &access.kind
        && matches!(
            access.expr.as_ref(),
            ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)
        )
    {
        return field.value.clone();
    }

    let base = expr_display_name_preserve_path(&access.expr);
    match &access.kind {
        ast::AccessKind::Field(field) => format!("{base}.{}", field.value),
        ast::AccessKind::Subscript(index) => format!("{base}[{}]", expr_display_name(index)),
        ast::AccessKind::Json { operator, path } => {
            let operator = match operator {
                ast::JsonOperator::Arrow => "->",
                ast::JsonOperator::ArrowText => "->>",
            };
            format!(
                "{base} {operator} {}",
                expr_display_name_preserve_path(path)
            )
        }
    }
}
fn expr_display_name_preserve_path(expr: &ast::Expr) -> String {
    match expr {
        ast::Expr::Nested(nested) => expr_display_name_preserve_path(&nested.expression),
        ast::Expr::CompoundIdentifier(identifier) => identifier
            .parts
            .iter()
            .map(|part| part.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        ast::Expr::Access(access) => format_access_display_name_preserve_path(access),
        _ => expr_display_name(expr),
    }
}

fn format_access_display_name_preserve_path(access: &ast::AccessExpr) -> String {
    let base = expr_display_name_preserve_path(&access.expr);
    match &access.kind {
        ast::AccessKind::Field(field) => format!("{base}.{}", field.value),
        ast::AccessKind::Subscript(index) => format!("{base}[{}]", expr_display_name(index)),
        ast::AccessKind::Json { operator, path } => {
            let operator = match operator {
                ast::JsonOperator::Arrow => "->",
                ast::JsonOperator::ArrowText => "->>",
            };
            format!(
                "{base} {operator} {}",
                expr_display_name_preserve_path(path)
            )
        }
    }
}

fn format_literal_display_name(literal: &ast::Literal) -> String {
    match &literal.kind {
        ast::LiteralKind::String(value) => format!("'{}'", value.replace('\'', "''")),
        ast::LiteralKind::Boolean(true) => "TRUE".to_string(),
        ast::LiteralKind::Boolean(false) => "FALSE".to_string(),
        ast::LiteralKind::Null => "NULL".to_string(),
        ast::LiteralKind::Long { spelling, .. } | ast::LiteralKind::Decimal(spelling) => {
            spelling.clone()
        }
        ast::LiteralKind::Double(value) => value.get().to_string(),
        ast::LiteralKind::Binary(bytes) => format!("X'{}'", hex::encode(bytes)),
        ast::LiteralKind::Generic(_) | ast::LiteralKind::Interval(_) => {
            printer::print_expr(&ast::Expr::Literal(literal.clone()))
        }
    }
}

fn expr_display_name_with_parens(expr: &ast::Expr) -> String {
    match expr {
        ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_) | ast::Expr::Literal(_) => {
            expr_display_name(expr)
        }
        ast::Expr::Unary(unary)
            if unary.operator == ast::UnaryOperator::Minus
                && matches!(unary.expression.as_ref(), ast::Expr::Literal(_)) =>
        {
            expr_display_name(expr)
        }
        ast::Expr::Nested(nested) => expr_display_name_with_parens(&nested.expression),
        _ => format!("({})", expr_display_name(expr)),
    }
}

fn format_predicated_display_name(expression: &ast::Expr, node: &ast::PredicatedExpr) -> String {
    let value = || expr_display_name_with_parens(&node.value);
    match &node.predicate {
        ast::Predicate::Comparison(p) => {
            let operator = match p.operator {
                ast::ComparisonOperator::Equal => "=",
                ast::ComparisonOperator::NotEqual => "!=",
                ast::ComparisonOperator::LessThan => "<",
                ast::ComparisonOperator::LessThanOrEqual => "<=",
                ast::ComparisonOperator::GreaterThan => ">",
                ast::ComparisonOperator::GreaterThanOrEqual => ">=",
            };
            format!(
                "{} {operator} {}",
                value(),
                expr_display_name_with_parens(&p.right)
            )
        }
        ast::Predicate::DistinctFrom(p) => {
            let equal = format!(
                "{} <=> {}",
                value(),
                expr_display_name_with_parens(&p.right)
            );
            if p.negated {
                equal
            } else {
                format!("NOT ({equal})")
            }
        }
        ast::Predicate::IsNull(p) => {
            format!("{} IS {}NULL", value(), if p.negated { "NOT " } else { "" })
        }
        ast::Predicate::BooleanTest(p) => {
            let truth = match p.truth_value {
                ast::TruthValue::True => "TRUE",
                ast::TruthValue::False => "FALSE",
                ast::TruthValue::Unknown => "UNKNOWN",
            };
            format!(
                "{} IS {}{truth}",
                value(),
                if p.negated { "NOT " } else { "" }
            )
        }
        ast::Predicate::In(p) => {
            let not = if p.negated { " NOT" } else { "" };
            match &p.value_list {
                ast::Expr::InList(list) => format!(
                    "{}{not} IN ({})",
                    value(),
                    list.values
                        .iter()
                        .map(expr_display_name_with_parens)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                ast::Expr::Subquery(subquery) => format!(
                    "{}{not} IN ((({})))",
                    value(),
                    format_subquery_display_name(&subquery.query)
                ),
                _ => lowercase_leading_keyword(&printer::print_expr(expression)),
            }
        }
        _ => lowercase_leading_keyword(&printer::print_expr(expression)),
    }
}

fn binary_operator_display(operator: ast::BinaryOperator) -> &'static str {
    match operator {
        ast::BinaryOperator::NamedArgument => "=>",
        ast::BinaryOperator::Add => "+",
        ast::BinaryOperator::Subtract => "-",
        ast::BinaryOperator::Multiply => "*",
        ast::BinaryOperator::Divide => "/",
        ast::BinaryOperator::Modulo => "%",
        ast::BinaryOperator::BitwiseAnd => "&",
        ast::BinaryOperator::BitwiseOr => "|",
        ast::BinaryOperator::BitwiseXor => "^",
        ast::BinaryOperator::ShiftLeft => "<<",
        ast::BinaryOperator::ShiftRight => ">>",
        ast::BinaryOperator::StringConcat => "||",
    }
}

fn type_name_is(data_type: &ast::TypeName, expected: &str) -> bool {
    matches!(&data_type.kind, ast::TypeKind::Generic(value) if value.name.value.eq_ignore_ascii_case(expected))
}

fn format_cast_type(data_type: &ast::TypeName) -> String {
    format_type_display(data_type, 0)
}

fn format_cast_type_for_expression(data_type: &ast::TypeName, expression: &ast::Expr) -> String {
    if is_parse_json_call(expression) {
        format_json_cast_type(data_type)
    } else {
        format_cast_type(data_type)
    }
}

fn is_parse_json_call(expression: &ast::Expr) -> bool {
    let ast::Expr::FunctionCall(function) = expression else {
        return false;
    };
    function
        .name
        .parts
        .last()
        .is_some_and(|part| part.value.eq_ignore_ascii_case("parse_json"))
}

fn format_json_cast_type(data_type: &ast::TypeName) -> String {
    format_type_display(data_type, 1)
}

fn numeric_type_argument(data_type: &ast::TypeName, index: usize) -> Option<u64> {
    let ast::TypeKind::Generic(value) = &data_type.kind else {
        return None;
    };
    match value.arguments.get(index) {
        Some(ast::DataTypeParameter::Numeric(value)) => u64::try_from(value.parsed_value).ok(),
        _ => None,
    }
}

fn type_name_argument_as_type(argument: &ast::DataTypeParameter) -> Option<&ast::TypeName> {
    match argument {
        ast::DataTypeParameter::Type(value) => Some(&value.data_type),
        ast::DataTypeParameter::Numeric(_) => None,
    }
}

// These are the established output-column display spellings, not SQL syntax.
// The parser printer remains the owner of reparsable Trino type text.
fn format_type_display(data_type: &ast::TypeName, style: u8) -> String {
    match &data_type.kind {
        ast::TypeKind::Row(row) => {
            let fields = row
                .fields
                .iter()
                .map(|field| {
                    let child =
                        format_type_display(&field.data_type, if style == 0 { 0 } else { 2 });
                    field
                        .name
                        .as_ref()
                        .map(|name| format!("{} {child}", name.value))
                        .unwrap_or(child)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("struct<{fields}>")
        }
        ast::TypeKind::Generic(generic) => {
            let name = generic.name.value.to_ascii_lowercase();
            let arguments = &generic.arguments;
            match name.as_str() {
                "array" => {
                    let child = arguments
                        .first()
                        .and_then(type_name_argument_as_type)
                        .map(|child| format_type_display(child, style))
                        .unwrap_or_default();
                    format!("{}<{child}>", if style == 2 { "array" } else { "ARRAY" })
                }
                "map" => {
                    let children = arguments
                        .iter()
                        .filter_map(type_name_argument_as_type)
                        .map(|child| format_type_display(child, style))
                        .collect::<Vec<_>>()
                        .join(if style == 2 { ", " } else { "," });
                    format!("{}<{children}>", if style == 2 { "map" } else { "MAP" })
                }
                "tinyint" if style == 2 => "tinyint(4)".into(),
                "smallint" if style == 2 => "smallint(6)".into(),
                "int" | "integer" if style == 2 => "int(11)".into(),
                "bigint" if style == 2 => "bigint(20)".into(),
                "string" if style == 2 => "varchar(65533)".into(),
                "binary" | "varbinary" if style == 2 => "varbinary".into(),
                "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
                    let precision = numeric_type_argument(data_type, 0).unwrap_or(38);
                    let scale = numeric_type_argument(data_type, 1).unwrap_or(0);
                    format!("{}({precision},{scale})", decimal_kind(precision))
                }
                "tinyint" => "TINYINT".into(),
                "smallint" => "SMALLINT".into(),
                "bigint" => "BIGINT".into(),
                "largeint" => "LARGEINT".into(),
                "int" | "integer" => "INT".into(),
                "string" => "VARCHAR(65533)".into(),
                "binary" | "varbinary" => "VARBINARY".into(),
                _ => printer::print_type_name(data_type),
            }
        }
        _ => printer::print_type_name(data_type),
    }
}

fn decimal_kind(precision: u64) -> &'static str {
    if precision <= 9 {
        "DECIMAL32"
    } else if precision <= 18 {
        "DECIMAL64"
    } else {
        "DECIMAL128"
    }
}

fn canonical_display_function_name(name: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "boolor_agg" => "bool_or".to_string(),
        "booland_agg" | "every" => "bool_and".to_string(),
        "string_agg" => "group_concat".to_string(),
        "array_agg_distinct" => "array_agg".to_string(),
        "approx_count_distinct_hll_sketch" => "ds_hll_count_distinct".to_string(),
        "struct" => "row".to_string(),
        other => other.to_string(),
    }
}

fn format_function_display_name(function: &ast::FunctionCall) -> String {
    let original_name = function
        .name
        .parts
        .last()
        .map(|part| part.value.to_ascii_lowercase())
        .unwrap_or_default();
    let canonical_name = canonical_display_function_name(&original_name);
    if canonical_name == "map" {
        return format_map_display_name(function);
    }
    if canonical_name == "element_at" && function.arguments.len() == 2 {
        return format!(
            "{}[{}]",
            expr_display_name_preserve_path(&function.arguments[0]),
            expr_display_name(&function.arguments[1])
        );
    }

    let distinct = matches!(function.quantifier, ast::FunctionQuantifier::Distinct)
        || original_name == "array_agg_distinct";
    let (arguments, order_arguments, implicit_separator) = if canonical_name == "group_concat" {
        // Prepared syntax owns values and separator in distinct fields.
        let values = function.arguments.as_slice();
        let separator = function.separator.as_deref();
        (
            values
                .iter()
                .map(expr_display_name_preserve_path)
                .collect::<Vec<_>>()
                .join(","),
            values,
            separator,
        )
    } else {
        (
            format_function_arguments(function, &canonical_name),
            function.arguments.as_slice(),
            None,
        )
    };
    let mut out = format!("{canonical_name}(");
    if distinct {
        out.push_str("DISTINCT ");
    }
    out.push_str(&arguments);
    if !function.order_by.is_empty() {
        let visible = function
            .order_by
            .iter()
            .filter(|item| !is_constant_function_order_by(item, order_arguments))
            .map(|item| format_function_order_by(item, order_arguments))
            .collect::<Vec<_>>();
        if !visible.is_empty() {
            if !arguments.is_empty() {
                out.push(' ');
            }
            out.push_str("ORDER BY ");
            out.push_str(&visible.join(", "));
        }
    }
    if canonical_name == "group_concat" {
        if !arguments.is_empty() {
            out.push(' ');
        }
        out.push_str("SEPARATOR ");
        out.push_str(
            function
                .separator
                .as_deref()
                .map(expr_display_name)
                .or_else(|| implicit_separator.map(expr_display_name))
                .as_deref()
                .unwrap_or("','"),
        );
    }
    out.push(')');

    if let Some(filter) = &function.filter {
        out.push_str(" FILTER (WHERE ");
        out.push_str(&expr_display_name(filter));
        out.push(')');
    }
    if let Some(null_treatment) = function.null_treatment
        && !function_null_treatment_is_argument(&canonical_name, function.arguments.len())
    {
        out.push(' ');
        out.push_str(match null_treatment {
            ast::NullTreatment::IgnoreNulls => "ignore nulls",
            ast::NullTreatment::RespectNulls => "respect nulls",
        });
    }
    if let Some(over) = &function.over {
        out.push_str(" OVER ");
        out.push_str(&format_window_display_name(over));
    }
    if canonical_name == "typeof" {
        restore_typeof_cast_spelling(out, function)
    } else {
        out
    }
}

fn restore_typeof_cast_spelling(display: String, function: &ast::FunctionCall) -> String {
    struct CastSpelling {
        canonical: String,
        display: String,
    }

    struct Collector {
        casts: Vec<CastSpelling>,
    }

    impl ast::Visit for Collector {
        fn visit_expr(&mut self, expression: &ast::Expr) {
            if let ast::Expr::Cast(cast) = expression {
                let source = match &cast.data_type.kind {
                    ast::TypeKind::Generic(value) => value.name.value.as_str(),
                    _ => "",
                };
                let canonical = format_cast_type(&cast.data_type);
                let display = match source {
                    // `typeof` has legacy result-header spellings for these
                    // narrow integer casts. Keep that compatibility local to
                    // typeof; ordinary CAST expressions use canonical names.
                    "tinyint" | "smallint" | "bigint" => source.to_string(),
                    _ => canonical.clone(),
                };
                self.casts.push(CastSpelling { canonical, display });
            }
            ast::walk_expr(self, expression);
        }
    }

    let mut collector = Collector { casts: Vec::new() };
    for argument in &function.arguments {
        ast::Visit::visit_expr(&mut collector, argument);
    }
    if collector.casts.is_empty() {
        return display;
    }

    let mut restored = String::with_capacity(display.len());
    let mut remaining = display.as_str();
    for cast in collector.casts {
        let needle = format!(" AS {})", cast.canonical);
        let Some(index) = remaining.find(&needle) else {
            return display;
        };
        restored.push_str(&remaining[..index]);
        restored.push_str(" AS ");
        restored.push_str(&cast.display);
        restored.push(')');
        remaining = &remaining[index + needle.len()..];
    }
    restored.push_str(remaining);
    restored
}

fn format_function_arguments(function: &ast::FunctionCall, canonical_name: &str) -> String {
    let mut arguments = function
        .arguments
        .iter()
        .map(expr_display_name_preserve_path)
        .collect::<Vec<_>>();
    if function_null_treatment_is_argument(canonical_name, arguments.len())
        && let Some(null_treatment) = function.null_treatment
    {
        let modifier = match null_treatment {
            ast::NullTreatment::IgnoreNulls => "ignore nulls",
            ast::NullTreatment::RespectNulls => "respect nulls",
        };
        arguments[0].push(' ');
        arguments[0].push_str(modifier);
    }
    arguments.join(", ")
}

fn function_null_treatment_is_argument(canonical_name: &str, argument_count: usize) -> bool {
    (matches!(canonical_name, "first_value" | "last_value") && argument_count >= 1)
        || (matches!(canonical_name, "lead" | "lag" | "nth_value") && argument_count >= 2)
}

fn format_map_display_name(function: &ast::FunctionCall) -> String {
    let mut pairs = Vec::new();
    let mut arguments = function.arguments.iter();
    while let Some(key) = arguments.next() {
        let key = expr_display_name_preserve_path(key);
        if let Some(value) = arguments.next() {
            pairs.push(format!("{key}:{}", expr_display_name_preserve_path(value)));
        } else {
            pairs.push(key);
        }
    }
    format!("map{{{}}}", pairs.join(","))
}

fn is_constant_function_order_by(order_by: &ast::FunctionOrderBy, arguments: &[ast::Expr]) -> bool {
    ordinal_function_argument(&order_by.expr, arguments)
        .map(is_constant_expr)
        .unwrap_or_else(|| is_constant_expr(&order_by.expr))
}

fn ordinal_function_argument<'a>(
    expr: &ast::Expr,
    arguments: &'a [ast::Expr],
) -> Option<&'a ast::Expr> {
    let ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::Long {
            parsed_value: position,
            ..
        },
        ..
    }) = expr
    else {
        return None;
    };
    usize::try_from(*position)
        .ok()
        .and_then(|position| arguments.get(position.saturating_sub(1)))
}

fn is_constant_expr(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(_) => true,
        ast::Expr::Nested(nested) => is_constant_expr(&nested.expression),
        _ => false,
    }
}

fn format_function_order_by(order_by: &ast::FunctionOrderBy, arguments: &[ast::Expr]) -> String {
    let expression = ordinal_function_argument(&order_by.expr, arguments)
        .map(expr_display_name_preserve_path)
        .unwrap_or_else(|| expr_display_name(&order_by.expr));
    format_order_by_display_name(expression, order_by.asc, order_by.nulls_first)
}

fn format_order_by_display_name(
    expression: String,
    ascending: Option<bool>,
    nulls_first: Option<bool>,
) -> String {
    let ascending = ascending.unwrap_or(true);
    let mut out = format!("{expression} {}", if ascending { "ASC" } else { "DESC" });
    if let Some(nulls_first) = nulls_first
        && nulls_first != ascending
    {
        out.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    out
}

fn format_window_display_name(over: &ast::WindowSpec) -> String {
    if let Some(name) = &over.existing_window_name {
        return name.value.clone();
    }

    let mut parts = Vec::new();
    if !over.partition_by.is_empty() {
        parts.push(format!(
            "PARTITION BY {}",
            over.partition_by
                .iter()
                .map(expr_display_name)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !over.order_by.is_empty() {
        parts.push(format!(
            "ORDER BY {}",
            over.order_by
                .iter()
                .map(|order_by| {
                    format_order_by_display_name(
                        expr_display_name(&order_by.expr),
                        order_by.asc,
                        order_by.nulls_first,
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(frame) = &over.window_frame {
        parts.push(format_window_frame_display_name(frame));
    }

    if parts.is_empty() {
        "()".to_string()
    } else if over.window_frame.is_some() {
        format!("({})", parts.join(" "))
    } else {
        format!("({} )", parts.join(" "))
    }
}

fn format_window_frame_display_name(frame: &ast::WindowFrame) -> String {
    let units = match frame.units {
        ast::WindowFrameUnits::Rows => "ROWS",
        ast::WindowFrameUnits::Range => "RANGE",
        ast::WindowFrameUnits::Groups => "GROUPS",
    };
    let start = format_window_bound_display_name(&frame.start_bound);
    match &frame.end_bound {
        Some(end) => format!(
            "{units} BETWEEN {start} AND {}",
            format_window_bound_display_name(end)
        ),
        None => format!("{units} {start}"),
    }
}

fn format_window_bound_display_name(bound: &ast::WindowFrameBound) -> String {
    match bound {
        ast::WindowFrameBound::CurrentRow(_) => "CURRENT ROW".to_string(),
        ast::WindowFrameBound::Preceding(None, _) => "UNBOUNDED PRECEDING".to_string(),
        ast::WindowFrameBound::Preceding(Some(expr), _) => {
            format!("{} PRECEDING", expr_display_name(expr))
        }
        ast::WindowFrameBound::Following(None, _) => "UNBOUNDED FOLLOWING".to_string(),
        ast::WindowFrameBound::Following(Some(expr), _) => {
            format!("{} FOLLOWING", expr_display_name(expr))
        }
    }
}

fn lowercase_leading_keyword(display: &str) -> String {
    let Some(paren) = display.find('(') else {
        return display.to_string();
    };
    let prefix = &display[..paren];
    if !prefix.is_empty()
        && prefix
            .chars()
            .all(|character| character.is_ascii_alphabetic())
    {
        format!("{}{}", prefix.to_ascii_lowercase(), &display[paren..])
    } else {
        display.to_string()
    }
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET extraction
// ---------------------------------------------------------------------------

pub(super) fn query_modifiers(query: &ast::Query) -> Result<&ast::QueryModifiers, AnalyzeError> {
    match query.body.as_ref() {
        ast::SetExpr::Select(select) => {
            if !query.modifiers.is_empty() {
                return Err(AnalyzeError::invalid_query_shape(
                    "direct SELECT modifiers must belong to the query specification",
                    query.span,
                ));
            }
            Ok(&select.modifiers)
        }
        _ => Ok(&query.modifiers),
    }
}

pub(super) fn extract_limit(modifiers: &ast::QueryModifiers) -> Result<Option<i64>, AnalyzeError> {
    match &modifiers.limit {
        Some(limit) => eval_limit_or_offset(limit, "LIMIT").map(Some),
        None => Ok(None),
    }
}

pub(super) fn extract_offset(modifiers: &ast::QueryModifiers) -> Result<Option<i64>, AnalyzeError> {
    match &modifiers.offset {
        Some(offset) => eval_limit_or_offset(&offset.value, "OFFSET").map(Some),
        None => Ok(None),
    }
}

fn eval_limit_or_offset(expr: &ast::Expr, keyword: &str) -> Result<i64, AnalyzeError> {
    match expr {
        ast::Expr::Literal(ast::Literal {
            kind:
                ast::LiteralKind::Long {
                    parsed_value: value,
                    ..
                },
            ..
        }) => Ok(*value),
        _ => Err(AnalyzeError::invalid_argument(
            format!("only constant {keyword} is supported"),
            expr.span(),
        )),
    }
}

/// Evaluate a constant integer expression (literals and simple arithmetic).
pub(super) fn eval_const_i64(expr: &ast::Expr) -> Result<i64, AnalyzeError> {
    match expr {
        ast::Expr::Literal(ast::Literal {
            kind:
                ast::LiteralKind::Long {
                    parsed_value: value,
                    ..
                },
            ..
        }) => Ok(*value),
        ast::Expr::Unary(unary) if unary.operator == ast::UnaryOperator::Minus => {
            Ok(-eval_const_i64(&unary.expression)?)
        }
        ast::Expr::Binary(binary) => {
            let left = eval_const_i64(&binary.left)?;
            let right = eval_const_i64(&binary.right)?;
            match binary.operator {
                ast::BinaryOperator::Add => Ok(left + right),
                ast::BinaryOperator::Subtract => Ok(left - right),
                ast::BinaryOperator::Multiply => Ok(left * right),
                ast::BinaryOperator::Divide if right != 0 => Ok(left / right),
                ast::BinaryOperator::Modulo if right != 0 => Ok(left % right),
                _ => Err(AnalyzeError::invalid_argument(
                    format!(
                        "unsupported operator in constant expression: {}",
                        binary_operator_display(binary.operator)
                    ),
                    expr.span(),
                )),
            }
        }
        ast::Expr::Nested(nested) => eval_const_i64(&nested.expression),
        _ => Err(AnalyzeError::invalid_literal(
            format!(
                "expected constant integer expression, got: {}",
                printer::print_expr(expr)
            ),
            expr.span(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::{expr_display_name, sql_type_to_arrow};

    fn parse_select_expr(sql: &str) -> novarocks_parser::ast::Expr {
        let statements = novarocks_parser::parse(sql).expect("parse SQL");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        let novarocks_parser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let [novarocks_parser::ast::SelectItem::UnnamedExpr(expr)] = select.projection.as_slice()
        else {
            panic!("expected unnamed expr");
        };
        expr.clone()
    }

    fn parse_native_cast_type(sql: &str) -> novarocks_parser::ast::TypeName {
        let statements = novarocks_parser::parse(sql).expect("parse native SQL");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected native query");
        };
        let novarocks_parser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected native select body");
        };
        let [
            novarocks_parser::ast::SelectItem::UnnamedExpr(novarocks_parser::ast::Expr::Cast(cast)),
        ] = select.projection.as_slice()
        else {
            panic!("expected native cast expression");
        };
        cast.data_type.clone()
    }

    #[test]
    fn logical_display_preserves_original_adjacent_group_names_and_projection_contract() {
        // Independent strings from the existing adjacent-pair parser/display
        // contract cover even and odd tails, nested operators and operands.
        for (sql, expected) in [
            ("SELECT TRUE AND FALSE", "TRUE AND FALSE"),
            (
                "SELECT TRUE AND FALSE AND NULL",
                "(TRUE AND FALSE) AND NULL",
            ),
            (
                "SELECT TRUE AND FALSE AND NULL AND TRUE",
                "(TRUE AND FALSE) AND (NULL AND TRUE)",
            ),
            (
                "SELECT TRUE AND FALSE AND NULL AND TRUE AND FALSE",
                "((TRUE AND FALSE) AND (NULL AND TRUE)) AND FALSE",
            ),
            (
                "SELECT TRUE OR FALSE OR NULL OR TRUE OR FALSE",
                "((TRUE OR FALSE) OR (NULL OR TRUE)) OR FALSE",
            ),
            (
                "SELECT TRUE AND (FALSE OR NULL) AND TRUE",
                "(TRUE AND (FALSE OR NULL)) AND TRUE",
            ),
            (
                "SELECT 1 = 10 AND 2 = 20 AND 3 = 30",
                "((1 = 10) AND (2 = 20)) AND (3 = 30)",
            ),
        ] {
            let expression = parse_select_expr(sql);
            assert_eq!(expr_display_name(&expression), expected, "{sql}");
            let statements = novarocks_parser::parse(sql).unwrap();
            let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
                panic!("query")
            };
            let (resolved, _, _) = crate::analyzer::analyze(
                query,
                &crate::planning::catalog::PlannerMemoryCatalog::default(),
                "default",
            )
            .unwrap();
            let crate::analysis::QueryBody::Select(select) = resolved.body else {
                panic!("resolved select")
            };
            assert_eq!(select.projection[0].output_name, expected, "{sql}");
        }
    }

    #[test]
    fn expr_display_name_formats_distinct_function_args_recursively() {
        let expr = parse_select_expr("SELECT ARRAY_AGG(DISTINCT score > 0)");
        assert_eq!(expr_display_name(&expr), "array_agg(DISTINCT score > 0)");
    }

    #[test]
    fn expr_display_name_lowercases_nested_function_names() {
        let expr = parse_select_expr("SELECT array_min(ARRAY_UNIQUE_AGG(col_boolean))");
        assert_eq!(
            expr_display_name(&expr),
            "array_min(array_unique_agg(col_boolean))"
        );
    }

    #[test]
    fn expr_display_name_preserves_array_unique_agg_name() {
        let expr = parse_select_expr("SELECT ARRAY_UNIQUE_AGG(s_1)");
        assert_eq!(expr_display_name(&expr), "array_unique_agg(s_1)");
    }

    #[test]
    fn sql_type_to_arrow_accepts_datetime_ns_alias() {
        let data_type = parse_native_cast_type("SELECT CAST(NULL AS DATETIME_NS)");

        assert_eq!(
            sql_type_to_arrow(&data_type).expect("type"),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn sql_decimal_type_producer_preserves_width_precision_scale_and_alias_domains() {
        for (type_name, expected) in [
            ("DECIMAL", DataType::Decimal128(38, 0)),
            ("DECIMAL32", DataType::Decimal128(9, 0)),
            ("DECIMAL64", DataType::Decimal128(18, 0)),
            ("DECIMAL128", DataType::Decimal128(38, 0)),
            ("DECIMAL(38,38)", DataType::Decimal128(38, 38)),
            ("DECIMAL(39,0)", DataType::Decimal256(39, 0)),
            ("DEC(60,2)", DataType::Decimal256(60, 2)),
            ("NUMERIC(76,76)", DataType::Decimal256(76, 76)),
            ("DECIMAL32(9,9)", DataType::Decimal128(9, 9)),
            ("DECIMAL64(18,18)", DataType::Decimal128(18, 18)),
            ("DECIMAL128(38,38)", DataType::Decimal128(38, 38)),
        ] {
            let target = parse_native_cast_type(&format!("SELECT CAST(NULL AS {type_name})"));
            assert_eq!(sql_type_to_arrow(&target).unwrap(), expected, "{type_name}");
        }
    }

    #[test]
    fn sql_decimal_type_producer_rejects_invalid_and_wrapping_parameters_at_type_span() {
        for type_name in [
            "DECIMAL(0,0)",
            "DECIMAL(77,0)",
            "DECIMAL(10,11)",
            "DECIMAL(294,0)",
            "DECIMAL(38,256)",
            "DECIMAL32(10,0)",
            "DECIMAL64(19,0)",
            "DECIMAL128(39,0)",
            "DECIMAL(10,2,3)",
        ] {
            let target = parse_native_cast_type(&format!("SELECT CAST(NULL AS {type_name})"));
            let error = sql_type_to_arrow(&target)
                .expect_err("invalid decimal type must fail before binding");
            assert_eq!(error.span(), Some(target.span), "{type_name}: {error}");
        }
        // Numeric type parameters accept integer tokens. Fractions are syntax
        // errors whose rejected span must identify source within this CAST.
        const FRACTIONAL_SOURCE: &str = "SELECT CAST(NULL AS DECIMAL(1.5,0))";
        let error = novarocks_parser::parse(FRACTIONAL_SOURCE)
            .expect_err("fractional type parameters have no final AST representation");
        let novarocks_parser::ParserError::Parse(novarocks_parser::ParseError::UnexpectedToken {
            span,
            ..
        }) = error
        else {
            panic!("fractional parameter must be a syntax rejection, got {error:?}");
        };
        let cast_start = FRACTIONAL_SOURCE.find("CAST").unwrap();
        assert!(!span.is_empty());
        assert!(span.start() >= cast_start && span.end() <= FRACTIONAL_SOURCE.len());
        // Signed parameters are already rejected by the native parser.
        for type_name in ["DECIMAL(-1,0)", "DECIMAL(10,-1)"] {
            assert!(
                novarocks_parser::parse(&format!("SELECT CAST(NULL AS {type_name})")).is_err(),
                "{type_name}"
            );
        }
    }

    #[test]
    fn sql_decimal_type_producer_preserves_nested_cast_carriers() {
        let target = parse_native_cast_type(
            "SELECT CAST(NULL AS STRUCT<a ARRAY<DECIMAL(60,2)>,m MAP<INT,DECIMAL(76,4)>>)",
        );
        let DataType::Struct(fields) = sql_type_to_arrow(&target).unwrap() else {
            panic!("expected STRUCT");
        };
        let DataType::List(item) = fields[0].data_type() else {
            panic!("expected ARRAY");
        };
        assert_eq!(item.data_type(), &DataType::Decimal256(60, 2));
        let DataType::Map(entries, _) = fields[1].data_type() else {
            panic!("expected MAP");
        };
        let DataType::Struct(entries) = entries.data_type() else {
            panic!("expected MAP key/value fields");
        };
        assert_eq!(entries[0].data_type(), &DataType::Int32);
        assert_eq!(entries[1].data_type(), &DataType::Decimal256(76, 4));
    }

    #[test]
    fn sql_type_to_arrow_accepts_variant_alias() {
        let data_type = parse_native_cast_type("SELECT CAST(NULL AS VARIANT)");

        assert_eq!(
            sql_type_to_arrow(&data_type).expect("type"),
            DataType::LargeBinary
        );
    }

    #[test]
    fn expr_display_name_formats_group_concat_like_starrocks() {
        let expr =
            parse_select_expr("SELECT group_concat(name, subject ORDER BY 1, 2 SEPARATOR ',')");
        assert_eq!(
            expr_display_name(&expr),
            "group_concat(name,subject ORDER BY name ASC, subject ASC SEPARATOR ',')"
        );
    }

    #[test]
    fn expr_display_name_normalizes_double_quoted_strings_to_single_quotes() {
        let expr = parse_select_expr("SELECT array_agg(\"中国\" ORDER BY 1, id)");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg('中国' ORDER BY id ASC)"
        );
    }

    #[test]
    fn expr_display_name_normalizes_array_literal_string_quotes() {
        let expr = parse_select_expr("SELECT array_agg(DISTINCT [json_object(\"2:3\")])");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg(DISTINCT [json_object('2:3')])"
        );
    }

    #[test]
    fn expr_display_name_formats_map_constructor_like_starrocks() {
        let expr = parse_select_expr("SELECT array_agg(map(2, 3))");
        assert_eq!(expr_display_name(&expr), "array_agg(map{2:3})");
    }

    #[test]
    fn expr_display_name_parenthesizes_is_not_null_inner_binary_expr() {
        let expr = parse_select_expr("SELECT count_if((v4 + v4) is not null)");
        assert_eq!(expr_display_name(&expr), "count_if((v4 + v4) IS NOT NULL)");
    }

    #[test]
    fn expr_display_name_formats_array_agg_distinct_like_starrocks() {
        let expr = parse_select_expr("SELECT array_agg_distinct(name ORDER BY 1 ASC)");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg(DISTINCT name ORDER BY name ASC)"
        );
    }

    #[test]
    fn expr_display_name_formats_in_subquery_like_starrocks() {
        let expr = parse_select_expr("SELECT ai_1 IN (SELECT ai_1 FROM db.array_test t)");
        assert_eq!(
            expr_display_name(&expr),
            "ai_1 IN (((SELECT ai_1 FROM db.array_test AS t)))"
        );
    }

    #[test]
    fn expr_display_name_preserves_lambda_field_paths() {
        let expr = parse_select_expr("SELECT array_sortby((x) -> x.item, x)");
        assert_eq!(expr_display_name(&expr), "array_sortby(x -> x.item, x)");
    }

    #[test]
    fn expr_display_name_strips_table_qualifier_for_compound_identifier() {
        // MySQL / StarRocks convention: `SELECT t.col` displays as `col`.
        // The qualifier exists for name resolution, not for the result
        // header. `expr_display_name_preserve_path` is the variant that
        // keeps the dotted chain when the caller needs the full path.
        let expr = parse_select_expr("SELECT c13.a");
        assert_eq!(expr_display_name(&expr), "a");
    }

    #[test]
    fn expr_display_name_preserves_struct_field_paths_inside_function_args() {
        let expr =
            parse_select_expr("SELECT cast(percentile_approx_weighted(c13.a, c1, 0.5) as int)");
        assert_eq!(
            expr_display_name(&expr),
            "CAST((percentile_approx_weighted(c13.a, c1, 0.5)) AS INT)"
        );
    }

    #[test]
    fn expr_display_name_canonicalizes_top_level_integer_cast_types() {
        let int_expr = parse_select_expr("SELECT CAST(NULL AS INT)");
        let tinyint_expr = parse_select_expr("SELECT CAST(NULL AS TINYINT)");
        let bigint_expr = parse_select_expr("SELECT CAST(NULL AS bigint)");

        assert_eq!(expr_display_name(&int_expr), "CAST(NULL AS INT)");
        assert_eq!(expr_display_name(&tinyint_expr), "CAST(NULL AS TINYINT)");
        assert_eq!(expr_display_name(&bigint_expr), "CAST(NULL AS BIGINT)");
    }

    #[test]
    fn expr_display_name_preserves_legacy_typeof_cast_spelling() {
        let scalar_expr = parse_select_expr(
            "SELECT typeof(CAST(1 AS tinyint), CAST(1 AS smallint), CAST(1 AS bigint))",
        );
        let nested_expr =
            parse_select_expr("SELECT typeof(greatest(CAST(100 AS int), CAST(200 AS bigint)))");

        assert_eq!(
            expr_display_name(&scalar_expr),
            "typeof(CAST(1 AS tinyint), CAST(1 AS smallint), CAST(1 AS bigint))"
        );
        assert_eq!(
            expr_display_name(&nested_expr),
            "typeof(greatest(CAST(100 AS INT), CAST(200 AS bigint)))"
        );
    }

    #[test]
    fn expr_display_name_keeps_json_struct_field_widths_but_canonicalizes_other_structs() {
        let json_expr = parse_select_expr(
            "SELECT CAST(parse_json('[1,2,3]') AS \
             STRUCT<col1 INT, col2 ARRAY<INT>, col3 STRUCT<nested TINYINT>>)",
        );
        let row_expr = parse_select_expr(
            "SELECT CAST(row(1, 2, 3) AS \
             STRUCT<col1 INT, col2 ARRAY<INT>, col3 STRUCT<nested TINYINT>>)",
        );

        assert_eq!(
            expr_display_name(&json_expr),
            "CAST((parse_json('[1,2,3]')) AS struct<col1 int(11), col2 array<int(11)>, \
             col3 struct<nested tinyint(4)>>)"
        );
        assert_eq!(
            expr_display_name(&row_expr),
            "CAST((row(1, 2, 3)) AS struct<col1 INT, col2 ARRAY<INT>, \
             col3 struct<nested TINYINT>>)"
        );
    }
    #[test]
    fn final_row_and_datetime_nodes_preserve_names_domains_and_microsecond_contract() {
        use novarocks_parser::{Span, ast, build};
        use novarocks_type_contract::{ValueKind, ValueLogicalType};
        let control = crate::compiler::SqlCompileControl::unbounded();
        let row = build::row_type(vec![ast::RowDataTypeField {
            name: None,
            data_type: build::simple_type("JSON"),
            span: Span::new(4, 8),
        }]);
        let value = super::sql_type_to_value_type(&row, true, &control).unwrap();
        let ValueKind::Struct(fields) = value.value_type().kind() else {
            panic!("row")
        };
        assert_eq!(fields[0].name(), "f1");
        assert!(matches!(
            fields[0].value_type().kind(),
            ValueKind::Nominal(ValueLogicalType::Json)
        ));
        for kind in [ast::DateTimeType::Time, ast::DateTimeType::Timestamp] {
            for precision in [None, Some(ast::AstBox::new(build::numeric_parameter(6)))] {
                let ty = ast::TypeName {
                    kind: ast::TypeKind::DateTime(ast::DateTimeDataType {
                        kind,
                        with_time_zone: false,
                        precision,
                    }),
                    span: Span::new(0, 12),
                };
                let expected = match kind {
                    ast::DateTimeType::Time => DataType::Time64(TimeUnit::Microsecond),
                    ast::DateTimeType::Timestamp => {
                        DataType::Timestamp(TimeUnit::Microsecond, None)
                    }
                };
                assert_eq!(sql_type_to_arrow(&ty).unwrap(), expected);
            }
        }
    }

    #[test]
    fn final_unsupported_type_and_literal_facts_reject_before_catalog_discovery() {
        use crate::analyze_error::AnalyzeErrorKind;
        use novarocks_parser::{Span, ast, build};
        struct NoLookup;
        impl crate::catalog::PlannerTableProvider for NoLookup {
            fn resolve_table_for_analysis(
                &self,
                _: Option<&str>,
                _: &str,
                _: &str,
            ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
                panic!("unsupported typed syntax must precede catalog lookup");
            }
        }
        let span = Span::new(20, 80);
        let qualifier = ast::IntervalQualifier::Simple(ast::SimpleIntervalQualifier {
            precision: None,
            field: ast::IntervalQualifierField::Year,
            span,
        });
        let explicit = ast::TypeName {
            kind: ast::TypeKind::ExplicitArray(ast::ExplicitArrayDataType {
                element: ast::AstBox::new(build::simple_type("INTEGER")),
                length: ast::ArrayLength {
                    spelling: "9999999999999999999999999999999999999999999999999".into(),
                    span,
                },
            }),
            span,
        };
        let unsupported_types = vec![
            explicit.clone(),
            build::type_name(
                build::ident("ARRAY", false),
                vec![build::type_parameter(explicit)],
            ),
            ast::TypeName {
                kind: ast::TypeKind::Interval(ast::IntervalDataType {
                    qualifier: qualifier.clone(),
                    span,
                }),
                span,
            },
            ast::TypeName {
                kind: ast::TypeKind::DateTime(ast::DateTimeDataType {
                    kind: ast::DateTimeType::Timestamp,
                    with_time_zone: true,
                    precision: None,
                }),
                span,
            },
            ast::TypeName {
                kind: ast::TypeKind::DateTime(ast::DateTimeDataType {
                    kind: ast::DateTimeType::Time,
                    with_time_zone: false,
                    precision: Some(ast::AstBox::new(build::numeric_parameter(3))),
                }),
                span,
            },
            ast::TypeName {
                kind: ast::TypeKind::DateTime(ast::DateTimeDataType {
                    kind: ast::DateTimeType::Timestamp,
                    with_time_zone: false,
                    precision: Some(ast::AstBox::new(build::type_parameter(build::simple_type(
                        "BIGINT",
                    )))),
                }),
                span,
            },
        ];
        let mut expressions = unsupported_types
            .into_iter()
            .map(|ty| build::cast(build::null(), ty))
            .collect::<Vec<_>>();
        for type_name in ["INTEGER", "\"DATE\""] {
            expressions.push(ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::Generic(ast::GenericLiteral {
                    type_name: type_name.into(),
                    value: "1".into(),
                }),
                span,
            }));
        }
        expressions.push(ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Interval(ast::IntervalLiteral {
                value: "1".into(),
                sign: ast::IntervalSign::Positive,
                qualifier,
            }),
            span,
        }));
        for expression in expressions {
            let mut statements =
                novarocks_parser::parse("SELECT NULL FROM missing_type_input").unwrap();
            let ast::Statement::Query(query) = &mut statements[0] else {
                panic!("query")
            };
            let ast::SetExpr::Select(select) = query.body.as_mut() else {
                panic!("select")
            };
            select.projection[0] = ast::SelectItem::UnnamedExpr(expression);
            let error = crate::analyzer::analyze(query, &NoLookup, "default").unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
            assert_eq!(error.span(), Some(span));
        }
    }
}
