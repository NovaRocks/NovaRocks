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

use std::collections::HashMap;

use novarocks_spi::connector::ConnectorOperationControl;

use super::{ReadFailure, check_read_control};
use crate::iceberg::spec::{NestedField, Schema, Type};

pub(super) struct NestedAverageSizeField<'a> {
    pub(super) id: i32,
    pub(super) name: &'a str,
    pub(super) leaves: Vec<i32>,
}

pub(super) fn nested_average_size_fields<'a>(
    schema: &'a Schema,
    requested: &[&str],
    control: Option<&dyn ConnectorOperationControl>,
) -> Result<Vec<NestedAverageSizeField<'a>>, ReadFailure> {
    let mut selected = Vec::new();
    for field in schema.as_struct().fields() {
        check_read_control(control)?;
        if !matches!(
            field.field_type.as_ref(),
            Type::Struct(_) | Type::List(_) | Type::Map(_)
        ) || !requested
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&field.name))
        {
            continue;
        }
        let mut leaves = Vec::new();
        if !collect_size_leaves(field, 1, &mut leaves, control)? {
            leaves.clear();
        }
        selected.push(NestedAverageSizeField {
            id: field.id,
            name: &field.name,
            leaves,
        });
    }
    Ok(selected)
}

fn collect_size_leaves(
    field: &NestedField,
    depth: usize,
    leaves: &mut Vec<i32>,
    control: Option<&dyn ConnectorOperationControl>,
) -> Result<bool, ReadFailure> {
    check_read_control(control)?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Ok(false);
    }
    match field.field_type.as_ref() {
        Type::Primitive(_) => {
            leaves.push(field.id);
            Ok(true)
        }
        Type::Struct(children) => {
            for child in children.fields() {
                if !collect_size_leaves(child, depth + 1, leaves, control)? {
                    return Ok(false);
                }
            }
            Ok(!children.fields().is_empty())
        }
        Type::List(list) => collect_size_leaves(&list.element_field, depth + 1, leaves, control),
        Type::Map(map) => Ok(
            collect_size_leaves(&map.key_field, depth + 1, leaves, control)?
                && collect_size_leaves(&map.value_field, depth + 1, leaves, control)?,
        ),
        Type::Variant(_) => Ok(false),
    }
}

/// A nested root owns width evidence only if this file reports every leaf from
/// the snapshot schema. Omitted leaves and overflow do not grant a size.
pub(super) fn manifest_nested_size(
    column_sizes: &HashMap<i32, u64>,
    leaves: &[i32],
    control: Option<&dyn ConnectorOperationControl>,
) -> Result<Option<i64>, ReadFailure> {
    if leaves.is_empty() {
        return Ok(None);
    }
    let mut size = 0_i64;
    for leaf in leaves {
        check_read_control(control)?;
        let Some(next) = column_sizes
            .get(leaf)
            .and_then(|bytes| i64::try_from(*bytes).ok())
            .and_then(|bytes| size.checked_add(bytes))
        else {
            return Ok(None);
        };
        size = next;
    }
    Ok(Some(size))
}

#[cfg(test)]
mod tests;
