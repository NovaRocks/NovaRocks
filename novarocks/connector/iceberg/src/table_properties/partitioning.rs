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

//! The provider's partition-field string grammar, without schema binding.

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorPartitionTransform,
    DdlPropertyValue,
};
use std::sync::Arc;

/// Parse an already evaluated ARRAY<VARCHAR>, retaining input order.
///
/// This is a property-domain parser, not SQL parsing or a schema capability.
/// Source IDs, source types, duplicate/redundant fields, generated names and
/// partition-spec publication remain with the original schema-bound authors.
/// Quoted names keep their decoded spelling; unquoted names become lowercase.
/// An empty input describes unpartitioned data, not a missing property.
pub fn parse_table_partitioning_property(
    value: &DdlPropertyValue,
    control: &dyn ConnectorOperationControl,
) -> Result<Vec<ConnectorPartitionTransform>, ConnectorError> {
    control.check_active()?;
    let DdlPropertyValue::ArrayVarchar(fields) = value else {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "partitioning requires a non-NULL array(varchar)",
        ));
    };
    let mut result = Vec::with_capacity(fields.len());
    for (index, field) in fields.iter().enumerate() {
        control.check_active()?;
        result.push(parse_optional_partitioning_element(
            field.as_deref(),
            index,
            control,
        )?);
    }
    control.check_active()?;
    Ok(result)
}

pub(super) fn parse_optional_partitioning_element(
    field: Option<&str>,
    index: usize,
    control: &dyn ConnectorOperationControl,
) -> Result<ConnectorPartitionTransform, ConnectorError> {
    let field = field
        .ok_or_else(|| invalid(&format!("partition field at index {index} cannot be NULL")))?;
    parse_table_partitioning_element(field, control)
}

/// Parse one partition-field declaration, without binding it to a schema.
///
/// Callers requiring schema-bound first-error order can parse and bind each
/// original field before advancing to the next one. No name/type/source lookup
/// or partition-spec mutation occurs here.
pub fn parse_table_partitioning_element(
    field: &str,
    control: &dyn ConnectorOperationControl,
) -> Result<ConnectorPartitionTransform, ConnectorError> {
    control.check_active()?;
    let result = parse_field(field, control)?;
    control.check_active()?;
    Ok(result)
}

struct Name {
    value: String,
    end: usize,
    quoted: bool,
}

fn name(
    text: &str,
    start: usize,
    control: &dyn ConnectorOperationControl,
) -> Result<Option<Name>, ConnectorError> {
    let bytes = text.as_bytes();
    if bytes.get(start) == Some(&b'"') {
        let mut end = start + 1;
        let mut value = String::new();
        while end < bytes.len() {
            control.check_active()?;
            if bytes[end] == b'"' {
                end += 1;
                if bytes.get(end) == Some(&b'"') {
                    value.push('"');
                    end += 1;
                } else {
                    return Ok(Some(Name {
                        value,
                        end,
                        quoted: true,
                    }));
                }
            } else {
                let character = text[end..]
                    .chars()
                    .next()
                    .expect("UTF-8 character boundary");
                value.push(character);
                end += character.len_utf8();
            }
        }
        return Ok(None);
    }
    if !bytes
        .get(start)
        .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
    {
        return Ok(None);
    }
    let mut end = start;
    let mut value = String::new();
    while let Some(byte) = bytes
        .get(end)
        .filter(|b| b.is_ascii_alphanumeric() || **b == b'_')
    {
        control.check_active()?;
        value.push(byte.to_ascii_lowercase() as char);
        end += 1;
    }
    Ok(Some(Name {
        value,
        end,
        quoted: false,
    }))
}

fn parse_field(
    text: &str,
    control: &dyn ConnectorOperationControl,
) -> Result<ConnectorPartitionTransform, ConnectorError> {
    let syntax = || invalid("invalid partition field declaration");
    let head = name(text, 0, control)?.ok_or_else(syntax)?;
    if head.end == text.len() {
        return Ok(ConnectorPartitionTransform::Identity {
            column: head.value.into(),
        });
    }
    if head.quoted || text.as_bytes().get(head.end) != Some(&b'(') {
        return Err(syntax());
    }
    let column = name(text, head.end + 1, control)?.ok_or_else(syntax)?;
    let mut end = column.end;
    let numeric = matches!(head.value.as_str(), "bucket" | "truncate");
    let width = if numeric {
        if text.as_bytes().get(end) != Some(&b',') {
            return Err(syntax());
        }
        end += 1;
        skip_java_space(text, &mut end, control)?;
        Some(positive_integer(text, &mut end, control)?)
    } else {
        None
    };
    if text.as_bytes().get(end) != Some(&b')') {
        return Err(syntax());
    }
    end += 1;
    // Only the single-name function patterns permit trailing Java regex \s*.
    if !numeric {
        skip_java_space(text, &mut end, control)?;
    }
    if end != text.len() {
        return Err(syntax());
    }
    let column: Arc<str> = column.value.into();
    Ok(match head.value.as_str() {
        "year" => ConnectorPartitionTransform::Year { column },
        "month" => ConnectorPartitionTransform::Month { column },
        "day" => ConnectorPartitionTransform::Day { column },
        "hour" => ConnectorPartitionTransform::Hour { column },
        "void" => ConnectorPartitionTransform::Void { column },
        "bucket" => ConnectorPartitionTransform::Bucket {
            column,
            num_buckets: width.expect("numeric function"),
        },
        "truncate" => ConnectorPartitionTransform::Truncate {
            column,
            width: width.expect("numeric function"),
        },
        _ => return Err(syntax()),
    })
}

fn skip_java_space(
    text: &str,
    position: &mut usize,
    control: &dyn ConnectorOperationControl,
) -> Result<(), ConnectorError> {
    while text
        .as_bytes()
        .get(*position)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r'))
    {
        control.check_active()?;
        *position += 1;
    }
    Ok(())
}

fn positive_integer(
    text: &str,
    position: &mut usize,
    control: &dyn ConnectorOperationControl,
) -> Result<u32, ConnectorError> {
    let first = *position;
    let mut value = 0_i32;
    while let Some(byte) = text
        .as_bytes()
        .get(*position)
        .filter(|byte| byte.is_ascii_digit())
    {
        control.check_active()?;
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(i32::from(*byte - b'0')))
            .ok_or_else(|| invalid("partition width is outside positive integer range"))?;
        *position += 1;
    }
    if *position == first || value <= 0 {
        return Err(invalid("partition width must be a positive integer"));
    }
    Ok(value as u32)
}

fn invalid(detail: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        format!("Invalid partitioning value: {detail}"),
    )
}
