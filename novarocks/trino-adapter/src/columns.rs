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

//! Trino column metadata projected only from the frozen client render contract.
use novarocks_result_contract::{
    ClientPresentationFamily, ClientRenderSchema, NativeRenderType, RenderTimeUnit, TrinoJsonFacts,
};
use serde::Serialize;
use std::fmt::{self, Write};

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TrinoColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub type_signature: ClientTypeSignature,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClientTypeSignature {
    raw_type: &'static str,
    arguments: Vec<ClientTypeArgument>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "kind", content = "value")]
enum ClientTypeArgument {
    #[serde(rename = "LONG")]
    Long(u64),
    #[serde(rename = "TYPE")]
    Type(ClientTypeSignature),
    #[serde(rename = "NAMED_TYPE")]
    NamedType(NamedTypeSignature),
}
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct NamedTypeSignature {
    field_name: RowFieldName,
    type_signature: ClientTypeSignature,
}
#[derive(Clone, Debug, Serialize, PartialEq)]
struct RowFieldName {
    name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnsError {
    WrongPresentationFamily,
    UnsupportedFrozenType,
    BackingSizeOverflow,
    BackingLimitExceeded,
    AllocationFailed,
}
impl std::fmt::Display for ColumnsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::WrongPresentationFamily => {
                "Trino metadata requires the frozen Trino presentation family"
            }
            Self::UnsupportedFrozenType => "Unsupported type in frozen Trino metadata",
            Self::BackingSizeOverflow => "Trino column metadata backing size overflows",
            Self::BackingLimitExceeded => {
                "Trino column metadata exceeds its original backing limit"
            }
            Self::AllocationFailed => "Trino column metadata allocation failed",
        })
    }
}
impl std::error::Error for ColumnsError {}
impl ColumnsError {
    pub(crate) fn is_backing_failure(self) -> bool {
        matches!(
            self,
            Self::BackingSizeOverflow | Self::BackingLimitExceeded | Self::AllocationFailed
        )
    }
}

/// Observe the complete owned heap graph, including spare vector/string capacity.
/// Inline column and argument storage is counted by its containing vector only;
/// static type-name literals own no allocation. This observation grants no bytes.
pub(crate) fn columns_backing_bytes(columns: &Vec<TrinoColumn>) -> Result<usize, ColumnsError> {
    let mut bytes = columns
        .capacity()
        .checked_mul(std::mem::size_of::<TrinoColumn>())
        .ok_or(ColumnsError::BackingSizeOverflow)?;
    for column in columns {
        add_backing(&mut bytes, column.name.capacity())?;
        add_backing(&mut bytes, column.type_name.capacity())?;
        signature_backing(&column.type_signature, &mut bytes)?;
    }
    Ok(bytes)
}

fn add_backing(bytes: &mut usize, additional: usize) -> Result<(), ColumnsError> {
    *bytes = bytes
        .checked_add(additional)
        .ok_or(ColumnsError::BackingSizeOverflow)?;
    Ok(())
}

fn signature_backing(
    signature: &ClientTypeSignature,
    bytes: &mut usize,
) -> Result<(), ColumnsError> {
    add_backing(
        bytes,
        signature
            .arguments
            .capacity()
            .checked_mul(std::mem::size_of::<ClientTypeArgument>())
            .ok_or(ColumnsError::BackingSizeOverflow)?,
    )?;
    for argument in &signature.arguments {
        match argument {
            ClientTypeArgument::Long(_) => {}
            ClientTypeArgument::Type(child) => signature_backing(child, bytes)?,
            ClientTypeArgument::NamedType(child) => {
                add_backing(bytes, child.field_name.name.capacity())?;
                signature_backing(&child.type_signature, bytes)?;
            }
        }
    }
    Ok(())
}

/// A source-only bound for all new owned metadata, computed without allocation.
/// The builder below allocates exactly these final blocks and never creates
/// transient child type-name strings, escaped-name strings or join buffers.
pub(crate) fn columns_construction_backing_bound(
    schema: &ClientRenderSchema,
) -> Result<usize, ColumnsError> {
    let facts = trino_facts(schema)?;
    let mut bytes = schema
        .columns()
        .len()
        .checked_mul(std::mem::size_of::<TrinoColumn>())
        .ok_or(ColumnsError::BackingSizeOverflow)?;
    for column in schema.columns() {
        let signature = signature_construction_backing(&column.field.native_type, facts)?;
        add_backing(&mut bytes, column.name.len())?;
        add_backing(
            &mut bytes,
            counted_type_name(&column.field.native_type, facts)?,
        )?;
        add_backing(&mut bytes, signature)?;
    }
    Ok(bytes)
}

/// Pure callers use the exact construction bound. Runtime callers must supply
/// the available extent already admitted by their original result window.
#[cfg(test)]
pub(crate) fn columns(schema: &ClientRenderSchema) -> Result<Vec<TrinoColumn>, ColumnsError> {
    columns_with_backing_limit(schema, columns_construction_backing_bound(schema)?)
}
pub(crate) fn columns_with_backing_limit(
    schema: &ClientRenderSchema,
    limit: usize,
) -> Result<Vec<TrinoColumn>, ColumnsError> {
    let bound = columns_construction_backing_bound(schema)?;
    if bound > limit {
        return Err(ColumnsError::BackingLimitExceeded);
    }
    let facts = trino_facts(schema)?;
    let mut projected = exact_vec(schema.columns().len())?;
    for column in schema.columns() {
        projected.push(TrinoColumn {
            name: exact_copy(&column.name)?,
            type_name: exact_type_name(&column.field.native_type, facts)?,
            type_signature: exact_signature(&column.field.native_type, facts)?,
        });
    }
    if columns_backing_bytes(&projected)? > bound {
        return Err(ColumnsError::BackingLimitExceeded);
    }
    Ok(projected)
}
fn trino_facts(schema: &ClientRenderSchema) -> Result<TrinoJsonFacts, ColumnsError> {
    match schema.family() {
        ClientPresentationFamily::TrinoJson(facts) => Ok(*facts),
        ClientPresentationFamily::MysqlText => Err(ColumnsError::WrongPresentationFamily),
    }
}
fn exact_vec<T>(length: usize) -> Result<Vec<T>, ColumnsError> {
    let mut value = Vec::new();
    value
        .try_reserve_exact(length)
        .map_err(|_| ColumnsError::AllocationFailed)?;
    if value.capacity() != length {
        return Err(ColumnsError::BackingLimitExceeded);
    }
    Ok(value)
}
fn exact_string(length: usize) -> Result<String, ColumnsError> {
    let mut value = String::new();
    value
        .try_reserve_exact(length)
        .map_err(|_| ColumnsError::AllocationFailed)?;
    if value.capacity() != length {
        return Err(ColumnsError::BackingLimitExceeded);
    }
    Ok(value)
}
fn exact_copy(source: &str) -> Result<String, ColumnsError> {
    let mut value = exact_string(source.len())?;
    value.push_str(source);
    Ok(value)
}
fn signature(raw_type: &'static str, arguments: Vec<ClientTypeArgument>) -> ClientTypeSignature {
    ClientTypeSignature {
        raw_type,
        arguments,
    }
}
fn precision(unit: RenderTimeUnit) -> u64 {
    match unit {
        RenderTimeUnit::Second => 0,
        RenderTimeUnit::Millisecond => 3,
        RenderTimeUnit::Microsecond => 6,
        RenderTimeUnit::Nanosecond => 9,
    }
}
#[derive(Clone, Copy)]
enum ScalarName {
    Plain(&'static str),
    Decimal(u8, i8),
    Temporal {
        keyword: &'static str,
        precision: u64,
        suffix: &'static str,
    },
}
struct ScalarInfo {
    raw_type: &'static str,
    name: ScalarName,
    arguments: [u64; 2],
    argument_count: usize,
}
enum TypeInfo<'a> {
    Scalar(ScalarInfo),
    Array(&'a novarocks_result_contract::RenderField),
    Map(
        &'a novarocks_result_contract::RenderField,
        &'a novarocks_result_contract::RenderField,
    ),
    Row(&'a [novarocks_result_contract::NamedRenderField]),
}
fn type_info(ty: &NativeRenderType, facts: TrinoJsonFacts) -> Result<TypeInfo<'_>, ColumnsError> {
    use NativeRenderType as N;
    let plain = |name| {
        TypeInfo::Scalar(ScalarInfo {
            raw_type: name,
            name: ScalarName::Plain(name),
            arguments: [0; 2],
            argument_count: 0,
        })
    };
    Ok(match ty {
        N::Null => plain("unknown"),
        N::Boolean => plain("boolean"),
        N::SignedInteger(bits) => plain(match bits {
            8 => "tinyint",
            16 => "smallint",
            32 => "integer",
            64 => "bigint",
            _ => return Err(ColumnsError::UnsupportedFrozenType),
        }),
        N::Float32 => plain("real"),
        N::Float64 => plain("double"),
        N::Decimal {
            precision, scale, ..
        } if *precision <= 38 && *scale >= 0 => TypeInfo::Scalar(ScalarInfo {
            raw_type: "decimal",
            name: ScalarName::Decimal(*precision, *scale),
            arguments: [u64::from(*precision), *scale as u64],
            argument_count: 2,
        }),
        N::String => TypeInfo::Scalar(ScalarInfo {
            raw_type: "varchar",
            name: ScalarName::Plain("varchar"),
            arguments: [2_147_483_647, 0],
            argument_count: 1,
        }),
        N::Binary => plain("varbinary"),
        N::Date => plain("date"),
        N::Time { unit } | N::Timestamp { unit, .. } => {
            let (raw_type, keyword, suffix) = match ty {
                N::Time { .. } => ("time", "time", ""),
                N::Timestamp { timezone: None, .. } => ("timestamp", "timestamp", ""),
                N::Timestamp {
                    timezone: Some(_), ..
                } => ("timestamp with time zone", "timestamp", " with time zone"),
                _ => unreachable!("matched temporal type"),
            };
            if facts.parametric_datetime {
                let precision = precision(*unit);
                TypeInfo::Scalar(ScalarInfo {
                    raw_type,
                    name: ScalarName::Temporal {
                        keyword,
                        precision,
                        suffix,
                    },
                    arguments: [precision, 0],
                    argument_count: 1,
                })
            } else {
                plain(raw_type)
            }
        }
        N::Json => plain("json"),
        N::Variant => plain(if facts.variant || facts.variant_binary {
            "variant"
        } else {
            "json"
        }),
        N::List(child) => TypeInfo::Array(child),
        N::Map { key, value } => TypeInfo::Map(key, value),
        N::Struct(fields) => TypeInfo::Row(fields),
        N::LargeInt | N::UnsignedInteger(_) | N::Opaque(_) | N::Decimal { .. } => {
            return Err(ColumnsError::UnsupportedFrozenType);
        }
    })
}
fn argument_backing(length: usize) -> Result<usize, ColumnsError> {
    length
        .checked_mul(std::mem::size_of::<ClientTypeArgument>())
        .ok_or(ColumnsError::BackingSizeOverflow)
}
fn signature_construction_backing(
    ty: &NativeRenderType,
    facts: TrinoJsonFacts,
) -> Result<usize, ColumnsError> {
    let mut bytes = 0;
    match type_info(ty, facts)? {
        TypeInfo::Scalar(info) => add_backing(&mut bytes, argument_backing(info.argument_count)?)?,
        TypeInfo::Array(child) => {
            add_backing(&mut bytes, argument_backing(1)?)?;
            add_backing(
                &mut bytes,
                signature_construction_backing(&child.native_type, facts)?,
            )?;
        }
        TypeInfo::Map(key, value) => {
            add_backing(&mut bytes, argument_backing(2)?)?;
            add_backing(
                &mut bytes,
                signature_construction_backing(&key.native_type, facts)?,
            )?;
            add_backing(
                &mut bytes,
                signature_construction_backing(&value.native_type, facts)?,
            )?;
        }
        TypeInfo::Row(fields) => {
            add_backing(&mut bytes, argument_backing(fields.len())?)?;
            for field in fields {
                add_backing(&mut bytes, field.name.len())?;
                add_backing(
                    &mut bytes,
                    signature_construction_backing(&field.field.native_type, facts)?,
                )?;
            }
        }
    }
    Ok(bytes)
}
fn exact_signature(
    ty: &NativeRenderType,
    facts: TrinoJsonFacts,
) -> Result<ClientTypeSignature, ColumnsError> {
    Ok(match type_info(ty, facts)? {
        TypeInfo::Scalar(info) => {
            let mut arguments = exact_vec(info.argument_count)?;
            for value in &info.arguments[..info.argument_count] {
                arguments.push(ClientTypeArgument::Long(*value));
            }
            signature(info.raw_type, arguments)
        }
        TypeInfo::Array(child) => {
            let mut arguments = exact_vec(1)?;
            arguments.push(ClientTypeArgument::Type(exact_signature(
                &child.native_type,
                facts,
            )?));
            signature("array", arguments)
        }
        TypeInfo::Map(key, value) => {
            let mut arguments = exact_vec(2)?;
            arguments.push(ClientTypeArgument::Type(exact_signature(
                &key.native_type,
                facts,
            )?));
            arguments.push(ClientTypeArgument::Type(exact_signature(
                &value.native_type,
                facts,
            )?));
            signature("map", arguments)
        }
        TypeInfo::Row(fields) => {
            let mut arguments = exact_vec(fields.len())?;
            for field in fields {
                arguments.push(ClientTypeArgument::NamedType(NamedTypeSignature {
                    field_name: RowFieldName {
                        name: exact_copy(&field.name)?,
                    },
                    type_signature: exact_signature(&field.field.native_type, facts)?,
                }));
            }
            signature("row", arguments)
        }
    })
}
struct NameCount {
    bytes: usize,
    overflowed: bool,
}
impl Write for NameCount {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let Some(bytes) = self.bytes.checked_add(value.len()) else {
            self.overflowed = true;
            return Err(fmt::Error);
        };
        self.bytes = bytes;
        Ok(())
    }
}
fn counted_type_name(ty: &NativeRenderType, facts: TrinoJsonFacts) -> Result<usize, ColumnsError> {
    let mut count = NameCount {
        bytes: 0,
        overflowed: false,
    };
    write_type_name(&mut count, ty, facts).map_err(|_| {
        if count.overflowed {
            ColumnsError::BackingSizeOverflow
        } else {
            ColumnsError::UnsupportedFrozenType
        }
    })?;
    Ok(count.bytes)
}
struct ExactName {
    value: String,
    limit: usize,
}
impl Write for ExactName {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if value.len() > self.limit - self.value.len() {
            return Err(fmt::Error);
        }
        self.value.push_str(value);
        Ok(())
    }
}
fn exact_type_name(ty: &NativeRenderType, facts: TrinoJsonFacts) -> Result<String, ColumnsError> {
    let length = counted_type_name(ty, facts)?;
    let mut writer = ExactName {
        value: exact_string(length)?,
        limit: length,
    };
    write_type_name(&mut writer, ty, facts).map_err(|_| ColumnsError::BackingLimitExceeded)?;
    if writer.value.len() != length {
        return Err(ColumnsError::BackingLimitExceeded);
    }
    Ok(writer.value)
}
fn write_type_name(
    out: &mut impl Write,
    ty: &NativeRenderType,
    facts: TrinoJsonFacts,
) -> fmt::Result {
    match type_info(ty, facts).map_err(|_| fmt::Error)? {
        TypeInfo::Scalar(info) => match info.name {
            ScalarName::Plain(name) => out.write_str(name),
            ScalarName::Decimal(precision, scale) => write!(out, "decimal({precision}, {scale})"),
            ScalarName::Temporal {
                keyword,
                precision,
                suffix,
            } => write!(out, "{keyword}({precision}){suffix}"),
        },
        TypeInfo::Array(child) => {
            out.write_str("array(")?;
            write_type_name(out, &child.native_type, facts)?;
            out.write_char(')')
        }
        TypeInfo::Map(key, value) => {
            out.write_str("map(")?;
            write_type_name(out, &key.native_type, facts)?;
            out.write_str(", ")?;
            write_type_name(out, &value.native_type, facts)?;
            out.write_char(')')
        }
        TypeInfo::Row(fields) => {
            out.write_str("row(")?;
            for (index, field) in fields.iter().enumerate() {
                if index != 0 {
                    out.write_str(", ")?;
                }
                write_field_name(out, &field.name)?;
                out.write_char(' ')?;
                write_type_name(out, &field.field.native_type, facts)?;
            }
            out.write_char(')')
        }
    }
}
fn write_field_name(out: &mut impl Write, name: &str) -> fmt::Result {
    if field_name_unquoted(name) {
        return out.write_str(name);
    }
    out.write_char('"')?;
    for ch in name.chars() {
        if ch == '"' {
            out.write_str("\"\"")?;
        } else {
            out.write_char(ch)?;
        }
    }
    out.write_char('"')
}

fn field_name_unquoted(name: &str) -> bool {
    // Trino 483 TypeDescriptorTranslator uses its identifier grammar and reserved
    // identifier set before ProtocolUtil formats row fields. These are protocol
    // display rules, independent of NovaRocks SQL parser keywords.
    const RESERVED: &[&str] = &[
        "ALTER",
        "AND",
        "AS",
        "AUTO",
        "BETWEEN",
        "BY",
        "CASE",
        "CAST",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CUBE",
        "CURRENT_CATALOG",
        "CURRENT_DATE",
        "CURRENT_PATH",
        "CURRENT_ROLE",
        "CURRENT_SCHEMA",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DEALLOCATE",
        "DELETE",
        "DESCRIBE",
        "DISTINCT",
        "DROP",
        "ELSE",
        "END",
        "ESCAPE",
        "EXCEPT",
        "EXISTS",
        "EXTRACT",
        "FALSE",
        "FOR",
        "FROM",
        "FULL",
        "GROUP",
        "GROUPING",
        "HAVING",
        "IN",
        "INNER",
        "INSERT",
        "INTERSECT",
        "INTO",
        "IS",
        "JOIN",
        "JSON_ARRAY",
        "JSON_EXISTS",
        "JSON_OBJECT",
        "JSON_QUERY",
        "JSON_TABLE",
        "JSON_VALUE",
        "LEFT",
        "LIKE",
        "LISTAGG",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "NATURAL",
        "NORMALIZE",
        "NOT",
        "NULL",
        "ON",
        "OR",
        "ORDER",
        "OUTER",
        "OVERLAPS",
        "PREPARE",
        "RECURSIVE",
        "RIGHT",
        "ROLLUP",
        "SELECT",
        "SKIP",
        "TABLE",
        "THEN",
        "TRIM",
        "TRUE",
        "UESCAPE",
        "UNION",
        "UNNEST",
        "USING",
        "VALUES",
        "WHEN",
        "WHERE",
        "WITH",
    ];
    let valid = !name.is_empty()
        && !name.as_bytes()[0].is_ascii_digit()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    valid
        && !RESERVED
            .iter()
            .any(|reserved| name.eq_ignore_ascii_case(reserved))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_result_contract::{
        NamedRenderField, RenderColumn, RenderField, RenderPresentation,
    };
    use serde_json::json;
    fn field(native_type: NativeRenderType) -> RenderField {
        RenderField {
            native_type,
            nullable: true,
            presentation: RenderPresentation::TrinoValue,
        }
    }
    fn schema(types: Vec<NativeRenderType>, facts: TrinoJsonFacts) -> ClientRenderSchema {
        let len = types.len();
        ClientRenderSchema::try_new(
            ClientPresentationFamily::TrinoJson(facts),
            types
                .into_iter()
                .enumerate()
                .map(|(index, ty)| RenderColumn {
                    source_ordinal: index as u32,
                    source_slot: None,
                    name: format!("c{index}"),
                    field: field(ty),
                })
                .collect(),
            len,
        )
        .unwrap()
    }
    #[test]
    fn scalar_metadata_uses_trino_names_and_signature_parameter_kinds() {
        let types = vec![
            NativeRenderType::Null,
            NativeRenderType::Boolean,
            NativeRenderType::SignedInteger(8),
            NativeRenderType::SignedInteger(16),
            NativeRenderType::SignedInteger(32),
            NativeRenderType::SignedInteger(64),
            NativeRenderType::Float32,
            NativeRenderType::Float64,
            NativeRenderType::String,
            NativeRenderType::Binary,
            NativeRenderType::Date,
            NativeRenderType::Json,
            NativeRenderType::Decimal {
                bits: 128,
                precision: 10,
                scale: 2,
            },
        ];
        let columns = columns(&schema(types, TrinoJsonFacts::default())).unwrap();
        assert_eq!(
            columns
                .iter()
                .map(|column| column.type_name.as_str())
                .collect::<Vec<_>>(),
            [
                "unknown",
                "boolean",
                "tinyint",
                "smallint",
                "integer",
                "bigint",
                "real",
                "double",
                "varchar",
                "varbinary",
                "date",
                "json",
                "decimal(10, 2)"
            ]
        );
        assert_eq!(
            serde_json::to_value(&columns[8]).unwrap(),
            json!({"name":"c8", "type":"varchar", "typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}})
        );
        assert_eq!(
            serde_json::to_value(&columns[12].type_signature).unwrap(),
            json!({"rawType":"decimal","arguments":[{"kind":"LONG","value":10},{"kind":"LONG","value":2}]})
        );
    }
    #[test]
    fn presentation_capabilities_apply_recursively_to_types_and_signatures() {
        let ty = NativeRenderType::Struct(vec![
            NamedRenderField {
                name: "event".into(),
                field: field(NativeRenderType::Timestamp {
                    unit: RenderTimeUnit::Microsecond,
                    timezone: Some("UTC".into()),
                }),
            },
            NamedRenderField {
                name: "value".into(),
                field: field(NativeRenderType::Variant),
            },
        ]);
        for (facts, time, variant) in [
            (
                TrinoJsonFacts::default(),
                "timestamp with time zone",
                "json",
            ),
            (
                TrinoJsonFacts {
                    parametric_datetime: true,
                    variant: true,
                    variant_binary: false,
                },
                "timestamp(6) with time zone",
                "variant",
            ),
            (
                TrinoJsonFacts {
                    parametric_datetime: false,
                    variant: false,
                    variant_binary: true,
                },
                "timestamp with time zone",
                "variant",
            ),
        ] {
            let columns = columns(&schema(vec![ty.clone()], facts)).unwrap();
            assert_eq!(
                columns[0].type_name,
                format!("row(event {time}, value {variant})")
            );
            let value = serde_json::to_value(&columns[0].type_signature).unwrap();
            assert_eq!(value["arguments"][0]["kind"], "NAMED_TYPE");
            assert_eq!(
                value["arguments"][0]["value"]["fieldName"],
                json!({"name":"event"})
            );
            assert_eq!(
                value["arguments"][0]["value"]["typeSignature"]["rawType"],
                "timestamp with time zone"
            );
            assert_eq!(
                value["arguments"][1]["value"]["typeSignature"]["rawType"],
                variant
            );
            assert_eq!(
                value["arguments"][0]["value"]["typeSignature"]["arguments"],
                if facts.parametric_datetime {
                    json!([{"kind":"LONG","value":6}])
                } else {
                    json!([])
                }
            );
        }
    }
    #[test]
    fn nested_container_metadata_and_quoted_row_names_are_exact() {
        let ty = NativeRenderType::List(Box::new(field(NativeRenderType::Map {
            key: Box::new(field(NativeRenderType::String)),
            value: Box::new(field(NativeRenderType::Struct(vec![
                NamedRenderField {
                    name: "select".into(),
                    field: field(NativeRenderType::Boolean),
                },
                NamedRenderField {
                    name: "a\"b".into(),
                    field: field(NativeRenderType::SignedInteger(64)),
                },
            ]))),
        })));
        let columns = columns(&schema(vec![ty], TrinoJsonFacts::default())).unwrap();
        assert_eq!(
            columns[0].type_name,
            "array(map(varchar, row(\"select\" boolean, \"a\"\"b\" bigint)))"
        );
        let value = serde_json::to_value(&columns[0]).unwrap();
        assert_eq!(value["typeSignature"]["arguments"][0]["kind"], "TYPE");
        assert_eq!(
            value["typeSignature"]["arguments"][0]["value"]["rawType"],
            "map"
        );
    }
    #[test]
    fn temporal_units_have_exact_precision_or_no_precision() {
        for (unit, p) in [
            (RenderTimeUnit::Second, 0),
            (RenderTimeUnit::Millisecond, 3),
            (RenderTimeUnit::Microsecond, 6),
            (RenderTimeUnit::Nanosecond, 9),
        ] {
            for parametric_datetime in [true, false] {
                let facts = TrinoJsonFacts {
                    parametric_datetime,
                    ..Default::default()
                };
                let columns = columns(&schema(
                    vec![
                        NativeRenderType::Time { unit },
                        NativeRenderType::Timestamp {
                            unit,
                            timezone: None,
                        },
                    ],
                    facts,
                ))
                .unwrap();
                assert_eq!(
                    columns[0].type_name,
                    if parametric_datetime {
                        format!("time({p})")
                    } else {
                        "time".into()
                    }
                );
                assert_eq!(
                    columns[1].type_name,
                    if parametric_datetime {
                        format!("timestamp({p})")
                    } else {
                        "timestamp".into()
                    }
                );
            }
        }
    }
    #[test]
    fn mysql_family_is_an_internal_contract_error() {
        let schema = ClientRenderSchema::try_new(
            ClientPresentationFamily::MysqlText,
            vec![RenderColumn {
                source_ordinal: 0,
                source_slot: None,
                name: "c".into(),
                field: RenderField {
                    native_type: NativeRenderType::String,
                    nullable: false,
                    presentation: RenderPresentation::ScalarText,
                },
            }],
            1,
        )
        .unwrap();
        assert_eq!(columns(&schema), Err(ColumnsError::WrongPresentationFamily));
    }

    #[test]
    fn backing_observation_counts_spare_capacity_and_nested_heap_once() {
        let mut field_name = String::with_capacity(73);
        field_name.push_str("field");
        let mut nested_arguments = Vec::with_capacity(5);
        nested_arguments.push(ClientTypeArgument::Long(17));
        let nested_capacity = nested_arguments.capacity();
        let mut arguments = Vec::with_capacity(7);
        arguments.push(ClientTypeArgument::NamedType(NamedTypeSignature {
            field_name: RowFieldName { name: field_name },
            type_signature: signature("decimal", nested_arguments),
        }));
        let mut type_arguments = Vec::with_capacity(3);
        type_arguments.push(ClientTypeArgument::Long(2_147_483_647));
        let type_arguments_capacity = type_arguments.capacity();
        arguments.push(ClientTypeArgument::Type(signature(
            "varchar",
            type_arguments,
        )));
        let arguments_capacity = arguments.capacity();
        let mut name = String::with_capacity(91);
        name.push('c');
        let mut type_name = String::with_capacity(117);
        type_name.push_str("row(field decimal(17, 0))");
        let mut projected = Vec::with_capacity(11);
        projected.push(TrinoColumn {
            name,
            type_name,
            type_signature: signature("row", arguments),
        });
        let column = &projected[0];
        let ClientTypeArgument::NamedType(named) = &column.type_signature.arguments[0] else {
            unreachable!("test constructed a named argument");
        };
        let expected = projected.capacity() * std::mem::size_of::<TrinoColumn>()
            + column.name.capacity()
            + column.type_name.capacity()
            + arguments_capacity * std::mem::size_of::<ClientTypeArgument>()
            + named.field_name.name.capacity()
            + nested_capacity * std::mem::size_of::<ClientTypeArgument>()
            + type_arguments_capacity * std::mem::size_of::<ClientTypeArgument>();
        assert_eq!(columns_backing_bytes(&projected).unwrap(), expected);
        assert!(expected > serde_json::to_vec(&projected).unwrap().len());
    }

    #[test]
    fn backing_observation_follows_actual_capacity_without_counting_static_type_literals() {
        let mut projected = Vec::with_capacity(2);
        projected.push(TrinoColumn {
            name: String::new(),
            type_name: String::new(),
            type_signature: signature("timestamp with time zone", Vec::new()),
        });
        assert_eq!(
            columns_backing_bytes(&projected).unwrap(),
            projected.capacity() * std::mem::size_of::<TrinoColumn>(),
        );
        let before = columns_backing_bytes(&projected).unwrap();
        let previous = projected[0].name.capacity();
        projected[0].name.reserve_exact(1024);
        assert_eq!(
            columns_backing_bytes(&projected).unwrap() - before,
            projected[0].name.capacity() - previous,
        );
        assert_eq!(columns_backing_bytes(&Vec::new()).unwrap(), 0);
    }

    #[test]
    fn backing_size_overflow_is_a_typed_refusal() {
        let mut bytes = usize::MAX;
        assert_eq!(
            add_backing(&mut bytes, 1),
            Err(ColumnsError::BackingSizeOverflow)
        );
        assert_eq!(bytes, usize::MAX);
    }

    fn assert_exact_construction(schema: &ClientRenderSchema) -> Vec<TrinoColumn> {
        let bound = columns_construction_backing_bound(schema).unwrap();
        assert!(bound > 0);
        assert_eq!(
            columns_with_backing_limit(schema, bound - 1),
            Err(ColumnsError::BackingLimitExceeded),
        );
        let output = columns_with_backing_limit(schema, bound).unwrap();
        assert_eq!(columns_backing_bytes(&output).unwrap(), bound);
        assert_eq!(output.capacity(), output.len());
        for column in &output {
            assert_eq!(column.name.capacity(), column.name.len());
            assert_eq!(column.type_name.capacity(), column.type_name.len());
        }
        output
    }

    #[test]
    fn construction_bound_matches_exact_builder_for_every_supported_scalar_and_capability() {
        for parametric_datetime in [false, true] {
            for variant in [false, true] {
                for variant_binary in [false, true] {
                    let facts = TrinoJsonFacts {
                        parametric_datetime,
                        variant,
                        variant_binary,
                    };
                    let mut types = vec![
                        NativeRenderType::Null,
                        NativeRenderType::Boolean,
                        NativeRenderType::Float32,
                        NativeRenderType::Float64,
                        NativeRenderType::String,
                        NativeRenderType::Binary,
                        NativeRenderType::Date,
                        NativeRenderType::Json,
                        NativeRenderType::Variant,
                    ];
                    for bits in [8, 16, 32, 64] {
                        types.push(NativeRenderType::SignedInteger(bits));
                    }
                    for unit in [
                        RenderTimeUnit::Second,
                        RenderTimeUnit::Millisecond,
                        RenderTimeUnit::Microsecond,
                        RenderTimeUnit::Nanosecond,
                    ] {
                        types.push(NativeRenderType::Time { unit });
                        types.push(NativeRenderType::Timestamp {
                            unit,
                            timezone: None,
                        });
                        types.push(NativeRenderType::Timestamp {
                            unit,
                            timezone: Some("UTC".into()),
                        });
                    }
                    for bits in [128, 256] {
                        // Frozen presentation metadata accepts nested Decimal256;
                        // top-level SQL carrier admission remains its original owner.
                        types.push(NativeRenderType::List(Box::new(field(
                            NativeRenderType::Decimal {
                                bits,
                                precision: 38,
                                scale: 18,
                            },
                        ))));
                    }
                    let schema = schema(types, facts);
                    let _ = assert_exact_construction(&schema);
                }
            }
        }
    }

    #[test]
    fn deep_and_wide_metadata_has_no_unaccounted_child_name_or_join_allocations() {
        let mut deep = NativeRenderType::String;
        for _ in 1..novarocks_result_contract::RootProfileV1::MAX_DEPTH {
            deep = NativeRenderType::List(Box::new(field(deep)));
        }
        let deep_schema = schema(vec![deep], TrinoJsonFacts::default());
        let deep_columns = assert_exact_construction(&deep_schema);
        let depth = novarocks_result_contract::RootProfileV1::MAX_DEPTH - 1;
        assert_eq!(
            deep_columns[0].type_name,
            format!("{}varchar{}", "array(".repeat(depth), ")".repeat(depth))
        );

        let mut children = Vec::with_capacity(64);
        for index in 0..32 {
            let mut name = String::with_capacity(1024);
            name.push_str(&format!("quoted\"中{index}"));
            children.push(NamedRenderField {
                name,
                field: field(NativeRenderType::Map {
                    key: Box::new(field(NativeRenderType::String)),
                    value: Box::new(field(NativeRenderType::Timestamp {
                        unit: RenderTimeUnit::Nanosecond,
                        timezone: Some("UTC".into()),
                    })),
                }),
            });
        }
        let wide_schema = schema(
            vec![NativeRenderType::Struct(children)],
            TrinoJsonFacts {
                parametric_datetime: true,
                ..Default::default()
            },
        );
        let wide_columns = assert_exact_construction(&wide_schema);
        let expected_fields = (0..32)
            .map(|index| {
                format!("\"quoted\"\"中{index}\" map(varchar, timestamp(9) with time zone)")
            })
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(wide_columns[0].type_name, format!("row({expected_fields})"));
        assert!(columns_backing_bytes(&wide_columns).unwrap() < wide_schema.backing_bytes());
    }

    #[test]
    fn bounded_string_writer_refuses_growth_and_resource_failures_stay_typed() {
        let mut writer = ExactName {
            value: exact_string(1).unwrap(),
            limit: 1,
        };
        writer.write_str("x").unwrap();
        assert!(writer.write_str("中").is_err());
        assert_eq!(writer.value, "x");
        assert_eq!(writer.value.capacity(), 1);
        for error in [
            ColumnsError::BackingSizeOverflow,
            ColumnsError::BackingLimitExceeded,
            ColumnsError::AllocationFailed,
        ] {
            assert!(error.is_backing_failure());
        }
        for error in [
            ColumnsError::WrongPresentationFamily,
            ColumnsError::UnsupportedFrozenType,
        ] {
            assert!(!error.is_backing_failure());
        }
    }
}
