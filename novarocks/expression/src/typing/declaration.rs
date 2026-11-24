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

use std::{fmt, sync::Arc};

use arrow::datatypes::{DataType, Field};
use novarocks_parser::{Span, ast};
use novarocks_type_contract::{
    CharacterKind, CharacterType, CompileCheckpoints, CompileControlError, CompilePhase,
    FunctionValueType, MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES, PureCompileControl, ValueField,
    ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypeDeclarationProblem {
    UnknownType,
    InvalidParameters,
    InvalidDecimalParameters,
    Unsupported,
    Control,
}

#[derive(Debug)]
pub struct TypeDeclarationError {
    pub span: Span,
    pub problem: TypeDeclarationProblem,
    pub control: Option<CompileControlError>,
    message: String,
}

impl TypeDeclarationError {
    fn new(problem: TypeDeclarationProblem, span: Span, message: impl Into<String>) -> Self {
        Self {
            span,
            problem,
            message: message.into(),
            control: None,
        }
    }

    fn control(span: Span, error: CompileControlError) -> Self {
        let mut failure = Self::new(TypeDeclarationProblem::Control, span, error.to_string());
        failure.control = Some(error);
        failure
    }
}

impl fmt::Display for TypeDeclarationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TypeDeclarationError {}

/// Resolve the same parser declaration for CAST, typed literals, and DDL.
pub fn resolve_trino_type(
    syntax: &ast::TypeName,
    nullable: bool,
    control: &dyn PureCompileControl,
) -> Result<FunctionValueType, TypeDeclarationError> {
    let control_error = |error| TypeDeclarationError::control(syntax.span, error);
    let mut work =
        CompileCheckpoints::try_new(control, CompilePhase::Validate).map_err(control_error)?;
    let result = (|| {
        let (value, carrier) = resolve_parts(syntax, nullable, 1, &mut 0, &mut work)?;
        let layout = ValueLayout::try_from_normalized_arrow_observed(
            carrier,
            ValueTypeStorageLifetime::pure(),
            |units| {
                for _ in 0..units {
                    work.step().map_err(control_error)?;
                }
                Ok::<(), TypeDeclarationError>(())
            },
        )?;
        FunctionValueType::try_from_value(value, layout)
            .map_err(|error| invalid(syntax.span, error.to_string()))
    })();
    work.finish_result(result, control_error)
}

impl From<novarocks_type_contract::ExecutionValueTypeError> for TypeDeclarationError {
    fn from(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        invalid(novarocks_parser::SYNTHETIC, error.to_string())
    }
}

fn resolve_parts(
    syntax: &ast::TypeName,
    nullable: bool,
    depth: usize,
    nodes: &mut usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ValueTypeRef, DataType), TypeDeclarationError> {
    work.step()
        .map_err(|error| TypeDeclarationError::control(syntax.span, error))?;
    *nodes += 1;
    if depth > MAX_VALUE_TYPE_DEPTH || *nodes > MAX_VALUE_TYPE_NODES {
        return Err(invalid(
            syntax.span,
            "SQL type exceeds the recursive type profile",
        ));
    }
    let storage = ValueTypeStorageLifetime::pure();
    let (kind, carrier) = match &syntax.kind {
        ast::TypeKind::Generic(generic) => {
            let name = generic.name.value.to_ascii_lowercase();
            if matches!(name.as_str(), "array" | "map") {
                let args = generic.arguments.as_slice();
                match (name.as_str(), args) {
                    ("array", [ast::DataTypeParameter::Type(item)]) => {
                        let (item, layout) =
                            resolve_parts(&item.data_type, true, depth + 1, nodes, work)?;
                        (
                            ValueKind::List(item),
                            DataType::List(Arc::new(Field::new("item", layout, true))),
                        )
                    }
                    (
                        "map",
                        [
                            ast::DataTypeParameter::Type(key),
                            ast::DataTypeParameter::Type(value),
                        ],
                    ) => {
                        let (key, key_layout) =
                            resolve_parts(&key.data_type, false, depth + 1, nodes, work)?;
                        let (value, value_layout) =
                            resolve_parts(&value.data_type, true, depth + 1, nodes, work)?;
                        let entries = DataType::Struct(
                            [
                                Arc::new(Field::new("key", key_layout, false)),
                                Arc::new(Field::new("value", value_layout, true)),
                            ]
                            .into(),
                        );
                        (
                            ValueKind::Map { key, value },
                            DataType::Map(Arc::new(Field::new("entries", entries, false)), false),
                        )
                    }
                    _ => {
                        return Err(invalid(
                            syntax.span,
                            format!("Invalid {name} type parameters"),
                        ));
                    }
                }
            } else {
                resolve_leaf(&name, &generic.arguments, syntax.span)?
            }
        }
        ast::TypeKind::Row(row) => {
            let mut values = Vec::with_capacity(row.fields.len());
            let mut fields = Vec::with_capacity(row.fields.len());
            for field in row.fields.iter() {
                let (value, layout) =
                    resolve_parts(&field.data_type, true, depth + 1, nodes, work)?;
                let name = match &field.name {
                    Some(name) => {
                        for _ in name.value.bytes() {
                            work.step()
                                .map_err(|error| TypeDeclarationError::control(name.span, error))?;
                        }
                        name.value.clone()
                    }
                    None => String::new(),
                };
                values.push(ValueField::new(name.as_str(), value, storage.clone()));
                fields.push(Arc::new(Field::new(name, layout, true)));
            }
            (
                ValueKind::Struct(values.into()),
                DataType::Struct(fields.into()),
            )
        }
        ast::TypeKind::DateTime(_) | ast::TypeKind::Interval(_) => {
            let temporal = super::declared_temporal_type(syntax)
                .map_err(|error| invalid(syntax.span, error.to_string()))?
                .expect("temporal syntax provides its exact declaration");
            (ValueKind::Temporal(temporal), temporal.carrier())
        }
        ast::TypeKind::ExplicitArray(_) => {
            return Err(unsupported(
                syntax.span,
                "explicit ARRAY lengths are not supported",
            ));
        }
    };
    let value = ValueTypeRef::try_new_observed(kind, nullable, storage, |units| {
        for _ in 0..units {
            work.step()
                .map_err(|error| TypeDeclarationError::control(syntax.span, error))?;
        }
        Ok::<(), TypeDeclarationError>(())
    })?;
    Ok((value, carrier))
}

/// Report a retired declaration in persisted SQL using the same AST words
/// accepted by the Trino declaration resolver.
pub fn find_retired_type_in_query(query: &ast::Query) -> Option<(Span, String)> {
    struct RetiredType(Option<(Span, String)>);
    impl ast::Visit for RetiredType {
        fn visit_type_name(&mut self, syntax: &ast::TypeName) {
            if self.0.is_some() {
                return;
            }
            if let ast::TypeKind::Generic(generic) = &syntax.kind {
                let name = generic.name.value.to_ascii_lowercase();
                if is_retired_type_name(&name)
                    || (name == "decimal"
                        && matches!(generic.arguments.first(), Some(ast::DataTypeParameter::Numeric(p)) if p.parsed_value > 38))
                {
                    self.0 = Some((syntax.span, generic.name.value.clone()));
                    return;
                }
            }
            ast::walk_type_name(self, syntax);
        }

        fn visit_expr(&mut self, expr: &ast::Expr) {
            if self.0.is_some() {
                return;
            }
            if let ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::Generic(literal),
                span,
            }) = expr
                && is_retired_type_name(&literal.type_name.to_ascii_lowercase())
            {
                self.0 = Some((*span, literal.type_name.clone()));
                return;
            }
            ast::walk_expr(self, expr);
        }
    }
    let mut visitor = RetiredType(None);
    ast::walk_query(&mut visitor, query);
    visitor.0
}

fn is_retired_type_name(name: &str) -> bool {
    matches!(
        name,
        "largeint"
            | "string"
            | "float"
            | "datetime"
            | "binary"
            | "dec"
            | "numeric"
            | "bool"
            | "character"
            | "decimal256"
    )
}

pub(super) fn supports_literal_type_name(name: &str) -> bool {
    validate_typed_string_type_name(name, novarocks_parser::SYNTHETIC).is_ok()
}

/// Classify a typed-string name before discovering catalog references.
pub fn validate_typed_string_type_name(name: &str, span: Span) -> Result<(), TypeDeclarationError> {
    resolve_leaf(&name.to_ascii_lowercase(), &[], span).map(|_| ())
}

fn resolve_leaf(
    name: &str,
    arguments: &[ast::DataTypeParameter],
    span: Span,
) -> Result<(ValueKind, DataType), TypeDeclarationError> {
    use DataType::*;
    let leaf = match name {
        "tinyint" => scalar(Int8),
        "smallint" => scalar(Int16),
        "int" | "integer" => scalar(Int32),
        "bigint" => scalar(Int64),
        "real" => scalar(Float32),
        "double" | "double precision" => scalar(Float64),
        "boolean" => scalar(Boolean),
        "date" => scalar(Date32),
        "time" | "timestamp" if arguments.is_empty() => {
            use novarocks_type_contract::{TemporalPrecision, TemporalType};
            let temporal = if name == "time" {
                TemporalType::Time(TemporalPrecision::DEFAULT)
            } else {
                TemporalType::Timestamp(TemporalPrecision::DEFAULT)
            };
            (ValueKind::Temporal(temporal), temporal.carrier())
        }
        "varbinary" => (ValueKind::Binary, Binary),
        "json" => (ValueKind::Nominal(ValueLogicalType::Json), Utf8),
        "uuid" => (
            ValueKind::Nominal(ValueLogicalType::Uuid),
            FixedSizeBinary(16),
        ),
        "varchar" | "char" => {
            let kind = if name == "varchar" {
                CharacterKind::Varchar
            } else {
                CharacterKind::Char
            };
            let length = match arguments {
                [] if kind == CharacterKind::Varchar => return Ok((ValueKind::String, Utf8)),
                [] => 1,
                [argument] => numeric_parameter(argument, span)?,
                _ => return Err(invalid(span, format!("Invalid {name} type parameters"))),
            };
            if kind == CharacterKind::Varchar
                && length == i64::from(CharacterType::MAX_VARCHAR_LENGTH) + 1
            {
                return Ok((ValueKind::String, Utf8));
            }
            let character = u32::try_from(length)
                .ok()
                .and_then(|length| CharacterType::new(kind, length));
            let bound = if kind == CharacterKind::Char {
                CharacterType::MAX_CHAR_LENGTH
            } else {
                CharacterType::MAX_VARCHAR_LENGTH + 1
            };
            return character
                .map(|value| (ValueKind::BoundedCharacter(value), Utf8))
                .ok_or_else(|| {
                    invalid(
                        span,
                        format!("{name} length {length} must be between 0 and {bound}"),
                    )
                });
        }
        "decimal" => {
            let (precision, scale) = match arguments {
                [] => (38, 0),
                [precision] => (numeric_parameter(precision, span)?, 0),
                [precision, scale] => (
                    numeric_parameter(precision, span)?,
                    numeric_parameter(scale, span)?,
                ),
                _ => return Err(invalid(span, "Invalid DECIMAL type parameters")),
            };
            let invalid = |message| {
                TypeDeclarationError::new(
                    TypeDeclarationProblem::InvalidDecimalParameters,
                    span,
                    message,
                )
            };
            if !(1..=38).contains(&precision) {
                return Err(invalid(format!(
                    "DECIMAL precision must be in range [1, 38]: {precision}"
                )));
            }
            if !(0..=precision).contains(&scale) {
                return Err(invalid(format!(
                    "DECIMAL scale must be in range [0, {precision}]: {scale}"
                )));
            }
            return Ok(scalar(Decimal128(precision as u8, scale as i8)));
        }
        "number" | "variant" | "ipaddress" | "hyperloglog" | "p4hyperloglog" | "qdigest"
        | "tdigest" => {
            return Err(unsupported(
                span,
                format!("SQL type {name} is not supported"),
            ));
        }
        _ => {
            return Err(TypeDeclarationError::new(
                TypeDeclarationProblem::UnknownType,
                span,
                format!("Unknown type: {name}"),
            ));
        }
    };
    if !arguments.is_empty() {
        return Err(invalid(
            span,
            format!("Type {name} does not accept parameters"),
        ));
    }
    Ok(leaf)
}

fn numeric_parameter(
    argument: &ast::DataTypeParameter,
    span: Span,
) -> Result<i64, TypeDeclarationError> {
    match argument {
        ast::DataTypeParameter::Numeric(number) => Ok(number.parsed_value),
        _ => Err(invalid(span, "Type parameter must be an integer")),
    }
}

fn scalar(carrier: DataType) -> (ValueKind, DataType) {
    (ValueKind::Scalar(carrier.clone()), carrier)
}

fn invalid(span: Span, message: impl Into<String>) -> TypeDeclarationError {
    TypeDeclarationError::new(TypeDeclarationProblem::InvalidParameters, span, message)
}

fn unsupported(span: Span, message: impl Into<String>) -> TypeDeclarationError {
    TypeDeclarationError::new(TypeDeclarationProblem::Unsupported, span, message)
}
