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

//! Private closed COW syntax construction. No parser or SQL text growth.
//! Signed schema/type declarations and original caller control are required.
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_parser::{Span, ast::*};
use std::collections::TryReserveError;
use std::fmt::{self, Write};

pub(super) const S: Span = Span::new(0, 0);
#[derive(Debug)]
pub(super) enum BuildError {
    ResourceExhausted,
    Allocation(TryReserveError),
    Original(novarocks_spi::connector::OriginalResultCheckError),
    Compile(novarocks_type_contract::CompileControlError),
    Constant(novarocks_functions::ConstantError),
    Kernel(novarocks_functions::KernelFailure),
    ArrayProjection(novarocks_functions::array_projection::ArrayProjectionFailure),
    Layout(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    ValueType(novarocks_type_contract::ExecutionValueTypeError),
    InvalidSource(&'static str),
    OriginalSemantic(String),
}
impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceExhausted | Self::Allocation(_) => {
                f.write_str("COW AST construction resource exhausted")
            }
            Self::Original(error) => error.fmt(f),
            Self::Compile(error) => error.fmt(f),
            Self::Constant(error) => error.fmt(f),
            Self::Kernel(error) => error.fmt(f),
            Self::ArrayProjection(error) => error.fmt(f),
            Self::Layout(error) => write!(f, "COW construction layout: {error:?}"),
            Self::ValueType(error) => error.fmt(f),
            Self::InvalidSource(reason) => f.write_str(reason),
            Self::OriginalSemantic(reason) => f.write_str(reason),
        }
    }
}
impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Allocation(error) => Some(error),
            Self::Original(error) => Some(error),
            Self::Compile(error) => Some(error),
            Self::Constant(error) => Some(error),
            Self::Kernel(error) => Some(error),
            Self::ArrayProjection(error) => Some(error),
            Self::ValueType(error) => Some(error),
            _ => None,
        }
    }
}
impl From<String> for BuildError {
    fn from(value: String) -> Self {
        Self::OriginalSemantic(value)
    }
}
impl From<novarocks_functions::array_projection::ArrayProjectionFailure> for BuildError {
    fn from(error: novarocks_functions::array_projection::ArrayProjectionFailure) -> Self {
        Self::ArrayProjection(error)
    }
}
impl From<novarocks_functions::KernelFailure> for BuildError {
    fn from(error: novarocks_functions::KernelFailure) -> Self {
        Self::Kernel(error)
    }
}
impl From<novarocks_type_contract::CompileControlError> for BuildError {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Compile(error)
    }
}
impl From<novarocks_type_contract::ExecutionValueTypeError> for BuildError {
    fn from(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::ValueType(error)
    }
}
pub(super) type Result<T> = std::result::Result<T, BuildError>;

// These are allocation requests, not new credits or a second capacity budget.
// The actual constructor validates and reserves its exact slot count.
pub(super) fn exact_vec<T>(length: usize) -> Result<Vec<T>> {
    let mut out = Vec::new();
    out.try_reserve_exact(length)
        .map_err(BuildError::Allocation)?;
    if std::mem::size_of::<T>() != 0 && out.capacity() != length {
        return Err(BuildError::ResourceExhausted);
    }
    Ok(out)
}
fn exact_string(bytes: usize) -> Result<String> {
    let mut out = String::new();
    out.try_reserve_exact(bytes)
        .map_err(BuildError::Allocation)?;
    if out.capacity() != bytes {
        return Err(BuildError::ResourceExhausted);
    }
    Ok(out)
}
fn copy_string(value: &str) -> Result<String> {
    let mut out = exact_string(value.len())?;
    out.push_str(value);
    Ok(out)
}
#[derive(Default)]
struct Count {
    bytes: usize,
}
impl Write for Count {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.bytes = self.bytes.checked_add(value.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}
fn formatted(args: fmt::Arguments<'_>) -> Result<String> {
    let mut count = Count::default();
    count
        .write_fmt(args)
        .map_err(|_| BuildError::ResourceExhausted)?;
    let mut out = exact_string(count.bytes)?;
    out.write_fmt(args)
        .map_err(|_| BuildError::ResourceExhausted)?;
    if out.len() != count.bytes {
        return Err(BuildError::InvalidSource("COW numeric format changed"));
    }
    Ok(out)
}

// Chrono DelayedFormat::Display creates an internal String first. The locked
// public write_to API writes directly into our counted/exact destinations.
fn date_formatted(
    value: chrono::format::DelayedFormat<chrono::format::StrftimeItems<'_>>,
) -> Result<String> {
    let mut count = Count::default();
    value
        .write_to(&mut count)
        .map_err(|_| BuildError::ResourceExhausted)?;
    let mut out = exact_string(count.bytes)?;
    value
        .write_to(&mut out)
        .map_err(|_| BuildError::ResourceExhausted)?;
    if out.len() != count.bytes {
        return Err(BuildError::InvalidSource("COW date format changed"));
    }
    Ok(out)
}

pub(super) fn value_alias(ordinal: usize) -> Result<String> {
    formatted(format_args!("__nr_v_{ordinal}"))
}
pub(super) fn ident(value: &str, quoted: bool) -> Result<Ident> {
    Ok(Ident {
        value: copy_string(value)?,
        quoted,
        quote_style: if quoted { Some('`') } else { None },
        span: S,
    })
}
fn name(value: &str) -> Result<ObjectName> {
    let mut parts = exact_vec(1)?;
    parts.push(ident(value, false)?);
    Ok(ObjectName { parts, span: S })
}
pub(super) fn literal(kind: LiteralKind) -> Expr {
    Expr::Literal(Literal { kind, span: S })
}
pub(super) fn bool_value(value: bool) -> Expr {
    literal(LiteralKind::Boolean(value))
}
pub(super) fn null() -> Expr {
    literal(LiteralKind::Null)
}

// Numbers preserve the old Display spelling, including Float32 promoted to
// f64 and unary-minus/-0 grammar. Positive payload has exact backing length.
pub(super) fn number(value: impl fmt::Display) -> Result<Expr> {
    struct NumberCount {
        bytes: usize,
        first: bool,
        negative: bool,
    }
    impl Write for NumberCount {
        fn write_str(&mut self, s: &str) -> fmt::Result {
            if !self.first && !s.is_empty() {
                self.negative = s.starts_with('-');
                self.first = true;
            }
            self.bytes = self.bytes.checked_add(s.len()).ok_or(fmt::Error)?;
            Ok(())
        }
    }
    struct Digits<'a> {
        out: &'a mut String,
        skip_first_minus: bool,
    }
    impl Write for Digits<'_> {
        fn write_str(&mut self, s: &str) -> fmt::Result {
            if self.skip_first_minus && !s.is_empty() {
                if let Some(rest) = s.strip_prefix('-') {
                    self.out.push_str(rest);
                } else {
                    return Err(fmt::Error);
                }
                self.skip_first_minus = false;
            } else {
                self.out.push_str(s);
            }
            Ok(())
        }
    }
    let mut count = NumberCount {
        bytes: 0,
        first: false,
        negative: false,
    };
    write!(&mut count, "{value}").map_err(|_| BuildError::ResourceExhausted)?;
    let length = count
        .bytes
        .checked_sub(usize::from(count.negative))
        .ok_or(BuildError::ResourceExhausted)?;
    let mut out = exact_string(length)?;
    write!(
        &mut Digits {
            out: &mut out,
            skip_first_minus: count.negative
        },
        "{value}"
    )
    .map_err(|_| BuildError::ResourceExhausted)?;
    if out.len() != length {
        return Err(BuildError::InvalidSource("COW number format changed"));
    }
    let payload = literal(LiteralKind::Number(out));
    Ok(if count.negative {
        Expr::Unary(UnaryExpr {
            operator: UnaryOperator::Minus,
            expression: Box::new(payload),
            span: S,
        })
    } else {
        payload
    })
}
fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref()
        .ok_or(BuildError::InvalidSource("COW Arrow downcast differs"))
}
fn binary_value(bytes: &[u8]) -> Result<Expr> {
    let length = bytes
        .len()
        .checked_mul(2)
        .ok_or(BuildError::ResourceExhausted)?;
    let mut out = exact_string(length)?;
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in bytes {
        out.push(char::from(HEX[(byte >> 4) as usize]));
        out.push(char::from(HEX[(byte & 15) as usize]));
    }
    Ok(literal(LiteralKind::HexString(out)))
}
fn function(name_value: &str, arguments: Vec<Expr>) -> Result<Expr> {
    Ok(Expr::FunctionCall(FunctionCall {
        name: name(name_value)?,
        arguments,
        quantifier: FunctionQuantifier::None,
        order_by: Vec::new(),
        separator: None,
        filter: None,
        null_treatment: None,
        over: None,
        substring_from_syntax: false,
        span: S,
    }))
}

// Validate the actual borrowed row and preserve its original caller control
// through nested construction. This creates no object-graph resource allowance.
pub(super) fn checked_value(
    array: &dyn Array,
    row: usize,
    original_check: &impl Fn() -> Result<()>,
) -> Result<Expr> {
    if row >= array.len() {
        return Err(BuildError::InvalidSource("COW value row is out of bounds"));
    }
    original_check()?;
    let mut control = ConstructionControl {
        visits: 0,
        check: original_check,
    };
    value_after_preflight(array, row, &mut control)
}
struct ConstructionControl<'a> {
    visits: u64,
    check: &'a dyn Fn() -> Result<()>,
}
impl ConstructionControl<'_> {
    fn enter(&mut self) -> Result<()> {
        self.visits = self
            .visits
            .checked_add(1)
            .ok_or(BuildError::ResourceExhausted)?;
        if self.visits.is_multiple_of(256) {
            (self.check)()?;
        }
        Ok(())
    }
}
fn value_after_preflight(
    array: &dyn Array,
    row: usize,
    control: &mut ConstructionControl<'_>,
) -> Result<Expr> {
    control.enter()?;
    if row >= array.len() {
        return Err(BuildError::InvalidSource("COW row index out of bounds"));
    }
    if array.is_null(row) {
        return Ok(null());
    }
    match array.data_type() {
        DataType::Boolean => Ok(bool_value(downcast::<BooleanArray>(array)?.value(row))),
        DataType::Int8 => number(i64::from(downcast::<Int8Array>(array)?.value(row))),
        DataType::Int16 => number(i64::from(downcast::<Int16Array>(array)?.value(row))),
        DataType::Int32 => number(i64::from(downcast::<Int32Array>(array)?.value(row))),
        DataType::Int64 => number(downcast::<Int64Array>(array)?.value(row)),
        DataType::Float32 => finite_float(f64::from(downcast::<Float32Array>(array)?.value(row))),
        DataType::Float64 => finite_float(downcast::<Float64Array>(array)?.value(row)),
        DataType::Decimal128(_, scale) => {
            let value = downcast::<Decimal128Array>(array)?.value(row);
            if *scale == 0 {
                let Ok(value64) = i64::try_from(value) else {
                    return Err(BuildError::OriginalSemantic(formatted(format_args!(
                        "decimal value {value} is out of range for INT64"
                    ))?));
                };
                return number(value64);
            }
            let Ok(scale_u32) = u32::try_from(*scale) else {
                return Err(BuildError::OriginalSemantic(formatted(format_args!(
                    "unsupported decimal scale: {scale}"
                ))?));
            };
            let Some(factor) = 10_u128.checked_pow(scale_u32) else {
                return Err(BuildError::OriginalSemantic(formatted(format_args!(
                    "unsupported decimal scale: {scale}"
                ))?));
            };
            let abs = value.unsigned_abs();
            Ok(literal(LiteralKind::String(formatted(format_args!(
                "{}{}.{:0width$}",
                if value.is_negative() { "-" } else { "" },
                abs / factor,
                abs % factor,
                width = scale_u32 as usize
            ))?)))
        }
        DataType::Utf8 => Ok(literal(LiteralKind::String(copy_string(
            downcast::<StringArray>(array)?.value(row),
        )?))),
        DataType::Binary => binary_value(downcast::<BinaryArray>(array)?.value(row)),
        DataType::LargeBinary => binary_value(downcast::<LargeBinaryArray>(array)?.value(row)),
        DataType::Date32 => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("fixed epoch");
            let date = epoch
                + chrono::Duration::days(i64::from(downcast::<Date32Array>(array)?.value(row)));
            Ok(literal(LiteralKind::String(date_formatted(
                date.format("%Y-%m-%d"),
            )?)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = downcast::<TimestampMicrosecondArray>(array)?.value(row);
            let date = chrono::DateTime::from_timestamp_micros(micros)
                .expect("timestamp micros should be valid")
                .naive_utc();
            Ok(literal(LiteralKind::String(date_formatted(
                date.format("%Y-%m-%d %H:%M:%S"),
            )?)))
        }
        DataType::List(_) => {
            let list = downcast::<ListArray>(array)?;
            let offsets = list.value_offsets();
            let begin = usize::try_from(offsets[row])
                .map_err(|_| BuildError::InvalidSource("COW list offset"))?;
            let end = usize::try_from(offsets[row + 1])
                .map_err(|_| BuildError::InvalidSource("COW list offset"))?;
            if end < begin || end > list.values().len() {
                return Err(BuildError::InvalidSource("COW list offsets"));
            }
            let mut elements = exact_vec(end - begin)?;
            for i in begin..end {
                elements.push(value_after_preflight(list.values().as_ref(), i, control)?);
            }
            Ok(Expr::Array(ArrayExpr {
                element_type: None,
                elements,
                span: S,
            }))
        }
        DataType::Struct(_) => {
            let array = downcast::<StructArray>(array)?;
            let mut arguments = exact_vec(array.num_columns())?;
            for child in array.columns() {
                arguments.push(value_after_preflight(child.as_ref(), row, control)?);
            }
            function("row", arguments)
        }
        DataType::Map(_, _) => {
            let map = downcast::<MapArray>(array)?;
            let entries = map.entries();
            if entries.num_columns() != 2 {
                return Err(BuildError::OriginalSemantic(formatted(format_args!(
                    "map entries must contain 2 fields, got {}",
                    entries.num_columns()
                ))?));
            }
            let offsets = map.value_offsets();
            let begin = usize::try_from(offsets[row])
                .map_err(|_| BuildError::InvalidSource("COW map offset"))?;
            let end = usize::try_from(offsets[row + 1])
                .map_err(|_| BuildError::InvalidSource("COW map offset"))?;
            if end < begin || end > entries.len() {
                return Err(BuildError::InvalidSource("COW map offsets"));
            }
            let mut arguments = exact_vec(
                (end - begin)
                    .checked_mul(2)
                    .ok_or(BuildError::ResourceExhausted)?,
            )?;
            for i in begin..end {
                for child in entries.columns() {
                    arguments.push(value_after_preflight(child.as_ref(), i, control)?);
                }
            }
            function("map", arguments)
        }
        other => Err(BuildError::OriginalSemantic(formatted(format_args!(
            "literal_from_batch does not support column type {other:?}"
        ))?)),
    }
}
fn finite_float(value: f64) -> Result<Expr> {
    if !value.is_finite() {
        return Err(BuildError::OriginalSemantic(formatted(format_args!(
            "non-finite floating literal is not supported: {value}"
        ))?));
    }
    number(value)
}

fn type_failure(error: super::cow_type_syntax::Failure) -> BuildError {
    use super::cow_type_syntax::Failure as F;
    match error {
        F::InvalidSource(reason) => BuildError::InvalidSource(reason),
        F::OriginalSemantic(reason) => BuildError::OriginalSemantic(reason.into()),
    }
}
pub(super) fn type_name(
    value: &novarocks_type_contract::FunctionValueType,
    check: &impl Fn() -> Result<()>,
) -> Result<TypeName> {
    type_name_node(super::cow_type_syntax::Node::root(value), check)
}
fn type_name_node(
    node: super::cow_type_syntax::Node<'_>,
    check: &impl Fn() -> Result<()>,
) -> Result<TypeName> {
    use super::cow_type_syntax::{Node, Spelling};
    check()?;
    let spelling = node.spelling().map_err(type_failure)?;
    let (word, count) = match &spelling {
        Spelling::Leaf(word) => (*word, 0),
        Spelling::Decimal { .. } => ("DECIMAL", 2),
        Spelling::Array(_) => ("ARRAY", 1),
        Spelling::Map(_, _) => ("MAP", 2),
        Spelling::Struct(fields, _) => ("STRUCT", fields.len()),
    };
    let mut arguments = exact_vec(count)?;
    match spelling {
        Spelling::Leaf(_) => {}
        Spelling::Decimal { precision, scale } => {
            for parameter in [precision, scale] {
                arguments.push(TypeNameArgument::Literal(Literal {
                    kind: LiteralKind::Number(formatted(format_args!("{parameter}"))?),
                    span: S,
                }));
            }
        }
        Spelling::Array(item) => {
            arguments.push(TypeNameArgument::Type(type_name_node(item, check)?))
        }
        Spelling::Map(key, value) => {
            arguments.push(TypeNameArgument::Type(type_name_node(key, check)?));
            arguments.push(TypeNameArgument::Type(type_name_node(value, check)?));
        }
        Spelling::Struct(fields, layout) => {
            for (field, layout) in fields.iter().zip(layout) {
                check()?;
                arguments.push(TypeNameArgument::Field(StructField {
                    name: ident(field.name(), true)?,
                    data_type: type_name_node(
                        Node {
                            value: field.value_type(),
                            layout: layout.data_type(),
                        },
                        check,
                    )?,
                    span: S,
                }));
            }
        }
    }
    let mut spaces = exact_vec(count.saturating_sub(1))?;
    spaces.resize(count.saturating_sub(1), true);
    Ok(TypeName {
        name: name(word)?,
        arguments,
        argument_separator_spaces: spaces,
        span: S,
    })
}
pub(super) fn cast(
    expr: Expr,
    value: &novarocks_type_contract::FunctionValueType,
    check: &impl Fn() -> Result<()>,
) -> Result<Expr> {
    // Build the validated spelling before publishing its owning Cast wrapper.
    let data_type = type_name(value, check)?;
    check()?;
    Ok(Expr::Cast(CastExpr {
        expr: Box::new(expr),
        data_type,
        kind: CastKind::Cast,
        format: None,
        span: S,
    }))
}
pub(super) fn cast_effect(expr: Expr, check: &impl Fn() -> Result<()>) -> Result<Expr> {
    // The sealed EffectField constructor owns this required Int8 protocol fact.
    check()?;
    let data_type = TypeName {
        name: name("TINYINT")?,
        arguments: Vec::new(),
        argument_separator_spaces: Vec::new(),
        span: S,
    };
    check()?;
    Ok(Expr::Cast(CastExpr {
        expr: Box::new(expr),
        data_type,
        kind: CastKind::Cast,
        format: None,
        span: S,
    }))
}
pub(super) fn column(alias: &str, field: &str) -> Result<Expr> {
    let mut parts = exact_vec(2)?;
    parts.push(ident(alias, true)?);
    parts.push(ident(field, true)?);
    Ok(Expr::CompoundIdentifier(CompoundIdentifier {
        parts,
        span: S,
    }))
}
pub(super) fn binary(left: Expr, operator: BinaryOperator, right: Expr) -> Expr {
    Expr::Binary(BinaryExpr {
        left: Box::new(left),
        operator,
        right: Box::new(right),
        span: S,
    })
}
pub(super) fn is_null(expr: Expr, not: bool) -> Expr {
    Expr::IsPredicate(IsPredicateExpr {
        expr: Box::new(expr),
        predicate: if not {
            IsPredicate::NotNull
        } else {
            IsPredicate::Null
        },
        span: S,
    })
}
pub(super) fn case(condition: Expr, result: Expr, otherwise: Expr) -> Result<Expr> {
    let mut conditions = exact_vec(1)?;
    conditions.push(condition);
    let mut results = exact_vec(1)?;
    results.push(result);
    Ok(Expr::Case(CaseExpr {
        operand: None,
        conditions,
        results,
        else_result: Some(Box::new(otherwise)),
        span: S,
    }))
}
pub(super) fn item(expr: Expr, alias: &str) -> Result<SelectItem> {
    Ok(SelectItem::ExprWithAlias {
        expr,
        alias: ident(alias, true)?,
        explicit_as: true,
        span: S,
    })
}
pub(super) fn values(
    rows: Vec<Vec<Expr>>,
    alias: &str,
    columns: Vec<Ident>,
) -> Result<TableFactor> {
    if rows.is_empty() || columns.is_empty() || rows.iter().any(|row| row.len() != columns.len()) {
        return Err(BuildError::OriginalSemantic(
            "COW VALUES relation is empty or has inconsistent width".into(),
        ));
    }
    Ok(TableFactor::Derived {
        lateral: false,
        subquery: Box::new(query(SetExpr::Values(Values {
            rows,
            explicit_row: false,
            span: S,
        }))),
        hints: Vec::new(),
        alias: Some(TableAlias {
            name: ident(alias, true)?,
            columns,
            explicit_as: true,
            span: S,
        }),
        span: S,
    })
}
/// An explicitly owned materialized source occupies this structural edge.
/// No SQL literal or CAST is authored here; the statement input supplies the
/// checked constant rows. Ordinary SQL VALUES still uses values() above.
pub(super) fn materialized_values(alias: &str, columns: Vec<Ident>) -> Result<TableFactor> {
    if columns.is_empty() {
        return Err(BuildError::InvalidSource(
            "COW materialized source has no columns",
        ));
    }
    Ok(TableFactor::Derived {
        lateral: false,
        subquery: Box::new(query(SetExpr::Values(Values {
            rows: Vec::new(),
            explicit_row: false,
            span: S,
        }))),
        hints: Vec::new(),
        alias: Some(TableAlias {
            name: ident(alias, true)?,
            columns,
            explicit_as: true,
            span: S,
        }),
        span: S,
    })
}
pub(super) fn table(
    catalog: &str,
    namespace: &str,
    table: &str,
    alias: &str,
) -> Result<TableFactor> {
    let mut parts = exact_vec(3)?;
    parts.push(ident(catalog, true)?);
    parts.push(ident(namespace, true)?);
    parts.push(ident(table, true)?);
    Ok(TableFactor::Table {
        name: ObjectName { parts, span: S },
        metadata: None,
        alias: Some(TableAlias {
            name: ident(alias, true)?,
            columns: Vec::new(),
            explicit_as: true,
            span: S,
        }),
        version: None,
        hints: Vec::new(),
        span: S,
    })
}
fn query(body: SetExpr) -> Query {
    Query {
        with: None,
        body: Box::new(body),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        limit_comma_offset: false,
        fetch: None,
        span: S,
    }
}
pub(super) fn select(
    projection: Vec<SelectItem>,
    from: TableFactor,
    join: Option<(TableFactor, Expr)>,
    selection: Option<Expr>,
) -> Result<Query> {
    let mut joins = exact_vec(usize::from(join.is_some()))?;
    if let Some((relation, expr)) = join {
        joins.push(Join {
            relation,
            operator: JoinOperator::LeftOuter,
            constraint: JoinConstraint::On(expr),
            span: S,
        });
    }
    let mut from_list = exact_vec(1)?;
    from_list.push(TableWithJoins {
        relation: from,
        joins,
        span: S,
    });
    Ok(query(SetExpr::Select(Box::new(Select {
        hints: Vec::new(),
        quantifier: SelectQuantifier::None,
        projection,
        from: from_list,
        selection,
        group_by: GroupBy::None,
        having: None,
        qualify: None,
        windows: Vec::new(),
        span: S,
    }))))
}

#[cfg(test)]
#[path = "cow_closed_ast_tests.rs"]
mod tests;
