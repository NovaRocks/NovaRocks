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

//! Syntax-only construction for queries authored by application owners.
//!
//! These helpers retain admitted user subtrees and build the same syntax shapes
//! as the parser. They perform no name resolution, type conversion, or SQL text
//! parsing. A wholly generated query envelope has no user source location;
//! expression wrappers retain the source location of their input expressions.

use std::{error::Error, fmt};

use crate::{ParseError, ParserError, SYNTHETIC, Span, ast::*, parser};

/// A construction defect in an application-authored syntax tree.
///
/// Callers classify these facts at their own boundary; they are not parser
/// failures for a client statement and do not add a user error vocabulary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BuildError {
    EmptyObjectName,
    EmptyProjection,
    EmptyValues,
    EmptyValuesRow,
    EmptyCase,
    EmptyTuple,
    EmptyUnion,
    EmptyWildcardPrefix,
    TypeArgumentSeparators { arguments: usize, separators: usize },
    Number(ParserError),
}

impl fmt::Display for BuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyObjectName => formatter.write_str("SQL object name must not be empty"),
            Self::EmptyProjection => formatter.write_str("SELECT projection must not be empty"),
            Self::EmptyValues => formatter.write_str("VALUES must contain at least one row"),
            Self::EmptyValuesRow => formatter.write_str("VALUES row must not be empty"),
            Self::EmptyCase => formatter.write_str("CASE must contain at least one branch"),
            Self::EmptyTuple => formatter.write_str("tuple must contain at least one expression"),
            Self::EmptyUnion => formatter.write_str("UNION ALL must contain at least one operand"),
            Self::EmptyWildcardPrefix => {
                formatter.write_str("qualified wildcard must contain a name prefix")
            }
            Self::TypeArgumentSeparators {
                arguments,
                separators,
            } => write!(
                formatter,
                "type with {arguments} arguments has {separators} separator markers"
            ),
            Self::Number(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl Error for BuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Number(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ParseError> for BuildError {
    fn from(error: ParseError) -> Self {
        Self::Number(ParserError::Parse(error))
    }
}

/// Builds an identifier with no user source position.
pub fn ident(value: impl Into<String>, quote_style: Option<char>) -> Ident {
    ident_at(value, quote_style, SYNTHETIC)
}

/// Builds an identifier derived from a particular user name.
pub fn ident_at(value: impl Into<String>, quote_style: Option<char>, span: Span) -> Ident {
    Ident {
        value: value.into(),
        quoted: quote_style.is_some(),
        quote_style,
        span,
    }
}

pub fn object_name(parts: Vec<Ident>) -> Result<ObjectName, BuildError> {
    if parts.is_empty() {
        return Err(BuildError::EmptyObjectName);
    }
    let span = cover(parts.iter().map(|part| part.span));
    Ok(ObjectName { parts, span })
}

pub fn identifier(name: Ident) -> Expr {
    Expr::Identifier(name)
}

pub fn column(qualifier: Ident, name: Ident) -> Expr {
    Expr::CompoundIdentifier(CompoundIdentifier {
        span: qualifier.span.cover(name.span),
        parts: vec![qualifier, name],
    })
}

/// Builds an admitted or program-authored numeric spelling without reparsing SQL.
///
/// The caller owns the spelling's lexical validity. Integer-width validation is
/// shared with the parser. Only a negative integer is folded into its literal;
/// signed fractional/exponent spellings retain the parser's unary syntax.
pub fn number(text: impl Into<String>) -> Result<Expr, BuildError> {
    let text = text.into();
    if let Some(magnitude) = text.strip_prefix('-') {
        if parser::is_integer_number(magnitude) {
            return Ok(literal(parser::number_literal_kind(text, SYNTHETIC)?));
        }
        return unary(
            UnaryOperator::Minus,
            literal(parser::number_literal_kind(
                magnitude.to_owned(),
                SYNTHETIC,
            )?),
        );
    }
    if let Some(magnitude) = text.strip_prefix('+') {
        return unary(
            UnaryOperator::Plus,
            literal(parser::number_literal_kind(
                magnitude.to_owned(),
                SYNTHETIC,
            )?),
        );
    }
    Ok(literal(parser::number_literal_kind(text, SYNTHETIC)?))
}

/// Builds an exactly representable integer without a fallible text conversion.
pub fn integer(value: i64) -> Expr {
    literal(LiteralKind::Number(value.to_string()))
}

pub fn string(value: impl Into<String>) -> Expr {
    literal(LiteralKind::String(value.into()))
}

/// The input is the admitted hexadecimal body, without a SQL delimiter.
pub fn hex_string(value: impl Into<String>) -> Expr {
    literal(LiteralKind::HexString(value.into()))
}

pub fn boolean(value: bool) -> Expr {
    literal(LiteralKind::Boolean(value))
}

pub fn null() -> Expr {
    literal(LiteralKind::Null)
}

fn literal(kind: LiteralKind) -> Expr {
    Expr::Literal(Literal {
        kind,
        span: SYNTHETIC,
    })
}

pub fn type_name(
    name: ObjectName,
    arguments: Vec<TypeNameArgument>,
    argument_separator_spaces: Vec<bool>,
) -> Result<TypeName, BuildError> {
    if argument_separator_spaces.len() != arguments.len().saturating_sub(1) {
        return Err(BuildError::TypeArgumentSeparators {
            arguments: arguments.len(),
            separators: argument_separator_spaces.len(),
        });
    }
    Ok(TypeName {
        name,
        arguments,
        argument_separator_spaces,
        span: SYNTHETIC,
    })
}

pub fn simple_type(name: impl Into<String>) -> TypeName {
    TypeName {
        name: ObjectName {
            parts: vec![ident(name, None)],
            span: SYNTHETIC,
        },
        arguments: Vec::new(),
        argument_separator_spaces: Vec::new(),
        span: SYNTHETIC,
    }
}

pub fn cast(expr: Expr, data_type: TypeName) -> Expr {
    let span = expr.span();
    Expr::Cast(CastExpr {
        expr: Box::new(expr),
        data_type,
        kind: CastKind::Cast,
        format: None,
        span,
    })
}

pub fn nested(expr: Expr) -> Expr {
    let span = expr.span();
    Expr::Nested(NestedExpr {
        expression: Box::new(expr),
        span,
    })
}

pub fn unary(operator: UnaryOperator, expr: Expr) -> Result<Expr, BuildError> {
    let span = expr.span();
    if operator == UnaryOperator::Minus
        && let Expr::Literal(Literal {
            kind: LiteralKind::Number(value),
            ..
        }) = &expr
        && !value.starts_with(['-', '+'])
        && parser::is_integer_number(value)
    {
        return Ok(Expr::Literal(Literal {
            kind: parser::number_literal_kind(format!("-{value}"), span)?,
            span,
        }));
    }
    Ok(Expr::Unary(UnaryExpr {
        operator,
        expression: Box::new(expr),
        span,
    }))
}

pub fn not(expr: Expr) -> Expr {
    let span = expr.span();
    Expr::Unary(UnaryExpr {
        operator: UnaryOperator::Not,
        expression: Box::new(expr),
        span,
    })
}

fn binary_node(left: Expr, operator: BinaryOperator, right: Expr) -> Expr {
    Expr::Binary(BinaryExpr {
        span: left.span().cover(right.span()),
        left: Box::new(left),
        operator,
        right: Box::new(right),
    })
}

pub fn binary(left: Expr, operator: BinaryOperator, right: Expr) -> Expr {
    parser::balance_boolean_chain(binary_node(left, operator, right))
}

/// An empty conjunction represents an absent predicate, not a guessed literal.
pub fn and_all(operands: Vec<Expr>) -> Option<Expr> {
    boolean_chain(operands, BinaryOperator::And)
}

/// An empty disjunction represents an absent predicate, not a guessed literal.
pub fn or_all(operands: Vec<Expr>) -> Option<Expr> {
    boolean_chain(operands, BinaryOperator::Or)
}

fn boolean_chain(operands: Vec<Expr>, operator: BinaryOperator) -> Option<Expr> {
    let mut operands = operands.into_iter();
    let first = operands.next()?;
    let expression = operands.fold(first, |left, right| binary_node(left, operator, right));
    Some(parser::balance_boolean_chain(expression))
}

pub fn is_null(expr: Expr, negated: bool) -> Expr {
    let span = expr.span();
    Expr::IsPredicate(IsPredicateExpr {
        expr: Box::new(expr),
        predicate: if negated {
            IsPredicate::NotNull
        } else {
            IsPredicate::Null
        },
        span,
    })
}

pub fn case_when(
    branches: Vec<(Expr, Expr)>,
    else_result: Option<Expr>,
) -> Result<Expr, BuildError> {
    if branches.is_empty() {
        return Err(BuildError::EmptyCase);
    }
    let span = cover(
        branches
            .iter()
            .flat_map(|(condition, result)| [condition.span(), result.span()])
            .chain(else_result.iter().map(Expr::span)),
    );
    let (conditions, results) = branches.into_iter().unzip();
    Ok(Expr::Case(CaseExpr {
        operand: None,
        conditions,
        results,
        else_result: else_result.map(Box::new),
        span,
    }))
}

pub fn function(name: ObjectName, arguments: Vec<Expr>) -> Expr {
    let span = name.span.cover(cover(arguments.iter().map(Expr::span)));
    Expr::FunctionCall(FunctionCall {
        name,
        arguments,
        quantifier: FunctionQuantifier::None,
        order_by: Vec::new(),
        separator: None,
        filter: None,
        null_treatment: None,
        over: None,
        substring_from_syntax: false,
        span,
    })
}

pub fn count_star() -> Expr {
    function(
        ObjectName {
            parts: vec![ident("COUNT", None)],
            span: SYNTHETIC,
        },
        vec![identifier(ident("*", None))],
    )
}

pub fn item(expr: Expr, alias: Option<Ident>, explicit_as: bool) -> SelectItem {
    match alias {
        Some(alias) => SelectItem::ExprWithAlias {
            span: expr.span().cover(alias.span),
            expr,
            alias,
            explicit_as,
        },
        None => SelectItem::UnnamedExpr(expr),
    }
}

pub fn wildcard() -> SelectItem {
    SelectItem::Wildcard {
        options: WildcardOptions::default(),
        span: SYNTHETIC,
    }
}

pub fn qualified_wildcard(prefix: Vec<Ident>) -> Result<SelectItem, BuildError> {
    if prefix.is_empty() {
        return Err(BuildError::EmptyWildcardPrefix);
    }
    Ok(SelectItem::QualifiedWildcard {
        span: cover(prefix.iter().map(|part| part.span)),
        prefix,
        options: WildcardOptions::default(),
    })
}

pub fn alias(name: Ident, columns: Vec<Ident>, explicit_as: bool) -> TableAlias {
    TableAlias {
        span: name
            .span
            .cover(cover(columns.iter().map(|column| column.span))),
        name,
        columns,
        explicit_as,
    }
}

pub fn table(
    name: ObjectName,
    version: Option<TableVersion>,
    alias: Option<TableAlias>,
) -> TableFactor {
    TableFactor::Table {
        name,
        metadata: None,
        alias,
        version,
        span: SYNTHETIC,
    }
}

pub fn derived(query: Query, alias: Option<TableAlias>) -> TableFactor {
    TableFactor::Derived {
        lateral: false,
        subquery: Box::new(query),
        alias,
        span: SYNTHETIC,
    }
}

pub fn relation(relation: TableFactor) -> TableWithJoins {
    TableWithJoins {
        relation,
        joins: Vec::new(),
        span: SYNTHETIC,
    }
}

pub fn join(
    mut left: TableWithJoins,
    operator: JoinOperator,
    right: TableFactor,
    constraint: JoinConstraint,
) -> TableWithJoins {
    left.joins.push(Join {
        relation: right,
        operator,
        constraint,
        span: SYNTHETIC,
    });
    left
}

pub fn values(rows: Vec<Vec<Expr>>) -> Result<SetExpr, BuildError> {
    if rows.is_empty() {
        return Err(BuildError::EmptyValues);
    }
    if rows.iter().any(Vec::is_empty) {
        return Err(BuildError::EmptyValuesRow);
    }
    Ok(SetExpr::Values(Values {
        rows,
        explicit_row: false,
        span: SYNTHETIC,
    }))
}

pub fn select(
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
    group_by: GroupBy,
) -> Result<Select, BuildError> {
    if projection.is_empty() {
        return Err(BuildError::EmptyProjection);
    }
    Ok(Select {
        hints: Vec::new(),
        quantifier: SelectQuantifier::None,
        projection,
        from,
        selection,
        group_by,
        having: None,
        qualify: None,
        windows: Vec::new(),
        span: SYNTHETIC,
    })
}

pub fn query(body: SetExpr) -> Query {
    Query {
        with: None,
        body: Box::new(body),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        limit_comma_offset: false,
        fetch: None,
        span: SYNTHETIC,
    }
}

pub fn union_all(parts: Vec<SetExpr>) -> Result<SetExpr, BuildError> {
    let mut parts = parts.into_iter();
    let first = parts.next().ok_or(BuildError::EmptyUnion)?;
    let expression = parts.fold(first, |left, right| {
        SetExpr::SetOperation(SetOperation {
            span: left.span().cover(right.span()),
            left: Box::new(left),
            operator: SetOperator::Union,
            quantifier: SetQuantifier::All,
            right: Box::new(right),
        })
    });
    Ok(parser::balance_union_all(expression))
}

pub fn group_by(expressions: Vec<Expr>) -> GroupBy {
    if expressions.is_empty() {
        GroupBy::None
    } else {
        GroupBy::Expressions {
            span: cover(expressions.iter().map(Expr::span)),
            expressions,
        }
    }
}

pub fn array(elements: Vec<Expr>) -> Expr {
    Expr::Array(ArrayExpr {
        element_type: None,
        elements,
        span: SYNTHETIC,
    })
}

/// Parentheses around a single expression form a Nested node, not a tuple.
pub fn tuple(mut expressions: Vec<Expr>) -> Result<Expr, BuildError> {
    match expressions.len() {
        0 => Err(BuildError::EmptyTuple),
        1 => Ok(nested(expressions.pop().expect("one expression"))),
        _ => Ok(Expr::Tuple(TupleExpr {
            expressions,
            span: SYNTHETIC,
        })),
    }
}

pub fn map(entries: Vec<(Expr, Expr)>) -> Expr {
    Expr::Map(MapExpr {
        entries: entries
            .into_iter()
            .map(|(key, value)| MapEntry {
                key,
                value,
                span: SYNTHETIC,
            })
            .collect(),
        span: SYNTHETIC,
    })
}

fn cover(spans: impl IntoIterator<Item = Span>) -> Span {
    spans.into_iter().fold(SYNTHETIC, Span::cover)
}
