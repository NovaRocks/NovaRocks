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
    TooFewCoalesceOperands,
    TooFewFormatArguments,
    EmptyTuple,
    EmptyInList,
    EmptyOrderBy,
    EmptyGroupingSets,
    EmptyUnion,
    EmptyWildcardPrefix,
    Number(ParserError),
}

impl fmt::Display for BuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyObjectName => formatter.write_str("SQL object name must not be empty"),
            Self::EmptyProjection => formatter.write_str("SELECT projection must not be empty"),
            Self::EmptyValues => formatter.write_str("VALUES must contain at least one row"),
            Self::EmptyValuesRow => formatter.write_str("VALUES row must not be empty"),
            Self::TooFewCoalesceOperands => {
                formatter.write_str("COALESCE requires at least two operands")
            }
            Self::TooFewFormatArguments => {
                formatter.write_str("FORMAT requires at least two arguments")
            }
            Self::EmptyCase => formatter.write_str("CASE must contain at least one branch"),
            Self::EmptyTuple => formatter.write_str("tuple must contain at least one expression"),
            Self::EmptyInList => {
                formatter.write_str("IN list must contain at least one expression")
            }
            Self::EmptyOrderBy => {
                formatter.write_str("ORDER BY must contain at least one sort item")
            }
            Self::EmptyGroupingSets => formatter.write_str("grouping sets cannot be empty"),
            Self::EmptyUnion => formatter.write_str("UNION ALL must contain at least one operand"),
            Self::EmptyWildcardPrefix => {
                formatter.write_str("qualified wildcard must contain a name prefix")
            }
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
pub fn ident(value: impl Into<String>, quoted: bool) -> Ident {
    ident_at(value, quoted, SYNTHETIC)
}

/// Builds an identifier derived from a particular user name.
pub fn ident_at(value: impl Into<String>, quoted: bool, span: Span) -> Ident {
    Ident {
        value: value.into(),
        quoted,
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
    let span = qualifier.span.cover(name.span);
    Expr::Dereference(AstBox::new(DereferenceExpr {
        base: Expr::Identifier(qualifier),
        field: Some(name),
        span,
    }))
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
    literal(LiteralKind::Long {
        spelling: value.to_string(),
        parsed_value: value,
    })
}

pub fn string(value: impl Into<String>) -> Expr {
    literal(LiteralKind::String(value.into()))
}

/// Builds a binary literal from its decoded bytes.
pub fn binary_literal(value: Vec<u8>) -> Expr {
    literal(LiteralKind::Binary(value))
}

pub fn boolean(value: bool) -> Expr {
    literal(LiteralKind::Boolean(value))
}

pub fn null() -> Expr {
    literal(LiteralKind::Null)
}

/// Preserve an already-authored literal kind, including typed program values.
/// This does not admit source text or reapply the source lexer's integer width.
pub fn literal(kind: LiteralKind) -> Expr {
    Expr::Literal(Literal {
        kind,
        span: SYNTHETIC,
    })
}

pub fn type_name(name: Ident, arguments: Vec<DataTypeParameter>) -> TypeName {
    TypeName {
        kind: TypeKind::Generic(GenericDataType {
            name,
            arguments: arguments.into(),
        }),
        span: SYNTHETIC,
    }
}

pub fn simple_type(name: impl Into<String>) -> TypeName {
    let name = name.into();
    let kind = match name.to_ascii_uppercase().as_str() {
        "TIME" => Some(DateTimeType::Time),
        "TIMESTAMP" | "DATETIME" => Some(DateTimeType::Timestamp),
        _ => None,
    };
    if let Some(kind) = kind {
        TypeName {
            kind: TypeKind::DateTime(DateTimeDataType {
                kind,
                with_time_zone: false,
                precision: None,
            }),
            span: SYNTHETIC,
        }
    } else {
        type_name(ident(name, false), Vec::new())
    }
}

pub fn row_type(fields: Vec<RowDataTypeField>) -> TypeName {
    TypeName {
        kind: TypeKind::Row(RowDataType {
            fields: fields.into(),
        }),
        span: SYNTHETIC,
    }
}

pub fn type_parameter(data_type: TypeName) -> DataTypeParameter {
    DataTypeParameter::Type(TypeParameter {
        data_type: AstBox::new(data_type),
    })
}

pub fn numeric_parameter(value: i64) -> DataTypeParameter {
    DataTypeParameter::Numeric(NumericParameter {
        spelling: value.to_string(),
        parsed_value: value,
        span: SYNTHETIC,
    })
}

pub fn cast(expr: Expr, data_type: TypeName) -> Expr {
    let span = expr.span();
    Expr::Cast(AstBox::new(CastExpr {
        expression: expr,
        data_type,
        safe: false,
        span,
    }))
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
            kind: LiteralKind::Long {
                spelling: value, ..
            },
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
    binary_node(left, operator, right)
}

/// An empty conjunction represents an absent predicate, not a guessed literal.
pub fn and_all(operands: Vec<Expr>) -> Option<Expr> {
    boolean_chain(operands, LogicalOperator::And)
}

/// An empty disjunction represents an absent predicate, not a guessed literal.
pub fn or_all(operands: Vec<Expr>) -> Option<Expr> {
    boolean_chain(operands, LogicalOperator::Or)
}

fn boolean_chain(mut operands: Vec<Expr>, operator: LogicalOperator) -> Option<Expr> {
    match operands.len() {
        0 => None,
        1 => operands.pop(),
        _ => Some(Expr::Logical(LogicalExpr {
            operator,
            span: cover(operands.iter().map(Expr::span)),
            terms: operands.into(),
        })),
    }
}

pub fn comparison(left: Expr, operator: ComparisonOperator, right: Expr) -> Expr {
    let span = left.span().cover(right.span());
    Expr::Predicated(AstBox::new(PredicatedExpr {
        value: left,
        predicate: Predicate::Comparison(ComparisonPredicate {
            operator,
            span: right.span(),
            right,
        }),
        span,
    }))
}

pub fn is_distinct_from(left: Expr, right: Expr, negated: bool) -> Expr {
    let span = left.span().cover(right.span());
    Expr::Predicated(AstBox::new(PredicatedExpr {
        value: left,
        predicate: Predicate::DistinctFrom(DistinctFromPredicate {
            negated,
            span: right.span(),
            right,
        }),
        span,
    }))
}

pub fn is_null(expr: Expr, negated: bool) -> Expr {
    let span = expr.span();
    Expr::Predicated(AstBox::new(PredicatedExpr {
        value: expr,
        predicate: Predicate::IsNull(IsNullPredicate { negated, span }),
        span,
    }))
}

pub fn in_list(expr: Expr, values: Vec<Expr>, negated: bool) -> Result<Expr, BuildError> {
    if values.is_empty() {
        return Err(BuildError::EmptyInList);
    }
    let list_span = cover(values.iter().map(Expr::span));
    let span = expr.span().cover(list_span);
    let value_list = Expr::InList(AstBox::new(InListExpr {
        values: values.into(),
        span: list_span,
    }));
    Ok(Expr::Predicated(AstBox::new(PredicatedExpr {
        value: expr,
        predicate: Predicate::In(InPredicate {
            negated,
            value_list,
            span: list_span,
        }),
        span,
    })))
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
    let when_clauses = branches
        .into_iter()
        .map(|(condition, result)| WhenClause {
            span: condition.span().cover(result.span()),
            match_value: WhenMatch::Operand(condition),
            result,
        })
        .collect();
    Ok(Expr::SearchedCase(AstBox::new(SearchedCaseExpr {
        when_clauses,
        default_value: else_result,
        span,
    })))
}

pub fn function(name: ObjectName, arguments: Vec<Expr>) -> Expr {
    let span = name.span.cover(cover(arguments.iter().map(Expr::span)));
    Expr::FunctionCall(AstBox::new(FunctionCall {
        name,
        arguments: arguments
            .into_iter()
            .map(|value| call_argument(value, None))
            .collect(),
        distinct: false,
        order_by: None,
        filter: None,
        null_treatment: None,
        window: None,
        processing_mode: None,
        span,
    }))
}

pub fn count_star() -> Expr {
    function(
        ObjectName {
            parts: vec![ident("COUNT", false)],
            span: SYNTHETIC,
        },
        vec![identifier(ident("*", false))],
    )
}

pub fn item(expression: Expr, alias: Option<Ident>) -> SelectItem {
    let span = alias.as_ref().map_or(expression.span(), |alias| {
        expression.span().cover(alias.span)
    });
    SelectItem::SingleColumn(AstBox::new(SingleColumn {
        expression,
        alias,
        span,
    }))
}
pub fn wildcard() -> SelectItem {
    SelectItem::AllColumns(AstBox::new(AllColumns {
        target: None,
        aliases: Vec::new(),
        span: SYNTHETIC,
    }))
}
pub fn qualified_wildcard(prefix: Vec<Ident>) -> Result<SelectItem, BuildError> {
    let mut parts = prefix.into_iter();
    let first = parts.next().ok_or(BuildError::EmptyWildcardPrefix)?;
    let mut target = Expr::Identifier(first);
    for field in parts {
        let span = target.span().cover(field.span);
        target = Expr::Dereference(AstBox::new(DereferenceExpr {
            base: target,
            field: Some(field),
            span,
        }));
    }
    let span = target.span();
    Ok(SelectItem::AllColumns(AstBox::new(AllColumns {
        target: Some(target),
        aliases: Vec::new(),
        span,
    })))
}

/// Wraps one real relation with its authored alias and optional column list.
pub fn alias(relation: Relation, name: Ident, column_names: Option<Vec<Ident>>) -> Relation {
    let span = relation.span().cover(name.span).cover(cover(
        column_names.iter().flatten().map(|column| column.span),
    ));
    Relation::Aliased(AstBox::new(AliasedRelation {
        relation: AstBox::new(relation),
        alias: name,
        column_names,
        span,
    }))
}
pub fn table(
    name: ObjectName,
    query_period: Option<QueryPeriod>,
    branch: Option<Ident>,
) -> Relation {
    let span = name.span;
    Relation::Table(AstBox::new(Table {
        name,
        query_period,
        branch,
        span,
    }))
}
pub fn derived(query: Query) -> Relation {
    let span = query.span;
    Relation::TableSubquery(AstBox::new(TableSubquery {
        query: AstBox::new(query),
        span,
    }))
}
pub fn join(
    left: Relation,
    kind: JoinType,
    right: Relation,
    criteria: Option<JoinCriteria>,
) -> Relation {
    let span = left.span().cover(right.span());
    Relation::Join(AstBox::new(Join {
        left: AstBox::new(left),
        right: AstBox::new(right),
        kind,
        criteria,
        span,
    }))
}

pub fn values(rows: Vec<Vec<Expr>>) -> Result<SetExpr, BuildError> {
    if rows.is_empty() {
        return Err(BuildError::EmptyValues);
    }
    if rows.iter().any(Vec::is_empty) {
        return Err(BuildError::EmptyValuesRow);
    }
    Ok(SetExpr::Values(Values {
        rows: rows
            .into_iter()
            .map(|cells| {
                Expr::Row(RowExpr {
                    fields: cells
                        .into_iter()
                        .map(|expression| RowField {
                            name: None,
                            span: expression.span(),
                            expression,
                        })
                        .collect(),
                    span: SYNTHETIC,
                })
            })
            .collect(),
        span: SYNTHETIC,
    }))
}

pub fn select(
    projection: Vec<SelectItem>,
    from: Option<Relation>,
    selection: Option<Expr>,
    group_by: Option<GroupBy>,
) -> Result<Select, BuildError> {
    if projection.is_empty() {
        return Err(BuildError::EmptyProjection);
    }
    Ok(Select {
        distinct: false,
        projection: projection.into(),
        from: from,
        selection: selection,
        group_by: group_by,
        having: None,
        qualify: None,
        windows: AstVec::new(),
        modifiers: Default::default(),
        span: SYNTHETIC,
    })
}

pub fn query(body: SetExpr) -> Query {
    Query {
        session_properties: AstVec::new(),
        functions: AstVec::new(),
        with: None,
        body: AstBox::new(body),
        modifiers: Default::default(),
        span: SYNTHETIC,
    }
}

/// Constructs a set operation without balancing syntax into a binary tree.
/// Only homogeneous, unparenthesized UNION ALL inputs are merged.
pub fn set_operation(
    left: SetExpr,
    operator: SetOperator,
    distinct: bool,
    right: SetExpr,
    corresponding: Option<Corresponding>,
) -> SetExpr {
    let span = left.span().cover(right.span());
    let flatten = operator == SetOperator::Union && !distinct && corresponding.is_none();
    let mut inputs = match left {
        SetExpr::SetOperation(operation)
            if flatten
                && operation.operator == SetOperator::Union
                && !operation.distinct
                && operation.corresponding.is_none() =>
        {
            operation.inputs
        }
        other => vec![other].into(),
    };
    match right {
        SetExpr::SetOperation(operation)
            if flatten
                && operation.operator == SetOperator::Union
                && !operation.distinct
                && operation.corresponding.is_none() =>
        {
            inputs.extend(operation.inputs)
        }
        other => inputs.push(other),
    }
    SetExpr::SetOperation(SetOperation {
        inputs,
        operator,
        distinct,
        corresponding,
        span,
    })
}

pub fn union_all(parts: Vec<SetExpr>) -> Result<SetExpr, BuildError> {
    let mut parts = parts.into_iter();
    let first = parts.next().ok_or(BuildError::EmptyUnion)?;
    Ok(parts.fold(first, |left, right| {
        set_operation(left, SetOperator::Union, false, right, None)
    }))
}

pub fn group_by(expressions: Vec<Expr>) -> Option<GroupBy> {
    if expressions.is_empty() {
        return None;
    }
    let span = cover(expressions.iter().map(Expr::span));
    Some(GroupBy {
        distinct: false,
        elements: expressions
            .into_iter()
            .map(|expression| {
                GroupingElement::Simple(SimpleGroupBy {
                    span: expression.span(),
                    columns: vec![expression].into(),
                })
            })
            .collect(),
        span,
    })
}

/// Builds one nonempty family of grouping sets, preserving empty member sets.
pub fn grouping_sets(
    kind: GroupingSetsKind,
    sets: Vec<Vec<Expr>>,
) -> Result<GroupingSets, BuildError> {
    if sets.is_empty() {
        return Err(BuildError::EmptyGroupingSets);
    }
    let span = cover(sets.iter().flat_map(|set| set.iter().map(Expr::span)));
    Ok(GroupingSets {
        kind,
        sets: sets.into_iter().map(AstVec::from).collect(),
        span,
    })
}

pub fn array(values: Vec<Expr>) -> Expr {
    Expr::Array(ArrayExpr {
        values: values.into(),
        span: SYNTHETIC,
    })
}

/// Parentheses around a single expression form a Nested node, not a tuple.
pub fn tuple(mut expressions: Vec<Expr>) -> Result<Expr, BuildError> {
    match expressions.len() {
        0 => Err(BuildError::EmptyTuple),
        1 => Ok(nested(expressions.pop().expect("one expression"))),
        _ => tuple_value(expressions),
    }
}

/// Preserve a positional value tuple whose arity is owned by a typed caller.
/// A one-field value tuple remains distinct from source parentheses.
pub fn tuple_value(expressions: Vec<Expr>) -> Result<Expr, BuildError> {
    if expressions.is_empty() {
        return Err(BuildError::EmptyTuple);
    }
    Ok(Expr::Row(RowExpr {
        fields: expressions
            .into_iter()
            .map(|expression| RowField {
                span: expression.span(),
                name: None,
                expression,
            })
            .collect(),
        span: SYNTHETIC,
    }))
}

/// Builds the ordinary variadic map call, retaining key/value order and nodes.
pub fn map(entries: Vec<(Expr, Expr)>) -> Expr {
    function(
        ObjectName {
            parts: vec![ident("map", false)],
            span: SYNTHETIC,
        },
        entries
            .into_iter()
            .flat_map(|(key, value)| [key, value])
            .collect(),
    )
}

fn cover(spans: impl IntoIterator<Item = Span>) -> Span {
    spans.into_iter().fold(SYNTHETIC, Span::cover)
}

/// Builds one sort item while retaining its original key expression.
pub fn sort_item(sort_key: Expr, ordering: SortOrdering, null_ordering: NullOrdering) -> SortItem {
    let span = sort_key.span();
    SortItem {
        sort_key,
        ordering,
        null_ordering,
        span,
    }
}

/// Builds an authored ORDER BY node without claiming an original keyword span.
pub fn order_by(items: Vec<SortItem>) -> Result<OrderBy, BuildError> {
    if items.is_empty() {
        return Err(BuildError::EmptyOrderBy);
    }
    Ok(OrderBy {
        items: items.into(),
        span: SYNTHETIC,
    })
}

/// Retains the original argument value and optional source-authored name.
pub fn call_argument(value: Expr, name: Option<Ident>) -> CallArgument {
    let span = name
        .as_ref()
        .map_or_else(|| value.span(), |name| name.span.cover(value.span()));
    CallArgument { name, value, span }
}

/// Builds the exact optional-else IF syntax while retaining original operands.
pub fn if_expr(condition: Expr, true_value: Expr, false_value: Option<Expr>) -> Expr {
    let span = condition
        .span()
        .cover(true_value.span())
        .cover(false_value.as_ref().map_or(SYNTHETIC, Expr::span));
    Expr::If(AstBox::new(IfExpr {
        condition,
        true_value,
        false_value,
        span,
    }))
}
pub fn null_if(first: Expr, second: Expr) -> Expr {
    let span = first.span().cover(second.span());
    Expr::NullIf(AstBox::new(NullIfExpr {
        first,
        second,
        span,
    }))
}
pub fn coalesce(operands: Vec<Expr>) -> Result<Expr, BuildError> {
    if operands.len() < 2 {
        return Err(BuildError::TooFewCoalesceOperands);
    }
    let span = cover(operands.iter().map(Expr::span));
    Ok(Expr::Coalesce(AstBox::new(CoalesceExpr {
        operands: operands.into(),
        span,
    })))
}
pub fn try_expr(inner: Expr) -> Expr {
    let span = inner.span();
    Expr::Try(AstBox::new(TryExpr { inner, span }))
}
pub fn format_expr(arguments: Vec<Expr>) -> Result<Expr, BuildError> {
    if arguments.len() < 2 {
        return Err(BuildError::TooFewFormatArguments);
    }
    let span = cover(arguments.iter().map(Expr::span));
    Ok(Expr::Format(AstBox::new(FormatExpr {
        arguments: arguments.into(),
        span,
    })))
}

/// Builds an EXTRACT node without reinterpreting the original operand.
pub fn extract(expression: Expr, field: ExtractField) -> Expr {
    let span = expression.span();
    Expr::Extract(AstBox::new(ExtractExpr {
        expression,
        field,
        span,
    }))
}
