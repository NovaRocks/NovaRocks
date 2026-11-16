// SPDX-License-Identifier: Apache-2.0
//! Type and literal facts matching the pinned Trino syntax vocabulary.
//! Explicit array lengths retain syntax for analyzer-owned capability refusal.
//! Raw token spellings below are individual numeric facts, never raw SQL nodes.

use super::{AstBox, AstVec, Fold, Ident, SyntaxEq, Visit};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypeName {
    pub kind: TypeKind,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeKind {
    Generic(GenericDataType),
    Row(RowDataType),
    DateTime(DateTimeDataType),
    Interval(IntervalDataType),
    ExplicitArray(ExplicitArrayDataType),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenericDataType {
    // Java GenericDataType uses one Identifier, not a QualifiedName.
    pub name: Ident,
    pub arguments: AstVec<DataTypeParameter>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowDataType {
    pub fields: AstVec<RowDataTypeField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowDataTypeField {
    pub name: Option<Ident>,
    pub data_type: TypeName,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DateTimeDataType {
    pub kind: DateTimeType,
    // Explicit WITHOUT TIME ZONE and absence are both false in AstBuilder.
    pub with_time_zone: bool,
    pub precision: Option<AstBox<DataTypeParameter>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DateTimeType {
    Time,
    Timestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntervalDataType {
    pub span: Span,
    pub qualifier: IntervalQualifier,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataTypeParameter {
    Numeric(NumericParameter),
    Type(TypeParameter),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NumericParameter {
    // Both are Java facts: getValue() and getParsedValue(). The parser must
    // establish their consistency with exact NumericParser signed-i64 rules.
    pub spelling: String,
    pub parsed_value: i64,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypeParameter {
    // Java TypeParameter itself has no NodeLocation; its child has one.
    pub data_type: AstBox<TypeName>,
}

/// D31 only: the reference builder rejects before producing an AST node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplicitArrayDataType {
    pub element: AstBox<TypeName>,
    pub length: ArrayLength,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArrayLength {
    // The complete INTEGER_VALUE token. No invented i64/usize bound: D31
    // requires syntax retention and analyzer NOT_SUPPORTED, including overflow.
    pub spelling: String,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntervalQualifier {
    Simple(SimpleIntervalQualifier),
    Composite(CompositeIntervalQualifier),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimpleIntervalQualifier {
    pub precision: Option<AstBox<DataTypeParameter>>,
    pub field: IntervalQualifierField,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompositeIntervalQualifier {
    pub precision: Option<AstBox<DataTypeParameter>>,
    pub from: IntervalQualifierField,
    pub to: IntervalQualifierField,
    pub span: Span,
}

// The Java sealed records are not Nodes and do not own locations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntervalQualifierField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second {
        fractional_precision: Option<AstBox<DataTypeParameter>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Literal {
    // The enclosing span is the concrete Java literal's NodeLocation analogue.
    pub kind: LiteralKind,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiteralKind {
    Null,
    Boolean(bool),
    Long { spelling: String, parsed_value: i64 },
    Double(DoubleValue),
    // DecimalLiteral stores text, not parsed decimal or a primitive f64. For a
    // type constructor this is decoded string content; for number it is text.
    Decimal(String),
    // Both ordinary and Unicode strings produce this decoded value only.
    String(String),
    // BinaryLiteral stores decoded bytes, not hex spelling/whitespace/case.
    Binary(Vec<u8>),
    Generic(GenericLiteral),
    Interval(IntervalLiteral),
}

/// Exact Java Double.compare equality, including signed zero and all NaNs.
/// The reference node retains a double value, not its source numeric lexeme.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DoubleValue {
    canonical_bits: u64,
}

impl DoubleValue {
    pub fn from_f64(value: f64) -> Self {
        Self {
            canonical_bits: if value.is_nan() {
                0x7ff8_0000_0000_0000
            } else {
                value.to_bits()
            },
        }
    }

    pub fn get(self) -> f64 {
        f64::from_bits(self.canonical_bits)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenericLiteral {
    // AstBuilder uses context.identifier().getText(), not visitIdentifier:
    // preserve this tag string, including the original delimiter if present.
    pub type_name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntervalLiteral {
    pub value: String,
    pub sign: IntervalSign,
    pub qualifier: IntervalQualifier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntervalSign {
    Positive,
    Negative,
}

// All traversal dispatches are typed. Root adds the corresponding default
// methods in Visit/Fold (trait_hooks.txt). Node owners retain their own spans.
#[recursive::recursive]
pub fn walk_type_name<V: Visit + ?Sized>(visitor: &mut V, node: &TypeName) {
    match &node.kind {
        TypeKind::Generic(value) => visitor.visit_generic_data_type(value),
        TypeKind::Row(value) => visitor.visit_row_data_type(value),
        TypeKind::DateTime(value) => visitor.visit_date_time_data_type(value),
        TypeKind::Interval(value) => visitor.visit_interval_data_type(value),
        TypeKind::ExplicitArray(value) => visitor.visit_explicit_array_data_type(value),
    }
}

pub fn walk_generic_data_type<V: Visit + ?Sized>(visitor: &mut V, node: &GenericDataType) {
    visitor.visit_ident(&node.name);
    for value in &node.arguments {
        visitor.visit_data_type_parameter(value);
    }
}

pub fn walk_row_data_type<V: Visit + ?Sized>(visitor: &mut V, node: &RowDataType) {
    for value in &node.fields {
        visitor.visit_row_data_type_field(value);
    }
}

pub fn walk_row_data_type_field<V: Visit + ?Sized>(visitor: &mut V, node: &RowDataTypeField) {
    if let Some(name) = &node.name {
        visitor.visit_ident(name);
    }
    visitor.visit_type_name(&node.data_type);
}

pub fn walk_date_time_data_type<V: Visit + ?Sized>(visitor: &mut V, node: &DateTimeDataType) {
    if let Some(value) = &node.precision {
        visitor.visit_data_type_parameter(value);
    }
}

pub fn walk_interval_data_type<V: Visit + ?Sized>(visitor: &mut V, node: &IntervalDataType) {
    visitor.visit_interval_qualifier(&node.qualifier);
}

pub fn walk_explicit_array_data_type<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &ExplicitArrayDataType,
) {
    visitor.visit_type_name(&node.element);
    visitor.visit_array_length(&node.length);
}

pub fn walk_array_length<V: Visit + ?Sized>(_: &mut V, _: &ArrayLength) {}

pub fn walk_data_type_parameter<V: Visit + ?Sized>(visitor: &mut V, node: &DataTypeParameter) {
    match node {
        DataTypeParameter::Numeric(value) => visitor.visit_numeric_parameter(value),
        DataTypeParameter::Type(value) => visitor.visit_type_parameter(value),
    }
}

pub fn walk_numeric_parameter<V: Visit + ?Sized>(_: &mut V, _: &NumericParameter) {}

pub fn walk_type_parameter<V: Visit + ?Sized>(visitor: &mut V, node: &TypeParameter) {
    visitor.visit_type_name(&node.data_type);
}

pub fn walk_interval_qualifier<V: Visit + ?Sized>(visitor: &mut V, node: &IntervalQualifier) {
    match node {
        IntervalQualifier::Simple(value) => visitor.visit_simple_interval_qualifier(value),
        IntervalQualifier::Composite(value) => visitor.visit_composite_interval_qualifier(value),
    }
}

pub fn walk_simple_interval_qualifier<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &SimpleIntervalQualifier,
) {
    if let Some(value) = &node.precision {
        visitor.visit_data_type_parameter(value);
    }
    visitor.visit_interval_qualifier_field(&node.field);
}

pub fn walk_composite_interval_qualifier<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &CompositeIntervalQualifier,
) {
    if let Some(value) = &node.precision {
        visitor.visit_data_type_parameter(value);
    }
    visitor.visit_interval_qualifier_field(&node.from);
    visitor.visit_interval_qualifier_field(&node.to);
}

pub fn walk_interval_qualifier_field<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &IntervalQualifierField,
) {
    if let IntervalQualifierField::Second {
        fractional_precision: Some(value),
    } = node
    {
        visitor.visit_data_type_parameter(value);
    }
}

pub fn walk_literal<V: Visit + ?Sized>(visitor: &mut V, node: &Literal) {
    // All other Java literal facts are leaf scalars. GenericLiteral.type is a
    // String, not an Identifier child that an identifier visitor may rename.
    if let LiteralKind::Interval(value) = &node.kind {
        visitor.visit_interval_qualifier(&value.qualifier);
    }
}

#[recursive::recursive]
pub fn fold_type_name<F: Fold + ?Sized>(folder: &mut F, mut node: TypeName) -> TypeName {
    node.kind = match node.kind {
        TypeKind::Generic(value) => TypeKind::Generic(folder.fold_generic_data_type(value)),
        TypeKind::Row(value) => TypeKind::Row(folder.fold_row_data_type(value)),
        TypeKind::DateTime(value) => TypeKind::DateTime(folder.fold_date_time_data_type(value)),
        TypeKind::Interval(value) => TypeKind::Interval(folder.fold_interval_data_type(value)),
        TypeKind::ExplicitArray(value) => {
            TypeKind::ExplicitArray(folder.fold_explicit_array_data_type(value))
        }
    };
    node
}

pub fn fold_generic_data_type<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: GenericDataType,
) -> GenericDataType {
    node.name = folder.fold_ident(node.name);
    node.arguments = node
        .arguments
        .into_iter()
        .map(|value| folder.fold_data_type_parameter(value))
        .collect();
    node
}

pub fn fold_row_data_type<F: Fold + ?Sized>(folder: &mut F, mut node: RowDataType) -> RowDataType {
    node.fields = node
        .fields
        .into_iter()
        .map(|value| folder.fold_row_data_type_field(value))
        .collect();
    node
}

pub fn fold_row_data_type_field<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: RowDataTypeField,
) -> RowDataTypeField {
    node.name = node.name.map(|name| folder.fold_ident(name));
    node.data_type = folder.fold_type_name(node.data_type);
    node
}

fn fold_optional_parameter<F: Fold + ?Sized>(
    folder: &mut F,
    value: Option<AstBox<DataTypeParameter>>,
) -> Option<AstBox<DataTypeParameter>> {
    value.map(|value| AstBox::new(folder.fold_data_type_parameter(value.into_inner())))
}

pub fn fold_date_time_data_type<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: DateTimeDataType,
) -> DateTimeDataType {
    node.precision = fold_optional_parameter(folder, node.precision);
    node
}

pub fn fold_interval_data_type<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: IntervalDataType,
) -> IntervalDataType {
    node.qualifier = folder.fold_interval_qualifier(node.qualifier);
    node
}

pub fn fold_explicit_array_data_type<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: ExplicitArrayDataType,
) -> ExplicitArrayDataType {
    node.element = AstBox::new(folder.fold_type_name(node.element.into_inner()));
    node.length = folder.fold_array_length(node.length);
    node
}

pub fn fold_array_length<F: Fold + ?Sized>(_: &mut F, node: ArrayLength) -> ArrayLength {
    node
}

pub fn fold_data_type_parameter<F: Fold + ?Sized>(
    folder: &mut F,
    node: DataTypeParameter,
) -> DataTypeParameter {
    match node {
        DataTypeParameter::Numeric(value) => {
            DataTypeParameter::Numeric(folder.fold_numeric_parameter(value))
        }
        DataTypeParameter::Type(value) => {
            DataTypeParameter::Type(folder.fold_type_parameter(value))
        }
    }
}

pub fn fold_numeric_parameter<F: Fold + ?Sized>(
    _: &mut F,
    node: NumericParameter,
) -> NumericParameter {
    node
}

pub fn fold_type_parameter<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TypeParameter,
) -> TypeParameter {
    node.data_type = AstBox::new(folder.fold_type_name(node.data_type.into_inner()));
    node
}

pub fn fold_interval_qualifier<F: Fold + ?Sized>(
    folder: &mut F,
    node: IntervalQualifier,
) -> IntervalQualifier {
    match node {
        IntervalQualifier::Simple(value) => {
            IntervalQualifier::Simple(folder.fold_simple_interval_qualifier(value))
        }
        IntervalQualifier::Composite(value) => {
            IntervalQualifier::Composite(folder.fold_composite_interval_qualifier(value))
        }
    }
}

pub fn fold_simple_interval_qualifier<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: SimpleIntervalQualifier,
) -> SimpleIntervalQualifier {
    node.precision = fold_optional_parameter(folder, node.precision);
    node.field = folder.fold_interval_qualifier_field(node.field);
    node
}

pub fn fold_composite_interval_qualifier<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: CompositeIntervalQualifier,
) -> CompositeIntervalQualifier {
    node.precision = fold_optional_parameter(folder, node.precision);
    node.from = folder.fold_interval_qualifier_field(node.from);
    node.to = folder.fold_interval_qualifier_field(node.to);
    node
}

pub fn fold_interval_qualifier_field<F: Fold + ?Sized>(
    folder: &mut F,
    node: IntervalQualifierField,
) -> IntervalQualifierField {
    match node {
        IntervalQualifierField::Second {
            fractional_precision,
        } => IntervalQualifierField::Second {
            fractional_precision: fold_optional_parameter(folder, fractional_precision),
        },
        other => other,
    }
}

pub fn fold_literal<F: Fold + ?Sized>(folder: &mut F, mut node: Literal) -> Literal {
    node.kind = match node.kind {
        LiteralKind::Interval(mut value) => {
            value.qualifier = folder.fold_interval_qualifier(value.qualifier);
            LiteralKind::Interval(value)
        }
        other => other,
    };
    node
}

fn eq_slice<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(a, b)| a.syntax_eq(b))
}

fn eq_option<T: SyntaxEq>(left: Option<&T>, right: Option<&T>) -> bool {
    match (left, right) {
        (Some(a), Some(b)) => a.syntax_eq(b),
        (None, None) => true,
        _ => false,
    }
}

fn eq_precision(
    left: &Option<AstBox<DataTypeParameter>>,
    right: &Option<AstBox<DataTypeParameter>>,
) -> bool {
    eq_option(left.as_deref(), right.as_deref())
}

impl SyntaxEq for TypeName {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (&self.kind, &other.kind) {
            (TypeKind::Generic(a), TypeKind::Generic(b)) => a.syntax_eq(b),
            (TypeKind::Row(a), TypeKind::Row(b)) => a.syntax_eq(b),
            (TypeKind::DateTime(a), TypeKind::DateTime(b)) => a.syntax_eq(b),
            (TypeKind::Interval(a), TypeKind::Interval(b)) => a.syntax_eq(b),
            (TypeKind::ExplicitArray(a), TypeKind::ExplicitArray(b)) => a.syntax_eq(b),
            _ => false,
        }
    }
}

impl SyntaxEq for GenericDataType {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && eq_slice(&self.arguments, &other.arguments)
    }
}
impl SyntaxEq for RowDataType {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.fields, &other.fields)
    }
}
impl SyntaxEq for RowDataTypeField {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_option(self.name.as_ref(), other.name.as_ref())
            && self.data_type.syntax_eq(&other.data_type)
    }
}
impl SyntaxEq for DateTimeDataType {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.with_time_zone == other.with_time_zone
            && eq_precision(&self.precision, &other.precision)
    }
}
impl SyntaxEq for IntervalDataType {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.qualifier.syntax_eq(&other.qualifier)
    }
}
impl SyntaxEq for DataTypeParameter {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Numeric(a), Self::Numeric(b)) => a.syntax_eq(b),
            (Self::Type(a), Self::Type(b)) => a.syntax_eq(b),
            _ => false,
        }
    }
}
impl SyntaxEq for NumericParameter {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.spelling == other.spelling && self.parsed_value == other.parsed_value
    }
}
impl SyntaxEq for TypeParameter {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.data_type.syntax_eq(&other.data_type)
    }
}
impl SyntaxEq for ExplicitArrayDataType {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.element.syntax_eq(&other.element) && self.length.syntax_eq(&other.length)
    }
}
impl SyntaxEq for ArrayLength {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.spelling == other.spelling
    }
}
impl SyntaxEq for IntervalQualifier {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Simple(a), Self::Simple(b)) => a.syntax_eq(b),
            (Self::Composite(a), Self::Composite(b)) => a.syntax_eq(b),
            _ => false,
        }
    }
}
impl SyntaxEq for SimpleIntervalQualifier {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_precision(&self.precision, &other.precision) && self.field.syntax_eq(&other.field)
    }
}
impl SyntaxEq for CompositeIntervalQualifier {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_precision(&self.precision, &other.precision)
            && self.from.syntax_eq(&other.from)
            && self.to.syntax_eq(&other.to)
    }
}
impl SyntaxEq for IntervalQualifierField {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Year, Self::Year)
            | (Self::Month, Self::Month)
            | (Self::Day, Self::Day)
            | (Self::Hour, Self::Hour)
            | (Self::Minute, Self::Minute) => true,
            (
                Self::Second {
                    fractional_precision: a,
                },
                Self::Second {
                    fractional_precision: b,
                },
            ) => eq_precision(a, b),
            _ => false,
        }
    }
}
impl SyntaxEq for Literal {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (&self.kind, &other.kind) {
            (LiteralKind::Interval(a), LiteralKind::Interval(b)) => {
                a.value == b.value && a.sign == b.sign && a.qualifier.syntax_eq(&b.qualifier)
            }
            // All remaining variants contain only scalar facts, not child spans.
            (a, b) => a == b,
        }
    }
}

#[cfg(test)]
#[path = "type_literal_tests.rs"]
mod tests;
