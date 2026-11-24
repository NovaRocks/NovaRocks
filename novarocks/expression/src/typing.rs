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

//! Node-level typing for scalar expressions after syntax and name resolution.

// Design: ADR-0245 (docs/adr/ADR-0245-scalar-typing-finalizes-the-original-overload.md)
use std::fmt;

use arrow::datatypes::DataType;
use novarocks_constant_contract::{ConstantError, ConstantPolicy, ConstantValue};
use novarocks_functions::{
    EngineFunctionCatalog, FunctionArgument, FunctionArgumentType, FunctionBindingError,
    FunctionBindingRequest, FunctionResultType, ResolvedFunctionBinding,
};
use novarocks_type_contract::{
    AssignmentCommonTypeError, CompileCheckpoints, CompileControlError, DecimalOverflowPolicy,
    FunctionId, FunctionKind, FunctionOverloadId, FunctionValueType, OperatorFunction,
    PureCompileControl, ValueTypeError,
};

mod conversion;
pub mod declaration;
mod literal;
mod route;
#[doc(hidden)]
pub mod syntax_constant;
mod temporal;
pub use conversion::operator_decimal_float;
pub use literal::{LiteralEmission, LiteralSyntax, TypedLiteral, supports_typed_string};
pub use route::{ScalarTypingRoute, scalar_typing_route};
pub use syntax_constant::{LiteralValue, admit_syntax_constant};
pub use temporal::{
    TemporalSyntax, at_zone_syntax, bind_extract, bind_temporal_syntax, clock_syntax,
    declared_temporal_type, qualified_datetime_difference, select_temporal_cast,
    temporal_function_value_type,
};

/// Project numeric and comparison syntax into the installed operator vocabulary.
pub fn syntax_arithmetic(operator: novarocks_parser::ast::BinaryOperator) -> OperatorFunction {
    use novarocks_parser::ast::BinaryOperator::*;
    match operator {
        Add => OperatorFunction::Add,
        Subtract => OperatorFunction::Subtract,
        Multiply => OperatorFunction::Multiply,
        Divide => OperatorFunction::Divide,
        Modulo => OperatorFunction::Modulo,
    }
}

pub fn syntax_comparison(operator: novarocks_parser::ast::ComparisonOperator) -> OperatorFunction {
    use novarocks_parser::ast::ComparisonOperator::*;
    match operator {
        Equal => OperatorFunction::Equal,
        NotEqual => OperatorFunction::NotEqual,
        LessThan => OperatorFunction::LessThan,
        LessThanOrEqual => OperatorFunction::LessThanOrEqual,
        GreaterThan => OperatorFunction::GreaterThan,
        GreaterThanOrEqual => OperatorFunction::GreaterThanOrEqual,
    }
}

pub fn syntax_distinct_from(negated: bool) -> OperatorFunction {
    if negated {
        OperatorFunction::Identical
    } else {
        OperatorFunction::Distinct
    }
}

/// The caller's original statement binding authority. Implementations retain
/// their declaration and installed implementation admission at both steps.
pub trait ScalarBindingCatalog {
    fn select_scalar(
        &self,
        name: &str,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError>;

    fn select_operator(
        &self,
        operator: OperatorFunction,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError>;

    /// Select a syntax-owned hidden scalar before converting its arguments.
    fn select_trusted_scalar(
        &self,
        function: &FunctionId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError>;

    fn select_exact_scalar(
        &self,
        function: &FunctionId,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError>;

    fn select_value_conversion(
        &self,
        argument: &FunctionArgument,
        target: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError>;
}

impl ScalarBindingCatalog for EngineFunctionCatalog {
    fn select_scalar(
        &self,
        name: &str,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(name, FunctionKind::Scalar, request, control)
    }

    fn select_operator(
        &self,
        operator: OperatorFunction,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.select_trusted_scalar(&operator.function_id(), request, control)
    }

    fn select_trusted_scalar(
        &self,
        function: &FunctionId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted_by_id(function, FunctionKind::Scalar, request, control)
    }

    fn select_exact_scalar(
        &self,
        function: &FunctionId,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted_at_overload(
            function,
            FunctionKind::Scalar,
            overload,
            request,
            control,
        )
    }

    fn select_value_conversion(
        &self,
        argument: &FunctionArgument,
        target: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted(
            novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            request(std::slice::from_ref(argument), Some(target)),
            control,
        )
    }
}

pub struct TypingContext<'a> {
    pub catalog: &'a dyn ScalarBindingCatalog,
    pub constant_policy: ConstantPolicy,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
}

#[derive(Clone, Copy, Debug)]
pub enum NodeShape<'a> {
    CatalogCall {
        name: &'a str,
        expected_result: Option<&'a FunctionValueType>,
    },
    Row {
        fields: Option<&'a [novarocks_parser::ast::RowField]>,
    },
    RowField,
    MapSubscript,
    Operator(OperatorFunction),
    Coalesce,
    UnaryPlus,
    And,
    Or,
    IsNull,
    SearchedCase {
        when_count: usize,
        has_else: bool,
    },
    Literal(LiteralSyntax<'a>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildForm {
    IntegerLiteral(i64),
    /// The caller has checked every negation in its original signed width.
    NegatedIntegerLiteral(i64),
    NullLiteral,
    Other,
}

#[derive(Clone, Debug)]
pub struct ChildTyping {
    pub value_type: FunctionValueType,
    pub constant: Option<ConstantValue>,
    pub form: ChildForm,
}

impl ChildTyping {
    pub(crate) fn argument(&self) -> FunctionArgument {
        FunctionArgument::Value {
            value_type: self.value_type.clone(),
            constant: self.constant.clone(),
        }
    }
}

#[derive(Debug)]
pub struct CarrierConversion {
    pub target: FunctionValueType,
    pub mode: Option<novarocks_type_contract::TrinoCastMode>,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
}

#[derive(Debug)]
pub struct ValueDomainConversion {
    pub binding: ResolvedFunctionBinding,
    pub expected_result: FunctionValueType,
    pub target: FunctionValueType,
    pub tail: Option<CarrierConversion>,
    pub origin: ValueConversionOrigin,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueConversionOrigin {
    ValueDomain,
    ArrayCommonCoercion,
    OperatorDecimalFloat,
}

#[derive(Debug)]
pub enum Conversion {
    Keep,
    RetypeLiteral(TypedLiteral),
    Cast(CarrierConversion),
    ValueDomain(Box<ValueDomainConversion>),
}

#[derive(Debug)]
pub struct SelectedCall {
    binding: ResolvedFunctionBinding,
    expected_result: Option<FunctionValueType>,
}

#[derive(Debug)]
pub enum NodeTyping {
    Value {
        result: FunctionValueType,
        conversions: Vec<Conversion>,
    },
    Call {
        selected: SelectedCall,
        conversions: Vec<Conversion>,
    },
    Literal(LiteralEmission),
}

#[derive(Debug)]
pub enum TypingError {
    TypeMismatch(String),
    InvalidLiteral(String),
    NumericOutOfRange(String),
    Unsupported(String),
    InvalidShape(&'static str),
    Binding(FunctionBindingError),
    Constant(ConstantError),
    JsonLiteralValidation(novarocks_functions::KernelFailure),
    ValueType(ValueTypeError),
    CommonType(AssignmentCommonTypeError),
    Control(CompileControlError),
}

impl From<FunctionBindingError> for TypingError {
    fn from(error: FunctionBindingError) -> Self {
        match error {
            FunctionBindingError::Control(error) => Self::Control(error),
            FunctionBindingError::Constant(error) => error.into(),
            error => Self::Binding(error),
        }
    }
}

impl From<ConstantError> for TypingError {
    fn from(error: ConstantError) -> Self {
        match error {
            ConstantError::Control(error) => Self::Control(error),
            error => Self::Constant(error),
        }
    }
}

impl From<ValueTypeError> for TypingError {
    fn from(error: ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}

impl From<novarocks_type_contract::ExecutionValueTypeError> for TypingError {
    fn from(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::ValueType(error.into())
    }
}

impl From<AssignmentCommonTypeError> for TypingError {
    fn from(error: AssignmentCommonTypeError) -> Self {
        match error {
            AssignmentCommonTypeError::Control(error) => Self::Control(error),
            error => Self::CommonType(error),
        }
    }
}

impl From<CompileControlError> for TypingError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

impl fmt::Display for TypingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch(message)
            | Self::InvalidLiteral(message)
            | Self::NumericOutOfRange(message)
            | Self::Unsupported(message) => f.write_str(message),
            Self::InvalidShape(message) => f.write_str(message),
            Self::Binding(error) => error.fmt(f),
            Self::Constant(error) => error.fmt(f),
            Self::JsonLiteralValidation(error) => error.fmt(f),
            Self::ValueType(error) => error.fmt(f),
            Self::CommonType(error) => error.fmt(f),
            Self::Control(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for TypingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Binding(error) => Some(error),
            Self::Constant(error) => Some(error),
            Self::JsonLiteralValidation(error) => Some(error),
            Self::ValueType(error) => Some(error),
            Self::CommonType(error) => Some(error),
            Self::Control(error) => Some(error),
            _ => None,
        }
    }
}

/// Select the temporal operator for typed binary operands shared by SQL and EXL.
pub fn temporal_binary_operator(
    operator: novarocks_type_contract::ArithmeticOperator,
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<Option<OperatorFunction>, TypingError> {
    use novarocks_type_contract::ArithmeticOperator as A;
    if !temporal_arithmetic_type(left) && !temporal_arithmetic_type(right) {
        return Ok(None);
    }
    Ok(Some(match operator {
        A::Add => OperatorFunction::TemporalAdd,
        A::Subtract => OperatorFunction::TemporalSubtract,
        A::Multiply => OperatorFunction::TemporalMultiply,
        A::Divide => OperatorFunction::TemporalDivide,
        A::Modulo => {
            return Err(TypingError::TypeMismatch(
                "No matching temporal arithmetic overload for modulo".into(),
            ));
        }
    }))
}

fn temporal_arithmetic_type(value: &FunctionValueType) -> bool {
    value.value_type().temporal().is_some() || value.data_type() == &DataType::Date32
}

pub fn validate_temporal_unary_plus(operand: &FunctionValueType) -> Result<(), TypingError> {
    if temporal_arithmetic_type(operand) {
        Err(TypingError::TypeMismatch(
            "Unary plus requires a numeric operand".into(),
        ))
    } else {
        Ok(())
    }
}

pub fn negation_operator(operand: &FunctionValueType) -> OperatorFunction {
    if matches!(
        operand.value_type().temporal(),
        Some(novarocks_type_contract::TemporalType::Interval(_))
    ) {
        OperatorFunction::TemporalNegation
    } else {
        OperatorFunction::Negation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowFieldResolution {
    NotRow,
    Missing,
    Ambiguous,
    Found(usize),
}

/// Resolve the SQL name against named fields of the semantic ROW declaration.
pub fn resolve_row_field_name<E>(
    source: &FunctionValueType,
    requested: &str,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<RowFieldResolution, E> {
    let novarocks_type_contract::ValueKind::Struct(fields) = source.value_type().kind() else {
        return Ok(RowFieldResolution::NotRow);
    };
    let mut selected = None;
    for (ordinal, field) in fields.iter().enumerate() {
        observe()?;
        if field
            .row_name()
            .is_some_and(|name| row_names_match(name, requested))
            && selected.replace(ordinal).is_some()
        {
            return Ok(RowFieldResolution::Ambiguous);
        }
    }
    Ok(selected.map_or(RowFieldResolution::Missing, RowFieldResolution::Found))
}

fn row_names_match(left: &str, right: &str) -> bool {
    if left.is_ascii() && right.is_ascii() {
        return left.eq_ignore_ascii_case(right);
    }
    fn simple_case(character: char) -> char {
        let mut uppercase = character.to_uppercase();
        let upper = uppercase.next().expect("a char has an uppercase mapping");
        // ROW dereference compares individual characters. Multi-character
        // display mappings (e.g. ß -> SS) do not merge two field names.
        let simple_upper = if uppercase.next().is_none() {
            upper
        } else {
            character
        };
        simple_upper
            .to_lowercase()
            .next()
            .expect("a char has a lowercase mapping")
    }
    left.chars()
        .map(simple_case)
        .eq(right.chars().map(simple_case))
}

/// Construct a precise nonnull ROW from actual children and their declarations.
pub fn row_constructor_result<'a>(
    fields: impl ExactSizeIterator<Item = (Option<&'a str>, &'a FunctionValueType)>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, TypingError> {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };

    let storage = ValueTypeStorageLifetime::pure();
    let mut layout = Vec::with_capacity(fields.len());
    let mut semantic = Vec::with_capacity(fields.len());
    for (name, field) in fields {
        work.step()?;
        if name == Some("") {
            return Err(TypingError::InvalidShape(
                "a named ROW field requires a nonempty name",
            ));
        }
        let name = name.unwrap_or_default();
        layout.push(arrow::datatypes::Field::new(
            name,
            field.data_type().clone(),
            field.nullable(),
        ));
        semantic.push(ValueField::new(
            name,
            field.value_type().clone(),
            storage.clone(),
        ));
    }
    work.flush()?;
    let mut observe = |units| -> Result<(), TypingError> {
        for _ in 0..units {
            work.step()?;
        }
        Ok(())
    };
    let semantic = ValueTypeRef::try_new_observed(
        ValueKind::Struct(semantic.into()),
        false,
        storage.clone(),
        &mut observe,
    )?;
    let layout = ValueLayout::try_from_normalized_arrow_observed(
        DataType::Struct(layout.into()),
        storage,
        &mut observe,
    )?;
    let binding = novarocks_type_contract::TypedValueBinding::try_new_observed(
        semantic,
        layout,
        &mut observe,
    )?;
    Ok(FunctionValueType::try_from_typed_binding(binding)?)
}

pub fn rebind_row_constructor_result(
    original: &FunctionValueType,
    arguments: &[FunctionArgument],
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, TypingError> {
    let novarocks_type_contract::ValueKind::Struct(fields) = original.value_type().kind() else {
        return Err(TypingError::InvalidShape(
            "ROW constructor lacks its original ROW declaration",
        ));
    };
    if original.nullable() || fields.len() != arguments.len() {
        return Err(TypingError::InvalidShape(
            "ROW declaration does not match its value operands",
        ));
    }
    let values = arguments
        .iter()
        .map(|argument| match argument {
            FunctionArgument::Value { value_type, .. } => Ok(value_type),
            _ => Err(TypingError::InvalidShape(
                "ROW constructor requires value operands",
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    row_constructor_result(
        fields.iter().map(|field| field.row_name()).zip(values),
        work,
    )
}

impl SelectedCall {
    pub fn expected_result(&self) -> Option<&FunctionValueType> {
        self.expected_result.as_ref()
    }
}

/// Select a node's result and its ordered child conversions. The caller
/// applies the decisions and hands the actual converted facts to finish_call.
pub fn type_node(
    context: &TypingContext<'_>,
    shape: NodeShape<'_>,
    children: &[ChildTyping],
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeTyping, TypingError> {
    work.flush()?;
    let typed = match shape {
        NodeShape::CatalogCall {
            name,
            expected_result,
        } => type_catalog_call(context, name, expected_result, children, work)?,
        NodeShape::Coalesce => {
            let Some((first, remaining)) = children.split_first() else {
                return Err(TypingError::InvalidShape("COALESCE requires operands"));
            };
            remaining
                .iter()
                .try_fold(first.value_type.clone(), |common, child| {
                    novarocks_type_contract::assignment_common_value_type(
                        &common,
                        &child.value_type,
                        work,
                    )
                })?;
            type_catalog_call(context, "coalesce", None, children, work)?
        }
        NodeShape::Row { fields } => {
            if fields.is_some_and(|fields| fields.len() != children.len()) {
                return Err(TypingError::InvalidShape(
                    "ROW names do not match its value operands",
                ));
            }
            let names = fields.map(|fields| {
                fields
                    .iter()
                    .map(|field| {
                        field.name.as_ref().map(|name| {
                            if name.quoted {
                                std::borrow::Cow::Borrowed(name.value.as_str())
                            } else {
                                std::borrow::Cow::Owned(name.value.to_ascii_uppercase())
                            }
                        })
                    })
                    .collect::<Vec<_>>()
            });
            let result = row_constructor_result(
                children.iter().enumerate().map(|(ordinal, child)| {
                    let name = names.as_ref().and_then(|names| names[ordinal].as_deref());
                    (name, &child.value_type)
                }),
                work,
            )?;
            let function = FunctionId::try_new("builtin.scalar/row/v1")
                .expect("valid ROW constructor function identity");
            let overload = FunctionOverloadId::try_new("builtin.scalar/row/dynamic-v1")
                .expect("valid ROW constructor overload identity");
            let arguments = arguments(children, work)?;
            work.flush()?;
            let binding = context.catalog.select_exact_scalar(
                &function,
                &overload,
                request(&arguments, Some(&result)),
                work.control(),
            )?;
            type_selected_call(context, binding, Some(&result), children, work)?
        }
        NodeShape::RowField => {
            let arguments = arguments(children, work)?;
            let function = FunctionId::try_new("builtin.scalar/__row_field/v1")
                .expect("valid ROW field function identity");
            work.flush()?;
            let binding = context.catalog.select_trusted_scalar(
                &function,
                request(&arguments, None),
                work.control(),
            )?;
            type_selected_call(context, binding, None, children, work)?
        }
        NodeShape::MapSubscript => {
            let arguments = arguments(children, work)?;
            let function = FunctionId::try_new("builtin.scalar/__map_subscript/v1")
                .expect("valid MAP subscript function identity");
            work.flush()?;
            let binding = context.catalog.select_trusted_scalar(
                &function,
                request(&arguments, None),
                work.control(),
            )?;
            type_selected_call(context, binding, None, children, work)?
        }
        NodeShape::Operator(operator) => {
            let unary = matches!(
                operator,
                OperatorFunction::Negation | OperatorFunction::TemporalNegation
            );
            if children.len() != if unary { 1 } else { 2 } {
                return Err(TypingError::InvalidShape(
                    "operator has the wrong number of operands",
                ));
            }
            if operator == OperatorFunction::Negation
                && children[0].value_type.data_type() == &DataType::Null
            {
                return Err(FunctionBindingError::AmbiguousOverload.into());
            }
            let arguments = arguments(children, work)?;
            work.flush()?;
            let binding = match context.catalog.select_operator(
                operator,
                request(&arguments, None),
                work.control(),
            ) {
                Err(FunctionBindingError::NoMatchingOverload) => {
                    if matches!(
                        operator,
                        OperatorFunction::TemporalAdd
                            | OperatorFunction::TemporalSubtract
                            | OperatorFunction::TemporalMultiply
                            | OperatorFunction::TemporalDivide
                            | OperatorFunction::TemporalNegation
                    ) {
                        return Err(TypingError::TypeMismatch(format!(
                            "No matching temporal arithmetic overload for {:?}",
                            operator
                        )));
                    }
                    if operator == OperatorFunction::ArraySubscript {
                        return Err(TypingError::TypeMismatch(format!(
                            "cannot subscript {:?} with {:?}",
                            children[0].value_type.data_type(),
                            children[1].value_type.data_type(),
                        )));
                    }
                    if unary {
                        return Err(TypingError::TypeMismatch(format!(
                            "unary minus requires a supported numeric operand, got {:?}",
                            children[0].value_type.data_type()
                        )));
                    }
                    return Err(TypingError::TypeMismatch(format!(
                        "operator {} does not accept {:?} and {:?}",
                        operator.sql(),
                        children[0].value_type.data_type(),
                        children[1].value_type.data_type()
                    )));
                }
                result => result?,
            };
            type_selected_call(context, binding, None, children, work)?
        }
        NodeShape::UnaryPlus => {
            let [child] = children else {
                return Err(TypingError::InvalidShape("unary plus requires one operand"));
            };
            validate_temporal_unary_plus(&child.value_type)?;
            if child.value_type.logical_type()
                != novarocks_type_contract::ValueLogicalType::Physical
                || !matches!(
                    child.value_type.data_type(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Float32
                        | DataType::Float64
                )
            {
                return Err(TypingError::TypeMismatch(format!(
                    "unary plus requires a signed integer, real, or double operand, got {:?}",
                    child.value_type.data_type(),
                )));
            }
            NodeTyping::Value {
                result: child.value_type.clone(),
                conversions: vec![Conversion::Keep],
            }
        }
        NodeShape::IsNull => {
            if children.len() != 1 {
                return Err(TypingError::InvalidShape("IS NULL requires one operand"));
            }
            NodeTyping::Value {
                result: boolean(false),
                conversions: vec![Conversion::Keep],
            }
        }
        NodeShape::And | NodeShape::Or => {
            if children.len() < 2 {
                return Err(TypingError::InvalidShape(
                    "a logical expression requires at least two terms",
                ));
            }
            let name = if matches!(shape, NodeShape::And) {
                "AND"
            } else {
                "OR"
            };
            let mut conversions = Vec::with_capacity(children.len());
            let mut nullable = false;
            for child in children {
                conversions.push(boolean_operand(context, child, name, work)?);
                nullable |=
                    child.value_type.nullable() || child.value_type.data_type() == &DataType::Null;
            }
            NodeTyping::Value {
                result: boolean(nullable),
                conversions,
            }
        }
        NodeShape::SearchedCase {
            when_count,
            has_else,
        } => type_searched_case(context, children, when_count, has_else, work)?,
        NodeShape::Literal(value) => {
            if !children.is_empty() {
                return Err(TypingError::InvalidShape("a literal has no operands"));
            }
            NodeTyping::Literal(literal::type_literal(context, value, work)?)
        }
    };
    work.flush()?;
    Ok(typed)
}

fn arguments(
    children: &[ChildTyping],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<FunctionArgument>, TypingError> {
    children
        .iter()
        .map(|child| {
            work.step()?;
            Ok(child.argument())
        })
        .collect()
}

fn request<'a>(
    arguments: &'a [FunctionArgument],
    expected_result_type: Option<&'a FunctionValueType>,
) -> FunctionBindingRequest<'a> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type,
    }
}

fn type_catalog_call(
    context: &TypingContext<'_>,
    name: &str,
    expected_result: Option<&FunctionValueType>,
    children: &[ChildTyping],
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeTyping, TypingError> {
    let arguments = arguments(children, work)?;
    work.flush()?;
    let binding = context
        .catalog
        .select_scalar(name, request(&arguments, expected_result), work.control())
        .map_err(|error| route::unavailable_string_binding(name, children, error))?;
    type_selected_call(context, binding, expected_result, children, work)
}

fn type_selected_call(
    context: &TypingContext<'_>,
    binding: ResolvedFunctionBinding,
    expected_result: Option<&FunctionValueType>,
    children: &[ChildTyping],
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeTyping, TypingError> {
    check_shape(&binding, children.len())?;
    let arithmetic = OperatorFunction::from_function_id(&binding.function_id)
        .is_some_and(|operator| operator.arithmetic().is_some());
    let array_concat = binding.function_id.as_str() == "builtin.scalar/concat/v1"
        && matches!(&binding.selected.result_type, FunctionResultType::Scalar(result)
            if matches!(result.data_type(), DataType::List(_)));
    let conversions = binding
        .selected
        .argument_types
        .iter()
        .zip(children)
        .map(|(argument, child)| {
            let FunctionArgumentType::Value(target) = argument else {
                return Err(TypingError::InvalidShape(
                    "selected scalar expects a lambda argument",
                ));
            };
            if array_concat && matches!(target.data_type(), DataType::List(_)) {
                return conversion::array_common(context, child, target, work);
            }
            if arithmetic
                && matches!(child.value_type.data_type(), DataType::Decimal128(..))
                && matches!(target.data_type(), DataType::Float32 | DataType::Float64)
            {
                return conversion::operator_decimal_float(context, child, target, work);
            }
            conversion::coerce_argument(context, child, target, work)
        })
        .collect::<Result<_, _>>()?;
    Ok(NodeTyping::Call {
        selected: SelectedCall {
            binding,
            expected_result: expected_result.cloned(),
        },
        conversions,
    })
}

/// Ask the selected value author for legality and possible successful SQL NULL.
pub fn type_cast(
    context: &TypingContext<'_>,
    source: &FunctionValueType,
    target: FunctionValueType,
    mode: novarocks_type_contract::TrinoCastMode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CarrierConversion, TypingError> {
    use novarocks_functions::{CastPrepareError, PreparedCastRecipe};

    work.flush()?;
    let nullable = PreparedCastRecipe::trino_result_nullable(source, &target, mode, work.control())
        .map_err(|error| {
            if let Some(cause) = error.control_error() {
                return TypingError::Control(cause);
            }
            let message = format!(
                "Cannot cast {:?} to {:?}: {error}",
                source.value_type().kind(),
                target.value_type().kind(),
            );
            match error {
                CastPrepareError::Illegal => TypingError::TypeMismatch(message),
                CastPrepareError::Unsupported => TypingError::Unsupported(message),
                _ => TypingError::Binding(FunctionBindingError::InvalidBinding(message.into())),
            }
        })?;
    Ok(CarrierConversion {
        target: target.with_nullable(nullable)?,
        mode: Some(mode),
        decimal_overflow_policy: context.decimal_overflow_policy,
    })
}

/// Bind the original overload to the actual converted arguments and retain its
/// new result, specialization and admitted ABI as one declaration.
pub fn finish_call(
    context: &TypingContext<'_>,
    selected: &SelectedCall,
    converted: &[ChildTyping],
    work: &mut CompileCheckpoints<'_>,
) -> Result<ResolvedFunctionBinding, TypingError> {
    check_shape(&selected.binding, converted.len())?;
    let arguments = arguments(converted, work)?;
    work.flush()?;
    let first = &selected.binding;
    let exact = context.catalog.select_exact_scalar(
        &first.function_id,
        &first.selected.overload,
        request(&arguments, selected.expected_result.as_ref()),
        work.control(),
    )?;
    check_shape(&exact, converted.len())?;
    if exact.function_id != first.function_id || exact.selected.overload != first.selected.overload
    {
        return Err(TypingError::InvalidShape(
            "scalar binding changed its selected overload",
        ));
    }
    for (argument, child) in exact.selected.argument_types.iter().zip(converted) {
        let FunctionArgumentType::Value(target) = argument else {
            return Err(TypingError::InvalidShape(
                "selected scalar expects a lambda argument",
            ));
        };
        if !target.exactly_equals_checked_observed(&child.value_type, || work.step())? {
            return Err(TypingError::InvalidShape(
                "exact scalar binding changed a converted argument type",
            ));
        }
    }
    work.flush()?;
    Ok(exact)
}

fn check_shape(binding: &ResolvedFunctionBinding, arity: usize) -> Result<(), TypingError> {
    if binding.kind != FunctionKind::Scalar
        || binding.logical_argument_count != arity
        || binding.selected.argument_types.len() != arity
        || !matches!(binding.selected.result_type, FunctionResultType::Scalar(_))
    {
        return Err(TypingError::InvalidShape("invalid scalar binding shape"));
    }
    Ok(())
}

fn boolean(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Boolean, nullable)
}

fn boolean_operand(
    context: &TypingContext<'_>,
    child: &ChildTyping,
    name: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Conversion, TypingError> {
    work.step()?;
    match child.value_type.data_type() {
        DataType::Boolean => Ok(Conversion::Keep),
        DataType::Null if child.form == ChildForm::NullLiteral => {
            conversion::typed_null(context, boolean(true), work)
        }
        actual => Err(TypingError::TypeMismatch(format!(
            "{name} requires a boolean operand, got {actual:?}"
        ))),
    }
}

fn type_searched_case(
    context: &TypingContext<'_>,
    children: &[ChildTyping],
    when_count: usize,
    has_else: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeTyping, TypingError> {
    if when_count == 0
        || when_count
            .checked_mul(2)
            .and_then(|count| count.checked_add(usize::from(has_else)))
            != Some(children.len())
    {
        return Err(TypingError::InvalidShape("invalid searched CASE operands"));
    }
    let (cases, otherwise) = children.as_chunks::<2>();
    let mut result = FunctionValueType::new(DataType::Null, true);
    for branch in cases.iter().map(|[_, branch]| branch).chain(otherwise) {
        work.step()?;
        result = novarocks_type_contract::assignment_common_value_type(
            &result,
            &branch.value_type,
            work,
        )?;
    }
    let mut conversions = Vec::with_capacity(children.len());
    for [condition, branch] in cases {
        conversions.push(boolean_operand(context, condition, "CASE WHEN", work)?);
        conversions.push(conversion::assign(context, branch, &result, work)?);
    }
    for branch in otherwise {
        conversions.push(conversion::assign(context, branch, &result, work)?);
    }
    Ok(NodeTyping::Value {
        result,
        conversions,
    })
}
