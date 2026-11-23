// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Type and compile the migrated scalar syntax against an explicit environment.

use super::register_constant;
use crate::{
    CallAuthorError, CallAuthorInputs, CompiledExprSet, EvaluationEnvironment, ExprCompileError,
    ExprEvaluationFailure, ScopeColumn, ScopeLayout, ScopeLayouts, author_call_controls,
    author_unit_calls, compile_expr_set_pure,
};
use arrow::{datatypes::Schema, error::ArrowError};
use novarocks_constant_contract::{ConstantError, ConstantPolicy, ConstantPools};
use novarocks_expression_contract::{
    BoundExprSet, BoundExprSetBuilder, ExprId, ExprPort, RootScope, UnitError, UnitLimits, ValueId,
};
use novarocks_functions::{KernelFailure, PureEngineFunctionCatalog};
use novarocks_parser::{ParserError, Span, UnsupportedCallSyntax, parser::parse_expression};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    FunctionValueType, PureCompileControl, SemanticParameterProjectionError, SemanticParameterRef,
    SemanticParameters, ValueTypeError, ValueTypeStorageLifetime,
};
use std::{collections::BTreeMap, convert::Infallible, error::Error, fmt, sync::Arc};

mod rows;
mod typing;
pub use rows::{
    CallInapplicability, CheckedRows, Consistency, EvaluationShape, Inapplicability, RowEvaluation,
};

#[derive(Clone, Debug)]
pub struct TextColumn {
    pub name: String,
    pub value: ValueId,
    pub value_type: FunctionValueType,
}

pub struct TextContext<'a> {
    pub functions: &'a PureEngineFunctionCatalog,
    pub constant_policy: ConstantPolicy,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub parameters: &'a SemanticParameters,
    pub allow_throw_exception: SemanticParameterRef,
}

/// One standalone authored root and its actual input projection and environment.
pub struct TypedText<'a> {
    unit: BoundExprSet,
    constants: ConstantPools,
    parameters: SemanticParameters,
    columns: Vec<TextColumn>,
    used_ordinals: Vec<usize>,
    layout: Option<ScopeLayout>,
    functions: &'a PureEngineFunctionCatalog,
    constant_policy: ConstantPolicy,
    decimal_overflow_policy: DecimalOverflowPolicy,
}

impl TypedText<'_> {
    pub fn unit(&self) -> &BoundExprSet {
        &self.unit
    }

    pub fn environment(&self) -> EvaluationEnvironment<'_> {
        EvaluationEnvironment {
            constants: &self.constants,
            parameters: &self.parameters,
        }
    }

    pub fn layout(&self) -> Option<&ScopeLayout> {
        self.layout.as_ref()
    }
}

#[derive(Debug)]
pub enum TextError {
    Parse(ParserError),
    UnknownColumn {
        name: String,
        span: Span,
    },
    NotTypedByLayer {
        construct: String,
        span: Span,
    },
    UnsupportedSyntax(UnsupportedCallSyntax),
    Typing {
        cause: crate::typing::TypingError,
        span: Span,
    },
    Control(CompileControlError),
    Unit(UnitError),
    Constant(ConstantError),
    Parameter(SemanticParameterProjectionError),
    ValueType(ValueTypeError),
    CallAuthor(Box<CallAuthorError>),
    Compile(Box<ExprCompileError>),
    Instance(Box<KernelFailure>),
    Evaluation(Box<ExprEvaluationFailure<Infallible>>),
    Input(ArrowError),
    Layout(&'static str),
    Mismatch {
        row: usize,
        detail: String,
    },
}

impl fmt::Display for TextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownColumn { name, .. } => write!(f, "unknown column {name}"),
            Self::NotTypedByLayer { construct, .. } => {
                write!(f, "{construct} is not typed by the expression layer")
            }
            Self::Layout(message) => f.write_str(message),
            Self::Mismatch { row, detail } => write!(f, "input row {row}: {detail}"),
            other => match other.source() {
                Some(cause) => fmt::Display::fmt(cause, f),
                None => unreachable!("typed text error has a source"),
            },
        }
    }
}

impl Error for TextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse(cause) => Some(cause),
            Self::UnsupportedSyntax(cause) => Some(cause),
            Self::Typing { cause, .. } => Some(cause),
            Self::Control(cause) => Some(cause),
            Self::Unit(cause) => Some(cause),
            Self::Constant(cause) => Some(cause),
            Self::Parameter(cause) => Some(cause),
            Self::ValueType(cause) => Some(cause),
            Self::CallAuthor(cause) => Some(cause.as_ref()),
            Self::Compile(cause) => Some(cause.as_ref()),
            Self::Instance(cause) => Some(cause.as_ref()),
            Self::Evaluation(cause) => Some(cause.as_ref()),
            Self::Input(cause) => Some(cause),
            _ => None,
        }
    }
}

impl From<CompileControlError> for TextError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<UnitError> for TextError {
    fn from(cause: UnitError) -> Self {
        match cause {
            UnitError::Control(cause) => Self::Control(cause),
            cause => Self::Unit(cause),
        }
    }
}
impl From<ConstantError> for TextError {
    fn from(cause: ConstantError) -> Self {
        match cause {
            ConstantError::Control(cause) => Self::Control(cause),
            cause => Self::Constant(cause),
        }
    }
}
impl From<CallAuthorError> for TextError {
    fn from(cause: CallAuthorError) -> Self {
        match cause {
            CallAuthorError::Control(cause) => Self::Control(cause),
            cause => Self::CallAuthor(Box::new(cause)),
        }
    }
}
impl From<ExprCompileError> for TextError {
    fn from(cause: ExprCompileError) -> Self {
        match cause {
            ExprCompileError::Control(cause) => Self::Control(cause),
            cause => Self::Compile(Box::new(cause)),
        }
    }
}

pub fn type_expression_text<'a>(
    text: &str,
    columns: &[TextColumn],
    context: &TextContext<'a>,
    control: &dyn PureCompileControl,
) -> Result<TypedText<'a>, TextError> {
    control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
    let expression = parse_expression(text).map_err(TextError::Parse)?;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = type_parsed(&expression, columns, context, &mut work);
    work.finish_result(result, TextError::Control)
}

fn type_parsed<'a>(
    expression: &novarocks_parser::ast::Expr,
    columns: &[TextColumn],
    context: &TextContext<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<TypedText<'a>, TextError> {
    let mut input = typing::TextTyping::new(columns, context, work)?;
    let root = input.lower(expression, work, 1)?;
    let used_ordinals = input.used_ordinals();
    let mut layout_columns = Vec::with_capacity(used_ordinals.len());
    let mut fields = Vec::with_capacity(used_ordinals.len());
    for (position, &ordinal) in used_ordinals.iter().enumerate() {
        work.step()?;
        let column = &columns[ordinal];
        let ty = column.value_type.clone();
        fields.push(
            ty.try_to_field(&column.name)
                .map_err(TextError::ValueType)?,
        );
        layout_columns.push(ScopeColumn {
            ordinal: u32::try_from(position).map_err(|_| CompileControlError::ResourceExhausted)?,
            value: column.value,
            ty,
        });
    }
    let (ports, layout, scope) = if layout_columns.is_empty() {
        (Vec::new(), None, RootScope::AllPorts)
    } else {
        let values = layout_columns
            .iter()
            .map(|column| (column.value, column.ty.clone()))
            .collect();
        let layout = ScopeLayout::try_new(
            Arc::new(Schema::new(fields)),
            layout_columns.iter().map(|c| c.ty.clone()).collect(),
            layout_columns.into(),
            ValueTypeStorageLifetime::pure(),
            work,
        )?;
        (vec![ExprPort { values }], Some(layout), RootScope::Port(0))
    };
    let mut draft = BoundExprSetBuilder::new(unit_limits());
    let mut constants = ConstantPools::empty();
    let root = root.emit(&mut draft, &mut constants, context, work)?;
    draft.root(root, EvaluationDemand::Value, scope)?;
    work.flush()?;
    let parameters = context
        .parameters
        .project_observed(
            draft.definitions().flat_map(|node| {
                let call_refs = match &node.kind {
                    novarocks_expression_contract::ExprKind::FunctionCall { function, .. } => {
                        function
                            .legacy_metadata
                            .as_ref()
                            .map_or(&[][..], |info| info.semantic_parameters.as_ref())
                    }
                    _ => &[],
                };
                node.kind
                    .intrinsic_parameter_references()
                    .chain(call_refs)
                    .copied()
            }),
            CompilePhase::FunctionSpecialization,
            work.control(),
        )
        .map_err(|error| match error {
            SemanticParameterProjectionError::Control(cause) => TextError::Control(cause),
            error => TextError::Parameter(error),
        })?;
    let unit = freeze(
        draft,
        ports,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        context.functions,
        context.constant_policy,
        context.decimal_overflow_policy,
        work,
    )?;
    Ok(TypedText {
        unit,
        constants,
        parameters,
        columns: columns.to_vec(),
        used_ordinals,
        layout,
        functions: context.functions,
        constant_policy: context.constant_policy,
        decimal_overflow_policy: context.decimal_overflow_policy,
    })
}

fn unit_limits() -> UnitLimits {
    UnitLimits {
        expression_semantic_depth: crate::MAX_STATIC_EXPRESSION_DEPTH,
    }
}

fn freeze(
    mut draft: BoundExprSetBuilder,
    ports: Vec<ExprPort>,
    environment: EvaluationEnvironment<'_>,
    functions: &PureEngineFunctionCatalog,
    constant_policy: ConstantPolicy,
    decimal_overflow_policy: DecimalOverflowPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundExprSet, TextError> {
    let decimal_overflow = draft
        .requests()
        .keys()
        .copied()
        .map(|id| (id, decimal_overflow_policy))
        .collect::<BTreeMap<ExprId, DecimalOverflowPolicy>>();
    let inputs = CallAuthorInputs {
        environment,
        constant_policy,
        functions,
        decimal_overflow: &decimal_overflow,
    };
    for (definition, control) in author_call_controls(&draft, &inputs, work)? {
        draft.definition_control(definition, control)?;
    }
    let sealed = draft.seal(ports, work)?;
    let claims = author_unit_calls(&sealed, &inputs, work)?.claims;
    Ok(sealed.finish(claims, work)?)
}

fn compile(
    text: &TypedText<'_>,
    unit: &BoundExprSet,
    constants: &ConstantPools,
    layout: Option<&ScopeLayout>,
    control: &dyn PureCompileControl,
) -> Result<Arc<CompiledExprSet>, TextError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let compiled = compile_expr_set_pure(
        unit,
        EvaluationEnvironment {
            constants,
            parameters: &text.parameters,
        },
        ScopeLayouts {
            ports: layout.map_or(&[], std::slice::from_ref),
            all_ports: None,
        },
        text.constant_policy,
        text.functions,
        &mut work,
    )
    .map_err(TextError::from);
    work.finish_result(compiled, TextError::Control)
}
