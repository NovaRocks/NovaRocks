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

//! Checked call declarations built from actual Functions preparation.
use super::register_constant;
use novarocks_constant_contract::{ConstantPolicy, ConstantPools};
use novarocks_expression_contract::{
    BoundExprSet, BoundExprSetBuilder, BoundFunction, ExprId, ExprKind, LegacyBindingMetadata,
    PhysicalCallRequest, SealedUnit,
};
use novarocks_function_contract::FunctionArgument as StaticFunctionArgument;
use novarocks_functions::{
    FunctionArgument, FunctionBindingRequest, FunctionBindingSelection, FunctionKind,
    FunctionResultType, PureEngineFunctionCatalog, ResolvedFunctionBinding,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, ControlShape, DecimalOverflowPolicy, FunctionValueType,
    PureCompileControl, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

/// The first resolver request and selected owner of a checked call definition.
pub struct ClaimAuthor {
    function: BoundFunction,
    #[expect(
        dead_code,
        reason = "the installed catalogue reconstructs this selection when authorship runs"
    )]
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    constant_policy: ConstantPolicy,
    decimal_policy: DecimalOverflowPolicy,
    expected_result_type: Option<FunctionValueType>,
    #[expect(
        dead_code,
        reason = "declared fixture shapes are not the control author; the installed catalogue is"
    )]
    shape: ControlShape,
}

impl ClaimAuthor {
    /// Preserve fixtures constructed through the legacy binding metadata path.
    pub fn with_legacy_binding(
        functions: &PureEngineFunctionCatalog,
        name: &str,
        arguments: Vec<FunctionArgument>,
        shape: ControlShape,
        expected_result_type: Option<FunctionValueType>,
        constant_policy: ConstantPolicy,
        control: &dyn PureCompileControl,
    ) -> Self {
        let bound = resolve(
            functions,
            name,
            &arguments,
            expected_result_type.as_ref(),
            control,
        );
        Self::from_resolved_legacy_binding(
            bound,
            arguments,
            shape,
            expected_result_type,
            constant_policy,
        )
    }

    pub(super) fn from_resolved_legacy_binding(
        bound: ResolvedFunctionBinding,
        arguments: Vec<FunctionArgument>,
        shape: ControlShape,
        expected_result_type: Option<FunctionValueType>,
        constant_policy: ConstantPolicy,
    ) -> Self {
        let selected = Arc::new(bound.selected);
        let FunctionResultType::Scalar(result) = &selected.result_type else {
            panic!("scalar owner")
        };
        let function = BoundFunction {
            function_id: bound.function_id,
            overload: selected.overload.clone(),
            kind: bound.kind,
            argument_types: selected.argument_types.clone(),
            result_type: result.clone(),
            legacy_metadata: Some(LegacyBindingMetadata {
                volatility: bound.semantics.volatility,
                argument_evaluation: bound.semantics.argument_evaluation,
                failure_behavior: bound.semantics.failure_behavior,
                intrinsic_row_error: bound.semantics.intrinsic_row_error,
                semantic_parameters: Box::default(),
            }),
        };
        Self {
            function,
            selected,
            arguments,
            constant_policy,
            decimal_policy: DecimalOverflowPolicy::ReportError,
            expected_result_type,
            shape,
        }
    }

    /// Keep exact signature bindings and require the installed owner to declare eagerness.
    pub fn with_exact_eager_binding(
        functions: &PureEngineFunctionCatalog,
        name: &str,
        arguments: Vec<FunctionArgument>,
        constant_policy: ConstantPolicy,
        control: &dyn PureCompileControl,
    ) -> Self {
        let bound = resolve(functions, name, &arguments, None, control);
        let selected = Arc::new(bound.selected);
        let FunctionResultType::Scalar(result) = &selected.result_type else {
            panic!("scalar owner")
        };
        let declaration = functions
            .metadata()
            .pure_overload_declaration_observed(
                &bound.function_id,
                bound.kind,
                &selected.overload,
                control,
            )
            .unwrap();
        assert_eq!(
            declaration.effects().argument_control,
            ArgumentControl::Eager,
            "the exact scalar owner must declare eager control",
        );
        let function = BoundFunction::from_exact_signature(
            bound.function_id,
            selected.overload.clone(),
            bound.kind,
            selected.argument_types.clone(),
            result.clone(),
        );
        Self {
            function,
            selected,
            arguments,
            constant_policy,
            decimal_policy: DecimalOverflowPolicy::ReportError,
            expected_result_type: None,
            shape: ControlShape::Eager,
        }
    }

    pub fn with_decimal_policy(mut self, policy: DecimalOverflowPolicy) -> Self {
        self.decimal_policy = policy;
        self
    }

    pub fn function(&self) -> &BoundFunction {
        &self.function
    }

    pub fn result(&self) -> FunctionValueType {
        self.function.result_type.clone()
    }

    fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.arguments.len(),
            expected_result_type: self.expected_result_type.as_ref(),
        }
    }
}

fn resolve(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: &[FunctionArgument],
    expected_result_type: Option<&FunctionValueType>,
    control: &dyn PureCompileControl,
) -> ResolvedFunctionBinding {
    let request = FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type,
    };
    let metadata = functions.metadata();
    if expected_result_type.is_some() {
        metadata.resolve_bound_trusted(name, FunctionKind::Scalar, request, control)
    } else {
        metadata.resolve_bound_user(name, FunctionKind::Scalar, request, control)
    }
    .unwrap()
}

pub fn add_call(
    draft: &mut BoundExprSetBuilder,
    authors: &mut BTreeMap<ExprId, ClaimAuthor>,
    owner: ClaimAuthor,
    arguments: Vec<ExprId>,
) -> ExprId {
    let id = draft
        .add(
            None,
            owner.result(),
            ExprKind::FunctionCall {
                function: owner.function.clone(),
                args: arguments.into_boxed_slice(),
            },
        )
        .unwrap();
    authors.insert(id, owner);
    id
}

/// Freeze the original requests using the explicit constant table of the caller.
pub fn register_call_requests(
    draft: &mut BoundExprSetBuilder,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    constants: &mut ConstantPools,
) {
    for (&definition, owner) in authors {
        let original = owner.request();
        let arguments = original
            .arguments
            .iter()
            .map(|argument| match argument {
                FunctionArgument::Value {
                    value_type,
                    constant,
                } => StaticFunctionArgument::Value {
                    value_type: value_type.clone(),
                    constant: constant
                        .as_ref()
                        .map(|value| register_constant(constants, value)),
                },
                FunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                } => StaticFunctionArgument::Lambda {
                    parameter_types: parameter_types.clone(),
                    result_type: result_type.clone(),
                },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        draft
            .request(
                definition,
                PhysicalCallRequest {
                    arguments,
                    logical_argument_count: original.logical_argument_count,
                    expected_result_type: original.expected_result_type.cloned(),
                    constant_policy: owner.constant_policy,
                },
            )
            .unwrap();
    }
}

pub fn declare_call_controls(
    draft: &mut BoundExprSetBuilder,
    functions: &PureEngineFunctionCatalog,
    constants: &ConstantPools,
    parameters: &SemanticParameters,
    policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) {
    let inputs = crate::CallAuthorInputs {
        environment: crate::EvaluationEnvironment {
            constants,
            parameters,
        },
        constant_policy: policy,
        functions,
        decimal_overflow: &BTreeMap::new(),
    };
    for (definition, control) in crate::author_call_controls(draft, &inputs, work).unwrap() {
        draft.definition_control(definition, control).unwrap();
    }
}

/// Complete the actual uses of a unit from its sealed control graph.
pub fn finish_unit(
    functions: &PureEngineFunctionCatalog,
    sealed: SealedUnit,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    constants: &ConstantPools,
    parameters: &SemanticParameters,
    policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> BoundExprSet {
    try_finish_unit(
        functions, sealed, authors, constants, parameters, policy, work,
    )
    .expect("call author")
}

/// Same completion as [`finish_unit`], preserving a recipe refusal for the caller.
pub fn try_finish_unit(
    functions: &PureEngineFunctionCatalog,
    sealed: SealedUnit,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    constants: &ConstantPools,
    parameters: &SemanticParameters,
    policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundExprSet, crate::CallAuthorError> {
    let decimal_overflow = authors
        .iter()
        .map(|(&definition, owner)| (definition, owner.decimal_policy))
        .collect::<BTreeMap<_, _>>();
    let inputs = crate::CallAuthorInputs {
        environment: crate::EvaluationEnvironment {
            constants,
            parameters,
        },
        constant_policy: policy,
        functions,
        decimal_overflow: &decimal_overflow,
    };
    let authored = crate::author_unit_calls(&sealed, &inputs, work)?;
    Ok(sealed.finish(authored.claims, work).unwrap())
}
