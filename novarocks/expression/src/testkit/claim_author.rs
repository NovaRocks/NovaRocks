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
    BoundExprSet, BoundExprSetBuilder, BoundFunction, DefinitionControl, ExprId, ExprKind,
    ExpressionCallClaim, LegacyBindingMetadata, PhysicalCallRequest, SealedUnit,
};
use novarocks_function_contract::FunctionArgument as StaticFunctionArgument;
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, FunctionArgument, FunctionBindingRequest,
    FunctionBindingSelection, FunctionKind, FunctionResultType, PureCallPreparation,
    PureEngineFunctionCatalog, ResolvedFunctionBinding, ScopedExpressionEffects,
    TemporalSourceChannel,
};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileCheckpoints, ControlShape, DecimalOverflowPolicy,
    ExpressionInvocation, FunctionValueType, PureCompileControl, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

/// The first resolver request and selected owner of a checked call definition.
pub struct ClaimAuthor {
    function: BoundFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    constant_policy: ConstantPolicy,
    decimal_policy: DecimalOverflowPolicy,
    expected_result_type: Option<FunctionValueType>,
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
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    work: &mut CompileCheckpoints<'_>,
) {
    for (&definition, owner) in authors {
        let temporal_sources = if let ControlShape::TemporalSource(shape) = owner.shape {
            let ExprKind::FunctionCall { args, .. } = &draft.definition(definition).unwrap().kind
            else {
                panic!("original temporal call definition")
            };
            Some(
                novarocks_expression_contract::temporal_source_definitions_from_lookup_observed(
                    shape.kind(),
                    |id| draft.definition(id),
                    args,
                    work,
                )
                .unwrap(),
            )
        } else {
            None
        };
        draft
            .definition_control(
                definition,
                DefinitionControl {
                    shape: owner.shape,
                    temporal_sources,
                },
            )
            .unwrap();
    }
}

/// Complete the actual uses of a unit from its sealed control graph.
pub fn finish_unit(
    functions: &PureEngineFunctionCatalog,
    sealed: SealedUnit,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    parameters: &SemanticParameters,
    work: &mut CompileCheckpoints<'_>,
    mut primitive_effects: impl FnMut(
        &SealedUnit,
        &ExpressionInvocation<ExprId>,
        ScopedExpressionEffects,
    ) -> ScopedExpressionEffects,
) -> BoundExprSet {
    let flow = sealed.control_flow();
    let mut summaries = BTreeMap::new();
    let mut claims = BTreeMap::new();
    // Consume only the SDK's actual root/argument graph. Use IDs remain
    // opaque and never determine dependency or preparation order.
    let mut pending = sealed
        .root_use_ids()
        .iter()
        .rev()
        .map(|&id| (id, false))
        .collect::<Vec<_>>();
    let mut ordered = Vec::new();
    while let Some((id, complete)) = pending.pop() {
        if complete {
            ordered.push(id);
        } else {
            pending.push((id, true));
            for &child in sealed.uses()[&id].arguments.iter().rev() {
                pending.push((child, false));
            }
        }
        work.step().unwrap();
    }
    for id in ordered {
        let invocation = &sealed.uses()[&id];
        let context = invocation.context;
        let mut children = ScopedExpressionEffects::pure_value(context);
        for (ordinal, child) in invocation.arguments.iter().enumerate() {
            children = children
                .join_control_argument(summaries[child], flow, ordinal)
                .unwrap();
        }
        let summary = if let Some(owner) = authors.get(&invocation.definition) {
            let argument_uses = if owner.shape == ControlShape::NoArguments {
                vec![None; owner.arguments.len()]
            } else {
                invocation
                    .arguments
                    .iter()
                    .map(|id| Some(*id))
                    .collect::<Vec<_>>()
            };
            let preparation = if matches!(
                owner.shape,
                ControlShape::If | ControlShape::Coalesce | ControlShape::TemporalSource(_)
            ) {
                PureCallPreparation::ControlIntrinsic {
                    arguments: children,
                }
            } else {
                PureCallPreparation::Scalar {
                    arguments: children,
                }
            };
            let source_plan = sealed.temporal_source(context.use_id).cloned();
            let source_channels = source_plan
                .as_ref()
                .map(|source| {
                    source
                        .sources
                        .iter()
                        .map(|channel| TemporalSourceChannel {
                            role: channel.role,
                            context: flow.uses()[&channel.use_id].context,
                            value_type: &sealed.definition(channel.definition).unwrap().ty,
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let definition = sealed.definition(invocation.definition).unwrap();
            let ExprKind::FunctionCall { args, .. } = &definition.kind else {
                panic!("actual authored call")
            };
            let regexp_count_pattern_source =
                if owner.function.function_id.as_str() == "builtin.scalar/regexp_count/v1" {
                    Some(
                        novarocks_expression_contract::regexp_count_pattern_source_observed(
                            sealed.definitions(),
                            args,
                            work,
                        )
                        .unwrap(),
                    )
                } else {
                    None
                };
            let to_base64_byte_source =
                if owner.function.function_id.as_str() == "builtin.scalar/to_base64/v1" {
                    Some(
                        novarocks_expression_contract::to_base64_byte_source_observed(
                            sealed.definitions(),
                            args,
                            work,
                        )
                        .unwrap(),
                    )
                } else {
                    None
                };
            let token = functions
                .prepare_fresh(
                    CallEffectInput {
                        context: context.into(),
                        argument_uses: match &source_plan {
                            Some(source) => CallArgumentUses::TemporalSources {
                                facts: &source.facts,
                                channels: &source_channels,
                            },
                            None => match regexp_count_pattern_source {
                                Some(source) => CallArgumentUses::RegexpCountPattern {
                                    source,
                                    channels: &argument_uses,
                                },
                                None => match to_base64_byte_source {
                                    Some(source) => CallArgumentUses::ToBase64Bytes {
                                        source,
                                        channels: &argument_uses,
                                    },
                                    None => CallArgumentUses::SelectedChannels(&argument_uses),
                                },
                            },
                        },
                        function_id: &owner.function.function_id,
                        kind: FunctionKind::Scalar,
                        selected: owner.selected.as_ref(),
                        request: owner.request(),
                        environment: &[],
                        parameters,
                        decimal_overflow_policy: owner.decimal_policy,
                        proof_scope: CallProofScope::Region(context.region),
                    },
                    owner.selected.clone(),
                    preparation,
                    work.control(),
                )
                .unwrap();
            claims.insert(
                context.use_id,
                ExpressionCallClaim {
                    context,
                    effects: token.call_contract().effects().clone(),
                    decimal_overflow_policy: owner.decimal_policy,
                    regexp_count_pattern_source,
                    to_base64_byte_source,
                    temporal_source: source_plan,
                },
            );
            ScopedExpressionEffects::primitive(context, token.effects().for_use(context).unwrap())
        } else {
            primitive_effects(&sealed, invocation, children)
        };
        summaries.insert(context.use_id, summary);
    }
    sealed.finish(claims, work).unwrap()
}
