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

use super::{ScalarArena, ScalarId, ScalarNode};

use crate::analysis::scalar_effects::{
    ScalarEffectQuery, ScalarEffects, call_demands_values, call_effects,
};
use crate::compiler::{SqlCompileError, SqlFunctionCatalog};
use novarocks_type_contract::{FunctionValueType, PureCompileControl};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(super) struct EffectEnvironment {
    functions: Arc<dyn SqlFunctionCatalog>,
    allow_throw_exception: bool,
}

impl ScalarArena {
    /// The arena is scoped to the original SQL statement and installed
    /// catalogue. New definitions use that same environment as the old ones.
    pub(crate) fn bind_effect_environment(
        &mut self,
        functions: Arc<dyn SqlFunctionCatalog>,
        allow_throw_exception: bool,
        control: &dyn PureCompileControl,
    ) -> Result<(), SqlCompileError> {
        let environment = EffectEnvironment {
            functions,
            allow_throw_exception,
        };
        let query = environment.query(control);
        let mut effects = Vec::new();
        effects
            .try_reserve_exact(self.nodes.len())
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        for (node, result) in self.nodes.iter().zip(&self.value_types) {
            effects.push(definition_effects(self, &effects, node, result, &query)?);
        }
        self.definition_effects = effects;
        self.effect_environment = Some(environment);
        Ok(())
    }

    pub(crate) fn call_argument_control(
        &self,
        id: ScalarId,
        control: &dyn PureCompileControl,
    ) -> Result<Option<novarocks_type_contract::ArgumentControl>, SqlCompileError> {
        let ScalarNode::FunctionCall { binding, .. } = self.node(id) else {
            return Ok(None);
        };
        Ok(self
            .scalar_effect_query(control)
            .scalar_call(binding)?
            .map(|call| call.argument_control))
    }

    pub(crate) fn comparison_casts_can_share(
        &self,
        left: &FunctionValueType,
        right: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<bool, SqlCompileError> {
        self.scalar_effect_query(control)
            .comparison_casts_can_share(left, right)
    }

    fn scalar_effect_query<'a>(
        &'a self,
        control: &'a dyn PureCompileControl,
    ) -> ScalarEffectQuery<'a> {
        match &self.effect_environment {
            Some(environment) => environment.query(control),
            None => ScalarEffectQuery {
                functions: None,
                allow_throw_exception: true,
                control,
            },
        }
    }

    pub(crate) fn effects(&self, id: ScalarId) -> ScalarEffects {
        self.definition_effects[id.0 as usize]
    }

    pub(super) fn new_definition_effects(
        &self,
        node: &ScalarNode,
        ty: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ScalarEffects, SqlCompileError> {
        let Some(environment) = &self.effect_environment else {
            return Ok(match node {
                ScalarNode::ColumnRef(_)
                | ScalarNode::LambdaParamRef { .. }
                | ScalarNode::Literal(_)
                | ScalarNode::Constant(_) => ScalarEffects::PURE,
                _ => ScalarEffects::UNKNOWN,
            });
        };
        definition_effects(
            self,
            &self.definition_effects,
            node,
            ty,
            &environment.query(control),
        )
    }
}

impl EffectEnvironment {
    fn query<'a>(&'a self, control: &'a dyn PureCompileControl) -> ScalarEffectQuery<'a> {
        ScalarEffectQuery {
            functions: Some(self.functions.as_ref()),
            allow_throw_exception: self.allow_throw_exception,
            control,
        }
    }
}

fn definition_effects(
    arena: &ScalarArena,
    cached: &[ScalarEffects],
    node: &ScalarNode,
    result: &FunctionValueType,
    query: &ScalarEffectQuery<'_>,
) -> Result<ScalarEffects, SqlCompileError> {
    let child = |id: ScalarId| cached[id.0 as usize];
    let ty = |id: ScalarId| arena.value_type(id);
    fn join(effects: impl IntoIterator<Item = ScalarEffects>) -> ScalarEffects {
        effects
            .into_iter()
            .fold(ScalarEffects::PURE, ScalarEffects::join)
    }
    query
        .control
        .checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let effects = match node {
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::Constant(_) => ScalarEffects::PURE,
        ScalarNode::BinaryOp {
            op,
            left,
            right,
            decimal_overflow_policy,
        } => query
            .binary(*op, ty(*left), ty(*right), result, *decimal_overflow_policy)?
            .join(child(*left))
            .join(child(*right)),
        ScalarNode::Cast {
            child: operand,
            cast_mode,
            ..
        } => query
            .cast(ty(*operand), result, *cast_mode)?
            .join(child(*operand)),
        ScalarNode::FunctionCall { args, binding, .. } => {
            let Some(mut own) = query.scalar_call(binding)? else {
                return Ok(ScalarEffects::UNKNOWN);
            };
            if own.own_effects.may_raise_row_error
                && super::super::scalar_expr::prove_signed_integer_operator_total(
                    arena, result, binding, args,
                )
                .is_some()
            {
                own.own_effects.may_raise_row_error = false;
            }
            let own_effects = call_effects(own, &binding.function_id);
            if call_demands_values(own.argument_control) {
                own_effects.join(join(args.iter().copied().map(child)))
            } else {
                own_effects
            }
        }
        ScalarNode::InList {
            child: source,
            list,
            ..
        } => query
            .in_list(ty(*source), list.iter().copied().map(ty), result)?
            .join(child(*source))
            .join(join(list.iter().copied().map(child))),
        ScalarNode::Between {
            child: source,
            low,
            high,
            negated,
        } => query
            .between(ty(*source), ty(*low), ty(*high), result, *negated)?
            .join(child(*source))
            .join(child(*low))
            .join(child(*high)),
        ScalarNode::Like {
            child: source,
            pattern,
            negated,
        } => query
            .like(ty(*source), ty(*pattern), result, *negated)?
            .join(child(*source))
            .join(child(*pattern)),
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            let own = if let Some(operand) = operand {
                query.simple_case(
                    ty(*operand),
                    when_then.iter().map(|(when, _)| ty(*when)),
                    when_then
                        .iter()
                        .map(|(_, then)| ty(*then))
                        .chain(else_expr.iter().copied().map(ty)),
                    result,
                )?
            } else {
                ScalarEffects::PURE
            };
            own.join(join(
                operand
                    .iter()
                    .copied()
                    .chain(when_then.iter().flat_map(|&(when, then)| [when, then]))
                    .chain(else_expr.iter().copied())
                    .map(child),
            ))
        }
        ScalarNode::AggregateCall {
            args,
            order_by,
            filter,
            ..
        } => join(
            args.iter()
                .copied()
                .chain(order_by.iter().map(|sort| sort.expr))
                .chain(filter.iter().copied())
                .map(child),
        ),
        ScalarNode::WindowCall {
            args,
            order_by,
            function_order_by,
            partition_by,
            ..
        } => join(
            args.iter()
                .copied()
                .chain(partition_by.iter().copied())
                .chain(
                    order_by
                        .iter()
                        .chain(function_order_by)
                        .map(|sort| sort.expr),
                )
                .map(child),
        ),
        ScalarNode::UnaryOp { child: input, .. }
        | ScalarNode::IsNull { child: input, .. }
        | ScalarNode::IsTruthValue { child: input, .. }
        | ScalarNode::Nested(input)
        | ScalarNode::LambdaFunction { body: input, .. }
        | ScalarNode::Lambda { body: input, .. } => child(*input),
    };
    Ok(effects)
}
