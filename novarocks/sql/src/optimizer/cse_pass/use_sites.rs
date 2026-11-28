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

use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;
use novarocks_type_contract::{
    BetweenSourceRole, CompileCheckpoints, CompilePhase, ControlShape, EvaluationDemand,
    NativeBetweenPlan, PureCompileControl, control_argument_semantics,
};

use super::{child_ids, eligible, is_complex_data_type, scalar_contains_column_ref, subtree_size};
use crate::common::BinOp;
use crate::compiler::SqlCompileError;
use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

#[derive(Clone, Copy)]
pub(super) struct UseRoot {
    pub(super) expr: ScalarId,
    demand: EvaluationDemand,
    guarded: bool,
}

impl UseRoot {
    pub(super) fn value(expr: ScalarId) -> Self {
        Self::new(expr, EvaluationDemand::Value)
    }

    pub(super) fn new(expr: ScalarId, demand: EvaluationDemand) -> Self {
        Self {
            expr,
            demand,
            guarded: false,
        }
    }

    pub(super) fn selected_value(expr: ScalarId) -> Self {
        Self {
            guarded: true,
            ..Self::value(expr)
        }
    }
}

struct Occurrences {
    first: usize,
    count: usize,
    can_read_projected_value: bool,
}

/// Discover runtime uses rather than treating interned definitions as
/// evaluation sites. A definition-wide substitution is allowed only if each
/// reachable occurrence it will replace can consume the same projected value.
#[derive(Default)]
pub(super) struct UseSites {
    occurrences: HashMap<ScalarId, Occurrences>,
    unchanged_call_arguments: HashSet<ScalarId>,
    call_shapes: HashMap<ScalarId, Option<ControlShape>>,
}

impl UseSites {
    pub(super) fn inspect(
        scalars: &ScalarArena,
        roots: impl IntoIterator<Item = UseRoot>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, SqlCompileError> {
        let mut sites = Self::default();
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        for root in roots {
            sites.visit(scalars, root, None, &mut work)?;
        }
        work.finish()?;
        Ok(sites)
    }

    pub(super) fn candidates(&self, scalars: &ScalarArena) -> Vec<ScalarId> {
        let mut candidates = self
            .occurrences
            .iter()
            .filter_map(|(&id, uses)| {
                (uses.count >= 2 && uses.can_read_projected_value && eligible(scalars, id))
                    .then_some((id, uses.first))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|&(id, first)| (subtree_size(scalars, id), first));
        candidates.into_iter().map(|(id, _)| id).collect()
    }

    pub(super) fn unchanged_call_arguments(&self) -> &HashSet<ScalarId> {
        &self.unchanged_call_arguments
    }

    #[recursive::recursive]
    fn visit(
        &mut self,
        scalars: &ScalarArena,
        site: UseRoot,
        flattened_by: Option<BinOp>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlCompileError> {
        work.step()?;
        let id = site.expr;
        let node = scalars.node(id);
        if let ScalarNode::Nested(child) = node {
            return self.visit(
                scalars,
                UseRoot {
                    expr: *child,
                    ..site
                },
                None,
                work,
            );
        }
        let child_shape = self.child_shape(scalars, id, work)?;
        let connective = boolean_connective(node);
        if connective.is_none() || connective != flattened_by {
            let first = self.occurrences.len();
            let occurrences = self.occurrences.entry(id).or_insert(Occurrences {
                first,
                count: 0,
                can_read_projected_value: true,
            });
            occurrences.count += 1;
            // Materialized VALUE preserves Boolean NULL for Value readers.
            // An existing TruthOnly reader can still discard NULL.
            occurrences.can_read_projected_value &= child_shape.is_some()
                && !site.guarded
                && (site.demand == EvaluationDemand::Value
                    || scalars.data_type(id) == &DataType::Boolean);
        }

        let Some(shape) = child_shape else {
            return Ok(());
        };
        if matches!(shape, ControlShape::TypeOnly | ControlShape::NoArguments) {
            return Ok(());
        }

        let children = match node {
            // The expression author reads the source once per comparison.
            ScalarNode::Between {
                child,
                low,
                high,
                negated,
            } => NativeBetweenPlan::new(*negated)
                .sources()
                .into_iter()
                .map(|role| match role {
                    BetweenSourceRole::Operand => *child,
                    BetweenSourceRole::Lower => *low,
                    BetweenSourceRole::Upper => *high,
                })
                .collect(),
            _ => child_ids(scalars, id),
        };
        let guard_complex_in_source = matches!(node, ScalarNode::InList { child, .. }
            if is_complex_data_type(scalars.data_type(*child))
                && !scalar_contains_column_ref(scalars, *child));

        for (ordinal, &child) in children.iter().enumerate() {
            let (demand, guard) =
                control_argument_semantics(shape, children.len(), ordinal, site.demand)
                    .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
            self.visit(
                scalars,
                UseRoot {
                    expr: child,
                    demand,
                    guarded: site.guarded
                        || guard.is_some()
                        || (ordinal == 0 && guard_complex_in_source),
                },
                connective,
                work,
            )?;
        }
        Ok(())
    }

    fn child_shape(
        &mut self,
        scalars: &ScalarArena,
        id: ScalarId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<ControlShape>, SqlCompileError> {
        Ok(Some(match scalars.node(id) {
            ScalarNode::BinaryOp { op: BinOp::And, .. } => ControlShape::Conjunction,
            ScalarNode::BinaryOp { op: BinOp::Or, .. } => ControlShape::Disjunction,
            ScalarNode::InList { negated, .. } => ControlShape::Membership { negated: *negated },
            ScalarNode::Between { negated, .. } => ControlShape::Between { negated: *negated },
            ScalarNode::Case {
                operand,
                when_then,
                else_expr,
                ..
            } => ControlShape::Case {
                simple: operand.is_some(),
                arms: u32::try_from(when_then.len())
                    .map_err(|_| SqlCompileError::ResourceExhausted)?,
                has_else: else_expr.is_some(),
            },
            ScalarNode::FunctionCall { args, .. } => {
                if let Some(&shape) = self.call_shapes.get(&id) {
                    return Ok(shape);
                }
                work.flush()?;
                let shape = match scalars.call_argument_control(id, work.control())? {
                    Some(control) => {
                        novarocks_expression::installed_scalar_control_shape(control, args.len())
                    }
                    None => None,
                };
                if matches!(
                    shape,
                    None | Some(ControlShape::NoArguments | ControlShape::TypeOnly)
                ) {
                    self.unchanged_call_arguments.insert(id);
                }
                self.call_shapes.insert(id, shape);
                return Ok(shape);
            }
            // These definitions have a different consumer or a lexical
            // invocation. The physical drivers visit their actual value
            // roots separately where the operator exposes those roots.
            ScalarNode::AggregateCall { .. }
            | ScalarNode::WindowCall { .. }
            | ScalarNode::Lambda { .. }
            | ScalarNode::LambdaFunction { .. } => return Ok(None),
            _ => ControlShape::Eager,
        }))
    }
}

/// Emission folds adjacent binary nodes with the same Boolean connective
/// into one ordered invocation. Parentheses stop that syntactic flattening.
pub(super) fn boolean_connective(node: &ScalarNode) -> Option<BinOp> {
    match node {
        ScalarNode::BinaryOp {
            op: op @ (BinOp::And | BinOp::Or),
            ..
        } => Some(*op),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
