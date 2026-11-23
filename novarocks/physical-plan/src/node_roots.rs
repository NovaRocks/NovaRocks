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

//! The node owner supplies root purpose and input scope; units supply actual
//! expression occurrences. No fragment-wide expression identity is introduced.
use crate::{AggregateCall, Fragment, NodeKind, PhysicalNode, RootOrdinal, RootScope, ValueOrigin};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, EvaluationDemand};

#[derive(Clone, Copy, Debug)]
pub(crate) struct NodeRootRequirement {
    pub root: RootOrdinal,
    pub demand: EvaluationDemand,
    pub scope: RootScope,
    pub constant_only: bool,
}
#[derive(Debug)]
pub(crate) enum NodeRootError {
    Control(CompileControlError),
    InvalidScanDerived(crate::ValueId),
}
impl From<CompileControlError> for NodeRootError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
pub(crate) fn visit_node_roots(
    fragment: &Fragment,
    node: &PhysicalNode,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(
        NodeRootRequirement,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), CompileControlError>,
) -> Result<(), NodeRootError> {
    use EvaluationDemand::{TruthOnly, Value};
    use RootScope::{AllPorts, Port};
    let mut add = |root, demand, scope, constant_only, work: &mut CompileCheckpoints<'_>| {
        work.step()?;
        visit(
            NodeRootRequirement {
                root,
                demand,
                scope,
                constant_only,
            },
            work,
        )
    };
    fn calls(
        calls: &[AggregateCall],
        add: &mut impl FnMut(
            RootOrdinal,
            EvaluationDemand,
            RootScope,
            bool,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), CompileControlError> {
        for call in calls {
            work.step()?;
            for root in &call.arguments {
                add(*root, Value, Port(0), false, work)?;
            }
            for key in &call.order_by {
                add(key.expr, Value, Port(0), false, work)?;
            }
            if let Some(root) = call.filter_root {
                add(root, TruthOnly, Port(0), false, work)?;
            }
        }
        Ok(())
    }
    match &node.kind {
        NodeKind::Scan {
            residuals,
            derived_values,
            ..
        } => {
            for root in residuals {
                add(*root, TruthOnly, Port(0), false, work)?;
            }
            for value in derived_values {
                work.step()?;
                let Some(crate::ValueDef {
                    origin: ValueOrigin::Expr { node: owner, root },
                    ..
                }) = fragment.values().get(value)
                else {
                    return Err(NodeRootError::InvalidScanDerived(*value));
                };
                if *owner != node.id {
                    return Err(NodeRootError::InvalidScanDerived(*value));
                }
                add(*root, Value, Port(0), false, work)?;
            }
        }
        NodeKind::Filter { predicates } => {
            for root in predicates {
                add(*root, TruthOnly, Port(0), false, work)?;
            }
        }
        NodeKind::Project { expressions } => {
            for (root, _) in expressions {
                add(*root, Value, Port(0), false, work)?;
            }
        }
        NodeKind::Aggregate {
            group_by,
            calls: items,
            ..
        } => {
            for (root, _) in group_by {
                add(*root, Value, Port(0), false, work)?;
            }
            calls(items, &mut add, work)?;
        }
        NodeKind::HashJoin { condition, .. } => {
            for key in condition
                .keys()
                .iter()
                .chain(condition.scope_keys().into_iter().flatten())
            {
                add(key.left, Value, Port(0), false, work)?;
                add(key.right, Value, Port(1), false, work)?;
            }
            if let Some(root) = condition.residual() {
                add(
                    *root,
                    if condition.is_null_aware() {
                        Value
                    } else {
                        TruthOnly
                    },
                    AllPorts,
                    false,
                    work,
                )?;
            }
        }
        NodeKind::NestLoopJoin {
            kind, predicate, ..
        } => {
            if let Some(root) = predicate {
                add(
                    *root,
                    if matches!(
                        kind,
                        crate::JoinKind::NullAwareLeftAnti | crate::JoinKind::LeftMark
                    ) {
                        Value
                    } else {
                        TruthOnly
                    },
                    AllPorts,
                    false,
                    work,
                )?;
            }
        }
        NodeKind::Sort { order_by, mode } => {
            for key in order_by {
                add(key.expr, Value, Port(0), false, work)?;
            }
            match mode {
                crate::SortMode::Global => {}
                crate::SortMode::Analytic { partition_by }
                | crate::SortMode::PartitionTopN { partition_by, .. } => {
                    for key in partition_by {
                        add(key.expr, Value, Port(0), false, work)?;
                    }
                }
            }
        }
        NodeKind::TopN {
            order_by,
            reduction,
            ..
        } => {
            for key in order_by {
                add(key.expr, Value, Port(0), false, work)?;
            }
            if let crate::TopNReduction::GroupedStates {
                group_by,
                calls: items,
                ..
            } = reduction
            {
                for (root, _) in group_by {
                    add(*root, Value, Port(0), false, work)?;
                }
                calls(items, &mut add, work)?;
            }
        }
        NodeKind::Window(spec) => {
            for key in spec.partition_by.iter().chain(spec.order_by.iter()) {
                add(key.expr, Value, Port(0), false, work)?;
            }
            for call in &spec.calls {
                work.step()?;
                for root in &call.arguments {
                    add(*root, Value, Port(0), false, work)?;
                }
                for key in &call.order_by {
                    add(key.expr, Value, Port(0), false, work)?;
                }
            }
        }
        NodeKind::Values { rows } => {
            for row in rows {
                work.step()?;
                for root in row {
                    add(*root, Value, AllPorts, false, work)?;
                }
            }
        }
        NodeKind::GenerateSeries { start, stop, step } => {
            add(*start, Value, AllPorts, false, work)?;
            add(*stop, Value, AllPorts, false, work)?;
            if let Some(root) = step {
                add(*root, Value, AllPorts, false, work)?;
            }
        }
        NodeKind::TableFunction { arguments, .. } => {
            for root in arguments {
                add(
                    *root,
                    Value,
                    if node.inputs.is_empty() {
                        AllPorts
                    } else {
                        Port(0)
                    },
                    false,
                    work,
                )?;
            }
        }
        NodeKind::Unpivot { spec } => {
            for mapping in &spec.mappings {
                work.step()?;
                for constant in &mapping.constants {
                    work.step()?;
                    if let crate::UnpivotConstant::Scalar(root) = constant {
                        add(*root, Value, Port(0), true, work)?;
                    }
                }
            }
        }
        NodeKind::ChangeEventExpand { events, .. } => {
            for event in events {
                work.step()?;
                if let Some(root) = event.predicate {
                    add(root, TruthOnly, Port(0), false, work)?;
                }
                for assignment in &event.assignments {
                    work.step()?;
                    if let Some(root) = assignment.expression {
                        add(root, Value, Port(0), false, work)?;
                    }
                }
            }
        }
        NodeKind::TableFinish(spec) => {
            if let Some(unpivot) = &spec.grouped_unpivot {
                for mapping in &unpivot.mappings {
                    work.step()?;
                    for constant in &mapping.constants {
                        work.step()?;
                        if let crate::UnpivotConstant::Scalar(root) = constant {
                            add(*root, Value, Port(spec.inputs.writer_results), true, work)?;
                        }
                    }
                }
            }
        }
        NodeKind::SystemRelation { .. }
        | NodeKind::Limit { .. }
        | NodeKind::SetOp { .. }
        | NodeKind::Repeat { .. }
        | NodeKind::AssertOneRow(_)
        | NodeKind::ExchangeSource { .. }
        | NodeKind::PositionShard(_)
        | NodeKind::WriteLayoutKey { .. }
        | NodeKind::TableWriter { .. } => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests;
