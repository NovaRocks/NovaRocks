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

//! Optimizer physical operator tree extracted from the Memo after optimization.

use std::sync::Arc;

use crate::sql::common::OutputColumn;
use crate::sql::optimizer::cost::BroadcastDecision;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::property::PhysicalPropertySet;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::optimizer::statistics::{CostEstimate, Statistics};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinExecutionDistribution {
    Broadcast,
    Partitioned,
    Colocate,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanExecutionProps {
    pub output_property: PhysicalPropertySet,
    pub child_output_properties: Vec<PhysicalPropertySet>,
    pub join_distribution: Option<JoinExecutionDistribution>,
    /// Shared scalar arena that owns all `ScalarId` handles referenced by this
    /// optimizer physical tree. Attached after extraction so codegen can materialize the
    /// scalar handles at its TypedExpr boundary.
    pub scalar_arena: Option<Arc<ScalarArena>>,
}

impl Default for PlanExecutionProps {
    fn default() -> Self {
        Self {
            output_property: PhysicalPropertySet::any(),
            child_output_properties: Vec::new(),
            join_distribution: None,
            scalar_arena: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct OptimizerExplainStats {
    pub cost_estimate: Option<CostEstimate>,
    pub broadcast_decision: Option<BroadcastDecision>,
}

/// A node in the optimizer physical operator tree produced by `extract_best`.
#[derive(Clone, Debug)]
pub(crate) struct OptimizerPhysicalNode {
    pub op: Operator,
    pub children: Vec<OptimizerPhysicalNode>,
    pub stats: Statistics,
    pub explain_stats: OptimizerExplainStats,
    pub output_columns: Vec<OutputColumn>,
    pub execution_props: PlanExecutionProps,
}

pub(crate) fn attach_scalar_arena(root: &mut OptimizerPhysicalNode, arena: Arc<ScalarArena>) {
    root.execution_props.scalar_arena = Some(Arc::clone(&arena));
    for child in &mut root.children {
        attach_scalar_arena(child, Arc::clone(&arena));
    }
}

#[cfg(test)]
mod execution_prop_tests {
    use super::*;

    #[test]
    fn physical_node_carries_execution_properties() {
        let node = OptimizerPhysicalNode {
            op: make_test_op(),
            children: vec![],
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![],
            execution_props: PlanExecutionProps {
                output_property: crate::sql::optimizer::property::PhysicalPropertySet::broadcast(),
                child_output_properties: vec![
                    crate::sql::optimizer::property::PhysicalPropertySet::any(),
                ],
                join_distribution: Some(JoinExecutionDistribution::Broadcast),
                scalar_arena: None,
            },
        };

        assert_eq!(
            node.execution_props.join_distribution,
            Some(JoinExecutionDistribution::Broadcast)
        );
        assert_eq!(
            node.execution_props.output_property.distribution,
            crate::sql::optimizer::property::DistributionSpec::Broadcast
        );
    }

    fn make_test_op() -> Operator {
        use crate::sql::optimizer::operator::ValuesOp;
        Operator::PhysicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        })
    }
}
