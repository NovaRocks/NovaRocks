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
use crate::exec::node::assert::{AssertNumRowsNode, Assertion};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::layout::Layout;
use crate::lower::node::Lowered;
use crate::plan_nodes;

pub(crate) fn lower_assert_num_rows_node(
    mut children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    _out_layout: &mut Layout,
) -> Result<Lowered, String> {
    if children.len() != 1 {
        return Err(format!(
            "ASSERT_NUM_ROWS_NODE expected 1 child, got {}",
            children.len()
        ));
    }
    let child = children.pop().expect("child");

    let t_assert = node
        .assert_num_rows_node
        .as_ref()
        .ok_or_else(|| "ASSERT_NUM_ROWS_NODE missing payload".to_string())?;

    let desired_num_rows = t_assert.desired_num_rows.map(|v| v as usize);
    let subquery_string = t_assert.subquery_string.clone();

    let assertion = match t_assert.assertion {
        Some(plan_nodes::TAssertion::EQ) | None => Assertion::Eq,
        Some(plan_nodes::TAssertion::NE) => Assertion::Ne,
        Some(plan_nodes::TAssertion::LT) => Assertion::Lt,
        Some(plan_nodes::TAssertion::LE) => Assertion::Le,
        Some(plan_nodes::TAssertion::GT) => Assertion::Gt,
        Some(plan_nodes::TAssertion::GE) => Assertion::Ge,
        Some(_) => Assertion::Eq,
    };

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::AssertNumRows(AssertNumRowsNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                desired_num_rows,
                assertion,
                subquery_string,
            }),
        },
        // AssertNumRows is a pass-through node, keep child's layout.
        layout: child.layout,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lower::layout::Layout;
    use crate::plan_nodes::{TAssertNumRowsNode, TAssertion, TPlanNodeType};
    use std::collections::HashMap;

    #[test]
    fn lower_assert_num_rows_carries_config() {
        // Fake child Lowered with a simple empty layout
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let child = Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(crate::exec::node::values::ValuesNode {
                    chunk: crate::exec::chunk::Chunk::default(),
                    node_id: 0,
                }),
            },
            layout,
        };

        let mut t_node =
            crate::lower::node::test_plan_node(0, TPlanNodeType::ASSERT_NUM_ROWS_NODE, 1);
        t_node.assert_num_rows_node = Some(TAssertNumRowsNode {
            desired_num_rows: Some(1),
            subquery_string: Some("select c1 from test".to_string()),
            assertion: Some(TAssertion::EQ),
        });

        let mut out_layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let lowered = lower_assert_num_rows_node(vec![child], &t_node, &mut out_layout)
            .expect("lower assert node");

        match lowered.node.kind {
            ExecNodeKind::AssertNumRows(n) => {
                assert_eq!(n.desired_num_rows, Some(1));
                assert!(matches!(n.assertion, Assertion::Eq));
                assert_eq!(n.subquery_string.as_deref(), Some("select c1 from test"));
            }
            _ => panic!("expected AssertNumRows exec node"),
        }
    }
}
