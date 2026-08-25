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

//! Fragment assert-node decoding.

use super::DecodedNode;
use super::common::parse_optional_nonnegative_i64;
use crate::fragment::decode::plan::error::NativeFragmentLeafDecodeError;
use novarocks_execution::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode, Assertion};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto::plan;
use novarocks_proto::{FieldPath, ProtocolErrorKind};

pub(super) fn lower_assert_one_row_node(
    node: &plan::DistributedNode,
    assert: &plan::AssertOneRowNode,
    path: FieldPath,
    mut children: Vec<DecodedNode>,
) -> Result<DecodedNode, crate::fragment::decode::plan::error::NativeFragmentDecodeError> {
    let decoded = (|| -> Result<DecodedNode, NativeFragmentLeafDecodeError> {
        let child = children.pop().expect("child");
        let desired_num_rows = parse_optional_nonnegative_i64(
            assert.desired_num_rows,
            "AssertOneRowNode.desired_num_rows",
        )
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::OutOfRange,
                "desired_num_rows",
                error,
            )
        })?
        .or(Some(1));
        let assertion = lower_row_count_assertion(assert.assertion)?;
        let mode = if assert.group_key_column_ids.is_empty() {
            if !assert.group_key_labels.is_empty() || assert.keyed_message_prefix.is_some() {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::MissingField,
                    "group_key_column_ids",
                    "AssertOneRowNode group_key_column_ids is required when keyed metadata is present",
                ));
            }
            AssertNumRowsMode::Global {
                desired_num_rows,
                assertion,
                subquery_string: Some(assert.subquery_text.clone()),
            }
        } else {
            if desired_num_rows != Some(1) {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "desired_num_rows",
                    "AssertOneRowNode keyed assertions only support desired_num_rows <= 1",
                ));
            }
            if !matches!(assertion, Assertion::Le) {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "assertion",
                    "AssertOneRowNode keyed assertions only support desired_num_rows <= 1",
                ));
            }
            if !assert.group_key_labels.is_empty()
                && assert.group_key_labels.len() != assert.group_key_column_ids.len()
            {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "group_key_labels",
                    format!(
                        "AssertOneRowNode group_key_labels length mismatch: key_columns={} labels={}",
                        assert.group_key_column_ids.len(),
                        assert.group_key_labels.len()
                    ),
                ));
            }
            let key_slots = assert
                .group_key_column_ids
                .iter()
                .enumerate()
                .map(|(index, column_id)| {
                    child.layout.resolve_column_id(*column_id).map_err(|error| {
                        NativeFragmentLeafDecodeError::at_field(
                            ProtocolErrorKind::InvalidValue,
                            "group_key_column_ids",
                            format!("AssertOneRowNode group key: {error}"),
                        )
                        .append_index(index)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let key_labels = if assert.group_key_labels.is_empty() {
                assert
                    .group_key_column_ids
                    .iter()
                    .map(|column_id| format!("column_{column_id}"))
                    .collect()
            } else {
                assert.group_key_labels.clone()
            };
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots,
                key_labels,
                message_prefix: assert
                    .keyed_message_prefix
                    .clone()
                    .unwrap_or_else(|| "assert_num_rows failed".to_string()),
            }
        };
        Ok(DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::AssertNumRows(AssertNumRowsNode {
                    input: Box::new(child.node),
                    node_id: node.node_id,
                    mode,
                }),
            },
            layout: child.layout,
            output_schema: child.output_schema,
        })
    })();
    decoded.map_err(|error| error.into_native(path))
}

fn lower_row_count_assertion(value: i32) -> Result<Assertion, NativeFragmentLeafDecodeError> {
    match value {
        value if value == plan::RowCountAssertion::Unspecified as i32 => Ok(Assertion::Le),
        value if value == plan::RowCountAssertion::Eq as i32 => Ok(Assertion::Eq),
        value if value == plan::RowCountAssertion::Ne as i32 => Ok(Assertion::Ne),
        value if value == plan::RowCountAssertion::Lt as i32 => Ok(Assertion::Lt),
        value if value == plan::RowCountAssertion::Le as i32 => Ok(Assertion::Le),
        value if value == plan::RowCountAssertion::Gt as i32 => Ok(Assertion::Gt),
        value if value == plan::RowCountAssertion::Ge as i32 => Ok(Assertion::Ge),
        other => Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "assertion",
            format!("AssertOneRowNode assertion {other} is not supported"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{lower, one_col_values_node, physical_node};
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_execution::exec::node::assert::AssertNumRowsMode;
    use novarocks_proto::ProtocolErrorKind;
    use novarocks_proto::plan;
    use novarocks_types::SlotId;

    fn decode_error(
        node: &plan::DistributedNode,
    ) -> crate::fragment::decode::plan::error::NativeFragmentDecodeError {
        let mut arena = ExprArena::default();
        super::super::decode_node(
            node,
            &mut arena,
            &super::super::NativePlanDecodeContext::default(),
        )
        .expect_err("invalid AssertOneRow node must fail")
    }

    #[test]
    fn lowers_keyed_assert_num_rows_from_native_proto() {
        let assert_node = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: "DML change-stream matched row uniqueness".to_string(),
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: vec![1],
                group_key_labels: vec!["_row_id".to_string()],
                keyed_message_prefix: Some("MOR UPDATE matched target row".to_string()),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&assert_node);
        let ExecNodeKind::AssertNumRows(assert) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        match assert.mode {
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots,
                key_labels,
                message_prefix,
            } => {
                assert_eq!(key_slots, vec![SlotId::new(1)]);
                assert_eq!(key_labels, vec!["_row_id".to_string()]);
                assert_eq!(message_prefix, "MOR UPDATE matched target row");
            }
            AssertNumRowsMode::Global { .. } => panic!("expected keyed assert"),
        }
    }

    #[test]
    fn unknown_assertion_uses_exact_path_and_kind() {
        let node = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                assertion: 999,
                ..Default::default()
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let error = decode_error(&node);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.assert_one_row.assertion"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidEnum);
    }

    #[test]
    fn negative_desired_rows_uses_exact_path_and_kind() {
        let node = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                desired_num_rows: Some(-1),
                ..Default::default()
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let error = decode_error(&node);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.assert_one_row.desired_num_rows"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
    }

    #[test]
    fn unknown_group_key_uses_exact_indexed_path_and_kind() {
        let node = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: vec![999],
                ..Default::default()
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let error = decode_error(&node);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.assert_one_row.group_key_column_ids[0]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
    }
}
