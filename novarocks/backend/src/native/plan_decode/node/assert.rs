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

use super::DecodedNode;
use super::common::parse_optional_nonnegative_i64;
use crate::native::plan_decode::error::NativeFragmentLeafDecodeError;
use novarocks::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode, Assertion};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::plan;
use novarocks::protocol::common::error::{FieldPath, ProtocolErrorKind};

pub(super) fn lower_assert_one_row_node(
    node: &plan::DistributedNode,
    assert: &plan::AssertOneRowNode,
    path: FieldPath,
    mut children: Vec<DecodedNode>,
) -> Result<DecodedNode, crate::native::plan_decode::error::NativeFragmentDecodeError> {
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
