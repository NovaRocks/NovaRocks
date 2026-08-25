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

//! Fragment limit-node decoding.

use super::DecodedNode;
use super::common::{merge_limits, parse_distributed_limit, parse_optional_nonnegative_i64};
use crate::fragment::decode::plan::error::NativeFragmentLeafDecodeError;
use novarocks_execution::exec::node::limit::LimitNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto::plan;
use novarocks_proto::{FieldPath, ProtocolErrorKind};

pub(super) fn lower_limit_node(
    node: &plan::DistributedNode,
    limit_node: &plan::LimitNode,
    path: FieldPath,
    node_path: FieldPath,
    mut children: Vec<DecodedNode>,
) -> Result<DecodedNode, crate::fragment::decode::plan::error::NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    let payload_limit = parse_optional_nonnegative_i64(limit_node.limit, "LimitNode.limit")
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "limit", error)
                .into_native(path.clone())
        })?;
    let outer_limit = parse_distributed_limit(node.limit, "LimitNode DistributedNode.limit")
        .map_err(|error| {
            crate::fragment::decode::plan::error::NativeFragmentDecodeError::out_of_range(
                node_path.field("limit"),
                error,
            )
        })?;
    let limit = merge_limits("LimitNode", payload_limit, outer_limit).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "limit",
            error,
        )
        .into_native(path.clone())
    })?;
    let offset = parse_optional_nonnegative_i64(limit_node.offset, "LimitNode.offset")
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "offset", error)
                .into_native(path)
        })?
        .unwrap_or(0);
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Limit(LimitNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                limit,
                offset,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::{one_col_values_node, physical_node};
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_proto::ProtocolErrorKind;
    use novarocks_proto::plan;

    fn limit_node(payload_limit: Option<i64>, offset: Option<i64>) -> plan::DistributedNode {
        physical_node(
            20,
            plan::plan_node::Kind::Limit(plan::LimitNode {
                limit: payload_limit,
                offset,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        )
    }

    fn decode_error(
        node: &plan::DistributedNode,
    ) -> crate::fragment::decode::plan::error::NativeFragmentDecodeError {
        let mut arena = ExprArena::default();
        super::super::decode_node(
            node,
            &mut arena,
            &super::super::NativePlanDecodeContext::default(),
        )
        .expect_err("invalid Limit node must fail")
    }

    #[test]
    fn negative_payload_limit_uses_exact_path_and_kind() {
        let error = decode_error(&limit_node(Some(-2), None));
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.limit.limit"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
    }

    #[test]
    fn negative_outer_limit_uses_exact_node_path_and_kind() {
        let mut node = limit_node(None, None);
        node.limit = -2;

        let error = decode_error(&node);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.root.limit");
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
    }

    #[test]
    fn conflicting_limits_use_payload_limit_path_and_kind() {
        let mut node = limit_node(Some(2), None);
        node.limit = 3;

        let error = decode_error(&node);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.limit.limit"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InconsistentFields);
    }

    #[test]
    fn negative_offset_uses_exact_path_and_kind() {
        let error = decode_error(&limit_node(None, Some(-1)));
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.limit.offset"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
    }
}
