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

//! Backend-owned structural validation for native fragment wire payloads.

use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks_protocol::plan;

use super::expression::validate_proto_expr_shape_at;

pub(crate) fn validate_fragment_expressions(
    fragment: &plan::PlanFragment,
) -> Result<(), ProtocolError> {
    for (index, expression) in fragment.output_exprs.iter().enumerate() {
        validate_proto_expr_shape_at(
            expression,
            FieldPath::root("plan_fragment")
                .field("output_exprs")
                .index(index),
        )
        .map_err(|error| error.into_protocol())?;
    }
    let Some(table) = fragment.runtime_filter_bindings.as_ref() else {
        return Ok(());
    };
    for (index, binding) in table.bindings.iter().enumerate() {
        let path = FieldPath::root("plan_fragment")
            .field("runtime_filter_bindings")
            .field("bindings")
            .index(index)
            .field("expression");
        let expression = binding.expression.as_ref().ok_or_else(|| {
            ProtocolError::new(
                ProtocolFamily::Native,
                path.clone(),
                ProtocolErrorKind::MissingField,
                "native runtime-filter binding requires expression",
            )
        })?;
        validate_proto_expr_shape_at(expression, path).map_err(|error| error.into_protocol())?;
    }
    Ok(())
}

pub(crate) fn validate_node_required_fields(
    node: &plan::DistributedNode,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let payload = node.payload.as_ref().ok_or_else(|| {
        ProtocolError::new(
            ProtocolFamily::Native,
            path.clone().field("payload"),
            ProtocolErrorKind::MissingField,
            format!("native DistributedNode {} requires payload", node.node_id),
        )
    })?;
    if let plan::distributed_node::Payload::Physical(physical) = payload {
        let kind = physical.kind.as_ref().ok_or_else(|| {
            ProtocolError::new(
                ProtocolFamily::Native,
                path.clone()
                    .field("payload")
                    .field("physical")
                    .field("kind"),
                ProtocolErrorKind::MissingField,
                format!("native PlanNode {} requires kind", node.node_id),
            )
        })?;
        if let plan::plan_node::Kind::Values(values) = kind {
            for (row_index, row) in values.rows.iter().enumerate() {
                for (value_index, value) in row.values.iter().enumerate() {
                    validate_proto_expr_shape_at(
                        value,
                        path.clone()
                            .field("payload")
                            .field("physical")
                            .field("values")
                            .field("rows")
                            .index(row_index)
                            .field("values")
                            .index(value_index),
                    )
                    .map_err(|error| error.into_protocol())?;
                }
            }
        }
    }
    for (index, child) in node.children.iter().enumerate() {
        validate_node_required_fields(child, path.clone().field("children").index(index))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_fragment_expressions, validate_node_required_fields};
    use novarocks::protocol::FieldPath;
    use novarocks_protocol::plan;

    #[test]
    fn missing_node_payload_preserves_validation_contract() {
        let error = validate_node_required_fields(
            &plan::DistributedNode {
                node_id: 7,
                ..Default::default()
            },
            FieldPath::root("plan_fragment").field("root"),
        )
        .expect_err("payload is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.root.payload (missing field): native DistributedNode 7 requires payload"
        );
    }

    #[test]
    fn missing_runtime_filter_expression_preserves_validation_contract() {
        let error = validate_fragment_expressions(&plan::PlanFragment {
            runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
                fragment_id: 7,
                bindings: vec![plan::RuntimeFilterBinding {
                    binding_id: 1,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        })
        .expect_err("expression is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.runtime_filter_bindings.bindings[0].expression (missing field): native runtime-filter binding requires expression"
        );
    }
}
