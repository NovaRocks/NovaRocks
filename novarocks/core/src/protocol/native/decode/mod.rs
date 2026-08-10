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

mod error;
mod expr;
mod instance;
mod layout;
mod node;
mod scan;
mod sink;
mod submission;

pub(crate) use crate::protocol::native::type_mapping::{decode_field_type, decode_type};
pub(crate) use error::NativeFragmentDecodeError;
#[allow(unused_imports)]
pub(crate) use instance::{
    NativeSubmissionMetadata, decode_destinations, decode_query_options, decode_scan_range_params,
};
#[allow(unused_imports)]
pub(crate) use node::{DecodedNode, NativePlanDecodeContext, decode_node};
pub(crate) use sink::decode_fragment_sink_program_with_context;
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(crate) use sink::{decode_fragment_sink_assignment, decode_fragment_sink_program};
pub(crate) use submission::assemble_fragment_submission_with_connectors_and_execution_resolver;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use submission::decode_fragment_submission;
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(crate) use submission::{
    decode_fragment_submission_with_connectors,
    decode_fragment_submission_with_connectors_and_execution_resolver, decode_query_execution_id,
};

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{NativePlanDecodeContext, decode_node};
    use crate::protocol::native::type_mapping::encode_type;
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_protocol::{common, expr, plan};
    use novarocks_types::SlotId;

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(encode_type(&data_type).expect("encode type")),
            nullable: true,
            is_internal: false,
        }
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(encode_type(&DataType::Int64).expect("encode type")),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn values_node() -> plan::DistributedNode {
        let columns = vec![output_column(7, "value", DataType::Int64)];
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: columns.clone(),
                kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                    rows: vec![plan::ExprList {
                        values: vec![int_literal(42)],
                    }],
                    columns,
                })),
            })),
        }
    }

    #[test]
    fn values_node_decodes_from_protocol_owner() {
        let mut arena = ExprArena::default();
        let decoded = decode_node(
            &values_node(),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .expect("decode values node");

        let ExecNodeKind::Values(values) = decoded.node.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.chunk.chunk_schema().slot_ids(), &[SlotId::new(7)]);
        assert_eq!(decoded.output_schema.slot_ids(), &[SlotId::new(7)]);
    }
}
