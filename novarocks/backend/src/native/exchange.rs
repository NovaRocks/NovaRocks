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

//! Backend-owned native exchange-contract DTO decoding.

use std::collections::BTreeMap;

use novarocks::exec::fragment::program::{ExchangeInputContract, FragmentNodeId};
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks_protocol::plan;

use super::layout::chunk_schema_from_output_columns;

pub(crate) fn decode_exchange_contracts(
    root: &plan::DistributedNode,
    path: FieldPath,
) -> Result<BTreeMap<FragmentNodeId, ExchangeInputContract>, ProtocolError> {
    fn visit(
        node: &plan::DistributedNode,
        path: FieldPath,
        contracts: &mut BTreeMap<FragmentNodeId, ExchangeInputContract>,
    ) -> Result<(), ProtocolError> {
        if let Some(plan::distributed_node::Payload::Exchange(exchange)) = node.payload.as_ref() {
            let schema = chunk_schema_from_output_columns(
                &exchange.output_columns,
                path.clone()
                    .field("payload")
                    .field("exchange")
                    .field("output_columns"),
            )?;
            if contracts
                .insert(
                    FragmentNodeId::new(node.node_id),
                    ExchangeInputContract::new(schema),
                )
                .is_some()
            {
                return Err(ProtocolError::new(
                    ProtocolFamily::Native,
                    path.field("node_id"),
                    ProtocolErrorKind::InconsistentFields,
                    format!("duplicate exchange node_id={}", node.node_id),
                ));
            }
        }
        for (index, child) in node.children.iter().enumerate() {
            visit(
                child,
                path.clone().field("children").index(index),
                contracts,
            )?;
        }
        Ok(())
    }

    let mut contracts = BTreeMap::new();
    visit(root, path, &mut contracts)?;
    Ok(contracts)
}

#[cfg(test)]
mod tests {
    use super::decode_exchange_contracts;
    use novarocks::protocol::FieldPath;
    use novarocks_protocol::{common, plan};

    fn exchange_node(
        node_id: i32,
        output_columns: Vec<common::OutputColumn>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    output_columns,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn int_column(column_id: u32, name: &str) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            nullable: true,
            r#type: Some(common::TypeDesc {
                kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                    r#type: common::PrimitiveType::Int as i32,
                    ..Default::default()
                })),
            }),
            is_internal: false,
        }
    }

    #[test]
    fn decodes_exchange_schema_without_core_decoder() {
        let root = exchange_node(7, vec![int_column(3, "id")]);

        let contracts =
            decode_exchange_contracts(&root, FieldPath::root("plan_fragment").field("root"))
                .expect("decode exchange contract");

        let schema = contracts
            .get(&novarocks::exec::fragment::program::FragmentNodeId::new(7))
            .expect("exchange contract for node");
        assert_eq!(schema.expected_schema().slots().len(), 1);
        assert_eq!(
            schema.expected_schema().slots()[0].slot_id(),
            novarocks::common::ids::SlotId::new(3)
        );
    }

    #[test]
    fn preserves_output_column_error_path() {
        let root = exchange_node(
            7,
            vec![common::OutputColumn {
                column_id: 3,
                name: "id".to_string(),
                nullable: true,
                r#type: None,
                is_internal: false,
            }],
        );

        let error =
            decode_exchange_contracts(&root, FieldPath::root("plan_fragment").field("root"))
                .expect_err("missing output type must fail");

        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.root.payload.exchange.output_columns[0].type (missing field): OutputColumn.type missing for column_id=3 name='id' at index 0"
        );
    }
}
