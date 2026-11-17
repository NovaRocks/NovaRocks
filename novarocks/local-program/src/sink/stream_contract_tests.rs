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

//! The sender must accept precisely the declared hierarchical key domain.

use super::*;
use crate::{StaticExprKind, StaticExprNode};
use arrow_schema::DataType;

fn stream(key_type: DataType, conditional: bool) -> Result<StaticSinkProgram, StaticSinkError> {
    let mut nodes = vec![StaticExprNode::new_value_typed(
        StaticExprKind::SlotId(SlotId::new(1)),
        novarocks_functions::FunctionValueType::new(key_type, false),
    )];
    let routing = if conditional {
        nodes.push(StaticExprNode::new_value_typed(
            StaticExprKind::SlotId(SlotId::new(2)),
            novarocks_functions::FunctionValueType::new(DataType::Boolean, false),
        ));
        StaticStreamRouting::ConditionalKeyV1 {
            applies: ProgramExprId::new(1),
        }
    } else {
        StaticStreamRouting::HierarchicalHashV1
    };
    let arena = Arc::new(crate::test_support::arena(nodes).unwrap());
    let branch = StaticStreamBranch::try_new(
        1,
        DataStreamPartitionType::HashPartitioned,
        vec![ProgramExprId::new(0)],
        vec![SlotId::new(1)],
        None,
        StaticStreamContract {
            role: ProgramOutputRole::Relational,
            delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            routing,
        },
    )
    .unwrap();
    StaticSinkProgram::try_data_stream(branch, arena)
}

#[test]
fn hierarchical_and_conditional_sender_refuse_keys_outside_their_signed_algorithm_domain() {
    for conditional in [false, true] {
        stream(DataType::Int64, conditional)
            .expect("a canonical native key is valid before fault injection");
        {
            assert!(
                matches!(
                    stream(DataType::UInt64, conditional),
                    Err(StaticSinkError::InvalidExpression)
                ),
                "a same-shaped expression is insufficient evidence of supported key semantics"
            );
        }
    }
}
