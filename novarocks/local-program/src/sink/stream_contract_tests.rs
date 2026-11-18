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
use arrow_schema::DataType;

fn stream(key_type: DataType, conditional: bool) -> Result<StaticSinkProgram, StaticSinkError> {
    use novarocks_expression_contract::ValueId;
    use novarocks_type_contract::{EvaluationDemand, FunctionValueType};
    let mut fields = vec![arrow_schema::Field::new("key", key_type.clone(), false)];
    let mut values = vec![(ValueId::new(1), FunctionValueType::new(key_type, false))];
    let mut roots = vec![(0, EvaluationDemand::Value)];
    let routing = if conditional {
        fields.push(arrow_schema::Field::new(
            "applies",
            DataType::Boolean,
            false,
        ));
        values.push((
            ValueId::new(2),
            FunctionValueType::new(DataType::Boolean, false),
        ));
        roots.push((1, EvaluationDemand::TruthOnly));
        StaticStreamRouting::ConditionalKeyV1 {
            applies: crate::test_support::root(0, 1),
        }
    } else {
        StaticStreamRouting::HierarchicalHashV1
    };
    let layout = StaticLayout::try_new(
        Arc::new(arrow_schema::Schema::new(fields)),
        (1..=values.len()).map(|i| SlotId::new(i as u32)).collect(),
    )
    .unwrap();
    let ordinal_types: Vec<_> = values.iter().map(|(_, ty)| ty.clone()).collect();
    let units = vec![crate::test_support::read_unit(
        &layout,
        &ordinal_types,
        &values,
        &roots,
    )];
    let branch = StaticStreamBranch::try_new(
        1,
        DataStreamPartitionType::HashPartitioned,
        vec![crate::test_support::root(0, 0)],
        vec![SlotId::new(1)],
        None,
        StaticStreamContract {
            role: ProgramOutputRole::Relational,
            delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            routing,
        },
    )
    .unwrap();
    StaticSinkProgram::try_data_stream(branch, &units)
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
