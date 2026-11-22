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

//! Fragment shape/ref gates borrow real compiled units; port-role derivation is
//! covered by PhysicalPlan's node_roots::tests rather than a second root bank.
use crate::*;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_type_contract::{EvaluationDemand, FunctionValueType};
use novarocks_types::SlotId;
use std::{num::NonZeroUsize, sync::Arc};

fn r(ordinal: u32) -> ProgramRootRef {
    crate::test_support::root(0, ordinal)
}
fn key(probe: u32, build: u32, null_safe: bool) -> ProgramJoinKey {
    ProgramJoinKey {
        probe: r(probe),
        build: r(build),
        null_safe,
        match_relation: novarocks_type_contract::JoinKeyMatch::Grouping,
    }
}
fn condition() -> ProgramJoinCondition {
    ProgramJoinCondition::NullAwareEqual {
        value_equal_keys: vec![key(0, 0, true)],
        scope_keys: vec![key(1, 1, false), key(0, 1, true)],
        eligibility: Some(r(2)),
    }
}
fn graph(
    kind: JoinType,
    condition: ProgramJoinCondition,
    filters: Vec<FilterProducerAtExpr>,
) -> Result<LocalProgramGraph, LocalProgramError> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let layout = StaticLayout::try_new(schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        layout.clone(),
    )
    .unwrap();
    let units = [crate::test_support::constant_unit(
        &[
            crate::test_support::integer(7),
            crate::test_support::integer(9),
            crate::test_support::boolean(true),
        ],
        &[
            (0, EvaluationDemand::Value),
            (1, EvaluationDemand::Value),
            (2, EvaluationDemand::Value),
            (2, EvaluationDemand::TruthOnly),
        ],
    )];
    let requirements = if filters.is_empty() {
        vec![]
    } else {
        vec![BindingRequirement::RuntimeFilter { binding_id: 7 }]
    };
    LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(
                10,
                ProgramNodeKind::Values {
                    values: values.clone(),
                },
                layout.clone(),
            ),
            ProgramNode::new(11, ProgramNodeKind::Values { values }, layout.clone()),
            ProgramNode::new(
                20,
                ProgramNodeKind::Join {
                    left: ProgramNodeId::new(0),
                    right: ProgramNodeId::new(1),
                    join_type: kind,
                    distribution_mode: JoinDistributionMode::SharedBuild,
                    left_layout: layout.clone(),
                    right_layout: layout.clone(),
                    join_scope_layout: layout.clone(),
                    condition,
                    runtime_filters: filters,
                    runtime_filter_consumers: vec![],
                },
                layout.clone(),
            ),
        ],
        ProgramNodeId::new(2),
        &units,
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(requirements).unwrap(),
    )
}
#[test]
fn naa_kind_and_closed_condition_must_agree_and_values_cannot_be_empty() {
    for (kind, condition) in [
        (
            JoinType::NullAwareLeftAnti,
            ProgramJoinCondition::Ordinary {
                keys: vec![key(0, 0, false)],
                residual_predicate: None,
            },
        ),
        (JoinType::Inner, condition()),
        (
            JoinType::NullAwareLeftAnti,
            ProgramJoinCondition::NullAwareEqual {
                value_equal_keys: vec![],
                scope_keys: vec![key(0, 0, false)],
                eligibility: None,
            },
        ),
    ] {
        assert_eq!(
            graph(kind, condition, vec![]).unwrap_err(),
            LocalProgramError::InvalidNodeShape
        );
    }
    assert!(
        graph(
            JoinType::NullAwareLeftAnti,
            ProgramJoinCondition::NullAwareEqual {
                value_equal_keys: vec![key(0, 0, false)],
                scope_keys: vec![],
                eligibility: None
            },
            vec![]
        )
        .is_ok()
    );
}
#[test]
fn every_naa_role_participates_in_expression_reference_validation() {
    for role in 0..5 {
        let mut condition = condition();
        let ProgramJoinCondition::NullAwareEqual {
            value_equal_keys,
            scope_keys,
            eligibility,
        } = &mut condition
        else {
            unreachable!()
        };
        match role {
            0 => value_equal_keys[0].probe = r(999),
            1 => value_equal_keys[0].build = r(999),
            2 => scope_keys[0].probe = r(999),
            3 => scope_keys[0].build = r(999),
            4 => *eligibility = Some(r(999)),
            _ => unreachable!(),
        }
        assert_eq!(
            graph(JoinType::NullAwareLeftAnti, condition, vec![]).unwrap_err(),
            LocalProgramError::InvalidExpression
        );
    }
}
#[test]
fn naa_runtime_filter_ordinals_name_value_keys_and_never_scope_keys() {
    let producer = FilterProducerAtExpr {
        expr_id: r(0),
        key_ordinal: 0,
        producer: StaticFilterProducer::try_new(
            7,
            8,
            FilterProducerKind::Membership,
            StaticFilterContract::membership(
                &FunctionValueType::new(DataType::Int64, false),
                FilterNullSemantics::NeverMatches,
            )
            .unwrap(),
            FilterReduction::SetUnion,
        )
        .unwrap(),
    };
    assert!(
        graph(
            JoinType::NullAwareLeftAnti,
            condition(),
            vec![producer.clone()]
        )
        .is_ok()
    );
    let mut out_of_value_domain = producer.clone();
    out_of_value_domain.key_ordinal = 1;
    assert_eq!(
        graph(
            JoinType::NullAwareLeftAnti,
            condition(),
            vec![out_of_value_domain]
        )
        .unwrap_err(),
        LocalProgramError::InvalidNodeShape
    );
    let mut scope_definition = producer;
    scope_definition.expr_id = r(1);
    assert_eq!(
        graph(
            JoinType::NullAwareLeftAnti,
            condition(),
            vec![scope_definition]
        )
        .unwrap_err(),
        LocalProgramError::InvalidNodeShape
    );
}
