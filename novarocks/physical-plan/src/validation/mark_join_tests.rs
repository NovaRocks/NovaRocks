// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
use super::*;
use crate::*;
use arrow_schema::DataType;

fn mark_fragment(
    mark_ty: ValueType,
    owner: NodeId,
    include_probe: bool,
    include_build: bool,
    hash: bool,
) -> Result<Fragment, Box<dyn std::error::Error>> {
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let [left, right, join] = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
    let mut inputs = Vec::new();
    for node in [left, right] {
        let value = builder.add_value(
            ValueType::new(DataType::Utf8, true),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )?;
        builder.add_values(node, Box::default(), Box::from([value]))?;
        inputs.push(value);
    }
    let mark = builder.add_value(mark_ty, ValueOrigin::JoinMark { node: owner })?;
    let properties = PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let mut output = Vec::new();
    if include_probe {
        output.push(inputs[0]);
    }
    if include_build {
        output.push(inputs[1]);
    }
    output.push(mark);
    let kind = if hash {
        let key_left = builder.add_expression(
            join,
            ValueType::new(DataType::Utf8, true),
            ExprKind::Value(inputs[0]),
        )?;
        let key_right = builder.add_expression(
            join,
            ValueType::new(DataType::Utf8, true),
            ExprKind::Value(inputs[1]),
        )?;
        NodeKind::HashJoin {
            kind: JoinKind::LeftMark,
            distribution: JoinDistribution::Singleton,
            build_side: JoinSide::Right,
            condition: HashJoinCondition::NullAwareEqual {
                value_equal_keys: Box::from([JoinKey {
                    left: key_left,
                    right: key_right,
                    null_safe: false,
                }]),
                scope_keys: Box::default(),
                eligibility: None,
            },
            null_extended: Box::default(),
        }
    } else {
        let predicate = builder.add_expression(
            join,
            ValueType::new(DataType::Boolean, false),
            ExprKind::Literal(LiteralValue::Boolean(true)),
        )?;
        NodeKind::NestLoopJoin {
            kind: JoinKind::LeftMark,
            distribution: NestLoopJoinDistribution::Singleton,
            predicate: Some(predicate),
            null_extended: Box::default(),
        }
    };
    builder.add_join(
        join,
        [left, right],
        Box::from([properties.clone(), properties]),
        output.into_boxed_slice(),
        Distribution::Singleton,
        kind,
    )?;
    Ok(builder.finish_definition(
        join,
        FragmentSink::Noop,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        StagePlacementPolicy::Singleton,
    )?)
}
#[test]
fn left_mark_preserves_probe_and_declares_nullable_boolean_origin() {
    let fragment = mark_fragment(
        ValueType::new(DataType::Boolean, true),
        NodeId::new(3),
        true,
        false,
        true,
    )
    .unwrap();
    validate_fragment(&fragment, &FragmentCuts::default()).unwrap();
    for side in [JoinSide::Left, JoinSide::Right] {
        assert!(!JoinKind::LeftMark.key_filter_reaches_side(side));
        assert!(!JoinKind::LeftMark.side_only_loses_rows(side));
    }
}
#[test]
fn left_mark_broadcast_requires_complete_build_and_single_copy_probe() {
    let mut parts = mark_fragment(
        ValueType::new(DataType::Boolean, true),
        NodeId::new(3),
        true,
        false,
        true,
    )
    .unwrap()
    .into_parts();
    let join = NodeId::new(3);
    let replicated = PhysicalProperties {
        distribution: Distribution::Broadcast,
        row_multiplicity: RowMultiplicity::Replicated,
        ordering: Box::default(),
    };
    parts
        .nodes
        .get_mut(&NodeId::new(2))
        .unwrap()
        .output_properties = replicated.clone();
    let node = parts.nodes.get_mut(&join).unwrap();
    node.required_inputs[1] = replicated;
    let NodeKind::HashJoin { distribution, .. } = &mut node.kind else {
        unreachable!()
    };
    *distribution = JoinDistribution::BroadcastBuild;
    let fragment = Fragment::from(parts);
    validate_fragment(&fragment, &FragmentCuts::default()).unwrap();
    for side in [NodeId::new(1), NodeId::new(2)] {
        let mut parts = fragment.clone().into_parts();
        let input = parts.nodes.get_mut(&side).unwrap();
        input.output_properties.row_multiplicity = match side.get() {
            1 => RowMultiplicity::Replicated,
            _ => RowMultiplicity::SingleCopy,
        };
        assert!(validate_fragment(&Fragment::from(parts), &FragmentCuts::default()).is_err());
    }
}
#[test]
fn left_mark_rejects_noncanonical_hash_condition_and_placement() {
    let fragment = mark_fragment(
        ValueType::new(DataType::Boolean, true),
        NodeId::new(3),
        true,
        false,
        true,
    )
    .unwrap();
    for variant in 0..5 {
        let mut parts = fragment.clone().into_parts();
        let NodeKind::HashJoin {
            condition,
            distribution,
            build_side,
            ..
        } = &mut parts.nodes.get_mut(&NodeId::new(3)).unwrap().kind
        else {
            unreachable!()
        };
        match variant {
            0 => {
                *condition = HashJoinCondition::Ordinary {
                    keys: condition.keys().clone(),
                    residual: None,
                }
            }
            1 => *distribution = JoinDistribution::Partitioned,
            2 => *build_side = JoinSide::Left,
            3 => condition.keys_mut()[0].null_safe = true,
            _ => *condition.scope_keys_mut().unwrap() = condition.keys().clone(),
        }
        assert!(validate_fragment(&Fragment::from(parts), &FragmentCuts::default()).is_err());
    }
}
#[test]
fn left_mark_rejects_wrong_origin_type_outputs_and_nested_loop_algorithm() {
    for (ty, owner, probe, build, hash) in [
        (
            ValueType::new(DataType::Boolean, false),
            NodeId::new(3),
            true,
            false,
            true,
        ),
        (
            ValueType::new(DataType::Int64, true),
            NodeId::new(3),
            true,
            false,
            true,
        ),
        (
            ValueType::new(DataType::Boolean, true),
            NodeId::new(1),
            true,
            false,
            true,
        ),
        (
            ValueType::new(DataType::Boolean, true),
            NodeId::new(3),
            false,
            false,
            true,
        ),
        (
            ValueType::new(DataType::Boolean, true),
            NodeId::new(3),
            true,
            true,
            true,
        ),
        (
            ValueType::new(DataType::Boolean, true),
            NodeId::new(3),
            true,
            false,
            false,
        ),
    ] {
        assert!(mark_fragment(ty, owner, probe, build, hash).is_err());
    }
}
