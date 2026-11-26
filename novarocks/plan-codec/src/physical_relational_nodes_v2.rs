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

//! Complete borrowed HashJoin, NestLoopJoin, Sort, Window, SetOp and
//! ExchangeSource projection. Fragment retains semantic/graph/property proofs;
//! remote exchange source values are not members of the local Values namespace.

pub use crate::physical_node_v2::{
    NodeCodecError as RelationalNodeCodecError,
    NodeProjectionFacts as RelationalNodeProjectionFacts,
    NodeProjectionLimits as RelationalNodeProjectionLimits,
};
use crate::{
    physical_aggregate_binding_v2::{
        MaterializedAggregateBindings, copy_aggregate_binding_observed,
        preflight_aggregate_binding_copy_counts, preflight_aggregate_binding_copy_counts_in,
        preflight_aggregate_binding_copy_types, preflight_aggregate_binding_copy_types_in,
    },
    physical_binding_v2::{
        BindingProjectionLimits, MaterializationModel, MaterializedFunctionBinding,
        MaterializedFunctionBindings, copy_scalar_signature_observed,
        preflight_scalar_signature_copy_counts, preflight_scalar_signature_copy_counts_in,
        preflight_scalar_signature_copy_types, preflight_scalar_signature_copy_types_in,
    },
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_value_v2::EncodedValues,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::{physical_control_v2::Empty, physical_package_v2 as wire};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
type Error = RelationalNodeCodecError;
fn required(id: Option<u32>, w: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    w.step()?;
    id.ok_or_else(|| invalid("relational required reference is absent"))
}
fn expression_encode(
    id: novarocks_expression_contract::RootOrdinal,
    e: &EncodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = e.root_expression_observed(id.get(), w)?.is_some();
    w.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "relational expression is absent from original emission",
        ))
    }
}
fn expression_decode(
    id: u32,
    e: &DecodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = e.root_definition_observed(id, w)?.is_some();
    w.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "relational expression is absent from original receiving namespace",
        ))
    }
}
fn encode_join_kind(input: p::JoinKind) -> i32 {
    match input {
        p::JoinKind::Cross => wire::JoinKind::Cross as i32,
        p::JoinKind::Inner => wire::JoinKind::Inner as i32,
        p::JoinKind::LeftOuter => wire::JoinKind::LeftOuter as i32,
        p::JoinKind::RightOuter => wire::JoinKind::RightOuter as i32,
        p::JoinKind::FullOuter => wire::JoinKind::FullOuter as i32,
        p::JoinKind::LeftSemi => wire::JoinKind::LeftSemi as i32,
        p::JoinKind::RightSemi => wire::JoinKind::RightSemi as i32,
        p::JoinKind::LeftAnti => wire::JoinKind::LeftAnti as i32,
        p::JoinKind::RightAnti => wire::JoinKind::RightAnti as i32,
        p::JoinKind::LeftMark => wire::JoinKind::LeftMark as i32,
        p::JoinKind::NullAwareLeftAnti => wire::JoinKind::NullAwareLeftAnti as i32,
    }
}
fn decode_join_kind(input: i32, w: &mut CompileCheckpoints<'_>) -> Result<p::JoinKind, Error> {
    let result = match wire::JoinKind::try_from(input) {
        Ok(wire::JoinKind::Cross) => Ok(p::JoinKind::Cross),
        Ok(wire::JoinKind::Inner) => Ok(p::JoinKind::Inner),
        Ok(wire::JoinKind::LeftOuter) => Ok(p::JoinKind::LeftOuter),
        Ok(wire::JoinKind::RightOuter) => Ok(p::JoinKind::RightOuter),
        Ok(wire::JoinKind::FullOuter) => Ok(p::JoinKind::FullOuter),
        Ok(wire::JoinKind::LeftSemi) => Ok(p::JoinKind::LeftSemi),
        Ok(wire::JoinKind::RightSemi) => Ok(p::JoinKind::RightSemi),
        Ok(wire::JoinKind::LeftAnti) => Ok(p::JoinKind::LeftAnti),
        Ok(wire::JoinKind::RightAnti) => Ok(p::JoinKind::RightAnti),
        Ok(wire::JoinKind::LeftMark) => Ok(p::JoinKind::LeftMark),
        Ok(wire::JoinKind::NullAwareLeftAnti) => Ok(p::JoinKind::NullAwareLeftAnti),
        _ => Err(invalid("relational enum is unspecified or unknown")),
    };
    w.step()?;
    result
}
pub(crate) fn encode_join_side(input: p::JoinSide) -> i32 {
    match input {
        p::JoinSide::Left => wire::JoinSide::Left as i32,
        p::JoinSide::Right => wire::JoinSide::Right as i32,
    }
}
pub(crate) fn decode_join_side(
    input: i32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::JoinSide, Error> {
    let result = match wire::JoinSide::try_from(input) {
        Ok(wire::JoinSide::Left) => Ok(p::JoinSide::Left),
        Ok(wire::JoinSide::Right) => Ok(p::JoinSide::Right),
        _ => Err(invalid("relational enum is unspecified or unknown")),
    };
    w.step()?;
    result
}
fn encode_join_distribution(input: p::JoinDistribution) -> i32 {
    match input {
        p::JoinDistribution::Colocated => wire::JoinDistribution::Colocated as i32,
        p::JoinDistribution::Partitioned => wire::JoinDistribution::Partitioned as i32,
        p::JoinDistribution::BroadcastBuild => wire::JoinDistribution::BroadcastBuild as i32,
        p::JoinDistribution::Singleton => wire::JoinDistribution::Singleton as i32,
    }
}
fn decode_join_distribution(
    input: i32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::JoinDistribution, Error> {
    let result = match wire::JoinDistribution::try_from(input) {
        Ok(wire::JoinDistribution::Colocated) => Ok(p::JoinDistribution::Colocated),
        Ok(wire::JoinDistribution::Partitioned) => Ok(p::JoinDistribution::Partitioned),
        Ok(wire::JoinDistribution::BroadcastBuild) => Ok(p::JoinDistribution::BroadcastBuild),
        Ok(wire::JoinDistribution::Singleton) => Ok(p::JoinDistribution::Singleton),
        _ => Err(invalid("relational enum is unspecified or unknown")),
    };
    w.step()?;
    result
}
fn encode_nest_distribution(input: p::NestLoopJoinDistribution) -> i32 {
    match input {
        p::NestLoopJoinDistribution::Singleton => wire::NestLoopJoinDistribution::Singleton as i32,
        p::NestLoopJoinDistribution::BroadcastRight => {
            wire::NestLoopJoinDistribution::BroadcastRight as i32
        }
    }
}
fn decode_nest_distribution(
    input: i32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::NestLoopJoinDistribution, Error> {
    let result = match wire::NestLoopJoinDistribution::try_from(input) {
        Ok(wire::NestLoopJoinDistribution::Singleton) => Ok(p::NestLoopJoinDistribution::Singleton),
        Ok(wire::NestLoopJoinDistribution::BroadcastRight) => {
            Ok(p::NestLoopJoinDistribution::BroadcastRight)
        }
        _ => Err(invalid("relational enum is unspecified or unknown")),
    };
    w.step()?;
    result
}
fn encode_partition_kind(input: p::PartitionTopNType) -> i32 {
    match input {
        p::PartitionTopNType::RowNumber => wire::PartitionTopNType::RowNumber as i32,
        p::PartitionTopNType::Rank => wire::PartitionTopNType::Rank as i32,
        p::PartitionTopNType::DenseRank => wire::PartitionTopNType::DenseRank as i32,
    }
}
fn decode_partition_kind(
    input: i32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PartitionTopNType, Error> {
    let result = match wire::PartitionTopNType::try_from(input) {
        Ok(wire::PartitionTopNType::RowNumber) => Ok(p::PartitionTopNType::RowNumber),
        Ok(wire::PartitionTopNType::Rank) => Ok(p::PartitionTopNType::Rank),
        Ok(wire::PartitionTopNType::DenseRank) => Ok(p::PartitionTopNType::DenseRank),
        _ => Err(invalid("relational enum is unspecified or unknown")),
    };
    w.step()?;
    result
}
fn encode_set_kind(input: p::SetOperationKind) -> i32 {
    match input {
        p::SetOperationKind::UnionAll => wire::SetOperationKind::UnionAll as i32,
        p::SetOperationKind::Intersect => wire::SetOperationKind::Intersect as i32,
        p::SetOperationKind::Except => wire::SetOperationKind::Except as i32,
    }
}
fn decode_set_kind(
    input: i32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::SetOperationKind, Error> {
    let result = match wire::SetOperationKind::try_from(input) {
        Ok(wire::SetOperationKind::UnionAll) => Ok(p::SetOperationKind::UnionAll),
        Ok(wire::SetOperationKind::Intersect) => Ok(p::SetOperationKind::Intersect),
        Ok(wire::SetOperationKind::Except) => Ok(p::SetOperationKind::Except),
        _ => Err(invalid("relational enum is unspecified or unknown")),
    };
    w.step()?;
    result
}

fn physical_partitions(mode: &p::SortMode) -> &[p::SortExpr] {
    match mode {
        p::SortMode::Global => &[],
        p::SortMode::Analytic { partition_by }
        | p::SortMode::PartitionTopN { partition_by, .. } => partition_by,
    }
}
fn wire_partitions(mode: &wire::SortMode) -> Result<&[wire::SortExpression], Error> {
    match mode.kind.as_ref() {
        Some(wire::sort_mode::Kind::Global(_)) => Ok(&[]),
        Some(wire::sort_mode::Kind::Analytic(v)) => Ok(&v.partition_by),
        Some(wire::sort_mode::Kind::PartitionTopN(v)) => Ok(&v.partition_by),
        None => Err(invalid("relational sort mode kind is absent")),
    }
}
fn wire_partition_capacity(mode: &wire::SortMode) -> Result<usize, Error> {
    match mode.kind.as_ref() {
        Some(wire::sort_mode::Kind::Global(_)) => Ok(0),
        Some(wire::sort_mode::Kind::Analytic(v)) => Ok(v.partition_by.capacity()),
        Some(wire::sort_mode::Kind::PartitionTopN(v)) => Ok(v.partition_by.capacity()),
        None => Err(invalid("relational sort mode kind is absent")),
    }
}
/// O(1) root layout/count facts before variable-length counting or lookups.
type PhysicalHashCondition =
    p::HashJoinCondition<Box<[p::JoinKey]>, novarocks_expression_contract::RootOrdinal>;

fn physical_join_parts(
    condition: &PhysicalHashCondition,
) -> (
    &[p::JoinKey],
    &[p::JoinKey],
    Option<novarocks_expression_contract::RootOrdinal>,
) {
    (
        condition.keys(),
        condition.scope_keys().map_or(&[][..], |keys| keys.as_ref()),
        *condition.residual(),
    )
}

fn validate_physical_join_condition(
    kind: p::JoinKind,
    condition: &PhysicalHashCondition,
) -> Result<(), Error> {
    if matches!(kind, p::JoinKind::NullAwareLeftAnti | p::JoinKind::LeftMark)
        != condition.is_null_aware()
    {
        return Err(invalid("hash join kind and condition variant disagree"));
    }
    if condition.is_null_aware() && condition.keys().is_empty() {
        return Err(invalid("hash join requires non-empty value equality keys"));
    }
    Ok(())
}

type WireJoinParts<'a> = (&'a [wire::JoinKey], &'a [wire::JoinKey], Option<u32>);

fn wire_join_parts(node: &wire::HashJoinNode) -> Result<WireJoinParts<'_>, Error> {
    match node.condition.as_ref() {
        Some(wire::hash_join_node::Condition::Ordinary(v)) => Ok((&v.keys, &[], v.residual_root)),
        Some(wire::hash_join_node::Condition::NullAwareEqual(v)) => {
            Ok((&v.value_equal_keys, &v.scope_keys, v.eligibility_root))
        }
        None => Err(invalid("hash join condition is absent")),
    }
}

fn wire_join_capacity(node: &wire::HashJoinNode) -> Result<usize, Error> {
    match node.condition.as_ref() {
        Some(wire::hash_join_node::Condition::Ordinary(v)) => Ok(v.keys.capacity()),
        Some(wire::hash_join_node::Condition::NullAwareEqual(v)) => {
            add(v.value_equal_keys.capacity(), v.scope_keys.capacity())
        }
        None => Err(invalid("hash join condition is absent")),
    }
}

fn validate_wire_join_condition(node: &wire::HashJoinNode) -> Result<(), Error> {
    let (keys, _, _) = wire_join_parts(node)?;
    let naa = matches!(
        node.condition,
        Some(wire::hash_join_node::Condition::NullAwareEqual(_))
    );
    if (node.kind == wire::JoinKind::NullAwareLeftAnti as i32
        || node.kind == wire::JoinKind::LeftMark as i32)
        != naa
    {
        return Err(invalid("hash join kind and condition variant disagree"));
    }
    if naa && keys.is_empty() {
        return Err(invalid("hash join requires non-empty value equality keys"));
    }
    Ok(())
}

fn encode_join_keys(
    keys: &[p::JoinKey],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::JoinKey>, Error> {
    let mut output = reserve(keys.len(), w)?;
    for key in keys {
        output.push(wire::JoinKey {
            left_root: Some(key.left.get()),
            right_root: Some(key.right.get()),
            null_safe: key.null_safe,
            match_relation: match key.match_relation {
                novarocks_type_contract::JoinKeyMatch::Grouping => {
                    wire::JoinKeyMatch::Grouping as i32
                }
                novarocks_type_contract::JoinKeyMatch::IeeeFloat => {
                    wire::JoinKeyMatch::IeeeFloat as i32
                }
            },
        });
        w.step()?;
    }
    Ok(output)
}

fn decode_join_keys(
    keys: &[wire::JoinKey],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Box<[p::JoinKey]>, Error> {
    let mut output = reserve(keys.len(), w)?;
    for key in keys {
        let match_relation = match wire::JoinKeyMatch::try_from(key.match_relation) {
            Ok(wire::JoinKeyMatch::Grouping) => novarocks_type_contract::JoinKeyMatch::Grouping,
            Ok(wire::JoinKeyMatch::IeeeFloat) if !key.null_safe => {
                novarocks_type_contract::JoinKeyMatch::IeeeFloat
            }
            Ok(wire::JoinKeyMatch::IeeeFloat) => {
                return Err(invalid("IEEE hash equality cannot be null-safe"));
            }
            _ => return Err(invalid("missing or unknown hash join key match relation")),
        };
        output.push(p::JoinKey {
            left: novarocks_expression_contract::RootOrdinal::new(required(key.left_root, w)?),
            right: novarocks_expression_contract::RootOrdinal::new(required(key.right_root, w)?),
            null_safe: key.null_safe,
            match_relation,
        });
        w.step()?;
    }
    boxed(output, w)
}

fn physical_outer(input: &p::PhysicalNode) -> Result<(usize, usize), Error> {
    match &input.kind {
        p::NodeKind::HashJoin {
            condition,
            null_extended,
            ..
        } => {
            let (keys, scopes, _) = physical_join_parts(condition);
            let count = add(keys.len(), scopes.len())?;
            Ok((
                add(count, null_extended.len())?,
                add(
                    bytes::<p::JoinKey>(count)?,
                    bytes::<p::ValueId>(null_extended.len())?,
                )?,
            ))
        }
        p::NodeKind::NestLoopJoin { null_extended, .. } => Ok((
            null_extended.len(),
            bytes::<p::ValueId>(null_extended.len())?,
        )),
        p::NodeKind::Sort { order_by, mode } => {
            let partition = physical_partitions(mode);
            Ok((
                add(order_by.len(), partition.len())?,
                add(
                    bytes::<p::SortExpr>(order_by.len())?,
                    bytes::<p::SortExpr>(partition.len())?,
                )?,
            ))
        }
        p::NodeKind::Window(v) => Ok((
            add(add(v.partition_by.len(), v.order_by.len())?, v.calls.len())?,
            add(
                add(
                    bytes::<p::SortExpr>(v.partition_by.len())?,
                    bytes::<p::SortExpr>(v.order_by.len())?,
                )?,
                bytes::<p::WindowCallSpec>(v.calls.len())?,
            )?,
        )),
        p::NodeKind::SetOp { input_mappings, .. } => Ok((
            input_mappings.len(),
            bytes::<Box<[p::ValueId]>>(input_mappings.len())?,
        )),
        p::NodeKind::ExchangeSource { imports, .. } => Ok((
            imports.len(),
            bytes::<(p::ValueId, p::ValueId)>(imports.len())?,
        )),
        _ => Err(invalid("physical node is outside relational family")),
    }
}
fn wire_outer(input: &wire::PhysicalNode) -> Result<(usize, usize), Error> {
    match input.kind.as_ref() {
        Some(wire::physical_node::Kind::HashJoin(v)) => {
            let (keys, scopes, _) = wire_join_parts(v)?;
            Ok((
                add(
                    add(keys.len(), scopes.len())?,
                    v.null_extended_value_ids.len(),
                )?,
                add(
                    bytes::<wire::JoinKey>(wire_join_capacity(v)?)?,
                    bytes::<u32>(v.null_extended_value_ids.capacity())?,
                )?,
            ))
        }
        Some(wire::physical_node::Kind::NestLoopJoin(v)) => Ok((
            v.null_extended_value_ids.len(),
            bytes::<u32>(v.null_extended_value_ids.capacity())?,
        )),
        Some(wire::physical_node::Kind::Sort(v)) => {
            let mode = v
                .mode
                .as_ref()
                .ok_or_else(|| invalid("relational sort mode is absent"))?;
            let partition = wire_partitions(mode)?;
            Ok((
                add(v.order_by.len(), partition.len())?,
                add(
                    bytes::<wire::SortExpression>(v.order_by.capacity())?,
                    bytes::<wire::SortExpression>(wire_partition_capacity(mode)?)?,
                )?,
            ))
        }
        Some(wire::physical_node::Kind::Window(v)) => Ok((
            add(add(v.partition_by.len(), v.order_by.len())?, v.calls.len())?,
            add(
                add(
                    bytes::<wire::SortExpression>(v.partition_by.capacity())?,
                    bytes::<wire::SortExpression>(v.order_by.capacity())?,
                )?,
                bytes::<wire::WindowCall>(v.calls.capacity())?,
            )?,
        )),
        Some(wire::physical_node::Kind::SetOperation(v)) => Ok((
            v.input_mappings.len(),
            bytes::<wire::ValueIds>(v.input_mappings.capacity())?,
        )),
        Some(wire::physical_node::Kind::ExchangeSource(v)) => Ok((
            v.imports.len(),
            bytes::<wire::ValueMapping>(v.imports.capacity())?,
        )),
        _ => Err(invalid("wire node is outside relational family")),
    }
}
fn prepare_encode(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    l: Option<RelationalNodeProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
    mut parent: Option<&mut NodeAdmit<'_>>,
) -> Result<RelationalNodeProjectionFacts, Error> {
    let namespace_floor = if parent.is_some() {
        values
            .retained_floor_header()?
            .max(expressions.retained_floor_header_in()?)
    } else {
        0
    };
    // Synchronous snapshots borrow the sole containing-node numerical author.
    // They replace this contribution; they do not create a meter or another B.
    let gate = |model: &Model,
                known: usize,
                parent: &mut Option<&mut NodeAdmit<'_>>|
     -> Result<(), Error> {
        if let Some(admit) = parent.as_deref_mut() {
            model.admit_in(source, values.count(), l, admit)?;
            if source < known.max(namespace_floor) {
                return Err(invalid("Repeat source invoice omits original backing"));
            }
        }
        Ok(())
    };

    let same = std::ptr::eq(values.types(), expressions.types())
        && if parent.is_some() {
            std::ptr::addr_eq(values.original_control(), expressions.original_control())
        } else {
            std::ptr::eq(values.original_control(), expressions.original_control())
        };
    if parent.is_none() {
        w.step()?;
    }
    if !same {
        return Err(invalid(
            "relational namespaces differ in original type table or control",
        ));
    }
    let (payload_items, backing) = physical_outer(input)?;
    let items = add(
        payload_items,
        add(input.required_inputs.len(), input.output.columns.len())?,
    )?;
    let mut known = add(physical_header_floor(input)?, backing)?;
    if parent.is_none() {
        count_prefix(input.inputs.len(), items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    let mut model = Model {
        inputs: input.inputs.len(),
        items,
        refs: input.output.columns.len(),
        ..Model::default()
    };
    encode_header_requests(input, &mut model)?;
    let occurrences = match &input.kind {
        p::NodeKind::HashJoin {
            kind,
            condition,
            null_extended,
            ..
        } => {
            validate_physical_join_condition(*kind, condition)?;
            let (keys, scopes, residual) = physical_join_parts(condition);
            model.request::<wire::JoinKey>(keys.len(), 1)?;
            model.request::<wire::JoinKey>(scopes.len(), 1)?;
            model.request::<u32>(null_extended.len(), 1)?;
            model.refs = add(model.refs, null_extended.len())?;
            add(
                mul(add(keys.len(), scopes.len())?, 2)?,
                usize::from(residual.is_some()),
            )?
        }
        p::NodeKind::NestLoopJoin {
            kind,
            predicate,
            null_extended,
            ..
        } => {
            if matches!(kind, p::JoinKind::NullAwareLeftAnti | p::JoinKind::LeftMark)
                && predicate.is_none()
            {
                return Err(invalid("three-valued nested-loop predicate is absent"));
            }
            model.request::<u32>(null_extended.len(), 1)?;
            model.refs = add(model.refs, null_extended.len())?;
            usize::from(predicate.is_some())
        }
        p::NodeKind::Sort { order_by, mode } => {
            let partition = physical_partitions(mode);
            model.request::<wire::SortExpression>(order_by.len(), 1)?;
            model.request::<wire::SortExpression>(partition.len(), 1)?;
            add(order_by.len(), partition.len())?
        }
        p::NodeKind::Window(v) => {
            model.request::<wire::SortExpression>(v.partition_by.len(), 1)?;
            model.request::<wire::SortExpression>(v.order_by.len(), 1)?;
            model.request::<wire::WindowCall>(v.calls.len(), 1)?;
            model.refs = add(model.refs, v.calls.len())?;
            let mut occurrences = add(v.partition_by.len(), v.order_by.len())?;
            for call in &v.calls {
                let nested = add(call.arguments.len(), call.order_by.len())?;
                model.items = add(model.items, nested)?;
                occurrences = add(occurrences, nested)?;
                known = add(
                    known,
                    add(
                        bytes::<novarocks_expression_contract::RootOrdinal>(call.arguments.len())?,
                        bytes::<p::SortExpr>(call.order_by.len())?,
                    )?,
                )?;
                model.request::<u32>(call.arguments.len(), 1)?;
                model.request::<wire::SortExpression>(call.order_by.len(), 1)?;
                w.step()?;
            }
            occurrences
        }
        p::NodeKind::SetOp { input_mappings, .. } => {
            model.request::<wire::ValueIds>(input_mappings.len(), 1)?;
            for row in input_mappings {
                model.items = add(model.items, row.len())?;
                model.refs = add(model.refs, row.len())?;
                known = add(known, bytes::<p::ValueId>(row.len())?)?;
                model.request::<u32>(row.len(), 1)?;
                gate(&model, known, &mut parent)?;
                count_prefix(model.inputs, model.items, source, known, l, w)?;
                gate(&model, known, &mut parent)?;
                w.step()?;
            }
            0
        }
        p::NodeKind::ExchangeSource { imports, .. } => {
            model.request::<wire::ValueMapping>(imports.len(), 1)?;
            model.refs = add(model.refs, imports.len())?;
            0
        }
        _ => {
            return Err(invalid(
                "prepared physical kind is outside relational family",
            ));
        }
    };
    model.delegated_work = add(
        model.delegated_work,
        mul(occurrences, expressions.lookup_work_upper_bound()?)?,
    )?;
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
        count_prefix(input.inputs.len(), items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        if parent.is_some() {
            let pf = properties::properties_encode_numerical_facts_in(property, source)?;
            properties::check_properties_numerical_facts(pf, l.map(|limits| limits.properties))?;
            model.property(pf)?;
            gate(&model, known, &mut parent)?;
            properties::preflight_encode_observed(
                property,
                source,
                l.map(|limits| limits.properties),
                w,
            )?;
        } else {
            model.property(properties::preflight_encode_observed(
                property,
                source,
                l.map(|limits| limits.properties),
                w,
            )?)?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    let facts = if let Some(admit) = parent.as_mut() {
        model.facts_in(source, values.count(), l, admit, w)?
    } else {
        model.facts(source, values.count(), l, w)?
    };
    for id in &input.output.columns {
        reference(id.get(), values, w)?;
    }
    match &input.kind {
        p::NodeKind::HashJoin {
            condition,
            null_extended,
            ..
        } => {
            let (keys, scopes, residual) = physical_join_parts(condition);
            for key in keys.iter().chain(scopes) {
                expression_encode(key.left, expressions, w)?;
                expression_encode(key.right, expressions, w)?;
            }
            if let Some(residual) = residual {
                expression_encode(residual, expressions, w)?;
            }
            for id in null_extended {
                reference(id.get(), values, w)?;
            }
        }
        p::NodeKind::NestLoopJoin {
            predicate,
            null_extended,
            ..
        } => {
            if let Some(predicate) = predicate {
                expression_encode(*predicate, expressions, w)?;
            }
            for id in null_extended {
                reference(id.get(), values, w)?;
            }
        }
        p::NodeKind::Sort { order_by, mode } => {
            for key in order_by.iter().chain(physical_partitions(mode)) {
                expression_encode(key.expr, expressions, w)?;
            }
        }
        p::NodeKind::Window(v) => {
            for key in v.partition_by.iter().chain(v.order_by.iter()) {
                expression_encode(key.expr, expressions, w)?;
            }
            for call in &v.calls {
                for root in &call.arguments {
                    expression_encode(*root, expressions, w)?;
                }
                for key in &call.order_by {
                    expression_encode(key.expr, expressions, w)?;
                }
                expressions
                    .functions()
                    .scalar_source_id_observed(&call.binding, w)?;
                if let Some(binding) = &call.aggregate_binding {
                    expressions.aggregates().source_id_observed(binding, w)?;
                }
                reference(call.output.get(), values, w)?;
            }
        }
        p::NodeKind::SetOp { input_mappings, .. } => {
            for row in input_mappings {
                for id in row {
                    reference(id.get(), values, w)?;
                }
                w.step()?;
            }
        }
        p::NodeKind::ExchangeSource { imports, .. } => {
            for (_, destination) in imports {
                reference(destination.get(), values, w)?;
            }
        }
        _ => unreachable!(),
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        physical_property_refs(property, values, w)?;
    }
    Ok(facts)
}
fn validate_sort(
    input: &wire::SortExpression,
    e: &DecodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let direction = properties::decode_direction(input.direction);
    w.step()?;
    direction?;
    let nulls = properties::decode_nulls(input.null_ordering);
    w.step()?;
    nulls?;
    expression_decode(required(input.root_ordinal, w)?, e, w)
}
fn prepare_decode(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    functions: &MaterializedFunctionBindings<'_, '_>,
    aggregates: &MaterializedAggregateBindings<'_, '_, '_>,
    binding_limits: BindingProjectionLimits,
    source: usize,
    l: Option<RelationalNodeProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
    mut parent: Option<&mut NodeAdmit<'_>>,
) -> Result<RelationalNodeProjectionFacts, Error> {
    let values = expressions.values();
    let namespace_floor = if parent.is_some() {
        values
            .retained_floor_header()?
            .max(expressions.retained_floor_header_in()?)
    } else {
        0
    };
    // Synchronous snapshots borrow the sole containing-node numerical author.
    // They replace this contribution; they do not create a meter or another B.
    let gate = |model: &Model,
                known: usize,
                parent: &mut Option<&mut NodeAdmit<'_>>|
     -> Result<(), Error> {
        if let Some(admit) = parent.as_deref_mut() {
            model.admit_in(source, values.count(), l, admit)?;
            if source < known.max(namespace_floor) {
                return Err(invalid("Repeat source invoice omits original backing"));
            }
        }
        Ok(())
    };

    let (payload_items, backing) = wire_outer(input)?;
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("relational output port is absent"))?;
    if parent.is_none() {
        required(port.node_id, w)?;
    } else {
        port.node_id
            .ok_or_else(|| invalid("node output ID is absent"))?;
    }
    let output_properties = input
        .output_properties
        .as_ref()
        .ok_or_else(|| invalid("relational output properties are absent"))?;
    let items = add(
        payload_items,
        add(input.required_inputs.len(), port.value_ids.len())?,
    )?;
    let mut known = add(wire_header_floor(input, port)?, backing)?;
    if parent.is_none() {
        count_prefix(input.input_node_ids.len(), items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    let mut model = Model {
        inputs: input.input_node_ids.len(),
        items,
        refs: port.value_ids.len(),
        ..Model::default()
    };
    decode_header_requests(input, port, &mut model)?;
    let occurrences = match input.kind.as_ref() {
        Some(wire::physical_node::Kind::HashJoin(v)) => {
            validate_wire_join_condition(v)?;
            let (keys, scopes, residual) = wire_join_parts(v)?;
            model.request::<p::JoinKey>(keys.len(), 2)?;
            model.request::<p::JoinKey>(scopes.len(), 2)?;
            model.request::<p::ValueId>(v.null_extended_value_ids.len(), 2)?;
            model.refs = add(model.refs, v.null_extended_value_ids.len())?;
            add(
                mul(add(keys.len(), scopes.len())?, 2)?,
                usize::from(residual.is_some()),
            )?
        }
        Some(wire::physical_node::Kind::NestLoopJoin(v)) => {
            if (v.kind == wire::JoinKind::NullAwareLeftAnti as i32
                || v.kind == wire::JoinKind::LeftMark as i32)
                && v.predicate_root.is_none()
            {
                return Err(invalid("three-valued nested-loop predicate is absent"));
            }
            model.request::<p::ValueId>(v.null_extended_value_ids.len(), 2)?;
            model.refs = add(model.refs, v.null_extended_value_ids.len())?;
            usize::from(v.predicate_root.is_some())
        }
        Some(wire::physical_node::Kind::Sort(v)) => {
            let partition = wire_partitions(
                v.mode
                    .as_ref()
                    .ok_or_else(|| invalid("relational sort mode is absent"))?,
            )?;
            model.request::<p::SortExpr>(v.order_by.len(), 2)?;
            model.request::<p::SortExpr>(partition.len(), 2)?;
            add(v.order_by.len(), partition.len())?
        }
        Some(wire::physical_node::Kind::Window(v)) => {
            model.request::<p::SortExpr>(v.partition_by.len(), 2)?;
            model.request::<p::SortExpr>(v.order_by.len(), 2)?;
            model.request::<p::WindowCallSpec>(v.calls.len(), 2)?;
            model.refs = add(model.refs, v.calls.len())?;
            let mut occurrences = add(v.partition_by.len(), v.order_by.len())?;
            for call in &v.calls {
                let nested = add(call.argument_roots.len(), call.order_by.len())?;
                model.items = add(model.items, nested)?;
                occurrences = add(occurrences, nested)?;
                known = add(
                    known,
                    add(
                        bytes::<u32>(call.argument_roots.capacity())?,
                        bytes::<wire::SortExpression>(call.order_by.capacity())?,
                    )?,
                )?;
                model.request::<novarocks_expression_contract::RootOrdinal>(
                    call.argument_roots.len(),
                    2,
                )?;
                model.request::<p::SortExpr>(call.order_by.len(), 2)?;
                if call.aggregate_binding_id.is_some() {
                    model.request::<p::AggregateBinding>(1, 1)?;
                }
                w.step()?;
            }
            occurrences
        }
        Some(wire::physical_node::Kind::SetOperation(v)) => {
            model.request::<Box<[p::ValueId]>>(v.input_mappings.len(), 2)?;
            for row in &v.input_mappings {
                model.items = add(model.items, row.value_ids.len())?;
                model.refs = add(model.refs, row.value_ids.len())?;
                known = add(known, bytes::<u32>(row.value_ids.capacity())?)?;
                model.request::<p::ValueId>(row.value_ids.len(), 2)?;
                gate(&model, known, &mut parent)?;
                count_prefix(model.inputs, model.items, source, known, l, w)?;
                gate(&model, known, &mut parent)?;
                w.step()?;
            }
            0
        }
        Some(wire::physical_node::Kind::ExchangeSource(v)) => {
            model.request::<(p::ValueId, p::ValueId)>(v.imports.len(), 2)?;
            model.refs = add(model.refs, v.imports.len())?;
            0
        }
        _ => return Err(invalid("prepared wire kind is outside relational family")),
    };
    model.delegated_work = add(
        model.delegated_work,
        mul(occurrences, expressions.lookup_work_upper_bound()?)?,
    )?;
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
        count_prefix(input.input_node_ids.len(), items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_properties))
    {
        if parent.is_some() {
            let pf = properties::properties_decode_numerical_facts_in(property, source)?;
            properties::check_properties_numerical_facts(pf, l.map(|limits| limits.properties))?;
            model.property(pf)?;
            gate(&model, known, &mut parent)?;
            properties::preflight_decode_observed(
                property,
                source,
                l.map(|limits| limits.properties),
                w,
            )?;
        } else {
            model.property(properties::preflight_decode_observed(
                property,
                source,
                l.map(|limits| limits.properties),
                w,
            )?)?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    let facts = if let Some(admit) = parent.as_mut() {
        model.facts_in(source, values.count(), l, admit, w)?
    } else {
        model.facts(source, values.count(), l, w)?
    };
    for id in &port.value_ids {
        reference(*id, values, w)?;
    }
    match input.kind.as_ref() {
        Some(wire::physical_node::Kind::HashJoin(v)) => {
            decode_join_kind(v.kind, w)?;
            decode_join_side(v.build_side, w)?;
            decode_join_distribution(v.distribution, w)?;
            let (keys, scopes, residual) = wire_join_parts(v)?;
            for key in keys.iter().chain(scopes) {
                expression_decode(required(key.left_root, w)?, expressions, w)?;
                expression_decode(required(key.right_root, w)?, expressions, w)?;
            }
            if let Some(residual) = residual {
                expression_decode(residual, expressions, w)?;
            }
            for id in &v.null_extended_value_ids {
                reference(*id, values, w)?;
            }
        }
        Some(wire::physical_node::Kind::NestLoopJoin(v)) => {
            decode_join_kind(v.kind, w)?;
            decode_nest_distribution(v.distribution, w)?;
            if let Some(predicate) = v.predicate_root {
                expression_decode(predicate, expressions, w)?;
            }
            for id in &v.null_extended_value_ids {
                reference(*id, values, w)?;
            }
        }
        Some(wire::physical_node::Kind::Sort(v)) => {
            let mode = v
                .mode
                .as_ref()
                .ok_or_else(|| invalid("relational sort mode is absent"))?;
            if let Some(wire::sort_mode::Kind::PartitionTopN(top)) = mode.kind.as_ref() {
                decode_partition_kind(top.kind, w)?;
            }
            for key in v.order_by.iter().chain(wire_partitions(mode)?) {
                validate_sort(key, expressions, w)?;
            }
        }
        Some(wire::physical_node::Kind::Window(v)) => {
            for key in v.partition_by.iter().chain(v.order_by.iter()) {
                validate_sort(key, expressions, w)?;
            }
            for call in &v.calls {
                for root in &call.argument_roots {
                    expression_decode(*root, expressions, w)?;
                }
                for key in &call.order_by {
                    validate_sort(key, expressions, w)?;
                }
                reference(required(call.output_value_id, w)?, values, w)?;
                if let Some(frame) = &call.frame {
                    crate::physical_expression_v2::decode_window_frame(frame)?;
                }
            }
        }
        Some(wire::physical_node::Kind::SetOperation(v)) => {
            decode_set_kind(v.kind, w)?;
            for row in &v.input_mappings {
                for id in &row.value_ids {
                    reference(*id, values, w)?;
                }
                w.step()?;
            }
        }
        Some(wire::physical_node::Kind::ExchangeSource(v)) => {
            required(v.edge_id, w)?;
            for pair in &v.imports {
                required(pair.source_value_id, w)?;
                reference(required(pair.destination_value_id, w)?, values, w)?;
            }
        }
        _ => unreachable!(),
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_properties))
    {
        wire_property_refs(property, values, w)?;
    }
    if let Some(wire::physical_node::Kind::Window(window)) = &input.kind {
        if let Some(parent) = parent {
            // Reborrow the same callback for this one nested synchronous loan.
            // It creates no checkpoint, invoice, retained token or authority.
            let mut forward = |facts: &NodeProjectionFacts| parent(facts);
            return window_copy_facts(
                window,
                expressions,
                functions,
                aggregates,
                WindowCopyProjection {
                    model,
                    source,
                    dependency: known,
                    limits: l,
                },
                binding_limits,
                Some(&mut forward),
                w,
            );
        }
        return window_copy_facts(
            window,
            expressions,
            functions,
            aggregates,
            WindowCopyProjection {
                model,
                source,
                dependency: known,
                limits: l,
            },
            binding_limits,
            None,
            w,
        );
    }
    Ok(facts)
}
/// Admit every signature container across the real Window group before any
/// deep type walk. Delegated prefixes retain the containing node's sole gate.
struct WindowCopyProjection {
    model: Model,
    source: usize,
    dependency: usize,
    limits: Option<RelationalNodeProjectionLimits>,
}
fn window_copy_facts<'parent>(
    window: &wire::WindowNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    functions: &MaterializedFunctionBindings<'_, '_>,
    aggregates: &MaterializedAggregateBindings<'_, '_, '_>,
    projection: WindowCopyProjection,
    binding_limits: BindingProjectionLimits,
    parent: Option<&'parent mut NodeAdmit<'parent>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, Error> {
    let WindowCopyProjection {
        model,
        source,
        dependency,
        limits,
    } = projection;
    let observed = parent.is_some();
    let mut child = MaterializationModel::for_composition(
        window.calls.len(),
        expressions.lookup_work_upper_bound()?,
        source,
        dependency,
    );
    match parent {
        Some(parent) => child.compose_in_node_in(
            model,
            expressions.values().count(),
            limits,
            binding_limits,
            parent,
        )?,
        None => {
            child.compose_in_node(model, expressions.values().count(), limits, binding_limits)?
        }
    }
    for call in &window.calls {
        let binding = window_binding(functions, required(call.function_binding_id, work)?, work)?;
        if observed {
            preflight_scalar_signature_copy_counts_in(
                binding,
                &mut child,
                binding_limits,
                &mut |_| Ok(()),
                work,
            )?;
        } else {
            preflight_scalar_signature_copy_counts(binding, &mut child, binding_limits, work)?;
        }
        if let Some(id) = call.aggregate_binding_id {
            let binding = aggregates
                .definition_observed(id, work)?
                .ok_or_else(|| invalid("window aggregate binding is absent"))?;
            if observed {
                preflight_aggregate_binding_copy_counts_in(
                    binding,
                    &mut child,
                    binding_limits,
                    &mut |_| Ok(()),
                    work,
                )?;
            } else {
                preflight_aggregate_binding_copy_counts(binding, &mut child, binding_limits, work)?;
            }
        }
        work.step()?;
    }
    child.node_facts(0, work)?;
    for call in &window.calls {
        let binding = window_binding(functions, required(call.function_binding_id, work)?, work)?;
        if observed {
            preflight_scalar_signature_copy_types_in(
                binding,
                &mut child,
                binding_limits,
                &mut |_| Ok(()),
                work,
            )?;
        } else {
            preflight_scalar_signature_copy_types(binding, &mut child, binding_limits, work)?;
        }
        if let Some(id) = call.aggregate_binding_id {
            let binding = aggregates
                .definition_observed(id, work)?
                .ok_or_else(|| invalid("window aggregate binding is absent"))?;
            if observed {
                preflight_aggregate_binding_copy_types_in(
                    binding,
                    &mut child,
                    binding_limits,
                    &mut |_| Ok(()),
                    work,
                )?;
            } else {
                preflight_aggregate_binding_copy_types(binding, &mut child, binding_limits, work)?;
            }
        }
        work.step()?;
    }
    Ok(child.node_facts(mul(2, child.facts.cumulative_work_upper_bound)?, work)?)
}

pub(crate) fn encode_sorts(
    input: &[p::SortExpr],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::SortExpression>, Error> {
    let mut output = reserve(input.len(), w)?;
    for key in input {
        output.push(wire::SortExpression {
            root_ordinal: Some(key.expr.get()),
            direction: properties::encode_direction(key.direction),
            null_ordering: properties::encode_nulls(key.null_ordering),
        });
        w.step()?;
    }
    Ok(output)
}
pub(crate) fn decode_sorts(
    input: &[wire::SortExpression],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Box<[p::SortExpr]>, Error> {
    let mut output = reserve(input.len(), w)?;
    for key in input {
        output.push(p::SortExpr {
            expr: novarocks_expression_contract::RootOrdinal::new(required(key.root_ordinal, w)?),
            direction: properties::decode_direction(key.direction)?,
            null_ordering: properties::decode_nulls(key.null_ordering)?,
        });
        w.step()?;
    }
    boxed(output, w)
}
fn encode_sort_mode(
    mode: &p::SortMode,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::SortMode, Error> {
    let kind = match mode {
        p::SortMode::Global => wire::sort_mode::Kind::Global(Empty {}),
        p::SortMode::Analytic { partition_by } => {
            wire::sort_mode::Kind::Analytic(wire::AnalyticSort {
                partition_by: encode_sorts(partition_by, w)?,
            })
        }
        p::SortMode::PartitionTopN {
            partition_by,
            limit,
            kind,
        } => wire::sort_mode::Kind::PartitionTopN(wire::PartitionTopN {
            partition_by: encode_sorts(partition_by, w)?,
            limit: *limit,
            kind: encode_partition_kind(*kind),
        }),
    };
    w.step()?;
    Ok(wire::SortMode { kind: Some(kind) })
}
fn decode_sort_mode(
    mode: &wire::SortMode,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::SortMode, Error> {
    let output = match mode.kind.as_ref() {
        Some(wire::sort_mode::Kind::Global(_)) => p::SortMode::Global,
        Some(wire::sort_mode::Kind::Analytic(v)) => p::SortMode::Analytic {
            partition_by: decode_sorts(&v.partition_by, w)?,
        },
        Some(wire::sort_mode::Kind::PartitionTopN(v)) => p::SortMode::PartitionTopN {
            partition_by: decode_sorts(&v.partition_by, w)?,
            limit: v.limit,
            kind: decode_partition_kind(v.kind, w)?,
        },
        None => return Err(invalid("prepared sort mode is absent")),
    };
    w.step()?;
    Ok(output)
}
fn emit_encode(
    input: &p::PhysicalNode,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    l: Option<RelationalNodeProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) = encode_header(input, source, l, w)?;
    let kind = match &input.kind {
        p::NodeKind::HashJoin {
            kind,
            condition,
            build_side,
            distribution,
            null_extended,
        } => {
            let condition = match condition {
                p::HashJoinCondition::Ordinary { keys, residual } => {
                    wire::hash_join_node::Condition::Ordinary(wire::OrdinaryHashJoinCondition {
                        keys: encode_join_keys(keys, w)?,
                        residual_root: residual
                            .map(novarocks_expression_contract::RootOrdinal::get),
                    })
                }
                p::HashJoinCondition::NullAwareEqual {
                    value_equal_keys,
                    scope_keys,
                    eligibility,
                } => wire::hash_join_node::Condition::NullAwareEqual(
                    wire::NullAwareEqualHashJoinCondition {
                        value_equal_keys: encode_join_keys(value_equal_keys, w)?,
                        scope_keys: encode_join_keys(scope_keys, w)?,
                        eligibility_root: eligibility
                            .map(novarocks_expression_contract::RootOrdinal::get),
                    },
                ),
            };
            wire::physical_node::Kind::HashJoin(wire::HashJoinNode {
                kind: encode_join_kind(*kind),
                condition: Some(condition),
                build_side: encode_join_side(*build_side),
                distribution: encode_join_distribution(*distribution),
                null_extended_value_ids: encode_ids(null_extended, w)?,
            })
        }
        p::NodeKind::NestLoopJoin {
            kind,
            distribution,
            predicate,
            null_extended,
        } => wire::physical_node::Kind::NestLoopJoin(wire::NestLoopJoinNode {
            kind: encode_join_kind(*kind),
            distribution: encode_nest_distribution(*distribution),
            predicate_root: predicate.map(novarocks_expression_contract::RootOrdinal::get),
            null_extended_value_ids: encode_ids(null_extended, w)?,
        }),
        p::NodeKind::Sort { order_by, mode } => wire::physical_node::Kind::Sort(wire::SortNode {
            order_by: encode_sorts(order_by, w)?,
            mode: Some(encode_sort_mode(mode, w)?),
        }),
        p::NodeKind::Window(v) => {
            let mut outputs = reserve(v.calls.len(), w)?;
            for call in &v.calls {
                let mut argument_roots = reserve(call.arguments.len(), w)?;
                for root in &call.arguments {
                    argument_roots.push(root.get());
                    w.step()?;
                }
                outputs.push(wire::WindowCall {
                    function_binding_id: Some(
                        expressions
                            .functions()
                            .scalar_source_id_observed(&call.binding, w)?,
                    ),
                    aggregate_binding_id: call
                        .aggregate_binding
                        .as_ref()
                        .map(|binding| expressions.aggregates().source_id_observed(binding, w))
                        .transpose()?,
                    distinct: call.distinct,
                    argument_roots,
                    order_by: encode_sorts(&call.order_by, w)?,
                    frame: call
                        .frame
                        .as_ref()
                        .map(|frame| crate::physical_expression_v2::encode_window_frame(frame, w))
                        .transpose()?,
                    ignore_nulls: call.ignore_nulls,
                    output_value_id: Some(call.output.get()),
                });
                w.step()?;
            }
            wire::physical_node::Kind::Window(wire::WindowNode {
                partition_by: encode_sorts(&v.partition_by, w)?,
                order_by: encode_sorts(&v.order_by, w)?,
                calls: outputs,
            })
        }
        p::NodeKind::SetOp {
            kind,
            input_mappings,
        } => {
            let mut output = reserve(input_mappings.len(), w)?;
            for row in input_mappings {
                output.push(wire::ValueIds {
                    value_ids: encode_ids(row, w)?,
                });
                w.step()?;
            }
            wire::physical_node::Kind::SetOperation(wire::SetOperationNode {
                kind: encode_set_kind(*kind),
                input_mappings: output,
            })
        }
        p::NodeKind::ExchangeSource { edge, imports } => {
            let mut output = reserve(imports.len(), w)?;
            for (from, to) in imports {
                output.push(wire::ValueMapping {
                    source_value_id: Some(from.get()),
                    destination_value_id: Some(to.get()),
                });
                w.step()?;
            }
            wire::physical_node::Kind::ExchangeSource(wire::ExchangeSourceNode {
                edge_id: Some(edge.get()),
                imports: output,
            })
        }
        _ => {
            return Err(invalid(
                "prepared physical kind is outside relational family",
            ));
        }
    };
    w.step()?;
    Ok(wire::PhysicalNode {
        expressions: None,
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(kind),
    })
}
fn window_binding<'a>(
    functions: &'a MaterializedFunctionBindings<'_, '_>,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a novarocks_expression_contract::BoundFunction, Error> {
    match functions.definition_observed(id, work)? {
        Some(MaterializedFunctionBinding::Scalar(binding)) => Ok(binding),
        _ => Err(invalid("window scalar-result binding is absent")),
    }
}
fn emit_decode(
    input: &wire::PhysicalNode,
    functions: &MaterializedFunctionBindings<'_, '_>,
    aggregates: &MaterializedAggregateBindings<'_, '_, '_>,
    source: usize,
    l: Option<RelationalNodeProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) = decode_header(input, source, l, w)?;
    let kind = match input.kind.as_ref() {
        Some(wire::physical_node::Kind::HashJoin(v)) => {
            let condition = match v.condition.as_ref() {
                Some(wire::hash_join_node::Condition::Ordinary(v)) => {
                    p::HashJoinCondition::Ordinary {
                        keys: decode_join_keys(&v.keys, w)?,
                        residual: v
                            .residual_root
                            .map(novarocks_expression_contract::RootOrdinal::new),
                    }
                }
                Some(wire::hash_join_node::Condition::NullAwareEqual(v)) => {
                    let value_equal_keys = decode_join_keys(&v.value_equal_keys, w)?;
                    for key in &value_equal_keys {
                        if key.match_relation != novarocks_type_contract::JoinKeyMatch::Grouping {
                            return Err(invalid(
                                "NULL-aware membership value keys require grouping equality",
                            ));
                        }
                        w.step()?;
                    }
                    p::HashJoinCondition::NullAwareEqual {
                        value_equal_keys,
                        scope_keys: decode_join_keys(&v.scope_keys, w)?,
                        eligibility: v
                            .eligibility_root
                            .map(novarocks_expression_contract::RootOrdinal::new),
                    }
                }
                None => return Err(invalid("hash join condition is absent")),
            };
            p::NodeKind::HashJoin {
                kind: decode_join_kind(v.kind, w)?,
                condition,
                build_side: decode_join_side(v.build_side, w)?,
                distribution: decode_join_distribution(v.distribution, w)?,
                null_extended: decode_ids(&v.null_extended_value_ids, w)?,
            }
        }
        Some(wire::physical_node::Kind::NestLoopJoin(v)) => p::NodeKind::NestLoopJoin {
            kind: decode_join_kind(v.kind, w)?,
            distribution: decode_nest_distribution(v.distribution, w)?,
            predicate: v
                .predicate_root
                .map(novarocks_expression_contract::RootOrdinal::new),
            null_extended: decode_ids(&v.null_extended_value_ids, w)?,
        },
        Some(wire::physical_node::Kind::Sort(v)) => p::NodeKind::Sort {
            order_by: decode_sorts(&v.order_by, w)?,
            mode: decode_sort_mode(
                v.mode
                    .as_ref()
                    .ok_or_else(|| invalid("prepared sort mode is absent"))?,
                w,
            )?,
        },
        Some(wire::physical_node::Kind::Window(v)) => {
            let mut outputs = reserve(v.calls.len(), w)?;
            for call in &v.calls {
                let mut arguments = reserve(call.argument_roots.len(), w)?;
                for root in &call.argument_roots {
                    arguments.push(novarocks_expression_contract::RootOrdinal::new(*root));
                    w.step()?;
                }
                let binding = window_binding(functions, required(call.function_binding_id, w)?, w)?;
                outputs.push(p::WindowCallSpec {
                    binding: copy_scalar_signature_observed(binding, w)?,
                    distinct: call.distinct,
                    arguments: boxed(arguments, w)?,
                    order_by: decode_sorts(&call.order_by, w)?,
                    frame: call
                        .frame
                        .as_ref()
                        .map(crate::physical_expression_v2::decode_window_frame)
                        .transpose()?,
                    ignore_nulls: call.ignore_nulls,
                    aggregate_binding: call
                        .aggregate_binding_id
                        .map(|id| -> Result<_, Error> {
                            let binding = aggregates
                                .definition_observed(id, w)?
                                .ok_or_else(|| invalid("window aggregate binding is absent"))?;
                            w.flush()?;
                            let copied = Box::new(copy_aggregate_binding_observed(binding, w)?);
                            w.step()?;
                            w.flush()?;
                            Ok(copied)
                        })
                        .transpose()?,
                    output: p::ValueId::new(required(call.output_value_id, w)?),
                });
                w.step()?;
            }
            p::NodeKind::Window(p::WindowSpec {
                partition_by: decode_sorts(&v.partition_by, w)?,
                order_by: decode_sorts(&v.order_by, w)?,
                calls: boxed(outputs, w)?,
            })
        }
        Some(wire::physical_node::Kind::SetOperation(v)) => {
            let mut mappings = reserve(v.input_mappings.len(), w)?;
            for row in &v.input_mappings {
                mappings.push(decode_ids(&row.value_ids, w)?);
                w.step()?;
            }
            p::NodeKind::SetOp {
                kind: decode_set_kind(v.kind, w)?,
                input_mappings: boxed(mappings, w)?,
            }
        }
        Some(wire::physical_node::Kind::ExchangeSource(v)) => {
            let mut output = reserve(v.imports.len(), w)?;
            for pair in &v.imports {
                output.push((
                    p::ValueId::new(required(pair.source_value_id, w)?),
                    p::ValueId::new(required(pair.destination_value_id, w)?),
                ));
                w.step()?;
            }
            p::NodeKind::ExchangeSource {
                edge: p::EdgeId::new(required(v.edge_id, w)?),
                imports: boxed(output, w)?,
            }
        }
        _ => return Err(invalid("prepared wire kind is outside relational family")),
    };
    w.step()?;
    Ok(p::PhysicalNode {
        expressions: None,
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind,
    })
}
/// Immutable preparation retains both actual emission loans. It does not rerun
/// source counting or reference validation when consumed.
pub struct PreparedRelationalNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source: usize,
    limits: Option<RelationalNodeProjectionLimits>,
    facts: RelationalNodeProjectionFacts,
}
impl PreparedRelationalNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &RelationalNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, RelationalNodeProjectionFacts), Error> {
        // Retaining this Values loan prevents exchanging the admitted namespace.
        let mut work =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        debug_assert!(std::ptr::eq(
            self.values.original_control(),
            self.expressions.original_control()
        ));
        let result = emit_encode(
            self.input,
            self.expressions,
            self.source,
            self.limits,
            &mut work,
        )
        .map(|node| (node, self.facts));
        finish(work, result)
    }

    /// Emit the already admitted original body in the containing caller scope.
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(wire::PhysicalNode, RelationalNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.values.original_control(), work.control()) {
            return Err(invalid(
                "node caller does not borrow the original controller",
            ));
        }
        admit(&self.facts)?;
        let node = emit_encode(self.input, self.expressions, self.source, self.limits, work)?;
        Ok((node, self.facts))
    }
}
pub fn prepare_relational_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: RelationalNodeProjectionLimits,
) -> Result<PreparedRelationalNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let limits = Some(limits);
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = prepare_encode(
        input,
        values,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
        None,
    );
    let facts = finish(work, result)?;
    Ok(PreparedRelationalNodeEncode {
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_relational_node_encode_in<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: impl Into<Option<RelationalNodeProjectionLimits>>,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedRelationalNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let limits = limits.into();
    if !std::ptr::addr_eq(values.original_control(), work.control()) {
        return Err(invalid(
            "node caller does not borrow the original controller",
        ));
    }
    let facts = prepare_encode(
        input,
        values,
        expressions,
        source_retained_bytes,
        limits,
        work,
        Some(admit),
    )?;
    Ok(PreparedRelationalNodeEncode {
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}

pub struct PreparedRelationalNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    functions: &'namespace MaterializedFunctionBindings<'loan, 'wire>,
    aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
    source: usize,
    limits: Option<RelationalNodeProjectionLimits>,
    facts: RelationalNodeProjectionFacts,
}
impl PreparedRelationalNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &RelationalNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, RelationalNodeProjectionFacts), Error> {
        let mut work =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Decode)?;
        let result = emit_decode(
            self.input,
            self.functions,
            self.aggregates,
            self.source,
            self.limits,
            &mut work,
        )
        .map(|node| (node, self.facts));
        finish(work, result)
    }

    /// Emit the already admitted original body in the containing caller scope.
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(p::PhysicalNode, RelationalNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.expressions.original_control(), work.control()) {
            return Err(invalid(
                "node caller does not borrow the original controller",
            ));
        }
        admit(&self.facts)?;
        let node = emit_decode(
            self.input,
            self.functions,
            self.aggregates,
            self.source,
            self.limits,
            work,
        )?;
        Ok((node, self.facts))
    }
}
pub fn prepare_relational_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    functions: &'namespace MaterializedFunctionBindings<'loan, 'wire>,
    aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
    binding_limits: BindingProjectionLimits,
    source_retained_bytes: usize,
    limits: RelationalNodeProjectionLimits,
) -> Result<PreparedRelationalNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let limits = Some(limits);
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = prepare_decode(
        input,
        expressions,
        functions,
        aggregates,
        binding_limits,
        source_retained_bytes,
        limits,
        &mut work,
        None,
    );
    let facts = finish(work, result)?;
    Ok(PreparedRelationalNodeDecode {
        input,
        expressions,
        functions,
        aggregates,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_relational_node_decode_in<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    functions: &'namespace MaterializedFunctionBindings<'loan, 'wire>,
    aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
    binding_limits: BindingProjectionLimits,
    source_retained_bytes: usize,
    limits: impl Into<Option<RelationalNodeProjectionLimits>>,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedRelationalNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let limits = limits.into();
    if !std::ptr::addr_eq(expressions.original_control(), work.control()) {
        return Err(invalid(
            "node caller does not borrow the original controller",
        ));
    }
    let facts = prepare_decode(
        input,
        expressions,
        functions,
        aggregates,
        binding_limits,
        source_retained_bytes,
        limits,
        work,
        Some(admit),
    )?;
    Ok(PreparedRelationalNodeDecode {
        input,
        expressions,
        functions,
        aggregates,
        source: source_retained_bytes,
        limits,
        facts,
    })
}

pub fn encode_relational_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: RelationalNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, RelationalNodeProjectionFacts), Error> {
    let limits = Some(limits);
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let facts = prepare_encode(
            input,
            values,
            expressions,
            source_retained_bytes,
            limits,
            &mut work,
            None,
        )?;
        let node = emit_encode(input, expressions, source_retained_bytes, limits, &mut work)?;
        Ok((node, facts))
    })();
    finish(work, result)
}
pub fn decode_relational_node(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    functions: &MaterializedFunctionBindings<'_, '_>,
    aggregates: &MaterializedAggregateBindings<'_, '_, '_>,
    binding_limits: BindingProjectionLimits,
    source_retained_bytes: usize,
    limits: RelationalNodeProjectionLimits,
) -> Result<(p::PhysicalNode, RelationalNodeProjectionFacts), Error> {
    let limits = Some(limits);
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = prepare_decode(
            input,
            expressions,
            functions,
            aggregates,
            binding_limits,
            source_retained_bytes,
            limits,
            &mut work,
            None,
        )?;
        let node = emit_decode(
            input,
            functions,
            aggregates,
            source_retained_bytes,
            limits,
            &mut work,
        )?;
        Ok((node, facts))
    })();
    finish(work, result)
}
#[cfg(test)]
mod tests;
