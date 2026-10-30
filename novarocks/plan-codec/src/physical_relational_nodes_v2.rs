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
    id: p::ExprId,
    e: &EncodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = e.expression_observed(id.get(), w)?.is_some();
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
    let found = e.definition_observed(id, w)?.is_some();
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
type PhysicalHashCondition = p::HashJoinCondition<Box<[p::JoinKey]>, p::ExprId>;

fn physical_join_parts(
    condition: &PhysicalHashCondition,
) -> (&[p::JoinKey], &[p::JoinKey], Option<p::ExprId>) {
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
    if (kind == p::JoinKind::NullAwareLeftAnti) != condition.is_null_aware() {
        return Err(invalid("hash join kind and condition variant disagree"));
    }
    if condition.is_null_aware() && condition.keys().is_empty() {
        return Err(invalid("hash join requires non-empty value equality keys"));
    }
    Ok(())
}

fn wire_join_parts(
    node: &wire::HashJoinNode,
) -> Result<(&[wire::JoinKey], &[wire::JoinKey], Option<u32>), Error> {
    match node.condition.as_ref() {
        Some(wire::hash_join_node::Condition::Ordinary(v)) => {
            Ok((&v.keys, &[], v.residual_expr_id))
        }
        Some(wire::hash_join_node::Condition::NullAwareEqual(v)) => {
            Ok((&v.value_equal_keys, &v.scope_keys, v.eligibility_expr_id))
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
    if (node.kind == wire::JoinKind::NullAwareLeftAnti as i32) != naa {
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
            left_expr_id: Some(key.left.get()),
            right_expr_id: Some(key.right.get()),
            null_safe: key.null_safe,
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
        output.push(p::JoinKey {
            left: p::ExprId::new(required(key.left_expr_id, w)?),
            right: p::ExprId::new(required(key.right_expr_id, w)?),
            null_safe: key.null_safe,
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
            add(
                add(v.partition_by.len(), v.order_by.len())?,
                v.expressions.len(),
            )?,
            add(
                add(
                    bytes::<p::SortExpr>(v.partition_by.len())?,
                    bytes::<p::SortExpr>(v.order_by.len())?,
                )?,
                bytes::<p::WindowExpression>(v.expressions.len())?,
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
            add(
                add(v.partition_by.len(), v.order_by.len())?,
                v.expressions.len(),
            )?,
            add(
                add(
                    bytes::<wire::SortExpression>(v.partition_by.capacity())?,
                    bytes::<wire::SortExpression>(v.order_by.capacity())?,
                )?,
                bytes::<wire::ExpressionOutput>(v.expressions.capacity())?,
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
    l: RelationalNodeProjectionLimits,
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
            if *kind == p::JoinKind::NullAwareLeftAnti && predicate.is_none() {
                return Err(invalid("null-aware nested-loop predicate is absent"));
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
            model.request::<wire::ExpressionOutput>(v.expressions.len(), 1)?;
            model.refs = add(model.refs, v.expressions.len())?;
            add(
                add(v.partition_by.len(), v.order_by.len())?,
                v.expressions.len(),
            )?
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
            properties::check_properties_numerical_facts(pf, l.properties)?;
            model.property(pf)?;
            gate(&model, known, &mut parent)?;
            properties::preflight_encode_observed(property, source, l.properties, w)?;
        } else {
            model.property(properties::preflight_encode_observed(
                property,
                source,
                l.properties,
                w,
            )?)?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    let facts = if let Some(admit) = parent {
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
            for output in &v.expressions {
                expression_encode(output.expression, expressions, w)?;
                reference(output.output.get(), values, w)?;
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
    expression_decode(required(input.expr_id, w)?, e, w)
}
fn prepare_decode(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source: usize,
    l: RelationalNodeProjectionLimits,
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
            if v.kind == wire::JoinKind::NullAwareLeftAnti as i32 && v.predicate_expr_id.is_none() {
                return Err(invalid("null-aware nested-loop predicate is absent"));
            }
            model.request::<p::ValueId>(v.null_extended_value_ids.len(), 2)?;
            model.refs = add(model.refs, v.null_extended_value_ids.len())?;
            usize::from(v.predicate_expr_id.is_some())
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
            model.request::<p::WindowExpression>(v.expressions.len(), 2)?;
            model.refs = add(model.refs, v.expressions.len())?;
            add(
                add(v.partition_by.len(), v.order_by.len())?,
                v.expressions.len(),
            )?
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
            properties::check_properties_numerical_facts(pf, l.properties)?;
            model.property(pf)?;
            gate(&model, known, &mut parent)?;
            properties::preflight_decode_observed(property, source, l.properties, w)?;
        } else {
            model.property(properties::preflight_decode_observed(
                property,
                source,
                l.properties,
                w,
            )?)?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    let facts = if let Some(admit) = parent {
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
                expression_decode(required(key.left_expr_id, w)?, expressions, w)?;
                expression_decode(required(key.right_expr_id, w)?, expressions, w)?;
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
            if let Some(predicate) = v.predicate_expr_id {
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
            for output in &v.expressions {
                expression_decode(required(output.expr_id, w)?, expressions, w)?;
                reference(required(output.value_id, w)?, values, w)?;
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
    Ok(facts)
}
pub(crate) fn encode_sorts(
    input: &[p::SortExpr],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::SortExpression>, Error> {
    let mut output = reserve(input.len(), w)?;
    for key in input {
        output.push(wire::SortExpression {
            expr_id: Some(key.expr.get()),
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
            expr: p::ExprId::new(required(key.expr_id, w)?),
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
    source: usize,
    l: RelationalNodeProjectionLimits,
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
                        residual_expr_id: residual.map(p::ExprId::get),
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
                        eligibility_expr_id: eligibility.map(p::ExprId::get),
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
            predicate_expr_id: predicate.map(p::ExprId::get),
            null_extended_value_ids: encode_ids(null_extended, w)?,
        }),
        p::NodeKind::Sort { order_by, mode } => wire::physical_node::Kind::Sort(wire::SortNode {
            order_by: encode_sorts(order_by, w)?,
            mode: Some(encode_sort_mode(mode, w)?),
        }),
        p::NodeKind::Window(v) => {
            let mut outputs = reserve(v.expressions.len(), w)?;
            for expression in &v.expressions {
                outputs.push(wire::ExpressionOutput {
                    expr_id: Some(expression.expression.get()),
                    value_id: Some(expression.output.get()),
                });
                w.step()?;
            }
            wire::physical_node::Kind::Window(wire::WindowNode {
                partition_by: encode_sorts(&v.partition_by, w)?,
                order_by: encode_sorts(&v.order_by, w)?,
                expressions: outputs,
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
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(kind),
    })
}
fn emit_decode(
    input: &wire::PhysicalNode,
    source: usize,
    l: RelationalNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) = decode_header(input, source, l, w)?;
    let kind = match input.kind.as_ref() {
        Some(wire::physical_node::Kind::HashJoin(v)) => {
            let condition = match v.condition.as_ref() {
                Some(wire::hash_join_node::Condition::Ordinary(v)) => {
                    p::HashJoinCondition::Ordinary {
                        keys: decode_join_keys(&v.keys, w)?,
                        residual: v.residual_expr_id.map(p::ExprId::new),
                    }
                }
                Some(wire::hash_join_node::Condition::NullAwareEqual(v)) => {
                    p::HashJoinCondition::NullAwareEqual {
                        value_equal_keys: decode_join_keys(&v.value_equal_keys, w)?,
                        scope_keys: decode_join_keys(&v.scope_keys, w)?,
                        eligibility: v.eligibility_expr_id.map(p::ExprId::new),
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
            predicate: v.predicate_expr_id.map(p::ExprId::new),
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
            let mut outputs = reserve(v.expressions.len(), w)?;
            for output in &v.expressions {
                outputs.push(p::WindowExpression {
                    expression: p::ExprId::new(required(output.expr_id, w)?),
                    output: p::ValueId::new(required(output.value_id, w)?),
                });
                w.step()?;
            }
            p::NodeKind::Window(p::WindowSpec {
                partition_by: decode_sorts(&v.partition_by, w)?,
                order_by: decode_sorts(&v.order_by, w)?,
                expressions: boxed(outputs, w)?,
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
    limits: RelationalNodeProjectionLimits,
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
        let result = emit_encode(self.input, self.source, self.limits, &mut work)
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
        let node = emit_encode(self.input, self.source, self.limits, work)?;
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
    limits: RelationalNodeProjectionLimits,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedRelationalNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
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
    source: usize,
    limits: RelationalNodeProjectionLimits,
    facts: RelationalNodeProjectionFacts,
}
impl PreparedRelationalNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &RelationalNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, RelationalNodeProjectionFacts), Error> {
        let mut work =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Decode)?;
        let result = emit_decode(self.input, self.source, self.limits, &mut work)
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
        let node = emit_decode(self.input, self.source, self.limits, work)?;
        Ok((node, self.facts))
    }
}
pub fn prepare_relational_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: RelationalNodeProjectionLimits,
) -> Result<PreparedRelationalNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = prepare_decode(
        input,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
        None,
    );
    let facts = finish(work, result)?;
    Ok(PreparedRelationalNodeDecode {
        input,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_relational_node_decode_in<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: RelationalNodeProjectionLimits,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedRelationalNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    if !std::ptr::addr_eq(expressions.original_control(), work.control()) {
        return Err(invalid(
            "node caller does not borrow the original controller",
        ));
    }
    let facts = prepare_decode(
        input,
        expressions,
        source_retained_bytes,
        limits,
        work,
        Some(admit),
    )?;
    Ok(PreparedRelationalNodeDecode {
        input,
        expressions,
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
        let node = emit_encode(input, source_retained_bytes, limits, &mut work)?;
        Ok((node, facts))
    })();
    finish(work, result)
}
pub fn decode_relational_node(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: RelationalNodeProjectionLimits,
) -> Result<(p::PhysicalNode, RelationalNodeProjectionFacts), Error> {
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = prepare_decode(
            input,
            expressions,
            source_retained_bytes,
            limits,
            &mut work,
            None,
        )?;
        let node = emit_decode(input, source_retained_bytes, limits, &mut work)?;
        Ok((node, facts))
    })();
    finish(work, result)
}
#[cfg(test)]
mod tests;
