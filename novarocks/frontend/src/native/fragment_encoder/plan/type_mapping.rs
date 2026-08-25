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

use arrow::datatypes::{DataType, Field, TimeUnit};

use super::super::expr::encode_expr;
use novarocks_proto::{common, plan};
use novarocks_spi::connector::ConnectorRowMutationEffect;
use novarocks_sql::plan_read::{
    AggMode, DataPartition, HashSource, JoinDistribution, JoinExecutionMode, JoinKind,
    PartitionKind, PlanSetOpKind, RedistributeMode, SqlTopNType, TopNPhase,
};
use novarocks_types::logical::{LogicalType, logical_type_of_field};
use novarocks_types::schema::SqlType;

pub(super) fn encode_sql_type(src: &SqlType) -> Result<common::TypeDesc, String> {
    use common::type_desc::Kind;

    Ok(common::TypeDesc {
        kind: Some(match src {
            SqlType::Array(element) => Kind::List(Box::new(common::ListType {
                element: Some(Box::new(encode_sql_type(element)?)),
            })),
            SqlType::Map(key, value) => Kind::Map(Box::new(common::MapType {
                key: Some(Box::new(encode_sql_type(key)?)),
                value: Some(Box::new(encode_sql_type(value)?)),
            })),
            SqlType::Struct(fields) => Kind::Strct(common::StructType {
                fields: fields
                    .iter()
                    .map(|(name, ty)| {
                        Ok(common::StructField {
                            name: name.clone(),
                            r#type: Some(encode_sql_type(ty)?),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            }),
            other => Kind::Scalar(sql_scalar_type(other)?),
        }),
    })
}

pub(crate) fn encode_type(dt: &DataType) -> Result<common::TypeDesc, String> {
    encode_type_inner(dt, None)
}

fn encode_type_inner(dt: &DataType, field: Option<&Field>) -> Result<common::TypeDesc, String> {
    if let Some(logical_type) = field.and_then(logical_type_of_field) {
        return Ok(scalar_desc(
            logical_primitive(logical_type),
            None,
            None,
            None,
        ));
    }
    use common::type_desc::Kind;
    let kind = match dt {
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            Kind::List(Box::new(common::ListType {
                element: Some(Box::new(encode_type_inner(item.data_type(), Some(item))?)),
            }))
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(format!(
                    "MAP logical entries field must be Struct, got {:?}",
                    entries.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "MAP logical entries field must have exactly 2 children, got {}",
                    fields.len()
                ));
            }
            Kind::Map(Box::new(common::MapType {
                key: Some(Box::new(encode_type_inner(
                    fields[0].data_type(),
                    Some(&fields[0]),
                )?)),
                value: Some(Box::new(encode_type_inner(
                    fields[1].data_type(),
                    Some(&fields[1]),
                )?)),
            }))
        }
        DataType::Struct(fields) => Kind::Strct(common::StructType {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(common::StructField {
                        name: field.name().to_string(),
                        r#type: Some(encode_type_inner(field.data_type(), Some(field))?),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
        }),
        _ => return encode_scalar_type(dt),
    };
    Ok(common::TypeDesc { kind: Some(kind) })
}

fn encode_scalar_type(dt: &DataType) -> Result<common::TypeDesc, String> {
    use common::PrimitiveType;
    let (primitive, precision, scale, time_unit) = match dt {
        DataType::Null => (PrimitiveType::NullType, None, None, None),
        DataType::Boolean => (PrimitiveType::Boolean, None, None, None),
        DataType::Int8 => (PrimitiveType::Tinyint, None, None, None),
        DataType::Int16 => (PrimitiveType::Smallint, None, None, None),
        DataType::Int32 => (PrimitiveType::Int, None, None, None),
        DataType::Int64 => (PrimitiveType::Bigint, None, None, None),
        DataType::Float32 => (PrimitiveType::Float, None, None, None),
        DataType::Float64 => (PrimitiveType::Double, None, None, None),
        DataType::Decimal128(precision, scale) => {
            validate_decimal(*precision, *scale, 38, "Decimal128")?;
            (
                PrimitiveType::Decimal128,
                Some(i32::from(*precision)),
                Some(i32::from(*scale)),
                None,
            )
        }
        DataType::Decimal256(precision, scale) => {
            validate_decimal(*precision, *scale, 76, "Decimal256")?;
            (
                PrimitiveType::Decimal256,
                Some(i32::from(*precision)),
                Some(i32::from(*scale)),
                None,
            )
        }
        DataType::Date32 => (PrimitiveType::Date, None, None, None),
        DataType::Timestamp(unit, _) => {
            let time_unit = match unit {
                TimeUnit::Microsecond => None,
                TimeUnit::Nanosecond => Some(3),
                other => {
                    return Err(format!(
                        "unsupported timestamp unit {other:?}; only Microsecond/Nanosecond supported"
                    ));
                }
            };
            (PrimitiveType::Datetime, None, None, time_unit)
        }
        DataType::Time64(TimeUnit::Microsecond) => (PrimitiveType::Time, None, None, None),
        DataType::Time64(unit) => {
            return Err(format!(
                "unsupported Time64 unit {unit:?}; only Microsecond supported"
            ));
        }
        DataType::Utf8 | DataType::LargeUtf8 => (PrimitiveType::Varchar, None, None, None),
        DataType::Binary => (PrimitiveType::Varbinary, None, None, None),
        DataType::LargeBinary => (PrimitiveType::Variant, None, None, None),
        DataType::FixedSizeBinary(16) => (PrimitiveType::Largeint, None, None, None),
        other => {
            return Err(format!(
                "Arrow-to-native TypeDesc conversion does not support data type {other:?}"
            ));
        }
    };
    Ok(scalar_desc(primitive, precision, scale, time_unit))
}

fn scalar_desc(
    primitive: common::PrimitiveType,
    precision: Option<i32>,
    scale: Option<i32>,
    time_unit: Option<i32>,
) -> common::TypeDesc {
    common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: primitive as i32,
            len: None,
            precision,
            scale,
            time_unit,
        })),
    }
}

fn validate_decimal(
    precision: u8,
    scale: i8,
    max_precision: u8,
    label: &str,
) -> Result<(), String> {
    if precision == 0 || precision > max_precision {
        return Err(format!(
            "{label} precision {precision} must be between 1 and {max_precision}"
        ));
    }
    if scale < 0 || i32::from(scale) > i32::from(precision) {
        return Err(format!(
            "{label} scale {scale} must be between 0 and precision {precision}"
        ));
    }
    Ok(())
}

fn logical_primitive(logical_type: LogicalType) -> common::PrimitiveType {
    match logical_type {
        LogicalType::Json => common::PrimitiveType::Json,
        LogicalType::Hll => common::PrimitiveType::Hll,
        LogicalType::Bitmap => common::PrimitiveType::Bitmap,
        LogicalType::Object => common::PrimitiveType::Object,
        LogicalType::Percentile => common::PrimitiveType::Percentile,
    }
}

fn sql_scalar_type(src: &SqlType) -> Result<common::ScalarType, String> {
    use common::PrimitiveType;

    let (primitive, precision, scale, time_unit) = match src {
        SqlType::TinyInt => (PrimitiveType::Tinyint, None, None, None),
        SqlType::SmallInt => (PrimitiveType::Smallint, None, None, None),
        SqlType::Int => (PrimitiveType::Int, None, None, None),
        SqlType::BigInt => (PrimitiveType::Bigint, None, None, None),
        SqlType::LargeInt => (PrimitiveType::Largeint, None, None, None),
        SqlType::Float => (PrimitiveType::Float, None, None, None),
        SqlType::Double => (PrimitiveType::Double, None, None, None),
        SqlType::Decimal { precision, scale } => (
            PrimitiveType::Decimal128,
            Some(i32::from(*precision)),
            Some(i32::from(*scale)),
            None,
        ),
        SqlType::String => (PrimitiveType::Varchar, None, None, None),
        SqlType::Json => (PrimitiveType::Json, None, None, None),
        SqlType::Binary => (PrimitiveType::Varbinary, None, None, None),
        SqlType::Bitmap => (PrimitiveType::Bitmap, None, None, None),
        SqlType::Hll => (PrimitiveType::Hll, None, None, None),
        SqlType::Boolean => (PrimitiveType::Boolean, None, None, None),
        SqlType::Date => (PrimitiveType::Date, None, None, None),
        SqlType::DateTime => (PrimitiveType::Datetime, None, None, None),
        SqlType::DateTimeNs => (PrimitiveType::Datetime, None, None, Some(3)),
        SqlType::Time => (PrimitiveType::Time, None, None, None),
        SqlType::Variant => (PrimitiveType::Variant, None, None, None),
        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_) => {
            return Err("nested SqlType cannot be encoded as scalar TypeDesc".to_string());
        }
    };
    Ok(common::ScalarType {
        r#type: primitive as i32,
        len: None,
        precision,
        scale,
        time_unit,
    })
}

pub(super) fn encode_edge_partition_type(src: &DataPartition) -> i32 {
    match src.kind {
        PartitionKind::Unpartitioned => plan::PartitionType::Unpartitioned as i32,
        PartitionKind::Random => plan::PartitionType::Random as i32,
        PartitionKind::Hash => plan::PartitionType::Hash as i32,
    }
}

pub(super) fn encode_data_partition(src: &DataPartition) -> Result<plan::DataPartition, String> {
    Ok(plan::DataPartition {
        kind: match src.kind {
            PartitionKind::Unpartitioned => plan::PartitionKind::Unpartitioned as i32,
            PartitionKind::Random => plan::PartitionKind::Random as i32,
            PartitionKind::Hash => plan::PartitionKind::Hash as i32,
        },
        exprs: src
            .exprs
            .iter()
            .map(encode_expr)
            .collect::<Result<Vec<_>, String>>()?,
    })
}

pub(super) fn encode_join_kind(src: JoinKind) -> i32 {
    match src {
        JoinKind::Inner => plan::JoinKind::Inner as i32,
        JoinKind::LeftOuter => plan::JoinKind::LeftOuter as i32,
        JoinKind::RightOuter => plan::JoinKind::RightOuter as i32,
        JoinKind::FullOuter => plan::JoinKind::FullOuter as i32,
        JoinKind::Cross => plan::JoinKind::Cross as i32,
        JoinKind::LeftSemi => plan::JoinKind::LeftSemi as i32,
        JoinKind::RightSemi => plan::JoinKind::RightSemi as i32,
        JoinKind::LeftAnti => plan::JoinKind::LeftAnti as i32,
        JoinKind::RightAnti => plan::JoinKind::RightAnti as i32,
        JoinKind::NullAwareLeftAnti => plan::JoinKind::NullAwareLeftAnti as i32,
    }
}

pub(super) fn encode_join_distribution(src: &JoinDistribution) -> i32 {
    match src {
        JoinDistribution::Unknown => plan::JoinDistribution::Unknown as i32,
        JoinDistribution::Shuffle => plan::JoinDistribution::Shuffle as i32,
        JoinDistribution::Broadcast => plan::JoinDistribution::Broadcast as i32,
        JoinDistribution::Colocate => plan::JoinDistribution::Colocate as i32,
    }
}

pub(super) fn encode_join_execution_mode(src: JoinExecutionMode) -> i32 {
    match src {
        JoinExecutionMode::Broadcast => plan::JoinExecutionMode::Broadcast as i32,
        JoinExecutionMode::Partitioned => plan::JoinExecutionMode::Partitioned as i32,
        JoinExecutionMode::Colocate => plan::JoinExecutionMode::Colocate as i32,
    }
}

pub(super) fn encode_agg_mode(src: AggMode) -> i32 {
    match src {
        AggMode::Single => plan::AggMode::Single as i32,
        AggMode::Local => plan::AggMode::Local as i32,
        AggMode::Global => plan::AggMode::Global as i32,
        AggMode::DistinctGlobal => plan::AggMode::DistinctGlobal as i32,
        AggMode::DistinctLocal => plan::AggMode::DistinctLocal as i32,
    }
}

pub(super) fn encode_topn_phase(src: TopNPhase) -> i32 {
    match src {
        TopNPhase::Partial => plan::TopNPhase::TopnPhasePartial as i32,
        TopNPhase::Final => plan::TopNPhase::TopnPhaseFinal as i32,
    }
}

pub(super) fn encode_set_op_kind(src: PlanSetOpKind) -> i32 {
    match src {
        PlanSetOpKind::UnionAll => plan::PlanSetOpKind::UnionAll as i32,
        PlanSetOpKind::UnionDistinct => plan::PlanSetOpKind::UnionDistinct as i32,
        PlanSetOpKind::Intersect => plan::PlanSetOpKind::Intersect as i32,
        PlanSetOpKind::Except => plan::PlanSetOpKind::Except as i32,
    }
}

pub(super) fn encode_row_mutation_effect(src: ConnectorRowMutationEffect) -> i32 {
    match src {
        ConnectorRowMutationEffect::Delete => plan::RowMutationEffect::Delete as i32,
        ConnectorRowMutationEffect::Replace => plan::RowMutationEffect::Replace as i32,
        ConnectorRowMutationEffect::Insert => plan::RowMutationEffect::Insert as i32,
    }
}

/// Explicit SQL-to-native conversion for the SQL-owned TopN ranking fact.
pub(super) fn encode_sort_topn_type(src: SqlTopNType) -> i32 {
    match src {
        SqlTopNType::RowNumber => plan::SortTopNType::SortTopnTypeRowNumber as i32,
        SqlTopNType::Rank => plan::SortTopNType::SortTopnTypeRank as i32,
        SqlTopNType::DenseRank => plan::SortTopNType::SortTopnTypeDenseRank as i32,
    }
}

fn encode_hash_source(src: HashSource) -> i32 {
    match src {
        HashSource::ShuffleAgg => plan::HashSource::ShuffleAgg as i32,
        HashSource::ShuffleJoin => plan::HashSource::ShuffleJoin as i32,
    }
}

pub(super) fn encode_redistribute_mode(src: &RedistributeMode) -> plan::RedistributeMode {
    use plan::redistribute_mode::Mode;

    plan::RedistributeMode {
        mode: Some(match src {
            RedistributeMode::Gather => Mode::Gather(true),
            RedistributeMode::Hash { cols, source } => Mode::Hash(plan::RedistributeHash {
                cols: cols.iter().map(|id| id.0).collect(),
                source: encode_hash_source(*source),
            }),
            RedistributeMode::Broadcast => Mode::Broadcast(true),
        }),
    }
}

pub(super) fn usize_to_u64(value: usize) -> u64 {
    value as u64
}

#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
fn usize_to_u32(value: usize) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("value {value} does not fit in u32"))
}
