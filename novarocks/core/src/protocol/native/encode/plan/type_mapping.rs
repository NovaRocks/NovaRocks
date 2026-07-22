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

use super::super::expr::encode_expr;
use crate::proto::{common, plan};
use crate::sql::common::{ChangeStreamBranchKind, JoinKind};
use crate::sql::planner::distributed::write::sink::IcebergWriteSinkMode;
use crate::sql::planner::distributed::{DataPartition, PartitionKind};
use crate::sql::planner::physical::{
    AggMode, HashSource, JoinDistribution, JoinExecutionMode, PlanSetOpKind, RedistributeMode,
    TopNPhase,
};
use novarocks_catalog::schema::SqlType;

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

pub(super) fn encode_change_stream_branch_kind(src: ChangeStreamBranchKind) -> i32 {
    match src {
        ChangeStreamBranchKind::DeleteDv => plan::ChangeStreamBranchKind::DeleteDv as i32,
        ChangeStreamBranchKind::ReuseData => plan::ChangeStreamBranchKind::ReuseData as i32,
        ChangeStreamBranchKind::FreshData => plan::ChangeStreamBranchKind::FreshData as i32,
    }
}

pub(super) fn encode_sort_topn_type(src: crate::exec::node::sort::SortTopNType) -> i32 {
    match src {
        crate::exec::node::sort::SortTopNType::RowNumber => {
            plan::SortTopNType::SortTopnTypeRowNumber as i32
        }
        crate::exec::node::sort::SortTopNType::Rank => plan::SortTopNType::SortTopnTypeRank as i32,
        crate::exec::node::sort::SortTopNType::DenseRank => {
            plan::SortTopNType::SortTopnTypeDenseRank as i32
        }
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

pub(super) fn encode_iceberg_metadata_table_type(
    src: &crate::connector::iceberg::IcebergMetadataTableType,
) -> i32 {
    match src {
        crate::connector::iceberg::IcebergMetadataTableType::Files => {
            plan::IcebergMetadataTableType::Files as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Manifests => {
            plan::IcebergMetadataTableType::Manifests as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::LogicalIcebergMetadata => {
            plan::IcebergMetadataTableType::LogicalIcebergMetadata as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Snapshots => {
            plan::IcebergMetadataTableType::Snapshots as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::History => {
            plan::IcebergMetadataTableType::History as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Refs => {
            plan::IcebergMetadataTableType::Refs as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Partitions => {
            plan::IcebergMetadataTableType::Partitions as i32
        }
    }
}

pub(super) fn encode_iceberg_write_sink_mode(src: IcebergWriteSinkMode) -> i32 {
    match src {
        IcebergWriteSinkMode::Data => plan::IcebergWriteSinkMode::Data as i32,
        IcebergWriteSinkMode::RowLineageData => plan::IcebergWriteSinkMode::RowLineageData as i32,
        IcebergWriteSinkMode::PositionDeletes => plan::IcebergWriteSinkMode::PositionDeletes as i32,
        IcebergWriteSinkMode::DeletionVectors => plan::IcebergWriteSinkMode::DeletionVectors as i32,
        IcebergWriteSinkMode::EqualityDeletes => plan::IcebergWriteSinkMode::EqualityDeletes as i32,
    }
}

pub(super) fn usize_to_u64(value: usize) -> u64 {
    value as u64
}

fn usize_to_u32(value: usize) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("value {value} does not fit in u32"))
}
