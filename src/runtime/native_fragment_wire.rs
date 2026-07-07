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

//! Runtime wire adapters for native fragment submission.

use crate::common::types::UniqueId;
use crate::proto;
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::fragment_exec_params::FragmentExecParams;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
#[cfg(test)]
use crate::thrift::internal_service;
use crate::thrift::{data_sinks, partitions, types};

pub(crate) type DataStreamSink = data_sinks::TDataStreamSink;
pub(crate) type IcebergChangeStreamRouterBranch = data_sinks::TIcebergChangeStreamRouterBranch;
pub(crate) type IcebergChangeStreamRouterBranchKind =
    data_sinks::TIcebergChangeStreamRouterBranchKind;
pub(crate) type IcebergChangeStreamRouterSink = data_sinks::TIcebergChangeStreamRouterSink;
pub(crate) type MultiCastDataStreamSink = data_sinks::TMultiCastDataStreamSink;
pub(crate) type DataPartition = partitions::TDataPartition;

#[cfg(test)]
pub(crate) type SpillMode = internal_service::TSpillMode;
pub(crate) fn query_options_from_native(
    src: &proto::novarocks::QueryOptions,
) -> Result<QueryOptions, String> {
    QueryOptions::from_native(src)
}

pub(crate) fn runtime_filter_params_from_native(
    src: &proto::novarocks::RuntimeFilterParams,
) -> Result<RuntimeFilterParams, String> {
    RuntimeFilterParams::from_native(src)
}

pub(crate) fn endpoint_from_native(src: &str) -> Result<RuntimeEndpoint, String> {
    RuntimeEndpoint::parse(src)
}

pub(crate) fn destination_from_native(
    src: &proto::novarocks::Destination,
) -> Result<FragmentDestination, String> {
    let finst_id = src
        .finst_id
        .as_ref()
        .ok_or_else(|| "native Destination missing finst_id".to_string())?;
    Ok(FragmentDestination::new(
        types::TUniqueId::new(finst_id.hi, finst_id.lo),
        endpoint_from_native(&src.endpoint)?,
    ))
}

pub(crate) fn destinations_from_native(
    src: &[proto::novarocks::Destination],
) -> Result<Vec<FragmentDestination>, String> {
    src.iter().map(destination_from_native).collect()
}

pub(crate) fn exec_params_from_native(
    src: &proto::novarocks::InstanceParams,
    destinations: Vec<FragmentDestination>,
) -> Result<FragmentExecParams, String> {
    let query_id = src
        .query_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing query_id".to_string())?;
    let fragment_instance_id = src
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing fragment_instance_id".to_string())?;
    FragmentExecParams::new(
        UniqueId {
            hi: query_id.hi,
            lo: query_id.lo,
        },
        UniqueId {
            hi: fragment_instance_id.hi,
            lo: fragment_instance_id.lo,
        },
        Default::default(),
        src.per_exch_num_senders
            .iter()
            .map(|(node_id, count)| (*node_id, *count))
            .collect(),
        destinations,
    )
}

pub(crate) fn data_partition_without_exprs(
    src: &proto::plan::DataPartition,
) -> Result<DataPartition, String> {
    let partition_type = match proto::plan::PartitionKind::try_from(src.kind)
        .map_err(|_| format!("unknown native PartitionKind value {}", src.kind))?
    {
        proto::plan::PartitionKind::Unpartitioned => partitions::TPartitionType::UNPARTITIONED,
        proto::plan::PartitionKind::Random => partitions::TPartitionType::RANDOM,
        proto::plan::PartitionKind::Hash => partitions::TPartitionType::HASH_PARTITIONED,
        proto::plan::PartitionKind::Unspecified => {
            return Err("native DataPartition kind is unspecified".to_string());
        }
    };
    Ok(partitions::TDataPartition::new(
        partition_type,
        None::<Vec<crate::thrift::exprs::TExpr>>,
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    ))
}

pub(crate) fn data_stream_sink_from_native(
    src: &proto::plan::DataStreamSink,
) -> Result<DataStreamSink, String> {
    let output_partition = src
        .output_partition
        .as_ref()
        .ok_or_else(|| "native DATA_STREAM_SINK missing output_partition".to_string())
        .and_then(data_partition_without_exprs)?;
    let output_columns = (!src.output_columns.is_empty()).then_some(src.output_columns.clone());
    Ok(data_sinks::TDataStreamSink::new(
        src.dest_node_id,
        output_partition,
        None::<bool>,
        None::<bool>,
        None::<i32>,
        output_columns,
        src.limit,
    ))
}

pub(crate) fn stream_destination_from_native(
    src: &proto::plan::StreamDestination,
) -> Result<FragmentDestination, String> {
    let finst_id = src
        .finst_id
        .as_ref()
        .ok_or_else(|| "native StreamDestination missing finst_id".to_string())?;
    Ok(FragmentDestination::new(
        types::TUniqueId::new(finst_id.hi, finst_id.lo),
        endpoint_from_native(&src.endpoint)?,
    ))
}

pub(crate) fn stream_destinations_from_native(
    src: &proto::plan::StreamDestinationList,
) -> Result<Vec<FragmentDestination>, String> {
    src.destinations
        .iter()
        .map(stream_destination_from_native)
        .collect()
}

pub(crate) fn multi_cast_data_stream_sink_from_native(
    src: &proto::plan::MultiCastDataStreamSink,
) -> Result<MultiCastDataStreamSink, String> {
    if src.sinks.len() != src.destinations.len() {
        return Err(format!(
            "native MULTI_CAST_DATA_STREAM_SINK sinks size {} != destinations size {}",
            src.sinks.len(),
            src.destinations.len()
        ));
    }
    let sinks = src
        .sinks
        .iter()
        .map(data_stream_sink_from_native)
        .collect::<Result<Vec<_>, _>>()?;
    let destinations = src
        .destinations
        .iter()
        .map(stream_destinations_from_native)
        .collect::<Result<Vec<_>, _>>()?;
    let destinations = destinations
        .into_iter()
        .map(|group| {
            group
                .into_iter()
                .map(crate::runtime::fragment_exec_params::compat_destination_from_runtime)
                .collect()
        })
        .collect();
    Ok(data_sinks::TMultiCastDataStreamSink::new(
        sinks,
        destinations,
    ))
}

pub(crate) fn iceberg_change_stream_branch_kind_from_native(
    value: i32,
) -> Result<IcebergChangeStreamRouterBranchKind, String> {
    match proto::plan::ChangeStreamBranchKind::try_from(value)
        .map_err(|_| format!("unknown native ChangeStreamBranchKind value {value}"))?
    {
        proto::plan::ChangeStreamBranchKind::DeleteDv => {
            Ok(data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV)
        }
        proto::plan::ChangeStreamBranchKind::ReuseData => {
            Ok(data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA)
        }
        proto::plan::ChangeStreamBranchKind::FreshData => {
            Ok(data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA)
        }
        proto::plan::ChangeStreamBranchKind::Unspecified => {
            Err("native ChangeStreamBranchKind is unspecified".to_string())
        }
    }
}
