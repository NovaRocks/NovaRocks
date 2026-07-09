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
#[cfg(all(test, feature = "compat"))]
use crate::thrift::internal_service;

pub(crate) type DataStreamSink = proto::plan::DataStreamSink;
pub(crate) type IcebergChangeStreamRouterBranch = proto::plan::IcebergChangeStreamBranchRoute;
pub(crate) type IcebergChangeStreamRouterBranchKind = proto::plan::ChangeStreamBranchKind;
pub(crate) type IcebergChangeStreamRouterSink = proto::plan::IcebergChangeStreamRouterSink;
pub(crate) type MultiCastDataStreamSink = proto::plan::MultiCastDataStreamSink;
pub(crate) type DataPartition = proto::plan::DataPartition;

#[cfg(all(test, feature = "compat"))]
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
        UniqueId {
            hi: finst_id.hi,
            lo: finst_id.lo,
        },
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
