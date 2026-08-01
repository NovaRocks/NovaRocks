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

//! Provider-private StarRocks external-connector contracts.
//!
//! This crate owns StarRocks read policy, opaque payloads and the control/
//! execution binding implementations.  It deliberately does not depend on a
//! NovaRocks host, Compat, or a concrete StarRocks wire client.

mod codec;
mod domain;

pub mod direct;

pub mod control;
pub mod execution;

pub use control::{
    StarRocksControlGeneration, StarRocksDirectSplitPlanner, StarRocksMetadataSource,
    StarRocksRpcSplitPlanner,
};
pub use direct::{
    StarRocksDirectColumnBinding, StarRocksDirectIoRuntime, StarRocksDirectLocation,
    StarRocksDirectLocationSource, StarRocksDirectMetadataLayout, StarRocksDirectSplit,
    StarRocksDirectStorageResolver, StarRocksDirectTabletDescriptor,
    StarRocksDirectTabletPlanningSource, StarRocksSharedDataDirectPlanner,
    StarRocksSharedDataDirectReaderFactory, StarRocksSharedDataStorageResolver,
    StarRocksStarManagerRouting, StarRocksStarOsClient, StarRocksStorageBindingRef,
};
pub use domain::{
    StarRocksCapabilitySnapshot, StarRocksConnectorConfig, StarRocksFreezeDigest,
    StarRocksLocalBindingRef, StarRocksReadAttemptId, StarRocksReadPolicy, StarRocksResolvedTable,
    StarRocksRpcOpaquePayload, StarRocksRpcTransport, StarRocksSelectedStrategy,
    StarRocksSplitPlanningInput, StarRocksStrategySplit, StarRocksStrategySplitPayload,
    StarRocksTopology,
};
pub use execution::{
    StarRocksDirectReaderFactory, StarRocksExecutionBindings, StarRocksExecutionInstaller,
    StarRocksLocalExecutionBinding, StarRocksRpcReaderFactory,
};

pub const STARROCKS_PROVIDER_ID: &str = "starrocks";
pub const STARROCKS_CONTRACT_VERSION: u16 = 1;
