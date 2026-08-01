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

//! Shared-data direct-read contracts and implementations.
//!
//! This module is deliberately provider-private.  Its public surface exposes
//! only startup-composed planner, location and reader ports; StarRocks wire
//! messages and object-store credentials never cross this boundary.

mod codec;
mod planning;
mod reader;
mod staros;

mod storage;

pub use planning::{
    StarRocksDirectColumnBinding, StarRocksDirectLocation, StarRocksDirectLocationSource,
    StarRocksDirectMetadataLayout, StarRocksDirectSplit, StarRocksDirectTabletDescriptor,
    StarRocksDirectTabletPlanningSource, StarRocksSharedDataDirectPlanner,
    StarRocksStorageBindingRef,
};
pub use reader::{StarRocksDirectStorageResolver, StarRocksSharedDataDirectReaderFactory};
pub use staros::{
    StarOsV1Client, StarOsV1LocationSource, StarOsV1ObjectStoreResolver,
    StarOsV1Routing as StarRocksStarManagerRouting, StarRocksDirectIoRuntime,
};
pub use storage::StarRocksSharedDataStorageResolver;
pub type StarRocksStarOsClient = StarOsV1Client;

pub(crate) use codec::{DirectOuterFacts, decode_direct_split, encode_direct_split};
