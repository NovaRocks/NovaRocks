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

mod op;
mod reader;

pub use op::DeferredLakeScanResolution;
pub(crate) use op::build_native_object_store_profile_from_properties;
pub use op::plan_compat_starrocks_read_source;
pub(crate) use op::read_starrocks_batches;
pub use op::{
    LakeScanSchemaMeta, StarRocksScanConfig, StarRocksScanRange, StarRocksSchemaColumnHint,
};
pub use op::{
    plan_native_starrocks_read_source, plan_native_starrocks_read_source_with_cancellation,
};
