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

//! SQL-specific Iceberg write transaction policy captured before execution.

use std::collections::BTreeMap;

#[allow(
    dead_code,
    reason = "Typed Iceberg policy retains full provider commit facts for target-gated write paths."
)]
pub(crate) struct IcebergWriteCommitPolicy {
    pub(crate) base_snapshot_id: Option<i64>,
    pub(crate) base_snapshot_map: BTreeMap<String, i64>,
    pub(crate) target_ref: String,
    pub(crate) snapshot_properties: BTreeMap<String, String>,
}

#[allow(
    dead_code,
    reason = "Typed Iceberg validation policy remains part of the captured write transaction contract."
)]
pub(crate) struct IcebergWriteValidationPolicy {
    pub(crate) require_v3_for_branch: bool,
}

pub(crate) enum IcebergWriteSource {
    CoordinatedPlan,
}

#[allow(
    dead_code,
    reason = "The captured write transaction spec retains all policy facets for target-gated execution paths."
)]
pub(crate) struct IcebergWriteTransactionSpec {
    pub(crate) is_overwrite: bool,
    pub(crate) attempt_id: String,
    pub(crate) commit: IcebergWriteCommitPolicy,
    pub(crate) validation: IcebergWriteValidationPolicy,
    pub(crate) source: IcebergWriteSource,
}
