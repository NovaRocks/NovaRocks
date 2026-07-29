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

mod dependency;
pub(crate) mod descriptor;
mod endpoint;
mod error;
pub(crate) mod expr;
mod instance;
pub(crate) mod layout;
pub(crate) mod node;
mod options;
pub(crate) mod schema;
pub(crate) mod sink;
mod submission;
pub(crate) mod type_lowering;

pub use dependency::{
    DraftDependencyValue, FragmentExprArenaOwner, LakeMetaColumnKind, LakeMetaColumnRequest,
    LakeMetaStorageFacts, LakeMetaStorageRequest, LakeMetaTabletRequest,
    StarRocksExternalDependency, StarRocksExternalDependencyDraft, StarRocksResolvedDependencies,
    StarRocksResolvedDependencyValue,
};
pub(crate) use endpoint::decode_fragment_destination;
pub use endpoint::decode_runtime_endpoint;
pub use error::{
    StarRocksDependencyContractError, StarRocksDependencyContractErrorKind,
    StarRocksFragmentDecodeError,
};
pub(crate) use instance::{
    LakeMetaScanRangeFact, LakeScanProgramFacts, StarRocksJdbcFacts, StarRocksObjectStoreDefaults,
    StarRocksPathRewriteFacts, decode_lake_meta_scan_range_facts, decode_lake_scan_program_facts,
    decode_scan_contracts_and_raw_ranges,
};
pub use instance::{StarRocksDecodeFacts, decode_incremental_scan_ranges, snapshot_decode_facts};
pub(crate) use options::decode_query_options;
pub use submission::{
    DecodedStarRocksFragment, StarRocksDecodeInput, StarRocksFragmentDraft,
    StarRocksLookupCloseTarget, StarRocksReportDestination, StarRocksSubmissionMetadata,
    finish_fragment_submission, prepare_fragment_submission,
};

pub(crate) fn decode_expression_for_layout(
    expr: &crate::thrift::exprs::TExpr,
    arena: &mut crate::exec::expr::ExprArena,
    layout: &layout::Layout,
) -> Result<crate::exec::expr::ExprId, String> {
    expr::lower_t_expr(expr, arena, layout, None, None)
}

#[cfg(test)]
mod task2_tests;
