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
//! Model-aware reader dispatcher.
//!
//! This module routes one native read plan to a concrete table-model reader.
//! It keeps model branching out of low-level page decode code.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::connector::MinMaxPredicate;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::formats::starrocks::plan::{StarRocksNativeReadPlan, StarRocksTableModelPlan};
use crate::formats::starrocks::segment::StarRocksSegmentFooter;

mod agg;
mod dup;
mod primary;

pub(super) fn build_record_batch_by_model(
    plan: &StarRocksNativeReadPlan,
    segment_footers: &[StarRocksSegmentFooter],
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
    output_schema: &SchemaRef,
    min_max_predicates: &[MinMaxPredicate],
) -> Result<RecordBatch, String> {
    match plan.table_model {
        StarRocksTableModelPlan::DupKeys => dup::build_dup_record_batch(
            plan,
            segment_footers,
            tablet_root_path,
            object_store_profile,
            output_schema,
            min_max_predicates,
        ),
        StarRocksTableModelPlan::AggKeys => agg::build_agg_record_batch(
            plan,
            segment_footers,
            tablet_root_path,
            object_store_profile,
            output_schema,
            min_max_predicates,
        ),
        StarRocksTableModelPlan::UniqueKeys => agg::build_agg_record_batch(
            plan,
            segment_footers,
            tablet_root_path,
            object_store_profile,
            output_schema,
            min_max_predicates,
        ),
        StarRocksTableModelPlan::PrimaryKeys => primary::build_primary_record_batch(
            plan,
            segment_footers,
            tablet_root_path,
            object_store_profile,
            output_schema,
            min_max_predicates,
        ),
    }
}
