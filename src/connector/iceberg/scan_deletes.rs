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

//! Position-delete reverse projection for IVM Phase 2.
//!
//! Reads `PositionDeleteRef`s produced by `plan_changes` and, for each
//! deleted `(data_file, pos)` pair, projects the *original* base row
//! out of the source data file. The output is a `Vec<RecordBatch>` of
//! the deleted rows in the base table's full schema, ready for WHERE
//! re-application (which `materialize_changes` does in SQL by
//! registering these as a temp parquet table and running the MV's
//! SELECT).
//!
//! This is the inverse of `iceberg::position_delete`'s scan-time
//! filtering: that module *removes* deleted rows from a scan; we keep
//! only the deleted rows.

#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use std::collections::HashMap;

#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use arrow::array::{Array, ArrayRef, BooleanArray};
#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use arrow::compute::filter_record_batch;
#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use arrow::record_batch::RecordBatch;
#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use parquet::arrow::ProjectionMask;
#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use roaring::RoaringTreemap;

#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use crate::connector::iceberg::changes::{ChangeError, PositionDeleteRef};

// TODO PR-3 Task 3: implement read_delete_positions_per_data_file
// TODO PR-3 Task 4: implement read_data_file_at_positions
// TODO PR-3 Task 5: implement scan_deletes (top-level orchestrator)
