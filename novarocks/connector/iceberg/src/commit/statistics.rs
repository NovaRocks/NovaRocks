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

//! Eager statistics staging for provider-private atomic publication.

use crate::iceberg::spec::StatisticsFile;
use crate::iceberg::table::Table;
use crate::iceberg::transaction::{ApplyTransactionAction, Transaction};

/// Eagerly evaluate `SetStatistics` and export its catalog payload without
/// dispatching. The caller must hand the result to the provider-private
/// publication frontier; this function never owns OCC retries.
pub(crate) async fn stage_statistics_file(
    table: &Table,
    stats_file: StatisticsFile,
) -> Result<crate::iceberg::TableCommit, String> {
    let tx = Transaction::new(table);
    let tx = tx
        .update_statistics()
        .set_statistics(stats_file)
        .apply(tx)
        .await
        .map_err(|error| format!("stage Iceberg SetStatistics: {error}"))?;
    Ok(tx.into_table_commit())
}
