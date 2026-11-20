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

use parquet::file::statistics::Statistics;

pub(super) fn known_nulls(stats: &Statistics, values: u64) -> Option<u64> {
    stats.null_count_opt().filter(|count| *count <= values)
}

pub(super) fn usable_statistics(stats: &Statistics, nulls: Option<u64>) -> bool {
    // parquet-mr's isEmpty requires a NULL count or a pair of footer extrema.
    nulls.is_some() || (stats.min_bytes_opt().is_some() && stats.max_bytes_opt().is_some())
}

pub(super) fn no_ordered_values(stats: &Statistics, values: u64, nulls: Option<u64>) -> bool {
    if values == 0 || nulls == Some(values) {
        return true;
    }
    matches!(stats, Statistics::Float(_) | Statistics::Double(_))
        && stats
            .nan_count_opt()
            .is_some_and(|count| count == values - nulls.unwrap_or(0))
}
