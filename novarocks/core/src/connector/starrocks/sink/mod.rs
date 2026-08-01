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

pub(crate) mod auto_increment;
mod factory;
pub(crate) mod operator;
pub mod partition_key;
pub mod plan;
pub(crate) mod routing;

/// Drops cached auto-increment allocation facts for a table after an FE agent
/// task removes that table's allocation map.
pub fn clear_auto_increment_cache_for_table(table_id: i64) {
    auto_increment::clear_auto_increment_cache_for_table(table_id);
}
