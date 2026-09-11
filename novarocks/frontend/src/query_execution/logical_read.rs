// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Narrow Frontend launch boundary for one prepared logical read.

use novarocks_query_application::api::QueryExecutionFuture;
use novarocks_workload_control::WorkOwner;

use crate::query_execution::PreparedLogicalRead;

/// Production composition injects one implementation backed by the process
/// Query Application runtime. The SQL session transfers the complete prepared
/// carrier and the unique governed owner without learning Native adapter parts.
pub(crate) trait LogicalReadLauncher: Send + Sync + 'static {
    fn start(&self, read: PreparedLogicalRead, owner: WorkOwner) -> QueryExecutionFuture;
}
