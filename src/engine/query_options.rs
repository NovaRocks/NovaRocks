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

use crate::exec::spill::SpillConfig;

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StandaloneQueryOptions {
    pub pipeline_dop: Option<i32>,
    pub query_timeout: Option<i32>,
    pub batch_size: Option<i32>,
    pub enable_profile: bool,
    pub exec_mem_limit: Option<i64>,
    pub connector_io_tasks_per_scan_operator: Option<i32>,
    pub allow_throw_exception: bool,
    pub group_concat_max_len: Option<i64>,
    pub spill: Option<SpillConfig>,
}
