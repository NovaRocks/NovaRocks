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

//! Frozen spill options consumed by the execution kernel.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpillMode {
    None,
    Force,
    Auto,
    Random,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SpillConfig {
    pub enable_spill: bool,
    pub spill_mode: SpillMode,
    pub spill_mem_limit_threshold: Option<f64>,
    pub spill_operator_min_bytes: Option<i64>,
    pub spill_operator_max_bytes: Option<i64>,
    pub spill_encode_level: Option<i32>,
    pub enable_spill_buffer_read: Option<bool>,
    pub max_spill_read_buffer_bytes_per_driver: Option<i64>,
    pub spill_mem_table_size: Option<i32>,
    pub spill_mem_table_num: Option<i32>,
}
