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

//! Scalar functions for IVM materialized view state combinator operations.
//!
//! Each kind has:
//!   - <kind>_state_union(a, b)  -> VARBINARY  (merge two states)
//!   - <kind>_state_visible(s)   -> <return type> (finalize to user-visible)
//!
//! Plus the debug helper:
//!   - DEBUG_DUMP_MV_STATE(mv_name, row_id) -> Utf8 (JSON representation)

pub(super) mod approx_count_distinct;
pub(super) mod avg;
pub(super) mod bool_or_and;
pub(super) mod common;
pub(super) mod count;
pub(super) mod count_distinct;
pub(super) mod debug_dump;
pub(super) mod dispatch;
pub(super) mod min_max;
pub(super) mod sum;

pub use dispatch::{eval_mv_state_function, metadata, register};
