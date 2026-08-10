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

//! Set-operation operator module exports.
//!
//! Responsibilities:
//! - Hosts shared/operator implementations for UNION ALL / INTERSECT / EXCEPT.
//! - Exposes a stable import surface for set-operation pipeline construction.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

mod distinct_set_shared;
mod distinct_set_sink;
mod distinct_set_source;
mod except_shared;
mod except_sink;
mod except_source;
mod intersect_shared;
mod intersect_sink;
mod intersect_source;
mod set_op_stage;
pub(crate) mod union_all_shared;
mod union_all_sink;
mod union_all_source;

pub(crate) use except_shared::ExceptSharedState;
pub use except_sink::ExceptSinkFactory;
pub use except_source::ExceptSourceFactory;
pub(crate) use intersect_shared::IntersectSharedState;
pub use intersect_sink::IntersectSinkFactory;
pub use intersect_source::IntersectSourceFactory;
pub(crate) use set_op_stage::SetOpStageController;
pub(crate) use union_all_shared::UnionAllSharedState;
pub use union_all_sink::UnionAllSinkFactory;
pub use union_all_source::UnionAllSourceFactory;
