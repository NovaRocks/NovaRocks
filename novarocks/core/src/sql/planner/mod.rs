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

//! Planner — builds logical plans, materializes optimizer output into planner
//! physical IR, and plans distributed fragment topology.
//!
//! The stage transitions are explicit: optimizer bridge produces
//! `PhysicalPlanNode`; the planner pipeline applies physical placement passes
//! before distributed planning cuts fragments and wires cross-fragment state.

#[cfg(feature = "runtime-filter-test-support")]
pub mod distributed;
#[cfg(not(feature = "runtime-filter-test-support"))]
pub(crate) mod distributed;
pub(crate) mod imv_rewrite;
pub(crate) mod logical;
pub(crate) mod optimizer_bridge;
pub(crate) mod ordering;
pub(crate) mod payload;
pub(crate) mod physical;
pub(crate) mod pipeline;
pub(crate) mod runtime_filter;
pub(crate) mod table;
pub(crate) use logical::build::{plan_output_columns, plan_query};
