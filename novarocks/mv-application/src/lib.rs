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

//! Connector-neutral materialized-view application mechanics.
//!
//! This crate owns only current-process product mechanics: candidate isolation
//! and target/publication runtime state. Provider lake packages, SQL rewrite
//! proofs, query bindings, and physical execution stay at their respective
//! boundaries.

pub mod activity;
pub mod candidate;
pub mod dependency;
pub mod maintenance;
pub mod persistence;
pub mod ports;
pub mod process_runtime;
pub mod product;
pub mod publication;
pub mod readiness;
pub mod repository;
mod repository_metrics;
pub mod scheduler;
pub mod scheduler_runtime;
pub mod service;
pub mod state_family;
pub mod state_store_repository;
#[doc(hidden)]
pub mod test_repository;
