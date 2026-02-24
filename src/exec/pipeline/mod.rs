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
//! Pipeline runtime module exports.
//!
//! Responsibilities:
//! - Exposes builder, driver, executor, scheduling, and operator abstractions for pipeline execution.
//! - Defines the pipeline module surface consumed by higher-level exec services.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

pub mod blocked_driver_poller;
pub mod builder;
pub mod chunk_buffer_memory_manager;
pub mod dependency;
pub mod distribution;
pub mod driver;
pub mod executor;
pub mod fragment_context;
pub mod global_driver_executor;
pub mod operator;
pub mod operator_factory;
pub mod pipeline;
pub mod scan;
pub mod schedule;
