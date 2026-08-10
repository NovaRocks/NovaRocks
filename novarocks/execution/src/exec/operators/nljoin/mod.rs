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
//! Nested-loop join operator module exports.
//!
//! Responsibilities:
//! - Registers NL-join build and probe operators with their shared state definitions.
//! - Exposes NL-join factories consumed by pipeline builder.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

mod nljoin_build_sink;
mod nljoin_probe_processor;
mod nljoin_shared;

pub use nljoin_build_sink::NlJoinBuildSinkFactory;
pub use nljoin_probe_processor::NlJoinProbeProcessorFactory;
pub(crate) use nljoin_shared::NlJoinSharedState;
