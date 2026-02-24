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
//! Operator factory trait definitions.
//!
//! Responsibilities:
//! - Defines factory contracts used by pipeline builder to instantiate operators per driver.
//! - Separates plan-time operator configuration from runtime operator instances.
//!
//! Key exported interfaces:
//! - Types: `OperatorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use super::operator::Operator;

/// Factory contract for constructing runtime operators from plan-time configuration.
pub trait OperatorFactory: Send + Sync {
    fn name(&self) -> &str;

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator>;

    fn is_source(&self) -> bool {
        false
    }

    fn is_sink(&self) -> bool {
        false
    }
}
