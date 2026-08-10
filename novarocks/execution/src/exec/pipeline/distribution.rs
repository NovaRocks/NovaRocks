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
//! Pipeline stream distribution descriptors.
//!
//! Responsibilities:
//! - Describes local/remote stream routing and exchange distribution requirements.
//! - Carries stream metadata consumed by pipeline builder and data stream sinks.
//!
//! Key exported interfaces:
//! - Types: `Distribution`, `StreamDesc`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::expr::ExprId;

#[derive(Clone, Debug, PartialEq, Eq)]
/// Data distribution modes used for stream routing and exchange planning.
pub enum Distribution {
    Any,
    Single,
    Hash {
        keys: Vec<ExprId>,
        partitions: usize,
        hash_version: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Descriptor of one stream edge including distribution and expression metadata.
pub struct StreamDesc {
    pub dop: i32,
    pub distribution: Distribution,
}

impl StreamDesc {
    pub fn any(dop: i32) -> Self {
        Self {
            dop: dop.max(1),
            distribution: Distribution::Any,
        }
    }

    pub fn single() -> Self {
        Self {
            dop: 1,
            distribution: Distribution::Single,
        }
    }
}
