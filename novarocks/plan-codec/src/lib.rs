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

//! Deterministic planner-IR-to-protobuf encoding for the native FE/BE
//! boundary.
//!
//! A sealed distributed plan plus the frozen scan facts of its bindings map to
//! native protobuf here and nowhere else. The encoding is a pure function of
//! those two inputs: it reads no session, no catalog runtime, no live topology,
//! and no request-assembly state, and it decides nothing about placement,
//! scheduling or instance identity.
//!
//! That constraint is what this crate exists to hold. It depends on the planner
//! IR ([`novarocks_sql::plan_read`]), the wire models and their codec, and the
//! connector SPI vocabulary -- and deliberately not on the frontend. A future
//! encoder change therefore cannot reach a frontend request-assembly type by
//! accident: it would first have to add a dependency edge that does not exist.
//!
//! The scan facts arrive through the [`NativeScanFacts`] trait family rather
//! than as a concrete producer type, so the frontend keeps ownership of how its
//! bindings are resolved while the encoder sees only the frozen projections.
//!
//! Everything that is not part of a single sealed plan's encoding stays with
//! its owner: instance sidecars, submission assembly, and the bundle adapter
//! that unwraps the frontend's prepared encoding view remain in the frontend.

mod expr;
mod plan;

pub use expr::encode_expr;
pub use plan::scan_facts::{
    NativeConnectorRead, NativeScanBinding, NativeScanColumn, NativeScanColumnKind,
    NativeScanExecutionKind, NativeScanFacts, NoScanFacts,
};
pub use plan::write_dataflow::SealedWriteTargets;
pub use plan::{
    encode_data_partition, encode_distributed_plan, encode_distributed_plan_with_write_targets,
};
