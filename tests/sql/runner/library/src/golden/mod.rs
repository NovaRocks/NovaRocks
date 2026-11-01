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

//! Canonical, typed SQL expectations and their temporary migration blocks.
// Design: ADR-0210 (docs/adr/ADR-0210-sql-golden-is-canonical-rendering-of-typed-trino-results.md)
mod cell;
mod grammar;
mod java_float;
mod trino_type;

pub use cell::{parse_cell, parse_text, render_cell, render_text};
pub use grammar::{Block, GoldenFile, Section};
pub use trino_type::parse_type;

#[cfg(test)]
mod tests;

pub mod legacy;

pub mod execution;
pub mod migration;
