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

//! Typed, deterministic Trino protocol client shared by test owners.
//!
//! This dependency leaf has no NovaRocks product dependencies. Sessions belong
//! to the caller; a successful statement returns effects for explicit application.
// Design: ADR-0210 (docs/adr/ADR-0210-sql-golden-is-canonical-rendering-of-typed-trino-results.md)
mod decode;
mod error;
mod session;
mod statement;
mod trace;
mod value;

pub use decode::{decode_map_key, decode_value};
pub use error::{StatementFailure, TrinoError};
pub use session::{CLIENT_CAPABILITIES, ClientSession, SessionEffects};
pub use statement::{CancelHandle, ClientConfig, PendingStatement, TrinoClient};
pub use trace::{StatementTrace, StatementTraceState};
pub use value::{
    Column, NamedTypeSignature, RowFieldName, StatementResult, TrinoValue, TypeArgument,
    TypeSignature,
};

#[cfg(test)]
mod tests;
