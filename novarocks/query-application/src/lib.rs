// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Query application ownership and consumer-facing contracts.
//!
//! Product services depend on this crate. The query application never depends
//! on their implementations; role composition supplies the consumer ports.
// Design: ADR-0144 (docs/adr/ADR-0144-application-domains-follow-stable-ownership-seams.md)

pub mod api;

/// First-wins statement cancellation shared by query application consumers.
pub mod cancellation;

/// Bounded process-local execution of CPU-bound query application work.
pub mod cpu;

/// Stable query and mutation error model, without wire-specific encoding.
pub mod engine_error;

/// Exact client-session identity and protocol-owned termination contracts.
pub mod client_connection;

/// Session registry and cancellation implementation owned by the query application.
pub mod query_control;

/// Session-derived facts frozen before role-local request assembly.
pub mod request_session;

/// Query-session errors before protocol-specific encoding.
pub mod session_error;

/// Session-local deadline and terminal-error interpretation.
pub mod session_outcome;

/// Query-session admission requests independent of a wire protocol.
pub mod session;

/// Process-local serving admission whose transitions are driven by role composition.
pub mod serving_admission;
/// Session generations, governed statement control, and protocol settlement.
pub mod session_control;
/// Statement-local external-effect boundary for safe topology retry.
pub mod statement_effect;

/// Read-only `information_schema` materialization contracts and providers.
pub mod system_catalog;
pub mod system_catalog_rewrite;
pub mod view;

/// SQL source parsing and statement-shape admission owned by the query
/// application before role adapters route a statement to a product consumer.
pub mod sql;

/// Query-scoped acquisition of immutable metadata and optional optimization facts.
pub mod observation;

/// Move-only settlement of governed protocol output and streaming rows.
pub mod protocol_delivery;

pub mod persisted_query_definition;
/// Startup-frozen admission policy for lake-publication attempts.
pub mod publication;

/// Pure, topology-free preparation and its immutable execution handoff.
pub mod preparation;

// These policies are query-coordination decisions, not worker protocol. The
// hidden visibility supports role-adapter integration while keeping them out
// of the consumer-facing contract.
#[doc(hidden)]
pub mod coordination;

/// High-level lifecycle harness for role-adapter integration tests.
#[cfg(feature = "test-support")]
pub mod test_support;
