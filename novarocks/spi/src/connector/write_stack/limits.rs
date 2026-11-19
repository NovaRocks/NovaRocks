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

//! Frozen budgets for the distributed write data plane.
//!
//! Every constant here is a wire-visible budget: exceeding it is a typed
//! rejection before any provider I/O or external side effect. The values are
//! frozen by the NCP-6 design and must not be widened, narrowed, or traded for
//! truncation, compression, spill, or partial commit.

/// The largest canonical encoding of one logical [`ConnectorWriterHandle`].
///
/// Charged once per *unique logical* handle. Copying the same canonical handle
/// into additional physical writer placements does not charge again.
///
/// [`ConnectorWriterHandle`]: super::ConnectorWriterHandle
pub use novarocks_connector_contract::MAX_CONNECTOR_WRITER_HANDLE_BYTES;

/// The largest total canonical encoding of all unique logical writer handles in
/// one sealed query plan. The frontend is the only owner of this budget: a
/// backend sees one carrier at a time and cannot reconstruct the query-wide
/// unique set.
pub const MAX_CONNECTOR_UNIQUE_WRITER_HANDLE_BYTES: usize = 64 * 1024 * 1024;

/// The largest number of logical write targets one begin session may seal.
/// Target ordinals are dense, so this also bounds the highest legal ordinal.
pub use novarocks_connector_contract::MAX_CONNECTOR_WRITE_TARGETS;
