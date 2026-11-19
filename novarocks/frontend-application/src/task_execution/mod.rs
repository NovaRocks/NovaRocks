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

//! Frozen task graphs, Native intents, accepted protocol facts and original
//! frontend Sources hosted by the shared control runtime.
//!
//! Attempt Protocol owns Task and Participation decisions. Logical Execution
//! Protocol owns whole-execution decisions. Their final passive ports carry
//! complete facts, while these application owners retain the original Native
//! I/O, payloads, Work, reports, joins and actual exit obligations.
//!
// Design: ADR-0161 (docs/adr/ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md)

pub(crate) mod blocking_io;
pub(crate) mod clock;
pub(crate) mod completion;
pub(crate) mod creation;
pub(crate) mod error;
pub(crate) mod graph;
pub(crate) mod intent;
pub(crate) mod lease;
pub(crate) mod manifest_projection;
pub(crate) mod operation;
pub(crate) mod profile_diagnostics;
pub(crate) mod protocol_facts;
pub(crate) mod pump_health;
pub(crate) mod sources;

#[cfg(test)]
pub(crate) mod graph_manifest_tests;

pub(crate) mod host;
