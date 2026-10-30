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

//! Carrier-neutral byte values, frozen-layout construction and conversion.
//!
//! This crate owns value algorithms and original failure facts. Input acceptance,
//! capacity authorization, scheduling and SQL semantics remain with callers.
//!
//! Supply the complete typed binding from its semantic owner. `access` borrows
//! actual storage, `decode` separates bottom coverage from logical row mapping,
//! and `build` publishes the requested frozen layout. `hydrate` and `compact`
//! plan storage before execution; a complete-batch boundary calls
//! `HydratePlan::require_complete` rather than silently returning a prefix.
//!
//! Conversion contexts belong to the real serial caller (for example a driver
//! or a COW operation). Borrow them across related synchronous calls and clear
//! their cache when work exits. Efficiency budgets may skip reuse, never reject
//! an otherwise legal value; published aliases retain their own source custody.
//! Growth, Work and structure observations remain separate caller policies.

// Design: ADR-0195 (docs/adr/ADR-0195-public-value-access-and-construction.md)
pub mod access;
pub(crate) mod arrow_raw;
pub mod carrier;
pub mod control;
pub mod error;
pub mod reachability;

pub use carrier::ByteLayout;
pub use error::{ValueError, ValueFailure, ValueViolation};

pub mod build;

pub mod storage;

pub mod decode;

pub mod hash_eq;

pub mod compact;

pub mod hydrate;

pub mod batch;
