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

// Design: ADR-0140 (docs/adr/ADR-0140-state-store-contract-and-testkit-crates.md)

//! Shared StateStore test mechanics.
//!
//! It carries the in-memory reference store and the behaviour suite every
//! provider is expected to satisfy. It is a test-only dependency: the domain
//! crate does not depend on it, so no production closure can reach a fake.

pub mod conformance;
pub mod testing;
