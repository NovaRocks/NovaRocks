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

//! Explicit lineage and capacity admission for retained Arrow allocations.
//!
//! A domain keeps one real memory leaf. Importing a group of immutable Arrow
//! batches measures their full backing allocations and pays once per distinct
//! backing in that group. Derived outputs carry source entries for shared
//! allocations, and pay only for new allocations and lineage metadata.
//! `Retained<T>` keeps the payload and its immutable lineage together; its
//! final drop destroys the payload before settling the backing charges.
// Design: ADR-0160 (docs/adr/ADR-0160-governed-retention-accounting.md)

pub mod backing;
pub mod domain;
pub mod retained;
pub mod shared;

pub use backing::{BackingCollector, BackingError, BackingProvenance};
pub use domain::{DomainCensus, RetentionDomain};
pub use retained::{RetainError, Retained};
pub use shared::{SharedError, SharedRetention};
