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

//! Closed manifest of frontend-local state families.
//!
//! Every remaining Frontend-local state belongs to exactly one family and has
//! one classification, one authority or rebuild source, and one
//! retain/clone/wipe policy. Durable product state is deliberately absent:
//! each product exposes its own descriptor and the composition root validates
//! their complete set before opening a StateStore.
//!
//! Two structural properties do the enforcing, so the manifest is not another
//! convention that has to be remembered during review:
//!
//! 1. **A `ProcessRuntime` family cannot have a persistent prefix.** The prefix
//!    lives in the `Accelerator` variant only, so the illegal state is not
//!    representable — see
//!    [`ProcessRuntimeContract`].
//! 2. **Frontend cannot mint a durable prefix.** [`PersistentKeyPrefix`] has
//!    no public constructor. Product descriptors are the sole definition point
//!    for their deployed prefix and record version.
//!
//! The manifest is a frontend application fact, not an SPI contract: the
//! StateStore boundary knows about keys and values, and has no opinion about
//! which frontend family owns them.

mod classification;
mod manifest;

#[cfg(test)]
mod tests;
