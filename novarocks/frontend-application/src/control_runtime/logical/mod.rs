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

// The approved staged cutover keeps this carrier unwired until P12. Remove
// this temporary expectation with that cutover; tests exercise the real carrier.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "P12 composes the approved logical carrier into the production owner"
    )
)]

pub(crate) mod adapter;
mod funding;
mod geometry;

#[cfg_attr(
    test,
    expect(
        dead_code,
        reason = "P12 composes the public installer; carrier tests exercise fake application executors"
    )
)]
mod application;
mod failure;
mod inlet;
mod input_bank;
mod inputs;
#[cfg_attr(
    test,
    expect(
        dead_code,
        reason = "P12 composes the public installer; carrier tests exercise fake application executors"
    )
)]
mod install;
mod installed;
#[cfg_attr(
    test,
    expect(
        dead_code,
        reason = "P12 composes the original Native link and endpoint bindings"
    )
)]
pub(crate) mod links;
mod purpose;
mod source;
mod work;
#[expect(
    unused_imports,
    reason = "P10 implements the sealed host port against this carrier"
)]
pub(crate) use application::LogicalHostPorts;
#[expect(
    unused_imports,
    reason = "P10 and P12 consume the installation composition interfaces"
)]
pub(crate) use install::{
    LogicalPortInstallContext, LogicalPortInstaller, LogicalRuntimeInstaller,
};
mod auxiliary_work;
mod tails;

#[cfg(test)]
#[path = "../../fixtures/logical.rs"]
pub(crate) mod test_fixtures;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod integration_tests;
