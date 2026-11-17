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

//! The production logical execution carrier and its original application Sources.

pub(crate) mod adapter;
mod application;
mod failure;
mod funding;
mod geometry;
mod inlet;
mod input_bank;
pub(crate) use input_bank::{LogicalInputBank, LogicalInputsLoan};
mod inputs;
mod install;
mod installed;
pub(crate) mod links;
mod purpose;
mod source;
mod work;
pub(crate) use application::LogicalHostPorts;
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
