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

pub(crate) mod activity;
pub(crate) mod background;
pub(crate) mod background_engine;
// Consumed when lake-first discovery replaces ledger-driven enumeration; the
// classifier is pure and lands with its rules tested first.
#[allow(dead_code)]
mod attempt_classification;
// The acquisition surface here is consumed when the three refresh entry points
// are switched onto it. The repository-side fence it feeds is already live, so
// it lands first and is wired next rather than being held back.
#[allow(dead_code)]
pub mod coordination;
// Additive lake-first enumeration. Replacing ledger-driven enumeration is a
// separate change to startup ordering; landing both at once would make a
// regression in either indistinguishable from a regression in the other.
mod create;
#[allow(dead_code)]
mod lake_discovery;
pub(crate) mod maintenance;
pub(crate) mod maintenance_worker;
mod recovery;
// Installed by the composition root so the frontend owns startup ordering.
mod refresh;
pub mod repository;
pub(crate) mod scheduler;
mod service;
#[allow(dead_code)]
pub(crate) mod startup_restore;

pub use recovery::FrontendMvRecoverySummary;
pub(crate) use refresh::FrontendMvRefreshProviderActivationPort;
pub use service::FrontendMvService;
