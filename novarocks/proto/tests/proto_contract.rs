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

//! NIDL contract round-trip tests: hand-written internal <-> proto conversions
//! + encode/decode/assert_eq, locking each wire contract. Permanent (unlike the
//!   NIDL-0 spike). One submodule per proto file; expr/plan added in later PRs.

#[path = "proto_contract/common.rs"]
mod common;
#[path = "proto_contract/expr.rs"]
mod expr;
#[path = "proto_contract/filter.rs"]
mod filter;
#[path = "proto_contract/instance_params.rs"]
mod instance_params;
#[path = "proto_contract/plan.rs"]
mod plan;
#[path = "proto_contract/release_fixtures.rs"]
mod release_fixtures;
#[path = "proto_contract/service.rs"]
mod service;
