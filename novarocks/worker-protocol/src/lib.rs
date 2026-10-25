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

//! Shared control-plane worker protocol contracts.

#![forbid(unsafe_code)]

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

pub mod control;
pub mod context;
pub mod task;
pub mod creation;
pub mod admission;
pub mod lifecycle;
pub mod domain;
pub mod observation;
pub mod conclusion;
pub mod proposal_summary;
pub mod inbound_cost;
pub mod retention;

pub mod lease;
pub mod convergence;
