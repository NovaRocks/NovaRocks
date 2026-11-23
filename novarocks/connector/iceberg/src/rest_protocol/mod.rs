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

//! Provider-owned Iceberg REST session, transport, wire models and endpoints.
//! One configuration handshake establishes the generation's immutable route,
//! authentication provenance and capabilities. Every frontend request shares
//! that session; backend credential acquisition uses its frozen routing facts.
//!
//! Design: ADR-0175 (docs/adr/ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md)

pub(crate) mod attempt;
pub(crate) mod auth;
pub(crate) mod binding;
pub(crate) mod boundary;
pub(crate) mod deferred;
pub(crate) mod endpoints;
pub(crate) mod evidence;
pub(crate) mod http;
pub(crate) mod json;
pub(crate) mod minimal_view;
pub(crate) mod mutation;
pub(crate) mod routing;
#[cfg(test)]
pub(crate) mod test_server;

pub(crate) mod configuration;
pub(crate) mod view_metadata;
pub(crate) mod wire;

#[cfg(test)]
mod activity_tests;

#[cfg(test)]
mod publication_owner_tests;
