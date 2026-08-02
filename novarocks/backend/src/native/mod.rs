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

//! Backend-owned native gRPC transport declarations.
//!
//! This module intentionally contains no service implementation yet. It owns
//! the generated server stub while `novarocks-protocol` remains the sole owner
//! of all native protobuf DTO definitions.

pub(crate) mod codec;
pub(crate) mod connector_binding;
pub(crate) mod decode;
pub(crate) mod envelope;
pub(crate) mod exchange;
pub(crate) mod expression;
pub(crate) mod ingress;
pub(crate) mod instance;
pub(crate) mod layout;
pub(crate) mod lifecycle_adapter;
pub(crate) mod plan_decode;
pub(crate) mod runtime_filter;
pub(crate) mod runtime_filter_adapter;
pub(crate) mod runtime_filter_sender;
pub(crate) mod scan_contract;
pub(crate) mod service;
pub(crate) mod sink_assignment;
pub(crate) mod submission_validation;
pub(crate) mod type_decode;

pub(crate) mod transport {
    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}

#[cfg(test)]
mod tests {
    #[test]
    fn generated_native_server_stub_references_protocol_dtos() {
        let generated = include_str!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
        assert!(generated.contains("::novarocks_protocol::novarocks::HeartbeatRequest"));
        assert!(generated.contains("::novarocks_protocol::filter::LookupRequest"));
    }
}
