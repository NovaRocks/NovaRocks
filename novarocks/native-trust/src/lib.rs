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

//! Deployment-scoped authentication material for Native RPC.
//!
//! This crate deliberately has no configuration-source, filesystem, role, or
//! protocol ownership. The Server supplies already-resolved inputs and role
//! applications own their listeners, channels, retries, and observability.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

mod adapter;
mod auth;
mod deployment;
mod error;
mod transport;

pub use adapter::{
    NATIVE_DNS_RESOLUTION_POSITIONS, NativeConnectFailure, NativeDnsResolver,
    NativeEndpointConnector, NativeIncomingAdapter,
};
pub use auth::{
    AuthenticatedNativeCaller, ManualClock, NativeCallerSubject, NativeClientAuthInterceptor,
    NativeListenerAuthLayer, NativeListenerAuthService, NativeProcessIdentity,
    NativeServerAdmission, NativeTrust, NativeTrustClock, SystemClock, TOKEN_LIFETIME_SECONDS,
};
pub use deployment::{DeploymentId, ValidatedSharedSecret};
pub use error::NativeTrustFailureKind;
pub use transport::{
    AutomaticTlsMaterial, NativeTlsMaterial, NativeTransportMode, NativeTransportProfile,
    PemTransportMaterial,
};
