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

//! Provider-neutral application support for durable StateStore operations.
//!
//! Concrete provider construction and configuration belong to Server. Durable
//! record families and prefixes remain with the application domain that owns
//! those records. This crate owns the selected provider instance's host
//! lifecycle, but Server remains the only source of provider selection and
//! registration contributions.

mod durable;
mod family;
mod host;
mod host_error;
mod policy;
mod provider;
mod runner;

pub use durable::{
    DurableOpaqueBytes, DurableRecord, DurableRecordError, DurableRecordStore, EncodedRecord,
};
pub use family::{
    PersistentStateFamily, PersistentStateFamilyError, validate_persistent_state_families,
};
pub use host::{PreparedStateStoreHost, StateStoreHost, StateStoreHostLifecycle};
pub use host_error::{StateStoreHostError, StateStoreHostErrorKind};

pub use policy::{
    DEFAULT_MAX_ATTEMPTS, DEFAULT_OPERATION_TIMEOUT, MAX_ATTEMPTS_CEILING,
    OPERATION_TIMEOUT_CEILING, StateStoreRunPolicy, StateStoreRunPolicyError,
};
pub use provider::{
    StateStoreHostInput, StateStoreProviderRegistration, StateStoreProviderRegistry,
};
pub use runner::{
    RunFailure, RunSuccess, StateStoreRunMetrics, StateStoreRunReceiving, run_side_effect_free,
    run_side_effect_free_in, run_side_effect_free_in_receiving,
};
