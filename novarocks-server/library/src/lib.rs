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

#[cfg(all(feature = "mem-1-m07-hms-listing-observe", not(novarocks_dev_controls)))]
compile_error!("mem-1-m07-hms-listing-observe requires test-controls");
#[cfg(all(feature = "mem-1-m07-closing-pressure", not(novarocks_dev_controls)))]
compile_error!("mem-1-m07-closing-pressure requires test-controls");
#[cfg(all(feature = "mem-1-m07-root-observation", not(novarocks_dev_controls)))]
compile_error!("mem-1-m07-root-observation requires test-controls");
#[cfg(all(feature = "mem-1-m07-exact-mysql-write", not(novarocks_dev_controls)))]
compile_error!("mem-1-m07-exact-mysql-write requires test-controls");
#[cfg(all(
    feature = "native-compatibility-test-fixture",
    not(novarocks_dev_controls)
))]
compile_error!("native-compatibility-test-fixture requires test-controls");

/// Capabilities compiled by this owner; activation still requires a role session.
pub const fn compiled_test_capabilities() -> (&'static str, &'static [&'static str]) {
    (
        "novarocks-server",
        &[
            #[cfg(novarocks_dev_controls)]
            "test-controls",
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            "mem-1-m07-hms-listing-observe",
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            "mem-1-m07-closing-pressure",
            #[cfg(feature = "mem-1-m07-root-observation")]
            "mem-1-m07-root-observation",
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            "mem-1-m07-exact-mysql-write",
            #[cfg(feature = "native-compatibility-test-fixture")]
            "native-compatibility-test-fixture",
        ],
    )
}

mod command;
pub use command::main;

pub mod app_config;
pub mod build_information;
pub mod catalog_credential_registry;
pub mod catalog_source_config;
pub mod cgroup_memory;
mod compiled_composition;
pub mod composition;
mod env_reference;
pub mod launch;
pub mod logging;
pub mod memory_limit;
pub mod memory_observation;
pub mod native_compatibility;
pub mod native_trust;
pub mod network;
pub mod provider_manifest;
pub mod roles;
pub mod scan_io;
mod sdk_listing_profile;
pub mod state_store_config;
mod state_store_limits;
pub mod static_plan;
pub mod supervisor;

#[cfg(novarocks_dev_controls)]
pub mod provider_test_slots;

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");
