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

#[cfg(test)]
pub(crate) mod decode;
pub(crate) mod encode;
pub(crate) mod query_options_contract;
pub(crate) mod runtime_filter_contract_codec;
mod runtime_filter_install;
#[cfg(test)]
pub(crate) mod test_assembly;
pub(crate) mod type_encode;
pub(crate) mod type_mapping;

// Narrow Task 4/5 surface: handlers and client adapters need only these DTOs
// and boundary entry points, not the codec implementation module.
#[allow(unused_imports)]
pub(crate) use runtime_filter_install::{
    DecodedRuntimeFilterParticipantInstall, RuntimeFilterDeploymentAbort,
    RuntimeFilterQueryLifecycleOptions, decode_abort_runtime_filter_deployment,
    decode_participant_install, encode_abort_runtime_filter_deployment, encode_participant_install,
};
