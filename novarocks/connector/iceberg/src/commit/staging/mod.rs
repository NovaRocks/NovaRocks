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

//! Provider-owned canonical staging, without SDK transaction state.
mod engine;
mod manifests;
mod normalize;
mod requirements;
mod snapshot_id;
mod view;

pub use engine::{PreparedChange, Preparer, StagingBase, StagingEngine};
pub use manifests::{
    FirstRowIdInheritance, ManifestEntryWrite, ManifestListOutput, write_manifest,
    write_manifest_list,
};
pub use snapshot_id::new_snapshot_id;
pub use view::StagedView;

fn invalid(message: impl Into<String>) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod interop_tests;
