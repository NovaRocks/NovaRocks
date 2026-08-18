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

pub use crate::query_execution::native_fragment::{
    NativeFragmentAttachment, NativeFragmentEncodingView,
};

/// Encode one immutable distributed plan and its exact prepared bindings into
/// the native FE-to-BE wire bundle.
pub fn encode_native_fragment_bundle(
    source: NativeFragmentEncodingView<'_>,
) -> Result<NativeFragmentAttachment, String> {
    let plan = source.distributed_plan();
    let scan_facts = source.scan_facts();
    let encoded = super::plan::encode_distributed_plan(plan, scan_facts)?;
    source.seal(encoded.fragments)
}
