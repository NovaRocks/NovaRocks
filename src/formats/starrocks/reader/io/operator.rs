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
//! OpenDAL operator builder for native reader.
//!
//! Current limitations:
//! - Supports S3-compatible and local filesystem tablet roots.

use opendal::Operator;

use crate::connector::starrocks::ObjectStoreProfile;

use super::tablet_root::TabletRoot;

/// Build an object operator for native segment reads.
pub(crate) fn build_operator(
    root: &TabletRoot,
    object_store_profile: Option<&ObjectStoreProfile>,
) -> Result<Operator, String> {
    match root {
        TabletRoot::S3 { bucket, root } => {
            let profile = object_store_profile.ok_or_else(|| {
                format!(
                    "missing object store profile for native data loader: root=s3://{}/{}",
                    bucket, root
                )
            })?;
            build_s3_operator(bucket, root, profile)
        }
        TabletRoot::Local { root } => build_local_operator(root),
    }
}

fn build_local_operator(root: &str) -> Result<Operator, String> {
    let builder = opendal::services::Fs::default().root(root);
    let operator_builder = Operator::new(builder)
        .map_err(|e| format!("init local native data operator failed: {e}"))?;
    Ok(operator_builder.finish())
}

fn build_s3_operator(
    bucket: &str,
    root: &str,
    object_store_profile: &ObjectStoreProfile,
) -> Result<Operator, String> {
    let cfg = object_store_profile.to_object_store_config(bucket, root);
    crate::fs::object_store::build_oss_operator(&cfg)
        .map_err(|e| format!("init object store native data operator failed: {e}"))
}
