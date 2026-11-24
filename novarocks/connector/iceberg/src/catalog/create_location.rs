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

use futures::StreamExt;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorOperationControl};

use crate::access_binding::IcebergReadBinding;
use crate::catalog_control::maintenance_error::filesystem_failure;
use crate::fs_io::resolve_request_access_for_location;

pub(super) async fn check_empty(
    location: &str,
    binding: &IcebergReadBinding,
) -> Result<(), ConnectorError> {
    let filesystem_error =
        || filesystem_failure(format!("Failed checking new table's location: {location}"));
    let access = resolve_request_access_for_location(location, binding)
        .await
        .map_err(|error| {
            if matches!(
                error.kind(),
                ConnectorErrorKind::Cancelled
                    | ConnectorErrorKind::DeadlineExceeded
                    | ConnectorErrorKind::ResourceExhausted
                    | ConnectorErrorKind::Interrupted
            ) {
                error
            } else {
                filesystem_error()
            }
        })?;
    let relative = access
        .single_relative_path()
        .map_err(|_| filesystem_error())?;
    let prefix = if relative.is_empty() || relative.ends_with('/') {
        relative.to_owned()
    } else {
        format!("{relative}/")
    };
    let _io = binding.admit_non_http_io(location).await?;
    let operator = access.operator();
    let mut entries = operator
        .lister_with(&prefix)
        .recursive(true)
        .limit(1)
        .await
        .map_err(|_| filesystem_error())?;
    // A local backend can yield its own directory before the first file.
    while let Some(entry) = entries.next().await {
        if let Some(context) = binding.request_context() {
            context.check_active()?;
        }
        if !entry.map_err(|_| filesystem_error())?.metadata().is_dir() {
            return Err(filesystem_failure(format!(
                "Cannot create a table on a non-empty location: {location}"
            )));
        }
    }
    Ok(())
}
