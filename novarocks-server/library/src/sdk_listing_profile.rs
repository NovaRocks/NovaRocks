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

//! Startup agreement for the original external-listing concurrency and timeouts.
use anyhow::{Result, ensure};
pub(crate) fn validate_current() -> Result<()> {
    let (positions, connect, read) =
        novarocks_connector_iceberg::catalog_runtime::frozen_sdk_listing_parameters();
    ensure!(
        positions == 8,
        "external listing Iceberg catalog positions do not match the configured policy"
    );
    ensure!(
        connect == std::time::Duration::from_millis(5000),
        "external listing REST connect timeout does not match the configured policy"
    );
    ensure!(
        read == std::time::Duration::from_millis(30000),
        "external listing REST read timeout does not match the configured policy"
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    #[test]
    fn original_sdk_concurrency_and_timeouts_remain_installed() {
        super::validate_current().unwrap();
    }
}
