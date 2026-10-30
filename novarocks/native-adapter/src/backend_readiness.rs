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

//! Native endpoint readiness probing for Backend role composition.

use std::time::Duration;

use novarocks_types::NativeEndpoint;

use crate::BackendDataRuntime;

/// Confirms an HTTP/2 channel to this process's advertised Native endpoint.
///
/// The role keeps ownership of listener startup and cleanup. This transport
/// adapter owns dial admission and its timeout. It does not submit an
/// application RPC or attest its JWT authentication.
pub fn wait_for_backend_native_endpoint_ready(
    runtime: &BackendDataRuntime,
    endpoint: NativeEndpoint,
    class: crate::native_transport_admission::TransportClass,
    timeout: Duration,
) -> Result<(), String> {
    let connector = crate::native_client::admitted_connector(runtime, &endpoint, class, None)?;
    let channel_endpoint = crate::native_client::native_endpoint(runtime, &endpoint)?;
    runtime.block_on(async move {
        let channel =
            tokio::time::timeout(timeout, channel_endpoint.connect_with_connector(connector))
                .await
                .map_err(|_| {
                    format!(
                        "advertised Native endpoint {endpoint} did not become ready within {}ms",
                        timeout.as_millis()
                    )
                })?
                .map_err(|error| {
                    format!(
                        "advertised Native endpoint {endpoint} readiness failed: {}",
                        readiness_error_chain(&error)
                    )
                })?;
        // Dropping the channel closes its connection; the admission positions
        // follow that connection's IO until it is destroyed.
        drop(channel);
        Ok(())
    })
}

// This startup probe uses only the typed Native endpoint and IO connector;
// no RPC, request headers or application error is part of this chain.
fn readiness_error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut detail = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        detail.push_str(": ");
        detail.push_str(&cause.to_string());
        source = cause.source();
    }
    detail
}

#[cfg(test)]
mod tests {
    #[derive(Debug)]
    struct TransportFailure(std::io::Error);

    impl std::fmt::Display for TransportFailure {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("transport error")
        }
    }

    impl std::error::Error for TransportFailure {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn readiness_retains_the_original_os_failure_under_transport_display() {
        let original = std::io::Error::from_raw_os_error(libc::EADDRNOTAVAIL);
        let expected = format!("transport error: {original}");
        let error = TransportFailure(original);
        assert_eq!(error.to_string(), "transport error");
        assert_eq!(super::readiness_error_chain(&error), expected);
    }
}
