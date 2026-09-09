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

//! Backend role-local async runtime dependencies.
//!
//! The server composition root creates this adapter from the role data-plane
//! runtime and passes clones through every Backend outbound RPC client. It keeps
//! the runtime handle and channel cache instance-owned; no Core or process
//! global runtime participates in Backend transport work.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use novarocks_native_trust::{
    AutomaticTlsMaterial, NativeEndpointConnector, NativeIncomingAdapter, NativeTlsMaterial,
    NativeTrust,
};
use novarocks_task_codec::domain::ConfidentialTransport;
use novarocks_types::NativeEndpoint;
use tokio::runtime::Handle;
use tonic::transport::Channel;

/// Server-resolved transport material for the Backend-owned Native listener
/// and outbound connections. The backend receives this capability only; it
/// never reads configuration files, credentials, or PEM paths.
#[derive(Clone, Debug)]
pub enum BackendNativeTransport {
    Plaintext,
    Automatic(AutomaticTlsMaterial),
    Pem(NativeTlsMaterial),
}

impl BackendNativeTransport {
    /// The confidentiality fact shared by this process's Native listener and
    /// its raw RPC admission adapters.
    ///
    /// This is derived from the Server-resolved transport mode rather than
    /// inferred from a request or endpoint. Automatic and PEM listeners both
    /// terminate TLS before the RPC service sees a request; plaintext h2c does
    /// not protect the payload in transit.
    pub(crate) const fn confidentiality(&self) -> ConfidentialTransport {
        match self {
            Self::Plaintext => ConfidentialTransport::Plaintext,
            Self::Automatic(_) | Self::Pem(_) => ConfidentialTransport::Confidential,
        }
    }

    pub(crate) fn connector_for(
        &self,
        endpoint: NativeEndpoint,
    ) -> Result<NativeEndpointConnector, String> {
        match self {
            Self::Plaintext => Ok(NativeEndpointConnector::plaintext(endpoint)),
            Self::Automatic(material) => NativeEndpointConnector::automatic(endpoint, material)
                .map_err(|error| format!("construct automatic native connector: {error}")),
            Self::Pem(material) => Ok(NativeEndpointConnector::pem(endpoint, material)),
        }
    }

    pub(crate) fn incoming_adapter(&self) -> NativeIncomingAdapter {
        match self {
            Self::Plaintext => NativeIncomingAdapter::plaintext(),
            Self::Automatic(material) => NativeIncomingAdapter::automatic(material),
            Self::Pem(material) => NativeIncomingAdapter::pem(material),
        }
    }
}

#[derive(Clone)]
pub struct BackendDataRuntime {
    handle: Handle,
    native_trust: Arc<NativeTrust>,
    native_transport: BackendNativeTransport,
    channels: Arc<Mutex<HashMap<NativeEndpoint, Channel>>>,
}

impl BackendDataRuntime {
    pub fn new(
        handle: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: BackendNativeTransport,
    ) -> Self {
        Self {
            handle,
            native_trust,
            native_transport,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.handle.block_on(future))
        } else {
            self.handle.block_on(future)
        }
    }

    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }

    pub(crate) fn native_trust(&self) -> &Arc<NativeTrust> {
        &self.native_trust
    }

    pub(crate) fn native_transport(&self) -> &BackendNativeTransport {
        &self.native_transport
    }

    pub(crate) fn channels(&self) -> &Arc<Mutex<HashMap<NativeEndpoint, Channel>>> {
        &self.channels
    }
}

#[cfg(test)]
pub(crate) fn test_backend_data_runtime() -> BackendDataRuntime {
    static TEST_RUNTIME: std::sync::LazyLock<(tokio::runtime::Runtime, BackendDataRuntime)> =
        std::sync::LazyLock::new(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(1)
                .build()
                .expect("build Backend test data runtime");
            let adapter = BackendDataRuntime::new(
                runtime.handle().clone(),
                test_backend_native_trust(),
                BackendNativeTransport::Plaintext,
            );
            (runtime, adapter)
        });
    TEST_RUNTIME.1.clone()
}

#[cfg(test)]
pub(crate) fn test_backend_native_trust() -> Arc<NativeTrust> {
    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeTransportMode, ValidatedSharedSecret,
    };
    use novarocks_secret::SecretValue;

    Arc::new(NativeTrust::new(
        DeploymentId::parse("backend-test").expect("valid deployment"),
        ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
            .expect("valid secret"),
        NativeCallerSubject::parse("be@127.0.0.1:9070").expect("valid subject"),
        NativeTransportMode::Disabled,
    ))
}

#[cfg(test)]
mod tests {
    use super::{BackendDataRuntime, BackendNativeTransport, test_backend_native_trust};

    #[test]
    fn cloned_adapter_shares_one_role_cache_and_distinct_adapters_do_not() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .expect("build adapter test runtime");
        let first = BackendDataRuntime::new(
            runtime.handle().clone(),
            test_backend_native_trust(),
            BackendNativeTransport::Plaintext,
        );
        let clone = first.clone();
        let second = BackendDataRuntime::new(
            runtime.handle().clone(),
            test_backend_native_trust(),
            BackendNativeTransport::Plaintext,
        );

        assert!(std::sync::Arc::ptr_eq(first.channels(), clone.channels()));
        assert!(!std::sync::Arc::ptr_eq(first.channels(), second.channels()));
    }

    #[test]
    fn block_on_preserves_external_and_active_runtime_paths() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .expect("build adapter test runtime");
        let adapter = BackendDataRuntime::new(
            runtime.handle().clone(),
            test_backend_native_trust(),
            BackendNativeTransport::Plaintext,
        );

        assert_eq!(adapter.block_on(async { 7_u8 }), 7);
        let active_adapter = adapter.clone();
        runtime.block_on(async move {
            assert_eq!(active_adapter.block_on(async { 11_u8 }), 11);
        });
    }
}
