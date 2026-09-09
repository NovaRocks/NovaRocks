//! FE-owned runtime access for synchronous native transport ports.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use novarocks_native_trust::NativeTrust;
use novarocks_task_codec::TransportBudget;
use novarocks_types::NativeEndpoint;
use tokio::runtime::Handle;
use tonic::transport::Channel;

use super::transport::FrontendNativeTransport;
use super::transport_supervisor::NativeTransportSupervisor;

/// The Frontend role's explicitly composed Tokio runtime capability.
///
/// Native transport ports are synchronous Core-facing traits.  Their RPC work
/// runs on this role-owned handle, and they retain the historical two-path
/// `block_on` behavior when called both inside and outside a Tokio context.
#[derive(Clone)]
pub(crate) struct FrontendDataRuntime {
    handle: Handle,
    native_trust: Arc<NativeTrust>,
    native_transport: FrontendNativeTransport,
    channels: Arc<Mutex<HashMap<NativeEndpoint, Channel>>>,
    task_transport_supervisor: NativeTransportSupervisor,
}

impl FrontendDataRuntime {
    pub(crate) fn new_with_native_trust(
        handle: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
        task_transport_budget: TransportBudget,
    ) -> Result<Self, String> {
        Ok(Self {
            handle,
            native_trust,
            native_transport,
            channels: Arc::new(Mutex::new(HashMap::new())),
            task_transport_supervisor: NativeTransportSupervisor::from_transport(
                task_transport_budget,
            )?,
        })
    }

    #[cfg(test)]
    pub(crate) fn new(handle: Handle) -> Self {
        use novarocks_native_trust::{
            DeploymentId, NativeCallerSubject, NativeTransportMode, ValidatedSharedSecret,
        };
        use novarocks_secret::SecretValue;

        let trust = NativeTrust::new(
            DeploymentId::parse("frontend-test").expect("fixed test deployment"),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .expect("fixed test shared secret"),
            NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("fixed test caller"),
            NativeTransportMode::Disabled,
        );
        Self::new_with_native_trust(
            handle,
            Arc::new(trust),
            FrontendNativeTransport::plaintext(),
            TransportBudget::DEFAULT,
        )
        .expect("the default task transport budget is valid")
    }

    pub(crate) fn native_trust(&self) -> &Arc<NativeTrust> {
        &self.native_trust
    }

    pub(crate) fn native_transport(&self) -> &FrontendNativeTransport {
        &self.native_transport
    }

    pub(crate) fn task_transport_supervisor(&self) -> &NativeTransportSupervisor {
        &self.task_transport_supervisor
    }

    pub(crate) fn block_on<F>(&self, future: F) -> Result<F::Output, String>
    where
        F: Future,
    {
        if Handle::try_current().is_ok() {
            Ok(tokio::task::block_in_place(|| self.handle.block_on(future)))
        } else {
            Ok(self.handle.block_on(future))
        }
    }

    pub(crate) fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    pub(crate) fn cached_channel(&self, endpoint: &NativeEndpoint) -> Option<Channel> {
        self.channels
            .lock()
            .expect("frontend native channel cache lock")
            .get(endpoint)
            .cloned()
    }

    pub(crate) fn cache_channel(&self, endpoint: NativeEndpoint, channel: Channel) {
        self.channels
            .lock()
            .expect("frontend native channel cache lock")
            .insert(endpoint, channel);
    }

    pub(crate) fn invalidate_channel(&self, endpoint: &NativeEndpoint) {
        self.channels
            .lock()
            .expect("frontend native channel cache lock")
            .remove(endpoint);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
    };
    use novarocks_secret::SecretValue;
    use novarocks_task_codec::TransportBudget;
    use novarocks_types::NativeEndpoint;

    use super::FrontendDataRuntime;
    use crate::native::transport::FrontendNativeTransport;

    fn data_runtime(handle: tokio::runtime::Handle) -> FrontendDataRuntime {
        let trust = NativeTrust::new(
            DeploymentId::parse("frontend-test").expect("deployment"),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .expect("secret"),
            NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("subject"),
            NativeTransportMode::Disabled,
        );
        FrontendDataRuntime::new_with_native_trust(
            handle,
            Arc::new(trust),
            FrontendNativeTransport::plaintext(),
            TransportBudget::DEFAULT,
        )
        .expect("the default task transport budget is valid")
    }

    #[test]
    fn block_on_runs_outside_a_tokio_context() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let data_runtime = data_runtime(runtime.handle().clone());
        assert_eq!(data_runtime.block_on(async { 7_u8 }).expect("block_on"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn block_on_runs_inside_a_tokio_context() {
        let data_runtime = data_runtime(tokio::runtime::Handle::current());
        assert_eq!(
            data_runtime.block_on(async { 11_u8 }).expect("block_on"),
            11
        );
    }

    #[test]
    fn channel_cache_is_scoped_to_one_role_runtime_generation() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let first = data_runtime(runtime.handle().clone());
        let (channel, _updates) =
            runtime.block_on(async { tonic::transport::Channel::balance_channel::<String>(1) });
        let endpoint = NativeEndpoint::from_host_port("be.example", 19040).expect("endpoint");
        first.cache_channel(endpoint.clone(), channel);
        assert!(first.cached_channel(&endpoint).is_some());
        first.invalidate_channel(&endpoint);
        assert!(first.cached_channel(&endpoint).is_none());

        let next_generation = data_runtime(runtime.handle().clone());
        assert!(next_generation.cached_channel(&endpoint).is_none());
    }
}
