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

//! The provider-private seam between concrete write domain types and the
//! neutral values the rest of the process moves.
//!
//! A provider implements [`ProviderWriteRuntime`] with its own concrete commit
//! handle, writer handle, and commit fragment. Its codec and control code keep
//! a [`WriteRuntimeAdapter`] privately and use it to wrap and recover those
//! values. No installed role service exposes an adapter, an erased payload, or
//! a generic downcast operation, so a role host structurally cannot read or
//! forge provider write state.

use std::fmt::Debug;
use std::sync::Arc;

use crate::connector::write_stack::runtime::{
    ConnectorCommitFragment, ConnectorWriteBinding, ConnectorWriteCommitHandle,
    ConnectorWriterHandle, OpaqueWritePayload,
};
use crate::connector::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
};

/// One provider's concrete write domain, bound to one exact catalog generation.
pub trait ProviderWriteRuntime: Send + Sync + 'static {
    /// The provider's frontend-only write transaction or session.
    type CommitHandle: Debug + Send + Sync + 'static;
    /// The provider's immutable logical write recipe. It is `Clone` because one
    /// logical target is copied to every physical writer placement.
    type WriterHandle: Clone + Debug + Send + Sync + 'static;
    /// One provider artifact description produced by one finished writer.
    type CommitFragment: Debug + Send + Sync + 'static;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn catalog_handle(&self) -> &CatalogHandle;
}

pub struct WriteRuntimeAdapter<P> {
    provider: Arc<P>,
    binding: ConnectorWriteBinding,
}

impl<P> Clone for WriteRuntimeAdapter<P> {
    fn clone(&self) -> Self {
        Self {
            provider: self.provider.clone(),
            binding: self.binding.clone(),
        }
    }
}

impl<P: ProviderWriteRuntime> WriteRuntimeAdapter<P> {
    pub fn new(provider: Arc<P>) -> Self {
        assert_eq!(
            provider.descriptor().instance_id,
            *provider.catalog_handle().catalog_name(),
            "provider write runtime descriptor and catalog handle must name the same catalog"
        );
        let binding = ConnectorWriteBinding::new(
            provider.descriptor().clone(),
            provider.catalog_handle().clone(),
        );
        Self { provider, binding }
    }

    pub const fn binding(&self) -> &ConnectorWriteBinding {
        &self.binding
    }

    /// Wrap a provider commit handle. The result never leaves the frontend.
    pub fn wrap_commit_handle(&self, commit: P::CommitHandle) -> ConnectorWriteCommitHandle {
        ConnectorWriteCommitHandle::from_parts(
            self.binding.clone(),
            OpaqueWritePayload::new(commit),
        )
    }

    /// Wrap a provider writer recipe at a provider-owned codec or control
    /// boundary.
    pub fn wrap_writer_handle(&self, handle: P::WriterHandle) -> ConnectorWriterHandle {
        ConnectorWriterHandle::from_parts(self.binding.clone(), OpaqueWritePayload::new(handle))
    }

    /// Wrap one provider artifact description at a provider-owned codec or
    /// writer boundary.
    pub fn wrap_commit_fragment(&self, fragment: P::CommitFragment) -> ConnectorCommitFragment {
        ConnectorCommitFragment::from_parts(self.binding.clone(), OpaqueWritePayload::new(fragment))
    }

    /// Retain an already-admitted original holder in the same Arc as the
    /// Provider artifact. This neither authorizes allocation nor estimates the
    /// graph; the original creator must check its complete coexistence budget.
    pub fn wrap_retained_commit_fragment(
        &self,
        fragment: P::CommitFragment,
        retention: crate::connector::ConnectorPayloadRetentionGuard,
    ) -> ConnectorCommitFragment {
        ConnectorCommitFragment::from_parts(
            self.binding.clone(),
            OpaqueWritePayload::retained(fragment, retention),
        )
    }

    /// Recover a provider commit handle only through this adapter's exact
    /// binding.
    pub fn commit_handle<'a>(
        &self,
        handle: &'a ConnectorWriteCommitHandle,
    ) -> Result<&'a P::CommitHandle, ConnectorError> {
        if handle.binding() != &self.binding {
            return Err(binding_error());
        }
        handle.payload().downcast_ref().ok_or_else(type_error)
    }

    /// Recover a provider writer recipe only through this adapter's exact
    /// binding.
    pub fn writer_handle<'a>(
        &self,
        handle: &'a ConnectorWriterHandle,
    ) -> Result<&'a P::WriterHandle, ConnectorError> {
        if handle.binding() != &self.binding {
            return Err(binding_error());
        }
        handle.payload().downcast_ref().ok_or_else(type_error)
    }

    /// Recover a provider artifact description only through this adapter's
    /// exact binding.
    pub fn commit_fragment<'a>(
        &self,
        fragment: &'a ConnectorCommitFragment,
    ) -> Result<&'a P::CommitFragment, ConnectorError> {
        if fragment.binding() != &self.binding {
            return Err(binding_error());
        }
        fragment.payload().downcast_ref().ok_or_else(type_error)
    }
}

fn binding_error() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "connector write value does not belong to this exact provider generation",
    )
}

fn type_error() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "connector write value payload does not match its provider domain type",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{CatalogVersion, ConnectorInstanceId, ConnectorProviderId};

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeCommit(u8);

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeWriterHandle(u8);

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeCommitFragment(u8);

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct OtherWriterHandle(u8);

    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl FakeProvider {
        fn new(catalog: &str, version: u8) -> Arc<Self> {
            let instance_id = ConnectorInstanceId::parse(catalog).expect("instance id");
            Arc::new(Self {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                    instance_id: instance_id.clone(),
                },
                catalog_handle: CatalogHandle::new(
                    instance_id,
                    CatalogVersion::from_bytes([version; 32]),
                ),
            })
        }
    }

    impl ProviderWriteRuntime for FakeProvider {
        type CommitHandle = FakeCommit;
        type WriterHandle = FakeWriterHandle;
        type CommitFragment = FakeCommitFragment;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }
    }

    struct OtherProvider {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl ProviderWriteRuntime for OtherProvider {
        type CommitHandle = FakeCommit;
        type WriterHandle = OtherWriterHandle;
        type CommitFragment = FakeCommitFragment;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }
    }

    #[derive(Debug)]
    struct TrackedFragment {
        value: u8,
        events: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }
    impl Drop for TrackedFragment {
        fn drop(&mut self) {
            self.events.lock().unwrap().push("payload");
        }
    }
    struct TrackedRetention(Arc<std::sync::Mutex<Vec<&'static str>>>);
    impl Drop for TrackedRetention {
        fn drop(&mut self) {
            self.0.lock().unwrap().push("retention");
        }
    }
    struct TrackedProvider(Arc<FakeProvider>);
    impl ProviderWriteRuntime for TrackedProvider {
        type CommitHandle = FakeCommit;
        type WriterHandle = FakeWriterHandle;
        type CommitFragment = TrackedFragment;
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.0.descriptor
        }
        fn catalog_handle(&self) -> &CatalogHandle {
            &self.0.catalog_handle
        }
    }

    #[test]
    fn opaque_alias_retains_the_original_holder_until_its_real_payload_exits() {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let adapter =
            WriteRuntimeAdapter::new(Arc::new(TrackedProvider(FakeProvider::new("unit", 1))));
        let fragment = adapter.wrap_retained_commit_fragment(
            TrackedFragment {
                value: 9,
                events: Arc::clone(&events),
            },
            crate::connector::ConnectorPayloadRetentionGuard::new(TrackedRetention(Arc::clone(
                &events,
            ))),
        );
        assert_eq!(adapter.commit_fragment(&fragment).unwrap().value, 9);
        let opaque_alias = fragment.payload().clone();
        drop(fragment);
        assert!(events.lock().unwrap().is_empty());
        assert_eq!(
            opaque_alias
                .downcast_ref::<TrackedFragment>()
                .unwrap()
                .value,
            9
        );
        drop(opaque_alias);
        assert_eq!(*events.lock().unwrap(), vec!["payload", "retention"]);
    }

    #[test]
    fn wrapped_write_values_round_trip_through_their_own_adapter() {
        let adapter = WriteRuntimeAdapter::new(FakeProvider::new("unit", 1));
        let commit = adapter.wrap_commit_handle(FakeCommit(7));
        let handle = adapter.wrap_writer_handle(FakeWriterHandle(8));
        let fragment = adapter.wrap_commit_fragment(FakeCommitFragment(9));

        assert_eq!(
            adapter.commit_handle(&commit).expect("commit"),
            &FakeCommit(7)
        );
        assert_eq!(
            adapter.writer_handle(&handle).expect("handle"),
            &FakeWriterHandle(8)
        );
        assert_eq!(
            adapter.commit_fragment(&fragment).expect("fragment"),
            &FakeCommitFragment(9)
        );
        assert_eq!(commit.binding(), adapter.binding());
        assert_eq!(handle.binding(), adapter.binding());
        assert_eq!(fragment.binding(), adapter.binding());
    }

    #[test]
    fn a_writer_handle_copy_stays_recoverable_because_copies_are_the_production_shape() {
        let adapter = WriteRuntimeAdapter::new(FakeProvider::new("unit", 1));
        let handle = adapter.wrap_writer_handle(FakeWriterHandle(3));
        let copies = vec![handle.clone(), handle.clone(), handle];
        for copy in &copies {
            assert_eq!(
                adapter.writer_handle(copy).expect("copy"),
                &FakeWriterHandle(3)
            );
        }
    }

    #[test]
    fn a_later_generation_of_the_same_catalog_cannot_recover_an_earlier_value() {
        let current = WriteRuntimeAdapter::new(FakeProvider::new("unit", 1));
        let replacement = WriteRuntimeAdapter::new(FakeProvider::new("unit", 2));
        let handle = current.wrap_writer_handle(FakeWriterHandle(4));

        assert_eq!(
            replacement
                .writer_handle(&handle)
                .expect_err("foreign generation")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn another_catalog_cannot_recover_a_value() {
        let mine = WriteRuntimeAdapter::new(FakeProvider::new("unit", 1));
        let theirs = WriteRuntimeAdapter::new(FakeProvider::new("other", 1));
        let commit = mine.wrap_commit_handle(FakeCommit(5));

        assert_eq!(
            theirs
                .commit_handle(&commit)
                .expect_err("foreign catalog")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn a_same_binding_adapter_with_a_different_domain_type_is_corrupt_data_not_a_silent_cast() {
        let instance_id = ConnectorInstanceId::parse("unit").expect("instance id");
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
            instance_id: instance_id.clone(),
        };
        let catalog_handle = CatalogHandle::new(instance_id, CatalogVersion::from_bytes([1; 32]));
        let mine = WriteRuntimeAdapter::new(FakeProvider::new("unit", 1));
        let impostor = WriteRuntimeAdapter::new(Arc::new(OtherProvider {
            descriptor,
            catalog_handle,
        }));
        assert_eq!(impostor.binding(), mine.binding());

        let handle = mine.wrap_writer_handle(FakeWriterHandle(6));
        assert_eq!(
            impostor
                .writer_handle(&handle)
                .expect_err("mismatched domain type")
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }
}
