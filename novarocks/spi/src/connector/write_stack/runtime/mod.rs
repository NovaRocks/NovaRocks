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

//! Transport-neutral connector write runtime values.
//!
//! The public values here reveal only an exact binding and neutral facts. Their
//! provider payloads are private to this crate: a role host, the SQL planner,
//! and the execution engine can move them, count them, and hand them back to
//! their owning provider, but they cannot read or construct them. A provider
//! supplies concrete associated types to the generic adapter in the sibling
//! module.
//!
//! Three values cross module boundaries, and each has a different reach:
//!
//! * [`ConnectorWriteCommitHandle`] never leaves the frontend. It is not
//!   `Clone`, has no codec facet, and is borrowed rather than moved by every
//!   terminal call, so it structurally cannot be duplicated into a native
//!   fragment, a state store, a result row, or a backend binding.
//! * [`ConnectorWriterHandle`] is an immutable logical write recipe. It *is*
//!   `Clone`, because one logical target is copied to every physical writer
//!   placement that serves it.
//! * [`ConnectorCommitFragment`] describes one provider artifact produced by
//!   one finished writer. It travels backend to frontend as canonical bytes and
//!   becomes a provider value again only inside the frontend control binding.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub use novarocks_connector_contract::ConnectorWriteBinding;

// Both fields occupy the same Arc pointee. Field order destroys the actual
// Provider value before returning its original caller-owned retention.
struct WritePayloadOwner<T> {
    value: T,
    _retention: Option<crate::connector::ConnectorPayloadRetentionGuard>,
}

#[derive(Clone)]
pub(super) struct OpaqueWritePayload(Arc<dyn Any + Send + Sync>);

impl OpaqueWritePayload {
    pub(super) fn new<T: Send + Sync + 'static>(value: T) -> Self {
        Self(Arc::new(WritePayloadOwner {
            value,
            _retention: None,
        }))
    }

    pub(super) fn retained<T: Send + Sync + 'static>(
        value: T,
        retention: crate::connector::ConnectorPayloadRetentionGuard,
    ) -> Self {
        Self(Arc::new(WritePayloadOwner {
            value,
            _retention: Some(retention),
        }))
    }

    pub(super) fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.0
            .downcast_ref::<WritePayloadOwner<T>>()
            .map(|owner| &owner.value)
    }
}

/// A frontend-only provider write transaction or session.
///
/// It is deliberately not `Clone` and has no accessor for its payload. The
/// only ways to use it are the terminal calls on the provider's own control
/// binding, which borrow it.
///
/// ```compile_fail
/// use novarocks_spi::connector::write_stack::ConnectorWriteCommitHandle;
///
/// fn leak(handle: ConnectorWriteCommitHandle) {
///     let _ = handle.payload;
/// }
/// ```
///
/// ```compile_fail
/// use novarocks_spi::connector::write_stack::ConnectorWriteCommitHandle;
///
/// fn duplicate(handle: &ConnectorWriteCommitHandle) -> ConnectorWriteCommitHandle {
///     handle.clone()
/// }
/// ```
pub struct ConnectorWriteCommitHandle {
    binding: ConnectorWriteBinding,
    payload: OpaqueWritePayload,
}

impl ConnectorWriteCommitHandle {
    pub(super) const fn from_parts(
        binding: ConnectorWriteBinding,
        payload: OpaqueWritePayload,
    ) -> Self {
        Self { binding, payload }
    }

    pub(super) const fn payload(&self) -> &OpaqueWritePayload {
        &self.payload
    }

    pub const fn binding(&self) -> &ConnectorWriteBinding {
        &self.binding
    }
}

impl Debug for ConnectorWriteCommitHandle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorWriteCommitHandle")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

/// An immutable, schedule-independent logical write recipe.
///
/// It describes one logical write target and branch: table format facts, target
/// schema, partition spec, file format, delete semantics, output naming, and
/// exact references to any pre-existing artifacts the writer must re-read. It
/// deliberately contains no query, attempt, fragment, backend, driver, or sink
/// identity, no credential value, and no already-read bulk object such as a
/// materialized deletion vector.
///
/// ```compile_fail
/// use novarocks_spi::connector::write_stack::ConnectorWriterHandle;
///
/// fn leak(handle: ConnectorWriterHandle) {
///     let _ = handle.payload;
/// }
/// ```
#[derive(Clone)]
pub struct ConnectorWriterHandle {
    binding: ConnectorWriteBinding,
    payload: OpaqueWritePayload,
}

impl ConnectorWriterHandle {
    pub(super) const fn from_parts(
        binding: ConnectorWriteBinding,
        payload: OpaqueWritePayload,
    ) -> Self {
        Self { binding, payload }
    }

    pub(super) const fn payload(&self) -> &OpaqueWritePayload {
        &self.payload
    }

    pub const fn binding(&self) -> &ConnectorWriteBinding {
        &self.binding
    }
}

impl Debug for ConnectorWriterHandle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorWriterHandle")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

/// One provider artifact produced by one finished writer.
///
/// A commit fragment is a single written data or delete artifact description,
/// not a per-writer report document. A writer emits zero or more of them, each
/// independently encodable, so the transport never has to split one provider
/// document across frames.
///
/// ```compile_fail
/// use novarocks_spi::connector::write_stack::ConnectorCommitFragment;
///
/// fn leak(fragment: ConnectorCommitFragment) {
///     let _ = fragment.payload;
/// }
/// ```
pub struct ConnectorCommitFragment {
    binding: ConnectorWriteBinding,
    payload: OpaqueWritePayload,
}

impl ConnectorCommitFragment {
    pub(super) const fn from_parts(
        binding: ConnectorWriteBinding,
        payload: OpaqueWritePayload,
    ) -> Self {
        Self { binding, payload }
    }

    pub(super) const fn payload(&self) -> &OpaqueWritePayload {
        &self.payload
    }

    pub const fn binding(&self) -> &ConnectorWriteBinding {
        &self.binding
    }
}

impl Debug for ConnectorCommitFragment {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorCommitFragment")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}
