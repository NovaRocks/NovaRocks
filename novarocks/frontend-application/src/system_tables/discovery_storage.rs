// SPDX-License-Identifier: Apache-2.0

use novarocks_failure_contract::SharedFailureCause;
use novarocks_spi::connector::DiscoveryAllocation;
use novarocks_type_contract::{
    CompileControlError, ValueTypeStorageHold, ValueTypeStorageLifetime,
    owned_resources::layout::arc_layout,
};
use novarocks_workload_control::{ResultPayloadBytes, ResultWindowAlias, WorkError};
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
};

/// Actual schema construction backing charged to this original statement.
pub(crate) struct DiscoveryStorage {
    bytes: Mutex<ResultPayloadBytes>,
}

impl DiscoveryStorage {
    pub(crate) fn try_new(window: &ResultWindowAlias) -> Result<Arc<Self>, WorkError> {
        let layout =
            arc_layout(Layout::new::<Self>()).map_err(|_| WorkError::ArithmeticOverflow)?;
        let header = u64::try_from(layout.size()).map_err(|_| WorkError::ArithmeticOverflow)?;
        let bytes = window.reserve_payload_bytes(header)?;
        Ok(Arc::new(Self {
            bytes: Mutex::new(bytes),
        }))
    }
}

impl ValueTypeStorageHold for DiscoveryStorage {}

impl DiscoveryAllocation for DiscoveryStorage {
    fn request(&self, layout: Layout) -> Result<(), CompileControlError> {
        let bytes = u64::try_from(layout.size()).map_err(|_| {
            CompileControlError::Source(SharedFailureCause::new(WorkError::ArithmeticOverflow))
        })?;
        let mut held = self
            .bytes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        held.try_grow(bytes)
            .inspect_err(|error| {
                tracing::debug!(
                    requested_bytes = bytes,
                    discovery_retained_bytes = held.bytes(),
                    error = %error,
                    "Provider metadata layout request refused"
                );
            })
            .map_err(|error| CompileControlError::Source(SharedFailureCause::new(error)))
    }

    fn retention(self: Arc<Self>) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};

    #[test]
    fn metadata_discovery_reuses_the_original_window_through_storage_aliases() {
        let (_control, _root, capacity, _auth) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let window = capacity.window_alias();
        let before = window.retained_payload_bytes();
        let allocation = DiscoveryStorage::try_new(&window).unwrap();
        let header_only = window.retained_payload_bytes();
        assert!(header_only > before);
        allocation
            .request(Layout::array::<u8>(100).unwrap())
            .unwrap();
        let retained = window.retained_payload_bytes();
        assert_eq!(retained, header_only + 100);

        let too_wide = usize::try_from(window.payload_capacity_bytes()).unwrap();
        let error = allocation
            .request(Layout::array::<u8>(too_wide).unwrap())
            .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        assert_eq!(window.retained_payload_bytes(), retained);

        let alias = Arc::clone(&allocation).retention();
        drop(allocation);
        assert_eq!(window.retained_payload_bytes(), retained);
        drop(alias);
        assert_eq!(window.retained_payload_bytes(), before);
    }
}
