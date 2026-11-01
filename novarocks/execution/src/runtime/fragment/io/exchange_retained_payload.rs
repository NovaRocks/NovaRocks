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

//! The original encoded allocation and its accounting travel with all byte aliases.

use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};

/// One driver-owned publication. Cloning its bytes neither copies the encoded
/// allocation nor creates another accounting charge. Native request aliases
/// retain the same backing owner after local custody or Task cancellation.
pub(crate) struct ExchangeRetainedPayload {
    bytes: Bytes,
    capacity: usize,
    accounting: Arc<Mutex<Option<TrackedBytes>>>,
}

struct EncodedBacking {
    // Drop the physical allocation before releasing its accounting holder.
    storage: Vec<u8>,
    _accounting: Arc<Mutex<Option<TrackedBytes>>>,
}

impl AsRef<[u8]> for EncodedBacking {
    fn as_ref(&self) -> &[u8] {
        &self.storage
    }
}

impl ExchangeRetainedPayload {
    pub(crate) fn new(storage: Vec<u8>, accounting: Option<TrackedBytes>) -> Self {
        let capacity = storage.capacity();
        let accounting = Arc::new(Mutex::new(accounting));
        let bytes = Bytes::from_owner(EncodedBacking {
            storage,
            _accounting: Arc::clone(&accounting),
        });
        Self {
            bytes,
            capacity,
            accounting,
        }
    }

    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub(crate) fn transfer_to(&self, tracker: Arc<MemTracker>) {
        let mut accounting = self
            .accounting
            .lock()
            .expect("exchange backing accounting lock");
        match accounting.as_mut() {
            Some(accounting) => accounting.transfer_to(tracker),
            None => *accounting = Some(TrackedBytes::new(self.capacity, tracker)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_allocation_charge_survives_custody_and_every_actual_rpc_alias() {
        let pending = MemTracker::new_root("PendingPayloads");
        let retained = MemTracker::new_root("SendQueuePayloads");
        let mut storage = Vec::with_capacity(128);
        storage.extend_from_slice(b"original wire bytes");
        let pointer = storage.as_ptr();
        let capacity = storage.capacity();
        let payload = ExchangeRetainedPayload::new(
            storage,
            Some(TrackedBytes::new(capacity, pending.clone())),
        );
        assert_eq!(payload.bytes().as_ptr(), pointer);
        assert_eq!(pending.current(), capacity as i64);
        let rpc = payload.bytes().clone();
        let replay = rpc.clone();
        payload.transfer_to(retained.clone());
        assert_eq!(pending.current(), 0);
        assert_eq!(retained.current(), capacity as i64);
        // The sender/driver can release custody while real RPC aliases live.
        drop(payload);
        assert_eq!(rpc.as_ref(), b"original wire bytes");
        assert_eq!(retained.current(), capacity as i64);
        drop(rpc);
        assert_eq!(retained.current(), capacity as i64);
        drop(replay);
        assert_eq!(retained.current(), 0);
        assert_eq!(retained.allocated(), retained.deallocated());
    }

    #[test]
    fn late_binding_and_repeated_transfer_keep_one_charge_for_the_same_backing() {
        let first = MemTracker::new_root("First");
        let second = MemTracker::new_root("Second");
        let mut storage = Vec::with_capacity(64);
        storage.extend_from_slice(b"same backing");
        let capacity = storage.capacity();
        let payload = ExchangeRetainedPayload::new(storage, None);
        let rpc = payload.bytes().clone();
        payload.transfer_to(first.clone());
        payload.transfer_to(first.clone());
        assert_eq!(first.current(), capacity as i64);
        assert_eq!(first.allocated(), capacity as i64);
        payload.transfer_to(second.clone());
        assert_eq!(first.current(), 0);
        assert_eq!(second.current(), capacity as i64);
        drop(payload);
        assert_eq!(second.current(), capacity as i64);
        drop(rpc);
        assert_eq!(second.current(), 0);
    }
}
