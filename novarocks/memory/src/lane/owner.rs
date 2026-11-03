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

//! Owner-side access responsibility, released only outside allocator hooks.
use super::{
    record::{LaneRecord, ResponsibilityClass},
    store::RecordStorePin,
    token::RecordRef,
};

/// Unique owner of a registry record. Shared lane handles retain this value
/// through an Arc; allocation tokens deliberately do not retain the owner.
#[derive(Debug)]
pub struct RecordOwner {
    pub(crate) store: RecordStorePin,
    pub(crate) reference: RecordRef,
}
impl RecordOwner {
    pub const fn reference(&self) -> RecordRef {
        self.reference
    }
    pub fn record(&self) -> &LaneRecord {
        self.store
            .store()
            .resolve(self.reference)
            .expect("live record owner")
    }
    pub fn store(&self) -> &super::store::RecordStore {
        self.store.store()
    }
    pub fn store_pin(&self) -> &RecordStorePin {
        &self.store
    }
    pub fn responsibility_class(&self) -> ResponsibilityClass {
        self.record().responsibility_class()
    }
}
impl Drop for RecordOwner {
    fn drop(&mut self) {
        self.store.store().drain(self.reference);
    }
}
