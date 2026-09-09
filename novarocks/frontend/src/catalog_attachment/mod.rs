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

//! Frontend-owned durable catalog attachment facts.
//!
//! A catalog attachment is a StateStore fact.  Process-local Connector
//! bindings, health, leases, retries and BE installation never appear here.

mod codec;
mod key;
mod repository;
mod wakeup;

pub use key::{attachment_key, attachment_prefix};
pub(crate) use repository::assert_attachment_versions;
pub use repository::{
    CatalogAttachment, CatalogAttachmentError, CatalogAttachmentErrorKind,
    CatalogAttachmentRepository, CatalogAttachmentVersioned,
};
pub use wakeup::{CatalogAttachmentWakeup, CatalogAttachmentWakeupSignal};
