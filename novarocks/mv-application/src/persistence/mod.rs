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

//! Lake-authoritative MV documents and rebuildable Accelerator values.

pub mod aggregate_bindings;
mod binding_envelope;
pub mod codec;
pub mod create_documents;
pub mod definition;
pub mod dependency;
pub mod documents;
pub mod exact_revision;
pub mod identity;
pub mod keyed_documents;
pub mod publication_facts;
pub mod rebuild_classification;
pub mod runtime_bindings;
pub mod schema;
pub mod semantic;
pub mod storage_mapping;
pub mod validation;

#[cfg(any(test, novarocks_dev_controls))]
#[doc(hidden)]
pub mod test_support;

pub(crate) mod generated {
    include!(concat!(env!("OUT_DIR"), "/novarocks.mv.persistence.v1.rs"));
}
