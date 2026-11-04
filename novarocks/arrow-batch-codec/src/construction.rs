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

//! Shared Arrow IPC construction geometry. These numbers describe the locked
//! implementation's requests and opaque operation bounds, never memory grants
//! or semantic declarations. Caller source provenance and receiving policy stay
//! at their original boundary.

mod schema_geometry;
mod stream_geometry;
pub use schema_geometry::{
    LockedFieldLocation, SchemaAllocationMetrics, SchemaGeometry, SchemaRequestAccumulator,
    SchemaResourceSource, TypeHeaderGeometry, locked_field_location_layout,
    schema_backing_upper_bound, schema_writer_allocation_metrics, type_header_geometry,
};
pub use stream_geometry::{
    ConstructionWork, IpcConstructionError, IpcStreamBatchFacts, IpcStreamSchemaFacts,
    observe_stream_logical_size_construction, observe_stream_writer_batch_construction,
    observe_stream_writer_schema_construction, observe_unit_batch_construction,
    observe_unit_writer_declaration,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IpcGeometryError {
    Overflow,
    SourceModel,
}
impl std::fmt::Display for IpcGeometryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overflow => f.write_str("Arrow IPC construction geometry overflow"),
            Self::SourceModel => f.write_str("Arrow IPC construction source model changed"),
        }
    }
}
impl std::error::Error for IpcGeometryError {}

// The error and its neutral fact move together. Preserve the original reason
// identity consumed by existing Function, constant-codec and execution callers;
// this projection owns no Task conclusion, SQL mapping or recovery policy.
impl novarocks_failure_contract::FailureProjection for IpcGeometryError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self {
            Self::Overflow => (FailureClass::Resource, 148),
            Self::SourceModel => (FailureClass::Invariant, 149),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

mod schema_verification;
pub use schema_verification::{
    FieldVerificationFailure, FieldVerificationPurpose, IpcFieldRole, verify_field,
};

mod reader_geometry;
pub use reader_geometry::{ReaderStructureClass, ReaderStructureFailure, ReaderStructureObserver};

mod reader_array_geometry;
pub use reader_array_geometry::{
    ReaderArrayHeaderClass, ReaderArrayStructureObserver, original_reader_buffer_header_upper_bound,
};
