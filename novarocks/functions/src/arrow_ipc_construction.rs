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

pub use novarocks_arrow_batch_codec::construction::{
    FieldVerificationFailure, FieldVerificationPurpose, IpcFieldRole, IpcGeometryError,
    LockedFieldLocation, ReaderArrayHeaderClass, ReaderArrayStructureObserver,
    ReaderStructureClass, ReaderStructureFailure, ReaderStructureObserver, SchemaAllocationMetrics,
    SchemaGeometry, SchemaRequestAccumulator, SchemaResourceSource, TypeHeaderGeometry,
    locked_field_location_layout, original_reader_buffer_header_upper_bound,
    schema_backing_upper_bound, schema_writer_allocation_metrics, type_header_geometry,
    verify_field,
};
mod stream_geometry;
pub use stream_geometry::{
    IpcConstructionError, IpcStreamBatchFacts, IpcStreamSchemaFacts,
    observe_stream_logical_size_construction, observe_stream_writer_batch_construction,
    observe_stream_writer_schema_construction, observe_unit_batch_construction,
};
