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

//! Pure root-result vocabulary and flat client-row validation.
//!
//! Contracts contain immutable semantic facts only. Arrow arrays, sockets,
//! allocator leases, MySQL packet sequences and async work remain runtime-owned.

mod client_rows;
pub use client_rows::{
    BorrowedPayloadSpan, ClientBodyError, ClientRowProfile, ClientRowStreamCursor,
    ValidatedClientBody,
};

mod root;
pub use root::{
    FrozenRootOutput, InternalResultDomain, RootContractError, RootOutputContract, RootOutputKind,
    RootProfileId, RootProfileV1,
};

mod scalar;
pub use scalar::{
    NamedScalarField, ScalarField, ScalarOpaqueType, ScalarProfileV1, ScalarSchema,
    ScalarTimestampUnit, ScalarValueType,
};

mod scalar_leaf;
pub use scalar_leaf::{
    BorrowedScalarLeaf, SCALAR_LEAF_HEADER_BYTES, ScalarLeafCursor, ScalarLeafError,
    ScalarLeafHeader, ScalarLeafTurn,
};

mod scalar_visit;
pub use scalar_visit::{BorrowedScalarRecord, ScalarRecordEvent, ScalarVisitError};

mod scalar_record;
pub use scalar_record::{SCALAR_RECORD_MAX_BYTES, ScalarRecord, ScalarRecordWriter, ScalarValue};

mod render_schema;
pub use render_schema::{
    ClientPresentationFamily, ClientRenderSchema, NamedRenderField, NativeRenderType,
    OpaqueRenderType, RenderColumn, RenderField, RenderPresentation, RenderTimeUnit,
    TrinoJsonFacts,
};

mod write_counts;
pub use write_counts::*;
