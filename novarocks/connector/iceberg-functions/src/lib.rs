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

//! Lightweight Iceberg-owned engine functions.
//!
//! This crate deliberately contains no catalog, table, Puffin, transaction,
//! frontend, backend, or server dependency. It owns only the standard Iceberg
//! value serialization used by the hidden Theta aggregate and pure compact
//! sketch operations.

mod canonical;
mod theta;

use arrow_schema::DataType;

/// Whether the hidden Iceberg Theta aggregate has an exact canonical encoding
/// for this Arrow input type.
///
/// Planning and runtime both call this single table so a provider can never
/// advertise an overload that the kernel later refuses.
pub fn supports_theta_input_type(data_type: &DataType) -> bool {
    canonical::CanonicalKind::from_data_type(data_type).is_ok()
}

pub use theta::{
    ICEBERG_THETA_AGGREGATE_NAME, ICEBERG_THETA_IMPLEMENTATION_IDENTITY,
    ICEBERG_THETA_MAX_COMPACT_BYTES, ICEBERG_THETA_STATE_FORMAT_IDENTITY, IcebergFunctionBundle,
    IcebergThetaAggregateFamily, IcebergThetaError, estimate_compact_theta,
    iceberg_theta_registration, union_compact_theta, validate_compact_theta,
};
