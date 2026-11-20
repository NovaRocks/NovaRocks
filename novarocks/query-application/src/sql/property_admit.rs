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

//! Typed user-data admission errors for DDL property scopes.
//!
//! Only errors authored by property evaluation/validation use these identities.
//! Source, control, outer failures and unsupported capabilities retain their
//! original authors; this module defines no conversion from those errors.

use novarocks_parser::Span;
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

const ADMIT_INVALID_CATALOG_PROPERTY: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.invalid_catalog_property"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_INVALID_SCHEMA_PROPERTY: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.invalid_schema_property"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_INVALID_TABLE_PROPERTY: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.invalid_table_property"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_INVALID_MATERIALIZED_VIEW_PROPERTY: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.invalid_materialized_view_property"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_INVALID_ANALYZE_PROPERTY: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.invalid_analyze_property"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_INVALID_BRANCH_PROPERTY: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.invalid_branch_property"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

/// Stable DDL-property identities exported to the original manifest owner.
/// The analyzer's existing invalid_analyze_property descriptor remains separate.
pub const DDL_PROPERTY_ERROR_CODE_DESCRIPTORS: &[ErrorCodeDescriptor] = &[
    ADMIT_INVALID_CATALOG_PROPERTY,
    ADMIT_INVALID_SCHEMA_PROPERTY,
    ADMIT_INVALID_TABLE_PROPERTY,
    ADMIT_INVALID_MATERIALIZED_VIEW_PROPERTY,
    ADMIT_INVALID_ANALYZE_PROPERTY,
    ADMIT_INVALID_BRANCH_PROPERTY,
];

/// The owner-provided scope of a user property data error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlPropertyScope {
    Catalog,
    Schema,
    Table,
    MaterializedView,
    Analyze,
    Branch,
}

impl DdlPropertyScope {
    const fn descriptor(self) -> ErrorCodeDescriptor {
        match self {
            Self::Catalog => ADMIT_INVALID_CATALOG_PROPERTY,
            Self::Schema => ADMIT_INVALID_SCHEMA_PROPERTY,
            Self::Table => ADMIT_INVALID_TABLE_PROPERTY,
            Self::MaterializedView => ADMIT_INVALID_MATERIALIZED_VIEW_PROPERTY,
            Self::Analyze => ADMIT_INVALID_ANALYZE_PROPERTY,
            Self::Branch => ADMIT_INVALID_BRANCH_PROPERTY,
        }
    }

    pub fn to_user_error(self, source: &str, span: Span, message: impl Into<String>) -> UserError {
        UserError::from_descriptor(
            self.descriptor(),
            message,
            Some(span.to_user_error_location(source)),
            RetryClass::Never,
        )
    }

    pub fn without_location(self, message: impl Into<String>) -> UserError {
        UserError::from_descriptor(self.descriptor(), message, None, RetryClass::Never)
    }
}
