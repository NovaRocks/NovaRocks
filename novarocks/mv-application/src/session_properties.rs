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

//! Catalog session property declarations owned by MV refresh planning.

use novarocks_query_application::sql::session::registry::{
    PropertyDeclaration, SessionPropertyError, SessionPropertyInputs, SessionPropertyType,
    SessionPropertyValue,
};
use novarocks_spi::connector::ConnectorProviderBindingKey;

pub const INCREMENTAL_REFRESH_ENABLED: &str = "incremental_refresh_enabled";
const DEFAULT_INCREMENTAL_REFRESH_ENABLED: bool = true;

pub fn incremental_refresh_enabled_declaration() -> Result<PropertyDeclaration, SessionPropertyError>
{
    PropertyDeclaration::catalog(
        INCREMENTAL_REFRESH_ENABLED,
        SessionPropertyType::Boolean,
        Some(SessionPropertyValue::Boolean(
            DEFAULT_INCREMENTAL_REFRESH_ENABLED,
        )),
        "Enable incremental refresh for materialized views when possible",
        false,
        |_| Ok(()),
    )
}

/// Resolve the manual refresh setting against the actual target placement.
/// The legacy FULL syntax affects this one request.
pub fn manual_refresh_is_full(
    inputs: &SessionPropertyInputs,
    target: &ConnectorProviderBindingKey,
    legacy_full: bool,
) -> Result<bool, &'static str> {
    let incremental =
        match inputs.read_catalog_override_for_binding(target, INCREMENTAL_REFRESH_ENABLED) {
            Some(SessionPropertyValue::Boolean(value)) => *value,
            None => DEFAULT_INCREMENTAL_REFRESH_ENABLED,
            Some(_) => return Err("MV refresh received a non-boolean validated session property"),
        };
    Ok(legacy_full || !incremental)
}
