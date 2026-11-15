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

use novarocks_spi::connector::ConnectorError;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemTableError {
    Provider(ConnectorError),
    InvalidRequest(String),
    Internal(String),
}

impl From<ConnectorError> for SystemTableError {
    fn from(error: ConnectorError) -> Self {
        Self::Provider(error)
    }
}
impl fmt::Display for SystemTableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider(error) => error.fmt(f),
            Self::InvalidRequest(message) | Self::Internal(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for SystemTableError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Provider(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemTableRegistryError {
    InvalidName { kind: &'static str, name: String },
    InvalidScope { schema: String },
    DuplicateTable { schema: String, table: String },
    StandardNameConflict { schema: String, table: String },
    DuplicateColumn { name: String },
    InvalidKeyColumn { ordinal: usize },
    DuplicateKeyColumn { ordinal: usize },
}
impl fmt::Display for SystemTableRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName { kind, name } => write!(
                f,
                "system table {kind} must be a lowercase identifier: {name}"
            ),
            Self::InvalidScope { schema } => write!(
                f,
                "system table schema is not allowed in this scope: {schema}"
            ),
            Self::DuplicateTable { schema, table } => {
                write!(f, "duplicate system table registration: {schema}.{table}")
            }
            Self::StandardNameConflict { schema, table } => write!(
                f,
                "system table conflicts with a Trino standard name: {schema}.{table}"
            ),
            Self::DuplicateColumn { name } => write!(f, "duplicate system table column: {name}"),
            Self::InvalidKeyColumn { ordinal } => write!(
                f,
                "system table key ordinal is outside its columns: {ordinal}"
            ),
            Self::DuplicateKeyColumn { ordinal } => {
                write!(f, "duplicate system table key ordinal: {ordinal}")
            }
        }
    }
}
impl std::error::Error for SystemTableRegistryError {}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorErrorKind;
    #[test]
    fn provider_errors_keep_the_original_typed_fact() {
        let original = ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "view listing is unsupported",
        );
        let wrapped = SystemTableError::from(original.clone());
        assert_eq!(wrapped, SystemTableError::Provider(original));
    }
}
