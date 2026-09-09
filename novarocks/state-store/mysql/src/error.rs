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

use mysql_async::{DriverError, Error, IoError, TlsError};
use novarocks_state_store_api::{StateStoreError, StateStoreErrorKind};

#[derive(Clone, Copy)]
enum MysqlTlsErrorClass {
    InvalidDnsName,
    Pem,
    VerifierBuilder,
    InvalidCertificateEncoding,
    Runtime,
}

fn classify_tls_error(error: &TlsError) -> StateStoreErrorKind {
    let class = match error {
        TlsError::InvalidDnsName(_) => MysqlTlsErrorClass::InvalidDnsName,
        TlsError::Pem(_) => MysqlTlsErrorClass::Pem,
        TlsError::VerifierBuilderError(_) => MysqlTlsErrorClass::VerifierBuilder,
        TlsError::Tls(rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)) => {
            MysqlTlsErrorClass::InvalidCertificateEncoding
        }
        TlsError::Tls(_) => MysqlTlsErrorClass::Runtime,
    };
    classify_tls_error_class(class)
}

fn classify_tls_error_class(class: MysqlTlsErrorClass) -> StateStoreErrorKind {
    match class {
        MysqlTlsErrorClass::InvalidDnsName
        | MysqlTlsErrorClass::Pem
        | MysqlTlsErrorClass::VerifierBuilder
        | MysqlTlsErrorClass::InvalidCertificateEncoding => {
            StateStoreErrorKind::InvalidConfiguration
        }
        MysqlTlsErrorClass::Runtime => StateStoreErrorKind::ProviderUnavailable,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MysqlNativeError {
    public: StateStoreError,
    transaction_disposition: MysqlTransactionDisposition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MysqlTransactionDisposition {
    RollbackRequired,
    TransactionEnded,
    DestroyConnection,
}

pub(super) enum MysqlReadStatementError {
    Public(StateStoreError),
    Native(MysqlNativeError),
    Deadline(StateStoreError),
}

impl From<StateStoreError> for MysqlReadStatementError {
    fn from(error: StateStoreError) -> Self {
        Self::Public(error)
    }
}

impl MysqlNativeError {
    pub(crate) fn provider_unavailable() -> Self {
        Self {
            public: StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "MySQL provider operation failed",
            ),
            transaction_disposition: MysqlTransactionDisposition::DestroyConnection,
        }
    }

    pub(crate) fn deadline() -> Self {
        Self {
            public: StateStoreError::new(
                StateStoreErrorKind::DeadlineExceeded,
                "MySQL provider operation exceeded its deadline",
            ),
            transaction_disposition: MysqlTransactionDisposition::DestroyConnection,
        }
    }

    pub(crate) fn invalid_configuration() -> Self {
        Self {
            public: StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL provider configuration was rejected",
            ),
            transaction_disposition: MysqlTransactionDisposition::DestroyConnection,
        }
    }

    fn conflict(transaction_disposition: MysqlTransactionDisposition) -> Self {
        Self {
            public: StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "MySQL transaction encountered a concurrent lock conflict",
            ),
            transaction_disposition,
        }
    }

    pub(crate) fn into_public(self) -> StateStoreError {
        self.public
    }

    pub(crate) const fn transaction_disposition(&self) -> MysqlTransactionDisposition {
        self.transaction_disposition
    }
}

impl From<Error> for MysqlNativeError {
    fn from(error: Error) -> Self {
        match error {
            Error::Server(server) if matches!(server.code, 1205 | 1213) => {
                Self::conflict(classify_server_transaction_disposition(server.code))
            }
            Error::Server(server) if matches!(server.code, 1044 | 1045 | 1049) => {
                Self::invalid_configuration()
            }
            Error::Io(IoError::Tls(error)) => match classify_tls_error(&error) {
                StateStoreErrorKind::InvalidConfiguration => Self::invalid_configuration(),
                StateStoreErrorKind::ProviderUnavailable => Self::provider_unavailable(),
                _ => unreachable!("TLS classifier returns only supported public classes"),
            },
            Error::Url(_) => Self::invalid_configuration(),
            Error::Driver(
                DriverError::UnknownAuthPlugin { .. }
                | DriverError::MysqlOldPasswordDisabled
                | DriverError::NoKeyFound
                | DriverError::NoClientSslFlagFromServer
                | DriverError::CleartextPluginDisabled
                | DriverError::InvalidParsecSalt,
            ) => Self::invalid_configuration(),
            Error::Server(_) => Self {
                public: StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "MySQL provider operation failed",
                ),
                transaction_disposition: MysqlTransactionDisposition::RollbackRequired,
            },
            Error::Driver(_) | Error::Io(IoError::Io(_)) | Error::Other(_) => {
                Self::provider_unavailable()
            }
        }
    }
}

const fn classify_server_transaction_disposition(code: u16) -> MysqlTransactionDisposition {
    match code {
        1205 => MysqlTransactionDisposition::RollbackRequired,
        1213 => MysqlTransactionDisposition::TransactionEnded,
        1044 | 1045 | 1049 => MysqlTransactionDisposition::DestroyConnection,
        _ => MysqlTransactionDisposition::RollbackRequired,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_pem_tls_error_is_invalid_configuration() {
        assert_eq!(
            classify_tls_error_class(MysqlTlsErrorClass::Pem),
            StateStoreErrorKind::InvalidConfiguration
        );
    }

    #[test]
    fn runtime_tls_error_is_provider_unavailable() {
        assert_eq!(
            classify_tls_error_class(MysqlTlsErrorClass::Runtime),
            StateStoreErrorKind::ProviderUnavailable
        );
    }

    #[test]
    fn invalid_certificate_encoding_is_invalid_configuration() {
        assert_eq!(
            classify_tls_error_class(MysqlTlsErrorClass::InvalidCertificateEncoding),
            StateStoreErrorKind::InvalidConfiguration
        );
    }

    #[test]
    fn native_lock_errors_preserve_transaction_disposition() {
        assert_eq!(
            classify_server_transaction_disposition(1205),
            MysqlTransactionDisposition::RollbackRequired
        );
        assert_eq!(
            classify_server_transaction_disposition(1213),
            MysqlTransactionDisposition::TransactionEnded
        );
        assert_eq!(
            classify_server_transaction_disposition(1045),
            MysqlTransactionDisposition::DestroyConnection
        );
    }
}
