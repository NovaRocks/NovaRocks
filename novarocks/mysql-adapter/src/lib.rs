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

//! MySQL protocol adaptation for Query Application contracts.

mod authentication;
mod connection_registry;
mod disconnect_watcher;
mod error_mapping;
mod governed_result_writer;
mod listener;
mod listener_settings;
mod query_application_shim;
mod result_encoding;
mod result_value;
mod result_writer;
mod row_encoding;
mod terminal;

use novarocks_query_application::session_error::QueryServiceErrorKind;
use opensrv_mysql::{ErrorKind, IntermediaryOptions};

pub use authentication::authenticate_empty_password;
pub use connection_registry::MysqlClientConnectionRegistry;
pub use disconnect_watcher::{ClientDisconnectWatcher, spawn_disconnect_watcher};
pub use error_mapping::error_kind_for_domain_code;
pub use governed_result_writer::{
    MysqlStatementWriteOutcome, write_governed_query_result, write_governed_query_result_one,
    write_query_result, write_query_result_one, write_streaming_query_result,
    write_streaming_query_result_one,
};
pub use listener::{
    serve_tcp_until_drain_then_shutdown, serve_tcp_until_shutdown,
    serve_tcp_until_shutdown_with_drain_timeout,
};
pub use listener_settings::{
    DEFAULT_MYSQL_USER, ResolvedMysqlListenerSettings, resolve_mysql_listener_settings,
};
pub use query_application_shim::{
    QUERY_APPLICATION_MYSQL_SESSION_DRAIN_TIMEOUT, QueryApplicationMysqlShim,
    serve_query_application_mysql_connection,
    serve_query_application_mysql_until_drain_then_shutdown,
    serve_query_application_mysql_until_shutdown,
};
pub use result_encoding::mysql_column_for_result_field;
pub use result_value::MysqlResultValue;
pub use result_writer::{
    MysqlBatchWriteError, MysqlResultFinishError, MysqlResultStartError, finish_result,
    finish_result_error, finish_result_one, finish_streaming_result, finish_streaming_result_one,
    mysql_columns_for_result_fields, start_cancellable_result, start_streaming_result,
    write_cancellable_batch, write_record_batches, write_record_batches_one, write_streaming_batch,
};
pub use row_encoding::{array_value_to_mysql_value, build_mysql_row};
pub use terminal::{
    mysql_error_kind, write_governed_init_error, write_governed_init_ok,
    write_governed_terminal_error, write_governed_terminal_ok, write_governed_terminal_ok_one,
    write_terminal_ok, write_terminal_ok_one,
};

/// `USE ...` must remain an ordinary COM_QUERY for typed SQL validation;
/// genuine COM_INIT_DB packets still use the protocol callback.
pub const MYSQL_INTERMEDIARY_OPTIONS: IntermediaryOptions = IntermediaryOptions {
    process_use_statement_on_query: true,
    reject_connection_on_dbname_absence: false,
};

pub fn normalize_init_database_schema(schema: &str) -> String {
    schema
        .split('.')
        .map(|part| part.trim_matches('`'))
        .collect::<Vec<_>>()
        .join(".")
}

pub fn error_kind_for_query_service_error(kind: QueryServiceErrorKind) -> ErrorKind {
    match kind {
        QueryServiceErrorKind::Parse => ErrorKind::ER_PARSE_ERROR,
        QueryServiceErrorKind::BadDatabase => ErrorKind::ER_BAD_DB_ERROR,
        QueryServiceErrorKind::Unsupported => ErrorKind::ER_NOT_SUPPORTED_YET,
        QueryServiceErrorKind::PermissionDenied => ErrorKind::ER_SPECIFIC_ACCESS_DENIED_ERROR,
        QueryServiceErrorKind::NoSuchSession => ErrorKind::ER_NO_SUCH_THREAD,
        QueryServiceErrorKind::Interrupted => ErrorKind::ER_QUERY_INTERRUPTED,
        QueryServiceErrorKind::Timeout => ErrorKind::ER_UNKNOWN_ERROR,
        QueryServiceErrorKind::InvalidValue => ErrorKind::ER_WRONG_VALUE,
        QueryServiceErrorKind::Unavailable | QueryServiceErrorKind::Internal => {
            ErrorKind::ER_UNKNOWN_ERROR
        }
        QueryServiceErrorKind::FrontendDraining => ErrorKind::ER_SERVER_SHUTDOWN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_service_error_mapping_uses_mysql_wire_kinds() {
        assert_eq!(
            error_kind_for_query_service_error(QueryServiceErrorKind::BadDatabase),
            ErrorKind::ER_BAD_DB_ERROR
        );
        assert_eq!(
            error_kind_for_query_service_error(QueryServiceErrorKind::Interrupted),
            ErrorKind::ER_QUERY_INTERRUPTED
        );
        assert_eq!(
            error_kind_for_query_service_error(QueryServiceErrorKind::Unavailable),
            ErrorKind::ER_UNKNOWN_ERROR
        );
        assert_eq!(
            error_kind_for_query_service_error(QueryServiceErrorKind::FrontendDraining),
            ErrorKind::ER_SERVER_SHUTDOWN
        );
    }

    #[test]
    fn init_database_normalization_stays_in_the_protocol_adapter() {
        assert_eq!(
            normalize_init_database_schema("`iceberg_cat`.`ssb`"),
            "iceberg_cat.ssb"
        );
        assert_eq!(
            normalize_init_database_schema("iceberg_cat.ssb"),
            "iceberg_cat.ssb"
        );
    }
}
