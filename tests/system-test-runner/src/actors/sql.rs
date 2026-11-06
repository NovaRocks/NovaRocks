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

//! Ordinary SQL uses the shared typed client. Each actor owns its session;
//! raw responses and intentional slow reading belong to `trino_protocol`.

use anyhow::{Result, ensure};
use novarocks_test_trino_client::{ClientConfig, TrinoClient, TrinoError};
pub(crate) use novarocks_test_trino_client::{
    ClientSession, PendingStatement, StatementFailure, StatementResult, TrinoValue,
};
use std::time::Instant;

#[derive(Clone)]
pub(crate) struct SqlSession {
    client: TrinoClient,
    session: ClientSession,
    deadline: Instant,
}

impl SqlSession {
    pub(crate) fn new(user: &str, port: u16, deadline: Instant) -> Result<Self, StatementFailure> {
        Self::with_request_timeout_cap(user, port, deadline, None)
    }

    pub(crate) fn new_with_request_timeout_cap(
        user: &str,
        port: u16,
        deadline: Instant,
        cap: std::time::Duration,
    ) -> Result<Self, StatementFailure> {
        Self::with_request_timeout_cap(user, port, deadline, Some(cap))
    }

    fn with_request_timeout_cap(
        user: &str,
        port: u16,
        deadline: Instant,
        cap: Option<std::time::Duration>,
    ) -> Result<Self, StatementFailure> {
        let config = ClientConfig {
            host: "127.0.0.1".into(),
            port,
            user: user.into(),
            source: "novarocks-system-tests".into(),
        };
        let client = match cap {
            Some(cap) => TrinoClient::new_with_request_timeout_cap(config, cap)?,
            None => TrinoClient::new(config)?,
        };
        Ok(Self {
            client,
            // This is an explicit test input, also used by the SQL corpus.
            session: ClientSession::new("Asia/Shanghai"),
            deadline,
        })
    }

    /// A clone has an independent session value and the same absolute budget.
    pub(crate) fn session(&self) -> &ClientSession {
        &self.session
    }

    pub(crate) fn execute(
        &mut self,
        sql: impl AsRef<str>,
    ) -> Result<StatementResult, StatementFailure> {
        self.client
            .execute(&mut self.session, sql.as_ref(), self.deadline)
    }

    /// POST only; the caller can arm this exact query before the first GET.
    pub(crate) fn submit(
        &self,
        sql: impl AsRef<str>,
    ) -> Result<PendingStatement, StatementFailure> {
        self.client
            .submit(&self.session, sql.as_ref(), self.deadline)
    }

    /// Complete a submission from this actor and apply its successful effects.
    pub(crate) fn run_pending(
        &mut self,
        pending: PendingStatement,
    ) -> Result<StatementResult, StatementFailure> {
        let (result, effects) = pending.run(self.deadline)?;
        self.session.apply(&effects);
        Ok(result)
    }
}

fn integer_type(result: &StatementResult, index: usize) -> Result<()> {
    ensure!(
        result.is_result_set(),
        "expected a query result set, got command completion {result:?}"
    );
    let column = result
        .columns
        .get(index)
        .ok_or_else(|| anyhow::anyhow!("missing integer result column {index}"))?;
    ensure!(
        matches!(
            column.type_signature.raw_type.as_str(),
            "tinyint" | "smallint" | "integer" | "bigint"
        ) && column.type_signature.arguments.is_empty(),
        "expected an integer column, got {column:?}"
    );
    Ok(())
}

pub(crate) fn single_integer(result: &StatementResult) -> Result<i64> {
    ensure!(
        result.columns.len() == 1 && result.rows.len() == 1,
        "expected exactly one integer column and row, got {result:?}"
    );
    integer_type(result, 0)?;
    match result.rows[0].as_slice() {
        [TrinoValue::Integer(value)] => Ok(*value),
        row => anyhow::bail!("expected a non-null integer cell, got {row:?}"),
    }
}

pub(crate) fn integer_column(result: &StatementResult) -> Result<Vec<i64>> {
    ensure!(
        result.columns.len() == 1,
        "expected exactly one integer column, got {result:?}"
    );
    integer_type(result, 0)?;
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Integer(value)] => Ok(*value),
            row => anyhow::bail!("expected a non-null integer cell, got {row:?}"),
        })
        .collect()
}

pub(crate) fn expect_update_count(result: &StatementResult, expected: u64) -> Result<()> {
    ensure!(
        result.is_completion() && result.update_count == Some(expected),
        "expected command updateCount={expected}, got {result:?}"
    );
    let expected_cell = i64::try_from(expected)?;
    ensure!(
        result.columns.len() == 1
            && result.columns[0].name == "rows"
            && result.columns[0].type_name == "bigint"
            && result.columns[0].type_signature.raw_type == "bigint"
            && result.columns[0].type_signature.arguments.is_empty()
            && result.rows == vec![vec![TrinoValue::Integer(expected_cell)]],
        "expected one rows bigint completion cell {expected}, got {result:?}"
    );
    Ok(())
}

/// HTTP, transport and local deadline failures cannot satisfy a SQL oracle.
pub(crate) fn expect_error<'a>(
    failure: &'a StatementFailure,
    code: i64,
    name: &str,
) -> Result<&'a TrinoError> {
    let StatementFailure::Trino(error) = failure else {
        anyhow::bail!("expected SQL error {code} {name}, got {failure}");
    };
    ensure!(
        error.error_code == code && error.error_name == name,
        "expected SQL error {code} {name}, got {failure}"
    );
    Ok(error)
}

#[cfg(test)]
fn expect_sql_code(
    failure: &StatementFailure,
    code: i64,
    name: &str,
    sql_code: &str,
) -> Result<()> {
    let error = expect_error(failure, code, name)?;
    ensure!(
        error.message.starts_with(&format!("[{sql_code}] ")),
        "expected original NovaRocks error [{sql_code}], got {failure}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_test_trino_client::{Column, TypeSignature};

    fn result(value: TrinoValue, ty: &str) -> StatementResult {
        StatementResult {
            columns: vec![Column {
                name: "value".into(),
                type_name: ty.into(),
                type_signature: TypeSignature::simple(ty),
            }],
            rows: vec![vec![value]],
            query_id: "20261029_120000_00001_abcde".into(),
            pages: 2,
            update_type: None,
            update_count: None,
        }
    }

    #[test]
    fn integer_oracles_preserve_type_null_cardinality_and_order() {
        let mut values = result(TrinoValue::Integer(4), "integer");
        assert_eq!(single_integer(&values).unwrap(), 4);
        values.rows.push(vec![TrinoValue::Integer(-2)]);
        assert_eq!(integer_column(&values).unwrap(), vec![4, -2]);
        assert!(single_integer(&values).is_err());
        assert!(single_integer(&result(TrinoValue::Text("4".into()), "varchar")).is_err());
        assert!(single_integer(&result(TrinoValue::Null, "bigint")).is_err());
        assert!(single_integer(&result(TrinoValue::Integer(4), "varchar")).is_err());
    }

    #[test]
    fn command_count_is_never_inferred_from_a_result_cell() {
        let mut value = result(TrinoValue::Integer(0), "bigint");
        assert!(expect_update_count(&value, 0).is_err());
        value.update_type = Some("INSERT".into());
        assert!(expect_update_count(&value, 0).is_err());
        value.update_count = Some(0);
        value.columns[0].name = "rows".into();
        expect_update_count(&value, 0).unwrap();
        assert!(expect_update_count(&value, 1).is_err());
    }

    #[test]
    fn only_the_original_typed_sql_error_satisfies_a_failure_oracle() {
        let wire = r#"{"message":"[query.execution.cancelled] stopped","errorCode":3,"errorName":"USER_CANCELED","errorType":"USER_ERROR"}"#;
        let failure = StatementFailure::Trino(serde_json::from_str(wire).unwrap());
        expect_sql_code(&failure, 3, "USER_CANCELED", "query.execution.cancelled").unwrap();
        assert!(expect_error(&failure, 38, "ADMINISTRATIVELY_KILLED").is_err());
        let mut mentioned: serde_json::Value = serde_json::from_str(wire).unwrap();
        mentioned["message"] = "detail mentioning [query.execution.cancelled] stopped".into();
        assert!(
            expect_sql_code(
                &StatementFailure::Trino(serde_json::from_value(mentioned).unwrap()),
                3,
                "USER_CANCELED",
                "query.execution.cancelled"
            )
            .is_err()
        );
        for local in [
            StatementFailure::Http {
                status: 500,
                body: wire.into(),
            },
            StatementFailure::Transport(wire.into()),
            StatementFailure::Deadline,
        ] {
            assert!(expect_error(&local, 3, "USER_CANCELED").is_err());
        }
    }
}
