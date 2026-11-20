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

use super::*;
use std::{sync::Arc, time::Duration};

use arrow::{
    array::{
        Array, ArrayData, BooleanArray, Int32Array, Int64Array, ListBuilder, StringArray,
        StringBuilder,
    },
    datatypes::{DataType, Field},
};
use novarocks_functions::{ConstantPool, ConstantValue};
use novarocks_parser::{
    ast::{Expr, LiteralKind, TrinoStatement},
    parser::parse_statement,
};
use novarocks_query_application::sql::property_evaluation::DdlPropertyConstantError;
use novarocks_spi::connector::{ConnectorColumnPath, ConnectorProviderId};
use novarocks_sql::compiler::{RequiredConstantTarget, SqlCompileControl};
use novarocks_type_contract::{CompilePhase, FunctionValueType};

struct LiteralValues;
impl DdlPropertyConstantPort for LiteralValues {
    fn evaluate(
        &mut self,
        expression: &Expr,
        target: RequiredConstantTarget,
    ) -> Result<ConstantValue, DdlPropertyConstantError> {
        let data: ArrayData = match expression {
            Expr::Literal(literal) => match (target, &literal.kind) {
                (RequiredConstantTarget::Varchar, LiteralKind::String(text)) => {
                    StringArray::from(vec![text.as_str()]).to_data()
                }
                (RequiredConstantTarget::Integer, LiteralKind::Long { parsed_value, .. }) => {
                    Int32Array::from(vec![i32::try_from(*parsed_value).unwrap()]).to_data()
                }
                (RequiredConstantTarget::Bigint, LiteralKind::Long { parsed_value, .. }) => {
                    Int64Array::from(vec![*parsed_value]).to_data()
                }
                (RequiredConstantTarget::Boolean, LiteralKind::Boolean(value)) => {
                    BooleanArray::from(vec![*value]).to_data()
                }
                _ => panic!("unexpected required-constant target"),
            },
            Expr::Array(array) if target == RequiredConstantTarget::ArrayVarchar => {
                let mut builder = ListBuilder::new(StringBuilder::new());
                for element in &array.values {
                    let Expr::Literal(literal) = element else {
                        panic!("expected literal array element");
                    };
                    match &literal.kind {
                        LiteralKind::String(value) => builder.values().append_value(value),
                        LiteralKind::Null => builder.values().append_null(),
                        _ => panic!("expected optional string literal"),
                    }
                }
                builder.append(true);
                builder.finish().to_data()
            }
            _ => panic!("the test port only supplies checked literal values"),
        };
        let data_type: DataType = data.data_type().clone();
        let pool = ConstantPool::try_new(
            Arc::new(Field::new("argument", data_type.clone(), false)),
            FunctionValueType::new(data_type, false),
            data,
            crate::application::test_constant_policy(),
            CompilePhase::Validate,
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
        Ok(pool.value(0).unwrap())
    }
}

fn project_at(
    source: &str,
    statement_started_at: SystemTime,
) -> Result<Procedure, QueryServiceError> {
    let TrinoStatement::TableExecute(statement) = parse_statement(source).unwrap() else {
        panic!("expected a table procedure");
    };
    let control = SqlCompileControl::unbounded();
    let bound = bind_trino_table_execute(
        source,
        &statement,
        &ConnectorProviderId::parse("iceberg").unwrap(),
        "ice",
        &mut LiteralValues,
        &mut OriginalTimestamp { source },
        &control,
    )?;
    project_bound(
        source,
        statement.span,
        bound,
        statement_started_at,
        &control,
        novarocks_connector_iceberg::table_properties::extra_property_unsets,
    )
}

fn project(source: &str) -> Result<Procedure, QueryServiceError> {
    project_at(
        source,
        SystemTime::UNIX_EPOCH + Duration::from_millis(800_000_123),
    )
}

#[cfg(novarocks_dev_controls)]
#[test]
fn development_equality_procedure_keeps_original_rows_for_the_typed_sink() {
    use crate::query_execution::dml::delete::equality::{
        PreparedEqualityDelete, tests::assert_prepared_equality_delete_sink,
    };

    let sql = "ALTER TABLE t EXECUTE add_equality_delete(columns => ARRAY['key'], rows => ARRAY[ROW(9007199254740993.125), ROW(-9007199254740993.125)])";
    let TrinoStatement::TableExecute(statement) = parse_statement(sql).unwrap() else {
        panic!("expected the equality-delete table procedure");
    };
    let statement = Arc::new(statement);
    let control = SqlCompileControl::unbounded();
    let bind = |provider: &ConnectorProviderId| {
        bind_trino_table_execute(
            sql,
            &statement,
            provider,
            "ice",
            &mut LiteralValues,
            &mut OriginalTimestamp { source: sql },
            &control,
        )
    };
    let bound = bind(&ConnectorProviderId::parse("iceberg").unwrap()).unwrap();
    let prepared = PreparedEqualityDelete::from_bound(sql, &statement, bound, &control).unwrap();
    assert_prepared_equality_delete_sink(sql, &statement, &prepared);

    let error = bind(&ConnectorProviderId::parse("paimon").unwrap()).unwrap_err();
    assert_eq!(
        error.user_error().unwrap().code().as_str(),
        "sql.admit.table_procedure_not_found",
    );
}

#[test]
fn schema_procedures_preserve_nested_paths_and_reference_leaf() {
    let path = ConnectorColumnPath {
        segments: vec![Arc::from("address"), Arc::from("zip")],
    };
    let schema = |change| Procedure::CatalogMutation(IcebergTableMutation::Schema(change));
    assert_eq!(
        project("ALTER TABLE lake.ns.t EXECUTE move_column(column => 'Address.Zip', position => 'BEFORE', reference => 'Address.Street')").unwrap(),
        schema(ConnectorSchemaChange::ReorderColumn {
            path: path.clone(),
            position: ConnectorColumnPosition::Before {
                column: Arc::from("street"),
            },
        }),
    );
    assert_eq!(
        project("ALTER TABLE lake.ns.t EXECUTE move_column('address.zip', 'FIRST')").unwrap(),
        schema(ConnectorSchemaChange::ReorderColumn {
            path: path.clone(),
            position: ConnectorColumnPosition::First,
        }),
    );
    assert_eq!(
        project("ALTER TABLE lake.ns.t EXECUTE set_column_not_null('address.zip')").unwrap(),
        schema(ConnectorSchemaChange::SetColumnNullability {
            path,
            nullable: false,
        }),
    );
}

#[test]
fn schema_procedures_reject_incomplete_paths_and_reference_shapes() {
    for (source, message) in [
        (
            "ALTER TABLE t EXECUTE move_column('a', 'AFTER')",
            "require a reference column",
        ),
        (
            "ALTER TABLE t EXECUTE move_column('a', 'FIRST', 'b')",
            "does not take a reference",
        ),
        (
            "ALTER TABLE t EXECUTE set_column_not_null('a..b')",
            "empty segment",
        ),
    ] {
        let error = project(source).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }
}

#[test]
fn unset_property_procedure_keeps_provider_key_rules_and_default() {
    for (sql, if_exists) in [
        (
            "ALTER TABLE t EXECUTE unset_extra_properties(ARRAY['Custom.Key'])",
            false,
        ),
        (
            "ALTER TABLE t EXECUTE unset_extra_properties(keys => ARRAY['Custom.Key'], if_exists => true)",
            true,
        ),
    ] {
        assert_eq!(
            project(sql).unwrap(),
            Procedure::CatalogMutation(IcebergTableMutation::UnsetProperties(vec![
                ConnectorPropertyChange::Unset {
                    key: Arc::from("custom.key"),
                    if_exists
                },
            ]))
        );
    }
    for (sql, message) in [
        (
            "ALTER TABLE t EXECUTE unset_extra_properties(ARRAY[])",
            "must not be empty",
        ),
        (
            "ALTER TABLE t EXECUTE unset_extra_properties(ARRAY[NULL])",
            "must not contain NULL",
        ),
        (
            "ALTER TABLE t EXECUTE unset_extra_properties(ARRAY['custom.key', 'CUSTOM.KEY'])",
            "duplicate normalized extra property",
        ),
        (
            "ALTER TABLE t EXECUTE unset_extra_properties(ARRAY['gc.enabled'], true)",
            "has a dedicated declared property",
        ),
    ] {
        let error = project(sql).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
        let diagnostic = error.user_error().unwrap();
        assert_eq!(
            diagnostic.code().as_str(),
            "sql.admit.invalid_table_procedure_argument",
        );
        assert!(diagnostic.location().is_some());
    }
}

#[test]
fn typed_table_procedure_uses_original_absolute_time_conversion() {
    assert_eq!(
        project("ALTER TABLE lake.ns.t EXECUTE optimize_manifests").unwrap(),
        Procedure::RewriteManifests,
    );
    assert_eq!(
        project("ALTER TABLE lake.ns.t EXECUTE expire_snapshots(older_than => TIMESTAMP '1970-01-01 00:00:07')").unwrap(),
        Procedure::ExpireSnapshots { older_than_ms: Some(7000), retain_last: None },
    );
    assert_eq!(
        project("ALTER TABLE lake.ns.t EXECUTE remove_orphan_files(older_than => TIMESTAMP '1970-01-01 00:00:03')").unwrap(),
        Procedure::RemoveOrphanFiles(3000),
    );
    for (source, expected) in [
        (
            "ALTER TABLE t EXECUTE remove_orphan_files(older_than => 7001)",
            7001,
        ),
        (
            "ALTER TABLE t EXECUTE remove_orphan_files(older_than => '1970-01-01T01:00:07.123456+01:00')",
            7123,
        ),
        (
            "ALTER TABLE t EXECUTE remove_orphan_files(older_than => TIMESTAMP '1970-01-01T01:00:07.123456+01:00')",
            7123,
        ),
        (
            "ALTER TABLE t EXECUTE remove_orphan_files(older_than => TIMESTAMP '7001')",
            7001,
        ),
    ] {
        assert_eq!(
            project(source).unwrap(),
            Procedure::RemoveOrphanFiles(expected),
            "{source}"
        );
    }
}

#[test]
fn relative_procedures_use_bound_duration_and_one_supplied_statement_start() {
    for source in [
        "ALTER TABLE lake.ns.t EXECUTE expire_snapshots",
        "ALTER TABLE lake.ns.t EXECUTE expire_snapshots(retention_threshold => '7d')",
    ] {
        assert_eq!(
            project(source).unwrap(),
            Procedure::ExpireSnapshots {
                older_than_ms: Some(195_200_123),
                retain_last: None,
            }
        );
    }
    assert_eq!(
        project("ALTER TABLE lake.ns.t EXECUTE remove_orphan_files").unwrap(),
        Procedure::RemoveOrphanFiles(195_200_123),
    );
    assert_eq!(
        project(
            "ALTER TABLE lake.ns.t EXECUTE remove_orphan_files(retention_threshold => '0.5ms')"
        )
        .unwrap(),
        Procedure::RemoveOrphanFiles(800_000_122),
    );
    assert_eq!(
        project("ALTER TABLE lake.ns.t EXECUTE expire_snapshots(retention_threshold => '0d', retain_last => 2)").unwrap(),
        Procedure::ExpireSnapshots { older_than_ms: Some(800_000_123), retain_last: Some(2) },
    );
    assert_eq!(
        project("ALTER TABLE lake.ns.t EXECUTE expire_snapshots(retain_last => 2)").unwrap(),
        Procedure::ExpireSnapshots {
            older_than_ms: None,
            retain_last: Some(2)
        },
    );
}

#[test]
fn bad_retention_is_reported_at_its_argument_before_a_later_bad_retain_count() {
    let source = "ALTER TABLE t EXECUTE expire_snapshots(retention_threshold => 'one day', retain_last => 0)";
    let error = project(source).unwrap_err();
    let user = error.user_error().unwrap();
    assert_eq!(
        user.code().as_str(),
        "sql.admit.invalid_table_procedure_argument"
    );
    assert!(user.message().contains("retention_threshold duration"));
    let start = source.find("retention_threshold").unwrap();
    let end = source.find(',').unwrap();
    assert_eq!(
        user.location(),
        Some(&Span::new(start, end).to_user_error_location(source))
    );
}

#[test]
fn relative_procedures_keep_typed_argument_errors_and_original_cutoff_limits() {
    let start = SystemTime::UNIX_EPOCH + Duration::from_millis(7);
    assert_eq!(
        project_at(
            "ALTER TABLE t EXECUTE expire_snapshots(retention_threshold => '7ms')",
            start
        )
        .unwrap(),
        Procedure::ExpireSnapshots {
            older_than_ms: Some(0),
            retain_last: None
        },
    );
    for source in [
        "ALTER TABLE t EXECUTE remove_orphan_files(retention_threshold => '7ms')",
        "ALTER TABLE t EXECUTE expire_snapshots(retention_threshold => '8ms')",
        "ALTER TABLE t EXECUTE expire_snapshots(retention_threshold => 'one day')",
        "ALTER TABLE t EXECUTE remove_orphan_files(retention_threshold => '1d', older_than => TIMESTAMP '1970-01-01 00:00:07')",
    ] {
        let error = project_at(source, start).unwrap_err();
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.admit.invalid_table_procedure_argument",
            "{source}",
        );
    }
}
