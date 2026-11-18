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

//! Temporary preparation from the original CREATE TABLE AST to evaluated facts.

use super::property_admit::DdlPropertyScope;
use super::property_evaluation::{
    DdlPropertyConstantPort, DdlPropertyEvaluationMode, evaluate_ddl_properties,
    prepare_legacy_create_table_properties,
};
use super::{lower_command_literal, lower_object_name, lower_sql_type};
use crate::session_error::{QueryServiceError, QueryServiceErrorKind};
use novarocks_parser::ast::{TablePartition, legacy::CreateTable};
use novarocks_spi::connector::{
    ConnectorCatalogMutationSession, ConnectorOperationControl, DdlPropertyChange,
};
use novarocks_sql::semantic::{
    IcebergPartitionFieldExpr,
    command::{CreateTableSqlCommand, TableColumnSqlCommand},
};
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use novarocks_types::schema::SqlType;

fn admission(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}
fn check(control: &dyn PureCompileControl) -> Result<(), QueryServiceError> {
    control
        .checkpoint(CompilePhase::Validate, 1)
        .map_err(QueryServiceError::from_compile_control)
}
fn provider(error: novarocks_spi::connector::ConnectorError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(
        error,
    ))
}

/// The original column/default author, invoked synchronously on this same
/// planning CPU. It may retain its prepared product columns for final request
/// construction; QA neither duplicates that algorithm nor fabricates values.
pub trait LegacyCreateTableColumnsPort {
    fn prepare_columns(
        &mut self,
        columns: &[TableColumnSqlCommand],
    ) -> Result<(), QueryServiceError>;
}

/// Prepare one real old-parser CREATE. The caller retains its admitted controls
/// and exact mutation generation. No parser AST crosses the returned command.
pub fn prepare_legacy_create_table(
    source: &str,
    statement: &CreateTable,
    mutation: &ConnectorCatalogMutationSession,
    connector_control: &dyn ConnectorOperationControl,
    port: &mut dyn DdlPropertyConstantPort,
    columns_port: &mut dyn LegacyCreateTableColumnsPort,
    control: &dyn PureCompileControl,
) -> Result<CreateTableSqlCommand, QueryServiceError> {
    check(control)?;
    // Keep the original QA order before the old FE shape admission.
    let partition = match &statement.partition {
        Some(TablePartition::Transform(partition)) => {
            let mut fields = Vec::with_capacity(partition.expressions.len());
            for field in &partition.expressions {
                check(control)?;
                fields.push(lower_table_partition_transform(field).map_err(admission)?);
            }
            Some((fields, partition.span))
        }
        None | Some(TablePartition::LegacyRange(_)) => None,
    };
    let mut columns = Vec::with_capacity(statement.columns.len());
    for column in &statement.columns {
        check(control)?;
        columns.push(lower_table_column(column).map_err(admission)?);
    }
    if let Some(key) = &statement.key {
        for key_column in &key.columns {
            check(control)?;
            if !columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(&key_column.value))
            {
                return Err(admission(format!(
                    "Iceberg key column `{}` does not exist",
                    key_column.value
                )));
            }
        }
    }
    if let Some(distribution) = &statement.distribution {
        for dist_column in &distribution.columns {
            check(control)?;
            if let Some(column) = columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&dist_column.value))
                && matches!(column.data_type, SqlType::Bitmap | SqlType::Hll)
            {
                return Err(admission(format!(
                    "BITMAP/HLL columns cannot be used as distribution key (column `{}` has type {:?})",
                    column.name, column.data_type
                )));
            }
        }
        distribution
            .buckets
            .map(|value| {
                u32::try_from(value).map_err(|_| admission("distribution bucket count exceeds u32"))
            })
            .transpose()?;
    }
    // The original literal admission preceded FE shape checks. Reuse its
    // actual leaf author, without creating Expr/Query nodes or a raw command.
    for property in &statement.properties {
        check(control)?;
        lower_command_literal(&property.key).map_err(admission)?;
        lower_command_literal(&property.value).map_err(admission)?;
    }
    if let Some(comment) = &statement.comment {
        check(control)?;
        lower_command_literal(comment).map_err(admission)?;
    }
    let name = lower_object_name(&statement.name);
    let like = statement.like.as_ref().map(lower_object_name);
    if like.is_some() {
        // The real old parser returns at LIKE: flags before it were ignored by
        // the old executor. Source-column loading remains the FE I/O owner's job.
        let declarations = mutation.table_property_declarations().map_err(provider)?;
        let evaluated = evaluate_ddl_properties(
            DdlPropertyScope::Table,
            DdlPropertyEvaluationMode::Create,
            source,
            &declarations,
            &[],
            port,
            control,
        )?;
        let properties = evaluated
            .into_properties()
            .into_iter()
            .map(|property| {
                let DdlPropertyChange::Set(value) = property.change else {
                    return Err(admission("Legacy LIKE defaults unexpectedly contain Reset"));
                };
                Ok((property.name, value))
            })
            .collect::<Result<Vec<_>, QueryServiceError>>()?;
        check(control)?;
        return Ok(CreateTableSqlCommand {
            if_not_exists: statement.if_not_exists,
            name,
            like,
            columns,
            comment: None,
            properties,
        });
    }
    if statement.temporary || statement.external {
        return Err(admission(
            "CREATE TABLE does not support TEMPORARY or EXTERNAL tables",
        ));
    }
    if let Some(engine) = &statement.engine
        && !engine.value.eq_ignore_ascii_case("iceberg")
    {
        return Err(admission(format!(
            "CREATE TABLE does not support ENGINE = {}",
            engine.value
        )));
    }
    if !statement.order_by.is_empty() {
        return Err(admission("CREATE TABLE does not support ORDER BY"));
    }
    if matches!(&statement.partition, Some(TablePartition::LegacyRange(_))) {
        return Err(admission(
            "CREATE TABLE does not support legacy RANGE partition definitions",
        ));
    }
    let partitioning = partition.as_ref().map(|(fields, span)| {
        (
            fields
                .iter()
                .map(super::connector_partition_transform)
                .collect::<Vec<_>>(),
            *span,
        )
    });
    let (properties, comment) = prepare_legacy_create_table_properties(
        source,
        statement,
        partitioning
            .as_ref()
            .map(|(fields, span)| (fields.as_slice(), *span)),
        mutation,
        connector_control,
        port,
        control,
        || columns_port.prepare_columns(&columns),
    )?;
    check(control)?;
    Ok(CreateTableSqlCommand {
        if_not_exists: statement.if_not_exists,
        name,
        like,
        columns,
        comment,
        properties,
    })
}

fn lower_table_column(
    column: &novarocks_parser::ast::ColumnDefinition,
) -> Result<TableColumnSqlCommand, String> {
    let data_type = lower_sql_type(&column.data_type)?;
    if let Some(aggregation) = &column.aggregation {
        match aggregation.value.to_ascii_lowercase().as_str() {
            "sum"
            | "min"
            | "max"
            | "replace"
            | "replace_if_not_null"
            | "bitmap_union"
            | "hll_union" => {}
            other => return Err(format!("unsupported column aggregation `{other}`")),
        }
    }
    Ok(TableColumnSqlCommand {
        name: column.name.value.clone(),
        data_type,
        nullable: column.nullable,
        default: column
            .default
            .as_ref()
            .map(lower_command_literal)
            .transpose()?,
        comment: column
            .comment
            .as_ref()
            .map(lower_command_literal)
            .transpose()?,
    })
}

fn lower_table_partition_transform(
    transform: &novarocks_parser::ast::PartitionTransform,
) -> Result<IcebergPartitionFieldExpr, String> {
    use novarocks_parser::ast::PartitionTransform;

    let column = |value: &novarocks_parser::ast::Ident| value.value.clone();
    Ok(match transform {
        PartitionTransform::Identity { column: value, .. } => IcebergPartitionFieldExpr::Identity {
            column: column(value),
        },
        PartitionTransform::Year { column: value, .. } => IcebergPartitionFieldExpr::Year {
            column: column(value),
        },
        PartitionTransform::Month { column: value, .. } => IcebergPartitionFieldExpr::Month {
            column: column(value),
        },
        PartitionTransform::Day { column: value, .. } => IcebergPartitionFieldExpr::Day {
            column: column(value),
        },
        PartitionTransform::Hour { column: value, .. } => IcebergPartitionFieldExpr::Hour {
            column: column(value),
        },
        PartitionTransform::Void { column: value, .. } => IcebergPartitionFieldExpr::Void {
            column: column(value),
        },
        PartitionTransform::Bucket {
            buckets,
            column: value,
            ..
        } => IcebergPartitionFieldExpr::Bucket {
            column: column(value),
            num_buckets: u32::try_from(*buckets)
                .map_err(|_| "partition bucket count exceeds u32".to_string())?,
        },
        PartitionTransform::Truncate {
            width,
            column: value,
            ..
        } => IcebergPartitionFieldExpr::Truncate {
            column: column(value),
            width: u32::try_from(*width)
                .map_err(|_| "partition truncate width exceeds u32".to_string())?,
        },
    })
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use crate::sql::property_evaluation::DdlPropertyConstantError;
    use novarocks_constant_contract::ConstantValue;
    use novarocks_spi::connector as spi;
    use novarocks_sql::compiler::RequiredConstantTarget;
    use novarocks_type_contract::{CompileControlError, CompilePhase};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Default)]
    pub struct Control {
        pub stop: AtomicBool,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            if self.stop.load(Ordering::SeqCst) {
                Err(CompileControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    impl spi::ConnectorOperationControl for Control {
        fn check_active(&self) -> Result<(), spi::ConnectorError> {
            if self.stop.load(Ordering::SeqCst) {
                Err(spi::ConnectorError::new(
                    spi::ConnectorErrorKind::Cancelled,
                    "original test control",
                ))
            } else {
                Ok(())
            }
        }
    }
    struct Port;
    impl DdlPropertyConstantPort for Port {
        fn evaluate(
            &mut self,
            _: &novarocks_parser::ast::Expr,
            _: RequiredConstantTarget,
        ) -> Result<ConstantValue, DdlPropertyConstantError> {
            panic!(
                "legacy prepared facts and declaration defaults do not invoke expression evaluation"
            )
        }
    }
    struct Preparation {
        descriptor: spi::ConnectorInstanceDescriptor,
        expected_raw: Option<Vec<(Arc<str>, Arc<str>)>>,
        response: Mutex<
            Option<
                Result<
                    Vec<spi::LegacyPreparedTableProperty>,
                    spi::LegacyTablePropertyPreparationFailure,
                >,
            >,
        >,
        partition: Option<(Vec<spi::ConnectorPartitionTransform>, spi::DdlPropertyValue)>,
        raw_calls: Arc<AtomicUsize>,
    }
    impl spi::ConnectorCatalogMutation for Preparation {
        fn descriptor(&self) -> &spi::ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> spi::ProviderBindingEpoch {
            spi::ProviderBindingEpoch::from_bytes([17; 16])
        }
        fn table_property_declarations(
            &self,
        ) -> Result<Vec<spi::DdlPropertyDeclaration>, spi::ConnectorError> {
            use spi::{DdlPropertyType as T, DdlPropertyValue as V};
            Ok(vec![
                spi::DdlPropertyDeclaration::new(
                    "format",
                    T::Varchar,
                    V::Varchar("PARQUET".into()),
                    false,
                )
                .unwrap(),
                spi::DdlPropertyDeclaration::new(
                    "format_version",
                    T::Integer,
                    V::Integer(2),
                    false,
                )
                .unwrap(),
                spi::DdlPropertyDeclaration::new(
                    "partitioning",
                    T::ArrayVarchar,
                    V::ArrayVarchar(vec![]),
                    false,
                )
                .unwrap(),
            ])
        }
        fn check_table_property_name(&self, _: &str) -> Result<(), spi::ConnectorError> {
            Ok(())
        }
        fn prepare_legacy_create_table_property_inputs(
            &self,
            input: &[(Arc<str>, Arc<str>)],
            control: &dyn spi::ConnectorOperationControl,
        ) -> Result<Vec<spi::LegacyPreparedTableProperty>, spi::LegacyTablePropertyPreparationFailure>
        {
            self.raw_calls.fetch_add(1, Ordering::SeqCst);
            control
                .check_active()
                .map_err(spi::LegacyTablePropertyPreparationFailure::whole)?;
            assert_eq!(
                input,
                self.expected_raw
                    .as_ref()
                    .expect("LIKE must not prepare ignored legacy raw fields")
                    .as_slice()
            );
            self.response
                .lock()
                .unwrap()
                .take()
                .expect("one actual provider preparation")
        }
        fn prepare_legacy_create_table_partitioning_property(
            &self,
            fields: &[spi::ConnectorPartitionTransform],
            control: &dyn spi::ConnectorOperationControl,
        ) -> Result<spi::DdlPropertyValue, spi::ConnectorError> {
            control.check_active()?;
            let (expected, value) = self
                .partition
                .as_ref()
                .expect("only actual partition input invokes inverse");
            assert_eq!(fields, expected.as_slice());
            Ok(value.clone())
        }
        fn execute(
            &self,
            _: spi::ConnectorCatalogMutationRequest,
        ) -> Result<
            spi::ExternalMutationOutcome<spi::ConnectorCatalogMutationReceipt>,
            spi::ConnectorError,
        > {
            panic!("pure preparation cannot dispatch")
        }
        fn reconcile(
            &self,
            _: spi::ConnectorCatalogMutationReconcileRequest,
        ) -> Result<
            spi::ExternalMutationOutcome<spi::ConnectorCatalogMutationReceipt>,
            spi::ConnectorError,
        > {
            panic!("pure preparation cannot reconcile")
        }
    }
    // This fixture returns independently authored neutral facts. It verifies
    // real input slots and type/default projection, not Iceberg reader parsing.
    pub fn fixture_prepare(
        source: &str,
        expected_raw: Option<Vec<(Arc<str>, Arc<str>)>>,
        response: Result<
            Vec<spi::LegacyPreparedTableProperty>,
            spi::LegacyTablePropertyPreparationFailure,
        >,
        partition: Option<(Vec<spi::ConnectorPartitionTransform>, spi::DdlPropertyValue)>,
        control: &Control,
    ) -> Result<CreateTableSqlCommand, QueryServiceError> {
        struct Columns;
        impl LegacyCreateTableColumnsPort for Columns {
            fn prepare_columns(
                &mut self,
                _: &[TableColumnSqlCommand],
            ) -> Result<(), QueryServiceError> {
                Ok(())
            }
        }
        fixture_prepare_with_columns(
            source,
            expected_raw,
            response,
            partition,
            control,
            &mut Columns,
            Arc::new(AtomicUsize::new(0)),
        )
    }

    pub fn fixture_prepare_with_columns(
        source: &str,
        expected_raw: Option<Vec<(Arc<str>, Arc<str>)>>,
        response: Result<
            Vec<spi::LegacyPreparedTableProperty>,
            spi::LegacyTablePropertyPreparationFailure,
        >,
        partition: Option<(Vec<spi::ConnectorPartitionTransform>, spi::DdlPropertyValue)>,
        control: &Control,
        columns_port: &mut dyn LegacyCreateTableColumnsPort,
        raw_calls: Arc<AtomicUsize>,
    ) -> Result<CreateTableSqlCommand, QueryServiceError> {
        let parsed =
            super::super::parse_single_statement(source).expect("actual old CREATE syntax");
        let novarocks_parser::ast::Statement::Table(novarocks_parser::ast::TableStatement::Create(
            statement,
        )) = parsed
        else {
            panic!("actual table statement")
        };
        let descriptor = spi::ConnectorInstanceDescriptor {
            provider_id: spi::ConnectorProviderId::parse("fixture").unwrap(),
            instance_id: spi::ConnectorInstanceId::parse("ice").unwrap(),
        };
        let epoch = spi::ProviderBindingEpoch::from_bytes([17; 16]);
        let mutation = spi::ConnectorCatalogMutationSession::new(
            descriptor.clone(),
            spi::ConnectorControlRuntimeId::new(),
            epoch,
            Arc::new(Preparation {
                descriptor,
                expected_raw,
                response: Mutex::new(Some(response)),
                partition,
                raw_calls,
            }),
            || {},
        )
        .unwrap();
        prepare_legacy_create_table(
            source,
            &statement,
            &mutation,
            control,
            &mut Port,
            columns_port,
            control,
        )
    }
    pub fn simple(
        source: &str,
        version: i32,
        comment: &str,
        partition: Option<(Vec<spi::ConnectorPartitionTransform>, spi::DdlPropertyValue)>,
    ) -> Result<CreateTableSqlCommand, QueryServiceError> {
        fixture_prepare(
            source,
            Some(vec![
                ("format-version".into(), version.to_string().into()),
                ("comment".into(), comment.into()),
            ]),
            Ok(vec![
                spi::LegacyPreparedTableProperty {
                    input_index: 0,
                    value: spi::LegacyPreparedTablePropertyValue::Declared {
                        name: "format_version",
                        value: spi::DdlPropertyValue::Integer(version),
                    },
                },
                spi::LegacyPreparedTableProperty {
                    input_index: 1,
                    value: spi::LegacyPreparedTablePropertyValue::Extra {
                        key: "comment".into(),
                        value: comment.into(),
                    },
                },
            ]),
            partition,
            &Control::default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{test_support::*, *};
    use novarocks_spi::connector as spi;
    use std::sync::Arc;
    #[test]
    fn columns_stage_preserves_render_error_priority_and_original_borrow_before_provider() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Columns {
            calls: usize,
            pointer: Option<usize>,
            refusal: Option<QueryServiceError>,
        }
        impl LegacyCreateTableColumnsPort for Columns {
            fn prepare_columns(
                &mut self,
                columns: &[TableColumnSqlCommand],
            ) -> Result<(), QueryServiceError> {
                self.calls += 1;
                self.pointer = Some(columns.as_ptr() as usize);
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "id");
                assert_eq!(
                    columns[0].default,
                    Some(novarocks_sql::semantic::command::CommandLiteral::Number(
                        "7".into()
                    ))
                );
                if let Some(error) = self.refusal.take() {
                    return Err(error);
                }
                Ok(())
            }
        }
        let raw = Arc::new(AtomicUsize::new(0));
        let mut columns = Columns {
            calls: 0,
            pointer: None,
            refusal: None,
        };
        let error = fixture_prepare_with_columns(
            "CREATE TABLE ice.db.t (id INT DEFAULT 7) COMMENT NULL",
            None,
            Ok(vec![]),
            None,
            &Control::default(),
            &mut columns,
            raw.clone(),
        )
        .unwrap_err();
        assert_eq!(
            error.message(),
            "table properties do not support NULL values"
        );
        assert_eq!(columns.calls, 0);
        assert_eq!(raw.load(Ordering::SeqCst), 0);

        columns.refusal = Some(QueryServiceError::new(
            QueryServiceErrorKind::Unsupported,
            "original column-stage refusal",
        ));
        let error = fixture_prepare_with_columns(
            "CREATE TABLE ice.db.t (id INT DEFAULT 7) COMMENT 'original'",
            None,
            Ok(vec![]),
            None,
            &Control::default(),
            &mut columns,
            raw.clone(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), QueryServiceErrorKind::Unsupported);
        assert_eq!(error.message(), "original column-stage refusal");
        assert_eq!(columns.calls, 1);
        assert_eq!(raw.load(Ordering::SeqCst), 0);

        let mut columns = Columns {
            calls: 0,
            pointer: None,
            refusal: None,
        };
        let command = fixture_prepare_with_columns(
            "CREATE TABLE ice.db.t (id INT DEFAULT 7) COMMENT 'original'",
            Some(vec![("comment".into(), "original".into())]),
            Ok(vec![spi::LegacyPreparedTableProperty {
                input_index: 0,
                value: spi::LegacyPreparedTablePropertyValue::Extra {
                    key: "comment".into(),
                    value: "original".into(),
                },
            }]),
            None,
            &Control::default(),
            &mut columns,
            raw.clone(),
        )
        .unwrap();
        assert_eq!(columns.calls, 1);
        assert_eq!(raw.load(Ordering::SeqCst), 1);
        assert_eq!(columns.pointer, Some(command.columns.as_ptr() as usize));
        assert_eq!(command.comment, Some("original".into()));
    }

    #[test]
    fn like_preserves_real_early_branch_and_uses_declaration_create_defaults() {
        struct OrdinaryColumns;
        impl LegacyCreateTableColumnsPort for OrdinaryColumns {
            fn prepare_columns(
                &mut self,
                _: &[TableColumnSqlCommand],
            ) -> Result<(), QueryServiceError> {
                panic!("LIKE cannot enter the ordinary column/default stage")
            }
        }
        use std::sync::atomic::{AtomicUsize, Ordering};
        for source in [
            "CREATE TEMPORARY TABLE ice.db.copy LIKE ice.db.source_table",
            "CREATE EXTERNAL TABLE ice.db.copy LIKE ice.db.source_table",
        ] {
            let raw_calls = Arc::new(AtomicUsize::new(0));
            let command = fixture_prepare_with_columns(
                source,
                None,
                Ok(vec![]),
                None,
                &Control::default(),
                &mut OrdinaryColumns,
                raw_calls.clone(),
            )
            .unwrap();
            assert_eq!(raw_calls.load(Ordering::SeqCst), 0);
            assert_eq!(command.name.parts, ["ice", "db", "copy"]);
            assert_eq!(command.like.unwrap().parts, ["ice", "db", "source_table"]);
            assert!(command.columns.is_empty());
            assert_eq!(command.comment, None);
            assert_eq!(
                command.properties,
                vec![
                    (
                        "format".into(),
                        spi::DdlPropertyValue::Varchar("PARQUET".into())
                    ),
                    ("format_version".into(), spi::DdlPropertyValue::Integer(2)),
                    (
                        "partitioning".into(),
                        spi::DdlPropertyValue::ArrayVarchar(vec![])
                    ),
                ]
            );
        }
    }
    #[test]
    fn real_comment_slot_is_consumed_once_and_bad_provider_custody_is_internal() {
        let source = "CREATE TABLE ice.db.t (id INT) COMMENT 'original'";
        let expected = Some(vec![(Arc::from("comment"), Arc::from("original"))]);
        let good = spi::LegacyPreparedTableProperty {
            input_index: 0,
            value: spi::LegacyPreparedTablePropertyValue::Extra {
                key: "comment".into(),
                value: "original".into(),
            },
        };
        let command = fixture_prepare(
            source,
            expected.clone(),
            Ok(vec![good]),
            None,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(command.comment, Some("original".into()));
        assert!(
            command
                .properties
                .iter()
                .all(|(name, _)| name != "extra_properties")
        );
        let bad = spi::LegacyPreparedTableProperty {
            input_index: 0,
            value: spi::LegacyPreparedTablePropertyValue::Extra {
                key: "comment".into(),
                value: "foreign".into(),
            },
        };
        let error = fixture_prepare(
            source,
            expected.clone(),
            Ok(vec![bad]),
            None,
            &Control::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), QueryServiceErrorKind::Internal);
        assert!(error.message().contains("changed its original slot"));
        let error =
            fixture_prepare(source, expected, Ok(vec![]), None, &Control::default()).unwrap_err();
        assert_eq!(error.kind(), QueryServiceErrorKind::Internal);
        assert!(error.message().contains("omitted an original input"));
    }
}
