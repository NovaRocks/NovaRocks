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

//! The Partitions relation over the same frozen Files observation.

use crate::iceberg::spec::{PrimitiveType, TableMetadata, Transform, Type};
use crate::typed_read::system_page_source::{metadata_row_value, value_for_primitive};
use novarocks_connector_contract::{ConnectorReadExecutionSchema, PureProviderCompileError};
use novarocks_parser::{SYNTHETIC, ast, build, printer};
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use novarocks_spi::connector::{
    ConnectorDerivedView, ConnectorError, ConnectorErrorKind, ConnectorViewColumn,
    ConnectorViewDefinition, ConnectorViewDialect,
};
use std::{alloc::Layout, sync::Arc};

type Failure = PureProviderCompileError<ConnectorError>;

pub(crate) fn definition(
    catalog: &str,
    namespace: &str,
    table: &str,
    metadata: &TableMetadata,
    files: &ConnectorReadExecutionSchema,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Arc<ConnectorDerivedView>, Failure> {
    let partition = files
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == "partition");
    let schema = metadata.current_schema();
    let mut fields = creation.reserve(schema.as_struct().fields().len())?;
    let spec = metadata.default_partition_spec();
    for field in schema.as_struct().fields() {
        creation.work(1)?;
        let Type::Primitive(primitive) = field.field_type.as_ref() else {
            continue;
        };
        let mut identity_source = false;
        for partition_field in spec.fields() {
            creation.work(1)?;
            if partition_field.source_id == field.id
                && partition_field.transform == Transform::Identity
            {
                identity_source = true;
                break;
            }
        }
        if !identity_source {
            fields.push((field, primitive));
        }
    }

    let width = 3 + usize::from(partition.is_some()) + usize::from(!fields.is_empty());
    let mut projection = creation.reserve(width)?;
    let mut columns = creation.reserve(width)?;
    if let Some(ordinal) = partition {
        let binding = files
            .binding(ordinal)
            .ok_or_else(|| invalid("Files partition binding is missing"))?;
        columns.push(ConnectorViewColumn {
            name: shared_text("partition", creation)?,
            value_type: binding.clone_observed(|| creation.work(1))?,
        });
        projection.push(build::item(column("partition"), None));
    }
    for (name, expression, nullable) in [
        (
            "record_count",
            call("sum", vec![column("record_count")]),
            true,
        ),
        ("file_count", build::count_star(), false),
        (
            "total_size",
            call("sum", vec![column("file_size_in_bytes")]),
            true,
        ),
    ] {
        projection.push(build::item(expression, Some(build::ident(name, false))));
        columns.push(ConnectorViewColumn {
            name: shared_text(name, creation)?,
            value_type: value_for_primitive(&PrimitiveType::Long, nullable, creation)?,
        });
    }
    if !fields.is_empty() {
        let mut values = creation.reserve(fields.len())?;
        let mut types = creation.reserve(fields.len())?;
        let mut declarations = creation.reserve(fields.len())?;
        for (field, primitive) in fields {
            creation.work(field.name.len())?;
            let field_name = &field.name;
            let field_type = metric_syntax(primitive);
            values.push(metric(field.id, field_type.clone())?);
            types.push(named_type(field_name, field_type));
            let mut metric_fields = creation.reserve(4)?;
            for (name, ty) in [
                ("min", primitive),
                ("max", primitive),
                ("null_count", &PrimitiveType::Long),
                ("nan_count", &PrimitiveType::Long),
            ] {
                metric_fields.push((
                    creation.text(name)?,
                    value_for_primitive(ty, true, creation)?,
                ));
            }
            declarations.push((
                creation.text(field_name)?,
                metadata_row_value(metric_fields, true, creation)?,
            ));
        }
        projection.push(build::item(
            build::cast(row(values), build::row_type(types)),
            Some(build::ident("data", false)),
        ));
        columns.push(ConnectorViewColumn {
            name: shared_text("data", creation)?,
            value_type: metadata_row_value(declarations, false, creation)?,
        });
    }

    let files = build::object_name(vec![
        build::ident(catalog, true),
        build::ident(namespace, true),
        build::ident(format!("{table}$files"), true),
    ])
    .map_err(|error| invalid(error.to_string()))?;
    let mut select = build::select(
        projection,
        Some(build::table(files, None, None)),
        Some(build::comparison(
            column("content"),
            ast::ComparisonOperator::Equal,
            build::integer(0),
        )),
        partition.and_then(|_| build::group_by(vec![column("partition")])),
    )
    .map_err(|error| invalid(error.to_string()))?;
    if partition.is_none() {
        select.having = Some(build::comparison(
            build::count_star(),
            ast::ComparisonOperator::GreaterThan,
            build::integer(0),
        ));
    }
    creation.work(1)?;
    let raw_sql = printer::print_query(&build::query(ast::SetExpr::Select(ast::AstBox::new(
        select,
    ))))
    .map_err(|error| invalid(error.to_string()))?;
    let view = ConnectorDerivedView {
        name: shared_text(&format!("{table}$partitions"), creation)?,
        definition: ConnectorViewDefinition {
            dialect: ConnectorViewDialect::Trino,
            raw_sql: shared_text(&raw_sql, creation)?,
            default_catalog: Some(shared_text(catalog, creation)?),
            default_namespace: shared_text(namespace, creation)?,
            columns: creation.arc_slice(columns)?,
        },
    };
    creation.arc(view)
}

fn shared_text(
    source: &str,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Arc<str>, Failure> {
    let payload = Layout::array::<u8>(source.len()).map_err(|_| extent())?;
    let layout = novarocks_type_contract::owned_resources::layout::arc_layout(payload)
        .map_err(|_| extent())?;
    creation.request(layout)?;
    creation.work(source.len())?;
    Ok(Arc::from(source))
}

fn invalid(reason: impl Into<String>) -> Failure {
    ConnectorError::new(ConnectorErrorKind::Internal, reason).into()
}

fn extent() -> Failure {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Iceberg derived view exceeds its finite extent",
    )
    .into()
}

fn column(name: &str) -> ast::Expr {
    build::identifier(build::ident(name, false))
}

fn call(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    build::function(
        ast::ObjectName {
            parts: vec![build::ident(name, false)],
            span: SYNTHETIC,
        },
        args,
    )
}

fn row(values: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::Row(ast::RowExpr {
        fields: values
            .into_iter()
            .map(|expression| ast::RowField {
                name: None,
                expression,
                span: SYNTHETIC,
            })
            .collect(),
        span: SYNTHETIC,
    })
}

fn named_type(name: &str, data_type: ast::TypeName) -> ast::RowDataTypeField {
    ast::RowDataTypeField {
        name: Some(build::ident(name, true)),
        data_type,
        span: SYNTHETIC,
    }
}

fn bound(name: &str, field_id: i32) -> ast::Expr {
    build::column(
        build::ident(name, false),
        build::ident(field_id.to_string(), true),
    )
}

fn metric(field_id: i32, ty: ast::TypeName) -> Result<ast::Expr, Failure> {
    let map_value = |name| {
        call(
            "element_at",
            vec![column(name), build::integer(i64::from(field_id))],
        )
    };
    let min = call("min", vec![bound("lower_bounds", field_id)]);
    let max = call("max", vec![bound("upper_bounds", field_id)]);
    let null_count = call("sum", vec![map_value("null_value_counts")]);
    let nan_count = call("sum", vec![map_value("nan_value_counts")]);
    let missing = build::and_all(vec![
        build::is_null(min.clone(), false),
        build::is_null(max.clone(), false),
        build::is_null(null_count.clone(), false),
    ])
    .ok_or_else(|| invalid("Iceberg metric presence predicate is empty"))?;
    build::case_when(
        vec![(missing, build::cast(build::null(), ty.clone()))],
        Some(build::cast(row(vec![min, max, null_count, nan_count]), ty)),
    )
    .map_err(|error| invalid(error.to_string()))
}

fn metric_syntax(primitive: &PrimitiveType) -> ast::TypeName {
    let ty = primitive_syntax(primitive);
    build::row_type(vec![
        named_type("min", ty.clone()),
        named_type("max", ty),
        named_type("null_count", build::simple_type("BIGINT")),
        named_type("nan_count", build::simple_type("BIGINT")),
    ])
}

fn primitive_syntax(primitive: &PrimitiveType) -> ast::TypeName {
    let name = match primitive {
        PrimitiveType::Boolean => "BOOLEAN",
        PrimitiveType::Int => "INTEGER",
        PrimitiveType::Long => "BIGINT",
        PrimitiveType::Float => "REAL",
        PrimitiveType::Double => "DOUBLE",
        PrimitiveType::Date => "DATE",
        PrimitiveType::String => "VARCHAR",
        PrimitiveType::Uuid => "UUID",
        PrimitiveType::Fixed(_) | PrimitiveType::Binary => "VARBINARY",
        PrimitiveType::Decimal { precision, scale } => {
            return build::type_name(
                build::ident("DECIMAL", false),
                vec![
                    build::numeric_parameter(i64::from(*precision)),
                    build::numeric_parameter(i64::from(*scale)),
                ],
            );
        }
        PrimitiveType::Time
        | PrimitiveType::Timestamp
        | PrimitiveType::Timestamptz
        | PrimitiveType::TimestampNs
        | PrimitiveType::TimestamptzNs => {
            let kind = if *primitive == PrimitiveType::Time {
                ast::DateTimeType::Time
            } else {
                ast::DateTimeType::Timestamp
            };
            let with_time_zone = matches!(
                primitive,
                PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs
            );
            let precision = if matches!(
                primitive,
                PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs
            ) {
                9
            } else {
                6
            };
            return ast::TypeName {
                kind: ast::TypeKind::DateTime(ast::DateTimeDataType {
                    kind,
                    with_time_zone,
                    precision: Some(ast::AstBox::new(build::numeric_parameter(precision))),
                }),
                span: SYNTHETIC,
            };
        }
    };
    build::simple_type(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, Schema, SortOrder, TableMetadataBuilder,
    };
    use crate::metadata_batch_reader::{
        MetadataTableType, metadata_output_execution_schema, metadata_table_output_columns,
    };
    use novarocks_type_contract::{ValueKind, ValueLogicalType, ValueTypeStorageLifetime};
    use std::collections::HashMap;

    #[test]
    fn generated_partitions_round_trips_quoted_source_and_metric_types() {
        let extra = [
            ("select", PrimitiveType::Uuid),
            ("mixed\"name", PrimitiveType::Fixed(6)),
            ("utc_ns", PrimitiveType::TimestamptzNs),
        ];
        for (partitioned, report_metrics) in [(true, true), (true, false), (false, true)] {
            let mut columns = vec![
                NestedField::required(1, "identity_id", Type::Primitive(PrimitiveType::Int)).into(),
            ];
            if report_metrics {
                columns.extend(extra.iter().enumerate().map(|(index, (name, ty))| {
                    Arc::new(NestedField::optional(
                        index as i32 + 2,
                        *name,
                        Type::Primitive(ty.clone()),
                    ))
                }));
            }
            let schema = Schema::builder().with_fields(columns).build().unwrap();
            let spec = if partitioned {
                PartitionSpec::builder(Arc::new(schema.clone()))
                    .add_partition_field("identity_id", "identity_id", Transform::Identity)
                    .unwrap()
                    .build()
                    .unwrap()
            } else {
                PartitionSpec::unpartition_spec()
            };
            let metadata = TableMetadataBuilder::new(
                schema,
                spec.into_unbound(),
                SortOrder::unsorted_order(),
                "file:///partitions-view-test".into(),
                FormatVersion::V3,
                HashMap::new(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            let mut request = |_| Ok(());
            let mut work = |_| Ok(());
            let mut creation = ConnectorReadSchemaCreation::new(
                ValueTypeStorageLifetime::pure(),
                &mut request,
                &mut work,
            );
            let columns =
                metadata_table_output_columns(MetadataTableType::Files, &metadata, &mut creation)
                    .unwrap();
            let files = metadata_output_execution_schema(&columns, &mut creation).unwrap();
            let derived = definition(
                "ice",
                "warehouse",
                "a\"table",
                &metadata,
                &files,
                &mut creation,
            )
            .unwrap();
            assert_eq!(derived.name.as_ref(), "a\"table$partitions");
            let body = novarocks_parser::parser::parse_query(&derived.definition.raw_sql).unwrap();
            let ast::SetExpr::Select(select) = body.body.as_ref() else {
                panic!("partitions SELECT");
            };
            let Some(ast::Relation::Table(files)) = &select.from else {
                panic!("source Files relation");
            };
            assert!(files.name.parts.iter().all(|part| part.quoted));
            assert_eq!(
                files
                    .name
                    .parts
                    .iter()
                    .map(|part| part.value.as_str())
                    .collect::<Vec<_>>(),
                ["ice", "warehouse", "a\"table$files"]
            );
            assert!(files.query_period.is_none());
            assert!(select.selection.is_some());
            assert_eq!(select.group_by.is_some(), partitioned);
            assert_eq!(select.having.is_some(), !partitioned);
            let names = derived
                .definition
                .columns
                .iter()
                .map(|column| column.name.as_ref())
                .collect::<Vec<_>>();
            let mut expected = vec!["record_count", "file_count", "total_size"];
            if partitioned {
                expected.insert(0, "partition");
            }
            if report_metrics {
                expected.push("data");
            }
            assert_eq!(names, expected);
            if report_metrics {
                let data = derived
                    .definition
                    .columns
                    .last()
                    .unwrap()
                    .value_type
                    .value_type();
                assert!(!data.nullable());
                let ValueKind::Struct(fields) = data.kind() else {
                    panic!("named data row");
                };
                let expected_names = extra.iter().map(|(name, _)| *name).collect::<Vec<_>>();
                let names = fields.iter().map(|field| field.name()).collect::<Vec<_>>();
                if partitioned {
                    assert_eq!(names, expected_names);
                } else {
                    assert_eq!(&names[1..], expected_names);
                }
                let at = usize::from(!partitioned);
                let min_type = |index: usize| {
                    let stats = fields[at + index].value_type();
                    assert!(stats.nullable());
                    let ValueKind::Struct(metrics) = stats.kind() else {
                        panic!("one field's statistic row");
                    };
                    assert_eq!(
                        metrics.iter().map(|field| field.name()).collect::<Vec<_>>(),
                        ["min", "max", "null_count", "nan_count"]
                    );
                    metrics[0].value_type()
                };
                assert_eq!(min_type(0).domain(), Some(ValueLogicalType::Uuid));
                assert!(matches!(min_type(1).kind(), ValueKind::Binary));
                assert_eq!(
                    min_type(2).temporal(),
                    crate::temporal::declaration(&PrimitiveType::TimestamptzNs)
                );
            }
        }
    }
}
