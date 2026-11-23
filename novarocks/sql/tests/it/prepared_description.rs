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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use novarocks_functions::ConstantValue;
use novarocks_spi::connector::{ConnectorReadSelector, ConnectorTablePlanningFacts};
use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::binding::SqlTableBindingAllocator;
use novarocks_sql::compiler::{
    CatalogQualifications, CatalogRelationFact, SqlCapturedCatalogPreparation, SqlCompileControl,
    SqlCompileError, SqlConstantExpressionSite, SqlExpressionConstant, SqlExpressionConstants,
    SqlPreparedDescription, SqlPreparedParameters, SqlWriteAssignmentSite, SqlWriteAssignments,
    builtin_sql_function_catalog,
};
use novarocks_sql::planning::catalog::{ConnectorReadTableFacts, materialize_connector_read_table};
use novarocks_sql::sql_mode::SqlSemanticSettings;
use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueLogicalType};
use novarocks_types::naming::TableIdentity;
use novarocks_types::schema::ColumnDef;

fn describe(
    sql: &str,
    assignments: Option<&SqlWriteAssignments>,
) -> Result<SqlPreparedDescription, SqlCompileError> {
    describe_with_materialized_constants(sql, assignments, None)
}

fn describe_with_materialized_constants(
    sql: &str,
    assignments: Option<&SqlWriteAssignments>,
    materialized_constants: Option<SqlExpressionConstants>,
) -> Result<SqlPreparedDescription, SqlCompileError> {
    let (preparation, parameters, session) = capture(sql)?;
    preparation.describe(
        &parameters,
        &session,
        builtin_sql_function_catalog(),
        novarocks_sql_testkit::sql_fixture_constant_policy(),
        assignments,
        materialized_constants,
    )
}

fn capture(
    sql: &str,
) -> Result<
    (
        SqlCapturedCatalogPreparation,
        SqlPreparedParameters,
        novarocks_sql::compiler::SqlSessionContext,
    ),
    SqlCompileError,
> {
    let query = novarocks_sql_testkit::parse_query(sql);
    let parameters = SqlPreparedParameters::describe_query(&query);
    let mut session = super::fixtures::session(SqlSemanticSettings::default(), false);
    session.current_catalog = Some("iceberg".into());
    let control = SqlCompileControl::unbounded();
    let mut preparation = SqlCapturedCatalogPreparation::try_new_description(
        *query,
        &parameters,
        &session,
        control,
        CatalogQualifications::default(),
    )?;
    let mut bindings = SqlTableBindingAllocator::new_unique().unwrap();
    while !preparation.needs().is_empty() {
        let facts = preparation
            .needs()
            .iter()
            .map(|need| {
                let relation = need.relation();
                if relation.table != "t" {
                    return CatalogRelationFact::missing(need, "Table does not exist").unwrap();
                }
                let table = materialize_connector_read_table(ConnectorReadTableFacts {
                    catalog: relation.catalog.clone(),
                    namespace: relation.namespace.clone(),
                    table: relation.table.clone(),
                    columns: vec![ColumnDef {
                        name: "x".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
                    binding: bindings.allocate().unwrap(),
                    selector: ConnectorReadSelector::Current,
                    planning_facts: ConnectorTablePlanningFacts::empty(),
                })
                .unwrap()
                .into_resolved_table();
                CatalogRelationFact::resolved(need, table).unwrap()
            })
            .collect();
        preparation = preparation.continue_catalog(facts)?;
    }
    Ok((preparation, parameters, session))
}

#[test]
fn ctas_description_checks_original_definition_names_before_presentation() {
    let describe_ctas = |sql| {
        let (preparation, parameters, session) = capture(sql)?;
        preparation.describe_ctas_source(
            &parameters,
            &session,
            builtin_sql_function_catalog(),
            novarocks_sql_testkit::sql_fixture_constant_policy(),
        )
    };
    let described = describe_ctas("SELECT CAST(? AS BIGINT) AS id, x FROM t").unwrap();
    assert_eq!(described.parameters[0].data_type(), &DataType::Null);
    assert_eq!(described.columns[0].name, "id");
    assert_eq!(
        described.columns[0].value_type.data_type(),
        &DataType::Int64
    );
    assert!(matches!(
        describe_ctas("SELECT CAST(? AS BIGINT), x FROM t"),
        Err(SqlCompileError::Analyze(error)) if error.kind() == AnalyzeErrorKind::MissingColumnName
    ));
    assert!(matches!(
        describe_ctas("SELECT CAST(? AS BIGINT) AS x, x FROM t"),
        Err(SqlCompileError::Analyze(error)) if error.kind() == AnalyzeErrorKind::DuplicateColumnName
    ));
}

#[test]
fn parameter_types_follow_the_selected_implicit_conversions() {
    let described = describe(
        "SELECT CAST(? AS BIGINT), ? + BIGINT '3', ? FROM t WHERE ? OFFSET ? LIMIT ?",
        None,
    )
    .unwrap();
    assert_eq!(
        described
            .parameters
            .iter()
            .map(FunctionValueType::data_type)
            .collect::<Vec<_>>(),
        [
            &DataType::Null,
            &DataType::Int64,
            &DataType::Null,
            &DataType::Boolean,
            &DataType::Int64,
            &DataType::Int64
        ],
    );
    assert_eq!(
        described.columns[0].value_type.data_type(),
        &DataType::Int64
    );
    assert_eq!(described.columns[2].value_type.data_type(), &DataType::Null);
}

#[test]
fn visible_columns_keep_direct_origin_and_only_their_own_output_alias() {
    let described = describe(
        r#"WITH q AS (SELECT x AS nested FROM t)
         SELECT q.nested, q.nested AS "é😀", q.*, q.nested + 1, CAST(q.nested AS BIGINT) FROM q"#,
        None,
    )
    .unwrap();
    let origin = TableIdentity::new("iceberg", "fixture", "t");
    assert_eq!(
        described
            .columns
            .iter()
            .map(|field| { (field.name.as_str(), field.origin.as_ref(), field.aliased) })
            .collect::<Vec<_>>(),
        [
            ("nested", Some(&origin), false),
            ("é😀", Some(&origin), true),
            ("nested", Some(&origin), false),
            ("_col3", None, false),
            ("_col4", None, false),
        ],
    );
}

#[test]
fn merged_using_output_does_not_claim_one_input_table() {
    let described = describe("SELECT x AS merged FROM t a JOIN t b USING (x)", None).unwrap();
    assert_eq!(described.columns[0].name, "merged");
    assert!(described.columns[0].aliased);
    assert_eq!(described.columns[0].origin, None);
}

#[test]
fn widths_come_from_the_trino_type_even_when_the_arrow_carrier_differs() {
    let described = describe(
        "SELECT CAST(NULL AS CHAR(4)), CAST(NULL AS VARCHAR(4)), CAST(NULL AS DECIMAL(18, 4)), CAST(NULL AS DECIMAL(19, 4)), CAST(NULL AS TIMESTAMP(3) WITH TIME ZONE), CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE)",
        None,
    ).unwrap();
    assert_eq!(
        described.columns[4].value_type.data_type(),
        &DataType::FixedSizeBinary(14)
    );
    assert_eq!(
        described
            .columns
            .iter()
            .map(|column| column.trino_type_size())
            .collect::<Vec<_>>(),
        [0, 0, 8, 16, 8, 12],
    );
}

#[test]
fn write_source_assignment_checks_do_not_infer_a_parameter_type() {
    let control = SqlCompileControl::unbounded();
    let site = SqlWriteAssignmentSite::try_new(
        0,
        FunctionValueType::new(DataType::Int64, true),
        None,
        &control,
    )
    .unwrap();
    let sites = SqlWriteAssignments::try_new(Box::new([site]), &control).unwrap();
    let described = describe("SELECT ?", Some(&sites)).unwrap();
    assert_eq!(described.parameters[0].data_type(), &DataType::Null);
    assert_eq!(
        described.columns[0].value_type.data_type(),
        &DataType::Int64
    );
    for (sql, kind) in [
        ("SELECT x FROM missing", AnalyzeErrorKind::UnknownTable),
        ("SELECT missing FROM t", AnalyzeErrorKind::UnknownColumn),
        ("SELECT ARRAY[1]", AnalyzeErrorKind::TypeMismatch),
        ("SELECT 1, 2", AnalyzeErrorKind::TypeMismatch),
    ] {
        let error = describe(sql, Some(&sites)).unwrap_err();
        let SqlCompileError::Analyze(error) = error else {
            panic!("{error:?}")
        };
        assert_eq!(error.kind(), kind, "{sql}: {error}");
    }
}

#[test]
fn write_source_description_retains_admitted_nominal_materialization() {
    let control = SqlCompileControl::unbounded();
    let ty = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Variant,
    )
    .unwrap();
    let bytes =
        novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes("null").unwrap();
    let value = ConstantValue::from_binary(
        Arc::new(Field::new("", DataType::LargeBinary, false)),
        ty.clone(),
        &bytes,
        novarocks_sql_testkit::sql_fixture_constant_policy(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let constants = vec![SqlExpressionConstant::new(
        Vec::new(),
        SqlConstantExpressionSite::ValuesCell { row: 0, column: 0 },
        value,
    )]
    .into();
    let site = SqlWriteAssignmentSite::try_new(0, ty, None, &control).unwrap();
    let sites = SqlWriteAssignments::try_new(Box::new([site]), &control).unwrap();
    let description =
        describe_with_materialized_constants("VALUES (NULL)", Some(&sites), Some(constants))
            .unwrap();
    let described = &description.columns[0];
    assert_eq!(
        described.value_type.logical_type(),
        ValueLogicalType::Variant
    );
    assert_eq!(described.value_type.data_type(), &DataType::LargeBinary);
}

#[test]
fn unknown_table_version_is_refused_before_catalog_requests_are_published() {
    let query = novarocks_sql_testkit::parse_query("SELECT x FROM t FOR VERSION AS OF ?");
    let parameters = SqlPreparedParameters::describe_query(&query);
    let result = SqlCapturedCatalogPreparation::try_new_description(
        *query,
        &parameters,
        &super::fixtures::session(SqlSemanticSettings::default(), false),
        SqlCompileControl::unbounded(),
        CatalogQualifications::default(),
    );
    let Err(SqlCompileError::Analyze(error)) = result else {
        panic!("unknown version must be rejected before SchemaOnly capture");
    };
    assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedQueryShape);
    assert_eq!(error.span(), Some(parameters.positions()[0]));
}

#[test]
fn mutation_description_observes_original_assignment_root_after_valid_conversion() {
    let control = SqlCompileControl::unbounded();
    let assignments = |columns: &[(usize, DataType)]| {
        let sites = columns
            .iter()
            .map(|(ordinal, ty)| {
                SqlWriteAssignmentSite::try_new(
                    *ordinal,
                    FunctionValueType::new(ty.clone(), true),
                    None,
                    &control,
                )
                .unwrap()
            })
            .collect();
        SqlWriteAssignments::try_new(sites, &control)
            .unwrap()
            .for_mutation_description()
    };
    let write = assignments(&[
        (2, DataType::Int64),
        (3, DataType::Utf8),
        (4, DataType::Int64),
    ]);
    let described = describe(
        "SELECT 3 AS identity, t.*, ? + BIGINT '1', (?), CAST(? AS BIGINT) FROM t WHERE x = ?",
        Some(&write),
    )
    .unwrap();
    assert_eq!(
        described
            .parameters
            .iter()
            .map(FunctionValueType::data_type)
            .collect::<Vec<_>>(),
        [
            &DataType::Int64,
            &DataType::Utf8,
            &DataType::Null,
            &DataType::Int64
        ],
    );

    let write = assignments(&[(1, DataType::Int64)]);
    assert!(matches!(
        describe("SELECT t.*, 'x' FROM t", Some(&write)),
        Err(SqlCompileError::Analyze(error)) if error.kind() == AnalyzeErrorKind::TypeMismatch
    ));
}
