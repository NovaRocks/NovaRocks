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
use crate::catalog::{SqlInlineColumn, SqlInlineRelation, SqlViewDefinition};
use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::{CompilePhase, FunctionValueType};
use std::sync::Arc;

fn query(sql: &str) -> novarocks_parser::ast::Query {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query fixture")
    };
    query.clone()
}
fn data(rows: &[(&str, Option<&str>)]) -> Arc<SqlInlineRelation> {
    let control = SqlCompileControl::unbounded();
    let columns = [("name", false), ("optional", true)]
        .into_iter()
        .map(|(name, nullable)| SqlInlineColumn {
            name: name.into(),
            value_type: FunctionValueType::new(DataType::Utf8, nullable),
        })
        .collect::<Vec<_>>();
    let arrays = [
        StringArray::from(rows.iter().map(|(name, _)| Some(*name)).collect::<Vec<_>>()),
        StringArray::from(rows.iter().map(|(_, value)| *value).collect::<Vec<_>>()),
    ];
    let pools = columns
        .iter()
        .zip(arrays)
        .map(|(column, array)| {
            ConstantPool::try_new(
                Arc::new(Field::new(
                    column.name.as_ref(),
                    DataType::Utf8,
                    column.value_type.nullable(),
                )),
                column.value_type.clone(),
                array.to_data(),
                crate::constant::test_constant_policy(),
                CompilePhase::Validate,
                &control,
            )
            .unwrap()
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Arc::new(
        SqlInlineRelation::try_new(
            columns.into_boxed_slice(),
            vec![pools].into_boxed_slice(),
            &control,
        )
        .unwrap(),
    )
}
fn declaration(values: Arc<SqlInlineRelation>, sql: &str) -> Box<[crate::catalog::SqlViewColumn]> {
    let source = SqlCapturedCatalogPreparation::try_new_definition(
        query(sql),
        "ice",
        "db",
        None,
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    let [need] = source.needs() else {
        panic!("one definition need")
    };
    assert!(need.is_definition_context());
    assert!(!need.allows_local_view_shadow());
    let fact = CatalogRelationFact::inline(need, values).unwrap();
    source
        .continue_catalog(vec![fact].into_boxed_slice())
        .unwrap()
        .analyze_view_definition(
            builtin_sql_function_catalog(),
            crate::constant::test_constant_policy(),
        )
        .unwrap()
}

#[test]
fn inline_values_keep_declared_schema_for_zero_rows_and_all_null_cells() {
    let mut original = None;
    for rows in [
        &[][..],
        &[("alpha", None)][..],
        &[("beta", Some("value"))][..],
    ] {
        let values = data(rows);
        assert!(values.retained_bytes() > 0);
        let declared = declaration(values, "SELECT * FROM information_schema.schemata");
        assert_eq!(declared[0].name.as_ref(), "name");
        assert_eq!(declared[1].name.as_ref(), "optional");
        assert_eq!(
            declared[0].value_type,
            FunctionValueType::new(DataType::Utf8, true)
        );
        assert_eq!(
            declared[1].value_type,
            FunctionValueType::new(DataType::Utf8, true)
        );
        if let Some(original) = &original {
            assert_eq!(&declared, original);
        } else {
            original = Some(declared);
        }
    }
}

#[test]
fn a_saved_view_uses_definition_inline_facts_without_borrowing_current_lookup_context() {
    let columns = declaration(data(&[]), "SELECT * FROM information_schema.schemata");
    let view = Arc::new(SqlViewDefinition {
        identity: novarocks_types::naming::TableIdentity::new("ice", "db", "v"),
        version: "declared-v1".into(),
        raw_sql: "SELECT * FROM information_schema.schemata".into(),
        default_catalog: "ice".into(),
        default_database: "db".into(),
        columns: columns.into(),
    });
    for rows in [
        &[][..],
        &[("alpha", None)][..],
        &[("beta", Some("value"))][..],
    ] {
        let outer = query("SELECT v.name, v.optional FROM ice.db.v");
        let source = SqlCapturedCatalogPreparation::try_new(
            outer.clone(),
            Some("caller"),
            "elsewhere",
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        let [need] = source.needs() else {
            panic!("one current view need")
        };
        assert!(!need.is_definition_context());
        let fact = CatalogRelationFact::view(need, view.clone()).unwrap();
        let source = source
            .continue_catalog(vec![fact].into_boxed_slice())
            .unwrap();
        let [need] = source.needs() else {
            panic!("one saved-body need")
        };
        assert!(need.is_definition_context());
        assert_eq!(need.relation().fqn(), "ice.information_schema.schemata");
        let values = data(rows);
        let fact = CatalogRelationFact::inline(need, values.clone()).unwrap();
        let source = source
            .continue_catalog(vec![fact].into_boxed_slice())
            .unwrap();
        let catalog = source.capture(&[]).unwrap();
        let (resolved, _, _) = crate::analyzer::analyze_with_function_catalog(
            &outer,
            catalog.provider(),
            "elsewhere",
            builtin_sql_function_catalog(),
            crate::constant::test_constant_policy(),
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
        let crate::analysis::QueryBody::Select(outer) = &resolved.body else {
            panic!("outer SELECT")
        };
        let Some(crate::analysis::Relation::Subquery {
            query: view_query, ..
        }) = &outer.from
        else {
            panic!("saved view body")
        };
        let crate::analysis::QueryBody::Select(body) = &view_query.body else {
            panic!("saved SELECT")
        };
        let Some(crate::analysis::Relation::Subquery {
            query: inline_query,
            ..
        }) = &body.from
        else {
            panic!("original inline relation")
        };
        let crate::analysis::QueryBody::Values(inline) = &inline_query.body else {
            panic!("original typed inline values")
        };
        for (ordinal, column) in values.columns.iter().enumerate() {
            assert!(view.columns[ordinal].value_type.nullable());
            for fields in [
                &resolved.output_columns,
                &view_query.output_columns,
                &inline_query.output_columns,
            ] {
                assert_eq!(fields[ordinal].value_type, column.value_type);
                assert_eq!(
                    fields[ordinal].column_id,
                    inline.output_columns[ordinal].column_id
                );
            }
            assert!(matches!(
                body.projection[ordinal].expr.kind,
                crate::analysis::ExprKind::ColumnRef { column_id, .. }
                    if column_id == inline.output_columns[ordinal].column_id
            ));
            for (row, cells) in inline.rows.iter().enumerate() {
                let crate::analysis::ExprKind::Constant(value) = &cells[ordinal].kind else {
                    panic!("original pool value")
                };
                assert_eq!(cells[ordinal].value_type, column.value_type);
                assert_eq!(value.ordinal(), u32::try_from(row).unwrap());
                assert!(Arc::ptr_eq(
                    value.pool().array(),
                    values.batches[0][ordinal].array()
                ));
            }
        }
    }
}

#[test]
fn current_and_definition_needs_at_the_same_name_do_not_merge_or_accept_the_wrong_fact() {
    let outer = query("SELECT * FROM ice.information_schema.schemata s JOIN ice.db.v v ON 1=1");
    let source = SqlCapturedCatalogPreparation::try_new(
        outer,
        Some("ice"),
        "db",
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    let facts = source
        .needs()
        .iter()
        .map(|need| {
            assert!(!need.is_definition_context());
            assert!(CatalogRelationFact::inline(need, data(&[])).is_err());
            if need.relation().table == "v" {
                CatalogRelationFact::view(
                    need,
                    Arc::new(SqlViewDefinition {
                        identity: need.relation().clone(),
                        version: "v1".into(),
                        raw_sql: "SELECT * FROM information_schema.schemata".into(),
                        default_catalog: "ice".into(),
                        default_database: "db".into(),
                        columns: declaration(
                            data(&[]),
                            "SELECT * FROM information_schema.schemata",
                        )
                        .into(),
                    }),
                )
                .unwrap()
            } else {
                CatalogRelationFact::missing(need, "ordinary fixture absence").unwrap()
            }
        })
        .collect::<Vec<_>>();
    let source = source.continue_catalog(facts.into_boxed_slice()).unwrap();
    let [need] = source.needs() else {
        panic!("definition need must be separately requested")
    };
    assert!(need.is_definition_context());
    assert_eq!(need.relation().fqn(), "ice.information_schema.schemata");
    let fact = CatalogRelationFact::inline(need, data(&[])).unwrap();
    source
        .continue_catalog(vec![fact].into_boxed_slice())
        .unwrap()
        .capture(&[])
        .unwrap();
}

#[test]
fn inline_declarations_cannot_relabel_a_pool_or_override_its_full_type() {
    for columns in [
        vec![
            SqlInlineColumn {
                name: "wrong".into(),
                value_type: FunctionValueType::new(DataType::Utf8, false),
            },
            SqlInlineColumn {
                name: "optional".into(),
                value_type: FunctionValueType::new(DataType::Utf8, true),
            },
        ],
        vec![
            SqlInlineColumn {
                name: "name".into(),
                value_type: FunctionValueType::new(DataType::Utf8, true),
            },
            SqlInlineColumn {
                name: "optional".into(),
                value_type: FunctionValueType::new(DataType::Utf8, true),
            },
        ],
    ] {
        let original = data(&[("one", None)]);
        let batches = original
            .batches
            .iter()
            .map(|batch| batch.to_vec().into_boxed_slice())
            .collect::<Vec<_>>();
        assert!(
            SqlInlineRelation::try_new(
                columns.into_boxed_slice(),
                batches.into_boxed_slice(),
                &SqlCompileControl::unbounded()
            )
            .is_err()
        );
    }
}

#[test]
fn definition_fact_cannot_be_answered_with_a_current_context_at_the_same_id_and_name() {
    let source = SqlCapturedCatalogPreparation::try_new_definition(
        query("SELECT * FROM information_schema.schemata"),
        "ice",
        "db",
        None,
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    let [need] = source.needs() else {
        panic!("one need")
    };
    let current =
        CatalogRelationNeed::try_new(need.id(), need.relation().clone(), need.target()).unwrap();
    let fact = CatalogRelationFact::missing(&current, "wrong context").unwrap();
    assert!(
        source
            .continue_catalog(vec![fact].into_boxed_slice())
            .is_err()
    );
}

#[test]
fn original_pool_retained_representation_observes_the_original_control() {
    struct Stop;
    impl novarocks_type_contract::PureCompileControl for Stop {
        fn checkpoint(
            &self,
            _: CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Err(novarocks_type_contract::CompileControlError::DeadlineExceeded)
        }
    }
    let original = data(&[("one", None)]);
    let pool = &original.batches[0][0];
    assert!(matches!(
        pool.exchange_resource_bytes_observed(&Stop),
        Err(novarocks_constant_contract::ConstantError::Control(
            novarocks_type_contract::CompileControlError::DeadlineExceeded
        ))
    ));
}

#[test]
fn historical_inline_definition_is_a_pure_capability_refusal_and_never_current_values() {
    let preparation = SqlCapturedCatalogPreparation::try_new_definition(
        query("SELECT name FROM information_schema.schemata FOR VERSION AS OF 17"),
        "ice",
        "db",
        None,
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    let [need] = preparation.needs() else {
        panic!("one exact need")
    };
    assert!(CatalogRelationFact::inline(need, data(&[])).is_err());
    let refusal = CatalogRelationFact::inline_time_travel_unsupported(need).unwrap();
    let preparation = preparation
        .continue_catalog(vec![refusal].into_boxed_slice())
        .unwrap();
    let error = preparation
        .analyze_view_definition(
            builtin_sql_function_catalog(),
            crate::constant::test_constant_policy(),
        )
        .unwrap_err();
    assert!(
        matches!(error, SqlCompileError::Analyze(ref error) if error.kind() == crate::analyze_error::AnalyzeErrorKind::UnsupportedQueryShape)
    );
}
