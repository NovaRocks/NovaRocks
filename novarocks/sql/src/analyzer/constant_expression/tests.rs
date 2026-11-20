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
use crate::compiler::{SqlCatalogSnapshot, SqlCompileControl};
use std::sync::Arc;

struct NoRelations;
impl crate::catalog::PlannerTableProvider for NoRelations {
    fn resolve_table_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        _: &str,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        panic!("required constant must not resolve a relation")
    }
}
impl SqlCatalogSnapshot for NoRelations {
    fn planner_table_provider(&self) -> &dyn crate::catalog::PlannerTableProvider {
        self
    }
}
fn syntax(
    sql: &str,
    target: RequiredConstantTarget,
) -> Result<PreparedRequiredConstant, SqlCompileError> {
    let expression = novarocks_parser::parser::parse_expression(sql).unwrap();
    prepare(RequiredConstantRequest {
        input: RequiredConstantInput::Expression(&expression),
        target,
        catalog: &NoRelations,
        current_database: "db",
        functions: crate::functions::test_function_catalog_snapshot(),
        sql_semantics: &crate::sql_mode::SqlSemanticSettings::default(),
        constant_policy: crate::constant::test_constant_policy(),
        control: &SqlCompileControl::unbounded(),
    })
}

#[test]
fn required_constant_implicit_targets_do_not_use_general_cast_admission() {
    use RequiredConstantTarget::*;
    for (sql, target, datatype) in [
        ("true", Boolean, DataType::Boolean),
        ("CAST(1 AS tinyint)", Integer, DataType::Int32),
        ("CAST(1 AS smallint)", Bigint, DataType::Int64),
        ("1 + 2", Integer, DataType::Int32),
        ("1", Bigint, DataType::Int64),
        ("CAST(1 AS bigint)", Double, DataType::Float64),
        ("CAST(1 AS decimal(8,2))", Double, DataType::Float64),
        ("lower('ABC')", Varchar, DataType::Utf8),
    ] {
        let prepared = syntax(sql, target).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(prepared.value_type().data_type(), &datatype, "{sql}");
        let source = prepared.source();
        assert!(source.unit().ports().is_empty());
        assert_eq!(source.unit().roots().len(), 1);
        let root = source.unit().root(source.root()).unwrap();
        assert_eq!(
            root.scope,
            novarocks_expression_contract::RootScope::AllPorts
        );
        assert_eq!(
            root.demand,
            novarocks_type_contract::EvaluationDemand::Value
        );
        assert_eq!(
            &source.unit().definition(root.definition).unwrap().ty,
            prepared.value_type()
        );
    }
    for (sql, target) in [
        ("1", Boolean),
        ("'true'", Boolean),
        ("'1'", Integer),
        ("2147483648", Integer),
        ("1.0", Integer),
        ("1", Varchar),
        ("CAST('1' AS largeint)", Bigint),
    ] {
        assert!(syntax(sql, target).is_err(), "{sql}");
    }
    for sql in ["JSON '1'", "parse_json('1')", "CAST(NULL AS json)"] {
        let Err(SqlCompileError::Analyze(error)) = syntax(sql, Varchar) else {
            panic!("{sql}: expected nominal JSON implicit type refusal");
        };
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression,
            "{sql}: a missing function or capability is not a nominal type refusal"
        );
        assert_eq!(
            error.message(),
            "required constant cannot be implicitly converted to the declared property type",
            "{sql}: preserve the actual implicit target gate"
        );
        assert_eq!(error.span(), Some(Span::new(0, sql.len())), "{sql}");
    }
    // A declared JSON cast alone does not validate text or author a nominal
    // JSON value. Preserve the ordinary analyzer's String result here.
    let text = syntax("CAST('1' AS json)", Varchar).unwrap();
    assert!(matches!(
        text.value_type().value_type().kind(),
        ValueKind::String
    ));
}

#[test]
fn required_constant_rejects_dependencies_before_any_relation_lookup() {
    for sql in [
        "(SELECT count(*) FROM private_table)",
        "EXISTS(SELECT 1 FROM private_table)",
        "1 IN (SELECT v FROM private_table)",
        "v",
        "sum(1)",
        "row_number() OVER ()",
        "rand()",
    ] {
        assert!(
            syntax(sql, RequiredConstantTarget::Bigint).is_err(),
            "{sql}"
        );
    }
    let nullable = syntax("NULL", RequiredConstantTarget::Integer).unwrap();
    assert_eq!(nullable.value_type().data_type(), &DataType::Int32);
    assert!(
        nullable.value_type().nullable(),
        "NULL is preserved for application validation"
    );
}

#[test]
fn required_materialized_value_keeps_its_pool_and_nonzero_ordinal() {
    let policy = crate::constant::test_constant_policy();
    let control = SqlCompileControl::unbounded();
    let array = arrow::array::Int32Array::from(vec![Some(91), Some(7), None]);
    let pool = novarocks_functions::ConstantPool::try_new(
        Arc::new(arrow::datatypes::Field::new(
            "original",
            DataType::Int32,
            true,
        )),
        FunctionValueType::new(DataType::Int32, true),
        arrow::array::Array::to_data(&array),
        policy,
        CompilePhase::FunctionSpecialization,
        &control,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let prepared = prepare(RequiredConstantRequest {
        input: RequiredConstantInput::Materialized(value.clone()),
        target: RequiredConstantTarget::Integer,
        catalog: &NoRelations,
        current_database: "db",
        functions: crate::functions::test_function_catalog_snapshot(),
        sql_semantics: &crate::sql_mode::SqlSemanticSettings::default(),
        constant_policy: policy,
        control: &control,
    })
    .unwrap();
    let original = prepared.original_value().expect("unchanged checked value");
    assert_eq!(
        original.pool().backing_identity(),
        value.pool().backing_identity()
    );
    assert_eq!(original.ordinal(), 1);
    assert_eq!(original.try_i64().unwrap(), Some(7));
    let unit = prepared.source().unit();
    let root = unit.root(prepared.source().root()).unwrap();
    let novarocks_expression_contract::ExprKind::Constant(reference) =
        unit.definition(root.definition).unwrap().kind
    else {
        panic!("unchanged materialized input must remain the actual constant root");
    };
    let mut work =
        novarocks_type_contract::CompileCheckpoints::try_new(&control, CompilePhase::Validate)
            .unwrap();
    let emitted = prepared
        .source()
        .constants()
        .resolve_source_observed(reference, &mut work)
        .unwrap();
    work.finish().unwrap();
    assert_eq!(emitted.ordinal(), 1);
    assert_eq!(
        emitted.pool().backing_identity(),
        value.pool().backing_identity()
    );
    assert!(Arc::ptr_eq(
        emitted.pool().field_ref(),
        value.pool().field_ref()
    ));
    assert!(Arc::ptr_eq(emitted.pool().array(), value.pool().array()));
}

#[test]
fn required_preparation_does_not_fold_and_preserves_original_publication_source() {
    use crate::compiler::{
        SqlFoldDependencyInput, SqlFoldDependencyObserver, SqlFoldEvaluationOutcome,
    };
    use novarocks_type_contract::CompileControlError;
    struct RejectPublication {
        cause: CompileControlError,
    }
    impl SqlFoldDependencyObserver for RejectPublication {
        fn before_fold_dependency_observed(
            &self,
            _: SqlFoldDependencyInput<'_>,
            _: &dyn PureCompileControl,
        ) -> Result<(), CompileControlError> {
            panic!("required preparation must not enter optional folding")
        }
        fn after_fold_dependency_observed(
            &self,
            _: SqlFoldDependencyInput<'_>,
            _: SqlFoldEvaluationOutcome<'_>,
        ) {
            panic!("required preparation must not enter optional folding")
        }
        fn observe_required_scalar_source_observed(
            &self,
            source: &crate::compiler::SqlAuthoredScalarUnit,
            _: &dyn PureCompileControl,
        ) -> Result<(), CompileControlError> {
            assert!(source.unit().ports().is_empty());
            assert_eq!(source.unit().roots().len(), 1);
            Err(self.cause.clone())
        }
    }
    let source = novarocks_failure_contract::SharedFailureCause::new(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
    );
    let control =
        SqlCompileControl::unbounded().with_fold_dependency_observer(Arc::new(RejectPublication {
            cause: CompileControlError::Source(source.clone()),
        }));

    let expression = novarocks_parser::parser::parse_expression("1 + 2").unwrap();
    let result = prepare(RequiredConstantRequest {
        input: RequiredConstantInput::Expression(&expression),
        target: RequiredConstantTarget::Integer,
        catalog: &NoRelations,
        current_database: "db",
        functions: crate::functions::test_function_catalog_snapshot(),
        sql_semantics: &crate::sql_mode::SqlSemanticSettings::default(),
        constant_policy: crate::constant::test_constant_policy(),
        control: &control,
    });
    let Err(SqlCompileError::CompilationSource(actual)) = result else {
        panic!("original source refusal");
    };
    assert_eq!(actual, source);
}

#[test]
fn required_implicit_coercion_uses_semantic_kind_not_encoded_layout() {
    use novarocks_type_contract::{
        ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let binding = |kind, layout| {
        let storage = ValueTypeStorageLifetime::pure();
        FunctionValueType::try_from_value(
            ValueTypeRef::try_new(kind, true, storage.clone()).unwrap(),
            ValueLayout::try_new(&layout, storage).unwrap(),
        )
        .unwrap()
    };
    let encoded_integer = binding(
        ValueKind::Scalar(DataType::Int32),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
    );
    assert!(
        validate_implicit_target(
            &encoded_integer,
            RequiredConstantTarget::Integer,
            Span::new(0, 0)
        )
        .is_ok()
    );
    let encoded_string = binding(
        ValueKind::String,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    );
    assert!(
        validate_implicit_target(
            &encoded_string,
            RequiredConstantTarget::Varchar,
            Span::new(0, 0)
        )
        .is_ok()
    );
    let nominal_json = binding(ValueKind::Nominal(ValueLogicalType::Json), DataType::Utf8);
    assert!(
        validate_implicit_target(
            &nominal_json,
            RequiredConstantTarget::Varchar,
            Span::new(0, 0)
        )
        .is_err()
    );
    for data_type in [DataType::Decimal32(4, 1), DataType::Decimal64(10, 2)] {
        let source = binding(ValueKind::Scalar(data_type.clone()), data_type);
        assert!(
            validate_implicit_target(&source, RequiredConstantTarget::Double, Span::new(0, 0))
                .is_ok()
        );
    }
}

#[test]
fn required_array_varchar_uses_the_whole_expression_author() {
    for sql in [
        "ARRAY['a', 'b']",
        "ARRAY['a', NULL]",
        "ARRAY[]",
        "ARRAY[NULL]",
        "NULL",
        "IF(TRUE, ARRAY['a'], ARRAY['b'])",
    ] {
        let prepared = syntax(sql, RequiredConstantTarget::ArrayVarchar)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        let ValueKind::List(child) = prepared.value_type().value_type().kind() else {
            panic!("{sql}: a declared List result")
        };
        assert!(matches!(child.kind(), ValueKind::String), "{sql}");
        assert!(child.nullable(), "{sql}");
        let DataType::List(field) = prepared.value_type().data_type() else {
            panic!()
        };
        assert_eq!(field.name(), "item");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(field.is_nullable());
        assert_eq!(prepared.value_type().nullable(), sql == "NULL", "{sql}");
        let source = prepared.source();
        assert!(source.unit().ports().is_empty());
        assert_eq!(source.unit().roots().len(), 1);
        let root = source.unit().root(source.root()).unwrap();
        assert_eq!(
            root.scope,
            novarocks_expression_contract::RootScope::AllPorts
        );
        assert_eq!(
            root.demand,
            novarocks_type_contract::EvaluationDemand::Value
        );
        assert_eq!(
            &source.unit().definition(root.definition).unwrap().ty,
            prepared.value_type()
        );
    }
    for sql in [
        "ARRAY[1]",
        "ARRAY[TRUE]",
        "ARRAY[JSON '1']",
        "ARRAY[ARRAY['a']]",
        "'not an array'",
    ] {
        assert!(
            syntax(sql, RequiredConstantTarget::ArrayVarchar).is_err(),
            "{sql}"
        );
    }
}

#[test]
fn required_array_varchar_identity_keeps_the_complete_original_binding() {
    for root_nullable in [false, true] {
        for child_nullable in [false, true] {
            let original = FunctionValueType::new(
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Utf8,
                    child_nullable,
                ))),
                root_nullable,
            );
            validate_implicit_target(
                &original,
                RequiredConstantTarget::ArrayVarchar,
                Span::new(7, 17),
            )
            .unwrap();
            let target = required_target_value_type(
                &original,
                RequiredConstantTarget::ArrayVarchar,
                &SqlCompileControl::unbounded(),
            )
            .unwrap();
            assert_eq!(target, original);
            assert!(target.value_type().shares_storage(original.value_type()));
            assert!(
                target
                    .typed_binding()
                    .layout()
                    .shares_storage(original.typed_binding().layout())
            );
            assert_eq!(target.child_at(0).unwrap().nullable(), child_nullable);
            assert_eq!(target.nullable(), root_nullable);
        }
    }
    assert_eq!(
        RequiredConstantTarget::ArrayVarchar.scalar_data_type(),
        None
    );
    assert_eq!(
        RequiredConstantTarget::Varchar.scalar_data_type(),
        Some(DataType::Utf8)
    );
}

#[test]
fn required_array_target_checks_semantic_children_before_carrier_casts() {
    use novarocks_type_contract::{
        ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
    };
    for child_kind in [
        ValueKind::Nominal(ValueLogicalType::Json),
        ValueKind::Scalar(DataType::Int32),
    ] {
        let child_carrier = if matches!(&child_kind, ValueKind::Scalar(DataType::Int32)) {
            DataType::Int32
        } else {
            DataType::Utf8
        };
        let storage = ValueTypeStorageLifetime::pure();
        let child = ValueTypeRef::try_new(child_kind, true, storage.clone()).unwrap();
        let value = ValueTypeRef::try_new(ValueKind::List(child), false, storage.clone()).unwrap();
        let layout = ValueLayout::try_new(
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                child_carrier,
                true,
            ))),
            storage,
        )
        .unwrap();
        let original = FunctionValueType::try_from_value(value, layout).unwrap();
        assert!(
            validate_implicit_target(
                &original,
                RequiredConstantTarget::ArrayVarchar,
                Span::new(7, 17),
            )
            .is_err()
        );
    }
}

#[test]
fn required_array_unknown_declaration_preserves_original_control_cause() {
    use novarocks_type_contract::CompileControlError;
    struct Stop(CompileControlError);
    impl PureCompileControl for Stop {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }
    let original = FunctionValueType::new(DataType::Null, true);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::ResourceExhausted,
        CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )),
    ] {
        let error = required_target_value_type(
            &original,
            RequiredConstantTarget::ArrayVarchar,
            &Stop(cause.clone()),
        )
        .unwrap_err();
        assert_eq!(error.control_error(), Some(cause));
    }
}

#[test]
fn required_scalar_unit_errors_keep_original_resource_fact_and_control_source() {
    use crate::planner::distributed::build::ContractLoweringError;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_type_contract::CompileControlError;
    let original =
        novarocks_expression_contract::UnitError::Resource("original unit resource refusal");
    let expected = original.failure_fact();
    assert_eq!(expected.class(), FailureClass::Resource);
    let error = required_lowering_error(ContractLoweringError::from(original));
    let SqlCompileError::CompilationSource(actual) = error else {
        panic!("the original typed unit resource source must be retained");
    };
    assert_eq!(actual.failure_fact(), expected);
    assert!(matches!(
        actual.downcast_ref::<novarocks_expression_contract::UnitError>(),
        Some(novarocks_expression_contract::UnitError::Resource(
            "original unit resource refusal"
        ))
    ));
    let source = novarocks_failure_contract::SharedFailureCause::new(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
    );
    let error = required_lowering_error(ContractLoweringError::from(
        novarocks_expression_contract::UnitError::Control(CompileControlError::Source(
            source.clone(),
        )),
    ));
    let SqlCompileError::CompilationSource(actual) = error else {
        panic!("the original unit control source must be retained");
    };
    assert_eq!(actual, source);
}
