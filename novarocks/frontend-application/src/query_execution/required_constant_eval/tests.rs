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
use arrow::{
    array::Int32Array,
    datatypes::{DataType, Field},
};
use novarocks_failure_contract::FailureClass;
use novarocks_functions::EngineFunctionCatalog;
use novarocks_sql::compiler::{
    RequiredConstantInput, RequiredConstantRequest, RequiredConstantTarget, SqlCapturedCatalog,
    SqlCapturedCatalogPreparation, SqlCompileControl, SqlFunctionCatalog,
    prepare_required_constant,
};
use novarocks_type_contract::FunctionValueType;
use novarocks_workload_control::{
    ResourceConfig, ResultCapacityConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig,
    WorkloadControl,
};

struct Fixture {
    interpreter: RequiredConstantInterpreter,
    metadata: Arc<EngineFunctionCatalog>,
    catalog: SqlCapturedCatalog,
    control: WorkloadControl,
    root: RootWork,
    window: ResultWindowAlias,
}
impl Fixture {
    fn new() -> Self {
        Self::with_capacity(ResultCapacityConfig::V1)
    }
    fn with_capacity(config: ResultCapacityConfig) -> Self {
        let compile = SqlCompileControl::unbounded();
        let (interpreter, metadata) = super::test_support::test_product();
        let statements = novarocks_parser::parse("SELECT 1").unwrap();
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("empty captured catalogue fixture");
        };
        let catalog =
            SqlCapturedCatalogPreparation::try_new(query.clone(), None, "db", compile.clone())
                .unwrap()
                .capture(&[])
                .unwrap();
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        control.configure_result_capacity(config).unwrap();
        control.mark_ready().unwrap();
        let (root, grant) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                novarocks_workload_control::ResultWindowClass::Local,
            )
            .unwrap();
        let window = grant.retain_alias();
        drop(grant);
        Self {
            interpreter,
            metadata,
            catalog,
            control,
            root,
            window,
        }
    }
    fn prepare(
        &self,
        sql: &str,
        target: RequiredConstantTarget,
        control: &SqlCompileControl,
    ) -> PreparedRequiredConstant {
        let tokens = novarocks_parser::lex(sql).unwrap();
        let expr = novarocks_parser::parser::parse_expression(sql, &tokens).unwrap();
        let functions: Arc<dyn SqlFunctionCatalog> = self.metadata.clone();
        prepare_required_constant(RequiredConstantRequest {
            input: RequiredConstantInput::Expression(&expr),
            target,
            catalog: &self.catalog,
            current_database: "db",
            functions,
            sql_semantics: &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            constant_policy: crate::application::test_constant_policy(),
            control,
            version: novarocks_physical_plan::PlanVersionId::try_new(
                *uuid::Uuid::now_v7().as_bytes(),
            )
            .unwrap(),
        })
        .unwrap_or_else(|cause| panic!("{sql}: {cause}"))
    }
    fn evaluate(
        &self,
        sql: &str,
        target: RequiredConstantTarget,
    ) -> Result<ConstantValue, SqlConstantEvaluationError> {
        self.interpreter.evaluate(
            self.prepare(sql, target, &SqlCompileControl::unbounded()),
            &self.root.owner.scope(),
            &self.window,
        )
    }
    fn close(self) {
        assert_eq!(
            self.window.retained_payload_bytes(),
            0,
            "all result/source aliases really dropped"
        );
        drop(self.window);
        self.root.owner.complete();
        self.root.business.release();
        assert_eq!(self.control.snapshot().root_responsibilities, 0);
    }
}

#[test]
fn required_constants_run_real_compiled_calls_and_selected_control_branches() {
    use RequiredConstantTarget::*;
    let fixture = Fixture::new();
    for sql in [
        "1 + 2",
        "CASE WHEN true THEN 3 ELSE 1 / 0 END",
        "CASE WHEN false THEN 1 / 0 ELSE 3 END",
        "if(true, 3, 1 / 0)",
        "coalesce(CAST(3 AS integer), 1 / 0)",
    ] {
        let value = fixture
            .evaluate(sql, Integer)
            .unwrap_or_else(|cause| panic!("{sql}: {cause}"));
        assert_eq!(value.value_type().data_type(), &DataType::Int32);
        assert_eq!(value.try_i64().unwrap(), Some(3), "{sql}");
        assert!(fixture.window.retained_payload_bytes() > 0);
        drop(value);
        assert_eq!(fixture.window.retained_payload_bytes(), 0);
    }
    for (sql, expected) in [
        ("NULL IS NULL", true),
        ("2 IN (1, 2, NULL)", true),
        ("false AND (1 / 0 = 1)", false),
        ("true OR (1 / 0 = 1)", true),
    ] {
        let value = fixture
            .evaluate(sql, Boolean)
            .unwrap_or_else(|cause| panic!("{sql}: {cause}"));
        assert_eq!(value.try_boolean().unwrap(), Some(expected));
    }
    assert_eq!(
        fixture
            .evaluate("lower(concat('A', 'B'))", Varchar)
            .unwrap()
            .try_utf8()
            .unwrap(),
        Some("ab")
    );
    assert_eq!(
        fixture.evaluate("1", Bigint).unwrap().try_i64().unwrap(),
        Some(1)
    );
    assert_eq!(
        fixture
            .evaluate("1", Double)
            .unwrap()
            .try_f64_bits()
            .unwrap(),
        Some(1.0_f64.to_bits())
    );
    assert!(
        fixture
            .evaluate("NULL", Integer)
            .unwrap()
            .is_null_observed(
                CompilePhase::FunctionSpecialization,
                &SqlCompileControl::unbounded()
            )
            .unwrap()
    );
    fixture.close();
}

#[test]
fn required_active_branch_error_preserves_original_data_and_ordinal() {
    let fixture = Fixture::new();
    for sql in [
        "1 / 0",
        "CASE WHEN true THEN 1 / 0 ELSE 3 END",
        "2147483647 + 1",
    ] {
        let error = fixture
            .evaluate(sql, RequiredConstantTarget::Integer)
            .unwrap_err();
        let SqlConstantEvaluationError::Source(cause) = error else {
            panic!("selected row Data must use the normal source port");
        };
        assert_eq!(
            cause.failure_fact().class(),
            FailureClass::Data,
            "{sql}: {cause}"
        );
        let execution = cause
            .downcast_ref::<ExecutionFailure>()
            .expect("original Execution failure");
        let novarocks_execution::runtime::task::ExecutionFailureCause::RequiredRow(required) =
            execution.cause()
        else {
            panic!("original required row");
        };
        assert_eq!(required.error().selected_ordinal(), 0);
        assert_eq!(required.batch_row(), 0);
        // A producer with the same Data fact arriving through the external
        // evaluator port remains Outer; classification alone cannot erase it.
        let outer = SqlConstantEvaluationError::outer_domain(execution.clone());
        assert!(matches!(&outer, SqlConstantEvaluationError::Outer(actual)
            if actual.failure_fact().class() == FailureClass::Data));
        drop(outer);
        let independently_retained = execution.clone();
        drop(cause);
        assert!(
            fixture.window.retained_payload_bytes() > 0,
            "the canonical original source retains its receipt"
        );
        drop(independently_retained);
    }
    fixture.close();
}

#[test]
fn required_constant_uses_the_original_cancellation_source_after_preparation() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Stop(AtomicBool);
    impl novarocks_sql::compiler::SqlCancellationObservation for Stop {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }
    let fixture = Fixture::new();
    let stop = Arc::new(Stop(AtomicBool::new(false)));
    let control = SqlCompileControl::new(None, stop.clone());
    let prepared = fixture.prepare("1 + 2", RequiredConstantTarget::Integer, &control);
    stop.0.store(true, Ordering::SeqCst);
    assert!(matches!(
        fixture
            .interpreter
            .evaluate(prepared, &fixture.root.owner.scope(), &fixture.window),
        Err(SqlConstantEvaluationError::Control(
            CompileControlError::Cancelled
        ))
    ));
    fixture.close();
}

#[test]
fn required_constant_root_capacity_refusal_is_not_invalid_property_data() {
    let mut capacity = ResultCapacityConfig::V1;
    capacity.all_objects_bytes[1] = 1;
    let fixture = Fixture::with_capacity(capacity);
    let error = fixture
        .evaluate("1 + 2", RequiredConstantTarget::Integer)
        .unwrap_err();
    let SqlConstantEvaluationError::Control(CompileControlError::Source(source)) = error else {
        panic!("original Root capacity source");
    };
    assert_eq!(source.failure_fact().class(), FailureClass::Resource);
    assert!(
        source
            .downcast_ref::<novarocks_workload_control::WorkError>()
            .is_some()
    );
    drop(source);
    fixture.close();
}

#[test]
fn required_unchanged_materialized_input_keeps_original_nonzero_selected_value() {
    let fixture = Fixture::new();
    let control = SqlCompileControl::unbounded();
    let array = Int32Array::from(vec![Some(8), Some(41), None]);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("source", DataType::Int32, true)),
        FunctionValueType::new(DataType::Int32, true),
        array.to_data(),
        crate::application::test_constant_policy(),
        CompilePhase::FunctionSpecialization,
        &control,
    )
    .unwrap();
    let original = pool.value(1).unwrap();
    let functions: Arc<dyn SqlFunctionCatalog> = fixture.metadata.clone();
    let prepared = prepare_required_constant(RequiredConstantRequest {
        input: RequiredConstantInput::Materialized(original.clone()),
        target: RequiredConstantTarget::Integer,
        catalog: &fixture.catalog,
        current_database: "db",
        functions,
        sql_semantics: &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        constant_policy: crate::application::test_constant_policy(),
        control: &control,
        version: novarocks_physical_plan::PlanVersionId::try_new(*uuid::Uuid::now_v7().as_bytes())
            .unwrap(),
    })
    .unwrap();
    let value = fixture
        .interpreter
        .evaluate(prepared, &fixture.root.owner.scope(), &fixture.window)
        .unwrap();
    assert_eq!(
        value.pool().backing_identity(),
        original.pool().backing_identity()
    );
    assert_eq!(value.ordinal(), 1);
    assert_eq!(value.try_i64().unwrap(), Some(41));
    drop(value);
    drop(original);
    drop(pool);
    fixture.close();
}

#[test]
fn required_package_author_data_and_control_keep_their_distinct_ports() {
    use novarocks_failure_contract::SharedFailureCause;
    let original = SharedFailureCause::new(novarocks_functions::RowDataError::with_reason(
        0,
        novarocks_functions::DataFailureReason::DivisionByZero,
        "original package author Data",
    ));
    let error = package_semantics_error(PackageSemanticsError::PrimitiveSource {
        source: original.clone(),
        cast: None,
    });
    let SqlConstantEvaluationError::Outer(outer) = error else {
        panic!("preparation Data must stay on the outer port");
    };
    assert_eq!(outer.failure_fact(), original.failure_fact());
    let Some(PackageSemanticsError::PrimitiveSource { source, .. }) =
        outer.downcast_ref::<PackageSemanticsError>()
    else {
        panic!("the original package source owner");
    };
    assert_eq!(source, &original);
    assert!(
        outer
            .downcast_ref::<RequiredConstantEligibilityFailure>()
            .is_none()
    );
    let control = CompileControlError::Source(original);
    assert!(
        matches!(package_semantics_error(PackageSemanticsError::Control(control.clone())), SqlConstantEvaluationError::Control(actual) if actual == control)
    );
}

#[test]
fn required_real_primitive_capability_refusal_is_not_constant_eligibility() {
    use novarocks_failure_contract::SharedFailureCause;
    use novarocks_functions::{CastOperation, CastPrepareError, PreparedCastRecipe};
    let control = SqlCompileControl::unbounded();
    // This exact installed cast author explicitly refuses DATE -> narrow int.
    // No function name or diagnostic text determines the refusal category.
    let source_type = FunctionValueType::new(DataType::Date32, false);
    let target_type = FunctionValueType::new(DataType::Int8, true);
    let refusal = PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &source_type,
        &target_type,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        false,
        None,
        &control,
    )
    .unwrap_err();
    assert_eq!(refusal, CastPrepareError::Unsupported);
    let original = SharedFailureCause::new(refusal);
    let error = package_semantics_error(PackageSemanticsError::PrimitiveSource {
        source: original.clone(),
        cast: Some((DataType::Date32, DataType::Int8)),
    });
    let SqlConstantEvaluationError::Outer(outer) = error else {
        panic!("real installed capability remains an outer refusal");
    };
    assert_eq!(outer.failure_fact(), original.failure_fact());
    let Some(PackageSemanticsError::PrimitiveSource { source, .. }) =
        outer.downcast_ref::<PackageSemanticsError>()
    else {
        panic!("original installed cast cause retained");
    };
    assert_eq!(source, &original);
    assert!(
        outer
            .downcast_ref::<RequiredConstantEligibilityFailure>()
            .is_none()
    );
    let eligibility = RequiredConstantEligibilityFailure::new();
    assert_eq!(
        eligibility.failure_fact().class(),
        FailureClass::Unsupported
    );
    assert!(
        std::error::Error::source(&eligibility)
            .unwrap()
            .downcast_ref::<PackageSemanticsError>()
            .is_some()
    );
    assert!(
        matches!(SqlConstantEvaluationError::from_domain(eligibility), SqlConstantEvaluationError::Source(source) if source.downcast_ref::<RequiredConstantEligibilityFailure>().is_some())
    );
}

impl Fixture {
    fn prepare_core_array(
        &self,
        sql: &str,
        control: &SqlCompileControl,
    ) -> PreparedRequiredConstant {
        let expression = novarocks_parser::trino_parser::parse_expression(sql).unwrap();
        let functions: Arc<dyn SqlFunctionCatalog> = self.metadata.clone();
        prepare_required_constant(RequiredConstantRequest {
            input: RequiredConstantInput::Expression(&expression),
            target: RequiredConstantTarget::ArrayVarchar,
            catalog: &self.catalog,
            current_database: "db",
            functions,
            sql_semantics: &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            constant_policy: crate::application::test_constant_policy(),
            control,
            version: novarocks_physical_plan::PlanVersionId::try_new(
                *uuid::Uuid::now_v7().as_bytes(),
            )
            .unwrap(),
        })
        .unwrap_or_else(|cause| panic!("{sql}: {cause}"))
    }
}

#[test]
fn required_array_constants_use_core_syntax_and_the_original_interpreter() {
    use novarocks_type_contract::ValueKind;
    let fixture = Fixture::new();
    let control = SqlCompileControl::unbounded();
    for (sql, expected) in [
        ("ARRAY['a',NULL]", Some(vec![Some("a"), None])),
        ("ARRAY[]", Some(vec![])),
        ("ARRAY[NULL]", Some(vec![None])),
        ("NULL", None),
        ("IF(TRUE,ARRAY['a'],ARRAY['b'])", Some(vec![Some("a")])),
    ] {
        let prepared = fixture.prepare_core_array(sql, &control);
        // This is the independent declared SQL target, including both NULL
        // layers; no expected type is copied from the evaluated Arrow carrier.
        let expected_type = FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            expected.is_none(),
        );
        assert_eq!(prepared.value_type(), &expected_type, "{sql}");
        let value = fixture
            .interpreter
            .evaluate(prepared, &fixture.root.owner.scope(), &fixture.window)
            .unwrap_or_else(|cause| panic!("{sql}: {cause}"));
        assert_eq!(value.value_type(), &expected_type, "{sql}");
        // Semantic admission precedes the borrowed carrier accessor. A List
        // of nominal JSON can have the same Utf8 storage and is not String.
        let ValueKind::List(child) = value.value_type().value_type().kind() else {
            panic!("{sql}: expected a semantic List")
        };
        assert!(matches!(child.kind(), ValueKind::String), "{sql}");
        assert!(child.nullable(), "{sql}");
        let actual = value
            .utf8_list_observed(CompilePhase::Validate, &control)
            .unwrap();
        match (expected.as_ref(), actual) {
            (None, None) => {}
            (Some(expected), Some(actual)) => {
                assert!(std::ptr::eq(actual.source(), &value));
                assert_eq!(actual.len(), expected.len(), "{sql}");
                for (index, expected) in expected.iter().enumerate() {
                    assert_eq!(
                        actual
                            .item(index, CompilePhase::Validate, &control)
                            .unwrap(),
                        *expected,
                        "{sql}: item {index}",
                    );
                }
            }
            _ => panic!("{sql}: root NULL differs from an empty or non-NULL array"),
        }
        drop(value);
        assert_eq!(fixture.window.retained_payload_bytes(), 0, "{sql}");
    }
    fixture.close();
}

#[test]
fn required_materialized_array_identity_keeps_full_field_type_pool_and_ordinal() {
    use arrow::{
        array::{ListArray, StringArray},
        buffer::OffsetBuffer,
    };
    use novarocks_type_contract::ValueKind;
    use std::collections::HashMap;
    let fixture = Fixture::new();
    let control = SqlCompileControl::unbounded();
    for nullable in [false, true] {
        let child_field = Arc::new(
            Field::new("authored_items", DataType::Utf8, nullable)
                .with_metadata(HashMap::from([("child-owned".into(), "original".into())])),
        );
        let children = if nullable {
            StringArray::from(vec![Some("prefix"), Some("selected"), None])
        } else {
            StringArray::from(vec![Some("prefix"), Some("selected")])
        };
        let end = if nullable { 3 } else { 2 };
        let array = ListArray::try_new(
            child_field.clone(),
            OffsetBuffer::new(vec![0_i32, 1, end].into()),
            Arc::new(children),
            None,
        )
        .unwrap();
        let field = Arc::new(
            Field::new("authored_array", array.data_type().clone(), nullable)
                .with_metadata(HashMap::from([("source-owned".into(), "original".into())])),
        );
        let value_type = FunctionValueType::try_from_field(&field).unwrap();
        let pool = ConstantPool::try_new(
            field.clone(),
            value_type.clone(),
            array.to_data(),
            crate::application::test_constant_policy(),
            CompilePhase::FunctionSpecialization,
            &control,
        )
        .unwrap();
        let original = pool.value(1).unwrap();
        let backing = pool.backing_identity();
        let functions: Arc<dyn SqlFunctionCatalog> = fixture.metadata.clone();
        let prepared = prepare_required_constant(RequiredConstantRequest {
            input: RequiredConstantInput::Materialized(original.clone()),
            target: RequiredConstantTarget::ArrayVarchar,
            catalog: &fixture.catalog,
            current_database: "db",
            functions,
            sql_semantics: &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            constant_policy: crate::application::test_constant_policy(),
            control: &control,
            version: novarocks_physical_plan::PlanVersionId::try_new(
                *uuid::Uuid::now_v7().as_bytes(),
            )
            .unwrap(),
        })
        .unwrap();
        assert_eq!(prepared.value_type(), &value_type);
        let retained = prepared.original_value().expect("unchanged admitted array");
        assert_eq!(retained.ordinal(), 1);
        assert_eq!(retained.pool().backing_identity(), backing);
        assert!(Arc::ptr_eq(retained.pool().field_ref(), &field));
        let value = fixture
            .interpreter
            .evaluate(prepared, &fixture.root.owner.scope(), &fixture.window)
            .unwrap();
        assert_eq!(value.pool().backing_identity(), backing);
        assert_eq!(value.ordinal(), 1);
        assert!(Arc::ptr_eq(value.pool().field_ref(), &field));
        assert_eq!(value.value_type(), &value_type);
        assert!(
            value
                .value_type()
                .value_type()
                .shares_storage(value_type.value_type())
        );
        assert!(
            value
                .value_type()
                .typed_binding()
                .layout()
                .shares_storage(value_type.typed_binding().layout())
        );
        assert_eq!(value.value_type().nullable(), nullable);
        let ValueKind::List(child) = value.value_type().value_type().kind() else {
            panic!()
        };
        assert!(matches!(child.kind(), ValueKind::String));
        assert_eq!(child.nullable(), nullable);
        assert_eq!(value.field().metadata()["source-owned"], "original");
        let DataType::List(actual_child) = value.field().data_type() else {
            panic!()
        };
        assert!(Arc::ptr_eq(actual_child, &child_field));
        assert_eq!(actual_child.metadata()["child-owned"], "original");
        {
            let view = value
                .utf8_list_observed(CompilePhase::Validate, &control)
                .unwrap()
                .unwrap();
            assert!(std::ptr::eq(view.source(), &value));
            assert_eq!(view.len(), if nullable { 2 } else { 1 });
            assert_eq!(
                view.item(0, CompilePhase::Validate, &control).unwrap(),
                Some("selected")
            );
            if nullable {
                assert_eq!(
                    view.item(1, CompilePhase::Validate, &control).unwrap(),
                    None
                );
            }
            let first = pool.value(0).unwrap();
            let first_view = first
                .utf8_list_observed(CompilePhase::Validate, &control)
                .unwrap()
                .unwrap();
            assert_eq!(first_view.len(), 1);
            assert_eq!(
                first_view
                    .item(0, CompilePhase::Validate, &control)
                    .unwrap(),
                Some("prefix")
            );
        }
        assert_eq!(original.ordinal(), 1);
        assert_eq!(original.pool().backing_identity(), backing);
        assert_eq!(original.value_type(), &value_type);
        assert!(Arc::ptr_eq(original.pool().field_ref(), &field));
        drop(value);
        drop(original);
        drop(pool);
        assert_eq!(fixture.window.retained_payload_bytes(), 0);
    }
    fixture.close();
}

#[test]
fn required_unknown_array_conversion_keeps_input_source_without_retagging_its_pool() {
    use arrow::{
        array::{ListArray, NullArray},
        buffer::OffsetBuffer,
    };
    use novarocks_type_contract::ValueKind;
    use std::collections::HashMap;
    let fixture = Fixture::new();
    let control = SqlCompileControl::unbounded();
    let child_field = Arc::new(
        Field::new("unknown_items", DataType::Null, true).with_metadata(HashMap::from([(
            "child-owned".into(),
            "unknown-source".into(),
        )])),
    );
    // Distinct original rows: [] and [NULL]. Selecting ordinal 1 must retain
    // the second row while its actual conversion creates a String child type.
    let array = ListArray::try_new(
        child_field.clone(),
        OffsetBuffer::new(vec![0_i32, 0, 1].into()),
        Arc::new(NullArray::new(1)),
        None,
    )
    .unwrap();
    let field = Arc::new(
        Field::new("unknown_array", array.data_type().clone(), false).with_metadata(HashMap::from(
            [("source-owned".into(), "unknown-source".into())],
        )),
    );
    let source_type = FunctionValueType::try_from_field(&field).unwrap();
    let pool = ConstantPool::try_new(
        field.clone(),
        source_type.clone(),
        array.to_data(),
        crate::application::test_constant_policy(),
        CompilePhase::FunctionSpecialization,
        &control,
    )
    .unwrap();
    let original = pool.value(1).unwrap();
    let backing = pool.backing_identity();
    let functions: Arc<dyn SqlFunctionCatalog> = fixture.metadata.clone();
    let prepared = prepare_required_constant(RequiredConstantRequest {
        input: RequiredConstantInput::Materialized(original.clone()),
        target: RequiredConstantTarget::ArrayVarchar,
        catalog: &fixture.catalog,
        current_database: "db",
        functions,
        sql_semantics: &novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        constant_policy: crate::application::test_constant_policy(),
        control: &control,
        version: novarocks_physical_plan::PlanVersionId::try_new(*uuid::Uuid::now_v7().as_bytes())
            .unwrap(),
    })
    .unwrap();
    assert!(
        prepared.original_value().is_none(),
        "conversion is not an identity CV"
    );
    let expected_type = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        false,
    );
    assert_eq!(prepared.value_type(), &expected_type);
    let value = fixture
        .interpreter
        .evaluate(prepared, &fixture.root.owner.scope(), &fixture.window)
        .unwrap();
    assert_eq!(value.value_type(), &expected_type);
    assert_ne!(value.pool().backing_identity(), backing);
    let ValueKind::List(child) = value.value_type().value_type().kind() else {
        panic!()
    };
    assert!(matches!(child.kind(), ValueKind::String));
    {
        let view = value
            .utf8_list_observed(CompilePhase::Validate, &control)
            .unwrap()
            .unwrap();
        assert_eq!(view.len(), 1);
        assert_eq!(
            view.item(0, CompilePhase::Validate, &control).unwrap(),
            None
        );
    }
    // Only the original input owns its old Field/metadata and selected ordinal.
    // The output's new Field is authored by the actual compiled conversion.
    assert_eq!(original.ordinal(), 1);
    assert_eq!(original.pool().backing_identity(), backing);
    assert_eq!(original.value_type(), &source_type);
    assert!(Arc::ptr_eq(original.pool().field_ref(), &field));
    assert_eq!(
        original.field().metadata()["source-owned"],
        "unknown-source"
    );
    let DataType::List(original_child) = original.field().data_type() else {
        panic!()
    };
    assert!(Arc::ptr_eq(original_child, &child_field));
    assert_eq!(original_child.metadata()["child-owned"], "unknown-source");
    assert!(
        matches!(original.value_type().value_type().kind(), ValueKind::List(child) if matches!(child.kind(), ValueKind::Scalar(DataType::Null)))
    );
    drop(value);
    drop(original);
    drop(pool);
    fixture.close();
}

impl Fixture {
    fn evaluate_ddl_property(
        &self,
        source: &str,
        value_type: novarocks_spi::connector::DdlPropertyType,
        control: &SqlCompileControl,
    ) -> Result<
        novarocks_query_application::sql::property_evaluation::EvaluatedDdlProperties,
        novarocks_query_application::session_error::QueryServiceError,
    > {
        use novarocks_query_application::sql::{
            property_admit::DdlPropertyScope,
            property_evaluation::{
                DdlPropertyAssignment, DdlPropertyAssignmentValue, DdlPropertyEvaluationMode,
                evaluate_ddl_properties,
            },
        };
        use novarocks_spi::connector::{DdlPropertyDeclaration, DdlPropertyValue};
        let expression = novarocks_parser::trino_parser::parse_expression(source).unwrap();
        let declaration =
            DdlPropertyDeclaration::new("value", value_type, DdlPropertyValue::Null, false)
                .unwrap();
        let assignments = [DdlPropertyAssignment {
            name: "value",
            name_span: expression.span(),
            span: expression.span(),
            value: DdlPropertyAssignmentValue::Expression(&expression),
        }];
        let semantics = novarocks_sql::sql_mode::SqlSemanticSettings::default();
        let scope = self.root.owner.scope();
        let mut port = crate::query_execution::ddl_properties::DdlPropertyConstantEvaluator {
            catalog: &self.catalog,
            current_database: "db",
            functions: self.metadata.clone(),
            sql_semantics: &semantics,
            constant_policy: crate::application::test_constant_policy(),
            control,
            interpreter: &self.interpreter,
            scope: &scope,
            window: &self.window,
        };
        evaluate_ddl_properties(
            DdlPropertyScope::Table,
            DdlPropertyEvaluationMode::Create,
            source,
            &[declaration],
            &assignments,
            &mut port,
            control,
        )
    }
}

#[test]
fn required_ddl_property_port_runs_real_scalar_array_and_structural_map_inputs() {
    use novarocks_query_application::sql::property_evaluation::DdlPropertyChange;
    use novarocks_spi::connector::{DdlPropertyType as T, DdlPropertyValue as V};
    let fixture = Fixture::new();
    let control = SqlCompileControl::unbounded();
    for (source, target, expected) in [
        ("1 + 2", T::Integer, V::Integer(3)),
        ("false AND (1 / 0 = 1)", T::Boolean, V::Boolean(false)),
        ("CAST(1 AS BIGINT) + 2", T::Bigint, V::Bigint(3)),
        ("CAST(3 AS DOUBLE) / 2", T::Double, V::Double(1.5)),
        (
            "IF(TRUE, 'selected', 'other')",
            T::Varchar,
            V::Varchar("selected".into()),
        ),
        (
            "IF(TRUE, ARRAY['selected',NULL], ARRAY['other'])",
            T::ArrayVarchar,
            V::ArrayVarchar(vec![Some("selected".into()), None]),
        ),
        (
            "MAP(ARRAY['first','second'],ARRAY['value',NULL])",
            T::MapVarcharVarchar,
            V::MapVarcharVarchar(vec![
                ("first".into(), Some("value".into())),
                ("second".into(), None),
            ]),
        ),
    ] {
        let properties = fixture
            .evaluate_ddl_property(source, target, &control)
            .unwrap_or_else(|error| panic!("{source}: {error}"));
        let [property] = properties.properties() else {
            panic!("one evaluated property: {source}")
        };
        assert_eq!(property.name, "value");
        assert_eq!(
            property.change,
            DdlPropertyChange::Set(expected),
            "{source}"
        );
        drop(properties);
        assert_eq!(fixture.window.retained_payload_bytes(), 0, "{source}");
    }
    fixture.close();
}

#[test]
fn required_ddl_property_port_projects_real_input_errors_and_retains_control_resource() {
    use novarocks_spi::connector::DdlPropertyType;
    let fixture = Fixture::new();
    let control = SqlCompileControl::unbounded();
    for source in ["NULL", "missing_column", "(SELECT 1)", "1 / 0", "1.5"] {
        let error = fixture
            .evaluate_ddl_property(source, DdlPropertyType::Integer, &control)
            .unwrap_err();
        let user = error
            .user_error()
            .unwrap_or_else(|| panic!("{source}: {error}"));
        assert_eq!(
            user.code().as_str(),
            "sql.admit.invalid_table_property",
            "{source}"
        );
        assert!(user.location().is_some(), "{source}");
        drop(error);
    }
    struct Stop;
    impl novarocks_sql::compiler::SqlCancellationObservation for Stop {
        fn is_cancelled(&self) -> bool {
            true
        }
    }
    let cancelled = SqlCompileControl::new(None, Arc::new(Stop));
    let error = fixture
        .evaluate_ddl_property("1 + 2", DdlPropertyType::Integer, &cancelled)
        .unwrap_err();
    assert_eq!(
        error.compile_control_error(),
        Some(CompileControlError::Cancelled)
    );
    drop(error);
    fixture.close();

    let mut capacity = ResultCapacityConfig::V1;
    capacity.all_objects_bytes[1] = 1;
    let fixture = Fixture::with_capacity(capacity);
    let error = fixture
        .evaluate_ddl_property("1 + 2", DdlPropertyType::Integer, &control)
        .unwrap_err();
    let Some(CompileControlError::Source(source)) = error.compile_control_error() else {
        panic!("original result capacity failure")
    };
    assert_eq!(source.failure_fact().class(), FailureClass::Resource);
    assert!(
        source
            .downcast_ref::<novarocks_workload_control::WorkError>()
            .is_some()
    );
    assert!(
        error
            .user_error()
            .is_none_or(|user| user.code().as_str() != "sql.admit.invalid_table_property")
    );
    drop(error);
    drop(source);
    fixture.close();
}

#[test]
fn required_array_child_buffers_keep_original_result_custody_until_last_alias_exits() {
    use arrow::array::{ListArray, StringArray};
    let fixture = Fixture::new();
    let prepared = fixture.prepare_core_array(
        "ARRAY['selected child', NULL]",
        &SqlCompileControl::unbounded(),
    );
    let value = fixture
        .interpreter
        .evaluate(prepared, &fixture.root.owner.scope(), &fixture.window)
        .unwrap();
    let alias = value.pool().array().clone();
    drop(value);
    assert!(
        fixture.window.retained_payload_bytes() > 0,
        "the real Array alias still owns its original result custody"
    );
    {
        let array = alias.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(array.value_offsets(), &[0, 2]);
        let items = array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(items.value(0), "selected child");
        assert!(items.is_null(1));
    }
    drop(alias);
    assert_eq!(fixture.window.retained_payload_bytes(), 0);
    fixture.close();
}
