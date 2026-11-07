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
