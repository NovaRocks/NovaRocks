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

//! Folded SQL constants and unfolded LocalCompiler roots are checked against
//! independent expected values. Rule exclusions keep the same runtime result.

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int8Array,
        Int32Array, Int64Array, StringArray,
    },
    datatypes::{DataType, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{CompiledExprSet, StaticExprKind};
use novarocks_expression_contract::RootOrdinal;
use novarocks_functions::testkit::{
    installed_builtin_owner_catalogue, shared_installed_builtin_owner_catalogue,
};
use novarocks_functions::{
    CastOperation, ConstantPolicy, FunctionArgumentType, FunctionValueType,
    KernelEvaluationControl, KernelFailure, PreparedPureKernel, PureEngineFunctionCatalog,
    SelectedValues, Selection,
};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef, ProgramUnitId};
use novarocks_sql_testkit::{sql_source_with_folding_and_semantics, sql_source_with_semantics};
use novarocks_test_compile_to_execute::programs::programs_with_catalogue;

use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ExpressionUseId, OperatorFunction, PureCompileControl,
};
use std::{sync::Arc, time::Duration};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("a constant expression cannot wait")
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
#[derive(Clone, Copy, Debug)]
enum Family {
    Constant,
    Operator(OperatorFunction),
    And,
    Or,
    Not,
    Carrier,
    Call(&'static str),
}
impl Family {
    fn matches(self, kind: &StaticExprKind, function: Option<&str>) -> bool {
        match self {
            Self::Constant => matches!(kind, StaticExprKind::Constant(_)),
            Self::Operator(operator) => {
                Self::Call(operator.function_id_text()).matches(kind, function)
            }
            Self::And => matches!(kind, StaticExprKind::NaryAnd { .. }),
            Self::Or => matches!(kind, StaticExprKind::NaryOr { .. }),
            Self::Not => matches!(kind, StaticExprKind::Not(_)),
            Self::Carrier => matches!(
                kind,
                StaticExprKind::PreparedCast {
                    operation: CastOperation::Carrier,
                    ..
                }
            ),
            Self::Call(expected) => {
                matches!(kind, StaticExprKind::BoundCall { .. }) && function == Some(expected)
            }
        }
    }
}

struct Fixture {
    program: Arc<LocalProgram>,
    root: ProgramRootRef,
    occurrence: ExpressionUseId,
    sql: String,
    family: Family,
    semantics: SqlSemanticSettings,
}
impl Fixture {
    fn compile(
        expression: &str,
        family: Family,
        mode: &str,
        functions: &PureEngineFunctionCatalog,
    ) -> Self {
        let sql = format!("SELECT {expression} AS entry_parity");
        let semantics =
            SqlSemanticSettings::default().with_sql_mode(SqlMode::from_assignment(mode));
        let source = sql_source_with_semantics(&sql, DataType::Int64, semantics.clone());
        Self::find_root(source, family, sql, semantics, functions)
    }

    fn find_root(
        source: novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
        family: Family,
        sql: String,
        semantics: SqlSemanticSettings,
        functions: &PureEngineFunctionCatalog,
    ) -> Self {
        let declared = source.plan().result_port().unwrap().fields[0].ty.clone();
        assert_eq!(
            &declared,
            &source
                .original_public_result_declaration()
                .unwrap()
                .fields()[0]
                .ty,
            "original SQL public declaration remains exact: {sql}"
        );
        let programs = programs_with_catalogue(&source, functions);
        let mut found = Vec::new();
        for program in programs.values() {
            assert!(
                program
                    .graph()
                    .nodes()
                    .iter()
                    .all(|node| !matches!(node.kind(), ProgramNodeKind::Scan { .. })),
                "constant-only SQL must not contain a scan: {sql}"
            );
            for (unit_ordinal, unit) in program.units().iter().enumerate() {
                for (root_ordinal, compiled_root) in unit.roots().iter().enumerate() {
                    let root = ProgramRootRef {
                        unit: ProgramUnitId::new(unit_ordinal),
                        root: RootOrdinal::new(u32::try_from(root_ordinal).unwrap()),
                    };
                    let occurrence = compiled_root.use_id;
                    let definition = compiled_root.definition;
                    let node = unit.definitions().node(definition).unwrap();
                    let call = unit.calls().get(&occurrence);
                    if family.matches(
                        node.kind(),
                        call.map(|c| c.call_contract().function_id().as_str()),
                    ) {
                        let FunctionArgumentType::Value(actual) = &unit.types()[definition.index()]
                        else {
                            panic!("the actual SQL root is a scalar value")
                        };
                        assert_eq!(actual, &declared, "SQL/program complete root type: {sql}");
                        let schema = unit.scope(compiled_root.scope).unwrap().schema();
                        assert!(schema.fields().is_empty(), "empty original input: {sql}");
                        found.push(Self {
                            program: program.clone(),
                            root,
                            occurrence,
                            sql: sql.clone(),
                            family,
                            semantics: semantics.clone(),
                        });
                    }
                }
            }
        }
        assert_eq!(
            found.len(),
            1,
            "one actual {family:?} root must occur in the SQL-authored program: {sql}"
        );
        found.pop().unwrap()
    }
    fn unit(&self) -> &CompiledExprSet {
        self.program.unit(self.root.unit).unwrap()
    }
    fn kind(&self, at: ExpressionUseId) -> &StaticExprKind {
        let definition = self.unit().control_flow().uses()[&at].definition;
        self.unit().definitions().node(definition).unwrap().kind()
    }
    fn ty(&self, at: ExpressionUseId) -> &FunctionValueType {
        let definition = self.unit().control_flow().uses()[&at].definition;
        let FunctionArgumentType::Value(ty) = &self.unit().types()[definition.index()] else {
            panic!("value occurrence required")
        };
        ty
    }
    fn args(&self, at: ExpressionUseId) -> Vec<ExpressionUseId> {
        self.unit().control_flow().uses()[&at].arguments.to_vec()
    }
    fn call(&self, at: ExpressionUseId) -> &PreparedPureKernel {
        self.unit().calls()[&at].prepared()
    }
    fn source_before_coercion(
        &self,
        mut at: ExpressionUseId,
        source: &DataType,
    ) -> ExpressionUseId {
        while self.ty(at).data_type() != source {
            match self.kind(at) {
                StaticExprKind::PreparedCast {
                    operation: CastOperation::Carrier,
                    ..
                } => {}
                StaticExprKind::BoundCall { .. } => assert_eq!(
                    self.call(at).call_contract().function_id().as_str(),
                    "builtin.scalar/value_domain_conversion/v1",
                ),
                other => panic!("missing real conversion to {source:?} from {other:?}"),
            }
            let args = self.args(at);
            let [child] = args.as_slice() else {
                panic!("unary conversion")
            };
            at = *child;
        }
        at
    }
    fn run(&self) -> SelectedValues<'static> {
        let compiled_root = self.unit().root(self.root.root).unwrap();
        let schema = self
            .unit()
            .scope(compiled_root.scope)
            .unwrap()
            .schema()
            .clone();
        assert!(schema.fields().is_empty());
        let batch = RecordBatch::try_new_with_options(
            schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let mut instance = CompiledExpressionInstance::try_new(
            self.program.unit(self.root.unit).unwrap().clone(),
            self.root.root,
            &Control,
        )
        .unwrap();
        instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap()
    }

    fn assert_both(
        &self,
        functions: &PureEngineFunctionCatalog,
        outcome: FoldOutcome,
        expected: Expected,
    ) {
        let enabled_source = sql_source_with_folding_and_semantics(
            &self.sql,
            DataType::Int64,
            self.semantics.clone(),
        );
        let enabled = Self::find_root(
            enabled_source,
            match outcome {
                FoldOutcome::Folded => Family::Constant,
                FoldOutcome::Kept => self.family,
            },
            self.sql.clone(),
            self.semantics.clone(),
            functions,
        );
        assert_eq!(
            self.ty(self.occurrence),
            enabled.ty(enabled.occurrence),
            "folding keeps the original declared type: {}",
            self.sql,
        );
        let disabled = self.run();
        let enabled = enabled.run();
        match expected {
            Expected::Value(expected) => {
                for output in [&disabled, &enabled] {
                    assert!(
                        output.errors().is_empty(),
                        "unexpected runtime row error: {}",
                        self.sql,
                    );
                    assert_array(output.values(), &expected);
                }
                assert_array(disabled.values(), enabled.values());
            }
            Expected::RowError(message) => {
                for output in [&disabled, &enabled] {
                    let [actual] = output.errors() else {
                        panic!("expected one row error: {}", self.sql)
                    };
                    assert_eq!(actual.selected_ordinal(), 0);
                    assert_eq!(actual.message(), message);
                    assert!(
                        output.values().is_null(0),
                        "failed row has NULL placeholder"
                    );
                }
                assert_eq!(disabled.errors(), enabled.errors());
            }
        }
    }
}

#[derive(Clone, Copy)]
enum FoldOutcome {
    Folded,
    Kept,
}

enum Expected {
    Value(ArrayRef),
    RowError(&'static str),
}
fn assert_array(actual: &ArrayRef, expected: &ArrayRef) {
    assert_eq!(actual.data_type(), expected.data_type());
    assert_eq!(actual.len(), expected.len());
    assert_eq!(actual.to_data(), expected.to_data());
    // Array semantic equality is insufficient for a promised floating bit pattern.
    if let (Some(actual), Some(expected)) = (
        actual.as_any().downcast_ref::<Float64Array>(),
        expected.as_any().downcast_ref::<Float64Array>(),
    ) {
        for row in 0..actual.len() {
            if !actual.is_null(row) {
                assert_eq!(actual.value(row).to_bits(), expected.value(row).to_bits());
            }
        }
    }
    if let (Some(actual), Some(expected)) = (
        actual.as_any().downcast_ref::<Float32Array>(),
        expected.as_any().downcast_ref::<Float32Array>(),
    ) {
        for row in 0..actual.len() {
            if !actual.is_null(row) {
                assert_eq!(actual.value(row).to_bits(), expected.value(row).to_bits());
            }
        }
    }
}
fn int(value: Option<i32>) -> ArrayRef {
    Arc::new(Int32Array::from(vec![value]))
}
fn bigint(value: Option<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(vec![value]))
}
fn boolean(value: Option<bool>) -> ArrayRef {
    Arc::new(BooleanArray::from(vec![value]))
}
fn text(value: &str) -> ArrayRef {
    Arc::new(StringArray::from(vec![value]))
}
fn float(value: f64) -> ArrayRef {
    Arc::new(Float64Array::from(vec![value]))
}
fn decimal(value: Option<i128>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(vec![value])
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

#[test]
fn constant_entry_every_family_matches_real_noop_sql_constant_root() {
    let functions = shared_installed_builtin_owner_catalogue();
    metadata_cv_source_cross_entry(&functions, false);
    metadata_cv_source_cross_entry(&functions, true);
    let cases: Vec<(&str, Family, bool, ArrayRef)> = vec![
        (
            "20 + 22",
            Family::Operator(OperatorFunction::Add),
            false,
            int(Some(42)),
        ),
        (
            "1e0 * 1e0",
            Family::Operator(OperatorFunction::Multiply),
            false,
            float(1.0),
        ),
        (
            "20 < 22",
            Family::Operator(OperatorFunction::LessThan),
            false,
            boolean(Some(true)),
        ),
        (
            "1 IS NOT DISTINCT FROM 1",
            Family::Operator(OperatorFunction::Identical),
            false,
            boolean(Some(true)),
        ),
        ("FALSE AND TRUE", Family::And, false, boolean(Some(false))),
        ("TRUE OR FALSE", Family::Or, false, boolean(Some(true))),
        ("NOT TRUE", Family::Not, false, boolean(Some(false))),
        (
            "CAST(42 AS TINYINT)",
            Family::Carrier,
            false,
            Arc::new(Int8Array::from(vec![42])),
        ),
        (
            "upper('AbC')",
            Family::Call("builtin.scalar/upper/v1"),
            false,
            text("ABC"),
        ),
        (
            "if(FALSE, 11, 22)",
            Family::Call("builtin.scalar/if/v1"),
            false,
            int(Some(22)),
        ),
        (
            "coalesce(11, 22)",
            Family::Call("builtin.scalar/coalesce/v1"),
            false,
            int(Some(11)),
        ),
    ];
    for (expression, family, nullable, expected) in cases {
        let fixture = Fixture::compile(expression, family, "32", &functions);
        assert_eq!(
            fixture.ty(fixture.occurrence).nullable(),
            nullable,
            "frozen root: {expression}"
        );
        for at in fixture.args(fixture.occurrence) {
            assert!(
                matches!(fixture.kind(at), StaticExprKind::Constant(_)),
                "T1 uses one actual node over constant leaves: {expression}"
            );
            if matches!(family, Family::Call("builtin.scalar/upper/v1")) {
                assert_eq!(
                    fixture.ty(at).value_type().kind(),
                    &novarocks_type_contract::ValueKind::String
                );
            }
        }
        fixture.assert_both(&functions, FoldOutcome::Folded, Expected::Value(expected));
    }
    // Compare the sign bit for subtraction, Trino's signed literal, and a
    // cast of signed text.
    for (expression, family, nullable, outcome, expected) in [
        (
            "(0e0 - 0e0) * 1e0",
            Family::Operator(OperatorFunction::Multiply),
            false,
            FoldOutcome::Folded,
            float(0.0),
        ),
        (
            "-0e0 * 1e0",
            Family::Operator(OperatorFunction::Multiply),
            false,
            FoldOutcome::Folded,
            float(-0.0),
        ),
        (
            "CAST('-0' AS DOUBLE) * 1e0",
            Family::Operator(OperatorFunction::Multiply),
            false,
            FoldOutcome::Folded,
            float(-0.0),
        ),
        (
            "TRUE AND CAST(NULL AS BOOLEAN)",
            Family::And,
            true,
            FoldOutcome::Folded,
            boolean(None),
        ),
        (
            "if(FALSE, CAST(NULL AS BIGINT), 22)",
            Family::Call("builtin.scalar/if/v1"),
            true,
            FoldOutcome::Folded,
            bigint(Some(22)),
        ),
        (
            "coalesce(CAST(NULL AS BIGINT), CAST(NULL AS BIGINT))",
            Family::Call("builtin.scalar/coalesce/v1"),
            true,
            FoldOutcome::Folded,
            bigint(None),
        ),
        (
            "CAST(NULL AS BIGINT) IS NOT DISTINCT FROM CAST(NULL AS BIGINT)",
            Family::Operator(OperatorFunction::Identical),
            false,
            FoldOutcome::Folded,
            boolean(Some(true)),
        ),
        (
            "NOT CAST(NULL AS BOOLEAN)",
            Family::Not,
            true,
            FoldOutcome::Folded,
            boolean(None),
        ),
    ] {
        let fixture = Fixture::compile(expression, family, "32", &functions);
        assert_eq!(
            fixture.ty(fixture.occurrence).nullable(),
            nullable,
            "frozen root: {expression}"
        );
        fixture.assert_both(&functions, outcome, Expected::Value(expected));
    }
}

#[test]
fn constant_entry_standard_widths_follow_original_catalogue_selection() {
    let functions = installed_builtin_owner_catalogue();
    for (expression, operator, result) in [
        (
            "DECIMAL '1.00' / DECIMAL '3.00'",
            OperatorFunction::Divide,
            decimal(Some(333_333), 9, 6),
        ),
        (
            "CAST(0 AS REAL) + DECIMAL '1.0000000596046447753906250000000000001'",
            OperatorFunction::Add,
            Arc::new(Float32Array::from(vec![f32::from_bits(0x3f80_0001)])) as ArrayRef,
        ),
        (
            "0e0 + DECIMAL '1801439850948188.9'",
            OperatorFunction::Add,
            float(f64::from_bits(0x4319_9999_9999_9974)),
        ),
    ] {
        let fixture = Fixture::compile(expression, Family::Operator(operator), "32", &functions);
        fixture.assert_both(&functions, FoldOutcome::Folded, Expected::Value(result));
    }
}

#[test]
fn constant_entry_heterogeneous_comparisons_replay_actual_sql_conversion_order() {
    let functions = installed_builtin_owner_catalogue();
    let cases = [
        (
            "CAST('1970-01-02' AS DATE) < CAST('1970-01-03 00:00:00' AS TIMESTAMP(6))",
            DataType::Date32,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            // The timestamp value is outside the rule's result-literal domain.
            FoldOutcome::Kept,
        ),
        (
            "CAST('12.3' AS DECIMAL(4,1)) < CAST('12.34' AS DECIMAL(6,2))",
            DataType::Decimal128(4, 1),
            DataType::Decimal128(6, 2),
            FoldOutcome::Folded,
        ),
        (
            "12 < CAST('12.34' AS DECIMAL(6,2))",
            DataType::Int32,
            DataType::Decimal128(6, 2),
            FoldOutcome::Folded,
        ),
    ];
    for (expression, left_source, right_source, outcome) in cases {
        let fixture = Fixture::compile(
            expression,
            Family::Operator(OperatorFunction::LessThan),
            "32",
            &functions,
        );
        let args = fixture.args(fixture.occurrence);
        let [left_at, right_at] = args.as_slice() else {
            panic!("binary comparison")
        };
        assert_ne!(left_source, right_source, "original heterogeneous inputs");
        let original_left = fixture.source_before_coercion(*left_at, &left_source);
        let original_right = fixture.source_before_coercion(*right_at, &right_source);
        assert!(
            original_left != *left_at || original_right != *right_at,
            "real lowering must retain at least one conversion: {expression}"
        );
        assert_eq!(
            fixture.ty(*left_at).data_type(),
            fixture.ty(*right_at).data_type(),
            "conversion destinations keep the same exact carrier"
        );
        assert_eq!(
            fixture.ty(fixture.occurrence).nullable(),
            fixture.ty(*left_at).nullable() || fixture.ty(*right_at).nullable(),
            "comparison root uses both actual converted nullable facts"
        );
        fixture.assert_both(&functions, outcome, Expected::Value(boolean(Some(true))));
    }
}

#[test]
fn constant_entry_decimal_policy_cross_allow_matrix_matches_independent_row_error_contract() {
    use OperatorFunction::{Add, Multiply};
    let functions = installed_builtin_owner_catalogue();
    let max = 10_i128.pow(38) - 1;
    for mode in [
        "32",
        "ALLOW_THROW_EXCEPTION",
        "ERROR_IF_OVERFLOW",
        "ERROR_IF_OVERFLOW,ALLOW_THROW_EXCEPTION",
    ] {
        for (operator, rhs) in [(Add, 1), (Multiply, 2)] {
            let symbol = operator.sql();
            let expression =
                format!("CAST('{max}' AS DECIMAL(38,0)) {symbol} CAST({rhs} AS DECIMAL(2,0))");
            let fixture =
                Fixture::compile(&expression, Family::Operator(operator), mode, &functions);
            let kernel = fixture.call(fixture.occurrence);
            assert_eq!(
                kernel.call_contract().function_id(),
                &operator.function_id()
            );
            assert_eq!(
                fixture.ty(fixture.occurrence).data_type(),
                &DataType::Decimal128(38, 0)
            );
            fixture.assert_both(
                &functions,
                FoldOutcome::Kept,
                Expected::RowError("Decimal overflow"),
            );
        }
        let fixture = Fixture::compile(
            "CAST(CAST('999.99' AS DECIMAL(5,2)) AS DECIMAL(4,1))",
            Family::Carrier,
            mode,
            &functions,
        );
        let StaticExprKind::PreparedCast {
            operation: CastOperation::Carrier,
            cast_mode,
            ..
        } = fixture.kind(fixture.occurrence)
        else {
            panic!("actual decimal carrier cast")
        };
        assert_eq!(*cast_mode, novarocks_type_contract::TrinoCastMode::Explicit);
        assert_eq!(
            fixture.ty(fixture.occurrence).data_type(),
            &DataType::Decimal128(4, 1)
        );
        let expected = Expected::RowError("Cast value is outside the target numeric range");
        let outcome = FoldOutcome::Kept;
        fixture.assert_both(&functions, outcome, expected);
    }
}

// The source is a REAL checked CV and enters the actual SQL compiler before
// package/LocalCompiler/Frame. No frozen call, program, output, or capability is forged.
fn metadata_cv_source_cross_entry(functions: &Arc<PureEngineFunctionCatalog>, spare_empty: bool) {
    use arrow::array::{Int32Array, ListArray};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::Field;
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
        SqlCompileProgress, SqlCompiler, SqlConstantExpressionSite, SqlExpressionConstant,
        SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
        builtin_sql_function_catalog,
    };
    let metadata = if spare_empty {
        let mut staging = std::collections::HashMap::<String, String>::with_capacity(16);
        staging.insert("removed".into(), "source".into());
        staging.clear();
        assert!(staging.capacity() >= 16);
        staging
    } else {
        [("provider.fact".into(), "kept".into())].into()
    };
    let raw_field = Arc::new(
        Field::new(
            if spare_empty { "item" } else { "source" },
            DataType::Int32,
            true,
        )
        .with_metadata(metadata),
    );
    let raw: ArrayRef = Arc::new(ListArray::new(
        raw_field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 4])),
        Arc::new(Int32Array::from(vec![111, 22, 33, 999])),
        None,
    ));
    let ty = FunctionValueType::new(raw.data_type().clone(), false);
    let pool = novarocks_functions::ConstantPool::try_new(
        Arc::new(Field::new("actual-source", raw.data_type().clone(), false)),
        ty.clone(),
        raw.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let original = pool.value(1).unwrap();
    let source_identity = pool.backing_identity();
    let query = novarocks_sql_testkit::parse_query("SELECT NULL AS metadata_cv_source");
    let control = SqlCompileControl::unbounded();
    let statement = SqlStatementInput::query_with_expression_constants(
        query,
        vec![SqlExpressionConstant::new(
            vec![],
            SqlConstantExpressionSite::SelectProjection { column: 0 },
            original.clone(),
        )]
        .into(),
        &control,
    )
    .unwrap();
    let request = SqlFinalPlanCompileRequest::new(
        novarocks_physical_plan::PlanVersionId::try_new([69; 16]).unwrap(),
        statement,
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                ..SessionOptimizerSettings::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        Arc::clone(functions),
        policy(),
        control.clone(),
        novarocks_physical_plan::PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        DEFAULT_COMPLETION_LIMITS,
    );
    let SqlCompileProgress::Complete(completed) =
        SqlCompiler::start(request.try_into_completion().unwrap(), &control).unwrap()
    else {
        panic!("a source-only constant query requires no external facts")
    };
    let source = completed.into_plan();
    let programs = programs_with_catalogue(&source, functions);
    let mut found = 0;
    for program in programs.values() {
        for unit in program.units() {
            for (root_ordinal, root) in unit.roots().iter().enumerate() {
                let definition = root.definition;
                let node = unit.definitions().node(definition).unwrap();
                let StaticExprKind::Constant(actual) = node.kind() else {
                    continue;
                };
                if !actual
                    .equals_observed(&original, CompilePhase::Validate, &Control)
                    .unwrap()
                {
                    continue;
                }
                assert_eq!(actual.ordinal(), 1);
                assert_eq!(actual.pool().backing_identity(), source_identity);
                assert_eq!(
                    unit.types().get(definition.index()),
                    Some(&FunctionArgumentType::Value(ty.clone()))
                );
                found += 1;
                let mut instance = CompiledExpressionInstance::try_new(
                    unit.clone(),
                    RootOrdinal::new(u32::try_from(root_ordinal).unwrap()),
                    &Control,
                )
                .unwrap();
                for (rows, selected) in [(1usize, vec![0]), (7, vec![1, 4, 6]), (7, vec![])] {
                    let batch = RecordBatch::try_new_with_options(
                        Arc::new(Schema::empty()),
                        vec![],
                        &RecordBatchOptions::new().with_row_count(Some(rows)),
                    )
                    .unwrap();
                    let selection = Selection::try_sparse(rows, &selected).unwrap();
                    let compiled = instance
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &batch,
                                source: None,
                            },
                            ScalarInvocationScope::new(selection, selection),
                            ScalarInvocationActivation::Activated,
                            &Control,
                            &mut PassThroughCallHost,
                        )
                        .unwrap();
                    assert!(compiled.errors().is_empty());
                    assert_eq!(compiled.values().data_type(), ty.data_type());
                    let DataType::List(field) = compiled.values().data_type() else {
                        panic!("canonical List")
                    };
                    assert_eq!(field.name(), "item");
                    assert!(field.metadata().as_arc().is_none());
                    let list = compiled
                        .values()
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();
                    assert_eq!(list.len(), selected.len());
                    let values = list.values().as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(
                        values.iter().collect::<Vec<_>>(),
                        (0..selected.len())
                            .flat_map(|_| [Some(22), Some(33)])
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }
    assert!(
        found > 0,
        "the actual original CV must reach a compiled constant root"
    );
    assert_eq!(original.ordinal(), 1);
    assert_eq!(original.pool().backing_identity(), source_identity);
    assert!(Arc::ptr_eq(pool.array(), original.pool().array()));
    let DataType::List(field) = original.field().data_type() else {
        panic!("source List")
    };
    assert_eq!(field.name(), if spare_empty { "item" } else { "source" });
    if spare_empty {
        assert!(field.metadata().is_empty());
        assert!(field.metadata().as_arc().is_none());
    } else {
        assert_eq!(field.metadata()["provider.fact"], "kept");
    }
}

mod numeric_not;
