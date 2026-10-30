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

//! Real SQL constant roots and the single-node entry share exact values and
//! row errors, with independent expected values for every case.
//! SQL source helpers install noop_constant_evaluator: the tested node must
//! survive SQL completion and LocalCompiler, over literal/constant leaves.

use super::super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use super::super::numeric_unary_owned_transaction_tests::programs_with_catalogue;
use super::super::sql_test_source::sql_source_with_semantics;
use super::CompiledExpressionInstance;
use super::constant_node::{
    ConstantCarrierStep, ConstantConversion, ConstantNode, ConstantNodeOutcome,
    evaluate_constant_node,
};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int8Array, Int64Array, StringArray, TimestampMicrosecondArray,
        new_null_array,
    },
    datatypes::{DataType, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use novarocks_functions::{
    CastOperation, ConstantPolicy, ConstantValue, FunctionArgumentType, FunctionValueType,
    KernelEvaluationControl, KernelFailure, PreparedPureKernel, PureEngineFunctionCatalog,
    Selection,
};
use novarocks_local_program::{
    LocalProgram, ProgramCallSite, ProgramExpressionRootSite, ProgramNodeKind, ProgramRootInput,
    ProgramUseRef, StaticExprKind, root_input_layout,
};
use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings};
use novarocks_type_contract::{
    ArithmeticOperator, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    PureCompileControl, ValueLogicalType,
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
    Arithmetic,
    Comparison,
    NullSafe,
    And,
    Or,
    Not,
    Carrier,
    Call(&'static str),
}
impl Family {
    fn matches(self, kind: &StaticExprKind, function: Option<&str>) -> bool {
        match self {
            Self::Arithmetic => matches!(kind, StaticExprKind::PreparedArithmetic { .. }),
            Self::Comparison => kind.ordinary_comparison().is_some(),
            Self::NullSafe => matches!(kind, StaticExprKind::PreparedNullSafeComparison { .. }),
            Self::And => matches!(
                kind,
                StaticExprKind::And(..) | StaticExprKind::NaryAnd { .. }
            ),
            Self::Or => matches!(kind, StaticExprKind::Or(..) | StaticExprKind::NaryOr { .. }),
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
    root: ProgramExpressionRootSite,
    occurrence: ProgramUseRef,
    sql: String,
}
impl Fixture {
    fn compile(
        expression: &str,
        family: Family,
        mode: &str,
        functions: &PureEngineFunctionCatalog,
    ) -> Self {
        let sql = format!("SELECT {expression} AS entry_parity");
        let source = sql_source_with_semantics(
            &sql,
            DataType::Int64,
            SqlSemanticSettings::default()
                .with_sql_mode(SqlMode::from_assignment(mode))
                .with_decimal_overflow_to_double(false),
        );
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
            let typed = program.checked().channels().expressions();
            let snapshot = typed.resolved_calls().snapshot();
            for (&root, &use_id) in snapshot.bindings() {
                let occurrence = ProgramUseRef {
                    arena: root.arena(),
                    use_id,
                };
                let flow = &snapshot.flows()[&root.arena()];
                let definition = flow.uses()[&use_id].definition;
                let node = snapshot.roots().arenas()[&root.arena()]
                    .node(definition)
                    .unwrap();
                let call = typed
                    .resolved_calls()
                    .calls()
                    .get(&ProgramCallSite::Expression(occurrence));
                if family.matches(
                    node.kind(),
                    call.map(|c| c.call_contract().function_id().as_str()),
                ) {
                    let FunctionArgumentType::Value(actual) =
                        typed.definition_type(root.arena(), definition).unwrap()
                    else {
                        panic!("the actual SQL root is a scalar value")
                    };
                    // This preserves the REAL frozen nullable declaration, including
                    // nullable outputs whose particular row is not NULL. Never derive
                    // a type from null_count(), and never widen to true for convenience.
                    assert_eq!(actual, &declared, "SQL/program complete root type: {sql}");
                    eprintln!(
                        "CONSTANT_ENTRY_PARITY frozen root sql={sql:?} result={actual:?} nullable={}",
                        actual.nullable()
                    );
                    // A Project over the original zero-column VALUES port and
                    // an original ValuesCell both have an empty one-row input.
                    // Read the real layout; do not substitute a single-column scan.
                    let schema = match root_input_layout(program.graph(), root).unwrap() {
                        ProgramRootInput::Empty => Arc::new(Schema::empty()),
                        ProgramRootInput::Layout { node, role } => program
                            .checked()
                            .channels()
                            .channel_layout(node, role)
                            .unwrap()
                            .schema()
                            .clone(),
                    };
                    assert!(schema.fields().is_empty(), "empty original input: {sql}");
                    found.push(Self {
                        program: program.clone(),
                        root,
                        occurrence,
                        sql: sql.clone(),
                    });
                }
            }
        }
        assert_eq!(
            found.len(),
            1,
            "one actual {family:?} constant root must survive noop folding: {sql}"
        );
        found.pop().unwrap()
    }
    fn kind(&self, at: ProgramUseRef) -> &StaticExprKind {
        let snapshot = self
            .program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        let definition = snapshot.flows()[&at.arena].uses()[&at.use_id].definition;
        snapshot.roots().arenas()[&at.arena]
            .node(definition)
            .unwrap()
            .kind()
    }
    fn ty(&self, at: ProgramUseRef) -> &FunctionValueType {
        let typed = self.program.checked().channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let definition = snapshot.flows()[&at.arena].uses()[&at.use_id].definition;
        let FunctionArgumentType::Value(ty) = typed.definition_type(at.arena, definition).unwrap()
        else {
            panic!("value occurrence required")
        };
        ty
    }
    fn args(&self, at: ProgramUseRef) -> Vec<ProgramUseRef> {
        let snapshot = self
            .program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        snapshot.flows()[&at.arena].uses()[&at.use_id]
            .arguments
            .iter()
            .map(|&use_id| ProgramUseRef {
                arena: at.arena,
                use_id,
            })
            .collect()
    }
    fn call(&self, at: ProgramUseRef) -> &PreparedPureKernel {
        self.program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .calls()[&ProgramCallSite::Expression(at)]
            .specialization()
            .prepared()
    }
    fn constant(&self, at: ProgramUseRef, array: ArrayRef) -> ConstantValue {
        let ty = self.ty(at);
        assert_eq!(
            ty.data_type(),
            array.data_type(),
            "independent input carrier: {}",
            self.sql
        );
        ConstantValue::from_scalar_array(
            Arc::new(ty.try_to_field("actual_frozen_source").unwrap()),
            ty.clone(),
            array.to_data(),
            policy(),
            CompilePhase::FunctionSpecialization,
            &Control,
        )
        .unwrap()
    }
    fn entry_node(&self) -> ConstantNode<'_> {
        match self.kind(self.occurrence) {
            StaticExprKind::PreparedArithmetic {
                operator,
                decimal_overflow_policy,
                allow_throw_exception,
                ..
            } => ConstantNode::Arithmetic {
                operator: *operator,
                decimal_overflow_policy: *decimal_overflow_policy,
                allow_throw_exception: *allow_throw_exception,
            },
            kind if kind.ordinary_comparison().is_some() => ConstantNode::Comparison {
                operator: kind.ordinary_comparison().unwrap().0,
                left: ConstantConversion::Identity,
                right: ConstantConversion::Identity,
            },
            StaticExprKind::PreparedNullSafeComparison { .. } => ConstantNode::NullSafeEquality {
                left: ConstantConversion::Identity,
                right: ConstantConversion::Identity,
            },
            StaticExprKind::And(..) | StaticExprKind::NaryAnd { .. } => ConstantNode::Conjunction,
            StaticExprKind::Or(..) | StaticExprKind::NaryOr { .. } => ConstantNode::Disjunction,
            StaticExprKind::Not(_) => ConstantNode::Not,
            StaticExprKind::PreparedCast {
                operation: CastOperation::Carrier,
                ..
            } => {
                ConstantNode::Conversion(ConstantConversion::Carrier(self.carrier(self.occurrence)))
            }
            StaticExprKind::BoundCall { .. } => ConstantNode::Call(self.call(self.occurrence)),
            kind => panic!("unexpected original root: {kind:?}"),
        }
    }
    fn carrier(&self, at: ProgramUseRef) -> ConstantCarrierStep<'_> {
        let StaticExprKind::PreparedCast {
            operation: CastOperation::Carrier,
            decimal_overflow_policy,
            allow_throw_exception,
            ..
        } = self.kind(at)
        else {
            panic!("real frozen carrier cast required")
        };
        ConstantCarrierStep {
            target: self.ty(at),
            decimal_overflow_policy: *decimal_overflow_policy,
            allow_throw_exception: *allow_throw_exception,
        }
    }
    // Stop at the independently declared ORIGINAL input type, so explicit SQL
    // constructors (e.g. string -> DATE) are not mistaken for comparison coercions.
    // Only the actual selected conversion and at most one carrier tail are admitted.
    fn conversion_from(
        &self,
        at: ProgramUseRef,
        source: &DataType,
    ) -> (ProgramUseRef, ConstantConversion<'_>) {
        if self.ty(at).data_type() == source {
            return (at, ConstantConversion::Identity);
        }
        match self.kind(at) {
            StaticExprKind::PreparedCast {
                operation: CastOperation::Carrier,
                ..
            } => {
                let args = self.args(at);
                let [child] = args.as_slice() else {
                    panic!("unary carrier occurrence")
                };
                if self.ty(*child).data_type() == source {
                    return (*child, ConstantConversion::Carrier(self.carrier(at)));
                }
                let (original, conversion) = self.conversion_from(*child, source);
                let ConstantConversion::ValueConversion {
                    call,
                    call_result,
                    tail: None,
                } = conversion
                else {
                    panic!("one actual value conversion, then one carrier tail")
                };
                (
                    original,
                    ConstantConversion::ValueConversion {
                        call,
                        call_result,
                        tail: Some(self.carrier(at)),
                    },
                )
            }
            StaticExprKind::BoundCall { .. } => {
                let call = self.call(at);
                assert_eq!(
                    call.call_contract().function_id().as_str(),
                    "builtin.scalar/value_domain_conversion/v1"
                );
                let args = self.args(at);
                let [original] = args.as_slice() else {
                    panic!("unary value conversion")
                };
                assert_eq!(self.ty(*original).data_type(), source);
                (
                    *original,
                    ConstantConversion::ValueConversion {
                        call,
                        call_result: self.ty(at),
                        tail: None,
                    },
                )
            }
            kind => panic!(
                "missing real conversion from {source:?} to {:?}: {kind:?}",
                self.ty(at)
            ),
        }
    }
    fn assert_both(
        &self,
        node: ConstantNode<'_>,
        children: Vec<ConstantValue>,
        expected: Expected,
    ) {
        let schema = match root_input_layout(self.program.graph(), self.root).unwrap() {
            ProgramRootInput::Empty => Arc::new(Schema::empty()),
            ProgramRootInput::Layout { node, role } => self
                .program
                .checked()
                .channels()
                .channel_layout(node, role)
                .unwrap()
                .schema()
                .clone(),
        };
        assert!(schema.fields().is_empty());
        let batch = RecordBatch::try_new_with_options(
            schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let mut instance =
            CompiledExpressionInstance::try_new(self.program.clone(), self.root, &Control).unwrap();
        let compiled = instance
            .evaluate(&batch, Selection::all(1), &Control)
            .unwrap();
        let entry = evaluate_constant_node(
            node,
            &children,
            self.ty(self.occurrence),
            &Control,
            CompilePhase::FunctionSpecialization,
        )
        .unwrap();
        match expected {
            Expected::Value(expected) => {
                assert!(
                    compiled.errors().is_empty(),
                    "unexpected compiled row error: {}",
                    self.sql
                );
                let ConstantNodeOutcome::Value(entry) = entry else {
                    panic!("expected value: {entry:?}; {}", self.sql)
                };
                assert_array(compiled.values(), &expected);
                assert_array(&entry, &expected);
                assert_array(&entry, compiled.values());
            }
            Expected::RowError(message) => {
                let ConstantNodeOutcome::RowError(entry) = entry else {
                    panic!("row error classification: {entry:?}; {}", self.sql)
                };
                assert_eq!(compiled.errors().len(), 1, "{}", self.sql);
                let actual = &compiled.errors()[0];
                assert_eq!(actual.selected_ordinal(), 0);
                assert_eq!(entry.selected_ordinal(), 0);
                assert_eq!(actual.message(), message);
                assert_eq!(entry.message(), message);
                assert_eq!(&entry, actual);
                assert!(
                    compiled.values().is_null(0),
                    "failed row has NULL placeholder"
                );
            }
        }
    }
    fn direct(&self, arrays: Vec<ArrayRef>, expected: Expected) {
        let args = self.args(self.occurrence);
        assert_eq!(args.len(), arrays.len());
        let constants = args
            .into_iter()
            .zip(arrays)
            .map(|(at, array)| self.constant(at, array))
            .collect();
        self.assert_both(self.entry_node(), constants, expected);
    }
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
fn int(value: Option<i64>) -> ArrayRef {
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
fn largeint(value: i128) -> ArrayRef {
    let bytes = value.to_be_bytes();
    Arc::new(FixedSizeBinaryArray::try_from_iter([bytes.as_slice()].into_iter()).unwrap())
}

#[test]
fn constant_entry_every_family_matches_real_noop_sql_constant_root() {
    let functions = installed_builtin_owner_catalogue();
    metadata_cv_source_cross_entry(&functions, false);
    metadata_cv_source_cross_entry(&functions, true);
    let cases: Vec<(&str, Family, bool, Vec<ArrayRef>, ArrayRef)> = vec![
        (
            "20 + 22",
            Family::Arithmetic,
            true,
            vec![int(Some(20)), int(Some(22))],
            int(Some(42)),
        ),
        (
            "1e0 * 1e0",
            Family::Arithmetic,
            true,
            vec![float(1.0), float(1.0)],
            float(1.0),
        ),
        (
            "20 < 22",
            Family::Comparison,
            false,
            vec![int(Some(20)), int(Some(22))],
            boolean(Some(true)),
        ),
        (
            "1 <=> 1",
            Family::NullSafe,
            false,
            vec![int(Some(1)), int(Some(1))],
            boolean(Some(true)),
        ),
        (
            "FALSE AND TRUE",
            Family::And,
            false,
            vec![boolean(Some(false)), boolean(Some(true))],
            boolean(Some(false)),
        ),
        (
            "TRUE OR FALSE",
            Family::Or,
            false,
            vec![boolean(Some(true)), boolean(Some(false))],
            boolean(Some(true)),
        ),
        (
            "NOT TRUE",
            Family::Not,
            false,
            vec![boolean(Some(true))],
            boolean(Some(false)),
        ),
        (
            "CAST(42 AS TINYINT)",
            Family::Carrier,
            true,
            vec![int(Some(42))],
            Arc::new(Int8Array::from(vec![42])),
        ),
        (
            "upper('AbC')",
            Family::Call("builtin.scalar/upper/v1"),
            false,
            vec![text("AbC")],
            text("ABC"),
        ),
        (
            "if(FALSE, 11, 22)",
            Family::Call("builtin.scalar/if/v1"),
            false,
            vec![boolean(Some(false)), int(Some(11)), int(Some(22))],
            int(Some(22)),
        ),
        (
            "coalesce(11, 22)",
            Family::Call("builtin.scalar/coalesce/v1"),
            false,
            vec![int(Some(11)), int(Some(22))],
            int(Some(11)),
        ),
    ];
    for (expression, family, nullable, children, expected) in cases {
        let fixture = Fixture::compile(expression, family, "32", &functions);
        assert_eq!(
            fixture.ty(fixture.occurrence).nullable(),
            nullable,
            "frozen root: {expression}"
        );
        for at in fixture.args(fixture.occurrence) {
            assert!(
                matches!(
                    fixture.kind(at),
                    StaticExprKind::Constant(_) | StaticExprKind::Literal(_)
                ),
                "T1 uses one actual node over constant leaves: {expression}"
            );
        }
        fixture.direct(children, Expected::Value(expected));
    }
    // Native unary minus is zero subtraction: +0 - +0 is +0. An explicit
    // text CAST authors the separate -0 bit pattern. Preserve both real child
    // subexpressions, plus typed NULL, without replacing the depth-one cases.
    for (expression, family, nullable, children, expected) in [
        (
            "-0e0 * 1e0",
            Family::Arithmetic,
            true,
            vec![float(0.0), float(1.0)],
            float(0.0),
        ),
        (
            "CAST('-0' AS DOUBLE) * 1e0",
            Family::Arithmetic,
            true,
            vec![float(-0.0), float(1.0)],
            float(-0.0),
        ),
        (
            "TRUE AND CAST(NULL AS BOOLEAN)",
            Family::And,
            true,
            vec![boolean(Some(true)), boolean(None)],
            boolean(None),
        ),
        (
            "if(FALSE, CAST(NULL AS BIGINT), 22)",
            Family::Call("builtin.scalar/if/v1"),
            true,
            vec![boolean(Some(false)), int(None), int(Some(22))],
            int(Some(22)),
        ),
        (
            "coalesce(CAST(NULL AS BIGINT), CAST(NULL AS BIGINT))",
            Family::Call("builtin.scalar/coalesce/v1"),
            true,
            vec![int(None), int(None)],
            int(None),
        ),
        (
            "CAST(NULL AS BIGINT) <=> CAST(NULL AS BIGINT)",
            Family::NullSafe,
            false,
            vec![int(None), int(None)],
            boolean(Some(true)),
        ),
        (
            "NOT CAST(NULL AS BOOLEAN)",
            Family::Not,
            true,
            vec![boolean(None)],
            boolean(None),
        ),
    ] {
        let fixture = Fixture::compile(expression, family, "32", &functions);
        assert_eq!(
            fixture.ty(fixture.occurrence).nullable(),
            nullable,
            "frozen root: {expression}"
        );
        fixture.direct(children, Expected::Value(expected));
    }
}

#[test]
fn constant_entry_heterogeneous_comparisons_replay_actual_sql_conversion_order() {
    let functions = installed_builtin_owner_catalogue();
    let cases: Vec<(&str, ArrayRef, ArrayRef)> = vec![
        (
            "CAST('1970-01-02' AS DATE) < CAST('1970-01-03 00:00:00' AS DATETIME)",
            Arc::new(Date32Array::from(vec![1])),
            Arc::new(TimestampMicrosecondArray::from(vec![172_800_000_000])),
        ),
        (
            "CAST('12.3' AS DECIMAL(4,1)) < CAST('12.34' AS DECIMAL(6,2))",
            decimal(Some(123), 4, 1),
            decimal(Some(1234), 6, 2),
        ),
        (
            "12 < CAST('12.34' AS DECIMAL(6,2))",
            int(Some(12)),
            decimal(Some(1234), 6, 2),
        ),
    ];
    for (expression, left_array, right_array) in cases {
        let fixture = Fixture::compile(expression, Family::Comparison, "32", &functions);
        let args = fixture.args(fixture.occurrence);
        let [left_at, right_at] = args.as_slice() else {
            panic!("binary comparison")
        };
        assert_ne!(
            left_array.data_type(),
            right_array.data_type(),
            "original heterogeneous inputs"
        );
        let (left_source, left) = fixture.conversion_from(*left_at, left_array.data_type());
        let (right_source, right) = fixture.conversion_from(*right_at, right_array.data_type());
        assert!(
            !matches!(
                (&left, &right),
                (ConstantConversion::Identity, ConstantConversion::Identity)
            ),
            "real lowering must retain at least one conversion: {expression}"
        );
        assert_eq!(
            fixture.ty(*left_at).data_type(),
            fixture.ty(*right_at).data_type(),
            "conversion destinations, not guessed source carriers"
        );
        assert_eq!(
            fixture.ty(fixture.occurrence).nullable(),
            fixture.ty(*left_at).nullable() || fixture.ty(*right_at).nullable(),
            "comparison root uses both actual converted nullable facts"
        );
        let operator = fixture
            .kind(fixture.occurrence)
            .ordinary_comparison()
            .unwrap()
            .0;
        let children = vec![
            fixture.constant(left_source, left_array),
            fixture.constant(right_source, right_array),
        ];
        fixture.assert_both(
            ConstantNode::Comparison {
                operator,
                left,
                right,
            },
            children,
            Expected::Value(boolean(Some(true))),
        );
    }
}

#[test]
fn constant_entry_actual_largeint_conversion_then_carrier_tail_preserves_value_and_null() {
    let functions = installed_builtin_owner_catalogue();
    // LARGEINT -> signed widths is a direct value conversion, so it cannot
    // honestly stand in for a conversion+tail. The actual SQL author explicitly
    // composes LARGEINT -> BIGINT -> DATETIME (see conversion_intermediate_type).
    // Both integers exceed i64::MAX and are genuine LARGEINT literals. Their
    // exact difference is 20200102030405, authored LargeInt by real arithmetic.
    // F3 retains the nominal same-carrier CAST boundary (spec 1.3/5.6/6.4);
    // introducing CAST(... AS LARGEINT) here would test that separate refusal,
    // rather than the admitted value-conversion and DATETIME carrier tail.
    for (expression, value, expected) in [
        (
            "CAST(9223392236956806213 - 9223372036854775808 AS DATETIME)".to_owned(),
            20_200_102_030_405_i128,
            Some(1_577_934_245_000_000_i64),
        ),
        (format!("CAST({} AS DATETIME)", i128::MAX), i128::MAX, None),
    ] {
        let fixture = Fixture::compile(&expression, Family::Carrier, "32", &functions);
        let (source, conversion) =
            fixture.conversion_from(fixture.occurrence, &DataType::FixedSizeBinary(16));
        assert_eq!(
            fixture.ty(source).logical_type(),
            ValueLogicalType::LargeInt,
            "fixed binary alone never grants LARGEINT identity"
        );
        let ConstantConversion::ValueConversion {
            call,
            call_result,
            tail: Some(tail),
        } = &conversion
        else {
            panic!("actual selected value conversion plus carrier tail required")
        };
        assert!(
            call.call_contract()
                .selected()
                .overload
                .as_str()
                .ends_with("largeint_to_signed_null_overflow/v1")
        );
        assert_eq!(call_result.data_type(), &DataType::Int64);
        assert!(call_result.nullable());
        assert_eq!(
            tail.target.data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        assert!(tail.target.nullable());
        assert_eq!(
            tail.decimal_overflow_policy,
            DecimalOverflowPolicy::OutputNull
        );
        assert!(!tail.allow_throw_exception);
        assert!(
            fixture.ty(fixture.occurrence).nullable(),
            "even the successful calendar row retains nullable=true"
        );
        let children = vec![fixture.constant(source, largeint(value))];
        fixture.assert_both(
            ConstantNode::Conversion(conversion),
            children,
            Expected::Value(Arc::new(TimestampMicrosecondArray::from(vec![expected]))),
        );
    }
}

#[test]
fn constant_entry_decimal_policy_cross_allow_matrix_matches_independent_row_error_contract() {
    use ArithmeticOperator::{Add, Multiply};
    use DecimalOverflowPolicy::{OutputNull, ReportError};
    let functions = installed_builtin_owner_catalogue();
    let max = 10_i128.pow(38) - 1;
    for (policy, allow, mode) in [
        (OutputNull, false, "32"),
        (OutputNull, true, "ALLOW_THROW_EXCEPTION"),
        (ReportError, false, "ERROR_IF_OVERFLOW"),
        (ReportError, true, "ERROR_IF_OVERFLOW,ALLOW_THROW_EXCEPTION"),
    ] {
        for (operator, symbol, rhs, error) in [
            (
                Add,
                "+",
                1,
                "Expr evaluate meet error: The 'add' operation involving decimal values overflows",
            ),
            (
                Multiply,
                "*",
                2,
                "Expr evaluate meet error: The 'mul' operation involving decimal values overflows",
            ),
        ] {
            let expression =
                format!("CAST('{max}' AS DECIMAL(38,0)) {symbol} CAST({rhs} AS DECIMAL(2,0))");
            let fixture = Fixture::compile(&expression, Family::Arithmetic, mode, &functions);
            let ConstantNode::Arithmetic {
                operator: frozen_op,
                decimal_overflow_policy: frozen_policy,
                allow_throw_exception: frozen_allow,
            } = fixture.entry_node()
            else {
                unreachable!()
            };
            assert_eq!(
                (frozen_op, frozen_policy, frozen_allow),
                (operator, policy, allow)
            );
            assert_eq!(
                fixture.ty(fixture.occurrence).data_type(),
                &DataType::Decimal128(38, 0)
            );
            assert!(fixture.ty(fixture.occurrence).nullable());
            // Independent existing owner contract: ALLOW adds multiplication's
            // row error, but it does not turn OutputNull decimal ADD into an error.
            let expected = if policy == ReportError || (operator == Multiply && allow) {
                Expected::RowError(error)
            } else {
                Expected::Value(decimal(None, 38, 0))
            };
            fixture.direct(
                vec![decimal(Some(max), 38, 0), decimal(Some(rhs), 2, 0)],
                expected,
            );
        }
        let fixture = Fixture::compile(
            "CAST(CAST('999.99' AS DECIMAL(5,2)) AS DECIMAL(4,1))",
            Family::Carrier,
            mode,
            &functions,
        );
        let step = fixture.carrier(fixture.occurrence);
        assert_eq!(
            (step.decimal_overflow_policy, step.allow_throw_exception),
            (policy, allow)
        );
        assert_eq!(step.target.data_type(), &DataType::Decimal128(4, 1));
        // Root nullability is authored by SQL and checked against the program
        // in Fixture::compile; unlike arithmetic, do not invent nullable=true
        // for checked modes whose source declaration may already be non-null.
        let expected = if policy == ReportError || allow {
            Expected::RowError(
                "Expr evaluate meet error: The numeric type cast involving decimal overflows",
            )
        } else {
            Expected::Value(new_null_array(&DataType::Decimal128(4, 1), 1))
        };
        fixture.direct(vec![decimal(Some(99_999), 5, 2)], expected);
    }
}

// The source is a REAL checked CV and enters the actual SQL compiler before
// package/LocalCompiler/Frame. No frozen call, program, output, or capability is forged.
fn metadata_cv_source_cross_entry(functions: &PureEngineFunctionCatalog, spare_empty: bool) {
    use arrow::array::{Int32Array, ListArray};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::Field;
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
        SqlCompileProgress, SqlCompiler, SqlConstantExpressionSite, SqlExpressionConstant,
        SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
        builtin_sql_function_catalog, noop_constant_evaluator,
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
    let parsed = novarocks_parser::parse("SELECT NULL AS metadata_cv_source").unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = parsed.as_slice() else {
        panic!("one query")
    };
    let control = SqlCompileControl::unbounded();
    let statement = SqlStatementInput::query_with_expression_constants(
        Box::new(query.clone()),
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
        noop_constant_evaluator(),
        policy(),
        control.clone(),
        novarocks_physical_plan::PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        novarocks_physical_plan::ScanReadBudget {
            max_batch_rows: 64,
            max_batch_bytes: 1 << 20,
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
        let expressions = program.checked().channels().expressions();
        let snapshot = expressions.resolved_calls().snapshot();
        for (&root, &use_id) in snapshot.bindings() {
            let definition = snapshot.flows()[&root.arena()].uses()[&use_id].definition;
            let node = snapshot.roots().arenas()[&root.arena()]
                .node(definition)
                .unwrap();
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
                expressions.definition_type(root.arena(), definition),
                Some(&FunctionArgumentType::Value(ty.clone()))
            );
            found += 1;
            let mut instance =
                CompiledExpressionInstance::try_new(program.clone(), root, &Control).unwrap();
            for (rows, selected) in [(1usize, vec![0]), (7, vec![1, 4, 6]), (7, vec![])] {
                let batch = RecordBatch::try_new_with_options(
                    Arc::new(Schema::empty()),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(rows)),
                )
                .unwrap();
                let selection = Selection::try_sparse(rows, &selected).unwrap();
                let compiled = instance.evaluate(&batch, selection, &Control).unwrap();
                assert!(compiled.errors().is_empty());
                assert_eq!(compiled.values().data_type(), ty.data_type());
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
    assert!(
        found > 0,
        "the actual original CV must reach a compiled constant root"
    );
    let ConstantNodeOutcome::Value(single) = evaluate_constant_node(
        ConstantNode::Conversion(ConstantConversion::Identity),
        std::slice::from_ref(&original),
        &ty,
        &Control,
        CompilePhase::FunctionSpecialization,
    )
    .unwrap() else {
        panic!("identity value")
    };
    let list = single.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(22), Some(33)]
    );
    assert_eq!(single.data_type(), ty.data_type());
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
    let DataType::List(field) = single.data_type() else {
        panic!("canonical List")
    };
    assert_eq!(field.name(), "item");
    assert!(field.metadata().as_arc().is_none());
}
