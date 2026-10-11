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
//! Complete selected TRUNCATE profiles and raw original fatal outcomes.
use super::generate::InputProfile;
use super::{
    CatalogKind, DiffArgument, FloatComparison, HarnessControl, InputGenerator, PreparedScalarDiff,
    ScalarCall, ScalarDiffSpec, builtin_engine_function_catalog, installed_declaration,
    legacy_wire_name, prepare_scalar, resolve_like_sql, run_scalar_differential,
    scalar_legacy_implementation,
};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int64Array, new_empty_array,
    new_null_array,
};
use arrow::datatypes::DataType;
use novarocks_functions::{ScalarEvaluationInstance, Selection};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

const SEEDS: [u64; 3] = [0xC06A0, 0xC06A1, 0xC06A2];
const POLICIES: [DecimalOverflowPolicy; 2] = [
    DecimalOverflowPolicy::OutputNull,
    DecimalOverflowPolicy::ReportError,
];
fn types() -> [DataType; 8] {
    [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(38, 2),
        DataType::Null,
    ]
}
fn signed(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}
fn floats(v: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(v))
}
fn decimal(v: Vec<Option<i128>>, p: u8, s: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(v)
            .with_precision_and_scale(p, s)
            .unwrap(),
    )
}
fn typed(a: &ArrayRef, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(a.data_type().clone(), nullable)
}
#[derive(Default)]
struct Collection {
    cases: usize,
    selections: usize,
    rows: usize,
    fatal_pairs: usize,
    value_pairs: usize,
    failures: Vec<String>,
}
impl Collection {
    fn check(&mut self, label: String, spec: ScalarDiffSpec) {
        self.cases += 1;
        match run_scalar_differential(&spec) {
            Ok(summary) => {
                self.selections += summary.selections;
                self.rows += summary.rows;
            }
            Err(error) => self.failures.push(format!("{label}: {error:?}")),
        }
    }
    fn pair(&mut self, label: String, spec: ScalarDiffSpec, rows: Option<&[usize]>) {
        self.cases += 1;
        let (left, right) = raw_pair(&spec, rows);
        match &left {
            Raw::Fatal(_) => self.fatal_pairs += 1,
            Raw::Values(..) => self.value_pairs += 1,
            Raw::Error(_) => {}
        }
        match (left, right) {
            (Raw::Values(a, ae), Raw::Values(b, be)) if a.to_data() == b.to_data() && ae == be => {}
            (Raw::Fatal(a), Raw::Fatal(b)) if a == b => {}
            (Raw::Error(a), b) => self
                .failures
                .push(format!("{label}: unexpected legacy error {a}; pure {b:?}")),
            (a, Raw::Error(b)) => self
                .failures
                .push(format!("{label}: legacy {a:?}; unexpected pure error {b}")),
            (a, b) => self
                .failures
                .push(format!("{label}: legacy {a:?}; pure {b:?}")),
        }
    }
    fn finish(self) {
        println!(
            "C06-P2 completed profiles={} successful_selections={} successful_profile_rows={} legacy_fatal_pairs={} legacy_value_pairs={} disagreements={}",
            self.cases,
            self.selections,
            self.rows,
            self.fatal_pairs,
            self.value_pairs,
            self.failures.len()
        );
        assert!(self.cases > 0);
        assert!(self.failures.is_empty(), "{}", self.failures.join("\n"));
    }
}
#[derive(Debug)]
enum Raw {
    Values(ArrayRef, Vec<String>),
    Fatal((String, String)),
    Error(String),
}
fn capture(f: impl FnOnce() -> Raw) -> Raw {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(p) => {
            let payload = if let Some(s) = p.downcast_ref::<String>() {
                ("String".into(), s.clone())
            } else if let Some(s) = p.downcast_ref::<&str>() {
                ("&str".into(), (*s).into())
            } else {
                panic!("TRUNCATE produced an unknown fatal payload type")
            };
            Raw::Fatal(payload)
        }
    }
}
fn raw_pair(spec: &ScalarDiffSpec, rows: Option<&[usize]>) -> (Raw, Raw) {
    let catalog = builtin_engine_function_catalog();
    let bound =
        resolve_like_sql(catalog, "truncate", CatalogKind::Scalar, &spec.arguments).unwrap();
    let name = legacy_wire_name(&bound.function_id).unwrap();
    let (_, dependencies) = installed_declaration(
        catalog,
        "truncate",
        &bound,
        CatalogKind::Scalar,
        super::LegacyStatus::Available,
    )
    .unwrap();
    let prepared = prepare_scalar(catalog, spec, &bound, &dependencies).unwrap();
    let result_type = prepared.contract().result_type();
    let call = ScalarCall {
        spec,
        legacy_kind: scalar_legacy_implementation(&name).unwrap(),
        result_type,
        rows: spec.rows().unwrap(),
    };
    let legacy = capture(|| match call.evaluate_legacy_inner(rows) {
        Ok(a) => Raw::Values(a, vec![]),
        Err(e) => Raw::Error(e),
    });
    let selection = rows.map_or_else(
        || Selection::all(call.rows),
        |r| Selection::try_sparse(call.rows, r).unwrap(),
    );
    let args = spec
        .arguments
        .iter()
        .map(DiffArgument::evaluated)
        .collect::<Vec<_>>();
    let pure = capture(|| {
        let PreparedScalarDiff::Kernel(p) = &prepared else {
            panic!("TRUNCATE must retain ScalarV1")
        };
        let mut instance = ScalarEvaluationInstance::instantiate(Arc::clone(p)).unwrap();
        match instance.evaluate(selection, &args, &HarnessControl) {
            Ok(v) => Raw::Values(
                Arc::clone(v.values()),
                v.errors().iter().map(|e| e.message().to_string()).collect(),
            ),
            Err(e) => Raw::Error(e.to_string()),
        }
    });
    (legacy, pure)
}
fn safe_digits(ty: &DataType, nullable: bool, rows: usize) -> ArrayRef {
    if *ty == DataType::Null {
        return new_null_array(ty, rows);
    }
    let pattern: &[i64] = if *ty == DataType::Int8 {
        &[-2, -1, 0, 1, 2]
    } else {
        &[-2, -1, 0, 1, 2, 308, -308]
    };
    let values = (0..rows)
        .map(|r| {
            if nullable && r % 13 == 0 {
                None
            } else {
                Some(pattern[r % pattern.len()])
            }
        })
        .collect::<Vec<_>>();
    if let DataType::Decimal128(p, s) = ty {
        return decimal(
            values
                .into_iter()
                .map(|v| v.map(|x| i128::from(x) * 100))
                .collect(),
            *p,
            *s,
        );
    }
    arrow::compute::cast(&signed(values), ty).unwrap()
}
#[test]
fn c06_p2_truncate_generated_eight_unary_and_sixtyfour_binary_profiles() {
    let mut c = Collection::default();
    for seed in SEEDS {
        for policy in POLICIES {
            for left in types() {
                for ln in [false, true] {
                    if left == DataType::Null && !ln {
                        continue;
                    }
                    let lt = FunctionValueType::new(left.clone(), ln);
                    let mut g = InputGenerator::new(seed);
                    let a = g.column(&lt, 515, &InputProfile::default()).slice(1, 513);
                    let base = ScalarDiffSpec::new("truncate")
                        .typed_column(lt.clone(), a.clone())
                        .decimal_overflow(policy)
                        .float_comparison(FloatComparison::Exact)
                        .sparse_selections(7, seed);
                    c.check(
                        format!("seed={seed} unary={lt:?} policy={policy:?}"),
                        base.clone(),
                    );
                    for right in types() {
                        for rn in [false, true] {
                            if right == DataType::Null && !rn {
                                continue;
                            }
                            let d = safe_digits(&right, rn, 515).slice(1, 513);
                            c.check(
                                format!(
                                    "seed={seed} left={lt:?} right={right:?}/{rn} policy={policy:?}"
                                ),
                                base.clone().typed_column(typed(&d, rn), d),
                            );
                        }
                    }
                }
            }
        }
    }
    assert_eq!(c.cases, 1440);
    c.finish();
}
#[test]
fn c06_p2_truncate_typed_empty_constant_broadcast_and_static_scale_recipes() {
    let mut c = Collection::default();
    for policy in POLICIES {
        for left in types() {
            c.check(
                format!("typed empty unary {left:?}/{policy:?}"),
                ScalarDiffSpec::new("truncate")
                    .column(new_empty_array(&left))
                    .decimal_overflow(policy),
            );
            let a = if left == DataType::Null {
                new_null_array(&left, 1)
            } else {
                safe_digits(&left, false, 1)
            };
            for rows in [0, 513] {
                c.check(
                    format!("constant unary {left:?}/{rows}/{policy:?}"),
                    ScalarDiffSpec::new("truncate")
                        .constant_array(a.clone())
                        .constant_rows(rows)
                        .decimal_overflow(policy)
                        .sparse_selections(7, SEEDS[0]),
                );
                for right in types() {
                    let d = safe_digits(&right, false, 1);
                    c.check(
                        format!("constant binary {left:?}/{right:?}/{rows}/{policy:?}"),
                        ScalarDiffSpec::new("truncate")
                            .constant_array(a.clone())
                            .constant_array(d)
                            .constant_rows(rows)
                            .decimal_overflow(policy)
                            .sparse_selections(7, SEEDS[0]),
                    );
                    c.check(
                        format!("NULL constant binary {left:?}/{right:?}/{rows}/{policy:?}"),
                        ScalarDiffSpec::new("truncate")
                            .constant_array(a.clone())
                            .constant_array(new_null_array(&right, 1))
                            .constant_rows(rows)
                            .decimal_overflow(policy)
                            .sparse_selections(7, SEEDS[0]),
                    );
                    c.check(
                        format!("empty binary {left:?}/{right:?}/{policy:?}"),
                        ScalarDiffSpec::new("truncate")
                            .column(new_empty_array(&left))
                            .column(new_empty_array(&right))
                            .decimal_overflow(policy),
                    );
                }
            }
            let values = safe_digits(&left, true, 513);
            for right in types() {
                let d = safe_digits(&right, false, 2).slice(1, 1);
                c.check(
                    format!("broadcast {left:?}/{right:?}/{policy:?}"),
                    ScalarDiffSpec::new("truncate")
                        .column(values.clone())
                        .constant_array(d)
                        .decimal_overflow(policy)
                        .sparse_selections(7, SEEDS[1]),
                );
            }
        }
        for digits in [257, 128, -1, 0, 2] {
            let a = decimal(vec![Some(12345), Some(-12345), None], 38, 2);
            let scale = (digits as i8).clamp(0, 2);
            c.check(
                format!("static scale signed={digits}/{policy:?}"),
                ScalarDiffSpec::new("truncate")
                    .column(a.clone())
                    .constant_array(signed(vec![Some(digits)]))
                    .decimal_overflow(policy)
                    .expect_result_type(FunctionValueType::new(
                        DataType::Decimal128(38, scale),
                        true,
                    )),
            );
            for d in [
                floats(vec![Some(digits as f64)]),
                decimal(vec![Some(i128::from(digits) * 100)], 38, 2),
            ] {
                c.check(
                    format!("non-signed literal {digits}/{:?}/{policy:?}", d.data_type()),
                    ScalarDiffSpec::new("truncate")
                        .column(a.clone())
                        .constant_array(d)
                        .decimal_overflow(policy)
                        .expect_result_type(FunctionValueType::new(
                            DataType::Decimal128(38, 2),
                            true,
                        )),
                );
            }
        }
    }
    c.finish();
}
#[test]
fn c06_p2_truncate_all_5643_decimal_declarations_in_each_argument_position() {
    let mut c = Collection::default();
    let mut declarations = 0;
    for p in 1..=38u8 {
        for s in -128i16..=i16::from(p) {
            declarations += 1;
            let s = s as i8;
            let bound = 10_i128.pow(u32::from(p)) - 1;
            let a = decimal(
                vec![Some(0), Some(1), Some(-1), Some(bound), Some(-bound), None],
                p,
                s,
            );
            for policy in POLICIES {
                let base = ScalarDiffSpec::new("truncate")
                    .column(a.clone())
                    .decimal_overflow(policy)
                    .sparse_selections(0, SEEDS[2]);
                c.check(
                    format!("grid left unary p={p}/s={s}/{policy:?}"),
                    base.clone(),
                );
                c.check(
                    format!("grid left literal p={p}/s={s}/{policy:?}"),
                    base.clone().constant_array(signed(vec![Some(1)])),
                );
                c.check(
                    format!("grid left dynamic p={p}/s={s}/{policy:?}"),
                    base.column(signed(vec![
                        Some(-2),
                        Some(-1),
                        Some(0),
                        Some(1),
                        Some(2),
                        None,
                    ])),
                );
                let divisor = 10_f64.powi(i32::from(s));
                // Classify the original digit conversion before any evaluation.
                // Negative saturation reaches the original MIN negation fatal path.
                let fatal = [-1, -bound]
                    .into_iter()
                    .any(|raw| ((raw as f64) / divisor) as i64 == i64::MIN);
                let digits = ScalarDiffSpec::new("truncate")
                    .column(floats(vec![Some(12.99); 6]))
                    .column(a.clone())
                    .decimal_overflow(policy)
                    .sparse_selections(0, SEEDS[2]);
                if fatal {
                    c.pair(
                        format!("grid digits fatal p={p}/s={s}/{policy:?}"),
                        digits.clone(),
                        None,
                    );
                } else {
                    c.check(
                        format!("grid digits p={p}/s={s}/{policy:?}"),
                        digits.clone(),
                    );
                }
                // The nonfatal demanded subset remains a normal harness comparison.
                c.check(
                    format!("grid digits positive subset p={p}/s={s}/{policy:?}"),
                    ScalarDiffSpec::new("truncate")
                        .column(floats(vec![Some(12.99); 4]))
                        .column(decimal(vec![Some(0), Some(1), Some(bound), None], p, s))
                        .decimal_overflow(policy)
                        .sparse_selections(0, SEEDS[2]),
                );
            }
        }
    }
    assert_eq!(declarations, 5643);
    c.finish();
}
#[test]
fn c06_p2_truncate_raw_minimum_digit_fatals_and_null_unselected_masks() {
    let mut c = Collection::default();
    let digits: Vec<ArrayRef> = vec![
        signed(vec![Some(i64::MIN)]),
        floats(vec![Some(-f64::MAX)]),
        Arc::new(Float32Array::from(vec![Some(-f32::MAX)])),
        decimal(vec![Some(-1)], 1, -19),
    ];
    for policy in POLICIES {
        for d in &digits {
            for primary in [
                Some(12.99),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                None,
            ] {
                let spec = ScalarDiffSpec::new("truncate")
                    .column(floats(vec![primary]))
                    .column(d.clone())
                    .decimal_overflow(policy);
                c.pair(
                    format!("fatal {primary:?}/{:?}/{policy:?}", d.data_type()),
                    spec,
                    None,
                );
            }
        }
    }
    for policy in POLICIES {
        let spec = ScalarDiffSpec::new("truncate")
            .column(floats(vec![Some(12.99), Some(12.99), None]))
            .column(signed(vec![Some(1), Some(i64::MIN), Some(i64::MIN)]))
            .decimal_overflow(policy);
        c.pair(
            format!("MIN outside demand/{policy:?}"),
            spec,
            Some(&[0, 2]),
        );
    }
    c.finish();
}
#[test]
fn c06_p2_truncate_wide_nonfinite_digits_and_decimal_successful_null_gap() {
    let mut c = Collection::default();
    for policy in POLICIES {
        c.check(
            format!("wide signed/{policy:?}"),
            ScalarDiffSpec::new("truncate")
                .column(floats(vec![Some(12.99); 5]))
                .column(signed(vec![
                    Some(i64::MAX),
                    Some(4294967296),
                    Some(4294967297),
                    Some(9007199254740992),
                    Some(9007199254740993),
                ]))
                .decimal_overflow(policy)
                .sparse_selections(7, SEEDS[0]),
        );
        c.check(
            format!("nonfinite float/{policy:?}"),
            ScalarDiffSpec::new("truncate")
                .column(floats(vec![Some(123.99); 8]))
                .column(floats(vec![
                    Some(1.9),
                    Some(-1.9),
                    Some(f64::MAX),
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(-0.0),
                    None,
                ]))
                .decimal_overflow(policy)
                .sparse_selections(7, SEEDS[1]),
        );
        c.check(
            format!("safe decimal output NULL/{policy:?}"),
            ScalarDiffSpec::new("truncate")
                .column(decimal(
                    vec![
                        Some(10_i128.pow(38) - 1),
                        Some(-(10_i128.pow(38) - 1)),
                        None,
                    ],
                    38,
                    -46,
                ))
                .decimal_overflow(policy)
                .sparse_selections(7, SEEDS[2]),
        );
        c.check(
            format!("lossy decimal digits/{policy:?}"),
            ScalarDiffSpec::new("truncate")
                .column(floats(vec![Some(12.99)]))
                .column(decimal(vec![Some(9007199254740993)], 38, 0))
                .decimal_overflow(policy),
        );
    }
    c.finish();
}

fn compiled_slot_program(arrays: &[ArrayRef]) -> Arc<novarocks_local_program::LocalProgram> {
    use crate::exec::expr::compiled_program::guarded_tests as host;
    use novarocks_functions::{
        EngineFunctionCatalogBuilder, FunctionArgument, FunctionId, FunctionKind,
        FunctionOverloadId, InstalledPureKernel, PureImplementationDeclaration,
        PureImplementationId, PureKernelAbi,
    };
    use novarocks_physical_plan::{
        ExprKind, FragmentBuilder, FragmentId, FragmentSink, LiteralValue, NodeId,
        PipelineDopDomain, ResultField, ResultPort, ValueDef, ValueId, ValueOrigin,
    };
    use novarocks_type_contract::{ControlShape, SemanticParameters};
    use std::collections::BTreeMap;
    let actual = builtin_engine_function_catalog();
    let mut catalog = EngineFunctionCatalogBuilder::new();
    catalog
        .register(
            actual
                .definition("truncate", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    let functions = catalog
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/truncate/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new("builtin.scalar/truncate/dynamic-v1")
                    .unwrap(),
                implementation: PureImplementationId::try_new(
                    "builtin.scalar/truncate/selected-v1",
                )
                .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        }])
        .unwrap();
    let fragment_id = FragmentId::new(196);
    let output = NodeId::new(0);
    let input = NodeId::new(1);
    let source = NodeId::new(2);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = vec![];
    let mut values = vec![];
    let mut args = vec![];
    let mut requests = vec![];
    for (i, array) in arrays.iter().enumerate() {
        let ty = typed(array, true);
        let value = ValueId::new(i as u32 + 10);
        let literal = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    expr: literal,
                },
            })
            .unwrap();
        projections.push((literal, value));
        values.push(value);
        args.push(
            builder
                .add_expression(output, ty.clone(), ExprKind::Value(value))
                .unwrap(),
        );
        requests.push(FunctionArgument::Value {
            value_type: ty,
            constant: None,
        });
    }
    builder
        .add_project(
            input,
            source,
            projections.into_boxed_slice(),
            values.into_boxed_slice(),
        )
        .unwrap();
    let author = host::author(&functions, "truncate", requests, ControlShape::Eager);
    let result_ty = author.result();
    let mut authors = BTreeMap::new();
    let call = host::call(&mut builder, &mut authors, author, args);
    let value = builder
        .add_value(
            result_ty.clone(),
            ValueOrigin::Expr {
                node: output,
                expr: call,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(call, value)]),
            Box::from([value]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&output].output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_ty),
            name: "truncate".into(),
            alias: None,
            value,
            ty: result_ty,
        }]),
    };
    host::compile_checked_fragment_with_parameters(
        &functions,
        fragment,
        &authors,
        result,
        SemanticParameters::try_new([]).unwrap(),
    )
}
#[test]
fn c06_p2_truncate_actual_compiler_slot_inputs_preserve_v1_values_and_exact_types() {
    use crate::exec::expr::compiled_program::CompiledExpressionInstance;
    use arrow::record_batch::RecordBatch;
    use novarocks_local_program::{
        ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeKind,
    };
    let mut c = Collection::default();
    for value in [
        floats(vec![Some(123.99), Some(-123.99), None, Some(-0.25)]),
        decimal(vec![Some(12399), Some(-12399), None, Some(-25)], 18, 2),
    ] {
        for binary in [false, true] {
            let mut arrays = vec![value.clone()];
            if binary {
                arrays.push(signed(vec![Some(1), Some(-1), Some(2), None]));
            }
            let program = compiled_slot_program(&arrays);
            let root_id = program.graph().root();
            let root_node = program
                .graph()
                .nodes()
                .iter()
                .find(|node| node.local_id() == Some(root_id))
                .unwrap();
            let ProgramNodeKind::Project { input, .. } = root_node.kind() else {
                panic!("compiled TRUNCATE root is not a Project")
            };
            let input_node = program
                .graph()
                .nodes()
                .iter()
                .find(|node| node.local_id() == Some(*input))
                .unwrap();
            let batch =
                RecordBatch::try_new(input_node.output_layout().schema().clone(), arrays.clone())
                    .unwrap();
            let root = ProgramExpressionRootSite::Node {
                node: root_id,
                role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
            };
            let spec = arrays
                .iter()
                .fold(ScalarDiffSpec::new("truncate"), |s, a| s.column(a.clone()))
                .decimal_overflow(DecimalOverflowPolicy::ReportError);
            let catalog = builtin_engine_function_catalog();
            let bound = resolve_like_sql(catalog, "truncate", CatalogKind::Scalar, &spec.arguments)
                .unwrap();
            let novarocks_functions::FunctionResultType::Scalar(result_type) =
                &bound.selected.result_type
            else {
                panic!("scalar result")
            };
            let legacy = ScalarCall {
                spec: &spec,
                legacy_kind: scalar_legacy_implementation("truncate").unwrap(),
                result_type,
                rows: 4,
            };
            for rows in [None, Some(&[0, 2, 3][..])] {
                c.cases += 1;
                let selection = rows.map_or_else(
                    || Selection::all(4),
                    |r| Selection::try_sparse(4, r).unwrap(),
                );
                let reference = legacy.evaluate_legacy_inner(rows).unwrap();
                let mut instance =
                    CompiledExpressionInstance::try_new(program.clone(), root, &HarnessControl)
                        .unwrap();
                let actual = instance
                    .evaluate(&batch, selection, &HarnessControl)
                    .unwrap();
                assert_eq!(actual.values().data_type(), &result_type.data_type);
                let label = format!(
                    "compiled source={:?}/binary={binary}/rows={rows:?}",
                    value.data_type()
                );
                if !actual.errors().is_empty() {
                    c.failures.push(format!(
                        "{label} newly visible errors {:?}",
                        actual.errors()
                    ));
                }
                assert_eq!(reference.len(), actual.values().len());
                for i in 0..reference.len() {
                    if let Err(e) = super::compare_value(
                        &reference,
                        i,
                        actual.values(),
                        i,
                        FloatComparison::Exact,
                    ) {
                        c.failures.push(format!("{label} ordinal={i}: {e}"));
                    }
                }
                c.selections += 1;
                c.rows += selection.len();
            }
        }
    }
    assert_eq!(c.cases, 8);
    c.finish();
}
