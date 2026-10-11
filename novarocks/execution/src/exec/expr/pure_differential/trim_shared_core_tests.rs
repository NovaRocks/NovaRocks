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
use arrow::array::{StringArray, new_empty_array};
use arrow::datatypes::Field;
use generate::InputProfile;

const NAMES: [&str; 3] = ["trim", "ltrim", "rtrim"];
const POLICIES: [DecimalOverflowPolicy; 2] = [
    DecimalOverflowPolicy::OutputNull,
    DecimalOverflowPolicy::ReportError,
];
const WHITE: &str = "\t\n\u{b}\u{c}\r \u{85}\u{a0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}";
#[derive(Default)]
struct Collection {
    profiles: usize,
    selections: usize,
    rows: usize,
    failures: Vec<String>,
}
impl Collection {
    fn check(&mut self, label: String, spec: ScalarDiffSpec) {
        self.profiles += 1;
        match run_scalar_differential(&spec) {
            Ok(s) => {
                self.selections += s.selections;
                self.rows += s.rows;
            }
            Err(e) => self.failures.push(format!("{label}: {e:?}")),
        }
    }
    fn finish(self) {
        println!(
            "C12-P1 profiles={} selections={} rows={} disagreements={}",
            self.profiles,
            self.selections,
            self.rows,
            self.failures.len()
        );
        assert!(self.profiles > 0);
        assert!(self.failures.is_empty(), "{}", self.failures.join("\n"));
    }
}
#[test]
fn c12_p1_trim_generated_sliced_all_actual_profiles_match_v1() {
    let mut c = Collection::default();
    for name in NAMES {
        for nullable in [false, true] {
            for policy in POLICIES {
                for seed in [0xC1201, 0xC1202, 0xC1203] {
                    let ty = FunctionValueType::new(DataType::Utf8, nullable);
                    let a = InputGenerator::new(seed).column(&ty, 259, &InputProfile::default());
                    let s = ScalarDiffSpec::new(name)
                        .typed_column(ty, a.slice(1, 257))
                        .decimal_overflow(policy)
                        .expect_result_type(FunctionValueType::new(DataType::Utf8, true))
                        .sparse_selections(7, seed);
                    c.check(format!("{name}/{nullable}/{policy:?}/{seed}"), s);
                }
            }
        }
    }
    c.finish();
}
#[test]
fn c12_p1_trim_fixed_white_nonwhite_long_constants_and_empty_match_v1() {
    let text = [
        String::new(),
        WHITE.into(),
        format!("{WHITE}x{WHITE}"),
        "\0".into(),
        "\u{200b}\u{180e}\u{feff}\u{1c}".into(),
        "e\u{301}👩‍💻".into(),
        format!("{}x{}", "\u{2003}".repeat(1025), "\u{3000}".repeat(1025)),
    ];
    let mut c = Collection::default();
    for name in NAMES {
        for policy in POLICIES {
            for nullable in [false, true] {
                let ty = FunctionValueType::new(DataType::Utf8, nullable);
                let mut v = text.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>();
                if nullable {
                    v.push(None);
                }
                let a = Arc::new(StringArray::from(v)) as ArrayRef;
                c.check(
                    format!("fixed/{name}/{nullable}/{policy:?}"),
                    ScalarDiffSpec::new(name)
                        .typed_column(ty.clone(), a)
                        .decimal_overflow(policy)
                        .sparse_selections(7, 0xC1203),
                );
                c.check(
                    format!("empty/{name}/{nullable}/{policy:?}"),
                    ScalarDiffSpec::new(name)
                        .typed_column(ty, new_empty_array(&DataType::Utf8))
                        .decimal_overflow(policy),
                );
            }
            for value in text.iter().map(|s| Some(s.as_str())).chain([None]) {
                for rows in [0, 1, 9] {
                    c.check(
                        format!("constant/{name}/{policy:?}/{rows}/{value:?}"),
                        ScalarDiffSpec::new(name)
                            .constant_array(Arc::new(StringArray::from(vec![value])))
                            .constant_rows(rows)
                            .decimal_overflow(policy)
                            .sparse_selections(5, 0xC1201),
                    );
                }
            }
        }
    }
    c.finish();
}
#[test]
fn c12_p1_trim_real_sql_column_calls_keep_ordered_requests_in_both_modes() {
    use crate::exec::expr::numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_single_field;
    use novarocks_physical_plan::{
        ExprKind, FunctionArgumentType, PhysicalCallDefinition, StaticFunctionArgument,
    };
    use novarocks_sql::compiler::SqlPhysicalEmissionMode;
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        for name in NAMES {
            let owner = sql_source_with_single_field(
                &format!("SELECT {name}(k) FROM t"),
                Field::new("k", DataType::Utf8, true),
                mode,
            );
            let mut count = 0;
            for f in owner.plan().fragments().values() {
                for (id, node) in f.expressions().iter() {
                    let ExprKind::FunctionCall { function, args } = &node.kind else {
                        continue;
                    };
                    if function.function_id.as_str() != format!("builtin.scalar/{name}/v1") {
                        continue;
                    }
                    let request = f
                        .call_requests()
                        .get(PhysicalCallDefinition::Expression(*id))
                        .unwrap();
                    assert_eq!(args.len(), 1);
                    assert_eq!(request.logical_argument_count, 1);
                    let StaticFunctionArgument::Value {
                        value_type,
                        constant,
                    } = &request.arguments[0]
                    else {
                        panic!("real value source")
                    };
                    assert!(constant.is_none());
                    let FunctionArgumentType::Value(selected) = &function.argument_types[0] else {
                        panic!("selected value source")
                    };
                    assert_eq!(value_type, selected);
                    assert_eq!(selected, &FunctionValueType::new(DataType::Utf8, true));
                    let child = f.expressions().get(args[0]).unwrap();
                    assert_eq!(&child.ty, selected);
                    assert!(
                        !matches!(child.kind, ExprKind::Literal(_)),
                        "column call must not become a constant"
                    );
                    assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
                    println!(
                        "C12-P1 retained {mode:?}/{name} definition={id:?} child={child:?} request={request:?}"
                    );
                    count += 1;
                }
            }
            assert!(count > 0, "real retained call required");
        }
    }
}
fn compiled_slot_program(
    name: &str,
    arrays: &[ArrayRef],
) -> Arc<novarocks_local_program::LocalProgram> {
    use crate::exec::expr::compiled_program::guarded_tests as host;
    use novarocks_functions::{
        EngineFunctionCatalogBuilder, FunctionArgument, FunctionId, FunctionKind,
        InstalledPureKernel, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
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
                .definition(name, FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    let functions = catalog
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: actual
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .binding_declaration()
                    .unwrap()
                    .overloads()[0]
                    .identity
                    .clone(),
                implementation: PureImplementationId::try_new(format!(
                    "builtin.scalar/{name}/selected-v1"
                ))
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
        let ty = FunctionValueType::new(array.data_type().clone(), true);
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
    let author = host::author(&functions, name, requests, ControlShape::Eager);
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
            name: name.into(),
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
fn c12_p1_trim_actual_compiled_slot_and_sparse_strict_null_match_v1() {
    use crate::exec::expr::compiled_program::CompiledExpressionInstance;
    use novarocks_local_program::{
        ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeKind,
    };
    let mut observations = 0;
    for name in NAMES {
        for values in [
            vec![Some(" \u{2003}x\u{a0}"), None, Some(""), Some(WHITE)],
            vec![None; 4],
        ] {
            let a = Arc::new(StringArray::from(values)) as ArrayRef;
            let program = compiled_slot_program(name, std::slice::from_ref(&a));
            let root_id = program.graph().root();
            let node = program
                .graph()
                .nodes()
                .iter()
                .find(|n| n.local_id() == Some(root_id))
                .unwrap();
            let ProgramNodeKind::Project { input, .. } = node.kind() else {
                panic!("actual compiled Project root")
            };
            let input_node = program
                .graph()
                .nodes()
                .iter()
                .find(|n| n.local_id() == Some(*input))
                .unwrap();
            let batch =
                RecordBatch::try_new(input_node.output_layout().schema().clone(), vec![a.clone()])
                    .unwrap();
            let root = ProgramExpressionRootSite::Node {
                node: root_id,
                role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
            };
            let spec = ScalarDiffSpec::new(name)
                .column(a)
                .decimal_overflow(DecimalOverflowPolicy::ReportError);
            let bound = resolve_like_sql(
                builtin_engine_function_catalog(),
                name,
                CatalogKind::Scalar,
                &spec.arguments,
            )
            .unwrap();
            let FunctionResultType::Scalar(ty) = &bound.selected.result_type else {
                panic!("scalar result")
            };
            let legacy = ScalarCall {
                spec: &spec,
                legacy_kind: scalar_legacy_implementation(name).unwrap(),
                result_type: ty,
                rows: 4,
            };
            for rows in [None, Some(&[0, 2, 3][..]), Some(&[][..])] {
                let selection = rows.map_or_else(
                    || Selection::all(4),
                    |r| Selection::try_sparse(4, r).unwrap(),
                );
                let expected = legacy.evaluate_legacy_inner(rows).unwrap();
                let mut instance =
                    CompiledExpressionInstance::try_new(program.clone(), root, &HarnessControl)
                        .unwrap();
                let actual = instance
                    .evaluate(&batch, selection, &HarnessControl)
                    .unwrap();
                assert_eq!(actual.selection(), selection);
                assert_eq!(actual.values().data_type(), &ty.data_type);
                assert!(actual.errors().is_empty());
                assert_eq!(actual.values().len(), expected.len());
                for i in 0..expected.len() {
                    compare_value(&expected, i, actual.values(), i, FloatComparison::Exact)
                        .unwrap();
                }
                observations += 1;
            }
        }
    }
    assert_eq!(observations, 18);
    println!("C12-P1 actual compiled ReportError observations={observations}");
}
