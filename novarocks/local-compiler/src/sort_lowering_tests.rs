// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::values_lowering_tests::{build, checked_pool, compile_options, providers};
use super::*;
use crate::sort::lower_sort;
use crate::{FragmentCompileError, compile_fragment};
use arrow_array::StringArray;
use novarocks_local_program::{
    ProgramExprId, ProgramExpressionArena, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind,
};
use novarocks_physical_plan::{
    ExprNode, NullOrdering, SortDirection, SortExpr, SortMode, TopNPhase,
};
use novarocks_type_contract::ValueLogicalType;

pub(super) fn package(
    source: &ConstantPool,
    keys: usize,
    tail: bool,
    mut mode: SortMode,
    topn: bool,
) -> Arc<FragmentPackage> {
    let base = build(
        std::slice::from_ref(source),
        &[vec![(0, 2)], vec![(0, 1)]],
        &[0],
        false,
    );
    let mut input = (*base).clone().into_input();
    let mut builder = FragmentBuilder::new(input.fragment.id());
    for value in input.fragment.values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for (_, expr) in input.fragment.expressions().iter() {
        builder.insert_expression(expr.clone()).unwrap();
    }
    for node in input.fragment.nodes().values() {
        builder.insert_node_unchecked(node.clone()).unwrap();
    }
    let values = NodeId::new(u32::MAX);
    let column = input.fragment.nodes()[&values].output.columns[0];
    let sort = NodeId::new(0);
    let key = ExprId::new(1000);
    builder
        .insert_expression(ExprNode {
            id: key,
            owner: sort,
            lambda_scope: None,
            ty: source.value_type().clone(),
            kind: ExprKind::Value(column),
        })
        .unwrap();
    match &mut mode {
        SortMode::Analytic { partition_by } | SortMode::PartitionTopN { partition_by, .. } => {
            *partition_by = Box::from([SortExpr {
                expr: key,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]);
        }
        SortMode::Global => {}
    }
    let order: Box<[_]> = (0..keys)
        .map(|i| SortExpr {
            expr: key,
            direction: if i % 2 == 0 {
                SortDirection::Descending
            } else {
                SortDirection::Ascending
            },
            null_ordering: if i % 2 == 0 {
                NullOrdering::First
            } else {
                NullOrdering::Last
            },
        })
        .collect();
    if topn {
        builder
            .add_top_n(sort, values, order, 2, 1, TopNPhase::Single)
            .unwrap();
    } else {
        builder.add_sort(sort, values, order, mode).unwrap();
    }
    let root = if tail {
        let filter = NodeId::new(7);
        let predicate = builder
            .add_expression(
                filter,
                FunctionValueType::new(DataType::Boolean, false),
                ExprKind::Literal(novarocks_physical_plan::LiteralValue::Boolean(true)),
            )
            .unwrap();
        builder
            .add_filter(filter, sort, Box::from([predicate]))
            .unwrap();
        let project = NodeId::new(8);
        let expr = builder
            .add_expression(
                project,
                source.value_type().clone(),
                ExprKind::Value(column),
            )
            .unwrap();
        // The original property author retains named ordering only for values
        // actually kept in the output. A new alias ValueId loses that fact.
        let output = column;
        builder
            .add_project(
                project,
                filter,
                Box::from([(expr, output)]),
                Box::from([output]),
            )
            .unwrap();
        let limit = NodeId::new(9);
        builder.add_limit(limit, project, Some(1), 0).unwrap();
        limit
    } else {
        sort
    };
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control::good()).unwrap();
    let domain = EvaluationRegionId::new(0);
    let mut uses = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(position, (&site, root))| {
            let use_id = ExpressionUseId::new(match position {
                0 => 0,
                1 => u32::MAX,
                n => n as u32 - 1,
            });
            uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    region: domain,
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, use_id)
        })
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationRegion {
            id: domain,
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let expression_uses =
        PhysicalRootUses::try_new(&fragment, flow, bindings, &Control::good()).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &Control::good())
        .unwrap();
    let output = fragment.nodes()[&root].output.clone();
    let fields = output
        .columns
        .iter()
        .map(|&value| ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: "result".into(),
            alias: Some("original-label".into()),
            value,
            ty: fragment.values()[&value].ty.clone(),
        })
        .collect();
    input.result = Some(ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output,
        fields,
    });
    input.fragment = fragment;
    input.expression_uses = expression_uses;
    input.calls = calls;
    Arc::new(FragmentPackage::try_new(input, package_admission(), &Control::good()).unwrap())
}
fn compile(
    package: Arc<FragmentPackage>,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    compile_fragment(providers(package), &functions(), compile_options(), control)
}
fn component(
    package: &FragmentPackage,
    control: &dyn PureCompileControl,
    missing: bool,
) -> Result<(ProgramNodeKind, novarocks_local_program::StaticLayout), FragmentCompileError> {
    let good = compile(Arc::new(package.clone()), &Control::good()).unwrap();
    let layout = good.graph().nodes()[0].output_layout();
    let mut ids = BTreeMap::from([(ExprId::new(1000), ProgramExprId::new(0))]);
    if missing {
        ids.clear();
    }
    lower_sort(
        &package.fragment().nodes()[&NodeId::new(0)],
        ProgramNodeId::new(0),
        layout,
        &ids,
        control,
    )
}
fn is_control(error: &FragmentCompileError, cause: CompileControlError) -> bool {
    matches!(error, FragmentCompileError::Control(actual) if *actual == cause)
}
#[test]
fn global_sort_public_compile_preserves_keys_occurrences_channels_and_inactive_fields() {
    let pool = integer_pool();
    let package = package(&pool, 3, false, SortMode::Global, false);
    let program = compile(package, &Control::good()).unwrap();
    let ProgramNodeKind::Sort {
        input,
        use_top_n,
        order_by,
        limit,
        offset,
        topn_type,
        max_buffered_rows,
        max_buffered_bytes,
        partition_exprs,
        partition_limit,
    } = program.graph().nodes()[1].kind()
    else {
        panic!("sort")
    };
    assert_eq!(input.index(), 0);
    assert!(!use_top_n);
    assert_eq!(*limit, None);
    assert_eq!(*offset, 0);
    assert!(matches!(
        topn_type,
        novarocks_local_program::SortTopNType::RowNumber
    ));
    assert_eq!(*max_buffered_rows, None);
    assert_eq!(*max_buffered_bytes, None);
    assert!(partition_exprs.is_empty());
    assert_eq!(*partition_limit, None);
    assert_eq!(
        order_by
            .iter()
            .map(|k| (k.asc, k.nulls_first))
            .collect::<Vec<_>>(),
        [(false, true), (true, false), (false, true)]
    );
    assert!(order_by.iter().all(|key| key.expr == order_by[0].expr));
    assert_eq!(
        program.graph().nodes()[0].output_layout().slots(),
        program.graph().nodes()[1].output_layout().slots()
    );
    let roots = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let roles: Vec<_> = roots
        .bindings()
        .keys()
        .filter_map(|site| match site {
            ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::SortOrder { key },
            } if node.index() == 1 => Some(*key),
            _ => None,
        })
        .collect();
    assert_eq!(roles, [0, 1, 2]);
    assert_eq!(roots.bindings().len(), 3);
    assert_eq!(program.checked().slots().len(), 3);
}
#[test]
fn global_sort_downstream_project_filter_limit_keep_original_ordered_source() {
    let pool = checked_pool(
        Arc::new(StringArray::from(vec![Some("{}"), None, Some("[1]")])),
        true,
        ValueLogicalType::Json,
    );
    let package = package(&pool, 2, true, SortMode::Global, false);
    assert!(
        package.fragment().nodes()[&NodeId::new(9)]
            .output_properties
            .ordering
            .len()
            == 2
    );
    let program = compile(package, &Control::good()).unwrap();
    assert_eq!(program.graph().nodes().len(), 5);
    assert!(matches!(
        program.graph().nodes()[2].kind(),
        ProgramNodeKind::Filter { .. }
    ));
    assert!(matches!(
        program.graph().nodes()[3].kind(),
        ProgramNodeKind::Project { .. }
    ));
    assert!(matches!(
        program.graph().nodes()[4].kind(),
        ProgramNodeKind::Limit {
            limit: Some(1),
            offset: 0,
            ..
        }
    ));
    for node in program.graph().nodes() {
        assert_eq!(
            node.output_layout().schema().field(0).data_type(),
            &DataType::Utf8
        );
    }
    let ty = program
        .checked()
        .channels()
        .expressions()
        .types()
        .get(&ProgramExpressionArena::Main)
        .unwrap();
    assert!(ty.iter().any(|ty| matches!(ty, novarocks_type_contract::FunctionArgumentType::Value(ty) if ty.logical_type()==ValueLogicalType::Json && ty.nullable())));
}
#[test]
fn partition_topn_sort_mode_stays_explicitly_unsupported() {
    let source = integer_pool();
    let package = package(
        &source,
        1,
        false,
        SortMode::PartitionTopN {
            partition_by: Box::default(),
            limit: 1,
            kind: novarocks_physical_plan::PartitionTopNType::RowNumber,
        },
        false,
    );
    assert!(
        matches!(compile(package,&Control::good()),Err(FragmentCompileError::Unsupported {node:Some(node),feature:"sort mode without a local owner"}) if node==NodeId::new(0))
    );
}
#[test]
fn analytic_sort_leads_with_its_partition_keys_and_binds_their_roots() {
    let source = integer_pool();
    let package = package(
        &source,
        1,
        false,
        SortMode::Analytic {
            partition_by: Box::default(),
        },
        false,
    );
    let program = compile(package, &Control::good()).unwrap();
    let sort = ProgramNodeId::new(1);
    let ProgramNodeKind::Sort {
        order_by,
        partition_exprs,
        partition_limit,
        use_top_n,
        ..
    } = program.graph().nodes()[sort.index()].kind()
    else {
        panic!("analytic sort lowers to the local sort owner");
    };
    assert_eq!((order_by.len(), partition_exprs.len()), (1, 1));
    // Partition keys keep their own frozen direction and NULL placement.
    assert!(partition_exprs[0].asc && !partition_exprs[0].nulls_first);
    assert!(!order_by[0].asc && order_by[0].nulls_first);
    assert!(partition_limit.is_none() && !use_top_n);
    let bindings = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot()
        .bindings();
    for role in [
        ProgramNodeExpressionRole::SortPartition { key: 0 },
        ProgramNodeExpressionRole::SortOrder { key: 0 },
    ] {
        assert!(bindings.contains_key(&ProgramExpressionRootSite::Node { node: sort, role }));
    }
}
#[test]
fn global_sort_component_success_and_missing_key_every_actual_control_prefix() {
    let package = package(&integer_pool(), 2, false, SortMode::Global, false);
    for missing in [false, true] {
        let baseline = Control::good();
        let result = component(&package, &baseline, missing);
        if missing {
            assert!(matches!(
                result,
                Err(FragmentCompileError::Invalid("missing sort key expression"))
            ));
        } else {
            result.unwrap();
        }
        let trace = baseline.trace();
        assert!(trace.last().unwrap().1 > 0);
        for at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control::refusing(at, cause.clone());
                let error = component(&package, &control, missing).unwrap_err();
                assert!(is_control(&error, cause));
                assert_eq!(control.trace(), trace[..at]);
            }
        }
    }
}
#[test]
fn global_sort_public_compilation_all_actual_first_causes_and_wide_key_quantum() {
    let package = package(&integer_pool(), 1, false, SortMode::Global, false);
    let baseline = Control::good();
    compile(package.clone(), &baseline).unwrap();
    let trace = baseline.trace();
    for at in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::refusing(at, cause.clone());
            let error = compile(package.clone(), &control).unwrap_err();
            assert!(is_control(&error, cause));
            assert_eq!(control.trace(), trace[..at]);
        }
    }
    let wide = self::package(&integer_pool(), 320, false, SortMode::Global, false);
    let baseline = Control::good();
    let (kind, _) = component(&wide, &baseline, false).unwrap();
    assert!(matches!(kind,ProgramNodeKind::Sort {order_by,..} if order_by.len()==320));
    let trace = baseline.trace();
    let positions: Vec<_> = trace
        .iter()
        .enumerate()
        .filter_map(|(i, (_, n))| (*n == 256).then_some(i + 1))
        .collect();
    assert!(!positions.is_empty());
    for at in positions.into_iter().chain([1, trace.len()]) {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::refusing(at, cause.clone());
            assert!(is_control(
                &component(&wide, &control, false).unwrap_err(),
                cause
            ));
            assert_eq!(control.trace(), trace[..at]);
        }
    }
}
