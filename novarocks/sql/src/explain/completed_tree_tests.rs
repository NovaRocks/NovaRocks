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
use crate::test_constants::{self as checked, FragmentBuilder};
use novarocks_expression_contract::{RootScope, UnaryOperator};
use novarocks_physical_plan::{
    OutputPort, PipelineDopDomain, PlanBuilder, PlanVersionId, ResultField, ResultPort,
    ResultValueDomain, ValueOrigin, ValueType,
};
use novarocks_type_contract::EvaluationDemand;

fn base() -> (FragmentBuilder, NodeId, ValueId, ValueType) {
    let mut builder = FragmentBuilder::new(FragmentId::new(0));
    let node = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let definition = builder.constant(node, ty.clone(), checked::i64(1));
    let expression = builder
        .unit(node)
        .unwrap()
        .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_values(
            node,
            Box::from([Box::from([expression])]),
            Box::from([value]),
        )
        .unwrap();
    (builder, node, value, ty)
}

fn finish(
    builder: FragmentBuilder,
    root: NodeId,
    value: ValueId,
    ty: ValueType,
    annotations: Vec<PlanAnnotation>,
) -> PhysicalPlan {
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
    let (fragment, constants) = fragment.into_parts();
    let fragment_id = fragment.id();
    let mut plan =
        PlanBuilder::new(PlanVersionId::try_new([1; 16]).unwrap()).with_constant_pools(constants);
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: OutputPort {
            node: root,
            columns: Box::from([value]),
        },
        fields: Box::from([ResultField {
            domain: ResultValueDomain::Plain,
            name: "result".into(),
            alias: None,
            value,
            ty,
        }]),
    })
    .unwrap();
    for annotation in annotations {
        plan.add_annotation(annotation);
    }
    plan.finish_observed(&crate::compiler::SqlCompileControl::unbounded())
        .unwrap()
}

fn project(
    kind: impl FnOnce(&mut FragmentBuilder, NodeId, &ValueType) -> ExprId,
    ty: ValueType,
    name: Option<&str>,
) -> PhysicalPlan {
    let (mut builder, input, _, _) = base();
    let node = builder.reserve_node_id().unwrap();
    let definition = kind(&mut builder, node, &ty);
    let expression = builder
        .unit(node)
        .unwrap()
        .root(definition, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::Expr {
                node,
                root: expression,
            },
        )
        .unwrap();
    builder
        .add_project(
            node,
            input,
            Box::from([(expression, value)]),
            Box::from([value]),
        )
        .unwrap();
    let annotations = name
        .into_iter()
        .map(|name| PlanAnnotation {
            subject: AnnotationSubject::Value(FragmentId::new(0), value),
            key: "sql.display_name".into(),
            value: name.into(),
        })
        .collect();
    finish(builder, node, value, ty, annotations)
}

fn small_budget() -> ExplainRenderBudget {
    ExplainRenderBudget::try_new(65_536, 512).unwrap()
}
fn assert_budget(error: SqlCompileError) {
    assert!(
        matches!(error, SqlCompileError::InvalidRequest(ref message) if message.contains("exceeds budget")),
        "{error}"
    );
}

#[test]
fn normal_bytes_keep_alias_suppression_literals_and_default_value_names() {
    for (name, expected) in [
        (Some("one"), "0:PROJECT [1 AS one]"),
        (Some("1"), "0:PROJECT [1]"),
        (None, "0:PROJECT [1 AS v1]"),
    ] {
        let plan = project(
            |builder, node, ty| builder.constant(node, ty.clone(), checked::i64(1)),
            ValueType::new(DataType::Int64, false),
            name,
        );
        assert_eq!(
            render_completed_plan_tree(
                &plan,
                ExplainLevel::Normal,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            [expected, "  1:VALUES (1 rows)"]
        );
    }
    let plan = project(
        |builder, node, ty| builder.constant(node, ty.clone(), checked::utf8("名🦀".into())),
        ValueType::new(DataType::Utf8, false),
        Some("'名🦀'"),
    );
    assert_eq!(
        render_completed_plan_tree(
            &plan,
            ExplainLevel::Normal,
            &crate::compiler::SqlCompileControl::unbounded()
        )
        .unwrap()[0],
        "0:PROJECT ['名🦀']"
    );
}

#[test]
fn large_borrowed_alias_and_literal_are_refused_by_the_streamed_line_budget() {
    let alias = "a".repeat(16 * 1024);
    let plan = project(
        |builder, node, ty| builder.constant(node, ty.clone(), checked::i64(1)),
        ValueType::new(DataType::Int64, false),
        Some(&alias),
    );
    assert_budget(
        render_tree_with_budget(&plan, ExplainLevel::Normal, small_budget()).unwrap_err(),
    );
    let literal = "🦀".repeat(4096);
    let plan = project(
        |builder, node, ty| builder.constant(node, ty.clone(), checked::utf8(literal)),
        ValueType::new(DataType::Utf8, false),
        Some("text"),
    );
    assert_budget(
        render_tree_with_budget(&plan, ExplainLevel::Normal, small_budget()).unwrap_err(),
    );
    let control = crate::compiler::SqlCompileControl::unbounded();
    let diagnostic = RenderDiagnostic::new(&control);
    // Admit the genuine source before checking the independent output bound.
    let policy = novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
            max_logical_elements: 16 << 20,
            max_retained_buffer_bytes: 32 << 20,
            ..checked::policy()
        },
    );
    let ty = ValueType::new(DataType::Utf8, false);
    let literal = novarocks_constant_contract::ConstantValue::from_utf8(
        std::sync::Arc::new(ty.try_to_field("large-source").unwrap()),
        ty,
        &"x".repeat(8 * 1024 * 1024 + 1),
        policy,
        novarocks_type_contract::CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let mut output = ExplainRenderOutput::new(ExplainRenderBudget::default());
    assert_budget(
        output
            .push(format_args!(
                "{}",
                text(|f| diagnostic.constant(&literal, f))
            ))
            .unwrap_err(),
    );
    // Hex expansion is admitted against output bytes, not borrowed input size.
    let ty = ValueType::new(DataType::Binary, false);
    let literal = novarocks_constant_contract::ConstantValue::from_binary(
        std::sync::Arc::new(ty.try_to_field("binary-source").unwrap()),
        ty,
        &[255; 256],
        checked::policy(),
        novarocks_type_contract::CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let mut output = ExplainRenderOutput::new(small_budget());
    assert_budget(
        output
            .push(format_args!(
                "{}",
                text(|f| diagnostic.constant(&literal, f))
            ))
            .unwrap_err(),
    );
}

#[test]
fn wide_boolean_arguments_remain_shallow_and_streamed() {
    let plan = project(
        |builder, node, ty| {
            let leaf = builder.constant(node, ty.clone(), checked::boolean(true));
            builder
                .unit(node)
                .unwrap()
                .add(
                    None,
                    ty.clone(),
                    ExprKind::Conjunction {
                        args: vec![leaf; 4096].into_boxed_slice(),
                    },
                )
                .unwrap()
        },
        ValueType::new(DataType::Boolean, false),
        Some("all"),
    );
    assert_budget(
        render_tree_with_budget(&plan, ExplainLevel::Normal, small_budget()).unwrap_err(),
    );
    let lines = render_completed_plan_tree(
        &plan,
        ExplainLevel::Normal,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap();
    assert_eq!(lines[0].matches("true").count(), 4096);
    assert!(!lines[0].contains("..."));
}

#[test]
fn line_bound_is_enforced_by_the_production_tree_traversal() {
    let (builder, root, value, ty) = base();
    let plan = finish(builder, root, value, ty, Vec::new());
    assert_budget(
        render_tree_with_budget(
            &plan,
            ExplainLevel::Verbose,
            ExplainRenderBudget::try_new(3, 4096).unwrap(),
        )
        .unwrap_err(),
    );
    // There is no extra source Vec: each accepted line belongs directly to
    // the collector, which refuses line 65,537 before rendering it.
    let mut output = ExplainRenderOutput::new(ExplainRenderBudget::default());
    for _ in 0..65_536 {
        output.push(format_args!("")).unwrap();
    }
    assert_budget(output.push(format_args!("beyond")).unwrap_err());
    assert_eq!(output.finish().len(), 65_536);
}

#[test]
fn valid_but_deep_node_and_expression_inputs_are_refused_without_elision() {
    let (mut builder, mut root, value, ty) = base();
    for _ in 0..MAX_TREE_DEPTH + 1 {
        let node = builder.reserve_node_id().unwrap();
        builder.add_limit(node, root, Some(1), 0).unwrap();
        root = node;
    }
    let plan = finish(builder, root, value, ty, Vec::new());
    assert!(
        matches!(render_completed_plan_tree(&plan, ExplainLevel::Normal, &crate::compiler::SqlCompileControl::unbounded()), Err(SqlCompileError::InvalidRequest(message)) if message.contains("depth bound"))
    );
    let plan = project(
        |builder, node, ty| {
            let mut expression = builder.constant(node, ty.clone(), checked::boolean(true));
            for _ in 0..ExprText::MAX_DEPTH + 1 {
                expression = builder
                    .unit(node)
                    .unwrap()
                    .add(
                        None,
                        ty.clone(),
                        ExprKind::Unary {
                            op: UnaryOperator::Not,
                            expr: expression,
                        },
                    )
                    .unwrap();
            }
            expression
        },
        ValueType::new(DataType::Boolean, false),
        Some("deep"),
    );
    assert_budget(
        render_completed_plan_tree(
            &plan,
            ExplainLevel::Normal,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap_err(),
    );
}

#[test]
fn node_prepass_rejects_a_back_edge_and_preserves_shared_node_numbering() {
    let fragment = FragmentId::new(0);
    let mut entries = (0..3)
        .map(|id| DisplayNode {
            key: (fragment, NodeId::new(id)),
            display: usize::MAX,
            active: false,
        })
        .collect::<Vec<_>>();
    let inputs = [
        vec![NodeId::new(1), NodeId::new(2)],
        vec![NodeId::new(2)],
        vec![NodeId::new(0)],
    ];
    let mut next = 0;
    assert!(
        assign_display_ids(
            &mut entries,
            fragment,
            NodeId::new(0),
            0,
            &mut next,
            &|id| Some(inputs[id.get() as usize].as_slice())
        )
        .is_err()
    );
    let mut entries = (0..3)
        .map(|id| DisplayNode {
            key: (fragment, NodeId::new(id)),
            display: usize::MAX,
            active: false,
        })
        .collect::<Vec<_>>();
    let inputs = [
        vec![NodeId::new(1), NodeId::new(2)],
        vec![NodeId::new(2)],
        vec![],
    ];
    let mut next = 0;
    assign_display_ids(
        &mut entries,
        fragment,
        NodeId::new(0),
        0,
        &mut next,
        &|id| Some(inputs[id.get() as usize].as_slice()),
    )
    .unwrap();
    assert_eq!(next, 3);
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.display)
            .collect::<Vec<_>>(),
        [0, 1, 3]
    );
}

#[test]
fn borrowed_stats_and_name_indexes_keep_first_node_and_last_value_semantics() {
    let (builder, root, value, ty) = base();
    let node_subject = AnnotationSubject::Node(FragmentId::new(0), root);
    let value_subject = AnnotationSubject::Value(FragmentId::new(0), value);
    let annotations = vec![
        PlanAnnotation {
            subject: node_subject,
            key: "optimizer.statistics".into(),
            value: "rows=1.6, conf=high".into(),
        },
        PlanAnnotation {
            subject: node_subject,
            key: "optimizer.statistics".into(),
            value: "rows=9".into(),
        },
        PlanAnnotation {
            subject: value_subject,
            key: "sql.display_name".into(),
            value: "before".into(),
        },
        PlanAnnotation {
            subject: value_subject,
            key: "sql.display_name".into(),
            value: "ice.sales.final".into(),
        },
    ];
    let plan = finish(builder, root, value, ty, annotations);
    let context = TreeContext::new(&plan, ExplainLevel::Costs).unwrap();
    assert_eq!(
        context.value_name(FragmentId::new(0), value).to_string(),
        "ice.sales.final"
    );
    assert_eq!(
        context
            .value_name(FragmentId::new(0), value)
            .column()
            .to_string(),
        "final"
    );
    assert_eq!(
        render_completed_plan_tree(
            &plan,
            ExplainLevel::Costs,
            &crate::compiler::SqlCompileControl::unbounded()
        )
        .unwrap()
        .last()
        .unwrap(),
        "0:VALUES (1 rows) stats={rows=2 conf=high}"
    );
    for (input, expected) in [
        ("-1", "?"),
        ("NaN", "?"),
        ("inf", ">=1e15"),
        ("1e15", ">=1e15"),
        ("not-a-count", "not-a-count"),
    ] {
        assert_eq!(RowCount(input).to_string(), expected);
    }
}

#[test]
fn project_expression_and_alias_comparison_share_one_bounded_formatting_pass() {
    use std::cell::Cell;
    struct Probe<'a> {
        calls: &'a Cell<usize>,
        text: &'a str,
    }
    impl Display for Probe<'_> {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.calls.set(self.calls.get() + 1);
            formatter.write_str(self.text)
        }
    }
    for (name, expected) in [("same", "same"), ("alias", "same AS alias")] {
        let calls = Cell::new(0);
        let display = ProjectExpressionDisplay {
            expression: Probe {
                calls: &calls,
                text: "same",
            },
            name: ValueName {
                name: Some(name),
                value: ValueId::new(1),
            },
        };
        let mut output = ExplainRenderOutput::new(small_budget());
        output.push(format_args!("{display}")).unwrap();
        assert_eq!(output.finish(), [expected]);
        assert_eq!(calls.get(), 1);
    }
    let huge = "prefix".repeat(1024);
    let calls = Cell::new(0);
    let display = ProjectExpressionDisplay {
        expression: Probe {
            calls: &calls,
            text: &huge,
        },
        name: ValueName {
            name: Some(&huge),
            value: ValueId::new(1),
        },
    };
    let mut output = ExplainRenderOutput::new(small_budget());
    assert_budget(output.push(format_args!("{display}")).unwrap_err());
    assert_eq!(calls.get(), 1);
    assert!(output.finish().is_empty());
}
