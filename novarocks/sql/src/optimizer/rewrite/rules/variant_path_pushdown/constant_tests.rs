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

use super::*;
use crate::analysis::{ExprKind, TypedExpr};
use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
use crate::optimizer::operator::ScalarProjectItem;
use crate::optimizer::options::SessionOptimizerSettings;
use crate::planner::table::{
    SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
};
use arrow::array::{
    Array, ArrayRef, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
};
use novarocks_functions::{ConstantPolicy, ConstantPool, ConstantValue};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType,
    PureCompileControl, ValueLogicalType,
};
use novarocks_types::schema::{ColumnDef, SqlType};
use std::{
    cell::RefCell,
    collections::HashMap,
    num::{NonZeroU32, NonZeroU64},
    rc::Rc,
    sync::{Arc, Mutex},
};

struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::new(vec![]),
            refusal: None,
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.refusal {
            assert!(trace.len() < at, "no callback after primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((at, cause)) if trace.len() == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 64,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 16,
        max_type_nodes: 128,
        max_dictionary_depth: 4,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 24,
        max_library_validation_bytes: 1 << 24,
    }
}
fn text(
    carrier: DataType,
    values: &[Option<&str>],
    ordinal: u32,
    logical: ValueLogicalType,
) -> ConstantValue {
    let array: ArrayRef = match carrier {
        DataType::Utf8 => Arc::new(StringArray::from(values.to_vec())),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(values.to_vec())),
        DataType::Utf8View => Arc::new(StringViewArray::from(values.to_vec())),
        _ => panic!("text fixture carrier"),
    };
    let ty = FunctionValueType::try_with_logical_type(
        carrier,
        values.iter().any(Option::is_none),
        logical,
    )
    .unwrap();
    constant(array, ty, ordinal)
}

fn json_text(encoded: &[u8]) -> ConstantValue {
    let ty = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    constant(Arc::new(LargeBinaryArray::from(vec![encoded])), ty, 0)
}

fn constant(array: ArrayRef, ty: FunctionValueType, ordinal: u32) -> ConstantValue {
    let field = Arc::new(
        ty.try_to_field("source-path")
            .unwrap()
            .with_metadata(HashMap::from([(
                "provider.unknown".to_owned(),
                "retained".to_owned(),
            )])),
    );
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
struct Fixture {
    arena: Rc<RefCell<ScalarArena>>,
    factory: Rc<RefCell<ColumnRefFactory>>,
    expression: OptExpr,
    call: ScalarId,
    path: ConstantValue,
    target: ConstantValue,
}
fn fixture(path: ConstantValue, target: ConstantValue, strict_predicate: bool) -> Fixture {
    let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
    let source_ty = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Variant,
    )
    .unwrap();
    let source_column = factory
        .borrow_mut()
        .create(None, "v".to_owned(), source_ty.clone());
    let output = factory.borrow_mut().create(
        None,
        "answer".to_owned(),
        FunctionValueType::new(DataType::Int64, true),
    );
    let source = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: source_column,
            qualifier: None,
            column: "v".to_owned(),
        },
        value_type: source_ty.clone(),
    };
    let arguments = [
        source,
        TypedExpr {
            kind: ExprKind::Constant(path.clone()),
            value_type: path.value_type().clone(),
        },
        TypedExpr {
            kind: ExprKind::Constant(target.clone()),
            value_type: target.value_type().clone(),
        },
    ];
    // This is the existing SQL fixture binding author, not a fake pure kernel
    // installation. The rule preserves this exact frozen binding in its slot.
    let binding = crate::analysis::test_function_binding(
        "variant_get",
        &arguments,
        DataType::Int64,
        true,
        crate::functions::FunctionVolatility::Immutable,
    );
    let mut arena = ScalarArena::with_constant_policy(policy());
    let source = arena
        .intern_observed(
            ScalarNode::ColumnRef(source_column),
            source_ty.clone(),
            &Control::good(),
        )
        .unwrap();
    let path_id = arena
        .intern_observed(
            ScalarNode::Constant(path.clone()),
            path.value_type().clone(),
            &Control::good(),
        )
        .unwrap();
    let target_id = arena
        .intern_observed(
            ScalarNode::Constant(target.clone()),
            target.value_type().clone(),
            &Control::good(),
        )
        .unwrap();
    let call = arena
        .intern_observed(
            ScalarNode::FunctionCall {
                name: "variant_get".to_owned(),
                args: vec![source, path_id, target_id],
                distinct: false,
                volatility: binding.semantics.volatility,
                binding,
            },
            FunctionValueType::new(DataType::Int64, true),
            &Control::good(),
        )
        .unwrap();
    let nested = strict_predicate.then(|| {
        arena
            .intern_observed(
                ScalarNode::Nested(call),
                FunctionValueType::new(DataType::Int64, true),
                &Control::good(),
            )
            .unwrap()
    });
    let scan = ScanOp {
        database: "db".to_owned(),
        table: TableDef {
            name: "t".to_owned(),
            columns: vec![ColumnDef {
                name: "v".to_owned(),
                data_type: DataType::LargeBinary,
                nullable: true,
                write_default: None,
                logical_type: Some(SqlType::Variant),
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::Sql(SqlScanSource::new(
                SqlTableBindingId::new(
                    SqlTableBindingScopeId::new(NonZeroU64::new(1).unwrap()),
                    NonZeroU32::new(1).unwrap(),
                ),
                SqlTableIdentity {
                    catalog: "ice".to_owned(),
                    namespace: "db".to_owned(),
                    table: "t".to_owned(),
                },
                SqlScanKind::Data {
                    version: SqlTableVersionSelector::Current,
                },
            )),
        },
        alias: None,
        stats_ref: None,
        columns: vec![OutputColumn {
            column_id: source_column,
            name: "v".to_owned(),
            value_type: source_ty,
            is_internal: false,
        }],
        predicates: nested.into_iter().collect(),
        required_columns: None,
        variant_columns: vec![],
        mv_rewritten_from: None,
    };
    Fixture {
        arena: Rc::new(RefCell::new(arena)),
        factory,
        expression: OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![ScalarProjectItem {
                    expr: call,
                    output_name: "answer".to_owned(),
                    output_column_id: output,
                    expr_display: None,
                }],
                output_qualifier: None,
            }),
            vec![OptExpr::leaf(Operator::LogicalScan(scan))],
        ),
        call,
        path,
        target,
    }
}
fn apply(
    fixture: &Fixture,
    control: &dyn PureCompileControl,
) -> Result<RewriteResult, SqlCompileError> {
    let mut context = RewriteContext::for_query_with_settings(
        SessionOptimizerSettings::default(),
        DecimalOverflowPolicy::OutputNull,
        control,
    );
    context.set_column_ref_factory(Rc::clone(&fixture.factory));
    context.set_scalar_arena(Rc::clone(&fixture.arena));
    VariantPathPushdownRule.apply(fixture.expression.clone(), &mut context)
}

#[test]
fn selected_multirow_text_carriers_push_exact_path_type_and_preserve_metadata_source() {
    for carrier in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let path = text(
            carrier.clone(),
            &[Some("$[0]"), Some("$.a['not plain']"), Some("$.wrong")],
            1,
            ValueLogicalType::Physical,
        );
        let target = text(
            carrier,
            &[Some("not-a-type"), Some("string"), Some("bigint")],
            2,
            ValueLogicalType::Physical,
        );
        let f = fixture(path.clone(), target.clone(), true);
        let request = variant_request_scalar(&f.arena.borrow(), f.call, &Control::good())
            .unwrap()
            .unwrap();
        assert_eq!(request.canonical_path, "$.a['not plain']");
        assert_eq!(request.requested_type_literal, "bigint");
        assert_eq!(request.requested_type, DataType::Int64);
        let RewriteResult::Changed(rewritten) = apply(&f, &Control::good()).unwrap() else {
            panic!("materialized path must push down")
        };
        let Operator::LogicalScan(scan) = &rewritten.children[0].op else {
            panic!("scan")
        };
        assert_eq!(scan.variant_columns.len(), 1);
        assert_eq!(
            scan.variant_columns[0].canonical_path(),
            request.canonical_path
        );
        assert_eq!(scan.variant_columns[0].binding(), &request.binding);
        for original in [&path, &target] {
            assert_eq!(
                original.pool().field_ref().metadata()["provider.unknown"],
                "retained"
            );
            assert_eq!(
                original.ordinal(),
                if std::ptr::eq(original, &path) { 1 } else { 2 }
            );
        }
        let arena = f.arena.borrow();
        let ScalarNode::FunctionCall { args, .. } = arena.node(f.call) else {
            panic!("original call")
        };
        for (id, original) in [(args[1], &path), (args[2], &target)] {
            let ScalarNode::Constant(retained) = arena.node(id) else {
                panic!("original constant")
            };
            assert_eq!(
                retained.pool().backing_identity(),
                original.pool().backing_identity()
            );
            assert_eq!(retained.ordinal(), original.ordinal());
            assert!(Arc::ptr_eq(
                retained.pool().field_ref(),
                original.pool().field_ref()
            ));
        }
    }
}

#[test]
fn selected_null_nominal_json_and_unparseable_paths_remain_nonmatches_without_publication() {
    let valid_path = text(
        DataType::Utf8,
        &[Some("$.a")],
        0,
        ValueLogicalType::Physical,
    );
    let valid_target = text(
        DataType::Utf8,
        &[Some("bigint")],
        0,
        ValueLogicalType::Physical,
    );
    for (path, target) in [
        (
            text(
                DataType::Utf8,
                &[Some("$.unused"), None],
                1,
                ValueLogicalType::Physical,
            ),
            valid_target.clone(),
        ),
        (
            valid_path.clone(),
            text(
                DataType::Utf8,
                &[Some("bigint"), None],
                1,
                ValueLogicalType::Physical,
            ),
        ),
        (json_text(b"\x01\x44\x03$.a"), valid_target.clone()),
        (valid_path.clone(), json_text(b"\x01\x44\x06bigint")),
        (
            text(
                DataType::Utf8,
                &[Some("$[0]")],
                0,
                ValueLogicalType::Physical,
            ),
            valid_target.clone(),
        ),
        (
            valid_path.clone(),
            text(
                DataType::Utf8,
                &[Some("array")],
                0,
                ValueLogicalType::Physical,
            ),
        ),
    ] {
        let f = fixture(path, target, false);
        let before = f.arena.borrow().node_count();
        assert!(
            variant_request_scalar(&f.arena.borrow(), f.call, &Control::good())
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            apply(&f, &Control::good()).unwrap(),
            RewriteResult::Unchanged
        ));
        assert_eq!(f.arena.borrow().node_count(), before);
    }
}

#[test]
fn materialized_variant_requests_keep_original_control_at_all_read_and_rewrite_boundaries() {
    let key = "a".repeat(2048);
    let path_text = format!("$.{key}");
    let make = || {
        fixture(
            text(
                DataType::LargeUtf8,
                &[Some("$.unused"), Some(&path_text)],
                1,
                ValueLogicalType::Physical,
            ),
            text(
                DataType::Utf8View,
                &[Some("string"), Some("bigint")],
                1,
                ValueLogicalType::Physical,
            ),
            true,
        )
    };
    let f = make();
    let baseline = Control::good();
    assert!(matches!(
        apply(&f, &baseline).unwrap(),
        RewriteResult::Changed(_)
    ));
    let trace = baseline.trace();
    assert!(trace.len() > 3);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=trace.len() {
            let f = make();
            let before = f.arena.borrow().node_count();
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let error = apply(&f, &control).unwrap_err();
            assert!(matches!(
                (cause.clone(), error),
                (CompileControlError::Cancelled, SqlCompileError::Cancelled)
                    | (
                        CompileControlError::DeadlineExceeded,
                        SqlCompileError::DeadlineExceeded
                    )
                    | (
                        CompileControlError::ResourceExhausted,
                        SqlCompileError::ResourceExhausted
                    )
            ));
            assert_eq!(control.trace(), trace[..at]);
            assert_eq!(f.arena.borrow().node_count(), before);
            assert_eq!(f.path.ordinal(), 1);
            assert_eq!(f.target.ordinal(), 1);
        }
    }
}
