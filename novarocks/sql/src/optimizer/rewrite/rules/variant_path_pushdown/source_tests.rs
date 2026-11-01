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
use arrow::array::{Array, StringArray};
use novarocks_functions::{ConstantPool, ConstantValue, FunctionArgument};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use std::sync::Arc;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}

fn selected_text(value: &str, sentinel: &str, logical: ValueLogicalType) -> ConstantValue {
    let ty = FunctionValueType::try_with_logical_type(DataType::Utf8, false, logical).unwrap();
    let field = ty.try_to_field("original_variant_argument").unwrap();
    let mut metadata = field.metadata().clone();
    metadata.insert("provider.unknown".to_string(), "original-field-metadata".to_string());
    let field = Arc::new(field.with_metadata(metadata));
    let data = StringArray::from(vec![sentinel, value, "unused-tail"]).to_data();
    ConstantPool::try_new(
        field,
        ty,
        data,
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap()
    .value(1)
    .unwrap()
}

fn physical_text(value: &str, sentinel: &str) -> ConstantValue {
    selected_text(value, sentinel, ValueLogicalType::Physical)
}

fn constant_expr(value: &ConstantValue) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Constant(value.clone()),
        value_type: value.value_type().clone(),
    }
}

fn call(column: &OutputColumn, path: &ConstantValue, target: &ConstantValue) -> TypedExpr {
    variant_get_with_args(
        "variant_get",
        vec![
            column_ref(column),
            constant_expr(path),
            constant_expr(target),
        ],
    )
}

fn project_calls(
    source: LogicalPlanNode,
    expressions: Vec<TypedExpr>,
    factory: &Rc<RefCell<ColumnRefFactory>>,
) -> LogicalPlanNode {
    LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items: expressions
                .into_iter()
                .enumerate()
                .map(|(ordinal, expr)| ProjectItem {
                    expr,
                    output_name: format!("answer_{ordinal}"),
                    output_column_id: add_column(
                        factory,
                        &format!("answer_{ordinal}"),
                        DataType::Int64,
                        true,
                        false,
                    )
                    .column_id,
                })
                .collect(),
            output_qualifier: None,
        }),
        vec![source],
        None,
    )
}

fn captured_value(
    source: &crate::binding::CapturedLogicalCallArguments,
    index: usize,
) -> &ConstantValue {
    let FunctionArgument::Value {
        constant: Some(value),
        ..
    } = &source.request().arguments[index]
    else {
        panic!("original materialized request channel")
    };
    value
}

fn assert_same_source(actual: &ConstantValue, original: &ConstantValue) {
    assert_eq!(actual.ordinal(), 1);
    assert_eq!(actual.value_type(), original.value_type());
    assert_eq!(
        actual.pool().backing_identity(),
        original.pool().backing_identity()
    );
    assert!(Arc::ptr_eq(
        actual.pool().field_ref(),
        original.pool().field_ref()
    ));
    assert_eq!(
        actual.pool().field_ref().name(),
        "original_variant_argument"
    );
    assert_eq!(
        actual.pool().field_ref().metadata()["provider.unknown"],
        "original-field-metadata"
    );
}

#[test]
fn variant_source_rewrite_retains_original_selected_pool_and_full_field_loans() {
    let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
    let (scan, column) = scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
    let path = physical_text("$.a", "UNUSED_PATH_SOURCE_SENTINEL");
    let target = physical_text("BIGINT", "UNUSED_TYPE_SOURCE_SENTINEL");
    let expression = call(&column, &path, &target);
    let ExprKind::FunctionCall {
        binding: original_binding,
        ..
    } = &expression.kind
    else {
        unreachable!()
    };
    let original_binding = original_binding.clone();
    let (plan, changed) = rewrite(
        project_calls(scan, vec![expression], &factory),
        Rc::clone(&factory),
    );
    assert!(changed);
    let descriptor = &scan_from_plan(&plan).variant_columns[0];
    assert_eq!(descriptor.source_column_id(), column.column_id);
    assert_eq!(descriptor.canonical_path(), "$.a");
    assert_eq!(descriptor.requested_type_literal(), "BIGINT");
    assert_eq!(descriptor.requested_type(), &DataType::Int64);
    assert!(descriptor.strict());
    let source = descriptor.source();
    assert_same_source(captured_value(source.captured(), 1), &path);
    assert_same_source(captured_value(source.captured(), 2), &target);
    assert!(std::ptr::eq(
        source.captured().binding().resolved(),
        original_binding.resolved()
    ));
    assert!(std::ptr::eq(
        descriptor.binding().resolved(),
        original_binding.resolved()
    ));
    assert_eq!(
        source.captured().constant_policy(),
        crate::constant::test_constant_policy()
    );
    let clone = descriptor.clone();
    assert!(Arc::ptr_eq(clone.source(), descriptor.source()));
}

#[test]
fn variant_source_original_noncanonical_path_is_separate_from_emission_values() {
    let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
    let (scan, column) = scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
    let path = physical_text("$['a']['not plain']", "UNUSED_NONCANONICAL");
    let target = physical_text("BIGINT", "UNUSED_TARGET");
    let (plan, changed) = rewrite(
        project_calls(scan, vec![call(&column, &path, &target)], &factory),
        Rc::clone(&factory),
    );
    assert!(changed);
    let descriptor = &scan_from_plan(&plan).variant_columns[0];
    let source = descriptor.source();
    assert_eq!(
        captured_value(source.captured(), 1).try_utf8().unwrap(),
        Some("$['a']['not plain']")
    );
    assert_eq!(
        source.canonical_path().try_utf8().unwrap(),
        Some("$.a['not plain']")
    );
    assert_eq!(source.type_literal().try_utf8().unwrap(), Some("BIGINT"));
    assert_eq!(descriptor.canonical_path(), "$.a['not plain']");
    assert_eq!(descriptor.requested_type_literal(), "BIGINT");
    assert_ne!(
        source.canonical_path().pool().backing_identity(),
        path.pool().backing_identity()
    );
    assert_same_source(captured_value(source.captured(), 1), &path);
    assert_same_source(captured_value(source.captured(), 2), &target);
}

#[test]
fn variant_source_normalized_requests_deduplicate_without_replacing_first_origin() {
    let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
    let (scan, column) = scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
    let first = physical_text("$['a']", "FIRST_UNUSED_SOURCE");
    let second = physical_text("$.a", "SECOND_UNUSED_SOURCE");
    let target = physical_text("bigint", "UNUSED_TARGET");
    assert_ne!(
        first.pool().backing_identity(),
        second.pool().backing_identity()
    );
    let expressions = vec![
        call(&column, &first, &target),
        call(&column, &second, &target),
    ];
    let (plan, changed) = rewrite(
        project_calls(scan, expressions, &factory),
        Rc::clone(&factory),
    );
    assert!(changed);
    let scan = scan_from_plan(&plan);
    assert_eq!(scan.variant_columns.len(), 1);
    let descriptor = &scan.variant_columns[0];
    assert_eq!(descriptor.canonical_path(), "$.a");
    assert_same_source(captured_value(descriptor.source().captured(), 1), &first);
    assert_eq!(
        captured_value(descriptor.source().captured(), 1)
            .try_utf8()
            .unwrap(),
        Some("$['a']")
    );
    let LogicalPlanKind::Project(project) = &plan.kind else {
        unreachable!()
    };
    assert_eq!(
        column_ref_id(&project.items[0].expr),
        descriptor.synthetic_column_id()
    );
    assert_eq!(
        column_ref_id(&project.items[1].expr),
        descriptor.synthetic_column_id()
    );
}

#[test]
fn variant_source_debug_preserves_legacy_manifest_without_retained_backing() {
    let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
    let (scan, column) = scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
    let path = physical_text("$['a']", "UNUSED_DEBUG_PATH_SENTINEL");
    let target = physical_text("bigint", "UNUSED_DEBUG_TYPE_SENTINEL");
    let (plan, changed) = rewrite(
        project_calls(scan, vec![call(&column, &path, &target)], &factory),
        Rc::clone(&factory),
    );
    assert!(changed);
    let descriptor = &scan_from_plan(&plan).variant_columns[0];
    let expected = format!(
        "ScanVariantColumn {{ source_column_id: {:?}, source_column: {:?}, synthetic_column_id: {:?}, synthetic_column: {:?}, canonical_path: {:?}, requested_type: {:?}, requested_type_literal: {:?}, strict: {:?}, binding: {:?} }}",
        descriptor.source_column_id(),
        descriptor.source_column(),
        descriptor.synthetic_column_id(),
        descriptor.synthetic_column(),
        "$.a",
        DataType::Int64,
        "bigint",
        true,
        descriptor.binding(),
    );
    let actual = format!("{descriptor:?}");
    assert_eq!(actual, expected);
    for hidden in [
        "UNUSED_DEBUG_PATH_SENTINEL",
        "UNUSED_DEBUG_TYPE_SENTINEL",
        "ConstantPool",
        "DerivedVariantSource",
        "captured",
        "original-field-metadata",
    ] {
        assert!(
            !actual.contains(hidden),
            "retained source is not the legacy debug manifest: {hidden}"
        );
    }
}

#[test]
fn variant_source_nominal_json_nonmatch_and_full_type_mismatch_keep_original_boundaries() {
    let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
    let (scan, column) = scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
    let json_path = selected_text("\"$.a\"", "UNUSED_JSON", ValueLogicalType::Json);
    let target = physical_text("bigint", "UNUSED_TARGET");
    let (plan, changed) = rewrite(
        project_calls(scan, vec![call(&column, &json_path, &target)], &factory),
        Rc::clone(&factory),
    );
    assert!(!changed);
    assert!(scan_from_plan(&plan).variant_columns.is_empty());
    assert_eq!(json_path.value_type().logical_type(), ValueLogicalType::Json);

    // The original interner rejects an inconsistent CV before the rewrite can
    // inspect it. Do not manufacture an admitted malformed arena to bypass it.
    let mut arena = ScalarArena::new();
    let before = arena.node_count();
    let error = arena
        .intern_observed(
            crate::optimizer::scalar::ScalarNode::Constant(json_path),
            FunctionValueType::new(DataType::Utf8, false),
            &Control,
        )
        .unwrap_err();
    assert!(
        matches!(error, crate::compiler::SqlCompileError::Compilation(ref text)
        if text.contains("interner constant source differs from its frozen value type"))
    );
    assert_eq!(arena.node_count(), before);
}
