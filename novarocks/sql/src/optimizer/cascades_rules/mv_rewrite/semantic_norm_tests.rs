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
use arrow::array::{Array, Int64Array, ListArray};
use arrow::datatypes::{DataType, Field, Int64Type};
use novarocks_functions::ConstantPool;
use novarocks_type_contract::{CompileControlError, DecimalOverflowPolicy, ValueLogicalType};
use std::sync::{Arc, Mutex};

fn control() -> crate::compiler::SqlCompileControl {
    crate::compiler::SqlCompileControl::unbounded()
}
fn selected(name: &str, metadata: &str, values: Vec<Option<i64>>, ordinal: u32) -> ConstantValue {
    let nullable = values.iter().any(Option::is_none);
    let ty = FunctionValueType::new(DataType::Int64, nullable);
    ConstantPool::try_new(
        Arc::new(Field::new(name, DataType::Int64, nullable).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider".into(),
                metadata.into(),
            )]),
        )),
        ty,
        Int64Array::from(values).to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::LowerProgram,
        &control(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn constant(value: ConstantValue) -> NormExpr {
    NormExpr::Constant {
        value_type: value.value_type().clone(),
        value,
    }
}
fn col(name: &str, ty: FunctionValueType) -> NormExpr {
    NormExpr::Column {
        name: name.into(),
        value_type: ty,
    }
}
fn same(a: &NormExpr, b: &NormExpr) -> bool {
    checked(&control(), |work| equal(a, b, work.control(), work)).unwrap()
}
fn key(expr: &NormExpr) -> u64 {
    checked(&control(), |work| fingerprint(expr, work.control(), work)).unwrap()
}
fn normalized(arena: &ScalarArena, id: ScalarId, names: &HashMap<ColumnId, String>) -> NormExpr {
    normalize(arena, id, names, &control()).unwrap().unwrap()
}
fn intern(arena: &mut ScalarArena, node: ScalarNode, ty: FunctionValueType) -> ScalarId {
    arena.intern_observed(node, ty, &control()).unwrap()
}
fn scalar_constant(arena: &mut ScalarArena, value: ConstantValue) -> ScalarId {
    let ty = value.value_type().clone();
    intern(arena, ScalarNode::Constant(value), ty)
}
fn call(args: Vec<NormExpr>) -> NormExpr {
    NormExpr::Call {
        name: "ordered-test".into(),
        value_type: FunctionValueType::new(DataType::Int64, false),
        distinct: false,
        args,
        binding: None,
        decimal_overflow_policy: None,
        order_by: vec![],
        argument_order: NormArgumentOrder::Ordered,
    }
}

#[test]
fn normalization_keeps_original_selected_ordinal_backing_and_nested_field_facts() {
    let original = selected("source", "frozen", vec![Some(-99), Some(42), Some(900)], 1);
    let equivalent = selected("source", "frozen", vec![Some(42), Some(-100)], 0);
    let mut arena = ScalarArena::new();
    let id = scalar_constant(&mut arena, original.clone());
    let norm = normalized(&arena, id, &HashMap::new());
    let NormExpr::Constant { value, value_type } = &norm else {
        panic!("actual checked constant")
    };
    assert_eq!(value.ordinal(), 1);
    assert!(Arc::ptr_eq(value.pool().array(), original.pool().array()));
    assert_eq!(value_type, original.value_type());
    assert_eq!(value.field().metadata().get("provider").unwrap(), "frozen");
    assert!(same(&norm, &constant(equivalent)));
    assert!(!same(
        &norm,
        &constant(selected("source", "frozen", vec![Some(43)], 0))
    ));

    let null = selected("source", "frozen", vec![Some(7), None], 1);
    let null_id = scalar_constant(&mut arena, null.clone());
    let null_norm = normalized(&arena, null_id, &HashMap::new());
    assert!(same(
        &null_norm,
        &constant(selected("source", "frozen", vec![None, Some(900)], 0))
    ));
    assert!(
        null.is_null_observed(CompilePhase::LowerProgram, &control())
            .unwrap()
    );
    assert!(null_norm.value_type().nullable());

    let original_list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(7)]),
        Some(vec![Some(42), None]),
    ]);
    let field = Arc::new(Field::new("elements", DataType::Int64, true).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "provider-child".into(),
            "retained".into(),
        )]),
    ));
    let list = ListArray::new(
        field.clone(),
        original_list.offsets().clone(),
        original_list.values().clone(),
        original_list.nulls().cloned(),
    );
    let ty = FunctionValueType::new(list.data_type().clone(), false);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("nested", list.data_type().clone(), false)),
        ty.clone(),
        list.to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::LowerProgram,
        &control(),
    )
    .unwrap();
    let source = pool.value(1).unwrap();
    let id = scalar_constant(&mut arena, source.clone());
    let nested = normalized(&arena, id, &HashMap::new());
    assert_eq!(nested.value_type(), &ty);
    let NormExpr::Constant { value, .. } = nested else {
        panic!("actual nested constant")
    };
    assert_eq!(value.ordinal(), 1);
    assert!(Arc::ptr_eq(value.pool().array(), source.pool().array()));
    assert!(
        matches!(value.value_type().data_type(),DataType::List(f) if f.name()=="item" && f.metadata().is_empty())
    );
    assert!(
        matches!(value.field().data_type(),DataType::List(f) if std::sync::Arc::ptr_eq(f, &field))
    );
    assert_eq!(field.metadata()["provider-child"], "retained");
}

#[test]
fn coarse_constant_collisions_require_exact_field_identity_and_last_exact_overwrite() {
    let first = constant(selected("left", "one", vec![Some(99), Some(42)], 1));
    let name_changed = constant(selected("right", "one", vec![Some(42)], 0));
    let metadata_changed = constant(selected("left", "two", vec![Some(42)], 0));
    let equivalent = constant(selected("left", "one", vec![Some(42), Some(-9)], 0));
    assert_eq!(key(&first), key(&name_changed));
    assert_eq!(key(&first), key(&metadata_changed));
    assert!(same(&first, &name_changed));
    assert!(same(&first, &metadata_changed));
    let value_changed = constant(selected("left", "one", vec![Some(43)], 0));
    assert_ne!(
        key(&first),
        key(&value_changed),
        "the original constant fingerprint includes its selected value"
    );
    assert!(!same(&first, &value_changed));
    let mut index = NormIndex::new();
    index.insert(first.clone(), 10, &control()).unwrap();
    index.insert(name_changed.clone(), 20, &control()).unwrap();
    index
        .insert(metadata_changed.clone(), 30, &control())
        .unwrap();
    index.insert(value_changed.clone(), 50, &control()).unwrap();
    index.insert(equivalent, 40, &control()).unwrap();
    assert_eq!(index.buckets.len(), 2);
    assert!(index.buckets.values().all(|bucket| bucket.len() == 1));
    assert_eq!(index.get(&first, &control()).unwrap(), Some(&40));
    assert_eq!(index.get(&name_changed, &control()).unwrap(), Some(&40));
    assert_eq!(index.get(&metadata_changed, &control()).unwrap(), Some(&40));
    assert_eq!(index.get(&value_changed, &control()).unwrap(), Some(&50));
    let ty = FunctionValueType::new(DataType::Int64, false);
    assert!(!same(&col("a", ty.clone()), &col("b", ty)));
}

#[test]
fn normalized_multisets_preserve_duplicates_in_head_and_case_arm_order() {
    let mut arena = ScalarArena::new();
    let one = scalar_constant(&mut arena, selected("source", "same", vec![Some(1)], 0));
    let two = scalar_constant(&mut arena, selected("source", "same", vec![Some(2)], 0));
    let integer = FunctionValueType::new(DataType::Int64, false);
    let boolean = FunctionValueType::new(DataType::Boolean, false);
    let add = |arena: &mut ScalarArena, left, right| {
        intern(
            arena,
            ScalarNode::BinaryOp {
                left,
                right,
                op: BinOp::Add,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            integer.clone(),
        )
    };
    let first = add(&mut arena, one, two);
    let flipped = add(&mut arena, two, one);
    assert!(same(
        &normalized(&arena, first, &HashMap::new()),
        &normalized(&arena, flipped, &HashMap::new())
    ));
    let ins = |arena: &mut ScalarArena, child, list| {
        intern(
            arena,
            ScalarNode::InList {
                child,
                list,
                negated: false,
            },
            boolean.clone(),
        )
    };
    let a = ins(&mut arena, one, vec![one, one, two]);
    let b = ins(&mut arena, one, vec![two, one, one]);
    let changed = ins(&mut arena, one, vec![two, two, one]);
    assert!(same(
        &normalized(&arena, a, &HashMap::new()),
        &normalized(&arena, b, &HashMap::new())
    ));
    assert!(!same(
        &normalized(&arena, a, &HashMap::new()),
        &normalized(&arena, changed, &HashMap::new())
    ));
    let a = ins(&mut arena, one, vec![two]);
    let b = ins(&mut arena, two, vec![one]);
    assert!(!same(
        &normalized(&arena, a, &HashMap::new()),
        &normalized(&arena, b, &HashMap::new())
    ));
    let t = crate::constant::admit_syntax_constant(
        &crate::common::LiteralValue::Bool(true),
        &boolean,
        crate::constant::test_constant_policy(),
        &control(),
    )
    .unwrap();
    let f = crate::constant::admit_syntax_constant(
        &crate::common::LiteralValue::Bool(false),
        &boolean,
        crate::constant::test_constant_policy(),
        &control(),
    )
    .unwrap();
    let yes = scalar_constant(&mut arena, t);
    let no = scalar_constant(&mut arena, f);
    let case = |arena: &mut ScalarArena, when_then, else_expr| {
        intern(
            arena,
            ScalarNode::Case {
                operand: None,
                when_then,
                else_expr,
            },
            FunctionValueType::new(DataType::Int64, true),
        )
    };
    let first = case(&mut arena, vec![(yes, one), (no, two)], None);
    let reordered = case(&mut arena, vec![(no, two), (yes, one)], None);
    let explicit_else = case(&mut arena, vec![(yes, one), (no, two)], Some(two));
    assert!(!same(
        &normalized(&arena, first, &HashMap::new()),
        &normalized(&arena, reordered, &HashMap::new())
    ));
    assert!(!same(
        &normalized(&arena, first, &HashMap::new()),
        &normalized(&arena, explicit_else, &HashMap::new())
    ));
}

#[test]
fn normalized_types_policy_and_authored_ordering_are_complete_identity() {
    let base = call(vec![constant(selected(
        "source",
        "same",
        vec![Some(42)],
        0,
    ))]);
    let mut nullable = base.clone();
    if let NormExpr::Call { value_type, .. } = &mut nullable {
        *value_type = value_type.with_nullable(true).unwrap();
    }
    assert!(!same(&base, &nullable));
    let mut policy = base.clone();
    if let NormExpr::Call {
        decimal_overflow_policy,
        ..
    } = &mut policy
    {
        *decimal_overflow_policy = Some(DecimalOverflowPolicy::ReportError);
    }
    assert!(!same(&base, &policy));
    let mut ordered = base.clone();
    if let NormExpr::Call { order_by, .. } = &mut ordered {
        order_by.push(NormSortKey {
            expr: base.clone(),
            asc: true,
            nulls_first: false,
        });
    }
    let mut changed = ordered.clone();
    if let NormExpr::Call { order_by, .. } = &mut changed {
        order_by[0].nulls_first = true;
    }
    assert!(!same(&ordered, &changed));
    let nested = |value: &str| {
        FunctionValueType::new(
            DataType::List(Arc::new(
                Field::new("item", DataType::Int64, value == "one").with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider".into(),
                        value.into(),
                    )]),
                ),
            )),
            true,
        )
    };
    assert!(!same(&col("a", nested("one")), &col("a", nested("two"))));
    let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    assert!(!same(&col("a", physical), &col("a", uuid)));
}

struct Recording {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<usize>,
    cause: CompileControlError,
}
impl PureCompileControl for Recording {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if self.stop == Some(trace.len() - 1) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
fn recording(stop: Option<usize>, cause: CompileControlError) -> Recording {
    Recording {
        trace: Default::default(),
        stop,
        cause,
    }
}
fn every_callback<T>(run: impl Fn(&Recording) -> Result<T, SqlCompileError>) {
    let good = recording(None, CompileControlError::Cancelled);
    let _ = run(&good).unwrap();
    let trace = good.trace.into_inner().unwrap();
    assert_eq!(trace.first(), Some(&(CompilePhase::LowerProgram, 0)));
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::LowerProgram && *units <= 256)
    );
    for stop in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refused = recording(Some(stop), cause.clone());
            assert!(matches!(run(&refused),Err(error) if error==SqlCompileError::from(cause)));
            assert_eq!(*refused.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn normalize_index_and_contains_refusals_preserve_original_prefix_and_no_publication() {
    let ty = FunctionValueType::new(DataType::Int64, false);
    let wide = call(
        (0..320)
            .map(|i| col(&format!("column_{i}"), ty.clone()))
            .collect(),
    );
    every_callback(|c| {
        let mut index = NormIndex::new();
        let result = index.insert(wide.clone(), 20, c);
        if result.is_err() {
            assert!(index.buckets.is_empty());
        }
        result
    });
    let mut old = NormIndex::new();
    old.insert(wide.clone(), 10, &control()).unwrap();
    every_callback(|c| {
        let mut index = NormIndex::new();
        index.insert(wide.clone(), 10, &control()).unwrap();
        let result = index.insert(wide.clone(), 20, c);
        if result.is_err() {
            assert_eq!(index.get(&wide, &control()).unwrap(), Some(&10));
        }
        result
    });
    every_callback(|c| old.get(&wide, c).map(|value| value.copied()));
    every_callback(|c| norm_contains(std::slice::from_ref(&wide), &wide, c));
    let miss = call(
        (0..320)
            .map(|i| col(&format!("missing_{i}"), ty.clone()))
            .collect(),
    );
    every_callback(|c| old.get(&miss, c).map(|value| value.copied()));
    every_callback(|c| norm_contains(std::slice::from_ref(&wide), &miss, c));
    let good = recording(None, CompileControlError::Cancelled);
    assert!(norm_contains(std::slice::from_ref(&wide), &wide, &good).unwrap());
    assert!(
        good.trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    let mut arena = ScalarArena::new();
    let constant = scalar_constant(
        &mut arena,
        selected("source", "same", vec![Some(99), Some(42)], 1),
    );
    every_callback(|c| normalize(&arena, constant, &HashMap::new(), c));
    let absent = intern(
        &mut arena,
        ScalarNode::ColumnRef(ColumnId::new_for_test(77)),
        ty,
    );
    every_callback(|c| normalize(&arena, absent, &HashMap::new(), c));
    assert!(
        normalize(&arena, absent, &HashMap::new(), &control())
            .unwrap()
            .is_none()
    );
}

#[test]
fn mv_dimension_mapping_requires_exact_source_and_output_types_before_publication() {
    use super::super::column_mapping::MvColumnMap;
    use crate::common::OutputColumn;
    let source = constant(selected("source", "same", vec![Some(99), Some(42)], 1));
    let output = |value_type| OutputColumn {
        column_id: ColumnId::new_for_test(101),
        name: "mv_output".into(),
        value_type,
        is_internal: false,
    };
    assert!(
        MvColumnMap::try_new(
            vec![(source.clone(), output(source.value_type().clone()))],
            &control(),
        )
        .unwrap()
        .is_some()
    );
    let nested = |metadata: &str| {
        FunctionValueType::new(
            DataType::List(Arc::new(
                Field::new("item", DataType::Int64, metadata == "source").with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider".into(),
                        metadata.into(),
                    )]),
                ),
            )),
            true,
        )
    };
    let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    let cases = [
        (
            source.clone(),
            FunctionValueType::new(DataType::Int64, true),
        ),
        (source, FunctionValueType::new(DataType::Int32, false)),
        (col("a", physical), uuid),
        (col("a", nested("source")), nested("changed")),
    ];
    for (source, target) in cases {
        let single = vec![(source.clone(), output(target.clone()))];
        let recorded = recording(None, CompileControlError::Cancelled);
        assert!(
            MvColumnMap::try_new(single.clone(), &recorded)
                .unwrap()
                .is_none()
        );
        let trace = recorded.trace.into_inner().unwrap();
        assert!(
            trace.last().unwrap().1 > 0,
            "ordinary mismatch must observe its completed pending work"
        );
        every_callback(|c| MvColumnMap::try_new(single.clone(), c).map(|map| map.is_some()));
        // A prior valid dimension must also stay unpublished when a later
        // source/output association fails; the constructor's result is None.
        let dims = vec![
            (source.clone(), output(source.value_type().clone())),
            (source, output(target)),
        ];
        assert!(
            MvColumnMap::try_new(dims.clone(), &control())
                .unwrap()
                .is_none()
        );
        every_callback(|c| MvColumnMap::try_new(dims.clone(), c).map(|map| map.is_some()));
    }
    let empty = recording(None, CompileControlError::Cancelled);
    assert!(MvColumnMap::try_new(Vec::new(), &empty).unwrap().is_some());
    assert_eq!(
        *empty.trace.lock().unwrap(),
        [
            (CompilePhase::LowerProgram, 0),
            (CompilePhase::LowerProgram, 0)
        ]
    );
    every_callback(|c| MvColumnMap::try_new(Vec::new(), c).map(|map| map.is_some()));
    let complete = vec![(
        col("a", FunctionValueType::new(DataType::Int64, true)),
        output(FunctionValueType::new(DataType::Int64, true)),
    )];
    every_callback(|c| MvColumnMap::try_new(complete.clone(), c).map(|map| map.is_some()));
}
