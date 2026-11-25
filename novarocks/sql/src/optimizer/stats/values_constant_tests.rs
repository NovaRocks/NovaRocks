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
use crate::optimizer::memo::LogicalProperties;
use crate::optimizer::operator::ValuesOp;
use arrow::array::{
    ArrayRef, Decimal128Array, FixedSizeBinaryArray, Float32Array, Int64Array, LargeBinaryArray,
    UInt64Array,
};
use novarocks_functions::{ConstantPool, ConstantValue};
use novarocks_type_contract::{CompileControlError, FunctionValueType, ValueLogicalType};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        match self.refuse.clone() {
            Some((at, cause)) if trace.len() == at + 1 => Err(cause),
            _ => Ok(()),
        }
    }
}

fn pool(array: ArrayRef, logical: ValueLogicalType) -> ConstantPool {
    let ty =
        FunctionValueType::try_with_logical_type(array.data_type().clone(), true, logical).unwrap();
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("values.source").unwrap()),
        ty,
        array.to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap()
}

fn values(memo: &mut Memo, values: &[ConstantValue], physical: bool) -> MExpr {
    let ty = values[0].value_type().clone();
    let rows = values
        .iter()
        .map(|value| {
            vec![
                memo.scalars
                    .intern_observed(
                        ScalarNode::Constant(value.clone()),
                        value.value_type().clone(),
                        &Control::default(),
                    )
                    .unwrap(),
            ]
        })
        .collect();
    let values = ValuesOp {
        rows,
        columns: vec![OutputColumn {
            column_id: ColumnId::new_for_test(701),
            name: "result".into(),
            value_type: ty,
            is_internal: false,
        }],
    };
    MExpr {
        id: memo.next_expr_id(),
        op: if physical {
            Operator::PhysicalValues(values)
        } else {
            Operator::LogicalValues(values)
        },
        children: vec![],
    }
}

fn fixture(physical: bool, other_pool: bool) -> (Memo, MExpr) {
    let (source, ordinals): (ArrayRef, [u32; 4]) = if other_pool {
        (
            Arc::new(Int64Array::from(vec![None, Some(-2), Some(7), Some(100)])),
            [2, 0, 1, 2],
        )
    } else {
        (
            Arc::new(Int64Array::from(vec![Some(100), Some(7), None, Some(-2)])),
            [1, 2, 3, 1],
        )
    };
    let pool = pool(source, ValueLogicalType::Physical);
    let selected = ordinals.map(|ordinal| pool.value(ordinal).unwrap());
    let mut memo = Memo::new();
    let expr = values(&mut memo, &selected, physical);
    (memo, expr)
}

fn derive(
    expr: &MExpr,
    memo: &Memo,
    control: &dyn PureCompileControl,
) -> Result<Statistics, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = derive_statistics(
        expr,
        memo,
        &OptimizerStatsInput::from_test_table_statistics(&HashMap::new()),
        &mut work,
        control,
    )?;
    work.finish()?;
    Ok(result)
}

#[test]
fn values_constants_logical_and_physical_use_selected_pool_ordinals_and_nulls() {
    for physical in [false, true] {
        for other_pool in [false, true] {
            let (memo, expr) = fixture(physical, other_pool);
            let stats = derive(&expr, &memo, &Control::default()).unwrap();
            assert_eq!(stats.output_row_count, 4.0);
            let column = &stats.column_statistics[&ColumnId::new_for_test(701)];
            assert_eq!(column.confidence, Confidence::Exact);
            assert_eq!(column.min_value, -2.0);
            assert_eq!(column.max_value, 7.0);
            assert_eq!(column.nulls_fraction, 0.25);
            assert_eq!(column.trusted_ndv_value(), Some(2.0));
            assert_eq!(column.ndv_source(), Some(StatsSource::Derived));
        }
        let source = pool(
            Arc::new(Int64Array::from(vec![None, None])),
            ValueLogicalType::Physical,
        );
        let mut memo = Memo::new();
        let expr = values(&mut memo, &[source.value(1).unwrap()], physical);
        let stats = derive(&expr, &memo, &Control::default()).unwrap();
        let column = &stats.column_statistics[&ColumnId::new_for_test(701)];
        assert_eq!(column.nulls_fraction, 1.0);
        assert_eq!(column.min_value, f64::NEG_INFINITY);
        assert_eq!(column.max_value, f64::INFINITY);
        // Preserve the existing VALUES NDV floor, including all-NULL columns.
        assert_eq!(column.trusted_ndv_value(), Some(1.0));
    }
}

#[test]
fn values_constants_read_actual_signedness_float_width_scale_and_largeint_domain() {
    let raw_largeint: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter([(-9i128).to_be_bytes()].into_iter()).unwrap(),
    );
    let cases: Vec<(ArrayRef, ValueLogicalType, f64)> = vec![
        (
            Arc::new(Int64Array::from(vec![i64::MIN])),
            ValueLogicalType::Physical,
            i64::MIN as f64,
        ),
        (
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            ValueLogicalType::Physical,
            u64::MAX as f64,
        ),
        (
            Arc::new(Float32Array::from(vec![0.1f32])),
            ValueLogicalType::Physical,
            0.1f32 as f64,
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![1i128])
                    .with_precision_and_scale(18, -2)
                    .unwrap(),
            ),
            ValueLogicalType::Physical,
            100.0,
        ),
        (raw_largeint.clone(), ValueLogicalType::LargeInt, -9.0),
    ];
    for (array, logical, expected) in cases {
        let source = pool(array, logical).value(0).unwrap();
        let source_type = source.value_type().clone();
        let mut memo = Memo::new();
        let expr = values(&mut memo, &[source], false);
        let stats = derive(&expr, &memo, &Control::default()).unwrap();
        let column = &stats.column_statistics[&ColumnId::new_for_test(701)];
        assert_eq!(column.min_value.to_bits(), expected.to_bits());
        assert_eq!(column.max_value.to_bits(), expected.to_bits());
        let Operator::LogicalValues(values) = &expr.op else {
            unreachable!()
        };
        assert_eq!(memo.scalars.value_type(values.rows[0][0]), &source_type);
    }
    for (array, logical) in [
        (raw_largeint.clone(), ValueLogicalType::Physical),
        (raw_largeint, ValueLogicalType::Uuid),
        (
            Arc::new(LargeBinaryArray::from(vec![b"\x01\x46\x07".as_slice()])) as ArrayRef,
            ValueLogicalType::Json,
        ),
    ] {
        let source = pool(array, logical).value(0).unwrap();
        let mut memo = Memo::new();
        let expr = values(&mut memo, &[source], true);
        assert!(
            derive(&expr, &memo, &Control::default())
                .unwrap()
                .column_statistics
                .is_empty()
        );
    }
}

#[test]
fn values_constants_every_original_control_refusal_keeps_published_group_properties() {
    for physical in [false, true] {
        let (mut baseline_memo, expr) = fixture(physical, false);
        let root = baseline_memo.new_group(expr);
        baseline_memo.groups[root].logical_props = Some(LogicalProperties::new(vec![], 999.0));
        let input = OptimizerStatsInput::from_test_table_statistics(&HashMap::new());
        let baseline = Control::default();
        derive_group_statistics_for(&mut baseline_memo, root, &input, &baseline).unwrap();
        let trace = baseline.trace.into_inner().unwrap();
        assert_eq!(
            baseline_memo.groups[root]
                .logical_props
                .as_ref()
                .unwrap()
                .row_count,
            4.0
        );
        for at in 0..trace.len() {
            for (cause, expected) in [
                (CompileControlError::Cancelled, SqlCompileError::Cancelled),
                (
                    CompileControlError::DeadlineExceeded,
                    SqlCompileError::DeadlineExceeded,
                ),
                (
                    CompileControlError::ResourceExhausted,
                    SqlCompileError::ResourceExhausted,
                ),
            ] {
                let (mut memo, expr) = fixture(physical, false);
                let root = memo.new_group(expr);
                memo.groups[root].logical_props = Some(LogicalProperties::new(vec![], 999.0));
                let control = Control {
                    refuse: Some((at, cause)),
                    ..Default::default()
                };
                assert_eq!(
                    derive_group_statistics_for(&mut memo, root, &input, &control).unwrap_err(),
                    expected
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let props = memo.groups[root].logical_props.as_ref().unwrap();
                assert_eq!(props.row_count, 999.0);
                assert!(props.column_statistics.is_empty());
            }
        }
    }
}
