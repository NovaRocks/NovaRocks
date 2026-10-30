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
use crate::optimizer::operator::{FilterOp, LogicalJoinOp, ScanOp, ValuesOp};
use crate::optimizer::stats_input::{
    BaseColumnStatistics, BaseTableStatistics, QueryStatsSnapshot, StatValue, StatsMissingReason,
    StatsRef,
};
use arrow::array::{
    ArrayRef, Decimal128Array, FixedSizeBinaryArray, Float32Array, Int64Array, StringArray,
    StructArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field};
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
    let field = Field::new("selected.source", array.data_type().clone(), true)
        .with_metadata(HashMap::from([("source".into(), "preserved".into())]));
    ConstantPool::try_new(
        Arc::new(field),
        ty,
        array.to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap()
}

fn selected_integer(ordinal: u32) -> ConstantValue {
    pool(
        Arc::new(Int64Array::from(vec![Some(100), Some(7), None, Some(-2)])),
        ValueLogicalType::Physical,
    )
    .value(ordinal)
    .unwrap()
}

fn constant(memo: &mut Memo, value: ConstantValue) -> ScalarId {
    memo.scalars
        .intern_observed(
            ScalarNode::Constant(value.clone()),
            value.value_type().clone(),
            &Control::default(),
        )
        .unwrap()
}

fn column(id: u32, ty: FunctionValueType) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId::new_for_test(id),
        name: "k".into(),
        value_type: ty,
        is_internal: false,
    }
}

fn column_scalar(memo: &mut Memo, output: &OutputColumn) -> ScalarId {
    memo.scalars
        .intern_observed(
            ScalarNode::ColumnRef(output.column_id),
            output.value_type.clone(),
            &Control::default(),
        )
        .unwrap()
}

fn binary(memo: &mut Memo, op: BinOp, left: ScalarId, right: ScalarId) -> ScalarId {
    memo.scalars
        .intern_observed(
            ScalarNode::BinaryOp {
                op,
                left,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            FunctionValueType::new(DataType::Boolean, true),
            &Control::default(),
        )
        .unwrap()
}

fn range_stat(ndv: Option<f64>) -> ColumnStatistic {
    let stat = ColumnStatistic {
        min_value: 0.0,
        max_value: 10.0,
        nulls_fraction: 0.2,
        confidence: Confidence::Exact,
        ..ColumnStatistic::unknown()
    };
    ndv.map_or(stat.clone(), |ndv| {
        stat.with_known_ndv(ndv, Confidence::Exact, StatsSource::TestFixture)
    })
}

fn child(memo: &mut Memo, output: OutputColumn, rows: f64, ndv: Option<f64>) -> GroupId {
    let expr = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![output.clone()],
        }),
        children: vec![],
    };
    let group = memo.new_group(expr);
    let mut props = LogicalProperties::new(vec![output.clone()], rows);
    props.row_count_confidence = Confidence::Exact;
    props
        .column_statistics
        .insert(output.column_id, range_stat(ndv));
    memo.groups[group].logical_props = Some(props);
    group
}

fn input() -> OptimizerStatsInput {
    OptimizerStatsInput::from_query_stats(&QueryStatsSnapshot::empty())
}

fn derive(
    expr: &MExpr,
    memo: &Memo,
    input: &OptimizerStatsInput,
    control: &Control,
) -> Result<Statistics, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = derive_statistics(expr, memo, input, &mut work, control)?;
    work.finish()?;
    Ok(result)
}

fn filter(memo: &mut Memo, predicate: ScalarId, child: GroupId, physical: bool) -> MExpr {
    MExpr {
        id: memo.next_expr_id(),
        op: if physical {
            Operator::PhysicalFilter(FilterOp { predicate })
        } else {
            Operator::LogicalFilter(FilterOp { predicate })
        },
        children: vec![child],
    }
}

#[test]
fn selected_cv_filter_keeps_discrete_range_ndv_and_null_fallback_formulas() {
    for physical in [false, true] {
        for (op, ordinal, ndv, expected) in [
            (BinOp::Eq, 1, None, 1000.0 / 11.0),
            (BinOp::Eq, 0, None, 1.0),
            (BinOp::Eq, 1, Some(5.0), 200.0),
            (BinOp::Lt, 1, None, 700.0),
            (BinOp::Le, 1, None, 800.0),
            (BinOp::Gt, 1, None, 300.0),
            (BinOp::Ge, 1, None, 400.0),
            (BinOp::Eq, 2, Some(5.0), 250.0),
            (BinOp::Lt, 2, None, 500.0),
        ] {
            let mut memo = Memo::new();
            let output = column(1, FunctionValueType::new(DataType::Int64, true));
            let lhs = column_scalar(&mut memo, &output);
            let value = selected_integer(ordinal);
            let identity = value.pool().backing_identity();
            let rhs = constant(&mut memo, value);
            let predicate = binary(&mut memo, op, lhs, rhs);
            let source = child(&mut memo, output, 1000.0, ndv);
            let expr = filter(&mut memo, predicate, source, physical);
            let actual = derive(&expr, &memo, &input(), &Control::default()).unwrap();
            assert!((actual.output_row_count - expected).abs() < 1e-10, "{op:?}");
            let ScalarNode::Constant(retained) = memo.scalars.node(rhs) else {
                panic!("constant source was rewritten")
            };
            assert_eq!(retained.ordinal(), ordinal);
            assert_eq!(retained.pool().backing_identity(), identity);
            assert_eq!(retained.field().metadata()["source"], "preserved");
        }
    }
}

#[test]
fn selected_cv_scan_and_join_consume_bound_source_statistics() {
    use crate::planner::table::{SqlScanKind, TableDef};
    use novarocks_types::schema::ColumnDef;

    let mut memo = Memo::new();
    let output = column(1, FunctionValueType::new(DataType::Int64, true));
    let lhs = column_scalar(&mut memo, &output);
    let rhs = constant(&mut memo, selected_integer(1));
    let predicate = binary(&mut memo, BinOp::Lt, lhs, rhs);
    let stats_ref = StatsRef::new(42);
    let known = |value| StatValue::known(value, Confidence::Exact, StatsSource::TestFixture);
    let mut snapshot = QueryStatsSnapshot::empty();
    snapshot.insert(
        stats_ref,
        "db.source",
        BaseTableStatistics {
            row_count: StatValue::known(1000, Confidence::Exact, StatsSource::TestFixture),
            columns: HashMap::from([(
                "k".into(),
                BaseColumnStatistics {
                    nulls_fraction: known(0.2),
                    average_row_size: known(8.0),
                    min_value: known(0.0),
                    max_value: known(10.0),
                    ndv: StatValue::missing(StatsMissingReason::ColumnNotReported("k".into())),
                },
            )]),
            source: StatsSource::TestFixture,
        },
    );
    let stats_input = OptimizerStatsInput::from_query_stats(&snapshot);
    let scan = ScanOp {
        database: "db".into(),
        table: TableDef {
            name: "source".into(),
            columns: vec![ColumnDef {
                name: "k".into(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::compiler::mv_rewrite::test_scan_source(SqlScanKind::ConnectorRead),
        },
        alias: None,
        stats_ref: Some(stats_ref),
        columns: vec![output.clone()],
        predicates: vec![predicate],
        required_columns: None,
        variant_columns: vec![],
        mv_rewritten_from: None,
    };
    for op in [
        Operator::LogicalScan(scan.clone()),
        Operator::PhysicalScan(scan),
    ] {
        let expr = MExpr {
            id: memo.next_expr_id(),
            op,
            children: vec![],
        };
        assert_eq!(
            derive(&expr, &memo, &stats_input, &Control::default())
                .unwrap()
                .output_row_count,
            700.0
        );
    }
    let left = child(&mut memo, output, 1000.0, None);
    let right = child(
        &mut memo,
        column(2, FunctionValueType::new(DataType::Int64, true)),
        10.0,
        None,
    );
    let expr = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalJoin(LogicalJoinOp {
            naa_author: None,
            join_type: JoinKind::Inner,
            condition: Some(predicate),
        }),
        children: vec![left, right],
    };
    assert_eq!(
        derive(&expr, &memo, &input(), &Control::default())
            .unwrap()
            .output_row_count,
        7000.0
    );
}

#[test]
fn selected_cv_numeric_width_scale_and_nominal_domain_share_values_reader() {
    let cases: Vec<(ArrayRef, ValueLogicalType, Option<f64>)> = vec![
        (
            Arc::new(Float32Array::from(vec![100.0, 0.1])),
            ValueLogicalType::Physical,
            Some(0.1f32 as f64),
        ),
        (
            Arc::new(UInt64Array::from(vec![0, u64::MAX])),
            ValueLogicalType::Physical,
            Some(u64::MAX as f64),
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![0, 1])
                    .with_precision_and_scale(18, -2)
                    .unwrap(),
            ),
            ValueLogicalType::Physical,
            Some(100.0),
        ),
        (
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    [0i128.to_be_bytes(), (-9i128).to_be_bytes()].into_iter(),
                )
                .unwrap(),
            ),
            ValueLogicalType::LargeInt,
            Some(-9.0),
        ),
        (
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    [0i128.to_be_bytes(), (-9i128).to_be_bytes()].into_iter(),
                )
                .unwrap(),
            ),
            ValueLogicalType::Uuid,
            None,
        ),
        (
            Arc::new(StringArray::from(vec!["100", "7"])),
            ValueLogicalType::Json,
            None,
        ),
    ];
    for (array, logical, expected) in cases {
        let mut memo = Memo::new();
        let value = pool(array, logical).value(1).unwrap();
        let id = constant(&mut memo, value.clone());
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let actual = scalar_literal_f64(&memo.scalars, id, &mut work).unwrap();
        work.finish().unwrap();
        assert_eq!(actual.map(f64::to_bits), expected.map(f64::to_bits));

        // Unsupported numeric-statistics sources stay unknown rather than
        // becoming implicit casts; a typed predicate uses that same source.
        if expected.is_none() {
            let output = column(1, value.value_type().clone());
            let lhs = column_scalar(&mut memo, &output);
            let predicate = binary(&mut memo, BinOp::Eq, lhs, id);
            let source = child(&mut memo, output, 1000.0, Some(5.0));
            let expr = filter(&mut memo, predicate, source, false);
            assert_eq!(
                derive(&expr, &memo, &input(), &Control::default())
                    .unwrap()
                    .output_row_count,
                250.0
            );
        }
    }
}

#[test]
fn selected_cv_full_source_type_mismatch_is_typed_and_never_admitted() {
    let integer = selected_integer(1);
    let nested_field = Arc::new(
        Field::new("nested", DataType::Int64, true)
            .with_metadata(HashMap::from([("source".into(), "a".into())])),
    );
    let nested = pool(
        Arc::new(StructArray::from(vec![(
            nested_field,
            Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef,
        )])),
        ValueLogicalType::Physical,
    )
    .value(0)
    .unwrap();
    let uuid = pool(
        Arc::new(FixedSizeBinaryArray::try_from_iter([7i128.to_be_bytes()].into_iter()).unwrap()),
        ValueLogicalType::Uuid,
    )
    .value(0)
    .unwrap();
    let cases = [
        (
            integer.clone(),
            FunctionValueType::new(DataType::Int64, false),
        ),
        (
            integer.clone(),
            FunctionValueType::new(DataType::UInt64, true),
        ),
        (
            uuid,
            FunctionValueType::new(DataType::FixedSizeBinary(16), true),
        ),
        (
            nested,
            FunctionValueType::new(
                DataType::Struct(
                    vec![Arc::new(
                        Field::new("different_nested_name", DataType::Int64, true)
                            .with_metadata(HashMap::from([("source".into(), "b".into())])),
                    )]
                    .into(),
                ),
                true,
            ),
        ),
    ];
    let mut memo = Memo::new();
    for (value, ty) in cases {
        let before = memo.scalars.node_count();
        assert!(matches!(
            memo.scalars.intern_observed(
                ScalarNode::Constant(value.clone()),
                ty.clone(),
                &Control::default()
            ),
            Err(SqlCompileError::Compilation(_))
        ));
        assert_eq!(memo.scalars.node_count(), before);

        // Safe arenas cannot publish that malformed Constant. Exercise the
        // shared reader's independent full-source gate with an authored slot.
        let id = column_scalar(&mut memo, &column(1, ty));
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        assert!(matches!(
            values_constant_f64_observed(&memo.scalars, id, &value, &control, &mut work),
            Err(SqlCompileError::InvalidRequest(_))
        ));
        work.finish().unwrap();
    }
    assert_eq!(integer.try_i64().unwrap(), Some(7));
}

fn publication_fixture() -> (Memo, GroupId) {
    let mut memo = Memo::new();
    let output = column(1, FunctionValueType::new(DataType::Int64, true));
    let lhs = column_scalar(&mut memo, &output);
    let rhs = constant(&mut memo, selected_integer(1));
    let eq = binary(&mut memo, BinOp::Eq, lhs, rhs);
    let mut predicate = eq;
    for _ in 1..320 {
        predicate = binary(&mut memo, BinOp::And, predicate, eq);
    }
    let source = child(&mut memo, output, 1000.0, Some(5.0));
    let expr = filter(&mut memo, predicate, source, false);
    let root = memo.new_group(expr);
    memo.groups[root].logical_props = Some(LogicalProperties::new(vec![], 999.0));
    (memo, root)
}

#[test]
fn selected_cv_every_original_callback_refusal_keeps_group_publication_unchanged() {
    let (fixture, root) = publication_fixture();
    let mut baseline_memo = fixture.clone();
    let baseline = Control::default();
    derive_group_statistics_for(&mut baseline_memo, root, &input(), &baseline).unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert!(
        trace.contains(&256),
        "no real bounded scalar traversal occurred"
    );
    assert!(trace.iter().any(|units| *units > 0 && *units < 256));
    assert_ne!(
        baseline_memo.groups[root]
            .logical_props
            .as_ref()
            .unwrap()
            .row_count,
        999.0
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
            let mut memo = fixture.clone();
            let before = memo.scalars.node_count();
            let control = Control {
                refuse: Some((at, cause)),
                ..Default::default()
            };
            assert_eq!(
                derive_group_statistics_for(&mut memo, root, &input(), &control).unwrap_err(),
                expected
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            let props = memo.groups[root].logical_props.as_ref().unwrap();
            assert_eq!(props.row_count, 999.0);
            assert!(props.column_statistics.is_empty());
            assert_eq!(memo.scalars.node_count(), before);
        }
    }
}

#[test]
fn selected_cv_ordinary_source_refusal_observes_tail_and_preserves_typed_primary() {
    let value = selected_integer(1);
    let mut memo = Memo::new();
    let id = column_scalar(
        &mut memo,
        &column(1, FunctionValueType::new(DataType::Int64, false)),
    );
    let run = |control: &Control| -> Result<(), SqlCompileError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = values_constant_f64_observed(&memo.scalars, id, &value, control, &mut work);
        if matches!(
            &result,
            Err(SqlCompileError::Cancelled
                | SqlCompileError::DeadlineExceeded
                | SqlCompileError::ResourceExhausted)
        ) {
            return result.map(|_| ());
        }
        work.finish()?;
        result.map(|_| ())
    };
    let baseline = Control::default();
    assert!(matches!(
        run(&baseline),
        Err(SqlCompileError::InvalidRequest(_))
    ));
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.iter().any(|units| *units > 0));
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
            let control = Control {
                refuse: Some((at, cause)),
                ..Default::default()
            };
            assert_eq!(run(&control).unwrap_err(), expected);
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
