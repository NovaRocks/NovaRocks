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
use crate::column_id::ColumnId;
use crate::compiler::SqlCompileError;
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use std::sync::{Arc, Mutex};

type Trace = Vec<(CompilePhase, u32)>;

#[derive(Default)]
struct Control {
    calls: Mutex<Trace>,
    fail_at: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Trace {
        self.calls.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        calls.push((phase, units));
        if let Some((fail_at, cause)) = self.fail_at.clone()
            && fail_at == at
        {
            return Err(cause);
        }
        Ok(())
    }
}

struct Fixture {
    arena: ScalarArena,
    items: Vec<ScalarSortKey>,
    union: Vec<OutputColumn>,
    branch: Vec<OutputColumn>,
}
impl Fixture {
    fn new(
        expression_type: FunctionValueType,
        union_type: FunctionValueType,
        branch_type: FunctionValueType,
    ) -> Self {
        let mut arena = ScalarArena::new();
        let expr = arena
            .intern_observed(
                ScalarNode::ColumnRef(ColumnId(73)),
                expression_type,
                &Control::default(),
            )
            .unwrap();
        Self {
            arena,
            items: vec![ScalarSortKey {
                expr,
                asc: false,
                nulls_first: true,
                display: Some(ColumnDisplay {
                    qualifier: Some("original_scope".into()),
                    column: "union_key".into(),
                }),
            }],
            union: vec![column(73, "union_key", union_type)],
            branch: vec![column(901, "branch_key", branch_type)],
        }
    }

    fn run(&mut self, control: &Control) -> Result<Option<Vec<ScalarSortKey>>, SqlCompileError> {
        remap_sort_keys_through_union(
            &mut self.arena,
            &self.items,
            &self.union,
            &self.branch,
            control,
        )
    }
}

fn column(id: u32, name: &str, value_type: FunctionValueType) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId(id),
        name: name.into(),
        value_type,
        is_internal: false,
    }
}

fn physical(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data_type, nullable)
}

fn nominal(data_type: DataType, logical: ValueLogicalType) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(data_type, true, logical).unwrap()
}

#[allow(deprecated)]
fn dictionary(id: i64, ordered: bool, nullable: bool, name: &str, metadata: &str) -> DataType {
    DataType::Struct(Fields::from(vec![Arc::new(
        Field::new_dict(
            name,
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            nullable,
            id,
            ordered,
        )
        .with_metadata(std::collections::HashMap::<String, String>::from([(
            "provider.fact".into(),
            metadata.into(),
        )])),
    )]))
}

fn assert_refused(left: FunctionValueType, right: FunctionValueType) {
    // Test both independent relations: sort-expression -> UNION output and
    // UNION output -> branch output. Neither may replace a complete domain.
    let mut branch = Fixture::new(left.clone(), left.clone(), right.clone());
    assert!(branch.run(&Control::default()).unwrap().is_none());
    let mut expression = Fixture::new(right, left.clone(), left);
    assert!(expression.run(&Control::default()).unwrap().is_none());
}

fn check_prefixes(trace: &Trace, make: impl Fn() -> Fixture) {
    assert_eq!(trace.first().map(|entry| entry.1), Some(0));
    assert!(trace.iter().all(|entry| entry.1 <= 256));
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                fail_at: Some((at, cause.clone())),
                ..Control::default()
            };
            let error = make().run(&control).unwrap_err();
            let expected = match cause {
                CompileControlError::Source(source) => SqlCompileError::CompilationSource(source),
                CompileControlError::Cancelled => SqlCompileError::Cancelled,
                CompileControlError::DeadlineExceeded => SqlCompileError::DeadlineExceeded,
                CompileControlError::ResourceExhausted => SqlCompileError::ResourceExhausted,
            };
            assert_eq!(error, expected);
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn setop_exact_remap_preserves_order_positions_full_type_and_inverse() {
    let ty = physical(dictionary(-7, true, true, "nested", "original"), true);
    let mut fixture = Fixture::new(ty.clone(), ty.clone(), ty.clone());
    let second = fixture
        .arena
        .intern_observed(
            ScalarNode::ColumnRef(ColumnId(17)),
            physical(DataType::Int64, false),
            &Control::default(),
        )
        .unwrap();
    fixture
        .union
        .push(column(17, "second", physical(DataType::Int64, false)));
    fixture.branch.push(column(
        211,
        "second_branch",
        physical(DataType::Int64, false),
    ));
    let first = fixture.items[0].clone();
    fixture.items = vec![
        ScalarSortKey {
            expr: second,
            asc: true,
            nulls_first: false,
            display: None,
        },
        first.clone(),
        first,
    ];
    let original = fixture.items.clone();
    let remapped = fixture.run(&Control::default()).unwrap().unwrap();
    assert_eq!(remapped.len(), 3);
    for (index, id) in [211, 901, 901].into_iter().enumerate() {
        assert_eq!(
            scalar_expr_to_column_id(&fixture.arena, remapped[index].expr),
            Some(ColumnId(id))
        );
        assert_eq!(remapped[index].asc, original[index].asc);
        assert_eq!(remapped[index].nulls_first, original[index].nulls_first);
        let display = remapped[index].display.as_ref().unwrap();
        assert_eq!(display.qualifier, None);
        assert_eq!(
            display.column,
            if index == 0 {
                "second_branch"
            } else {
                "branch_key"
            }
        );
    }
    assert_eq!(fixture.arena.value_type(remapped[1].expr), &ty);
    assert_eq!(fixture.items, original);
    let inverse = remap_sort_keys_through_union(
        &mut fixture.arena,
        &remapped,
        &fixture.branch,
        &fixture.union,
        &Control::default(),
    )
    .unwrap()
    .unwrap();
    for (index, id) in [17, 73, 73].into_iter().enumerate() {
        assert_eq!(
            scalar_expr_to_column_id(&fixture.arena, inverse[index].expr),
            Some(ColumnId(id))
        );
        assert_eq!(inverse[index].asc, original[index].asc);
        assert_eq!(inverse[index].nulls_first, original[index].nulls_first);
        assert_eq!(
            fixture.arena.value_type(inverse[index].expr),
            fixture.arena.value_type(original[index].expr)
        );
    }
}

#[test]
fn setop_exact_remap_rejects_same_carrier_root_domain_substitution() {
    assert_refused(
        nominal(DataType::Utf8, ValueLogicalType::Json),
        physical(DataType::Utf8, true),
    );
    assert_refused(
        nominal(DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
        physical(DataType::FixedSizeBinary(16), true),
    );
    assert_refused(
        nominal(DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
        nominal(DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
    );
}

#[test]
fn setop_exact_remap_rejects_full_nested_dictionary_and_field_drift() {
    let source = dictionary(7, false, true, "nested", "original");
    let changed_id = dictionary(901, false, true, "nested", "original");
    let changed_order = dictionary(7, true, true, "nested", "original");
    // Arrow PartialEq cannot certify these two frozen Field attributes.
    assert_eq!(source, changed_id);
    assert_eq!(source, changed_order);
    for same_value in [
        changed_id,
        changed_order,
        dictionary(7, false, true, "nested", "changed"),
    ] {
        let ty = physical(source.clone(), true);
        let mut fixture = Fixture::new(ty.clone(), ty, physical(same_value, true));
        assert!(fixture.run(&Control::default()).unwrap().is_some());
    }
    for target in [
        dictionary(7, false, false, "nested", "original"),
        dictionary(7, false, true, "different_name", "original"),
    ] {
        assert_refused(physical(source.clone(), true), physical(target, true));
    }
    assert_refused(
        physical(DataType::Int64, true),
        physical(DataType::Int64, false),
    );
    assert_refused(
        physical(DataType::Timestamp(TimeUnit::Microsecond, None), true),
        physical(
            DataType::Timestamp(TimeUnit::Microsecond, Some("".into())),
            true,
        ),
    );
}

#[test]
fn setop_exact_remap_success_and_ordinary_decline_preserve_every_control_prefix() {
    let good = || {
        let ty = physical(dictionary(7, false, true, "nested", "original"), true);
        Fixture::new(ty.clone(), ty.clone(), ty)
    };
    let success = Control::default();
    assert!(good().run(&success).unwrap().is_some());
    check_prefixes(&success.trace(), good);

    let ordinary = || {
        Fixture::new(
            nominal(DataType::Utf8, ValueLogicalType::Json),
            nominal(DataType::Utf8, ValueLogicalType::Json),
            physical(DataType::Utf8, true),
        )
    };
    let declining = Control::default();
    assert!(ordinary().run(&declining).unwrap().is_none());
    let trace = declining.trace();
    assert!(
        trace.len() >= 2,
        "entry and completed ordinary tail are both observed"
    );
    assert!(
        trace.last().unwrap().1 > 0,
        "root flags and attempted remap are completed work"
    );
    check_prefixes(&trace, ordinary);
}

fn wide_type() -> FunctionValueType {
    let fields = (0..320)
        .map(|index| {
            Arc::new(
                Field::new(format!("field_{index}"), DataType::Int64, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.fact".into(),
                        format!("fact_{index}"),
                    )]),
                ),
            )
        })
        .collect::<Vec<_>>();
    physical(DataType::Struct(Fields::from(fields)), true)
}

#[test]
fn setop_exact_remap_wide_nested_fields_observe_quantum_and_every_control_prefix() {
    let ty = wide_type();
    let make = || Fixture::new(ty.clone(), ty.clone(), ty.clone());
    let observed = Control::default();
    assert!(make().run(&observed).unwrap().is_some());
    let trace = observed.trace();
    assert!(trace.iter().any(|entry| entry.1 == 256));
    check_prefixes(&trace, make);
}

#[test]
fn setop_exact_remap_empty_and_missing_positions_keep_original_control_boundaries() {
    for mode in 0..5 {
        let make = || {
            let ty = physical(DataType::Int64, true);
            let mut fixture = Fixture::new(ty.clone(), ty.clone(), ty);
            match mode {
                0 => fixture.items.clear(),
                1 => fixture.union.clear(),
                2 => fixture.branch.clear(),
                3 => {
                    fixture.items[0].expr = fixture
                        .arena
                        .intern_observed(
                            ScalarNode::Nested(fixture.items[0].expr),
                            physical(DataType::Int64, true),
                            &Control::default(),
                        )
                        .unwrap();
                }
                4 => fixture.union[0].column_id = ColumnId(777),
                _ => unreachable!(),
            }
            fixture
        };
        let observed = Control::default();
        let result = make().run(&observed).unwrap();
        if mode == 0 {
            assert!(result.unwrap().is_empty());
        } else {
            assert!(result.is_none());
        }
        assert!(observed.trace().len() >= 2);
        check_prefixes(&observed.trace(), make);
    }
}

#[test]
fn setop_exact_remap_long_position_lookup_observes_its_own_quantum_and_decline() {
    for missing in [false, true] {
        let make = || {
            let ty = physical(DataType::Int64, true);
            let mut fixture = Fixture::new(ty.clone(), ty.clone(), ty.clone());
            fixture.union = (0..320)
                .map(|index| column(1_000 + index, "other_union", ty.clone()))
                .collect();
            fixture.branch = (0..320)
                .map(|index| column(2_000 + index, "other_branch", ty.clone()))
                .collect();
            if !missing {
                fixture.union[319].column_id = ColumnId(73);
                fixture.branch[319].column_id = ColumnId(901);
            }
            fixture
        };
        let observed = Control::default();
        let mut fixture = make();
        let result = fixture.run(&observed).unwrap();
        if missing {
            assert!(result.is_none());
        } else {
            let mapped = result.unwrap();
            assert_eq!(mapped.len(), 1);
            assert_eq!(
                scalar_expr_to_column_id(&fixture.arena, mapped[0].expr),
                Some(ColumnId(901)),
            );
        }
        let trace = observed.trace();
        // Primitive types cannot produce this quantum: it belongs to the
        // actual positional search before either type walk or interning.
        assert_eq!(trace[0], (CompilePhase::Validate, 0));
        assert_eq!(trace[1], (CompilePhase::Validate, 256));
        assert!(trace.last().unwrap().1 > 0);
        check_prefixes(&trace, make);
    }
}
