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

use super::super::*;
use crate::ConnectorReadRelationKind;
use crate::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorFunctionName,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding,
    ConnectorReadRelationPayload, ConnectorValue, Domain,
};
use bytes::Bytes;
use novarocks_type_contract::owned_resources::layout;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::{alloc::Layout, sync::Mutex};
const SOURCE: usize = 64 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal")
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn recipe(columns: usize) -> ConnectorReadRelationRecipeDraft {
    let instance = ConnectorInstanceId::try_from_canonical("scan_fixture").unwrap();
    let binding = ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([7; 32])),
    );
    let payload = |category| {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            Bytes::from_static(b"actual scan fixture"),
        )
    };
    ConnectorReadRelationRecipeDraft::try_new(
        binding.clone(),
        ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            payload(ConnectorCodecCategory::ReadTable),
            payload(ConnectorCodecCategory::ReadView),
        ),
        (0..columns)
            .map(|_| payload(ConnectorCodecCategory::ReadColumn))
            .collect(),
    )
    .unwrap()
}
fn input() -> ScanConstructionInput<ConnectorReadRelationRecipeDraft> {
    ScanConstructionInput {
        recipe: recipe(1),
        assignments: vec![StaticScanAssignment::new(
            Arc::from("x"),
            ConnectorValueType::BigInt,
        )],
        enforced_predicate: TupleDomain::all(),
        unenforced_predicate: TupleDomain::all(),
        remaining_expression: None,
        dynamic_filters: vec![],
        max_batch_rows: NonZeroU64::new(17).unwrap(),
        max_batch_bytes: NonZeroU64::new(101).unwrap(),
    }
}
fn variable() -> ConnectorExpression {
    ConnectorExpression::Variable {
        name: Arc::from("x"),
        value_type: ConnectorValueType::BigInt,
    }
}
fn call(arguments: Vec<ConnectorExpression>) -> ConnectorExpression {
    ConnectorExpression::Call {
        function: ConnectorFunctionName::try_new("fixture").unwrap(),
        value_type: ConnectorValueType::BigInt,
        arguments,
    }
}
fn plain(
    i: ScanConstructionInput<ConnectorReadRelationRecipeDraft>,
) -> Result<FrozenConnectorScan, StaticConnectorScanError> {
    FrozenConnectorScan::try_new(
        i.recipe,
        i.assignments,
        i.enforced_predicate,
        i.unenforced_predicate,
        i.remaining_expression,
        i.dynamic_filters,
        i.max_batch_rows,
        i.max_batch_bytes,
    )
}
fn observed(
    i: ScanConstructionInput<ConnectorReadRelationRecipeDraft>,
    c: &Control,
) -> Result<FrozenConnectorScan, PureProviderCompileError<ReadScanOwnedError>> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
    let result = FrozenConnectorScan::try_new_observed(i, SOURCE, &mut |_| Ok(()), &mut work);
    match result {
        Err(PureProviderCompileError::Control(e)) => Err(PureProviderCompileError::Control(e)),
        result => {
            work.finish()?;
            result
        }
    }
}
fn fixture() -> ScanConstructionInput<ConnectorReadRelationRecipeDraft> {
    let mut i = input();
    let mut arguments = Vec::with_capacity(128);
    arguments.push(ConnectorExpression::Constant {
        value: Some(ConnectorValue::BigInt(-9)),
        value_type: ConnectorValueType::BigInt,
    });
    arguments.push(ConnectorExpression::Constant {
        value: None,
        value_type: ConnectorValueType::Varchar,
    });
    arguments.push(ConnectorExpression::FieldDereference {
        target: Box::new(variable()),
        field_index: u32::MAX,
        value_type: ConnectorValueType::BigInt,
    });
    i.remaining_expression = Some(call(arguments));
    i.dynamic_filters = vec![StaticScanDynamicFilter::new(u32::MAX, Arc::from("x"))];
    i
}
#[test]
fn observed_scan_preserves_original_law_values_and_detached_expression_containers() {
    let i = fixture();
    let old = plain(fixture()).unwrap();
    let original = i.remaining_expression.as_ref().unwrap();
    let ConnectorExpression::Call {
        function,
        arguments,
        ..
    } = original
    else {
        panic!()
    };
    let fn_name = function.clone();
    let ConnectorExpression::FieldDereference { target, .. } = &arguments[2] else {
        panic!()
    };
    let ConnectorExpression::Variable { name, .. } = target.as_ref() else {
        panic!()
    };
    let name = name.clone();
    let pointer = target.as_ref() as *const _;
    let scan = observed(i, &Control::default()).unwrap();
    assert_eq!(scan, old);
    assert_eq!(scan.max_batch_rows().get(), 17);
    assert_eq!(scan.max_batch_bytes().get(), 101);
    assert_eq!(scan.dynamic_filters()[0].filter_id(), u32::MAX);
    let ConnectorExpression::Call {
        function,
        arguments,
        ..
    } = scan.remaining_expression().unwrap()
    else {
        panic!()
    };
    assert_eq!(function, &fn_name);
    assert_eq!(arguments.capacity(), arguments.len());
    assert_eq!(arguments.len(), 3);
    assert!(matches!(
        arguments[0],
        ConnectorExpression::Constant {
            value: Some(ConnectorValue::BigInt(-9)),
            ..
        }
    ));
    assert!(matches!(
        arguments[1],
        ConnectorExpression::Constant {
            value: None,
            value_type: ConnectorValueType::Varchar
        }
    ));
    let ConnectorExpression::FieldDereference {
        target,
        field_index,
        ..
    } = &arguments[2]
    else {
        panic!()
    };
    assert_eq!(*field_index, u32::MAX);
    assert_ne!(target.as_ref() as *const _, pointer);
    let ConnectorExpression::Variable { name: copied, .. } = target.as_ref() else {
        panic!()
    };
    assert!(Arc::ptr_eq(copied, &name));
}
#[test]
fn scan_requests_have_independent_locked_tree_and_arc_layout_oracle() {
    let c = Control::default();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let mut final_facts = WriterOwnedResourceFacts::default();
    let scan = FrozenConnectorScan::try_new_observed(
        input(),
        SOURCE,
        &mut |facts| {
            final_facts = *facts;
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    // Diagnostic16, insertion-only variables1, assignment trim1+Arc1,
    // empty filter Arc1 and private facts Arc1. Source input Vec is not a request.
    assert_eq!(final_facts.allocation_requests, 21);
    let tree = btree::node_layout_typed::<&str, ConnectorValueType>()
        .unwrap()
        .size();
    let expected = 16 * 1024
        + tree
        + Layout::array::<StaticScanAssignment>(1).unwrap().size()
        + layout::arc_layout(Layout::array::<StaticScanAssignment>(1).unwrap())
            .unwrap()
            .size()
        + layout::arc_layout(Layout::array::<StaticScanDynamicFilter>(0).unwrap())
            .unwrap()
            .size()
        + layout::arc_layout(Layout::new::<ConnectorScanFacts>())
            .unwrap()
            .size();
    assert_eq!(final_facts.requested_bytes, expected);
    assert_eq!(final_facts.coexistence_bytes, SOURCE + expected);
    assert_eq!(scan.assignments()[0].variable(), "x");
    assert!(scan.remaining_expression().is_none());
}
fn malformed(case: usize) -> ScanConstructionInput<ConnectorReadRelationRecipeDraft> {
    let mut i = input();
    match case {
        0 => i.assignments.clear(),
        1 => {
            i.assignments[0] = StaticScanAssignment::new(Arc::from(""), ConnectorValueType::BigInt)
        }
        2 => {
            i.recipe = recipe(2);
            i.assignments.push(i.assignments[0].clone())
        }
        3 => {
            i.enforced_predicate = TupleDomain::with_column_domains(BTreeMap::from([(
                ScanColumnId::new(1),
                Domain::single_value(ConnectorValue::BigInt(3)).unwrap(),
            )]))
            .unwrap()
        }
        4 => {
            i.enforced_predicate = TupleDomain::with_column_domains(BTreeMap::from([(
                ScanColumnId::new(0),
                Domain::single_value(ConnectorValue::Varchar(Arc::from("three"))).unwrap(),
            )]))
            .unwrap()
        }
        5 => {
            i.remaining_expression = Some(ConnectorExpression::Constant {
                value: Some(ConnectorValue::BigInt(1)),
                value_type: ConnectorValueType::Varchar,
            })
        }
        6 => {
            i.remaining_expression = Some(ConnectorExpression::Variable {
                name: Arc::from("foreign"),
                value_type: ConnectorValueType::BigInt,
            })
        }
        7 => {
            i.remaining_expression = Some(ConnectorExpression::Variable {
                name: Arc::from("x"),
                value_type: ConnectorValueType::Varchar,
            })
        }
        8 => {
            i.dynamic_filters = vec![
                StaticScanDynamicFilter::new(0, Arc::from("x")),
                StaticScanDynamicFilter::new(0, Arc::from("x")),
            ]
        }
        9 => i.dynamic_filters = vec![StaticScanDynamicFilter::new(0, Arc::from("foreign"))],
        _ => panic!(),
    }
    i
}
#[test]
fn scan_ordinary_errors_remain_original_typed_categories_and_order() {
    let expected = [
        StaticConnectorScanError::EmptyAssignments,
        StaticConnectorScanError::InvalidVariable,
        StaticConnectorScanError::DuplicateVariable,
        StaticConnectorScanError::InvalidPredicateColumn,
        StaticConnectorScanError::PredicateTypeMismatch,
        StaticConnectorScanError::InvalidExpression,
        StaticConnectorScanError::UnknownExpressionVariable,
        StaticConnectorScanError::ExpressionTypeMismatch,
        StaticConnectorScanError::DuplicateDynamicFilter,
        StaticConnectorScanError::UnknownDynamicFilterVariable,
    ];
    for (case, expected) in expected.into_iter().enumerate() {
        assert_eq!(plain(malformed(case)).unwrap_err(), expected);
        assert!(
            matches!(observed(malformed(case),&Control::default()),Err(PureProviderCompileError::Provider(ReadScanOwnedError::Contract(e))) if e==expected)
        );
    }
    let mut i = malformed(8);
    i.remaining_expression = Some(ConnectorExpression::Variable {
        name: Arc::from("foreign"),
        value_type: ConnectorValueType::Varchar,
    });
    assert!(matches!(
        observed(i, &Control::default()),
        Err(PureProviderCompileError::Provider(
            ReadScanOwnedError::Contract(StaticConnectorScanError::UnknownExpressionVariable)
        ))
    ));
}
#[test]
fn scan_every_actual_small_success_and_ordinary_tail_preserves_three_control_causes() {
    for case in 0..4 {
        let make = || match case {
            0 => fixture(),
            1 => malformed(6),
            2 => malformed(8),
            _ => malformed(5),
        };
        let baseline = Control::default();
        let result = observed(make(), &baseline);
        if case == 0 {
            assert!(result.is_ok())
        } else {
            assert!(result.is_err())
        }
        let trace = baseline.trace();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in CAUSES {
                let c = Control {
                    trace: Mutex::default(),
                    stop: Some((at, cause.clone())),
                };
                assert!(
                    matches!(observed(make(),&c),Err(PureProviderCompileError::Control(e)) if e==cause)
                );
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
    }
}
#[test]
fn scan_known_request_and_true_source_floor_refuse_before_late_control() {
    for cause in CAUSES {
        let c = Control {
            trace: Mutex::default(),
            stop: Some((1, cause)),
        };
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        for _ in 0..255 {
            work.step().unwrap()
        }
        let result = FrozenConnectorScan::try_new_observed(
            input(),
            SOURCE,
            &mut |facts| {
                if facts.allocation_requests > 16 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            },
            &mut work,
        );
        assert!(matches!(
            result,
            Err(PureProviderCompileError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(c.trace(), vec![(CompilePhase::Decode, 0)]);
    }
    // With no expression, all work is known in prepare. A cap one below
    // that actual admitted snapshot must win before its first opaque flush.
    let baseline = Control::default();
    let mut baseline_work = CompileCheckpoints::try_new(&baseline, CompilePhase::Decode).unwrap();
    let mut work_bound = 0;
    FrozenConnectorScan::try_new_observed(
        input(),
        SOURCE,
        &mut |facts| {
            work_bound = work_bound.max(facts.work_units);
            Ok(())
        },
        &mut baseline_work,
    )
    .unwrap();
    baseline_work.finish().unwrap();
    assert!(work_bound > 2 * SOURCE + 256);
    for cause in CAUSES {
        let c = Control {
            trace: Mutex::default(),
            stop: Some((1, cause)),
        };
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let result = FrozenConnectorScan::try_new_observed(
            input(),
            SOURCE,
            &mut |facts| {
                if facts.work_units > work_bound - 1 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            },
            &mut work,
        );
        assert!(matches!(
            result,
            Err(PureProviderCompileError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(c.trace(), vec![(CompilePhase::Decode, 0)]);
    }
    let c = Control::default();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    assert!(
        matches!(FrozenConnectorScan::try_new_observed(input(),0,&mut |_|Ok(()),&mut work),Err(PureProviderCompileError::Provider(ReadScanOwnedError::Resources(e))) if e.kind()==ConnectorErrorKind::InvalidRequest)
    );
    assert_eq!(c.trace(), vec![(CompilePhase::Decode, 0)]);
}
#[test]
fn scan_wide_actual_expression_loops_have_real_quantum_and_ordered_copy() {
    let make = || {
        let mut i = input();
        i.remaining_expression = Some(call(
            (0..320)
                .map(|n| ConnectorExpression::Constant {
                    value: Some(ConnectorValue::BigInt(n)),
                    value_type: ConnectorValueType::BigInt,
                })
                .collect(),
        ));
        i
    };
    let c = Control::default();
    let scan = observed(make(), &c).unwrap();
    let trace = c.trace();
    let quantum = trace
        .iter()
        .position(|(_, n)| *n == 256)
        .expect("actual validation or copy loop quantum");
    let ConnectorExpression::Call { arguments, .. } = scan.remaining_expression().unwrap() else {
        panic!()
    };
    assert_eq!(arguments.len(), 320);
    for (n, a) in arguments.iter().enumerate() {
        assert!(
            matches!(a,ConnectorExpression::Constant{value:Some(ConnectorValue::BigInt(v)),..} if *v==n as i64)
        )
    }
    for at in [0, quantum, trace.len() - 1] {
        for cause in CAUSES {
            let c = Control {
                trace: Mutex::default(),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(observed(make(),&c),Err(PureProviderCompileError::Control(e)) if e==cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}
#[test]
fn scan_expression_depth_and_retained_bytes_keep_original_contract_law() {
    let make = |depth| {
        let mut i = input();
        let mut e = variable();
        for _ in 1..depth {
            e = ConnectorExpression::FieldDereference {
                target: Box::new(e),
                field_index: 0,
                value_type: ConnectorValueType::BigInt,
            }
        }
        i.remaining_expression = Some(e);
        i
    };
    assert!(observed(make(64), &Control::default()).is_ok());
    assert_eq!(
        plain(make(65)).unwrap_err(),
        StaticConnectorScanError::InvalidExpression
    );
    assert!(matches!(
        observed(make(65), &Control::default()),
        Err(PureProviderCompileError::Provider(
            ReadScanOwnedError::Contract(StaticConnectorScanError::InvalidExpression)
        ))
    ));
    let make_large = || {
        let mut i = input();
        let payload: Arc<str> = Arc::from("z".repeat(32 * 1024));
        i.remaining_expression = Some(call(
            (0..1024)
                .map(|_| ConnectorExpression::Constant {
                    value: Some(ConnectorValue::Varchar(payload.clone())),
                    value_type: ConnectorValueType::Varchar,
                })
                .collect(),
        ));
        i
    };
    assert_eq!(
        plain(make_large()).unwrap_err(),
        StaticConnectorScanError::TooManyRetainedBytes
    );
    assert!(matches!(
        observed(make_large(), &Control::default()),
        Err(PureProviderCompileError::Provider(
            ReadScanOwnedError::Contract(StaticConnectorScanError::TooManyRetainedBytes)
        ))
    ));
}

#[test]
fn captured_expression_copy_headers_admit_box_and_call_before_pending_quantum() {
    for expression in [
        ConnectorExpression::FieldDereference {
            target: Box::new(variable()),
            field_index: 0,
            value_type: ConnectorValueType::BigInt,
        },
        call(vec![variable()]),
    ] {
        expression.validate().unwrap();
        for cause in CAUSES {
            let c = Control {
                trace: Mutex::default(),
                stop: Some((1, cause)),
            };
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                work.step().unwrap()
            }
            let mut admit = |facts: &WriterOwnedResourceFacts| {
                if facts.allocation_requests > 16 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            };
            let mut context = ObservedCopy::new(SOURCE, &mut admit, &mut work).unwrap();
            let result = ObservedScan(&mut context).expression_node(&expression);
            assert!(matches!(
                result,
                Err(PureProviderCompileError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(c.trace(), vec![(CompilePhase::Decode, 0)]);
        }
    }
}
