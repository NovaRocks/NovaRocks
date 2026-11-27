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
use crate::{ConnectorCodecRevision, ConnectorEnvelopeHeader, ConnectorReadRelationKind};
use bytes::Bytes;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::{alloc::Layout, sync::Mutex};

const SOURCE: usize = 128 * 1024 * 1024;
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push(units);
        if let Some((index, cause)) = self.stop.clone()
            && index == at
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn relation(
    binding: &ConnectorReadBinding,
    table: Bytes,
    view: Bytes,
) -> ConnectorReadRelationPayload {
    ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::Table,
        super::tests::payload(binding, ConnectorCodecCategory::ReadTable, table),
        super::tests::payload(binding, ConnectorCodecCategory::ReadView, view),
    )
}
fn invoke(
    binding: &ConnectorReadBinding,
    relation: ConnectorReadRelationPayload,
    columns: Vec<ConnectorEncodedPayload>,
    control: &Control,
    admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ReadRecipeOwnedError>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
    let result = ConnectorReadRelationRecipeDraft::try_new_observed(
        binding, relation, columns, SOURCE, admit, &mut work,
    );
    match result {
        Err(PureProviderCompileError::Control(cause)) => {
            Err(PureProviderCompileError::Control(cause))
        }
        other => {
            work.finish()?;
            other
        }
    }
}
fn fixture() -> (
    ConnectorReadBinding,
    ConnectorReadRelationPayload,
    Vec<ConnectorEncodedPayload>,
) {
    let binding = super::tests::binding(1);
    let relation = relation(
        &binding,
        Bytes::from_static(b"table"),
        Bytes::from_static(b"view"),
    );
    let columns = vec![super::tests::payload(
        &binding,
        ConnectorCodecCategory::ReadColumn,
        Bytes::from_static(b"col"),
    )];
    (binding, relation, columns)
}
#[test]
fn observed_read_recipe_preserves_ordered_bytes_and_detaches_short_sliced_backing() {
    let (binding, _, _) = fixture();
    let backing = Bytes::from(vec![42; 1024 * 1024]);
    let table = backing.slice(17..22);
    let view = Bytes::from_static(b"view");
    let input = relation(&binding, table.clone(), view.clone());
    let column = super::tests::payload(
        &binding,
        ConnectorCodecCategory::ReadColumn,
        backing.slice(51..58),
    );
    let expected = ConnectorReadRelationRecipeDraft::try_new(
        binding.clone(),
        input.clone(),
        vec![column.clone(), column.clone()],
    )
    .unwrap();
    let actual = invoke(
        &binding,
        input,
        vec![column.clone(), column],
        &Control::default(),
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.payload_bytes(), 5 + 4 + 7 + 7);
    assert_eq!(actual.relation().table().payload().as_ref(), [42; 5]);
    assert_eq!(actual.relation().view().payload().as_ref(), b"view");
    assert_eq!(actual.columns().len(), 2);
    assert_eq!(actual.columns()[0].payload().as_ref(), [42; 7]);
    assert_ne!(actual.relation().table().payload().as_ptr(), table.as_ptr());
    assert_ne!(
        actual.columns()[0].payload().as_ptr(),
        actual.columns()[1].payload().as_ptr()
    );
}
#[test]
fn observed_read_recipe_retains_original_header_and_limit_errors_without_string_reclassification() {
    let (binding, input, columns) = fixture();
    let other = super::tests::binding(2);
    let wrong = ConnectorReadRelationPayload::new(
        input.kind(),
        input.table().clone(),
        super::tests::payload(&other, ConnectorCodecCategory::ReadView, Bytes::new()),
    );
    let oversized = relation(
        &binding,
        Bytes::from(vec![0; MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES + 1]),
        Bytes::new(),
    );
    let wide = vec![columns[0].clone(); 4097];
    let recipe = invoke(&binding, input, wide, &Control::default(), &mut |_| Ok(())).unwrap();
    assert_eq!(recipe.columns().len(), 4097);
    for (input, columns) in [(wrong, vec![]), (oversized, vec![])] {
        let expected = ConnectorReadRelationRecipeDraft::try_new(
            binding.clone(),
            input.clone(),
            columns.clone(),
        )
        .unwrap_err();
        let actual = invoke(&binding, input, columns, &Control::default(), &mut |_| {
            Ok(())
        })
        .unwrap_err();
        assert!(
            matches!(actual,PureProviderCompileError::Provider(ReadRecipeOwnedError::Contract(error)) if error==expected)
        );
    }
    let bad_category = ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::Table,
        super::tests::payload(
            &binding,
            ConnectorCodecCategory::ReadView,
            Bytes::from_static(b"wrong"),
        ),
        super::tests::payload(&binding, ConnectorCodecCategory::ReadView, Bytes::new()),
    );
    assert!(matches!(
        invoke(
            &binding,
            bad_category,
            vec![],
            &Control::default(),
            &mut |_| Ok(())
        ),
        Err(PureProviderCompileError::Provider(
            ReadRecipeOwnedError::Contract(ConnectorReadRelationRecipeError::Header(
                ConnectorCodecContractError::CategoryMismatch
            ))
        ))
    ));
    // The header mismatch precedes the byte law exactly as in the original.
    let wrong = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            ConnectorCodecCategory::ReadColumn,
            ConnectorCodecRevision::try_new(2).unwrap(),
        ),
        Bytes::new(),
    );
    let input = relation(&binding, Bytes::new(), Bytes::new());
    assert!(matches!(
        invoke(
            &binding,
            input,
            vec![wrong],
            &Control::default(),
            &mut |_| Ok(())
        ),
        Err(PureProviderCompileError::Provider(
            ReadRecipeOwnedError::Contract(ConnectorReadRelationRecipeError::Header(
                ConnectorCodecContractError::RevisionMismatch
            ))
        ))
    ));
}
#[test]
fn observed_read_recipe_all_relation_kinds_and_empty_columns_keep_empty_arc_request() {
    let binding = super::tests::binding(1);
    for kind in [
        ConnectorReadRelationKind::Table,
        ConnectorReadRelationKind::TableFunction,
        ConnectorReadRelationKind::ChangeWindow,
        ConnectorReadRelationKind::SystemTable,
        ConnectorReadRelationKind::MergeTable,
    ] {
        let input = relation(&binding, Bytes::new(), Bytes::new());
        let input =
            ConnectorReadRelationPayload::new(kind, input.table().clone(), input.view().clone());
        let mut facts = WriterOwnedResourceFacts::default();
        let actual = invoke(&binding, input, vec![], &Control::default(), &mut |known| {
            facts = *known;
            Ok(())
        })
        .unwrap();
        assert_eq!(actual.relation().kind(), kind);
        assert_eq!(actual.payload_bytes(), 0);
        assert!(actual.columns().is_empty());
        assert_eq!(facts.allocation_requests, 17); // diagnostic16 + empty Arc slice
        assert_eq!(
            facts.requested_bytes,
            16 * 1024
                + novarocks_type_contract::owned_resources::layout::arc_layout(
                    Layout::array::<ConnectorEncodedPayload>(0).unwrap()
                )
                .unwrap()
                .size()
        );
    }
}
#[test]
fn observed_read_recipe_hand_layout_and_each_axis_exact_under_cover_complete_copy() {
    let (binding, input, columns) = fixture();
    let mut upper = WriterOwnedResourceFacts::default();
    invoke(
        &binding,
        input.clone(),
        columns.clone(),
        &Control::default(),
        &mut |known| {
            upper = *known;
            Ok(())
        },
    )
    .unwrap();
    let shared = novarocks_type_contract::owned_resources::layout::bytes_shared_upper().unwrap();
    let arc = novarocks_type_contract::owned_resources::layout::arc_layout(
        Layout::array::<ConnectorEncodedPayload>(1).unwrap(),
    )
    .unwrap()
    .size();
    assert_eq!(upper.allocation_requests, 25);
    assert_eq!(
        upper.requested_bytes,
        16 * 1024 + 5 + 4 + 3 + 3 * shared + 2 * size_of::<ConnectorEncodedPayload>() + arc
    );
    assert_eq!(upper.coexistence_bytes, SOURCE + upper.requested_bytes);
    for axis in 0..4 {
        let extract = |f: &WriterOwnedResourceFacts| match axis {
            0 => f.allocation_requests,
            1 => f.requested_bytes,
            2 => f.coexistence_bytes,
            _ => f.work_units,
        };
        for (cap, accepted) in [(extract(&upper), true), (extract(&upper) - 1, false)] {
            let result = invoke(
                &binding,
                input.clone(),
                columns.clone(),
                &Control::default(),
                &mut |known| {
                    if extract(known) > cap {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
            );
            if accepted {
                assert!(result.is_ok());
            } else {
                assert!(matches!(
                    result,
                    Err(PureProviderCompileError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
            }
        }
    }
}
#[test]
fn observed_read_recipe_success_and_ordinary_failure_keep_all_three_actual_control_prefixes() {
    let (binding, input, columns) = fixture();
    let wrong = ConnectorReadRelationPayload::new(
        input.kind(),
        input.table().clone(),
        super::tests::payload(
            &super::tests::binding(2),
            ConnectorCodecCategory::ReadView,
            Bytes::new(),
        ),
    );
    for (input, success) in [(input, true), (wrong, false)] {
        let control = Control::default();
        let result = invoke(
            &binding,
            input.clone(),
            columns.clone(),
            &control,
            &mut |_| Ok(()),
        );
        assert_eq!(result.is_ok(), success);
        let trace = control.trace.lock().unwrap().clone();
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    stop: Some((at, cause.clone())),
                };
                let result = invoke(
                    &binding,
                    input.clone(),
                    columns.clone(),
                    &control,
                    &mut |_| Ok(()),
                );
                assert!(
                    matches!(result,Err(PureProviderCompileError::Control(actual))if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
#[test]
fn observed_read_recipe_numeric_request_refusal_precedes_pending_255_next_control() {
    let (binding, input, columns) = fixture();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            trace: Mutex::new(vec![]),
            stop: Some((1, cause)),
        };
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let result = ConnectorReadRelationRecipeDraft::try_new_observed(
            &binding,
            input.clone(),
            columns.clone(),
            SOURCE,
            &mut |known| {
                if known.allocation_requests > 16 {
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
        assert_eq!(*control.trace.lock().unwrap(), [0]);
    }
}
#[test]
fn observed_read_recipe_actual_wide_chunk_copy_and_source_counts_observe_quantum() {
    let binding = super::tests::binding(1);
    let input = relation(&binding, Bytes::from(vec![19; 260 * 256]), Bytes::new());
    let column = super::tests::payload(
        &binding,
        ConnectorCodecCategory::ReadColumn,
        Bytes::from_static(b"c"),
    );
    let columns = vec![column; 320];
    let control = Control::default();
    let actual = invoke(
        &binding,
        input.clone(),
        columns.clone(),
        &control,
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(actual.columns().len(), 320);
    assert_eq!(
        actual.relation().table().payload().as_ref(),
        vec![19; 260 * 256]
    );
    let trace = control.trace.lock().unwrap().clone();
    let positions = trace
        .iter()
        .enumerate()
        .filter_map(|(i, n)| (*n == 256).then_some(i))
        .collect::<Vec<_>>();
    assert!(positions.len() >= 2); // real source counts and real 256-byte chunks
    for at in [0, positions[0], positions[1], trace.len() - 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            let result = invoke(
                &binding,
                input.clone(),
                columns.clone(),
                &control,
                &mut |_| Ok(()),
            );
            assert!(
                matches!(result,Err(PureProviderCompileError::Control(actual))if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
