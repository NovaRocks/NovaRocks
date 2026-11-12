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
use crate::physical_connector_payload_v2::{
    ConnectorPayloadCodecError, arc_u8_slice_bytes, bytes_shared_upper,
    decode_connector_payloads_in, encode_connector_payloads_in,
};
use crate::physical_provider_binding_v2::{
    ProviderBindingCodecError, ProviderBindingSource, decode_joint_provider_bindings_in,
    encode_joint_provider_bindings_in,
};
use novarocks_connector_contract::ConnectorWriteBinding;

fn run_read(
    c: &Control,
    stop: Option<(usize, CompileControlError)>,
    decode: bool,
    malformed: bool,
    count: usize,
    l: ProviderReadProjectionLimits,
    snapshots: &mut Vec<ProviderReadProjectionFacts>,
) -> Result<(), Error> {
    let sources: Vec<_> = (0..count)
        .map(|_| read(ConnectorReadRelationKind::Table))
        .collect();
    let base = sources.first().unwrap();
    let bi = [(u32::MAX, &base.binding)];
    let pi = [(0, base.relation.table()), (u32::MAX, base.relation.view())];
    let b = encode_provider_bindings(&bi, PRIOR_SOURCE, binding_limits(), c)?;
    let p = encode_connector_payloads(&pi, PRIOR_SOURCE, payload_limits(), c)?;
    let db = decode_provider_bindings(b.as_wire(), PRIOR_SOURCE, binding_limits(), c)?;
    let dp = decode_connector_payloads(p.as_wire(), PRIOR_SOURCE, payload_limits(), c)?;
    c.arm(stop);
    let mut work = CompileCheckpoints::try_new(
        c,
        if decode {
            CompilePhase::Decode
        } else {
            CompilePhase::Encode
        },
    )?;
    let result = (|| {
        if decode {
            let mut raw: Vec<_> = (0..count).map(|i| expected(i as u32, 1)).collect();
            if malformed {
                raw[0].table_payload_id = Some(7);
            }
            let out = decode_provider_reads_in(
                &raw,
                &db,
                &dp,
                SOURCE,
                l,
                &mut |f| {
                    snapshots.push(*f);
                    Ok(())
                },
                &mut work,
            )?;
            assert_eq!(out.as_wire(), raw);
        } else {
            // Sender identity is whole source pointer, so lend the original source
            // repeatedly at distinct IDs rather than substituting equal bindings.
            let inputs: Vec<_> = (0..count).map(|i| (i as u32, base)).collect();
            let out = encode_provider_reads_in(
                &inputs,
                &b,
                &p,
                SOURCE,
                l,
                &mut |f| {
                    snapshots.push(*f);
                    Ok(())
                },
                &mut work,
            )?;
            assert_eq!(out.as_wire().len(), count);
        }
        Ok(())
    })();
    finish(result, work)
}

#[test]
fn caller_owned_joint_and_all_six_read_kinds_keep_original_sparse_sources() {
    for (i, kind) in KINDS.into_iter().enumerate() {
        let source = read(kind);
        let write = ConnectorWriteBinding::new(
            source.binding.descriptor().clone(),
            source.binding.catalog_handle().clone(),
        );
        let inputs = [
            (0, ProviderBindingSource::Write(&write)),
            (u32::MAX, ProviderBindingSource::Read(&source.binding)),
        ];
        let payload_inputs = [
            (0, source.relation.table()),
            (u32::MAX, source.relation.view()),
        ];
        let c = Control::default();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
        let b = encode_joint_provider_bindings_in(
            &inputs,
            PRIOR_SOURCE,
            binding_limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let p = encode_connector_payloads_in(
            &payload_inputs,
            PRIOR_SOURCE,
            payload_limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let reads = [(u32::MAX, &source)];
        let r =
            encode_provider_reads_in(&reads, &b, &p, SOURCE, limits(), &mut |_| Ok(()), &mut work)
                .unwrap();
        assert_eq!(r.as_wire(), [expected(u32::MAX, i as i32 + 1)]);
        assert!(std::ptr::eq(
            b.write_binding_in(0, &mut work).unwrap().unwrap(),
            &write
        ));
        assert!(b.binding_in(0, &mut work).unwrap().is_none());
        assert!(std::ptr::eq(
            r.read_in(u32::MAX, &mut work).unwrap().unwrap(),
            &source
        ));
        work.finish().unwrap();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let db = decode_joint_provider_bindings_in(
            b.as_wire(),
            PRIOR_SOURCE,
            binding_limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let dp = decode_connector_payloads_in(
            p.as_wire(),
            PRIOR_SOURCE,
            payload_limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let raw = [expected(0, i as i32 + 1)];
        let dr =
            decode_provider_reads_in(&raw, &db, &dp, SOURCE, limits(), &mut |_| Ok(()), &mut work)
                .unwrap();
        assert_eq!(dr.read_in(0, &mut work).unwrap(), Some(&source));
        assert!(db.write_binding_in(0, &mut work).unwrap().is_some());
        work.finish().unwrap();
    }
}

#[test]
fn caller_owned_success_and_ordinary_refusal_preserve_every_actual_prefix() {
    for (decode, malformed) in [(false, false), (true, false), (true, true)] {
        let c = Control::default();
        let result = run_read(&c, None, decode, malformed, 1, limits(), &mut Vec::new());
        assert_eq!(result.is_ok(), !malformed);
        if malformed {
            assert!(matches!(result, Err(Error::InvalidShape(_))));
        }
        let baseline = trace(&c);
        assert!(baseline.last().unwrap().1 > 0);
        for at in 0..baseline.len() {
            for cause in CAUSES {
                let c = Control::default();
                let result = run_read(
                    &c,
                    Some((at, cause.clone())),
                    decode,
                    malformed,
                    1,
                    limits(),
                    &mut Vec::new(),
                );
                assert!(matches!(result, Err(Error::Control(actual)) if actual == cause));
                assert_eq!(trace(&c), baseline[..=at]);
            }
        }
    }
}

#[test]
fn caller_owned_read_exact_layout_and_all_five_axes_replay_without_prefix_overcharge() {
    for decode in [false, true] {
        let c = Control::default();
        let mut snapshots = Vec::new();
        run_read(&c, None, decode, false, 1, limits(), &mut snapshots).unwrap();
        let f = *snapshots.last().unwrap();
        let expected_requests = if decode { 6 } else { 3 };
        assert_eq!(f.allocation_requests_upper_bound, expected_requests);
        let bytes = if decode {
            Layout::array::<usize>(1).unwrap().size()
                + Layout::array::<ProviderReadReference>(1).unwrap().size()
                + ConnectorReadInputVersion::invalid_length_diagnostic().len()
                + arc_u8_slice_bytes(3).unwrap()
                + 2 * bytes_shared_upper().unwrap()
        } else {
            Layout::array::<usize>(1).unwrap().size()
                + Layout::array::<wire::ProviderReadReferenceDefinition>(1)
                    .unwrap()
                    .size()
                + 3
        };
        assert_eq!(f.allocation_request_bytes_upper_bound, bytes);
        assert_eq!(
            f.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + bytes
        );
        for prior in snapshots {
            assert!(prior.allocation_requests_upper_bound <= f.allocation_requests_upper_bound);
            assert!(
                prior.allocation_request_bytes_upper_bound
                    <= f.allocation_request_bytes_upper_bound
            );
            assert!(prior.cumulative_work_upper_bound <= f.cumulative_work_upper_bound);
        }
        run_read(
            &Control::default(),
            None,
            decode,
            false,
            1,
            constrained(&f),
            &mut Vec::new(),
        )
        .unwrap();
        for axis in 0..5 {
            let result = run_read(
                &Control::default(),
                None,
                decode,
                false,
                1,
                under(constrained(&f), axis),
                &mut Vec::new(),
            );
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
    }
}

#[test]
fn caller_owned_known_namespace_headers_precede_pending_quantum_and_late_causes() {
    let source = read(ConnectorReadRelationKind::Table);
    let bi = [(u32::MAX, ProviderBindingSource::Read(&source.binding))];
    let pi = [(0, source.relation.table())];
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            let c = Control::default();
            c.arm(Some((1, cause.clone())));
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
            for _ in 0..pending {
                w.step().unwrap();
            }
            let result = encode_joint_provider_bindings_in(
                &bi,
                PRIOR_SOURCE,
                binding_limits(),
                &mut |_| Err(CompileControlError::ResourceExhausted),
                &mut w,
            );
            assert!(matches!(
                result,
                Err(ProviderBindingCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(trace(&c), [(CompilePhase::Encode, 0)]);
            let c = Control::default();
            c.arm(Some((1, cause)));
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
            for _ in 0..pending {
                w.step().unwrap();
            }
            let result = encode_connector_payloads_in(
                &pi,
                PRIOR_SOURCE,
                payload_limits(),
                &mut |_| Err(CompileControlError::ResourceExhausted),
                &mut w,
            );
            assert!(matches!(
                result,
                Err(ConnectorPayloadCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(trace(&c), [(CompilePhase::Encode, 0)]);
        }
    }
}

#[test]
fn caller_owned_payload_promotion_gates_captured_request_before_lookup_callback() {
    // Receiver: index Vec + output Vec + one terminal diagnostic = three;
    // actual InputVersion Arc = fourth; selected nonempty table Bytes = fifth.
    let c = Control::default();
    let mut l = limits();
    l.max_allocation_requests = 4;
    let mut snapshots = Vec::new();
    let result = run_read(&c, None, true, false, 1, l, &mut snapshots);
    assert!(matches!(
        result,
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    assert_eq!(snapshots.last().unwrap().allocation_requests_upper_bound, 4);
    let refused_prefix = trace(&c);
    for cause in CAUSES {
        let c = Control::default();
        let result = run_read(
            &c,
            Some((refused_prefix.len(), cause)),
            true,
            false,
            1,
            l,
            &mut Vec::new(),
        );
        assert!(matches!(
            result,
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(trace(&c), refused_prefix);
    }
}

#[test]
fn caller_owned_wide_actual_read_work_and_foreign_controller_are_not_namespace_grants() {
    for decode in [false, true] {
        let c = Control::default();
        run_read(&c, None, decode, false, 320, limits(), &mut Vec::new()).unwrap();
        let baseline = trace(&c);
        let quantum = baseline
            .iter()
            .position(|(_, n)| *n == 256)
            .expect("actual quantum");
        for at in [0, quantum, baseline.len() - 1] {
            for cause in CAUSES {
                let c = Control::default();
                assert!(
                    matches!(run_read(&c, Some((at, cause.clone())), decode, false, 320, limits(), &mut Vec::new()), Err(Error::Control(actual)) if actual == cause)
                );
                assert_eq!(trace(&c), baseline[..=at]);
            }
        }
    }
    let source = read(ConnectorReadRelationKind::Table);
    let c = Control::default();
    let other = Control::default();
    let bi = [(u32::MAX, &source.binding)];
    let pi = [
        (0, source.relation.table()),
        (u32::MAX, source.relation.view()),
    ];
    let b = encode_provider_bindings(&bi, PRIOR_SOURCE, binding_limits(), &c).unwrap();
    let p = encode_connector_payloads(&pi, PRIOR_SOURCE, payload_limits(), &c).unwrap();
    let mut w = CompileCheckpoints::try_new(&other, CompilePhase::Encode).unwrap();
    let inputs = [(0, &source)];
    let result =
        encode_provider_reads_in(&inputs, &b, &p, SOURCE, limits(), &mut |_| Ok(()), &mut w);
    assert!(matches!(result, Err(Error::InvalidShape(_))));
    assert_eq!(trace(&other), [(CompilePhase::Encode, 0)]);
    let foreign = source.clone();
    let inputs = [(0, &foreign)];
    c.arm(None);
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
    let result =
        encode_provider_reads_in(&inputs, &b, &p, SOURCE, limits(), &mut |_| Ok(()), &mut w)
            .map(|_| ());
    assert!(matches!(finish(result, w), Err(Error::Binding(_))));
    c.arm(None);
    let inputs = [(0, &source)];
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
    let result =
        encode_provider_reads_in(&inputs, &b, &p, 0, limits(), &mut |_| Ok(()), &mut w).map(|_| ());
    assert!(matches!(finish(result, w), Err(Error::InvalidShape(_))));
    assert!(trace(&c).last().unwrap().1 > 0);
}

#[test]
fn caller_owned_payload_all_six_categories_original_data_and_exact_axes() {
    use crate::physical_connector_payload_v2::ConnectorPayloadProjectionFacts;
    let categories = [
        ConnectorCodecCategory::ReadTable,
        ConnectorCodecCategory::ReadView,
        ConnectorCodecCategory::ReadColumn,
        ConnectorCodecCategory::ReadSplit,
        ConnectorCodecCategory::WriteHandle,
        ConnectorCodecCategory::CommitFragment,
    ];
    let sources: Vec<_> = categories
        .into_iter()
        .map(|k| payload(k, &[0, 255, 17]))
        .collect();
    let ids = [u32::MAX, 0, 9, 17, 42, 3];
    let inputs: Vec<_> = ids.into_iter().zip(sources.iter()).collect();
    let c = Control::default();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
    let encoded = encode_connector_payloads_in(
        &inputs,
        PRIOR_SOURCE,
        payload_limits(),
        &mut |_| Ok(()),
        &mut w,
    )
    .unwrap();
    w.finish().unwrap();
    for (i, raw) in encoded.as_wire().iter().enumerate() {
        assert_eq!(raw.id, ids[i]);
        assert_eq!(raw.payload.as_ref().unwrap().payload, [0, 255, 17]);
        assert_eq!(
            raw.payload
                .as_ref()
                .unwrap()
                .header
                .as_ref()
                .unwrap()
                .category,
            i as i32 + 1
        );
    }
    let exact = |f: ConnectorPayloadProjectionFacts| {
        crate::physical_connector_payload_v2::ConnectorPayloadProjectionLimits {
            max_definitions: f.definition_count,
            max_payload_bytes: f.payload_bytes,
            max_allocation_requests: f.allocation_requests_upper_bound,
            max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: f
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: f.cumulative_work_upper_bound,
        }
    };
    for decode in [false, true] {
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let decoded = decode_connector_payloads_in(
            encoded.as_wire(),
            PRIOR_SOURCE,
            payload_limits(),
            &mut |_| Ok(()),
            &mut w,
        )
        .unwrap();
        w.finish().unwrap();
        let f = if decode {
            *decoded.facts()
        } else {
            *encoded.facts()
        };
        // Every category has one nonempty original body. Sender output owns
        // provider/catalog/body/version; receiver uses two identity Arcs and
        // one body Vec plus its conditional Bytes Shared allocation.
        assert_eq!(f.allocation_requests_upper_bound, 26);
        assert_eq!(f.payload_bytes, 18);
        for axis in 0..=6 {
            let mut l = exact(f);
            match axis {
                0 => {}
                1 => l.max_definitions -= 1,
                2 => l.max_payload_bytes -= 1,
                3 => l.max_allocation_requests -= 1,
                4 => l.max_allocation_request_bytes -= 1,
                5 => l.max_coexisting_source_and_request_bytes -= 1,
                6 => l.max_work -= 1,
                _ => unreachable!(),
            }
            let c = Control::default();
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            let result = if decode {
                decode_connector_payloads_in(
                    encoded.as_wire(),
                    PRIOR_SOURCE,
                    l,
                    &mut |_| Ok(()),
                    &mut w,
                )
                .map(|_| ())
            } else {
                encode_connector_payloads_in(&inputs, PRIOR_SOURCE, l, &mut |_| Ok(()), &mut w)
                    .map(|_| ())
            };
            if axis == 0 {
                result.unwrap();
                w.finish().unwrap();
            } else {
                assert!(matches!(
                    result,
                    Err(ConnectorPayloadCodecError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
            }
        }
    }
}

#[test]
fn original_arc_numeric_author_keeps_plain_diagnostics_and_parent_first_cause() {
    use crate::physical_connector_payload_v2::{
        arc_u8_slice_bytes_for_mode, layout_resource_error_for_mode,
    };
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    // These are genuine pure Layout/Arc request-author inputs, not forged
    // slices or a claim that an enormous Connector source was allocated.
    for (n, message) in [
        (usize::MAX, "connector identity layout overflow"),
        (
            isize::MAX as usize,
            "connector identity Arc layout overflow",
        ),
    ] {
        assert!(matches!(arc_u8_slice_bytes_for_mode(n, false),
            Err(ConnectorPayloadCodecError::InvalidShape(actual)) if actual == message));
        for pending in [0, 254, 255] {
            for cause in CAUSES {
                let c = Control::default();
                c.arm(Some((1, cause)));
                let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
                for _ in 0..pending {
                    work.step().unwrap();
                }
                let result = (|| {
                    let _known_request = arc_u8_slice_bytes_for_mode(n, true)?;
                    work.step()?;
                    Ok::<(), ConnectorPayloadCodecError>(())
                })();
                assert!(matches!(
                    result,
                    Err(ConnectorPayloadCodecError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(trace(&c), [(CompilePhase::Decode, 0)]);
            }
        }
    }
    for observed in [false, true] {
        assert!(matches!(
            layout_resource_error_for_mode(
                LayoutResourceError::SourceModel,
                observed,
                "original source-model diagnostic"
            ),
            ConnectorPayloadCodecError::InvalidShape("original source-model diagnostic")
        ));
    }
    for error in [
        LayoutResourceError::ArcHeader,
        LayoutResourceError::ArcBacking,
        LayoutResourceError::Arithmetic,
        LayoutResourceError::BytesShared,
    ] {
        assert!(matches!(
            layout_resource_error_for_mode(error, true, "numeric"),
            ConnectorPayloadCodecError::Control(CompileControlError::ResourceExhausted)
        ));
        assert!(matches!(
            layout_resource_error_for_mode(error, false, "numeric"),
            ConnectorPayloadCodecError::InvalidShape("numeric")
        ));
    }
    assert_eq!(arc_u8_slice_bytes_for_mode(0, true).unwrap(), 16);
    assert_eq!(arc_u8_slice_bytes_for_mode(3, true).unwrap(), 24);
}
