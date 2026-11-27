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
use novarocks_connector_contract::ConnectorWriteBinding;
use std::sync::{Mutex, atomic::AtomicUsize};

const SOURCE: usize = 1024 * 1024;
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
            assert!(at <= stop, "callback after refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn trace(c: &Control) -> Vec<(CompilePhase, u32)> {
    c.trace.lock().unwrap().clone()
}
fn limits() -> ProviderBindingProjectionLimits {
    ProviderBindingProjectionLimits {
        max_definitions: 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * SOURCE,
        max_coexisting_source_and_request_bytes: 8 * SOURCE,
        max_work: 64 * SOURCE,
    }
}
fn read(instance: &str, name: &str) -> ConnectorReadBinding {
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::try_from_canonical(instance).unwrap(),
        },
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical(name).unwrap(),
            CatalogVersion::from_bytes([17; 32]),
        ),
    )
}
fn write(instance: &str, name: &str) -> ConnectorWriteBinding {
    let r = read(instance, name);
    ConnectorWriteBinding::new(r.descriptor().clone(), r.catalog_handle().clone())
}
fn expected(id: u32, instance: &str, name: &str) -> wire::ProviderBindingDefinition {
    wire::ProviderBindingDefinition {
        id,
        provider_id: "iceberg".into(),
        instance_id: instance.into(),
        catalog: Some(catalog::CatalogHandle {
            catalog_name: name.into(),
            version: vec![17; 32],
        }),
    }
}
fn exact(f: ProviderBindingProjectionFacts) -> ProviderBindingProjectionLimits {
    ProviderBindingProjectionLimits {
        max_definitions: f.definition_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    }
}
fn under(mut l: ProviderBindingProjectionLimits, axis: usize) -> ProviderBindingProjectionLimits {
    let n = match axis {
        0 => &mut l.max_definitions,
        1 => &mut l.max_allocation_requests,
        2 => &mut l.max_allocation_request_bytes,
        3 => &mut l.max_coexisting_source_and_request_bytes,
        _ => &mut l.max_work,
    };
    *n -= 1;
    l
}
fn all_prefixes(
    mut invoke: impl FnMut(&Control) -> Result<(), Error>,
    ordinary: bool,
    phase: CompilePhase,
) {
    let baseline = Control::default();
    assert_eq!(invoke(&baseline).is_err(), ordinary);
    let positive = trace(&baseline);
    assert!(!positive.is_empty());
    assert!(positive.iter().all(|(p, _)| *p == phase));
    for at in 0..positive.len() {
        for cause in CAUSES {
            let c = Control {
                stop: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(matches!(invoke(&c),Err(Error::Control(actual)) if actual==cause));
            assert_eq!(trace(&c), positive[..=at]);
        }
    }
}
fn array_bytes<T>(n: usize) -> usize {
    Layout::array::<T>(n).unwrap().size()
}
fn arc_bytes(n: usize) -> usize {
    // Independent Rust 1.92 ArcInner repr(C) layout oracle, not the production helper.
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::array::<u8>(n).unwrap())
        .unwrap()
        .0
        .pad_to_align()
        .size()
}

#[test]
fn joint_provider_mixed_wire_and_role_exact_loans_preserve_sparse_order() {
    let r = read("read_lake", "catalog_r");
    let w = write("write_lake", "catalog_w");
    let inputs = [
        (u32::MAX, ProviderBindingSource::Write(&w)),
        (0, ProviderBindingSource::Read(&r)),
    ];
    let c = Control::default();
    let encoded = encode_joint_provider_bindings(&inputs, SOURCE, limits(), &c).unwrap();
    assert_eq!(
        encoded.as_wire(),
        [
            expected(u32::MAX, "write_lake", "catalog_w"),
            expected(0, "read_lake", "catalog_r")
        ]
    );
    assert!(std::ptr::eq(
        encoded.original_control(),
        &c as &dyn PureCompileControl
    ));
    assert!(std::ptr::eq(encoded.binding(0).unwrap().unwrap(), &r));
    assert!(std::ptr::eq(
        encoded.write_binding(u32::MAX).unwrap().unwrap(),
        &w
    ));
    assert!(encoded.binding(u32::MAX).unwrap().is_none());
    assert!(encoded.write_binding(0).unwrap().is_none());
    assert_eq!(encoded.source_id(&r).unwrap(), 0);
    assert_eq!(encoded.write_source_id(&w).unwrap(), u32::MAX);
    let decoded = decode_joint_provider_bindings(encoded.as_wire(), SOURCE, limits(), &c).unwrap();
    assert!(std::ptr::eq(decoded.as_wire(), encoded.as_wire()));
    for (id, instance, name) in [
        (u32::MAX, "write_lake", "catalog_w"),
        (0, "read_lake", "catalog_r"),
    ] {
        let r = decoded.binding(id).unwrap().unwrap();
        let w = decoded.write_binding(id).unwrap().unwrap();
        assert_eq!(r.descriptor(), w.descriptor());
        assert_eq!(r.catalog_handle(), w.catalog_handle());
        assert_eq!(w.descriptor().instance_id.as_str(), instance);
        assert_eq!(w.catalog_handle().catalog_name().as_str(), name);
        assert_eq!(w.catalog_handle().version().as_bytes(), &[17; 32]);
        assert!(std::ptr::eq(
            r.descriptor().provider_id.as_str(),
            w.descriptor().provider_id.as_str()
        ));
        assert!(std::ptr::eq(
            r.descriptor().instance_id.as_str(),
            w.descriptor().instance_id.as_str()
        ));
        assert!(std::ptr::eq(
            r.catalog_handle().catalog_name().as_str(),
            w.catalog_handle().catalog_name().as_str()
        ));
    }
    assert!(decoded.binding(42).unwrap().is_none());
    assert!(decoded.write_binding(42).unwrap().is_none());
}

#[test]
fn joint_provider_cross_role_duplicate_and_exact_pointer_ambiguity_never_fall_through() {
    let r = read("lake", "lake");
    let w = write("lake", "lake");
    let foreign_r = read("lake", "lake");
    let foreign_w = write("lake", "lake");
    let c = Control::default();
    let duplicate = [
        (0, ProviderBindingSource::Read(&r)),
        (0, ProviderBindingSource::Write(&w)),
    ];
    assert!(matches!(
        encode_joint_provider_bindings(&duplicate, SOURCE, limits(), &c),
        Err(Error::Index(_))
    ));
    let aliases = [
        (u32::MAX, ProviderBindingSource::Read(&r)),
        (0, ProviderBindingSource::Read(&r)),
        (7, ProviderBindingSource::Write(&w)),
        (9, ProviderBindingSource::Write(&w)),
    ];
    let e = encode_joint_provider_bindings(&aliases, SOURCE, limits(), &c).unwrap();
    assert!(matches!(
        e.source_id(&r),
        Err(Error::InvalidShape(
            "provider binding source association is ambiguous"
        ))
    ));
    assert!(matches!(
        e.write_source_id(&w),
        Err(Error::InvalidShape(
            "provider binding source association is ambiguous"
        ))
    ));
    assert!(matches!(
        e.source_id(&foreign_r),
        Err(Error::InvalidShape(_))
    ));
    assert!(matches!(
        e.write_source_id(&foreign_w),
        Err(Error::InvalidShape(_))
    ));
    let write_only = [(0, ProviderBindingSource::Write(&w))];
    let e = encode_joint_provider_bindings(&write_only, SOURCE, limits(), &c).unwrap();
    assert!(e.binding(0).unwrap().is_none());
    assert!(e.source_id(&r).is_err());
    let raw = [expected(0, "lake", "lake"), expected(0, "lake", "lake")];
    assert!(matches!(
        decode_joint_provider_bindings(&raw, SOURCE, limits(), &c),
        Err(Error::Index(_))
    ));
}

#[test]
fn joint_provider_independent_request_layout_shares_three_arcs_once_and_preserves_read_facade() {
    let r = read("lake", "lake");
    let w = write("lake", "lake");
    let c = Control::default();
    let inputs = [
        (0, ProviderBindingSource::Read(&r)),
        (u32::MAX, ProviderBindingSource::Write(&w)),
    ];
    let e = encode_joint_provider_bindings(&inputs, SOURCE, limits(), &c).unwrap();
    let ebytes = array_bytes::<usize>(2)
        + array_bytes::<wire::ProviderBindingDefinition>(2)
        + 2 * (7 + 4 + 4 + 32);
    assert_eq!(e.facts().allocation_requests_upper_bound, 10);
    assert_eq!(e.facts().allocation_request_bytes_upper_bound, ebytes);
    assert_eq!(
        e.facts().coexisting_source_and_request_bytes_upper_bound,
        SOURCE + ebytes
    );
    let d = decode_joint_provider_bindings(e.as_wire(), SOURCE, limits(), &c).unwrap();
    let dbytes = array_bytes::<usize>(2)
        + array_bytes::<ConnectorReadBinding>(2)
        + array_bytes::<ConnectorWriteBinding>(2)
        + 2 * (arc_bytes(7) + 2 * arc_bytes(4));
    assert_eq!(d.facts().allocation_requests_upper_bound, 9);
    assert_eq!(d.facts().allocation_request_bytes_upper_bound, dbytes);
    assert_eq!(
        d.facts().coexisting_source_and_request_bytes_upper_bound,
        SOURCE + dbytes
    );
    // The receiving pair shares identities; retained lower floor bills their backing once.
    let retained = SOURCE
        + size_of::<DecodedProviderBindings<'_, '_>>()
        + array_bytes::<usize>(2)
        + array_bytes::<ConnectorReadBinding>(d.bindings.capacity())
        + array_bytes::<ConnectorWriteBinding>(d.write_bindings.capacity())
        + 2 * (arc_bytes(7) + 2 * arc_bytes(4));
    assert_eq!(d.retained_invoice_floor().unwrap(), retained);
    let old_inputs = [(0, &r)];
    let joint_inputs = [(0, ProviderBindingSource::Read(&r))];
    let old = encode_provider_bindings(&old_inputs, SOURCE, limits(), &c).unwrap();
    let joint = encode_joint_provider_bindings(&joint_inputs, SOURCE, limits(), &c).unwrap();
    assert_eq!(old.as_wire(), joint.as_wire());
    assert_eq!(old.facts(), joint.facts());
    let old = decode_provider_bindings(joint.as_wire(), SOURCE, limits(), &c).unwrap();
    assert!(old.write_binding(0).unwrap().is_none());
    assert_eq!(old.facts().allocation_requests_upper_bound, 5);
    let empty = encode_joint_provider_bindings(&[], 0, limits(), &c).unwrap();
    assert_eq!(empty.facts().allocation_requests_upper_bound, 0);
    let empty = decode_joint_provider_bindings(&[], 0, limits(), &c).unwrap();
    assert_eq!(empty.facts().allocation_requests_upper_bound, 0);
    assert!(empty.write_binding(u32::MAX).unwrap().is_none());
}

#[test]
fn joint_provider_all_five_exact_under_limits_keep_numeric_resource_primary() {
    let r = read("lake", "lake");
    let w = write("lake", "lake");
    let inputs = [
        (0, ProviderBindingSource::Read(&r)),
        (u32::MAX, ProviderBindingSource::Write(&w)),
    ];
    let c = Control::default();
    let e = encode_joint_provider_bindings(&inputs, SOURCE, limits(), &c).unwrap();
    let raw = e.as_wire();
    let d = decode_joint_provider_bindings(raw, SOURCE, limits(), &c).unwrap();
    for direction in 0..2 {
        let exact = exact(if direction == 0 {
            *e.facts()
        } else {
            *d.facts()
        });
        let invoke = |control: &Control, l| {
            if direction == 0 {
                encode_joint_provider_bindings(&inputs, SOURCE, l, control).map(|_| ())
            } else {
                decode_joint_provider_bindings(raw, SOURCE, l, control).map(|_| ())
            }
        };
        assert!(invoke(&Control::default(), exact).is_ok());
        for axis in 0..5 {
            let l = under(exact, axis);
            let baseline = Control::default();
            assert!(matches!(
                invoke(&baseline, l),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            let positive = trace(&baseline);
            for cause in CAUSES {
                let late = Control {
                    stop: Some((positive.len(), cause)),
                    ..Control::default()
                };
                assert!(matches!(
                    invoke(&late, l),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(trace(&late), positive);
            }
        }
    }
    assert!(matches!(
        encode_joint_provider_bindings(&inputs, 0, limits(), &c),
        Err(Error::InvalidShape(_))
    ));
    let mut raw = expected(0, "lake", "lake");
    raw.instance_id.reserve(8192);
    assert!(matches!(
        decode_joint_provider_bindings(
            &[raw],
            size_of::<wire::ProviderBindingDefinition>(),
            limits(),
            &c
        ),
        Err(Error::InvalidShape(_))
    ));
}

#[test]
fn joint_provider_small_success_and_ordinary_tails_keep_every_original_control_prefix() {
    let r = read("lake", "lake");
    let w = write("writer", "catalog");
    let inputs = [
        (u32::MAX, ProviderBindingSource::Write(&w)),
        (0, ProviderBindingSource::Read(&r)),
    ];
    all_prefixes(
        |c| encode_joint_provider_bindings(&inputs, SOURCE, limits(), c).map(|_| ()),
        false,
        CompilePhase::Encode,
    );
    let raw = [
        expected(u32::MAX, "writer", "catalog"),
        expected(0, "lake", "lake"),
    ];
    all_prefixes(
        |c| decode_joint_provider_bindings(&raw, SOURCE, limits(), c).map(|_| ()),
        false,
        CompilePhase::Decode,
    );
    let duplicate = [
        (0, ProviderBindingSource::Read(&r)),
        (0, ProviderBindingSource::Write(&w)),
    ];
    all_prefixes(
        |c| encode_joint_provider_bindings(&duplicate, SOURCE, limits(), c).map(|_| ()),
        true,
        CompilePhase::Encode,
    );
    let mut absent = expected(0, "lake", "lake");
    absent.catalog = None;
    all_prefixes(
        |c| {
            decode_joint_provider_bindings(std::slice::from_ref(&absent), SOURCE, limits(), c)
                .map(|_| ())
        },
        true,
        CompilePhase::Decode,
    );
    let invalid = expected(0, "UPPER", "lake");
    all_prefixes(
        |c| {
            decode_joint_provider_bindings(std::slice::from_ref(&invalid), SOURCE, limits(), c)
                .map(|_| ())
        },
        true,
        CompilePhase::Decode,
    );
    for n in [0, 31, 33] {
        let mut invalid = expected(0, "lake", "lake");
        invalid.catalog.as_mut().unwrap().version = vec![17; n];
        assert!(matches!(
            decode_joint_provider_bindings(&[invalid], SOURCE, limits(), &Control::default()),
            Err(Error::InvalidShape(_))
        ));
    }
}

#[test]
fn joint_provider_real_mixed_320_source_loops_observe_quantum_and_keep_ordered_views() {
    let r = read("read_lake", "catalog_r");
    let w = write("write_lake", "catalog_w");
    let inputs: Vec<_> = (0..320)
        .rev()
        .map(|id| {
            (
                if id == 319 { u32::MAX } else { id },
                if id.is_multiple_of(2) {
                    ProviderBindingSource::Read(&r)
                } else {
                    ProviderBindingSource::Write(&w)
                },
            )
        })
        .collect();
    let baseline = Control::default();
    let e = encode_joint_provider_bindings(&inputs, SOURCE, limits(), &baseline).unwrap();
    for (at, raw) in e.as_wire().iter().enumerate() {
        let id = 319 - at as u32;
        assert_eq!(
            *raw,
            expected(
                if id == 319 { u32::MAX } else { id },
                if id.is_multiple_of(2) {
                    "read_lake"
                } else {
                    "write_lake"
                },
                if id.is_multiple_of(2) {
                    "catalog_r"
                } else {
                    "catalog_w"
                }
            )
        );
    }
    let encode_trace = trace(&baseline);
    let baseline = Control::default();
    let d = decode_joint_provider_bindings(e.as_wire(), SOURCE, limits(), &baseline).unwrap();
    let decode_trace = trace(&baseline);
    assert_eq!(d.source_count(), 320);
    for direction in 0..2 {
        let positive = if direction == 0 {
            &encode_trace
        } else {
            &decode_trace
        };
        let quantum = positive
            .iter()
            .position(|(_, units)| *units == 256)
            .expect("actual owned-loop quantum");
        for at in [0, quantum, positive.len() - 1] {
            for cause in CAUSES {
                let c = Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                };
                let result = if direction == 0 {
                    encode_joint_provider_bindings(&inputs, SOURCE, limits(), &c).map(|_| ())
                } else {
                    decode_joint_provider_bindings(e.as_wire(), SOURCE, limits(), &c).map(|_| ())
                };
                assert!(matches!(result,Err(Error::Control(actual)) if actual==cause));
                assert_eq!(trace(&c), positive[..=at]);
            }
        }
    }
    assert!(e.binding(u32::MAX).unwrap().is_none());
    assert!(e.write_binding(u32::MAX).unwrap().is_some());
    assert!(d.binding(u32::MAX).unwrap().is_some());
    assert!(d.write_binding(u32::MAX).unwrap().is_some());
}

#[test]
fn joint_provider_sealed_lookup_and_retained_floor_use_original_controller() {
    let r = read("lake", "lake");
    let w = write("writer", "catalog");
    let inputs = [
        (0, ProviderBindingSource::Read(&r)),
        (u32::MAX, ProviderBindingSource::Write(&w)),
    ];
    let raw = [
        expected(0, "lake", "lake"),
        expected(u32::MAX, "writer", "catalog"),
    ];
    for direction in 0..2 {
        for operation in 0..5 {
            if direction == 1 && operation >= 3 {
                continue;
            }
            let invoke = |c: &Control| -> Result<usize, Error> {
                if direction == 0 {
                    let token = encode_joint_provider_bindings(&inputs, SOURCE, limits(), c)?;
                    let before = trace(c).len();
                    match operation {
                        0 => {
                            token.binding(0)?;
                        }
                        1 => {
                            token.write_binding(u32::MAX)?;
                        }
                        2 => {
                            token.source_id(&r)?;
                        }
                        3 => {
                            token.write_source_id(&w)?;
                        }
                        _ => {
                            token.retained_invoice_floor()?;
                        }
                    }
                    Ok(before)
                } else {
                    let token = decode_joint_provider_bindings(&raw, SOURCE, limits(), c)?;
                    let before = trace(c).len();
                    match operation {
                        0 => {
                            token.binding(0)?;
                        }
                        1 => {
                            token.write_binding(u32::MAX)?;
                        }
                        _ => {
                            token.retained_invoice_floor()?;
                        }
                    }
                    Ok(before)
                }
            };
            let baseline = Control::default();
            let before = invoke(&baseline).unwrap();
            let positive = trace(&baseline);
            for at in before..positive.len() {
                for cause in CAUSES {
                    let c = Control {
                        stop: Some((at, cause.clone())),
                        ..Control::default()
                    };
                    assert!(matches!(invoke(&c),Err(Error::Control(actual)) if actual==cause));
                    assert_eq!(trace(&c), positive[..=at]);
                }
            }
        }
    }
}

#[test]
fn joint_provider_known_facts_gate_precedes_pending_quantum_and_read_facade_trace_is_identical() {
    // This isolates the numerical author's known-facts boundary, not a forged
    // source namespace or a claim about callbacks inside standard libraries.
    let requests = Requests {
        observed: false,
        count: 10,
        bytes: 256,
    };
    let known = numerical_facts(2, 30, &requests, SOURCE).unwrap();
    for axis in 0..5 {
        for cause in CAUSES {
            let c = Control {
                stop: Some((1, cause)),
                ..Control::default()
            };
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let result = facts(
                2,
                30,
                Requests {
                    observed: false,
                    count: 10,
                    bytes: 256,
                },
                SOURCE,
                Some(under(exact(known), axis)),
                &mut None,
                &mut work,
            );
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(trace(&c), [(CompilePhase::Encode, 0)]);
        }
    }
    let c = Control::default();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
    for _ in 0..255 {
        work.step().unwrap();
    }
    assert_eq!(
        facts(
            2,
            30,
            Requests {
                observed: false,
                count: 10,
                bytes: 256
            },
            SOURCE,
            Some(exact(known)),
            &mut None,
            &mut work
        )
        .unwrap(),
        known
    );
    work.finish().unwrap();
    assert_eq!(
        trace(&c),
        [
            (CompilePhase::Encode, 0),
            (CompilePhase::Encode, 256),
            (CompilePhase::Encode, 3)
        ]
    );
    let r = read("lake", "lake");
    for ordinary in [false, true] {
        let read_inputs = if ordinary {
            vec![(0, &r), (0, &r)]
        } else {
            vec![(u32::MAX, &r), (0, &r)]
        };
        let joint_inputs: Vec<_> = read_inputs
            .iter()
            .map(|(id, r)| (*id, ProviderBindingSource::Read(r)))
            .collect();
        let old_control = Control::default();
        let joint_control = Control::default();
        let old = encode_provider_bindings(&read_inputs, SOURCE, limits(), &old_control);
        let joint = encode_joint_provider_bindings(&joint_inputs, SOURCE, limits(), &joint_control);
        assert_eq!(old.is_err(), ordinary);
        assert_eq!(joint.is_err(), ordinary);
        assert_eq!(trace(&old_control), trace(&joint_control));
        if !ordinary {
            assert_eq!(
                old.as_ref().unwrap().as_wire(),
                joint.as_ref().unwrap().as_wire()
            );
            assert_eq!(old.unwrap().facts(), joint.unwrap().facts());
        }
    }
    // The joint tuple's actual enum layout, including the role discriminant,
    // belongs to the source invoice even though output requests are identical.
    let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let shared = ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([17; 32])),
    );
    let inputs = [(0, ProviderBindingSource::Write(&shared))];
    let known = array_bytes::<(u32, ProviderBindingSource<'_>)>(1)
        + size_of::<ConnectorWriteBinding>()
        + arc_bytes(7)
        + arc_bytes(4);
    assert!(encode_joint_provider_bindings(&inputs, known, limits(), &Control::default()).is_ok());
    assert!(matches!(
        encode_joint_provider_bindings(&inputs, known - 1, limits(), &Control::default()),
        Err(Error::InvalidShape(_))
    ));
}
