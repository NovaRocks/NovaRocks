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
use std::sync::Mutex;
const SOURCE: usize = 128 * 1024;
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
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> PhysicalPropertyProjectionLimits {
    PhysicalPropertyProjectionLimits {
        max_value_references: 2048,
        max_allocation_requests: 8,
        max_allocation_request_bytes: 128 * 1024,
        max_coexisting_source_and_request_bytes: 512 * 1024,
        max_work: 4 * 1024 * 1024,
    }
}
fn hash(algorithm: PartitionHashAlgorithm) -> physical::Distribution {
    physical::Distribution::Hash {
        keys: vec![
            physical::ValueId::new(u32::MAX),
            physical::ValueId::new(0),
            physical::ValueId::new(u32::MAX),
        ]
        .into_boxed_slice(),
        scheme: physical::HashPartitionScheme {
            space: PartitionSpaceId::try_new([7; 32]).unwrap(),
            count: physical::PartitionCountParameter {
                id: PartitionCountParameterId::try_new([8; 32]).unwrap(),
                admissible: physical::PartitionCountDomain {
                    min: 2,
                    max: 8,
                    requires_power_of_two: true,
                },
            },
            definition: physical::HashDefinition { algorithm },
        },
    }
}
fn bucket(algorithm: PartitionHashAlgorithm) -> physical::Distribution {
    physical::Distribution::BucketShuffle {
        keys: vec![
            physical::ValueId::new(0),
            physical::ValueId::new(u32::MAX),
            physical::ValueId::new(0),
        ]
        .into_boxed_slice(),
        scheme: physical::BucketPartitionScheme {
            space: PartitionSpaceId::try_new([9; 32]).unwrap(),
            bucket_count: 3,
            hash: algorithm,
            layout: BucketLayoutAlgorithm::DenseZeroBasedV1,
            ordinal_domain: physical::BucketOrdinalDomainProof {
                first_ordinal: 0,
                ordinal_count: 3,
                evidence_digest: [10; 32],
            },
        },
    }
}
fn expected_hash(algorithm: i32) -> wire::Distribution {
    wire::Distribution {
        kind: Some(wire::distribution::Kind::Hash(wire::HashDistribution {
            key_value_ids: vec![u32::MAX, 0, u32::MAX],
            scheme: Some(wire::HashPartitionScheme {
                partition_space: vec![7; 32],
                count: Some(wire::PartitionCountParameter {
                    id: vec![8; 32],
                    admissible: Some(wire::PartitionCountDomain {
                        min: 2,
                        max: 8,
                        requires_power_of_two: true,
                    }),
                }),
                algorithm,
            }),
        })),
    }
}
fn expected_bucket(algorithm: i32) -> wire::Distribution {
    wire::Distribution {
        kind: Some(wire::distribution::Kind::BucketShuffle(
            wire::BucketDistribution {
                key_value_ids: vec![0, u32::MAX, 0],
                scheme: Some(wire::BucketPartitionScheme {
                    partition_space: vec![9; 32],
                    bucket_count: 3,
                    hash: algorithm,
                    layout: 1,
                    ordinal_domain: Some(wire::BucketOrdinalDomainProof {
                        first_ordinal: 0,
                        ordinal_count: 3,
                        evidence_digest: vec![10; 32],
                    }),
                }),
            },
        )),
    }
}
fn hash_wire(value: &mut wire::Distribution) -> &mut wire::HashDistribution {
    let Some(wire::distribution::Kind::Hash(hash)) = &mut value.kind else {
        panic!("hash fixture")
    };
    hash
}
fn bucket_wire(value: &mut wire::Distribution) -> &mut wire::BucketDistribution {
    let Some(wire::distribution::Kind::BucketShuffle(bucket)) = &mut value.kind else {
        panic!("bucket fixture")
    };
    bucket
}
fn exact(f: PhysicalPropertyProjectionFacts) -> PhysicalPropertyProjectionLimits {
    PhysicalPropertyProjectionLimits {
        max_value_references: f.value_reference_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    }
}
fn lower(l: &mut PhysicalPropertyProjectionLimits, axis: usize) {
    match axis {
        0 => l.max_value_references -= 1,
        1 => l.max_allocation_requests -= 1,
        2 => l.max_allocation_request_bytes -= 1,
        3 => l.max_coexisting_source_and_request_bytes -= 1,
        4 => l.max_work -= 1,
        _ => unreachable!(),
    }
}
fn prefixes<T>(call: impl Fn(&Control) -> Result<T, Error>, good: bool, phase: CompilePhase) {
    let base = Control::default();
    assert_eq!(call(&base).is_ok(), good);
    let trace = base.trace.lock().unwrap().clone();
    assert!(trace.len() >= 2);
    assert_eq!(trace[0], (phase, 0));
    assert!(trace.iter().all(|(p, _)| *p == phase));
    for stop in 0..trace.len() {
        for cause in CAUSES {
            let ctrl = Control {
                trace: Mutex::new(vec![]),
                stop: Some((stop, cause.clone())),
            };
            assert!(matches!(call(&ctrl),Err(Error::Control(actual)) if actual==cause));
            assert_eq!(*ctrl.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn distribution_six_variants_and_both_hash_algorithms_have_independent_wire_oracles() {
    let cases = [
        (
            physical::Distribution::Unconstrained,
            wire::Distribution {
                kind: Some(wire::distribution::Kind::Unconstrained(Empty {})),
            },
        ),
        (
            physical::Distribution::Singleton,
            wire::Distribution {
                kind: Some(wire::distribution::Kind::Singleton(Empty {})),
            },
        ),
        (
            physical::Distribution::RoundRobin,
            wire::Distribution {
                kind: Some(wire::distribution::Kind::RoundRobin(Empty {})),
            },
        ),
        (
            physical::Distribution::Broadcast,
            wire::Distribution {
                kind: Some(wire::distribution::Kind::Broadcast(Empty {})),
            },
        ),
        (
            hash(PartitionHashAlgorithm::NativeExchangeV1),
            expected_hash(1),
        ),
        (
            hash(PartitionHashAlgorithm::NativeExchangeV2),
            expected_hash(4),
        ),
        (
            hash(PartitionHashAlgorithm::NativeBucketCrc32V1),
            expected_hash(2),
        ),
        (
            bucket(PartitionHashAlgorithm::NativeExchangeV1),
            expected_bucket(1),
        ),
        (
            bucket(PartitionHashAlgorithm::NativeBucketCrc32V1),
            expected_bucket(2),
        ),
    ];
    for (source, wire) in cases {
        assert_eq!(
            encode_distribution(&source, SOURCE, limits(), &Control::default())
                .unwrap()
                .0,
            wire
        );
        assert_eq!(
            decode_distribution(&wire, SOURCE, limits(), &Control::default())
                .unwrap()
                .0,
            source
        );
    }
}

#[test]
fn distribution_raw_domains_empty_keys_and_sparse_max_id_preserve_representation() {
    let mut source = hash(PartitionHashAlgorithm::NativeExchangeV1);
    let mut wire = expected_hash(1);
    let physical::Distribution::Hash { keys, scheme } = &mut source else {
        panic!()
    };
    *keys = Box::from([]);
    scheme.count.admissible = physical::PartitionCountDomain {
        min: u32::MAX,
        max: 0,
        requires_power_of_two: false,
    };
    let raw = hash_wire(&mut wire);
    raw.key_value_ids.clear();
    raw.scheme
        .as_mut()
        .unwrap()
        .count
        .as_mut()
        .unwrap()
        .admissible = Some(wire::PartitionCountDomain {
        min: u32::MAX,
        max: 0,
        requires_power_of_two: false,
    });
    assert_eq!(
        encode_distribution(&source, SOURCE, limits(), &Control::default())
            .unwrap()
            .0,
        wire
    );
    assert_eq!(
        decode_distribution(&wire, SOURCE, limits(), &Control::default())
            .unwrap()
            .0,
        source
    );
    let mut source = bucket(PartitionHashAlgorithm::NativeBucketCrc32V1);
    let mut wire = expected_bucket(2);
    let physical::Distribution::BucketShuffle { scheme, .. } = &mut source else {
        panic!()
    };
    scheme.bucket_count = 0;
    scheme.ordinal_domain = physical::BucketOrdinalDomainProof {
        first_ordinal: u32::MAX,
        ordinal_count: 0,
        evidence_digest: [0; 32],
    };
    let raw = bucket_wire(&mut wire).scheme.as_mut().unwrap();
    raw.bucket_count = 0;
    raw.ordinal_domain = Some(wire::BucketOrdinalDomainProof {
        first_ordinal: u32::MAX,
        ordinal_count: 0,
        evidence_digest: vec![0; 32],
    });
    assert_eq!(
        encode_distribution(&source, SOURCE, limits(), &Control::default())
            .unwrap()
            .0,
        wire
    );
    assert_eq!(
        decode_distribution(&wire, SOURCE, limits(), &Control::default())
            .unwrap()
            .0,
        source
    );
    // Raw projection is not a proof of count/key/bucket semantic validity.
    let mut only_max = hash(PartitionHashAlgorithm::NativeExchangeV1);
    let physical::Distribution::Hash { keys, .. } = &mut only_max else {
        panic!()
    };
    *keys = Box::from([physical::ValueId::new(u32::MAX)]);
    let f = encode_distribution(&only_max, SOURCE, limits(), &Control::default())
        .unwrap()
        .1;
    assert_eq!(f.value_reference_count, 1);
    assert_eq!(
        f.allocation_request_bytes_upper_bound,
        Layout::array::<u32>(1).unwrap().size() + 64
    );
}

#[test]
fn distribution_closed_headers_and_exact_identities_reject_before_materialization() {
    for bad in 0..18 {
        let mut raw = if bad < 10 {
            expected_hash(1)
        } else {
            expected_bucket(2)
        };
        match bad {
            0 => raw.kind = None,
            1 => hash_wire(&mut raw).scheme = None,
            2 => hash_wire(&mut raw).scheme.as_mut().unwrap().count = None,
            3 => {
                hash_wire(&mut raw)
                    .scheme
                    .as_mut()
                    .unwrap()
                    .count
                    .as_mut()
                    .unwrap()
                    .admissible = None
            }
            4 => hash_wire(&mut raw).scheme.as_mut().unwrap().partition_space = vec![0; 32],
            5 => hash_wire(&mut raw).scheme.as_mut().unwrap().partition_space = vec![1; 31],
            6 => {
                hash_wire(&mut raw)
                    .scheme
                    .as_mut()
                    .unwrap()
                    .count
                    .as_mut()
                    .unwrap()
                    .id = vec![0; 32]
            }
            7 => {
                hash_wire(&mut raw)
                    .scheme
                    .as_mut()
                    .unwrap()
                    .count
                    .as_mut()
                    .unwrap()
                    .id = vec![1; 33]
            }
            8 => hash_wire(&mut raw).scheme.as_mut().unwrap().algorithm = 0,
            9 => hash_wire(&mut raw).scheme.as_mut().unwrap().algorithm = 99,
            10 => bucket_wire(&mut raw).scheme = None,
            11 => {
                bucket_wire(&mut raw)
                    .scheme
                    .as_mut()
                    .unwrap()
                    .ordinal_domain = None
            }
            12 => bucket_wire(&mut raw).scheme.as_mut().unwrap().layout = 0,
            13 => bucket_wire(&mut raw).scheme.as_mut().unwrap().layout = 99,
            14 => bucket_wire(&mut raw).scheme.as_mut().unwrap().hash = 0,
            15 => bucket_wire(&mut raw).scheme.as_mut().unwrap().hash = 99,
            16 => {
                bucket_wire(&mut raw)
                    .scheme
                    .as_mut()
                    .unwrap()
                    .partition_space = vec![0; 32]
            }
            _ => {
                bucket_wire(&mut raw)
                    .scheme
                    .as_mut()
                    .unwrap()
                    .ordinal_domain
                    .as_mut()
                    .unwrap()
                    .evidence_digest = vec![1; 33]
            }
        }
        assert!(
            matches!(
                decode_distribution(&raw, SOURCE, limits(), &Control::default()),
                Err(Error::InvalidShape(_))
            ),
            "bad {bad}"
        );
    }
}

#[test]
fn distribution_independent_layouts_and_all_five_exact_under_limits() {
    for (source, wire) in [
        (
            hash(PartitionHashAlgorithm::NativeExchangeV1),
            expected_hash(1),
        ),
        (
            bucket(PartitionHashAlgorithm::NativeBucketCrc32V1),
            expected_bucket(2),
        ),
    ] {
        let ef = encode_distribution(&source, SOURCE, limits(), &Control::default())
            .unwrap()
            .1;
        let df = decode_distribution(&wire, SOURCE, limits(), &Control::default())
            .unwrap()
            .1;
        // Encoder: one u32 key Vec plus two independently requested 32-byte
        // buffers. Decoder: original keys copied then Vec-to-Box shrink.
        let ebytes =
            Layout::array::<u32>(3).unwrap().size() + 2 * Layout::array::<u8>(32).unwrap().size();
        let dbytes = 2 * Layout::array::<physical::ValueId>(3).unwrap().size();
        for (facts, requests, bytes) in [(ef, 3, ebytes), (df, 2, dbytes)] {
            assert_eq!(facts.value_reference_count, 3);
            assert_eq!(facts.allocation_requests_upper_bound, requests);
            assert_eq!(facts.allocation_request_bytes_upper_bound, bytes);
            assert_eq!(
                facts.coexisting_source_and_request_bytes_upper_bound,
                SOURCE + bytes
            );
            assert_eq!(facts.cumulative_work_upper_bound, 128 + 3 * 32 + 4 * bytes);
        }
        assert!(encode_distribution(&source, SOURCE, exact(ef), &Control::default()).is_ok());
        assert!(decode_distribution(&wire, SOURCE, exact(df), &Control::default()).is_ok());
        for axis in 0..5 {
            let mut l = exact(ef);
            lower(&mut l, axis);
            assert!(matches!(
                encode_distribution(&source, SOURCE, l, &Control::default()),
                Err(Error::InvalidShape(_))
            ));
            let mut l = exact(df);
            lower(&mut l, axis);
            assert!(matches!(
                decode_distribution(&wire, SOURCE, l, &Control::default()),
                Err(Error::InvalidShape(_))
            ));
        }
    }
    for source in [
        physical::Distribution::Unconstrained,
        physical::Distribution::Singleton,
        physical::Distribution::RoundRobin,
        physical::Distribution::Broadcast,
    ] {
        let (wire, f) =
            encode_distribution(&source, SOURCE, limits(), &Control::default()).unwrap();
        assert_eq!(f.value_reference_count, 0);
        assert_eq!(f.allocation_requests_upper_bound, 0);
        assert_eq!(f.allocation_request_bytes_upper_bound, 0);
        assert_eq!(f.cumulative_work_upper_bound, 128);
        assert!(encode_distribution(&source, SOURCE, exact(f), &Control::default()).is_ok());
        let df = decode_distribution(&wire, SOURCE, limits(), &Control::default())
            .unwrap()
            .1;
        assert_eq!(df, f);
        assert!(decode_distribution(&wire, SOURCE, exact(df), &Control::default()).is_ok());
    }
}

#[test]
fn distribution_inline_and_original_capacity_floors_are_exact_and_not_visible_lengths() {
    let source = hash(PartitionHashAlgorithm::NativeExchangeV1);
    let known =
        size_of::<physical::Distribution>() + Layout::array::<physical::ValueId>(3).unwrap().size();
    assert!(encode_distribution(&source, known, limits(), &Control::default()).is_ok());
    assert!(matches!(
        encode_distribution(&source, known - 1, limits(), &Control::default()),
        Err(Error::InvalidShape(_))
    ));
    let mut raw = expected_hash(1);
    let h = hash_wire(&mut raw);
    h.key_value_ids.reserve_exact(512);
    h.scheme
        .as_mut()
        .unwrap()
        .partition_space
        .reserve_exact(1024);
    h.scheme
        .as_mut()
        .unwrap()
        .count
        .as_mut()
        .unwrap()
        .id
        .reserve_exact(2048);
    let h = hash_wire(&mut raw);
    let s = h.scheme.as_ref().unwrap();
    let actual = size_of::<wire::Distribution>()
        + Layout::array::<u32>(h.key_value_ids.capacity())
            .unwrap()
            .size()
        + s.partition_space.capacity()
        + s.count.as_ref().unwrap().id.capacity();
    let visible = size_of::<wire::Distribution>() + 3 * size_of::<u32>() + 64;
    assert!(matches!(
        decode_distribution(&raw, visible, limits(), &Control::default()),
        Err(Error::InvalidShape(_))
    ));
    assert!(decode_distribution(&raw, actual, limits(), &Control::default()).is_ok());
    assert!(matches!(
        decode_distribution(&raw, actual - 1, limits(), &Control::default()),
        Err(Error::InvalidShape(_))
    ));
    for source in [
        physical::Distribution::Unconstrained,
        physical::Distribution::Singleton,
    ] {
        assert!(
            encode_distribution(
                &source,
                size_of::<physical::Distribution>(),
                limits(),
                &Control::default()
            )
            .is_ok()
        );
        assert!(matches!(
            encode_distribution(&source, 0, limits(), &Control::default()),
            Err(Error::InvalidShape(_))
        ));
    }
}

#[test]
fn distribution_every_small_callback_three_causes_and_ordinary_tails() {
    for source in [
        physical::Distribution::Unconstrained,
        hash(PartitionHashAlgorithm::NativeExchangeV1),
        bucket(PartitionHashAlgorithm::NativeBucketCrc32V1),
    ] {
        prefixes(
            |c| encode_distribution(&source, SOURCE, limits(), c),
            true,
            CompilePhase::Encode,
        );
    }
    for raw in [
        wire::Distribution {
            kind: Some(wire::distribution::Kind::Singleton(Empty {})),
        },
        expected_hash(1),
        expected_bucket(2),
    ] {
        prefixes(
            |c| decode_distribution(&raw, SOURCE, limits(), c),
            true,
            CompilePhase::Decode,
        );
    }
    let mut bad = expected_hash(1);
    hash_wire(&mut bad).scheme.as_mut().unwrap().algorithm = 99;
    let base = Control::default();
    assert!(matches!(
        decode_distribution(&bad, SOURCE, limits(), &base),
        Err(Error::InvalidShape(_))
    ));
    assert!(
        base.trace
            .lock()
            .unwrap()
            .last()
            .is_some_and(|(_, u)| *u > 0),
        "ordinary completed header comparisons must finish"
    );
    prefixes(
        |c| decode_distribution(&bad, SOURCE, limits(), c),
        false,
        CompilePhase::Decode,
    );
    prefixes(
        |c| decode_distribution(&wire::Distribution { kind: None }, SOURCE, limits(), c),
        false,
        CompilePhase::Decode,
    );
    let source = hash(PartitionHashAlgorithm::NativeExchangeV1);
    let mut low = limits();
    low.max_value_references = 2;
    prefixes(
        |c| encode_distribution(&source, SOURCE, low, c),
        false,
        CompilePhase::Encode,
    );
}

#[test]
fn distribution_real_wide_key_copy_quantum_preserves_original_prefix() {
    let mut source = hash(PartitionHashAlgorithm::NativeExchangeV1);
    let physical::Distribution::Hash { keys, .. } = &mut source else {
        panic!()
    };
    *keys = (0..320)
        .map(|n| physical::ValueId::new(if n % 2 == 0 { 0 } else { u32::MAX }))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let mut raw = expected_hash(1);
    hash_wire(&mut raw).key_value_ids = (0..320)
        .map(|n| if n % 2 == 0 { 0 } else { u32::MAX })
        .collect();
    for encode in [true, false] {
        let call = |c: &Control| {
            if encode {
                encode_distribution(&source, SOURCE, limits(), c).map(|(_, f)| f)
            } else {
                decode_distribution(&raw, SOURCE, limits(), c).map(|(_, f)| f)
            }
        };
        let base = Control::default();
        let facts = call(&base).unwrap();
        assert_eq!(facts.value_reference_count, 320);
        let trace = base.trace.lock().unwrap().clone();
        let quantum = trace
            .iter()
            .position(|(_, u)| *u == 256)
            .expect("actual key copy quantum");
        for stop in [0, quantum, trace.len() - 1] {
            for cause in CAUSES {
                let ctrl = Control {
                    trace: Mutex::new(vec![]),
                    stop: Some((stop, cause.clone())),
                };
                assert!(matches!(call(&ctrl),Err(Error::Control(actual)) if actual==cause));
                assert_eq!(*ctrl.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn conditional_key_roundtrip_accounts_for_mask_and_refuses_missing_mask() {
    let physical::Distribution::Hash { keys, scheme } =
        hash(PartitionHashAlgorithm::NativeHierarchicalV1)
    else {
        panic!()
    };
    let original = physical::Distribution::ConditionalKey {
        applies: physical::ValueId::new(17),
        keys,
        scheme,
    };
    let (encoded, encode_facts) =
        encode_distribution(&original, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(encode_facts.value_reference_count, 4);
    let (decoded, decode_facts) =
        decode_distribution(&encoded, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(decoded, original);
    assert_eq!(decode_facts.value_reference_count, 4);
    let mut limit = limits();
    limit.max_value_references = 3;
    assert!(encode_distribution(&original, SOURCE, limit, &Control::default()).is_err());
    assert!(decode_distribution(&encoded, SOURCE, limit, &Control::default()).is_err());
    let Some(wire::distribution::Kind::ConditionalKey(mut conditional)) = encoded.kind else {
        panic!()
    };
    assert_eq!(
        conditional.scheme.as_ref().unwrap().algorithm,
        wire::PartitionHashAlgorithm::NativeHierarchicalV1 as i32
    );
    conditional.applies_value_id = None;
    let missing = wire::Distribution {
        kind: Some(wire::distribution::Kind::ConditionalKey(conditional)),
    };
    assert!(decode_distribution(&missing, SOURCE, limits(), &Control::default()).is_err());
}
