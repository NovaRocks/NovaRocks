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

use super::super::super::{
    strategy::{StrategyKind, StrategyReason, choose_layout},
    table::integer_hash_costs,
};
use super::*;
use std::num::NonZeroUsize;

fn complete(
    values: Vec<Option<i64>>,
    nullable: bool,
    nulls: NullPolicy,
    purpose: IndexPurpose,
    ports: &CaptureWork<'_>,
) -> CompleteInput {
    let domain = setup(ports, nulls);
    let mut builder = domain.begin_capture(ports).unwrap();
    let occurrences: Vec<_> = (0..values.len()).collect();
    page(&mut builder, values, nullable, &occurrences, ports);
    let input = BulkInput::new(&domain, vec![builder.seal(ports).unwrap()], ports).unwrap();
    CompleteInput::from_captures(input, purpose, ports).unwrap()
}

#[test]
fn dense_and_negative_integer_domains_select_one_shared_direct_index_for_both_purposes() {
    let (owner, tracker) = owner_with_tracker();
    let ports = work(owner.as_ref(), &tracker, &CopyControl);
    for purpose in [IndexPurpose::KeyIds, IndexPurpose::PresenceOnly] {
        for (min, max) in [(1, 4096), (-512, 512)] {
            let input = complete(
                (min..=max).map(Some).collect(),
                false,
                NullPolicy::Equal,
                purpose,
                &ports,
            );
            let before = tracker.current();
            let decision = choose_layout(
                input.stats(),
                Some(input.stats().hint()),
                NonZeroUsize::new(3).unwrap(),
                &|| Ok(()),
            )
            .unwrap();
            assert_eq!(
                tracker.current(),
                before,
                "cost observation must not acquire backing"
            );
            let StrategyKind::Direct(geometry) = decision.kind else {
                panic!("dense integer cost must select the real direct representation")
            };
            assert_eq!(geometry.purpose(), purpose);
            assert_eq!(geometry.slots(), (max - min + 1) as usize);
            assert!(geometry.shard_count() >= 2); // W tasks, still P=1.
            assert_eq!(decision.reason, StrategyReason::DirectKnownStructureCheaper);
            let comparison = decision.comparison.unwrap();
            assert_eq!(comparison.hash.purpose, purpose);
            assert!(comparison.direct_modeled_bytes < comparison.hash_modeled_bytes);
            assert!(comparison.has_unmodeled_terms);
            assert!(comparison.hash.has_unmodeled_custody_and_capacity);
            assert!(comparison.hash.directory.unmodeled_trailer_and_alignment);
            assert!(
                comparison.hash.representative_payload_estimate > 0,
                "hash membership still requires exact representatives"
            );
            assert_eq!(comparison.hash_batches_modeled, 1);
            if purpose == IndexPurpose::PresenceOnly {
                assert_eq!(comparison.direct.slot_id_bytes, 0);
                assert_eq!(comparison.direct.count_prefix_peak_bytes, 0);
                assert_eq!(comparison.direct.representative_payload_estimate, 0);
            }
        }
    }
}

#[test]
fn sparse_and_unrepresentable_direct_ranges_select_hash_without_refusing_legal_values() {
    let (owner, tracker) = owner_with_tracker();
    let ports = work(owner.as_ref(), &tracker, &CopyControl);
    for (values, expected) in [
        (
            vec![Some(-(1i64 << 39)), Some(1i64 << 39)],
            StrategyReason::HashKnownStructureCheaper,
        ),
        (
            vec![Some(i64::MIN), Some(i64::MAX)],
            StrategyReason::DirectLayoutUnrepresentable,
        ),
    ] {
        let input = complete(
            values,
            false,
            NullPolicy::Equal,
            IndexPurpose::KeyIds,
            &ports,
        );
        let before = tracker.current();
        let decision = choose_layout(
            input.stats(),
            Some(input.stats().hint()),
            NonZeroUsize::new(4).unwrap(),
            &|| Ok(()),
        )
        .unwrap();
        assert_eq!(tracker.current(), before);
        let StrategyKind::Hash(layout) = decision.kind else {
            panic!("sparse/large layouts must keep their normal hash representation")
        };
        assert_eq!(decision.reason, expected);
        assert_eq!(layout.closed_input_bound(), Some(2));
        assert!(layout.partitions() > 1);
    }
}

#[test]
fn missing_hint_uses_hash_and_foreign_qualified_hint_is_rejected() {
    let (owner, tracker) = owner_with_tracker();
    let ports = work(owner.as_ref(), &tracker, &CopyControl);
    let input = complete(
        (1..=1024).map(Some).collect(),
        false,
        NullPolicy::Equal,
        IndexPurpose::KeyIds,
        &ports,
    );
    let decision = choose_layout(input.stats(), None, NonZeroUsize::new(4).unwrap(), &|| {
        Ok(())
    })
    .unwrap();
    assert!(matches!(decision.kind, StrategyKind::Hash(_)));
    assert_eq!(decision.reason, StrategyReason::MissingNdvHint);
    assert!(decision.comparison.is_none());
    let foreign = complete(
        (1..=1024).map(Some).collect(),
        false,
        NullPolicy::Equal,
        IndexPurpose::KeyIds,
        &ports,
    );
    assert_value_failure(
        &error(choose_layout(
            input.stats(),
            Some(foreign.stats().hint()),
            NonZeroUsize::new(4).unwrap(),
            &|| Ok(()),
        )),
        ValueViolation::Layout,
    );
}

#[test]
fn exact_empty_indexable_input_needs_no_hint_or_value_domain_initialization() {
    let (owner, tracker) = owner_with_tracker();
    let ports = work(owner.as_ref(), &tracker, &CopyControl);
    for purpose in [IndexPurpose::KeyIds, IndexPurpose::PresenceOnly] {
        let input = complete(
            vec![None; 32],
            true,
            NullPolicy::Unmatchable,
            purpose,
            &ports,
        );
        assert_eq!(input.stats().exact().input_rows(), 32);
        let decision = choose_layout(input.stats(), None, NonZeroUsize::new(8).unwrap(), &|| {
            Ok(())
        })
        .unwrap();
        assert!(matches!(decision.kind, StrategyKind::Empty));
        assert_eq!(decision.reason, StrategyReason::ExactEmptyIndexableDomain);
        assert!(decision.comparison.is_none());
    }
}

#[test]
fn hash_storage_cost_preserves_exact_key_state_and_explicit_fragmentation_model() {
    let (owner, tracker) = owner_with_tracker();
    let ports = work(owner.as_ref(), &tracker, &CopyControl);
    let mut prior = None;
    for purpose in [IndexPurpose::KeyIds, IndexPurpose::PresenceOnly] {
        let input = complete(
            (0..64).map(Some).collect(),
            true,
            NullPolicy::Equal,
            purpose,
            &ports,
        );
        let model = integer_hash_costs(input.stats()).unwrap().unwrap();
        assert_eq!(
            model.reserve_classes_estimate,
            input.stats().hint().initial_reservation()
        );
        assert!(model.reserve_classes_estimate <= model.exact_eligible);
        assert!(model.class_live_bytes_estimate > 0);
        assert!(model.representative_payload_estimate > 0);
        assert_eq!(
            model.representative_payload_estimate,
            8 * model.reserve_classes_estimate as u128
        );
        let low = model.modeled_bytes(*model.batch_range().start()).unwrap();
        let high = model.modeled_bytes(*model.batch_range().end()).unwrap();
        assert!(high > low);
        assert!(model.modeled_bytes(0).is_none());
        if let Some((classes, payload)) = prior {
            assert_eq!(model.class_live_bytes_estimate, classes);
            assert_eq!(model.representative_payload_estimate, payload);
        }
        prior = Some((
            model.class_live_bytes_estimate,
            model.representative_payload_estimate,
        ));
    }
}
