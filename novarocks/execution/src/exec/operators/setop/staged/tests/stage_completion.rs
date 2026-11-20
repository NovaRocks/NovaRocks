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

use super::fixtures::{Fixture, turn};
use crate::exec::shared_stage::tail_ranges::TailClaim;
use arrow::{
    array::{Float64Array, Int32Array},
    datatypes::DataType,
};
use novarocks_local_program::SetOpKind;
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;

#[test]
fn parallel_build_and_probe_barriers_cover_every_input_and_tail_range() {
    let ty = FunctionValueType::new(DataType::Int32, true);
    let fixture = Fixture::new(SetOpKind::Intersect, &ty, 2);
    let work = fixture.work();
    let mut workers = fixture.workers();
    fixture.input(0, Arc::new(Int32Array::from_iter_values(0..1100)));
    for worker in &mut workers {
        worker.step(&fixture.frame, &mut turn(), &work).unwrap();
    }
    assert!(fixture.frame.completion.published().unwrap().is_none());
    fixture.input(1, Arc::new(Int32Array::from_iter_values(600..1800)));
    fixture.finalize(&mut workers);
    assert!(
        workers
            .iter()
            .filter(|worker| worker.worked_on_keys())
            .count()
            > 1
    );

    // The second logical probe input may complete while the first has yet to run.
    fixture.probe(1, 3, Arc::new(Int32Array::from_iter_values(0..900)));
    fixture.probe(1, 2, Arc::new(Int32Array::from_iter_values(900..1800)));
    fixture.probe(0, 0, Arc::new(Int32Array::from_iter_values(0..900)));
    assert!(matches!(
        fixture.frame.claim_tail(&fixture.work()).unwrap(),
        TailClaim::Pending
    ));
    fixture.probe(0, 1, Arc::new(Int32Array::from_iter_values(900..1800)));

    // Multiple range obligations may be live together; completing one cannot
    // duplicate or consume another worker's exact range.
    let [mut a, mut b, mut c] = std::array::from_fn(|_| {
        let TailClaim::Range(range) = fixture.frame.claim_tail(&fixture.work()).unwrap() else {
            panic!("expected installed tail range");
        };
        range
    });
    assert_eq!(a.rows().start, 0);
    assert_eq!(a.rows().end, b.rows().start);
    assert_eq!(b.rows().end, c.rows().start);
    assert_eq!(a.artifact().keys.len(), 1800);
    assert!((0..1800).all(|ordinal| a.artifact().survives(ordinal, work.control).unwrap()));
    for range in [&mut b, &mut c, &mut a] {
        range.complete_after_output().unwrap();
    }
    let TailClaim::Range(mut remainder) = fixture.frame.claim_tail(&fixture.work()).unwrap() else {
        panic!("remaining range");
    };
    assert_eq!(remainder.rows(), c.rows().end..1800);
    remainder.complete_after_output().unwrap();
    assert!(matches!(
        fixture.frame.claim_tail(&fixture.work()).unwrap(),
        TailClaim::Finished
    ));
}

#[test]
fn nullable_float_membership_uses_grouping_equivalence() {
    let ty = FunctionValueType::new(DataType::Float64, true);
    let fixture = Fixture::new(SetOpKind::Intersect, &ty, 2);
    fixture.input(
        0,
        Arc::new(Float64Array::from(vec![
            None,
            Some(f64::NAN),
            Some(0.),
            Some(-0.),
        ])),
    );
    fixture.input(
        1,
        Arc::new(Float64Array::from(vec![Some(-f64::NAN), None, Some(4.)])),
    );
    fixture.finalize(&mut fixture.workers());
    fixture.probe(
        1,
        2,
        Arc::new(Float64Array::from(vec![None, Some(0.), Some(f64::NAN)])),
    );
    fixture.probe(1, 3, Arc::new(Float64Array::from(Vec::<f64>::new())));
    fixture.probe(
        0,
        0,
        Arc::new(Float64Array::from(vec![Some(-0.), Some(-f64::NAN), None])),
    );
    fixture.probe(0, 1, Arc::new(Float64Array::from(vec![None, None])));
    let TailClaim::Range(mut range) = fixture.frame.claim_tail(&fixture.work()).unwrap() else {
        panic!("expected surviving keys");
    };
    assert_eq!(range.rows(), 0..4);
    let work = fixture.work();
    let retained = range
        .rows()
        .filter(|&ordinal| range.artifact().survives(ordinal, work.control).unwrap())
        .count();
    assert_eq!(retained, 3);
    range.complete_after_output().unwrap();
}

#[test]
fn changing_dictionary_codes_and_bottom_nulls_preserve_owned_view_keys() {
    use arrow::{
        array::{DictionaryArray, StringArray, StringViewArray},
        datatypes::Int32Type,
    };
    let ty = FunctionValueType::new(DataType::Utf8View, true);
    let fixture = Fixture::new(SetOpKind::Except, &ty, 1);
    fixture.input(
        0,
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(1), Some(0), Some(1), None, Some(2)]),
                Arc::new(StringArray::from(vec![Some("beta"), Some("alpha"), None])),
            )
            .unwrap(),
        ),
    );
    fixture.input(
        1,
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![1, 0]),
                Arc::new(StringArray::from(vec!["alpha", "gamma"])),
            )
            .unwrap(),
        ),
    );
    fixture.finalize(&mut fixture.workers());
    fixture.probe(
        0,
        0,
        Arc::new(StringViewArray::from(vec![
            Some("beta"),
            None,
            Some("absent"),
        ])),
    );
    fixture.probe(
        0,
        1,
        Arc::new(StringViewArray::from(Vec::<Option<&str>>::new())),
    );
    let TailClaim::Range(mut range) = fixture.frame.claim_tail(&fixture.work()).unwrap() else {
        panic!("expected class tail");
    };
    let work = fixture.work();
    let artifact = range.artifact();
    let mut results = Vec::new();
    for partition in 0..artifact.keys.view.partition_count() {
        let count = artifact.keys.view.key_count(partition).unwrap();
        if count == 0 {
            continue;
        }
        let base = (0..partition)
            .map(|previous| artifact.keys.view.key_count(previous).unwrap())
            .sum::<usize>();
        let output = artifact
            .keys
            .view
            .export_partition_range(
                partition,
                0..count,
                |id| artifact.survives(base + id, work.control),
                std::slice::from_ref(&ty),
                &work,
            )
            .unwrap();
        assert_eq!(output.bindings(), std::slice::from_ref(&ty));
        let array = output.arrays()[0]
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        results.extend(
            array
                .iter()
                .map(|key| key.expect("remaining keys are non-null").to_owned()),
        );
    }
    results.sort();
    assert_eq!(results, ["alpha", "gamma"]);
    range.complete_after_output().unwrap();
}
