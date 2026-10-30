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

//! Real capture/page fixtures. They never manufacture or publish a Ready.

use super::super::{
    build_capture::{EvaluatedJoinKeyPage, EvaluatedKeyProjection, InputRunWriter},
    build_collection::{BeginClaim, BeginLease, BuildCollection},
    join_completion::{
        JoinCompletion, JoinParticipants,
        tests::{Control, fixture},
    },
};
use super::*;
use crate::{
    exec::hash_table::{
        allocation::AggregateAllocator,
        key_index::{KeySpec, NullPolicy},
    },
    runtime::{
        mem_tracker::MemTracker, observable::Observable,
        tracked_state_allocator::expression_allocation_host,
    },
};
use arrow::{
    array::{Array, ArrayRef, Int32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use std::num::NonZeroUsize;

pub(crate) enum NoArtifact {}
pub(crate) fn work<'a>(
    owner: &'a dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
    control: &'a dyn novarocks_functions::KernelEvaluationControl,
) -> BulkWork<'a> {
    BulkWork {
        owner,
        allocator: AggregateAllocator::new(Arc::clone(tracker)),
        host: expression_allocation_host(Arc::clone(tracker)),
        control,
    }
}
pub(crate) fn ordinary(
    ty: &FunctionValueType,
    nulls: NullPolicy,
    purpose: IndexPurpose,
    work: &BulkWork<'_>,
) -> ProjectionDomains {
    let spec = KeySpec::new(std::slice::from_ref(ty), &[nulls], work.owner, &|| {
        check(work)
    })
    .unwrap();
    ProjectionDomains::Ordinary {
        keys: BulkDomain::new(&spec, work.owner, work.control).unwrap(),
        purpose,
    }
}
pub(crate) fn naa(
    types: &[FunctionValueType],
    modes: &[NullPolicy],
    values: Range<usize>,
    indices: &[usize],
    purpose: IndexPurpose,
    work: &BulkWork<'_>,
) -> ProjectionDomains {
    let full_spec = KeySpec::new(types, modes, work.owner, &|| check(work)).unwrap();
    let full = BulkDomain::new(&full_spec, work.owner, work.control).unwrap();
    let group = if indices.is_empty() {
        None
    } else {
        let group_types = indices
            .iter()
            .map(|&i| types[i].clone())
            .collect::<Vec<_>>();
        let group_modes = indices.iter().map(|&i| modes[i]).collect::<Vec<_>>();
        let spec = KeySpec::new(&group_types, &group_modes, work.owner, &|| check(work)).unwrap();
        Some(BulkDomain::new(&spec, work.owner, work.control).unwrap())
    };
    let mut group_full_components = funded_vec(work.owner, indices.len()).unwrap();
    group_full_components.extend_from_slice(indices);
    ProjectionDomains::Naa {
        group,
        full,
        group_full_components,
        value_components: values,
        full_purpose: purpose,
        _storage: work.owner.storage_lifetime(),
    }
}
pub(crate) fn chunk(array: ArrayRef, nullable: bool) -> crate::exec::chunk::Chunk {
    let ty = FunctionValueType::new(array.data_type().clone(), nullable);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        array.data_type().clone(),
        nullable,
    )]));
    crate::exec::chunk::typed_fixture::chunk(
        RecordBatch::try_new(schema, vec![array]).unwrap(),
        &[novarocks_types::SlotId::new(1)],
        &[ty],
    )
}
pub(crate) fn source(rows: usize) -> crate::exec::chunk::Chunk {
    chunk(
        Arc::new(Int32Array::from(
            (0..rows).map(|i| i as i32).collect::<Vec<_>>(),
        )),
        false,
    )
}
pub(crate) fn error<T>(value: ExecutionResult<T>) -> ExecutionFailure {
    match value {
        Err(cause) => cause,
        Ok(_) => panic!("original refusal must remain visible"),
    }
}
pub(crate) fn collect(
    receipts: Vec<BuildInputReceipt>,
    work: &BulkWork<'_>,
) -> (
    CollectedInputs<BuildInputReceipt>,
    BeginLease<BuildInputReceipt, NoArtifact>,
) {
    let completion: Arc<JoinCompletion<NoArtifact>> = JoinCompletion::new(
        JoinParticipants {
            inputs: receipts.len(),
            finalizers: NonZeroUsize::new(2).unwrap(),
            probes: 1,
            tails: 0,
        },
        None,
        funded_shared(work.owner, Observable::new()).unwrap(),
        work.owner,
        work.control,
    )
    .unwrap();
    let collection = BuildCollection::new(completion, work.owner, work.control).unwrap();
    // Arrival order is independent of the actual installed input slot.
    for receipt in receipts {
        collection
            .normal_input(receipt.run().index(), receipt)
            .unwrap();
    }
    match collection.claim_begin(work.owner, work.control).unwrap() {
        BeginClaim::Claimed { inputs, lease } => (inputs, lease),
        _ => panic!("all actual input receipts must be available"),
    }
}
fn append(
    writer: &mut InputRunWriter,
    values: Vec<Option<i32>>,
    ty: &FunctionValueType,
    work: &BulkWork<'_>,
) -> BuildPageProjection {
    let array: ArrayRef = Arc::new(Int32Array::from(values));
    let source = chunk(Arc::clone(&array), true);
    writer
        .push_evaluated(
            &source,
            EvaluatedJoinKeyPage::Ordinary(EvaluatedKeyProjection {
                arrays: &[array],
                bindings: std::slice::from_ref(ty),
            }),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            work,
        )
        .unwrap()
}

#[test]
fn hierarchical_rows_keep_e_slots_empty_pages_and_true_k_coordinates() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int32, true);
    let domains = ordinary(&ty, NullPolicy::Unmatchable, IndexPurpose::KeyIds, &work);
    let mut e2 = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(2, 3).unwrap(),
        true,
        &work,
    )
    .unwrap();
    append(&mut e2, vec![Some(9)], &ty, &work);
    let mut e0 = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(0, 3).unwrap(),
        true,
        &work,
    )
    .unwrap();
    append(&mut e0, vec![None, Some(1)], &ty, &work);
    append(&mut e0, vec![], &ty, &work);
    let last_page = append(&mut e0, vec![Some(2)], &ty, &work);
    let id = last_page.projection_id(ProjectionKind::Ordinary).unwrap();
    assert_ne!(id.run_index(), 0);
    let mut e1 = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(1, 3).unwrap(),
        true,
        &work,
    )
    .unwrap();
    let (inputs, _begin) = collect(
        vec![
            e2.seal(&work).unwrap(),
            e1.seal(&work).unwrap(),
            e0.seal(&work).unwrap(),
        ],
        &work,
    );
    let (rows, captures) = BuildRows::from_inputs(inputs, &domains, &work).unwrap();
    assert_eq!((rows.len(), rows.run_count()), (4, 3));
    let expected = [(0, 0, 0), (0, 0, 1), (0, 2, 0), (2, 0, 0)];
    for (i, (run, page, row)) in expected.into_iter().enumerate() {
        let address = rows.locate(rows.row(i).unwrap()).unwrap();
        assert_eq!(
            (address.run().index(), address.page(), address.row_in_page()),
            (run, page, row)
        );
        assert_eq!(rows.row_id(address).unwrap().index(), i);
    }
    assert!(rows.row(4).is_err());
    let address = rows
        .address_for_assignment(ProjectionKind::Ordinary, id, 0)
        .unwrap();
    assert_eq!(rows.row_id(address).unwrap().index(), 2);
    assert!(
        rows.address_for_assignment(ProjectionKind::Group, id, 0)
            .is_err()
    );
    assert!(
        rows.address_for_assignment(ProjectionKind::Ordinary, id, 1)
            .is_err()
    );
    assert_eq!(
        rows.run(B3RunId::from_input_slot(1, 3).unwrap())
            .unwrap()
            .page_count(),
        0
    );
    let CapturesForGraphs::Ordinary { keys } = captures else {
        panic!("ordinary capture")
    };
    let ProjectionDomains::Ordinary { keys: domain, .. } = &domains else {
        unreachable!()
    };
    let _actual_complete = keys
        .into_complete(domain, IndexPurpose::KeyIds, &work)
        .unwrap();
}

#[test]
fn ordinary_presence_can_omit_payload_without_confusing_empty_lookup_with_physical_empty() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int32, true);
    let domains = ordinary(
        &ty,
        NullPolicy::Unmatchable,
        IndexPurpose::PresenceOnly,
        &work,
    );
    let mut writer = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(0, 1).unwrap(),
        false,
        &work,
    )
    .unwrap();
    let page = append(&mut writer, vec![None, None], &ty, &work);
    assert_eq!(page.rows(), 2);
    assert!(page.payload().is_none());
    assert!(page.values().is_none());
    let (inputs, _begin) = collect(vec![writer.seal(&work).unwrap()], &work);
    let (rows, _) = BuildRows::from_inputs(inputs, &domains, &work).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.locate(rows.row(1).unwrap()).unwrap().row_in_page(), 1);
}

#[test]
fn one_page_projection_and_escaped_buffer_keep_the_original_source_creator() {
    use novarocks_functions::{
        EvaluationCheckpoints, SourceBackingOwner, opaque_memory::OpaqueRetainedCharge,
        retain_source_backing, source_custody_prefix_facts,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Creator(Arc<AtomicUsize>);
    impl Drop for Creator {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let drops = Arc::new(AtomicUsize::new(0));
    let authority = OpaqueRetainedCharge::try_new(Arc::clone(&work.host)).unwrap();
    let prefix = authority
        .reserve_operation(source_custody_prefix_facts().requests().bytes())
        .unwrap();
    let mut observations = EvaluationCheckpoints::new(&Control);
    let creator = SourceBackingOwner::try_from_owned_direct(
        Creator(Arc::clone(&drops)),
        Arc::clone(&work.host),
        &mut observations,
    )
    .unwrap();
    let values =
        retain_source_backing(Arc::new(Int32Array::from(vec![5, 7])), &creator, &Control).unwrap();
    observations.finish().unwrap();
    drop(creator);
    drop(prefix);
    drop(authority);
    let ty = FunctionValueType::new(DataType::Int32, false);
    let domains = ordinary(&ty, NullPolicy::Equal, IndexPurpose::KeyIds, &work);
    let source = chunk(Arc::clone(&values), false);
    let mut writer = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(0, 1).unwrap(),
        true,
        &work,
    )
    .unwrap();
    let page = writer
        .push_evaluated(
            &source,
            EvaluatedJoinKeyPage::Ordinary(EvaluatedKeyProjection {
                arrays: std::slice::from_ref(&values),
                bindings: std::slice::from_ref(&ty),
            }),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            &work,
        )
        .unwrap();
    let receipt = writer.seal(&work).unwrap();
    drop(receipt);
    drop(writer);
    drop(source);
    drop(values);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let payload = page.payload().unwrap();
    let column = payload.column(0).unwrap();
    assert_eq!(column.slot.typed_binding().value_type().nullable(), false);
    let buffer = column.array.to_data().buffers()[0].clone();
    assert_eq!(
        column
            .array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[5, 7]
    );
    drop(page);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(buffer);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn directory_refusal_retains_the_original_cause_and_cannot_publish_partial_rows() {
    struct Deny<'a>(&'a dyn InputConstructionOwner, ExecutionFailure);
    impl InputConstructionOwner for Deny<'_> {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            self.0.storage_lifetime()
        }
        fn request(&self, _: std::alloc::Layout) -> Result<(), ConstructionFailure> {
            Err(ConstructionFailure::from(self.1.clone()))
        }
        fn observe_opaque_upper_bound(&self, n: usize) -> Result<(), ConstructionFailure> {
            self.0.observe_opaque_upper_bound(n)
        }
        fn observe(
            &self,
            n: usize,
            path: &[crate::exec::chunk::ArrayPathStep],
            step: crate::exec::chunk::ArrayNormalizationStep<'_>,
        ) -> Result<(), ConstructionFailure> {
            self.0.observe(n, path, step)
        }
    }
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int32, true);
    let domains = ordinary(&ty, NullPolicy::Equal, IndexPurpose::KeyIds, &work);
    let mut writer = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(0, 1).unwrap(),
        true,
        &work,
    )
    .unwrap();
    append(&mut writer, vec![Some(1)], &ty, &work);
    let (inputs, mut begin) = collect(vec![writer.seal(&work).unwrap()], &work);
    let original = build_contract("injected original directory constructor refusal");
    let denial = Deny(owner.as_ref(), original.clone());
    let denied = BulkWork {
        owner: &denial,
        allocator: work.allocator.clone(),
        host: Arc::clone(&work.host),
        control: &Control,
    };
    let cause = error(BuildRows::from_inputs(inputs, &domains, &denied));
    assert!(cause.same_occurrence(&original));
    assert!(begin.fail(cause).same_occurrence(&original));
}
