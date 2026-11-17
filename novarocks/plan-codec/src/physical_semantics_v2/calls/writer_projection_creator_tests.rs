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
use novarocks_type_contract::{
    CompilePhase, SemanticParameterId, SemanticParameterKey, ValueSchemaCreation,
    ValueTypeStorageHold, ValueTypeStorageLifetime,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
fn source() -> FrozenPhysicalCall {
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(41),
    );
    FrozenPhysicalCall {
        site: PhysicalCallSite::WriterPartial {
            node: NodeId::new(3),
            call: 0,
        },
        context,
        materialized_writer_input: Some(
            novarocks_type_contract::RelationEffectContext::materialized_input(
                context.identity.call,
            ),
        ),
        effects: CallEffects {
            value_stability: FunctionVolatility::Immutable,
            own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: FunctionNullBehavior::CalledOnNull,
            argument_control: ArgumentControl::Aggregate,
            instance_state: FunctionInstanceState::AggregateInstance,
            observable_effects: ObservableEffects::NONE,
            environment: Box::from([novarocks_type_contract::SemanticParameterRef {
                id: SemanticParameterId::new(7),
                expected_key: SemanticParameterKey::TimeZone,
            }]),
            proof_scope: CallProofScope::Relation(context.identity),
        },
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
    }
}
struct Scope(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Scope {}
impl Drop for Scope {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn writer_projection_roundtrip_keeps_exact_source_context_and_last_creator() {
    let source = source();
    let control = Control;
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let raw = encode_frozen_writer_call_with_creator_in::<std::convert::Infallible>(
        &source, None, &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(Scope(drops.clone()));
    let storage = ValueTypeStorageLifetime::held(owner.clone());
    let mut actual = Vec::new();
    let mut opaque = Vec::new();
    let mut request = |layout: std::alloc::Layout| {
        actual.push(layout);
        Ok::<_, std::convert::Infallible>(())
    };
    let mut upper = |bytes| {
        opaque.push(bytes);
        Ok::<_, std::convert::Infallible>(())
    };
    let mut creation = ValueSchemaCreation::new(storage, &mut request, &mut upper);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let created =
        decode_frozen_writer_call_with_creator_in(&raw, source.site, &mut creation, &mut work)
            .unwrap();
    work.finish().unwrap();
    assert_eq!(created.call(), &source);
    drop(creation);
    drop(owner);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(actual.iter().any(|request| request.size() > 0));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(created);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn writer_projection_rejects_missing_receipt_and_preserves_original_host_without_tail() {
    use crate::host_projection_v2::ProjectionFailure;
    let source = source();
    let control = Control;
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let raw = encode_frozen_writer_call_with_creator_in::<std::convert::Infallible>(
        &source, None, &mut work,
    )
    .unwrap();
    let original = Arc::new(17u64);
    let expected = original.clone();
    let mut requests = 0usize;
    let mut request = |_: std::alloc::Layout| {
        requests += 1;
        assert_eq!(requests, 1, "allocation after first caller refusal");
        Err(original.clone())
    };
    let mut upper =
        |_: usize| -> Result<(), Arc<u64>> { panic!("opaque callback after first caller refusal") };
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut upper);
    let mut absent = raw.clone();
    absent.materialized_input = None;
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    assert!(matches!(
        decode_frozen_writer_call_with_creator_in(&absent, source.site, &mut creation, &mut work),
        Err(ProjectionFailure::Codec(E::InvalidShape(_)))
    ));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let result =
        decode_frozen_writer_call_with_creator_in(&raw, source.site, &mut creation, &mut work);
    assert!(
        matches!(result,Err(ProjectionFailure::Host(ref cause)) if Arc::ptr_eq(cause,&expected))
    );
    drop(creation);
    assert_eq!(requests, 1);
}
#[test]
fn whole_call_parts_keep_original_host_before_any_child_or_final_validator() {
    use crate::host_projection_v2::ProjectionFailure;
    let source = source();
    let control = Control;
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let raw = encode_frozen_writer_call_with_creator_in::<std::convert::Infallible>(
        &source, None, &mut work,
    )
    .unwrap();
    let source = wire::FrozenCalls { entries: vec![raw] };
    let invoice = std::mem::size_of::<wire::FrozenCalls>()
        + std::alloc::Layout::array::<wire::FrozenCall>(source.entries.capacity())
            .unwrap()
            .size()
        + std::alloc::Layout::array::<wire::SemanticParameterRef>(
            source.entries[0]
                .effects
                .as_ref()
                .unwrap()
                .environment
                .capacity(),
        )
        .unwrap()
        .size();
    let original = Arc::new(17u64);
    let expected = original.clone();
    let mut count = 0;
    let mut exact = |_: std::alloc::Layout| {
        count += 1;
        assert_eq!(count, 1, "allocation after original refusal");
        Err(original.clone())
    };
    let mut opaque =
        |_: usize| -> Result<(), Arc<u64>> { panic!("opaque tail after original refusal") };
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let result = decode_frozen_call_parts_input_derived_with_creator_in(
        &source,
        invoice,
        &mut creation,
        &mut work,
    );
    assert!(matches!(result,Err(ProjectionFailure::Host(cause)) if Arc::ptr_eq(&cause,&expected)));
    drop(creation);
    assert_eq!(count, 1);
}
#[test]
fn whole_call_parts_keep_exact_sources_and_last_finite_creator_without_fragment_proof() {
    let original = source();
    let control = Control;
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let raw = encode_frozen_writer_call_with_creator_in::<std::convert::Infallible>(
        &original, None, &mut work,
    )
    .unwrap();
    let source = wire::FrozenCalls { entries: vec![raw] };
    let invoice = std::mem::size_of::<wire::FrozenCalls>()
        + std::alloc::Layout::array::<wire::FrozenCall>(source.entries.capacity())
            .unwrap()
            .size()
        + std::alloc::Layout::array::<wire::SemanticParameterRef>(
            source.entries[0]
                .effects
                .as_ref()
                .unwrap()
                .environment
                .capacity(),
        )
        .unwrap()
        .size();
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(Scope(drops.clone()));
    let mut requests = Vec::new();
    let mut exact = |layout: std::alloc::Layout| {
        requests.push(layout);
        Ok::<_, std::convert::Infallible>(())
    };
    let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
    let mut creation = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::held(owner.clone()),
        &mut exact,
        &mut opaque,
    );
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let (parts, facts) = decode_frozen_call_parts_input_derived_with_creator_in(
        &source,
        invoice,
        &mut creation,
        &mut work,
    )
    .unwrap();
    assert_eq!(parts.calls(), &[original]);
    assert!(facts.list_item_count >= 1);
    assert!(facts.allocation_request_bytes_upper_bound > 0);
    drop(creation);
    drop(owner);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(requests.iter().any(|layout| layout.size() > 0));
    let (calls, storage) = parts.into_parts();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(calls);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(storage);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
