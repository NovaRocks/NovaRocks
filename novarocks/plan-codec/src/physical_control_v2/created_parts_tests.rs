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
    ValueSchemaCreation, ValueTypeStorageHold, ValueTypeStorageLifetime,
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
struct Hold(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Hold {}
impl Drop for Hold {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn raw() -> wire::ExpressionControl {
    wire::ExpressionControl {
        regions: vec![wire::EvaluationRegion {
            id: 0,
            parent_region_id: None,
            guard: None,
        }],
        uses: Vec::new(),
    }
}
#[test]
fn created_control_parts_preserve_sparse_zero_and_finite_source_owner_without_graph_claim() {
    let source = raw();
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(Hold(drops.clone()));
    let mut requests = Vec::new();
    let mut exact = |layout: Layout| {
        requests.push(layout);
        Ok::<_, std::convert::Infallible>(())
    };
    let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
    let mut creation = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::held(owner.clone()),
        &mut exact,
        &mut opaque,
    );
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let (parts, facts) = decode_expression_control_parts_input_derived_with_creator_in(
        &source,
        control_source_floor(&source).unwrap(),
        &mut creation,
        &mut work,
    )
    .unwrap();
    assert_eq!(parts.regions()[0].id.get(), 0);
    assert!(parts.uses().is_empty());
    assert_eq!(facts.region_count, 1);
    drop(creation);
    drop(owner);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(requests.iter().any(|layout| layout.size() > 0));
    let (_, _, storage) = parts.into_parts();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(storage);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn created_control_parts_return_first_original_host_with_no_allocation_tail() {
    use crate::host_projection_v2::ProjectionFailure;
    let source = raw();
    let original = Arc::new(17u64);
    let expected = original.clone();
    let mut count = 0;
    let mut exact = |_: Layout| {
        count += 1;
        assert_eq!(count, 1, "allocation after original refusal");
        Err(original.clone())
    };
    let mut opaque =
        |_: usize| -> Result<(), Arc<u64>> { panic!("opaque tail after original refusal") };
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let result = decode_expression_control_parts_input_derived_with_creator_in(
        &source,
        control_source_floor(&source).unwrap(),
        &mut creation,
        &mut work,
    );
    assert!(matches!(result,Err(ProjectionFailure::Host(cause)) if Arc::ptr_eq(&cause,&expected)));
    drop(creation);
    assert_eq!(count, 1);
}
