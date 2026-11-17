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

//! The original argument-type publisher keeps each independent alias funded.
use super::*;
use novarocks_type_contract::{
    CompilePhase, PureCompileControl, ValueSchemaCreation, ValueTypeStorageHold,
    ValueTypeStorageLifetime,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
struct Control(AtomicBool);
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        if self.0.load(Ordering::SeqCst) {
            Err(CompileControlError::Cancelled)
        } else {
            Ok(())
        }
    }
}
struct Hold(Arc<AtomicUsize>);
impl Drop for Hold {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl ValueTypeStorageHold for Hold {}
#[test]
fn independently_cloned_argument_types_keep_only_their_arena_creator() {
    use novarocks_type_contract::{FunctionArgumentType, FunctionValueType};
    let arena_drops = Arc::new(AtomicUsize::new(0));
    let unrelated_drops = Arc::new(AtomicUsize::new(0));
    let unrelated = ValueTypeStorageLifetime::held(Arc::new(Hold(unrelated_drops.clone())));
    let control = Control(AtomicBool::new(false));
    let mut request = |_: Layout| Ok::<(), std::convert::Infallible>(());
    let mut opaque = |_: usize| Ok::<(), std::convert::Infallible>(());
    let mut creation = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::held(Arc::new(Hold(arena_drops.clone()))),
        &mut request,
        &mut opaque,
    );
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let mut types = ProgramCreation {
        creator: Some(&mut creation),
    }
    .reserve::<std::convert::Infallible, FunctionArgumentType>(1, &mut work)
    .unwrap();
    types.push(FunctionArgumentType::Value(FunctionValueType::new(
        arrow::datatypes::DataType::Int64,
        false,
    )));
    let types = publish_argument_types(types, Some(&mut creation), &mut work).unwrap();
    let last = types.clone();
    drop(types);
    drop(creation);
    drop(unrelated);
    assert_eq!(unrelated_drops.load(Ordering::SeqCst), 1);
    assert_eq!(arena_drops.load(Ordering::SeqCst), 0);
    assert_eq!(last.len(), 1);
    drop(last);
    assert_eq!(arena_drops.load(Ordering::SeqCst), 1);
}
