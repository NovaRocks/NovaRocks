// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the License; you may not use this file except in
// compliance with the License.  You may obtain a copy at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
// See the License for the specific language governing permissions
// and limitations under the License.

//! Original request component publication on one caller-owned meter.
use super::*;
use arrow_array::{Array, Int64Array};
use novarocks_constant_contract::ConstantPool;

const CALLER: CompilePhase = CompilePhase::LowerProgram;
#[derive(Default)]
struct OwnedControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for OwnedControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CALLER, "request publication created a child phase");
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.stop {
            assert!(trace.len() < at, "callback after the originating refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((at, cause)) if at == trace.len() => Err(cause),
            _ => Ok(()),
        }
    }
}
fn complete<T>(
    control: &dyn PureCompileControl,
    call: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, CallRequestError>,
) -> Result<T, CallRequestError> {
    let mut work = CompileCheckpoints::try_new(control, CALLER)?;
    let result = call(&mut work);
    finish(result, work)
}
fn publish(
    source: &Fragment,
    entries: &[(PhysicalCallSite, PhysicalCallRequest)],
    control: &dyn PureCompileControl,
) -> Result<Fragment, CallRequestError> {
    complete(control, |work| {
        source.clone().with_call_requests_in(entries.to_vec(), work)
    })
}
fn owned_prefixes(
    call: impl Fn(&dyn PureCompileControl) -> Result<(), CallRequestError>,
    expected: Option<CallRequestError>,
) {
    let baseline = OwnedControl::default();
    assert_eq!(call(&baseline).err(), expected);
    let trace = baseline.trace.into_inner().unwrap();
    assert_eq!(trace.first(), Some(&(CALLER, 0)));
    assert!(trace.len() >= 2);
    for at in 1..=trace.len() {
        for cause in CAUSES {
            let control = OwnedControl {
                trace: Mutex::default(),
                stop: Some((at, cause.clone())),
            };
            assert_eq!(call(&control), Err(CallRequestError::Control(cause)));
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}

#[test]
fn caller_owned_publication_retains_dead_type_only_none_and_original_typed_null() {
    let ty = integer(true);
    let field = Arc::new(Field::new("original.null", DataType::Int64, true));
    let array = Int64Array::from(vec![Some(11), None, Some(33)]);
    let backing = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        array.to_data(),
        policy(),
        PHASE,
        &Control::default(),
    )
    .unwrap();
    let reference = ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal: 1,
    };
    let mut pools = crate::ConstantPools::empty();
    pools.insert(reference.pool, backing.clone()).unwrap();
    let setup = Control::default();
    let mut setup_work = CompileCheckpoints::try_new(&setup, PHASE).unwrap();
    let original_null = pools
        .resolve_observed(reference, &ty, &mut setup_work)
        .unwrap();
    setup_work.finish().unwrap();
    assert!(array.is_null(original_null.ordinal() as usize));
    assert!(Arc::ptr_eq(original_null.pool().array(), backing.array()));
    assert!(Arc::ptr_eq(original_null.pool().field_ref(), &field));

    let source = relation_fragment(
        u32::MAX,
        vec![(0, vec![ty.clone(), ty.clone()]), (u32::MAX, vec![])],
    );
    let original = PhysicalCallRequest {
        expected_result_type: Some(integer(true)),
        ..request(vec![
            value(ty.clone()),
            FunctionArgument::Value {
                value_type: ty,
                constant: Some(reference),
            },
        ])
    };
    let entries = vec![(key(0), original.clone()), (key(u32::MAX), request(vec![]))];
    let published = publish(&source, &entries, &OwnedControl::default()).unwrap();
    let legacy = source
        .clone()
        .with_call_requests_observed(entries.clone(), &Control::default())
        .unwrap();
    assert_eq!(published.call_requests(), legacy.call_requests());
    assert_eq!(published.call_requests().get(key(0)), Some(&original));
    assert!(matches!(
        original.arguments[0],
        FunctionArgument::Value { constant: None, .. }
    ));
    assert!(
        matches!(original.arguments[1], FunctionArgument::Value { constant: Some(r), .. } if r == reference)
    );
    // Pool closure was checked above by its own author; this publication port
    // preserves that actual address without claiming to resolve constant data.
    owned_prefixes(|c| publish(&source, &entries, c).map(|_| ()), None);
    owned_prefixes(
        |c| publish(&source, &entries[..1], c).map(|_| ()),
        Some(CallRequestError::MissingDefinition(key(u32::MAX))),
    );
}

#[test]
fn caller_owned_complete_binding_and_sparse_coverage_keep_first_control_refusal() {
    // Value channel and exact full nested source stay a genuine Table binding.
    // The original Lambda's complete parameter/result/nominal cases are moved
    // unchanged in meaning to the neutral scalar borrowed-scope candidate.
    let source = relation_fragment(7, vec![(u32::MAX, vec![nested(false, false, "original")])]);
    let entries = vec![(
        key(u32::MAX),
        request(vec![value(nested(false, false, "original"))]),
    )];
    owned_prefixes(|c| publish(&source, &entries, c).map(|_| ()), None);
    let mut duplicate = entries.clone();
    duplicate.push(entries[0].clone());
    owned_prefixes(
        |c| publish(&source, &duplicate, c).map(|_| ()),
        Some(CallRequestError::DuplicateDefinition(key(u32::MAX))),
    );
    let mut extra = entries.clone();
    extra.push((key(0), request(vec![])));
    owned_prefixes(
        |c| publish(&source, &extra, c).map(|_| ()),
        Some(CallRequestError::ExtraDefinition),
    );
    for wrong in [
        request(vec![]),
        request(vec![FunctionArgument::Lambda {
            parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                Box::default(),
            ),
            result_type: integer(true),
        }]),
        request(vec![value(nested(false, false, "foreign"))]),
        request(vec![value(nested(true, false, "original"))]),
    ] {
        let expected = if wrong.arguments.is_empty() {
            CallRequestError::InvalidArgumentCount(key(u32::MAX))
        } else {
            CallRequestError::ArgumentTypeMismatch(key(u32::MAX))
        };
        owned_prefixes(
            |c| publish(&source, &[(key(u32::MAX), wrong.clone())], c).map(|_| ()),
            Some(expected),
        );
    }
}

#[test]
fn caller_owned_validation_leaves_empty_success_and_wrong_fragment_tail_to_caller() {
    let source = relation_fragment(7, vec![]);
    let table = FragmentCallRequests::try_new(&source, vec![], &Control::default()).unwrap();
    let control = OwnedControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CALLER).unwrap();
    table.validate_fragment_in(&source, &mut work).unwrap();
    assert_eq!(*control.trace.lock().unwrap(), vec![(CALLER, 0)]);
    work.finish().unwrap();
    // Fragment identity, actual Values node visit and extra-record check.
    assert_eq!(
        *control.trace.lock().unwrap(),
        vec![(CALLER, 0), (CALLER, 3)]
    );
    let foreign = relation_fragment(8, vec![]);
    let control = OwnedControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CALLER).unwrap();
    let result = table.validate_fragment_in(&foreign, &mut work);
    assert_eq!(result, Err(CallRequestError::WrongFragment));
    assert_eq!(*control.trace.lock().unwrap(), vec![(CALLER, 0)]);
    assert_eq!(finish(result, work), Err(CallRequestError::WrongFragment));
    assert_eq!(
        *control.trace.lock().unwrap(),
        vec![(CALLER, 0), (CALLER, 1)]
    );
    owned_prefixes(
        |c| complete(c, |w| table.validate_fragment_in(&source, w)),
        None,
    );
    owned_prefixes(
        |c| complete(c, |w| table.validate_fragment_in(&foreign, w)),
        Some(CallRequestError::WrongFragment),
    );
    owned_prefixes(|c| publish(&source, &[], c).map(|_| ()), None);
    owned_prefixes(
        |c| publish(&source, &[(key(0), request(vec![]))], c).map(|_| ()),
        Some(CallRequestError::ExtraDefinition),
    );
}

#[test]
fn caller_owned_relational_records_follow_actual_call_array_ordinals() {
    let source = aggregate_fragment();
    let site = |call| PhysicalCallSite::Aggregate {
        node: NodeId::new(9),
        call,
    };
    let entries = vec![(site(0), request(vec![])), (site(1), request(vec![]))];
    let published = publish(&source, &entries, &OwnedControl::default()).unwrap();
    assert_eq!(published.call_requests().entries().len(), 2);
    owned_prefixes(|c| publish(&source, &entries, c).map(|_| ()), None);
    owned_prefixes(
        |c| {
            publish(
                &source,
                &[(site(0), request(vec![])), (site(7), request(vec![]))],
                c,
            )
            .map(|_| ())
        },
        Some(CallRequestError::MissingDefinition(site(1))),
    );
}
