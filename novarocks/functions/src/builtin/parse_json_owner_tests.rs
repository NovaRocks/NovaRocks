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

//! Original Native N1 profile and genuine host/control lifecycle failures.
use super::*;
use crate::kernel_control::{internal, invalid};
use crate::opaque_memory::OpaqueAllocationHost;
use crate::{
    AggregateStateAllocator, EvaluatedArgument, FunctionArgument, FunctionSpecializationFailure,
    FunctionValueType, KernelDiagnostic, RowDataError, ScalarEvaluationInstance,
    ScopedExpressionEffects, SelectedValues, Selection, specialize_scalar,
};
use arrow_array::{Array, ArrayRef, LargeBinaryArray, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters,
};
use std::{alloc::Layout, ptr::NonNull, sync::Mutex, time::Duration};
fn owner(operation: JsonFunction) -> ParseJsonOwner {
    let signatures = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(n, _)| n == operation.name())
        .unwrap()
        .1;
    let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
        operation.name(),
        &signatures,
        FunctionKind::Scalar,
    )
    .unwrap();
    ParseJsonOwner::new(operation, declaration, resolver).unwrap()
}

fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        region: EvaluationRegionId::new(7),
        demand: EvaluationDemand::Value,
    }
}
fn arguments(n: usize) -> Vec<FunctionArgument> {
    (0..n)
        .map(|_| FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, true),
            constant: None,
        })
        .collect()
}
fn request(a: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        expected_result_type: None,
        arguments: a,
        logical_argument_count: a.len(),
    }
}
fn prepare_with(
    n: usize,
    uses: &[Option<ExpressionUseId>],
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    let args = arguments(n);
    prepare_operation(JsonFunction::Parse, &args, uses, control)
}

fn prepare_operation(
    operation: JsonFunction,
    args: &[FunctionArgument],
    uses: &[Option<ExpressionUseId>],
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    let owner = owner(operation);
    let selected = Arc::new(owner.resolve(request(args), control)?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    specialize_scalar(
        &owner,
        CallEffectInput {
            context: (context()).into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
            function_id: owner.declaration.function_id(),
            kind: FunctionKind::Scalar,
            selected: &selected,
            request: request(args),
            environment: &[],
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        },
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        control,
    )
    .map(|s| s.into_prepared())
}
fn prepare(n: usize) -> Arc<dyn PreparedScalarKernel> {
    let mut uses = vec![None; n];
    uses[0] = Some(ExpressionUseId::new(42));
    prepare_with(n, &uses, crate::binding_test_control()).unwrap()
}
fn instance(n: usize, host: &Arc<Host>) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate_with_allocator(
        prepare(n),
        Some(host.clone() as Arc<dyn AggregateStateAllocator>),
    )
    .unwrap()
}
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
struct Unmetered;
impl crate::json_value::JsonWork for Unmetered {
    type Reservation = ();

    fn step(&mut self) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn reserve(&mut self, _: usize) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn reserve_output(&mut self, _: usize) -> Result<(), KernelFailure> {
        Ok(())
    }
}

fn texts(out: &ArrayRef) -> Vec<Option<String>> {
    out.as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .iter()
        .map(|value| value.map(|bytes| crate::json_value::format(bytes, &mut Unmetered).unwrap()))
        .collect()
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after primary refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("PARSE_JSON never waits")
    }
}
#[derive(Default)]
struct Ledger {
    opaque_attempts: usize,
    opaque_bytes: usize,
    attempts: usize,
    bytes: usize,
    peak: usize,
    live: Vec<(usize, Layout)>,
    metadata: Option<(usize, Layout)>,
}
#[derive(Default)]
struct Host {
    opaque_refusal: Mutex<Option<(usize, KernelFailure)>>,
    ledger: Mutex<Ledger>,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
}
impl AggregateStateAllocator for Host {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap() {
            assert!(at <= *stop, "owned allocation after first refusal");
            if *stop == at {
                return Err(cause.clone());
            }
        }
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes += layout.size();
        ledger.peak = ledger.peak.max(ledger.bytes);
        let block = (pointer.as_ptr().addr(), layout);
        if ledger.metadata.is_none() {
            ledger.metadata = Some(block);
        }
        ledger.live.push(block);
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, actual)| *address == pointer.as_ptr().addr() && *actual == layout)
            .expect("exact block released once");
        ledger.live.swap_remove(at);
        ledger.bytes -= layout.size();
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
fn arm_refusal(host: &Host, offset: usize, cause: KernelFailure) {
    let next = host.ledger.lock().unwrap().attempts;
    *host.refusal.lock().unwrap() = Some((next + offset, cause));
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("host original"),
        internal("host original"),
        KernelFailure::Operational(KernelDiagnostic::new("host original")),
        KernelFailure::InstanceFailed,
    ]
}

impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.opaque_attempts;
        ledger.opaque_attempts += 1;
        if let Some((stop, cause)) = &*self.opaque_refusal.lock().unwrap() {
            assert!(at <= *stop, "opaque reservation after first refusal");
            if *stop == at {
                return Err(cause.clone());
            }
        }
        ledger.opaque_bytes = ledger.opaque_bytes.checked_add(bytes).unwrap();
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.opaque_bytes = ledger
            .opaque_bytes
            .checked_sub(bytes)
            .expect("actual opaque charge released once");
    }
}
fn empty(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    assert_eq!(ledger.bytes, 0);
    assert_eq!(ledger.opaque_bytes, 0);
    assert!(ledger.live.is_empty());
}

#[test]
fn parse_json_actual_host_precedes_parser_and_preserves_all_seven_refusals() {
    let array = strings(vec![Some("{bad"), Some("[1,2]")]);
    for cause in causes() {
        let host = Arc::new(Host::default());
        let mut kernel = instance(1, &host);
        let next = host.ledger.lock().unwrap().opaque_attempts;
        *host.opaque_refusal.lock().unwrap() = Some((next, cause.clone()));
        assert_eq!(
            kernel
                .evaluate(
                    Selection::all(2),
                    &[EvaluatedArgument::Column(&array)],
                    &Control::default()
                )
                .unwrap_err(),
            cause
        );
        assert_eq!(
            kernel
                .evaluate(
                    Selection::all(2),
                    &[EvaluatedArgument::Column(&array)],
                    &Control::default()
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        drop(kernel);
        empty(&host);
    }
}

#[test]
fn parse_json_selected_compact_nulls_and_original_data_errors() {
    let host = Arc::new(Host::default());
    let array = strings(vec![
        Some("guard"),
        Some("{bad"),
        None,
        Some("null"),
        Some("[1,2]"),
        Some("guard"),
    ])
    .slice(1, 4);
    let rows = [0, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("{bad"), Some("null"), Some("[1,2]")]),
        Box::default(),
    )
    .unwrap();
    for argument in [
        EvaluatedArgument::Column(&array),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        let mut kernel = instance(1, &host);
        let args = [argument];
        let out = kernel
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(
            texts(out.values()),
            vec![None, Some("null".into()), Some("[1,2]".into())]
        );
        assert_eq!(out.errors().len(), 1);
        assert_eq!(out.errors()[0].selected_ordinal(), 0);
        assert_eq!(
            out.errors()[0].reason(),
            crate::DataFailureReason::Domain(
                novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT
            ),
        );
        drop(out);
        drop(kernel);
        empty(&host);
    }
}

#[test]
fn parse_json_result_backing_lives_until_last_arrow_alias() {
    for input in [
        strings(vec![]),
        strings(vec![None; 3]),
        strings(vec![Some("{\"b\":1,\"a\":2}"), None, Some("null")]),
    ] {
        let host = Arc::new(Host::default());
        let mut kernel = instance(1, &host);
        let args = [EvaluatedArgument::Column(&input)];
        let out = kernel
            .evaluate(Selection::all(input.len()), &args, &Control::default())
            .unwrap();
        let empty_demand = input.is_empty();
        let clone = out.values().clone();
        let slice = clone.slice(0, clone.len());
        let retained = host.ledger.lock().unwrap().opaque_bytes;
        assert!(retained > 0);
        drop(out);
        drop(kernel);
        // ScalarV1 deliberately skips empty demand; its empty array creates no
        // owner payload. The only earlier opaque stock is the instance body.
        let output_charge = if empty_demand {
            0
        } else {
            host.ledger.lock().unwrap().opaque_bytes
        };
        assert_eq!(
            output_charge,
            if empty_demand {
                0
            } else {
                retained - std::mem::size_of::<ParseJsonInstance>()
            }
        );
        drop(clone);
        assert_eq!(host.ledger.lock().unwrap().opaque_bytes, output_charge);
        drop(slice);
        empty(&host);
    }
}

#[test]
fn parse_json_observed_seven_causes_stop_before_null_and_preserve_prefix() {
    let array = strings(vec![Some("{bad"), Some("{\"b\":[1,2],\"a\":\"é\"}"), None]);
    let host = Arc::new(Host::default());
    let mut kernel = instance(1, &host);
    let control = Control::default();
    let args = [EvaluatedArgument::Column(&array)];
    let out = kernel.evaluate(Selection::all(3), &args, &control).unwrap();
    let trace = control.trace.lock().unwrap().clone();
    drop(out);
    drop(kernel);
    empty(&host);
    // Every actual opaque/owned observation boundary can refuse; no boundary
    // may convert the control cause to the original successful NULL channel.
    for cause in causes() {
        for stop in 0..trace.len() {
            let host = Arc::new(Host::default());
            let mut kernel = instance(1, &host);
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert_eq!(
                kernel
                    .evaluate(
                        Selection::all(3),
                        &[EvaluatedArgument::Column(&array)],
                        &control
                    )
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            let before = control.trace.lock().unwrap().len();
            assert_eq!(
                kernel
                    .evaluate(
                        Selection::all(3),
                        &[EvaluatedArgument::Column(&array)],
                        &control
                    )
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert_eq!(control.trace.lock().unwrap().len(), before);
            drop(kernel);
            empty(&host);
        }
    }
}

#[test]
fn parse_json_owned_custody_host_refusals_release_every_actual_block() {
    let input = strings(vec![Some("[1,2]")]);
    for cause in causes() {
        let host = Arc::new(Host::default());
        let mut kernel = instance(1, &host);
        arm_refusal(&host, 0, cause.clone());
        assert_eq!(
            kernel
                .evaluate(
                    Selection::all(1),
                    &[EvaluatedArgument::Column(&input)],
                    &Control::default()
                )
                .unwrap_err(),
            cause
        );
        drop(kernel);
        empty(&host);
    }
}

#[test]
fn parse_json_missing_real_allocation_capability_is_named_rejection() {
    assert!(
        matches!(prepare(1).create_instance(), Err(KernelFailure::InvalidProgram(ref diagnostic))
        if diagnostic.message() == "JSON requires its actual scalar allocation host")
    );
    assert!(matches!(
        prepare(1).create_instance_with_allocator(None),
        Err(KernelFailure::InvalidProgram(_))
    ));
    struct NoOpaqueHost;
    impl AggregateStateAllocator for NoOpaqueHost {
        fn allocate(&self, _: Layout) -> Result<NonNull<u8>, KernelFailure> {
            panic!("missing opaque capability must reject before allocation")
        }
        unsafe fn release(&self, _: NonNull<u8>, _: Layout) {
            panic!("missing opaque capability owns no allocation")
        }
    }
    assert!(matches!(
        prepare(1).create_instance_with_allocator(Some(Arc::new(NoOpaqueHost))),
        Err(KernelFailure::InvalidProgram(ref diagnostic))
            if diagnostic.message() == "JSON requires its actual scalar allocation host"
    ));
}

fn selected_operation(
    operation: JsonFunction,
    arg_types: Vec<FunctionValueType>,
    host: &Arc<Host>,
) -> ScalarEvaluationInstance {
    let count = arg_types.len();
    let args = arg_types
        .into_iter()
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        })
        .collect::<Vec<_>>();
    let uses = (0..count)
        .map(|index| Some(ExpressionUseId::new(u32::try_from(index).unwrap() + 42)))
        .collect::<Vec<_>>();
    let prepared =
        prepare_operation(operation, &args, &uses, crate::binding_test_control()).unwrap();
    ScalarEvaluationInstance::instantiate_with_allocator(
        prepared,
        Some(host.clone() as Arc<dyn AggregateStateAllocator>),
    )
    .unwrap()
}

fn json_strings(values: &[Option<&str>]) -> ArrayRef {
    let owned = values
        .iter()
        .map(|value| value.map(|raw| crate::json_value::parse(raw, &mut Unmetered).unwrap()))
        .collect::<Vec<_>>();
    let array = LargeBinaryArray::from_iter(owned.iter().map(|value| value.as_deref()));
    Arc::new(array)
}

#[test]
fn json_object_selected_keeps_original_members_and_applies_null_and_unique_policies() {
    let key = strings(vec![Some("k"), Some("k"), Some("k"), None]);
    let rhs_key = strings(vec![Some("k"), Some("k"), Some("x"), Some("k")]);
    let left = json_strings(&[Some("1.00"), Some("1.00"), Some(r#"{"a":1,"a":2}"#), None]);
    let right = json_strings(&[Some("2e0"), None, None, None]);
    let args = [
        EvaluatedArgument::Column(&key),
        EvaluatedArgument::Column(&rhs_key),
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&right),
    ];
    for unique in [false, true] {
        let json = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let text = FunctionValueType::new(DataType::Utf8, true);
        let host = Arc::new(Host::default());
        let mut kernel = selected_operation(
            JsonFunction::Object {
                null_on_null: false,
                unique,
            },
            vec![text.clone(), text, json.clone(), json],
            &host,
        );
        let out = kernel
            .evaluate(Selection::all(4), &args, &Control::default())
            .unwrap();
        assert_eq!(
            texts(out.values()),
            if unique {
                vec![None, Some(r#"{"k":1.00}"#.into()), None, None]
            } else {
                vec![
                    Some(r#"{"k":1.00,"k":2.0}"#.into()),
                    Some(r#"{"k":1.00}"#.into()),
                    Some(r#"{"k":{"a":1,"a":2}}"#.into()),
                    None,
                ]
            }
        );
        let ordinals = out
            .errors()
            .iter()
            .map(RowDataError::selected_ordinal)
            .collect::<Vec<_>>();
        assert_eq!(ordinals, if unique { vec![0, 2, 3] } else { vec![3] });
        let hold = out.values().slice(0, 2);
        drop(out);
        drop(kernel);
        assert!(host.ledger.lock().unwrap().opaque_bytes > 0);
        drop(hold);
        empty(&host);
    }
}

#[test]
fn json_serialization_input_error_is_row_scoped_and_can_return_sql_null() {
    use super::super::json_sql::JsonEncoding;
    let source: ArrayRef = Arc::new(arrow_array::BinaryArray::from(vec![
        Some([b'"', 0xff, b'"'].as_slice()),
        Some(b"oops".as_slice()),
        None,
    ]));
    for null_on_error in [false, true] {
        let host = Arc::new(Host::default());
        let mut kernel = selected_operation(
            JsonFunction::ReadBinary {
                encoding: JsonEncoding::Utf8,
                null_on_error,
            },
            vec![FunctionValueType::new(DataType::Binary, true)],
            &host,
        );
        let args = [EvaluatedArgument::Column(&source)];
        let out = kernel
            .evaluate(Selection::all(3), &args, &Control::default())
            .unwrap();
        assert_eq!(texts(out.values()), vec![Some(r#""�""#.into()), None, None]);
        if null_on_error {
            assert!(out.errors().is_empty());
        } else {
            assert_eq!(out.errors().len(), 1);
            assert_eq!(out.errors()[0].selected_ordinal(), 1);
            assert_eq!(
                out.errors()[0].reason(),
                crate::DataFailureReason::Domain(
                    novarocks_function_contract::kernel_failure::JSON_INPUT_CONVERSION_ERROR,
                )
            );
        }
        drop(out);
        drop(kernel);
        empty(&host);
    }
}
