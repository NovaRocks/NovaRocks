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

use crate::kernel_control::{internal, invalid};
use crate::*;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, ListArray, StringArray, StructArray,
    types::Int8Type,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationDomainId, ExpressionEffectContext, ExpressionUseId,
    FunctionInstanceState, FunctionNullBehavior, PureCompileControl, SemanticParameters,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after primary");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Default)]
struct RuntimeControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "runtime callback after primary");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("UNNEST never waits");
    }
}
fn runtime_causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}
fn compile_causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(0),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    }
}
struct Fixture {
    catalog: PureEngineFunctionCatalog,
    function: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    parameters: SemanticParameters,
}
impl Fixture {
    fn new(types: &[FunctionValueType]) -> Self {
        let original = super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                original
                    .definition("unnest", FunctionKind::Table)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        // This single-owner fixture has an independently specified installed
        // record. It does not assert closure of the full builtin catalogue.
        let catalog = builder
            .seal_pure([InstalledPureKernel {
                function: FunctionId::try_new("builtin.table/unnest/v1").unwrap(),
                kind: FunctionKind::Table,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new("builtin.table/unnest/array-variadic-v1")
                        .unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.table/unnest/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::TableV1,
                },
                aggregate_state_format: None,
            }])
            .unwrap();
        let arguments: Vec<_> = types
            .iter()
            .cloned()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect();
        let resolved = catalog
            .metadata()
            .resolve_bound_user(
                "unnest",
                FunctionKind::Table,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .unwrap();
        Self {
            catalog,
            function: resolved.function_id,
            selected: Arc::new(resolved.selected),
            uses: (0..arguments.len())
                .map(|n| Some(ExpressionUseId::new(n as u32 + 1)))
                .collect(),
            arguments,
            parameters: SemanticParameters::try_new([]).unwrap(),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: context(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.function,
            kind: FunctionKind::Table,
            selected: self.selected.as_ref(),
            request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
                expected_result_type: None,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Domain(context().domain),
        }
    }
    fn options(&self) -> PureCallPreparation {
        PureCallPreparation::Table {
            arguments: ScopedExpressionEffects::pure_value(context()),
        }
    }
    fn prepare(
        &self,
        control: &CompileControl,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        self.catalog
            .prepare_fresh(self.input(), self.selected.clone(), self.options(), control)
    }
    fn kernel(&self) -> Arc<dyn PreparedTableKernel> {
        let PreparedPureKernel::Table(kernel) = self
            .prepare(&CompileControl::default())
            .unwrap()
            .into_prepared()
        else {
            panic!("actual table kernel required");
        };
        kernel
    }
}
fn ty(array: &ArrayRef, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), nullable)
}
fn lists(offsets: Vec<i32>, values: Vec<Option<i32>>, validity: Option<Vec<bool>>) -> ArrayRef {
    let raw: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("original-item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(Int32Array::from(values)),
        validity.map(NullBuffer::from),
    ));
    crate::test_value_ingress::array_for_binding(
        &raw,
        &FunctionValueType::new(raw.data_type().clone(), true),
    )
    .unwrap()
}
fn capacity(rows: usize, completions: usize) -> TableStepCapacity {
    TableStepCapacity {
        page: TablePageCapacity { rows, completions },
        parent_errors: 0,
    }
}
fn input<'a>(
    kernel: &'a Arc<dyn PreparedTableKernel>,
    selection: Selection<'a>,
    args: &'a [EvaluatedArgument<'a>],
) -> SelectedTableInput<'a, 'a> {
    SelectedTableInput::try_new(
        kernel.contract(),
        selection,
        args,
        &RuntimeControl::default(),
    )
    .unwrap()
}
fn page(step: TableCursorStep) -> OwnedTableOutputPage {
    let TableCursorStep::Page(page) = step else {
        panic!("expected a page");
    };
    page
}
fn ints(column: &ArrayRef) -> Vec<Option<i32>> {
    column
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
fn policy() -> ConstantPolicy {
    // Explicit finite fixture admission, separate from formal host MEM grants.
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    }
}

#[test]
fn installed_unnest_fresh_and_frozen_keep_exact_selection_full_fields_and_table_effects() {
    let child = Arc::new(Field::new("inner", DataType::Int32, true).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "nested-source".into(),
            "preserved".into(),
        )]),
    ));
    let items = Arc::new(
        Field::new("struct-item", DataType::Struct(vec![child].into()), false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "item-source".into(),
                "exact".into(),
            )]),
        ),
    );
    let source =
        FunctionValueType::from_legacy_declared_projection(DataType::List(items.clone()), true);
    let fixture = Fixture::new(std::slice::from_ref(&source));
    let fresh = fixture.prepare(&CompileControl::default()).unwrap();
    let frozen = fixture
        .catalog
        .prepare_frozen(
            fixture.input(),
            fixture.selected.clone(),
            fresh.call_contract().effects(),
            fixture.options(),
            &CompileControl::default(),
        )
        .unwrap();
    for prepared in [&fresh, &frozen] {
        assert!(Arc::ptr_eq(
            prepared.call_contract().selected_owner(),
            &fixture.selected
        ));
        assert_eq!(
            prepared.call_contract().decimal_overflow_policy(),
            DecimalOverflowPolicy::ReportError
        );
        assert_eq!(prepared.implementation().abi, PureKernelAbi::TableV1);
        let effects = prepared.call_contract().effects();
        assert_eq!(effects.value_stability, FunctionVolatility::Immutable);
        assert_eq!(effects.null_behavior, FunctionNullBehavior::CalledOnNull);
        assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
        assert_eq!(effects.argument_control, ArgumentControl::Table);
        assert_eq!(effects.instance_state, FunctionInstanceState::TableInstance);
        assert!(effects.environment.is_empty());
        assert_eq!(
            prepared.call_contract().selected().argument_types[0],
            FunctionArgumentType::Value(source.clone())
        );
        let PreparedPureKernel::Table(kernel) = prepared.prepared() else {
            panic!("table ABI required");
        };
        assert_eq!(
            kernel.contract().result_types()[0],
            FunctionValueType::from_legacy_declared_projection(items.data_type().clone(), true)
        );
    }
    assert_eq!(fresh.source(), PurePreparationSource::Fresh);
    assert_eq!(frozen.source(), PurePreparationSource::Frozen);
    let mut forged = fixture.selected.as_ref().clone();
    let FunctionResultType::Relation(results) = &mut forged.result_type else {
        unreachable!()
    };
    let mut revised = results.to_vec();
    revised[0] = revised[0]
        .with_nullable(false)
        .expect("valid function nullability declaration");
    *results = novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
        revised.into_boxed_slice(),
    );
    let forged = Arc::new(forged);
    let mut bad_input = fixture.input();
    bad_input.selected = forged.as_ref();
    assert!(
        fixture
            .catalog
            .prepare_frozen(
                bad_input,
                forged.clone(),
                fresh.call_contract().effects(),
                fixture.options(),
                &CompileControl::default()
            )
            .is_err()
    );
    // Scalar options cannot silently substitute for the table lifecycle.
    assert!(
        fixture
            .catalog
            .prepare_fresh(
                fixture.input(),
                fixture.selected.clone(),
                PureCallPreparation::Scalar {
                    arguments: ScopedExpressionEffects::pure_value(context())
                },
                &CompileControl::default()
            )
            .is_err()
    );
}

#[test]
fn longest_zip_sliced_sparse_parents_split_pages_and_preserve_nullable_child_values() {
    let left = lists(
        vec![0, 1, 4, 4, 5, 7],
        vec![Some(99), Some(1), None, Some(3), Some(9), Some(4), Some(5)],
        None,
    )
    .slice(1, 4);
    let right = lists(
        vec![0, 1, 2, 2, 4, 5],
        vec![Some(99), Some(10), Some(20), Some(21), Some(40)],
        None,
    )
    .slice(1, 4);
    let fixture = Fixture::new(&[ty(&left, true), ty(&right, true)]);
    let kernel = fixture.kernel();
    let selected_rows = [0, 2, 3];
    let selection = Selection::try_sparse(4, &selected_rows).unwrap();
    let args = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&right),
    ];
    let mut cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(&kernel, selection, &args),
        &RuntimeControl::default(),
    )
    .unwrap();
    let a = page(
        cursor
            .next(capacity(2, 0), &RuntimeControl::default())
            .unwrap(),
    );
    assert_eq!(ints(&a.columns[0]), [Some(1), None]);
    assert_eq!(ints(&a.columns[1]), [Some(10), None]);
    assert_eq!(&*a.parent_ordinals, [0, 0]);
    assert!(a.completed_parents.is_empty());
    let b = page(
        cursor
            .next(capacity(2, 1), &RuntimeControl::default())
            .unwrap(),
    );
    assert_eq!(ints(&b.columns[0]), [Some(3)]);
    assert_eq!(ints(&b.columns[1]), [None]);
    assert_eq!(&*b.completed_parents, [0]);
    let c = page(
        cursor
            .next(capacity(5, 1), &RuntimeControl::default())
            .unwrap(),
    );
    assert_eq!(ints(&c.columns[0]), [Some(9), None]);
    assert_eq!(ints(&c.columns[1]), [Some(20), Some(21)]);
    assert_eq!(&*c.parent_ordinals, [1, 1]);
    let d = page(
        cursor
            .next(capacity(5, 1), &RuntimeControl::default())
            .unwrap(),
    );
    assert_eq!(ints(&d.columns[0]), [Some(4), Some(5)]);
    assert_eq!(ints(&d.columns[1]), [Some(40), None]);
    assert_eq!(&*d.completed_parents, [2]);
    assert!(d.eof);
    cursor.finish(&RuntimeControl::default()).unwrap();
    assert!(
        cursor
            .next(capacity(1, 1), &RuntimeControl::default())
            .is_err()
    );
}

#[test]
fn null_and_empty_parents_require_explicit_completion_and_capacity_refusal_does_not_advance() {
    let left = lists(
        vec![0, 2, 2, 3],
        vec![Some(88), Some(89), Some(7)],
        Some(vec![false, true, true]),
    );
    let right = lists(vec![0, 0, 0, 0], vec![], None);
    let fixture = Fixture::new(&[ty(&left, true), ty(&right, true)]);
    let kernel = fixture.kernel();
    let args = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&right),
    ];
    let mut cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(&kernel, Selection::all(3), &args),
        &RuntimeControl::default(),
    )
    .unwrap();
    assert!(matches!(
        cursor
            .next(capacity(4, 0), &RuntimeControl::default())
            .unwrap(),
        TableCursorStep::CapacityRequired(TableCapacityRequirements {
            row: false,
            completion: true,
            parent_error: false
        })
    ));
    let a = page(
        cursor
            .next(capacity(0, 1), &RuntimeControl::default())
            .unwrap(),
    );
    assert!(a.parent_ordinals.is_empty());
    assert_eq!(&*a.completed_parents, [0]);
    assert!(!a.eof);
    let b = page(
        cursor
            .next(capacity(0, 1), &RuntimeControl::default())
            .unwrap(),
    );
    assert_eq!(&*b.completed_parents, [1]);
    assert!(matches!(
        cursor
            .next(capacity(0, 1), &RuntimeControl::default())
            .unwrap(),
        TableCursorStep::CapacityRequired(TableCapacityRequirements {
            row: true,
            completion: false,
            parent_error: false
        })
    ));
    let c = page(
        cursor
            .next(capacity(1, 0), &RuntimeControl::default())
            .unwrap(),
    );
    assert_eq!(ints(&c.columns[0]), [Some(7)]);
    assert_eq!(ints(&c.columns[1]), [None]);
    assert!(c.completed_parents.is_empty());
    assert!(matches!(
        cursor
            .next(capacity(0, 0), &RuntimeControl::default())
            .unwrap(),
        TableCursorStep::CapacityRequired(_)
    ));
    let d = page(
        cursor
            .next(capacity(0, 1), &RuntimeControl::default())
            .unwrap(),
    );
    assert_eq!(&*d.completed_parents, [2]);
    assert!(d.eof);
    cursor.finish(&RuntimeControl::default()).unwrap();
    let empty_rows = [];
    let empty = Selection::try_sparse(3, &empty_rows).unwrap();
    let mut empty_cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(&kernel, empty, &args),
        &RuntimeControl::default(),
    )
    .unwrap();
    let end = page(
        empty_cursor
            .next(capacity(0, 0), &RuntimeControl::default())
            .unwrap(),
    );
    assert!(end.eof && end.completed_parents.is_empty());
    empty_cursor.finish(&RuntimeControl::default()).unwrap();
}

#[test]
fn original_constant_ordinal_scalar_and_compact_channels_have_independent_addresses() {
    let original = lists(
        vec![0, 1, 3, 4],
        vec![Some(99), Some(1), Some(2), Some(77)],
        None,
    );
    let source = ty(&original, true);
    let pool = ConstantPool::try_new(
        Arc::new(source.try_to_field("original").unwrap()),
        source.clone(),
        original.to_data(),
        policy(),
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap();
    let constant = pool.value(1).unwrap();
    let scalar = lists(vec![0, 1], vec![Some(10)], None);
    let compact = lists(
        vec![0, 1, 4],
        vec![Some(20), Some(30), Some(31), Some(32)],
        None,
    );
    let rows = [1, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        compact.data_type(),
        compact.clone(),
        Box::default(),
    )
    .unwrap();
    let fixture = Fixture::new(&[source, ty(&scalar, true), ty(compact.values(), true)]);
    let kernel = fixture.kernel();
    let args = [
        EvaluatedArgument::Constant(&constant),
        EvaluatedArgument::Scalar(&scalar),
        EvaluatedArgument::SelectedColumn(&compact),
    ];
    let mut cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(&kernel, selection, &args),
        &RuntimeControl::default(),
    )
    .unwrap();
    let a = page(
        cursor
            .next(capacity(8, 1), &RuntimeControl::default())
            .unwrap(),
    );
    assert_eq!(ints(&a.columns[0]), [Some(1), Some(2)]);
    assert_eq!(ints(&a.columns[1]), [Some(10), None]);
    assert_eq!(ints(&a.columns[2]), [Some(20), None]);
    let b = page(
        cursor
            .next(capacity(8, 1), &RuntimeControl::default())
            .unwrap(),
    );
    assert_eq!(ints(&b.columns[0]), [Some(1), Some(2), None]);
    assert_eq!(ints(&b.columns[1]), [Some(10), None, None]);
    assert_eq!(ints(&b.columns[2]), [Some(30), Some(31), Some(32)]);
    assert!(b.eof);
}

#[test]
fn explicit_dictionary_plain_ingress_and_nested_values_keep_padding_semantics() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec!["first", "second"])),
        )
        .unwrap(),
    );
    let dictionary = arrow_cast::cast(dictionary.as_ref(), &DataType::Utf8).unwrap();
    let field = Arc::new(
        Field::new("dictionary-item", dictionary.data_type().clone(), true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "child-source".into(),
                "unaltered".into(),
            )]),
        ),
    );
    let array: ArrayRef = Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 3])),
        dictionary,
        None,
    ));
    let nested_field = Arc::new(Field::new("nested", DataType::Int32, true).with_metadata(
        std::collections::HashMap::<String, String>::from([("nested-key".into(), "exact".into())]),
    ));
    let structs: ArrayRef = Arc::new(StructArray::new(
        vec![nested_field].into(),
        vec![Arc::new(Int32Array::from(vec![Some(7)]))],
        None,
    ));
    let nested: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("struct", structs.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        structs,
        None,
    ));
    let array = crate::test_value_ingress::array_for_binding(&array, &ty(&array, true)).unwrap();
    let nested = crate::test_value_ingress::array_for_binding(&nested, &ty(&nested, true)).unwrap();
    let fixture = Fixture::new(&[ty(&array, true), ty(&nested, true)]);
    let kernel = fixture.kernel();
    let args = [
        EvaluatedArgument::Column(&array),
        EvaluatedArgument::Column(&nested),
    ];
    let mut cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(&kernel, Selection::all(1), &args),
        &RuntimeControl::default(),
    )
    .unwrap();
    let result = page(
        cursor
            .next(capacity(4, 1), &RuntimeControl::default())
            .unwrap(),
    );
    let plain = result.columns[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        plain.iter().collect::<Vec<_>>(),
        [Some("first"), None, Some("second")]
    );
    assert!(novarocks_type_contract::arrow_data_types_exact(
        result.columns[1].data_type(),
        kernel.contract().result_types()[1].data_type()
    ));
    let structs = result.columns[1]
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(structs.len(), 3);
    assert!(structs.is_null(1) && structs.is_null(2));
    assert_eq!(ints(structs.column(0))[0], Some(7));
}

#[test]
fn union_padding_passes_full_page_contract_while_run_encoded_input_is_refused() {
    use arrow_array::{Int16Array, RunArray, UnionArray, types::Int16Type};
    use arrow_schema::UnionFields;
    let fields = UnionFields::try_new(
        [0],
        [Field::new("original-union-child", DataType::Int32, true)],
    )
    .unwrap();
    let union: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8]),
            Some(ScalarBuffer::from(vec![0_i32])),
            vec![Arc::new(Int32Array::from(vec![Some(7)]))],
        )
        .unwrap(),
    );
    let run: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![1]),
            &Int32Array::from(vec![Some(9)]),
        )
        .unwrap(),
    );
    let union_list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("union-item", union.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        union,
        None,
    ));
    let run_list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("run-item", run.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        run,
        None,
    ));
    let longest = lists(vec![0, 3], vec![Some(1), Some(2), Some(3)], None);
    let run_kernel = Fixture::new(&[ty(&run_list, true)]).kernel();
    assert!(
        SelectedTableInput::try_new(
            run_kernel.contract(),
            Selection::all(1),
            &[EvaluatedArgument::Column(&run_list)],
            &RuntimeControl::default()
        )
        .is_err()
    );
    let union_list =
        crate::test_value_ingress::array_for_binding(&union_list, &ty(&union_list, true)).unwrap();
    let fixture = Fixture::new(&[ty(&union_list, true), ty(&longest, true)]);
    let kernel = fixture.kernel();
    let args = [
        EvaluatedArgument::Column(&union_list),
        EvaluatedArgument::Column(&longest),
    ];
    let mut cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(&kernel, Selection::all(1), &args),
        &RuntimeControl::default(),
    )
    .unwrap();
    // This page has already passed TableOutputPage's full selected result-type
    // checker, not merely a local Arrow constructor.
    let output = page(
        cursor
            .next(capacity(3, 1), &RuntimeControl::default())
            .unwrap(),
    );
    for (index, column) in output.columns.iter().enumerate() {
        assert!(novarocks_type_contract::arrow_data_types_exact(
            column.data_type(),
            kernel.contract().result_types()[index].data_type()
        ));
        assert_eq!(column.len(), 3);
    }
    assert_eq!(output.columns[0].logical_null_count(), 2);
    let union = output.columns[0]
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    assert_eq!(ints(union.child(0))[0], Some(7));
    assert_eq!(ints(&output.columns[1]), [Some(1), Some(2), Some(3)]);
    assert_eq!(&*output.completed_parents, [0]);
    assert!(output.eof);
    cursor.finish(&RuntimeControl::default()).unwrap();
}

#[test]
fn exact_source_class_metadata_null_promise_and_required_child_errors_are_refused() {
    let original = lists(vec![0, 0, 1], vec![Some(7)], Some(vec![false, true]));
    let fixture = Fixture::new(&[ty(&original, false)]);
    let kernel = fixture.kernel();
    let args = [EvaluatedArgument::Column(&original)];
    assert!(
        SelectedTableInput::try_new(
            kernel.contract(),
            Selection::all(2),
            &args,
            &RuntimeControl::default()
        )
        .is_err()
    );
    let rows = [1];
    let selected = Selection::try_sparse(2, &rows).unwrap();
    assert!(
        SelectedTableInput::try_new(
            kernel.contract(),
            selected,
            &args,
            &RuntimeControl::default()
        )
        .is_ok()
    );
    let wrong: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let wrong_args = [EvaluatedArgument::Column(&wrong)];
    assert!(
        SelectedTableInput::try_new(
            kernel.contract(),
            Selection::all(2),
            &wrong_args,
            &RuntimeControl::default()
        )
        .is_err()
    );
    let field = Arc::new(Field::new("changed-item", DataType::Int32, true));
    let changed: ArrayRef = Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        Arc::new(Int32Array::from(vec![7])),
        None,
    ));
    let changed_args = [EvaluatedArgument::Scalar(&changed)];
    assert!(
        SelectedTableInput::try_new(
            kernel.contract(),
            selected,
            &changed_args,
            &RuntimeControl::default()
        )
        .is_err()
    );
    let nullable_fixture = Fixture::new(&[ty(&original, true)]);
    let kernel = nullable_fixture.kernel();
    let errors = Box::from([RowDataError::new(0, "required child failed")]);
    let output = SelectedValues::try_new(
        Selection::all(2),
        original.data_type(),
        original.clone(),
        errors,
    )
    .unwrap();
    let args = [EvaluatedArgument::SelectedColumn(&output)];
    assert!(
        SelectedTableInput::try_new(
            kernel.contract(),
            Selection::all(2),
            &args,
            &RuntimeControl::default()
        )
        .is_err()
    );
    // The installed binder does not admit raw LargeList or zero arguments.
    let empty = [];
    assert!(
        fixture
            .catalog
            .metadata()
            .resolve_bound_user(
                "unnest",
                FunctionKind::Table,
                FunctionBindingRequest {
                    arguments: &empty,
                    logical_argument_count: 0,
                    expected_result_type: None
                },
                &CompileControl::default()
            )
            .is_err()
    );
}

fn compile_refusal(
    result: Result<PureCallSpecialization, FunctionSpecializationFailure>,
    cause: CompileControlError,
) {
    let matches = match result {
        Err(FunctionSpecializationFailure::Control(actual))
        | Err(FunctionSpecializationFailure::Binding(FunctionBindingError::Control(actual))) => {
            actual == cause
        }
        Err(FunctionSpecializationFailure::Kernel(actual)) => {
            actual == KernelFailure::CompileControl(cause.clone())
        }
        _ => false,
    };
    assert!(matches, "original typed compile cause was lost");
}
#[test]
fn actual_installed_compile_callbacks_preserve_three_causes_success_and_ordinary_tails() {
    let array = lists(vec![0, 1], vec![Some(7)], None);
    let fixture = Fixture::new(&[ty(&array, true)]);
    for ordinary in [false, true] {
        let run = |control: &CompileControl| {
            let mut input = fixture.input();
            if ordinary {
                input.request.logical_argument_count = 2;
            }
            fixture.catalog.prepare_fresh(
                input,
                fixture.selected.clone(),
                fixture.options(),
                control,
            )
        };
        let healthy = CompileControl::default();
        assert_eq!(run(&healthy).is_err(), ordinary);
        let trace = healthy.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in compile_causes() {
                let control = CompileControl {
                    refusal: Some((at, cause.clone())),
                    ..Default::default()
                };
                compile_refusal(run(&control), cause);
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
    let fixture = Fixture::new(&vec![ty(&array, true); 320]);
    let healthy = CompileControl::default();
    fixture.prepare(&healthy).unwrap();
    let trace = healthy.trace.lock().unwrap().clone();
    let quantum = trace
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("actual variadic 256 work boundary");
    for at in [0, quantum, trace.len() - 1] {
        for cause in compile_causes() {
            let control = CompileControl {
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            compile_refusal(fixture.prepare(&control), cause);
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn actual_next_callbacks_preserve_all_seven_causes_and_poison_future_invocation_calls() {
    for count in [3, 320] {
        let array = lists(vec![0, count], (0..count).map(Some).collect(), None);
        let fixture = Fixture::new(&[ty(&array, true)]);
        let kernel = fixture.kernel();
        let args = [EvaluatedArgument::Column(&array)];
        let create = || {
            TableEvaluationCursor::begin(
                kernel.clone(),
                input(&kernel, Selection::all(1), &args),
                &RuntimeControl::default(),
            )
            .unwrap()
        };
        let healthy = RuntimeControl::default();
        let mut cursor = create();
        let result = page(cursor.next(capacity(count as usize, 1), &healthy).unwrap());
        assert_eq!(result.columns[0].len(), count as usize);
        let trace = healthy.trace.lock().unwrap().clone();
        let indices: Vec<_> = if count == 3 {
            (0..trace.len()).collect()
        } else {
            vec![
                0,
                trace
                    .iter()
                    .position(|n| *n == 256)
                    .expect("actual 320 child-copy boundary"),
                trace.len() - 1,
            ]
        };
        for at in indices {
            for cause in runtime_causes() {
                let control = RuntimeControl {
                    refusal: Some((at, cause.clone())),
                    ..Default::default()
                };
                let mut cursor = create();
                let actual = cursor.next(capacity(count as usize, 1), &control);
                assert!(
                    matches!(&actual, Err(error) if error == &cause),
                    "count={count}, at={at}, cause={cause:?}, actual={actual:?}"
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                assert!(matches!(
                    cursor.next(capacity(1, 1), &control),
                    Err(KernelFailure::InstanceFailed)
                ));
                assert!(matches!(
                    cursor.finish(&control),
                    Err(KernelFailure::InstanceFailed)
                ));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn selected_input_begin_and_normal_finish_callbacks_keep_original_seven_typed_causes() {
    let array = lists(vec![0, 1], vec![Some(7)], None);
    let fixture = Fixture::new(&[ty(&array, true)]);
    let kernel = fixture.kernel();
    let args = [EvaluatedArgument::Column(&array)];
    let run = |control: &RuntimeControl| -> Result<(), KernelFailure> {
        let input =
            SelectedTableInput::try_new(kernel.contract(), Selection::all(1), &args, control)?;
        let mut cursor = TableEvaluationCursor::begin(kernel.clone(), input, control)?;
        let result = page(cursor.next(capacity(1, 1), control)?);
        assert!(result.eof);
        cursor.finish(control)
    };
    let healthy = RuntimeControl::default();
    run(&healthy).unwrap();
    let trace = healthy.trace.lock().unwrap().clone();
    for at in 0..trace.len() {
        for cause in runtime_causes() {
            let control = RuntimeControl {
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            let actual = run(&control);
            assert!(
                matches!(&actual, Err(error) if error == &cause),
                "at={at}, cause={cause:?}, actual={actual:?}"
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
