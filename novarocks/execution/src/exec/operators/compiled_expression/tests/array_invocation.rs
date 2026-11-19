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

use crate::exec::{
    chunk::{Chunk, ChunkSchema},
    expr::fixtures::{author, call, compile_checked_fragment_with_parameters},
    operators::compiled_expression::{
        RuntimeKernelControl, assert_original_chunk_input, chunk_evaluation_input, evaluate_all,
    },
};
use crate::runtime::{
    mem_tracker::MemTracker, tracked_state_allocator::expression_allocation_host,
};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, StructArray},
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_expression::{
    CompiledExpressionInstance,
    testkit::{
        PassThroughCallHost, checked_constants,
        frame_fixtures::{Control, POLICY},
        register_constant,
    },
};
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_functions::{
    ConstantPool, ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument, FunctionKind,
    InstalledPureKernel, ScalarInvocationActivation as Activation, ScalarInvocationScope,
    Selection,
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_physical_plan::{
    ConstantPools, FragmentBuilder, FragmentId, NodeId, ResultField, ResultPort, ResultValueDomain,
    ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompilePhase, ControlShape, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn argument(value_type: FunctionValueType, constant: Option<ConstantValue>) -> FunctionArgument {
    FunctionArgument::Value {
        value_type,
        constant,
    }
}

fn input_type() -> FunctionValueType {
    FunctionValueType::new(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::new(Field::new("ID", DataType::Int32, true))].into()),
            true,
        ))),
        true,
    )
}
#[derive(Clone, Copy, PartialEq)]
enum SourceMode {
    Slot,
    IfThen,
}

fn original_source_program(field_name: &str, source_mode: SourceMode) -> Arc<LocalProgram> {
    let catalog =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for name in ["__array_struct_subfield", "if", "rand"] {
        let definition = catalog.definition(name, FunctionKind::Scalar).unwrap();
        let binding = definition.binding_declaration().unwrap();
        builder.register(definition.clone()).unwrap();
        for overload in binding.overloads() {
            let declaration = catalog
                .pure_overload_declaration_observed(
                    binding.function_id(),
                    definition.kind(),
                    &overload.identity,
                    &Control,
                )
                .unwrap();
            installed.push(InstalledPureKernel {
                function: binding.function_id().clone(),
                kind: definition.kind(),
                implementation: declaration.implementation().clone(),
                aggregate_state_format: None,
            });
        }
    }
    let functions = builder.seal_pure(installed).unwrap();
    let mut source_pools = ConstantPools::empty();
    let id = FragmentId::new(271);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let source_type = input_type();
    let flag_type = FunctionValueType::new(DataType::Boolean, true);
    let columns = [
        (ValueId::new(1), source_type.clone()),
        (ValueId::new(2), flag_type.clone()),
        (
            ValueId::new(3),
            FunctionValueType::new(DataType::Int64, true),
        ),
    ];
    let mut builder = FragmentBuilder::new(id);
    let mut authors = BTreeMap::new();
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = Vec::new();
    for (value, ty) in &columns {
        let expr = builder
            .unit(input)
            .unwrap()
            .add(
                None,
                ty.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(ty.clone()), POLICY),
                ),
            )
            .unwrap();
        let expr = builder
            .unit(input)
            .unwrap()
            .root(expr, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    root: expr,
                },
            })
            .unwrap();
        items.push((expr, *value));
    }
    builder
        .add_project(
            input,
            source,
            items.into_boxed_slice(),
            columns
                .iter()
                .map(|(value, _)| *value)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
        .unwrap();
    let mut data = builder
        .unit(output)
        .unwrap()
        .add(None, source_type.clone(), ExprKind::Value(columns[0].0))
        .unwrap();
    let name_type = FunctionValueType::new(DataType::Utf8, false);
    let name = if source_mode == SourceMode::Slot {
        ConstantValue::from_utf8(
            Arc::new(name_type.try_to_field("actual field name").unwrap()),
            name_type.clone(),
            field_name,
            POLICY,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap()
    } else {
        let array: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![
            "poison_not_a_field",
            field_name,
        ]));
        ConstantPool::try_new(
            Arc::new(name_type.try_to_field("actual field name").unwrap()),
            name_type.clone(),
            array.to_data(),
            POLICY,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap()
        .value(1)
        .unwrap()
    };
    let name_expr = builder
        .unit(output)
        .unwrap()
        .add(
            None,
            name_type.clone(),
            ExprKind::Constant(register_constant(&mut source_pools, &name)),
        )
        .unwrap();
    if source_mode == SourceMode::IfThen {
        let flag = builder
            .unit(output)
            .unwrap()
            .add(None, flag_type.clone(), ExprKind::Value(columns[1].0))
            .unwrap();
        let null = builder
            .unit(output)
            .unwrap()
            .add(
                None,
                source_type.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(source_type.clone()), POLICY),
                ),
            )
            .unwrap();
        data = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(flag_type.clone(), None),
                    argument(source_type.clone(), None),
                    argument(source_type.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![flag, data, null],
        );
    }
    let result_expr = call(
        &mut builder,
        &mut authors,
        author(
            &functions,
            "__array_struct_subfield",
            vec![argument(source_type, None), argument(name_type, Some(name))],
            ControlShape::Eager,
        ),
        vec![data, name_expr],
    );
    let result_type = authors[&result_expr].result();
    let expr = builder
        .unit(output)
        .unwrap()
        .root(result_expr, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: output,
                root: expr,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(expr, value)]),
            Box::from([value]),
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: id,
        output: builder.construction_node(output).unwrap().output.clone(),
        fields: Box::from([ResultField {
            domain: ResultValueDomain::Plain,
            name: "original_array_failure".into(),
            alias: None,
            value,
            ty: result_type,
        }]),
    };
    compile_checked_fragment_with_parameters(
        &functions,
        builder,
        output,
        &authors,
        result,
        SemanticParameters::try_new([]).unwrap(),
        source_pools,
    )
}

fn root(program: &LocalProgram) -> ProgramRootRef {
    let ProgramNodeKind::Project { exprs, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("actual observed Project");
    };
    exprs[0]
}

fn input(program: &LocalProgram, flags: Vec<Option<bool>>, root_null: bool) -> RecordBatch {
    let rows = flags.len();
    let struct_field = Arc::new(Field::new("ID", DataType::Int32, true));
    let values = Arc::new(StructArray::new(
        vec![struct_field].into(),
        vec![Arc::new(Int32Array::from(vec![Some(7); rows])) as ArrayRef],
        None,
    )) as ArrayRef;
    let offsets = OffsetBuffer::new(ScalarBuffer::from(
        (0..=rows)
            .map(|r| i32::try_from(r).unwrap())
            .collect::<Vec<_>>(),
    ));
    let declared_type = input_type();
    let DataType::List(item) = declared_type.data_type() else {
        unreachable!()
    };
    let list = Arc::new(ListArray::new(
        item.clone(),
        offsets,
        values,
        root_null.then(|| NullBuffer::new_null(rows)),
    )) as ArrayRef;
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            list,
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(vec![Some(1); rows])),
        ],
    )
    .unwrap()
}
fn tracked_instance(program: &Arc<LocalProgram>) -> (CompiledExpressionInstance, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("actual-array-invocation-probe");
    let host = expression_allocation_host(tracker.clone());
    (
        CompiledExpressionInstance::try_new_with_allocator(
            program.unit(root(program).unit).unwrap().clone(),
            root(program).root,
            &Control,
            Some(host),
        )
        .unwrap(),
        tracker,
    )
}
#[test]
fn scalar_invocation_case_insensitive_required_result_keeps_exact_type_and_clone_drop() {
    let program = original_source_program("id", SourceMode::Slot);
    for null in [false, true] {
        let batch = input(&program, vec![Some(true); 3], null);
        let (mut instance, tracker) = tracked_instance(&program);
        let output = evaluate_all(
            &mut instance,
            root(&program),
            &batch,
            &RuntimeKernelControl::new(Arc::default()),
        )
        .unwrap();
        assert_eq!(
            output.data_type(),
            program.graph().nodes()[2]
                .output_layout()
                .schema()
                .field(0)
                .data_type()
        );
        let list = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list.null_count(), if null { 3 } else { 0 });
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[7, 7, 7]
        );
        let clone = output.clone();
        assert!(tracker.current() > 0);
        drop(output);
        drop(instance);
        assert!(tracker.current() > 0);
        drop(clone);
        assert_eq!(tracker.current(), 0);
    }
}

fn sliced_full_child_batch(program: &LocalProgram, flags: Vec<Option<bool>>) -> RecordBatch {
    assert_eq!(flags.len(), 2);
    let structure = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("ID", DataType::Int32, true))].into(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50]))],
        None,
    )) as ArrayRef;
    let DataType::List(item) = input_type().data_type().clone() else {
        unreachable!()
    };
    // These are real sliced offsets and unused children, not a prediction from
    // the implementation's selected output or a normalized fixture.
    let list = Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![2i32, 4, 5])),
        structure,
        None,
    ));
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            list,
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(vec![1, 1])),
        ],
    )
    .unwrap()
}

#[test]
fn original_domain_mixed_and_selected_root_copies_keep_finite_source_until_last_child_buffer() {
    for selected in [false, true] {
        let program = original_source_program("ID", SourceMode::IfThen);
        let batch = sliced_full_child_batch(&program, vec![Some(true), Some(false)]);
        let schema =
            ChunkSchema::from_compiled_layout(program.graph().nodes()[1].output_layout()).unwrap();
        let mut chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
        let source = MemTracker::new_root("actual-mixed-source-custody");
        chunk.try_transfer_to(&source).unwrap();
        let bytes = source.current();
        let (mut frame, _) = tracked_instance(&program);
        let selected_rows = [0];
        let rows = if selected {
            Selection::try_sparse(2, &selected_rows).unwrap()
        } else {
            Selection::all(2)
        };
        let visits = Arc::new(AtomicUsize::new(0));
        let seen = visits.clone();
        let mut adapter = RuntimeKernelControl::new(Arc::default());
        adapter.observe_input_adapter(Arc::new(move |chunk, input| {
            assert_original_chunk_input(chunk, input);
            seen.fetch_add(1, Ordering::Relaxed);
        }));
        let result = frame
            .evaluate(
                chunk_evaluation_input(&chunk, &adapter),
                ScalarInvocationScope::new(rows, rows),
                Activation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let list = result
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(30), Some(40)]
        );
        assert_eq!(
            list.value_offsets(),
            if selected {
                &[0, 2][..]
            } else {
                &[0, 2, 2][..]
            }
        );
        let last = list.values().to_data().buffers()[0].clone();
        assert_eq!(visits.load(Ordering::Relaxed), 1);
        drop((result, chunk, frame));
        assert_eq!(
            source.current(),
            bytes,
            "copied/assembled output must retain the original finite chain"
        );
        std::thread::spawn(move || drop(last)).join().unwrap();
        assert_eq!(source.current(), 0);
        assert_eq!(source.allocated(), source.deallocated());
    }
}
