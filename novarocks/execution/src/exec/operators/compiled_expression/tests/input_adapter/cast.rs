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

//! Native identity CAST over the original permissive Chunk carrier.
use crate::exec::expr::fixtures::compile_checked_fragment_with_parameters;
use crate::exec::operators::compiled_expression::{
    RuntimeKernelControl, assert_original_chunk_input, chunk_evaluation_input,
};
use arrow::{
    array::{Array, BooleanArray, Int64Array},
    datatypes::DataType,
};
use novarocks_expression::testkit::{
    PassThroughCallHost, checked_constants,
    guarded_fixtures::{GUARDED_CONTROL as Control, Shape, catalogue, fixture_constant_policy},
};
use novarocks_expression::{CompiledExpressionInstance, StaticExprKind};
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_functions::{
    CastOperation, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind};
use novarocks_physical_plan::{
    ConstantPools, FragmentBuilder, FragmentId, NodeId, ResultField, ResultPort, ValueDef, ValueId,
    ValueOrigin,
};
use novarocks_type_contract::{EvaluationDemand, FunctionValueType, SemanticParameters};
use std::{collections::BTreeMap, sync::Arc};

// This is the original Column/Bare branch: an empty Values node, the
// typed-NULL input Project, and one bare Cast in the result Project.
fn identity_cast_program(ty: FunctionValueType) -> Arc<LocalProgram> {
    let functions = catalogue(Shape::Decimal);
    let fragment = FragmentId::new(213);
    let start = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let columns = [
        (ValueId::new(91), ty.clone()),
        (
            ValueId::new(7),
            FunctionValueType::new(DataType::Int64, true),
        ),
        (
            ValueId::new(333),
            FunctionValueType::new(DataType::Boolean, true),
        ),
    ];
    let mut constants = ConstantPools::empty();
    let mut builder = FragmentBuilder::new(fragment);
    builder
        .add_values(start, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = Vec::new();
    for (id, ty) in &columns {
        let value = checked_constants::null(ty, fixture_constant_policy());
        let unit = builder.unit(input).unwrap();
        let definition = unit
            .add(
                None,
                ty.clone(),
                checked_constants::kind(&mut constants, value),
            )
            .unwrap();
        let root = unit
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr { node: input, root },
            })
            .unwrap();
        projections.push((root, *id));
    }
    builder
        .add_project(
            input,
            start,
            projections.into_boxed_slice(),
            Box::from([columns[0].0, columns[1].0, columns[2].0]),
        )
        .unwrap();
    let unit = builder.unit(output).unwrap();
    let child = unit
        .add(None, ty.clone(), ExprKind::Value(columns[0].0))
        .unwrap();
    let cast = unit
        .add(
            None,
            ty.clone(),
            ExprKind::Cast {
                cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
                expr: child,
                target: ty.data_type().clone(),
            },
        )
        .unwrap();
    let root = unit
        .root(cast, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let value = builder
        .add_value(ty.clone(), ValueOrigin::Expr { node: output, root })
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(root, value)]),
            Box::from([value]),
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment,
        output: builder.construction_node(output).unwrap().output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&ty),
            name: "cast_result".into(),
            alias: None,
            value,
            ty,
        }]),
    };
    compile_checked_fragment_with_parameters(
        &functions,
        builder,
        output,
        &BTreeMap::new(),
        result,
        SemanticParameters::default(),
        constants,
    )
}

#[test]
fn identity_cast_keeps_a_required_child_binding_and_permissive_source_carrier() {
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::pipeline::operator::input_construction::{
        ConstructionFailure, try_runtime_input_construction_factory,
    };
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use arrow::array::{ArrayRef, FixedSizeBinaryArray, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Field;
    use novarocks_type_contract::ValueTypeStorageLifetime;

    let expected =
        DataType::Struct(vec![Field::new("f", DataType::FixedSizeBinary(4), false)].into());
    let ty = FunctionValueType::new(expected, true);
    let program = identity_cast_program(ty.clone());
    let ProgramNodeKind::Project { exprs, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("actual Cast Project")
    };
    let root = exprs[0];
    let unit = program.unit(root.unit).unwrap();
    assert!(
        unit.recipes()
            .cast(unit.root(root.root).unwrap().use_id)
            .unwrap()
            .is_identity()
    );
    let mut count = 0;
    for unit in program.units() {
        for (&id, invocation) in unit.control_flow().uses() {
            let definition = unit.definitions().node(invocation.definition).unwrap();
            if matches!(definition.kind(), StaticExprKind::PreparedCast { .. }) {
                let recipe = unit.recipes().cast(id).unwrap();
                assert_eq!(recipe.operation(), CastOperation::Carrier);
                assert_eq!(
                    recipe.cast_mode(),
                    Some(novarocks_type_contract::TrinoCastMode::Implicit)
                );
                count += usize::from(recipe.source_type() == &ty && recipe.result_type() == &ty);
            }
        }
    }
    assert_eq!(count, 1);
    let layout = program.graph().nodes()[1].output_layout();
    let binding = layout.value_binding_at(0).unwrap();
    let expected = binding.data_type().clone();
    let DataType::Struct(fields) = &expected else {
        panic!("frozen source Struct")
    };
    assert!(!fields[0].is_nullable());
    let actual: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(fields[0].as_ref().clone().with_nullable(true))].into(),
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(b"abcd".as_slice()), None].into_iter(),
                    4,
                )
                .unwrap(),
            )],
            Some(NullBuffer::from(vec![true, false])),
        )
        .unwrap(),
    );
    let actual_type = actual.data_type().clone();
    let columns: Vec<ArrayRef> = vec![
        actual,
        Arc::new(Int64Array::from(vec![Some(42), Some(42)])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(true)])),
    ];
    let pure = ValueTypeStorageLifetime::pure();
    let slots = columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| {
            ChunkSlotSchema::try_new_typed(
                layout.slots()[ordinal],
                Arc::new(
                    layout
                        .schema()
                        .field(ordinal)
                        .clone()
                        .with_data_type(column.data_type().clone()),
                ),
                layout.value_binding_at(ordinal).unwrap().clone(),
                None,
                pure.clone(),
            )
            .unwrap()
        })
        .collect();
    let schema = Arc::new(ChunkSchema::try_new_typed(slots, pure).unwrap());
    assert_eq!(schema.slots()[0].typed_binding(), binding);
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("permissive identity Cast source construction"),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    owner
        .observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::CreateChunkColumns { channels: 3 },
        )
        .unwrap();
    let mut chunk = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        columns,
        2,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap();
    let source_tracker = MemTracker::new_root("permissive identity Cast source payload");
    chunk.try_transfer_to(&source_tracker).unwrap();
    for selected in [vec![0, 1], vec![1]] {
        let selection = if selected.len() == 2 {
            Selection::all(2)
        } else {
            Selection::try_sparse(2, &selected).unwrap()
        };
        let host = crate::runtime::tracked_state_allocator::expression_allocation_host(
            MemTracker::new_root("permissive identity Cast result"),
        );
        let mut evaluator = CompiledExpressionInstance::try_new_with_allocator(
            unit.clone(),
            root.root,
            &Control,
            Some(host),
        )
        .unwrap();
        let adapter = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
        let input = chunk_evaluation_input(&chunk, &adapter);
        assert_original_chunk_input(&chunk, input);
        let result = evaluator.evaluate(
            input,
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        );
        let result = result.expect("an existing identity Cast preserves the legal source carrier");
        assert_eq!(result.values().data_type(), &actual_type);
        assert!(result.errors().is_empty());
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let child = values
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (ordinal, source_row) in selected.iter().copied().enumerate() {
            assert_eq!(values.is_null(ordinal), source_row == 1);
            if source_row == 0 {
                assert_eq!(child.value(ordinal), b"abcd");
            } else {
                assert!(child.is_null(ordinal));
            }
        }
    }
    drop(chunk);
    assert_eq!(source_tracker.current(), 0);
}
