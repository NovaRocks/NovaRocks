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

use crate::record_geometry::{
    GeometryError, GeometryProfile, GeometryWork, preflight_record_batch,
};
use crate::{
    BatchRecipe, CodecError, DecodeKind, RecipeField, RepresentationPolicy, UnitObserver,
    decode_unit, encode_unit,
};
use arrow::{
    array::{
        Array, ArrayRef, DictionaryArray, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
        StructArray, new_empty_array, types::Int32Type,
    },
    buffer::Buffer,
    datatypes::{DataType, Field, FieldRef, Schema},
};
use novarocks_type_contract::{
    CarrierNullabilityPolicy, CompileControlError, FunctionValueType, ValueTypeError,
};
use std::{alloc::Layout, collections::HashMap, sync::Arc};

#[derive(Debug)]
enum Refusal {
    Cancelled,
    Allocations,
    SourceAmplification,
    Geometry(GeometryError),
    Control(CompileControlError),
    Type(ValueTypeError),
}
impl std::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("test control cancelled"),
            Self::Allocations => formatter.write_str("test allocation refused"),
            Self::SourceAmplification => {
                formatter.write_str("test source occurrence budget exhausted")
            }
            Self::Geometry(error) => error.fmt(formatter),
            Self::Control(error) => error.fmt(formatter),
            Self::Type(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for Refusal {}
impl From<GeometryError> for Refusal {
    fn from(e: GeometryError) -> Self {
        Self::Geometry(e)
    }
}
impl From<CompileControlError> for Refusal {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<ValueTypeError> for Refusal {
    fn from(e: ValueTypeError) -> Self {
        Self::Type(e)
    }
}
#[derive(Default)]
struct Observed {
    steps: usize,
    allocations: usize,
    metadata_bytes: usize,
    source_occurrences: usize,
    source_bytes: usize,
    decode_calls: usize,
    cancel: bool,
    refuse_allocations: bool,
    max_source: Option<usize>,
    copied_bodies: Vec<Buffer>,
    cancel_after_body: bool,
}
impl UnitObserver for Observed {
    type Error = Refusal;
    fn before_type_scratch(&mut self, work_upper: usize) -> Result<(), Refusal> {
        assert_eq!(
            work_upper,
            novarocks_type_contract::owned_resources::type_validation::scratch_work_upper_bound()
        );
        self.checkpoint()
    }
    fn project_carrier(
        &mut self,
        _: usize,
        _: &ArrayRef,
        _: &DataType,
    ) -> Result<ArrayRef, Refusal> {
        Err(Refusal::Allocations)
    }
    fn checkpoint(&mut self) -> Result<(), Refusal> {
        self.steps += 1;
        if self.cancel {
            Err(Refusal::Cancelled)
        } else {
            Ok(())
        }
    }
    fn before_allocation(&mut self, layout: Layout) -> Result<(), Refusal> {
        self.checkpoint()?;
        self.allocations += usize::from(layout.size() != 0);
        if self.refuse_allocations {
            Err(Refusal::Allocations)
        } else {
            Ok(())
        }
    }
    fn before_recipe(&mut self, fields: &[RecipeField<'_>]) -> Result<(), Refusal> {
        self.checkpoint()?;
        self.source_occurrences += fields.len();
        Ok(())
    }
    fn before_unit_recipe(&mut self, recipe: &BatchRecipe, tags: &[u8]) -> Result<(), Refusal> {
        self.checkpoint()?;
        assert_eq!(tags.len(), recipe.schema().fields().len());
        self.source_occurrences += tags.len();
        Ok(())
    }
    fn before_metadata_verification(&mut self, metadata: &[u8]) -> Result<(), Refusal> {
        self.checkpoint()?;
        self.metadata_bytes += metadata.len();
        Ok(())
    }
    fn before_encode_batch(&mut self, _: &BatchRecipe, batch: &RecordBatch) -> Result<(), Refusal> {
        self.checkpoint()?;
        // Sum actual column occurrences, deliberately retaining alias repetition.
        self.source_occurrences += batch.num_columns();
        let bytes = batch
            .columns()
            .iter()
            .map(|a| a.get_buffer_memory_size())
            .sum::<usize>();
        self.source_bytes += bytes;
        if self.max_source.is_some_and(|max| bytes > max) {
            Err(Refusal::SourceAmplification)
        } else {
            Ok(())
        }
    }
    fn before_decode_message(
        &mut self,
        schema: &Schema,
        batch: arrow::ipc::RecordBatch<'_>,
        body: &[u8],
    ) -> Result<(), Refusal> {
        let fields = schema
            .fields()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>();
        preflight_record_batch(batch, body, &fields, None, GeometryProfile::Online, self)?;
        Ok(())
    }
    fn copy_body(&mut self, body: &[u8]) -> Result<Buffer, Refusal> {
        self.before_allocation(
            Layout::from_size_align(body.len().max(1).next_multiple_of(64), 64).unwrap(),
        )?;
        let buffer = Buffer::from_slice_ref(body);
        self.copied_bodies.push(buffer.clone());
        if self.cancel_after_body {
            self.cancel = true;
        }
        Ok(buffer)
    }
    fn before_arrow_decode(
        &mut self,
        _: &Schema,
        _: arrow::ipc::RecordBatch<'_>,
        _: &Buffer,
        dictionaries: &HashMap<i64, ArrayRef>,
        kind: DecodeKind,
    ) -> Result<(), Refusal> {
        self.checkpoint()?;
        self.decode_calls += 1;
        if let DecodeKind::Dictionary { id } = kind {
            assert_eq!(id as usize, dictionaries.len());
        }
        Ok(())
    }
}
impl GeometryWork for Observed {
    type Error = Refusal;
    fn has_admission(&self) -> bool {
        true
    }
    fn step(&mut self) -> Result<(), Refusal> {
        self.checkpoint()
    }
    fn flush(&mut self) -> Result<(), Refusal> {
        self.checkpoint()
    }
    fn geometry_request(&mut self, layout: Layout, construction: bool) -> Result<(), Refusal> {
        if construction {
            self.before_allocation(layout)
        } else {
            self.checkpoint()
        }
    }
    fn source_work(&mut self) -> Result<(), Refusal> {
        self.checkpoint()
    }
    fn validate_root(&mut self, field: &Field) -> Result<(), Refusal> {
        novarocks_type_contract::validate_arrow_layout_structure_with_profile_observed(
            field.data_type(),
            novarocks_type_contract::ArrowTypeProfile::Value,
            |_| self.checkpoint(),
        )
    }
}
fn recipe(
    fields: &[FieldRef],
    policies: &[RepresentationPolicy],
    observer: &mut Observed,
) -> BatchRecipe {
    let values = fields
        .iter()
        .map(|f| FunctionValueType::new(f.data_type().clone(), f.is_nullable()))
        .collect::<Vec<_>>();
    let inputs = fields
        .iter()
        .zip(&values)
        .zip(policies)
        .map(|((field, value_type), representation)| RecipeField {
            field,
            value_type,
            representation: *representation,
            nullability: CarrierNullabilityPolicy::Exact,
        })
        .collect::<Vec<_>>();
    BatchRecipe::try_new(&inputs, observer).unwrap()
}
fn exact(fields: &[FieldRef], observer: &mut Observed) -> BatchRecipe {
    recipe(
        fields,
        &vec![RepresentationPolicy::Exact; fields.len()],
        observer,
    )
}
fn batch(fields: Vec<FieldRef>, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn multiple_columns_round_trip_and_root_names_are_local() {
    let fields = vec![
        Arc::new(Field::new("sender_a", DataType::Int64, true)),
        Arc::new(Field::new("sender_b", DataType::Utf8, true)),
    ];
    let input = batch(
        fields.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])),
        ],
    );
    let mut observer = Observed::default();
    let sender = exact(&fields, &mut observer);
    let receiver_fields = vec![
        Arc::new(Field::new("other_x", DataType::Int64, true)),
        Arc::new(Field::new("other_y", DataType::Utf8, true)),
    ];
    let receiver = exact(&receiver_fields, &mut observer);
    assert_eq!(sender.digest(), receiver.digest());
    let wire = encode_unit(&sender, &input, &mut observer).unwrap();
    let output = decode_unit(&receiver, &wire, &mut observer).unwrap();
    assert_eq!(output.num_rows(), 3);
    assert_eq!(output.columns(), input.columns());
    assert!(observer.metadata_bytes > 0);
    assert_eq!(observer.decode_calls, 1);
}
#[test]
fn permissive_carrier_flags_do_not_change_the_frozen_logical_identity() {
    let declared = DataType::Struct(
        vec![Field::new(
            "named",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
            false,
        )]
        .into(),
    );
    let permissive = DataType::Struct(
        vec![Field::new(
            "named",
            DataType::List(Arc::new(Field::new("renamed_role", DataType::Int64, true))),
            true,
        )]
        .into(),
    );
    let required = FunctionValueType::new(declared.clone(), false);
    let logically_nullable = FunctionValueType::new(permissive.clone(), true);
    let build = |carrier, value_type: &FunctionValueType| {
        let field = Arc::new(Field::new("local", carrier, true));
        BatchRecipe::try_new(
            &[RecipeField {
                field: &field,
                value_type,
                representation: RepresentationPolicy::Exact,
                nullability: CarrierNullabilityPolicy::Permissive,
            }],
            &mut Observed::default(),
        )
        .unwrap()
    };
    let strict_carrier = build(declared, &required);
    let loose_carrier = build(permissive.clone(), &required);
    assert_eq!(strict_carrier.digest(), loose_carrier.digest());
    assert_eq!(strict_carrier.schema(), loose_carrier.schema());
    assert_eq!(strict_carrier.declarations(), &[required]);
    let other_declaration = build(permissive, &logically_nullable);
    assert_ne!(strict_carrier.digest(), other_declaration.digest());
}
#[test]
fn zero_columns_preserve_empty_and_nonempty_row_counts_without_a_marker_column() {
    let mut observer = Observed::default();
    let recipe = exact(&[], &mut observer);
    for rows in [0, 1, 7] {
        let input = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .unwrap();
        let wire = encode_unit(&recipe, &input, &mut observer).unwrap();
        let output = decode_unit(&recipe, &wire, &mut observer).unwrap();
        assert_eq!(output.num_columns(), 0);
        assert_eq!(output.num_rows(), rows);
    }
}
#[test]
fn empty_partition_keeps_declared_columns_and_does_not_change_the_recipe() {
    let fields = vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::new(Field::new("text", DataType::Utf8, true)),
    ];
    let input = batch(
        fields.clone(),
        fields
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect(),
    );
    let mut observer = Observed::default();
    let recipe = exact(&fields, &mut observer);
    let wire = encode_unit(&recipe, &input, &mut observer).unwrap();
    let output = decode_unit(&recipe, &wire, &mut observer).unwrap();
    assert_eq!(output.num_rows(), 0);
    assert_eq!(output.columns(), input.columns());

    let count_only = exact(&[], &mut observer);
    assert!(matches!(
        encode_unit(&count_only, &input, &mut observer),
        Err(CodecError::Invalid("unit columns differ from recipe"))
    ));
    assert!(matches!(
        decode_unit(&count_only, &wire, &mut observer),
        Err(CodecError::Invalid(
            "Arrow unit recipe digest or version differs"
        ))
    ));
}
#[test]
fn complete_dictionaries_are_independent_in_every_unit() {
    let field = Arc::new(Field::new("s", DataType::Utf8, true));
    let mut observer = Observed::default();
    let recipe = recipe(
        &[field],
        &[RepresentationPolicy::CanonicalOrInt32Utf8Dictionary],
        &mut observer,
    );
    for values in [
        vec![Some("a"), Some("a"), None],
        vec![Some("b"), None, Some("c")],
    ] {
        let dictionary = DictionaryArray::<Int32Type>::from_iter(values);
        let input = batch(
            vec![Arc::new(Field::new(
                "local",
                dictionary.data_type().clone(),
                true,
            ))],
            vec![Arc::new(dictionary)],
        );
        let wire = encode_unit(&recipe, &input, &mut observer).unwrap();
        let output = decode_unit(&recipe, &wire, &mut observer).unwrap();
        assert_eq!(output.columns(), input.columns());
    }
    let plain = batch(
        vec![Arc::new(Field::new("local", DataType::Utf8, true))],
        vec![Arc::new(StringArray::from(vec!["plain"]))],
    );
    let wire = encode_unit(&recipe, &plain, &mut observer).unwrap();
    assert_eq!(
        decode_unit(&recipe, &wire, &mut observer)
            .unwrap()
            .columns(),
        plain.columns()
    );
}
#[test]
fn exact_policy_refuses_runtime_dictionary_and_invalid_unit_tags() {
    let field = Arc::new(Field::new("s", DataType::Utf8, true));
    let mut observer = Observed::default();
    let recipe = exact(&[field], &mut observer);
    let dictionary = DictionaryArray::<Int32Type>::from_iter([Some("a")]);
    let input = batch(
        vec![Arc::new(Field::new(
            "local",
            dictionary.data_type().clone(),
            true,
        ))],
        vec![Arc::new(dictionary)],
    );
    assert!(matches!(
        encode_unit(&recipe, &input, &mut observer),
        Err(CodecError::Invalid(_))
    ));
    let plain = batch(
        vec![Arc::new(Field::new("local", DataType::Utf8, true))],
        vec![Arc::new(StringArray::from(vec!["plain"]))],
    );
    let mut wire = encode_unit(&recipe, &plain, &mut observer).unwrap();
    wire[36] = 1;
    assert!(matches!(
        decode_unit(&recipe, &wire, &mut observer),
        Err(CodecError::Invalid(_))
    ));
}
#[test]
fn nested_dictionary_ids_are_reconstructed_in_postorder() {
    let dictionary = Arc::new(DictionaryArray::<Int32Type>::from_iter([
        Some("a"),
        None,
        Some("b"),
    ])) as ArrayRef;
    let child = Arc::new(Field::new("inner", dictionary.data_type().clone(), true));
    let structure =
        Arc::new(StructArray::new(vec![child].into(), vec![dictionary], None)) as ArrayRef;
    let field = Arc::new(Field::new("root", structure.data_type().clone(), true));
    let mut observer = Observed::default();
    let recipe = exact(&[field.clone()], &mut observer);
    let input = batch(vec![field], vec![structure]);
    let wire = encode_unit(&recipe, &input, &mut observer).unwrap();
    let output = decode_unit(&recipe, &wire, &mut observer).unwrap();
    assert_eq!(output.columns(), input.columns());
}
#[test]
fn sliced_struct_with_parent_null_round_trips() {
    let child = Arc::new(Field::new("value", DataType::Int64, true));
    let structure = StructArray::new(
        vec![child].into(),
        vec![Arc::new(Int64Array::from(vec![10, 20, 30, 40]))],
        Some(arrow::buffer::NullBuffer::from(vec![
            true, false, true, true,
        ])),
    )
    .slice(1, 2);
    let field = Arc::new(Field::new("r", structure.data_type().clone(), true));
    let mut observer = Observed::default();
    let recipe = exact(&[field.clone()], &mut observer);
    let input = batch(vec![field], vec![Arc::new(structure)]);
    let wire = encode_unit(&recipe, &input, &mut observer).unwrap();
    assert_eq!(
        decode_unit(&recipe, &wire, &mut observer)
            .unwrap()
            .columns(),
        input.columns()
    );
}
#[test]
fn digest_trailing_message_and_geometry_mismatches_fail_before_array_decode() {
    let fields = vec![Arc::new(Field::new("n", DataType::Int64, true))];
    let mut observer = Observed::default();
    let recipe = exact(&fields, &mut observer);
    let input = batch(fields, vec![Arc::new(Int64Array::from(vec![1, 2]))]);
    let wire = encode_unit(&recipe, &input, &mut observer).unwrap();
    let mut wrong = wire.clone();
    wrong[4] ^= 1;
    assert!(decode_unit(&recipe, &wrong, &mut observer).is_err());
    assert_eq!(observer.decode_calls, 0);
    let mut trailing = wire.clone();
    trailing.extend_from_slice(&wire[37..]);
    assert!(decode_unit(&recipe, &trailing, &mut observer).is_err());
    assert_eq!(observer.decode_calls, 0);
    // Same frozen declaration but substitute an encoded two-field batch behind
    // its prefix; the geometry author must catch the extra nodes/buffers.
    let field = Arc::new(Field::new("v", DataType::Int64, true));
    let wider_recipe = exact(&[field.clone(), field.clone()], &mut observer);
    let wider = batch(
        vec![field.clone(), field],
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![3, 4])),
        ],
    );
    let wider_wire = encode_unit(&wider_recipe, &wider, &mut observer).unwrap();
    let mut malformed = wire[..37].to_vec();
    malformed.extend_from_slice(&wider_wire[38..]);
    assert!(matches!(
        decode_unit(&recipe, &malformed, &mut observer),
        Err(CodecError::Observer(Refusal::Geometry(_)))
    ));
    assert_eq!(observer.decode_calls, 0);
}
#[test]
fn cancellation_and_alias_amplification_are_original_refusals() {
    let field = Arc::new(Field::new("x", DataType::Int64, true));
    let mut observer = Observed::default();
    let recipe = exact(&[field.clone(), field.clone()], &mut observer);
    let array = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let weak = Arc::downgrade(&array);
    let one = array.get_buffer_memory_size();
    let input = batch(
        vec![field.clone(), field],
        vec![array.clone(), array.clone()],
    );
    drop(array);
    observer.max_source = Some(one);
    assert!(matches!(
        encode_unit(&recipe, &input, &mut observer),
        Err(CodecError::Observer(Refusal::SourceAmplification))
    ));
    observer.max_source = None;
    observer.cancel = true;
    assert!(matches!(
        encode_unit(&recipe, &input, &mut observer),
        Err(CodecError::Observer(Refusal::Cancelled))
    ));
    drop(input);
    assert!(weak.upgrade().is_none());
}
#[test]
fn depth_64_round_trips_and_65_is_rejected() {
    let mut array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    for _ in 1..64 {
        let field = Arc::new(Field::new("n", array.data_type().clone(), true));
        array = Arc::new(StructArray::new(vec![field].into(), vec![array], None));
    }
    let field = Arc::new(Field::new("root", array.data_type().clone(), true));
    let mut observer = Observed::default();
    let recipe = exact(&[field.clone()], &mut observer);
    let input = batch(vec![field.clone()], vec![array]);
    let wire = encode_unit(&recipe, &input, &mut observer).unwrap();
    assert_eq!(
        decode_unit(&recipe, &wire, &mut observer)
            .unwrap()
            .columns(),
        input.columns()
    );
    let deeper = Arc::new(Field::new(
        "root",
        DataType::Struct(vec![field].into()),
        true,
    ));
    let value = FunctionValueType::new(DataType::Int64, true);
    assert!(matches!(
        BatchRecipe::try_new(
            &[RecipeField {
                field: &deeper,
                value_type: &value,
                representation: RepresentationPolicy::Exact,
                nullability: CarrierNullabilityPolicy::Exact,
            }],
            &mut observer
        ),
        Err(CodecError::ValueType(ValueTypeError::TooDeep))
    ));
}

#[test]
fn logical_depth_64_maps_keep_entries_and_key_nodes() {
    use arrow::array::{Int32Array, MapArray};
    use arrow::buffer::OffsetBuffer;
    let mut array = Arc::new(Int64Array::from(vec![17])) as ArrayRef;
    for _ in 1..64 {
        let entries = StructArray::new(
            vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", array.data_type().clone(), true),
            ]
            .into(),
            vec![Arc::new(Int32Array::from(vec![1])), array],
            None,
        );
        let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
        array = Arc::new(MapArray::new(
            field,
            OffsetBuffer::from_lengths([1]),
            entries,
            None,
            false,
        ));
    }
    let field = Arc::new(Field::new("root", array.data_type().clone(), true));
    let mut observer = Observed::default();
    let recipe = exact(&[field.clone()], &mut observer);
    let input = batch(vec![field], vec![array]);
    let wire = encode_unit(&recipe, &input, &mut observer).unwrap();
    let output = decode_unit(&recipe, &wire, &mut observer).unwrap();
    assert_eq!(input.columns(), output.columns());
    let mut nodes = 0;
    novarocks_type_contract::validate_arrow_layout_structure_with_profile_observed::<Refusal>(
        output.column(0).data_type(),
        novarocks_type_contract::ArrowTypeProfile::Value,
        |visit| {
            nodes += usize::from(matches!(
                visit,
                novarocks_type_contract::ValueTypeVisit::TypeNode(_)
            ));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        nodes, 190,
        "Map key leaves and entries scaffolds remain real nodes"
    );
}

#[test]
fn prepared_carrier_cannot_change_declared_domain_parameters_or_child_identity() {
    let value = FunctionValueType::new(
        DataType::Struct(
            vec![Arc::new(Field::new(
                "child",
                DataType::Decimal128(20, 4),
                false,
            ))]
            .into(),
        ),
        true,
    );
    for (physical, accepted) in [
        (
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "child",
                    DataType::Decimal128(20, 4),
                    true,
                ))]
                .into(),
            ),
            true,
        ),
        (
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "renamed",
                    DataType::Decimal128(20, 4),
                    true,
                ))]
                .into(),
            ),
            false,
        ),
        (
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "child",
                    DataType::Decimal128(20, 3),
                    true,
                ))]
                .into(),
            ),
            false,
        ),
    ] {
        let field = Arc::new(Field::new("root", physical, true));
        let result = BatchRecipe::try_new(
            &[RecipeField {
                field: &field,
                value_type: &value,
                representation: RepresentationPolicy::Exact,
                nullability: CarrierNullabilityPolicy::Permissive,
            }],
            &mut Observed::default(),
        );
        assert_eq!(result.is_ok(), accepted);
    }
}

// The original Buffer allocation is observed directly, including independent
// ArrayRef aliases; an encoder/decoder return alone is not its actual exit.
#[test]
fn decoded_body_lives_until_the_last_array_alias_really_exits() {
    let field = Arc::new(Field::new("n", DataType::Int64, false));
    let input = batch(
        vec![field.clone()],
        vec![Arc::new(Int64Array::from(vec![7, 11]))],
    );
    let mut sender = Observed::default();
    let recipe = exact(&[field], &mut sender);
    let wire = encode_unit(&recipe, &input, &mut sender).unwrap();
    let mut receiver = Observed::default();
    let decoded = decode_unit(&recipe, &wire, &mut receiver).unwrap();
    assert_eq!(receiver.copied_bodies.len(), 1);
    assert!(receiver.copied_bodies[0].strong_count() > 1);
    let alias = decoded.column(0).clone();
    drop(decoded);
    assert!(receiver.copied_bodies[0].strong_count() > 1);
    assert_eq!(
        alias
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1),
        11
    );
    drop(alias);
    assert_eq!(receiver.copied_bodies[0].strong_count(), 1);
}

#[test]
fn cancellation_after_body_creation_releases_the_actual_decode_buffer() {
    let field = Arc::new(Field::new("n", DataType::Int64, false));
    let input = batch(
        vec![field.clone()],
        vec![Arc::new(Int64Array::from(vec![7]))],
    );
    let mut sender = Observed::default();
    let recipe = exact(&[field], &mut sender);
    let wire = encode_unit(&recipe, &input, &mut sender).unwrap();
    let mut receiver = Observed {
        cancel_after_body: true,
        ..Observed::default()
    };
    assert!(matches!(
        decode_unit(&recipe, &wire, &mut receiver),
        Err(CodecError::Observer(Refusal::Cancelled))
    ));
    assert_eq!(receiver.decode_calls, 0);
    assert_eq!(receiver.copied_bodies.len(), 1);
    assert_eq!(receiver.copied_bodies[0].strong_count(), 1);
}
