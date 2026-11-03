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

use arrow::{
    array::{ArrayRef, RecordBatch},
    buffer::Buffer,
    datatypes::{Field, Schema, SchemaRef},
};
use novarocks_arrow_batch_codec::{
    BatchRecipe, DecodeKind, RecipeField, RepresentationPolicy, UnitObserver,
};
use novarocks_native_adapter::root_cow_selection_codec::CowSelectionRecipe;
use std::{alloc::Layout, collections::HashMap, sync::Arc};

#[derive(Default)]
struct Observer {
    bytes: usize,
}
impl UnitObserver for Observer {
    type Error = &'static str;
    fn before_type_scratch(&mut self, work_upper: usize) -> Result<(), Self::Error> {
        assert_eq!(
            work_upper,
            novarocks_type_contract::owned_resources::type_validation::scratch_work_upper_bound()
        );
        self.checkpoint()
    }
    fn checkpoint(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_allocation(&mut self, layout: Layout) -> Result<(), Self::Error> {
        self.bytes = self
            .bytes
            .checked_add(layout.size())
            .ok_or("test recipe allocation overflow")?;
        Ok(())
    }
    fn before_recipe(&mut self, _: &[RecipeField<'_>]) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_metadata_verification(&mut self, _: &[u8]) -> Result<(), Self::Error> {
        Err("recipe fixture cannot verify online IPC")
    }
    fn copy_body(&mut self, _: &[u8]) -> Result<Buffer, Self::Error> {
        Err("recipe fixture cannot decode online IPC")
    }
    fn before_arrow_decode(
        &mut self,
        _: &Schema,
        _: arrow::ipc::RecordBatch<'_>,
        _: &Buffer,
        _: &HashMap<i64, ArrayRef>,
        _: DecodeKind,
    ) -> Result<(), Self::Error> {
        Err("recipe fixture cannot decode online IPC")
    }
    fn before_unit_recipe(&mut self, _: &BatchRecipe, _: &[u8]) -> Result<(), Self::Error> {
        Err("recipe fixture cannot select online IPC")
    }
    fn project_carrier(
        &mut self,
        _: usize,
        _: &ArrayRef,
        _: &arrow::datatypes::DataType,
    ) -> Result<ArrayRef, Self::Error> {
        Err("recipe fixture cannot project an online carrier")
    }
    fn before_encode_batch(&mut self, _: &BatchRecipe, _: &RecordBatch) -> Result<(), Self::Error> {
        Err("recipe fixture cannot encode online IPC")
    }
    fn before_decode_message(
        &mut self,
        _: &Schema,
        _: arrow::ipc::RecordBatch<'_>,
        _: &[u8],
    ) -> Result<(), Self::Error> {
        Err("recipe fixture cannot decode online IPC")
    }
}
pub fn recipe(schema: &SchemaRef) -> Arc<CowSelectionRecipe> {
    let declarations = schema
        .fields()
        .iter()
        .map(|field| {
            novarocks_type_contract::FunctionValueType::new(
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect::<Vec<_>>();
    let fields = declarations
        .iter()
        .map(|value| Arc::new(Field::new("", value.data_type().clone(), value.nullable())))
        .collect::<Vec<_>>();
    let inputs = fields
        .iter()
        .zip(&declarations)
        .map(|(field, value_type)| RecipeField {
            field,
            value_type,
            representation: RepresentationPolicy::Exact,
            nullability: novarocks_type_contract::CarrierNullabilityPolicy::Permissive,
        })
        .collect::<Vec<_>>();
    let mut observer = Observer::default();
    let batch = BatchRecipe::try_new(&inputs, &mut observer).unwrap();
    assert!(observer.bytes != 0);
    Arc::new(CowSelectionRecipe::try_new(batch, declarations).unwrap())
}
pub fn normalized(batch: &RecordBatch, recipe: &CowSelectionRecipe) -> RecordBatch {
    use novarocks_value_arrays::{
        decode::ValueConversionContext,
        hydrate::{HydrateOptions, HydratePlan, HydrateProjection},
    };
    CowSelectionRecipe::validate_supported(batch.schema_ref()).unwrap();
    assert_eq!(batch.num_columns(), recipe.declarations().len());
    assert_eq!(batch.num_columns(), recipe.schema().fields().len());
    let mut context = ValueConversionContext::new(0);
    let columns = batch
        .columns()
        .iter()
        .zip(recipe.declarations())
        .zip(recipe.schema().fields())
        .map(|((source, binding), carrier)| {
            let mut requested = 0usize;
            let plan = HydratePlan::plan_with_options_observed::<std::convert::Infallible>(
                source,
                binding.typed_binding(),
                HydrateOptions {
                    dictionary_value_layout: None,
                    projection: HydrateProjection::FrozenNamesAndNullability,
                },
                &mut context,
                &mut |_, _| false,
                &mut |layout| {
                    requested += layout.size();
                    Ok(())
                },
                &mut |_| Ok(()),
                &mut |_| Ok(()),
            )
            .unwrap();
            plan.require_complete().unwrap();
            let logical = plan
                .execute_observed(
                    &mut context,
                    &mut |layout| {
                        requested += layout.size();
                        Ok::<_, std::convert::Infallible>(())
                    },
                    &mut |_| Ok(()),
                    &mut |_| Ok(()),
                )
                .unwrap();
            // Hydrate above is the logical declaration's original validator.
            // Only then rebuild metadata to the recipe's permissive carrier;
            // this representation step cannot widen the logical promise.
            use novarocks_functions::array_projection::{
                ArrayProjectionFailure, observe_array_normalization_construction,
                rebuild_array_observed,
            };
            let mut opaque_bytes = 0usize;
            let mut projection_work = 0usize;
            let projected = rebuild_array_observed::<ArrayProjectionFailure, _>(
                &logical,
                carrier.data_type(),
                &mut |step| {
                    observe_array_normalization_construction(
                        step,
                        &mut |layout| {
                            requested = requested
                                .checked_add(layout.size())
                                .ok_or(ArrayProjectionFailure::RequestArithmetic)?;
                            Ok(())
                        },
                        &mut |bound| {
                            opaque_bytes = opaque_bytes
                                .checked_add(bound.bytes_upper)
                                .ok_or(ArrayProjectionFailure::RequestArithmetic)?;
                            Ok(())
                        },
                        &mut |units| {
                            projection_work = projection_work
                                .checked_add(units)
                                .ok_or(ArrayProjectionFailure::RequestArithmetic)?;
                            Ok(())
                        },
                    )
                },
            )
            .unwrap();
            assert!(
                projection_work > 0,
                "metadata projection observes the original array"
            );
            requested
                .checked_add(opaque_bytes)
                .expect("test normalization construction bound");
            projected
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::clone(recipe.schema()), columns).unwrap()
}
