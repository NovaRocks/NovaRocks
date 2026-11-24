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
//! MAP_ENTRIES lends the original key and value arrays to the declared row shape.
use super::array_literal_core::CollectionObservation;
use super::collection_arrow::{ArrowSite, arrow_failure, contract};
use crate::KernelFailure;
use arrow_array::{Array, ArrayRef, ListArray, MapArray, StructArray};
use arrow_schema::{DataType, Field};
use std::sync::Arc;
#[derive(Debug)]
pub enum MapEntriesFailure<E> {
    Operation(KernelFailure),
    Control(E),
}
/// Array admission is the same original raw downcast; no key/value decoder is involved.
pub fn project_observed<E>(
    map_arr: &ArrayRef,
    output_type: Option<&DataType>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, MapEntriesFailure<E>> {
    let map = map_arr.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        MapEntriesFailure::Operation(contract(format_args!(
            "map_entries expects MapArray, got {:?}",
            map_arr.data_type()
        )))
    })?;
    observer(CollectionObservation::OpaqueBoundary).map_err(MapEntriesFailure::Control)?;
    let operation = |source| {
        MapEntriesFailure::Operation(arrow_failure("map_entries", ArrowSite::Construct, source))
    };
    let (item, entries) = match output_type {
        Some(DataType::List(item)) => {
            let DataType::Struct(fields) = item.data_type() else {
                return Err(MapEntriesFailure::Operation(contract(
                    "map_entries result item is not ROW",
                )));
            };
            let entries = StructArray::try_new_with_length(
                fields.clone(),
                map.entries().columns().to_vec(),
                map.entries().nulls().cloned(),
                map.entries().len(),
            )
            .map_err(operation)?;
            (Arc::clone(item), entries)
        }
        None => (
            Arc::new(Field::new("item", map.entries().data_type().clone(), true)),
            map.entries().clone(),
        ),
        _ => {
            return Err(MapEntriesFailure::Operation(contract(
                "map_entries result is not ARRAY",
            )));
        }
    };
    let list = ListArray::try_new(
        item,
        map.offsets().clone(),
        Arc::new(entries),
        map.nulls().cloned(),
    )
    .map_err(operation)?;
    observer(CollectionObservation::OpaqueBoundary).map_err(MapEntriesFailure::Control)?;
    Ok(Arc::new(list))
}
/// Raw wrapper uses the same core with no runtime authority or observation.
pub fn project(
    map_arr: &ArrayRef,
    output_type: Option<&DataType>,
) -> Result<ArrayRef, KernelFailure> {
    project_observed(map_arr, output_type, &mut |_| Ok::<_, KernelFailure>(())).map_err(|failure| {
        match failure {
            MapEntriesFailure::Operation(cause) | MapEntriesFailure::Control(cause) => cause,
        }
    })
}
