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

use super::{HydrateEvent, HydrateObserver, HydratePathStep, child, invalid, reserve, tree::Node};
use crate::{
    ValueError, ValueFailure, ValueViolation,
    access::ByteSource,
    arrow_raw::{self, StructuralRef},
    build::{ValueBuilder, WriteFailure},
    control::{Growth, Work},
    decode::ValueConversionContext,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::BorrowedTypedValueBinding;
use std::sync::Arc;
impl Node {
    pub(super) fn execute<E>(
        &self,
        binding: BorrowedTypedValueBinding<'_>,
        context: &mut ValueConversionContext,
        path: &mut [HydratePathStep],
        depth: usize,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut HydrateObserver<'_, E>,
    ) -> Result<ArrayRef, ValueFailure<E>> {
        work(1).map_err(ValueFailure::Caller)?;
        if !self.rebuild {
            return Ok(Arc::clone(&self.source));
        }
        if self.convert {
            return self.materialize(binding, context, &path[..depth], growth, work, observe);
        }
        if self.children.is_empty() {
            return self.normalize_null_wrapper(
                Arc::clone(&self.source),
                binding.data_type(),
                &path[..depth],
                observe,
            );
        }
        let mut children = Vec::new();
        reserve(&mut children, self.children.len(), growth)?;
        for (index, node) in self.children.iter().enumerate() {
            path[depth] = match arrow_raw::structural_view(&self.source)? {
                StructuralRef::Struct { .. } => HydratePathStep::StructField(index),
                StructuralRef::Union { fields, .. } => {
                    HydratePathStep::UnionField(fields.iter().nth(index).ok_or_else(invalid)?.0)
                }
                StructuralRef::Map { .. } => {
                    if index == 0 {
                        HydratePathStep::MapKey
                    } else {
                        HydratePathStep::MapValue
                    }
                }
                _ => HydratePathStep::ListItem,
            };
            children.push(node.execute(
                child(binding, index)?,
                context,
                path,
                depth + 1,
                growth,
                work,
                observe,
            )?);
        }
        let changed = self
            .children
            .iter()
            .zip(&children)
            .any(|(old, new)| !Arc::ptr_eq(&old.source, new));
        if !self.clear_nulls
            && !changed
            && !local_metadata_changes(self.source.data_type(), binding.data_type())?
        {
            return Ok(Arc::clone(&self.source));
        }
        observe(HydrateEvent::RebuildType {
            path: &path[..depth],
            source: binding.data_type(),
        })
        .map_err(ValueFailure::Caller)?;
        observe_fields(binding.data_type(), &children, &path[..depth], observe)?;
        let mut child_types = Vec::new();
        reserve(&mut child_types, children.len(), growth)?;
        for child in &children {
            child_types.push(child.data_type().clone());
        }
        let target = arrow_raw::metadata_free_type_rebuild(
            self.source.data_type(),
            binding.data_type(),
            &child_types,
        )?;
        observe(HydrateEvent::RebuildArray {
            path: &path[..depth],
            source: &self.source,
            target: &target,
        })
        .map_err(ValueFailure::Caller)?;
        if matches!(target, DataType::Map(..)) {
            observe(HydrateEvent::MapEntries {
                path: &path[..depth],
                source: &self.source,
            })
            .map_err(ValueFailure::Caller)?;
        }
        let result = arrow_raw::rebuild_structural(&self.source, &target, children)?;
        observe(HydrateEvent::OpaqueOperationFinished).map_err(ValueFailure::Caller)?;
        self.normalize_null_wrapper(result, &target, &path[..depth], observe)
    }
    fn normalize_null_wrapper<E>(
        &self,
        array: ArrayRef,
        target: &DataType,
        path: &[HydratePathStep],
        observe: &mut HydrateObserver<'_, E>,
    ) -> Result<ArrayRef, ValueFailure<E>> {
        if !self.clear_nulls {
            return Ok(array);
        }
        observe(HydrateEvent::RebuildArray {
            path,
            source: &array,
            target,
        })
        .map_err(ValueFailure::Caller)?;
        let result = arrow_raw::without_nulls(&array)?;
        observe(HydrateEvent::OpaqueOperationFinished).map_err(ValueFailure::Caller)?;
        Ok(result)
    }
    fn materialize<E>(
        &self,
        binding: BorrowedTypedValueBinding<'_>,
        context: &mut ValueConversionContext,
        path: &[HydratePathStep],
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut HydrateObserver<'_, E>,
    ) -> Result<ArrayRef, ValueFailure<E>> {
        let layout = self.layout.ok_or_else(invalid)?;
        let source = ByteSource::parse(
            &self.source,
            self.source_layout.ok_or_else(invalid)?,
            self.dictionary_layout,
        )?;
        // View descriptors and cache choices were already acquired and frozen
        // during planning. Execution neither allocates another decoded bottom
        // nor changes the strategy after output capacity has been requested.
        let decoded = self
            .decode_plan
            .as_ref()
            .map(|plan| context.decode_planned(plan));
        let mut builder = ValueBuilder::new(layout);
        builder.reserve_planned_observed(
            self.source.len(),
            self.planned_payload,
            self.planned_regions,
            growth,
            work,
        )?;
        for row in 0..self.source.len() {
            work(1).map_err(ValueFailure::Caller)?;
            let result = if !self.active.contains(row) {
                if binding.value_type().nullable() {
                    builder.append_null(growth, work)
                } else if layout.is_utf8() {
                    builder.append_str("", growth, work)
                } else {
                    builder.append_bytes(&[], growth, work)
                }
            } else if let Some(decoded) = &decoded {
                match decoded.mapping().mapped_row(row)? {
                    None => builder.append_null(growth, work),
                    Some(index) => {
                        builder.append_array_value(decoded.bottom().as_ref(), index, growth, work)
                    }
                }
            } else {
                match source.mapped_row(row)? {
                    None => builder.append_null(growth, work),
                    Some(index) => {
                        builder.append_array_value(source.values().as_ref(), index, growth, work)
                    }
                }
            };
            result.map_err(|error| match error {
                WriteFailure::Failure(error) => error,
                WriteFailure::Full => ValueError::Invariant(ValueViolation::RowTransaction).into(),
            })?;
        }
        let result = builder
            .finish_structured_observed(growth, work, &mut |event| {
                observe(HydrateEvent::ValueStructure { path, event })
            })?
            .array;
        observe(HydrateEvent::OpaqueOperationFinished).map_err(ValueFailure::Caller)?;
        Ok(result)
    }
}
fn field_changes(source: &Field, target: &Field) -> bool {
    source.name() != target.name()
        || source.is_nullable() != target.is_nullable()
        || source.metadata().as_arc().is_some()
}
pub(super) fn local_metadata_changes(
    source: &DataType,
    target: &DataType,
) -> Result<bool, ValueError> {
    Ok(match (source, target) {
        (DataType::Struct(a), DataType::Struct(b)) if a.len() == b.len() => {
            a.iter().zip(b).any(|(a, b)| field_changes(a, b))
        }
        (DataType::Union(a, am), DataType::Union(b, bm)) if am == bm && a.len() == b.len() => a
            .iter()
            .zip(b.iter())
            .any(|((ai, a), (bi, b))| ai != bi || field_changes(a, b)),
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => field_changes(a, b),
        (DataType::FixedSizeList(a, aw), DataType::FixedSizeList(b, bw)) if aw == bw => {
            field_changes(a, b)
        }
        (DataType::Map(a, asort), DataType::Map(b, bsort)) if asort == bsort => {
            let (DataType::Struct(af), DataType::Struct(bf)) = (a.data_type(), b.data_type())
            else {
                return Err(invalid());
            };
            field_changes(a, b) || af.iter().zip(bf).any(|(a, b)| field_changes(a, b))
        }
        _ => return Err(invalid()),
    })
}
fn observe_fields<E>(
    target: &DataType,
    children: &[ArrayRef],
    path: &[HydratePathStep],
    observe: &mut HydrateObserver<'_, E>,
) -> Result<(), ValueFailure<E>> {
    match target {
        DataType::Struct(fields) => {
            for (field, child) in fields.iter().zip(children) {
                observe(HydrateEvent::RebuildField {
                    path,
                    source: field,
                    target: child.data_type(),
                })
                .map_err(ValueFailure::Caller)?;
            }
        }
        DataType::Union(fields, _) => {
            for ((_, field), child) in fields.iter().zip(children) {
                observe(HydrateEvent::RebuildField {
                    path,
                    source: field,
                    target: child.data_type(),
                })
                .map_err(ValueFailure::Caller)?;
            }
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            observe(HydrateEvent::RebuildField {
                path,
                source: field,
                target: children[0].data_type(),
            })
            .map_err(ValueFailure::Caller)?
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(invalid().into());
            };
            for (field, child) in fields.iter().zip(children) {
                observe(HydrateEvent::RebuildField {
                    path,
                    source: field,
                    target: child.data_type(),
                })
                .map_err(ValueFailure::Caller)?;
            }
            observe(HydrateEvent::RebuildField {
                path,
                source: entries,
                target: entries.data_type(),
            })
            .map_err(ValueFailure::Caller)?;
        }
        _ => return Err(invalid().into()),
    }
    Ok(())
}
