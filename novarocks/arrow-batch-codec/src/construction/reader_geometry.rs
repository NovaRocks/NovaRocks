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

//! Pinned Arrow60.0 try_fb_to_schema construction events, after the shared
//! borrowed field verifier. Temporary constructor backing and actual source
//! header backing use separate caller scopes. These are operation requests,
//! not exact retained-byte measurements or unique allocator facts.
use arrow_ipc as ipc;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use novarocks_type_contract::{
    ControlResourceError,
    owned_resources::{
        arrow_metadata::hashmap_conversion_requests,
        btree::BTreeResourceError,
        hashmap::{HashMapResourceError, original_fresh_unique_insert_tables_observed},
        layout::{LayoutResourceError, arc_layout},
        vec::original_fresh_push_allocation_requests_observed,
    },
};
use std::alloc::Layout;

#[derive(Debug)]
pub enum ReaderStructureFailure<H> {
    Creator(H),
    Layout(LayoutResourceError),
    Vector(ControlResourceError),
    Map(HashMapResourceError),
    BTree(BTreeResourceError),
    SourceModel(&'static str),
}
impl<H> From<ControlResourceError> for ReaderStructureFailure<H> {
    fn from(e: ControlResourceError) -> Self {
        Self::Vector(e)
    }
}
impl<H> From<HashMapResourceError> for ReaderStructureFailure<H> {
    fn from(e: HashMapResourceError) -> Self {
        Self::Map(e)
    }
}
impl<H> From<BTreeResourceError> for ReaderStructureFailure<H> {
    fn from(e: BTreeResourceError) -> Self {
        Self::BTree(e)
    }
}
impl<H> From<LayoutResourceError> for ReaderStructureFailure<H> {
    fn from(e: LayoutResourceError) -> Self {
        Self::Layout(e)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReaderStructureClass {
    SourceHeader,
    Temporary,
}

/// Supplied by the actual caller, not retained by any decoded type. The model
/// chooses the constructor class; callers cannot sign payload coverage with it.
pub struct ReaderStructureObserver<'a, H> {
    pub request: &'a mut dyn FnMut(Layout, ReaderStructureClass) -> Result<(), H>,
    pub opaque: &'a mut dyn FnMut(usize, ReaderStructureClass) -> Result<(), H>,
    pub work: &'a mut dyn FnMut() -> Result<(), H>,
}
impl<H> ReaderStructureObserver<'_, H> {
    fn step(&mut self) -> Result<(), ReaderStructureFailure<H>> {
        (self.work)().map_err(ReaderStructureFailure::Creator)
    }
    fn layout(
        &mut self,
        layout: Layout,
        class: ReaderStructureClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        (self.request)(layout, class).map_err(ReaderStructureFailure::Creator)
    }
    fn array<T>(
        &mut self,
        count: usize,
        class: ReaderStructureClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        self.layout(
            Layout::array::<T>(count).map_err(|_| LayoutResourceError::Arithmetic)?,
            class,
        )
    }
    fn shared<T>(&mut self) -> Result<(), ReaderStructureFailure<H>> {
        self.layout(
            arc_layout(Layout::new::<T>())?,
            ReaderStructureClass::SourceHeader,
        )
    }
    fn shared_slice<T>(&mut self, count: usize) -> Result<(), ReaderStructureFailure<H>> {
        self.layout(
            arc_layout(Layout::array::<T>(count).map_err(|_| LayoutResourceError::Arithmetic)?)?,
            ReaderStructureClass::SourceHeader,
        )
    }
    fn fresh_pushes<T>(&mut self, count: usize) -> Result<(), ReaderStructureFailure<H>> {
        let request = &mut *self.request;
        let work = &mut *self.work;
        let mut loan = |layout, copies| {
            for _ in 0..copies {
                request(layout, ReaderStructureClass::Temporary)
                    .map_err(ReaderStructureFailure::Creator)?;
            }
            Ok(())
        };
        original_fresh_push_allocation_requests_observed::<T, ReaderStructureFailure<H>>(
            count,
            &mut || work().map_err(ReaderStructureFailure::Creator),
            &mut Some(&mut loan),
        )?;
        Ok(())
    }
    fn metadata(
        &mut self,
        entries: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<ipc::KeyValue<'_>>>>,
    ) -> Result<(), ReaderStructureFailure<H>> {
        let Some(entries) = entries else {
            return Ok(());
        };
        // A real borrowed association scratch author avoids a quadratic source
        // scan. This temporary map creates no String and never escapes this
        // metadata callback; its exact constructor is admitted before use.
        let table = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
            &str,
            (usize, usize),
        >(entries.len())?;
        (self.opaque)(
            table.request_bytes_upper_bound,
            ReaderStructureClass::Temporary,
        )
        .map_err(ReaderStructureFailure::Creator)?;
        self.step()?;
        let mut associations =
            std::collections::HashMap::<&str, (usize, usize)>::with_capacity(entries.len());
        self.step()?;
        for index in 0..entries.len() {
            let entry = entries.get(index);
            let (Some(key), Some(_)) = (entry.key(), entry.value()) else {
                self.step()?;
                continue;
            };
            self.step()?;
            associations
                .entry(key)
                .and_modify(|positions| positions.1 = index)
                .or_insert((index, index));
            self.step()?;
        }
        for index in 0..entries.len() {
            let entry = entries.get(index);
            let (Some(key), Some(value)) = (entry.key(), entry.value()) else {
                self.step()?;
                continue;
            };
            self.step()?;
            let (first, last) =
                *associations
                    .get(key)
                    .ok_or(ReaderStructureFailure::SourceModel(
                        "original metadata association missing",
                    ))?;
            self.step()?;
            // std insert retains the first equal key and last replacement
            // value. Every to_string is real; overwritten strings are scratch.
            self.array::<u8>(
                key.len(),
                if index == first {
                    ReaderStructureClass::SourceHeader
                } else {
                    ReaderStructureClass::Temporary
                },
            )?;
            self.array::<u8>(
                value.len(),
                if index == last {
                    ReaderStructureClass::SourceHeader
                } else {
                    ReaderStructureClass::Temporary
                },
            )?;
        }
        let unique = associations.len();
        let opaque = &mut *self.opaque;
        let work = &mut *self.work;
        original_fresh_unique_insert_tables_observed::<String, String, ReaderStructureFailure<H>>(
            unique,
            || work().map_err(ReaderStructureFailure::Creator),
            |table, _last| {
                // Arrow 60 first builds the same HashMap, then consumes it
                // into Metadata's shared tree. No hash table survives.
                opaque(
                    table.request_bytes_upper_bound,
                    ReaderStructureClass::Temporary,
                )
                .map_err(ReaderStructureFailure::Creator)
            },
        )?;
        let conversion = hashmap_conversion_requests(unique)?;
        if let Some(layout) = conversion.entries_vector {
            self.layout(layout, ReaderStructureClass::Temporary)?;
        }
        if let Some(layout) = conversion.sort_scratch {
            self.layout(layout, ReaderStructureClass::Temporary)?;
        }
        // The common private-node model is a source upper, not an exact
        // allocator Layout; preserve that distinction at the caller port.
        for _ in 0..conversion.tree_nodes {
            (self.opaque)(
                conversion.tree_node.size(),
                ReaderStructureClass::SourceHeader,
            )
            .map_err(ReaderStructureFailure::Creator)?;
            self.step()?;
        }
        if let Some(layout) = conversion.map_arc {
            self.layout(layout, ReaderStructureClass::SourceHeader)?;
        }
        Ok(())
    }
    /// Call once after the sole schema's profile/field count has been checked.
    pub fn schema(&mut self, source: ipc::Schema<'_>) -> Result<(), ReaderStructureFailure<H>> {
        if !novarocks_type_contract::owned_resources::profile::LOCKED_TOOLCHAIN {
            return Err(ReaderStructureFailure::SourceModel(
                "IPC reader toolchain source model drift",
            ));
        }
        let fields = source.fields().ok_or(ReaderStructureFailure::SourceModel(
            "IPC reader schema fields missing",
        ))?;
        self.fresh_pushes::<Field>(fields.len())?;
        // Vec<Field>::IntoIter.map(Arc::new) is TrustedLen; Arc FromIterator
        // allocates the final slice directly, without a temporary FieldRef Vec.
        self.shared_slice::<FieldRef>(fields.len())?;
        self.metadata(source.custom_metadata())?;
        self.shared::<Schema>()
    }
    /// Constructor facts only. The source must first pass the shared full
    /// verifier; this callback does not interpret metadata as value semantics.
    pub fn field(&mut self, source: ipc::Field<'_>) -> Result<(), ReaderStructureFailure<H>> {
        self.array::<u8>(
            source.name().unwrap_or_default().len(),
            ReaderStructureClass::SourceHeader,
        )?;
        self.shared::<Field>()?;
        self.metadata(source.custom_metadata())?;
        if source.dictionary().is_some() {
            self.layout(
                Layout::new::<DataType>(),
                ReaderStructureClass::SourceHeader,
            )?;
            self.layout(
                Layout::new::<DataType>(),
                ReaderStructureClass::SourceHeader,
            )?;
        }
        let children = source.children().map_or(0, |children| children.len());
        match source.type_type() {
            ipc::Type::Struct_ => {
                // try_field_from collects Result<Vec<Field>, _>; GenericShunt
                // has zero lower hint. Vec<Field>::into then maps Arc::new
                // over a trusted iterator and builds the final shared slice.
                self.fresh_pushes::<Field>(children)?;
                self.shared_slice::<FieldRef>(children)?;
            }
            ipc::Type::Timestamp => {
                let timestamp =
                    source
                        .type_as_timestamp()
                        .ok_or(ReaderStructureFailure::SourceModel(
                            "unverified timestamp constructor",
                        ))?;
                if let Some(timezone) = timestamp.timezone() {
                    self.shared_slice::<u8>(timezone.len())?;
                }
            }
            ipc::Type::Union => {
                let union = source
                    .type_as_union()
                    .ok_or(ReaderStructureFailure::SourceModel(
                        "unverified union constructor",
                    ))?;
                if union.typeIds().is_none() {
                    return Err(ReaderStructureFailure::SourceModel(
                        "IPC union implicit IDs are outside the verified source author",
                    ));
                }
                self.fresh_pushes::<Field>(children)?;
                self.fresh_pushes::<(i8, FieldRef)>(children)?;
                self.shared_slice::<(i8, FieldRef)>(children)?;
            }
            _ => {}
        }
        Ok(())
    }
}
