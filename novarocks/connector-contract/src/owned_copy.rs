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

//! Writer-owned request contributions to an existing preparation invoice.
//! These facts are numerical upper bounds, not a wallet or allocator grant.

use crate::{ConnectorError, ConnectorErrorKind, PureProviderCompileError};
use novarocks_type_contract::owned_resources::{copy, hashmap, layout};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::{alloc::Layout, collections::TryReserveError};

mod created;
pub(crate) use created::CreatedWriteCopy;
pub use created::WriterOwnedCreationFailure;

/// Contributions accumulated by the original writer constructors. The caller
/// must provide the truthful whole retained source invoice, including private
/// backing and shared allocations once. `source_floor` is only a necessary
/// lower bound; it is never used to bound an opaque source-table operation.
/// Requests include temporary allocations and conservative reallocation costs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriterOwnedResourceFacts {
    pub source_retained_bytes: usize,
    pub source_floor: usize,
    pub allocation_requests: usize,
    pub requested_bytes: usize,
    pub coexistence_bytes: usize,
    pub work_units: usize,
}

pub(crate) trait OwnedCopy {
    type Error: From<ConnectorError>;
    // Optional actual construction ownership. Pure/count actors have no scope
    // capability; their original work/request observation sequence is unchanged.
    fn created_fields(&self) -> bool {
        false
    }
    fn begin_field(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn finish_field(
        &mut self,
    ) -> Result<novarocks_type_contract::ValueTypeStorageLifetime, Self::Error> {
        Ok(novarocks_type_contract::ValueTypeStorageLifetime::pure())
    }
    fn retain_shared_field(
        &mut self,
        _: novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn created_wrapper(&mut self, _layout: Layout) -> Result<(), Self::Error> {
        Ok(())
    }
    // Pure/counting actors have no construction capability. These operation
    // hooks are overridden only by the actual scoped writer copy.
    fn temporary_request(&mut self, _: Layout) -> Result<(), Self::Error> {
        Ok(())
    }
    fn prefunded_request(&mut self, _: Layout, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn receipt_request(&mut self, _: Layout, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        novarocks_type_contract::ValueTypeStorageLifetime::pure()
    }
    fn receipt_storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        novarocks_type_contract::ValueTypeStorageLifetime::pure()
    }
    fn prepare_field_materialization(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn materialized_field(
        &mut self,
        _: &novarocks_type_contract::owned_resources::metadata_materialization::SharedMaterializedField,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn materialized_schema(
        &mut self,
        _: &novarocks_type_contract::owned_resources::metadata_materialization::SharedMaterializedSchema,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn materializes(&self) -> bool {
        true
    }
    fn source_invoice(&self) -> Option<usize> {
        None
    }
    fn temporary_model_request(
        &mut self,
        layout: Layout,
        copies: usize,
    ) -> Result<(), Self::Error> {
        self.request(layout, copies)
    }
    fn temporary_array<T>(&mut self, count: usize, copies: usize) -> Result<(), Self::Error> {
        let layout = Layout::array::<T>(count).map_err(|_| self.arithmetic())?;
        if layout.size() != 0 {
            self.temporary_model_request(layout, copies)?;
        }
        Ok(())
    }
    fn request(&mut self, _: Layout, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    /// One opaque allocation upper per original library request. Numerical
    /// actors retain the same request count; actual creators use their opaque
    /// channel and never publish a guessed private layout as exact.
    fn opaque_model_request(&mut self, bytes: usize, copies: usize) -> Result<(), Self::Error> {
        let layout = Layout::array::<u8>(bytes).map_err(|_| self.arithmetic())?;
        self.request(layout, copies)
    }
    fn work(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn source_floor(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn begin_copy(&mut self) -> Result<(), Self::Error> {
        self.flush()
    }
    fn arithmetic(&self) -> Self::Error;
    fn reserve_exit(&mut self, result: Result<(), TryReserveError>) -> Result<(), Self::Error>;
    fn spelling(&mut self, value: &str) -> Result<String, Self::Error>;

    fn add(&self, left: usize, right: usize) -> Result<usize, Self::Error> {
        left.checked_add(right).ok_or_else(|| self.arithmetic())
    }
    fn mul(&self, left: usize, right: usize) -> Result<usize, Self::Error> {
        left.checked_mul(right).ok_or_else(|| self.arithmetic())
    }
    fn array<T>(&mut self, count: usize, copies: usize) -> Result<(), Self::Error> {
        let request = Layout::array::<T>(count).map_err(|_| self.arithmetic())?;
        if request.size() != 0 {
            self.request(request, copies)?;
        }
        Ok(())
    }
    fn arc_slice<T>(&mut self, count: usize) -> Result<(), Self::Error> {
        if self.source_invoice().is_some() {
            let payload = Layout::array::<T>(count).map_err(|_| self.arithmetic())?;
            let request = layout::arc_layout(payload).map_err(|_| self.arithmetic())?;
            self.request(request, 1)?;
        }
        Ok(())
    }
    fn arc<T>(&mut self) -> Result<(), Self::Error> {
        self.arc_slice::<T>(1)
    }
    fn table<K, V>(&mut self, count: usize) -> Result<usize, Self::Error> {
        if self.source_invoice().is_none() {
            return Ok(0);
        }
        let facts =
            hashmap::fresh_table_layout::<K, V>(count).map_err(|error| self.hash_error(error))?;
        if let Some(request) = facts.layout {
            self.request(request, 1)?;
        }
        // Includes control-byte initialization and inline element moves. The
        // actual closed key hash/probe work remains with the collection owner.
        self.work(self.add(
            self.mul(count, size_of::<(K, V)>())?,
            self.add(facts.buckets, 16)?,
        )?)?;
        Ok(facts.buckets)
    }
    /// Prefund a closed table owner's possible cleanup. Raw-table destruction
    /// walks real control groups even after an early failure. The caller must
    /// supply the work of its actual key/value destructors; arbitrary user Drop
    /// implementations are not admitted by this table geometry helper.
    fn table_cleanup<K, V>(&mut self, count: usize, entry_work: usize) -> Result<(), Self::Error> {
        if self.source_invoice().is_some() {
            let facts = hashmap::fresh_table_layout::<K, V>(count)
                .map_err(|error| self.hash_error(error))?;
            let iteration = if count == 0 {
                64
            } else {
                hashmap::source_iterator_work_upper_bound(facts.request_bytes_upper_bound, count)
                    .map_err(|error| self.hash_error(error))?
            };
            self.work(self.add(iteration, self.mul(count, entry_work)?)?)?;
        }
        Ok(())
    }
    fn hash_error(&self, error: hashmap::HashMapResourceError) -> Self::Error {
        match error {
            hashmap::HashMapResourceError::Arithmetic(_) => self.arithmetic(),
            hashmap::HashMapResourceError::SourceModel(message) => {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, message).into()
            }
        }
    }
    fn metadata_error(
        &self,
        error: novarocks_type_contract::owned_resources::btree::BTreeResourceError,
    ) -> Self::Error {
        let kind = match error {
            novarocks_type_contract::owned_resources::btree::BTreeResourceError::Arithmetic(_) => {
                ConnectorErrorKind::ResourceExhausted
            }
            novarocks_type_contract::owned_resources::btree::BTreeResourceError::SourceModel(_) => {
                ConnectorErrorKind::InvalidRequest
            }
        };
        ConnectorError::from_domain(kind, error).into()
    }
    fn metadata_iteration(&mut self, entries: usize, traversals: usize) -> Result<(), Self::Error> {
        if self.source_invoice().is_some() {
            // Arrow 60 Metadata has BTree cursor traversal, not a sparse
            // RawTable scan. The existing locked insertion-only projection
            // covers the strictly smaller full cursor/parent-link traversal;
            // text processing remains each caller's separate operation.
            let units = if entries == 0 {
                64
            } else {
                novarocks_type_contract::owned_resources::btree::insertion_only::<String, String>(
                    entries,
                )
                .map_err(|error| self.metadata_error(error))?
                .cumulative_work_upper_bound
            };
            self.work(self.mul(units, traversals)?)?;
        }
        self.flush()
    }
    fn metadata_iteration_with_bound(
        &mut self,
        entries: usize,
        traversals: usize,
        _source_work_bound: Option<usize>,
    ) -> Result<(), Self::Error> {
        self.metadata_iteration(entries, traversals)
    }
    fn string(&mut self, value: &str) -> Result<Option<String>, Self::Error> {
        self.array::<u8>(value.len(), 1)?;
        // The same body is visited once for counting and once for copying.
        self.work(self.add(self.mul(value.len(), 2)?, 32)?)?;
        self.flush()?;
        if self.materializes() {
            self.spelling(value).map(Some)
        } else {
            Ok(None)
        }
    }
    fn real_string(&mut self, value: &str) -> Result<String, Self::Error> {
        // Original uniqueness validation needs actual keys even in Count.
        self.array::<u8>(value.len(), 1)?;
        self.work(self.add(value.len(), 16)?)?;
        self.flush()?;
        self.temporary_request(Layout::array::<u8>(value.len()).map_err(|_| self.arithmetic())?)?;
        self.spelling(value)
    }
}

pub(crate) struct PlainCopy;
impl OwnedCopy for PlainCopy {
    type Error = ConnectorError;
    fn arithmetic(&self) -> Self::Error {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "writer owned resource arithmetic overflowed",
        )
    }
    fn reserve_exit(&mut self, result: Result<(), TryReserveError>) -> Result<(), Self::Error> {
        result.map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "writer owned allocation was refused",
            )
        })
    }
    fn spelling(&mut self, value: &str) -> Result<String, Self::Error> {
        Ok(value.to_owned())
    }
}

pub(crate) struct ObservedCopy<'a, 'control, A> {
    facts: WriterOwnedResourceFacts,
    counting: bool,
    admit: &'a mut A,
    checkpoints: &'a mut CompileCheckpoints<'control>,
}
impl<'a, 'control, A> ObservedCopy<'a, 'control, A>
where
    A: FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
{
    pub(crate) fn new(
        source_retained_bytes: usize,
        admit: &'a mut A,
        checkpoints: &'a mut CompileCheckpoints<'control>,
    ) -> Result<Self, PureProviderCompileError<ConnectorError>> {
        let mut context = Self {
            facts: WriterOwnedResourceFacts {
                source_retained_bytes,
                coexistence_bytes: source_retained_bytes,
                ..Default::default()
            },
            counting: true,
            admit,
            checkpoints,
        };
        context.gate()?;
        // Only the current fixed Writer/Codec/ValueType/Union diagnostic set.
        // This cannot authorize arbitrary provider strings or Debug payloads.
        context.array::<u8>(1024, 16)?;
        context.work(16 * 1024)?;
        Ok(context)
    }
    pub(super) fn extra_request(
        &mut self,
        layout: Layout,
    ) -> Result<(), PureProviderCompileError<ConnectorError>> {
        extra_request(&mut self.facts, self.admit, layout)
    }
    fn gate(&mut self) -> Result<(), PureProviderCompileError<ConnectorError>> {
        (self.admit)(&self.facts).map_err(PureProviderCompileError::Control)
    }
}
impl<A> OwnedCopy for ObservedCopy<'_, '_, A>
where
    A: FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
{
    type Error = PureProviderCompileError<ConnectorError>;
    fn materializes(&self) -> bool {
        !self.counting
    }
    fn source_invoice(&self) -> Option<usize> {
        Some(self.facts.source_retained_bytes)
    }
    fn arithmetic(&self) -> Self::Error {
        CompileControlError::ResourceExhausted.into()
    }
    fn request(&mut self, request: Layout, copies: usize) -> Result<(), Self::Error> {
        if self.counting {
            let bytes = self.mul(request.size(), copies)?;
            self.facts.allocation_requests = self.add(self.facts.allocation_requests, copies)?;
            self.facts.requested_bytes = self.add(self.facts.requested_bytes, bytes)?;
            self.facts.coexistence_bytes =
                self.add(self.facts.source_retained_bytes, self.facts.requested_bytes)?;
        }
        self.gate()
    }
    fn work(&mut self, units: usize) -> Result<(), Self::Error> {
        if self.counting {
            self.facts.work_units = self.add(self.facts.work_units, units)?;
        }
        self.gate()
    }
    fn source_floor(&mut self, minimum: usize) -> Result<(), Self::Error> {
        self.facts.source_floor = self.facts.source_floor.max(minimum);
        if minimum > self.facts.source_retained_bytes {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "writer retained source invoice is understated",
            )
            .into());
        }
        self.gate()
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        self.gate()?;
        self.checkpoints.step().map_err(Into::into)
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.gate()?;
        self.checkpoints.flush().map_err(Into::into)
    }
    fn begin_copy(&mut self) -> Result<(), Self::Error> {
        self.flush()?;
        self.counting = false;
        Ok(())
    }
    fn reserve_exit(&mut self, result: Result<(), TryReserveError>) -> Result<(), Self::Error> {
        // Captured refusal precedes all later callbacks.
        result.map_err(|_| Self::Error::from(CompileControlError::ResourceExhausted))?;
        self.step()?;
        copy::reserve_exit::<Self::Error>(Ok(()), self.checkpoints)
    }
    fn spelling(&mut self, value: &str) -> Result<String, Self::Error> {
        self.gate()?;
        copy::copy_string::<Self::Error>(value, self.checkpoints)
    }
}

// Extra geometry comes only from the actual scope/flat-holder/Vec constructor.
// This extends the original cumulative model; it never resets source/work/caps.
fn extra_request<A: FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>>(
    facts: &mut WriterOwnedResourceFacts,
    admit: &mut A,
    layout: Layout,
) -> Result<(), PureProviderCompileError<ConnectorError>> {
    let add = |a: usize, b: usize| {
        a.checked_add(b)
            .ok_or(CompileControlError::ResourceExhausted)
    };
    facts.allocation_requests = add(facts.allocation_requests, 1)?;
    facts.requested_bytes = add(facts.requested_bytes, layout.size())?;
    facts.coexistence_bytes = add(facts.source_retained_bytes, facts.requested_bytes)?;
    // New scope headers and actual pointer/flat holder copies have real byte
    // initialization/move work, separate from unchanged original copy work.
    facts.work_units = add(facts.work_units, add(layout.size(), 128)?)?;
    admit(facts).map_err(PureProviderCompileError::Control)
}
