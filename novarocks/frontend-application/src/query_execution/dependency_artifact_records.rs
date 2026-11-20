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

//! Original binding DTO -> original Prost writer under the actual artifact pair.
//! This component does not certify complete statement dependency coverage.
use super::dependency_artifact_storage::{
    CaptureArtifact, CaptureRecordError, CaptureStorageError,
};
use novarocks_plan_codec::physical_binding_v2::{
    BindingCodecError, BindingProjectionFacts, EncodedFunctionBindings,
};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use novarocks_workload_control::{
    AllocationCharge, LocalResourceAuthority, Reservation, ResourceClass, WorkError, WorkScope,
};
use prost::Message;
/// One reservation on the actual existing authority; no new account or policy.
pub(crate) struct BindingProjectionStock {
    authority: LocalResourceAuthority,
    scope: WorkScope,
    reservation: Option<Reservation>,
}
impl BindingProjectionStock {
    pub(crate) fn admit(&mut self, facts: &BindingProjectionFacts) -> Result<(), WorkError> {
        let bytes = u64::try_from(facts.request_bytes_upper_bound)
            .map_err(|_| WorkError::ArithmeticOverflow)?;
        match self.reservation.as_mut() {
            Some(current) if bytes > current.remaining_bytes() => {
                current.grow(bytes - current.remaining_bytes())
            }
            Some(_) => Ok(()),
            None if bytes > 0 => {
                self.reservation = Some(self.authority.reserve(
                    &self.scope,
                    bytes,
                    ResourceClass::Data,
                )?);
                Ok(())
            }
            None => Ok(()),
        }
    }
    fn charge(&mut self, bytes: usize) -> Result<Option<AllocationCharge>, BindingRecordError> {
        if bytes == 0 {
            return Ok(None);
        }
        let bytes = u64::try_from(bytes).map_err(|_| BindingRecordError::UnrepresentableBacking)?;
        let granted = self
            .reservation
            .as_ref()
            .map_or(0, Reservation::remaining_bytes);
        // Never request late stock for an allocation that has already happened.
        if bytes > granted {
            return Err(BindingRecordError::BackingExceedsGrant {
                actual: bytes,
                granted,
            });
        }
        self.reservation
            .as_mut()
            .expect("nonzero checked grant")
            .charge(bytes)
            .map(Some)
            .map_err(BindingRecordError::Work)
    }
}
struct ChargedBindings<'loan, 'source> {
    value: Option<EncodedFunctionBindings<'loan, 'source>>,
    _charge: Option<AllocationCharge>,
}
impl Drop for ChargedBindings<'_, '_> {
    fn drop(&mut self) {
        drop(self.value.take());
    } // Backing is destroyed before _charge.
}
#[derive(Debug)]
pub(crate) enum BindingRecordError {
    Codec(BindingCodecError),
    Work(WorkError),
    Storage(CaptureRecordError<prost::EncodeError>),
    BackingExceedsGrant { actual: u64, granted: u64 },
    UnrepresentableBacking,
    ForeignStock,
    Closed,
}
impl From<CompileControlError> for BindingRecordError {
    fn from(cause: CompileControlError) -> Self {
        Self::Codec(cause.into())
    }
}
/// The one private storage instance is retained; no statement registry or queue.
pub(crate) struct OriginalBindingRecordWriter {
    artifact: CaptureArtifact,
    failed: bool,
}
impl OriginalBindingRecordWriter {
    pub(crate) fn new(artifact: CaptureArtifact) -> Self {
        Self {
            artifact,
            failed: false,
        }
    }
    pub(crate) fn projection_stock(&self) -> BindingProjectionStock {
        let (authority, scope) = self.artifact.resource_loan();
        BindingProjectionStock {
            authority: authority.clone(),
            scope: scope.clone(),
            reservation: None,
        }
    }
    pub(crate) fn write_bindings(
        &mut self,
        bindings: EncodedFunctionBindings<'_, '_>,
        stock: &mut BindingProjectionStock,
        tag: u8,
        occurrence: u64,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), BindingRecordError> {
        if self.failed {
            return Err(BindingRecordError::Closed);
        }
        let result = self.write_bindings_core(bindings, stock, tag, occurrence, work);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn write_bindings_core(
        &mut self,
        bindings: EncodedFunctionBindings<'_, '_>,
        stock: &mut BindingProjectionStock,
        tag: u8,
        occurrence: u64,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), BindingRecordError> {
        let (authority, scope) = self.artifact.resource_loan();
        // Check the original authority without requesting capacity.
        // Also require the original storage WorkId, rather than matching SQL/token text.
        authority
            .validate_scope_authority(&stock.scope)
            .map_err(BindingRecordError::Work)?;
        if scope.id() != stock.scope.id() {
            return Err(BindingRecordError::ForeignStock);
        }
        let bytes = bindings
            .owned_backing_bytes_observed(work)
            .map_err(BindingRecordError::Codec)?;
        let charge = stock.charge(bytes)?;
        let owned = ChargedBindings {
            value: Some(bindings),
            _charge: charge,
        };
        for row in owned.value.as_ref().expect("live owned DTO").as_wire() {
            work.flush()?;
            let length = row.encoded_len();
            work.flush()?;
            // Existing storage admits and charges the exact output backing before
            // this ONE original Prost encoder runs. No intermediate Vec/serializer.
            self.artifact
                .write_record(tag, occurrence, length, |mut bytes: &mut [u8]| {
                    row.encode(&mut bytes)
                })
                .map_err(BindingRecordError::Storage)?;
            work.step()?;
            work.flush()?;
        }
        drop(owned);
        Ok(())
    }
    pub(crate) fn finish_storage(&mut self) -> Result<(), CaptureStorageError> {
        if self.failed {
            return Err(CaptureStorageError::Closed);
        }
        self.artifact.finish_storage()
    }
    pub(crate) fn bytes_written(&self) -> u64 {
        self.artifact.bytes_written()
    }
}

#[cfg(test)]
mod tests;
