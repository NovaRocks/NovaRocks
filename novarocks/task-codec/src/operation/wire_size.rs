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

//! Allocation-free invoice for the original operation codec. Payloads stay
//! borrowed from the same codec-produced facts; no DTO or transport is formed.
use super::*;
use crate::creation::size::{
    CreationSizeOverflow, message as raw_message, packed, participation, task, uint,
};
use crate::domain::{stored_credential, stored_message};
use novarocks_execution_contract::task_execution::operation::{
    CredentialUpdate, ParticipationDomainUpdate, TaskDomainUpdate,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireSizeError {
    Overflow,
    NotCodecProduced,
}
impl From<CreationSizeOverflow> for WireSizeError {
    fn from(_: CreationSizeOverflow) -> Self {
        Self::Overflow
    }
}
pub type SizeResult = Result<usize, WireSizeError>;
fn message(tag: u32, bytes: usize) -> SizeResult {
    raw_message(tag, bytes).map_err(Into::into)
}
fn add(a: usize, b: usize) -> SizeResult {
    a.checked_add(b).ok_or(WireSizeError::Overflow)
}
fn sum(values: impl IntoIterator<Item = usize>) -> SizeResult {
    values.into_iter().try_fold(0, add)
}
fn bytes(tag: u32, len: usize) -> SizeResult {
    if len == 0 { Ok(0) } else { message(tag, len) }
}
fn envelope(value: OperationEnvelope) -> SizeResult {
    envelope_wait(value.max_wait())
}
fn envelope_wait(wait: MaxWait) -> SizeResult {
    // Every original operation identity is one UUID-v7 byte field. Pricing
    // this fixed representation does not mint or substitute an identity.
    add(
        message(1, message(1, 16)?)?,
        uint(2, wait.get().as_millis() as u64),
    )
}
fn operation(value: OperationEnvelope, tag: u32, body: usize) -> SizeResult {
    add(message(1, envelope(value)?)?, message(tag, body)?)
}
pub fn batch_item(operation_bytes: usize) -> SizeResult {
    message(1, operation_bytes)
}

pub fn credential(value: &CredentialUpdate) -> SizeResult {
    let original =
        stored_credential(value.material().as_ref()).ok_or(WireSizeError::NotCodecProduced)?;
    let mut out = uint(1, value.credential_domain_id().get()) + uint(2, value.epoch().get());
    for descriptor in original.descriptors() {
        out = add(out, message(3, descriptor.encoded_len())?)?;
    }
    Ok(out)
}
pub fn participation_domain(value: &ParticipationDomainUpdate) -> SizeResult {
    match value {
        ParticipationDomainUpdate::CatalogBinding { version, payload } => {
            let original =
                stored_message::<novarocks_proto_models::catalog::CatalogSet>(payload.as_ref())
                    .ok_or(WireSizeError::NotCodecProduced)?;
            message(
                1,
                add(uint(1, version.get()), message(2, original.encoded_len())?)?,
            )
        }
        ParticipationDomainUpdate::SharedDynamicFilter { version, payload } => {
            let original = stored_message::<novarocks::RuntimeFilterContribution>(payload.as_ref())
                .ok_or(WireSizeError::NotCodecProduced)?;
            message(
                2,
                add(uint(1, version.get()), message(2, original.encoded_len())?)?,
            )
        }
        ParticipationDomainUpdate::Credential(value) => message(3, credential(value)?),
    }
}
fn close_destination(value: &TaskDomainUpdate) -> SizeResult {
    let TaskDomainUpdate::CloseExchangeDestination {
        version,
        edge,
        destination,
    } = value
    else {
        return Err(WireSizeError::NotCodecProduced);
    };
    add(
        uint(1, version.get()) + uint(2, edge.get() as u64),
        message(3, task(*destination)?)?,
    )
}
pub fn task_domain(value: &TaskDomainUpdate) -> SizeResult {
    match value {
        TaskDomainUpdate::SplitAssignment(value) => {
            let original =
                stored_message::<novarocks_proto_models::connector_read::SplitAssignment>(
                    value.payload().as_ref(),
                )
                .ok_or(WireSizeError::NotCodecProduced)?;
            message(1, message(1, original.encoded_len())?)
        }
        TaskDomainUpdate::TaskDynamicFilter { version, payload } => {
            let original = stored_message::<novarocks_proto_models::filter::RuntimeFilterEnvelope>(
                payload.as_ref(),
            )
            .ok_or(WireSizeError::NotCodecProduced)?;
            message(
                2,
                add(uint(1, version.get()), message(2, original.encoded_len())?)?,
            )
        }
        TaskDomainUpdate::OpenExchangeEdges { version, edges } => {
            let values = sum(edges
                .iter()
                .map(|edge| prost::encoding::encoded_len_varint(edge.get() as u64)))?;
            message(3, add(uint(1, version.get() as u64), packed(2, values)?)?)
        }
        TaskDomainUpdate::CloseExchangeDestination { .. } => message(4, close_destination(value)?),
    }
}
pub fn create(value: OperationEnvelope, fragment_len: usize, metadata_len: usize) -> SizeResult {
    operation(
        value,
        2,
        add(bytes(4, fragment_len)?, bytes(5, metadata_len)?)?,
    )
}
fn update_body(identity: TaskIdentity, domains: &[TaskDomainUpdate], control: bool) -> SizeResult {
    let mut body = message(1, task(identity)?)?;
    for value in domains {
        let domain = if control {
            close_destination(value)?
        } else {
            task_domain(value)?
        };
        body = add(body, message(2, domain)?)?;
    }
    Ok(body)
}
pub fn update(value: &UpdateTask, control: bool) -> SizeResult {
    operation(
        value.envelope(),
        if control { 7 } else { 3 },
        update_body(value.identity(), value.domains(), control)?,
    )
}
/// Exact one-item request size before the pending domain enters its queue.
/// UpdateTask::try_new uses this same default wait and UUID representation;
/// no request identity or allocation is created for a preview.
pub fn pending_update(identity: TaskIdentity, domain: &TaskDomainUpdate) -> SizeResult {
    let control = matches!(domain, TaskDomainUpdate::CloseExchangeDestination { .. });
    let body = update_body(identity, std::slice::from_ref(domain), control)?;
    let operation = add(
        message(
            1,
            envelope_wait(MaxWait::default_for(OperationKind::UpdateTask))?,
        )?,
        message(if control { 7 } else { 3 }, body)?,
    )?;
    batch_item(operation)
}
pub fn cancel(value: CancelTask, control: bool) -> SizeResult {
    operation(
        value.envelope(),
        if control { 3 } else { 5 },
        add(
            message(1, task(value.identity())?)?,
            uint(2, encode_cancel_reason(value.reason()) as u64),
        )?,
    )
}
pub fn abort(value: AbortParticipation, control: bool) -> SizeResult {
    operation(
        value.envelope(),
        if control { 4 } else { 6 },
        add(
            message(1, participation(value.participation())?)?,
            uint(2, encode_abort_cause(value.cause()) as u64),
        )?,
    )
}
pub fn release(value: ReleaseParticipation, control: bool) -> SizeResult {
    operation(
        value.envelope(),
        if control { 5 } else { 7 },
        message(1, participation(value.participation())?)?,
    )
}
pub fn quiesce(value: QuiesceParticipation, control: bool) -> SizeResult {
    operation(
        value.envelope(),
        if control { 6 } else { 9 },
        message(1, participation(value.participation())?)?,
    )
}
pub fn acquire(value: AcquireParticipationAdmissionTicket) -> SizeResult {
    operation(
        value.envelope(),
        8,
        sum([
            message(1, participation(value.participation())?)?,
            uint(2, value.valid_for().get().as_millis() as u64),
            message(
                3,
                message(1, value.native_compatibility_id().as_bytes().len())?,
            )?,
            message(4, message(1, 16)?)?,
        ])?,
    )
}
pub fn renew(value: &RenewParticipationLease, control: bool) -> SizeResult {
    let lease =
        uint(1, value.sequence().get()) + uint(2, value.valid_for().get().as_millis() as u64);
    let body = add(
        message(1, participation(value.participation())?)?,
        message(2, lease)?,
    )?;
    if control {
        operation(value.envelope(), 2, body)
    } else {
        operation(value.envelope(), 4, message(3, body)?)
    }
}
pub fn advance(value: &AdvanceParticipationDomain) -> SizeResult {
    operation(
        value.envelope(),
        4,
        message(
            2,
            add(
                message(1, participation(value.participation())?)?,
                message(2, participation_domain(value.domain())?)?,
            )?,
        )?,
    )
}
pub fn establish(value: &EstablishParticipation) -> SizeResult {
    let catalog = stored_message::<novarocks_proto_models::catalog::CatalogSet>(
        value.catalog_binding().as_ref(),
    )
    .ok_or(WireSizeError::NotCodecProduced)?;
    let filter = stored_message::<novarocks::RuntimeFilterContribution>(
        value.initial_runtime_filter().as_ref(),
    )
    .ok_or(WireSizeError::NotCodecProduced)?;
    let options = stored_message::<novarocks::QueryOptions>(value.query_options().as_ref())
        .ok_or(WireSizeError::NotCodecProduced)?;
    let lease = uint(1, value.initial_lease_sequence().get())
        + uint(2, value.initial_lease_valid_for().get().as_millis() as u64);
    let body = sum([
        message(1, participation(value.participation())?)?,
        message(2, catalog.encoded_len())?,
        message(3, filter.encoded_len())?,
        message(4, credential(value.initial_credential())?)?,
        message(5, lease)?,
        message(6, options.encoded_len())?,
        // Compatibility IDs have the original fixed 32-byte representation.
        message(7, message(1, 32)?)?,
        message(8, message(1, 16)?)?,
    ])?;
    operation(value.envelope(), 4, message(1, body)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn borrowed_create_uses_the_original_codec_at_length_prefix_boundaries() {
        let envelope = OperationEnvelope::with_default_wait(
            TaskOperationId::new_v7(),
            OperationKind::CreateTask,
        );
        for (fragment, metadata) in [(0, 0), (1, 1), (127, 128), (16384, 3), (70000, 200000)] {
            let fragment = FrozenBytes::freeze(bytes::Bytes::from(vec![7; fragment]));
            let metadata = FrozenBytes::freeze(bytes::Bytes::from(vec![9; metadata]));
            let actual = encode_create_task(envelope, &fragment, &metadata);
            assert_eq!(
                create(envelope, fragment.len(), metadata.len()).unwrap(),
                actual.encoded_len()
            );
        }
        assert_eq!(
            create(envelope, usize::MAX, 1),
            Err(WireSizeError::Overflow)
        );
    }
}
