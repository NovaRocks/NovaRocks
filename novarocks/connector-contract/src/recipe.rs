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

//! Bounded, provider-validated relation recipes and Task split inputs.
//!
//! The neutral carrier never interprets private bytes. An exact provider codec
//! decodes every field of a draft, checks its private facts, and re-encodes a
//! canonical draft before sealing it as a program recipe. A recipe holds no
//! reader, catalog client, credential, task, or runtime adapter.

use crate::owned_copy::{ObservedCopy, OwnedCopy};
use crate::{ConnectorError, PureProviderCompileError, WriterOwnedResourceFacts};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::{error::Error, fmt, sync::Arc};

use crate::{
    ConnectorCodecCategory, ConnectorCodecContractError, ConnectorEncodedPayload,
    ConnectorReadBinding, ConnectorReadRelationPayload,
};

pub const MAX_CONNECTOR_RECIPE_COLUMNS: usize = 4_096;
pub const MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CONNECTOR_RECIPE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_CONNECTOR_RECIPE_ADDRESSES: usize = 256;
pub const MAX_CONNECTOR_RECIPE_AFFINITY_BYTES: usize = 4_096;
pub const MAX_CONNECTOR_RECIPE_SPLIT_WEIGHT: u64 = 100_000_000;
const RECIPE_IDENTITY_AND_ALLOCATION_CHARGE: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadRelationRecipeError {
    Header(ConnectorCodecContractError),
    BindingMismatch,
    PublicFactsMismatch,
    InvalidSplitFacts,
    TooManyColumns,
    PayloadTooLarge,
    RecipeTooLarge,
}

impl fmt::Display for ConnectorReadRelationRecipeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Header(_) => "connector read recipe header does not match its binding",
            Self::BindingMismatch => "connector read recipe binding does not match its compiler",
            Self::PublicFactsMismatch => {
                "connector read recipe public facts changed during canonicalization"
            }
            Self::InvalidSplitFacts => "connector read recipe split facts are invalid",
            Self::TooManyColumns => "connector read recipe has too many columns",
            Self::PayloadTooLarge => "connector read recipe payload exceeds the hard limit",
            Self::RecipeTooLarge => "connector read recipe exceeds the retained byte limit",
        })
    }
}

impl std::error::Error for ConnectorReadRelationRecipeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Header(source) => Some(source),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorRecipeHostAddress {
    host: Arc<str>,
    port: u16,
}

impl ConnectorRecipeHostAddress {
    pub fn try_new(
        host: impl AsRef<str>,
        port: u16,
    ) -> Result<Self, ConnectorReadRelationRecipeError> {
        let host = host.as_ref();
        if host.is_empty() || host.len() > 255 {
            return Err(ConnectorReadRelationRecipeError::InvalidSplitFacts);
        }
        Ok(Self {
            host: Arc::from(host),
            port,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub const fn port(&self) -> u16 {
        self.port
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadRecipeSplitFacts {
    remotely_accessible: bool,
    addresses: Arc<[ConnectorRecipeHostAddress]>,
    affinity_key: Option<Arc<str>>,
    split_weight: u64,
    retained_size_in_bytes: u64,
}

impl ConnectorReadRecipeSplitFacts {
    pub fn try_new(
        remotely_accessible: bool,
        addresses: Vec<ConnectorRecipeHostAddress>,
        affinity_key: Option<impl AsRef<str>>,
        split_weight: u64,
        retained_size_in_bytes: u64,
    ) -> Result<Self, ConnectorReadRelationRecipeError> {
        let affinity_key = affinity_key.map(|value| Arc::<str>::from(value.as_ref()));
        if addresses.len() > MAX_CONNECTOR_RECIPE_ADDRESSES
            || affinity_key
                .as_ref()
                .is_some_and(|value| value.len() > MAX_CONNECTOR_RECIPE_AFFINITY_BYTES)
            || split_weight == 0
            || split_weight > MAX_CONNECTOR_RECIPE_SPLIT_WEIGHT
            || retained_size_in_bytes > MAX_CONNECTOR_RECIPE_BYTES as u64
        {
            return Err(ConnectorReadRelationRecipeError::InvalidSplitFacts);
        }
        Ok(Self {
            remotely_accessible,
            addresses: Arc::from(addresses),
            affinity_key,
            split_weight,
            retained_size_in_bytes,
        })
    }

    pub const fn remotely_accessible(&self) -> bool {
        self.remotely_accessible
    }

    pub fn addresses(&self) -> &[ConnectorRecipeHostAddress] {
        &self.addresses
    }

    pub fn affinity_key(&self) -> Option<&str> {
        self.affinity_key.as_deref()
    }

    pub const fn split_weight(&self) -> u64 {
        self.split_weight
    }

    pub const fn retained_size_in_bytes(&self) -> u64 {
        self.retained_size_in_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadRecipeSplitKind {
    Data,
    TableChanges,
    ChangeWindow,
    SystemManifest,
    SystemRelation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadRecipeSplitDraft {
    kind: ConnectorReadRecipeSplitKind,
    payload: ConnectorEncodedPayload,
    facts: ConnectorReadRecipeSplitFacts,
}

impl ConnectorReadRecipeSplitDraft {
    pub const fn new(
        kind: ConnectorReadRecipeSplitKind,
        payload: ConnectorEncodedPayload,
        facts: ConnectorReadRecipeSplitFacts,
    ) -> Self {
        Self {
            kind,
            payload,
            facts,
        }
    }

    pub const fn kind(&self) -> ConnectorReadRecipeSplitKind {
        self.kind
    }

    pub const fn payload(&self) -> &ConnectorEncodedPayload {
        &self.payload
    }

    pub const fn facts(&self) -> &ConnectorReadRecipeSplitFacts {
        &self.facts
    }
}

/// A dynamic split whose private payload was checked by the exact provider
/// compiler. Construction is sealed so a raw Task update cannot impersonate
/// a validated provider input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadRecipeSplit(ConnectorReadRecipeSplitDraft);

impl ConnectorReadRecipeSplit {
    pub fn try_compile_with_provider<C: ConnectorReadRelationRecipeCompiler + ?Sized>(
        binding: &ConnectorReadBinding,
        draft: &ConnectorReadRecipeSplitDraft,
        compiler: &C,
    ) -> Result<Self, ConnectorReadRelationRecipeCompileError<C::Error>> {
        if binding.descriptor().instance_id != *binding.catalog_handle().catalog_name() {
            return Err(ConnectorReadRelationRecipeCompileError::Contract(
                ConnectorReadRelationRecipeError::BindingMismatch,
            ));
        }
        let check_header = |candidate: &ConnectorReadRecipeSplitDraft| {
            candidate
                .payload
                .header()
                .validate_expected::<ConnectorCodecContractError>(
                    &binding.descriptor().provider_id,
                    binding.catalog_handle(),
                    ConnectorCodecCategory::ReadSplit,
                    draft.payload.header().codec_revision(),
                )
                .map_err(ConnectorReadRelationRecipeError::Header)?;
            if candidate.payload.payload().len() > MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES {
                return Err(ConnectorReadRelationRecipeError::PayloadTooLarge);
            }
            Ok::<(), ConnectorReadRelationRecipeError>(())
        };
        check_header(draft).map_err(ConnectorReadRelationRecipeCompileError::Contract)?;
        let canonical = compiler
            .compile_split_private(binding, draft)
            .map_err(ConnectorReadRelationRecipeCompileError::Provider)?;
        check_header(&canonical).map_err(ConnectorReadRelationRecipeCompileError::Contract)?;
        if canonical.kind != draft.kind
            || canonical.facts != draft.facts
            || canonical.payload.header() != draft.payload.header()
        {
            return Err(ConnectorReadRelationRecipeCompileError::Contract(
                ConnectorReadRelationRecipeError::PublicFactsMismatch,
            ));
        }
        Ok(Self(ConnectorReadRecipeSplitDraft::new(
            canonical.kind,
            owned_payload(&canonical.payload),
            canonical.facts,
        )))
    }

    pub const fn draft(&self) -> &ConnectorReadRecipeSplitDraft {
        &self.0
    }
}

/// Structurally checked input. Private payloads have not been interpreted yet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadRelationRecipeDraft {
    binding: ConnectorReadBinding,
    relation: ConnectorReadRelationPayload,
    columns: Arc<[ConnectorEncodedPayload]>,
    payload_bytes: usize,
    charged_bytes: usize,
}

impl ConnectorReadRelationRecipeDraft {
    pub fn try_new(
        binding: ConnectorReadBinding,
        relation: ConnectorReadRelationPayload,
        columns: Vec<ConnectorEncodedPayload>,
    ) -> Result<Self, ConnectorReadRelationRecipeError> {
        let (payload_bytes, charged_bytes) =
            match validate_read_recipe(&binding, &relation, &columns, &mut || {
                Ok::<(), std::convert::Infallible>(())
            }) {
                Ok(facts) => facts,
                Err(ReadRecipeValidationError::Contract(error)) => return Err(error),
                Err(ReadRecipeValidationError::Observer(never)) => match never {},
            };
        let (relation, columns) = match copy_read_recipe(&relation, &columns, &mut PlainPayloadCopy)
        {
            Ok(owned) => owned,
            Err(never) => match never {},
        };
        Ok(Self {
            binding,
            relation,
            columns,
            payload_bytes,
            charged_bytes,
        })
    }

    /// Original structurally checked recipe law plus one detached owned copy.
    /// The caller owns the true source union, admission and checkpoint footer.
    pub fn try_new_observed(
        binding: &ConnectorReadBinding,
        relation: ConnectorReadRelationPayload,
        columns: Vec<ConnectorEncodedPayload>,
        source_retained_bytes: usize,
        admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PureProviderCompileError<ReadRecipeOwnedError>> {
        let mut observer =
            ObservedCopy::new(source_retained_bytes, admit, work).map_err(lift_read_owned_error)?;
        let identity = preflight_read_recipe_header(binding, &columns, &mut observer)
            .map_err(lift_read_owned_error)?;
        observer.flush().map_err(lift_read_owned_error)?;
        let header = validate_read_recipe_header(binding, &columns, &mut || {
            observer.step()?;
            observer.flush()
        });
        match header {
            Ok(()) => {}
            Err(ReadRecipeValidationError::Contract(error)) => {
                return Err(PureProviderCompileError::Provider(
                    ReadRecipeOwnedError::Contract(error),
                ));
            }
            Err(ReadRecipeValidationError::Observer(error)) => {
                return Err(lift_read_owned_error(error));
            }
        }
        preflight_read_recipe_payloads(&relation, &columns, identity, &mut observer)
            .map_err(lift_read_owned_error)?;
        observer.flush().map_err(lift_read_owned_error)?;
        let result = validate_read_recipe_payloads(binding, &relation, &columns, &mut || {
            observer.step()?;
            observer.flush()
        });
        let (payload_bytes, charged_bytes) = match result {
            Ok(facts) => facts,
            Err(ReadRecipeValidationError::Contract(error)) => {
                return Err(PureProviderCompileError::Provider(
                    ReadRecipeOwnedError::Contract(error),
                ));
            }
            Err(ReadRecipeValidationError::Observer(error)) => {
                return Err(lift_read_owned_error(error));
            }
        };
        observer.begin_copy().map_err(lift_read_owned_error)?;
        let (relation, columns) =
            copy_read_recipe(&relation, &columns, &mut ObservedPayloadCopy(&mut observer))
                .map_err(lift_read_owned_error)?;
        observer.flush().map_err(lift_read_owned_error)?;
        let binding = binding.clone();
        observer.step().map_err(lift_read_owned_error)?;
        observer.flush().map_err(lift_read_owned_error)?;
        Ok(Self {
            binding,
            relation,
            columns,
            payload_bytes,
            charged_bytes,
        })
    }

    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    pub const fn relation(&self) -> &ConnectorReadRelationPayload {
        &self.relation
    }

    pub fn columns(&self) -> &[ConnectorEncodedPayload] {
        &self.columns
    }

    pub const fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    pub const fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }

    pub fn into_parts(
        self,
    ) -> (
        ConnectorReadBinding,
        ConnectorReadRelationPayload,
        Vec<ConnectorEncodedPayload>,
    ) {
        (self.binding, self.relation, self.columns.to_vec())
    }
}

/// The original recipe contract and the original owned-resource rejection stay
/// distinct. Neither branch certifies provider-private payload semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadRecipeOwnedError {
    Contract(ConnectorReadRelationRecipeError),
    Resources(ConnectorError),
}
impl fmt::Display for ReadRecipeOwnedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => error.fmt(f),
            Self::Resources(error) => error.fmt(f),
        }
    }
}
impl Error for ReadRecipeOwnedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Contract(error) => error,
            Self::Resources(error) => error,
        })
    }
}
fn lift_read_owned_error(
    error: PureProviderCompileError<ConnectorError>,
) -> PureProviderCompileError<ReadRecipeOwnedError> {
    match error {
        PureProviderCompileError::Control(cause) => PureProviderCompileError::Control(cause),
        PureProviderCompileError::Provider(error) => {
            PureProviderCompileError::Provider(ReadRecipeOwnedError::Resources(error))
        }
    }
}
enum ReadRecipeValidationError<E> {
    Contract(ConnectorReadRelationRecipeError),
    Observer(E),
}
fn validate_read_recipe<E>(
    binding: &ConnectorReadBinding,
    relation: &ConnectorReadRelationPayload,
    columns: &[ConnectorEncodedPayload],
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<(usize, usize), ReadRecipeValidationError<E>> {
    validate_read_recipe_header(binding, columns, observe)?;
    validate_read_recipe_payloads(binding, relation, columns, observe)
}
fn validate_read_recipe_header<E>(
    binding: &ConnectorReadBinding,
    columns: &[ConnectorEncodedPayload],
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), ReadRecipeValidationError<E>> {
    let same_instance =
        binding.descriptor().instance_id == *binding.catalog_handle().catalog_name();
    observe().map_err(ReadRecipeValidationError::Observer)?;
    if !same_instance {
        return Err(ReadRecipeValidationError::Contract(
            ConnectorReadRelationRecipeError::BindingMismatch,
        ));
    }
    let too_many = columns.len() > MAX_CONNECTOR_RECIPE_COLUMNS;
    observe().map_err(ReadRecipeValidationError::Observer)?;
    if too_many {
        return Err(ReadRecipeValidationError::Contract(
            ConnectorReadRelationRecipeError::TooManyColumns,
        ));
    }
    Ok(())
}
fn validate_read_recipe_payloads<E>(
    binding: &ConnectorReadBinding,
    relation: &ConnectorReadRelationPayload,
    columns: &[ConnectorEncodedPayload],
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<(usize, usize), ReadRecipeValidationError<E>> {
    let revision = relation.table().header().codec_revision();
    let mut payload_bytes = 0usize;
    // The fixed charge covers the binding's bounded identity allocation.
    // Element sizes include each embedded header; its variable string
    // backing and allocation overhead are charged in the loop below.
    let initial = (|| {
        let charged_bytes = std::mem::size_of::<ConnectorReadRelationRecipeDraft>()
            .checked_add(RECIPE_IDENTITY_AND_ALLOCATION_CHARGE)
            .ok_or(ConnectorReadRelationRecipeError::RecipeTooLarge)?
            .checked_add(
                columns
                    .len()
                    .checked_mul(std::mem::size_of::<ConnectorEncodedPayload>())
                    .ok_or(ConnectorReadRelationRecipeError::RecipeTooLarge)?,
            )
            .ok_or(ConnectorReadRelationRecipeError::RecipeTooLarge)?;
        Ok::<usize, ConnectorReadRelationRecipeError>(charged_bytes)
    })();
    observe().map_err(ReadRecipeValidationError::Observer)?;
    let mut charged_bytes = initial.map_err(ReadRecipeValidationError::Contract)?;
    for (payload, category) in [
        (relation.table(), ConnectorCodecCategory::ReadTable),
        (relation.view(), ConnectorCodecCategory::ReadView),
    ]
    .into_iter()
    .chain(
        columns
            .iter()
            .map(|payload| (payload, ConnectorCodecCategory::ReadColumn)),
    ) {
        let header = payload
            .header()
            .validate_expected::<ConnectorCodecContractError>(
                &binding.descriptor().provider_id,
                binding.catalog_handle(),
                category,
                revision,
            )
            .map_err(ConnectorReadRelationRecipeError::Header);
        observe().map_err(ReadRecipeValidationError::Observer)?;
        header.map_err(ReadRecipeValidationError::Contract)?;
        let oversized = payload.payload().len() > MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES;
        observe().map_err(ReadRecipeValidationError::Observer)?;
        if oversized {
            return Err(ReadRecipeValidationError::Contract(
                ConnectorReadRelationRecipeError::PayloadTooLarge,
            ));
        }
        let charge = (|| {
            let next_payload_bytes = payload_bytes
                .checked_add(payload.payload().len())
                .ok_or(ConnectorReadRelationRecipeError::RecipeTooLarge)?;
            let next_charged_bytes = charged_bytes
                .checked_add(payload.payload().len())
                .and_then(|bytes| bytes.checked_add(payload.header().provider_id().as_str().len()))
                .and_then(|bytes| {
                    bytes.checked_add(payload.header().catalog().catalog_name().as_str().len())
                })
                .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
                .ok_or(ConnectorReadRelationRecipeError::RecipeTooLarge)?;
            Ok::<_, ConnectorReadRelationRecipeError>((next_payload_bytes, next_charged_bytes))
        })();
        observe().map_err(ReadRecipeValidationError::Observer)?;
        (payload_bytes, charged_bytes) = charge.map_err(ReadRecipeValidationError::Contract)?;
        if charged_bytes > MAX_CONNECTOR_RECIPE_BYTES {
            return Err(ReadRecipeValidationError::Contract(
                ConnectorReadRelationRecipeError::RecipeTooLarge,
            ));
        }
    }
    Ok((payload_bytes, charged_bytes))
}

fn preflight_read_recipe_header<O: OwnedCopy>(
    binding: &ConnectorReadBinding,
    columns: &Vec<ConnectorEncodedPayload>,
    observer: &mut O,
) -> Result<usize, O::Error> {
    // The caller's input Vec is source backing, not a fresh-output request.
    observer.source_floor(observer.add(
        size_of::<Vec<ConnectorEncodedPayload>>(),
        observer.mul(columns.capacity(), size_of::<ConnectorEncodedPayload>())?,
    )?)?;
    observer.source_floor(size_of::<ConnectorReadRelationPayload>())?;
    observer.source_floor(size_of::<ConnectorReadBinding>())?;
    observer.array::<ConnectorEncodedPayload>(columns.len(), 2)?;
    observer.arc_slice::<ConnectorEncodedPayload>(columns.len())?;
    observer.work(observer.add(
        observer.mul(columns.len(), 2 * size_of::<ConnectorEncodedPayload>())?,
        256,
    )?)?;
    let identity = observer.add(
        binding.descriptor().provider_id.as_str().len(),
        observer.add(
            binding.descriptor().instance_id.as_str().len(),
            binding.catalog_handle().catalog_name().as_str().len(),
        )?,
    )?;
    observer.work(observer.add(observer.mul(identity, 8)?, 128)?)?;
    Ok(identity)
}
fn preflight_read_recipe_payloads<O: OwnedCopy>(
    relation: &ConnectorReadRelationPayload,
    columns: &[ConnectorEncodedPayload],
    identity: usize,
    observer: &mut O,
) -> Result<(), O::Error> {
    for payload in [relation.table(), relation.view()]
        .into_iter()
        .chain(columns.iter())
    {
        observer.source_floor(payload.payload().len())?;
        observer.array::<u8>(payload.payload().len(), 1)?;
        if !payload.payload().is_empty() {
            let shared = novarocks_type_contract::owned_resources::layout::bytes_shared_upper()
                .map_err(|error| match error {
                    novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel =>
                        O::Error::from(ConnectorError::new(crate::ConnectorErrorKind::InvalidRequest,
                            "read recipe bytes request source model differs from the locked implementation")),
                    _ => observer.arithmetic(),
                })?;
            observer.array::<u8>(shared, 1)?;
        }
        let text = observer.add(
            payload.header().provider_id().as_str().len(),
            payload.header().catalog().catalog_name().as_str().len(),
        )?;
        observer.source_floor(text)?;
        observer.work(observer.add(
            observer.mul(payload.payload().len(), 2)?,
            observer.add(observer.mul(observer.add(identity, text)?, 8)?, 256)?,
        )?)?;
        observer.step()?;
    }
    Ok(())
}

trait PayloadCopy {
    type Error;
    fn reserve<T>(&mut self, count: usize) -> Result<Vec<T>, Self::Error>;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
}
struct PlainPayloadCopy;
impl PayloadCopy for PlainPayloadCopy {
    type Error = std::convert::Infallible;
    fn reserve<T>(&mut self, count: usize) -> Result<Vec<T>, Self::Error> {
        Ok(Vec::with_capacity(count))
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
struct ObservedPayloadCopy<'a, O>(&'a mut O);
impl<O: OwnedCopy> PayloadCopy for ObservedPayloadCopy<'_, O> {
    type Error = O::Error;
    fn reserve<T>(&mut self, count: usize) -> Result<Vec<T>, Self::Error> {
        self.0.flush()?;
        let mut output = Vec::new();
        let result = output.try_reserve_exact(count);
        self.0.reserve_exit(result)?;
        Ok(output)
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        self.0.step()
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush()
    }
}
fn owned_payload_with<P: PayloadCopy>(
    payload: &ConnectorEncodedPayload,
    policy: &mut P,
) -> Result<ConnectorEncodedPayload, P::Error> {
    policy.flush()?;
    let header = payload.header().clone();
    policy.step()?;
    policy.flush()?;
    let mut bytes = policy.reserve(payload.payload().len())?;
    for segment in payload.payload().chunks(256) {
        bytes.extend_from_slice(segment);
        policy.step()?;
    }
    policy.flush()?;
    let output = ConnectorEncodedPayload::new(header, bytes::Bytes::from(bytes));
    policy.step()?;
    policy.flush()?;
    Ok(output)
}
fn copy_read_recipe<P: PayloadCopy>(
    relation: &ConnectorReadRelationPayload,
    columns: &[ConnectorEncodedPayload],
    policy: &mut P,
) -> Result<(ConnectorReadRelationPayload, Arc<[ConnectorEncodedPayload]>), P::Error> {
    let relation = ConnectorReadRelationPayload::new(
        relation.kind(),
        owned_payload_with(relation.table(), policy)?,
        owned_payload_with(relation.view(), policy)?,
    );
    policy.step()?;
    let mut output = policy.reserve(columns.len())?;
    for column in columns {
        output.push(owned_payload_with(column, policy)?);
        policy.step()?;
    }
    policy.flush()?;
    let output = output.into_boxed_slice().into_vec();
    policy.step()?;
    policy.flush()?;
    let output = Arc::from(output);
    policy.step()?;
    policy.flush()?;
    Ok((relation, output))
}
fn owned_payload(payload: &ConnectorEncodedPayload) -> ConnectorEncodedPayload {
    match owned_payload_with(payload, &mut PlainPayloadCopy) {
        Ok(output) => output,
        Err(never) => match never {},
    }
}

/// Exact-generation pure recipe returned only after provider-private decode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadRelationRecipe(ConnectorReadRelationRecipeDraft);

/// A pure provider codec. The caller must select this from the exact installed
/// provider binding; no runtime adapter or credential participates in compile.
pub trait ConnectorReadRelationRecipeCompiler: Send + Sync {
    type Error: Error;

    fn compile_private(
        &self,
        draft: &ConnectorReadRelationRecipeDraft,
    ) -> Result<ConnectorReadRelationRecipeDraft, Self::Error>;

    fn compile_split_private(
        &self,
        binding: &ConnectorReadBinding,
        draft: &ConnectorReadRecipeSplitDraft,
    ) -> Result<ConnectorReadRecipeSplitDraft, Self::Error>;
}

#[derive(Debug)]
pub enum ConnectorReadRelationRecipeCompileError<E: Error> {
    Contract(ConnectorReadRelationRecipeError),
    Provider(E),
}

impl<E: Error> fmt::Display for ConnectorReadRelationRecipeCompileError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => fmt::Display::fmt(error, formatter),
            Self::Provider(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl<E: Error> Error for ConnectorReadRelationRecipeCompileError<E> {}

impl ConnectorReadRelationRecipe {
    /// The only safe constructor: provider-private validation followed by a
    /// neutral check that canonicalization preserved exact public facts.
    pub fn try_compile_with_provider<C: ConnectorReadRelationRecipeCompiler + ?Sized>(
        draft: &ConnectorReadRelationRecipeDraft,
        compiler: &C,
    ) -> Result<Self, ConnectorReadRelationRecipeCompileError<C::Error>> {
        let canonical = compiler
            .compile_private(draft)
            .map_err(ConnectorReadRelationRecipeCompileError::Provider)?;
        Self::validate_canonical_public_headers(draft, &canonical)
            .map_err(ConnectorReadRelationRecipeCompileError::Contract)?;
        Ok(Self(canonical))
    }

    pub(crate) fn validate_canonical_public_headers(
        draft: &ConnectorReadRelationRecipeDraft,
        canonical: &ConnectorReadRelationRecipeDraft,
    ) -> Result<(), ConnectorReadRelationRecipeError> {
        Self::validate_canonical_header_shape(draft, canonical)?;
        if canonical
            .columns
            .iter()
            .zip(draft.columns.iter())
            .any(|(canonical, original)| canonical.header() != original.header())
        {
            return Err(ConnectorReadRelationRecipeError::PublicFactsMismatch);
        }
        Ok(())
    }

    pub(crate) fn validate_canonical_header_shape(
        draft: &ConnectorReadRelationRecipeDraft,
        canonical: &ConnectorReadRelationRecipeDraft,
    ) -> Result<(), ConnectorReadRelationRecipeError> {
        if canonical.binding != draft.binding {
            return Err(ConnectorReadRelationRecipeError::BindingMismatch);
        }
        if canonical.relation.kind() != draft.relation.kind()
            || canonical.columns.len() != draft.columns.len()
            || canonical.relation.table().header() != draft.relation.table().header()
            || canonical.relation.view().header() != draft.relation.view().header()
        {
            return Err(ConnectorReadRelationRecipeError::PublicFactsMismatch);
        }
        Ok(())
    }

    pub const fn draft(&self) -> &ConnectorReadRelationRecipeDraft {
        &self.0
    }

    pub fn validate_binding(
        &self,
        binding: &ConnectorReadBinding,
    ) -> Result<(), ConnectorReadRelationRecipeError> {
        if &self.0.binding == binding {
            Ok(())
        } else {
            Err(ConnectorReadRelationRecipeError::BindingMismatch)
        }
    }
}

impl AsRef<ConnectorReadRelationRecipeDraft> for ConnectorReadRelationRecipeDraft {
    fn as_ref(&self) -> &ConnectorReadRelationRecipeDraft {
        self
    }
}

impl AsRef<ConnectorReadRelationRecipeDraft> for ConnectorReadRelationRecipe {
    fn as_ref(&self) -> &ConnectorReadRelationRecipeDraft {
        self.draft()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{
        CatalogHandle, CatalogVersion, ConnectorCodecRevision, ConnectorEnvelopeHeader,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        ConnectorReadRelationKind,
    };

    pub(super) fn binding(version: u8) -> ConnectorReadBinding {
        let instance_id = ConnectorInstanceId::try_from_canonical("lake").unwrap();
        ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: instance_id.clone(),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([version; 32])),
        )
    }

    pub(super) fn payload(
        binding: &ConnectorReadBinding,
        category: ConnectorCodecCategory,
        bytes: Bytes,
    ) -> ConnectorEncodedPayload {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            bytes,
        )
    }

    fn draft() -> ConnectorReadRelationRecipeDraft {
        let binding = binding(1);
        ConnectorReadRelationRecipeDraft::try_new(
            binding.clone(),
            ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                payload(
                    &binding,
                    ConnectorCodecCategory::ReadTable,
                    Bytes::from_static(b"table"),
                ),
                payload(
                    &binding,
                    ConnectorCodecCategory::ReadView,
                    Bytes::from_static(b"view"),
                ),
            ),
            vec![],
        )
        .unwrap()
    }

    struct IdentityCompiler;

    impl ConnectorReadRelationRecipeCompiler for IdentityCompiler {
        type Error = ConnectorReadRelationRecipeError;

        fn compile_private(
            &self,
            draft: &ConnectorReadRelationRecipeDraft,
        ) -> Result<ConnectorReadRelationRecipeDraft, Self::Error> {
            Ok(draft.clone())
        }

        fn compile_split_private(
            &self,
            _binding: &ConnectorReadBinding,
            draft: &ConnectorReadRecipeSplitDraft,
        ) -> Result<ConnectorReadRecipeSplitDraft, Self::Error> {
            Ok(draft.clone())
        }
    }

    #[test]
    fn exact_generation_is_required_at_rehydrate() {
        let recipe =
            ConnectorReadRelationRecipe::try_compile_with_provider(&draft(), &IdentityCompiler)
                .unwrap();
        assert_eq!(recipe.validate_binding(&binding(1)), Ok(()));
        assert_eq!(
            recipe.validate_binding(&binding(2)),
            Err(ConnectorReadRelationRecipeError::BindingMismatch)
        );
    }

    #[test]
    fn dynamic_split_requires_the_exact_provider_binding() {
        let current = binding(1);
        let facts =
            ConnectorReadRecipeSplitFacts::try_new(true, Vec::new(), None::<&str>, 100, 1).unwrap();
        let draft = ConnectorReadRecipeSplitDraft::new(
            ConnectorReadRecipeSplitKind::Data,
            payload(
                &current,
                ConnectorCodecCategory::ReadSplit,
                Bytes::from_static(b"split"),
            ),
            facts,
        );
        let validated = ConnectorReadRecipeSplit::try_compile_with_provider(
            &current,
            &draft,
            &IdentityCompiler,
        )
        .unwrap();
        assert_eq!(validated.draft(), &draft);
        assert!(matches!(
            ConnectorReadRecipeSplit::try_compile_with_provider(
                &binding(2),
                &draft,
                &IdentityCompiler,
            ),
            Err(ConnectorReadRelationRecipeCompileError::Contract(
                ConnectorReadRelationRecipeError::Header(
                    ConnectorCodecContractError::CatalogMismatch
                )
            ))
        ));
    }

    #[test]
    fn draft_rejects_cross_generation_and_oversized_payload() {
        let original = draft();
        let other = binding(2);
        let relation = ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            original.relation().table().clone(),
            payload(
                &other,
                ConnectorCodecCategory::ReadView,
                Bytes::from_static(b"view"),
            ),
        );
        assert_eq!(
            ConnectorReadRelationRecipeDraft::try_new(binding(1), relation, vec![]),
            Err(ConnectorReadRelationRecipeError::Header(
                ConnectorCodecContractError::CatalogMismatch
            ))
        );
        let current = binding(1);
        let oversized = payload(
            &current,
            ConnectorCodecCategory::ReadTable,
            Bytes::from(vec![0; MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES + 1]),
        );
        let relation = ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            oversized,
            payload(&current, ConnectorCodecCategory::ReadView, Bytes::new()),
        );
        assert_eq!(
            ConnectorReadRelationRecipeDraft::try_new(current, relation, vec![]),
            Err(ConnectorReadRelationRecipeError::PayloadTooLarge)
        );
    }

    #[test]
    fn split_facts_have_hard_bounds() {
        assert_eq!(
            ConnectorReadRecipeSplitFacts::try_new(
                true,
                vec![],
                None::<&str>,
                MAX_CONNECTOR_RECIPE_SPLIT_WEIGHT + 1,
                0,
            ),
            Err(ConnectorReadRelationRecipeError::InvalidSplitFacts)
        );
        assert_eq!(
            ConnectorReadRecipeSplitFacts::try_new(
                true,
                vec![],
                None::<&str>,
                1,
                MAX_CONNECTOR_RECIPE_BYTES as u64 + 1,
            ),
            Err(ConnectorReadRelationRecipeError::InvalidSplitFacts)
        );
    }
}

#[cfg(test)]
#[path = "recipe/owned_tests.rs"]
mod owned_tests;
