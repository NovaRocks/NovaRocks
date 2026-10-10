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

//! Frozen references to pre-existing delete artifacts, and the backend-side
//! read that turns them back into positions.
//!
//! This module owns the NCP-6 D10 inversion. Previously the frontend read every
//! old position-delete artifact at write activation, materialized a roaring
//! bitmap, and embedded the serialized bitmap in the writer handle. That put an
//! unbounded already-read object on the FE/BE boundary and made a distributed
//! write's delete merge depend on frontend object-store access.
//!
//! Now the frontend freezes only *exact references*: path, content and format
//! kind, file size, record count, the Puffin blob range when there is one, the
//! sequence and snapshot facts, the partition spec and descriptor, and the
//! non-secret storage route. The backend writer re-reads each reference through
//! its own query-leased storage resolver, validates it against the data file it
//! claims to belong to, merges, and writes the new artifact.
//!
//! Every failure mode is closed:
//!
//! | condition | verdict |
//! |---|---|
//! | reference missing from storage | error from `file_size` / the reader |
//! | unreadable | error from the reader |
//! | corrupt payload | `CorruptData` from the decoder |
//! | size, cardinality, or referenced data file disagrees | `CorruptData` |
//! | a position falls outside the referenced data file | `CorruptData` |
//!
//! There is deliberately no branch that turns any of those into an empty old
//! delete set, and no fallback to a frontend-inlined bitmap.

use crate::commit::model::EntryIdentity;
use std::collections::BTreeSet;

use novarocks_fs::FileCancellation;
use novarocks_spi::connector::{ConnectorError, ConnectorRequestContext};
use roaring::RoaringTreemap;

use crate::access_binding::IcebergReadBinding;
use crate::commit::write_stack::domain::{
    IcebergArtifactPartition, IcebergContentRange, corrupt, invalid, validate_location,
};
use crate::delete_file::{IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat};

/// The non-secret route an artifact is reachable through.
///
/// It carries a scheme, an optional authority (bucket or host), and the
/// directory prefix the artifact lives under. It never carries an access key, a
/// session token, an endpoint credential, or any other secret: the backend
/// resolves real access through its own query lease, and this value only says
/// *where* the artifact is, not *how* to be allowed to read it.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IcebergStorageRoute {
    scheme: String,
    authority: Option<String>,
    prefix: String,
}

impl IcebergStorageRoute {
    pub fn try_new(
        scheme: String,
        authority: Option<String>,
        prefix: String,
    ) -> Result<Self, ConnectorError> {
        if scheme.is_empty() {
            return Err(invalid("Iceberg storage route requires a scheme"));
        }
        if scheme.contains("://") || scheme.contains('\0') {
            return Err(invalid("Iceberg storage route scheme is malformed"));
        }
        if let Some(authority) = &authority {
            if authority.is_empty() || authority.contains('/') || authority.contains('\0') {
                return Err(invalid("Iceberg storage route authority is malformed"));
            }
            if authority.contains('@') || authority.contains(':') {
                return Err(invalid(
                    "Iceberg storage route authority must not embed credentials or a port userinfo",
                ));
            }
        }
        validate_location("storage route prefix", &prefix)?;
        Ok(Self {
            scheme,
            authority,
            prefix,
        })
    }

    /// Derive the route of an existing artifact location.
    ///
    /// The prefix is the artifact's parent directory taken from the location's
    /// own text, so the route provably covers the artifact it was derived from
    /// no matter how the scheme and authority are spelled.
    pub fn try_for_location(location: &str) -> Result<Self, ConnectorError> {
        validate_location("old delete artifact", location)?;
        let (scheme, authority) = match location.split_once("://") {
            Some((scheme, remainder)) => {
                let authority = remainder.split('/').next().unwrap_or_default();
                if authority.contains('@') {
                    return Err(invalid(
                        "Iceberg old delete artifact location must not embed credentials",
                    ));
                }
                (
                    scheme.to_string(),
                    (!authority.is_empty()).then(|| authority.to_string()),
                )
            }
            // A bare path is a local filesystem location.
            None => ("file".to_string(), None),
        };
        let prefix = match location.rfind('/') {
            Some(index) => location[..=index].to_string(),
            None => {
                return Err(invalid(
                    "Iceberg old delete artifact location has no directory prefix",
                ));
            }
        };
        Self::try_new(scheme, authority, prefix)
    }

    /// Whether this route covers `location`. A reference whose route does not
    /// cover its own path is rejected: the route would then name storage the
    /// artifact does not live in.
    pub fn covers(&self, location: &str) -> bool {
        location.starts_with(&self.prefix)
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }
    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// One exact reference to a pre-existing delete artifact.
///
/// The frontend freezes this; it never reads the artifact. Every field is a
/// fact the backend can independently re-verify against storage, so a reference
/// that has drifted is detectable rather than silently trusted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergOldDeleteArtifactRef {
    path: String,
    content: IcebergFileContent,
    file_format: IcebergFileFormat,
    file_size_in_bytes: u64,
    record_count: Option<u64>,
    content_range: Option<IcebergContentRange>,
    referenced_data_file: Option<String>,
    data_sequence_number: Option<i64>,
    added_snapshot_id: Option<i64>,
    partition_spec_id: i32,
    storage_route: IcebergStorageRoute,
}

impl IcebergOldDeleteArtifactRef {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        path: String,
        content: IcebergFileContent,
        file_format: IcebergFileFormat,
        file_size_in_bytes: u64,
        record_count: Option<u64>,
        content_range: Option<IcebergContentRange>,
        referenced_data_file: Option<String>,
        data_sequence_number: Option<i64>,
        added_snapshot_id: Option<i64>,
        partition_spec_id: i32,
        storage_route: IcebergStorageRoute,
    ) -> Result<Self, ConnectorError> {
        validate_location("old delete artifact", &path)?;
        if content != IcebergFileContent::PositionDeletes {
            return Err(invalid(
                "Iceberg old delete reference must describe a position-delete artifact",
            ));
        }
        if file_size_in_bytes == 0 {
            return Err(invalid(
                "Iceberg old delete reference must carry a positive file size",
            ));
        }
        if record_count == Some(0) {
            return Err(invalid(
                "Iceberg old delete reference record count, when known, must be positive",
            ));
        }
        let size = i64::try_from(file_size_in_bytes)
            .map_err(|_| invalid("Iceberg old delete reference file size overflows i64"))?;
        match (file_format, content_range) {
            (IcebergFileFormat::Puffin, Some(range)) => {
                if range.end() > size {
                    return Err(corrupt(
                        "Iceberg old deletion-vector blob range extends past its Puffin file",
                    ));
                }
                if referenced_data_file.is_none() {
                    return Err(invalid(
                        "Iceberg old deletion-vector reference must name its referenced data file",
                    ));
                }
            }
            (IcebergFileFormat::Puffin, None) => {
                return Err(invalid(
                    "Iceberg old deletion-vector reference requires its Puffin blob range",
                ));
            }
            (IcebergFileFormat::Parquet, None) => {}
            (IcebergFileFormat::Parquet, Some(_)) => {
                return Err(invalid(
                    "Iceberg old Parquet position-delete reference must not carry a blob range",
                ));
            }
            (IcebergFileFormat::Unknown, _) => {
                return Err(invalid(
                    "Iceberg old delete reference requires an exact file format",
                ));
            }
        }
        if let Some(referenced) = &referenced_data_file {
            validate_location("old delete referenced data file", referenced)?;
        }
        if partition_spec_id < 0 {
            return Err(invalid(
                "Iceberg old delete reference partition spec id must not be negative",
            ));
        }
        if data_sequence_number.is_some_and(|value| value < 0)
            || added_snapshot_id.is_some_and(|value| value < 0)
        {
            return Err(invalid(
                "Iceberg old delete reference sequence facts must not be negative",
            ));
        }
        if !storage_route.covers(&path) {
            return Err(invalid(
                "Iceberg old delete reference storage route does not cover its own artifact",
            ));
        }
        Ok(Self {
            path,
            content,
            file_format,
            file_size_in_bytes,
            record_count,
            content_range,
            referenced_data_file,
            data_sequence_number,
            added_snapshot_id,
            partition_spec_id,
            storage_route,
        })
    }

    pub fn entry_identity(&self) -> EntryIdentity {
        match self.file_format {
            IcebergFileFormat::Puffin => {
                let range = self.content_range.expect("validated Puffin content range");
                EntryIdentity::DeletionVector {
                    path: self.path.clone(),
                    offset: range.offset(),
                    length: range.size_in_bytes(),
                    referenced_data_file: self
                        .referenced_data_file
                        .clone()
                        .expect("validated Puffin referenced data file"),
                }
            }
            _ => EntryIdentity::DeleteFile {
                path: self.path.clone(),
            },
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub const fn content(&self) -> IcebergFileContent {
        self.content
    }
    pub const fn file_format(&self) -> IcebergFileFormat {
        self.file_format
    }
    pub const fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }
    /// The artifact's exact row count, when the frozen manifest projection
    /// supplies one.
    ///
    /// Iceberg's manifest carries a record count per delete file, but the
    /// provider's current read-model projection
    /// (`crate::read_model::IcebergReadDeleteFile`) does not surface it, so
    /// this is `None` for references frozen from that projection. `None`
    /// weakens only the *exactness* of the backend's count check; it never
    /// weakens the fail-closed rule, because an exclusive artifact that decodes
    /// to nothing is still rejected.
    pub const fn record_count(&self) -> Option<u64> {
        self.record_count
    }
    pub const fn content_range(&self) -> Option<IcebergContentRange> {
        self.content_range
    }
    pub fn referenced_data_file(&self) -> Option<&str> {
        self.referenced_data_file.as_deref()
    }
    pub const fn data_sequence_number(&self) -> Option<i64> {
        self.data_sequence_number
    }
    pub const fn added_snapshot_id(&self) -> Option<i64> {
        self.added_snapshot_id
    }
    pub const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }
    pub const fn storage_route(&self) -> &IcebergStorageRoute {
        &self.storage_route
    }

    /// Whether this artifact belongs to exactly one data file. Only an
    /// exclusive reference may be required to yield at least one position.
    pub const fn is_exclusive(&self) -> bool {
        self.referenced_data_file.is_some()
    }

    fn to_spec(&self) -> IcebergDeleteFileSpec {
        IcebergDeleteFileSpec {
            path: self.path.clone(),
            file_format: self.file_format,
            file_content: self.content,
            length: Some(self.file_size_in_bytes),
            content_offset: self.content_range.map(|range| range.offset()),
            content_size_in_bytes: self.content_range.map(|range| range.size_in_bytes()),
            referenced_data_file: self.referenced_data_file.clone(),
        }
    }
}

/// Everything one logical writer needs to merge old deletes for one data file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergOldDeleteMergeTarget {
    data_file_path: String,
    data_file_record_count: u64,
    data_file_sequence_number: Option<i64>,
    partition: IcebergArtifactPartition,
    base_snapshot_id: i64,
    references: Vec<IcebergOldDeleteArtifactRef>,
}

impl IcebergOldDeleteMergeTarget {
    pub fn try_new(
        data_file_path: String,
        data_file_record_count: u64,
        data_file_sequence_number: Option<i64>,
        partition: IcebergArtifactPartition,
        base_snapshot_id: i64,
        mut references: Vec<IcebergOldDeleteArtifactRef>,
    ) -> Result<Self, ConnectorError> {
        validate_location("old delete merge data file", &data_file_path)?;
        if base_snapshot_id < 0 {
            return Err(invalid(
                "Iceberg old delete merge target requires a frozen base snapshot",
            ));
        }
        references.sort_by_key(IcebergOldDeleteArtifactRef::entry_identity);
        let mut seen = BTreeSet::new();
        for reference in &references {
            if !seen.insert(reference.entry_identity()) {
                return Err(invalid(
                    "Iceberg old delete merge target repeats a delete artifact",
                ));
            }
            if let Some(referenced) = reference.referenced_data_file()
                && referenced != data_file_path
            {
                return Err(invalid(format!(
                    "Iceberg old delete artifact {} belongs to data file {referenced}, not {data_file_path}",
                    reference.path()
                )));
            }
            if reference.partition_spec_id() != partition.partition_spec_id() {
                return Err(invalid(format!(
                    "Iceberg old delete artifact {} names partition spec {} but its data file uses {}",
                    reference.path(),
                    reference.partition_spec_id(),
                    partition.partition_spec_id()
                )));
            }
        }
        Ok(Self {
            data_file_path,
            data_file_record_count,
            data_file_sequence_number,
            partition,
            base_snapshot_id,
            references,
        })
    }

    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }
    pub const fn data_file_record_count(&self) -> u64 {
        self.data_file_record_count
    }
    pub const fn data_file_sequence_number(&self) -> Option<i64> {
        self.data_file_sequence_number
    }
    pub const fn partition(&self) -> &IcebergArtifactPartition {
        &self.partition
    }
    pub const fn base_snapshot_id(&self) -> i64 {
        self.base_snapshot_id
    }
    pub fn references(&self) -> &[IcebergOldDeleteArtifactRef] {
        &self.references
    }

    /// Every location the backend must be able to reach to honour this target.
    pub fn locations(&self) -> Vec<&str> {
        self.references
            .iter()
            .map(IcebergOldDeleteArtifactRef::path)
            .collect()
    }
}

/// What a validated old-delete read produced.
#[derive(Clone, Debug, Default)]
pub struct IcebergOldDeleteMergeOutcome {
    positions: RoaringTreemap,
    merged_references: Vec<EntryIdentity>,
}

impl IcebergOldDeleteMergeOutcome {
    pub const fn positions(&self) -> &RoaringTreemap {
        &self.positions
    }

    pub fn into_positions(self) -> RoaringTreemap {
        self.positions
    }

    /// The exact artifacts that were read, sorted. A staged artifact records
    /// this so `finish_write` can prove the new artifact superseded exactly the
    /// references the session froze.
    pub fn merged_references(&self) -> &[EntryIdentity] {
        &self.merged_references
    }
}

/// Read, validate, and merge every frozen old-delete reference for one data
/// file.
///
/// The read happens through `binding.for_request(context)`, which is what
/// installs the request's storage resolver, so a vended-credential catalog
/// resolves its access from the query lease rather than from any process-global
/// state.
pub fn read_and_merge_old_deletes(
    target: &IcebergOldDeleteMergeTarget,
    binding: &IcebergReadBinding,
    context: &ConnectorRequestContext,
) -> Result<IcebergOldDeleteMergeOutcome, ConnectorError> {
    novarocks_spi::connector::ConnectorOperationControl::check_active(context)?;
    if target.references().is_empty() {
        // The session froze "this data file has no old deletes". That is a
        // decision, not a read result, so it is the one legal empty outcome.
        return Ok(IcebergOldDeleteMergeOutcome::default());
    }
    let request_binding = binding.for_request(context.clone());
    if request_binding.requires_request_storage_resolver() && context.storage_resolver().is_none() {
        return Err(invalid(
            "Iceberg old delete merge requires the query-leased storage resolver",
        ));
    }
    let access = request_binding.resolve_access_for_locations(target.locations())?;
    let file_context = request_binding.file_read_context(
        FileCancellation::from_connector_request(context),
        context.deadline(),
    )?;

    let mut positions = RoaringTreemap::new();
    let mut merged_references = Vec::with_capacity(target.references().len());
    for reference in target.references() {
        file_context
            .check_active()
            .map_err(crate::file_reader::map_file_error)?;
        // A stale or replaced artifact is caught before it is parsed: the
        // frozen size is an exact fact, and a differing observed size means the
        // reference no longer describes what is in storage. A missing artifact
        // fails here too, and never degrades into an empty delete set.
        let observed = request_binding.file_size(reference.path(), &access, &file_context)?;
        if observed != reference.file_size_in_bytes() {
            return Err(corrupt(format!(
                "Iceberg old delete artifact {} is stale: frozen size {} but storage reports {observed}",
                reference.path(),
                reference.file_size_in_bytes()
            )));
        }
        let decoded = crate::position_delete::load_position_deletes_with_context(
            &[reference.to_spec()],
            target.data_file_path(),
            &access,
            &file_context,
        )
        .map_err(|error| match file_context.check_active() {
            Err(stopped) => crate::file_reader::map_file_error(stopped),
            Ok(()) => corrupt(format!(
                "read Iceberg old delete artifact {} failed: {error}",
                reference.path()
            )),
        })?;
        file_context
            .check_active()
            .map_err(crate::file_reader::map_file_error)?;
        validate_decoded_positions(target, reference, &decoded)?;
        positions |= decoded;
        merged_references.push(reference.entry_identity());
    }
    file_context
        .check_active()
        .map_err(crate::file_reader::map_file_error)?;
    merged_references.sort();
    Ok(IcebergOldDeleteMergeOutcome {
        positions,
        merged_references,
    })
}

fn validate_decoded_positions(
    target: &IcebergOldDeleteMergeTarget,
    reference: &IcebergOldDeleteArtifactRef,
    decoded: &RoaringTreemap,
) -> Result<(), ConnectorError> {
    let decoded_count = decoded.len();
    match (reference.is_exclusive(), reference.record_count()) {
        // An exclusive artifact is entirely about this data file, so a known
        // record count is an exact expectation. A short or empty read is
        // corruption, never an empty old delete set.
        (true, Some(expected)) if decoded_count != expected => {
            return Err(corrupt(format!(
                "Iceberg old delete artifact {} claims {expected} positions for {} but decoded {decoded_count}",
                reference.path(),
                target.data_file_path()
            )));
        }
        // With no frozen count, an exclusive artifact must still contribute at
        // least one position: it was frozen precisely because the manifest
        // attaches it to this data file, so an empty read means the artifact is
        // missing content, not that the delete set is empty.
        (true, None) if decoded_count == 0 => {
            return Err(corrupt(format!(
                "Iceberg old delete artifact {} decoded no positions for {}",
                reference.path(),
                target.data_file_path()
            )));
        }
        // A shared Parquet delete file may cover several data files, so it may
        // contribute fewer positions than it holds — but never more.
        (false, Some(held)) if decoded_count > held => {
            return Err(corrupt(format!(
                "Iceberg old delete artifact {} decoded {decoded_count} positions for {} but holds only {held}",
                reference.path(),
                target.data_file_path()
            )));
        }
        _ => {}
    }
    if let Some(highest) = decoded.max()
        && highest >= target.data_file_record_count()
    {
        return Err(corrupt(format!(
            "Iceberg old delete artifact {} deletes position {highest} of {} which has only {} rows",
            reference.path(),
            target.data_file_path(),
            target.data_file_record_count()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::write_stack::test_support::{parquet_ref, puffin_ref, sample_partition};

    #[test]
    fn a_storage_route_is_derived_from_the_artifact_and_covers_it() {
        let route = IcebergStorageRoute::try_for_location("s3://bucket/wh/db/t/data/d-0.parquet")
            .expect("route");
        assert_eq!(route.scheme(), "s3");
        assert_eq!(route.authority(), Some("bucket"));
        assert_eq!(route.prefix(), "s3://bucket/wh/db/t/data/");
        assert!(route.covers("s3://bucket/wh/db/t/data/d-0.parquet"));
        assert!(!route.covers("s3://bucket/wh/db/other/data/d-0.parquet"));
    }

    #[test]
    fn a_storage_route_never_carries_a_credential() {
        assert!(
            IcebergStorageRoute::try_for_location("s3://key:secret@bucket/a/b.parquet").is_err()
        );
        assert!(
            IcebergStorageRoute::try_new(
                "s3".to_string(),
                Some("k:s@bucket".to_string()),
                "s3://x/".to_string()
            )
            .is_err()
        );
    }

    #[test]
    fn a_puffin_reference_requires_its_blob_range_and_its_data_file() {
        assert!(puffin_ref("s3://b/x.puffin", Some("s3://b/a.parquet"), 4096, 3, 0, 64).is_ok());
        // No blob range at all.
        assert!(
            IcebergOldDeleteArtifactRef::try_new(
                "s3://b/x.puffin".to_string(),
                IcebergFileContent::PositionDeletes,
                IcebergFileFormat::Puffin,
                4096,
                Some(3),
                None,
                Some("s3://b/a.parquet".to_string()),
                Some(1),
                Some(2),
                0,
                IcebergStorageRoute::try_for_location("s3://b/x.puffin").expect("route"),
            )
            .is_err()
        );
        // Range past the end of the file.
        assert!(puffin_ref("s3://b/x.puffin", Some("s3://b/a.parquet"), 32, 3, 0, 64).is_err());
        // Puffin without a referenced data file.
        assert!(puffin_ref("s3://b/x.puffin", None, 4096, 3, 0, 64).is_err());
    }

    #[test]
    fn a_parquet_reference_must_not_carry_a_blob_range() {
        assert!(parquet_ref("s3://b/d.parquet", None, 4096, 9).is_ok());
        assert!(
            IcebergOldDeleteArtifactRef::try_new(
                "s3://b/d.parquet".to_string(),
                IcebergFileContent::PositionDeletes,
                IcebergFileFormat::Parquet,
                4096,
                Some(9),
                Some(IcebergContentRange::try_new(0, 16).expect("range")),
                None,
                None,
                None,
                0,
                IcebergStorageRoute::try_for_location("s3://b/d.parquet").expect("route"),
            )
            .is_err()
        );
    }

    #[test]
    fn a_zero_size_reference_cannot_exist_and_a_known_count_must_be_positive() {
        assert!(parquet_ref("s3://b/d.parquet", None, 0, 9).is_err());
        // An unknown record count is legal — the manifest projection does not
        // always surface one — but a *claimed* count of zero is not: it would
        // assert that the artifact deletes nothing while still existing.
        assert!(parquet_ref("s3://b/d.parquet", None, 4096, 0).is_ok());
        assert!(
            IcebergOldDeleteArtifactRef::try_new(
                "s3://b/d.parquet".to_string(),
                IcebergFileContent::PositionDeletes,
                IcebergFileFormat::Parquet,
                4096,
                Some(0),
                None,
                None,
                None,
                None,
                0,
                IcebergStorageRoute::try_for_location("s3://b/d.parquet").expect("route"),
            )
            .is_err()
        );
    }

    #[test]
    fn a_merge_target_rejects_a_reference_for_another_data_file() {
        let foreign = parquet_ref("s3://b/d.parquet", Some("s3://b/other.parquet"), 4096, 9)
            .expect("reference");
        let error = IcebergOldDeleteMergeTarget::try_new(
            "s3://b/a.parquet".to_string(),
            100,
            Some(4),
            sample_partition(),
            77,
            vec![foreign],
        )
        .expect_err("mismatched reference");
        assert!(error.message().contains("belongs to data file"));
    }

    #[test]
    fn a_merge_target_rejects_a_repeated_artifact_and_sorts_the_rest() {
        let one = parquet_ref("s3://b/d1.parquet", None, 4096, 9).expect("reference");
        let two = parquet_ref("s3://b/d2.parquet", None, 4096, 9).expect("reference");
        assert!(
            IcebergOldDeleteMergeTarget::try_new(
                "s3://b/a.parquet".to_string(),
                100,
                None,
                sample_partition(),
                77,
                vec![one.clone(), one.clone()],
            )
            .is_err()
        );
        let target = IcebergOldDeleteMergeTarget::try_new(
            "s3://b/a.parquet".to_string(),
            100,
            None,
            sample_partition(),
            77,
            vec![two, one],
        )
        .expect("target");
        assert_eq!(
            target.locations(),
            vec!["s3://b/d1.parquet", "s3://b/d2.parquet"]
        );
    }

    #[test]
    fn decoded_positions_must_agree_with_the_frozen_reference() {
        let exclusive = puffin_ref("s3://b/x.puffin", Some("s3://b/a.parquet"), 4096, 3, 0, 64)
            .expect("reference");
        let shared = parquet_ref("s3://b/d.parquet", None, 4096, 9).expect("reference");
        let target = IcebergOldDeleteMergeTarget::try_new(
            "s3://b/a.parquet".to_string(),
            100,
            None,
            sample_partition(),
            77,
            vec![exclusive.clone(), shared.clone()],
        )
        .expect("target");

        let exact = RoaringTreemap::from_iter([1_u64, 2, 3]);
        assert!(validate_decoded_positions(&target, &exclusive, &exact).is_ok());

        // An exclusive reference that decodes short is corruption, never empty.
        let short = RoaringTreemap::from_iter([1_u64]);
        assert!(validate_decoded_positions(&target, &exclusive, &short).is_err());
        assert!(
            validate_decoded_positions(&target, &exclusive, &RoaringTreemap::new()).is_err(),
            "an empty read of an exclusive artifact must never be accepted as an empty delete set"
        );

        // A shared artifact may contribute fewer positions, but never more.
        assert!(validate_decoded_positions(&target, &shared, &short).is_ok());
        assert!(validate_decoded_positions(&target, &shared, &RoaringTreemap::new()).is_ok());
        let too_many = RoaringTreemap::from_iter(0_u64..10);
        assert!(validate_decoded_positions(&target, &shared, &too_many).is_err());

        // A position outside the referenced data file is a mismatch.
        let out_of_range = RoaringTreemap::from_iter([1_u64, 2, 100]);
        let error = validate_decoded_positions(&target, &exclusive, &out_of_range)
            .expect_err("position past the data file");
        assert!(error.message().contains("which has only 100 rows"));
    }
}
