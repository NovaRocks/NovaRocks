// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Applicable publication from the exact current carrier and table markers.

use super::codec::keyed::KeyedPublicationDocument;
use super::codec::{
    Marker, PublicationEvidence, RECOVERY_RECORD_NAME, RETIREMENT_RECORD_NAME, RetirementRecord,
};
use super::documents::MvDocumentError;
use super::identity::{DocumentRevision, PublicationIdentity};
use super::keyed_documents::{MV_DOCUMENT_OWNER, never_published};
use novarocks_spi::connector::{
    ConnectorMarkerKey, ConnectorSnapshotRecord, KeyedDocumentView, KeyedFullRefreshMarkers,
};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicablePublication {
    Bound(PublicationIdentity),
    RetiredPublication(RetirementRecord),
    NeverPublished(DocumentRevision),
    Unattributed,
}

impl ApplicablePublication {
    pub fn evidence(&self) -> PublicationEvidence<'_> {
        match self {
            Self::Bound(publication) => PublicationEvidence::Current(publication),
            Self::RetiredPublication(retirement) => {
                PublicationEvidence::Retired(&retirement.retired_publication)
            }
            Self::NeverPublished(binding) => PublicationEvidence::Unpublished(binding),
            Self::Unattributed => PublicationEvidence::Unattributed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenMvMarkerKeys(BTreeSet<ConnectorMarkerKey>);

impl FrozenMvMarkerKeys {
    pub fn capture(view: &KeyedDocumentView) -> Self {
        Self(current_mv_marker_keys(view).cloned().collect())
    }

    pub fn matches(&self, view: &KeyedDocumentView) -> bool {
        self.0.iter().eq(current_mv_marker_keys(view))
    }

    pub fn keys(&self) -> &BTreeSet<ConnectorMarkerKey> {
        &self.0
    }

    pub fn into_keys(self) -> BTreeSet<ConnectorMarkerKey> {
        self.0
    }

    pub fn into_full_refresh_policy(self) -> KeyedFullRefreshMarkers {
        KeyedFullRefreshMarkers::Fenced {
            owner: MV_DOCUMENT_OWNER.into(),
            keys: self.0,
        }
    }
}

pub fn rebuild_mv_marker_policy() -> KeyedFullRefreshMarkers {
    KeyedFullRefreshMarkers::ClearObserved {
        owner: MV_DOCUMENT_OWNER.into(),
        names: ["invalid", "pending", "pending-released"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
    }
}

fn current_mv_marker_keys(view: &KeyedDocumentView) -> impl Iterator<Item = &ConnectorMarkerKey> {
    view.markers
        .keys()
        .filter(|key| key.owner == MV_DOCUMENT_OWNER)
}

pub(super) struct CurrentPublicationRecords<'a> {
    pub(super) publication: Option<&'a ConnectorSnapshotRecord>,
    pub(super) retirement: Option<&'a ConnectorSnapshotRecord>,
    pub(super) recovery: Option<&'a ConnectorSnapshotRecord>,
}

pub(super) fn current_publication_records(
    snapshot: Option<i64>,
    snapshot_records: &[ConnectorSnapshotRecord],
) -> Result<CurrentPublicationRecords<'_>, MvDocumentError> {
    let mut records = CurrentPublicationRecords {
        publication: None,
        retirement: None,
        recovery: None,
    };
    for record in snapshot_records {
        if record.owner != MV_DOCUMENT_OWNER {
            continue;
        }
        let slot = match record.name.as_str() {
            "publication" => &mut records.publication,
            RETIREMENT_RECORD_NAME => &mut records.retirement,
            RECOVERY_RECORD_NAME => &mut records.recovery,
            _ => continue,
        };
        if slot.replace(record).is_some() {
            return Err(MvDocumentError::Contract(
                "duplicate MV record on one snapshot".into(),
            ));
        }
    }
    if records.publication.is_some() && records.retirement.is_some() {
        return Err(MvDocumentError::Contract(
            "MV current snapshot carries both publication and retirement records".into(),
        ));
    }
    if records.recovery.is_some() && records.publication.is_none() {
        return Err(MvDocumentError::Contract(
            "MV recovery record must accompany its publication".into(),
        ));
    }
    if (records.publication.is_some() || records.retirement.is_some()) && snapshot.is_none() {
        return Err(MvDocumentError::Contract(
            "MV publication or retirement record has no carrier snapshot".into(),
        ));
    }
    Ok(records)
}

pub(super) fn resolve_applicable_publication(
    view: &KeyedDocumentView,
    publication: Option<&KeyedPublicationDocument>,
    retirement: Option<&ConnectorSnapshotRecord>,
    root_binding: DocumentRevision,
    markers: &[(ConnectorMarkerKey, Marker)],
) -> Result<ApplicablePublication, MvDocumentError> {
    if let Some(publication) = publication {
        return Ok(ApplicablePublication::Bound(
            publication.publication_id.clone(),
        ));
    }
    if let Some(record) = retirement {
        let retirement = RetirementRecord::decode(record)?;
        let revision = retirement.revision()?;
        if !markers.iter().any(|(_, marker)| matches!(marker,
            Marker::Invalid { publication, retirement_record_revision }
                if publication == &retirement.retired_publication && *retirement_record_revision == revision
        )) {
            return Err(MvDocumentError::Contract(
                "MV retirement lacks its exact Invalid marker".into(),
            ));
        }
        return Ok(ApplicablePublication::RetiredPublication(retirement));
    }
    if never_published(view) {
        Ok(ApplicablePublication::NeverPublished(root_binding))
    } else {
        Ok(ApplicablePublication::Unattributed)
    }
}

#[cfg(test)]
mod tests;
