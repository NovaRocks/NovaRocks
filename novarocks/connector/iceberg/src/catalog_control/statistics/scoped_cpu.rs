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

//! One measured statistics intent, prepared on fresh authoritative metadata.

//! The three original Statistics CPU phases. Every input and partial output
//! resides in this same Provider Source; the worker only borrows a loan.
use super::*;
use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::model::FrozenRequest;
use crate::commit::operation::IcebergCommitOperation;
use crate::iceberg::puffin::Blob;
use crate::iceberg::spec::BlobMetadata;
use novarocks_spi::connector::{StatisticsCpuInput, StatisticsCpuPort};
use std::sync::{Mutex, Weak};

type PublicationResult = Result<async_publication::PreparedStatisticsPublication, ConnectorError>;
type EvidenceResult = crate::iceberg::Result<(ExternalMutationEvidence, StatisticsReceipt)>;
#[path = "scoped_capture.rs"]
pub(crate) mod scoped_capture;
#[path = "scoped_prepared_source.rs"]
pub(crate) mod scoped_prepared_source;
#[path = "scoped_prepared_split.rs"]
pub(crate) mod scoped_prepared_split;
#[path = "scoped_read.rs"]
mod scoped_read;
#[path = "scoped_snapshot.rs"]
pub(crate) mod scoped_snapshot;
enum CpuBody {
    ViewListing(Box<crate::catalog_control::views::async_listing::Work>),
    Compaction(crate::catalog_control::metadata_maintenance::observation::CompactionWork),
    PreparedSource(Box<scoped_prepared_source::Work>),
    PreparedSplit(Box<scoped_prepared_split::Work>),
    Cleanup(Box<crate::catalog_control::cleanup_maintenance::async_original::CpuWork>),
    ReadSnapshot(scoped_snapshot::SnapshotBody),
    Read(Box<scoped_read::ReadBody>),
    MetadataBinding {
        provider: IcebergMetadata,
        facts: crate::metadata::IcebergPreparedTableFacts,
        expected: Option<novarocks_spi::connector::ConnectorTableObjectId>,
        result: Option<
            Result<
                (
                    novarocks_spi::connector::ConnectorTableObjectBinding,
                    Option<i64>,
                ),
                ConnectorError,
            >,
        >,
    },
    Schema {
        provider: IcebergMetadata,
        request: StatisticsCollectionStartRequest,
        table_payload: IcebergTablePayload,
        physical: crate::iceberg::table::Table,
        cpu: Weak<dyn StatisticsCpuPort>,
        result: Option<Result<AsyncStatisticsCollectionStart, ConnectorError>>,
    },
    Artifacts {
        session: Arc<StatisticsSessionBody>,
        drafts: Option<Vec<StatisticsArtifactDraft>>,
        cpu: Weak<dyn StatisticsCpuPort>,
        result: Option<PublicationResult>,
    },
    Blob {
        drafts: Arc<[StatisticsArtifactDraft]>,
        index: usize,
        snapshot: i64,
        sequence: i64,
        result: Option<(Blob, BlobMetadata)>,
    },
    Evidence {
        projection: publication::Projection,
        request: Option<FrozenRequest>,
        operation: IcebergCommitOperation,
        journal: PublicationJournal<StatisticsReceipt>,
        result: Option<EvidenceResult>,
    },
}
struct OriginalBody {
    started: bool,
    body: CpuBody,
}
pub(crate) struct IcebergMetadataCpuOriginal {
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    incarnation: novarocks_spi::connector::ProviderBindingEpoch,
    state: Mutex<Option<OriginalBody>>,
}
pub(crate) type StatisticsCpuOriginal = IcebergMetadataCpuOriginal;
struct BodyLoan<'a> {
    source: &'a IcebergMetadataCpuOriginal,
    body: Option<OriginalBody>,
}
impl<'a> BodyLoan<'a> {
    fn take(source: &'a IcebergMetadataCpuOriginal) -> Self {
        let body = source
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        assert!(body.is_some(), "one original Statistics CPU loan");
        Self { source, body }
    }
}
impl Drop for BodyLoan<'_> {
    fn drop(&mut self) {
        let mut target = self.source.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(target.is_none(), "same Statistics CPU receiver");
        *target = self.body.take();
    }
}
impl IcebergMetadataCpuOriginal {
    pub(crate) fn view_listing(
        provider: IcebergMetadata,
        request: novarocks_spi::connector::ConnectorListViewsRequest,
    ) -> Arc<Self> {
        Self::new(
            provider.descriptor().clone(),
            provider.incarnation(),
            CpuBody::ViewListing(Box::new(
                crate::catalog_control::views::async_listing::Work::new(provider, request),
            )),
        )
    }
    pub(crate) fn view_listing_context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
        let original = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let CpuBody::ViewListing(work) = &original.as_ref().expect("same listing source").body
        else {
            panic!("same view listing source")
        };
        work.context().clone()
    }
    pub(crate) fn view_listing_receive(&self, raw: Result<Vec<u8>, ConnectorError>) {
        let mut loan = BodyLoan::take(self);
        let original = loan.body.as_mut().unwrap();
        let CpuBody::ViewListing(work) = &mut original.body else {
            panic!("same view listing source")
        };
        work.receive(raw);
        original.started = false;
    }
    pub(crate) fn view_listing_error(&self) -> Option<ConnectorError> {
        let original = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let CpuBody::ViewListing(work) = &original.as_ref().expect("same listing source").body
        else {
            panic!("same view listing source")
        };
        work.error()
    }
    pub(crate) fn view_listing_next(&self) -> crate::catalog_control::views::async_listing::Next {
        let mut loan = BodyLoan::take(self);
        let CpuBody::ViewListing(work) = &mut loan.body.as_mut().unwrap().body else {
            panic!("same view listing source")
        };
        work.next()
    }

    pub(crate) fn compaction_source(
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        incarnation: novarocks_spi::connector::ProviderBindingEpoch,
        work: crate::catalog_control::metadata_maintenance::observation::CompactionWork,
    ) -> Arc<Self> {
        Self::new(descriptor, incarnation, CpuBody::Compaction(work))
    }
    pub(crate) fn compaction_received(
        &self,
        raw: crate::catalog_control::metadata_maintenance::observation::RawResult,
    ) {
        let mut loan = BodyLoan::take(self);
        let original = loan.body.as_mut().unwrap();
        let CpuBody::Compaction(work) = &mut original.body else {
            panic!("same compaction source");
        };
        work.receive(raw);
        original.started = false;
    }
    pub(crate) fn compaction_received_result(
        &self,
    ) -> Option<Result<novarocks_spi::connector::ConnectorMaxCompactableDataFiles, ConnectorError>>
    {
        let mut loan = BodyLoan::take(self);
        let CpuBody::Compaction(work) = &mut loan.body.as_mut().unwrap().body else {
            panic!("same compaction source");
        };
        work.received_result()
    }
    pub(crate) fn compaction_next(
        &self,
    ) -> crate::catalog_control::metadata_maintenance::observation::Next {
        let mut loan = BodyLoan::take(self);
        let CpuBody::Compaction(work) = &mut loan.body.as_mut().unwrap().body else {
            panic!("same compaction source");
        };
        work.next()
    }
    pub(crate) fn cleanup_source(
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        incarnation: novarocks_spi::connector::ProviderBindingEpoch,
        work: crate::catalog_control::cleanup_maintenance::async_original::CpuWork,
    ) -> Arc<Self> {
        Self::new(descriptor, incarnation, CpuBody::Cleanup(Box::new(work)))
    }
    pub(in crate::catalog_control) fn take_cleanup_output(
        &self,
    ) -> Result<
        crate::catalog_control::cleanup_maintenance::async_original::CpuOutput,
        ConnectorError,
    > {
        let mut loan = BodyLoan::take(self);
        let CpuBody::Cleanup(work) = &mut loan.body.as_mut().unwrap().body else {
            return Err(invalid("cleanup CPU source differs"));
        };
        work.take_output()
    }

    fn new(
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        incarnation: novarocks_spi::connector::ProviderBindingEpoch,
        body: CpuBody,
    ) -> Arc<Self> {
        Arc::new(Self {
            descriptor,
            incarnation,
            state: Mutex::new(Some(OriginalBody {
                started: false,
                body,
            })),
        })
    }
    pub(crate) fn metadata_binding(
        provider: IcebergMetadata,
        facts: crate::metadata::IcebergPreparedTableFacts,
        expected: Option<novarocks_spi::connector::ConnectorTableObjectId>,
    ) -> Arc<Self> {
        Self::new(
            provider.descriptor().clone(),
            provider.incarnation(),
            CpuBody::MetadataBinding {
                provider,
                facts,
                expected,
                result: None,
            },
        )
    }
    pub(crate) fn take_binding_result(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorTableObjectBinding, ConnectorError> {
        self.take_binding_with_snapshot_result()
            .map(|(binding, _)| binding)
    }
    pub(crate) fn take_binding_with_snapshot_result(
        &self,
    ) -> Result<
        (
            novarocks_spi::connector::ConnectorTableObjectBinding,
            Option<i64>,
        ),
        ConnectorError,
    > {
        let mut original = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let CpuBody::MetadataBinding { result, .. } = &mut original.as_mut().unwrap().body else {
            panic!("same Statistics metadata binding result");
        };
        result.take().expect("actual metadata binding CPU result")
    }
    pub(super) fn schema(
        provider: IcebergMetadata,
        request: StatisticsCollectionStartRequest,
        table_payload: IcebergTablePayload,
        physical: crate::iceberg::table::Table,
        cpu: Weak<dyn StatisticsCpuPort>,
    ) -> Arc<Self> {
        Self::new(
            provider.descriptor().clone(),
            provider.incarnation(),
            CpuBody::Schema {
                provider,
                request,
                table_payload,
                physical,
                cpu,
                result: None,
            },
        )
    }
    pub(super) fn artifacts(
        session: Arc<StatisticsSessionBody>,
        drafts: Vec<StatisticsArtifactDraft>,
        cpu: Weak<dyn StatisticsCpuPort>,
    ) -> Arc<Self> {
        Self::new(
            session.provider.descriptor().clone(),
            session.provider.incarnation(),
            CpuBody::Artifacts {
                session,
                drafts: Some(drafts),
                cpu,
                result: None,
            },
        )
    }
    pub(crate) fn blob(
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        incarnation: novarocks_spi::connector::ProviderBindingEpoch,
        drafts: Arc<[StatisticsArtifactDraft]>,
        index: usize,
        snapshot: i64,
        sequence: i64,
    ) -> Arc<Self> {
        Self::new(
            descriptor,
            incarnation,
            CpuBody::Blob {
                drafts,
                index,
                snapshot,
                sequence,
                result: None,
            },
        )
    }
    pub(super) fn evidence(
        projection: publication::Projection,
        request: FrozenRequest,
        operation: IcebergCommitOperation,
        journal: PublicationJournal<StatisticsReceipt>,
    ) -> Arc<Self> {
        Self::new(
            projection.descriptor.clone(),
            projection.incarnation,
            CpuBody::Evidence {
                projection,
                request: Some(request),
                operation,
                journal,
                result: None,
            },
        )
    }
    fn run(&self) {
        let mut loan = BodyLoan::take(self);
        let original = loan.body.as_mut().expect("same original CPU body");
        assert!(!original.started, "one original Statistics CPU execution");
        original.started = true;
        match &mut original.body {
            CpuBody::ViewListing(work) => work.run(),
            CpuBody::Compaction(work) => work.run(),
            CpuBody::PreparedSource(body) => body.run(),
            CpuBody::PreparedSplit(body) => body.run(),
            CpuBody::Cleanup(work) => work.run(),
            CpuBody::Read(body) => body.run(),
            CpuBody::ReadSnapshot(body) => body.run(),
            CpuBody::MetadataBinding {
                provider,
                facts,
                expected,
                result,
            } => {
                *result = Some(provider.create_binding_with_snapshot_borrowed(facts, expected));
            }
            CpuBody::Schema {
                provider,
                request,
                table_payload,
                physical,
                cpu,
                result,
            } => {
                // The original schema/SDK/context owners stay here on unwind.
                *result = Some(
                    provider
                        .collection_session_borrowed(
                            request,
                            table_payload,
                            physical,
                            Some(cpu.clone()),
                        )
                        .and_then(|(table, version, ordinal, required, session)| {
                            AsyncStatisticsCollectionStart::try_new_async(
                                table,
                                version,
                                ordinal,
                                required,
                                Box::new(session),
                            )
                        }),
                );
            }
            CpuBody::Artifacts {
                session,
                drafts,
                cpu,
                result,
            } => {
                let normalized = validate_artifacts(
                    &session.expectations,
                    drafts.as_ref().expect("same original drafts").clone(),
                );
                *result = Some(normalized.and_then(|normalized| {
                    let revision = collection_revision(
                        &session.data_version,
                        session.operation_id,
                        &session.expectations,
                    )?;
                    if normalized.is_empty() {
                        Ok(async_publication::PreparedStatisticsPublication::NoOp(
                            statistics_receipt(
                                &session.provider,
                                session.operation_id,
                                session.data_version.clone(),
                                revision,
                                Bytes::new(),
                                ExternalMutationEffect::NoOp,
                            )?,
                        ))
                    } else {
                        Ok(async_publication::PreparedStatisticsPublication::Active(
                            async_publication::StatisticsPublicationSource::new(
                                session,
                                normalized,
                                revision,
                                cpu.clone(),
                            )?,
                        ))
                    }
                }));
            }
            CpuBody::Blob {
                drafts,
                index,
                snapshot,
                sequence,
                result,
            } => {
                *result = Some(crate::stats_assembler::prepare_puffin_blob(
                    &drafts[*index],
                    *snapshot,
                    *sequence,
                ));
            }
            CpuBody::Evidence {
                projection,
                request,
                operation,
                result,
                ..
            } => {
                *result = Some(projection.prepare_evidence(
                    request.as_ref().expect("same original frozen request"),
                    operation,
                ));
            }
        }
        // This guard restores every input/partial output before worker unwind
        // can reach the original physical receipt. It runs no Provider Drop.
    }
    pub(super) fn take_schema_result(
        &self,
    ) -> Result<AsyncStatisticsCollectionStart, ConnectorError> {
        let mut original = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let CpuBody::Schema { result, .. } = &mut original.as_mut().unwrap().body else {
            panic!("same Statistics schema result");
        };
        result.take().expect("actual schema CPU result")
    }
    pub(super) fn take_publication_result(&self) -> PublicationResult {
        let mut original = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let CpuBody::Artifacts { result, .. } = &mut original.as_mut().unwrap().body else {
            panic!("same Statistics artifacts result");
        };
        result.take().expect("actual artifacts CPU result")
    }
    pub(super) fn restore_artifacts_in(&self, target: &mut Option<Vec<StatisticsArtifactDraft>>) {
        assert!(target.is_none());
        let mut original = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let CpuBody::Artifacts { drafts, .. } = &mut original.as_mut().unwrap().body else {
            panic!("same original artifacts");
        };
        *target = drafts.take();
    }
    pub(crate) fn take_blob(&self) -> (Blob, BlobMetadata) {
        let mut original = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let CpuBody::Blob { result, .. } = &mut original.as_mut().unwrap().body else {
            panic!("same Statistics blob result");
        };
        result.take().expect("actual blob CPU result")
    }
    pub(super) fn evidence_request_returned(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|s| {
                matches!(
                    &s.body,
                    CpuBody::Evidence {
                        request: None,
                        result: None,
                        ..
                    }
                )
            })
    }
    pub(super) fn install_evidence_and_return_request(
        &self,
        target: &mut Option<FrozenRequest>,
    ) -> crate::iceberg::Result<()> {
        assert!(target.is_none());
        let mut loan = BodyLoan::take(self);
        let CpuBody::Evidence {
            request,
            operation: _,
            journal,
            result,
            ..
        } = &mut loan.body.as_mut().unwrap().body
        else {
            panic!("same evidence source");
        };
        let result = result.take().expect("actual evidence CPU result");
        let outcome = match result {
            Ok((evidence, receipt)) => journal.record(
                evidence,
                receipt,
                request.as_ref().expect("same frozen request"),
            ),
            Err(error) => Err(error),
        };
        // No clone/remint. A journal callback panic above retains the request
        // in this stable Source; success or ordinary error returns it once.
        *target = request.take();
        outcome
    }
}
macro_rules! original_cpu_source {
    ($trait:ident, $method:ident) => {
        impl novarocks_spi::connector::$trait for IcebergMetadataCpuOriginal {
            fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
                &self.descriptor
            }
            fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
                self.incarnation
            }
            fn $method(&self) {
                self.run();
            }
        }
    };
}
impl novarocks_spi::connector::StatisticsBindingCpuSource for IcebergMetadataCpuOriginal {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.incarnation
    }
    fn run_binding(&self) {
        {
            let body = self.state.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                matches!(
                    &body.as_ref().expect("original binding source").body,
                    CpuBody::MetadataBinding { .. }
                ),
                "foreground binding source cannot dispatch another Statistics phase"
            );
        }
        self.run();
    }
}
original_cpu_source!(StatisticsSchemaCpuSource, run_schema);
original_cpu_source!(StatisticsArtifactsCpuSource, run_artifacts);
original_cpu_source!(StatisticsEvidenceCpuSource, run_evidence);

pub(super) fn original_cpu(
    weak: &Option<Weak<dyn StatisticsCpuPort>>,
) -> Result<Arc<dyn StatisticsCpuPort>, ConnectorError> {
    weak.as_ref()
        .and_then(Weak::upgrade)
        .ok_or_else(|| invalid("Statistics requires its original scoped CPU source"))
}
async fn run(
    cpu: &Arc<dyn StatisticsCpuPort>,
    mut input: Option<StatisticsCpuInput>,
) -> Result<(), ConnectorError> {
    cpu.submit_in(&mut input)?;
    std::future::poll_fn(|cx| cpu.poll_finished(cx)).await
}
pub(crate) async fn run_provider_metadata(
    cpu: &Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    source: &Arc<IcebergMetadataCpuOriginal>,
) -> Result<(), ConnectorError> {
    let mut input = Some(novarocks_spi::connector::ProviderMetadataCpuInput(
        source.clone(),
    ));
    cpu.submit_metadata_in(&mut input)?;
    std::future::poll_fn(|cx| cpu.poll_finished(cx)).await
}
pub(crate) async fn run_binding(
    cpu: &Arc<dyn StatisticsCpuPort>,
    source: &Arc<IcebergMetadataCpuOriginal>,
) -> Result<(), ConnectorError> {
    run(cpu, Some(StatisticsCpuInput::Binding(source.clone()))).await
}
pub(crate) async fn run_schema(
    cpu: &Arc<dyn StatisticsCpuPort>,
    source: &Arc<IcebergMetadataCpuOriginal>,
) -> Result<(), ConnectorError> {
    run(cpu, Some(StatisticsCpuInput::Schema(source.clone()))).await
}
pub(crate) async fn run_artifacts(
    cpu: &Arc<dyn StatisticsCpuPort>,
    source: &Arc<IcebergMetadataCpuOriginal>,
) -> Result<(), ConnectorError> {
    run(cpu, Some(StatisticsCpuInput::Artifacts(source.clone()))).await
}
pub(super) async fn run_evidence(
    cpu: &Arc<dyn StatisticsCpuPort>,
    source: &Arc<IcebergMetadataCpuOriginal>,
) -> Result<(), ConnectorError> {
    run(cpu, Some(StatisticsCpuInput::Evidence(source.clone()))).await
}
pub(super) fn known_source_bytes() -> Result<usize, ConnectorError> {
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    use std::alloc::Layout;
    let arc = |layout| {
        arc_layout(layout)
            .map(|l| l.size())
            .map_err(|_| internal("Statistics source layout overflow"))
    };
    // The completed evidence Source stays in the Publisher until its next
    // Source is stable. Two actual phase headers coexist during replacement;
    // a poisoned Source forbids replacement. The shared Session body is one.
    let statistics = arc(Layout::new::<IcebergMetadataCpuOriginal>())?
        .checked_add(if cfg!(target_os = "macos") { 64 } else { 0 })
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| n.checked_add(arc(Layout::new::<StatisticsSessionBody>()).ok()?))
        .ok_or_else(|| internal("Statistics source layout overflow"))?;
    Ok(statistics.max(metadata_cpu_source_bytes()?))
}

impl novarocks_spi::connector::StatisticsReadCpuSource for IcebergMetadataCpuOriginal {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.incarnation
    }
    fn run_read(&self) {
        {
            let body = self.state.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                matches!(&body.as_ref().expect("original read CPU source").body,CpuBody::Read(read) if matches!(read.phase,2|3)),
                "Statistics Read input cannot dispatch another metadata/product phase"
            );
        }
        self.run();
    }
}

impl crate::catalog::listing_admission::ListingOriginalError for IcebergMetadataCpuOriginal {
    fn selected_error(&self) -> Option<ConnectorError> {
        // A loaned body has not published a result at this same mutex. Do not
        // wait for CPU/footer exit and then overwrite an already chosen stop.
        let original = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let original = original.as_ref()?;
        match &original.body {
            CpuBody::ViewListing(work) => work.error(),
            _ => None,
        }
    }
}

impl novarocks_spi::connector::ProviderMetadataCpuSource for IcebergMetadataCpuOriginal {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.incarnation
    }
    fn run_metadata(&self) {
        {
            let body = self.state.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                matches!(
                    &body.as_ref().expect("same metadata source").body,
                    CpuBody::MetadataBinding { .. }
                        | CpuBody::ViewListing(_)
                        | CpuBody::PreparedSource(_)
                        | CpuBody::PreparedSplit(_)
                        | CpuBody::ReadSnapshot(_)
                        | CpuBody::Cleanup(_)
                        | CpuBody::Compaction(_)
                ) || matches!(&body.as_ref().unwrap().body,CpuBody::Read(read) if read.phase<=1
                ),
                "metadata source cannot dispatch a Statistics product phase"
            );
        }
        self.run();
    }
}
/// Two exact original phase headers coexist: parent read and its snapshot
/// continuation. Existing Statistics phase replacement has the same peak.
pub(crate) fn metadata_cpu_source_bytes() -> Result<usize, ConnectorError> {
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    let header = arc_layout(std::alloc::Layout::new::<IcebergMetadataCpuOriginal>())
        .map_err(|_| internal("metadata source layout overflow"))?
        .size()
        .checked_add(if cfg!(target_os = "macos") { 64 } else { 0 })
        .ok_or_else(|| internal("metadata source layout overflow"))?;
    let reader = scoped_snapshot::reader_backing_bytes()?
        .max(IcebergMetadata::statistics_read_preparation_bytes()?);
    let error = scoped_read::error_backing_bytes()?;
    // One actual parent Source keeps its owned body through every phase.
    // Read and prepared-source bodies are mutually exclusive; the nested
    // snapshot Source owns neither parent packet.
    let parent_body = std::mem::size_of::<scoped_read::ReadBody>()
        .max(std::mem::size_of::<scoped_prepared_source::Work>())
        .max(std::mem::size_of::<scoped_prepared_split::Work>())
        .max(std::mem::size_of::<
            crate::catalog_control::views::async_listing::Work,
        >());
    header
        .checked_mul(2)
        .and_then(|n| n.checked_add(parent_body))
        .and_then(|n| n.checked_add(reader))
        .and_then(|n| n.checked_add(error))
        .ok_or_else(|| internal("metadata source layout overflow"))
}
