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

//! The one typed source preparation. Pure phases borrow the original CPU
//! Source; only the original async driver services request access and FileIO.
// Design: ADR-0207 (docs/adr/ADR-0207-single-prepared-change-window-and-frozen-source-endpoints.md)
use super::*;
use crate::change_planning::ChangeWindowPlan;
use crate::iceberg::spec::SchemaRef;
use crate::loaded_table::{IcebergAttemptTableAccess, IcebergPhysicalTable};
use novarocks_spi::connector::read_stack::{
    ConnectorReadSourcePreparationRequest, ConnectorReadSourceSelection,
    adapter::ProviderPreparedSourceRead,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorChangeWindowAdmission, ConnectorExactSemanticRevision,
    ConnectorOriginalResultScope, ConnectorProviderBindingKey, ConnectorRequestContext,
    ConnectorTableMetadata, ConnectorTableObjectId, ProviderMetadataCpuPort,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Original observation coverage and access provenance. All fields are
/// immutable, process-local facts; none is a live access or resource grant.
pub(crate) struct IcebergObservedSourceReadFacts {
    pub(crate) access: Arc<IcebergAttemptTableAccess>,
    pub(crate) snapshots: crate::catalog::SnapshotLoading,
    pub(crate) projection: crate::metadata::TableMetadataProjection,
}

pub(crate) struct PreparedRelations {
    pub(crate) to: crate::typed_read::IcebergTableHandle,
    pub(crate) from: Option<crate::typed_read::IcebergTableHandle>,
    pub(crate) delta: Option<crate::typed_read::IcebergChangeWindowHandle>,
}

/// Immutable FE facts only. No request FileIO, live reader, CPU receipt or
/// credential response crosses from the original Source into this value.
pub struct IcebergPreparedSourceRead {
    pub(crate) owner: ConnectorProviderBindingKey,
    pub(crate) catalog: CatalogHandle,
    pub(crate) request: ConnectorReadSourcePreparationRequest,
    pub(crate) access: Arc<IcebergAttemptTableAccess>,
    pub(crate) relations: PreparedRelations,
    pub(crate) admission: Option<ConnectorChangeWindowAdmission>,
    pub(crate) to_metadata: Arc<ConnectorTableMetadata>,
}
impl std::fmt::Debug for IcebergPreparedSourceRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergPreparedSourceRead")
            .finish_non_exhaustive()
    }
}

enum Phase {
    Prepare,
    Bind,
    Finish,
}
type AccessLoader =
    Pin<Box<dyn Future<Output = Result<IcebergPhysicalTable, ConnectorError>> + Send>>;

pub(crate) struct Work {
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    incarnation: novarocks_spi::connector::ProviderBindingEpoch,
    catalog: CatalogHandle,
    runtime: Arc<crate::metadata_context::IcebergMetadataContext>,
    request: ConnectorReadSourcePreparationRequest,
    context: ConnectorRequestContext,
    original: ConnectorOriginalResultScope,
    phase: Phase,
    access: Option<Arc<IcebergAttemptTableAccess>>,
    physical: Option<IcebergPhysicalTable>,
    schema: Option<SchemaRef>,
    pin_schema_id: Option<i32>,
    loader: Option<AccessLoader>,
    loaded: Option<Result<IcebergPhysicalTable, ConnectorError>>,
    loader_foreign: bool,
    loader_exited: bool,
    snapshot: Option<Arc<IcebergMetadataCpuOriginal>>,
    endpoints: [Option<super::scoped_snapshot::ScopedReadSnapshot>; 2],
    result: Option<Result<Arc<IcebergPreparedSourceRead>, ConnectorError>>,
}
impl Work {
    fn run_phase(&mut self) -> Result<(), ConnectorError> {
        self.original.check_active().map_err(|e| {
            super::scoped_read::checked_read(
                crate::read_snapshot::cow_capture::ReadFailure::Original(e.into()),
            )
        })?;
        self.context.check_active()?;
        match self.phase {
            Phase::Prepare => self.prepare(),
            Phase::Bind => {
                // The actual original access result wins over a later check.
                let physical = self
                    .loaded
                    .as_ref()
                    .expect("original access result")
                    .as_ref()
                    .map_err(Clone::clone)?;
                self.physical = Some(physical.clone());
                Ok(())
            }
            Phase::Finish => {
                let physical = self.physical.as_ref().expect("frozen source physical view");
                let admission = crate::change_planning::finish_change_window(
                    physical.table.metadata(),
                    &self.endpoints[0]
                        .as_ref()
                        .expect("original From observation")
                        .files,
                    &self.endpoints[1]
                        .as_ref()
                        .expect("original To observation")
                        .files,
                    &self.context,
                )?;
                self.finish(Some(admission))
            }
        }
    }
    pub(super) fn run(&mut self) {
        if let Err(error) = self.run_phase() {
            self.result = Some(Err(error));
        }
    }
    fn prepare(&mut self) -> Result<(), ConnectorError> {
        let owner = ConnectorProviderBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        };
        self.request
            .observed_source_facts()
            .validate_owner(&owner)?;
        if self.request.observed_table().owner() != &self.descriptor.instance_id {
            return Err(invalid("source pin belongs to another provider instance"));
        }
        let payload: crate::metadata::IcebergTablePayload =
            serde_json::from_slice(self.request.observed_table().payload())
                .map_err(|e| invalid(format!("decode original source observation: {e}")))?;
        if payload.metadata_table_type.is_some()
            || payload.row_mutation_frozen_source
            || payload.explicit_files.is_some()
            || !payload.prepared_files.is_empty()
            || payload.namespace != self.request.name().schema_name()
            || payload.table != self.request.name().table_name()
        {
            return Err(invalid("source pin does not name its exact base table"));
        }
        let info = payload
            .table_info
            .as_ref()
            .ok_or_else(|| invalid("source pin has no frozen table descriptor"))?;
        let serialized = info
            .serialized_metadata
            .as_ref()
            .ok_or_else(|| invalid("source pin has no complete frozen metadata"))?;
        let metadata: crate::iceberg::spec::TableMetadata = serde_json::from_str(serialized)
            .map_err(|e| invalid(format!("decode frozen source metadata: {e}")))?;
        let observed = self
            .request
            .observed_source_facts()
            .value::<IcebergObservedSourceReadFacts>(self.descriptor.instance_id.as_str())?;
        if matches!(
            self.request.selection(),
            ConnectorReadSourceSelection::ChangeWindow(_)
        ) && observed.snapshots != crate::catalog::SnapshotLoading::All
        {
            return Err(invalid(
                "change-window source observation does not cover all historical snapshots",
            ));
        }
        let access = Arc::clone(&observed.access);
        if crate::delete_semantics::canonical_metadata_json(&metadata)
            .map_err(|e| invalid(e.to_string()))?
            != crate::delete_semantics::canonical_metadata_json(access.metadata())
                .map_err(|e| invalid(e.to_string()))?
            || access.identifier().namespace().to_url_string() != payload.namespace
            || access.identifier().name() != payload.table
        {
            return Err(invalid(
                "source access frame differs from its original observation",
            ));
        }
        let object = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(
            metadata.uuid().to_string().as_bytes(),
        ))?;
        if &object != self.request.object_id()
            || info.table_uuid.as_deref() != Some(metadata.uuid().to_string().as_str())
        {
            return Err(ConnectorError::table_object_binding(
                novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced,
                "source pin UUID differs from its original table object",
            ));
        }
        if info.schema_id != metadata.current_schema_id() {
            return Err(invalid(
                "source observation schema fallback differs from its frozen metadata",
            ));
        }
        self.pin_schema_id = Some(info.schema_id);
        let to = match self.request.selection() {
            ConnectorReadSourceSelection::Snapshot(to) => {
                if self.request.from_revision().is_some() {
                    return Err(invalid("snapshot source has a From revision"));
                }
                to
            }
            ConnectorReadSourceSelection::ChangeWindow(window) => {
                check_revision(
                    &self.descriptor,
                    &object,
                    window.from_snapshot_id(),
                    self.request
                        .from_revision()
                        .ok_or_else(|| invalid("window source has no From revision"))?,
                )?;
                window.to_snapshot_id()
            }
        };
        check_revision(&self.descriptor, &object, to, self.request.to_revision())?;
        // A missing schema_id is resolved only against this ORIGINAL pin's
        // schema id, never a later Current/catalog view.
        let schema = frozen_snapshot_schema(&metadata, to, info.schema_id)?;
        let physical = access.reacquired_request_scoped(
            self.runtime
                .resources()
                .planning_binding()
                .for_request(self.context.clone()),
            self.runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        )?;
        self.access = Some(access);
        self.physical = Some(physical);
        self.schema = Some(schema);
        match self.request.selection() {
            ConnectorReadSourceSelection::Snapshot(_) => self.finish(None),
            ConnectorReadSourceSelection::ChangeWindow(window) => {
                match crate::change_planning::prepare_change_window(
                    &metadata,
                    window.from_snapshot_id(),
                    window.to_snapshot_id(),
                    &self.context,
                )? {
                    ChangeWindowPlan::Resolved(admission) => self.finish(Some(admission)),
                    ChangeWindowPlan::Endpoints { schema } => {
                        if self.schema.as_ref() != Some(&schema) {
                            return Err(corrupt(
                                "change-window lineage schema differs from its original To pin",
                            ));
                        }
                        Ok(())
                    }
                }
            }
        }
    }
    fn finish(
        &mut self,
        admission: Option<ConnectorChangeWindowAdmission>,
    ) -> Result<(), ConnectorError> {
        let physical = self.physical.as_ref().expect("same frozen source view");
        let to = match self.request.selection() {
            ConnectorReadSourceSelection::Snapshot(to) => to,
            ConnectorReadSourceSelection::ChangeWindow(window) => window.to_snapshot_id(),
        };
        let request = novarocks_spi::connector::ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: self.descriptor.instance_id.clone(),
                namespace: Arc::from(self.request.name().schema_name()),
                table: Arc::from(self.request.name().table_name()),
            },
            resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            context: self.context.clone(),
        };
        let to_metadata = crate::metadata::with_pure_schema_creation(&self.context, |creation| {
            crate::metadata::build_table_metadata(
                crate::metadata::TableMetadataBuildInput {
                    descriptor: &self.descriptor,
                    incarnation: self.incarnation,
                    request,
                    loaded: physical.clone(),
                    table_name: self.request.name().table_name().to_string(),
                    metadata_table_type: None,
                    snapshots: self
                        .request
                        .observed_source_facts()
                        .value::<IcebergObservedSourceReadFacts>(
                            self.descriptor.instance_id.as_str(),
                        )?
                        .snapshots,
                    projection: crate::metadata::TableMetadataProjection::SnapshotTo(to),
                },
                creation,
            )
            .map_err(crate::metadata::metadata_creation_error)
        })?;
        if let Some(admission) = &admission {
            admission.validate(&self.context)?;
        }
        let relations = crate::typed_boundary::frozen_prepared_source_relations(
            self.request.name(),
            physical.table.metadata(),
            self.request.selection(),
            self.schema.as_ref().expect("frozen To schema").clone(),
            self.pin_schema_id.expect("original schema fallback"),
            admission.as_ref(),
        )?;
        self.result = Some(Ok(Arc::new(IcebergPreparedSourceRead {
            owner: ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.incarnation,
            },
            catalog: self.catalog.clone(),
            request: self.request.clone(),
            access: Arc::clone(self.access.as_ref().expect("original source frame")),
            relations,
            admission,
            to_metadata: Arc::new(to_metadata),
        })));
        Ok(())
    }
}

pub(crate) fn frozen_snapshot_schema(
    metadata: &crate::iceberg::spec::TableMetadata,
    snapshot: i64,
    fallback: i32,
) -> Result<SchemaRef, ConnectorError> {
    let snapshot = metadata
        .snapshot_by_id(snapshot)
        .ok_or_else(|| invalid("frozen source snapshot is absent"))?;
    metadata
        .schema_by_id(snapshot.schema_id().unwrap_or(fallback))
        .cloned()
        .ok_or_else(|| invalid("frozen source schema is absent"))
}
fn check_revision(
    descriptor: &novarocks_spi::connector::ConnectorInstanceDescriptor,
    object: &ConnectorTableObjectId,
    snapshot: i64,
    actual: &ConnectorExactSemanticRevision,
) -> Result<(), ConnectorError> {
    let expected = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
        descriptor.provider_id.clone(),
        object,
        Some(snapshot),
    )?;
    if actual != &expected {
        return Err(invalid(
            "source snapshot and exact semantic revision disagree",
        ));
    }
    Ok(())
}
impl IcebergMetadataCpuOriginal {
    pub(crate) fn prepared_source(
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        incarnation: novarocks_spi::connector::ProviderBindingEpoch,
        catalog: CatalogHandle,
        runtime: Arc<crate::metadata_context::IcebergMetadataContext>,
        request: ConnectorReadSourcePreparationRequest,
        context: ConnectorRequestContext,
        original: ConnectorOriginalResultScope,
    ) -> Arc<Self> {
        Self::new(
            descriptor.clone(),
            incarnation,
            CpuBody::PreparedSource(Box::new(Work {
                descriptor,
                incarnation,
                catalog,
                runtime,
                request,
                context,
                original,
                phase: Phase::Prepare,
                access: None,
                physical: None,
                schema: None,
                pin_schema_id: None,
                loader: None,
                loaded: None,
                loader_foreign: false,
                loader_exited: false,
                snapshot: None,
                endpoints: [None, None],
                result: None,
            })),
        )
    }
    fn prepared_result_exists(&self) -> bool {
        let original = self.state.lock().unwrap_or_else(|p| p.into_inner());
        matches!(original.as_ref().map(|original| &original.body), Some(CpuBody::PreparedSource(body)) if body.result.is_some())
    }
    fn selected_prepared_error_exists(&self) -> bool {
        let original = self.state.lock().unwrap_or_else(|p| p.into_inner());
        matches!(original.as_ref().map(|original| &original.body), Some(CpuBody::PreparedSource(body)) if body.result.as_ref().is_some_and(Result::is_err))
    }
    fn poll_prepared_access(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        let mut loan = BodyLoan::take(self);
        let CpuBody::PreparedSource(body) = &mut loan.body.as_mut().unwrap().body else {
            return Poll::Ready(Err(invalid("foreign prepared source")));
        };
        if body.loader_foreign {
            return Poll::Ready(Err(invalid(
                "original source access poll or Drop is unproven",
            )));
        }
        if body.loader_exited {
            return Poll::Ready(Ok(()));
        }
        body.loader_foreign = true;
        match body
            .loader
            .as_mut()
            .expect("original access future")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => {
                body.loader_foreign = false;
                Poll::Pending
            }
            Poll::Ready(raw) => {
                body.loaded = Some(raw);
                drop(body.loader.take());
                body.loader_exited = true;
                body.loader_foreign = false;
                Poll::Ready(Ok(()))
            }
        }
    }
    pub(crate) async fn drive_prepared_source(
        self: &Arc<Self>,
        cpu: &Arc<dyn ProviderMetadataCpuPort>,
    ) -> Result<(), ConnectorError> {
        let finished = super::run_provider_metadata(cpu, self).await;
        if self.selected_prepared_error_exists() {
            return Ok(());
        }
        finished?;
        if self.prepared_result_exists() {
            return Ok(());
        }
        {
            let mut loan = BodyLoan::take(self);
            let CpuBody::PreparedSource(body) = &mut loan.body.as_mut().unwrap().body else {
                return Err(invalid("foreign prepared source"));
            };
            let runtime = Arc::clone(&body.runtime);
            let access = Arc::clone(body.access.as_ref().expect("prepared frozen frame"));
            let context = body.context.clone();
            let name = body.request.name().clone();
            // Reacquisition may obtain the original vended access path. The
            // returned interpretation remains frozen; this is NOT fresh proof.
            body.loader = Some(Box::pin(async move {
                runtime
                    .reacquire_table_access_for_request_async(
                        name.schema_name(),
                        name.table_name(),
                        &access,
                        &context,
                    )
                    .await
                    .map_err(|(kind, message)| ConnectorError::new(kind, message))
            }));
        }
        std::future::poll_fn(|cx| self.poll_prepared_access(cx)).await?;
        {
            let mut loan = BodyLoan::take(self);
            let original = loan.body.as_mut().unwrap();
            let CpuBody::PreparedSource(body) = &mut original.body else {
                return Err(invalid("foreign prepared source"));
            };
            body.phase = Phase::Bind;
            // Preserve an access Err before any subsequent CPU admission.
            if let Some(Err(error)) = &body.loaded {
                body.result = Some(Err(error.clone()));
            }
            original.started = false;
        }
        if self.selected_prepared_error_exists() {
            return Ok(());
        }
        let finished = super::run_provider_metadata(cpu, self).await;
        if self.selected_prepared_error_exists() {
            return Ok(());
        }
        finished?;
        for index in 0..2 {
            let snapshot = {
                let mut loan = BodyLoan::take(self);
                let CpuBody::PreparedSource(body) = &mut loan.body.as_mut().unwrap().body else {
                    return Err(invalid("foreign prepared source"));
                };
                let ConnectorReadSourceSelection::ChangeWindow(window) = body.request.selection()
                else {
                    return Err(invalid("snapshot source entered window discovery"));
                };
                let id = [window.from_snapshot_id(), window.to_snapshot_id()][index];
                let source = Self::read_snapshot_source_for_generation(
                    body.descriptor.clone(),
                    body.incarnation,
                    body.physical.as_ref().unwrap().table.clone(),
                    id,
                    body.context.clone(),
                    body.original.clone(),
                    super::scoped_snapshot::SnapshotReadKind::ChangeWindow {
                        schema: body.schema.as_ref().unwrap().clone(),
                    },
                );
                body.snapshot = Some(Arc::clone(&source));
                source
            };
            let id = {
                let loan = BodyLoan::take(self);
                let CpuBody::PreparedSource(body) = &loan.body.as_ref().unwrap().body else {
                    return Err(invalid("foreign prepared source"));
                };
                let ConnectorReadSourceSelection::ChangeWindow(window) = body.request.selection()
                else {
                    return Err(invalid("snapshot source entered window discovery"));
                };
                [window.from_snapshot_id(), window.to_snapshot_id()][index]
            };
            let finished =
                super::scoped_snapshot::drive_change_window_snapshot(&snapshot, cpu, id).await;
            if let Some(error) = snapshot.selected_read_snapshot_error() {
                let mut loan = BodyLoan::take(self);
                let CpuBody::PreparedSource(body) = &mut loan.body.as_mut().unwrap().body else {
                    return Err(invalid("foreign prepared source"));
                };
                body.result = Some(Err(error));
                return Ok(());
            }
            finished?;
            // Store the raw endpoint before retiring its Source. At most one
            // nested snapshot Source is active/retained alongside the parent.
            let mut received = None;
            let taken = snapshot.take_read_snapshot_in(&mut received);
            {
                let mut loan = BodyLoan::take(self);
                let CpuBody::PreparedSource(body) = &mut loan.body.as_mut().unwrap().body else {
                    return Err(invalid("foreign prepared source"));
                };
                match (taken, received) {
                    (Ok(()), Some(Ok(endpoint))) => body.endpoints[index] = Some(endpoint),
                    (Ok(()), Some(Err(error))) => body.result = Some(Err(error)),
                    (Err(error), _) => body.result = Some(Err(error)),
                    _ => {}
                }
            }
            if self.selected_prepared_error_exists() {
                return Ok(());
            }
            {
                let mut loan = BodyLoan::take(self);
                let CpuBody::PreparedSource(body) = &mut loan.body.as_mut().unwrap().body else {
                    return Err(invalid("foreign prepared source"));
                };
                drop(body.snapshot.take());
            }
            drop(snapshot);
        }
        {
            let mut loan = BodyLoan::take(self);
            let original = loan.body.as_mut().unwrap();
            let CpuBody::PreparedSource(body) = &mut original.body else {
                return Err(invalid("foreign prepared source"));
            };
            body.phase = Phase::Finish;
            original.started = false;
        }
        let finished = super::run_provider_metadata(cpu, self).await;
        if self.selected_prepared_error_exists() {
            return Ok(());
        }
        finished
    }
    #[cfg(test)]
    pub(crate) fn prepared_source_endpoint_facts_for_test(
        &self,
    ) -> [Vec<crate::read_model::IcebergReadFile>; 2] {
        let mut loan = BodyLoan::take(self);
        let CpuBody::PreparedSource(body) = &mut loan.body.as_mut().unwrap().body else {
            panic!("same original prepared source");
        };
        std::array::from_fn(|index| {
            body.endpoints[index]
                .as_ref()
                .expect("actual endpoint returned")
                .files
                .clone()
        })
    }
    pub(crate) fn take_prepared_source(
        &self,
    ) -> Result<ProviderPreparedSourceRead<Arc<IcebergPreparedSourceRead>>, ConnectorError> {
        let mut loan = BodyLoan::take(self);
        let CpuBody::PreparedSource(body) = &mut loan.body.as_mut().unwrap().body else {
            return Err(invalid("foreign prepared source"));
        };
        let source = body
            .result
            .take()
            .ok_or_else(|| invalid("prepared source is not ready"))??;
        Ok(match source.admission.clone() {
            Some(admission) => ProviderPreparedSourceRead::change_window(
                Arc::clone(&source),
                admission,
                Arc::clone(&source.to_metadata),
            ),
            None => ProviderPreparedSourceRead::snapshot(
                Arc::clone(&source),
                Arc::clone(&source.to_metadata),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SortOrder, Summary, TableMetadataBuilder, Type,
    };
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorOriginalResultScopeCheck, ConnectorProviderId, OriginalResultCheckError,
        ProviderBindingEpoch, ProviderMetadataCpuInput,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    struct Original(Instant);
    impl ConnectorOriginalResultScopeCheck for Original {
        fn original_deadline(&self) -> Instant {
            self.0
        }
        fn check_active(&self) -> Result<(), OriginalResultCheckError> {
            Ok(())
        }
    }
    struct Cpu {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        calls: AtomicUsize,
    }
    impl ProviderMetadataCpuPort for Cpu {
        fn submit_metadata_in(
            &self,
            input: &mut Option<ProviderMetadataCpuInput>,
        ) -> Result<(), ConnectorError> {
            let original = input.as_ref().unwrap();
            assert_eq!(original.descriptor(), &self.descriptor);
            assert_eq!(original.incarnation(), self.incarnation);
            assert_eq!(
                original.original_source_type_id(),
                std::any::TypeId::of::<IcebergMetadataCpuOriginal>()
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            input.take().unwrap().run_original_phase();
            Ok(())
        }
        fn poll_finished(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
            Poll::Ready(Ok(()))
        }
        fn poll_closed(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
            Poll::Ready(Ok(()))
        }
        fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
            None
        }
    }

    #[test]
    fn typed_source_revision_checks_the_original_provider_object_and_numeric_endpoint() {
        use novarocks_spi::connector::{
            ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        };
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("source").unwrap(),
        };
        let object = ConnectorTableObjectId::try_new(Bytes::from_static(b"object-A")).unwrap();
        let revision = |provider, object: &ConnectorTableObjectId, snapshot| {
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                provider, object, snapshot,
            )
            .unwrap()
        };
        // This author validates the exact named fact, without requiring From
        // to exist or To to be Current. Actual lineage has its own classifier.
        let absent_from = revision(descriptor.provider_id.clone(), &object, Some(999));
        check_revision(&descriptor, &object, 999, &absent_from).unwrap();
        let to = revision(descriptor.provider_id.clone(), &object, Some(42));
        check_revision(&descriptor, &object, 42, &to).unwrap();
        let foreign = ConnectorTableObjectId::try_new(Bytes::from_static(b"object-B")).unwrap();
        for mismatch in [
            revision(
                ConnectorProviderId::parse("paimon").unwrap(),
                &object,
                Some(42),
            ),
            revision(descriptor.provider_id.clone(), &foreign, Some(42)),
            revision(descriptor.provider_id.clone(), &object, Some(41)),
            revision(descriptor.provider_id.clone(), &object, None),
        ] {
            assert_eq!(
                check_revision(&descriptor, &object, 42, &mismatch)
                    .unwrap_err()
                    .kind(),
                ConnectorErrorKind::InvalidRequest
            );
        }
    }

    #[test]
    fn frozen_source_prepares_snapshot_and_closed_lineage_without_catalog_or_from_io() {
        let executor = tokio::runtime::Runtime::new().unwrap();
        let warehouse = tempfile::tempdir().unwrap();
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            256 * 1024,
            1024 * 1024,
        )
        .unwrap();
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
        };
        let incarnation = ProviderBindingEpoch::new();
        let catalog = CatalogHandle::new(
            descriptor.instance_id.clone(),
            CatalogVersion::from_bytes([1; 32]),
        );
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(executor.handle().clone())),
        );
        let runtime = Arc::new(
            crate::metadata_context::IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(
                    crate::catalog_config::parse_catalog_configuration(
                        "ice",
                        &[(
                            "iceberg.catalog.warehouse".into(),
                            warehouse.path().display().to_string(),
                        )],
                    )
                    .unwrap(),
                ),
                crate::resources::IcebergMetadataResources::new(
                    binding.clone(),
                    executor.handle().clone(),
                ),
            )
            .unwrap(),
        );
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            warehouse.path().display().to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .add_snapshot(
            Snapshot::builder()
                .with_snapshot_id(42)
                .with_sequence_number(1)
                .with_timestamp_ms(1_700_000_000_000)
                // Neither this nonexistent manifest nor absent From may be
                // read for the closed lineage and ordinary snapshot cases.
                .with_manifest_list("memory://never-enumerated/42.avro".to_string())
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let object = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(
            metadata.uuid().to_string().as_bytes(),
        ))
        .unwrap();
        let physical = IcebergPhysicalTable::new(
            crate::iceberg::table::Table::builder()
                .identifier(crate::iceberg::TableIdent::new(
                    crate::iceberg::NamespaceIdent::new("db".to_string()),
                    "t".into(),
                ))
                .metadata(Arc::new(metadata))
                .runtime(
                    runtime
                        .resources()
                        .catalog_runtime()
                        .iceberg_runtime()
                        .clone(),
                )
                .file_io(crate::fs_io::build_file_io_for_location(
                    &warehouse.path().display().to_string(),
                    binding.for_request(context.clone()),
                ))
                .build()
                .unwrap(),
        );
        let observed = crate::metadata::with_pure_schema_creation(&context, |creation| {
            crate::metadata::build_table_metadata(
                crate::metadata::TableMetadataBuildInput {
                    descriptor: &descriptor,
                    incarnation,
                    request: novarocks_spi::connector::ConnectorTableRequest {
                        table: novarocks_spi::connector::ConnectorTableIdentity {
                            instance_id: descriptor.instance_id.clone(),
                            namespace: "db".into(),
                            table: "t".into(),
                        },
                        resolution:
                            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                        context: context.clone(),
                    },
                    loaded: physical,
                    table_name: "t".into(),
                    metadata_table_type: None,
                    snapshots: crate::catalog::SnapshotLoading::All,
                    projection: crate::metadata::TableMetadataProjection::Current,
                },
                creation,
            )
            .map_err(crate::metadata::metadata_creation_error)
        })
        .unwrap();
        let revision = |snapshot| {
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                descriptor.provider_id.clone(),
                &object,
                Some(snapshot),
            )
            .unwrap()
        };
        let name =
            novarocks_spi::connector::read_stack::SchemaTableName::try_new("db", "t").unwrap();
        for from in [None, Some(42), Some(999)] {
            let request = match from {
                None => ConnectorReadSourcePreparationRequest::snapshot(
                    name.clone(),
                    observed.table.clone(),
                    observed.source_read_facts().unwrap().clone(),
                    object.clone(),
                    42,
                    revision(42),
                ),
                Some(from) => ConnectorReadSourcePreparationRequest::change_window(
                    name.clone(),
                    observed.table.clone(),
                    observed.source_read_facts().unwrap().clone(),
                    object.clone(),
                    novarocks_spi::connector::read_stack::ConnectorReadChangeWindow::new(from, 42),
                    revision(from),
                    revision(42),
                ),
            };
            let concrete = Arc::new(Cpu {
                descriptor: descriptor.clone(),
                incarnation,
                calls: AtomicUsize::new(0),
            });
            let cpu: Arc<dyn ProviderMetadataCpuPort> = concrete.clone();
            let source = IcebergMetadataCpuOriginal::prepared_source(
                descriptor.clone(),
                incarnation,
                catalog.clone(),
                runtime.clone(),
                request,
                context.clone(),
                ConnectorOriginalResultScope::from_original(Arc::new(Original(context.deadline()))),
            );
            executor
                .block_on(source.drive_prepared_source(&cpu))
                .unwrap();
            let loan = BodyLoan::take(&source);
            let CpuBody::PreparedSource(body) = &loan.body.as_ref().unwrap().body else {
                panic!("original source");
            };
            let prepared = body.result.as_ref().unwrap().as_ref().unwrap();
            assert_eq!(prepared.relations.to.snapshot_id(), Some(42));
            assert!(prepared.relations.from.is_none());
            match from {
                Some(42) => {
                    let delta = prepared.relations.delta.as_ref().unwrap();
                    assert_eq!(delta.from_snapshot_id_exclusive(), 42);
                    assert_eq!(delta.to_snapshot_id_inclusive(), 42);
                }
                _ => assert!(prepared.relations.delta.is_none()),
            }
            assert_eq!(prepared.to_metadata.schema().field(0).name(), "id");
            assert_eq!(prepared.admission, match from {
                None => None,
                Some(42) => Some(ConnectorChangeWindowAdmission::MetadataOnly),
                Some(from) => Some(ConnectorChangeWindowAdmission::FullRebuild(novarocks_spi::connector::ConnectorChangeWindowFullRebuildReason::LineageBroken { from_snapshot_id: from })),
            });
            assert_eq!(
                concrete.calls.load(Ordering::SeqCst),
                1,
                "one pure preparation; no access loader or endpoint CPU"
            );
        }
        let original_facts = observed
            .source_read_facts()
            .unwrap()
            .value::<IcebergObservedSourceReadFacts>(descriptor.instance_id.as_str())
            .unwrap();
        let refs = novarocks_spi::connector::ConnectorProcessLocalPayload::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            Arc::new(IcebergObservedSourceReadFacts {
                access: Arc::clone(&original_facts.access),
                snapshots: crate::catalog::SnapshotLoading::Refs,
                projection: original_facts.projection,
            }),
        );
        let request = ConnectorReadSourcePreparationRequest::change_window(
            name,
            observed.table.clone(),
            refs,
            object.clone(),
            novarocks_spi::connector::read_stack::ConnectorReadChangeWindow::new(999, 42),
            revision(999),
            revision(42),
        );
        let cpu: Arc<dyn ProviderMetadataCpuPort> = Arc::new(Cpu {
            descriptor: descriptor.clone(),
            incarnation,
            calls: AtomicUsize::new(0),
        });
        let source = IcebergMetadataCpuOriginal::prepared_source(
            descriptor,
            incarnation,
            catalog,
            runtime,
            request,
            context.clone(),
            ConnectorOriginalResultScope::from_original(Arc::new(Original(context.deadline()))),
        );
        executor
            .block_on(source.drive_prepared_source(&cpu))
            .unwrap();
        let loan = BodyLoan::take(&source);
        let CpuBody::PreparedSource(body) = &loan.body.as_ref().unwrap().body else {
            panic!("original source");
        };
        let error = body.result.as_ref().unwrap().as_ref().unwrap_err();
        assert_eq!(
            error.kind(),
            ConnectorErrorKind::InvalidRequest,
            "Refs absence cannot become an expired From/full rebuild fact"
        );
    }
}
