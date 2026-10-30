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

//! Original Rewrite observation and formation in its closed CPU source.
use super::*;
use crate::catalog_control::statistics::scoped_cpu::{
    IcebergMetadataCpuOriginal,
    scoped_snapshot::{ScopedReadSnapshot, SnapshotReadKind, drive_read_snapshot},
};
use crate::read_snapshot::cow_capture::{CaptureScope, ReadFailure};
use novarocks_spi::connector::ProviderMetadataCpuPort;
use novarocks_spi::connector::{
    ConnectorMetadata, ConnectorOriginalResultScope, ConnectorRewritePlanSource,
    ConnectorRewriteSelection, ConnectorRewriteSourceRequest, ConnectorTableRequest,
    ConnectorTableResolution, RewriteCpuInput, RewriteCpuPort, RewritePlanCpuSource,
};
use std::sync::Mutex;

#[derive(Clone, Copy)]
enum CpuPhase {
    Metadata,
    Plan,
}
struct PlanBody {
    control: IcebergDistributedRewriteControl,
    source_request: ConnectorRewriteSourceRequest,
    request: Option<ConnectorDistributedRewritePlanningRequest>,
    facts: Option<crate::metadata::IcebergPreparedTableFacts>,
    metadata: Option<novarocks_spi::connector::ConnectorTableMetadata>,
    table: Option<crate::iceberg::table::Table>,
    capture: Option<ConnectorOriginalResultScope>,
    files: Option<crate::manifest::CheckedCowDataFiles>,
    snapshot_result: Option<ScopedReadSnapshot>,
    deletes: Vec<Arc<crate::delete_semantics::DeleteFact>>,
    phase: CpuPhase,
    started: bool,
    metadata_result: Option<Result<(), ConnectorError>>,
    result: Option<Result<Arc<ConnectorDistributedRewritePlan>, ConnectorError>>,
}
pub(super) struct RewritePlanOriginal {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    body: Mutex<Option<PlanBody>>,
}
struct BodyLoan<'a> {
    source: &'a RewritePlanOriginal,
    body: Option<PlanBody>,
}
impl<'a> BodyLoan<'a> {
    fn take(source: &'a RewritePlanOriginal) -> Self {
        let body = source.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        assert!(body.is_some(), "one original rewrite plan loan");
        Self { source, body }
    }
}
impl Drop for BodyLoan<'_> {
    fn drop(&mut self) {
        let mut slot = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "same original rewrite plan slot");
        *slot = self.body.take();
    }
}
pub(crate) fn formation_failure(
    cause: novarocks_spi::connector::ConnectorCowBeginCause,
) -> ConnectorError {
    use novarocks_spi::connector::{ConnectorCowBeginCause, OriginalResultCheckClass};
    let kind = match &cause {
        ConnectorCowBeginCause::Provider(error) => error.kind(),
        ConnectorCowBeginCause::OriginalResult(error) => match error.class() {
            OriginalResultCheckClass::Coverage => ConnectorErrorKind::ResourceExhausted,
            OriginalResultCheckClass::Control => ConnectorErrorKind::Interrupted,
            _ => ConnectorErrorKind::Internal,
        },
    };
    ConnectorError::from_domain(kind, cause)
}
pub(crate) fn capture_failure(error: ReadFailure) -> ConnectorError {
    formation_failure(error.into_cause())
}
/// Reuse the existing controlled schema creator on the selected CPU phase.
/// Control/retention is not a prospective graph allowance or an IO grant.
fn with_original_creation<T>(
    body: &PlanBody,
    operation: impl FnOnce(
        &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        T,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    >,
) -> Result<T, ConnectorError> {
    let capture = body.capture.as_ref().expect("original Rewrite capture");
    capture
        .check_active()
        .map_err(|cause| formation_failure(cause.into()))?;
    let value =
        crate::metadata::with_pure_schema_creation(&body.source_request.context, |creation| {
            operation(creation).map_err(crate::metadata::metadata_creation_error)
        })?;
    capture
        .check_active()
        .map_err(|cause| formation_failure(cause.into()))?;
    Ok(value)
}
impl RewritePlanCpuSource for RewritePlanOriginal {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
    fn run_plan(&self) {
        let mut loan = BodyLoan::take(self);
        let body = loan.body.as_mut().unwrap();
        assert!(!body.started, "one original Rewrite CPU invocation");
        body.started = true;
        match body.phase {
            CpuPhase::Metadata => {
                body.metadata_result = Some((|| {
                    let metadata = with_original_creation(body, |creation| {
                        body.control.provider.create_rewrite_table_borrowed(
                            body.facts.as_ref().expect("original metadata facts"),
                            creation,
                        )
                    })?;
                    if metadata.identity != body.source_request.table
                        || metadata.table.owner() != &body.control.key.instance_id
                    {
                        return Err(invalid(
                            "Rewrite metadata differs from its selected original target",
                        ));
                    }
                    let operation = match body.source_request.selection {
                        ConnectorRewriteSelection::DataFiles { rewrite_all } => {
                            ConnectorDistributedRewriteOperation::RewriteDataFiles {
                                table: metadata.table.clone(),
                                rewrite_all,
                            }
                        }
                        ConnectorRewriteSelection::PositionDeletes {
                            rewrite_all,
                            min_input_files,
                        } => ConnectorDistributedRewriteOperation::RewritePositionDeletes {
                            table: metadata.table.clone(),
                            rewrite_all,
                            min_input_files,
                        },
                    };
                    body.metadata = Some(metadata);
                    body.request = Some(ConnectorDistributedRewritePlanningRequest::try_new(
                        body.source_request.operation_id,
                        body.control.key.clone(),
                        operation,
                        body.source_request.context.clone(),
                    )?);
                    Ok(())
                })());
            }
            CpuPhase::Plan => {
                body.result = Some((|| {
                    let capture = body.capture.as_ref().unwrap();
                    let scope = CaptureScope::new(capture).map_err(capture_failure)?;
                    let checked = if let Some(snapshot) = body.snapshot_result.take() {
                        let table = body.table.as_ref().unwrap();
                        let snapshot_id = table
                            .metadata()
                            .current_snapshot_id()
                            .ok_or_else(|| internal("Rewrite original snapshot disappeared"))?;
                        let checked =
                            crate::manifest::finish_checked_cow_data_files_from_original_files(
                                table.metadata(),
                                snapshot_id,
                                snapshot.files,
                                capture,
                                &scope,
                            )
                            .map_err(capture_failure)?;
                        body.deletes = snapshot.all_deletes;
                        checked
                    } else {
                        crate::manifest::CheckedCowDataFiles {
                            files: Vec::new(),
                            _retention_guard: capture.retention_guard(),
                        }
                    };
                    body.files = Some(checked);
                    let checked = body
                        .files
                        .as_ref()
                        .expect("original checked Rewrite capture");
                    let mut live = BTreeSet::new();
                    for fact in &body.deletes {
                        scope.active().map_err(capture_failure)?;
                        use crate::delete_semantics::{DeleteContentAddress, DeleteKind};
                        let identity = match (fact.address(), fact.kind()) {
                            (
                                DeleteContentAddress::PuffinBlob {
                                    path,
                                    offset,
                                    length,
                                },
                                DeleteKind::DeletionVector { exact_target },
                            ) => EntryIdentity::DeletionVector {
                                path: path.to_string(),
                                offset: i64::try_from(*offset)
                                    .map_err(|_| corrupt("Rewrite DV offset exceeds i64"))?,
                                length: i64::try_from(*length)
                                    .map_err(|_| corrupt("Rewrite DV length exceeds i64"))?,
                                referenced_data_file: exact_target.to_string(),
                            },
                            (DeleteContentAddress::File(path), _) => EntryIdentity::DeleteFile {
                                path: path.to_string(),
                            },
                            _ => {
                                return Err(corrupt(
                                    "Rewrite delete address differs from its exact kind",
                                ));
                            }
                        };
                        identity.validate().map_err(|e| corrupt(e.to_string()))?;
                        if !live.insert(identity) {
                            return Err(corrupt("Rewrite snapshot repeats a logical delete entry"));
                        }
                    }
                    let files = checked
                        .files
                        .iter()
                        .map(|file| file.stats.clone())
                        .collect::<Vec<_>>();
                    let plan = with_original_creation(body, |creation| {
                        body.control.build_loaded_plan(
                            body.request.as_ref().unwrap(),
                            body.table.as_ref().unwrap(),
                            files,
                            &live,
                            creation,
                        )
                    })?;
                    // Full validation belongs to this SAME actual CPU phase.
                    plan.validate()?;
                    scope.active().map_err(capture_failure)?;
                    Ok(Arc::new(plan))
                })());
            }
        }
    }
}

struct RewritePlanSource {
    request: ConnectorRewriteSourceRequest,
    original: Arc<RewritePlanOriginal>,
    prepared: bool,
    failure: Option<ConnectorError>,
    snapshot_source: Option<Arc<IcebergMetadataCpuOriginal>>,
}
pub(super) fn prepare_source_in(
    control: &IcebergDistributedRewriteControl,
    request: &mut Option<ConnectorRewriteSourceRequest>,
    receiving: &mut Option<Box<dyn ConnectorRewritePlanSource>>,
) -> Result<(), ConnectorError> {
    if receiving.is_some() {
        return Err(invalid("rewrite source receiver is occupied"));
    }
    let original = request
        .as_ref()
        .ok_or_else(|| invalid("rewrite source request is empty"))?;
    original.validate()?;
    if original.table.instance_id != control.key.instance_id {
        return Err(invalid("rewrite source request has a foreign generation"));
    }
    let operation = match original.selection {
        ConnectorRewriteSelection::DataFiles { .. } => CatalogOperation::RewriteDataFiles,
        ConnectorRewriteSelection::PositionDeletes { .. } => {
            CatalogOperation::RewritePositionDeletes
        }
    };
    control
        .runtime
        .novarocks_catalog()
        .admit(&CatalogAdmissionRequest::new(
            operation,
            CatalogTableName::new(
                original.table.namespace.to_string(),
                original.table.table.to_string(),
            ),
            original.context.initiation(),
        ))
        .map_err(connector_unsupported)?;
    let request = request.take().expect("validated original Rewrite request");
    *receiving = Some(Box::new(RewritePlanSource {
        request: request.clone(),
        prepared: false,
        failure: None,
        snapshot_source: None,
        original: Arc::new(RewritePlanOriginal {
            descriptor: control.descriptor.clone(),
            incarnation: control.key.incarnation,
            body: Mutex::new(Some(PlanBody {
                control: control.clone(),
                source_request: request,
                request: None,
                facts: None,
                metadata: None,
                table: None,
                capture: None,
                files: None,
                snapshot_result: None,
                deletes: Vec::new(),
                phase: CpuPhase::Metadata,
                started: false,
                metadata_result: None,
                result: None,
            })),
        }),
    }));
    Ok(())
}
impl RewritePlanSource {
    async fn close_original(&mut self, cpu: &dyn RewriteCpuPort) -> Result<(), ConnectorError> {
        std::future::poll_fn(|cx| cpu.poll_closed(cx)).await?;
        drop(self.snapshot_source.take());
        Ok(())
    }
    async fn prepare_original(
        &mut self,
        cpu: &dyn RewriteCpuPort,
        metadata_cpu: &Arc<dyn ProviderMetadataCpuPort>,
        capture: &ConnectorOriginalResultScope,
    ) -> Result<(), ConnectorError> {
        let result = self.prepare_steps(cpu, metadata_cpu, capture).await;
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
    async fn prepare_steps(
        &mut self,
        cpu: &dyn RewriteCpuPort,
        metadata_cpu: &Arc<dyn ProviderMetadataCpuPort>,
        capture: &ConnectorOriginalResultScope,
    ) -> Result<(), ConnectorError> {
        if self.prepared {
            return Ok(());
        }
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let (control, request) = {
            let mut loan = BodyLoan::take(&self.original);
            let body = loan.body.as_mut().unwrap();
            if body.capture.is_some() {
                return Err(invalid("Rewrite preparation was already started"));
            }
            if capture.original_deadline() != body.source_request.context.deadline() {
                return Err(invalid(
                    "Rewrite preparation changed its original absolute deadline",
                ));
            }
            capture
                .check_active()
                .map_err(|cause| formation_failure(cause.into()))?;
            body.capture = Some(capture.clone());
            (body.control.clone(), body.source_request.clone())
        };
        // The actual selected MIO source remains required before real catalog IO;
        // its identity alone does not grant bytes or create another request.
        request
            .context
            .io_admission()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| invalid("Rewrite metadata source has no live original MIO"))?;
        let preparation = control
            .provider
            .prepare_table(ConnectorTableRequest {
                table: request.table.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: request.context.clone(),
            })
            .await?;
        let facts = preparation
            .into_facts::<crate::metadata::IcebergPreparedTableFacts>(&control.key.instance_id)?;
        {
            let mut loan = BodyLoan::take(&self.original);
            let body = loan.body.as_mut().unwrap();
            body.table = Some(facts.rewrite_physical_table().clone());
            body.facts = Some(facts);
        }
        let mut input = Some(RewriteCpuInput(self.original.clone()));
        cpu.submit_in(&mut input)?;
        std::future::poll_fn(|cx| cpu.poll_finished(cx)).await?;
        let table = {
            let loan = BodyLoan::take(&self.original);
            let body = loan.body.as_ref().unwrap();
            body.metadata_result
                .as_ref()
                .ok_or_else(|| internal("Rewrite metadata CPU has no result"))?
                .clone()?;
            body.table.as_ref().unwrap().clone()
        };
        if let Some(snapshot) = table.metadata().current_snapshot_id() {
            self.snapshot_source = Some(IcebergMetadataCpuOriginal::read_snapshot_source(
                control.provider.as_ref().clone(),
                table.clone(),
                snapshot,
                request.context.clone(),
                capture.clone(),
                SnapshotReadKind::Rewrite,
            ));
            let source = self
                .snapshot_source
                .as_ref()
                .expect("installed original Rewrite snapshot source");
            drive_read_snapshot(source, metadata_cpu).await?;
            let mut result = None;
            source.take_read_snapshot_in(&mut result)?;
            let snapshot = result.expect("same Rewrite snapshot receiver")?;
            {
                let mut loan = BodyLoan::take(&self.original);
                loan.body.as_mut().unwrap().snapshot_result = Some(snapshot);
            }
            // The same CPU/forwarder and IO driver exited. Receive the raw
            // original output before dropping its parser Source header.
            drop(self.snapshot_source.take());
        }
        {
            let mut loan = BodyLoan::take(&self.original);
            let body = loan.body.as_mut().unwrap();
            body.phase = CpuPhase::Plan;
            body.started = false;
        }
        let mut input = Some(RewriteCpuInput(self.original.clone()));
        cpu.submit_in(&mut input)?;
        std::future::poll_fn(|cx| cpu.poll_finished(cx)).await?;
        self.prepared = true;
        Ok(())
    }
}

impl ConnectorRewritePlanSource for RewritePlanSource {
    fn request(&self) -> &ConnectorRewriteSourceRequest {
        &self.request
    }
    fn prepare<'a>(
        &'a mut self,
        cpu: &'a dyn RewriteCpuPort,
        metadata_cpu: &'a Arc<dyn ProviderMetadataCpuPort>,
        capture: &'a ConnectorOriginalResultScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ConnectorError>> + Send + 'a>>
    {
        Box::pin(self.prepare_original(cpu, metadata_cpu, capture))
    }
    fn take_preparation_in(
        &mut self,
        receiving: &mut Option<novarocks_spi::connector::ConnectorRewritePrepared>,
    ) -> Result<(), ConnectorError> {
        if receiving.is_some() || !self.prepared {
            return Err(invalid("Rewrite plan is not transferable"));
        }
        let mut loan = BodyLoan::take(&self.original);
        let body = loan.body.as_mut().unwrap();
        let plan = body
            .result
            .as_ref()
            .ok_or_else(|| invalid("Rewrite plan result was already received"))?
            .as_ref()
            .map_err(Clone::clone)?;
        let request = body.request.as_ref().unwrap();
        if plan.owner() != request.owner()
            || plan.operation_id() != request.operation_id()
            || plan.request_digest() != request.request_digest()
        {
            return Err(invalid("Rewrite plan differs from its original request"));
        }
        let metadata = body
            .metadata
            .as_ref()
            .ok_or_else(|| invalid("Rewrite original metadata was already received"))?;
        if metadata.identity != self.request.table
            || metadata.table.owner() != &body.control.key.instance_id
        {
            return Err(invalid("Rewrite table differs from its original target"));
        }
        body.capture
            .as_ref()
            .unwrap()
            .check_active()
            .map_err(|e| formation_failure(e.into()))?;
        *receiving = Some(novarocks_spi::connector::ConnectorRewritePrepared {
            plan: body
                .result
                .take()
                .unwrap()
                .expect("validated original plan"),
            table: body.metadata.take().unwrap(),
            capture: body.capture.as_ref().unwrap().clone(),
        });
        Ok(())
    }
    fn close<'a>(
        &'a mut self,
        cpu: &'a dyn RewriteCpuPort,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ConnectorError>> + Send + 'a>>
    {
        Box::pin(self.close_original(cpu))
    }
}
