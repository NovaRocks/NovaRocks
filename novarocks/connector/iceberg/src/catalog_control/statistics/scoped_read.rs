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

//! The original SQL statistics read, outside its borrowed I/O and CPU waiters.
use super::*;
use crate::read_snapshot::cow_capture::{CaptureScope, ReadFailure};
use novarocks_spi::connector::{
    ConnectorMetadata, ConnectorOriginalResultScope, ConnectorTableMetadata, ConnectorTableRequest,
    OriginalResultCheckClass, StatisticsMetricRequest, StatisticsReadOriginal,
};

pub(super) struct ReadBody {
    provider: Option<IcebergMetadata>,
    request: Option<ConnectorTableRequest>,
    facts: Option<crate::metadata::IcebergPreparedTableFacts>,
    metadata: Option<ConnectorTableMetadata>,
    read_request: Option<StatisticsReadRequest>,
    files: Option<Vec<crate::read_model::IcebergReadFile>>,
    result: Option<Result<StatisticsEvidence, ConnectorError>>,
    pub(super) phase: u8,
    snapshot: Option<Arc<IcebergMetadataCpuOriginal>>,
    payload: Option<IcebergTablePayload>,
    snapshot_id: Option<i64>,
    retiring: bool,
    closed: bool,
    // Last: every first-party/SDK input is disposed before this paid owner.
    original: ConnectorOriginalResultScope,
}
const READ_QUOTA_DETAIL: &str = "statistics read original payload check failed";
#[derive(Debug)]
struct ReadQuotaFailure {
    original: novarocks_spi::connector::OriginalResultCheckError,
}
impl std::fmt::Display for ReadQuotaFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(READ_QUOTA_DETAIL)
    }
}
impl std::error::Error for ReadQuotaFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}
impl novarocks_failure_contract::FailureProjection for ReadQuotaFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.original.failure_fact()
    }
}
pub(super) fn checked_read(error: ReadFailure) -> ConnectorError {
    use novarocks_spi::connector::ConnectorCowBeginCause;
    match error.into_cause() {
        ConnectorCowBeginCause::Provider(error) => error,
        ConnectorCowBeginCause::OriginalResult(error) => {
            let kind = match error.class() {
                OriginalResultCheckClass::Control => ConnectorErrorKind::Interrupted,
                OriginalResultCheckClass::Admission | OriginalResultCheckClass::Coverage => {
                    ConnectorErrorKind::ResourceExhausted
                }
                OriginalResultCheckClass::Identity => ConnectorErrorKind::InvalidRequest,
            };
            ConnectorError::from_domain_static(
                kind,
                ReadQuotaFailure { original: error },
                READ_QUOTA_DETAIL,
            )
        }
    }
}
impl ReadBody {
    pub(super) fn run(&mut self) {
        // CPU work retains the same original operation and stable receiver.
        let original_scope = self.original.clone();
        let capture = match CaptureScope::new(&original_scope) {
            Ok(value) => value,
            Err(error) => {
                self.result = Some(Err(checked_read(error)));
                return;
            }
        };
        let result = match self.phase {
            1 => self.prepare_request(&capture),
            3 => self.project(&capture),
            _ => Err(invalid("statistics read CPU phase was dispatched twice")),
        };
        if let Err(error) = result {
            tracing::debug!(phase = self.phase, "statistics read CPU phase refused");
            self.result = Some(Err(error));
        }
    }
    fn prepare_request(&mut self, scope: &CaptureScope<'_>) -> Result<(), ConnectorError> {
        scope.active().map_err(checked_read)?;
        let facts = self.facts.as_ref().expect("original prepared read facts");
        self.metadata = Some(
            self.provider
                .as_ref()
                .unwrap()
                .statistics_read_metadata(facts)?,
        );
        let metadata = self.metadata.as_ref().expect("original metadata receiver");
        self.payload = Some(
            self.provider
                .as_ref()
                .unwrap()
                .table_payload(&metadata.table)?,
        );
        self.snapshot_id =
            base_table_info(self.payload.as_ref().unwrap(), "statistics read")?.current_snapshot_id;
        let data_version = metadata
            .statistics_data_version
            .clone()
            .ok_or_else(|| invalid("statistics read metadata has no pinned data version"))?;
        let schema = metadata.schema();
        let columns = || {
            schema.fields().iter().filter(|field| {
                field
                    .metadata()
                    .get(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                    .is_none_or(|value| !value.eq_ignore_ascii_case("true"))
            })
        };
        let count = columns()
            .count()
            .checked_mul(5)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| invalid("statistics read metric count overflow"))?;
        if count.saturating_sub(1) > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_METRICS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "SHOW TABLE STATS exceeds the supported column metric count",
            ));
        }
        let mut metrics = Vec::with_capacity(count);
        metrics.push(StatisticsMetric::RowCount);
        for column in columns() {
            let column: Arc<str> = Arc::from(column.name().as_str());
            metrics.extend([
                StatisticsMetric::NullCount {
                    column: column.clone(),
                },
                StatisticsMetric::Minimum {
                    column: column.clone(),
                },
                StatisticsMetric::Maximum {
                    column: column.clone(),
                },
                StatisticsMetric::AverageSize {
                    column: column.clone(),
                },
                StatisticsMetric::ThetaNdv { column },
            ]);
        }
        self.read_request = Some(StatisticsReadRequest {
            narrowed_read: None,
            table: metadata.table.clone(),
            data_version,
            metrics: StatisticsMetricRequest::try_new(metrics)?,
            context: self.request.as_ref().unwrap().context.clone(),
        });
        Ok(())
    }
    fn project(&mut self, scope: &CaptureScope<'_>) -> Result<(), ConnectorError> {
        scope.active().map_err(checked_read)?;
        let request = self.read_request.as_ref().expect("original read request");
        let facts = self.facts.as_ref().expect("original physical table");
        let table = IcebergMetadata::statistics_read_physical(facts);
        let info = base_table_info(self.payload.as_ref().unwrap(), "statistics read")?;
        let snapshot = info.current_snapshot_id.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg table has no current snapshot for statistics",
            )
        })?;
        let files = self
            .files
            .as_ref()
            .expect("original manifest read receiver");
        self.result = Some(project_statistics_evidence(
            table.metadata(),
            info,
            snapshot,
            &request.data_version,
            request,
            files,
        ));
        Ok(())
    }
}
impl IcebergMetadataCpuOriginal {
    pub(crate) fn read_source(
        provider: IcebergMetadata,
        request: ConnectorTableRequest,
        original: ConnectorOriginalResultScope,
    ) -> Arc<Self> {
        Self::new(
            provider.descriptor().clone(),
            provider.incarnation(),
            CpuBody::Read(Box::new(ReadBody {
                provider: Some(provider),
                request: Some(request),
                original,
                facts: None,
                metadata: None,
                read_request: None,
                files: None,
                result: None,
                phase: 1,
                snapshot: None,
                payload: None,
                snapshot_id: None,
                retiring: false,
                closed: false,
            })),
        )
    }
    async fn read_original(
        self: Arc<Self>,
        cpu: Arc<dyn StatisticsCpuPort>,
        metadata: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> Result<(), ConnectorError> {
        let result = self.drive_read(&cpu, &metadata).await;
        // Ordinary result is received into the original body before this F
        // can exit or be dropped. The CPU port independently preserves health.
        if let Err(error) = result {
            let mut loan = BodyLoan::take(&self);
            let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
                unreachable!()
            };
            if body.result.is_none() {
                body.result = Some(Err(error));
            }
        }
        Ok(())
    }
    async fn drive_read(
        self: &Arc<Self>,
        cpu: &Arc<dyn StatisticsCpuPort>,
        metadata: &Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> Result<(), ConnectorError> {
        {
            let mut loan = BodyLoan::take(self);
            let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
                unreachable!()
            };
            let capture = CaptureScope::new(&body.original).map_err(checked_read)?;
            capture.active().map_err(checked_read)?;
            let preparation = body
                .provider
                .as_ref()
                .unwrap()
                .prepare_table(body.request.as_ref().unwrap().clone())
                .await?;
            body.facts = Some(
                preparation.into_facts::<crate::metadata::IcebergPreparedTableFacts>(
                    &body.provider.as_ref().unwrap().descriptor().instance_id,
                )?,
            );
        }
        let mut input = Some(novarocks_spi::connector::ProviderMetadataCpuInput(
            self.clone(),
        ));
        metadata.submit_metadata_in(&mut input)?;
        std::future::poll_fn(|cx| metadata.poll_finished(cx)).await?;
        {
            let mut loan = BodyLoan::take(self);
            let original = loan.body.as_mut().unwrap();
            let CpuBody::Read(body) = &mut original.body else {
                unreachable!()
            };
            if body.result.is_some() {
                return Ok(());
            }
            let snapshot = body.snapshot_id.ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "Iceberg table has no current snapshot for statistics",
                )
            })?;
            let table = IcebergMetadata::statistics_read_physical(body.facts.as_ref().unwrap());
            body.snapshot = Some(IcebergMetadataCpuOriginal::read_snapshot_source(
                body.provider.as_ref().unwrap().clone(),
                table.clone(),
                snapshot,
                body.request.as_ref().unwrap().context.clone(),
                body.original.clone(),
                super::scoped_snapshot::SnapshotReadKind::Statistics,
            ));
        }
        let snapshot = {
            let mut loan = BodyLoan::take(self);
            let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
                unreachable!()
            };
            body.snapshot.as_ref().unwrap().clone()
        };
        super::scoped_snapshot::drive_read_snapshot(&snapshot, metadata).await?;
        {
            let mut loan = BodyLoan::take(self);
            let original = loan.body.as_mut().unwrap();
            let CpuBody::Read(body) = &mut original.body else {
                unreachable!()
            };
            let mut received = None;
            snapshot.take_read_snapshot_in(&mut received)?;
            let received = received.expect("original snapshot receiver")?;
            body.files = Some(received.files);
            body.phase = 3;
            original.started = false;
        }
        run_read(cpu, self).await
    }
    pub(crate) fn read_future_bytes() -> usize {
        fn bytes<A, B, C, F>(_: fn(A, B, C) -> F) -> usize {
            std::mem::size_of::<F>()
        }
        bytes(Self::read_original)
    }
}
async fn run_read(
    cpu: &Arc<dyn StatisticsCpuPort>,
    original: &Arc<IcebergMetadataCpuOriginal>,
) -> Result<(), ConnectorError> {
    let mut input = Some(StatisticsCpuInput::Read(original.clone()));
    cpu.submit_in(&mut input)?;
    futures::future::poll_fn(|cx| cpu.poll_finished(cx)).await
}
impl StatisticsReadOriginal for IcebergMetadataCpuOriginal {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.incarnation
    }
    fn read(
        self: Arc<Self>,
        cpu: Arc<dyn StatisticsCpuPort>,
        metadata: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> StatisticsSessionFuture<'static, ()> {
        Box::pin(Self::read_original(self, cpu, metadata))
    }
    fn close_original(&self) -> Result<(), ConnectorError> {
        let mut loan = BodyLoan::take(self);
        let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
            return Err(invalid("foreign read close source"));
        };
        if body.closed {
            return Ok(());
        }
        if body.retiring {
            return Err(invalid("statistics read source disposal is unproven"));
        }
        body.retiring = true;
        let inputs = (
            body.snapshot.take(),
            body.files.take(),
            body.read_request.take(),
            body.metadata.take(),
            body.payload.take(),
            body.facts.take(),
            body.request.take(),
            body.provider.take(),
        );
        drop(inputs); // foreign Drop is outside the mutex, original paid scope stays in body
        body.closed = true;
        Ok(())
    }
    fn take_result_in(
        &self,
        target: &mut Option<Result<StatisticsEvidence, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        if target.is_some() {
            return Err(invalid("statistics read receiver is occupied"));
        }
        let mut loan = BodyLoan::take(self);
        let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
            return Err(invalid("foreign statistics read body"));
        };
        if body.result.is_none() {
            return Err(invalid("statistics read result is not ready"));
        }
        *target = body.result.take();
        Ok(())
    }
}

pub(super) fn error_backing_bytes() -> Result<usize, ConnectorError> {
    ConnectorError::static_domain_backing_bytes::<ReadQuotaFailure>(READ_QUOTA_DETAIL)
        .map_err(|_| internal("statistics error carrier layout overflow"))
}

#[cfg(test)]
mod failure_geometry_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
    use novarocks_spi::connector::OriginalResultCheckError;
    use std::{
        error::Error,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct OriginalRefusal(Arc<AtomicUsize>);
    impl std::fmt::Display for OriginalRefusal {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("original scope refusal must not be formatted")
        }
    }
    impl Error for OriginalRefusal {}
    impl novarocks_failure_contract::FailureProjection for OriginalRefusal {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_failure_contract::FailureProjection::failure_fact(
                &ConnectorErrorKind::ResourceExhausted,
            )
        }
    }

    impl Drop for OriginalRefusal {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn original_quota_refusal_moves_once_and_retains_its_typed_source() {
        let drops = Arc::new(AtomicUsize::new(0));
        let input = OriginalResultCheckError::new(
            OriginalResultCheckClass::Control,
            OriginalRefusal(drops.clone()),
        );
        let error = checked_read(ReadFailure::Original(input.into()));
        assert_eq!(error.kind(), ConnectorErrorKind::Interrupted);
        assert_eq!(
            error.failure_fact(),
            ConnectorErrorKind::ResourceExhausted.failure_fact()
        );
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        assert_eq!(error.failure_fact().reason().code(), 7);
        assert_eq!(error.message(), READ_QUOTA_DETAIL);
        let shared = error
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        let quota = shared.downcast_ref::<ReadQuotaFailure>().unwrap();
        assert_eq!(quota.original.class(), OriginalResultCheckClass::Control);
        assert!(
            quota
                .original
                .source()
                .unwrap()
                .downcast_ref::<OriginalRefusal>()
                .is_some()
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            error_backing_bytes().unwrap(),
            ConnectorError::static_domain_backing_bytes::<ReadQuotaFailure>(READ_QUOTA_DETAIL)
                .unwrap()
        );
        drop(error);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn provider_error_is_moved_without_another_cause_or_diagnostic_carrier() {
        let input = super::super::scoped_snapshot::raw_read_failure(crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::TableNotFound,
            "private original table",
        ));
        let fact = input.failure_fact();
        let original = input
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
            .clone();
        let output = checked_read(ReadFailure::Original(input.into()));
        assert_eq!(output.failure_fact(), fact);
        assert_eq!(
            output
                .source()
                .unwrap()
                .downcast_ref::<SharedFailureCause>(),
            Some(&original)
        );
        assert_eq!(output.message(), original.detail());
    }
}
