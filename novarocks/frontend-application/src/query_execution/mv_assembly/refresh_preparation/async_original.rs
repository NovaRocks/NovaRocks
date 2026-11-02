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

//! Stable, closed original readers for refresh preparation and catalog capture.

use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};

use super::io_gate::MvRefreshPreparationIo;
use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::catalog_application::query_catalog::CatalogServiceSource;
use crate::catalog_application::query_materializer::{
    OriginalCatalogLookup, QueryLocalTableOverlay,
    build_catalog_service_provider_with_bindings_and_query_local_overlays,
};
use crate::mv::domain::application::MvApplicationError;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_sql::compiler::{
    SqlCapturedCatalog, SqlCapturedCatalogPreparation, SqlCompileControl,
};
use novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifact;

/// The opaque SQL artifact comes back unchanged with its captured catalog.
/// Parsing its private SQL remains the original SQL author's responsibility.
pub(crate) enum MvCatalogCaptureQuery {
    Query(Box<novarocks_parser::ast::Query>),
    FirstSql(SqlMvFirstRefreshArtifact),
}
pub(crate) struct MvCatalogCaptureInput {
    pub(crate) query: MvCatalogCaptureQuery,
    pub(crate) ports: IcebergMvCorePorts,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) context: ConnectorRequestContext,
    pub(crate) control: SqlCompileControl,
    pub(crate) bindings: Arc<QueryTableBindingStore>,
    pub(crate) overlays: Vec<QueryLocalTableOverlay>,
}
pub(crate) struct MvCatalogCaptureOutput {
    pub(crate) query: MvCatalogCaptureQuery,
    pub(crate) catalog: SqlCapturedCatalog,
    pub(crate) bindings: Arc<QueryTableBindingStore>,
}
pub(crate) struct MvRefreshDependencyGraphInput {
    pub(crate) readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    pub(crate) requested: novarocks_mv_application::dependency::MvDependencyObjectRef,
}
pub(crate) enum MvRefreshPreparationOutput {
    Create(Arc<crate::mv::domain::iceberg_refresh::IcebergMvCreatePlan>),
    Explain(Vec<String>),
    DependencySteps(Vec<crate::mv::domain::dependency::refresh::MvRefreshDependencyStep>),
    Refresh(Box<super::pure_reader::MvRefreshPrepared>),
    WriteAnalysis(Box<super::super::activation_preparation::MvWriteAnalysisPrepared>),
}

type Reader = Pin<
    Box<
        dyn Future<Output = Result<MvRefreshPreparationOutput, MvApplicationError>>
            + Send
            + 'static,
    >,
>;
struct Body {
    reader: Option<Reader>,
    result: Option<Result<MvRefreshPreparationOutput, MvApplicationError>>,
    received: bool,
}
/// The CPU packet and the original Action borrow this same Source. Neither a
/// waiter nor a CPU marker owns the unique reader, input graph or output.
pub(crate) struct MvRefreshPreparationOriginal {
    body: Mutex<Option<Body>>,
    poisoned: AtomicBool,
    io: Arc<MvRefreshPreparationIo>,
}
struct Loan<'a> {
    source: &'a MvRefreshPreparationOriginal,
    body: Option<Body>,
}
impl Drop for Loan<'_> {
    fn drop(&mut self) {
        let body = self.body.take().expect("MV reader loan owns its body");
        let mut slot = self.source.body.lock().unwrap_or_else(|e| e.into_inner());
        assert!(slot.is_none(), "MV reader loan was restored twice");
        *slot = Some(body);
    }
}
impl MvRefreshPreparationOriginal {
    pub(crate) fn for_dependency_graph(
        input: MvRefreshDependencyGraphInput,
        io: Arc<MvRefreshPreparationIo>,
    ) -> Arc<Self> {
        let reader_io = Arc::clone(&io);
        Arc::new(Self {
            body: Mutex::new(Some(Body {
                reader: Some(Box::pin(async move {
                    let facts = reader_io.refresh_graph(input.readiness).await?;
                    crate::mv::domain::dependency::refresh::finish_refresh_graph(
                        facts,
                        &input.requested,
                    )
                    .map(MvRefreshPreparationOutput::DependencySteps)
                    .map_err(|message| {
                        MvApplicationError::new(
                            crate::mv::domain::application::MvApplicationErrorKind::InvalidRequest,
                            message,
                        )
                    })
                })),
                result: None,
                received: false,
            })),
            poisoned: AtomicBool::new(false),
            io,
        })
    }
    #[cfg(test)]
    pub(crate) fn original_io_for_test(&self) -> &Arc<MvRefreshPreparationIo> {
        &self.io
    }

    pub(crate) fn is_for_io(&self, io: &Arc<MvRefreshPreparationIo>) -> bool {
        Arc::ptr_eq(&self.io, io)
    }
    pub(crate) fn for_refresh(
        input: Arc<super::pure_reader::MvRefreshPreparationInput>,
        io: Arc<MvRefreshPreparationIo>,
    ) -> Arc<Self> {
        let reader_io = Arc::clone(&io);
        Arc::new(Self {
            body: Mutex::new(Some(Body {
                reader: Some(Box::pin(async move {
                    super::pure_reader::prepare_refresh(input, &reader_io)
                        .await
                        .map(Box::new)
                        .map(MvRefreshPreparationOutput::Refresh)
                })),
                result: None,
                received: false,
            })),
            poisoned: AtomicBool::new(false),
            io,
        })
    }
    pub(crate) fn for_create(
        input: super::super::create_preparation::MvCreatePreparationInput,
        io: Arc<MvRefreshPreparationIo>,
    ) -> Arc<Self> {
        let reader_io = Arc::clone(&io);
        Arc::new(Self {
            body: Mutex::new(Some(Body {
                reader: Some(Box::pin(async move {
                    super::super::create_preparation::prepare_create(input, &reader_io)
                        .await
                        .map(MvRefreshPreparationOutput::Create)
                })),
                result: None,
                received: false,
            })),
            poisoned: AtomicBool::new(false),
            io,
        })
    }
    pub(crate) fn for_explain(
        input: super::super::refresh_explain::MvExplainPreparationInput,
        io: Arc<MvRefreshPreparationIo>,
    ) -> Arc<Self> {
        let reader_io = Arc::clone(&io);
        Arc::new(Self {
            body: Mutex::new(Some(Body {
                reader: Some(Box::pin(async move {
                    super::super::refresh_explain::explain_original(input, &reader_io)
                        .await
                        .map(MvRefreshPreparationOutput::Explain)
                })),
                result: None,
                received: false,
            })),
            poisoned: AtomicBool::new(false),
            io,
        })
    }
    pub(crate) fn for_write_analysis(
        input: super::super::activation_preparation::MvWriteAnalysisPreparationInput,
        io: Arc<MvRefreshPreparationIo>,
    ) -> Arc<Self> {
        let reader_io = Arc::clone(&io);
        Arc::new(Self {
            body: Mutex::new(Some(Body {
                reader: Some(Box::pin(async move {
                    super::super::activation_preparation::prepare_analysis(input, &reader_io)
                        .await
                        .map(Box::new)
                        .map(MvRefreshPreparationOutput::WriteAnalysis)
                })),
                result: None,
                received: false,
            })),
            poisoned: AtomicBool::new(false),
            io,
        })
    }
    fn loan(&self) -> Loan<'_> {
        let body = self
            .body
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("MV reader has one original driver");
        Loan {
            source: self,
            body: Some(body),
        }
    }

    /// The closed InternalResultCpu turn is the only caller. Every suspension
    /// below is on the named I/O gate, never on an external Provider future.
    pub(crate) fn run_once(&self) {
        assert!(
            !self.poisoned.swap(true, Ordering::AcqRel),
            "MV reader is poisoned"
        );
        let mut loan = self.loan();
        let body = loan.body.as_mut().unwrap();
        if body.result.is_some() || body.received {
            self.poisoned.store(false, Ordering::Release);
            return;
        }
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        match body
            .reader
            .as_mut()
            .expect("MV reader is retained")
            .as_mut()
            .poll(&mut cx)
        {
            Poll::Pending => assert!(
                self.io.has_request(),
                "MV reader suspended outside its named I/O gate"
            ),
            Poll::Ready(result) => {
                body.result = Some(result);
                // The result is stable before any child Future can drop.
                let retired = body.reader.take();
                drop(retired);
            }
        }
        self.poisoned.store(false, Ordering::Release);
    }
    pub(crate) fn is_ready(&self) -> bool {
        let loan = self.loan();
        let body = loan.body.as_ref().unwrap();
        body.result.is_some() || body.received
    }
    pub(crate) fn poll_io(&self, cx: &mut Context<'_>) -> Poll<()> {
        assert!(
            !self.poisoned.load(Ordering::Acquire),
            "MV reader is poisoned"
        );
        self.io.poll_io(cx)
    }
    /// Observation of the original foreign-poll boundary only. The caller
    /// retains the actual CPU marker/cause; this does not fabricate an exit.
    pub(crate) fn foreign_exit_unproven(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub(crate) fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.io.local_health_failure()
    }
    pub(crate) fn poll_closed(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), novarocks_query_application::api::StatementNativeHealthFailure>> {
        if self.poisoned.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        match self.io.poll_closed(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        let mut loan = self.loan();
        let retired = loan.body.as_mut().unwrap().reader.take();
        self.poisoned.store(true, Ordering::Release);
        drop(retired);
        self.poisoned.store(false, Ordering::Release);
        Poll::Ready(Ok(()))
    }
    pub(crate) fn take_result_in(
        &self,
        destination: &mut Option<Result<MvRefreshPreparationOutput, MvApplicationError>>,
    ) -> bool {
        assert!(
            destination.is_none(),
            "MV preparation result receiver is occupied"
        );
        let mut loan = self.loan();
        let body = loan.body.as_mut().unwrap();
        let Some(result) = body.result.take() else {
            return false;
        };
        *destination = Some(result);
        body.received = true;
        true
    }
}

pub(crate) async fn capture_catalog(
    input: MvCatalogCaptureInput,
    io: &MvRefreshPreparationIo,
) -> Result<MvCatalogCaptureOutput, MvApplicationError> {
    let preparation = match &input.query {
        MvCatalogCaptureQuery::Query(query) => SqlCapturedCatalogPreparation::try_new(
            query.as_ref().clone(),
            input.current_catalog.as_deref(),
            &input.current_database,
            input.control.clone(),
        ),
        MvCatalogCaptureQuery::FirstSql(source) => source.catalog_preparation(
            input.current_catalog.as_deref(),
            &input.current_database,
            input.control.clone(),
        ),
    }
    .map_err(MvApplicationError::from_compile)?;
    let materializer = build_catalog_service_provider_with_bindings_and_query_local_overlays(
        input.current_catalog.as_deref(),
        input.ports.catalog_service().as_ref(),
        input.ports.connector_control(),
        input.context.clone(),
        Arc::clone(&input.bindings),
        input.overlays,
        input.ports.catalog_application(),
    );
    let mut facts = Vec::with_capacity(preparation.needs().len());
    for need in preparation.needs() {
        let fact = match materializer
            .begin_original_catalog_lookup(need, input.ports.connector_control())?
        {
            OriginalCatalogLookup::Fact(fact) => fact,
            OriginalCatalogLookup::Metadata(lookup) => {
                let observed = io
                    .metadata(
                        &lookup.lease,
                        input.context.clone(),
                        &lookup.namespace,
                        &lookup.physical_table,
                        lookup.resolution,
                    )
                    .await;
                materializer.finish_original_catalog_lookup(lookup, observed)?
            }
        };
        facts.push(fact);
    }
    let catalog = preparation
        .capture(&facts)
        .map_err(MvApplicationError::from_compile)?;
    Ok(MvCatalogCaptureOutput {
        query: input.query,
        catalog,
        bindings: input.bindings,
    })
}
