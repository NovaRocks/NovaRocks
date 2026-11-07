// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0.

//! Side-effect tripwires for document-management admission tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use novarocks_spi::connector::ConnectorError;

use crate::catalog::error::{CatalogOutcome, CatalogUnsupported};
use crate::catalog::{CatalogNamespaceName, CatalogTableName, CatalogTablePage, NovaRocksCatalog};

#[derive(Debug)]
pub(crate) struct AdmissionCatalogSpy {
    inner: Arc<dyn NovaRocksCatalog>,
    reads: AtomicUsize,
    mutations: AtomicUsize,
}

impl AdmissionCatalogSpy {
    pub(crate) fn new(inner: Arc<dyn NovaRocksCatalog>) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            mutations: AtomicUsize::new(0),
        }
    }

    pub(crate) fn assert_no_io(&self) {
        assert_eq!(self.reads.load(Ordering::SeqCst), 0, "catalog reads");
        assert_eq!(
            self.mutations.load(Ordering::SeqCst),
            0,
            "catalog mutations"
        );
    }

    fn read<T>(&self) -> T {
        self.reads.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported document admission attempted catalog I/O");
    }

    fn mutation<T>(&self) -> T {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported document admission attempted a catalog mutation");
    }
}

#[async_trait]
impl NovaRocksCatalog for AdmissionCatalogSpy {
    fn bind_publication_generation(
        &self,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
    ) -> Result<(), ConnectorError> {
        self.inner.bind_publication_generation(generation)
    }
    fn evidence_policy(&self) -> crate::catalog::publication::CatalogEvidencePolicy {
        self.inner.evidence_policy()
    }
    async fn prepare_catalog_publication(
        &self,
        request: crate::catalog::catalog_request::FrozenCatalogPublication,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
        document: crate::catalog::catalog_request::CatalogResponseDocument,
        read_binding: crate::access_binding::IcebergReadBinding,
    ) -> crate::catalog::publication_dispatch::PreparationResult<
        crate::catalog::publication_dispatch::PreparedPublication,
    > {
        let _ = (
            request,
            descriptor,
            generation,
            preparation,
            document,
            read_binding,
        );
        self.mutation()
    }
    async fn prepare_frozen_publication(
        &self,
        request: crate::commit::model::FrozenRequest,
        operation: crate::catalog::admission::CatalogOperation,
        staged_expectation: Option<crate::catalog::StagedPublicationExpectation>,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
    ) -> crate::catalog::publication_dispatch::PreparationResult<
        crate::catalog::publication_dispatch::PreparedPublication,
    > {
        let _ = (
            request,
            operation,
            staged_expectation,
            descriptor,
            generation,
            preparation,
        );
        self.mutation()
    }

    fn listing_admission(&self) -> Arc<crate::catalog::listing_admission::ListingAdmission> {
        Arc::new(Default::default())
    }

    fn implementation_name(&self) -> &'static str {
        self.inner.implementation_name()
    }

    fn admit_operation(
        &self,
        operation: &crate::catalog::admission::CatalogOperation,
        target: &crate::catalog::admission::CatalogAdmissionTarget,
    ) -> Result<(), CatalogUnsupported> {
        self.inner.admit_operation(operation, target)
    }
    fn admit_initiation(
        &self,
        request: &crate::catalog::admission::CatalogAdmissionRequest,
    ) -> Result<crate::catalog::admission::CatalogAdmission, CatalogUnsupported> {
        self.inner.admit_initiation(request)
    }

    async fn list_namespaces(
        &self,
        _bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.read()
    }

    async fn namespace_exists(
        &self,
        _namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        self.read()
    }

    async fn list_tables(
        &self,
        _namespace: CatalogNamespaceName,
        _bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.read()
    }

    async fn list_tables_page(
        &self,
        _namespace: CatalogNamespaceName,
        _page_token: Option<Arc<str>>,
        _page_size: usize,
    ) -> Result<CatalogTablePage, ConnectorError> {
        self.read()
    }

    async fn table_exists(&self, _table: CatalogTableName) -> Result<bool, ConnectorError> {
        self.read()
    }

    async fn load_table(
        &self,
        _table: CatalogTableName,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        self.read()
    }

    async fn view_exists(&self, _view: CatalogTableName) -> Result<bool, ConnectorError> {
        self.read()
    }

    async fn list_views(
        &self,
        _namespace: CatalogNamespaceName,
        _bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.read()
    }

    async fn load_view(
        &self,
        _view: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        self.read()
    }

    async fn anchor_written_metadata(
        &self,
        _table: CatalogTableName,
        _metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        self.mutation()
    }
}

/// Records every filesystem runtime entry without driving a future. The
/// warehouse and reserved metastore endpoint are checked separately, so a
/// passing test does not confuse an unpolled request with an external effect.
#[derive(Default)]
pub(crate) struct AdmissionFileIoSpy {
    dispatches: AtomicUsize,
}

impl AdmissionFileIoSpy {
    pub(crate) fn assert_no_dispatch(&self) {
        assert_eq!(
            self.dispatches.load(Ordering::SeqCst),
            0,
            "filesystem I/O dispatches"
        );
    }

    fn unexpected<T>(&self) -> T {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported document admission dispatched filesystem I/O");
    }
}

impl novarocks_fs::FileIoRuntime for AdmissionFileIoSpy {
    fn block_on_bytes(
        &self,
        _future: novarocks_fs::FileBytesFuture,
    ) -> novarocks_fs::FileResult<bytes::Bytes> {
        self.unexpected()
    }

    fn block_on_u64(&self, _future: novarocks_fs::FileU64Future) -> novarocks_fs::FileResult<u64> {
        self.unexpected()
    }
}

impl novarocks_fs::FileIoSpawner for AdmissionFileIoSpy {
    fn spawn(
        &self,
        _task: novarocks_fs::FileIoFuture,
    ) -> novarocks_fs::FileResult<novarocks_fs::FileIoJob> {
        self.unexpected()
    }
}

impl novarocks_fs::RefreshExecutor for AdmissionFileIoSpy {
    fn execute(&self, _job: Box<dyn FnOnce() + Send + 'static>) {
        self.unexpected()
    }
}
