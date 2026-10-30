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

//! Original raw pages and closed CPU turns of one bounded view listing.
use super::*;
use crate::catalog_control::statistics::scoped_cpu::{
    IcebergMetadataCpuOriginal, run_provider_metadata,
};
use novarocks_spi::connector::{
    ConnectorListingCollector, ConnectorRequestContext, ProviderMetadataCpuPort,
};
use std::collections::HashSet;

pub(crate) struct PageRequest {
    namespace: NamespaceIdent,
    token: Option<String>,
    size: usize,
}
pub(crate) enum Next {
    Page(PageRequest),
    Complete(Result<Vec<ConnectorViewIdentity>, ConnectorError>),
}
pub(crate) struct Work {
    provider: IcebergMetadata,
    request: ConnectorListViewsRequest,
    namespace: Option<NamespaceIdent>,
    collector: Option<ConnectorListingCollector<String>>,
    seen: HashSet<String>,
    raw: Option<Result<Vec<u8>, ConnectorError>>,
    next: Option<PageRequest>,
    result: Option<Result<Vec<ConnectorViewIdentity>, ConnectorError>>,
}
impl Work {
    pub(crate) fn new(provider: IcebergMetadata, request: ConnectorListViewsRequest) -> Self {
        Self {
            provider,
            request,
            namespace: None,
            collector: None,
            seen: HashSet::new(),
            raw: None,
            next: None,
            result: None,
        }
    }
    pub(crate) fn context(&self) -> &ConnectorRequestContext {
        &self.request.context
    }
    pub(crate) fn receive(&mut self, raw: Result<Vec<u8>, ConnectorError>) {
        if let Err(original) = &raw {
            self.result = Some(Err(original.clone()));
        }
        assert!(self.raw.is_none(), "one original view page receiving slot");
        self.raw = Some(raw);
    }
    pub(crate) fn error(&self) -> Option<ConnectorError> {
        self.result
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .cloned()
    }
    pub(crate) fn next(&mut self) -> Next {
        if let Some(result) = self.result.take() {
            return Next::Complete(result);
        }
        Next::Page(self.next.take().expect("same CPU authored the next page"))
    }
    pub(crate) fn run(&mut self) {
        if self.error().is_some() {
            return;
        }
        if let Err(original) = self.phase() {
            self.result = Some(Err(original));
        }
    }
    fn phase(&mut self) -> Result<(), ConnectorError> {
        ensure_request(
            &self.provider,
            &self.request.namespace.instance_id,
            &self.request.context,
        )?;
        if let Some(namespace) = &self.namespace {
            let raw = self.raw.take().expect("one received original page")?;
            let page: crate::rest_protocol::wire::ListTablesResponse = serde_json::from_slice(&raw)
                .map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::Unavailable,
                        "Iceberg REST list views returned a response that could not be decoded",
                    )
                })?;
            if page
                .identifiers
                .iter()
                .any(|view| view.namespace() != namespace)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg REST catalog returned an out-of-scope view identifier",
                ));
            }
            let collector = self.collector.as_mut().unwrap();
            crate::catalog::rest::preflight_rest_page(
                collector,
                page.identifiers.iter().map(|view| view.name.as_str()),
                page.next_page_token.as_deref(),
                &self.seen,
            )?;
            crate::catalog::rest::accept_rest_page(
                collector,
                page.identifiers
                    .iter()
                    .map(|view| view.name.to_owned())
                    .collect(),
                page.next_page_token,
                &mut self.seen,
            )?;
            if collector.continuation_token().is_none() {
                let names = crate::catalog::delegate::sorted_unique(
                    self.collector.take().unwrap().finish()?,
                );
                self.result = Some(listed_view_identities(
                    &self.provider,
                    &self.request,
                    Ok(names),
                ));
                return Ok(());
            }
        } else {
            self.namespace = Some(crate::catalog::delegate::namespace_ident(
                &crate::catalog::CatalogNamespaceName::new(
                    self.request.namespace.namespace.clone(),
                ),
            )?);
            self.collector = Some(ConnectorListingCollector::new(self.request.bound)?);
        }
        self.next = Some(PageRequest {
            namespace: self.namespace.as_ref().unwrap().clone(),
            token: self
                .collector
                .as_ref()
                .unwrap()
                .continuation_token()
                .map(str::to_owned),
            size: self.request.bound.page_entries,
        });
        Ok(())
    }
}

pub(crate) async fn run(
    endpoints: Arc<crate::rest_protocol::endpoints::RestEndpoints>,
    source: Arc<IcebergMetadataCpuOriginal>,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
    let context = source.view_listing_context();
    let http = crate::rest_protocol::http::RequestContext::from_connector(&context);
    loop {
        // Raw receipt has priority over any later stop or CPU refusal.
        if let Some(original) = source.view_listing_error() {
            return Err(original);
        }
        let finished = run_provider_metadata(&cpu, &source).await;
        if let Some(original) = source.view_listing_error() {
            return Err(original);
        }
        finished?;
        match source.view_listing_next() {
            Next::Complete(result) => return result,
            Next::Page(page) => {
                let mut child = Box::pin(endpoints.list_views_page_bytes_in(
                    &page.namespace,
                    page.token.as_deref(),
                    page.size,
                    &http,
                ));
                let raw = std::future::poll_fn(|cx| child.as_mut().poll(cx)).await;
                // The original result is stable before this same opaque F Drop.
                source.view_listing_receive(raw);
                drop(child);
            }
        }
    }
}
