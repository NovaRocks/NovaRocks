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

//! Runner-owned shared REST namespace and fixed run-storage checks.
use crate::types::{ConnectionConfig, IcebergViewFixtureTarget};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::fixture_run::{RunStorageScope, ScratchIdentity, canonical_s3};
use novarocks_cluster_harness::isolated_iceberg_rest::{RestObjectStoreOwner, RestStorageBinding};
use novarocks_parser::ast::{
    Expr, LiteralKind, ObjectName, Relation, TrinoCreateCatalog, TrinoStatement,
};
use novarocks_parser::parser;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Debug)]
pub(crate) enum Catalog {
    Rest(String),
    Hadoop(String),
    Other,
}
#[derive(Debug)]
pub(crate) enum PreparedTrinoNamespace {
    None,
    RejectedSyntax,
    CatalogCreated(String, Catalog),
    CatalogDropped(String),
    VerifyObject {
        base: String,
        namespace: String,
        object: String,
        collection: &'static str,
    },
}

fn fixture_catalog(value: &TrinoCreateCatalog) -> Result<Catalog> {
    if !value.connector_name.value.eq_ignore_ascii_case("iceberg") {
        return Ok(Catalog::Other);
    }
    let mut properties = BTreeMap::new();
    for property in &value.properties {
        let key = property.name.value.to_ascii_lowercase();
        if !matches!(
            key.as_str(),
            "iceberg.catalog.type"
                | "uri"
                | "iceberg.catalog.uri"
                | "warehouse"
                | "iceberg.catalog.warehouse"
        ) {
            continue;
        }
        let Some(Expr::Literal(literal)) = &property.value else {
            anyhow::bail!("fixture catalog ownership requires a literal {key}");
        };
        let LiteralKind::String(interpreted) = &literal.kind else {
            anyhow::bail!("fixture catalog ownership requires a string {key}");
        };
        ensure!(
            properties.insert(key, interpreted).is_none(),
            "fixture catalog repeats an ownership property"
        );
    }
    // The provider's two accepted aliases have ordered precedence.
    match properties
        .get("iceberg.catalog.type")
        .map(|kind| kind.to_ascii_lowercase())
        .as_deref()
    {
        Some("rest") => Ok(Catalog::Rest(
            properties
                .get("uri")
                .or_else(|| properties.get("iceberg.catalog.uri"))
                .context("REST catalog omitted exact URI")?
                .trim_end_matches('/')
                .to_owned(),
        )),
        None | Some("hadoop") => Ok(Catalog::Hadoop(
            (*properties
                .get("iceberg.catalog.warehouse")
                .or_else(|| properties.get("warehouse"))
                .context("Hadoop catalog omitted exact warehouse")?)
            .clone(),
        )),
        _ => Ok(Catalog::Other),
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct CatalogFacts(
    Arc<Mutex<BTreeMap<String, Catalog>>>,
    Arc<Mutex<BTreeMap<String, RestObjectStoreOwner>>>,
);
impl CatalogFacts {
    pub fn bind_fixture_storage(&self, binding: &RestStorageBinding) -> Result<()> {
        self.bind_origins(&binding.rest_uris(), binding.owner())
    }
    fn bind_origins(&self, origins: &[&str], owner: RestObjectStoreOwner) -> Result<()> {
        let mut facts = self
            .1
            .lock()
            .map_err(|_| anyhow::anyhow!("SQL REST owner facts lock poisoned"))?;
        for origin in origins {
            ensure!(!origin.trim().is_empty(), "owned REST endpoint is empty");
            if let Some(previous) = facts.get(origin.trim_end_matches('/')) {
                ensure!(
                    *previous == owner,
                    "owned REST endpoint changed its storage owner"
                );
            }
        }
        for origin in origins {
            facts.insert(origin.trim_end_matches('/').to_string(), owner);
        }
        Ok(())
    }
    pub fn bind_owned_proxy_alias(&self, alias: &str, downstream: &str) -> Result<()> {
        let owner = self
            .1
            .lock()
            .map_err(|_| anyhow::anyhow!("SQL REST owner facts lock poisoned"))?
            .get(downstream.trim_end_matches('/'))
            .copied()
            .context("owned REST proxy downstream has no fixture storage binding")?;
        self.bind_origins(&[alias], owner)
    }
    fn rest_storage_owner(&self, origin: &str) -> Result<Option<RestObjectStoreOwner>> {
        Ok(self
            .1
            .lock()
            .map_err(|_| anyhow::anyhow!("SQL REST owner facts lock poisoned"))?
            .get(origin.trim_end_matches('/'))
            .copied())
    }
    fn private_rest(&self, origin: &str) -> Result<bool> {
        Ok(self.rest_storage_owner(origin)? == Some(RestObjectStoreOwner::PrivateObjectStore))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NamespaceGuard {
    identity: ScratchIdentity,
    storage: Option<RunStorageScope>,
    facts: CatalogFacts,
    catalog: Option<String>,
    namespace: Option<String>,
    deadline: Instant,
}
impl NamespaceGuard {
    pub fn new(
        identity: &ScratchIdentity,
        storage: Option<&RunStorageScope>,
        facts: CatalogFacts,
        conn: &ConnectionConfig,
        deadline: Instant,
    ) -> Self {
        Self {
            identity: identity.clone(),
            storage: storage.cloned(),
            facts,
            catalog: conn.catalog.clone(),
            namespace: conn.db.clone(),
            deadline,
        }
    }
    pub fn for_cleanup(&self, deadline: Instant) -> Self {
        let mut guard = self.clone();
        guard.deadline = deadline;
        guard
    }
    pub fn set_session_context(&mut self, catalog: Option<String>, schema: Option<String>) {
        self.catalog = catalog;
        self.namespace = schema;
    }
    fn catalog_fact(&self, name: &str) -> Result<Option<Catalog>> {
        Ok(self
            .facts
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("SQL catalog facts lock poisoned"))?
            .get(name)
            .cloned())
    }
    fn target(
        &self,
        name: &ObjectName,
        database: bool,
    ) -> Result<Option<(String, String, Option<String>)>> {
        let parts = name
            .parts
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>();
        ensure!(
            (1..=if database { 2 } else { 3 }).contains(&parts.len()),
            "unproven qualification for guarded SQL object"
        );
        Ok(match (database, parts.as_slice()) {
            (true, [catalog, namespace]) => Some((catalog.clone(), namespace.clone(), None)),
            (true, [namespace]) => self
                .catalog
                .clone()
                .map(|catalog| (catalog, namespace.clone(), None)),
            (false, [catalog, namespace, table]) => {
                Some((catalog.clone(), namespace.clone(), Some(table.clone())))
            }
            (false, [namespace, table]) => self
                .catalog
                .clone()
                .map(|catalog| (catalog, namespace.clone(), Some(table.clone()))),
            (false, [table]) => self
                .catalog
                .clone()
                .zip(self.namespace.clone())
                .map(|(catalog, namespace)| (catalog, namespace, Some(table.clone()))),
            _ => None,
        })
    }
    fn owned(&self, namespace: &str) -> bool {
        if let Some(storage) = &self.storage {
            return storage.owns_namespace(namespace);
        }
        namespace
            .strip_prefix(&format!("{}_", self.identity.run_id))
            .is_some_and(|purpose| {
                namespace.len() <= 63
                    && !purpose.is_empty()
                    && purpose.as_bytes()[0].is_ascii_lowercase()
                    && purpose
                        .bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
            })
    }
    fn within(&self, uri: &str) -> Result<bool> {
        let value = canonical_s3(uri)?;
        Ok(value == self.identity.scope_root
            || value.starts_with(&format!("{}/", self.identity.scope_root)))
    }
    fn ensure_write(&self, name: &ObjectName, database: bool) -> Result<()> {
        let Some((catalog, namespace, _)) = self.target(name, database)? else {
            return Ok(());
        };
        match self.catalog_fact(&catalog)? {
            Some(Catalog::Rest(base)) => {
                if !self.facts.private_rest(&base)? {
                    ensure!(
                        self.owned(&namespace),
                        "shared REST write requires an exact namespace of the admitted run"
                    );
                }
            }
            Some(Catalog::Hadoop(root)) if root.starts_with("s3://") => {
                ensure!(
                    self.within(&root)?,
                    "prepared Hadoop warehouse is read-only"
                );
            }
            _ => {}
        }
        Ok(())
    }
    pub(crate) fn prepare_trino(
        &self,
        sql: &str,
        enrolled_syntax_negative: bool,
    ) -> Result<PreparedTrinoNamespace> {
        ensure!(
            Instant::now() < self.deadline,
            "original SQL namespace clock expired"
        );
        let parsed = match parser::parse_statement(sql) {
            Ok(parsed) => parsed,
            Err(
                novarocks_parser::ParserError::Lex(_) | novarocks_parser::ParserError::Parse(_),
            ) if enrolled_syntax_negative => return Ok(PreparedTrinoNamespace::RejectedSyntax),
            Err(error) => return Err(error).context("prove guarded SQL namespace ownership"),
        };
        self.prepare_statement(&parsed)
    }

    fn prepare_statement(&self, parsed: &TrinoStatement) -> Result<PreparedTrinoNamespace> {
        use TrinoStatement::*;
        let (name, database) = match parsed {
            CreateCatalog(value) => {
                let name = novarocks_types::naming::normalize_identifier(&value.catalog_name.value)
                    .map_err(anyhow::Error::msg)?;
                // A conditional attachment cannot overwrite an already known effective fact.
                if value.not_exists && self.catalog_fact(&name)?.is_some() {
                    return Ok(PreparedTrinoNamespace::None);
                }
                return Ok(PreparedTrinoNamespace::CatalogCreated(
                    name,
                    fixture_catalog(value)?,
                ));
            }
            DropCatalog(value) => {
                return Ok(PreparedTrinoNamespace::CatalogDropped(
                    value.catalog_name.value.to_ascii_lowercase(),
                ));
            }
            CreateSchema(value) => (&value.schema_name, true),
            DropSchema(value) => (&value.schema_name, true),
            RenameSchema(value) => (&value.source, true),
            CreateTable(value) => return self.prepare_verify(&value.name, "tables"),
            CreateTableAsSelect(value) => return self.prepare_verify(&value.name, "tables"),
            CreateView(value) => return self.prepare_verify(&value.name, "views"),
            CreateMaterializedView(value) => return self.prepare_verify(&value.name, "tables"),
            DropTable(value) => (&value.table_name, false),
            DropView(value) => (&value.name, false),
            DropMaterializedView(value) => (&value.name, false),
            RefreshView(value) => (&value.name, false),
            RefreshMaterializedView(value) => (&value.table.name, false),
            MaterializedViewExecute(value) => (&value.table.name, false),
            TruncateTable(value) => (&value.table_name, false),
            Insert(value) => (&value.table.name, false),
            Delete(value) => (&value.table.name, false),
            Update(value) => (&value.table.name, false),
            Merge(value) => {
                let mut relation = &value.target;
                while let Relation::Aliased(alias) = relation {
                    relation = alias.relation.as_ref();
                }
                let Relation::Table(table) = relation else {
                    anyhow::bail!("guarded MERGE target is not a table");
                };
                (&table.name, false)
            }
            TableExecute(value) => (&value.table.name, false),
            RenameTable(value) => {
                self.ensure_write(&value.target, false)?;
                (&value.source, false)
            }
            RenameView(value) => {
                self.ensure_write(&value.target, false)?;
                (&value.source, false)
            }
            RenameMaterializedView(value) => {
                self.ensure_write(&value.target, false)?;
                (&value.source, false)
            }
            SetProperties(value) => (&value.name, false),
            AddColumn(value) => (&value.name, false),
            DropColumn(value) => (&value.table, false),
            RenameColumn(value) => (&value.table, false),
            SetDefaultValue(value) => (&value.table_name, false),
            DropDefaultValue(value) => (&value.table_name, false),
            SetColumnType(value) => (&value.table_name, false),
            DropNotNullConstraint(value) => (&value.table, false),
            CreateBranch(value) => (&value.table_name, false),
            DropBranch(value) => (&value.table_name, false),
            FastForwardBranch(value) => (&value.table_name, false),
            ExplainAnalyze(value) => return self.prepare_statement(&value.statement),
            // Role, query, session and procedure admission remain with the FE.
            _ => return Ok(PreparedTrinoNamespace::None),
        };
        self.ensure_write(name, database)?;
        Ok(PreparedTrinoNamespace::None)
    }

    fn prepare_verify(
        &self,
        name: &ObjectName,
        collection: &'static str,
    ) -> Result<PreparedTrinoNamespace> {
        self.ensure_write(name, false)?;
        if let Some((catalog, namespace, Some(object))) = self.target(name, false)?
            && let Some(Catalog::Rest(base)) = self.catalog_fact(&catalog)?
            && !self.facts.private_rest(&base)?
        {
            return Ok(PreparedTrinoNamespace::VerifyObject {
                base,
                namespace,
                object,
                collection,
            });
        }
        Ok(PreparedTrinoNamespace::None)
    }

    pub(crate) fn complete_trino(&mut self, prepared: PreparedTrinoNamespace) -> Result<()> {
        match prepared {
            PreparedTrinoNamespace::None => {}
            PreparedTrinoNamespace::RejectedSyntax => {
                anyhow::bail!("guarded syntax-negative SQL unexpectedly succeeded")
            }
            PreparedTrinoNamespace::CatalogCreated(name, fact) => {
                self.facts
                    .0
                    .lock()
                    .map_err(|_| anyhow::anyhow!("SQL catalog facts lock poisoned"))?
                    .insert(name, fact);
            }
            PreparedTrinoNamespace::CatalogDropped(name) => {
                self.facts
                    .0
                    .lock()
                    .map_err(|_| anyhow::anyhow!("SQL catalog facts lock poisoned"))?
                    .remove(&name);
            }
            PreparedTrinoNamespace::VerifyObject {
                base,
                namespace,
                object,
                collection,
            } => {
                self.verify_object(&base, &namespace, &object, collection)?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn before(&self, sql: &str) -> Result<()> {
        for frame in crate::session::split_trino_sql_statements(sql)? {
            self.prepare_trino(&frame, false)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn after(&mut self, sql: &str) -> Result<()> {
        for frame in crate::session::split_trino_sql_statements(sql)? {
            let effect = self.prepare_trino(&frame, false)?;
            self.complete_trino(effect)?;
        }
        Ok(())
    }

    fn remaining(&self) -> Result<std::time::Duration> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        ensure!(!left.is_zero(), "original REST namespace deadline expired");
        Ok(left)
    }
    fn namespace_url(base: &str, namespace: &str) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(&format!("{base}/v1/namespaces/"))?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid REST namespace base URI"))?
            .pop_if_empty()
            .push(namespace);
        Ok(url)
    }
    pub(crate) fn orphan_fixture_table_url(
        &self,
        catalog: Option<&str>,
        namespace: &str,
        table: &str,
        rest_endpoint: &str,
    ) -> Result<reqwest::Url> {
        ensure!(
            self.owned(namespace) && self.namespace.as_deref() == Some(namespace),
            "orphan fixture must address the original case namespace"
        );
        let catalog = catalog
            .or(self.catalog.as_deref())
            .context("orphan fixture has no bound catalog")?;
        let Some(Catalog::Rest(base)) = self.catalog_fact(catalog)? else {
            anyhow::bail!("orphan fixture catalog {catalog} is not a bound REST catalog");
        };
        ensure!(
            base == rest_endpoint.trim_end_matches('/')
                && self.facts.rest_storage_owner(&base)?.is_some(),
            "orphan fixture catalog differs from the original REST fixture"
        );
        let mut url = Self::namespace_url(&base, namespace)?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid orphan fixture table URI"))?
            .push("tables")
            .push(table);
        Ok(url)
    }

    pub(crate) fn view_fixture_catalog<'a>(
        &'a self,
        target: &'a IcebergViewFixtureTarget,
    ) -> Result<&'a str> {
        target
            .catalog
            .as_deref()
            .or(self.catalog.as_deref())
            .context("Iceberg view fixture has no bound catalog")
    }

    pub(crate) fn view_fixture_url(
        &self,
        target: &IcebergViewFixtureTarget,
        rest_endpoint: &str,
        owns_case_namespace: impl FnOnce(&str) -> bool,
    ) -> Result<reqwest::Url> {
        let namespace = &target.namespace;
        ensure!(
            self.owned(namespace) && owns_case_namespace(namespace),
            "Iceberg view fixture must address a namespace declared by the original case"
        );
        let catalog = self.view_fixture_catalog(target)?;
        let Some(Catalog::Rest(base)) = self.catalog_fact(catalog)? else {
            anyhow::bail!("Iceberg view fixture catalog {catalog} is not a bound REST catalog");
        };
        ensure!(
            base == rest_endpoint.trim_end_matches('/')
                && (self.facts.rest_storage_owner(&base)?.is_some() || self.storage.is_some()),
            "Iceberg view fixture catalog differs from the original REST fixture"
        );
        let mut url = Self::namespace_url(&base, namespace)?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid Iceberg view fixture URI"))?
            .push("views")
            .push(&target.view);
        Ok(url)
    }

    fn verify_object(
        &self,
        base: &str,
        namespace: &str,
        object: &str,
        collection: &str,
    ) -> Result<()> {
        ensure!(
            self.owned(namespace),
            "REST table validation refused a foreign namespace"
        );
        let mut url = Self::namespace_url(base, namespace)?;
        url.path_segments_mut()
            .unwrap()
            .push(collection)
            .push(object);
        let response: serde_json::Value = reqwest::blocking::Client::new()
            .get(url)
            .timeout(self.remaining()?)
            .send()?
            .error_for_status()?
            .json()?;
        let location = response["metadata"]["location"]
            .as_str()
            .context("REST table metadata omitted actual location")?;
        ensure!(
            self.storage
                .as_ref()
                .context("shared REST verification omitted original storage projection")?
                .belongs(location)?,
            "REST object actual location lies outside its admitted run storage scope"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    fn guard() -> NamespaceGuard {
        let identity = ScratchIdentity::create("s3://novarocks/guard-tests").unwrap();
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../docker/iceberg-rest/tests/run_storage_scope_vectors.json"
        ))
        .unwrap();
        let storage = RunStorageScope::from_record(&identity, &vectors["record"]).unwrap();
        NamespaceGuard::new(
            &identity,
            Some(&storage),
            CatalogFacts::default(),
            &ConnectionConfig {
                time_zone: "Asia/Shanghai".into(),

                host: "127.0.0.1".into(),
                port: "0".into(),
                user: "root".into(),

                catalog: Some("catalog".into()),
                db: None,
            },
            Instant::now() + Duration::from_secs(10),
        )
    }
    #[test]
    fn orphan_fixture_requires_the_bound_rest_catalog_and_exact_case_namespace() {
        let mut guard = guard();
        let namespace = format!("{}_case", guard.identity.run_id);
        guard.set_session_context(Some("catalog".into()), Some(namespace.clone()));
        let endpoint = "http://127.0.0.1:1";
        guard.after("CREATE CATALOG catalog USING iceberg WITH (\"iceberg.catalog.warehouse\" = 's3://novarocks/guard-tests')").unwrap();
        assert!(
            guard
                .orphan_fixture_table_url(None, &namespace, "test", endpoint)
                .is_err()
        );

        guard.after("CREATE CATALOG owned_rest USING iceberg WITH (\"iceberg.catalog.type\" = 'rest', \"uri\" = 'http://127.0.0.1:1')").unwrap();
        assert!(
            guard
                .orphan_fixture_table_url(Some("owned_rest"), &namespace, "test", endpoint)
                .is_err()
        );
        guard
            .facts
            .bind_origins(&[endpoint], RestObjectStoreOwner::MachineScratch)
            .unwrap();
        let url = guard
            .orphan_fixture_table_url(Some("owned_rest"), &namespace, "test", endpoint)
            .unwrap();
        assert_eq!(
            url.as_str(),
            format!("{endpoint}/v1/namespaces/{namespace}/tables/test")
        );
        assert!(
            guard
                .orphan_fixture_table_url(
                    Some("owned_rest"),
                    &namespace,
                    "test",
                    "http://127.0.0.1:2"
                )
                .is_err()
        );
        let foreign_case = format!("{}_peer", guard.identity.run_id);
        assert!(guard.owned(&foreign_case));
        assert!(
            guard
                .orphan_fixture_table_url(Some("owned_rest"), &foreign_case, "test", endpoint)
                .is_err()
        );
    }

    #[test]
    fn view_fixture_uses_the_published_endpoint_and_the_declared_case_namespace() {
        let mut guard = guard();
        let endpoint = "http://127.0.0.1:1";
        let different_endpoint = "http://127.0.0.1:2";
        let namespace = format!("{}_case", guard.identity.run_id);
        let mut target = IcebergViewFixtureTarget {
            catalog: None,
            namespace: namespace.clone(),
            view: "fixture".into(),
        };
        guard.set_session_context(Some("mv".into()), Some("different_schema".into()));
        guard
            .after("CREATE CATALOG mv USING iceberg WITH (\"iceberg.catalog.type\" = 'rest', \"uri\" = 'http://127.0.0.1:1')")
            .unwrap();
        // Shared publications carry original run storage and pass their exact
        // published origin, while private fixtures bind their origins directly.
        assert!(guard.view_fixture_url(&target, endpoint, |_| true).is_ok());
        guard.storage = None;
        assert!(guard.view_fixture_url(&target, endpoint, |_| true).is_err());
        guard
            .facts
            .bind_origins(
                &[endpoint, different_endpoint],
                RestObjectStoreOwner::MachineScratch,
            )
            .unwrap();
        let url = guard
            .view_fixture_url(&target, endpoint, |name| name == namespace)
            .unwrap();
        assert_eq!(
            url.as_str(),
            format!("{endpoint}/v1/namespaces/{namespace}/views/fixture")
        );
        assert!(
            guard
                .view_fixture_url(&target, endpoint, |_| false)
                .is_err()
        );
        assert!(
            guard
                .view_fixture_url(&target, different_endpoint, |_| true)
                .is_err()
        );
        target.namespace = "foreign".into();
        assert!(guard.view_fixture_url(&target, endpoint, |_| true).is_err());
        target.namespace = namespace;
        target.catalog = Some("different_catalog".into());
        assert!(guard.view_fixture_url(&target, endpoint, |_| true).is_err());
    }

    fn server(responses: Vec<serde_json::Value>) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let thread = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let deadline = Instant::now() + Duration::from_secs(5);
                let (mut stream, _) = loop {
                    match listener.accept() {
                        Ok(value) => break value,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(5))
                        }
                        Err(error) => panic!("expected REST request before deadline: {error}"),
                    }
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0; 1024];
                loop {
                    let n = stream.read(&mut chunk).unwrap();
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&bytes[..end]);
                        let size = head
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|n| n.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + size {
                            break;
                        }
                    }
                }
                requests.push(String::from_utf8(bytes).unwrap());
                let body = response.to_string();
                write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
            }
            requests
        });
        (base, thread)
    }
    #[test]
    fn exact_private_rest_owner_preserves_ordinary_names_and_does_not_relocate_tables() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let mut guard = guard();
        guard
            .facts
            .bind_origins(&[&base], RestObjectStoreOwner::PrivateObjectStore)
            .unwrap();
        guard.after(&format!(r#"CREATE CATALOG catalog USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '{base}');"#)).unwrap();
        for sql in [
            "CREATE SCHEMA ordinary_private_namespace;",
            "CREATE TABLE ordinary_private_namespace.t (id INT);",
            "INSERT INTO ordinary_private_namespace.t VALUES (1);",
        ] {
            guard.before(sql).unwrap();
            guard.after(sql).unwrap();
        }
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        // An independently declared catalog does not inherit the private owner
        // merely because it shares the invocation, loopback host or URI prefix.
        guard.after(&format!(r#"CREATE CATALOG unrelated USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '{base}/unowned');"#)).unwrap();
        assert!(
            guard
                .before("CREATE SCHEMA unrelated.foreign_namespace;")
                .unwrap_err()
                .to_string()
                .contains("exact namespace")
        );
    }
    #[test]
    fn owned_proxy_alias_requires_an_exact_previously_bound_downstream() {
        let facts = CatalogFacts::default();
        assert!(
            facts
                .bind_owned_proxy_alias("http://127.0.0.1:38002", "http://127.0.0.1:38001")
                .is_err()
        );
        assert!(!facts.private_rest("http://127.0.0.1:38002").unwrap());
        facts
            .bind_origins(
                &["http://127.0.0.1:38001"],
                RestObjectStoreOwner::PrivateObjectStore,
            )
            .unwrap();
        facts
            .bind_owned_proxy_alias("http://127.0.0.1:38002", "http://127.0.0.1:38001/")
            .unwrap();
        assert!(facts.private_rest("http://127.0.0.1:38002").unwrap());
        facts
            .bind_origins(
                &["http://127.0.0.1:38004"],
                RestObjectStoreOwner::MachineScratch,
            )
            .unwrap();
        facts
            .bind_owned_proxy_alias("http://127.0.0.1:38005", "http://127.0.0.1:38004")
            .unwrap();
        assert!(!facts.private_rest("http://127.0.0.1:38005").unwrap());
        assert!(!facts.private_rest("http://127.0.0.1:38003").unwrap());
        assert!(!facts.private_rest("http://127.0.0.1:38002/other").unwrap());
        assert!(
            facts
                .bind_origins(
                    &["http://127.0.0.1:38002"],
                    RestObjectStoreOwner::MachineScratch
                )
                .is_err()
        );
        let mut guard = guard();
        guard.facts = facts;
        guard.after(r#"CREATE CATALOG catalog USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = 'http://127.0.0.1:38002');"#).unwrap();
        guard
            .before("CREATE SCHEMA private_through_owned_proxy;")
            .unwrap();
        guard
            .after("CREATE SCHEMA private_through_owned_proxy;")
            .unwrap();
        guard
            .after("CREATE TABLE private_through_owned_proxy.t (id INT);")
            .unwrap();
    }
    #[test]
    fn registered_shared_storage_still_validates_actual_table_scope_in_a_private_invocation() {
        let mut guard = guard();
        let namespace = guard.identity.resource_name("d0000").unwrap();
        let (base, thread) = server(vec![
            serde_json::json!({"metadata":{"location":"s3://foreign/table"}}),
        ]);
        guard
            .facts
            .bind_origins(
                &[&format!("{base}/private")],
                RestObjectStoreOwner::PrivateObjectStore,
            )
            .unwrap();
        guard
            .facts
            .bind_origins(&[&base], RestObjectStoreOwner::MachineScratch)
            .unwrap();
        guard.after(&format!(r#"CREATE CATALOG catalog USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '{base}');"#)).unwrap();
        assert!(guard.before("CREATE SCHEMA catalog.foreign;").is_err());
        let error = guard
            .after(&format!("CREATE TABLE {namespace}.t (id INT);"))
            .unwrap_err();
        assert!(error.to_string().contains("outside"), "{error:#}");
        assert_eq!(thread.join().unwrap().len(), 1);
    }
    #[test]
    fn private_storage_binding_does_not_bypass_original_work_or_cleanup_deadlines() {
        let mut guard = guard();
        guard
            .facts
            .bind_origins(
                &["http://127.0.0.1:38001"],
                RestObjectStoreOwner::PrivateObjectStore,
            )
            .unwrap();
        guard.after(r#"CREATE CATALOG catalog USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = 'http://127.0.0.1:38001');"#).unwrap();
        guard.deadline = Instant::now();
        assert!(
            guard
                .before("CREATE SCHEMA original_private_name;")
                .unwrap_err()
                .to_string()
                .contains("clock expired")
        );
        let cleanup = guard.for_cleanup(Instant::now() + Duration::from_secs(10));
        cleanup
            .before("DROP SCHEMA original_private_name;")
            .unwrap();
        assert!(
            cleanup
                .facts
                .private_rest("http://127.0.0.1:38001")
                .unwrap()
        );
        let expired_cleanup = guard.for_cleanup(Instant::now());
        assert!(
            expired_cleanup
                .before("DROP SCHEMA original_private_name;")
                .is_err()
        );
    }
    #[test]
    fn rest_catalog_aliases_follow_provider_uri_precedence() {
        for (properties, expected) in [
            (r#""uri" = 'http://127.0.0.1:1/'"#, "http://127.0.0.1:1"),
            (
                r#""iceberg.catalog.uri" = 'http://127.0.0.1:2'"#,
                "http://127.0.0.1:2",
            ),
            (
                r#""uri" = 'http://127.0.0.1:3', "iceberg.catalog.uri" = 'http://127.0.0.1:4'"#,
                "http://127.0.0.1:3",
            ),
        ] {
            let mut guard = guard();
            guard.after(&format!(
                r#"CREATE CATALOG catalog USING iceberg WITH ("iceberg.catalog.type" = 'REST', {properties});"#
            )).unwrap();
            assert!(
                matches!(guard.catalog_fact("catalog").unwrap(), Some(Catalog::Rest(uri)) if uri == expected)
            );
        }
        let mut guard = guard();
        assert!(guard.after(r#"CREATE CATALOG catalog USING iceberg WITH ("iceberg.catalog.type" = 'rest');"#).is_err());
    }
    #[test]
    fn non_iceberg_catalog_does_not_acquire_a_hadoop_write_guard() {
        let mut guard = guard();
        guard.after(r#"CREATE CATALOG catalog USING paimon WITH ("warehouse" = 's3://novarocks/ready');"#).unwrap();
        assert!(matches!(
            guard.catalog_fact("catalog").unwrap(),
            Some(Catalog::Other)
        ));
    }
    #[test]
    fn hadoop_catalog_aliases_follow_provider_default_and_warehouse_precedence() {
        for (properties, expected) in [
            (r#""warehouse" = 's3://novarocks/a'"#, "s3://novarocks/a"),
            (
                r#""iceberg.catalog.warehouse" = 's3://novarocks/b'"#,
                "s3://novarocks/b",
            ),
            (
                r#""iceberg.catalog.type" = 'HADOOP', "warehouse" = 's3://novarocks/c', "iceberg.catalog.warehouse" = 's3://novarocks/d'"#,
                "s3://novarocks/d",
            ),
        ] {
            let mut guard = guard();
            guard
                .after(&format!(
                    r#"CREATE CATALOG catalog USING iceberg WITH ({properties});"#
                ))
                .unwrap();
            assert!(
                matches!(guard.catalog_fact("catalog").unwrap(), Some(Catalog::Hadoop(root)) if root == expected)
            );
            assert!(guard.before("CREATE SCHEMA catalog.foreign").is_err());
        }
    }
    #[test]
    fn create_database_keeps_jdbc_native_layout_without_rest_property_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut guard = guard();
        guard.facts.0.lock().unwrap().insert(
            "catalog".into(),
            Catalog::Rest(format!("http://{}", listener.local_addr().unwrap())),
        );
        let namespace = guard.identity.resource_name("db").unwrap();
        guard
            .before(&format!("CREATE SCHEMA {namespace};"))
            .unwrap();
        guard.after(&format!("CREATE SCHEMA {namespace};")).unwrap();
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        for bad in [
            &guard.identity.run_id,
            &format!("{}_123", guard.identity.run_id),
            &format!("{}_a12345", guard.identity.run_id),
        ] {
            assert!(guard.before(&format!("CREATE SCHEMA {bad};")).is_err());
        }
    }

    #[test]
    fn ordinary_rename_explicit_and_ctas_locations_share_one_run_predicate() {
        for category in ["ordinary", "renamed", "explicit", "ctas", "view", "mv"] {
            let mut guard = guard();
            let namespace = guard.identity.resource_name("now").unwrap();
            let old = guard.identity.resource_name("old").unwrap();
            let location = match category {
                "ordinary" => format!("s3://warehouse/service/{namespace}/table"),
                "renamed" => format!("s3a://warehouse/service/{old}/table"),
                "view" | "mv" => format!("s3://warehouse/service-mv/{old}/view"),
                _ => format!("{}/{category}/staged/table", guard.identity.scope_root),
            };
            let (base, thread) =
                server(vec![serde_json::json!({"metadata":{"location":location}})]);
            guard
                .facts
                .0
                .lock()
                .unwrap()
                .insert("catalog".into(), Catalog::Rest(base));
            let sql = match category {
                "view" => format!("CREATE VIEW {namespace}.t AS SELECT 1"),
                "mv" => format!(
                    "CREATE MATERIALIZED VIEW {namespace}.t WITH (refresh_policy = 'MANUAL') AS SELECT 1 AS id"
                ),
                "ctas" => format!("CREATE TABLE {namespace}.t AS SELECT 1"),
                _ => format!("CREATE TABLE {namespace}.t (id INT)"),
            };
            let frames = crate::session::split_trino_sql_statements(&sql).unwrap();
            for frame in frames {
                parser::parse_statement(&frame).expect("test SQL is admitted parser input");
            }
            guard.after(&sql).unwrap();
            let requests = thread.join().unwrap();
            assert_eq!(requests.len(), 1);
            let collection = if category == "view" {
                "views"
            } else {
                "tables"
            };
            assert!(
                requests[0].starts_with(&format!("GET /v1/namespaces/{namespace}/{collection}/t "))
            );
        }
    }
    #[test]
    fn actual_table_location_outside_original_scope_is_a_failure() {
        let guard = guard();
        let namespace = guard.identity.resource_name("d0000").unwrap();
        let (base, thread) = server(vec![
            serde_json::json!({"metadata":{"location":"s3://novarocks/default/table"}}),
        ]);
        guard
            .facts
            .0
            .lock()
            .unwrap()
            .insert("catalog".into(), Catalog::Rest(base));
        let mut guard = guard;
        assert!(
            guard
                .after(&format!("CREATE TABLE {namespace}.t (id INT);"))
                .unwrap_err()
                .to_string()
                .contains("outside")
        );
        thread.join().unwrap();
    }
    #[test]
    fn readonly_warehouses_and_foreign_rest_namespaces_reject_mutation() {
        let guard = guard();
        guard.facts.0.lock().unwrap().insert(
            "catalog".into(),
            Catalog::Hadoop("s3://novarocks/benchmark".into()),
        );
        for sql in [
            "CREATE TABLE ns.t (id INT)",
            "INSERT INTO ns.t SELECT 1",
            "DROP SCHEMA ns",
            "DELETE FROM ns.t",
        ] {
            assert!(guard.before(sql).is_err(), "{sql}");
        }
        guard
            .facts
            .0
            .lock()
            .unwrap()
            .insert("catalog".into(), Catalog::Rest("http://127.0.0.1:1".into()));
        assert!(guard.before("CREATE SCHEMA readonly_namespace").is_err());
        assert!(guard.before("SELECT * FROM readonly_namespace.t").is_ok());
    }
    #[test]
    fn qualified_branch_writes_require_the_original_rest_namespace() {
        let guard = guard();
        let namespace = guard.identity.resource_name("d0000").unwrap();
        guard
            .facts
            .0
            .lock()
            .unwrap()
            .insert("catalog".into(), Catalog::Rest("http://127.0.0.1:1".into()));
        for reference in ["dev", "release"] {
            assert!(
                guard
                    .before(&format!(
                        "INSERT INTO catalog.foreign.t@{reference} VALUES (1)"
                    ))
                    .unwrap_err()
                    .to_string()
                    .contains("exact namespace")
            );
            assert!(
                guard
                    .before(&format!(
                        "INSERT INTO catalog.{namespace}.t@{reference} VALUES (1)"
                    ))
                    .is_ok()
            );
        }
        for reference in ["unknown", "branch_", "tag_"] {
            assert!(
                guard
                    .before(&format!(
                        "INSERT INTO catalog.foreign.t.{reference} VALUES (1)"
                    ))
                    .is_err()
            );
        }
    }
    #[test]
    fn expired_clock_rejects_before_rest_network_access() {
        let mut guard = guard();
        let namespace = guard.identity.resource_name("d0000").unwrap();
        guard
            .facts
            .0
            .lock()
            .unwrap()
            .insert("catalog".into(), Catalog::Rest("http://127.0.0.1:1".into()));
        guard.deadline = Instant::now();
        assert!(guard.before(&format!("CREATE SCHEMA {namespace}")).is_err());
        assert!(
            guard
                .remaining()
                .unwrap_err()
                .to_string()
                .contains("expired")
        );
    }
}
