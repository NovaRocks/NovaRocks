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
use crate::types::ConnectionConfig;
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::fixture_run::{RunStorageScope, ScratchIdentity, canonical_s3};
use novarocks_cluster_harness::isolated_iceberg_rest::{RestObjectStoreOwner, RestStorageBinding};
use novarocks_parser::ast::{
    CatalogStatement, DmlStatement, IcebergStatement, LiteralKind, MaintenanceStatement,
    MaterializedViewStatement, ObjectName, SessionStatement, SetTarget, SetValue, SetWord,
    Statement, TableStatement, ViewStatement,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Debug)]
enum Catalog {
    Rest(String),
    Hadoop(String),
    Other,
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
    fn private_rest(&self, origin: &str) -> Result<bool> {
        Ok(self
            .1
            .lock()
            .map_err(|_| anyhow::anyhow!("SQL REST owner facts lock poisoned"))?
            .get(origin.trim_end_matches('/'))
            == Some(&RestObjectStoreOwner::PrivateObjectStore))
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
    pub fn connection_context(&self, base: &ConnectionConfig) -> ConnectionConfig {
        ConnectionConfig {
            catalog: self.catalog.clone(),
            db: self.namespace.clone(),
            ..base.clone()
        }
    }
    pub fn set_database(&mut self, value: &str) {
        self.namespace = Some(value.to_string());
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
        Ok(match (database, parts.as_slice()) {
            (true, [catalog, namespace]) => Some((catalog.clone(), namespace.clone(), None)),
            (true, [namespace]) => self
                .catalog
                .clone()
                .map(|catalog| (catalog, namespace.clone(), None)),
            (false, [catalog, namespace, table]) => {
                Some((catalog.clone(), namespace.clone(), Some(table.clone())))
            }
            (false, [catalog, namespace, table, reference])
                if reference
                    .strip_prefix("branch_")
                    .or_else(|| reference.strip_prefix("tag_"))
                    .is_some_and(|name| !name.is_empty()) =>
            {
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
    pub fn before(&self, sql: &str) -> Result<()> {
        ensure!(
            Instant::now() < self.deadline,
            "original SQL namespace clock expired"
        );
        let Ok(statements) = novarocks_parser::parse(sql) else {
            return Ok(());
        };
        for statement in statements {
            match statement {
                Statement::Catalog(CatalogStatement::CreateDatabase(value)) => {
                    self.ensure_write(&value.name, true)?
                }
                Statement::Table(TableStatement::Create(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::Catalog(CatalogStatement::DropDatabase(value)) => {
                    self.ensure_write(&value.name, true)?
                }
                Statement::Catalog(CatalogStatement::DropTable(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::Catalog(CatalogStatement::TruncateTable(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::Dml(DmlStatement::Delete(value)) => {
                    self.ensure_write(&value.target, false)?
                }
                Statement::Dml(DmlStatement::Update(value)) => {
                    self.ensure_write(&value.target, false)?
                }
                Statement::Dml(DmlStatement::Merge(value)) => {
                    self.ensure_write(&value.target, false)?
                }
                Statement::Dml(DmlStatement::AddEqualityDelete(value)) => {
                    self.ensure_write(&value.target, false)?
                }
                Statement::Dml(DmlStatement::Insert(value)) => {
                    self.ensure_write(&value.target, false)?
                }
                Statement::Dml(DmlStatement::CreateTableAsSelect(value)) => {
                    let TableStatement::Create(table) = value.table;
                    self.ensure_write(&table.name, false)?;
                }
                Statement::Iceberg(IcebergStatement::AlterTable(value)) => {
                    self.ensure_write(&value.table, false)?
                }
                Statement::View(ViewStatement::Create(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::View(ViewStatement::Drop(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::MaterializedView(MaterializedViewStatement::Create(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::MaterializedView(MaterializedViewStatement::Drop(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::MaterializedView(MaterializedViewStatement::Alter(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::MaterializedView(MaterializedViewStatement::Refresh(value)) => {
                    self.ensure_write(&value.name, false)?
                }
                Statement::Maintenance(MaintenanceStatement::Optimize(value)) => {
                    self.ensure_write(&value.table, false)?
                }
                Statement::Maintenance(MaintenanceStatement::RewriteManifests(value)) => {
                    self.ensure_write(&value.table, false)?
                }
                Statement::Maintenance(MaintenanceStatement::ExpireSnapshots(value)) => {
                    self.ensure_write(&value.table, false)?
                }
                Statement::Maintenance(MaintenanceStatement::RemoveOrphanFiles(value)) => {
                    self.ensure_write(&value.table, false)?
                }
                _ => {}
            }
        }
        Ok(())
    }
    pub fn after(&mut self, sql: &str) -> Result<()> {
        let Ok(statements) = novarocks_parser::parse(sql) else {
            return Ok(());
        };
        for statement in statements {
            match statement {
                Statement::Catalog(CatalogStatement::CreateCatalog(value)) => {
                    let properties = value
                        .properties
                        .iter()
                        .filter_map(|p| match (&p.key.kind, &p.value.kind) {
                            (LiteralKind::String(k), LiteralKind::String(v)) => {
                                Some((k.to_ascii_lowercase(), v.clone()))
                            }
                            _ => None,
                        })
                        .collect::<BTreeMap<_, _>>();
                    // Mirror the provider's accepted aliases and precedence. This runs
                    // after successful SQL; it must project the same catalog facts.
                    let kind = properties.get("iceberg.catalog.type");
                    let provider = properties.get("type");
                    let fact =
                        if provider.is_some_and(|value| !value.eq_ignore_ascii_case("iceberg")) {
                            Catalog::Other
                        } else {
                            match kind.map(|value| value.to_ascii_lowercase()).as_deref() {
                                Some("rest") => Catalog::Rest(
                                    properties
                                        .get("uri")
                                        .or_else(|| properties.get("iceberg.catalog.uri"))
                                        .context("REST catalog omitted exact URI")?
                                        .trim_end_matches('/')
                                        .to_string(),
                                ),
                                None | Some("hadoop") => Catalog::Hadoop(
                                    properties
                                        .get("iceberg.catalog.warehouse")
                                        .or_else(|| properties.get("warehouse"))
                                        .context("Hadoop catalog omitted exact warehouse")?
                                        .clone(),
                                ),
                                _ => Catalog::Other,
                            }
                        };
                    self.facts
                        .0
                        .lock()
                        .map_err(|_| anyhow::anyhow!("SQL catalog facts lock poisoned"))?
                        .insert(value.name.value, fact);
                }
                Statement::Table(TableStatement::Create(value)) => {
                    self.verify_object(&value.name, "tables")?
                }
                Statement::Dml(DmlStatement::CreateTableAsSelect(value)) => {
                    let TableStatement::Create(table) = value.table;
                    self.verify_object(&table.name, "tables")?;
                }
                Statement::View(ViewStatement::Create(value)) => {
                    self.verify_object(&value.name, "views")?
                }
                Statement::MaterializedView(MaterializedViewStatement::Create(value)) => {
                    // MVs are document-managed Iceberg tables published through
                    // the existing staged-table provider path.
                    self.verify_object(&value.name, "tables")?
                }
                Statement::Session(SessionStatement::Use(value)) => {
                    if let Some(catalog) = value.catalog {
                        self.catalog = Some(catalog.value);
                    }
                    self.namespace = Some(value.database.value);
                }
                Statement::Session(SessionStatement::Set(value)) => {
                    for assignment in value.assignments {
                        if matches!(assignment.target, SetTarget::Catalog { .. }) {
                            self.catalog = match assignment.value {
                                SetValue::Words(words) if words.len() == 1 => match &words[0] {
                                    SetWord::Ident(v) => Some(v.value.clone()),
                                    SetWord::Literal(v) => match &v.kind {
                                        LiteralKind::String(v) => Some(v.clone()),
                                        _ => None,
                                    },
                                },
                                _ => None,
                            };
                        }
                    }
                }
                _ => {}
            }
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
    fn verify_object(&self, name: &ObjectName, collection: &str) -> Result<()> {
        let Some((catalog, namespace, Some(table))) = self.target(name, false)? else {
            return Ok(());
        };
        if let Some(Catalog::Rest(base)) = self.catalog_fact(&catalog)? {
            if self.facts.private_rest(&base)? {
                return Ok(());
            }
            ensure!(
                self.owned(&namespace),
                "REST table validation refused a foreign namespace"
            );
            let mut url = Self::namespace_url(&base, &namespace)?;
            url.path_segments_mut()
                .unwrap()
                .push(collection)
                .push(&table);
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
        }
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
                mysql: "mysql".into(),
                host: "127.0.0.1".into(),
                port: "0".into(),
                user: "root".into(),
                password: None,
                catalog: Some("catalog".into()),
                db: None,
            },
            Instant::now() + Duration::from_secs(10),
        )
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
        guard.after(&format!(r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="rest", "uri"="{base}");"#)).unwrap();
        for sql in [
            "CREATE DATABASE ordinary_private_namespace;",
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
        guard.after(&format!(r#"CREATE EXTERNAL CATALOG unrelated PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="rest", "uri"="{base}/unowned");"#)).unwrap();
        assert!(
            guard
                .before("CREATE DATABASE unrelated.foreign_namespace;")
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
        guard.after(r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="rest", "uri"="http://127.0.0.1:38002");"#).unwrap();
        guard
            .before("CREATE DATABASE private_through_owned_proxy;")
            .unwrap();
        guard
            .after("CREATE DATABASE private_through_owned_proxy;")
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
        guard.after(&format!(r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="rest", "uri"="{base}");"#)).unwrap();
        assert!(guard.before("CREATE DATABASE catalog.foreign;").is_err());
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
        guard.after(r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="rest", "uri"="http://127.0.0.1:38001");"#).unwrap();
        guard.deadline = Instant::now();
        assert!(
            guard
                .before("CREATE DATABASE original_private_name;")
                .unwrap_err()
                .to_string()
                .contains("clock expired")
        );
        let cleanup = guard.for_cleanup(Instant::now() + Duration::from_secs(10));
        cleanup
            .before("DROP DATABASE original_private_name;")
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
                .before("DROP DATABASE original_private_name;")
                .is_err()
        );
    }
    #[test]
    fn rest_catalog_aliases_follow_provider_uri_precedence() {
        for (properties, expected) in [
            (r#""uri"="http://127.0.0.1:1/""#, "http://127.0.0.1:1"),
            (
                r#""iceberg.catalog.uri"="http://127.0.0.1:2""#,
                "http://127.0.0.1:2",
            ),
            (
                r#""uri"="http://127.0.0.1:3", "iceberg.catalog.uri"="http://127.0.0.1:4""#,
                "http://127.0.0.1:3",
            ),
        ] {
            let mut guard = guard();
            guard.after(&format!(
                r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="REST", {properties});"#
            )).unwrap();
            assert!(
                matches!(guard.catalog_fact("catalog").unwrap(), Some(Catalog::Rest(uri)) if uri == expected)
            );
        }
        let mut guard = guard();
        assert!(guard.after(r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="rest");"#).is_err());
    }
    #[test]
    fn non_iceberg_catalog_does_not_acquire_a_hadoop_write_guard() {
        let mut guard = guard();
        guard.after(r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="paimon", "warehouse"="s3://novarocks/ready");"#).unwrap();
        assert!(matches!(
            guard.catalog_fact("catalog").unwrap(),
            Some(Catalog::Other)
        ));
    }
    #[test]
    fn hadoop_catalog_aliases_follow_provider_default_and_warehouse_precedence() {
        for (properties, expected) in [
            (r#""warehouse"="s3://novarocks/a""#, "s3://novarocks/a"),
            (
                r#""iceberg.catalog.warehouse"="s3://novarocks/b""#,
                "s3://novarocks/b",
            ),
            (
                r#""iceberg.catalog.type"="HADOOP", "warehouse"="s3://novarocks/c", "iceberg.catalog.warehouse"="s3://novarocks/d""#,
                "s3://novarocks/d",
            ),
        ] {
            let mut guard = guard();
            guard.after(&format!(r#"CREATE EXTERNAL CATALOG catalog PROPERTIES ("type"="iceberg", {properties});"#)).unwrap();
            assert!(
                matches!(guard.catalog_fact("catalog").unwrap(), Some(Catalog::Hadoop(root)) if root == expected)
            );
            assert!(guard.before("CREATE DATABASE catalog.foreign").is_err());
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
            .before(&format!("CREATE DATABASE {namespace};"))
            .unwrap();
        guard
            .after(&format!("CREATE DATABASE {namespace};"))
            .unwrap();
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        for bad in [
            &guard.identity.run_id,
            &format!("{}_123", guard.identity.run_id),
            &format!("{}_a12345", guard.identity.run_id),
        ] {
            assert!(guard.before(&format!("CREATE DATABASE {bad};")).is_err());
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
                    "CREATE MATERIALIZED VIEW {namespace}.t DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH MANUAL AS SELECT 1 AS id"
                ),
                "ctas" => format!("CREATE TABLE {namespace}.t AS SELECT 1"),
                _ => format!("CREATE TABLE {namespace}.t (id INT)"),
            };
            novarocks_parser::parse(&sql).expect("test SQL is admitted parser input");
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
            "DROP DATABASE ns",
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
        assert!(guard.before("CREATE DATABASE readonly_namespace").is_err());
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
        for reference in ["branch_dev", "tag_release"] {
            assert!(
                guard
                    .before(&format!(
                        "INSERT INTO catalog.foreign.t.{reference} VALUES (1)"
                    ))
                    .unwrap_err()
                    .to_string()
                    .contains("exact namespace")
            );
            assert!(
                guard
                    .before(&format!(
                        "INSERT INTO catalog.{namespace}.t.{reference} VALUES (1)"
                    ))
                    .is_ok()
            );
        }
        for reference in ["unknown", "branch_", "tag_"] {
            let statements = novarocks_parser::parse(&format!(
                "INSERT INTO catalog.foreign.t.{reference} VALUES (1)"
            ))
            .unwrap();
            let Statement::Dml(DmlStatement::Insert(value)) = &statements[0] else {
                panic!("expected INSERT");
            };
            assert!(guard.target(&value.target, false).unwrap().is_none());
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
        assert!(
            guard
                .before(&format!("CREATE DATABASE {namespace}"))
                .is_err()
        );
        assert!(
            guard
                .remaining()
                .unwrap_err()
                .to_string()
                .contains("expired")
        );
    }
}
