# Iceberg REST Catalog View 支持 — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 standalone 模式下为 REST 类型 iceberg catalog 实现 view 元数据 CRUD(CREATE [OR REPLACE] / DROP / SHOW CREATE VIEW / SHOW VIEWS)与 SELECT 内联展开;并删除 `{version}-{uuid}.metadata.json` 回退读取兼容逻辑。

**Architecture:** vendored `iceberg` crate 的 `Catalog` trait 增加默认返回 FeatureUnsupported 的 view 方法;`iceberg-catalog-rest` 0.9.0 vendor 进仓库并实现 6 个 REST view endpoint(复用其 HttpClient 的认证/prefix);NovaRocks 侧依次为 registry 包装层(`views.rs`)、`CatalogBackend` trait、engine 路由(`statistics.rs` 前缀拦截 → `iceberg_view.rs`)与 SELECT 预分析展开(`iceberg_view_rewrite.rs`)。Spec: `docs/design/specs/2026-06-10-iceberg-rest-view-design.md`。

**Tech Stack:** Rust、iceberg-rust 0.9.0(vendored)、sqlparser 0.61、mockito(单测)、sql-test-runner + docker/iceberg-rest fixture(e2e)。

---

## 实施前置须知(零上下文必读)

- 仓库根:本 worktree(`git rev-parse --show-toplevel`)。所有路径相对仓库根。
- 错误处理惯例:这些路径全部 `Result<_, String>`,第三方错误在边界 `.map_err(|e| format!(...))`。
- 语言规范:代码注释/日志/错误消息/commit message 用英文;commit 不加 Co-Authored-By。
- `block_on_iceberg(async { ... })` 返回 `Result<F::Output, String>`(外层 runtime 错误);当 future 自身返回 `Result` 时用 `??` 或两层 `.map_err`(外层 `"... runtime failed: {e}"`,内层 `"<op> <ident>: {e}"`)。
- 单测中调用含 `block_on_iceberg` 的同步函数必须包 `tokio::task::spawn_blocking`(见 `registry.rs:3434` 现有测试)。
- e2e 测试环境:`source docker/iceberg-rest/runtime/current/env.sh`;若缺失先 `docker/iceberg-rest/up.sh --prepare-only`。standalone-server 后台启动必须等 `NOVAROCKS_READY` 标记(脚本见 CLAUDE.md §7.3)。
- 编译验证统一用 `cargo build --profile dev-opt`(后续 e2e 直接复用产物 `target/dev-opt/novarocks`);纯单测步骤用 `cargo test --lib <filter>`。

### 文件全景

| 动作 | 文件 | 职责 |
| --- | --- | --- |
| 改 | `src/connector/iceberg/catalog/registry.rs` | Task 1 删回退;Task 5 个别 `fn` 提升 `pub(crate)` |
| 增 | `vendor/iceberg-catalog-rest-0.9.0/`(整 crate) | Task 2 vendor;Task 4 实现 view endpoint |
| 改 | `vendor/iceberg-0.9.0/src/catalog/mod.rs` | Task 3:trait view 方法、ViewRequirement、ViewCommit、ViewCreation.location 改 Option |
| 改 | `vendor/iceberg-0.9.0/src/spec/view_version.rs` | Task 3:`ViewRepresentations::new` |
| 改 | `vendor/iceberg-0.9.0/src/spec/view_metadata_builder.rs` | Task 3:from_view_creation 适配 Option location |
| 改 | `vendor/iceberg-0.9.0/PATCH.md` | Task 3:记录 patch |
| 增 | `src/connector/iceberg/catalog/views.rs` | Task 5:registry 层 view CRUD + mockito 测试 |
| 改 | `src/connector/iceberg/catalog/mod.rs` | Task 5:`pub(crate) mod views;` |
| 改 | `src/connector/backend.rs` | Task 6:trait view 方法 + CreateViewRequest/ResolvedView |
| 改 | `src/connector/iceberg/catalog/backend.rs` | Task 6:iceberg 实现 |
| 增 | `src/engine/iceberg_view.rs` | Task 7 目标解析 + Task 8 create/Task 9 drop 流程 |
| 增 | `src/engine/iceberg_view_rewrite.rs` | Task 7:SELECT 展开 + 裸名限定 + 循环检测 |
| 改 | `src/engine/mod.rs` | Task 7 wiring;Task 10 SHOW handler |
| 改 | `src/engine/statistics.rs` | Task 8/9:CREATE/DROP VIEW 前缀路由 |
| 改 | `src/engine/statement.rs` | Task 9 DROP TABLE 严格检查;Task 10 SHOW 探测/解析 |
| 改 | `src/server/mod.rs` | Task 10:SHOW 语句出 noop 门 |
| 改 | `docker/iceberg-rest/compose.yml` | Task 11:JdbcCatalog view 开关 |
| 增 | `sql-tests/iceberg-rest/sql/iceberg_rest_view_{ddl,select,show}.sql` + result | Task 11 |
| 增 | `sql-tests/iceberg-compatibility/sql/{spark_rest_view_read,novarocks_view_spark_read}.sql` + result | Task 12 |

### 任务依赖

Task 1 独立(先行合入)。Task 2→3→4→5→6 严格串行(crate 分层)。Task 7(展开)在 6 后;Task 8(CREATE)依赖 7(创建时要先展开 view-on-view 再分析);Task 9、10 在 8 后;Task 11、12 依赖全部;Task 13 收尾。

---

### Task 1: 删除 `{version}-{uuid}.metadata.json` 回退读取(工作项 A,独立 commit)

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs:1706-1796`(注释、`choose_latest_metadata_filename`、删除 `parse_internal_metadata_version`)
- Test: 同文件新增 `#[cfg(test)] mod metadata_filename_tests`

- [ ] **Step 1: 写失败测试**

在 `registry.rs` 中 `parse_hadoop_metadata_version` 函数之后插入:

```rust
#[cfg(test)]
mod metadata_filename_tests {
    use super::choose_latest_metadata_filename;

    #[test]
    fn picks_highest_hadoop_version() {
        let files = vec![
            "v1.metadata.json".to_string(),
            "v10.metadata.json".to_string(),
            "v2.metadata.json".to_string(),
        ];
        assert_eq!(
            choose_latest_metadata_filename(&files).unwrap(),
            "v10.metadata.json"
        );
    }

    #[test]
    fn ignores_internal_uuid_format() {
        let files = vec![
            "00009-9a8b7c6d-1111-2222-3333-444455556666.metadata.json".to_string(),
            "v3.metadata.json".to_string(),
        ];
        assert_eq!(
            choose_latest_metadata_filename(&files).unwrap(),
            "v3.metadata.json"
        );
    }

    #[test]
    fn internal_format_only_is_an_error() {
        // Pre-deletion the {version}-{uuid} fallback would return Ok here.
        // NovaRocks has no historical users; the internal naming was never a
        // supported on-disk layout, so it must not be recognized.
        let files =
            vec!["00001-9a8b7c6d-1111-2222-3333-444455556666.metadata.json".to_string()];
        assert!(choose_latest_metadata_filename(&files).is_err());
    }
}
```

- [ ] **Step 2: 跑测试确认第三个失败**

Run: `cargo test --lib connector::iceberg::catalog::registry::metadata_filename_tests`
Expected: `internal_format_only_is_an_error` FAIL(当前回退分支返回 Ok),前两个 PASS。

- [ ] **Step 3: 实现删除**

1. 删除整个 `parse_internal_metadata_version` 函数(registry.rs:1747-1755)。
2. `choose_latest_metadata_filename`(registry.rs:1771)替换为:

```rust
/// Choose the latest metadata file from a list of file names found in the
/// metadata directory. Only the Hadoop-catalog naming convention
/// (`v{N}.metadata.json`) is recognized — it is the only layout NovaRocks
/// writes and the only one StarRocks FE / Spark / Trino can discover via
/// `version-hint.text`.
fn choose_latest_metadata_filename(file_names: &[String]) -> Result<String, String> {
    let mut hadoop: Vec<(i32, &str)> = file_names
        .iter()
        .filter_map(|name| parse_hadoop_metadata_version(name).map(|v| (v, name.as_str())))
        .collect();
    if hadoop.is_empty() {
        return Err("no iceberg metadata files found".to_string());
    }
    hadoop.sort_by_key(|(v, _)| *v);
    Ok(hadoop.last().unwrap().1.to_string())
}
```

3. 更新 `latest_table_metadata_file_s3` 内注释(registry.rs:1706-1708),改为:

```rust
    // Find the latest metadata JSON in the Hadoop-catalog naming convention
    // (`vN.metadata.json`) — the only layout NovaRocks writes.
```

4. `parse_hadoop_metadata_version` 内的注释 `// Must be purely numeric (no dash, no UUID suffix) to distinguish from internal format.` 改为 `// Must be purely numeric — reject names like "v1-foo.metadata.json".`(dash 守卫保留)。

- [ ] **Step 4: 跑测试确认全绿**

Run: `cargo test --lib connector::iceberg::catalog::registry`
Expected: PASS(含既有 registry 测试)。

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/registry.rs
git commit -m "refactor(iceberg): drop {version}-{uuid}.metadata.json fallback read path

NovaRocks has no historical users and the internal naming was never a
supported on-disk layout for Hadoop-catalog tables. vN.metadata.json is
now the only recognized metadata file name; directories containing only
the internal format surface as 'unknown table'."
```

---

### Task 2: vendor `iceberg-catalog-rest-0.9.0`

**Files:**
- Create: `vendor/iceberg-catalog-rest-0.9.0/`(从 cargo registry 缓存拷贝)
- Modify: `Cargo.toml`(`[patch.crates-io]`)

- [ ] **Step 1: 拷贝 crate 源码**

```bash
SRC=$(echo ~/.cargo/registry/src/*/iceberg-catalog-rest-0.9.0)
cp -R "$SRC" vendor/iceberg-catalog-rest-0.9.0
chmod -R u+w vendor/iceberg-catalog-rest-0.9.0
rm -f vendor/iceberg-catalog-rest-0.9.0/.cargo-checksum.json \
      vendor/iceberg-catalog-rest-0.9.0/Cargo.toml.orig \
      vendor/iceberg-catalog-rest-0.9.0/.cargo_vcs_info.json
```

注意:registry 缓存文件是只读的,`chmod -R u+w` 必须执行。

- [ ] **Step 2: 接入 patch**

`Cargo.toml` 的 `[patch.crates-io]` 段(约 :742)追加一行,并把上方注释扩为两个 crate 的说明:

```toml
[patch.crates-io]
iceberg = { path = "vendor/iceberg-0.9.0" }
# Vendored to add Iceberg view REST endpoints (create/load/commit/drop/
# exists/list) that upstream 0.9.0 does not implement. See
# vendor/iceberg-catalog-rest-0.9.0/PATCH.md.
iceberg-catalog-rest = { path = "vendor/iceberg-catalog-rest-0.9.0" }
```

- [ ] **Step 3: 验证编译不变**

Run: `cargo build --profile dev-opt 2>&1 | tail -5`
Expected: 编译成功;`grep -A2 'name = "iceberg-catalog-rest"' Cargo.lock` 不再有 `source = "registry+..."` 行。

- [ ] **Step 4: Commit**

```bash
git add vendor/iceberg-catalog-rest-0.9.0 Cargo.toml Cargo.lock
git commit -m "build: vendor iceberg-catalog-rest 0.9.0 for view endpoint support

Verbatim copy from crates.io; no source changes yet. View endpoints land
in a follow-up commit."
```

---

### Task 3: vendored `iceberg` crate 的 view API 表面

**Files:**
- Modify: `vendor/iceberg-0.9.0/src/catalog/mod.rs`(Catalog trait、ViewCreation、ViewRequirement、ViewCommit)
- Modify: `vendor/iceberg-0.9.0/src/spec/view_version.rs`(`ViewRepresentations::new`)
- Modify: `vendor/iceberg-0.9.0/src/spec/view_metadata_builder.rs`(`from_view_creation` 适配)
- Modify: `vendor/iceberg-0.9.0/PATCH.md`

- [ ] **Step 1: `ViewCreation.location` 改为 `Option<String>`**

`catalog/mod.rs` 约 :931(先 `grep -rn "ViewCreation" vendor/iceberg-0.9.0/src/` 确认全部用点,应只有定义处与 `view_metadata_builder.rs:115`):

```rust
    /// The view's base location; used to create metadata file locations.
    /// `None` lets a server-side catalog (e.g. REST) assign the location.
    #[builder(default)]
    pub location: Option<String>,
```

`view_metadata_builder.rs` 的 `from_view_creation`(:115)解构后加:

```rust
        let location = location.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "ViewCreation.location is required for filesystem-backed catalogs",
            )
        })?;
```

(`Error`/`ErrorKind` 若未导入则补 `use crate::{Error, ErrorKind};`。)

- [ ] **Step 2: `ViewRepresentations` 公开构造器**

`spec/view_version.rs` 的 `impl ViewRepresentations`(约 :165,含 len/is_empty/iter 的那个 impl 块)内追加:

```rust
    /// Create a list of view representations.
    pub fn new(representations: Vec<ViewRepresentation>) -> Self {
        Self(representations)
    }
```

- [ ] **Step 3: 新增 `ViewRequirement` 与 `ViewCommit`**

`catalog/mod.rs` 在 `ViewUpdate` enum 之后追加(`take` 已由 TableCommit 引入 `std::mem::take`,确认 import 存在):

```rust
/// ViewRequirement represents a validation the catalog must perform before
/// applying a view commit.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ViewRequirement {
    /// The view UUID must match the requirement's `uuid`.
    #[serde(rename = "assert-view-uuid")]
    UuidMatch {
        /// Uuid of the view to assert.
        uuid: Uuid,
    },
}

/// ViewCommit represents a commit of view updates to the catalog.
#[derive(Debug, TypedBuilder)]
#[builder(build_method(vis = "pub"))]
pub struct ViewCommit {
    ident: TableIdent,
    requirements: Vec<ViewRequirement>,
    updates: Vec<ViewUpdate>,
}

impl ViewCommit {
    /// The identifier of the view to update.
    pub fn identifier(&self) -> &TableIdent {
        &self.ident
    }

    /// Take the requirements out of the commit.
    pub fn take_requirements(&mut self) -> Vec<ViewRequirement> {
        take(&mut self.requirements)
    }

    /// Take the updates out of the commit.
    pub fn take_updates(&mut self) -> Vec<ViewUpdate> {
        take(&mut self.updates)
    }
}
```

- [ ] **Step 4: Catalog trait 增加默认 view 方法**

`catalog/mod.rs` 的 `pub trait Catalog`(:50-112)末尾、`update_table` 之后追加(顶部 import 需补 `ViewMetadata`:`use crate::spec::ViewMetadata;` 或并入既有 spec import):

```rust
    /// Create a new view inside the namespace. Only catalogs with view
    /// support (e.g. REST) override the default.
    async fn create_view(
        &self,
        _namespace: &NamespaceIdent,
        _creation: ViewCreation,
    ) -> Result<ViewMetadata> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "create_view is not supported by this catalog",
        ))
    }

    /// Load a view's metadata from the catalog.
    async fn load_view(&self, _view: &TableIdent) -> Result<ViewMetadata> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "load_view is not supported by this catalog",
        ))
    }

    /// Commit updates to an existing view.
    async fn update_view(&self, _commit: ViewCommit) -> Result<ViewMetadata> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "update_view is not supported by this catalog",
        ))
    }

    /// Drop a view from the catalog.
    async fn drop_view(&self, _view: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "drop_view is not supported by this catalog",
        ))
    }

    /// Check if a view exists in the catalog.
    async fn view_exists(&self, _view: &TableIdent) -> Result<bool> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "view_exists is not supported by this catalog",
        ))
    }

    /// List views in the namespace.
    async fn list_views(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "list_views is not supported by this catalog",
        ))
    }
```

注意 trait 有 `#[cfg_attr(test, automock)]`——mockall 支持带默认体的方法,若 vendored crate 自身测试编译报错,按报错补 mock 期望即可(NovaRocks 构建不编译该 crate 的 test target,不受影响)。

- [ ] **Step 5: 更新 PATCH.md**

`vendor/iceberg-0.9.0/PATCH.md` 追加一节:

```markdown
## View API surface (NovaRocks)

- `Catalog` trait: added default-erroring view methods (`create_view`,
  `load_view`, `update_view`, `drop_view`, `view_exists`, `list_views`).
- Added `ViewRequirement` (`assert-view-uuid`) and `ViewCommit` (public
  builder, mirrors `TableCommit`).
- `ViewCreation.location` is now `Option<String>` so REST servers can
  assign the location; `ViewMetadataBuilder::from_view_creation` errors
  on `None`.
- `ViewRepresentations::new` is public so downstream crates can build
  representation lists.
```

- [ ] **Step 6: 编译验证**

Run: `cargo build --profile dev-opt 2>&1 | tail -5`
Expected: 编译成功(rest crate 来自 vendor,其对 iceberg 的依赖被 patch 到同一份,新 trait 默认方法不破坏现有实现)。

- [ ] **Step 7: Commit**

```bash
git add vendor/iceberg-0.9.0 Cargo.lock
git commit -m "feat(vendor/iceberg): add view API surface to Catalog trait

Default-erroring create/load/update/drop/exists/list view methods,
ViewRequirement (assert-view-uuid), ViewCommit, optional
ViewCreation.location and a public ViewRepresentations constructor.
Documented in PATCH.md."
```

---

### Task 4: REST crate 实现 view endpoint

**Files:**
- Modify: `vendor/iceberg-catalog-rest-0.9.0/src/types.rs`
- Modify: `vendor/iceberg-catalog-rest-0.9.0/src/catalog.rs`
- Create: `vendor/iceberg-catalog-rest-0.9.0/PATCH.md`

- [ ] **Step 1: types.rs 增加 view 报文类型**

参照 `CreateTableRequest`/`LoadTableResult`/`CommitTableRequest`(types.rs:203-300)的 serde 模式追加(import 补 `ViewMetadata`、`ViewVersion`、`ViewRequirement`、`ViewUpdate`,与既有 `iceberg::spec::...` import 合并):

```rust
/// Request body for `POST .../namespaces/{ns}/views`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct CreateViewRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    pub schema: Schema,
    pub view_version: ViewVersion,
    pub properties: HashMap<String, String>,
}

/// Response body shared by view create / load / commit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct LoadViewResult {
    pub metadata_location: String,
    pub metadata: ViewMetadata,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub config: HashMap<String, String>,
}

/// Request body for `POST .../namespaces/{ns}/views/{view}` (view commit).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CommitViewRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,
    pub requirements: Vec<ViewRequirement>,
    pub updates: Vec<ViewUpdate>,
}

/// `GET .../namespaces/{ns}/views` shares the list-tables wire shape.
pub type ListViewsResponse = ListTablesResponse;
```

- [ ] **Step 2: catalog.rs 增加 endpoint builder**

`RestCatalogConfig` 的 endpoint 函数区(catalog.rs:161-208)追加:

```rust
    fn views_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "views"])
    }

    fn view_endpoint(&self, view: &TableIdent) -> String {
        self.url_prefixed(&[
            "namespaces",
            &view.namespace.to_url_string(),
            "views",
            &view.name,
        ])
    }
```

- [ ] **Step 3: catalog.rs 在 `impl Catalog for RestCatalog` 中实现 6 个方法**

紧随 `update_table` 实现之后。import 补:`iceberg::{ViewCommit, ViewCreation, ViewRequirement}`、`iceberg::spec::{ViewMetadata, ViewVersion}`、types 的 `CommitViewRequest, CreateViewRequest, ListViewsResponse, LoadViewResult`。状态码匹配/错误转换完全镜像表实现(create_table :696-771、load_table :778-827、update_table :973-1043、table_exists、list_tables):

```rust
    async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        creation: ViewCreation,
    ) -> Result<ViewMetadata> {
        let context = self.context().await?;

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, format!("system clock before epoch: {e}"))
            })?
            .as_millis() as i64;
        let view_version = ViewVersion::builder()
            .with_version_id(1)
            .with_schema_id(creation.schema.schema_id())
            .with_timestamp_ms(timestamp_ms)
            .with_summary(creation.summary)
            .with_representations(creation.representations)
            .with_default_catalog(creation.default_catalog)
            .with_default_namespace(creation.default_namespace)
            .build();

        let request = context
            .client
            .request(Method::POST, context.config.views_endpoint(namespace))
            .json(&CreateViewRequest {
                name: creation.name,
                location: creation.location,
                schema: creation.schema,
                view_version,
                properties: creation.properties,
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;
        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<LoadViewResult>(http_response).await?;
                Ok(response.metadata)
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::NamespaceNotFound,
                "Tried to create a view under a namespace that does not exist",
            )),
            StatusCode::CONFLICT => {
                Err(Error::new(ErrorKind::Unexpected, "The view already exists"))
            }
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn load_view(&self, view: &TableIdent) -> Result<ViewMetadata> {
        let context = self.context().await?;
        let request = context
            .client
            .request(Method::GET, context.config.view_endpoint(view))
            .build()?;
        let http_response = context.client.query_catalog(request).await?;
        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<LoadViewResult>(http_response).await?;
                Ok(response.metadata)
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to load a view that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn update_view(&self, mut commit: ViewCommit) -> Result<ViewMetadata> {
        let context = self.context().await?;
        let request = context
            .client
            .request(Method::POST, context.config.view_endpoint(commit.identifier()))
            .json(&CommitViewRequest {
                identifier: Some(commit.identifier().clone()),
                requirements: commit.take_requirements(),
                updates: commit.take_updates(),
            })
            .build()?;
        let http_response = context.client.query_catalog(request).await?;
        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<LoadViewResult>(http_response).await?;
                Ok(response.metadata)
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to update a view that does not exist",
            )),
            StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "View commit failed due to a conflicting update",
            )
            .with_retryable(true)),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn drop_view(&self, view: &TableIdent) -> Result<()> {
        let context = self.context().await?;
        let request = context
            .client
            .request(Method::DELETE, context.config.view_endpoint(view))
            .build()?;
        let http_response = context.client.query_catalog(request).await?;
        match http_response.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to drop a view that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn view_exists(&self, view: &TableIdent) -> Result<bool> {
        let context = self.context().await?;
        let request = context
            .client
            .request(Method::HEAD, context.config.view_endpoint(view))
            .build()?;
        let http_response = context.client.query_catalog(request).await?;
        match http_response.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn list_views(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let context = self.context().await?;
        let request = context
            .client
            .request(Method::GET, context.config.views_endpoint(namespace))
            .build()?;
        let http_response = context.client.query_catalog(request).await?;
        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<ListViewsResponse>(http_response).await?;
                Ok(response.identifiers)
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::NamespaceNotFound,
                "Tried to list views under a namespace that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }
```

注意:`with_retryable`、`ListTablesResponse.identifiers` 字段名以该文件中表实现的实际写法为准(镜像 `update_table`/`list_tables` 的对应行);若 `Error::with_retryable` 不存在则去掉该调用。

- [ ] **Step 4: 写 PATCH.md**

`vendor/iceberg-catalog-rest-0.9.0/PATCH.md`:

```markdown
# NovaRocks patches on top of crates.io iceberg-catalog-rest 0.9.0

- Implemented the Iceberg REST view endpoints on `RestCatalog`:
  create_view / load_view / update_view (commit) / drop_view /
  view_exists / list_views, plus `CreateViewRequest`, `LoadViewResult`,
  `CommitViewRequest` wire types. Upstream 0.9.0 has no view support.
```

- [ ] **Step 5: 编译验证**

Run: `cargo build --profile dev-opt 2>&1 | tail -5`
Expected: 编译成功。wire 格式正确性由 Task 5 的 mockito 测试端到端验证。

- [ ] **Step 6: Commit**

```bash
git add vendor/iceberg-catalog-rest-0.9.0
git commit -m "feat(vendor/iceberg-catalog-rest): implement Iceberg REST view endpoints

create/load/commit/drop/exists/list views on RestCatalog, reusing the
existing HttpClient auth and prefix handling. Wire types follow the
Iceberg REST OpenAPI spec."
```

---

### Task 5: registry 层 view 包装(`views.rs`)+ mockito 测试

**Files:**
- Create: `src/connector/iceberg/catalog/views.rs`
- Modify: `src/connector/iceberg/catalog/mod.rs`(注册模块)
- Modify: `src/connector/iceberg/catalog/registry.rs`(`build_catalog_entry` 等个别函数提升 `pub(crate)`,见 Step 3)

- [ ] **Step 1: 写失败的 mockito 测试(文件骨架 + 测试)**

新建 `src/connector/iceberg/catalog/views.rs`,先放常量、类型与 `#[cfg(test)]` 模块(函数体暂 `todo!()` 会编译失败——因此本任务把类型/签名与测试一起写,Step 2 直接补实现后跑红绿;以下为完整目标代码,Step 1 先写到 `loaded_view_from_metadata` 之前的声明部分加 `unimplemented!()` 体,确认测试编译后失败,再进入 Step 2):

测试模块(置于文件尾):

```rust
#[cfg(test)]
mod rest_view_tests {
    //! Mocked unit tests for the REST view wiring, following the
    //! `rest_catalog_tests` pattern in registry.rs: mock `GET /v1/config`
    //! first, then the view route, and wrap sync entry points in
    //! `spawn_blocking`.
    use mockito::Server;

    use super::super::registry::{build_catalog_entry, IcebergCatalogEntry};
    use super::{create_view, drop_view, list_views, load_view, view_exists};
    use crate::sql::parser::ast::TableColumnDef;
    use crate::sql::SqlType;

    fn rest_props(uri: &str) -> Vec<(String, String)> {
        vec![
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ("uri".to_string(), uri.to_string()),
        ]
    }

    const EMPTY_CONFIG_BODY: &str = r#"{"overrides":{},"defaults":{}}"#;

    fn rest_entry(uri: &str) -> IcebergCatalogEntry {
        build_catalog_entry("ice_rest", &rest_props(uri)).expect("rest entry")
    }

    /// Minimal spec-valid LoadViewResult body with the given representations
    /// JSON array (e.g. `[{"type":"sql","sql":"SELECT 1","dialect":"spark"}]`).
    fn load_view_body(representations: &str) -> String {
        format!(
            r#"{{
              "metadata-location": "s3://warehouse/db/v/metadata/00001-x.metadata.json",
              "metadata": {{
                "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
                "format-version": 1,
                "location": "s3://warehouse/db/v",
                "current-version-id": 1,
                "versions": [{{
                  "version-id": 1,
                  "schema-id": 0,
                  "timestamp-ms": 1700000000000,
                  "summary": {{"engine-name": "novarocks"}},
                  "default-namespace": ["analytics"],
                  "representations": {representations}
                }}],
                "version-log": [{{"version-id": 1, "timestamp-ms": 1700000000000}}],
                "schemas": [{{
                  "schema-id": 0,
                  "type": "struct",
                  "fields": [{{"id": 1, "name": "id", "required": false, "type": "long"}}]
                }}],
                "properties": {{"comment": "a test view"}}
              }},
              "config": {{}}
            }}"#
        )
    }

    #[tokio::test]
    async fn create_view_posts_starrocks_dialect() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let create = server
            .mock("POST", "/v1/namespaces/analytics/views")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""dialect":"starrocks""#.to_string()),
                mockito::Matcher::Regex(r#""sql":"SELECT id FROM t""#.to_string()),
            ]))
            .with_status(200)
            .with_body(load_view_body(
                r#"[{"type":"sql","sql":"SELECT id FROM t","dialect":"starrocks"}]"#,
            ))
            .expect(1)
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let columns = vec![TableColumnDef {
            name: "id".to_string(),
            data_type: SqlType::BigInt,
            nullable: true,
            aggregation: None,
            default: None,
        }];
        tokio::task::spawn_blocking(move || {
            create_view(
                &entry,
                "analytics",
                "v_demo",
                &columns,
                "SELECT id FROM t",
                Some("a test view"),
                false,
            )
            .expect("create view via mock");
        })
        .await
        .expect("join");
        create.assert_async().await;
    }

    #[tokio::test]
    async fn load_view_prefers_starrocks_representation() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _load = server
            .mock("GET", "/v1/namespaces/analytics/views/v_demo")
            .with_status(200)
            .with_body(load_view_body(
                r#"[{"type":"sql","sql":"SELECT 1","dialect":"spark"},
                   {"type":"sql","sql":"SELECT 2","dialect":"StarRocks"}]"#,
            ))
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let view = tokio::task::spawn_blocking(move || {
            load_view(&entry, "analytics", "v_demo").expect("load view")
        })
        .await
        .expect("join");
        assert_eq!(view.sql, "SELECT 2");
        assert!(view.dialect.eq_ignore_ascii_case("starrocks"));
        assert_eq!(view.default_namespace, "analytics");
        assert_eq!(view.column_names, vec!["id".to_string()]);
        assert_eq!(view.comment.as_deref(), Some("a test view"));
    }

    #[tokio::test]
    async fn load_view_falls_back_to_first_sql_representation() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _load = server
            .mock("GET", "/v1/namespaces/analytics/views/v_spark")
            .with_status(200)
            .with_body(load_view_body(
                r#"[{"type":"sql","sql":"SELECT 1","dialect":"spark"}]"#,
            ))
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let view = tokio::task::spawn_blocking(move || {
            load_view(&entry, "analytics", "v_spark").expect("load view")
        })
        .await
        .expect("join");
        assert_eq!(view.dialect, "spark");
        assert_eq!(view.sql, "SELECT 1");
    }

    #[tokio::test]
    async fn load_view_not_found_maps_to_unknown_view() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _load = server
            .mock("GET", "/v1/namespaces/analytics/views/missing")
            .with_status(404)
            .with_body(r#"{"error":{"message":"not found","type":"NoSuchViewException","code":404}}"#)
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let err = tokio::task::spawn_blocking(move || {
            load_view(&entry, "analytics", "missing").expect_err("must fail")
        })
        .await
        .expect("join");
        assert!(err.contains("unknown view: analytics.missing"), "got: {err}");
    }

    #[tokio::test]
    async fn drop_view_not_found_maps_to_unknown_view() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _drop = server
            .mock("DELETE", "/v1/namespaces/analytics/views/missing")
            .with_status(404)
            .with_body(r#"{"error":{"message":"not found","type":"NoSuchViewException","code":404}}"#)
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let err = tokio::task::spawn_blocking(move || {
            drop_view(&entry, "analytics", "missing").expect_err("must fail")
        })
        .await
        .expect("join");
        assert!(err.contains("unknown view: analytics.missing"), "got: {err}");
    }

    #[tokio::test]
    async fn view_exists_and_list_views_roundtrip() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _head = server
            .mock("HEAD", "/v1/namespaces/analytics/views/v_demo")
            .with_status(204)
            .create_async()
            .await;
        let _list = server
            .mock("GET", "/v1/namespaces/analytics/views")
            .with_status(200)
            .with_body(r#"{"identifiers":[{"namespace":["analytics"],"name":"v_demo"}]}"#)
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let entry2 = entry.clone();
        let exists = tokio::task::spawn_blocking(move || {
            view_exists(&entry, "analytics", "v_demo").expect("exists")
        })
        .await
        .expect("join");
        assert!(exists);
        let names = tokio::task::spawn_blocking(move || {
            list_views(&entry2, "analytics").expect("list")
        })
        .await
        .expect("join");
        assert_eq!(names, vec!["v_demo".to_string()]);
    }

    #[test]
    fn view_ops_require_rest_catalog() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let props = vec![
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
            (
                "warehouse".to_string(),
                format!("file://{}", dir.path().display()),
            ),
        ];
        let entry = build_catalog_entry("ice_hadoop", &props).expect("hadoop entry");
        let err = list_views(&entry, "analytics").expect_err("must fail");
        assert!(err.contains("require a REST iceberg catalog"), "got: {err}");
    }
}
```

- [ ] **Step 2: 实现 views.rs 主体**

文件头与实现(测试模块之上):

```rust
//! Iceberg view metadata operations. Views are only supported on REST
//! catalogs: Hadoop/Memory catalogs reject every operation here. Views
//! are deliberately not cached — each query re-loads the view metadata
//! so external changes are visible immediately.

use std::collections::HashMap;

use iceberg::spec::{
    NestedField, Schema, SqlViewRepresentation, ViewMetadata, ViewRepresentation,
    ViewRepresentations, ViewVersion,
};
use iceberg::{
    Catalog, NamespaceIdent, TableIdent, ViewCommit, ViewCreation, ViewRequirement,
};

use super::registry::{
    block_on_iceberg, build_iceberg_catalog, iceberg_type_for_sql_type, IcebergCatalogEntry,
    IcebergCatalogKind,
};
use crate::engine::catalog::normalize_identifier;
use crate::sql::parser::ast::TableColumnDef;

/// Dialect tag NovaRocks writes into view representations. NovaRocks parses
/// StarRocks-flavoured SQL, so it shares StarRocks' dialect tag for
/// cross-engine interop.
pub(crate) const VIEW_DIALECT_STARROCKS: &str = "starrocks";

/// A view loaded from an iceberg catalog, reduced to what the engine needs.
#[derive(Clone, Debug)]
pub(crate) struct LoadedIcebergView {
    pub sql: String,
    pub dialect: String,
    /// Dotted default namespace from the current view version; bare table
    /// names in `sql` resolve against this (and the catalog the view was
    /// loaded from — the stored default-catalog is intentionally ignored,
    /// matching StarRocks, because other engines write their own local
    /// catalog aliases there).
    pub default_namespace: String,
    pub column_names: Vec<String>,
    pub comment: Option<String>,
}

fn catalog_for_views(
    entry: &IcebergCatalogEntry,
) -> Result<std::sync::Arc<dyn Catalog>, String> {
    if !matches!(entry.kind, IcebergCatalogKind::Rest) {
        return Err(format!(
            "view operations require a REST iceberg catalog; this catalog is {:?}",
            entry.kind
        ));
    }
    build_iceberg_catalog(entry)
}

fn view_ident(namespace: &str, view: &str) -> Result<(NamespaceIdent, TableIdent), String> {
    let ns_name = normalize_identifier(namespace)?;
    let view_name = normalize_identifier(view)?;
    let ident = TableIdent::from_strs([ns_name.as_str(), view_name.as_str()])
        .map_err(|e| format!("build view ident: {e}"))?;
    Ok((NamespaceIdent::new(ns_name), ident))
}

fn current_millis() -> Result<i64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .map_err(|e| format!("system clock before epoch: {e}"))
}

fn build_view_schema(columns: &[TableColumnDef]) -> Result<Schema, String> {
    let mut next_nested_field_id =
        i32::try_from(columns.len() + 1).map_err(|_| "too many view columns".to_string())?;
    let fields = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let field_id =
                i32::try_from(idx + 1).map_err(|_| "too many view columns".to_string())?;
            let iceberg_type =
                iceberg_type_for_sql_type(&column.data_type, &mut next_nested_field_id)?;
            let field = if column.nullable {
                NestedField::optional(field_id, &column.name, iceberg_type)
            } else {
                NestedField::required(field_id, &column.name, iceberg_type)
            };
            Ok(field.into())
        })
        .collect::<Result<Vec<_>, String>>()?;
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build iceberg view schema failed: {e}"))
}

pub(crate) fn create_view(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    view_name: &str,
    columns: &[TableColumnDef],
    view_sql: &str,
    comment: Option<&str>,
    or_replace: bool,
) -> Result<(), String> {
    let catalog = catalog_for_views(entry)?;
    let (ns, ident) = view_ident(namespace, view_name)?;
    let schema = build_view_schema(columns)?;
    let representations = ViewRepresentations::new(vec![ViewRepresentation::Sql(
        SqlViewRepresentation {
            sql: view_sql.to_string(),
            dialect: VIEW_DIALECT_STARROCKS.to_string(),
        },
    )]);
    let mut properties = HashMap::new();
    if let Some(comment) = comment {
        properties.insert("comment".to_string(), comment.to_string());
    }
    let mut summary = HashMap::new();
    summary.insert("engine-name".to_string(), "novarocks".to_string());

    if or_replace {
        let existing = block_on_iceberg(async { catalog.load_view(&ident).await })
            .map_err(|e| format!("load iceberg view runtime failed: {e}"))?;
        match existing {
            Ok(current) => {
                return replace_view(
                    catalog.as_ref(),
                    &ident,
                    current,
                    schema,
                    representations,
                    properties,
                    summary,
                );
            }
            Err(err)
                if err
                    .to_string()
                    .contains("Tried to load a view that does not exist") => {}
            Err(err) => return Err(format!("load iceberg view {ident}: {err}")),
        }
    }

    let creation = ViewCreation::builder()
        .name(ident.name.clone())
        .location(None)
        .representations(representations)
        .schema(schema)
        .properties(properties)
        .default_namespace(ns.clone())
        .default_catalog(None)
        .summary(summary)
        .build();
    block_on_iceberg(async { catalog.create_view(&ns, creation).await })
        .map_err(|e| format!("create iceberg view runtime failed: {e}"))?
        .map_err(|e| {
            let message = e.to_string();
            if message.contains("The view already exists") {
                format!("view already exists: {ident}")
            } else {
                format!("create iceberg view {ident}: {message}")
            }
        })?;
    Ok(())
}

fn replace_view(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    current: ViewMetadata,
    schema: Schema,
    representations: ViewRepresentations,
    properties: HashMap<String, String>,
    summary: HashMap<String, String>,
) -> Result<(), String> {
    let uuid = current.uuid();
    let new_version = ViewVersion::builder()
        .with_version_id(1) // reassigned by the builder when added
        .with_schema_id(schema.schema_id())
        .with_timestamp_ms(current_millis()?)
        .with_summary(summary)
        .with_representations(representations)
        .with_default_catalog(None)
        .with_default_namespace(ident.namespace.clone())
        .build();

    let mut builder = current.into_builder();
    if !properties.is_empty() {
        builder = builder
            .set_properties(properties)
            .map_err(|e| format!("set replaced view properties: {e}"))?;
    }
    let result = builder
        .set_current_version(new_version, schema)
        .map_err(|e| format!("stage replaced view version: {e}"))?
        .build()
        .map_err(|e| format!("build replaced view metadata: {e}"))?;

    let commit = ViewCommit::builder()
        .ident(ident.clone())
        .requirements(vec![ViewRequirement::UuidMatch { uuid }])
        .updates(result.changes)
        .build();
    block_on_iceberg(async { catalog.update_view(commit).await })
        .map_err(|e| format!("replace iceberg view runtime failed: {e}"))?
        .map_err(|e| format!("replace iceberg view {ident}: {e}"))?;
    Ok(())
}

pub(crate) fn load_view(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    view_name: &str,
) -> Result<LoadedIcebergView, String> {
    let catalog = catalog_for_views(entry)?;
    let (_ns, ident) = view_ident(namespace, view_name)?;
    let metadata = block_on_iceberg(async { catalog.load_view(&ident).await })
        .map_err(|e| format!("load iceberg view runtime failed: {e}"))?
        .map_err(|e| format_view_not_found(&ident, "load", e))?;
    loaded_view_from_metadata(&ident, &metadata)
}

pub(crate) fn drop_view(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    view_name: &str,
) -> Result<(), String> {
    let catalog = catalog_for_views(entry)?;
    let (_ns, ident) = view_ident(namespace, view_name)?;
    block_on_iceberg(async { catalog.drop_view(&ident).await })
        .map_err(|e| format!("drop iceberg view runtime failed: {e}"))?
        .map_err(|e| format_view_not_found(&ident, "drop", e))
}

pub(crate) fn view_exists(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    view_name: &str,
) -> Result<bool, String> {
    let catalog = catalog_for_views(entry)?;
    let (_ns, ident) = view_ident(namespace, view_name)?;
    block_on_iceberg(async { catalog.view_exists(&ident).await })
        .map_err(|e| format!("view exists runtime failed: {e}"))?
        .map_err(|e| format!("check iceberg view {ident}: {e}"))
}

pub(crate) fn list_views(
    entry: &IcebergCatalogEntry,
    namespace: &str,
) -> Result<Vec<String>, String> {
    let catalog = catalog_for_views(entry)?;
    let ns = NamespaceIdent::new(normalize_identifier(namespace)?);
    let idents = block_on_iceberg(async { catalog.list_views(&ns).await })
        .map_err(|e| format!("list iceberg views runtime failed: {e}"))?
        .map_err(|e| format!("list iceberg views in {ns}: {e}"))?;
    let mut names: Vec<String> = idents.into_iter().map(|ident| ident.name).collect();
    names.sort();
    Ok(names)
}

fn format_view_not_found<E: std::fmt::Display>(
    ident: &TableIdent,
    op: &str,
    err: E,
) -> String {
    let message = err.to_string();
    if message.contains("view that does not exist") {
        format!("unknown view: {ident}")
    } else {
        format!("{op} REST iceberg view {ident}: {message}")
    }
}

fn loaded_view_from_metadata(
    ident: &TableIdent,
    metadata: &ViewMetadata,
) -> Result<LoadedIcebergView, String> {
    let version = metadata.current_version();
    // Prefer the starrocks representation; otherwise fall back to the first
    // SQL representation (mirrors iceberg-java View::sqlFor).
    let mut chosen: Option<&SqlViewRepresentation> = None;
    for representation in version.representations().iter() {
        let ViewRepresentation::Sql(sql_repr) = representation;
        if sql_repr.dialect.eq_ignore_ascii_case(VIEW_DIALECT_STARROCKS) {
            chosen = Some(sql_repr);
            break;
        }
        if chosen.is_none() {
            chosen = Some(sql_repr);
        }
    }
    let chosen =
        chosen.ok_or_else(|| format!("iceberg view {ident} has no SQL representation"))?;
    let default_namespace = version
        .default_namespace()
        .iter()
        .map(|part| part.to_string())
        .collect::<Vec<_>>()
        .join(".");
    let column_names = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect();
    Ok(LoadedIcebergView {
        sql: chosen.sql.clone(),
        dialect: chosen.dialect.clone(),
        default_namespace,
        column_names,
        comment: metadata.properties().get("comment").cloned(),
    })
}
```

注意单元(`unknown view: {ident}`):`TableIdent` Display 为 `ns.name`,与测试断言 `unknown view: analytics.missing` 一致。

- [ ] **Step 3: 模块注册与可见性**

1. `src/connector/iceberg/catalog/mod.rs` 增加 `pub(crate) mod views;`。
2. `registry.rs` 中按编译报错将以下项提升为 `pub(crate)`(若已是则跳过):`build_catalog_entry`、`IcebergCatalogEntry.kind`(已是)、`build_iceberg_catalog`(已是)、`iceberg_type_for_sql_type`(已是)、`block_on_iceberg`(已是)。
3. `ViewVersion` 的 representations 访问器、`Schema::as_struct().fields()` 字段访问方式以 vendored crate 实际 API 为准,编译报错时对照 `vendor/iceberg-0.9.0/src/spec/{view_version.rs,schema.rs}` 调整。

- [ ] **Step 4: 红→绿**

Run: `cargo test --lib connector::iceberg::catalog::views`
Expected: 7 个测试 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/views.rs src/connector/iceberg/catalog/mod.rs src/connector/iceberg/catalog/registry.rs
git commit -m "feat(iceberg): registry-level view CRUD for REST catalogs

create (with OR REPLACE via view commit), load (starrocks-dialect
preferred, first-SQL fallback), drop, exists and list. Non-REST iceberg
catalogs reject view operations. Covered by mockito endpoint tests."
```

---

### Task 6: `CatalogBackend` trait view 方法 + iceberg 实现

**Files:**
- Modify: `src/connector/backend.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`

- [ ] **Step 1: backend.rs 增加请求/响应结构与 trait 方法**

`CreateTableRequest` 后追加:

```rust
/// Create-view request routed to a catalog backend.
#[derive(Clone, Debug)]
pub(crate) struct CreateViewRequest {
    pub catalog: String,
    pub namespace: String,
    pub view: String,
    pub columns: Vec<TableColumnDef>,
    /// The view body as SQL text (StarRocks dialect).
    pub view_sql: String,
    pub comment: Option<String>,
    pub or_replace: bool,
}

/// A view loaded through a catalog backend.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedView {
    pub sql: String,
    pub dialect: String,
    pub default_namespace: String,
    pub column_names: Vec<String>,
    pub comment: Option<String>,
}
```

`CatalogBackend` trait 内、`current_schema_id` 之后追加默认方法:

```rust
    fn create_view(&self, _req: CreateViewRequest) -> Result<(), String> {
        Err(format!("{} backend does not support views", self.name()))
    }

    fn drop_view(&self, _catalog: &str, _namespace: &str, _view: &str) -> Result<(), String> {
        Err(format!("{} backend does not support views", self.name()))
    }

    fn load_view(
        &self,
        _catalog: &str,
        _namespace: &str,
        _view: &str,
    ) -> Result<ResolvedView, String> {
        Err(format!("{} backend does not support views", self.name()))
    }

    /// Whether a view with this name exists. Backends without view support
    /// report `false` so strict DROP-type checks degrade gracefully.
    fn view_exists(
        &self,
        _catalog: &str,
        _namespace: &str,
        _view: &str,
    ) -> Result<bool, String> {
        Ok(false)
    }

    fn list_views(&self, _catalog: &str, _namespace: &str) -> Result<Vec<String>, String> {
        Err(format!("{} backend does not support views", self.name()))
    }
```

- [ ] **Step 2: iceberg backend 实现**

`src/connector/iceberg/catalog/backend.rs` 的 `impl CatalogBackend for IcebergCatalogBackend` 内追加(import 区补 `use super::views;` 与 backend.rs 的 `CreateViewRequest, ResolvedView`):

```rust
    fn create_view(&self, req: CreateViewRequest) -> Result<(), String> {
        let entry = self.entry(&req.catalog)?;
        views::create_view(
            &entry,
            &req.namespace,
            &req.view,
            &req.columns,
            &req.view_sql,
            req.comment.as_deref(),
            req.or_replace,
        )
    }

    fn drop_view(&self, catalog: &str, namespace: &str, view: &str) -> Result<(), String> {
        views::drop_view(&self.entry(catalog)?, namespace, view)
    }

    fn load_view(
        &self,
        catalog: &str,
        namespace: &str,
        view: &str,
    ) -> Result<ResolvedView, String> {
        let loaded = views::load_view(&self.entry(catalog)?, namespace, view)?;
        Ok(ResolvedView {
            sql: loaded.sql,
            dialect: loaded.dialect,
            default_namespace: loaded.default_namespace,
            column_names: loaded.column_names,
            comment: loaded.comment,
        })
    }

    fn view_exists(&self, catalog: &str, namespace: &str, view: &str) -> Result<bool, String> {
        views::view_exists(&self.entry(catalog)?, namespace, view)
    }

    fn list_views(&self, catalog: &str, namespace: &str) -> Result<Vec<String>, String> {
        views::list_views(&self.entry(catalog)?, namespace)
    }
```

- [ ] **Step 3: 编译 + 既有测试**

Run: `cargo test --lib connector::iceberg`
Expected: PASS。

- [ ] **Step 4: Commit**

```bash
git add src/connector/backend.rs src/connector/iceberg/catalog/backend.rs
git commit -m "feat(connector): view methods on CatalogBackend with iceberg impl

Default implementations reject views (view_exists reports false) so the
StarRocks backend is unaffected; the iceberg backend delegates to the
registry view layer."
```

---

### Task 7: view 目标解析 + SELECT 内联展开

**Files:**
- Create: `src/engine/iceberg_view.rs`(本任务只放目标解析;create/drop 流程在 Task 8/9 补)
- Create: `src/engine/iceberg_view_rewrite.rs`
- Modify: `src/engine/mod.rs`(模块声明 + Query/EXPLAIN 两处 wiring)

- [ ] **Step 1: 目标解析(`iceberg_view.rs`)**

```rust
//! Iceberg-catalog view DDL flows and name-target resolution.
//!
//! A view name routes to an iceberg catalog when it is a 3-part name
//! naming a registered iceberg catalog, or a 1/2-part name while a
//! session catalog (`SET CATALOG`) is active. Everything else stays a
//! session view in `StandaloneState::views`.

use std::sync::Arc;

use crate::engine::catalog::normalize_identifier;
use crate::engine::StandaloneState;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergViewTarget {
    pub catalog: String,
    pub namespace: String,
    pub view: String,
}

/// Resolve a view name (already split into identifier parts) to an iceberg
/// target. `Ok(None)` means "session view" (default catalog). The catalog
/// must exist in the iceberg registry; an unknown catalog is an error.
pub(crate) fn resolve_iceberg_view_target_parts(
    state: &Arc<StandaloneState>,
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Option<IcebergViewTarget>, String> {
    let session_catalog = current_catalog
        .filter(|catalog| !catalog.eq_ignore_ascii_case("default_catalog"));
    let (catalog, namespace, view) = match parts {
        [catalog, db, view] => {
            if catalog.eq_ignore_ascii_case("default_catalog") {
                return Ok(None);
            }
            (catalog.clone(), db.clone(), view.clone())
        }
        [db, view] => match session_catalog {
            Some(catalog) => (catalog.to_string(), db.clone(), view.clone()),
            None => return Ok(None),
        },
        [view] => match session_catalog {
            Some(catalog) => (
                catalog.to_string(),
                current_database.to_string(),
                view.clone(),
            ),
            None => return Ok(None),
        },
        _ => return Err(format!("invalid view name: {}", parts.join("."))),
    };
    let target = IcebergViewTarget {
        catalog: normalize_identifier(&catalog)?,
        namespace: normalize_identifier(&namespace)?,
        view: normalize_identifier(&view)?,
    };
    // Validate catalog existence eagerly so DDL gets a clear error.
    state
        .iceberg_catalogs
        .read()
        .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?
        .get(&target.catalog)?;
    Ok(Some(target))
}

/// Helper for sqlparser names: extract identifier parts then resolve.
pub(crate) fn resolve_iceberg_view_target(
    state: &Arc<StandaloneState>,
    name: &sqlparser::ast::ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Option<IcebergViewTarget>, String> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .collect();
    resolve_iceberg_view_target_parts(state, &parts, current_catalog, current_database)
}
```

`src/engine/mod.rs` 模块声明区追加 `pub(crate) mod iceberg_view;` 与 `pub(crate) mod iceberg_view_rewrite;`(与 `mod view_rewrite;` 同区)。

- [ ] **Step 2: 写裸名限定器的失败单测(`iceberg_view_rewrite.rs` 尾部)**

```rust
#[cfg(test)]
mod qualify_tests {
    use super::qualify_view_body_names;
    use crate::sql::parser::dialect::StarRocksDialect;
    use sqlparser::parser::Parser;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
        let stmt = parser.parse_statement().expect("statement");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        *query
    }

    #[test]
    fn qualifies_bare_and_two_part_names() {
        let mut query =
            parse_query("SELECT a.id FROM t1 a JOIN db2.t2 b ON a.id = b.id");
        qualify_view_body_names(&mut query, "ice", "ns1");
        let rendered = query.to_string();
        assert!(rendered.contains("ice.ns1.t1"), "got: {rendered}");
        assert!(rendered.contains("ice.db2.t2"), "got: {rendered}");
    }

    #[test]
    fn leaves_three_part_names_and_ctes_alone() {
        let mut query = parse_query(
            "WITH c AS (SELECT id FROM t1) SELECT * FROM c JOIN other_cat.db.t3 ON true",
        );
        qualify_view_body_names(&mut query, "ice", "ns1");
        let rendered = query.to_string();
        assert!(rendered.contains("FROM c"), "got: {rendered}");
        assert!(rendered.contains("other_cat.db.t3"), "got: {rendered}");
        assert!(rendered.contains("ice.ns1.t1"), "got: {rendered}");
    }
}
```

Run: `cargo test --lib engine::iceberg_view_rewrite`
Expected: 编译失败(模块/函数不存在)——即红。

- [ ] **Step 3: 实现 `iceberg_view_rewrite.rs`**

```rust
//! Inline expansion of iceberg-catalog views referenced by SELECT queries.
//!
//! Runs after session-view expansion and before the analyzer. For every
//! table factor that resolves to a REST iceberg catalog the table is
//! probed first (matching StarRocks' table-then-view order); when no
//! table exists but a view does, the view's SQL representation is parsed
//! and spliced inline as a derived subquery. Bare names inside the view
//! body are qualified against the view's own catalog and stored
//! default-namespace — not the session database. Nested views expand
//! recursively with cycle detection.

use std::collections::HashSet;
use std::sync::Arc;

use sqlparser::ast as sqlast;

use crate::connector::backend::ResolvedView;
use crate::engine::iceberg_view::{resolve_iceberg_view_target_parts, IcebergViewTarget};
use crate::engine::StandaloneState;

type ViewKey = (String, String, String);

pub(crate) fn expand_iceberg_views_in_query(
    state: &Arc<StandaloneState>,
    query: &mut sqlast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), String> {
    let mut stack: Vec<ViewKey> = Vec::new();
    expand_query(state, query, current_catalog, current_database, &mut stack)
}

fn expand_query(
    state: &Arc<StandaloneState>,
    query: &mut sqlast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
    stack: &mut Vec<ViewKey>,
) -> Result<(), String> {
    // CTE names shadow tables/views. Collect them up-front; the slight
    // over-shadowing (a later CTE name visible in an earlier body) only
    // suppresses expansion, never mis-expands.
    let mut cte_names: HashSet<String> = HashSet::new();
    if let Some(with_clause) = query.with.as_ref() {
        for cte in &with_clause.cte_tables {
            cte_names.insert(cte.alias.name.value.to_ascii_lowercase());
        }
    }
    if let Some(with_clause) = query.with.as_mut() {
        for cte in &mut with_clause.cte_tables {
            expand_query(state, cte.query.as_mut(), current_catalog, current_database, stack)?;
        }
    }
    expand_set_expr(
        state,
        query.body.as_mut(),
        current_catalog,
        current_database,
        &cte_names,
        stack,
    )
}

fn expand_set_expr(
    state: &Arc<StandaloneState>,
    expr: &mut sqlast::SetExpr,
    current_catalog: Option<&str>,
    current_database: &str,
    cte_names: &HashSet<String>,
    stack: &mut Vec<ViewKey>,
) -> Result<(), String> {
    match expr {
        sqlast::SetExpr::Select(select) => {
            for twj in select.from.iter_mut() {
                expand_table_factor(
                    state,
                    &mut twj.relation,
                    current_catalog,
                    current_database,
                    cte_names,
                    stack,
                )?;
                for join in twj.joins.iter_mut() {
                    expand_table_factor(
                        state,
                        &mut join.relation,
                        current_catalog,
                        current_database,
                        cte_names,
                        stack,
                    )?;
                }
            }
            Ok(())
        }
        sqlast::SetExpr::Query(q) => {
            expand_query(state, q.as_mut(), current_catalog, current_database, stack)
        }
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            expand_set_expr(state, left.as_mut(), current_catalog, current_database, cte_names, stack)?;
            expand_set_expr(state, right.as_mut(), current_catalog, current_database, cte_names, stack)
        }
        _ => Ok(()),
    }
}

fn expand_table_factor(
    state: &Arc<StandaloneState>,
    factor: &mut sqlast::TableFactor,
    current_catalog: Option<&str>,
    current_database: &str,
    cte_names: &HashSet<String>,
    stack: &mut Vec<ViewKey>,
) -> Result<(), String> {
    match factor {
        sqlast::TableFactor::Table { name, alias, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                    _ => None,
                })
                .collect();
            if parts.len() == 1 && cte_names.contains(&parts[0].to_ascii_lowercase()) {
                return Ok(());
            }
            let Some(target) =
                rest_view_candidate(state, &parts, current_catalog, current_database)
            else {
                return Ok(());
            };
            // Table-first, matching StarRocks: a probe failure (connectivity
            // etc.) leaves the factor untouched so the analyzer surfaces the
            // canonical error for tables.
            if probe_table_exists(state, &target) {
                return Ok(());
            }
            let Some(view) = probe_load_view(state, &target)? else {
                return Ok(());
            };
            let key = (
                target.catalog.clone(),
                target.namespace.clone(),
                target.view.clone(),
            );
            if stack.contains(&key) {
                return Err(format!(
                    "circular view reference: {}.{}.{}",
                    key.0, key.1, key.2
                ));
            }
            let mut body = parse_view_sql(&view, &key)?;
            qualify_view_body_names(&mut body, &target.catalog, &view.default_namespace);
            stack.push(key);
            expand_query(
                state,
                &mut body,
                Some(&target.catalog),
                &view.default_namespace,
                stack,
            )?;
            stack.pop();

            let alias = alias.take().unwrap_or_else(|| sqlast::TableAlias {
                name: sqlast::Ident::new(parts.last().cloned().unwrap_or_default()),
                columns: Vec::new(),
                explicit: false,
            });
            *factor = sqlast::TableFactor::Derived {
                lateral: false,
                subquery: Box::new(body),
                alias: Some(alias),
                sample: None,
            };
            Ok(())
        }
        sqlast::TableFactor::Derived { subquery, .. } => {
            expand_query(state, subquery.as_mut(), current_catalog, current_database, stack)
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            expand_table_factor(
                state,
                &mut table_with_joins.relation,
                current_catalog,
                current_database,
                cte_names,
                stack,
            )?;
            for join in table_with_joins.joins.iter_mut() {
                expand_table_factor(
                    state,
                    &mut join.relation,
                    current_catalog,
                    current_database,
                    cte_names,
                    stack,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Resolve to a target only when the name lands in a registered REST
/// iceberg catalog; all probe-ineligible names return None.
fn rest_view_candidate(
    state: &Arc<StandaloneState>,
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Option<IcebergViewTarget> {
    let target = resolve_iceberg_view_target_parts(state, parts, current_catalog, current_database)
        .ok()
        .flatten()?;
    let registry = state.iceberg_catalogs.read().ok()?;
    let entry = registry.get(&target.catalog).ok()?;
    if !matches!(
        entry.kind,
        crate::connector::iceberg::catalog::registry::IcebergCatalogKind::Rest
    ) {
        return None;
    }
    Some(target)
}

fn probe_table_exists(state: &Arc<StandaloneState>, target: &IcebergViewTarget) -> bool {
    let Ok(backend) = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend("iceberg")
    else {
        return true;
    };
    backend
        .table_exists(&target.catalog, &target.namespace, &target.view)
        .unwrap_or(true)
}

fn probe_load_view(
    state: &Arc<StandaloneState>,
    target: &IcebergViewTarget,
) -> Result<Option<ResolvedView>, String> {
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend("iceberg")?;
    match backend.load_view(&target.catalog, &target.namespace, &target.view) {
        Ok(view) => Ok(Some(view)),
        Err(err) if err.contains("unknown view") => Ok(None),
        Err(err) => Err(err),
    }
}

fn parse_view_sql(view: &ResolvedView, key: &ViewKey) -> Result<sqlast::Query, String> {
    let dialect = crate::sql::parser::dialect::StarRocksDialect;
    let mut parser = sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(&view.sql)
        .map_err(|e| view_parse_error(key, &view.dialect, &e.to_string()))?;
    let stmt = parser
        .parse_statement()
        .map_err(|e| view_parse_error(key, &view.dialect, &e.to_string()))?;
    let sqlast::Statement::Query(query) = stmt else {
        return Err(format!(
            "iceberg view {}.{}.{} body is not a SELECT query",
            key.0, key.1, key.2
        ));
    };
    Ok(*query)
}

fn view_parse_error(key: &ViewKey, dialect: &str, err: &str) -> String {
    format!(
        "parse iceberg view {}.{}.{} (representation dialect `{dialect}`) failed: {err}",
        key.0, key.1, key.2
    )
}

/// Qualify bare/2-part table names inside a view body against the view's
/// catalog and default namespace. 3-part names and CTE references are left
/// untouched. Pure AST transform, unit-tested below.
pub(crate) fn qualify_view_body_names(
    query: &mut sqlast::Query,
    catalog: &str,
    default_namespace: &str,
) {
    let mut cte_names: HashSet<String> = HashSet::new();
    if let Some(with_clause) = query.with.as_ref() {
        for cte in &with_clause.cte_tables {
            cte_names.insert(cte.alias.name.value.to_ascii_lowercase());
        }
    }
    if let Some(with_clause) = query.with.as_mut() {
        for cte in &mut with_clause.cte_tables {
            qualify_view_body_names(cte.query.as_mut(), catalog, default_namespace);
        }
    }
    qualify_set_expr(query.body.as_mut(), catalog, default_namespace, &cte_names);
}

fn qualify_set_expr(
    expr: &mut sqlast::SetExpr,
    catalog: &str,
    default_namespace: &str,
    cte_names: &HashSet<String>,
) {
    match expr {
        sqlast::SetExpr::Select(select) => {
            for twj in select.from.iter_mut() {
                qualify_table_factor(&mut twj.relation, catalog, default_namespace, cte_names);
                for join in twj.joins.iter_mut() {
                    qualify_table_factor(&mut join.relation, catalog, default_namespace, cte_names);
                }
            }
        }
        sqlast::SetExpr::Query(q) => {
            qualify_view_body_names(q.as_mut(), catalog, default_namespace)
        }
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            qualify_set_expr(left.as_mut(), catalog, default_namespace, cte_names);
            qualify_set_expr(right.as_mut(), catalog, default_namespace, cte_names);
        }
        _ => {}
    }
}

fn qualify_table_factor(
    factor: &mut sqlast::TableFactor,
    catalog: &str,
    default_namespace: &str,
    cte_names: &HashSet<String>,
) {
    match factor {
        sqlast::TableFactor::Table { name, .. } => {
            let ident_count = name
                .0
                .iter()
                .filter(|part| matches!(part, sqlast::ObjectNamePart::Identifier(_)))
                .count();
            match ident_count {
                1 => {
                    if let Some(sqlast::ObjectNamePart::Identifier(table)) = name.0.first() {
                        if cte_names.contains(&table.value.to_ascii_lowercase()) {
                            return;
                        }
                    }
                    let mut parts = vec![
                        sqlast::ObjectNamePart::Identifier(sqlast::Ident::new(catalog)),
                        sqlast::ObjectNamePart::Identifier(sqlast::Ident::new(
                            default_namespace,
                        )),
                    ];
                    parts.append(&mut name.0);
                    name.0 = parts;
                }
                2 => {
                    name.0.insert(
                        0,
                        sqlast::ObjectNamePart::Identifier(sqlast::Ident::new(catalog)),
                    );
                }
                _ => {}
            }
        }
        sqlast::TableFactor::Derived { subquery, .. } => {
            qualify_view_body_names(subquery.as_mut(), catalog, default_namespace);
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            qualify_table_factor(
                &mut table_with_joins.relation,
                catalog,
                default_namespace,
                cte_names,
            );
            for join in table_with_joins.joins.iter_mut() {
                qualify_table_factor(&mut join.relation, catalog, default_namespace, cte_names);
            }
        }
        _ => {}
    }
}
```

注意:`IcebergCatalogKind` 的可见路径若不同(`crate::connector::iceberg::catalog::registry::IcebergCatalogKind`),按编译报错修正 import。

- [ ] **Step 4: 单测转绿**

Run: `cargo test --lib engine::iceberg_view_rewrite`
Expected: 2 个 qualify 测试 PASS。

- [ ] **Step 5: wiring 到 SELECT 与 EXPLAIN**

`src/engine/mod.rs` 的 `Statement::Query` 分支(:933-937,session view 展开之后、`virtual_table::rewrite_query` 之前)插入:

```rust
            // Inline iceberg-catalog views (REST only). Runs after session
            // views so local definitions keep precedence.
            self::iceberg_view_rewrite::expand_iceberg_views_in_query(
                &self.inner,
                &mut prepared,
                current_catalog,
                current_database,
            )?;
```

`prepare_explain_query`(:2552-2572)中 session 展开后同样插入(state 形参版本):

```rust
    self::iceberg_view_rewrite::expand_iceberg_views_in_query(
        state,
        &mut prepared,
        current_catalog,
        current_database,
    )?;
```

- [ ] **Step 6: 编译 + lib 测试**

Run: `cargo test --lib engine`
Expected: PASS。

- [ ] **Step 7: Commit**

```bash
git add src/engine/iceberg_view.rs src/engine/iceberg_view_rewrite.rs src/engine/mod.rs
git commit -m "feat(engine): inline expansion of iceberg REST views in SELECT

Pre-analyzer pass probes REST-catalog table factors table-first, then
falls back to load_view and splices the parsed body as a derived
subquery. Bare names in view bodies qualify against the view's catalog
and stored default-namespace; nested views expand recursively with
cycle detection."
```

---

### Task 8: CREATE [OR REPLACE] VIEW 路由到 iceberg

**Files:**
- Modify: `src/engine/statistics.rs`(:110 前缀、`handle_create_view` :2245)
- Modify: `src/engine/iceberg_view.rs`(create 流程)

- [ ] **Step 1: 前缀拦截扩展**

`statistics.rs` `try_handle_statement`(:110)改为:

```rust
    if lower.starts_with("create view ") || lower.starts_with("create or replace view ") {
        return handle_create_view(state, trimmed, current_catalog, current_database).map(Some);
    }
```

- [ ] **Step 2: `handle_create_view` 增加路由**

签名加 `current_catalog: Option<&str>`;在拿到 `create_view` 结构后、写 session 注册表之前插入:

```rust
    if let Some(target) = crate::engine::iceberg_view::resolve_iceberg_view_target(
        state,
        &create_view.name,
        current_catalog,
        current_database,
    )? {
        return crate::engine::iceberg_view::create_iceberg_view(state, &target, *create_view);
    }
```

注意 `Statement::CreateView(create_view)` 模式拿到的是值;若为 `Box`,按编译报错调整解引用。session 分支其余逻辑不变。

- [ ] **Step 3: `iceberg_view.rs` 实现 create 流程**

```rust
use crate::connector::backend::CreateViewRequest;
use crate::engine::StatementResult;
use crate::sql::analysis::OutputColumn;
use crate::sql::parser::ast::TableColumnDef;

pub(crate) fn create_iceberg_view(
    state: &Arc<StandaloneState>,
    target: &IcebergViewTarget,
    stmt: sqlparser::ast::CreateView,
) -> Result<StatementResult, String> {
    if stmt.materialized {
        return Err(
            "CREATE MATERIALIZED VIEW must go through the materialized-view DDL path"
                .to_string(),
        );
    }
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend("iceberg")?;

    // Views and tables share the namespace on iceberg catalogs; reject
    // shadowing instead of letting the REST server pick a winner.
    if backend.table_exists(&target.catalog, &target.namespace, &target.view)? {
        return Err(format!(
            "a table named {}.{}.{} already exists",
            target.catalog, target.namespace, target.view
        ));
    }
    if stmt.if_not_exists
        && backend.view_exists(&target.catalog, &target.namespace, &target.view)?
    {
        return Ok(StatementResult::Ok);
    }

    // Persist the original body; analyze an expanded copy so views over
    // views type-check. Bare names in the body resolve against the view's
    // own catalog/namespace — identical to read-time qualification.
    let view_sql = stmt.query.to_string();
    let mut analyzed_query = (*stmt.query).clone();
    crate::engine::iceberg_view_rewrite::expand_iceberg_views_in_query(
        state,
        &mut analyzed_query,
        Some(&target.catalog),
        &target.namespace,
    )?;
    let output_columns =
        analyze_view_query(state, &target.catalog, &target.namespace, &analyzed_query)?;
    let columns = view_columns(&output_columns, &stmt.columns)?;

    backend.create_view(CreateViewRequest {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        view: target.view.clone(),
        columns,
        view_sql,
        comment: stmt.comment.clone(),
        or_replace: stmt.or_replace,
    })?;
    Ok(StatementResult::Ok)
}

fn analyze_view_query(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    query: &sqlparser::ast::Query,
) -> Result<Vec<OutputColumn>, String> {
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let catalog_mgr_snapshot = crate::engine::catalog_mgr_snapshot(state);
    let provider = crate::engine::build_analyzer_provider(
        Some(catalog),
        &catalog_snapshot,
        &catalog_mgr_snapshot,
        &connectors_snapshot,
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );
    let (resolved, _ctes, _factory) =
        crate::sql::analyzer::analyze(query, &provider, namespace)
            .map_err(|e| format!("analyze view definition failed: {e}"))?;
    let columns: Vec<OutputColumn> = resolved
        .output_columns
        .into_iter()
        .filter(|column| !column.is_internal)
        .collect();
    if columns.is_empty() {
        return Err("CREATE VIEW: SELECT produced no output columns".to_string());
    }
    Ok(columns)
}

fn view_columns(
    output: &[OutputColumn],
    aliases: &[sqlparser::ast::ViewColumnDef],
) -> Result<Vec<TableColumnDef>, String> {
    if !aliases.is_empty() && aliases.len() != output.len() {
        return Err(format!(
            "view column list has {} names but the SELECT produces {} columns",
            aliases.len(),
            output.len()
        ));
    }
    output
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let name = if aliases.is_empty() {
                column.name.clone()
            } else {
                aliases[idx].name.value.clone()
            };
            let data_type =
                crate::engine::iceberg_ctas::arrow_data_type_to_sql_type(&column.data_type)?;
            Ok(TableColumnDef {
                name,
                data_type,
                nullable: column.nullable,
                aggregation: None,
                default: None,
            })
        })
        .collect()
}
```

(`build_analyzer_provider` 借用签名以 `src/engine/mod.rs:123` 实际为准;若快照参数按引用/值不同,按编译报错调整。)

- [ ] **Step 4: 编译 + lib 测试**

Run: `cargo test --lib engine`
Expected: PASS(e2e 行为在 Task 11 验证)。

- [ ] **Step 5: Commit**

```bash
git add src/engine/statistics.rs src/engine/iceberg_view.rs
git commit -m "feat(engine): route CREATE [OR REPLACE] VIEW to iceberg REST catalogs

3-part names and active-session-catalog names persist views through the
REST backend (schema derived by analyzing the body against the view's
own catalog/namespace); default-catalog names keep the session-view
behaviour. CREATE OR REPLACE commits a new view version instead of
drop+create."
```

---

### Task 9: DROP VIEW 路由 + DROP TABLE 严格类型检查

**Files:**
- Modify: `src/engine/statistics.rs`(`handle_drop_view` :2279)
- Modify: `src/engine/iceberg_view.rs`(drop 流程)
- Modify: `src/engine/statement.rs`(`execute_drop_table_statement` :1142 的最终 Err 分支)

- [ ] **Step 1: `handle_drop_view` 路由**

签名加 `current_catalog: Option<&str>`(调用点 :113 同步传参);模式绑定补 `if_exists`:

```rust
    let sqlparser::ast::Statement::Drop {
        object_type: sqlparser::ast::ObjectType::View,
        names,
        if_exists,
        ..
    } = stmt
```

循环体改为:

```rust
    for name in names {
        if let Some(target) = crate::engine::iceberg_view::resolve_iceberg_view_target(
            state,
            &name,
            current_catalog,
            current_database,
        )? {
            crate::engine::iceberg_view::drop_iceberg_view(state, &target, if_exists)?;
            continue;
        }
        let (db, view) = view_name_parts(&name, current_database)?;
        let mut views = state
            .views
            .write()
            .map_err(|e| format!("view registry write lock: {e}"))?;
        views.remove(&(db, view));
    }
```

(锁获取移进循环,避免跨 backend 调用持锁。)

- [ ] **Step 2: `iceberg_view.rs` 实现 drop 流程**

```rust
pub(crate) fn drop_iceberg_view(
    state: &Arc<StandaloneState>,
    target: &IcebergViewTarget,
    if_exists: bool,
) -> Result<(), String> {
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend("iceberg")?;
    match backend.drop_view(&target.catalog, &target.namespace, &target.view) {
        Ok(()) => Ok(()),
        Err(err) if err.contains("unknown view") => {
            if if_exists {
                return Ok(());
            }
            if backend.table_exists(&target.catalog, &target.namespace, &target.view)? {
                return Err(format!(
                    "{}.{}.{} is a table, use DROP TABLE",
                    target.catalog, target.namespace, target.view
                ));
            }
            Err(err)
        }
        Err(err) => Err(err),
    }
}
```

- [ ] **Step 3: DROP TABLE 碰到 view 时报错**

`src/engine/statement.rs` `execute_drop_table_statement` 的最终分支(:1142)`Err(err) => Err(err),` 替换为:

```rust
        Err(err) => {
            // A DROP TABLE aimed at a view must say so instead of "unknown
            // table" — views and tables are separate REST resources.
            if target.backend_name == "iceberg"
                && backend
                    .view_exists(&target.catalog, &target.namespace, &target.table)
                    .unwrap_or(false)
            {
                return Err(format!(
                    "{}.{}.{} is a view, use DROP VIEW",
                    target.catalog, target.namespace, target.table
                ));
            }
            Err(err)
        }
```

- [ ] **Step 4: 编译 + lib 测试**

Run: `cargo test --lib engine`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/engine/statistics.rs src/engine/iceberg_view.rs src/engine/statement.rs
git commit -m "feat(engine): DROP VIEW on iceberg catalogs with strict type checks

DROP VIEW routes to the REST backend (IF EXISTS swallows unknown view);
dropping a table with DROP VIEW or a view with DROP TABLE produces an
explicit type-mismatch error instead of unknown-object noise."
```

---

### Task 10: SHOW CREATE VIEW / SHOW VIEWS

**Files:**
- Modify: `src/engine/statement.rs`(探测 + 解析函数,放在 `parse_show_create_table` :1445 之后)
- Modify: `src/engine/mod.rs`(路由 :810 区 + 两个 handler,放在 `handle_show_create_table` :1254 附近)
- Modify: `src/server/mod.rs`(noop 门 :1041-1047 与 allowlist 门 :1065-1074)

- [ ] **Step 1: 探测与解析**

`statement.rs` 追加:

```rust
/// Detect `SHOW CREATE VIEW <name>` so the server routes it to the engine.
pub(crate) fn looks_like_show_create_view(sql: &str) -> bool {
    let lower = sql.trim_start().to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("show") else {
        return false;
    };
    let rest = rest.trim_start();
    let Some(rest) = rest.strip_prefix("create") else {
        return false;
    };
    rest.trim_start().starts_with("view")
}

/// Detect `SHOW VIEWS [FROM db]`. `SHOW MATERIALIZED VIEWS` does not match
/// because its second token is `materialized`.
pub(crate) fn looks_like_show_views(sql: &str) -> bool {
    let lower = sql.trim_start().to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("show") else {
        return false;
    };
    rest.trim_start().starts_with("views")
}

pub(crate) fn parse_show_create_view(
    sql: &str,
) -> Result<crate::sql::parser::ast::ObjectName, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse SHOW CREATE VIEW: {e}"))?;
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|e| format!("parse SHOW CREATE VIEW: {e}"))?;
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(|e| format!("parse SHOW CREATE VIEW: {e}"))?;
    parser
        .expect_keyword(Keyword::VIEW)
        .map_err(|e| format!("parse SHOW CREATE VIEW: {e}"))?;
    let obj = parser
        .parse_object_name(false)
        .map_err(|e| format!("parse SHOW CREATE VIEW view name: {e}"))?;
    crate::sql::parser::dialect::convert_object_name(obj)
}

/// Returns the optional `FROM <db>` database.
pub(crate) fn parse_show_views(sql: &str) -> Result<Option<String>, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse SHOW VIEWS: {e}"))?;
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|e| format!("parse SHOW VIEWS: {e}"))?;
    parser
        .expect_keyword(Keyword::VIEWS)
        .map_err(|e| format!("parse SHOW VIEWS: {e}"))?;
    let database = if parser.parse_keyword(Keyword::FROM) {
        let ident = parser
            .parse_identifier()
            .map_err(|e| format!("parse SHOW VIEWS database after FROM: {e}"))?;
        Some(ident.value)
    } else {
        None
    };
    if parser.parse_keyword(Keyword::LIKE) || parser.parse_keyword(Keyword::WHERE) {
        return Err("SHOW VIEWS LIKE/WHERE is not supported".to_string());
    }
    Ok(database)
}
```

(import 区已有 `Parser`/`StarRocksDialect`/`Keyword`,沿用 `parse_show_create_table` 同款。)

- [ ] **Step 2: engine 路由与 handler**

`mod.rs` :810 的 SHOW CREATE TABLE 路由旁追加:

```rust
        if crate::engine::statement::looks_like_show_create_view(&normalized) {
            return self.handle_show_create_view(&normalized, current_catalog, current_database);
        }
        if crate::engine::statement::looks_like_show_views(&normalized) {
            return self.handle_show_views(&normalized, current_catalog, current_database);
        }
```

(顺序:放在 `looks_like_show_create_table` 检查之前或之后均可——探测词不重叠。)

handler(镜像 `handle_show_create_table` 的结果构造,:1254-1318):

```rust
    fn handle_show_create_view(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let view_name = crate::engine::statement::parse_show_create_view(sql)?;
        let Some(target) = crate::engine::iceberg_view::resolve_iceberg_view_target_parts(
            &self.inner,
            &view_name.parts,
            current_catalog,
            current_database,
        )?
        else {
            return Err(
                "SHOW CREATE VIEW only supports views in iceberg catalogs".to_string(),
            );
        };
        let backend = self
            .inner
            .connectors
            .read()
            .expect("connector registry read")
            .catalog_backend("iceberg")?;
        let view = backend.load_view(&target.catalog, &target.namespace, &target.view)?;

        let columns = view
            .column_names
            .iter()
            .map(|name| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut ddl = format!(
            "CREATE VIEW `{}`.`{}`.`{}` ({})",
            target.catalog, target.namespace, target.view, columns
        );
        if let Some(comment) = &view.comment {
            ddl.push_str(&format!("\nCOMMENT \"{}\"", comment.replace('"', "\\\"")));
        }
        ddl.push_str(&format!("\nAS {};", view.sql));

        let fields = vec![
            Field::new("View", DataType::Utf8, false),
            Field::new("Create View", DataType::Utf8, false),
        ];
        let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(StringArray::from(vec![target.view.clone()])),
            Arc::new(StringArray::from(vec![ddl])),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| format!("build SHOW CREATE VIEW result failed: {e}"))?;
        Ok(StatementResult::Query(QueryResult {
            columns: vec![
                QueryResultColumn {
                    name: "View".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                },
                QueryResultColumn {
                    name: "Create View".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                },
            ],
            chunks: vec![record_batch_to_chunk(batch)?],
        }))
    }

    fn handle_show_views(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let from_db = crate::engine::statement::parse_show_views(sql)?;
        let db = from_db.as_deref().unwrap_or(current_database);
        let session_catalog = current_catalog
            .filter(|catalog| !catalog.eq_ignore_ascii_case("default_catalog"));
        let names: Vec<String> = match session_catalog {
            Some(catalog) => {
                let backend = self
                    .inner
                    .connectors
                    .read()
                    .expect("connector registry read")
                    .catalog_backend("iceberg")?;
                backend.list_views(catalog, db)?
            }
            None => {
                let views = self
                    .inner
                    .views
                    .read()
                    .map_err(|e| format!("view registry read lock: {e}"))?;
                let db_lower = db.to_ascii_lowercase();
                let mut names: Vec<String> = views
                    .keys()
                    .filter(|(database, _)| database == &db_lower)
                    .map(|(_, view)| view.clone())
                    .collect();
                names.sort();
                names
            }
        };
        let column_name = format!("Views_in_{db}");
        let fields = vec![Field::new(column_name.clone(), DataType::Utf8, false)];
        let arrays: Vec<Arc<dyn arrow::array::Array>> =
            vec![Arc::new(StringArray::from(names))];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| format!("build SHOW VIEWS result failed: {e}"))?;
        Ok(StatementResult::Query(QueryResult {
            columns: vec![QueryResultColumn {
                name: column_name,
                data_type: DataType::Utf8,
                nullable: false,
                logical_type: None,
            }],
            chunks: vec![record_batch_to_chunk(batch)?],
        }))
    }
```

(import 以 `handle_show_create_table` 现状为准——`Field`/`DataType`/`StringArray`/`RecordBatch`/`Schema` 已在该文件可用。)

- [ ] **Step 3: server 放行**

`src/server/mod.rs` 两处门各追加两个条件(与 `looks_like_show_create_table` 同样的调用风格):

```rust
            && !crate::engine::statement::looks_like_show_create_view(trimmed)
            && !crate::engine::statement::looks_like_show_views(trimmed)
```

(:1041-1047 的 noop 门用 `trimmed`,:1065-1074 的 allowlist 门用 `rewritten`——以两处现有变量名为准。)

- [ ] **Step 4: 编译 + lib 测试**

Run: `cargo test --lib`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/engine/statement.rs src/engine/mod.rs src/server/mod.rs
git commit -m "feat(engine): SHOW CREATE VIEW and SHOW VIEWS

SHOW CREATE VIEW reconstructs the DDL from REST view metadata; SHOW
VIEWS lists REST views for the active catalog and falls back to session
views on the default catalog. Both escape the server's SHOW noop gate."
```

---

### Task 11: fixture 开关 + iceberg-rest 套件 e2e 用例

**Files:**
- Modify: `docker/iceberg-rest/compose.yml`(rest 服务 env)
- Create: `sql-tests/iceberg-rest/sql/iceberg_rest_view_ddl.sql` + `result/iceberg_rest_view_ddl.result`
- Create: `sql-tests/iceberg-rest/sql/iceberg_rest_view_select.sql` + `result/iceberg_rest_view_select.result`
- Create: `sql-tests/iceberg-rest/sql/iceberg_rest_view_show.sql` + `result/iceberg_rest_view_show.result`
- Modify: `sql-tests/iceberg-rest/README.md`(coverage 表追加三行)

- [ ] **Step 1: compose.yml 启用 JdbcCatalog view 支持**

`rest` 服务 `environment:` 段(`CATALOG_URI` 之后)追加:

```yaml
      # JdbcCatalog only exposes the Iceberg view endpoints on the V1 JDBC
      # schema; the default V0 layout has no view tables.
      CATALOG_JDBC_SCHEMA__VERSION: V1
```

- [ ] **Step 2: 重启共享 REST 服务**

```bash
docker/iceberg-rest/up.sh
curl -s "http://127.0.0.1:${NOVA_ENV_REST_PORT:-8181}/v1/config" | head -c 200
```

Expected: `up.sh` 重建 rest 容器(compose config hash 变化);curl 返回 JSON。
**注意**:共享 Docker 工程,重启会清掉 fixture 内的临时 catalog 状态,其他 worktree 正在跑的 REST 测试会受影响——执行前确认没有并行任务。

- [ ] **Step 3: 启动 standalone-server(后续步骤复用)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo build --profile dev-opt
LOG=/tmp/novarocks-view-e2e.log
NO_PROXY=127.0.0.1,localhost target/dev-opt/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { tail -20 "$LOG"; exit 1; }
  sleep 1
done
grep '^NOVAROCKS_READY ' "$LOG"
```

- [ ] **Step 4: 写 `iceberg_rest_view_ddl.sql`**

```sql
-- @order_sensitive=true
-- Validate REST view CRUD: create / duplicate-create error / IF NOT EXISTS
-- no-op / OR REPLACE re-definition / strict DROP type separation /
-- DROP [IF EXISTS].

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.view_ddl_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.base_${uuid0} (
  id BIGINT,
  name STRING,
  amount DOUBLE
);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.base_${uuid0} VALUES
  (1, 'a', 1.5), (2, 'b', 2.5), (3, 'c', 3.5);

-- query 4
-- @skip_result_check=true
CREATE VIEW iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0} AS
SELECT id, name FROM iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.base_${uuid0}
WHERE amount > 2.0;

-- query 5
SELECT * FROM iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0} ORDER BY id;

-- query 6
-- @expect_error=view already exists
CREATE VIEW iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0} AS
SELECT id FROM iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.base_${uuid0};

-- query 7
-- @skip_result_check=true
-- IF NOT EXISTS on an existing view is a no-op: definition must not change.
CREATE VIEW IF NOT EXISTS iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0} AS
SELECT id FROM iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.base_${uuid0};

-- query 8
SELECT * FROM iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0} ORDER BY id;

-- query 9
-- @skip_result_check=true
CREATE OR REPLACE VIEW iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0} AS
SELECT id, amount FROM iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.base_${uuid0};

-- query 10
SELECT * FROM iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0} ORDER BY id;

-- query 11
-- @expect_error=is a view, use DROP VIEW
DROP TABLE iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0};

-- query 12
-- @expect_error=is a table, use DROP TABLE
DROP VIEW iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.base_${uuid0};

-- query 13
-- @expect_error=a table named
CREATE VIEW iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.base_${uuid0} AS
SELECT 1;

-- query 14
-- @skip_result_check=true
DROP VIEW iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0};

-- query 15
-- @expect_error=unknown view
DROP VIEW iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0};

-- query 16
-- @skip_result_check=true
DROP VIEW IF EXISTS iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.v_basic_${uuid0};

-- query 17
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.view_ddl_${uuid0}.base_${uuid0};

-- query 18
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.view_ddl_${uuid0};
```

- [ ] **Step 5: 写 `iceberg_rest_view_select.sql`**

```sql
-- @order_sensitive=true
-- Validate SELECT-time view expansion: views over views, bare-name
-- resolution via stored default-namespace, and cycle detection.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.view_sel_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.view_sel_${uuid0}.base_${uuid0} (
  id BIGINT,
  region STRING,
  amount DOUBLE
);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.view_sel_${uuid0}.base_${uuid0} VALUES
  (1, 'asia', 10.0), (2, 'emea', 20.0), (3, 'asia', 30.0);

-- query 4
-- @skip_result_check=true
SET CATALOG iceberg_rest_${suite_uuid0};

-- query 5
-- @skip_result_check=true
USE view_sel_${uuid0};

-- query 6
-- @skip_result_check=true
-- Bare table name in the body: stored as-is, resolved via the view's
-- default-namespace at read time.
CREATE VIEW v_bare_${uuid0} AS
SELECT id, region, amount FROM base_${uuid0} WHERE region = 'asia';

-- query 7
-- @skip_result_check=true
-- View over view, also with a bare reference.
CREATE VIEW v_nested_${uuid0} AS
SELECT region, SUM(amount) AS total FROM v_bare_${uuid0} GROUP BY region;

-- query 8
SELECT * FROM iceberg_rest_${suite_uuid0}.view_sel_${uuid0}.v_bare_${uuid0} ORDER BY id;

-- query 9
SELECT * FROM iceberg_rest_${suite_uuid0}.view_sel_${uuid0}.v_nested_${uuid0};

-- query 10
-- @skip_result_check=true
-- Build a cycle through OR REPLACE: v_bare now reads v_nested, which
-- still reads (the old) v_bare.
CREATE OR REPLACE VIEW v_bare_${uuid0} AS
SELECT region, total AS amount, 0 AS id FROM v_nested_${uuid0};

-- query 11
-- @expect_error=circular view reference
SELECT * FROM iceberg_rest_${suite_uuid0}.view_sel_${uuid0}.v_bare_${uuid0};

-- query 12
-- @skip_result_check=true
DROP VIEW v_nested_${uuid0};

-- query 13
-- @skip_result_check=true
DROP VIEW v_bare_${uuid0};

-- query 14
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.view_sel_${uuid0}.base_${uuid0};

-- query 15
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.view_sel_${uuid0};
```

注意 query 10 的 OR REPLACE 创建时分析:展开 `v_nested` → 其 body 引用旧 `v_bare`(老定义,无环)→ 分析通过;query 11 运行时形成 v_bare→v_nested→v_bare 环,命中循环检测。若 query 10 的创建时分析就报循环(实现差异),把 query 10 改加 `-- @expect_error=circular view reference` 并删 query 11,golden 重录。

- [ ] **Step 6: 写 `iceberg_rest_view_show.sql`**

```sql
-- @order_sensitive=true
-- SHOW CREATE VIEW reconstructs the DDL; SHOW VIEWS lists the namespace.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.view_show_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.view_show_${uuid0}.base_${uuid0} (id BIGINT, name STRING);

-- query 3
-- @skip_result_check=true
CREATE VIEW iceberg_rest_${suite_uuid0}.view_show_${uuid0}.v_show_${uuid0} AS
SELECT id, name FROM iceberg_rest_${suite_uuid0}.view_show_${uuid0}.base_${uuid0};

-- query 4
-- @result_contains=CREATE VIEW
-- @result_contains=v_show_${uuid0}
-- @result_contains=AS SELECT
SHOW CREATE VIEW iceberg_rest_${suite_uuid0}.view_show_${uuid0}.v_show_${uuid0};

-- query 5
-- @skip_result_check=true
SET CATALOG iceberg_rest_${suite_uuid0};

-- query 6
-- @result_contains=v_show_${uuid0}
SHOW VIEWS FROM view_show_${uuid0};

-- query 7
-- @skip_result_check=true
DROP VIEW iceberg_rest_${suite_uuid0}.view_show_${uuid0}.v_show_${uuid0};

-- query 8
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.view_show_${uuid0}.base_${uuid0};

-- query 9
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.view_show_${uuid0};
```

- [ ] **Step 7: 录制 golden 并验证**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_view_ddl,iceberg_rest_view_select,iceberg_rest_view_show \
  --mode record --record-from target
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Expected: record 生成三个 `.result`(人工检查内容合理:query 5/8 两行、query 10 三行带 amount 列等);verify 全套 PASS(确认未破坏既有 case)。

- [ ] **Step 8: README coverage 表追加**

`sql-tests/iceberg-rest/README.md` 的表格追加:

```markdown
| `iceberg_rest_view_ddl` | REST view CRUD: create / IF NOT EXISTS / OR REPLACE / drop / strict DROP type checks |
| `iceberg_rest_view_select` | SELECT through views, nested views, bare-name default-namespace resolution, cycle detection |
| `iceberg_rest_view_show` | SHOW CREATE VIEW / SHOW VIEWS |
```

- [ ] **Step 9: Commit**

```bash
git add docker/iceberg-rest/compose.yml sql-tests/iceberg-rest
git commit -m "test(iceberg-rest): view CRUD, SELECT expansion and SHOW e2e cases

Enables the JdbcCatalog V1 schema on the REST fixture so the view
endpoints exist, and adds three suite cases covering DDL, query-time
expansion (nested views, bare-name resolution, cycle detection) and
SHOW statements."
```

---

### Task 12: iceberg-compatibility 跨引擎用例(Spark ↔ NovaRocks)

**Files:**
- Create: `sql-tests/iceberg-compatibility/sql/spark_rest_view_read.sql` + result
- Create: `sql-tests/iceberg-compatibility/sql/novarocks_view_spark_read.sql` + result

- [ ] **Step 1: Spark 建 view → NovaRocks 读**

`spark_rest_view_read.sql`(模式照抄 `spark_rest_minio_v3_read.sql`):

```sql
-- @order_sensitive=true
-- @sequential=true
-- Spark creates an Iceberg view through the REST catalog; NovaRocks reads
-- it via the spark-dialect representation fallback.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-view-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_view_${suite_uuid0};

USE ice_rest.nr_view_${suite_uuid0};

DROP TABLE IF EXISTS sv_base_${uuid0};

CREATE TABLE sv_base_${uuid0} (
  id BIGINT,
  data STRING,
  metric INT
) USING iceberg;

INSERT INTO sv_base_${uuid0} VALUES
  (1, 'a', 10), (2, 'b', 20), (3, 'c', 30);

CREATE OR REPLACE VIEW sv_view_${uuid0} AS
SELECT id, data FROM sv_base_${uuid0} WHERE metric >= 20;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT id, data
FROM iceberg_compat_${suite_uuid0}.nr_view_${suite_uuid0}.sv_view_${uuid0}
ORDER BY id;

-- query 3
-- @result_contains=CREATE VIEW
-- @result_contains=sv_view_${uuid0}
SHOW CREATE VIEW iceberg_compat_${suite_uuid0}.nr_view_${suite_uuid0}.sv_view_${uuid0};

-- query 4
-- @skip_result_check=true
DROP VIEW iceberg_compat_${suite_uuid0}.nr_view_${suite_uuid0}.sv_view_${uuid0};

-- query 5
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.nr_view_${suite_uuid0}.sv_base_${uuid0} FORCE;
```

注意:Spark 建 view 时 body 用裸名(`USE` 设定 namespace),NovaRocks 读取时按 default-namespace 限定;NovaRocks 解析 spark dialect 的简单 SELECT(列引用 + WHERE)可以通过 StarRocksDialect。

- [ ] **Step 2: NovaRocks 建 view → Spark 读**

`novarocks_view_spark_read.sql`:

```sql
-- @order_sensitive=true
-- @sequential=true
-- NovaRocks creates an Iceberg view (starrocks-dialect representation);
-- Spark reads it via its own dialect fallback.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_compat_${suite_uuid0}.nrv_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_compat_${suite_uuid0}.nrv_${uuid0}.nv_base_${uuid0} (
  id BIGINT,
  data STRING
);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_compat_${suite_uuid0}.nrv_${uuid0}.nv_base_${uuid0} VALUES
  (1, 'x'), (2, 'y'), (3, 'z');

-- query 4
-- @skip_result_check=true
SET CATALOG iceberg_compat_${suite_uuid0};

-- query 5
-- @skip_result_check=true
USE nrv_${uuid0};

-- query 6
-- @skip_result_check=true
CREATE VIEW nv_view_${uuid0} AS
SELECT id, data FROM nv_base_${uuid0} WHERE id >= 2;

-- query 7
-- @result_contains=2	y
-- @result_contains=3	z
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-view-read-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
SELECT id, data FROM ice_rest.nrv_${uuid0}.nv_view_${uuid0} ORDER BY id;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"

-- query 8
-- @skip_result_check=true
DROP VIEW iceberg_compat_${suite_uuid0}.nrv_${uuid0}.nv_view_${uuid0};

-- query 9
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.nrv_${uuid0}.nv_base_${uuid0};

-- query 10
-- @skip_result_check=true
DROP DATABASE iceberg_compat_${suite_uuid0}.nrv_${uuid0};
```

注意 query 7 的 `@result_contains` 值中是字面 TAB(spark-sql 输出 tab 分隔);若运行发现 Spark 输出格式不同,按实际输出调整断言。若 Spark 因 default-catalog 为空无法解析 view(报 catalog 解析错),记录失败信息并把 query 7 断言降级为 `-- @expect_error=` + 实际错误,同时在 PR 描述里注明 Spark 侧限制——不要静默删除用例。

- [ ] **Step 3: 录制并验证**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --only spark_rest_view_read,novarocks_view_spark_read \
  --mode record --record-from target
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```

Expected: 两个新 case PASS,既有 case 不回归。

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg-compatibility
git commit -m "test(iceberg-compatibility): cross-engine view cases

Spark-created views read by NovaRocks (dialect fallback) and
NovaRocks-created views read by Spark."
```

---

### Task 13: 收尾质量门

**Files:** 无新文件;全仓验证。

- [ ] **Step 1: 格式与静态检查**

```bash
cargo fmt
cargo clippy --profile dev-opt 2>&1 | tail -20
```

Expected: fmt 无 diff(或仅本分支文件);clippy 无新增 warning(若有,修复后 amend 对应 commit 或单独小 commit)。

- [ ] **Step 2: 全量单测**

Run: `cargo test --lib`
Expected: PASS。

- [ ] **Step 3: 三个相关 SQL 套件回归**

```bash
source docker/iceberg-rest/runtime/current/env.sh
# server 若已停,按 Task 11 Step 3 重启
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-rest --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-compatibility --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg --mode verify
```

Expected: 三套全 PASS(`iceberg` 套件确认 Task 1 删除与展开 hook 未影响 hadoop 路径)。

- [ ] **Step 4: 收尾 commit(如有零散修复)**

```bash
git add -A
git commit -m "chore: post-implementation cleanup for iceberg REST views"
```

(若 Step 1-3 没有产生改动则跳过。)

---

## 计划自检记录

- **Spec 覆盖**:工作项 A→Task 1;vendored 两 crate→Task 2-4;registry/backend→Task 5-6;CREATE [OR REPLACE]/IF NOT EXISTS→Task 8;DROP [IF EXISTS]+严格类型检查→Task 9;SELECT 展开(嵌套/裸名限定/循环检测/dialect 回退)→Task 7+11;SHOW 两语句→Task 10;fixture V1 开关→Task 11;Spark 互通→Task 12;非 REST catalog 报错→Task 5(`catalog_for_views`)+ mockito 测试;无缓存→设计即如此(views.rs 文件头注明)。
- **类型一致性**:`LoadedIcebergView`(registry)→`ResolvedView`(backend)字段一一对应;`CreateViewRequest` 两层(backend 层与 REST 报文层)同名但分属 `src/connector/backend.rs` 与 vendored rest crate,互不引用;`resolve_iceberg_view_target[_parts]` 的两个变体在 Task 7 定义、Task 8/9/10 引用,签名一致。
- **已知不确定点(实施时按编译报错微调,不影响结构)**:vendored crate 内部 API 细节(`Schema::as_struct().fields()`、`Error::with_retryable`、builder 借用形式)、`build_analyzer_provider` 形参借用方式、Spark 读 NovaRocks view 的 default-catalog 行为(Task 12 Step 2 已写明降级路径)。
