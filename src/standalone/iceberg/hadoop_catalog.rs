/// HadoopFileSystemCatalog — a Hadoop-catalog-compatible implementation of the
/// iceberg `Catalog` trait.
///
/// Differences from `MemoryCatalog`:
/// - Metadata files are written as `v{N}.metadata.json` (Hadoop convention).
/// - `version-hint.text` is maintained alongside each metadata directory so
///   that StarRocks FE, Spark, and Trino can discover the current version.
/// - `update_table` manually applies requirements/updates instead of delegating
///   to `TableCommit::apply()`, which calls `MetadataLocation::from_str()` and
///   only accepts the `{version}-{uuid}.metadata.json` format.
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use tokio::sync::Mutex;

#[derive(Debug)]
pub(crate) struct HadoopFileSystemCatalog {
    file_io: FileIO,
    warehouse_location: String,
    /// Maps `"namespace/table"` to the current metadata file location.
    tables: Mutex<HashMap<String, String>>,
}

impl HadoopFileSystemCatalog {
    /// Create a new catalog backed by `file_io` writing under `warehouse_location`.
    pub(crate) fn new(file_io: FileIO, warehouse_location: String) -> Self {
        Self {
            file_io,
            warehouse_location: warehouse_location.trim_end_matches('/').to_string(),
            tables: Mutex::new(HashMap::new()),
        }
    }

    // -----------------------------------------------------------------------
    // Path helpers (pub(crate) for unit tests)
    // -----------------------------------------------------------------------

    /// Returns the table root location derived from the warehouse location and
    /// the table identifier, e.g. `oss://bucket/warehouse/ns/table`.
    pub(crate) fn table_location(&self, ident: &TableIdent) -> String {
        let namespace = ident.namespace().join("/");
        format!("{}/{}/{}", self.warehouse_location, namespace, ident.name())
    }

    /// Returns the path to the `vN.metadata.json` file for a given table location
    /// and version number.
    pub(crate) fn metadata_path(table_location: &str, version: u32) -> String {
        let base = table_location.trim_end_matches('/');
        format!("{}/metadata/v{}.metadata.json", base, version)
    }

    /// Returns the path to the `version-hint.text` file for a given table location.
    pub(crate) fn version_hint_path(table_location: &str) -> String {
        let base = table_location.trim_end_matches('/');
        format!("{}/metadata/version-hint.text", base)
    }

    /// Read the current version stored in `version-hint.text`. Returns `0` if
    /// the file does not exist or cannot be parsed.
    async fn read_version_hint(&self, table_location: &str) -> u32 {
        let path = Self::version_hint_path(table_location);
        let Ok(input) = self.file_io.new_input(&path) else {
            return 0;
        };
        let Ok(bytes) = input.read().await else {
            return 0;
        };
        let s = String::from_utf8_lossy(&bytes);
        s.trim().parse::<u32>().unwrap_or(0)
    }

    /// Write `version-hint.text` with the given version number.
    async fn write_version_hint(&self, table_location: &str, version: u32) -> Result<()> {
        let path = Self::version_hint_path(table_location);
        let output = self.file_io.new_output(&path)?;
        output.write(format!("{}\n", version).into()).await
    }

    /// Persist table metadata at `v{version}.metadata.json` and update
    /// `version-hint.text`.
    async fn write_metadata(
        &self,
        table_location: &str,
        metadata: &TableMetadata,
        version: u32,
    ) -> Result<String> {
        let metadata_path = Self::metadata_path(table_location, version);
        metadata
            .write_to(&self.file_io, &metadata_path)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("write metadata to {}: {}", metadata_path, e),
                )
            })?;
        self.write_version_hint(table_location, version).await?;
        Ok(metadata_path)
    }

    /// Build a `Table` value from metadata and a metadata location.
    fn build_table(
        &self,
        ident: TableIdent,
        metadata: TableMetadata,
        metadata_location: String,
    ) -> Result<Table> {
        Table::builder()
            .file_io(self.file_io.clone())
            .metadata(Arc::new(metadata))
            .identifier(ident)
            .metadata_location(metadata_location)
            .build()
    }

    /// Return the table key used as the key in the `tables` map.
    fn table_key(ident: &TableIdent) -> String {
        let namespace = ident.namespace().join("/");
        format!("{}/{}", namespace, ident.name())
    }
}

#[async_trait]
impl Catalog for HadoopFileSystemCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        Ok(vec![])
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        Ok(Namespace::new(namespace.clone()))
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
        Ok(true)
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Ok(())
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        Ok(())
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Ok(vec![])
    }

    /// Create a table: write `v1.metadata.json` and `version-hint.text=1`.
    ///
    /// If `creation.location` is `None` the table location is inferred from
    /// the warehouse location and the table identifier.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let table_location = creation
            .location
            .clone()
            .unwrap_or_else(|| self.table_location(&ident));

        // Inject the location into the creation so the builder can use it.
        let creation_with_location = TableCreation {
            location: Some(table_location.clone()),
            ..creation
        };

        let build_result = TableMetadataBuilder::from_table_creation(creation_with_location)
            .map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("build metadata from creation: {}", e),
                )
            })?
            .build()
            .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("build metadata: {}", e)))?;

        let metadata = build_result.metadata;
        let metadata_location = self.write_metadata(&table_location, &metadata, 1).await?;

        let key = Self::table_key(&ident);
        self.tables
            .lock()
            .await
            .insert(key, metadata_location.clone());

        self.build_table(ident, metadata, metadata_location)
    }

    /// Load a table from its registered metadata location.
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let key = Self::table_key(table);
        let metadata_location = {
            let guard = self.tables.lock().await;
            guard.get(&key).cloned().ok_or_else(|| {
                Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("table not found: {}", key),
                )
            })?
        };

        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("read metadata from {}: {}", metadata_location, e),
                )
            })?;

        self.build_table(table.clone(), metadata, metadata_location)
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let key = Self::table_key(table);
        self.tables.lock().await.remove(&key);
        Ok(())
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        let key = Self::table_key(table);
        Ok(self.tables.lock().await.contains_key(&key))
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let src_key = Self::table_key(src);
        let dest_key = Self::table_key(dest);
        let mut guard = self.tables.lock().await;
        if let Some(loc) = guard.remove(&src_key) {
            guard.insert(dest_key, loc);
        }
        Ok(())
    }

    /// Register an existing table that already has metadata written at
    /// `metadata_location`.
    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("read metadata from {}: {}", metadata_location, e),
                )
            })?;

        let key = Self::table_key(table);
        self.tables
            .lock()
            .await
            .insert(key, metadata_location.clone());

        self.build_table(table.clone(), metadata, metadata_location)
    }

    /// Apply a table commit (requirements + updates) and write a new versioned
    /// metadata file.
    ///
    /// This method bypasses `TableCommit::apply()` which internally calls
    /// `MetadataLocation::from_str()`. That function rejects the Hadoop
    /// `vN.metadata.json` naming convention, so we manually apply requirements
    /// and updates here.
    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let ident = commit.identifier().clone();

        // Load the current metadata.
        let current_table = self.load_table(&ident).await?;
        let current_metadata_location = current_table
            .metadata_location()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "no metadata location for table: {}",
                        Self::table_key(&ident)
                    ),
                )
            })?
            .to_string();
        let current_metadata = current_table.metadata();

        // Check all requirements against the current metadata.
        for requirement in commit.take_requirements() {
            requirement.check(Some(current_metadata))?;
        }

        // Apply all updates to produce new metadata.
        let mut builder = current_metadata
            .clone()
            .into_builder(Some(current_metadata_location));
        for update in commit.take_updates() {
            builder = update.apply(builder)?;
        }
        let new_metadata = builder.build()?.metadata;

        // Determine the next version number.
        let table_location = current_metadata.location().to_string();
        let current_version = self.read_version_hint(&table_location).await;
        let next_version = current_version + 1;

        // Write the new metadata and update version-hint.text.
        let new_metadata_location = self
            .write_metadata(&table_location, &new_metadata, next_version)
            .await?;

        // Update the in-memory registry.
        let key = Self::table_key(&ident);
        self.tables
            .lock()
            .await
            .insert(key, new_metadata_location.clone());

        self.build_table(ident, new_metadata, new_metadata_location)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_path() {
        assert_eq!(
            HadoopFileSystemCatalog::metadata_path("oss://bucket/warehouse/db/tbl", 1),
            "oss://bucket/warehouse/db/tbl/metadata/v1.metadata.json"
        );
        assert_eq!(
            HadoopFileSystemCatalog::metadata_path("file:///tmp/wh/db/tbl", 3),
            "file:///tmp/wh/db/tbl/metadata/v3.metadata.json"
        );
    }

    #[test]
    fn test_version_hint_path() {
        assert_eq!(
            HadoopFileSystemCatalog::version_hint_path("oss://bucket/warehouse/db/tbl"),
            "oss://bucket/warehouse/db/tbl/metadata/version-hint.text"
        );
    }

    #[test]
    fn test_table_location() {
        let file_io = iceberg::io::FileIO::new_with_memory();
        let catalog = HadoopFileSystemCatalog::new(file_io, "oss://bucket/warehouse".to_string());
        let ident = TableIdent::from_strs(["ns1", "my_table"]).unwrap();
        assert_eq!(
            catalog.table_location(&ident),
            "oss://bucket/warehouse/ns1/my_table"
        );
    }
}
