use std::any::Any;

use crate::connector::scan_planning::{
    ConnectorScanHandle, ConnectorSplit, ConnectorTableHandle, ScanHandle, Split,
};
use crate::sql::catalog::{IcebergDataFileInfo, IcebergTableInfo};

const CONNECTOR_ID: &str = "iceberg";

#[derive(Clone, Debug)]
pub(crate) struct IcebergTableHandle {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) table_info: IcebergTableInfo,
    pub(crate) files: Vec<IcebergDataFileInfo>,
}

impl ConnectorTableHandle for IcebergTableHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergScanHandle {
    pub(crate) table: IcebergTableHandle,
}

impl ConnectorScanHandle for IcebergScanHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergSplit {
    pub(crate) data_file: IcebergDataFileInfo,
}

impl ConnectorSplit for IcebergSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) fn iceberg_scan_handle(scan: &ScanHandle) -> Result<&IcebergScanHandle, String> {
    scan.downcast_ref::<IcebergScanHandle>()
        .ok_or_else(|| "expected IcebergScanHandle for iceberg scan".to_string())
}

pub(crate) fn iceberg_split(split: &Split) -> Result<&IcebergSplit, String> {
    split
        .downcast_ref::<IcebergSplit>()
        .ok_or_else(|| "expected IcebergSplit for iceberg split".to_string())
}

use crate::connector::scan_planning::{
    BeginScanContext, ConnectorScanPlanner, SplitPlanningContext, TableHandle, ThriftScanContext,
    ThriftScanPlan, validate_split_connectors,
};

#[derive(Debug, Default)]
pub(crate) struct IcebergConnectorScanPlanner;

impl IcebergConnectorScanPlanner {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn table_handle_from_source(
        catalog: &str,
        namespace: &str,
        table: &str,
        snapshot_id: Option<i64>,
        table_info: IcebergTableInfo,
        files: Vec<IcebergDataFileInfo>,
    ) -> TableHandle {
        TableHandle::new(
            CONNECTOR_ID,
            IcebergTableHandle {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                snapshot_id,
                table_info,
                files,
            },
        )
    }
}

impl ConnectorScanPlanner for IcebergConnectorScanPlanner {
    fn name(&self) -> &'static str {
        CONNECTOR_ID
    }

    fn begin_scan(&self, table: TableHandle, _ctx: BeginScanContext) -> Result<ScanHandle, String> {
        let inner = table
            .downcast_ref::<IcebergTableHandle>()
            .ok_or_else(|| "expected IcebergTableHandle for iceberg scan".to_string())?
            .clone();
        Ok(ScanHandle::new(
            CONNECTOR_ID,
            IcebergScanHandle { table: inner },
        ))
    }

    fn plan_splits(
        &self,
        scan: &ScanHandle,
        _ctx: SplitPlanningContext,
    ) -> Result<Vec<Split>, String> {
        let scan = iceberg_scan_handle(scan)?;
        Ok(scan
            .table
            .files
            .iter()
            .map(|file| {
                Split::new(
                    CONNECTOR_ID,
                    IcebergSplit {
                        data_file: file.clone(),
                    },
                )
            })
            .collect())
    }

    fn to_thrift_scan(
        &self,
        scan: &ScanHandle,
        splits: &[Split],
        _ctx: ThriftScanContext,
    ) -> Result<ThriftScanPlan, String> {
        validate_split_connectors(scan, splits)?;
        Err(
            "IcebergConnectorScanPlanner::to_thrift_scan is not yet implemented; \
             codegen still produces HDFS scan ranges via build_hdfs_scan_range_params_for_file"
                .to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::scan_planning::{ScanHandle, Split, validate_split_connectors};
    use crate::sql::catalog::{IcebergSchemaDef, IcebergTableInfo};

    fn dummy_iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "memory".to_string(),
            namespace: "default".to_string(),
            table: "orders".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 1,
            location: String::new(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
        }
    }

    fn dummy_iceberg_file() -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: "s3://bucket/data/file.parquet".to_string(),
            size: 1024,
            row_count: Some(1),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }
    }

    #[test]
    fn downcasts_iceberg_scan_and_split() {
        let table = IcebergTableHandle {
            catalog: "memory".to_string(),
            namespace: "default".to_string(),
            table: "orders".to_string(),
            snapshot_id: Some(42),
            table_info: dummy_iceberg_table_info(),
            files: vec![dummy_iceberg_file()],
        };
        let scan = ScanHandle::new(
            CONNECTOR_ID,
            IcebergScanHandle {
                table: table.clone(),
            },
        );
        let splits = vec![Split::new(
            CONNECTOR_ID,
            IcebergSplit {
                data_file: dummy_iceberg_file(),
            },
        )];

        validate_split_connectors(&scan, &splits).expect("same connector");
        assert_eq!(
            iceberg_scan_handle(&scan).expect("scan").table.table,
            "orders"
        );
        assert_eq!(
            iceberg_split(&splits[0]).expect("split").data_file.path,
            "s3://bucket/data/file.parquet"
        );
    }
}
