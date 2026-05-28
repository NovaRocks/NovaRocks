use std::any::Any;

use crate::connector::scan_planning::{ConnectorScanHandle, ConnectorSplit, ScanHandle, Split};

const CONNECTOR_ID: &str = "starrocks";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksTableHandle {
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
}

impl crate::connector::scan_planning::ConnectorTableHandle for StarRocksTableHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StarRocksSplit {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) version: i64,
}

impl ConnectorSplit for StarRocksSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StarRocksScanHandle {
    pub(crate) table: StarRocksTableHandle,
    pub(crate) schema_id: i64,
}

impl ConnectorScanHandle for StarRocksScanHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) fn starrocks_scan_handle(scan: &ScanHandle) -> Result<&StarRocksScanHandle, String> {
    scan.downcast_ref::<StarRocksScanHandle>()
        .ok_or_else(|| "expected StarRocksScanHandle for starrocks scan".to_string())
}

pub(crate) fn starrocks_split(split: &Split) -> Result<&StarRocksSplit, String> {
    split
        .downcast_ref::<StarRocksSplit>()
        .ok_or_else(|| "expected StarRocksSplit for starrocks split".to_string())
}

use std::sync::{Arc, Weak};

use crate::connector::scan_planning::{
    BeginScanContext, ConnectorScanPlanner, SplitPlanningContext, TableHandle, ThriftScanContext,
    ThriftScanPlan, validate_split_connectors,
};
use crate::engine::StandaloneState;
use crate::{internal_service, plan_nodes};

#[derive(Debug)]
pub(crate) struct StarRocksTableScanPlanner {
    state: Weak<StandaloneState>,
}

impl StarRocksTableScanPlanner {
    pub(crate) fn new(state: &Arc<StandaloneState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }

    /// Construct a planner instance that does not reference any
    /// `StandaloneState`. Safe to use ONLY from call sites that invoke
    /// methods which never call `self.state()` — currently `to_thrift_scan`.
    /// Adding a state-reading method without also updating call sites here
    /// would panic at the upgrade in `state()`.
    pub(crate) fn stateless_for_codegen() -> Self {
        Self { state: Weak::new() }
    }

    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state
            .upgrade()
            .ok_or_else(|| "standalone state dropped".to_string())
    }

    pub(crate) fn table_handle_from_source(
        database: &str,
        table: &str,
        db_id: i64,
        table_id: i64,
    ) -> TableHandle {
        TableHandle::new(
            CONNECTOR_ID,
            StarRocksTableHandle {
                database: database.to_string(),
                table: table.to_string(),
                db_id,
                table_id,
            },
        )
    }

    fn build_internal_scan_range_params(
        database: &str,
        table: &str,
        schema_id: i64,
        split: &StarRocksSplit,
    ) -> internal_service::TScanRangeParams {
        let internal_scan_range = plan_nodes::TInternalScanRange::new(
            vec![],
            schema_id.to_string(),
            split.version.to_string(),
            split.version.to_string(),
            split.tablet_id,
            database.to_string(),
            None::<Vec<plan_nodes::TKeyRange>>,
            None::<String>,
            Some(table.to_string()),
            Some(split.partition_id),
            None::<i64>,
            Some(true),
            None::<i32>,
            Some(false),
            Some(false),
            None::<i64>,
        );

        internal_service::TScanRangeParams::new(
            plan_nodes::TScanRange::new(
                Some(internal_scan_range),
                None::<Vec<u8>>,
                None::<plan_nodes::TBrokerScanRange>,
                None::<plan_nodes::TEsScanRange>,
                None::<plan_nodes::THdfsScanRange>,
                None::<plan_nodes::TBinlogScanRange>,
                None::<plan_nodes::TBenchmarkScanRange>,
            ),
            None::<i32>,
            Some(false),
            Some(false),
        )
    }
}

impl ConnectorScanPlanner for StarRocksTableScanPlanner {
    fn name(&self) -> &'static str {
        CONNECTOR_ID
    }

    fn begin_scan(&self, table: TableHandle, _ctx: BeginScanContext) -> Result<ScanHandle, String> {
        let table = table
            .downcast_ref::<StarRocksTableHandle>()
            .ok_or_else(|| "expected StarRocksTableHandle for starrocks scan".to_string())?
            .clone();
        let state = self.state()?;
        let catalog = state
            .starrocks_table
            .read()
            .map_err(|e| format!("starrocks table catalog read lock poisoned: {e}"))?;
        let runtime = catalog.table(&table.database, &table.table)?;
        Ok(ScanHandle::new(
            CONNECTOR_ID,
            StarRocksScanHandle {
                table,
                schema_id: runtime.table.current_schema_id,
            },
        ))
    }

    fn plan_splits(
        &self,
        scan: &ScanHandle,
        _ctx: SplitPlanningContext,
    ) -> Result<Vec<Split>, String> {
        let scan = starrocks_scan_handle(scan)?;
        let state = self.state()?;
        let catalog = state
            .starrocks_table
            .read()
            .map_err(|e| format!("starrocks table catalog read lock poisoned: {e}"))?;
        let runtime = catalog.table(&scan.table.database, &scan.table.table)?;
        let layout = super::catalog::starrocks_table_physical_layout(runtime)?;
        Ok(layout
            .tablets
            .into_iter()
            .map(|tablet| {
                Split::new(
                    CONNECTOR_ID,
                    StarRocksSplit {
                        tablet_id: tablet.tablet_id,
                        partition_id: tablet.partition_id,
                        version: tablet.version,
                    },
                )
            })
            .collect())
    }

    fn to_thrift_scan(
        &self,
        scan: &ScanHandle,
        splits: &[Split],
        ctx: ThriftScanContext,
    ) -> Result<ThriftScanPlan, String> {
        validate_split_connectors(scan, splits)?;
        let scan = starrocks_scan_handle(scan)?;
        let scan_ranges = splits
            .iter()
            .map(|split| {
                let split = starrocks_split(split)?;
                Ok(Self::build_internal_scan_range_params(
                    &ctx.database,
                    &ctx.table,
                    scan.schema_id,
                    split,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(ThriftScanPlan {
            node: None,
            scan_ranges,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::scan_planning::{ScanHandle, Split, validate_split_connectors};

    #[test]
    fn downcasts_starrocks_scan_and_split() {
        let scan = ScanHandle::new(
            CONNECTOR_ID,
            StarRocksScanHandle {
                table: StarRocksTableHandle {
                    database: "default".to_string(),
                    table: "orders".to_string(),
                    db_id: 10,
                    table_id: 20,
                },
                schema_id: 30,
            },
        );
        let splits = vec![Split::new(
            CONNECTOR_ID,
            StarRocksSplit {
                tablet_id: 300,
                partition_id: 100,
                version: 7,
            },
        )];

        validate_split_connectors(&scan, &splits).expect("same connector");
        assert_eq!(starrocks_scan_handle(&scan).expect("scan").schema_id, 30);
        assert_eq!(starrocks_split(&splits[0]).expect("split").tablet_id, 300);
    }
}
