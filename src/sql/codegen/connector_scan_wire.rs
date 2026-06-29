use std::collections::BTreeMap;

use crate::common::min_max_predicate::MinMaxPredicate;
use crate::connector::scan_planning::{ScanHandle, Split, validate_split_connectors};
use crate::thrift::{exprs, internal_service, plan_nodes, types};

#[derive(Clone, Debug, Default)]
pub(crate) struct ThriftScanContext {
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) node_id: i32,
    pub(crate) scan_tuple_id: types::TTupleId,
    pub(crate) conjuncts: Vec<exprs::TExpr>,
    pub(crate) min_max_predicates: Vec<MinMaxPredicate>,
    pub(crate) change_op_slot: Option<types::TSlotId>,
    pub(crate) cloud_properties: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ThriftScanPlan {
    pub(crate) node: Option<plan_nodes::TPlanNode>,
    pub(crate) scan_ranges: Vec<internal_service::TScanRangeParams>,
}

pub(crate) fn to_thrift_scan(
    connector_id: &str,
    scan: &ScanHandle,
    splits: &[Split],
    ctx: ThriftScanContext,
) -> Result<ThriftScanPlan, String> {
    validate_split_connectors(scan, splits)?;
    match connector_id {
        "iceberg" => {
            crate::connector::iceberg::scan_planner::iceberg_to_thrift_scan(scan, splits, ctx)
        }
        "starrocks" => {
            crate::connector::starrocks::table::starrocks_to_thrift_scan(scan, splits, ctx)
        }
        other => Err(format!(
            "unsupported connector scan thrift emitter: {other}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::*;
    use crate::connector::scan_planning::{ConnectorScanHandle, ConnectorSplit};
    use crate::connector::starrocks::table::{
        StarRocksScanHandle, StarRocksSplit, StarRocksTableHandle,
    };

    #[derive(Debug)]
    struct DummyScanHandle;

    impl ConnectorScanHandle for DummyScanHandle {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct DummySplit;

    impl ConnectorSplit for DummySplit {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn to_thrift_scan_rejects_mismatched_splits_before_dispatch() {
        let scan = ScanHandle::new("starrocks", DummyScanHandle);
        let splits = vec![Split::new("iceberg", DummySplit)];

        let err = to_thrift_scan("missing", &scan, &splits, ThriftScanContext::default())
            .expect_err("split validation must run before connector dispatch");

        assert!(
            err.contains("split connector mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn to_thrift_scan_rejects_unknown_connector_id() {
        let scan = ScanHandle::new("missing", DummyScanHandle);

        let err = to_thrift_scan("missing", &scan, &[], ThriftScanContext::default())
            .expect_err("unknown connector must fail");

        assert!(
            err.contains("unsupported connector scan thrift emitter: missing"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn to_thrift_scan_dispatches_starrocks_wire_emission() {
        let scan = ScanHandle::new(
            "starrocks",
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
            "starrocks",
            StarRocksSplit {
                tablet_id: 300,
                partition_id: 100,
                version: 7,
            },
        )];

        let plan = to_thrift_scan(
            "starrocks",
            &scan,
            &splits,
            ThriftScanContext {
                database: "default".to_string(),
                table: "orders".to_string(),
                node_id: 11,
                scan_tuple_id: 1,
                ..ThriftScanContext::default()
            },
        )
        .expect("starrocks thrift scan");

        let node = plan.node.expect("lake scan node");
        assert_eq!(node.node_id, 11);
        assert_eq!(node.node_type, plan_nodes::TPlanNodeType::LAKE_SCAN_NODE);
        assert_eq!(plan.scan_ranges.len(), 1);
        let internal = plan.scan_ranges[0]
            .scan_range
            .internal_scan_range
            .as_ref()
            .expect("internal scan range");
        assert_eq!(internal.tablet_id, 300);
    }
}
