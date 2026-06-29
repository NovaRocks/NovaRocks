use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ConnectorId(String);

impl ConnectorId {
    pub(crate) fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&'static str> for ConnectorId {
    fn from(value: &'static str) -> Self {
        Self::new(value)
    }
}

pub(crate) trait ConnectorTableHandle: fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub(crate) trait ConnectorScanHandle: fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub(crate) trait ConnectorSplit: fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Debug)]
pub(crate) struct TableHandle {
    connector_id: ConnectorId,
    handle: Arc<dyn ConnectorTableHandle>,
}

impl TableHandle {
    pub(crate) fn new(
        connector_id: impl Into<ConnectorId>,
        handle: impl ConnectorTableHandle + 'static,
    ) -> Self {
        Self {
            connector_id: connector_id.into(),
            handle: Arc::new(handle),
        }
    }

    pub(crate) fn connector_id(&self) -> &ConnectorId {
        &self.connector_id
    }

    pub(crate) fn downcast_ref<T: ConnectorTableHandle + 'static>(&self) -> Option<&T> {
        self.handle.as_any().downcast_ref::<T>()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScanHandle {
    connector_id: ConnectorId,
    handle: Arc<dyn ConnectorScanHandle>,
}

impl ScanHandle {
    pub(crate) fn new(
        connector_id: impl Into<ConnectorId>,
        handle: impl ConnectorScanHandle + 'static,
    ) -> Self {
        Self {
            connector_id: connector_id.into(),
            handle: Arc::new(handle),
        }
    }

    pub(crate) fn connector_id(&self) -> &ConnectorId {
        &self.connector_id
    }

    pub(crate) fn downcast_ref<T: ConnectorScanHandle + 'static>(&self) -> Option<&T> {
        self.handle.as_any().downcast_ref::<T>()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Split {
    connector_id: ConnectorId,
    handle: Arc<dyn ConnectorSplit>,
}

impl Split {
    pub(crate) fn new(
        connector_id: impl Into<ConnectorId>,
        handle: impl ConnectorSplit + 'static,
    ) -> Self {
        Self {
            connector_id: connector_id.into(),
            handle: Arc::new(handle),
        }
    }

    pub(crate) fn connector_id(&self) -> &ConnectorId {
        &self.connector_id
    }

    pub(crate) fn downcast_ref<T: ConnectorSplit + 'static>(&self) -> Option<&T> {
        self.handle.as_any().downcast_ref::<T>()
    }
}

pub(crate) fn validate_split_connectors(scan: &ScanHandle, splits: &[Split]) -> Result<(), String> {
    for split in splits {
        if split.connector_id() != scan.connector_id() {
            return Err(format!(
                "split connector mismatch: scan connector={} split connector={}",
                scan.connector_id().as_str(),
                split.connector_id().as_str()
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BeginScanContext;

#[derive(Clone, Debug, Default)]
pub(crate) struct SplitPlanningContext;

pub(crate) trait ConnectorScanPlanner: fmt::Debug + Send + Sync {
    fn name(&self) -> &'static str;

    fn begin_scan(&self, table: TableHandle, ctx: BeginScanContext) -> Result<ScanHandle, String>;

    fn plan_splits(
        &self,
        scan: &ScanHandle,
        ctx: SplitPlanningContext,
    ) -> Result<Vec<Split>, String>;
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn validate_splits_rejects_mismatched_connector_ids() {
        let scan = ScanHandle::new("starrocks", DummyScanHandle);
        let splits = vec![Split::new("iceberg", DummySplit)];

        let err = validate_split_connectors(&scan, &splits)
            .expect_err("mismatched split connector must fail");

        assert!(
            err.contains("split connector mismatch"),
            "unexpected error: {err}"
        );
    }
}
