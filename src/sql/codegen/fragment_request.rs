use crate::connector::ConnectorRegistry;
use crate::engine::mv::refresh_context::IcebergMvRefreshContext;
use crate::sql::catalog::CatalogProvider;
use crate::sql::planner::DistributedPlan;

pub(crate) struct FragmentBuildRequest<'a> {
    pub distributed_plan: &'a DistributedPlan,
    pub catalog: &'a dyn CatalogProvider,
    pub connectors: &'a ConnectorRegistry,
    pub mv_refresh_ctx: Option<&'a IcebergMvRefreshContext>,
}

impl<'a> FragmentBuildRequest<'a> {
    pub(crate) fn result(
        distributed_plan: &'a DistributedPlan,
        catalog: &'a dyn CatalogProvider,
        connectors: &'a ConnectorRegistry,
        mv_refresh_ctx: Option<&'a IcebergMvRefreshContext>,
    ) -> Self {
        Self {
            distributed_plan,
            catalog,
            connectors,
            mv_refresh_ctx,
        }
    }
}
