use crate::sql::catalog::{ColumnDef, IcebergMvTargetStateScan, ScanSource};

pub(crate) fn build_target_state_scan_source(
    catalog: String,
    database: String,
    table: String,
    columns: Vec<ColumnDef>,
    group_key_names: Vec<String>,
    aggregate_state_names: Vec<String>,
) -> ScanSource {
    ScanSource::IcebergMvTargetState(IcebergMvTargetStateScan {
        catalog,
        database,
        table,
        columns,
        group_key_names,
        aggregate_state_names,
    })
}
