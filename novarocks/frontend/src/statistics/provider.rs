use super::model::TableKey;
use super::query::normalize_name;
use super::{CatalogColumnStatistics, CatalogTableStatistics, FrontendStatisticsService};

pub(super) fn catalog_table_statistics(
    service: &FrontendStatisticsService,
    database: &str,
    table: &str,
) -> Result<Option<CatalogTableStatistics>, String> {
    let key = TableKey {
        db: normalize_name(database)?,
        table: normalize_name(table)?,
    };
    let rows = {
        let state = service.state.read().expect("frontend statistics read lock");
        state
            .column_stats
            .iter()
            .filter(|row| row.key == key)
            .cloned()
            .collect::<Vec<_>>()
    };
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(CatalogTableStatistics {
        columns: rows
            .into_iter()
            .map(|row| CatalogColumnStatistics {
                column_name: row.column_name,
                row_count: row.row_count,
                min: row.min,
                max: row.max,
                ndv: row.ndv,
            })
            .collect(),
    }))
}
