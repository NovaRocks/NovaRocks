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

use crate::coordinator::prepare::scan::{
    ResolvedReadColumn, ResolvedReadReason, ResolvedScanColumn, ResolvedScanColumnKind,
};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;

pub(super) fn resolve_physical_columns(
    node_id: i32,
    scan: &PlanScanNode,
) -> Result<Vec<ResolvedScanColumn>, String> {
    if let Some(projected_names) = refresh_scan_projected_names(&scan.table.source) {
        return projected_names
            .into_iter()
            .map(|name| {
                let planner = scan
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&name))
                    .ok_or_else(|| {
                        format!(
                            "scan binding node_id={node_id} cannot resolve projected planner column '{name}' in table '{}'",
                            scan.table.name
                        )
                    })?;
                let (source, kind) = resolved_source_column(scan, &name).ok_or_else(|| {
                    format!(
                        "scan binding node_id={node_id} cannot resolve projected physical column '{name}' in table '{}'",
                        scan.table.name
                    )
                })?;
                Ok(ResolvedScanColumn {
                    planner: planner.clone(),
                    source: source.clone(),
                    kind,
                })
            })
            .collect();
    }

    let keep_only_resolved = matches!(scan.table.source, ScanSource::IcebergVersionTable { .. });
    scan.columns
        .iter()
        .filter(|planner| !is_variant_synthetic_column(scan, planner.column_id))
        .filter_map(|planner| {
            let Some((source, kind)) = resolved_source_column(scan, &planner.name) else {
                return if keep_only_resolved {
                    None
                } else {
                    Some(Err(format!(
                        "scan binding node_id={node_id} cannot resolve planner physical column '{}' in table '{}'",
                        planner.name, scan.table.name
                    )))
                };
            };
            Some(Ok(ResolvedScanColumn {
                planner: planner.clone(),
                source: source.clone(),
                kind,
            }))
        })
        .collect()
}

fn refresh_scan_projected_names(source: &ScanSource) -> Option<Vec<String>> {
    match source {
        ScanSource::IcebergMvTargetState(scan) => Some(projected_target_state_column_names(scan)),
        ScanSource::IcebergMvTargetLocator(scan) => {
            Some(projected_target_locator_column_names(scan))
        }
        _ => None,
    }
}

fn projected_target_state_column_names(
    scan: &crate::sql::planner::table::IcebergMvTargetStateScan,
) -> Vec<String> {
    let mut names = Vec::new();
    push_unique_projected_name(&mut names, &scan.row_id_column_name);
    for name in scan
        .group_key_names
        .iter()
        .chain(scan.aggregate_state_names.iter())
    {
        push_unique_projected_name(&mut names, name);
    }
    if let crate::sql::planner::table::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
        branch_scope: Some(scope),
        ..
    } = &scan.row_filter
    {
        push_unique_projected_name(&mut names, &scope.branch_id_column_name);
    }
    for name in [
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
    ] {
        push_unique_projected_name(&mut names, name);
    }
    names
}

fn projected_target_locator_column_names(
    scan: &crate::sql::planner::table::IcebergMvTargetLocatorScan,
) -> Vec<String> {
    let mut names = vec![scan.apply_key_column.clone()];
    if let Some(branch_id_column) = &scan.branch_id_column {
        push_unique_projected_name(&mut names, branch_id_column);
    }
    for name in [
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
    ] {
        push_unique_projected_name(&mut names, name);
    }
    names
}

fn push_unique_projected_name(names: &mut Vec<String>, name: &str) {
    if !names
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(name))
    {
        names.push(name.to_string());
    }
}

pub(super) fn resolve_effective_required_reads(
    node_id: i32,
    scan: &PlanScanNode,
    equality_required: &[String],
) -> Result<Vec<ResolvedReadColumn>, String> {
    let required_names = match refresh_scan_projected_names(&scan.table.source) {
        Some(projected) => {
            merge_required_columns_with_projected(scan.required_columns.clone(), &projected)
        }
        None => scan.required_columns.clone().unwrap_or_else(|| {
            scan.columns
                .iter()
                .filter(|column| {
                    !matches!(scan.table.source, ScanSource::IcebergVersionTable { .. })
                        || resolved_source_column(scan, &column.name).is_some()
                })
                .map(|column| column.name.clone())
                .collect()
        }),
    }
    .into_iter()
    .filter(|name| !is_variant_synthetic_name(scan, name))
    .collect::<Vec<_>>();
    let mut reads = required_names
        .into_iter()
        .map(|name| {
            let (source, _) = resolved_source_column(scan, &name).ok_or_else(|| {
                format!(
                    "scan binding node_id={node_id} cannot resolve required physical column '{name}' in table '{}'",
                    scan.table.name
                )
            })?;
            let planner = scan
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&name))
                .ok_or_else(|| {
                    format!(
                        "scan binding node_id={node_id} required physical column '{name}' has no planner ColumnId"
                    )
                })?;
            Ok(ResolvedReadColumn {
                planner_column_id: Some(planner.column_id),
                source: source.clone(),
                reason: ResolvedReadReason::PlannerRequiredOrOutput,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    for name in equality_required {
        if reads
            .iter()
            .any(|read| read.source.name.eq_ignore_ascii_case(name))
        {
            continue;
        }
        let (source, _) = resolved_source_column(scan, name).ok_or_else(|| {
            format!(
                "scan binding node_id={node_id} cannot resolve equality-delete physical column '{name}' in table '{}'",
                scan.table.name
            )
        })?;
        if let Some(planner) = scan
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
        {
            reads.push(ResolvedReadColumn {
                planner_column_id: Some(planner.column_id),
                source: source.clone(),
                reason: ResolvedReadReason::PlannerRequiredOrOutput,
            });
        } else {
            reads.push(ResolvedReadColumn {
                planner_column_id: None,
                source: source.clone(),
                reason: ResolvedReadReason::EqualityDeleteKey,
            });
        }
    }
    Ok(reads)
}

pub(super) fn merge_required_columns_with_projected(
    existing: Option<Vec<String>>,
    projected_names: &[String],
) -> Vec<String> {
    use std::collections::BTreeSet;

    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for name in projected_names
        .iter()
        .cloned()
        .chain(existing.unwrap_or_default())
    {
        if seen.insert(name.to_lowercase()) {
            out.push(name);
        }
    }
    out
}

fn resolved_source_column<'a>(
    scan: &'a PlanScanNode,
    name: &str,
) -> Option<(
    &'a novarocks_catalog::schema::ColumnDef,
    ResolvedScanColumnKind,
)> {
    if let Some(column) = scan
        .table
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
    {
        return Some((column, ResolvedScanColumnKind::PhysicalTableColumn));
    }
    scan.table
        .iceberg_row_lineage_metadata_columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
        .map(|column| (column, ResolvedScanColumnKind::IcebergMetadataColumn))
}

pub(super) fn effective_scan_column_names(scan: &PlanScanNode) -> Vec<String> {
    if let Some(projected) = refresh_scan_projected_names(&scan.table.source) {
        return merge_required_columns_with_projected(scan.required_columns.clone(), &projected);
    }
    let mut names = scan.required_columns.clone().unwrap_or_else(|| {
        scan.table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    });
    names.retain(|name| !is_variant_synthetic_name(scan, name));
    for variant in &scan.variant_columns {
        push_unique_projected_name(&mut names, &variant.source_column);
    }
    names
}

fn is_variant_synthetic_column(scan: &PlanScanNode, column_id: ColumnId) -> bool {
    scan.variant_columns
        .iter()
        .any(|variant| variant.synthetic_column_id == column_id)
}

fn is_variant_synthetic_name(scan: &PlanScanNode, name: &str) -> bool {
    scan.variant_columns.iter().any(|variant| {
        variant.synthetic_column.eq_ignore_ascii_case(name)
            || scan.columns.iter().any(|column| {
                column.column_id == variant.synthetic_column_id
                    && column.name.eq_ignore_ascii_case(name)
            })
    })
}
