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
use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::common::ids::SlotId;
use crate::descriptors;
use crate::exec::chunk::field_with_slot_id;
use crate::exprs;
use crate::planner;
use crate::novarocks_config::config as novarocks_app_config;
use crate::types;
use crate::{data_sinks, partitions};

#[derive(Clone, Debug)]
pub(crate) struct Layout {
    pub(crate) order: Vec<(types::TTupleId, types::TSlotId)>,
    pub(crate) index: HashMap<(types::TTupleId, types::TSlotId), usize>,
}

/// Build an Arrow `SchemaRef` for a given `Layout` using FE's descriptor table.
pub(crate) fn schema_for_layout(
    desc_tbl: &descriptors::TDescriptorTable,
    layout: &Layout,
) -> Result<SchemaRef, String> {
    let slot_descs = desc_tbl
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "missing slot_descriptors in desc_tbl".to_string())?;

    let mut info: HashMap<(types::TTupleId, types::TSlotId), (String, DataType, bool)> =
        HashMap::new();
    for s in slot_descs {
        let (Some(parent), Some(id), Some(slot_type)) = (s.parent, s.id, s.slot_type.as_ref())
        else {
            continue;
        };
        let dt = crate::lower::type_lowering::arrow_type_from_desc(slot_type)
            .ok_or_else(|| format!("unsupported slot_type for tuple_id={parent} slot_id={id}"))?;
        let name = s
            .col_name
            .clone()
            .unwrap_or_else(|| format!("col_{parent}_{id}"));
        let nullable = s.is_nullable.unwrap_or(true);
        info.insert((parent, id), (name, dt, nullable));
    }

    let mut fields = Vec::with_capacity(layout.order.len());
    for (idx, (tuple_id, slot_id)) in layout.order.iter().enumerate() {
        let (name, dt, nullable) = info.get(&(*tuple_id, *slot_id)).ok_or_else(|| {
            format!(
                "missing slot descriptor for layout column {idx}: tuple_id={tuple_id} slot_id={slot_id}"
            )
        })?;
        let slot_id = SlotId::try_from(*slot_id)?;
        fields.push(field_with_slot_id(
            Field::new(name, dt.clone(), *nullable),
            slot_id,
        ));
    }

    Ok(SchemaRef::new(Schema::new(fields)))
}

/// Build an Arrow `SchemaRef` for all slots of a given tuple in descriptor table.
pub(crate) fn schema_for_tuple(
    desc_tbl: &descriptors::TDescriptorTable,
    tuple_id: types::TTupleId,
) -> Result<SchemaRef, String> {
    let slot_descs = desc_tbl
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "missing slot_descriptors in desc_tbl".to_string())?;

    let mut fields = Vec::new();
    for s in slot_descs {
        let (Some(parent), Some(id), Some(slot_type)) = (s.parent, s.id, s.slot_type.as_ref())
        else {
            continue;
        };
        if parent != tuple_id {
            continue;
        }
        let dt = crate::lower::type_lowering::arrow_type_from_desc(slot_type)
            .ok_or_else(|| format!("unsupported slot_type for tuple_id={parent} slot_id={id}"))?;
        let name = s
            .col_name
            .clone()
            .unwrap_or_else(|| format!("col_{parent}_{id}"));
        let nullable = s.is_nullable.unwrap_or(true);

        let mut field = Field::new(name, dt, nullable);
        if let Some(unique_id) = s.col_unique_id.filter(|v| *v > 0) {
            let mut meta = field.metadata().clone();
            meta.insert(
                "starrocks.format.column.id".to_string(),
                unique_id.to_string(),
            );
            field = field.with_metadata(meta);
        }

        let slot_id = SlotId::try_from(id)?;
        fields.push(field_with_slot_id(field, slot_id));
    }

    if fields.is_empty() {
        return Err(format!(
            "missing slot descriptors for tuple_id={tuple_id} when building required schema"
        ));
    }

    Ok(SchemaRef::new(Schema::new(fields)))
}

pub(crate) fn infer_tuple_slot_order(
    fragment: &planner::TPlanFragment,
) -> HashMap<types::TTupleId, Vec<types::TSlotId>> {
    fn push_unique(
        out: &mut HashMap<types::TTupleId, Vec<types::TSlotId>>,
        tuple_id: types::TTupleId,
        slot_id: types::TSlotId,
    ) {
        let entry = out.entry(tuple_id).or_default();
        if !entry.contains(&slot_id) {
            entry.push(slot_id);
        }
    }

    fn collect_expr(e: &exprs::TExpr, out: &mut HashMap<types::TTupleId, Vec<types::TSlotId>>) {
        for n in &e.nodes {
            if let Some(slot) = n.slot_ref.as_ref() {
                push_unique(out, slot.tuple_id, slot.slot_id);
            }
        }
    }

    fn collect_exprs(
        list: &[exprs::TExpr],
        out: &mut HashMap<types::TTupleId, Vec<types::TSlotId>>,
    ) {
        for e in list {
            collect_expr(e, out);
        }
    }

    fn collect_partition_exprs(
        partition: &partitions::TDataPartition,
        out: &mut HashMap<types::TTupleId, Vec<types::TSlotId>>,
    ) {
        if let Some(exprs) = partition.partition_exprs.as_ref() {
            collect_exprs(exprs, out);
        }
    }

    let mut out: HashMap<types::TTupleId, Vec<types::TSlotId>> = HashMap::new();

    if let Some(exprs) = fragment.output_exprs.as_ref() {
        collect_exprs(exprs, &mut out);
    }

    if let Some(sink) = fragment.output_sink.as_ref() {
        match sink.type_ {
            data_sinks::TDataSinkType::DATA_STREAM_SINK => {
                if let Some(s) = sink.stream_sink.as_ref() {
                    collect_partition_exprs(&s.output_partition, &mut out);
                }
            }
            data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
                if let Some(m) = sink.multi_cast_stream_sink.as_ref() {
                    for s in &m.sinks {
                        collect_partition_exprs(&s.output_partition, &mut out);
                    }
                }
            }
            data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
                if let Some(sp) = sink.split_stream_sink.as_ref() {
                    if let Some(sinks) = sp.sinks.as_ref() {
                        for s in sinks {
                            collect_partition_exprs(&s.output_partition, &mut out);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let Some(plan) = fragment.plan.as_ref() else {
        return out;
    };
    for n in &plan.nodes {
        if let Some(conj) = n.conjuncts.as_ref() {
            collect_exprs(conj, &mut out);
        }
        if let Some(p) = n.project_node.as_ref().and_then(|p| p.slot_map.as_ref()) {
            for e in p.values() {
                collect_expr(e, &mut out);
            }
        }
        if let Some(sort) = n.sort_node.as_ref() {
            let info = &sort.sort_info;
            collect_exprs(&info.ordering_exprs, &mut out);
            if let Some(exprs) = info.sort_tuple_slot_exprs.as_ref() {
                collect_exprs(exprs, &mut out);
            }
        }
        if let Some(analytic) = n.analytic_node.as_ref() {
            collect_exprs(&analytic.partition_exprs, &mut out);
            collect_exprs(&analytic.order_by_exprs, &mut out);
            collect_exprs(&analytic.analytic_functions, &mut out);
            if let Some(w) = analytic.window.as_ref() {
                if let Some(b) = w.window_start.as_ref() {
                    if let Some(pred) = b.range_offset_predicate.as_ref() {
                        collect_expr(pred, &mut out);
                    }
                }
                if let Some(b) = w.window_end.as_ref() {
                    if let Some(pred) = b.range_offset_predicate.as_ref() {
                        collect_expr(pred, &mut out);
                    }
                }
            }
        }
        if let Some(exch) = n.exchange_node.as_ref() {
            if let Some(info) = exch.sort_info.as_ref() {
                collect_exprs(&info.ordering_exprs, &mut out);
            }
        }
        if let Some(agg) = n.agg_node.as_ref() {
            if let Some(g) = agg.grouping_exprs.as_ref() {
                collect_exprs(g, &mut out);
            }
            collect_exprs(&agg.aggregate_functions, &mut out);
        }
        if let Some(join) = n.hash_join_node.as_ref() {
            for c in &join.eq_join_conjuncts {
                collect_expr(&c.left, &mut out);
                collect_expr(&c.right, &mut out);
            }
            if let Some(other) = join.other_join_conjuncts.as_ref() {
                collect_exprs(other, &mut out);
            }
        }
        if let Some(nl) = n.nestloop_join_node.as_ref() {
            if let Some(conj) = nl.join_conjuncts.as_ref() {
                collect_exprs(conj, &mut out);
            }
        }
    }

    out
}

pub(crate) fn build_tuple_slot_order(
    desc: Option<&descriptors::TDescriptorTable>,
) -> HashMap<types::TTupleId, Vec<types::TSlotId>> {
    let mut map: HashMap<types::TTupleId, Vec<types::TSlotId>> = HashMap::new();
    let Some(desc) = desc else { return map };
    let Some(slots) = desc.slot_descriptors.as_ref() else {
        return map;
    };
    for s in slots {
        let (Some(tuple_id), Some(slot_id)) = (s.parent, s.id) else {
            continue;
        };
        map.entry(tuple_id).or_default().push(slot_id);
    }
    // Do not sort slots! The order in desc_tbl.slot_descriptors is significant.
    map
}

pub(crate) fn tuple_slot_col_names(
    desc: &descriptors::TDescriptorTable,
    tuple_id: types::TTupleId,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
) -> Result<Vec<String>, String> {
    let slots = tuple_slots
        .get(&tuple_id)
        .ok_or_else(|| format!("missing slot descriptors for tuple_id={tuple_id}"))?;
    let slot_descs = desc
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "missing slot_descriptors in desc_tbl".to_string())?;

    let mut names: HashMap<types::TSlotId, String> = HashMap::new();
    for s in slot_descs {
        let (Some(parent), Some(id)) = (s.parent, s.id) else {
            continue;
        };
        if parent != tuple_id {
            continue;
        }
        if let Some(name) = slot_name_from_desc(s) {
            names.insert(id, name);
        }
    }

    let mut out = Vec::with_capacity(slots.len());
    for slot_id in slots {
        let name = names
            .get(slot_id)
            .ok_or_else(|| format!("missing col_name for tuple_id={tuple_id} slot_id={slot_id}"))?;
        out.push(name.clone());
    }
    Ok(out)
}

pub(crate) fn slot_name_from_desc(desc: &descriptors::TSlotDescriptor) -> Option<String> {
    if let Some(name) = desc.col_name.as_ref().filter(|v| !v.is_empty()) {
        return Some(name.clone());
    }
    if let Some(name) = desc.col_physical_name.as_ref().filter(|v| !v.is_empty()) {
        return Some(name.clone());
    }
    None
}

pub(crate) fn resolve_jdbc_table<'a>(
    desc: &'a descriptors::TDescriptorTable,
    tuple_id: types::TTupleId,
) -> Result<&'a descriptors::TJDBCTable, String> {
    let tuple_desc = find_tuple_descriptor(desc, tuple_id)?;
    let table_id = tuple_desc
        .table_id
        .ok_or_else(|| format!("tuple_id={tuple_id} missing table_id"))?;
    resolve_jdbc_table_by_table_id(desc, table_id)
}

pub(crate) fn resolve_jdbc_table_by_table_id<'a>(
    desc: &'a descriptors::TDescriptorTable,
    table_id: types::TTableId,
) -> Result<&'a descriptors::TJDBCTable, String> {
    let table_descs = desc
        .table_descriptors
        .as_ref()
        .ok_or_else(|| format!("missing table_descriptors for table_id={table_id}"))?;
    let table_desc = table_descs
        .iter()
        .find(|t| t.id == table_id)
        .ok_or_else(|| format!("missing table_descriptor for table_id={table_id}"))?;
    table_desc
        .jdbc_table
        .as_ref()
        .ok_or_else(|| format!("table_id={table_id} is not a JDBC table"))
}

pub(crate) fn resolve_jdbc_table_by_name<'a>(
    desc: &'a descriptors::TDescriptorTable,
    table_name: Option<&str>,
) -> Result<&'a descriptors::TJDBCTable, String> {
    fn strip_ticks(s: &str) -> &str {
        s.strip_prefix('`')
            .and_then(|x| x.strip_suffix('`'))
            .unwrap_or(s)
    }
    fn norm_full(s: &str) -> String {
        strip_ticks(s.trim()).to_ascii_lowercase()
    }
    fn norm_last(s: &str) -> String {
        let s = strip_ticks(s.trim());
        let last = s.rsplit_once('.').map(|(_, b)| b).unwrap_or(s);
        strip_ticks(last).to_ascii_lowercase()
    }

    let table_descs = desc
        .table_descriptors
        .as_ref()
        .ok_or_else(|| "missing table_descriptors".to_string())?;

    let candidates: Vec<&descriptors::TTableDescriptor> = if let Some(name) = table_name {
        let name_full = norm_full(name);
        let name_last = norm_last(name);
        table_descs
            .iter()
            .filter(|t| {
                let t_name = norm_last(&t.table_name);
                let t_db = norm_last(&t.db_name);
                t_name == name_last || format!("{t_db}.{t_name}") == name_full
            })
            .collect()
    } else {
        Vec::new()
    };

    let pick_from = if !candidates.is_empty() {
        candidates
    } else {
        table_descs.iter().collect()
    };

    let mut jdbc = pick_from.into_iter().filter(|t| t.jdbc_table.is_some());
    let first = jdbc.next().ok_or_else(|| {
        if let Some(name) = table_name {
            format!("no JDBC table descriptor matched table_name={name}")
        } else {
            "no JDBC table descriptors present".to_string()
        }
    })?;
    if jdbc.next().is_some() {
        return Err("multiple JDBC table descriptors present; tuple mapping required".to_string());
    }
    first
        .jdbc_table
        .as_ref()
        .ok_or_else(|| "matched table descriptor missing jdbc_table".to_string())
}

pub(crate) fn resolve_mysql_table<'a>(
    desc: &'a descriptors::TDescriptorTable,
    tuple_id: types::TTupleId,
) -> Result<&'a descriptors::TMySQLTable, String> {
    let tuple_desc = find_tuple_descriptor(desc, tuple_id)?;
    let table_id = tuple_desc
        .table_id
        .ok_or_else(|| format!("tuple_id={tuple_id} missing table_id"))?;
    let table_descs = desc
        .table_descriptors
        .as_ref()
        .ok_or_else(|| format!("missing table_descriptors for tuple_id={tuple_id}"))?;
    let table_desc = table_descs
        .iter()
        .find(|t| t.id == table_id)
        .ok_or_else(|| format!("missing table_descriptor for table_id={table_id}"))?;
    table_desc
        .mysql_table
        .as_ref()
        .ok_or_else(|| format!("table_id={table_id} is not a MySQL table"))
}

pub(crate) fn find_tuple_descriptor<'a>(
    desc: &'a descriptors::TDescriptorTable,
    tuple_id: types::TTupleId,
) -> Result<&'a descriptors::TTupleDescriptor, String> {
    if let Some(td) = desc
        .tuple_descriptors
        .iter()
        .find(|t| t.id == Some(tuple_id))
    {
        return Ok(td);
    }
    if tuple_id < 0 {
        return Err(format!("invalid tuple_id={tuple_id}"));
    }
    desc.tuple_descriptors
        .get(tuple_id as usize)
        .ok_or_else(|| format!("missing tuple_descriptor for tuple_id={tuple_id}"))
}

pub(crate) fn jdbc_conn_from_config() -> Result<(String, Option<String>, Option<String>), String> {
    let cfg = novarocks_app_config().map_err(|e| e.to_string())?;
    let jdbc = cfg
        .jdbc_config()
        .ok_or_else(|| "missing [jdbc] config (url/user/password)".to_string())?;
    Ok((jdbc.url.clone(), jdbc.user.clone(), jdbc.password.clone()))
}

pub(crate) fn qualify_table_name(table: String, db_name: Option<&str>) -> String {
    if table.contains('.') {
        return table;
    }
    let cfg_db = novarocks_app_config()
        .ok()
        .and_then(|c| c.jdbc_config().and_then(|j| j.default_db.clone()));
    let db = db_name
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .or(cfg_db);
    let Some(db) = db else { return table };
    format!("{db}.{table}")
}

pub(crate) fn normalize_slot_name(name: &str) -> String {
    let mut s = name.trim();
    if let Some(rest) = s.strip_prefix('`').and_then(|x| x.strip_suffix('`')) {
        s = rest;
    }
    if let Some((_prefix, last)) = s.rsplit_once('.') {
        s = last;
        if let Some(rest) = s.strip_prefix('`').and_then(|x| x.strip_suffix('`')) {
            s = rest;
        }
    }
    s.to_ascii_lowercase()
}

pub(crate) fn layout_for_tuple_columns(
    desc: &descriptors::TDescriptorTable,
    tuple_id: types::TTupleId,
    columns: &[String],
) -> Result<Layout, String> {
    let slot_descs = desc
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "missing slot_descriptors in desc_tbl".to_string())?;

    let mut by_name: HashMap<String, types::TSlotId> = HashMap::new();
    for s in slot_descs {
        let (Some(parent), Some(id), Some(name)) = (s.parent, s.id, s.col_name.as_ref()) else {
            continue;
        };
        if parent == tuple_id {
            by_name.insert(normalize_slot_name(name), id);
        }
    }

    let mut slot_ids = Vec::with_capacity(columns.len());
    for col in columns {
        let key = normalize_slot_name(col);
        let slot_id = by_name
            .get(&key)
            .copied()
            .ok_or_else(|| format!("missing slot_id for tuple_id={tuple_id} column={col}"))?;
        slot_ids.push(slot_id);
    }

    Ok(layout_from_slot_ids(tuple_id, slot_ids))
}

pub(crate) fn layout_for_row_tuples(
    row_tuples: &[types::TTupleId],
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
) -> Layout {
    let mut order = Vec::new();
    for tuple_id in row_tuples {
        if let Some(slots) = tuple_slots.get(tuple_id) {
            for slot_id in slots {
                order.push((*tuple_id, *slot_id));
            }
        }
    }
    let index = order.iter().enumerate().map(|(i, key)| (*key, i)).collect();
    Layout { order, index }
}

pub(crate) fn layout_from_slot_ids(
    tuple_id: types::TTupleId,
    slot_ids: impl IntoIterator<Item = types::TSlotId>,
) -> Layout {
    let order: Vec<(types::TTupleId, types::TSlotId)> =
        slot_ids.into_iter().map(|s| (tuple_id, s)).collect();
    let index = order.iter().enumerate().map(|(i, key)| (*key, i)).collect();
    Layout { order, index }
}

pub(crate) fn col_names_from_layout(
    desc: &descriptors::TDescriptorTable,
    layout: &Layout,
) -> Result<Vec<String>, String> {
    let slot_descs = desc
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "missing slot_descriptors in desc_tbl".to_string())?;

    let mut name_map: HashMap<(types::TTupleId, types::TSlotId), String> = HashMap::new();
    for s in slot_descs {
        let (Some(parent), Some(id)) = (s.parent, s.id) else {
            continue;
        };
        if let Some(name) = slot_name_from_desc(s) {
            name_map.insert((parent, id), name);
        }
    }

    let mut names = Vec::with_capacity(layout.order.len());
    for (tuple_id, slot_id) in &layout.order {
        let name = name_map.get(&(*tuple_id, *slot_id)).ok_or_else(|| {
            format!("missing column name for tuple_id={tuple_id} slot_id={slot_id}")
        })?;
        names.push(name.clone());
    }
    Ok(names)
}

pub(crate) fn reorder_tuple_slots(
    tuple_slots: &mut HashMap<types::TTupleId, Vec<types::TSlotId>>,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
) {
    let Some(desc) = desc_tbl else { return };

    let mut tuple_to_table = HashMap::new();
    for t in &desc.tuple_descriptors {
        if let (Some(tid), Some(tbl_id)) = (t.id, t.table_id) {
            tuple_to_table.insert(tid, tbl_id);
        }
    }

    let mut table_columns = HashMap::new();
    if let Some(tables) = &desc.table_descriptors {
        for t in tables {
            let cols: Option<&Vec<descriptors::TColumn>> = if let Some(ice) = &t.iceberg_table {
                ice.columns.as_ref()
            } else if let Some(hdfs) = &t.hdfs_table {
                hdfs.columns.as_ref()
            } else {
                None
            };

            if let Some(cols) = cols {
                let names: Vec<String> = cols.iter().map(|c| c.column_name.clone()).collect();
                table_columns.insert(t.id, names);
            }
        }
    }

    let mut slot_names = HashMap::new();
    if let Some(slots) = &desc.slot_descriptors {
        for s in slots {
            if let (Some(sid), Some(name)) = (s.id, &s.col_name) {
                slot_names.insert(sid, name.clone());
            }
        }
    }

    for (tuple_id, slots) in tuple_slots.iter_mut() {
        if let Some(table_id) = tuple_to_table.get(tuple_id) {
            if let Some(col_order) = table_columns.get(table_id) {
                slots.sort_by_key(|sid| {
                    if let Some(name) = slot_names.get(sid) {
                        col_order
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(name))
                            .unwrap_or(usize::MAX)
                    } else {
                        usize::MAX
                    }
                });
            }
        }
    }
}
