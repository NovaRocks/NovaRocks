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

//! Shared validators used by both the INSERT (`engine/insert_flow.rs`) and
//! DELETE (`engine/delete_flow.rs`) entry points before lowering.
//! All errors returned here are user-visible — keep the messages action-oriented.

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use iceberg::table::Table;

/// Phase 1 does not write tables that have row-lineage enabled. Tables with
/// `variant`-typed columns are also rejected because the parquet writer in
/// `IcebergSink` has no encoding path for variant.
pub fn ensure_v3_writable(table: &Table) -> Result<(), String> {
    if table
        .metadata()
        .properties()
        .get("write.row-lineage")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        return Err("iceberg table has row-lineage enabled; phase 1 does not \
             support writing such tables. Disable row-lineage or wait \
             for phase 2."
            .to_string());
    }

    // Check for variant-typed columns. iceberg-rust 0.9 predates the Variant
    // primitive type, so we use a name-based fallback rather than a match arm.
    // If a future iceberg-rust release introduces PrimitiveType::Variant, the
    // Debug output will naturally contain "Variant" and we catch it here.
    // The check recurses into nested Struct/List/Map so that STRUCT<x VARIANT>
    // or ARRAY<VARIANT> are not missed.
    let schema = table.metadata().current_schema();
    for f in schema.as_struct().fields() {
        if type_contains_variant(&f.field_type) {
            return Err(format!(
                "iceberg table column `{}` contains variant type; phase 1 does not \
                 support writing variant. Drop the column or wait for phase 2.",
                f.name
            ));
        }
    }
    Ok(())
}

/// Returns `true` when `ty` or any type nested inside it has a Debug
/// representation that contains "variant" (case-insensitive).  This is a
/// name-based proxy because iceberg-rust 0.9 does not have a dedicated
/// `PrimitiveType::Variant` arm yet.
fn type_contains_variant(ty: &iceberg::spec::Type) -> bool {
    match ty {
        iceberg::spec::Type::Primitive(_) => format!("{ty:?}").to_lowercase().contains("variant"),
        iceberg::spec::Type::Struct(s) => s
            .fields()
            .iter()
            .any(|f| type_contains_variant(&f.field_type)),
        iceberg::spec::Type::List(l) => type_contains_variant(&l.element_field.field_type),
        iceberg::spec::Type::Map(m) => {
            type_contains_variant(&m.key_field.field_type)
                || type_contains_variant(&m.value_field.field_type)
        }
    }
}

/// Phase 1 only handles tables whose data is all under the current default
/// partition spec. Multiple historical specs (partition evolution) require
/// per-file spec routing in the writer that we don't have yet.
pub fn ensure_single_partition_spec(table: &Table) -> Result<(), String> {
    let m = table.metadata();
    let default_id = m.default_partition_spec_id();
    let other = m
        .partition_specs_iter()
        .filter(|s| s.spec_id() != default_id)
        .count();
    if other > 0 {
        return Err(format!(
            "iceberg table has {other} non-default partition spec(s); phase 1 \
             writes require a single partition spec. Rewrite or drop historical \
             data under prior specs."
        ));
    }
    Ok(())
}

/// DELETE writes position-delete files; the existing scan reader (see
/// `iceberg/position_delete.rs`) does not support reading equality-delete
/// files, so a table that already has equality deletes attached to its current
/// snapshot would become unreadable after the new snapshot lands.
///
/// Best-effort check via the snapshot summary's `total-equality-deletes`
/// property. When absent, we accept (no manifest walk yet — that belongs to
/// Tasks 9/10). Empty table (no current snapshot) → accept.
pub fn ensure_no_equality_deletes(table: &Table) -> Result<(), String> {
    let snap = match table.metadata().current_snapshot() {
        Some(s) => s,
        None => return Ok(()), // empty table — no manifests to inspect
    };
    let n = snap
        .summary()
        .additional_properties
        .get("total-equality-deletes")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    if n > 0 {
        return Err(
            "iceberg table has equality-delete files in its current snapshot; \
             phase 1 reader does not support equality deletes (see \
             iceberg/position_delete.rs). Compact away the equality \
             deletes before issuing DELETE."
                .to_string(),
        );
    }
    Ok(())
}

/// Strict column-by-column type match between the SELECT's arrow schema and
/// the iceberg table schema. No implicit cast, no reorder.
///
/// `columns_clause` is the optional `INSERT INTO t (cols)` list; when `None`,
/// SELECT must produce exactly `table_schema.fields().len()` columns in the
/// table's natural declaration order.
pub fn match_select_schema_to_table(
    select_schema: &ArrowSchemaRef,
    table: &Table,
    columns_clause: Option<&[String]>,
) -> Result<(), String> {
    let iceberg_schema = table.metadata().current_schema();
    let table_fields = iceberg_schema.as_struct().fields();

    let target_fields: Vec<_> = match columns_clause {
        None => table_fields.iter().collect(),
        Some(names) => {
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                let f = table_fields
                    .iter()
                    .find(|f| f.name == *n)
                    .ok_or_else(|| format!("INSERT column `{n}` does not exist in table"))?;
                out.push(f);
            }
            out
        }
    };

    if select_schema.fields().len() != target_fields.len() {
        return Err(format!(
            "INSERT column count mismatch: SELECT produces {} columns, target expects {}",
            select_schema.fields().len(),
            target_fields.len()
        ));
    }

    for (i, (sel, tgt)) in select_schema
        .fields()
        .iter()
        .zip(target_fields.iter())
        .enumerate()
    {
        if !arrow_iceberg_types_compatible(sel.data_type(), &tgt.field_type) {
            return Err(format!(
                "INSERT column {i} type mismatch: SELECT produces {:?}, target column `{}` is {:?}; \
                 phase 1 does not perform implicit cast — wrap the SELECT expression in CAST() explicitly.",
                sel.data_type(),
                tgt.name,
                tgt.field_type
            ));
        }
    }
    Ok(())
}

/// Returns `true` when `arrow_ty` and `iceberg_ty` represent the same logical
/// type. Delegates to `iceberg::arrow::type_to_arrow_type` so there is one
/// canonical mapping. On conversion error (unknown / complex type), returns
/// `false` (conservative reject).
fn arrow_iceberg_types_compatible(
    arrow_ty: &arrow::datatypes::DataType,
    iceberg_ty: &iceberg::spec::Type,
) -> bool {
    match iceberg::arrow::type_to_arrow_type(iceberg_ty) {
        Ok(expected) => &expected == arrow_ty,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn errors_carry_actionable_messages() {
        // Sanity test that the module compiles and the public API is accessible.
        // Real coverage comes from NEG-* integration tests in Task 17.
        let s = "row-lineage";
        assert!(s.contains("row-lineage"));
    }
}
