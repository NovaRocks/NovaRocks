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

//! One old data file, a large delete and the next deletion-vector replacement.
//! Reuses one 20k/18k interval from the existing SQL regression rather than
//! copying its four-file corpus. Metadata proves the actual file shape; the
//! complete row bag and cardinality prove both deletion sets remain effective.
//! This collector does not invent a runtime batch count from input row count.
use super::*;

const SEED_ROWS: i64 = 20_000;
const FIRST_DELETE_ROWS: i64 = 18_000;
type FileFact = (i32, String, String, i64);

fn first_delete_sql(catalog: &str, database: &str, dop: i32) -> String {
    format!("DELETE FROM {catalog}.{database}.same_file_delete_{dop} WHERE id<={FIRST_DELETE_ROWS}")
}

pub(super) fn observation_specs(cluster_size: usize) -> impl Iterator<Item = StatementSpec> {
    [1, 4].into_iter().map(move |dop| StatementSpec {
        sql: first_delete_sql(CATALOG, DATABASE, dop),
        faults: vec![],
        observations: (0..cluster_size)
            .flat_map(|index| {
                [
                    (index, "reader-marker", None),
                    (index, "writer-marker", None),
                    (index, "grpc-fragment", None),
                ]
            })
            .collect(),
    })
}

fn files(control: &mut mysql::Conn, table: &str) -> Result<Vec<FileFact>> {
    control
        .query(format!(
            "SELECT content,file_path,file_format,record_count FROM {table}$files \
             ORDER BY content,file_path"
        ))
        .context("read current live data/delete entries from original Iceberg metadata")
}

fn assert_live_dv(observed: &[FileFact], old_file: &str, deleted_rows: i64) -> Result<()> {
    anyhow::ensure!(
        observed.len() == 2,
        "expected one unchanged old data file and one live DV entry, got {observed:?}"
    );
    let data = &observed[0];
    let delete = &observed[1];
    anyhow::ensure!(
        data.0 == 0 && data.1 == old_file && data.2 == "PARQUET" && data.3 == SEED_ROWS,
        "mutation replaced/duplicated the original MOR data file: {observed:?}"
    );
    anyhow::ensure!(
        delete.0 == 1 && delete.2 == "PUFFIN" && delete.3 == deleted_rows && !delete.1.is_empty(),
        "expected exactly one current deletion vector with cardinality {deleted_rows}, got {observed:?}"
    );
    Ok(())
}

fn assert_remaining(
    control: &mut mysql::Conn,
    table: &str,
    intervals: &[std::ops::RangeInclusive<i64>],
    subject: &str,
) -> Result<()> {
    let expected = intervals
        .iter()
        .flat_map(|range| range.clone())
        .map(|id| (id, id))
        .collect::<Vec<_>>();
    assert_rows(control, table, &expected, subject)?;
    let aggregate: Option<(i64, i64, i64, i64)> = control.query_first(format!(
        "SELECT count(*),min(id),max(id),sum(v) FROM {table}"
    ))?;
    let expected_summary: (i64, i64, i64, i64) = (
        expected.len() as i64,
        expected
            .first()
            .context("same-file oracle must have remaining rows")?
            .0,
        expected
            .last()
            .context("same-file oracle must have remaining rows")?
            .0,
        expected.iter().map(|(_, v)| *v).sum(),
    );
    anyhow::ensure!(
        aggregate == Some(expected_summary),
        "{subject}: boundary aggregate {aggregate:?}, expected {expected_summary:?}"
    );
    Ok(())
}

pub(super) fn run(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    dop: i32,
    selected: SelectedStatement,
) -> Result<()> {
    let table = format!("{catalog}.{database}.same_file_delete_{dop}");
    context.action(format!(
        "retain one old data file across large DELETE and DV merge with requested DOP {dop}"
    ));
    control.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT,v BIGINT) TBLPROPERTIES \
         (\"format-version\"=\"3\",\"write.row-lineage\"=\"true\",\"novarocks.update.mode\"=\"merge-on-read\")"
    ))?;
    // Only seeding is singleton. The original ORDER BY shape also gathers
    // the generated input, and the following observations verify the outcome.
    control.query_drop("SET pipeline_dop=1")?;
    control.query_drop(format!(
        "INSERT INTO {table} SELECT generate_series,generate_series \
         FROM TABLE(generate_series(1,{SEED_ROWS})) ORDER BY generate_series"
    ))?;
    assert_affected(control, SEED_ROWS as u64, "seed the one original old file")?;
    let row_files: Vec<(String, i64)> = control.query(format!(
        "SELECT _file,count(*) FROM {table} GROUP BY _file ORDER BY _file"
    ))?;
    anyhow::ensure!(
        row_files.len() == 1 && row_files[0].1 == SEED_ROWS,
        "same-file fixture did not produce exactly one {SEED_ROWS}-row file: {row_files:?}"
    );
    let before = files(control, &table)?;
    anyhow::ensure!(
        before == [(0, row_files[0].0.clone(), "PARQUET".to_owned(), SEED_ROWS)],
        "row-level source file differs from original live metadata: {before:?}"
    );
    let old_file = row_files[0].0.as_str();
    control.query_drop(format!("SET pipeline_dop={dop}"))?;

    let before_writers = all_write_counters(context)?;
    super::dop_observation::mutate_observed(
        context,
        control,
        &selected,
        &table,
        &first_delete_sql(catalog, database, dop),
        &before_writers,
        dop,
        FIRST_DELETE_ROWS as u64,
        Some(FIRST_DELETE_ROWS as u64),
        "large delete from the one old file",
    )?;
    assert_remaining(
        control,
        &table,
        std::slice::from_ref(&(18_001..=20_000)),
        "first same-file delete",
    )?;
    let after_first = files(control, &table)?;
    assert_live_dv(&after_first, old_file, FIRST_DELETE_ROWS)?;

    // The replacement DV must include the first 18k positions as well as the
    // next 500. Losing the old vector would resurrect those original rows.
    mutate(
        control,
        &table,
        &format!("DELETE FROM {table} WHERE id>19000 AND id<=19500"),
        500,
        1,
        "merge a second range into the existing deletion vector",
    )?;
    assert_remaining(
        control,
        &table,
        &[18_001..=19_000, 19_501..=20_000],
        "merged same-file delete",
    )?;
    let after_second = files(control, &table)?;
    assert_live_dv(&after_second, old_file, 18_500)?;
    mutate(
        control,
        &table,
        &format!("DELETE FROM {table} WHERE id<={FIRST_DELETE_ROWS}"),
        0,
        0,
        "already deleted positions remain absent",
    )?;
    assert_remaining(
        control,
        &table,
        &[18_001..=19_000, 19_501..=20_000],
        "same-file no-op delete",
    )?;
    anyhow::ensure!(
        files(control, &table)? == after_second,
        "no-op delete changed the live file set"
    );

    // The metadata evidence proves one final DV entry. Runtime reader batch
    // and writer DOP facts come from this exact first DELETE's authenticated
    // observation report, never from the declared input row count.
    std::fs::write(
        context
            .scenario_root()
            .join(format!("same-file-delete-{dop}.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "requested_dop":dop,"seed_requested_dop":1,
            "old_file_row_counts":row_files,"seed_files":before,
            "after_first_delete_files":after_first,"after_second_delete_files":after_second,
            "first_affected_rows":FIRST_DELETE_ROWS,"second_affected_rows":500,
            "live_dv_entry_count":1,
            "actual_delete_batches_evidence":format!("row-level-writer-dop-{dop}-same-file-delete.json"),
        }))?,
    )?;
    Ok(())
}
