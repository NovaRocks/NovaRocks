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

fn load_namespace_bound_case(
    case: &str,
    source: &str,
    variables: &HashMap<String, String>,
) -> crate::types::SqlCase {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join(format!("{case}.sql"));
    std::fs::write(&path, source).unwrap();
    let meta = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
    let marker = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").unwrap();
    crate::parser::load_sql_case_from_file(&path, &meta, &marker, variables)
        .unwrap()
        .unwrap()
}

use super::*;
fn typed_expected(source: &str) -> crate::types::ResultSet {
    let section = crate::golden::GoldenFile::parse(source)
        .unwrap()
        .sections
        .remove(0);
    let mut result = crate::results::project_golden_blocks(&section.blocks).unwrap();
    result.golden = Some(crate::types::GoldenSection {
        source: section.clone(),
        expanded: section,
    });
    result
}

#[test]
fn show_create_location_uses_exact_fixture_placeholders() {
    use crate::golden::{Block, render_text};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("show_create.result");
    let ddl = "CREATE TABLE catalog.${case_db}.t (id BIGINT)\nWITH (location = '${iceberg_rest_warehouse}/${case_db}/t')";
    let source = format!(
        "-- query 1\n-- types\tvarchar\nCreate Table\n{}\n",
        render_text(ddl)
    );
    std::fs::write(&path, &source).unwrap();

    for (warehouse, namespace) in [
        ("s3://warehouse/run-a/rest", "run_a_d0000"),
        ("file:///tmp/run-b/warehouse", "run_b_d0000"),
    ] {
        let variables = HashMap::from([
            ("iceberg_rest_warehouse".into(), warehouse.into()),
            ("case_db".into(), namespace.into()),
        ]);
        let mut expected = crate::results::load_expected_results_with_variables(&path, &variables)
            .unwrap()
            .remove(&1)
            .unwrap();
        let statement = format!("SHOW CREATE TABLE catalog.{namespace}.t");
        render_expected_for_execution(
            &statement,
            &mut expected,
            &variables,
            &BTreeMap::new(),
            std::slice::from_ref(&statement),
        )
        .unwrap();

        let actual_ddl = format!(
            "CREATE TABLE catalog.{namespace}.t (id BIGINT)\nWITH (location = '{warehouse}/{namespace}/t')"
        );
        let actual = Block::Typed {
            types: vec!["varchar".into()],
            names: vec!["Create Table".into()],
            rows: vec![vec![render_text(&actual_ddl)]],
        };
        let golden = expected.golden.unwrap();
        assert!(
            crate::golden::execution::compare_blocks(
                &golden.expanded.blocks,
                std::slice::from_ref(&actual),
                true,
                None,
            )
            .0
        );
        let Block::Typed { rows, .. } = &golden.source.blocks[0] else {
            unreachable!()
        };
        assert!(rows[0][0].contains("${case_db}"));
        assert!(rows[0][0].contains("${iceberg_rest_warehouse}"));
        let mut wrong_location = actual;
        let Block::Typed { rows, .. } = &mut wrong_location else {
            unreachable!()
        };
        rows[0][0] = render_text(&actual_ddl.replace("/t')", "/other_table')"));
        assert!(
            !crate::golden::execution::compare_blocks(
                &golden.expanded.blocks,
                &[wrong_location],
                true,
                None,
            )
            .0
        );
    }
    assert_eq!(std::fs::read_to_string(path).unwrap(), source);
}

#[test]
fn typed_metadata_retains_sql_null_and_rewrites_only_the_string_null() {
    let mut result = typed_expected("-- query 1\n-- types\tvarchar\nSchema\nNULL\n\"NULL\"\n");
    let source = result.golden.as_ref().unwrap().source.clone();
    let mapping = BTreeMap::from([("NULL".into(), "owned_namespace".into())]);
    render_expected_result("SHOW SCHEMAS", &mut result, &HashMap::new(), &mapping).unwrap();
    assert_eq!(result.golden.as_ref().unwrap().expanded, source);
    render_expected_for_execution(
        "SHOW SCHEMAS",
        &mut result,
        &HashMap::new(),
        &mapping,
        &["SHOW SCHEMAS".into()],
    )
    .unwrap();
    let golden = result.golden.unwrap();
    assert_eq!(golden.source, source);
    let crate::golden::Block::Typed { rows, .. } = &golden.expanded.blocks[0] else {
        panic!("typed expected")
    };
    assert_eq!(
        rows,
        &[
            vec!["NULL".to_string()],
            vec!["owned_namespace".to_string()]
        ]
    );
}

#[test]
fn typed_metadata_uses_each_exact_result_statement_owner() {
    let sql = "SHOW SCHEMAS; SELECT 'logical_db' AS Database;";
    let mut result = typed_expected(
        "-- query 1\n-- types\tvarchar\nSchema\nlogical_db\n-- types\tvarchar\nDatabase\nlogical_db\n",
    );
    let source = result.golden.as_ref().unwrap().source.clone();
    let mapping = BTreeMap::from([("logical_db".into(), "owned_namespace".into())]);
    render_expected_result(sql, &mut result, &HashMap::new(), &mapping).unwrap();
    assert_eq!(result.golden.as_ref().unwrap().expanded, source);
    render_expected_for_execution(
        sql,
        &mut result,
        &HashMap::new(),
        &mapping,
        &[
            "SHOW SCHEMAS".into(),
            "SELECT 'logical_db' AS Database".into(),
        ],
    )
    .unwrap();
    let golden = result.golden.unwrap();
    assert_eq!(golden.source, source);
    let crate::golden::Block::Typed { rows: metadata, .. } = &golden.expanded.blocks[0] else {
        panic!("typed expected")
    };
    let crate::golden::Block::Typed { rows: literal, .. } = &golden.expanded.blocks[1] else {
        panic!("typed expected")
    };
    assert_eq!(metadata, &[vec!["owned_namespace".to_string()]]);
    assert_eq!(literal, &[vec!["logical_db".to_string()]]);
}

#[test]
fn typed_metadata_rejects_ambiguous_result_binding_without_mutation() {
    let mut result = typed_expected("-- query 1\n-- types\tvarchar\nv\nx\n");
    let before = result.clone();
    let error = render_expected_for_execution(
        "SELECT 1",
        &mut result,
        &HashMap::new(),
        &BTreeMap::new(),
        &[],
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("ambiguous typed metadata binding")
    );
    assert_eq!(result, before);
}

#[test]
fn runtime_typed_binding_does_not_bind_legacy_metadata_twice() {
    let sql = "SHOW SCHEMAS; SELECT 'logical_db' AS Database;";
    let mut result = typed_expected(
        "-- query 1\n-- legacy\nSchema\nlogical_db\n-- types\tvarchar\nSchema\nlogical_db\n",
    );
    let mapping = BTreeMap::from([
        ("logical_db".into(), "owned_namespace".into()),
        ("owned_namespace".into(), "wrong_second_binding".into()),
    ]);
    render_expected_result(sql, &mut result, &HashMap::new(), &mapping).unwrap();
    render_expected_for_execution(
        sql,
        &mut result,
        &HashMap::new(),
        &mapping,
        &[
            "SHOW SCHEMAS".into(),
            "SELECT 'logical_db' AS Database".into(),
        ],
    )
    .unwrap();
    let golden = result.golden.unwrap();
    let crate::golden::Block::Legacy { rows: legacy, .. } = &golden.expanded.blocks[0] else {
        panic!("legacy expected")
    };
    let crate::golden::Block::Typed { rows: typed, .. } = &golden.expanded.blocks[1] else {
        panic!("typed expected")
    };
    assert_eq!(legacy, &[vec!["owned_namespace".to_string()]]);
    assert_eq!(typed, &[vec!["logical_db".to_string()]]);
}

#[test]
fn native_mv_management_status_cases_use_the_owned_namespace() {
    for (case, source, catalog) in [
        (
            "mv_atomic_repartition",
            include_str!(
                "../../../../correctness/lnp-3d-mv-accelerator/sql/mv_atomic_repartition.sql"
            ),
            "lnp3d_repart",
        ),
        (
            "mv_retain_restart",
            include_str!("../../../../correctness/lnp-3d-mv-accelerator/sql/mv_retain_restart.sql"),
            "lnp3d_retain",
        ),
    ] {
        let vars =
            crate::config::case_placeholder_variables(&test_variables(&[case]), case).unwrap();
        let rendered = render_expected(source, &vars);
        let old = format!("ns_{}", vars["uuid0"]);
        let map = namespace_mapping(&rendered, &vars).unwrap();
        let expected = &map[&old];
        let rewritten = rewrite_namespaces(&rendered, &vars).unwrap();
        assert!(rewritten.contains(&format!(
            "'{catalog}_{}', '{expected}', 'orders_mv'",
            vars["uuid0"]
        )));
        assert!(!rewritten.contains(&format!("'{old}', 'orders_mv'")));
        assert!(rewritten.contains(&format!(".{expected}.")));
    }
}

#[test]
fn management_status_rewrites_only_its_positional_namespace_literal() {
    let mapping = BTreeMap::from([("owned".into(), "mapped'namespace".into())]);
    let sql = "CALL NoVaRocks_Mv_Management_Status('owned', 'owned', 'owned');";
    assert_eq!(
        rewrite_sql_identifiers(sql, &mapping),
        "CALL NoVaRocks_Mv_Management_Status('owned', 'mapped''namespace', 'owned');"
    );
    for unchanged in [
        "CALL novarocks_mv_management_status(catalog => 'owned', namespace => 'owned', \"table\" => 'owned');",
        "CALL novarocks_mv_management_status('owned', 'owned');",
        "CALL novarocks_mv_management_status('owned', 'owned', 'owned', 'owned');",
        "CALL owned.novarocks_mv_management_status('owned', 'owned', 'owned');",
        "CALL novarocks_mv_management_status('owned', 'owned', 1);",
        "CALL other_procedure('owned', 'owned', 'owned');",
        "SELECT 'owned', 'novarocks_mv_management_status', 'CALL other_procedure(owned)';",
        "-- CALL novarocks_mv_management_status('owned', 'owned', 'owned');\nSELECT 1;",
    ] {
        assert_eq!(rewrite_sql_identifiers(unchanged, &mapping), unchanged);
    }
    let escaped = BTreeMap::from([("old'namespace".into(), "new'namespace".into())]);
    assert_eq!(
        rewrite_sql_identifiers(
            "CALL novarocks_mv_management_status('old''namespace', 'old''namespace', 'old''namespace');",
            &escaped,
        ),
        "CALL novarocks_mv_management_status('old''namespace', 'new''namespace', 'old''namespace');"
    );
}

/// Observes the private fixture namespace and mutation-oracle metadata before execution.
#[test]
fn native_lake_and_mv_cases_use_the_owned_namespace() {
    for (case, source) in [
        (
            "iru5_read_dependency_conflict",
            include_str!(
                "../../../../correctness/lake-publication/sql/iru5_read_dependency_conflict.sql"
            ),
        ),
        (
            "mv_catalog_isolation",
            include_str!(
                "../../../../correctness/lnp-3d-mv-accelerator/sql/mv_catalog_isolation.sql"
            ),
        ),
    ] {
        let mut vars =
            crate::config::case_placeholder_variables(&test_variables(&[case]), case).unwrap();
        // Syntax-only catalog properties are explicit, hermetic input;
        // this test never starts a fixture or reads external environment.
        for (key, value) in [
            ("iceberg_rest_uri", "http://127.0.0.1:1"),
            ("iceberg_rest_warehouse", "s3://syntax-only/warehouse"),
            ("iceberg_object_store_credential_mode", "named"),
            (
                "iceberg_object_store_metadata_credential_name",
                "syntax_metadata",
            ),
            ("iceberg_object_store_data_credential_name", "syntax_data"),
            ("iceberg_object_store_credential_generation", "1"),
            ("oss_endpoint", "http://127.0.0.1:1"),
            ("oss_region", "us-east-1"),
            ("oss_path_style", "true"),
        ] {
            vars.insert(key.into(), value.into());
        }
        let rendered = render_expected(source, &vars);
        let old = format!("ns_{}", vars["uuid0"]);
        let map = namespace_mapping(&rendered, &vars).unwrap();
        let expected = &map[&old];
        // Exercise the real parser pipeline: syntax identifiers first,
        // then metadata filters bound to each exact executable step.
        let bound = load_namespace_bound_case(case, source, &vars);
        let rewritten = bound
            .steps
            .iter()
            .map(|step| step.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rewritten.contains(&format!(".{expected}.")));
        if case == "mv_catalog_isolation" {
            assert!(rewritten.contains(&format!("schema_name = '{expected}'")));
            assert!(!rewritten.contains(&format!("schema_name = '{old}'")));
        } else {
            assert!(rewritten.contains(&format!(".{expected}.optimize_rows EXECUTE optimize")));
            let optimize = bound
                .steps
                .iter()
                .find(|step| {
                    step.sql
                        .contains(&format!(".{expected}.optimize_rows EXECUTE optimize"))
                })
                .expect("OPTIMIZE is prepared against the same owned namespace");
            assert!(optimize.meta.expect_error.is_none());
            assert!(optimize.meta.publication_catalog_fault.is_some());
            let delete = bound
                .steps
                .iter()
                .find(|step| {
                    step.sql
                        .contains(&format!(".{expected}.delete_rows WHERE id = 1"))
                })
                .expect("DELETE is prepared against the same owned namespace");
            assert!(delete.meta.expect_error.is_none());
        }
    }
}

#[test]
fn typed_mv_state_arrays_bind_catalog_elements_and_preserve_business_arrays() {
    use crate::golden::Block;
    let mut vars = test_variables(&["case"]);
    let symbols = crate::catalog_symbol::CatalogSymbols::seal(
        &vars["run_id"],
        &[format!(
            "CREATE CATALOG owned_{} USING iceberg",
            vars["run_id"]
        )],
    )
    .unwrap();
    symbols.extend_variables(&mut vars).unwrap();
    let catalog = format!("owned_{}", vars["run_id"]);
    let mapped = symbols.lookup(&catalog).unwrap();
    let source = format!(
        "-- query 1\n-- types\tarray(varchar)\tarray(varchar)\nbases\tbusiness\n[\"{catalog}.ns.t\"]\t[\"{catalog}.ns.literal\"]\n"
    );
    let mut result = typed_expected(&source);
    let statement = format!(
        "SELECT base_tables AS bases, ARRAY['{catalog}.ns.literal'] AS business FROM system.runtime.materialized_view_states"
    );
    render_expected_for_execution(
        &statement,
        &mut result,
        &vars,
        &BTreeMap::new(),
        std::slice::from_ref(&statement),
    )
    .unwrap();
    let Block::Typed { rows, .. } = &result.golden.unwrap().expanded.blocks[0] else {
        panic!("typed block expected")
    };
    assert_eq!(
        rows[0],
        vec![
            format!(r#"["{mapped}.ns.t"]"#),
            format!(r#"["{catalog}.ns.literal"]"#)
        ]
    );
}

#[test]
fn mv_state_metadata_filters_bind_namespaces_without_rewriting_data() {
    let vars =
        crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
    let source = "-- query 1\nCREATE SCHEMA owned.owned;\n-- query 2\nSELECT name FROM system.runtime.materialized_view_states WHERE schema_name = 'owned';\n-- query 3\nSELECT owned, 'owned', 'system.runtime.materialized_view_states' FROM ordinary;\n-- query 4\nSELECT * FROM owned.t;\n-- query 5\nSELECT * FROM external.owned;\n";
    let map = namespace_mapping(source, &vars).unwrap();
    let expected = &map["owned"];
    let bound = load_namespace_bound_case("case", source, &vars);
    let rewritten = bound
        .steps
        .iter()
        .map(|step| step.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rewritten.contains(&format!("schema_name = '{expected}'")));
    assert!(rewritten.contains(&format!("SELECT * FROM {expected}.t")));
    assert!(rewritten.contains(
        "SELECT owned, 'owned', 'system.runtime.materialized_view_states' FROM ordinary"
    ));
    assert!(rewritten.contains("SELECT * FROM external.owned"));
}

#[test]
fn closed_hms_input_keeps_add_files_family_without_promoting_a_verifier() {
    let mut vars = crate::config::case_placeholder_variables(
        &test_variables(&[crate::hms_case_input::CASE]),
        crate::hms_case_input::CASE,
    )
    .unwrap();
    vars.insert(
        "iceberg_hms_warehouse".into(),
        "s3://novarocks/unit/hms".into(),
    );
    let base = format!("iru7_{}", vars["uuid0"]);
    let original_uri = format!("s3://novarocks/unit/hms/iru7/{base}/v1/data");
    let source = format!(
        r#"-- query 1
shell: python3 "${{NOVAROCKS_WORKSPACE_ROOT:-.}}/tests/sql/fixtures/iceberg-hms/fixture.py" verify-prepared {base} --receipt 'not-executed'
-- query 2
ALTER TABLE c.{base}.v2 EXECUTE add_files(location => '{original_uri}');
SELECT '{original_uri}';
"#
    );
    assert!(namespace_declarations(&source).unwrap().is_empty());
    assert_eq!(rewrite_namespaces(&source, &vars).unwrap(), source);
    vars.insert(
        "__hms_case_input_kind".into(),
        crate::hms_case_input::KIND.into(),
    );
    let error = rewrite_namespaces(&source, &vars).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<TrinoNamespaceBoundary>(),
        Some(TrinoNamespaceBoundary::HmsUri)
    ));
}
#[test]
fn exact_hms_pair_and_trino_writer_bind_references_without_new_observer_ownership() {
    let mut vars =
        crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
    vars.insert(
        "iceberg_hms_warehouse".into(),
        "s3://novarocks/unit/hms".into(),
    );
    let base = format!("iru7_{}", vars["uuid0"]);
    let producer = format!(
        r#"python3 "${{NOVAROCKS_WORKSPACE_ROOT:-.}}/tests/sql/fixtures/iceberg-hms/fixture.py" prepare {base}"#
    );
    let source = format!(
        "-- query 1\nshell: {producer}\n-- query 2\nCREATE SCHEMA c.{base}_new;\nALTER TABLE c.{base}.v2 EXECUTE add_files(location => 's3://novarocks/unit/hms/iru7/{base}/v1/data');\nSELECT 's3://novarocks/unit/hms/iru7/{base}/v1/data';\n-- query 3\nshell: {}\n",
        producer.replace(" prepare ", " after ")
    );
    let mapping = namespace_mapping(&source, &vars).unwrap();
    let error = rewrite_namespaces(&source, &vars).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<TrinoNamespaceBoundary>(),
        Some(TrinoNamespaceBoundary::HmsUri)
    ));
    assert_ne!(mapping[&base], mapping[&format!("{base}_new")]);
    assert!(
        namespace_declarations(&source.replace(" prepare ", " before "))
            .unwrap()
            .iter()
            .all(|name| name != &base)
    );
    let trino = r#""${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/datasketches-tck/interop/trino/verify_rest_catalog.sh""#;
    let source = format!(
        "-- query 1\nshell: {trino} create-parent producer_ns table1\n-- query 2\nSELECT * FROM c.producer_ns.table1;\n-- query 3\nshell: {trino} verify-current producer_ns table1 id 4 3 1 3\n"
    );
    let mapping = namespace_mapping(&source, &vars).unwrap();
    let out = rewrite_namespaces(&source, &vars).unwrap();
    assert!(out.contains(&format!("create-parent {} table1", mapping["producer_ns"])));
    assert!(out.contains(&format!(
        "verify-current {} table1 id 4 3 1 3",
        mapping["producer_ns"]
    )));
    assert!(out.contains(&format!("c.{}.table1", mapping["producer_ns"])));
    assert!(
        namespace_declarations(&source.replace("create-parent", "verify-current"))
            .unwrap()
            .is_empty()
    );
    assert!(
        namespace_declarations("shell: arbitrary --namespace producer_ns")
            .unwrap()
            .is_empty()
    );
}
#[test]
fn delta_tvf_first_argument_uses_namespace_identity_but_ordinary_literals_do_not() {
    let mapping = BTreeMap::from([("old_ns".into(), "canonical_namespace".into())]);
    let sql =
        "SELECT 'c.old_ns.t', ordinary('c.old_ns.t') FROM TABLE(__nr_ivm_delta('c.old_ns.t',0,0));";
    let out = rewrite_delta_namespace_refs(sql, &mapping).unwrap();
    assert!(out.contains("SELECT 'c.old_ns.t', ordinary('c.old_ns.t')"));
    assert!(out.contains("__nr_ivm_delta('c.canonical_namespace.t',0,0)"));
}
#[test]
fn explicit_startup_sweep_skips_private_publications_without_starting_a_command() {
    let root = tempfile::tempdir().unwrap();
    let env_file = root.path().join("env.sh");
    std::fs::write(&env_file, "# private publication\n").unwrap();
    std::fs::write(
        root.path().join("manifest.json"),
        r#"{"ready":true,"runtime":{"catalog":{}}}"#,
    )
    .unwrap();
    let budget = RunBudget::admit(
        FixturePolicy::default(),
        ScratchIdentity::create("s3://novarocks/sql-startup").unwrap(),
        600,
        None,
    )
    .unwrap();
    let mut config = RunnerConfig::default();
    startup_sweep(&config, &root.path().join("missing-repository"), &budget).unwrap();
    config.values.insert(
        "fixture_env_file".into(),
        env_file.to_string_lossy().into_owned(),
    );
    startup_sweep(&config, &root.path().join("missing-repository"), &budget).unwrap();
    assert!(!root.path().join("started").exists());
}
#[test]
fn explicit_startup_sweep_passes_the_exact_original_context_once_for_run_clones() {
    let root = tempfile::tempdir().unwrap();
    let scripts = root.path().join("docker/iceberg-rest");
    std::fs::create_dir_all(&scripts).unwrap();
    std::fs::write(scripts.join("fixture_run.py"),r#"import json,os,pathlib,sys
assert sys.argv[1]=='sweep-start' and sys.argv[2]=='--env-file'
assert json.loads(os.environ['NOVA_FIXTURE_RUN_STORAGE'])['identity']==json.loads(os.environ['NOVA_FIXTURE_RUN_CONTEXT'])['identity']
env=pathlib.Path(sys.argv[3])
with (env.parent/'started').open('a') as output: output.write(os.environ['NOVA_FIXTURE_RUN_CONTEXT']+'\n')
print(json.dumps({'complete':True,'pending':False,'errors':[],'removed':[]}))
"#).unwrap();
    let env_file = root.path().join("env.sh");
    std::fs::write(&env_file, "# pinned publication\n").unwrap();
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../docker/iceberg-rest/tests/run_storage_scope_vectors.json"
    ))
    .unwrap();
    let mut record = vectors["record"].clone();
    record["phase"] = serde_json::json!("ready");
    std::fs::write(
        root.path().join("manifest.json"),
        serde_json::to_vec(
            &serde_json::json!({"ready":true,"shared_docker":true,"runtime":{"machine":record}}),
        )
        .unwrap(),
    )
    .unwrap();
    let budget = RunBudget::admit(
        FixturePolicy::default(),
        ScratchIdentity::create("s3://novarocks/sql-startup").unwrap(),
        600,
        None,
    )
    .unwrap();
    let storage = RunStorageScope::from_record(budget.identity(), &record).unwrap();
    let mut config = RunnerConfig::default();
    config.values.insert(
        "fixture_env_file".into(),
        env_file.to_string_lossy().into_owned(),
    );
    assert!(!root.path().join("started").exists());
    startup_sweep_in_scope(&config, root.path(), &budget, Some(&storage)).unwrap();
    startup_sweep_in_scope(&config, root.path(), &budget.clone(), Some(&storage)).unwrap();
    assert_eq!(
        std::fs::read_to_string(root.path().join("started"))
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        vec![budget.to_json().unwrap()]
    );
}

#[test]
fn cleanup_preserves_original_scope_deadline_and_raw_incomplete_report() {
    let root = tempfile::tempdir().unwrap();
    let scripts = root.path().join("docker/iceberg-rest");
    std::fs::create_dir_all(&scripts).unwrap();
    let script = scripts.join("fixture_run.py");
    std::fs::write(&script, r#"import json,os,pathlib,sys
assert sys.argv[1:3]==['cleanup-run','--env-file']
context=json.loads(os.environ['NOVA_FIXTURE_RUN_CONTEXT'])
storage=json.loads(os.environ['NOVA_FIXTURE_RUN_STORAGE'])
assert context['identity']==storage['identity']
pathlib.Path('cleanup-request.json').write_text(json.dumps({'context':context,'storage':storage,'env':sys.argv[3]}))
print(json.dumps({'complete':True,'catalog_barrier':{context['identity']['run_id']:{'complete':True}}}))
"#).unwrap();
    let env_file = root.path().join("env.sh");
    std::fs::write(&env_file, "# pinned publication\n").unwrap();
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../docker/iceberg-rest/tests/run_storage_scope_vectors.json"
    ))
    .unwrap();
    let budget = RunBudget::admit(
        FixturePolicy::default(),
        ScratchIdentity::create("s3://novarocks/sql-cleanup").unwrap(),
        600,
        None,
    )
    .unwrap();
    let storage = RunStorageScope::from_record(budget.identity(), &vectors["record"]).unwrap();
    let result = cleanup_run(root.path(), &env_file, &budget, &storage).unwrap();
    assert_eq!(result["complete"], true);
    let request: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("cleanup-request.json")).unwrap())
            .unwrap();
    assert_eq!(
        request["context"],
        serde_json::to_value(budget.context()).unwrap()
    );
    assert_eq!(
        request["storage"],
        serde_json::from_str::<serde_json::Value>(&storage.to_json().unwrap()).unwrap()
    );
    std::fs::write(script,"print('{\"complete\":false,\"errors\":[{\"code\":\"CatalogRemovalUnconfirmed\"}]}')\nraise SystemExit(1)\n").unwrap();
    let error = cleanup_run(root.path(), &env_file, &budget, &storage)
        .unwrap_err()
        .to_string();
    assert!(error.contains("CatalogRemovalUnconfirmed"), "{error}");
    assert!(error.contains("diagnostics retained"));
}

#[test]
fn ordinal_names_cover_every_slot_without_truncated_hashes_or_collisions() {
    let run = &test_names("suite", &[]).run_id;
    let mut seen = std::collections::HashSet::new();
    for index in [0, 1, 35, 36, 1295, 1296, CAPACITY - 1] {
        for kind in ['d', 'u', 's'] {
            for slot in 0..10 {
                let name = resource(run, kind, index, slot).unwrap();
                assert_eq!(name.len(), 63);
                assert_eq!(name.rsplit_once('_').unwrap().0, run);
                assert!(seen.insert(name));
            }
        }
    }
    assert!(resource(run, 'd', CAPACITY, 0).is_err());
    assert!(resource(run, 'd', 0, 10).is_err());
    assert_eq!(resource(run, 'd', 36, 2).unwrap(), format!("{run}_d0102"));
}
#[test]
fn explicit_namespace_mapping_preserves_sql_values_columns_and_catalogs() {
    let mut vars = test_variables(&["case"]);
    vars = crate::config::case_placeholder_variables(&vars, "case").unwrap();
    let old = format!("ns_{}", vars["uuid0"]);
    let source = format!(
        "CREATE SCHEMA catalog.{old};\nCREATE TABLE catalog.{old}.t (id INT);\nSELECT '{old}' AS value, {old} FROM ordinary;\nCREATE CATALOG {old} USING iceberg;\n-- @db={old}\nshell: trino --namespace {old}\n"
    );
    let rewritten = rewrite_namespaces(&source, &vars).unwrap();
    let db = resource(&vars["run_id"], 'd', 0, 1).unwrap();
    assert!(rewritten.contains(&format!("CREATE SCHEMA catalog.{db};")));
    assert!(rewritten.contains(&format!("catalog.{db}.t")));
    assert!(rewritten.contains(&format!("SELECT '{old}' AS value, {old} FROM ordinary")));
    assert!(rewritten.contains(&format!("CREATE CATALOG {old}")));
    assert!(rewritten.contains(&format!("-- @db={db}")));
    assert!(rewritten.contains(&format!("--namespace {db}")));
}
fn variant_command(mode: &str, namespace: &str, table: &str) -> String {
    format!(
        r#""${{NOVAROCKS_WORKSPACE_ROOT:-.}}/tests/sql/fixtures/iceberg-variant/run.sh" --mode {mode} --namespace {namespace} --table {table} --output "$receipt_dir" >"$receipt_dir/run.log""#
    )
}
#[test]
fn only_exact_variant_writer_invocations_declare_shell_namespace_ownership() {
    let writer = variant_command("empty-int", "iru9_${uuid0}", "target");
    assert_eq!(
        namespace_declarations(&format!("-- query 1\nshell: set -eu\n{writer}\n")).unwrap(),
        BTreeSet::from(["iru9_${uuid0}".into()])
    );
    for unowned in [
        writer.clone(),
        format!("shell: echo {writer}\n"),
        format!("shell: {writer} && other-command\n"),
        format!(
            "shell: {}\n",
            writer.replace("iceberg-variant/", "other-producer/")
        ),
        format!(
            "shell: {}\n",
            writer.replace("--mode empty-int", "--mode unknown")
        ),
        format!(
            "shell: {}\n",
            variant_command("observe", "old_namespace", "target")
        ),
        format!(
            "shell: {}\n",
            variant_command("unchanged", "old_namespace", "target")
        ),
        format!("shell: set -eu\n-- query 2\n{writer}\n"),
    ] {
        assert!(
            namespace_declarations(&unowned).unwrap().is_empty(),
            "{unowned}"
        );
    }
}
#[test]
fn variant_writer_and_readonly_arguments_share_mapping_without_changing_other_shell_values() {
    let vars =
        crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
    let source = format!(
        "-- query 1\nshell: set -eu\n{}\n{}\n{}\necho original_namespace\nother-producer --namespace original_namespace\n-- query 2\nSELECT * FROM catalog.original_namespace.target;\nSELECT 'original_namespace' AS value;\n",
        variant_command("empty-int", "original_namespace", "target"),
        variant_command("observe", "original_namespace", "target"),
        variant_command("unchanged", "original_namespace", "target"),
    );
    let mapping = namespace_mapping(&source, &vars).unwrap();
    let expected = resource(&vars["run_id"], 'd', 0, 1).unwrap();
    assert_eq!(
        mapping,
        BTreeMap::from([("original_namespace".into(), expected.clone())])
    );
    let rewritten = rewrite_namespaces(&source, &vars).unwrap();
    for mode in ["empty-int", "observe", "unchanged"] {
        assert!(rewritten.contains(&variant_command(mode, &expected, "target")));
    }
    assert!(rewritten.contains(&format!("catalog.{expected}.target")));
    assert!(rewritten.contains("SELECT 'original_namespace' AS value"));
    assert!(rewritten.contains("echo original_namespace"));
    assert!(rewritten.contains("other-producer --namespace original_namespace"));
}
#[test]
fn independent_variant_namespaces_receive_distinct_bounded_slots() {
    let vars =
        crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
    let source = format!(
        "shell: set -eu\n{}\n{}\n",
        variant_command("empty-int", "first", "t"),
        variant_command("empty-bigint", "second", "t")
    );
    let map = namespace_mapping(&source, &vars).unwrap();
    assert_eq!(map.len(), 2);
    assert_ne!(map["first"], map["second"]);
    assert!(
        map.values()
            .all(|name| name.starts_with(&format!("{}_", vars["run_id"])) && name.len() <= 63)
    );
}
#[test]
fn existing_variant_dml_cases_bind_the_original_producer_namespace_before_writes() {
    for (case, source) in [
        (
            "variant_get",
            include_str!("../../../../correctness/iceberg-dml/sql/variant_get.sql"),
        ),
        (
            "variant_insert",
            include_str!("../../../../correctness/iceberg-dml/sql/variant_insert.sql"),
        ),
        (
            "variant_mutations",
            include_str!("../../../../correctness/iceberg-dml/sql/variant_mutations.sql"),
        ),
    ] {
        assert_eq!(
            namespace_declarations(source).unwrap(),
            BTreeSet::from(["iru9_${uuid0}".into()])
        );
        let vars =
            crate::config::case_placeholder_variables(&test_variables(&[case]), case).unwrap();
        let rendered = render_expected(source, &vars);
        let old = format!("iru9_{}", vars["uuid0"]);
        let map = namespace_mapping(&rendered, &vars).unwrap();
        let expected = &map[&old];
        assert!(expected.starts_with(&format!("{}_", vars["run_id"])) && expected.len() <= 63);
        let rewritten = rewrite_namespaces(&rendered, &vars).unwrap();
        assert!(rewritten.contains(&format!("--namespace {expected} --table")));
        assert!(!rewritten.contains(&format!("--namespace {old} --table")));
        assert!(rewritten.contains(&format!(".{expected}.")));
    }
}
#[test]
fn branch_qualified_tables_share_namespace_mapping_without_changing_literals() {
    let vars =
        crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
    let old = format!("ns_{}", vars["uuid0"]);
    let source = format!(
        "CREATE SCHEMA catalog.{old};\nCREATE TABLE catalog.{old}.t (id INT);\nINSERT INTO catalog.{old}.t@dev VALUES (1);\nUPDATE catalog.\"{old}\".\"t\"@\"dev\" SET id=2;\nDELETE FROM {old}.t@dev WHERE id=2;\nSELECT * FROM catalog.{old}.t FOR VERSION AS OF 'snapshot';\nSELECT '{old}', 'catalog.{old}.t.branch_dev' AS data FROM ordinary;\n"
    );
    let rewritten = rewrite_namespaces(&source, &vars).unwrap();
    let db = resource(&vars["run_id"], 'd', 0, 1).unwrap();
    assert!(rewritten.contains(&format!("catalog.{db}.t@dev")));
    assert!(rewritten.contains(&format!(r#"catalog."{db}"."t"@"dev""#)));
    assert!(rewritten.contains(&format!("DELETE FROM {db}.t@dev")));
    assert!(rewritten.contains(&format!("catalog.{db}.t FOR VERSION AS OF 'snapshot'")));
    assert!(rewritten.contains(&format!(
        "SELECT '{old}', 'catalog.{old}.t.branch_dev' AS data"
    )));
}
#[test]
fn multiline_database_names_and_golden_references_use_the_same_mapping() {
    let vars =
        crate::config::case_placeholder_variables(&test_variables(&["case"]), "case").unwrap();
    let sql = "-- comment\nCREATE SCHEMA\n catalog.raw_namespace;\nSELECT CURRENT_SCHEMA, raw_namespace FROM t;\n";
    let mapping = namespace_mapping(sql, &vars).unwrap();
    let expected = resource(&vars["run_id"], 'd', 0, 1).unwrap();
    let rewritten = rewrite_namespaces(sql, &vars).unwrap();
    assert!(rewritten.contains(&format!("catalog.{expected};")));
    assert!(rewritten.contains("SELECT CURRENT_SCHEMA, raw_namespace FROM t"));
    let mut result = crate::types::ResultSet {
        golden: None,
        header: vec!["schema_name".into(), "value".into()],
        rows: vec![vec!["raw_namespace".into(), "raw_namespace".into()]],
    };
    render_expected_result(
        "SELECT schema_name, 'raw_namespace' AS value FROM information_schema.schemata",
        &mut result,
        &vars,
        &mapping,
    )
    .unwrap();
    assert_eq!(result.rows[0], vec![expected, "raw_namespace".to_string()]);
}
#[test]
fn independent_invocations_and_case_ordinals_never_share_database_or_uuid_names() {
    let left = test_variables(&["same", "other"]);
    let mut right = left.clone();
    right.insert(
        "suite_run_id".into(),
        "nr_scratch_1792540800000_fedcba9876543210fedcba9876543210".into(),
    );
    let a = crate::config::case_placeholder_variables(&left, "same").unwrap();
    let b = crate::config::case_placeholder_variables(&right, "same").unwrap();
    let c = crate::config::case_placeholder_variables(&left, "other").unwrap();
    assert_ne!(a["case_db"], b["case_db"]);
    assert_ne!(a["case_db"], c["case_db"]);
    assert_ne!(a["uuid0"], b["uuid0"]);
    assert_ne!(a["uuid0"], c["uuid0"]);
    assert_eq!(a["suite_uuid0"], c["suite_uuid0"]);
    assert_eq!(
        a["case_db"],
        crate::config::case_auto_db_name(&a["run_id"], 0).unwrap()
    );
}
#[test]
fn metadata_namespace_filters_and_expected_cells_do_not_rewrite_data_literals() {
    let mapping = BTreeMap::from([("old_ns".into(), "new_ns".into())]);
    let sql = "SELECT schema_name, 'old_ns' AS data_value FROM information_schema.schemata WHERE schema_name = 'old_ns'";
    let actual = rewrite_metadata_filters(sql, &mapping);
    assert!(actual.contains("'old_ns' AS data_value"));
    assert!(actual.contains("schema_name = 'new_ns'"));
    assert_eq!(
        rewrite_metadata_filters(
            "SELECT 'old_ns' FROM ordinary WHERE schema_name = 'old_ns'",
            &mapping
        ),
        "SELECT 'old_ns' FROM ordinary WHERE schema_name = 'old_ns'"
    );
    let mut result = crate::types::ResultSet {
        golden: None,
        header: vec!["schema_name".into(), "data_value".into()],
        rows: vec![vec!["old_ns".into(), "old_ns".into()]],
    };
    render_expected_result(&actual, &mut result, &HashMap::new(), &mapping).unwrap();
    assert_eq!(result.rows[0], vec!["new_ns", "old_ns"]);
}
#[test]
fn views_and_jdbc_metadata_filters_bind_fixture_schema_literals() {
    let mapping = BTreeMap::from([("old_ns".into(), "new_ns".into())]);
    for (relation, schema_column) in [
        ("owned.information_schema.views", "table_schema"),
        ("system.jdbc.schemas", "table_schem"),
        ("system.jdbc.tables", "table_schem"),
        ("system.jdbc.columns", "table_schem"),
    ] {
        let sql = format!(
            "SELECT {schema_column}, 'old_ns' AS data_value FROM {relation} WHERE {schema_column} = 'old_ns'"
        );
        let actual = rewrite_metadata_filters(&sql, &mapping);
        assert!(actual.contains(&format!("WHERE {schema_column} = 'new_ns'")));
        assert!(actual.contains("'old_ns' AS data_value"));
        assert_eq!(
            metadata_columns(&sql),
            BTreeSet::from([schema_column.to_owned()])
        );
    }
}
#[test]
fn standard_mv_metadata_maps_schema_and_storage_schema_without_changing_data() {
    let mapping = BTreeMap::from([("old_ns".into(), "new_ns".into())]);
    let sql = "SELECT schema_name, storage_schema AS stored_db, 'old_ns' AS business FROM system.metadata.materialized_views WHERE schema_name = 'old_ns' AND storage_schema = 'old_ns'";
    let actual = rewrite_metadata_filters(sql, &mapping);
    assert!(actual.contains("schema_name = 'new_ns'"));
    assert!(actual.contains("storage_schema = 'new_ns'"));
    assert!(actual.contains("'old_ns' AS business"));
    let mut result = crate::types::ResultSet {
        golden: None,
        header: vec!["schema_name".into(), "stored_db".into(), "business".into()],
        rows: vec![vec!["old_ns".into(); 3]],
    };
    render_expected_result(&actual, &mut result, &HashMap::new(), &mapping).unwrap();
    assert_eq!(result.rows[0], vec!["new_ns", "new_ns", "old_ns"]);
    let ordinary = "SELECT 'old_ns' FROM ordinary WHERE storage_schema = 'old_ns'";
    assert_eq!(rewrite_metadata_filters(ordinary, &mapping), ordinary);
    assert!(
        metadata_projection("SELECT name FROM information_schema.materialized_views").is_none()
    );
}
#[test]
fn namespace_metadata_and_data_projection_ambiguity_rejects_before_scratch() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("case.sql");
    std::fs::write(&file,"CREATE SCHEMA ns;\nSELECT schema_name, 'ns' AS schema_name FROM information_schema.schemata;").unwrap();
    let run = RunBudget::admit(
        FixturePolicy::default(),
        ScratchIdentity::create("s3://novarocks/test").unwrap(),
        600,
        None,
    )
    .unwrap();
    assert!(
        RunNames::prepare(&run, &BTreeMap::from([("suite".into(), vec![file])]))
            .unwrap_err()
            .to_string()
            .contains("metadata/data")
    );
}
#[test]
fn namespace_alias_ambiguity_is_rejected_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("case.sql");
    let run = RunBudget::admit(
        FixturePolicy::default(),
        ScratchIdentity::create("s3://novarocks/test").unwrap(),
        600,
        None,
    )
    .unwrap();
    for query in [
        "SELECT ns.id FROM t AS ns;",
        "SELECT ns.id FROM t ns;",
        "SELECT ns.id FROM (SELECT 1) ns;",
    ] {
        std::fs::write(&file, format!("CREATE SCHEMA ns;\n{query}\n")).unwrap();
        assert!(
            RunNames::prepare(
                &run,
                &BTreeMap::from([("suite".into(), vec![file.clone()])])
            )
            .unwrap_err()
            .to_string()
            .contains("ambiguous"),
            "{query}"
        );
    }
}
#[test]
fn namespace_capacity_and_duplicate_case_identity_fail_before_side_effects() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("case.sql");
    std::fs::write(
        &file,
        (0..10)
            .map(|n| format!("CREATE SCHEMA ns{n};\n"))
            .collect::<String>(),
    )
    .unwrap();
    let run = RunBudget::admit(
        FixturePolicy::default(),
        ScratchIdentity::create("s3://novarocks/test").unwrap(),
        600,
        None,
    )
    .unwrap();
    assert!(
        RunNames::prepare(
            &run,
            &BTreeMap::from([("suite".into(), vec![file.clone()])])
        )
        .is_err()
    );
    std::fs::write(&file, "SELECT 1;").unwrap();
    assert!(
        RunNames::prepare(
            &run,
            &BTreeMap::from([("suite".into(), vec![file.clone(), file])])
        )
        .is_err()
    );
}
