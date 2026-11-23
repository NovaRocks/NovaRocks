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
use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_sql::compiler::{
    CatalogRelationFact, CatalogRelationNeed, SqlCompilation, SqlCompileControl, SqlCompileIntent,
    SqlCompileProgress, SqlCompileProgressError, SqlCompiler, SqlFactBatch, SqlNeedBatch,
    SqlStatementInput, SystemTableSnapshotFact,
};
use novarocks_sql::system_table::{
    StatementSystemRelation, SystemTableBinding, SystemTableColumn, SystemTableDeclaration,
    SystemTableLookupError,
};
use novarocks_sql_testkit::parse_query;
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};

fn declarations(need: &CatalogRelationNeed) -> Arc<SystemTableDeclaration> {
    Arc::new(SystemTableDeclaration {
        binding: SystemTableBinding::System,
        schema: need.relation().namespace.clone(),
        table: need.relation().table.clone(),
        columns: [
            ("public_name", false),
            ("hidden_detail", true),
            ("public_state", false),
        ]
        .map(|(name, hidden)| SystemTableColumn {
            name: name.to_owned(),
            hidden,
            value_type: FunctionValueType::new(DataType::Utf8, true),
        })
        .into(),
        key_columns: Arc::from([0]),
    })
}

fn start(statement: SqlStatementInput) -> SqlCompileProgress {
    let control = SqlCompileControl::unbounded();
    let request = crate::fixtures::plan_request(
        statement,
        SqlCompileIntent::Query,
        crate::fixtures::session(Default::default(), false),
        &control,
    )
    .try_into_completion()
    .unwrap();
    SqlCompiler::start(request, &control).unwrap()
}

fn pending(progress: SqlCompileProgress) -> SqlCompilation {
    let SqlCompileProgress::Incomplete(compilation) = progress else {
        panic!("expected a completion fact request");
    };
    compilation
}

fn lookup(sql: &str) -> Result<SqlCompileProgress, SqlCompileProgressError> {
    let compilation = pending(start(SqlStatementInput::parsed_query(parse_query(sql))));
    let SqlNeedBatch::CatalogRelations(needs) = compilation.needs() else {
        panic!("expected public catalog declaration needs");
    };
    let facts = needs
        .iter()
        .map(|need| {
            let result = if need.relation().table == "fixture" {
                Ok(declarations(need))
            } else {
                Err(SystemTableLookupError::TableNotFound)
            };
            CatalogRelationFact::system_table(need, result).unwrap()
        })
        .collect::<Vec<_>>();
    SqlCompiler::finish(
        compilation,
        SqlFactBatch::CatalogRelations(facts.into()),
        &SqlCompileControl::unbounded(),
    )
}

fn fields(progress: SqlCompileProgress) -> Vec<String> {
    let compilation = pending(progress);
    let SqlNeedBatch::ProviderInputs {
        reads,
        system_tables,
    } = compilation.needs()
    else {
        panic!("expected actual system table needs");
    };
    assert!(reads.is_empty());
    let snapshots = system_tables
        .iter()
        .map(|need| {
            SystemTableSnapshotFact::try_new(need, Arc::from([]), ValueTypeStorageLifetime::pure())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let completed = SqlCompiler::finish(
        compilation,
        SqlFactBatch::ProviderInputs {
            reads: Box::default(),
            system_tables: snapshots.into(),
        },
        &SqlCompileControl::unbounded(),
    )
    .unwrap()
    .into_complete()
    .unwrap();
    completed
        .plan()
        .result_port()
        .unwrap()
        .fields
        .iter()
        .map(|field| field.alias.as_deref().unwrap_or(&field.name).to_owned())
        .collect()
}

#[test]
fn star_and_aliases_use_visible_system_fields_in_declaration_order() {
    for (sql, expected) in [
        (
            "SELECT * FROM system.metadata.fixture",
            ["public_name", "public_state"],
        ),
        (
            "SELECT a.* FROM system.metadata.fixture a",
            ["public_name", "public_state"],
        ),
        (
            "SELECT a.* FROM system.metadata.fixture a(name, state)",
            ["name", "state"],
        ),
    ] {
        assert_eq!(fields(lookup(sql).unwrap()), expected);
    }
    let compilation =
        pending(lookup("SELECT a.state FROM system.metadata.fixture a(name, state)").unwrap());
    let SqlNeedBatch::ProviderInputs { system_tables, .. } = compilation.needs() else {
        panic!();
    };
    assert_eq!(system_tables[0].required_columns(), &[2]);

    for sql in [
        "SELECT a.hidden_detail FROM system.metadata.fixture a(name, state)",
        "SELECT * FROM system.metadata.fixture a(name, middle, state)",
    ] {
        assert!(lookup(sql).is_err(), "{sql}");
    }
}

#[test]
fn hidden_system_field_is_explicitly_resolvable_and_has_original_ordinal() {
    for sql in [
        "SELECT hidden_detail FROM system.metadata.fixture",
        "SELECT a.hidden_detail FROM system.metadata.fixture a",
    ] {
        let progress = lookup(sql).unwrap();
        let compilation = pending(progress);
        let SqlNeedBatch::ProviderInputs { system_tables, .. } = compilation.needs() else {
            panic!();
        };
        assert_eq!(system_tables[0].required_columns(), &[1]);
        assert_eq!(fields(lookup(sql).unwrap()), ["hidden_detail"]);
    }
    assert_eq!(
        fields(
            lookup("SELECT public_name FROM system.metadata.fixture ORDER BY hidden_detail")
                .unwrap()
        ),
        ["public_name"],
    );
    assert_eq!(
        fields(lookup(
            "SELECT * FROM system.metadata.fixture a JOIN system.metadata.fixture b USING(hidden_detail)"
        ).unwrap()),
        ["hidden_detail", "public_name", "public_state", "public_name", "public_state"],
    );
}

#[test]
fn private_system_relation_requires_original_statement_input() {
    let sql = "SELECT catalog_name FROM system.metadata.private_label";
    let source = StatementSystemRelation::ShowCatalogs;
    let declaration = Arc::new(SystemTableDeclaration {
        binding: SystemTableBinding::System,
        schema: "metadata".to_owned(),
        table: "private_label".to_owned(),
        columns: Arc::from([SystemTableColumn {
            name: "catalog_name".to_owned(),
            hidden: false,
            value_type: FunctionValueType::new(DataType::Utf8, false),
        }]),
        key_columns: Arc::from([]),
    });
    let statement = SqlStatementInput::query_with_statement_system_relation(
        parse_query(sql),
        source,
        declaration.clone(),
    )
    .unwrap();
    let progress = start(statement);
    let compilation = pending(progress);
    let SqlNeedBatch::ProviderInputs { system_tables, .. } = compilation.needs() else {
        panic!("private FROM should proceed directly to snapshot collection");
    };
    assert_eq!(system_tables.len(), 1);
    assert_eq!(system_tables[0].statement_relation(), Some(source));
    assert_eq!(system_tables[0].declaration(), &declaration);
    assert_eq!(system_tables[0].required_columns(), &[0]);
    assert!(lookup(sql).is_err());

    let nested_public_lookup = SqlStatementInput::query_with_statement_system_relation(
        parse_query(
            "SELECT catalog_name, (SELECT count(*) FROM system.metadata.private_label) FROM system.metadata.private_label",
        ),
        source,
        declaration,
    )
    .unwrap();
    let compilation = pending(start(nested_public_lookup));
    let SqlNeedBatch::CatalogRelations(needs) = compilation.needs() else {
        panic!("the same spelling in a nested query still needs public resolution");
    };
    assert_eq!(needs.len(), 1);
    assert_eq!(needs[0].relation().table, "private_label");
}
