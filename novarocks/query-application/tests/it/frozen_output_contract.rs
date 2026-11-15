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
use super::fixtures::{freeze, freeze_root_purpose, table_free_sql};
use arrow::datatypes::DataType;
use novarocks_query_application::{
    api::QueryExecutionKind,
    preparation::{CompletedPhysicalPlanCandidate, OutputContract},
};
use novarocks_sql::compiler::SqlCompileControl;
use novarocks_sql_testkit::sql_source as nonnull_source;
use std::ptr;
#[test]
fn numeric_unary_public_transport_same_original_source_and_all_consumers() {
    for dt in [DataType::Int8, DataType::Int16, DataType::Int32] {
        let sql = "SELECT -k AS original_negated, k AS original_source, -k AS repeated_alias FROM fixture";
        let original = nonnull_source(sql, dt.clone());
        let source = nonnull_source(sql, dt.clone());
        let borrowed = source
            .checked_original_result_declaration_observed(&SqlCompileControl::unbounded())
            .unwrap()
            .unwrap();
        let original_port = original.plan().result_port().unwrap();
        let computed = source.plan().result_port().unwrap();
        assert_eq!(borrowed.original_port().fragment, computed.fragment);
        assert_eq!(borrowed.original_port().output, computed.output);
        assert_eq!(borrowed.fields().len(), 3);
        for (n, (before, public)) in original_port
            .fields
            .iter()
            .zip(borrowed.fields())
            .enumerate()
        {
            assert_eq!(before, public, "same original ordered declaration at {n}");
            assert_eq!(public.ty.data_type().clone(), dt);
            assert!(!public.ty.nullable());
            assert_eq!(public.ty.nullable(), computed.fields[n].ty.nullable());
            assert_eq!(public.value, computed.fields[n].value);
            assert_eq!(public.alias, computed.fields[n].alias);
            assert_eq!(public.name, computed.fields[n].name);
            assert_eq!(
                public.ty.logical_type(),
                computed.fields[n].ty.logical_type()
            );
            assert_eq!(public.ty.data_type(), computed.fields[n].ty.data_type());
        }
        assert!(!computed.fields[0].ty.nullable());
        assert!(!computed.fields[1].ty.nullable());
        assert!(!computed.fields[2].ty.nullable());
        let original_candidate = freeze_root_purpose(
            CompletedPhysicalPlanCandidate::for_sql_program(
                original,
                &SqlCompileControl::unbounded(),
            )
            .unwrap(),
        );
        let original_output =
            OutputContract::from_completed_candidate(QueryExecutionKind::Read, &original_candidate)
                .unwrap();
        // Clone shares the publication-proved original port, never copies its fields.
        let cloned = source.clone();
        assert!(ptr::eq(
            borrowed.original_port(),
            cloned
                .original_public_result_declaration()
                .unwrap()
                .original_port()
        ));
        drop(cloned);
        let candidate = CompletedPhysicalPlanCandidate::for_sql_program(
            source,
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
        // Preserve the original false-public-obligation rejection at the
        // exact root boundary. Only the two computed negations are forged.
        let wrong_candidate = CompletedPhysicalPlanCandidate::for_sql_program(
            nonnull_source(sql, dt.clone()),
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
        let render = novarocks_sql::compiler::client_render_schema_from_port(
            wrong_candidate.original_public_result_port(),
            novarocks_sql::compiler::RenderFamilyRequest::TrinoJson(
                &novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    variant: true,
                    variant_binary: true,
                },
            ),
        )
        .unwrap();
        let mut wrong_columns = render.columns().to_vec();
        wrong_columns[0].field.nullable = true;
        wrong_columns[2].field.nullable = true;
        let wrong_render = novarocks_result_contract::ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    variant: true,
                    variant_binary: true,
                },
            ),
            wrong_columns,
            3,
        )
        .unwrap();
        let refusal = wrong_candidate
            .freeze_root_output(novarocks_result_contract::FrozenRootOutput::ClientRows(
                wrong_render,
            ))
            .unwrap_err()
            .to_string();
        assert!(refusal.contains("result_port.render_schema"), "{refusal}");
        assert!(
            refusal.contains("render occurrence differs at ordinal 0"),
            "{refusal}"
        );
        assert!(
            refusal.contains("render occurrence differs at ordinal 2"),
            "{refusal}"
        );
        let candidate = freeze_root_purpose(candidate);
        let output =
            OutputContract::from_completed_candidate(QueryExecutionKind::Read, &candidate).unwrap();
        assert_eq!(output.fields().len(), 3);
        assert_eq!(output.fields(), original_output.fields());
        for (field, label) in
            output
                .fields()
                .iter()
                .zip(["original_negated", "original_source", "repeated_alias"])
        {
            assert_eq!(field.name(), label);
            assert_eq!(field.data_type(), &dt);
            assert!(!field.nullable());
        }
        let from_plan =
            OutputContract::from_completed_plan(QueryExecutionKind::Read, candidate.plan())
                .unwrap();
        assert_eq!(from_plan.fields(), output.fields());
        let wrong = OutputContract::Rows(
            output
                .fields()
                .iter()
                .map(|field| {
                    novarocks_query_application::api::ResultField::new(
                        field.name(),
                        field.value_type().with_nullable(true).unwrap(),
                    )
                })
                .collect::<Vec<_>>()
                .into(),
        );
        assert_eq!(
            freeze(candidate.clone(), wrong).unwrap_err(),
            "completed plan output differs from its frozen execution contract"
        );
        let published = freeze(candidate.clone(), output.clone()).unwrap();
        assert_eq!(published.output().fields(), output.fields());
        assert!(ptr::eq(
            candidate.original_public_result_port().unwrap(),
            candidate.clone().original_public_result_port().unwrap()
        ));
    }
}
#[test]
fn numeric_unary_public_transport_original_and_unchanged_nested_types_use_actual_source() {
    for sql in [
        "SELECT 1 AS a, CAST(NULL AS VARCHAR) AS b",
        "SELECT CAST([CAST(1 AS INT)] AS ARRAY<INT>) AS nested",
    ] {
        {
            let source = table_free_sql(sql);
            let port = source.plan().result_port().unwrap();
            let original = source.original_public_result_declaration().unwrap();
            assert_eq!(original.fields(), port.fields.as_ref());

            let candidate = CompletedPhysicalPlanCandidate::for_sql_program(
                source,
                &SqlCompileControl::unbounded(),
            )
            .unwrap();
            let candidate = freeze_root_purpose(candidate);
            let out =
                OutputContract::from_completed_candidate(QueryExecutionKind::Read, &candidate)
                    .unwrap();
            assert!(freeze(candidate, out).is_ok());
        }
    }
}
