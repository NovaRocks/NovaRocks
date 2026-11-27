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

//! Signed IN source/ordered lifecycle probes. UNRUN until serialized root tests.
//! Formal host MEM grants and the Utf8 Variant environment remain separate gates.
use arrow::datatypes::{DataType, Field};
use novarocks_expression::StaticExprKind;
use novarocks_sql_testkit::{
    installed_builtin_owner_catalogue, signed_inlist_source,
    sql_source_with_single_field_and_core_residuals,
};
use novarocks_test_compile_to_execute::programs::compiled_signed_inlist_root;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, PureCompileControl,
};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
#[test]
fn inlist_signed_actual_source_definitions_ordered_uses_guards_and_exact_recipe() {
    let (program, site) = compiled_signed_inlist_root(
        &signed_inlist_source("SELECT k IN (k,-k,k) FROM t0", DataType::Int64),
        &installed_builtin_owner_catalogue(),
    );
    let unit = program.unit(site.unit).unwrap();
    let flow = unit.control_flow();
    let occurrence = &flow.uses()[&unit.root(site.root).unwrap().use_id];
    assert_eq!(
        occurrence.control,
        ControlShape::Membership { negated: false }
    );
    assert_eq!(occurrence.arguments.len(), 4);
    assert_ne!(occurrence.arguments[0], occurrence.arguments[1]);
    assert_ne!(occurrence.arguments[1], occurrence.arguments[3]);
    let definition = unit.definitions().node(occurrence.definition).unwrap();
    let StaticExprKind::PreparedInList { child, values, .. } = definition.kind() else {
        panic!("actual original IN definition");
    };
    // SQL's original lowering owns each definition. Repeated lexical `k`
    // does not authorize this test to invent CSE or an aliasing receipt.
    assert_eq!(flow.uses()[&occurrence.arguments[0]].definition, *child);
    assert_eq!(values.len(), 3);
    for (ordinal, candidate) in values.iter().enumerate() {
        assert_eq!(
            flow.uses()[&occurrence.arguments[ordinal + 1]].definition,
            *candidate
        );
    }
    for ordinal in 1..4 {
        let child = &flow.uses()[&occurrence.arguments[ordinal]];
        assert_eq!(
            child.context.demand,
            novarocks_type_contract::EvaluationDemand::Value
        );
        assert_eq!(
            flow.regions()[&child.context.region].guard.unwrap().kind,
            novarocks_type_contract::GuardKind::MembershipAfterSource {
                ordinal: (ordinal - 1) as u32
            }
        );
    }
    let recipe = unit
        .recipes()
        .native_inlist(unit.root(site.root).unwrap().use_id)
        .unwrap();
    assert_eq!(recipe.source_type().data_type().clone(), DataType::Int64);
    assert_eq!(recipe.candidate_types().len(), 3);
    assert_eq!(recipe.result_type().data_type().clone(), DataType::Boolean);
    assert!(recipe.result_type().nullable());
}

#[test]
fn inlist_signed_actual_original_control_codec_roundtrip_and_invalid_ordered_use() {
    for (sql, negated) in [
        ("SELECT k IN (1,5,NULL) FROM t0", false),
        ("SELECT k NOT IN (1,5,NULL) FROM t0", true),
    ] {
        let source = sql_source_with_single_field_and_core_residuals(
            sql,
            Field::new("k", DataType::Int64, true),
        );
        let mut seen = 0;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                let Some(unit) = fragment.unit(node.id) else {
                    continue;
                };
                let mut encoded =
                    novarocks_plan_codec::physical_control_v2::encode_expression_control(
                        unit.control_flow(),
                        &Control,
                    )
                    .unwrap();
                let decoded = novarocks_plan_codec::physical_control_v2::decode_expression_control(
                    unit.definitions(),
                    &encoded,
                    &Control,
                )
                .unwrap();
                assert_eq!(&decoded, unit.control_flow());
                assert_eq!(decoded.root_use_ids(), unit.control_flow().root_use_ids());
                // Root declarations are unit-owned; control wire retains uses.
                for (root, use_id) in unit.roots().iter().zip(unit.root_use_ids()) {
                    assert_eq!(decoded.uses()[use_id].definition, root.definition);
                    assert_eq!(decoded.uses()[use_id].context.demand, root.demand);
                }
                let before = seen;
                for (&id, invocation) in unit.control_flow().uses() {
                    if invocation.control == (ControlShape::Membership { negated }) {
                        let emitted = encoded
                            .uses
                            .iter_mut()
                            .find(|entry| entry.id == id.get())
                            .unwrap();
                        assert_eq!(emitted.argument_use_ids.len(), 4);
                        emitted.argument_use_ids.pop();
                        seen += 1;
                    }
                }
                if seen != before {
                    assert!(
                        novarocks_plan_codec::physical_control_v2::decode_expression_control(
                            unit.definitions(),
                            &encoded,
                            &Control
                        )
                        .is_err()
                    );
                }
            }
        }
        assert_eq!(seen, 1);
    }
}
