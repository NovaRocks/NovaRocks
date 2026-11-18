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
// Transitional re-export for the remaining Execution SQL tests.
// Shared inputs live in novarocks-sql-testkit; remove this bridge after migration.
pub(super) use novarocks_sql_testkit::{
    sql_source, sql_source_with_declared_fields, sql_source_with_semantics,
    sql_source_with_single_field, sql_source_with_single_field_and_core_residuals,
    try_sql_source_with_declared_fields,
};

/// Borrow definitions with their actual node and unit namespace; no table is rebuilt.
pub(super) fn unit_definitions(
    fragment: &novarocks_physical_plan::Fragment,
) -> impl Iterator<
    Item = (
        &novarocks_physical_plan::PhysicalNode,
        &novarocks_expression_contract::BoundExprSet,
        &novarocks_expression_contract::ExprId,
        &novarocks_expression_contract::ExprNode,
    ),
> {
    fragment.nodes().values().flat_map(|node| {
        node.expressions.iter().flat_map(move |unit| {
            unit.definitions()
                .iter()
                .map(move |definition| (node, unit.as_ref(), &definition.id, definition))
        })
    })
}

/// Enumerate actual retained roots without a fragment-global expression bank.
pub(super) fn compiled_roots(
    program: &novarocks_local_program::LocalProgram,
) -> impl Iterator<
    Item = (
        novarocks_local_program::ProgramRootRef,
        &std::sync::Arc<novarocks_expression::CompiledExprSet>,
        &novarocks_expression::CompiledRoot,
    ),
> {
    program
        .units()
        .iter()
        .enumerate()
        .flat_map(|(index, unit)| {
            unit.roots().iter().enumerate().map(move |(ordinal, root)| {
                (
                    novarocks_local_program::ProgramRootRef::new(
                        novarocks_local_program::ProgramUnitId::new(index),
                        novarocks_expression_contract::RootOrdinal::new(
                            u32::try_from(ordinal).unwrap(),
                        ),
                    ),
                    unit,
                    root,
                )
            })
        })
}

/// Resolve a real Input use through its root's actual Project input layout.
pub(super) fn project_input_slot_for_use(
    program: &novarocks_local_program::LocalProgram,
    site: novarocks_local_program::ProgramRootRef,
    use_id: novarocks_type_contract::ExpressionUseId,
) -> novarocks_types::SlotId {
    let unit = program.unit(site.unit).unwrap();
    let root = unit.root(site.root).unwrap();
    let binding = unit.input(use_id).expect("actual compiled Input use");
    assert_eq!(binding.scope, root.scope);
    let input = program
        .graph()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            novarocks_local_program::ProgramNodeKind::Project { input, exprs, .. }
                if exprs.contains(&site) =>
            {
                Some(*input)
            }
            _ => None,
        })
        .expect("actual root belongs to its producer Project");
    program.graph().nodes()[input.index()]
        .output_layout()
        .slots()[binding.ordinal as usize]
}
