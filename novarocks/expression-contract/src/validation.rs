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

//! Unit-local structure, lexical scope, full types and root visibility.
use crate::validation_types::{self, ExpressionParentReference, TypeChecks};
use crate::{
    ExprId, ExprKind, ExprNode, ExprPort, ExprRoot, RootOrdinal, RootScope, UnitError, UnitLimits,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, EvaluationDemand, MAX_CONTROL_DEFINITIONS,
};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn reserve<T>(
    output: &mut Vec<T>,
    additional: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), UnitError> {
    work.flush()?;
    output
        .try_reserve_exact(additional)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;
    work.flush()?;
    Ok(())
}
fn filled<T>(
    count: usize,
    mut make: impl FnMut() -> T,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, UnitError> {
    let mut result = Vec::new();
    reserve(&mut result, count, work)?;
    for _ in 0..count {
        result.push(make());
        work.step()?;
    }
    Ok(result)
}
fn node(definitions: &[ExprNode], id: ExprId) -> Result<&ExprNode, UnitError> {
    definitions
        .get(id.get() as usize)
        .ok_or_else(|| UnitError::invalid(Some(id), None, "expression definition is absent"))
}

pub(crate) fn validate(
    ports: &[ExprPort],
    definitions: &[ExprNode],
    roots: &[ExprRoot],
    limits: UnitLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), UnitError> {
    if definitions.len() > MAX_CONTROL_DEFINITIONS {
        return Err(novarocks_type_contract::ExpressionControlFlowError::TooManyItems.into());
    }
    for (ordinal, expression) in definitions.iter().enumerate() {
        work.step()?;
        if expression.id.get() as usize != ordinal {
            return Err(UnitError::invalid(
                Some(expression.id),
                None,
                "definition IDs must be dense and match their ordinal",
            ));
        }
        if let Some(scope) = expression.lambda_scope
            && !matches!(node(definitions, scope)?.kind, ExprKind::Lambda { .. })
        {
            return Err(UnitError::invalid(
                Some(expression.id),
                None,
                "lexical scope is not a lambda definition",
            ));
        }
    }
    for port in ports {
        let mut seen = BTreeSet::new();
        for (id, _) in &port.values {
            work.flush()?;
            let fresh = seen.insert(*id);
            work.step()?;
            work.flush()?;
            if !fresh {
                return Err(UnitError::invalid(
                    None,
                    None,
                    "input port declares a duplicate value",
                ));
            }
        }
    }
    let mut parents: Vec<Vec<ExpressionParentReference>> =
        filled(definitions.len(), Vec::new, work)?;
    let mut children: Vec<Vec<ExprId>> = filled(definitions.len(), Vec::new, work)?;
    for expression in definitions {
        let mut ordered = Vec::new();
        expression
            .kind
            .expression_references_observed(|reference| {
                work.step()?;
                let child = node(definitions, reference)?;
                if !validation_types::expression_reference_scope_allowed(
                    definitions,
                    expression,
                    reference,
                    child,
                ) {
                    return Err(UnitError::invalid(
                        Some(expression.id),
                        None,
                        "dependency crosses a lambda lexical scope",
                    ));
                }
                reserve(&mut ordered, 1, work)?;
                ordered.push(reference);
                work.step()?;
                Ok::<_, UnitError>(())
            })?;
        let mut references = Vec::new();
        reserve(&mut references, ordered.len(), work)?;
        validation_types::expression_parent_references(expression, &mut references);
        for (reference, role) in references {
            let target = &mut parents[reference.get() as usize];
            reserve(target, 1, work)?;
            target.push(ExpressionParentReference {
                parent: expression.id,
                role,
            });
            work.step()?;
        }
        children[expression.id.get() as usize] = ordered;
    }
    // The lexical owner graph is distinct from the expression dependency DAG.
    let mut completed = BTreeSet::new();
    for expression in definitions {
        if completed.contains(&expression.id) {
            continue;
        }
        let mut local = BTreeSet::new();
        let mut current = Some(expression.id);
        while let Some(id) = current {
            work.step()?;
            if completed.contains(&id) {
                break;
            }
            work.flush()?;
            let fresh = local.insert(id);
            work.step()?;
            work.flush()?;
            if !fresh {
                return Err(UnitError::invalid(
                    Some(id),
                    None,
                    "lambda lexical-scope graph contains a cycle",
                ));
            }
            current = node(definitions, id)?.lambda_scope;
        }
        for id in local {
            work.flush()?;
            completed.insert(id);
            work.step()?;
            work.flush()?;
        }
    }
    let mut remaining = filled(definitions.len(), || 0usize, work)?;
    let mut depths = filled(definitions.len(), || 1usize, work)?;
    let mut dependents: Vec<Vec<ExprId>> = filled(definitions.len(), Vec::new, work)?;
    let mut ready = Vec::new();
    reserve(&mut ready, definitions.len(), work)?;
    for expression in definitions {
        let mut unique = BTreeSet::new();
        for &child in &children[expression.id.get() as usize] {
            work.flush()?;
            let fresh = unique.insert(child);
            work.step()?;
            work.flush()?;
            if fresh {
                remaining[expression.id.get() as usize] += 1;
                let users = &mut dependents[child.get() as usize];
                reserve(users, 1, work)?;
                users.push(expression.id);
                work.step()?;
            }
        }
        if unique.is_empty() {
            ready.push(expression.id);
        }
        work.step()?;
    }
    let mut processed = 0usize;
    while let Some(id) = ready.pop() {
        processed += 1;
        let depth = depths[id.get() as usize];
        work.step()?;
        if depth > limits.expression_semantic_depth {
            return Err(UnitError::Resource(
                "expression semantic depth exceeds the caller limit",
            ));
        }
        for &user in &dependents[id.get() as usize] {
            let ordinal = user.get() as usize;
            depths[ordinal] = depths[ordinal].max(depth.saturating_add(1));
            remaining[ordinal] -= 1;
            if remaining[ordinal] == 0 {
                ready.push(user);
            }
            work.step()?;
        }
    }
    if processed != definitions.len() {
        let id = remaining
            .iter()
            .position(|&n| n != 0)
            .map(|n| ExprId::new(n as u32));
        return Err(UnitError::invalid(
            id,
            None,
            "expression graph contains a cycle",
        ));
    }
    let mut root_definitions = BTreeSet::new();
    let mut globally_reachable = BTreeSet::new();
    for (ordinal, root) in roots.iter().enumerate() {
        let root_id = RootOrdinal::new(
            u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?,
        );
        let expression = node(definitions, root.definition)?;
        work.step()?;
        if expression.lambda_scope.is_some() {
            return Err(UnitError::invalid(
                Some(root.definition),
                Some(root_id),
                "root is inside a lambda scope",
            ));
        }
        if root.demand == EvaluationDemand::TruthOnly
            && *expression.ty.data_type() != arrow_schema::DataType::Boolean
        {
            return Err(UnitError::invalid(
                Some(root.definition),
                Some(root_id),
                "TruthOnly root is not Boolean",
            ));
        }
        let mut visible = BTreeMap::new();
        let selected: &[ExprPort] = match root.scope {
            RootScope::Port(index) => {
                std::slice::from_ref(ports.get(index as usize).ok_or_else(|| {
                    UnitError::invalid(
                        Some(root.definition),
                        Some(root_id),
                        "root input port is absent",
                    )
                })?)
            }
            RootScope::AllPorts => ports,
        };
        for port in selected {
            for (value, ty) in &port.values {
                work.flush()?;
                let previous = visible.insert(*value, ty);
                work.step()?;
                work.flush()?;
                if previous.is_some() {
                    return Err(UnitError::invalid(
                        Some(root.definition),
                        Some(root_id),
                        "root scope contains an ambiguous value",
                    ));
                }
            }
        }
        work.flush()?;
        root_definitions.insert(root.definition);
        work.step()?;
        work.flush()?;
        let mut visited = BTreeSet::new();
        let mut pending = Vec::new();
        reserve(&mut pending, 1, work)?;
        pending.push(root.definition);
        while let Some(id) = pending.pop() {
            work.flush()?;
            let fresh = visited.insert(id);
            work.step()?;
            work.flush()?;
            if !fresh {
                continue;
            }
            work.flush()?;
            globally_reachable.insert(id);
            work.step()?;
            work.flush()?;
            let expression = node(definitions, id)?;
            if let ExprKind::Value(value) = expression.kind {
                let ty = visible.get(&value).ok_or_else(|| {
                    UnitError::invalid(
                        Some(id),
                        Some(root_id),
                        "value is outside its root input scope",
                    )
                })?;
                if !expression
                    .ty
                    .exactly_equals_observed(ty, || work.step().map_err(UnitError::from))?
                {
                    return Err(UnitError::invalid(
                        Some(id),
                        Some(root_id),
                        "value type differs from its complete port declaration",
                    ));
                }
            }
            reserve(&mut pending, children[id.get() as usize].len(), work)?;
            for &child in &children[id.get() as usize] {
                pending.push(child);
                work.step()?;
            }
        }
    }
    if globally_reachable.len() != definitions.len() {
        let id = definitions
            .iter()
            .find(|n| !globally_reachable.contains(&n.id))
            .map(|n| n.id);
        return Err(UnitError::invalid(
            id,
            None,
            "definition is unreachable from all roots",
        ));
    }
    for expression in definitions {
        let mut checks = TypeChecks::default();
        validation_types::validate_expression_types(
            definitions,
            expression,
            &root_definitions,
            &parents[expression.id.get() as usize],
            &mut checks,
        );
        work.step()?;
        if let Some(issue) = checks.issue {
            return Err(UnitError::invalid(
                Some(expression.id),
                None,
                format!("definitions[{}]: {}", issue.definition.get(), issue.message),
            ));
        }
    }
    Ok(())
}
