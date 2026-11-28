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

use super::*;
use crate::{ExprKind, Fragment, NodeKind, PhysicalNode, RootScope, UnitLimits};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::collections::BTreeSet;

pub(super) fn validate_node_unit_in(
    fragment: &Fragment,
    node: &PhysicalNode,
    errors: &mut ValidationContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), CompileControlError> {
    let path = format!(
        "fragments[{}].nodes[{}].expressions",
        fragment.id().get(),
        node.id.get()
    );
    let mut referenced = BTreeSet::new();
    let visited = crate::node_roots::visit_node_roots(fragment, node, work, |required, work| {
        work.step()?;
        if !referenced.insert(required.root) {
            errors.push(ValidationError::new(
                &path,
                "two node fields share one root ordinal; preserve each ordered occurrence",
            ));
        }
        let Some(unit) = &node.expressions else {
            errors.push(ValidationError::new(
                &path,
                "node expression field requires an installed unit",
            ));
            return Ok(());
        };
        let Some(root) = unit.root(required.root) else {
            errors.push(ValidationError::new(
                &path,
                "node references an absent unit root",
            ));
            return Ok(());
        };
        if root.demand != required.demand || root.scope != required.scope {
            errors.push(ValidationError::new(
                &path,
                "unit root demand or scope differs from its node field",
            ));
        }
        if required.constant_only
            && !unit
                .definition(root.definition)
                .is_some_and(|d| matches!(d.kind, ExprKind::Constant(_)))
        {
            errors.push(ValidationError::new(
                &path,
                "Unpivot scalar root must name a checked constant definition",
            ));
        }
        Ok(())
    });
    match visited {
        Err(crate::node_roots::NodeRootError::Control(error)) => return Err(error),
        Err(crate::node_roots::NodeRootError::InvalidScanDerived(value)) => {
            errors.push(ValidationError::new(
                &path,
                format!(
                    "scan derived value {} has no root owned by this scan",
                    value.get()
                ),
            ))
        }
        Ok(()) => {}
    }
    // A provider guarantee is a metadata reference, not an additional runtime
    // field occurrence. Exact guarantees need not be residuals; a guarantee
    // may also name the same root an existing residual evaluates. Cover and
    // check that root without manufacturing another evaluation obligation.
    if let NodeKind::Scan { relation, .. } = &node.kind {
        for guarantee in relation.predicate_guarantees() {
            work.step()?;
            referenced.insert(guarantee.predicate);
            let root = node
                .expressions
                .as_ref()
                .and_then(|unit| unit.root(guarantee.predicate));
            if !root.is_some_and(|root| {
                root.demand == novarocks_type_contract::EvaluationDemand::TruthOnly
                    && root.scope == RootScope::Port(0)
            }) {
                errors.push(ValidationError::new(
                    &path,
                    "scan guarantee requires a TruthOnly root over the provider port",
                ));
            }
        }
    }
    let Some(unit) = &node.expressions else {
        return Ok(());
    };
    if referenced.len() != unit.roots().len() {
        errors.push(ValidationError::new(
            &path,
            "unit roots do not exactly cover this node's expression fields",
        ));
    }
    let expected_ports = if matches!(node.kind, NodeKind::Scan { .. }) {
        1
    } else {
        node.inputs.len()
    };
    if unit.ports().len() != expected_ports {
        errors.push(ValidationError::new(
            &path,
            "unit ports differ from the node's direct input count",
        ));
    }
    for (ordinal, port) in unit.ports().iter().enumerate().take(expected_ports) {
        let mut seen = BTreeSet::new();
        let mut position = 0usize;
        let mut check = |id: crate::ValueId| -> Result<(), CompileControlError> {
            work.step()?;
            if !seen.insert(id) {
                return Ok(());
            }
            let matches = match port.values.get(position) {
                Some((actual, ty)) if *actual == id => match fragment.values().get(&id) {
                    Some(value) => value
                        .ty
                        .exactly_equals_checked_observed(ty, || work.step())?,
                    None => false,
                },
                _ => false,
            };
            if !matches {
                errors.push(ValidationError::new(
                    &path,
                    "unit input declaration differs from the exact ordered input value/type",
                ));
            }
            position += 1;
            Ok(())
        };
        match &node.kind {
            NodeKind::Scan {
                provider_outputs, ..
            } => {
                for (_, id) in provider_outputs {
                    check(*id)?;
                }
            }
            _ => {
                if let Some(input) = node
                    .inputs
                    .get(ordinal)
                    .and_then(|id| fragment.nodes().get(id))
                {
                    for id in input.data_output_columns() {
                        check(*id)?;
                    }
                }
            }
        }
        if position != port.values.len() {
            errors.push(ValidationError::new(
                &path,
                "unit port contains values absent from its exact node input",
            ));
        }
    }
    let limits = UnitLimits {
        expression_semantic_depth: errors.limits().expression_semantic_depth,
    };
    if let Err(error) = unit.validate_structure_in(limits, work) {
        match error {
            novarocks_expression_contract::UnitError::Control(cause) => return Err(cause),
            error @ novarocks_expression_contract::UnitError::Resource(_) => {
                errors.push(ValidationError::resource_limit(&path, error.to_string()))
            }
            error => errors.push(ValidationError::new(&path, error.to_string())),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "node_units/empty_rows_tests.rs"]
mod empty_rows_tests;

#[cfg(test)]
#[path = "node_units/port_type_owned_tests.rs"]
mod port_type_owned_tests;
