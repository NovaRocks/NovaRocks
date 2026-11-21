//! Validated carrier for the typed connector scan source.
//!
//! The scan source carries the relation, its ordered output assignments, and
//! its predicates. It never carries a split list, a provider payload, or an
//! Arrow IPC schema: splits arrive at runtime through the task-update path.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::{ConnectorExpression, ConnectorValueType, TupleDomain};
use prost::Message;

use crate::{FieldPath, ProtocolError};

use super::handle::CatalogTableHandle;
use super::predicate::{ValidatedColumnHandle, decode_connector_expression, decode_tuple_domain};
use super::value::decode_value_type;
use super::{
    MAX_NAME_BYTES, MAX_SCAN_ASSIGNMENTS, bounded_text, inconsistent, missing, nest, out_of_range,
};

/// One ordered output column of a scan.
#[derive(Clone, Debug)]
pub struct ScanAssignment {
    raw: dto::ScanAssignment,
    column: ValidatedColumnHandle,
    value_type: ConnectorValueType,
}

impl ScanAssignment {
    pub fn parse(raw: dto::ScanAssignment, path: FieldPath) -> Result<Self, ProtocolError> {
        bounded_text(
            &raw.variable,
            MAX_NAME_BYTES,
            path.clone().field("variable"),
            false,
        )?;
        let column = raw.column.clone().ok_or_else(|| {
            missing(
                path.clone().field("column"),
                "scan assignment requires a column handle",
            )
        })?;
        let column = ValidatedColumnHandle::parse(column, path.clone().field("column"))?;
        let value_type = raw.value_type.as_ref().ok_or_else(|| {
            missing(
                path.clone().field("value_type"),
                "scan assignment requires its exact type",
            )
        })?;
        let value_type = decode_value_type(value_type, path.field("value_type"))?;
        Ok(Self {
            raw,
            column,
            value_type,
        })
    }

    pub fn variable(&self) -> &str {
        &self.raw.variable
    }

    pub const fn column(&self) -> &ValidatedColumnHandle {
        &self.column
    }

    pub const fn value_type(&self) -> ConnectorValueType {
        self.value_type
    }

    pub const fn as_proto(&self) -> &dto::ScanAssignment {
        &self.raw
    }
}

/// One dynamic filter bound to a variable of this scan.
#[derive(Clone, Debug)]
pub struct DynamicFilterBinding {
    raw: dto::DynamicFilterBinding,
}

impl DynamicFilterBinding {
    pub fn parse(raw: dto::DynamicFilterBinding, path: FieldPath) -> Result<Self, ProtocolError> {
        bounded_text(&raw.variable, MAX_NAME_BYTES, path.field("variable"), false)?;
        Ok(Self { raw })
    }

    pub const fn filter_id(&self) -> u32 {
        self.raw.filter_id
    }

    pub fn variable(&self) -> &str {
        &self.raw.variable
    }

    pub const fn as_proto(&self) -> &dto::DynamicFilterBinding {
        &self.raw
    }
}

/// The typed replacement for the opaque connector read source.
#[derive(Clone, Debug)]
pub struct ConnectorTableScanSource {
    raw: dto::ConnectorTableScanSource,
    table: CatalogTableHandle,
    assignments: Vec<ScanAssignment>,
    enforced_predicate: TupleDomain<ValidatedColumnHandle>,
    unenforced_predicate: TupleDomain<ValidatedColumnHandle>,
    remaining_expression: Option<ConnectorExpression>,
    dynamic_filters: Vec<DynamicFilterBinding>,
}

impl ConnectorTableScanSource {
    pub fn parse(
        raw: dto::ConnectorTableScanSource,
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        let table = raw.table.clone().ok_or_else(|| {
            missing(
                path.clone().field("table"),
                "scan source requires a catalog table handle",
            )
        })?;
        let table = CatalogTableHandle::parse(table, path.clone().field("table"))?;

        if raw.assignments.is_empty() {
            // A zero-column scan is expressed by a scan whose assignments are
            // empty only when the engine asked for a count; that case still
            // carries at least the relation, so an empty assignment list here
            // would leave the output order undefined.
            return Err(missing(
                path.clone().field("assignments"),
                "scan source requires at least one ordered assignment",
            ));
        }
        if raw.assignments.len() > MAX_SCAN_ASSIGNMENTS {
            return Err(out_of_range(
                path.clone().field("assignments"),
                "scan assignment count exceeds the hard limit",
            ));
        }
        let mut assignments = Vec::with_capacity(raw.assignments.len());
        let mut variables = BTreeMap::new();
        for (index, assignment) in raw.assignments.iter().enumerate() {
            let assignment_path = path.clone().field("assignments").index(index);
            let assignment = ScanAssignment::parse(assignment.clone(), assignment_path.clone())?;
            if variables
                .insert(Arc::<str>::from(assignment.variable()), index)
                .is_some()
            {
                return Err(inconsistent(
                    assignment_path.field("variable"),
                    "scan assignment variables must be unique",
                ));
            }
            assignments.push(assignment);
        }

        // A predicate may only constrain a column this scan actually produces;
        // otherwise the backend would silently ignore it.
        let assigned_columns = assignments
            .iter()
            .map(|assignment| assignment.column().clone())
            .collect::<BTreeSet<_>>();

        let enforced = raw.enforced_predicate.as_ref().ok_or_else(|| {
            missing(
                path.clone().field("enforced_predicate"),
                "scan source requires an enforced predicate",
            )
        })?;
        let enforced_predicate =
            decode_tuple_domain(enforced, path.clone().field("enforced_predicate"))
                .map_err(|error| nest(path.clone().field("enforced_predicate"), error))?;
        let unenforced = raw.unenforced_predicate.as_ref().ok_or_else(|| {
            missing(
                path.clone().field("unenforced_predicate"),
                "scan source requires an unenforced predicate",
            )
        })?;
        let unenforced_predicate =
            decode_tuple_domain(unenforced, path.clone().field("unenforced_predicate"))
                .map_err(|error| nest(path.clone().field("unenforced_predicate"), error))?;

        for (predicate, field) in [
            (&enforced_predicate, "enforced_predicate"),
            (&unenforced_predicate, "unenforced_predicate"),
        ] {
            for column in predicate.columns() {
                if !assigned_columns.contains(column) {
                    return Err(inconsistent(
                        path.clone().field(field),
                        "predicate constrains a column this scan does not assign",
                    ));
                }
            }
        }

        let remaining_expression = match raw.remaining_expression.as_ref() {
            None => None,
            Some(expression) => Some(decode_connector_expression(
                expression,
                path.clone().field("remaining_expression"),
            )?),
        };
        if let Some(expression) = &remaining_expression {
            let mut names = Vec::new();
            expression.variable_names(&mut names);
            for name in names {
                if !variables.contains_key(&name) {
                    return Err(inconsistent(
                        path.clone().field("remaining_expression"),
                        "remaining expression references an unassigned variable",
                    ));
                }
            }
        }

        let mut dynamic_filters = Vec::with_capacity(raw.dynamic_filters.len());
        let mut filter_ids = BTreeSet::new();
        for (index, binding) in raw.dynamic_filters.iter().enumerate() {
            let binding_path = path.clone().field("dynamic_filters").index(index);
            let binding = DynamicFilterBinding::parse(binding.clone(), binding_path.clone())?;
            if !filter_ids.insert(binding.filter_id()) {
                return Err(inconsistent(
                    binding_path.clone().field("filter_id"),
                    "dynamic filter ids must be unique within one scan",
                ));
            }
            if !variables.contains_key(&Arc::<str>::from(binding.variable())) {
                return Err(inconsistent(
                    binding_path.field("variable"),
                    "dynamic filter names a variable this scan does not assign",
                ));
            }
            dynamic_filters.push(binding);
        }

        Ok(Self {
            raw,
            table,
            assignments,
            enforced_predicate,
            unenforced_predicate,
            remaining_expression,
            dynamic_filters,
        })
    }

    pub const fn table(&self) -> &CatalogTableHandle {
        &self.table
    }

    pub fn assignments(&self) -> &[ScanAssignment] {
        &self.assignments
    }

    pub const fn enforced_predicate(&self) -> &TupleDomain<ValidatedColumnHandle> {
        &self.enforced_predicate
    }

    pub const fn unenforced_predicate(&self) -> &TupleDomain<ValidatedColumnHandle> {
        &self.unenforced_predicate
    }

    pub const fn remaining_expression(&self) -> Option<&ConnectorExpression> {
        self.remaining_expression.as_ref()
    }

    pub fn dynamic_filters(&self) -> &[DynamicFilterBinding] {
        &self.dynamic_filters
    }

    pub const fn as_proto(&self) -> &dto::ConnectorTableScanSource {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorTableScanSource {
        self.raw
    }

    pub fn encoded_len(&self) -> usize {
        self.raw.encoded_len()
    }
}
