// SPDX-License-Identifier: Apache-2.0

use super::*;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_mv_application::engine_properties::{
    MvEnginePropertyError, MvEnginePropertyErrorKind, engine_property_declarations,
    prepare_create_engine_properties,
};
use novarocks_parser::ast::{StatementProperty, TrinoCreateMaterializedView};
use novarocks_query_application::{
    session_error::{QueryServiceError, QueryServiceErrorKind},
    sql::{
        property_admit::DdlPropertyScope,
        property_evaluation::{
            DdlPropertyAssignment, DdlPropertyAssignmentValue, DdlPropertyConstantPort,
            DdlPropertyEvaluationMode, DdlPropertyNamePolicy, EvaluatedDdlProperty,
            evaluate_ddl_properties_with_name_policy,
        },
        trino_admission::unsupported_statement,
    },
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationSession, ConnectorControlPlanningPin, ConnectorError,
    ConnectorProviderBindingKey,
};
use novarocks_type_contract::PureCompileControl;

use crate::mv::domain::refresh::target::{IcebergMvTarget, resolve_refresh_target};

pub(crate) struct TrinoMvPropertyPreparation<'a> {
    pub(crate) source: &'a str,
    pub(crate) current_catalog: Option<&'a str>,
    pub(crate) current_database: &'a str,
    pub(crate) planning: &'a ConnectorControlPlanningPin,
    pub(crate) port: &'a mut dyn DdlPropertyConstantPort,
    pub(crate) control: &'a dyn PureCompileControl,
}

impl TrinoMvPropertyPreparation<'_> {
    pub(crate) fn create(
        &mut self,
        statement: TrinoCreateMaterializedView,
    ) -> Result<MvCreateStatement, QueryServiceError> {
        let (_, target, binding) = self.target(&statement.name)?;
        let (engine, storage) =
            self.evaluate(&statement.properties, DdlPropertyEvaluationMode::Create)?;
        let prepared = prepare_create_engine_properties(&engine)
            .map_err(|error| self.engine_error(error, &statement.properties, statement.span))?;
        let properties = storage
            .into_iter()
            .map(|property| match property.change {
                DdlPropertyChange::Set(value) => Ok((Arc::from(property.name), value)),
                DdlPropertyChange::Reset(_) => Err(invariant(
                    "CREATE materialized view storage received a reset",
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let range = statement.query.span.start()..statement.query.span.end();
        let select_sql = self
            .source
            .get(range)
            .ok_or_else(|| {
                invariant("Materialized view definition span does not belong to its SQL source")
            })?
            .to_owned();
        Ok(MvCreateStatement {
            if_not_exists: statement.not_exists,
            refresh: prepared.refresh,
            primary_key: prepared.primary_key,
            select_sql,
            select_query: statement.query,
            storage: MvCreateStorage::Evaluated {
                target,
                binding,
                properties: properties.into(),
            },
        })
    }

    pub(crate) fn alter(
        &mut self,
        name: TypedObjectName,
        properties: novarocks_parser::ast::AstVec<StatementProperty>,
        span: novarocks_parser::Span,
    ) -> Result<(novarocks_sql::planning::mv::SqlMvTarget, MvAlterStatement), QueryServiceError>
    {
        let (name_parts, target, owner) = self.target(&name)?;
        let (engine, storage) = self.evaluate(&properties, DdlPropertyEvaluationMode::Alter)?;
        let action = if engine.is_empty() {
            MvAlterAction::StorageProperties {
                owner,
                properties: frozen_storage_changes(storage),
            }
        } else {
            let changes = MvRefreshPropertyChanges::from_alter_properties(&engine)
                .map_err(|error| self.engine_error(error, &properties, span))?;
            if storage.is_empty() {
                MvAlterAction::RefreshProperties(changes)
            } else {
                MvAlterAction::RefreshAndStorageProperties {
                    changes,
                    owner,
                    properties: frozen_storage_changes(storage),
                }
            }
        };
        let target = novarocks_sql::planning::mv::SqlMvTarget {
            catalog: Some(target.catalog),
            database: target.namespace,
            name: target.table,
        };
        Ok((target, MvAlterStatement { name_parts, action }))
    }

    fn target(
        &self,
        name: &TypedObjectName,
    ) -> Result<(Vec<String>, IcebergMvTarget, ConnectorProviderBindingKey), QueryServiceError>
    {
        let (parts, target) =
            resolve_trino_target(name, self.current_catalog, self.current_database)?;
        let binding = self.planning.binding();
        if binding.descriptor().instance_id.as_str() != target.catalog {
            return Err(invariant(
                "Materialized view property scope names a different catalog",
            ));
        }
        if binding.descriptor().provider_id.as_str() != "iceberg" {
            return Err(unsupported_statement(
                self.source,
                name.span,
                "Materialized view storage in this catalog",
            ));
        }
        let owner = ConnectorProviderBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        };
        Ok((parts, target, owner))
    }

    fn evaluate(
        &mut self,
        properties: &[StatementProperty],
        mode: DdlPropertyEvaluationMode,
    ) -> Result<(Vec<EvaluatedDdlProperty>, Vec<EvaluatedDdlProperty>), QueryServiceError> {
        let mutation = self
            .planning
            .derive_mutation_lease()
            .map_err(provider_error)?;
        let engine =
            engine_property_declarations().map_err(|error| invariant(error.to_string()))?;
        let engine_count = engine.len();
        let mut declarations = engine;
        declarations.extend(
            mutation
                .table_property_declarations()
                .map_err(provider_error)?,
        );
        let assignments = properties
            .iter()
            .map(|property| DdlPropertyAssignment {
                name: &property.name.value,
                name_span: property.name.span,
                span: property.span,
                value: match &property.value {
                    Some(value) => DdlPropertyAssignmentValue::Expression(value),
                    None => DdlPropertyAssignmentValue::Default {
                        span: property.span,
                    },
                },
            })
            .collect::<Vec<_>>();
        let evaluated = evaluate_ddl_properties_with_name_policy(
            DdlPropertyScope::MaterializedView,
            mode,
            self.source,
            &declarations,
            &assignments,
            self.port,
            self.control,
            &mut StoragePropertyPolicy(&mutation),
        )?;
        Ok(evaluated
            .into_properties()
            .into_iter()
            .partition(|property| {
                declarations[..engine_count]
                    .iter()
                    .any(|declaration| declaration.name() == property.name)
            }))
    }

    fn engine_error(
        &self,
        error: MvEnginePropertyError,
        properties: &[StatementProperty],
        statement_span: novarocks_parser::Span,
    ) -> QueryServiceError {
        let span = properties
            .iter()
            .find(|property| property.name.value.eq_ignore_ascii_case(error.property()))
            .map_or(statement_span, |property| property.span);
        match error.kind() {
            MvEnginePropertyErrorKind::Unsupported => {
                unsupported_statement(self.source, span, "Changing primary_key")
            }
            MvEnginePropertyErrorKind::InvalidValue => QueryServiceError::from_user_error(
                DdlPropertyScope::MaterializedView.to_user_error(
                    self.source,
                    span,
                    error.to_string(),
                ),
            ),
            MvEnginePropertyErrorKind::Invariant => invariant(error.to_string()),
        }
    }
}

pub(super) fn resolve_trino_target(
    name: &TypedObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(Vec<String>, IcebergMvTarget), QueryServiceError> {
    let parts = lower_typed_name_parts(name)
        .map_err(|error| QueryServiceError::new(QueryServiceErrorKind::InvalidValue, error))?;
    let (catalog, local_parts) = match parts.as_slice() {
        [catalog, _, _] => (Some(catalog.as_str()), &parts[1..]),
        _ => (current_catalog, parts.as_slice()),
    };
    let target = resolve_refresh_target(catalog, current_database, local_parts)
        .map_err(|error| QueryServiceError::new(QueryServiceErrorKind::InvalidValue, error))?;
    Ok((parts, target))
}

fn frozen_storage_changes(
    properties: Vec<EvaluatedDdlProperty>,
) -> Arc<[(Arc<str>, DdlPropertyChange)]> {
    properties
        .into_iter()
        .map(|property| (Arc::from(property.name), property.change))
        .collect()
}

struct StoragePropertyPolicy<'a>(&'a ConnectorCatalogMutationSession);
impl DdlPropertyNamePolicy for StoragePropertyPolicy<'_> {
    fn check_unknown_property(&mut self, canonical: &str) -> Result<(), QueryServiceError> {
        self.0
            .check_table_property_name(canonical)
            .map_err(provider_error)
    }
}

fn invariant(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}

fn provider_error(error: ConnectorError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
}
