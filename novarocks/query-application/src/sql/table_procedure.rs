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

//! Table EXECUTE procedure declarations, name resolution and argument custody.

mod retention;

pub use retention::ProcedureRetentionDuration;

use novarocks_failure_contract::SharedFailureCause;
use novarocks_parser::{
    Span,
    ast::{CallArgument, Expr, Ident},
};
use novarocks_spi::connector::{ConnectorProviderId, DdlPropertyType, DdlPropertyValue};
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

use super::property_evaluation::{DdlPropertyConstantPort, evaluate_required_property_value};
use crate::session_error::{QueryServiceError, QueryServiceErrorKind};

const NOT_FOUND: ErrorCodeDescriptor = admit("sql.admit.table_procedure_not_found");
const INVALID_ARGUMENT: ErrorCodeDescriptor = admit("sql.admit.invalid_table_procedure_argument");
const INVALID_ARGUMENTS: ErrorCodeDescriptor = admit("sql.admit.invalid_table_procedure_arguments");
const DUPLICATE_ARGUMENT: ErrorCodeDescriptor =
    admit("sql.admit.duplicate_table_procedure_argument");

const fn admit(code: &'static str) -> ErrorCodeDescriptor {
    ErrorCodeDescriptor {
        code: ErrorCodeId::new(code),
        phase: ErrorPhase::Admit,
        status: ErrorCodeStatus::Active,
    }
}

pub const TABLE_PROCEDURE_ERROR_CODE_DESCRIPTORS: &[ErrorCodeDescriptor] = &[
    NOT_FOUND,
    INVALID_ARGUMENT,
    INVALID_ARGUMENTS,
    DUPLICATE_ARGUMENT,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableProcedureKind {
    Optimize,
    OptimizeManifests,
    ExpireSnapshots,
    RemoveOrphanFiles,
    AddFiles,
    UnsetExtraProperties,
    MoveColumn,
    SetColumnNotNull,
    CreateTag,
    DropTag,
    #[cfg(novarocks_dev_controls)]
    AddEqualityDelete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcedureArgumentType {
    Value(DdlPropertyType),
    EqualityRows,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcedureArgumentDefault {
    Boolean(bool),
    Varchar(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableProcedureArgument {
    pub name: &'static str,
    pub value_type: ProcedureArgumentType,
    pub default: Option<ProcedureArgumentDefault>,
    pub supports_explicit_value: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableProcedure {
    kind: TableProcedureKind,
    name: &'static str,
    arguments: &'static [TableProcedureArgument],
    dev_only: bool,
}

impl TableProcedure {
    pub fn kind(&self) -> TableProcedureKind {
        self.kind
    }
    pub fn name(&self) -> &'static str {
        self.name
    }
    pub fn arguments(&self) -> &'static [TableProcedureArgument] {
        self.arguments
    }
    pub fn dev_only(&self) -> bool {
        self.dev_only
    }
}

use DdlPropertyType as Type;
use ProcedureArgumentDefault as DefaultValue;
use ProcedureArgumentType::Value;

const fn argument(name: &'static str, value_type: ProcedureArgumentType) -> TableProcedureArgument {
    TableProcedureArgument {
        name,
        value_type,
        default: None,
        supports_explicit_value: true,
    }
}

const fn with_default(
    name: &'static str,
    value_type: ProcedureArgumentType,
    default: DefaultValue,
) -> TableProcedureArgument {
    TableProcedureArgument {
        default: Some(default),
        ..argument(name, value_type)
    }
}

const fn unsupported(declaration: TableProcedureArgument) -> TableProcedureArgument {
    TableProcedureArgument {
        supports_explicit_value: false,
        ..declaration
    }
}

const fn procedure(
    kind: TableProcedureKind,
    name: &'static str,
    arguments: &'static [TableProcedureArgument],
) -> TableProcedure {
    TableProcedure {
        kind,
        name,
        arguments,
        dev_only: false,
    }
}

const RETENTION: TableProcedureArgument = with_default(
    "retention_threshold",
    Value(Type::Varchar),
    DefaultValue::Varchar("7d"),
);
const ICEBERG: &[TableProcedure] = &[
    procedure(
        TableProcedureKind::Optimize,
        "OPTIMIZE",
        &[
            unsupported(with_default(
                "file_size_threshold",
                Value(Type::Varchar),
                DefaultValue::Varchar("100MB"),
            )),
            unsupported(argument("sorted_by", Value(Type::ArrayVarchar))),
        ],
    ),
    procedure(
        TableProcedureKind::OptimizeManifests,
        "OPTIMIZE_MANIFESTS",
        &[],
    ),
    procedure(
        TableProcedureKind::ExpireSnapshots,
        "EXPIRE_SNAPSHOTS",
        &[
            RETENTION,
            argument("retain_last", Value(Type::Integer)),
            with_default(
                "clean_expired_metadata",
                Value(Type::Boolean),
                DefaultValue::Boolean(false),
            ),
        ],
    ),
    procedure(
        TableProcedureKind::RemoveOrphanFiles,
        "REMOVE_ORPHAN_FILES",
        &[RETENTION],
    ),
    procedure(
        TableProcedureKind::AddFiles,
        "ADD_FILES",
        &[
            argument("location", Value(Type::Varchar)),
            argument("format", Value(Type::Varchar)),
            unsupported(with_default(
                "recursive_directory",
                Value(Type::Varchar),
                DefaultValue::Varchar("FAIL"),
            )),
        ],
    ),
    procedure(
        TableProcedureKind::UnsetExtraProperties,
        "UNSET_EXTRA_PROPERTIES",
        &[
            argument("keys", Value(Type::ArrayVarchar)),
            with_default(
                "if_exists",
                Value(Type::Boolean),
                DefaultValue::Boolean(false),
            ),
        ],
    ),
    procedure(
        TableProcedureKind::MoveColumn,
        "MOVE_COLUMN",
        &[
            argument("column", Value(Type::Varchar)),
            argument("position", Value(Type::Varchar)),
            argument("reference", Value(Type::Varchar)),
        ],
    ),
    procedure(
        TableProcedureKind::SetColumnNotNull,
        "SET_COLUMN_NOT_NULL",
        &[argument("column", Value(Type::Varchar))],
    ),
    procedure(
        TableProcedureKind::CreateTag,
        "CREATE_TAG",
        &[
            argument("name", Value(Type::Varchar)),
            argument("snapshot_id", Value(Type::Bigint)),
            with_default(
                "replace",
                Value(Type::Boolean),
                DefaultValue::Boolean(false),
            ),
            with_default(
                "if_not_exists",
                Value(Type::Boolean),
                DefaultValue::Boolean(false),
            ),
        ],
    ),
    procedure(
        TableProcedureKind::DropTag,
        "DROP_TAG",
        &[
            argument("name", Value(Type::Varchar)),
            with_default(
                "if_exists",
                Value(Type::Boolean),
                DefaultValue::Boolean(false),
            ),
        ],
    ),
    #[cfg(novarocks_dev_controls)]
    TableProcedure {
        dev_only: true,
        ..procedure(
            TableProcedureKind::AddEqualityDelete,
            "ADD_EQUALITY_DELETE",
            &[
                argument("columns", Value(Type::ArrayVarchar)),
                argument("rows", ProcedureArgumentType::EqualityRows),
            ],
        )
    },
];

/// Resolve against the already selected catalog provider. Catalog/table
/// resolution, privileges and Provider execution are the caller's owners.
pub fn resolve_table_procedure(
    provider: &ConnectorProviderId,
    name: &Ident,
) -> Result<&'static TableProcedure, QueryServiceError> {
    resolve_table_procedure_name(provider, &name.value, name.quoted)
}

/// Classify a declared statement shape before catalog admission. The provider
/// still resolves the procedure against the retained catalog generation.
pub fn declared_table_procedure_kind(name: &Ident) -> Option<TableProcedureKind> {
    lookup_iceberg_procedure(&canonical(&name.value, name.quoted)).map(TableProcedure::kind)
}

fn lookup_iceberg_procedure(canonical: &str) -> Option<&'static TableProcedure> {
    ICEBERG.iter().find(|procedure| procedure.name == canonical)
}

fn resolve_table_procedure_name(
    provider: &ConnectorProviderId,
    name: &str,
    quoted: bool,
) -> Result<&'static TableProcedure, QueryServiceError> {
    let canonical = canonical(name, quoted);
    if provider.as_str() == "iceberg"
        && let Some(procedure) = lookup_iceberg_procedure(&canonical)
    {
        return Ok(procedure);
    }
    Err(error(
        NOT_FOUND,
        None,
        format!("Table procedure not registered: {canonical}"),
        None,
    ))
}

#[derive(Clone, Copy, Debug)]
pub struct TableProcedureInputArgument<'a> {
    pub name: Option<ProcedureArgumentName<'a>>,
    pub input: ProcedureInput<'a>,
    pub span: Span,
}

impl<'a> From<&'a CallArgument> for TableProcedureInputArgument<'a> {
    fn from(argument: &'a CallArgument) -> Self {
        Self {
            name: argument.name.as_ref().map(|name| ProcedureArgumentName {
                value: &name.value,
                quoted: name.quoted,
            }),
            input: ProcedureInput::Expression(&argument.value),
            span: argument.span,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProcedureArgumentName<'a> {
    pub value: &'a str,
    pub quoted: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum ProcedureInput<'a> {
    Expression(&'a Expr),
    /// A parsed default clause only, never inferred from identifier text.
    ExplicitDefault,
}

#[derive(Debug, PartialEq)]
pub enum BoundProcedureValue<'a> {
    Value(DdlPropertyValue),
    RetentionDuration(ProcedureRetentionDuration),
    EqualityRows(&'a Expr),
}

#[derive(Debug, PartialEq)]
pub enum BoundProcedureArgument<'a> {
    Omitted,
    ExplicitDefault(Span),
    Present {
        value: BoundProcedureValue<'a>,
        span: Option<Span>,
    },
}

#[derive(Debug)]
pub struct BoundTableProcedure<'a> {
    procedure: &'static TableProcedure,
    arguments: Vec<BoundProcedureArgument<'a>>,
}

impl<'a> BoundTableProcedure<'a> {
    pub fn declaration(&self) -> &'static TableProcedure {
        self.procedure
    }

    /// Positional declaration order. An endpoint chooses when its declared
    /// default applies, based on actual presence of related arguments.
    pub fn arguments(
        &self,
    ) -> impl ExactSizeIterator<Item = (&TableProcedureArgument, &BoundProcedureArgument<'a>)> {
        self.procedure.arguments.iter().zip(&self.arguments)
    }
    pub fn into_arguments(
        self,
    ) -> impl ExactSizeIterator<Item = (&'static TableProcedureArgument, BoundProcedureArgument<'a>)>
    {
        self.procedure.arguments.iter().zip(self.arguments)
    }
}

pub fn bind_arguments<'a>(
    procedure: &'static TableProcedure,
    arguments: &[TableProcedureInputArgument<'a>],
    source: &str,
    statement_span: Span,
    constants: &mut dyn DdlPropertyConstantPort,
    catalog: &str,
    control: &dyn PureCompileControl,
) -> Result<BoundTableProcedure<'a>, QueryServiceError> {
    let arguments = arguments
        .iter()
        .map(|argument| BindingInput {
            name: argument.name,
            input: argument.input,
            span: Some(argument.span),
        })
        .collect::<Vec<_>>();
    bind_slots(
        procedure,
        &arguments,
        Some(source),
        Some((source, statement_span)),
        catalog,
        control,
        |slot, argument| {
            let span = argument.span.expect("original SQL argument span");
            match argument.input {
                ProcedureInput::ExplicitDefault => {
                    Ok(BoundProcedureArgument::ExplicitDefault(span))
                }
                input => Ok(BoundProcedureArgument::Present {
                    value: bind_value(slot, input, source, span, constants, catalog, control)?,
                    span: Some(span),
                }),
            }
        },
    )
}

#[derive(Clone, Copy)]
struct BindingInput<'a, A> {
    name: Option<ProcedureArgumentName<'a>>,
    input: A,
    span: Option<Span>,
}

fn bind_slots<'a, A: Copy>(
    procedure: &'static TableProcedure,
    arguments: &[BindingInput<'_, A>],
    source: Option<&str>,
    statement_location: Option<(&str, Span)>,
    catalog: &str,
    control: &dyn PureCompileControl,
    mut evaluate: impl FnMut(
        &TableProcedureArgument,
        BindingInput<'_, A>,
    ) -> Result<BoundProcedureArgument<'a>, QueryServiceError>,
) -> Result<BoundTableProcedure<'a>, QueryServiceError> {
    checkpoint(control, 0)?;
    let named_count = arguments
        .iter()
        .filter(|argument| argument.name.is_some())
        .count();
    if named_count != 0 && named_count != arguments.len() {
        return Err(error(
            INVALID_ARGUMENTS,
            statement_location,
            "Named and positional arguments cannot be mixed",
            None,
        ));
    }
    if named_count == 0 && arguments.len() > procedure.arguments.len() {
        return Err(error(
            INVALID_ARGUMENTS,
            statement_location,
            "Too many positional arguments",
            None,
        ));
    }
    let mut names = std::collections::HashSet::new();
    for argument in arguments {
        checkpoint(control, 1)?;
        if let Some(name) = argument.name
            && !names.insert(canonical(name.value, name.quoted))
        {
            return Err(error(
                DUPLICATE_ARGUMENT,
                source.zip(argument.span),
                format!("Duplicate named argument: {}", name.value),
                None,
            ));
        }
    }
    let mut bound = procedure
        .arguments
        .iter()
        .map(|_| BoundProcedureArgument::Omitted)
        .collect::<Vec<_>>();
    for (position, argument) in arguments.iter().enumerate() {
        checkpoint(control, 1)?;
        let index = if let Some(name) = argument.name {
            let folded = name.value.to_lowercase();
            procedure
                .arguments
                .iter()
                .position(|slot| slot.name == folded)
                .ok_or_else(|| {
                    error(
                        INVALID_ARGUMENT,
                        source.zip(argument.span),
                        match procedure.kind {
                            TableProcedureKind::OptimizeManifests
                            | TableProcedureKind::ExpireSnapshots
                            | TableProcedureKind::RemoveOrphanFiles => format!(
                                "Catalog '{catalog}' table procedure '{}' property '{folded}' does not exist",
                                procedure.name
                            ),
                            _ => format!(
                                "Table procedure {} argument '{}' does not exist",
                                procedure.name, folded
                            ),
                        },
                        None,
                    )
                })?
        } else {
            position
        };
        let slot = &procedure.arguments[index];
        if !slot.supports_explicit_value {
            return Err(not_supported(format!(
                "Table procedure {} argument '{}' is not supported",
                procedure.name, slot.name
            )));
        }
        let value = evaluate(slot, *argument)?;
        // The reference checks duplicates on canonical Ident values above,
        // then the property author uses a lowercase key for value assignment.
        bound[index] = value;
    }
    checkpoint(control, 0)?;
    Ok(BoundTableProcedure {
        procedure,
        arguments: bound,
    })
}

fn bind_value<'a>(
    slot: &TableProcedureArgument,
    input: ProcedureInput<'a>,
    source: &str,
    span: Span,
    constants: &mut dyn DdlPropertyConstantPort,
    catalog: &str,
    control: &dyn PureCompileControl,
) -> Result<BoundProcedureValue<'a>, QueryServiceError> {
    if let (ProcedureArgumentType::EqualityRows, ProcedureInput::Expression(expression)) =
        (slot.value_type, input)
    {
        return Ok(BoundProcedureValue::EqualityRows(expression));
    }
    let location = Some((source, span));
    let evaluate = |expression, ty, constants: &mut dyn DdlPropertyConstantPort| {
        evaluate_required_property_value(expression, ty, constants, control, |message, cause| {
            error(INVALID_ARGUMENT, location, message, cause)
        })
    };
    let value = match (slot.value_type, input) {
        (Value(ty), ProcedureInput::Expression(expression)) => evaluate(expression, ty, constants)?,
        _ => {
            return Err(invariant(
                "Table procedure input differs from its original argument author",
            ));
        }
    };
    validate_supplied_value(slot, &value, location, catalog)?;
    if let DdlPropertyValue::Varchar(text) = &value
        && slot.name == RETENTION.name
    {
        return ProcedureRetentionDuration::parse(source, span, text, control)
            .map(BoundProcedureValue::RetentionDuration);
    }
    Ok(BoundProcedureValue::Value(value))
}

fn validate_supplied_value(
    slot: &TableProcedureArgument,
    value: &DdlPropertyValue,
    location: Option<(&str, Span)>,
    catalog: &str,
) -> Result<(), QueryServiceError> {
    if matches!(value, DdlPropertyValue::Null) {
        return Err(error(
            INVALID_ARGUMENT,
            location,
            format!("Table procedure argument '{}' cannot be NULL", slot.name),
            None,
        ));
    }
    if !matches_type(slot.value_type, value) {
        return Err(invariant(
            "Table procedure argument author produced a different declared type",
        ));
    }
    if slot.name == "retain_last"
        && let DdlPropertyValue::Integer(count) = value
        && *count < 1
    {
        return Err(error(
            INVALID_ARGUMENT,
            location,
            format!(
                "Unable to set catalog '{catalog}' table procedure 'EXPIRE_SNAPSHOTS' property 'retain_last' to [{count}]: Number of snapshots to retain must be at least 1"
            ),
            None,
        ));
    }
    if slot.name == "format"
        && matches!(value, DdlPropertyValue::Varchar(format) if !format.eq_ignore_ascii_case("PARQUET"))
    {
        return Err(not_supported(
            "Table procedure ADD_FILES only supports PARQUET",
        ));
    }
    Ok(())
}

fn matches_type(expected: ProcedureArgumentType, value: &DdlPropertyValue) -> bool {
    match expected {
        Value(ty) => value.value_type() == Some(ty),
        ProcedureArgumentType::EqualityRows => false,
    }
}

fn canonical(value: &str, quoted: bool) -> String {
    if quoted {
        value.to_owned()
    } else {
        value.to_uppercase()
    }
}

fn checkpoint(control: &dyn PureCompileControl, units: u32) -> Result<(), QueryServiceError> {
    control
        .checkpoint(CompilePhase::Validate, units)
        .map_err(QueryServiceError::from_compile_control)
}

/// Reject an argument value at its actual SQL location after endpoint validation.
pub fn invalid_procedure_argument(
    source: &str,
    span: Span,
    message: impl Into<String>,
) -> QueryServiceError {
    error(INVALID_ARGUMENT, Some((source, span)), message, None)
}

/// Project an argument rejected by the selected provider after binding.
pub fn invalid_procedure_request(
    cause: SharedFailureCause,
    message: impl Into<String>,
) -> QueryServiceError {
    error(INVALID_ARGUMENT, None, message, Some(cause))
}

fn error(
    descriptor: ErrorCodeDescriptor,
    location: Option<(&str, Span)>,
    message: impl Into<String>,
    cause: Option<SharedFailureCause>,
) -> QueryServiceError {
    let user = UserError::from_descriptor(
        descriptor,
        message,
        location.map(|(source, span)| span.to_user_error_location(source)),
        RetryClass::Never,
    );
    match cause {
        Some(cause) => QueryServiceError::from_user_error_with_cause(user, cause),
        None => QueryServiceError::from_user_error(user),
    }
}

fn invariant(message: &str) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}

fn not_supported(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use super::super::property_evaluation::DdlPropertyConstantError;
    use super::*;
    use novarocks_constant_contract::ConstantValue;
    use novarocks_parser::parser::parse_expression;
    use novarocks_sql::compiler::{RequiredConstantTarget, SqlCompileControl};

    struct UnreachedEvaluation;
    impl DdlPropertyConstantPort for UnreachedEvaluation {
        fn evaluate(
            &mut self,
            _: &Expr,
            _: RequiredConstantTarget,
        ) -> Result<ConstantValue, DdlPropertyConstantError> {
            panic!("argument shape must be refused before evaluating its value")
        }
    }
    #[test]
    fn table_procedure_argument_shape_preserves_pin_error_identities() {
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        for (sql, expected) in [
            (
                "expire_snapshots(1, retain_last => 2)",
                "sql.admit.invalid_table_procedure_arguments",
            ),
            (
                "optimize_manifests(1)",
                "sql.admit.invalid_table_procedure_arguments",
            ),
            (
                "expire_snapshots(retain_last => 1, RETAIN_LAST => 2)",
                "sql.admit.duplicate_table_procedure_argument",
            ),
            (
                "expire_snapshots(unknown => 1)",
                "sql.admit.invalid_table_procedure_argument",
            ),
        ] {
            // These are real shared CallArgument nodes from the Trino parser.
            // The statement-level TableExecute producer is a separate consumer.
            let Expr::FunctionCall(call) = parse_expression(sql).unwrap() else {
                panic!("procedure argument syntax")
            };
            let procedure =
                resolve_table_procedure(&provider, call.name.parts.last().unwrap()).unwrap();
            let args = call
                .arguments
                .iter()
                .map(TableProcedureInputArgument::from)
                .collect::<Vec<_>>();
            let error = bind_arguments(
                procedure,
                &args,
                sql,
                call.span,
                &mut UnreachedEvaluation,
                "iceberg",
                &SqlCompileControl::unbounded(),
            )
            .unwrap_err();
            let user = error.user_error().unwrap();
            assert_eq!(user.code().as_str(), expected, "{sql}");
            assert!(user.location().is_some());
        }
        let Expr::Identifier(name) = parse_expression("missing_procedure").unwrap() else {
            panic!("real procedure identifier");
        };
        let error = resolve_table_procedure(&provider, &name).unwrap_err();
        let user = error.user_error().unwrap();
        assert_eq!(user.code().as_str(), "sql.admit.table_procedure_not_found");
        assert!(user.location().is_none());
    }

    #[test]
    fn equality_delete_resolution_matches_the_build_profile() {
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let Expr::Identifier(name) = parse_expression("add_equality_delete").unwrap() else {
            panic!("real procedure name");
        };
        let result = resolve_table_procedure(&provider, &name);
        #[cfg(novarocks_dev_controls)]
        assert!(result.unwrap().dev_only());
        #[cfg(not(novarocks_dev_controls))]
        assert_eq!(
            result.unwrap_err().user_error().unwrap().code().as_str(),
            "sql.admit.table_procedure_not_found"
        );
    }
}
