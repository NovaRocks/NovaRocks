// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::sql::trino_table_command::bind_trino_table_execute;
use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::ConstantPool;
use novarocks_parser::{
    ast::{LiteralKind, TrinoStatement, TrinoTableExecute},
    parser::parse_statement,
};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;

struct StringConstant;

impl DdlPropertyConstantPort for StringConstant {
    fn evaluate(
        &mut self,
        expression: &Expr,
        target: RequiredConstantTarget,
    ) -> Result<ConstantValue, DdlPropertyConstantError> {
        assert_eq!(target, RequiredConstantTarget::Varchar);
        let Expr::Literal(literal) = expression else {
            panic!("expected the original string argument");
        };
        let LiteralKind::String(text) = &literal.kind else {
            panic!("expected a string literal");
        };
        let data = StringArray::from(vec![text.as_str()]).to_data();
        let pool = ConstantPool::try_new(
            Arc::new(Field::new("argument", DataType::Utf8, false)),
            FunctionValueType::new(DataType::Utf8, false),
            data,
            crate::completed_plan_fixture::test_constant_policy(),
            CompilePhase::Validate,
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
        Ok(pool.value(0).unwrap())
    }
}

fn optimize(source: &str) -> TrinoTableExecute {
    let TrinoStatement::TableExecute(statement) = parse_statement(source).unwrap() else {
        panic!("expected parsed OPTIMIZE arguments");
    };
    statement
}

fn bind<'a>(
    source: &str,
    statement: &'a TrinoTableExecute,
    constants: &mut dyn DdlPropertyConstantPort,
) -> Result<BoundTableProcedure<'a>, QueryServiceError> {
    bind_trino_table_execute(
        source,
        statement,
        &ConnectorProviderId::parse("iceberg").unwrap(),
        constants,
        "iceberg",
        &SqlCompileControl::unbounded(),
    )
}

#[test]
fn absent_optimize_threshold_preserves_its_declared_default() {
    let source = "ALTER TABLE t EXECUTE optimize";
    let statement = optimize(source);
    let bound = bind(source, &statement, &mut UnreachedEvaluation).unwrap();
    let (slot, bound) = bound.arguments().next().unwrap();
    assert_eq!(
        slot.default,
        Some(ProcedureArgumentDefault::DataSizeBytes(104_857_600))
    );
    assert_eq!(bound, &BoundProcedureArgument::Omitted);
}

#[test]
fn optimize_threshold_binds_the_checked_byte_count() {
    for (literal, expected) in [
        ("'10MB'", 10_485_760),
        ("'1.5MB'", 1_572_864),
        ("'1024B'", 1024),
        ("'0B'", 0),
    ] {
        let source = format!("ALTER TABLE t EXECUTE optimize(file_size_threshold => {literal})");
        let statement = optimize(&source);
        let bound = bind(&source, &statement, &mut StringConstant).unwrap();
        let (_, argument) = bound.arguments().next().unwrap();
        let BoundProcedureArgument::Present {
            value: BoundProcedureValue::FileSizeThreshold(value),
            span: Some(_),
        } = argument
        else {
            panic!("expected the checked threshold for {literal}");
        };
        assert_eq!(value.bytes(), expected, "{literal}");
    }
}

#[test]
fn invalid_optimize_threshold_identifies_the_original_argument() {
    let source = "ALTER TABLE t EXECUTE optimize(file_size_threshold => 'abc')";
    let statement = optimize(source);
    let error = bind(source, &statement, &mut StringConstant).unwrap_err();
    let user = error.user_error().unwrap();
    assert_eq!(
        user.code().as_str(),
        "sql.admit.invalid_table_procedure_argument"
    );
    assert_eq!(
        user.message(),
        "Unable to set catalog 'iceberg' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['abc']: size is not a valid data size string: abc",
    );
    assert!(user.location().is_some());
}

#[test]
fn sorted_by_explains_which_write_capability_is_required() {
    let source = "ALTER TABLE t EXECUTE optimize(sorted_by => ARRAY['a'])";
    let statement = optimize(source);
    let error = bind(source, &statement, &mut UnreachedEvaluation).unwrap_err();
    assert_eq!(error.kind(), QueryServiceErrorKind::Unsupported);
    assert_eq!(
        error.message(),
        "Table procedure OPTIMIZE argument 'sorted_by' is not supported: it requires support for writing files in the specified sort order",
    );
}
