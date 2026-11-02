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

use novarocks_type_contract::FunctionValueType;

use crate::analysis::OutputColumn;
use crate::column_id::ColumnId;
use crate::optimizer::rewrite::context::RewriteContext;

enum DeclaredColumnError {
    Source(novarocks_types::ColumnValueTypeError),
    Control(novarocks_type_contract::CompileControlError),
}

impl From<novarocks_types::ColumnValueTypeError> for DeclaredColumnError {
    fn from(error: novarocks_types::ColumnValueTypeError) -> Self {
        Self::Source(error)
    }
}

impl From<novarocks_type_contract::ValueTypeError> for DeclaredColumnError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Source(error.into())
    }
}

/// Project the catalog owner's complete declaration under the same request.
/// Allocation and the remaining legacy IMV helpers are separate obligations.
pub(crate) fn declared_imv_column_value_type(
    column: &novarocks_types::schema::ColumnDef,
    ctx: &RewriteContext,
) -> Result<FunctionValueType, crate::compiler::SqlCompileError> {
    use crate::compiler::SqlCompileError;
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ValueTypeError};

    let control = ctx.control_view();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)?;
    let value_type = column
        .declared_value_type_observed(|| work.step().map_err(DeclaredColumnError::Control))
        .map_err(|error| match error {
            DeclaredColumnError::Control(error) => SqlCompileError::from(error),
            DeclaredColumnError::Source(novarocks_types::ColumnValueTypeError::Type(
                ValueTypeError::TooDeep | ValueTypeError::TooManyNodes,
            )) => SqlCompileError::ResourceExhausted,
            DeclaredColumnError::Source(error) => SqlCompileError::Compilation(format!(
                "invalid IMV catalog column `{}`: {error}",
                column.name
            )),
        })?;
    work.finish()?;
    Ok(value_type)
}

pub(crate) fn allocate_imv_column(
    ctx: &RewriteContext,
    name: &str,
    value_type: FunctionValueType,
) -> Result<ColumnId, String> {
    let factory = ctx
        .column_ref_factory()
        .ok_or_else(|| "IMV rewrite requires ColumnRefFactory in RewriteContext".to_string())?;
    Ok(factory
        .borrow_mut()
        .create(None, name.to_string(), value_type))
}

pub(crate) fn allocate_imv_output_column(
    ctx: &RewriteContext,
    name: &str,
    value_type: FunctionValueType,
    is_internal: bool,
) -> Result<OutputColumn, String> {
    let column_id = allocate_imv_column(ctx, name, value_type.clone())?;
    Ok(OutputColumn {
        column_id,
        name: name.to_string(),
        value_type,
        is_internal,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::column_id::ColumnRefFactory;

    #[test]
    fn allocate_imv_column_requires_factory_in_context() {
        let ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());

        let err = allocate_imv_column(
            &ctx,
            "__imv_probe",
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        )
        .expect_err("missing factory should be a hard IMV rewrite error");

        assert_eq!(
            err,
            "IMV rewrite requires ColumnRefFactory in RewriteContext"
        );
    }

    #[test]
    fn allocate_imv_output_column_records_real_factory_metadata() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_column_ref_factory(Rc::clone(&factory));

        let output = allocate_imv_output_column(
            &ctx,
            "__imv_branch",
            novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
            true,
        )
        .expect("factory-backed allocation should succeed");

        assert_eq!(output.column_id.0, 1);
        assert_eq!(output.name, "__imv_branch");
        assert_eq!(output.value_type.data_type().clone(), DataType::Int32);
        assert!(!output.value_type.nullable());
        assert!(output.is_internal);

        let meta = factory.borrow().get(output.column_id).clone();
        assert_eq!(meta.name, "__imv_branch");
        assert_eq!(meta.value_type.data_type().clone(), DataType::Int32);
        assert!(!meta.value_type.nullable());
        assert_eq!(factory.borrow().peek_next_id(), 2);
    }

    #[test]
    fn allocation_preserves_full_logical_and_nested_field_source() {
        use arrow::datatypes::Field;
        use novarocks_type_contract::ValueLogicalType;

        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_column_ref_factory(Rc::clone(&factory));
        let json =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let output = allocate_imv_output_column(&ctx, "visible", json.clone(), false).unwrap();
        assert_eq!(output.value_type, json);
        assert_eq!(factory.borrow().get(output.column_id).value_type, json);
        let nested = FunctionValueType::new(
            DataType::Struct(
                vec![Field::new("payload", DataType::Utf8, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([
                        ("nr_logical_type".into(), "json".into()),
                        ("provider.field-id".into(), "71".into()),
                    ]),
                )]
                .into(),
            ),
            false,
        );
        let output =
            allocate_imv_output_column(&ctx, "state_source", nested.clone(), true).unwrap();
        assert_eq!(output.value_type, nested);
        assert_eq!(factory.borrow().get(output.column_id).value_type, nested);
    }

    #[test]
    fn catalog_projection_keeps_declaration_and_same_request_typed_stops() {
        use crate::compiler::SqlCompileError;
        use crate::optimizer::rewrite::context::RewriteConsumer;
        use arrow::datatypes::Field;
        use novarocks_type_contract::{
            CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
        };
        use novarocks_types::schema::{ColumnDef, SqlType};
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Stop {
            error: CompileControlError,
            positive: bool,
            stopped: AtomicBool,
        }
        impl PureCompileControl for Stop {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                work: u32,
            ) -> Result<(), CompileControlError> {
                assert_eq!(phase, CompilePhase::Validate);
                assert!(work <= 256);
                if (work > 0) == self.positive && !self.stopped.swap(true, Ordering::SeqCst) {
                    return Err(self.error.clone());
                }
                Ok(())
            }
        }
        let json = ColumnDef {
            name: "visible".into(),
            data_type: DataType::Utf8,
            nullable: true,
            write_default: None,
            logical_type: Some(SqlType::Json),
        };
        let fixture_ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        assert_eq!(
            declared_imv_column_value_type(&json, &fixture_ctx).unwrap(),
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap()
        );
        let wide = ColumnDef {
            name: "wide".into(),
            data_type: DataType::Struct(
                (0..320)
                    .map(|i| Field::new(format!("f{i}"), DataType::Int64, false))
                    .collect::<Vec<_>>()
                    .into(),
            ),
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        for (error, expected) in [
            (CompileControlError::Cancelled, SqlCompileError::Cancelled),
            (
                CompileControlError::DeadlineExceeded,
                SqlCompileError::DeadlineExceeded,
            ),
            (
                CompileControlError::ResourceExhausted,
                SqlCompileError::ResourceExhausted,
            ),
        ] {
            for positive in [false, true] {
                let stop = Stop {
                    error: error.clone(),
                    positive,
                    stopped: AtomicBool::new(false),
                };
                let ctx = RewriteContext::new(
                    RewriteConsumer::MaterializedViewRefresh,
                    Default::default(),
                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    &stop,
                );
                assert_eq!(
                    declared_imv_column_value_type(&wide, &ctx),
                    Err(expected.clone())
                );
                assert!(stop.stopped.load(Ordering::SeqCst));
                // A one-shot stop is still the original failure, even though
                // the subsequent owner checkpoint is allowed to succeed.
                assert_eq!(stop.checkpoint(CompilePhase::Validate, 0), Ok(()));
            }
        }
    }
}
