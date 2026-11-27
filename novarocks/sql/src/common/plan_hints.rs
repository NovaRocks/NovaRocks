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

use arrow::datatypes::DataType;

use crate::column_id::ColumnId;

/// Ranking semantics selected by SQL for a TopN boundary.
///
/// Native encoding and execution translate this SQL fact explicitly; it is
/// deliberately not an execution-node re-export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlTopNType {
    RowNumber,
    Rank,
    DenseRank,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyKind {
    Scalar,
    Exists { negated: bool },
    In { negated: bool },
}

/// An immutable normalized derived operation and its original creation data.
/// Equality/Debug describe the operation; neither proves source provenance.
#[derive(Clone)]
pub struct ScanVariantColumn {
    source_column_id: ColumnId,
    source_column: String,
    synthetic_column_id: ColumnId,
    synthetic_column: String,
    canonical_path: String,
    requested_type: DataType,
    requested_type_literal: String,
    strict: bool,
    binding: crate::binding::SqlFunctionBinding,
    source: std::sync::Arc<super::variant_source::DerivedVariantSource>,
}
impl ScanVariantColumn {
    pub(crate) fn new_observed(
        source_column_id: ColumnId,
        source_column: String,
        synthetic_column_id: ColumnId,
        synthetic_column: String,
        canonical_path: String,
        requested_type: DataType,
        requested_type_literal: String,
        strict: bool,
        source: std::sync::Arc<super::variant_source::DerivedVariantSource>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, novarocks_type_contract::CompileControlError> {
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let binding = source.captured().binding().clone();
        work.step()?;
        let descriptor = Self {
            source_column_id,
            source_column,
            synthetic_column_id,
            synthetic_column,
            canonical_path,
            requested_type,
            requested_type_literal,
            strict,
            binding,
            source,
        };
        work.step()?;
        work.finish()?;
        Ok(descriptor)
    }
    pub fn source_column_id(&self) -> ColumnId {
        self.source_column_id
    }
    pub fn source_column(&self) -> &str {
        &self.source_column
    }
    pub fn synthetic_column_id(&self) -> ColumnId {
        self.synthetic_column_id
    }
    pub fn synthetic_column(&self) -> &str {
        &self.synthetic_column
    }
    pub fn canonical_path(&self) -> &str {
        &self.canonical_path
    }
    pub fn requested_type(&self) -> &DataType {
        &self.requested_type
    }
    pub fn requested_type_literal(&self) -> &str {
        &self.requested_type_literal
    }
    pub fn strict(&self) -> bool {
        self.strict
    }
    pub fn binding(&self) -> &crate::binding::SqlFunctionBinding {
        &self.binding
    }
    pub(crate) fn source(&self) -> &std::sync::Arc<super::variant_source::DerivedVariantSource> {
        &self.source
    }

    #[cfg(test)]
    pub(crate) fn test_fixture(
        source_column_id: ColumnId,
        source_column: String,
        synthetic_column_id: ColumnId,
        synthetic_column: String,
        canonical_path: String,
        requested_type: DataType,
        requested_type_literal: String,
        strict: bool,
        source_type: novarocks_type_contract::FunctionValueType,
    ) -> Self {
        use crate::analysis::{ExprKind, TypedExpr};
        use novarocks_functions::FunctionArgument;
        use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
        struct FixtureControl;
        impl novarocks_type_contract::PureCompileControl for FixtureControl {
            fn checkpoint(
                &self,
                _: novarocks_type_contract::CompilePhase,
                _: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                Ok(())
            }
        }
        let args = [
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: source_column_id,
                    qualifier: None,
                    column: source_column.clone(),
                },
                value_type: source_type,
            },
            TypedExpr {
                kind: ExprKind::Literal(crate::common::LiteralValue::String(
                    canonical_path.clone(),
                )),
                value_type: FunctionValueType::new(DataType::Utf8, false),
            },
            TypedExpr {
                kind: ExprKind::Literal(crate::common::LiteralValue::String(
                    requested_type_literal.clone(),
                )),
                value_type: FunctionValueType::new(DataType::Utf8, false),
            },
        ];
        // A structural fixture author, not an installed capability claim.
        let original = crate::analysis::test_function_binding(
            if strict {
                "variant_get"
            } else {
                "try_variant_get"
            },
            &args,
            requested_type.clone(),
            true,
            novarocks_functions::FunctionVolatility::Immutable,
        );
        let binding = crate::binding::SqlFunctionBinding::from_test(
            original.resolved().clone(),
            DecimalOverflowPolicy::ReportError,
        );
        let policy = crate::constant::test_constant_policy();
        let captured = crate::binding::capture_logical_call_arguments(
            &binding,
            3,
            &args,
            policy,
            &FixtureControl,
        )
        .unwrap();
        let cv = |i| match &captured.request().arguments[i] {
            FunctionArgument::Value {
                constant: Some(value),
                ..
            } => value.clone(),
            _ => panic!("fixture authored string constant"),
        };
        let path = cv(1);
        let target = cv(2);
        let source = super::variant_source::DerivedVariantSource::new_observed(
            captured,
            path,
            target,
            &FixtureControl,
        )
        .unwrap();
        Self::new_observed(
            source_column_id,
            source_column,
            synthetic_column_id,
            synthetic_column,
            canonical_path,
            requested_type,
            requested_type_literal,
            strict,
            std::sync::Arc::new(source),
            &FixtureControl,
        )
        .unwrap()
    }
}
impl PartialEq for ScanVariantColumn {
    fn eq(&self, other: &Self) -> bool {
        self.source_column_id == other.source_column_id
            && self.source_column == other.source_column
            && self.synthetic_column_id == other.synthetic_column_id
            && self.synthetic_column == other.synthetic_column
            && self.canonical_path == other.canonical_path
            && self.requested_type == other.requested_type
            && self.requested_type_literal == other.requested_type_literal
            && self.strict == other.strict
            && self.binding == other.binding
    }
}
impl std::fmt::Debug for ScanVariantColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanVariantColumn")
            .field("source_column_id", &self.source_column_id)
            .field("source_column", &self.source_column)
            .field("synthetic_column_id", &self.synthetic_column_id)
            .field("synthetic_column", &self.synthetic_column)
            .field("canonical_path", &self.canonical_path)
            .field("requested_type", &self.requested_type)
            .field("requested_type_literal", &self.requested_type_literal)
            .field("strict", &self.strict)
            .field("binding", &self.binding)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::SqlTopNType;

    #[test]
    fn sqlx2_planner_vocabulary_topn_ranking_is_sql_owned() {
        assert_ne!(SqlTopNType::RowNumber, SqlTopNType::Rank);
        assert_ne!(SqlTopNType::Rank, SqlTopNType::DenseRank);
        assert_ne!(SqlTopNType::DenseRank, SqlTopNType::RowNumber);
    }
}
