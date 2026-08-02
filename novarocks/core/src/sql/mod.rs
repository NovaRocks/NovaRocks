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

pub(crate) mod agg_mergeability;
pub mod common;

#[cfg(feature = "runtime-filter-test-support")]
pub mod analysis;
#[cfg(not(feature = "runtime-filter-test-support"))]
pub(crate) mod analysis;
pub(crate) mod catalog;
pub(crate) mod column_id;
pub(crate) mod compiler;
pub(crate) mod functions;
pub(crate) mod literal;
pub mod mv_refresh;
pub(crate) mod parser;

pub(crate) mod optimizer;

pub(crate) mod analyzer;
pub(crate) mod explain;
#[cfg(feature = "runtime-filter-test-support")]
pub mod planner;
#[cfg(not(feature = "runtime-filter-test-support"))]
pub(crate) mod planner;

pub(crate) use parser::ast::{
    ColumnAggregation, Literal, TableColumnDef, TableKeyDesc, TableKeyKind,
};
pub use parser::dialect::substitute_user_variables;

#[cfg(test)]
mod common_tests {
    use arrow::datatypes::DataType;

    use super::column_id::ColumnId;
    use super::common::{
        ApplyKind, BinOp, CteId, ImvVersionRef, ImvVersionRole, JoinKind, LambdaParam,
        LiteralValue, OutputColumn, ScanVariantColumn, UnOp, WindowBound, WindowFrame,
        WindowFrameType,
    };

    #[test]
    fn common_module_reexports_shared_optimizer_vocabulary() {
        let cte_id: CteId = 7;
        assert_eq!(cte_id, 7);

        let output = OutputColumn {
            column_id: ColumnId::new_for_test(1),
            name: "internal_col".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: true,
        };
        assert_eq!(output.column_id, ColumnId::new_for_test(1));
        assert!(output.is_internal);

        assert_eq!(JoinKind::NullAwareLeftAnti, JoinKind::NullAwareLeftAnti);
        let lambda = LambdaParam {
            name: "x".to_string(),
            slot_id: 3,
            data_type: DataType::Utf8,
            nullable: true,
        };
        assert_eq!(lambda.slot_id, 3);

        let frame = WindowFrame {
            frame_type: WindowFrameType::Rows,
            start: WindowBound::UnboundedPreceding,
            end: WindowBound::CurrentRow,
        };
        assert_eq!(frame.frame_type, WindowFrameType::Rows);
        assert_eq!(LiteralValue::LargeInt(123), LiteralValue::LargeInt(123));
        assert_eq!(BinOp::EqForNull, BinOp::EqForNull);
        assert_eq!(UnOp::BitwiseNot, UnOp::BitwiseNot);

        assert_eq!(
            ApplyKind::In { negated: true },
            ApplyKind::In { negated: true }
        );

        let variant = ScanVariantColumn {
            source_column_id: ColumnId::new_for_test(4),
            source_column: "v".to_string(),
            synthetic_column_id: ColumnId::new_for_test(5),
            synthetic_column: "v.a".to_string(),
            canonical_path: "$.a".to_string(),
            requested_type: DataType::Int32,
            strict: true,
        };
        assert_eq!(variant.canonical_path, "$.a");

        assert_eq!(ImvVersionRef::from_snapshot().role, ImvVersionRole::From);
        assert_eq!(ImvVersionRef::default().role, ImvVersionRole::To);
    }
}
