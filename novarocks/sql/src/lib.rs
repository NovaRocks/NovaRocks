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

#![cfg_attr(
    test,
    expect(
        clippy::disallowed_methods,
        reason = "Unit tests exercise thread/task mechanics under their original test owners; the production library is linted separately; see ADR-0218."
    )
)]

pub mod admission;
pub(crate) mod agg_mergeability;
pub mod analyze_error;
pub(crate) mod common;

pub(crate) mod analysis;
pub mod binding;
pub(crate) mod catalog;
mod column_id;
pub mod compiler;
pub(crate) mod constant;
pub(crate) mod functions;
pub mod literal;
pub(crate) mod mv_refresh;
pub(crate) mod parser;
pub mod planning;
pub mod semantic;
pub mod sql_mode;

pub(crate) mod optimizer;

pub(crate) mod analyzer;
pub(crate) mod explain;
pub(crate) mod planner;

#[cfg(test)]
pub(crate) mod test_value_types;

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
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: true,
        };
        assert_eq!(output.column_id, ColumnId::new_for_test(1));
        assert!(output.is_internal);

        assert_eq!(JoinKind::NullAwareLeftAnti, JoinKind::NullAwareLeftAnti);
        let lambda = LambdaParam {
            name: "x".to_string(),
            slot_id: 3,
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
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

        let variant = ScanVariantColumn::test_fixture(
            ColumnId::new_for_test(4),
            "v".to_string(),
            ColumnId::new_for_test(5),
            "v.a".to_string(),
            "$.a".to_string(),
            DataType::Int32,
            "int".to_string(),
            true,
            novarocks_type_contract::FunctionValueType::new(DataType::LargeBinary, true),
        );
        assert_eq!(variant.canonical_path(), "$.a");

        assert_eq!(ImvVersionRef::from_snapshot().role, ImvVersionRole::From);
        assert_eq!(ImvVersionRef::default().role, ImvVersionRole::To);
    }
}

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");
