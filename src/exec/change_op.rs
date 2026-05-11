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
use std::sync::Arc;

use arrow::array::{ArrayRef, Int8Array};
use arrow::datatypes::{DataType, Field};

pub const CHANGE_OP_COLUMN: &str = "__change_op";
pub const CHANGE_OP_INSERT: i8 = 1;
pub const CHANGE_OP_DELETE: i8 = -1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeOp {
    Insert,
    Delete,
}

impl ChangeOp {
    pub fn value(self) -> i8 {
        match self {
            ChangeOp::Insert => CHANGE_OP_INSERT,
            ChangeOp::Delete => CHANGE_OP_DELETE,
        }
    }

    pub fn from_i8(value: i8) -> Result<Self, String> {
        match value {
            CHANGE_OP_INSERT => Ok(ChangeOp::Insert),
            CHANGE_OP_DELETE => Ok(ChangeOp::Delete),
            _ => Err(format!(
                "invalid value {value} for {CHANGE_OP_COLUMN}; expected {CHANGE_OP_INSERT} for insert or {CHANGE_OP_DELETE} for delete"
            )),
        }
    }
}

pub fn change_op_field() -> Field {
    Field::new(CHANGE_OP_COLUMN, DataType::Int8, false)
}

pub fn change_op_array(op: ChangeOp, row_count: usize) -> ArrayRef {
    Arc::new(Int8Array::from(vec![op.value(); row_count])) as ArrayRef
}

pub fn validate_change_op_value(value: i8) -> Result<(), String> {
    ChangeOp::from_i8(value).map(|_| ())
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int8Array};
    use arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn change_op_values_are_stable() {
        assert_eq!(ChangeOp::Insert.value(), 1);
        assert_eq!(ChangeOp::Delete.value(), -1);
        assert_eq!(CHANGE_OP_INSERT, 1);
        assert_eq!(CHANGE_OP_DELETE, -1);
        assert_eq!(CHANGE_OP_COLUMN, "__change_op");

        assert_eq!(ChangeOp::from_i8(1).unwrap(), ChangeOp::Insert);
        assert_eq!(ChangeOp::from_i8(-1).unwrap(), ChangeOp::Delete);

        let error = ChangeOp::from_i8(0).unwrap_err();
        assert!(error.contains("0"));
        assert!(error.contains("1"));
        assert!(error.contains("-1"));
        assert!(error.contains("__change_op"));
        assert!(validate_change_op_value(0).is_err());
    }

    #[test]
    fn change_op_field_uses_int8_non_nullable_contract() {
        let field = change_op_field();

        assert_eq!(field.name(), "__change_op");
        assert_eq!(field.data_type(), &DataType::Int8);
        assert!(!field.is_nullable());
    }

    #[test]
    fn change_op_array_uses_int8_values() {
        let array = change_op_array(ChangeOp::Delete, 3);
        let values = array.as_any().downcast_ref::<Int8Array>().unwrap();

        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), -1);
        assert_eq!(values.value(1), -1);
        assert_eq!(values.value(2), -1);
    }
}
