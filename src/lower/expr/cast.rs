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
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::types;
use arrow::datatypes::DataType;

/// Lower CAST_EXPR expression to ExprNode::Cast.
pub(crate) fn lower_cast(
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
    target_primitive: Option<types::TPrimitiveType>,
    source_primitive: Option<types::TPrimitiveType>,
) -> Result<ExprId, String> {
    let child = children
        .first()
        .copied()
        .ok_or_else(|| "CAST missing child".to_string())?;
    if matches!(data_type, DataType::LargeBinary) {
        let child_type = arena
            .data_type(child)
            .ok_or_else(|| "CAST child missing data type".to_string())?;
        if !is_encoded_variant_payload_source(child_type) {
            return Err("CAST to VARIANT is not supported".to_string());
        }
    }
    if let Some(child_type) = arena.data_type(child)
        && matches!(child_type, DataType::LargeBinary)
        && !matches!(data_type, DataType::LargeBinary)
    {
        let supported = matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::Date32
                | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        if !supported {
            return Err("CAST from VARIANT is not supported".to_string());
        }
    }
    let node = if target_primitive == Some(types::TPrimitiveType::TIME) {
        if source_primitive == Some(types::TPrimitiveType::DATETIME) {
            ExprNode::CastTimeFromDatetime(child)
        } else {
            ExprNode::CastTime(child)
        }
    } else {
        ExprNode::Cast(child)
    };
    Ok(arena.push_typed(node, data_type))
}

fn is_encoded_variant_payload_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::Null
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;

    fn lower_variant_cast_from(source_type: DataType) -> Result<(ExprArena, ExprId), String> {
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), source_type);
        let cast = lower_cast(
            &[child],
            &mut arena,
            DataType::LargeBinary,
            Some(types::TPrimitiveType::VARIANT),
            None,
        )?;
        Ok((arena, cast))
    }

    #[test]
    fn lower_cast_allows_binary_payload_to_variant() {
        let (arena, cast) = lower_variant_cast_from(DataType::Binary).expect("cast");

        assert_eq!(arena.data_type(cast), Some(&DataType::LargeBinary));
        assert!(matches!(arena.node(cast), Some(ExprNode::Cast(_))));
    }

    #[test]
    fn lower_cast_allows_variant_payload_to_variant() {
        let (arena, cast) = lower_variant_cast_from(DataType::LargeBinary).expect("cast");

        assert_eq!(arena.data_type(cast), Some(&DataType::LargeBinary));
        assert!(matches!(arena.node(cast), Some(ExprNode::Cast(_))));
    }

    #[test]
    fn lower_cast_allows_null_to_variant() {
        let (arena, cast) = lower_variant_cast_from(DataType::Null).expect("cast");

        assert_eq!(arena.data_type(cast), Some(&DataType::LargeBinary));
        assert!(matches!(arena.node(cast), Some(ExprNode::Cast(_))));
    }

    #[test]
    fn lower_cast_rejects_scalar_to_variant() {
        let err = lower_variant_cast_from(DataType::Int64).unwrap_err();

        assert!(err.contains("CAST to VARIANT is not supported"));
    }
}
