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
use crate::connector::MinMaxPredicate;
use crate::exprs;
use crate::lower::expr::literals::{
    build_date_literal, build_decimal_literal, build_float_literal, build_int_literal,
};
use crate::lower::layout::Layout;

/// Parse a min/max conjunct TExpr into MinMaxPredicate used for row group pruning.
pub(crate) fn parse_min_max_conjunct(
    expr: &exprs::TExpr,
    layout: &Layout,
) -> Result<Option<MinMaxPredicate>, String> {
    if expr.nodes.is_empty() {
        return Ok(None);
    }

    let root = &expr.nodes[0];

    if root.node_type != exprs::TExprNodeType::BINARY_PRED {
        return Ok(None);
    }

    let Some(opcode) = root.opcode else {
        return Ok(None);
    };

    let predicate_type = if opcode == crate::opcodes::TExprOpcode::LE {
        "Le"
    } else if opcode == crate::opcodes::TExprOpcode::GE {
        "Ge"
    } else if opcode == crate::opcodes::TExprOpcode::LT {
        "Lt"
    } else if opcode == crate::opcodes::TExprOpcode::GT {
        "Gt"
    } else if opcode == crate::opcodes::TExprOpcode::EQ {
        "Eq"
    } else {
        return Ok(None);
    };

    if expr.nodes.len() < 3 {
        return Ok(None);
    }

    let left_node = &expr.nodes[1];
    let right_node = &expr.nodes[2];

    if left_node.node_type != exprs::TExprNodeType::SLOT_REF {
        return Ok(None);
    }
    let Some(slot_ref) = &left_node.slot_ref else {
        return Ok(None);
    };

    let column = get_column_name_from_slot(slot_ref, layout)?;
    let value = extract_literal_value(right_node)?;

    let predicate = match predicate_type {
        "Le" => MinMaxPredicate::Le { column, value },
        "Ge" => MinMaxPredicate::Ge { column, value },
        "Lt" => MinMaxPredicate::Lt { column, value },
        "Gt" => MinMaxPredicate::Gt { column, value },
        "Eq" => MinMaxPredicate::Eq { column, value },
        _ => return Ok(None),
    };

    Ok(Some(predicate))
}

fn get_column_name_from_slot(
    slot_ref: &exprs::TSlotRef,
    layout: &Layout,
) -> Result<String, String> {
    let key = (slot_ref.tuple_id, slot_ref.slot_id);
    let idx = layout
        .index
        .get(&key)
        .ok_or_else(|| format!("slot not found in layout: {:?}", key))?;

    Ok(idx.to_string())
}

fn extract_literal_value(
    node: &exprs::TExprNode,
) -> Result<crate::exec::expr::LiteralValue, String> {
    use crate::exec::expr::LiteralValue;

    match node.node_type {
        t if t == exprs::TExprNodeType::INT_LITERAL => {
            let v = node
                .int_literal
                .as_ref()
                .ok_or_else(|| "INT_LITERAL missing value".to_string())?
                .value;
            build_int_literal(node, v)
        }
        t if t == exprs::TExprNodeType::DECIMAL_LITERAL => {
            let v = node
                .decimal_literal
                .as_ref()
                .ok_or_else(|| "DECIMAL_LITERAL missing value".to_string())?
                .value
                .clone();
            build_decimal_literal(node, &v)
        }
        t if t == exprs::TExprNodeType::FLOAT_LITERAL => {
            let v = node
                .float_literal
                .as_ref()
                .ok_or_else(|| "FLOAT_LITERAL missing value".to_string())?
                .value;
            Ok(build_float_literal(node, v.0))
        }
        t if t == exprs::TExprNodeType::BOOL_LITERAL => {
            let v = node
                .bool_literal
                .as_ref()
                .ok_or_else(|| "BOOL_LITERAL missing value".to_string())?
                .value;
            Ok(LiteralValue::Bool(v))
        }
        t if t == exprs::TExprNodeType::STRING_LITERAL => {
            let v = node
                .string_literal
                .as_ref()
                .ok_or_else(|| "STRING_LITERAL missing value".to_string())?
                .value
                .clone();
            Ok(LiteralValue::Utf8(v))
        }
        t if t == exprs::TExprNodeType::DATE_LITERAL => {
            let v = node
                .date_literal
                .as_ref()
                .ok_or_else(|| "DATE_LITERAL missing value".to_string())?
                .value
                .clone();
            build_date_literal(node, &v)
        }
        t if t == exprs::TExprNodeType::NULL_LITERAL => Ok(LiteralValue::Null),
        _ => Err(format!("unsupported literal type: {:?}", node.node_type)),
    }
}
