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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, StringArray};
use std::sync::Arc;
use url::Url;

pub fn eval_parse_url(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let url_arr = arena.eval(args[0], chunk)?;
    let part_arr = arena.eval(args[1], chunk)?;
    let url_arr = url_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "parse_url expects string".to_string())?;
    let part_arr = part_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "parse_url expects string".to_string())?;
    let key_arr = if args.len() == 3 {
        Some(
            arena
                .eval(args[2], chunk)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "parse_url expects string".to_string())?
                .clone(),
        )
    } else {
        None
    };
    let len = url_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if url_arr.is_null(i) || part_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let url_str = url_arr.value(i);
        let part = part_arr.value(i).to_uppercase();
        let url = Url::parse(url_str).ok();
        let val = match (url, part.as_str()) {
            (Some(u), "HOST") => u.host_str().map(|s| s.to_string()),
            (Some(u), "PATH") => Some(u.path().to_string()),
            (Some(u), "PROTOCOL") => Some(u.scheme().to_string()),
            (Some(u), "REF") => u.fragment().map(|s| s.to_string()),
            (Some(u), "QUERY") => {
                if let Some(key_arr) = key_arr.as_ref() {
                    if key_arr.is_null(i) {
                        None
                    } else {
                        let key = key_arr.value(i);
                        u.query_pairs()
                            .find(|(k, _)| k == key)
                            .map(|(_, v)| v.to_string())
                    }
                } else {
                    u.query().map(|s| s.to_string())
                }
            }
            _ => None,
        };
        out.push(val);
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_parse_url_logic() {
        assert_string_function_logic("parse_url");
    }
}
