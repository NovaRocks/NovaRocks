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
//! Public INSERT literal byte projection and rejection, independent of SELECT coercion.
use novarocks_sql::{
    literal::{expr_to_literal, latin1_string_to_bytes},
    semantic::Literal,
};
fn literal_source(source: &str) -> Result<Literal, novarocks_sql::literal::LiteralFailure> {
    let expression = novarocks_parser::parser::parse_expression(source).unwrap();
    expr_to_literal(&expression)
}
fn insert_literal_bytes(source: &str) -> Vec<u8> {
    let Literal::String(value) = literal_source(source).unwrap() else {
        panic!("original HLL INSERT result is Latin1 string")
    };
    latin1_string_to_bytes(&value).unwrap()
}
fn expected(bytes: &[u8]) -> Vec<u8> {
    use novarocks_types::value::hll::{MURMUR_SEED, encode_hll_single, murmur_hash64a};
    encode_hll_single(murmur_hash64a(bytes, MURMUR_SEED))
}
#[test]
fn original_hll_insert_literal_byte_author_full_scalar_projections() {
    assert_eq!(
        insert_literal_bytes("hll_hash(NULL)"),
        novarocks_types::value::hll::encode_hll_empty()
    );
    for (source, value) in [
        ("hll_hash(0)", 0i64),
        ("hll_hash(1)", 1),
        ("hll_hash(-7)", -7),
        ("hll_hash(9223372036854775807)", i64::MAX),
    ] {
        assert_eq!(insert_literal_bytes(source), expected(&value.to_le_bytes()));
    }
    for (source, value) in [
        ("hll_hash(0.0)", 0.0f64),
        ("hll_hash(-0.0)", -0.0),
        ("hll_hash(1.25)", 1.25),
    ] {
        assert_eq!(insert_literal_bytes(source), expected(&value.to_le_bytes()));
    }
    for (source, value) in [("hll_hash(true)", true), ("hll_hash(false)", false)] {
        assert_eq!(insert_literal_bytes(source), expected(&[u8::from(value)]));
    }
    for (source, value) in [
        ("hll_hash('')", ""),
        ("hll_hash('ascii')", "ascii"),
        ("hll_hash('é💥')", "é💥"),
    ] {
        assert_eq!(insert_literal_bytes(source), expected(value.as_bytes()));
    }
}
#[test]
fn original_hll_insert_literal_narrowing_cast_refusal_and_allowed_peel() {
    for name in [
        "TINYINT",
        "SMALLINT",
        "INT",
        "INTEGER",
        "INT2",
        "INT4",
        "MEDIUMINT",
        "FLOAT",
    ] {
        assert_eq!(
            (literal_source(&format!("hll_hash(CAST(7 AS {name}))")).unwrap_err()).to_string(),
            "hll_hash with narrowing CAST argument is not supported in INSERT VALUES; wrap the value directly without CAST"
        );
    }
    for name in ["BIGINT", "DOUBLE", "VARCHAR"] {
        assert_eq!(
            insert_literal_bytes(&format!("hll_hash(CAST(7 AS {name}))")),
            expected(&7i64.to_le_bytes())
        );
    }
}
#[test]
fn original_hll_insert_literal_arity_and_unsupported_full_text() {
    for source in ["hll_hash()", "hll_hash(1,2)"] {
        assert_eq!(
            (literal_source(source).unwrap_err()).to_string(),
            "hll_hash expects 1 argument"
        );
    }
    assert_eq!(
        (literal_source("hll_hash([1,2])").unwrap_err()).to_string(),
        "hll_hash unsupported literal: Array([Int(1), Int(2)])"
    );
}
