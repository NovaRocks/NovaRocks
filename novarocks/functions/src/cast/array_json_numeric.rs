// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Numeric conversion from the original JSON value model.

use super::{
    EvaluationCheckpoints, LeafError, decimal_profile, number_facts, number_text, parse_double,
};
use crate::cast::{
    CastRowResult,
    trino::{self, ConversionFailure, Numeric},
};

pub(super) fn converted(
    result: Result<CastRowResult, ConversionFailure>,
) -> Result<Option<CastRowResult>, LeafError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(ConversionFailure::Outer(cause)) => Err(cause.into()),
        Err(_) => Ok(None),
    }
}

pub(super) fn convert(
    raw: &str,
    target: Numeric,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<CastRowResult>, LeafError> {
    let facts = number_facts(raw, work)?;
    if facts.exponent.is_some() {
        let value = parse_double(&facts, work)?;
        return converted(trino::convert_double(value, target, work));
    }
    let promoted;
    let value = if facts.zero {
        "0"
    } else if decimal_profile(&facts) {
        raw
    } else {
        promoted = number_text(raw, work)?;
        &promoted
    };
    match target {
        Numeric::Integer(width) => match trino::parse_decimal(value, 38, 0, work)? {
            Some(value) => converted(trino::signed_result(value, width)),
            None => Ok(None),
        },
        _ => converted(trino::parse_number(value, target, work)),
    }
}

pub(super) fn nonzero(raw: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<bool, LeafError> {
    let facts = number_facts(raw, work)?;
    if facts.exponent.is_some() {
        return Ok(parse_double(&facts, work)? != 0.0);
    }
    Ok(!facts.zero)
}
