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

pub(crate) fn unexpected_character(character: char) -> String {
    format!("unexpected character `{character}`")
}

pub(crate) fn unterminated(kind: &str) -> String {
    format!("unterminated {kind}")
}

pub(crate) fn unexpected_token(expected: &str, found: &str) -> String {
    format!("expected {expected}, found {found}")
}

pub(crate) fn unsupported_statement(statement: &str) -> String {
    format!("recognized but unsupported statement {statement}")
}

use crate::StructuralViolation;

pub(crate) fn invalid_structure(violation: StructuralViolation) -> String {
    match violation {
        StructuralViolation::EmptyWithCteList => {
            "invalid SQL structure: empty WITH common-table-expression list".to_owned()
        }
        StructuralViolation::EmptyValuesRowList => {
            "invalid SQL structure: empty VALUES row list".to_owned()
        }
        StructuralViolation::EmptyValuesRow => "invalid SQL structure: empty VALUES row".to_owned(),
        StructuralViolation::EmptySelectProjection => {
            "invalid SQL structure: empty SELECT projection list".to_owned()
        }
        StructuralViolation::EmptyGroupingSets => {
            "invalid SQL structure: empty grouping-set list".to_owned()
        }
        StructuralViolation::InvalidSetOperationOperandCount => {
            "invalid SQL structure: invalid set operation operand count".to_owned()
        }
        StructuralViolation::InvalidLimitRowCount => {
            "invalid SQL structure: LIMIT count must be ALL, a long literal, or a parameter"
                .to_owned()
        }
        StructuralViolation::InvalidOffsetRowCount => {
            "invalid SQL structure: OFFSET count must be a long literal or a parameter".to_owned()
        }
        StructuralViolation::InvalidFetchFirstRowCount => {
            "invalid SQL structure: FETCH count must be a long literal or a parameter".to_owned()
        }
        StructuralViolation::EmptyPatternList => {
            "invalid SQL structure: empty row-pattern list".to_owned()
        }
        StructuralViolation::EmptySubsetIdentifierList => {
            "invalid SQL structure: empty SUBSET identifier list".to_owned()
        }
        StructuralViolation::EmptyOrderBy => {
            "invalid SQL structure: empty ORDER BY list".to_owned()
        }
        StructuralViolation::EmptyInList => "invalid SQL structure: empty IN list".to_owned(),
        StructuralViolation::InvalidLogicalTermCount => {
            "invalid SQL structure: logical expression requires at least two terms".to_owned()
        }
        StructuralViolation::InvalidQueryModifierOwner => {
            "direct SELECT modifiers must belong to the query specification".to_string()
        }
        StructuralViolation::JsonConstructorInvariant(error) => {
            format!("invalid SQL structure: {error}")
        }
        StructuralViolation::EmptyUnnestExpressionList => {
            "invalid SQL structure: empty UNNEST expression list".to_owned()
        }
        StructuralViolation::TooFewCoalesceOperands => {
            "invalid SQL structure: COALESCE requires at least two operands".to_owned()
        }
        StructuralViolation::TooFewFormatArguments => {
            "invalid SQL structure: FORMAT requires at least two arguments".to_owned()
        }
        StructuralViolation::EmptyCaseBranches => {
            "invalid SQL structure: empty CASE branch list".to_owned()
        }
        StructuralViolation::DuplicateTableColumnName => {
            "invalid SQL structure: duplicate CREATE TABLE column name".to_owned()
        }
    }
}

pub(crate) fn duplicate_cte_name(name: &str) -> String {
    format!("duplicate common table expression name `{name}`")
}

pub(crate) fn duplicate_window_name(name: &str) -> String {
    format!("duplicate named window `{name}`")
}

pub(crate) fn invalid_window_frame_bounds() -> String {
    "invalid window frame bounds".to_owned()
}
