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

use crate::StatementFailure;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq)]
pub enum TrinoValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(f32),
    Double(f64),
    Text(String),
    Binary(Vec<u8>),
    Array(Vec<TrinoValue>),
    Row(Vec<TrinoValue>),
    /// Entries retain their order in the original JSON document.
    Map(Vec<(TrinoValue, TrinoValue)>),
    Variant(String),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TypeSignature {
    pub raw_type: String,
    #[serde(default)]
    pub arguments: Vec<TypeArgument>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value")]
pub enum TypeArgument {
    #[serde(rename = "LONG")]
    Long(u64),
    #[serde(rename = "TYPE")]
    Type(TypeSignature),
    #[serde(rename = "NAMED_TYPE")]
    NamedType(NamedTypeSignature),
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NamedTypeSignature {
    pub field_name: Option<RowFieldName>,
    pub type_signature: TypeSignature,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RowFieldName {
    pub name: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub type_signature: TypeSignature,
}
#[derive(Clone, Debug, PartialEq)]
pub struct StatementResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<TrinoValue>>,
    pub query_id: String,
    pub pages: usize,
    pub update_type: Option<String>,
}
impl StatementResult {
    pub fn has_columns(&self) -> bool {
        !self.columns.is_empty()
    }
}
impl TypeSignature {
    pub fn simple(raw_type: impl Into<String>) -> Self {
        Self {
            raw_type: raw_type.into(),
            arguments: Vec::new(),
        }
    }
    pub fn validate(&self) -> Result<(), StatementFailure> {
        use TypeArgument as A;
        let supported = match self.raw_type.as_str() {
            "unknown" | "boolean" | "tinyint" | "smallint" | "integer" | "bigint" | "real"
            | "double" | "varbinary" | "date" | "json" | "variant" => self.arguments.is_empty(),
            "varchar" => matches!(self.arguments.as_slice(), [] | [A::Long(_)]),
            "char" => matches!(self.arguments.as_slice(), [A::Long(_)]),
            "decimal" => {
                matches!(self.arguments.as_slice(), [A::Long(p), A::Long(s)] if (1..=38).contains(p) && s <= p)
            }
            "time" | "time with time zone" | "timestamp" | "timestamp with time zone" => {
                matches!(self.arguments.as_slice(), [] | [A::Long(0..=12)])
            }
            "array" => matches!(self.arguments.as_slice(), [A::Type(_)]),
            "map" => matches!(self.arguments.as_slice(), [A::Type(_), A::Type(_)]),
            "row" => self.arguments.iter().all(|a| matches!(a, A::NamedType(_))),
            _ => false,
        };
        if !supported {
            return Err(StatementFailure::protocol(format!(
                "unsupported Trino type signature: {self:?}"
            )));
        }
        for arg in &self.arguments {
            match arg {
                A::Type(child) => child.validate()?,
                A::NamedType(child) => child.type_signature.validate()?,
                A::Long(_) => {}
            }
        }
        Ok(())
    }
    pub fn type_argument(&self, index: usize) -> Result<&TypeSignature, StatementFailure> {
        match self.arguments.get(index) {
            Some(TypeArgument::Type(child)) => Ok(child),
            _ => Err(StatementFailure::protocol("missing Trino type argument")),
        }
    }
}
