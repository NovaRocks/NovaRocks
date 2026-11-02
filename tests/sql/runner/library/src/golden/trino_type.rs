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

use anyhow::{Result, bail};
use novarocks_test_trino_client::{NamedTypeSignature, RowFieldName, TypeArgument, TypeSignature};

/// Parses the complete type vocabulary emitted by Trino column metadata.
pub fn parse_type(text: &str) -> Result<TypeSignature> {
    let mut parser = Parser { text, position: 0 };
    let signature = parser.ty()?;
    parser.space();
    if parser.position != text.len() {
        bail!(
            "unexpected Trino type suffix at byte {}: {text}",
            parser.position
        );
    }
    signature.validate()?;
    Ok(signature)
}

struct Parser<'a> {
    text: &'a str,
    position: usize,
}

impl Parser<'_> {
    fn space(&mut self) {
        while self.rest().starts_with(char::is_whitespace) {
            self.position += self.rest().chars().next().unwrap().len_utf8();
        }
    }
    fn rest(&self) -> &str {
        &self.text[self.position..]
    }
    fn consume(&mut self, token: char) -> bool {
        self.space();
        if self.rest().starts_with(token) {
            self.position += token.len_utf8();
            true
        } else {
            false
        }
    }
    fn expect(&mut self, token: char) -> Result<()> {
        if !self.consume(token) {
            bail!("expected '{token}' in Trino type at byte {}", self.position);
        }
        Ok(())
    }
    fn word(&mut self) -> Result<String> {
        self.space();
        let start = self.position;
        while self
            .rest()
            .starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
        {
            self.position += 1;
        }
        if start == self.position {
            bail!("expected Trino type identifier at byte {start}");
        }
        Ok(self.text[start..self.position].to_string())
    }
    fn number(&mut self) -> Result<u64> {
        self.space();
        let start = self.position;
        while self.rest().starts_with(|c: char| c.is_ascii_digit()) {
            self.position += 1;
        }
        Ok(self.text[start..self.position].parse()?)
    }
    fn field_name(&mut self) -> Result<String> {
        if !self.consume('"') {
            return self.word();
        }
        let mut name = String::new();
        loop {
            let Some(ch) = self.rest().chars().next() else {
                bail!("unterminated row field name");
            };
            self.position += ch.len_utf8();
            if ch == '"' {
                if self.rest().starts_with('"') {
                    self.position += 1;
                    name.push('"');
                } else {
                    return Ok(name);
                }
            } else {
                name.push(ch);
            }
        }
    }
    fn ty(&mut self) -> Result<TypeSignature> {
        let name = self.word()?;
        let mut signature = TypeSignature::simple(name.clone());
        match name.as_str() {
            "array" => {
                self.expect('(')?;
                signature.arguments.push(TypeArgument::Type(self.ty()?));
                self.expect(')')?;
            }
            "map" => {
                self.expect('(')?;
                signature.arguments.push(TypeArgument::Type(self.ty()?));
                self.expect(',')?;
                signature.arguments.push(TypeArgument::Type(self.ty()?));
                self.expect(')')?;
            }
            "row" => {
                self.expect('(')?;
                if !self.consume(')') {
                    loop {
                        self.space();
                        let start = self.position;
                        // An unnamed field is a complete type followed by a delimiter.
                        let unnamed = if !self.rest().starts_with('"') {
                            match self.ty() {
                                Ok(ty) => {
                                    self.space();
                                    if self.rest().starts_with([',', ')']) {
                                        Some(ty)
                                    } else {
                                        None
                                    }
                                }
                                Err(_) => None,
                            }
                        } else {
                            None
                        };
                        let field = if let Some(ty) = unnamed {
                            NamedTypeSignature {
                                field_name: None,
                                type_signature: ty,
                            }
                        } else {
                            self.position = start;
                            let name = self.field_name()?;
                            let before_space = self.position;
                            self.space();
                            if self.position == before_space {
                                bail!("row field name and type require whitespace");
                            }
                            NamedTypeSignature {
                                field_name: Some(RowFieldName { name }),
                                type_signature: self.ty()?,
                            }
                        };
                        signature.arguments.push(TypeArgument::NamedType(field));
                        if self.consume(')') {
                            break;
                        }
                        self.expect(',')?;
                    }
                }
            }
            "decimal" => {
                self.expect('(')?;
                signature.arguments.push(TypeArgument::Long(self.number()?));
                self.expect(',')?;
                signature.arguments.push(TypeArgument::Long(self.number()?));
                self.expect(')')?;
            }
            "char" | "varchar" | "time" | "timestamp" => {
                if self.consume('(') {
                    signature.arguments.push(TypeArgument::Long(self.number()?));
                    self.expect(')')?;
                } else if name == "varchar" {
                    signature.arguments.push(TypeArgument::Long(2_147_483_647));
                }
                if name == "time" || name == "timestamp" {
                    let suffix_start = self.position;
                    self.space();
                    if self.rest().starts_with("with") {
                        for expected in ["with", "time", "zone"] {
                            if self.word()? != expected {
                                bail!("invalid temporal type suffix");
                            }
                        }
                        signature.raw_type.push_str(" with time zone");
                    } else {
                        self.position = suffix_start;
                    }
                }
            }
            "unknown" | "boolean" | "tinyint" | "smallint" | "integer" | "bigint" | "real"
            | "double" | "varbinary" | "date" | "json" | "variant" => {}
            _ => bail!("unsupported Trino type: {name}"),
        }
        signature.validate()?;
        Ok(signature)
    }
}
