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

//! The dependency-light owner for NovaRocks SQL source facts.

pub mod ast;
pub mod build;
pub mod parser;
pub mod printer;

mod error;
mod keyword;
mod lexer;
mod messages;
mod span;
mod token;

pub use ast::{validate_statement, validate_statements};
pub use error::{
    ERROR_CODE_DESCRIPTORS, LexError, ParseError, ParserError, StructuralViolation, ValidateError,
};
pub use keyword::{KeywordClass, class as keyword_class};
pub use lexer::lex;
pub use parser::parse;
pub use span::{LineCol, SYNTHETIC, Span};
pub use token::{Keyword, Symbol, Token, TokenKind, TriviaKind};
