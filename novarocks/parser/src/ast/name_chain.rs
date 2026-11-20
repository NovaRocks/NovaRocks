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

//! Borrowed syntax-only inspection of identifier/dereference name chains.
//! Observers retain their own control and authored-source decisions.

use super::{Expr, Ident};

#[derive(Clone, Copy)]
pub struct IdentifierChain<'a> {
    expression: &'a Expr,
    first: &'a Ident,
    len: usize,
}

impl<'a> IdentifierChain<'a> {
    pub fn len(self) -> usize {
        self.len
    }
    pub fn is_empty(self) -> bool {
        false
    }
    pub fn first(self) -> &'a Ident {
        self.first
    }
    pub fn last(self) -> &'a Ident {
        match self.expression {
            Expr::Identifier(ident) => ident,
            Expr::Dereference(node) => node.field.as_ref().expect("validated name field"),
            _ => unreachable!("validated identifier chain"),
        }
    }
    pub fn iter_rev(self) -> impl ExactSizeIterator<Item = &'a Ident> + 'a {
        ReverseIdents {
            cursor: Some(self.expression),
            remaining: self.len,
        }
    }
}

struct ReverseIdents<'a> {
    cursor: Option<&'a Expr>,
    remaining: usize,
}
impl<'a> Iterator for ReverseIdents<'a> {
    type Item = &'a Ident;
    fn next(&mut self) -> Option<Self::Item> {
        let expression = self.cursor.take()?;
        self.remaining -= 1;
        Some(match expression {
            Expr::Identifier(ident) => ident,
            Expr::Dereference(node) => {
                self.cursor = Some(&node.base);
                node.field.as_ref().expect("validated name field")
            }
            _ => unreachable!("validated identifier chain"),
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl ExactSizeIterator for ReverseIdents<'_> {}

/// Inspect each original expression from the outer node toward its base.
/// Only identifiers and dereferences with a named field form a name.
/// No source loan, type, resource or name-resolution policy is inferred here.
pub fn identifier_chain_observed<'a, E>(
    expression: &'a Expr,
    mut observer: impl FnMut(&'a Expr) -> Result<(), E>,
) -> Result<Option<IdentifierChain<'a>>, E> {
    let mut cursor = expression;
    let mut len = 0;
    loop {
        observer(cursor)?;
        len += 1;
        match cursor {
            Expr::Identifier(first) => {
                return Ok(Some(IdentifierChain {
                    expression,
                    first,
                    len,
                }));
            }
            Expr::Dereference(node) if node.field.is_some() => cursor = &node.base,
            _ => return Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Span,
        ast::{AstBox, DereferenceExpr},
        build,
    };

    #[test]
    fn view_borrows_original_identifiers_and_observes_each_real_expression() {
        let source = build::column(
            build::ident_at("table", true, Span::new(1, 8)),
            build::ident_at("column", true, Span::new(9, 17)),
        );
        let mut observed = Vec::new();
        let view = identifier_chain_observed(&source, |node| {
            observed.push(node);
            Ok::<_, ()>(())
        })
        .unwrap()
        .unwrap();
        let Expr::Dereference(node) = &source else {
            unreachable!()
        };
        let Expr::Identifier(first) = &node.base else {
            unreachable!()
        };
        assert_eq!(view.len(), 2);
        assert!(!view.is_empty());
        assert!(std::ptr::eq(view.first(), first));
        assert!(std::ptr::eq(view.last(), node.field.as_ref().unwrap()));
        assert!(std::ptr::eq(observed[0], &source));
        assert!(std::ptr::eq(observed[1], &node.base));
        let values = view
            .iter_rev()
            .map(|id| (id.value.as_str(), id.quoted, id.span))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            [
                ("column", true, Span::new(9, 17)),
                ("table", true, Span::new(1, 8))
            ]
        );
        let mut visits = Vec::new();
        let marker = String::from("original observer refusal");
        let error = identifier_chain_observed(&source, |node| {
            visits.push(node);
            Err::<(), _>(&marker)
        })
        .err()
        .unwrap();
        assert!(std::ptr::eq(error, &marker));
        assert_eq!(visits.len(), 1);
        assert!(std::ptr::eq(visits[0], &source));
    }

    #[test]
    fn nested_computed_and_absent_fields_are_not_flattened_to_names() {
        let plain = build::identifier(build::ident("base", false));
        let missing = Expr::Dereference(AstBox::new(DereferenceExpr {
            base: plain.clone(),
            field: None,
            span: crate::SYNTHETIC,
        }));
        for source in [missing, build::integer(7)] {
            assert!(
                identifier_chain_observed(&source, |_| Ok::<_, ()>(()))
                    .unwrap()
                    .is_none()
            );
        }
    }
}
