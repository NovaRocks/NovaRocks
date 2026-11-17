// SPDX-License-Identifier: Apache-2.0
//! Every completed query/expression grammar witness keeps its full typed AST.
use novarocks_parser::ast::SyntaxEq;
use novarocks_parser::{trino_parser, trino_printer};

fn check(rule: &str, source: &str) -> Result<(), String> {
    macro_rules! roundtrip {
        ($parse:path, $print:path) => {{
            let original = $parse(source).map_err(|error| format!("initial parse: {error:?}"))?;
            let printed = $print(&original).map_err(|error| format!("render: {error:?}"))?;
            let reparsed = $parse(&printed)
                .map_err(|error| format!("reparse {printed:?}: {error:?}"))?;
            if original.syntax_eq(&reparsed) {
                Ok(())
            } else {
                Err(format!("strict AST mismatch: {printed:?}\noriginal: {original:?}\nreparsed: {reparsed:?}"))
            }
        }};
    }
    match rule {
        "standaloneExpression" => roundtrip!(
            trino_parser::parse_expression,
            trino_printer::print_expression
        ),
        "standaloneType" => roundtrip!(trino_parser::parse_type, trino_printer::print_type),
        "standaloneRowPattern" => roundtrip!(
            trino_parser::parse_row_pattern,
            trino_printer::print_row_pattern
        ),
        "standaloneFunctionSpecification" => roundtrip!(
            trino_parser::parse_function_specification,
            trino_printer::print_function_specification
        ),
        _ => roundtrip!(trino_parser::parse_query, trino_printer::print_query),
    }
}

#[test]
fn completed_grammar_samples_strictly_roundtrip_without_losing_typed_facts() {
    let mut failures = Vec::new();
    for line in include_str!("grammar-coverage.tsv")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 10, "{line}");
        if !matches!(fields[0], "alternative" | "optional")
            || fields[6] == "pending-p09"
            || fields[7] != "accept"
        {
            continue;
        }
        let source = super::coverage::sample(fields[5]);
        if let Err(error) = check(fields[1], &source) {
            failures.push(format!("{line}\nsource: {source:?}\n{error}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
