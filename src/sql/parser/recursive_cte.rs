//! Static unrolling of `WITH RECURSIVE` CTEs.
//!
//! NovaRocks does not run a dedicated fix-point operator for recursive CTEs.
//! Instead, every `WITH RECURSIVE cte AS (anchor <UNION> recursive_body)`
//! whose recursive body self-references `cte` is rewritten in the parser
//! into a single non-recursive CTE whose body is a `UNION ALL` of `N`
//! branches — one per unrolled iteration:
//!
//! ```text
//! cte AS (
//!     SELECT * FROM (anchor) __nr_rec_cte_0 (cols...) UNION ALL
//!     SELECT * FROM (recursive_body with `cte` -> derived(anchor))
//!                   __nr_rec_cte_1 (cols...) UNION ALL
//!     SELECT * FROM (recursive_body with `cte` -> derived(prev iteration))
//!                   __nr_rec_cte_2 (cols...) UNION ALL
//!     ...
//! )
//! ```
//!
//! Inlining the iterations as nested derived subqueries (rather than as a
//! chain of named `WITH` entries) deliberately steers clear of NovaRocks's
//! CTE produce/consume codegen path, which produces incorrect slot ids when
//! more than ~8 CTEs reference each other in series. To prevent the
//! analyzer's derived-table column list from being shadowed by inner
//! expression-derived names, every branch's projection is also pinned with
//! explicit `AS <alias>` clauses matching the anchor's columns.
//!
//! The total recursion depth is bounded by `recursive_cte_max_depth` (default
//! `DEFAULT_MAX_DEPTH`). A query can override the bound with the StarRocks
//! optimizer hint `SET_VAR(recursive_cte_max_depth=N)`; that hint is parsed
//! out of the raw SQL text by [`extract_recursive_cte_max_depth`].
//!
//! Bodies that are not a recognised `UNION` chain (e.g. a single `SELECT`,
//! or a mixed `UNION ALL` + `UNION` chain) are left untouched. The
//! self-reference then falls through to the catalog and the analyzer reports
//! an "Unknown table" error — matching the StarRocks behaviour exercised by
//! the regression suite's negative cases.

use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    CaseWhen, Cte, Expr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem, SetExpr,
    SetOperator, SetQuantifier, Statement, TableAlias, TableAliasColumnDef, TableFactor,
    TableWithJoins, With,
};

/// Default upper bound on the number of iterations for `WITH RECURSIVE`
/// unrolling when no `SET_VAR(recursive_cte_max_depth=N)` hint is provided.
pub(crate) const DEFAULT_MAX_DEPTH: usize = 5;

/// Walk a top-level statement and unroll every `WITH RECURSIVE` clause it
/// transitively contains.
pub(crate) fn rewrite_statement(stmt: &mut Statement, max_depth: usize) -> Result<(), String> {
    let max_depth = max_depth.max(1);
    match stmt {
        Statement::Query(q) => rewrite_query(q.as_mut(), max_depth),
        Statement::Insert(insert) => {
            if let Some(q) = insert.source.as_deref_mut() {
                rewrite_query(q, max_depth)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Parse `SET_VAR(recursive_cte_max_depth=N)` out of `/*+ ... */` optimizer
/// hints in the raw SQL text. Returns `None` if no such hint is present.
pub(crate) fn extract_recursive_cte_max_depth(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    let mut single = false;
    let mut double = false;
    let mut backtick = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if single {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single = false;
            }
            idx += 1;
            continue;
        }
        if double {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double = false;
            }
            idx += 1;
            continue;
        }
        if backtick {
            if byte == b'`' {
                backtick = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => single = true,
            b'"' => double = true,
            b'`' => backtick = true,
            b'/' if bytes.get(idx + 1) == Some(&b'*') && bytes.get(idx + 2) == Some(&b'+') => {
                let end = sql[idx + 3..].find("*/").map(|off| idx + 3 + off)?;
                if let Some(depth) = scan_hint_for_max_depth(&sql[idx + 3..end]) {
                    return Some(depth);
                }
                idx = end + 2;
                continue;
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

fn scan_hint_for_max_depth(hint_text: &str) -> Option<usize> {
    let lower = hint_text.to_ascii_lowercase();
    let key = "recursive_cte_max_depth";
    let mut search = 0usize;
    while let Some(rel) = lower[search..].find(key) {
        let key_idx = search + rel;
        let mut cursor = key_idx + key.len();
        while lower
            .as_bytes()
            .get(cursor)
            .is_some_and(u8::is_ascii_whitespace)
        {
            cursor += 1;
        }
        if lower.as_bytes().get(cursor) != Some(&b'=') {
            search = key_idx + key.len();
            continue;
        }
        cursor += 1;
        while lower
            .as_bytes()
            .get(cursor)
            .is_some_and(u8::is_ascii_whitespace)
        {
            cursor += 1;
        }
        let mut end = cursor;
        while lower
            .as_bytes()
            .get(end)
            .is_some_and(|b| b.is_ascii_digit())
        {
            end += 1;
        }
        if end == cursor {
            search = key_idx + key.len();
            continue;
        }
        if let Ok(parsed) = lower[cursor..end].parse::<usize>() {
            return Some(parsed.max(1));
        }
        search = end;
    }
    None
}

fn rewrite_query(query: &mut Query, max_depth: usize) -> Result<(), String> {
    // Process nested queries (CTE bodies, FROM-list subqueries, expression
    // subqueries) before rewriting this scope's own `WITH RECURSIVE`. That way
    // an inner recursive CTE is fully expanded before it appears in any outer
    // unrolling iteration.
    rewrite_nested(query, max_depth)?;

    let needs_unroll = query
        .with
        .as_ref()
        .is_some_and(|w| w.recursive && !w.cte_tables.is_empty());
    if needs_unroll {
        let with = query.with.as_mut().expect("recursive flag checked above");
        unroll_with_clause(with, max_depth)?;
    }
    Ok(())
}

fn rewrite_nested(query: &mut Query, max_depth: usize) -> Result<(), String> {
    if let Some(with) = query.with.as_mut() {
        for cte in &mut with.cte_tables {
            rewrite_query(cte.query.as_mut(), max_depth)?;
        }
    }
    rewrite_set_expr(query.body.as_mut(), max_depth)
}

fn rewrite_set_expr(expr: &mut SetExpr, max_depth: usize) -> Result<(), String> {
    match expr {
        SetExpr::Select(select) => rewrite_select(select, max_depth),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left.as_mut(), max_depth)?;
            rewrite_set_expr(right.as_mut(), max_depth)
        }
        SetExpr::Query(q) => rewrite_query(q.as_mut(), max_depth),
        _ => Ok(()),
    }
}

fn rewrite_select(select: &mut Select, max_depth: usize) -> Result<(), String> {
    for table in &mut select.from {
        rewrite_table_factor(&mut table.relation, max_depth)?;
        for join in &mut table.joins {
            rewrite_table_factor(&mut join.relation, max_depth)?;
        }
    }
    if let Some(sel) = select.selection.as_mut() {
        rewrite_expr(sel, max_depth)?;
    }
    if let Some(having) = select.having.as_mut() {
        rewrite_expr(having, max_depth)?;
    }
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                rewrite_expr(e, max_depth)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn rewrite_table_factor(factor: &mut TableFactor, max_depth: usize) -> Result<(), String> {
    if let TableFactor::Derived { subquery, .. } = factor {
        rewrite_query(subquery.as_mut(), max_depth)?;
    }
    Ok(())
}

fn rewrite_expr(expr: &mut Expr, max_depth: usize) -> Result<(), String> {
    match expr {
        Expr::Subquery(q) => rewrite_query(q.as_mut(), max_depth)?,
        Expr::Exists { subquery, .. } => rewrite_query(subquery.as_mut(), max_depth)?,
        Expr::InSubquery { subquery, expr, .. } => {
            rewrite_query(subquery.as_mut(), max_depth)?;
            rewrite_expr(expr.as_mut(), max_depth)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_expr(left.as_mut(), max_depth)?;
            rewrite_expr(right.as_mut(), max_depth)?;
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            rewrite_expr(expr.as_mut(), max_depth)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_expr(expr.as_mut(), max_depth)?;
            rewrite_expr(low.as_mut(), max_depth)?;
            rewrite_expr(high.as_mut(), max_depth)?;
        }
        Expr::Function(function) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &mut function.args {
                for arg in &mut arg_list.args {
                    if let Some(inner) = function_arg_expr_mut(arg) {
                        rewrite_expr(inner, max_depth)?;
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand.as_deref_mut() {
                rewrite_expr(op, max_depth)?;
            }
            for CaseWhen { condition, result } in conditions {
                rewrite_expr(condition, max_depth)?;
                rewrite_expr(result, max_depth)?;
            }
            if let Some(else_expr) = else_result.as_deref_mut() {
                rewrite_expr(else_expr, max_depth)?;
            }
        }
        Expr::Cast { expr, .. } => rewrite_expr(expr.as_mut(), max_depth)?,
        _ => {}
    }
    Ok(())
}

fn function_arg_expr_mut(arg: &mut sqlparser::ast::FunctionArg) -> Option<&mut Expr> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr};
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        }
        | FunctionArg::ExprNamed {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => Some(e),
        _ => None,
    }
}

fn unroll_with_clause(with: &mut With, max_depth: usize) -> Result<(), String> {
    let originals = std::mem::take(&mut with.cte_tables);
    let mut rewritten: Vec<Cte> = Vec::with_capacity(originals.len() * (max_depth + 1));
    for cte in originals {
        if let Some(unrolled) = try_unroll_cte(&cte, max_depth)? {
            rewritten.extend(unrolled);
        } else {
            rewritten.push(cte);
        }
    }
    with.cte_tables = rewritten;
    Ok(())
}

fn try_unroll_cte(cte: &Cte, max_depth: usize) -> Result<Option<Vec<Cte>>, String> {
    // Bail on shapes we don't recognise (materialised hints, FROM aliases,
    // etc.) — the analyzer will surface the original error untouched.
    if cte.materialized.is_some() || cte.from.is_some() {
        return Ok(None);
    }

    // The body must be a chain of consistent `UNION` operators
    // (`UNION ALL` or `UNION` — possibly more than two operands), with
    // the leftmost operand acting as the anchor and the rest as recursive
    // bodies. Mixed quantifiers (e.g. `UNION ALL` + `UNION DISTINCT`) are
    // not a recognised recursive-CTE shape; leave them alone.
    let Some((chain_quantifier, operands)) = extract_union_chain(cte.query.body.as_ref()) else {
        return Ok(None);
    };
    let (anchor, recursive_parts) = operands
        .split_first()
        .expect("extract_union_chain guarantees >= 2 operands");

    let cte_name = cte.alias.name.value.clone();
    let cte_name_lower = cte_name.to_ascii_lowercase();

    // Anchor must not self-reference — that is an explicit error case in
    // SQL recursive CTEs ("recursive reference in anchor"). Leave the CTE
    // unchanged so the analyzer reports "Unknown table".
    if set_expr_references_table(anchor, &cte_name_lower) {
        return Ok(None);
    }

    // At least one recursive operand must contain a self-reference, else
    // there is no recursion to unroll.
    if !recursive_parts
        .iter()
        .any(|part| set_expr_references_table(part, &cte_name_lower))
    {
        return Ok(None);
    }

    // Column aliases force each iteration's output schema to be identical,
    // so the next iteration's `oh.col` lookup keeps resolving. Without this,
    // expression-derived column names ("oh.col + 1", "CONCAT(...)") drift
    // between iterations and the analyzer can't follow the chain past depth 1.
    let column_aliases = derive_column_aliases(cte, anchor)?;
    let column_alias_defs: Vec<TableAliasColumnDef> = column_aliases
        .iter()
        .map(|ident| TableAliasColumnDef {
            name: ident.clone(),
            data_type: None,
        })
        .collect();

    // Build each iteration's body as a self-contained Query. iteration 0 is
    // the anchor; iteration K substitutes every self-reference in the
    // recursive parts with a derived subquery that *embeds* iteration K-1.
    // The result is a single nested expression — the CTE produce/consume
    // infrastructure is never used for the unrolling itself, which avoids a
    // codegen issue triggered by chains of more than ~8 CTEs that reference
    // each other.
    //
    // Every iteration's outermost SELECT items are also pinned to the chosen
    // column aliases with explicit `AS` clauses. NovaRocks's analyzer does
    // not consistently honour a derived-table column list when an inner
    // SELECT's projection has only expression-derived names (it falls back
    // to looking up by the inner name, e.g. `b + c`, instead of the renamed
    // alias). Forcing explicit aliases inside the body eliminates that path.
    let mut iter_queries: Vec<Query> = Vec::with_capacity(max_depth);
    let mut anchor_body = (*anchor).clone();
    pin_projection_aliases(&mut anchor_body, &column_aliases);
    iter_queries.push(clone_query_with_body(&cte.query, anchor_body));
    for i in 1..max_depth {
        let prev_query = iter_queries[i - 1].clone();
        let renamed_parts: Vec<SetExpr> = recursive_parts
            .iter()
            .map(|part| {
                let mut p = (*part).clone();
                substitute_table_with_derived_in_set_expr(
                    &mut p,
                    &cte_name_lower,
                    &prev_query,
                    &column_alias_defs,
                );
                pin_projection_aliases(&mut p, &column_aliases);
                p
            })
            .collect();
        let body = build_set_op_chain(chain_quantifier, &renamed_parts);
        iter_queries.push(clone_query_with_body(&cte.query, body));
    }

    // Wrap each iteration in `SELECT * FROM (iter_K) iter_K_alias(<cols>)`
    // and UNION ALL them together to form the final CTE body. Using
    // explicit column aliases on the derived table both guarantees the
    // union's schema is consistent and gives the iteration a stable name.
    let wrapper_body =
        build_inline_wrapper_body(&iter_queries, &column_alias_defs, &cte_name_lower);
    let wrapper_query = Query {
        with: None,
        body: Box::new(wrapper_body),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };

    // Preserve the user-facing alias verbatim (including any explicit column
    // list `cte(c1, c2)` they wrote on the original CTE).
    let final_cte = Cte {
        alias: cte.alias.clone(),
        query: Box::new(wrapper_query),
        from: None,
        materialized: None,
        closing_paren_token: cte.closing_paren_token.clone(),
    };

    Ok(Some(vec![final_cte]))
}

fn build_inline_wrapper_body(
    iter_queries: &[Query],
    column_alias_defs: &[TableAliasColumnDef],
    cte_name_lower: &str,
) -> SetExpr {
    let branches: Vec<SetExpr> = iter_queries
        .iter()
        .enumerate()
        .map(|(idx, query)| {
            let alias_name = format!("__nr_rec_{cte_name_lower}_{idx}");
            wrap_query_as_select_star(query.clone(), &alias_name, column_alias_defs)
        })
        .collect();
    build_set_op_chain(SetQuantifier::All, &branches)
}

/// Rewrite each projection item in the outermost `SELECT` (or the top-level
/// `SELECT`s of a union chain) so it carries an explicit `AS <alias>` clause,
/// picking the alias name positionally from `aliases`. Items beyond
/// `aliases.len()` are left untouched. Wildcards are also left alone — they
/// expand to whatever the underlying scope already names correctly.
fn pin_projection_aliases(expr: &mut SetExpr, aliases: &[Ident]) {
    match expr {
        SetExpr::Select(select) => pin_select_projection_aliases(select.as_mut(), aliases),
        SetExpr::SetOperation { left, right, .. } => {
            pin_projection_aliases(left.as_mut(), aliases);
            pin_projection_aliases(right.as_mut(), aliases);
        }
        SetExpr::Query(q) => pin_projection_aliases(q.body.as_mut(), aliases),
        _ => {}
    }
}

fn pin_select_projection_aliases(select: &mut Select, aliases: &[Ident]) {
    for (idx, item) in select.projection.iter_mut().enumerate() {
        let Some(alias) = aliases.get(idx).cloned() else {
            break;
        };
        let placeholder =
            SelectItem::Wildcard(sqlparser::ast::WildcardAdditionalOptions::default());
        let replaced = std::mem::replace(item, placeholder);
        *item = match replaced {
            SelectItem::UnnamedExpr(expr) => SelectItem::ExprWithAlias { expr, alias },
            SelectItem::ExprWithAlias { expr, .. } => SelectItem::ExprWithAlias { expr, alias },
            other => other,
        };
    }
}

fn wrap_query_as_select_star(
    query: Query,
    alias_name: &str,
    column_alias_defs: &[TableAliasColumnDef],
) -> SetExpr {
    let mut select = default_select();
    select.projection = vec![SelectItem::Wildcard(
        sqlparser::ast::WildcardAdditionalOptions::default(),
    )];
    select.from = vec![TableWithJoins {
        relation: TableFactor::Derived {
            lateral: false,
            subquery: Box::new(query),
            alias: Some(TableAlias {
                explicit: false,
                name: Ident::new(alias_name),
                columns: column_alias_defs.to_vec(),
            }),
            sample: None,
        },
        joins: Vec::new(),
    }];
    SetExpr::Select(Box::new(select))
}

/// Walk the left spine of `expr` collecting operands of a consistent
/// `UNION` chain. Returns `None` if the chain length is less than two, if
/// any non-`UNION` set operator is involved, or if the `UNION` operators
/// disagree on their quantifier.
fn extract_union_chain(expr: &SetExpr) -> Option<(SetQuantifier, Vec<&SetExpr>)> {
    fn walk<'a>(
        expr: &'a SetExpr,
        operands: &mut Vec<&'a SetExpr>,
        quantifier: &mut Option<SetQuantifier>,
    ) -> bool {
        if let SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier,
            left,
            right,
        } = expr
        {
            match quantifier {
                None => *quantifier = Some(*set_quantifier),
                Some(existing) if existing == set_quantifier => {}
                Some(_) => return false,
            }
            if !walk(left, operands, quantifier) {
                return false;
            }
            operands.push(right);
            return true;
        }
        operands.push(expr);
        true
    }

    let mut operands: Vec<&SetExpr> = Vec::new();
    let mut quantifier: Option<SetQuantifier> = None;
    if walk(expr, &mut operands, &mut quantifier) && operands.len() >= 2 {
        // `quantifier` is Some because we observed at least one UNION node.
        Some((quantifier.expect("union chain quantifier"), operands))
    } else {
        None
    }
}

/// Build a left-associative `UNION` chain over `parts` using `quantifier`.
fn build_set_op_chain(quantifier: SetQuantifier, parts: &[SetExpr]) -> SetExpr {
    let mut iter = parts.iter();
    let mut combined = iter.next().expect("non-empty parts").clone();
    for part in iter {
        combined = SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: quantifier,
            left: Box::new(combined),
            right: Box::new(part.clone()),
        };
    }
    combined
}

fn derive_column_aliases(cte: &Cte, anchor: &SetExpr) -> Result<Vec<Ident>, String> {
    // Prefer the user-declared column list (e.g. `cte(c1, c2) AS (...)`).
    if !cte.alias.columns.is_empty() {
        return Ok(cte.alias.columns.iter().map(|c| c.name.clone()).collect());
    }
    // Otherwise extract names from the anchor's SELECT projection. We require
    // a `Select` directly under the anchor — other shapes (`VALUES`, nested
    // set ops, ...) are rare for recursive CTEs and not worth special-casing.
    let projection = anchor_projection(anchor)
        .ok_or_else(|| "recursive CTE anchor must be a SELECT statement".to_string())?;
    let mut names: Vec<Ident> = Vec::with_capacity(projection.len());
    for (idx, item) in projection.iter().enumerate() {
        let name = projection_item_name(item).unwrap_or_else(|| format!("__nr_rec_col_{idx}"));
        names.push(Ident::new(name));
    }
    Ok(names)
}

fn anchor_projection(anchor: &SetExpr) -> Option<&[SelectItem]> {
    match anchor {
        SetExpr::Select(select) => Some(&select.projection),
        SetExpr::Query(q) => anchor_projection(q.body.as_ref()),
        _ => None,
    }
}

fn projection_item_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expr) => unnamed_expr_default_name(expr),
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        _ => None,
    }
}

fn unnamed_expr_default_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
        _ => None,
    }
}

fn clone_query_with_body(template: &Query, new_body: SetExpr) -> Query {
    let mut q = template.clone();
    q.body = Box::new(new_body);
    // The unrolled iterations should not carry the original CTE's ORDER BY /
    // LIMIT — those belong to the recursive output as a whole, which is now
    // realized in the final UNION ALL wrapper below. Strip them here.
    q.with = None;
    q.order_by = None;
    q.limit_clause = None;
    q.fetch = None;
    q.locks = Vec::new();
    q.for_clause = None;
    q.settings = None;
    q.format_clause = None;
    q.pipe_operators = Vec::new();
    q
}

fn default_select() -> Select {
    Select {
        select_token: AttachedToken::empty(),
        optimizer_hint: None,
        distinct: None,
        select_modifiers: None,
        top: None,
        top_before_distinct: false,
        projection: Vec::new(),
        exclude: None,
        into: None,
        from: Vec::new(),
        lateral_views: Vec::new(),
        prewhere: None,
        selection: None,
        connect_by: Vec::new(),
        group_by: sqlparser::ast::GroupByExpr::Expressions(Vec::new(), Vec::new()),
        cluster_by: Vec::new(),
        distribute_by: Vec::new(),
        sort_by: Vec::new(),
        having: None,
        named_window: Vec::new(),
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        flavor: sqlparser::ast::SelectFlavor::Standard,
    }
}

// ---------------------------------------------------------------------------
// Self-reference detection and substitution
// ---------------------------------------------------------------------------

fn set_expr_references_table(expr: &SetExpr, target_lower: &str) -> bool {
    match expr {
        SetExpr::Select(select) => select_references_table(select, target_lower),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_references_table(left, target_lower)
                || set_expr_references_table(right, target_lower)
        }
        SetExpr::Query(q) => set_expr_references_table(q.body.as_ref(), target_lower),
        _ => false,
    }
}

fn select_references_table(select: &Select, target_lower: &str) -> bool {
    select.from.iter().any(|t| {
        table_factor_references(&t.relation, target_lower)
            || t.joins
                .iter()
                .any(|j| table_factor_references(&j.relation, target_lower))
    }) || select
        .selection
        .as_ref()
        .is_some_and(|e| expr_references_table(e, target_lower))
        || select
            .having
            .as_ref()
            .is_some_and(|e| expr_references_table(e, target_lower))
        || select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                expr_references_table(e, target_lower)
            }
            _ => false,
        })
}

fn table_factor_references(factor: &TableFactor, target_lower: &str) -> bool {
    match factor {
        TableFactor::Table { name, .. } => object_name_matches(name, target_lower),
        TableFactor::Derived { subquery, .. } => {
            set_expr_references_table(subquery.body.as_ref(), target_lower)
        }
        _ => false,
    }
}

fn expr_references_table(expr: &Expr, target_lower: &str) -> bool {
    match expr {
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            set_expr_references_table(q.body.as_ref(), target_lower)
        }
        Expr::InSubquery { subquery, expr, .. } => {
            set_expr_references_table(subquery.body.as_ref(), target_lower)
                || expr_references_table(expr, target_lower)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_references_table(left, target_lower) || expr_references_table(right, target_lower)
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            expr_references_table(expr, target_lower)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_table(expr, target_lower)
                || expr_references_table(low, target_lower)
                || expr_references_table(high, target_lower)
        }
        Expr::Function(function) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &function.args {
                arg_list.args.iter().any(|arg| {
                    function_arg_expr(arg).is_some_and(|e| expr_references_table(e, target_lower))
                })
            } else {
                false
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|e| expr_references_table(e, target_lower))
                || conditions.iter().any(|cw| {
                    expr_references_table(&cw.condition, target_lower)
                        || expr_references_table(&cw.result, target_lower)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_references_table(e, target_lower))
        }
        Expr::Cast { expr, .. } => expr_references_table(expr, target_lower),
        _ => false,
    }
}

fn function_arg_expr(arg: &sqlparser::ast::FunctionArg) -> Option<&Expr> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr};
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        }
        | FunctionArg::ExprNamed {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => Some(e),
        _ => None,
    }
}

fn object_name_matches(name: &ObjectName, target_lower: &str) -> bool {
    if name.0.len() != 1 {
        return false;
    }
    match &name.0[0] {
        ObjectNamePart::Identifier(ident) => ident.value.eq_ignore_ascii_case(target_lower),
        ObjectNamePart::Function(_) => false,
    }
}

fn substitute_table_with_derived_in_set_expr(
    expr: &mut SetExpr,
    target_lower: &str,
    replacement: &Query,
    column_alias_defs: &[TableAliasColumnDef],
) {
    match expr {
        SetExpr::Select(select) => substitute_in_select(
            select.as_mut(),
            target_lower,
            replacement,
            column_alias_defs,
        ),
        SetExpr::SetOperation { left, right, .. } => {
            substitute_table_with_derived_in_set_expr(
                left.as_mut(),
                target_lower,
                replacement,
                column_alias_defs,
            );
            substitute_table_with_derived_in_set_expr(
                right.as_mut(),
                target_lower,
                replacement,
                column_alias_defs,
            );
        }
        SetExpr::Query(q) => {
            substitute_in_query(q.as_mut(), target_lower, replacement, column_alias_defs)
        }
        _ => {}
    }
}

fn substitute_in_query(
    query: &mut Query,
    target_lower: &str,
    replacement: &Query,
    column_alias_defs: &[TableAliasColumnDef],
) {
    if let Some(with) = query.with.as_mut() {
        let shadowed = with
            .cte_tables
            .iter()
            .any(|c| c.alias.name.value.eq_ignore_ascii_case(target_lower));
        for cte in &mut with.cte_tables {
            substitute_in_query(
                cte.query.as_mut(),
                target_lower,
                replacement,
                column_alias_defs,
            );
        }
        if shadowed {
            // An inner `WITH` rebinds the same name; the outer reference is
            // shadowed there, so leave the rest of this query body alone.
            return;
        }
    }
    substitute_table_with_derived_in_set_expr(
        query.body.as_mut(),
        target_lower,
        replacement,
        column_alias_defs,
    );
}

fn substitute_in_select(
    select: &mut Select,
    target_lower: &str,
    replacement: &Query,
    column_alias_defs: &[TableAliasColumnDef],
) {
    for table in &mut select.from {
        substitute_in_factor(
            &mut table.relation,
            target_lower,
            replacement,
            column_alias_defs,
        );
        for join in &mut table.joins {
            substitute_in_factor(
                &mut join.relation,
                target_lower,
                replacement,
                column_alias_defs,
            );
        }
    }
    if let Some(sel) = select.selection.as_mut() {
        substitute_in_expr(sel, target_lower, replacement, column_alias_defs);
    }
    if let Some(having) = select.having.as_mut() {
        substitute_in_expr(having, target_lower, replacement, column_alias_defs);
    }
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                substitute_in_expr(e, target_lower, replacement, column_alias_defs);
            }
            _ => {}
        }
    }
}

fn substitute_in_factor(
    factor: &mut TableFactor,
    target_lower: &str,
    replacement: &Query,
    column_alias_defs: &[TableAliasColumnDef],
) {
    match factor {
        TableFactor::Table { name, alias, .. } if object_name_matches(name, target_lower) => {
            // Preserve the original join alias (`oh` in `JOIN cte oh ON ...`)
            // if present, otherwise fall back to the CTE name so qualified
            // references inside the recursive body keep resolving.
            let alias_name = alias
                .as_ref()
                .map(|a| a.name.clone())
                .unwrap_or_else(|| Ident::new(target_lower));
            *factor = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(replacement.clone()),
                alias: Some(TableAlias {
                    explicit: false,
                    name: alias_name,
                    columns: column_alias_defs.to_vec(),
                }),
                sample: None,
            };
        }
        TableFactor::Derived { subquery, .. } => {
            substitute_in_query(
                subquery.as_mut(),
                target_lower,
                replacement,
                column_alias_defs,
            );
        }
        _ => {}
    }
}

fn substitute_in_expr(
    expr: &mut Expr,
    target_lower: &str,
    replacement: &Query,
    column_alias_defs: &[TableAliasColumnDef],
) {
    match expr {
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            substitute_in_query(q.as_mut(), target_lower, replacement, column_alias_defs);
        }
        Expr::InSubquery { subquery, expr, .. } => {
            substitute_in_query(
                subquery.as_mut(),
                target_lower,
                replacement,
                column_alias_defs,
            );
            substitute_in_expr(expr.as_mut(), target_lower, replacement, column_alias_defs);
        }
        Expr::BinaryOp { left, right, .. } => {
            substitute_in_expr(left.as_mut(), target_lower, replacement, column_alias_defs);
            substitute_in_expr(right.as_mut(), target_lower, replacement, column_alias_defs);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            substitute_in_expr(expr.as_mut(), target_lower, replacement, column_alias_defs);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            substitute_in_expr(expr.as_mut(), target_lower, replacement, column_alias_defs);
            substitute_in_expr(low.as_mut(), target_lower, replacement, column_alias_defs);
            substitute_in_expr(high.as_mut(), target_lower, replacement, column_alias_defs);
        }
        Expr::Function(function) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &mut function.args {
                for arg in &mut arg_list.args {
                    if let Some(inner) = function_arg_expr_mut(arg) {
                        substitute_in_expr(inner, target_lower, replacement, column_alias_defs);
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand.as_deref_mut() {
                substitute_in_expr(op, target_lower, replacement, column_alias_defs);
            }
            for CaseWhen { condition, result } in conditions {
                substitute_in_expr(condition, target_lower, replacement, column_alias_defs);
                substitute_in_expr(result, target_lower, replacement, column_alias_defs);
            }
            if let Some(else_expr) = else_result.as_deref_mut() {
                substitute_in_expr(else_expr, target_lower, replacement, column_alias_defs);
            }
        }
        Expr::Cast { expr, .. } => {
            substitute_in_expr(expr.as_mut(), target_lower, replacement, column_alias_defs);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::raw::parse_normalized_sql_raw;

    fn parse(sql: &str) -> Statement {
        parse_normalized_sql_raw(sql).expect("parse")
    }

    fn rewrite(sql: &str, depth: usize) -> String {
        let mut stmt = parse(sql);
        rewrite_statement(&mut stmt, depth).expect("rewrite");
        match stmt {
            Statement::Query(q) => q.to_string(),
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn unrolls_basic_recursive_cte_to_fixed_depth() {
        let sql = "WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n WHERE x < 10) \
                   SELECT x FROM n";
        let out = rewrite(sql, 5);
        // The original CTE `n` survives as a single wrapper whose body
        // UNION ALLs five derived branches — one per unrolled iteration.
        for i in 0..5 {
            assert!(
                out.contains(&format!("__nr_rec_n_{i}")),
                "missing __nr_rec_n_{i}: {out}"
            );
        }
        // No iteration CTEs are introduced — the unrolling stays inline.
        assert!(!out.contains("__nr_rec_n_0 AS"), "out: {out}");
        // The self-reference inside the recursive body is rewritten to a
        // derived subquery aliased back to the CTE name `n`, so column
        // references like `oh.col` keep resolving.
        assert!(out.contains(") n ("), "out: {out}");
    }

    #[test]
    fn leaves_non_recursive_cte_alone() {
        let sql = "WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x";
        let out = rewrite(sql, 5);
        assert!(!out.contains("__nr_rec_"), "out: {out}");
        assert!(out.contains("x AS"), "out: {out}");
    }

    #[test]
    fn unrolls_union_distinct_recursive_cte() {
        // A pure `UNION` chain (all DISTINCT) is still a recognisable
        // recursive-CTE shape; only mixed quantifiers are rejected.
        let sql = "WITH RECURSIVE n AS (SELECT 1 UNION SELECT 2 FROM n) SELECT * FROM n";
        let out = rewrite(sql, 5);
        assert!(out.contains("__nr_rec_n_0"), "out: {out}");
        assert!(out.contains("__nr_rec_n_1"), "out: {out}");
        // Multiple recursive operands inside one chain show up as a nested
        // UNION inside each iteration body — we test that separately in
        // `preserves_union_quantifier_across_multiple_recursive_parts`.
    }

    #[test]
    fn substitutes_every_self_reference_independently() {
        // The recursive body cross-joins the CTE with itself under two
        // different aliases. Each self-reference must be replaced by its
        // own derived subquery copy, otherwise iteration K+1 would only see
        // one side of the join.
        let sql = "WITH RECURSIVE cte AS (\
            SELECT 1 AS v \
            UNION ALL \
            SELECT a.v + b.v FROM cte a, cte b) \
            SELECT * FROM cte";
        let out = rewrite(sql, 3);
        // Both join sides preserve their aliases (`a`, `b`) on the rewritten
        // derived subqueries — appearing as `) a (v)` and `) b (v)`.
        assert!(out.contains(") a (v)"), "out: {out}");
        assert!(out.contains(") b (v)"), "out: {out}");
    }

    #[test]
    fn preserves_union_quantifier_across_multiple_recursive_parts() {
        // Three-operand chain `(A UNION B) UNION C`. After rewrite each
        // iteration body re-emits `B' UNION C'` with the same DISTINCT
        // quantifier.
        let sql = "WITH RECURSIVE n AS (\
            SELECT 1 AS v \
            UNION SELECT v + 1 FROM n WHERE v < 5 \
            UNION SELECT v FROM n) \
            SELECT * FROM n";
        let out = rewrite(sql, 3);
        assert!(out.contains("__nr_rec_n_0"), "out: {out}");
        // Inside iteration 1+, the two recursive operands are combined with
        // UNION (DISTINCT) — there is at least one `UNION SELECT` substring.
        assert!(out.contains("UNION SELECT"), "out: {out}");
    }

    #[test]
    fn leaves_mixed_union_quantifier_cte_alone() {
        // `(A UNION ALL B) UNION C` mixes `UNION ALL` and `UNION` — not a
        // recognised recursive shape, so the self-reference stays
        // unresolved and the analyzer can report the original error.
        let sql = "WITH RECURSIVE n AS (\
            SELECT 1 AS v \
            UNION ALL SELECT v + 1 FROM n WHERE v < 5 \
            UNION SELECT v FROM n) \
            SELECT * FROM n";
        let out = rewrite(sql, 5);
        assert!(!out.contains("__nr_rec_"), "out: {out}");
    }

    #[test]
    fn preserves_explicit_column_aliases_on_final_wrapper() {
        let sql = "WITH RECURSIVE fib(n, a, b) AS (\
                       SELECT cast(1 as bigint), cast(0 as bigint), cast(1 as bigint) \
                       UNION ALL \
                       SELECT n + 1, b, a + b FROM fib WHERE n < 10) \
                   SELECT n FROM fib";
        let out = rewrite(sql, 3);
        // Final wrapper keeps the `(n, a, b)` alias list.
        let last = out.rsplit("fib").next().unwrap_or("");
        let _ = last;
        assert!(
            out.contains("fib (n, a, b)") || out.contains("fib(n, a, b)"),
            "out: {out}"
        );
    }

    #[test]
    #[ignore]
    fn debug_dump_fibonacci_rewrite() {
        let sql = "WITH RECURSIVE fibonacci(n, fib_n, fib_n_plus_1) AS (\
            SELECT cast(1 as bigint), cast(0 as bigint), cast(1 as bigint) \
            UNION ALL \
            SELECT n + 1, fib_n_plus_1, fib_n + fib_n_plus_1 \
            FROM fibonacci WHERE n < 10\
        ) SELECT n, fib_n FROM fibonacci ORDER BY n";
        let stmt = parse_normalized_sql_raw(sql).expect("parse");
        if let Statement::Query(q) = stmt {
            eprintln!(">>>>> REWRITTEN:\n{q}");
        }
        panic!("(dump)");
    }

    #[test]
    #[ignore]
    fn debug_dump_union_chain_structure() {
        let stmt =
            parse_normalized_sql_raw("SELECT 1 UNION SELECT 2 UNION SELECT 3").expect("parse");
        eprintln!("{stmt:#?}");
        panic!("(dump)");
    }

    #[test]
    #[ignore]
    fn debug_dump_org_hierarchy_rewrite() {
        let sql = "WITH RECURSIVE org_hierarchy AS (\
            SELECT employee_id, name, manager_id, title, cast(1 as bigint) AS `level`, name AS path \
            FROM employees \
            WHERE manager_id IS NULL \
            UNION ALL \
            SELECT e.employee_id, e.name, e.manager_id, e.title, oh.`level` + 1, CONCAT(oh.path, ' -> ', e.name) \
            FROM employees e \
            INNER JOIN org_hierarchy oh ON e.manager_id = oh.employee_id \
        ) \
        SELECT employee_id, name, title, `level`, path FROM org_hierarchy ORDER BY employee_id";
        let out = rewrite(sql, 3);
        eprintln!(">>>>> REWRITTEN:\n{out}");
        panic!("(dump)");
    }

    #[test]
    fn extracts_max_depth_hint() {
        assert_eq!(
            extract_recursive_cte_max_depth(
                "SELECT /*+ SET_VAR(recursive_cte_max_depth = 10) */ 1"
            ),
            Some(10)
        );
        assert_eq!(
            extract_recursive_cte_max_depth(
                "SELECT /*+ SET_VAR(enable_recursive_cte=true, recursive_cte_max_depth=7)*/ 1"
            ),
            Some(7)
        );
        assert_eq!(extract_recursive_cte_max_depth("SELECT 1"), None);
    }
}
