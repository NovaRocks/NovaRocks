// SPDX-License-Identifier: Apache-2.0
use super::*;

impl Writer {
    #[recursive::recursive]
    pub(super) fn query(&mut self, value: &Query) {
        if matches!(value.body.as_ref(), SetExpr::Select(_)) && !value.modifiers.is_empty() {
            unsupported("query-level modifiers on a direct Select body");
        }
        if !value.session_properties.is_empty() {
            self.text("WITH SESSION ");
            for (index, property) in value.session_properties.iter().enumerate() {
                if index != 0 {
                    self.text(", ");
                }
                self.name(&property.name);
                self.text(" = ");
                self.expression(&property.value);
            }
            self.character(' ');
        }
        if !value.functions.is_empty() {
            self.text("WITH ");
            for (index, function) in value.functions.iter().enumerate() {
                if index != 0 {
                    self.text(", ");
                }
                self.function_specification(function);
            }
            self.character(' ');
        }
        if let Some(with) = &value.with {
            self.text("WITH ");
            if with.recursive {
                self.text("RECURSIVE ");
            }
            for (index, cte) in with.ctes.iter().enumerate() {
                if index != 0 {
                    self.text(", ");
                }
                self.identifier(&cte.name);
                if let Some(columns) = &cte.columns {
                    if columns.is_empty() {
                        unsupported("Some(empty) CTE column aliases");
                    }
                    self.text(" (");
                    self.identifiers(columns);
                    self.character(')');
                }
                self.text(" AS (");
                self.query(&cte.query);
                self.character(')');
            }
            self.character(' ');
        }
        self.set_expression(&value.body);
        self.modifiers(&value.modifiers);
    }

    #[recursive::recursive]
    fn set_expression(&mut self, value: &SetExpr) {
        match value {
            SetExpr::Select(value) => self.select(value),
            SetExpr::Values(value) => {
                self.text("VALUES ");
                // Do not flatten a Row or synthesize a singleton Row. Both
                // forms are real and distinct AST facts in the new grammar.
                self.expressions(&value.rows);
            }
            SetExpr::Table(value) => {
                self.text("TABLE ");
                self.table(value);
            }
            SetExpr::Query(value) => {
                self.character('(');
                self.query(&value.query);
                self.character(')');
            }
            SetExpr::SetOperation(value) => {
                if value.inputs.len() < 2 {
                    unsupported("a set operation with fewer than two inputs");
                }
                if value.inputs.len() > 2
                    && !(value.operator == SetOperator::Union
                        && !value.distinct
                        && value.corresponding.is_none())
                {
                    unsupported("a non-UNION-ALL n-ary set shape outside the parsed first slice");
                }
                for (index, input) in value.inputs.iter().enumerate() {
                    if index != 0 {
                        self.text(match value.operator {
                            SetOperator::Union => " UNION ",
                            SetOperator::Intersect => " INTERSECT ",
                            SetOperator::Except => " EXCEPT ",
                        });
                        self.text(if value.distinct { "DISTINCT" } else { "ALL" });
                        if let Some(corresponding) = &value.corresponding {
                            self.text(" CORRESPONDING");
                            if !corresponding.columns.is_empty() {
                                self.text(" BY (");
                                self.identifiers(&corresponding.columns);
                                self.character(')');
                            }
                        }
                        self.character(' ');
                    }
                    // Only an actual SetExpr::Query prints a query-parenthesis
                    // node. Adding wrappers here would change strict AST facts.
                    self.set_expression(input);
                }
            }
        }
    }

    #[recursive::recursive]
    fn select(&mut self, value: &Select) {
        if value.legacy.qualify.is_some() {
            legacy("QUALIFY");
        }
        self.text("SELECT ");
        if value.distinct {
            self.text("DISTINCT ");
        }
        for (index, item) in value.projection.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            match item {
                SelectItem::SingleColumn(value) => {
                    self.expression(&value.expression);
                    if let Some(alias) = &value.alias {
                        self.text(" AS ");
                        self.identifier(alias);
                    }
                }
                SelectItem::AllColumns(value) => {
                    if value.target.is_none() && !value.aliases.is_empty() {
                        unsupported("aliases on an untargeted wildcard");
                    }
                    if let Some(target) = &value.target {
                        self.parenthesized_expression(target);
                        self.character('.');
                    }
                    self.character('*');
                    if !value.aliases.is_empty() {
                        self.text(" AS (");
                        self.identifiers(&value.aliases);
                        self.character(')');
                    }
                }
            }
        }
        if let Some(from) = &value.from {
            self.text(" FROM ");
            self.relation(from);
        }
        if let Some(selection) = &value.selection {
            self.text(" WHERE ");
            self.expression(selection);
        }
        if let Some(group_by) = &value.group_by {
            self.character(' ');
            self.group_by(group_by);
        }
        if let Some(having) = &value.having {
            self.text(" HAVING ");
            self.expression(having);
        }
        if !value.windows.is_empty() {
            self.text(" WINDOW ");
            for (index, definition) in value.windows.iter().enumerate() {
                if index != 0 {
                    self.text(", ");
                }
                self.identifier(&definition.name);
                self.text(" AS ");
                self.window_specification(&definition.specification);
            }
        }
        self.modifiers(&value.modifiers);
    }
    #[recursive::recursive]
    pub(super) fn group_by(&mut self, group_by: &GroupBy) {
        self.text("GROUP BY ");
        if group_by.distinct {
            self.text("DISTINCT ");
        }
        for (index, element) in group_by.elements.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            match element {
                GroupingElement::Simple(value) => self.grouping_set(&value.columns),
                GroupingElement::Sets(value) => {
                    self.text(match value.kind {
                        GroupingSetsKind::Explicit => "GROUPING SETS (",
                        GroupingSetsKind::Rollup => "ROLLUP (",
                        GroupingSetsKind::Cube => "CUBE (",
                    });
                    for (index, set) in value.sets.iter().enumerate() {
                        if index != 0 {
                            self.text(", ");
                        }
                        self.grouping_set(set);
                    }
                    self.character(')');
                }
                GroupingElement::Auto(_) => self.text("AUTO"),
            }
        }
    }

    fn grouping_set(&mut self, expressions: &[Expr]) {
        self.character('(');
        self.expressions(expressions);
        self.character(')');
    }

    #[recursive::recursive]
    fn modifiers(&mut self, value: &QueryModifiers) {
        if let Some(order_by) = &value.order_by {
            self.character(' ');
            self.order_by(order_by);
        }
        if let Some(offset) = &value.offset {
            self.text(" OFFSET ");
            self.expression(&offset.row_count);
            self.text(" ROWS");
        }
        if let Some(limit) = &value.limit {
            match limit {
                QueryLimit::Limit(value) => {
                    self.text(" LIMIT ");
                    match &value.row_count {
                        Expr::AllRows(_) => self.text("ALL"),
                        other => self.expression(other),
                    }
                }
                QueryLimit::FetchFirst(value) => {
                    self.text(" FETCH FIRST");
                    if let Some(count) = &value.row_count {
                        self.character(' ');
                        self.expression(count);
                    }
                    self.text(if value.with_ties {
                        " ROWS WITH TIES"
                    } else {
                        " ROWS ONLY"
                    });
                }
            }
        }
    }
    #[recursive::recursive]
    pub(super) fn order_by(&mut self, value: &OrderBy) {
        if value.items.is_empty() {
            unsupported("Some(empty) ORDER BY");
        }
        self.text("ORDER BY ");
        for (index, item) in value.items.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.sort_item(item);
        }
    }

    #[recursive::recursive]
    pub(super) fn sort_item(&mut self, item: &SortItem) {
        self.expression(&item.sort_key);
        self.text(match item.ordering {
            SortOrdering::Ascending => " ASC",
            SortOrdering::Descending => " DESC",
        });
        match item.null_ordering {
            NullOrdering::Undefined => {}
            NullOrdering::First => self.text(" NULLS FIRST"),
            NullOrdering::Last => self.text(" NULLS LAST"),
        }
    }

    #[recursive::recursive]
    fn table(&mut self, value: &Table) {
        if value.branch.is_some() {
            unsupported("a SELECT Table branch fact without pinned query syntax");
        }
        self.name(&value.name);
        if let Some(period) = &value.query_period {
            if period.start.is_some() {
                unsupported("a QueryPeriod start endpoint without pinned syntax");
            }
            self.text(match period.range_type {
                QueryPeriodRangeType::Timestamp => " FOR TIMESTAMP AS OF ",
                QueryPeriodRangeType::Version => " FOR VERSION AS OF ",
            });
            match &period.end {
                Some(end) => self.expression(end),
                None => unsupported("a QueryPeriod without its ending marker"),
            }
        }
    }
    #[recursive::recursive]
    pub(super) fn relation(&mut self, value: &Relation) {
        match value {
            Relation::Table(value) => self.table(value),
            Relation::TableSubquery(value) => {
                self.character('(');
                self.query(&value.query);
                self.character(')');
            }
            Relation::Lateral(value) => {
                self.text("LATERAL (");
                self.query(&value.query);
                self.character(')');
            }
            Relation::Aliased(value) => {
                match value.relation.as_ref() {
                    Relation::Join(join) if join.kind == JoinType::Implicit => {
                        unsupported("an alias around an implicit comma join");
                    }
                    Relation::Aliased(_) | Relation::Sampled(_) => {
                        self.character('(');
                        self.relation(&value.relation);
                        self.character(')');
                    }
                    _ => self.relation(&value.relation),
                }
                self.text(" AS ");
                self.identifier(&value.alias);
                if let Some(names) = &value.column_names {
                    if names.is_empty() {
                        unsupported("Some(empty) relation aliases");
                    }
                    self.text(" (");
                    self.identifiers(names);
                    self.character(')');
                }
            }
            Relation::Join(value) => {
                if matches!(value.kind, JoinType::Legacy(_)) {
                    legacy("join type");
                }
                if value.kind == JoinType::Implicit {
                    if value.criteria.is_some() {
                        unsupported("criteria on an implicit join");
                    }
                    self.relation(&value.left);
                    self.text(", ");
                    self.relation(&value.right);
                    return;
                }
                self.character('(');
                self.relation(&value.left);
                let natural = matches!(&value.criteria, Some(JoinCriteria::Natural(_)));
                self.text(if natural { " NATURAL " } else { " " });
                self.text(match value.kind {
                    JoinType::Cross => "CROSS JOIN ",
                    JoinType::Inner => "INNER JOIN ",
                    JoinType::Left => "LEFT JOIN ",
                    JoinType::Right => "RIGHT JOIN ",
                    JoinType::Full => "FULL JOIN ",
                    JoinType::Implicit | JoinType::Legacy(_) => unreachable!("handled join kind"),
                });
                self.relation(&value.right);
                if let Some(criteria) = &value.criteria {
                    match criteria {
                        JoinCriteria::On(value) => {
                            self.text(" ON ");
                            self.expression(&value.expression);
                        }
                        JoinCriteria::Using(value) => {
                            self.text(" USING (");
                            self.identifiers(&value.columns);
                            self.character(')');
                        }
                        JoinCriteria::Natural(_) => {}
                    }
                } else if value.kind != JoinType::Cross {
                    unsupported("an explicit non-CROSS join without criteria");
                }
                self.character(')');
            }
            Relation::Unnest(value) => {
                self.text("UNNEST(");
                self.expressions(&value.expressions);
                self.character(')');
                if value.with_ordinality {
                    self.text(" WITH ORDINALITY");
                }
            }
            Relation::Sampled(value) => {
                if matches!(value.relation.as_ref(), Relation::Join(join) if join.kind == JoinType::Implicit)
                {
                    unsupported("a TABLESAMPLE around an implicit comma join");
                }
                self.character('(');
                self.relation(&value.relation);
                self.character(')');
                self.text(match value.kind {
                    SamplingType::Bernoulli => " TABLESAMPLE BERNOULLI (",
                    SamplingType::System => " TABLESAMPLE SYSTEM (",
                });
                self.expression(&value.sample_percentage);
                self.character(')');
            }
            Relation::LegacyTableFunction(_) => legacy("table function"),
            Relation::JsonTable(value) => self.json_table(value),
            Relation::PatternRecognition(value) => self.pattern_recognition(value),
            Relation::Pivot(_) | Relation::Nearest(_) | Relation::TableFunction(_) => {
                self.advanced_relation(value)
            }
        }
    }

    #[recursive::recursive]
    pub(super) fn window(&mut self, value: &Window) {
        match value {
            Window::Reference(value) => self.identifier(&value.name),
            Window::Specification(value) => self.window_specification(value),
        }
    }
    #[recursive::recursive]
    fn window_specification(&mut self, value: &WindowSpecification) {
        self.character('(');
        let mut separator = false;
        if let Some(name) = &value.existing_window_name {
            self.identifier(name);
            separator = true;
        }
        if !value.partition_by.is_empty() {
            if separator {
                self.character(' ');
            }
            self.text("PARTITION BY ");
            self.expressions(&value.partition_by);
            separator = true;
        }
        if let Some(order_by) = &value.order_by {
            if separator {
                self.character(' ');
            }
            self.order_by(order_by);
            separator = true;
        }
        if let Some(frame) = &value.window_frame {
            if separator {
                self.character(' ');
            }
            self.window_frame(frame);
        }
        self.character(')');
    }
    #[recursive::recursive]
    fn window_frame(&mut self, value: &WindowFrame) {
        self.pattern_measures(&value.measures);
        self.text(match value.units {
            WindowFrameUnits::Rows => "ROWS ",
            WindowFrameUnits::Range => "RANGE ",
            WindowFrameUnits::Groups => "GROUPS ",
        });
        if let Some(end) = &value.end_bound {
            self.text("BETWEEN ");
            self.window_bound(&value.start_bound);
            self.text(" AND ");
            self.window_bound(end);
        } else {
            self.window_bound(&value.start_bound);
        }
        match value.exclusion {
            WindowFrameExclusion::NoOthers => {}
            WindowFrameExclusion::CurrentRow => self.text(" EXCLUDE CURRENT ROW"),
            WindowFrameExclusion::Group => self.text(" EXCLUDE GROUP"),
            WindowFrameExclusion::Ties => self.text(" EXCLUDE TIES"),
        }
        self.window_pattern_tail(value);
    }
    #[recursive::recursive]
    fn window_bound(&mut self, value: &WindowFrameBound) {
        match value {
            WindowFrameBound::CurrentRow(_) => self.text("CURRENT ROW"),
            WindowFrameBound::Preceding(value, _) | WindowFrameBound::Following(value, _) => {
                match value {
                    Some(value) => self.expression(value),
                    None => self.text("UNBOUNDED"),
                }
            }
        }
        match value {
            WindowFrameBound::Preceding(_, _) => self.text(" PRECEDING"),
            WindowFrameBound::Following(_, _) => self.text(" FOLLOWING"),
            WindowFrameBound::CurrentRow(_) => {}
        }
    }
}
