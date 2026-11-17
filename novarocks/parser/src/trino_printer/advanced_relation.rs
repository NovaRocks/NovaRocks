// SPDX-License-Identifier: Apache-2.0
//! Typed TVF, PIVOT, and NEAREST syntax on the one shared writer.
use super::*;

impl Writer {
    #[recursive::recursive]
    pub(super) fn advanced_relation(&mut self, value: &Relation) {
        match value {
            Relation::TableFunction(value) => self.table_function_invocation(value),
            Relation::Pivot(value) => self.pivot(value),
            Relation::Nearest(value) => {
                if matches!(value.relation.as_ref(), Relation::Join(join) if join.kind == JoinType::Implicit)
                {
                    unsupported("implicit comma join outside NEAREST relation grammar");
                }
                self.text("NEAREST(FROM ");
                self.relation(&value.relation);
                if let Some(where_clause) = &value.where_clause {
                    self.text(" WHERE ");
                    self.expression(where_clause);
                }
                self.text(" MATCH ");
                self.expression(&value.match_expression);
                self.character(')');
            }
            _ => unreachable!("advanced relation dispatch is a typed exhaustive subset"),
        }
    }

    #[recursive::recursive]
    fn table_function_invocation(&mut self, value: &TableFunctionInvocation) {
        self.text("TABLE(");
        self.name(&value.name);
        self.character('(');
        for (index, argument) in value.arguments.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            if let Some(name) = &argument.name {
                self.identifier(name);
                self.text(" => ");
            }
            match &argument.value {
                TableFunctionArgumentValue::Expression(expression) => {
                    // Parentheses select the expression argument alternative,
                    // even when its real head is DESCRIPTOR or COPARTITION.
                    self.parenthesized_expression(expression);
                }
                TableFunctionArgumentValue::Descriptor(value) => match &value.descriptor {
                    None => self.text("CAST(NULL AS DESCRIPTOR)"),
                    Some(descriptor) => {
                        if descriptor.fields.is_empty() {
                            unsupported("empty TVF descriptor fields");
                        }
                        self.text("DESCRIPTOR(");
                        for (index, field) in descriptor.fields.iter().enumerate() {
                            if index != 0 {
                                self.text(", ");
                            }
                            self.identifier(&field.name);
                            if let Some(data_type) = &field.data_type {
                                self.character(' ');
                                self.data_type(data_type);
                            }
                        }
                        self.character(')');
                    }
                },
                TableFunctionArgumentValue::Table(value) => {
                    self.table_function_table_relation(&value.table);
                    if let Some(partition_by) = &value.partition_by {
                        self.text(" PARTITION BY (");
                        self.expressions(partition_by);
                        self.character(')');
                    }
                    if let Some(treatment) = &value.empty_table_treatment {
                        self.text(match treatment.treatment {
                            EmptyTableTreatmentKind::Keep => " KEEP WHEN EMPTY",
                            EmptyTableTreatmentKind::Prune => " PRUNE WHEN EMPTY",
                        });
                    }
                    if let Some(order_by) = &value.order_by {
                        if order_by.items.is_empty() {
                            unsupported("empty TVF ORDER BY");
                        }
                        self.text(" ORDER BY (");
                        for (index, item) in order_by.items.iter().enumerate() {
                            if index != 0 {
                                self.text(", ");
                            }
                            self.sort_item(item);
                        }
                        self.character(')');
                    }
                }
            }
        }
        if !value.copartitioning.is_empty() {
            // The grammar has no comma between the last real argument and
            // this clause. The no-argument ambiguity retains its own NR ledger.
            self.text(" COPARTITION ");
            for (index, group) in value.copartitioning.iter().enumerate() {
                if group.len() < 2 {
                    unsupported("COPARTITION group with fewer than two real names");
                }
                if index != 0 {
                    self.text(", ");
                }
                self.character('(');
                for (index, name) in group.iter().enumerate() {
                    if index != 0 {
                        self.text(", ");
                    }
                    self.name(name);
                }
                self.character(')');
            }
        }
        self.text("))");
    }

    #[recursive::recursive]
    fn table_function_table_relation(&mut self, relation: &Relation) {
        let (base, alias) = match relation {
            Relation::Aliased(alias) => (alias.relation.as_ref(), Some(alias.as_ref())),
            base => (base, None),
        };
        self.text("TABLE(");
        match base {
            Relation::Table(table) => {
                if table.query_period.is_some() || table.branch.is_some() {
                    unsupported("TVF TABLE(name) carrying query-period/branch facts");
                }
                self.name(&table.name);
            }
            Relation::TableSubquery(table) => self.query(&table.query),
            _ => unsupported("a TVF table argument outside its Table/TableSubquery grammar"),
        }
        self.character(')');
        if let Some(alias) = alias {
            // Explicit AS is permitted even for an unquoted COPARTITION name.
            self.text(" AS ");
            self.identifier(&alias.alias);
            if let Some(names) = &alias.column_names {
                if names.is_empty() {
                    unsupported("Some(empty) TVF table aliases");
                }
                self.text(" (");
                self.identifiers(names);
                self.character(')');
            }
        }
    }

    #[recursive::recursive]
    fn pivot(&mut self, value: &Pivot) {
        if value.aggregations.is_empty()
            || value.pivot_columns.is_empty()
            || value.value_groups.is_empty()
        {
            unsupported("empty required PIVOT list");
        }
        if matches!(value.input.as_ref(), Relation::Join(join) if join.kind == JoinType::Implicit) {
            unsupported("implicit comma join outside PIVOT input grammar");
        }
        // A real input relation may itself be a join, pivot or sampled node.
        // Relation parentheses have no AST node and keep this grammar boundary.
        self.character('(');
        self.relation(&value.input);
        self.text(") PIVOT (");
        for (index, aggregation) in value.aggregations.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.expression(&aggregation.expression);
            self.advanced_alias(aggregation.alias.as_ref());
        }
        self.text(" FOR (");
        for (index, column) in value.pivot_columns.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            if identifier_chain_observed(column, |_| Ok::<_, ()>(()))
                .expect("infallible borrowed inspection")
                .is_none()
            {
                unsupported("computed PIVOT column outside qualifiedName grammar");
            }
            self.pivot_column(column);
        }
        self.text(") IN (");
        for (index, group) in value.value_groups.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            match group.values.as_slice() {
                [] => unsupported("empty PIVOT value group"),
                [value] => self.expression(value),
                values => {
                    self.character('(');
                    self.expressions(values);
                    self.character(')');
                }
            }
            self.advanced_alias(group.alias.as_ref());
        }
        self.character(')');
        if let Some(group_by) = &value.group_by {
            self.character(' ');
            self.group_by(group_by);
        }
        self.character(')');
    }

    fn advanced_alias(&mut self, alias: Option<&Ident>) {
        if let Some(alias) = alias {
            self.text(" AS ");
            self.identifier(alias);
        }
    }

    #[recursive::recursive]
    fn pivot_column(&mut self, column: &Expr) {
        match column {
            Expr::Identifier(value) => self.identifier(value),
            Expr::Dereference(value) => {
                self.pivot_column(&value.base);
                self.character('.');
                self.identifier(
                    value
                        .field
                        .as_ref()
                        .expect("validated qualified-name field"),
                );
            }
            _ => unreachable!("PIVOT's real name chain was checked before rendering"),
        }
    }
}
