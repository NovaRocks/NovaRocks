use crate::sql::ast::{
    ColumnRef, CompareOp, CreateCatalogStmt, CreateDatabaseStmt, CreateTableKind, CreateTableStmt,
    DropCatalogStmt, DropDatabaseStmt, DropTableStmt, Expr, InsertSource, InsertStmt, Literal,
    ObjectName, OrderByExpr, ProjectionItem, QueryStmt, SqlType, Statement, TableColumnDef,
    TableRef,
};
use crate::sql::parser::SqlParserBackend;

#[derive(Default)]
pub(crate) struct ManualParser;

impl SqlParserBackend for ManualParser {
    fn parse(&self, sql: &str) -> Result<Statement, String> {
        let tokens = Lexer::new(sql).tokenize()?;
        Parser::new(tokens).parse_statement()
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Token {
    kind: TokenKind,
    start: usize,
    end: usize,
}

#[derive(Clone, Debug, PartialEq)]
enum TokenKind {
    Select,
    From,
    Where,
    Order,
    By,
    Asc,
    Desc,
    Create,
    Drop,
    Database,
    Table,
    Properties,
    Catalog,
    External,
    If,
    Not,
    Exists,
    Force,
    Insert,
    Into,
    Values,
    Date,
    True,
    False,
    Null,
    TinyIntType,
    SmallIntType,
    IntType,
    BigIntType,
    FloatType,
    DoubleType,
    StringType,
    BooleanType,
    DateTimeType,
    TimeType,
    Identifier(String),
    Integer(i64),
    Float(f64),
    String(String),
    Star,
    Comma,
    Dot,
    LParen,
    RParen,
    Semicolon,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Eof,
}

struct Lexer<'a> {
    sql: &'a str,
    chars: Vec<(usize, char)>,
    idx: usize,
}

impl<'a> Lexer<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            chars: sql.char_indices().collect(),
            idx: 0,
        }
    }

    fn tokenize(mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();
        while let Some((start, ch)) = self.peek() {
            if ch.is_ascii_whitespace() {
                self.idx += 1;
                continue;
            }
            let token = match ch {
                '*' => self.single_char(TokenKind::Star),
                ',' => self.single_char(TokenKind::Comma),
                '.' => self.single_char(TokenKind::Dot),
                '(' => self.single_char(TokenKind::LParen),
                ')' => self.single_char(TokenKind::RParen),
                ';' => self.single_char(TokenKind::Semicolon),
                '=' => self.single_char(TokenKind::Eq),
                '!' => self.lex_bang_operator()?,
                '<' => self.lex_lt_operator(),
                '>' => self.lex_gt_operator(),
                '\'' | '"' => self.lex_string(ch)?,
                '`' => self.lex_quoted_identifier()?,
                ch if is_identifier_start(ch) => self.lex_identifier_or_keyword(),
                ch if ch.is_ascii_digit() => self.lex_number()?,
                _ => {
                    return Err(format!(
                        "unexpected character `{}` at position {}",
                        ch, start
                    ));
                }
            };
            tokens.push(token);
        }
        let eof_pos = self.sql.len();
        tokens.push(Token {
            kind: TokenKind::Eof,
            start: eof_pos,
            end: eof_pos,
        });
        Ok(tokens)
    }

    fn peek(&self) -> Option<(usize, char)> {
        self.chars.get(self.idx).copied()
    }

    fn peek_next(&self) -> Option<(usize, char)> {
        self.chars.get(self.idx + 1).copied()
    }

    fn single_char(&mut self, kind: TokenKind) -> Token {
        let (start, ch) = self.peek().expect("single char token");
        self.idx += 1;
        Token {
            kind,
            start,
            end: start + ch.len_utf8(),
        }
    }

    fn lex_bang_operator(&mut self) -> Result<Token, String> {
        let (start, _) = self.peek().expect("bang operator");
        let Some((_, '=')) = self.peek_next() else {
            return Err(format!("unexpected character `!` at position {}", start));
        };
        self.idx += 2;
        Ok(Token {
            kind: TokenKind::Ne,
            start,
            end: start + 2,
        })
    }

    fn lex_lt_operator(&mut self) -> Token {
        let (start, _) = self.peek().expect("lt operator");
        if matches!(self.peek_next(), Some((_, '='))) {
            self.idx += 2;
            Token {
                kind: TokenKind::Le,
                start,
                end: start + 2,
            }
        } else {
            self.idx += 1;
            Token {
                kind: TokenKind::Lt,
                start,
                end: start + 1,
            }
        }
    }

    fn lex_gt_operator(&mut self) -> Token {
        let (start, _) = self.peek().expect("gt operator");
        if matches!(self.peek_next(), Some((_, '='))) {
            self.idx += 2;
            Token {
                kind: TokenKind::Ge,
                start,
                end: start + 2,
            }
        } else {
            self.idx += 1;
            Token {
                kind: TokenKind::Gt,
                start,
                end: start + 1,
            }
        }
    }

    fn lex_string(&mut self, quote: char) -> Result<Token, String> {
        let (start, _) = self.peek().expect("string literal");
        self.idx += 1;
        let mut value = String::new();
        while let Some((_, ch)) = self.peek() {
            self.idx += 1;
            if ch == quote {
                if matches!(self.peek(), Some((_, next)) if next == quote) {
                    value.push(quote);
                    self.idx += 1;
                    continue;
                }
                let end = self
                    .chars
                    .get(self.idx - 1)
                    .map(|(offset, ch)| offset + ch.len_utf8())
                    .unwrap_or(start + 1);
                return Ok(Token {
                    kind: TokenKind::String(value),
                    start,
                    end,
                });
            }
            value.push(ch);
        }
        Err(format!("unterminated string literal starting at {}", start))
    }

    fn lex_quoted_identifier(&mut self) -> Result<Token, String> {
        let (start, _) = self.peek().expect("quoted identifier");
        self.idx += 1;
        let mut value = String::new();
        while let Some((_, ch)) = self.peek() {
            self.idx += 1;
            if ch == '`' {
                if matches!(self.peek(), Some((_, '`'))) {
                    value.push('`');
                    self.idx += 1;
                    continue;
                }
                let end = self
                    .chars
                    .get(self.idx - 1)
                    .map(|(offset, ch)| offset + ch.len_utf8())
                    .unwrap_or(start + 1);
                return Ok(Token {
                    kind: TokenKind::Identifier(value),
                    start,
                    end,
                });
            }
            value.push(ch);
        }
        Err(format!(
            "unterminated quoted identifier starting at {}",
            start
        ))
    }

    fn lex_identifier_or_keyword(&mut self) -> Token {
        let (start, first) = self.peek().expect("identifier");
        self.idx += 1;
        let mut end = start + first.len_utf8();
        while let Some((offset, ch)) = self.peek() {
            if !is_identifier_continue(ch) {
                break;
            }
            self.idx += 1;
            end = offset + ch.len_utf8();
        }
        let text = &self.sql[start..end];
        let upper = text.to_ascii_uppercase();
        let kind = match upper.as_str() {
            "SELECT" => TokenKind::Select,
            "FROM" => TokenKind::From,
            "WHERE" => TokenKind::Where,
            "ORDER" => TokenKind::Order,
            "BY" => TokenKind::By,
            "ASC" => TokenKind::Asc,
            "DESC" => TokenKind::Desc,
            "CREATE" => TokenKind::Create,
            "DROP" => TokenKind::Drop,
            "DATABASE" => TokenKind::Database,
            "TABLE" => TokenKind::Table,
            "PROPERTIES" => TokenKind::Properties,
            "CATALOG" => TokenKind::Catalog,
            "EXTERNAL" => TokenKind::External,
            "IF" => TokenKind::If,
            "NOT" => TokenKind::Not,
            "EXISTS" => TokenKind::Exists,
            "FORCE" => TokenKind::Force,
            "INSERT" => TokenKind::Insert,
            "INTO" => TokenKind::Into,
            "VALUES" => TokenKind::Values,
            "DATE" => TokenKind::Date,
            "TRUE" => TokenKind::True,
            "FALSE" => TokenKind::False,
            "NULL" => TokenKind::Null,
            "TINYINT" => TokenKind::TinyIntType,
            "SMALLINT" => TokenKind::SmallIntType,
            "INT" => TokenKind::IntType,
            "BIGINT" => TokenKind::BigIntType,
            "FLOAT" => TokenKind::FloatType,
            "DOUBLE" => TokenKind::DoubleType,
            "STRING" | "VARCHAR" => TokenKind::StringType,
            "BOOLEAN" | "BOOL" => TokenKind::BooleanType,
            "DATETIME" | "TIMESTAMP" => TokenKind::DateTimeType,
            "TIME" => TokenKind::TimeType,
            _ => TokenKind::Identifier(text.to_string()),
        };
        Token { kind, start, end }
    }

    fn lex_number(&mut self) -> Result<Token, String> {
        let (start, first) = self.peek().expect("number");
        self.idx += 1;
        let mut end = start + first.len_utf8();
        let mut seen_dot = false;
        while let Some((offset, ch)) = self.peek() {
            if ch.is_ascii_digit() {
                self.idx += 1;
                end = offset + ch.len_utf8();
                continue;
            }
            if ch == '.'
                && !seen_dot
                && matches!(self.peek_next(), Some((_, next)) if next.is_ascii_digit())
            {
                seen_dot = true;
                self.idx += 1;
                end = offset + ch.len_utf8();
                continue;
            }
            break;
        }
        let text = &self.sql[start..end];
        if seen_dot {
            let value = text
                .parse::<f64>()
                .map_err(|_| format!("invalid float literal `{}` at position {}", text, start))?;
            Ok(Token {
                kind: TokenKind::Float(value),
                start,
                end,
            })
        } else {
            let value = text
                .parse::<i64>()
                .map_err(|_| format!("invalid integer literal `{}` at position {}", text, start))?;
            Ok(Token {
                kind: TokenKind::Integer(value),
                start,
                end,
            })
        }
    }
}

struct Parser {
    tokens: Vec<Token>,
    idx: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, idx: 0 }
    }

    fn parse_statement(mut self) -> Result<Statement, String> {
        if self.at_eof() {
            return Err("SQL is empty".to_string());
        }
        let stmt = match self.peek().kind {
            TokenKind::Select => self.parse_select_statement()?,
            TokenKind::Create => self.parse_create_statement()?,
            TokenKind::Drop => self.parse_drop_statement()?,
            TokenKind::Insert => self.parse_insert_statement()?,
            _ => {
                return Err(self.error_at(
                    self.peek(),
                    "expected `SELECT`, `CREATE`, `DROP`, or `INSERT` at start of statement",
                ));
            }
        };
        if self.match_semicolon() && !self.at_eof() {
            return Err(self.unsupported_syntax_error(self.peek()));
        }
        if !self.at_eof() {
            return Err(self.unsupported_syntax_error(self.peek()));
        }
        Ok(stmt)
    }

    fn parse_select_statement(&mut self) -> Result<Statement, String> {
        self.expect_keyword(TokenKind::Select, "expected `SELECT` at start of query")?;
        let projection = self.parse_projection_list()?;
        self.expect_keyword(TokenKind::From, "expected `FROM` after projection list")?;
        let from = TableRef {
            name: self.parse_object_name("expected table name after `FROM`")?,
        };
        let selection = if self.match_keyword(TokenKind::Where) {
            Some(self.parse_where_clause()?)
        } else {
            None
        };
        let order_by = if self.match_keyword(TokenKind::Order) {
            self.expect_keyword(TokenKind::By, "expected `BY` after `ORDER`")?;
            self.parse_order_by_clause()?
        } else {
            Vec::new()
        };
        Ok(Statement::Query(QueryStmt {
            projection,
            from,
            selection,
            order_by,
        }))
    }

    fn parse_create_statement(&mut self) -> Result<Statement, String> {
        self.expect_keyword(TokenKind::Create, "expected `CREATE` at start of statement")?;
        if self.match_keyword(TokenKind::External) {
            self.expect_keyword(
                TokenKind::Catalog,
                "expected `CATALOG` after `CREATE EXTERNAL`",
            )?;
            return self.parse_create_catalog_statement();
        }
        if self.match_keyword(TokenKind::Catalog) {
            return self.parse_create_catalog_statement();
        }
        if self.match_keyword(TokenKind::Database) {
            return Ok(Statement::CreateDatabase(CreateDatabaseStmt {
                name: self.parse_object_name("expected database name after `CREATE DATABASE`")?,
            }));
        }
        if self.match_keyword(TokenKind::Table) {
            return self.parse_create_table_statement();
        }
        Err(self.error_at(
            self.peek(),
            "expected `CATALOG`, `DATABASE`, or `TABLE` after `CREATE`",
        ))
    }

    fn parse_create_catalog_statement(&mut self) -> Result<Statement, String> {
        self.parse_optional_if_not_exists()?;
        let name = self.parse_identifier("expected catalog name after `CREATE CATALOG`")?;
        let properties = self.parse_properties_clause()?;
        Ok(Statement::CreateCatalog(CreateCatalogStmt {
            name,
            properties,
        }))
    }

    fn parse_create_table_statement(&mut self) -> Result<Statement, String> {
        let name = self.parse_object_name("expected table name after `CREATE TABLE`")?;
        if self.peek().kind == TokenKind::Properties {
            let properties = self.parse_properties_clause()?;
            let path = properties
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("path"))
                .map(|(_, value)| value.clone())
                .ok_or_else(|| {
                    "standalone create table for local parquet requires `path` in `PROPERTIES`"
                        .to_string()
                })?;
            if properties.len() != 1 {
                return Err(
                    "standalone local parquet table supports exactly one `path` property"
                        .to_string(),
                );
            }
            return Ok(Statement::CreateTable(CreateTableStmt {
                name,
                kind: CreateTableKind::LocalParquet { path },
            }));
        }

        self.expect_kind(TokenKind::LParen, "expected `(` after table name")?;
        let columns = self.parse_column_defs()?;
        self.expect_kind(TokenKind::RParen, "expected `)` after column definitions")?;
        let properties = if self.peek().kind == TokenKind::Properties {
            self.parse_properties_clause()?
        } else {
            Vec::new()
        };
        Ok(Statement::CreateTable(CreateTableStmt {
            name,
            kind: CreateTableKind::Iceberg {
                columns,
                properties,
            },
        }))
    }

    fn parse_drop_statement(&mut self) -> Result<Statement, String> {
        self.expect_keyword(TokenKind::Drop, "expected `DROP` at start of statement")?;
        if self.match_keyword(TokenKind::Table) {
            let if_exists = self.parse_optional_if_exists()?;
            return Ok(Statement::DropTable(DropTableStmt {
                name: self.parse_object_name("expected table name after `DROP TABLE`")?,
                if_exists,
                force: self.match_keyword(TokenKind::Force),
            }));
        }
        if self.match_keyword(TokenKind::Database) {
            let if_exists = self.parse_optional_if_exists()?;
            return Ok(Statement::DropDatabase(DropDatabaseStmt {
                name: self.parse_object_name("expected database name after `DROP DATABASE`")?,
                if_exists,
                force: self.match_keyword(TokenKind::Force),
            }));
        }
        if self.match_keyword(TokenKind::Catalog) {
            let if_exists = self.parse_optional_if_exists()?;
            return Ok(Statement::DropCatalog(DropCatalogStmt {
                name: self.parse_identifier("expected catalog name after `DROP CATALOG`")?,
                if_exists,
            }));
        }
        Err(self.error_at(
            self.peek(),
            "expected `TABLE`, `DATABASE`, or `CATALOG` after `DROP`",
        ))
    }

    fn parse_insert_statement(&mut self) -> Result<Statement, String> {
        self.expect_keyword(TokenKind::Insert, "expected `INSERT` at start of statement")?;
        self.expect_keyword(TokenKind::Into, "expected `INTO` after `INSERT`")?;
        let table = self.parse_object_name("expected table name after `INSERT INTO`")?;
        let source = if self.match_keyword(TokenKind::Values) {
            InsertSource::Values(self.parse_insert_rows()?)
        } else if self.match_keyword(TokenKind::Select) {
            InsertSource::SelectLiteralRow(self.parse_insert_select_literal_row()?)
        } else {
            return Err(self.error_at(
                self.peek(),
                "expected `VALUES` or `SELECT` after target table",
            ));
        };
        Ok(Statement::Insert(InsertStmt { table, source }))
    }

    fn parse_insert_rows(&mut self) -> Result<Vec<Vec<Literal>>, String> {
        let mut rows = Vec::new();
        loop {
            self.expect_kind(TokenKind::LParen, "expected `(` before row values")?;
            let mut row = Vec::new();
            loop {
                row.push(self.parse_literal()?);
                if !self.match_kind(TokenKind::Comma) {
                    break;
                }
            }
            self.expect_kind(TokenKind::RParen, "expected `)` after row values")?;
            rows.push(row);
            if !self.match_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(rows)
    }

    fn parse_insert_select_literal_row(&mut self) -> Result<Vec<Literal>, String> {
        let mut row = Vec::new();
        loop {
            row.push(self.parse_literal()?);
            if !self.match_kind(TokenKind::Comma) {
                break;
            }
        }
        if self.peek().kind == TokenKind::From {
            return Err(self.error_at(
                self.peek(),
                "standalone insert-select only supports literal rows without `FROM`",
            ));
        }
        Ok(row)
    }

    fn parse_column_defs(&mut self) -> Result<Vec<TableColumnDef>, String> {
        let mut columns = Vec::new();
        loop {
            columns.push(TableColumnDef {
                name: self.parse_identifier("expected column name in column definition")?,
                data_type: self.parse_sql_type()?,
            });
            if !self.match_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(columns)
    }

    fn parse_sql_type(&mut self) -> Result<SqlType, String> {
        let token = self.peek();
        let data_type = match token.kind {
            TokenKind::TinyIntType => SqlType::TinyInt,
            TokenKind::SmallIntType => SqlType::SmallInt,
            TokenKind::IntType => SqlType::Int,
            TokenKind::BigIntType => SqlType::BigInt,
            TokenKind::FloatType => SqlType::Float,
            TokenKind::DoubleType => SqlType::Double,
            TokenKind::StringType => SqlType::String,
            TokenKind::BooleanType => SqlType::Boolean,
            TokenKind::Date => SqlType::Date,
            TokenKind::DateTimeType => SqlType::DateTime,
            TokenKind::TimeType => SqlType::Time,
            _ => {
                return Err(self.error_at(
                    token,
                    "expected column type (`TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `STRING`, `BOOLEAN`, `DATE`, `DATETIME`, `TIME`)",
                ));
            }
        };
        self.idx += 1;
        Ok(data_type)
    }

    fn parse_properties_clause(&mut self) -> Result<Vec<(String, String)>, String> {
        self.expect_keyword(TokenKind::Properties, "expected `PROPERTIES`")?;
        self.expect_kind(TokenKind::LParen, "expected `(` after `PROPERTIES`")?;
        let mut properties = Vec::new();
        loop {
            let key = self.parse_property_key("expected property name in `PROPERTIES`")?;
            self.expect_kind(TokenKind::Eq, "expected `=` after property name")?;
            let value = self.parse_string_value("expected string literal for property value")?;
            properties.push((key, value));
            if !self.match_kind(TokenKind::Comma) {
                break;
            }
        }
        self.expect_kind(TokenKind::RParen, "expected `)` after `PROPERTIES`")?;
        Ok(properties)
    }

    fn parse_projection_list(&mut self) -> Result<Vec<ProjectionItem>, String> {
        if self.match_kind(TokenKind::Star) {
            if matches!(self.peek().kind, TokenKind::Comma) {
                return Err(self.error_at(
                    self.peek(),
                    "wildcard projection cannot be combined with explicit columns",
                ));
            }
            return Ok(vec![ProjectionItem::Wildcard]);
        }
        let mut items = Vec::new();
        loop {
            items.push(ProjectionItem::Column(ColumnRef {
                name: self.parse_identifier("expected column name in select list")?,
            }));
            if !self.match_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_where_clause(&mut self) -> Result<Expr, String> {
        let left = ColumnRef {
            name: self.parse_identifier("expected column name after `WHERE`")?,
        };
        let op = self.parse_compare_op()?;
        let right = self.parse_literal()?;
        Ok(Expr::Comparison { left, op, right })
    }

    fn parse_order_by_clause(&mut self) -> Result<Vec<OrderByExpr>, String> {
        let mut items = Vec::new();
        loop {
            let column = ColumnRef {
                name: self.parse_identifier("expected column name in `ORDER BY`")?,
            };
            let descending = if self.match_keyword(TokenKind::Desc) {
                true
            } else {
                self.match_keyword(TokenKind::Asc);
                false
            };
            items.push(OrderByExpr { column, descending });
            if !self.match_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_compare_op(&mut self) -> Result<CompareOp, String> {
        let token = self.peek();
        let op = match token.kind {
            TokenKind::Eq => CompareOp::Eq,
            TokenKind::Ne => CompareOp::Ne,
            TokenKind::Lt => CompareOp::Lt,
            TokenKind::Le => CompareOp::Le,
            TokenKind::Gt => CompareOp::Gt,
            TokenKind::Ge => CompareOp::Ge,
            _ => {
                return Err(self.error_at(token, "expected comparison operator (`= != < <= > >=`)"));
            }
        };
        self.idx += 1;
        Ok(op)
    }

    fn parse_literal(&mut self) -> Result<Literal, String> {
        let token = self.peek().clone();
        let literal = match token.kind {
            TokenKind::Null => Literal::Null,
            TokenKind::True => Literal::Bool(true),
            TokenKind::False => Literal::Bool(false),
            TokenKind::Integer(value) => Literal::Int(value),
            TokenKind::Float(value) => Literal::Float(value),
            TokenKind::String(value) => Literal::String(value),
            TokenKind::Date => {
                self.idx += 1;
                let value = self.parse_string_value("expected string literal after `DATE`")?;
                return Ok(Literal::Date(value));
            }
            _ => return Err(self.error_at(&token, "expected literal")),
        };
        self.idx += 1;
        Ok(literal)
    }

    fn parse_object_name(&mut self, message: &str) -> Result<ObjectName, String> {
        let mut parts = vec![self.parse_identifier(message)?];
        while self.match_kind(TokenKind::Dot) {
            parts.push(self.parse_identifier("expected identifier after `.`")?);
        }
        Ok(ObjectName { parts })
    }

    fn parse_property_key(&mut self, message: &str) -> Result<String, String> {
        let token = self.peek().clone();
        let value = match token.kind {
            TokenKind::Identifier(value) | TokenKind::String(value) => value,
            _ => return Err(self.error_at(&token, message)),
        };
        self.idx += 1;
        Ok(value)
    }

    fn parse_string_value(&mut self, message: &str) -> Result<String, String> {
        let token = self.peek().clone();
        let TokenKind::String(value) = token.kind else {
            return Err(self.error_at(&token, message));
        };
        self.idx += 1;
        Ok(value)
    }

    fn parse_optional_if_not_exists(&mut self) -> Result<(), String> {
        if !self.match_keyword(TokenKind::If) {
            return Ok(());
        }
        self.expect_keyword(TokenKind::Not, "expected `NOT` after `IF`")?;
        self.expect_keyword(TokenKind::Exists, "expected `EXISTS` after `IF NOT`")
    }

    fn parse_optional_if_exists(&mut self) -> Result<bool, String> {
        if !self.match_keyword(TokenKind::If) {
            return Ok(false);
        }
        self.expect_keyword(TokenKind::Exists, "expected `EXISTS` after `IF`")?;
        Ok(true)
    }

    fn parse_identifier(&mut self, message: &str) -> Result<String, String> {
        let token = self.peek().clone();
        let TokenKind::Identifier(name) = token.kind else {
            return Err(self.error_at(&token, message));
        };
        self.idx += 1;
        Ok(name)
    }

    fn match_semicolon(&mut self) -> bool {
        self.match_kind(TokenKind::Semicolon)
    }

    fn match_keyword(&mut self, expected: TokenKind) -> bool {
        self.match_kind(expected)
    }

    fn match_kind(&mut self, expected: TokenKind) -> bool {
        if self.peek().kind == expected {
            self.idx += 1;
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, expected: TokenKind, message: &str) -> Result<(), String> {
        self.expect_kind(expected, message)
    }

    fn expect_kind(&mut self, expected: TokenKind, message: &str) -> Result<(), String> {
        if self.match_kind(expected) {
            Ok(())
        } else {
            Err(self.error_at(self.peek(), message))
        }
    }

    fn peek(&self) -> &Token {
        self.tokens
            .get(self.idx)
            .unwrap_or_else(|| self.tokens.last().expect("EOF token"))
    }

    #[allow(dead_code)]
    fn previous(&self) -> &Token {
        self.tokens
            .get(self.idx.saturating_sub(1))
            .expect("previous token")
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek().kind, TokenKind::Eof)
    }

    fn error_at(&self, token: &Token, message: &str) -> String {
        format!(
            "{} at position {}{}",
            message,
            token.start,
            token_suffix(token)
        )
    }

    fn unsupported_syntax_error(&self, token: &Token) -> String {
        self.error_at(token, "unsupported syntax")
    }
}

fn token_suffix(token: &Token) -> String {
    match &token.kind {
        TokenKind::Identifier(value) => format!(" near `{}`", value),
        TokenKind::String(value) => format!(" near `'{}'`", value),
        TokenKind::Integer(value) => format!(" near `{}`", value),
        TokenKind::Float(value) => format!(" near `{}`", value),
        TokenKind::Select => " near `SELECT`".to_string(),
        TokenKind::From => " near `FROM`".to_string(),
        TokenKind::Where => " near `WHERE`".to_string(),
        TokenKind::Order => " near `ORDER`".to_string(),
        TokenKind::By => " near `BY`".to_string(),
        TokenKind::Asc => " near `ASC`".to_string(),
        TokenKind::Desc => " near `DESC`".to_string(),
        TokenKind::Create => " near `CREATE`".to_string(),
        TokenKind::Drop => " near `DROP`".to_string(),
        TokenKind::Database => " near `DATABASE`".to_string(),
        TokenKind::Table => " near `TABLE`".to_string(),
        TokenKind::Properties => " near `PROPERTIES`".to_string(),
        TokenKind::Catalog => " near `CATALOG`".to_string(),
        TokenKind::External => " near `EXTERNAL`".to_string(),
        TokenKind::If => " near `IF`".to_string(),
        TokenKind::Not => " near `NOT`".to_string(),
        TokenKind::Exists => " near `EXISTS`".to_string(),
        TokenKind::Force => " near `FORCE`".to_string(),
        TokenKind::Insert => " near `INSERT`".to_string(),
        TokenKind::Into => " near `INTO`".to_string(),
        TokenKind::Values => " near `VALUES`".to_string(),
        TokenKind::Date => " near `DATE`".to_string(),
        TokenKind::True => " near `TRUE`".to_string(),
        TokenKind::False => " near `FALSE`".to_string(),
        TokenKind::Null => " near `NULL`".to_string(),
        TokenKind::TinyIntType => " near `TINYINT`".to_string(),
        TokenKind::SmallIntType => " near `SMALLINT`".to_string(),
        TokenKind::IntType => " near `INT`".to_string(),
        TokenKind::BigIntType => " near `BIGINT`".to_string(),
        TokenKind::FloatType => " near `FLOAT`".to_string(),
        TokenKind::DoubleType => " near `DOUBLE`".to_string(),
        TokenKind::StringType => " near `STRING`".to_string(),
        TokenKind::BooleanType => " near `BOOLEAN`".to_string(),
        TokenKind::DateTimeType => " near `DATETIME`".to_string(),
        TokenKind::TimeType => " near `TIME`".to_string(),
        TokenKind::Star => " near `*`".to_string(),
        TokenKind::Comma => " near `,`".to_string(),
        TokenKind::Dot => " near `.`".to_string(),
        TokenKind::LParen => " near `(`".to_string(),
        TokenKind::RParen => " near `)`".to_string(),
        TokenKind::Semicolon => " near `;`".to_string(),
        TokenKind::Eq => " near `=`".to_string(),
        TokenKind::Ne => " near `!=`".to_string(),
        TokenKind::Lt => " near `<`".to_string(),
        TokenKind::Le => " near `<=`".to_string(),
        TokenKind::Gt => " near `>`".to_string(),
        TokenKind::Ge => " near `>=`".to_string(),
        TokenKind::Eof => String::new(),
    }
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_identifier_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexer_tokenizes_catalog_and_table_names() {
        let tokens = Lexer::new("select * from cat.db.tbl where id = 1")
            .tokenize()
            .expect("tokenize");
        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[3].kind, TokenKind::Identifier("cat".to_string()));
        assert_eq!(tokens[4].kind, TokenKind::Dot);
        assert_eq!(tokens[5].kind, TokenKind::Identifier("db".to_string()));
        assert_eq!(tokens[6].kind, TokenKind::Dot);
        assert_eq!(tokens[7].kind, TokenKind::Identifier("tbl".to_string()));
    }

    #[test]
    fn lexer_tokenizes_create_table_properties() {
        let tokens = Lexer::new(r#"create table tbl properties("path"="/tmp/tbl.parquet")"#)
            .tokenize()
            .expect("tokenize");
        assert_eq!(tokens[0].kind, TokenKind::Create);
        assert_eq!(tokens[1].kind, TokenKind::Table);
        assert_eq!(tokens[2].kind, TokenKind::Identifier("tbl".to_string()));
        assert_eq!(tokens[3].kind, TokenKind::Properties);
        assert_eq!(tokens[4].kind, TokenKind::LParen);
        assert_eq!(tokens[5].kind, TokenKind::String("path".to_string()));
        assert_eq!(tokens[6].kind, TokenKind::Eq);
    }

    #[test]
    fn parser_accepts_select_projection_and_where() {
        let stmt = ManualParser
            .parse("select c1, c2 from tbl where c3 = 1")
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::Query(QueryStmt {
                projection: vec![
                    ProjectionItem::Column(ColumnRef {
                        name: "c1".to_string()
                    }),
                    ProjectionItem::Column(ColumnRef {
                        name: "c2".to_string()
                    }),
                ],
                from: TableRef {
                    name: ObjectName {
                        parts: vec!["tbl".to_string()]
                    }
                },
                selection: Some(Expr::Comparison {
                    left: ColumnRef {
                        name: "c3".to_string()
                    },
                    op: CompareOp::Eq,
                    right: Literal::Int(1),
                }),
                order_by: vec![],
            })
        );
    }

    #[test]
    fn parser_accepts_order_by_clause() {
        let stmt = ManualParser
            .parse("select id, name from tbl order by id desc, name asc")
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::Query(QueryStmt {
                projection: vec![
                    ProjectionItem::Column(ColumnRef {
                        name: "id".to_string()
                    }),
                    ProjectionItem::Column(ColumnRef {
                        name: "name".to_string()
                    }),
                ],
                from: TableRef {
                    name: ObjectName {
                        parts: vec!["tbl".to_string()]
                    }
                },
                selection: None,
                order_by: vec![
                    OrderByExpr {
                        column: ColumnRef {
                            name: "id".to_string()
                        },
                        descending: true,
                    },
                    OrderByExpr {
                        column: ColumnRef {
                            name: "name".to_string()
                        },
                        descending: false,
                    },
                ],
            })
        );
    }

    #[test]
    fn parser_accepts_create_catalog() {
        let stmt = ManualParser
            .parse(
                r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="/tmp/wh")"#,
            )
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::CreateCatalog(CreateCatalogStmt {
                name: "ice".to_string(),
                properties: vec![
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "memory".to_string()),
                    (
                        "iceberg.catalog.warehouse".to_string(),
                        "/tmp/wh".to_string()
                    ),
                ],
            })
        );
    }

    #[test]
    fn parser_accepts_create_database() {
        let stmt = ManualParser
            .parse("create database ice.db1")
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::CreateDatabase(CreateDatabaseStmt {
                name: ObjectName {
                    parts: vec!["ice".to_string(), "db1".to_string()],
                },
            })
        );
    }

    #[test]
    fn parser_accepts_local_create_table_with_path_property() {
        let stmt = ManualParser
            .parse(r#"create table tbl properties("path"="/tmp/tbl.parquet")"#)
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::CreateTable(CreateTableStmt {
                name: ObjectName {
                    parts: vec!["tbl".to_string()],
                },
                kind: CreateTableKind::LocalParquet {
                    path: "/tmp/tbl.parquet".to_string(),
                },
            })
        );
    }

    #[test]
    fn parser_accepts_iceberg_create_table() {
        let stmt = ManualParser
            .parse(r#"create table ice.db.tbl (id int, name string) properties("owner"="qa")"#)
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::CreateTable(CreateTableStmt {
                name: ObjectName {
                    parts: vec!["ice".to_string(), "db".to_string(), "tbl".to_string()],
                },
                kind: CreateTableKind::Iceberg {
                    columns: vec![
                        TableColumnDef {
                            name: "id".to_string(),
                            data_type: SqlType::Int,
                        },
                        TableColumnDef {
                            name: "name".to_string(),
                            data_type: SqlType::String,
                        },
                    ],
                    properties: vec![("owner".to_string(), "qa".to_string())],
                },
            })
        );
    }

    #[test]
    fn parser_accepts_extended_primitive_column_types() {
        let stmt = ManualParser
            .parse(
                "create table ice.db.tbl (c1 tinyint, c2 smallint, c3 float, c4 double, c5 date, c6 datetime, c7 time)",
            )
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::CreateTable(CreateTableStmt {
                name: ObjectName {
                    parts: vec!["ice".to_string(), "db".to_string(), "tbl".to_string()],
                },
                kind: CreateTableKind::Iceberg {
                    columns: vec![
                        TableColumnDef {
                            name: "c1".to_string(),
                            data_type: SqlType::TinyInt,
                        },
                        TableColumnDef {
                            name: "c2".to_string(),
                            data_type: SqlType::SmallInt,
                        },
                        TableColumnDef {
                            name: "c3".to_string(),
                            data_type: SqlType::Float,
                        },
                        TableColumnDef {
                            name: "c4".to_string(),
                            data_type: SqlType::Double,
                        },
                        TableColumnDef {
                            name: "c5".to_string(),
                            data_type: SqlType::Date,
                        },
                        TableColumnDef {
                            name: "c6".to_string(),
                            data_type: SqlType::DateTime,
                        },
                        TableColumnDef {
                            name: "c7".to_string(),
                            data_type: SqlType::Time,
                        },
                    ],
                    properties: vec![],
                },
            })
        );
    }

    #[test]
    fn parser_accepts_insert_values() {
        let stmt = ManualParser
            .parse("insert into ice.db.tbl values (1, 'a'), (2, null)")
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::Insert(InsertStmt {
                table: ObjectName {
                    parts: vec!["ice".to_string(), "db".to_string(), "tbl".to_string()],
                },
                source: InsertSource::Values(vec![
                    vec![Literal::Int(1), Literal::String("a".to_string())],
                    vec![Literal::Int(2), Literal::Null],
                ]),
            })
        );
    }

    #[test]
    fn parser_accepts_insert_select_literal_row() {
        let stmt = ManualParser
            .parse("insert into ice.db.tbl select 1, 'a'")
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::Insert(InsertStmt {
                table: ObjectName {
                    parts: vec!["ice".to_string(), "db".to_string(), "tbl".to_string()],
                },
                source: InsertSource::SelectLiteralRow(vec![
                    Literal::Int(1),
                    Literal::String("a".to_string()),
                ]),
            })
        );
    }

    #[test]
    fn parser_accepts_drop_table() {
        let stmt = ManualParser.parse("drop table tbl").expect("parse");
        assert_eq!(
            stmt,
            Statement::DropTable(DropTableStmt {
                name: ObjectName {
                    parts: vec!["tbl".to_string()],
                },
                if_exists: false,
                force: false,
            })
        );

        let stmt = ManualParser
            .parse("drop table if exists db.tbl force")
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::DropTable(DropTableStmt {
                name: ObjectName {
                    parts: vec!["db".to_string(), "tbl".to_string()],
                },
                if_exists: true,
                force: true,
            })
        );
    }

    #[test]
    fn parser_accepts_drop_database_and_catalog() {
        let stmt = ManualParser
            .parse("drop database if exists ice.db1 force")
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::DropDatabase(DropDatabaseStmt {
                name: ObjectName {
                    parts: vec!["ice".to_string(), "db1".to_string()],
                },
                if_exists: true,
                force: true,
            })
        );

        let stmt = ManualParser
            .parse("drop catalog if exists ice")
            .expect("parse");
        assert_eq!(
            stmt,
            Statement::DropCatalog(DropCatalogStmt {
                name: "ice".to_string(),
                if_exists: true,
            })
        );
    }

    #[test]
    fn parser_accepts_all_comparison_operators() {
        let cases = [
            ("=", CompareOp::Eq),
            ("!=", CompareOp::Ne),
            ("<", CompareOp::Lt),
            ("<=", CompareOp::Le),
            (">", CompareOp::Gt),
            (">=", CompareOp::Ge),
        ];
        for (op, expected) in cases {
            let sql = format!("select c1 from tbl where c2 {} 1", op);
            let Statement::Query(query) = ManualParser.parse(&sql).expect("parse") else {
                panic!("expected query statement");
            };
            let Some(Expr::Comparison { op, .. }) = query.selection else {
                panic!("expected comparison");
            };
            assert_eq!(op, expected);
        }
    }

    #[test]
    fn parser_accepts_wildcard_and_semicolon() {
        let stmt = ManualParser.parse("SELECT * FROM tbl;").expect("parse");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        assert_eq!(query.projection, vec![ProjectionItem::Wildcard]);
        assert_eq!(query.from.name.parts, vec!["tbl".to_string()]);
    }

    #[test]
    fn parser_rejects_unsupported_syntax() {
        let cases = [
            "select c1 as x from tbl",
            "select c1 from tbl join t2 on tbl.id = t2.id",
            "select upper(c1) from tbl",
            "select c1 from tbl where c2 = 1 and c3 = 2",
            "select c1 from tbl order by c1",
            "select c1 from tbl limit 1",
            "select c1 from (select * from tbl)",
            "create table tbl",
            "insert into tbl select 1",
            "drop database if missing analytics",
            "drop catalog if not exists ice",
        ];
        for sql in cases {
            let err = ManualParser.parse(sql).expect_err("expected rejection");
            assert!(
                err.contains("unsupported")
                    || err.contains("expected")
                    || err.contains("supported")
            );
        }
    }

    #[test]
    fn parser_rejects_wildcard_mixed_with_columns() {
        let err = ManualParser
            .parse("select *, c1 from tbl")
            .expect_err("mixed wildcard");
        assert!(err.contains("wildcard projection cannot be combined"));
    }

    #[test]
    fn parser_rejects_empty_sql() {
        let err = ManualParser.parse("   ").expect_err("empty sql");
        assert!(err.contains("SQL is empty"));
    }
}
