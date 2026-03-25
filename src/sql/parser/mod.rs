use crate::sql::ast::Statement;

mod manual;

use manual::ManualParser;

pub(crate) trait SqlParserBackend {
    fn parse(&self, sql: &str) -> Result<Statement, String>;
}

pub(crate) fn parse_sql(sql: &str) -> Result<Statement, String> {
    ManualParser.parse(sql)
}
