// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use sqlparser::parser::ParserError;
use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};

const DIALECT: GenericDialect = GenericDialect {};

/// Parse a string to a collection of statements.
///
/// # Example
/// ```rust
/// use kite_sql::parser::parse_sql;
/// let sql = "SELECT a, b, 123, myfunc(b) \
///            FROM table_1 \
///            WHERE a > b AND b < 100 \
///            ORDER BY a DESC, b";
/// let ast = parse_sql(sql).unwrap();
/// println!("{:?}", ast);
/// ```
pub fn parse_sql<S: AsRef<str>>(sql: S) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(&DIALECT, sql.as_ref())
}

#[cfg(test)]
mod tests {
    use super::parse_sql;
    use sqlparser::ast::{SetExpr, Statement};

    #[test]
    fn parses_optimizer_hint() {
        let statements = parse_sql("SELECT /*+ FORCE_AGG_SPILL */ a FROM t").unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query statement");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select query");
        };

        assert_eq!(
            select.optimizer_hint.as_ref().map(|hint| hint.text.trim()),
            Some("FORCE_AGG_SPILL")
        );
    }
}
