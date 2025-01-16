use kite_sql::db::{Database, ResultIter};
use kite_sql::errors::DatabaseError;
use kite_sql::storage::rocksdb::RocksStorage;
use sqllogictest::{DBOutput, DefaultColumnType, DB};
use std::time::Instant;

pub struct SQLBase {
    pub db: Database<RocksStorage>,
}

impl DB for SQLBase {
    type Error = DatabaseError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let start = Instant::now();
        let mut iter = self.db.run(sql)?;
        println!("|— Input SQL: {}", sql);
        let types = vec![DefaultColumnType::Any; iter.schema().len()];
        let mut rows = Vec::new();

        for tuple in iter.by_ref() {
            rows.push(
                tuple?
                    .values
                    .into_iter()
                    .map(|value| format!("{}", value))
                    .collect(),
            )
        }
        iter.done()?;
        println!(" |— time spent: {:?}", start.elapsed());
        if rows.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }
        Ok(DBOutput::Rows { types, rows })
    }
}
