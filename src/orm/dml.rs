use super::*;

impl<S: Storage> Database<S> {
    /// Refreshes optimizer statistics for the model table.
    ///
    /// This runs `ANALYZE TABLE` for the backing table so the optimizer can use
    /// up-to-date statistics.
    pub fn analyze<M: Model>(&self) -> Result<(), DatabaseError> {
        orm_analyze::<_, M>(self)
    }

    /// Inserts a model into its backing table.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kite_sql::db::DataBaseBuilder;
    /// use kite_sql::Model;
    ///
    /// #[derive(Default, Debug, PartialEq, Model)]
    /// #[model(table = "users")]
    /// struct User {
    ///     #[model(primary_key)]
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// let database = DataBaseBuilder::path(".").build_in_memory().unwrap();
    /// database.create_table::<User>().unwrap();
    /// database.insert(&User { id: 1, name: "Alice".to_string() }).unwrap();
    /// ```
    pub fn insert<M: Model>(&self, model: &M) -> Result<(), DatabaseError> {
        orm_insert::<_, M>(self, model)
    }
}

impl<'a, S: Storage> DBTransaction<'a, S> {
    /// Refreshes optimizer statistics for the model table inside the current transaction.
    pub fn analyze<M: Model>(&mut self) -> Result<(), DatabaseError> {
        orm_analyze::<_, M>(self)
    }

    /// Inserts a model inside the current transaction.
    pub fn insert<M: Model>(&mut self, model: &M) -> Result<(), DatabaseError> {
        orm_insert::<_, M>(self, model)
    }
}
