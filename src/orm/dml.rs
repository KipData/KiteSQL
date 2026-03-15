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

    /// Updates a model in its backing table using the primary key.
    pub fn update<M: Model>(&self, model: &M) -> Result<(), DatabaseError> {
        orm_update::<_, M>(self, model)
    }

    /// Deletes a model from its backing table by primary key.
    ///
    /// The primary-key type is inferred from `M`, so callers do not need a
    /// separate generic argument for the key type.
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
    /// database.delete_by_id::<User>(&1).unwrap();
    /// assert!(database.get::<User>(&1).unwrap().is_none());
    /// ```
    pub fn delete_by_id<M: Model>(&self, key: &M::PrimaryKey) -> Result<(), DatabaseError> {
        orm_delete_by_id::<_, M>(self, key)
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

    /// Updates a model inside the current transaction.
    pub fn update<M: Model>(&mut self, model: &M) -> Result<(), DatabaseError> {
        orm_update::<_, M>(self, model)
    }

    /// Deletes a model by primary key inside the current transaction.
    pub fn delete_by_id<M: Model>(&mut self, key: &M::PrimaryKey) -> Result<(), DatabaseError> {
        orm_delete_by_id::<_, M>(self, key)
    }
}
