use super::*;

impl<S: Storage> Database<S> {
    /// Loads a single model by primary key.
    ///
    /// The key type is taken from `M::PrimaryKey`, so `database.get::<User>(&1)`
    /// works without an extra generic parameter.
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
    /// let user = database.get::<User>(&1).unwrap().unwrap();
    /// assert_eq!(user.name, "Alice");
    /// ```
    pub fn get<M: Model>(&self, key: &M::PrimaryKey) -> Result<Option<M>, DatabaseError> {
        orm_get::<_, M>(self, key)
    }

    /// Fetches all rows from the model table as a typed iterator.
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
    ///
    /// let users = database.fetch::<User>().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    /// assert_eq!(users.len(), 1);
    /// assert_eq!(users[0].name, "Alice");
    /// ```
    pub fn fetch<M: Model>(&self) -> Result<OrmIter<DatabaseIter<'_, S>, M>, DatabaseError> {
        orm_list::<_, M>(self)
    }

    /// Starts a typed single-table query builder for the given model.
    pub fn from<M: Model>(&self) -> FromBuilder<&Database<S>, M> {
        FromBuilder::from_inner(QueryBuilder::new(self))
    }

    /// Starts a typed single-table query builder using a table alias.
    ///
    /// Fields can be rebound to the alias with [`Field::qualified`] when the
    /// query needs explicit qualification.
    pub fn from_alias<M: Model>(&self, alias: impl Into<String>) -> FromBuilder<&Database<S>, M> {
        FromBuilder::from_inner(QueryBuilder::aliased(self, alias))
    }
}

impl<'a, S: Storage> DBTransaction<'a, S> {
    /// Loads a single model by primary key inside the current transaction.
    pub fn get<M: Model>(&mut self, key: &M::PrimaryKey) -> Result<Option<M>, DatabaseError> {
        orm_get::<_, M>(self, key)
    }

    /// Fetches all rows for a model inside the current transaction.
    pub fn fetch<M: Model>(&mut self) -> Result<OrmIter<TransactionIter<'_>, M>, DatabaseError> {
        orm_list::<_, M>(self)
    }

    /// Starts a typed single-table query builder inside the current transaction.
    pub fn from<M: Model>(&mut self) -> FromBuilder<&mut DBTransaction<'a, S>, M> {
        FromBuilder::from_inner(QueryBuilder::new(self))
    }

    /// Starts a typed single-table query builder using a table alias inside the current transaction.
    pub fn from_alias<M: Model>(
        &mut self,
        alias: impl Into<String>,
    ) -> FromBuilder<&mut DBTransaction<'a, S>, M> {
        FromBuilder::from_inner(QueryBuilder::aliased(self, alias))
    }
}
