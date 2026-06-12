use super::*;

impl<S: Storage> Database<S> {
    /// Executes a binder-backed plan built inside a closure.
    pub fn bind<F>(&self, build: F) -> Result<DatabaseIter<'_, S>, DatabaseError>
    where
        F: for<'ctx, 'bind, 'parent, 'arena> FnOnce(
            &'ctx mut OrmContext<
                'ctx,
                'bind,
                'parent,
                'arena,
                S::TransactionType<'_>,
                &'static [(&'static str, DataValue)],
            >,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        bind_orm_context(self, build)
    }

    /// Explains a binder-backed plan built inside a closure.
    pub fn explain<F>(&self, build: F) -> Result<String, DatabaseError>
    where
        F: for<'ctx, 'bind, 'parent, 'arena> FnOnce(
            &'ctx mut OrmContext<
                'ctx,
                'bind,
                'parent,
                'arena,
                S::TransactionType<'_>,
                &'static [(&'static str, DataValue)],
            >,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        explain_orm_context(self, build)
    }

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

    /// Lists all table names.
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
    /// let tables = database.show_tables().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    /// assert!(tables.iter().any(|name| name == "users"));
    /// ```
    pub fn show_tables(
        &self,
    ) -> Result<ProjectValueIter<DatabaseIter<'_, S>, String>, DatabaseError> {
        Ok(ProjectValueIter::new(
            self.bind(|ctx| ctx.binder.bind_show_tables())?,
        ))
    }

    /// Lists all view names.
    pub fn show_views(
        &self,
    ) -> Result<ProjectValueIter<DatabaseIter<'_, S>, String>, DatabaseError> {
        Ok(ProjectValueIter::new(
            self.bind(|ctx| ctx.binder.bind_show_views())?,
        ))
    }

    /// Describes the schema of the model table.
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
    /// let columns = database.describe::<User>().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    /// assert!(columns.iter().any(|column| column.field == "id"));
    /// ```
    pub fn describe<M: Model>(
        &self,
    ) -> Result<OrmIter<DatabaseIter<'_, S>, DescribeColumn>, DatabaseError> {
        Ok(self
            .bind(|ctx| ctx.binder.bind_describe(M::table_name().into()))?
            .orm::<DescribeColumn>())
    }
}

impl<'a, S: Storage> DBTransaction<'a, S> {
    /// Executes a binder-backed plan inside the current transaction.
    pub fn bind<F>(
        &mut self,
        build: F,
    ) -> Result<TransactionIter<'_, S::TransactionType<'a>>, DatabaseError>
    where
        F: for<'ctx, 'bind, 'parent, 'arena> FnOnce(
            &'ctx mut OrmContext<
                'ctx,
                'bind,
                'parent,
                'arena,
                S::TransactionType<'a>,
                &'static [(&'static str, DataValue)],
            >,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        bind_orm_context(self, build)
    }

    /// Explains a binder-backed plan inside the current transaction.
    pub fn explain<F>(&mut self, build: F) -> Result<String, DatabaseError>
    where
        F: for<'ctx, 'bind, 'parent, 'arena> FnOnce(
            &'ctx mut OrmContext<
                'ctx,
                'bind,
                'parent,
                'arena,
                S::TransactionType<'a>,
                &'static [(&'static str, DataValue)],
            >,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        explain_orm_context(self, build)
    }

    /// Loads a single model by primary key inside the current transaction.
    pub fn get<M: Model>(&mut self, key: &M::PrimaryKey) -> Result<Option<M>, DatabaseError> {
        orm_get::<_, M>(self, key)
    }

    /// Fetches all rows for a model inside the current transaction.
    pub fn fetch<M: Model>(
        &mut self,
    ) -> Result<OrmIter<TransactionIter<'_, S::TransactionType<'a>>, M>, DatabaseError> {
        orm_list::<_, M>(self)
    }

    /// Lists all table names inside the current transaction.
    pub fn show_tables(
        &mut self,
    ) -> Result<ProjectValueIter<TransactionIter<'_, S::TransactionType<'a>>, String>, DatabaseError>
    {
        Ok(ProjectValueIter::new(
            self.bind(|ctx| ctx.binder.bind_show_tables())?,
        ))
    }

    /// Lists all view names inside the current transaction.
    pub fn show_views(
        &mut self,
    ) -> Result<ProjectValueIter<TransactionIter<'_, S::TransactionType<'a>>, String>, DatabaseError>
    {
        Ok(ProjectValueIter::new(
            self.bind(|ctx| ctx.binder.bind_show_views())?,
        ))
    }

    /// Describes the schema of the model table inside the current transaction.
    pub fn describe<M: Model>(
        &mut self,
    ) -> Result<OrmIter<TransactionIter<'_, S::TransactionType<'a>>, DescribeColumn>, DatabaseError>
    {
        Ok(self
            .bind(|ctx| ctx.binder.bind_describe(M::table_name().into()))?
            .orm::<DescribeColumn>())
    }
}
