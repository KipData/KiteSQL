use super::*;

#[maybe_async]
impl<S: Storage> Database<S> {
    /// Prepares an ORM-built plan once into a program for repeated execution with
    /// positional parameters. Use `e.param(id)` inside expression closures, then pass
    /// values to `Program::execute`.
    pub fn program<F>(
        &self,
        params: &[(usize, LogicalType)],
        build: F,
    ) -> Result<crate::db::Program<'_>, DatabaseError>
    where
        F: for<'ctx, 'bind, 'parent, 'arena> FnOnce(
            &'ctx mut OrmContext<
                'ctx,
                'bind,
                'parent,
                'arena,
                S::TransactionType<'_>,
                &[(usize, LogicalType)],
            >,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let transaction = self
            .storage
            .transaction_with_isolation(self.transaction_isolation, true)?;
        self.state
            .prepare_plan_with(params, &transaction, |binder, arena| {
                let mut context = OrmContext { binder, arena };
                build(&mut context)
            })
    }

    /// Executes a binder-backed plan built inside a closure.
    pub async fn bind<F>(&self, build: F) -> Result<DatabaseIter<'_, S>, DatabaseError>
    where
        F: for<'ctx, 'bind, 'parent, 'arena> FnOnce(
                &'ctx mut OrmContext<
                    'ctx,
                    'bind,
                    'parent,
                    'arena,
                    S::TransactionType<'_>,
                    &'static [(usize, LogicalType)],
                >,
            )
                -> Result<LogicalPlan, DatabaseError>
            + MaybeSend,
    {
        bind_orm_context(self, false, build).await
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
                &'static [(usize, LogicalType)],
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
    pub async fn get<M: Model>(&self, key: &M::PrimaryKey) -> Result<Option<M>, DatabaseError> {
        orm_get::<_, M>(self, key).await
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
    pub async fn fetch<M: Model>(&self) -> Result<OrmIter<DatabaseIter<'_, S>, M>, DatabaseError> {
        Ok(
            bind_orm_context(self, true, |ctx| ctx.from::<M>()?.finish())
                .await?
                .orm::<M>(),
        )
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
    pub async fn show_tables(
        &self,
    ) -> Result<ProjectValueIter<DatabaseIter<'_, S>, String>, DatabaseError> {
        Ok(ProjectValueIter::new(
            self.bind(|ctx| ctx.binder.bind_show_tables()).await?,
        ))
    }

    /// Lists all view names.
    pub async fn show_views(
        &self,
    ) -> Result<ProjectValueIter<DatabaseIter<'_, S>, String>, DatabaseError> {
        Ok(ProjectValueIter::new(
            self.bind(|ctx| ctx.binder.bind_show_views()).await?,
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
    pub async fn describe<M: Model>(
        &self,
    ) -> Result<OrmIter<DatabaseIter<'_, S>, DescribeColumn>, DatabaseError> {
        Ok(self
            .bind(|ctx| ctx.binder.bind_describe(M::table_name()))
            .await?
            .orm::<DescribeColumn>())
    }
}

#[maybe_async]
impl<'a, S: Storage> Transaction<'a, S> {
    /// Prepares an ORM-built plan into a program, against this transaction's view.
    pub fn program<F>(
        &self,
        params: &[(usize, LogicalType)],
        build: F,
    ) -> Result<crate::db::Program<'a>, DatabaseError>
    where
        F: for<'ctx, 'bind, 'parent, 'arena> FnOnce(
            &'ctx mut OrmContext<
                'ctx,
                'bind,
                'parent,
                'arena,
                S::TransactionType<'a>,
                &[(usize, LogicalType)],
            >,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        self.state
            .prepare_plan_with(params, &self.inner, |binder, arena| {
                let mut context = OrmContext { binder, arena };
                build(&mut context)
            })
    }

    /// Executes a binder-backed plan inside the current transaction.
    pub async fn bind<F>(
        &mut self,
        build: F,
    ) -> Result<TransactionIter<'_, '_, S::TransactionType<'a>>, DatabaseError>
    where
        F: for<'ctx, 'bind, 'parent, 'arena> FnOnce(
                &'ctx mut OrmContext<
                    'ctx,
                    'bind,
                    'parent,
                    'arena,
                    S::TransactionType<'a>,
                    &'static [(usize, LogicalType)],
                >,
            )
                -> Result<LogicalPlan, DatabaseError>
            + MaybeSend,
    {
        bind_orm_context(self, false, build).await
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
                &'static [(usize, LogicalType)],
            >,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        explain_orm_context(self, build)
    }

    /// Loads a single model by primary key inside the current transaction. A model with a
    /// lifetime may borrow its strings and binaries from the transaction.
    pub async fn get<'t, M: Model + FromTxnRow<'t>>(
        &'t mut self,
        key: &M::PrimaryKey,
    ) -> Result<Option<M>, DatabaseError> {
        let mut iter = bind_get::<_, M>(self, key).await?;
        Ok(
            match iter
                .next_tuple(|schema, row| M::from_txn_row(schema, row))
                .await?
            {
                Some(row) => Some(row?),
                None => None,
            },
        )
    }

    /// Fetches all rows for a model inside the current transaction. A model with a lifetime may
    /// borrow its strings and binaries from the transaction.
    pub async fn fetch<'t, M: Model + FromTxnRow<'t>>(
        &'t mut self,
    ) -> Result<TxnOrmIter<'t, TransactionIter<'t, 't, S::TransactionType<'a>>, M>, DatabaseError>
    {
        Ok(
            bind_orm_context(self, true, |ctx| ctx.from::<M>()?.finish())
                .await?
                .txn_orm::<M>(),
        )
    }

    /// Lists all table names inside the current transaction.
    pub async fn show_tables(
        &mut self,
    ) -> Result<
        ProjectValueIter<TransactionIter<'_, '_, S::TransactionType<'a>>, String>,
        DatabaseError,
    > {
        Ok(ProjectValueIter::new(
            self.bind(|ctx| ctx.binder.bind_show_tables()).await?,
        ))
    }

    /// Lists all view names inside the current transaction.
    pub async fn show_views(
        &mut self,
    ) -> Result<
        ProjectValueIter<TransactionIter<'_, '_, S::TransactionType<'a>>, String>,
        DatabaseError,
    > {
        Ok(ProjectValueIter::new(
            self.bind(|ctx| ctx.binder.bind_show_views()).await?,
        ))
    }

    /// Describes the schema of the model table inside the current transaction.
    pub async fn describe<M: Model>(
        &mut self,
    ) -> Result<
        OrmIter<TransactionIter<'_, '_, S::TransactionType<'a>>, DescribeColumn>,
        DatabaseError,
    > {
        Ok(self
            .bind(|ctx| ctx.binder.bind_describe(M::table_name()))
            .await?
            .orm::<DescribeColumn>())
    }
}
