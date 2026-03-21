use super::*;

impl<S: Storage> Database<S> {
    fn table_catalog(&self, table_name: &str) -> Result<Option<TableCatalog>, DatabaseError> {
        let transaction = self.storage.transaction()?;
        transaction
            .table(self.state.table_cache(), table_name.into())
            .map(|table| table.cloned())
    }

    /// Creates the table described by a model.
    ///
    /// Any secondary indexes declared with `#[model(index)]` are created after
    /// the table itself.
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
    ///     age: Option<i32>,
    /// }
    ///
    /// let database = DataBaseBuilder::path(".").build_in_memory().unwrap();
    /// database.create_table::<User>().unwrap();
    /// ```
    pub fn create_table<M: Model>(&self) -> Result<(), DatabaseError> {
        self.execute(M::create_table_statement(), &[])?.done()?;

        for statement in M::create_index_statements() {
            self.execute(statement, &[])?.done()?;
        }

        Ok(())
    }

    /// Creates the model table if it does not already exist.
    ///
    /// This is useful for examples, tests and bootstrap flows where rerunning
    /// schema initialization should stay idempotent. Secondary indexes declared
    /// with `#[model(index)]` are created with `IF NOT EXISTS` as well.
    pub fn create_table_if_not_exists<M: Model>(&self) -> Result<(), DatabaseError> {
        self.execute(M::create_table_if_not_exists_statement(), &[])?
            .done()?;

        for statement in M::create_index_if_not_exists_statements() {
            self.execute(statement, &[])?.done()?;
        }

        Ok(())
    }

    /// Truncates the model table.
    pub fn truncate<M: Model>(&self) -> Result<(), DatabaseError> {
        self.execute(&orm_truncate_statement(M::table_name()), &[])?
            .done()
    }

    /// Creates a view from an ORM query builder.
    pub fn create_view<Q: SubquerySource>(
        &self,
        view_name: &str,
        query: Q,
    ) -> Result<(), DatabaseError> {
        self.execute(
            &orm_create_view_statement(view_name, query.into_subquery(), false),
            &[],
        )?
        .done()
    }

    /// Creates or replaces a view from an ORM query builder.
    pub fn create_or_replace_view<Q: SubquerySource>(
        &self,
        view_name: &str,
        query: Q,
    ) -> Result<(), DatabaseError> {
        self.execute(
            &orm_create_view_statement(view_name, query.into_subquery(), true),
            &[],
        )?
        .done()
    }

    /// Drops a view by name.
    pub fn drop_view(&self, view_name: &str) -> Result<(), DatabaseError> {
        self.execute(&orm_drop_view_statement(view_name, false), &[])?
            .done()
    }

    /// Drops a view by name if it exists.
    pub fn drop_view_if_exists(&self, view_name: &str) -> Result<(), DatabaseError> {
        self.execute(&orm_drop_view_statement(view_name, true), &[])?
            .done()
    }

    /// Migrates an existing table to match the current model definition.
    ///
    /// This helper creates the table when it does not exist, adds missing
    /// columns, drops columns that are no longer declared by the model, applies
    /// supported `ALTER TABLE .. CHANGE/ALTER COLUMN` operations for existing
    /// columns, and ensures declared secondary indexes exist.
    ///
    /// The migration can automatically handle safe column renames plus changes
    /// to type, nullability and default expressions for non-primary-key columns
    /// when the underlying DDL supports them. Primary-key changes and unique
    /// constraint changes still return an error so you can handle them manually.
    pub fn migrate<M: Model>(&self) -> Result<(), DatabaseError> {
        let columns = M::columns();
        if columns.is_empty() {
            return Err(DatabaseError::UnsupportedStmt(
                "ORM migration requires Model::columns(); #[derive(Model)] provides it automatically"
                    .to_string(),
            ));
        }

        let Some(table) = self.table_catalog(M::table_name())? else {
            return self.create_table::<M>();
        };

        let model_primary_key = columns
            .iter()
            .find(|column| column.primary_key)
            .ok_or(DatabaseError::PrimaryKeyNotFound)?;
        let table_primary_key = table
            .primary_keys()
            .first()
            .map(|(_, column)| column.clone())
            .ok_or(DatabaseError::PrimaryKeyNotFound)?;
        if table_primary_key.name() != model_primary_key.name
            || !model_column_matches_catalog(model_primary_key, &table_primary_key)
        {
            return Err(DatabaseError::InvalidValue(::std::format!(
                "ORM migration does not support changing the primary key for table `{}`",
                M::table_name(),
            )));
        }

        let current_columns = table
            .columns()
            .map(|column| (column.name().to_string(), column.clone()))
            .collect::<BTreeMap<_, _>>();
        let model_columns = columns
            .iter()
            .map(|column| (column.name, column))
            .collect::<BTreeMap<_, _>>();
        let mut handled_current = BTreeMap::new();
        let mut handled_model = BTreeMap::new();

        for column in columns {
            let Some(current_column) = current_columns.get(column.name) else {
                continue;
            };
            handled_current.insert(current_column.name().to_string(), ());
            handled_model.insert(column.name, ());

            if column.primary_key != current_column.desc().is_primary() {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration does not support changing the primary key for table `{}`",
                    M::table_name(),
                )));
            }
            if column.unique != current_column.desc().is_unique() {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration cannot automatically change unique constraint on column `{}` of table `{}`",
                    column.name,
                    M::table_name(),
                )));
            }
            if model_column_matches_catalog(column, current_column) {
                continue;
            }

            if !model_column_type_matches_catalog(column, current_column) {
                let statement = orm_alter_column_type_statement(
                    M::table_name(),
                    column.name,
                    &column.ddl_type,
                )?;
                self.execute(&statement, &[])?.done()?;
            }

            if model_column_default(column) != catalog_column_default(current_column) {
                let statement = orm_alter_column_default_statement(
                    M::table_name(),
                    column.name,
                    column.default_expr,
                )?;
                self.execute(&statement, &[])?.done()?;
            }

            if column.nullable != current_column.nullable() {
                let statement = orm_alter_column_nullability_statement(
                    M::table_name(),
                    column.name,
                    column.nullable,
                );
                self.execute(&statement, &[])?.done()?;
            }
        }

        let mut rename_pairs = Vec::new();
        let unmatched_model_columns = columns
            .iter()
            .filter(|column| !handled_model.contains_key(column.name))
            .collect::<Vec<_>>();
        let unmatched_current_columns = table
            .columns()
            .filter(|column| !handled_current.contains_key(column.name()))
            .collect::<Vec<_>>();

        for model_column in &unmatched_model_columns {
            if model_column.primary_key {
                continue;
            }
            let candidates = unmatched_current_columns
                .iter()
                .copied()
                .filter(|column| !column.desc().is_primary())
                .filter(|column| model_column_rename_compatible(model_column, column))
                .collect::<Vec<_>>();
            if candidates.len() != 1 {
                continue;
            }
            let current_column = candidates[0];
            let reverse_candidates = unmatched_model_columns
                .iter()
                .filter(|other| !other.primary_key)
                .filter(|other| model_column_rename_compatible(other, current_column))
                .collect::<Vec<_>>();
            if reverse_candidates.len() != 1 {
                continue;
            }
            rename_pairs.push((current_column.name().to_string(), model_column.name));
            handled_current.insert(current_column.name().to_string(), ());
            handled_model.insert(model_column.name, ());
        }

        for (old_name, new_name) in rename_pairs {
            let statement = orm_rename_column_statement(M::table_name(), &old_name, new_name);
            self.execute(&statement, &[])?.done()?;
        }

        for column in table.columns() {
            if handled_current.contains_key(column.name())
                || model_columns.contains_key(column.name())
            {
                continue;
            }
            if column.desc().is_primary() {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration cannot drop the primary key column `{}` from table `{}`",
                    column.name(),
                    M::table_name(),
                )));
            }

            let statement = orm_drop_column_statement(M::table_name(), column.name());
            self.execute(&statement, &[])?.done()?;
        }

        for column in columns {
            if handled_model.contains_key(column.name) || current_columns.contains_key(column.name)
            {
                continue;
            }
            if column.primary_key {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration cannot add a new primary key column `{}` to an existing table `{}`",
                    column.name,
                    M::table_name(),
                )));
            }

            let statement = orm_add_column_statement(M::table_name(), column)?;
            self.execute(&statement, &[])?.done()?;
        }

        for statement in M::create_index_if_not_exists_statements() {
            self.execute(statement, &[])?.done()?;
        }

        Ok(())
    }

    /// Drops a non-primary-key model index by name.
    ///
    /// Primary-key indexes are managed by the table definition itself and
    /// cannot be dropped independently.
    pub fn drop_index<M: Model>(&self, index_name: &str) -> Result<(), DatabaseError> {
        let statement = orm_drop_index_statement(M::table_name(), index_name, false);

        self.execute(&statement, &[])?.done()
    }

    /// Drops a non-primary-key model index by name if it exists.
    pub fn drop_index_if_exists<M: Model>(&self, index_name: &str) -> Result<(), DatabaseError> {
        let statement = orm_drop_index_statement(M::table_name(), index_name, true);

        self.execute(&statement, &[])?.done()
    }

    /// Drops the model table.
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
    /// database.drop_table::<User>().unwrap();
    /// ```
    pub fn drop_table<M: Model>(&self) -> Result<(), DatabaseError> {
        self.execute(M::drop_table_statement(), &[])?.done()
    }

    /// Drops the model table if it exists.
    ///
    /// This variant is convenient for cleanup code that should succeed even if
    /// the table was already removed.
    pub fn drop_table_if_exists<M: Model>(&self) -> Result<(), DatabaseError> {
        self.execute(M::drop_table_if_exists_statement(), &[])?
            .done()
    }
}
