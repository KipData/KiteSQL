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
    pub fn create_table<M: Model>(&mut self) -> Result<(), DatabaseError> {
        execute_create_table::<_, M>(self, false)?;

        for index in M::indexes() {
            execute_create_index(self, M::table_name(), index, false)?;
        }

        Ok(())
    }

    /// Creates the model table if it does not already exist.
    ///
    /// This is useful for examples, tests and bootstrap flows where rerunning
    /// schema initialization should stay idempotent. Secondary indexes declared
    /// with `#[model(index)]` are created with `IF NOT EXISTS` as well.
    pub fn create_table_if_not_exists<M: Model>(&mut self) -> Result<(), DatabaseError> {
        execute_create_table::<_, M>(self, true)?;

        for index in M::indexes() {
            execute_create_index(self, M::table_name(), index, true)?;
        }

        Ok(())
    }

    /// Truncates the model table.
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
    /// database.truncate::<User>().unwrap();
    /// assert_eq!(database.fetch::<User>().unwrap().count(), 0);
    /// ```
    pub fn truncate<M: Model>(&self) -> Result<(), DatabaseError> {
        self.bind(|ctx| ctx.truncate::<M>())?.done()
    }

    /// Creates a view from a binder-backed ORM plan builder.
    pub fn create_view<F>(&mut self, view_name: &str, build: F) -> Result<(), DatabaseError>
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
        execute_create_view(self, view_name, build, false)
    }

    /// Creates or replaces a view from a binder-backed ORM plan builder.
    pub fn create_or_replace_view<F>(
        &mut self,
        view_name: &str,
        build: F,
    ) -> Result<(), DatabaseError>
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
        execute_create_view(self, view_name, build, true)
    }

    /// Drops a view by name.
    pub fn drop_view(&mut self, view_name: &str) -> Result<(), DatabaseError> {
        execute_drop_view(self, view_name, false)
    }

    /// Drops a view by name if it exists.
    pub fn drop_view_if_exists(&mut self, view_name: &str) -> Result<(), DatabaseError> {
        execute_drop_view(self, view_name, true)
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
    pub fn migrate<M: Model>(&mut self) -> Result<(), DatabaseError> {
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
        let (table_primary_key, current_columns) = {
            let table_arena = self.state.table_arena().borrow();
            let table_primary_key = table
                .primary_keys()
                .first()
                .map(|(_, column)| table_arena.column(*column).clone())
                .ok_or(DatabaseError::PrimaryKeyNotFound)?;
            let current_columns = table
                .columns()
                .map(|column| {
                    let column = table_arena.column(*column).clone();
                    (column.name().to_string(), column)
                })
                .collect::<BTreeMap<_, _>>();
            (table_primary_key, current_columns)
        };

        let model_primary_key = columns
            .iter()
            .find(|column| column.desc().is_primary())
            .ok_or(DatabaseError::PrimaryKeyNotFound)?;
        if table_primary_key.name() != model_primary_key.name()
            || !model_column_matches_catalog(model_primary_key, &table_primary_key)?
        {
            return Err(DatabaseError::InvalidValue(::std::format!(
                "ORM migration does not support changing the primary key for table `{}`",
                M::table_name(),
            )));
        }
        let model_columns = columns
            .iter()
            .map(|column| (column.name(), column))
            .collect::<BTreeMap<_, _>>();
        let mut handled_current = BTreeMap::new();
        let mut handled_model = BTreeMap::new();

        for column in columns {
            let Some(current_column) = current_columns.get(column.name()) else {
                continue;
            };
            handled_current.insert(current_column.name().to_string(), ());
            handled_model.insert(column.name(), ());

            if column.desc().is_primary() != current_column.desc().is_primary() {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration does not support changing the primary key for table `{}`",
                    M::table_name(),
                )));
            }
            if column.desc().is_unique() != current_column.desc().is_unique() {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration cannot automatically change unique constraint on column `{}` of table `{}`",
                    column.name(),
                    M::table_name(),
                )));
            }
            if model_column_matches_catalog(column, current_column)? {
                continue;
            }

            if !model_column_type_matches_catalog(column, current_column) {
                execute_change_column(
                    self,
                    M::table_name(),
                    column.name(),
                    column.name(),
                    column.datatype().clone(),
                    DefaultChange::NoChange,
                    NotNullChange::NoChange,
                )?;
            }

            if model_column_default(column)? != catalog_column_default(current_column)? {
                execute_change_column(
                    self,
                    M::table_name(),
                    column.name(),
                    column.name(),
                    column.datatype().clone(),
                    match column.desc().default.clone() {
                        Some(expr) => DefaultChange::Set(expr),
                        None => DefaultChange::Drop,
                    },
                    NotNullChange::NoChange,
                )?;
            }

            if column.nullable() != current_column.nullable() {
                execute_change_column(
                    self,
                    M::table_name(),
                    column.name(),
                    column.name(),
                    column.datatype().clone(),
                    DefaultChange::NoChange,
                    if column.nullable() {
                        NotNullChange::Drop
                    } else {
                        NotNullChange::Set
                    },
                )?;
            }
        }

        let mut rename_pairs = Vec::new();
        let unmatched_model_columns = columns
            .iter()
            .filter(|column| !handled_model.contains_key(column.name()))
            .collect::<Vec<_>>();
        let unmatched_current_columns = current_columns
            .values()
            .filter(|column| !handled_current.contains_key(column.name()))
            .cloned()
            .collect::<Vec<_>>();

        for model_column in &unmatched_model_columns {
            if model_column.desc().is_primary() {
                continue;
            }
            let mut candidates = Vec::new();
            for column in unmatched_current_columns
                .iter()
                .filter(|column| !column.desc().is_primary())
            {
                if model_column_rename_compatible(model_column, column)? {
                    candidates.push(column);
                }
            }
            if candidates.len() != 1 {
                continue;
            }
            let current_column = candidates[0];
            let mut reverse_candidates = Vec::new();
            for other in unmatched_model_columns
                .iter()
                .filter(|other| !other.desc().is_primary())
            {
                if model_column_rename_compatible(other, current_column)? {
                    reverse_candidates.push(other);
                }
            }
            if reverse_candidates.len() != 1 {
                continue;
            }
            rename_pairs.push((current_column.name().to_string(), model_column.name()));
            handled_current.insert(current_column.name().to_string(), ());
            handled_model.insert(model_column.name(), ());
        }

        for (old_name, new_name) in rename_pairs {
            let current_column = current_columns
                .get(&old_name)
                .ok_or_else(|| DatabaseError::column_not_found(old_name.clone()))?;
            execute_change_column(
                self,
                M::table_name(),
                &old_name,
                new_name,
                current_column.datatype().clone(),
                DefaultChange::NoChange,
                NotNullChange::NoChange,
            )?;
        }

        for column in current_columns.values() {
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

            execute_drop_column(self, M::table_name(), column.name())?;
        }

        for column in columns {
            if handled_model.contains_key(column.name())
                || current_columns.contains_key(column.name())
            {
                continue;
            }
            if column.desc().is_primary() {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration cannot add a new primary key column `{}` to an existing table `{}`",
                    column.name(),
                    M::table_name(),
                )));
            }

            execute_add_column(self, M::table_name(), column)?;
        }

        for index in M::indexes() {
            execute_create_index(self, M::table_name(), index, true)?;
        }

        Ok(())
    }

    /// Drops a non-primary-key model index by name.
    ///
    /// Primary-key indexes are managed by the table definition itself and
    /// cannot be dropped independently.
    pub fn drop_index<M: Model>(&mut self, index_name: &str) -> Result<(), DatabaseError> {
        execute_drop_index(self, M::table_name(), index_name, false)
    }

    /// Drops a non-primary-key model index by name if it exists.
    pub fn drop_index_if_exists<M: Model>(
        &mut self,
        index_name: &str,
    ) -> Result<(), DatabaseError> {
        execute_drop_index(self, M::table_name(), index_name, true)
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
    pub fn drop_table<M: Model>(&mut self) -> Result<(), DatabaseError> {
        execute_drop_table(self, M::table_name(), false)
    }

    /// Drops the model table if it exists.
    ///
    /// This variant is convenient for cleanup code that should succeed even if
    /// the table was already removed.
    pub fn drop_table_if_exists<M: Model>(&mut self) -> Result<(), DatabaseError> {
        execute_drop_table(self, M::table_name(), true)
    }
}

fn execute_create_table<S: Storage, M: Model>(
    database: &mut Database<S>,
    if_not_exists: bool,
) -> Result<(), DatabaseError> {
    let columns = M::columns().to_vec();
    database.execute_mut("ORM CREATE TABLE", &[], move |binder, _| {
        binder.bind_create_table(M::table_name().into(), columns, if_not_exists)
    })
}

fn execute_create_index<S: Storage>(
    database: &mut Database<S>,
    table_name: &'static str,
    index: &(&'static str, &'static [&'static str], bool),
    if_not_exists: bool,
) -> Result<(), DatabaseError> {
    let (index_name, index_columns, unique) = *index;
    let index_name = index_name.to_string();
    let column_names = index_columns.to_vec();
    database.execute_mut("ORM CREATE INDEX", &[], move |binder, arena| {
        let mut input = binder.bind_create_index_source(table_name.into(), arena)?;
        let schema = input.output_schema(arena).clone();
        let mut columns = Vec::with_capacity(column_names.len());
        for column_name in column_names {
            let column = schema
                .iter()
                .copied()
                .find(|column| arena.column(*column).name() == column_name)
                .ok_or_else(|| DatabaseError::column_not_found(column_name.to_string()))?;
            columns.push(column);
        }
        binder.bind_create_index(
            table_name.into(),
            index_name,
            columns,
            if_not_exists,
            unique,
            input,
        )
    })
}

fn execute_create_view<S: Storage, F>(
    database: &mut Database<S>,
    view_name: &str,
    build: F,
    or_replace: bool,
) -> Result<(), DatabaseError>
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
    static EMPTY_ORM_PARAMS: &[(&str, DataValue)] = &[];
    let view_name = view_name.to_string();
    database.execute_mut("ORM CREATE VIEW", EMPTY_ORM_PARAMS, move |binder, arena| {
        let mut context = OrmContext { binder, arena };
        let plan = build(&mut context)?;
        binder.bind_create_view(
            view_name.as_str().into(),
            or_replace,
            plan,
            vec![],
            vec![],
            arena,
        )
    })
}

fn execute_drop_view<S: Storage>(
    database: &mut Database<S>,
    view_name: &str,
    if_exists: bool,
) -> Result<(), DatabaseError> {
    let view_name = view_name.to_string();
    database.execute_mut("ORM DROP VIEW", &[], move |binder, _| {
        binder.bind_drop_view(view_name.as_str().into(), if_exists)
    })
}

fn execute_change_column<S: Storage>(
    database: &mut Database<S>,
    table_name: &'static str,
    old_column_name: &str,
    new_column_name: &str,
    data_type: LogicalType,
    default_change: DefaultChange,
    not_null_change: NotNullChange,
) -> Result<(), DatabaseError> {
    let old_column_name = old_column_name.to_string();
    let new_column_name = new_column_name.to_string();
    database.execute_mut("ORM CHANGE COLUMN", &[], move |binder, _| {
        binder.bind_change_column(
            table_name.into(),
            old_column_name,
            new_column_name,
            data_type,
            default_change,
            not_null_change,
        )
    })
}

fn execute_drop_column<S: Storage>(
    database: &mut Database<S>,
    table_name: &'static str,
    column_name: &str,
) -> Result<(), DatabaseError> {
    let column_name = column_name.to_string();
    database.execute_mut("ORM DROP COLUMN", &[], move |binder, _| {
        binder.bind_drop_column(table_name.into(), column_name, false)
    })
}

fn execute_add_column<S: Storage>(
    database: &mut Database<S>,
    table_name: &'static str,
    column: &ColumnCatalog,
) -> Result<(), DatabaseError> {
    let column = column.clone();
    database.execute_mut("ORM ADD COLUMN", &[], move |binder, _| {
        binder.bind_add_column(table_name.into(), column, false)
    })
}

fn execute_drop_index<S: Storage>(
    database: &mut Database<S>,
    table_name: &'static str,
    index_name: &str,
    if_exists: bool,
) -> Result<(), DatabaseError> {
    let index_name = index_name.to_string();
    database.execute_mut("ORM DROP INDEX", &[], move |binder, _| {
        binder.bind_drop_index(table_name.into(), index_name, if_exists)
    })
}

fn execute_drop_table<S: Storage>(
    database: &mut Database<S>,
    table_name: &'static str,
    if_exists: bool,
) -> Result<(), DatabaseError> {
    database.execute_mut("ORM DROP TABLE", &[], move |binder, _| {
        binder.bind_drop_table(table_name.into(), if_exists)
    })
}
