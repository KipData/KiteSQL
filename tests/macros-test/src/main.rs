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

fn main() {}

#[cfg(test)]
mod test {
    use kite_sql::catalog::column::{ColumnCatalog, ColumnDesc};
    use kite_sql::db::{DataBaseBuilder, Database, ResultIter};
    use kite_sql::errors::DatabaseError;
    use kite_sql::expression::function::scala::ScalarFunctionImpl;
    use kite_sql::expression::function::table::TableFunctionImpl;
    use kite_sql::expression::function::FunctionSummary;
    use kite_sql::expression::BinaryOperator;
    use kite_sql::expression::ScalarExpression;
    use kite_sql::orm::OrmQueryResultExt;
    use kite_sql::planner::{MetaArena, PlanArena, TableArena, TableArenaCell};
    use kite_sql::storage::rocksdb::RocksStorage;
    use kite_sql::types::evaluator::binary_create;
    use kite_sql::types::tuple::{Schema, SchemaView, Tuple};
    use kite_sql::types::value::{DataValue, Utf8Type};
    use kite_sql::types::{CharLengthUnits, LogicalType};
    use kite_sql::{from_tuple, scala_function, table_function, Model, Projection};
    use rust_decimal::Decimal;
    use tempfile::TempDir;

    fn build_tuple(arena: &mut impl MetaArena) -> (Tuple, Schema) {
        let schema = vec![
            arena.alloc_column(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            )),
            arena.alloc_column(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Varchar(None, CharLengthUnits::Characters),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            )),
        ];
        let values = vec![
            DataValue::Int32(9),
            DataValue::Utf8 {
                value: "LOL".to_string(),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            },
        ];

        (Tuple::new(None, values), schema)
    }

    fn tuple_owned(tuple: &Tuple) -> Tuple {
        Tuple {
            pk: tuple.pk.clone(),
            values: tuple.values.clone(),
        }
    }

    fn collect_result_tuples<I: ResultIter>(mut iter: I) -> Result<Vec<Tuple>, DatabaseError> {
        let mut rows = Vec::new();
        while let Some(row) = iter.next_tuple(|_, tuple| tuple_owned(tuple))? {
            rows.push(row);
        }
        iter.done()?;
        Ok(rows)
    }

    fn drain_result_iter<I: ResultIter>(mut iter: I) -> Result<(), DatabaseError> {
        while iter.next_tuple(|_, _| ())?.is_some() {}
        iter.done()
    }

    fn has_result_row<I: ResultIter>(mut iter: I) -> Result<bool, DatabaseError> {
        let exists = iter.next_tuple(|_, _| ())?.is_some();
        iter.done()?;
        Ok(exists)
    }

    fn build_test_database() -> Result<(TempDir, Database<RocksStorage>), DatabaseError> {
        let temp_dir = TempDir::new().expect("create temp dir for ORM test");
        let database = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        Ok((temp_dir, database))
    }

    #[test]
    fn test_from_data_value_reports_conversion_error() {
        let err = <i32 as kite_sql::orm::FromDataValue>::from_data_value(DataValue::Utf8 {
            value: "not an integer".to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        })
        .expect_err("converting utf8 directly into i32 should fail");

        assert!(matches!(
            err,
            DatabaseError::InvalidValue(message)
                if message.contains("Varchar") && message.contains("i32")
        ));
    }

    fn create_model_table<M: kite_sql::orm::Model>(
        database: &mut Database<RocksStorage>,
    ) -> Result<(), DatabaseError> {
        database.create_table::<M>()
    }

    fn create_model_table_if_not_exists<M: kite_sql::orm::Model>(
        database: &mut Database<RocksStorage>,
    ) -> Result<(), DatabaseError> {
        database.create_table_if_not_exists::<M>()
    }

    fn migrate_model<M: kite_sql::orm::Model>(
        database: &mut Database<RocksStorage>,
    ) -> Result<(), DatabaseError> {
        database.migrate::<M>()
    }

    fn drop_model_index<M: kite_sql::orm::Model>(
        database: &mut Database<RocksStorage>,
        index_name: &str,
    ) -> Result<(), DatabaseError> {
        database.drop_index::<M>(index_name)
    }

    fn drop_model_index_if_exists<M: kite_sql::orm::Model>(
        database: &mut Database<RocksStorage>,
        index_name: &str,
    ) -> Result<(), DatabaseError> {
        database.drop_index_if_exists::<M>(index_name)
    }

    #[derive(Default, Debug, PartialEq)]
    struct MyStruct {
        c1: i32,
        c2: String,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "derived_struct")]
    struct DerivedStruct {
        #[model(primary_key)]
        c1: i32,
        #[model(rename = "c2")]
        name: String,
        age: Option<i32>,
        #[model(skip)]
        skipped: String,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "users")]
    #[model(index(name = "users_name_age_index", columns = "name, age"))]
    struct User {
        #[model(primary_key)]
        id: i32,
        #[model(rename = "user_name", unique, varchar = 32)]
        name: String,
        #[model(default = "18", index)]
        age: Option<i32>,
        #[model(skip)]
        cache: String,
    }

    #[derive(Debug, PartialEq, Model)]
    #[model(table = "no_default_users")]
    struct NoDefaultUser {
        #[model(primary_key)]
        id: i32,
        name: String,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "wallets")]
    struct Wallet {
        #[model(primary_key)]
        id: i32,
        #[model(decimal_precision = 10, decimal_scale = 2)]
        balance: Decimal,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "country_codes")]
    struct CountryCode {
        #[model(primary_key)]
        id: i32,
        #[model(char = 2)]
        code: String,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "user_name_snapshots")]
    struct UserNameSnapshot {
        #[model(primary_key)]
        id: i32,
        #[model(rename = "user_name", varchar = 32)]
        name: String,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "event_logs")]
    struct EventLog {
        #[model(primary_key)]
        id: i32,
        category: String,
        score: i32,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "orders")]
    struct Order {
        #[model(primary_key)]
        id: i32,
        user_id: i32,
        amount: i32,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "archived_users")]
    struct ArchivedUser {
        #[model(primary_key)]
        id: i32,
        #[model(rename = "user_name", unique, varchar = 32)]
        name: String,
        #[model(default = "18")]
        age: Option<i32>,
        #[model(skip)]
        cache: String,
    }

    #[derive(Default, Debug, PartialEq, Projection)]
    struct UserSummary {
        id: i32,
        #[projection(rename = "user_name")]
        display_name: String,
        age: Option<i32>,
    }

    #[derive(Debug, PartialEq, Projection)]
    struct InvalidUserProjection {
        #[projection(rename = "user_name")]
        name: RejectDataValue,
    }

    #[derive(Debug, PartialEq)]
    struct RejectDataValue;

    impl kite_sql::orm::FromDataValue for RejectDataValue {
        fn logical_type() -> Option<LogicalType> {
            Some(LogicalType::Varchar(None, CharLengthUnits::Characters))
        }

        fn from_data_value(_: DataValue) -> Result<Self, DatabaseError> {
            Err(DatabaseError::InvalidValue(
                "rejecting value for test".to_string(),
            ))
        }
    }

    #[derive(Default, Debug, PartialEq, Projection)]
    struct UserOrderSummary {
        #[projection(from = "users", rename = "user_name")]
        display_name: String,
        #[projection(from = "orders")]
        amount: i32,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "migrating_users")]
    struct MigratingUserV1 {
        #[model(primary_key)]
        id: i32,
        name: String,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "migrating_users")]
    struct MigratingUserV2 {
        #[model(primary_key)]
        id: i32,
        name: String,
        #[model(default = "18")]
        age: i32,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "migrating_users")]
    struct MigratingUserV3 {
        #[model(primary_key)]
        id: i32,
        #[model(default = "18")]
        age: i32,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "migrating_users")]
    struct MigratingUserV4 {
        #[model(primary_key)]
        id: i32,
        #[model(default = "18")]
        years: i32,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "migrating_users")]
    struct MigratingUserV5 {
        #[model(primary_key)]
        id: i32,
        years: Option<String>,
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "migrating_users")]
    struct MigratingUserV6 {
        #[model(primary_key)]
        id: i32,
        #[model(unique)]
        years: Option<String>,
    }

    from_tuple!(
        MyStruct, (
            c1: i32 => |inner: &mut MyStruct, value| {
                if let DataValue::Int32(val) = value {
                    inner.c1 = val;
                }
            },
            c2: String => |inner: &mut MyStruct, value| {
                if let DataValue::Utf8 { value, .. } = value {
                    inner.c2 = value;
                }
            }
        )
    );

    #[test]
    fn test_from_tuple() {
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let (mut tuple, schema) = build_tuple(&mut plan_arena);
        let schema = SchemaView::new(&schema, &plan_arena);
        let my_struct =
            <MyStruct as kite_sql::orm::FromQueryRow>::from_query_row(&schema, &mut tuple).unwrap();

        println!("{:?}", my_struct);

        assert_eq!(my_struct.c1, 9);
        assert_eq!(my_struct.c2, "LOL");
    }

    #[test]
    fn test_model_mapping() {
        assert_eq!(
            <DerivedStruct as kite_sql::orm::Model>::fields()
                .iter()
                .map(|field| (field.column, field.column_index))
                .collect::<Vec<_>>(),
            vec![("c1", 0), ("c2", 1), ("age", 2)]
        );

        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let (_, mut schema) = build_tuple(&mut plan_arena);
        schema.push(plan_arena.alloc_column(ColumnCatalog::new(
            "age".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, true, None).unwrap(),
        )));
        let mut tuple = Tuple::new(
            None,
            vec![
                DataValue::Int32(9),
                DataValue::Utf8 {
                    value: "LOL".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                DataValue::Null,
            ],
        );

        let schema = SchemaView::new(&schema, &plan_arena);
        let derived =
            <DerivedStruct as kite_sql::orm::FromQueryRow>::from_query_row(&schema, &mut tuple)
                .unwrap();

        assert_eq!(derived.c1, 9);
        assert_eq!(derived.name, "LOL");
        assert_eq!(derived.age, None);
        assert_eq!(derived.skipped, "");
    }

    #[test]
    fn test_model_decode_does_not_require_default() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;

        create_model_table::<NoDefaultUser>(&mut database)?;
        database.insert(&NoDefaultUser {
            id: 1,
            name: "Alice".to_string(),
        })?;

        assert_eq!(
            database.get::<NoDefaultUser>(&1)?,
            Some(NoDefaultUser {
                id: 1,
                name: "Alice".to_string(),
            })
        );

        Ok(())
    }

    #[test]
    fn test_result_iter_to_orm_iter() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;

        database.ddl("create table users (c1 int primary key, c2 varchar, age int)")?;
        database
            .run("insert into users values (1, 'Alice', 18), (2, 'Bob', null)")?
            .done()?;

        let users = database
            .run("select c1, c2, age from users order by c1")?
            .orm::<DerivedStruct>()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(
            users,
            vec![
                DerivedStruct {
                    c1: 1,
                    name: "Alice".to_string(),
                    age: Some(18),
                    skipped: "".to_string(),
                },
                DerivedStruct {
                    c1: 2,
                    name: "Bob".to_string(),
                    age: None,
                    skipped: "".to_string(),
                }
            ]
        );

        Ok(())
    }

    #[test]
    fn test_model_decimal_ddl() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;

        create_model_table::<Wallet>(&mut database)?;
        for id in 1..=101 {
            database.insert(&Wallet {
                id,
                balance: Decimal::new((id * 100) as i64, 2),
            })?;
        }
        database.analyze_model::<Wallet>()?;

        let rows = collect_result_tuples(database.run("describe wallets")?)?;

        let balance = rows
            .iter()
            .find(|row| match &row.values[0] {
                DataValue::Utf8 { value, .. } => value == "balance",
                _ => false,
            })
            .expect("balance column should exist");

        match &balance.values[1] {
            DataValue::Utf8 { value, .. } => assert_eq!(value, "Decimal(Some(10), Some(2))"),
            other => panic!("unexpected describe datatype: {other:?}"),
        }

        database.drop_table::<Wallet>()?;

        Ok(())
    }

    #[test]
    fn test_model_char_ddl() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;

        create_model_table::<CountryCode>(&mut database)?;

        let rows = collect_result_tuples(database.run("describe country_codes")?)?;

        let code = rows
            .iter()
            .find(|row| match &row.values[0] {
                DataValue::Utf8 { value, .. } => value == "code",
                _ => false,
            })
            .expect("code column should exist");

        match &code.values[1] {
            DataValue::Utf8 { value, .. } => assert_eq!(value, "Char(2, CHARACTERS)"),
            other => panic!("unexpected describe datatype: {other:?}"),
        }

        database.drop_table::<CountryCode>()?;

        Ok(())
    }

    #[test]
    fn test_model_migrate() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;

        create_model_table::<MigratingUserV1>(&mut database)?;
        database.insert(&MigratingUserV1 {
            id: 1,
            name: "Alice".to_string(),
        })?;

        migrate_model::<MigratingUserV2>(&mut database)?;
        assert_eq!(database.get::<MigratingUserV2>(&1)?.unwrap().age, 18);

        migrate_model::<MigratingUserV3>(&mut database)?;
        assert_eq!(
            database.get::<MigratingUserV3>(&1)?,
            Some(MigratingUserV3 { id: 1, age: 18 })
        );

        let describe_rows = collect_result_tuples(database.run("describe migrating_users")?)?;
        let column_names = describe_rows
            .iter()
            .filter_map(|row| match row.values.first() {
                Some(DataValue::Utf8 { value, .. }) => Some(value.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(column_names, vec!["id", "age"]);

        migrate_model::<MigratingUserV4>(&mut database)?;
        assert_eq!(
            database.get::<MigratingUserV4>(&1)?,
            Some(MigratingUserV4 { id: 1, years: 18 })
        );

        migrate_model::<MigratingUserV5>(&mut database)?;
        assert_eq!(
            database.get::<MigratingUserV5>(&1)?,
            Some(MigratingUserV5 {
                id: 1,
                years: Some("18".to_string()),
            })
        );
        database.insert(&MigratingUserV5 { id: 2, years: None })?;
        assert_eq!(
            database.get::<MigratingUserV5>(&2)?,
            Some(MigratingUserV5 { id: 2, years: None })
        );

        let incompatible = database.migrate::<MigratingUserV6>();
        assert!(matches!(incompatible, Err(DatabaseError::InvalidValue(_))));

        database.drop_table::<MigratingUserV5>()?;

        Ok(())
    }

    #[test]
    fn test_orm_query_builder() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("create temp dir for ORM test");
        let mut database = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        database.load(kite_sql::db::CatalogKind::ScalarFunction(
            MyOrmFunction::new(),
        ))?;

        create_model_table::<User>(&mut database)?;
        drop_model_index::<User>(&mut database, "users_age_index")?;
        database.insert(&User {
            id: 1,
            name: "Alice".to_string(),
            age: Some(18),
            cache: "".to_string(),
        })?;
        database.insert(&User {
            id: 2,
            name: "Bob".to_string(),
            age: Some(30),
            cache: "".to_string(),
        })?;
        database.insert(&User {
            id: 3,
            name: "A'lex".to_string(),
            age: None,
            cache: "".to_string(),
        })?;

        create_model_table::<Order>(&mut database)?;
        database.insert(&Order {
            id: 1,
            user_id: 1,
            amount: 100,
        })?;
        database.insert(&Order {
            id: 2,
            user_id: 1,
            amount: 200,
        })?;
        database.insert(&Order {
            id: 3,
            user_id: 2,
            amount: 300,
        })?;

        create_model_table::<Wallet>(&mut database)?;
        database.insert(&Wallet {
            id: 1,
            balance: Decimal::new(5000, 2),
        })?;
        database.insert(&Wallet {
            id: 3,
            balance: Decimal::new(1250, 2),
        })?;
        database.insert(&Wallet {
            id: 4,
            balance: Decimal::new(9999, 2),
        })?;

        let adults = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let adult = e.column(User::age())?.gte(18)?;
                        let a_prefix = e.column(User::name())?.like("A%")?;
                        adult.and(a_prefix)
                    })?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(adults.len(), 1);
        assert_eq!(adults[0].name, "Alice");

        let adult_projection = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let adult = e.column(User::age())?.gte(18)?;
                        let a_prefix = e.column(User::name())?.like("A%")?;
                        adult.and(a_prefix)
                    })?
                    .order_by(User::age().desc())?
                    .project_scalars((User::id(), User::name()))?
                    .finish()
            })?
            .project_tuple::<(i32, String)>()
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        assert_eq!(adult_projection, vec![(1, "Alice".to_string())]);

        let joined_amounts = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .inner_join_as::<Order, _>("o", |e| {
                        e.column(User::id())?
                            .eq(e.qualified_column("o", Order::user_id())?)
                    })?
                    .project_tuple(|e| {
                        let name = e.column(User::name())?;
                        let amount = e.qualified_column("o", Order::amount())?;
                        Ok(vec![name, amount])
                    })?
                    .order_by_expr(|e| Ok(e.qualified_column("o", Order::id())?.asc()))?
                    .finish()
            })?
            .project_tuple::<(String, i32)>()
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        assert_eq!(
            joined_amounts,
            vec![
                ("Alice".to_string(), 100),
                ("Alice".to_string(), 200),
                ("Bob".to_string(), 300),
            ]
        );

        let union_ids = database
            .bind(|ctx| {
                ctx.union(
                    true,
                    |ctx| ctx.from::<User>()?.project_scalar(User::id())?.finish(),
                    |ctx| {
                        ctx.from::<Order>()?
                            .project_scalar(Order::user_id())?
                            .finish()
                    },
                )
            })?
            .project_value::<i32>()
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        assert_eq!(union_ids, vec![1, 2, 3, 1, 1, 2]);

        let quoted = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| e.column(User::name())?.eq("A'lex"))?
                    .finish()
            })?
            .orm::<User>()
            .next()
            .transpose()?;
        assert_eq!(quoted.unwrap().id, 3);

        let ordered = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| Ok(e.column(User::age())?.is_not_null()))?
                    .order_by(User::age().desc())?
                    .limit(1)?
                    .finish()
            })?
            .orm::<User>()
            .next()
            .transpose()?
            .unwrap();
        assert_eq!(ordered.id, 2);

        let count = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| Ok(e.column(User::age())?.is_not_null()))?
                    .count()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?
            .unwrap() as usize;
        assert_eq!(count, 2);

        let exists = has_result_row(database.bind(|ctx| {
            ctx.from::<User>()?
                .filter(|e| e.column(User::id())?.eq(2))?
                .exists()
        })?)?;
        assert!(exists);
        let missing = has_result_row(database.bind(|ctx| {
            ctx.from::<User>()?
                .filter(|e| e.column(User::id())?.eq(99))?
                .exists()
        })?)?;
        assert!(!missing);

        let two_users = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        let eq_one = id.clone().eq(1)?;
                        let eq_two = id.eq(2)?;
                        eq_one.or(eq_two)
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            two_users.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 2]
        );

        assert_eq!(
            database
                .bind(|ctx| {
                    ctx.from::<User>()?
                        .filter(|e| {
                            let age_present = e.column(User::age())?.is_not_null();
                            let not_b = e.column(User::name())?.not_like("B%")?;
                            age_present.and(not_b)
                        })?
                        .count()
                })?
                .project_value::<i32>()
                .next()
                .transpose()?
                .unwrap() as usize,
            1
        );

        let in_list = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| e.column(User::id())?.in_list([1, 3]))?
                    .order_by(User::id())?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            in_list.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 3]
        );

        let either_named_a_or_missing_age = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let a_name = e.column(User::name())?.like("A%")?;
                        let missing_age = e.column(User::age())?.is_null();
                        a_name.or(missing_age)
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            either_named_a_or_missing_age
                .iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );

        let query_value_function_matched = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        let add_one = e.function("add_one", vec![id])?;
                        add_one.eq(3)
                    })?
                    .finish()
            })?
            .orm::<User>()
            .next()
            .transpose()?
            .unwrap();
        assert_eq!(query_value_function_matched.id, 2);

        let cast_to_matched = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        let cast_id = e.cast(id, LogicalType::Bigint)?;
                        cast_id.eq(3_i64)
                    })?
                    .finish()
            })?
            .orm::<User>()
            .next()
            .transpose()?
            .unwrap();
        assert_eq!(cast_to_matched.id, 3);

        let add_matched = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        let add_one = e.binary(id, BinaryOperator::Plus, e.value(1))?;
                        add_one.eq(3)
                    })?
                    .finish()
            })?
            .orm::<User>()
            .next()
            .transpose()?
            .unwrap();
        assert_eq!(add_matched.id, 2);

        let arithmetic_projection = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_tuple(|e| {
                        let id = e.column(User::id())?;
                        let times_ten =
                            e.binary(id.clone(), BinaryOperator::Multiply, e.value(10))?;
                        let half_id = e.binary(id.clone(), BinaryOperator::Divide, e.value(2))?;
                        let id_mod_2 = e.binary(id.clone(), BinaryOperator::Modulo, e.value(2))?;
                        Ok(vec![
                            id,
                            e.alias(times_ten, "times_ten"),
                            e.alias(half_id, "half_id"),
                            e.alias(id_mod_2, "id_mod_2"),
                        ])
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .project_tuple::<(i32, i32, i32, i32)>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            arithmetic_projection,
            vec![(1, 10, 0, 1), (2, 20, 1, 0), (3, 30, 1, 1)]
        );

        let projected_name = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| e.column(User::id())?.eq(1))?
                    .project_scalar(User::name())?
                    .finish()
            })?
            .project_value::<String>()
            .next()
            .transpose()?;
        assert_eq!(projected_name.as_deref(), Some("Alice"));

        let age_buckets = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_value(|e| {
                        let age = e.column(User::age())?;
                        Ok(e.case_when(
                            vec![
                                (age.clone().is_null(), e.value("unknown")),
                                (age.lt(20)?, e.value("minor")),
                            ],
                            Some(e.value("adult")),
                        ))
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .project_value::<String>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(age_buckets, vec!["minor", "adult", "unknown"]);

        let simple_case_labels = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_value(|e| {
                        let id = e.column(User::id())?;
                        let label = e.case_value(
                            id,
                            vec![(e.value(1), e.value("one")), (e.value(2), e.value("two"))],
                            Some(e.value("other")),
                        );
                        Ok(e.alias(label, "id_label"))
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .project_value::<String>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(simple_case_labels, vec!["one", "two", "other"]);

        let arithmetic_query_value = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| e.column(User::id())?.eq(1))?
                    .project_value(|e| {
                        let id = e.column(User::id())?;
                        let add_one = e.function("add_one", vec![id])?;
                        let boosted_id = e.binary(add_one, BinaryOperator::Plus, e.value(10))?;
                        Ok(e.alias(boosted_id, "boosted_id"))
                    })?
                    .finish()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?;
        assert_eq!(arithmetic_query_value, Some(12));

        let id_sum = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_value(|e| {
                        let id = e.column(User::id())?;
                        e.aggregate("sum", vec![id])
                    })?
                    .finish()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?;
        assert_eq!(id_sum, Some(6));

        let total_users = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_value(|e| {
                        let count = e.count_all()?;
                        Ok(e.alias(count, "total_users"))
                    })?
                    .finish()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?;
        assert_eq!(total_users, Some(3));

        let min_user_id = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_value(|e| {
                        let id = e.column(User::id())?;
                        let min_id = e.aggregate("min", vec![id])?;
                        Ok(e.alias(min_id, "min_user_id"))
                    })?
                    .finish()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?;
        assert_eq!(min_user_id, Some(1));

        let max_user_id = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_value(|e| {
                        let id = e.column(User::id())?;
                        let max_id = e.aggregate("max", vec![id])?;
                        Ok(e.alias(max_id, "max_user_id"))
                    })?
                    .finish()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?;
        assert_eq!(max_user_id, Some(3));

        let projected_user_rows = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_scalars((User::id(), User::name()))?
                    .order_by(User::id())?
                    .finish()
            })?
            .project_tuple::<(i32, String)>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            projected_user_rows,
            vec![
                (1, "Alice".to_string()),
                (2, "Bob".to_string()),
                (3, "A'lex".to_string()),
            ]
        );

        let udf_projection = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_tuple(|e| {
                        let id = e.column(User::id())?;
                        let next_id = e.function("add_one", vec![id.clone()])?;
                        Ok(vec![id, e.alias(next_id, "next_id")])
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .project_tuple::<(i32, i32)>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(udf_projection, vec![(1, 2), (2, 3), (3, 4)]);

        let udf_projection_schema = database.bind(|ctx| {
            ctx.from::<User>()?
                .project_tuple(|e| {
                    let id = e.column(User::id())?;
                    let next_id = e.function("add_one", vec![id.clone()])?;
                    Ok(vec![id, e.alias(next_id, "next_id")])
                })?
                .order_by(User::id())?
                .finish()
        })?;
        assert_eq!(
            udf_projection_schema.schema(|schema| {
                schema
                    .iter()
                    .map(|column| column.name().to_string())
                    .collect::<Vec<_>>()
            }),
            vec!["id", "next_id"]
        );
        udf_projection_schema.done()?;

        let projected_users = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project::<UserSummary>()?
                    .order_by(User::id())?
                    .finish()
            })?
            .orm::<UserSummary>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            projected_users,
            vec![
                UserSummary {
                    id: 1,
                    display_name: "Alice".to_string(),
                    age: Some(18),
                },
                UserSummary {
                    id: 2,
                    display_name: "Bob".to_string(),
                    age: Some(30),
                },
                UserSummary {
                    id: 3,
                    display_name: "A'lex".to_string(),
                    age: None,
                },
            ]
        );

        let projected_user = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| e.column(User::id())?.eq(1))?
                    .project::<UserSummary>()?
                    .finish()
            })?
            .orm::<UserSummary>()
            .next()
            .transpose()?;
        assert_eq!(
            projected_user,
            Some(UserSummary {
                id: 1,
                display_name: "Alice".to_string(),
                age: Some(18),
            })
        );

        let invalid_projection = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| e.column(User::id())?.eq(1))?
                    .project::<InvalidUserProjection>()?
                    .finish()
            })?
            .orm::<InvalidUserProjection>()
            .next()
            .transpose();
        assert!(matches!(
            invalid_projection,
            Err(DatabaseError::InvalidValue(_))
        ));

        let aliased_total_users = database.bind(|ctx| {
            ctx.from::<User>()?
                .project_value(|e| {
                    let count = e.count_all()?;
                    Ok(e.alias(count, "total_users"))
                })?
                .finish()
        })?;
        aliased_total_users.schema(|schema| {
            assert_eq!(schema.get(0).unwrap().name(), "total_users");
        });
        aliased_total_users.done()?;

        let projected_schema = database.bind(|ctx| {
            ctx.from::<User>()?
                .project_tuple(|e| {
                    let id = e.column(User::id())?;
                    let name = e.column(User::name())?;
                    let age = e.column(User::age())?;
                    Ok(vec![
                        e.alias(id, "id"),
                        e.alias(name, "display_name"),
                        e.alias(age, "age"),
                    ])
                })?
                .finish()
        })?;
        assert_eq!(
            projected_schema.schema(|schema| {
                schema
                    .iter()
                    .map(|column| column.name().to_string())
                    .collect::<Vec<_>>()
            }),
            vec!["id", "display_name", "age"]
        );
        projected_schema.done()?;

        let projected_scalar_subquery = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .order_by(User::id())?
                    .project_value(|e| {
                        e.scalar_subquery(|ctx| {
                            ctx.from::<User>()?
                                .project_value(|e| {
                                    let id = e.column(User::id())?;
                                    e.aggregate("max", vec![id])
                                })?
                                .finish()
                        })
                    })?
                    .finish()
            })?
            .project_value::<i32>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(projected_scalar_subquery, vec![3, 3, 3]);

        assert!(database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_value(|e| {
                        e.scalar_subquery(|ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::user_id())?.eq(e.column(User::id())?))?
                                .project_scalar(Order::amount())?
                                .finish()
                        })
                    })?
                    .finish()
            })
            .is_err());

        assert_eq!(
            database
                .bind(|ctx| {
                    ctx.from::<User>()?
                        .filter(|e| {
                            e.exists_subquery(false, |ctx| {
                                ctx.from::<User>()?
                                    .filter(|e| e.column(User::id())?.eq(2))?
                                    .project_scalar(User::id())?
                                    .finish()
                            })
                        })?
                        .count()
                })?
                .project_value::<i32>()
                .next()
                .transpose()?
                .unwrap() as usize,
            3
        );

        assert_eq!(
            database
                .bind(|ctx| {
                    ctx.from::<User>()?
                        .filter(|e| {
                            e.exists_subquery(true, |ctx| {
                                ctx.from::<User>()?
                                    .filter(|e| e.column(User::id())?.eq(2))?
                                    .project_scalar(User::id())?
                                    .finish()
                            })
                        })?
                        .count()
                })?
                .project_value::<i32>()
                .next()
                .transpose()?
                .unwrap() as usize,
            0
        );

        let in_subquery = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        id.in_subquery(|ctx| {
                            ctx.from::<User>()?
                                .filter(|e| e.column(User::id())?.in_list([1, 3]))?
                                .project_scalar(User::id())?
                                .finish()
                        })
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            in_subquery.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 3]
        );

        let aliased_user = database
            .bind(|ctx| {
                ctx.from_as::<User>("u")?
                    .filter(|e| e.qualified_column("u", User::id())?.eq(2))?
                    .finish()
            })?
            .orm::<User>()
            .next()
            .transpose()?
            .unwrap();
        assert_eq!(aliased_user.name, "Bob");

        let aliased_projection = database
            .bind(|ctx| {
                ctx.from_as::<User>("u")?
                    .filter(|e| e.qualified_column("u", User::id())?.eq(2))?
                    .project::<UserSummary>()?
                    .finish()
            })?
            .orm::<UserSummary>()
            .next()
            .transpose()?;
        assert_eq!(
            aliased_projection,
            Some(UserSummary {
                id: 2,
                display_name: "Bob".to_string(),
                age: Some(30),
            })
        );

        let joined_projection = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .inner_join_as::<Order, _>("o", |e| {
                        e.column(User::id())?
                            .eq(e.qualified_column("o", Order::user_id())?)
                    })?
                    .project_tuple(|e| {
                        let name = e.column(User::name())?;
                        let amount = e.qualified_column("o", Order::amount())?;
                        Ok(vec![name, amount])
                    })?
                    .order_by_expr(|e| Ok(e.qualified_column("o", Order::id())?.asc()))?
                    .finish()
            })?
            .project_tuple::<(String, i32)>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            joined_projection,
            vec![
                ("Alice".to_string(), 100),
                ("Alice".to_string(), 200),
                ("Bob".to_string(), 300),
            ]
        );

        let using_joined_rows = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .inner_join_using::<Wallet>(["id"])?
                    .project_scalars((User::name(), Wallet::balance()))?
                    .order_by(User::id())?
                    .finish()
            })?
            .project_tuple::<(String, Decimal)>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            using_joined_rows,
            vec![
                ("Alice".to_string(), Decimal::new(5000, 2)),
                ("A'lex".to_string(), Decimal::new(1250, 2)),
            ]
        );

        let full_joined_rows = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .full_join_using::<Wallet>(["id"])?
                    .project_scalars((User::id(), Wallet::id()))?
                    .finish()
            })?
            .project_tuple::<(Option<i32>, Option<i32>)>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(full_joined_rows.len(), 4);

        let union_tuple = database
            .bind(|ctx| {
                ctx.union(
                    true,
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .project_scalars((User::id(), User::name()))?
                            .finish()
                    },
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .project_scalars((User::id(), User::name()))?
                            .finish()
                    },
                )
            })?
            .project_tuple::<(i32, String)>()
            .next()
            .transpose()?;
        assert_eq!(union_tuple, Some((2, "Bob".to_string())));

        let mut ordered_union_ids = database
            .bind(|ctx| {
                ctx.union(
                    true,
                    |ctx| ctx.from::<User>()?.project_scalar(User::id())?.finish(),
                    |ctx| {
                        ctx.from::<Order>()?
                            .project_scalar(Order::user_id())?
                            .finish()
                    },
                )
            })?
            .project_value::<i32>()
            .collect::<Result<Vec<_>, _>>()?;
        ordered_union_ids.sort();
        let ordered_union_ids = ordered_union_ids
            .into_iter()
            .skip(1)
            .take(3)
            .collect::<Vec<_>>();
        assert_eq!(ordered_union_ids, vec![1, 1, 2]);

        let mut ordered_customer_ids = database
            .bind(|ctx| {
                ctx.intersect(
                    false,
                    |ctx| ctx.from::<User>()?.project_scalar(User::id())?.finish(),
                    |ctx| {
                        ctx.from::<Order>()?
                            .project_scalar(Order::user_id())?
                            .finish()
                    },
                )
            })?
            .project_value::<i32>()
            .collect::<Result<Vec<_>, _>>()?;
        ordered_customer_ids.sort();
        assert_eq!(ordered_customer_ids, vec![1, 2]);

        let users_without_orders = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        id.in_subquery(|ctx| {
                            ctx.except(
                                false,
                                |ctx| ctx.from::<User>()?.project_scalar(User::id())?.finish(),
                                |ctx| {
                                    ctx.from::<Order>()?
                                        .project_scalar(Order::user_id())?
                                        .finish()
                                },
                            )
                        })
                    })?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            users_without_orders
                .iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            vec![3]
        );

        let left_joined_rows = database
            .bind(|ctx| {
                ctx.from_as::<User>("u")?
                    .left_join_as::<Order, _>("o", |e| {
                        e.qualified_column("u", User::id())?
                            .eq(e.qualified_column("o", Order::user_id())?)
                    })?
                    .project_tuple(|e| {
                        let user_id = e.qualified_column("u", User::id())?;
                        let amount = e.qualified_column("o", Order::amount())?;
                        Ok(vec![
                            e.alias(user_id, "user_id"),
                            e.alias(amount, "order_amount"),
                        ])
                    })?
                    .order_by_expr(|e| Ok(e.qualified_column("u", User::id())?.asc()))?
                    .order_by_expr(|e| Ok(e.qualified_column("o", Order::id())?.asc()))?
                    .finish()
            })?
            .project_tuple::<(i32, Option<i32>)>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            left_joined_rows,
            vec![(1, Some(100)), (1, Some(200)), (2, Some(300)), (3, None)]
        );

        let mut tx = database.new_transaction()?;
        let in_tx = tx
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| e.column(User::id())?.eq(2))?
                    .finish()
            })?
            .orm::<User>()
            .next()
            .transpose()?
            .unwrap();
        assert_eq!(in_tx.name, "Bob");
        tx.commit()?;

        database.drop_table::<Wallet>()?;
        database.drop_table::<Order>()?;
        database.drop_table::<User>()?;

        Ok(())
    }

    #[test]
    fn test_orm_expression_and_set_query_helpers() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;

        create_model_table::<User>(&mut database)?;
        database.insert(&User {
            id: 1,
            name: "Alice".to_string(),
            age: Some(18),
            cache: "".to_string(),
        })?;
        database.insert(&User {
            id: 2,
            name: "Bob".to_string(),
            age: Some(30),
            cache: "".to_string(),
        })?;
        database.insert(&User {
            id: 3,
            name: "Carol".to_string(),
            age: None,
            cache: "".to_string(),
        })?;

        create_model_table::<Order>(&mut database)?;
        database.insert(&Order {
            id: 1,
            user_id: 1,
            amount: 100,
        })?;
        database.insert(&Order {
            id: 2,
            user_id: 1,
            amount: 200,
        })?;
        database.insert(&Order {
            id: 3,
            user_id: 2,
            amount: 300,
        })?;

        let eq_any_users = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        id.in_subquery(|ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::amount())?.eq(300))?
                                .project_scalar(Order::user_id())?
                                .finish()
                        })
                    })?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            eq_any_users.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![2]
        );

        let eq_some_users = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        id.in_subquery(|ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::amount())?.eq(100))?
                                .project_scalar(Order::user_id())?
                                .finish()
                        })
                    })?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            eq_some_users.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1]
        );

        let gt_all_users = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        id.gt_all(|ctx| {
                            ctx.from::<Order>()?
                                .project_scalar(Order::user_id())?
                                .finish()
                        })
                    })?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            gt_all_users.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![3]
        );

        let lt_any_users = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        id.lt_any(|ctx| {
                            ctx.from::<Order>()?
                                .project_scalar(Order::user_id())?
                                .finish()
                        })
                    })?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            lt_any_users.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1]
        );

        let query_value_gt_all_users = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        let add_one = e.binary(id, BinaryOperator::Plus, e.value(1))?;
                        add_one.gt(2)
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            query_value_gt_all_users
                .iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        let exists_count = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        e.exists_subquery(false, |ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::id())?.eq(1))?
                                .project_scalar(Order::id())?
                                .finish()
                        })
                    })?
                    .count()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?
            .unwrap_or(0) as usize;
        assert_eq!(exists_count, 3);

        let not_exists_count = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        e.exists_subquery(true, |ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::id())?.eq(99))?
                                .project_scalar(Order::id())?
                                .finish()
                        })
                    })?
                    .count()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?
            .unwrap() as usize;
        assert_eq!(not_exists_count, 3);

        let blocked_by_not_exists = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        e.exists_subquery(true, |ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::id())?.eq(1))?
                                .project_scalar(Order::id())?
                                .finish()
                        })
                    })?
                    .count()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?
            .unwrap() as usize;
        assert_eq!(blocked_by_not_exists, 0);

        let users_with_orders = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        e.exists_subquery(false, |ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::user_id())?.eq(e.column(User::id())?))?
                                .project_scalar(Order::id())?
                                .finish()
                        })
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            users_with_orders
                .iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let users_without_orders = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        e.exists_subquery(true, |ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::user_id())?.eq(e.column(User::id())?))?
                                .project_scalar(Order::id())?
                                .finish()
                        })
                    })?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            users_without_orders
                .iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            vec![3]
        );

        database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        id.in_subquery(|ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::user_id())?.eq(e.column(User::id())?))?
                                .project_scalar(Order::user_id())?
                                .finish()
                        })
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .done()?;

        database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        id.not_in_subquery(|ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::user_id())?.eq(e.column(User::id())?))?
                                .project_scalar(Order::user_id())?
                                .finish()
                        })
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .done()?;

        database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        id.in_subquery(|ctx| {
                            ctx.union(
                                true,
                                |ctx| {
                                    ctx.from::<Order>()?
                                        .filter(|e| e.column(Order::amount())?.eq(100))?
                                        .project_scalar(Order::user_id())?
                                        .finish()
                                },
                                |ctx| {
                                    ctx.from::<Order>()?
                                        .filter(|e| e.column(Order::amount())?.eq(300))?
                                        .project_scalar(Order::user_id())?
                                        .finish()
                                },
                            )
                        })
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .done()?;

        assert!(database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        e.exists_subquery(false, |ctx| {
                            ctx.union(
                                false,
                                |ctx| {
                                    ctx.from::<Order>()?
                                        .filter(|e| {
                                            e.column(Order::user_id())?.eq(e.column(User::id())?)
                                        })?
                                        .project_scalar(Order::id())?
                                        .finish()
                                },
                                |ctx| {
                                    ctx.from::<Order>()?
                                        .filter(|e| e.column(Order::amount())?.eq(300))?
                                        .project_scalar(Order::id())?
                                        .finish()
                                },
                            )
                        })
                    })?
                    .count()
            })
            .is_err());

        let max_id_user = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        let max_id = e.scalar_subquery(|ctx| {
                            ctx.from::<User>()?
                                .project_value(|e| {
                                    let id = e.column(User::id())?;
                                    e.aggregate("max", vec![id])
                                })?
                                .finish()
                        })?;
                        id.eq(max_id)
                    })?
                    .finish()
            })?
            .orm::<User>()
            .next()
            .transpose()?
            .unwrap();
        assert_eq!(max_id_user.id, 3);

        let union_user = database
            .bind(|ctx| {
                ctx.union(
                    true,
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .finish()
                    },
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .finish()
                    },
                )
            })?
            .orm::<User>()
            .next()
            .transpose()?
            .unwrap();
        assert_eq!(union_user.id, 2);

        let ordered_union_user = database
            .bind(|ctx| {
                ctx.union(
                    false,
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(1))?
                            .finish()
                    },
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .finish()
                    },
                )
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max_by_key(|user| user.id)
            .unwrap();
        assert_eq!(ordered_union_user.id, 2);

        let union_value = database
            .bind(|ctx| {
                ctx.union(
                    true,
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .project_scalar(User::id())?
                            .finish()
                    },
                    |ctx| {
                        ctx.from::<Order>()?
                            .filter(|e| e.column(Order::user_id())?.eq(2))?
                            .project_scalar(Order::user_id())?
                            .finish()
                    },
                )
            })?
            .project_value::<i32>()
            .next()
            .transpose()?;
        assert_eq!(union_value, Some(2));

        let ordered_union_value = database
            .bind(|ctx| {
                ctx.union(
                    true,
                    |ctx| ctx.from::<User>()?.project_scalar(User::id())?.finish(),
                    |ctx| {
                        ctx.from::<Order>()?
                            .project_scalar(Order::user_id())?
                            .finish()
                    },
                )
            })?
            .project_value::<i32>()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max();
        assert_eq!(ordered_union_value, Some(3));

        let union_tuple = database
            .bind(|ctx| {
                ctx.union(
                    true,
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .project_scalars((User::id(), User::name()))?
                            .finish()
                    },
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .project_scalars((User::id(), User::name()))?
                            .finish()
                    },
                )
            })?
            .project_tuple::<(i32, String)>()
            .next()
            .transpose()?;
        assert_eq!(union_tuple, Some((2, "Bob".to_string())));

        let ordered_union_tuple = database
            .bind(|ctx| {
                ctx.union(
                    false,
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(1))?
                            .project_scalars((User::id(), User::name()))?
                            .finish()
                    },
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .project_scalars((User::id(), User::name()))?
                            .finish()
                    },
                )
            })?
            .project_tuple::<(i32, String)>()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max_by_key(|(id, _)| *id);
        assert_eq!(ordered_union_tuple, Some((2, "Bob".to_string())));

        let union_projection = database
            .bind(|ctx| {
                ctx.union(
                    true,
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .project::<UserSummary>()?
                            .finish()
                    },
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .project::<UserSummary>()?
                            .finish()
                    },
                )
            })?
            .orm::<UserSummary>()
            .next()
            .transpose()?;
        assert_eq!(
            union_projection,
            Some(UserSummary {
                id: 2,
                display_name: "Bob".to_string(),
                age: Some(30),
            })
        );

        let ordered_union_projection = database
            .bind(|ctx| {
                ctx.union(
                    false,
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(1))?
                            .project::<UserSummary>()?
                            .finish()
                    },
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(2))?
                            .project::<UserSummary>()?
                            .finish()
                    },
                )
            })?
            .orm::<UserSummary>()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max_by_key(|user| user.id);
        assert_eq!(
            ordered_union_projection,
            Some(UserSummary {
                id: 2,
                display_name: "Bob".to_string(),
                age: Some(30),
            })
        );

        database.drop_table::<Order>()?;
        database.drop_table::<User>()?;

        Ok(())
    }

    #[test]
    fn test_orm_group_by_builder() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;

        create_model_table::<EventLog>(&mut database)?;
        database.insert(&EventLog {
            id: 1,
            category: "alpha".to_string(),
            score: 10,
        })?;
        database.insert(&EventLog {
            id: 2,
            category: "alpha".to_string(),
            score: 20,
        })?;
        database.insert(&EventLog {
            id: 3,
            category: "beta".to_string(),
            score: 5,
        })?;

        let mut grouped_categories = database
            .bind(|ctx| {
                ctx.from::<EventLog>()?
                    .project_scalar(EventLog::category())?
                    .group_by_scalar(EventLog::category())?
                    .finish()
            })?
            .project_value::<String>()
            .collect::<Result<Vec<_>, _>>()?;
        grouped_categories.sort();
        assert_eq!(grouped_categories, vec!["alpha", "beta"]);

        let mut distinct_categories = database
            .bind(|ctx| {
                ctx.from::<EventLog>()?
                    .project_scalar(EventLog::category())?
                    .distinct()?
                    .finish()
            })?
            .project_value::<String>()
            .collect::<Result<Vec<_>, _>>()?;
        distinct_categories.sort();
        assert_eq!(distinct_categories, vec!["alpha", "beta"]);

        let distinct_category_count = database
            .bind(|ctx| {
                ctx.from::<EventLog>()?
                    .project_scalar(EventLog::category())?
                    .distinct()?
                    .count()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?
            .unwrap() as usize;
        assert_eq!(distinct_category_count, 2);

        let distinct_limited_count = database
            .bind(|ctx| {
                ctx.from::<EventLog>()?
                    .project_scalar(EventLog::category())?
                    .distinct()?
                    .limit(1)?
                    .count()
            })?
            .project_value::<i32>()
            .next()
            .transpose()?
            .unwrap() as usize;
        assert_eq!(distinct_limited_count, 1);

        let grouped_count = grouped_categories.len();
        assert_eq!(grouped_count, 2);

        let mut grouped_scores = database
            .bind(|ctx| {
                ctx.from::<EventLog>()?
                    .project_tuple(|e| {
                        let category = e.column(EventLog::category())?;
                        let score = e.column(EventLog::score())?;
                        let total_score = e.aggregate("sum", vec![score])?;
                        Ok(vec![category, e.alias(total_score, "total_score")])
                    })?
                    .group_by_scalar(EventLog::category())?
                    .finish()
            })?
            .project_tuple::<(String, i32)>()
            .collect::<Result<Vec<_>, _>>()?;
        grouped_scores.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            grouped_scores,
            vec![("alpha".to_string(), 30), ("beta".to_string(), 5)]
        );

        let mut grouped_stats = database
            .bind(|ctx| {
                ctx.from::<EventLog>()?
                    .project_tuple(|e| {
                        let category = e.column(EventLog::category())?;
                        let score = e.column(EventLog::score())?;
                        let total_score = e.aggregate("sum", vec![score])?;
                        let total_count = e.count_all()?;
                        Ok(vec![
                            category,
                            e.alias(total_score, "total_score"),
                            e.alias(total_count, "total_count"),
                        ])
                    })?
                    .group_by_scalar(EventLog::category())?
                    .finish()
            })?
            .project_tuple::<(String, i32, i32)>()
            .collect::<Result<Vec<_>, _>>()?;
        grouped_stats.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            grouped_stats,
            vec![("alpha".to_string(), 30, 2), ("beta".to_string(), 5, 1),]
        );

        let grouped_stats_schema = database.bind(|ctx| {
            ctx.from::<EventLog>()?
                .project_tuple(|e| {
                    let category = e.column(EventLog::category())?;
                    let score = e.column(EventLog::score())?;
                    let total_score = e.aggregate("sum", vec![score])?;
                    let total_count = e.count_all()?;
                    Ok(vec![
                        category,
                        e.alias(total_score, "total_score"),
                        e.alias(total_count, "total_count"),
                    ])
                })?
                .group_by_scalar(EventLog::category())?
                .finish()
        })?;
        assert_eq!(
            grouped_stats_schema.schema(|schema| {
                schema
                    .iter()
                    .map(|column| column.name().to_string())
                    .collect::<Vec<_>>()
            }),
            vec!["category", "total_score", "total_count"]
        );
        grouped_stats_schema.done()?;

        database.drop_table::<EventLog>()?;

        Ok(())
    }

    #[test]
    fn test_orm_subquery_bind_steps() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;

        create_model_table::<User>(&mut database)?;
        database.insert(&User {
            id: 1,
            name: "Alice".to_string(),
            age: Some(18),
            cache: String::new(),
        })?;
        database.insert(&User {
            id: 2,
            name: "Bob".to_string(),
            age: Some(30),
            cache: String::new(),
        })?;
        database.insert(&User {
            id: 3,
            name: "Carol".to_string(),
            age: None,
            cache: String::new(),
        })?;

        create_model_table::<Order>(&mut database)?;
        database.insert(&Order {
            id: 1,
            user_id: 1,
            amount: 100,
        })?;
        database.insert(&Order {
            id: 2,
            user_id: 1,
            amount: 200,
        })?;
        database.insert(&Order {
            id: 3,
            user_id: 2,
            amount: 300,
        })?;

        let where_scalar = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        let max_id = e.scalar_subquery(|ctx| {
                            ctx.from::<User>()?
                                .project_value(|e| {
                                    let id = e.column(User::id())?;
                                    e.aggregate("max", vec![id])
                                })?
                                .finish()
                        })?;
                        id.eq(max_id)
                    })?
                    .finish()
            })?
            .orm::<User>()
            .next()
            .transpose()?
            .unwrap();
        assert_eq!(where_scalar.id, 3);

        let where_exists = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        e.exists_subquery(false, |ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::user_id())?.eq(e.column(User::id())?))?
                                .project_scalar(Order::id())?
                                .finish()
                        })
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            where_exists.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 2]
        );

        let where_quantified = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .filter(|e| {
                        let id = e.column(User::id())?;
                        id.in_subquery(|ctx| {
                            ctx.from::<Order>()?
                                .project_scalar(Order::user_id())?
                                .finish()
                        })
                    })?
                    .order_by(User::id())?
                    .finish()
            })?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            where_quantified
                .iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let project_scalar = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .order_by(User::id())?
                    .project_value(|e| {
                        e.scalar_subquery(|ctx| {
                            ctx.from::<User>()?
                                .project_value(|e| {
                                    let id = e.column(User::id())?;
                                    e.aggregate("max", vec![id])
                                })?
                                .finish()
                        })
                    })?
                    .finish()
            })?
            .project_value::<i32>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(project_scalar, vec![3, 3, 3]);

        let join_subquery = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .inner_join::<Order, _>(|e| {
                        e.exists_subquery(false, |ctx| {
                            ctx.from::<Order>()?
                                .filter(|e| e.column(Order::id())?.eq(1))?
                                .project_scalar(Order::id())?
                                .finish()
                        })
                    })?
                    .finish()
            })
            .and_then(drain_result_iter);
        assert!(join_subquery.is_err());

        let group_by_subquery = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_scalar(User::id())?
                    .group_by(|e| {
                        e.scalar_subquery(|ctx| {
                            ctx.from::<User>()?.project_scalar(User::id())?.finish()
                        })
                    })?
                    .finish()
            })
            .and_then(drain_result_iter);
        assert!(group_by_subquery.is_err());

        let having_subquery = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_value(|e| {
                        let id = e.column(User::id())?;
                        e.aggregate("max", vec![id])
                    })?
                    .having(|e| {
                        let max_id = e.scalar_subquery(|ctx| {
                            ctx.from::<User>()?.project_scalar(User::id())?.finish()
                        })?;
                        e.column(User::id())?.eq(max_id)
                    })?
                    .finish()
            })
            .and_then(drain_result_iter);
        assert!(having_subquery.is_err());

        let sort_subquery = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .order_by_expr(|e| {
                        Ok(e.scalar_subquery(|ctx| {
                            ctx.from::<User>()?.project_scalar(User::id())?.finish()
                        })?
                        .asc())
                    })?
                    .finish()
            })
            .and_then(drain_result_iter);
        assert!(sort_subquery.is_err());

        database.drop_table::<Order>()?;
        database.drop_table::<User>()?;

        Ok(())
    }

    #[test]
    fn test_orm_model_lifecycle() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;

        create_model_table::<User>(&mut database)?;
        drop_model_index::<User>(&mut database, "users_age_index")?;
        create_model_table_if_not_exists::<User>(&mut database)?;
        drop_model_index::<User>(&mut database, "users_age_index")?;
        create_model_table_if_not_exists::<User>(&mut database)?;

        let user = User {
            id: 1,
            name: "Alice".to_string(),
            age: Some(18),
            cache: "warm".to_string(),
        };

        database.insert(&user)?;

        let loaded = database.get::<User>(&1)?.unwrap();
        assert_eq!(
            loaded,
            User {
                id: 1,
                name: "Alice".to_string(),
                age: Some(18),
                cache: "".to_string(),
            }
        );

        let users = database.fetch::<User>()?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].name, "Alice");

        for id in 1000..=1100 {
            database.insert(&User {
                id,
                name: format!("user_{id}"),
                age: Some(id),
                cache: "".to_string(),
            })?;
        }
        database.analyze_model::<User>()?;

        let explain_rows =
            collect_result_tuples(database.run("explain select age from users where age = 1050")?)?;
        let explain_plan = explain_rows
            .iter()
            .filter_map(|row| match row.values.first() {
                Some(DataValue::Utf8 { value, .. }) => Some(value.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            explain_plan.contains("IndexScan By #") && explain_plan.contains("Covered"),
            "unexpected explain plan: {explain_plan}"
        );

        database
            .run("insert into users (id, user_name) values (9, 'DefaultAge')")?
            .done()?;
        let defaulted = database.get::<User>(&9)?.unwrap();
        assert_eq!(defaulted.age, Some(18));

        database
            .bind(|ctx| {
                ctx.mutate::<User>()?
                    .filter(|e| e.column(User::id())?.eq(1))?
                    .update(|u| {
                        u.set_value(User::name(), "Bob")?;
                        u.set_value(User::age(), None::<i32>)
                    })
            })?
            .done()?;

        let updated = database.get::<User>(&1)?.unwrap();
        assert_eq!(updated.name, "Bob");
        assert_eq!(updated.age, None);

        database
            .bind(|ctx| {
                ctx.mutate::<User>()?
                    .filter(|e| e.column(User::id())?.eq(1))?
                    .delete()
            })?
            .done()?;
        assert!(database.get::<User>(&1)?.is_none());

        database.insert(&User {
            id: 2,
            name: "Carol".to_string(),
            age: Some(20),
            cache: "".to_string(),
        })?;

        let duplicate = database.insert(&User {
            id: 3,
            name: "Carol".to_string(),
            age: Some(22),
            cache: "".to_string(),
        });
        assert!(matches!(
            duplicate,
            Err(DatabaseError::DuplicateUniqueValue)
        ));

        database
            .bind(|ctx| {
                ctx.mutate::<User>()?
                    .filter(|e| e.column(User::id())?.eq(2))?
                    .delete()
            })?
            .done()?;
        assert!(database.get::<User>(&2)?.is_none());

        database.drop_table::<User>()?;
        database.drop_table_if_exists::<User>()?;

        Ok(())
    }

    #[test]
    fn test_orm_update_delete_builder() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;
        create_model_table::<User>(&mut database)?;

        for (id, name, age) in [
            (1, "Alice", Some(18)),
            (2, "Bob", Some(19)),
            (3, "Carol", Some(20)),
        ] {
            database.insert(&User {
                id,
                name: name.to_string(),
                age,
                cache: String::new(),
            })?;
        }

        database
            .bind(|ctx| {
                ctx.mutate_as::<User>("u")?
                    .filter(|e| e.qualified_column("u", User::id())?.eq(1))?
                    .update(|u| {
                        u.set_value(User::name(), "BuilderAlice")?;
                        u.set_value(User::age(), None::<i32>)
                    })
            })?
            .done()?;

        let updated = database.get::<User>(&1)?.unwrap();
        assert_eq!(updated.name, "BuilderAlice");
        assert_eq!(updated.age, None);

        database
            .bind(|ctx| {
                ctx.mutate::<User>()?
                    .filter(|e| e.column(User::id())?.eq(2))?
                    .update(|u| {
                        u.set_expr(User::age(), |e| {
                            let id = e.column(User::id())?;
                            e.binary(id, BinaryOperator::Plus, e.value(20))
                        })
                    })
            })?
            .done()?;

        assert_eq!(database.get::<User>(&2)?.unwrap().age, Some(22));

        database
            .bind(|ctx| {
                ctx.mutate_as::<User>("u")?
                    .filter(|e| e.qualified_column("u", User::name())?.eq("Carol"))?
                    .delete()
            })?
            .done()?;
        assert!(database.get::<User>(&3)?.is_none());

        let empty_update = database.bind(|ctx| {
            ctx.mutate::<User>()?
                .filter(|e| e.column(User::id())?.eq(1))?
                .update(|_| Ok(()))
        });
        assert!(matches!(empty_update, Err(DatabaseError::ColumnsEmpty)));

        Ok(())
    }

    #[test]
    fn test_orm_insert_query_builder() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;
        create_model_table::<User>(&mut database)?;
        create_model_table::<ArchivedUser>(&mut database)?;

        for (id, name, age) in [(1, "Alice", Some(18)), (2, "Bob", Some(19))] {
            database.insert(&ArchivedUser {
                id,
                name: name.to_string(),
                age,
                cache: String::new(),
            })?;
        }

        database
            .bind(|ctx| {
                ctx.insert_select::<User, _, _>(std::iter::empty::<String>(), |ctx| {
                    ctx.from::<ArchivedUser>()?.finish()
                })
            })?
            .done()?;

        let inserted_users = database
            .bind(|ctx| ctx.from::<User>()?.order_by(User::id())?.finish())?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(inserted_users.len(), 2);
        assert_eq!(inserted_users[0].name, "Alice");
        assert_eq!(inserted_users[1].name, "Bob");

        create_model_table::<UserNameSnapshot>(&mut database)?;
        database
            .bind(|ctx| {
                ctx.insert_select::<UserNameSnapshot, _, _>(["id", "user_name"], |ctx| {
                    ctx.from::<ArchivedUser>()?
                        .project_scalars((ArchivedUser::id(), ArchivedUser::name()))?
                        .finish()
                })
            })?
            .done()?;

        let snapshots = database
            .bind(|ctx| {
                ctx.from::<UserNameSnapshot>()?
                    .order_by(UserNameSnapshot::id())?
                    .finish()
            })?
            .orm::<UserNameSnapshot>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].name, "Alice");
        assert_eq!(snapshots[1].name, "Bob");

        database
            .bind(|ctx| {
                ctx.overwrite_select::<User, _, _>(std::iter::empty::<String>(), |ctx| {
                    ctx.from::<ArchivedUser>()?
                        .filter(|e| e.column(ArchivedUser::id())?.eq(2))?
                        .finish()
                })
            })?
            .done()?;

        let overwritten_users = database
            .bind(|ctx| ctx.from::<User>()?.order_by(User::id())?.finish())?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(overwritten_users.len(), 2);
        assert_eq!(overwritten_users[0].id, 1);
        assert_eq!(overwritten_users[0].name, "Alice");
        assert_eq!(overwritten_users[1].id, 2);
        assert_eq!(overwritten_users[1].name, "Bob");

        database
            .bind(|ctx| {
                ctx.overwrite_select::<UserNameSnapshot, _, _>(["id", "user_name"], |ctx| {
                    ctx.from::<ArchivedUser>()?
                        .filter(|e| e.column(ArchivedUser::id())?.eq(1))?
                        .project_scalars((ArchivedUser::id(), ArchivedUser::name()))?
                        .finish()
                })
            })?
            .done()?;

        let overwritten_snapshots = database
            .bind(|ctx| {
                ctx.from::<UserNameSnapshot>()?
                    .order_by(UserNameSnapshot::id())?
                    .finish()
            })?
            .orm::<UserNameSnapshot>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(overwritten_snapshots.len(), 2);
        assert_eq!(overwritten_snapshots[0].id, 1);
        assert_eq!(overwritten_snapshots[0].name, "Alice");
        assert_eq!(overwritten_snapshots[1].id, 2);
        assert_eq!(overwritten_snapshots[1].name, "Bob");

        Ok(())
    }

    #[test]
    fn test_orm_extended_write_and_ddl_helpers() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;
        create_model_table::<User>(&mut database)?;

        database.insert_many([
            User {
                id: 1,
                name: "Alice".to_string(),
                age: Some(18),
                cache: String::new(),
            },
            User {
                id: 2,
                name: "Bob".to_string(),
                age: None,
                cache: String::new(),
            },
            User {
                id: 3,
                name: "Carol".to_string(),
                age: Some(30),
                cache: String::new(),
            },
        ])?;

        let ages_nulls_first = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_scalar(User::age())?
                    .order_by(User::age().nulls_first())?
                    .finish()
            })?
            .project_value::<Option<i32>>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(ages_nulls_first, vec![None, Some(18), Some(30)]);

        let ages_nulls_last = database
            .bind(|ctx| {
                ctx.from::<User>()?
                    .project_scalar(User::age())?
                    .order_by(User::age())?
                    .finish()
            })?
            .project_value::<Option<i32>>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(ages_nulls_last, vec![Some(18), Some(30), None]);

        let mut set_query_ages = database
            .bind(|ctx| {
                ctx.union(
                    true,
                    |ctx| {
                        ctx.from::<User>()?
                            .filter(|e| e.column(User::id())?.eq(1))?
                            .project_scalar(User::age())?
                            .finish()
                    },
                    |ctx| ctx.from::<User>()?.project_scalar(User::age())?.finish(),
                )
            })?
            .project_value::<Option<i32>>()
            .collect::<Result<Vec<_>, _>>()?;
        set_query_ages.sort_by(|left, right| match (left, right) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(left), Some(right)) => right.cmp(left),
        });
        assert_eq!(set_query_ages, vec![None, Some(30), Some(18), Some(18)]);

        let mut tx = database.new_transaction()?;
        tx.insert_many([User {
            id: 4,
            name: "Dora".to_string(),
            age: None,
            cache: String::new(),
        }])?;
        tx.commit()?;

        let updated_users = database
            .bind(|ctx| ctx.from::<User>()?.order_by(User::id())?.finish())?
            .orm::<User>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            updated_users
                .iter()
                .map(|user| (user.id, user.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "Alice"), (2, "Bob"), (3, "Carol"), (4, "Dora")]
        );

        database.create_view("user_names", |ctx| {
            ctx.from::<User>()?
                .project_scalars((User::id(), User::name()))?
                .finish()
        })?;

        let mut view_rows = database
            .run("select * from user_names")?
            .orm::<UserNameSnapshot>()
            .collect::<Result<Vec<_>, _>>()?;
        view_rows.sort_by_key(|row| row.id);
        assert_eq!(view_rows.len(), 4);
        assert_eq!(view_rows[0].name, "Alice");

        database.create_or_replace_view("user_names", |ctx| {
            ctx.from::<User>()?
                .filter(|e| e.column(User::id())?.eq(2))?
                .project_scalars((User::id(), User::name()))?
                .finish()
        })?;

        let replaced_view_rows = database
            .run("select * from user_names")?
            .orm::<UserNameSnapshot>()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(replaced_view_rows.len(), 1);
        assert_eq!(replaced_view_rows[0].id, 2);
        assert_eq!(replaced_view_rows[0].name, "Bob");

        database.drop_view("user_names")?;
        database.drop_view_if_exists("user_names")?;

        database.truncate::<User>()?;
        let count = database
            .bind(|ctx| ctx.from::<User>()?.count())?
            .project_value::<i32>()
            .next()
            .transpose()?
            .unwrap() as usize;
        assert_eq!(count, 0);

        Ok(())
    }

    #[test]
    fn test_orm_introspection_helpers() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;
        create_model_table::<User>(&mut database)?;
        create_model_table::<Wallet>(&mut database)?;
        database.create_view("user_names", |ctx| {
            ctx.from::<User>()?
                .project_scalars((User::id(), User::name()))?
                .finish()
        })?;

        let tables = database.show_tables()?.collect::<Result<Vec<_>, _>>()?;
        assert!(tables.iter().any(|name| name == "users"));
        assert!(tables.iter().any(|name| name == "wallets"));

        let views = database.show_views()?.collect::<Result<Vec<_>, _>>()?;
        assert!(views.iter().any(|name| name == "user_names"));

        let describe_rows = database
            .describe::<User>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert!(describe_rows.iter().any(|column| column.field == "id"));
        assert!(describe_rows
            .iter()
            .any(|column| column.field == "user_name"));
        assert!(
            describe_rows
                .iter()
                .find(|column| column.field == "id")
                .map(|column| column.key.as_str())
                == Some("PRIMARY")
        );

        let plan = database.explain(|ctx| {
            ctx.from::<User>()?
                .filter(|e| e.column(User::id())?.eq(1))?
                .project_scalar(User::name())?
                .finish()
        })?;
        assert!(plan.contains("Projection"));
        assert!(plan.contains("Filter ("));
        assert!(plan.contains(" = 1"));
        assert!(plan.contains("TableScan users -> [#"));

        let set_plan = database.explain(|ctx| {
            ctx.union(
                false,
                |ctx| ctx.from::<User>()?.project_scalar(User::id())?.finish(),
                |ctx| ctx.from::<Wallet>()?.project_scalar(Wallet::id())?.finish(),
            )
        })?;
        assert!(set_plan.contains("Aggregate"));
        assert!(set_plan.contains("Union: [#"));
        assert!(set_plan.contains("TableScan users -> [#"));
        assert!(set_plan.contains("TableScan wallets -> [#"));

        let mut tx = database.new_transaction()?;
        let tx_tables = tx.show_tables()?.collect::<Result<Vec<_>, _>>()?;
        assert!(tx_tables.iter().any(|name| name == "users"));
        let tx_describe = tx.describe::<Wallet>()?.collect::<Result<Vec<_>, _>>()?;
        assert!(tx_describe.iter().any(|column| column.field == "balance"));
        tx.commit()?;

        Ok(())
    }

    #[test]
    fn test_orm_drop_index() -> Result<(), DatabaseError> {
        let (_temp_dir, mut database) = build_test_database()?;

        create_model_table::<User>(&mut database)?;
        database.insert(&User {
            id: 1,
            name: "Alice".to_string(),
            age: Some(18),
            cache: "".to_string(),
        })?;

        let duplicate = database.insert(&User {
            id: 2,
            name: "Alice".to_string(),
            age: Some(20),
            cache: "".to_string(),
        });
        assert!(matches!(
            duplicate,
            Err(DatabaseError::DuplicateUniqueValue)
        ));

        drop_model_index::<User>(&mut database, "users_age_index")?;
        drop_model_index_if_exists::<User>(&mut database, "users_age_index")?;

        drop_model_index::<User>(&mut database, "uk_user_name_index")?;

        database.insert(&User {
            id: 2,
            name: "Alice".to_string(),
            age: Some(20),
            cache: "".to_string(),
        })?;

        drop_model_index::<User>(&mut database, "users_name_age_index")?;
        drop_model_index_if_exists::<User>(&mut database, "users_name_age_index")?;

        assert!(matches!(
            database.drop_index::<User>("pk_index"),
            Err(DatabaseError::InvalidIndex)
        ));
        assert!(matches!(
            database.drop_index_if_exists::<User>("pk_index"),
            Err(DatabaseError::InvalidIndex)
        ));

        database.drop_table::<User>()?;

        Ok(())
    }

    scala_function!(MyScalaFunction::SUM(LogicalType::Integer, LogicalType::Integer) -> LogicalType::Integer => (|v1: DataValue, v2: DataValue| {
        binary_create(std::borrow::Cow::Owned(LogicalType::Integer), BinaryOperator::Plus)?.binary_eval(&v1, &v2)
    }));

    scala_function!(MyOrmFunction::ADD_ONE(LogicalType::Integer) -> LogicalType::Integer => (|v1: DataValue| {
        Ok(DataValue::Int32(v1.i32().unwrap() + 1))
    }));

    table_function!(MyTableFunction::TEST_NUMBERS(LogicalType::Integer) -> [c1: LogicalType::Integer, c2: LogicalType::Integer] => (|v1: DataValue| {
        let num = v1.i32().unwrap();

        Ok(Box::new((0..num)
            .into_iter()
            .map(|i| Ok(Tuple::new(None, vec![
                    DataValue::Int32(i),
                    DataValue::Int32(i),
                ])))) as Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>)
    }));

    #[test]
    fn test_scala_function() -> Result<(), DatabaseError> {
        let function = MyScalaFunction::new();
        let sum = function.eval(
            &[
                ScalarExpression::Constant(DataValue::Int8(1)),
                ScalarExpression::Constant(DataValue::Utf8 {
                    value: "1".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }),
            ],
            None,
        )?;

        println!("{:?}", function);

        assert_eq!(
            function.summary,
            FunctionSummary {
                name: "sum".to_string().into(),
                arg_types: vec![LogicalType::Integer, LogicalType::Integer],
            }
        );
        assert_eq!(sum, DataValue::Int32(2));
        Ok(())
    }

    #[test]
    fn test_table_function() -> Result<(), DatabaseError> {
        let function = MyTableFunction::new();
        let mut numbers = function.eval(&[ScalarExpression::Constant(DataValue::Int8(2))])?;

        println!("{:?}", function);

        assert_eq!(
            function.summary,
            FunctionSummary {
                name: "test_numbers".to_string().into(),
                arg_types: vec![LogicalType::Integer],
            }
        );
        assert_eq!(
            numbers.next().unwrap().unwrap(),
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0),])
        );
        assert_eq!(
            numbers.next().unwrap().unwrap(),
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(1),])
        );
        assert!(numbers.next().is_none());

        let mut table_arena = TableArena::default();
        let mut function_schema = Schema::new();
        function.output_schema_into(&mut table_arena, &mut function_schema);
        let c1_ref = function_schema[0];
        let c2_ref = function_schema[1];
        let c1 = table_arena.column(c1_ref);
        let c2 = table_arena.column(c2_ref);

        assert_eq!(c1.name(), "c1");
        assert_eq!(c1.datatype(), &LogicalType::Integer);
        assert!(c1.nullable());
        assert_eq!(c2.name(), "c2");
        assert_eq!(c2.datatype(), &LogicalType::Integer);
        assert!(c2.nullable());

        Ok(())
    }
}
