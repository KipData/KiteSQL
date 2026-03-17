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
    use kite_sql::catalog::column::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation};
    use kite_sql::db::{DataBaseBuilder, ResultIter};
    use kite_sql::errors::DatabaseError;
    use kite_sql::expression::function::scala::ScalarFunctionImpl;
    use kite_sql::expression::function::table::TableFunctionImpl;
    use kite_sql::expression::function::FunctionSummary;
    use kite_sql::expression::BinaryOperator;
    use kite_sql::expression::ScalarExpression;
    use kite_sql::orm::{func, QueryExpr, QueryValue};
    use kite_sql::types::evaluator::EvaluatorFactory;
    use kite_sql::types::tuple::{SchemaRef, Tuple};
    use kite_sql::types::value::{DataValue, Utf8Type};
    use kite_sql::types::LogicalType;
    use kite_sql::{from_tuple, scala_function, table_function, Model};
    use rust_decimal::Decimal;
    use sqlparser::ast::{
        BinaryOperator as SqlBinaryOperator, CharLengthUnits, Expr as SqlExpr, Query as SqlQuery,
        Statement as SqlStatement,
    };
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    use std::sync::Arc;

    fn build_tuple() -> (Tuple, SchemaRef) {
        let schema_ref = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
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
        ]);
        let values = vec![
            DataValue::Int32(9),
            DataValue::Utf8 {
                value: "LOL".to_string(),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            },
        ];

        (Tuple::new(None, values), schema_ref)
    }

    fn parse_query(sql: &str) -> SqlQuery {
        let dialect = PostgreSqlDialect {};
        let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
        match parser.parse_statement().unwrap() {
            SqlStatement::Query(query) => *query,
            statement => panic!("expected query statement, got {statement:?}"),
        }
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
        let (tuple, schema_ref) = build_tuple();
        let my_struct = MyStruct::from((&schema_ref, tuple));

        println!("{:?}", my_struct);

        assert_eq!(my_struct.c1, 9);
        assert_eq!(my_struct.c2, "LOL");
    }

    #[test]
    fn test_model_mapping() {
        let mut tuple_and_schema = build_tuple();
        tuple_and_schema.1 = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
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
            ColumnRef::from(ColumnCatalog::new(
                "age".to_string(),
                true,
                ColumnDesc::new(LogicalType::Integer, None, true, None).unwrap(),
            )),
        ]);
        tuple_and_schema.0 = Tuple::new(
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

        let derived = DerivedStruct::from((&tuple_and_schema.1, tuple_and_schema.0));

        assert_eq!(derived.c1, 9);
        assert_eq!(derived.name, "LOL");
        assert_eq!(derived.age, None);
        assert_eq!(derived.skipped, "");
    }

    #[test]
    fn test_result_iter_to_orm_iter() -> Result<(), DatabaseError> {
        let database = DataBaseBuilder::path(".").build_in_memory()?;

        database
            .run("create table users (c1 int primary key, c2 varchar, age int)")?
            .done()?;
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
        let database = DataBaseBuilder::path(".").build_in_memory()?;

        database.create_table::<Wallet>()?;
        for id in 1..=101 {
            database.insert(&Wallet {
                id,
                balance: Decimal::new((id * 100) as i64, 2),
            })?;
        }
        database.analyze::<Wallet>()?;

        let mut iter = database.run("describe wallets")?;
        let rows = iter.by_ref().collect::<Result<Vec<_>, _>>()?;
        iter.done()?;

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
        let database = DataBaseBuilder::path(".").build_in_memory()?;

        database.create_table::<CountryCode>()?;

        let mut iter = database.run("describe country_codes")?;
        let rows = iter.by_ref().collect::<Result<Vec<_>, _>>()?;
        iter.done()?;

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
        let database = DataBaseBuilder::path(".").build_in_memory()?;

        database.create_table::<MigratingUserV1>()?;
        database.insert(&MigratingUserV1 {
            id: 1,
            name: "Alice".to_string(),
        })?;

        database.migrate::<MigratingUserV2>()?;
        assert_eq!(database.get::<MigratingUserV2>(&1)?.unwrap().age, 18);

        database.migrate::<MigratingUserV3>()?;
        assert_eq!(
            database.get::<MigratingUserV3>(&1)?,
            Some(MigratingUserV3 { id: 1, age: 18 })
        );

        let describe_rows = database
            .run("describe migrating_users")?
            .collect::<Result<Vec<_>, _>>()?;
        let column_names = describe_rows
            .iter()
            .filter_map(|row| match row.values.first() {
                Some(DataValue::Utf8 { value, .. }) => Some(value.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(column_names, vec!["id", "age"]);

        database.migrate::<MigratingUserV4>()?;
        assert_eq!(
            database.get::<MigratingUserV4>(&1)?,
            Some(MigratingUserV4 { id: 1, years: 18 })
        );

        database.migrate::<MigratingUserV5>()?;
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
        let database = DataBaseBuilder::path(".")
            .register_scala_function(MyOrmFunction::new())
            .build_in_memory()?;

        database.create_table::<User>()?;
        database.run("drop index users.users_age_index")?.done()?;
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

        let adults = database
            .select::<User>()
            .and(User::age().gte(18), User::name().like("A%"))
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(adults.len(), 1);
        assert_eq!(adults[0].name, "Alice");

        let quoted = database.select::<User>().eq(User::name(), "A'lex").get()?;
        assert_eq!(quoted.unwrap().id, 3);

        let ordered = database
            .select::<User>()
            .not(User::age().is_null())
            .desc(User::age())
            .limit(1)
            .get()?
            .unwrap();
        assert_eq!(ordered.id, 2);

        let count = database.select::<User>().is_not_null(User::age()).count()?;
        assert_eq!(count, 2);

        let exists = database.select::<User>().eq(User::id(), 2).exists()?;
        assert!(exists);
        let missing = database.select::<User>().eq(User::id(), 99).exists()?;
        assert!(!missing);

        let two_users = database
            .select::<User>()
            .or(User::id().eq(1), User::id().eq(2))
            .asc(User::id())
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            two_users.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 2]
        );

        let alice_only = database
            .select::<User>()
            .and(User::age().is_not_null(), User::name().not_like("B%"))
            .count()?;
        assert_eq!(alice_only, 1);

        let in_list = database
            .select::<User>()
            .in_list(User::id(), [1, 3])
            .asc(User::id())
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            in_list.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 3]
        );

        let not_between = database
            .select::<User>()
            .not_between(User::id(), 2, 2)
            .asc(User::id())
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            not_between.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 3]
        );

        let either_named_a_or_missing_age = database
            .select::<User>()
            .or(User::name().like("A%"), User::age().is_null())
            .asc(User::id())
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            either_named_a_or_missing_age
                .iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );

        let udf_matched = database
            .select::<User>()
            .eq(func("add_one", [QueryValue::from(User::id())]), 2)
            .get()?
            .unwrap();
        assert_eq!(udf_matched.id, 1);

        let cast_matched = database
            .select::<User>()
            .eq(User::id().cast("BIGINT")?, 2_i64)
            .get()?
            .unwrap();
        assert_eq!(cast_matched.id, 2);

        let raw_ast_matched = database
            .select::<User>()
            .filter(QueryExpr::from_ast(SqlExpr::BinaryOp {
                left: Box::new(QueryValue::from(User::id()).into_ast()),
                op: SqlBinaryOperator::Eq,
                right: Box::new(QueryValue::from(3).into_ast()),
            }))
            .get()?
            .unwrap();
        assert_eq!(raw_ast_matched.id, 3);

        let exists_count = database
            .select::<User>()
            .where_exists(parse_query("select id from users where id = 2"))
            .count()?;
        assert_eq!(exists_count, 3);

        let in_subquery = database
            .select::<User>()
            .in_subquery(
                User::id(),
                parse_query("select id from users where id in (1, 3)"),
            )
            .asc(User::id())
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            in_subquery.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 3]
        );

        let mut tx = database.new_transaction()?;
        let in_tx = tx.select::<User>().eq(User::id(), 2).get()?.unwrap();
        assert_eq!(in_tx.name, "Bob");
        tx.commit()?;

        database.drop_table::<User>()?;

        Ok(())
    }

    #[test]
    fn test_orm_crud() -> Result<(), DatabaseError> {
        let database = DataBaseBuilder::path(".").build_in_memory()?;

        database.create_table::<User>()?;
        database.run("drop index users.users_age_index")?.done()?;
        database.create_table_if_not_exists::<User>()?;
        database.run("drop index users.users_age_index")?.done()?;
        database.create_table_if_not_exists::<User>()?;

        let mut user = User {
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
        database.analyze::<User>()?;

        let mut explain_iter = database.run("explain select age from users where age = 1050")?;
        let explain_rows = explain_iter.by_ref().collect::<Result<Vec<_>, _>>()?;
        explain_iter.done()?;
        let explain_plan = explain_rows
            .iter()
            .filter_map(|row| match row.values.first() {
                Some(DataValue::Utf8 { value, .. }) => Some(value.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            explain_plan.contains("IndexScan By users_age_index"),
            "unexpected explain plan: {explain_plan}"
        );

        database
            .run("insert into users (id, user_name) values (9, 'DefaultAge')")?
            .done()?;
        let defaulted = database.get::<User>(&9)?.unwrap();
        assert_eq!(defaulted.age, Some(18));

        user.name = "Bob".to_string();
        user.age = None;
        database.update(&user)?;

        let updated = database.get::<User>(&1)?.unwrap();
        assert_eq!(updated.name, "Bob");
        assert_eq!(updated.age, None);

        database.delete_by_id::<User>(&1)?;
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

        database.delete_by_id::<User>(&2)?;
        assert!(database.get::<User>(&2)?.is_none());

        database.drop_table::<User>()?;
        database.drop_table_if_exists::<User>()?;

        Ok(())
    }

    #[test]
    fn test_orm_drop_index() -> Result<(), DatabaseError> {
        let database = DataBaseBuilder::path(".").build_in_memory()?;

        database.create_table::<User>()?;
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

        database.drop_index::<User>("users_age_index")?;
        database.drop_index_if_exists::<User>("users_age_index")?;

        database.drop_index::<User>("uk_user_name_index")?;

        database.insert(&User {
            id: 2,
            name: "Alice".to_string(),
            age: Some(20),
            cache: "".to_string(),
        })?;

        database.drop_index::<User>("users_name_age_index")?;
        database.drop_index_if_exists::<User>("users_name_age_index")?;

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
        EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Plus)?.binary_eval(&v1, &v2)
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

        let function_schema = function.output_schema();
        let table_name: Arc<str> = "test_numbers".to_string().into();
        let mut c1 = ColumnCatalog::new(
            "c1".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        );
        c1.summary_mut().relation = ColumnRelation::Table {
            column_id: function_schema[0].id().unwrap(),
            table_name: table_name.clone(),
            is_temp: false,
        };
        let mut c2 = ColumnCatalog::new(
            "c2".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        );
        c2.summary_mut().relation = ColumnRelation::Table {
            column_id: function_schema[1].id().unwrap(),
            table_name: table_name.clone(),
            is_temp: false,
        };

        assert_eq!(
            function_schema,
            &Arc::new(vec![ColumnRef::from(c1), ColumnRef::from(c2)])
        );

        Ok(())
    }
}
