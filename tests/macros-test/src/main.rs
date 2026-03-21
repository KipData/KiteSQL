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
    use kite_sql::orm::{case_when, count_all, func, max, min, sum, QueryValue};
    use kite_sql::types::evaluator::EvaluatorFactory;
    use kite_sql::types::tuple::{SchemaRef, Tuple};
    use kite_sql::types::value::{DataValue, Utf8Type};
    use kite_sql::types::LogicalType;
    use kite_sql::{from_tuple, scala_function, table_function, Model, Projection};
    use rust_decimal::Decimal;
    use sqlparser::ast::{CharLengthUnits, DataType as SqlDataType};
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

    #[derive(Default, Debug, PartialEq, Projection)]
    struct UserSummary {
        id: i32,
        #[projection(rename = "user_name")]
        display_name: String,
        age: Option<i32>,
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

        database.create_table::<Order>()?;
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

        database.create_table::<Wallet>()?;
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
            .from::<User>()
            .and(User::age().gte(18), User::name().like("A%"))
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(adults.len(), 1);
        assert_eq!(adults[0].name, "Alice");

        let quoted = database.from::<User>().eq(User::name(), "A'lex").get()?;
        assert_eq!(quoted.unwrap().id, 3);

        let ordered = database
            .from::<User>()
            .not(User::age().is_null())
            .desc(User::age())
            .limit(1)
            .get()?
            .unwrap();
        assert_eq!(ordered.id, 2);

        let count = database.from::<User>().is_not_null(User::age()).count()?;
        assert_eq!(count, 2);

        let exists = database.from::<User>().eq(User::id(), 2).exists()?;
        assert!(exists);
        let missing = database.from::<User>().eq(User::id(), 99).exists()?;
        assert!(!missing);

        let two_users = database
            .from::<User>()
            .or(User::id().eq(1), User::id().eq(2))
            .asc(User::id())
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            two_users.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 2]
        );

        assert_eq!(
            database
                .from::<User>()
                .and(User::age().is_not_null(), User::name().not_like("B%"))
                .count()?,
            1
        );

        let in_list = database
            .from::<User>()
            .in_list(User::id(), [1, 3])
            .asc(User::id())
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            in_list.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 3]
        );

        let either_named_a_or_missing_age = database
            .from::<User>()
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

        let query_value_function_matched = database
            .from::<User>()
            .eq(QueryValue::function("add_one", [User::id()]), 3)
            .get()?
            .unwrap();
        assert_eq!(query_value_function_matched.id, 2);

        let cast_to_matched = database
            .from::<User>()
            .eq(User::id().cast_to(SqlDataType::BigInt(None)), 3_i64)
            .get()?
            .unwrap();
        assert_eq!(cast_to_matched.id, 3);

        let add_matched = database
            .from::<User>()
            .eq(User::id().add(1), 3)
            .get()?
            .unwrap();
        assert_eq!(add_matched.id, 2);

        let arithmetic_projection = database
            .from::<User>()
            .project_tuple((
                User::id(),
                User::id().mul(10).alias("times_ten"),
                User::id().div(2).alias("half_id"),
                User::id().modulo(2).alias("id_mod_2"),
            ))
            .asc(User::id())
            .fetch::<(i32, i32, i32, i32)>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            arithmetic_projection,
            vec![(1, 10, 0, 1), (2, 20, 1, 0), (3, 30, 1, 1)]
        );

        let projected_name = database
            .from::<User>()
            .project_value(User::name())
            .eq(User::id(), 1)
            .get::<String>()?;
        assert_eq!(projected_name.as_deref(), Some("Alice"));

        let age_buckets = database
            .from::<User>()
            .project_value(case_when(
                [
                    (User::age().is_null(), "unknown"),
                    (User::age().lt(20), "minor"),
                ],
                "adult",
            ))
            .asc(User::id())
            .fetch::<String>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(age_buckets, vec!["minor", "adult", "unknown"]);

        let simple_case_labels = database
            .from::<User>()
            .project_value(
                QueryValue::simple_case(User::id(), [(1, "one"), (2, "two")], "other")
                    .alias("id_label"),
            )
            .asc(User::id())
            .fetch::<String>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(simple_case_labels, vec!["one", "two", "other"]);

        let arithmetic_query_value = database
            .from::<User>()
            .project_value(
                QueryValue::function("add_one", [User::id()])
                    .add(10)
                    .alias("boosted_id"),
            )
            .eq(User::id(), 1)
            .get::<i32>()?;
        assert_eq!(arithmetic_query_value, Some(12));

        let id_sum = database
            .from::<User>()
            .project_value(sum(User::id()))
            .get::<i32>()?;
        assert_eq!(id_sum, Some(6));

        let total_users = database
            .from::<User>()
            .project_value(count_all().alias("total_users"))
            .get::<i32>()?;
        assert_eq!(total_users, Some(3));

        let min_user_id = database
            .from::<User>()
            .project_value(min(User::id()).alias("min_user_id"))
            .get::<i32>()?;
        assert_eq!(min_user_id, Some(1));

        let max_user_id = database
            .from::<User>()
            .project_value(max(User::id()).alias("max_user_id"))
            .get::<i32>()?;
        assert_eq!(max_user_id, Some(3));

        let projected_user_rows = database
            .from::<User>()
            .project_tuple((User::id(), User::name()))
            .asc(User::id())
            .fetch::<(i32, String)>()?
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
            .from::<User>()
            .project_tuple((
                User::id(),
                func("add_one", [QueryValue::from(User::id())]).alias("next_id"),
            ))
            .asc(User::id())
            .fetch::<(i32, i32)>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(udf_projection, vec![(1, 2), (2, 3), (3, 4)]);

        let udf_projection_schema = database
            .from::<User>()
            .project_tuple((
                User::id(),
                QueryValue::function("add_one", [User::id()]).alias("next_id"),
            ))
            .asc(User::id())
            .raw()?;
        assert_eq!(
            udf_projection_schema
                .schema()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>(),
            vec!["id", "next_id"]
        );
        udf_projection_schema.done()?;

        let projected_users = database
            .from::<User>()
            .project::<UserSummary>()
            .asc(User::id())
            .fetch()?
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
            .from::<User>()
            .project::<UserSummary>()
            .eq(User::id(), 1)
            .get()?;
        assert_eq!(
            projected_user,
            Some(UserSummary {
                id: 1,
                display_name: "Alice".to_string(),
                age: Some(18),
            })
        );

        let aliased_total_users = database
            .from::<User>()
            .project_value(count_all().alias("total_users"))
            .raw()?;
        assert_eq!(aliased_total_users.schema()[0].name(), "total_users");
        aliased_total_users.done()?;

        let projected_schema = database.from::<User>().project::<UserSummary>().raw()?;
        assert_eq!(
            projected_schema
                .schema()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>(),
            vec!["id", "display_name", "age"]
        );
        projected_schema.done()?;

        assert_eq!(
            database
                .from::<User>()
                .where_exists(
                    database
                        .from::<User>()
                        .project_value(User::id())
                        .eq(User::id(), 2),
                )
                .count()?,
            3
        );

        assert_eq!(
            database
                .from::<User>()
                .where_not_exists(
                    database
                        .from::<User>()
                        .project_value(User::id())
                        .eq(User::id(), 2),
                )
                .count()?,
            0
        );

        let in_subquery = database
            .from::<User>()
            .in_subquery(
                User::id(),
                database
                    .from::<User>()
                    .project_value(User::id())
                    .in_list(User::id(), [1, 3]),
            )
            .asc(User::id())
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            in_subquery.iter().map(|user| user.id).collect::<Vec<_>>(),
            vec![1, 3]
        );

        let aliased_user = database
            .from::<User>()
            .alias("u")
            .eq(User::id().qualify("u"), 2)
            .get()?
            .unwrap();
        assert_eq!(aliased_user.name, "Bob");

        let aliased_projection = database
            .from::<User>()
            .alias("u")
            .project::<UserSummary>()
            .eq(User::id().qualify("u"), 2)
            .get()?;
        assert_eq!(
            aliased_projection,
            Some(UserSummary {
                id: 2,
                display_name: "Bob".to_string(),
                age: Some(30),
            })
        );

        let joined_projection = database
            .from::<User>()
            .inner_join::<Order>()
            .on(User::id().eq(Order::user_id()))
            .project::<UserOrderSummary>()
            .asc(Order::id())
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            joined_projection,
            vec![
                UserOrderSummary {
                    display_name: "Alice".to_string(),
                    amount: 100,
                },
                UserOrderSummary {
                    display_name: "Alice".to_string(),
                    amount: 200,
                },
                UserOrderSummary {
                    display_name: "Bob".to_string(),
                    amount: 300,
                },
            ]
        );

        let using_joined_rows = database
            .from::<User>()
            .inner_join::<Wallet>()
            .using(User::id())
            .project_tuple((User::name(), Wallet::balance()))
            .asc(User::id())
            .fetch::<(String, Decimal)>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            using_joined_rows,
            vec![
                ("Alice".to_string(), Decimal::new(5000, 2)),
                ("A'lex".to_string(), Decimal::new(1250, 2)),
            ]
        );

        let full_joined_rows = database
            .from::<User>()
            .full_join::<Wallet>()
            .using(User::id())
            .project_tuple((User::id(), Wallet::id()))
            .fetch::<(Option<i32>, Option<i32>)>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(full_joined_rows.len(), 4);

        let union_tuple = database
            .from::<User>()
            .eq(User::id(), 2)
            .project_tuple((User::id(), User::name()))
            .union(
                database
                    .from::<User>()
                    .eq(User::id(), 2)
                    .project_tuple((User::id(), User::name())),
            )
            .all()
            .get::<(i32, String)>()?;
        assert_eq!(union_tuple, Some((2, "Bob".to_string())));

        let ordered_union_ids = database
            .from::<User>()
            .project_value(User::id())
            .union(database.from::<Order>().project_value(Order::user_id()))
            .all()
            .asc(User::id())
            .offset(1)
            .limit(3)
            .fetch::<i32>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(ordered_union_ids, vec![1, 1, 2]);

        let users_without_orders = database
            .from::<User>()
            .in_subquery(
                User::id(),
                database
                    .from::<User>()
                    .project_value(User::id())
                    .except(database.from::<Order>().project_value(Order::user_id())),
            )
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            users_without_orders
                .iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            vec![3]
        );

        let left_joined_rows = database
            .from::<User>()
            .alias("u")
            .left_join::<Order>()
            .alias("o")
            .on(User::id().qualify("u").eq(Order::user_id().qualify("o")))
            .project_tuple((
                User::id().qualify("u").alias("user_id"),
                Order::amount().qualify("o").alias("order_amount"),
            ))
            .asc(User::id().qualify("u"))
            .asc(Order::id().qualify("o"))
            .fetch::<(i32, Option<i32>)>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            left_joined_rows,
            vec![(1, Some(100)), (1, Some(200)), (2, Some(300)), (3, None)]
        );

        let mut tx = database.new_transaction()?;
        let in_tx = tx.from::<User>().eq(User::id(), 2).get()?.unwrap();
        assert_eq!(in_tx.name, "Bob");
        tx.commit()?;

        database.drop_table::<Wallet>()?;
        database.drop_table::<Order>()?;
        database.drop_table::<User>()?;

        Ok(())
    }

    #[test]
    fn test_orm_expression_and_set_query_helpers() -> Result<(), DatabaseError> {
        let database = DataBaseBuilder::path(".").build_in_memory()?;

        database.create_table::<User>()?;
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

        database.create_table::<Order>()?;
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

        let exists_count = database
            .from::<User>()
            .filter(kite_sql::orm::QueryExpr::exists(
                database
                    .from::<Order>()
                    .project_value(Order::id())
                    .eq(Order::id(), 1),
            ))
            .count()?;
        assert_eq!(exists_count, 3);

        let not_exists_count = database
            .from::<User>()
            .filter(kite_sql::orm::QueryExpr::not_exists(
                database
                    .from::<Order>()
                    .project_value(Order::id())
                    .eq(Order::id(), 99),
            ))
            .count()?;
        assert_eq!(not_exists_count, 3);

        let blocked_by_not_exists = database
            .from::<User>()
            .filter(kite_sql::orm::QueryExpr::not_exists(
                database
                    .from::<Order>()
                    .project_value(Order::id())
                    .eq(Order::id(), 1),
            ))
            .count()?;
        assert_eq!(blocked_by_not_exists, 0);

        let users_with_orders = database
            .from::<User>()
            .filter(kite_sql::orm::QueryExpr::exists(
                database
                    .from::<Order>()
                    .project_value(Order::id())
                    .eq(Order::user_id(), User::id()),
            ))
            .asc(User::id())
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            users_with_orders
                .iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let users_without_orders = database
            .from::<User>()
            .filter(kite_sql::orm::QueryExpr::not_exists(
                database
                    .from::<Order>()
                    .project_value(Order::id())
                    .eq(Order::user_id(), User::id()),
            ))
            .fetch()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            users_without_orders
                .iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            vec![3]
        );

        database
            .from::<User>()
            .filter(
                User::id().in_subquery(
                    database
                        .from::<Order>()
                        .project_value(Order::user_id())
                        .eq(Order::user_id(), User::id()),
                ),
            )
            .asc(User::id())
            .raw()?
            .done()?;

        database
            .from::<User>()
            .filter(
                User::id().not_in_subquery(
                    database
                        .from::<Order>()
                        .project_value(Order::user_id())
                        .eq(Order::user_id(), User::id()),
                ),
            )
            .asc(User::id())
            .raw()?
            .done()?;

        database
            .from::<User>()
            .filter(
                User::id().in_subquery(
                    database
                        .from::<Order>()
                        .project_value(Order::user_id())
                        .eq(Order::amount(), 100)
                        .union(
                            database
                                .from::<Order>()
                                .project_value(Order::user_id())
                                .eq(Order::amount(), 300),
                        )
                        .all(),
                ),
            )
            .asc(User::id())
            .raw()?
            .done()?;

        let correlated_exists_with_union = database
            .from::<User>()
            .filter(kite_sql::orm::QueryExpr::exists(
                database
                    .from::<Order>()
                    .project_value(Order::id())
                    .eq(Order::user_id(), User::id())
                    .union(
                        database
                            .from::<Order>()
                            .project_value(Order::id())
                            .eq(Order::amount(), 300),
                    ),
            ))
            .count();
        assert!(correlated_exists_with_union.is_err());

        let max_id_user = database
            .from::<User>()
            .eq(
                User::id(),
                QueryValue::subquery(database.from::<User>().project_value(max(User::id()))),
            )
            .get()?
            .unwrap();
        assert_eq!(max_id_user.id, 3);

        let union_user = database
            .from::<User>()
            .eq(User::id(), 2)
            .union(database.from::<User>().eq(User::id(), 2))
            .all()
            .get()?
            .unwrap();
        assert_eq!(union_user.id, 2);

        let ordered_union_user = database
            .from::<User>()
            .eq(User::id(), 1)
            .union(database.from::<User>().eq(User::id(), 2))
            .desc(User::id())
            .get()?
            .unwrap();
        assert_eq!(ordered_union_user.id, 2);

        let union_value = database
            .from::<User>()
            .project_value(User::id())
            .eq(User::id(), 2)
            .union(
                database
                    .from::<Order>()
                    .project_value(Order::user_id())
                    .eq(Order::user_id(), 2),
            )
            .all()
            .get::<i32>()?;
        assert_eq!(union_value, Some(2));

        let ordered_union_value = database
            .from::<User>()
            .project_value(User::id())
            .union(database.from::<Order>().project_value(Order::user_id()))
            .all()
            .desc(User::id())
            .get::<i32>()?;
        assert_eq!(ordered_union_value, Some(3));

        let union_tuple = database
            .from::<User>()
            .eq(User::id(), 2)
            .project_tuple((User::id(), User::name()))
            .union(
                database
                    .from::<User>()
                    .eq(User::id(), 2)
                    .project_tuple((User::id(), User::name())),
            )
            .all()
            .get::<(i32, String)>()?;
        assert_eq!(union_tuple, Some((2, "Bob".to_string())));

        let ordered_union_tuple = database
            .from::<User>()
            .eq(User::id(), 1)
            .project_tuple((User::id(), User::name()))
            .union(
                database
                    .from::<User>()
                    .eq(User::id(), 2)
                    .project_tuple((User::id(), User::name())),
            )
            .desc(User::id())
            .get::<(i32, String)>()?;
        assert_eq!(ordered_union_tuple, Some((2, "Bob".to_string())));

        let union_projection = database
            .from::<User>()
            .eq(User::id(), 2)
            .project::<UserSummary>()
            .union(
                database
                    .from::<User>()
                    .eq(User::id(), 2)
                    .project::<UserSummary>(),
            )
            .all()
            .get()?;
        assert_eq!(
            union_projection,
            Some(UserSummary {
                id: 2,
                display_name: "Bob".to_string(),
                age: Some(30),
            })
        );

        let ordered_union_projection = database
            .from::<User>()
            .eq(User::id(), 1)
            .project::<UserSummary>()
            .union(
                database
                    .from::<User>()
                    .eq(User::id(), 2)
                    .project::<UserSummary>(),
            )
            .desc(User::id())
            .get()?;
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
        let database = DataBaseBuilder::path(".").build_in_memory()?;

        database.create_table::<EventLog>()?;
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

        let repeated_categories = database
            .from::<EventLog>()
            .project_value(EventLog::category())
            .group_by(EventLog::category())
            .having(count_all().gt(1))
            .fetch::<String>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(repeated_categories, vec!["alpha"]);

        let distinct_categories = database
            .from::<EventLog>()
            .distinct()
            .project_value(EventLog::category())
            .asc(EventLog::category())
            .fetch::<String>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(distinct_categories, vec!["alpha", "beta"]);

        let distinct_category_count = database
            .from::<EventLog>()
            .distinct()
            .project_value(EventLog::category())
            .count()?;
        assert_eq!(distinct_category_count, 2);

        let distinct_limited_count = database
            .from::<EventLog>()
            .distinct()
            .project_value(EventLog::category())
            .asc(EventLog::category())
            .limit(1)
            .count()?;
        assert_eq!(distinct_limited_count, 1);

        let grouped_count = database
            .from::<EventLog>()
            .project_value(EventLog::category())
            .group_by(EventLog::category())
            .having(count_all().gt(1))
            .count()?;
        assert_eq!(grouped_count, 1);

        let grouped_scores = database
            .from::<EventLog>()
            .project_tuple((
                EventLog::category(),
                sum(EventLog::score()).alias("total_score"),
            ))
            .group_by(EventLog::category())
            .having(count_all().gt(0))
            .asc(EventLog::category())
            .fetch::<(String, i32)>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            grouped_scores,
            vec![("alpha".to_string(), 30), ("beta".to_string(), 5)]
        );

        let grouped_stats = database
            .from::<EventLog>()
            .project_tuple((
                EventLog::category(),
                sum(EventLog::score()).alias("total_score"),
                count_all().alias("total_count"),
            ))
            .group_by(EventLog::category())
            .having(count_all().gt(0))
            .asc(EventLog::category())
            .fetch::<(String, i32, i32)>()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            grouped_stats,
            vec![("alpha".to_string(), 30, 2), ("beta".to_string(), 5, 1),]
        );

        let grouped_stats_schema = database
            .from::<EventLog>()
            .project_tuple((
                EventLog::category(),
                sum(EventLog::score()).alias("total_score"),
                count_all().alias("total_count"),
            ))
            .group_by(EventLog::category())
            .asc(EventLog::category())
            .raw()?;
        assert_eq!(
            grouped_stats_schema
                .schema()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>(),
            vec!["category", "total_score", "total_count"]
        );
        grouped_stats_schema.done()?;

        database.drop_table::<EventLog>()?;

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
