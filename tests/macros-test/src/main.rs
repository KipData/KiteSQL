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
    use kite_sql::types::evaluator::EvaluatorFactory;
    use kite_sql::types::tuple::{SchemaRef, Tuple};
    use kite_sql::types::value::{DataValue, Utf8Type};
    use kite_sql::types::LogicalType;
    use kite_sql::{from_tuple, scala_function, table_function, Model};
    use sqlparser::ast::CharLengthUnits;
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
    struct User {
        #[model(primary_key)]
        id: i32,
        #[model(rename = "user_name")]
        name: String,
        age: Option<i32>,
        #[model(skip)]
        cache: String,
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
    fn test_orm_crud() -> Result<(), DatabaseError> {
        let database = DataBaseBuilder::path(".").build_in_memory()?;

        database
            .run("create table users (id int primary key, user_name varchar, age int)")?
            .done()?;

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

        let users = database.list::<User>()?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].name, "Alice");

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
        database.delete_by_id::<User>(&2)?;
        assert!(database.get::<User>(&2)?.is_none());

        Ok(())
    }

    scala_function!(MyScalaFunction::SUM(LogicalType::Integer, LogicalType::Integer) -> LogicalType::Integer => (|v1: DataValue, v2: DataValue| {
        EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Plus)?.binary_eval(&v1, &v2)
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
