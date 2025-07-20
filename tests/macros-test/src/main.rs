fn main() {}

#[cfg(test)]
mod test {
    use kite_sql::catalog::column::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation};
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
    use kite_sql::{implement_from_tuple, scala_function, table_function};
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

    implement_from_tuple!(
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
                name: "sum".to_string(),
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
                name: "test_numbers".to_string(),
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
        let table_name = Arc::new("test_numbers".to_string());
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
