/// # Examples
///
/// ```
///struct MyStruct {
///   c1: i32,
///   c2: String,
///}
///
///implement_from_tuple!(
///     MyStruct, (
///         c1: i32 => |inner: &mut MyStruct, value| {
///             if let DataValue::Int32(Some(val)) = value {
///                 inner.c1 = val;
///             }
///         },
///         c2: String => |inner: &mut MyStruct, value| {
///             if let DataValue::Utf8(Some(val)) = value {
///                 inner.c2 = val;
///             }
///         }
///     )
/// );
/// ```
#[macro_export]
macro_rules! implement_from_tuple {
    ($struct_name:ident, ($($field_name:ident : $field_type:ty => $closure:expr),+)) => {
        impl From<(&::kite_sql::types::tuple::SchemaRef, ::kite_sql::types::tuple::Tuple)> for $struct_name {
            fn from((schema, mut tuple): (&::kite_sql::types::tuple::SchemaRef, ::kite_sql::types::tuple::Tuple)) -> Self {
                fn try_get<T: 'static>(tuple: &mut ::kite_sql::types::tuple::Tuple, schema: &::kite_sql::types::tuple::SchemaRef, field_name: &str) -> Option<::kite_sql::types::value::DataValue> {
                    let ty = ::kite_sql::types::LogicalType::type_trans::<T>()?;
                    let (idx, _) = schema
                        .iter()
                        .enumerate()
                        .find(|(_, col)| col.name() == field_name)?;

                    std::mem::replace(&mut tuple.values[idx], ::kite_sql::types::value::DataValue::Null).cast(&ty).ok()
                }

                let mut struct_instance = $struct_name::default();
                $(
                    if let Some(value) = try_get::<$field_type>(&mut tuple, schema, stringify!($field_name)) {
                        $closure(
                            &mut struct_instance,
                            value
                        );
                    }
                )+
                struct_instance
            }
        }
    };
}

/// # Examples
///
/// ```
/// scala_function!(MyFunction::sum(LogicalType::Integer, LogicalType::Integer) -> LogicalType::Integer => |v1: DataValue, v2: DataValue| {
///     DataValue::binary_op(&v1, &v2, &BinaryOperator::Plus)
/// });
///
/// let kite_sql = DataBaseBuilder::path("./example")
///     .register_scala_function(TestFunction::new())
///     .build()
///     ?;
/// ```
#[macro_export]
macro_rules! scala_function {
    ($struct_name:ident::$function_name:ident($($arg_ty:expr),*) -> $return_ty:expr => $closure:expr) => {
        #[derive(Debug, ::serde::Serialize, ::serde::Deserialize)]
        pub(crate) struct $struct_name {
            summary: ::kite_sql::expression::function::FunctionSummary
        }

        impl $struct_name {
            #[allow(unused_mut)]
            pub(crate) fn new() -> Arc<Self> {
                let function_name = stringify!($function_name).to_lowercase();

                let mut arg_types = Vec::new();
                $({
                    arg_types.push($arg_ty);
                })*

                Arc::new(Self {
                    summary: ::kite_sql::expression::function::FunctionSummary {
                        name: function_name,
                        arg_types
                    }
                })
            }
        }

        #[typetag::serde]
        impl ::kite_sql::expression::function::scala::ScalarFunctionImpl for $struct_name {
            #[allow(unused_variables, clippy::redundant_closure_call)]
            fn eval(&self, args: &[::kite_sql::expression::ScalarExpression], tuple: Option<(&::kite_sql::types::tuple::Tuple, &[::kite_sql::catalog::column::ColumnRef])>) -> Result<::kite_sql::types::value::DataValue, ::kite_sql::errors::DatabaseError> {
                let mut _index = 0;

                $closure($({
                    let mut value = args[_index].eval(tuple)?;
                    _index += 1;

                    if value.logical_type() != $arg_ty {
                        value = value.cast(&$arg_ty)?;
                    }
                    value
                }, )*)
            }

            fn monotonicity(&self) -> Option<::kite_sql::expression::function::scala::FuncMonotonicity> {
                todo!()
            }

            fn return_type(&self) -> &::kite_sql::types::LogicalType {
                &$return_ty
            }

            fn summary(&self) -> &::kite_sql::expression::function::FunctionSummary {
                &self.summary
            }
        }
    };
}

/// # Examples
///
/// ```
/// table_function!(MyTableFunction::test_numbers(LogicalType::Integer) -> [c1: LogicalType::Integer, c2: LogicalType::Integer] => (|v1: DataValue| {
///     let num = v1.i32().unwrap();
///
///     Ok(Box::new((0..num)
///         .into_iter()
///         .map(|i| Ok(Tuple::new(None, vec![
///                 DataValue::Int32(Some(i)),
///                 DataValue::Int32(Some(i)),
///             ])))) as Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>)
///     }));
///
///     let kite_sql = DataBaseBuilder::path("./example")
///         .register_table_function(MyTableFunction::new())
///         .build()
///     ?;
/// ```
#[macro_export]
macro_rules! table_function {
    ($struct_name:ident::$function_name:ident($($arg_ty:expr),*) -> [$($output_name:ident: $output_ty:expr),*] => $closure:expr) => {
        static $function_name: ::std::sync::LazyLock<::kite_sql::catalog::table::TableCatalog> = ::std::sync::LazyLock::new(|| {
            let mut columns = Vec::new();

            $({
                columns.push(::kite_sql::catalog::column::ColumnCatalog::new(stringify!($output_name).to_lowercase(), true, ::kite_sql::catalog::column::ColumnDesc::new($output_ty, None, false, None).unwrap()));
            })*
            ::kite_sql::catalog::table::TableCatalog::new(Arc::new(stringify!($function_name).to_lowercase()), columns).unwrap()
        });

        #[derive(Debug, ::serde::Serialize, ::serde::Deserialize)]
        pub(crate) struct $struct_name {
            summary: ::kite_sql::expression::function::FunctionSummary
        }

        impl $struct_name {
            #[allow(unused_mut)]
            pub(crate) fn new() -> Arc<Self> {
                let function_name = stringify!($function_name).to_lowercase();

                let mut arg_types = Vec::new();
                $({
                    arg_types.push($arg_ty);
                })*

                Arc::new(Self {
                    summary: ::kite_sql::expression::function::FunctionSummary {
                        name: function_name,
                        arg_types
                    }
                })
            }
        }

        #[typetag::serde]
        impl ::kite_sql::expression::function::table::TableFunctionImpl for $struct_name {
            #[allow(unused_variables, clippy::redundant_closure_call)]
            fn eval(&self, args: &[::kite_sql::expression::ScalarExpression]) -> Result<Box<dyn Iterator<Item=Result<::kite_sql::types::tuple::Tuple, ::kite_sql::errors::DatabaseError>>>, ::kite_sql::errors::DatabaseError> {
                let mut _index = 0;

                $closure($({
                    let mut value = args[_index].eval(None)?;
                    _index += 1;

                    if value.logical_type() != $arg_ty {
                        value = value.cast(&$arg_ty)?;
                    }
                    value
                }, )*)
            }

            fn output_schema(&self) -> &::kite_sql::types::tuple::SchemaRef {
                $function_name.schema_ref()
            }

            fn summary(&self) -> &::kite_sql::expression::function::FunctionSummary {
                &self.summary
            }

            fn table(&self) -> &'static ::kite_sql::catalog::table::TableCatalog {
                &$function_name
            }
        }
    };
}
