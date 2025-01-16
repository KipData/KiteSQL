use async_trait::async_trait;
use clap::Parser;
use futures::stream;
use kite_sql::db::{DBTransaction, DataBaseBuilder, Database, ResultIter};
use kite_sql::errors::DatabaseError;
use kite_sql::storage::rocksdb::RocksStorage;
use kite_sql::types::tuple::{Schema, SchemaRef, Tuple};
use kite_sql::types::LogicalType;
use log::{error, info, LevelFilter};
use parking_lot::Mutex;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;
use std::fmt::Debug;
use std::io;
use std::mem::transmute;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::Arc;
use tokio::net::TcpListener;

pub(crate) const BANNER: &str = "
oooo    oooo  o8o      .                 .oooooo..o   .oooooo.      ooooo
`888   .8P'   `\"'    .o8                d8P'    `Y8  d8P'  `Y8b     `888'
 888  d8'    oooo  .o888oo  .ooooo.     Y88bo.      888      888     888
 88888[      `888    888   d88' `88b     `\"Y8888o.  888      888     888
 888`88b.     888    888   888ooo888         `\"Y88b 888      888     888
 888  `88b.   888    888 . 888    .o    oo     .d8P `88b    d88b     888       o
o888o  o888o o888o   \"888\" `Y8bod8P'    8\"\"88888P'   `Y8bood8P'Ybd' o888ooooood8

";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "127.0.0.1")]
    ip: String,
    #[clap(long, default_value = "5432")]
    port: u16,
    #[clap(long, default_value = "./kitesql_data")]
    path: String,
}

struct TransactionPtr(NonNull<DBTransaction<'static, RocksStorage>>);

impl Deref for TransactionPtr {
    type Target = NonNull<DBTransaction<'static, RocksStorage>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TransactionPtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

unsafe impl Send for TransactionPtr {}
unsafe impl Sync for TransactionPtr {}

pub struct KiteSQLBackend {
    inner: Arc<Database<RocksStorage>>,
}

impl KiteSQLBackend {
    pub fn new(path: impl Into<PathBuf> + Send) -> Result<KiteSQLBackend, DatabaseError> {
        let database = DataBaseBuilder::path(path).build()?;

        Ok(KiteSQLBackend {
            inner: Arc::new(database),
        })
    }
}

pub struct SessionBackend {
    inner: Arc<Database<RocksStorage>>,
    tx: Mutex<Option<TransactionPtr>>,
}

impl SessionBackend {
    pub fn new(inner: Arc<Database<RocksStorage>>) -> SessionBackend {
        SessionBackend {
            inner,
            tx: Mutex::new(None),
        }
    }
}

impl NoopStartupHandler for SessionBackend {}

struct CustomBackendFactory {
    handler: Arc<SessionBackend>,
}

impl CustomBackendFactory {
    pub fn new(handler: Arc<SessionBackend>) -> CustomBackendFactory {
        CustomBackendFactory { handler }
    }
}

impl PgWireServerHandlers for CustomBackendFactory {
    type StartupHandler = SessionBackend;
    type SimpleQueryHandler = SessionBackend;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

#[async_trait]
impl SimpleQueryHandler for SessionBackend {
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        match query.to_uppercase().as_str() {
            "BEGIN;" | "BEGIN" | "START TRANSACTION;" | "START TRANSACTION" => {
                let mut guard = self.tx.lock();

                if guard.is_some() {
                    return Err(PgWireError::ApiError(Box::new(
                        DatabaseError::TransactionAlreadyExists,
                    )));
                }
                let transaction = self
                    .inner
                    .new_transaction()
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                guard.replace(TransactionPtr(
                    Box::leak(Box::<DBTransaction<'static, RocksStorage>>::new(unsafe {
                        transmute(transaction)
                    }))
                    .into(),
                ));

                Ok(vec![Response::Execution(Tag::new("OK"))])
            }
            "COMMIT;" | "COMMIT" | "COMMIT WORK;" | "COMMIT WORK" => {
                let mut guard = self.tx.lock();

                if let Some(transaction) = guard.take() {
                    unsafe { Box::from_raw(transaction.as_ptr()) }
                        .commit()
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                    Ok(vec![Response::Execution(Tag::new("OK"))])
                } else {
                    Err(PgWireError::ApiError(Box::new(
                        DatabaseError::NoTransactionBegin,
                    )))
                }
            }
            "ROLLBACK;" | "ROLLBACK" => {
                let mut guard = self.tx.lock();

                if let Some(transaction) = guard.take() {
                    unsafe { drop(Box::from_raw(transaction.as_ptr())) }
                } else {
                    return Err(PgWireError::ApiError(Box::new(
                        DatabaseError::NoTransactionBegin,
                    )));
                }

                Ok(vec![Response::Execution(Tag::new("OK"))])
            }
            _ => {
                let mut guard = self.tx.lock();

                let mut tuples = Vec::new();
                let response = if let Some(transaction) = guard.as_mut() {
                    let mut iter = unsafe { transaction.as_mut().run(query) }
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    for tuple in iter.by_ref() {
                        tuples.push(tuple.map_err(|e| PgWireError::ApiError(Box::new(e)))?);
                    }
                    let schema = iter.schema().clone();
                    iter.done()
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    encode_tuples(&schema, tuples)?
                } else {
                    let mut iter = self
                        .inner
                        .run(query)
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    for tuple in iter.by_ref() {
                        tuples.push(tuple.map_err(|e| PgWireError::ApiError(Box::new(e)))?);
                    }
                    let schema = iter.schema().clone();
                    iter.done()
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                    encode_tuples(&schema, tuples)?
                };
                Ok(vec![Response::Query(response)])
            }
        }
    }
}

fn encode_tuples<'a>(schema: &SchemaRef, tuples: Vec<Tuple>) -> PgWireResult<QueryResponse<'a>> {
    if tuples.is_empty() {
        return Ok(QueryResponse::new(Arc::new(vec![]), stream::empty()));
    }

    let mut results = Vec::with_capacity(tuples.len());
    let schema = Arc::new(
        schema
            .iter()
            .map(|column| {
                let pg_type = into_pg_type(column.datatype())?;

                Ok(FieldInfo::new(
                    column.name().into(),
                    None,
                    None,
                    pg_type,
                    FieldFormat::Text,
                ))
            })
            .collect::<PgWireResult<Vec<FieldInfo>>>()?,
    );

    for tuple in tuples {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for value in tuple.values {
            match value.logical_type() {
                LogicalType::SqlNull => encoder.encode_field(&None::<i8>),
                LogicalType::Boolean => encoder.encode_field(&value.bool()),
                LogicalType::Tinyint => encoder.encode_field(&value.i8()),
                LogicalType::UTinyint => encoder.encode_field(&value.u8().map(|v| v as i8)),
                LogicalType::Smallint => encoder.encode_field(&value.i16()),
                LogicalType::USmallint => encoder.encode_field(&value.u16().map(|v| v as i16)),
                LogicalType::Integer => encoder.encode_field(&value.i32()),
                LogicalType::UInteger => encoder.encode_field(&value.u32()),
                LogicalType::Bigint => encoder.encode_field(&value.i64()),
                LogicalType::UBigint => encoder.encode_field(&value.u64().map(|v| v as i64)),
                LogicalType::Float => encoder.encode_field(&value.float()),
                LogicalType::Double => encoder.encode_field(&value.double()),
                LogicalType::Char(..) | LogicalType::Varchar(..) => {
                    encoder.encode_field(&value.utf8())
                }
                LogicalType::Date => encoder.encode_field(&value.date()),
                LogicalType::DateTime => encoder.encode_field(&value.datetime()),
                LogicalType::Time => encoder.encode_field(&value.time()),
                LogicalType::Decimal(_, _) => {
                    encoder.encode_field(&value.decimal().map(|decimal| decimal.to_string()))
                }
                _ => unreachable!(),
            }?;
        }

        results.push(encoder.finish());
    }

    Ok(QueryResponse::new(schema, stream::iter(results)))
}

fn into_pg_type(data_type: &LogicalType) -> PgWireResult<Type> {
    Ok(match data_type {
        LogicalType::SqlNull => Type::UNKNOWN,
        LogicalType::Boolean => Type::BOOL,
        LogicalType::Tinyint | LogicalType::UTinyint => Type::CHAR,
        LogicalType::Smallint | LogicalType::USmallint => Type::INT2,
        LogicalType::Integer | LogicalType::UInteger => Type::INT4,
        LogicalType::Bigint | LogicalType::UBigint => Type::INT8,
        LogicalType::Float => Type::FLOAT4,
        LogicalType::Double => Type::FLOAT8,
        LogicalType::Varchar(..) => Type::VARCHAR,
        LogicalType::Date | LogicalType::DateTime => Type::DATE,
        LogicalType::Char(..) => Type::CHAR,
        LogicalType::Time => Type::TIME,
        LogicalType::Decimal(_, _) => Type::NUMERIC,
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {data_type}"),
            ))));
        }
    })
}

async fn quit() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = interrupt.recv() => (),
            _ = terminate.recv() => (),
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        let mut signal = tokio::signal::windows::ctrl_c()?;
        let _ = signal.recv().await;

        Ok(())
    }
}

#[tokio::main(worker_threads = 8)]
async fn main() {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Info)
        .init();

    let args = Args::parse();
    info!("{} \nVersion: {}\n", BANNER, env!("CARGO_PKG_VERSION"));
    info!(":) Welcome to the KiteSQL🪁");
    info!("Listen on port {}", args.port);
    info!("Tips: ");
    info!(
        "1. all data is in the \'{}\' folder in the directory where the application is run",
        args.path
    );

    let backend = KiteSQLBackend::new(args.path).unwrap();
    let factory = Arc::new(CustomBackendFactory::new(Arc::new(SessionBackend::new(
        backend.inner,
    ))));
    let server_addr = format!("{}:{}", args.ip, args.port);
    let listener = TcpListener::bind(server_addr).await.unwrap();

    tokio::select! {
        res = server_run(listener,factory) => {
            if let Err(err) = res {
                error!("[Listener][Failed To Accept]: {}", err);
            }
        }
        _ = quit() => info!("Bye!")
    }
}

async fn server_run(
    listener: TcpListener,
    factory_ref: Arc<CustomBackendFactory>,
) -> io::Result<()> {
    loop {
        let incoming_socket = listener.accept().await?;
        let factory_ref = factory_ref.clone();

        tokio::spawn(async move {
            if let Err(err) = process_socket(incoming_socket.0, None, factory_ref).await {
                error!("Failed To Process: {}", err);
            }
        });
    }
}
