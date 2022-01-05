use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
    sync::{Arc, Mutex},
    time::Duration,
};

use actix_web::{
    middleware::Logger,
    post,
    web::{self, Bytes},
    App, HttpResponse, HttpServer, Result as WebResult,
};
use anyhow::Result;
use clap::Parser;
use dashmap::{DashMap, DashSet};

use log::*;
use prost::Message;

use libtaos::{self as taos, Taos, TaosCfgBuilder, TaosCode};

// pub mod protos;
pub mod utils;

use bailongma::*;
use taos::TaosError;
use utils::md5sum;

//use protos::WriteRequest;
fn table_name_escape(name: &str) -> String {
    let s = name
        .replace(":", "_")
        .replace(".", "_")
        .replace("-", "_")
        .replace(" ", "_")
        .to_lowercase();
    if s.len() > 190 {
        s.split_at(190).0.to_string()
    } else {
        s
    }
}
fn tag_name_escape(name: &str) -> String {
    name.replace(":", "_")
        .replace(".", "_")
        .replace("-", "_")
        .to_lowercase()
}

fn tag_value_escape(value: &str) -> String {
    value.replace("\"", "\\\"")
}

async fn handle_stable_schema<'prom>(
    _state: &AppState,
    taos: &Taos,
    database: &str,
    timeseries: &'prom TimeSeries,
) -> Result<()> {
    use itertools::Itertools;
    debug!("handle stable start");
    let (name, labels): (_, Vec<_>) = timeseries
        .labels
        .iter()
        .partition(|label| label.name == "__name__");

    // label __name__ should exist.
    assert!(name.len() == 1);

    // get metrics name
    let metrics_name = &name[0].value;
    let stable_name = table_name_escape(metrics_name);

    let mut schema = taos
        .describe(&format!("{}.{}", database, &stable_name))
        .await;
    trace!("schema: {:?}", &schema);
    //let _ = state.create_table_lock.lock().unwrap();
    if let Err(taos::Error::RawTaosError(taos::TaosError { code, ref err })) = schema {
        //let _ = state.create_table_lock.lock().unwrap();
        match code {
            taos::TaosCode::MndInvalidTableName => {
                // create super table
                let sql = format!(
                    "create stable if not exists {}.{} (ts timestamp, value double) tags (taghash binary({}), {})",
                    database,
                    stable_name,
                    34, // taghash length
                    labels
                        .iter()
                        .map(|label| { format!("t_{} binary({})", tag_name_escape(&label.name), 128) })  // TODO: default binary length is 128 
                        .join(", ")
                );
                trace!("exec sql: {}", &sql);
                taos.exec(&sql).await?;
                schema = taos
                    .describe(&format!("{}.{}", database, &stable_name))
                    .await;
            }
            taos::TaosCode::MndDbNotSelected => {
                // create database
                let sql = format!("create database if not exists {}", database);
                dbg!(taos.exec(&sql).await?);
                // create super table
                let sql = format!(
                    "create stable if not exists {}.{} (ts timestamp, value double) tags (taghash binary({}), {})",
                    database,
                    stable_name,
                    34, // taghash length
                    labels
                        .iter()
                        .map(|label| { format!("t_{} binary({})", tag_name_escape(&label.name), 128) })  // TODO: default binary length is 128 
                        .join(", ")
                );
                trace!("exec sql: {}", &sql);
                taos.exec(&sql).await?;
                schema = taos
                    .describe(&format!("{}.{}", database, &stable_name))
                    .await;
            }
            _ => {
                error!("error: TaosError {{ code: {}, err: {} }}", code, err);
                return Err(taos::Error::RawTaosError(TaosError {
                    code,
                    err: err.clone(),
                })
                .into());
            }
        }
    }

    let schema = schema.unwrap();
    use std::iter::FromIterator;
    let fields = BTreeSet::from_iter(schema.names().into_iter());

    let mut tagmap = BTreeMap::new();
    for label in &labels {
        if !fields.contains(&format!("t_{}", tag_name_escape(&label.name))) {
            let sql = format!(
                "alter stable {}.{} add tag t_{} binary({})",
                database,
                stable_name,
                tag_name_escape(&label.name),
                128
            );
            trace!("add tag {} for stable {}: {}", label.name, stable_name, sql);
            let res = taos.exec(&sql).await;
            if let Err(err) = res {
                match &err {
                    taos::Error::RawTaosError(TaosError { code, err }) => match code {
                        TaosCode::MndFieldAlreayExist => {}
                        _ => {
                            anyhow::bail!(err.clone());
                        }
                    },
                    _ => {}
                }
            }
        }
        tagmap.insert(&label.name, &label.value);
    }

    let tag_values = labels.iter().map(|label| &label.value).join("");
    let table_name = format!("{}{}", metrics_name, tag_values);
    let table_name = format!("md5_{}", md5sum(table_name.as_bytes()));
    let taghash = md5sum(tagmap.values().join("").as_bytes());

    // create sub table;
    // FIXME: It's better to keep a table exist set.
    let sql = format!(
        "create table if not exists {}.{} using {}.{} (taghash,{}) tags(\"{}\",{})",
        database,
        table_name,
        database,
        stable_name,
        tagmap
            .keys()
            .map(|v| format!("t_{}", tag_name_escape(&v)))
            .join(","),
        taghash,
        tagmap
            .values()
            .map(|value| {
                if value.len() < 127 {
                    format!("\"{}\"", tag_value_escape(value))
                } else {
                    format!("\"{}\"", tag_value_escape(&value[0..127]))
                }
            })
            .join(",")
    );
    debug!("created table {}.{}", database, table_name);
    trace!("create table with sql: {}", sql);
    if let Err(taos::Error::RawTaosError(TaosError { code, err })) = taos.exec(&sql).await {
        if err.contains("tag value too long") {
            error!("tag value too long: {}", sql);
        } else {
            anyhow::bail!(err.clone());
        }
    }
    debug!("handle stable done");
    Ok(())
}
async fn handle_table_schema(
    state: &AppState,
    taos: &Taos,
    database: &str,
    req: &WriteRequest,
) -> Result<()> {
    use futures::stream::{iter, StreamExt};
    let stream = iter(req.timeseries.iter());
    let res = stream
        .then(|ts| async move { handle_stable_schema(state, taos, database, ts).await })
        .collect::<Vec<_>>()
        .await;
    for i in res {
        let _ = i?;
    }
    // let stream = iter(req.timeseries.iter().map(|ts| {
    // handle_stable_schema(state, taos, database, ts)
    // }));
    // let vec: Vec<_> = TryStreamExt::try_collect(stream).await;
    // for ts in &req.timeseries {
    //     // handle stable
    //     handle_stable_schema(state, taos, database, ts).await?;
    // }
    debug!("handle table schema done");
    Ok(())
}
async fn write_tdengine_from_prometheus(
    state: &AppState,
    database: &str,
    req: &WriteRequest,
) -> Result<()> {
    use itertools::Itertools;
    debug!("Write tdengine from prometheus write request");
    let taos = state.pool.get()?;
    let taos = taos.deref();
    // build insert sql
    let chunks = req
        .timeseries
        .iter()
        .map(|ts| {
            let (name, labels): (_, Vec<_>) =
                ts.labels.iter().partition(|label| label.name == "__name__");
            // label __name__ should exist.
            assert!(name.len() == 1);

            // get metrics name
            let metrics_name = &name[0].value;
            let tag_values = labels.iter().map(|label| &label.value).join("");
            let table_name = format!("{}{}", metrics_name, tag_values);
            let table_name = format!("{}.md5_{}", database, md5sum(table_name.as_bytes()));
            ts.samples.iter().map(move |sample| match sample.value {
                Some(value) if value.is_nan() => {
                    format!(" {} values ({}, NULL)", table_name, sample.timestamp)
                }
                None => format!(" {} values ({}, NULL)", table_name, sample.timestamp),
                Some(value) => format!(" {} values ({}, {})", table_name, sample.timestamp, value),
            })
        })
        .flatten()
        .chunks(state.opts.chunk_size)
        .into_iter()
        .map(|mut chunk| chunk.join(""))
        .collect_vec();

    for chunk in chunks {
        let sql = format!("insert into {}", chunk);
        debug!("chunk sql length is {}", sql.len());

        if let Err(err) = taos.query(&sql).await {
            match err {
                taos::Error::RawTaosError(err) => match err.code {
                    TaosCode::MndDbNotSelected | TaosCode::MndInvalidTableName => {
                        handle_table_schema(&state, taos, database, &req).await?;
                        taos.query(&sql).await?;
                    }
                    code => {
                        warn!("insert into tdengine error: [{}]{}", code, err);
                    }
                },
                err => {
                    error!("error with query [{}]: {}", sql.len(), err);
                }
            }
        }
    }

    Ok(())
}

#[post("/adapters/prometheus/write")]
async fn prometheus(
    state: web::Data<Arc<AppState>>,
    web::Query(options): web::Query<PrometheusOptions>,
    bytes: Bytes,
) -> WebResult<HttpResponse> {
    let bytes = bytes.deref();

    info!("recieved {} bytes from prometheus", bytes.len());
    let database = options.database;
    let database = database.unwrap_or("prometheus".to_string());

    let mut decoder = snap::raw::Decoder::new();
    let decompressed = decoder
        .decompress_vec(bytes)
        .map_err(|_| actix_web::error::ErrorNotAcceptable("bad snappy stream"))?;

    let write_request = WriteRequest::decode(&mut decompressed.as_ref()).map_err(|prost_err| {
        // decompressed.len();
        let err = "bad prometheus write request: deserializing error";
        error!(
            "{}, protolens: {}, raw error: {:?}",
            err,
            decompressed.len(),
            prost_err
        );
        actix_web::error::ErrorNotAcceptable(err)
    })?;
    drop(decompressed); // drop decompressed data, it'll not be used after

    // std::fs::write(format!("prom-failed-{}.json", md5sum(bytes)), serde_json::to_string(&write_request).unwrap())?;

    // write tdengine, retry max 10 times if error.
    for _i in 0..10i32 {
        let res = write_tdengine_from_prometheus(&state, &database, &write_request).await;
        if let Err(err) = res {
            warn!(
                "write tdengine error : {}\nbacktraces: {}",
                // i,
                err,
                err.backtrace()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            return Ok(HttpResponse::Ok().finish());
        }
    }

    // if not success, write data and save it to persistent storage.
    error!("failed with retries, the data will be lost");
    std::fs::write(format!("prom-failed-write-{}.snappy", md5sum(bytes)), bytes)?;
    // });
    // body is loaded, now we can deserialize
    Ok(HttpResponse::InternalServerError().finish())
}

#[derive(Debug, serde::Deserialize)]
struct PrometheusOptions {
    database: Option<String>,
}
#[post("/adapters/prometheus/read")]
async fn prometheus_read_handler(
    state: web::Data<Arc<AppState>>,
    web::Query(options): web::Query<PrometheusOptions>,
    bytes: Bytes,
) -> WebResult<HttpResponse> {
    let bytes = bytes.deref();

    info!("recieved {} bytes from prometheus", bytes.len());

    let database = options.database;
    let database = database.unwrap_or("prometheus".to_string());
    // let database = database.to_string();

    let mut decoder = snap::raw::Decoder::new();
    let decompressed = decoder
        .decompress_vec(bytes)
        .map_err(|_| actix_web::error::ErrorNotAcceptable("bad snappy stream"))?;

    let read_request = ReadRequest::decode(&mut decompressed.as_ref()).map_err(|_| {
        actix_web::error::ErrorNotAcceptable("bad prometheus read request: deserializing error")
    })?;
    drop(decompressed); // drop decompressed data, it'll not be used after
    let taos = state.pool.get().expect("get connection from pool");
    let taos = taos.deref();
    for _i in 0..10i32 {
        let res = prometheus_read(taos, &database, &read_request).await;
        if let Err(err) = res {
            warn!("read tdengine error : {}", err,);
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            let res = res.unwrap();
            let mut buf: Vec<u8> = vec![];
            let _ = res.encode(&mut buf).map_err(|_| {
                actix_web::error::ErrorNotAcceptable("failed encode protobuf message")
            })?;
            let mut encoder = snap::raw::Encoder::new();
            let compressed = encoder.compress_vec(&buf).map_err(|_| {
                actix_web::error::ErrorNotAcceptable("failed to compress with snappy method")
            })?;
            return Ok(HttpResponse::Ok().body(compressed));
        }
    }

    // if not success, write data and save it to persistent storage.
    error!("failed with retries, the data will be lost");
    std::fs::write(format!("prom-failed-read-{}.snappy", md5sum(bytes)), bytes)?;
    // });
    // body is loaded, now we can deserialize
    Ok(HttpResponse::InternalServerError().finish())
}

/// TDengine adapter for prometheus.
#[derive(Debug, Clone, Parser)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
#[clap(version, author)]
struct Opts {
    /// Debug level
    #[clap(short, long, default_value = "info")]
    level: log::LevelFilter,
    /// TDengine host IP or hostname.
    #[clap(short, long, default_value = "localhost")]
    host: String,
    /// TDengine server port
    #[clap(short, long, default_value = "6030")]
    port: u16,
    /// TDengine user
    #[clap(short, long, default_value = "root")]
    user: String,
    /// TDengine password
    #[clap(short = 'P', long, default_value = "taosdata")]
    password: String,
    /// Listen to an specific ip and port
    #[clap(short = 'L', long, default_value = "0.0.0.0:10203")]
    listen: String,
    /// Thread works for web request
    #[clap(short, long, default_value = "10")]
    workers: usize,
    /// Sql chunk size.
    ///
    /// The larger your table column size is, the small chunk should be setted.
    #[clap(short, long, default_value = "600")]
    chunk_size: usize,
    /// Max TDengine connections
    ///
    ///   - in concurrent cases, use max as 50000
    ///   - for common use, set it as 5000
    #[clap(short = 'C', long, default_value = "500")]
    max_connections: u32,
    /// Max memroy, unit: GB
    #[clap(short = 'M', long, default_value = "50")]
    max_memory: u64,

    /// Tag data type
    #[clap(short = 't', long, default_value = "binary")]
    tag_type: String,
}

#[derive(Debug, Default)]
pub struct Tables(DashSet<String>);

impl Tables {
    pub fn exist(&self, name: &str) -> bool {
        self.0.contains(name)
    }
    pub fn add_table(&mut self, name: impl Into<String>) {
        self.0.insert(name.into());
    }
}

type StableHandler = DashMap<String, (DashSet<String>, DashMap<String, Tables>)>;
#[derive(Debug, Default)]
pub struct DatabasesHandler(
    DashMap<String, DashMap<String, (DashSet<String>, DashMap<String, Tables>)>>,
);

impl DatabasesHandler {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn assert_database<'a>(
        &'a self,
        database: &str,
    ) -> dashmap::mapref::one::Ref<String, StableHandler> {
        match self.0.get(database) {
            Some(st) => st,
            None => {
                self.0.insert(database.to_string(), Default::default());
                self.0.get(database).unwrap()
            }
        }
    }
}
#[derive(Debug)]
pub struct AppState {
    opts: Opts,
    pool: taos::TaosPool,
    create_table_lock: Mutex<i32>,
    tables: DatabasesHandler,
    max_memory: u64,
}

#[actix_web::main]
async fn main() -> Result<()> {
    let opts: Opts = Opts::parse();

    std::env::set_var(
        "RUST_LOG",
        format!(
            "actix_web=info,bailongma={level},main={level}",
            level = opts.level.to_string()
        ),
    );
    // fern::Dispatch::new()
    //         .level(opts.level)
    //         .chain(std::io::stdout())
    //         .chain(fern::log_file("output.log")?)
    //         .apply()?;
    env_logger::init();
    //dbg!(&opts);
    //let create_table_lock = Arc::new(Mutex::new(0));
    let taos_cfg = TaosCfgBuilder::default()
        .ip(&opts.host)
        .user(&opts.user)
        .pass(&opts.password)
        .db("log")
        .port(opts.port)
        .build()
        .expect("ToasCfg builder error");
    let taos_pool = r2d2::Pool::builder()
        .max_size(opts.max_connections)
        .test_on_check_out(false)
        .connection_timeout(Duration::from_secs(500))
        .max_lifetime(Some(Duration::from_secs(600)))
        .idle_timeout(Some(Duration::from_secs(300)))
        .build(taos_cfg)?;
    let workers = opts.workers;
    let listen = opts.listen.clone();

    let max_memory = opts.max_memory;
    let state = Arc::new(AppState {
        opts,
        pool: taos_pool.clone(),
        create_table_lock: Default::default(),
        tables: Default::default(),
        max_memory: max_memory * 1024 * 1024,
    });
    let server = HttpServer::new(move || {
        App::new()
            .data(state.clone())
            .wrap(Logger::default())
            .service(prometheus)
            .service(prometheus_read_handler)
    })
    .workers(workers)
    .bind(&listen)?
    .run();
    info!("start server, listen on {}", listen);

    server.await?;
    Ok(())
}
