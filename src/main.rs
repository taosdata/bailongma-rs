use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use actix_web::{
    middleware::Logger,
    post,
    web::{self, Bytes},
    App, HttpRequest, HttpResponse, HttpServer, Responder, Result as WebResult,
};
use anyhow::Result;
use clap::Clap;
use itertools::Itertools;
use log::*;
use prost::Message;

use libtaos::{self as taos, Taos, TaosCfg, TaosCfgBuilder, TaosCode, TaosPool};

use fnv::{FnvHashMap, FnvHashSet};
pub mod protos;
pub mod utils;

use taos::TaosError;
use utils::*;
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
async fn handle_stable_schema<'prom>(
    state: &web::Data<AppState>,
    taos: &Taos,
    database: &str,
    timeseries: &'prom protos::TimeSeries,
) -> Result<()> {
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
    let _ = state.create_table_lock.lock().unwrap();
    if let Err(taos::Error::RawTaosError(taos::TaosError { code, ref err })) = schema {
        let _ = state.create_table_lock.lock().unwrap();
        match code {
            taos::TaosCode::MnodeInvalidTableName => {
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
            taos::TaosCode::MnodeDbNotSelected => {
                // create database
                let sql = format!("create database {}", database);
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
                Err(taos::Error::RawTaosError(TaosError {
                    code,
                    err: err.clone(),
                }))?;
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
            taos.exec(&sql).await?;
        }
        tagmap.insert(&label.name, &label.value);
    }

    let tag_values = labels.iter().map(|label| &label.value).join("");
    let table_name = format!("{}{}", metrics_name, tag_values);
    let table_name = format!("md5_{}", md5sum(table_name.as_bytes()));
    let taghash = md5sum(tagmap.values().join("").as_bytes());

    // create sub table;

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
            .map(|value| format!("\"{}\"", value))
            .join(",")
    );
    debug!("created table {}.{}", database, table_name);
    trace!("create table with sql: {}", sql);
    taos.exec(&sql).await?;
    debug!("handle stable done");
    Ok(())
}
async fn handle_table_schema(
    state: &web::Data<AppState>,
    taos: &Taos,
    database: &str,
    req: &protos::WriteRequest,
) -> Result<()> {
    for ts in &req.timeseries {
        // handle stable
        handle_stable_schema(state, taos, database, ts).await?;
    }
    debug!("handle table schema done");
    Ok(())
}
async fn write_tdengine_from_prometheus(
    state: &web::Data<AppState>,
    database: &str,
    req: &protos::WriteRequest,
) -> Result<()> {
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
            ts.samples.iter().map(move |sample| {
                format!(
                    " {} values ({}, {})",
                    table_name, sample.timestamp, sample.value
                )
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
                    TaosCode::MnodeDbNotSelected
                    | TaosCode::ClientDbNotSelected
                    | TaosCode::ClientInvalidTableName
                    | TaosCode::MnodeInvalidTableName => {
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

    // while let Some(chunk) = chunks.next().await {

    // }

    // while let Some() {
    //     let sql = chunk.join("");
    //     let sql = format!("insert into {}", sql);
    //     debug!("chunk sql length is {}", sql.len());

    //     if let Err(err) = taos.query(&sql).await {
    //         if let taos::Error::RawTaosError(err) = &err {
    //             match err.code {
    //                 TaosCode::MnodeDbNotSelected
    //                 | TaosCode::ClientDbNotSelected
    //                 | TaosCode::ClientInvalidTableName
    //                 | TaosCode::MnodeInvalidTableName => {
    //                     handle_table_schema(&state, database, &req).await?;
    //                     taos.query(&sql).await?;
    //                     return Ok(());
    //                 }
    //                 code => {
    //                     warn!("insert into tdengine error: [{}]{}", code, err);
    //                 }
    //             }
    //         } else {
    //             error!("error with query [{}] : {}", sql.len(), err);
    //         }
    //         Err(err)?
    //     }
    // }

    // for mut chunk in iter {
    //     let sql = chunk.join("");
    //     let sql = format!("insert into {}", sql);
    //     debug!("chunk sql length is {}", sql.len());

    //     if let Err(err) = taos.query(&sql).await {
    //         if let taos::Error::RawTaosError(err) = &err {
    //             match err.code {
    //                 TaosCode::MnodeDbNotSelected
    //                 | TaosCode::ClientDbNotSelected
    //                 | TaosCode::ClientInvalidTableName
    //                 | TaosCode::MnodeInvalidTableName => {
    //                     handle_table_schema(&state, database, &req).await?;
    //                     taos.query(&sql).await?;
    //                     return Ok(());
    //                 }
    //                 code => {
    //                     warn!("insert into tdengine error: [{}]{}", code, err);
    //                 }
    //             }
    //         } else {
    //             error!("error with query [{}] : {}", sql.len(), err);
    //         }
    //         Err(err)?
    //     }
    // }

    Ok(())
}
#[post("/adapters/prometheus/{database}")]
async fn prometheus(
    state: web::Data<AppState>,
    database: web::Path<String>,
    bytes: Bytes,
) -> WebResult<HttpResponse> {
    let bytes = bytes.deref();
    let database = database.as_str();
    let mut decoder = snap::raw::Decoder::new();
    let decompressed = decoder.decompress_vec(bytes).expect("decompressingc error");

    let write_request =
        protos::WriteRequest::decode(&mut decompressed.as_ref()).expect("deserialzied ok");

    //println!("{:?}", write_request);

    // write prometheus with 8 retries in case of error
    let database = database.to_string();
    tokio::spawn(async move {
        for i in 0..8i32 {
            let res = write_tdengine_from_prometheus(&state, &database, &write_request).await;
            if let Err(err) = res {
                error!("error in {} retry: {}", i, err.backtrace());
            } else {
                return;
            }
        }

        // if not success, write data and save it to persistent storage.
        error!("failed with retries, the data will be lost");
    });
    // body is loaded, now we can deserialize
    Ok(HttpResponse::NoContent().finish())
}

/// TDengine adapter for prometheus.
#[derive(Debug, Clone, Clap)]
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
    #[clap(short, long, default_value = "50000")]
    max_connections: u32,
}

#[derive(Debug)]
pub struct AppState {
    opts: Opts,
    pool: taos::TaosPool,
    create_table_lock: Mutex<i32>,
}

#[actix_web::main]
async fn main() -> Result<()> {
    let opts: Opts = Opts::parse();

    std::env::set_var(
        "RUST_LOG",
        format!("actix_web=info,bailongma={}", opts.level.to_string()),
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
    let state = web::Data::new(AppState {
        opts,
        pool: taos_pool.clone(),
        create_table_lock: Default::default(),
    });
    let server = HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .wrap(Logger::default())
            .service(prometheus)
    })
    .workers(workers)
    .bind(&listen)?
    .run();

    server.await?;
    Ok(())
}
