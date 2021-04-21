use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
    sync::{Arc, Mutex, RwLock},
};

use std::path::PathBuf;

use actix_web::{
    middleware::Logger,
    post,
    web::{self, Bytes},
    App, HttpRequest, HttpResponse, HttpServer, Responder, Result as WebResult,
};
use anyhow::{Result, bail};
use clap::Clap;
use itertools::Itertools;
use log::*;
use prost::Message;

use libtdengine::{self as taos, Taos, TaosCfg, TaosCfgBuilder, TaosCode, TaosPool};

use fnv::{FnvHashMap, FnvHashSet};
pub mod protos;
pub mod utils;

use taos::TaosError;
use utils::*;
//use protos::WriteRequest;

type DatabaseExistsMap = Arc<RwLock<FnvHashMap<String, STableExistsMap>>>;
type STableExistsMap = RwLock<FnvHashMap<String, TableExistsMap>>;
type TableExistsMap = RwLock<FnvHashSet<String>>;

fn table_name_escape(name: &str) -> String {
    let s = name
        .replace(":", "_")
        .replace(".", "_")
        .replace("-", "_")
        .to_lowercase();
    if s.len() > 190 {
        s.split_at(190).0.to_string()
    } else {
        s
    }
}

fn handle_stable_schema<'prom>(
    state: &web::Data<AppState>,
    database: &str,
    timeseries: &'prom protos::TimeSeries,
) -> Result<()> {
    debug!("handle stable start");
    let taos = state.pool.get()?;
    let (name, labels): (_, Vec<_>) = timeseries
        .labels
        .iter()
        .partition(|label| label.name == "__name__");

    // label __name__ should exist.
    assert!(name.len() == 1);

    // get metrics name
    let metrics_name = &name[0].value;
    let stable_name = table_name_escape(metrics_name);

    let mut schema = taos.describe(&format!("{}.{}", database, &stable_name));
    trace!("schema: {:?}", &schema);
    if let Err(taos::Error::RawTaosError(taos::TaosError { code, err })) = schema {
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
                        .map(|label| { format!("t_{} binary({})", label.name, 128) })  // TODO: default binary length is 128 
                        .join(", ")
                );
                trace!("exec sql: {}", &sql);
                taos.exec(&sql)?;
                schema = taos.describe(&format!("{}.{}", database, &stable_name));
            }
            _ => {
                error!("error: TaosError {{ code: {}, err: {} }}", code, err);
                Err(taos::Error::RawTaosError(TaosError { code, err }))?;
            }
        }
    }

    let schema = schema.unwrap();
    use std::iter::FromIterator;
    let fields = BTreeSet::from_iter(schema.iter().map(|row| {
        row.first()
            .expect("first show always exist in describe results")
            .to_string()
    }));

    let mut tagmap = BTreeMap::new();
    for label in &labels {
        if !fields.contains(&format!("t_{}", &label.name)) {
            let sql = format!(
                "alter stable {}.{} add tag t_{} binary({})",
                database, stable_name, &label.name, 128
            );
            trace!("add tag {} for stable {}: {}", label.name, stable_name, sql);
            taos.exec(&sql)?;
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
        tagmap.keys().map(|v| format!("t_{}", v)).join(","),
        taghash,
        tagmap
            .values()
            .map(|value| format!("\"{}\"", value))
            .join(",")
    );
    debug!("created table {}.{}", database, table_name);
    trace!("create table with sql: {}", sql);
    taos.exec(&sql)?;
    debug!("handle stable done");
    Ok(())
}
fn handle_table_schema(
    state: &web::Data<AppState>,
    database: &str,
    req: &protos::WriteRequest,
) -> Result<()> {
    for ts in &req.timeseries {
        // handle stable
        handle_stable_schema(state, database, ts)?;
    }
    trace!("handle table schema");
    Ok(())
}
async fn write_tdengine_from_prometheus(
    state: &web::Data<AppState>,
    database: &str,
    req: &protos::WriteRequest,
) -> Result<()> {
    debug!("Write tdengine from prometheus write request");
    let taos = state.pool.get()?;
    // build insert sql
    let sql = req
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
            ts.samples
                .iter()
                .map(|sample| {
                    format!(
                        "{} values ({}, {})",
                        table_name, sample.timestamp, sample.value
                    )
                })
                .join(" ")
        })
        .join(" ");
    let sql = format!("insert into {}", sql);

    if let Err(taos_err) = taos.query(&sql) {
        if let taos::Error::RawTaosError(err) = &taos_err {
            match err.code {
                TaosCode::MnodeDbNotSelected | TaosCode::ClientDbNotSelected => {
                    let _ = state.create_table_lock.lock().unwrap();
                    taos.create_database(database)?;
                    handle_table_schema(&state, database, &req)?;
                    taos.query(&sql)?;
                    return Ok(());
                }
                TaosCode::ClientInvalidTableName | TaosCode::MnodeInvalidTableName => {
                    let _ = state.create_table_lock.lock().unwrap();
                    handle_table_schema(&state, database, &req)?;
                    taos.query(&sql)?;
                    return Ok(());
                }
                code => {
                    warn!("insert into tdengine error: [{}]{}", code, err);
                }
            }
        }
        Err(taos_err)?
    }
    Ok(())
}
#[post("/adapters/prometheus/{database}")]
async fn prometheus(
    //pool: web::Data<TaosPool>,
    //mgr: web::Data<DatabaseExistsMap>,
    //create_table_lock: web::Data<Mutex<i32>>,
    state: web::Data<AppState>,
    web::Path(database): web::Path<String>,
    bytes: Bytes,
) -> WebResult<HttpResponse> {
    let bytes = bytes.deref();
    let mut decoder = snap::raw::Decoder::new();
    let decompressed = decoder.decompress_vec(bytes).expect("decompressingc error");

    let write_request =
        protos::WriteRequest::decode(&mut decompressed.as_ref()).expect("deserialzied ok");

    //println!("{:?}", write_request);

    // write prometheus with 8 retries in case of error
    for i in 0..8 {
        let res =
            write_tdengine_from_prometheus(&state, &database, &write_request).await;
        if res.is_ok() {
            return Ok(HttpResponse::Ok().finish());
        } else {
            error!("error in {} retry: {:?}", i, res);
        }
    }

    // if not success, write data and save it to persistent storage.
    error!("failed with retries, the data will be lost");
    // body is loaded, now we can deserialize
    Ok(HttpResponse::NoContent().finish())
}

/// TDengine adapter for prometheus.
#[derive(Debug, Clap)]
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
    #[clap(short = 'L', long, default_value = "0.0.0.0:10203")]
    listen: String,
    #[clap(short, long, default_value = "10")]
    workers: usize,
}

pub struct AppState {
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
        .max_size(opts.workers as u32 * 2)
        .build(taos_cfg)?;
    let mgr = DatabaseExistsMap::default();
    let state = web::Data::new(AppState {
        pool: taos_pool.clone(),
        create_table_lock: Default::default(),
    });
    let server = HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .wrap(Logger::default())
            .service(prometheus)
    })
    .workers(opts.workers)
    .bind(&opts.listen)?
    .run();

    server.await?;
    Ok(())
}
