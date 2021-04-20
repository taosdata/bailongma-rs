use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
    sync::{Arc, RwLock},
};

use actix_web::{
    middleware::Logger,
    post,
    web::{self, Bytes},
    App, HttpRequest, HttpResponse, HttpServer, Responder, Result as WebResult,
};
use anyhow::Result;
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

fn create_database(taos: &Taos, database: &str) -> Result<()> {
    taos.query(&format!("create database if not exists {}", database))?;
    Ok(())
}

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
    taos: &Taos,
    database: &str,
    timeseries: &'prom protos::TimeSeries,
) -> Result<()> {
    info!("handle stable start");
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
    trace!("{:?}", &schema);
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
    dbg!(&fields);

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
    dbg!(&tagmap);

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
    info!("create table {}.{} done", database, table_name);
    trace!("create table with sql: {}", sql);
    taos.exec(&sql)?;
    info!("handle stable done");
    Ok(())
}
fn handle_table_schema(taos: &Taos, database: &str, req: &protos::WriteRequest) -> Result<()> {
    for ts in &req.timeseries {
        // handle stable
        handle_stable_schema(taos, database, ts)?;
    }
    trace!("handle table schema");
    Ok(())
}
async fn write_tdengine_from_prometheus_write_request(
    taos: &Taos,
    mgr: &DatabaseExistsMap,
    database: &str,
    req: protos::WriteRequest,
) -> Result<()> {
    trace!("Write tdengine from prometheus write request");

    // 1. check table exists.
    let database_exists = {
        let mgr = mgr.read().unwrap();
        trace!("manager: {:?}", mgr.deref());
        mgr.contains_key(database)
    };
    if !database_exists {
        // create database
        trace!("database does not exist, create it: {}", database);
        let mut mgr = mgr.write().unwrap();
        trace!("get write lock");
        let _ = create_database(taos, database)?;
        mgr.insert(database.into(), Default::default());
        trace!("manager: {:?}", mgr.deref());
        trace!("database created: {}", database);
    } else {
        trace!("database exists");
    }

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
        .join("");
    let sql = format!("insert into {}", sql);

    if let Err(taos::Error::RawTaosError(err)) = taos.query(&sql) {
        match err.code {
            TaosCode::ClientInvalidTableName | TaosCode::MnodeInvalidTableName => {
                handle_table_schema(taos, database, &req)?;
                taos.query(&sql)?;
            }
            _ => {}
        }
    }
    Ok(())
}
#[post("/adapters/prometheus/{database}")]
async fn prometheus(
    pool: web::Data<TaosPool>,
    mgr: web::Data<DatabaseExistsMap>,
    web::Path(database): web::Path<String>,
    bytes: Bytes,
    req: HttpRequest,
) -> WebResult<HttpResponse> {
    println!("{:?}", req.headers());
    let bytes = bytes.deref();
    let mut decoder = snap::raw::Decoder::new();
    // println!("decompressing");
    let decompressed = decoder.decompress_vec(bytes).expect("decompressingc error");

    let write_request =
        protos::WriteRequest::decode(&mut decompressed.as_ref()).expect("deserialzied ok");

    let conn = pool.get().expect("couldn't get db connection from pool");

    //println!("{:?}", write_request);
    let _ =
        write_tdengine_from_prometheus_write_request(&conn, mgr.deref(), &database, write_request)
            .await;
    // body is loaded, now we can deserialize
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=trace,bailongma=trace");
    env_logger::init();
    let taos_cfg = TaosCfgBuilder::default()
        .ip("localhost")
        .user("root")
        .pass("taosdata")
        .db("log")
        .port(6030u16)
        .build()
        .expect("ToasCfg builder error");
    let taos_pool = r2d2::Pool::builder().max_size(16).build(taos_cfg)?;
    let mgr = DatabaseExistsMap::default();

    let server = HttpServer::new(move || {
        App::new()
            .data(taos_pool.clone())
            .data(mgr.clone())
            .wrap(Logger::default())
            .service(prometheus)
    })
    .bind("0.0.0.0:10230")?
    .run();

    server.await?;
    Ok(())
}
