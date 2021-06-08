use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use actix_web::{
    middleware::Logger, post, web::Bytes, App, HttpResponse, HttpServer, Result as WebResult,
};
use anyhow::Result;
use clap::Clap;

use std::sync::atomic::AtomicI32;

static PROM_NUMBER: AtomicI32 = AtomicI32::new(0);

#[post("/adapters/prometheus/write2")]
async fn prometheus_write(bytes: Bytes) -> WebResult<HttpResponse> {
    let bytes = bytes.deref();
    let num = PROM_NUMBER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    std::fs::write(format!("prom-write-{:04}.snappy", num), bytes)?;
    // body is loaded, now we can deserialize
    Ok(HttpResponse::Ok().finish())
}
#[post("/adapters/prometheus/read")]
async fn prometheus_read(bytes: Bytes) -> WebResult<HttpResponse> {
    let bytes = bytes.deref();
    let num = PROM_NUMBER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    std::fs::write(format!("prom-read-{:04}.snappy", num), bytes)?;
    // body is loaded, now we can deserialize
    Ok(HttpResponse::Ok().finish())
}

/// TDengine adapter for prometheus.
#[derive(Debug, Clone, Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
#[clap(version, author)]
struct Opts {
    /// Debug level
    #[clap(short, long, default_value = "info")]
    level: log::Level,
    /// Listen to an specific ip and port
    #[clap(short = 'L', long, default_value = "0.0.0.0:10203")]
    listen: String,
    /// file number start at
    #[clap(short, long, default_value = "0")]
    file_number_start: u32,
    /// Stop at seconds timeout
    #[clap(short, long, default_value = "5")]
    stop_at: u64,
}

#[actix_web::main]
async fn main() -> Result<()> {
    let opts: Opts = Opts::parse();

    std::env::set_var(
        "RUST_LOG",
        format!(
            "actix_web=info,bailongma={level},{level}",
            level = opts.level.to_string()
        ),
    );
    env_logger::init();
    let listen = opts.listen.clone();
    let num = Arc::new(Mutex::new(0));
    let server = HttpServer::new(move || {
        App::new()
            .data(num.clone())
            .wrap(Logger::default())
            .service(prometheus_write)
            .service(prometheus_read)
    })
    .bind(&listen)?
    .shutdown_timeout(opts.stop_at)
    .run();

    server.await?;
    Ok(())
}
