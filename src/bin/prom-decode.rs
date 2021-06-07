use std::path::PathBuf;

use anyhow::Result;
use clap::Clap;
use prost::Message;

use bailongma::*;

/// TDengine adapter for prometheus.
#[derive(Debug, Clone, Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
#[clap(version, author)]
struct Opts {
    /// Prometheus raw request data file
    path: PathBuf,
    /// Output json
    json: Option<PathBuf>,
    /// Pretty print
    #[clap(short, long)]
    pretty: bool,
}

fn main() -> Result<()> {
    let opts = Opts::parse();
    env_logger::init();
    let bytes = std::fs::read(opts.path)?;
    let mut decoder = snap::raw::Decoder::new();
    let decompressed = decoder
        .decompress_vec(&bytes)
        .expect("decompressingc error");
    log::debug!(
        "decompressed {} bytes to {}",
        bytes.len(),
        decompressed.len()
    );

    let write_request = WriteRequest::decode(&mut decompressed.as_ref()).expect("deserialzied ok");

    match opts.json {
        None => {
            let output = std::io::stdout();
            if opts.pretty {
                serde_json::to_writer_pretty(output, &write_request)?;
            } else {
                serde_json::to_writer(output, &write_request)?;
            }
        }
        Some(path) => {
            let output = std::fs::File::create(path)?;

            if opts.pretty {
                serde_json::to_writer_pretty(output, &write_request)?;
            } else {
                serde_json::to_writer(output, &write_request)?;
            }
        }
    }

    Ok(())
}
