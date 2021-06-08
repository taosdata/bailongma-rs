use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use clap::Clap;
use prost::Message;

use bailongma::*;

#[derive(Debug, Clone)]
enum PromType {
    ReadRequest,
    WriteRequest,
}
impl FromStr for PromType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "read" | "read-request" | "readrequest" | "readreq" => Ok(PromType::ReadRequest),
            "write" | "write-request" | "writerequest" | "writereq" => Ok(PromType::WriteRequest),
            _ => Err("no match"),
        }
    }
}

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
    /// Read or Write
    #[clap(short = 't', long, default_value = "read")]
    prom_type: PromType,
}
fn write_json<T: ?Sized>(opts: &Opts, data: &T) -> Result<()> 
where
    T: serde::Serialize, 
{
    match &opts.json {
        None => {
            let output = std::io::stdout();
            if opts.pretty {
                serde_json::to_writer_pretty(output, &data)?;
            } else {
                serde_json::to_writer(output, &data)?;
            }
        }
        Some(path) => {
            let output = std::fs::File::create(path)?;

            if opts.pretty {
                serde_json::to_writer_pretty(output, &data)?;
            } else {
                serde_json::to_writer(output, &data)?;
            }
        }
    }
    Ok(())

}

fn main() -> Result<()> {
    let opts = Opts::parse();
    env_logger::init();
    let bytes = std::fs::read(&opts.path)?;
    let mut decoder = snap::raw::Decoder::new();
    let decompressed = decoder
        .decompress_vec(&bytes)
        .expect("decompressingc error");
    log::debug!(
        "decompressed {} bytes to {}",
        bytes.len(),
        decompressed.len()
    );

    match opts.prom_type {
        PromType::ReadRequest => {
            let request = ReadRequest::decode(&mut decompressed.as_ref()).expect("deserialzied ok");
            write_json(&opts, &request)?;
        }
        PromType::WriteRequest => {
            let request = WriteRequest::decode(&mut decompressed.as_ref()).expect("deserialzied ok");
            write_json(&opts, &request)?;
        }
    }

    Ok(())
}
