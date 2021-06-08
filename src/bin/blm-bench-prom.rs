use std::{sync::Arc, vec};

use bailongma::*;

use clap::Clap;
use itertools::Itertools;
use log::{error, trace};
use names::{Generator, Name};
use rayon::prelude::*;


/// TDengine adapter for prometheus.
#[derive(Debug, Clone, Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
#[clap(version, author)]
struct Opts {
    /// Bailongma prometheus endpoint.
    endpoint: String,
    /// Debug level
    #[clap(long, default_value = "info")]
    level: log::LevelFilter,
    /// Metrics number, stable size in tdegine
    #[clap(long, default_value = "1000")]
    metrics: usize,
    /// Labels number, tdengine tags
    #[clap(long, default_value = "4")]
    labels: usize,
    /// Data points, so the table size = metrics * points.
    #[clap(long, default_value = "1000")]
    points: usize,
    /// Write request batch size of timeseries data
    #[clap(long, default_value = "500")]
    chunks: usize,
    /// Time interval for each prom write request, unit: ms
    #[clap(long, default_value = "1000")]
    interval: u32,
    /// Threads
    #[clap(long, default_value = "8")]
    threads: usize,
    /// Samples per point, so the stable rows is points * samples
    #[clap(long, default_value = "100")]
    samples: u32,
}

fn random_names(len: usize) -> Vec<String> {
    let generator = Generator::with_naming(Name::Numbered);
    generator.take(len).collect()
}

impl Opts {
    fn random_metrics_names(&self) -> Arc<Vec<String>> {
        Arc::new(random_names(self.metrics))
    }
    fn random_labels(&self) -> Arc<Vec<String>> {
        Arc::new(random_names(self.labels))
    }
    fn random_points(&self) -> Arc<Vec<String>> {
        Arc::new(random_names(self.points))
    }
}

#[derive(Clone, Debug)]
struct PromGenerator {
    endpoint: String,
    timestamp: i64,
    chunks: usize,
    interval: u32,
    metrics: Arc<Vec<String>>,
    labels: Arc<Vec<String>>,
    points: Arc<Vec<String>>,
}

impl PromGenerator {
    fn build_timeseries(&self, _rt: &tokio::runtime::Runtime, ts_offset: i64) {
        let interval = self.interval;
        let sample = Sample {
            value: Some(ts_offset as _),
            timestamp: self.timestamp + ts_offset,
        };
        let samples = vec![sample];
        let extra_labels = self
            .labels
            .iter()
            .map(|name| Label {
                name: name.to_string(),
                value: name.to_string(),
            })
            .collect_vec();
        self.points
            .iter()
            .map(|point| {
                self.metrics
                    .iter()
                    .map(move |name| (name, point))
                    .map(|(name, point)| {
                        let name_label = Label {
                            name: "__name__".to_string(),
                            value: name.to_string(),
                        };
                        let point_label = Label {
                            name: "point".to_string(),
                            value: point.to_string(),
                        };
                        let mut labels = vec![name_label, point_label];
                        labels.extend_from_slice(&extra_labels.clone());
                        TimeSeries {
                            labels,
                            samples: samples.clone(),
                        }
                    })
            })
            .flatten()
            .chunks(self.chunks)
            .into_iter()
            .map(|chunk| WriteRequest {
                timeseries: chunk.collect_vec(),
                metadata: vec![],
            })
            .map(|req| {
                use prost::Message;
                let mut bytes = Vec::new();
                let _ = req.encode(&mut bytes);
                bytes
            })
            .map(|bytes| {
                snap::raw::Encoder::new()
                    .compress_vec(&bytes)
                    .expect("snappy compress error")
            })
            .collect_vec()
            .into_par_iter()
            .for_each(|data| {
                use tempfile::NamedTempFile;
                std::fs::write("test.prom", &data).expect("write to file");
                let url = self.endpoint.clone();
                trace!("datalen: {}", data.len());
                //rt.spawn_blocking(move || {
                let mut file = NamedTempFile::new_in("/dev/shm").unwrap();
                use std::io::prelude::*;
                file.as_file_mut()
                    .write_all(&data)
                    .expect("create file in /dev/shm");
                let path = file.path();
                let status = std::process::Command::new("curl")
                    .args(&["-X", "POST"])
                    .arg("--data-binary")
                    .arg(&format!("@{}", path.display()))
                    .arg(url)
                    .status()
                    .expect("run command error");
                if !status.success() {
                    error!("post data error");
                }
                std::thread::sleep(std::time::Duration::from_millis(interval as _));
                // });
            });
    }
}
//#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
fn main() {
    let opts: Opts = Opts::parse();
    let metrics = opts.random_metrics_names();
    let labels = opts.random_labels();
    let points = opts.random_points();

    use tokio::runtime;

    // This will spawn a work-stealing runtime with 4 worker threads.
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(opts.threads)
        .thread_name("bench-prom")
        .build()
        .unwrap();
    rayon::ThreadPoolBuilder::new()
        .num_threads(opts.threads)
        .build_global()
        .unwrap();
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let prom_g = PromGenerator {
        endpoint: opts.endpoint.clone(),
        timestamp,
        chunks: opts.chunks,
        interval: opts.interval,
        metrics,
        labels,
        points,
    };

    for i in 0..opts.samples {
        prom_g.build_timeseries(&rt, i as _);
    }
}
