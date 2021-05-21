use std::ops::Deref;

use actix::prelude::*;
use log::{debug, error, info, trace, warn};
use thiserror::Error;

mod protos;
mod utils;
mod prometheus;

#[cfg(feature = "protoc")]
pub use protos::*;

pub use prometheus::*;

#[cfg(test)]
mod test;

#[derive(Debug, Error)]
pub enum PrometheusRemoteWriteError {
    #[error("Reach max memory limit {0}/{1}")]
    MemoryLimit(u64, u64),
    #[error("Get process info error")]
    ProcessError,
    #[error("{0}")]
    Custom(&'static str),
}

pub struct SuperTableActor;

impl Actor for SuperTableActor {
    type Context = SyncContext<Self>;
}

// #[derive(Message)]
// #[rtype(result = "()")]
// struct (usize, usize);

#[derive(Debug)]
pub struct PrometheusRemoteWriteActor {
    sys: sysinfo::System,
    max_memory: u64,
}

impl PrometheusRemoteWriteActor {
    pub fn max_memory(mut self, mem: u64) -> Self {
        self.max_memory = mem;
        self
    }
}

impl Default for PrometheusRemoteWriteActor {
    fn default() -> Self {
        use sysinfo::{System, SystemExt};
        PrometheusRemoteWriteActor {
            sys: System::new_all(),
            ..Default::default()
        }
    }
}

impl Actor for PrometheusRemoteWriteActor {
    type Context = SyncContext<Self>;
}

#[derive(Message, Debug)]
#[rtype(result = "Result<(), PrometheusRemoteWriteError>")]
pub struct PrometheusRemoteWriteMessage {
    database: String,
    bytes: Vec<u8>,
}

impl PrometheusRemoteWriteMessage {
    pub fn new(database: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            database: database.into(),
            bytes: bytes.into(),
        }
    }
}

impl Deref for PrometheusRemoteWriteMessage {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl Handler<PrometheusRemoteWriteMessage> for PrometheusRemoteWriteActor {
    type Result = Result<(), PrometheusRemoteWriteError>;

    fn handle(
        &mut self,
        bytes: PrometheusRemoteWriteMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug!("recieved {} bytes from prometheus", bytes.len());

        use sysinfo::{ProcessExt, SystemExt};
        let used = self.sys.get_used_memory();
        let total = self.sys.get_total_memory();
        let ps = self
            .sys
            .get_process(std::process::id() as _)
            .ok_or_else(|| PrometheusRemoteWriteError::ProcessError)?;
        let ps_mem = ps.memory();
        info!(
            "MEMORY: {} in process, {}/{} used/total({:.2}%)",
            ps_mem,
            used,
            total,
            used as f32 / total as f32 * 100.
        );
        if ps_mem > self.max_memory {
            warn!("reached max memory limit: {}/{}", ps_mem, self.max_memory);
            return Err(PrometheusRemoteWriteError::MemoryLimit(
                ps_mem,
                self.max_memory,
            ));
        }
        Ok(())
    }
}
