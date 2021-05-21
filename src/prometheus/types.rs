#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetricMetadata {
    /// Represents the metric type, these match the set from Prometheus.
    /// Refer to pkg/textparse/interface.go for details.
    #[prost(enumeration="metric_metadata::MetricType", tag="1")]
    pub r#type: i32,
    #[prost(string, tag="2")]
    pub metric_family_name: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub help: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub unit: ::prost::alloc::string::String,
}
/// Nested message and enum types in `MetricMetadata`.
pub mod metric_metadata {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum MetricType {
        Unknown = 0,
        Counter = 1,
        Gauge = 2,
        Histogram = 3,
        Gaugehistogram = 4,
        Summary = 5,
        Info = 6,
        Stateset = 7,
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sample {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[prost(double, optional, tag="1")]
    pub value: Option<f64>,
    #[prost(int64, tag="2")]
    pub timestamp: i64,
}
/// TimeSeries represents samples and labels for a single time series.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag="1")]
    pub labels: ::prost::alloc::vec::Vec<Label>,
    #[prost(message, repeated, tag="2")]
    pub samples: ::prost::alloc::vec::Vec<Sample>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Label {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub value: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Labels {
    #[prost(message, repeated, tag="1")]
    pub labels: ::prost::alloc::vec::Vec<Label>,
}
/// Matcher specifies a rule, which can match or set of labels or not.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
#[serde(default)]
pub struct LabelMatcher {
    #[prost(enumeration="label_matcher::Type", tag="1")]
    pub r#type: i32,
    #[prost(string, tag="2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub value: ::prost::alloc::string::String,
}
/// Nested message and enum types in `LabelMatcher`.
pub mod label_matcher {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        Eq = 0,
        Neq = 1,
        Re = 2,
        Nre = 3,
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
#[serde(default)]
pub struct ReadHints {
    /// Query step size in milliseconds.
    #[prost(int64, optional, tag="1")]
    pub step_ms: Option<i64>,
    /// String representation of surrounding function or aggregation.
    #[prost(string, tag="2")]
    pub func: ::prost::alloc::string::String,
    /// Start time in milliseconds.
    #[prost(int64, tag="3")]
    pub start_ms: i64,
    /// End time in milliseconds.
    #[prost(int64, tag="4")]
    pub end_ms: i64,
    /// List of label names used in aggregation.
    #[prost(string, repeated, tag="5")]
    pub grouping: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Indicate whether it is without or by.
    #[prost(bool, tag="6")]
    pub by: bool,
    /// Range vector selector range in milliseconds.
    #[prost(int64, optional, tag="7")]
    pub range_ms: Option<i64>,
}
/// Chunk represents a TSDB chunk.
/// Time range [min, max] is inclusive.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Chunk {
    #[prost(int64, tag="1")]
    pub min_time_ms: i64,
    #[prost(int64, tag="2")]
    pub max_time_ms: i64,
    #[prost(enumeration="chunk::Encoding", tag="3")]
    pub r#type: i32,
    #[prost(bytes="vec", tag="4")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `Chunk`.
pub mod chunk {
    /// We require this to match chunkenc.Encoding.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Encoding {
        Unknown = 0,
        Xor = 1,
    }
}
/// ChunkedSeries represents single, encoded time series.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChunkedSeries {
    /// Labels should be sorted.
    #[prost(message, repeated, tag="1")]
    pub labels: ::prost::alloc::vec::Vec<Label>,
    /// Chunks will be in start time order and may overlap.
    #[prost(message, repeated, tag="2")]
    pub chunks: ::prost::alloc::vec::Vec<Chunk>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag="1")]
    pub timeseries: ::prost::alloc::vec::Vec<TimeSeries>,
    #[prost(message, repeated, tag="3")]
    pub metadata: ::prost::alloc::vec::Vec<MetricMetadata>,
}
/// ReadRequest represents a remote read request.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
#[serde(default)]
pub struct ReadRequest {
    #[prost(message, repeated, tag="1")]
    pub queries: ::prost::alloc::vec::Vec<Query>,
    /// accepted_response_types allows negotiating the content type of the response.
    ///
    /// Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
    /// implemented by server, error is returned.
    /// For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
    #[prost(enumeration="read_request::ResponseType", repeated, tag="2")]
    pub accepted_response_types: ::prost::alloc::vec::Vec<i32>,
}
/// Nested message and enum types in `ReadRequest`.
pub mod read_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ResponseType {
        /// Server will return a single ReadResponse message with matched series that includes list of raw samples.
        /// It's recommended to use streamed response types instead.
        ///
        /// Response headers:
        /// Content-Type: "application/x-protobuf"
        /// Content-Encoding: "snappy"
        Samples = 0,
        /// Server will stream a delimited ChunkedReadResponse message that contains XOR encoded chunks for a single series.
        /// Each message is following varint size and fixed size bigendian uint32 for CRC32 Castagnoli checksum.
        ///
        /// Response headers:
        /// Content-Type: "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
        /// Content-Encoding: ""
        StreamedXorChunks = 1,
    }
}
/// ReadResponse is a response when response_type equals SAMPLES.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadResponse {
    /// In same order as the request's queries.
    #[prost(message, repeated, tag="1")]
    pub results: ::prost::alloc::vec::Vec<QueryResult>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Query {
    #[prost(int64, tag="1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag="2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag="3")]
    pub matchers: ::prost::alloc::vec::Vec<LabelMatcher>,
    #[prost(message, optional, tag="4")]
    pub hints: ::core::option::Option<ReadHints>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryResult {
    /// Samples within a time series must be ordered by time.
    #[prost(message, repeated, tag="1")]
    pub timeseries: ::prost::alloc::vec::Vec<TimeSeries>,
}
/// ChunkedReadResponse is a response when response_type equals STREAMED_XOR_CHUNKS.
/// We strictly stream full series after series, optionally split by time. This means that a single frame can contain
/// partition of the single series, but once a new series is started to be streamed it means that no more chunks will
/// be sent for previous one. Series are returned sorted in the same way TSDB block are internally.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChunkedReadResponse {
    #[prost(message, repeated, tag="1")]
    pub chunked_series: ::prost::alloc::vec::Vec<ChunkedSeries>,
    /// query_index represents an index of the query from ReadRequest.queries these chunks relates to.
    #[prost(int64, tag="2")]
    pub query_index: i64,
}
