use crate::prometheus::types::*;

use thiserror::Error;

use libtaos::field::TaosQueryData;
use libtaos::Taos;

#[derive(Error, Debug)]
pub enum PrometheusReaderError {
    #[error("TDengine connection error")]
    TaosError(#[from] libtaos::Error),
    #[error("unknown table name in query")]
    NoneTableName,
    #[error("unknown metric name match type {r#type:?}")]
    UnknownMetricNameMatchType {
        r#type: label_matcher::Type,
        name: String,
    },
    #[error("unsupported matcher type for metrics __name__: {0}")]
    UnsupportedMatcherTypeForMetrics(String),
    // #[error(transparent)]
    // Other(#[from] anyhow::Error),
}

use PrometheusReaderError::*;

type Result<T> = std::result::Result<T, PrometheusReaderError>;

fn escape_value(value: &str) -> String {
    value.replace("'", "''")
}
fn anchor_value(value: &str) -> String {
    match (value.starts_with('^'), value.ends_with('$')) {
        (true, true) => value.replacen('^', "", 1).replacen('$', "", 1),
        (true, false) => value.replacen('^', "", 1),
        (false, true) => value.replacen('$', "", 1),
        (false, false) => value.to_string(),
    }
}
pub fn query_to_sql(query: &Query, database: &str) -> Result<(String, String)> {
    let mut table_name = String::new();
    let mut matchers = Vec::new();
    for matcher in &query.matchers {
        log::trace!("{:?}", matcher);
        let escaped_name = escape_value(&matcher.name);
        let value = escape_value(&matcher.value);
        match matcher.name.as_str() {
            "__name__" => match matcher.r#type() {
                label_matcher::Type::Eq => {
                    if value.is_empty() {
                        return Err(PrometheusReaderError::UnknownMetricNameMatchType {
                            r#type: label_matcher::Type::Eq,
                            name: value,
                        });
                    } else {
                        table_name = value.clone();
                    }
                }
                _ => {
                    return Err(PrometheusReaderError::UnsupportedMatcherTypeForMetrics(
                        value,
                    ));
                }
            },
            _ => {
                match matcher.r#type() {
                    label_matcher::Type::Eq => {
                        if value.is_empty() {
                            // From the PromQL docs: "Label matchers that match
                            // empty label values also select all time series that
                            // do not have the specific label set at all."
                            matchers.push(format!(
                                "(t_{name} = '' or t_{name} is null)",
                                name = escaped_name
                            ));
                        } else {
                            matchers.push(format!("t_{} = '{}'", escaped_name, value));
                        }
                    }
                    label_matcher::Type::Neq => {
                        matchers.push(format!("t_{} != '{}'", escaped_name, value));
                    }
                    label_matcher::Type::Re => {
                        matchers.push(format!(
                            "t_{} like '{}'",
                            escaped_name,
                            anchor_value(&value)
                        ));
                    }
                    _ => {
                        return Err(PrometheusReaderError::UnsupportedMatcherTypeForMetrics(
                            value,
                        ));
                    }
                }
            }
        }
    }
    if table_name.is_empty() {
        return Err(NoneTableName);
    }
    log::debug!(
        "start time: {}, end time: {}",
        query.start_timestamp_ms,
        query.end_timestamp_ms
    );
    matchers.push(format!("ts >= {}", query.start_timestamp_ms));
    matchers.push(format!("ts <= {}", query.end_timestamp_ms));
    let sql = format!(
        "SELECT * FROM {}.{} WHERE {} ORDER BY ts",
        database,
        table_name,
        matchers.join(" AND ")
    );
    Ok((table_name, sql))
}

#[test]
fn test_query_to_sql() {
    let data = r#"
         {
          "start_timestamp_ms": 1621511013040,
          "end_timestamp_ms": 1621511073040,
          "matchers": [
           {
            "name": "mode",
            "value": "system"
           },
           {
            "name": "__name__",
            "value": "node_cpu_seconds_total"
           },
           {
            "name": "monitor",
            "value": "example"
           }
          ],
          "hints": {
           "func": "rate",
           "start_ms": 1621511013040,
           "end_ms": 1621511073040
          }
         }"#;
    let query: Query = serde_json::from_str(data).unwrap();
    let (table_name, sql) = query_to_sql(&query, "prometheus").unwrap();
    assert_eq!(table_name, "node_cpu_seconds_total");
    println!("{}", sql);
    assert_eq!(sql, "SELECT * FROM prometheus.node_cpu_seconds_total WHERE t_mode = 'system' AND t_monitor = 'example' AND ts >= 1621511013040 AND ts <= 1621511073040 ORDER BY ts")
}

pub async fn read(taos: &Taos, database: &str, req: &ReadRequest) -> Result<ReadResponse> {
    let ReadRequest { queries, .. } = &req;
    let mut results = Vec::new();
    for query in queries {
        let (table_name, sql) = query_to_sql(query, database)?;
        log::trace!("sql: {}", sql);

        let TaosQueryData { column_meta, rows } = taos.query(&sql).await?;

        // let mut cache = Vec::new();

        let mut timeseries = Vec::new();
        let mut labels = Vec::new();
        let mut samples = Vec::new();
        let mut label_inited = false;
        labels.push(Label {
            name: "__name__".to_string(),
            value: table_name,
        });

        for row in rows {
            log::trace!("{:?}", row);
            let mut sample = Sample::default();
            for (field, meta) in row.into_iter().zip(&column_meta) {
                match meta.name.as_str() {
                    "ts" => {
                        sample.timestamp = field.as_raw_timestamp().expect("should be timestamp");
                    }
                    "value" => {
                        sample.value = field.as_double().map(|v| *v);
                    }
                    "taghash" => {}
                    name => {
                        if !label_inited {
                            let label = Label {
                                name: name.replacen("t_", "", 1),
                                value: field.as_string().unwrap().to_string(),
                            };
                            labels.push(label);
                        }
                    }
                }
            }
            label_inited = true;
            samples.push(sample);
        }
        let ts = TimeSeries { labels, samples };
        timeseries.push(ts);
        results.push(QueryResult { timeseries });
    }

    Ok(ReadResponse { results })
}

#[tokio::test]
async fn test_read_request() {
    let taos = crate::test::taos().unwrap();
    let data = r#"{
        "queries": [
         {
          "start_timestamp_ms": 1621511013040,
          "end_timestamp_ms": 1621511073040,
          "matchers": [
           {
            "name": "mode",
            "value": "system"
           },
           {
            "name": "__name__",
            "value": "node_cpu_seconds_total"
           },
           {
            "name": "monitor",
            "value": "example"
           }
          ],
          "hints": {
           "func": "rate",
           "start_ms": 1621511013040,
           "end_ms": 1621511073040
          }
         }
        ]
       }"#;

    let req: ReadRequest = serde_json::from_str(data).unwrap();
    let res = read(&taos, "prometheus", &req).await.unwrap();
    println!("{:?}", res);
    let file = std::fs::File::open("tests/read-response.json").unwrap();
    let expect: ReadResponse = serde_json::from_reader(file).unwrap();
    assert_eq!(res, expect);
}
