use std::collections::BTreeMap;

use crate::prometheus::types::*;

use thiserror::Error;

use libtaos::field::{ColumnMeta, Field, TaosQueryData};
use libtaos::Taos;

use regex::Regex;

#[derive(Error, Debug)]
pub enum PrometheusReaderError {
    #[error("TDengine connection error")]
    TaosError(#[from] libtaos::Error),
    #[error("Regex pattern error: {0}")]
    RegexError(#[from] regex::Error),
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
    value.escape_default().to_string()
}

pub enum LabelFilter {
    Re(Regex),
    Nre(Regex),
}
pub type LabelFilters = BTreeMap<String, LabelFilter>;
pub fn query_to_sql(query: &Query, database: &str) -> Result<(String, String, LabelFilters)> {
    let mut table_name = String::new();
    let mut matchers = Vec::new();
    let mut filters = LabelFilters::new();
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
            name => {
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
                            matchers.push(format!("t_{} = \"{}\"", escaped_name, value));
                        }
                    }
                    label_matcher::Type::Neq => {
                        matchers.push(format!("t_{} != \"{}\"", escaped_name, value));
                    }
                    label_matcher::Type::Re => {
                        filters.insert(name.to_string(), LabelFilter::Re(Regex::new(&value)?));
                    }
                    label_matcher::Type::Nre => {
                        filters.insert(name.to_string(), LabelFilter::Nre(Regex::new(&value)?));
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
    Ok((table_name, sql, filters))
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
    let (table_name, sql, _filters) = query_to_sql(&query, "prometheus").unwrap();
    assert_eq!(table_name, "node_cpu_seconds_total");
    println!("{}", sql);
    assert_eq!(sql, "SELECT * FROM prometheus.node_cpu_seconds_total WHERE t_mode = \"system\" AND t_monitor = \"example\" AND ts >= 1621511013040 AND ts <= 1621511073040 ORDER BY ts")
}

pub async fn read(taos: &Taos, database: &str, req: &ReadRequest) -> Result<ReadResponse> {
    let ReadRequest { queries, .. } = &req;
    // let mut results = Vec::new();
    type Map = linked_hash_map::LinkedHashMap<Vec<Label>, Vec<Sample>>;
    let mut results = Vec::new();
    for query in queries {
        let mut results_map = Map::default();
        let (table_name, sql, filters) = query_to_sql(query, database)?;
        log::debug!("sql: {}", sql);

        let TaosQueryData { column_meta, rows } = taos.query(&sql).await?;

        // call regex filters
        for row in rows {
            //log::trace!("{:?}", row);
            if filters.len() > 0 {
                if !row.iter().zip(&column_meta).all(|(field, meta)| {
                    if let Some(filter) = filters.get(&meta.name.as_str().replacen("t_", "", 1)) {
                        match filter {
                            LabelFilter::Re(pattern) => {
                                field.as_string().map_or(false, |v| pattern.is_match(&v))
                            }
                            LabelFilter::Nre(pattern) => {
                                field.as_string().map_or(false, |v| !pattern.is_match(&v))
                            }
                        }
                    } else {
                        true
                    }
                }) {
                    continue;
                };
            }

            let mut labels = Vec::new();
            let mut sample = Sample::default();
            labels.push(Label {
                name: "__name__".to_string(),
                value: table_name.to_string(),
            });
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
                        if let Some(value) = field.as_string() {
                            let label = Label {
                                name: name.replacen("t_", "", 1),
                                value: value.to_string(),
                            };
                            labels.push(label);
                        }
                    }
                }
            }

            if let Some(entry) = results_map.get_mut(&labels) {
                entry.push(sample);
            } else {
                results_map.insert(labels, vec![sample]);
            }
        }
        let timeseries = results_map
            .into_iter()
            .map(|(labels, samples)| TimeSeries { labels, samples })
            .collect();
        results.push(QueryResult { timeseries });
    }

    for result in results.iter() {
        log::debug!("total series: {}", result.timeseries.len());
        for ts in &result.timeseries {
            use itertools::Itertools;
            let labels = ts.labels.iter().map(|label| format!("{}={}",label.name, label.value)).join(",");
            log::debug!("labels: {}", labels);
        }
    }

    Ok(ReadResponse { results })
}

#[tokio::test]
async fn test_read_request() {
    let taos = crate::test::taos().unwrap();
    taos.exec("drop database if exists prom_read_0xabc")
        .await
        .unwrap();
    taos.exec("create database prom_read_0xabc").await.unwrap();
    taos.exec("use prom_read_0xabc").await.unwrap();
    taos.exec("create table tb1 (ts timestamp, str1 binary(10), str2 nchar(10))")
        .await
        .unwrap();
    taos.exec("insert into tb1 values(1621511073000, 'taosdata', NULL) (1621511073100, 'taos', '涛思数据')(1621511073200, 'a taos', '涛思数据')(1621511073200, 'nothing', 'abc')").await.unwrap();

    // Case 1, regex match `taos`.
    let data = r#"{
        "queries": [
         {
          "start_timestamp_ms": 1621511073000,
          "end_timestamp_ms": 1621511073400,
          "matchers": [
            {
             "name": "__name__",
             "value": "tb1"
            },
           {
            "name": "str1",
            "type": 2,
            "value": "^taos.*"
           }
          ],
          "hints": {
           "func": "rate",
           "start_ms": 1621511073000,
           "end_ms": 1621511073400
          }
         }
        ]
       }"#;

    let req: ReadRequest = serde_json::from_str(data).unwrap();
    let res = read(&taos, "prom_read_0xabc", &req).await.unwrap();
    println!("{:?}", res);
    assert_eq!(res.results[0].timeseries.len(), 2);

    // Case 2, regex match `^taos`.
    let data = r#"{
        "queries": [
         {
          "start_timestamp_ms": 1621511073000,
          "end_timestamp_ms": 1621511073400,
          "matchers": [
            {
             "name": "__name__",
             "value": "tb1"
            },
           {
            "name": "str1",
            "type": 2,
            "value": "taos.*"
           }
          ],
          "hints": {
           "func": "rate",
           "start_ms": 1621511073000,
           "end_ms": 1621511073400
          }
         }
        ]
       }"#;

    let req: ReadRequest = serde_json::from_str(data).unwrap();
    let res = read(&taos, "prom_read_0xabc", &req).await.unwrap();
    println!("{:?}", res);
    assert_eq!(res.results[0].timeseries.len(), 3);

    // Case 3, regex not match `^taos`.
    let data = r#"{
        "queries": [
         {
          "start_timestamp_ms": 1621511073000,
          "end_timestamp_ms": 1621511073400,
          "matchers": [
            {
             "name": "__name__",
             "value": "tb1"
            },
           {
            "name": "str1",
            "type": 3,
            "value": "^taos.*"
           }
          ],
          "hints": {
           "func": "rate",
           "start_ms": 1621511073000,
           "end_ms": 1621511073400
          }
         }
        ]
       }"#;

    let req: ReadRequest = serde_json::from_str(data).unwrap();
    let res = read(&taos, "prom_read_0xabc", &req).await.unwrap();
    println!("{:?}", res);
    assert_eq!(res.results[0].timeseries.len(), 1);

    // Case 4, regex match unicode `涛思数据`
    let data = r#"{
        "queries": [
         {
          "start_timestamp_ms": 1621511073000,
          "end_timestamp_ms": 1621511073400,
          "matchers": [
            {
             "name": "__name__",
             "value": "tb1"
            },
           {
            "name": "str2",
            "type": 2,
            "value": "涛思数据"
           }
          ],
          "hints": {
           "func": "rate",
           "start_ms": 1621511073000,
           "end_ms": 1621511073400
          }
         }
        ]
       }"#;

    let req: ReadRequest = serde_json::from_str(data).unwrap();
    let res = read(&taos, "prom_read_0xabc", &req).await.unwrap();
    println!("{:?}", res);
    assert_eq!(res.results[0].timeseries.len(), 2);
    taos.exec("drop database prom_read_0xabc").await.unwrap();
}
