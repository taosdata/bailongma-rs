use std::collections::BTreeMap;

use crate::prometheus::types::*;
use crate::utils::tag_value_escape;

use thiserror::Error;

use libtaos::field::TaosQueryData;
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

pub enum LabelFilter {
    Re(Regex),
    Nre(Regex),
}

#[derive(Debug)]
pub enum MetricFilter {
    Eq(String),
    Neq(String),
    Re(Regex),
    Nre(Regex),
}

impl ToString for MetricFilter {
    fn to_string(&self) -> String {
        use MetricFilter::*;
        match self {
            Eq(s) => s.to_string(),
            Neq(s) => format!("!{}", s),
            Re(r) => format!("{}", r),
            Nre(r) => format!("!{}", r),
        }
    }
}
pub type LabelFilters = BTreeMap<String, LabelFilter>;

/// Return a tuple:
/// 1. metric filter
/// 2. condition sql string
/// 3. regex label filters
pub fn query_to_sql(query: &Query) -> Result<(MetricFilter, String, LabelFilters)> {
    let mut metric_filter = None;
    let mut matchers = Vec::new();
    let mut filters = LabelFilters::new();
    for matcher in &query.matchers {
        log::trace!("{:?}", matcher);
        let escaped_name = tag_value_escape(&matcher.name);
        let value = tag_value_escape(&matcher.value);
        match matcher.name.as_str() {
            "__name__" => {
                if value.is_empty() {
                    return Err(PrometheusReaderError::UnknownMetricNameMatchType {
                        r#type: matcher.r#type(),
                        name: value,
                    });
                }
                match matcher.r#type() {
                    label_matcher::Type::Eq => {
                        metric_filter = Some(MetricFilter::Eq(value.clone()));
                    }
                    label_matcher::Type::Neq => {
                        metric_filter = Some(MetricFilter::Neq(value.clone()));
                    }
                    label_matcher::Type::Re => {
                        metric_filter = Some(MetricFilter::Re(Regex::new(&value)?));
                    }
                    label_matcher::Type::Nre => {
                        metric_filter = Some(MetricFilter::Nre(Regex::new(&value)?));
                    }
                }
            }
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
    if metric_filter.is_none() {
        return Err(NoneTableName);
    }
    let metric_filter = metric_filter.unwrap();
    log::debug!(
        "start time: {}, end time: {}, metric fiter: {:?}",
        query.start_timestamp_ms,
        query.end_timestamp_ms,
        metric_filter
    );
    matchers.push(format!("ts >= {}", query.start_timestamp_ms));
    matchers.push(format!("ts <= {}", query.end_timestamp_ms));
    let sql = format!("WHERE {} ORDER BY ts", matchers.join(" AND "));
    Ok((metric_filter, sql, filters))
}

async fn metric_filter_to_tables(
    taos: &Taos,
    database: &str,
    filter: &MetricFilter,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    use itertools::Itertools;
    use MetricFilter::*;

    if let Eq(name) = filter {
        names.push(name.to_string());
        return Ok(names);
    }
    taos.use_database(database).await?;
    let TaosQueryData { rows, .. } = taos.query("show stables").await?;
    let metrics = rows
        .into_iter()
        .filter_map(|a| a.into_iter().next())
        .map(|field| format!("{}", field))
        .collect_vec();
    match filter {
        Neq(name) => {
            names = metrics
                .into_iter()
                .filter(|metric| metric.as_str() != name)
                .collect();
        }
        Re(pattern) => {
            names = metrics
                .into_iter()
                .filter(|metric| pattern.is_match(&metric))
                .collect();
        }
        Nre(pattern) => {
            names = metrics
                .into_iter()
                .filter(|metric| !pattern.is_match(&metric))
                .collect();
        }
        Eq(_) => unreachable!(),
    }
    Ok(names)
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
    let (metric_filter, sql, _filters) = query_to_sql(&query).unwrap();
    assert_eq!(metric_filter.to_string(), "node_cpu_seconds_total");
    println!("{}", sql);
    assert_eq!(sql, "WHERE t_mode = \"system\" AND t_monitor = \"example\" AND ts >= 1621511013040 AND ts <= 1621511073040 ORDER BY ts")
}

pub async fn read(taos: &Taos, database: &str, req: &ReadRequest) -> Result<ReadResponse> {
    let ReadRequest { queries, .. } = &req;
    // let mut results = Vec::new();
    type Map = linked_hash_map::LinkedHashMap<Vec<Label>, Vec<Sample>>;
    let mut results = Vec::new();
    for query in queries {
        let (metric_filter, cond, filters) = query_to_sql(query)?;
        log::debug!("condition: {}", cond);
        let mut timeseries = Vec::new();

        for table_name in metric_filter_to_tables(taos, database, &metric_filter).await? {
            let mut results_map = Map::default();
            let sql = format!("select * from {}.{} {}", database, table_name, cond);
            log::debug!("sql: {}", sql);
            let TaosQueryData { column_meta, rows } = taos.query(&sql).await?;

            // call regex filters
            for row in rows {
                //log::trace!("{:?}", row);
                if !filters.is_empty()
                    && !row.iter().zip(&column_meta).all(|(field, meta)| {
                        if let Some(filter) = filters.get(&meta.name.as_str().replacen("t_", "", 1))
                        {
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
                    })
                {
                    continue;
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
                            sample.timestamp =
                                field.as_raw_timestamp().expect("should be timestamp");
                        }
                        "value" => {
                            sample.value = field.as_double().copied();
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
            timeseries.extend(results_map
                .into_iter()
                .map(|(labels, samples)| TimeSeries { labels, samples }));

        }
        results.push(QueryResult { timeseries });
    }

    for result in results.iter() {
        log::debug!("total series: {}", result.timeseries.len());
        for ts in &result.timeseries {
            use itertools::Itertools;
            let labels = ts
                .labels
                .iter()
                .map(|label| format!("{}={}", label.name, label.value))
                .join(",");
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
    taos.exec(
        "create stable stb1 (ts timestamp, value double) tags(str1 binary(10), str2 nchar(10))",
    )
    .await
    .unwrap();
    taos.exec("insert into tb1 using stb1 tags('taosdata', NULL) values(1621511073000, 1.0)")
        .await
        .unwrap();
    taos.exec(
        "insert into tb2 using stb1 tags('taosdata',  '涛思数据') values(1621511073000, 2.0)",
    )
    .await
    .unwrap();
    taos.exec("insert into tb3 using stb1 tags('abc',  '涛思数据') values(1621511073000, 3.0)")
        .await
        .unwrap();

    taos.exec(
        "create stable stb2 (ts timestamp, value double) tags(str1 binary(10), str2 nchar(10))",
    )
    .await
    .unwrap();
    taos.exec("insert into t2b1 using stb2 tags('taosdata', NULL) values(1621511073000, 1.0)")
        .await
        .unwrap();
    taos.exec(
        "insert into t2b2 using stb2 tags('taosdata',  '涛思数据') values(1621511073000, 2.0)",
    )
    .await
    .unwrap();

    // Case 1, regex match `taos`.
    let data = r#"{
        "queries": [
         {
          "start_timestamp_ms": 1621511073000,
          "end_timestamp_ms": 1621511073400,
          "matchers": [
            {
             "name": "__name__",
             "value": "stb1"
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
             "value": "stb1"
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
    assert_eq!(res.results[0].timeseries.len(), 2);

    // Case 3, regex not match `^taos`.
    let data = r#"{
        "queries": [
         {
          "start_timestamp_ms": 1621511073000,
          "end_timestamp_ms": 1621511073400,
          "matchers": [
            {
             "name": "__name__",
             "value": "stb1"
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
    assert_eq!(res.results[0].timeseries.len(), 1);

    // Case 5, regex match __name__
    let data = r#"{
        "queries": [
         {
          "start_timestamp_ms": 1621511073000,
          "end_timestamp_ms": 1621511073400,
          "matchers": [
            {
             "name": "__name__",
             "type": 2,
             "value": "tb"
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
    assert_eq!(res.results[0].timeseries.len(), 5);
    taos.exec("drop database prom_read_0xabc").await.unwrap();
}
