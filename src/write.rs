async fn prom_write(state: &AppState, database: &str, req: &WriteRequest) -> Result<()> {
    debug!("Write tdengine from prometheus write request");
    let taos = state.pool.get()?;
    let taos = taos.deref();

    let db_handler = state.tables.assert_database(database);
    let stables = db_handler.value();

    for timeseries in &req.timeseries {
        // handle stable
        debug!("handle stable start");
        let (name, labels): (_, Vec<_>) = timeseries
            .labels
            .iter()
            .partition(|label| label.name == "__name__");

        // label __name__ should exist.
        assert!(name.len() == 1);

        // get metrics name
        let metrics_name = &name[0].value;
        let stable_name = table_name_escape(metrics_name);

        if stables.get(&stable_name).is_none() {
            // check table exists.
            let mut schema = taos
                .describe(&format!("{}.{}", database, &stable_name))
                .await;
            trace!("super table schema: {:?}", &schema);
        }

        let mut schema = taos
            .describe(&format!("{}.{}", database, &stable_name))
            .await;
        trace!("schema: {:?}", &schema);
        let _ = state.create_table_lock.lock().unwrap();
        if let Err(taos::Error::RawTaosError(taos::TaosError { code, ref err })) = schema {
            let _ = state.create_table_lock.lock().unwrap();
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
                        .map(|label| { format!("t_{} binary({})", tag_name_escape(&label.name), 128) })  // TODO: default binary length is 128 
                        .join(", ")
                );
                    trace!("exec sql: {}", &sql);
                    taos.exec(&sql).await?;
                    schema = taos
                        .describe(&format!("{}.{}", database, &stable_name))
                        .await;
                }
                taos::TaosCode::MnodeDbNotSelected => {
                    // create database
                    let sql = format!("create database {}", database);
                    dbg!(taos.exec(&sql).await?);
                    // create super table
                    let sql = format!(
                    "create stable if not exists {}.{} (ts timestamp, value double) tags (taghash binary({}), {})",
                    database,
                    stable_name,
                    34, // taghash length
                    labels
                        .iter()
                        .map(|label| { format!("t_{} binary({})", tag_name_escape(&label.name), 128) })  // TODO: default binary length is 128 
                        .join(", ")
                );
                    trace!("exec sql: {}", &sql);
                    taos.exec(&sql).await?;
                    schema = taos
                        .describe(&format!("{}.{}", database, &stable_name))
                        .await;
                }
                _ => {
                    error!("error: TaosError {{ code: {}, err: {} }}", code, err);
                    Err(taos::Error::RawTaosError(TaosError {
                        code,
                        err: err.clone(),
                    }))?;
                }
            }
        }

        let schema = schema.unwrap();
        use std::iter::FromIterator;
        let fields = BTreeSet::from_iter(schema.names().into_iter());

        let mut tagmap = BTreeMap::new();
        for label in &labels {
            if !fields.contains(&format!("t_{}", tag_name_escape(&label.name))) {
                let sql = format!(
                    "alter stable {}.{} add tag t_{} binary({})",
                    database,
                    stable_name,
                    tag_name_escape(&label.name),
                    128
                );
                trace!("add tag {} for stable {}: {}", label.name, stable_name, sql);
                taos.exec(&sql).await?;
            }
            tagmap.insert(&label.name, &label.value);
        }

        let tag_values = labels.iter().map(|label| &label.value).join("");
        let table_name = format!("{}{}", metrics_name, tag_values);
        let table_name = format!("md5_{}", md5sum(table_name.as_bytes()));
        let taghash = md5sum(tagmap.values().join("").as_bytes());

        // create sub table;
        // FIXME: It's better to keep a table exist set.
        let sql = format!(
            "create table if not exists {}.{} using {}.{} (taghash,{}) tags(\"{}\",{})",
            database,
            table_name,
            database,
            stable_name,
            tagmap
                .keys()
                .map(|v| format!("t_{}", tag_name_escape(&v)))
                .join(","),
            taghash,
            tagmap
                .values()
                .map(|value| format!("\"{}\"", value))
                .join(",")
        );
        debug!("created table {}.{}", database, table_name);
        trace!("create table with sql: {}", sql);
        taos.exec(&sql).await?;
        debug!("handle stable done");
    }
    // build insert sql
    let chunks = req
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
            ts.samples.iter().map(move |sample| match sample.value {
                Some(value) => format!(" {} values ({}, {})", table_name, sample.timestamp, value),
                None => format!(" {} values ({}, NULL)", table_name, sample.timestamp),
            })
        })
        .flatten()
        .chunks(state.opts.chunk_size)
        .into_iter()
        .map(|mut chunk| chunk.join(""))
        .collect_vec();

    for chunk in chunks {
        let sql = format!("insert into {}", chunk);
        debug!("chunk sql length is {}", sql.len());

        if let Err(err) = taos.query(&sql).await {
            match err {
                taos::Error::RawTaosError(err) => match err.code {
                    TaosCode::MnodeDbNotSelected
                    | TaosCode::ClientDbNotSelected
                    | TaosCode::ClientInvalidTableName
                    | TaosCode::MnodeInvalidTableName => {
                        handle_table_schema(&state, taos, database, &req).await?;
                        taos.query(&sql).await?;
                    }
                    code => {
                        warn!("insert into tdengine error: [{}]{}", code, err);
                    }
                },
                err => {
                    error!("error with query [{}]: {}", sql.len(), err);
                }
            }
        }
    }

    Ok(())
}