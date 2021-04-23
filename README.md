# Bailongma

> TDengine adatper for Prometheus

For simple use case, just run:

```sh
bailongma
```

For all options, type `--help` for details.

Long usage here:

```sh
USAGE:
    bailongma [OPTIONS]

FLAGS:
        --help
            Prints help information

    -V, --version
            Prints version information


OPTIONS:
    -c, --chunk-size <chunk-size>
            Sql chunk size.
            
            The larger your table column size is, the small chunk should be setted. [default: 600]

    -h, --host <host>
            TDengine host IP or hostname [default: localhost]

    -l, --level <level>
            Debug level [default: info]

    -L, --listen <listen>
            Listen to an specific ip and port [default: 0.0.0.0:10203]

    -m, --max-connections <max-connections>
            Max TDengine connections
            
            - in concurrent cases, use max as 50000 - for common use, set it as 5000 [default:
            50000]

    -P, --password <password>
            TDengine password [default: taosdata]

    -p, --port <port>
            TDengine server port [default: 6030]

    -u, --user <user>
            TDengine user [default: root]

    -w, --workers <workers>
            Thread works for web request [default: 10]
```

Example:

```sh
bailongma -h tdengine -p 6030 -u root -P taospass --listen 0.0.0.0:10101
```

It will listen to port `10101`.

Configure in `prometheus.yml`:

```yaml
remote_write:
  - url: "localhost:10101/adapters/prometheus/test6"
```

The url endpoint is `/adapters/prometheus/{database}`. It will automatically create database for each endpoint. For the demo config, bailongma will write prometheus data to `test6` database.

## blm-bench-prom

`cargo build` will also produce a benchmark tool for memory tracking.

```sh
blm-bench-prom http://127.0.0.1:10230/adapters/prometheus/test5 \
  --points 10 \
  --metrics 10 \
  --interval 1000 \
  --chunks 1000 \
  --samples 100
```
