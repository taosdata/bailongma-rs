# Bailongma

> TDengine adatper for Prometheus, and others will happen soon.

For simple use case, just run:

```sh
bailongma
```

For all options, type `--help` for details.

Long usage here:

```sh
TDengine adapter for prometheus

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

    -C, --max-connections <max-connections>
            Max TDengine connections
            
            - in concurrent cases, use max as 50000 - for common use, set it as 5000 [default:
            50000]

    -M, --max-memory <max-memory>
            Max memroy, unit: GB [default: 50]

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

It will listen to port `10101`(default is `10230`).

Configure in `prometheus.yml`:

```yaml
remote_write:
  - url: "localhost:10101/adapters/prometheus/write"
remote_read:
  - url: "localhost:10101/adapters/prometheus/read"
```

The default database is `prometheus`, use query option `database` to modify this, configuration file is like:

```yaml
remote_write:
  - url: "localhost:10101/adapters/prometheus/write?database=prom1"
remote_read:
  - url: "localhost:10101/adapters/prometheus/read?database=prom1"
```

## Build and Install

```sh
cargo build
cargo install --path .
```

## blm-bench-prom

`cargo build` will also produce a benchmark tool.

```sh
blm-bench-prom http://127.0.0.1:10230/adapters/prometheus/test5 \
  --points 10 \
  --metrics 10 \
  --interval 1000 \
  --chunks 1000 \
  --samples 100
```
