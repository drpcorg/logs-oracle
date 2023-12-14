# DRPC Logs Oracle ![Test](https://github.com/p2p-org/drpc-logs-oracle/actions/workflows/test.yml/badge.svg)

It's a cache answering the question "how many logs will be in the request to the node".

LogsOracle consists of several components:
- [`liboracle`](#c-api): a kernel written in C
- [`liboracle.go`](#go-api): a Go library implemented through cgo
- [`LogsOracle.java`](#java-api): Java library implemented via JEP 424
- [`doracle`](#doracle---rpc-webserver): RPC webserver providing a custom methods for estimating log counts and indexer nodes.

**System Requirements:**
- 64-bit architecture, AVX2/3 will give a good performance boost;
- RAM >=16GB, latency is proportional to the amount of memory;
- SSD or NVMe >=128GB, do not recommend HDD;

## C API

See [liboracle.h](./liboracle.h)

## Go API

See [liboracle.go](./liboracle.go)

## Java API

See [LogsOracle.java](./java/org/drpc/logsoracle/LogsOracle.java)

## Doracle - RPC webserver

Run with docker:
```sh
docker build \
  --build-arg="UID=$(id -u)" --build-arg="GID=$(id -g)" \
  -t drpc-logs-oracle .

mkdir -p -m=0600 ~/.local/share/drpc-logs-oracle # dir for storage
docker run \
  -p 8000:8000 \
  --ulimit memlock=-1:-1 \
  -v ~/.local/share/drpc-logs-oracle:/home/nonroot/data \
  -e ORACLE_DATA_DIR="/home/nonroot/data" \
  -e ORACLE_NODE_ADDR="..." \
  drpc-logs-oracle
```

## Options

Use environment variables for configuration:

```
ORACLE_ENV string (default "production")
  enironment tag, 'development' or 'production'

BIND_PORT int (default ":8000")
  port to RPC server

METRICS_PORT int (default ":8001")
  port to RPC server

ACCESS_LOG bool (default "false")
  enable access logs

DATA_DIR string
  dir for save data

RAM_LIMIT string (default "16GB")
  RAM limit for disk cache

NODE_RPC string
  ethereum json rpc endpoint url

NODE_WS string
  ethereum json ws endpoint url
```
