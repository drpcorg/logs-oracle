# DRPC Logs Oracle

It's a cache answering the question "how many logs will be in the request to the node".

## System Requirements

- 64-bit architecture, AVX2/3 will give a good performance boost;
- RAM >=16GB, latency is proportional to the amount of memory;
- SSD or NVMe >=128GB, do not recommend HDD;

## Usage

**Local** (required go >= 1.20 and GCC):
- `go mod download`
- `go run . [OPTIONS]`

**With docker**:
```sh
docker build \
  --build-arg="UID=$(id -u)" --build-arg="GID=$(id -g)" \
  -t drpc-logs-oracle .

mkdir -m=0600 ~/.local/share/drpc-logs-oracle # dir for storage
docker run \
  -p 8000:8000 \
  --ulimit memlock=-1:-1 \
  -v ~/.local/share/drpc-logs-oracle:/home/nonroot/data \
  drpc-logs-oracle -data-dir=/home/nonroot/data [OPTIONS]
```

**Options:**

```
-addr string
  address to serve (default ":8000")

-data-dir string
  dir for save data

-buffer-limit string (default "16GB")
  RAM limit for disk cache

-rpc-endpoint string
  ethereum json rpc endpoint url

-rpc-batch uint (default 512)
  count of block for one request to node

-rpc-concurency uint (default 8)
  count of simultaneous requests to node
```
