# Indexer GRPC cache worker

Cache worker fetches data from fullnode GRPC and push data to Cache. 

## How to run it.

* A yaml file for the worker.

```yaml
indexer_address: 127.0.0.1 # Fullnode GRPC address at port 50051
redis_address: 127.0.0.1 # Redis address at port 6379.
starting_version: 0 # Optional, if not set, start from 0.
chain_id: 43 # For data verification.
```


* Set the `WORKER_CONFIG_PATH` ENV varaible to your yaml fille, and run your cache worker at current folder,
    `WORKER_CONFIG_PATH=worker.yaml cargo run --release`