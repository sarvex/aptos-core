// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{
    constants::{
        APTOS_INDEXER_CACHE_RECONNECT_LIMIT, APTOS_INDEXER_FULLNODE_GRPC_PORT,
        APTOS_INDEXER_GRPC_CACHE_PORT, APTOS_INDEXER_GRPC_CACHE_WORKER_CONFIG_PATH_VAR,
        CACHE_KEY_CHAIN_ID,
    },
    get_ttl_in_seconds, IndexerGrpcCacheWorkerConfig,
};
use aptos_logger::{error, info};
use aptos_protos::datastream::v1 as datastream;
use deadpool_redis::{redis::cmd, Config, Pool, Runtime};
use futures::{self, StreamExt};
use moving_average::MovingAverage;
use std::{path::PathBuf, sync::Arc};

pub fn get_worker_config_file_path() -> String {
    std::env::var(APTOS_INDEXER_GRPC_CACHE_WORKER_CONFIG_PATH_VAR)
        .expect("WORKER_CONFIG_PATH is required.")
}

pub struct Worker {
    redis_pool: Arc<Pool>,
    config: IndexerGrpcCacheWorkerConfig,
    next_version: Option<u64>,
    retry_count: u64,
}

impl Worker {
    pub async fn new() -> Self {
        let config_path = get_worker_config_file_path();
        let config = IndexerGrpcCacheWorkerConfig::load(PathBuf::from(config_path)).unwrap();
        let redis_address = match &config.redis_address {
            Some(addr) => addr.clone(),
            _ => "127.0.0.1".to_string(),
        };

        let cfg = Config::from_url(format!(
            "redis://{}:{}",
            redis_address, APTOS_INDEXER_GRPC_CACHE_PORT
        ));

        let redis_pool = Arc::new(cfg.create_pool(Some(Runtime::Tokio1)).unwrap());

        let retry_count = 0;

        let next_version = config.starting_version.as_ref().copied();

        Self {
            redis_pool,
            config,
            next_version,
            retry_count,
        }
    }

    /// Sync with the cache to verify and process the next version.
    async fn cache_status_sync(&mut self) {
        // Failure to connect to Redis is a fatal error.
        let mut conn = self.redis_pool.get().await.unwrap();

        // 1. If the chain id is not present in the cache, set it.
        let if_chain_id_set: bool = cmd("SETNX")
            .arg(&[
                CACHE_KEY_CHAIN_ID.to_string(),
                self.config.chain_id.to_string(),
            ])
            .query_async(&mut conn)
            .await
            .expect("[Redis] SETNX the chain id.");

        // 2. If the chain id is present, check if it matches the config.
        if !if_chain_id_set {
            let cache_chain_id: String = cmd("GET")
                .arg(&[CACHE_KEY_CHAIN_ID.to_string()])
                .query_async(&mut conn)
                .await
                .expect("[Redis] Set the chain id.");
            assert_eq!(cache_chain_id, self.config.chain_id.to_string());
        }

        match self.config.starting_version {
            Some(version) => {
                self.next_version = Some(version);
            },
            _ => {
                self.next_version = Some(0);
            },
        }
    }

    pub async fn run(&mut self) {
        self.cache_status_sync().await;
        // Failure to connect to Redis is a fatal error.
        let mut conn = self.redis_pool.get().await.unwrap();
        // Re-connect if lost.
        loop {
            let mut rpc_client =
                match datastream::indexer_stream_client::IndexerStreamClient::connect(format!(
                    "http://{}:{}",
                    self.config.indexer_address, APTOS_INDEXER_FULLNODE_GRPC_PORT
                ))
                .await
                {
                    Ok(client) => {
                        // Resets the retry count.
                        self.retry_count = 0;
                        client
                    },
                    Err(_e) => {
                        // It's possible that Node is not ready when connecting. Retry.
                        error!(
                            indexer_address = self.config.indexer_address,
                            indexer_port = APTOS_INDEXER_FULLNODE_GRPC_PORT,
                            "[Indexer Cache]Error connecting to indexer",
                        );
                        // TODO: Add a exponential backoff.
                        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                        self.retry_count += 1;
                        if self.retry_count > APTOS_INDEXER_CACHE_RECONNECT_LIMIT {
                            error!(
                                indexer_address = self.config.indexer_address,
                                indexer_port = APTOS_INDEXER_FULLNODE_GRPC_PORT,
                                "[Indexer Cache] Exceeded the retry limit to connecting to node."
                            );

                            panic!("[Indexer Cache] Exceeded the retry limit to connecting to node. Quitting...");
                        }
                        continue;
                    },
                };
            let mut ma = MovingAverage::new(10_000);
            let request = tonic::Request::new(datastream::RawDatastreamRequest {
                processor_task_count: match self.config.processor_task_count {
                    Some(num) => num,
                    _ => 10,
                },
                processor_batch_size: match self.config.processor_batch_size {
                    Some(num) => num,
                    _ => 10,
                },
                // Loads from the recent successful starting version.
                starting_version: self.next_version.unwrap(),
                chain_id: self.config.chain_id as u32,
                output_batch_size: match self.config.output_transaction_batch_size {
                    Some(num) => num,
                    _ => 100_u64,
                },
            });

            let response = rpc_client.raw_datastream(request).await.unwrap();
            let mut resp_stream = response.into_inner();
            let mut init_signal_received = false;
            let mut transaction_count = 0;
            while let Some(received) = resp_stream.next().await {
                let received = match received {
                    Ok(r) => r,
                    Err(e) => {
                        // If the connection is lost, reconnect.
                        error!("[Indexer Cache] Error receiving datastream response: {}", e);
                        break;
                    },
                };

                match received.response.unwrap() {
                    datastream::raw_datastream_response::Response::Status(status) => {
                        match status.r#type {
                            0 => {
                                if init_signal_received {
                                    error!("[Indexer Cache] Multiple init signals received. Restarting...");
                                    break;
                                }
                                init_signal_received = true;
                                // If requested starting version doesn't match, restart.
                                if self.next_version.unwrap() != status.start_version {
                                    {
                                        error!("[Indexer Cache] Current processing contains gap. Restarting...");
                                        break;
                                    }
                                }
                            },
                            1 => {
                                assert_eq!(
                                    self.next_version.unwrap() + transaction_count,
                                    status.end_version.expect("End version exists.") + 1
                                );
                                self.next_version =
                                    Some(status.end_version.expect("End version exists.") + 1);
                                transaction_count = 0;
                            },
                            _ => {
                                // There might be protobuf inconsistency between server and client.
                                // Panic to block running.
                                panic!(
                                    "[Indexer Cache] Unknown RawDatastreamResponse status type."
                                );
                            },
                        }
                    },
                    datastream::raw_datastream_response::Response::Data(data) => {
                        let transaction_len = data.transactions.len();

                        let batch_start_version =
                            data.transactions.as_slice().first().unwrap().version;
                        let batch_end_version =
                            data.transactions.as_slice().last().unwrap().version;
                        // TODO: Batch it correctly.
                        for e in data.transactions {
                            let version = e.version;
                            cmd("SET")
                                .arg(&[
                                    e.version.to_string(),
                                    e.encoded_proto_data,
                                    "EX".to_string(),
                                    get_ttl_in_seconds(version).to_string(),
                                ])
                                .query_async::<_, ()>(&mut conn)
                                .await
                                .unwrap();
                        }

                        ma.tick_now(transaction_len as u64);
                        transaction_count += transaction_len as u64;
                        info!(
                            batch_start_version = batch_start_version,
                            batch_end_version = batch_end_version,
                            tps = (ma.avg() * 1000.0) as u64,
                            "[Indexer Cache] Sent batch successfully"
                        );
                    },
                };
            }
            error!("[Indexer Cache] Datastream connection lost. Reconnecting...");
        }
    }
}
