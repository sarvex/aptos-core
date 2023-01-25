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
use aptos_protos::datastream::v1::{self as datastream, RawDatastreamRequest, RawDatastreamResponse, StreamStatus};
use futures::{self, StreamExt};
use moving_average::MovingAverage;
use tonic::{Response, Streaming};
use std::{path::PathBuf, sync::Arc, thread::current};
use redis::{cmd, Commands, ConnectionLike};

pub fn get_worker_config_file_path() -> String {
    std::env::var(APTOS_INDEXER_GRPC_CACHE_WORKER_CONFIG_PATH_VAR)
        .expect("WORKER_CONFIG_PATH is required.")
}

pub struct Worker {
    redis_client: redis::Client,
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

        let redis_client = redis::Client::open(format!("redis://{}", redis_address)).expect("Create redis client failed.");

        let retry_count = 0;

        let next_version = config.starting_version.as_ref().copied();

        Self {
            redis_client: redis_client,
            config,
            next_version,
            retry_count,
        }
    }

    pub async fn run(&mut self) {
        self.cache_status_sync().await;
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
            let request = tonic::Request::new( self.get_data_request());
            let response = rpc_client.raw_datastream(request).await.unwrap();
            let streaming_resp = response.into_inner();

            let mut conn = self.redis_client.get_connection().unwrap();
            Self::process_streaming_response(self.next_version.unwrap(), &mut conn, streaming_resp).await;
            // If streaming ends, try to reconnect.
            error!("[Indexer Cache] Indexer grpc connection lost. Reconnecting...");
        }
    }

    /// Sync with the cache to verify and process the next version.
    async fn cache_status_sync(&mut self) {
        // Failure to connect to Redis is a fatal error.
        let mut conn = self.redis_client.get_connection().unwrap();

        // 1. If the chain id is not present in the cache, set it.
        let if_chain_id_set: bool = conn.set_nx(CACHE_KEY_CHAIN_ID, self.config.chain_id.to_string()).expect("[Redis] SETNX the chain id.");


        // 2. If the chain id is present, check if it matches the config.
        if !if_chain_id_set {
            let cache_chain_id: String = conn.get(CACHE_KEY_CHAIN_ID).expect("[Redis] Get the chain id.");
            assert_eq!(cache_chain_id, self.config.chain_id.to_string());
        }
        // 3. Start the version from the config; if not present, start from 0.
        match self.config.starting_version {
            Some(version) => {
                self.next_version = Some(version);
            },
            _ => {
                self.next_version = Some(0);
            },
        }
    }

    fn get_data_request(&self) -> RawDatastreamRequest {
        RawDatastreamRequest {
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
        }
    }

    pub async fn process_streaming_response<C: ConnectionLike>(
        starting_version: u64,
        conn: &mut C,
        mut resp_stream: impl futures_core::Stream<Item = Result<RawDatastreamResponse, tonic::Status>>  + std::marker::Unpin) {
        let mut ma = MovingAverage::new(10_000);
        // let mut resp_stream = response.into_inner();
        let mut init_signal_received = false;
        let mut transaction_count = 0;
        let mut current_version = starting_version;

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
                            if current_version != status.start_version {
                                {
                                    error!("[Indexer Cache] Current processing contains gap. Restarting...");
                                    break;
                                }
                            }
                        },
                        1 => {
                            assert_eq!(
                                current_version + transaction_count,
                                status.end_version.expect("End version exists.") + 1
                            );
                            current_version= status.end_version.expect("End version exists.") + 1;
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
                        conn.set::<String, String, ()>(e.version.to_string(), e.encoded_proto_data).unwrap();
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
    }
}

#[cfg(test)]
mod test {
    use super::*;
    fn generate_fake_responses(resp: Vec<RawDatastreamResponse>) -> impl futures::Stream<Item = Result<RawDatastreamResponse, tonic::Status>> {
        futures::stream::iter(resp.into_iter().map(|r| Ok(r)))
    }
    use redis_test::{MockCmd, MockRedisConnection};

    #[tokio::test]
    async fn test_process_streaming_response() {
        let resp = vec![RawDatastreamResponse {
            response: Some(datastream::raw_datastream_response::Response::Status(
                StreamStatus {
                    r#type: 0,
                    start_version: 0,
                    end_version: None,
                }
            )),
        }];

        let streaming_resp = generate_fake_responses(resp);

        let mut mock_connection = MockRedisConnection::new(vec![
            MockCmd::new(redis::cmd("EXISTS").arg("foo"), Ok("1")),
        ]);
        Worker::process_streaming_response(0, &mut mock_connection, streaming_resp).await;
    }
}
