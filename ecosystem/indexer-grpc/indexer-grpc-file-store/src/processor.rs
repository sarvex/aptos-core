// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{
    blob_data::TransactionsBlob, generate_blob_name, get_blob_transaction_size,
    get_file_store_bucket_name,
};
use aptos_indexer_grpc_gcs_utils::get_next_file_store_version;
use aptos_logger::info;
use cloud_storage::{Bucket, Object};
use deadpool_redis::redis::cmd;
use moving_average::MovingAverage;
use std::{sync::Arc, thread::sleep, time::Duration};

pub struct Processor {
    pub redis_pool: Arc<deadpool_redis::Pool>,
    chain_id: u32,
    next_version: u64,
}

async fn upload_blob_transactions(
    starting_version: u64,
    encoded_proto_data_vec: Vec<String>,
) -> anyhow::Result<()> {
    let blob_object: TransactionsBlob = TransactionsBlob {
        starting_version,
        transactions: encoded_proto_data_vec,
    };

    match Object::create(
        &get_file_store_bucket_name(),
        serde_json::to_vec(&blob_object).unwrap(),
        generate_blob_name(starting_version).as_str(),
        "application/json",
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            info!(
                error = err.to_string(),
                "[indexer file store] Failed to process a blob; retrying in 1 second"
            );
            sleep(Duration::from_secs(1));
            Err(err.into())
        },
    }
}

impl Processor {
    pub fn new(redis_address: String, chain_id: u32) -> Self {
        Self {
            redis_pool: Arc::new(
                deadpool_redis::Config::from_url(redis_address)
                    .create_pool(Some(deadpool_redis::Runtime::Tokio1))
                    .unwrap(),
            ),
            chain_id,
            next_version: 0,
        }
    }

    /// Determines the next blob to process.
    pub async fn start(&mut self) {
        let mut conn = self.redis_pool.get().await.unwrap();
        let chain_id = cmd("GET")
            .arg("chain_id")
            .query_async::<_, String>(&mut conn)
            .await
            .unwrap();
        assert_eq!(chain_id, self.chain_id.to_string());
        let bucket_name = get_file_store_bucket_name();

        // It's fatal if the bucket doesn't exist.
        Bucket::read(&get_file_store_bucket_name())
            .await
            .expect("[Indexer File Store] file bucket exists.");
        self.next_version = get_next_file_store_version(bucket_name)
            .await
            .expect("[Indexer File Store] failed to get next file store version.");

        info!(
            start_version = self.next_version,
            "[indexer file store] Started processing"
        );
    }

    // Starts the processing.
    pub async fn process(&mut self) {
        let mut conn = self.redis_pool.get().await.unwrap();
        let mut ma = MovingAverage::new(10_000);
        loop {
            let versions = (self.next_version..self.next_version + 100).collect::<Vec<u64>>();
            let encoded_proto_data_vec = match cmd("MGET")
                .arg(&versions)
                .query_async::<_, Vec<String>>(&mut conn)
                .await
            {
                Ok(data) => data,
                Err(err) => {
                    info!(
                        error = err.to_string(),
                        "[indexer file store] Hit the head; retrying in 1 second"
                    );
                    sleep(Duration::from_secs(1));
                    continue;
                },
            };
            match upload_blob_transactions(self.next_version, encoded_proto_data_vec).await {
                Ok(_) => {
                    let size = get_blob_transaction_size();
                    self.next_version += size;
                    ma.tick_now(size);
                    info!(
                        version = self.next_version,
                        tps = (ma.avg() * 1000.0) as u64,
                        "[indexer file store] Processed a blob"
                    );
                },
                Err(err) => {
                    info!(
                        error = err.to_string(),
                        "[indexer file store] Failed to process a blob; retrying in 1 second"
                    );
                    sleep(Duration::from_secs(1));
                },
            }
        }
    }
}
