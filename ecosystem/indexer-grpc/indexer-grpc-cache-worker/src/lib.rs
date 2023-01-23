// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::{fs::File, io::Read, path::PathBuf};

pub mod constants;
pub mod worker;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcCacheWorkerConfig {
    /// Indexer GRPC address.
    pub indexer_address: String,

    /// Chain ID
    pub chain_id: u64,

    /// Starting version; if not provided, will start from the latest version in the cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starting_version: Option<u64>,

    /// Number of workers for processing data. Default is 10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processor_task_count: Option<u64>,

    /// Number of transactions received for each streaming Response. Default is 10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processor_batch_size: Option<u64>,

    /// Redis address. Default is 127.0.0.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redis_address: Option<String>,

    /// Output transaction batch size; default to 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_transaction_batch_size: Option<u64>,
}

impl IndexerGrpcCacheWorkerConfig {
    pub fn load(path: PathBuf) -> Result<Self, anyhow::Error> {
        let mut file = File::open(&path).map_err(|e| {
            anyhow::anyhow!(
                "Unable to open file {}. Error: {}",
                path.to_str().unwrap(),
                e
            )
        })?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).map_err(|e| {
            anyhow::anyhow!(
                "Unable to read file {}. Error: {}",
                path.to_str().unwrap(),
                e
            )
        })?;

        serde_yaml::from_str(&contents).map_err(|e| {
            anyhow::anyhow!(
                "Unable to read yaml {}. Error: {}",
                path.to_str().unwrap(),
                e
            )
        })
    }
}

// 2033-01-01 00:00:00 UTC
const BASE_EXPIRATION_EPOCH_TIME: u64 = 1988150400_u64;

/// Get the TTL in seconds for a given version. Monotonically increasing version will have a larger TTL.
#[inline(always)]
pub fn get_ttl_in_seconds(version: u64) -> u64 {
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    BASE_EXPIRATION_EPOCH_TIME - current_time + (version / 1000)
}
