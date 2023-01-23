// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

// Config-related
pub const APTOS_INDEXER_GRPC_CACHE_PORT: u32 = 6379;
pub const APTOS_INDEXER_FULLNODE_GRPC_PORT: u32 = 50051;
pub const APTOS_INDEXER_CACHE_RECONNECT_LIMIT: u64 = 10;
pub const APTOS_INDEXER_GRPC_CACHE_WORKER_CONFIG_PATH_VAR: &str = "WORKER_CONFIG_PATH";
pub const APTOS_INDEXER_GRPC_CACHE_WORKER_THREAD_NAME: &str = "aptos-indexer-grpc-cache-worker";
pub const CACHE_KEY_CHAIN_ID: &str = "chain_id";
