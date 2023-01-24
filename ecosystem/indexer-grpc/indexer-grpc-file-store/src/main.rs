// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use tokio::runtime::Builder;
use warp::Filter;

pub fn get_redis_address() -> String {
    match std::env::var("REDIS_ADDRESS") {
        Ok(address) => address,
        Err(_) => "127.0.0.1".to_string(),
    }
}

pub fn get_chain_id() -> u32 {
    match std::env::var("CHAIN_ID") {
        Ok(chain_id) => chain_id.parse().unwrap(),
        Err(_) => 1,
    }
}

fn main() {
    aptos_logger::Logger::new().init();
    aptos_crash_handler::setup_panic_handler();

    let runtime = Builder::new_multi_thread()
        .thread_name("aptos-indexer-grpc-file-store")
        .disable_lifo_slot()
        .enable_all()
        .build()
        .expect("[indexer file store] failed to create runtime");

    let redis_address = get_redis_address();

    let chain_id = get_chain_id();

    runtime.spawn(async move {
        let mut processor = aptos_indexer_grpc_file_store::processor::Processor::new(
            format!("redis://{}:6379", redis_address),
            chain_id,
        );

        processor.start().await;

        processor.process().await;
    });

    // Start liveness and readiness probes.
    runtime.spawn(async move {
        let readiness = warp::path("readiness")
            .map(move || warp::reply::with_status("ready", warp::http::StatusCode::OK));
        let liveness = warp::path("liveness")
            .map(move || warp::reply::with_status("ready", warp::http::StatusCode::OK));
        warp::serve(readiness.or(liveness))
            .run(([0, 0, 0, 0], 8080))
            .await;
    });
    std::thread::park();
}
