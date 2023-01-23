// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use aptos_indexer_grpc_cache_worker::{
    constants::APTOS_INDEXER_GRPC_CACHE_WORKER_THREAD_NAME, worker::Worker,
};
use tokio::runtime::Builder;
use warp::Filter;

fn main() {
    aptos_logger::Logger::new().init();
    aptos_crash_handler::setup_panic_handler();

    let runtime = Builder::new_multi_thread()
        .thread_name(APTOS_INDEXER_GRPC_CACHE_WORKER_THREAD_NAME)
        .disable_lifo_slot()
        .enable_all()
        .build()
        .expect("[indexer cache] failed to create runtime");

    // Start processing.
    runtime.spawn(async move {
        let mut worker = Worker::new().await;
        worker.run().await;
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
