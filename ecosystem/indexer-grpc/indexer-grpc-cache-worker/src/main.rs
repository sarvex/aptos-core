// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use aptos_indexer_grpc_cache_worker::{
    constants::APTOS_INDEXER_GRPC_CACHE_WORKER_THREAD_NAME, worker::Worker,
};
use std::{sync::atomic::Ordering, sync::Arc, sync::atomic::AtomicBool, thread};
use warp::Filter;

fn main() {
    aptos_logger::Logger::new().init();
    aptos_crash_handler::setup_panic_handler();

    let runtime = aptos_runtimes::spawn_named_runtime(
        APTOS_INDEXER_GRPC_CACHE_WORKER_THREAD_NAME.to_string(),
        None,
    );

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
    let term = Arc::new(AtomicBool::new(false));
    while !term.load(Ordering::Acquire) {
        thread::park();
    }
}
