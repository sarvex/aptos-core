// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use aptos_indexer_grpc_data_service::service::DatastreamServer;
use aptos_protos::datastream::v1::indexer_stream_server::IndexerStreamServer;
use deadpool_redis::{Config, Runtime};
use std::{net::ToSocketAddrs, sync::Arc};
use tokio::runtime::Builder;
use tonic::transport::Server;
use warp::Filter;

pub fn get_redis_address() -> String {
    match std::env::var("REDIS_ADDRESS") {
        Ok(address) => address,
        Err(_) => "127.0.0.1".to_string(),
    }
}

pub fn get_grpc_port() -> u16 {
    match std::env::var("GRPC_PORT") {
        Ok(port) => port.parse().unwrap(),
        Err(_) => 50051,
    }
}

fn main() {
    aptos_logger::Logger::new().init();
    aptos_crash_handler::setup_panic_handler();

    let redis_address = get_redis_address();
    let cfg = Config::from_url(format!("redis://{}:6379", redis_address));
    let redis_pool = Arc::new(cfg.create_pool(Some(Runtime::Tokio1)).unwrap());

    let server = DatastreamServer { redis_pool };

    let runtime = Builder::new_multi_thread()
        .thread_name("aptos-indexer-grpc-data-service")
        .disable_lifo_slot()
        .enable_all()
        .build()
        .expect("[indexer data] failed to create runtime");

    // Start serving.
    runtime.spawn(async move {
        Server::builder()
            .initial_stream_window_size(65535)
            .add_service(IndexerStreamServer::new(server))
            .serve(
                format!("0.0.0.0:{}", get_grpc_port())
                    .to_string()
                    .to_socket_addrs()
                    .unwrap()
                    .next()
                    .unwrap(),
            )
            .await
            .unwrap();
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
