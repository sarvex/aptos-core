// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use aptos_logger::{info, warn};
use aptos_protos::datastream::v1::{
    indexer_stream_server::IndexerStream,
    raw_datastream_response::Response as DatastreamProtoResponse, RawDatastreamRequest,
    RawDatastreamResponse, TransactionOutput, TransactionsOutput,
};
use deadpool_redis::{redis::cmd, Pool};
use futures::Stream;
use moving_average::MovingAverage;
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<RawDatastreamResponse, Status>> + Send>>;

pub struct DatastreamServer {
    pub redis_pool: Arc<Pool>,
}

#[tonic::async_trait]
impl IndexerStream for DatastreamServer {
    type RawDatastreamStream = ResponseStream;

    async fn raw_datastream(
        &self,
        req: Request<RawDatastreamRequest>,
    ) -> Result<Response<Self::RawDatastreamStream>, Status> {
        let (tx, rx) = mpsc::channel(100000);

        let mut conn = self
            .redis_pool
            .get()
            .await
            .expect("[Indexer Data] Failed to get redis connection.");
        let req = req.into_inner();
        let mut current_version = req.starting_version;
        let batch_size = req.output_batch_size;
        let mut ma = MovingAverage::new(10_000);

        tokio::spawn(async move {
            loop {
                // Check if the receiver is closed.
                if tx.is_closed() {
                    break;
                }
                // Last version in this batch is present in redis.
                let versions = (current_version..current_version + batch_size)
                    .into_iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>();

                let encoded_proto_data_vec = match cmd("MGET")
                    .arg(&versions)
                    .query_async::<_, Vec<String>>(&mut conn)
                    .await
                {
                    Ok(v) => v,
                    Err(_) => {
                        warn!(
                            start_version = current_version,
                            end_version = current_version + batch_size,
                            "[Indexer Data] Hit the head; retrying."
                        );
                        std::thread::sleep(Duration::from_millis(100));
                        continue;
                    },
                };

                let item = RawDatastreamResponse {
                    response: Some(DatastreamProtoResponse::Data(TransactionsOutput {
                        transactions: encoded_proto_data_vec
                            .iter()
                            .enumerate()
                            .map(|(i, e)| TransactionOutput {
                                encoded_proto_data: e.clone(),
                                version: current_version + i as u64,
                            })
                            .collect(),
                    })),
                };
                current_version += batch_size;

                if (current_version % 1000) == 0 {
                    ma.tick_now(1000);
                    info!(
                        batch_end_version = current_version - 1,
                        tps = (ma.avg() * 1000.0) as u64,
                        "[Indexer Data] Sent batch successfully"
                    );
                }
                match tx.send(Result::<_, Status>::Ok(item.clone())).await {
                    Ok(_) => {},
                    Err(_) => {
                        // Client disconnects.
                        break;
                    },
                }
            }
            info!("\tclient disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::RawDatastreamStream
        ))
    }
}
