// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use cloud_storage::{ListRequest, Object};
use futures_util::StreamExt;

pub async fn get_next_file_store_version(bucket_name: String) -> anyhow::Result<u64> {
    let mut object_list_stream = Box::pin(
        Object::list(&bucket_name, ListRequest::default())
            .await
            .expect("[indexer gcs] Failed to list objects in the bucket."),
    );

    let mut next_version = 0;

    while let Some(object_list) = object_list_stream.next().await {
        match object_list {
            Ok(list) => {
                for object in list.items {
                    let blob_name = object.name;
                    let blob_name_split: Vec<&str> = blob_name.split('_').collect();
                    let ending_version = blob_name_split[2].parse::<u64>().unwrap();
                    next_version = std::cmp::max(ending_version + 1, next_version);
                }
            },
            Err(err) => return Err(anyhow::Error::from(err)),
        }
    }
    Ok(next_version)
}
