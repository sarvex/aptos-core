// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

#[inline]
pub fn get_file_store_bucket_name() -> String {
    std::env::var("FILE_STORE_BUCKET_NAME")
        .unwrap_or_else(|_| "indexer-grpc-file-store".to_string())
}

#[inline]
pub fn get_file_store_blob_folder_name() -> String {
    std::env::var("FILE_STORE_BLOB_FOLDER_NAME").unwrap_or_else(|_| "blobs".to_string())
}

#[inline]
pub fn get_blob_transaction_size() -> u64 {
    std::env::var("BLOB_TRANSACTION_SIZE")
        .unwrap_or_else(|_| "1000".to_string())
        .parse::<u64>()
        .unwrap()
}

fn generate_blob_name(starting_version: u64) -> String {
    format!(
        "txn_{:0>11}_{:0>11}",
        starting_version,
        starting_version + get_blob_transaction_size() - 1
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn verify_blob_naming() {
        assert_eq!(super::generate_blob_name(0), "txn_00000000000_00000000099");
        assert_eq!(
            super::generate_blob_name(100_000_000),
            "txn_00100000000_00100000099"
        );
        assert_eq!(
            super::generate_blob_name(1_000_000_000),
            "txn_01000000000_01000000099"
        );
        assert_eq!(
            super::generate_blob_name(10_000_000_000),
            "txn_10000000000_10000000099"
        );
    }
}

pub mod blob_data;
pub mod processor;
